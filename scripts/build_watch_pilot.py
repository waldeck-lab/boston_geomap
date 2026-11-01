#!/usr/bin/env python3
"""Reproducible, bounded pilot import. No production or shared-store writes."""
import argparse
import hashlib
import json
import sys
import time
from datetime import datetime,timezone
from pathlib import Path
import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.watch_store import connect,initialize,source,import_matrix,import_traits,import_occurrences,inventory,profile,normalized

NAMES=['Cameraria ohridella','Macrosaccus robiniella','Parectopa robiniella','Phyllonorycter issikii','Phyllonorycter leucographella','Phyllonorycter platani','Phyllocnistis citrella','Cydalima perspectalis','Cacoecimorpha pronubana','Chrysodeixis chalcites','Etiella zinckenella','Thysanoplusia orichalcea','Trigonophora flammea','Vanessa cardui','Saturnia pavonia']
def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--stage',type=Path,required=True);ap.add_argument('--offline',action='store_true');args=ap.parse_args()
    stage=args.stage.resolve()
    if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise RuntimeError('Refusing non-pilot output directory')
    raw=stage/'source';raw.mkdir(parents=True,exist_ok=True);(raw/'gbif').mkdir(exist_ok=True)
    session=requests.Session();session.headers['User-Agent']='WCR-future-species-pilot/0.1 (bounded cached research queries)'
    def get_json(url,params):
        key=hashlib.sha256(json.dumps([url,params],sort_keys=True).encode()).hexdigest();path=raw/'gbif'/(key+'.json')
        if not path.exists():
            if args.offline:raise RuntimeError('Missing cached response '+key)
            response=session.get(url,params=params,timeout=35);response.raise_for_status();data=response.json()
            path.write_text(json.dumps(dict(url=response.url,retrieved_at=datetime.now(timezone.utc).isoformat(),data=data),ensure_ascii=False));time.sleep(.15)
        return json.loads(path.read_text()),path
    record=json.loads((raw/'zenodo-record.json').read_text());version=str(record['id']);matrix=next(raw.glob('Lepidoptera*csv'))
    if record['metadata']['license']['id']!='cc-by-4.0':raise RuntimeError('Unexpected source license')
    item=next(f for f in record['files'] if f['key']==matrix.name);algorithm,digest=item['checksum'].split(':')
    if hashlib.file_digest(matrix.open('rb'),algorithm).hexdigest()!=digest:raise RuntimeError('Matrix checksum mismatch')
    initialize(stage/'future-species.sqlite');db=connect(stage/'future-species.sqlite')
    run=db.execute("INSERT INTO import_runs(status) VALUES('running') RETURNING id").fetchone()[0];db.commit()
    try:
        with db:
            matrix_source='gaytan-'+version;traits_source='uk-traits-2024'
            source(db,matrix_source,'Gaytán — European Lepidoptera–plant associations','https://doi.org/10.5281/zenodo.'+version,'CC-BY-4.0',version,matrix)
            source(db,traits_source,'Cook et al. — British and Irish Lepidoptera traits 2024','https://doi.org/10.5285/dbc7cc17-cbbd-49dd-bab4-8e8855768d66','Open Government Licence','2024',raw/'uk-traits-dataset.zip')
            taxon_file=raw/'dyntaxa-lepidoptera.json';taxa=json.loads(taxon_file.read_text())['species'] if taxon_file.exists() else []
            taxon_ids={normalized(t['scientific_name']):t['taxon_id'] for t in taxa}
            ids={}
            for name in NAMES:
                db.execute('INSERT INTO candidates(scientific_name,role,dyntaxa_id) VALUES(?,?,?) ON CONFLICT(scientific_name) DO UPDATE SET dyntaxa_id=excluded.dyntaxa_id',(name,'reference-control' if name in NAMES[-2:] else 'exploratory',taxon_ids.get(name)))
                ids[name]=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(name,)).fetchone()[0]
            report=dict(matrix=import_matrix(db,matrix,ids,matrix_source),traits=import_traits(db,raw/'uk-traits-dataset.zip',ids,traits_source),occurrences={})
            if report['matrix']['missing']:raise RuntimeError('Pilot names absent from host matrix')
            for name,uid in ids.items():
                match,_=get_json('https://api.gbif.org/v1/species/match',{'name':name,'strict':'true'});match=match['data']
                if match.get('matchType')!='EXACT' or match.get('rank')!='SPECIES' or match.get('order')!='Lepidoptera':
                    report['occurrences'][name]={'error':'Taxonomic match requires review'};continue
                taxon=match.get('acceptedUsageKey',match['usageKey']);db.execute('UPDATE candidates SET gbif_key=? WHERE id=?',(taxon,uid))
                params={'taxonKey':taxon,'country':['DK','DE','PL','LT','LV','EE'],'decimalLatitude':'53,60','hasCoordinate':'true','hasGeospatialIssue':'false','occurrenceStatus':'PRESENT','eventDate':'2000-01-01,2026-11-01','license':['CC0_1_0','CC_BY_4_0'],'limit':300}
                response,path=get_json('https://api.gbif.org/v1/occurrence/search',params);data=response['data'];sid='gbif-'+path.stem
                source(db,sid,'GBIF public occurrence search — '+name,response['url'],'Per record: CC0 or CC-BY-4.0',response['retrieved_at'],path)
                counts=import_occurrences(db,data.get('results',[]),uid,sid)
                report['occurrences'][name]=dict(**counts,total_matching_query=data.get('count'),sample_limit=300,complete=data.get('endOfRecords',False))
                print(name,counts,flush=True)
            report['inventory']=inventory(db)
            db.execute("UPDATE import_runs SET status='complete',completed_at=CURRENT_TIMESTAMP,report_json=? WHERE id=?",(json.dumps(report,ensure_ascii=False),run))
        if db.execute('PRAGMA quick_check').fetchone()[0]!='ok' or db.execute('PRAGMA foreign_key_check').fetchall():raise RuntimeError('Database integrity failure')
        (stage/'inventory.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
        export=dict(generated_at=datetime.now(timezone.utc).isoformat(),pilot=True,notice='Technical source-coverage pilot; Swedish status and expansion unreviewed. Bounded GBIF samples are not complete histories.',inventory=inventory(db),profiles=[profile(db,r['id']) for r in db.execute('SELECT id FROM candidates ORDER BY id')],sources=[dict(r) for r in db.execute('SELECT * FROM sources')])
        (stage/'pilot-export.json').write_text(json.dumps(export,ensure_ascii=False,indent=2))
        print('Pilot database:',stage/'future-species.sqlite','species:',len(ids))
    except BaseException:
        db.rollback();db.execute("UPDATE import_runs SET status='failed',completed_at=CURRENT_TIMESTAMP WHERE id=?",(run,));db.commit();raise
    finally:db.close()
if __name__=='__main__':main()
