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
from geomap.watch_scope import get_scope
from geomap.watch_store import connect,initialize,source,import_matrix,import_traits,import_occurrences,import_search_guides,import_swedish_names,import_expansion_assessments,import_species_supplements,inventory,profile,normalized

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
    selection=json.loads((stage/'selection-review.json').read_text());names=list(selection['names'])
    additions_path=Path(__file__).resolve().parents[1]/'data/watch/additional-candidates.json'
    additions=json.loads(additions_path.read_text(encoding='utf-8'))
    for name,assessment in additions['assessments'].items():
        if name not in names:names.append(name)
        selection['assessments'][name]=assessment
    exclusions=json.loads((Path(__file__).resolve().parents[1]/'data/watch/selection-exclusions.json').read_text(encoding='utf-8'))
    excluded_names={r['scientific_name'] for r in exclusions['excluded']}
    if set(names)&excluded_names:raise RuntimeError('Excluded resident species in watch cohort')
    if not 10<=len(names)<=100 or any(selection['assessments'][n]['classification']!='watch_candidate' for n in names):raise RuntimeError('Candidate screening is required before import')
    for name in names:
        assessment=selection['assessments'][name]
        if 'S' in assessment['checklist']['countries'] or assessment.get('gbif_sweden_count')!=0 or assessment.get('front_2015_2026_count',0)<1:raise RuntimeError('Swedish record or missing front evidence in candidate list: '+name)
    initialize(stage/'future-species.sqlite');db=connect(stage/'future-species.sqlite')
    run=db.execute("INSERT INTO import_runs(status) VALUES('running') RETURNING id").fetchone()[0];db.commit()
    try:
        with db:
            matrix_source='gaytan-'+version;traits_source='uk-traits-2024'
            source(db,'nrm-candidates-'+additions['version'],'Source-screened additional watch candidates including Finland','http://www3.nrm.se/en/svenska_fjarilar/kataloger/CATLEP2015.xls','Source citation and factual screening only',additions['version'],additions_path)
            source(db,matrix_source,'Gaytán — European Lepidoptera–plant associations','https://doi.org/10.5281/zenodo.'+version,'CC-BY-4.0',version,matrix)
            source(db,traits_source,'Cook et al. — British and Irish Lepidoptera traits 2024','https://doi.org/10.5285/dbc7cc17-cbbd-49dd-bab4-8e8855768d66','Open Government Licence','2024',raw/'uk-traits-dataset.zip')
            taxon_file=raw/'dyntaxa-lepidoptera.json';taxa=json.loads(taxon_file.read_text())['species'] if taxon_file.exists() else []
            taxon_ids={normalized(t['scientific_name']):t['taxon_id'] for t in taxa}
            ids={}
            for name in names:
                db.execute('INSERT INTO candidates(scientific_name,role,dyntaxa_id) VALUES(?,?,?) ON CONFLICT(scientific_name) DO UPDATE SET dyntaxa_id=COALESCE(candidates.dyntaxa_id,excluded.dyntaxa_id)',(name,'watch_candidate',taxon_ids.get(name)))
                ids[name]=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(name,)).fetchone()[0]
            for filename,url in selection['source_urls'].items():
                source(db,'review-'+filename,'Country-status review: '+filename,url,'Copyright retained; source citation and factual screening only','review-2026-11-01',raw/'review'/filename)
            for name,assessment in selection['assessments'].items():
                candidate=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(name,)).fetchone()
                if not candidate:continue
                classification=assessment['classification'];status={'watch_candidate':'not_listed_in_reviewed_sources','swedish_record':'recorded','swedish_record_to_review':'public_record_unverified'}.get(classification,'unknown')
                role='reference-control' if classification.startswith('swedish_record') else classification
                db.execute('UPDATE candidates SET role=?,swedish_status=?,review_status=?,review_notes=? WHERE id=?',(role,status,'source-screened; expert review pending',assessment.get('reason'),candidate['id']))
                db.execute('INSERT OR REPLACE INTO candidate_assessments VALUES(?,?,?,?,?)',(candidate['id'],classification,assessment['checked_at'],assessment.get('reason','Further review required'),json.dumps(assessment,ensure_ascii=False)))
            report=dict(matrix=import_matrix(db,matrix,ids,matrix_source),traits=import_traits(db,raw/'uk-traits-dataset.zip',ids,traits_source),occurrences={})
            unexplained=[n for n in report['matrix']['missing'] if selection['assessments'][n].get('ecology_gap_acknowledged') is not True]
            if unexplained:raise RuntimeError('Unacknowledged host matrix gaps: '+', '.join(unexplained))
            for name,uid in ids.items():
                match,_=get_json('https://api.gbif.org/v1/species/match',{'name':name,'strict':'true'});match=match['data']
                if match.get('matchType')!='EXACT' or match.get('rank')!='SPECIES' or match.get('order')!='Lepidoptera':
                    report['occurrences'][name]={'error':'Taxonomic match requires review'};continue
                taxon=match.get('acceptedUsageKey',match['usageKey']);db.execute('UPDATE candidates SET gbif_key=? WHERE id=?',(taxon,uid))
                scope_name=selection['assessments'][name].get('occurrence_scope');geography=get_scope(scope_name)
                params={'taxonKey':taxon,'country':geography['countries'],'decimalLatitude':','.join(map(str,geography['latitude_range'])),'hasCoordinate':'true','hasGeospatialIssue':'false','occurrenceStatus':'PRESENT','eventDate':'2000-01-01,'+('2026-11-02' if scope_name else '2026-11-01'),'license':['CC0_1_0','CC_BY_4_0'],'limit':300}
                response,path=get_json('https://api.gbif.org/v1/occurrence/search',params);data=response['data'];sid='gbif-'+path.stem
                source(db,sid,'GBIF public occurrence search — '+name,response['url'],'Per record: CC0 or CC-BY-4.0',response['retrieved_at'],path)
                counts=import_occurrences(db,data.get('results',[]),uid,sid,scope=scope_name)
                report['occurrences'][name]=dict(**counts,total_matching_query=data.get('count'),sample_limit=300,complete=data.get('endOfRecords',False))
                print(name,counts,flush=True)
            report['search_guides']=import_search_guides(db,Path(__file__).resolve().parents[1]/'data/watch/search-guides.json')
            report['expansion_assessments']=import_expansion_assessments(db,Path(__file__).resolve().parents[1]/'data/watch/expansion-evidence.json')
            report['species_supplements']=import_species_supplements(db,Path(__file__).resolve().parents[1]/'data/watch/lepiforum-facts.json')
            if taxon_file.exists():
                name_source='dyntaxa-names-'+hashlib.sha256(taxon_file.read_bytes()).hexdigest()[:16]
                source(db,name_source,'SLU Artdatabanken / Dyntaxa – svenska namn i lokal taxonomikatalog','https://www.dyntaxa.se/','Taxon names; source attribution','cached snapshot',taxon_file)
                import_swedish_names(db,taxon_file,name_source)
            for name,swedish_name in additions.get('swedish_names',{}).items():
                if swedish_name and name in ids and not db.execute('SELECT 1 FROM vernacular_names WHERE candidate_id=?',(ids[name],)).fetchone():
                    db.execute('INSERT INTO vernacular_names VALUES(?,?,?,?,?)',(ids[name],swedish_name,'sv','nrm-candidates-'+additions['version'],name))
            overrides=Path(__file__).resolve().parents[1]/'data/watch/swedish-name-overrides.json'
            name_data=json.loads(overrides.read_text(encoding='utf-8'))
            for item in name_data['names']:
                row=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(item['scientific_name'],)).fetchone()
                if not row:continue
                sid='sv-name-override-'+str(row['id'])+'-'+name_data['version']
                source(db,sid,item['attribution'],'','User-supplied taxon name',name_data['version'],overrides)
                db.execute('INSERT OR REPLACE INTO vernacular_names VALUES(?,?,?,?,?)',(row['id'],item['swedish_name'],'sv',sid,item['scientific_name']))
            report['inventory']=inventory(db)
            db.execute("UPDATE import_runs SET status='complete',completed_at=CURRENT_TIMESTAMP,report_json=? WHERE id=?",(json.dumps(report,ensure_ascii=False),run))
        if db.execute('PRAGMA quick_check').fetchone()[0]!='ok' or db.execute('PRAGMA foreign_key_check').fetchall():raise RuntimeError('Database integrity failure')
        (stage/'inventory.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
        def export_profile(uid):
            result=profile(db,uid)
            result['occurrence_scope']=get_scope((result.get('assessment') or {}).get('evidence',{}).get('occurrence_scope'))
            response,path=get_json('https://api.gbif.org/v1/species/match',{'name':result['scientific_name'],'strict':'true'})
            taxon=response['data']
            if taxon.get('matchType')!='EXACT' or taxon.get('rank')!='SPECIES' or taxon.get('order')!='Lepidoptera' or not taxon.get('family'):raise RuntimeError('Family taxonomy requires review: '+result['scientific_name'])
            result['family']=taxon['family']
            result['family_source']=dict(url=response['url'],retrieved_at=response['retrieved_at'],sha256=hashlib.sha256(path.read_bytes()).hexdigest(),matched_name=taxon['scientificName'])
            return result
        export=dict(generated_at=datetime.now(timezone.utc).isoformat(),pilot=True,notice='Source-screened watch candidates, not proof of national absence or an establishment forecast. Bounded GBIF samples are not complete histories.',inventory=inventory(db),profiles=[export_profile(ids[name]) for name in names],references=[export_profile(r['id']) for r in db.execute("SELECT id,scientific_name FROM candidates WHERE role='reference-control' ORDER BY scientific_name") if r['scientific_name'] not in excluded_names],screening_limitations=selection['limitations'],sources=[dict(r) for r in db.execute('SELECT * FROM sources')])
        (stage/'pilot-export.json').write_text(json.dumps(export,ensure_ascii=False,indent=2))
        print('Pilot database:',stage/'future-species.sqlite','species:',len(ids))
    except BaseException:
        db.rollback();db.execute("UPDATE import_runs SET status='failed',completed_at=CURRENT_TIMESTAMP WHERE id=?",(run,));db.commit();raise
    finally:db.close()
if __name__=='__main__':main()
