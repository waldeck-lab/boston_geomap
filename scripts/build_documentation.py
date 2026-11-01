#!/usr/bin/env python3
"""Build an atomic, independent species documentation SQLite snapshot."""
import argparse
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path
import requests

ROOT=Path('/proj/boston/beta/wcr')
sys.path.insert(0,str(ROOT/'geomap'))
from geomap.documentation import NRM_INDEX,match_nrm

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--refresh-index',action='store_true');args=parser.parse_args()
    directory=ROOT/'stage/documentation';source=directory/'source';source.mkdir(parents=True,exist_ok=True)
    html_path=source/'nrm-index.html';retrieval_path=source/'nrm-index.meta.json'
    now=datetime.now(timezone.utc).isoformat()
    if args.refresh_index or not html_path.exists():
        response=requests.get(NRM_INDEX,timeout=45);response.raise_for_status()
        if len(response.content)>5*1024*1024:raise RuntimeError('Unexpectedly large NRM index')
        html_path.write_bytes(response.content);retrieval_path.write_text(json.dumps(dict(url=response.url,retrieved_at=now,encoding=response.encoding or 'iso-8859-1')))
    if not retrieval_path.exists():
        retrieval_path.write_text(json.dumps(dict(url=NRM_INDEX,retrieved_at=datetime.fromtimestamp(html_path.stat().st_mtime,timezone.utc).isoformat(),encoding='iso-8859-1')))
    catalogue_path=ROOT/'stage/lists/defaults/lepidoptera-species.json'
    raw_catalogue=catalogue_path.read_bytes();catalogue=json.loads(raw_catalogue)
    raw_html=html_path.read_bytes();retrieval=json.loads(retrieval_path.read_text())
    matches,index_rows=match_nrm(catalogue,raw_html.decode(retrieval['encoding']))
    if index_rows<2000:raise RuntimeError('NRM species index appears incomplete; refusing replacement')
    path=directory/'documentation.sqlite';temporary=directory/'documentation.sqlite.building'
    if temporary.exists():raise RuntimeError('Another build or unfinished database exists')
    db=sqlite3.connect(temporary)
    try:
        db.executescript('''PRAGMA foreign_keys=ON;
         CREATE TABLE species(taxon_id INTEGER PRIMARY KEY,scientific_name TEXT NOT NULL,swedish_name TEXT NOT NULL);
         CREATE TABLE documentation(taxon_id INTEGER NOT NULL REFERENCES species(taxon_id),source TEXT NOT NULL,
           url TEXT,status TEXT NOT NULL,matched_name TEXT,match_method TEXT,provenance_json TEXT NOT NULL,
           PRIMARY KEY(taxon_id,source)) WITHOUT ROWID;
         CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);''')
        with db:
            for row in catalogue['species']:
                tid=int(row['taxon_id']);db.execute('INSERT INTO species VALUES(?,?,?)',(tid,row['scientific_name'],row['swedish_name']))
                db.execute('INSERT INTO documentation VALUES(?,?,?,?,?,?,?)',(tid,'artfakta',f'https://artfakta.se/taxa/{tid}/information','taxon_route',row['scientific_name'],'dyntaxa-id',json.dumps(dict(taxonomy_source_sha256=catalogue['source_sha256']))))
                match=matches[tid]
                db.execute('INSERT INTO documentation VALUES(?,?,?,?,?,?,?)',(tid,'nrm',match['url'],match['status'],match.get('matched_name'),match.get('match_method'),json.dumps(dict(index=NRM_INDEX,candidates=match.get('candidates',[])))))
            report=dict(species=len(catalogue['species']),nrm_index_rows=index_rows,nrm=dict(Counter(r['status'] for r in matches.values())),artfakta=len(catalogue['species']),built_at=now,nrm_retrieved_at=retrieval['retrieved_at'],nrm_index_sha256=hashlib.sha256(raw_html).hexdigest(),catalogue_sha256=hashlib.sha256(raw_catalogue).hexdigest())
            db.executemany('INSERT INTO metadata VALUES(?,?)',[(key,json.dumps(value)) for key,value in report.items()])
        assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
    except BaseException:
        db.close();temporary.unlink();raise
    db.close()
    temporary.replace(path)
    (directory/'build-report.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report))

if __name__=='__main__':main()
