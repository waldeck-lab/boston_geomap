#!/usr/bin/env python3
"""Populate the independent beta store from public SOS search results, red list first."""
import argparse
import csv
import fcntl
import hashlib
import gzip
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import requests

ROOT=Path('/proj/boston/beta/wcr')
COVERAGE_SCOPE='public-only-start-v1'
sys.path.insert(0,str(ROOT/'geomap'))
from geomap.shared_store import connect, initialize, register_list, apply_rows, rebuild_cells, list_stats


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--owner-id',type=int,required=True)
    parser.add_argument('--date-from',default='2000-01-01')
    parser.add_argument('--date-to',required=True)
    args=parser.parse_args()
    directory=args.directory.resolve()
    if not directory.is_relative_to(ROOT/'stage'):
        raise RuntimeError('Import directory must be inside beta stage')
    directory.mkdir(parents=True,exist_ok=True)
    lock=(directory/'import.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    store=directory/'shared.sqlite'
    cache=directory/'exports';cache.mkdir(exist_ok=True)
    status_path=directory/'import-status.json'
    progress=dict(busy=True,phase='initializing',completed_batches=0,imported_rows=0,
                  date_from=args.date_from,date_to=args.date_to,source_scope='public')
    status_lock=threading.RLock()

    def status(**values):
        with status_lock:
            progress.update(values)
            tmp=status_path.with_suffix('.tmp');tmp.write_text(json.dumps(progress));tmp.replace(status_path)

    initialize(store)
    definitions=[('redlisted-lepidoptera','Rödlistade fjärilar i Sverige',
        ROOT/'stage/lists/defaults/redlisted-lepidoptera/redlisted_lepidoptera.csv',None),
        ('missing-jonas','Mina saknade arter',ROOT/'stage/lists/missing_species.csv',args.owner_id)]
    for list_id,title,path,owner in definitions:
        with connect(store,readonly=True) as db:
            exists=db.execute('SELECT 1 FROM lists WHERE id=?',(list_id,)).fetchone()
        if not exists:
            register_list(store,list_id,title,path,owner)
        else:
            with path.open(encoding='utf-8-sig',newline='') as stream:
                ids=sorted({int(float(r['taxon_id'])) for r in csv.DictReader(stream)})
            digest=hashlib.sha256(json.dumps(ids).encode()).hexdigest()
            with connect(store,readonly=True) as db:
                stored=db.execute('SELECT content_hash FROM lists WHERE id=?',(list_id,)).fetchone()[0]
            if digest!=stored:raise RuntimeError('Seed list changed during import; create a new list version')
    key=json.loads((ROOT/'ops/geomap-env.json').read_text())['ARTDATABANKEN_SUBSCRIPTION_KEY']
    clients=threading.local()
    # Deliberately no personal Authorization header and no observedByMe filter.
    headers={'Ocp-Apim-Subscription-Key':key,'X-Api-Version':'1.5'}
    base='https://api.artdatabanken.se/species-observation-system/v1'
    last_request=0
    blocked_until=0
    rate_lock=threading.Lock()

    def post(path,body,params):
        nonlocal last_request,blocked_until
        for attempt in range(8):
            with rate_lock:
                time.sleep(max(0,.6-(time.monotonic()-last_request),blocked_until-time.monotonic()))
                last_request=time.monotonic()
            print('SOS request',path,'taxa',len(body['taxon']['ids']),'attempt',attempt+1,flush=True)
            try:
                if not hasattr(clients,'session'):
                    clients.session=requests.Session();clients.session.headers.update(headers)
                response=clients.session.post(base+path,json=body,params=params,timeout=(10,45))
            except requests.RequestException:
                if attempt<1:
                    continue
                raise
            if response.status_code==429 or response.status_code>=500:
                if attempt<7:
                    try:wait=int(response.headers.get('Retry-After','3'))
                    except ValueError:wait=3
                    match=re.search(r'Try again in\s+(\d+)\s+seconds',response.text,flags=re.I)
                    if match:wait=max(wait,int(match.group(1)))
                    print('SOS transient HTTP',response.status_code,'retry wait',wait,'body',response.text[:250],flush=True)
                    status(retry_after_seconds=wait)
                    with rate_lock:blocked_until=max(blocked_until,time.monotonic()+wait)
                    for _ in range(max(1,wait)):time.sleep(1)
                    continue
            if not response.ok:
                (directory/'failed-request.json').write_text(json.dumps({'path':path,'body':body,'status':response.status_code,'message':response.text[:1000]}))
            response.raise_for_status()
            return response
        raise RuntimeError('Public SOS request failed')

    try:
        for phase,(list_id,_,_,_) in enumerate(definitions,1):
            with connect(store,readonly=True) as db:
                ids=[r[0] for r in db.execute('SELECT taxon_id FROM list_taxa WHERE list_id=? ORDER BY taxon_id',(list_id,))]
                covered={r[0] for r in db.execute('SELECT taxon_id FROM coverage WHERE date_from=? AND date_to=? AND source_scope=?',
                            (args.date_from,args.date_to,COVERAGE_SCOPE))}
            pending_ids=[t for t in ids if t not in covered]
            status(phase=f'importing-list-{phase}',taxa_total=len(ids),taxa_reused=len(ids)-len(pending_ids),taxa_remaining=len(pending_ids))
            pending=[(pending_ids[i:i+20],args.date_from,args.date_to) for i in range(0,len(pending_ids),20)]
            while pending:
                batch,start,end=pending.pop(0)
                with connect(store,readonly=True) as db:
                    done={r[0] for r in db.execute('SELECT taxon_id FROM coverage WHERE date_from=? AND date_to=? AND source_scope=?',
                        (start,end,COVERAGE_SCOPE))}
                batch=[t for t in batch if t not in done]
                if not batch:
                    continue
                body={'taxon':{'ids':batch,'includeUnderlyingTaxa':False},
                      'date':{'startDate':start+'T00:00:00Z','endDate':end+'T23:59:59Z','dateFilterType':'OnlyStartDate'}}
                count=int(post('/Observations/Count',body,{'sensitiveObservations':'false'}).json())
                # Keep each search below SOS's 10,000-record result window.
                if count>9000:
                    if len(batch)>1:
                        mid=len(batch)//2;pending[:0]=[(batch[:mid],start,end),(batch[mid:],start,end)]
                    else:
                        a,b=date.fromisoformat(start),date.fromisoformat(end)
                        if a>=b:raise RuntimeError('One-day export exceeds safe limit; no truncation accepted')
                        mid=a+(b-a)//2;pending[:0]=[(batch,start,mid.isoformat()),(batch,(mid+timedelta(days=1)).isoformat(),end)]
                    continue
                rows=[]
                cache_key=hashlib.sha256(json.dumps(body,sort_keys=True).encode()).hexdigest()
                if count:
                    search_body=dict(body,output={'fields':[
                        'occurrence.occurrenceId','taxon.id','event.startDate',
                        'location.decimalLatitude','location.decimalLongitude',
                        'occurrence.occurrenceStatus','occurrence.individualCount','modified']})
                    records=[]
                    def get_page(skip):
                        response=post('/Observations/Search',search_body,{'skip':skip,'take':1000,
                            'sensitiveObservations':'false','sortBy':'occurrence.occurrenceId','sortOrder':'Asc'})
                        payload=response.json();page=payload.get('records',[])
                        if len(page)!=min(1000,count-skip) or int(payload.get('totalCount',-1))!=count:
                            raise RuntimeError('Count/search mismatch; refusing incomplete coverage')
                        return page
                    with ThreadPoolExecutor(max_workers=3) as pool:
                        for page in pool.map(get_page,range(0,count,1000)):records.extend(page)
                    for record in records:
                        occurrence=record.get('occurrence') or {}
                        event=record.get('event') or {}
                        location=record.get('location') or {}
                        taxon=record.get('taxon') or {}
                        rows.append(dict(OccurrenceId=occurrence.get('occurrenceId',''),
                            DyntaxaTaxonId=taxon.get('id'),StartDate=event.get('startDate',''),
                            DecimalLatitude=location.get('decimalLatitude'),DecimalLongitude=location.get('decimalLongitude'),
                            OccurrenceStatus=(occurrence.get('occurrenceStatus') or {}).get('value',''),
                            IndividualCount=occurrence.get('individualCount'),Modified=record.get('modified')))
                    # Cache only the fields used by the map, not reporter names or other personal metadata.
                    with gzip.open(cache/(cache_key+'.rows.json.gz'),'wt',encoding='utf-8') as stream:
                        json.dump(rows,stream)
                    required={'OccurrenceId','DyntaxaTaxonId','StartDate','DecimalLatitude','DecimalLongitude'}
                    if not rows or not required.issubset(rows[0]):raise RuntimeError('Unexpected export columns')
                    if any(int(r['DyntaxaTaxonId']) not in batch for r in rows):raise RuntimeError('Out-of-scope taxon in export')
                with connect(store) as db:
                    written=apply_rows(db,rows)
                    # Remember completed subintervals too, so resumed date splits are reusable.
                    db.executemany('INSERT OR REPLACE INTO coverage(taxon_id,date_from,date_to,source_scope) VALUES(?,?,?,?)',
                        [(t,start,end,COVERAGE_SCOPE) for t in batch])
                progress['completed_batches']+=1;progress['imported_rows']+=written
                status(last_batch_count=count)
                print(f'Phase {phase}, batch {progress["completed_batches"]}: received={len(rows)} kept={written}',flush=True)
            with connect(store) as db:
                db.executemany('INSERT OR REPLACE INTO coverage(taxon_id,date_from,date_to,source_scope) VALUES(?,?,?,?)',
                    [(t,args.date_from,args.date_to,COVERAGE_SCOPE) for t in ids])
                rebuild_cells(db)
                counts=list_stats(db,list_id)
            status(phase=f'list-{phase}-complete')
            # Private list names are not written to the public progress file.
            print('Phase',phase,'complete:',json.dumps(counts),'reused_taxa',len(ids)-len(pending_ids),flush=True)
        with connect(store) as db:
            assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
            db.execute('ANALYZE')
            report={'unique_observations':db.execute('SELECT COUNT(*) FROM observations').fetchone()[0],
                    'base_cells':db.execute('SELECT COUNT(*) FROM taxon_cells').fetchone()[0],
                    'lists':{item[0]:list_stats(db,item[0]) for item in definitions}}
        (directory/'exercise-report.json').write_text(json.dumps(report,indent=2))
        status(busy=False,phase='done')
        print('COMPLETE',json.dumps(report),flush=True)
    except BaseException as exc:
        status(busy=False,phase='failed',error=type(exc).__name__)
        raise


if __name__=='__main__':main()
