"""Audit map-license false negatives; results are a review queue, not accepted species."""
import argparse,hashlib,json,time
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
from concurrent.futures import ThreadPoolExecutor
import requests

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--stage',type=Path,required=True);a=ap.parse_args();stage=a.stage.resolve()
    if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise ValueError('Pilot stage required')
    selection=json.loads((stage/'selection-review.json').read_text());cache=stage/'source/omissions-audit-20261102';cache.mkdir(exist_ok=True)
    old=[(n,r) for n,r in selection['assessments'].items() if r.get('gbif_sweden_count')==0 and r.get('front_2015_2026_count')==0]
    def check(item):
        name,row=item;url=next(u for u in row['evidence_urls'] if 'occurrence/search' in u and 'country=SE' not in u)
        params=parse_qs(urlsplit(url).query);params.pop('license',None)
        path=cache/(hashlib.sha256(json.dumps(params,sort_keys=True).encode()).hexdigest()+'.json')
        try:
            if path.exists():result=json.loads(path.read_text())
            else:
                time.sleep(1)
                r=requests.get('https://api.gbif.org/v1/occurrence/search',params=params,timeout=35);r.raise_for_status();result=dict(url=r.url,count=r.json()['count']);path.write_text(json.dumps(result))
            return dict(scientific_name=name,previous_licensed_front_count=0,all_license_front_count=result['count'],evidence_url=result['url'],swedish_check_date=row['checked_at'],status='needs_species_level_review',reason='Map-reuse license was used as a biological selection gate')
        except Exception as e:return dict(scientific_name=name,status='query_failed',error=str(e))
    with ThreadPoolExecutor(max_workers=1) as pool:results=list(pool.map(check,old))
    report=dict(checked_at='2026-11-02',tested=len(results),new_review_queue=[r for r in results if r.get('all_license_front_count',0)>0],no_current_front=[r['scientific_name'] for r in results if r.get('all_license_front_count')==0],failed=[r for r in results if r['status']=='query_failed'],limitations=['This only audits one known filter in already assessed species, not completeness of all European taxa.','Positive aggregate counts do not prove expansion or authorize republication of individual records.','Swedish counts were zero at previous screening; each queued species needs a fresh national and literature check before inclusion.','Species outside the Nordic-Baltic checklist, outside the host matrix, after the 50-species stop, and taxonomic aliases need separate review.'])
    target=stage/'omissions-audit.json';target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print('Audited',len(results),'false-negative review queue',len(report['new_review_queue']),'failed',len(report['failed']),flush=True)
    for r in report['new_review_queue']:print(r['scientific_name'],r['all_license_front_count'],flush=True)
if __name__=='__main__':main()
