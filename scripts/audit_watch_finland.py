"""Review queue, not automatic acceptance: Finnish checklist species absent from Swedish columns."""
import concurrent.futures,hashlib,json,re,sys,time
from datetime import datetime,timezone
from pathlib import Path
import requests
sys.path.insert(0,'/proj/boston/beta/pavag-pilot-20261101/geomap/scripts')
from review_watch_selection import baseline,add_updates,URLS
STAGE=Path('/proj/boston/beta/wcr/stage/watch-pilot-20261101'); OUT=STAGE/'source/finland-review';OUT.mkdir(exist_ok=True)
def api(endpoint,params):
 url='https://api.gbif.org/v1/'+endpoint;key=hashlib.sha256(json.dumps([url,params],sort_keys=True).encode()).hexdigest();path=OUT/(key+'.json')
 if path.exists():return json.loads(path.read_text())
 for attempt in range(3):
  try:
   r=requests.get(url,params=params,headers={'User-Agent':'WCR-Finland-omissions-audit/1.0 (beta.waldeck.se)'},timeout=40);r.raise_for_status();data=dict(url=r.url,retrieved_at=datetime.now(timezone.utc).isoformat(),data=r.json());path.write_text(json.dumps(data,ensure_ascii=False));return data
  except requests.RequestException:
   if attempt==2:raise
   time.sleep(2)
def main():
 raw=STAGE/'source/review';rows=baseline((raw/'checklist-2017.txt').read_text())
 for f in ['update-2021','update-2025']:add_updates(rows,(raw/(f+'.txt')).read_text(),f+'.pdf')
 pool={n:v for n,v in rows.items() if 'F' in v['countries'] and 'S' not in v['countries']}
 pool['Nola estonica']=dict(rows.get('Nola estonica',{}),supplement='Finnish checklist 2022 and Nocturna 2024')
 pool['Athetis correpta']=dict(rows.get('Athetis correpta',{}),supplement='Nocturna 2024: new to monitoring, not necessarily new to Finland')
 export=json.loads((STAGE/'pilot-export.json').read_text());visible={p['scientific_name'] for p in export['profiles']+export['references']}
 def check(item):
  n,row=item;result=dict(scientific_name=n,checklist=row,already_visible=n in visible,status='review_required')
  try:
   match=api('species/match',dict(name=n,strict='true'));m=match['data'];result['evidence_urls']=[match['url']]
   if m.get('matchType')!='EXACT' or m.get('rank')!='SPECIES' or m.get('order')!='Lepidoptera':return dict(result,status='taxonomic_review_required')
   key=m.get('acceptedUsageKey',m['usageKey']);result['gbif_key']=key
   for label,params in [('sweden',dict(taxonKey=key,country='SE',occurrenceStatus='PRESENT',limit=0)),('finland_recent',dict(taxonKey=key,country='FI',occurrenceStatus='PRESENT',eventDate='2015-01-01,2026-11-02',limit=0))]:
    res=api('occurrence/search',params);result[label+'_count']=res['data']['count'];result['evidence_urls'].append(res['url'])
   result['status']='swedish_public_record_requires_review' if result['sweden_count'] else 'recent_finnish_presence_needs_review' if result['finland_recent_count'] else 'no_recent_finnish_gbif_count'
  except Exception as exc:result.update(status='query_failed',error=str(exc))
  print(n,result['status'],result.get('finland_recent_count'),flush=True);return result
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool_executor: results=list(pool_executor.map(check,sorted(pool.items())))
 report=dict(checked_at='2026-11-02',scope='2017 Nordic-Baltic checklist with 2021/2025 country updates: F present and S absent; plus Nola estonica and Athetis correpta from Finnish sources',tested=len(results),prior_front=dict(countries=['DK','DE','PL','LT','LV','EE'],latitude=[53,60]),results=results,review_queue=[r for r in results if r['status']=='recent_finnish_presence_needs_review' and not r['already_visible']],limitations=['Not an exhaustive 2026 Finland-versus-Sweden checklist reconciliation.','GBIF absence is not proof of national absence; old checklist names and taxonomic splits need specialist review.','Recent Finnish counts do not prove expansion, arrival in Sweden or suitability.','Nocturna new-to-monitoring records must not be labelled first Finnish records.','Point licensing is independent of biological screening. No observations, taxa or high-interest flags are accepted automatically.'],sources=list(URLS.values())+['https://cdn.laji.fi/files/checklists/2022/Lajiluettelo2022_Checklist2022.pdf','https://www.ymparisto.fi/sites/default/files/documents/Baptria%204_2025%20Nocturna%20vuosiraportti%202024.pdf'])
 (STAGE/'finland-omissions-audit.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
 print('COMPLETE',report['tested'],'screened;',len(report['review_queue']),'not-yet-visible review candidates',flush=True)
if __name__=='__main__':main()
