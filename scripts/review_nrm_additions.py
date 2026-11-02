"""Screen six source-reviewed additions without treating database absence as proof."""
import argparse
import hashlib
import json
from pathlib import Path
import requests
from review_watch_selection import baseline,add_updates,front_params,URLS

NAMES={'Heliomata glarearia':'stäppnätmätare','Polymixis lichenea':'kusthöstfly','Scopula imitaria':'rödgul lövmätare','Aporophyla nigra':'svart puckelfly','Conistra ligula':'','Lithophane leautieri':''}
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--stage',type=Path,required=True);args=ap.parse_args()
    stage=args.stage.resolve()
    if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise ValueError('Pilot stage required')
    raw=stage/'source/review';cache=stage/'source/nrm-review-20261102';cache.mkdir(exist_ok=True)
    rows=baseline((raw/'checklist-2017.txt').read_text())
    for label in ['update-2021.pdf','update-2025.pdf']:add_updates(rows,(raw/label).with_suffix('.txt').read_text(),label)
    session=requests.Session()
    def api(endpoint,params):
        url='https://api.gbif.org/v1/'+endpoint
        path=cache/(hashlib.sha256(json.dumps([url,params],sort_keys=True).encode()).hexdigest()+'.json')
        if not path.exists():
            response=session.get(url,params=params,timeout=35);response.raise_for_status()
            path.write_text(json.dumps(dict(url=response.url,data=response.json()),ensure_ascii=False))
        return json.loads(path.read_text())
    assessments={}
    for name in NAMES:
        row=rows.get(name)
        if row is None:
            if name!='Conistra ligula':raise ValueError('Missing checklist row: '+name)
            row=dict(number=None,countries=[],sources=[],note='Not listed in examined Nordic-Baltic checklist; separate Lepiforum and Pyrgus range sources used. No country-column inference.')
        if 'S' in row['countries']:raise ValueError('Swedish checklist record: '+name)
        match=api('species/match',{'name':name,'strict':'true'})['data']
        if match.get('matchType')!='EXACT' or match.get('rank')!='SPECIES' or match.get('order')!='Lepidoptera':raise ValueError('Taxon mismatch: '+name)
        key=match.get('acceptedUsageKey',match['usageKey'])
        se=api('occurrence/search',{'taxonKey':key,'country':'SE','occurrenceStatus':'PRESENT','limit':0})
        params=front_params(key)
        front=api('occurrence/search',params)
        mappable=api('occurrence/search',front_params(key,mappable=True))
        if se['data']['count'] or not front['data']['count']:raise ValueError('Positive Swedish record or missing front: '+name)
        urls=[URLS[f] for f in row['sources']]+[se['url'],front['url']]
        if name in ['Conistra ligula','Lithophane leautieri']:urls+=['https://lepiforum.org/wiki/page/'+name.replace(' ','_')]
        else:urls+=['http://www3.nrm.se/en/svenska_fjarilar/kataloger/CATLEP2015.xls']
        assessments[name]=dict(classification='watch_candidate',checked_at='2026-11-02',checklist=row,gbif_sweden_count=0,front_2015_2026_count=front['data']['count'],mappable_front_2015_2026_count=mappable['data']['count'],gbif_key=key,evidence_urls=urls,reason='Källgranskad bevakningskandidat. Inga publika svenska GBIF-belägg i kontrollen och ingen svensk markering i granskat checklistunderlag. Fyndräkningen i närområdet omfattar alla licenser; endast CC0/CC BY-fynd återpubliceras på kartan. Detta är inte bevisad svensk frånvaro eller i sig belagd expansion; sena svenska rapporter behöver fortsatt expertgranskas.')
        print(name,front['data']['count'],'front records',flush=True)
    target=Path(__file__).resolve().parents[1]/'data/watch/additional-candidates.json'
    target.write_text(json.dumps(dict(version='2026-11-02-v2',swedish_names=NAMES,assessments=assessments),ensure_ascii=False,indent=2)+'\n')

if __name__=='__main__':main()
