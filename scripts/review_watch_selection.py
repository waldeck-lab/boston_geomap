#!/usr/bin/env python3
"""Screen candidates using country checklists, later Swedish records and public GBIF counts.

Not listed is a bounded evidence statement, never proof of national absence.
"""
import argparse
import csv
import hashlib
import json
import re
import subprocess
import time
import sqlite3
from pathlib import Path
import requests

URLS={
 'checklist-2017.pdf':'https://www.entomologiskforening.no/wp-content/uploads/2025/06/Aarvik_et_al_2017_Nordic-Baltic_Checklist_of_Lepidiptera.pdf',
 'update-2021.pdf':'https://www.entomologiskforening.no/wp-content/uploads/2025/06/Aarvik-et-al-2021.pdf',
 'update-2025.pdf':'https://www.entomologiskforening.no/wp-content/uploads/2025/08/nje-vol72-no1-94-107.pdf',
 'bab-2025.html':'https://et-online.nu/index.php/contents/article/view/234',
 'bab-2024.html':'https://et-online.nu/index.php/contents/article/view/206',
 'macro-2024.html':'https://et-online.nu/index.php/contents/article/view/211',
 'bab-2022.pdf':'https://et-online.nu/index.php/contents/article/download/129/135/365'}
# Positive Swedish records explicitly enumerated in the open original 2025 abstract.
BAB_2025={'Eriocrania marci','Caloptilia honoratella','Phyllonorycter issikii','Phyllonorycter mespilella','Phyllonorycter mespilellus','Phyllonorycter medicaginella','Phyllonorycter medicaginellus','Phyllocnistis xenia','Choreutis nemorana','Aproaerema cinctelloides','Aproaerema vinella','Agonopterix putridella','Endothenia ustulana','Epiblema turbidana'}
PRIORITY=['Theria primaria','Brenthis daphne','Caradrina kadenii','Clepsis dumicolana','Lithophane leautieri','Eudonia angustea','Carcharodus alceae','Pyronia tithonus','Melanargia galathea','Lycaena dispar','Lycaena tityrus','Euplagia quadripunctaria','Eilema caniola','Aporophyla australis','Phaiogramma etruscaria','Aleucis distinctata','Cerura erminea','Horisme radicaria','Eucarta amethystina','Omphaloscelis lunosa','Aporophyla nigra','Eccopisa effractella','Gymnancyla hornigii','Udea costalis','Parectopa robiniella','Macrosaccus robiniella','Stigmella rhamnella','Trifurcula beirnei','Trifurcula squamatella','Dialectica imperialella','Phyllonorycter messaniella','Phyllonorycter scopariella','Stigmella stettinensis','Isotrias rectifasciana','Rhagades pruni','Zygaena trifolii','Metalampra italica','Batia lambdella','Agonopterix umbellana','Monochroa divisella','Mirificarma interrupta','Gelechia senticetella','Scrobipalpa costella','Scrobipalpa ocellatella','Scrobipalpa instabilella','Xenolechia aethiops','Blastobasis phycidella','Endotricha flammealis','Idaea degeneraria','Idaea rusticata','Idaea subsericeata','Scopula imitaria','Scopula emutaria','Horisme corticata','Stegania trimaculata','Gastropacha populifolia','Odonestis pruni','Proserpinus proserpina','Drymonia obliterata','Ptilodon cucullina']
def front_params(taxon,mappable=False):
    """Biological presence is independent of permission to republish map points."""
    params={'taxonKey':taxon,'country':['DK','DE','PL','LT','LV','EE'],'decimalLatitude':'53,60','eventDate':'2015-01-01,2026-11-01','occurrenceStatus':'PRESENT','hasCoordinate':'true','hasGeospatialIssue':'false','limit':0}
    if mappable:params['license']=['CC0_1_0','CC_BY_4_0']
    return params

def validate_watch_assessment(name,assessment):
    """Presence may be supported by a checked publication, independently of GBIF."""
    from urllib.parse import urlsplit
    from datetime import date
    if 'S' in assessment['checklist']['countries'] or assessment.get('gbif_sweden_count')!=0:
        raise ValueError('Swedish record or unreviewed Swedish status: '+name)
    if assessment.get('front_2015_2026_count',0)>=1:
        return
    from geomap.watch_scope import get_scope
    scope=get_scope(assessment.get('occurrence_scope'))
    checked=date.fromisoformat(assessment['checked_at'])
    for evidence in assessment.get('published_front_evidence',[]):
        url=urlsplit(evidence.get('url',''))
        if (evidence.get('scientific_name')==name and evidence.get('review_status')=='primary_source_checked'
            and evidence.get('country') in scope['countries']
            and 2015<=evidence.get('event_year',0)<=checked.year
            and re.fullmatch('[a-f0-9]{64}',evidence.get('source_sha256',''))
            and url.scheme=='https' and url.hostname and evidence.get('document_page')):
            return
    raise ValueError('Missing reviewed front evidence: '+name)

def baseline(text,validate=True,accepted_genera=None):
    if accepted_genera is None and validate:
        accepted_genera=json.loads((Path(__file__).resolve().parents[1]/'data/watch/checklist-2017-accepted-genera.json').read_text())['genera']
    genus=None;rows={}
    for line in text.splitlines():
        g=re.match(r'^\s*(\d+)[a-z*]*\s+([A-Z][a-z]+)\b',line)
        if g and not g[2].endswith(('idae','inae','oidea')):
            if accepted_genera is not None:
                if accepted_genera.get(g[1])==g[2]:genus=g[2]
            elif re.match(r'^\s*\d+[a-z*]*\s+[A-Z][a-z]+\s+[A-Z(]',line):genus=g[2]
        m=re.match(r'^\s*(\d+)[a-z*]*\s+([a-z][a-z-]+)\s+(.+?)\s{2,}((?:(?:I|N|S|D|F|E|Lv|Lt)[?*()]*\s*)+)$',line)
        if m and genus:rows[genus+' '+m[2]]=dict(number=m[1],countries=re.findall(r'\b(?:I|N|S|D|F|E|Lv|Lt)\b',m[4]),sources=['checklist-2017.pdf'])
    if validate:
        if len(rows)!=3259 or sum('S' in r['countries'] for r in rows.values())!=2804:raise ValueError('Checklist parsing failed published country/total control counts')
        for name in ['Cameraria ohridella','Phyllonorycter leucographella','Vanessa cardui','Saturnia pavonia']:
            if 'S' not in rows[name]['countries']:raise ValueError('Known Swedish control failed')
        if rows.get('Coleophora algidella',{}).get('countries')!=['I','N'] or rows.get('Coleophora carelica',{}).get('countries')!=['F','E','Lv']:raise ValueError('Accepted genus / synonym control failed')
    return rows
def add_updates(rows,text,label):
    text=text.split('\nNew country records\n',1)[1].split('\nTaxonomic changes',1)[0]
    pattern=r'(?m)^\s*(\d{4}[a-z]?)\.?\s+([A-Z][a-z]+)\s+([a-z][a-z-]+)\b'
    starts=list(re.finditer(pattern,text))
    for i,m in enumerate(starts):
        block=text[m.end():starts[i+1].start() if i+1<len(starts) else len(text)]
        # References and author parentheses cannot contribute country-letter initials.
        block=re.sub(r'\([^()]*\)',' ',block)
        countries=re.findall(r'\b(?:I|N|S|D|F|E|Lv|Lt)\b',block)
        name=m[2]+' '+m[3]
        row=rows.setdefault(name,dict(number=m[1],countries=[],sources=[]))
        row['countries']=sorted(set(row['countries'])|set(countries));row['sources'].append(label)
def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--stage',type=Path,required=True);ap.add_argument('--count',type=int,default=14);args=ap.parse_args();stage=args.stage.resolve()
    if not 10<=args.count<=100:raise ValueError('Pilot count must be 10..100')
    if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise ValueError('Not a pilot stage')
    raw=stage/'source/review';raw.mkdir(exist_ok=True);cache=raw/'gbif';cache.mkdir(exist_ok=True)
    session=requests.Session();session.headers['User-Agent']='WCR-watch-country-review/0.2 (cached bounded queries)'
    for file,url in URLS.items():
        path=raw/file
        if not path.exists():response=session.get(url,timeout=35);response.raise_for_status();path.write_bytes(response.content)
        if file.endswith('.pdf'):subprocess.run(['pdftotext','-layout' if file=='checklist-2017.pdf' else '-raw',str(path),str(path.with_suffix('.txt'))],check=True)
    rows=baseline((raw/'checklist-2017.txt').read_text())
    for label in ['update-2021.pdf','update-2025.pdf']:add_updates(rows,(raw/label).with_suffix('.txt').read_text(),label)
    for name in BAB_2025|{'Luffia lapidella','Catephia alchymista','Acontia candefacta'}:
        row=rows.setdefault(name,dict(number=None,countries=[],sources=[]));row['countries']=sorted(set(row['countries'])|{'S'});row['sources'].append('bab-2025.html' if name in BAB_2025 else 'macro-2024.html')
    matrix=next((stage/'source').glob('Lepidoptera*csv'))
    with matrix.open(encoding='utf-8-sig',newline='') as stream:
        reader=csv.reader(stream,delimiter=';');next(reader);host_names={r[0].strip() for r in reader}
    pool=[name for name,row in rows.items() if 'S' not in row['countries'] and set(row['countries'])&{'D','E','Lv','Lt'} and name in host_names]
    pool=sorted(pool,key=lambda name:(PRIORITY.index(name) if name in PRIORITY else 10000,int(re.sub('[^0-9]','',rows[name]['number']) or 0)))
    assessments={};selected=[]
    def api(endpoint,params):
        url='https://api.gbif.org/v1/'+endpoint;key=hashlib.sha256(json.dumps([url,params],sort_keys=True).encode()).hexdigest();path=cache/(key+'.json')
        if not path.exists():
            response=session.get(url,params=params,timeout=35);response.raise_for_status();path.write_text(json.dumps(dict(url=response.url,data=response.json())));time.sleep(.15)
        return json.loads(path.read_text())
    # Existing pilot entries remain available for audit/reference, never silently deleted.
    db=sqlite3.connect((stage/'future-species.sqlite').as_uri()+'?mode=ro',uri=True)
    old_names=[r[0] for r in db.execute('SELECT scientific_name FROM candidates ORDER BY id')];db.close()
    work=old_names+[n for n in pool if n not in old_names]
    for name in work:
        row=rows.get(name);review=dict(checklist=row,checked_at='2026-11-01',classification='pending',evidence_urls=[URLS[f] for f in row['sources']] if row else [])
        if name=='Macrosaccus robiniella':
            review.update(classification='conflicting_evidence',reason='En senare forskningsartikel uppger Sverige i utbredningskartan, vilket strider mot screeningunderlaget. Fulltexten kunde inte kontrolleras. Utesluten tills konflikten har retts ut.')
            review['evidence_urls'].append('https://hrcak.srce.hr/file/481963');assessments[name]=review;print('CONFLICT',name,flush=True);continue
        if row and 'S' in row['countries']:
            review.update(classification='swedish_record',reason='Svenskt fynd finns i granskad checklista/årsrapport. Inte kandidat för första svenska fyndet.');assessments[name]=review;print('SWEDISH',name,flush=True);continue
        if name not in pool:assessments[name]=review;continue
        match=api('species/match',{'name':name,'strict':'true'})['data']
        if match.get('matchType')!='EXACT' or match.get('rank')!='SPECIES' or match.get('order')!='Lepidoptera':assessments[name]=review;continue
        key=match.get('acceptedUsageKey',match['usageKey'])
        se=api('occurrence/search',{'taxonKey':key,'country':'SE','occurrenceStatus':'PRESENT','limit':0});review['gbif_sweden_count']=se['data']['count'];review['evidence_urls'].append(se['url'])
        if se['data']['count']:
            review.update(classification='swedish_record_to_review',reason='Publika svenska GBIF-belägg finns; utesluten från förstafyndslistan tills de bedömts.');assessments[name]=review;print('SE RECORDS',name,se['data']['count'],flush=True);continue
        front=api('occurrence/search',front_params(key));review['front_2015_2026_count']=front['data']['count'];review['evidence_urls'].append(front['url'])
        if front['data']['count']:
            review.update(classification='watch_candidate',reason='Ej svensk i granskad nordisk-baltisk checklista med uppdateringar och senare öppna BAB-belägg; inga publika positiva SE-träffar i GBIF-kontrollen. Dokumenterade grannlandsfynd finns 2015–2026. Frånvaro och framtida etablering är inte bevisade.');selected.append(name);print('CANDIDATE',len(selected),name,front['data']['count'],flush=True)
        else:review.update(reason='Inget aktuellt fyndunderlag i vald geografisk front; inte prioriterad.');print('NO FRONT',name,flush=True)
        assessments[name]=review
        if len(selected)>=args.count:break
    if len(selected)<10:raise RuntimeError('Too few source-screened candidates; do not pad the list')
    if 'Blastobasis phycidella' in selected:
        selected.remove('Blastobasis phycidella');selected.insert(0,'Blastobasis phycidella')
        review=assessments['Blastobasis phycidella']
        review['reason']='BAB pekar uttryckligen ut arten för framtida eftersök i södra Skåne i rapporten om fyndåret 2022 (ET 2023, sidan 10). '+review['reason']
        review['expert_support']={'url':'https://et-online.nu/index.php/contents/article/view/129','page':10,'publication_year':2023,'type':'published watch suggestion'}
        review['evidence_urls'].append('https://et-online.nu/index.php/contents/article/view/129')
    result=dict(names=selected,assessments=assessments,source_urls=URLS,limitations=['Not listed is not proof of absence.','BAB 2024 and 2025 full texts require subscription; only their open pages/abstracts were accessed.','No published 2026 Swedish annual review is available in this screening.','Range expansion and Swedish establishment suitability require species-level assessment.'])
    (stage/'selection-review.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print('Selected',len(selected),'source-screened watch candidates')
if __name__=='__main__':main()
