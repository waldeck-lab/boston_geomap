"""Import cited ecology review and coarse geography without changing occurrences."""
import argparse,hashlib,json,re,sqlite3,sys
from datetime import datetime,timezone
from html.parser import HTMLParser
from pathlib import Path
import requests
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from geomap.watch_store import SCHEMA,profile
class Text(HTMLParser):
    def __init__(self):super().__init__();self.parts=[]
    def handle_data(self,data):self.parts.append(data)
def sha(raw):return hashlib.sha256(raw).hexdigest()
ap=argparse.ArgumentParser();ap.add_argument('--stage',type=Path,required=True);ap.add_argument('--review',type=Path,required=True);args=ap.parse_args()
stage=args.stage.resolve()
if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-p201-'):raise RuntimeError('Separate p201 beta stage required')
review=json.loads(args.review.read_text());export=json.loads((stage/'pilot-export.json').read_text())
source_index=json.loads((stage/'research/source-index.json').read_text());cache=stage/'research/source'
lookup={(x['scientific_name'],x['source']):x for x in source_index if x['status']==200}
extra={}
for sid,item in review.get('additional_sources',{}).items():
    path=cache/item['filename']
    if item.get('snapshot_policy')=='web-reviewed-without-local-copy' and not path.exists():
        extra[sid]={k:v for k,v in item.items() if k!='filename'}|{'sha256':None,'snapshot_available':False,'checked_at':review['checked_at'],'review_scope':'Primary fulltext reviewed via web; source page recorded'}
        continue
    if not path.exists():
        response=requests.get(item['url'],timeout=25);response.raise_for_status();path.write_bytes(response.content)
    extra[sid]={k:v for k,v in item.items() if k!='filename'}|{'sha256':sha(path.read_bytes()),'checked_at':review['checked_at'],'review_scope':'source reviewed; foreign evidence'}
def source(name,sid):
    if sid in extra:return extra[sid]
    row=lookup[(name,sid)]
    return {'title':('NRM: ' if sid=='nrm' else 'Lepiforum: ')+name,'url':row['url'],'sha256':row['sha256'],'checked_at':review['checked_at'],'review_scope':'source reviewed; foreign evidence'}
db=sqlite3.connect(stage/'future-species.sqlite');db.row_factory=sqlite3.Row;db.executescript(SCHEMA)
before=tuple(db.execute('select count(*),max(id) from occurrences').fetchone())
rows_by_name={p['scientific_name']:p for p in review['profiles']}
country_names={'Deutschland':('DE','Tyskland'),'Norwegen':('NO','Norge'),'Dänemark':('DK','Danmark'),'Finnland':('FI','Finland'),'Estland':('EE','Estland'),'Lettland':('LV','Lettland'),'Litauen':('LT','Litauen'),'Polen':('PL','Polen'),'Großbritannien':('GB','Storbritannien'),'Niederlande':('NL','Nederländerna'),'Belgien':('BE','Belgien'),'Frankreich':('FR','Frankrike'),'Island':('IS','Island')}
annotations=0;fact_count=0
with db:
    for old in export['profiles']+export.get('references',[]):
        name=old['scientific_name'];uid=old['id'];entry=rows_by_name.get(name,{})
        facts=[]
        for item in entry.get('facts',[]):
            if item['category'] not in {'habitat','hosts','search','distribution'}:raise ValueError('Invalid fact category')
            facts.append(item|{'status':item.get('status','documented'),'sources':[source(name,item.get('source','lepiforum'))],'checked_at':review['checked_at']})
        current_review={'version':review['version'],'checked_at':review['checked_at'],'scope':review['scope'],'facts':facts,'source_pages_reviewed':[source(name,label) for label in ['lepiforum','nrm'] if (name,label) in lookup],'unresolved_policy':'No inferred host or Swedish habitat is treated as documented.'}
        db.execute('insert or replace into knowledge_reviews values(?,?,?)',(uid,review['version'],json.dumps(current_review,ensure_ascii=False)))
        db.execute('delete from geographic_annotations where candidate_id=?',(uid,))
        lepi=lookup.get((name,'lepiforum'))
        if lepi:
            html=(cache/(name.lower().replace(' ','-')+'.lepiforum.html')).read_text()
            for german,(code,label) in country_names.items():
                evidence=None
                for raw in re.findall(r'<tr\b.*?</tr>',html,re.I|re.S):
                    parser=Text();parser.feed(raw);text=re.sub(r'\s+',' ',' '.join(parser.parts)).strip()
                    if re.match(re.escape(german)+r'(?:\s|\()',text) and 'Vorkommen' in text and not re.search(r'fraglich|ausgestorben|verschollen|Fehlmeldung|nicht bestätigt',text,re.I):
                        evidence=text;break
                if evidence:
                    note={'id':f'country-{uid}-{code}','precision':'country','country_code':code,'label':label,'event_year':None,'temporal_status':'undated','claim':'Landsöversikten anger förekomst. Uppgiften fastställer inte en exakt fyndplats, aktuell utbredningsgräns eller att hela landet är bebott.','sources':[source(name,'lepiforum')],'source_row_sha256':sha(evidence.encode()),'checked_at':review['checked_at'],'cartographic_role':'geographic_reference_only'}
                    db.execute('insert into geographic_annotations values(?,?,?)',(note['id'],uid,json.dumps(note,ensure_ascii=False)));annotations+=1
        # The point is a label anchor for a named locality/area, never a specimen coordinate.
        local={
            'Coleophora algidella':dict(key='stigen',precision='region',country_code='NO',label='Stigen – nedre / Aurlandsfjorden',anchor=[7.13,60.99],event_year=2022,sid='norway-report',claim='Rapporten redovisar arten vid Stigen – nedre efter fältarbete 30 juni 2022. Markeringen är en ungefärlig kartetikett för området, inte fyndets koordinat.'),
            'Coleophora polonicella':dict(key='uzava',precision='city',country_code='LV',label='Trakten av Užava',anchor=[21.42,57.23],event_year=2011,sid='lepiforum',claim='Publicerat belägg från kustdyner i trakten av Užava, med larver på Astragalus arenarius och ljusfynd 9 juli 2011. Ankaret är en symbolisk ortsetikett, inte fyndkoordinat.'),
            'Cosmopterix sibirica':dict(key='imatra',precision='city',country_code='FI',label='Imatra / Kurkisuo',anchor=[28.80,61.18],event_year=2020,sid='lepiforum',claim='Lepiforum beskriver larvminor i kvickrot och ett daterat habitatfoto från Kurkisuo vid Imatra 17 juni 2020. Markeringen anger trakten; den är inte en insamlad fyndkoordinat.')
        }.get(name)
        if local:
            key=local.pop('key');sid=local.pop('sid');note=local|{'id':f'locality-{uid}-{key}','temporal_status':'dated-locality-reference','anchor_role':'editorial_label_anchor_not_observation','cartographic_role':'geographic_reference_only','sources':[source(name,sid)],'checked_at':review['checked_at']}
            db.execute('insert into geographic_annotations values(?,?,?)',(note['id'],uid,json.dumps(note,ensure_ascii=False)));annotations+=1
        fact_count+=len(facts)
    assert tuple(db.execute('select count(*),max(id) from occurrences').fetchone())==before
    assert not db.execute('pragma foreign_key_check').fetchall()
    for field in ['profiles','references']:
        for old in export.get(field,[]):
            fresh=profile(db,old['id'])
            # Keep the already reviewed taxonomy/scope; update only imported knowledge fields.
            for key in ['knowledge_review','geographic_notes']:old[key]=fresh[key]
    export['generated_at']=datetime.now(timezone.utc).isoformat();export['beta_version']='p2.0.1';export['knowledge_review_version']=review['version']
assert db.execute('pragma integrity_check').fetchone()[0]=='ok'
(stage/'pilot-export.json').write_text(json.dumps(export,ensure_ascii=False,indent=2)+'\n')
report={'profiles_reviewed':len(export['profiles'])+len(export.get('references',[])),'profiles_with_new_facts':len(rows_by_name),'new_facts':fact_count,'geographic_annotations':annotations,'occurrence_count_unchanged':before[0],'country_annotations_are_not_observations':True,'checked_at':review['checked_at']}
(stage/'research/enrichment-report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
