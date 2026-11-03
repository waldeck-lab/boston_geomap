"""Independent, source-attributed knowledge database for the future-species pilot."""
import csv
import hashlib
import io
import json
import math
import re
import sqlite3
import unicodedata
import zipfile
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit
from .watch_scope import get_scope

SCHEMA='''
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_versions(version INTEGER PRIMARY KEY,description TEXT NOT NULL);
INSERT OR IGNORE INTO schema_versions VALUES(1,'Independent future-species knowledge store');
CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY,title TEXT NOT NULL,url TEXT NOT NULL,license TEXT NOT NULL,version TEXT NOT NULL,sha256 TEXT NOT NULL,retrieved_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS import_runs(id INTEGER PRIMARY KEY,started_at TEXT DEFAULT CURRENT_TIMESTAMP,completed_at TEXT,status TEXT NOT NULL,report_json TEXT);
CREATE TABLE IF NOT EXISTS candidates(id INTEGER PRIMARY KEY,scientific_name TEXT NOT NULL UNIQUE,role TEXT NOT NULL,swedish_status TEXT NOT NULL DEFAULT 'unknown',review_status TEXT NOT NULL DEFAULT 'unreviewed',review_notes TEXT,dyntaxa_id INTEGER,gbif_key INTEGER);
CREATE TABLE IF NOT EXISTS candidate_assessments(candidate_id INTEGER PRIMARY KEY REFERENCES candidates(id),classification TEXT NOT NULL,checked_at TEXT NOT NULL,rationale TEXT NOT NULL,evidence_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS search_guides(candidate_id INTEGER PRIMARY KEY REFERENCES candidates(id),version TEXT NOT NULL,guide_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS vernacular_names(candidate_id INTEGER PRIMARY KEY REFERENCES candidates(id),name TEXT NOT NULL,language TEXT NOT NULL CHECK(language='sv'),source_id TEXT NOT NULL REFERENCES sources(id),source_taxon_name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS expansion_assessments(candidate_id INTEGER PRIMARY KEY REFERENCES candidates(id),version TEXT NOT NULL,assessment_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS species_supplements(candidate_id INTEGER PRIMARY KEY REFERENCES candidates(id),version TEXT NOT NULL,supplement_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS knowledge_reviews(candidate_id INTEGER PRIMARY KEY REFERENCES candidates(id),version TEXT NOT NULL,review_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS geographic_annotations(id TEXT PRIMARY KEY,candidate_id INTEGER NOT NULL REFERENCES candidates(id),note_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS geographic_annotations_candidate ON geographic_annotations(candidate_id);
INSERT OR IGNORE INTO schema_versions VALUES(6,'Separate ecology reviews and coarse geographic references, never observation points');
INSERT OR IGNORE INTO schema_versions VALUES(5,'Separate cited habitat, host, search and locality supplements');
INSERT OR IGNORE INTO schema_versions VALUES(4,'Cited range-expansion assessments and evidence-based interest flag');
INSERT OR IGNORE INTO schema_versions VALUES(3,'Cited search guidance separate from Swedish habitat hypotheses');
INSERT OR IGNORE INTO schema_versions VALUES(2,'Source-screened candidate assessments and separate Swedish references');
CREATE TABLE IF NOT EXISTS host_associations(candidate_id INTEGER NOT NULL REFERENCES candidates(id),plant_name TEXT NOT NULL,plant_rank TEXT NOT NULL,is_primary INTEGER CHECK(is_primary IN (0,1) OR is_primary IS NULL),region TEXT NOT NULL,source_id TEXT NOT NULL REFERENCES sources(id),source_field TEXT NOT NULL,review_status TEXT NOT NULL DEFAULT 'unreviewed',PRIMARY KEY(candidate_id,plant_name,region,source_id));
CREATE TABLE IF NOT EXISTS habitat_requirements(candidate_id INTEGER NOT NULL REFERENCES candidates(id),property TEXT NOT NULL,value TEXT NOT NULL,is_primary INTEGER CHECK(is_primary IN (0,1) OR is_primary IS NULL),region TEXT NOT NULL,source_id TEXT NOT NULL REFERENCES sources(id),source_field TEXT NOT NULL,review_status TEXT NOT NULL DEFAULT 'unreviewed',PRIMARY KEY(candidate_id,property,region,source_id));
CREATE TABLE IF NOT EXISTS phenology(candidate_id INTEGER NOT NULL REFERENCES candidates(id),life_stage TEXT NOT NULL,month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),region TEXT NOT NULL,evidence_type TEXT NOT NULL,source_id TEXT NOT NULL REFERENCES sources(id),source_field TEXT NOT NULL,PRIMARY KEY(candidate_id,life_stage,month,region,source_id));
CREATE TABLE IF NOT EXISTS ecological_requirements(id INTEGER PRIMARY KEY,candidate_id INTEGER NOT NULL REFERENCES candidates(id),property TEXT NOT NULL,value TEXT NOT NULL,unit TEXT,region TEXT NOT NULL,source_id TEXT NOT NULL REFERENCES sources(id),source_field TEXT NOT NULL,review_status TEXT NOT NULL DEFAULT 'unreviewed');
CREATE TABLE IF NOT EXISTS occurrences(id INTEGER PRIMARY KEY,occurrence_uid TEXT NOT NULL UNIQUE,candidate_id INTEGER NOT NULL REFERENCES candidates(id),country TEXT NOT NULL CHECK(country<>'SE'),latitude REAL NOT NULL,longitude REAL NOT NULL,uncertainty_m REAL,event_date TEXT NOT NULL,year INTEGER NOT NULL,month INTEGER CHECK(month BETWEEN 1 AND 12),life_stage TEXT NOT NULL,basis_of_record TEXT,license TEXT NOT NULL,source_id TEXT NOT NULL REFERENCES sources(id),quality_flags TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS occurrence_records(provider TEXT NOT NULL,record_id TEXT NOT NULL,occurrence_id INTEGER NOT NULL REFERENCES occurrences(id),dataset_key TEXT NOT NULL,original_id TEXT,PRIMARY KEY(provider,record_id));
CREATE INDEX IF NOT EXISTS watch_occurrences_taxon ON occurrences(candidate_id,month,life_stage);
CREATE INDEX IF NOT EXISTS watch_occurrences_position ON occurrences(longitude,latitude);
CREATE TABLE IF NOT EXISTS source_traits(candidate_id INTEGER NOT NULL REFERENCES candidates(id),source_id TEXT NOT NULL REFERENCES sources(id),values_json TEXT NOT NULL,PRIMARY KEY(candidate_id,source_id));
'''
MONTHS={'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'july':7,'aug':8,'agu':8,'sep':9,'oct':10,'nov':11,'dec':12}
FRONTS={'DK','DE','PL','LT','LV','EE'}
def normalized(value):return ' '.join(unicodedata.normalize('NFKC',value).split())
def connect(path,readonly=False):
    db=sqlite3.connect(Path(path).resolve().as_uri()+('?mode=ro' if readonly else '?mode=rwc'),uri=True)
    db.row_factory=sqlite3.Row;db.execute('PRAGMA foreign_keys=ON');return db
def initialize(path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    db=connect(path)
    with db:db.executescript(SCHEMA)
    db.close()
def source(db,identifier,title,url,license,version,path):
    with Path(path).open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
    existing=db.execute('SELECT sha256 FROM sources WHERE id=?',(identifier,)).fetchone()
    if existing and existing[0]!=digest:raise ValueError('Changed source requires a new version ID: '+identifier)
    db.execute('INSERT OR IGNORE INTO sources(id,title,url,license,version,sha256) VALUES(?,?,?,?,?,?)',(identifier,title,url,license,version,digest))
def import_matrix(db,path,candidate_ids,source_id):
    rows_seen=0;matched=set();duplicates=0;missing_values=0
    with Path(path).open(encoding='utf-8-sig',newline='') as stream:
        reader=csv.reader(stream,delimiter=';');header=next(reader)
        if header[0]!='SPECIES' or len(header)<2:raise ValueError('Unexpected interaction matrix header')
        plants=[normalized(v) for v in header[1:]]
        for row in reader:
            if len(row)!=len(header) or any(v not in {'0','1',''} for v in row[1:]):raise ValueError('Malformed interaction matrix')
            missing_values+=row[1:].count('')
            rows_seen+=1;name=normalized(row[0]);uid=candidate_ids.get(name)
            if uid is None:continue
            duplicates+=int(name in matched);matched.add(name)
            for plant,value in zip(plants,row[1:]):
                if value=='1':db.execute('INSERT OR IGNORE INTO host_associations(candidate_id,plant_name,plant_rank,is_primary,region,source_id,source_field) VALUES(?,?,?,?,?,?,?)',
                    (uid,plant,'genus' if re.search(r'\bspp\.?$',plant) else 'source-name',None,'Europe; population relevance unreviewed',source_id,'matrix:'+row[0]))
    return dict(rows=rows_seen,plant_columns=len(plants),matched_species=len(matched),duplicate_pilot_names=duplicates,missing_cells=missing_values,missing=sorted(set(candidate_ids)-matched))
def import_traits(db,path,candidate_ids,source_id):
    with zipfile.ZipFile(path) as archive:
        rows=csv.reader(io.StringIO(archive.read('data/ecological_traits_2024.csv').decode('cp1252')))
        groups=next(rows);keys=[k.strip() for k in next(rows)]
        index=keys.index('scientific_name');matched=[]
        for row in rows:
            if len(row)!=len(keys):raise ValueError('Unexpected trait row width')
            name=normalized(row[index]);uid=candidate_ids.get(name)
            if uid is None:continue
            matched.append(name);raw={group+':'+key:value for group,key,value in zip(groups,keys,row) if value.strip()}
            db.execute('INSERT OR REPLACE INTO source_traits VALUES(?,?,?)',(uid,source_id,json.dumps(raw,ensure_ascii=False)))
            for group,key,value in zip(groups,keys,row):
                if group in {'egg_stage','larval_stage','pupal_stage','adult_stage'} and value=='1':
                    month=MONTHS.get(key.rsplit('_',1)[-1])
                    if month:db.execute('INSERT OR IGNORE INTO phenology VALUES(?,?,?,?,?,?,?)',(uid,{'egg_stage':'egg','larval_stage':'larva','pupal_stage':'pupa','adult_stage':'adult'}[group],month,'Great Britain and Ireland','published trait compilation',source_id,key))
                if group=='habitat' and value and value not in {'0','NA'}:
                    db.execute('INSERT OR IGNORE INTO habitat_requirements(candidate_id,property,value,is_primary,region,source_id,source_field) VALUES(?,?,?,?,?,?,?)',(uid,key,value,None,'Great Britain and Ireland',source_id,key))
                if group=='common_hostplants' and value in {'1','2','3','4'}:
                    db.execute('INSERT OR IGNORE INTO host_associations(candidate_id,plant_name,plant_rank,is_primary,region,source_id,source_field) VALUES(?,?,?,?,?,?,?)',(uid,key,'source-label',1 if value=='1' else 0,'Great Britain and Ireland',source_id,key+'='+value))
                if group in {'overwintering_stage','voltinism','photoperiod','larval_habitat','pupal_habit','host_plant_section'} and value.strip() and value not in {'0','NA','-'}:
                    prop=group+':'+key;region='Great Britain and Ireland'
                    if not db.execute('SELECT 1 FROM ecological_requirements WHERE candidate_id=? AND property=? AND value=? AND region=? AND source_id=?',(uid,prop,value,region,source_id)).fetchone():
                        db.execute('INSERT INTO ecological_requirements(candidate_id,property,value,region,source_id,source_field) VALUES(?,?,?,?,?,?)',(uid,prop,value,region,source_id,key))
    return dict(matched_species=len(matched),matched_names=matched,missing=sorted(set(candidate_ids)-set(matched)))
def import_occurrences(db,records,uid,source_id,scope=None):
    geography=get_scope(scope);low,high=geography['latitude_range']
    accepted=0;rejected=0
    for r in records:
        license=r.get('license') or '';country=r.get('countryCode');stamp=str(r.get('eventDate',''))[:10]
        parsed=urlsplit(license)
        if parsed.hostname!='creativecommons.org' or not(parsed.path.startswith('/licenses/by/4.0/') or parsed.path.startswith('/publicdomain/zero/1.0/')):
            rejected+=1;continue
        try:
            day=date.fromisoformat(stamp);lat=float(r['decimalLatitude']);lon=float(r['decimalLongitude'])
            if country not in geography['countries'] or not(low<=lat<=high and -180<=lon<=180) or not(date(2000,1,1)<=day<=date(2026,11,2)) or not math.isfinite(lat+lon):raise ValueError()
            if r.get('informationWithheld') or r.get('dataGeneralizations') or r.get('occurrenceStatus')=='ABSENT':raise ValueError()
            uncertainty=r.get('coordinateUncertaintyInMeters')
            if uncertainty is not None and (not math.isfinite(float(uncertainty)) or float(uncertainty)<0):raise ValueError()
        except (KeyError,ValueError,TypeError):rejected+=1;continue
        original=str(r.get('occurrenceID') or '');record=str(r['key'])
        identity=original if original.startswith(('https://','http://')) else str(r['datasetKey'])+':'+(original or record)
        db.execute('''INSERT OR IGNORE INTO occurrences(occurrence_uid,candidate_id,country,latitude,longitude,uncertainty_m,event_date,year,month,life_stage,basis_of_record,license,source_id,quality_flags) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (identity,uid,country,lat,lon,r.get('coordinateUncertaintyInMeters'),stamp,day.year,day.month,r.get('lifeStage') or 'unknown',r.get('basisOfRecord'),license,source_id,json.dumps(r.get('issues',[]))))
        occurrence=db.execute('SELECT id,candidate_id FROM occurrences WHERE occurrence_uid=?',(identity,)).fetchone()
        if occurrence['candidate_id']!=uid:rejected+=1;continue
        db.execute('INSERT OR IGNORE INTO occurrence_records VALUES(?,?,?,?,?)',('GBIF',record,occurrence['id'],r['datasetKey'],original));accepted+=1
    return dict(accepted_records=accepted,rejected_records=rejected)
def import_species_supplements(db,path):
    data=json.loads(Path(path).read_text(encoding='utf-8'));prepared=[]
    for p in data['profiles']:
        row=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(p['scientific_name'],)).fetchone()
        if not row:raise ValueError('Unknown supplement species')
        url=urlsplit(p['source_url'])
        if url.scheme!='https' or url.hostname!='lepiforum.org' or not url.path.startswith('/wiki/page/'):raise ValueError('Invalid supplement source')
        if p['summary_status'] not in {'editorial_summary','section_inventory_only'}:raise ValueError('Invalid summary status')
        if set(p['notes'])!={'habitat','hosts','search','locations','uncertainties'}:raise ValueError('Supplement categories must remain separate')
        if any(not isinstance(v,list) or any(not isinstance(x,str) for x in v) for v in p['notes'].values()):raise ValueError('Only textual source notes; no observation import')
        if p['summary_status']=='section_inventory_only' and any(p['notes'].values()):raise ValueError('Inventory is not a reviewed biological summary')
        for record in p.get('additional_records',[]):
            if record.get('country')=='SE' or 'latitude' in record or 'longitude' in record:raise ValueError('Literature examples must remain foreign text records, not map coordinates')
            date.fromisoformat(record['event_date'])
            u=urlsplit(record['source_url'])
            if u.scheme!='https' or not u.hostname or not record['source_title'] or not record['locality']:raise ValueError('Literature example requires a source and locality')
            placement=record.get('map_location')
            if placement:
                if placement.get('approximate') is not True or placement.get('precision')!='island' or placement.get('placement_method')!='user_requested_island_center':raise ValueError('Approximate island placement must be explicitly labelled')
                lat=float(placement['latitude']);lon=float(placement['longitude'])
                if not math.isfinite(lat+lon) or not(-90<=lat<=90 and -180<=lon<=180):raise ValueError('Invalid island placement')
        prepared.append((row['id'],data['version'],json.dumps(p,ensure_ascii=False)))
    db.executemany('INSERT OR REPLACE INTO species_supplements VALUES(?,?,?)',prepared)
    return len(prepared)

def import_expansion_assessments(db,path):
    data=json.loads(Path(path).read_text(encoding='utf-8'));prepared=[]
    for a in data['assessments']:
        row=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(a['scientific_name'],)).fetchone()
        if not row:continue
        if type(a['high_interest']) is not bool:raise ValueError('Interest must be an explicit boolean')
        if a['high_interest'] and (a['status']!='documented_range_expansion' or not a['sources'] or not a['direction'] or not a['period']):raise ValueError('High interest requires dated expansion evidence')
        if any(urlsplit(s['url']).scheme!='https' or not urlsplit(s['url']).hostname for s in a['sources']):raise ValueError('Invalid expansion source URL')
        prepared.append((row['id'],data['version'],json.dumps(a,ensure_ascii=False)))
    db.executemany('INSERT OR REPLACE INTO expansion_assessments VALUES(?,?,?)',prepared)
    return len(prepared)

def import_swedish_names(db,path,source_id):
    taxa={normalized(r['scientific_name']):r for r in json.loads(Path(path).read_text(encoding='utf-8'))['species']}
    # Explicit nomenclatural variants only; never fuzzy-match an epithet or genus.
    aliases={'Proserpinus proserpina':'Proserpinus proserpinus','Phyllonorycter messaniella':'Phyllonorycter messaniellus','Phyllonorycter leucographella':'Phyllonorycter leucographellus'}
    for r in db.execute('SELECT id,scientific_name FROM candidates').fetchall():
        taxon=taxa.get(aliases.get(r['scientific_name'],r['scientific_name']))
        if taxon and taxon.get('swedish_name','').strip():
            db.execute('INSERT OR REPLACE INTO vernacular_names VALUES(?,?,?,?,?)',(r['id'],taxon['swedish_name'].strip(),'sv',source_id,taxon['scientific_name']))

def import_search_guides(db,path):
    data=json.loads(Path(path).read_text(encoding='utf-8'))
    prepared=[]
    for guide in data['guides']:
        candidate=db.execute('SELECT id FROM candidates WHERE scientific_name=?',(guide['scientific_name'],)).fetchone()
        if not candidate:raise ValueError('Unknown guide species: '+guide['scientific_name'])
        sources={s['id']:s for s in guide['sources']}
        def valid_source(url):
            u=urlsplit(url)
            # NRM's legacy reference site serves HTTP only. Never invent an HTTPS URL.
            legacy_nrm=(u.scheme=='http' and u.netloc=='www3.nrm.se' and u.path.startswith('/en/svenska_fjarilar/'))
            return bool(u.hostname and (u.scheme=='https' or legacy_nrm))
        if any(not valid_source(s['url']) for s in sources.values()):raise ValueError('Invalid guide source URL')
        for field,kind in [('documented','documented'),('swedish_hypotheses','hypothesis')]:
            for item in guide[field]:
                if item['kind']!=kind or not item['source_ids'] or any(s not in sources for s in item['source_ids']):raise ValueError('Unlabelled hypothesis or missing source')
                if kind=='documented' and item['region'].startswith('Sverige'):raise ValueError('Foreign guidance cannot establish Swedish habitat')
        prepared.append((candidate['id'],data['version'],json.dumps(guide,ensure_ascii=False)))
    db.executemany('INSERT OR REPLACE INTO search_guides VALUES(?,?,?)',prepared)
    return len(prepared)

def profile(db,uid):
    row=db.execute('SELECT * FROM candidates WHERE id=?',(uid,)).fetchone()
    if not row:return None
    out=dict(row)
    supplement=db.execute('SELECT supplement_json FROM species_supplements WHERE candidate_id=?',(uid,)).fetchone()
    out['supplement']=json.loads(supplement[0]) if supplement else None
    knowledge=db.execute('SELECT review_json FROM knowledge_reviews WHERE candidate_id=?',(uid,)).fetchone()
    out['knowledge_review']=json.loads(knowledge[0]) if knowledge else None
    out['geographic_notes']=[json.loads(r[0]) for r in db.execute('SELECT note_json FROM geographic_annotations WHERE candidate_id=? ORDER BY id',(uid,))]
    expansion=db.execute('SELECT assessment_json FROM expansion_assessments WHERE candidate_id=?',(uid,)).fetchone()
    out['expansion']=json.loads(expansion[0]) if expansion else None
    vernacular=db.execute('SELECT name,source_id,source_taxon_name FROM vernacular_names WHERE candidate_id=?',(uid,)).fetchone()
    out['swedish_name']=vernacular['name'] if vernacular else None
    out['swedish_name_source']=dict(vernacular) if vernacular else None
    guide=db.execute('SELECT guide_json FROM search_guides WHERE candidate_id=?',(uid,)).fetchone()
    out['search_guide']=json.loads(guide[0]) if guide else None
    assessment=db.execute('SELECT * FROM candidate_assessments WHERE candidate_id=?',(uid,)).fetchone()
    out['assessment']=dict(assessment) if assessment else None
    if out['assessment']:out['assessment']['evidence']=json.loads(out['assessment'].pop('evidence_json'))
    for field,table in [('hosts','host_associations'),('habitats','habitat_requirements'),('phenology','phenology'),('other_requirements','ecological_requirements')]:
        out[field]=[dict(r) for r in db.execute('SELECT * FROM '+table+' WHERE candidate_id=?',(uid,))]
    out['observed_months']=[dict(r) for r in db.execute('SELECT country,life_stage,month,count(*) AS n FROM occurrences WHERE candidate_id=? GROUP BY country,life_stage,month ORDER BY month',(uid,))]
    out['occurrences']=[dict(r) for r in db.execute('SELECT o.*,group_concat(r.record_id) AS gbif_keys FROM occurrences o JOIN occurrence_records r ON r.occurrence_id=o.id WHERE o.candidate_id=? GROUP BY o.id ORDER BY o.event_date DESC LIMIT 300',(uid,))]
    out['gaps']=[label for label,missing in [('Svensk status och expansionsbedömning',out['review_status']!='reviewed'),('Huvudsaklig värdväxt för Sverige',not any(h['is_primary']==1 and h['region']=='Sweden' for h in out['hosts'])),('Primär biotop',not any(h['is_primary']==1 for h in out['habitats'])),('Publicerad fenologi',not out['phenology']),('Övriga ekologiska krav',not out['other_requirements'])] if missing]
    return out
def inventory(db):
    result=[]
    for r in db.execute('SELECT id,scientific_name,role,swedish_status,review_status,dyntaxa_id,gbif_key FROM candidates ORDER BY id'):
        row=dict(r)
        for key,table in [('host_count','host_associations'),('habitat_count','habitat_requirements'),('phenology_count','phenology'),('occurrence_count','occurrences')]:row[key]=db.execute('SELECT count(*) FROM '+table+' WHERE candidate_id=?',(r['id'],)).fetchone()[0]
        result.append(row)
    return result
