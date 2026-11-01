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

SCHEMA='''
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_versions(version INTEGER PRIMARY KEY,description TEXT NOT NULL);
INSERT OR IGNORE INTO schema_versions VALUES(1,'Independent future-species knowledge store');
CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY,title TEXT NOT NULL,url TEXT NOT NULL,license TEXT NOT NULL,version TEXT NOT NULL,sha256 TEXT NOT NULL,retrieved_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS import_runs(id INTEGER PRIMARY KEY,started_at TEXT DEFAULT CURRENT_TIMESTAMP,completed_at TEXT,status TEXT NOT NULL,report_json TEXT);
CREATE TABLE IF NOT EXISTS candidates(id INTEGER PRIMARY KEY,scientific_name TEXT NOT NULL UNIQUE,role TEXT NOT NULL,swedish_status TEXT NOT NULL DEFAULT 'unknown',review_status TEXT NOT NULL DEFAULT 'unreviewed',review_notes TEXT,dyntaxa_id INTEGER,gbif_key INTEGER);
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
def import_occurrences(db,records,uid,source_id):
    accepted=0;rejected=0
    for r in records:
        license=r.get('license') or '';country=r.get('countryCode');stamp=str(r.get('eventDate',''))[:10]
        parsed=urlsplit(license)
        if parsed.hostname!='creativecommons.org' or not(parsed.path.startswith('/licenses/by/4.0/') or parsed.path.startswith('/publicdomain/zero/1.0/')):
            rejected+=1;continue
        try:
            day=date.fromisoformat(stamp);lat=float(r['decimalLatitude']);lon=float(r['decimalLongitude'])
            if country not in FRONTS or not(53<=lat<=60 and -180<=lon<=180) or not(date(2000,1,1)<=day<=date(2026,11,1)) or not math.isfinite(lat+lon):raise ValueError()
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
def profile(db,uid):
    row=db.execute('SELECT * FROM candidates WHERE id=?',(uid,)).fetchone()
    if not row:return None
    out=dict(row)
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
