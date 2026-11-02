"""Build complete paginated, licensed historical series separately from pilot samples."""
import argparse,calendar,hashlib,json,math,re,sqlite3,time
from datetime import date,datetime,timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo
import requests

FRONT=['DK','DE','PL','LT','LV','EE']
def normalize(record,as_of):
    lic=urlsplit(record.get('license') or '')
    if lic.hostname!='creativecommons.org' or not (lic.path.startswith('/licenses/by/4.0/') or lic.path.startswith('/publicdomain/zero/1.0/')):return None
    if record.get('informationWithheld') or record.get('dataGeneralizations') or record.get('occurrenceStatus')=='ABSENT' or record.get('countryCode') not in FRONT:return None
    try:
        lat=float(record['decimalLatitude']);lon=float(record['decimalLongitude'])
        if not math.isfinite(lat+lon) or not 53<=lat<=60 or not -180<=lon<=180:return None
        uncertainty=record.get('coordinateUncertaintyInMeters')
        if uncertainty is not None and (not math.isfinite(float(uncertainty)) or float(uncertainty)<0):return None
        stamp=str(record.get('eventDate') or '')
        parts=stamp.split('/')
        matches=[re.match(r'^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?(?:T.*)?$',part) for part in parts]
        if not matches or any(m is None for m in matches) or len(parts)>2:return None
        years={int(m[1]) for m in matches}
        if len(years)!=1:return None
        year=years.pop()
        if not 1<=year<=as_of.year:return None
        for m in matches:
            date(int(m[1]),int(m[2] or 1),int(m[3] or 1))
        start=matches[0];precision='day' if start[3] else 'month' if start[2] else 'year'
        if date(year,int(start[2] or 1),int(start[3] or 1))>as_of:return None
        month=int(start[2]) if start[2] else None
        if len(matches)==2:
            end=matches[1]
            if date(year,int(end[2] or 12),int(end[3] or calendar.monthrange(year,int(end[2] or 12))[1]))<date(year,int(start[2] or 1),int(start[3] or 1)):return None
            if start[2]!=end[2]:month=None;precision='year'
            elif start[3]!=end[3]:precision='month'
        original=str(record.get('occurrenceID') or '');key=str(record['key']);dataset=str(record['datasetKey'])
        identity=original if original.startswith(('https://','http://')) else dataset+':'+(original or key)
        return dict(occurrence_uid=identity,gbif_keys=key,dataset_keys=[dataset],country=record['countryCode'],latitude=lat,longitude=lon,uncertainty_m=uncertainty,event_date=stamp,year=year,month=month,date_precision=precision,life_stage=record.get('lifeStage') or 'unknown',license=record['license'])
    except (KeyError,TypeError,ValueError,OverflowError):return None

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--stage',type=Path,required=True);ap.add_argument('--as-of',type=date.fromisoformat,default=datetime.now(ZoneInfo('Europe/Stockholm')).date());ap.add_argument('--offline',action='store_true');a=ap.parse_args()
    stage=a.stage.resolve()
    if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise ValueError('Pilot stage required')
    export=json.loads((stage/'pilot-export.json').read_text());out=stage/'timeline';out.mkdir(exist_ok=True);cache=stage/'source'/('timeline-'+a.as_of.isoformat());cache.mkdir(exist_ok=True)
    session=requests.Session();session.headers['User-Agent']='WCR-watch-timeline/0.1 (cached paginated licensed research)'
    def get(params):
        key=hashlib.sha256(json.dumps(params,sort_keys=True).encode()).hexdigest();path=cache/(key+'.json')
        if not path.exists():
            if a.offline:raise RuntimeError('Missing timeline cache: '+key)
            for attempt in range(4):
                time.sleep(1)
                r=session.get('https://api.gbif.org/v1/occurrence/search',params=params,timeout=45)
                if r.status_code==429 or r.status_code>=500:
                    if attempt==3:r.raise_for_status()
                    time.sleep(min(30,max(3,int(r.headers.get('Retry-After','3')))));continue
                r.raise_for_status();path.write_text(json.dumps(dict(url=r.url,retrieved_at=datetime.now(timezone.utc).isoformat(),data=r.json())));break
        return json.loads(path.read_text()),path
    manifest=dict(as_of=a.as_of.isoformat(),profiles=[])
    profiles=export['profiles']+export.get('references',[])
    profiles.sort(key=lambda p:(p['scientific_name']!='Proserpinus proserpina',p['scientific_name']))
    for p in profiles:
        records={};sources=[];datasets={};fetched=0;rejected=0;conflicts=0;offset=0;total=None;stable=True
        ranges=['1,'+str(a.as_of.year)];range_index=0;scope_total=None
        while True:
            response,path=get(dict(taxonKey=p['gbif_key'],country=FRONT,decimalLatitude='53,60',hasCoordinate='true',hasGeospatialIssue='false',occurrenceStatus='PRESENT',year=ranges[range_index],license=['CC0_1_0','CC_BY_4_0'],limit=300,offset=offset))
            data=response['data'];count=data['count']
            if total is None:total=count
            sources.append(dict(url=response['url'],retrieved_at=response['retrieved_at'],sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
            if len(ranges)==1 and count>10000:
                ranges=['1,1999']+[str(year) for year in range(2000,a.as_of.year+1)]
                continue
            if scope_total is None:scope_total=count
            stable=stable and count==scope_total
            batch=data.get('results',[]);fetched+=len(batch)
            for raw in batch:
                row=normalize(raw,a.as_of)
                if row is None:rejected+=1;continue
                identity=row['occurrence_uid'];previous=records.get(identity)
                if previous:
                    if any(previous[k]!=row[k] for k in ['year','latitude','longitude']):conflicts+=1;continue
                    previous['gbif_keys']=','.join(sorted(set(previous['gbif_keys'].split(','))|{str(raw['key'])}))
                    previous['dataset_keys']=sorted(set(previous['dataset_keys'])|set(row['dataset_keys']))
                else:records[identity]=row
                datasets[raw['datasetKey']]=dict(title=raw.get('datasetName') or raw.get('institutionCode') or raw['datasetKey'],license=raw['license'],url='https://www.gbif.org/dataset/'+raw['datasetKey'])
            if data.get('endOfRecords') or not batch:
                range_index+=1
                if range_index>=len(ranges):break
                offset=0;scope_total=None;continue
            offset+=len(batch)
            if offset+300>100000:raise RuntimeError('Use GBIF download API for this species: '+p['scientific_name'])
        result=dict(candidate_id=p['id'],scientific_name=p['scientific_name'],as_of=a.as_of.isoformat(),countries=FRONT,latitude_range=[53,60],api_total_records=total,fetched_records=fetched,rejected_records=rejected,identity_conflicts=conflicts,complete_api_paging=stable and fetched==total,unique_records=len(records),sources=sources,datasets=datasets,occurrences=sorted(records.values(),key=lambda r:(r['year'],r['event_date'],r['occurrence_uid'])))
        temp=out/(str(p['id'])+'.json.tmp');temp.write_text(json.dumps(result,ensure_ascii=False,separators=(',',':')));temp.replace(out/(str(p['id'])+'.json'))
        manifest['profiles'].append({k:result[k] for k in ['candidate_id','scientific_name','unique_records','complete_api_paging','rejected_records']})
        print(p['scientific_name'],len(records),'unique;',sum(r['year']<2000 for r in records.values()),'before 2000;',fetched,'/',total,'API records',flush=True)
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')

if __name__=='__main__':main()
