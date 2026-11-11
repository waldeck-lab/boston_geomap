"""Bounded public SOS reads for a complete, repeatable reconciliation candidate."""
from datetime import date, timedelta
import time
from urllib.parse import quote

import requests

from geomap.consolidation import map_record, fingerprint

BASE = "https://api.artdatabanken.se/species-observation-system/v1"
FIELDS = [
    "occurrence.occurrenceId","taxon.id","event.startDate","location.decimalLatitude",
    "location.decimalLongitude","occurrence.occurrenceStatus","occurrence.individualCount","modified",
]


class PublicSOSReader:
    def __init__(self, key, progress=lambda **values: None):
        self.session=requests.Session()
        self.session.headers.update({"Ocp-Apim-Subscription-Key":key,"X-Api-Version":"1.5","Accept":"application/json"})
        self.deadline=time.monotonic()+600
        self.next_request=0
        self.progress=progress

    def request(self, method, path, **kwargs):
        for attempt in range(5):
            if time.monotonic()>self.deadline:
                raise TimeoutError("SOS reconciliation deadline exceeded")
            time.sleep(max(0,self.next_request-time.monotonic()))
            self.next_request=time.monotonic()+.65
            response=self.session.request(method,BASE+path,timeout=(10,45),**kwargs)
            if response.status_code==404 and method=="GET":
                return None
            if response.status_code==429 or response.status_code>=500:
                if attempt<4:
                    try:
                        pause=min(30,max(2,int(response.headers.get("Retry-After","3"))))
                    except ValueError:
                        pause=3
                    time.sleep(pause)
                    continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError("SOS retry limit exceeded")

    def snapshot(self, ids, mapping, start, end):
        records={}
        excluded={}
        # Source timestamps can include Swedish UTC offsets. Query a padded UTC
        # interval, then keep the requested calendar dates in map_record.
        pending=[(ids,(date.fromisoformat(start)-timedelta(days=1)).isoformat(),
                      (date.fromisoformat(end)+timedelta(days=1)).isoformat())]
        partitions=0
        while pending:
            batch,first,last=pending.pop()
            body={"dataProvider":{"ids":[1]},"taxon":{"ids":batch,"includeUnderlyingTaxa":False},
                  "date":{"startDate":first+"T00:00:00Z","endDate":last+"T23:59:59Z","dateFilterType":"OnlyStartDate"}}
            params={"sensitiveObservations":"false"}
            count=self.request("POST","/Observations/Count",json=body,params=params)
            if type(count) is not int or count<0:
                raise ValueError("Invalid SOS count")
            if count>9000:
                if len(batch)>1:
                    middle=len(batch)//2
                    pending.extend([(batch[:middle],first,last),(batch[middle:],first,last)])
                else:
                    a,b=date.fromisoformat(first),date.fromisoformat(last)
                    if a>=b:
                        raise ValueError("One-day scope exceeds the safe SOS window")
                    middle=a+(b-a)//2
                    pending.extend([(batch,first,middle.isoformat()),(batch,(middle+timedelta(days=1)).isoformat(),last)])
                continue
            received=0
            for skip in range(0,count,1000):
                payload=self.request("POST","/Observations/Search",
                    json={**body,"output":{"fields":FIELDS}},params={**params,"skip":skip,"take":1000,
                    "sortBy":"occurrence.occurrenceId","sortOrder":"Asc"})
                page=payload.get("records",[])
                if payload.get("totalCount")!=count or len(page)!=min(1000,count-skip):
                    raise ValueError("Incomplete SOS page")
                for source in page:
                    source_id=(source.get("taxon") or {}).get("id")
                    if source_id not in batch:
                        raise ValueError("Source taxon outside the requested scope")
                    oid,row,reason=map_record(source,mapping,start,end)
                    if oid in records or oid in excluded:
                        raise ValueError("Duplicate or changing SOS result")
                    if row is None:
                        excluded[oid]=dict(reason=reason,source_taxon_id=source_id)
                    else:
                        records[oid]=row
                    received+=1
                    if len(records)+len(excluded)>100000:
                        raise ValueError("Reconciliation exceeds 100000 observations")
            if received!=count or self.request("POST","/Observations/Count",json=body,params=params)!=count:
                raise ValueError("SOS scope changed during retrieval")
            partitions+=1
            self.progress(partitions=partitions,source_records=len(records)+len(excluded))
        return records,excluded

    def stable_snapshot(self, ids, mapping, start, end):
        self.progress(phase="fetch_first")
        first=self.snapshot(ids,mapping,start,end)
        self.progress(phase="fetch_confirm")
        second=self.snapshot(ids,mapping,start,end)
        if fingerprint(first)!=fingerprint(second):
            raise ValueError("Source changed between complete reads")
        return second

    def lookup(self, occurrence_id, mapping, start, end):
        record=self.request("GET","/Observations/"+quote(occurrence_id,safe=""),
                            params={"sensitiveObservations":"false"})
        if record is None:
            return None,"not_publicly_returned"
        oid,row,reason=map_record(record,mapping,start,end)
        if oid!=occurrence_id:
            raise ValueError("Occurrence lookup returned another ID")
        return row,reason

    def confirm_missing(self, local_ids, records, excluded, mapping, start, end):
        missing=sorted(set(local_ids)-records.keys())
        self.progress(phase="confirm_missing",lookup_count=len(missing))
        for oid in missing:
            first=self.lookup(oid,mapping,start,end)
            second=self.lookup(oid,mapping,start,end)
            if fingerprint(first)!=fingerprint(second):
                raise ValueError("Occurrence changed between lookups")
            row,reason=second
            if row is not None:
                records[oid]=row
                excluded.pop(oid,None)
            else:
                excluded[oid]={"reason":reason}
