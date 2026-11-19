"""Cross-check the complete NRM review pool without treating positive/zero GBIF counts as verdicts."""
import argparse
import concurrent.futures
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re
import threading
import time
import requests

COUNTRIES=["DK","NO","FI","DE","PL","LT","LV","EE"]

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--audit",type=Path,required=True);p.add_argument("--old-audit",type=Path,required=True)
    p.add_argument("--pilot",type=Path,required=True);args=p.parse_args()
    root=args.audit.parent;raw=root/"gbif";raw.mkdir(exist_ok=True)
    audit=json.loads(args.audit.read_text());old=json.loads(args.old_audit.read_text());pilot=json.loads(args.pilot.read_text())
    existing={r["gbif_key"] for r in pilot["profiles"]+pilot["references"] if r.get("gbif_key")}
    nrm_names={r["scientific_name"] for r in old["results"] if "nrm_2015_neighbour_list" in r["origins"]}
    old_rows={r["scientific_name"]:r for r in old["results"]}
    selected=[]
    for row in audit["pages"]:
        if "error" in row:continue
        names={row["page_name"],row.get("title_name","")}
        origins=[]
        if row.get("distribution_flag"):origins.append("nrm_distribution")
        if names & nrm_names:origins.append("nrm_neighbour_catalogue_2015")
        if row["page_name"] in ("Anarsia lineatella","Crombrugghia tristis"):origins.append("user_example")
        if origins:selected.append(dict(row,origins=origins))
    local=threading.local()
    def api(endpoint,params):
        key=hashlib.sha256(json.dumps([endpoint,params],sort_keys=True).encode()).hexdigest()
        path=raw/(key+".json")
        if path.exists():return json.loads(path.read_text())
        if not hasattr(local,"session"):
            local.session=requests.Session()
            local.session.headers["User-Agent"]="WCR-NRM-status-review/1.0 (beta.waldeck.se)"
        for attempt in range(3):
            try:
                r=local.session.get("https://api.gbif.org/v1/"+endpoint,params=params,timeout=30);r.raise_for_status()
                result=dict(url=r.url,retrieved_at=datetime.now(timezone.utc).isoformat(),data=r.json())
                path.write_text(json.dumps(result,ensure_ascii=False));time.sleep(.04)
                return result
            except requests.RequestException:
                if attempt==2:raise
                time.sleep(1)
    def screen(row):
        name=row["page_name"]
        result={k:row[k] for k in ("url","page_name","title_name","page_updated","origins","content_sha256")}
        result["nrm_explicit_absence"]=row.get("explicit_swedish_absence",False)
        try:
            match=api("species/match",dict(name=name,strict="true"));m=match["data"]
            result["evidence_urls"]=[row["url"],match["url"]]
            if m.get("matchType")!="EXACT" or m.get("rank")!="SPECIES" or m.get("order")!="Lepidoptera":
                result.update(status="taxonomic_review",match=m);return result
            key=m.get("acceptedUsageKey",m["usageKey"])
            result.update(gbif_key=key,matched_name=m.get("scientificName"),family=m.get("family"),
                          match_status=m.get("status"))
            if key in existing:
                result["status"]="already_visible";return result
            se=api("occurrence/search",dict(taxonKey=key,country="SE",occurrenceStatus="PRESENT",limit=0))
            front=api("occurrence/search",dict(taxonKey=key,country=COUNTRIES,occurrenceStatus="PRESENT",
                      eventDate="2015-01-01,2026-11-19",facet="country",facetLimit=20,limit=0))
            result.update(sweden_count=se["data"]["count"],recent_neighbour_count=front["data"]["count"],
                country_counts=next((x.get("counts",[]) for x in front["data"].get("facets",[]) if x["field"]=="COUNTRY"),[]))
            result["evidence_urls"] += [se["url"],front["url"]]
            previous=old_rows.get(name,{})
            if previous.get("checklist"):
                result["older_checklist"]=previous["checklist"]
            if result["sweden_count"]>0:
                result["status"]="swedish_records_need_review"
            elif "S" in previous.get("checklist",{}).get("countries",[]):
                result["status"]="checklist_status_conflict"
            elif result["recent_neighbour_count"]>0:
                result["status"]="priority_review_candidate"
            else:result["status"]="source_only_review_candidate"
            if name=="Anarsia lineatella":
                result["status"]="taxonomic_split_candidate"
                result["review_note"]="Pre-2017 Swedish names cannot establish current A. lineatella status. ET2018 explicitly replaces the historical Swedish name by A. innoxiella."
                result["evidence_urls"] += ["https://doi.org/10.3897/nl.40.11184",
                    "https://www.sef.nu/download/entomologisk_tidskrift/et_2018/ET%20139%208-20%20Bengtsson%202018.pdf"]
            if key==1860076:
                result["review_note"]="Crombrugghia/Oxyptilus tristis: single Swedish public record requires specimen/record review; NRM text is not a complete current Nordic distribution."
                result["evidence_urls"] += ["https://www.gbif.org/occurrence/1821015801",
                    "https://lister.artsdatabanken.no/rodlisteforarter/2021/10800"]
        except Exception as error:
            result.update(status="query_failed",error=type(error).__name__)
        return result
    output=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for i,row in enumerate(pool.map(screen,selected),1):
            output.append(row)
            if i%25==0:print("SCREENED",i,"/",len(selected),flush=True)
    counts={}
    for row in output:counts[row["status"]]=counts.get(row["status"],0)+1
    report=dict(checked_at=datetime.now(timezone.utc).isoformat(),index_url=audit["index_url"],
        indexed_urls=audit["indexed_urls"],linked_urls=audit["linked_urls"],download_failures=audit["failed"],
        screened=len(output),counts=counts,results=output,
        method=dict(regional_countries=COUNTRIES,from_date="2015-01-01",to_date="2026-11-19",
            map_licence_filter=False,latitude_gate=False,host_matrix_gate=False,cohort_size_stop=False),
        limitations=["A priority candidate is a review result, not verified Swedish absence or expansion.",
          "Source-only candidates are retained even when regional GBIF data is absent.",
          "Swedish public records are conflicts for review, not an automatic rejection.",
          "NRM descriptions may predate later Swedish discoveries; page dates are retained.",
          "Country aggregates count database records, not necessarily unique observations.",
          "Raw HTML is privately retained; no full NRM text is republished."])
    (root/"screening.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print("COMPLETE",json.dumps(counts),flush=True)
if __name__=="__main__":main()
