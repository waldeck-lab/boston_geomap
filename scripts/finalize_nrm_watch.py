"""Finalize explicit name variants and produce a compact, attributable public review."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re
import time
import requests

COUNTRIES=["DK","NO","FI","DE","PL","LT","LV","EE"]
VARIANTS={
"Borearctia menetriesii":1811423,"Calamotropha aureliella":1890213,
"Capperia britanniodactyla":1859872,"Epermenia aequidentella":1869450,
"Macrosaccus robiniellus":8741260,"Peribatodes ilicarius":1957565,
"Phyllonorycter comparellus":1750056,"Phyllonorycter medicaginellus":1750286,
"Phyllonorycter mespilellus":1750213,"Phyllonorycter messaniellus":1750131,
"Phyllonorycter pyrifoliellus":1750220,"Phyllonorycter scopariellus":1750086,
"Phyllonorycter trifoliellus":1749994,"Proserpinus proserpinus":1864161,
"Pseudobissetia terrestrella":1889905}

def main():
    p=argparse.ArgumentParser();p.add_argument("--stage",type=Path,required=True)
    p.add_argument("--catalogue",type=Path,required=True);p.add_argument("--pilot",type=Path,required=True)
    args=p.parse_args();root=args.stage
    report=json.loads((root/"screening.json").read_text());audit=json.loads((root/"audit.json").read_text())
    catalogue=json.loads(args.catalogue.read_text());sv=json.loads((root/"index-swedish-names.json").read_text())
    pages={r["url"]:r for r in audit["pages"] if "error" not in r}
    pilot=json.loads(args.pilot.read_text());shown={r["gbif_key"]:r for r in pilot["profiles"]+pilot["references"] if r.get("gbif_key")}
    raw=root/"gbif";session=requests.Session()
    session.headers["User-Agent"]="WCR-NRM-final-review/1.0 (beta.waldeck.se)"
    def api(endpoint,params):
        key=hashlib.sha256(json.dumps([endpoint,params],sort_keys=True).encode()).hexdigest();path=raw/(key+".json")
        if path.exists():return json.loads(path.read_text())
        for attempt in range(4):
            r=session.get("https://api.gbif.org/v1/"+endpoint,params=params,timeout=30)
            if r.status_code!=429:break
            wait=min(60,max(10,int(r.headers.get("Retry-After","30"))))
            print("GBIF rate limit; respecting retry pause",wait,flush=True);time.sleep(wait)
        r.raise_for_status()
        data=dict(url=r.url,retrieved_at=datetime.now(timezone.utc).isoformat(),data=r.json())
        path.write_text(json.dumps(data,ensure_ascii=False));return data
    crosswalk=[]
    for row in report["results"]:
        name=row["page_name"]
        if row["status"]=="taxonomic_review" and name in VARIANTS:
            key=VARIANTS[name];m=row["match"]
            ids=catalogue.get("name_index",{}).get(name.lower(),[])
            if len(ids)!=1 or m.get("usageKey")!=key or m.get("rank")!="SPECIES" or m.get("order")!="Lepidoptera":
                raise ValueError("Name variant evidence changed: "+name)
            review=dict(name=name,gbif_key=key,dyntaxa_id=ids[0],gbif_name=m.get("canonicalName"),
                status="reviewed_ending_or_spelling_variant",
                sources=[row["url"],"https://artfakta.se/taxa/"+str(ids[0])+"/information",
                         "https://api.gbif.org/v1/species/"+str(key)],
                note="Explicitly reviewed NRM/Dyntaxa name versus GBIF ending/spelling variant. No general fuzzy admission.")
            crosswalk.append(review);row["name_review"]=review
            row.update(gbif_key=key,matched_name=m["scientificName"],family=m.get("family"))
            row["evidence_urls"]+=review["sources"]
            if key in shown:row["status"]="already_visible"
            else:
                se=api("occurrence/search",dict(taxonKey=key,country="SE",occurrenceStatus="PRESENT",limit=0))
                front=api("occurrence/search",dict(taxonKey=key,country=COUNTRIES,occurrenceStatus="PRESENT",
                    eventDate="2015-01-01,2026-11-19",facet="country",facetLimit=20,limit=0))
                row.update(sweden_count=se["data"]["count"],recent_neighbour_count=front["data"]["count"],
                    country_counts=next((x.get("counts",[]) for x in front["data"].get("facets",[]) if x["field"]=="COUNTRY"),[]))
                row["evidence_urls"] += [se["url"],front["url"]]
                row["status"]="swedish_records_need_review" if row["sweden_count"] else "priority_review_candidate" if row["recent_neighbour_count"] else "source_only_review_candidate"
        if name=="Eumichtis lichenea":
            row.update(status="already_visible",shown_as="Polymixis lichenea")
            row["review_note"]="Same species shown as Polymixis lichenea; GBIF currently splits the nomenclatural combinations between different keys."
            row["evidence_urls"].append("https://lepiforum.org/wiki/page/Polymixis_Lichenea")
        source=pages[row["url"]]
        if source.get("swedish_locality") and not source.get("explicit_swedish_absence") and row["status"]!="already_visible":
            row["status"]="swedish_in_nrm"
        row["swedish_name"]=sv.get(source.get("index_link_correction",row["url"]),"")
        fields=source.get("fields",{})
        row["nrm_phenology"]=fields.get("Flygtid","")[:140]
        row["nrm_hosts"]=(fields.get("Värdväxt") or fields.get("Näringsväxt") or "")[:160]
        habitat=fields.get("Levnadssätt","")+" "+fields.get("Förekomst","")
        tags=[]
        for pattern,label in [(r"torr.*äng|torr.*gräs","torr gräsmark"),(r"skogsbryn","skogsbryn"),
            (r"lövskog","lövskog"),(r"barrskog","barrskog"),(r"sand","sandmark"),(r"äng","ängsmark"),
            (r"myr|mosse","våtmark"),(r"ljus|UV","ljuslockning")]:
            if re.search(pattern,habitat,re.I):tags.append(label)
        row["habitat_tags"]=tags
        row["introduction_mentioned"]=bool(re.search(r"införd|importerad",source.get("distribution_text",""),re.I))
        row["page_updated"]=source.get("page_updated")
        if row.get("gbif_key") in shown:
            row["status"]="already_visible";row.setdefault("shown_as",shown[row["gbif_key"]]["scientific_name"])
    # Species-level deduplication, retaining every NRM URL/name and review finding.
    groups={}
    for row in report["results"]:
        key="shown:"+row["shown_as"] if row.get("shown_as") else "gbif:"+str(row["gbif_key"]) if row.get("gbif_key") else "nrm:"+row["page_name"]
        groups.setdefault(key,[]).append(row)
    rows=[]
    priority={"already_visible":0,"swedish_in_nrm":1,"taxonomic_split_candidate":2,
              "swedish_records_need_review":3,"checklist_status_conflict":4,"priority_review_candidate":5,
              "source_only_review_candidate":6,"taxonomic_review":7,"query_failed":8}
    for group in groups.values():
        group.sort(key=lambda r:priority.get(r["status"],9));row=dict(group[0])
        row["aliases"]=sorted({r["page_name"] for r in group}|{r.get("title_name","") for r in group}- {""})
        row["source_urls"]=sorted({r["url"] for r in group})
        row["evidence_urls"]=list(dict.fromkeys(u for r in group for u in r["evidence_urls"]))
        if row.get("shown_as"):row["aliases"]=sorted(set(row["aliases"])|{row["shown_as"]})
        row.pop("match",None)
        rows.append(row)
    counts={}
    for row in rows:counts[row["status"]]=counts.get(row["status"],0)+1
    report.update(results=rows,counts=counts,screened_rows=report["screened"],unique_taxa=len(rows),
        name_variant_reviews=crosswalk,finished_at=datetime.now(timezone.utc).isoformat())
    (root/"final-review.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(dict(unique=len(rows),counts=counts,name_variants=len(crosswalk)),ensure_ascii=False))
if __name__=="__main__":main()
