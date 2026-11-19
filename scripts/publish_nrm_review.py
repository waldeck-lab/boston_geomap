"""Build a compact source-cited review artifact; never publish cached full HTML."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

def apply_record_reviews(rows,reviews):
    for row in rows:
        rejected=[r for r in reviews if r["gbif_key"]==row.get("gbif_key")
                  and r["decision"]=="misidentified_not_supporting_species"]
        if not rejected:continue
        if row.get("sweden_count",0)<len(rejected):
            raise ValueError("Refresh Swedish source count before applying record reviews")
        row["record_reviews"]=rejected
        row["excluded_sweden_record_ids"]=[r["record_id"] for r in rejected]
        row["usable_swedish_records_in_review"]=row["sweden_count"]-len(rejected)
        row["review_note"]="Missjö-posten 2017 är felbestämd enligt Jonas Waldeck (2026-11-19) och används inte som svenskt belägg för arten. Råposten och rättelsens ursprung behålls."
        if row["usable_swedish_records_in_review"]==0:
            row["status"]="priority_review_candidate" if row.get("recent_neighbour_count",0)>0 else "source_only_review_candidate"
        for review in rejected:
            if review["record_url"] not in row["evidence_urls"]:row["evidence_urls"].append(review["record_url"])


def main():
    p=argparse.ArgumentParser();p.add_argument("--stage",type=Path,required=True);p.add_argument("--output",type=Path,required=True)
    args=p.parse_args();root=args.stage;report=json.loads((root/"final-review.json").read_text())
    results=report["results"]
    for row in results:
        if row["page_name"]=="Anarsia lineatella":
            row["review_note"]="Taxonuppdelningen 2017 måste vägas in. ET 2018 rättar det äldre svenska namnet till A. innoxiella; äldre svenska etiketter är inte ett automatiskt belägg för dagens A. lineatella."
        elif row.get("gbif_key")==1860076:
            row["review_note"]="Norsk förekomst är expertbelagd. Den svenska posten från Missjö 2017 behöver kvalitetssäkras; artens svenska status lämnas öppen."
        elif row["page_name"]=="Eumichtis lichenea":
            row["review_note"]="Samma art visas redan som Polymixis lichenea. GBIF har olika nycklar för namnvarianterna."
    # Catalog-only names are not silently discarded when no linked species page exists.
    for name in ("Agrotis fatidica","Dichonia convergens"):
        if not any(r["page_name"]==name for r in results):
            results.append(dict(page_name=name,title_name=name,status="catalogue_only_taxon_review",
                family="Noctuidae",swedish_name="",aliases=[name],source_urls=[
                "http://www3.nrm.se/en/svenska_fjarilar/kataloger/CATLEP2015.xls"],
                evidence_urls=["http://www3.nrm.se/en/svenska_fjarilar/kataloger/CATLEP2015.xls"],
                page_updated="Äldre katalog 2015",nrm_phenology="",nrm_hosts="",habitat_tags=[],
                review_note="Namnet finns i det äldre NRM-grannlandsunderlaget. Ingen aktuell länkad artsida kunde verifieras; nomenklatur och svensk status behöver granskas.",
                content_sha256=None))
    review_path=Path(__file__).resolve().parents[1]/"data/watch/nrm-record-reviews.json"
    if review_path.exists():
        record_reviews=json.loads(review_path.read_text())
        apply_record_reviews(results,record_reviews["reviews"])
        report["record_review_version"]=record_reviews["version"]
    counts={}
    for row in results:counts[row["status"]]=counts.get(row["status"],0)+1
    report.update(results=results,counts=counts,unique_taxa=len(results))
    args.output.mkdir(parents=True,exist_ok=True)
    # Remove private raw-response/worker-only fields; source facts and provenance stay.
    for row in report["results"]:
        row.pop("match",None)
    target=args.output/"nrm-review.json"
    target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    fields=["scientific_name","swedish_name","family","status","sweden_records","neighbour_records",
            "nrm_updated","nrm_urls","evidence_urls"]
    with (args.output/"nrm-review.csv").open("w",encoding="utf-8-sig",newline="") as stream:
        writer=csv.DictWriter(stream,fieldnames=fields,delimiter=";");writer.writeheader()
        for r in sorted(results,key=lambda x:(x.get("family") or "",x["page_name"])):
            writer.writerow(dict(scientific_name=r["page_name"],swedish_name=r.get("swedish_name",""),
                family=r.get("family",""),status=r["status"],sweden_records=r.get("sweden_count"),
                neighbour_records=r.get("recent_neighbour_count"),nrm_updated=r.get("page_updated"),
                nrm_urls=" | ".join(r["source_urls"]),evidence_urls=" | ".join(r["evidence_urls"])))
    receipt=dict(records=len(results),counts=counts,sha256=hashlib.sha256(target.read_bytes()).hexdigest())
    (root/"publication-summary.json").write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
    print(json.dumps(receipt,ensure_ascii=False))
if __name__=="__main__":main()
