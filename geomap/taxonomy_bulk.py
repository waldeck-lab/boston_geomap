"""One reviewed, stale-checked atomic SQL update using the existing per-taxon writer."""
from datetime import datetime,timezone
import hashlib
import json
from geomap.taxonomy_history import load_source,normalized_taxon,record_source,apply_taxon,encode

def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()

def baseline(db):
    return digest([dict(row) for row in db.execute("SELECT * FROM taxa ORDER BY taxon_id")])

def prepare(db,source_path):
    source,sha=load_source(source_path)
    taxa={int(t["taxonId"]):t for t in source["taxa"]}
    stored={row["taxon_id"]:dict(row) for row in db.execute("SELECT * FROM taxa")}
    accepted={tid for tid,t in taxa.items() if (
        (t.get("category") or {}).get("value"),(t.get("type") or {}).get("value"),
        (t.get("status") or {}).get("value"))==("Species","Taxonomic","Accepted")}
    changes=[]
    name_changes=source_changes=structural_changes=0
    for tid in sorted(set(stored)|accepted):
        row=stored.get(tid);taxon=taxa.get(tid)
        if taxon is None:
            if row and row["is_active"]:
                changes.append(dict(taxon_id=tid,kind="deactivate",scientific_name=row["sci_name"]))
            continue
        n=normalized_taxon(taxon)
        if row:
            previous=normalized_taxon(json.loads(row["raw_json"]))
            name_changes+=int([row["sci_name"] or "",row["swe_name"] or ""]!=[n["scientific_name"],n["swedish_name"]])
            source_changes+=int(previous!=n)
            structural_changes+=int(any(previous.get(k)!=n.get(k) for k in
                ("category","type","status","parent_id","secondary_parents","relations","exclude_from_reporting")))
        values=(n["scientific_name"],n["swedish_name"],n["category"],n["type"],n["status"],n["parent_id"],int(tid in accepted))
        before=tuple(row[k] for k in ("sci_name","swe_name","category","type","status","parent_id","is_active")) if row else None
        if row is None or values!=before or json.loads(row["raw_json"])!=taxon:
            kind="insert" if row is None else "deactivate" if row["is_active"] and tid not in accepted else "update"
            changes.append(dict(taxon_id=tid,kind=kind,scientific_name=n["scientific_name"],
                before_names=[row["sci_name"],row["swe_name"]] if row else None,
                after_names=[n["scientific_name"],n["swedish_name"]]))
    counts={kind:sum(row["kind"]==kind for row in changes) for kind in ("insert","update","deactivate")}
    active=sum(bool(row["is_active"]) for row in stored.values())
    result=dict(source_hash=sha,source_at=source["retrieved_at"],baseline=baseline(db),
                changes=changes,counts=counts,total=len(changes),
                name_changes=name_changes,source_changes=source_changes,structural_changes=structural_changes,
                suspicious=max(counts["deactivate"],structural_changes)>max(50,active*.25))
    result["fingerprint"]=digest(result)
    return result

def apply(db,source_path,plan,job_id,actor_id):
    db.execute("""CREATE TABLE IF NOT EXISTS taxonomy_bulk_receipts(
        job_id TEXT PRIMARY KEY,receipt_json TEXT NOT NULL)""")
    prior=db.execute("SELECT receipt_json FROM taxonomy_bulk_receipts WHERE job_id=?",(job_id,)).fetchone()
    if prior:return json.loads(prior[0])
    check={k:v for k,v in plan.items() if k!="fingerprint"}
    if digest(check)!=plan["fingerprint"]:
        raise ValueError("Plan fingerprint mismatch")
    if baseline(db)!=plan["baseline"]:
        raise ValueError("Local SQL changed since preview")
    source,sha=load_source(source_path)
    if sha!=plan["source_hash"]:
        raise ValueError("Source changed since preview")
    age=(datetime.now(timezone.utc)-datetime.fromisoformat(source["retrieved_at"])).total_seconds()
    if not -300<=age<=3600:
        raise ValueError("Preview expired; fetch a new source")
    # Recompute candidates too, to prevent a modified on-disk plan omitting a change.
    if prepare(db,source_path)["fingerprint"]!=plan["fingerprint"]:
        raise ValueError("Preview no longer matches source")
    source_id=record_source(db,source_path)
    latest=db.execute("SELECT MAX(retrieved_at) FROM taxonomy_sources").fetchone()[0]
    if source["retrieved_at"]<latest:
        raise ValueError("A newer source exists")
    for row in plan["changes"]:
        apply_taxon(db,source_path=source_path,taxon_id=row["taxon_id"],
            job_id=job_id+":"+str(row["taxon_id"]),actor_id=actor_id,
            prepared=(source,sha,source_id))
    receipt=dict(job_id=job_id,source_hash=sha,source_at=source["retrieved_at"],
                 counts=plan["counts"],total=plan["total"],actor_id=actor_id,
                 completed_at=datetime.now(timezone.utc).isoformat(),fingerprint=plan["fingerprint"])
    db.execute("INSERT INTO taxonomy_bulk_receipts VALUES(?,?)",(job_id,encode(receipt)))
    db.execute("UPDATE meta SET value=? WHERE key='last_source_hash'",(sha,))
    return receipt
