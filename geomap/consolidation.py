"""Deterministic species-complex reconciliation with an atomic, stale-safe apply."""
from datetime import date, datetime, timezone
import hashlib
import json
import math

from geomap.shared_store import apply_rows

ARTPORTALEN_PREFIX = "urn:lsid:artportalen.se:sighting:"
SOURCE_SCOPE = "artportalen-reconciled-v1"


def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()


def taxon_name(taxon, kind):
    return next((row["name"] for row in taxon.get("names",[])
                 if row.get("isRecommended") and (row.get("category") or {}).get("value")==kind),"")


def complex_scope(taxa, roots):
    by_id={int(row["taxonId"]):row for row in taxa}
    if not roots or len(set(roots))>10 or any(tid not in by_id for tid in roots):
        raise ValueError("Unknown or excessive root taxon IDs")
    mapping={}
    for tid in by_id:
        seen=set()
        node=tid
        while node in by_id and node not in seen:
            seen.add(node)
            taxon=by_id[node]
            if ((taxon.get("category") or {}).get("value")=="Species"
                and (taxon.get("status") or {}).get("value")=="Accepted"
                and (taxon.get("type") or {}).get("value")=="Taxonomic"):
                mapping[tid]=node
                break
            node=taxon.get("parentId")
    scope=set(roots)
    while True:
        expanded=scope | {mapping[tid] for tid in scope if tid in mapping}
        expanded.update(tid for tid,row in by_id.items()
                        if row.get("parentId") in scope or scope.intersection(row.get("secondaryParents") or []))
        if len(expanded)>200:
            raise ValueError("Taxon complex exceeds 200 taxa")
        if expanded==scope:
            return sorted(scope),mapping,by_id
        scope=expanded


def map_record(record, mapping, start, end):
    """Retain only public map fields. None means the current record is outside the map scope."""
    occurrence=record.get("occurrence") or {}
    occurrence_id=occurrence.get("occurrenceId")
    if not isinstance(occurrence_id,str) or not occurrence_id.startswith(ARTPORTALEN_PREFIX):
        raise ValueError("Unexpected data provider or missing occurrence ID")
    tid=(record.get("taxon") or {}).get("id")
    if type(tid) is not int or tid<=0:
        raise ValueError("Missing source taxon")
    stamp=(record.get("event") or {}).get("startDate")
    if not isinstance(stamp,str):
        return occurrence_id,None,"no_usable_date"
    try:
        observed=date.fromisoformat(stamp[:10])
    except ValueError:
        return occurrence_id,None,"no_usable_date"
    status=occurrence.get("occurrenceStatus") or {}
    status=status.get("value") if isinstance(status,dict) else status
    if status and status!="present":
        return occurrence_id,None,"not_present"
    if not start<=observed.isoformat()<=end:
        return occurrence_id,None,"outside_date_range"
    location=record.get("location") or {}
    try:
        latitude=float(location["decimalLatitude"])
        longitude=float(location["decimalLongitude"])
    except (KeyError,TypeError,ValueError):
        return occurrence_id,None,"no_usable_coordinates"
    if not math.isfinite(latitude) or not math.isfinite(longitude) or not (
        -85.05112878<=latitude<=85.05112878 and -180<=longitude<=180):
        return occurrence_id,None,"no_usable_coordinates"
    return occurrence_id,dict(OccurrenceId=occurrence_id,DyntaxaTaxonId=mapping.get(tid,tid),
        StartDate=observed.isoformat(),DecimalLatitude=latitude,DecimalLongitude=longitude,SourceTaxonId=tid,
        OccurrenceStatus="present",IndividualCount=occurrence.get("individualCount"),
        Modified=record.get("modified")),None


def local_slice(db, plan):
    return [dict(row) for row in db.execute("""SELECT * FROM observations WHERE
        occurrence_id LIKE ? AND taxon_id IN (SELECT value FROM json_each(?))
        AND observation_date BETWEEN ? AND ? ORDER BY occurrence_id""",
        (ARTPORTALEN_PREFIX+"%",json.dumps(plan["scope_taxa"]),plan["date_from"],plan["date_to"]))]


def state_hash(db, plan):
    incoming=sorted(plan["rows"])
    affected=json.dumps(plan["affected_taxa"])
    state={
        "slice":local_slice(db,plan),
        "incoming_existing":[dict(row) for row in db.execute("""SELECT * FROM observations
            WHERE occurrence_id IN (SELECT value FROM json_each(?)) ORDER BY occurrence_id""",(json.dumps(incoming),))],
        "taxa":[list(row) for row in db.execute("SELECT * FROM taxa WHERE taxon_id IN (SELECT value FROM json_each(?)) ORDER BY taxon_id",(affected,))],
        "coverage":[list(row) for row in db.execute("""SELECT * FROM coverage
            WHERE taxon_id IN (SELECT value FROM json_each(?)) ORDER BY taxon_id,date_from,date_to,source_scope""",(affected,))],
        "memberships":[list(row) for row in db.execute("""SELECT list_id,taxon_id FROM list_taxa
            WHERE taxon_id IN (SELECT value FROM json_each(?)) ORDER BY list_id,taxon_id""",(affected,))],
        "revision":db.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()[0],
    }
    return fingerprint(state)


def build_plan(db, *, job_id, roots, scope, mapping, taxa, records, excluded, start, end, source_hash):
    plan=dict(schema=1,id=job_id,generated_at=datetime.now(timezone.utc).isoformat(),roots=roots,scope_taxa=scope,date_from=start,date_to=end,
              source="artportalen-public",taxonomy_hash=source_hash,rows=records,excluded=excluded,
              complete_taxa=sorted({mapping.get(tid,tid) for tid in scope}))
    old={row["occurrence_id"]:row for row in local_slice(db,plan)}
    existing={row["occurrence_id"]:dict(row) for row in db.execute(
        "SELECT * FROM observations WHERE occurrence_id IN (SELECT value FROM json_each(?))",(json.dumps(sorted(records)),))}
    plan["prune_ids"]=sorted(old.keys()-records.keys())
    plan["affected_taxa"]=sorted(set(scope)|{row["taxon_id"] for row in old.values()}|
                                  {row["DyntaxaTaxonId"] for row in records.values()}|
                                  {row["taxon_id"] for row in existing.values()})
    if any(tid not in taxa for tid in plan["affected_taxa"]):
        raise ValueError("Affected source taxon missing from the current taxonomy")
    plan["taxa"]={str(tid):dict(scientific_name=taxon_name(taxa[tid],"ScientificName"),
                               swedish_name=taxon_name(taxa[tid],"SwedishName"))
                  for tid in plan["affected_taxa"]}
    # Stage normalization through the same writer used by ordinary GeoMap imports.
    changed=moved=unchanged=0
    for oid,row in records.items():
        before=existing.get(oid)
        if before is None:
            continue
        moved += before["taxon_id"]!=row["DyntaxaTaxonId"]
        fields=(("taxon_id","DyntaxaTaxonId"),("observation_date","StartDate"),
                ("latitude","DecimalLatitude"),("longitude","DecimalLongitude"),("modification_date","Modified"))
        different=any(before[left]!=row.get(right) for left,right in fields)
        count=row.get("IndividualCount")
        try:
            count=int(float(count)) if count not in (None,"") else None
        except (ValueError,TypeError,OverflowError):
            count=None
        different |= before["individual_count"]!=count
        changed += different
        unchanged += not different
    affected=json.dumps(plan["affected_taxa"])
    plan["summary"]=dict(local_in_scope=len(old),source_mappable=len(records),
        added=len(records.keys()-existing.keys()),updated=changed,moved=moved,unchanged=unchanged,
        pruned=len(plan["prune_ids"]),excluded=len(excluded),
        affected_taxa=len(plan["affected_taxa"]),
        affected_lists=db.execute("""SELECT COUNT(DISTINCT list_id) FROM list_taxa
          WHERE taxon_id IN (SELECT value FROM json_each(?))""",(affected,)).fetchone()[0])
    plan["baseline_hash"]=state_hash(db,plan)
    plan["plan_hash"]=fingerprint(plan)
    return plan


def apply_plan(db, plan, *, max_age_seconds=3600):
    """Caller supplies a shared-store connection. All data, cells and audit commit together."""
    expected=plan["plan_hash"]
    if fingerprint({key:value for key,value in plan.items() if key!="plan_hash"})!=expected:
        raise ValueError("Candidate plan changed")
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("""CREATE TABLE IF NOT EXISTS consolidation_runs(
            id TEXT PRIMARY KEY,plan_hash TEXT NOT NULL,receipt_json TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
        prior=db.execute("SELECT plan_hash,receipt_json FROM consolidation_runs WHERE id=?",(plan["id"],)).fetchone()
        if prior:
            if prior[0]!=expected:
                raise ValueError("Job ID reused for a different plan")
            db.commit()
            return json.loads(prior[1])
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(plan["generated_at"])).total_seconds()
        if not -30<=age<=max_age_seconds:
            raise ValueError("Candidate expired; fetch a new candidate")
        if state_hash(db,plan)!=plan["baseline_hash"]:
            raise ValueError("Local baseline changed; fetch a new candidate")
        db.executemany("""INSERT INTO taxa VALUES(?,?,?) ON CONFLICT(taxon_id) DO UPDATE SET
            scientific_name=excluded.scientific_name,swedish_name=excluded.swedish_name""",
            [(int(tid),row["scientific_name"],row["swedish_name"]) for tid,row in plan["taxa"].items()])
        db.execute("DELETE FROM observations WHERE occurrence_id IN (SELECT value FROM json_each(?))",(json.dumps(plan["prune_ids"]),))
        written=apply_rows(db,list(plan["rows"].values()))
        if written!=len(plan["rows"]):
            raise ValueError("Candidate normalization differs from the import writer")
        affected=json.dumps(plan["affected_taxa"])
        db.execute("DELETE FROM taxon_cells WHERE taxon_id IN (SELECT value FROM json_each(?))",(affected,))
        db.execute("""INSERT INTO taxon_cells SELECT taxon_id,year,slot_id,tile_x,tile_y,COUNT(*)
            FROM observations WHERE taxon_id IN (SELECT value FROM json_each(?))
            GROUP BY taxon_id,year,slot_id,tile_x,tile_y""",(affected,))
        db.execute("DELETE FROM coverage WHERE taxon_id IN (SELECT value FROM json_each(?))",(affected,))
        mapped_scope=plan["complete_taxa"]
        # Provider-specific completeness is distinct from the legacy multi-provider import scope.
        db.executemany("""INSERT OR REPLACE INTO coverage(taxon_id,date_from,date_to,source_scope) VALUES(?,?,?,?)""",
            [(tid,plan["date_from"],plan["date_to"],SOURCE_SCOPE) for tid in mapped_scope])
        db.execute("UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")
        if db.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Foreign key validation failed")
        count=db.execute("SELECT COUNT(*) FROM observations WHERE taxon_id IN (SELECT value FROM json_each(?))",(affected,)).fetchone()[0]
        total=db.execute("SELECT COALESCE(SUM(n),0) FROM taxon_cells WHERE taxon_id IN (SELECT value FROM json_each(?))",(affected,)).fetchone()[0]
        if count!=total:
            raise ValueError("Observation/cell total mismatch")
        receipt=dict(plan_hash=expected,summary=plan["summary"],affected_observations=count,
                     revision=db.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()[0])
        db.execute("INSERT INTO consolidation_runs(id,plan_hash,receipt_json) VALUES(?,?,?)",
                   (plan["id"],expected,json.dumps(receipt)))
        db.commit()
        return receipt
    except BaseException:
        db.rollback()
        raise
