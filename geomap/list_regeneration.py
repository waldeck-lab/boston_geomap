"""Replace an owned missing-species list from a new CSV; retain version receipts."""
from datetime import datetime,timezone
import hashlib
import json
from uuid import uuid4
from geomap.shared_store import connect
from geomap.observed_csv import CsvValidationError

SCHEMA="""CREATE TABLE IF NOT EXISTS list_regenerations(
 id TEXT PRIMARY KEY,list_id TEXT NOT NULL,owner_id INTEGER NOT NULL,title TEXT NOT NULL,
 created_at TEXT NOT NULL,before_json TEXT NOT NULL,after_json TEXT NOT NULL,
 observed_json TEXT NOT NULL,metadata_json TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS regeneration_list ON list_regenerations(list_id,created_at);"""

class RegenerationConflict(CsvValidationError):
    pass

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def owned(db,list_id,owner):
    row=db.execute("SELECT * FROM lists WHERE id=? AND owner_id=? AND visibility='private'",(list_id,owner)).fetchone()
    if row is None:
        raise PermissionError("Listan hittades inte.")
    return dict(row)

def preview(db,list_id,owner,rows,summary):
    item=owned(db,list_id,owner)
    before=[r[0] for r in db.execute("SELECT taxon_id FROM list_taxa WHERE list_id=? ORDER BY taxon_id",(list_id,))]
    if hashlib.sha256(json.dumps(before).encode()).hexdigest()!=item["content_hash"]:
        raise RegenerationConflict("Listans kontrollsumma stämmer inte. Validera listan innan den uppdateras.")
    after=sorted({int(r["taxon_id"]) for r in rows})
    added=sorted(set(after)-set(before));removed=sorted(set(before)-set(after))
    token=digest(dict(list_id=list_id,owner=owner,before=before,after=after,
                      before_hash=item["content_hash"],title=item["title"],
                      source=summary["taxonomy_source_sha256"],catalogue=summary["catalogue_sha256"],
                      upload=summary["upload_sha256"]))
    imported=0
    for tid in after:
        imported+=bool(db.execute("SELECT 1 FROM coverage WHERE taxon_id=? LIMIT 1",(tid,)).fetchone())
    return dict(list_id=list_id,title=item["title"],token=token,before=before,after=after,
                added=added,removed=removed,unchanged=len(set(before)&set(after)),
                reusable_taxa=imported,before_hash=item["content_hash"],
                large_change=len(added)+len(removed)>max(50,len(before)*.25))

def activate(db_path,list_id,owner,rows,observed,summary,token,confirm_large=False):
    with connect(db_path) as db:
        db.executescript(SCHEMA)
        db.execute("BEGIN IMMEDIATE")
        plan=preview(db,list_id,owner,rows,summary)
        if not token or token!=plan["token"]:
            raise RegenerationConflict("Förhandsvisningen är inaktuell. Kontrollera CSV-filen igen.")
        if plan["large_change"] and not confirm_large:
            raise RegenerationConflict("Stor liständring. Bekräfta skillnaden i förhandsvisningen.")
        stamp=datetime.now(timezone.utc)
        receipt=stamp.strftime("%Y%m%dT%H%M%SZ")+"-"+uuid4().hex[:8]
        metadata={k:v for k,v in summary.items() if k not in {"missing_preview","replacement"}}
        metadata.update(regeneration_id=receipt,regenerated_at=stamp.isoformat(),catalogue_scope="lepidoptera")
        # No observations/cells are touched. Membership is replaced inside this transaction.
        db.executemany("""INSERT INTO taxa VALUES(?,?,?) ON CONFLICT(taxon_id) DO UPDATE SET scientific_name=excluded.scientific_name,swedish_name=excluded.swedish_name""",
            [(r["taxon_id"],r["scientific_name"],r["swedish_name"]) for r in rows])
        db.execute("DELETE FROM list_taxa WHERE list_id=?",(list_id,))
        db.executemany("INSERT INTO list_taxa VALUES(?,?)",[(list_id,tid) for tid in plan["after"]])
        after_hash=hashlib.sha256(json.dumps(plan["after"]).encode()).hexdigest()
        db.execute("UPDATE lists SET content_hash=? WHERE id=?",(after_hash,list_id))
        db.execute("""CREATE TABLE IF NOT EXISTS list_imports(
            list_id TEXT PRIMARY KEY REFERENCES lists(id) ON DELETE CASCADE,
            source_type TEXT NOT NULL,metadata_json TEXT NOT NULL)""")
        db.execute("""INSERT INTO list_imports VALUES(?,'observed-species-csv',?)
            ON CONFLICT(list_id) DO UPDATE SET source_type=excluded.source_type,metadata_json=excluded.metadata_json""",
            (list_id,json.dumps(metadata)))
        db.execute("INSERT INTO list_regenerations VALUES(?,?,?,?,?,?,?,?,?)",
            (receipt,list_id,owner,plan["title"],stamp.isoformat(),json.dumps(plan["before"]),
             json.dumps(plan["after"]),json.dumps(sorted(observed)),json.dumps(metadata)))
        db.execute("UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")
        # The CSV list is now current; map completeness is independently checked by coverage.
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='list_taxonomy_state'").fetchone():
            db.execute("DELETE FROM list_taxonomy_state WHERE list_id=?",(list_id,))
        coverage=db.execute("""SELECT COUNT(*) FROM list_taxa l WHERE l.list_id=? AND EXISTS(
            SELECT 1 FROM coverage c WHERE c.taxon_id=l.taxon_id AND c.date_from='2000-01-01'
            AND c.date_to='2026-10-31' AND c.source_scope='public-only-start-v1')""",(list_id,)).fetchone()[0]
    return dict(id=list_id,title=plan["title"],visibility="private",taxa_count=len(plan["after"]),
                taxa_needing_import=len(plan["after"])-coverage,regeneration_id=receipt,
                replaced=True,added_count=len(plan["added"]),removed_count=len(plan["removed"]),
                csv_url=f"/geomap-api/lists/{list_id}/csv",map_url=f"/geomap?list_id={list_id}")
