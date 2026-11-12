"""Persistent source observations and transactional local taxonomy audit."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

from geomap.consolidation import taxon_name

SCHEMA = [
"""CREATE TABLE IF NOT EXISTS taxonomy_sources(
 id INTEGER PRIMARY KEY, source_hash TEXT NOT NULL UNIQUE, retrieved_at TEXT NOT NULL,
 recorded_at TEXT NOT NULL, content_hash TEXT NOT NULL, taxon_count INTEGER NOT NULL,
 data BLOB NOT NULL, source_path TEXT NOT NULL, comparison_status TEXT NOT NULL)""",
"""CREATE TABLE IF NOT EXISTS taxonomy_history_events(
 id INTEGER PRIMARY KEY, phase TEXT NOT NULL, kind TEXT NOT NULL, taxon_id INTEGER NOT NULL,
 label TEXT NOT NULL, event_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
 source_id INTEGER REFERENCES taxonomy_sources(id), actor_id INTEGER, job_id TEXT,
 fields_json TEXT NOT NULL, before_json TEXT, after_json TEXT)""",
"""CREATE INDEX IF NOT EXISTS taxonomy_events_taxon ON taxonomy_history_events(taxon_id,id)""",
"""CREATE TABLE IF NOT EXISTS taxonomy_local_baseline(
 taxon_id INTEGER PRIMARY KEY,captured_at TEXT NOT NULL,state_json TEXT NOT NULL)""",
"""CREATE TABLE IF NOT EXISTS taxonomy_update_receipts(
 job_id TEXT PRIMARY KEY,taxon_id INTEGER NOT NULL,source_id INTEGER NOT NULL REFERENCES taxonomy_sources(id),
 outcome TEXT NOT NULL,event_id INTEGER,checked_at TEXT NOT NULL,actor_id INTEGER)""",
"""CREATE TABLE IF NOT EXISTS taxonomy_write_context(
 id INTEGER PRIMARY KEY CHECK(id=1),source_id INTEGER REFERENCES taxonomy_sources(id),
 actor_id INTEGER,job_id TEXT)""",
]
LOCAL_FIELDS = {
 "local_index":"local_index","scientific_name":"sci_name","swedish_name":"swe_name",
 "category":"category","type":"type","status":"status","parent_id":"parent_id",
 "is_active":"is_active","raw_sha256":"sha256","raw_taxon":"raw_json",
}


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False)


def local_json(alias):
    return "json_object("+",".join("'"+key+"',"+alias+"."+column for key,column in LOCAL_FIELDS.items())+")"


def ensure_schema(db):
    for statement in SCHEMA:
        db.execute(statement)
    for operation,old,new,kind,label in (
        ("INSERT","NULL",local_json("NEW"),"'local_inserted'","COALESCE(NEW.sci_name,'')"),
        ("UPDATE",local_json("OLD"),local_json("NEW"),
         "CASE WHEN OLD.is_active=1 AND NEW.is_active=0 THEN 'local_deactivated' WHEN OLD.is_active=0 AND NEW.is_active=1 THEN 'local_reactivated' ELSE 'local_changed' END",
         "COALESCE(NEW.sci_name,OLD.sci_name,'')"),
        ("DELETE",local_json("OLD"),"NULL","'local_deleted'","COALESCE(OLD.sci_name,'')"),
    ):
        if operation=="UPDATE":
            differences=" UNION ALL ".join(
                f"SELECT '{key}' AS field WHERE OLD.{column} IS NOT NEW.{column}" for key,column in LOCAL_FIELDS.items())
            fields="(SELECT json_group_array(field) FROM ("+differences+"))"
            condition=" WHEN "+" OR ".join(f"OLD.{column} IS NOT NEW.{column}" for column in LOCAL_FIELDS.values())
        else:
            fields="'[]'"
            condition=""
        taxon="OLD.taxon_id" if operation=="DELETE" else "NEW.taxon_id"
        db.execute(f"""CREATE TRIGGER IF NOT EXISTS taxonomy_audit_{operation.lower()}
            AFTER {operation} ON taxa{condition}
            BEGIN
              INSERT INTO taxonomy_history_events(phase,kind,taxon_id,label,event_at,recorded_at,
                  source_id,actor_id,job_id,fields_json,before_json,after_json)
              VALUES('local',{kind},{taxon},{label},strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                  (SELECT source_id FROM taxonomy_write_context WHERE id=1),
                  (SELECT actor_id FROM taxonomy_write_context WHERE id=1),
                  (SELECT job_id FROM taxonomy_write_context WHERE id=1),
                  {fields},{old},{new});
            END""")
    if db.execute("SELECT 1 FROM taxonomy_local_baseline LIMIT 1").fetchone() is None:
        stamp=now()
        for row in db.execute("SELECT * FROM taxa").fetchall():
            state={key:row[column] for key,column in LOCAL_FIELDS.items()}
            db.execute("INSERT INTO taxonomy_local_baseline VALUES(?,?,?)",(row["taxon_id"],stamp,encode(state)))


@contextmanager
def history_db(path):
    db=sqlite3.connect(Path(path).resolve().as_uri()+"?mode=rw",uri=True,timeout=15)
    db.row_factory=sqlite3.Row
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        ensure_schema(db)
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def normalized_taxon(taxon):
    return dict(
        scientific_name=taxon_name(taxon,"ScientificName"),
        swedish_name=taxon_name(taxon,"SwedishName"),
        category=(taxon.get("category") or {}).get("value"),
        type=(taxon.get("type") or {}).get("value"),
        status=(taxon.get("status") or {}).get("value"),
        parent_id=taxon.get("parentId"), secondary_parents=sorted(taxon.get("secondaryParents") or []),
        names=sorted([dict(name=row.get("name"),category=(row.get("category") or {}).get("value"),
                           recommended=bool(row.get("isRecommended")),author=row.get("author"))
                      for row in taxon.get("names",[])],key=encode),
        redlist_category=taxon.get("redlistCategory"),
        status_reason=taxon.get("statusReason"),external_comment=taxon.get("externalComment"),
        exclude_from_reporting=taxon.get("excludeFromReportingSystem"),
        is_microspecies=taxon.get("isMicrospecies"),relations=taxon.get("typedRelations"),
    )


def load_source(path):
    raw=Path(path).read_bytes()
    source=json.loads(raw)
    ids=[int(taxon["taxonId"]) for taxon in source["taxa"]]
    if (source.get("root_taxon_id")!=3000188 or len(ids)!=len(set(ids))
        or set(ids)!=set(source["child_ids"]) or not ids):
        raise ValueError("Incomplete Lepidoptera source")
    stamp=datetime.fromisoformat(source["retrieved_at"])
    if stamp.tzinfo is None:
        raise ValueError("Source timestamp lacks timezone")
    source["retrieved_at"]=stamp.astimezone(timezone.utc).isoformat()
    return source,hashlib.sha256(raw).hexdigest()


def record_source(db,path):
    source,digest=load_source(path)
    previous=db.execute("SELECT * FROM taxonomy_sources WHERE source_hash=?",(digest,)).fetchone()
    if previous:
        return previous["id"]
    current={str(taxon["taxonId"]):normalized_taxon(taxon) for taxon in source["taxa"]}
    latest=db.execute("SELECT * FROM taxonomy_sources ORDER BY retrieved_at DESC,id DESC LIMIT 1").fetchone()
    status="baseline" if latest is None else "compared"
    if latest and source["retrieved_at"]<latest["retrieved_at"]:
        status="archived_older"
    stamp=now()
    cursor=db.execute("""INSERT INTO taxonomy_sources(source_hash,retrieved_at,recorded_at,content_hash,
        taxon_count,data,source_path,comparison_status) VALUES(?,?,?,?,?,?,?,?)""",
        (digest,source["retrieved_at"],stamp,hashlib.sha256(encode(current).encode()).hexdigest(),
         len(current),zlib.compress(encode(current).encode()),str(Path(path).resolve()),status))
    source_id=cursor.lastrowid
    if latest is not None and status=="compared":
        before=json.loads(zlib.decompress(latest["data"]))
        for tid in sorted(before.keys()|current.keys(),key=int):
            old,new=before.get(tid),current.get(tid)
            if old==new:
                continue
            kind="source_added" if old is None else "source_removed" if new is None else "source_changed"
            fields=sorted(key for key in (old or {}).keys()|(new or {}).keys() if (old or {}).get(key)!=(new or {}).get(key))
            db.execute("""INSERT INTO taxonomy_history_events(phase,kind,taxon_id,label,event_at,recorded_at,
                source_id,fields_json,before_json,after_json) VALUES('source',?,?,?,?,?,?,?,?,?)""",
                (kind,int(tid),(new or old)["scientific_name"],source["retrieved_at"],stamp,source_id,
                 encode(fields),encode(old) if old is not None else None,encode(new) if new is not None else None))
    return source_id


def record_source_file(database,path):
    with history_db(database) as db:
        return record_source(db,path)


def apply_taxon(db, *, source_path, taxon_id, job_id, actor_id):
    prior=db.execute("SELECT * FROM taxonomy_update_receipts WHERE job_id=?",(job_id,)).fetchone()
    if prior:
        if prior["taxon_id"]!=taxon_id:
            raise ValueError("Job ID belongs to another taxon")
        return dict(prior)
    source,digest=load_source(source_path)
    age=(datetime.now(timezone.utc)-datetime.fromisoformat(source["retrieved_at"])).total_seconds()
    if not -300<=age<=3600:
        raise ValueError("Fetch a fresh source before updating")
    source_id=record_source(db,source_path)
    latest=db.execute("SELECT MAX(retrieved_at) FROM taxonomy_sources").fetchone()[0]
    if source["retrieved_at"]<latest:
        raise ValueError("A newer source is already recorded")
    taxon=next((row for row in source["taxa"] if row["taxonId"]==taxon_id),None)
    before=db.execute("SELECT * FROM taxa WHERE taxon_id=?",(taxon_id,)).fetchone()
    if taxon is None and before is None:
        raise ValueError("Taxon is outside both local and source scope")
    db.execute("INSERT INTO taxonomy_write_context VALUES(1,?,?,?)",(source_id,actor_id,job_id))
    event_before=db.execute("SELECT COALESCE(MAX(id),0) FROM taxonomy_history_events").fetchone()[0]
    if taxon is None:
        if before["is_active"]:
            db.execute("UPDATE taxa SET is_active=0,updated_at=? WHERE taxon_id=?",
                       (int(datetime.now(timezone.utc).timestamp()),taxon_id))
    else:
        normalized=normalized_taxon(taxon)
        active=int((normalized["category"],normalized["type"],normalized["status"])==("Species","Taxonomic","Accepted"))
        raw=encode(taxon)
        sha=hashlib.sha256(raw.encode()).hexdigest()
        if before is None:
            next_index=max(int(db.execute("SELECT value FROM meta WHERE key='next_local_index'").fetchone()[0]),
                           db.execute("SELECT COALESCE(MAX(local_index)+1,0) FROM taxa").fetchone()[0])
            db.execute("UPDATE meta SET value=? WHERE key='next_local_index'",(str(next_index+1),))
            db.execute("""INSERT INTO taxa(taxon_id,local_index,sci_name,swe_name,category,type,status,parent_id,
                is_active,sha256,updated_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (taxon_id,next_index,normalized["scientific_name"],normalized["swedish_name"],normalized["category"],
                 normalized["type"],normalized["status"],normalized["parent_id"],active,sha,
                 int(datetime.now(timezone.utc).timestamp()),raw))
        else:
            values=(normalized["scientific_name"],normalized["swedish_name"],normalized["category"],
                    normalized["type"],normalized["status"],normalized["parent_id"],active)
            old=tuple(before[key] for key in ("sci_name","swe_name","category","type","status","parent_id","is_active"))
            same_raw=before["raw_json"] and json.loads(before["raw_json"])==taxon
            if values!=old or not same_raw:
                db.execute("""UPDATE taxa SET sci_name=?,swe_name=?,category=?,type=?,status=?,parent_id=?,
                    is_active=?,sha256=?,updated_at=?,raw_json=? WHERE taxon_id=?""",
                    (*values,sha,int(datetime.now(timezone.utc).timestamp()),raw,taxon_id))
    event=db.execute("""SELECT id,kind FROM taxonomy_history_events
        WHERE phase='local' AND id>? AND job_id=? ORDER BY id DESC LIMIT 1""",(event_before,job_id)).fetchone()
    outcome=event["kind"] if event else "unchanged"
    db.execute("DELETE FROM taxonomy_write_context WHERE id=1")
    db.execute("INSERT INTO taxonomy_update_receipts VALUES(?,?,?,?,?,?,?)",
               (job_id,taxon_id,source_id,outcome,event["id"] if event else None,now(),actor_id))
    return dict(db.execute("SELECT * FROM taxonomy_update_receipts WHERE job_id=?",(job_id,)).fetchone())


def saved_sources(config,jobs_root):
    root=Path(jobs_root)
    paths=[Path(config["GEOMAP_DEFAULT_LISTS_DIR"])/"redlisted-lepidoptera/source/complete-run.json"]
    paths.extend(root.glob("*/source/complete-run.json"))
    paths.extend(root.glob("consolidations/*/taxonomy/source/complete-run.json"))
    paths.extend(root.glob("taxon-updates/*/fresh/source/complete-run.json"))
    sources=[]
    for path in paths:
        if path.is_file():
            source,_=load_source(path)
            sources.append((source["retrieved_at"],path))
    return sorted(sources,key=lambda item:item[0])


def bootstrap_history(config,jobs_root):
    with history_db(config["GEOMAP_DYNTAXA_DB"]) as db:
        for _,path in saved_sources(config,jobs_root):
            record_source(db,path)
        return dict(sources=db.execute("SELECT COUNT(*) FROM taxonomy_sources").fetchone()[0],
                    events=db.execute("SELECT COUNT(*) FROM taxonomy_history_events").fetchone()[0],
                    local_baseline=db.execute("SELECT COUNT(*) FROM taxonomy_local_baseline").fetchone()[0])
