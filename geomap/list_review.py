"""Preserving list taxonomy reviews, bound to source facts and original membership."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from geomap.taxonomy_history import saved_sources, load_source, normalized_taxon

SCHEMA = """CREATE TABLE IF NOT EXISTS list_taxonomy_reviews(
 id TEXT PRIMARY KEY, list_id TEXT NOT NULL, taxon_id INTEGER NOT NULL,
 actor_id INTEGER NOT NULL, action TEXT NOT NULL, note TEXT NOT NULL,
 created_at TEXT NOT NULL, source_hash TEXT NOT NULL, source_at TEXT NOT NULL,
 basis_hash TEXT NOT NULL, before_hash TEXT NOT NULL, after_hash TEXT NOT NULL,
 before_json TEXT NOT NULL, after_json TEXT NOT NULL, additions_json TEXT NOT NULL,
 source_json TEXT NOT NULL, backup TEXT NOT NULL, owner_id INTEGER, visibility TEXT, title TEXT);
 CREATE TABLE IF NOT EXISTS list_taxonomy_state(
 list_id TEXT PRIMARY KEY REFERENCES lists(id) ON DELETE CASCADE,
 needs_regeneration INTEGER NOT NULL, review_id TEXT NOT NULL, updated_at TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS list_review_lookup ON list_taxonomy_reviews(list_id,taxon_id,created_at);
"""

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def content_hash(ids):
    return hashlib.sha256(json.dumps(sorted(ids)).encode()).hexdigest()

def source_context(settings, jobs, tid):
    sources = sorted(saved_sources(settings, Path(jobs)))
    if not sources:
        raise ValueError("source_unavailable")
    source, sha = load_source(sources[-1][1])
    taxa = {int(t["taxonId"]):t for t in source["taxa"]}
    root = taxa.get(tid)
    if root is None:
        raise ValueError("taxon_absent_in_source")
    candidates_by_id = {}
    linked_facts = []
    # Explicit source links may end at a subspecies. Follow its primary ancestry
    # to the accepted species used by our species-based catalogue.
    for linked in taxa.values():
        if linked.get("parentId")!=tid and tid not in (linked.get("secondaryParents") or []):
            continue
        linked_facts.append(dict(taxon_id=linked["taxonId"],**normalized_taxon(linked)))
        target=linked
        seen=set()
        while target and target["taxonId"] not in seen:
            seen.add(target["taxonId"])
            if (target.get("category") or {}).get("value")=="Species":
                break
            if (target.get("category") or {}).get("value") not in {"Subspecies","Variety","Form"}:
                target=None
                break
            target=taxa.get(target.get("parentId"))
        if target and (target.get("category") or {}).get("value")=="Species" and (
            (target.get("type") or {}).get("value")=="Taxonomic" and
            (target.get("status") or {}).get("value")=="Accepted"):
            target_id=int(target["taxonId"])
            candidate=candidates_by_id.setdefault(target_id,dict(taxon_id=target_id,
                **normalized_taxon(target),relation_taxa=[]))
            candidate["relation_taxa"].append(dict(taxon_id=linked["taxonId"],
                scientific_name=normalized_taxon(linked)["scientific_name"]))
    candidates=sorted(candidates_by_id.values(),key=lambda row:row["taxon_id"])
    for candidate in candidates:
        candidate["relation_taxa"].sort(key=lambda row:row["taxon_id"])
    linked_facts.sort(key=lambda row:row["taxon_id"])
    facts=dict(taxon_id=tid,**normalized_taxon(root))
    basis=dict(taxon=facts,candidates=candidates,linked_taxa=linked_facts)
    return dict(**basis,basis_hash=digest(basis),source_hash=sha,source_at=source["retrieved_at"])

def members(db,list_id):
    return [r[0] for r in db.execute("SELECT taxon_id FROM list_taxa WHERE list_id=? ORDER BY taxon_id",(list_id,))]

def authorized_list(db,list_id,actor):
    row=db.execute("SELECT * FROM lists WHERE id=?",(list_id,)).fetchone()
    # Private-list ownership is mandatory even for administrators.
    if row is None or (row["visibility"]=="private" and row["owner_id"]!=actor):
        raise PermissionError("list_unavailable")
    return dict(row)

def latest_review(db,list_id,tid):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='list_taxonomy_reviews'").fetchone():
        return None
    cursor=db.execute("SELECT * FROM list_taxonomy_reviews WHERE list_id=? AND taxon_id=? ORDER BY created_at DESC,id DESC LIMIT 1",(list_id,tid))
    row=cursor.fetchone()
    return dict(zip([column[0] for column in cursor.description],row)) if row else None

def approved(db,list_id,tid,context,ids):
    review=latest_review(db,list_id,tid)
    return bool(review and review["basis_hash"]==context["basis_hash"] and tid in ids
                and set(json.loads(review["additions_json"])).issubset(ids))

def preview(db,settings,jobs,list_id,tid,actor):
    item=authorized_list(db,list_id,actor)
    ids=members(db,list_id)
    if tid not in ids:
        raise ValueError("taxon_not_in_list")
    context=source_context(settings,jobs,tid)
    if item["content_hash"]!=content_hash(ids):
        raise ValueError("list_hash_mismatch")
    token=digest(dict(list_id=list_id,ids=ids,owner=item["owner_id"],visibility=item["visibility"],
                      basis=context["basis_hash"],source=context["source_hash"]))
    review=latest_review(db,list_id,tid)
    return dict(list_id=list_id,title=item["title"],taxon_id=tid,members=ids,
                content_hash=item["content_hash"],owner_id=item["owner_id"],visibility=item["visibility"],token=token,context=context,
                approved=approved(db,list_id,tid,context,ids),review=review)

def apply(db,settings,jobs,list_id,tid,actor,body,backup):
    p=preview(db,settings,jobs,list_id,tid,actor)
    if body["token"]!=p["token"]:
        raise ValueError("stale_preview")
    action=body["action"]
    additions=body["additions"]
    allowed={t["taxon_id"]:t for t in p["context"]["candidates"]}
    if action not in {"keep","add"} or (action=="keep" and additions) or (
        action=="add" and (not additions or not set(additions).issubset(allowed))):
        raise ValueError("invalid_additions")
    if p["approved"]:
        raise ValueError("already_reviewed")
    before=p["members"]
    after=sorted(set(before)|set(additions))
    stamp=datetime.now(timezone.utc)
    now=stamp.isoformat()
    receipt=stamp.strftime("%Y%m%dT%H%M%SZ")+"-"+uuid4().hex[:8]
    for target in additions:
        taxon=allowed[target]
        db.execute("""INSERT INTO taxa(taxon_id,scientific_name,swedish_name) VALUES(?,?,?)
            ON CONFLICT(taxon_id) DO UPDATE SET scientific_name=excluded.scientific_name,swedish_name=excluded.swedish_name""",
            (target,taxon["scientific_name"],taxon["swedish_name"]))
        db.execute("INSERT OR IGNORE INTO list_taxa(list_id,taxon_id) VALUES(?,?)",(list_id,target))
    after_hash=content_hash(after)
    db.execute("UPDATE lists SET content_hash=? WHERE id=?",(after_hash,list_id))
    db.execute("""INSERT INTO list_taxonomy_reviews VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (receipt,list_id,tid,actor,action,body["note"],now,p["context"]["source_hash"],
         p["context"]["source_at"],p["context"]["basis_hash"],p["content_hash"],after_hash,
         json.dumps(before),json.dumps(after),json.dumps(sorted(additions)),
         json.dumps(p["context"],ensure_ascii=False),str(backup),p["owner_id"],p["visibility"],p["title"]))
    if after!=before:
        db.execute("""INSERT INTO list_taxonomy_state VALUES(?,1,?,?) ON CONFLICT(list_id)
            DO UPDATE SET needs_regeneration=1,review_id=excluded.review_id,updated_at=excluded.updated_at""",
            (list_id,receipt,now))
        db.execute("UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")
    return dict(id=receipt,kind="list_taxonomy_review",status="updated",created_at=now,
                list_id=list_id,taxon_id=tid,action=action,added=sorted(set(after)-set(before)),
                original_preserved=True,map_rebuild_required=after!=before)
