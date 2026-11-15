"""Repair derived SQL name columns from stored recommended names, with audit/backup."""
import argparse
from contextlib import closing
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.taxonomy_history import history_db

def recommended(raw,kind):
    rows=[r["name"] for r in raw.get("names",[]) if r.get("isRecommended") and
          (r.get("category") or {}).get("value")==kind and r.get("name")]
    if len(rows)>1:
        raise ValueError("Multiple recommended names")
    return rows[0] if rows else ""

def plan(db):
    changes=[]
    for row in db.execute("SELECT taxon_id,sci_name,swe_name,raw_json FROM taxa ORDER BY taxon_id"):
        tid,sci,swe,raw=row
        data=json.loads(raw)
        after=[recommended(data,"ScientificName"),recommended(data,"SwedishName")]
        if not after[0]:
            raise ValueError("Recommended scientific name missing")
        if [sci or "",swe or ""]!=after:
            changes.append(dict(taxon_id=tid,before=[sci,swe],after=after,raw_hash=hashlib.sha256(raw.encode()).hexdigest()))
    return changes

def fingerprint(changes):
    return hashlib.sha256(json.dumps(changes,sort_keys=True).encode()).hexdigest()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--fingerprint",help="Required reviewed plan fingerprint for --apply")
    args=parser.parse_args()
    settings=json.loads(args.settings.read_text());path=Path(settings["GEOMAP_DYNTAXA_DB"])
    args.output.mkdir(mode=0o700,parents=True,exist_ok=True)
    with closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)) as db:
        changes=plan(db)
        digest=fingerprint(changes)
        (args.output/"plan.json").write_text(json.dumps(dict(fingerprint=digest,changes=changes),ensure_ascii=False,indent=2))
        if args.apply:
            if args.fingerprint!=digest:raise ValueError("Explicit reviewed fingerprint required")
            backup=args.output/"before.sqlite"
            if backup.exists():raise ValueError("Backup already exists")
            with closing(sqlite3.connect(backup)) as target:db.backup(target)
            backup.chmod(0o600)
    stamp=datetime.now(timezone.utc);job="name-repair-"+stamp.strftime("%Y%m%dT%H%M%SZ")
    if args.apply:
        with history_db(path) as db:
            if fingerprint(plan(db))!=digest:
                raise ValueError("Local state changed after preview")
            db.execute("INSERT INTO taxonomy_write_context VALUES(1,NULL,NULL,?)",(job,))
            for change in changes:
                db.execute("UPDATE taxa SET sci_name=?,swe_name=?,updated_at=? WHERE taxon_id=?",
                    (*change["after"],int(stamp.timestamp()),change["taxon_id"]))
            db.execute("DELETE FROM taxonomy_write_context WHERE id=1")
    result=dict(id=job,kind="name_projection_repair",applied=args.apply,rows=len(changes),
                scientific=sum(r["before"][0]!=r["after"][0] for r in changes),
                swedish=sum((r["before"][1] or "")!=r["after"][1] for r in changes),
                fingerprint=digest,at=stamp.isoformat())
    (args.output/"report.json").write_text(json.dumps(result,indent=2))
    print(json.dumps(result))
if __name__=="__main__":main()
