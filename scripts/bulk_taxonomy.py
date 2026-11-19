"""Fixed beta bulk-taxonomy worker; preview and apply use the same single-writer lock."""
import argparse
from contextlib import closing
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.taxonomy_bulk import prepare,apply
from geomap.taxonomy_history import history_db
from geomap.data_validation import Validator

def write(path,body):
    tmp=path.with_suffix(".tmp");tmp.write_text(json.dumps(body),encoding="utf-8");tmp.replace(path)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--settings",type=Path,required=True);p.add_argument("--job",type=Path,required=True)
    p.add_argument("--mode",choices=("prepare","apply"),required=True);p.add_argument("--lock-fd",type=int,required=True)
    args=p.parse_args();path=args.job/"report.json";report=json.loads(path.read_text())
    try:
        config=json.loads(args.settings.read_text());source=args.job/"fresh/source/complete-run.json"
        if args.mode=="prepare":
            report["phase"]="fetch_taxonomy";write(path,report)
            env={**os.environ,"ARTDATABANKEN_SUBSCRIPTION_KEY":config["ARTDATABANKEN_SUBSCRIPTION_KEY"]}
            with (args.job/"fetch.log").open("w") as log:
                subprocess.run([sys.executable,str(Path(__file__).with_name("build_redlist_default.py")),
                    "--output-dir",str(args.job/"fresh")],env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600,check=True)
            with closing(sqlite3.connect(Path(config["GEOMAP_DYNTAXA_DB"]).resolve().as_uri()+"?mode=ro",uri=True)) as db:
                db.row_factory=sqlite3.Row;db.execute("BEGIN");plan=prepare(db,source)
            write(args.job/"plan.json",plan)
            report.update(status="ready",phase="review",summary={k:v for k,v in plan.items() if k not in {"changes","baseline"}},
                          examples=plan["changes"][:20])
        else:
            plan=json.loads((args.job/"plan.json").read_text())
            if report["requested_fingerprint"]!=plan["fingerprint"] or (plan["suspicious"] and not report.get("confirmed_large")):
                raise ValueError("Reviewed preview required")
            with closing(sqlite3.connect(Path(config["GEOMAP_DYNTAXA_DB"]).resolve().as_uri()+"?mode=ro",uri=True)) as db:
                backup=args.job/"before.sqlite"
                if not backup.exists():
                    with closing(sqlite3.connect(backup)) as target:db.backup(target)
                    backup.chmod(0o600)
            with history_db(config["GEOMAP_DYNTAXA_DB"]) as db:
                receipt=apply(db,source,plan,report["id"],report["actor_id"])
            report.update(status="updated",phase="complete",receipt=receipt)
            write(path,report)
            try:
                with Validator(config) as validator:report["results"]=[validator.validate("taxonomy")]
            except Exception as error:report["validation_error"]=type(error).__name__
    except Exception as error:
        report.update(status="failed",error=type(error).__name__)
    finally:
        report["finished_at"]=datetime.now(timezone.utc).isoformat();write(path,report);os.close(args.lock_fd)
if __name__=="__main__":main()
