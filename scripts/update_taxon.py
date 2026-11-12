#!/usr/bin/env python3
"""Back up and update one local Dyntaxa taxon with a transactional audit trail."""
from contextlib import closing
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import argparse

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.taxonomy_history import saved_sources,load_source,history_db,apply_taxon
from geomap.data_validation import Validator


def write(path, data):
    temporary=path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data,ensure_ascii=False),encoding="utf-8")
    temporary.replace(path)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings",type=Path,required=True)
    parser.add_argument("--job",type=Path,required=True)
    parser.add_argument("--lock-fd",type=int,required=True)
    args=parser.parse_args()
    path=args.job/"report.json"
    report=json.loads(path.read_text())
    try:
        config=json.loads(args.settings.read_text())
        source_path=None
        for stamp,candidate in reversed(saved_sources(config,args.job.parents[1])):
            age=(datetime.now(timezone.utc)-datetime.fromisoformat(stamp)).total_seconds()
            if -300<=age<=3600:
                source_path=candidate
                break
        if source_path is None:
            report["phase"]="fetch_taxonomy"
            write(path,report)
            env={**os.environ,"ARTDATABANKEN_SUBSCRIPTION_KEY":config["ARTDATABANKEN_SUBSCRIPTION_KEY"]}
            with (args.job/"fetch.log").open("w") as log:
                subprocess.run([sys.executable,str(Path(__file__).with_name("build_redlist_default.py")),
                    "--output-dir",str(args.job/"fresh")],env=env,stdout=log,stderr=subprocess.STDOUT,
                    timeout=600,check=True)
            source_path=args.job/"fresh/source/complete-run.json"
        source,digest=load_source(source_path)
        report.update(phase="update_sqlite",source_at=source["retrieved_at"],source_hash=digest)
        write(path,report)
        backup=args.job/"before.sqlite"
        if backup.exists():
            raise ValueError("Backup already exists")
        with closing(sqlite3.connect(Path(config["GEOMAP_DYNTAXA_DB"]).resolve().as_uri()+"?mode=ro",uri=True)) as original:
            with closing(sqlite3.connect(backup)) as target:
                original.backup(target)
        with history_db(config["GEOMAP_DYNTAXA_DB"]) as db:
            receipt=apply_taxon(db,source_path=source_path,taxon_id=report["taxon_id"],
                               job_id=report["id"],actor_id=report.get("actor_id"))
        report.update(status="updated" if receipt["outcome"]!="unchanged" else "unchanged",
                      phase="complete",outcome=receipt["outcome"],event_id=receipt["event_id"])
        write(path,report)
        # Refresh the taxonomy status independently of the committed update.
        try:
            with Validator(config) as validator:
                report["results"]=[validator.validate("taxonomy")]
        except Exception as error:
            report["validation_error"]=type(error).__name__
    except Exception as error:
        report.update(status="failed",error=type(error).__name__)
    finally:
        report["finished_at"]=datetime.now(timezone.utc).isoformat()
        write(path,report)
        os.close(args.lock_fd)


if __name__=="__main__":
    main()
