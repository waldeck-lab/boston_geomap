#!/usr/bin/env python3
"""Prepare and rehearse one public Artportalen taxon-complex reconciliation."""
import argparse
from collections import Counter
from contextlib import closing
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo
import sqlite3
import subprocess
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.consolidation import build_plan, complex_scope, local_slice, apply_plan, taxon_name
from geomap.shared_store import connect
from geomap.sos_reconciliation import PublicSOSReader
from geomap.taxonomy_history import record_source_file


def write(path, value):
    temporary=path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value,ensure_ascii=False),encoding="utf-8")
    temporary.replace(path)


def backup(source, target):
    if target.exists():
        raise ValueError("Rehearsal target already exists")
    with closing(sqlite3.connect(source.resolve().as_uri()+"?mode=ro",uri=True)) as original:
        with closing(sqlite3.connect(target)) as clone:
            original.backup(clone)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings",type=Path,required=True)
    parser.add_argument("--job",type=Path,required=True)
    parser.add_argument("--lock-fd",type=int,required=True)
    args=parser.parse_args()
    report_path=args.job/"report.json"
    report=json.loads(report_path.read_text())
    def progress(**values):
        report.update(values)
        write(report_path,report)
    try:
        files=[Path(__file__),Path(__file__).parents[1]/"geomap/consolidation.py",
               Path(__file__).parents[1]/"geomap/sos_reconciliation.py"]
        progress(worker_sha256=hashlib.sha256(b"".join(path.read_bytes() for path in files)).hexdigest())
        config=json.loads(args.settings.read_text())
        parameters=json.loads((args.job/"scope.json").read_text())
        roots=parameters["taxon_ids"]
        first=date.fromisoformat(parameters["date_from"])
        last=date.fromisoformat(parameters["date_to"])
        if first>last or first<date(1900,1,1) or last>datetime.now(ZoneInfo("Europe/Stockholm")).date():
            raise ValueError("Invalid date scope")
        progress(phase="taxonomy",date_from=first.isoformat(),date_to=last.isoformat(),taxon_ids=roots)
        env={**os.environ,"ARTDATABANKEN_SUBSCRIPTION_KEY":config["ARTDATABANKEN_SUBSCRIPTION_KEY"]}
        with (args.job/"taxonomy-fetch.log").open("w") as log:
            subprocess.run([sys.executable,str(Path(__file__).with_name("build_redlist_default.py")),
                "--output-dir",str(args.job/"taxonomy")],env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600,check=True)
        source_path=args.job/"taxonomy/source/complete-run.json"
        raw=source_path.read_bytes()
        source=json.loads(raw)
        returned=[int(row["taxonId"]) for row in source["taxa"]]
        if len(returned)!=len(set(returned)) or set(returned)!=set(source["child_ids"]):
            raise ValueError("Incomplete taxonomy snapshot")
        record_source_file(config["GEOMAP_DYNTAXA_DB"],source_path)
        scope,mapping,taxa=complex_scope(source["taxa"],roots)
        progress(phase="snapshot_local",scope_taxa=scope,
                 taxa=[dict(id=tid,name=taxon_name(taxa[tid],"ScientificName")) for tid in scope])
        baseline=args.job/"baseline.sqlite"
        backup(Path(config["GEOMAP_SHARED_DB"]),baseline)
        with connect(baseline,readonly=True) as db:
            old=local_slice(db,dict(scope_taxa=scope,date_from=first.isoformat(),date_to=last.isoformat()))
        reader=PublicSOSReader(config["ARTDATABANKEN_SUBSCRIPTION_KEY"],progress)
        records,excluded=reader.stable_snapshot(scope,mapping,first.isoformat(),last.isoformat())
        reader.confirm_missing([row["occurrence_id"] for row in old],records,excluded,mapping,first.isoformat(),last.isoformat())
        with connect(baseline,readonly=True) as db:
            plan=build_plan(db,job_id=report["id"],roots=roots,scope=scope,mapping=mapping,taxa=taxa,
                records=records,excluded=excluded,start=first.isoformat(),end=last.isoformat(),
                source_hash=hashlib.sha256(raw).hexdigest())
        write(args.job/"plan.json",plan)
        progress(phase="rehearsal",summary=plan["summary"],
                 exclusions=dict(Counter(row["reason"] for row in excluded.values())))
        trial=args.job/"rehearsal.sqlite"
        backup(baseline,trial)
        with connect(trial) as db:
            receipt=apply_plan(db,plan)
            if apply_plan(db,plan)!=receipt:
                raise ValueError("Rehearsal retry changed the result")
            if db.execute("PRAGMA quick_check").fetchone()[0]!="ok":
                raise ValueError("Rehearsal SQLite integrity failed")
        progress(status="ready",phase="complete",plan_hash=plan["plan_hash"],
                 rehearsal=dict(passed=True,revision=receipt["revision"],
                                affected_observations=receipt["affected_observations"]),
                 source_at=datetime.now(timezone.utc).isoformat())
    except Exception as error:
        progress(status="failed",error=type(error).__name__)
    finally:
        report["finished_at"]=datetime.now(timezone.utc).isoformat()
        write(report_path,report)
        os.close(args.lock_fd)


if __name__=="__main__":
    main()
