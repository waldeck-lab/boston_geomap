#!/usr/bin/env python3
"""Validate configured GeoMap databases read-only and persist a report."""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.data_validation import DATASETS, Validator


def write(path, data):
    temporary=path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data,ensure_ascii=False),encoding="utf-8")
    temporary.replace(path)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings",type=Path,required=True)
    parser.add_argument("--job",type=Path,required=True)
    parser.add_argument("--dataset",choices=(*DATASETS,"all"),default="all")
    parser.add_argument("--lock-fd",type=int)
    args=parser.parse_args()
    lock=None
    if args.lock_fd is None:
        lock=(args.job.parent/"validation.lock").open("a")
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    path=args.job/"report.json"
    report=json.loads(path.read_text()) if path.exists() else {
        "id":args.job.name,"kind":"local_validation","dataset":args.dataset,
        "started_at":datetime.now(timezone.utc).isoformat(),"actor_id":None,
    }
    report.update(status="running", results=[])
    write(path,report)
    datasets=DATASETS if args.dataset=="all" else (args.dataset,)
    try:
        settings=json.loads(args.settings.read_text())
        with Validator(settings) as validator:
            for dataset in datasets:
                try:
                    result=validator.validate(dataset)
                except Exception as error:
                    result={"id":dataset,"status":"failed","checks":[{"code":"validation_error","status":"failed"}],
                            "metrics":{},"error":type(error).__name__}
                report["results"].append(result)
                write(path,report)
        statuses={result["status"] for result in report["results"]}
        report["status"]=next((status for status in ("failed","unavailable","warning") if status in statuses),"passed")
    except Exception as error:
        report.update(status="failed",error=type(error).__name__,results=[
            {"id":dataset,"status":"failed","checks":[{"code":"validation_error","status":"failed"}],"metrics":{}}
            for dataset in datasets])
    finally:
        report["finished_at"]=datetime.now(timezone.utc).isoformat()
        write(path,report)
        if args.lock_fd is not None:
            os.close(args.lock_fd)
        if lock:
            lock.close()


if __name__=="__main__":
    main()
