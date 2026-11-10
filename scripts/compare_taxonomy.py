#!/usr/bin/env python3
"""Fetch and compare a Dyntaxa candidate without writing to active data."""
import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

from build_redlist_default import preferred_name, select_rows, category_code


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_report(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def taxonomy(snapshot):
    rows = {}
    for taxon in snapshot["taxa"]:
        tid = int(taxon["taxonId"])
        fields = {key: (taxon.get(key) or {}).get("value") for key in ("category", "type", "status")}
        fields["parent"] = taxon.get("parentId")
        fields["redlist"] = category_code(taxon.get("redlistCategory"))
        fields["scientific_name"] = preferred_name(taxon, "ScientificName")
        fields["swedish_name"] = preferred_name(taxon, "SwedishName")
        fields["names"] = sorted({
            ((name.get("category") or {}).get("value") or "", name["name"], bool(name.get("isRecommended")))
            for name in taxon.get("names", []) if name.get("name")
        })
        if tid in rows:
            raise ValueError("Duplicate taxon ID")
        rows[tid] = fields
    if len(rows) != len(snapshot["child_ids"]) or rows.keys() != set(snapshot["child_ids"]):
        raise ValueError("Incomplete taxonomy snapshot")
    return rows


def compare(before, after):
    added = sorted(after.keys() - before.keys())
    removed = sorted(before.keys() - after.keys())
    changed = [
        {"id": tid, "before": before[tid]["scientific_name"], "after": after[tid]["scientific_name"],
         "fields": [key for key in before[tid] if before[tid][key] != after[tid][key]]}
        for tid in sorted(before.keys() & after.keys()) if before[tid] != after[tid]
    ]
    return dict(added=len(added), removed=len(removed), changed=len(changed),
                unchanged=len(before.keys() & after.keys())-len(changed),
                added_preview=[{"id": tid, "name": after[tid]["scientific_name"]} for tid in added[:30]],
                removed_preview=[{"id": tid, "name": before[tid]["scientific_name"]} for tid in removed[:30]],
                changed_preview=changed[:30])


def list_impact(path, species):
    # Only aggregate coverage is reported. User IDs, names and memberships stay private.
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        lists = {}
        for list_id, taxon_id in db.execute("""SELECT l.id, t.taxon_id FROM lists l
                LEFT JOIN list_taxa t ON l.id=t.list_id WHERE l.visibility='private'"""):
            ids = lists.setdefault(list_id, set())
            if taxon_id is not None:
                ids.add(taxon_id)
        impact = {"checked": len(lists), "needs_review": sum(bool(ids-species) for ids in lists.values()),
                  "out_of_scope_entries": sum(len(ids-species) for ids in lists.values())}
        has_imports = db.execute("SELECT 1 FROM sqlite_master WHERE name='list_imports'").fetchone()
        sample = db.execute("""SELECT l.id,l.owner_id FROM lists l JOIN list_imports i ON l.id=i.list_id
            WHERE l.visibility='private' AND i.source_type='observed-species-csv' ORDER BY l.id LIMIT 1""").fetchone() if has_imports else None
        if sample:
            list_id, owner = sample
            observed_path = path.parent / "user-lists" / str(owner) / list_id / "observed.csv"
            if observed_path.is_file():
                with observed_path.open(encoding="utf-8", newline="") as f:
                    observed = {int(row["taxon_id"]) for row in csv.DictReader(f)}
                if observed <= species:
                    candidate = species-observed
                    impact["sample_difference"] = {"added": len(candidate-lists[list_id]),
                                                   "removed": len(lists[list_id]-candidate)}
        return impact


def run(job, settings):
    config = read_json(settings)
    defaults = Path(config["GEOMAP_DEFAULT_LISTS_DIR"])
    active_catalogue = read_json(defaults/"lepidoptera-species.json")
    baseline_path = defaults/"redlisted-lepidoptera/source/complete-run.json"
    baseline_bytes = baseline_path.read_bytes()
    import hashlib
    if hashlib.sha256(baseline_bytes).hexdigest() != active_catalogue["source_sha256"]:
        raise ValueError("Active catalogue does not match its archived source")
    baseline = json.loads(baseline_bytes)
    env = {**os.environ, "ARTDATABANKEN_SUBSCRIPTION_KEY": config["ARTDATABANKEN_SUBSCRIPTION_KEY"]}
    with (job/"fetch.log").open("w", encoding="utf-8") as log:
        subprocess.run([sys.executable, str(Path(__file__).with_name("build_redlist_default.py")),
                        "--output-dir", str(job)], env=env, stdout=log, stderr=subprocess.STDOUT,
                       timeout=600, check=True)
    source = read_json(job/"source/complete-run.json")
    if source["root_taxon_id"] != baseline["root_taxon_id"]:
        raise ValueError("Lepidoptera root differs from baseline")
    before, after = taxonomy(baseline), taxonomy(source)
    species = {tid for tid, row in after.items()
               if (row["category"],row["type"],row["status"]) == ("Species","Taxonomic","Accepted")}
    current_species = {row["taxon_id"] for row in active_catalogue["species"]}
    old_red = {row["taxon_id"]: category_code(row["redlist_category"]) for row in select_rows(baseline["taxa"])}
    new_red = {row["taxon_id"]: category_code(row["redlist_category"]) for row in select_rows(source["taxa"])}
    delta = compare(before, after)
    return dict(status="changed" if any(delta[key] for key in ("added","removed","changed")) else "unchanged",
                baseline_at=active_catalogue["retrieved_at"], source_at=source["retrieved_at"],
                baseline_sha256=active_catalogue["source_sha256"],
                candidate_sha256=hashlib.sha256((job/"source/complete-run.json").read_bytes()).hexdigest(),
                taxonomy=delta, species={"total":len(species),"added":len(species-current_species),
                                         "removed":len(current_species-species)},
                redlist={"added":len(new_red.keys()-old_red.keys()),"removed":len(old_red.keys()-new_red.keys()),
                         "category_changed":sum(new_red[tid]!=old_red[tid] for tid in old_red.keys() & new_red.keys())},
                lists=list_impact(Path(config["GEOMAP_SHARED_DB"]),species))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--settings", type=Path, required=True)
    parser.add_argument("--lock-fd", type=int, required=True)
    args = parser.parse_args()
    path = args.job/"report.json"
    report = read_json(path)
    try:
        report.update(run(args.job,args.settings))
    except subprocess.TimeoutExpired:
        report.update(status="failed", error="timeout")
    except Exception as error:
        report.update(status="failed", error=type(error).__name__)
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_report(path, report)
        os.close(args.lock_fd)
