#!/usr/bin/env python3

# script:import_csv_export.py 

# MIT License
#
# Copyright (c) 2025 Jonas Waldeck
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import sys
import zipfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional, List, Set

# If you run inside the repo, this should work (same pattern as server/app.py)
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geomap import storage  
from geomap.config import Config
from geomap.sos_export import export_csv_zip_to_file
from geomap.csv_import import (
    IngestArgs,
    find_csv_inside_zip,
    import_observations_raw,
    consolidate_taxon_grid_from_raw,
)
from geomap.taxon_lists import read_taxon_ids_from_csv, chunked
from geomap.sos_filters_ext import make_sos_export_filter

SLOT_ALL = 0



def slot_from_yyyy_mm_dd(s: str) -> int:
    return slot_from_date(parse_yyyy_mm_dd(s))

    
def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch SOS CSV exports in taxon batches and import into geomap.sqlite.")

    ap.add_argument("--db", required=True, type=str, help="Path to geomap.sqlite")
    ap.add_argument("--zooms", default="15,14,13", type=str)
    
    ap.add_argument("--taxon-list-csv", required=True, type=str)
    ap.add_argument("--batch-size", default=100, type=int)
    
    ap.add_argument("--year-from", required=True, type=int)
    ap.add_argument("--year-to", required=True, type=int)
    
    ap.add_argument("--csv-stash-dir", default="", type=str)
    ap.add_argument("--import-existing", default="", type=str, help="Debug: import an existing ZIP/CSV instead of fetching SOS")
    
    ap.add_argument("--include-slot0", action="store_true")
    ap.add_argument("--date-field", default="StartDate", choices=["StartDate", "EndDate"])
    ap.add_argument("--occurrence-status", default="present", type=str)


    args0 = ap.parse_args()

    db_path = Path(args0.db).expanduser().resolve()
    taxon_list_csv = Path(args0.taxon_list_csv).expanduser().resolve()

    zooms = [int(z.strip()) for z in args0.zooms.split(",") if z.strip()]
    zooms = sorted(set(zooms), reverse=True)
    if not zooms:
        raise SystemExit("No zooms provided")

    taxon_ids_all = read_taxon_ids_from_csv(taxon_list_csv)
    if not taxon_ids_all:
        raise SystemExit(f"No taxon ids found in {taxon_list_csv}")

    print(f"[import] loaded {len(taxon_ids_all)} taxon ids from {taxon_list_csv}")

    batches = list(chunked(taxon_ids_all, int(args0.batch_size)))
    occ = args0.occurrence_status.strip() or None

    cfg = Config(repo_root=REPO_ROOT)
    
    stash_dir = (
        Path(args0.csv_stash_dir).expanduser().resolve()
        if args0.csv_stash_dir.strip()
        else cfg.csv_stash_dir
    )
    stash_dir.mkdir(parents=True, exist_ok=True)
    
    conn = storage.connect(db_path)
    conn.isolation_level = None

    try:
        storage.ensure_schema(conn)
        
        total_touched_scopes = 0
        total_layers_written = 0

        for batch_index, batch_taxon_ids in enumerate(batches, start=1):
            print(f"[import] batch {batch_index}/{len(batches)} taxa={len(batch_taxon_ids)}")

            if args0.import_existing.strip():
                export_path = Path(args0.import_existing).expanduser().resolve()
                if not export_path.exists():
                    raise SystemExit(f"Missing --import-existing file: {export_path}")
            else:
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                export_path = stash_dir / f"{stamp}__batch{batch_index:04d}_taxa{len(batch_taxon_ids)}.zip"

                search_filter = make_sos_export_filter(
                    taxon_ids=batch_taxon_ids,
                    year_from=int(args0.year_from),
                    year_to=int(args0.year_to),
                )

                print(f"[export] batch {batch_index}: writing {export_path}")
                export_csv_zip_to_file(
                    cfg,
                    search_filter,
                    export_path,
                    output_field_set="All",
                    gzip=True,
                    culture_code="sv-SE",
                )
            
            if export_path.stat().st_size == 0:
                print(f"[import] batch {batch_index}: empty export, skipping")
                continue

            csv_or_zip = export_path
            if csv_or_zip.suffix.lower() == ".zip":
                extracted = find_csv_inside_zip(csv_or_zip)
                print(f"[import] batch {batch_index}: extracted CSV: {extracted}")
                csv_or_zip = extracted

            ingest = IngestArgs(
                zip_or_csv=csv_or_zip,
                db_path=db_path,
                zooms=zooms,
                taxon_ids=batch_taxon_ids,
                include_slot0=bool(args0.include_slot0),
                date_field=args0.date_field,
                occurrence_status=occ,
            )

            touched = import_observations_raw(conn, ingest)
            print(f"[import] batch {batch_index}: touched raw scopes: {len(touched)}")

            if not touched:
                continue

            years = sorted({k[0] for k in touched})
            slot_ids = sorted({k[1] for k in touched})
            zooms_scope = sorted({k[2] for k in touched}, reverse=True)
            taxon_ids_scope = sorted({k[3] for k in touched})

            layers_written = consolidate_taxon_grid_from_raw(
                conn,
                taxon_ids=taxon_ids_scope,
                years=years,
                slot_ids=slot_ids,
                zooms=zooms_scope,
                include_slot0=bool(args0.include_slot0),
            )
            
            total_touched_scopes += len(touched)
            total_layers_written += layers_written
            
            print(f"[import] batch {batch_index}: wrote consolidated layers: {layers_written}")
            
        print(f"[import] total touched raw scopes: {total_touched_scopes}")
        print(f"[import] total consolidated layers: {total_layers_written}")
        print("[import] done.")
        return 0

    finally:
        conn.close()
    
if __name__ == "__main__":
    raise SystemExit(main())
