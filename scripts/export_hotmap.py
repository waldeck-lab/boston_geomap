#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2026 Jonas Waldeck
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

# -*- coding: utf-8 -*-

# script:export_hotmap.py 

from __future__ import annotations

import sys
from pathlib import Path
import argparse
import os

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.cli_paths import apply_path_overrides

from geomap.config import Config
from geomap.config import SLOT_MIN, SLOT_MAX, SLOT_ALL
from geomap.logging_utils import setup_logger
from geomap import storage
from geomap.export_geojson import export_hotmap_geojson
from geomap.export_csv import export_top_sites_csv  
from geomap.storage import YEAR_ALL


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zoom", type=int, default=15)
    ap.add_argument("--slot", type=int, default=SLOT_ALL, help=f"Calendar slot id: {SLOT_ALL}=all-time, 1..{SLOT_MAX}=time buckets")
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--alpha", type=float, default=None)
    ap.add_argument("--beta", type=float, default=None)
    ap.add_argument("--db-dir", default=None)
    ap.add_argument("--lists-dir", default=None)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--cache-dir", default=None)
    ap.add_argument("--logs-dir", default=None)
    ap.add_argument("--year", type=int, default=YEAR_ALL, help="Year bucket. 0 = all-years aggregate.")

    return ap.parse_args()
    

def main() -> int:
    args = parse_args()
    
    apply_path_overrides(
        db_dir=args.db_dir,
        lists_dir=args.lists_dir,
        geomap_lists_dir=args.out_dir,
        cache_dir=args.cache_dir,
        logs_dir=args.logs_dir,
    )

    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("export_hotmap", cfg.logs_dir)

    zoom = args.zoom
    logger.info("Zoom: %d", zoom)
    slot_id = args.slot
    if slot_id == SLOT_ALL:
        logger.info("Slot: %d (all-time aggregate)", slot_id)
    else:
        logger.info("Slot: %d (calendar bucket 1..%d)", slot_id, SLOT_MAX)
    if slot_id < SLOT_MIN or slot_id > SLOT_MAX:
        logger.error(
            "slot_id out of range: %d (valid: %d..%d, where %d = all-time)",
            slot_id, SLOT_MIN, SLOT_MAX, SLOT_ALL
        )
        return 2

    logger.info("Slot: %d", slot_id)

    year = int(args.year)
    logger.info("Year: %d", year)
    
    out_dir = cfg.geomap_lists_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_geojson = out_dir / f"hotmap_zoom{zoom}_year{year}_slot{slot_id}.geojson"
    out_csv = out_dir / f"top_sites_zoom{zoom}_year{year}_slot{slot_id}.csv"

    logger.info("Exporting slot_id: %d", slot_id)
    logger.info("Exporting hotmap to: %s", out_geojson)
    logger.info("Exporting top sites to: %s", out_csv)

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)
        export_hotmap_geojson(conn, zoom, year, slot_id, out_geojson)
        export_top_sites_csv(conn, zoom, year, slot_id, out_csv, limit=200)
        logger.info("Export complete.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
