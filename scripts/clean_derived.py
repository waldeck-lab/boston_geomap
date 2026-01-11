#!/usr/bin/env python3

#  Desc.:Remove derived files that can be re-generated locally 

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

# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# script:clean_derived.py
# Desc.: Remove derived files/rows that can be re-generated locally

from __future__ import annotations

import argparse
import os
import sys
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geomap.cli_paths import apply_path_overrides
from geomap.config import Config
from geomap.logging_utils import setup_logger
from geomap import storage
from geomap.storage import YEAR_ALL



def _ensure_schema_or_nuke_db(cfg: Config, logger) -> None:
    """
    Ensure schema exists. If we detect an old/incompatible DB (e.g. missing 'year' column),
    we delete the DB and recreate it from schema.sql.
    """
    db_path = Path(cfg.geomap_db_path)

    # If DB doesn't exist yet, just create it by ensuring schema.
    if not db_path.exists():
        conn = storage.connect(db_path)
        try:
            storage.ensure_schema(conn)
        finally:
            conn.close()
        return

    # Try normal schema apply first
    conn = storage.connect(db_path)
    try:
        try:
            storage.ensure_schema(conn)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            # Common symptoms of "old DB" when schema adds year-aware views/indexes
            incompatible = (
                "no such column: year" in msg
                or "no such column: slot_id" in msg
                or "no such table: grid_hotmap" in msg  # sometimes old DBs differ
            )
            if not incompatible:
                raise

            logger.warning("Detected incompatible/old DB schema (%s). Recreating DB: %s", e, db_path)

    finally:
        conn.close()

    # Nuke DB and recreate
    try:
        db_path.unlink()
    except FileNotFoundError:
        pass

    conn2 = storage.connect(db_path)
    try:
        storage.ensure_schema(conn2)
    finally:
        conn2.close()

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zoom", type=int, default=None, help="Limit cleanup to this zoom (optional)")
    ap.add_argument("--slot", type=int, default=None, help="Limit cleanup to this slot (optional)")
    ap.add_argument("--year", type=int, default=YEAR_ALL, help="Year bucket (0=all-years). Use -1 to mean ALL years.")
    ap.add_argument("--keep-zoom", type=int, default=15, help="Keep this zoom in taxon_grid/layer_state (base zoom).")

    ap.add_argument("--hotmap", action="store_true", help="Delete grid_hotmap + hotmap_taxa_set")
    ap.add_argument("--exports", action="store_true", help="Delete exported geojson/csv files")
    ap.add_argument("--derived-zooms", action="store_true", help="Delete derived zoom caches in DB")
    ap.add_argument("--all", action="store_true", help="Do all cleanup types")

    ap.add_argument("--db-dir", default=None, help="Override DB dir (expects geomap.sqlite there)")
    ap.add_argument("--out-dir", default=None, help="Override geomap lists dir (export drop area)")
    ap.add_argument("--logs-dir", default=None, help="Override logs dir")
    return ap.parse_args()

def _delete_exports(out_dir: Path, zoom: int | None, slot: int | None, year: int | None) -> int:
    if not out_dir.exists():
        return 0

    deleted = 0
    for p in out_dir.iterdir():
        if not p.is_file():
            continue
        name = p.name

        # Accept both old and new naming, so you can purge everything confidently.
        # New:
        #   hotmap_zoom{z}_year{y}_slot{s}.geojson
        #   top_sites_zoom{z}_year{y}_slot{s}.csv
        # Old:
        #   hotmap_zoom{z}_slot{s}.geojson
        #   top_sites_zoom{z}_slot{s}.csv
        def match_tokens(prefix: str, ext: str) -> tuple[int | None, int | None, int | None] | None:
            if not (name.startswith(prefix) and name.endswith(ext)):
                return None
            core = name[len(prefix) : -len(ext)]
            # core could be: "{z}_year{y}_slot{s}" or "{z}_slot{s}"
            parts = core.split("_")
            if not parts:
                return None

            z = None
            y = None
            s = None

            # first token should be zoom
            if parts[0].isdigit():
                z = int(parts[0])
            else:
                return None

            # parse remaining tokens
            for token in parts[1:]:
                if token.startswith("year") and token[4:].isdigit():
                    y = int(token[4:])
                elif token.startswith("slot") and token[4:].isdigit():
                    s = int(token[4:])
                else:
                    return None

            if s is None:
                return None
            return (z, y, s)

        parsed = (
            match_tokens("hotmap_zoom", ".geojson")
            or match_tokens("top_sites_zoom", ".csv")
        )
        if not parsed:
            continue

        z_file, y_file, s_file = parsed

        if zoom is not None and z_file != zoom:
            continue
        if slot is not None and s_file != slot:
            continue
        # If caller specifies a year filter, only delete year-tagged files that match.
        # For old files without year, treat y_file as None and only delete them when year filter is None.
        if year is not None:
            if y_file is None:
                continue
            if y_file != year:
                continue

        try:
            p.unlink()
            deleted += 1
        except OSError:
            pass

    return deleted

def main() -> int:
    args = parse_args()

    # Treat year = -1 as "all years" for DB filters
    year_filter: int | None
    if args.year == -1:
        year_filter = None
    else:
        year_filter = int(args.year)

    # Path overrides (OVE-friendly)
    apply_path_overrides(
        db_dir=args.db_dir,
        geomap_lists_dir=args.out_dir,
        logs_dir=args.logs_dir,
    )

    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("clean_derived", cfg.logs_dir)

    do_hotmap = args.hotmap or args.all
    do_exports = args.exports or args.all
    do_derived = args.derived_zooms or args.all

    if not (do_hotmap or do_exports or do_derived):
        logger.info("Nothing selected. Use --hotmap and/or --exports and/or --derived-zooms (or --all).")
        return 2

    zoom_i = args.zoom
    slot_i = args.slot
    keep_zoom = int(args.keep_zoom)

        
    if do_hotmap or do_derived:
        #
        cfg = Config(repo_root=REPO_ROOT)
        logger = setup_logger("clean_derived", cfg.logs_dir)

        # Ensure schema (or recreate DB if it's an old incompatible file)
        _ensure_schema_or_nuke_db(cfg, logger)

        conn = storage.connect(cfg.geomap_db_path)        
        try:
            storage.ensure_schema(conn)
            conn.execute("BEGIN;")
            try:
                if do_hotmap:
                    n_hotmap, n_set = storage.clear_hotmap(
                        conn,
                        zoom=zoom_i,
                        year=year_filter,
                        slot_id=slot_i,
                    )
                    logger.info("Deleted grid_hotmap rows: %d", n_hotmap)
                    logger.info("Deleted hotmap_taxa_set rows: %d", n_set)

                if do_derived:
                    n_grid, n_state = storage.clear_derived_zoom_cache(
                        conn,
                        keep_zoom=keep_zoom,
                        year=year_filter,
                        slot_id=slot_i,
                    )
                    logger.info("Deleted derived taxon_grid rows: %d", n_grid)
                    logger.info("Deleted derived taxon_layer_state rows: %d", n_state)

                conn.commit()
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.close()

    if do_exports:
        out_dir = cfg.geomap_lists_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        # If year_filter is YEAR_ALL (0), delete only year0-tagged exports.
        # If you want to nuke all year exports, pass --year -1.
        n_files = _delete_exports(out_dir, zoom=zoom_i, slot=slot_i, year=year_filter if year_filter is not None else None)
        logger.info("Deleted export files: %d", n_files)

    logger.info("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
