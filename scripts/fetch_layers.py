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

# script:fetch_layers.py 


from __future__ import annotations

import csv
import sys

import argparse
import os

from pathlib import Path
# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.cli_paths import apply_path_overrides
from geomap.config import Config
from geomap.config import SLOT_MIN, SLOT_MAX, SLOT_ALL
from geomap.logging_utils import setup_logger
from geomap.sos_client import SOSClient, stable_gridcells_hash, throttle
from geomap import storage

from geomap.cli_paths import apply_path_overrides

from geomap.storage import YEAR_ALL


def _ove_default_stage_paths(repo_root: Path) -> tuple[Path, Path]:
    """
    Defaults: if OVE_BASE_DIR is set, use stage paths.
    Otherwise fallback to repo-local data dirs.
    """
    ove_base = os.getenv("OVE_BASE_DIR")
    if ove_base:
        base = Path(ove_base)
        return base / "stage" / "db", base / "stage" / "lists"
    return repo_root / "data" / "db", repo_root / "data" / "lists"

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zooms", "--zoom", dest="zooms", default="15", help="Comma-separated zoom levels.")
    ap.add_argument("--slot", type=int, default=SLOT_ALL, help=f"Calendar slot id: {SLOT_ALL}=all-time, 1..{SLOT_MAX}=time buckets")

    
    ap.add_argument("--n", type=int, default=5, help="Number of taxa (0 = all).")
    ap.add_argument("--alpha", type=float, default=None)
    ap.add_argument("--beta", type=float, default=None)
    ap.add_argument("--db-dir", default=None, help="Override DB dir (writes geomap.sqlite there).")
    ap.add_argument("--lists-dir", default=None, help="Override lists dir (reads missing_species.csv there).")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--cache-dir", default=None, help="Override cache dir.")
    ap.add_argument("--logs-dir", default=None, help="Override logs dir.")
    ap.add_argument("--year", type=int, default=YEAR_ALL, help="Year bucket. 0 = all-years aggregate.")

    return ap.parse_args()

def _parse_zooms(arg: str) -> list[int]:
    # accepts "15,14,13" or "15"
    zs = []
    for part in (arg or "").split(","):
        part = part.strip()
        if not part:
            continue
        zs.append(int(part))
    if not zs:
        raise ValueError("empty --zooms")
    # unique, sorted descending (highest zoom first)
    return sorted(set(zs), reverse=True)


def read_first_n_taxa(csv_path: Path, n: int) -> list[int]:
    taxa: list[int] = []
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.reader(f)
        for row in r:
            if not row:
                continue
            tid = row[0].strip()
            if tid.isdigit():
                taxa.append(int(tid))
            if n > 0 and len(taxa) >= n:
                break
    return taxa

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
    logger = setup_logger("fetch_layers", cfg.logs_dir)
    
    logger = setup_logger("fetch_layers", cfg.logs_dir)
    if not cfg.missing_species_csv.exists():
        logger.error("Missing species CSV not found: %s", cfg.missing_species_csv)
        logger.error("Tip: run crossmatch first, or pass --lists-dir pointing to stage/lists.")
        return 2

    logger.info("Geomap DB: %s", cfg.geomap_db_path)

    zooms = _parse_zooms(args.zooms)
    base_zoom = zooms[0]
    logger.info("Zooms: %s (base=%d fetched from SOS)", zooms, base_zoom)
    logger.info("Zoom: %d", base_zoom)
    
    slot_id = int(args.slot)
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
    year = int(args.year)
    logger.info("Year: %d", year)
    
    if not cfg.subscription_key:
        logger.error("Missing ARTDATABANKEN_SUBSCRIPTION_KEY")
        return 2
    if not cfg.authorization:
        logger.error("Missing ARTDATABANKEN_AUTHORIZATION")
        return 2

    n = int(args.n)

    taxon_ids = read_first_n_taxa(cfg.missing_species_csv, n)
    logger.info("Selected taxon ids (n=%d): %s", n, taxon_ids)

    client = SOSClient(
        base_url=cfg.base_url,
        api_version=cfg.api_version,
        subscription_key=cfg.subscription_key,
        authorization=cfg.authorization,
    )

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)

        throttle_state = {}
        for taxon_id in taxon_ids:
            throttle(2.0, throttle_state)


            logger.info("Fetching GeoGridAggregation: taxon_id=%d zoom=%d", taxon_id, base_zoom)
            payload = client.geogrid_aggregation([taxon_id], zoom=base_zoom)
            
            grid_cells = payload.get("gridCells") or []
            base_sha = stable_gridcells_hash(payload)

            prev_base = storage.get_layer_state(conn, taxon_id, base_zoom, slot_id, year=YEAR_ALL)
            base_changed = (not prev_base) or (prev_base[1] != base_sha)
            
            conn.execute("BEGIN;")
            try:
                if base_changed:
                    logger.info(
                        "Updating BASE layer for taxon_id=%d zoom=%d slot=%d: gridCells=%d (changed=%s)",
                        taxon_id, base_zoom, slot_id, len(grid_cells),
                        "new" if not prev_base else "yes",
                    )
                    storage.replace_taxon_grid(conn, taxon_id, base_zoom, slot_id, grid_cells, year=YEAR_ALL)
                    storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, base_sha, len(grid_cells), year=YEAR_ALL)

                else:
                    # keep last_fetch fresh even if unchanged
                    logger.info("No change for BASE taxon_id=%d (sha256 match). gridCells=%d", taxon_id, len(grid_cells))
                    storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, base_sha, len(grid_cells), year=YEAR_ALL)


                # Ensure derived zooms exist and are valid relative to current base_sha


                # Ensure derived zooms exist and are valid relative to current base_sha
                for z in zooms[1:]:
                    prev_z = storage.get_layer_state(conn, taxon_id, z, slot_id, year=YEAR_ALL)

                    valid = False
                    if prev_z:
                        valid = storage.is_valid_local_from(prev_z[1], base_zoom, base_sha)
                        
                    if valid and storage.has_any_taxon_grid(conn, taxon_id=taxon_id, zoom=z, slot_id=slot_id, year=YEAR_ALL):
                        logger.info(
                            "Derived zoom=%d OK for taxon_id=%d slot=%d (cache valid)",
                            z, taxon_id, slot_id
                        )
                        continue

                    # Verbose info
                    reason = "stale"
                    if not prev_z:
                        reason = "missing state"
                    elif not storage.has_any_taxon_grid(conn, taxon_id=taxon_id, zoom=z, slot_id=slot_id, year=YEAR_ALL):
                        reason = "missing rows"
                    logger.info(
                        "Rebuilding derived zoom=%d from base_zoom=%d for taxon_id=%d slot=%d (reason=%s)",
                        z, base_zoom, taxon_id, slot_id,
                        str(reason),
                    )

                    storage.materialize_parent_zoom_from_child(
                        conn,
                        taxon_id=taxon_id,
                        slot_id=slot_id,
                        src_zoom=base_zoom,
                        dst_zoom=z,
                        src_sha=base_sha,
                        year=YEAR_ALL,
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
