#!/usr/bin/env python3

# script:build_hotmap.py 

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

from __future__ import annotations

import csv
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
from geomap.logging_utils import setup_logger
from geomap import storage
from geomap.scoring import top_hotspots


from dataclasses import dataclass

@dataclass(frozen=True)
class TaxonRow:
    taxon_id: int
    scientific_name: str
    swedish_name: str
    
def _parse_zooms(arg: str) -> list[int]:
    zs = []
    for part in (arg or "").split(","):
        part = part.strip()
        if not part:
            continue
        zs.append(int(part))
    if not zs:
        raise ValueError("empty --zooms/--zoom")
    return sorted(set(zs), reverse=True)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zooms", "--zoom", dest="zooms", default="15", help="Comma-separated zoom levels.")
    ap.add_argument("--slot", type=int, default=0, help="calendar slot id (0=all,1-48")
    ap.add_argument("--n", type=int, default=5, help="Number of taxa (0 = all).")
    ap.add_argument("--alpha", type=float, default=None)
    ap.add_argument("--beta", type=float, default=None)
    ap.add_argument("--db-dir", default=None, help="Override DB dir (writes geomap.sqlite there).")
    ap.add_argument("--lists-dir", default=None, help="Override lists dir (reads missing_species.csv there).")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--cache-dir", default=None, help="Override cache dir.")
    ap.add_argument("--logs-dir", default=None, help="Override logs dir.")
    return ap.parse_args()

    
def read_first_n_taxa_rows(csv_path: Path, n: int) -> list[TaxonRow]:
    rows: list[TaxonRow] = []

    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for rec in r:
            tid = (rec.get("taxon_id") or "").strip()
            if not tid.isdigit():
                continue

            rows.append(
                TaxonRow(
                    taxon_id=int(tid),
                    scientific_name=(rec.get("scientific_name") or "").strip(),
                    swedish_name=(rec.get("swedish_name") or "").strip(),
                )
            )
            if n > 0 and len(rows) >= n:
                break

    return rows

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

    # MUST be done before Config() so Config picks up GEOMAP_* env overrides
    apply_path_overrides(
        db_dir=args.db_dir,
        lists_dir=args.lists_dir,
        geomap_lists_dir=args.out_dir,  # output drop area (stage/lists/geomap)
        cache_dir=args.cache_dir,
        logs_dir=args.logs_dir,
    )

    cfg = Config(repo_root=REPO_ROOT)

    logger = setup_logger("build_hotmap", cfg.logs_dir)

    zooms = _parse_zooms(args.zooms)
    zoom = int(zooms[0])  # build_hotmap is per-zoom; pipeline passes "--zoom <z>"
    logger.info("Zoom: %d", zoom)

    slot_id = int(args.slot)
    logger.info("Slot: %d", slot_id)

    n = int(args.n)

    alpha = float(args.alpha) if args.alpha is not None else cfg.hotmap_alpha
    beta = float(args.beta) if args.beta is not None else cfg.hotmap_beta

    # Hard fail with helpful message (your logger already did something similar)
    if not cfg.missing_species_csv.exists():
        logger.error("Missing required CSV: %s", cfg.missing_species_csv)
        logger.error("Hint: ensure crossmatch project published missing_species.csv into stage/lists/")
        return 2

    taxa = read_first_n_taxa_rows(cfg.missing_species_csv, n)
    taxon_ids = [t.taxon_id for t in taxa]

    logger.info("Aggregating hotmap for n=%d taxa at zoom=%d", len(taxon_ids), zoom)

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)
        conn.execute("BEGIN;")

        storage.upsert_taxon_dim(
            conn,
            [(t.taxon_id, t.scientific_name, t.swedish_name) for t in taxa],
        )

        storage.rebuild_hotmap(
            conn,
            zoom,
            slot_id,
            taxon_ids,
            alpha=alpha,
            beta=beta,
        )
        conn.commit()

        tops = top_hotspots(conn, zoom, slot_id, limit=10)
        for i, h in enumerate(tops, 1):
            logger.info(
                "Top %d: coverage=%d score=%.3f cell=(%d,%d) bbox=[(%.5f,%.5f)->(%.5f,%.5f)]",
                i,
                h.coverage,
                h.score,
                h.x,
                h.y,
                h.bbox_top_lat,
                h.bbox_left_lon,
                h.bbox_bottom_lat,
                h.bbox_right_lon,
            )

        return 0
    finally:
        conn.close()

# def main() -> int:
#     args = parse_args()

#     apply_path_overrides(
#         db_dir=args.db_dir,
#         lists_dir=args.lists_dir,
#         geomap_lists_dir=args.out_dir,
#         cache_dir=args.cache_dir,
#         logs_dir=args.logs_dir,
#     )
#     cfg = Config(repo_root=REPO_ROOT)
#     logger = setup_logger("build_hotmap", cfg.logs_dir)

#     alpha = args.alpha if args.alpha is not None else cfg.hotmap_alpha
#     beta  = args.beta  if args.beta  is not None else cfg.hotmap_beta
    
#     logger = setup_logger("build_hotmap", cfg.logs_dir)

#     if not cfg.missing_species_csv.exists():
#         logger.error("Missing required CSV: %s", cfg.missing_species_csv)
#         logger.error("Hint: ensure crossmatch project published missing_species.csv into stage/lists/")
#         return 2

#     zoom = args.zoom
#     logger.info("Zoom: %d", zoom)
#     slot_id = args.slot
#     logger.info("Slot: %d", slot_id)
#     n = args.n

#     taxa = read_first_n_taxa_rows(cfg.missing_species_csv, n)
#     taxon_ids = [t.taxon_id for t in taxa]

#     logger.info(
#         "Aggregating hotmap for n=%d taxa at zoom=%d",
#         len(taxon_ids), zoom
#     )

#     conn = storage.connect(cfg.geomap_db_path)
#     try:
#         storage.ensure_schema(conn)
#         conn.execute("BEGIN;")

#         # NEW: populate taxon_dim
#         storage.upsert_taxon_dim(
#             conn,
#             [(t.taxon_id, t.scientific_name, t.swedish_name) for t in taxa],
#         )

#         storage.rebuild_hotmap(
#             conn,
#             zoom,
#             slot_id,
#             taxon_ids,
#             alpha=alpha,
#             beta=beta,
#         )
#         conn.commit()

#         tops = top_hotspots(conn, zoom, slot_id, limit=10)
#         for i, h in enumerate(tops, 1):
#             logger.info(
#                 "Top %d: coverage=%d score=%.3f cell=(%d,%d) bbox=[(%.5f,%.5f)->(%.5f,%.5f)]",
#                 i,
#                 h.coverage,
#                 h.score,
#                 h.x,
#                 h.y,
#                 h.bbox_top_lat,
#                 h.bbox_left_lon,
#                 h.bbox_bottom_lat,
#                 h.bbox_right_lon,
#             )

#         return 0
#     finally:
#         conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
