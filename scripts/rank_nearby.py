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

# script:rank_nearby.py 

from __future__ import annotations

import sys
import argparse
import os
from pathlib import Path

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------
from geomap.config import SLOT_MIN, SLOT_MAX, SLOT_ALL
from geomap.storage import YEAR_MAX, YEAR_MIN, YEAR_ALL
from geomap.config import Config
from geomap.logging_utils import setup_logger
from geomap import storage
from geomap.distance import haversine_km, distance_weight_rational, distance_weight_exp


# Dalby center of this Universe
DEFAULT_LAT = "55.667"
DEFAULT_LON = "13.350"

def _path_status(p: Path) -> str:
    try:
        if not p.exists():
            return "missing"
        if p.is_dir():
            return "dir"
        sz = p.stat().st_size
        return f"file size={sz}"
    except Exception as e:
        return f"error({e})"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Rank nearby geomap hotspots by distance-weighted score.",
    )

    ap.add_argument("--lat", type=float, default=float(DEFAULT_LAT),
                    help="Latitude (default: Dalby, Skåne)")
    ap.add_argument("--lon", type=float, default=float(DEFAULT_LON),
                    help="Longitude (default: Dalby, Skåne)")

    ap.add_argument("--zoom", type=int, default=15,
                    help="Geomap zoom level (default: 15)")
    ap.add_argument("--slot", type=int, default=SLOT_ALL, help=f"Calendar slot id: {SLOT_ALL}=all-time, 1..{SLOT_MAX}=time buckets")

    ap.add_argument("--limit", type=int, default=20,
                    help="Number of ranked cells to show")
    ap.add_argument("--max-km", type=float, default=250,
                    help="Ignore cells farther than this distance (km)")

    ap.add_argument("--mode", choices=("rational", "exp"), default="rational",
                    help="Distance decay model")
    ap.add_argument("--d0-km", type=float, default=30.0,
                    help="Characteristic distance for decay (km)")
    ap.add_argument("--gamma", type=float, default=2.0,
                    help="Gamma exponent (rational mode only)")

    ap.add_argument("--show-all-taxa", action="store_true",
                    help="Show all taxa per cell (otherwise top-N)")
    ap.add_argument("--taxa-top", type=int, default=10,
                    help="Number of taxa shown when not using --show-all-taxa")
    ap.add_argument("--candidates", type=int, default=20000,
                    help="How many hotspot candidates to consider before distance filtering (default: 20000).")
    ap.add_argument("--year", type=int, default=YEAR_ALL, help="Year bucket. 0 = all-years aggregate.")


    # Path overrides (OVE-compatible)
    ove_base = (os.getenv("OVE_BASE_DIR") or "").strip()
    default_db_dir = str(Path(ove_base) / "stage" / "db") if ove_base else None

    ap.add_argument(
        "--db-dir",
        default=default_db_dir,
        help="Override DB directory (expects geomap.sqlite). Defaults to $OVE_BASE_DIR/stage/db when running in OVE.",
    )
    ap.add_argument("--logs-dir", default=None,
                    help="Override logs directory")
    return ap.parse_args()

def taxa_for_cell(conn, zoom: int, year: int, slot_id: int, x: int, y: int) -> list[tuple[int,str,str,int]]:
    rows = conn.execute(
        """
        SELECT taxon_id, scientific_name, swedish_name, observations_count
        FROM grid_hotmap_taxa_names_v
        WHERE zoom=? AND year=? AND slot_id=? AND x=? AND y=?
        ORDER BY observations_count DESC, taxon_id;
        """,
        (zoom, int(year), int(slot_id), int(x), int(y)),
    ).fetchall()
    return [(int(r[0]), r[1], r[2], int(r[3])) for r in rows]

def fmt_taxa(taxa: list[tuple[int, str, str, int]], max_items: int = 8) -> str:
    # tid, sci, swe, obs
    parts = []
    for tid, sci, swe, obs in taxa[:max_items]:
        name = swe.strip() or sci.strip() or str(tid)
        parts.append(f"{tid}:{name}({obs})")
    more = "" if len(taxa) <= max_items else f" …+{len(taxa)-max_items}"
    return ", ".join(parts) + more


def main() -> int:
    args = parse_args()
    # Apply OVE-style path overrides
    from geomap.cli_paths import apply_path_overrides
    apply_path_overrides(
        db_dir=args.db_dir,
        logs_dir=args.logs_dir,
    )
    
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("rank_nearby", cfg.logs_dir)
    logger.info("OVE_BASE_DIR=%s", os.getenv("OVE_BASE_DIR"))
    logger.info("Resolved geomap_db_path=%s", cfg.geomap_db_path)
    

    db_path = Path(cfg.geomap_db_path)
    logger.info("Geomap DB path: %s (%s)", db_path, _path_status(db_path))

    if not db_path.exists():
        logger.error("Geomap DB does not exist: %s", db_path)
        return 2
    if db_path.stat().st_size == 0:
        logger.error("Geomap DB is empty (0 bytes): %s", db_path)
        return 2
    ove_base = (os.getenv("OVE_BASE_DIR") or "").strip()
    if ove_base and str(db_path).startswith(str(Path(ove_base))) and "/stage/" not in str(db_path):
        logger.warning("DB is not under stage/. Did you mean --db-dir %s ?", Path(ove_base)/"stage"/"db")
        
    lat = args.lat
    lon = args.lon
    zoom = args.zoom
    slot_id = args.slot
    limit = args.limit
    d0_km = args.d0_km
    mode = args.mode
    gamma = args.gamma
    max_km = args.max_km
    show_all_taxa = args.show_all_taxa
    taxa_top_n = args.taxa_top
    candidates = args.candidates

    
    logger.info("Zoom: %d", zoom)

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
    
    logger.info("Using position: lat=%.6f lon=%.6f", lat, lon)
    logger.info("Decay: mode=%s d0_km=%.3f gamma=%.3f max_km=%.3f", mode, d0_km, gamma, max_km)
    logger.info("Candidate prefetch limit: %d", candidates)

    conn = storage.connect(cfg.geomap_db_path)

    # Extra debug
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()
    logger.info("DB tables: %s", [t[0] for t in tables])
    
    try:
        storage.ensure_schema(conn)

        # What slots/zooms exist?
        try:
            slots = conn.execute(
                "SELECT slot_id, COUNT(*) c FROM grid_hotmap WHERE year=? GROUP BY slot_id ORDER BY slot_id;",
                (int(year),),
            ).fetchall()
            if slots:
                logger.info("Hotmap rows per slot_id: %s", [(int(r[0]), int(r[1])) for r in slots])
            else:
                logger.warning("grid_hotmap_v has 0 rows total (did you run build_hotmap?)")
        except Exception as e:
            logger.error("Could not query grid_hotmap_v. Missing view/schema? %s", e)
            return 2

        try:
            zc = conn.execute(
                "SELECT zoom, COUNT(*) c FROM grid_hotmap WHERE year=? GROUP BY zoom ORDER BY zoom;",
                (int(year),),
            ).fetchall()
            logger.info("Hotmap rows per zoom: %s", [(int(r[0]), int(r[1])) for r in zc])
        except Exception as e:
            logger.warning("Could not query zoom distribution: %s", e)

        # For the requested zoom/slot
        n_zoom_slot = conn.execute(
            "SELECT COUNT(*) FROM grid_hotmap WHERE zoom=? AND year=? AND slot_id=?;",
            (int(zoom), int(year), int(slot_id)),
        ).fetchone()[0]
        logger.info("Hotmap rows for zoom=%d slot=%d: %d", zoom, slot_id, int(n_zoom_slot))

        if int(n_zoom_slot) == 0:
            logger.error("No hotmap rows for zoom=%d slot=%d in DB: %s", zoom, slot_id, db_path)
            logger.error("Hint: run build_hotmap for that zoom/slot (or use --slot 0 if you only built slot 0).")
            return 2
        

        # Pull a larger candidate set, then distance-rank it.
        # This keeps it fast without needing SQL trig functions.

        candidate_rows = conn.execute(
            """
            SELECT
            zoom, year, slot_id, x, y, coverage, score,
            centroid_lat, centroid_lon,
            topLeft_lat, topLeft_lon, bottomRight_lat, bottomRight_lon,
            obs_total,
            taxa_list
            FROM grid_hotmap_v
            WHERE zoom=? AND year=? AND slot_id=?
            ORDER BY coverage DESC, score DESC
            LIMIT ?;
            """,
            (zoom, year, slot_id, candidates),
        ).fetchall()
        
        if not candidate_rows:
            logger.warning("No hotmap rows at all for zoom=%d", zoom)
            return 0

        scored = []
        seen: set[tuple[int, int, int, int]] = set()

        logger.info("Candidates fetched: %d", len(candidate_rows))

        for row in candidate_rows:
            key = (int(row["zoom"]), int(row["year"]), int(row["slot_id"]), int(row["x"]), int(row["y"]))
            if key in seen:
                continue
            seen.add(key)

            base_score = float(row["score"])

            # grid_hotmap_v exposes centroid_lat/centroid_lon (and aliases for bbox)
            c_lat = float(row["centroid_lat"])
            c_lon = float(row["centroid_lon"])
            
            
            d_km = haversine_km(lat, lon, c_lat, c_lon)

            # Debug a few rows
            if len(seen) <= 5:
                logger.info(
                    "DEBUG cand %d key=%s centroid=(%.6f,%.6f) d_km=%.2f base=%.6f",
                    len(seen), key, c_lat, c_lon, d_km, base_score
                )

            if d_km > max_km:
                continue

            if mode == "exp":
                w = distance_weight_exp(d_km, d0_km)
            else:
                w = distance_weight_rational(d_km, d0_km, gamma)

            dw_score = base_score * w
            scored.append((dw_score, d_km, row))

        logger.info("Unique cells considered: %d", len(seen))
        logger.info("Scored within max_km: %d", len(scored))

        if scored:
            dists = [d for (_, d, _) in scored]
            logger.info(
                "Distance stats within max_km: min=%.1f km p50=%.1f km p90=%.1f km max=%.1f km",
                min(dists),
                sorted(dists)[len(dists)//2],
                sorted(dists)[int(len(dists)*0.9)],
                max(dists),
            )
        else:
            logger.warning(
                "No hotspots within max_km=%.1f km (zoom=%d). Closest exists, try --max-km 600",
                max_km, zoom)
            return 0
            
        scored.sort(key=lambda t: (-t[0], t[1]))  # highest dw_score, then nearest

        ## Debug vvv
        out_keys = set()
        for i, (dw_score, d_km, row) in enumerate(scored[:limit], 1):
            key = (int(row["zoom"]), int(row["slot_id"]), int(row["x"]), int(row["y"]))
            if key in out_keys:
                logger.info("DEBUG DUP OUTPUT at rank %d key=%s", i, key)
            out_keys.add(key)
        ## Debug ^^^

        show = scored[:limit]
        for i, (dw_score, d_km, r) in enumerate(show, 1):
            zoom, slot_id, x, y = int(r["zoom"]), int(r["slot_id"]),int(r["x"]), int(r["y"])
            coverage = int(r["coverage"])
            base_score = float(r["score"])


            taxa = taxa_for_cell(conn, zoom, year, slot_id, x, y)
            taxa_named = fmt_taxa(taxa, max_items=8)

            logger.info(
                "Rank %d: dw_score=%.6f base=%.6f dist_km=%.2f coverage=%d cell=(%d,%d) taxa=%s",
                i, dw_score, base_score, d_km, coverage, x, y, taxa_named
            )

            logger.info("        species: %d taxa", len(taxa))

            if not taxa:
                continue

            if show_all_taxa:
                logger.info("        all:")
                for tid, sci, swe, obs in taxa:
                    name = swe or sci or ""
                    logger.info("          %d\t%s\tobs=%d", tid, name, obs)
            else:
                top_taxa = taxa[:taxa_top_n]
                logger.info(
                    "        top: %s",
                    ", ".join(
                        f"{tid}:{(swe or sci or '')}({obs})"
                        for tid, sci, swe, obs in top_taxa
                    )
                )
                        
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
