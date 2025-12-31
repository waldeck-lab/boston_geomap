#!/usr/bin/env python3

# script:rank_nearby.py 

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

import sys
from pathlib import Path

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.config import Config
from geomap.logging_utils import setup_logger
from geomap import storage
from geomap.distance import haversine_km, distance_weight_rational, distance_weight_exp


# Dalby center of this Universe
DEFAULT_LAT = "55.667"
DEFAULT_LON = "13.350"


def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    return default


def taxa_for_cell(conn, zoom: int, slot_id: int, x: int, y: int) -> list[tuple[int,str,str,int]]:
    rows = conn.execute(
        """
        SELECT taxon_id, scientific_name, swedish_name, observations_count
        FROM grid_hotmap_taxa_names_v
        WHERE zoom=? AND slot_id=? AND x=? AND y=?
        ORDER BY observations_count DESC, taxon_id;
        """,
        (zoom, slot_id, x, y),
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
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("rank_nearby", cfg.logs_dir)

    lat = float(_get_arg("--lat", DEFAULT_LAT))   # default: Dalby
    lon = float(_get_arg("--lon", DEFAULT_LON))
    limit = int(_get_arg("--limit", "20"))
    d0_km = float(_get_arg("--d0-km", "30"))    # 30 km characteristic distance
    mode = (_get_arg("--mode", "rational") or "rational").lower()
    gamma = float(_get_arg("--gamma", "2.0"))   # only used for rational
    max_km = float(_get_arg("--max-km", "250")) # ignore very far cells

    show_all_taxa = "--show-all-taxa" in sys.argv
    taxa_top_n = int(_get_arg("--taxa-top", "10"))  # used when not showing all

    slot_id = int(_get_arg("--slot", "0"))
    zoom = int(_get_arg("--zoom", "15"))
    logger.info("Zoom: %d", zoom)

    logger.info("Slot: %d", slot_id)
    if slot_id < 0 or slot_id > 47:
        logger.error("slot_id out of range: %d", slot_id)
        return 2
    logger.info("Using position: lat=%.6f lon=%.6f", lat, lon)
    logger.info("Decay: mode=%s d0_km=%.3f gamma=%.3f max_km=%.3f", mode, d0_km, gamma, max_km)

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)

        # Pull a larger candidate set, then distance-rank it.
        # This keeps it fast without needing SQL trig functions.
        candidate_rows = conn.execute(
            """
            SELECT
              zoom, slot_id, x, y, coverage, score,
              centroid_lat, centroid_lon,
              topLeft_lat, topLeft_lon, bottomRight_lat, bottomRight_lon,
              obs_total,
              taxa_list
            FROM grid_hotmap_v
            WHERE zoom=? AND slot_id=?
            ORDER BY coverage DESC, score DESC
            LIMIT 2000;
            """,
            (zoom, slot_id),
        ).fetchall()


        if not candidate_rows:
            logger.warning("No hotmap rows at all for zoom=%d", zoom)
            return 0

        scored = []
        seen: set[tuple[int, int, int, int]] = set()

        logger.info("Candidates fetched: %d", len(candidate_rows))

        for row in candidate_rows:
            key = (int(row["zoom"]), int(row["slot_id"]), int(row["x"]), int(row["y"]))
            if key in seen:
                continue
            seen.add(key)

            base_score = float(row["score"])
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

        if not scored:
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


            taxa = taxa_for_cell(conn, zoom, slot_id, x, y)
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
