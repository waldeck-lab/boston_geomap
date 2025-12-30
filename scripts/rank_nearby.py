#!/usr/bin/env python3

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


def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    return default


# Dalby center of this Universe
DEFAULT_LAT = "55.667"
DEFAULT_LON = "13.350"

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
              zoom, x, y, coverage, score,
              centroid_lat, centroid_lon,
              topLeft_lat, topLeft_lon, bottomRight_lat, bottomRight_lon,
              obs_total,
              taxa_list
            FROM grid_hotmap_v
            WHERE zoom=?
            ORDER BY coverage DESC, score DESC
            LIMIT 2000;
            """,
            (cfg.zoom,),
        ).fetchall()


        if not candidate_rows:
            logger.warning("No hotmap rows at all for zoom=%d", cfg.zoom)
            return 0
        
        scored = []        
        for r in candidate_rows:
            for idx, r in enumerate(candidate_rows):
                base_score = float(r["score"])
                c_lat = float(r["centroid_lat"])
                c_lon = float(r["centroid_lon"])

                d_km = haversine_km(lat, lon, c_lat, c_lon)
                if d_km > max_km:
                    continue

                if mode == "exp":
                    w = distance_weight_exp(d_km, d0_km)
                else:
                    w = distance_weight_rational(d_km, d0_km, gamma)

                dw_score = base_score * w
                scored.append((dw_score, d_km, r))

        if not scored:
            logger.warning(
                "No hotspots within max_km=%.1f km (zoom=%d). Closest exists, try --max-km 600",
                max_km, cfg.zoom)
            return 0
            
        scored.sort(key=lambda t: (t[0], -t[1]), reverse=True)

        show = scored[:limit]
        for i, (dw_score, d_km, r) in enumerate(show, 1):
            zoom, x, y = int(r["zoom"]), int(r["x"]), int(r["y"])
            coverage = int(r["coverage"])
            base_score = float(r["score"])
            taxa_list = r["taxa_list"] or ""
            logger.info(
                "Rank %d: dw_score=%.6f base=%.6f dist_km=%.2f coverage=%d cell=(%d,%d) taxa=%s",
                i, dw_score, base_score, d_km, coverage, x, y, taxa_list
            )

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
