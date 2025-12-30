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
from geomap.export_geojson import export_hotmap_geojson
from geomap.export_csv import export_top_sites_csv  # NEW

def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    return default

def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("export_hotmap", cfg.logs_dir)

    slot_id = int(_get_arg("--slot", "0"))

    out_geojson = cfg.repo_root / "data" / "out" / f"hotmap_zoom{cfg.zoom}_slot{slot_id}.geojson"
    out_csv  = cfg.repo_root / "data" / "out" / f"top_sites_zoom{cfg.zoom}_slot{slot_id}.csv"

    logger.info("Exporting slot_id: %d", slot_id)
    logger.info("Exporting hotmap to: %s", out_geojson)
    logger.info("Exporting top sites to: %s", out_csv)

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)

        export_hotmap_geojson(conn, cfg.zoom, slot_id, out_geojson)
        export_top_sites_csv(conn, cfg.zoom, slot_id, out_csv, limit=200)

        logger.info("Export complete.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
