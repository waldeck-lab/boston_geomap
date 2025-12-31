#!/usr/bin/env python3

# script:clean_derived.py
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
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geomap.config import Config
from geomap.logging_utils import setup_logger
from geomap import storage

def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default

def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("clean_derived", cfg.logs_dir)

    zoom = _get_arg("--zoom")
    slot = _get_arg("--slot")
    keep_zoom = int(_get_arg("--keep-zoom", "15"))

    zoom_i = int(zoom) if zoom is not None else None
    slot_i = int(slot) if slot is not None else None

    do_hotmap = "--hotmap" in sys.argv or "--all" in sys.argv
    do_exports = "--exports" in sys.argv or "--all" in sys.argv
    do_derived = "--derived-zooms" in sys.argv or "--all" in sys.argv

    if not (do_hotmap or do_exports or do_derived):
        logger.info("Nothing selected. Use: --hotmap and/or --exports and/or --derived-zooms (or --all)")
        return 2

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)
        conn.execute("BEGIN;")

        if do_hotmap:
            n_hotmap, n_set = storage.clear_hotmap(conn, zoom=zoom_i, slot_id=slot_i)
            logger.info("Deleted grid_hotmap rows: %d", n_hotmap)
            logger.info("Deleted hotmap_taxa_set rows: %d", n_set)

        if do_derived:
            n_grid, n_state = storage.clear_derived_zoom_cache(conn, keep_zoom=keep_zoom, slot_id=slot_i)
            logger.info("Deleted derived taxon_grid rows: %d", n_grid)
            logger.info("Deleted derived taxon_layer_state rows: %d", n_state)

        conn.commit()

    finally:
        conn.close()

    if do_exports:
        out_dir = cfg.repo_root / "data" / "out"
        n_files = storage.clear_export_files(out_dir, zoom=zoom_i, slot_id=slot_i)
        logger.info("Deleted export files: %d", n_files)

    logger.info("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
