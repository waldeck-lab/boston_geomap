#!/usr/bin/env python3

# script:fetch_layers.py 

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
# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.config import Config
from geomap.logging_utils import setup_logger
from geomap.sos_client import SOSClient, stable_gridcells_hash, throttle
from geomap import storage


def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    return default


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
    cfg = Config(repo_root=REPO_ROOT)

    logger = setup_logger("fetch_layers", cfg.logs_dir)
    logger.info("Missing species CSV: %s", cfg.missing_species_csv)
    logger.info("Geomap DB: %s", cfg.geomap_db_path)

    zooms = _parse_zooms(_get_arg("--zooms", _get_arg("--zoom", "15")))
    base_zoom = zooms[0]
    logger.info("Zooms: %s (base=%d fetched from SOS)", zooms, base_zoom)
    logger.info("Zoom: %d", base_zoom)
    
    slot_id = int(_get_arg("--slot", "0"))
    logger.info("Slot: %d", slot_id)
    
    if not cfg.subscription_key:
        logger.error("Missing ARTDATABANKEN_SUBSCRIPTION_KEY")
        return 2
    if not cfg.authorization:
        logger.error("Missing ARTDATABANKEN_AUTHORIZATION")
        return 2

    n = 5
    if "--n" in sys.argv:
        n = int(sys.argv[sys.argv.index("--n") + 1])

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
            sha = stable_gridcells_hash(payload)

            prev = storage.get_layer_state(conn, taxon_id, base_zoom, slot_id)
            base_unchanged = bool(prev and prev[1] == sha)

            # Always make sure base zoom is stored/up-to-date first
            conn.execute("BEGIN;")
            if base_unchanged:
                logger.info("No change for taxon_id=%d (sha256 match). gridCells=%d", taxon_id, len(grid_cells))
                storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, sha, len(grid_cells))
            else:
                logger.info(
                    "Updating layer for taxon_id=%d: gridCells=%d (changed=%s)",
                    taxon_id,
                    len(grid_cells),
                    "yes" if prev else "new",
                )
                storage.replace_taxon_grid(conn, taxon_id, base_zoom, slot_id, grid_cells)
                storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, sha, len(grid_cells))
            conn.commit()

            # Now materialize derived zooms from the (now correct) base zoom.
            # Simple version: always do it (fast, avoids missing layers).
            if len(zooms) > 1:
                conn.execute("BEGIN;")
                for z in zooms[1:]:
                    logger.info(
                        "Materializing local zoom=%d from base_zoom=%d for taxon_id=%d slot=%d",
                        z, base_zoom, taxon_id, slot_id
                    )
                    storage.materialize_parent_zoom_from_child(
                        conn,
                        taxon_id=taxon_id,
                        slot_id=slot_id,
                        src_zoom=base_zoom,
                        dst_zoom=z,
                    )
                conn.commit()
        #     prev = storage.get_layer_state(conn, taxon_id, base_zoom, slot_id)

        #     if prev and prev[1] == sha:
        #         logger.info("No change for taxon_id=%d (sha256 match). gridCells=%d", taxon_id, len(grid_cells))
        #         conn.execute("BEGIN;")
        #         storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, sha, len(grid_cells))
        #         conn.commit()
        #         continue

        #     # Materialize lower zooms locally from the base zoom
        #     for z in zooms[1:]:
        #         logger.info("Materializing local zoom=%d from base_zoom=%d for taxon_id=%d slot=%d", z, base_zoom, taxon_id, slot_id)
        #         storage.materialize_parent_zoom_from_child(
        #             conn,
        #             taxon_id=taxon_id,
        #             slot_id=slot_id,
        #             src_zoom=base_zoom,
        #             dst_zoom=z,
        #         )
            
        #     logger.info(
        #         "Updating layer for taxon_id=%d: gridCells=%d (changed=%s)",
        #         taxon_id,
        #         len(grid_cells),
        #         "yes" if prev else "new",
        #     )

        #     conn.execute("BEGIN;")
        #     storage.replace_taxon_grid(conn, taxon_id, base_zoom, slot_id, grid_cells)
        #     storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, sha, len(grid_cells))
        #     conn.commit()

        # logger.info("Done.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
