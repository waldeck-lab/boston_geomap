#!/usr/bin/env python3
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


def read_first_n_taxa(csv_path: Path, n: int) -> list[int]:
    taxa: list[int] = []
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.reader(f)
        for row in r:
            if not row:
                continue
            taxon_id = row[0].strip()
            if taxon_id.isdigit():
                taxa.append(int(taxon_id))
            if len(taxa) >= n:
                break
    return taxa


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)

    logger = setup_logger("fetch_layers", cfg.logs_dir)
    logger.info("Missing species CSV: %s", cfg.missing_species_csv)
    logger.info("Geomap DB: %s", cfg.geomap_db_path)
    logger.info("Zoom: %d", cfg.zoom)

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

            logger.info("Fetching GeoGridAggregation: taxon_id=%d zoom=%d", taxon_id, cfg.zoom)
            payload = client.geogrid_aggregation([taxon_id], zoom=cfg.zoom)

            grid_cells = payload.get("gridCells") or []
            sha = stable_gridcells_hash(payload)

            prev = storage.get_layer_state(conn, taxon_id, cfg.zoom)
            if prev and prev[1] == sha:
                logger.info("No change for taxon_id=%d (sha256 match). gridCells=%d", taxon_id, len(grid_cells))
                conn.execute("BEGIN;")
                storage.upsert_layer_state(conn, taxon_id, cfg.zoom, sha, len(grid_cells))
                conn.commit()
                continue

            logger.info(
                "Updating layer for taxon_id=%d: gridCells=%d (changed=%s)",
                taxon_id,
                len(grid_cells),
                "yes" if prev else "new",
            )

            conn.execute("BEGIN;")
            storage.replace_taxon_grid(conn, taxon_id, cfg.zoom, grid_cells)
            storage.upsert_layer_state(conn, taxon_id, cfg.zoom, sha, len(grid_cells))
            conn.commit()

        logger.info("Done.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
