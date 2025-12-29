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
from geomap import storage
from geomap.scoring import top_hotspots


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
            if len(taxa) >= n:
                break
    return taxa


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("build_hotmap", cfg.logs_dir)

    n = 5
    if "--n" in sys.argv:
        n = int(sys.argv[sys.argv.index("--n") + 1])

    taxon_ids = read_first_n_taxa(cfg.missing_species_csv, n)
    logger.info("Aggregating hotmap for n=%d taxa at zoom=%d", n, cfg.zoom)

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)
        conn.execute("BEGIN;")
        storage.rebuild_hotmap(conn, cfg.zoom, taxon_ids)
        conn.commit()

        tops = top_hotspots(conn, cfg.zoom, limit=10)
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


if __name__ == "__main__":
    raise SystemExit(main())
