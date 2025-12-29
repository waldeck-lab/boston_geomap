#!/usr/bin/env python3
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


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("export_hotmap", cfg.logs_dir)

    out_path = cfg.repo_root / "data" / "out" / f"hotmap_zoom{cfg.zoom}.geojson"
    logger.info("Exporting hotmap to: %s", out_path)

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)
        export_hotmap_geojson(conn, cfg.zoom, out_path)
        logger.info("Export complete.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
