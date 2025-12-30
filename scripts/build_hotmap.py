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


from dataclasses import dataclass

@dataclass(frozen=True)
class TaxonRow:
    taxon_id: int
    scientific_name: str
    swedish_name: str


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

            if len(rows) >= n:
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
            if len(taxa) >= n:
                break
    return taxa


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)
    alpha = cfg.hotmap_alpha
    beta = cfg.hotmap_beta
    if "--alpha" in sys.argv:
        alpha = float(sys.argv[sys.argv.index("--alpha") + 1])
    if "--beta" in sys.argv:
        beta = float(sys.argv[sys.argv.index("--beta") + 1])

    logger = setup_logger("build_hotmap", cfg.logs_dir)

    n = 5
    if "--n" in sys.argv:
        n = int(sys.argv[sys.argv.index("--n") + 1])

    taxa = read_first_n_taxa_rows(cfg.missing_species_csv, n)
    taxon_ids = [t.taxon_id for t in taxa]

    logger.info(
        "Aggregating hotmap for n=%d taxa at zoom=%d",
        len(taxon_ids), cfg.zoom
    )

    conn = storage.connect(cfg.geomap_db_path)
    try:
        storage.ensure_schema(conn)
        conn.execute("BEGIN;")

        # NEW: populate taxon_dim
        storage.upsert_taxon_dim(
            conn,
            [(t.taxon_id, t.scientific_name, t.swedish_name) for t in taxa],
        )

        storage.rebuild_hotmap(
            conn,
            cfg.zoom,
            taxon_ids,
            alpha=alpha,
            beta=beta,
        )
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
