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

# geomap:export_csv.py

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

def export_top_sites_csv(
    conn: sqlite3.Connection,
    zoom: int,
    year: int,
    slot_id: int,
    out_path: Path,
    limit: int = 200,
    source_table: str = "grid_hotmap",
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if source_table not in {"grid_hotmap"}:
        raise ValueError(f"Unsupported source_table: {source_table}")

    rows = conn.execute(
        f"""
        SELECT zoom, year, slot_id, x, y, coverage, score,
               bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM {source_table}
        WHERE zoom=? AND year=? AND slot_id=?
        ORDER BY coverage DESC, score DESC
        LIMIT ?;
        """,
        (int(zoom), int(year), int(slot_id), int(limit)),
    ).fetchall()

    # Consolidation check on DB integrity and script extration 
    if rows and len(rows[0]) != 11:
        raise RuntimeError(f"Unexpected row shape from {source_table}: got {len(rows[0])} cols, expected 11")

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "rank",
                "zoom",
                "year",
                "slot_id",
                "x",
                "y",
                "coverage",
                "score",
                "centroid_lat",
                "centroid_lon",
                "topLeft_lat",
                "topLeft_lon",
                "bottomRight_lat",
                "bottomRight_lon",
                "source",
            ]
        )

        for i, (z, y_db, sid, x, y, coverage, score, top_lat, left_lon, bottom_lat, right_lon) in enumerate(rows, start=1):
            centroid_lat = (float(top_lat) + float(bottom_lat)) / 2.0
            centroid_lon = (float(left_lon) + float(right_lon)) / 2.0

            w.writerow(
                [
                    i,                 # rank
                    int(z),            # zoom
                    int(y_db),         # year
                    int(sid),          # slot_id
                    int(x),
                    int(y),
                    int(coverage),
                    float(score),
                    centroid_lat,
                    centroid_lon,
                    float(top_lat),    # topLeft_lat
                    float(left_lon),   # topLeft_lon
                    float(bottom_lat), # bottomRight_lat
                    float(right_lon),  # bottomRight_lon
                    source_table,
                ]
            )
