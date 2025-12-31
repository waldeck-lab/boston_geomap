# geomap:export_csv.py

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


from __future__ import annotations

import csv
import math
import sqlite3
from pathlib import Path
from typing import Tuple

def export_top_sites_csv(
    conn: sqlite3.Connection,
    zoom: int,
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
        SELECT zoom, slot_id, x, y, coverage, score,
               bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM {source_table}
        WHERE zoom=? AND slot_id=?
        ORDER BY coverage DESC, score DESC
        LIMIT ?;
        """,
        (zoom, slot_id, limit),
    ).fetchall()

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "rank",
                "zoom",
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

        for i, (z, sid, x, y, coverage, score, tl_lat, tl_lon, br_lat, br_lon) in enumerate(rows, start=1):
            centroid_lat = (float(tl_lat) + float(br_lat)) / 2.0
            centroid_lon = (float(tl_lon) + float(br_lon)) / 2.0

            w.writerow(
                [
                    i,
                    int(z),
                    int(sid),
                    int(x),
                    int(y),
                    int(coverage),
                    float(score),
                    centroid_lat,
                    centroid_lon,
                    float(tl_lat),
                    float(tl_lon),
                    float(br_lat),
                    float(br_lon),
                    source_table,
                ]
            )
