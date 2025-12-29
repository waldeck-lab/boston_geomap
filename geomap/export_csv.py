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


def _tile_bbox_latlon(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    """
    Returns (topLeft_lat, topLeft_lon, bottomRight_lat, bottomRight_lon)
    for slippy-map tiles in Web Mercator (EPSG:3857).
    """

    n = 2 ** z

    # longitudes are linear
    lon_left = x / n * 360.0 - 180.0
    lon_right = (x + 1) / n * 360.0 - 180.0

    # latitudes use inverse mercator
    def lat_from_ytile(yy: int) -> float:
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * yy / n)))
        return lat_rad * 180.0 / math.pi

    lat_top = lat_from_ytile(y)
    lat_bottom = lat_from_ytile(y + 1)

    return (lat_top, lon_left, lat_bottom, lon_right)


def export_top_sites_csv(
    conn: sqlite3.Connection,
    zoom: int,
    out_path: Path,
    limit: int = 200,
    source_table: str = "grid_hotmap",
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT zoom, x, y, coverage, score
        FROM {source_table}
        WHERE zoom = ?
        ORDER BY coverage DESC, score DESC
        LIMIT ?
        """,
        (zoom, limit),
    )
    rows = cur.fetchall()

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "rank",
                "zoom",
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

        for i, (z, x, y, coverage, score) in enumerate(rows, start=1):
            tl_lat, tl_lon, br_lat, br_lon = _tile_bbox_latlon(x, y, z)
            centroid_lat = (tl_lat + br_lat) / 2.0
            centroid_lon = (tl_lon + br_lon) / 2.0

            w.writerow(
                [
                    i,
                    z,
                    x,
                    y,
                    coverage,
                    score,
                    centroid_lat,
                    centroid_lon,
                    tl_lat,
                    tl_lon,
                    br_lat,
                    br_lon,
                    source_table,
                ]
            )
