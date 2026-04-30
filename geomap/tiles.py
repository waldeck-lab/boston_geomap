# geomap:tiles.py

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

import math
import sqlite3
from typing import Tuple

def tile_bbox_latlon(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    """
    Returns (top_lat, left_lon, bottom_lat, right_lon) for slippy tiles (Web Mercator).
    """
    n = 2 ** z
    left_lon = x / n * 360.0 - 180.0
    right_lon = (x + 1) / n * 360.0 - 180.0

    def lat_from_ytile(yy: int) -> float:
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * yy / n)))
        return math.degrees(lat_rad)

    top_lat = lat_from_ytile(y)
    bottom_lat = lat_from_ytile(y + 1)
    return top_lat, left_lon, bottom_lat, right_lon


# --- Slippy map helpers (WebMercator tile math) ---

def lonlat_to_tile_xy(lon: float, lat: float, z: int) -> Tuple[int, int]:
    """
    Convert lon/lat to slippy tile x,y at zoom z.
    """
    lat = max(min(lat, 85.05112878), -85.05112878)  # clamp for Mercator
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    # clamp to tile range
    x = max(0, min(x, n - 1))
    y = max(0, min(y, n - 1))
    return x, y


def tile_xy_to_bbox(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    """
    Returns (top_lat, left_lon, bottom_lat, right_lon) for tile x,y,z.
    """
    n = 2 ** z
    left_lon = x / n * 360.0 - 180.0
    right_lon = (x + 1) / n * 360.0 - 180.0

    def merc_to_lat(a: float) -> float:
        return math.degrees(math.atan(math.sinh(a)))

    top_lat = merc_to_lat(math.pi * (1 - 2 * (y / n)))
    bottom_lat = merc_to_lat(math.pi * (1 - 2 * ((y + 1) / n)))
    return (top_lat, left_lon, bottom_lat, right_lon)


def ensure_tile_bbox_rows(
    conn: sqlite3.Connection,
    *,
    zooms: list[int],
    xys_by_zoom: dict[int, set[tuple[int, int]]],
) -> int:
    rows = []

    for z in zooms:
        for x, y in xys_by_zoom.get(int(z), set()):
            top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(int(x), int(y), int(z))
            rows.append(
                (
                    int(z),
                    int(x),
                    int(y),
                    float(top_lat),
                    float(left_lon),
                    float(bottom_lat),
                    float(right_lon),
                )
            )

    if not rows:
        return 0

    conn.executemany(
        """
        INSERT OR IGNORE INTO tile_bbox(
            zoom, x, y,
            bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )

    return len(rows)




