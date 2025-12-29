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

from dataclasses import dataclass
from typing import Iterable, List, Tuple
import sqlite3


@dataclass(frozen=True)
class Hotspot:
    zoom: int
    x: int
    y: int
    coverage: int
    score: float
    bbox_top_lat: float
    bbox_left_lon: float
    bbox_bottom_lat: float
    bbox_right_lon: float


def top_hotspots(conn: sqlite3.Connection, zoom: int, limit: int = 20) -> List[Hotspot]:
    rows = conn.execute(
        """
        SELECT zoom, x, y, coverage, score,
               bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM grid_hotmap
        WHERE zoom=?
        ORDER BY score DESC, coverage DESC
        LIMIT ?;
        """,
        (zoom, limit),
    ).fetchall()

    return [
        Hotspot(
            zoom=int(r[0]),
            x=int(r[1]),
            y=int(r[2]),
            coverage=int(r[3]),
            score=float(r[4]),
            bbox_top_lat=float(r[5]),
            bbox_left_lon=float(r[6]),
            bbox_bottom_lat=float(r[7]),
            bbox_right_lon=float(r[8]),
        )
        for r in rows
    ]
