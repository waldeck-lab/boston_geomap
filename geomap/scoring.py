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

# geomap:scoring.py

from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from geomap.storage import YEAR_ALL

@dataclass(frozen=True)
class Hotspot:
    zoom: int
    year: int
    slot_id: int
    x: int
    y: int
    coverage: int
    score: float
    bbox_top_lat: float
    bbox_left_lon: float
    bbox_bottom_lat: float
    bbox_right_lon: float

def top_hotspots(
    conn: sqlite3.Connection,
    zoom: int,
    slot_id: int,
    *,
    year: int = YEAR_ALL,
    limit: int = 10,
) -> list[Hotspot]:
    rows = conn.execute(
        """
        SELECT x, y, coverage, score,
               bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM grid_hotmap
        WHERE zoom=? AND year=? AND slot_id=?
        ORDER BY coverage DESC, score DESC
        LIMIT ?;
        """,
        (int(zoom), int(year), int(slot_id), int(limit)),
    ).fetchall()

    return [
        Hotspot(
            zoom=int(zoom),
            year=int(year),
            slot_id=int(slot_id),
            x=int(r[0]),
            y=int(r[1]),
            coverage=int(r[2]),
            score=float(r[3]),
            bbox_top_lat=float(r[4]),
            bbox_left_lon=float(r[5]),
            bbox_bottom_lat=float(r[6]),
            bbox_right_lon=float(r[7]),
        )
        for r in rows
    ]
