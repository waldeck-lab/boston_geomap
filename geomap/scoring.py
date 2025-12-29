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
        ORDER BY coverage DESC, score DESC
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
