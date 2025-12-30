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

import json
from pathlib import Path
import sqlite3

def export_hotmap_geojson(conn: sqlite3.Connection, zoom: int, slot_id: int, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = conn.execute(
        """
        SELECT slot_id, x, y, coverage, score,
               bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM grid_hotmap
        WHERE zoom=? AND slot_id=?
        ORDER BY coverage DESC, score DESC;
        """,
        (zoom, slot_id),
    ).fetchall()

    features = []
    for (slot_id_db, x, y, coverage, score, top_lat, left_lon, bottom_lat, right_lon) in rows:
        poly = [
            [left_lon, top_lat],
            [right_lon, top_lat],
            [right_lon, bottom_lat],
            [left_lon, bottom_lat],
            [left_lon, top_lat],
        ]
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "zoom": int(zoom),
                    "slot_id": int(slot_id_db),
                    "x": int(x),
                    "y": int(y),
                    "coverage": int(coverage),
                    "score": float(score),
                },
                "geometry": {"type": "Polygon", "coordinates": [poly]},
            }
        )

    fc = {"type": "FeatureCollection", "features": features}
    out_path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
