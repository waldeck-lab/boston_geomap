from __future__ import annotations

import json
from pathlib import Path
import sqlite3


def export_hotmap_geojson(conn: sqlite3.Connection, zoom: int, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = conn.execute(
        """
        SELECT x, y, coverage, score,
               bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM grid_hotmap
        WHERE zoom=?
        ORDER BY coverage DESC, score DESC;
        """,
        (zoom,),
    ).fetchall()

    features = []
    for (x, y, coverage, score, top_lat, left_lon, bottom_lat, right_lon) in rows:
        # bbox polygon (lon,lat)
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
                    "zoom": zoom,
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
