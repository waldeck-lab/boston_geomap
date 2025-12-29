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

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
import math


DDL = """
CREATE TABLE IF NOT EXISTS taxon_grid (
  taxon_id INTEGER NOT NULL,
  zoom INTEGER NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  observations_count INTEGER NOT NULL,
  taxa_count INTEGER NOT NULL,
  bbox_top_lat REAL NOT NULL,
  bbox_left_lon REAL NOT NULL,
  bbox_bottom_lat REAL NOT NULL,
  bbox_right_lon REAL NOT NULL,
  fetched_at_utc TEXT NOT NULL,
  PRIMARY KEY (taxon_id, zoom, x, y)
);

CREATE TABLE IF NOT EXISTS taxon_layer_state (
  taxon_id INTEGER NOT NULL,
  zoom INTEGER NOT NULL,
  last_fetch_utc TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  grid_cell_count INTEGER NOT NULL,
  PRIMARY KEY (taxon_id, zoom)
);

CREATE TABLE IF NOT EXISTS grid_hotmap (
  zoom INTEGER NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  coverage INTEGER NOT NULL,
  score REAL NOT NULL,
  bbox_top_lat REAL NOT NULL,
  bbox_left_lon REAL NOT NULL,
  bbox_bottom_lat REAL NOT NULL,
  bbox_right_lon REAL NOT NULL,
  updated_at_utc TEXT NOT NULL,
  PRIMARY KEY (zoom, x, y)
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def connect(db_path: Path) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # existing tables …
    cur.execute("""
    CREATE TABLE IF NOT EXISTS grid_hotmap (
      zoom INTEGER NOT NULL,
      x INTEGER NOT NULL,
      y INTEGER NOT NULL,
      coverage INTEGER NOT NULL,
      score REAL NOT NULL,
      bbox_top_lat REAL NOT NULL,
      bbox_left_lon REAL NOT NULL,
      bbox_bottom_lat REAL NOT NULL,
      bbox_right_lon REAL NOT NULL,
      updated_at_utc TEXT NOT NULL,
      PRIMARY KEY (zoom, x, y)
    );
    """)

    # existing tables taxon_grid, taxon_layer_state, etc…

    # ---- ADD THIS PART ----
    cur.execute("DROP VIEW IF EXISTS grid_hotmap_v;")
    cur.execute("""
    CREATE VIEW grid_hotmap_v AS
    SELECT
      h.zoom,
      h.x,
      h.y,
      h.coverage,
      h.score,

      -- bbox
      h.bbox_top_lat      AS topLeft_lat,
      h.bbox_left_lon     AS topLeft_lon,
      h.bbox_bottom_lat   AS bottomRight_lat,
      h.bbox_right_lon    AS bottomRight_lon,

      -- centroid
      (h.bbox_top_lat + h.bbox_bottom_lat) / 2.0 AS centroid_lat,
      (h.bbox_left_lon + h.bbox_right_lon) / 2.0 AS centroid_lon,

      -- strength of signal
      COALESCE(SUM(t.observations_count), 0) AS obs_total,

      -- explainability
      GROUP_CONCAT(t.taxon_id, ';') AS taxa_list,

      h.updated_at_utc
    FROM grid_hotmap h
    LEFT JOIN taxon_grid t
      ON t.zoom = h.zoom AND t.x = h.x AND t.y = h.y
    GROUP BY
      h.zoom, h.x, h.y, h.coverage, h.score,
      h.bbox_top_lat, h.bbox_left_lon,
      h.bbox_bottom_lat, h.bbox_right_lon,
      h.updated_at_utc;
    """)

    conn.commit()

def get_layer_state(conn: sqlite3.Connection, taxon_id: int, zoom: int) -> Optional[Tuple[str, str, int]]:
    row = conn.execute(
        "SELECT last_fetch_utc, payload_sha256, grid_cell_count FROM taxon_layer_state WHERE taxon_id=? AND zoom=?;",
        (taxon_id, zoom),
    ).fetchone()
    if not row:
        return None
    return (row[0], row[1], int(row[2]))


def upsert_layer_state(conn: sqlite3.Connection, taxon_id: int, zoom: int, payload_sha256: str, grid_cell_count: int) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO taxon_layer_state(taxon_id, zoom, last_fetch_utc, payload_sha256, grid_cell_count)
        VALUES(?,?,?,?,?)
        ON CONFLICT(taxon_id, zoom) DO UPDATE SET
          last_fetch_utc=excluded.last_fetch_utc,
          payload_sha256=excluded.payload_sha256,
          grid_cell_count=excluded.grid_cell_count;
        """,
        (taxon_id, zoom, now, payload_sha256, int(grid_cell_count)),
    )


def replace_taxon_grid(conn: sqlite3.Connection, taxon_id: int, zoom: int, grid_cells: Iterable[Dict[str, Any]]) -> None:
    now = utc_now_iso()

    conn.execute("DELETE FROM taxon_grid WHERE taxon_id=? AND zoom=?;", (taxon_id, zoom))

    rows = []
    for c in grid_cells:
        bb = c.get("boundingBox") or {}
        tl = bb.get("topLeft") or {}
        br = bb.get("bottomRight") or {}
        rows.append(
            (
                taxon_id,
                zoom,
                int(c["x"]),
                int(c["y"]),
                int(c.get("observationsCount") or 0),
                int(c.get("taxaCount") or 0),
                float(tl["latitude"]),
                float(tl["longitude"]),
                float(br["latitude"]),
                float(br["longitude"]),
                now,
            )
        )

    conn.executemany(
        """
        INSERT INTO taxon_grid(
          taxon_id, zoom, x, y, observations_count, taxa_count,
          bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
          fetched_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?);
        """,
        rows,
    )


def rebuild_hotmap(
    conn: sqlite3.Connection,
    zoom: int,
    taxon_ids: list[int],
    *,
    alpha: float = 2.0,
    beta: float = 0.5,
) -> None:
    if not taxon_ids:
        return

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Clear previous hotmap for this zoom
    conn.execute("DELETE FROM grid_hotmap WHERE zoom=?;", (zoom,))

    # Build placeholders (?, ?, ?, ...)
    placeholders = ",".join(["?"] * len(taxon_ids))

    # SQLite doesn't have POWER() enabled everywhere consistently, so use pow() via math in Python
    # Option A (simple + portable): compute score in Python after fetching aggregates.
    rows = conn.execute(
        f"""
        SELECT
            zoom,
            x,
            y,
            COUNT(DISTINCT taxon_id) AS coverage,
            SUM(observations_count) AS obs_total,
            bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM taxon_grid
        WHERE zoom=? AND taxon_id IN ({placeholders})
        GROUP BY zoom, x, y;
        """,
        [zoom, *taxon_ids],
    ).fetchall()

    # Insert rows with computed score
    conn.executemany(
        """
        INSERT INTO grid_hotmap(
            zoom, x, y, coverage, score,
            bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            (
                int(r[0]),
                int(r[1]),
                int(r[2]),
                int(r[3]),
                (float(r[3]) ** float(alpha)) / ((float(r[4] or 0) + 1.0) ** float(beta)),
                float(r[5]),
                float(r[6]),
                float(r[7]),
                float(r[8]),
                now,
            )
            for r in rows
        ],
    )

