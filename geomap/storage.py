# geomap:storage.py

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
  slot_id INTEGER NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  observations_count INTEGER NOT NULL,
  taxa_count INTEGER NOT NULL,
  bbox_top_lat REAL NOT NULL,
  bbox_left_lon REAL NOT NULL,
  bbox_bottom_lat REAL NOT NULL,
  bbox_right_lon REAL NOT NULL,
  fetched_at_utc TEXT NOT NULL,
  PRIMARY KEY (taxon_id, zoom, slot_id, x, y)
);

CREATE TABLE IF NOT EXISTS taxon_layer_state (
  taxon_id INTEGER NOT NULL,
  zoom INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  last_fetch_utc TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  grid_cell_count INTEGER NOT NULL,
  PRIMARY KEY (taxon_id, zoom, slot_id)
);

CREATE TABLE IF NOT EXISTS grid_hotmap (
  zoom INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  coverage INTEGER NOT NULL,
  score REAL NOT NULL,
  bbox_top_lat REAL NOT NULL,
  bbox_left_lon REAL NOT NULL,
  bbox_bottom_lat REAL NOT NULL,
  bbox_right_lon REAL NOT NULL,
  updated_at_utc TEXT NOT NULL,
  PRIMARY KEY (zoom, slot_id, x, y)
);

CREATE TABLE IF NOT EXISTS hotmap_taxa_set (
  zoom INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  taxon_id INTEGER NOT NULL,
  PRIMARY KEY (zoom, slot_id, taxon_id)
);

CREATE TABLE IF NOT EXISTS taxon_dim (
  taxon_id INTEGER PRIMARY KEY,
  scientific_name TEXT,
  swedish_name TEXT,
  updated_at_utc TEXT NOT NULL
);
"""

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def connect(db_path: Path) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row 
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.executescript(DDL)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_taxon_grid_cell ON taxon_grid(zoom, slot_id, x, y);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_taxon_grid_taxon ON taxon_grid(taxon_id, zoom, slot_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_grid_hotmap_slot ON grid_hotmap(zoom, slot_id, coverage, score);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hotmap_taxa_set_slot ON hotmap_taxa_set(zoom, slot_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_taxon_grid_cell_obs ON taxon_grid(zoom, slot_id, x, y, observations_count DESC);")


    # Drop views in dependency order
    cur.execute("DROP VIEW IF EXISTS grid_hotmap_taxa_names_v;")
    cur.execute("DROP VIEW IF EXISTS grid_hotmap_v;")
    cur.execute("DROP VIEW IF EXISTS grid_hotspot_taxa_v;")
    cur.execute("DROP VIEW IF EXISTS grid_taxa_v;")

    cur.execute("""
    CREATE VIEW grid_taxa_v AS
    SELECT
    tg.zoom,
    tg.slot_id,
    tg.x,
    tg.y,
    tg.taxon_id,
    COALESCE(td.scientific_name, '') AS scientific_name,
    COALESCE(td.swedish_name, '') AS swedish_name,
    tg.observations_count
    FROM taxon_grid tg
    LEFT JOIN taxon_dim td ON td.taxon_id = tg.taxon_id;
    """)
    
    cur.execute("""
    CREATE VIEW grid_hotspot_taxa_v AS
    SELECT
    gh.zoom, gh.slot_id, gh.x, gh.y,
    gh.coverage, gh.score,
    gh.bbox_top_lat, gh.bbox_left_lon, gh.bbox_bottom_lat, gh.bbox_right_lon,
    gt.taxon_id, gt.scientific_name, gt.swedish_name, gt.observations_count,
    gh.updated_at_utc
    FROM grid_hotmap gh
    JOIN grid_taxa_v gt
    ON gt.zoom=gh.zoom AND gt.slot_id=gh.slot_id AND gt.x=gh.x AND gt.y=gh.y;
    """)


    cur.execute("""
    CREATE VIEW grid_hotmap_v AS
    SELECT
      h.zoom,
      h.slot_id,
      h.x,
      h.y,
      h.coverage,
      h.score,

      h.bbox_top_lat    AS topLeft_lat,
      h.bbox_left_lon   AS topLeft_lon,
      h.bbox_bottom_lat AS bottomRight_lat,
      h.bbox_right_lon  AS bottomRight_lon,

      (h.bbox_top_lat + h.bbox_bottom_lat) / 2.0 AS centroid_lat,
      (h.bbox_left_lon + h.bbox_right_lon) / 2.0 AS centroid_lon,

      COALESCE(
        SUM(CASE WHEN s.taxon_id IS NOT NULL THEN t.observations_count ELSE 0 END),
        0
      ) AS obs_total,

      GROUP_CONCAT(
        CASE WHEN s.taxon_id IS NOT NULL THEN CAST(t.taxon_id AS TEXT) END,
        ';'
      ) AS taxa_list,

      h.updated_at_utc
    FROM grid_hotmap h
    LEFT JOIN taxon_grid t
      ON t.zoom=h.zoom AND t.slot_id=h.slot_id AND t.x=h.x AND t.y=h.y
    LEFT JOIN hotmap_taxa_set s
      ON s.zoom=t.zoom AND s.slot_id=t.slot_id AND s.taxon_id=t.taxon_id
    GROUP BY
      h.zoom, h.slot_id, h.x, h.y, h.coverage, h.score,
      h.bbox_top_lat, h.bbox_left_lon,
      h.bbox_bottom_lat, h.bbox_right_lon,
      h.updated_at_utc;
    """)
    
    # For --show-all-taxa / GUI “click a cell”
    cur.execute("""
    CREATE VIEW grid_hotmap_taxa_names_v AS
    SELECT
    h.zoom, h.slot_id, h.x, h.y,
    t.taxon_id,
    COALESCE(d.scientific_name,'') AS scientific_name,
    COALESCE(d.swedish_name,'')    AS swedish_name,
    t.observations_count
    FROM grid_hotmap h
    JOIN taxon_grid t
    ON t.zoom=h.zoom AND t.slot_id=h.slot_id AND t.x=h.x AND t.y=h.y
    JOIN hotmap_taxa_set s
    ON s.zoom=t.zoom AND s.slot_id=t.slot_id AND s.taxon_id=t.taxon_id
    LEFT JOIN taxon_dim d
    ON d.taxon_id=t.taxon_id;
    """)
    conn.commit()

def get_layer_state(conn: sqlite3.Connection, taxon_id: int, zoom: int, slot_id: int) -> Optional[Tuple[str, str, int]]:
    row = conn.execute(
        """
        SELECT last_fetch_utc, payload_sha256, grid_cell_count
        FROM taxon_layer_state
        WHERE taxon_id=? AND zoom=? AND slot_id=?;
        """,
        (taxon_id, zoom, slot_id),
    ).fetchone()
    if not row:
        return None
    return (row[0], row[1], int(row[2]))

def upsert_taxon_dim(
    conn: sqlite3.Connection,
    taxa: list[tuple[int, str, str]],
) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    conn.executemany(
        """
        INSERT INTO taxon_dim(taxon_id, scientific_name, swedish_name, updated_at_utc)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(taxon_id) DO UPDATE SET
          scientific_name=excluded.scientific_name,
          swedish_name=excluded.swedish_name,
          updated_at_utc=excluded.updated_at_utc;
        """,
        [(tid, sci, swe, now) for tid, sci, swe in taxa],
    )

def upsert_layer_state(
    conn: sqlite3.Connection,
    taxon_id: int,
    zoom: int,
    slot_id: int,
    payload_sha256: str,
    grid_cell_count: int
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO taxon_layer_state(taxon_id, zoom, slot_id, last_fetch_utc, payload_sha256, grid_cell_count)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(taxon_id, zoom, slot_id) DO UPDATE SET
          last_fetch_utc=excluded.last_fetch_utc,
          payload_sha256=excluded.payload_sha256,
          grid_cell_count=excluded.grid_cell_count;
        """,
        (taxon_id, zoom, slot_id, now, payload_sha256, int(grid_cell_count)),
    )

def replace_taxon_grid(
    conn: sqlite3.Connection,
    taxon_id: int,
    zoom: int,
    slot_id: int,
    grid_cells: Iterable[Dict[str, Any]]
) -> None:
    now = utc_now_iso()

    conn.execute(
        "DELETE FROM taxon_grid WHERE taxon_id=? AND zoom=? AND slot_id=?;",
        (taxon_id, zoom, slot_id),
    )

    rows = []
    for c in grid_cells:
        bb = c.get("boundingBox") or {}
        tl = bb.get("topLeft") or {}
        br = bb.get("bottomRight") or {}
        rows.append(
            (
                taxon_id,
                zoom,
                slot_id,
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
          taxon_id, zoom, slot_id, x, y, observations_count, taxa_count,
          bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
          fetched_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
        """,
        rows,
    )

def rebuild_hotmap(
    conn: sqlite3.Connection,
    zoom: int,
    slot_id: int,
    taxon_ids: list[int],
    *,
    alpha: float = 2.0,
    beta: float = 0.5,
) -> None:
    if not taxon_ids:
        return

    now = utc_now_iso()

    # Clear previous hotmap for this zoom+slot
    conn.execute("DELETE FROM grid_hotmap WHERE zoom=? AND slot_id=?;", (zoom, slot_id))

    placeholders = ",".join(["?"] * len(taxon_ids))

    # Replace active taxa set for this zoom+slot
    conn.execute("DELETE FROM hotmap_taxa_set WHERE zoom=? AND slot_id=?;", (zoom, slot_id))
    conn.executemany(
        "INSERT OR IGNORE INTO hotmap_taxa_set(zoom, slot_id, taxon_id) VALUES (?, ?, ?);",
        [(zoom, slot_id, tid) for tid in taxon_ids],
    )

    rows = conn.execute(
        f"""
        SELECT
            zoom,
            slot_id,
            x,
            y,
            COUNT(DISTINCT taxon_id) AS coverage,
            SUM(observations_count) AS obs_total,
            bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
        FROM taxon_grid
        WHERE zoom=? AND slot_id=? AND taxon_id IN ({placeholders})
        GROUP BY zoom, slot_id, x, y;
        """,
        [zoom, slot_id, *taxon_ids],
    ).fetchall()

    conn.executemany(
        """
        INSERT INTO grid_hotmap(
            zoom, slot_id, x, y, coverage, score,
            bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            (
                int(r[0]),
                int(r[1]),
                int(r[2]),
                int(r[3]),
                int(r[4]),
                (float(r[4]) ** float(alpha)) / ((float(r[5] or 0) + 1.0) ** float(beta)),
                float(r[6]),
                float(r[7]),
                float(r[8]),
                float(r[9]),
                now,
            )
            for r in rows
        ],
    )
