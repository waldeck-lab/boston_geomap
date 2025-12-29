from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


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
    conn.executescript(DDL)
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


def rebuild_hotmap(conn: sqlite3.Connection, zoom: int, taxon_ids: list[int]) -> None:
    """
    coverage = number of taxa present in cell
    score = sum(log(1+obs_count)) across taxa (implemented as ln via SQLite log() not portable)
    To keep it portable, compute score as sum of a simple saturation: obs_count**0.5
    (good enough for now; easy to swap later).
    """
    now = utc_now_iso()
    conn.execute("DELETE FROM grid_hotmap WHERE zoom=?;", (zoom,))

    placeholders = ",".join(["?"] * len(taxon_ids))
    q = f"""
    INSERT INTO grid_hotmap(
      zoom, x, y, coverage, score,
      bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
      updated_at_utc
    )
    SELECT
      zoom,
      x,
      y,
      COUNT(*) AS coverage,
      SUM( (observations_count * 1.0) * (observations_count * 1.0) ) AS score_sq,
      MAX(bbox_top_lat) AS bbox_top_lat,
      MIN(bbox_left_lon) AS bbox_left_lon,
      MIN(bbox_bottom_lat) AS bbox_bottom_lat,
      MAX(bbox_right_lon) AS bbox_right_lon,
      ? AS updated_at_utc
    FROM taxon_grid
    WHERE zoom = ?
      AND taxon_id IN ({placeholders})
      AND observations_count > 0
    GROUP BY zoom, x, y;
    """
    args = [now, zoom] + taxon_ids
    conn.execute(q, args)

    conn.execute("UPDATE grid_hotmap SET score = sqrt(score) WHERE zoom=?;", (zoom,))
