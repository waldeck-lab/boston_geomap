#!/usr/bin/env python3

# script:ingest_export_csv.py

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

# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Make repo importable when running as a script
REPO_ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(REPO_ROOT))

from geomap.config import Config
from geomap import storage


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_zooms(val: str) -> List[int]:
    zs = [int(x.strip()) for x in val.split(",") if x.strip()]
    zs = sorted(set(zs), reverse=True)
    if not zs:
        raise ValueError("Empty zoom list")
    return zs


def parse_date_yyyy_mm_dd(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Export sample: 2024-06-21 (no time)
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        # Sometimes ISO date-time happens
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None


def slot_from_date(d: datetime) -> int:
    """
    48 slots = 12 months * 4 "week quartiles" inside month:
      1..7 => q1, 8..14 => q2, 15..21 => q3, 22..end => q4
    slot = (month-1)*4 + q  => 1..48
    """
    month = d.month  # 1..12
    day = d.day
    q = 1 if day <= 7 else 2 if day <= 14 else 3 if day <= 21 else 4
    return (month - 1) * 4 + q


# --- WebMercator / slippy-tile helpers (MapLibre/OSM compatible) ---

import math

def lonlat_to_tile_xy(lon: float, lat: float, z: int) -> Tuple[int, int]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    # clamp
    x = max(0, min(x, n - 1))
    y = max(0, min(y, n - 1))
    return x, y


def tile_xy_to_bbox(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    """
    Returns (top_lat, left_lon, bottom_lat, right_lon)
    """
    n = 2 ** z

    left_lon = x / n * 360.0 - 180.0
    right_lon = (x + 1) / n * 360.0 - 180.0

    def y_to_lat(yy: int) -> float:
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * yy / n)))
        return math.degrees(lat_rad)

    top_lat = y_to_lat(y)
    bottom_lat = y_to_lat(y + 1)

    return (top_lat, left_lon, bottom_lat, right_lon)


def detect_delimiter(sample: str) -> str:
    # SOS “CSV” is often actually TSV
    if "\t" in sample and sample.count("\t") >= sample.count(","):
        return "\t"
    return ","


@dataclass
class Row:
    taxon_id: int
    lat: float
    lon: float
    d: datetime
    count: int


def iter_rows(path: Path) -> Iterable[Row]:
    opener = gzip.open if path.suffix.endswith("gz") else open

    # read a sample to detect delimiter
    with opener(path, "rt", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
    delim = detect_delimiter(sample)

    with opener(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)

        # Required columns (based on your sample)
        # "DyntaxaTaxonId", "StartDate", "DecimalLatitude", "DecimalLongitude"
        for rec in reader:
            try:
                tid_s = (rec.get("DyntaxaTaxonId") or "").strip()
                if not tid_s:
                    continue
                taxon_id = int(tid_s)

                lat_s = (rec.get("DecimalLatitude") or "").strip()
                lon_s = (rec.get("DecimalLongitude") or "").strip()
                if not lat_s or not lon_s:
                    continue
                lat = float(lat_s)
                lon = float(lon_s)

                d = parse_date_yyyy_mm_dd(rec.get("StartDate") or "") or parse_date_yyyy_mm_dd(rec.get("EndDate") or "")
                if d is None:
                    continue

                # If IndividualCount exists, use it; else default to 1
                cnt_s = (rec.get("IndividualCount") or "").strip()
                count = int(cnt_s) if cnt_s.isdigit() else 1

                yield Row(taxon_id=taxon_id, lat=lat, lon=lon, d=d, count=max(1, count))
            except Exception:
                # ignore bad rows
                continue


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest SOS Export CSV(.gz) into geomap taxon_grid and rebuild hotmaps.")
    ap.add_argument("--in", dest="inp", required=True, help="Path to SOS export file (.csv or .csv.gz).")
    ap.add_argument("--zooms", default="15,14,13", help="Comma-separated zooms to ingest (e.g. 15,14,13).")
    ap.add_argument("--n", type=int, default=0, help="Limit number of taxa to ingest (0=all present in file).")
    ap.add_argument("--alpha", type=float, default=None, help="Override hotmap alpha (else config default).")
    ap.add_argument("--beta", type=float, default=None, help="Override hotmap beta (else config default).")
    ap.add_argument("--db-dir", default=None, help="Override DB dir (expects geomap.sqlite there).")
    ap.add_argument("--lists-dir", default=None, help="Override lists dir (Config).")
    ap.add_argument("--logs-dir", default=None, help="Override logs dir (Config).")
    ap.add_argument("--rebuild-slots", default="touched", choices=["touched", "all"],
                    help="Rebuild hotmaps for 'touched' slots only, or 'all' slots (0..48).")
    args = ap.parse_args()

    inp = Path(args.inp).expanduser().resolve()
    if not inp.exists():
        print(f"ERROR: input not found: {inp}", file=sys.stderr)
        return 2

    zooms = parse_zooms(args.zooms)

    # Configure paths through Config
    from geomap.cli_paths import apply_path_overrides
    apply_path_overrides(db_dir=args.db_dir, lists_dir=args.lists_dir, logs_dir=args.logs_dir)
    cfg = Config(repo_root=REPO_ROOT)

    alpha = float(args.alpha) if args.alpha is not None else float(cfg.hotmap_alpha)
    beta = float(args.beta) if args.beta is not None else float(cfg.hotmap_beta)

    conn = storage.connect(cfg.geomap_db_path)
    conn.isolation_level = None
    storage.ensure_schema(conn)

    now = utc_now_iso()

    # Aggregate: (taxon, zoom, slot, x, y) -> (obs_count, bbox)
    agg: Dict[Tuple[int, int, int, int, int], int] = defaultdict(int)
    touched_slots: set[int] = set()
    taxa_seen: Dict[int, int] = {}  # taxon_id -> rows count (just for limiting)

    total_rows = 0
    for r in iter_rows(inp):
        total_rows += 1

        # enforce --n taxa limit (by first-seen taxon ids)
        if args.n and r.taxon_id not in taxa_seen and len(taxa_seen) >= args.n:
            continue
        taxa_seen.setdefault(r.taxon_id, 0)
        taxa_seen[r.taxon_id] += 1

        slot = slot_from_date(r.d)
        touched_slots.add(slot)

        for z in zooms:
            x, y = lonlat_to_tile_xy(r.lon, r.lat, z)
            agg[(r.taxon_id, z, slot, x, y)] += r.count
            # slot 0 all-time aggregate too
            agg[(r.taxon_id, z, 0, x, y)] += r.count

    taxon_ids = sorted(taxa_seen.keys())
    if not taxon_ids:
        print("No valid rows found to ingest (check column names / delimiter).", file=sys.stderr)
        return 1

    # Also touched slot 0 because we built it
    touched_slots.add(0)

    print(f"Ingest input: {inp}")
    print(f"Parsed rows: {total_rows}")
    print(f"Taxa ingested: {len(taxon_ids)} -> {taxon_ids[:10]}{'...' if len(taxon_ids) > 10 else ''}")
    print(f"Zooms: {zooms}")
    print(f"Touched slots (incl 0): {len(touched_slots)}")

    # Build rows grouped by (taxon, zoom, slot) so we can delete+insert cleanly
    grouped: Dict[Tuple[int, int, int], List[Tuple]] = defaultdict(list)

    for (taxon_id, z, slot_id, x, y), obs_cnt in agg.items():
        top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(x, y, z)
        grouped[(taxon_id, z, slot_id)].append(
            (
                taxon_id,
                z,
                slot_id,
                x,
                y,
                int(obs_cnt),
                1,  # taxa_count (per-taxon grid)
                float(top_lat),
                float(left_lon),
                float(bottom_lat),
                float(right_lon),
                now,
            )
        )

    # Write to DB
    insert_sql = """
        INSERT INTO taxon_grid(
          taxon_id, zoom, slot_id, x, y, observations_count, taxa_count,
          bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
          fetched_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
    """

    deleted = 0
    inserted = 0

    for (taxon_id, z, slot_id), rows in grouped.items():
        # delete old layer first to avoid stale cells
        conn.execute("BEGIN;")
        try:
            conn.execute(
                "DELETE FROM taxon_grid WHERE taxon_id=? AND zoom=? AND slot_id=?;",
                (taxon_id, z, slot_id),
            )
            deleted += 1
            conn.executemany(insert_sql, rows)
            inserted += len(rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(f"Deleted layers: {deleted} (taxon,zoom,slot)")
    print(f"Inserted rows into taxon_grid: {inserted}")

    # Rebuild hotmaps
    if args.rebuild_slots == "all":
        slots_to_build = list(range(0, 49))
    else:
        slots_to_build = sorted(touched_slots)

    for z in zooms:
        for slot_id in slots_to_build:
            conn.execute("BEGIN;")
            try:
                storage.rebuild_hotmap(conn, z, slot_id, taxon_ids, alpha=alpha, beta=beta)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    conn.close()
    print("OK: ingest + hotmap rebuild done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
