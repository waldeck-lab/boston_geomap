#!/usr/bin/env python3

# script:import_csv_export.py 

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

import argparse
import csv
import math
import sqlite3
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional, List

# If you run inside the repo, this should work (same pattern as server/app.py)
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geomap import storage  # noqa: E402


SLOT_ALL = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slot_from_yyyy_mm_dd(s: str) -> int:
    """
    Slot mapping:
      month 1..12, quartile 1..4 in month:
        1-7 => q1, 8-14 => q2, 15-21 => q3, else => q4
      slot_id = (month-1)*4 + q, so 1..48
    """
    d = datetime.strptime(s, "%Y-%m-%d").date()
    day = d.day
    q = 1 if day <= 7 else 2 if day <= 14 else 3 if day <= 21 else 4
    return (d.month - 1) * 4 + q


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


@dataclass(frozen=True)
class IngestArgs:
    zip_or_csv: Path
    db_path: Path
    zooms: List[int]
    taxon_ids: Optional[List[int]]
    include_slot0: bool
    date_field: str  # StartDate or EndDate
    occurrence_status: Optional[str]  # e.g. "present"


def find_csv_inside_zip(zip_path: Path) -> Path:
    """
    Extracts the first *.csv found into a temp folder next to the zip.
    Returns path to extracted CSV.
    """
    out_dir = zip_path.parent / (zip_path.stem + "_unzipped")
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No .csv found inside {zip_path}")
        # pick the largest csv if multiple
        csv_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        chosen = csv_names[0]
        zf.extract(chosen, path=out_dir)
        return out_dir / chosen


def iter_observations_tsv(csv_path: Path) -> Iterable[Dict[str, str]]:
    """
    The export CSV is actually TSV (tab separated), with quoted fields.
    """
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # normalize keys a bit (strip)
            yield { (k or "").strip(): (v or "").strip() for k, v in row.items() }


def parse_float(row: Dict[str, str], key: str) -> Optional[float]:
    s = row.get(key, "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_int(row: Dict[str, str], key: str) -> Optional[int]:
    s = row.get(key, "")
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def build_counts(
    args: IngestArgs,
) -> Dict[Tuple[int, int, int], Dict[Tuple[int, int], int]]:
    """
    Returns:
      per_slot_taxon[(slot_id, zoom, taxon_id)][(x,y)] = observations_count
    """
    # slot, zoom, taxon -> cell -> count
    out: Dict[Tuple[int, int, int], Dict[Tuple[int, int], int]] = defaultdict(lambda: defaultdict(int))

    rows = 0
    kept = 0

    for row in iter_observations_tsv(args.zip_or_csv):
        rows += 1

        tid = parse_int(row, "DyntaxaTaxonId")
        if tid is None:
            continue
        if args.taxon_ids is not None and tid not in args.taxon_ids:
            continue

        if args.occurrence_status:
            st = (row.get("OccurrenceStatus") or "").strip().lower()
            if st != args.occurrence_status.lower():
                continue

        lat = parse_float(row, "DecimalLatitude")
        lon = parse_float(row, "DecimalLongitude")
        if lat is None or lon is None:
            continue

        ds = (row.get(args.date_field) or "").strip()
        if not ds:
            continue

        # export uses YYYY-MM-DD
        try:
            slot_id = slot_from_yyyy_mm_dd(ds)
        except Exception:
            continue

        for z in args.zooms:
            x, y = lonlat_to_tile_xy(lon, lat, z)
            out[(slot_id, z, tid)][(x, y)] += 1

        kept += 1

    print(f"[import] rows={rows} kept={kept} (after filters)")
    return out


def ensure_slot0(
    per_slot: Dict[Tuple[int, int, int], Dict[Tuple[int, int], int]]
) -> Dict[Tuple[int, int, int], Dict[Tuple[int, int], int]]:
    """
    Create slot_id=0 layers by summing over all slots 1..48 per (zoom,taxon,x,y).
    """
    agg: Dict[Tuple[int, int, int], Dict[Tuple[int, int], int]] = defaultdict(lambda: defaultdict(int))
    for (slot_id, z, tid), cells in per_slot.items():
        if slot_id == SLOT_ALL:
            continue
        dst_key = (SLOT_ALL, z, tid)
        for (x, y), c in cells.items():
            agg[dst_key][(x, y)] += c

    # merge agg into per_slot (don’t overwrite existing slot0 if any)
    out = dict(per_slot)
    for k, v in agg.items():
        if k not in out:
            out[k] = v
        else:
            for cell, c in v.items():
                out[k][cell] = out[k].get(cell, 0) + c
    return out


def replace_taxon_grid_offline(
    conn: sqlite3.Connection,
    taxon_id: int,
    zoom: int,
    slot_id: int,
    cell_counts: Dict[Tuple[int, int], int],
) -> None:
    """
    Deletes and inserts taxon_grid rows for a (taxon_id, zoom, slot_id).
    observations_count := number of observations (rows) in that tile for that taxon in that slot
    taxa_count := 1 for any tile that has observations (matches "per taxon" semantics)
    bbox_* := tile bbox
    """
    now = utc_now_iso()

    conn.execute(
        "DELETE FROM taxon_grid WHERE taxon_id=? AND zoom=? AND slot_id=?;",
        (taxon_id, zoom, slot_id),
    )

    rows = []
    for (x, y), obs_count in cell_counts.items():
        top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(x, y, zoom)
        rows.append(
            (
                taxon_id,
                zoom,
                slot_id,
                int(x),
                int(y),
                int(obs_count),
                1,  # taxa_count (per-taxon layer)
                float(top_lat),
                float(left_lon),
                float(bottom_lat),
                float(right_lon),
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Import SOS CSV export ZIP/CSV into geomap.sqlite taxon_grid (slot-aware).")
    ap.add_argument("zip_or_csv", type=str, help="Export.zip or extracted CSV file path")
    ap.add_argument("--db", required=True, type=str, help="Path to geomap.sqlite")
    ap.add_argument("--zooms", default="15,14,13", type=str, help="Comma-separated zooms (slippy tile zooms)")
    ap.add_argument("--taxon-ids", default="", type=str, help="Comma-separated dyntaxa taxon ids (optional)")
    ap.add_argument("--include-slot0", action="store_true", help="Also build slot_id=0 (all-year) by summing slots")
    ap.add_argument("--date-field", default="StartDate", choices=["StartDate", "EndDate"], help="Which date column to slotize")
    ap.add_argument("--occurrence-status", default="", type=str, help="Optional filter, e.g. present")

    args0 = ap.parse_args()

    zip_or_csv = Path(args0.zip_or_csv).expanduser().resolve()
    db_path = Path(args0.db).expanduser().resolve()

    zooms = [int(z.strip()) for z in args0.zooms.split(",") if z.strip()]
    zooms = sorted(set(zooms), reverse=True)
    if not zooms:
        raise SystemExit("No zooms provided")

    taxon_ids: Optional[List[int]] = None
    if args0.taxon_ids.strip():
        taxon_ids = [int(x.strip()) for x in args0.taxon_ids.split(",") if x.strip()]

    occ = args0.occurrence_status.strip() or None

    # unzip if needed
    if zip_or_csv.suffix.lower() == ".zip":
        extracted = find_csv_inside_zip(zip_or_csv)
        print(f"[import] extracted CSV: {extracted}")
        zip_or_csv = extracted

    if not zip_or_csv.exists():
        raise SystemExit(f"Missing input file: {zip_or_csv}")
    if not db_path.exists():
        print(f"[import] DB does not exist yet, will create: {db_path}")

    ingest = IngestArgs(
        zip_or_csv=zip_or_csv,
        db_path=db_path,
        zooms=zooms,
        taxon_ids=taxon_ids,
        include_slot0=bool(args0.include_slot0),
        date_field=args0.date_field,
        occurrence_status=occ,
    )

    per_slot = build_counts(ingest)
    if ingest.include_slot0:
        per_slot = ensure_slot0(per_slot)

    conn = storage.connect(ingest.db_path)
    conn.isolation_level = None  # autocommit
    try:
        storage.ensure_schema(conn)

        # Write all (slot,zoom,taxon) layers found
        keys = sorted(per_slot.keys(), key=lambda k: (k[2], k[1], k[0]))  # taxon, zoom, slot
        print(f"[import] writing layers: {len(keys)}")

        for (slot_id, z, tid) in keys:
            cell_counts = per_slot[(slot_id, z, tid)]
            with conn:
                replace_taxon_grid_offline(conn, tid, z, slot_id, cell_counts)

        print("[import] done.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
