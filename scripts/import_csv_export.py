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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional, List, Set

# If you run inside the repo, this should work (same pattern as server/app.py)
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geomap import storage  
from geomap.config import Config
from geomap.sos_export import export_csv_zip_to_file


SLOT_ALL = 0



def normalize_occurrence_id(row: Dict[str, str]) -> str:
    return ((row.get("OccurrenceId") or row.get("\ufeffOccurrenceId") or "").strip())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def slot_from_date(d: date) -> int:
    """
    Slot mapping:
      month 1..12, quartile 1..4 in month:
        1-7 => q1, 8-14 => q2, 15-21 => q3, else => q4
      slot_id = (month-1)*4 + q, so 1..48
    """
    day = d.day
    q = 1 if day <= 7 else 2 if day <= 14 else 3 if day <= 21 else 4
    return (d.month - 1) * 4 + q

def slot_from_yyyy_mm_dd(s: str) -> int:
    return slot_from_date(parse_yyyy_mm_dd(s))


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
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:    
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

def read_taxon_ids_from_csv(csv_path: Path) -> List[int]:
    """
    Reads taxon ids from CSV where first column contains taxon_id.
    First row is assumed header.
    """

    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))

    out: List[int] = []
    seen: Set[int] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        header = next(reader, None)
        if header is None:
            return []

        for row in reader:
            if not row:
                continue

            try:
                tid = int(float(row[0]))
            except Exception:
                continue

            if tid <= 0:
                continue

            if tid in seen:
                continue

            seen.add(tid)
            out.append(tid)

    return out


def chunked(values: List[int], batch_size: int):
    if batch_size <= 0:
        yield values
        return

    for i in range(0, len(values), batch_size):
        yield values[i:i + batch_size]
    
    
def upsert_observation_raw(conn, row):
    conn.execute(
        """
        INSERT INTO observations_raw (
            occurrence_id,
            taxon_id,
            observation_date,
            modification_date,
            year,
            slot_id,
            latitude,
            longitude,
            tile_x,
            tile_y,
            zoom,
            occurrence_status,
            individual_count,
            imported_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))

        ON CONFLICT(occurrence_id)
        DO UPDATE SET
            taxon_id = excluded.taxon_id,
            observation_date = excluded.observation_date,
            modification_date = excluded.modification_date,
            year = excluded.year,
            slot_id = excluded.slot_id,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            tile_x = excluded.tile_x,
            tile_y = excluded.tile_y,
            zoom = excluded.zoom,
            occurrence_status = excluded.occurrence_status,
            individual_count = excluded.individual_count,
            imported_at_utc = datetime('now')
        WHERE
            excluded.modification_date IS NOT NULL
            AND (
                observations_raw.modification_date IS NULL
                OR excluded.modification_date > observations_raw.modification_date
            );
        """,
        (
            row["occurrence_id"],
            row["taxon_id"],
            row["observation_date"],
            row["modification_date"],
            row["year"],
            row["slot_id"],
            row["latitude"],
            row["longitude"],
            row["tile_x"],
            row["tile_y"],
            row["zoom"],
            row["occurrence_status"],
            row["individual_count"],
        ),
    )
    

def import_observations_raw(
        conn: sqlite3.Connection,
        args: IngestArgs,
) -> Set[Tuple[int, int, int, int]]:
    """
    Import raw observations into observations_raw.

    Returns a set of affected scopes:
      (year, slot_id, zoom, taxon_id)
    These scopes can then be consolidated into taxon_grid.
    """
    touched: Set[Tuple[int, int, int, int]] = set()
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

        try:
            d = parse_yyyy_mm_dd(ds)
            year = int(d.year)
            slot_id = slot_from_date(d)
        except Exception:
            continue

        occurrence_id = normalize_occurrence_id(row)
        if not occurrence_id:
            continue
        
        modification_date = (
            row.get("Modified")
            or row.get("ModificationDate")
            or None
        )

        occurrence_status = row.get("OccurrenceStatus")
        individual_count = parse_int(row, "IndividualCount")

        for z in args.zooms:
            x, y = lonlat_to_tile_xy(lon, lat, z)

            # raw observation write
            raw_row = {
                "occurrence_id": occurrence_id,
                "taxon_id": tid,
                "observation_date": ds,
                "modification_date": modification_date,
                "year": year,
                "slot_id": slot_id,
                "latitude": lat,
                "longitude": lon,
                "tile_x": x,
                "tile_y": y,
                "zoom": z,
                "occurrence_status": occurrence_status,
                "individual_count": individual_count,
            }
            upsert_observation_raw(conn, raw_row)
            touched.add((year, slot_id, z, tid))

        kept += 1
    return touched

def _replace_taxon_grid_from_rows(
    conn: sqlite3.Connection,
    taxon_id: int,
    zoom: int,
    slot_id: int,
    year: int,
    rows_in: Iterable[Tuple[int, int, int]],
) -> None:
    """
    Replace a single (taxon_id, zoom, year, slot_id) layer from aggregated rows:
      (tile_x, tile_y, observations_count)
    """
    now = utc_now_iso()

    conn.execute(
        "DELETE FROM taxon_grid WHERE taxon_id=? AND zoom=? AND year=? AND slot_id=?;",
        (taxon_id, zoom, int(year), slot_id),
    )

    rows = []
    for (x, y, obs_count) in rows_in:
        top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(int(x), int(y), zoom)
        rows.append(
            (
                taxon_id,
                zoom,
                int(year),
                slot_id,
                int(x),
                int(y),
                int(obs_count),
                1,
                float(top_lat),
                float(left_lon),
                float(bottom_lat),
                float(right_lon),
                now,
            )
        )

    if rows:
        conn.executemany(
            """
            INSERT INTO taxon_grid(
              taxon_id, zoom, year, slot_id, x, y, observations_count, taxa_count,
              bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
              fetched_at_utc
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            rows,
        )


def consolidate_taxon_grid_from_raw(
    conn: sqlite3.Connection,
    *,
    taxon_ids: Optional[List[int]] = None,
    zooms: Optional[List[int]] = None,
    years: Optional[List[int]] = None,
    slot_ids: Optional[List[int]] = None,
    include_slot0: bool = True,
) -> int:
    """
    Rebuild taxon_grid deterministically from observations_raw for the requested scope.

    Returns number of layers written.
    """
    where = []
    args: List[object] = []

    if taxon_ids:
        where.append("taxon_id IN ({})".format(",".join(["?"] * len(taxon_ids))))
        args.extend(int(t) for t in taxon_ids)
    if zooms:
        where.append("zoom IN ({})".format(",".join(["?"] * len(zooms))))
        args.extend(int(z) for z in zooms)
    if years:
        where.append("year IN ({})".format(",".join(["?"] * len(years))))
        args.extend(int(y) for y in years)
    if slot_ids:
        real_slots = [int(s) for s in slot_ids if int(s) != SLOT_ALL]
        if real_slots:
            where.append("slot_id IN ({})".format(",".join(["?"] * len(real_slots))))
            args.extend(real_slots)

    wh = ("WHERE " + " AND ".join(where)) if where else ""

    scopes = conn.execute(
        f"""
        SELECT DISTINCT taxon_id, year, zoom, slot_id
        FROM observations_raw
        {wh}
        ORDER BY taxon_id, year, zoom, slot_id;
        """,
        args,
    ).fetchall()

    layers_written = 0

    # Regular slots 1..48
    for scope in scopes:
        taxon_id = int(scope[0])
        year = int(scope[1])
        zoom = int(scope[2])
        slot_id = int(scope[3])

        rows = conn.execute(
            """
            SELECT tile_x, tile_y, COUNT(*) AS observations_count
            FROM observations_raw
            WHERE taxon_id=? AND year=? AND zoom=? AND slot_id=?
            GROUP BY tile_x, tile_y
            ORDER BY tile_x, tile_y;
            """,
            (taxon_id, year, zoom, slot_id),
        ).fetchall()

        with conn:
            _replace_taxon_grid_from_rows(
                conn,
                taxon_id,
                zoom,
                slot_id,
                year,
                [(int(r[0]), int(r[1]), int(r[2])) for r in rows],
            )
        layers_written += 1

    # Derived slot 0 per (taxon, year, zoom)
    if include_slot0:
        slot0_where = []
        slot0_args: List[object] = []
        if taxon_ids:
            slot0_where.append("taxon_id IN ({})".format(",".join(["?"] * len(taxon_ids))))
            slot0_args.extend(int(t) for t in taxon_ids)
        if zooms:
            slot0_where.append("zoom IN ({})".format(",".join(["?"] * len(zooms))))
            slot0_args.extend(int(z) for z in zooms)
        if years:
            slot0_where.append("year IN ({})".format(",".join(["?"] * len(years))))
            slot0_args.extend(int(y) for y in years)
        slot0_wh = ("WHERE " + " AND ".join(slot0_where)) if slot0_where else ""

        slot0_scopes = conn.execute(
            f"""
            SELECT DISTINCT taxon_id, year, zoom
            FROM observations_raw
            {slot0_wh}
            ORDER BY taxon_id, year, zoom;
            """,
            slot0_args,
        ).fetchall()

        for scope in slot0_scopes:
            taxon_id = int(scope[0])
            year = int(scope[1])
            zoom = int(scope[2])

            rows = conn.execute(
                """
                SELECT tile_x, tile_y, COUNT(*) AS observations_count
                FROM observations_raw
                WHERE taxon_id=? AND year=? AND zoom=?
                GROUP BY tile_x, tile_y
                ORDER BY tile_x, tile_y;
                """,
                (taxon_id, year, zoom),
            ).fetchall()

            with conn:
                _replace_taxon_grid_from_rows(
                    conn,
                    taxon_id,
                    zoom,
                    SLOT_ALL,
                    year,
                    [(int(r[0]), int(r[1]), int(r[2])) for r in rows],
                )
            layers_written += 1

    return layers_written




def make_sos_export_filter(
    *,
    taxon_ids: List[int],
    year_from: int,
    year_to: int,
) -> Dict[str, object]:
    return {
        "taxon": {
            "ids": taxon_ids,
            "includeUnderlyingTaxa": True,
        },
        "date": {
            "startDate": f"{year_from}-01-01T00:00:00Z",
            "endDate": f"{year_to}-12-31T23:59:59Z",
            "dateFilterType": "BetweenStartDateAndEndDate",
        },
    }

    
def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch SOS CSV exports in taxon batches and import into geomap.sqlite.")

    ap.add_argument("--db", required=True, type=str, help="Path to geomap.sqlite")
    ap.add_argument("--zooms", default="15,14,13", type=str)
    
    ap.add_argument("--taxon-list-csv", required=True, type=str)
    ap.add_argument("--batch-size", default=100, type=int)
    
    ap.add_argument("--year-from", required=True, type=int)
    ap.add_argument("--year-to", required=True, type=int)
    
    ap.add_argument("--csv-stash-dir", default="", type=str)
    ap.add_argument("--import-existing", default="", type=str, help="Debug: import an existing ZIP/CSV instead of fetching SOS")
    
    ap.add_argument("--include-slot0", action="store_true")
    ap.add_argument("--date-field", default="StartDate", choices=["StartDate", "EndDate"])
    ap.add_argument("--occurrence-status", default="present", type=str)


    args0 = ap.parse_args()

    db_path = Path(args0.db).expanduser().resolve()
    taxon_list_csv = Path(args0.taxon_list_csv).expanduser().resolve()

    zooms = [int(z.strip()) for z in args0.zooms.split(",") if z.strip()]
    zooms = sorted(set(zooms), reverse=True)
    if not zooms:
        raise SystemExit("No zooms provided")

    taxon_ids_all = read_taxon_ids_from_csv(taxon_list_csv)
    if not taxon_ids_all:
        raise SystemExit(f"No taxon ids found in {taxon_list_csv}")

    print(f"[import] loaded {len(taxon_ids_all)} taxon ids from {taxon_list_csv}")

    batches = list(chunked(taxon_ids_all, int(args0.batch_size)))
    occ = args0.occurrence_status.strip() or None

    cfg = Config(repo_root=REPO_ROOT)
    
    stash_dir = (
        Path(args0.csv_stash_dir).expanduser().resolve()
        if args0.csv_stash_dir.strip()
        else cfg.csv_stash_dir
    )
    stash_dir.mkdir(parents=True, exist_ok=True)
    
    conn = storage.connect(db_path)
    conn.isolation_level = None

    try:
        storage.ensure_schema(conn)
        
        total_touched_scopes = 0
        total_layers_written = 0

        for batch_index, batch_taxon_ids in enumerate(batches, start=1):
            print(f"[import] batch {batch_index}/{len(batches)} taxa={len(batch_taxon_ids)}")

            if args0.import_existing.strip():
                export_path = Path(args0.import_existing).expanduser().resolve()
                if not export_path.exists():
                    raise SystemExit(f"Missing --import-existing file: {export_path}")
            else:
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                export_path = stash_dir / f"{stamp}__batch{batch_index:04d}_taxa{len(batch_taxon_ids)}.zip"

                search_filter = make_sos_export_filter(
                    taxon_ids=batch_taxon_ids,
                    year_from=int(args0.year_from),
                    year_to=int(args0.year_to),
                )

                print(f"[export] batch {batch_index}: writing {export_path}")
                export_csv_zip_to_file(
                    cfg,
                    search_filter,
                    export_path,
                    output_field_set="All",
                    gzip=True,
                    culture_code="sv-SE",
                )
            
            if export_path.stat().st_size == 0:
                print(f"[import] batch {batch_index}: empty export, skipping")
                continue

            csv_or_zip = export_path
            if csv_or_zip.suffix.lower() == ".zip":
                extracted = find_csv_inside_zip(csv_or_zip)
                print(f"[import] batch {batch_index}: extracted CSV: {extracted}")
                csv_or_zip = extracted

            ingest = IngestArgs(
                zip_or_csv=csv_or_zip,
                db_path=db_path,
                zooms=zooms,
                taxon_ids=batch_taxon_ids,
                include_slot0=bool(args0.include_slot0),
                date_field=args0.date_field,
                occurrence_status=occ,
            )

            touched = import_observations_raw(conn, ingest)
            print(f"[import] batch {batch_index}: touched raw scopes: {len(touched)}")

            if not touched:
                continue

            years = sorted({k[0] for k in touched})
            slot_ids = sorted({k[1] for k in touched})
            zooms_scope = sorted({k[2] for k in touched}, reverse=True)
            taxon_ids_scope = sorted({k[3] for k in touched})

            layers_written = consolidate_taxon_grid_from_raw(
                conn,
                taxon_ids=taxon_ids_scope,
                years=years,
                slot_ids=slot_ids,
                zooms=zooms_scope,
                include_slot0=bool(args0.include_slot0),
            )
            
            total_touched_scopes += len(touched)
            total_layers_written += layers_written
            
            print(f"[import] batch {batch_index}: wrote consolidated layers: {layers_written}")
            
        print(f"[import] total touched raw scopes: {total_touched_scopes}")
        print(f"[import] total consolidated layers: {total_layers_written}")
        print("[import] done.")
        return 0

    finally:
        conn.close()
    
if __name__ == "__main__":
    raise SystemExit(main())
