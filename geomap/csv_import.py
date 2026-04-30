# geomap/csv_import.py

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

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional, List, Set
from datetime import date, datetime, timezone
import sqlite3
import csv
import zipfile

from .tiles import lonlat_to_tile_xy, tile_xy_to_bbox, ensure_tile_bbox_rows

SLOT_ALL = 0


@dataclass(frozen=True)
class IngestArgs:
    zip_or_csv: Path
    db_path: Path
    zooms: List[int]
    taxon_ids: Optional[List[int]]
    include_slot0: bool
    date_field: str  # StartDate or EndDate
    occurrence_status: Optional[str]  # e.g. "present"


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    
def normalize_occurrence_id(row: Dict[str, str]) -> str:
    return ((row.get("OccurrenceId") or row.get("\ufeffOccurrenceId") or "").strip())

    
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

        ON CONFLICT(occurrence_id, zoom)
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


def consolidate_taxon_grid_year_all_from_grid(
    conn,
    *,
    taxon_ids,
    years,
    slot_ids,
    zooms,
    include_slot0=True,
):
    if not taxon_ids:
        return 0

    taxon_ph = ",".join(["?"] * len(taxon_ids))
    year_ph  = ",".join(["?"] * len(years))
    slot_ph  = ",".join(["?"] * len(slot_ids))
    zoom_ph  = ",".join(["?"] * len(zooms))

    now = utc_now_iso()

    with conn:

        conn.execute(
            f"""
            DELETE FROM taxon_grid
            WHERE taxon_id IN ({taxon_ph})
              AND zoom IN ({zoom_ph})
              AND slot_id IN ({slot_ph})
              AND year=0;
            """,
            (*taxon_ids, *zooms, *slot_ids),
        )

        conn.execute(
            f"""
            INSERT INTO taxon_grid(
                taxon_id,
                zoom,
                year,
                slot_id,
                x,
                y,
                observations_count,
                taxa_count,
                bbox_top_lat,
                bbox_left_lon,
                bbox_bottom_lat,
                bbox_right_lon,
                fetched_at_utc
            )
            SELECT
                taxon_id,
                zoom,
                0,
                slot_id,
                x,
                y,
                SUM(observations_count),
                1,
                bbox_top_lat,
                bbox_left_lon,
                bbox_bottom_lat,
                bbox_right_lon,
                ?
            FROM taxon_grid
            WHERE taxon_id IN ({taxon_ph})
              AND zoom IN ({zoom_ph})
              AND slot_id IN ({slot_ph})
              AND year IN ({year_ph})
            GROUP BY
                taxon_id,
                zoom,
                slot_id,
                x,
                y;
            """,
            (now, *taxon_ids, *zooms, *slot_ids, *years),
        )

        if include_slot0:

            conn.execute(
                f"""
                DELETE FROM taxon_grid
                WHERE taxon_id IN ({taxon_ph})
                  AND zoom IN ({zoom_ph})
                  AND slot_id=?
                  AND year=0;
                """,
                (*taxon_ids, *zooms, SLOT_ALL),
            )

            conn.execute(
                f"""
                INSERT INTO taxon_grid(
                    taxon_id,
                    zoom,
                    year,
                    slot_id,
                    x,
                    y,
                    observations_count,
                    taxa_count,
                    bbox_top_lat,
                    bbox_left_lon,
                    bbox_bottom_lat,
                    bbox_right_lon,
                    fetched_at_utc
                )
                SELECT
                    taxon_id,
                    zoom,
                    0,
                    ?,
                    x,
                    y,
                    SUM(observations_count),
                    1,
                    bbox_top_lat,
                    bbox_left_lon,
                    bbox_bottom_lat,
                    bbox_right_lon,
                    ?
                FROM taxon_grid
                WHERE taxon_id IN ({taxon_ph})
                  AND zoom IN ({zoom_ph})
                  AND slot_id IN ({slot_ph})
                  AND year IN ({year_ph})
                GROUP BY
                    taxon_id,
                    zoom,
                    x,
                    y;
                """,
                (SLOT_ALL, now, *taxon_ids, *zooms, *slot_ids, *years),
            )

    regular_layers = (
        len(taxon_ids)
        * len(zooms)
        * len(slot_ids)
    )

    slot0_layers = (
        len(taxon_ids)
        * len(zooms)
        if include_slot0
        else 0
    )

    return regular_layers + slot0_layers

def consolidate_taxon_grid_from_raw_bulk_tile_bbox(
    conn: sqlite3.Connection,
    *,
    taxon_ids: Optional[List[int]] = None,
    zooms: Optional[List[int]] = None,
    years: Optional[List[int]] = None,
    slot_ids: Optional[List[int]] = None,
    include_slot0: bool = True,
) -> int:
    where = []
    args: list[object] = []

    def add_in(col: str, values: Optional[List[int]]) -> None:
        if values:
            vals = sorted({int(v) for v in values})
            where.append(f"{col} IN ({','.join(['?'] * len(vals))})")
            args.extend(vals)

    add_in("taxon_id", taxon_ids)
    add_in("zoom", zooms)
    add_in("year", years)

    real_slots = sorted({int(s) for s in (slot_ids or []) if int(s) != SLOT_ALL})
    if real_slots:
        where.append(f"slot_id IN ({','.join(['?'] * len(real_slots))})")
        args.extend(real_slots)

    wh = ("WHERE " + " AND ".join(where)) if where else ""

    scope_rows = conn.execute(
        f"""
        SELECT DISTINCT taxon_id, year, zoom, slot_id, tile_x, tile_y
        FROM observations_raw
        {wh};
        """,
        args,
    ).fetchall()

    if not scope_rows:
        return 0

    scope_taxa = sorted({int(r[0]) for r in scope_rows})
    scope_years = sorted({int(r[1]) for r in scope_rows})
    scope_zooms = sorted({int(r[2]) for r in scope_rows})
    scope_slots = sorted({int(r[3]) for r in scope_rows if int(r[3]) != SLOT_ALL})

    xys_by_zoom: dict[int, set[tuple[int, int]]] = {}
    for r in scope_rows:
        z = int(r[2])
        x = int(r[4])
        y = int(r[5])
        xys_by_zoom.setdefault(z, set()).add((x, y))

    now = utc_now_iso()

    taxon_ph = ",".join(["?"] * len(scope_taxa))
    year_ph = ",".join(["?"] * len(scope_years))
    zoom_ph = ",".join(["?"] * len(scope_zooms))
    slot_ph = ",".join(["?"] * len(scope_slots))

    with conn:
        ensure_tile_bbox_rows(
            conn,
            zooms=scope_zooms,
            xys_by_zoom=xys_by_zoom,
        )

        conn.execute(
            f"""
            DELETE FROM taxon_grid
            WHERE taxon_id IN ({taxon_ph})
              AND year IN ({year_ph})
              AND zoom IN ({zoom_ph})
              AND slot_id IN ({slot_ph});
            """,
            (*scope_taxa, *scope_years, *scope_zooms, *scope_slots),
        )

        conn.execute(
            f"""
            INSERT INTO taxon_grid(
                taxon_id, zoom, year, slot_id,
                x, y,
                observations_count, taxa_count,
                bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
                fetched_at_utc
            )
            SELECT
                r.taxon_id,
                r.zoom,
                r.year,
                r.slot_id,
                r.tile_x,
                r.tile_y,
                COUNT(*) AS observations_count,
                1 AS taxa_count,
                b.bbox_top_lat,
                b.bbox_left_lon,
                b.bbox_bottom_lat,
                b.bbox_right_lon,
                ?
            FROM observations_raw r
            JOIN tile_bbox b
              ON b.zoom = r.zoom
             AND b.x = r.tile_x
             AND b.y = r.tile_y
            WHERE r.taxon_id IN ({taxon_ph})
              AND r.year IN ({year_ph})
              AND r.zoom IN ({zoom_ph})
              AND r.slot_id IN ({slot_ph})
            GROUP BY
                r.taxon_id,
                r.zoom,
                r.year,
                r.slot_id,
                r.tile_x,
                r.tile_y;
            """,
            (now, *scope_taxa, *scope_years, *scope_zooms, *scope_slots),
        )

        if include_slot0:
            conn.execute(
                f"""
                DELETE FROM taxon_grid
                WHERE taxon_id IN ({taxon_ph})
                  AND year IN ({year_ph})
                  AND zoom IN ({zoom_ph})
                  AND slot_id=?;
                """,
                (*scope_taxa, *scope_years, *scope_zooms, SLOT_ALL),
            )

            conn.execute(
                f"""
                INSERT INTO taxon_grid(
                    taxon_id, zoom, year, slot_id,
                    x, y,
                    observations_count, taxa_count,
                    bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
                    fetched_at_utc
                )
                SELECT
                    r.taxon_id,
                    r.zoom,
                    r.year,
                    ? AS slot_id,
                    r.tile_x,
                    r.tile_y,
                    COUNT(*) AS observations_count,
                    1 AS taxa_count,
                    b.bbox_top_lat,
                    b.bbox_left_lon,
                    b.bbox_bottom_lat,
                    b.bbox_right_lon,
                    ?
                FROM observations_raw r
                JOIN tile_bbox b
                  ON b.zoom = r.zoom
                 AND b.x = r.tile_x
                 AND b.y = r.tile_y
                WHERE r.taxon_id IN ({taxon_ph})
                  AND r.year IN ({year_ph})
                  AND r.zoom IN ({zoom_ph})
                  AND r.slot_id IN ({slot_ph})
                GROUP BY
                    r.taxon_id,
                    r.zoom,
                    r.year,
                    r.tile_x,
                    r.tile_y;
                """,
                (SLOT_ALL, now, *scope_taxa, *scope_years, *scope_zooms, *scope_slots),
            )

    regular_layers = len(scope_taxa) * len(scope_years) * len(scope_zooms) * len(scope_slots)
    slot0_layers = len(scope_taxa) * len(scope_years) * len(scope_zooms) if include_slot0 else 0

    return regular_layers + slot0_layers

# Legacy -- to be removed
def consolidate_taxon_grid_from_raw_bulk_python_bbox(
    conn: sqlite3.Connection,
    *,
    taxon_ids: Optional[List[int]] = None,
    zooms: Optional[List[int]] = None,
    years: Optional[List[int]] = None,
    slot_ids: Optional[List[int]] = None,
    include_slot0: bool = True,
) -> int:
    where = []
    args: list[object] = []

    def add_in(col: str, values: Optional[List[int]]) -> None:
        if values:
            vals = sorted({int(v) for v in values})
            where.append(f"{col} IN ({','.join(['?'] * len(vals))})")
            args.extend(vals)

    add_in("taxon_id", taxon_ids)
    add_in("zoom", zooms)
    add_in("year", years)

    real_slots = sorted({int(s) for s in (slot_ids or []) if int(s) != SLOT_ALL})
    if real_slots:
        where.append(f"slot_id IN ({','.join(['?'] * len(real_slots))})")
        args.extend(real_slots)

    wh = ("WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(
        f"""
        SELECT
          taxon_id,
          zoom,
          year,
          slot_id,
          tile_x,
          tile_y,
          COUNT(*) AS observations_count
        FROM observations_raw
        {wh}
        GROUP BY taxon_id, zoom, year, slot_id, tile_x, tile_y
        ORDER BY taxon_id, zoom, year, slot_id, tile_x, tile_y;
        """,
        args,
    ).fetchall()

    if not rows:
        return 0

    now = utc_now_iso()

    scope_taxa = sorted({int(r[0]) for r in rows})
    scope_zooms = sorted({int(r[1]) for r in rows})
    scope_years = sorted({int(r[2]) for r in rows})
    scope_slots = sorted({int(r[3]) for r in rows})

    taxon_ph = ",".join(["?"] * len(scope_taxa))
    zoom_ph = ",".join(["?"] * len(scope_zooms))
    year_ph = ",".join(["?"] * len(scope_years))
    slot_ph = ",".join(["?"] * len(scope_slots))

    insert_rows = []
    layers = set()

    for taxon_id, zoom, year, slot_id, x, y, obs_count in rows:
        top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(int(x), int(y), int(zoom))
        insert_rows.append(
            (
                int(taxon_id),
                int(zoom),
                int(year),
                int(slot_id),
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
        layers.add((int(taxon_id), int(zoom), int(year), int(slot_id)))

    if include_slot0:
        slot0_rows = conn.execute(
            f"""
            SELECT
              taxon_id,
              zoom,
              year,
              tile_x,
              tile_y,
              COUNT(*) AS observations_count
            FROM observations_raw
            {wh}
            GROUP BY taxon_id, zoom, year, tile_x, tile_y
            ORDER BY taxon_id, zoom, year, tile_x, tile_y;
            """,
            args,
        ).fetchall()

        for taxon_id, zoom, year, x, y, obs_count in slot0_rows:
            top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(int(x), int(y), int(zoom))
            insert_rows.append(
                (
                    int(taxon_id),
                    int(zoom),
                    int(year),
                    SLOT_ALL,
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
            layers.add((int(taxon_id), int(zoom), int(year), SLOT_ALL))

    with conn:
        conn.execute(
            f"""
            DELETE FROM taxon_grid
            WHERE taxon_id IN ({taxon_ph})
              AND zoom IN ({zoom_ph})
              AND year IN ({year_ph})
              AND slot_id IN ({slot_ph});
            """,
            (*scope_taxa, *scope_zooms, *scope_years, *scope_slots),
        )

        if include_slot0:
            conn.execute(
                f"""
                DELETE FROM taxon_grid
                WHERE taxon_id IN ({taxon_ph})
                  AND zoom IN ({zoom_ph})
                  AND year IN ({year_ph})
                  AND slot_id=?;
                """,
                (*scope_taxa, *scope_zooms, *scope_years, SLOT_ALL),
            )

        conn.executemany(
            """
            INSERT INTO taxon_grid(
              taxon_id, zoom, year, slot_id,
              x, y,
              observations_count, taxa_count,
              bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
              fetched_at_utc
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            insert_rows,
        )

    return len(layers)


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

    
