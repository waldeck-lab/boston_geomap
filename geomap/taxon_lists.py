#!/usr/bin/env python3

# geomap:taxon_lists.py 

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

import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


REDLIST_ELEVATED_CATEGORIES = (
    "Nära hotad (NT)",
    "Kunskapsbrist (DD)",
    "Sårbar (VU)",
    "Starkt hotad (EN)",
    "Akut hotad (CR)",
    "Nationellt utdöd (RE)",
)



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

def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(str(db_path))
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _active_species_sql(where_extra: str = "") -> str:
    return f"""
        SELECT
            taxon_id,
            COALESCE(sci_name, '') AS scientific_name,
            COALESCE(swe_name, '') AS swedish_name,
            COALESCE(json_extract(raw_json, '$.redlistCategory'), '') AS redlist_category
        FROM taxa
        WHERE is_active = 1
          AND category = 'Species'
          AND type = 'Taxonomic'
          AND status = 'Accepted'
          {where_extra}
        ORDER BY taxon_id;
    """


def build_personal_missing_list(dyntaxa_db_path: Path, observed_db_path: Path) -> List[Dict[str, Any]]:
    """
    Build the current personal-target list:
    active accepted Dyntaxa species that do not exist in the personal SOS species_counts table.
    """
    with _connect_readonly(dyntaxa_db_path) as dyntaxa_conn, _connect_readonly(observed_db_path) as observed_conn:
        observed_rows = observed_conn.execute("SELECT taxon_id FROM species_counts;").fetchall()

        observed_ids: Set[int] = set()
        for row in observed_rows:
            try:
                observed_ids.add(int(row["taxon_id"]))
            except Exception:
                continue

        rows: List[Dict[str, Any]] = []
        for row in dyntaxa_conn.execute(_active_species_sql()).fetchall():
            taxon_id = int(row["taxon_id"])
            if taxon_id in observed_ids:
                continue
            rows.append({
                "taxon_id": taxon_id,
                "scientific_name": row["scientific_name"],
                "swedish_name": row["swedish_name"],
                "observations_count": 0,
                "redlist_category": row["redlist_category"],
            })
        return rows


def build_redlist_elevated_list(
    dyntaxa_db_path: Path,
    *,
    categories: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Build a list of active accepted Dyntaxa species with an elevated red-list category.
    By default this means any non-empty redlistCategory in the local Dyntaxa cache.
    """
    selected = [c.strip() for c in (categories or []) if c and c.strip()]

    args: list[Any] = []
    if selected:
        placeholders = ",".join(["?"] * len(selected))
        where_extra = f"AND json_extract(raw_json, '$.redlistCategory') IN ({placeholders})"
        args.extend(selected)
    else:
        where_extra = "AND COALESCE(json_extract(raw_json, '$.redlistCategory'), '') <> ''"

    with _connect_readonly(dyntaxa_db_path) as conn:
        rows = conn.execute(_active_species_sql(where_extra), args).fetchall()
        return [
            {
                "taxon_id": int(row["taxon_id"]),
                "scientific_name": row["scientific_name"],
                "swedish_name": row["swedish_name"],
                "observations_count": "",
                "redlist_category": row["redlist_category"],
            }
            for row in rows
        ]


def write_taxon_list_csv(rows: Iterable[Dict[str, Any]], out_path: Path) -> int:
    """
    Write a SOS-import-compatible taxon list CSV.
    taxon_id remains the first column for read_taxon_ids_from_csv().
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "taxon_id",
                "scientific_name",
                "swedish_name",
                "observations_count",
                "redlist_category",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "taxon_id": row.get("taxon_id", ""),
                "scientific_name": row.get("scientific_name", ""),
                "swedish_name": row.get("swedish_name", ""),
                "observations_count": row.get("observations_count", ""),
                "redlist_category": row.get("redlist_category", ""),
            })
            count += 1
    return count

