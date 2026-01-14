#!/usr/bin/env python3

# server/app.py

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

# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask, request, jsonify
from flask_cors import CORS


# repo root resolution
REPO_ROOT = Path(__file__).resolve().parents[1]
import sys
import argparse

import sqlite3

sys.path.insert(0, str(REPO_ROOT))

from geomap.config import Config
from geomap import storage
from geomap.sos_client import SOSClient, stable_gridcells_hash, throttle
from geomap.distance import haversine_km, distance_weight_rational, distance_weight_exp
from geomap.storage import YEAR_MAX, YEAR_MIN, YEAR_ALL
from geomap.config import SLOT_MIN, SLOT_MAX, SLOT_ALL


import threading
BUILD_LOCK = threading.Lock()

import logging
logger = logging.getLogger("geomap-server")

ZOOM_DEFAULT = 15  # server default if client doesn't send zooms

from werkzeug.exceptions import BadRequest, HTTPException

def parse_year(value: Any, *, name: str) -> int:
    try:
        y = int(value)
    except Exception:
        raise BadRequest(description=f"{name} must be an integer")

    if y == YEAR_ALL:
        return YEAR_ALL
    if y < YEAR_MIN or y > YEAR_MAX:
        raise BadRequest(description=f"{name} out of range: {y} (valid: {YEAR_MIN}..{YEAR_MAX}, or 0=all-years)")
    return y

def parse_year_range_args(args) -> tuple[int, int]:
    """
    Returns (year_from, year_to)
    If neither is provided => (0,0) meaning "all-years aggregate" (year=0).
    If one is provided => use it for both (single year).
    """
    yf = args.get("year_from", None)
    yt = args.get("year_to", None)

    if yf is None and yt is None:
        return (YEAR_ALL, YEAR_ALL)

    if yf is None:
        yf = yt
    if yt is None:
        yt = yf

    yf_i = parse_year(yf, name="year_from")
    yt_i = parse_year(yt, name="year_to")

    # If someone passes 0 explicitly, treat as "all-years aggregate"
    if yf_i == YEAR_ALL or yt_i == YEAR_ALL:
        return (YEAR_ALL, YEAR_ALL)

    if yf_i > yt_i:
        yf_i, yt_i = yt_i, yf_i
    return (yf_i, yt_i)

def parse_slot_ids_arg(value: Any, *, name: str = "slot_ids") -> list[int]:
    """
    Accepts:
      - "1,2,3" (string)
      - [1,2,3] (list)
      - single int
    Returns sorted unique slot ids (each validated with parse_slot_id),
    excluding SLOT_ALL unless it's the only value.
    """
    if value is None:
        raise BadRequest(description=f"{name} is required")

    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        vals = parts
    elif isinstance(value, (list, tuple)):
        vals = list(value)
    else:
        vals = [value]

    out: list[int] = []
    for v in vals:
        s = parse_slot_id(v, name=name)
        out.append(s)

    # unique + stable order
    out = sorted(set(out))

    # If SLOT_ALL is included along with others, it's ambiguous; reject.
    if SLOT_ALL in out and len(out) > 1:
        raise BadRequest(description=f"{name} cannot include 0 (all-time) together with specific slots")

    return out

def parse_slot_id(value: Any, *, name: str = "slot_id") -> int:
    try:
        slot = int(value)
    except Exception:
        raise BadRequest(description=f"{name} must be an integer")

    if slot < SLOT_MIN or slot > SLOT_MAX:
        raise BadRequest(
            description=f"{name} out of range: {slot} (valid: {SLOT_MIN}..{SLOT_MAX}, where {SLOT_ALL} means all-time)"
        )
    return slot

def _path_status(p: Path) -> str:
    try:
        if not p.exists():
            return "missing"
        if p.is_dir():
            return "dir"
        return f"file size={p.stat().st_size}"
    except Exception as e:
        return f"error({e})"

def _infer_default_server_logs_dir() -> Path:
    # Prefer stage/logs/server when running in OVE
    stage_root = os.getenv("OVE_STAGE_DIR", "").strip()
    if stage_root:
        return Path(stage_root).expanduser().resolve() / "logs" / "server"

    base = os.getenv("OVE_BASE_DIR", "").strip()
    if base:
        return Path(base).expanduser().resolve() / "stage" / "logs" / "server"

    # Non-OVE fallback
    return REPO_ROOT / "logs"


def parse_zooms(val) -> list[int]:
    if val is None:
        return [ZOOM_DEFAULT]

    if isinstance(val, str):
        parts = [p.strip() for p in val.split(",") if p.strip()]
        zs = [int(p) for p in parts]
    elif isinstance(val, (list, tuple)):
        zs = [int(z) for z in val]
    else:
        zs = [int(val)]

    # unique, sorted desc (highest zoom first)
    zs = sorted(set(zs), reverse=True)
    if not zs:
        raise ValueError("empty zooms")
    return zs

def read_taxa_rows(csv_path: Path, n: int) -> list[dict[str, Any]]:
    """
    Supports:
      1) header CSV with columns: taxon_id, scientific_name, swedish_name, ...
      2) legacy CSV where first column is taxon_id
    Returns rows with keys: taxon_id, scientific_name, swedish_name
    """
    import csv

    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))

    out: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        peek = f.read(4096)
        f.seek(0)

        # Heuristic: headered CSV
        if "taxon_id" in peek.splitlines()[0]:
            r = csv.DictReader(f)
            for rec in r:
                tid = (rec.get("taxon_id") or "").strip()
                if not tid.isdigit():
                    continue
                out.append(
                    {
                        "taxon_id": int(tid),
                        "scientific_name": (rec.get("scientific_name") or "").strip(),
                        "swedish_name": (rec.get("swedish_name") or "").strip(),
                    }
                )
                if n > 0 and len(out) >= n:
                    break
            return out

        # Legacy format
        r2 = csv.reader(f)
        for row in r2:
            if not row:
                continue
            tid = (row[0] or "").strip()
            if tid.isdigit():
                out.append({"taxon_id": int(tid), "scientific_name": "", "swedish_name": ""})
            if n > 0 and len(out) >= n:
                break
        return out


def make_app() -> Flask:
    app = Flask(__name__)
    CORS(app)  # keep it simple for local dev

    cfg = Config(repo_root=REPO_ROOT)

    logger.info("OVE_BASE_DIR=%s", os.getenv("OVE_BASE_DIR", ""))
    logger.info("OVE_STAGE_DIR=%s", os.getenv("OVE_STAGE_DIR", ""))
    logger.info("Resolved logs_dir=%s", cfg.logs_dir)
    
    logger.info("Resolved missing_species_csv=%s (%s)", cfg.missing_species_csv, _path_status(cfg.missing_species_csv))
    logger.info("Resolved geomap_db_path=%s (%s)", cfg.geomap_db_path, _path_status(cfg.geomap_db_path))
    logger.info("Resolved observed_db_path=%s (%s)", cfg.observed_db_path, _path_status(cfg.observed_db_path))
    logger.info("Resolved dyntaxa_db_path=%s (%s)", cfg.dyntaxa_db_path, _path_status(cfg.dyntaxa_db_path))
    
    if not cfg.geomap_db_path.exists():
        logger.warning("Geomap DB not found yet (will be created on first build?): %s", cfg.geomap_db_path)
    
    # Build SOS client once
    client = SOSClient(
        base_url=cfg.base_url,
        api_version=cfg.api_version,
        subscription_key=cfg.subscription_key,
        authorization=cfg.authorization,
    )
       
    @app.before_request
    def log_request():
        logger.info(
            "REQUEST %s %s from %s",
            request.method,
            request.path,
            request.remote_addr,
        )
    

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    @app.post("/api/pipeline/build")
    def pipeline_build():
        if not BUILD_LOCK.acquire(blocking=False):
            return jsonify({"ok": False, "code": "busy", "error": "Build already running"}), 409

        try:
            from datetime import datetime, timezone
            from geomap.timeslots import slot_bounds

            body = request.get_json(force=True) or {}

            # Backwards compatible inputs
            slot_id = parse_slot_id(body.get("slot_id", SLOT_ALL))  # single slot (0..48)

            # New input: list of slots to build (preferred when you want all 1..48)
            slot_ids_raw = body.get("slot_ids", None)
            if slot_ids_raw is None:
                slots_to_build = [slot_id]
            else:
                # Accept list or "1,2,3" string
                if isinstance(slot_ids_raw, str):
                    parts = [p.strip() for p in slot_ids_raw.split(",") if p.strip()]
                    slots_to_build = [parse_slot_id(p, name="slot_ids") for p in parts]
                elif isinstance(slot_ids_raw, (list, tuple)):
                    slots_to_build = [parse_slot_id(s, name="slot_ids") for s in slot_ids_raw]
                else:
                    slots_to_build = [parse_slot_id(slot_ids_raw, name="slot_ids")]

                # unique + sorted (0 first if present)
                slots_to_build = sorted(set(slots_to_build), key=lambda s: (s != 0, s))

            zooms = parse_zooms(body.get("zooms", [ZOOM_DEFAULT]))
            base_zoom = zooms[0]

            n = int(body.get("n", 5))
            alpha = float(body.get("alpha", cfg.hotmap_alpha))
            beta = float(body.get("beta", cfg.hotmap_beta))
            force = bool(body.get("force", False))

            # Seasonal build settings (for slots 1..48)
            # “All years but seasonal window” by default
            this_year = datetime.now(timezone.utc).year
            year_from = int(body.get("year_from", 2000))
            year_to = int(body.get("year_to", this_year))
            if year_to < year_from:
                year_from, year_to = year_to, year_from

            if not cfg.subscription_key:
                return jsonify({"ok": False, "error": "Missing ARTDATABANKEN_SUBSCRIPTION_KEY"}), 500
            if not cfg.authorization:
                return jsonify({"ok": False, "error": "Missing ARTDATABANKEN_AUTHORIZATION"}), 500

            taxa_rows = read_taxa_rows(cfg.missing_species_csv, n)
            taxon_ids = [t["taxon_id"] for t in taxa_rows]
            if not taxon_ids:
                return jsonify({"ok": False, "error": "No taxon ids found in CSV"}), 400

            def _iso_local_day_bounds(year: int, month: int, start_day: int, end_day: int) -> tuple[str, str]:
                # Use ISO date-time strings; SOS says “If no timezone is specified, GMT+1 (CEST) is assumed”.
                # We’ll include explicit UTC "Z" to be deterministic.
                start = datetime(year, month, start_day, 0, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                end = datetime(year, month, end_day, 23, 59, 59, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                return start, end

            def _extra_filter_for_slot_year(slot: int, year: int) -> dict[str, Any]:
                # slot 0 = all-time, no filter
                if slot == SLOT_ALL:
                    return {}

                m, q = ((slot - 1) // 4 + 1), ((slot - 1) % 4 + 1)
                ts = slot_bounds(m, q, year_for_days=year)  # correct month-length (Feb leap years, etc.)
                start_iso, end_iso = _iso_local_day_bounds(year, m, ts.start_day, ts.end_day)

                return {
                    "date": {
                        "startDate": start_iso,
                        "endDate": end_iso,
                        "dateFilterType": "BetweenStartDateAndEndDate",
                    }
                }

            def _merge_payloads_gridcells(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
                """
                Merge multiple GeoGridAggregation payloads by (x,y,zoom).
                - observationsCount: sum
                - taxaCount: max (safe-ish; SOS taxaCount meaning can vary; max avoids under-reporting)
                - boundingBox: union (min/max)
                """
                acc: dict[tuple[int, int, int], dict[str, Any]] = {}

                for p in payloads:
                    for c in (p.get("gridCells") or []):
                        x = int(c.get("x"))
                        y = int(c.get("y"))
                        z = int(c.get("zoom"))
                        key = (x, y, z)

                        obs = int(c.get("observationsCount") or 0)
                        taxa = int(c.get("taxaCount") or 0)

                        bb = c.get("boundingBox") or {}
                        tl = bb.get("topLeft") or {}
                        br = bb.get("bottomRight") or {}

                        top_lat = float(tl.get("latitude"))
                        left_lon = float(tl.get("longitude"))
                        bot_lat = float(br.get("latitude"))
                        right_lon = float(br.get("longitude"))

                        if key not in acc:
                            acc[key] = {
                                "x": x,
                                "y": y,
                                "zoom": z,
                                "observationsCount": obs,
                                "taxaCount": taxa,
                                "boundingBox": {
                                    "topLeft": {"latitude": top_lat, "longitude": left_lon},
                                    "bottomRight": {"latitude": bot_lat, "longitude": right_lon},
                                },
                            }
                        else:
                            a = acc[key]
                            a["observationsCount"] = int(a.get("observationsCount") or 0) + obs
                            a["taxaCount"] = max(int(a.get("taxaCount") or 0), taxa)

                            abb = a.get("boundingBox") or {}
                            atl = abb.get("topLeft") or {}
                            abr = abb.get("bottomRight") or {}

                            atl_lat = float(atl.get("latitude"))
                            atl_lon = float(atl.get("longitude"))
                            abr_lat = float(abr.get("latitude"))
                            abr_lon = float(abr.get("longitude"))

                            # Union of bounds
                            new_top_lat = max(atl_lat, top_lat)
                            new_left_lon = min(atl_lon, left_lon)
                            new_bot_lat = min(abr_lat, bot_lat)
                            new_right_lon = max(abr_lon, right_lon)

                            a["boundingBox"] = {
                                "topLeft": {"latitude": new_top_lat, "longitude": new_left_lon},
                                "bottomRight": {"latitude": new_bot_lat, "longitude": new_right_lon},
                            }

                # stable ordering
                out = list(acc.values())
                out.sort(key=lambda c: (int(c["x"]), int(c["y"])))
                return out

            conn = storage.connect(cfg.geomap_db_path)
            conn.isolation_level = None  # autocommit; avoids lingering read txns

            try:
                storage.ensure_schema(conn)
                logger.info(
                    "pipeline_build slots=%s zooms=%s base_zoom=%d years=%d..%d force=%s",
                    ",".join(map(str, slots_to_build)),
                    ",".join(map(str, zooms)),
                    base_zoom,
                    year_from,
                    year_to,
                    str(force),
                )

                throttle_state: dict[str, float] = {}

                # Build each requested slot
                for s in slots_to_build:

                    # For each slot we build:
                    #  1) year-specific layers for yr in [year_from..year_to]
                    #  2) an all-years aggregate (year=0) by merging the same year-specific payloads
                    for taxon_id in taxon_ids:
                        throttle(2.0, throttle_state)

                        # Fetch per-year payloads and write year buckets
                        yearly_payloads: list[dict[str, Any]] = []
                        
                        for yr in range(year_from, year_to + 1):
                            if s == SLOT_ALL:
                                # Slot 0: constrain to that calendar year
                                extra = {
                                    "date": {
                                        "startDate": f"{yr}-01-01T00:00:00Z",
                                        "endDate": f"{yr}-12-31T23:59:59Z",
                                        "dateFilterType": "BetweenStartDateAndEndDate",
                                    }
                                }
                            else:
                                # Slot 1..48: seasonal bounds for that year
                                extra = _extra_filter_for_slot_year(s, yr)

                            payload_y = client.geogrid_aggregation_resilient([taxon_id], zoom=base_zoom,extra_filter=extra)
                            grid_cells_y = payload_y.get("gridCells") or []
                            sha_y = stable_gridcells_hash(payload_y)
                            
                            yearly_payloads.append(payload_y)
                            
                            prev_y = storage.get_layer_state(conn, taxon_id, base_zoom, yr, s)
                            unchanged_y = (prev_y is not None and prev_y[1] == sha_y)

                            if (not unchanged_y) or force:
                                with conn:
                                    storage.replace_taxon_grid(conn, taxon_id, base_zoom, yr, s, grid_cells_y)
                                    storage.upsert_layer_state(conn, taxon_id, base_zoom, yr, s, sha_y, len(grid_cells_y))

                            # Derived zooms for this (taxon, slot, year)
                            src_zoom = base_zoom
                            src_sha = sha_y
                            for dst_zoom in zooms[1:]:
                                with conn:
                                    storage.materialize_parent_zoom_from_child(
                                        conn,
                                        taxon_id=taxon_id,
                                        slot_id=s,
                                        year=yr,
                                        src_zoom=src_zoom,
                                        dst_zoom=dst_zoom,
                                        src_sha=src_sha,
                                    )
                                    src_zoom = dst_zoom

                        # Build / refresh all-years aggregate bucket year=0
                        merged_cells = _merge_payloads_gridcells(yearly_payloads)
                        merged_sha = stable_gridcells_hash({"gridCells": merged_cells})

                        prev_all = storage.get_layer_state(conn, taxon_id, base_zoom, YEAR_ALL, s)
                        unchanged_all = (prev_all is not None and prev_all[1] == merged_sha)

                        if (not unchanged_all) or force:
                            with conn:
                                storage.replace_taxon_grid(conn, taxon_id, base_zoom, YEAR_ALL, s, merged_cells)
                                storage.upsert_layer_state(conn, taxon_id, base_zoom, YEAR_ALL, s, merged_sha, len(merged_cells))

                        # Derived zooms for (year=0)
                        src_zoom = base_zoom
                        src_sha = merged_sha
                        for dst_zoom in zooms[1:]:
                            with conn:
                                storage.materialize_parent_zoom_from_child(
                                    conn,
                                    taxon_id=taxon_id,
                                    slot_id=s,
                                    year=YEAR_ALL,
                                    src_zoom=src_zoom,
                                    dst_zoom=dst_zoom,
                                    src_sha=src_sha,
                                )
                            src_zoom = dst_zoom

                    # Upsert taxon dim once per slot
                    with conn:
                        storage.upsert_taxon_dim(
                            conn,
                            [(t["taxon_id"], t["scientific_name"], t["swedish_name"]) for t in taxa_rows],
                        )

                    # Rebuild hotmaps for each year bucket + all-years bucket
                    for yr in list(range(year_from, year_to + 1)) + [YEAR_ALL]:
                        for z in zooms:
                            with conn:
                                storage.rebuild_hotmap(conn, z, yr, s, taxon_ids, alpha=alpha, beta=beta)
                
                return jsonify(
                    {
                        "ok": True,
                        "slots_built": slots_to_build,
                        "zooms": zooms,
                        "base_zoom": base_zoom,
                        "n_taxa": len(taxon_ids),
                        "alpha": alpha,
                        "beta": beta,
                        "year_from": year_from,
                        "year_to": year_to,
                    }
                )

            finally:
                conn.close()

        finally:
            BUILD_LOCK.release()

    @app.errorhandler(Exception)
    def handle_any_exception(e: Exception):
        logger.exception("Unhandled exception: %s", e)
        return jsonify({"ok": False, "code": "internal_error", "error": str(e), "status": 500}), 500

    @app.errorhandler(HTTPException)
    def handle_http_exception(e: HTTPException):
        # e.code is the HTTP status (e.g. 400)
        # e.description is what we set above
        return jsonify({
            "ok": False,
            "code": "bad_request" if e.code == 400 else "http_error",
            "error": e.description,
            "status": e.code,
        }), e.code

    @app.errorhandler(sqlite3.OperationalError)
    def handle_sqlite_operational(e):
        msg = str(e).lower()
        if "database is locked" in msg:
            return jsonify({"ok": False, "code": "db_locked", "error": str(e), "status": 503}), 503
        return jsonify({"ok": False, "code": "db_error", "error": str(e), "status": 500}), 500

    @app.get("/api/hotmap")
    def hotmap_geojson():
        zoom = int(request.args.get("zoom", "15"))
        slot_id = parse_slot_id(request.args.get("slot_id", SLOT_ALL))
        year_from, year_to = parse_year_range_args(request.args)

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                nrows = conn.execute(
                    "SELECT COUNT(*) FROM grid_hotmap WHERE zoom=? AND year=? AND slot_id=?;",
                    (zoom, YEAR_ALL, slot_id),
                ).fetchone()[0]

                logger.info("hotmap request zoom=%d slot=%d year=0 rows=%d", zoom, slot_id, int(nrows))

                rows = conn.execute(
                    """
                    SELECT slot_id, x, y, coverage, score,
                        bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    FROM grid_hotmap
                    WHERE zoom=? AND year=? AND slot_id=?
                    ORDER BY coverage DESC, score DESC;
                    """,
                    (zoom, YEAR_ALL, slot_id),
                ).fetchall()

            else:
                nrows = conn.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT 1
                        FROM grid_hotmap
                        WHERE zoom=? AND slot_id=? AND year BETWEEN ? AND ?
                        GROUP BY slot_id, x, y
                    );
                    """,
                    (zoom, slot_id, year_from, year_to),
                ).fetchone()[0]

                logger.info(
                    "hotmap request zoom=%d slot=%d year=%d..%d rows=%d",
                    zoom, slot_id, year_from, year_to, int(nrows)
                )

                # Important: aggregate across years to avoid duplicate tiles
                rows = conn.execute(
                    """
                    SELECT
                    slot_id, x, y,
                    MAX(coverage) AS coverage,
                    MAX(score)    AS score,
                    bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    FROM grid_hotmap
                    WHERE zoom=? AND slot_id=? AND year BETWEEN ? AND ?
                    GROUP BY slot_id, x, y, bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    ORDER BY coverage DESC, score DESC;
                    """,
                    (zoom, slot_id, year_from, year_to),
                ).fetchall()

            features = []
            for (slot_id_db, x, y, coverage, score, top_lat, left_lon, bottom_lat, right_lon) in rows:
                poly = [
                    [float(left_lon), float(top_lat)],
                    [float(right_lon), float(top_lat)],
                    [float(right_lon), float(bottom_lat)],
                    [float(left_lon), float(bottom_lat)],
                    [float(left_lon), float(top_lat)],
                ]
                features.append(
                    {
                        "type": "Feature",
                        "properties": {
                            "zoom": int(zoom),
                            "slot_id": int(slot_id_db),
                            "year_from": None if (year_from == YEAR_ALL) else int(year_from),
                            "year_to": None if (year_to == YEAR_ALL) else int(year_to),
                            "x": int(x),
                            "y": int(y),
                            "coverage": int(coverage),
                            "score": float(score),
                        },
                        "geometry": {"type": "Polygon", "coordinates": [poly]},
                    }
                )

            return jsonify({"type": "FeatureCollection", "features": features})
        finally:
            conn.close()

    @app.get("/api/hotmap_window")
    def hotmap_window_geojson():
        zoom = int(request.args.get("zoom", "15"))
        slot_ids = parse_slot_ids_arg(request.args.get("slot_ids", None), name="slot_ids")
        year_from, year_to = parse_year_range_args(request.args)

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            if not slot_ids:
                return jsonify({"type": "FeatureCollection", "features": []})

            placeholders = ",".join(["?"] * len(slot_ids))

            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                rows = conn.execute(
                    f"""
                    SELECT slot_id, x, y, coverage, score,
                        bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    FROM grid_hotmap
                    WHERE zoom=? AND year=? AND slot_id IN ({placeholders})
                    ORDER BY slot_id ASC, coverage DESC, score DESC;
                    """,
                    (zoom, YEAR_ALL, *slot_ids),
                ).fetchall()

                logger.info(
                    "hotmap_window request zoom=%d year=0 slots=%s rows=%d",
                    zoom, ",".join(map(str, slot_ids)), len(rows)
                )

            else:
                rows = conn.execute(
                    f"""
                    SELECT
                    slot_id, x, y,
                    MAX(coverage) AS coverage,
                    MAX(score)    AS score,
                    bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    FROM grid_hotmap
                    WHERE zoom=? AND year BETWEEN ? AND ? AND slot_id IN ({placeholders})
                    GROUP BY slot_id, x, y, bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    ORDER BY slot_id ASC, coverage DESC, score DESC;
                    """,
                    (zoom, year_from, year_to, *slot_ids),
                ).fetchall()

                logger.info(
                    "hotmap_window request zoom=%d year=%d..%d slots=%s rows=%d",
                    zoom, year_from, year_to, ",".join(map(str, slot_ids)), len(rows)
                )

            features = []
            for (slot_id_db, x, y, coverage, score, top_lat, left_lon, bottom_lat, right_lon) in rows:
                poly = [
                    [float(left_lon), float(top_lat)],
                    [float(right_lon), float(top_lat)],
                    [float(right_lon), float(bottom_lat)],
                    [float(left_lon), float(bottom_lat)],
                    [float(left_lon), float(top_lat)],
                ]
                features.append(
                    {
                        "type": "Feature",
                        "properties": {
                            "zoom": int(zoom),
                            "slot_id": int(slot_id_db),
                            "year_from": None if (year_from == YEAR_ALL) else int(year_from),
                            "year_to": None if (year_to == YEAR_ALL) else int(year_to),
                            "x": int(x),
                            "y": int(y),
                            "coverage": int(coverage),
                            "score": float(score),
                        },
                        "geometry": {"type": "Polygon", "coordinates": [poly]},
                    }
                )

            return jsonify({"type": "FeatureCollection", "features": features})
        finally:
            conn.close()

    @app.get("/api/cell/taxa")
    def cell_taxa():
        zoom = int(request.args.get("zoom", "15"))
        slot_id = parse_slot_id(request.args.get("slot_id", SLOT_ALL))
        x = int(request.args["x"])
        y = int(request.args["y"])
        limit = int(request.args.get("limit", "200"))
        year_from, year_to = parse_year_range_args(request.args)

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                rows = conn.execute(
                    """
                    SELECT taxon_id, scientific_name, swedish_name, observations_count
                    FROM grid_hotmap_taxa_names_v
                    WHERE zoom=? AND year=? AND slot_id=? AND x=? AND y=?
                    ORDER BY observations_count DESC, taxon_id
                    LIMIT ?;
                    """,
                    (zoom, YEAR_ALL, slot_id, x, y, limit),
                ).fetchall()
            else:
                # Aggregate across years for the same cell
                rows = conn.execute(
                    """
                    SELECT
                    taxon_id,
                    COALESCE(MAX(scientific_name), '') AS scientific_name,
                    COALESCE(MAX(swedish_name), '') AS swedish_name,
                    SUM(observations_count) AS observations_count
                    FROM grid_hotmap_taxa_names_v
                    WHERE zoom=? AND slot_id=? AND x=? AND y=? AND year BETWEEN ? AND ?
                    GROUP BY taxon_id
                    ORDER BY observations_count DESC, taxon_id
                    LIMIT ?;
                    """,
                    (zoom, slot_id, x, y, year_from, year_to, limit),
                ).fetchall()

            out = []
            for r in rows:
                out.append(
                    {
                        "taxon_id": int(r[0]),
                        "scientific_name": r[1] or "",
                        "swedish_name": r[2] or "",
                        "observations_count": int(r[3] or 0),
                    }
                )
            return jsonify(out)
        finally:
            conn.close()
            

    @app.get("/api/cell/taxa_window")
    def cell_taxa_window():
        zoom = int(request.args.get("zoom", "15"))
        slot_ids = parse_slot_ids_arg(request.args.get("slot_ids", None), name="slot_ids")
        x = int(request.args["x"])
        y = int(request.args["y"])
        limit = int(request.args.get("limit", "200"))
        year_from, year_to = parse_year_range_args(request.args)

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            if not slot_ids:
                return jsonify([])

            placeholders = ",".join(["?"] * len(slot_ids))

            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                rows = conn.execute(
                    f"""
                    SELECT
                    taxon_id,
                    COALESCE(MAX(scientific_name), '') AS scientific_name,
                    COALESCE(MAX(swedish_name), '') AS swedish_name,
                    SUM(observations_count) AS observations_count
                    FROM grid_hotmap_taxa_names_v
                    WHERE zoom=? AND year=? AND x=? AND y=? AND slot_id IN ({placeholders})
                    GROUP BY taxon_id
                    ORDER BY observations_count DESC, taxon_id
                    LIMIT ?;
                    """,
                    (zoom, YEAR_ALL, x, y, *slot_ids, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                    taxon_id,
                    COALESCE(MAX(scientific_name), '') AS scientific_name,
                    COALESCE(MAX(swedish_name), '') AS swedish_name,
                    SUM(observations_count) AS observations_count
                    FROM grid_hotmap_taxa_names_v
                    WHERE zoom=? AND x=? AND y=? AND year BETWEEN ? AND ? AND slot_id IN ({placeholders})
                    GROUP BY taxon_id
                    ORDER BY observations_count DESC, taxon_id
                    LIMIT ?;
                    """,
                    (zoom, x, y, year_from, year_to, *slot_ids, limit),
                ).fetchall()

            logger.info(
                "cell_taxa_window zoom=%d x=%d y=%d year=%s slots=%s rows=%d",
                zoom, x, y,
                "0" if (year_from == YEAR_ALL and year_to == YEAR_ALL) else f"{year_from}..{year_to}",
                ",".join(map(str, slot_ids)),
                len(rows),
            )

            out = []
            for r in rows:
                out.append(
                    {
                        "taxon_id": int(r[0]),
                        "scientific_name": r[1] or "",
                        "swedish_name": r[2] or "",
                        "observations_count": int(r[3] or 0),
                    }
                )
            return jsonify(out)
        finally:
            conn.close()

            
    @app.get("/api/rank_nearby")
    def rank_nearby():
        lat = float(request.args.get("lat", "55.667"))
        lon = float(request.args.get("lon", "13.350"))
        zoom = int(request.args.get("zoom", "15"))
        slot_id = parse_slot_id(request.args.get("slot_id", SLOT_ALL))
        max_km = float(request.args.get("max_km", "250"))
        mode = (request.args.get("mode", "rational") or "rational").lower()
        d0_km = float(request.args.get("d0_km", "30"))
        gamma = float(request.args.get("gamma", "2.0"))
        limit = int(request.args.get("limit", "20"))

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None  # autocommit; avoids lingering read txns
        try:
            storage.ensure_schema(conn)
            year_from, year_to = parse_year_range_args(request.args)

            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                candidate_rows = conn.execute(
                    """
                    SELECT zoom, year, slot_id, x, y, coverage, score,
                    centroid_lat, centroid_lon,
                    topLeft_lat, topLeft_lon, bottomRight_lat, bottomRight_lon,
                    obs_total, taxa_list
                    FROM grid_hotmap_v
                    WHERE zoom=? AND year=? AND slot_id=?
                    ORDER BY coverage DESC, score DESC
                    LIMIT 4000;
                    """,
                    (zoom, YEAR_ALL, slot_id),
                ).fetchall()
            else:
                # Aggregate across years first to avoid duplicates per tile
                candidate_rows = conn.execute(
                    """
                    SELECT
                    zoom,
                    ? AS year,
                    slot_id,
                    x,
                    y,
                    MAX(coverage) AS coverage,
                    MAX(score)    AS score,
                    (bbox_top_lat + bbox_bottom_lat) / 2.0 AS centroid_lat,
                    (bbox_left_lon + bbox_right_lon) / 2.0 AS centroid_lon,
                    bbox_top_lat    AS topLeft_lat,
                    bbox_left_lon   AS topLeft_lon,
                    bbox_bottom_lat AS bottomRight_lat,
                    bbox_right_lon  AS bottomRight_lon,
                    0 AS obs_total,
                    '' AS taxa_list
                    FROM grid_hotmap
                    WHERE zoom=? AND slot_id=? AND year BETWEEN ? AND ?
                    GROUP BY zoom, slot_id, x, y, bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                    ORDER BY coverage DESC, score DESC
                    LIMIT 4000;
                    """,
                    (YEAR_ALL, zoom, slot_id, year_from, year_to),
                ).fetchall()
            
            scored = []
            seen: set[tuple[int, int, int, int]] = set()

            for row in candidate_rows:
                key = (int(row["zoom"]), int(row["year"]), int(row["slot_id"]), int(row["x"]), int(row["y"]))

                if key in seen:
                    continue
                seen.add(key)

                c_lat = float(row["centroid_lat"])
                c_lon = float(row["centroid_lon"])
                d_km = haversine_km(lat, lon, c_lat, c_lon)
                if d_km > max_km:
                    continue

                base_score = float(row["score"])
                if mode == "exp":
                    w = distance_weight_exp(d_km, d0_km)
                else:
                    w = distance_weight_rational(d_km, d0_km, gamma)

                scored.append((base_score * w, d_km, row))

            scored.sort(key=lambda t: (-t[0], t[1]))
            out = []
            for (dw_score, d_km, r) in scored[:limit]:
                out.append(
                    {
                        "dw_score": float(dw_score),
                        "dist_km": float(d_km),
                        "zoom": int(r["zoom"]),
                        "year": int(r["year"]),
                        "slot_id": int(r["slot_id"]),
                        "x": int(r["x"]),
                        "y": int(r["y"]),
                        "coverage": int(r["coverage"]),
                        "score": float(r["score"]),
                        "taxa_list": (r["taxa_list"] or ""),
                        "obs_total": int(r["obs_total"] or 0),
                    }
                )
            return jsonify(out)
        finally:
            conn.close()

    return app



if __name__ == "__main__":
    import argparse
    import os
    from pathlib import Path

    ap = argparse.ArgumentParser(description="Geomap dev API server (OVE-friendly).")
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8088")))
    ap.add_argument("--db-dir", default=None, help="Override DB directory (expects geomap.sqlite).")
    ap.add_argument("--lists-dir", default=None, help="Override lists directory (expects missing_species.csv).")
    ap.add_argument("--logs-dir", default=None, help="Override logs directory (default: stage/logs/server in OVE).")
    args = ap.parse_args()

    # Default logs dir in OVE if not provided
    if args.logs_dir is None:
        ove_stage = os.environ.get("OVE_STAGE_DIR", "").strip()
        ove_base = os.environ.get("OVE_BASE_DIR", "").strip()
        stage = Path(ove_stage).expanduser().resolve() if ove_stage else (Path(ove_base).expanduser().resolve() / "stage" if ove_base else None)
        if stage:
            args.logs_dir = str(stage / "logs" / "server")

    # Map CLI dirs into GEOMAP_* env vars so Config sees them
    from geomap.cli_paths import apply_path_overrides
    apply_path_overrides(
        db_dir=args.db_dir,
        lists_dir=args.lists_dir,
        logs_dir=args.logs_dir,
    )
    logger.info("GEOMAP_DB=%s", os.getenv("GEOMAP_DB"))
    logger.info("GEOMAP_OBSERVED_DB=%s", os.getenv("GEOMAP_OBSERVED_DB"))
    logger.info("GEOMAP_DYNTAXA_DB=%s", os.getenv("GEOMAP_DYNTAXA_DB"))
    logger.info("GEOMAP_MISSING_SPECIES_CSV=%s", os.getenv("GEOMAP_MISSING_SPECIES_CSV"))
    logger.info("GEOMAP_LOGS_DIR=%s", os.getenv("GEOMAP_LOGS_DIR"))
    
    # Reconfigure the module-level logger to use the final logs dir
    from server.logging_utils import setup_server_logger
    setup_server_logger(
        name="geomap-server",
        log_dir=Path(args.logs_dir).expanduser().resolve() if args.logs_dir else None,)
    
    log_dir = Path(args.logs_dir).expanduser().resolve() if args.logs_dir else None
    logger = setup_server_logger(name="geomap-server", log_dir=log_dir)
    
    logger.info("Starting server with host=%s port=%d", args.host, args.port)
    if args.db_dir:
        logger.info("DB dir override: %s", args.db_dir)
    if args.lists_dir:
        logger.info("Lists dir override: %s", args.lists_dir)
    if args.logs_dir:
        logger.info("Logs dir: %s", str(log_dir))

    app = make_app()

    debug = bool(os.environ.get("GEOMAP_SERVER_DEBUG", "1") == "1")
    use_reloader = bool(os.environ.get("GEOMAP_SERVER_RELOAD", "0") == "1")

    app.run(
        host=args.host,
        port=args.port,
        debug=debug,
        use_reloader=use_reloader,
    )

