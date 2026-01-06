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


import threading
BUILD_LOCK = threading.Lock()

import logging
logger = logging.getLogger("geomap-server")

ZOOM_DEFAULT = 15  # server default if client doesn't send zooms

# Consolidate with ./geomap/config.py, import from geomap later.
#from geomap.config import SLOT_MIN, SLOT_MAX, SLOT_ALL
SLOT_MIN = 0
SLOT_MAX = 48
SLOT_ALL = 0

from werkzeug.exceptions import BadRequest, HTTPException

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
            body = request.get_json(force=True) or {}
            slot_id = parse_slot_id(body.get("slot_id", SLOT_ALL))
            zooms = parse_zooms(body.get("zooms",[ZOOM_DEFAULT])) 
            base_zoom = zooms[0]
            n = int(body.get("n", 5))
            alpha = float(body.get("alpha", cfg.hotmap_alpha))
            beta = float(body.get("beta", cfg.hotmap_beta))
            force = bool(body.get("force", False))

            if not cfg.subscription_key:
                return jsonify({"ok": False, "error": "Missing ARTDATABANKEN_SUBSCRIPTION_KEY"}), 500
            if not cfg.authorization:
                return jsonify({"ok": False, "error": "Missing ARTDATABANKEN_AUTHORIZATION"}), 500

            taxa_rows = read_taxa_rows(cfg.missing_species_csv, n)
            taxon_ids = [t["taxon_id"] for t in taxa_rows]
            if not taxon_ids:
                return jsonify({"ok": False, "error": "No taxon ids found in CSV"}), 400

            conn = storage.connect(cfg.geomap_db_path)
            conn.isolation_level = None  # autocommit; avoids lingering read txns
            try:
                storage.ensure_schema(conn)
                logger.info("sqlite isolation_level=%r in_transaction=%s", conn.isolation_level, conn.in_transaction)

                # DB sanity check
                tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
                logger.info("DB tables: %s", tables)

                # optional, quick counts:
                try:
                    c = conn.execute("SELECT COUNT(*) FROM taxon_grid;").fetchone()[0]
                    logger.info("taxon_grid rows: %d", int(c))
                except Exception:
                    pass
            
                throttle_state: dict[str, float] = {}
                for taxon_id in taxon_ids:
                    throttle(2.0, throttle_state)

                    payload = client.geogrid_aggregation([taxon_id], zoom=base_zoom)
                    grid_cells = payload.get("gridCells") or []
                    base_sha = stable_gridcells_hash(payload)

                    prev = storage.get_layer_state(conn, taxon_id, base_zoom, slot_id)
                    unchanged = (prev is not None and prev[1] == base_sha)

                    if unchanged and not force:
                        # still ensure derived zooms exist; simplest: rebuild derived always if you want
                        pass
                    else:
                        with conn:
                            storage.replace_taxon_grid(conn, taxon_id, base_zoom, slot_id, grid_cells)
                            storage.upsert_layer_state(conn, taxon_id, base_zoom, slot_id, base_sha, len(grid_cells))
                    # Derived zooms: materialize from the closest available source (step-wise)
                    # If you already implemented src_sha tagging, pass it here.
                    src_zoom = base_zoom
                    for dst_zoom in zooms[1:]:
                        # assumes you implemented: materialize_parent_zoom_from_child(...)
                        with conn:
                            storage.materialize_parent_zoom_from_child(
                                conn,
                                taxon_id=taxon_id,
                                slot_id=slot_id,
                                src_zoom=src_zoom,
                                dst_zoom=dst_zoom,
                                src_sha=base_sha,
                            )
                        src_zoom = dst_zoom

                # upsert taxons for readability
                with conn:
                    storage.upsert_taxon_dim(
                        conn,
                        [(t["taxon_id"], t["scientific_name"], t["swedish_name"]) for t in taxa_rows],
                    )
                # Build hotmaps for each zoom requested
                for z in zooms:
                    with conn:
                        storage.rebuild_hotmap(conn, z, slot_id, taxon_ids, alpha=alpha, beta=beta)

                return jsonify(
                    {
                        "ok": True,
                        "slot_id": slot_id,
                        "zooms": zooms,
                        "base_zoom": base_zoom,
                        "n_taxa": len(taxon_ids),
                        "alpha": alpha,
                        "beta": beta,
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

        cfg_local = cfg
        
        conn = storage.connect(cfg_local.geomap_db_path)
        conn.isolation_level = None  # autocommit; avoids lingering read txns

        # Produce geojson
        try:            
            storage.ensure_schema(conn)
            # Make sure an non-empty db is found
            nrows = conn.execute("SELECT COUNT(*) FROM grid_hotmap WHERE zoom=? AND slot_id=?;", (zoom, slot_id)).fetchone()[0]
            logger.info("hotmap request zoom=%d slot=%d rows=%d", zoom, slot_id, int(nrows))

            rows = conn.execute(
                """
                SELECT slot_id, x, y, coverage, score,
                       bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                FROM grid_hotmap
                WHERE zoom=? AND slot_id=?
                ORDER BY coverage DESC, score DESC;
                """,
                (zoom, slot_id),
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

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None  # autocommit; avoids lingering read txns
        try:
            storage.ensure_schema(conn)

            if not slot_ids:
                return jsonify({"type": "FeatureCollection", "features": []})

            placeholders = ",".join(["?"] * len(slot_ids))

            rows = conn.execute(
                f"""
                SELECT slot_id, x, y, coverage, score,
                       bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon
                FROM grid_hotmap
                WHERE zoom=? AND slot_id IN ({placeholders})
                ORDER BY slot_id ASC, coverage DESC, score DESC;
                """,
                (zoom, *slot_ids),
            ).fetchall()

            logger.info("hotmap_window request zoom=%d slots=%s rows=%d", zoom, ",".join(map(str, slot_ids)), len(rows))

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

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None  # autocommit; avoids lingering read txns
        try:
            storage.ensure_schema(conn)


            # Debug: do we even have any per-taxon rows for this cell?
            try:
                c1 = conn.execute(
                    "SELECT COUNT(*) FROM grid_hotmap_taxa WHERE zoom=? AND slot_id=? AND x=? AND y=?;",
                    (zoom, slot_id, x, y),
                ).fetchone()[0]
            except Exception:
                c1 = None

            try:
                c2 = conn.execute(
                    "SELECT COUNT(*) FROM taxon_grid WHERE zoom=? AND slot_id=? AND x=? AND y=?;",
                    (zoom, slot_id, x, y),
                ).fetchone()[0]
            except Exception:
                c2 = None

            logger.info("cell_taxa debug zoom=%d slot=%d x=%d y=%d cnt_hotmap_taxa=%s cnt_taxon_grid=%s",
                        zoom, slot_id, x, y, str(c1), str(c2))
            
            rows = conn.execute(
                """
                SELECT taxon_id, scientific_name, swedish_name, observations_count
                FROM grid_hotmap_taxa_names_v
                WHERE zoom=? AND slot_id=? AND x=? AND y=?
                ORDER BY observations_count DESC, taxon_id
                LIMIT ?;
                """,
                (zoom, slot_id, x, y, limit),
            ).fetchall()

            out = []
            for r in rows:
                out.append(
                    {
                        "taxon_id": int(r[0]),
                        "scientific_name": r[1] or "",
                        "swedish_name": r[2] or "",
                        "observations_count": int(r[3]),
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

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None  # autocommit; avoids lingering read txns
        try:
            storage.ensure_schema(conn)

            if not slot_ids:
                return jsonify([])

            placeholders = ",".join(["?"] * len(slot_ids))

            # NOTE:
            # We aggregate from the view you already use (grid_hotmap_taxa_names_v),
            # but across multiple slot_id values.
            rows = conn.execute(
                f"""
                SELECT
                  taxon_id,
                  COALESCE(MAX(scientific_name), '') AS scientific_name,
                  COALESCE(MAX(swedish_name), '') AS swedish_name,
                  SUM(observations_count) AS observations_count
                FROM grid_hotmap_taxa_names_v
                WHERE zoom=? AND x=? AND y=? AND slot_id IN ({placeholders})
                GROUP BY taxon_id
                ORDER BY observations_count DESC, taxon_id
                LIMIT ?;
                """,
                (zoom, x, y, *slot_ids, limit),
            ).fetchall()

            logger.info(
                "cell_taxa_window zoom=%d x=%d y=%d slots=%s rows=%d",
                zoom, x, y, ",".join(map(str, slot_ids)), len(rows),
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
            candidate_rows = conn.execute(
                """
                SELECT zoom, slot_id, x, y, coverage, score,
                       centroid_lat, centroid_lon,
                       topLeft_lat, topLeft_lon, bottomRight_lat, bottomRight_lon,
                       obs_total, taxa_list
                FROM grid_hotmap_v
                WHERE zoom=? AND slot_id=?
                ORDER BY coverage DESC, score DESC
                LIMIT 4000;
                """,
                (zoom, slot_id),
            ).fetchall()

            scored = []
            seen: set[tuple[int, int, int, int]] = set()

            for row in candidate_rows:
                key = (int(row["zoom"]), int(row["slot_id"]), int(row["x"]), int(row["y"]))
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

