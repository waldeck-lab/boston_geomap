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
from pathlib import Path                                                                                                 
from typing import Any, Callable, Optional                                                                               
from dataclasses import dataclass, field                                                                                 
from copy import deepcopy                                                                                                
from datetime import datetime, timezone                                                                                  
import time                                                                                                              
import uuid                                                                                                              
import traceback

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
import logging
logger = logging.getLogger("geomap-server")

ZOOM_DEFAULT = 15  # server default if client doesn't send zooms

from werkzeug.exceptions import BadRequest, HTTPException

class CancelledJobError(RuntimeError):
    pass


def _utc_now_ts() -> float:
    return time.time()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_or_none(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class JobState:
    job_id: str
    kind: str
    status: str = "queued"  # queued|running|done|failed|cancelled
    phase: str = "planning"
    current_step: str = ""
    total_steps: int = 0
    completed_steps: int = 0
    created_at: float = field(default_factory=_utc_now_ts)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    updated_at: float = field(default_factory=_utc_now_ts)
    error: Optional[str] = None
    traceback_text: Optional[str] = None
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    spec: dict[str, Any] = field(default_factory=dict)
    cancel_requested: bool = False

class JobManager:
    def __init__(self) -> None:
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._jobs: dict[str, JobState] = {}
        self._current_job_id: Optional[str] = None
        self._last_finished_job_id: Optional[str] = None

    def busy(self) -> bool:
        with self._state_lock:
            return self._current_job_id is not None

    def current_job_id(self) -> Optional[str]:
        with self._state_lock:
            return self._current_job_id

    def _snapshot(self, job: JobState) -> dict[str, Any]:
        eta_seconds: Optional[int] = None
        progress_pct = 0.0

        if job.total_steps > 0:
            progress_pct = round((job.completed_steps / job.total_steps) * 100.0, 2)

        if job.started_at and job.completed_steps > 0 and job.status == "running":
            elapsed = max(_utc_now_ts() - job.started_at, 0.001)
            sec_per_step = elapsed / max(job.completed_steps, 1)
            remaining = max(job.total_steps - job.completed_steps, 0)
            eta_seconds = int(sec_per_step * remaining)

        return {
            "job_id": job.job_id,
            "kind": job.kind,
            "status": job.status,
            "phase": job.phase,
            "current_step": job.current_step,
            "total_steps": job.total_steps,
            "completed_steps": job.completed_steps,
            "progress_pct": progress_pct,
            "eta_seconds": eta_seconds,
            "created_at": _iso_or_none(job.created_at),
            "started_at": _iso_or_none(job.started_at),
            "finished_at": _iso_or_none(job.finished_at),
            "updated_at": _iso_or_none(job.updated_at),
            "error": job.error,
            "warnings": list(job.warnings),
            "summary": deepcopy(job.summary),
            "spec": deepcopy(job.spec),
            "cancel_requested": bool(job.cancel_requested),
            "traceback_text": job.traceback_text,
        }

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        with self._state_lock:
            job = self._jobs.get(job_id)
            return None if job is None else self._snapshot(job)

    def get_status(self) -> dict[str, Any]:
        with self._state_lock:
            current = self._jobs.get(self._current_job_id) if self._current_job_id else None
            last_finished = self._jobs.get(self._last_finished_job_id) if self._last_finished_job_id else None
            return {
                "ok": True,
                "busy": current is not None,
                "current_job": None if current is None else self._snapshot(current),
                "last_job": None if last_finished is None else self._snapshot(last_finished),
            }

    def start_job(self, *, kind: str, spec: dict[str, Any], target: Callable[[str], dict[str, Any]]) -> JobState:
        if not self._write_lock.acquire(blocking=False):
            raise RuntimeError("busy")

        job = JobState(
            job_id=f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}",
            kind=kind,
            spec=deepcopy(spec),
        )

        with self._state_lock:
            self._jobs[job.job_id] = job
            self._current_job_id = job.job_id

        logger.info(
            "job %s queued kind=%s spec=%s",
            job.job_id,
            job.kind,
            _job_log_spec_summary(job.spec),
        )

        def runner() -> None:
            self.mark_running(job.job_id)
            try:
                summary = target(job.job_id)
                self.mark_done(job.job_id, summary=summary)
            except CancelledJobError:
                self.mark_cancelled(job.job_id)
            except Exception as exc:
                self.mark_failed(job.job_id, exc)
            finally:
                with self._state_lock:
                    if self._current_job_id == job.job_id:
                        self._current_job_id = None
                    self._last_finished_job_id = job.job_id
                self._write_lock.release()

        t = threading.Thread(target=runner, name=f"geomap-job-{job.job_id}", daemon=True)
        t.start()
        return job

    def mark_running(self, job_id: str) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            now = _utc_now_ts()
            job.status = "running"
            job.started_at = now
            job.updated_at = now

        logger.info(
            "job %s started kind=%s spec=%s",
            job_id,
            job.kind,
            _job_log_spec_summary(job.spec),
        )

    def set_total_steps(self, job_id: str, total_steps: int) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            job.total_steps = max(int(total_steps), 0)
            job.updated_at = _utc_now_ts()

    def set_phase(self, job_id: str, phase: str, current_step: str = "") -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            phase_changed = (job.phase != phase)
            step_changed = bool(current_step and current_step != job.current_step)

            job.phase = phase
            if current_step:
                job.current_step = current_step
            job.updated_at = _utc_now_ts()

            completed_steps = job.completed_steps
            total_steps = job.total_steps
            step_value = job.current_step

        if phase_changed:
            logger.info(
                "job %s phase=%s completed=%d/%d",
                job_id,
                phase,
                completed_steps,
                total_steps,
            )

        if step_changed:
            logger.info("job %s step=%s", job_id, step_value)

    def advance(self, job_id: str, *, phase: str, current_step: str, inc: int = 1) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            phase_changed = (job.phase != phase)
            step_changed = bool(current_step and current_step != job.current_step)

            job.phase = phase
            job.current_step = current_step
            job.completed_steps = min(job.total_steps, job.completed_steps + max(int(inc), 0))
            job.updated_at = _utc_now_ts()

            completed_steps = job.completed_steps
            total_steps = job.total_steps

        if phase_changed:
            logger.info(
                "job %s phase=%s completed=%d/%d",
                job_id,
                phase,
                completed_steps,
                total_steps,
            )

        if step_changed:
            logger.info("job %s step=%s", job_id, current_step)

    def append_warning(self, job_id: str, warning: str) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            job.warnings.append(warning)
            job.updated_at = _utc_now_ts()

        logger.warning("job %s warning=%s", job_id, warning)

    def mark_done(self, job_id: str, *, summary: dict[str, Any]) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            now = _utc_now_ts()
            job.status = "done"
            job.phase = "done"
            job.current_step = "completed"
            job.summary = deepcopy(summary)
            job.finished_at = now
            job.updated_at = now
            if job.total_steps > 0:
                job.completed_steps = job.total_steps

            completed_steps = job.completed_steps
            total_steps = job.total_steps

        logger.info(
            "job %s finished status=done completed=%d/%d summary=%s",
            job_id,
            completed_steps,
            total_steps,
            summary,
        )

    def mark_failed(self, job_id: str, exc: Exception) -> None:
        tb = traceback.format_exc()
        with self._state_lock:
            job = self._jobs[job_id]
            now = _utc_now_ts()
            job.status = "failed"
            job.error = str(exc)
            job.traceback_text = tb
            job.finished_at = now
            job.updated_at = now

            phase = job.phase
            current_step = job.current_step
            completed_steps = job.completed_steps
            total_steps = job.total_steps
            spec = deepcopy(job.spec)

        logger.exception(
            "job %s failed kind=%s phase=%s step=%s completed=%d/%d spec=%s",
            job_id,
            job.kind,
            phase,
            current_step,
            completed_steps,
            total_steps,
            _job_log_spec_summary(spec),
        )

    def mark_cancelled(self, job_id: str) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            now = _utc_now_ts()
            job.status = "cancelled"
            job.finished_at = now
            job.updated_at = now

            phase = job.phase
            current_step = job.current_step
            completed_steps = job.completed_steps
            total_steps = job.total_steps

        logger.warning(
            "job %s cancelled phase=%s step=%s completed=%d/%d",
            job_id,
            phase,
            current_step,
            completed_steps,
            total_steps,
        )

    def cancel(self, job_id: str) -> bool:
        with self._state_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.status not in {"queued", "running"}:
                return False
            job.cancel_requested = True
            job.updated_at = _utc_now_ts()

        logger.warning("job %s cancel requested", job_id)
        return True

    def ensure_not_cancelled(self, job_id: str) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            if job.cancel_requested:
                raise CancelledJobError(f"Job {job_id} cancelled")

JOB_MANAGER = JobManager()


def _job_log_spec_summary(spec: dict) -> dict:
    return {
        "year_from": spec.get("year_from"),
        "year_to": spec.get("year_to"),
        "fetch_slots": spec.get("fetch_slots") or spec.get("slot_ids"),
        "final_slots": spec.get("final_slots"),
        "zooms": spec.get("zooms"),
        "force": spec.get("force"),
        "include_slot0": spec.get("include_slot0"),
        "include_all_years": spec.get("include_all_years"),
        "n": spec.get("n"),
        "alpha": spec.get("alpha"),
        "beta": spec.get("beta"),
    }


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



def parse_year_range_body(body: dict[str, Any], *, default_from: Optional[int] = None, default_to: Optional[int] = None) -> tuple[int, int]:
    yf = body.get("year_from", default_from)
    yt = body.get("year_to", default_to)

    if yf is None and yt is None:
        return (YEAR_ALL, YEAR_ALL)
    if yf is None:
        yf = yt
    if yt is None:
        yt = yf

    yf_i = parse_year(yf, name="year_from")
    yt_i = parse_year(yt, name="year_to")

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



def parse_taxon_ids_arg(value: Any, *, name: str = "taxon_ids") -> list[int]:
    """
    Accepts:
      - "123,456" (string)
      - [123,456] (list)
      - single int
    Returns sorted unique positive taxon ids.
    """
    if value is None:
        return []

    if isinstance(value, str):
        vals = [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, (list, tuple)):
        vals = list(value)
    else:
        vals = [value]

    out: list[int] = []
    for v in vals:
        try:
            tid = int(v)
        except Exception:
            raise BadRequest(description=f"{name} must contain integers")
        if tid <= 0:
            raise BadRequest(description=f"{name} must contain positive integers")
        out.append(tid)
    return sorted(set(out))


    
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

    from geomap.timeslots import slot_bounds

    def _iso_local_day_bounds(year: int, month: int, start_day: int, end_day: int) -> tuple[str, str]:
        start = datetime(year, month, start_day, 0, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        end = datetime(year, month, end_day, 23, 59, 59, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return start, end

    def _extra_filter_for_slot_year(slot: int, year: int) -> dict[str, Any]:
        m, q = ((slot - 1) // 4 + 1), ((slot - 1) % 4 + 1)
        ts = slot_bounds(m, q, year_for_days=year)
        start_iso, end_iso = _iso_local_day_bounds(year, m, ts.start_day, ts.end_day)
        return {
            "date": {
                "startDate": start_iso,
                "endDate": end_iso,
                "dateFilterType": "BetweenStartDateAndEndDate",
            }
        }

    def _merge_payloads_gridcells(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
                    continue

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

                a["boundingBox"] = {
                    "topLeft": {"latitude": max(atl_lat, top_lat), "longitude": min(atl_lon, left_lon)},
                    "bottomRight": {"latitude": min(abr_lat, bot_lat), "longitude": max(abr_lon, right_lon)},
                }

        out = list(acc.values())
        out.sort(key=lambda c: (int(c["x"]), int(c["y"])))
        return out


    def _store_payload(
            conn: sqlite3.Connection,
            *,
            taxon_id: int,
            zooms: list[int],
            year: int,
            slot_id: int,
            payload: dict[str, Any],
            force: bool,
    ) -> None:
        base_zoom = zooms[0]
        grid_cells = payload.get("gridCells") or []
        sha = stable_gridcells_hash(payload)
        
        prev = storage.get_layer_state(conn, taxon_id, base_zoom, slot_id, year=year)
        unchanged = (prev is not None and prev[1] == sha)
        
        if (not unchanged) or force:
            with conn:
                storage.replace_taxon_grid(conn, taxon_id, base_zoom, slot_id, grid_cells, year=year)
                storage.upsert_layer_state(
                    conn,
                    taxon_id,
                    base_zoom,
                    slot_id,
                    sha,
                    len(grid_cells),
                    year=year,
                )

        src_zoom = base_zoom
        src_sha = sha
        for dst_zoom in zooms[1:]:
            with conn:
                storage.materialize_parent_zoom_from_child(
                    conn,
                    taxon_id=taxon_id,
                    slot_id=slot_id,
                    year=year,
                    src_zoom=src_zoom,
                    dst_zoom=dst_zoom,
                    src_sha=src_sha,
                )
            src_zoom = dst_zoom

    def _normalize_rebuild_spec(body: dict[str, Any], *, default_n: int, default_all_slots: bool) -> dict[str, Any]:
        this_year = datetime.now(timezone.utc).year
        year_from, year_to = parse_year_range_body(body, default_from=2000, default_to=this_year)
        refresh_mode = str(body.get("refresh_mode", "upstream")).strip().lower()
        if refresh_mode not in {"upstream", "local"}:
            raise BadRequest(description="refresh_mode must be 'upstream' or 'local'")

        if year_from == YEAR_ALL or year_to == YEAR_ALL:
            raise BadRequest(description="jobs/rebuild requires concrete year_from/year_to, not 0")

        slots_raw = body.get("slot_ids", body.get("slots", None))
        if slots_raw is None:
            slot_single = body.get("slot_id", None)
            if slot_single is None:
                fetch_slots = list(range(1, SLOT_MAX + 1)) if default_all_slots else [1]
            else:
                slot_single = parse_slot_id(slot_single, name="slot_id")
                fetch_slots = list(range(1, SLOT_MAX + 1)) if slot_single == SLOT_ALL else [slot_single]
        else:
            parsed_slots = parse_slot_ids_arg(slots_raw, name="slot_ids")
            fetch_slots = list(range(1, SLOT_MAX + 1)) if parsed_slots == [SLOT_ALL] else parsed_slots

        if SLOT_ALL in fetch_slots:
            raise BadRequest(description="slot_id 0 is derived; pass slots 1..48 and set include_slot0=true")

        include_slot0 = bool(body.get("include_slot0", True))
        include_all_years = bool(body.get("include_all_years", True))
        zooms = parse_zooms(body.get("zooms", [ZOOM_DEFAULT]))
        taxon_ids = parse_taxon_ids_arg(body.get("taxon_ids", None), name="taxon_ids")

        spec = {
            "refresh_mode": refresh_mode,
            "taxon_ids": taxon_ids,
            "fetch_slots": sorted(set(fetch_slots)),
            "final_slots": sorted(set(fetch_slots + ([SLOT_ALL] if include_slot0 else []))),
            "zooms": zooms,
            "base_zoom": zooms[0],
            "n": int(body.get("n", default_n)),
            "alpha": float(body.get("alpha", cfg.hotmap_alpha)),
            "beta": float(body.get("beta", cfg.hotmap_beta)),
            "force": bool(body.get("force", False)),
            "year_from": int(year_from),
            "year_to": int(year_to),
            "include_slot0": include_slot0,
            "include_all_years": include_all_years,
        }
        return spec

    def _run_rebuild_job(job_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        refresh_mode = str(spec.get("refresh_mode", "upstream")).strip().lower()
        if refresh_mode not in {"upstream", "local"}:
            raise RuntimeError(f"Unsupported refresh_mode={refresh_mode}")

        if refresh_mode == "upstream":
            if not cfg.subscription_key:
                raise RuntimeError("Missing ARTDATABANKEN_SUBSCRIPTION_KEY")
            if not cfg.authorization:
                raise RuntimeError("Missing ARTDATABANKEN_AUTHORIZATION")

        years = list(range(spec["year_from"], spec["year_to"] + 1))
        fetch_slots = list(spec["fetch_slots"])
        final_slots = list(spec["final_slots"])
        zooms = list(spec["zooms"])
        explicit_taxon_ids = [int(t) for t in (spec.get("taxon_ids") or [])]

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            if explicit_taxon_ids:
                taxon_ids = explicit_taxon_ids
                taxa_rows = []
                for tid in taxon_ids:
                    dim = conn.execute(
                        "SELECT scientific_name, swedish_name FROM taxon_dim WHERE taxon_id=?;",
                        (tid,),
                    ).fetchone()
                    taxa_rows.append(
                        {
                            "taxon_id": tid,
                            "scientific_name": (dim[0] if dim else "") or "",
                            "swedish_name": (dim[1] if dim else "") or "",
                        }
                    )
            else:
                taxa_rows = read_taxa_rows(cfg.missing_species_csv, int(spec["n"]))
                taxon_ids = [int(t["taxon_id"]) for t in taxa_rows]
                if not taxon_ids:
                    raise RuntimeError("No taxon ids found in CSV")

            if refresh_mode == "local":
                # Restrict to taxa that actually have local taxon_grid data for the requested range/slots/zooms.
                placeholders = ",".join(["?"] * len(taxon_ids))
                slot_placeholders = ",".join(["?"] * len(final_slots))
                zoom_placeholders = ",".join(["?"] * len(zooms))
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT taxon_id
                    FROM taxon_grid
                    WHERE taxon_id IN ({placeholders})
                      AND zoom IN ({zoom_placeholders})
                      AND slot_id IN ({slot_placeholders})
                      AND year BETWEEN ? AND ?
                    ORDER BY taxon_id;
                    """,
                    (*taxon_ids, *zooms, *final_slots, spec["year_from"], spec["year_to"]),
                ).fetchall()
                taxon_ids = [int(r[0]) for r in rows]
                taxa_rows = [t for t in taxa_rows if int(t["taxon_id"]) in set(taxon_ids)]
                if not taxon_ids:
                    raise RuntimeError("No local taxon_grid rows found for requested taxon_ids/year_range/slots/zooms")

            fetch_steps = 0 if refresh_mode == "local" else len(taxon_ids) * len(years) * len(fetch_slots)
            derive_slot0_steps = len(taxon_ids) * len(years) if spec["include_slot0"] else 0
            derive_all_years_steps = len(taxon_ids) * len(final_slots) if spec["include_all_years"] else 0
            rebuild_years = list(years) + ([YEAR_ALL] if spec["include_all_years"] else [])
            rebuild_steps = len(zooms) * len(rebuild_years) * len(final_slots)
            total_steps = fetch_steps + derive_slot0_steps + derive_all_years_steps + rebuild_steps

            JOB_MANAGER.set_total_steps(job_id, total_steps)
            JOB_MANAGER.set_phase(job_id, "planning", current_step=f"taxa={len(taxon_ids)} years={len(years)} slots={len(fetch_slots)}")

            throttle_state: dict[str, float] = {}

            with conn:
                storage.upsert_taxon_dim(
                    conn,
                    [(t["taxon_id"], t["scientific_name"], t["swedish_name"]) for t in taxa_rows],
                )

            logger.info(
                "job=%s rebuild start refresh_mode=%s years=%d..%d fetch_slots=%s final_slots=%s zooms=%s taxa=%d force=%s explicit_taxon_ids=%s",
                job_id,
                refresh_mode,
                spec["year_from"],
                spec["year_to"],
                ",".join(map(str, fetch_slots)),
                ",".join(map(str, final_slots)),
                ",".join(map(str, zooms)),
                len(taxon_ids),
                str(spec["force"]),
                ",".join(map(str, explicit_taxon_ids)) if explicit_taxon_ids else "",
            )

            if refresh_mode == "upstream":
                for taxon_id in taxon_ids:
                    JOB_MANAGER.ensure_not_cancelled(job_id)
                    per_slot_payloads: dict[int, list[dict[str, Any]]] = {s: [] for s in final_slots}

                    for yr in years:
                        JOB_MANAGER.ensure_not_cancelled(job_id)
                        yearly_slot_payloads: list[dict[str, Any]] = []

                        for slot_id in fetch_slots:
                            JOB_MANAGER.ensure_not_cancelled(job_id)
                            throttle(2.0, throttle_state)
                            extra = _extra_filter_for_slot_year(slot_id, yr)
                            payload = client.geogrid_aggregation_resilient([taxon_id], zoom=spec["base_zoom"], extra_filter=extra)
                            _store_payload(
                                conn,
                                taxon_id=taxon_id,
                                zooms=zooms,
                                year=yr,
                                slot_id=slot_id,
                                payload=payload,
                                force=bool(spec["force"]),
                            )
                            per_slot_payloads[slot_id].append(payload)
                            yearly_slot_payloads.append(payload)
                            JOB_MANAGER.advance(
                                job_id,
                                phase="fetch_slots",
                                current_step=f"taxon={taxon_id} year={yr} slot={slot_id}",
                            )

                        if spec["include_slot0"]:
                            merged_year_payload = {"gridCells": _merge_payloads_gridcells(yearly_slot_payloads)}
                            _store_payload(
                                conn,
                                taxon_id=taxon_id,
                                zooms=zooms,
                                year=yr,
                                slot_id=SLOT_ALL,
                                payload=merged_year_payload,
                                force=bool(spec["force"]),
                            )
                            per_slot_payloads[SLOT_ALL].append(merged_year_payload)
                            JOB_MANAGER.advance(
                                job_id,
                                phase="derive_slot0_per_year",
                                current_step=f"taxon={taxon_id} year={yr} slot=0",
                            )

                    if spec["include_all_years"]:
                        for slot_id in final_slots:
                            JOB_MANAGER.ensure_not_cancelled(job_id)
                            merged_all_payload = {"gridCells": _merge_payloads_gridcells(per_slot_payloads[slot_id])}
                            _store_payload(
                                conn,
                                taxon_id=taxon_id,
                                zooms=zooms,
                                year=YEAR_ALL,
                                slot_id=slot_id,
                                payload=merged_all_payload,
                                force=bool(spec["force"]),
                            )
                            JOB_MANAGER.advance(
                                job_id,
                                phase="derive_all_years",
                                current_step=f"taxon={taxon_id} year=0 slot={slot_id}",
                            )

            for slot_id in final_slots:
                for yr in rebuild_years:
                    for z in zooms:
                        JOB_MANAGER.ensure_not_cancelled(job_id)
                        with conn:
                            storage.rebuild_hotmap(conn, z, slot_id, taxon_ids, alpha=spec["alpha"], beta=spec["beta"], year=yr)
                        JOB_MANAGER.advance(
                            job_id,
                            phase="rebuild_hotmaps",
                            current_step=f"zoom={z} year={yr} slot={slot_id}",
                        )
            JOB_MANAGER.set_phase(job_id, "finalizing", current_step="writing summary")
            summary = {
                "ok": True,
                "finished_at": _utc_now_iso(),
                "refresh_mode": refresh_mode,
                "taxon_ids": taxon_ids,
                "n_taxa": len(taxon_ids),
                "year_from": spec["year_from"],
                "year_to": spec["year_to"],
                "fetch_slots": fetch_slots,
                "final_slots": final_slots,
                "zooms": zooms,
                "alpha": spec["alpha"],
                "beta": spec["beta"],
                "force": bool(spec["force"]),
            }
            logger.info("job=%s rebuild done summary=%s", job_id, summary)
            return summary
        finally:
            conn.close()

    @app.get("/api/jobs/status")
    def jobs_status():
        return jsonify(JOB_MANAGER.get_status())

    @app.get("/api/jobs/<job_id>")
    def jobs_get(job_id: str):
        snap = JOB_MANAGER.get_job(job_id)
        if snap is None:
            return jsonify({"ok": False, "code": "not_found", "error": f"Unknown job_id: {job_id}"}), 404
        return jsonify({"ok": True, "busy": JOB_MANAGER.busy(), "job": snap})

    @app.post("/api/jobs/<job_id>/cancel")
    def jobs_cancel(job_id: str):
        ok = JOB_MANAGER.cancel(job_id)
        if not ok:
            snap = JOB_MANAGER.get_job(job_id)
            if snap is None:
                return jsonify({"ok": False, "code": "not_found", "error": f"Unknown job_id: {job_id}"}), 404
            return jsonify({"ok": False, "code": "invalid_state", "error": f"Cannot cancel job in state={snap['status']}"}), 409
        return jsonify({"ok": True, "job_id": job_id, "status": "cancelling"}), 202

    @app.post("/api/jobs/rebuild")
    def jobs_rebuild():
        body = request.get_json(force=True) or {}
        spec = _normalize_rebuild_spec(body, default_n=0, default_all_slots=True)
        try:
            job = JOB_MANAGER.start_job(
                kind="rebuild",
                spec=spec,
                target=lambda job_id: _run_rebuild_job(job_id, spec),
            )
        except RuntimeError:
            current = JOB_MANAGER.get_status().get("current_job")
            return jsonify({
                "ok": False,
                "code": "busy",
                "error": "A write job is already running",
                "current_job": current,
            }), 409

        return jsonify({
            "ok": True,
            "job_id": job.job_id,
            "status": "queued",
            "status_url": f"/api/jobs/{job.job_id}",
            "busy": True,
            "spec": spec,
        }), 202

    @app.post("/api/pipeline/build")
    def pipeline_build():
        body = request.get_json(force=True) or {}
        spec = _normalize_rebuild_spec(body, default_n=5, default_all_slots=True)
        try:
            job = JOB_MANAGER.start_job(
                kind="rebuild",
                spec=spec,
                target=lambda job_id: _run_rebuild_job(job_id, spec),
            )
        except RuntimeError:
            current = JOB_MANAGER.get_status().get("current_job")
            return jsonify({
                "ok": False,
                "code": "busy",
                "error": "A write job is already running",
                "current_job": current,
            }), 409

        return jsonify({
            "ok": True,
            "job_id": job.job_id,
            "status": "queued",
            "status_url": f"/api/jobs/{job.job_id}",
            "busy": True,
            "spec": spec,
        }), 202
    
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

    from geomap.cli_paths import apply_path_overrides
    from server.logging_utils import setup_server_logger

    ap = argparse.ArgumentParser(description="Geomap dev API server (OVE-friendly).")
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8088")))
    ap.add_argument("--db-dir", default=None, help="Override DB directory (expects geomap.sqlite).")
    ap.add_argument("--lists-dir", default=None, help="Override lists directory (expects missing_species.csv).")
    ap.add_argument("--logs-dir", default=None, help="Override logs directory (default: stage/logs/server in OVE).")
    args = ap.parse_args()

    # Resolve default logs dir through the shared helper
    if args.logs_dir is None:
        args.logs_dir = str(_infer_default_server_logs_dir())

    # Map CLI dirs into GEOMAP_* env vars so Config sees them
    apply_path_overrides(
        db_dir=args.db_dir,
        lists_dir=args.lists_dir,
        logs_dir=args.logs_dir,
    )

    log_dir = Path(args.logs_dir).expanduser().resolve() if args.logs_dir else None
    logger = setup_server_logger(name="geomap-server", log_dir=log_dir)

    logger.info("GEOMAP_DB=%s", os.getenv("GEOMAP_DB"))
    logger.info("GEOMAP_OBSERVED_DB=%s", os.getenv("GEOMAP_OBSERVED_DB"))
    logger.info("GEOMAP_DYNTAXA_DB=%s", os.getenv("GEOMAP_DYNTAXA_DB"))
    logger.info("GEOMAP_MISSING_SPECIES_CSV=%s", os.getenv("GEOMAP_MISSING_SPECIES_CSV"))
    logger.info("GEOMAP_LOGS_DIR=%s", os.getenv("GEOMAP_LOGS_DIR"))

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

    
