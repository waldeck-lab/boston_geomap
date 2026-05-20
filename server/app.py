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
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from copy import deepcopy
from datetime import datetime, timezone
import time
import json
import uuid
import traceback
import hashlib

from auth_client import require_grant

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

from geomap.csv_import import (
    IngestArgs as CsvIngestArgs,
    find_csv_inside_zip,
    import_observations_raw,
    consolidate_taxon_grid_from_raw_bulk_tile_bbox,
    consolidate_taxon_grid_year_all_from_grid,
)

from geomap.taxon_lists import (
    read_taxon_ids_from_csv,
    chunked,
)

from geomap.sos_filters_ext import make_sos_export_filter
from geomap.tiles import tile_xy_to_bbox
from geomap.sos_export import export_csv_zip_to_file

import threading                                                                                                         
from logging_utils import iso_or_none, local_now_ts, local_now_iso
import logging
logger = logging.getLogger("geomap-server")

ZOOM_DEFAULT = 15  # server default if client doesn't send zooms

from werkzeug.exceptions import BadRequest, HTTPException

class CancelledJobError(RuntimeError):
    pass

# Global bool-parser helper
def parse_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    return default


@dataclass
class JobState:
    job_id: str
    kind: str
    status: str = "queued"  # queued|running|done|failed|cancelled
    phase: str = "planning"
    current_step: str = ""
    total_steps: int = 0
    completed_steps: int = 0
    created_at: float = field(default_factory=local_now_ts)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    updated_at: float = field(default_factory=local_now_ts)
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
            elapsed = max(local_now_ts() - job.started_at, 0.001)
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
            "created_at": iso_or_none(job.created_at),
            "started_at": iso_or_none(job.started_at),
            "finished_at": iso_or_none(job.finished_at),
            "updated_at": iso_or_none(job.updated_at),
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
            now = local_now_ts()
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
            job.updated_at = local_now_ts()

    def add_total_steps(self, job_id: str, extra_steps: int) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            job.total_steps = max(job.total_steps + max(int(extra_steps), 0), job.completed_steps)
            job.updated_at = local_now_ts()
            
    def set_phase(self, job_id: str, phase: str, current_step: str = "") -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            phase_changed = (job.phase != phase)
            step_changed = bool(current_step and current_step != job.current_step)

            job.phase = phase
            if current_step:
                job.current_step = current_step
            job.updated_at = local_now_ts()

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
            job.updated_at = local_now_ts()

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
            job.updated_at = local_now_ts()

        logger.warning("job %s warning=%s", job_id, warning)

    def mark_done(self, job_id: str, *, summary: dict[str, Any]) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            now = local_now_ts()
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
            now = local_now_ts()
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
            now = local_now_ts()
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
            job.updated_at = local_now_ts()

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
        "csv_name": spec.get("csv_name"),
        "csv_path": spec.get("csv_path"),
        "full_reconsolidate": spec.get("full_reconsolidate"),
        "taxon_list_csv": spec.get("taxon_list_csv"),
        "initial_batch_size": spec.get("initial_batch_size"),
        "min_batch_size": spec.get("min_batch_size"),
        "adaptive_split": spec.get("adaptive_split"),
    }

def _timed_log(label: str, fn):
    t0 = time.monotonic()
    logger.info("TIMER start %s", label)
    try:
        return fn()
    finally:
        logger.info("TIMER done %s dt=%.3fs", label, time.monotonic() - t0)

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

def _upsert_taxon_dim_from_sources(
        conn,
        taxon_ids: list[int],
        cfg,
):
    if not taxon_ids:
        return 0

    csv_rows = storage.read_taxa_rows(
        cfg.missing_species_csv,
        0,
    )

    csv_map = {
        int(r["taxon_id"]): r
        for r in csv_rows
    }

    taxa = []

    for tid in taxon_ids:

        r = csv_map.get(tid)

        if r:
            taxa.append(
                (
                    int(tid),
                    (r.get("scientific_name") or "").strip(),
                    (r.get("swedish_name") or "").strip(),
                )
            )
        else:
            taxa.append(
                (
                    int(tid),
                    "",
                    "",
                )
            )

    storage.upsert_taxon_dim(conn, taxa)

    return len(taxa)


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

def _resolve_csv_input_path(cfg: Config, csv_name: str | None, csv_path: str | None) -> Path:
    if csv_path:
        p = Path(csv_path).expanduser().resolve()
        if not p.exists():
            raise RuntimeError(f"csv_path does not exist: {p}")
        return p
    if csv_name:
        p = (cfg.csv_stash_dir / csv_name).expanduser().resolve()
        if not p.exists():
            raise RuntimeError(f"csv_name not found in stash: {p}")
        return p
    raise RuntimeError("Either csv_name or csv_path must be provided for refresh_mode='csv_import'")

def _write_csv_job_metadata(cfg: Config, job_id: str, payload: dict[str, Any]) -> Path:
    cfg.csv_meta_dir.mkdir(parents=True, exist_ok=True)
    meta_path = cfg.csv_meta_dir / f"{job_id}.json"
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta_path

def _copy_to_stash(job_id: str, src: Path, dst_dir: Path) -> Path:
    import shutil

    dst_dir.mkdir(parents=True, exist_ok=True)
    safe_name = src.name
    dst = dst_dir / f"{job_id}__{safe_name}"
    shutil.copy2(src, dst)
    return dst


def make_app() -> Flask:
    app = Flask(__name__)
    CORS(app)  # keep it simple for local dev

    cfg = Config(repo_root=REPO_ROOT)
    logger.info("OVE_BASE_DIR=%s", os.getenv("OVE_BASE_DIR", ""))
    logger.info("OVE_STAGE_DIR=%s", os.getenv("OVE_STAGE_DIR", ""))
    logger.info("Resolved logs_dir=%s", cfg.logs_dir)
    logger.info("Resolved csv_stash_dir=%s (%s)", cfg.csv_stash_dir, _path_status(cfg.csv_stash_dir))
    logger.info("Resolved csv_meta_dir=%s (%s)", cfg.csv_meta_dir, _path_status(cfg.csv_meta_dir))
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


    @app.get("/geomap-api/health")
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

    def _normalize_sos_import_spec(body: dict[str, Any]) -> dict[str, Any]:
        this_year = datetime.now(timezone.utc).year

        taxon_list_csv = (body.get("taxon_list_csv") or "").strip()
        if not taxon_list_csv:
            taxon_list_csv = str(cfg.missing_species_csv)
            
        year_from, year_to = parse_year_range_body(
            body,
            default_from=2000,
            default_to=this_year,
        )
        if year_from == YEAR_ALL or year_to == YEAR_ALL:
            raise BadRequest(description="sos_import requires concrete year_from/year_to, not 0")

        slots_raw = body.get("slot_ids", body.get("slots", list(range(1, SLOT_MAX + 1))))
        parsed_slots = parse_slot_ids_arg(slots_raw, name="slot_ids")
        fetch_slots = list(range(1, SLOT_MAX + 1)) if parsed_slots == [SLOT_ALL] else parsed_slots

        if SLOT_ALL in fetch_slots:
            raise BadRequest(description="slot_id 0 is derived; pass slots 1..48 and set include_slot0=true")

        csv_date_field = str(body.get("csv_date_field", "StartDate")).strip()
        if csv_date_field not in {"StartDate", "EndDate"}:
            raise BadRequest(description="csv_date_field must be 'StartDate' or 'EndDate'")

        taxon_ids = parse_taxon_ids_arg(body.get("taxon_ids", None), name="taxon_ids")
        
        return {
            "taxon_list_csv": taxon_list_csv,
            "initial_batch_size": int(body.get("batch_size", 20)),
            "min_batch_size": int(body.get("min_batch_size", 1)),
            "adaptive_split": parse_bool(body.get("adaptive_split", True)),
            "year_from": int(year_from),
            "year_to": int(year_to),
            "fetch_slots": sorted(set(fetch_slots)),
            "final_slots": sorted(set(fetch_slots + ([SLOT_ALL] if parse_bool(body.get("include_slot0", True)) else []))),
            "zooms": parse_zooms(body.get("zooms", [ZOOM_DEFAULT])),
            "include_slot0": parse_bool(body.get("include_slot0", True)),
            "include_all_years": parse_bool(body.get("include_all_years", True)),
            "csv_date_field": csv_date_field,
            "csv_occurrence_status": (body.get("occurrence_status") or
                                      body.get("csv_occurrence_status") or
                                      "present").strip() or None,
            "alpha": float(body.get("alpha", cfg.hotmap_alpha)),
            "beta": float(body.get("beta", cfg.hotmap_beta)),
            "output_field_set": str(body.get("output_field_set", "All")),
            "force": parse_bool(body.get("force"), False),
            "taxon_ids": taxon_ids,
        }

            
    def _normalize_rebuild_spec(body: dict[str, Any], *, default_n: int, default_all_slots: bool) -> dict[str, Any]:
        this_year = datetime.now(timezone.utc).year
        year_from, year_to = parse_year_range_body(body, default_from=2000, default_to=this_year)

        refresh_mode = str(body.get("refresh_mode", "upstream")).strip().lower()
        if refresh_mode not in {"upstream", "local", "csv_import", "raw_rebuild"}:
            raise BadRequest(description="refresh_mode must be 'upstream', 'local', 'csv_import' or 'raw_rebuild'")

        full_reconsolidate = parse_bool(body.get("full_reconsolidate", False))

        csv_name = (body.get("csv_name") or "").strip() or None
        csv_path = (body.get("csv_path") or "").strip() or None
        stash_copy = parse_bool(body.get("stash_copy", True))
        csv_date_field = str(body.get("csv_date_field", "StartDate")).strip()
        if csv_date_field not in {"StartDate", "EndDate"}:
            raise BadRequest(description="csv_date_field must be 'StartDate' or 'EndDate'")
        csv_occurrence_status = (body.get("csv_occurrence_status") or "present").strip() or None

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

        include_slot0 = parse_bool(body.get("include_slot0", True))
        if refresh_mode == "csv_import" and not include_slot0:
            # allow false, but the current raw pipeline commonly derives slot0 and the UI relies on it
            pass

        include_all_years = parse_bool(body.get("include_all_years", True))
        zooms = parse_zooms(body.get("zooms", [ZOOM_DEFAULT]))
        taxon_ids = parse_taxon_ids_arg(body.get("taxon_ids", None), name="taxon_ids")

        spec = {
            "refresh_mode": refresh_mode,
            "taxon_ids": taxon_ids,
            "csv_name": csv_name,
            "csv_path": csv_path,
            "stash_copy": stash_copy,
            "csv_date_field": csv_date_field,
            "csv_occurrence_status": csv_occurrence_status,
            "fetch_slots": sorted(set(fetch_slots)),
            "final_slots": sorted(set(fetch_slots + ([SLOT_ALL] if include_slot0 else []))),
            "zooms": zooms,
            "base_zoom": zooms[0],
            "n": int(body.get("n", default_n)),
            "alpha": float(body.get("alpha", cfg.hotmap_alpha)),
            "beta": float(body.get("beta", cfg.hotmap_beta)),
            "force": parse_bool(body.get("force", False)),
            "year_from": int(year_from),
            "year_to": int(year_to),
            "include_slot0": include_slot0,
            "include_all_years": include_all_years,
            "full_reconsolidate": full_reconsolidate,
        }
        return spec

    def _is_sos_export_limit_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return (
            "exceeds limit of 50000 observations" in msg
            or "query exceeds limit" in msg
        )

    def _split_taxon_batch(taxon_ids: list[int]) -> tuple[list[int], list[int]]:
        mid = max(1, len(taxon_ids) // 2)
        return taxon_ids[:mid], taxon_ids[mid:]

    def _split_year_range(year_from: int, year_to: int) -> tuple[tuple[int, int], tuple[int, int]] | None:
        if year_from >= year_to:
            return None
        mid = (year_from + year_to) // 2
        return (year_from, mid), (mid + 1, year_to)
    
    def _sha256_file(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def _stable_json_sha256(obj: Any) -> str:
        blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()

    def _sos_export_cache_key(
            *,
            taxon_ids: list[int],
            year_from: int,
            year_to: int,
            occurrence_status: str | None,
            output_field_set: str,
            culture_code: str = "sv-SE",
    ) -> str:
        return _stable_json_sha256(
            {
                "kind": "sos_csv_export_v1",
                "taxon_ids": sorted(int(t) for t in taxon_ids),
                "year_from": int(year_from),
                "year_to": int(year_to),
                "occurrence_status": occurrence_status or "",
                "output_field_set": output_field_set,
                "culture_code": culture_code,
            }
        )

    def _csv_index_dir(cfg: Config) -> Path:
        p = cfg.csv_stash_dir.parent / "csv_index"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _read_export_cache_manifest(cfg: Config, cache_key: str) -> dict[str, Any] | None:
        p = _csv_index_dir(cfg) / f"{cache_key}.json"
        if not p.exists():
            return None
        try:
            m = json.loads(p.read_text(encoding="utf-8"))
            zp = Path(m.get("zip_path", ""))
            
            if not zp.exists():
                return None

            expected_sha = m.get("content_sha256")
            if expected_sha and _sha256_file(zp) != expected_sha:
                logger.warning("cache manifest sha mismatch key=%s path=%s", cache_key[:16], zp)
                return None

            return m
        except Exception:
            return None

    def _write_export_cache_manifest(
            cfg: Config,
            *,
            cache_key: str,
                zip_path: Path,
            taxon_ids: list[int],
            year_from: int,
            year_to: int,
            occurrence_status: str | None,
            output_field_set: str,
    ) -> dict[str, Any]:
        manifest = {
            "cache_key": cache_key,
            "zip_path": str(zip_path),
            "content_sha256": _sha256_file(zip_path),
            "taxon_ids_sha256": _stable_json_sha256(sorted(int(t) for t in taxon_ids)),
            "taxon_count": len(taxon_ids),
            "year_from": int(year_from),
            "year_to": int(year_to),
            "occurrence_status": occurrence_status or "",
            "output_field_set": output_field_set,
            "created_at": local_now_iso(),
            "last_used_at": local_now_iso(),
        }
        p = _csv_index_dir(cfg) / f"{cache_key}.json"
        p.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest    

    def _touch_export_cache_manifest(cfg: Config, manifest: dict[str, Any]) -> None:
        manifest = dict(manifest)
        manifest["last_used_at"] = local_now_iso()
        p = _csv_index_dir(cfg) / f"{manifest['cache_key']}.json"
        p.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    def _safe_batch_label(batch_index: int, taxon_count: int) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{stamp}__sosjob_batch{batch_index:05d}_taxa{taxon_count}.zip"

    def _run_sos_import_job(job_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        if not cfg.subscription_key:
            raise RuntimeError("Missing ARTDATABANKEN_SUBSCRIPTION_KEY")
        if not cfg.authorization:
            raise RuntimeError("Missing ARTDATABANKEN_AUTHORIZATION")

        taxon_list_path = Path(spec["taxon_list_csv"]).expanduser().resolve()

        explicit_taxon_ids = [int(t) for t in (spec.get("taxon_ids") or [])]
        if explicit_taxon_ids:
            taxon_ids_all = sorted(set(explicit_taxon_ids))
        else:
            taxon_list_path = Path(spec["taxon_list_csv"]).expanduser().resolve()
            taxon_ids_all = read_taxon_ids_from_csv(taxon_list_path)

        if not taxon_ids_all:
            raise RuntimeError(f"No taxon ids found in {taxon_list_path}")

        initial_batch_size = max(1, int(spec.get("initial_batch_size", 20)))
        min_batch_size = max(1, int(spec.get("min_batch_size", 1)))
        adaptive_split = bool(spec.get("adaptive_split", True))

        pending: list[dict[str, Any]] = [
            {
                "taxon_ids": batch,
                "year_from": int(spec["year_from"]),
                "year_to": int(spec["year_to"]),
                "split_depth": 0,
            }
            for batch in chunked(taxon_ids_all, initial_batch_size)
        ]

        cfg.csv_stash_dir.mkdir(parents=True, exist_ok=True)
        cfg.csv_meta_dir.mkdir(parents=True, exist_ok=True)

        summary: dict[str, Any] = {
            "ok": True,
            "job_id": job_id,
            "taxon_list_csv": str(taxon_list_path),
            "taxa_total": len(taxon_ids_all),
            "initial_batch_size": initial_batch_size,
            "min_batch_size": min_batch_size,
            "adaptive_split": adaptive_split,
            "exports_ok": 0,
            "exports_empty": 0,
            "exports_split": 0,
            "exports_failed": 0,
            "raw_scopes": 0,
            "layers_written": 0,
            "rebuilds_written": 0,
            "stashed_exports": [],
            "failed_batches": [],
            "warnings": [],
            "year_splits": 0,
            "taxon_splits": 0,
        }

        # Estimate: export+import+consolidate per initial batch, plus rebuild phase.
        JOB_MANAGER.set_total_steps(
            job_id,
            max(len(pending) * 3 + 1, 1)
        )
        JOB_MANAGER.set_phase(job_id, "planning", current_step=f"taxa={len(taxon_ids_all)} batches={len(pending)}")
        
        all_touched_taxa: set[int] = set()
        all_touched_years: set[int] = set()
        all_touched_slots: set[int] = set()
        all_touched_zooms: set[int] = set()

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None

        batch_serial = 0

        try:
            storage.ensure_schema(conn)
            with conn:
                _upsert_taxon_dim_from_sources(conn, taxon_ids_all, cfg)
            while pending:
                JOB_MANAGER.ensure_not_cancelled(job_id)

                batch_serial += 1
                batch_obj = pending.pop(0)

                batch_taxon_ids = [int(t) for t in batch_obj["taxon_ids"]]
                batch_year_from = int(batch_obj["year_from"])
                batch_year_to = int(batch_obj["year_to"])
                split_depth = int(batch_obj.get("split_depth", 0))                
                batch_size = len(batch_taxon_ids)
                
                JOB_MANAGER.set_phase(
                    job_id,
                    "sos_export",
                    current_step=f"batch={batch_serial} pending={len(pending)} taxa={batch_size} years={batch_year_from}-{batch_year_to}",
                )
                # Stopwatch (debug) 
                batch_t0 = time.perf_counter()
                export_t0 = time.perf_counter()
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                export_path = cfg.csv_stash_dir / (
                    f"{stamp}__sosjob_batch{batch_serial:05d}_"
                    f"taxa{batch_size}_years{batch_year_from}-{batch_year_to}.zip"
                )

                search_filter = make_sos_export_filter(
                    taxon_ids=batch_taxon_ids,
                    year_from=batch_year_from,
                    year_to=batch_year_to,
                )

                output_field_set = str(spec.get("output_field_set", "All"))
                occurrence_status = spec.get("csv_occurrence_status") or spec.get("occurrence_status")

                cache_key = _sos_export_cache_key(
                    taxon_ids=batch_taxon_ids,
                    year_from=batch_year_from,
                    year_to=batch_year_to,
                    occurrence_status=occurrence_status,
                    output_field_set=output_field_set,
                )
                logger.info(
                    "job %s cache probe batch=%d force=%s key=%s taxa=%d years=%d-%d",
                    job_id,
                    batch_serial,
                    bool(spec.get("force", False)),
                    cache_key[:16],
                    len(batch_taxon_ids),
                    batch_year_from,
                    batch_year_to,
                )

                cached_manifest = None if bool(spec.get("force", False)) else _read_export_cache_manifest(cfg, cache_key)
                reused_export = False

                if cached_manifest:
                    export_path = Path(cached_manifest["zip_path"])
                    _touch_export_cache_manifest(cfg, cached_manifest)
                    reused_export = True

                    summary.setdefault("exports_reused", 0)
                    summary["exports_reused"] += 1

                    logger.info(
                        "job %s step=reused %s cache_key=%s",
                        job_id,
                        export_path.name,
                        cache_key[:12],
                    )
                    logger.info(
                        "job %s cache hit batch=%d key=%s path=%s",
                        job_id,
                        batch_serial,
                        cache_key[:16],
                        cached_manifest.get("zip_path"),
                    )

                    JOB_MANAGER.advance(
                        job_id,
                        phase="sos_export",
                        current_step=f"reused {export_path.name}",
                    )

                else:
                    logger.info(
                        "job %s cache miss batch=%d key=%s",
                        job_id,
                        batch_serial,
                        cache_key[:16],
                    )

                    try:
                        export_csv_zip_to_file(
                            cfg,
                            search_filter,
                            export_path,
                            output_field_set=output_field_set,
                            gzip=True,
                            culture_code="sv-SE",
                        )

                    except Exception as exc:
                        if _is_sos_export_limit_error(exc):
                            if adaptive_split and batch_size > min_batch_size:
                                left, right = _split_taxon_batch(batch_taxon_ids)

                                pending.insert(0, {
                                    "taxon_ids": right,
                                    "year_from": batch_year_from,
                                    "year_to": batch_year_to,
                                    "split_depth": split_depth + 1,
                                })
                                pending.insert(0, {
                                    "taxon_ids": left,
                                    "year_from": batch_year_from,
                                    "year_to": batch_year_to,
                                    "split_depth": split_depth + 1,
                                })
                                JOB_MANAGER.add_total_steps(job_id, 6)
                                                                
                                summary["taxon_splits"] += 1
                                summary["exports_split"] += 1

                                msg = (
                                    f"SOS export limit hit for {batch_size} taxa "
                                    f"years={batch_year_from}-{batch_year_to}; "
                                    f"split taxa into {len(left)} + {len(right)}"
                                )
                                summary["warnings"].append(msg)
                                JOB_MANAGER.append_warning(job_id, msg)
                                continue

                            yr_split = _split_year_range(batch_year_from, batch_year_to)
                            if adaptive_split and yr_split is not None:
                                (a_from, a_to), (b_from, b_to) = yr_split

                                pending.insert(0, {
                                    "taxon_ids": batch_taxon_ids,
                                    "year_from": b_from,
                                    "year_to": b_to,
                                    "split_depth": split_depth + 1,
                                })
                                pending.insert(0, {
                                    "taxon_ids": batch_taxon_ids,
                                    "year_from": a_from,
                                    "year_to": a_to,
                                    "split_depth": split_depth + 1,
                                })
                                JOB_MANAGER.add_total_steps(job_id, 6)
                                summary["year_splits"] += 1
                                summary["exports_split"] += 1

                                msg = (
                                    f"SOS export limit hit for single-taxon batch "
                                    f"taxon={batch_taxon_ids[0]} years={batch_year_from}-{batch_year_to}; "
                                    f"split years into {a_from}-{a_to} + {b_from}-{b_to}"
                                )
                                summary["warnings"].append(msg)
                                JOB_MANAGER.append_warning(job_id, msg)
                                continue

                            failed_path = _write_failed_taxon_batch(
                                cfg,
                                job_id,
                                batch_serial,
                                batch_taxon_ids,
                                f"{exc}; years={batch_year_from}-{batch_year_to}",
                            )

                            summary["exports_failed"] += 1
                            summary["failed_batches"].append({
                                "batch_serial": batch_serial,
                                "taxon_ids": batch_taxon_ids,
                                "size": batch_size,
                                "year_from": batch_year_from,
                                "year_to": batch_year_to,
                                "error": str(exc),
                                "failed_path": str(failed_path),
                            })

                            msg = (
                                f"SOS export limit hit and cannot split further; "
                                f"taxa={batch_taxon_ids} years={batch_year_from}-{batch_year_to}; "
                                f"dumped to {failed_path}"
                            )
                            summary["warnings"].append(msg)
                            JOB_MANAGER.append_warning(job_id, msg)
                            continue

                        summary["exports_failed"] += 1
                        summary["failed_batches"].append({
                            "batch_serial": batch_serial,
                            "taxon_ids": batch_taxon_ids,
                            "size": batch_size,
                            "year_from": batch_year_from,
                            "year_to": batch_year_to,
                            "error": str(exc),
                        })
                        raise

                    JOB_MANAGER.advance(
                        job_id,
                        phase="sos_export",
                        current_step=f"exported {export_path.name}",
                    )

                    _write_export_cache_manifest(
                        cfg,
                        cache_key=cache_key,
                        zip_path=export_path,
                        taxon_ids=batch_taxon_ids,
                        year_from=batch_year_from,
                        year_to=batch_year_to,
                        occurrence_status=occurrence_status,
                        output_field_set=output_field_set,
                    )

                export_dt = time.perf_counter() - export_t0
                logger.info(
                    "job %s timing batch=%s export_seconds=%.3f reused=%s taxa=%s years=%s-%s",
                    job_id,
                    batch_serial,
                    export_dt,
                    reused_export,
                    len(batch_taxon_ids),
                    batch_year_from,
                    batch_year_to,
                )

                if not export_path.exists() or export_path.stat().st_size == 0:
                    summary["exports_empty"] += 1
                    JOB_MANAGER.append_warning(job_id, f"Empty SOS export for batch size={batch_size}")
                    continue

                summary["exports_ok"] += 1
                summary["stashed_exports"].append({
                    "path": str(export_path),
                    "taxon_count": batch_size,
                    "year_from": batch_year_from,
                    "year_to": batch_year_to,
                    "reused": reused_export,
                    "cache_key": cache_key,
                })

                csv_or_zip = export_path
                if csv_or_zip.suffix.lower() == ".zip":
                    csv_or_zip = find_csv_inside_zip(csv_or_zip)

                ingest = CsvIngestArgs(
                    zip_or_csv=csv_or_zip,
                    db_path=cfg.geomap_db_path,
                    zooms=list(spec["zooms"]),
                    taxon_ids=batch_taxon_ids,
                    include_slot0=bool(spec.get("include_slot0", True)),
                    date_field=str(spec.get("csv_date_field", "StartDate")),
                    occurrence_status=spec.get("csv_occurrence_status"),
                )

                raw_t0 = time.perf_counter()

                JOB_MANAGER.set_phase(
                    job_id,
                    "csv_import_raw",
                    current_step=f"batch={batch_serial} taxa={batch_size}",
                )

                touched = import_observations_raw(conn, ingest)

                summary["raw_scopes"] += len(touched)
                JOB_MANAGER.advance(
                    job_id,
                    phase="csv_import_raw",
                    current_step=f"batch={batch_serial} scopes={len(touched)}",
                )

                if not touched:
                    logger.info(
                        "job %s batch=%s imported no scopes reused=%s path=%s",
                        job_id,
                        batch_serial,
                        reused_export,
                        export_path,
                    )
                    continue

                years = sorted({k[0] for k in touched})
                slot_ids = sorted({k[1] for k in touched})
                zooms_scope = sorted({k[2] for k in touched}, reverse=True)
                taxon_ids_scope = sorted({k[3] for k in touched})

                all_touched_taxa.update(taxon_ids_scope)
                all_touched_years.update(years)
                all_touched_slots.update(slot_ids)
                all_touched_zooms.update(zooms_scope)

                raw_dt = time.perf_counter() - raw_t0
                logger.info(
                    "job %s timing batch=%s raw_import_seconds=%.3f scopes=%s reused=%s",
                    job_id,
                    batch_serial,
                    raw_dt,
                    len(touched),
                    reused_export,
                )

                consolidate_t0 = time.perf_counter()

                JOB_MANAGER.set_phase(
                    job_id,
                    "csv_consolidate",
                    current_step=f"batch={batch_serial} scopes={len(touched)}",
                )

                layers_written = consolidate_taxon_grid_from_raw_bulk_tile_bbox(
                    conn,
                    taxon_ids=taxon_ids_scope,
                    years=years,
                    slot_ids=slot_ids,
                    zooms=zooms_scope,
                    include_slot0=bool(spec.get("include_slot0", True)),
                )

                summary["layers_written"] += int(layers_written)
                JOB_MANAGER.advance(
                    job_id,
                    phase="csv_consolidate",
                    current_step=f"batch={batch_serial} layers={layers_written}",
                )

                consolidate_dt = time.perf_counter() - consolidate_t0
                batch_dt = time.perf_counter() - batch_t0
                logger.info(
                    "job %s timing batch=%s consolidate_seconds=%.3f total_batch_seconds=%.3f layers=%s reused=%s",
                    job_id,
                    batch_serial,
                    consolidate_dt,
                    batch_dt,
                    layers_written,
                    reused_export,
                )                
                # end of if (cached_manifest)
                
            if not all_touched_taxa:
                raise RuntimeError("SOS import completed, but no observations were imported")

            rebuild_years = sorted(all_touched_years)
            if bool(spec.get("include_all_years", True)):
                rebuild_years = sorted(set(rebuild_years + [YEAR_ALL]))

            rebuild_slots = sorted(set(all_touched_slots))
            if bool(spec.get("include_slot0", True)):
                rebuild_slots = sorted(set(rebuild_slots + [SLOT_ALL]))
                
            rebuild_zooms = sorted(all_touched_zooms, reverse=True)
            rebuild_taxa = sorted(all_touched_taxa)
            
            JOB_MANAGER.set_phase(
                job_id,
                "rebuild_hotmaps",
                current_step=(
                    f"bulk rebuild "
                    f"years={len(rebuild_years)} "
                    f"slots={len(rebuild_slots)} "
                    f"zooms={len(rebuild_zooms)}"
                ),
            )
            
            JOB_MANAGER.ensure_not_cancelled(job_id)
            
            with conn:
                rows_written = storage.rebuild_hotmap_bulk(
                    conn,
                    zooms=rebuild_zooms,
                    slot_ids=rebuild_slots,
                    years=rebuild_years,
                    taxon_ids=rebuild_taxa,
                    alpha=float(spec["alpha"]),
                    beta=float(spec["beta"]),
                )
                
            summary["rebuilds_written"] = rows_written
            
            JOB_MANAGER.advance(
                job_id,
                phase="rebuild_hotmaps",
                current_step=f"bulk rows={rows_written}",
            )
            
            meta_payload = dict(summary)
            meta_payload.update(
                {
                    "finished_at": local_now_iso(),
                    "taxon_ids": sorted(all_touched_taxa),
                    "years": sorted(all_touched_years),
                    "slot_ids": sorted(all_touched_slots),
                    "zooms": sorted(all_touched_zooms, reverse=True),
                }
            )
            meta_path = _write_csv_job_metadata(cfg, job_id, meta_payload)

            summary["meta_path"] = str(meta_path)
            summary["taxon_ids"] = sorted(all_touched_taxa)
            summary["years"] = sorted(all_touched_years)
            summary["slot_ids"] = sorted(all_touched_slots)
            summary["zooms"] = sorted(all_touched_zooms, reverse=True)
            summary["finished_at"] = local_now_iso()

            return summary

        finally:
            conn.close()


    def _write_failed_taxon_batch(
            cfg: Config,
            job_id: str,
            batch_serial: int,
            taxon_ids: list[int],
            error: str,
    ) -> Path:
        cfg.csv_meta_dir.mkdir(parents=True, exist_ok=True)
        out = cfg.csv_meta_dir / f"{job_id}__failed_batch{batch_serial:05d}.csv"

        import csv
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["taxon_id", "error"])
            for tid in taxon_ids:
                w.writerow([tid, error])
                
        return out
            

    def _run_csv_import_job(job_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        input_path = _resolve_csv_input_path(cfg, spec.get("csv_name"), spec.get("csv_path"))
        stashed_path = _copy_to_stash(job_id, input_path, cfg.csv_stash_dir) if bool(spec.get("stash_copy", True)) else input_path
        extracted_or_csv = find_csv_inside_zip(stashed_path) if stashed_path.suffix.lower() == ".zip" else stashed_path

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            ingest = CsvIngestArgs(
                zip_or_csv=extracted_or_csv,
                db_path=cfg.geomap_db_path,
                zooms=list(spec["zooms"]),
                taxon_ids=[int(t) for t in (spec.get("taxon_ids") or [])] or None,
                include_slot0=bool(spec.get("include_slot0", True)),
                date_field=str(spec.get("csv_date_field", "StartDate")),
                occurrence_status=spec.get("csv_occurrence_status"),
            )

            JOB_MANAGER.set_phase(job_id, "csv_import_raw", current_step=stashed_path.name)
            touched = import_observations_raw(conn, ingest)

            years = sorted({k[0] for k in touched})
            slot_ids = sorted({k[1] for k in touched})
            zooms_scope = sorted({k[2] for k in touched}, reverse=True)
            taxon_ids_scope = sorted({k[3] for k in touched})
            with conn:
                _upsert_taxon_dim_from_sources(
                    conn,
                    taxon_ids_scope,
                    cfg,
                )
            
            JOB_MANAGER.set_phase(job_id, "csv_consolidate", current_step=f"scopes={len(touched)}")
            layers_written = consolidate_taxon_grid_from_raw_bulk_tile_bbox(
                conn,
                taxon_ids=taxon_ids_scope,
                years=years,
                slot_ids=slot_ids,
                zooms=zooms_scope,
                include_slot0=ingest.include_slot0,
            )

            all_year_layers_written = 0
            if bool(spec.get("include_all_years", True)):
                all_year_layers_written = consolidate_taxon_grid_year_all_from_grid(
                    conn,
                    taxon_ids=taxon_ids_scope,
                    years=years,
                    slot_ids=slot_ids,
                    zooms=zooms_scope,
                    include_slot0=ingest.include_slot0,
                )

            meta_payload = {
                "job_id": job_id,
                "source_path": str(input_path),
                "stashed_path": str(stashed_path),
                "csv_path": str(extracted_or_csv),
                "imported_at": local_now_iso(),
                "taxon_ids": taxon_ids_scope,
                "years": years,
                "slot_ids": slot_ids,
                "zooms": zooms_scope,
                "touched_scopes": len(touched),
                "layers_written": layers_written,
                "all_year_layers_written": all_year_layers_written,
            }
            meta_path = _write_csv_job_metadata(cfg, job_id, meta_payload)

            return {
                "ok": True,
                "job_id": job_id,
                "source_path": str(input_path),
                "stashed_path": str(stashed_path),
                "csv_path": str(extracted_or_csv),
                "meta_path": str(meta_path),
                "touched_scopes": len(touched),
                "layers_written": layers_written,
                "all_year_layers_written": all_year_layers_written,
                "taxon_ids": taxon_ids_scope,
                "years": years,
                "slot_ids": slot_ids,
                "zooms": zooms_scope,
            }
        finally:
            conn.close()

    def _replace_taxon_grid_year_all_from_counts(
        conn: sqlite3.Connection,
            *,
            taxon_id: int,
            zoom: int,
            slot_id: int,
            cell_counts: list[tuple[int, int, int]],
    ) -> int:
        now = local_now_iso()
        
        conn.execute(
            "DELETE FROM taxon_grid WHERE taxon_id=? AND zoom=? AND year=? AND slot_id=?;",
            (taxon_id, zoom, YEAR_ALL, slot_id),
        )

        rows = []
        for x, y, obs_count in cell_counts:
            top_lat, left_lon, bottom_lat, right_lon = tile_xy_to_bbox(int(x), int(y), int(zoom))
            rows.append(
                (
                    taxon_id,
                    zoom,
                    YEAR_ALL,
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
                taxon_id, zoom, year, slot_id, x, y,
                observations_count, taxa_count,
                bbox_top_lat, bbox_left_lon, bbox_bottom_lat, bbox_right_lon,
                fetched_at_utc
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
                """,
                rows,
            )
            
        return len(rows)
    
    def _run_raw_rebuild_job(job_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            explicit_taxon_ids = [int(t) for t in (spec.get("taxon_ids") or [])]
            years_scope = list(range(int(spec["year_from"]), int(spec["year_to"]) + 1))
            zooms_scope = list(spec["zooms"])
            slot_ids_scope = list(spec["fetch_slots"])
            full_reconsolidate = bool(spec.get("full_reconsolidate", False))

            if full_reconsolidate:
                JOB_MANAGER.set_phase(job_id, "raw_scan", current_step="discovering raw scopes")

                where = []
                args: list[Any] = []

                if explicit_taxon_ids:
                    where.append("taxon_id IN ({})".format(",".join(["?"] * len(explicit_taxon_ids))))
                    args.extend(explicit_taxon_ids)

                if zooms_scope:
                    where.append("zoom IN ({})".format(",".join(["?"] * len(zooms_scope))))
                    args.extend([int(z) for z in zooms_scope])

                if years_scope:
                    where.append("year IN ({})".format(",".join(["?"] * len(years_scope))))
                    args.extend([int(y) for y in years_scope])

                if slot_ids_scope:
                    where.append("slot_id IN ({})".format(",".join(["?"] * len(slot_ids_scope))))
                    args.extend([int(s) for s in slot_ids_scope])

                wh = ("WHERE " + " AND ".join(where)) if where else ""
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT taxon_id, year, zoom, slot_id
                    FROM observations_raw
                    {wh}
                    ORDER BY taxon_id, year, zoom, slot_id;
                    """,
                    args,
                ).fetchall()

                taxon_ids_scope = sorted({int(r[0]) for r in rows})
                _upsert_taxon_dim_from_sources(
                    conn,
                    taxon_ids_scope,
                    cfg,
                )
                years_scope = sorted({int(r[1]) for r in rows})
                zooms_scope = sorted({int(r[2]) for r in rows}, reverse=True)
                slot_ids_scope = sorted({int(r[3]) for r in rows})
            else:
                taxon_ids_scope = explicit_taxon_ids

            if not taxon_ids_scope:
                raise RuntimeError("No raw observations found for requested raw_rebuild scope")

            JOB_MANAGER.set_phase(
                job_id,
                "raw_consolidate",
                current_step=f"taxa={len(taxon_ids_scope)} years={len(years_scope)} slots={len(slot_ids_scope)}",
            )

            layers_written = _timed_log(
                "consolidate_taxon_grid_from_raw_bulk_tile_bbox",
                lambda: consolidate_taxon_grid_from_raw_bulk_tile_bbox(
                    conn,
                    taxon_ids=taxon_ids_scope,
                    years=years_scope or None,
                    slot_ids=slot_ids_scope or None,
                    zooms=zooms_scope or None,
                    include_slot0=bool(spec.get("include_slot0", True)),
                ),
            )
            
            all_year_layers_written = 0
            if bool(spec.get("include_all_years", True)):
                JOB_MANAGER.set_phase(
                    job_id,
                    "raw_consolidate_all_years",
                    current_step=(
                        f"taxa={len(taxon_ids_scope)} years={len(years_scope)} "
                        f"slots={len(slot_ids_scope)} zooms={len(zooms_scope)}"
                    ),
                )
                
                all_year_layers_written = _timed_log(
                    "consolidate_taxon_grid_year_all_from_raw",
                    lambda: consolidate_taxon_grid_year_all_from_grid(
                        conn,
                        taxon_ids=taxon_ids_scope,
                        years=years_scope,
                        slot_ids=slot_ids_scope,
                        zooms=zooms_scope,
                        include_slot0=bool(spec.get("include_slot0", True)),
                    ),
                )

            raw_rows_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM observations_raw
                WHERE year BETWEEN ? AND ?
                """,
                (int(spec["year_from"]), int(spec["year_to"])),
            ).fetchone()[0]

            grid_rows_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM taxon_grid
                WHERE year BETWEEN ? AND ?
                """,
                (int(spec["year_from"]), int(spec["year_to"])),
            ).fetchone()[0]

            return {
                "ok": True,
                "job_id": job_id,
                "full_reconsolidate": full_reconsolidate,
                "taxon_ids": taxon_ids_scope,
                "years": years_scope,
                "slot_ids": slot_ids_scope,
                "zooms": zooms_scope,
                "layers_written": int(layers_written),
                "all_year_layers_written": int(all_year_layers_written),
                "raw_rows_count": int(raw_rows_count),
                "grid_rows_count": int(grid_rows_count),
            }
        finally:
            conn.close()

            
    def _run_rebuild_job(job_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        refresh_mode = str(spec.get("refresh_mode", "upstream")).strip().lower()
        if refresh_mode not in {"upstream", "local", "csv_import", "raw_rebuild"}:
            raise RuntimeError(f"Unsupported refresh_mode={refresh_mode}")

        csv_summary = None
        raw_rebuild_summary = None
        
        if refresh_mode == "csv_import":
            csv_summary = _run_csv_import_job(job_id, spec)
            spec = dict(spec)
            if not spec.get("taxon_ids"):
                spec["taxon_ids"] = list(csv_summary.get("taxon_ids") or [])

        if refresh_mode == "raw_rebuild":
            raw_rebuild_summary = _run_raw_rebuild_job(job_id, spec)
            spec = dict(spec)
            if not spec.get("taxon_ids"):
                spec["taxon_ids"] = list(raw_rebuild_summary.get("taxon_ids") or [])
                
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
                taxa_rows = storage.read_taxa_rows(cfg.missing_species_csv, int(spec["n"]))
                taxon_ids = [int(t["taxon_id"]) for t in taxa_rows]
                if not taxon_ids:
                    raise RuntimeError("No taxon ids found in CSV")

            if refresh_mode in {"local", "raw_rebuild"}:
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

            rebuild_years = list(years) + ([YEAR_ALL] if spec["include_all_years"] else [])
            rebuild_steps = len(zooms) * len(rebuild_years) * len(final_slots)

            if refresh_mode == "raw_rebuild":
                fetch_steps = 0
                derive_slot0_steps = 0
                derive_all_years_steps = 0
                total_steps = 3
            elif refresh_mode == "local":
                fetch_steps = 0
                derive_slot0_steps = 0
                derive_all_years_steps = 0
                total_steps = rebuild_steps
            else:
                fetch_steps = len(taxon_ids) * len(years) * len(fetch_slots)
                derive_slot0_steps = len(taxon_ids) * len(years) if spec["include_slot0"] else 0
                derive_all_years_steps = len(taxon_ids) * len(final_slots) if spec["include_all_years"] else 0
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

            hotmap_rows_written = 0

            JOB_MANAGER.set_phase(
                job_id,
                "rebuild_hotmaps",
                current_step=(
                    f"bulk rebuild years={len(rebuild_years)} "
                    f"slots={len(final_slots)} zooms={len(zooms)} taxa={len(taxon_ids)}"
                ),
            )
            
            JOB_MANAGER.ensure_not_cancelled(job_id)
            
            with conn:
                hotmap_rows_written = storage.rebuild_hotmap_bulk(
                    conn,
                    zooms=zooms,
                    years=rebuild_years,
                    slot_ids=final_slots,
                    taxon_ids=taxon_ids,
                alpha=spec["alpha"],
                    beta=spec["beta"],
                )

            JOB_MANAGER.advance(
                job_id,
                phase="rebuild_hotmaps",
                current_step=f"bulk rows={hotmap_rows_written}",
                inc=1,
            )

                        
            JOB_MANAGER.set_phase(job_id, "finalizing", current_step="writing summary")
            summary = {
                "ok": True,
                "finished_at": local_now_iso(),
                "csv_import": csv_summary if refresh_mode == "csv_import" else None,
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
                "hotmap_rows_written": int(hotmap_rows_written),                
            }
            if csv_summary is not None:
                summary["csv_import"] = csv_summary
            if raw_rebuild_summary is not None:
                summary["raw_rebuild"] = raw_rebuild_summary
                
            logger.info("job=%s rebuild done summary=%s", job_id, summary)
            return summary
        finally:
            conn.close()

    
    def _hotmap_score(coverage: int, obs_total: int) -> float:
        return (float(coverage) ** float(cfg.hotmap_alpha)) / (
            (float(obs_total or 0) + 1.0) ** float(cfg.hotmap_beta)
        )


    def _hotmap_feature(row, zoom, year_from, year_to):
        (
            slot_id_db,
            x,
            y,
            coverage,
            obs_total,
            top_lat,
            left_lon,
            bottom_lat,
            right_lon,
        ) = row
        
        score = _hotmap_score(int(coverage), int(obs_total or 0))

        poly = [
            [float(left_lon), float(top_lat)],
            [float(right_lon), float(top_lat)],
            [float(right_lon), float(bottom_lat)],
            [float(left_lon), float(bottom_lat)],
            [float(left_lon), float(top_lat)],
        ]

        return {
            "type": "Feature",
            "properties": {
                "zoom": int(zoom),
                "slot_id": int(slot_id_db),
                "year_from": None if year_from == YEAR_ALL else int(year_from),
                "year_to": None if year_to == YEAR_ALL else int(year_to),
                "x": int(x),
                "y": int(y),
                "coverage": int(coverage),
                "score": float(score),
                "obs_total": int(obs_total or 0),
            },
            "geometry": {"type": "Polygon", "coordinates": [poly]},
        }

    @app.get("/geomap-api/jobs/status")
    def jobs_status():
        return jsonify(JOB_MANAGER.get_status())

    @app.get("/geomap-api/jobs/<job_id>")
    @require_grant("jobs.read")
    def jobs_get(job_id: str):
        snap = JOB_MANAGER.get_job(job_id)
        if snap is None:
            return jsonify({"ok": False, "code": "not_found", "error": f"Unknown job_id: {job_id}"}), 404
        return jsonify({"ok": True, "busy": JOB_MANAGER.busy(), "job": snap})

    @app.post("/geomap-api/jobs/<job_id>/cancel")
    @require_grant("jobs.read")
    def jobs_cancel(job_id: str):
        ok = JOB_MANAGER.cancel(job_id)
        if not ok:
            snap = JOB_MANAGER.get_job(job_id)
            if snap is None:
                return jsonify({"ok": False, "code": "not_found", "error": f"Unknown job_id: {job_id}"}), 404
            return jsonify({"ok": False, "code": "invalid_state", "error": f"Cannot cancel job in state={snap['status']}"}), 409
        return jsonify({"ok": True, "job_id": job_id, "status": "cancelling"}), 202

    @app.post("/geomap-api/jobs/rebuild")
    @require_grant("jobs.read")
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
            "status_url": f"/geomap-api/jobs/{job.job_id}",
            "busy": True,
            "spec": spec,
        }), 202

    @app.post("/geomap-api/jobs/sos_import")
    @require_grant("jobs.read")
    def jobs_sos_import():
        body = request.get_json(force=True) or {}
        spec = _normalize_sos_import_spec(body)

        try:
            job = JOB_MANAGER.start_job(
                kind="sos_import",
                spec=spec,
                target=lambda job_id: _run_sos_import_job(job_id, spec),
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
            "status_url": f"/geomap-api/jobs/{job.job_id}",
            "busy": True,
            "spec": spec,
        }), 202

    
    @app.post("/geomap-api/pipeline/build")
    @require_grant("jobs.read")
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
            "status_url": f"/geomap-api/jobs/{job.job_id}",
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


    @app.get("/geomap-api/hotmap")
    def hotmap_geojson():
        zoom = int(request.args.get("zoom", "15"))
        slot_id = parse_slot_id(request.args.get("slot_id", SLOT_ALL))
        year_from, year_to = parse_year_range_args(request.args)
        
        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        
        try:
            storage.ensure_schema(conn)
        
            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                rows = conn.execute(
                    """
                    SELECT
                        h.slot_id,
                        h.x,
                        h.y,
                        h.coverage,
                        COALESCE(SUM(v.observations_count), 0) AS obs_total,
                        h.bbox_top_lat,
                        h.bbox_left_lon,
                        h.bbox_bottom_lat,
                        h.bbox_right_lon
                    FROM grid_hotmap h
                    LEFT JOIN grid_hotmap_taxa_names_v v
                      ON v.zoom = h.zoom
                     AND v.year = h.year
                     AND v.slot_id = h.slot_id
                     AND v.x = h.x
                     AND v.y = h.y
                    WHERE h.zoom = ?
                      AND h.year = ?
                      AND h.slot_id = ?
                    GROUP BY
                        h.slot_id, h.x, h.y, h.coverage,
                        h.bbox_top_lat, h.bbox_left_lon,
                        h.bbox_bottom_lat, h.bbox_right_lon
                    ORDER BY h.coverage DESC, obs_total DESC;
                    """,
                    (zoom, YEAR_ALL, slot_id),
                ).fetchall()
        
                logger.info(
                    "hotmap request zoom=%d slot=%d year=ALL rows=%d",
                    zoom,
                    slot_id,
                    len(rows),
                )
        
            else:
                rows = conn.execute(
                    """
                    WITH taxa AS (
                        SELECT
                            v.slot_id,
                            v.x,
                            v.y,
                            COUNT(DISTINCT v.taxon_id) AS coverage,
                            SUM(v.observations_count) AS obs_total
                        FROM grid_hotmap_taxa_names_v v
                        WHERE v.zoom = ?
                          AND v.year BETWEEN ? AND ?
                          AND (? = 0 OR v.slot_id = ?)
                        GROUP BY v.slot_id, v.x, v.y
                    ),
                    bbox AS (
                        SELECT
                            h.slot_id,
                            h.x,
                            h.y,
                            MAX(h.bbox_top_lat) AS bbox_top_lat,
                            MIN(h.bbox_left_lon) AS bbox_left_lon,
                            MIN(h.bbox_bottom_lat) AS bbox_bottom_lat,
                            MAX(h.bbox_right_lon) AS bbox_right_lon
                        FROM grid_hotmap h
                        WHERE h.zoom = ?
                          AND h.year BETWEEN ? AND ?
                          AND (? = 0 OR h.slot_id = ?)
                        GROUP BY h.slot_id, h.x, h.y
                    )
                    SELECT
                        taxa.slot_id,
                        taxa.x,
                        taxa.y,
                        taxa.coverage,
                        taxa.obs_total,
                        bbox.bbox_top_lat,
                        bbox.bbox_left_lon,
                        bbox.bbox_bottom_lat,
                        bbox.bbox_right_lon
                    FROM taxa
                    JOIN bbox
                      ON bbox.slot_id = taxa.slot_id
                     AND bbox.x = taxa.x
                     AND bbox.y = taxa.y
                    ORDER BY taxa.coverage DESC, taxa.obs_total DESC;
                    """,
                    (
                        zoom,
                        year_from,
                        year_to,
                        slot_id,
                        slot_id,
                        zoom,
                        year_from,
                        year_to,
                        slot_id,
                        slot_id,
                    ),
                ).fetchall()
        
                logger.info(
                    "hotmap request zoom=%d slot=%d year=%d..%d rows=%d",
                    zoom,
                    slot_id,
                    year_from,
                    year_to,
                    len(rows),
                )
        
            return jsonify({
                "type": "FeatureCollection",
                "features": [
                    _hotmap_feature(row, zoom, year_from, year_to)
                    for row in rows
                ],
            })
        
        finally:
            conn.close()

        
    @app.get("/geomap-api/hotmap_window")
    def hotmap_window_geojson():
        zoom = int(request.args.get("zoom", "15"))
        slot_ids = parse_slot_ids_arg(request.args.get("slot_ids", None), name="slot_ids")
        year_from, year_to = parse_year_range_args(request.args)
        
        if not slot_ids:
            return jsonify({"type": "FeatureCollection", "features": []})
        
        placeholders = ",".join(["?"] * len(slot_ids))
        window_slot_id = slot_ids[len(slot_ids) // 2]
        
        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        
        try:
            storage.ensure_schema(conn)
        
            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                rows = conn.execute(
                    f"""
                    WITH taxa AS (
                        SELECT
                            v.x,
                            v.y,
                            COUNT(DISTINCT v.taxon_id) AS coverage,
                            SUM(v.observations_count) AS obs_total
                        FROM grid_hotmap_taxa_names_v v
                        WHERE v.zoom = ?
                          AND v.year = ?
                          AND v.slot_id IN ({placeholders})
                        GROUP BY v.x, v.y
                    ),
                    bbox AS (
                        SELECT
                            h.x,
                            h.y,
                            MAX(h.bbox_top_lat) AS bbox_top_lat,
                            MIN(h.bbox_left_lon) AS bbox_left_lon,
                            MIN(h.bbox_bottom_lat) AS bbox_bottom_lat,
                            MAX(h.bbox_right_lon) AS bbox_right_lon
                        FROM grid_hotmap h
                        WHERE h.zoom = ?
                          AND h.year = ?
                          AND h.slot_id IN ({placeholders})
                        GROUP BY h.x, h.y
                    )
                    SELECT
                        ? AS slot_id,
                        taxa.x,
                        taxa.y,
                        taxa.coverage,
                        taxa.obs_total,
                        bbox.bbox_top_lat,
                        bbox.bbox_left_lon,
                        bbox.bbox_bottom_lat,
                        bbox.bbox_right_lon
                    FROM taxa
                    JOIN bbox
                      ON bbox.x = taxa.x
                     AND bbox.y = taxa.y
                    ORDER BY taxa.coverage DESC, taxa.obs_total DESC;
                    """,
                    (
                        zoom,
                        YEAR_ALL,
                        *slot_ids,
                        zoom,
                        YEAR_ALL,
                        *slot_ids,
                        window_slot_id,
                    ),
                ).fetchall()
        
                logger.info(
                    "hotmap_window request zoom=%d year=ALL slots=%s center_slot=%d rows=%d",
                    zoom,
                    ",".join(map(str, slot_ids)),
                    window_slot_id,
                    len(rows),
                )
        
            else:
                rows = conn.execute(
                    f"""
                    WITH taxa AS (
                        SELECT
                            v.x,
                            v.y,
                            COUNT(DISTINCT v.taxon_id) AS coverage,
                            SUM(v.observations_count) AS obs_total
                        FROM grid_hotmap_taxa_names_v v
                        WHERE v.zoom = ?
                          AND v.year BETWEEN ? AND ?
                          AND v.slot_id IN ({placeholders})
                        GROUP BY v.x, v.y
                    ),
                    bbox AS (
                        SELECT
                            h.x,
                            h.y,
                            MAX(h.bbox_top_lat) AS bbox_top_lat,
                            MIN(h.bbox_left_lon) AS bbox_left_lon,
                            MIN(h.bbox_bottom_lat) AS bbox_bottom_lat,
                            MAX(h.bbox_right_lon) AS bbox_right_lon
                        FROM grid_hotmap h
                        WHERE h.zoom = ?
                          AND h.year BETWEEN ? AND ?
                          AND h.slot_id IN ({placeholders})
                        GROUP BY h.x, h.y
                    )
                    SELECT
                        ? AS slot_id,
                        taxa.x,
                        taxa.y,
                        taxa.coverage,
                        taxa.obs_total,
                        bbox.bbox_top_lat,
                        bbox.bbox_left_lon,
                        bbox.bbox_bottom_lat,
                        bbox.bbox_right_lon
                    FROM taxa
                    JOIN bbox
                      ON bbox.x = taxa.x
                     AND bbox.y = taxa.y
                    ORDER BY taxa.coverage DESC, taxa.obs_total DESC;
                    """,
                    (
                        zoom,
                        year_from,
                        year_to,
                        *slot_ids,
                        zoom,
                        year_from,
                        year_to,
                        *slot_ids,
                        window_slot_id,
                    ),
                ).fetchall()
        
                logger.info(
                    "hotmap_window request zoom=%d year=%d..%d slots=%s center_slot=%d rows=%d",
                    zoom,
                    year_from,
                    year_to,
                    ",".join(map(str, slot_ids)),
                    window_slot_id,
                    len(rows),
                )
        
            return jsonify({
                "type": "FeatureCollection",
                "features": [
                    _hotmap_feature(row, zoom, year_from, year_to)
                    for row in rows
                ],
            })
        
        finally:
            conn.close()
            

        
    @app.get("/geomap-api/cell/taxa")
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
                    SELECT
                    r.taxon_id,
                    COALESCE(MAX(d.scientific_name), '') AS scientific_name,
                    COALESCE(MAX(d.swedish_name), '') AS swedish_name,
                    COUNT(*) AS observations_count,
                    MIN(r.observation_date) AS first_observed,
                    MAX(r.observation_date) AS last_observed
                    FROM observations_raw r
                    LEFT JOIN taxon_dim d
                    ON d.taxon_id = r.taxon_id
                    WHERE r.zoom=?
                    AND r.year BETWEEN ? AND ?
                    AND r.tile_x=?
                    AND r.tile_y=?
                    AND (? = 0 OR r.slot_id = ?)
                    GROUP BY r.taxon_id
                    ORDER BY observations_count DESC, r.taxon_id
                    LIMIT ?;
                    """,
                    (zoom, YEAR_MIN, YEAR_MAX, x, y, slot_id, slot_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                    r.taxon_id,
                    COALESCE(MAX(d.scientific_name), '') AS scientific_name,
                    COALESCE(MAX(d.swedish_name), '') AS swedish_name,
                    COUNT(*) AS observations_count,
                    MIN(r.observation_date) AS first_observed,
                    MAX(r.observation_date) AS last_observed
                    FROM observations_raw r
                    LEFT JOIN taxon_dim d
                    ON d.taxon_id = r.taxon_id
                    WHERE r.zoom=?
                    AND r.tile_x=?
                    AND r.tile_y=?
                    AND r.year BETWEEN ? AND ?
                    AND (? = 0 OR r.slot_id = ?)
                    GROUP BY r.taxon_id
                    ORDER BY observations_count DESC, r.taxon_id
                    LIMIT ?;
                    """,
                    (zoom, x, y, year_from, year_to, slot_id, slot_id, limit),
                ).fetchall()

            out = []
            for r in rows:
                out.append(
                    {
                        "taxon_id": int(r[0]),
                        "scientific_name": r[1] or "",
                        "swedish_name": r[2] or "",
                        "observations_count": int(r[3] or 0),
                        "first_observed": r[4] or None,
                        "last_observed": r[5] or None,
                    }
                )
            return jsonify(out)
        finally:
            conn.close()


    @app.get("/geomap-api/cell/taxa_window")
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
                    r.taxon_id,
                    COALESCE(MAX(d.scientific_name), '') AS scientific_name,
                    COALESCE(MAX(d.swedish_name), '') AS swedish_name,
                    COUNT(*) AS observations_count,
                    MIN(r.observation_date) AS first_observed,
                    MAX(r.observation_date) AS last_observed
                    FROM observations_raw r
                    LEFT JOIN taxon_dim d
                    ON d.taxon_id = r.taxon_id
                    WHERE r.zoom=?
                    AND r.year BETWEEN ? AND ?
                    AND r.tile_x=?
                    AND r.tile_y=?
                    AND r.slot_id IN ({placeholders})
                    GROUP BY r.taxon_id
                    ORDER BY observations_count DESC, r.taxon_id
                    LIMIT ?;
                    """,
                    (zoom, YEAR_MIN, YEAR_MAX, x, y, *slot_ids, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                    r.taxon_id,
                    COALESCE(MAX(d.scientific_name), '') AS scientific_name,
                    COALESCE(MAX(d.swedish_name), '') AS swedish_name,
                    COUNT(*) AS observations_count,
                    MIN(r.observation_date) AS first_observed,
                    MAX(r.observation_date) AS last_observed
                    FROM observations_raw r
                    LEFT JOIN taxon_dim d
                    ON d.taxon_id = r.taxon_id
                    WHERE r.zoom=?
                    AND r.tile_x=?
                    AND r.tile_y=?
                    AND r.year BETWEEN ? AND ?
                    AND r.slot_id IN ({placeholders})
                    GROUP BY r.taxon_id
                    ORDER BY observations_count DESC, r.taxon_id
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
                        "first_observed": r[4] or None,
                        "last_observed": r[5] or None,
                    }
                )
            return jsonify(out)
        finally:
            conn.close()        
            
    @app.get("/geomap-api/slots/coverage")
    def slots_coverage():
        zoom = int(request.args.get("zoom", "15"))
        year_from, year_to = parse_year_range_args(request.args)

        conn = storage.connect(cfg.geomap_db_path)
        conn.isolation_level = None
        try:
            storage.ensure_schema(conn)

            if year_from == YEAR_ALL and year_to == YEAR_ALL:
                rows = conn.execute(
                    """
                    SELECT slot_id, COUNT(*) AS cells
                    FROM grid_hotmap
                    WHERE zoom=? AND year=?
                    GROUP BY slot_id
                    ORDER BY slot_id;
                    """,
                    (zoom, YEAR_ALL),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT slot_id, COUNT(*) AS cells
                    FROM grid_hotmap
                    WHERE zoom=? AND year BETWEEN ? AND ?
                    GROUP BY slot_id
                    ORDER BY slot_id;
                    """,
                    (zoom, year_from, year_to),
                ).fetchall()
            
            out = [
                {
                    "slot_id": int(r[0]),
                    "cells": int(r[1]),
                }
                for r in rows
            ]

            return jsonify(out)

        finally:
            conn.close()

    @app.get("/geomap-api/cell/phenology")
    def cell_phenology():
         zoom = int(request.args.get("zoom", "15"))
         x = int(request.args["x"])
         y = int(request.args["y"])
         year_from, year_to = parse_year_range_args(request.args)

         conn = storage.connect(cfg.geomap_db_path)
         conn.isolation_level = None
         try:
             storage.ensure_schema(conn)

             if year_from == YEAR_ALL and year_to == YEAR_ALL:
                 rows = conn.execute(
                     """
                     SELECT
                         r.slot_id,
                         COUNT(*) AS observations_count,
                         COUNT(DISTINCT r.taxon_id) AS taxa_count,
                         MIN(r.observation_date) AS first_observed,
                         MAX(r.observation_date) AS last_observed
                     FROM observations_raw r
                     WHERE r.zoom=?
                       AND r.year BETWEEN ? AND ?
                       AND r.tile_x=?
                       AND r.tile_y=?
                       AND r.slot_id BETWEEN 1 AND 48
                     GROUP BY r.slot_id
                     ORDER BY r.slot_id;
                     """,
                     (zoom, YEAR_MIN, YEAR_MAX, x, y),
                 ).fetchall()
             else:
                 rows = conn.execute(
                     """
                     SELECT
                         r.slot_id,
                         COUNT(*) AS observations_count,
                         COUNT(DISTINCT r.taxon_id) AS taxa_count,
                         MIN(r.observation_date) AS first_observed,
                         MAX(r.observation_date) AS last_observed
                     FROM observations_raw r
                     WHERE r.zoom=?
                       AND r.tile_x=?
                       AND r.tile_y=?
                       AND r.year BETWEEN ? AND ?
                       AND r.slot_id BETWEEN 1 AND 48
                     GROUP BY r.slot_id
                     ORDER BY r.slot_id;
                     """,
                     (zoom, x, y, year_from, year_to),
                 ).fetchall()

             by_slot = {
                 int(r[0]): {
                     "slot_id": int(r[0]),
                     "observations_count": int(r[1] or 0),
                     "taxa_count": int(r[2] or 0),
                     "first_observed": r[3] or None,
                     "last_observed": r[4] or None,
                 }
                 for r in rows
             }

             max_obs = max(
                 [v["observations_count"] for v in by_slot.values()] or [0]
             )

             slots = []
             for slot_id in range(1, 49):
                 item = by_slot.get(
                     slot_id,
                     {
                         "slot_id": slot_id,
                         "observations_count": 0,
                         "taxa_count": 0,
                         "first_observed": None,
                         "last_observed": None,
                     },
                 )
                 item["normalized"] = (
                     float(item["observations_count"]) / float(max_obs)
                     if max_obs > 0
                     else 0.0
                 )
                 slots.append(item)

             logger.info(
                 "cell_phenology zoom=%d x=%d y=%d year=%s rows=%d max_obs=%d",
                 zoom,
                 x,
                 y,
                 "0" if (year_from == YEAR_ALL and year_to == YEAR_ALL) else f"{year_from}..{year_to}",
                 len(rows),
                 max_obs,
             )

             return jsonify({
                 "ok": True,
                 "zoom": int(zoom),
                 "x": int(x),
                 "y": int(y),
                 "year_from": None if year_from == YEAR_ALL else int(year_from),
                 "year_to": None if year_to == YEAR_ALL else int(year_to),
                 "max_observations_count": int(max_obs),
                 "slots": slots,
             })
         finally:
             conn.close()    
            
    @app.get("/geomap-api/rank_nearby")
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

    
