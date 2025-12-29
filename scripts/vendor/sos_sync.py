#!/usr/bin/env python3

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

"""
Incremental local sync (SQLite) for a lightweight "species list" cache:
- observation_index: observationId -> (taxonId, modifiedUtc)
- species_counts: taxonId -> count
- sync_state: watermarkUtc

This script targets the SOS endpoint:
POST /Exports/Download/GeoJson

It relies on two key filter features in SearchFilterDto:
- observedByMe / reportedByMe (to scope to "my observations")
- modifiedDate (to fetch changes since a given timestamp)

You must verify the actual field names in the GeoJSON properties once
(SightingId/TaxonId/Modified may differ in casing or naming).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import io
import zipfile
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Dict, Iterable, Optional, List

import re
import time
import requests

from sos_filters import SearchFilter


@dataclass(frozen=True)
class Config:
    # Base SOS API URL
    base_url: str = "https://api.artdatabanken.se/species-observation-system/v1"

    # Export endpoint 
    endpoint: str = "/Exports/Download/Csv"

    # Query params (matches your example call)
    params: Dict[str, str] = None

    # Headers / auth
    subscription_key: str = os.getenv("ARTDATABANKEN_SUBSCRIPTION_KEY", "").strip()
    authorization: str = os.getenv("ARTDATABANKEN_AUTHORIZATION", "").strip()
    api_version_header: str = os.getenv("ARTDATABANKEN_X_API_VERSION", "1.5").strip()

    # SQLite file
    db_path: str = os.getenv("SOS_DB_PATH",os.path.join("data", "db", "sos_counts.sqlite"))

    # How much to rewind watermark each run to catch late updates
    overlap_hours: int = 48

    # CSV "Minimum" export uses these column names
    obs_id_field: str = "OccurrenceId"
    taxon_id_field: str = "DyntaxaTaxonId"
    modified_field: str = "Modified"
    
    # GeoJSON keys
    geojson_features_key: str = "features"
    geojson_properties_key: str = "properties"

    # Logging
    logs_dir: str = "logs"
    log_level: str = os.getenv("SOS_LOG_LEVEL", "INFO").strip().upper()
    log_to_console: bool = os.getenv("SOS_LOG_CONSOLE", "1") == "1"

    # Debug switch (off by default)
    debug: bool = (
        os.getenv("SOS_DEBUG", "0") == "1"
        or ("--debug" in sys.argv)
    )

def dbg(logger: logging.Logger, enabled: bool, msg: str, *args: Any) -> None:
    if enabled:
        logger.info("DEBUG: " + msg, *args)
    
def log_db_status(conn: sqlite3.Connection, logger: logging.Logger) -> None:
    cur = conn.execute("SELECT watermark_utc FROM sync_state WHERE id=1;")
    wm = cur.fetchone()[0]
    n_obs = conn.execute("SELECT COUNT(*) FROM observation_index;").fetchone()[0]
    n_taxa = conn.execute("SELECT COUNT(*) FROM species_counts;").fetchone()[0]
    logger.info("DB status: watermark=%s observations=%d taxa=%d", wm, n_obs, n_taxa)
    
def auto_select_field_mapping(cfg: Config, records: list[dict], logger: logging.Logger) -> Config:
    """
    Return a new Config with obs/taxon/modified field names adjusted based on record keys.
    """
    if not records:
        return cfg

    keys = set(records[0].keys())

    # CSV export
    if {"OccurrenceId", "DyntaxaTaxonId", "Modified"}.issubset(keys):
        logger.info("Auto field mapping: CSV (OccurrenceId/DyntaxaTaxonId/Modified)")
        return Config(
            **{**cfg.__dict__,
               "obs_id_field": "OccurrenceId",
               "taxon_id_field": "DyntaxaTaxonId",
               "modified_field": "Modified"}
        )

    # GeoJSON properties (your old default)
    if {"SightingId", "TaxonId", "Modified"}.issubset(keys):
        logger.info("Auto field mapping: GeoJSON properties (SightingId/TaxonId/Modified)")
        return cfg

    logger.warning("Could not auto-detect field mapping from keys: %s", sorted(list(keys))[:50])
    return cfg
        

import base64

def jwt_expiry_utc(auth_header: str) -> Optional[datetime]:
    """
    Extract exp from a JWT in an Authorization header.
    Returns UTC datetime or None if not parseable.
    """
    s = (auth_header or "").strip()
    if s.lower().startswith("bearer "):
        s = s[7:].strip()

    parts = s.split(".")
    if len(parts) != 3:
        return None

    payload_b64 = parts[1]
    # base64url padding
    payload_b64 += "=" * (-len(payload_b64) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
    except Exception:
        return None

    exp = payload.get("exp")
    if exp is None:
        return None

    try:
        return datetime.fromtimestamp(int(exp), tz=timezone.utc)
    except Exception:
        return None

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    
def ensure_token_not_expired(cfg: Config, logger: logging.Logger) -> None:
    if not cfg.authorization:
        return  # caller decides if auth is required

    exp = jwt_expiry_utc(cfg.authorization)
    if not exp:
        logger.warning("Could not parse JWT exp; continuing anyway.")
        return

    now = datetime.now(timezone.utc)
    if exp <= now:
        raise RuntimeError(
            f"Authorization token expired at {iso_utc(exp)} (now {iso_utc(now)}). "
            "Refresh ARTDATABANKEN_AUTHORIZATION."
        )
    
def post_with_rate_limit(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    body: Dict[str, Any],
    logger: logging.Logger,
    timeout_s: int,
    max_retries: int = 3,
) -> requests.Response:
    """
    POST helper for SOS download endpoints:
    - enforces spacing between calls (5/min limit)
    - retries on HTTP 429, respecting Retry-After or message 'Try again in N seconds'
    """
    last_resp: Optional[requests.Response] = None

    for attempt in range(1, max_retries + 1):
        throttle_download_calls(logger, min_interval_s=15.0)

        resp = requests.post(url, headers=headers, params=params, json=body, timeout=timeout_s)
        last_resp = resp

        if resp.status_code != 429:
            return resp

        delay = parse_retry_after_seconds(resp) or 60
        logger.warning(
            "HTTP 429 rate limit. Retry in %d s (attempt %d/%d).",
            delay, attempt, max_retries
        )
        time.sleep(delay)

    # Exhausted retries; return last response so caller can log details
    return last_resp  # type: ignore[return-value]
    
def setup_logger(cfg: Config) -> logging.Logger:
    """
    Create a logger that writes to ./logs/<scriptname>.log and optionally to console.
    Uses daily rotation and keeps a limited history.
    """
    os.makedirs(cfg.logs_dir, exist_ok=True)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    log_path = os.path.join(cfg.logs_dir, f"{script_name}.log")

    logger = logging.getLogger(script_name)
    logger.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    logger.propagate = False  # avoid duplicate logs if root logger is configured

    # Clear existing handlers (important if the module is reloaded or called multiple times)
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler: rotate daily, keep 14 days
    file_handler = TimedRotatingFileHandler(
        log_path,
        when="D",
        interval=1,
        backupCount=14,
        encoding="utf-8",
        utc=True,
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    if cfg.log_to_console:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(fmt)
        logger.addHandler(console)

    logger.debug("Logger initialized. Writing to %s", log_path)
    return logger


def parse_csv_from_zip_bytes(zip_bytes: bytes, logger: logging.Logger, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Extract a CSV/TSV file from a ZIP archive and parse into list-of-dicts (header -> value).
    SOS "CSV" exports are often TSV (tab-separated). We auto-detect delimiter.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        logger.info("ZIP entries: %s", names)

        # Log filter.json only in debug mode
        if debug:
            filt = next((n for n in names if n.lower().endswith("filter.json")), None)
            if filt:
                try:
                    txt_f = zf.read(filt).decode("utf-8-sig", errors="replace")
                    dbg(logger, debug, "filter.json (first 4000 chars):\n%s", txt_f[:4000])
                except Exception as e:
                    logger.warning("Could not read filter.json: %s", e)

        csv_name = next((n for n in names if n.lower().endswith(".csv")), None)
        if csv_name is None:
            raise RuntimeError(f"ZIP did not contain a .csv file. Entries: {names}")

        raw = zf.read(csv_name)
        text = raw.decode("utf-8-sig", errors="replace")

        first_line = text.splitlines()[0] if text else ""
        if "\t" in first_line:
            delimiter = "\t"
        elif ";" in first_line and "," not in first_line:
            delimiter = ";"
        else:
            delimiter = ","

        logger.info("Detected CSV delimiter: %r", delimiter)

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter, restval="")
        rows = [dict(row) for row in reader]

        if rows and debug:
            dbg(logger, debug, "CSV columns: %s", list(rows[0].keys())[:200])
            # show a small stable sample of the first row
            first_keys = list(rows[0].keys())[:12]
            dbg(logger, debug, "First CSV row sample: %s", {k: rows[0].get(k) for k in first_keys})

        return rows

def check_api_info(cfg: Config, logger: logging.Logger) -> Dict[str, Any]:
    """
    Perform a simple connectivity/auth check against SOS ApiInfo endpoint.
    Returns the parsed ApiInfo JSON.
    """
    if not cfg.subscription_key:
        raise RuntimeError("Missing subscription key. Set ARTDATABANKEN_SUBSCRIPTION_KEY.")

    url = cfg.base_url.rstrip("/") + "/api/ApiInfo"

    headers = {
        "X-Api-Version": cfg.api_version_header,
        "Ocp-Apim-Subscription-Key": cfg.subscription_key,
        "Accept": "application/json",
        "Cache-Control": "no-cache",
    }

    logger.info("ApiInfo check: GET %s (X-Api-Version=%s)", url, cfg.api_version_header)

    resp = requests.get(url, headers=headers, timeout=30)

    if resp.status_code != 200:
        # Include body text (usually short) for troubleshooting
        raise RuntimeError(f"ApiInfo check failed: HTTP {resp.status_code} – {resp.text}")

    info = resp.json()
    logger.info("SOS API reachable: apiName=%s apiVersion=%s apiStatus=%s",
                info.get("apiName"), info.get("apiVersion"), info.get("apiStatus"))
    logger.debug("ApiInfo payload: %s", json.dumps(info, ensure_ascii=False))
    return info


_OBS_ID_TAIL_RE = re.compile(r"(\d+)\s*$")
def normalize_observation_id(raw: Any) -> str:
    """
    Convert OccurrenceId/SightingId variants into a compact stable key.
    Examples:
      'urn:lsid:artportalen.se:sighting:121870623' -> '121870623'
      '121870623' -> '121870623'
    Falls back to stripped string if no numeric tail exists.
    """
    s = "" if raw is None else str(raw).strip()
    if not s:
        return s

    m = _OBS_ID_TAIL_RE.search(s)
    if m:
        return m.group(1)

    # fallback: also try last ':' chunk if it is numeric
    tail = s.rsplit(":", 1)[-1].strip()
    if tail.isdigit():
        return tail

    return s
            
_LAST_DOWNLOAD_CALL_TS = 0.0
def throttle_download_calls(logger: logging.Logger, min_interval_s: float = 15.0) -> None:
    """
    Enforce spacing between download calls to stay under the 5/min limit.
    """
    global _LAST_DOWNLOAD_CALL_TS
    now = time.time()
    wait = (_LAST_DOWNLOAD_CALL_TS + min_interval_s) - now
    if wait > 0:
        logger.info("Throttling download call: sleeping %.1f s", wait)
        time.sleep(wait)
    _LAST_DOWNLOAD_CALL_TS = time.time()


def parse_retry_after_seconds(resp: requests.Response) -> Optional[int]:
    """
    Try to obtain retry delay from Retry-After header or the known message format:
    'Try again in 49 seconds.'
    """
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return int(ra)
        except ValueError:
            pass

    m = re.search(r"Try again in\s+(\d+)\s+seconds", resp.text or "", flags=re.IGNORECASE)
    if m:
        return int(m.group(1))

    return None

def parse_iso_utc(s: str) -> datetime:
    """
    Parse ISO-ish timestamps and normalize to UTC.

    Supports:
      - YYYY-MM-DD
      - YYYY-MM-DDTHH:MM:SS
      - YYYY-MM-DDTHH:MM:SS.sss
      - Optional trailing Z
      - Optional timezone offset (+01:00, etc.)
      - If no timezone present, assume UTC.
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("empty timestamp")

    # Date-only
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        dt = datetime.fromisoformat(s)  # naive date
        return dt.replace(tzinfo=timezone.utc)

    # Normalize Z suffix to +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = datetime.fromisoformat(s)

    # If no tzinfo, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)

def iso_utc(dt: datetime) -> str:
    """Render datetime as ISO-8601 UTC string with Z suffix."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_db(conn: sqlite3.Connection) -> None:
    """Create required tables if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            watermark_utc TEXT NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS observation_index (
            observation_id TEXT PRIMARY KEY,
            taxon_id TEXT NOT NULL,
            modified_utc TEXT NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS species_counts (
            taxon_id TEXT PRIMARY KEY,
            count INTEGER NOT NULL
        );
    """)

    # Initialize watermark if missing
    cur = conn.execute("SELECT watermark_utc FROM sync_state WHERE id=1;")
    row = cur.fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO sync_state (id, watermark_utc) VALUES (1, ?);",
            ("1970-01-01T00:00:00Z",)
        )
    conn.commit()


def get_watermark(conn: sqlite3.Connection) -> datetime:
    """Read current watermark from DB."""
    cur = conn.execute("SELECT watermark_utc FROM sync_state WHERE id=1;")
    (wm,) = cur.fetchone()
    return parse_iso_utc(wm)


def set_watermark(conn: sqlite3.Connection, wm: datetime) -> None:
    """Persist watermark to DB (no commit here; caller controls transaction)."""
    conn.execute("UPDATE sync_state SET watermark_utc=? WHERE id=1;", (iso_utc(wm),))


def upsert_count(conn: sqlite3.Connection, taxon_id: str, delta: int) -> None:
    """Apply a delta (+/-) to taxon count, keeping it non-negative."""
    conn.execute("""
        INSERT INTO species_counts(taxon_id, count) VALUES(?, ?)
        ON CONFLICT(taxon_id) DO UPDATE SET count = count + excluded.count;
    """, (taxon_id, delta))

    conn.execute(
        "UPDATE species_counts SET count=0 WHERE taxon_id=? AND count < 0;",
        (taxon_id,)
    )
    

def get_first_present(d: Dict[str, Any], candidates: Iterable[str]) -> Optional[Any]:
    for k in candidates:
        if k in d and d[k] is not None:
            return d[k]
    return None

def fetch_changed_observations(
    cfg: Config,
    search_filter: Dict[str, Any],
    logger: logging.Logger,
    replay_zip_path: Optional[str] = None,
    capture_zip_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch changed observations via SOS export endpoint (CSV).
    Returns list-of-dicts, each dict is one CSV row (header -> value).
    """
    if replay_zip_path:
        logger.info("Replaying export ZIP from disk: %s", replay_zip_path)
        with open(replay_zip_path, "rb") as f:
            zip_bytes = f.read()
        return parse_csv_from_zip_bytes(zip_bytes, logger, debug=cfg.debug)

    if not cfg.subscription_key:
        raise RuntimeError("Missing subscription key. Set ARTDATABANKEN_SUBSCRIPTION_KEY.")

    if not cfg.authorization:
        raise RuntimeError("Authorization missing. Set ARTDATABANKEN_AUTHORIZATION='Bearer <token>'.")

    url = cfg.base_url.rstrip("/") + cfg.endpoint
    params = cfg.params or {
        "outputFieldSet": "Minimum",
        "validateSearchFilter": "false",
        "cultureCode": "sv-SE",
        "gzip": "true",
        "sensitiveObservations": "false",
    }

    logger.info(
        "Export request flags: observedByMe=%s reportedByMe=%s sensitiveObservations=%s outputFieldSet=%s",
        search_filter.get("observedByMe"),
        search_filter.get("reportedByMe"),
        params.get("sensitiveObservations"),
        params.get("outputFieldSet"),
    )

    headers: Dict[str, str] = {
        "X-Api-Version": cfg.api_version_header,
        "Ocp-Apim-Subscription-Key": cfg.subscription_key,
        "Content-Type": "application/json",
        "Accept": "application/zip, application/octet-stream, */*",
        "Cache-Control": "no-cache",
        "Authorization": cfg.authorization,
    }

    # Auth diagnostics only in debug mode (do NOT log the token itself)
    dbg(logger, cfg.debug, "Authorization header present: %s", True)
    auth = (cfg.authorization or "").strip()
    dbg(logger, cfg.debug, "Authorization looks like Bearer: %s (len=%d)", auth.startswith("Bearer "), len(auth))
    exp = jwt_expiry_utc(cfg.authorization)
    if exp:
        dbg(logger, cfg.debug, "JWT exp (UTC): %s (now UTC: %s)", iso_utc(exp), iso_utc(datetime.now(timezone.utc)))
    else:
        dbg(logger, cfg.debug, "JWT exp (UTC): <unavailable>")

    resp = post_with_rate_limit(
        url=url,
        headers=headers,
        params=params,
        body=search_filter,
        logger=logger,
        timeout_s=180,
        max_retries=3,
    )

    if resp.status_code == 204:
        return []

    if resp.status_code == 202:
        logger.warning("Export returned HTTP 202 Accepted; treating as empty for now.")
        return []

    if resp.status_code != 200:
        logger.error("Export call failed: HTTP %s", resp.status_code)
        logger.error("Response headers: %s", dict(resp.headers))
        logger.error("Response body (first 500 chars): %s", (resp.text or "")[:500])
        www_auth = resp.headers.get("WWW-Authenticate")
        if www_auth:
            logger.error("WWW-Authenticate: %s", www_auth)
        raise RuntimeError(f"Export call failed: HTTP {resp.status_code}")

    ct = (resp.headers.get("Content-Type") or "").lower()
    logger.info("Export response: HTTP 200 Content-Type=%s Content-Length=%s",
                ct, resp.headers.get("Content-Length"))

    if "application/zip" not in ct and resp.content[:4] != b"PK\x03\x04":
        raise RuntimeError(f"Expected ZIP response, got Content-Type={ct}")

    if capture_zip_path:
        os.makedirs(os.path.dirname(capture_zip_path) or ".", exist_ok=True)
        with open(capture_zip_path, "wb") as f:
            f.write(resp.content)
        logger.info("Captured export ZIP to: %s", capture_zip_path)

    return parse_csv_from_zip_bytes(resp.content, logger, debug=cfg.debug)

def apply_changes(conn, cfg, records, logger) -> datetime:
    max_modified: Optional[datetime] = None
    inserted = 0
    updated = 0
    skipped_older = 0
    skipped_missing = 0

    for r in records:
        raw_obs_id = get_first_present(r, [cfg.obs_id_field, "SightingId", "sightingId", "ObservationId", "observationId", "id"])
        obs_id = normalize_observation_id(raw_obs_id)
        taxon_id = get_first_present(r, [cfg.taxon_id_field, "TaxonId", "taxonId"])
        modified = get_first_present(r, [cfg.modified_field, "Modified", "modified", "ModifiedUtc", "modifiedUtc"])

        if not obs_id or taxon_id is None or modified is None:
            skipped_missing += 1
            continue

        try:
            modified_dt = parse_iso_utc(str(modified))
        except Exception:
            skipped_missing += 1
            continue

        if max_modified is None or modified_dt > max_modified:
            max_modified = modified_dt

        row = conn.execute(
            "SELECT taxon_id, modified_utc FROM observation_index WHERE observation_id=?;",
            (str(obs_id),)
        ).fetchone()

        if row is None:
            conn.execute(
                "INSERT INTO observation_index(observation_id, taxon_id, modified_utc) VALUES(?,?,?);",
                (str(obs_id), str(taxon_id), iso_utc(modified_dt))
            )
            upsert_count(conn, str(taxon_id), +1)
            inserted += 1
            continue

        old_taxon, old_modified = row
        old_modified_dt = parse_iso_utc(old_modified)

        if modified_dt <= old_modified_dt:
            skipped_older += 1
            continue

        if str(old_taxon) != str(taxon_id):
            upsert_count(conn, str(old_taxon), -1)
            upsert_count(conn, str(taxon_id), +1)

        conn.execute(
            "UPDATE observation_index SET taxon_id=?, modified_utc=? WHERE observation_id=?;",
            (str(taxon_id), iso_utc(modified_dt), str(obs_id))
        )
        updated += 1

    if skipped_missing:
        logger.warning("Skipped %d records due to missing/invalid fields.", skipped_missing)

    logger.info(
        "Changes: inserted=%d updated=%d skipped_older_or_equal=%d",
        inserted, updated, skipped_older
    )
    return max_modified if max_modified is not None else get_watermark(conn)

def main() -> int:
    cfg = Config()
    logger = setup_logger(cfg)
    logger.info("Debug mode: %s", cfg.debug)

    if len(sys.argv) < 1:
        script_name = os.path.basename(__file__)
        logger.error("Usage: %s ", script_name)
        return 2

    skip_export_probes = ("--skip-export-probes" in sys.argv)
    replay_zip_path = None
    capture_zip_path = None

    if "--replay-zip" in sys.argv:
        i = sys.argv.index("--replay-zip")
        replay_zip_path = sys.argv[i + 1]

    if "--capture-zip" in sys.argv:
        i = sys.argv.index("--capture-zip")
        capture_zip_path = sys.argv[i + 1]
    

    # ---- INIT CHECK -------------------------------------------------
    try:
        check_api_info(cfg, logger)
    except Exception as e:
        logger.exception("SOS API init check failed: %s", e)
        return 1
    # ----------------------------------------------------------------
    
    ensure_parent_dir(cfg.db_path)
    conn = sqlite3.connect(cfg.db_path)
    
    try:
        ensure_db(conn)
        log_db_status(conn,logger)

        wm = get_watermark(conn)

        DEV_START_ISO = "1972-04-14T00:00:00Z" # Set start of time (for late debug or early for production)
        dev_start_dt = parse_iso_utc(DEV_START_ISO)

        # If DB is fresh (epoch watermark), clamp to dev start date
        if wm.year <= 1971:
            logger.info(
                "Fresh database detected. Using development start date: %s",
                DEV_START_ISO,
            )
            since_dt = dev_start_dt
        else:
            since_dt = wm - timedelta(hours=cfg.overlap_hours)

        since_iso = iso_utc(since_dt)

        logger.info("Watermark=%s overlapHours=%d effectiveFrom=%s",
                    iso_utc(wm), cfg.overlap_hours, since_iso)

        sf = (
            SearchFilter.defaults_user_scope(reported_by_me=True, minimum_fields=None)
            .with_modified_from(since_iso)
            .with_output_fields(["OccurrenceId", "DyntaxaTaxonId", "Modified"])
        )
        search_filter = sf.to_dict()
                
        if (search_filter.get("observedByMe") or search_filter.get("reportedByMe")) and not cfg.authorization:
            logger.error("Template uses observedByMe/reportedByMe but Authorization is missing.")
            logger.error("Set ARTDATABANKEN_AUTHORIZATION='Bearer <token>' and retry.")
            return 1
        
        # Just validate token
        try:
            ensure_token_not_expired(cfg, logger)
        except Exception as e:
            logger.exception("Token/bearer expired: %s", e)
            return 1
        
        records = fetch_changed_observations(
            cfg,
            search_filter,
            logger,
            replay_zip_path=replay_zip_path,
            capture_zip_path=capture_zip_path,
        )

        logger.info("Fetched %d records from export response.", len(records))

        #cfg = auto_select_field_mapping(cfg, records, logger)

        if records and cfg.debug:
            dbg(logger, cfg.debug, "First CSV core fields: OccurrenceId=%s DyntaxaTaxonId=%s Modified=%s",
                records[0].get("OccurrenceId"),
                records[0].get("DyntaxaTaxonId"),
                records[0].get("Modified"))

            missing_mod = sum(1 for r in records if r.get("Modified") in (None, ""))
            dbg(logger, cfg.debug, "Records missing Modified: %d / %d", missing_mod, len(records))
                
        conn.execute("BEGIN;")
        
        logger.info(
            "Field mapping: obs_id_field=%s taxon_id_field=%s modified_field=%s",
            cfg.obs_id_field, cfg.taxon_id_field, cfg.modified_field
        )

        new_max = apply_changes(conn, cfg, records, logger)

        old_wm = get_watermark(conn)
        new_max = apply_changes(conn, cfg, records, logger)

        if new_max <= old_wm:
            logger.info("Watermark unchanged (new_max=%s <= old=%s). Likely date-only Modified + overlap window.",
                iso_utc(new_max), iso_utc(old_wm))
        
        # Persist both data + watermark in one transaction
        set_watermark(conn, new_max)
        conn.commit()
        
        log_db_status(conn, logger)
        
        logger.info("Sync complete. New watermark=%s SQLite=%s", iso_utc(new_max), cfg.db_path)
        return 0
        
    except Exception as e:
        conn.rollback()
        logger.exception("Sync failed: %s", e)
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())



