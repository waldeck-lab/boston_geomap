# geomap/sos_export.py

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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import time, re, io, zipfile, csv, requests
from .config import Config

def parse_retry_after_seconds(resp: requests.Response) -> Optional[int]:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return int(ra)
        except ValueError:
            pass
    m = re.search(r"Try again in\s+(\d+)\s+seconds", resp.text or "", flags=re.IGNORECASE)
    return int(m.group(1)) if m else None

_LAST_TS = 0.0
def throttle(min_interval_s: float) -> None:
    global _LAST_TS
    now = time.time()
    wait = (_LAST_TS + min_interval_s) - now
    if wait > 0:
        time.sleep(wait)
    _LAST_TS = time.time()

def post_with_backoff(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    body: Dict[str, Any],
    *,
    timeout_s: int = 180,
    min_interval_s: float = 15.0,
    max_retries: int = 8,
) -> requests.Response:
    last = None
    for _attempt in range(max_retries):
        throttle(min_interval_s)
        resp = requests.post(url, headers=headers, params=params, json=body, timeout=timeout_s)
        last = resp
        if resp.status_code != 429:
            return resp
        delay = parse_retry_after_seconds(resp) or 60
        time.sleep(min(max(delay, 1), 120) + 0.5)
    return last  # type: ignore[return-value]

def parse_csv_from_zip_bytes(zip_bytes: bytes) -> List[Dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise RuntimeError("ZIP did not contain a .csv file")
        raw = zf.read(csv_name)
        text = raw.decode("utf-8-sig", errors="replace")
        first = text.splitlines()[0] if text else ""
        if "\t" in first:
            delim = "\t"
        elif ";" in first and "," not in first:
            delim = ";"
        else:
            delim = ","
        r = csv.DictReader(io.StringIO(text), delimiter=delim, restval="")
        return [dict(row) for row in r]

def export_csv(
    cfg: Config,
    search_filter: Dict[str, Any],
    *,
    output_field_set: str = "Minimum",
    output_fields: Optional[List[str]] = None,
    gzip: bool = True,
    sensitive_observations: bool = False,
    validate_search_filter: bool = False,
    culture_code: str = "sv-SE",
) -> List[Dict[str, Any]]:
    if not cfg.subscription_key:
        raise RuntimeError("Missing ARTDATABANKEN_SUBSCRIPTION_KEY")
    # Depending on SOS rules, export may require Authorization even without observedByMe
    if not cfg.authorization:
        raise RuntimeError("Missing ARTDATABANKEN_AUTHORIZATION (Bearer token)")

    url = cfg.base_url.rstrip("/") + "/Exports/Download/Csv"
    params = {
        "outputFieldSet": output_field_set,
        "validateSearchFilter": "true" if validate_search_filter else "false",
        "cultureCode": culture_code,
        "gzip": "true" if gzip else "false",
        "sensitiveObservations": "true" if sensitive_observations else "false",
    }
    if output_fields:
        # If SOS expects these inside filter instead, move them there
        search_filter = dict(search_filter)
        search_filter["outputFields"] = output_fields

    headers = {
        "X-Api-Version": cfg.api_version,
        "Ocp-Apim-Subscription-Key": cfg.subscription_key,
        "Content-Type": "application/json",
        "Accept": "application/zip, application/octet-stream, */*",
        "Cache-Control": "no-cache",
        "Authorization": cfg.authorization,
    }

    resp = post_with_backoff(url, headers, params, search_filter)
    if resp.status_code == 204:
        return []
    if resp.status_code != 200:
        raise RuntimeError(f"Export failed: HTTP {resp.status_code} – {(resp.text or '')[:500]}")
    return parse_csv_from_zip_bytes(resp.content)
