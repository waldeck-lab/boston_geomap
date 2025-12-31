# geomap:sos_client.py

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

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass(frozen=True)
class SOSClient:
    base_url: str
    api_version: str
    subscription_key: str
    authorization: str
    timeout_s: int = 180

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Api-Version": self.api_version,
            "Ocp-Apim-Subscription-Key": self.subscription_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "Authorization": self.authorization,
        }

    def geogrid_aggregation(
        self,
        taxon_ids: list[int],
        zoom: int,
        validate_search_filter: bool = False,
        translation_culture_code: str = "sv-SE",
        sensitive_observations: bool = False,
        skip_cache: bool = False,
        extra_filter: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = self.base_url.rstrip("/") + "/Observations/GeoGridAggregation"
        params = {
            "zoom": str(zoom),
            "validateSearchFilter": "true" if validate_search_filter else "false",
            "translationCultureCode": translation_culture_code,
            "sensitiveObservations": "true" if sensitive_observations else "false",
            "skipCache": "true" if skip_cache else "false",
        }

        body: Dict[str, Any] = {"taxon": {"ids": taxon_ids, "includeUnderlyingTaxa": False}}
        if extra_filter:
            body.update(extra_filter)

        resp = requests.post(url, headers=self._headers(), params=params, json=body, timeout=self.timeout_s)
        if resp.status_code != 200:
            raise RuntimeError(f"GeoGridAggregation failed: HTTP {resp.status_code} – {resp.text[:500]}")
        return resp.json()


def stable_gridcells_hash(payload: Dict[str, Any]) -> str:
    """
    Hash only what matters for change detection.
    Sort gridCells by (x,y) and hash relevant fields.
    """
    cells = payload.get("gridCells") or []
    slim = []
    for c in cells:
        bb = c.get("boundingBox") or {}
        tl = bb.get("topLeft") or {}
        br = bb.get("bottomRight") or {}
        slim.append(
            (
                int(c.get("x")),
                int(c.get("y")),
                int(c.get("zoom")),
                int(c.get("observationsCount") or 0),
                int(c.get("taxaCount") or 0),
                float(tl.get("latitude")),
                float(tl.get("longitude")),
                float(br.get("latitude")),
                float(br.get("longitude")),
            )
        )
    slim.sort(key=lambda t: (t[0], t[1]))

    blob = json.dumps(slim, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def throttle(min_interval_s: float, state: Dict[str, float]) -> None:
    """
    Simple in-process throttle for calls that might be rate-limited.
    """
    last = state.get("last_ts", 0.0)
    now = time.time()
    wait = (last + min_interval_s) - now
    if wait > 0:
        time.sleep(wait)
    state["last_ts"] = time.time()
