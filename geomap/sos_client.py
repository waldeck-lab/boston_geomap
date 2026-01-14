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
class BBox:
    top_lat: float
    left_lon: float
    bottom_lat: float
    right_lon: float


def _make_bbox_filter(bb: BBox) -> Dict[str, Any]:
    return {
        "geographics": {
            "boundingBox": {
                "topLeft": {"latitude": float(bb.top_lat), "longitude": float(bb.left_lon)},
                "bottomRight": {"latitude": float(bb.bottom_lat), "longitude": float(bb.right_lon)},
            }
        }
    }


def _split_bbox_4(bb: BBox) -> List[BBox]:
    mid_lat = (bb.top_lat + bb.bottom_lat) / 2.0
    mid_lon = (bb.left_lon + bb.right_lon) / 2.0

    # Quadrants:
    # NW, NE, SW, SE (non-overlapping)
    return [
        BBox(bb.top_lat, bb.left_lon, mid_lat, mid_lon),
        BBox(bb.top_lat, mid_lon,  mid_lat, bb.right_lon),
        BBox(mid_lat,   bb.left_lon, bb.bottom_lat, mid_lon),
        BBox(mid_lat,   mid_lon,  bb.bottom_lat, bb.right_lon),
    ]


def _merge_geogrid_payloads(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple GeoGridAggregation payloads into one.
    We merge gridCells by (x,y) and sum observationsCount/taxaCount.
    """
    out: Dict[str, Any] = {}
    merged: Dict[Tuple[int, int], Dict[str, Any]] = {}

    for p in payloads:
        if not out:
            # carry some top-level fields if present; gridCells handled separately
            out = {k: v for k, v in p.items() if k != "gridCells"}

        for c in (p.get("gridCells") or []):
            x = int(c.get("x"))
            y = int(c.get("y"))
            key = (x, y)
            if key not in merged:
                merged[key] = dict(c)
            else:
                # sum counts safely
                merged[key]["observationsCount"] = int(merged[key].get("observationsCount") or 0) + int(c.get("observationsCount") or 0)
                merged[key]["taxaCount"] = int(merged[key].get("taxaCount") or 0) + int(c.get("taxaCount") or 0)

    out["gridCells"] = list(merged.values())
    return out


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

    def geogrid_aggregation_resilient(
        self,
        taxon_ids: list[int],
        zoom: int,
        *,
        bbox: Optional[BBox] = None,
        extra_filter: Optional[Dict[str, Any]] = None,
        max_depth: int = 6,
    ) -> Dict[str, Any]:
        """
        Try once; if 400-too-many-cells, recursively split bbox and merge.
        """
        # Default AOI: Sweden-ish (tune if you want)
        bb = bbox or BBox(left=10.0, bottom=55.0, right=25.0, top=69.6)

        base_filter: Dict[str, Any] = {}
        if extra_filter:
            base_filter.update(extra_filter)

        def _try(bb_try: BBox, depth: int) -> Dict[str, Any]:
            f = dict(base_filter)
            f.update(_bbox_filter(bb_try))

            url = self.base_url.rstrip("/") + "/Observations/GeoGridAggregation"
            params = {
                "zoom": str(zoom),
                "validateSearchFilter": "false",
                "translationCultureCode": "sv-SE",
                "sensitiveObservations": "false",
                "skipCache": "false",
            }
            body: Dict[str, Any] = {"taxon": {"ids": taxon_ids, "includeUnderlyingTaxa": False}}
            body.update(f)

            resp = requests.post(url, headers=self._headers(), params=params, json=body, timeout=self.timeout_s)
            if resp.status_code == 200:
                return resp.json()

            if _is_too_many_cells_error(resp) and depth < max_depth:
                parts = _split_bbox(bb_try)
                sub_payloads = [_try(p, depth + 1) for p in parts]
                return _merge_gridcells(sub_payloads)

            raise RuntimeError(f"GeoGridAggregation failed: HTTP {resp.status_code} – {resp.text[:500]}")

        return _try(bb, 0)


    def geogrid_aggregation_resilient(
        self,
        taxon_ids: List[int],
        zoom: int,
        *,
        extra_filter: Optional[Dict[str, Any]] = None,
        max_depth: int = 12,
    ) -> Dict[str, Any]:
        """
        Retry GeoGridAggregation by recursively splitting bbox when SOS returns
        "number of cells too large" (HTTP 400 with that message).

        If caller provides extra_filter with geographics.boundingBox already,
        we split that bbox. Otherwise we start with a default world bbox.
        """
        # Default bbox: WebMercator-ish latitude clamp, full longitude span
        world = BBox(top_lat=85.0, left_lon=-180.0, bottom_lat=-85.0, right_lon=180.0)

        # If caller already provided a bbox filter, extract it
        bb = world
        if extra_filter:
            geo = extra_filter.get("geographics") or {}
            bbf = geo.get("boundingBox") or {}
            tl = bbf.get("topLeft") or {}
            br = bbf.get("bottomRight") or {}
            if "latitude" in tl and "longitude" in tl and "latitude" in br and "longitude" in br:
                bb = BBox(
                    top_lat=float(tl["latitude"]),
                    left_lon=float(tl["longitude"]),
                    bottom_lat=float(br["latitude"]),
                    right_lon=float(br["longitude"]),
                )

        def _try(bb_local: BBox, depth: int) -> Dict[str, Any]:
            filt = _make_bbox_filter(bb_local)
            if extra_filter:
                # merge extra_filter “on top”, but keep geographics.boundingBox we set
                merged_filter = dict(extra_filter)
                merged_filter["geographics"] = dict(extra_filter.get("geographics") or {})
                merged_filter["geographics"]["boundingBox"] = filt["geographics"]["boundingBox"]
                filt = merged_filter

            try:
                return self.geogrid_aggregation(
                    taxon_ids,
                    zoom=zoom,
                    extra_filter=filt,
                )
            except RuntimeError as e:
                msg = str(e).lower()
                too_big = (
                    "number of cells that can be returned is too large" in msg
                    or "limit is 65535 cells" in msg
                )
                if (not too_big) or depth >= max_depth:
                    raise

                parts = _split_bbox_4(bb_local)
                sub_payloads = [_try(p, depth + 1) for p in parts]
                return _merge_geogrid_payloads(sub_payloads)

        return _try(bb, 0)

    
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
