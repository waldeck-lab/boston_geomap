#!/usr/bin/env python3

# script:run_geomap_pipeline.py 

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

import subprocess
import sys
import argparse
import os

from pathlib import Path

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.config import Config
from geomap.config import SLOT_MIN, SLOT_MAX, SLOT_ALL
from geomap.logging_utils import setup_logger

from geomap.cli_paths import apply_path_overrides

ZOOM_DEFAULT=15 # 1200 m, see zoom levels below

DEFAULT_N = 5 # Default limit of number of species 

# Zoom definitions
# Source: https://api-portal.artdatabanken.se/api-details#api=sos-api-v1&operation=Observations_GeogridAggregation
# Zoom	WGS84		Web Mercator*	SWEREF99TM(S)	SWEREF99TM(N)
# 1	180		20000km		8000km		12000km
# 2	90		10000km		4000km		6000km
# 3	45		5000km		2000km		3000km
# 4	22.5		2500km		1000km		1500km
# 5	11.25		1250km		500km		750km
# 6	5.625		600km		250km		360km
# 7	2.8125		300km		120km		180km
# 8	1.406250	150km		60km		90km
# 9	0.703125	80km		30km		45km
# 10	0.351563	40km		15km		23km
# 11	0.175781	20km		8km		11km
# 12	0.087891	10km		4km		6km
# 13	0.043945	5km		2km		3km
# 14	0.021973	2500m		1000m		1400m
# 15	0.010986	1200m		500m		700m
# 16	0.005493	600m		240m		350m
# 17	0.002747	300m		120m		180m
# 18	0.001373	150m		60m		90m
# 19	0.000687	80m		30m		45m
# 20	0.000343	40m		15m		22m
# 21	0.000172	19m		7m		11m
#
# *) The script pipeline is using the Web Mercator zoom levels



def _default_stage_paths(repo_root: Path) -> tuple[Path, Path]:
    """
    Return (db_dir, lists_dir) defaults.
    Prefer OVE stage if available, else fall back to repo-local data dirs.
    """
    ove_base = os.getenv("OVE_BASE_DIR")
    if ove_base:
        base = Path(ove_base)
        return base / "stage" / "db", base / "stage" / "lists"

    # non-OVE fallback (keeps script usable outside OVE)
    return repo_root / "data" / "db", repo_root / "data" / "lists"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the full geomap pipeline.")
    ap.add_argument("--n", type=int, default=DEFAULT_N, help="Number of species to use (0 = all).")
    ap.add_argument("--slot", type=int, default=SLOT_ALL, help=f"Calendar slot id: {SLOT_ALL}=all-time, 1..{SLOT_MAX}=time buckets")
    ap.add_argument("--alpha", type=float, default=None, help="Override hotmap alpha.")
    ap.add_argument("--beta", type=float, default=None, help="Override hotmap beta.")
    ap.add_argument("--zooms", "--zoom", dest="zooms", default="15",
                    help="Comma-separated zoom levels (e.g. 14,15,16).")

    db_def, lists_def = _default_stage_paths(REPO_ROOT)
    ap.add_argument("--db-dir", default=str(db_def), help=f"DB dir (default: {db_def})")
    ap.add_argument("--lists-dir", default=str(lists_def), help=f"Lists dir (default: {lists_def})")
    ap.add_argument("--out-dir", default=None, help=f"stage/lists/geomap (output drop)") 
    ap.add_argument("--cache-dir", default=None, help=f"local cache for Artdatabanken fetches")
    ap.add_argument("--logs-dir", default=None, help=f"override log-dir")
    return ap.parse_args(argv)

def _parse_zooms(arg: str) -> list[int]:
    zs = []
    for part in (arg or "").split(","):
        part = part.strip()
        if not part:
            continue
        zs.append(int(part))
    if not zs:
        raise ValueError("empty --zooms")
    return sorted(set(zs), reverse=True)

def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> int:
    args = parse_args()

    apply_path_overrides(
        db_dir=args.db_dir,
        lists_dir=args.lists_dir,
        geomap_lists_dir=args.out_dir,
        cache_dir=args.cache_dir,
        logs_dir=args.logs_dir,
    )
    
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("run_geomap_pipeline", cfg.logs_dir)

    n = int(args.n)
    slot_id = int(args.slot)
    if slot_id == SLOT_ALL:
        logger.info("Slot: %d (all-time aggregate)", slot_id)
    else:
        logger.info("Slot: %d (calendar bucket 1..%d)", slot_id, SLOT_MAX)
    if slot_id < SLOT_MIN or slot_id > SLOT_MAX:
        logger.error(
            "slot_id out of range: %d (valid: %d..%d, where %d = all-time)",
            slot_id, SLOT_MIN, SLOT_MAX, SLOT_ALL
        )
        return 2

    alpha = float(args.alpha if args.alpha is not None else cfg.hotmap_alpha)
    beta = float(args.beta if args.beta is not None else cfg.hotmap_beta)

    zooms = _parse_zooms(args.zooms)

    # Normalize optional dirs (args.* may be None)
    db_dir = Path(args.db_dir).expanduser().resolve() if args.db_dir else None
    lists_dir = Path(args.lists_dir).expanduser().resolve() if args.lists_dir else None
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else None
    cache_dir = Path(args.cache_dir).expanduser().resolve() if args.cache_dir else None
    logs_dir = Path(args.logs_dir).expanduser().resolve() if args.logs_dir else None

    logger.info("Running pipeline: n=%d slot=%d zooms=%s", n, slot_id, zooms)
    logger.info("Using db_dir=%s lists_dir=%s out_dir=%s", db_dir, lists_dir, out_dir)

    python = sys.executable

    # Only path overrides here (safe to reuse everywhere)
    common_paths: list[str] = []
    if db_dir:
        common_paths += ["--db-dir", str(db_dir)]
    if lists_dir:
        common_paths += ["--lists-dir", str(lists_dir)]
    if out_dir:
        common_paths += ["--out-dir", str(out_dir)]
    if cache_dir:
        common_paths += ["--cache-dir", str(cache_dir)]
    if logs_dir:
        common_paths += ["--logs-dir", str(logs_dir)]

    run([python, str(REPO_ROOT / "scripts" / "fetch_layers.py"),
         "--n", str(n),
         "--zooms", ",".join(str(z) for z in zooms),
         "--slot", str(slot_id),
         *common_paths])
    
    for zoom in zooms:
        run([python, str(REPO_ROOT / "scripts" / "build_hotmap.py"),
             "--n", str(n),
             "--zoom", str(zoom),
             "--slot", str(slot_id),
             "--alpha", str(alpha),
             "--beta", str(beta),
             *common_paths])

        run([python, str(REPO_ROOT / "scripts" / "export_hotmap.py"),
             "--zoom", str(zoom),
             "--slot", str(slot_id),
             *common_paths])

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

