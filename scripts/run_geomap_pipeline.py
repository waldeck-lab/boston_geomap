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
from pathlib import Path

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.config import Config
from geomap.logging_utils import setup_logger

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

from geomap.config import Config


def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default

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
    cfg = Config(repo_root=REPO_ROOT)
    logger = setup_logger("run_geomap_pipeline", cfg.logs_dir)

    n = int(_get_arg("--n", str(DEFAULT_N)))
    if n == 0:
        logger.info("Using ALL species from CSV")
    else:
        logger.info("Using first %d species from CSV", n)

    slot_id = int(_get_arg("--slot", "0"))

    alpha = float(_get_arg("--alpha", str(cfg.hotmap_alpha)))
    beta = float(_get_arg("--beta", str(cfg.hotmap_beta)))

    zooms = _parse_zooms(_get_arg("--zooms", _get_arg("--zoom", "15")))
    logger.info("Running pipeline: n=%d slot=%d zooms=%s", n, slot_id, zooms)
    
    python = sys.executable

    # Always pass slot through the whole chain
    run([python, str(REPO_ROOT / "scripts" / "fetch_layers.py"),
         "--n", str(n),
         "--zooms", ",".join(str(z) for z in zooms),
         "--slot", str(slot_id)])

    # Re-run all pipeline steps per zoom-level
    for zoom in zooms:
        run([python, str(REPO_ROOT / "scripts" / "build_hotmap.py"),
             "--n", str(n),
             "--zoom", str(zoom),
             "--slot", str(slot_id),
             "--alpha", str(alpha),
             "--beta", str(beta)])

        run([python, str(REPO_ROOT / "scripts" / "export_hotmap.py"),
             "--zoom", str(zoom),
             "--slot", str(slot_id)])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

