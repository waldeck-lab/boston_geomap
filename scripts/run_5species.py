#!/usr/bin/env python3

# script:run_5species.py 

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


def _get_arg(name: str, default: str | None = None) -> str | None:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)

    n = int(_get_arg("--n", "5"))
    slot_id = int(_get_arg("--slot", "0"))

    alpha = float(_get_arg("--alpha", str(cfg.hotmap_alpha)))
    beta = float(_get_arg("--beta", str(cfg.hotmap_beta)))

    python = sys.executable

    # Always pass slot through the whole chain
    run([python, str(REPO_ROOT / "scripts" / "fetch_layers.py"),
         "--n", str(n),
         "--slot", str(slot_id)])

    run([python, str(REPO_ROOT / "scripts" / "build_hotmap.py"),
         "--n", str(n),
         "--slot", str(slot_id),
         "--alpha", str(alpha),
         "--beta", str(beta)])

    run([python, str(REPO_ROOT / "scripts" / "export_hotmap.py"),
         "--slot", str(slot_id)])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

