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


from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

from geomap.config import Config

def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)
    if "--alpha" in sys.argv:
        cfg.hotmap_alpha = float(sys.argv[sys.argv.index("--alpha") + 1])
    if "--beta" in sys.argv:
        cfg.hotmap_beta = float(sys.argv[sys.argv.index("--beta") + 1])


    python = sys.executable

    run([python, str(REPO_ROOT / "scripts" / "fetch_layers.py"), "--n", "5"])
    run([python, str(REPO_ROOT / "scripts" / "build_hotmap.py"), "--n", "5"])
    run([python, str(REPO_ROOT / "scripts" / "export_hotmap.py")])
    return 0


def main() -> int:
    cfg = Config(repo_root=REPO_ROOT)

    alpha = cfg.hotmap_alpha
    beta = cfg.hotmap_beta
    if "--alpha" in sys.argv:
        alpha = float(sys.argv[sys.argv.index("--alpha") + 1])
    if "--beta" in sys.argv:
        beta = float(sys.argv[sys.argv.index("--beta") + 1])

    python = sys.executable
    # pass args through to the scripts that actually compute/rebuild
    run([python, str(REPO_ROOT / "scripts" / "fetch_layers.py"), "--n", "5"])
    run([python, str(REPO_ROOT / "scripts" / "build_hotmap.py"), "--n", "5","--alpha", str(alpha), "--beta", str(beta)])
    run([python, str(REPO_ROOT / "scripts" / "export_hotmap.py")])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
