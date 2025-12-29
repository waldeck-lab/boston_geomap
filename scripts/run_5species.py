#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# --- make repo root importable ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
# --------------------------------

def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> int:
    python = sys.executable

    run([python, str(REPO_ROOT / "scripts" / "fetch_layers.py"), "--n", "5"])
    run([python, str(REPO_ROOT / "scripts" / "build_hotmap.py"), "--n", "5"])
    run([python, str(REPO_ROOT / "scripts" / "export_hotmap.py")])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
