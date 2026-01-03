# geomap:cli_paths.py

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

import os
from pathlib import Path
from typing import Optional


def _as_dir(p: str, *, create: bool = False, name: str = "path") -> Path:
    pp = Path(p).expanduser().resolve()
    if create:
        pp.mkdir(parents=True, exist_ok=True)
    # If not creating, allow non-existing (some pipelines set paths before producing them)
    # but reject if it exists and is not a directory.
    if pp.exists() and not pp.is_dir():
        raise ValueError(f"{name} exists but is not a directory: {pp}")
    return pp


def apply_path_overrides(
    *,
    db_dir: Optional[str] = None,
    lists_dir: Optional[str] = None,         # input lists root (missing_species.csv)
    geomap_lists_dir: Optional[str] = None,  # output drop area root
    cache_dir: Optional[str] = None,
    logs_dir: Optional[str] = None,
    create_dirs: bool = False,
) -> None:
    """
    Map directory overrides into GEOMAP_* env vars that Config reads.

    create_dirs=False by default (safe for pipelines that only configure paths).
    Set create_dirs=True in "run" wrappers to ensure drop dirs exist.
    """
    if db_dir:
        p = _as_dir(db_dir, create=create_dirs, name="db_dir")
        os.environ["GEOMAP_DB"] = str(p / "geomap.sqlite")

        # Only apply these conventions if caller hasn't explicitly configured them
        os.environ.setdefault("GEOMAP_OBSERVED_DB", str(p / "sos_counts.sqlite"))
        os.environ.setdefault("GEOMAP_DYNTAXA_DB", str(p / "dyntaxa_lepidoptera.sqlite"))

    if lists_dir:
        p = _as_dir(lists_dir, create=create_dirs, name="lists_dir")
        os.environ["GEOMAP_MISSING_SPECIES_CSV"] = str(p / "missing_species.csv")

    if geomap_lists_dir:
        p = _as_dir(geomap_lists_dir, create=create_dirs, name="geomap_lists_dir")
        os.environ["GEOMAP_LISTS_DIR"] = str(p)
        # Only keep this if you still use geomap_out_dir / GEOMAP_OUT_DIR
        os.environ.setdefault("GEOMAP_OUT_DIR", str(p))

    if cache_dir:
        p = _as_dir(cache_dir, create=create_dirs, name="cache_dir")
        os.environ["GEOMAP_CACHE_DIR"] = str(p)

    if logs_dir:
        p = _as_dir(logs_dir, create=create_dirs, name="logs_dir")
        os.environ["GEOMAP_LOGS_DIR"] = str(p)
