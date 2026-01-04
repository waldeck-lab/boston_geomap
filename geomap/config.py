# geomap:config.py

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
from pathlib import Path
import os

# Calendar slot semantics
SLOT_ALL = 0          # special slot meaning "all-time"
SLOT_MIN = 0
SLOT_MAX = 48         # 12 months × 4 buckets

@dataclass(frozen=True)
class Config:
    repo_root: Path
    base_url: str = "https://api.artdatabanken.se/species-observation-system/v1"
    api_version: str = os.getenv("ARTDATABANKEN_X_API_VERSION", "1.5").strip()
    subscription_key: str = os.getenv("ARTDATABANKEN_SUBSCRIPTION_KEY", "").strip()
    authorization: str = os.getenv("ARTDATABANKEN_AUTHORIZATION", "").strip()

    missing_species_csv: Path = None  # type: ignore[assignment]
    observed_db_path: Path = None     # type: ignore[assignment]
    dyntaxa_db_path: Path = None      # type: ignore[assignment]
    geomap_db_path: Path = None       # type: ignore[assignment]
    cache_dir: Path = None            # type: ignore[assignment]
    logs_dir: Path = None             # type: ignore[assignment]
    geomap_lists_dir: Path = None     # type: ignore[assignment]
    geomap_out_dir: Path = None       # type: ignore[assignment]

    hotmap_alpha: float = 2.0
    hotmap_beta: float = 0.5

    def __post_init__(self) -> None:
        def _p(env_key: str, default_path: Path) -> Path:
            raw = os.getenv(env_key, str(default_path))
            return Path(raw).expanduser().resolve()

        # Prefer explicit stage root from OVE, else infer from OVE_BASE_DIR's parent.

        stage_root = os.getenv("OVE_STAGE_DIR", "").strip()
        if stage_root:
            ove_stage = Path(stage_root).expanduser().resolve()
        else:
            ove_base = os.getenv("OVE_BASE_DIR", "").strip()
            ove_stage = (Path(ove_base).expanduser().resolve() / "stage") if ove_base else None

        if ove_stage and ove_stage.exists():
            default_missing_species = ove_stage / "lists" / "missing_species.csv"
            default_observed_db    = ove_stage / "db" / "sos_counts.sqlite"
            default_dyntaxa_db     = ove_stage / "db" / "dyntaxa_lepidoptera.sqlite"

            default_geomap_db      = ove_stage / "db" / "geomap.sqlite"
            default_cache_dir      = ove_stage / "cache" / "geomap"
            default_logs_dir       = ove_stage / "logs" / "geomap"
            default_geomap_lists   = ove_stage / "lists" / "geomap"
            default_geomap_out     = default_geomap_lists
        else:
            # Legacy / non-OVE fallbacks
            default_missing_species = self.repo_root / ".." / "boston_sos_observatory" / "data" / "out" / "missing_species.csv"
            default_observed_db     = self.repo_root / ".." / "boston_sos_observatory" / "data" / "db" / "sos_counts.sqlite"
            default_dyntaxa_db      = self.repo_root / ".." / "boston_viewer" / "data" / "db" / "dyntaxa_lepidoptera.sqlite"

            default_geomap_db       = self.repo_root / "data" / "db" / "geomap.sqlite"
            default_cache_dir       = self.repo_root / "data" / "cache" / "geogrid"
            default_logs_dir        = self.repo_root / "logs"
            default_geomap_lists    = self.repo_root / "data" / "out"
            default_geomap_out      = self.repo_root / "data" / "out"

        object.__setattr__(self, "missing_species_csv", _p("GEOMAP_MISSING_SPECIES_CSV", default_missing_species))
        object.__setattr__(self, "observed_db_path",     _p("GEOMAP_OBSERVED_DB",        default_observed_db))
        object.__setattr__(self, "dyntaxa_db_path",      _p("GEOMAP_DYNTAXA_DB",         default_dyntaxa_db))

        object.__setattr__(self, "geomap_db_path",       _p("GEOMAP_DB",                default_geomap_db))
        object.__setattr__(self, "cache_dir",            _p("GEOMAP_CACHE_DIR",         default_cache_dir))
        object.__setattr__(self, "logs_dir",             _p("GEOMAP_LOGS_DIR",          default_logs_dir))

        object.__setattr__(self, "geomap_lists_dir",     _p("GEOMAP_LISTS_DIR",         default_geomap_lists))
        object.__setattr__(self, "geomap_out_dir",       _p("GEOMAP_OUT_DIR",           default_geomap_out))
