from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Config:
    repo_root: Path

    base_url: str = "https://api.artdatabanken.se/species-observation-system/v1"
    api_version: str = os.getenv("ARTDATABANKEN_X_API_VERSION", "1.5").strip()

    subscription_key: str = os.getenv("ARTDATABANKEN_SUBSCRIPTION_KEY", "").strip()
    authorization: str = os.getenv("ARTDATABANKEN_AUTHORIZATION", "").strip()

    zoom: int = int(os.getenv("GEOMAP_ZOOM", "15"))

    missing_species_csv: Path = None  # type: ignore[assignment]
    observed_db_path: Path = None     # type: ignore[assignment]
    dyntaxa_db_path: Path = None      # type: ignore[assignment]

    geomap_db_path: Path = None       # type: ignore[assignment]
    cache_dir: Path = None            # type: ignore[assignment]
    logs_dir: Path = None             # type: ignore[assignment]

    def __post_init__(self) -> None:
        def _p(env_key: str, default_rel: Path) -> Path:
            raw = os.getenv(env_key, str(default_rel))
            return Path(raw).expanduser().resolve()

        object.__setattr__(
            self,
            "missing_species_csv",
            _p(
                "GEOMAP_MISSING_SPECIES_CSV",
                self.repo_root / ".." / "boston_sos_observatory" / "data" / "out" / "missing_species.csv",
            ),
        )
        object.__setattr__(
            self,
            "observed_db_path",
            _p(
                "GEOMAP_OBSERVED_DB",
                self.repo_root / ".." / "boston_sos_observatory" / "data" / "db" / "sos_counts.sqlite",
            ),
        )
        object.__setattr__(
            self,
            "dyntaxa_db_path",
            _p(
                "GEOMAP_DYNTAXA_DB",
                self.repo_root / ".." / "boston_viewer" / "data" / "db" / "dyntaxa_lepidoptera.sqlite",
            ),
        )
        object.__setattr__(self, "geomap_db_path", _p("GEOMAP_DB", self.repo_root / "data" / "db" / "geomap.sqlite"))
        object.__setattr__(
            self, "cache_dir", _p("GEOMAP_CACHE_DIR", self.repo_root / "data" / "cache" / "geogrid")
        )
        object.__setattr__(self, "logs_dir", _p("GEOMAP_LOGS_DIR", self.repo_root / "logs"))
