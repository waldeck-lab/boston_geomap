# boston_geomap

Builds a visit "hotmap" over Sweden using SOS `Observations/GeoGridAggregation` for a set of missing species.

## Layout assumptions (default)
This repo sits next to your other repos in `~/repo/`:

- `../boston_sos_observatory/data/out/missing_species.csv`
- `../boston_sos_observatory/data/db/sos_counts.sqlite` (optional, for later)
- `../boston_viewer/data/db/dyntaxa_lepidoptera.sqlite` (optional, for later)

You can override any of these with env vars (see `.env.example`).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .