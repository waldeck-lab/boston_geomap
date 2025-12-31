# boston_geomap

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

Build and explore biodiversity “hotmaps” from Artportalen / Artdatabanken SOS GeoGridAggregation data.

This repo:
1) fetches per-taxon geogrid layers from SOS for a **base zoom**  
2) materializes **derived zooms locally** (aggregate child tiles into parent tiles)  
3) builds a **hotmap** (coverage + score per tile)  
4) exports **GeoJSON + CSV** suitable for GUI overlays and ranking nearby hotspots

> Works with **time slots** (`slot_id`) to model seasonal occurrence patterns.

---

## What you get

- **SQLite cache** of per-taxon grid tiles (`taxon_grid`) keyed by `(taxon_id, zoom, slot_id, x, y)`
- **Hotmap tiles** (`grid_hotmap`) per `(zoom, slot_id)` with bbox + score
- **Exports**:
  - `data/out/hotmap_zoom{zoom}_slot{slot}.geojson`
  - `data/out/top_sites_zoom{zoom}_slot{slot}.csv`
- **Tools**:
  - end-to-end pipeline runner
  - `rank_nearby.py` distance-weighted ranking
  - cache/derived cleanup helper

---

## Quick start

### 1) Create and activate a venv
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

### 2) Configure secrets

Create settings.env (not committed) and export it:
set -a
source settings.env
set +a

Expected env vars:

ARTDATABANKEN_SUBSCRIPTION_KEY

ARTDATABANKEN_AUTHORIZATION

### 3) Run pipeline

Example: 100 taxa, slot 0, build zooms 15→14→13, export each:

./scripts/run_geomap_pipeline.py --n 100 --slot 0 --zooms 15,14,13

### Concepts
Time slots (slot_id)

A slot is a coarse “calendar bin” used to model seasonal patterns.
You can use geomap/timeslots.py to map dates to slots (or later hook GUI date selection to slot selection).

## Zooms

We fetch only the highest zoom (smallest tiles) from SOS and aggregate locally for lower zooms.

## Why?
It reduces API calls, keeps the most precise geometry at the base layer, and ensures zoomed-out views remain consistent.


### How local zoom aggregation works (tile math)

## If you fetch zoom Z and want zoom Z-1:
o parent_x = floor(child_x / 2)
o parent_y = floor(child_y / 2)
o parent bbox is the union of the four children’s bboxes (or computed via tile bounds)

## For each (taxon_id, slot_id) we aggregate:
o observations_count = sum(child observations)
o taxa_count = sum or max (depending on SOS semantics; currently stored but not used for scoring)
o bbox = union of child bboxes

### Scoring

## Hotmap is computed per tile as:
o coverage = number of distinct taxa present in the tile (within the active taxa set)
o obs_total = summed observations across that taxa set (via view)
o score = (coverage ** alpha) / ((obs_total + 1) ** beta)
## Tune via:
o --alpha
o --beta

### Project layout
geomap/              core library modules (storage, SOS client, scoring, tiles)
scripts/             CLI scripts (fetch, build, export, rank, pipeline)
data/db/             SQLite DB (local cache) [gitignored]
data/out/            exports (GeoJSON/CSV) [gitignored]
logs/                run logs [gitignored]
settings.env         local secrets [gitignored]

### Common commands
## Fetch + materialize zooms
./scripts/fetch_layers.py --n 100 --slot 0 --zooms 15,14,13

## Build hotmap for one zoom
./scripts/build_hotmap.py --n 100 --slot 0 --zoom 14 --alpha 2 --beta 0.5

## Export for one zoom
./scripts/export_hotmap.py --zoom 14 --slot 0

## Rank nearby hotspots
./scripts/rank_nearby.py --slot 0 --zoom 14 --lat 55.667 --lon 13.35 --max-km 70 --limit 10 --show-all-taxa

## Clean derived outputs safely
./scripts/clean_derived.py --all

### Status

## This is an experimental tooling repo. The schema and scoring are evolving as we add:
o calendar-aware layers
o GUI integration
o richer metadata for taxa and occurrence windows


---

