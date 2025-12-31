# CONTRIBUTING.md

```md
# Contributing

## Dev setup
- Use Python 3.10+ (matches current environment)
- Prefer a venv (`python -m venv .venv`)
- Install deps: `pip install -r requirements.txt`

## Style
- Keep scripts in `scripts/` as thin wrappers calling `geomap/` functions.
- Keep SQL schema/view changes in `geomap/storage.py` only.
- Prefer deterministic output formats (stable ordering, explicit sorting).

## Safety / secrets
Never commit:
- `settings.env`
- subscription keys / authorization tokens
- `data/db/*` (SQLite)
- `data/out/*` exports

## PR checklist
- [ ] Updated README if behavior changes
- [ ] Ran pipeline at least once (`run_geomap_pipeline.py`)
- [ ] Confirmed schema migration works from clean DB
