# Independent shared-data beta

The experiment is separate from both production and the original beta copy.
It imports public SOS observations for 2000-01-01 through 2026-10-31, first for
the red-listed Lepidoptera list, then for Jonas's saved missing-species CSV.
No personal Authorization token or observedByMe filter is used. WCR remains
the login authority; account, dissection and highlight data are not cloned.

## Storage

- `lists`: immutable seed identity, owner, visibility and content hash.
- `list_taxa`: species membership; the two lists overlap in 297 species.
- `observations`: one row per globally unique SOS occurrence ID, public scope.
- `taxon_cells`: counts by species, year, seasonal period and base tile at z21.
- `coverage`: completed taxon/date/scope imports, reused across lists.
- `metadata.revision`: invalidates bounded in-memory query caches after rebuild.

Lower zooms group base tiles by integer shift. Species coverage is counted
distinct across seasons and years, while observation counts are summed.
Exact points and observed dates come from canonical observation rows.
No full raw observation row is copied for each list or zoom.
Identical list contents may share cached read results, but ownership is checked
before every cache lookup. This cache accepts public source observations only;
future protected variants must use a separate store or scope-aware identity and
aggregation keys, never overwrite the public coordinates for the same source ID.

## Import

`scripts/import_shared_beta.py` runs as a separate, single-writer job with a
file lock. It checks counts, splits large taxon/date scopes below the 10,000-row
search window, follows API retry instructions, validates result completeness
and checkpoints successful scopes. Public search avoids the stricter CSV-export
rate limit encountered during the first batches.
Search pages contain at most 1,000 records, are ordered by occurrence ID, and
use at most three concurrent readers under one shared rate/backoff limiter.
Date partitions use OnlyStartDate so multi-day events cannot disappear across
partition boundaries. Coverage scope versions separate this corrected filter
from preliminary exports that used BetweenStartDateAndEndDate.
Coverage includes successful zero-result scopes. Received observations without
usable coordinates/dates or with absence status are excluded from map storage.
Initial public CSV exports and later compressed map-only JSON rows remain in
the private experiment directory for audit; new JSON caches omit reporter names.
The importer is a beta seed/import tool, not yet a general refresh/sync service:
upstream deletions and later source revisions need a future reconciliation job.

## API and UI

`GEOMAP_SHARED_DB` enables `server/shared_api.py`. It intercepts map reads and
blocks the legacy write APIs, preventing accidental modification of the old
demo database. Public default: `redlisted-lepidoptera`. Private: `missing-jonas`,
visible only to its WCR account owner. Exact points require an administrator.

- `/geomap-api/lists`: public and own list identities.
- Existing map/cell endpoints accept `list_id`.
- `/geomap-api/jobs/status`: sanitized import phase, no private-list counts.
- `/geomap?list_id=missing-jonas`: temporary URL selection, requires login.

There is deliberately no list popup/menu yet. List creation, validation UI,
general import controls, history, deletion/grace-period GC and nearby ranking
are future work. Legacy admin import/build actions return a clear 409 while
this store is active. The production app and v1.0.1 tag are unchanged.

## Recovery

Original beta DB remains `stage/db/geomap.sqlite`. The launcher and configuration
are saved under `ops/*.before-shared-store`. To revert, restore those two files
and restart only `wcr-beta-geomap.service`. This restores the old demo read mode.
Production baseline is in `backups/pre-beta-20261031T174553`.

## Completed exercise (2026-10-31)

Active database: `stage/shared-exercise-20261031/shared.sqlite`.
Range: 2000-01-01 through 2026-10-31. Rödlistan is the default; the saved
missing-species list is private to WCR user id 2 (Jonas).

- Red list: 565 species; 504,181 public mappable observations in 524 species.
- Personal list: 1,030 species; 174,409 observations in 675 species.
- Shared membership: 297 species, 144,688 observations.
- Canonical union: 533,902 observations; second list adds only 29,721 rows.
- Base cells: 460,986; logical SQLite allocation: 122,802,176 bytes.
- Integrity, foreign keys, unique source IDs and live map totals verified.
- Fourteen tests passed, including ownership and shared-cache authorization.
- The active beta environment no longer contains a personal Artportalen token.

Public map: `https://beta.waldeck.se/geomap`.
Own map (login required): `https://beta.waldeck.se/geomap?list_id=missing-jonas`.
No popup/menu was added. Original demo beta database and production remain intact.
