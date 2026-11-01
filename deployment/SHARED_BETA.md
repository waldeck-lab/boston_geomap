
## Personal missing-list CSV upload
The beta controls list public presets and only the authenticated owner's private lists.
POST /geomap-api/lists/from-observed-csv accepts a multipart CSV file, title,
and save=false (preview) or save=true. The server determines owner_id from WCR
introspection; client owner fields are ignored. CSV input is capped at 2 MiB,
supports UTF-8/BOM or Windows-1252 and comma, semicolon or tab.
A recognized Dyntaxa taxon-id header is mandatory. Unknown or ambiguous IDs
reject the whole request. Accepted infraspecific taxa map to their parent species.
scripts/build_species_catalogue.py derives the comparison universe from the
stored complete-run taxon response: 3217 accepted taxonomic Lepidoptera species.
This describes the Dyntaxa snapshot, not a separately verified native-Swedish checklist.

Saving creates a UUID list with private visibility and normalized observed/missing
CSV files plus provenance beneath the shared DB's user-lists directory.
Original uploads and extra personal columns are not retained. No personal SOS token
is needed or stored. Lists reference shared taxon IDs; no observations or cells
are inserted. The response and list catalog expose incomplete import coverage
relative to this beta exercise's 2000-01-01 through 2026-10-31 scope.
GET /geomap-api/lists/<id>/csv authorizes the owner for private lists, including
against other administrators, and returns no-store. Other users receive 404.
All existing legacy writes remain blocked. SOS token/OAuth and geodata generation
from the controls are not yet implemented.

### Artportalen name exports
TaxonList.csv's headerless five-column format is supported: Swedish name;
scientific name; author; redlist category; observation count. Only membership
in the exported list is used, not the observation count.
Named ScientificName / SwedishName columns are supported too.
Names use exact Unicode-normalized, case-insensitive Dyntaxa name/synonym lookup.
Intersecting scientific and Swedish matches can resolve an otherwise ambiguous
historical synonym. A name shared with a historical group requires confirmation
from the second name column. No fuzzy matching or automatic guesses are made.
Known genus/family/collective-taxon rows are excluded with a preview count.
Unknown, conflicting or ambiguous names reject the upload and return row details.
The source CSV remains unretained. Tests run only in temporary beta fixture DBs.

## Independent beta accounts
Beta registration/login/me/logout/availability are served by the beta gateway,
using wcr/backend/src/app/beta_accounts.py. stage/auth/accounts.sqlite contains
only a snapshot of the six WCR auth tables, preserving existing account IDs for
private-list ownership. No dissection/highlight content is copied. Production
was opened read-only. Runtime settings in ops/auth-settings.json contain a fresh
beta-only session secret and internal secret; do not commit this file.
The host-only Secure/HttpOnly/SameSite=Lax cookie is __Host-wcr_beta_session.
Users must sign in again after activation. Existing copied password hashes work.
New registrations join only the users group, can never request admin rights, and
remain exclusive to beta. Username normalization is NFKC plus casefold, enforced
by a unique DB index. Registration rechecks uniqueness transactionally.
The Guest menu first opens password confirmation, then submits registration
when both passwords match (8–256 characters). Registration signs in immediately.
No email confirmation or password-reset UI is implemented.
Availability and auth attempts have database-backed per-client-IP limits.
Neither request passwords nor confirmation values are logged by frontend API helpers.
GeoMap validates the same beta cookie locally against the shared auth module and
reads current users/groups from the isolated auth DB. This avoids an HTTP call
back into the gateway while its workers proxy GeoMap requests.
Production WCR content is fetched anonymously: the gateway strips Cookie and
Authorization for production reads, preventing old beta production-auth cookies
from carrying over into another beta identity. Production writes stay blocked.
prepare_beta_auth.py refuses to replace an existing store or settings file.
Snapshot ops/snapshots/before-beta-auth-* has the old gateway, launcher and env
for rollback. Restoring only code does not erase the isolated account DB.
Before promotion, account IDs and list ownership need an explicit migration plan;
do not merge two independently allocated numeric user-ID spaces blindly.

## Selected-cell species popup
Authenticated beta users get a map-anchored, independently scrolling species list.
It shares the same selected-cell taxa response as the retained controls list.
A different grid cell replaces the selection; the same x/y/grid-zoom toggles it off,
clearing observation points and x/y from the bookmark while retaining list_id.
The close button does the same. Grid-zoom changes clear an incompatible selection.
Public SOS observation points are available to authenticated users (not only admins);
private-list ownership is still checked before all reads. Anonymous callers get 403.
Exact point results are never cached. The response reports truncated=true when its
2,000 grouped-location cap is exceeded, and the popup explains that limit.
Old fetches are aborted and checked again before committing results to React state.
The selected popup uses a React portal inside MapLibre, preserving its scroll
position on updates. MapLibre hover labels are suppressed while the list is open.
User list titles/taxon names remain unchanged. New popup text supports sv/en.

## Species documentation register
Independent SQLite catalogue: stage/documentation/documentation.sqlite.
The builder scripts/build_documentation.py reads the accepted-species Dyntaxa
snapshot and the cached NRM index, storing all 3217 species and a row per provider.
Artfakta links use https://artfakta.se/taxa/<Dyntaxa-ID>/information; this route
was verified in the rendered Artfakta application. NRM links are taken from its
actual species index, including scientific-name aliases and updated Latin
gender endings that differ from historical filenames. No fuzzy matching is used.
The initial build matched 3175 NRM pages; 42 species have no confirmed indexed
match and retain an explicit not_indexed status and a null URL.
Provider rows store match method/name and source provenance; metadata stores
taxonomy/index hashes and retrieval/build times. Page contents are not copied.
NRM's provided legacy service is reachable via HTTP; HTTPS was unavailable.

The builder defaults to the saved index. --refresh-index refreshes it. It builds
a separate temporary DB, validates quick_check and minimum index size, then
atomically replaces the catalogue. Read API caches include the catalogue file
revision so updates appear without rebuilding observation data or restarting.
Do not edit live documentation rows as a workflow: preserve changes in matching
inputs or add a reviewed override mechanism before introducing manual corrections.

GEOMAP_DOCUMENTATION_DB points only to this beta-stage database.
GET /geomap-api/documentation/<taxon_id> returns a species and its references.
GET /geomap-api/documentation?taxon_ids=... returns a batch (up to 2000 IDs).
Both are public read-only taxonomy/reference metadata, unrelated to user lists.
Cell taxa responses are enriched by a batched read from the same catalogue.
Private list authorization still runs before those taxa can be returned.
Only recognized provider hosts and species URL patterns are exposed.

The shared SpeciesReferences component displays nrm/artfakta next to each
scientific name in the selected-cell popup and retained controls list. Anchors
use target=_blank and rel=noopener noreferrer; clicking does not toggle the
cell. Missing references are disabled with a translated explanation.
Frontend tests: npm run test:documentation; existing i18n and geomap tests remain.
Database backups/release packaging should include stage/documentation and its
source snapshots alongside the observation store; source code does not embed URLs
for individual taxa, authentication details or SQLite database binaries.

## Email account identity and recovery
The beta gateway now uses email-keyed accounts and optional editable display
names. Existing numeric account IDs and private list ownership are retained.
Legacy usernames work until their owner binds and verifies an email. New users
must verify their mailbox before a full session is issued. A temporary recovery
password gives only a restricted password-change session; GeoMap introspection
returns unauthenticated until the password has been replaced and the user signs
in normally. Completing a reset revokes older sessions.

Migration, private SMTP configuration and rollback are documented in the WCR
checkout at backend/BETA_ACCOUNTS.md. The migration script is versioned there as
backend/scripts/migrate_beta_email_accounts.py. Neither SMTP secrets nor the
accounts database are tracked. Missing SMTP credentials disable mail actions but
preserve legacy login. The identity integration test covers private list denial
during reset and retained ownership after recovery.
