
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
