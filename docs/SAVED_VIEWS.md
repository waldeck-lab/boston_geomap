## Private saved map views and corner navigation (beta)

The controls panel has a Saved map views section for signed-in users. Save
records a named snapshot; open restores it, and delete removes only that
snapshot. Saving again creates a new view, without changing an older one.
Maximum 100 views per account, with titles of at most 120 characters.

The snapshot includes list ID, camera position/zoom/bearing/pitch, background map,
grid zoom, years, seasonal center/radius/window, species color thresholds,
phenology visibility and optional selected cell. It contains no observations,
passwords, email addresses or CSV contents. Camera zoom and grid detail are
separate settings and both are restored. Initial automatic map fitting is
suppressed when opening a view so it cannot override the saved camera. Automatic
fitting is disabled on restore; the user can enable it again in the controls.
Changing interface language preserves the current camera.

The metadata table `saved_map_views` is created idempotently in the independent
beta shared store at startup. Its index starts with the authenticated numeric
owner ID. GET/POST /geomap-api/views list/create only the caller's views;
GET/DELETE /geomap-api/views/<id> require that same owner. Admins have no ownership
bypass. The referenced list must be visible to that user both when saving and
opening. If a list disappears, the view remains listed as unavailable and can
still be deleted. No observation records or geoboxes are duplicated or modified.

Mutation requests check origin, size, value ranges and owner quota. Supplied
owner IDs are ignored. Signed-out and restricted password-reset sessions cannot
use any view endpoint. Public cell links omit the private view ID; private list
authorization is unchanged. Include the metadata table in shared-store backups.
Older code safely ignores the additive table when rolling beta code back.

On the selected cell, four inside plus buttons choose NW/NE/SE/SW children and
the vertex minus buttons choose its common parent. Both grid detail and map
camera advance or retreat one level. No network import or geobox build starts.
Child coordinates are (2x+dx,2y+dy,z+1); parent coordinates are
(floor(x/2),floor(y/2),z-1). Buttons unavailable at grid zoom 1/21 are omitted.
For cells smaller than 100 screen pixels, a single focus button first enlarges
the cell to avoid eight overlapping targets. The outline/controls follow camera
movement and rotation, and remain usable for an empty child cell. The same-cell
click/close behavior for species and observation points is preserved.

Tests: backend test_saved_views.py, test_beta_identity_integration.py and the
existing shared-store suite; frontend test/navigation.test.mjs and the existing
account/i18n/cell/documentation tests. Integration checks real beta session
cookies, cross-user access, and retained view ownership after password recovery.
