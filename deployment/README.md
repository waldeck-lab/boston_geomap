# Beta launchers

These are source-controlled copies of the active launchers under
`/proj/boston/beta/wcr/ops`. Runtime secrets remain in the private
`ops/geomap-env.json` and are intentionally not copied into Git.

The gateway serves the beta WCR dist, sends GeoMap requests to 8089 and only
allows selected read/auth operations to the existing WCR authority on 5000.
The GeoMap launcher clears inherited settings and reads explicit beta paths.

See `docs/SHARED_BETA.md` for the data model, import scope and recovery.
