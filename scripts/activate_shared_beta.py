"""Activate the completed isolated beta exercise. Does not touch production."""
import json
import os
import sqlite3
from pathlib import Path

ROOT=Path('/proj/boston/beta/wcr')
DIRECTORY=ROOT/'stage/shared-exercise-20261031'
state=json.loads((DIRECTORY/'import-status.json').read_text())
assert state['phase']=='done' and not state['busy'], 'Import is not complete'
database=DIRECTORY/'shared.sqlite'
with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as db:
    assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
    expected=db.execute('SELECT count(DISTINCT taxon_id) FROM list_taxa').fetchone()[0]
    covered=db.execute("SELECT count(DISTINCT taxon_id) FROM coverage WHERE date_from='2000-01-01' AND date_to='2026-10-31' AND source_scope='public-only-start-v1'").fetchone()[0]
    assert covered==expected==1298, (covered,expected)
    assert db.execute('SELECT count(*) FROM taxon_cells').fetchone()[0]>0
env_path=ROOT/'ops/geomap-env.json'
env=json.loads(env_path.read_text())
env.update(GEOMAP_SHARED_DB=str(database),GEOMAP_DB=str(database),
    GEOMAP_MISSING_SPECIES_CSV=str(ROOT/'stage/lists/defaults/redlisted-lepidoptera/redlisted_lepidoptera.csv'),
    GEOMAP_DEFAULT_LISTS_DIR=str(ROOT/'stage/lists/defaults'))
env.pop('ARTDATABANKEN_AUTHORIZATION',None)
os.umask(0o077)
temporary=env_path.with_suffix('.json.tmp')
temporary.write_text(json.dumps(env,indent=2));temporary.replace(env_path)
launcher=ROOT/'ops/beta_geomap.py'
text=launcher.read_text()
start=text.index('sys.argv = [')
end=text.index('runpy.run_path',start)
text=text[:start]+'''sys.argv = [str(ROOT/'geomap/server/app.py'), '--host', '127.0.0.1', '--port', '8089',
    '--logs-dir', str(ROOT/'stage/logs')]
'''+text[end:]
launcher.write_text(text)
print('Beta shared store configured; personal observations token removed from beta runtime')
