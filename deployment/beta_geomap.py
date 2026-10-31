"""Launch GeoMap with explicit beta paths and no production path fallback."""
import json
import os
import runpy
import sys
from pathlib import Path

ROOT = Path('/proj/boston/beta/wcr')
env = json.loads((ROOT/'ops/geomap-env.json').read_text())
os.environ.clear()
os.environ.update(env)
sys.path.insert(0, str(ROOT/'geomap'))
sys.path.insert(0, str(ROOT/'geomap/server'))
os.chdir(ROOT/'geomap')
sys.argv = [str(ROOT/'geomap/server/app.py'), '--host', '127.0.0.1', '--port', '8089',
    '--logs-dir', str(ROOT/'stage/logs')]
runpy.run_path(sys.argv[0], run_name='__main__')
