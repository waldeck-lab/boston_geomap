"""Independent beta store: public occurrences once, lists reference taxa."""
import csv
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

BASE_ZOOM = 21
SCHEMA = '''
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS lists (
 id TEXT PRIMARY KEY, title TEXT NOT NULL, owner_id INTEGER,
 visibility TEXT NOT NULL CHECK(visibility IN ('public','private')),
 content_hash TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
 CHECK(visibility='public' OR owner_id IS NOT NULL));
CREATE TABLE IF NOT EXISTS taxa (taxon_id INTEGER PRIMARY KEY,
 scientific_name TEXT NOT NULL DEFAULT '', swedish_name TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS list_taxa (
 list_id TEXT REFERENCES lists(id) ON DELETE CASCADE,
 taxon_id INTEGER REFERENCES taxa(taxon_id), PRIMARY KEY(list_id,taxon_id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS list_taxa_reverse ON list_taxa(taxon_id,list_id);
CREATE TABLE IF NOT EXISTS observations (
 id INTEGER PRIMARY KEY, occurrence_id TEXT NOT NULL UNIQUE,
 taxon_id INTEGER NOT NULL REFERENCES taxa(taxon_id),
 observation_date TEXT NOT NULL, modification_date TEXT,
 year INTEGER NOT NULL, slot_id INTEGER NOT NULL,
 latitude REAL NOT NULL, longitude REAL NOT NULL,
 tile_x INTEGER NOT NULL, tile_y INTEGER NOT NULL,
 individual_count INTEGER, source_scope TEXT NOT NULL DEFAULT 'public');
CREATE INDEX IF NOT EXISTS observations_taxon ON observations(taxon_id,year,slot_id,tile_x,tile_y);
CREATE TABLE IF NOT EXISTS taxon_cells (
 taxon_id INTEGER NOT NULL REFERENCES taxa(taxon_id), year INTEGER NOT NULL,
 slot_id INTEGER NOT NULL, x INTEGER NOT NULL, y INTEGER NOT NULL,
 n INTEGER NOT NULL, PRIMARY KEY(taxon_id,year,slot_id,x,y)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS coverage (
 taxon_id INTEGER NOT NULL REFERENCES taxa(taxon_id),
 date_from TEXT NOT NULL,date_to TEXT NOT NULL,source_scope TEXT NOT NULL,
 completed_at TEXT DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(taxon_id,date_from,date_to,source_scope)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT OR IGNORE INTO metadata VALUES ('revision','0');
'''


@contextmanager
def connect(path, readonly=False):
    path = Path(path)
    if readonly:
        conn = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=30)
    else:
        conn = sqlite3.connect(path, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        if not readonly:
            conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with connect(path) as db:
        db.executescript(SCHEMA)


def register_list(path, list_id, title, csv_path, owner_id=None):
    with Path(csv_path).open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream))
    ids = sorted({int(float(r['taxon_id'])) for r in rows})
    if not ids or any(x <= 0 for x in ids):
        raise ValueError('Invalid or empty list')
    digest = hashlib.sha256(json.dumps(ids).encode()).hexdigest()
    with connect(path) as db:
        db.execute('INSERT INTO lists(id,title,owner_id,visibility,content_hash) VALUES(?,?,?,?,?)',
                   (list_id,title,owner_id,'private' if owner_id is not None else 'public',digest))
        for row in rows:
            tid=int(float(row['taxon_id']))
            db.execute('INSERT INTO taxa VALUES(?,?,?) ON CONFLICT(taxon_id) DO NOTHING',
                       (tid,row.get('scientific_name',''),row.get('swedish_name','')))
        db.executemany('INSERT INTO list_taxa VALUES(?,?)',[(list_id,t) for t in ids])
    return ids


def apply_rows(db, rows):
    from datetime import date
    from geomap.tiles import lonlat_to_tile_xy
    written=0
    for row in rows:
        tid=row.get('DyntaxaTaxonId')
        occurrence=str(row.get('OccurrenceId') or '').strip()
        stamp=str(row.get('StartDate') or '')[:10]
        status=str(row.get('OccurrenceStatus') or '').strip().lower()
        if status and status != 'present':
            continue
        try:
            tid=int(tid);d=date.fromisoformat(stamp)
            lat=float(row['DecimalLatitude']);lon=float(row['DecimalLongitude'])
            if not(-85.05112878<=lat<=85.05112878 and -180<=lon<=180) or not occurrence:
                continue
            x,y=lonlat_to_tile_xy(lon,lat,BASE_ZOOM)
            slot=(d.month-1)*4+(1 if d.day<=7 else 2 if d.day<=15 else 3 if d.day<=23 else 4)
        except (ValueError,TypeError,KeyError):
            continue
        count=row.get('IndividualCount','')
        try:count=int(float(count)) if count else None
        except ValueError:count=None
        db.execute('''INSERT INTO observations(occurrence_id,taxon_id,observation_date,
         modification_date,year,slot_id,latitude,longitude,tile_x,tile_y,individual_count)
         VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(occurrence_id) DO UPDATE SET
         taxon_id=excluded.taxon_id,observation_date=excluded.observation_date,
         modification_date=excluded.modification_date,year=excluded.year,slot_id=excluded.slot_id,
         latitude=excluded.latitude,longitude=excluded.longitude,tile_x=excluded.tile_x,
         tile_y=excluded.tile_y,individual_count=excluded.individual_count''',
         (occurrence,tid,stamp,row.get('Modified') or row.get('ModificationDate'),d.year,slot,lat,lon,x,y,count))
        written+=1
    return written


def rebuild_cells(db):
    db.execute('DELETE FROM taxon_cells')
    db.execute('''INSERT INTO taxon_cells SELECT taxon_id,year,slot_id,tile_x,tile_y,COUNT(*)
                  FROM observations GROUP BY taxon_id,year,slot_id,tile_x,tile_y''')
    db.execute("UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")


def list_stats(db, list_id):
    return dict(db.execute('''SELECT count(*) AS observations_count,
       count(DISTINCT o.taxon_id) AS observed_taxa FROM observations o
       JOIN list_taxa l ON l.taxon_id=o.taxon_id WHERE l.list_id=?''',(list_id,)).fetchone())
