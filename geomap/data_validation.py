"""Local read-only validation of the configured GeoMap data dependencies."""
from contextlib import ExitStack, closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from geomap.documentation import valid_reference_url

DATASETS = ("taxonomy", "lists", "observations", "cells", "documentation")


def scalar(db, sql, args=()):
    return db.execute(sql, args).fetchone()[0]


class Validator:
    def __init__(self, settings):
        self.settings = settings
        self.stack = ExitStack()
        self.databases = {}
        self.integrity = {}
        self.catalogue_bytes = (Path(settings["GEOMAP_DEFAULT_LISTS_DIR"])/"lepidoptera-species.json").read_bytes()
        self.catalogue = json.loads(self.catalogue_bytes)
        self.species = {int(row["taxon_id"]) for row in self.catalogue["species"]}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stack.close()

    def db(self, key):
        path = Path(self.settings[key]).resolve()
        if path not in self.databases:
            db = self.stack.enter_context(closing(sqlite3.connect(path.as_uri()+"?mode=ro", uri=True, timeout=5)))
            deadline = time.monotonic()+120
            db.set_progress_handler(lambda: int(time.monotonic()>deadline), 10000)
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            self.integrity[path] = (
                [row[0] for row in db.execute("PRAGMA quick_check")] == ["ok"],
                len(db.execute("PRAGMA foreign_key_check").fetchall()),
            )
            self.databases[path] = db
        return self.databases[path]

    def base(self, dataset, key):
        db = self.db(key)
        integrity, foreign_keys = self.integrity[Path(self.settings[key]).resolve()]
        return db, {
            "id":dataset, "checks":[
                {"code":"sqlite_integrity", "status":"passed" if integrity else "failed", "count":0 if integrity else 1},
                {"code":"foreign_keys", "status":"passed" if foreign_keys==0 else "failed", "count":foreign_keys},
            ], "metrics":{},
        }

    @staticmethod
    def check(result, code, count, severity="failed"):
        result["checks"].append({"code":code, "status":severity if count else "passed", "count":count})

    def taxonomy(self):
        db, result = self.base("taxonomy", "GEOMAP_DYNTAXA_DB")
        ids = {row[0] for row in db.execute("SELECT taxon_id FROM taxa WHERE is_active=1")}
        run = db.execute("SELECT MAX(finished_at) FROM runs").fetchone()[0]
        rows = self.catalogue["species"]
        self.check(result,"catalogue_unique_ids",len(rows)-len(self.species))
        self.check(result,"catalogue_names",sum(not row.get("scientific_name") for row in rows))
        mapping = self.catalogue["species_mapping"]
        self.check(result,"catalogue_mapping",sum(int(tid) not in self.species for tid in mapping.values()))
        self.check(result,"catalogue_alignment",len(ids ^ self.species),"warning")
        source = Path(self.settings["GEOMAP_DEFAULT_LISTS_DIR"])/"redlisted-lepidoptera/source/complete-run.json"
        self.check(result,"catalogue_source_hash",int(hashlib.sha256(source.read_bytes()).hexdigest()!=self.catalogue["source_sha256"]))
        result["metrics"] = dict(catalogue_species=len(self.species), sqlite_species=len(ids),
                                 csv_only=len(self.species-ids), sqlite_only=len(ids-self.species))
        result["source"] = dict(retrieved_at=self.catalogue["retrieved_at"], sqlite_run_at=run,
                                hash=self.catalogue["source_sha256"])
        return result

    def lists(self):
        db, result = self.base("lists", "GEOMAP_SHARED_DB")
        rows = db.execute("SELECT id,visibility,owner_id,content_hash FROM lists").fetchall()
        invalid_hashes = outside = bad_owners = private = 0
        accounts_path = self.settings.get("WCR_BETA_AUTH_SETTINGS")
        owners = None
        if accounts_path:
            account_settings = json.loads(Path(accounts_path).read_text())
            with closing(sqlite3.connect(Path(account_settings["database"]).resolve().as_uri()+"?mode=ro", uri=True)) as accounts:
                owners = {row[0] for row in accounts.execute("SELECT id FROM users")}
        for list_id, visibility, owner, digest in rows:
            ids = [row[0] for row in db.execute("SELECT taxon_id FROM list_taxa WHERE list_id=? ORDER BY taxon_id",(list_id,))]
            invalid_hashes += hashlib.sha256(json.dumps(ids).encode()).hexdigest()!=digest
            outside += len(set(ids)-self.species)
            private += visibility=="private"
            bad_owners += visibility=="private" and (owner is None or (owners is not None and owner not in owners))
        self.check(result,"list_content_hashes",invalid_hashes)
        self.check(result,"list_taxa",outside,"warning")
        self.check(result,"list_owners",bad_owners)
        if owners is None:
            result["checks"].append({"code":"owner_registry","status":"unavailable"})
        result["metrics"] = dict(lists=len(rows), private_lists=private, out_of_scope_entries=outside)
        result["source"] = dict(hash=self.catalogue["source_sha256"])
        return result

    def observations(self):
        db, result = self.base("observations", "GEOMAP_SHARED_DB")
        total, unique, missing = db.execute("""SELECT COUNT(*),COUNT(DISTINCT occurrence_id),
            COALESCE(SUM(occurrence_id IS NULL OR trim(occurrence_id)=''),0) FROM observations""").fetchone()
        self.check(result,"occurrence_ids",total-unique+missing)
        self.check(result,"observation_coordinates",scalar(db,"""SELECT COUNT(*) FROM observations
            WHERE latitude NOT BETWEEN -85.05112878 AND 85.05112878 OR longitude NOT BETWEEN -180 AND 180
            OR latitude IS NULL OR longitude IS NULL"""))
        self.check(result,"observation_dates",scalar(db,"""SELECT COUNT(*) FROM observations
            WHERE date(observation_date) IS NULL OR date(observation_date,'+0 days')!=observation_date
            OR year != CAST(strftime('%Y',observation_date) AS INTEGER)
            OR slot_id != (CAST(strftime('%m',observation_date) AS INTEGER)-1)*4+
                CASE WHEN CAST(strftime('%d',observation_date) AS INTEGER)<=7 THEN 1
                     WHEN CAST(strftime('%d',observation_date) AS INTEGER)<=15 THEN 2
                     WHEN CAST(strftime('%d',observation_date) AS INTEGER)<=23 THEN 3 ELSE 4 END"""))
        self.check(result,"observation_tiles",scalar(db,"""SELECT COUNT(*) FROM observations
            WHERE tile_x<0 OR tile_y<0 OR tile_x>=2097152 OR tile_y>=2097152"""))
        self.check(result,"coverage_intervals",scalar(db,"""SELECT COUNT(*) FROM coverage
            WHERE date(date_from) IS NULL OR date(date_to) IS NULL OR date_from>date_to"""))
        taxa = {row[0] for row in db.execute("SELECT DISTINCT taxon_id FROM observations")}
        self.check(result,"observation_taxa",len(taxa-self.species),"warning")
        covered = db.execute("""SELECT MIN(date_from),MAX(date_to),MAX(completed_at) FROM coverage
            WHERE source_scope='public-only-start-v1'""").fetchone()
        result["metrics"] = dict(observations=total, taxa=len(taxa), coverage_scopes=scalar(db,"SELECT COUNT(*) FROM coverage"))
        result["source"] = dict(coverage_from=covered[0], coverage_to=covered[1], last_completed_scope_at=covered[2],
            revision=scalar(db,"SELECT value FROM metadata WHERE key='revision'"))
        return result

    def cells(self):
        db, result = self.base("cells", "GEOMAP_SHARED_DB")
        mismatch = scalar(db,"""WITH expected AS (
            SELECT taxon_id,year,slot_id,tile_x AS x,tile_y AS y,COUNT(*) AS n
            FROM observations GROUP BY taxon_id,year,slot_id,tile_x,tile_y
        ), missing_or_changed AS (
            SELECT * FROM expected EXCEPT SELECT * FROM taxon_cells
        ), unexpected_or_changed AS (
            SELECT * FROM taxon_cells EXCEPT SELECT * FROM expected
        ) SELECT (SELECT COUNT(*) FROM missing_or_changed)+(SELECT COUNT(*) FROM unexpected_or_changed)""")
        self.check(result,"cell_observation_match",mismatch)
        total = scalar(db,"SELECT COUNT(*) FROM observations")
        summed = scalar(db,"SELECT COALESCE(SUM(n),0) FROM taxon_cells")
        self.check(result,"cell_totals",abs(total-summed))
        result["metrics"] = dict(cells=scalar(db,"SELECT COUNT(*) FROM taxon_cells"), observations=total, cell_observations=summed)
        result["source"] = dict(revision=scalar(db,"SELECT value FROM metadata WHERE key='revision'"))
        return result

    def documentation(self):
        db, result = self.base("documentation", "GEOMAP_DOCUMENTATION_DB")
        metadata = {key:json.loads(value) for key,value in db.execute("SELECT key,value FROM metadata")}
        documented = {row[0] for row in db.execute("SELECT taxon_id FROM species")}
        self.check(result,"documentation_catalogue",len(documented ^ self.species),"warning")
        self.check(result,"documentation_source_hash",int(metadata.get("catalogue_sha256")!=hashlib.sha256(self.catalogue_bytes).hexdigest()),"warning")
        invalid = missing_nrm = missing_artfakta = links = 0
        for source, url, status in db.execute("SELECT source,url,status FROM documentation"):
            if url:
                links += 1
                invalid += not valid_reference_url(source,url)
            if source=="nrm" and not url:
                missing_nrm += 1
        missing_artfakta = scalar(db,"""SELECT COUNT(*) FROM species s WHERE NOT EXISTS (
            SELECT 1 FROM documentation d WHERE d.taxon_id=s.taxon_id AND source='artfakta' AND url IS NOT NULL)""")
        self.check(result,"documentation_urls",invalid)
        self.check(result,"documentation_artfakta",missing_artfakta,"warning")
        self.check(result,"documentation_nrm",missing_nrm,"warning")
        result["metrics"] = dict(species=len(documented), links=links, missing_nrm=missing_nrm)
        result["source"] = {key:metadata.get(key) for key in ("built_at","nrm_retrieved_at","catalogue_sha256")}
        return result

    def validate(self, dataset):
        result = getattr(self, dataset)()
        statuses = {check["status"] for check in result["checks"]}
        result["status"] = next((status for status in ("failed","unavailable","warning") if status in statuses),"passed")
        return result
