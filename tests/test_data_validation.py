"""Catch consistent-looking totals that hide incorrect map cells and list state."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from geomap.data_validation import Validator
from geomap.shared_store import initialize, connect, rebuild_cells


class DataValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.store=self.root/"shared.sqlite"
        initialize(self.store)
        catalogue={"species":[{"taxon_id":1,"scientific_name":"Example species"}],
                   "species_mapping":{"1":1},"source_sha256":"catalogue-hash","retrieved_at":"2026-11-10T00:00:00Z"}
        (self.root/"lepidoptera-species.json").write_text(json.dumps(catalogue))
        self.settings={"GEOMAP_DEFAULT_LISTS_DIR":str(self.root),"GEOMAP_SHARED_DB":str(self.store)}
        with connect(self.store) as db:
            db.execute("INSERT INTO taxa VALUES(1,'Example species','')")
            db.execute("INSERT INTO lists VALUES('fixture','Private fixture',2,'private',?,CURRENT_TIMESTAMP)",
                       (hashlib.sha256(json.dumps([1]).encode()).hexdigest(),))
            db.execute("INSERT INTO list_taxa VALUES('fixture',1)")
            db.executemany("""INSERT INTO observations(occurrence_id,taxon_id,observation_date,year,slot_id,
                latitude,longitude,tile_x,tile_y) VALUES(?,1,'2026-01-01',2026,1,55,13,?,2)""",
                [("urn:fixture:1",1),("urn:fixture:2",2)])
            rebuild_cells(db)

    def tearDown(self):
        self.tmp.cleanup()

    def test_detects_wrong_cells_even_when_totals_match_and_changes_nothing(self):
        with Validator(self.settings) as validator:
            self.assertEqual(validator.validate("cells")["status"],"passed")
        with connect(self.store) as db:
            db.execute("UPDATE taxon_cells SET n=2 WHERE x=1")
            db.execute("DELETE FROM taxon_cells WHERE x=2")
        before=self.store.read_bytes()
        with Validator(self.settings) as validator:
            result=validator.validate("cells")
            checks={row["code"]:row for row in result["checks"]}
            self.assertEqual(checks["cell_totals"]["status"],"passed")
            self.assertEqual(checks["cell_observation_match"]["status"],"failed")
            with self.assertRaises(sqlite3.OperationalError):
                validator.db("GEOMAP_SHARED_DB").execute("DELETE FROM lists")
        self.assertEqual(self.store.read_bytes(),before)

    def test_detects_invalid_list_fingerprint_and_missing_owner(self):
        accounts=self.root/"accounts.sqlite"
        with sqlite3.connect(accounts) as db:
            db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
        settings=self.root/"accounts.json"
        settings.write_text(json.dumps({"database":str(accounts)}))
        self.settings["WCR_BETA_AUTH_SETTINGS"]=str(settings)
        with connect(self.store) as db:
            db.execute("UPDATE lists SET content_hash='wrong'")
        with Validator(self.settings) as validator:
            result=validator.validate("lists")
        checks={row["code"]:row for row in result["checks"]}
        self.assertEqual(checks["list_content_hashes"]["status"],"failed")
        self.assertEqual(checks["list_owners"]["status"],"failed")
        self.assertNotIn("Private fixture",json.dumps(result))
        self.assertNotIn("\"fixture\"",json.dumps(result))

    def test_collective_taxon_detail_keeps_original_id_and_is_owner_scoped(self):
        source={"taxa":[{
            "taxonId":2,"category":{"value":"CollectiveTaxon"},"type":{"value":"NonTaxonomic"},
            "names":[{"name":"Example species s.lat.","isRecommended":True,"category":{"value":"ScientificName"}}],
        }]}
        source_path=self.root/"redlisted-lepidoptera/source/complete-run.json"
        source_path.parent.mkdir(parents=True)
        source_path.write_text(json.dumps(source))
        path=self.root/"lepidoptera-species.json"
        catalogue=json.loads(path.read_text())
        catalogue["source_sha256"]=hashlib.sha256(source_path.read_bytes()).hexdigest()
        path.write_text(json.dumps(catalogue))
        with connect(self.store) as db:
            db.execute("INSERT INTO taxa VALUES(2,'Example species','')")
            db.execute("INSERT INTO list_taxa VALUES('fixture',2)")
        with Validator(self.settings) as validator:
            result=validator.validate("lists")
        check=next(row for row in result["checks"] if row["code"]=="list_taxa")
        self.assertEqual(check["details"],[])
        detail=check["private_details"]["2"]["details"][0]
        self.assertEqual(detail["taxon_id"],2)
        self.assertEqual(detail["category"],"CollectiveTaxon")
        self.assertEqual(detail["scientific_name"],"Example species s.lat.")
        self.assertEqual(detail["stored_scientific_name"],"Example species")

    def test_missing_database_is_not_created(self):
        missing=self.root/"missing.sqlite"
        self.settings["GEOMAP_SHARED_DB"]=str(missing)
        with Validator(self.settings) as validator:
            with self.assertRaises(sqlite3.OperationalError):
                validator.validate("observations")
        self.assertFalse(missing.exists())


if __name__=="__main__":
    unittest.main()
