"""Taxonomy snapshots and local row updates must preserve an atomic, durable audit."""
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from geomap.taxonomy_history import history_db,record_source,apply_taxon,encode


class TaxonomyHistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.path=self.root/"taxa.sqlite"
        self.first=self.taxon(1,"Old name")
        with sqlite3.connect(self.path) as db:
            db.executescript("""CREATE TABLE taxa(taxon_id INTEGER PRIMARY KEY,local_index INTEGER UNIQUE NOT NULL,
                sci_name TEXT,swe_name TEXT,category TEXT,type TEXT,status TEXT,parent_id INTEGER,
                is_active INTEGER NOT NULL,sha256 TEXT,updated_at INTEGER NOT NULL,raw_json TEXT);
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                INSERT INTO meta VALUES('next_local_index','1');""")
            db.execute("INSERT INTO taxa VALUES(1,0,'Old name','','Species','Taxonomic','Accepted',3000188,1,'old',1,?)",(encode(self.first),))
        self.one=self.source("one.json",[self.first],-60)
        self.two=self.source("two.json",[self.taxon(1,"New name"),self.taxon(2,"Another species")],0)

    def tearDown(self):
        self.tmp.cleanup()

    def taxon(self,tid,name):
        return dict(taxonId=tid,parentId=3000188,secondaryParents=[],category={"value":"Species"},
            type={"value":"Taxonomic"},status={"value":"Accepted"},
            names=[dict(name=name,isRecommended=True,category={"value":"ScientificName"})])

    def source(self,name,taxa,seconds):
        path=self.root/name
        path.write_text(json.dumps(dict(root_taxon_id=3000188,child_ids=[t["taxonId"] for t in taxa],taxa=taxa,
            retrieved_at=(datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat())))
        return path

    def test_source_change_and_duplicate_snapshot_are_distinct_from_local_changes(self):
        with history_db(self.path) as db:
            record_source(db,self.one)
            record_source(db,self.two)
            record_source(db,self.two)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_sources").fetchone()[0],2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_history_events WHERE phase='source'").fetchone()[0],2)
            event=db.execute("SELECT * FROM taxonomy_history_events WHERE taxon_id=1").fetchone()
            self.assertEqual(json.loads(event["before_json"])["scientific_name"],"Old name")
            self.assertEqual(json.loads(event["after_json"])["scientific_name"],"New name")
            self.assertIn("scientific_name",json.loads(event["fields_json"]))
            self.assertEqual(db.execute("SELECT sci_name FROM taxa WHERE taxon_id=1").fetchone()[0],"Old name")

    def test_update_is_idempotent_and_external_sql_is_logged_without_actor(self):
        with history_db(self.path) as db:
            first=apply_taxon(db,source_path=self.two,taxon_id=1,job_id="one",actor_id=7)
            again=apply_taxon(db,source_path=self.two,taxon_id=1,job_id="one",actor_id=7)
            self.assertEqual(first,again)
            self.assertEqual(first["outcome"],"local_changed")
            checked=apply_taxon(db,source_path=self.two,taxon_id=1,job_id="second-check",actor_id=7)
            self.assertEqual(checked["outcome"],"unchanged")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_write_context").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_history_events WHERE phase='local'").fetchone()[0],1)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE taxa SET swe_name='Changed outside admin' WHERE taxon_id=1")
        with history_db(self.path) as db:
            last=db.execute("SELECT * FROM taxonomy_history_events ORDER BY id DESC LIMIT 1").fetchone()
            self.assertIsNone(last["actor_id"])
            self.assertIsNone(last["job_id"])
            self.assertIn("swedish_name",json.loads(last["fields_json"]))

    def test_rollback_undoes_taxon_change_receipt_and_audit(self):
        with self.assertRaises(RuntimeError):
            with history_db(self.path) as db:
                apply_taxon(db,source_path=self.two,taxon_id=1,job_id="one",actor_id=7)
                raise RuntimeError("fixture")
        with history_db(self.path) as db:
            self.assertEqual(db.execute("SELECT sci_name FROM taxa WHERE taxon_id=1").fetchone()[0],"Old name")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_update_receipts").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_history_events").fetchone()[0],0)

    def test_new_species_gets_stable_index_and_collective_taxon_becomes_inactive(self):
        collective=self.taxon(1,"Old name s.lat.")
        collective.update(category={"value":"CollectiveTaxon"},type={"value":"NonTaxonomic"})
        source=self.source("collective.json",[collective,self.taxon(2,"Another species")],0)
        with history_db(self.path) as db:
            receipt=apply_taxon(db,source_path=source,taxon_id=1,job_id="old",actor_id=7)
            self.assertEqual(receipt["outcome"],"local_deactivated")
            self.assertEqual(tuple(db.execute("SELECT is_active,category,local_index FROM taxa WHERE taxon_id=1").fetchone()),(0,"CollectiveTaxon",0))
            receipt=apply_taxon(db,source_path=source,taxon_id=2,job_id="new",actor_id=7)
            self.assertEqual(receipt["outcome"],"local_inserted")
            self.assertEqual(tuple(db.execute("SELECT local_index,is_active FROM taxa WHERE taxon_id=2").fetchone()),(1,1))


if __name__=="__main__":
    unittest.main()
