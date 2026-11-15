"""A fresh observed CSV replaces one owned list version without touching observations."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from geomap.shared_store import initialize,connect
from geomap.observed_csv import build_difference,save_difference
from geomap.list_regeneration import preview,activate,RegenerationConflict

class RegenerationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name);self.db=self.root/"shared.sqlite";initialize(self.db)
        self.catalogue=self.root/"catalogue.json"
        self.source=dict(retrieved_at="2026-11-15",source_sha256="fixture",species=[
            dict(taxon_id=i,scientific_name=f"Species {i}",swedish_name="") for i in (1,2,3)],species_mapping={str(i):i for i in (1,2,3)})
        self.catalogue.write_text(json.dumps(self.source))
        rows,obs,summary=build_difference(b"TaxonId\n1\n",self.catalogue)
        self.item=save_difference(self.db,2,"My list",rows,obs,summary)
        with connect(self.db) as db:
            db.execute("INSERT INTO observations(occurrence_id,taxon_id,observation_date,year,slot_id,latitude,longitude,tile_x,tile_y) VALUES('fixture',2,'2026-01-01',2026,1,55,13,1,1)")

    def tearDown(self):self.temp.cleanup()

    def candidate(self,raw=b"TaxonId\n2\n"):
        rows,obs,summary=build_difference(raw,self.catalogue)
        with connect(self.db,readonly=True) as db:
            plan=preview(db,self.item["id"],2,rows,summary)
        return rows,obs,summary,plan

    def test_replaces_membership_with_stable_identity_and_durable_history(self):
        rows,obs,summary,plan=self.candidate()
        self.assertEqual(plan["added"],[1]);self.assertEqual(plan["removed"],[2])
        result=activate(self.db,self.item["id"],2,rows,obs,summary,plan["token"])
        self.assertEqual(result["id"],self.item["id"])
        with connect(self.db,readonly=True) as db:
            self.assertEqual([r[0] for r in db.execute("SELECT taxon_id FROM list_taxa ORDER BY taxon_id")],[1,3])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0],1)
            receipt=db.execute("SELECT * FROM list_regenerations").fetchone()
            self.assertEqual(json.loads(receipt["before_json"]),[2,3])
            self.assertEqual(json.loads(receipt["after_json"]),[1,3])
            self.assertEqual(db.execute("SELECT owner_id FROM lists").fetchone()[0],2)
            ids=[1,3]
            self.assertEqual(db.execute("SELECT content_hash FROM lists").fetchone()[0],hashlib.sha256(json.dumps(ids).encode()).hexdigest())

    def test_owner_and_stale_file_or_catalogue_preview_are_rejected(self):
        rows,obs,summary,plan=self.candidate()
        with self.assertRaises(PermissionError):activate(self.db,self.item["id"],3,rows,obs,summary,plan["token"])
        altered={**summary,"upload_sha256":"different"}
        with self.assertRaises(RegenerationConflict):activate(self.db,self.item["id"],2,rows,obs,altered,plan["token"])
        altered={**summary,"catalogue_sha256":"different"}
        with self.assertRaises(RegenerationConflict):activate(self.db,self.item["id"],2,rows,obs,altered,plan["token"])
        with connect(self.db,readonly=True) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_regenerations").fetchone()[0],0)

    def test_failed_receipt_rolls_back_list_and_revision(self):
        rows,obs,summary,plan=self.candidate()
        from geomap.list_regeneration import SCHEMA
        with connect(self.db) as db:
            db.executescript(SCHEMA)
            db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON list_regenerations BEGIN SELECT RAISE(ABORT,'fixture');END")
        with self.assertRaises(sqlite3.IntegrityError):activate(self.db,self.item["id"],2,rows,obs,summary,plan["token"])
        with connect(self.db,readonly=True) as db:
            self.assertEqual([r[0] for r in db.execute("SELECT taxon_id FROM list_taxa ORDER BY taxon_id")],[2,3])
            self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()[0],"0")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_regenerations").fetchone()[0],0)

    def test_large_change_requires_explicit_confirmation(self):
        rows,obs,summary,_=self.candidate()
        rows=[dict(taxon_id=i,scientific_name=f"Species {i}",swedish_name="") for i in range(100,160)]
        with connect(self.db,readonly=True) as db:
            plan=preview(db,self.item["id"],2,rows,summary)
        self.assertTrue(plan["large_change"])
        with self.assertRaises(RegenerationConflict):activate(self.db,self.item["id"],2,rows,obs,summary,plan["token"])
        result=activate(self.db,self.item["id"],2,rows,obs,summary,plan["token"],True)
        self.assertEqual(result["taxa_count"],60)
