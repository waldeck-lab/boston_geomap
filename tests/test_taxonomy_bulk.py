"""The whole SQL catalogue update must be audited, stale-safe and atomic."""
import json
import unittest
from unittest.mock import patch
import test_taxonomy_history as fixture
from geomap.taxonomy_history import history_db,apply_taxon
from geomap.taxonomy_bulk import prepare,apply

class BulkTests(unittest.TestCase):
    setUp=fixture.TaxonomyHistoryTests.setUp
    tearDown=fixture.TaxonomyHistoryTests.tearDown
    taxon=fixture.TaxonomyHistoryTests.taxon
    source=fixture.TaxonomyHistoryTests.source

    def test_all_changes_and_receipt_commit_together_and_retry_is_idempotent(self):
        with history_db(self.path) as db:
            plan=prepare(db,self.two)
            self.assertEqual(plan["counts"],dict(insert=1,update=1,deactivate=0))
            first=apply(db,self.two,plan,"bulk",7)
            self.assertEqual(apply(db,self.two,plan,"bulk",7),first)
            self.assertEqual(db.execute("SELECT local_index FROM taxa WHERE taxon_id=1").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_history_events WHERE phase='local'").fetchone()[0],2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_update_receipts").fetchone()[0],2)
            self.assertEqual(prepare(db,self.two)["total"],0)

    def test_partial_write_failure_rolls_back_every_taxon_and_history(self):
        with history_db(self.path) as db:plan=prepare(db,self.two)
        calls=[]
        def fail_second(db,**kwargs):
            calls.append(kwargs["taxon_id"])
            if len(calls)==2:raise RuntimeError("fixture")
            return apply_taxon(db,**kwargs)
        with self.assertRaises(RuntimeError):
            with history_db(self.path) as db:
                with patch("geomap.taxonomy_bulk.apply_taxon",side_effect=fail_second):
                    apply(db,self.two,plan,"failed",7)
        with history_db(self.path) as db:
            self.assertEqual(db.execute("SELECT sci_name FROM taxa WHERE taxon_id=1").fetchone()[0],"Old name")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxa").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_history_events").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM taxonomy_update_receipts").fetchone()[0],0)

    def test_stale_sql_and_modified_plan_are_rejected(self):
        with history_db(self.path) as db:plan=prepare(db,self.two)
        with history_db(self.path) as db:
            invalid={**plan,"changes":[]}
            with self.assertRaises(ValueError):apply(db,self.two,invalid,"invalid",7)
            db.execute("UPDATE taxa SET swe_name='Changed' WHERE taxon_id=1")
            with self.assertRaisesRegex(ValueError,"Local SQL changed"):
                apply(db,self.two,plan,"stale",7)

    def test_removed_species_is_deactivated_and_retained(self):
        source=self.source("new.json",[self.taxon(2,"Other")],0)
        with history_db(self.path) as db:
            plan=prepare(db,source)
            self.assertEqual(plan["counts"]["deactivate"],1)
            apply(db,source,plan,"remove",7)
            self.assertEqual(db.execute("SELECT is_active FROM taxa WHERE taxon_id=1").fetchone()[0],0)
