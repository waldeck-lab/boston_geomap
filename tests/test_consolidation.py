"""Ensure scoped replacement preserves other sources/lists and is retry/stale safe."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from geomap.consolidation import (
    ARTPORTALEN_PREFIX, apply_plan, build_plan, complex_scope, fingerprint,
)
from geomap.shared_store import initialize, connect, apply_rows, rebuild_cells
from geomap.sos_reconciliation import PublicSOSReader

P=ARTPORTALEN_PREFIX


class ConsolidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/"store.sqlite"
        initialize(self.path)
        self.taxa={tid:dict(taxonId=tid,names=[dict(name="Species "+str(tid),isRecommended=True,
            category={"value":"ScientificName"})]) for tid in (1,2,9)}
        self.initial=[
            self.row("one",1),self.row("two",1),self.row("prune",1),
            self.row("incoming",9),self.row("unrelated",9),self.row("foreign",1),
        ]
        self.initial[-1]["OccurrenceId"]="urn:another-provider:foreign"
        with connect(self.path) as db:
            db.executemany("INSERT INTO taxa VALUES(?,?,?)",[(tid,"Species "+str(tid),"") for tid in self.taxa])
            db.execute("INSERT INTO lists VALUES('private','Private fixture',2,'private','unchanged',CURRENT_TIMESTAMP)")
            db.execute("INSERT INTO list_taxa VALUES('private',1)")
            apply_rows(db,self.initial)
            rebuild_cells(db)
            db.executemany("INSERT INTO coverage(taxon_id,date_from,date_to,source_scope) VALUES(?,?,?,?)",
                [(tid,"2000-01-01","2026-10-31","public-only-start-v1") for tid in (1,2,9)])

    def tearDown(self):
        self.tmp.cleanup()

    def row(self, oid, tid):
        return dict(OccurrenceId=P+oid,DyntaxaTaxonId=tid,StartDate="2026-01-10",
                    DecimalLatitude=55.0,DecimalLongitude=13.0,OccurrenceStatus="present",IndividualCount=1)

    def plan(self):
        rows={P+"one":self.row("one",2),P+"two":self.row("two",1),
              P+"new":self.row("new",2),P+"incoming":self.row("incoming",2)}
        with connect(self.path,readonly=True) as db:
            return build_plan(db,job_id="fixture",roots=[1],scope=[1,2],mapping={1:1,2:2},taxa=self.taxa,
                records=rows,excluded={P+"prune":{"reason":"not_publicly_returned"}},
                start="2000-01-01",end="2026-11-11",source_hash="fixture")

    def test_atomic_scoped_replacement_and_retry_preserve_other_sources_and_membership(self):
        plan=self.plan()
        self.assertEqual(plan["summary"]["pruned"],1)
        self.assertEqual(plan["summary"]["moved"],2)
        with connect(self.path) as db:
            before_lists=[tuple(row) for row in db.execute("SELECT * FROM lists")]
            receipt=apply_plan(db,plan)
            after=[tuple(row) for row in db.execute("SELECT occurrence_id,taxon_id FROM observations ORDER BY occurrence_id")]
            self.assertNotIn((P+"prune",1),after)
            self.assertIn((P+"one",2),after)
            self.assertIn(("urn:another-provider:foreign",1),after)
            self.assertIn((P+"unrelated",9),after)
            self.assertEqual([tuple(row) for row in db.execute("SELECT * FROM lists")],before_lists)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_taxa WHERE list_id='private' AND taxon_id=1").fetchone()[0],1)
            self.assertEqual(apply_plan(db,plan),receipt)
            self.assertEqual([tuple(row) for row in db.execute("SELECT occurrence_id,taxon_id FROM observations ORDER BY occurrence_id")],after)

    def test_stale_baseline_and_partial_failure_leave_data_unchanged(self):
        plan=self.plan()
        with connect(self.path) as db:
            db.execute("UPDATE observations SET latitude=56 WHERE occurrence_id=?",(P+"one",))
        with connect(self.path) as db:
            with self.assertRaisesRegex(ValueError,"baseline changed"):
                apply_plan(db,plan)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0],6)
        plan=self.plan()
        with connect(self.path) as db:
            with patch("geomap.consolidation.apply_rows",side_effect=RuntimeError("fixture")):
                with self.assertRaises(RuntimeError):
                    apply_plan(db,plan)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations WHERE occurrence_id=?",(P+"prune",)).fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0],6)

    def test_expired_or_changed_candidate_is_rejected(self):
        plan=self.plan()
        plan["generated_at"]="2000-01-01T00:00:00+00:00"
        plan["plan_hash"]=fingerprint({key:value for key,value in plan.items() if key!="plan_hash"})
        with connect(self.path) as db:
            with self.assertRaisesRegex(ValueError,"expired"):
                apply_plan(db,plan)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0],6)
        plan=self.plan()
        plan["prune_ids"].append(P+"unrelated")
        with connect(self.path) as db:
            with self.assertRaisesRegex(ValueError,"plan changed"):
                apply_plan(db,plan)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0],6)

    def test_complex_expands_secondary_children_and_subspecies_parent_without_renaming(self):
        taxa=[
          dict(taxonId=1,secondaryParents=[],category={"value":"CollectiveTaxon"}),
          dict(taxonId=2,parentId=9,secondaryParents=[1],category={"value":"Species"},status={"value":"Accepted"},type={"value":"Taxonomic"}),
          dict(taxonId=3,parentId=4,secondaryParents=[1],category={"value":"Subspecies"},status={"value":"Accepted"},type={"value":"Taxonomic"}),
          dict(taxonId=4,parentId=9,category={"value":"Species"},status={"value":"Accepted"},type={"value":"Taxonomic"}),
        ]
        scope,mapping,_=complex_scope(taxa,[1])
        self.assertEqual(scope,[1,2,3,4])
        self.assertEqual(mapping[3],4)
        self.assertNotIn(1,mapping)

    def test_changed_remote_snapshot_and_wrong_lookup_are_rejected(self):
        reader=PublicSOSReader("fixture")
        with patch.object(reader,"snapshot",side_effect=[({P+"one":self.row("one",1)},{}),({P+"one":self.row("one",2)},{})]):
            with self.assertRaisesRegex(ValueError,"changed"):
                reader.stable_snapshot([1],{},"2000-01-01","2026-11-11")
        record={"occurrence":{"occurrenceId":P+"other"},"taxon":{"id":1},"event":{"startDate":"2026-01-10"},
                "location":{"decimalLatitude":55,"decimalLongitude":13}}
        with patch.object(reader,"request",return_value=record):
            with self.assertRaisesRegex(ValueError,"another ID"):
                reader.lookup(P+"one",{},"2000-01-01","2026-11-11")

    def test_partial_search_page_is_rejected(self):
        reader=PublicSOSReader("fixture")
        with patch.object(reader,"request",side_effect=[1,{"totalCount":2,"records":[]}]):
            with self.assertRaisesRegex(ValueError,"Incomplete SOS page"):
                reader.snapshot([1],{},"2000-01-01","2026-11-11")

    def test_lookup_recovers_observation_moved_outside_complex(self):
        reader=PublicSOSReader("fixture")
        moved=self.row("one",9)
        records={}
        with patch.object(reader,"lookup",return_value=(moved,None)):
            reader.confirm_missing([P+"one"],records,{},{},"2000-01-01","2026-11-11")
        self.assertEqual(records[P+"one"]["DyntaxaTaxonId"],9)


if __name__=="__main__":
    unittest.main()
