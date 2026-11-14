"""List originals, ownership, source-bound acceptance and transactional rollback."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from geomap.shared_store import initialize,connect
from geomap.list_review import SCHEMA,preview,apply,approved,content_hash
from geomap.data_validation import Validator

class ListReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.path=self.root/"shared.sqlite"
        initialize(self.path)
        self.settings={"GEOMAP_SHARED_DB":str(self.path),"GEOMAP_DEFAULT_LISTS_DIR":str(self.root),
                       "GEOMAP_TAXONOMY_JOBS":str(self.root)}
        (self.root/"lepidoptera-species.json").write_text(json.dumps(dict(
            species=[{"taxon_id":2}],species_mapping={},source_sha256="hash",retrieved_at="2026-01-01")))
        self.context=dict(taxon={"scientific_name":"Complex"},candidates=[
            dict(taxon_id=2,scientific_name="Species two",swedish_name="")],
            basis_hash="facts-one",source_hash="source-one",source_at="2026-01-01")
        self.mock=patch("geomap.list_review.source_context",return_value=self.context)
        self.mock.start()
        with connect(self.path) as db:
            db.executescript(SCHEMA)
            db.execute("INSERT INTO taxa VALUES(1,'Original','')")
            for lid,owner in (("mine",2),("other",3)):
                db.execute("INSERT INTO lists(id,title,owner_id,visibility,content_hash) VALUES(?,?,?,'private',?)",
                           (lid,lid,owner,content_hash([1])))
                db.execute("INSERT INTO list_taxa VALUES(?,1)",(lid,))

    def tearDown(self):
        self.mock.stop();self.temp.cleanup()

    def body(self,db,action="add",ids=None):
        p=preview(db,self.settings,self.root,"mine",1,2)
        return dict(token=p["token"],action=action,additions=[2] if ids is None else ids,note="Reviewed source relation")

    def test_preserves_original_and_journals_before_after(self):
        with connect(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            result=apply(db,self.settings,self.root,"mine",1,2,self.body(db),"backup")
            self.assertTrue(result["map_rebuild_required"])
            self.assertEqual(db.execute("SELECT needs_regeneration FROM list_taxonomy_state WHERE list_id='mine'").fetchone()[0],1)
            self.assertEqual([r[0] for r in db.execute("SELECT taxon_id FROM list_taxa WHERE list_id='mine' ORDER BY taxon_id")],[1,2])
            self.assertEqual(db.execute("SELECT taxon_id FROM list_taxa WHERE list_id='other'").fetchone()[0],1)
            row=db.execute("SELECT * FROM list_taxonomy_reviews").fetchone()
            self.assertEqual(json.loads(row["before_json"]),[1])
            self.assertEqual(json.loads(row["after_json"]),[1,2])
            self.assertTrue(approved(db,"mine",1,self.context,[1,2]))
            # Unrelated new source snapshot does not reopen relevant identical facts.
            changed={**self.context,"source_hash":"another-source"}
            self.assertTrue(approved(db,"mine",1,changed,[1,2]))
            self.assertFalse(approved(db,"mine",1,{**changed,"basis_hash":"new-facts"},[1,2]))
            self.assertFalse(approved(db,"mine",1,self.context,[1]))

    def test_no_other_owner_or_arbitrary_replacement_and_stale_preview(self):
        with connect(self.path) as db:
            with self.assertRaises(PermissionError):
                preview(db,self.settings,self.root,"other",1,2)
            body=self.body(db,ids=[999])
            with self.assertRaises(ValueError):
                apply(db,self.settings,self.root,"mine",1,2,body,"backup")
            body=self.body(db);body["token"]="0"*64
            with self.assertRaisesRegex(ValueError,"stale_preview"):
                apply(db,self.settings,self.root,"mine",1,2,body,"backup")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_taxonomy_reviews").fetchone()[0],0)

    def test_rollback_removes_membership_and_audit_together(self):
        with self.assertRaises(RuntimeError):
            with connect(self.path) as db:
                db.execute("BEGIN IMMEDIATE")
                apply(db,self.settings,self.root,"mine",1,2,self.body(db),"backup")
                raise RuntimeError("after write")
        with connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_taxa WHERE list_id='mine'").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_taxonomy_reviews").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()[0],"0")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM list_taxonomy_state").fetchone()[0],0)

    def test_keep_is_reviewed_not_deleted_and_revalidation_closes_only_own_issue(self):
        with connect(self.path) as db:
            apply(db,self.settings,self.root,"mine",1,2,self.body(db,"keep",[]),"backup")
        # Avoid depending on source rendering for the remaining other-owner warning.
        with patch.object(Validator,"describe_taxon",return_value=dict(taxon_id=1,reason="outside_catalogue")):
            with Validator(self.settings) as v:
                result=v.validate("lists")
        self.assertEqual(result["metrics"]["reviewed_entries"],1)
        self.assertEqual(result["metrics"]["out_of_scope_entries"],1)
        self.assertEqual(result["metrics"]["lists"],2)
        check=next(c for c in result["checks"] if c["code"]=="list_taxa")
        self.assertNotIn("2",check["private_details"])
        self.assertEqual(check["private_details"]["3"]["count"],1)
