"""A corrected source record must not erase provenance or dismiss other evidence."""
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location("publisher",Path(__file__).resolve().parents[1]/"scripts/publish_nrm_review.py")
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class RecordReviews(unittest.TestCase):
    def test_rejected_single_record_preserves_raw_count_and_correction_source(self):
        row=dict(gbif_key=1860076,sweden_count=1,recent_neighbour_count=137,status="swedish_records_need_review",evidence_urls=[])
        review=dict(gbif_key=1860076,record_id=1821015801,decision="misidentified_not_supporting_species",record_url="https://www.gbif.org/occurrence/1821015801",asserted_by="Jonas Waldeck",asserted_at="2026-11-19")
        module.apply_record_reviews([row],[review])
        self.assertEqual(row["sweden_count"],1)
        self.assertEqual(row["usable_swedish_records_in_review"],0)
        self.assertEqual(row["status"],"priority_review_candidate")
        self.assertEqual(row["record_reviews"][0]["asserted_by"],"Jonas Waldeck")

    def test_other_swedish_evidence_is_not_removed_by_one_correction(self):
        row=dict(gbif_key=1860076,sweden_count=2,recent_neighbour_count=137,status="swedish_records_need_review",evidence_urls=[])
        review=dict(gbif_key=1860076,record_id=1821015801,decision="misidentified_not_supporting_species",record_url="https://www.gbif.org/occurrence/1821015801")
        module.apply_record_reviews([row],[review])
        self.assertEqual(row["usable_swedish_records_in_review"],1)
        self.assertEqual(row["status"],"swedish_records_need_review")
