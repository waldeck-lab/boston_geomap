"""NRM pages include obsolete combinations, copied titles and inline range text."""
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location("nrm_audit",Path(__file__).resolve().parents[1]/"scripts/audit_nrm_watch.py")
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class NrmAuditTests(unittest.TestCase):
    def parse(self,description,slug="exemplaria_borealis",title="Exemplaria borealis"):
        raw=("<html><title>"+title+" (Insecta: Lepidoptera)</title><p>"+description+"</p></html>").encode("iso-8859-1")
        return module.parse(raw,"http://www3.nrm.se/en/svenska_fjarilar/e/"+slug+".html")

    def test_foreign_range_in_inline_macro_page_is_not_missed(self):
        r=self.parse("Exemplaria borealis förekommer i Danmark men saknas i övriga Norden.")
        self.assertTrue(r["distribution_flag"])
        self.assertFalse(r["fields"])
        self.assertIn("Danmark",r["distribution_text"])

    def test_swedish_provinces_and_nordic_comment_do_not_become_foreign(self):
        for region in ("Skåne","Sk, Bl, Ha och Sm"):
            r=self.parse("Utbredning: Förekommer i "+region+". I övriga Norden saknas den i Finland.")
            self.assertFalse(r["distribution_flag"])

    def test_absence_must_refer_to_sweden_not_cross_sentence_boundary(self):
        r=self.parse("Togs som ny art för Sverige 2021 på Öland. Den saknas i övriga Norden.")
        self.assertFalse(r["explicit_swedish_absence"])
        self.assertFalse(r["distribution_flag"])

    def test_copied_title_is_retained_but_not_used_as_page_identity(self):
        r=self.parse("Utbredning: Förekommer i Finland.","exemplaria_alba","Exemplaria nigra")
        self.assertEqual(r["page_name"],"Exemplaria alba")
        self.assertEqual(r["title_name"],"Exemplaria nigra")

    def test_older_absence_and_host_phenology_fields_are_retained(self):
        r=self.parse("Flygtid: Juli. Näringsväxt: Salix. Utbredning: Ännu inte funnen i Sverige. I Norden känd från Danmark.")
        self.assertTrue(r["explicit_swedish_absence"])
        self.assertEqual(r["fields"]["Flygtid"],"Juli.")
        self.assertEqual(r["fields"]["Näringsväxt"],"Salix.")
