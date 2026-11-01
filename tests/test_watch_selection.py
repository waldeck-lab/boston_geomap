import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from review_watch_selection import baseline,add_updates,BAB_2025

class WatchSelectionTests(unittest.TestCase):
    def test_country_parser_does_not_treat_taxonomy_as_swedish_occurrence(self):
        rows=baseline('577 Cameraria Chapman, 1902\n578 ohridella Deschka & Dimić, 1986    N S D F E Lv Lt\n503 Macrosaccus Davis, 2011\n504 robiniella (Clemens, 1859)    D Lt\n',validate=False)
        self.assertIn('S',rows['Cameraria ohridella']['countries'])
        self.assertNotIn('S',rows['Macrosaccus robiniella']['countries'])
    def test_broken_or_incomplete_checklist_fails_published_count_controls(self):
        with self.assertRaises(ValueError):baseline('577 Cameraria Chapman, 1902\n578 ohridella Author, 1986    D\n')
    def test_updates_apply_wrapped_country_records_without_author_initials(self):
        rows={}
        add_updates(rows,'\nNew country records\n0500 Species alpha (Author, 1900) S (Bengtsson 2024),\nD.\n0501 Species beta (Author, 1900) D (Smith S. 2020).\nTaxonomic changes\n0502 Species gamma (Author, 1900) S.','update.pdf')
        self.assertEqual(rows['Species alpha']['countries'],['D','S'])
        self.assertEqual(rows['Species beta']['countries'],['D'])
        self.assertNotIn('Species gamma',rows)
    def test_missing_update_section_fails_closed(self):
        with self.assertRaises(IndexError):add_updates({},'An unrelated article without country records','wrong.pdf')
    def test_latest_bab_positive_records_exclude_recent_swedish_newcomers(self):
        self.assertIn('Phyllonorycter issikii',BAB_2025)
        self.assertIn('Phyllonorycter medicaginella',BAB_2025)
        self.assertIn('Caloptilia honoratella',BAB_2025)
        self.assertNotIn('Blastobasis phycidella',BAB_2025)

if __name__=='__main__':unittest.main()
