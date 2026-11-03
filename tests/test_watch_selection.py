import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from review_watch_selection import baseline,add_updates,front_params,BAB_2025,validate_watch_assessment

class WatchSelectionTests(unittest.TestCase):
    def test_publication_can_support_presence_without_inventing_gbif_records(self):
        row=dict(checklist=dict(countries=['N']),gbif_sweden_count=0,front_2015_2026_count=0,checked_at='2026-11-03',occurrence_scope='nordic-front',published_front_evidence=[dict(scientific_name='Coleophora algidella',review_status='primary_source_checked',country='NO',event_year=2022,url='https://example.org/report.pdf',source_sha256='a'*64,document_page=20)])
        validate_watch_assessment('Coleophora algidella',row)
        row['gbif_sweden_count']=1
        with self.assertRaises(ValueError):validate_watch_assessment('Coleophora algidella',row)
    def test_unclear_or_mismatched_publication_is_not_an_admission_override(self):
        row=dict(checklist=dict(countries=['N']),gbif_sweden_count=0,front_2015_2026_count=0,checked_at='2026-11-03',occurrence_scope='nordic-front',published_front_evidence=[dict(url='https://example.org/report.pdf')])
        with self.assertRaises(ValueError):validate_watch_assessment('Coleophora algidella',row)
    def test_biological_presence_does_not_require_a_map_reuse_license(self):
        biological=front_params(1759673)
        self.assertNotIn('license',biological)
        self.assertEqual(biological['limit'],0)
        mapped=front_params(1759673,mappable=True)
        self.assertEqual(mapped.pop('license'),['CC0_1_0','CC_BY_4_0'])
        self.assertEqual(mapped,biological)
    def test_country_parser_does_not_treat_taxonomy_as_swedish_occurrence(self):
        rows=baseline('577 Cameraria Chapman, 1902\n578 ohridella Deschka & Dimić, 1986    N S D F E Lv Lt\n503 Macrosaccus Davis, 2011\n504 robiniella (Clemens, 1859)    D Lt\n',validate=False)
        self.assertIn('S',rows['Cameraria ohridella']['countries'])
        self.assertNotIn('S',rows['Macrosaccus robiniella']['countries'])
    def test_genus_synonyms_cannot_rename_following_coleophora_species(self):
        text='2104    Coleophora Hübner, 1822\n 2105        Goniodoma Zeller, 1849\n 2106        Metriotes Herrich-Schäffer, 1853\n2109           carelica Hackman, 1945    F E Lv\n2112           algidella Zeller, 1857    I N\n'
        rows=baseline(text,validate=False,accepted_genera={'2104':'Coleophora'})
        self.assertEqual(rows['Coleophora carelica']['countries'],['F','E','Lv'])
        self.assertEqual(rows['Coleophora algidella']['countries'],['I','N'])
        self.assertNotIn('Metriotes carelica',rows)
    def test_body_size_genus_inventory_excludes_small_synonym_entries(self):
        import json
        data=json.loads((Path(__file__).resolve().parents[1]/'data/watch/checklist-2017-accepted-genera.json').read_text())['genera']
        self.assertEqual(data['2104'],'Coleophora')
        self.assertNotIn('2105',data)
        self.assertNotIn('2106',data)
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
