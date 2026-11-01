import csv
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from geomap.watch_store import initialize,connect,source,import_matrix,import_traits,import_occurrences,profile

class WatchStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.path=self.root/'watch.sqlite';initialize(self.path);self.db=connect(self.path)
        self.file=self.root/'source.txt';self.file.write_text('fixture')
        source(self.db,'fixture','Fixture','https://example.test','CC0','v1',self.file)
        self.db.execute("INSERT INTO candidates(id,scientific_name,role) VALUES(1,'Species alpha','exploratory')")
    def tearDown(self):self.db.close();self.temp.cleanup()
    def record(self,**patch):
        value=dict(key=1,occurrenceID='https://example.test/obs/1',datasetKey='fixture',license='https://creativecommons.org/licenses/by/4.0/legalcode',countryCode='DK',eventDate='2024-06-01',decimalLatitude=55.5,decimalLongitude=12.5,coordinateUncertaintyInMeters=50,lifeStage='Adult',occurrenceStatus='PRESENT',issues=[])
        value.update(patch);return value
    def test_matrix_whitespace_duplicates_union_and_missing_are_not_absence(self):
        p=self.root/'matrix.csv';p.write_text('SPECIES;Plant alpha;Plant beta\nSpecies alpha;1;\nSpecies alpha ;0;1\n')
        for _ in range(2):report=import_matrix(self.db,p,{'Species alpha':1},'fixture')
        self.assertEqual(report['duplicate_pilot_names'],1);self.assertEqual(report['missing_cells'],1)
        rows=self.db.execute('SELECT * FROM host_associations').fetchall();self.assertEqual(len(rows),2);self.assertTrue(all(r['is_primary'] is None for r in rows))
    def test_source_revision_is_immutable(self):
        self.file.write_text('changed')
        with self.assertRaises(ValueError):source(self.db,'fixture','Fixture','https://example.test','CC0','v1',self.file)
    def test_licensed_foreign_positive_records_only(self):
        records=[self.record(),self.record(key=2,license='https://creativecommons.org/licenses/by-nc/4.0/legalcode'),self.record(key=3,countryCode='SE'),self.record(key=4,eventDate='2027-01-01'),self.record(key=5,informationWithheld='sensitive'),self.record(key=6,decimalLatitude=float('nan')),self.record(key=7,occurrenceStatus='ABSENT'),self.record(key=8,license='https://other.test/licenses/by/4.0/legalcode'),self.record(key=9,coordinateUncertaintyInMeters=-1)]
        counts=import_occurrences(self.db,records,1,'fixture');self.assertEqual(counts,{'accepted_records':1,'rejected_records':8})
    def test_repeat_and_cross_dataset_duplicates_preserve_source_records(self):
        records=[self.record(),self.record(key=2,datasetKey='another')]
        for _ in range(2):import_occurrences(self.db,records,1,'fixture')
        self.assertEqual(self.db.execute('SELECT count(*) FROM occurrences').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM occurrence_records').fetchone()[0],2)
        self.assertIn('Svensk status och expansionsbedömning',profile(self.db,1)['gaps'])
    def test_traits_two_headers_region_stage_and_primary_host_are_preserved(self):
        groups=['species_details','adult_stage','larval_stage','common_hostplants','habitat','overwintering_stage'];keys=['scientific_name ','ad_agu','lar_dec','Plant alpha','woodland','overwinter_larva']
        text=io.StringIO();writer=csv.writer(text);writer.writerows([groups,keys,['Species\u00a0alpha','1','1','1','1','1']])
        p=self.root/'traits.zip'
        with zipfile.ZipFile(p,'w') as z:z.writestr('data/ecological_traits_2024.csv',text.getvalue().encode('cp1252'))
        for _ in range(2):import_traits(self.db,p,{'Species alpha':1},'fixture')
        rows=self.db.execute('SELECT life_stage,month,region FROM phenology ORDER BY month').fetchall()
        self.assertEqual([(r[0],r[1]) for r in rows],[('adult',8),('larva',12)])
        self.assertTrue(all(r['region']=='Great Britain and Ireland' for r in rows))
        self.assertEqual(self.db.execute('SELECT is_primary FROM host_associations').fetchone()[0],1)
        self.assertIsNone(self.db.execute('SELECT is_primary FROM habitat_requirements').fetchone()[0])
        self.assertEqual(self.db.execute('SELECT count(*) FROM ecological_requirements').fetchone()[0],1)
    def test_bad_matrix_is_rejected_and_foreign_keys_are_enforced(self):
        p=self.root/'bad.csv';p.write_text('SPECIES;Plant alpha\nSpecies alpha;probably\n')
        with self.assertRaises(ValueError):import_matrix(self.db,p,{'Species alpha':1},'fixture')
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(),[])

if __name__=='__main__':unittest.main()
