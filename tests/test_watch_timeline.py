import sys,unittest
from datetime import date
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from build_watch_timeline import normalize

class TimelineTests(unittest.TestCase):
    def record(self,**patch):
        r=dict(key=1,datasetKey='dataset',occurrenceID='https://example.test/1',license='https://creativecommons.org/licenses/by/4.0/legalcode',eventDate='1900-06-01',countryCode='DK',decimalLatitude=55,decimalLongitude=12,lifeStage='Adult');r.update(patch);return r
    def parse(self,**patch):return normalize(self.record(**patch),date(2026,11,2))
    def test_historical_and_partial_dates_preserve_real_precision(self):
        self.assertEqual(self.parse()['year'],1900)
        r=self.parse(eventDate='1850');self.assertEqual((r['year'],r['month'],r['date_precision']),(1850,None,'year'))
        r=self.parse(eventDate='1974-07');self.assertEqual((r['month'],r['date_precision']),(7,'month'))
        self.assertEqual(self.parse(eventDate='1999-01-01/1999-12-31')['month'],None)
    def test_ambiguous_years_invalid_dates_and_future_days_are_rejected(self):
        for value in ['1999/2000','not dated','2026-11-03','2027','0000','1963-02-29','1963-08/1963-07']:
            with self.subTest(value=value):self.assertIsNone(self.parse(eventDate=value))
    def test_license_geography_and_suppression_remain_enforced(self):
        for patch in [dict(license='https://creativecommons.org/licenses/by-nc/4.0/legalcode'),dict(countryCode='SE'),dict(informationWithheld='hidden'),dict(dataGeneralizations='generalized'),dict(decimalLatitude=float('nan')),dict(coordinateUncertaintyInMeters=-1),dict(occurrenceStatus='ABSENT')]:
            with self.subTest(patch=patch):self.assertIsNone(self.parse(**patch))
    def test_same_origin_keeps_identity_across_gbif_records(self):
        self.assertEqual(self.parse()['occurrence_uid'],self.parse(key=2,datasetKey='other')['occurrence_uid'])

if __name__=='__main__':unittest.main()
