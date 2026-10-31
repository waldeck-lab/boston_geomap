import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'server'))
from flask import Flask
from geomap.observed_csv import parse_observed_csv,build_difference,CsvValidationError,MAX_BYTES
from geomap.shared_store import initialize,connect
from server.shared_api import register_shared_api


class ObservedCsvTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='csv-test-',dir='/proj/boston/beta/wcr/stage')
        self.root=Path(self.temp.name);self.db=self.root/'shared.sqlite';initialize(self.db)
        self.catalogue=self.root/'lepidoptera-species.json'
        self.catalogue.write_text(json.dumps(dict(retrieved_at='2026-10-31',source_sha256='fixture',
          species=[dict(taxon_id=i,scientific_name=f'Species {i}',swedish_name='') for i in [1,2,3]],
          species_mapping={'1':1,'2':2,'3':3,'11':1})))
        self.app=Flask(__name__)
        with patch.dict(os.environ,{'GEOMAP_SHARED_DB':str(self.db),'GEOMAP_DEFAULT_LISTS_DIR':str(self.root)}):
            register_shared_api(self.app)
        self.client=self.app.test_client();self.client.set_cookie('session','test-only')
        self.owner=patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':2}})
        self.owner.start();self.addCleanup(self.owner.stop)

    def tearDown(self):self.temp.cleanup()

    def post(self,save=False,raw=b'TaxonId\n1\n',**kw):
        return self.client.post('/geomap-api/lists/from-observed-csv',data={
          'file':(io.BytesIO(raw),'mina.csv'),'save':str(save).lower(),'title':'Min lista','owner_id':'99'},**kw)

    def test_delimiters_encodings_and_duplicate_normalization(self):
        for delimiter in [';',',','\t']:
            for encoding in ['utf-8-sig','cp1252']:
                raw=(f'TaxonId{delimiter}Namn\n1{delimiter}blå\n1{delimiter}blå\n').encode(encoding)
                ids,summary=parse_observed_csv(raw)
                self.assertEqual(ids,{1});self.assertEqual(summary['duplicate_rows'],1)
        ids,_=parse_observed_csv(b'sep=;\nTaxonId;Namn\n1;art\n');self.assertEqual(ids,{1})

    def test_rejects_ambiguous_invalid_empty_and_oversized_files(self):
        for raw in [b'',b'Name\nart',b'TaxonId;taxon_id\n1;2',b'TaxonId\n',b'TaxonId\nNaN',
                    b'TaxonId\n1.5',b'TaxonId\n0',b'TaxonId\n1;extra',b'x'*(MAX_BYTES+1)]:
            with self.subTest(raw=raw[:30]):
                with self.assertRaises(CsvValidationError):parse_observed_csv(raw)

    def test_species_mapping_and_unknown_ids(self):
        rows,observed,summary=build_difference(b'TaxonId\n11\n',self.catalogue)
        self.assertEqual(observed,{1});self.assertEqual([r['taxon_id'] for r in rows],[2,3])
        self.assertEqual(summary['mapped_infraspecific_taxa'],1)
        with self.assertRaises(CsvValidationError):build_difference(b'TaxonId\n999',self.catalogue)

    def test_preview_makes_no_changes(self):
        r=self.post();self.assertEqual(r.status_code,200);self.assertEqual(r.json['preview']['missing_species'],2)
        with connect(self.db,readonly=True) as db:self.assertEqual(db.execute('SELECT count(*) FROM lists').fetchone()[0],0)
        self.assertFalse((self.root/'user-lists').exists())

    def test_saved_list_uses_authenticated_owner_and_has_no_geodata(self):
        response=self.post(save=True);self.assertEqual(response.status_code,200)
        saved=response.json['list'];self.assertEqual(saved['taxa_needing_import'],2)
        with connect(self.db,readonly=True) as db:
            row=db.execute('SELECT owner_id,visibility FROM lists').fetchone();self.assertEqual(tuple(row),(2,'private'))
            self.assertEqual([r[0] for r in db.execute('SELECT taxon_id FROM list_taxa ORDER BY taxon_id')],[2,3])
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM taxon_cells').fetchone()[0],0)
        download=self.client.get('/geomap-api/lists/'+saved['id']+'/csv')
        self.assertEqual(download.status_code,200);self.assertIn(b'2,Species 2',download.data)
        self.assertEqual(download.headers['Cache-Control'],'no-store')
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':99,'is_admin':True}}):
            self.assertEqual(self.client.get('/geomap-api/lists/'+saved['id']+'/csv').status_code,404)
            self.assertEqual(self.client.get('/geomap-api/lists').json['lists'],[])
            self.assertEqual(self.client.get('/geomap-api/hotmap?list_id='+saved['id']).status_code,404)
        self.client.delete_cookie('session')
        self.assertEqual(self.client.get('/geomap-api/lists/'+saved['id']+'/csv').status_code,404)

    def test_auth_origin_and_size_limits(self):
        self.assertEqual(self.post(headers={'Origin':'https://other.example'}).status_code,403)
        self.assertEqual(self.post(raw=b'x'*(MAX_BYTES+65536)).status_code,413)
        self.client.delete_cookie('session');self.assertEqual(self.post().status_code,401)

    def test_invalid_upload_never_saves(self):
        self.assertEqual(self.post(save=True,raw=b'TaxonId\n999').status_code,400)
        with connect(self.db,readonly=True) as db:self.assertEqual(db.execute('SELECT count(*) FROM lists').fetchone()[0],0)

if __name__=='__main__':unittest.main()
