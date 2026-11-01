import csv
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from flask import Flask

root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root));sys.path.insert(0,str(root/'server'))
from geomap.documentation import match_nrm,read_references,valid_reference_url
from geomap.shared_store import initialize,register_list,connect,apply_rows,rebuild_cells
from geomap.tiles import lonlat_to_tile_xy
from server.shared_api import register_shared_api

class DocumentationTests(unittest.TestCase):
    def catalogue(self):
        return dict(species=[dict(taxon_id=1,scientific_name='New alpha',swedish_name='första arten'),dict(taxon_id=2,scientific_name='Other beta',swedish_name='andra arten')],
            name_index={'new alpha':[1],'other beta':[2],'old alpha':[1],'split alpha':[1,2]})

    def test_index_current_names_synonyms_and_gender_ending_changes(self):
        html='<a href="o/old_alpha.html"><i>alpha</i></a> första arten<br><a href="o/other_beta.html"><i>beta</i></a> andra arten'
        refs,count=match_nrm(self.catalogue(),html)
        self.assertEqual(count,2);self.assertEqual(refs[1]['match_method'],'synonym');self.assertTrue(refs[2]['url'].endswith('/o/other_beta.html'))
        catalogue=dict(species=[dict(taxon_id=1,scientific_name='Archips podanus',swedish_name='')],name_index={'archips podanus':[1]})
        refs,_=match_nrm(catalogue,'<a href="a/archips_podana.html"><i>podanus</i></a>')
        self.assertTrue(refs[1]['url'].endswith('archips_podana.html'))

    def test_ambiguous_and_missing_species_are_not_guessed(self):
        refs,_=match_nrm(self.catalogue(),'<a href="s/split_alpha.html">alpha</a>')
        self.assertIsNone(refs[1]['url']);self.assertEqual(refs[1]['status'],'ambiguous');self.assertIsNone(refs[2]['url'])
        refs,_=match_nrm(self.catalogue(),'<a href="s/split_alpha.html">alpha</a> första arten<br>')
        self.assertTrue(refs[1]['url']);self.assertIsNone(refs[2]['url'])
        refs,_=match_nrm(self.catalogue(),'<a href="n/new_alfa.html">alfa</a>')
        self.assertEqual(refs[1]['status'],'not_indexed')

    def test_external_and_non_species_links_are_excluded(self):
        html='<a href="https://evil.example/n/new_alpha.html">alpha</a><a href="n/new_genitalia.html">Male genitalia</a><a href="javascript:alert(1)">alpha</a>'
        refs,count=match_nrm(self.catalogue(),html);self.assertEqual(count,0);self.assertIsNone(refs[1]['url'])
        self.assertFalse(valid_reference_url('nrm','https://www3.nrm.se@evil.example/en/svenska_fjarilar/n/new_alpha.html'))
        self.assertFalse(valid_reference_url('artfakta','javascript:alert(1)'))

class DocumentationApiTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='documentation-test-',dir='/proj/boston/beta/wcr/stage');self.root=Path(self.temp.name)
        self.docs=self.root/'documentation.sqlite';db=sqlite3.connect(self.docs)
        db.executescript('CREATE TABLE species(taxon_id INTEGER PRIMARY KEY,scientific_name TEXT,swedish_name TEXT); CREATE TABLE documentation(taxon_id INTEGER,source TEXT,url TEXT,status TEXT,matched_name TEXT,match_method TEXT);')
        db.execute('INSERT INTO species VALUES(1,?,?)',('New alpha','första arten'))
        db.execute('INSERT INTO documentation VALUES(1,?,?,?,?,?)',('nrm','http://www3.nrm.se/en/svenska_fjarilar/o/old_alpha.html','indexed','old alpha','synonym'))
        db.execute('INSERT INTO documentation VALUES(1,?,?,?,?,?)',('artfakta','https://artfakta.se/taxa/1/information','taxon_route','New alpha','dyntaxa-id'))
        db.commit();db.close()
        self.shared=self.root/'shared.sqlite';initialize(self.shared)
        seed=self.root/'seed.csv';seed.write_text('taxon_id,scientific_name,swedish_name\n1,New alpha,första arten\n')
        register_list(self.shared,'redlisted-lepidoptera','Fixture',seed)
        with connect(self.shared) as db:
            apply_rows(db,[dict(OccurrenceId='fixture-1',DyntaxaTaxonId=1,StartDate='2026-07-10',DecimalLatitude=55.7,DecimalLongitude=13.2,OccurrenceStatus='present')]);rebuild_cells(db)
        self.app=Flask(__name__)
        with patch.dict(os.environ,{'GEOMAP_SHARED_DB':str(self.shared),'GEOMAP_DOCUMENTATION_DB':str(self.docs)}):register_shared_api(self.app)
        self.client=self.app.test_client()

    def tearDown(self):self.temp.cleanup()

    def test_public_species_and_batch_routes_validate_ids(self):
        response=self.client.get('/geomap-api/documentation/1');self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['documentation']['artfakta']['url'],'https://artfakta.se/taxa/1/information')
        self.assertEqual(self.client.get('/geomap-api/documentation/999').status_code,404)
        self.assertEqual(self.client.get('/geomap-api/documentation?taxon_ids=1,999').status_code,200)
        self.assertEqual(self.client.get('/geomap-api/documentation?taxon_ids=abc').status_code,400)
        self.assertEqual(self.client.post('/geomap-api/documentation/1').status_code,409)

    def test_cell_taxa_are_enriched_and_catalogue_replacement_invalidates_cache(self):
        x,y=lonlat_to_tile_xy(13.2,55.7,15);url=f'/geomap-api/cell/taxa?zoom=15&x={x}&y={y}'
        response=self.client.get(url);self.assertEqual(response.status_code,200)
        self.assertTrue(response.json[0]['documentation']['nrm']['url'].endswith('old_alpha.html'))
        replacement=self.root/'replacement.sqlite'
        source=sqlite3.connect(self.docs);target=sqlite3.connect(replacement);source.backup(target);source.close()
        target.execute('UPDATE documentation SET url=? WHERE source=?',('http://www3.nrm.se/en/svenska_fjarilar/n/new_alpha.html','nrm'));target.commit();target.close();replacement.replace(self.docs)
        response=self.client.get(url);self.assertTrue(response.json[0]['documentation']['nrm']['url'].endswith('new_alpha.html'))

    def test_stored_unsafe_urls_are_never_returned(self):
        db=sqlite3.connect(self.docs);db.execute("UPDATE documentation SET url='javascript:alert(1)' WHERE source='nrm'");db.commit();db.close()
        ref=read_references(self.docs,[1])[1]['nrm'];self.assertIsNone(ref['url']);self.assertEqual(ref['status'],'invalid_url')

if __name__=='__main__':unittest.main()
