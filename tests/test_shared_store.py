import csv
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'server'))
from geomap.shared_store import initialize, register_list, connect, apply_rows, rebuild_cells
from geomap.tiles import lonlat_to_tile_xy
from server.shared_api import register_shared_api
from flask import Flask


class SharedStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='shared-test-',dir='/proj/boston/beta/wcr/stage')
        self.root=Path(self.temp.name);self.db=self.root/'shared.sqlite'
        initialize(self.db)
        for slug,ids,owner in [('redlisted-lepidoptera',[1,2],None),('private',[2,3],2)]:
            path=self.root/(slug+'.csv')
            with path.open('w') as f:
                w=csv.writer(f);w.writerow(['taxon_id','scientific_name','swedish_name'])
                for tid in ids:w.writerow([tid,'Species '+str(tid),''])
            register_list(self.db,slug,slug,path,owner)
        self.rows=[dict(OccurrenceId=str(i),DyntaxaTaxonId=str(i),StartDate='2026-07-10',
             DecimalLatitude='55.7',DecimalLongitude='13.2',OccurrenceStatus='present') for i in [1,2,3]]
        with connect(self.db) as db:
            apply_rows(db,self.rows);apply_rows(db,[self.rows[1]]);rebuild_cells(db)
        self.app=Flask(__name__)
        with patch.dict(os.environ,{'GEOMAP_SHARED_DB':str(self.db)}):register_shared_api(self.app)
        self.client=self.app.test_client()

    def tearDown(self):self.temp.cleanup()

    def test_overlap_does_not_duplicate_observation(self):
        with connect(self.db,readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0],3)
            self.assertEqual(db.execute('SELECT sum(n) FROM taxon_cells').fetchone()[0],3)

    def test_same_source_id_updates_existing_record(self):
        changed=dict(self.rows[1],DecimalLatitude='56.0',StartDate='2026-08-10')
        with connect(self.db) as db:
            apply_rows(db,[changed]);rebuild_cells(db)
            row=db.execute("SELECT latitude,observation_date FROM observations WHERE occurrence_id='2'").fetchone()
            self.assertEqual(tuple(row),(56.0,'2026-08-10'))
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0],3)

    def test_default_map_counts_each_species_once(self):
        r=self.client.get('/geomap-api/hotmap_window?zoom=15&slot_ids=25,26,27&year_from=2026&year_to=2026')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.json['features'][0]['properties']['coverage'],2)
        self.assertEqual(r.json['features'][0]['properties']['obs_total'],2)

    def test_private_list_hidden_anonymous_and_wrong_user(self):
        self.assertEqual(self.client.get('/geomap-api/hotmap?list_id=private').status_code,404)
        self.client.set_cookie('session','test-only')
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':3}}):
            self.assertEqual(self.client.get('/geomap-api/hotmap?list_id=private').status_code,404)
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':2}}):
            r=self.client.get('/geomap-api/hotmap?list_id=private')
            self.assertEqual(r.status_code,200)
            self.assertEqual(sum(f['properties']['obs_total'] for f in r.json['features']),2)

    def test_same_contents_can_share_cache_without_sharing_permissions(self):
        register_list(self.db,'private-copy','Private copy',self.root/'redlisted-lepidoptera.csv',2)
        public=self.client.get('/geomap-api/hotmap').json
        self.client.set_cookie('session','test-only')
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':2}}):
            own=self.client.get('/geomap-api/hotmap?list_id=private-copy')
            self.assertEqual(own.status_code,200)
            self.assertEqual(own.json,public)
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':3}}):
            self.assertEqual(self.client.get('/geomap-api/hotmap?list_id=private-copy').status_code,404)

    def test_legacy_writes_are_blocked(self):
        self.assertEqual(self.client.post('/geomap-api/jobs/sos_import',json={}).status_code,409)

    def test_public_catalog_and_progress_do_not_reveal_private_list(self):
        (self.root/'import-status.json').write_text(json.dumps(dict(busy=True,phase='importing-list-2',taxa_total=1030,taxa_reused=297)))
        response=self.client.get('/geomap-api/lists')
        self.assertEqual([r['id'] for r in response.json['lists']],['redlisted-lepidoptera'])
        status=self.client.get('/geomap-api/jobs/status').json
        self.assertNotIn('taxa_total',status['data_import'])
        self.assertEqual(status['data_import']['phase'],'importing')

    def test_exact_point_cache_never_bypasses_authentication(self):
        x,y=lonlat_to_tile_xy(13.2,55.7,15)
        path=f'/geomap-api/cell/observations?zoom=15&x={x}&y={y}'
        self.client.set_cookie('session','test-only')
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':2,'is_admin':True}}):
            self.assertEqual(self.client.get(path).status_code,200)
        self.client.delete_cookie('session')
        self.assertEqual(self.client.get(path).status_code,403)

    def test_regular_user_points_follow_list_and_private_owner_permissions(self):
        x,y=lonlat_to_tile_xy(13.2,55.7,15)
        path=f'/geomap-api/cell/observations?zoom=15&x={x}&y={y}'
        self.client.set_cookie('session','test-only')
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':2,'is_admin':False}}):
            r=self.client.get(path)
            self.assertEqual(r.status_code,200)
            self.assertEqual({f['properties']['taxon_id'] for f in r.json['features']},{1,2})
            self.assertFalse(r.json['truncated'])
            r=self.client.get(path+'&list_id=private')
            self.assertEqual({f['properties']['taxon_id'] for f in r.json['features']},{2,3})
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':3,'is_admin':False}}):
            self.assertEqual(self.client.get(path+'&list_id=private').status_code,404)

    def test_point_limit_is_explicit_in_response(self):
        rows=[dict(self.rows[0],OccurrenceId='cap-'+str(i),DecimalLongitude=str(13.2+i*1e-9)) for i in range(2001)]
        with connect(self.db) as db:apply_rows(db,rows)
        x,y=lonlat_to_tile_xy(13.2,55.7,15)
        self.client.set_cookie('session','test-only')
        with patch('auth_client.introspect_current_request',return_value={'authenticated':True,'user':{'id':2,'is_admin':False}}):
            r=self.client.get(f'/geomap-api/cell/observations?zoom=15&x={x}&y={y}')
            self.assertEqual(r.status_code,200)
            self.assertTrue(r.json['truncated']);self.assertEqual(len(r.json['features']),2000)

    def test_parent_tile_matches_direct_projection(self):
        x,y=lonlat_to_tile_xy(13.2,55.7,21)
        for z in [5,12,15,20,21]:
            self.assertEqual((x>>(21-z),y>>(21-z)),lonlat_to_tile_xy(13.2,55.7,z))

    def test_deleting_private_list_keeps_shared_observations(self):
        with connect(self.db) as db:
            db.execute("DELETE FROM lists WHERE id='private'")
            db.execute('DELETE FROM observations WHERE NOT EXISTS (SELECT 1 FROM list_taxa l WHERE l.taxon_id=observations.taxon_id)')
            self.assertEqual([r[0] for r in db.execute('SELECT taxon_id FROM observations ORDER BY taxon_id')],[1,2])


if __name__=='__main__':unittest.main()
