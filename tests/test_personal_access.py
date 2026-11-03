import io,os,tempfile,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"server"))
from pathlib import Path
from unittest.mock import patch
from flask import Flask,request
from geomap.shared_store import initialize,connect
from server.shared_api import register_shared_api

class PersonalAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'shared.sqlite';initialize(self.path)
        with connect(self.path) as db:
            db.execute("INSERT INTO lists(id,title,owner_id,visibility,content_hash) VALUES('private','Private title',1,'private','private-hash')")
            db.execute("INSERT INTO lists(id,title,visibility,content_hash) VALUES('redlisted-lepidoptera','Public title','public','public-hash')")
        self.env=patch.dict(os.environ,GEOMAP_SHARED_DB=str(self.path),GEOMAP_DATA_ROOT=self.temp.name);self.env.start()
        self.allowed=True;self.uid=1
        def auth():return dict(authenticated=True,user=dict(id=self.uid,is_admin=False,list_access=dict(allowed=self.allowed)))
        self.auth=patch('auth_client.introspect_current_request',side_effect=auth);self.auth.start()
        self.app=Flask(__name__);register_shared_api(self.app);self.client=self.app.test_client();self.client.set_cookie('fixture','test')
    def tearDown(self):self.auth.stop();self.env.stop();self.temp.cleanup()
    def test_expiry_hides_private_catalog_and_blocks_cached_maps_csv_and_uploads(self):
        url='/geomap-api/hotmap?list_id=private&zoom=15'
        self.assertEqual(self.client.get(url).status_code,200) # populate server cache
        self.allowed=False
        rows=self.client.get('/geomap-api/lists').json['lists'];self.assertEqual([r['id'] for r in rows],['redlisted-lepidoptera'])
        for route in ['hotmap','hotmap_window','slots/coverage','cell/taxa','cell/taxa_window','cell/phenology','cell/observations']:
            response=self.client.get('/geomap-api/'+route+'?list_id=private&zoom=15&x=1&y=1');self.assertEqual(response.status_code,403,route);self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.assertEqual(self.client.get('/geomap-api/lists/private/csv').status_code,403)
        self.assertEqual(self.client.post('/geomap-api/lists/from-observed-csv',data={'file':(io.BytesIO(b'taxon_id\n1\n'),'test.csv')}).status_code,403)
        self.assertEqual(self.client.get('/geomap-api/hotmap?zoom=15').status_code,200)
        with connect(self.path,readonly=True) as db:self.assertEqual(db.execute("SELECT title FROM lists WHERE id='private'").fetchone()[0],'Private title')
        self.allowed=True;self.assertEqual(self.client.get(url).status_code,200)
    def test_saved_private_view_is_unavailable_after_expiry_but_preserved(self):
        from test_saved_views import STATE
        state=dict(STATE,list_id='private')
        response=self.client.post('/geomap-api/views',json=dict(title='Saved private view',state=state))
        self.assertEqual(response.status_code,201);identifier=response.json['view']['id']
        self.allowed=False
        self.assertFalse(self.client.get('/geomap-api/views').json['views'][0]['available'])
        self.assertEqual(self.client.get('/geomap-api/views/'+identifier).status_code,404)
        self.assertEqual(self.client.post('/geomap-api/views',json=dict(title='No bypass',state=state)).status_code,404)
        self.allowed=True
        self.assertEqual(self.client.get('/geomap-api/views/'+identifier).status_code,200)

    def test_other_users_still_receive_not_found(self):
        self.uid=2;self.allowed=True
        self.assertEqual(self.client.get('/geomap-api/hotmap?list_id=private').status_code,404)
        self.assertEqual(self.client.get('/geomap-api/lists/private/csv').status_code,404)

if __name__=='__main__':unittest.main()
