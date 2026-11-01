import copy
import tempfile
import unittest
from pathlib import Path
from flask import Flask,request
from geomap.shared_store import initialize,connect
from server.saved_views import register_saved_views

STATE=dict(version=1,list_id='private',camera=dict(center=[16.4,56.2],zoom=10.25,bearing=12,pitch=30),
    grid_zoom=15,year_from=2000,year_to=2026,slot_center=23,slot_radius=1,use_window=True,
    show_phenology=True,base_layer='topo',species_stops=[1,2,5,89],selected=dict(x=17875,y=10150,zoom=15))

class SavedViewsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'shared.sqlite';initialize(self.path)
        with connect(self.path) as db:
            db.execute("INSERT INTO lists(id,title,owner_id,visibility,content_hash) VALUES('private','Owner list',1,'private','fixture')")
            db.execute("INSERT INTO lists(id,title,visibility,content_hash) VALUES('public','Public list','public','fixture')")
        self.app=Flask(__name__)
        def auth():
            uid=request.headers.get('X-Test-User');return dict(authenticated=bool(uid),user=dict(id=int(uid),is_admin=True) if uid else None)
        register_saved_views(self.app,self.path,auth);self.client=self.app.test_client()
    def tearDown(self):self.temp.cleanup()
    def call(self,method,path='',user=1,**kwargs):
        return self.client.open('/geomap-api/views'+path,method=method,headers={'X-Test-User':str(user)} if user else {},**kwargs)
    def save(self,state=None,user=1):return self.call('POST',json={'title':'My Öland map','state':STATE if state is None else state,'owner_id':99},user=user)

    def test_complete_settings_roundtrip_without_copying_observations(self):
        response=self.save();self.assertEqual(response.status_code,201);identifier=response.json['view']['id']
        loaded=self.call('GET','/'+identifier);self.assertEqual(loaded.json['view']['state'],STATE)
        self.assertEqual(self.call('GET').json['views'][0]['title'],'My Öland map')
        with connect(self.path,readonly=True) as db:
            self.assertEqual(db.execute('SELECT owner_id FROM saved_map_views').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0],0)

    def test_other_users_including_admin_cannot_enumerate_read_or_delete(self):
        identifier=self.save().json['view']['id']
        self.assertEqual(self.call('GET',user=2).json['views'],[])
        for method in ['GET','DELETE']:self.assertEqual(self.call(method,'/'+identifier,user=2).status_code,404)
        self.assertEqual(self.call('DELETE','/'+identifier).status_code,200)
        self.assertEqual(self.call('GET','/'+identifier).status_code,404)

    def test_private_list_authorization_is_checked_at_save_and_open(self):
        self.assertEqual(self.save(user=2).status_code,404)
        state=copy.deepcopy(STATE);state['list_id']='public';self.assertEqual(self.save(state,user=2).status_code,201)
        identifier=self.save().json['view']['id']
        with connect(self.path) as db:db.execute("UPDATE lists SET owner_id=2 WHERE id='private'")
        self.assertFalse(self.call('GET').json['views'][0]['available'])
        self.assertEqual(self.call('GET','/'+identifier).status_code,404)
        self.assertEqual(self.call('DELETE','/'+identifier).status_code,200)

    def test_unauthenticated_sessions_cannot_use_views(self):
        identifier=self.save().json['view']['id']
        for method,path in [('GET',''),('GET','/'+identifier),('DELETE','/'+identifier)]:self.assertEqual(self.call(method,path,user=None).status_code,401)
        self.assertEqual(self.save(user=None).status_code,401)

    def test_invalid_coordinates_ranges_and_nonfinite_numbers_are_rejected(self):
        for key,value in [('grid_zoom',22),('grid_zoom',1.5),('year_from',2027),('slot_center',49),('slot_radius',True),('use_window',1),('base_layer','javascript:alert(1)'),('selected',dict(x=-1,y=0,zoom=15)),('species_stops',[float('nan')])]:
            with self.subTest(key=key):
                state=copy.deepcopy(STATE);state[key]=value;self.assertEqual(self.save(state).status_code,400)
        for zoom in [1,21]:
            state=copy.deepcopy(STATE);state['grid_zoom']=zoom;state['selected']=dict(x=0,y=2**zoom-1,zoom=zoom)
            self.assertEqual(self.save(state).status_code,201)

    def test_csrf_size_and_per_owner_limit(self):
        identifier=self.save().json['view']['id']
        for method,path in [('POST',''),('DELETE','/'+identifier)]:
            response=self.client.open('/geomap-api/views'+path,method=method,headers={'X-Test-User':'1','Origin':'https://other.test'},json={'title':'x','state':STATE})
            self.assertEqual(response.status_code,403)
        self.assertEqual(self.call('POST',json={'title':'x'*9000,'state':STATE}).status_code,400)
        with connect(self.path) as db:
            db.executemany('INSERT INTO saved_map_views(id,owner_id,title,list_id,state_json) VALUES(?,1,?,?,?)',[(str(i),'fixture','public','{}') for i in range(99)])
        self.assertEqual(self.save().status_code,409)
        state=copy.deepcopy(STATE);state['list_id']='public';self.assertEqual(self.save(state,user=2).status_code,201)

if __name__=='__main__':unittest.main()
