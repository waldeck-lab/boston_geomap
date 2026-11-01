"""Exercise registration cookies through the real GeoMap owner checks."""
import io
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
sys.path.insert(0,'/proj/boston/beta/wcr/wcr/backend/src/app')
from beta_accounts import register_beta_accounts
from geomap.shared_store import initialize,connect
from server.shared_api import register_shared_api

class BetaIdentityIntegrationTests(unittest.TestCase):
    def test_two_registered_accounts_cannot_read_each_others_list(self):
        with tempfile.TemporaryDirectory(prefix='identity-test-',dir='/proj/boston/beta/wcr/stage') as directory:
            temp=Path(directory);auth_db=temp/'accounts.sqlite';shared=temp/'shared.sqlite';initialize(shared)
            source=sqlite3.connect('file:/proj/boston/beta/wcr/stage/auth/accounts.sqlite?mode=ro',uri=True)
            target=sqlite3.connect(auth_db);source.backup(target);source.close()
            with target:
                target.execute('DELETE FROM user_groups');target.execute('DELETE FROM users');target.execute('DELETE FROM auth_rate_limits')
            target.close()
            settings=temp/'auth.json';settings.write_text(json.dumps(dict(database=str(auth_db),session_secret='fixture-secret',internal_secret='fixture-internal')))
            (temp/'lepidoptera-species.json').write_text(json.dumps(dict(retrieved_at='fixture',source_sha256='fixture',
                species=[dict(taxon_id=i,scientific_name=f'Species {i}',swedish_name='') for i in [1,2]],species_mapping={'1':1,'2':2})))
            with patch.dict(os.environ,{'GEOMAP_SHARED_DB':str(shared),'GEOMAP_DEFAULT_LISTS_DIR':str(temp),'WCR_BETA_AUTH_SETTINGS':str(settings)}):
                app=Flask(__name__);register_beta_accounts(app,settings);register_shared_api(app)
                owner=app.test_client();other=app.test_client()
                for client,name in [(owner,'fixture-owner'),(other,'fixture-other')]:
                    r=client.post('/api/auth/register',json=dict(username=name,password='fixture-password',password_confirmation='fixture-password'),base_url='https://beta.waldeck.se')
                    self.assertEqual(r.status_code,201)
                response=owner.post('/geomap-api/lists/from-observed-csv',data={'file':(io.BytesIO(b'TaxonId\n1'),'fixture.csv'),'title':'Private fixture','save':'true'},base_url='https://beta.waldeck.se')
                self.assertEqual(response.status_code,200)
                list_id=response.json['list']['id']
                self.assertEqual(owner.get('/geomap-api/lists',base_url='https://beta.waldeck.se').json['lists'][0]['id'],list_id)
                self.assertEqual(other.get('/geomap-api/lists',base_url='https://beta.waldeck.se').json['lists'],[])
                self.assertEqual(other.get('/geomap-api/lists/'+list_id+'/csv',base_url='https://beta.waldeck.se').status_code,404)
                self.assertEqual(other.get('/geomap-api/hotmap?list_id='+list_id,base_url='https://beta.waldeck.se').status_code,404)
                points='/geomap-api/cell/observations?zoom=15&x=17585&y=10249&list_id='+list_id
                self.assertEqual(owner.get(points,base_url='https://beta.waldeck.se').status_code,200)
                self.assertEqual(other.get(points,base_url='https://beta.waldeck.se').status_code,404)

                with connect(shared,readonly=True) as db:
                    self.assertEqual(db.execute('SELECT owner_id FROM lists').fetchone()[0],1)

if __name__=='__main__':unittest.main()
