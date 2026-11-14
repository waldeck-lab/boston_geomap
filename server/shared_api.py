"""Beta read API for the independent shared occurrence store.

Registered as a request dispatcher so legacy write APIs cannot accidentally
write to the old demonstration database while shared-store mode is enabled.
"""
import json
import os
import re
import time
import threading
from collections import OrderedDict
from pathlib import Path

from flask import jsonify, request, Response
from werkzeug.exceptions import BadRequest, NotFound, Forbidden

from server.personal_access import allowed,require_personal_access
from geomap.shared_store import connect, BASE_ZOOM
from geomap.documentation import read_references, version as documentation_version
from geomap.tiles import tile_xy_to_bbox


def register_shared_api(app):
    path=os.environ.get('GEOMAP_SHARED_DB')
    if not path:
        return
    path=Path(path)
    if not path.resolve().is_relative_to(Path(os.environ.get('GEOMAP_DATA_ROOT','/proj/boston/beta/wcr/stage')).resolve()):
        raise RuntimeError('Shared database must be inside the configured data root')
    documentation_path=os.environ.get('GEOMAP_DOCUMENTATION_DB')
    if documentation_path and not Path(documentation_path).resolve().is_relative_to(Path(os.environ.get('GEOMAP_DATA_ROOT','/proj/boston/beta/wcr/stage')).resolve()):
        raise RuntimeError('Documentation database must be inside the configured data root')
    from server.documentation_api import register_documentation_api
    register_documentation_api(app,documentation_path)
    cache=OrderedDict()
    cache_size=0
    cache_lock=threading.RLock()

    def current_auth():
        from auth_client import introspect_current_request
        if not request.cookies:
            return {}
        return introspect_current_request()

    def selected(db):
        list_id=request.args.get('list_id','redlisted-lepidoptera')
        row=db.execute('SELECT * FROM lists WHERE id=?',(list_id,)).fetchone()
        if not row:
            raise NotFound('List not found')
        if row['visibility']=='private':
            auth=current_auth()
            if not auth.get('authenticated') or int((auth.get('user') or {}).get('id',-1))!=row['owner_id']:
                raise NotFound('List not found')
            require_personal_access(auth)
        return row

    from server.observed_upload import register_observed_upload
    register_observed_upload(app,path,current_auth)
    from server.saved_views import register_saved_views
    register_saved_views(app,path,current_auth)

    def filters(cell=False, all_slots=False):
        z=int(request.args.get('zoom',15))
        if not 1<=z<=BASE_ZOOM:
            raise BadRequest('zoom must be 1..21')
        a=int(request.args.get('year_from',0));b=int(request.args.get('year_to',0))
        if a==b==0:a,b=1900,2050
        if not 1900<=a<=b<=2050:raise BadRequest('Invalid year range')
        text=request.args.get('slot_ids',request.args.get('slot_id','0'))
        slots=sorted(set(int(x.strip()) for x in text.split(',') if x.strip()))
        if not slots or any(not 0<=x<=48 for x in slots):raise BadRequest('Invalid seasonal slots')
        conditions=['c.year BETWEEN ? AND ?'];params=[a,b]
        if not all_slots and 0 not in slots:
            conditions.append('c.slot_id IN ('+','.join('?' for x in slots)+')');params+=slots
        shift=BASE_ZOOM-z
        if cell:
            x=int(request.args['x']);y=int(request.args['y'])
            if not(0<=x<2**z and 0<=y<2**z):raise BadRequest('Invalid cell')
            conditions+=['(c.x >> ?) = ?','(c.y >> ?) = ?'];params += [shift,x,shift,y]
        return z,a,b,slots,' AND '.join(conditions),params

    def imported_status():
        p=path.parent/'import-status.json'
        try:
            state=json.loads(p.read_text())
            # Never reveal a private list's size or progress through public polling.
            public={k:state[k] for k in ['busy','date_from','date_to','source_scope'] if k in state}
            public['phase']='importing' if state.get('busy') else state.get('phase','unknown')
            return public
        except FileNotFoundError:return {'busy':False,'phase':'not-started'}

    @app.after_request
    def personal_data_no_cache(response):
        if request.path.startswith("/geomap-api/"):response.headers["Cache-Control"]="no-store"
        return response

    @app.before_request
    def shared_dispatch():
        nonlocal cache_size
        prefix='/geomap-api/'
        if not request.path.startswith(prefix):return None
        endpoint=request.path[len(prefix):]
        if endpoint=='views' or re.fullmatch(r'views/[^/]+',endpoint):return None
        if endpoint=='health':return None
        if endpoint.startswith('taxon-lists/defaults'):return None
        if endpoint=='lists/from-observed-csv' and request.method=='POST':return None
        if re.fullmatch(r'lists/[^/]+/csv',endpoint) and request.method in {'GET','HEAD'}:return None
        if (endpoint=='documentation' or re.fullmatch(r'documentation/[0-9]+',endpoint)) and request.method in {'GET','HEAD'}:return None
        if endpoint=='jobs/status':
            state=imported_status()
            return jsonify(ok=True,busy=state.get('busy',False),current_job=None,last_job=None,data_import=state)
        if request.method not in {'GET','HEAD'}:
            return jsonify(ok=False,error='Shared beta imports are managed separately; legacy write APIs are disabled'),409
        known={'lists','hotmap','hotmap_window','slots/coverage','cell/taxa','cell/taxa_window','cell/phenology','cell/observations'}
        if endpoint not in known:
            return jsonify(ok=False,error='This endpoint is not enabled for the shared beta store'),409
        try:
            with connect(path,readonly=True) as db:
                if endpoint=='lists':
                    auth=current_auth();uid=(auth.get('user') or {}).get('id') if allowed(auth) else None
                    rows=db.execute('''SELECT l.id,l.title,l.visibility,l.content_hash,
                      (SELECT COUNT(*) FROM list_taxa t WHERE t.list_id=l.id) AS taxa_count,
                      (SELECT COUNT(*) FROM list_taxa t WHERE t.list_id=l.id AND NOT EXISTS
                       (SELECT 1 FROM coverage c WHERE c.taxon_id=t.taxon_id
                        AND c.date_from='2000-01-01' AND c.date_to='2026-10-31'
                        AND c.source_scope='public-only-start-v1')) AS taxa_needing_import
                      FROM lists l WHERE l.visibility='public' OR l.owner_id=? ORDER BY l.visibility,l.title''',(uid,)).fetchall()
                    items=[dict(r) for r in rows]
                    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='list_taxonomy_state'").fetchone():
                        for item in items:
                            state=db.execute("SELECT needs_regeneration,updated_at FROM list_taxonomy_state WHERE list_id=?",(item['id'],)).fetchone()
                            if state:
                                item['taxonomy_needs_regeneration']=bool(state[0])
                                item['taxonomy_reviewed_at']=state[1]
                    return jsonify(ok=True,lists=items,default_list_id='redlisted-lepidoptera',data_import=imported_status())
                chosen=selected(db)
                revision=db.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()[0]
                key=(chosen['content_hash'],revision,endpoint,documentation_version(documentation_path) if endpoint in {'cell/taxa','cell/taxa_window'} else None,
                     tuple(sorted((k,v) for k,v in request.args.items() if k!='list_id')))
                with cache_lock:
                    payload=cache.get(key)
                    if payload is not None:cache.move_to_end(key)
                if payload is not None:
                    return Response(payload,mimetype='application/json')
                z,a,b,slots,where,params=filters(cell=endpoint.startswith('cell/'),all_slots=endpoint=='cell/phenology')
                shift=BASE_ZOOM-z
                base=' FROM taxon_cells c JOIN list_taxa l ON l.taxon_id=c.taxon_id WHERE l.list_id=? AND '+where
                bindings=[chosen['id']]+params
                if endpoint in {'hotmap','hotmap_window'}:
                    rows=db.execute('SELECT (c.x >> ?) AS x,(c.y >> ?) AS y,COUNT(DISTINCT c.taxon_id) AS coverage,SUM(c.n) AS obs_total'+base+
                        ' GROUP BY 1,2 ORDER BY coverage DESC,obs_total DESC LIMIT 100001',[shift,shift]+bindings).fetchall()
                    if len(rows)>100000:
                        return jsonify(ok=False,error='Too many cells; choose a lower zoom or narrower date window'),422
                    features=[]
                    for r in rows:
                        top,left,bottom,right=tile_xy_to_bbox(r['x'],r['y'],z)
                        properties=dict(zoom=z,x=r['x'],y=r['y'],slot_id=slots[len(slots)//2],
                            coverage=r['coverage'],obs_total=r['obs_total'],
                            score=r['coverage']**2/(r['obs_total']+1)**.5,year_from=a,year_to=b)
                        features.append(dict(type='Feature',properties=properties,geometry=dict(type='Polygon',
                            coordinates=[[[left,top],[right,top],[right,bottom],[left,bottom],[left,top]]])))
                    result=dict(type='FeatureCollection',features=features,list_content_hash=chosen['content_hash'],data_revision=revision)
                elif endpoint=='slots/coverage':
                    rows=db.execute('SELECT slot_id,COUNT(*) AS cells FROM (SELECT c.slot_id,(c.x >> ?) AS gx,(c.y >> ?) AS gy'+base+
                        ' GROUP BY c.slot_id,gx,gy) GROUP BY slot_id ORDER BY slot_id',[shift,shift]+bindings).fetchall()
                    result=[dict(r) for r in rows]
                elif endpoint=='cell/phenology':
                    rows=db.execute('SELECT c.slot_id,SUM(c.n) AS observations_count,COUNT(DISTINCT c.taxon_id) AS taxa_count'+base+
                                    ' GROUP BY c.slot_id',bindings).fetchall()
                    by_slot={r['slot_id']:dict(r) for r in rows};maximum=max([r['observations_count'] for r in rows] or [0])
                    series=[]
                    for slot in range(1,49):
                        item=by_slot.get(slot,dict(slot_id=slot,observations_count=0,taxa_count=0))
                        item['normalized']=item['observations_count']/maximum if maximum else 0;series.append(item)
                    result=dict(ok=True,slots=series,max_observations_count=maximum,zoom=z,x=int(request.args['x']),y=int(request.args['y']))
                else:
                    # Point details and observed dates use the single canonical observation row.
                    raw_where=re.sub(r'\bc\.([xy])\b',lambda m:'c.tile_'+m.group(1),where)
                    raw_base=' FROM observations c JOIN list_taxa l ON l.taxon_id=c.taxon_id WHERE l.list_id=? AND '+raw_where
                    if endpoint=='cell/observations':
                        auth=current_auth()
                        if not auth.get('authenticated'):
                            raise Forbidden('Sign in to view observation points.')
                        rows=db.execute('SELECT c.taxon_id,c.latitude,c.longitude,COUNT(*) AS observations_count'+raw_base+
                            ' GROUP BY c.taxon_id,c.latitude,c.longitude ORDER BY c.taxon_id,c.latitude,c.longitude LIMIT 2001',bindings).fetchall()
                        result=dict(type='FeatureCollection',truncated=len(rows)>2000,limit=2000,features=[dict(type='Feature',properties=dict(taxon_id=r['taxon_id'],
                            observations_count=r['observations_count']),geometry=dict(type='Point',coordinates=[r['longitude'],r['latitude']])) for r in rows[:2000]])
                    else:
                        limit=max(1,min(2000,int(request.args.get('limit',200))))
                        rows=db.execute('''SELECT c.taxon_id,MAX(t.scientific_name) AS scientific_name,
                           MAX(t.swedish_name) AS swedish_name,COUNT(*) AS observations_count,
                           MIN(c.observation_date) AS first_observed,MAX(c.observation_date) AS last_observed
                           FROM observations c JOIN list_taxa l ON l.taxon_id=c.taxon_id
                           JOIN taxa t ON t.taxon_id=c.taxon_id WHERE l.list_id=? AND '''+raw_where+
                           ' GROUP BY c.taxon_id ORDER BY observations_count DESC LIMIT ?',bindings+[limit]).fetchall()
                        result=[dict(r) for r in rows]
                if endpoint in {'cell/taxa','cell/taxa_window'}:
                    references=read_references(documentation_path,[row['taxon_id'] for row in result])
                    for row in result:row['documentation']=references.get(row['taxon_id'],{})
                payload=app.json.dumps(result).encode()
                # Exact points are never served from a cache before authorization.
                if endpoint!='cell/observations' and len(payload)<=2*1024*1024:
                    with cache_lock:
                        previous=cache.pop(key,None)
                        if previous is not None:cache_size-=len(previous)
                        cache[key]=payload;cache_size+=len(payload)
                        while cache and (cache_size>16*1024*1024 or len(cache)>64):
                            _,old=cache.popitem(last=False);cache_size-=len(old)
                return Response(payload,mimetype='application/json')
        except (ValueError,KeyError) as exc:
            raise BadRequest('Invalid shared-store query parameters') from exc
