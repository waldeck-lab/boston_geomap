"""Private, owner-scoped map settings; observation data is never copied."""
import json
import math
import uuid
from flask import jsonify, request
from geomap.shared_store import connect

SCHEMA = '''CREATE TABLE IF NOT EXISTS saved_map_views (
 id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL, title TEXT NOT NULL,
 list_id TEXT NOT NULL, state_json TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
 CREATE INDEX IF NOT EXISTS saved_map_views_owner ON saved_map_views(owner_id,created_at);'''


def valid_state(value):
    def number(v, low, high, integer=False):
        if isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or not low<=v<=high or (integer and int(v)!=v):
            raise ValueError('Ogiltiga kartinställningar.')
        return int(v) if integer else v
    if not isinstance(value,dict) or value.get('version')!=1:raise ValueError('Ogiltiga kartinställningar.')
    list_id=value.get('list_id')
    if not isinstance(list_id,str) or not 1<=len(list_id)<=128:raise ValueError('Ogiltiga kartinställningar.')
    camera=value.get('camera')
    if not isinstance(camera,dict):raise ValueError('Ogiltiga kartinställningar.')
    center=camera.get('center')
    if not isinstance(center,list) or len(center)!=2:raise ValueError('Ogiltiga kartinställningar.')
    out=dict(version=1,list_id=list_id,camera=dict(center=[number(center[0],-180,180),number(center[1],-85.051129,85.051129)],
        zoom=number(camera.get('zoom'),0,22),bearing=number(camera.get('bearing'),-180,180),pitch=number(camera.get('pitch'),0,85)),
        grid_zoom=number(value.get('grid_zoom'),1,21,True),year_from=number(value.get('year_from'),1900,2050,True),
        year_to=number(value.get('year_to'),1900,2050,True),slot_center=number(value.get('slot_center'),0,48,True),
        slot_radius=number(value.get('slot_radius'),0,24,True))
    if out['year_from']>out['year_to']:raise ValueError('Ogiltiga kartinställningar.')
    for key in ['use_window','show_phenology']:
        if not isinstance(value.get(key),bool):raise ValueError('Ogiltiga kartinställningar.')
        out[key]=value[key]
    if value.get('base_layer') not in {'osm','satellite','topo'}:raise ValueError('Ogiltiga kartinställningar.')
    out['base_layer']=value['base_layer']
    stops=value.get('species_stops')
    if not isinstance(stops,list) or not 1<=len(stops)<=32:raise ValueError('Ogiltiga kartinställningar.')
    out['species_stops']=sorted(set(number(x,0.001,1000000) for x in stops))
    selected=value.get('selected')
    out['selected']=None
    if selected is not None:
        if not isinstance(selected,dict):raise ValueError('Ogiltiga kartinställningar.')
        z=out['grid_zoom'];out['selected']=dict(zoom=z,x=number(selected.get('x'),0,2**z-1,True),y=number(selected.get('y'),0,2**z-1,True))
        if selected.get('zoom')!=z:raise ValueError('Ogiltiga kartinställningar.')
    return out


def register_saved_views(app,path,current_auth):
    with connect(path) as db:db.executescript(SCHEMA)

    def owner():
        auth=current_auth()
        return (auth.get('user') or {}).get('id') if auth.get('authenticated') else None

    def accessible(db,list_id,uid):
        return db.execute("SELECT 1 FROM lists WHERE id=? AND (visibility='public' OR owner_id=?)",(list_id,uid)).fetchone() is not None

    @app.route('/geomap-api/views',methods=['GET','POST'])
    def saved_views():
        uid=owner()
        if not isinstance(uid,int):return jsonify(error='Logga in för att använda sparade kartvyer.'),401
        if request.method=='GET':
            with connect(path,readonly=True) as db:
                rows=db.execute('SELECT id,title,list_id,created_at FROM saved_map_views WHERE owner_id=? ORDER BY created_at DESC,id DESC',(uid,)).fetchall()
                return jsonify(views=[dict(r,available=accessible(db,r['list_id'],uid)) for r in rows])
        if request.headers.get('Origin') not in {None,'https://beta.waldeck.se'}:return jsonify(error='Använd kartformuläret på betan.'),403
        if not request.is_json or request.content_length is None or request.content_length>8192:return jsonify(error='Ogiltiga kartinställningar.'),400
        data=request.get_json(silent=True)
        if not isinstance(data,dict):return jsonify(error='Ogiltiga kartinställningar.'),400
        title=data.get('title')
        if not isinstance(title,str) or not title.strip() or len(title)>120 or any(ord(c)<32 for c in title):return jsonify(error='Ange ett namn på vyn med högst 120 tecken.'),400
        try:state=valid_state(data.get('state'))
        except ValueError as exc:return jsonify(error=str(exc)),400
        with connect(path) as db:
            db.execute('BEGIN IMMEDIATE')
            if not accessible(db,state['list_id'],uid):return jsonify(error='Listan är inte längre tillgänglig.'),404
            if db.execute('SELECT count(*) FROM saved_map_views WHERE owner_id=?',(uid,)).fetchone()[0]>=100:return jsonify(error='Du kan spara högst 100 kartvyer.'),409
            identifier=uuid.uuid4().hex
            db.execute('INSERT INTO saved_map_views(id,owner_id,title,list_id,state_json) VALUES(?,?,?,?,?)',
                (identifier,uid,title.strip(),state['list_id'],json.dumps(state,allow_nan=False)))
        return jsonify(view=dict(id=identifier,title=title.strip(),state=state)),201

    @app.route('/geomap-api/views/<identifier>',methods=['GET','DELETE'])
    def saved_view(identifier):
        uid=owner()
        if not isinstance(uid,int):return jsonify(error='Logga in för att använda sparade kartvyer.'),401
        if request.method=='DELETE' and request.headers.get('Origin') not in {None,'https://beta.waldeck.se'}:return jsonify(error='Använd kartformuläret på betan.'),403
        with connect(path,readonly=request.method=='GET') as db:
            row=db.execute('SELECT id,title,list_id,state_json FROM saved_map_views WHERE id=? AND owner_id=?',(identifier,uid)).fetchone()
            if row is None:return jsonify(error='Kartvyn hittades inte.'),404
            if request.method=='DELETE':
                db.execute('DELETE FROM saved_map_views WHERE id=? AND owner_id=?',(identifier,uid))
                return jsonify(ok=True)
            if not accessible(db,row['list_id'],uid):return jsonify(error='Listan är inte längre tillgänglig.'),404
            return jsonify(view=dict(id=row['id'],title=row['title'],state=json.loads(row['state_json'])))
