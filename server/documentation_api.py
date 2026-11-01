"""Public read-only access to the beta species reference catalogue."""
import sqlite3
from pathlib import Path
from flask import jsonify,request
from werkzeug.exceptions import BadRequest,NotFound
from geomap.documentation import read_references,version

def register_documentation_api(app,path):
    if not path:return
    path=Path(path)

    @app.get('/geomap-api/documentation')
    def documentation_batch():
        try:ids=sorted({int(value) for value in request.args.get('taxon_ids','').split(',') if value})
        except ValueError as exc:raise BadRequest('Invalid taxon IDs') from exc
        if not ids or len(ids)>2000 or any(tid<=0 for tid in ids):raise BadRequest('Provide 1–2000 positive taxon IDs')
        if not version(path):return jsonify(ok=False,error='Documentation catalogue unavailable'),503
        return jsonify(ok=True,documentation=read_references(path,ids))

    @app.get('/geomap-api/documentation/<int:taxon_id>')
    def documentation_species(taxon_id):
        if not version(path):return jsonify(ok=False,error='Documentation catalogue unavailable'),503
        db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
        try:row=db.execute('SELECT * FROM species WHERE taxon_id=?',(taxon_id,)).fetchone()
        finally:db.close()
        if not row:raise NotFound('Taxon not found in documentation catalogue')
        return jsonify(ok=True,**dict(row),documentation=read_references(path,[taxon_id]).get(taxon_id,{}))
