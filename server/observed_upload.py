"""Authenticated beta CSV preview/save and list download routes."""
import csv
import io
import os
from pathlib import Path

from flask import jsonify, request, Response
from server.personal_access import require_personal_access
from geomap.observed_csv import MAX_BYTES, CsvValidationError, build_difference, save_difference
from geomap.shared_store import connect


def register_observed_upload(app, db_path, current_auth):
    catalogue=Path(os.environ.get('GEOMAP_DEFAULT_LISTS_DIR', '/proj/boston/beta/wcr/stage/lists/defaults'))/'lepidoptera-species.json'

    @app.post('/geomap-api/lists/from-observed-csv')
    def observed_csv_upload():
        auth=current_auth()
        user=auth.get('user') or {}
        if not auth.get('authenticated') or not user.get('id'):
            return jsonify(ok=False,error='Logga in för att skapa en personlig lista.'),401
        require_personal_access(auth)
        if request.headers.get('Origin') not in {None,*os.environ.get('WCR_ALLOWED_ORIGINS','https://beta.waldeck.se').split(',')}:
            return jsonify(ok=False,error='Uppladdningen måste göras från webbplatsen.'),403
        if request.content_length is None or request.content_length>MAX_BYTES+65536:
            return jsonify(ok=False,error='CSV-filen får vara högst 2 MiB.'),413
        upload=request.files.get('file')
        if not upload:
            return jsonify(ok=False,error='Välj en CSV-fil.'),400
        try:
            rows,observed,summary=build_difference(upload.read(MAX_BYTES+1),catalogue)
            result=dict(ok=True,preview=summary)
            if request.form.get('save')=='true':
                result['list']=save_difference(db_path,user['id'],request.form.get('title','Mina saknade arter'),rows,observed,summary)
            response=jsonify(result)
            response.headers['Cache-Control']='no-store'
            return response
        except CsvValidationError as exc:
            return jsonify(ok=False,error=str(exc),details=exc.details),400

    @app.get('/geomap-api/lists/<list_id>/csv')
    def download_list(list_id):
        with connect(db_path,readonly=True) as db:
            chosen=db.execute('SELECT * FROM lists WHERE id=?',(list_id,)).fetchone()
            if chosen is None:return jsonify(ok=False,error='Listan hittades inte.'),404
            if chosen['visibility']=='private':
                auth=current_auth()
                if not auth.get('authenticated') or (auth.get('user') or {}).get('id')!=chosen['owner_id']:
                    return jsonify(ok=False,error='Listan hittades inte.'),404
                require_personal_access(auth)
            rows=db.execute('''SELECT t.taxon_id,t.scientific_name,t.swedish_name FROM taxa t
                JOIN list_taxa l ON t.taxon_id=l.taxon_id WHERE l.list_id=? ORDER BY t.taxon_id''',(list_id,)).fetchall()
        output=io.StringIO(newline='');writer=csv.writer(output)
        writer.writerow(['taxon_id','scientific_name','swedish_name'])
        writer.writerows(rows)
        response=Response(output.getvalue(),mimetype='text/csv; charset=utf-8')
        response.headers['Content-Disposition']='attachment; filename="taxon-list.csv"'
        response.headers['Cache-Control']='no-store'
        return response
