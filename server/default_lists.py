"""Public, non-personal default taxon lists. Never expose private list paths."""
import hashlib
import json

from flask import Blueprint, jsonify, send_file


def register_default_lists(app, defaults_root):
    bp = Blueprint('default_taxon_lists', __name__)
    directory = defaults_root / 'redlisted-lepidoptera'

    def verified_metadata():
        csv_path = directory / 'redlisted_lepidoptera.csv'
        metadata = json.loads((directory / 'redlisted_lepidoptera.meta.json').read_text(encoding='utf-8'))
        if hashlib.sha256(csv_path.read_bytes()).hexdigest() != metadata['csv_sha256']:
            raise RuntimeError('Default list checksum mismatch')
        return metadata, csv_path

    @bp.get('/geomap-api/taxon-lists/defaults')
    def catalog():
        try:
            metadata, _ = verified_metadata()
        except FileNotFoundError:
            return jsonify(ok=True, lists=[])
        public = {key: metadata[key] for key in [
            'title', 'source', 'retrieved_at', 'categories', 'taxa_count',
            'counts_by_category', 'csv_sha256', 'redlist_edition', 'redlist_edition_note']}
        public.update(id='redlisted-lepidoptera',
            csv_url='/geomap-api/taxon-lists/defaults/redlisted-lepidoptera.csv',
            personal_observations_used=False)
        return jsonify(ok=True, lists=[public])

    @bp.get('/geomap-api/taxon-lists/defaults/redlisted-lepidoptera.csv')
    def download():
        try:
            metadata, csv_path = verified_metadata()
        except FileNotFoundError:
            return jsonify(ok=False, error='Default list not yet generated'), 404
        return send_file(csv_path, mimetype='text/csv; charset=utf-8',
            as_attachment=True, download_name='redlisted_lepidoptera.csv',
            etag=metadata['csv_sha256'], max_age=0)

    app.register_blueprint(bp)
