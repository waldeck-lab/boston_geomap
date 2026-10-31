"""Beta-only gateway: isolated GeoMap, explicitly limited production WCR access."""
import re
import sys
import http.client
from pathlib import Path
from urllib.parse import urlsplit, urlencode

from flask import Flask, Response, jsonify, request, send_from_directory

ROOT = Path('/proj/boston/beta/wcr')
DIST = ROOT / 'wcr/frontend/dist'
app = Flask(__name__, static_folder=None)
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
sys.path.insert(0, str(ROOT / 'wcr/backend/src/app'))
from beta_accounts import register_beta_accounts
register_beta_accounts(app, ROOT / 'ops/auth-settings.json')
HOP = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
       'te', 'trailer', 'transfer-encoding', 'upgrade', 'content-length'}


def forward(base, path):
    headers = {k: v for k, v in request.headers if k.lower() not in HOP | {'host'}}
    # Never pass a caller-supplied internal auth credential into production WCR.
    headers.pop('X-Internal-Auth', None)
    headers = {k: v for k, v in headers.items() if k.lower() != 'x-internal-auth'}
    headers['X-Forwarded-Proto'] = 'https'
    headers['X-Forwarded-Host'] = 'beta.waldeck.se'
    target = urlsplit(base)
    if target.port == 5000:
        # Beta identities and old production session cookies never authorize WCR reads.
        headers = {k: v for k, v in headers.items() if k.lower() not in {'cookie', 'authorization'}}
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=120)
    try:
        query = urlencode(list(request.args.items(multi=True)))
        connection.request(request.method, path + ('?' + query if query else ''),
                           body=request.get_data(), headers=headers)
        upstream = connection.getresponse()
        response = Response(upstream.read(), status=upstream.status)
        for k, v in upstream.getheaders():
            if k.lower() not in HOP:
                if k.lower() == 'set-cookie':
                    response.headers.add(k, v)
                else:
                    response.headers[k] = v
    except (OSError, http.client.HTTPException):
        return jsonify(error='Beta upstream unavailable'), 502
    finally:
        connection.close()
    return response


@app.after_request
def beta_headers(response):
    response.headers['X-WCR-Environment'] = 'beta'
    response.headers['X-Robots-Tag'] = 'noindex, nofollow'
    response.headers['Cache-Control'] = 'no-store'
    return response


@app.route('/api', defaults={'path': ''}, methods=['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
@app.route('/api/<path:path>', methods=['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
def wcr_api(path):
    if any(part in {'.', '..'} for part in path.split('/')) or '%' in path or '\\' in path:
        return jsonify(error='Invalid API path'), 400
    read = request.method in {'GET', 'HEAD'} and (
        re.fullmatch(r'(dissections|highlights)(/\d+)?', path) or
        re.fullmatch(r'(dissections|highlights)/\d+/images', path) or
        re.fullmatch(r'(dissections|highlights)/meta/[a-z-]+', path) or
        re.fullmatch(r'(dissections|highlights)/select', path) or
        re.fullmatch(r'dissections/tag/[^/]+', path) or
        re.fullmatch(r'images/(dissections|highlights)/.+', path))
    if not read:
        return jsonify(error='Production WCR operation blocked in beta'), 403
    # Production thumbnail GETs may generate files. Serve original images instead.
    path = re.sub(r'^images/(dissections|highlights)/thumbs/', r'images/\1/', path)
    return forward('http://127.0.0.1:5000', '/api/' + path)


@app.route('/geomap-api', defaults={'path': ''}, methods=['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
@app.route('/geomap-api/<path:path>', methods=['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
def geomap_api(path):
    return forward('http://127.0.0.1:8089', '/geomap-api/' + path)


@app.get('/__beta/health')
def health():
    return jsonify(ok=True, environment='beta', geomap_port=8089,
                   production_wcr='limited proxy', frontend_present=(DIST/'index.html').is_file())


@app.get('/', defaults={'path': ''})
@app.get('/<path:path>')
def frontend(path):
    if path.startswith(('api/', 'geomap-api/')):
        return jsonify(error='Unknown API route'), 404
    resolved = (DIST / path).resolve()
    if path and resolved.is_relative_to(DIST.resolve()) and resolved.is_file():
        return send_from_directory(DIST, path)
    return send_from_directory(DIST, 'index.html')
