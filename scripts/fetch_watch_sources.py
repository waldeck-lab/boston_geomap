#!/usr/bin/env python3
"""Fetch the pinned, openly licensed input files for the first watch pilot."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path
from datetime import datetime,timezone
import requests

ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--stage',type=Path,required=True);ap.add_argument('--taxonomy',type=Path);args=ap.parse_args()
stage=args.stage.resolve()
if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise RuntimeError('Refusing non-pilot output')
raw=stage/'source';raw.mkdir(parents=True,exist_ok=True);session=requests.Session();session.headers['User-Agent']='WCR-watch-pilot-source-fetcher/0.1'
record=session.get('https://zenodo.org/api/records/18197194',timeout=40);record.raise_for_status();metadata=record.json()
if metadata['id']!=18197194 or metadata['metadata']['license']['id']!='cc-by-4.0':raise RuntimeError('Unexpected version/license')
(raw/'zenodo-record.json').write_text(json.dumps(metadata,indent=2))
manifest={'retrieved_at':datetime.now(timezone.utc).isoformat(),'files':[]}
def download(url,path):
    if not path.exists():
        temp=path.with_name(path.name+'.part')
        with session.get(url,stream=True,timeout=60) as response:
            response.raise_for_status()
            with temp.open('wb') as stream:
                for chunk in response.iter_content(1048576):stream.write(chunk)
        temp.replace(path)
    with path.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
    manifest['files'].append(dict(file=path.name,url=url,sha256=digest))
for f in metadata['files']:
    if f['key'].endswith('.xlsx'):continue
    path=raw/f['key'];download(f['links']['self'],path)
    algorithm,expected=f['checksum'].split(':')
    with path.open('rb') as stream:actual=hashlib.file_digest(stream,algorithm).hexdigest()
    if actual!=expected:raise RuntimeError('Published checksum mismatch: '+path.name)
download('https://data-package.ceh.ac.uk/data/dbc7cc17-cbbd-49dd-bab4-8e8855768d66.zip',raw/'uk-traits-dataset.zip')
if args.taxonomy:shutil.copyfile(args.taxonomy,raw/'dyntaxa-lepidoptera.json')
(raw/'sources-manifest.json').write_text(json.dumps(manifest,indent=2))
print('Fetched and verified pinned pilot inputs:',raw)
