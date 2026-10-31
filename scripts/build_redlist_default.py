#!/usr/bin/env python3
"""Build a non-personal Lepidoptera default CSV from public Dyntaxa taxon data.

Requires ARTDATABANKEN_SUBSCRIPTION_KEY, never a personal Authorization token.
Only the explicitly selected output directory is written. No observations or
geoboxes are fetched or built. Cached responses support reproducible offline runs.
"""
import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = 'https://api.artdatabanken.se/taxonservice/v1'
CATEGORIES = {'NT', 'VU', 'EN', 'CR', 'RE', 'DD'}
OTHER_CATEGORIES = {'LC', 'NE', 'NA'}
FIELDS = ['taxon_id', 'scientific_name', 'swedish_name', 'observations_count',
          'redlist_category']


def category_code(value):
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError('Unexpected redlistCategory format')
    match = re.search(r'\(([A-Z]{2})\)\s*$', value)
    code = match.group(1) if match else value.strip()
    if code not in CATEGORIES | OTHER_CATEGORIES:
        raise ValueError(f'Unknown red-list category: {value!r}')
    return code


def preferred_name(taxon, kind):
    names = [n for n in taxon.get('names', [])
             if (n.get('category') or {}).get('value') == kind]
    preferred = [n for n in names if n.get('isRecommended')]
    return ((preferred or names or [{}])[0].get('name') or '')


def select_rows(taxa):
    rows = {}
    for taxon in taxa:
        if any((taxon.get(k) or {}).get('value') != v for k, v in
               [('category', 'Species'), ('type', 'Taxonomic'), ('status', 'Accepted')]):
            continue
        if category_code(taxon.get('redlistCategory')) not in CATEGORIES:
            continue
        tid = int(taxon['taxonId'])
        if tid <= 0 or tid in rows:
            raise ValueError('Invalid or duplicate species taxon ID')
        name = preferred_name(taxon, 'ScientificName')
        if not name:
            raise ValueError(f'Missing scientific name for {tid}')
        rows[tid] = dict(taxon_id=tid, scientific_name=name,
            swedish_name=preferred_name(taxon, 'SwedishName'),
            observations_count='', redlist_category=taxon['redlistCategory'])
    return [rows[k] for k in sorted(rows)]


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--offline', action='store_true', help='Rebuild from a complete saved API run')
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    raw = output / 'source'
    raw.mkdir(exist_ok=True)
    source_file = raw / 'complete-run.json'
    if args.offline:
        source = json.loads(source_file.read_text(encoding='utf-8'))
        taxa = source['taxa']
    else:
        key = os.environ.get('ARTDATABANKEN_SUBSCRIPTION_KEY')
        if not key:
            parser.error('ARTDATABANKEN_SUBSCRIPTION_KEY is required')
        session = requests.Session()
        session.headers.update({'Ocp-Apim-Subscription-Key': key, 'Accept': 'application/json'})

        def fetch(method, path, **kwargs):
            for attempt in range(4):
                response = session.request(method, BASE+path, timeout=(10, 90), **kwargs)
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < 3:
                        time.sleep(min(30, int(response.headers.get('Retry-After', '2'))))
                        continue
                response.raise_for_status()
                return response.json()
            raise RuntimeError('Taxon API request failed')

        names = fetch('GET', '/taxa/names', params=dict(searchString='Lepidoptera',
            searchFields='Both', culture='sv_SE', page=1, pageSize=100))
        roots = {int(x['taxonInformation']['taxonId']) for x in names['data']
                 if x.get('taxonInformation', {}).get('recommendedScientificName') == 'Lepidoptera'
                 and (x.get('category') or {}).get('value') == 'Order'
                 and (x.get('type') or {}).get('value') == 'Taxonomic'
                 and (x.get('status') or {}).get('value') == 'Accepted'}
        if len(roots) != 1:
            raise RuntimeError('Cannot identify a unique accepted Lepidoptera root')
        root = roots.pop()
        children = fetch('GET', f'/taxa/{root}/childids', params={'useMainChildren': 'false'})
        ids = children if isinstance(children, list) else children.get('taxonIds', children.get('data', []))
        ids = sorted(set(int(x) for x in ids))
        if not ids:
            raise RuntimeError('Empty Lepidoptera taxonomy response')
        taxa = []
        for start in range(0, len(ids), 200):
            batch = ids[start:start+200]
            result = fetch('POST', '/taxa', params={'culture': 'sv_SE'},
                json={'taxonIds': batch}, headers={'Content-Type': 'application/json-patch+json'})
            if not isinstance(result, list) or {int(t['taxonId']) for t in result} != set(batch):
                raise RuntimeError('Incomplete taxon batch; previous CSV is left intact')
            taxa.extend(result)
            print(f'Taxa fetched: {len(taxa)}/{len(ids)}', flush=True)
            time.sleep(.15)
        source = dict(api_base=BASE, root_taxon_id=root,
            retrieved_at=datetime.now(timezone.utc).isoformat(), child_ids=ids, taxa=taxa)
        write_json(raw/'names.json', names)
        write_json(raw/'childids.json', children)
        write_json(source_file, source)
    rows = select_rows(taxa)
    if not rows:
        raise RuntimeError('No red-listed species; previous CSV is left intact')
    csv_path = output/'redlisted_lepidoptera.csv'
    temporary = csv_path.with_suffix('.csv.tmp')
    with temporary.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(csv_path)
    metadata = dict(title='Rödlistade fjärilar i Sverige', source='SLU Artdatabanken — Dyntaxa Taxon Service API',
        api_base=BASE, retrieved_at=source['retrieved_at'], root_taxon_id=source['root_taxon_id'],
        redlist_edition=None, redlist_edition_note='API redlistCategory does not identify the assessment edition; do not label this snapshot as a specific red-list year.',
        categories=sorted(CATEGORIES), taxa_count=len(rows),
        counts_by_category=dict(sorted(Counter(category_code(r['redlist_category']) for r in rows).items())),
        csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        source_sha256=hashlib.sha256(source_file.read_bytes()).hexdigest(),
        personal_observations_used=False, personal_authorization_used=False,
        scope='Accepted taxonomic Species below Lepidoptera; excludes subspecies, synonyms, LC, NE and NA.',
        definition_reference='https://www.slu.se/miljoanalys/statistik-och-miljodata/miljodatakatalogen/den-svenska-rodlistan/')
    write_json(output/'redlisted_lepidoptera.meta.json', metadata)
    print(json.dumps({k:metadata[k] for k in ['taxa_count','counts_by_category','retrieved_at','redlist_edition']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
