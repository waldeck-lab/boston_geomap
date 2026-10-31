"""Build a reproducible CSV comparison universe from the stored taxon response."""
import hashlib
import json
import unicodedata
from pathlib import Path

root=Path('/proj/boston/beta/wcr/stage/lists/defaults')
raw=(root/'redlisted-lepidoptera/source/complete-run.json').read_bytes()
source=json.loads(raw)
taxa={int(t['taxonId']):t for t in source['taxa']}
def accepted(t):
    return t.get('status',{}).get('value')=='Accepted' and t.get('type',{}).get('value')=='Taxonomic'
species={tid:t for tid,t in taxa.items() if accepted(t) and t.get('category',{}).get('value')=='Species'}
def name(t,kind):
    return next((n['name'] for n in t.get('names',[]) if n.get('category',{}).get('value')==kind and n.get('isRecommended')),'')
mapping={}
for tid,t in taxa.items():
    if not accepted(t):continue
    seen=set();node=tid
    while node in taxa and node not in seen:
        seen.add(node)
        if node in species:
            mapping[str(tid)]=node;break
        node=taxa[node].get('parentId')
def key(value):
    return ' '.join(unicodedata.normalize('NFKC',value).casefold().split())
name_index={};non_species_names={'lepidoptera'}  # The queried root is absent from childids.
for tid,t in taxa.items():
    for n in t.get('names',[]):
        if n.get('category',{}).get('value') not in {'ScientificName','SwedishName'}:continue
        if not n.get('name'):continue
        normalized=key(n['name'])
        if str(tid) in mapping:
            name_index.setdefault(normalized,set()).add(mapping[str(tid)])
        elif t.get('status',{}).get('value')=='Accepted':
            non_species_names.add(normalized)
result=dict(retrieved_at=source['retrieved_at'],source_sha256=hashlib.sha256(raw).hexdigest(),
    species=[dict(taxon_id=tid,scientific_name=name(t,'ScientificName'),swedish_name=name(t,'SwedishName')) for tid,t in sorted(species.items())],
    species_mapping=mapping,name_index={k:sorted(v) for k,v in sorted(name_index.items())},
    non_species_names=sorted(non_species_names))
(root/'lepidoptera-species.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(dict(species=len(species),mappable_taxa=len(mapping))))
