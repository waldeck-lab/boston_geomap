"""Read-only species documentation and exact name matching for the NRM index."""
import json
import re
import sqlite3
import unicodedata
from collections import defaultdict
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin,urlsplit,unquote

NRM_INDEX='http://www3.nrm.se/en/svenska_fjarilar/svenska_fjarilar.html'

def normalized(value):
    return ' '.join(unicodedata.normalize('NFKC',value).casefold().split())

def valid_reference_url(source,url):
    if not isinstance(url,str):return False
    p=urlsplit(url)
    if p.username or p.password or p.query or p.fragment:return False
    if source=='artfakta':
        return p.scheme=='https' and p.netloc=='artfakta.se' and re.fullmatch(r'/taxa/[0-9]+/information',p.path) is not None
    if source=='nrm':
        return p.scheme in {'http','https'} and p.netloc=='www3.nrm.se' and re.fullmatch(r'/en/svenska_fjarilar/[a-z]/[a-z]+_[a-z-]+\.html',p.path) is not None
    return False

class NrmIndex(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True);self.rows=[];self.current=None;self.inside=False
    def finish(self):
        if self.current:
            row=self.current
            url=urljoin(NRM_INDEX,row['href'])
            match=re.fullmatch(r'/en/svenska_fjarilar/[a-z]/([a-z]+)_([a-z-]+)\.html',unquote(urlsplit(url).path))
            if match and valid_reference_url('nrm',url):
                genus,epithet=match.groups()
                label=normalized(''.join(row['label']))
                if epithet not in {'genitalia','nyckel','larv','pupa','imago','index'}:
                    names={genus+' '+epithet}
                    # Updated Latin gender endings can differ from the historical filename.
                    if re.fullmatch(r'[a-z][a-z-]+',label):names.add(genus+' '+label)
                    elif re.fullmatch(r'[a-z]+ [a-z][a-z-]+',label):names.add(label)
                    self.rows.append(dict(name=genus+' '+epithet,names=sorted(names),url=url,context=normalized(''.join(row['tail']))))
        self.current=None;self.inside=False
    def handle_starttag(self,tag,attrs):
        if tag=='a':
            self.finish();href=dict(attrs).get('href')
            if href:self.current=dict(href=href,label=[],tail=[]);self.inside=True
        elif tag=='br':self.finish()
    def handle_endtag(self,tag):
        if tag=='a':self.inside=False
        elif tag in {'ol','body'}:self.finish()
    def handle_data(self,value):
        if self.current:self.current['label' if self.inside else 'tail'].append(value)

def match_nrm(catalogue,index_text):
    species={int(r['taxon_id']):r for r in catalogue['species']}
    preferred=defaultdict(set)
    for tid,row in species.items():preferred[normalized(row['scientific_name'])].add(tid)
    parser=NrmIndex();parser.feed(index_text);parser.finish()
    matched=defaultdict(list);ambiguous=defaultdict(list)
    for record in parser.rows:
        candidates=set().union(*(set(catalogue['name_index'].get(name,[])) for name in record['names'])) & species.keys()
        direct=set().union(*(preferred.get(name,set()) for name in record['names']))
        if len(candidates)>1:
            swedish={tid for tid in candidates if species[tid]['swedish_name'] and re.search(r'(?<!\w)'+re.escape(normalized(species[tid]['swedish_name']))+r'(?!\w)',record['context'])}
            if len(swedish)==1:candidates=swedish;method='scientific-and-swedish'
            elif len(direct)==1:candidates=direct;method='current-scientific-name'
            else:
                for tid in candidates:ambiguous[tid].append(record)
                continue
        else:method='current-scientific-name' if direct else 'synonym'
        if len(candidates)==1:
            tid=next(iter(candidates))
            matched_name=next((name for name in record['names'] if tid in preferred.get(name,set())),record['name'])
            matched[tid].append(dict(record,name=matched_name,method=method,priority=0 if tid in direct else 1))
    output={}
    for tid in species:
        matches=matched[tid]
        if matches:
            priority=min(r['priority'] for r in matches)
            best={r['url']:r for r in matches if r['priority']==priority}
            if len(best)==1:
                row=next(iter(best.values()));output[tid]=dict(url=row['url'],status='indexed',matched_name=row['name'],match_method=row['method'])
            else:output[tid]=dict(url=None,status='ambiguous',candidates=sorted(best))
        else:output[tid]=dict(url=None,status='ambiguous' if ambiguous[tid] else 'not_indexed',candidates=sorted({r['url'] for r in ambiguous[tid]}))
    return output,len(parser.rows)

def version(path):
    if not path:return None
    try:
        stat=Path(path).stat();return (stat.st_ino,stat.st_mtime_ns,stat.st_size)
    except FileNotFoundError:return None

def read_references(path,taxon_ids):
    ids=sorted(set(int(tid) for tid in taxon_ids))
    if not ids or not path or not version(path):return {}
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
    try:
        output={}
        for start in range(0,len(ids),500):
            batch=ids[start:start+500]
            rows=db.execute('SELECT taxon_id,source,url,status,matched_name,match_method FROM documentation WHERE taxon_id IN ('+','.join('?' for _ in batch)+')',batch)
            for row in rows:
                data=dict(row);tid=data.pop('taxon_id');source=data.pop('source')
                if data['url'] and not valid_reference_url(source,data['url']):data['url']=None;data['status']='invalid_url'
                output.setdefault(tid,{})[source]=data
        return output
    finally:db.close()
