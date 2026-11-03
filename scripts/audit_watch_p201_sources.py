"""Cache public NRM/Lepiforum pages for a reproducible, manual ecology review."""
import argparse,concurrent.futures,hashlib,json,re,threading,time
from pathlib import Path
from html.parser import HTMLParser
import requests
class Text(HTMLParser):
    def __init__(self):super().__init__();self.parts=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {'script','style'}:self.hidden+=1
        elif tag in {'p','br','h1','h2','h3','li','tr'}:self.parts.append('\n')
    def handle_endtag(self,tag):
        if tag in {'script','style'} and self.hidden:self.hidden-=1
    def handle_data(self,data):
        if not self.hidden:self.parts.append(data)
ap=argparse.ArgumentParser();ap.add_argument('--stage',type=Path,required=True);args=ap.parse_args();stage=args.stage.resolve()
if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')):raise RuntimeError('Beta stage required')
data=json.loads((stage/'pilot-export.json').read_text());cache=stage/'research/source';cache.mkdir(parents=True,exist_ok=True)
stop=threading.Event()
def fetch(job):
    name,label,url=job;filename=name.lower().replace(' ','-')+'.'+label;raw=cache/(filename+'.html')
    if stop.is_set():return dict(scientific_name=name,source=label,url=url,status='rate-limit-stop')
    try:
        if raw.exists():body=raw.read_bytes();status=200
        else:
            response=requests.get(url,timeout=22,headers={'User-Agent':'WCR ecology source review (beta.waldeck.se; low-rate public page retrieval)'})
            status=response.status_code;body=response.content
            if status==429:stop.set()
            if status!=200:return dict(scientific_name=name,source=label,url=url,status=status)
            raw.write_bytes(body)
        encoding="utf-8" if label=="lepiforum" or re.search(br"charset\s*=\s*[\"']?utf-8",body,re.I) else "latin1"
        parser=Text();parser.feed(body.decode(encoding,errors="replace"))
        text="\n".join(re.sub(r"\s+"," ",line).strip() for line in "".join(parser.parts).splitlines() if line.strip())
        (cache/(filename+'.txt')).write_text(text,encoding='utf8')
        return dict(scientific_name=name,source=label,url=url,status=status,sha256=hashlib.sha256(body).hexdigest(),text_file=str(cache/(filename+'.txt')))
    except Exception as exc:return dict(scientific_name=name,source=label,url=url,status=type(exc).__name__)
    finally:time.sleep(.35)
jobs=[]
for p in data['profiles']+data.get('references',[]):
    name=p['scientific_name'];nrm=next((s['url'] for s in (p.get('search_guide') or {}).get('sources',[]) if s.get('id')=='nrm'),None)
    nrm=nrm or 'http://www3.nrm.se/en/svenska_fjarilar/'+name[0].lower()+'/'+name.lower().replace(' ','_')+'.html'
    jobs.extend([(name,'nrm',nrm),(name,'lepiforum',(p.get('supplement') or {}).get('source_url') or 'https://lepiforum.org/wiki/page/'+name.replace(' ','_'))])
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(fetch,jobs))
(stage/'research/source-index.json').write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'pages':len(results),'retrieved':sum(x['status']==200 for x in results),'failed':[dict(scientific_name=x['scientific_name'],source=x['source'],status=x['status']) for x in results if x['status']!=200]},ensure_ascii=False))
