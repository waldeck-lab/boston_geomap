import concurrent.futures,hashlib,html,json,re,time
from html.parser import HTMLParser
from pathlib import Path
import requests
STAGE=Path('/proj/boston/beta/wcr/stage/watch-pilot-20261101');OUT=STAGE/'source/finland-review/species';OUT.mkdir(exist_ok=True)
class Text(HTMLParser):
 def __init__(self):super().__init__();self.lines=[];self.skip=0
 def handle_starttag(self,tag,attrs):
  if tag in ['script','style']:self.skip+=1
  if tag in ['p','li','h1','h2','h3','br','div','tr']:self.lines.append('\n')
 def handle_endtag(self,tag):
  if tag in ['script','style']:self.skip=max(0,self.skip-1)
  if tag in ['p','li','h1','h2','h3','div','tr']:self.lines.append('\n')
 def handle_data(self,data):
  if not self.skip:self.lines.append(data)
def clean(s):
 p=Text();p.feed(s);return '\n'.join(' '.join(l.split()) for l in ''.join(p.lines).splitlines() if l.strip())
def process(row):
 n=row['scientific_name'];slug=n.replace(' ','_');path=OUT/(slug+'.html');url='https://lepiforum.org/wiki/page/'+slug
 try:
  if not path.exists():
   time.sleep(.4);r=requests.get(url,timeout=55);r.raise_for_status();path.write_bytes(r.content)
  raw=path.read_text();text=clean(raw);(OUT/(slug+'.txt')).write_text(text)
  # Keep country-status context plus the editorial biology section, not all photo captions.
  sw=[text[max(0,m.start()-100):m.start()+420] for m in re.finditer('Schweden',text)]
  start=text.find('3. Biologie');end=text.find('4. Weitere Informationen',start)
  biology=text[start:end if end>start else start+4000] if start>=0 else ''
  result=dict(scientific_name=n,url=url,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),sweden_mentions=sw,biology=biology[:7000],text_bytes=len(text),status='source_read')
 except Exception as e:result=dict(scientific_name=n,url=url,status='source_unavailable',error=str(e))
 print(n,result['status'],len(result.get('sweden_mentions',[])),flush=True);return result
if __name__=='__main__':
 rows=json.loads((STAGE/'finland-omissions-audit.json').read_text())['review_queue']
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(process,rows))
 (OUT/'screening-snippets.json').write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n')
 print('COMPLETE',len(results),flush=True)
