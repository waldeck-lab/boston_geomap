"""Archive six cited species pages and NRM specimen photos for beta enrichment."""
import argparse
from datetime import datetime,timezone
import hashlib
import html
import json
from pathlib import Path
import re
from urllib.parse import urljoin
import requests

NAMES=["Anarsia lineatella","Crombrugghia tristis","Agdistis adactyla","Agdistis bennetii","Aterpia chalybeia","Atremaea lonchoptera"]
EXTRA={
"norway-tristis":("Norsk rödlista 2021: Oxyptilus tristis","https://lister.artsdatabanken.no/rodlisteforarter/2021/10800"),
"et2018":("Bengtsson 2018: fynd 2017 och rättelse av Anarsia","https://www.sef.nu/download/entomologisk_tidskrift/et_2018/ET%20139%208-20%20Bengtsson%202018.pdf"),
"anarsia-original":("Gregersen & Karsholt 2017: uppdelningen av Anarsia","https://nl.pensoft.net/article/11184/download/pdf/"),
"uk-bennetii":("UKmoths: Agdistis bennetii","https://www.ukmoths.org.uk/species/agdistis-bennetii/"),
}
def main():
    p=argparse.ArgumentParser();p.add_argument("--stage",type=Path,required=True);args=p.parse_args()
    root=args.stage.resolve()
    if not root.is_relative_to(Path("/proj/boston/beta/wcr/stage")) or not root.name.startswith("watch-pilot-"):raise ValueError("Isolated pilot stage required")
    source=root/"source";source.mkdir(mode=0o700,parents=True,exist_ok=True);images=root/"images";images.mkdir(exist_ok=True)
    audit_root=Path("/proj/boston/beta/wcr/stage/watch-pilot-nrm-audit-20261119")
    audit=json.loads((audit_root/"audit.json").read_text())
    entries=[];photos=[];session=requests.Session();session.headers["User-Agent"]="WCR-source-reviewed-beta/1.0 (beta.waldeck.se)"
    def fetch(sid,title,url,path):
        if not path.exists():
            r=session.get(url,timeout=40);r.raise_for_status();path.write_bytes(r.content)
        entries.append(dict(id=sid,title=title,url=url,path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                            checked_at="2026-11-19",retrieved_at=datetime.now(timezone.utc).isoformat()))
    for name in NAMES:
        slug=name.lower().replace(" ","-")
        row=next(r for r in audit["pages"] if r.get("page_name")==name)
        raw=(audit_root/"pages"/(hashlib.sha256(row.get("index_link_correction",row["url"]).encode()).hexdigest()+".html")).read_bytes()
        path=source/(slug+".nrm.html");path.write_bytes(raw)
        entries.append(dict(id=slug+"-nrm",title="NRM: "+name,url=row["url"],path=str(path),sha256=hashlib.sha256(raw).hexdigest(),checked_at="2026-11-19"))
        lepi_name="Oxyptilus tristis" if name=="Crombrugghia tristis" else name
        fetch(slug+"-lepi","Lepiforum: "+lepi_name,"https://lepiforum.org/wiki/page/"+lepi_name.replace(" ","_"),source/(slug+".lepiforum.html"))
        tags=re.findall(r'<img[^>]+>',raw.decode("iso-8859-1"),re.I)
        candidates=[tag for tag in tags if re.search(r'(?:_bild|_male|_female)\.jpg',tag,re.I)]
        if len(candidates)!=1:raise ValueError("Specimen image needs review: "+name)
        src=re.search(r'src=["\x27]([^"\x27]+)',candidates[0],re.I)[1];url=urljoin(row["url"],html.unescape(src));image=images/(slug+".jpg")
        fetch(slug+"-image","NRM artbild: "+name,url,image)
        photos.append(dict(scientific_name=name,url=url,path=str(image),source_page=row["url"],
            source_html_sha256=hashlib.sha256(raw).hexdigest(),image_sha256=hashlib.sha256(image.read_bytes()).hexdigest(),
            caption_source="NRM species-page IMG alt label; photo author not separately stated in HTML"))
        print(name,"sources and image saved",flush=True)
    for sid,(title,url) in EXTRA.items():
        try:fetch(sid,title,url,source/(sid+(".pdf" if "pdf" in url else ".html")))
        except requests.RequestException as error:print("Additional source unavailable",sid,type(error).__name__,flush=True)
    (root/"source-manifest.json").write_text(json.dumps(dict(sources=entries,photos=photos),ensure_ascii=False,indent=2))
    print("COMPLETE",len(entries),"sources",len(photos),"images",flush=True)
if __name__=="__main__":main()
