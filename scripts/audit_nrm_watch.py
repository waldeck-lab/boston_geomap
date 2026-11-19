"""Read every indexed NRM species page; retain a source-linked review queue."""
import argparse
import concurrent.futures
from datetime import datetime,timezone
import hashlib
import html
import json
from pathlib import Path
import re
import threading
import time
from urllib.parse import urljoin,urlsplit
import requests

INDEX="http://www3.nrm.se/en/svenska_fjarilar/svenska_fjarilar.html"
FIELDS=("Kännetecken","Levnadssätt","Flygtid","Biologi","Värdväxt","Näringsväxt","Förekomst","Utbredning")

def text(value):
    return re.sub(r"\s+"," ",html.unescape(re.sub(r"<[^>]*>"," ",value))).strip()

def parse(raw,url):
    body=raw.decode("iso-8859-1")
    plain=text(body)
    title=re.search(r"<title[^>]*>(.*?)</title>",body,re.I|re.S)
    label=text(title[1]) if title else ""
    title_name=re.match(r"([A-Z][a-z]+\s+[a-z][a-z-]+)(?:\s|$|\()",label)
    slug=Path(urlsplit(url).path).stem.split("_")
    url_name=(slug[0].capitalize()+" "+slug[1]) if len(slug)>=2 else ""
    name=url_name
    fields={}
    for field in FIELDS:
        match=re.search(re.escape(field)+r"\s*:\s*(.*?)(?=(?:"+"|".join(FIELDS)+r")\s*:|Mer om denna art|Latest update:|http://www[23]\.nrm|$)",plain,re.I)
        if match:fields[field]=match[1].strip()
    date=re.search(r"Latest update:\s*([^<>\r\n]+)",plain,re.I)
    main=re.split("Mer om denna art",plain,flags=re.I)[0]
    distribution=fields.get("Utbredning","")
    if not distribution:
        sentences=re.split(r"(?<=[.!?])\s+",main)
        distribution=" ".join(part for part in sentences if re.search(r"Sverige|Norden|Danmark|Bornholm|Norge|Finland|landet|utbredning",part,re.I))
    absent=bool(re.search(r"(?:inte|ej|aldrig)[^.!?]{0,55}Sverige|saknas[^.!?]{0,25}Sverige|Sverige[^.!?]{0,25}(?:saknas|inte|ej)",distribution,re.I))
    provinces=r"Skåne|Blekinge|Småland|Öland|Gotland|Halland|Västergötland|Bohuslän|Östergötland|Södermanland|Uppland|Dalarna|Värmland|Västmanland|Gästrikland|Hälsingland|Medelpad|Ångermanland|Jämtland|Härjedalen|Västerbotten|Norrbotten|lappmark|hela landet|nästan hela landet|Götaland|Svealand"
    swedish_locality=bool(re.search(provinces,distribution,re.I) or re.search(r"\b(?:Sk|Bl|Sm|Öl|Go|Ha|Vg|Ög|Bo|Ds|Nä|Sö|Vs|Up|Vr|Dr|Gä|Hs|Me|Ån|Jä|Hr|Vb|Nb)\b",distribution))
    nearby=bool(re.search(r"Danmark|Bornholm|Finland|Norge|Tyskland|Estland|Lettland|Litauen|Polen",distribution,re.I))
    foreign=absent or (nearby and not swedish_locality and not re.search(r"Sverige",distribution,re.I))
    links=[]
    for link in re.findall(r'href\s*=\s*["\x27]([^"\x27]+)["\x27]',body,re.I):
        target=urljoin(url,html.unescape(link));parts=urlsplit(target)
        if parts.hostname=="www3.nrm.se" and re.fullmatch(r"/en/svenska_fjarilar/[a-z]/[a-z0-9_]+\.html",parts.path,re.I) and not re.search(r"_(?:bilder|genitalia|genitalier|larver|larva|pupa|puppa|imagines)\.html$",parts.path,re.I):
            links.append("http://www3.nrm.se"+parts.path)
    date_line=date[1] if date else None
    if date_line:date_line=re.split(r"Kommentarer|Uppdater",date_line)[0].strip()
    return dict(url=url,page_name=name,title=label,title_name=title_name[1] if title_name else "",
                fields=fields,distribution_text=distribution,page_updated=date_line,
                distribution_flag=foreign,explicit_swedish_absence=absent,swedish_locality=swedish_locality,
                linked_pages=sorted(set(links)),content_sha256=hashlib.sha256(raw).hexdigest())

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--pilot",type=Path,required=True)
    args=parser.parse_args();root=args.output.resolve()
    if not root.is_relative_to(Path("/proj/boston/beta/wcr/stage")):
        raise ValueError("Beta stage output required")
    cache=root/"pages";cache.mkdir(parents=True,mode=0o700,exist_ok=True)
    response=requests.get(INDEX,timeout=25);response.raise_for_status()
    (root/"index.html").write_bytes(response.content)
    links=sorted({urljoin(INDEX,link) for link in re.findall(r'href\s*=\s*["\x27]([a-z]/[a-z0-9_]+\.html)["\x27]',response.text,re.I)
                  if not re.search(r"_(?:bilder|genitalia|genitalier|larver|larva|pupa|puppa|imagines)\.html$",link,re.I)})
    pilot=json.loads(args.pilot.read_text())
    visible={r["scientific_name"] for r in pilot.get("profiles",[])+pilot.get("references",[])}
    local=threading.local()
    def fetch(url):
        key=hashlib.sha256(url.encode()).hexdigest()
        path=cache/(key+".html")
        try:
            if path.exists():raw=path.read_bytes()
            else:
                if not hasattr(local,"session"):
                    local.session=requests.Session()
                    local.session.headers["User-Agent"]="WCR-NRM-review/1.0 (beta.waldeck.se)"
                for attempt in range(2):
                    try:
                        actual_url=url.replace("hodebertia_teastalis.html","hodebertia_testalis.html")
                        r=local.session.get(actual_url,timeout=15);r.raise_for_status();raw=r.content;break
                    except requests.RequestException:
                        if attempt:raise
                        time.sleep(1)
                path.write_bytes(raw);time.sleep(.08)
            row=parse(raw,url.replace("hodebertia_teastalis.html","hodebertia_testalis.html"))
            if row["url"]!=url:row["index_link_correction"]=url
            row["already_visible_exact"]=row["page_name"] in visible
            return row
        except Exception as error:
            return dict(url=url,error=type(error).__name__)
    rows=[]
    seen=set()
    pending=links
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        while pending:
            seen.update(pending)
            discovered=set()
            for row in pool.map(fetch,pending):
                rows.append(row);discovered.update(row.get("linked_pages",[]))
                if len(rows)%100==0:print("FETCHED",len(rows),"known",len(seen),flush=True)
            pending=sorted(discovered-seen)
            if len(seen)+len(pending)>5000:raise ValueError("Unexpected graph size; review crawler scope")
    report=dict(retrieved_at=datetime.now(timezone.utc).isoformat(),index_url=INDEX,
        index_sha256=hashlib.sha256(response.content).hexdigest(),indexed_urls=len(links),linked_urls=len(rows),
        pages=rows,failed=[r for r in rows if "error" in r],
        flagged_not_visible=[r for r in rows if r.get("distribution_flag") and not r.get("already_visible_exact")],
        limitations=["Distribution flags are a broad review queue, not proof of Swedish absence or expansion.",
          "Every indexed species URL is examined, including aliases and obsolete combinations.",
          "Successful downloads and missing distribution sections are counted separately.",
          "Raw pages are retained privately for source audit; public summaries must be paraphrased."])
    (root/"audit.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print("COMPLETE",json.dumps(dict(indexed=len(links),downloaded=sum("error" not in r for r in rows),
        distribution_pages=sum(bool(r.get("fields",{}).get("Utbredning")) for r in rows),
        flagged_new=len(report["flagged_not_visible"]),failed=len(report["failed"]))),flush=True)
if __name__=="__main__":main()
