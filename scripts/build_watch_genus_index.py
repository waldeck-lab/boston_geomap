#!/usr/bin/env python3
"""Recover the 2017 checklist's accepted genus headings from PDF typography.

Requires Poppler pdftohtml. The pinned PDF hash and named controls deliberately
reject other editions instead of silently interpreting a changed layout.
"""
import argparse
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
import xml.etree.ElementTree as ET

PDF_SHA256 = '89289fe9dbdd8abaff6f5a2f54fee8048cfea7a98931eac0e86d0860156affc1'
METHOD = 'Poppler pdftohtml XML; accepted italic genus at body size >=16 with full-size taxon number; smaller synonym entries do not change genus'

def genus_index(root):
    fonts = {}
    result = {}
    for page in root.findall('page'):
        fonts.update({f.attrib['id']: float(f.attrib['size']) for f in page.findall('fontspec')})
        texts = page.findall('text')
        for text in texts:
            name = ''.join(text.itertext()).strip()
            if text.find('i') is None or not re.fullmatch(r'[A-Z][a-z]+', name):
                continue
            if fonts.get(text.attrib['font'], 0) < 16:
                continue
            numbers = [n for n in texts if float(n.attrib['left']) < 130
                       and abs(float(n.attrib['top']) - float(text.attrib['top'])) <= 6
                       and fonts.get(n.attrib['font'], 0) >= 16
                       and re.fullmatch(r'\d+[a-z*]*', ''.join(n.itertext()).strip())]
            if len(numbers) == 1:
                number = re.match(r'\d+', ''.join(numbers[0].itertext()).strip())[0]
                if number in result and result[number] != name:
                    raise ValueError('Conflicting genus heading: ' + number)
                result[number] = name
    if len(result) != 1030 or result.get('2104') != 'Coleophora' or '2105' in result or '2106' in result:
        raise ValueError('Checklist typography controls failed')
    return dict(sorted(result.items(), key=lambda item: int(item[0])))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pdf', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if hashlib.sha256(args.pdf.read_bytes()).hexdigest() != PDF_SHA256:
        raise ValueError('Unexpected checklist PDF edition')
    with tempfile.TemporaryDirectory(prefix='watch-genus-') as temporary:
        xml = Path(temporary) / 'checklist.xml'
        subprocess.run(['pdftohtml', '-xml', '-i', str(args.pdf), str(xml)], check=True, stdout=subprocess.DEVNULL)
        genera = genus_index(ET.parse(xml).getroot())
    args.output.write_text(json.dumps(dict(pdf_sha256=PDF_SHA256, method=METHOD, genera=genera), ensure_ascii=False, indent=2)+'\n', encoding='utf-8')

if __name__ == '__main__':
    main()
