"""Validate an observed-species CSV and create a personal missing-species list."""
import csv
import hashlib
import io
import itertools
import json
import re
import unicodedata
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path

from geomap.shared_store import connect

MAX_BYTES=2*1024*1024
ID_HEADERS={'taxonid','dyntaxataxonid','dyntaxaid','artid'}


class CsvValidationError(ValueError):
    def __init__(self,message,details=None):
        super().__init__(message)
        self.details=details or {}


def normalized_header(value):
    return ''.join(c for c in unicodedata.normalize('NFKD',value).casefold() if c.isalnum())


def detect_delimiter(text):
    # Authors contain unquoted commas in Artportalen's semicolon export.
    # Prefer a consistent column structure over punctuation frequency.
    choices=[]
    for delimiter in [';',',','\t']:
        try:
            rows=[r for r in itertools.islice(csv.reader(io.StringIO(text),delimiter=delimiter,strict=True),50) if r]
        except csv.Error:continue
        if not rows:continue
        width=len(rows[0])
        if width>1:
            consistent=sum(len(r)==width for r in rows)/len(rows)
            recognized=any(normalized_header(h) in ID_HEADERS|{'scientificname','vetenskapligtnamn','swedishname','svensktnamn','latinsktnamn','taxonname'} for h in rows[0])
            choices.append((recognized,consistent,width,delimiter))
    return max(choices)[-1] if choices else ';'


def parse_observed_csv(raw):
    if not raw or len(raw)>MAX_BYTES:
        raise CsvValidationError('Välj en CSV-artlista som är högst 2 MiB.')
    if b'\x00' in raw:
        raise CsvValidationError('Filen är inte en stödd CSV-textfil. Spara som CSV UTF-8.')
    try:
        text=raw.decode('utf-8-sig');encoding='utf-8'
    except UnicodeDecodeError:
        try:text=raw.decode('cp1252');encoding='windows-1252'
        except UnicodeDecodeError as exc:raise CsvValidationError('CSV-filens teckenkodning kunde inte läsas.') from exc
    lines=text.splitlines()
    delimiter=None
    if lines and re.fullmatch(r'sep=[;,\t]',lines[0].strip(),flags=re.I):
        delimiter=lines.pop(0).strip()[-1];text='\n'.join(lines)
    if delimiter is None:
        delimiter=detect_delimiter(text)
    try:
        reader=csv.reader(io.StringIO(text,newline=''),delimiter=delimiter,strict=True)
        header=next(reader,None)
        if not header:raise CsvValidationError('CSV-filen saknar kolumnrubriker.')
        columns=[i for i,h in enumerate(header) if normalized_header(h.strip()) in ID_HEADERS]
        if len(columns)!=1:
            raise CsvValidationError('Filen måste ha exakt en taxon-id-kolumn, exempelvis TaxonId, DyntaxaTaxonId eller taxon_id.',dict(missing_id_column=not columns))
        index=columns[0];ids=set();duplicates=0;row_count=0
        for row in reader:
            if not row or not any(cell.strip() for cell in row):continue
            row_count+=1
            if row_count>50000:raise CsvValidationError('För många rader. Exportera en artlista, inte alla observationer.')
            if len(row)!=len(header):raise CsvValidationError(f'Fel antal kolumner på rad {reader.line_num}.')
            value=row[index].strip()
            try:
                number=Decimal(value)
                if not number.is_finite() or number!=number.to_integral_value() or not 0<number<=2147483647:
                    raise InvalidOperation
                tid=int(number)
            except InvalidOperation as exc:
                raise CsvValidationError(f'Ogiltigt taxon-id på rad {reader.line_num}.') from exc
            if tid in ids:duplicates+=1
            ids.add(tid)
    except csv.Error as exc:
        raise CsvValidationError('CSV-filen har felaktig citering eller struktur.') from exc
    if not ids:raise CsvValidationError('Artlistan är tom. Inga arter har tolkats som observerade.')
    return ids,dict(input_rows=row_count,unique_input_taxa=len(ids),duplicate_rows=duplicates,
                    encoding=encoding,delimiter='tab' if delimiter=='\t' else delimiter)


def normalized_name(value):
    return ' '.join(unicodedata.normalize('NFKC',value).casefold().split())


def parse_observed_names(raw,source):
    # The size and text-encoding checks already ran in the ID parser.
    try:text=raw.decode('utf-8-sig');encoding='utf-8'
    except UnicodeDecodeError:text=raw.decode('cp1252');encoding='windows-1252'
    lines=text.splitlines();delimiter=None
    if lines and re.fullmatch(r'sep=[;,\t]',lines[0].strip(),flags=re.I):
        delimiter=lines.pop(0).strip()[-1];text='\n'.join(lines)
    if delimiter is None:
        delimiter=detect_delimiter(text)
    reader=csv.reader(io.StringIO(text,newline=''),delimiter=delimiter,strict=True)
    first=next(reader,None)
    if not first:raise CsvValidationError('Artlistan är tom.')
    header=[normalized_header(h) for h in first]
    sci=[i for i,h in enumerate(header) if h in {'scientificname','vetenskapligtnamn','latinsktnamn','taxonname'}]
    swe=[i for i,h in enumerate(header) if h in {'swedishname','svensktnamn','svensknamn'}]
    name_index=source.get('name_index',{});groups=set(source.get('non_species_names',[]))
    if len(sci)>1 or len(swe)>1:
        raise CsvValidationError('CSV-filen har flera möjliga namnkolumner.')
    if sci or swe:
        index=(sci or swe)[0];secondary=swe[0] if sci and swe else None;data=reader;fmt='named-columns'
    elif len(first)==5 and (normalized_name(first[1]) in name_index or normalized_name(first[1]) in groups):
        # Artportalen TaxonList: Swedish name; scientific name; author; redlist; count.
        index=1;secondary=0;data=itertools.chain([first],reader);fmt='artportalen-taxonlist'
    else:
        raise CsvValidationError('Filen behöver en taxon-id- eller namnkolumn, eller vara Artportalens femkolumners TaxonList-export utan rubriker.')
    observed=set();row_count=duplicates=0;ignored=[];issues=[]
    try:
        for row in data:
            if not row or not any(cell.strip() for cell in row):continue
            row_count+=1
            if row_count>50000:raise CsvValidationError('För många rader. Exportera en artlista.')
            if len(row)!=len(first):raise CsvValidationError(f'Fel antal kolumner på rad {reader.line_num}.')
            value=row[index].strip();key=normalized_name(value)
            candidates=set(name_index.get(key,[]));conflicting=False
            if key in groups and not candidates:
                # A family/genus/species-group observation does not prove any particular species.
                ignored.append(dict(row=reader.line_num,name=value));continue
            needs_secondary=key in groups and bool(candidates)
            if secondary is not None and row[secondary].strip():
                other=set(name_index.get(normalized_name(row[secondary]),[]))
                if candidates and other:
                    candidates=candidates & other
                    conflicting=not candidates
                    needs_secondary=False
            if len(candidates)!=1 or needs_secondary:
                issues.append(dict(row=reader.line_num,name=value,reason='conflicting' if conflicting else 'ambiguous' if candidates else 'unmatched',candidate_taxon_ids=sorted(candidates)))
                continue
            tid=int(next(iter(candidates)))
            if tid in observed:duplicates+=1
            observed.add(tid)
    except csv.Error as exc:
        raise CsvValidationError('CSV-filen har felaktig citering eller struktur.') from exc
    if issues:
        raise CsvValidationError(f'{len(issues)} namn kunde inte kopplas entydigt till en fjärilsart. Ingen lista har sparats.',
            dict(name_issues=issues[:50],name_issue_count=len(issues),ignored_non_species_rows=len(ignored),matched_species=len(observed)))
    if not observed:raise CsvValidationError('Inga entydiga fjärilsarter hittades i filen.')
    return observed,dict(input_rows=row_count,unique_input_taxa=len(observed),duplicate_rows=duplicates,
        encoding=encoding,delimiter='tab' if delimiter=='\t' else delimiter,input_format=fmt,
        matched_by_name=True,ignored_non_species_rows=len(ignored),ignored_non_species_preview=ignored[:10])


def build_difference(raw,catalogue_path):
    source=json.loads(Path(catalogue_path).read_text(encoding='utf-8'))
    try:ids,summary=parse_observed_csv(raw)
    except CsvValidationError as exc:
        if not exc.details.get('missing_id_column'):raise
        ids,summary=parse_observed_names(raw,source)
    species={int(r['taxon_id']):r for r in source['species']}
    mapping={int(k):int(v) for k,v in source['species_mapping'].items()}
    unknown=sorted(ids-mapping.keys())
    if unknown:
        raise CsvValidationError('Vissa taxon-id:n kunde inte kopplas till aktuella fjärilsarter. Kontrollera att exporten gäller fjärilar och innehåller Dyntaxa-id:n. Ingen lista har sparats.',
            dict(unmatched_count=len(unknown),unmatched_taxon_ids=unknown[:30]))
    observed={mapping[tid] for tid in ids}
    missing=sorted(species.keys()-observed)
    summary.update(universe_taxa=len(species),observed_species=len(observed),missing_species=len(missing),
        mapped_infraspecific_taxa=sum(tid!=mapping[tid] for tid in ids),
        taxonomy_retrieved_at=source['retrieved_at'],taxonomy_source_sha256=source['source_sha256'],
        upload_sha256=hashlib.sha256(raw).hexdigest(),
        missing_preview=[species[t] for t in missing[:20]])
    return [species[t] for t in missing],observed,summary


def save_difference(db_path,owner_id,title,rows,observed,summary):
    title=title.strip()
    if not 1<=len(title)<=120 or any(ord(c)<32 for c in title):
        raise CsvValidationError('Listnamnet måste innehålla 1–120 tecken.')
    list_id=uuid.uuid4().hex
    directory=Path(db_path).parent/'user-lists'/str(int(owner_id))/list_id
    directory.mkdir(parents=True,mode=0o700)
    directory.chmod(0o700)
    missing_path=directory/'missing.csv';observed_path=directory/'observed.csv'
    with missing_path.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=['taxon_id','scientific_name','swedish_name'])
        writer.writeheader();writer.writerows(rows)
    with observed_path.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(['taxon_id'])
        writer.writerows([[tid] for tid in sorted(observed)])
    metadata={k:v for k,v in summary.items() if k!='missing_preview'}
    (directory/'metadata.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding='utf-8')
    for p in directory.iterdir():p.chmod(0o600)
    ids=sorted(int(r['taxon_id']) for r in rows)
    content_hash=hashlib.sha256(json.dumps(ids).encode()).hexdigest()
    try:
        with connect(db_path) as db:
            db.execute('''CREATE TABLE IF NOT EXISTS list_imports(
             list_id TEXT PRIMARY KEY REFERENCES lists(id) ON DELETE CASCADE,
             source_type TEXT NOT NULL, metadata_json TEXT NOT NULL)''')
            db.execute('INSERT INTO lists(id,title,owner_id,visibility,content_hash) VALUES(?,?,?,?,?)',
                (list_id,title,int(owner_id),'private',content_hash))
            db.executemany('INSERT INTO taxa VALUES(?,?,?) ON CONFLICT(taxon_id) DO NOTHING',
                [(r['taxon_id'],r['scientific_name'],r['swedish_name']) for r in rows])
            db.executemany('INSERT INTO list_taxa VALUES(?,?)',[(list_id,tid) for tid in ids])
            db.execute('INSERT INTO list_imports VALUES(?,?,?)',(list_id,'observed-species-csv',json.dumps(metadata)))
            available=db.execute('''SELECT count(*) FROM list_taxa l WHERE l.list_id=? AND EXISTS
              (SELECT 1 FROM coverage c WHERE c.taxon_id=l.taxon_id AND c.date_from='2000-01-01'
               AND c.date_to='2026-10-31' AND c.source_scope='public-only-start-v1')''',(list_id,)).fetchone()[0]
    except BaseException:
        # Only remove this newly generated UUID directory, never any pre-existing user data.
        for p in directory.iterdir():p.unlink()
        directory.rmdir()
        raise
    return dict(id=list_id,title=title,visibility='private',taxa_count=len(ids),
        taxa_with_import_coverage=available,taxa_needing_import=len(ids)-available,
        csv_url=f'/geomap-api/lists/{list_id}/csv',map_url=f'/geomap?list_id={list_id}')
