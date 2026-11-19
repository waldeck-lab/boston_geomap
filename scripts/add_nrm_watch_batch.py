"""Add the first source-reviewed NRM cohort to an isolated beta knowledge snapshot.
Never updates the operational GeoMap observation, taxonomy or private-list stores.
Raw national counts and reviewed exceptions are retained separately.
"""
import argparse,collections,hashlib,json,shutil,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from geomap.watch_store import connect,profile,inventory,source
from geomap.watch_scope import get_scope
VERSION='nrm-beta-batch1-20261119'
def read(p):return json.loads(Path(p).read_text())
def write(p,v):Path(p).write_text(json.dumps(v,ensure_ascii=False,indent=2)+'\n')
def build(stage,baseline,seed):
    stage=stage.resolve();baseline=baseline.resolve();seed=seed.resolve()
    if not stage.is_relative_to(Path('/proj/boston/beta/wcr/stage')) or not stage.name.startswith('watch-pilot-'):raise ValueError('Isolated beta stage required')
    if not baseline.is_relative_to(Path('/proj/boston/beta/wcr/stage')):raise ValueError('Beta baseline required')
    if (stage/'future-species.sqlite').exists() or (stage/'dist').exists():raise ValueError('Refuse to overwrite an existing publication')
    root=Path(__file__).resolve().parents[1];batch=read(root/'data/watch'/f'{VERSION}.json')
    audit={r['page_name']:r for r in read(root/'data/watch/nrm-complete-review-20261119.json')['results']}
    manifest=read(stage/'source-manifest.json');sources={s['id']:s for s in manifest['sources']}
    old=read(baseline/'watch-pilot/pilot-data.json')
    names={p['scientific_name'] for p in old['profiles']+old['references']}
    if any(p['scientific_name'] in names for p in batch['profiles']):raise ValueError('Already published species')
    src=sqlite3.connect(seed.as_uri()+'?mode=ro',uri=True);dest=sqlite3.connect(stage/'future-species.sqlite');src.backup(dest);src.close();dest.close()
    db=connect(stage/'future-species.sqlite');new=[];images=[];new_sources=[]
    def cite(s):
        r=sources[s];return {k:r[k] for k in ['title','url','sha256','checked_at']}
    with db:
        for s in manifest['sources']:
            source(db,VERSION+'-'+s['id'],s['title'],s['url'],'Source rights retained; editorial factual summary only',VERSION,s['path'])
        for p in batch['profiles']:
            name=p['scientific_name'];uid=p['id'];slug=name.lower().replace(' ','-');a=audit[name]
            ids=lambda x:[x if x in sources else slug+'-'+x][0]
            if db.execute('SELECT 1 FROM candidates WHERE id=? OR scientific_name=?',(uid,name)).fetchone():raise ValueError('Candidate collision')
            e=dict(occurrence_scope='nordic-front',gbif_sweden_count=a['sweden_count'],front_2015_2026_count=a['recent_neighbour_count'],
                evidence_urls=list(dict.fromkeys(a['evidence_urls']+[sources[slug+'-lepi']['url']])),
                source_titles={s['url']:s['title'] for s in manifest['sources']},
                status_summary=p['status_summary'],review_scope_note=p['limitations'],
                national_record_policy='Raw query counts are not verified specimens or national status.',
                audit_status=a['status'],audit_checked_at='2026-11-19')
            e['source_titles'].update({'https://doi.org/10.3897/nl.40.11184':'Gregersen & Karsholt 2017: taxonuppdelningen', 'https://www.gbif.org/occurrence/1821015801':'Missjö 2017: råpost med lokal rättelse i WCR'})
            if a['sweden_count']:
                if name=='Anarsia lineatella':
                    assert a.get('screening_status_before_publication',a['status'])=='taxonomic_split_candidate' and p['status']=='taxonomic_split_review'
                    e['national_review_exception']={'kind':'taxonomic_split','sources':['https://doi.org/10.3897/nl.40.11184',sources['et2018']['url']],'accepted_swedish_record_count':None}
                elif name=='Crombrugghia tristis':
                    assert a['excluded_sweden_record_ids']==[1821015801] and a['usable_swedish_records_in_review']==0
                    e.update({k:a[k] for k in ['record_reviews','excluded_sweden_record_ids','usable_swedish_records_in_review']})
                    e['national_review_exception']={'kind':'user_determination_correction','asserted_by':'Jonas Waldeck','asserted_at':'2026-11-19','record_url':'https://www.gbif.org/occurrence/1821015801','upstream_corrected':False}
                else:raise ValueError('Unreviewed positive Swedish query')
            db.execute('INSERT INTO candidates VALUES(?,?,?,?,?,?,?,?)',(uid,name,'watch_candidate',p['status'],'source-reviewed; expert review pending',p['limitations'],None,a['gbif_key']))
            db.execute('INSERT INTO candidate_assessments VALUES(?,?,?,?,?)',(uid,'watch_candidate',batch['checked_at'],p['rationale'],json.dumps(e,ensure_ascii=False)))
            nrm_id=VERSION+'-'+slug+'-nrm'
            db.execute('INSERT INTO vernacular_names VALUES(?,?,?,?,?)',(uid,a['swedish_name'],'sv',nrm_id,name))
            facts=[]
            for f in p['facts']:
                category,text,refs=f[:3];cs=[]
                for ref in refs:
                    if ref=='record-correction':
                        cs.append({'title':'Missjöposten; lokal rättelse av Jonas Waldeck 2026-11-19 (inte upstream-korrigerad)','url':'https://www.gbif.org/occurrence/1821015801','checked_at':'2026-11-19','review_scope':'User correction in WCR project chat; raw source retained separately'})
                    else:cs.append(cite(ids(ref)))
                facts.append(dict(category=category,text=text,status=f[3] if len(f)>3 else 'documented',sources=cs,checked_at=batch['checked_at']))
            review=dict(version=VERSION,checked_at=batch['checked_at'],scope='Foreign source facts, uncertainty and Swedish editorial hypotheses remain separate',facts=facts,source_pages_reviewed=list({s['url']:s for f in facts for s in f['sources']}.values()),unresolved_policy='Unknown larval hosts are never inferred from vernacular names or congeners.')
            db.execute('INSERT INTO knowledge_reviews VALUES(?,?,?)',(uid,VERSION,json.dumps(review,ensure_ascii=False)))
            guide=dict(scientific_name=name,swedish_name=a['swedish_name'],review_status='source-reviewed; Swedish proposals are hypotheses',
                sources=[dict(id=k,**cite(k)) for k in [slug+'-nrm',slug+'-lepi']],
                documented=[dict(kind='documented',category='distribution',title='Regionalt bevakningsförslag',text=p['rationale'],region='Europeiskt grannområde; nationell status bedöms separat',source_ids=[slug+'-nrm',slug+'-lepi'])],
                swedish_hypotheses=[dict(kind='hypothesis',title=t,text=v,region='Sverige – redaktionell hypotes',source_ids=[slug+'-nrm',slug+'-lepi']) for t,v in p['hypotheses']],
                limitations=p['limitations'],follow_up='Dokumentera datum, plats, miljö, värdväxt och bestämningsunderlag. Misstänkta nya svenska fynd behöver specialistgranskas och rapporteras genom ordinarie Artportalenflöde. '+p['status_summary'])
            db.execute('INSERT INTO search_guides VALUES(?,?,?)',(uid,VERSION,json.dumps(guide,ensure_ascii=False)))
            supplement=dict(scientific_name=name,source_url=sources[slug+'-lepi']['url'],checked_at=batch['checked_at'],summary_status='editorial_summary',availability=dict(habitat=True,hosts=True,search=True),notes={k:[] for k in ['habitat','hosts','search','locations','uncertainties']})
            supplement['notes']['uncertainties']=p['gaps']
            db.execute('INSERT INTO species_supplements VALUES(?,?,?)',(uid,VERSION,json.dumps(supplement,ensure_ascii=False)))
            for plant in p['hosts']:
                db.execute('INSERT INTO host_associations VALUES(?,?,?,?,?,?,?,?)',(uid,plant,'genus' if plant.endswith('sp.') else 'species',None,'Europe; no Swedish use established',VERSION+'-'+slug+'-lepi','reviewed biological section; genus/species precision retained','source-reviewed'))
            for code in p['countries']:
                country_names={'DE':'Tyskland','NO':'Norge','PL':'Polen','FI':'Finland','DK':'Danmark','GB':'Storbritannien'}
                csource='norway-tristis' if name=='Crombrugghia tristis' and code=='NO' else slug+'-nrm' if code=='DK' else 'uk-bennetii' if code=='GB' else slug+'-lepi'
                note=dict(id=f'nrm-batch1-{uid}-{code}',country_code=code,label=country_names[code],precision='country',event_year=None,temporal_status='undated',
                    claim='Källan beskriver förekomst eller ett platsexempel i landet. Konturen är en geografisk referens, inte en fyndpunkt, heltäckande utbredning eller daterad expansion. Se artens statusförbehåll.',
                    sources=[cite(csource)],checked_at=batch['checked_at'],cartographic_role='geographic_reference_only')
                db.execute('INSERT INTO geographic_annotations VALUES(?,?,?)',(note['id'],uid,json.dumps(note,ensure_ascii=False)))
            timeline=read(stage/'timeline'/f'{uid}.json')
            if p.get('occurrence_identity_policy'):
                cutoff=p['occurrence_identity_policy']['exclude_unreviewed_before_year']
                excluded=[o for o in timeline['occurrences'] if o['year']<cutoff]
                if excluded:
                    write(stage/f'timeline-{uid}-before-identity-review.json',timeline)
                    timeline['occurrences']=[o for o in timeline['occurrences'] if o['year']>=cutoff]
                    timeline['unique_records']=len(timeline['occurrences']);timeline['rejected_records']+=len(excluded)
                    timeline['identity_review']=dict(excluded_before_year=cutoff,excluded_records=len(excluded),reason=p['occurrence_identity_policy']['reason'],sources=p['occurrence_identity_policy']['sources'])
                    write(stage/'timeline'/f'{uid}.json',timeline)
                assert all(o['year']>=cutoff for o in timeline['occurrences'])
            assert timeline['scientific_name']==name and timeline['complete_api_paging'] and not timeline['identity_conflicts']
            for ts in timeline['sources']:
                sid=VERSION+'-gbif-'+ts['sha256'][:20]
                db.execute('INSERT OR IGNORE INTO sources(id,title,url,license,version,sha256,retrieved_at) VALUES(?,?,?,?,?,?,?)',(sid,'GBIF: avgränsad licensierad fyndsökning '+name,ts['url'],'Individual record CC0 1.0 / CC BY 4.0',VERSION,ts['sha256'],ts['retrieved_at']))
            # Keep an independently normalized complete history for animation. Only exact-day
            # records since 2000 enter the legacy sample table; never synthesize a month/day.
            sid=VERSION+'-gbif-'+timeline['sources'][0]['sha256'][:20]
            for o in timeline['occurrences']:
                if o['year']<2000 or o['date_precision']!='day' or '/' in o['event_date']:continue
                assert o['country']!='SE'
                db.execute('INSERT INTO occurrences(occurrence_uid,candidate_id,country,latitude,longitude,uncertainty_m,event_date,year,month,life_stage,basis_of_record,license,source_id,quality_flags) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(o['occurrence_uid'],uid,o['country'],o['latitude'],o['longitude'],o['uncertainty_m'],o['event_date'],o['year'],o['month'],o['life_stage'],None,o['license'],sid,'[]'))
                oid=db.execute('SELECT id FROM occurrences WHERE occurrence_uid=?',(o['occurrence_uid'],)).fetchone()[0]
                for key in o['gbif_keys'].split(','):
                    db.execute('INSERT INTO occurrence_records VALUES(?,?,?,?,?)',('GBIF',key,oid,','.join(o['dataset_keys']),o['occurrence_uid']))
            imsource=sources[slug+'-image'];img=Path(imsource['path'])
            im=dict(scientific_name=name,candidate_id=uid,filename=slug+'.jpg',local_path='images/'+slug+'.jpg',
                sha256=imsource['sha256'],bytes=img.stat().st_size,provider='Naturhistoriska riksmuseet',title='NRM: '+name,source_page=sources[slug+'-nrm']['url'],
                original_source_url=sources[slug+'-nrm']['url'],original_source_label='NRM:s artblad',original_url=imsource['url'],download_url=imsource['url'],
                reviewed_at=batch['checked_at'],review_status='source_identity_and_embedded_credits_visually_reviewed',
                alt='Uppspänt exemplar av '+name,selection_note='NRM:s artillustration; ingen ny genital- eller DNA-bestämning. '+('Fyra exemplar i originalets sammanställning.' if uid==92 else ''),
                modifications='Originalets JPEG sparad oförändrad; ingen beskärning eller retusch.',
                identity_basis='NRM:s bildkoppling och visuell granskning; historiska taxonförbehåll gäller.',
                author=p['author'],license='NRM-bildlån enligt projektägarens uppgift; inte öppen licens',license_url=sources[slug+'-nrm']['url'],
                permission=dict(note='Jonas Waldeck har uppgett tidigare bildutbyte och tillstånd att låna NRM-bilder till WCR. Fotografens rättigheter kvarstår. Upphov har inte härletts från HTML-författaren. Okänd fotograf anges uttryckligen.'),
                credit=p['author']+'; återgiven från NRM')
            out=profile(db,uid);out.update(family=a['family'],family_source={'title':'GBIF taxon match, NRM audit 2026-11-19','url':a['evidence_urls'][1]},image=im,occurrence_scope=get_scope('nordic-front'))
            new.append(out);images.append(im)
        assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        assert not db.execute('PRAGMA foreign_key_check').fetchall()
    old['profiles']+=new
    old['sources']=[dict(r) for r in db.execute('SELECT * FROM sources ORDER BY id')]
    existing_inventory={r['id'] for r in old['inventory']}
    old['inventory'] += [r for r in inventory(db) if r['id'] not in existing_inventory]
    old['generated_at']='2026-11-19T00:00:00+00:00'
    old['batch_review_note']='New source review and six timelines dated 2026-11-19; previous species and histories retain their individual review dates.'
    old['batch_review']={'version':VERSION,'checked_at':batch['checked_at'],'candidate_ids':[p['id'] for p in new],'previous_profiles_preserved':78,'previous_references_preserved':9}
    db.close()
    shutil.copytree(baseline,stage/'dist',symlinks=False);public=stage/'dist/watch-pilot'
    for img in images:shutil.copy2(stage/'images'/img['filename'],public/img['local_path'])
    image_manifest=read(public/'images/manifest.json');image_manifest['images']+=images;image_manifest['version']=VERSION;image_manifest['selection_policy']+=' Six further original NRM specimen images: project-owner loan attestation, individual credits retained, unknown photographers explicitly labelled.'
    write(public/'images/manifest.json',image_manifest)
    old_timeline=read(public/'timeline/manifest.json');new_timeline=read(stage/'timeline/manifest.json')
    
    for item in new_timeline['profiles']:
        reviewed=read(stage/'timeline'/f"{item['candidate_id']}.json")
        item.update(unique_records=reviewed['unique_records'],rejected_records=reviewed['rejected_records'])
    old_timeline['profiles']+=new_timeline['profiles'];old_timeline['batch_updates']=[{'as_of':new_timeline['as_of'],'candidate_ids':[p['id'] for p in new]}]
    for p in new:shutil.copy2(stage/'timeline'/f"{p['id']}.json",public/'timeline'/f"{p['id']}.json")
    write(public/'timeline/manifest.json',old_timeline);write(public/'pilot-data.json',old);write(stage/'pilot-export.json',old)
    import collections,csv
    report=read(root/'data/watch/nrm-complete-review-20261119.json')
    ids={p['scientific_name']:p['id'] for p in new}
    for row in report['results']:
        if row['page_name'] in ids:
            row.setdefault('screening_status_before_publication',row['status'])
            row.update(status='already_visible',published_profile_id=ids[row['page_name']],published_review_version=VERSION,shown_as=row['page_name'])
    report['counts']=dict(collections.Counter(r['status'] for r in report['results']))
    write(public/'nrm-review.json',report)
    with (public/'nrm-review.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.writer(stream,delimiter=';');writer.writerow(['scientific_name','swedish_name','family','status','screening_status','published_profile_id','sweden_raw_records','neighbour_raw_records','nrm_urls','evidence_urls'])
        for r in sorted(report['results'],key=lambda r:(r.get('family') or '',r['page_name'])):
            writer.writerow([r['page_name'],r.get('swedish_name',''),r.get('family',''),r['status'],r.get('screening_status_before_publication',''),r.get('published_profile_id',''),r.get('sweden_count'),r.get('recent_neighbour_count'),' | '.join(r['source_urls']),' | '.join(r['evidence_urls'])])

    write(stage/'publication-summary.json',dict(version=VERSION,profiles=len(old['profiles']),references=len(old['references']),new_species=[p['scientific_name'] for p in new],database=str(stage/'future-species.sqlite'),previous_static_target=str(baseline)))
    print(json.dumps(read(stage/'publication-summary.json'),ensure_ascii=False))
if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--stage',type=Path,required=True);ap.add_argument('--baseline',type=Path,required=True);ap.add_argument('--seed',type=Path,required=True);a=ap.parse_args();build(a.stage,a.baseline,a.seed)
