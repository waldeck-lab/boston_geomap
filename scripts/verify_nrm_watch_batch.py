"""Verify additive beta publication against the previous static snapshot."""
import argparse,hashlib,json,sqlite3
from pathlib import Path
def read(path):return json.loads(path.read_text())
def check(stage,baseline):
    public=stage/'dist/watch-pilot';previous=baseline/'watch-pilot'
    before=read(previous/'pilot-data.json');after=read(public/'pilot-data.json')
    assert len(after['profiles'])==84 and len(after['references'])==9
    assert after['references']==before['references']
    assert after['profiles'][:len(before['profiles'])]==before['profiles'],'Previous species changed'
    for old in (previous/'timeline').glob('*.json'):
        if old.name!='manifest.json':assert (public/'timeline'/old.name).read_bytes()==old.read_bytes()
    oldids={p['id'] for p in before['profiles']+before['references']};new=[p for p in after['profiles'] if p['id'] not in oldids]
    assert len(new)==6 and len({p['id'] for p in after['profiles']+after['references']})==93
    db=sqlite3.connect(stage/'future-species.sqlite')
    assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    assert not db.execute('PRAGMA foreign_key_check').fetchall()
    sourceids={s['id'] for s in after['sources']}
    for p in new:
        assert p['role']==p['assessment']['classification']=='watch_candidate'
        assert not (p.get('expansion') or {}).get('high_interest')
        assert all(f['sources'] and f['checked_at']=='2026-11-19' for f in p['knowledge_review']['facts'])
        assert {'distribution','habitat','hosts','search'}<={f['category'] for f in p['knowledge_review']['facts']}
        assert all(h['kind']=='hypothesis' for h in p['search_guide']['swedish_hypotheses'])
        assert all(h['source_id'] in sourceids for h in p['hosts'])
        assert p['geographic_notes'] and all(g['cartographic_role']=='geographic_reference_only' and 'anchor' not in g for g in p['geographic_notes'])
        assert hashlib.sha256((public/p['image']['local_path']).read_bytes()).hexdigest()==p['image']['sha256']
        t=read(public/'timeline'/f"{p['id']}.json")
        assert t['complete_api_paging'] and not t['identity_conflicts']
        assert all(o['country']!='SE' and ('creativecommons.org' in o['license']) for o in t['occurrences'])
        assert t['unique_records']==len(t['occurrences'])
        if p['scientific_name']=='Anarsia lineatella':
            assert all(o['year']>=2017 for o in t['occurrences'])
            assert p['assessment']['evidence']['gbif_sweden_count']==10
            assert p['swedish_status']=='taxonomic_split_review'
        if p['scientific_name']=='Crombrugghia tristis':
            assert p['assessment']['evidence']['gbif_sweden_count']==1
            assert p['assessment']['evidence']['excluded_sweden_record_ids']==[1821015801]
            assert p['assessment']['evidence']['national_review_exception']['upstream_corrected'] is False
        if p['scientific_name']=='Aterpia chalybeia':
            assert p['hosts']==[]
            assert any(f['category']=='hosts' and f['status']=='unresolved' for f in p['knowledge_review']['facts'])
    # Root WCR assets, legal pages and other views are identical to previous beta.
    for old in baseline.rglob('*'):
        if old.is_file() and 'watch-pilot' not in old.relative_to(baseline).parts:
            assert (stage/'dist'/old.relative_to(baseline)).read_bytes()==old.read_bytes()
    print(json.dumps({'profiles':84,'references':9,'new_profiles_checked':6,'old_profiles_and_timelines':'unchanged','new_timeline_records':sum(read(public/'timeline'/f"{p['id']}.json")['unique_records'] for p in new),'integrity':'passed','root_WCR_assets':'unchanged'}))
if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--stage',type=Path,required=True);a.add_argument('--baseline',type=Path,required=True);v=a.parse_args();check(v.stage,v.baseline)
