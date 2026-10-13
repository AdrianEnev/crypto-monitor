"""Offline B4-only correction orchestration. No writes to the original study/cache."""
from pathlib import Path
import difflib, hashlib, json, shutil, sys
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ORIGINAL=HERE.with_name('level_sweeps_20261012')
ROOT=HERE.parents[1]
CACHE=Path.home()/'Library/Caches/crypto-monitor-research/20261012-b4-v1'

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):
    (HERE/name).write_text(json.dumps(value,indent=2)+'\n')

def integrity():
    before=json.loads((HERE/'original_integrity.json').read_text())
    current={str(p.relative_to(ORIGINAL)):sha(p) for p in ORIGINAL.rglob('*')
             if p.is_file() and not any(x in {'.venv','__pycache__'} for x in p.relative_to(ORIGINAL).parts)}
    assert current==before['original_study'],'Original study changed'
    assert all(sha(ROOT/p)==h for p,h in before['live_project'].items()),'Live file changed'
    live={str(p.relative_to(ROOT)) for name in ['crypto_monitor','tests'] for p in (ROOT/name).rglob('*.py')}
    live|={'README.md','RECOVERY.md','requirements.txt','pyproject.toml'}
    assert live==set(before['live_project']),'Live file set changed'
    assert (HERE/'protocol.md').read_bytes()==(ORIGINAL/'protocol.md').read_bytes()
    return {'original_study_files_unchanged':len(current),'live_files_unchanged':len(live),'file_sets_unchanged':True,'protocol_unchanged':True}

def offline():
    import data
    data.RAW=CACHE/'metadata';data.PARSED=CACHE/'parsed'
    for p in [data.RAW,data.PARSED]:p.mkdir(parents=True,exist_ok=True)
    manifest=pd.read_csv(ORIGINAL/'data_manifest.csv')
    records={}
    for r in manifest.itertuples():
        records[r.symbol,r.interval,r.month]=json.loads(Path(r.local_archive+'.json').read_text())
    def fetch(s,i,m):
        assert (s,i,m) in records,('Unpinned archive request',s,i,m)
        return records[s,i,m].copy()
    def blocked(*a,**kw):raise RuntimeError('Network disabled for B4 correction')
    data.get=blocked;data.fetch=fetch
    data.manifest=lambda:shutil.copyfile(ORIGINAL/'data_manifest.csv',HERE/'data_manifest.csv')
    return data,manifest

def check_lock():
    lock=json.loads((HERE/'development_locked.json').read_text())
    assert lock['diagnostic_inspected_holdout'] is True
    for name,digest in lock['code_sha256'].items():assert sha(HERE/name)==digest, name
    assert lock['volatility_training_cutpoints']==json.loads((ORIGINAL/'development_locked.json').read_text())['volatility_training_cutpoints']

def prepare():
    integrity();check_lock();data,manifest=offline()
    assert not any(data.PARSED.iterdir()),'Fresh raw reparse requires an empty version-specific parsed cache'
    verified=manifest[manifest.status=='verified']
    for r in verified.itertuples():
        assert sha(Path(r.local_archive))==r.actual_sha256==r.expected_sha256
        assert Path(r.local_archive+'.CHECKSUM').read_text().split()[0]==r.actual_sha256
    print('All pinned ZIP/checksum hashes verified; reparsing raw candles and funding',flush=True)
    # Original parser, isolated destination, no access to original parsed Parquet files.
    data.batch(((r.symbol,r.interval,r.month) for r in manifest.itertuples()),workers=4)
    parsed=list(data.PARSED.glob('*.parquet'));assert len(parsed)==len(verified)
    pd.DataFrame([{'file':p.name,'sha256':sha(p),'bytes':p.stat().st_size} for p in sorted(parsed)]).to_csv(HERE/'parsed_cache_manifest.csv',index=False)
    save('cache_prepared.json',{'completed_utc':pd.Timestamp.now(tz='UTC').isoformat(),'verified_raw_archives':len(verified),'fresh_parsed_archives':len(parsed),'source_manifest_sha256':sha(ORIGINAL/'data_manifest.csv'),'cache':str(CACHE),'network_requests':0})

def simulate(stage):
    integrity();check_lock();assert (HERE/'cache_prepared.json').exists()
    offline()
    import run
    out=HERE/'results'/stage
    assert not out.exists(),'Refuse to append or overwrite a prior simulation'
    run.full(['train','validation'] if stage=='development' else ['holdout'])
    record=json.loads((out/'completed.json').read_text())
    record.update(correction='B4-v1',recomputed_from_scratch=True,
                  holdout_status='diagnostic: previously inspected April-June 2023; not a fresh untouched test')
    save('results/'+stage+'/completed.json',record)

def compare():
    integrity();check_lock()
    out=HERE/'comparison';out.mkdir(exist_ok=True)
    counts=[];changes=[];field_counts=[]
    for stage in ['development','holdout']:
        for kind,key in [('events','event_id'),('trades','trade_id')]:
            olddir=ORIGINAL/'results'/stage;newdir=HERE/'results'/stage
            names=sorted({p.name for p in olddir.glob('*_'+kind+'.parquet')}|{p.name for p in newdir.glob('*_'+kind+'.parquet')})
            for name in names:
                old=pd.read_parquet(olddir/name).set_index(key) if (olddir/name).exists() else pd.DataFrame()
                new=pd.read_parquet(newdir/name).set_index(key) if (newdir/name).exists() else pd.DataFrame()
                assert old.index.is_unique and new.index.is_unique
                if len(old) and len(new):assert list(old.columns)==list(new.columns)
                common=old.index.intersection(new.index);added=new.index.difference(old.index);removed=old.index.difference(new.index)
                a=old.loc[common];b=new.loc[common]
                different=~(a.eq(b)|(a.isna()&b.isna()))
                changed=different.any(axis=1)
                for col in different:
                    n=int(different[col].sum())
                    if n:field_counts.append(dict(stage=stage,kind=kind,file=name,field=col,changed_rows=n))
                for split in ['train','validation','holdout']:
                    oldn=int((old.split==split).sum()) if len(old) else 0
                    newn=int((new.split==split).sum()) if len(new) else 0
                    plus=int((new.loc[added,'split']==split).sum()) if len(added) else 0
                    minus=int((old.loc[removed,'split']==split).sum()) if len(removed) else 0
                    delta=int((b.loc[changed,'split']==split).sum()) if len(common) else 0
                    if oldn or newn:
                        counts.append(dict(stage=stage,split=split,kind=kind,symbol=name.removesuffix('_'+kind+'.parquet'),original=oldn,corrected=newn,added=plus,removed=minus,changed=delta,unchanged=newn-plus-delta))
                for action,ids,frame in [('added',added,new),('removed',removed,old),('changed',common[changed],new)]:
                    if not len(ids):continue
                    selected=frame.loc[ids].reset_index();selected.insert(0,'change',action);selected.insert(0,'stage',stage)
                    selected.to_parquet(out/(stage+'_'+name.removesuffix('.parquet')+'_'+action+'.parquet'),index=False)
                    if action=='changed':old.loc[ids].reset_index().to_parquet(out/(stage+'_'+name.removesuffix('.parquet')+'_changed_before.parquet'),index=False)
                    for ident in ids:changes.append(dict(stage=stage,kind=kind,change=action,identity=ident,split=frame.loc[ident,'split'],symbol=frame.loc[ident,'symbol']))
            print('Compared',stage,kind,flush=True)
    detail=pd.DataFrame(counts);detail.to_csv(out/'by_symbol.csv',index=False)
    totals=detail.groupby(['split','kind'],sort=False)[['original','corrected','added','removed','changed','unchanged']].sum().reset_index()
    totals.to_csv(out/'counts.csv',index=False)
    pd.DataFrame(changes,columns=['stage','kind','change','identity','split','symbol']).to_csv(out/'changed_ids.csv',index=False)
    pd.DataFrame(field_counts,columns=['stage','kind','file','field','changed_rows']).to_csv(out/'changed_fields.csv',index=False)
    assert (totals.original+totals.added-totals.removed==totals.corrected).all()
    assert (totals.unchanged+totals.changed+totals.added==totals.corrected).all()
    # Preserve the frozen set of cells; report numeric summary deltas without ranking winners.
    for stage in ['development','holdout']:
        keys=['split','sweep_type','timeframe','stop_rule','target_rule','entry_rule','level','side']
        a=pd.read_csv(ORIGINAL/'tables'/f'{stage}_full_grid.csv').set_index(keys)
        b=pd.read_csv(HERE/'tables'/f'{stage}_full_grid.csv').set_index(keys)
        assert set(a.index)==set(b.index)
        numeric=list(b.select_dtypes(include='number').columns)
        delta=b[numeric]-a[numeric]
        delta.to_csv(out/f'{stage}_full_grid_numeric_deltas.csv')
    print(totals.to_string(index=False),flush=True)

def validate():
    integrity();check_lock();data,manifest=offline()
    import audit_results,eligibility_audit,pilot_data,oracle_checks
    audit_results.audit()
    pilot_data.run()
    oracle_checks.run()
    eligibility_audit.run()
    a=pd.read_parquet(ORIGINAL/'known_eligibility_flags.parquet').sort_values('trade_id').reset_index(drop=True)
    b=pd.read_parquet(HERE/'known_eligibility_flags.parquet').sort_values('trade_id').reset_index(drop=True)
    pd.testing.assert_frame_equal(a,b)
    for stage in ['development','holdout']:
        for name in ['execution_coverage.csv','reference_coverage.csv']:
            pd.testing.assert_frame_equal(pd.read_csv(ORIGINAL/'results'/stage/name),pd.read_csv(HERE/'results'/stage/name))
    for r in manifest[manifest.status=='verified'].itertuples():assert sha(Path(r.local_archive))==r.actual_sha256
    for r in pd.read_csv(HERE/'parsed_cache_manifest.csv').itertuples():assert sha(CACHE/'parsed'/r.file)==r.sha256
    save('validation/integrity.json',dict(**integrity(),corrected_code_lock_matches=True,original_srm_flags_preserved=len(b),raw_and_fresh_parsed_cache_hashes_match=True,coverage_unchanged=True))

def comparisons():
    t=HERE/'tables';keys=['split','sweep_type','timeframe','stop_rule','target_rule','entry_rule']
    v=pd.concat([pd.read_csv(t/(s+'_variants.csv')) for s in ['development','holdout']])
    ci=pd.concat([pd.read_csv(t/(s+'_clustered_ci.csv')) for s in ['development','holdout']])
    v=v.merge(ci[keys+['ci_95_low','ci_95_high','fragile_clusters']],on=keys)
    base=v[(v.stop_rule=='baseline')&(v.entry_rule=='next_open')]
    base.to_csv(t/'baseline_chronological_comparison.csv',index=False)
    h=base[base.split=='holdout']
    v[(v.split=='holdout')&(v.entry_rule=='next_open')].pivot_table(index=['sweep_type','timeframe','target_rule','entry_rule'],columns='stop_rule',values='net_base_no_funding_expectancy_r').reset_index().to_csv(t/'holdout_stop_comparison.csv',index=False)
    base.pivot_table(index=['split','sweep_type','timeframe'],columns='target_rule',values='net_base_no_funding_expectancy_r').reset_index().to_csv(t/'baseline_target_comparison.csv',index=False)
    h[['sweep_type','timeframe','target_rule','completed_trades','gross_expectancy_r','net_low_no_funding_expectancy_r','net_base_no_funding_expectancy_r','net_stress_no_funding_expectancy_r','net_base_expectancy_r','funding_omitted_trades']].to_csv(t/'holdout_baseline_costs.csv',index=False)
    v[v.split=='holdout'].pivot_table(index=['sweep_type','timeframe','stop_rule','target_rule'],columns='entry_rule',values='net_base_no_funding_expectancy_r').reset_index().to_csv(t/'holdout_entry_sensitivity.csv',index=False)

if __name__=='__main__':
    action=sys.argv[1]
    if action=='prepare':prepare()
    elif action in ['development','holdout']:simulate(action)
    elif action=='summarize':
        check_lock();import analyze
        analyze.summarize(sys.argv[2])
    elif action=='compare':compare();comparisons()
    elif action=='validate':validate()
    elif action=='integrity':print(json.dumps(integrity(),indent=2))
    else:raise ValueError(action)
