"""B3 analysis only: settlement-screened proxy sensitivity using immutable B4 rows."""
from pathlib import Path
import copy, hashlib, json, sys
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
BASE=HERE.with_name('level_sweeps_20261012_b4_v1')
ROOT=HERE.parents[1]
sys.path.append(str(BASE))
import analyze, engine, data
analyze.HERE=HERE
analyze.TABLES=HERE/'tables'
CUTOFF=int(pd.Timestamp('2022-11-15T04:30:00Z').value//10**6)
CORE=analyze.CORE
CELL=CORE+['level','side']
METHOD='crossed symbol x UTC week bootstrap; base fees/slippage; funding omitted'

def forbidden(*a,**kw):raise RuntimeError('Signal generation, simulation and network access forbidden in B3 analysis')
engine.make_signals=forbidden
engine.simulate=forbidden
data.get=forbidden

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):(HERE/name).write_text(json.dumps(value,indent=2)+'\n')

def integrity():
    snapshot=json.loads((HERE/'input_integrity.json').read_text());counts={}
    for name,files in snapshot['studies'].items():
        root=HERE.with_name(name)
        actual={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()
                and not any(x in {'.venv','__pycache__'} for x in p.relative_to(root).parts)}
        assert actual==files,('Input study changed',name)
        counts[name]=len(files)
    live=snapshot['live_project']
    assert all(sha(ROOT/p)==digest for p,digest in live.items())
    files={str(p.relative_to(ROOT)) for name in ['crypto_monitor','tests'] for p in (ROOT/name).rglob('*.py')}
    files|={'README.md','RECOVERY.md','requirements.txt','pyproject.toml'}
    assert files==set(live)
    assert (HERE/'protocol.md').read_bytes()==(BASE/'protocol.md').read_bytes()
    assert (HERE/'analyze.py').read_bytes()==(BASE/'analyze.py').read_bytes()
    lock=json.loads((HERE/'analysis_lock.json').read_text())
    for name,digest in lock['code_sha256'].items():assert sha(HERE/name)==digest,name
    return {'unchanged_study_artifacts':counts,'unchanged_live_files':len(live),'file_sets_unchanged':True,'protocol_unchanged':True,'analysis_code_lock_matches':True}

def cutoff_masks(df):
    srm=df.symbol.eq('SRMUSDT')
    ineligible=srm & df.entry_time.ge(CUTOFF)
    crossing=srm & df.entry_time.lt(CUTOFF) & df.exit_time.gt(CUTOFF)
    return ineligible,crossing

def join_screen(df,flags):
    assert df.trade_id.is_unique and flags.trade_id.is_unique,'Duplicate trade/flag ID'
    joined=df.merge(flags[['trade_id','settlement_time','reason']].rename(columns={'reason':'known_flag_reason'}),
                    on='trade_id',how='left',validate='one_to_one')
    invalid,crossing=cutoff_masks(joined)
    flagged=joined.settlement_time.notna()
    assert np.array_equal(flagged.to_numpy(),(invalid|crossing).to_numpy()),'Known flags and explicit cutoff predicates disagree'
    assert joined.loc[flagged,'settlement_time'].eq(CUTOFF).all()
    joined['cutoff_reason']=np.where(invalid,'entry_at_or_after_settlement',np.where(crossing,'holding_crosses_settlement','retained'))
    return joined.loc[~flagged,df.columns].copy(),joined.loc[flagged].copy()

def partition():
    integrity()
    flags=pd.read_parquet(HERE/'known_eligibility_flags.parquet')
    assert flags.symbol.eq('SRMUSDT').all() and flags.trade_id.is_unique
    ledger=[];quarantined=[];observed=[]
    for stage in ['development','holdout']:
        for path in sorted((BASE/'results'/stage).glob('*_trades.parquet')):
            frame=pd.read_parquet(path);keep,q=join_screen(frame,flags)
            assert len(keep)+len(q)==len(frame)
            if len(q):
                pd.testing.assert_frame_equal(q[frame.columns].reset_index(drop=True),frame[frame.trade_id.isin(q.trade_id)].reset_index(drop=True))
                observed.extend(q.trade_id);q['source_stage']=stage;q['source_file']=path.name
                q['analysis_stratum']='quarantined_known_settlement'
                q['value_provenance']='Unchanged B4 simulated exit/P&L; not settlement P&L'
                quarantined.append(q)
                keep.to_parquet(HERE/'quarantine'/('retained_'+path.name),index=False)
            ledger.append(dict(stage=stage,file=path.name,source_sha256=sha(path),original_rows=len(frame),retained_rows=len(keep),quarantined_rows=len(q)))
    assert sorted(observed)==sorted(flags.trade_id),'Missing, duplicate or unmatched known flags'
    q=pd.concat(quarantined,ignore_index=True)
    q.to_parquet(HERE/'quarantine/rows.parquet',index=False)
    q.to_csv(HERE/'quarantine/rows.csv',index=False)
    q.groupby(CELL+['cutoff_reason'],dropna=False).size().rename('quarantined_rows').reset_index().to_csv(HERE/'quarantine/cell_counts.csv',index=False)
    pd.DataFrame(ledger).to_csv(HERE/'retained_sources.csv',index=False)
    invalid,crossing=cutoff_masks(q)
    save('partition.json',{'label':'settlement-screened proxy sensitivity','cutoff_utc':'2022-11-15T04:30:00Z',
         'entry_rule':'SRM entry_time >= cutoff is ineligible','crossing_rule':'SRM entry_time < cutoff and exit_time > cutoff crosses',
         'exit_at_cutoff':'Entry before cutoff with exit exactly at cutoff is retained by this screen',
         'source_trade_rows':sum(r['original_rows'] for r in ledger),'retained_trade_rows':sum(r['retained_rows'] for r in ledger),
         'quarantined_trade_rows':len(q),'quarantined_event_ids':q.event_id.nunique(),
         'entries_at_or_after_cutoff':int(invalid.sum()),'holdings_crossing_cutoff':int(crossing.sum()),
         'signals_regenerated':False,'trades_resimulated':False,'settlement_pnl_estimated':False})

def source_load(stage,kind,columns=None):
    return pd.concat([pd.read_parquet(p,columns=columns) for p in sorted((BASE/'results'/stage).glob('*_'+kind+'.parquet'))],ignore_index=True)

def retained_load(stage,kind,columns=None):
    if kind!='trades':return source_load(stage,kind,columns)
    requested=columns
    columns=None if columns is None else list(dict.fromkeys(columns+['trade_id']))
    frame=source_load(stage,kind,columns)
    flags=pd.read_parquet(HERE/'known_eligibility_flags.parquet',columns=['trade_id'])
    frame=frame.loc[~frame.trade_id.isin(flags.trade_id)].reset_index(drop=True)
    return frame if requested is None else frame[requested]

def ci_row(key,g,rng):
    dates=pd.to_datetime(g.entry_time,unit='ms',utc=True).dt.tz_localize(None).dt.to_period('W-SUN').astype(str)
    z=pd.DataFrame({'symbol':g.symbol.to_numpy(),'week':dates.to_numpy(),'r':g.net_base_no_funding_r.to_numpy()})
    sums=z.pivot_table(index='symbol',columns='week',values='r',aggfunc='sum',fill_value=0).to_numpy()
    count=z.pivot_table(index='symbol',columns='week',values='r',aggfunc='count',fill_value=0).to_numpy()
    ns,nw=sums.shape
    sw=rng.multinomial(ns,np.full(ns,1/ns),size=500);ww=rng.multinomial(nw,np.full(nw,1/nw),size=500)
    totals=np.einsum('bi,ij,bj->b',sw,sums,ww);n=np.einsum('bi,ij,bj->b',sw,count,ww)
    means=totals[n>0]/n[n>0];lo,hi=np.quantile(means,[.025,.975])
    return dict(zip(CORE,key),trades=len(g),symbols=ns,weeks=nw,expectancy_r=g.net_base_no_funding_r.mean(),
                ci_95_low=lo,ci_95_high=hi,replicates=len(means),fragile_clusters=ns<5 or nw<8,method=METHOD)

def matched_bootstrap(retained,baseline):
    """Preserve each B4 group's RNG starting state so unrelated intervals do not drift."""
    seed=np.random.default_rng(20261012);before=[];after=[]
    screened=dict(tuple(retained.groupby(CORE,observed=True,sort=True)))
    for key,g in baseline.groupby(CORE,observed=True,sort=True):
        state=copy.deepcopy(seed.bit_generator.state)
        before.append(ci_row(key,g,seed))  # Advances through the ORIGINAL group dimensions.
        replay=np.random.default_rng();replay.bit_generator.state=state
        assert key in screened,'Entire variant removed; requires explicit empty-interval handling'
        after.append(ci_row(key,screened[key],replay))
    return pd.DataFrame(after),pd.DataFrame(before)

def summarize(stage):
    integrity();assert (HERE/'partition.json').exists()
    analyze.load=retained_load
    def bootstrap(retained):
        baseline=source_load(stage,'trades',CORE+['symbol','entry_time','net_base_no_funding_r'])
        screened,reproduced=matched_bootstrap(retained,baseline)
        saved=pd.read_csv(BASE/'tables'/(stage+'_clustered_ci.csv'))
        pd.testing.assert_frame_equal(reproduced,saved,check_dtype=False,rtol=1e-12,atol=1e-12)
        same=reproduced.trades.eq(screened.trades)
        pd.testing.assert_frame_equal(reproduced.loc[same].reset_index(drop=True),screened.loc[same].reset_index(drop=True),check_dtype=False,rtol=1e-12,atol=1e-12)
        save('validation/'+stage+'_bootstrap.json',{'B4_intervals_reproduced':len(saved),'retained_intervals_recomputed':len(screened),'unchanged_input_intervals_verified_unchanged':int(same.sum()),'affected_intervals':int((~same).sum()),'replicates':500,'seed':20261012,'rng':'Original per-group starting states preserved; screened group uses retained symbol/week dimensions'})
        return screened
    analyze.bootstrap=bootstrap
    analyze.summarize(stage)

TABLE_KEYS={
    'full_grid':CELL,'variants':CORE,'clustered_ci':CORE,'by_symbol':CORE+['symbol'],
    'by_year':CORE+['year'],'by_volume_group':CORE+['volume_group'],
    'baseline_feature_slices':CORE+['feature','feature_value'],'exit_reasons':CORE+['exit_reason'],
}

def derived_comparisons():
    t=HERE/'tables'
    v=pd.concat([pd.read_csv(t/(s+'_variants.csv')) for s in ['development','holdout']])
    ci=pd.concat([pd.read_csv(t/(s+'_clustered_ci.csv')) for s in ['development','holdout']])
    v=v.merge(ci[CORE+['ci_95_low','ci_95_high','fragile_clusters']],on=CORE)
    base=v[(v.stop_rule=='baseline')&(v.entry_rule=='next_open')]
    base.to_csv(t/'baseline_chronological_comparison.csv',index=False)
    h=base[base.split=='holdout']
    v[(v.split=='holdout')&(v.entry_rule=='next_open')].pivot_table(index=['sweep_type','timeframe','target_rule','entry_rule'],columns='stop_rule',values='net_base_no_funding_expectancy_r').reset_index().to_csv(t/'holdout_stop_comparison.csv',index=False)
    base.pivot_table(index=['split','sweep_type','timeframe'],columns='target_rule',values='net_base_no_funding_expectancy_r').reset_index().to_csv(t/'baseline_target_comparison.csv',index=False)
    h[['sweep_type','timeframe','target_rule','completed_trades','gross_expectancy_r','net_low_no_funding_expectancy_r','net_base_no_funding_expectancy_r','net_stress_no_funding_expectancy_r','net_base_expectancy_r','funding_omitted_trades']].to_csv(t/'holdout_baseline_costs.csv',index=False)
    v[v.split=='holdout'].pivot_table(index=['sweep_type','timeframe','stop_rule','target_rule'],columns='entry_rule',values='net_base_no_funding_expectancy_r').reset_index().to_csv(t/'holdout_entry_sensitivity.csv',index=False)

def compare():
    integrity();derived_comparisons();summary=[];details=[];affected_rows=[]
    extra={
        'baseline_chronological_comparison.csv':CORE,
        'baseline_target_comparison.csv':['split','sweep_type','timeframe'],
        'holdout_baseline_costs.csv':['sweep_type','timeframe','target_rule'],
        'holdout_stop_comparison.csv':['sweep_type','timeframe','target_rule','entry_rule'],
        'holdout_entry_sensitivity.csv':['sweep_type','timeframe','stop_rule','target_rule'],
    }
    jobs=[(stage+'_'+name+'.csv',keys) for stage in ['development','holdout'] for name,keys in TABLE_KEYS.items()]+list(extra.items())
    for file,keys in jobs:
            old=pd.read_csv(BASE/'tables'/file);new=pd.read_csv(HERE/'tables'/file)
            assert set(old.columns)==set(new.columns),file
            a=old.set_index(keys);b=new.set_index(keys);assert a.index.is_unique and b.index.is_unique
            common=a.index.intersection(b.index);added=b.index.difference(a.index);removed=a.index.difference(b.index)
            changed_indices=set()
            for col in a.columns:
                x=a.loc[common,col];y=b.loc[common,col]
                # Ignore only floating round-trip/summation noise; store every substantive delta.
                if pd.api.types.is_numeric_dtype(x.dtype) and not pd.api.types.is_bool_dtype(x.dtype):
                    same=np.isclose(x.to_numpy(float),y.to_numpy(float),rtol=1e-12,atol=1e-12,equal_nan=True)
                else:same=(x.eq(y)|(x.isna()&y.isna())).fillna(False).to_numpy(bool)
                for idx in common[~same]:
                    key=dict(zip(keys,idx if isinstance(idx,tuple) else (idx,)))
                    before=a.loc[idx,col];after=b.loc[idx,col]
                    details.append(dict(table=file,change='changed',**key,field=col,before=before,after=after))
                    changed_indices.add(idx)
            for change,ids,frame in [('added',added,b),('removed',removed,a)]:
                for idx in ids:
                    key=dict(zip(keys,idx if isinstance(idx,tuple) else (idx,)))
                    details.append(dict(table=file,change=change,**key,field='__row__',before=json.dumps(frame.loc[idx].to_dict(),default=str) if change=='removed' else None,after=json.dumps(frame.loc[idx].to_dict(),default=str) if change=='added' else None))
            summary.append(dict(table=file,original_rows=len(a),screened_rows=len(b),changed_rows=len(changed_indices),added_rows=len(added),removed_rows=len(removed)))
            if file.endswith('_full_grid.csv'):
                for idx in sorted(changed_indices):
                    affected_rows.append(dict(zip(keys,idx),original_count=int(a.loc[idx,'completed_trades']),retained_count=int(b.loc[idx,'completed_trades']),quarantined_count=int(a.loc[idx,'completed_trades']-b.loc[idx,'completed_trades']),original_gross_mean=a.loc[idx,'gross_expectancy_r'],screened_gross_mean=b.loc[idx,'gross_expectancy_r'],original_base_net_mean=a.loc[idx,'net_base_no_funding_expectancy_r'],screened_base_net_mean=b.loc[idx,'net_base_no_funding_expectancy_r']))
    for stage in ['development','holdout']:
        old=json.loads((BASE/'tables'/(stage+'_summary.json')).read_text());new=json.loads((HERE/'tables'/(stage+'_summary.json')).read_text())
        summary.append(dict(table=stage+'_summary.json',original_rows=1,screened_rows=1,changed_rows=int(old!=new),added_rows=0,removed_rows=0))
        for key in old:
            if old[key]!=new[key]:details.append(dict(table=stage+'_summary.json',change='changed',field=key,before=old[key],after=new[key]))
    pd.DataFrame(summary).to_csv(HERE/'comparison/table_changes.csv',index=False)
    pd.DataFrame(details).to_csv(HERE/'comparison/field_changes.csv',index=False)
    pd.DataFrame(affected_rows).to_csv(HERE/'comparison/affected_cells.csv',index=False)
    flags=pd.read_parquet(HERE/'quarantine/rows.parquet')
    expected=set(map(tuple,flags[CELL].to_numpy()))
    assert set(tuple(r[k] for k in CELL) for r in affected_rows)==expected
    assert all(r['changed_rows']==r['added_rows']==r['removed_rows']==0 for r in summary if r['table'].startswith('holdout_'))
    print(pd.DataFrame(summary).to_string(index=False),flush=True)

def validate():
    result=integrity();ledger=pd.read_csv(HERE/'retained_sources.csv');q=pd.read_parquet(HERE/'quarantine/rows.parquet')
    assert ledger.quarantined_rows.sum()==len(q)==10 and q.trade_id.is_unique
    assert (ledger.original_rows==ledger.retained_rows+ledger.quarantined_rows).all()
    flags=pd.read_parquet(HERE/'known_eligibility_flags.parquet');assert set(q.trade_id)==set(flags.trade_id)
    assert q.value_provenance.eq('Unchanged B4 simulated exit/P&L; not settlement P&L').all()
    for stage in ['development','holdout']:
        grid=pd.read_csv(HERE/'tables'/(stage+'_full_grid.csv'));old=pd.read_csv(BASE/'tables'/(stage+'_full_grid.csv'))
        assert grid.completed_trades.sum()==ledger.loc[ledger.stage==stage,'retained_rows'].sum()
        pd.testing.assert_frame_equal(grid[CELL+['armed_signals','confirmed_signals']],old[CELL+['armed_signals','confirmed_signals']])
        pd.testing.assert_frame_equal(grid[CELL],old[CELL])
        assert (grid.trade_exclusions==grid.confirmed_signals-grid.completed_trades).all()
    result.update(source_trade_rows=int(ledger.original_rows.sum()),retained_trade_rows=int(ledger.retained_rows.sum()),quarantined_rows=len(q),quarantined_events=int(q.event_id.nunique()),signals_regenerated=False,trades_resimulated=False,settlement_pnl_estimated=False,holdout_label='diagnostic; previously inspected, not fresh untouched')
    save('validation/integrity.json',result);print(json.dumps(result,indent=2),flush=True)

if __name__=='__main__':
    action=sys.argv[1]
    if action=='partition':partition()
    elif action=='summarize':summarize(sys.argv[2])
    elif action=='compare':compare()
    elif action=='validate':validate()
    else:raise ValueError(action)
