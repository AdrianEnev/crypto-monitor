"""Read-only, offline audit of the original bounded study; writes only audit_support."""
from pathlib import Path
import hashlib, json, sys
from unittest.mock import patch
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import data, engine, analyze, audit_results, oracle_checks
from pilot_data import CASES, aggregate
from test_research import fixture, ts

def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def no_network(*a, **kw): raise AssertionError('Network forbidden during offline audit')
data.get = no_network
result = {}
manifest = pd.read_csv(HERE/'data_manifest.csv')
for r in manifest[manifest.status == 'verified'].itertuples():
    p = Path(r.local_archive)
    assert digest(p) == r.actual_sha256 == r.expected_sha256
    assert Path(str(p)+'.CHECKSUM').read_text().split()[0] == r.actual_sha256
result['verified_zip_and_checksum_hashes'] = int((manifest.status=='verified').sum())
print('Archive hashes passed', flush=True)

def integrity():
    artifacts = pd.read_csv(HERE/'artifact_manifest.csv')
    assert all(digest(HERE/r.path)==r.sha256 for r in artifacts.itertuples())
    original = json.loads((HERE/'live_project_before.json').read_text())
    assert all(digest(ROOT/p)==sha for p,sha in original.items())
    paths = {str(p.relative_to(ROOT)) for name in ['crypto_monitor','tests'] for p in (ROOT/name).rglob('*.py')}
    paths |= {'README.md','RECOVERY.md','requirements.txt','pyproject.toml'}
    assert paths == set(original)
    return {'original_artifacts_unchanged':len(artifacts), 'live_files_unchanged':len(original), 'live_file_set_unchanged':True}
result['integrity_before'] = integrity()

# Reconstruct all rankings directly from pinned daily ZIPs, independently of prepare.select.
import zipfile
daily=[]
for r in manifest[(manifest.status=='verified') & (manifest.interval=='1d')].itertuples():
    with zipfile.ZipFile(r.local_archive) as z:
        f=pd.read_csv(z.open(z.namelist()[0]))
    assert 'open_time' in f and 'quote_volume' in f
    f=f[['open_time','quote_volume']].rename(columns={'open_time':'prior_time'})
    f['symbol']=r.symbol; daily.append(f)
rank=pd.concat(daily,ignore_index=True)
rank['day']=rank.prior_time+data.DAY
rank=rank[(rank.day>=ts('2022-10-01')) & (rank.day<ts('2023-07-01'))]
rank=rank.sort_values(['day','quote_volume','symbol'],ascending=[True,False,True])
rank['rank']=rank.groupby('day').cumcount()+1
rank=rank[rank['rank']<=20].reset_index(drop=True)
saved=pd.read_csv(HERE/'universe_daily.csv').reset_index(drop=True)
pd.testing.assert_frame_equal(rank[['day','symbol','rank']],saved[['day','symbol','rank']],check_dtype=False)
np.testing.assert_allclose(rank.quote_volume,saved.prior_day_quote_volume,rtol=1e-14)
result['ranking']={'candidate_symbols':int(pd.concat(daily).symbol.nunique()),'days':int(rank.day.nunique()),'symbol_days':len(rank),'distinct_symbols':int(rank.symbol.nunique()),'mismatches':0}
print('Raw daily ZIP rankings passed',flush=True)

# Rerun existing checks without overwriting their frozen evidence files.
write_text=Path.write_text
def redirected_write(p, text, *a, **kw):
    assert p in [HERE/'results_audit.json',HERE/'oracle_checks.json'], p
    return write_text(OUT/p.name,text,*a,**kw)
with patch.object(Path,'write_text',redirected_write):
    audit_results.audit()
    oracle_checks.run()

comparisons=0
for s,m in CASES:
    base=data.parse(s,'1m',m)
    for interval,n in [('5m',5),('1h',60),('1d',1440)]:
        native=data.parse(s,interval,m).set_index('time',drop=False)
        derived=aggregate(base,n); common=derived.index.intersection(native.index)
        np.testing.assert_array_equal(derived.loc[common,['open','high','low','close','trades']],native.loc[common,['open','high','low','close','trades']])
        np.testing.assert_allclose(derived.loc[common,'quote_volume'],native.loc[common,'quote_volume'],rtol=1e-9,atol=1e-6)
        comparisons+=len(common)
result['native_aggregation_comparisons']=comparisons

cutoff=ts('2022-11-15 04:30')
srm=pd.read_parquet(HERE/'results/development/SRMUSDT_trades.parquet')
affected=srm[(srm.entry_time>=cutoff)|(srm.exit_time>cutoff)]
flags=pd.read_parquet(HERE/'known_eligibility_flags.parquet')
assert set(affected.trade_id)==set(flags.trade_id)
result['srm']={'rows':len(srm),'crossing_rows':len(affected),'events_crossing':int(affected.event_id.nunique()),'entries_at_or_after_cutoff':int((srm.entry_time>=cutoff).sum()),'exits_exactly_at_cutoff':int((srm.exit_time==cutoff).sum()),'all_crossings_flagged':True,'flag_columns_embedded_in_trade_rows':bool('settlement_time' in srm)}
affected.to_csv(OUT/'srm_crossing_rows.csv',index=False)

# Independent cost formulas on every trade; reconstruct summaries from audit rows.
stage_results={}
for stage in ['development','holdout']:
    frames=[]; allrows=0; events=set(); omitted=0
    for p in sorted((HERE/'results'/stage).glob('*_trades.parquet')):
        f=pd.read_parquet(p)
        for name,fee,slip in [('zero',0,0),('low',2,1),('base',5,2),('stress',10,5)]:
            cost=(f.entry+f.exit_price)/f.initial_risk/10000
            np.testing.assert_allclose(f['fee_'+name+'_r'],cost*fee,rtol=1e-10,atol=1e-10)
            np.testing.assert_allclose(f['slippage_'+name+'_r'],cost*slip,rtol=1e-10,atol=1e-10)
            np.testing.assert_allclose(f['net_'+name+'_r'],f.gross_r-cost*(fee+slip)-f.funding_cost_r.fillna(0),rtol=1e-10,atol=1e-10)
        for bp in [1,5]:
            penalty=f.entry/f.initial_risk*bp/10000*f.holding_minutes/480
            np.testing.assert_allclose(f[f'funding_sensitivity_{bp}bp_cost_r'],f.net_base_no_funding_r-penalty)
            np.testing.assert_allclose(f[f'funding_sensitivity_{bp}bp_credit_r'],f.net_base_no_funding_r+penalty)
        allrows+=len(f);events.update(f.event_id);omitted+=int(f.funding_cost_r.isna().sum())
        frames.append(f[analyze.CORE+['level','side','symbol','entry_time','event_id','gross_r','net_base_no_funding_r']])
    f=pd.concat(frames,ignore_index=True)
    rebuilt=f.groupby(analyze.CORE+['level','side']).agg(completed_trades=('gross_r','size'),gross_expectancy_r=('gross_r','mean'),net_base_no_funding_expectancy_r=('net_base_no_funding_r','mean')).reset_index()
    saved_grid=pd.read_csv(HERE/'tables'/(stage+'_full_grid.csv'))
    merged=rebuilt.merge(saved_grid,on=analyze.CORE+['level','side'],suffixes=('_new','_old'),validate='one_to_one')
    assert len(merged)==len(rebuilt)==int((saved_grid.completed_trades>0).sum())
    for c in ['completed_trades','gross_expectancy_r','net_base_no_funding_expectancy_r']:
        np.testing.assert_allclose(merged[c+'_new'],merged[c+'_old'],rtol=1e-10,atol=1e-10)
    ci=analyze.bootstrap(f)
    oldci=pd.read_csv(HERE/'tables'/(stage+'_clustered_ci.csv'))
    pd.testing.assert_frame_equal(ci,oldci,check_dtype=False,rtol=1e-10,atol=1e-10)
    if stage=='holdout':
        base=f[(f.stop_rule=='baseline')&(f.entry_rule=='next_open')]
        lines=(HERE/'report.md').read_text().splitlines()
        count=0
        for line in lines:
            if not line.startswith('| ') or not any(line.startswith('| '+k+' |') for k in ['live_1m','delayed_reclaim','close_beyond','reclaim_only']): continue
            kind,tf,target,n,gross,net,ci_text,win=[x.strip() for x in line.strip('|').split('|')]
            g=base[(base.sweep_type==kind)&(base.timeframe==int(tf[:-1]))&(base.target_rule==target)]
            assert len(g)==int(n)
            assert f'{g.gross_r.mean():.3f}'==gross and f'{g.net_base_no_funding_r.mean():.3f}'==net
            assert f'{100*(g.net_base_no_funding_r>0).mean():.1f}'==win
            c=ci[(ci.sweep_type==kind)&(ci.timeframe==int(tf[:-1]))&(ci.target_rule==target)&(ci.stop_rule=='baseline')&(ci.entry_rule=='next_open')].iloc[0]
            assert f'[{c.ci_95_low:.3f}, {c.ci_95_high:.3f}]'==ci_text
            count+=1
        assert count==14
        result['report_table_rows_reproduced']=count
    stage_results[stage]={'rows':allrows,'distinct_event_ids_with_trades':len(events),'funding_omitted':omitted,'full_grid_cells':len(saved_grid),'active_grid_cells_reproduced':len(rebuilt),'variant_ci_rows_reproduced':len(ci)}
    print('Summary and bootstrap reproduction passed',stage,flush=True)
result['stages']=stage_results
training=pd.concat([pd.read_parquet(p,columns=['split','status','event_id','prior_24h_volatility']) for p in (HERE/'results/development').glob('*_events.parquet')])
training=training[(training.split=='train')&(training.status=='confirmed')].drop_duplicates('event_id').prior_24h_volatility.dropna()
assert training.quantile([1/3,2/3]).tolist()==json.loads((HERE/'development_locked.json').read_text())['volatility_training_cutpoints']
result['training_only_thresholds']={'events':len(training),'matches_frozen_cutpoints':True}

# Synthetic probes of tick dependence and timeout boundary; no corrected performance.
minute,daily,members=fixture()
i=1441
minute.loc[i,'high']=100*data.SCALE+data.SCALE//100
small=engine.MarketData('TESTUSDT',minute,daily,members,pd.DataFrame())
large=engine.MarketData('TESTUSDT',minute,daily,members,pd.DataFrame())
small.tick[:]=data.SCALE//100;large.tick[:]=data.SCALE//50
def specific(m):
    return [e for e in engine.make_signals(m,['train'])[0] if e['level']=='monday_high' and e['side']=='short' and e['sweep_type']=='live_1m' and e['timeframe']==5 and e['sweep_time']==ts('2022-11-08 00:01')]
assert len(specific(small))==1 and len(specific(large))==0
result['tick_synthetic_probe']='Changing tick from 0.01 to 0.02 removes the same otherwise-qualified sweep.'
minute,daily,members=fixture()
minute.loc[1444,['open','high','low','close']]=np.array([95,96,94,95])*data.SCALE
j=i+361
minute.loc[j,['open','high','low','close']]=np.array([99,101,98,99])*data.SCALE
m=engine.MarketData('TESTUSDT',minute,daily,members,pd.DataFrame())
events,_=engine.make_signals(m,['train'])
target=[e for e in events if e['level']=='monday_high' and e['side']=='short' and e['sweep_type']=='live_1m' and e['timeframe']==5]
assert target[0]['status']=='confirmation_timeout'
assert not any(e['sweep_time']==int(m.t[j]) for e in target)
assert m.c[j-1]<=100*data.SCALE and m.h[j]>=100*data.SCALE+m.tick[j] and m.c[j]<100*data.SCALE
result['timeout_probe']={'first_sweep':pd.Timestamp(m.t[i],unit='ms',tz='UTC').isoformat(),'expiry':pd.Timestamp(m.t[i]+361*data.MS,unit='ms',tz='UTC').isoformat(),'suppressed_qualifying_sweep_open':pd.Timestamp(m.t[j],unit='ms',tz='UTC').isoformat(),'observed':pd.Timestamp(m.t[j]+data.MS,unit='ms',tz='UTC').isoformat(),'cause':'busy=until+1 instead of release at until on six-hour timeout'}
result['integrity_after']=integrity()
(OUT/'checks.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2),flush=True)
