"""Reproducible research stages; holdout requires frozen protocol and passed pilot."""
import hashlib,json,shutil,sys,time
from collections import Counter
from pathlib import Path
import numpy as np
import pandas as pd
from data import HERE,DAY,MS,CACHE,PARSED,batch,parse,manifest
from engine import MarketData,make_signals,simulate,SPLITS,LEVELS
from prepare import daily_frames
from pilot_data import CASES

EXCOLS=['split','level','side','sweep_type','timeframe','stop_rule','target_rule','entry_rule','reason']
COUNTCOLS=['split','level','side','sweep_type','timeframe','status']

def frozen():
    freeze=json.loads((HERE/'protocol_freeze.json').read_text())
    assert hashlib.sha256((HERE/'protocol.md').read_bytes()).hexdigest()==freeze['sha256'],'Frozen protocol modified'

def concat(frames):
    frames=[f for f in frames if len(f)]
    return pd.concat(frames,ignore_index=True).sort_values('time').drop_duplicates('time',keep=False) if frames else pd.DataFrame()

def pilot():
    frozen()
    import unittest
    suite=unittest.defaultTestLoader.discover(str(HERE),pattern='test_research.py')
    with (HERE/'research_tests.txt').open('w') as f:
        res=unittest.TextTestRunner(stream=f,verbosity=2).run(suite)
    if not res.wasSuccessful():raise RuntimeError('Research acceptance fixtures failed')
    native=pd.read_csv(HERE/'pilot_native_comparison.csv');assert not native.mismatches.any()
    warm=[(s,'1m',(pd.Timestamp(m+'-01')-pd.offsets.MonthBegin(1)).strftime('%Y-%m')) for s,m in CASES]
    batch(warm)
    daily=daily_frames();rows=[]
    for s,month in CASES:
        a=pd.Timestamp(month+'-01',tz='UTC');b=a+pd.offsets.MonthBegin(1)
        previous=(a-pd.offsets.MonthBegin(1)).strftime('%Y-%m')
        minute=concat([parse(s,'1m',previous),parse(s,'1m',month)])
        members=pd.DataFrame({'day':np.arange(a.value//10**6,b.value//10**6,DAY),'rank':1})
        funding=parse(s,'fundingRate',month)
        market=MarketData(s,minute,daily[s],members,funding)
        events,_=make_signals(market,['train','validation']);trades,_=simulate(market,events)
        frame=pd.DataFrame(trades)
        assert len(frame),'Pilot has no trade paths to validate'
        assert (frame.entry_time>=frame.confirmation_time).all()
        assert (frame.exit_time>=frame.entry_time).all()
        assert (frame.initial_risk>0).all()
        assert (frame.holding_minutes<=1440).all()
        assert np.isfinite(frame.gross_r).all()
        assert (frame.net_base_no_funding_r<=frame.gross_r+1e-12).all()
        rows.append({'symbol':s,'month':month,'armed_events':len(events),'confirmed_events':sum(e['status']=='confirmed' for e in events),
                     'trade_variants':len(frame),'path_checks':'pass','purpose':'mechanics only; fixed pilot universe'})
    pd.DataFrame(rows).to_csv(HERE/'pilot_execution_checks.csv',index=False)
    (HERE/'pilot_passed.json').write_text(json.dumps({'passed_utc':pd.Timestamp.now(tz='UTC').isoformat(),'research_tests':res.testsRun,
                                                    'native_comparisons':int(native.compared.sum()),'cases':rows},indent=2)+'\n')
    print('PILOT PASSED',json.dumps(rows),flush=True)

def full(split_names):
    frozen();assert (HERE/'pilot_passed.json').exists()
    label='development' if split_names==['train','validation'] else 'holdout'
    if label=='holdout':
        assert (HERE/'development_locked.json').exists(),'Freeze development feature bins before holdout'
        lock=json.loads((HERE/'development_locked.json').read_text())
        for name,digest in lock['code_sha256'].items():
            assert hashlib.sha256((HERE/name).read_bytes()).hexdigest()==digest,('Code changed after development lock',name)
    out=HERE/'results'/label;out.mkdir(parents=True,exist_ok=True)
    universe=pd.read_csv(HERE/'universe_daily.csv');plan=pd.read_csv(HERE/'minute_download_plan.csv');daily=daily_frames()
    excluded=Counter();counts=Counter();coverage=[];reference_coverage=[]
    selected_days=set()
    for name,a,b in SPLITS:
        if name in split_names:selected_days.update(range(a,b,DAY))
    symbols=sorted(universe[universe.day.isin(selected_days)].symbol.unique())
    for si,s in enumerate(symbols,1):
        start=time.monotonic()
        months=sorted(plan[plan.symbol==s].month.unique())
        # Development process cannot read holdout price paths.
        months=[m for m in months if (m<'2023-04' if label=='development' else m>='2023-03')]
        minute=concat([parse(s,'1m',m) for m in months]);funding=concat([parse(s,'fundingRate',m) for m in months if m>='2022-10'])
        membership=universe[(universe.symbol==s)&universe.day.isin(selected_days)]
        if minute.empty:
            coverage.append(dict(symbol=s,selected_minutes=len(membership)*1440,available_selected_minutes=0,feature_eligible_minutes=0,missing_minutes=len(membership)*1440));continue
        market=MarketData(s,minute,daily[s],membership,funding)
        for name,a,b in SPLITS:
            if name not in split_names:continue
            selected=membership[(membership.day>=a)&(membership.day<b)]
            for watch in LEVELS:
                available=sum(watch in market.refs.get(int(day),{}) for day in selected.day)
                reference_coverage.append(dict(symbol=s,split=name,level=watch,selected_symbol_days=len(selected),
                                               available_reference_days=available,unavailable_reference_days=len(selected)-available))
        covered=int((market.selected&market.valid).sum());expected=len(membership)*1440
        coverage.append(dict(symbol=s,selected_minutes=expected,available_selected_minutes=covered,
                             feature_eligible_minutes=int((market.selected&market.valid&(market.tick>0)).sum()),missing_minutes=expected-covered))
        events,c=make_signals(market,split_names);trades,x=simulate(market,events)
        if events:pd.DataFrame(events).to_parquet(out/(s+'_events.parquet'),index=False,compression='zstd')
        if trades:pd.DataFrame(trades).to_parquet(out/(s+'_trades.parquet'),index=False,compression='zstd')
        excluded.update(x);counts.update(c)
        print(label,s,si,'/',len(symbols),'events',len(events),'trades',len(trades),'seconds',round(time.monotonic()-start,1),flush=True)
        del market,events,trades,minute,funding
    pd.DataFrame([dict(zip(EXCOLS,key),count=v) for key,v in excluded.items()]).to_csv(out/'trade_exclusions.csv',index=False)
    pd.DataFrame([dict(zip(COUNTCOLS,key),count=v) for key,v in counts.items()]).to_csv(out/'signal_counts.csv',index=False)
    pd.DataFrame(coverage).to_csv(out/'execution_coverage.csv',index=False)
    pd.DataFrame(reference_coverage).to_csv(out/'reference_coverage.csv',index=False)
    (out/'completed.json').write_text(json.dumps({'completed_utc':pd.Timestamp.now(tz='UTC').isoformat(),'split_names':split_names,'symbols':len(symbols)},indent=2)+'\n')
    manifest()

if __name__=='__main__':
    if sys.argv[1]=='pilot':pilot()
    elif sys.argv[1]=='development':full(['train','validation'])
    elif sys.argv[1]=='holdout':full(['holdout'])
