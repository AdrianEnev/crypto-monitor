"""Replay representative original outputs from fresh offline ZIP parses, without correction."""
from pathlib import Path
import json,sys,tempfile
from unittest.mock import patch
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1];OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import data,engine
from run import concat
def no_network(*a,**kw):raise AssertionError('Offline replay only')
data.get=no_network
manifest=pd.read_csv(HERE/'data_manifest.csv')
meta={(r.symbol,r.interval,r.month):r._asdict() for r in manifest.itertuples(index=False)}
def local_fetch(s,i,m):return meta[(s,i,m)].copy()
plan=pd.read_csv(HERE/'minute_download_plan.csv');universe=pd.read_csv(HERE/'universe_daily.csv')
counts=[];timeout_examples=[];checked=set();funding_checks=0
with tempfile.TemporaryDirectory(prefix='crypto-audit-fresh-parse-') as tmp:
    temp=Path(tmp)
    with patch.object(data,'fetch',local_fetch),patch.object(data,'PARSED',temp),patch.object(data,'RAW',temp):
        for symbol,stage in [('BTCUSDT','development'),('BTCUSDT','holdout'),('SRMUSDT','development'),('FTTUSDT','development')]:
            names=['train','validation'] if stage=='development' else ['holdout']
            months=sorted(plan[plan.symbol==symbol].month.unique())
            months=[m for m in months if (m<'2023-04' if stage=='development' else m>='2023-03')]
            def fresh(interval,month):
                f=data.parse(symbol,interval,month)
                if len(f):
                    cached=pd.read_parquet(data.CACHE/'parsed'/f'{symbol}-{interval}-{month}.parquet')
                    pd.testing.assert_frame_equal(f,cached)
                    checked.add((symbol,interval,month))
                return f
            minute=concat([fresh('1m',m) for m in months])
            funding=concat([fresh('fundingRate',m) for m in months if m>='2022-10'])
            daily=concat([fresh('1d',r.month) for r in manifest[(manifest.symbol==symbol)&(manifest.interval=='1d')].itertuples()])
            days=set()
            for name,a,b in engine.SPLITS:
                if name in names:days.update(range(a,b,data.DAY))
            members=universe[(universe.symbol==symbol)&universe.day.isin(days)]
            market=engine.MarketData(symbol,minute,daily,members,funding)
            events,_=engine.make_signals(market,names)
            trades,_=engine.simulate(market,events)
            for kind,rows,key in [('events',events,'event_id'),('trades',trades,'trade_id')]:
                actual=pd.DataFrame(rows).sort_values(key).reset_index(drop=True)
                expected=pd.read_parquet(HERE/'results'/stage/f'{symbol}_{kind}.parquet').sort_values(key).reset_index(drop=True)
                pd.testing.assert_frame_equal(actual[expected.columns],expected,check_dtype=False,rtol=1e-12,atol=1e-12)
            # Independently recompute covered funding cashflows using interval ends.
            completed=minute.time.to_numpy()+data.MS
            for t in trades[::37]:
                if pd.isna(t['funding_cost_r']):continue
                settlements=funding[(funding.time>t['entry_time'])&(funding.time<=t['exit_time'])]
                cost=0.
                for r in settlements.itertuples():
                    index=np.searchsorted(completed,r.time,side='right')-1
                    assert index>=0
                    cost+=r.rate*minute.iloc[index]['close']/data.SCALE
                cost*= (1 if t['side']=='long' else -1)/t['initial_risk']
                np.testing.assert_allclose(cost,t['funding_cost_r'],rtol=1e-8,atol=1e-9)
                funding_checks+=1
            # Count qualifying candidates suppressed in the minute AFTER a six-hour expiry.
            event_keys={(e['level'],e['side'],e['sweep_type'],e['timeframe'],e['sweep_time']) for e in events}
            suppressed=0
            for e in events:
                if e['status']!='confirmation_timeout':continue
                j=int(np.searchsorted(market.t,e['sweep_time']))+361
                if j>=market.n or not (market.valid[j] and market.valid[j-1] and market.selected[j] and market.tick[j]>0):continue
                ref=market.refs[int(market.days[j])].get(e['level'])
                if ref is None or ref[1]!=e['reference_start']:continue
                a,b=next((a,b) for n,a,b in engine.SPLITS if n==e['split'])
                if not a<=market.t[j]<b:continue
                p=ref[0];short=e['side']=='short'
                inside=market.c[j-1]<=p if short else market.c[j-1]>=p
                breach=market.h[j]>=p+market.tick[j] if short else market.l[j]<=p-market.tick[j]
                reclaim=market.c[j]<p if short else market.c[j]>p
                beyond=market.c[j]>=p+market.tick[j] if short else market.c[j]<=p-market.tick[j]
                qualifies=inside and (beyond if e['sweep_type']=='close_beyond' else breach and (reclaim if e['sweep_type']=='live_1m' else True))
                if qualifies:
                    assert (e['level'],e['side'],e['sweep_type'],e['timeframe'],int(market.t[j])) not in event_keys
                    suppressed+=1
                    if len(timeout_examples)<10:timeout_examples.append({'symbol':symbol,'stage':stage,'expired_event':e['event_id'],'level':e['level'],'side':e['side'],'sweep_type':e['sweep_type'],'timeframe':e['timeframe'],'candidate_open':pd.Timestamp(market.t[j],unit='ms',tz='UTC').isoformat()})
            row={'symbol':symbol,'stage':stage,'events_reproduced':len(events),'trade_rows_reproduced':len(trades),'qualifying_candidates_suppressed_after_timeout':suppressed}
            counts.append(row);print(json.dumps(row),flush=True)
result={'fresh_parses_matching_cached_frames':len(checked),'sampled_covered_funding_cashflows_checked':funding_checks,'replays':counts,'timeout_examples':timeout_examples,'scope':'Representative raw-to-result replay, not a full-window rerun; unchanged original engine; no corrected performance produced.'}
(OUT/'replay.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2),flush=True)
