"""Point-in-time archive eligibility proxy and bounded minute download plan."""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
from data import HERE, CACHE, DAY, batch, iso, listing, parse

START=pd.Timestamp('2022-10-01',tz='UTC').value//10**6
END=pd.Timestamp('2023-07-01',tz='UTC').value//10**6

def daily_frames():
    inv=pd.read_csv(HERE/'daily_archive_inventory.csv'); result={}
    for symbol, rows in inv.groupby('symbol'):
        frames=[parse(symbol,'1d',m) for m in rows.month]
        frames=[f for f in frames if len(f)]
        if frames: result[symbol]=pd.concat(frames,ignore_index=True).sort_values('time').set_index('time',drop=False)
    return result

def select():
    frames=daily_frames(); rows=[]; coverage=[]
    for day in range(START,END,DAY):
        candidates=[]
        for s,f in frames.items():
            if day-DAY in f.index:
                candidates.append((s,float(f.loc[day-DAY,'quote_volume'])))
        candidates.sort(key=lambda v:(-v[1],v[0]))
        coverage.append({'date':iso(day),'candidate_symbols':len(candidates),'selected':min(20,len(candidates))})
        for rank,(symbol,qv) in enumerate(candidates[:20],1):
            rows.append({'day':day,'date':iso(day),'symbol':symbol,'rank':rank,'prior_day_quote_volume':qv,
                         'volume_group':'1-5' if rank<=5 else '6-10' if rank<=10 else '11-20',
                         'universe_tier':'archive_eligibility_proxy'})
    df=pd.DataFrame(rows); df.to_csv(HERE/'universe_daily.csv',index=False)
    pd.DataFrame(coverage).to_csv(HERE/'universe_coverage.csv',index=False)
    tasks=set()
    for r in df.itertuples():
        month=pd.Timestamp(r.day,unit='ms',tz='UTC').replace(day=1)
        for offset in [-1,0,1]:
            m=month+pd.offsets.MonthBegin(offset)
            if pd.Timestamp('2022-09-01',tz='UTC')<=m<pd.Timestamp('2023-07-01',tz='UTC'):
                tasks.add((r.symbol,'1m',m.strftime('%Y-%m')))
    inventory={}
    with ThreadPoolExecutor(8) as pool:
        futures={pool.submit(listing,f'data/futures/um/monthly/klines/{s}/1m/'):s for s in sorted(df.symbol.unique())}
        for future in as_completed(futures):
            symbol=futures[future]
            inventory[symbol]={Path(r['key']).name[-11:-4]:r['bytes'] for r in future.result()['items'] if r['key'].endswith('.zip')}
    plan=pd.DataFrame([{'symbol':s,'interval':i,'month':m,'available':m in inventory[s],'bytes':inventory[s].get(m,0)} for s,i,m in sorted(tasks)])
    plan.to_csv(HERE/'minute_download_plan.csv',index=False)
    total=int(plan.bytes.sum()); pilot_overhead=30*1024**2
    estimate={'study_start':iso(START),'study_end_exclusive':iso(END),'symbol_days':len(df),'selected_symbols':int(df.symbol.nunique()),
              'archives_requested':len(plan),'archives_available':int(plan.available.sum()),'compressed_minute_bytes':total,
              'estimated_raw_plus_parsed_bytes':total*5,'compressed_limit_bytes':4*1024**3,
              'selection':'prior complete UTC day quote volume; eligibility proxy only'}
    (HERE/'download_estimate.json').write_text(json.dumps(estimate,indent=2)+'\n')
    print(json.dumps(estimate,indent=2),flush=True)
    if total+pilot_overhead>4*1024**3: raise RuntimeError('Download budget exceeded; revise month subset before download')

def download():
    if not (HERE/'pilot_passed.json').exists(): raise RuntimeError('Pilot must pass before expansion')
    plan=pd.read_csv(HERE/'minute_download_plan.csv')
    batch((r.symbol,r.interval,r.month) for r in plan.itertuples())
    funding=plan[plan.month>='2022-10'][['symbol','month']].drop_duplicates()
    batch((r.symbol,'fundingRate',r.month) for r in funding.itertuples())

if __name__=='__main__':
    import sys
    if sys.argv[1]=='select': select()
    elif sys.argv[1]=='download': download()
