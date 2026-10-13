"""Read-only parity audit against the original pure reference/predicate source.

The simulator never imports the live monitor. This separate audit executes only
its pure core source in a temporary namespace; it calls no adapters or services.
"""
import hashlib,json,sys,types
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from pathlib import Path
import numpy as np
import pandas as pd
from data import HERE,SCALE
from engine import MarketData,periods,make_signals
from prepare import daily_frames
from pilot_data import CASES
from run import concat
from data import parse,DAY

def run():
    path=HERE.parents[1]/'crypto_monitor/core/level_sweep.py'
    module=types.ModuleType('_research_readonly_core_oracle');sys.modules[module.__name__]=module
    exec(compile(path.read_text(),str(path),'exec'),module.__dict__)
    dates=pd.date_range('2022-07-01','2023-07-01',freq='6h',tz='UTC');period_checks=0
    for day in dates:
        ours=periods(day.value//10**6)
        theirs=module.reference_periods(day.to_pydatetime())
        normalized={k:None if v is None else (int(v.start.timestamp()*1000),int(v.end.timestamp()*1000)) for k,v in theirs.items()}
        assert ours==normalized,(day,ours,normalized)
        period_checks+=1
    daily=daily_frames();checks=0
    for symbol,month in CASES:
        start=pd.Timestamp(month+'-01',tz='UTC');end=start+pd.offsets.MonthBegin(1)
        previous=(start-pd.offsets.MonthBegin(1)).strftime('%Y-%m')
        minute=concat([parse(symbol,'1m',previous),parse(symbol,'1m',month)])
        members=pd.DataFrame({'day':np.arange(start.value//10**6,end.value//10**6,DAY),'rank':1})
        m=MarketData(symbol,minute,daily[symbol],members,pd.DataFrame())
        events,_=make_signals(m,['train','validation'])
        for e in events:
            if e['sweep_type']!='live_1m':continue
            i=int(np.searchsorted(m.t,e['sweep_time']));dec=lambda x:Decimal(int(x))/Decimal(SCALE)
            candle=module.Candle(pd.Timestamp(m.t[i],unit='ms',tz='UTC').to_pydatetime(),module.MINUTE,dec(m.h[i]),dec(m.l[i]),dec(m.c[i]),True)
            side='high' if e['side']=='short' else 'low'
            price=m.refs[int(m.days[i])][e['level']][0]
            assert module.detect_sweep(side,dec(price),dec(m.c[i-1]),dec(m.tick[i]),candle),e
            checks+=1
    result={'pure_core_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'reference_timestamp_checks':period_checks,
            'pilot_live_predicate_decimal_checks':checks,'mismatches':0,'mode':'read-only isolated pure-core oracle; no runtime or adapter calls'}
    (HERE/'oracle_checks.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)

if __name__=='__main__':run()
