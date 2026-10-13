"""Validate representative native bars against complete minute aggregates."""
import json
import numpy as np
import pandas as pd
from data import HERE, MS, batch, parse
CASES=[('BTCUSDT','2022-11'),('DOGEUSDT','2022-11'),('FTTUSDT','2022-11'),('BTCUSDT','2023-03')]

def aggregate(df,minutes):
    x=df.copy(); x['bucket']=x.time//(minutes*MS)*(minutes*MS)
    g=x.groupby('bucket')
    out=g.agg(time=('time','first'),open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),
              volume=('volume','sum'),quote_volume=('quote_volume','sum'),trades=('trades','sum'),count=('time','size'))
    return out[(out['count']==minutes)&(out.time==out.index)].drop(columns='count')

def run():
    batch((s,i,m) for s,m in CASES for i in ['1m','5m','1h','1d','fundingRate'])
    rows=[]
    for s,m in CASES:
        base=parse(s,'1m',m)
        if base.empty:
            rows.append(dict(symbol=s,month=m,interval='all',status='missing pilot market',compared=0,mismatches=0));continue
        for interval,n in [('5m',5),('1h',60),('1d',1440)]:
            native=parse(s,interval,m).set_index('time',drop=False)
            derived=aggregate(base,n)
            common=derived.index.intersection(native.index)
            if not len(common): raise AssertionError(f'No comparable bars: {s} {m} {interval}')
            a=derived.loc[common];b=native.loc[common]
            price_bad=(a[['open','high','low','close']].to_numpy()!=b[['open','high','low','close']].to_numpy()).any(axis=1)
            trade_bad=a.trades.to_numpy()!=b.trades.to_numpy()
            volume_bad=~np.isclose(a.quote_volume,b.quote_volume,rtol=1e-9,atol=1e-6)
            bad=price_bad|trade_bad|volume_bad
            rows.append(dict(symbol=s,month=m,interval=interval,status='pass' if not bad.any() else 'mismatch',
                             compared=len(common),mismatches=int(bad.sum()),incomplete_derived=len(native)-len(common)))
    result=pd.DataFrame(rows);result.to_csv(HERE/'pilot_native_comparison.csv',index=False)
    print(result.to_string(index=False),flush=True)
    assert not result.mismatches.any(),'Native interval mismatch blocks expansion'
    assert (result.compared>0).sum()>=9,'Insufficient varied pilot coverage'

if __name__=='__main__':run()
