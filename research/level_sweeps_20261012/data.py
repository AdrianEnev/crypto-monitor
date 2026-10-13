"""Research-only official archive inventory, verification, and quality checks."""
from __future__ import annotations
import calendar, hashlib, io, json, os, threading, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
CACHE = Path(os.environ.get('CRYPTO_RESEARCH_CACHE', Path.home() / 'Library/Caches/crypto-monitor-research/20261012'))
RAW = CACHE / 'raw'; PARSED = CACHE / 'parsed'; LISTINGS = CACHE / 'listings'
for p in [RAW, PARSED, LISTINGS]: p.mkdir(parents=True, exist_ok=True)
BASE = 'https://data.binance.vision/'
LIST = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
NS = {'s':'http://s3.amazonaws.com/doc/2006-03-01/'}
MS = 60_000; DAY = MS*1440; SCALE = 100_000_000
INTERVALS = {'1m':MS, '5m':5*MS, '1h':60*MS, '1d':DAY}
COLS = ['time','open','high','low','close','volume','close_time','quote_volume','trades','taker_volume','taker_quote','ignore']
STABLE = set('USDT USDC BUSD FDUSD TUSD USDP DAI USDS USDD USDE PYUSD USD1 RLUSD FRAX LUSD GUSD SUSD EURC EURI AEUR'.split())
_local=threading.local()
def get(url):
    if not hasattr(_local,'session'): _local.session=requests.Session()
    for attempt in range(3):
        try:
            r=_local.session.get(url,timeout=(15,60))
            if r.status_code==404: return r
            r.raise_for_status(); return r
        except (requests.RequestException, OSError):
            if attempt==2: raise
            time.sleep(1+attempt)

def listing(prefix, delimiter=None):
    path=LISTINGS/(hashlib.sha256((prefix+str(delimiter)).encode()).hexdigest()+'.json')
    if path.exists(): return json.loads(path.read_text())
    items=[]; prefixes=[]; token=None
    while True:
        args={'list-type':'2','prefix':prefix,'max-keys':1000}
        if delimiter: args['delimiter']=delimiter
        if token: args['continuation-token']=token
        r=get(LIST+'?'+urlencode(args)); root=ET.fromstring(r.content)
        items.extend({'key':v.findtext('s:Key',namespaces=NS),'bytes':int(v.findtext('s:Size',namespaces=NS)),
                      'last_modified':v.findtext('s:LastModified',namespaces=NS)} for v in root.findall('s:Contents',NS))
        prefixes.extend(v.text for v in root.findall('s:CommonPrefixes/s:Prefix',NS))
        if root.findtext('s:IsTruncated',namespaces=NS) != 'true': break
        token=root.findtext('s:NextContinuationToken',namespaces=NS)
        if not token: raise RuntimeError('Missing inventory continuation token')
    out={'prefix':prefix,'retrieved_utc':datetime.now(timezone.utc).isoformat(),'items':items,'prefixes':prefixes,'source':LIST}
    path.write_text(json.dumps(out)); return out

def key(symbol, interval, month):
    if interval=='fundingRate': return f'data/futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{month}.zip'
    return f'data/futures/um/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{month}.zip'

def month_bounds(month):
    d=pd.Timestamp(month+'-01',tz='UTC'); e=d+pd.offsets.MonthBegin(1)
    return d.value//1_000_000,e.value//1_000_000

def fetch(symbol, interval, month):
    k=key(symbol,interval,month); name=Path(k).name; meta=RAW/(name+'.json'); z=RAW/name
    if meta.exists():
        m=json.loads(meta.read_text())
        if m['status']=='verified' and z.exists():
            if hashlib.sha256(z.read_bytes()).hexdigest()!=m['actual_sha256']: raise ValueError('Cached archive changed: '+name)
            return m
        if m['status']=='missing': return m
    m={'symbol':symbol,'interval':interval,'month':month,'source_url':BASE+k,
       'checksum_url':BASE+k+'.CHECKSUM','download_utc':datetime.now(timezone.utc).isoformat(),
       'status':'error','local_archive':str(z),'expected_start_utc':month+'-01T00:00:00Z'}
    try:
        checksum=get(m['checksum_url'])
        if checksum.status_code==404: m.update(status='missing',quality_notes='Official checksum unavailable; archive not admitted')
        else:
            expected=checksum.text.split()[0].lower()
            if len(expected)!=64: raise ValueError('Malformed checksum')
            r=get(m['source_url'])
            if r.status_code==404: m.update(status='missing',quality_notes='Archive unavailable')
            else:
                actual=hashlib.sha256(r.content).hexdigest()
                m.update(expected_sha256=expected,actual_sha256=actual,archive_bytes=len(r.content),last_modified=r.headers.get('Last-Modified',''))
                if actual!=expected: m.update(status='unverified',quality_notes='Checksum mismatch: excluded')
                else:
                    z.write_bytes(r.content); (RAW/(name+'.CHECKSUM')).write_text(checksum.text)
                    m.update(status='verified',quality_notes='SHA-256 matches official checksum; no local corrections')
    except Exception as exc: m['quality_notes']=type(exc).__name__+': '+str(exc)
    meta.write_text(json.dumps(m,indent=2)); return m

def parse(symbol, interval, month):
    m=fetch(symbol,interval,month)
    if m['status']!='verified': return pd.DataFrame()
    p=PARSED/(Path(m['local_archive']).stem+'.parquet')
    if p.exists(): return pd.read_parquet(p)
    z=zipfile.ZipFile(m['local_archive']); members=z.namelist()
    if len(members)!=1: raise ValueError('Expected exactly one CSV')
    raw=z.read(members[0]); first=raw.splitlines()[0].split(b',')[0]
    has_header=not first.isdigit()
    if interval=='fundingRate':
        df=pd.read_csv(io.BytesIO(raw))
        expected=['calc_time','funding_interval_hours','last_funding_rate']
        if list(df.columns)!=expected: raise ValueError(f'Unknown funding columns {df.columns}')
        df=df.rename(columns={'calc_time':'time','last_funding_rate':'rate','funding_interval_hours':'hours'})
        a,b=month_bounds(month)
        mask=(df.time>=a)&(df.time<b)&np.isfinite(df.rate)&(df.hours>0)
        dup=df.time.duplicated(keep=False)
        m.update(raw_rows=len(df),malformed_rows=int((~mask).sum()),duplicate_rows=int(dup.sum()))
        df=df.loc[mask&~dup].sort_values('time').reset_index(drop=True)
        m.update(valid_rows=len(df),first_time_utc=iso(df.time.min()) if len(df) else '',last_time_utc=iso(df.time.max()) if len(df) else '')
    else:
        df=pd.read_csv(io.BytesIO(raw),header=0 if has_header else None,dtype=str)
        if df.shape[1]!=12: raise ValueError(f'Expected 12 columns, got {df.shape[1]}')
        df.columns=COLS
        for col in COLS: df[col]=pd.to_numeric(df[col],errors='coerce')
        # USD-M archive timestamp units are validated, never assumed from spot rules.
        a,b=month_bounds(month); dt=INTERVALS[interval]
        mask=np.isfinite(df[COLS].to_numpy(dtype=float)).all(axis=1)
        mask &= ((df.time>=a)&(df.time<b)&(df.time%dt==0)&(df.close_time==df.time+dt-1)).to_numpy()
        mask &= ((df.low>0)&(df.low<=df.open)&(df.low<=df.close)&(df.high>=df.open)&(df.high>=df.close)&(df.high>=df.low)).to_numpy()
        mask &= ((df[['volume','quote_volume','trades','taker_volume','taker_quote']]>=0).all(axis=1)&(df.trades%1==0)).to_numpy()
        dup=df.time.duplicated(keep=False)
        m.update(raw_rows=len(df),malformed_rows=int((~mask).sum()),duplicate_rows=int(dup.sum()),timestamp_units='milliseconds')
        df=df.loc[mask&~dup].sort_values('time').reset_index(drop=True)
        df['time']=df.time.astype('int64')
        for col in ['open','high','low','close']:
            units=df[col].to_numpy()*SCALE
            if np.any(np.abs(units-np.rint(units))>0.05): raise ValueError('Prices exceed validated 1e-8 lattice')
            df[col]=np.rint(units).astype('int64')
        expected=(b-a)//dt
        m.update(valid_rows=len(df),expected_rows=expected,missing_rows=int(expected-len(df)),
                 first_time_utc=iso(df.time.min()) if len(df) else '',last_time_utc=iso(df.time.max()) if len(df) else '',
                 internal_gap_intervals=int(np.maximum(np.diff(df.time)//dt-1,0).sum()) if len(df) else 0)
    df.to_parquet(p,index=False)
    (RAW/(Path(m['local_archive']).name+'.json')).write_text(json.dumps(m,indent=2))
    return df

def iso(ms): return pd.Timestamp(int(ms),unit='ms',tz='UTC').isoformat()
def batch(tasks,workers=8):
    tasks=list(dict.fromkeys(tasks)); done=0
    with ThreadPoolExecutor(workers) as pool:
        futures={pool.submit(parse,*task):task for task in tasks}
        for future in as_completed(futures):
            task=futures[future]
            try: future.result()
            except Exception as e:
                raise RuntimeError(f'Validation failed for {task}: {e}') from e
            done+=1
            if done%50==0 or done==len(tasks): print(f'archives {done}/{len(tasks)}',flush=True)
    manifest()

def manifest():
    rows=[json.loads(p.read_text()) for p in RAW.glob('*.zip.json')]
    if rows: pd.DataFrame(rows).sort_values(['symbol','interval','month']).to_csv(HERE/'data_manifest.csv',index=False)

def inventory():
    out=listing('data/futures/um/monthly/klines/','/')
    symbols=sorted(p.split('/')[-2] for p in out['prefixes'])
    symbols=[s for s in symbols if s.endswith('USDT') and '_' not in s and s[:-4] not in STABLE]
    print('All archived plain USDT candidate symbols:',len(symbols),flush=True)
    rows=[]
    with ThreadPoolExecutor(8) as pool:
        futures={pool.submit(listing,f'data/futures/um/monthly/klines/{s}/1d/'):s for s in symbols}
        for count,future in enumerate(as_completed(futures),1):
            s=futures[future]; vals=future.result()['items']
            for item in vals:
                if not item['key'].endswith('.zip'): continue
                month=Path(item['key']).name[-11:-4]
                if '2022-07'<=month<='2023-06': rows.append(dict(symbol=s,month=month,**item))
            if count%100==0: print('symbol inventories',count,'/',len(symbols),flush=True)
    pd.DataFrame(rows).sort_values(['symbol','month']).to_csv(HERE/'daily_archive_inventory.csv',index=False)
    (HERE/'archive_symbols.json').write_text(json.dumps({'source':LIST,'retrieved_utc':out['retrieved_utc'],'all_candidate_symbols':symbols,'historical_symbols':sorted({r['symbol'] for r in rows})},indent=2)+'\n')
    print('Historical daily archives:',len(rows),'symbols:',len({r['symbol'] for r in rows}),flush=True)

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(); p.add_argument('action',choices=['inventory','daily','manifest']); a=p.parse_args()
    if a.action=='inventory': inventory()
    elif a.action=='daily':
        inv=pd.read_csv(HERE/'daily_archive_inventory.csv'); batch((r.symbol,'1d',r.month) for r in inv.itertuples())
    else: manifest()
