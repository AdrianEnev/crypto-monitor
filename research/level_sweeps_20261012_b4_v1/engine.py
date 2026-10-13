"""Isolated event-study simulator. No imports or calls into the live monitor."""
from __future__ import annotations
from collections import Counter
import hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd
from data import DAY, MS, SCALE, HERE, parse

PERIODS=['monday','previous_week','previous_month','previous_quarter']
LEVELS=[f'{p}_{s}' for p in PERIODS for s in ['high','low']]
STOPS=['baseline','1.25x','1.5x','2x','excursion']
TARGETS=['2R','opposing']; ENTRIES=['next_open','signal_close']
SPLITS=[('train','2022-10-01','2023-01-01'),('validation','2023-01-01','2023-04-01'),('holdout','2023-04-01','2023-07-01')]
SPLITS=[(name,pd.Timestamp(a,tz='UTC').value//10**6,pd.Timestamp(b,tz='UTC').value//10**6) for name,a,b in SPLITS]

def periods(ms):
    today=pd.Timestamp(ms,unit='ms',tz='UTC').normalize()
    monday=today-pd.Timedelta(days=today.weekday());month=today.replace(day=1)
    quarter=month.replace(month=(today.month-1)//3*3+1)
    out={'monday':None if today.weekday()==0 else (monday,monday+pd.Timedelta(days=1)),
         'previous_week':(monday-pd.Timedelta(days=7),monday),
         'previous_month':(month-pd.offsets.MonthBegin(1),month),
         'previous_quarter':(quarter-pd.DateOffset(months=3),quarter)}
    return {k:None if v is None else tuple(t.value//10**6 for t in v) for k,v in out.items()}

def references(daily, days):
    out={}; cache={}
    for day in days:
        refs={}
        for name,period in periods(int(day)).items():
            if period is None: continue
            a,b=period
            if (a,b) not in cache:
                f=daily.loc[(daily.time>=a)&(daily.time<b)]
                expected=np.arange(a,b,DAY)
                cache[a,b]=None if not np.array_equal(f.time.to_numpy(),expected) else (int(f.high.max()),int(f.low.min()))
            val=cache[a,b]
            if val is not None:
                for side,price in zip(['high','low'],val): refs[f'{name}_{side}']=(price,a,b)
        out[int(day)]=refs
    return out

class MarketData:
    def __init__(self,symbol,minute,daily,membership,funding):
        self.symbol=symbol
        start=int(minute.time.min())//DAY*DAY;end=(int(minute.time.max())//DAY+1)*DAY
        self.t=np.arange(start,end,MS,dtype=np.int64);self.n=len(self.t)
        df=minute.set_index('time').reindex(self.t)
        self.valid=df.open.notna().to_numpy()
        self.o=df.open.fillna(0).to_numpy(dtype=np.int64);self.h=df.high.fillna(0).to_numpy(dtype=np.int64)
        self.l=df.low.fillna(0).to_numpy(dtype=np.int64);self.c=df.close.fillna(0).to_numpy(dtype=np.int64)
        self.q=df.quote_volume.to_numpy(float)
        self.days=self.t//DAY*DAY;days=np.arange(start,end,DAY,dtype=np.int64)
        self.refs=references(daily,days)
        self.tick=np.zeros(self.n,dtype=np.int64)
        for d in range(1,len(days)):
            a=(d-1)*1440;b=d*1440
            if self.valid[a:b].all():
                tick=int(np.gcd.reduce(np.concatenate([self.o[a:b],self.h[a:b],self.l[a:b],self.c[a:b]])))
                self.tick[b:b+1440]=tick
        ranks=dict(zip(membership.day,membership['rank']))
        self.rank=np.array([ranks.get(int(d),0) for d in self.days],dtype=np.int16)
        self.selected=self.rank>0
        self.prior_q=pd.Series(self.q).rolling(1440,min_periods=1440).sum().shift(1).to_numpy()
        close=pd.Series(np.where(self.valid,self.c,np.nan))
        returns=np.log(close/close.shift(1))
        self.vol=returns.rolling(1440,min_periods=1440).std().shift(1).to_numpy()
        prev=close.shift(1).to_numpy()
        tr=np.maximum(self.h-self.l,np.maximum(np.abs(self.h-prev),np.abs(self.l-prev)))
        tr[~self.valid]=np.nan
        self.atr=pd.Series(tr).rolling(60,min_periods=60).mean().shift(1).to_numpy()
        self.funding=funding.sort_values('time').reset_index(drop=True) if len(funding) else funding
        self.ft=self.funding.time.to_numpy(dtype=np.int64) if len(funding) else np.array([],dtype=np.int64)
        if len(self.ft):
            fi=np.searchsorted(self.t+MS,self.ft,side='right')-1
            good=(fi>=0)&(fi<self.n)
            fi=np.clip(fi,0,self.n-1);good &= self.valid[fi]
            amounts=self.funding.rate.to_numpy()*self.c[fi]
            self.fc=np.r_[0,np.cumsum(np.where(good,amounts,0))]
            self.fb=np.r_[0,np.cumsum(~good)]
            # Archive calculation times jitter by milliseconds around settlement.
            # Round only for schedule coverage; retain actual times for cashflows.
            scheduled=np.rint(self.ft/MS).astype(np.int64)*MS
            gaps=np.r_[False,np.diff(scheduled)>self.funding.hours.to_numpy()[1:]*3_600_000]
            self.fg=np.r_[0,np.cumsum(gaps)]
        self.bars={}
        for n in [5,60]:
            valid=self.valid.reshape(-1,n).all(axis=1)
            self.bars[n]={'end':np.arange(n,self.n+1,n),'valid':valid,'close':self.c[n-1::n],
                          'high':self.h.reshape(-1,n).max(axis=1),'low':self.l.reshape(-1,n).min(axis=1)}
        self.gaps=np.flatnonzero(~self.valid)

    def features(self,i,direction,level):
        body=abs(int(self.c[i])-int(self.o[i]));wick=int(self.h[i])-max(int(self.o[i]),int(self.c[i])) if direction==-1 else min(int(self.o[i]),int(self.c[i]))-int(self.l[i])
        overshoot=int(self.h[i])-level if direction==-1 else level-int(self.l[i])
        q=self.prior_q[i];atr=self.atr[i]
        return {'prior_24h_quote_volume':q,'prior_day_volume_rank':int(self.rank[i]),
                'volume_group':'1-5' if self.rank[i]<=5 else '6-10' if self.rank[i]<=10 else '11-20',
                'sweep_quote_volume':float(self.q[i]),'relative_sweep_volume':self.q[i]/(q/1440) if q>0 else np.nan,
                'overshoot_price':overshoot/SCALE,'normalized_overshoot':overshoot/atr if atr>0 else np.nan,
                'wick_body_ratio':wick/body if body else np.nan,'zero_body':body==0,'prior_24h_volatility':self.vol[i]}


def make_signals(m, split_names=None):
    events=[]; counts=Counter(); prev=np.roll(m.c,1);prev_valid=np.roll(m.valid,1);prev_valid[0]=False
    for split,a,b in SPLITS:
        if split_names is not None and split not in split_names: continue
        in_split=(m.t>=a)&(m.t<b)
        for watch in LEVELS:
            vals={d:refs.get(watch,(0,0,0)) for d,refs in m.refs.items()}
            level=np.array([vals[int(d)][0] for d in m.days],dtype=np.int64)
            source=np.array([vals[int(d)][1] for d in m.days],dtype=np.int64)
            allowed=m.valid&m.selected&(m.tick>0)&(level>0)&in_split
            boundaries=np.flatnonzero(~allowed | (np.r_[False,np.diff(source)!=0]))
            for side,direction in [('short',-1),('long',1)]:
                inside=prev<=level if direction==-1 else prev>=level
                breach=m.h>=level+m.tick if direction==-1 else m.l<=level-m.tick
                reclaim=m.c<level if direction==-1 else m.c>level
                close_beyond=m.c>=level+m.tick if direction==-1 else m.c<=level-m.tick
                masks={'live_1m':breach&reclaim,'delayed_reclaim':breach,'close_beyond':close_beyond,'reclaim_only':breach}
                for kind,mask in masks.items():
                    indices=np.flatnonzero(allowed&prev_valid&inside&mask)
                    for tf in ([1] if kind=='reclaim_only' else [5,60]):
                        busy=-1
                        for i0 in indices:
                            i=int(i0)
                            if i<busy:
                                counts[split,watch,side,kind,tf,'suppressed_pending']+=1;continue
                            p=int(level[i]);tick=int(m.tick[i]);src=vals[int(m.days[i])]
                            break_pos=np.searchsorted(boundaries,i+1)
                            boundary=int(boundaries[break_pos]) if break_pos<len(boundaries) else m.n
                            until=min(i+361,boundary-1,m.n-1)
                            eid=hashlib.sha256(f'{m.symbol}|{watch}|{side}|{kind}|{tf}|{m.t[i]}'.encode()).hexdigest()[:24]
                            event={'event_id':eid,'symbol':m.symbol,'split':split,'year':pd.Timestamp(m.t[i],unit='ms').year,
                                   'level':watch,'side':side,'sweep_type':kind,'timeframe':tf,'sweep_time':int(m.t[i]),
                                   'sweep_observed_time':int(m.t[i]+MS),'reference_price':p/SCALE,
                                   'reference_start':int(src[1]),'reference_end':int(src[2]),'tick_size':tick/SCALE,
                                   'tick_source':'prior_day_inferred_lattice','universe_tier':'archive_eligibility_proxy',
                                   **m.features(i,direction,p)}
                            end=None;first=None;reason='confirmation_timeout';stop=None
                            if kind=='reclaim_only':
                                hits=np.flatnonzero(reclaim[i:until])
                                if len(hits):
                                    r=i+int(hits[0]);end=r+1;first=i;stop=int(m.h[i] if direction==-1 else m.l[i])
                            else:
                                bars=m.bars[tf];j=int(np.searchsorted(bars['end'],i+1));last=int(np.searchsorted(bars['end'],until,side='right'))
                                beyond=bars['close'][j:last]>p if direction==-1 else bars['close'][j:last]<p
                                hits=np.flatnonzero(beyond&bars['valid'][j:last])
                                if len(hits):
                                    k=j+int(hits[0]);first=int(bars['end'][k]);second=first+tf
                                    if second<=until and k+1<len(bars['end']) and bars['valid'][k+1]:
                                        yes=bars['close'][k+1]<p if direction==-1 else bars['close'][k+1]>p
                                        if yes:
                                            end=second;stop=int(bars['high'][k] if direction==-1 else bars['low'][k])
                                        else:reason='next_confirmation_failed';busy=second
                            if end is None:
                                if reason=='confirmation_timeout' and until<i+361: reason='membership_reference_gap_or_split_expiry'
                                # B4: the candle opening at six-hour expiry is no longer pending.
                                if reason=='confirmation_timeout':busy=max(busy,until)
                                elif reason!='next_confirmation_failed':busy=max(busy,until+1)
                                event.update(status=reason,confirmation_time=None,first_confirmation_time=None)
                            else:
                                busy=end
                                # All complete base minutes between event and entry must stay available.
                                assert end<m.n and allowed[end] and m.valid[i:end+1].all()
                                extreme=int(m.h[i:end].max()+tick if direction==-1 else m.l[i:end].min()-tick)
                                event.update(status='confirmed',confirmation_time=int(m.t[end]),
                                             first_confirmation_time=int(m.t[first] if tf!=1 else m.t[i]+MS),
                                             baseline_stop_units=stop,excursion_stop_units=extreme,
                                             entry_index=end,sweep_index=i,reclaim_minutes=(end-i-1),
                                             signal_close_units=int(m.c[end-1]))
                            counts[split,watch,side,kind,tf,event['status']]+=1
                            events.append(event)
    return events,counts


def exit_path(m,entry_index,entry,stop,target,direction,split_end,hold_minutes=1440):
    risk=abs(entry-stop)
    limit=min(entry_index+hold_minutes,m.n,int(np.searchsorted(m.t,split_end)))
    reason='max_hold' if limit==entry_index+hold_minutes else 'split_end'
    gi=np.searchsorted(m.gaps,entry_index)
    if gi<len(m.gaps) and m.gaps[gi]<limit:limit=int(m.gaps[gi]);reason='data_gap'
    if limit<=entry_index: return None
    o=m.o[entry_index:limit];h=m.h[entry_index:limit];l=m.l[entry_index:limit]
    stop_hit=h>=stop if direction==-1 else l<=stop
    target_hit=l<=target if direction==-1 else h>=target
    hits=np.flatnonzero(stop_hit|target_hit)
    ambiguous=False;exit_index=limit-1;price=int(m.c[exit_index]);exit_time=int(m.t[exit_index]+MS);intraminute=False
    if len(hits):
        j=int(hits[0]);exit_index=entry_index+j;op=int(o[j]);intraminute=True
        open_stop=op>=stop if direction==-1 else op<=stop
        open_target=op<=target if direction==-1 else op>=target
        if open_stop:price=op;reason='stop_gap';exit_time=int(m.t[exit_index]);intraminute=False
        elif open_target:price=target;reason='target_gap';exit_time=int(m.t[exit_index]);intraminute=False
        elif stop_hit[j]:
            price=stop;reason='stop';ambiguous=bool(target_hit[j]);exit_time=int(m.t[exit_index]+MS)
        else:price=target;reason='target';exit_time=int(m.t[exit_index]+MS)
    # Exclude the unknown-order exit bar's extrema; keep all known prior path and actual exit.
    path_end=exit_index if reason not in ['max_hold','split_end','data_gap'] else exit_index+1
    lows=m.l[entry_index:path_end];highs=m.h[entry_index:path_end]
    minprice=min(entry,price,int(lows.min()) if len(lows) else entry)
    maxprice=max(entry,price,int(highs.max()) if len(highs) else entry)
    mae=(maxprice-entry if direction==-1 else entry-minprice)/risk
    mfe=(entry-minprice if direction==-1 else maxprice-entry)/risk
    return dict(exit_price=price/SCALE,exit_time=exit_time,exit_reason=reason,
                gross_r=direction*(price-entry)/risk,ambiguous_stop_first=ambiguous,
                holding_minutes=(exit_time-int(m.t[entry_index]))/MS,mae_r=mae,mfe_r=mfe,
                exit_index=exit_index)


def funding_cost(m,entry_i,exit_time,direction,risk):
    if not len(m.ft):return np.nan,'omitted_missing_archive'
    a=int(np.searchsorted(m.ft,m.t[entry_i],side='right'))
    b=int(np.searchsorted(m.ft,exit_time,side='right'))
    # Require a following settlement as evidence of the schedule through the exit.
    if a==0 or b==len(m.ft):return np.nan,'omitted_unbounded_coverage'
    if m.fg[b+1]-m.fg[a]>0:return np.nan,'omitted_funding_gap'
    if m.fb[b]-m.fb[a]>0:return np.nan,'omitted_mark_proxy_gap'
    return direction*(m.fc[b]-m.fc[a])/risk,'verified_rates_trade_close_notional_proxy'


def simulate(m,events):
    trades=[];excluded=Counter()
    ends={name:b for name,a,b in SPLITS}
    for e in events:
        if e['status']!='confirmed':continue
        i=int(e['entry_index']);direction=-1 if e['side']=='short' else 1;tick=int(m.tick[i])
        for entry_rule in ENTRIES:
            entry=int(m.o[i] if entry_rule=='next_open' else e['signal_close_units'])
            base_dist=direction*(entry-int(e['baseline_stop_units']))
            for stop_rule in STOPS:
                if stop_rule=='excursion':stop=int(e['excursion_stop_units'])
                elif base_dist>0:
                    factor={'baseline':1.,'1.25x':1.25,'1.5x':1.5,'2x':2.}[stop_rule]
                    distance=base_dist if factor==1 else int(math.ceil(base_dist*factor/tick)*tick)
                    stop=entry-direction*distance
                else:stop=entry
                risk=direction*(entry-stop)
                for target_rule in TARGETS:
                    exkey=(e['split'],e['level'],e['side'],e['sweep_type'],e['timeframe'],stop_rule,target_rule,entry_rule)
                    if risk<=0:
                        excluded[exkey+('nonpositive_risk',)]+=1;continue
                    target_name='fixed_2R';target_start=None;target_end=None
                    if target_rule=='2R':target=entry+direction*2*risk
                    else:
                        refs=m.refs[int(m.days[i])];suffix='_low' if direction==-1 else '_high'
                        choices=[(name,price,a,b) for name,(price,a,b) in refs.items() if name.endswith(suffix) and direction*(price-entry)>0]
                        if not choices:
                            excluded[exkey+('no_opposing_reference',)]+=1;continue
                        target_name,target,target_start,target_end=min(choices,key=lambda row:(abs(row[1]-entry),row[0]))
                    if target<=0:
                        excluded[exkey+('nonpositive_target',)]+=1;continue
                    result=exit_path(m,i,entry,stop,target,direction,ends[e['split']])
                    if result is None:
                        excluded[exkey+('no_execution_coverage',)]+=1;continue
                    funding,funding_status=funding_cost(m,i,result['exit_time'],direction,risk)
                    exit_units=result['exit_price']*SCALE
                    trade={k:v for k,v in e.items() if k not in ['entry_index','sweep_index','baseline_stop_units','excursion_stop_units','signal_close_units','status']}
                    variant=f"{e['sweep_type']}|{e['timeframe']}m|{stop_rule}|{target_rule}|{entry_rule}"
                    trade.update(variant=variant,trade_id=e['event_id']+'|'+stop_rule+'|'+target_rule+'|'+entry_rule,
                                 entry_rule=entry_rule,stop_rule=stop_rule,target_rule=target_rule,
                                 entry_tick_size=tick/SCALE,
                                 entry_time=int(m.t[i]),entry=entry/SCALE,stop=stop/SCALE,target=target/SCALE,
                                 initial_risk=risk/SCALE,target_reference=target_name,target_reference_start=target_start,target_reference_end=target_end,
                                 funding_cost_r=funding,funding_status=funding_status,**result)
                    trade.pop('exit_index')
                    for label,fee,slip in [('zero',0,0),('low',2,1),('base',5,2),('stress',10,5)]:
                        fee_r=(entry+exit_units)*fee/10000/risk;slip_r=(entry+exit_units)*slip/10000/risk
                        trade['fee_'+label+'_r']=fee_r;trade['slippage_'+label+'_r']=slip_r
                        trade['net_'+label+'_no_funding_r']=result['gross_r']-fee_r-slip_r
                        trade['net_'+label+'_r']=result['gross_r']-fee_r-slip_r-(funding if np.isfinite(funding) else 0)
                    for bp in [1,5]:
                        penalty=entry/risk*(bp/10000)*(result['holding_minutes']/480)
                        trade[f'funding_sensitivity_{bp}bp_cost_r']=trade['net_base_no_funding_r']-penalty
                        trade[f'funding_sensitivity_{bp}bp_credit_r']=trade['net_base_no_funding_r']+penalty
                    trades.append(trade)
    return trades,excluded
