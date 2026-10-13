"""Descriptive research summaries and symbol/week clustered uncertainty."""
from __future__ import annotations
import hashlib,itertools,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
from data import HERE
from engine import LEVELS,STOPS,TARGETS,ENTRIES

TABLES=HERE/'tables';TABLES.mkdir(exist_ok=True)
CORE=['split','sweep_type','timeframe','stop_rule','target_rule','entry_rule']
METRIC_COLUMNS=['gross_r','net_low_no_funding_r','net_base_no_funding_r','net_stress_no_funding_r','net_base_r']

def files(stage,kind):return sorted((HERE/'results'/stage).glob('*_'+kind+'.parquet'))
def load(stage,kind,columns=None):
    return pd.concat([pd.read_parquet(p,columns=columns) for p in files(stage,kind)],ignore_index=True)

def lock():
    assert (HERE/'results/development/completed.json').exists()
    events=load('development','events',['split','status','event_id','prior_24h_volatility'])
    train=events[(events.split=='train')&(events.status=='confirmed')].drop_duplicates('event_id')
    vol=train.prior_24h_volatility.dropna();cuts=vol.quantile([1/3,2/3]).to_list()
    code={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in HERE.glob('*.py')}
    record={'locked_utc':pd.Timestamp.now(tz='UTC').isoformat(),'volatility_training_cutpoints':cuts,
            'training_events_for_cutpoints':len(vol),'strategy_changes_after_pilot':False,
            'no_winner_selection':True,'code_sha256':code,
            'protocol_sha256':hashlib.sha256((HERE/'protocol.md').read_bytes()).hexdigest()}
    (HERE/'development_locked.json').write_text(json.dumps(record,indent=2)+'\n')
    print('Training-only volatility thresholds frozen:',cuts,flush=True)

def metrics(g):
    out={'completed_trades':len(g),'unique_events':g.event_id.nunique(),'symbols':g.symbol.nunique(),
         'mean_holding_minutes':g.holding_minutes.mean(),'median_holding_minutes':g.holding_minutes.median(),
         'mean_mae_r':g.mae_r.mean(),'mean_mfe_r':g.mfe_r.mean(),
         'ambiguous_stop_first':int(g.ambiguous_stop_first.sum()),'funding_omitted_trades':int(g.funding_cost_r.isna().sum()),
         'mean_funding_cost_r_available':g.funding_cost_r.mean()}
    order=g.sort_values(['exit_time','trade_id'])
    for col in METRIC_COLUMNS:
        v=g[col].to_numpy(float);name=col.removesuffix('_r');loss=-v[v<0].sum()
        cum=np.r_[0,np.cumsum(order[col].to_numpy(float))]
        out.update({name+'_win_rate':float((v>0).mean()),name+'_expectancy_r':float(v.mean()),
                    name+'_median_r':float(np.median(v)),name+'_profit_factor':float(v[v>0].sum()/loss) if loss else np.nan,
                    name+'_max_closed_trade_drawdown_r':float(np.max(np.maximum.accumulate(cum)-cum))})
    for col in ['funding_sensitivity_1bp_cost_r','funding_sensitivity_1bp_credit_r','funding_sensitivity_5bp_cost_r','funding_sensitivity_5bp_credit_r']:
        out[col+'_mean']=g[col].mean()
    return out

def grouped(df,cols):
    rows=[]
    for key,g in df.groupby(cols,observed=True,dropna=False,sort=True):
        if not isinstance(key,tuple):key=(key,)
        rows.append(dict(zip(cols,key),**metrics(g)))
    return pd.DataFrame(rows)

def classify(df):
    lock=json.loads((HERE/'development_locked.json').read_text());a,b=lock['volatility_training_cutpoints']
    df['volatility_regime']=pd.cut(df.prior_24h_volatility,[-np.inf,a,b,np.inf],labels=['low_train','middle_train','high_train']).astype('string').fillna('missing')
    rules={
        'volume_at_sweep_group':('relative_sweep_volume',[-np.inf,1,2,5,np.inf],['<1x','1-2x','2-5x','>=5x']),
        'overshoot_group':('normalized_overshoot',[-np.inf,.5,1,2,np.inf],['<0.5ATR','0.5-1ATR','1-2ATR','>=2ATR']),
        'reclaim_group':('reclaim_minutes',[-np.inf,5,30,60,180,np.inf],['<5m','5-30m','30-60m','60-180m','>=180m']),
        'wick_body_group':('wick_body_ratio',[-np.inf,1,3,10,np.inf],['<1','1-3','3-10','>=10'])}
    for name,(col,bins,labels) in rules.items():df[name]=pd.cut(df[col],bins,labels=labels,right=False).astype('string').fillna('missing')
    df.loc[df.zero_body,'wick_body_group']='zero_body'
    return df

def bootstrap(df):
    rows=[];seed=np.random.default_rng(20261012)
    for key,g in df.groupby(CORE,observed=True,sort=True):
        dates=pd.to_datetime(g.entry_time,unit='ms',utc=True).dt.tz_localize(None).dt.to_period('W-SUN').astype(str)
        z=pd.DataFrame({'symbol':g.symbol.to_numpy(),'week':dates.to_numpy(),'r':g.net_base_no_funding_r.to_numpy()})
        sums=z.pivot_table(index='symbol',columns='week',values='r',aggfunc='sum',fill_value=0).to_numpy()
        count=z.pivot_table(index='symbol',columns='week',values='r',aggfunc='count',fill_value=0).to_numpy()
        ns,nw=sums.shape
        sw=seed.multinomial(ns,np.full(ns,1/ns),size=500);ww=seed.multinomial(nw,np.full(nw,1/nw),size=500)
        totals=np.einsum('bi,ij,bj->b',sw,sums,ww);n=np.einsum('bi,ij,bj->b',sw,count,ww)
        means=totals[n>0]/n[n>0]
        lo,hi=np.quantile(means,[.025,.975])
        rows.append(dict(zip(CORE,key),trades=len(g),symbols=ns,weeks=nw,expectancy_r=g.net_base_no_funding_r.mean(),
                         ci_95_low=lo,ci_95_high=hi,replicates=len(means),fragile_clusters=ns<5 or nw<8,
                         method='crossed symbol x UTC week bootstrap; base fees/slippage; funding omitted'))
    return pd.DataFrame(rows)

def summarize(stage):
    print('Loading',stage,flush=True)
    df=load(stage,'trades');df=classify(df)
    # Factors as categoricals reduce memory without changing underlying audit files.
    for col in ['symbol','split','level','side','sweep_type','entry_rule','stop_rule','target_rule']:
        df[col]=df[col].astype('category')
    events=load(stage,'events',['event_id','split','level','side','sweep_type','timeframe','status'])
    print(stage,'trade rows',len(df),'armed events',len(events),flush=True)
    detail=grouped(df,CORE+['level','side'])
    grid=[]
    for split in sorted(df.split.unique()):
        for kind,tfs in [('live_1m',[5,60]),('delayed_reclaim',[5,60]),('close_beyond',[5,60]),('reclaim_only',[1])]:
            for tf,stop,target,entry,level,side in itertools.product(tfs,STOPS,TARGETS,ENTRIES,LEVELS,['short','long']):
                grid.append(dict(zip(CORE+['level','side'],[split,kind,tf,stop,target,entry,level,side])))
    grid=pd.DataFrame(grid).merge(detail,on=CORE+['level','side'],how='left')
    sigcols=['split','sweep_type','timeframe','level','side']
    armed=events.groupby(sigcols).size().rename('armed_signals').reset_index()
    confirmed=events[events.status=='confirmed'].groupby(sigcols).size().rename('confirmed_signals').reset_index()
    grid=grid.merge(armed,on=sigcols,how='left').merge(confirmed,on=sigcols,how='left')
    for col in ['completed_trades','unique_events','symbols','armed_signals','confirmed_signals','ambiguous_stop_first','funding_omitted_trades']:
        grid[col]=grid[col].fillna(0).astype(int)
    grid['trade_exclusions']=grid.confirmed_signals-grid.completed_trades
    grid.to_csv(TABLES/(stage+'_full_grid.csv'),index=False)
    overall=grouped(df,CORE);overall.to_csv(TABLES/(stage+'_variants.csv'),index=False)
    bootstrap(df).to_csv(TABLES/(stage+'_clustered_ci.csv'),index=False)
    # Full variants by requested symbol/year/rank dimensions; individual level/side is in full_grid.
    for dimension in ['symbol','year','volume_group']:
        print('Summary',stage,dimension,flush=True)
        grouped(df,CORE+[dimension]).to_csv(TABLES/(stage+'_by_'+dimension+'.csv'),index=False)
    # Feature slices use the predeclared baseline stop/entry, with sweep/TF/target distinct.
    base=df[(df.stop_rule=='baseline')&(df.entry_rule=='next_open')]
    feature_frames=[]
    for dimension in ['level','side','volume_at_sweep_group','overshoot_group','reclaim_group','wick_body_group','volatility_regime']:
        frame=grouped(base,CORE+[dimension]).rename(columns={dimension:'feature_value'});frame['feature']=dimension;feature_frames.append(frame)
    pd.concat(feature_frames,ignore_index=True).to_csv(TABLES/(stage+'_baseline_feature_slices.csv'),index=False)
    exits=df.groupby(CORE+['exit_reason'],observed=True).size().rename('trades').reset_index();exits.to_csv(TABLES/(stage+'_exit_reasons.csv'),index=False)
    # One row per strategy; never treat duplicated variants as independent trades.
    summary={'stage':stage,'armed_events':len(events),'confirmed_events':int((events.status=='confirmed').sum()),
             'simulated_trade_variants':len(df),'distinct_event_ids_with_trades':df.event_id.nunique(),
             'symbols_with_trades':df.symbol.nunique(),'full_grid_cells':len(grid),'zero_trade_cells':int((grid.completed_trades==0).sum()),
             'ambiguous_trade_variants':int(df.ambiguous_stop_first.sum()),'funding_omitted_trade_variants':int(df.funding_cost_r.isna().sum())}
    (TABLES/(stage+'_summary.json')).write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary),flush=True)

if __name__=='__main__':
    if sys.argv[1]=='lock':lock()
    else:summarize(sys.argv[1])
