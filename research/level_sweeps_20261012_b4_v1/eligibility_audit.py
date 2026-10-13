"""Known-delisting annotations, not changes to the frozen proxy strategy."""
from pathlib import Path
import json
import pandas as pd
from data import HERE,PARSED,MS

NOTICES=[{'symbol':'SRMUSDT','settlement_utc':'2022-11-15T04:30:00Z','publication_utc':'2022-11-14T07:00:00Z',
          'source':'https://www.binance.com/en/support/announcement/detail/9a6676662bd5417c92267953f3819dd0'},
         {'symbol':'RAYUSDT','settlement_utc':'2022-11-15T04:00:00Z','publication_utc':'2022-11-14T07:00:00Z',
          'source':'https://www.binance.com/en/support/announcement/detail/9a6676662bd5417c92267953f3819dd0'}]

def run():
    rows=[];summaries=[]
    for notice in NOTICES:
        cutoff=pd.Timestamp(notice['settlement_utc']).value//10**6
        flagged=0;new=0;crossed=0;byvariant=[]
        for stage in ['development','holdout']:
            path=HERE/'results'/stage/(notice['symbol']+'_trades.parquet')
            if not path.exists():continue
            df=pd.read_parquet(path);bad=df[df.exit_time>cutoff].copy()
            for r in bad.itertuples():
                reason='entry_after_known_settlement' if r.entry_time>=cutoff else 'holding_crosses_known_settlement'
                rows.append({'trade_id':r.trade_id,'symbol':r.symbol,'split':r.split,'reason':reason,
                             'entry_time':r.entry_time,'exit_time':r.exit_time,'settlement_time':cutoff,'source_url':notice['source']})
            flagged+=len(bad);new+=int((bad.entry_time>=cutoff).sum());crossed+=int((bad.entry_time<cutoff).sum())
        archive=PARSED/(notice['symbol']+'-1m-2022-11.parquet')
        zero=None;observed=None
        if archive.exists():
            minute=pd.read_parquet(archive);after=minute[(minute.time>cutoff)&(minute.time<cutoff//(1440*MS)*(1440*MS)+1440*MS)]
            observed=len(after);zero=int((after.trades==0).sum())
        summaries.append(dict(**notice,flagged_trade_variants=flagged,entries_after_settlement=new,holdings_crossing_settlement=crossed,
                              observed_later_minutes_same_day=observed,zero_trade_later_minutes_same_day=zero))
    pd.DataFrame(rows,columns=['trade_id','symbol','split','reason','entry_time','exit_time','settlement_time','source_url']).to_parquet(HERE/'known_eligibility_flags.parquet',index=False)
    (HERE/'known_eligibility_audit.json').write_text(json.dumps({'notices':summaries,'note':'Separate known-ineligibility annotation; proxy trades are retained for full disclosure, not presented as executable evidence.'},indent=2)+'\n')
    print(json.dumps(summaries),flush=True)

if __name__=='__main__':run()
