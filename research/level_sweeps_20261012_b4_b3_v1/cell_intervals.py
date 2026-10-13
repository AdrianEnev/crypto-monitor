"""Requested cell-level diagnostic intervals; B4 originally published variant intervals."""
import json
import pandas as pd
from screen import HERE, BASE, CELL, CORE, integrity, source_load, matched_bootstrap


def run():
    integrity()
    q=pd.read_parquet(HERE/'quarantine/rows.parquet')
    keys=q[CELL].drop_duplicates().sort_values(CELL)
    # Every quarantined row has this same level/direction, so CORE identifies these cells.
    assert q.level.nunique()==q.side.nunique()==1
    columns=CELL+['trade_id','symbol','entry_time','net_base_no_funding_r']
    baseline=source_load('development','trades',columns).merge(keys,on=CELL,validate='many_to_one')
    retained=baseline.loc[~baseline.trade_id.isin(q.trade_id)]
    after,before=matched_bootstrap(retained,baseline)
    paired=before.merge(after,on=CORE,suffixes=('_B4','_screened'),validate='one_to_one')
    paired['level']=q.level.iloc[0];paired['side']=q.side.iloc[0]
    assert len(paired)==len(keys)==10
    assert (paired.trades_B4-paired.trades_screened).eq(1).all()
    paired.to_csv(HERE/'comparison/affected_cell_intervals.csv',index=False)
    (HERE/'comparison/affected_cell_intervals_method.json').write_text(json.dumps({
        'label':'settlement-screened proxy sensitivity; diagnostic cell-level intervals',
        'scope':'Ten affected level/direction cells only; B4 published variant-level intervals, not these cell intervals',
        'method':'500 crossed symbol x UTC calendar-week bootstrap replicates; seed 20261012',
        'pairing':'B4 per-cell starting RNG states reused for screened cell; recompute using retained symbol/week counts',
        'metric':'mean net_base_no_funding_r',
        'multiplicity':'Unadjusted exploratory intervals; not profitability evidence',
        'new_strategy_parameters':False,
    },indent=2)+'\n')
    print(paired[CELL+['trades_B4','trades_screened','ci_95_low_B4','ci_95_high_B4','ci_95_low_screened','ci_95_high_screened']].to_string(index=False))


if __name__=='__main__':run()
