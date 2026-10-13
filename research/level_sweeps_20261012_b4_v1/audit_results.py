"""Independent arithmetic, chronology, completeness, and source-integrity audit."""
import hashlib,json
from pathlib import Path
import numpy as np
import pandas as pd
from data import HERE,MS
from engine import SPLITS

def audit():
    rows=[];bounds={name:(a,b) for name,a,b in SPLITS}
    for stage in ['development','holdout']:
        total=0
        for path in sorted((HERE/'results'/stage).glob('*_trades.parquet')):
            df=pd.read_parquet(path)
            symbol=path.name.removesuffix('_trades.parquet')
            events=pd.read_parquet(path.with_name(symbol+'_events.parquet'))
            confirmed=set(events.loc[events.status=='confirmed','event_id'])
            assert set(df.event_id)<=confirmed,path
            assert df.trade_id.is_unique,path
            direction=np.where(df.side=='long',1,-1)
            assert (df.initial_risk>0).all()
            np.testing.assert_allclose(direction*(df.entry-df.stop),df.initial_risk,rtol=1e-8,atol=1e-10)
            assert (direction*(df.target-df.entry)>0).all()
            np.testing.assert_allclose(direction*(df.exit_price-df.entry)/df.initial_risk,df.gross_r,rtol=1e-8,atol=1e-7)
            np.testing.assert_allclose(df.gross_r-df.fee_base_r-df.slippage_base_r,df.net_base_no_funding_r)
            np.testing.assert_allclose(df.net_base_no_funding_r-df.funding_cost_r.fillna(0),df.net_base_r)
            assert (df.reference_end<=df.sweep_time).all()
            assert (df.confirmation_time>=df.sweep_observed_time).all()
            assert (df.confirmation_time<=df.sweep_observed_time+360*MS).all()
            assert (df.entry_time==df.confirmation_time).all()
            assert (df.exit_time>=df.entry_time).all()
            assert (df.holding_minutes<=1440).all()
            assert (df.loc[df.ambiguous_stop_first,'exit_reason']=='stop').all()
            for split,g in df.groupby('split'):
                a,b=bounds[split];assert (g.sweep_time>=a).all() and (g.entry_time<b).all() and (g.exit_time<=b).all()
            total+=len(df)
        grid=pd.read_csv(HERE/'tables'/(stage+'_full_grid.csv'))
        assert int(grid.completed_trades.sum())==total,(stage,total,grid.completed_trades.sum())
        assert (grid.trade_exclusions>=0).all()
        rows.append({'stage':stage,'trade_rows_checked':total,'full_grid_cells':len(grid),'status':'pass'})
    before=json.loads((HERE/'live_project_before.json').read_text());root=HERE.parents[1]
    after={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in before}
    assert before==after,'Live project files changed'
    frozen=json.loads((HERE/'protocol_freeze.json').read_text())
    assert frozen['sha256']==hashlib.sha256((HERE/'protocol.md').read_bytes()).hexdigest()
    lock=json.loads((HERE/'development_locked.json').read_text())
    changed=[name for name,digest in lock['code_sha256'].items() if hashlib.sha256((HERE/name).read_bytes()).hexdigest()!=digest]
    assert not changed,('Research code changed after holdout lock',changed)
    result={'completed_utc':pd.Timestamp.now(tz='UTC').isoformat(),'arithmetic_chronology_and_full_grid':rows,
            'unchanged_live_files':len(before),'protocol_unchanged':True,'holdout_locked_code_unchanged':True}
    (HERE/'results_audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)

if __name__=='__main__':audit()
