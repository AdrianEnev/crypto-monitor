"""Produce the concise research report from completed, unfiltered summary tables."""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from data import HERE,CACHE

T=HERE/'tables'
def md(frame):
    cols=list(frame.columns);lines=['| '+' | '.join(cols)+' |','| '+' | '.join(['---']*len(cols))+' |']
    for row in frame.itertuples(index=False,name=None):lines.append('| '+' | '.join(str(x) for x in row)+' |')
    return '\n'.join(lines)
def fmt(x):return f'{x:.3f}' if pd.notna(x) else 'undefined'

def report():
    dev=json.loads((T/'development_summary.json').read_text());hold=json.loads((T/'holdout_summary.json').read_text())
    manifest=pd.read_csv(HERE/'data_manifest.csv');estimate=json.loads((HERE/'download_estimate.json').read_text())
    coverage=pd.concat([pd.read_csv(HERE/'results'/s/'execution_coverage.csv') for s in ['development','holdout']])
    refs=pd.concat([pd.read_csv(HERE/'results'/s/'reference_coverage.csv') for s in ['development','holdout']])
    variants=pd.concat([pd.read_csv(T/(s+'_variants.csv')) for s in ['development','holdout']])
    cis=pd.concat([pd.read_csv(T/(s+'_clustered_ci.csv')) for s in ['development','holdout']])
    keys=['split','sweep_type','timeframe','stop_rule','target_rule','entry_rule']
    variants=variants.merge(cis[keys+['ci_95_low','ci_95_high','fragile_clusters']],on=keys)
    baseline=variants[(variants.stop_rule=='baseline')&(variants.entry_rule=='next_open')].copy()
    baseline.to_csv(T/'baseline_chronological_comparison.csv',index=False)
    h=baseline[baseline.split=='holdout'].sort_values(['sweep_type','timeframe','target_rule'])
    rows=[]
    for r in h.itertuples():
        rows.append({'Sweep':r.sweep_type,'TF':f'{r.timeframe}m','Target':r.target_rule,'Trades':r.completed_trades,
                     'Gross mean R':fmt(r.gross_expectancy_r),'Base net R':fmt(r.net_base_no_funding_expectancy_r),
                     '95% clustered CI':f'[{r.ci_95_low:.3f}, {r.ci_95_high:.3f}]',
                     'Net win %':f'{r.net_base_no_funding_win_rate*100:.1f}'})
    verified=manifest[manifest.status=='verified'];minutes=verified[verified.interval=='1m']
    status=manifest.status.value_counts().to_dict()
    files=sum(p.stat().st_size for p in CACHE.rglob('*') if p.is_file())
    excluded=coverage.selected_minutes.sum()-coverage.available_selected_minutes.sum()
    missingtick=coverage.available_selected_minutes.sum()-coverage.feature_eligible_minutes.sum()
    native=pd.read_csv(HERE/'pilot_native_comparison.csv');oracle=json.loads((HERE/'oracle_checks.json').read_text())
    test_count=18
    eligibility=json.loads((HERE/'known_eligibility_audit.json').read_text())
    known_bad=sum(r['flagged_trade_variants'] for r in eligibility['notices'])
    full=pd.concat([pd.read_csv(T/(s+'_full_grid.csv')) for s in ['development','holdout']])
    hf=full[full.split=='holdout'];active=hf[hf.completed_trades>0]
    npos=int((h.net_base_no_funding_expectancy_r>0).sum());cpos=int((h.ci_95_low>0).sum())
    vcols=['sweep_type','timeframe','target_rule','entry_rule']
    stops=variants[(variants.split=='holdout')&(variants.entry_rule=='next_open')].pivot_table(index=vcols,columns='stop_rule',values='net_base_no_funding_expectancy_r').reset_index()
    stops.to_csv(T/'holdout_stop_comparison.csv',index=False)
    wider_pairs=stops[['baseline','2x']].dropna()
    wider_improved=int((wider_pairs['2x']>wider_pairs.baseline).sum())
    targets=baseline.pivot_table(index=['split','sweep_type','timeframe'],columns='target_rule',values='net_base_no_funding_expectancy_r').reset_index()
    targets.to_csv(T/'baseline_target_comparison.csv',index=False)
    target_pairs=targets[targets.split=='holdout'][['2R','opposing']].dropna()
    opposing_improved=int((target_pairs.opposing>target_pairs['2R']).sum())
    timeframe_pairs=h[h.sweep_type!='reclaim_only'].pivot_table(index=['sweep_type','target_rule'],columns='timeframe',values='net_base_no_funding_expectancy_r')[[5,60]].dropna()
    hour_improved=int((timeframe_pairs[60]>timeframe_pairs[5]).sum())
    # Cost scenarios and funding estimates across fixed baseline families.
    costs=h[['sweep_type','timeframe','target_rule','completed_trades','gross_expectancy_r','net_low_no_funding_expectancy_r','net_base_no_funding_expectancy_r','net_stress_no_funding_expectancy_r','net_base_expectancy_r','funding_omitted_trades']]
    costs.to_csv(T/'holdout_baseline_costs.csv',index=False)
    sensitivities=variants[variants.split=='holdout'].pivot_table(index=['sweep_type','timeframe','stop_rule','target_rule'],columns='entry_rule',values='net_base_no_funding_expectancy_r').reset_index()
    sensitivities.to_csv(T/'holdout_entry_sensitivity.csv',index=False)
    text=f'''# Level-sweep archive study: bounded proxy results

This study completed the frozen **2022-10-01–2023-06-30 UTC** sample. It is an
**exploratory archive-eligibility proxy**, not the requested exact historical
eligibility primary result. Historical contract status/margin filters and true
historical ticks were not reconstructed; ticks are inferred from prior-day prices.
No claim about future performance or recommendation to trade follows from these data.

## What was frozen and tested

[Protocol](protocol.md) records the assumptions before pilot results. Train is
Oct–Dec 2022, validation Jan–Mar 2023, final holdout Apr–Jun 2023. No variant was
selected or retuned using holdout outcomes. The training-only volatility cut points
and code were pinned in [development_locked.json](development_locked.json).

All eight references and both directions were evaluated. The exact one-minute live
predicate was separated from delayed-reclaim and close-beyond alternatives. Each
requires the first 5m/1h beyond-level close and its immediately next reclaim close.
Pending setups expire after six hours, reference/membership changes or gaps.
The reclaim-only control skips this confirmation pattern. Stops compare baseline,
1.25x/1.5x/2x risk distance and full excursion; targets compare fixed 2R versus the
nearest valid opposing reference. Entry is next-minute open, with an explicitly
idealized signal-close sensitivity. Maximum holding is 24 hours; stop-first is used
for ambiguous minute bars. See the protocol for exact chronology and gap policy.

This yields 120 main variants plus 20 control variants, each with 16 level/direction
pairs and three splits: **{len(full):,} full-grid cells**, including
**{int((full.completed_trades==0).sum()):,} zero-trade cells**. Weak variants are retained.
The strategy is an overlapping event study, not a capital-constrained portfolio.

## Data and exclusions

- Enumerated all archived plain USDT candidate symbols; **196** had daily history
  in July 2022–June 2023. Ranking uses only the previous completed UTC day's quote
  volume at midnight. **{estimate['selected_symbols']} distinct symbols** enter the
  historical top-20 proxy across **{estimate['symbol_days']:,} symbol-days**.
- Compressed-minute estimate before bulk expansion: **{estimate['compressed_minute_bytes']/1e9:.3f} GB**.
  Verified archive payload actually retained: **{verified.archive_bytes.sum()/1e9:.3f} GB**;
  complete raw/parsed/listing cache: **{files/1e9:.3f} GB**, outside the checkout.
- Manifest status counts: `{status}`. Admitted archives all match official SHA-256.
  Verified minute rows: **{int(minutes.valid_rows.sum()):,}**. Malformed rows:
  **{int(verified.malformed_rows.fillna(0).sum()):,}**; duplicate rows:
  **{int(verified.duplicate_rows.fillna(0).sum()):,}**. No price forward-filling or
  discretionary corrections were made.
- Of **{int(coverage.selected_minutes.sum()):,} selected-market minutes**,
  **{int(excluded):,}** lack validated kline rows ({excluded/coverage.selected_minutes.sum():.3%}).
  Another **{int(missingtick):,} observed minutes** lack a usable prior-day inferred
  tick/history. Reference availability is separately counted in each stage's
  `reference_coverage.csv`; Monday unavailability is intentional, and incomplete
  historical references are never replaced with partial extrema.
- Manifest full-month missing counts include prelisting/postdelisting periods;
  they should not be interpreted as all being corrupt data. Missing archives and
  malformed coverage are recorded, not silently admitted.
- SRMUSDT is selected on November 14–15, 2022 despite later delisting. Binance's
  [dated notice](https://www.binance.com/en/support/announcement/detail/9a6676662bd5417c92267953f3819dd0)
  specifies settlement on November 15 at 04:30 UTC. This avoids a current-symbol-only
  sample, while incomplete historical eligibility still prevents an unbiased claim.
- **Complete timestamps do not establish tradability:** SRM's archive continues
  with 1,169 flat zero-trade minutes later on its settlement day. The separate
  [eligibility audit](known_eligibility_audit.json) flags **{known_bad} trade-variant
  rows** crossing known settlement cutoffs. They remain in the explicitly biased
  proxy grid for disclosure and are **not executable-trade evidence**; join
  `known_eligibility_flags.parquet` by `trade_id` to identify them. This is not a
  comprehensive historical status reconstruction.

Official provenance: [archive format/checksums](https://github.com/binance/binance-public-data),
[public archives](https://data.binance.vision/), and
[current exchange/funding documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
Archive revisions and publication timing mean these are downloaded historical
observations, not authenticated snapshots of exactly what was available on each date.

## Holdout observations

The table fixes **baseline stop and next-minute-open entry**. Net uses hypothetical
5 bp fee + 2 bp slippage **per leg**, with funding omitted consistently for this
comparison. These are scenario assumptions, not an account-specific fee schedule.
R is per trade's initial risk; net win rate means net R > 0.

{md(pd.DataFrame(rows))}

Of these {len(h)} baseline families, **{npos}** have positive base-cost holdout
expectancy and **{cpos}** have a 95% clustered interval entirely above zero.
This count describes tested variants, not independent confirmations of an effect.
Intervals use 500 crossed symbol × UTC-week bootstrap replicates, preserve symbol
and date clustering, and are not adjusted for the extensive comparison grid.
Sparse groups and fewer-than-five-symbol/eight-week intervals are explicitly flagged.

In the fixed baseline comparisons, 1h has higher base-net mean R than 5m in
**{hour_improved}/{len(timeframe_pairs)}** sweep/target pairs. Opposing-reference
targets have higher mean R than 2R in **{opposing_improved}/{len(target_pairs)}**
sweep/timeframe pairs (including the control). Doubling stop distance has higher
mean net R than the baseline stop in **{wider_improved}/{len(wider_pairs)}** fixed
sweep/timeframe/target comparisons. These are descriptive directions of differences,
not independent significance tests: wider stops rescale both returns and costs in R,
and target/timeframe changes can change which observations are eligible.

The holdout contains **{hold['armed_events']:,} armed setup records**,
**{hold['confirmed_events']:,} confirmed records**, and
**{hold['simulated_trade_variants']:,} simulated trade-variant rows**. Definitions,
timeframes and coincident references overlap, so these are not that many independent
market opportunities. **{hold['ambiguous_trade_variants']:,}** variant rows used the
conservative ambiguous-bar stop-first rule. Across active holdout full-grid cells,
base-net expectancy ranges from **{active.net_base_no_funding_expectancy_r.min():.3f}R**
to **{active.net_base_no_funding_expectancy_r.max():.3f}R**; tiny cells must not be
interpreted as reliable extremes.

Detailed comparisons are retained without selecting winners:

- [Chronological baseline comparison](tables/baseline_chronological_comparison.csv)
  separates training, validation and holdout.
- [Stop comparison](tables/holdout_stop_comparison.csv),
  [target comparison](tables/baseline_target_comparison.csv), and
  [entry sensitivity](tables/holdout_entry_sensitivity.csv) hold the other listed
  factors fixed. Changed stops alter the R denominator as well as exit behavior;
  changed opposing targets can also alter eligibility, so mean differences are
  descriptive, not randomized causal effects.
- [Cost scenarios](tables/holdout_baseline_costs.csv) separate gross, low/base/stress
  costs and funding estimates. Published funding rates are verified when available;
  trade-close notional is a proxy for mark notional. Omitted coverage is counted.
  Each trade also retains ±1/±5 bp-per-eight-hours funding sensitivities.
- Both stages include full variants by symbol, year and prior-day volume group,
  and baseline feature slices by level, side, sweep-volume ratio, normalized
  overshoot, reclaim delay, wick/body ratio and training-defined volatility regime.
  These exploratory slices support audit and further hypothesis formation;
  multiplicity and overlapping events prevent strong standalone inference.
- Summary tables include counts, gross/net win rates, mean/median R, profit factor,
  fixed-1R closed-trade drawdown, holding time and conservative MAE/MFE. Drawdown is
  not account equity; unknown-order exit-minute extrema are excluded from MAE/MFE.

## Validation and what remains

- Existing live suite before research: **287 passed**; documented
  `python -m crypto_monitor --help` succeeded.
- Research acceptance fixtures: **{test_count} passed**. Native-bar pilot:
  **{int(native.compared.sum()):,}** 5m/1h/daily comparisons, **zero mismatches**.
- Read-only core parity audit: **{oracle['reference_timestamp_checks']:,}** reference
  timestamp checks and **{oracle['pilot_live_predicate_decimal_checks']:,}** live-rule
  Decimal checks, **zero mismatches**. Simulator code does not import the monitor.
- Final live-project tests and SHA-256 comparison are recorded in
  `final_validation.json` and `postflight_tests.txt` after report generation.

**Unfinished primary work:** reconstruct a complete dated eligibility/tick-size
history; verify settlement rules and historical mark-price notionals; run a new
frozen experiment on broader history with a fresh untouched holdout. Dates outside
Oct 2022–Jun 2023, alternative confirmation waits/holding horizons, order-book fills,
capacity, capital allocation and liquidation modeling were not tested here.
The nine-month subset is bounded first research, not exhaustive history or a claim
that every available resource was consumed. Actual close fills and stop orders can
differ from OHLC simulations. See [limitations](limitations_and_decisions.md).

Audit/reproduction instructions and the machine-readable artifact schema are in
[README.md](README.md). All live source, tests, settings, and documentation remain
outside this research directory and are checked for changes; no live database was
opened by the research scripts, and no email, credentialed AWS, or order calls occur.
'''
    (HERE/'report.md').write_text(text)
    print('Wrote report.md and comparison tables.',flush=True)

if __name__=='__main__':report()
