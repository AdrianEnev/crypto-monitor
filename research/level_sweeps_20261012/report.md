# Level-sweep archive study: bounded proxy results

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
pairs and three splits: **6,720 full-grid cells**, including
**260 zero-trade cells**. Weak variants are retained.
The strategy is an overlapping event study, not a capital-constrained portfolio.

## Data and exclusions

- Enumerated all archived plain USDT candidate symbols; **196** had daily history
  in July 2022–June 2023. Ranking uses only the previous completed UTC day's quote
  volume at midnight. **150 distinct symbols** enter the
  historical top-20 proxy across **5,460 symbol-days**.
- Compressed-minute estimate before bulk expansion: **1.312 GB**.
  Verified archive payload actually retained: **1.317 GB**;
  complete raw/parsed/listing cache: **3.477 GB**, outside the checkout.
- Manifest status counts: `{'verified': 3711, 'missing': 35}`. Admitted archives all match official SHA-256.
  Verified minute rows: **38,355,509**. Malformed rows:
  **0**; duplicate rows:
  **0**. No price forward-filling or
  discretionary corrections were made.
- Of **7,862,400 selected-market minutes**,
  **0** lack validated kline rows (0.000%).
  Another **10,080 observed minutes** lack a usable prior-day inferred
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
  [eligibility audit](known_eligibility_audit.json) flags **10 trade-variant
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

| Sweep | TF | Target | Trades | Gross mean R | Base net R | 95% clustered CI | Net win % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| close_beyond | 5m | 2R | 4709 | 0.033 | -0.612 | [-0.786, -0.427] | 32.9 |
| close_beyond | 5m | opposing | 4488 | -0.024 | -0.682 | [-0.932, -0.357] | 11.2 |
| close_beyond | 60m | 2R | 1357 | 0.080 | -0.096 | [-0.250, 0.084] | 38.2 |
| close_beyond | 60m | opposing | 1307 | -0.013 | -0.192 | [-0.393, 0.062] | 29.5 |
| delayed_reclaim | 5m | 2R | 4711 | 0.032 | -0.612 | [-0.763, -0.440] | 32.9 |
| delayed_reclaim | 5m | opposing | 4490 | -0.024 | -0.683 | [-0.947, -0.347] | 11.2 |
| delayed_reclaim | 60m | 2R | 1344 | 0.078 | -0.099 | [-0.257, 0.086] | 38.2 |
| delayed_reclaim | 60m | opposing | 1294 | -0.020 | -0.200 | [-0.397, 0.035] | 29.3 |
| live_1m | 5m | 2R | 2490 | 0.056 | -0.583 | [-0.770, -0.415] | 33.8 |
| live_1m | 5m | opposing | 2371 | 0.001 | -0.652 | [-0.945, -0.284] | 11.6 |
| live_1m | 60m | 2R | 1000 | 0.080 | -0.105 | [-0.260, 0.064] | 38.0 |
| live_1m | 60m | opposing | 962 | -0.038 | -0.227 | [-0.433, 0.016] | 28.3 |
| reclaim_only | 1m | 2R | 43684 | -0.012 | -1.742 | [-2.177, -1.289] | 25.1 |
| reclaim_only | 1m | opposing | 41523 | 0.034 | -1.734 | [-2.149, -1.259] | 5.6 |

Of these 14 baseline families, **0** have positive base-cost holdout
expectancy and **0** have a 95% clustered interval entirely above zero.
This count describes tested variants, not independent confirmations of an effect.
Intervals use 500 crossed symbol × UTC-week bootstrap replicates, preserve symbol
and date clustering, and are not adjusted for the extensive comparison grid.
Sparse groups and fewer-than-five-symbol/eight-week intervals are explicitly flagged.

In the fixed baseline comparisons, 1h has higher base-net mean R than 5m in
**6/6** sweep/target pairs. Opposing-reference
targets have higher mean R than 2R in **1/7**
sweep/timeframe pairs (including the control). Doubling stop distance has higher
mean net R than the baseline stop in **14/14** fixed
sweep/timeframe/target comparisons. These are descriptive directions of differences,
not independent significance tests: wider stops rescale both returns and costs in R,
and target/timeframe changes can change which observations are eligible.

The holdout contains **109,043 armed setup records**,
**59,306 confirmed records**, and
**1,157,345 simulated trade-variant rows**. Definitions,
timeframes and coincident references overlap, so these are not that many independent
market opportunities. **5,712** variant rows used the
conservative ambiguous-bar stop-first rule. Across active holdout full-grid cells,
base-net expectancy ranges from **-2.392R**
to **0.495R**; tiny cells must not be
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
- Research acceptance fixtures: **18 passed**. Native-bar pilot:
  **37,873** 5m/1h/daily comparisons, **zero mismatches**.
- Read-only core parity audit: **1,461** reference
  timestamp checks and **775** live-rule
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

Final validation: **287/287 live tests and 18/18 research checks passed**; module
help exited 0. All **40 live files**, the protocol, and holdout-locked research
code match their recorded hashes. Arithmetic/chronology/full-grid audits passed
for **3,547,157 trade-variant rows**. See [final_validation.json](final_validation.json)
and [results_audit.json](results_audit.json).
