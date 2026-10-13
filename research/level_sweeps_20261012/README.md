# Research-only level-sweep study

Start with `report.md` (written after the run), `protocol.md`, and the explicit
limitations in both. This directory is independent of the live monitor. It has its
own environment and acceptance fixtures. Nothing here is imported by the service.

Raw ZIPs, official checksum files, parsed price/funding data, and archive-listing
responses are stored OUTSIDE the checkout:
`~/Library/Caches/crypto-monitor-research/20261012`.
Override with `CRYPTO_RESEARCH_CACHE` to reproduce elsewhere. Never commit that
cache; `raw/` and `cache/` are also ignored here as a guard against accidental moves.
The files in `results/` are derived event/trade audits, not raw market archives.

Reproduce from the project root (network stages use only Binance public data):

```sh
python3 -m venv research/level_sweeps_20261012/.venv
source research/level_sweeps_20261012/.venv/bin/activate
python -m pip install -r research/level_sweeps_20261012/requirements-research.txt
python research/level_sweeps_20261012/data.py inventory
python research/level_sweeps_20261012/pilot_data.py
python research/level_sweeps_20261012/data.py daily
python research/level_sweeps_20261012/run.py pilot
python research/level_sweeps_20261012/prepare.py select
python research/level_sweeps_20261012/prepare.py download
python research/level_sweeps_20261012/run.py development
python research/level_sweeps_20261012/analyze.py lock
python research/level_sweeps_20261012/analyze.py development
python research/level_sweeps_20261012/run.py holdout
python research/level_sweeps_20261012/analyze.py holdout
python research/level_sweeps_20261012/eligibility_audit.py
python research/level_sweeps_20261012/audit_results.py
python research/level_sweeps_20261012/build_report.py
```

This is a frozen first study, not a general backtesting framework. Reproduction
may encounter revised upstream archives; retain and compare manifest hashes before
substituting data. `protocol_freeze.json` pins the pre-run assumptions.
`development_locked.json` pins the training-only volatility cut points and code
before the holdout runner is opened. Do not overwrite a frozen study to tune it;
start a new directory and reserve a new untouched period.

Artifact guide:

- `data_manifest.csv`: official archive provenance, SHA-256 verification, missing
  files and row-quality counts. Its absolute cache paths are local conveniences;
  source URLs and hashes identify data independently of this machine.
- `daily_archive_inventory.csv`, `minute_download_plan.csv`, `download_estimate.json`:
  available history and the resource estimate made before minute expansion.
- `universe_daily.csv`, `universe_coverage.csv`: prior-day top-20 eligibility proxy.
- `pilot_native_comparison.csv`, `pilot_execution_checks.csv`, `research_tests.txt`:
  pilot checks and synthetic execution acceptance tests.
- `results/{development,holdout}/*_events.parquet`: every armed setup, including
  failed/expired setups. Suppressed repeated candidates are counted in
  `signal_counts.csv`. An event is identified separately for each sweep definition,
  timeframe, level and direction; these are not independent economic events.
- `results/{development,holdout}/*_trades.parquet`: one row per event/entry/stop/target
  simulation; Zstandard-compressed Parquet. Unix timestamps are **milliseconds UTC**;
  price/risk fields are USDT. Columns ending `_units` in event files are integer
  prices scaled by 1e8. No account sizing is implied by unit-risk R.
- `trade_exclusions.csv`, `execution_coverage.csv`, `reference_coverage.csv` in each
  result directory quantify separate reasons that observations cannot be traded.
- `tables/*_full_grid.csv`: every predeclared strategy/level/direction/split cell,
  including zero trades. `*_variants.csv` pools levels/directions within ONE variant.
- `tables/*_by_symbol.csv`, `*_by_year.csv`, `*_by_volume_group.csv`: all variants.
  `*_baseline_feature_slices.csv` fixes baseline stop and next-open entry so variants
  are not accidentally counted repeatedly in a feature comparison.
- `tables/*_clustered_ci.csv`: crossed symbol/week bootstrap for base-cost expectancy
  with funding omitted consistently; unadjusted, exploratory confidence intervals.
- `known_eligibility_flags.parquet`: join by `trade_id` to flag the known SRM
  settlement overlap in the deliberately retained biased proxy results. Complete
  archive timestamps and flat zero-trade candles do not certify tradability.

`net_*_no_funding_r` excludes all funding. `net_*_r` applies verified rates with
trade-close notional proxies where available and omits funding otherwise; always
inspect `funding_status`/omitted counts. Sensitivity columns impose independent
hypothetical funding cost/credit scenarios, not account-specific fee claims.

Acceptance checks only (offline after data is cached):

```sh
python -B -m unittest discover -s research/level_sweeps_20261012 -p test_research.py -v
```

The live project's original suite remains `python -B -m unittest discover -s tests -t .`
using its own environment. Its files and tests are not modified by this research.
