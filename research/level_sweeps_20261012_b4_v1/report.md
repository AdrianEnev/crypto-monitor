# B4-v1 correction: six-hour timeout boundary

Only audit finding **B4** is corrected in this separate research version. The
frozen protocol, original study artifacts and live project are unchanged. The
window remains **2022-10-01 through 2023-07-01 exclusive**, with the original
150-symbol archive-eligibility proxy and all original strategy parameters.

**April–June 2023 is diagnostic, previously inspected data. It is not a fresh
untouched holdout.** The `holdout` identifier remains in machine-readable outputs
solely to preserve split and identity comparisons. No profitability conclusion
is drawn from this correction.

## What changed

The original engine kept a setup busy until one minute after its six-hour expiry.
For a true six-hour timeout, the corrected engine releases pending state at the
expiry index. A qualifying candle opening exactly then can arm once it closes.
Reference/membership/gap/split expiries and failed-confirmation handling are unchanged.
The complete inherited-source difference is [engine.patch](engine.patch).

The new [regression test](test_timeout_boundary.py) was added first and failed
against the uncorrected engine, as recorded in
[regression_before.txt](validation/regression_before.txt). It now passes: the
candle immediately before expiry remains suppressed, the candle opening at expiry
is eligible when closed, and its subsequent confirmation succeeds.

All **3,711** pinned raw ZIPs/checksums were verified and freshly parsed into a
separate external cache, with network fetching disabled. Those fresh files are
byte-identical to the original parsed files; comparison occurred after parsing.
Both simulation stages then ran from empty output directories across all selected
symbols. Signals, trades, exclusions, full grids, summaries, feature slices and
clustered intervals were rebuilt. No missed candidates were appended. Original
frozen feature thresholds were retained without refitting.

## Identity-level comparison

“Changed” means the same ID exists in both runs but a stored field differs.
Additions/removals use IDs, not row positions; nulls compare equal. All retained
event and trade identities have identical stored values.

| Split | Records | Original | Corrected | Added | Removed | Changed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Oct–Dec training | Events | 105,511 | 105,519 | 35 | 27 | 0 |
| Jan–Mar validation | Events | 119,785 | 119,802 | 62 | 45 | 0 |
| Apr–Jun diagnostic | Events | 109,043 | 109,059 | 62 | 46 | 0 |
| Oct–Dec training | Trade variants | 1,103,536 | 1,103,516 | 160 | 180 | 0 |
| Jan–Mar validation | Trade variants | 1,286,276 | 1,286,336 | 270 | 210 | 0 |
| Apr–Jun diagnostic | Trade variants | 1,157,345 | 1,157,405 | 380 | 320 | 0 |

Totals: **159 events added, 118 removed, 0 changed**; **810 trade-variant rows
added, 710 removed, 0 changed**. Corrected totals are **334,380 armed event
records**, **181,721 confirmed records**, and **3,547,257 trade-variant rows**.
These are correlated records, not independent market opportunities. Removals
demonstrate why a full pending-state replay was necessary.

[Comparison counts](comparison/counts.csv), [identity lists](comparison/changed_ids.csv),
[per-symbol counts](comparison/by_symbol.csv) and the adjacent added/removed Parquet
extracts provide row-level evidence. [Development grid deltas](comparison/development_full_grid_numeric_deltas.csv)
and [diagnostic grid deltas](comparison/holdout_full_grid_numeric_deltas.csv) compare
all numeric grid summaries. The `tables/` directory contains every rebuilt summary,
including the full **6,720-cell** grid with **260 zero-trade cells**, and all
**420** variant/split bootstrap intervals. Comparisons remain exploratory and
unadjusted for multiplicity; no variants were selected or ranked as winners.

## Unresolved limitations and SRM

Historical contract eligibility and true historical tick sizes remain unresolved
audit findings **B1/B2**. The universe and inferred prior-day tick proxy are
unchanged; this correction does not establish an exact historical primary result.

All **10 original SRM settlement-crossing trade flags** were regenerated and
matched to the original flags. They remain in
[`known_eligibility_flags.parquet`](known_eligibility_flags.parquet), joined by
`trade_id`. Flagged rows still remain in the proxy aggregates, as in the original
study: **B3 is deliberately not corrected**. Funding, fees, slippage, execution
approximations and overlapping exposure retain their original limitations.

## Validation

- **19 research tests passed**, including the new regression; **287 live tests
  passed**. `python -m crypto_monitor --help` exited 0.
- Arithmetic, chronology, split boundaries and grid counts passed for all
  **3,547,257** corrected trade rows. Execution and reference coverage are unchanged.
- **37,873** native-bar comparisons, **1,461** reference timestamp checks and
  **775** Decimal sweep checks passed with zero mismatches.
- All **475 original study artifacts**, all **40 live-project files**, their
  recorded file sets, and the frozen protocol remain unchanged. Corrected code
  matches its separate version lock; all raw/fresh-parsed hashes match.

See [validation evidence](validation/integrity.json),
[research test log](validation/research_tests.txt),
[live test log](validation/live_tests.txt),
[correction record](correction_record.json), and [reproduction instructions](README.md).
The code-lock field named `holdout_locked_code_unchanged` in the inherited audit
output refers to this correction's version lock, not a new untouched holdout.
