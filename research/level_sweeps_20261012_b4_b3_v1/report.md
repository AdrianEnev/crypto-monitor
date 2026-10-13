# B4-B3-v1: settlement-screened proxy sensitivity

This separate **analysis-only** version excludes the known SRM settlement flags
from completed B4-v1 trade outputs. No study signals or trades were regenerated,
no prices were changed, and the October 2022–June 2023 window was not expanded.
Original study and B4-v1 artifacts remain unchanged.

**April–June remains an inspected diagnostic sample, not a fresh untouched holdout.**
This is a **settlement-screened proxy sensitivity**, not an exact primary result.
Historical eligibility beyond this known SRM cutoff and true historical tick sizes
remain unresolved. No profitability claim follows from these exploratory comparisons.

## Screen and quarantine

The cutoff is **2022-11-15 04:30:00 UTC**. For SRM:

- Entry **at or after** the cutoff is ineligible.
- Entry before the cutoff with exit **strictly after** it crosses settlement.
- Entry before the cutoff with exit exactly at it is retained under this screen.

The known flags were joined by unique trade ID and checked against those predicates
across all source trade files. Every flag matched exactly once; no unflagged row
violated the predicates. **10 trade-variant rows from one event** were quarantined:
all crossed the cutoff, and none entered at/after it. The retained stratum contains
**3,547,247 rows**, down from **3,547,257**. All retained rows preserve their B4 values.

The [quarantined rows](quarantine/rows.csv), [Parquet equivalent](quarantine/rows.parquet)
and [quarantine cell counts](quarantine/cell_counts.csv) are separate from retained
aggregates. Existing exit/P&L fields there are explicitly labeled **B4 simulation
diagnostics, not settlement P&L**. Flat post-cutoff candles were not treated as
settlement prices; no settlement return was estimated. This screen removes affected
variants, not their entire event: other variants that exited before cutoff remain.

## Exactly which cells changed

All ten affected full-grid cells are **training / reclaim-only / 1 minute /
Monday high / long / opposing target**, spanning five stops and two entry rules.
Each loses one SRM row; these cells aggregate across symbols.
Base-net R retains B4's hypothetical 5 bp fee plus 2 bp slippage per leg, with
funding consistently omitted. Values below describe the sensitivity, not executable
settlement returns.

| Stop | Entry | Rows B4 → screened | Base-net mean R: B4 → screened | 95% cell CI: B4 → screened |
| --- | --- | ---: | ---: | --- |
| 1.25x | next_open | 4,812 → 4,811 | -1.407100 → -1.408057 | [-2.2702, -0.6455] → [-2.2501, -0.6896] |
| 1.25x | signal_close | 4,813 → 4,812 | -1.372870 → -1.373820 | [-2.2170, -0.6466] → [-2.1119, -0.5781] |
| 1.5x | next_open | 4,812 → 4,811 | -1.237569 → -1.238380 | [-1.9308, -0.5488] → [-1.9912, -0.5140] |
| 1.5x | signal_close | 4,813 → 4,812 | -1.188640 → -1.189440 | [-1.9807, -0.4550] → [-1.8682, -0.4709] |
| 2x | next_open | 4,812 → 4,811 | -0.991094 → -0.991715 | [-1.6180, -0.2415] → [-1.6277, -0.2546] |
| 2x | signal_close | 4,813 → 4,812 | -0.928643 → -0.929251 | [-1.5456, -0.1157] → [-1.6080, -0.2068] |
| baseline | next_open | 4,812 → 4,811 | -1.817553 → -1.818761 | [-2.7184, -0.8971] → [-2.9475, -0.9333] |
| baseline | signal_close | 4,813 → 4,812 | -1.737306 → -1.738497 | [-2.7987, -0.8140] → [-2.6725, -0.8664] |
| excursion | next_open | 4,813 → 4,812 | -1.218191 → -1.219267 | [-2.0441, -0.3717] → [-2.1209, -0.3889] |
| excursion | signal_close | 4,813 → 4,812 | -1.204778 → -1.205852 | [-2.0141, -0.4938] → [-2.1012, -0.5101] |

[Exact cell counts and gross/base-net means](comparison/affected_cells.csv) and
[full interval results](comparison/affected_cell_intervals.csv) retain precision
and cluster counts. These **cell-level** intervals were computed for this audit;
B4 previously published **variant-level** intervals. All affected cells have
57 → 56 symbols and 14 weeks. Both use the fixed 500-replicate crossed symbol ×
UTC-week bootstrap, seed 20261012, without multiplicity adjustment. Cluster removal
and finite Monte Carlo resampling contribute to interval differences.

## Exactly which summaries changed

All **23** B4 summary files were rebuilt. No summary rows were added or removed.
Only training-period values changed:

- **10 rows each** in development `full_grid`, `variants`, `clustered_ci`,
  `by_symbol` (SRM), `by_year` (2022), `by_volume_group` (11–20), and `exit_reasons`
  (`max_hold`) tables.
- **7 rows** in development baseline feature slices, all baseline stop/next-open:
  `level=monday_high`, `side=long`, sweep volume `>=5x`, overshoot `<0.5ATR`, reclaim
  `<5m`, wick/body `<1`, and volatility `high_train`.
- **1 row each** in `baseline_chronological_comparison.csv` and
  `baseline_target_comparison.csv`: training, reclaim-only, 1 minute, opposing target.
- `development_summary.json` changes only `simulated_trade_variants`:
  **2,389,852 → 2,389,842**.

All January–March validation and April–June diagnostic values/intervals are unchanged.
The full grid remains **6,720 cells**, including **260 zero-trade cells**. Armed and
confirmed signal counts are unchanged. Full-grid `trade_exclusions` now includes
the separate settlement quarantine; original simulation exclusions were not altered.

[All-table change inventory](comparison/table_changes.csv) includes unchanged files.
[Field-level differences](comparison/field_changes.csv) give every changed key,
field and before/after value. Comparisons ignore only numerical noise within
`rtol=atol=1e-12`; integer count differences are exact.

For the original variant-level intervals, all **420 B4 intervals** were reproduced
first. Original per-group random starting states were retained for the screened
calculation, so changing one group's cluster membership cannot shift unrelated
intervals. **10 training intervals changed; 410 are unchanged.** Strategy parameters,
feature thresholds, cost assumptions and bootstrap replicate count are unchanged.

## Validation and preservation

- **6 new screening tests**, **19 B4 research tests**, and **all 287 live tests passed**.
  Boundary equality, flag completeness/uniqueness, row preservation, and unaffected
  bootstrap streams are covered. Research signal fixtures are synthetic; the study
  itself was not rerun.
- Input/retained/quarantine counts reconcile exactly. Known flags and cutoff
  predicates agree; source/retained rows and signal counts are preserved.
- All **475 original-study artifacts**, **617 B4-v1 artifacts**, **40 live files**,
  their recorded file sets, and the frozen protocol remain unchanged.
- The copied analysis functions and frozen feature-bin configuration are unchanged;
  new analysis scripts match their version lock. No network or simulation calls
  occurred in the analysis pipeline.

See [validation evidence](validation/integrity.json), [version lock](analysis_lock.json),
[test logs](validation/screen_tests.txt), and [reproduction instructions](README.md).
