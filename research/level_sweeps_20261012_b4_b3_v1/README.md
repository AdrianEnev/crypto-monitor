# B4-B3-v1 — settlement-screened proxy sensitivity

This analysis-only version reads completed B4-v1 event/trade outputs. It does not
generate study signals, simulate trades, parse market candles, or fetch data.
The original October 2022–June 2023 study and B4-v1 remain immutable inputs.
See [report.md](report.md) for counts, exact changed cells/summaries and validation.

The SRM cutoff is **2022-11-15 04:30:00 UTC**. An entry at or after the cutoff is
ineligible. An entry before it with an exit strictly after it crosses settlement.
An entry before it with an exit exactly at it is retained by this explicit screen;
minute-resolution execution timing still has the original limitations.

`screen.py` joins the known flags by unique `trade_id` and verifies that flag
membership exactly matches the cutoff predicates. Duplicate IDs, unmatched flags,
or disagreement between flags and predicates abort the analysis. The retained view
is the immutable B4 rows minus these IDs; rows are never repriced or resimulated.

- `retained_sources.csv` pins source trade files and reconciles original, retained
  and quarantined counts. Unaffected trade files are referenced, not duplicated.
- `quarantine/rows.parquet` and `.csv` contain all flagged rows, their source,
  cutoff reason, and explicit provenance. Existing exit/P&L fields are **unchanged
  B4 simulation diagnostics, not settlement P&L**. No flat post-cutoff candle is
  used to calculate a settlement value. `cell_counts.csv` reports this separate
  stratum; `retained_SRMUSDT_trades.parquet` contains the 30 remaining SRM variants.
- `tables/` regenerates all 23 B4 summary files from retained rows, using unchanged
  event records and frozen feature bins. Full-grid `trade_exclusions` still equals
  confirmed signals minus retained trades; it now includes the analysis quarantine.
  Original execution exclusions and signal generation are unchanged. The added
  exclusions are separately enumerated in `quarantine/cell_counts.csv`.
- `comparison/table_changes.csv` covers all tables, including unchanged ones.
  `field_changes.csv` identifies every substantive changed field with its original
  and screened value, and any added/removed summary rows. Numeric comparison uses
  `rtol=atol=1e-12` solely to ignore floating-point round-trip/summation noise;
  integer count changes are exact. `affected_cells.csv` names all changed grid cells.

Intervals retain the original 500-replicate crossed symbol × UTC-week bootstrap,
seed 20261012, and mean base-cost R with funding consistently omitted. The wrapper
reproduces all B4 intervals and preserves each original group's starting random
state before recomputing with retained symbols/weeks. This prevents removing one
symbol from shifting the random stream for unrelated variants. It is not a new
strategy parameter or a multiple-comparison adjustment.

`comparison/affected_cell_intervals.csv` additionally gives before/after intervals
for the ten affected level/direction cells requested in this correction. B4 had
published variant-level intervals, not these cell-level intervals. These new
diagnostic comparisons use the same fixed bootstrap specification and paired
starting states within the affected-cell set; the method JSON records scope.

**April–June remains an inspected diagnostic sample, not a fresh untouched holdout.**
The machine-readable `holdout` name is retained for compatibility. Historical
eligibility beyond the known SRM cutoff and true historical tick sizes remain
unresolved. This is a **settlement-screened proxy sensitivity**, not an exact primary
result, complete delisting model, settlement P&L reconstruction or profitability
claim. Correlated variants and unadjusted exploratory intervals remain limitations.

`analysis_lock.json` pins the new analysis scripts; `analyze.py` is a byte-for-byte
copy of B4's analysis functions. Its `HERE`, input loader and bootstrap function are
redirected in memory by `screen.py`; no prior-version source is modified. The
copied `development_locked.json` supplies the original frozen volatility bins and
is not a new claim of an untouched holdout. `input_integrity.json` pins both source
versions and the live project.

From the project root, using the existing pinned research environment:

```sh
research/level_sweeps_20261012/.venv/bin/python -B -m unittest discover -s research/level_sweeps_20261012_b4_b3_v1 -p test_screen.py -v
research/level_sweeps_20261012/.venv/bin/python -B -m unittest discover -s research/level_sweeps_20261012_b4_v1 -p 'test_*.py' -v
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_b3_v1/screen.py partition
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_b3_v1/screen.py summarize development
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_b3_v1/screen.py summarize holdout
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_b3_v1/screen.py compare
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_b3_v1/cell_intervals.py
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_b3_v1/screen.py validate
.venv/bin/python -B -m unittest discover -s tests -t . -v
```

Study simulation and network functions are disabled by the analysis wrapper.
The B4 research tests exercise synthetic fixtures only; they do not rerun the
historical study. Reproduction writes only this analysis version's output files;
use a separate copy to preserve the delivered artifacts. Do not invoke the
inherited analysis CLI directly or refit its feature bins.
