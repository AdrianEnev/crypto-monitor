# B4-v1: six-hour timeout correction

This is a separate implementation correction of the October 2022–June 2023
archive-eligibility proxy study. See `report.md` for completed-run comparisons and
validation. **April–June is diagnostic, previously inspected data, not a fresh
untouched holdout.** The internal `holdout` split name is retained for identity
matching and compatibility with the original artifacts.

Only B4 is corrected. The original study, its protocol and results, and live
project files are preserved. `engine.patch` is the complete change to inherited
source code. `test_timeout_boundary.py` was run against the uncorrected copy first:
`validation/regression_before.txt` records its expected failure. The correction
releases pending state at the six-hour expiry, allowing a sweep candle opening
there to arm when it closes. Other expiry and execution rules remain unchanged.

`correction_record.json` records scope and provenance. `original_integrity.json`
pins every original non-environment research artifact and all 40 live files.
`original_development_locked.json` is an unchanged copy of the original lock;
`development_locked.json` pins this correction's code and explicitly marks the
inspected diagnostic holdout. Original frozen training volatility thresholds are
retained, not refitted. No strategy parameter is selected from corrected results.

Every pinned ZIP and checksum is verified, then the original parser builds fresh
Parquet files under `~/Library/Caches/crypto-monitor-research/20261012-b4-v1`.
The original parsed cache is not an input. `parsed_cache_manifest.csv` pins the
fresh parse. Network fetching is disabled. Simulation starts with empty output
directories and recomputes every selected symbol in both stages, including all
pending-state transitions; missed candidates are never appended to old results.

The full grid, variant summaries, clustered intervals, feature slices and other
tables are regenerated. `comparison/counts.csv` reports added, removed, changed
and unchanged identities by split. “Changed” means the identity exists in both
runs but at least one stored field differs (null equals null; exact value
comparison). Different identities are reported as additions/removals, even if
they describe nearby market activity. `changed_ids.csv`, per-symbol counts,
changed-field counts, and before/after Parquet extracts allow row-level review.
Full-grid numeric deltas preserve all original cells without ranking winners.

Known SRM settlement flags are regenerated and checked against the original
flags. They remain a separate annotation, and flagged rows remain in the proxy
aggregates: **B3 is not corrected here.** Historical eligibility (B1) and true
historical ticks (B2) remain unresolved. All outputs retain the original proxy
status, overlapping variants, approximate funding treatment and unadjusted
exploratory comparisons. These outputs establish no profitability claim.

The completed run uses the original research environment and pinned dependency
file. From the project root, the commands were:

```sh
research/level_sweeps_20261012/.venv/bin/python -B -m unittest discover -s research/level_sweeps_20261012_b4_v1 -p 'test_*.py' -v
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py prepare
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py development
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py holdout
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py summarize development
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py summarize holdout
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py compare
research/level_sweeps_20261012/.venv/bin/python -B research/level_sweeps_20261012_b4_v1/correction.py validate
.venv/bin/python -B -m unittest discover -s tests -t . -v
.venv/bin/python -B -m crypto_monitor --help
```

Prepare and simulation deliberately refuse nonempty prior output locations.
Reproduction of those stages requires a separate clean workspace/cache containing
the pinned original inputs; do not delete or overwrite the delivered correction
to rerun them. Validation and integrity checks can be rerun against this version.
Do not call the inherited `analyze.py lock` or `build_report.py`: the former would
refit frozen bins and the latter contains original untouched-holdout prose.
