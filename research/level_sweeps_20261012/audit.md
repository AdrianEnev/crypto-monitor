# Audit: October 2022–June 2023

**Do not expand or present an exact primary result yet.** The published proxy
table is reproducible from stored trade rows, but historical eligibility/ticks
remain unverified, known SRM settlement overlaps remain in aggregates, and a
reproducible timeout defect violates the six-hour rule. No corrected performance
figures were calculated. Protocol, original research code/results, and live files
were left unchanged.

## Blocking findings

**B1 — Historical eligibility is not established.** `prepare.py:select` correctly
ranks prior-day volume across 196 archived candidates; 150 is the union of daily
selections, not a fixed surviving-symbol universe. Independently rebuilding all
273 daily rankings from ZIPs reproduces 5,460 symbol-days. However, a 2026 archive
inventory cannot prove a complete historical contract roster. Plain `USDT` names,
a frozen stablecoin exclusion set, and available candles do not establish dated
TRADING status, perpetual type, quote/margin assets, base classification, or
settlement boundaries. Retaining SRM reduces obvious survivor-only selection but
does not eliminate archive-availability/survivorship bias. No current-day volume
look-ahead was found; historical eligibility remains unknown.

**Smallest correction:** obtain an authoritative effective-dated contract roster
covering every candidate in the window, including retired/renamed contracts,
status changes, launch and settlement times, contract type and asset fields.
Apply eligibility before ranking; reconcile the roster against archive coverage.
If it identifies missing candidates, their preceding-day quote volumes and any
newly selected execution/reference candles are also needed. **Bounded recalculation:
yes, after those data exist; the present cache cannot certify completeness.**

**B2 — Inferred ticks cannot support the exact sweep claim.** In `engine.py`,
prior-day OHLC GCD controls the minimum breach for all three sweep definitions and
the reclaim-only control, excursion-stop padding, and widened-stop rounding.
Consequently it can change event incidence, pending-setup suppression, risk,
2R targets, exits, and costs expressed in R. Confirmation comparisons themselves
are strict above/below tests, without a tick threshold. GCD can overestimate a
tick when prices occupy a coarser lattice, and mixed or changing grids can make
it stale or too small. A synthetic probe changes only 0.01 to 0.02 and removes an
otherwise qualifying sweep. This proves sensitivity, not the number of historical
misclassifications. The Decimal oracle checks the supplied proxy tick, not its
historical authenticity. The report's cited [tick-change notice](https://www.binance.com/en/support/announcement/detail/8764f0d84e6b4c7abc97f036bb6f9ebc)
is from August 2023, outside this window, and supplies no complete in-window history.

**Smallest correction:** obtain initial official PRICE_FILTER tick sizes plus all
effective UTC changes for the relevant historical contracts, including intraday
changes; resolve ticks at the sweep and applicable stop-construction time instead
of inferring them. **Bounded recalculation: yes, with that history; exact impact
cannot be resolved from OHLC/GCD alone.**

**B3 — SRM is flagged, but not isolated from the main development aggregates.**
The [official notice](https://www.binance.com/en/support/announcement/detail/9a6676662bd5417c92267953f3819dd0)
sets settlement at **2022-11-15 04:30 UTC**. Independent inclusive-entry/crossing-exit
checks find **10 of 40 SRM variant rows, from one event**, crossing it; no entries
at/after cutoff or exits exactly at cutoff exist. All ten are correctly in
`known_eligibility_flags.parquet`, but `analyze.py` never joins or filters that
file. They remain in development counts, means and intervals; they do not affect
the April–June holdout. The prose discloses this accurately, but a separate flag
file is not separation within the aggregate results. This is a blocker to treating
those aggregates as executable evidence, rather than an undisclosed proxy-rule change.

**Smallest correction:** retain the original proxy output, join flags by trade ID,
and quarantine all known-invalid rows in a separate explicitly counted stratum
before producing any eligibility-screened summaries. Include entry `>= cutoff`
and holding `exit > cutoff` predicates. **Bounded recalculation: this separation
is possible now.** Replacing the affected trades with actual settlement P&L instead
requires the official settlement price, mechanics and applicable charges; flat
post-cutoff klines are insufficient. Neither step repairs other unknown eligibility.

**B4 — Six-hour expiry suppresses one extra minute.** `engine.py:133–159` sets
`until=i+361` for a sweep observed at `i+1`, then `busy=until+1` on timeout.
A qualifying candle opening at the six-hour expiry is therefore discarded even
though it completes after expiry. The synthetic case expires at 06:02 but loses
the 06:02–06:03 sweep. Raw-data replay finds **six such BTC candidates in development
and three in holdout**, across correlated watch/definition/timeframe records;
these are candidates, not necessarily confirmed trades. Existing tests check the
timeout status but miss rearming at the boundary.

**Smallest correction:** for a true six-hour timeout, release suppression at the
expiry index, preserving the separate reference/membership/gap/split boundary
rules. Add an expiry/rearming regression test, then rerun signals, trades and
summaries; do not merely append missed rows because pending state can cascade.
**Bounded recalculation: yes, with cached data.** Keep the protocol and original
code lock intact; record a versioned implementation correction. A corrected
April–June rerun remains a correction of an already inspected holdout, not a fresh
untouched test. No correction was applied in this audit.

## Material limitations

- **Multiplicity and dependence:** the 3,547,157 rows represent variants of
  181,716 confirmed event IDs, themselves overlapping across definitions,
  timeframes and references. Summaries/intervals group by a single variant and
  split; no pooled 3.55-million-observation significance test was found. The
  crossed symbol/week bootstrap follows the protocol. Its intervals and the
  extensive comparisons have **no multiple-testing adjustment**. The report
  explicitly acknowledges this; it accounts for multiplicity in interpretation,
  not by statistical correction. Neither positive cells nor comparative win
  counts establish profitability. Cluster resampling is not an independence
  guarantee, particularly across weeks or coincident references.
- **Execution/funding approximations:** fees/slippage are hypothetical per-leg
  scenarios, not historical account costs or an order-book fill model. Slippage
  is deducted without changing barrier touches. Signal-close fills are idealized,
  with exposure starting next minute. Funding uses verified rates but completed
  trade-close notionals, not mark prices; exit-minute settlement timing is uncertain.
  The 20,166 missing-coverage rows explicitly omit funding. `net_*_r` combines
  estimated funding where covered with omission elsewhere; it must not be called
  fully funding-adjusted. Exact funding requires settlement-time mark notionals,
  missing rate/schedule records and finer exit timing. The headline comparison
  consistently uses `net_base_no_funding_r`, as frozen.
- **Reproduction scope:** no exact primary result exists to reproduce; the report
  says so. Pinned raw ZIPs are locally available, but fresh inventory/download
  commands can encounter archive revisions. Preserve pinned manifests and cached
  inputs. This audit reproduced every headline row and sampled raw-to-trade
  replays, not a fresh simulation of all 150 symbols.

## Verified checks

- **Costs/chronology:** all 3,547,157 rows pass existing arithmetic, split-boundary,
  reference-time, confirmation-time and grid-total checks. Independent formulas
  reproduce all four fee/slippage scenarios and both funding sensitivities.
  Entry timing, stop-first ambiguous bars, worse stop-gap fills, capped target-gap
  fills, 24-hour holding and split/gap exits match the frozen rules in inspected
  code and fixtures, apart from B4's pending-expiry defect.
- **Holdout:** Oct–Dec training, Jan–Mar validation, Apr–Jun holdout match the
  protocol. Development minute loading excludes April onward; references and
  ranking use prior observations. Volatility thresholds independently reproduce
  from 56,320 training event records. Protocol/code hashes and recorded lock/run
  chronology agree; these records cannot independently prove that nobody previously
  viewed holdout data.
- **Reproduction:** all 6,720 grid cells reconcile, including 260 zero cells;
  counts and gross/base-net means reproduce for all active cells. All **420**
  variant/split bootstrap intervals and the report's **14** headline rows reproduce.
  Fresh offline parses of **67** archives match cached frames. BTC development/
  holdout, SRM development and FTT development replay **185,580 trade rows** and
  **16,855 event rows** exactly. Independent funding cashflow checks pass for
  **5,008** covered replay rows.
- **Validation rerun:** **287 live tests passed; 18 research tests passed**;
  module `--help` exited 0. All **3,711** retained ZIP hashes match manifest and
  checksum files. **37,873** native-bar comparisons, **1,461** reference checks and
  **775** Decimal predicate checks pass. All **40 live-project files**, their
  recorded source/config/documentation file set, and **461 original research
  artifacts** match recorded hashes. No original protocol, code or results changed.

Evidence and reproducible offline audit commands are in
[`audit_support/check.py`](audit_support/check.py),
[`checks.json`](audit_support/checks.json),
[`replay.py`](audit_support/replay.py), and
[`replay.json`](audit_support/replay.json); test logs are alongside them. Run the
scripts with this study's `.venv/bin/python -B`. They block network fetching and
write only new audit evidence; replay parses use a temporary directory. Missing
eligibility/tick metadata prevents an exact corrected primary result, so this
audit stops before corrected performance reporting or expansion.
