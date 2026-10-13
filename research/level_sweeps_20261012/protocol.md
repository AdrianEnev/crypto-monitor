# Level-sweep archive study — frozen protocol v1

Frozen on 2026-10-12, before pilot outcomes or any holdout strategy results.
Purpose: reproducible historical research only. No execution, live-monitor integration,
email, credentials, account APIs, or AWS SDK. The user explicitly permits unauthenticated
reads of Binance's public archive directory listing. Production files and tests are
hash-checked before and after. All analysis code lives here; raw archives and parsed
market data live outside the repository under `~/Library/Caches/crypto-monitor-research/20261012`.

## Scope and evidence tiers

Target sample: 2022-10-01 00:00 through 2023-07-01 00:00 UTC (exclusive).
Warmup daily history begins 2022-07-01; minute history includes the previous month
for prior-only features. Train/exploration: Oct–Dec 2022. Validation: Jan–Mar 2023.
Final chronological holdout: Apr–Jun 2023. Freeze every rule here before opening
holdout results; do not optimize or select a winning variant. All grid comparisons
remain exploratory, with the holdout providing a separate historical check.
Dates cover different calendar quarters and two years, but not a representative
sample of every market regime. They are chosen for bounded reproducible computation,
not based on computed strategy outcomes.

Pilot: BTCUSDT, DOGEUSDT, FTTUSDT during November 2022, then BTCUSDT March 2023;
FTT is deliberately included as a historical distressed/delisted-contract case,
not chosen for profitability. Pilot validates data and execution mechanics; its
results belong only to train/validation, never the final holdout.

Resource bounds: at most 4 GiB compressed archive downloads and approximately
12 GiB total cache/output storage, 8 concurrent public requests, two retries for
transient errors. Inventory and estimate bytes before bulk 1-minute download.
Expand after the pilot to every available symbol selected by the daily historical
ranking during the nine-month sample. If limits bind, take complete calendar months
in chronological order; report omitted periods instead of silently shrinking scope.

Primary exact-eligibility result requires historical `TRADING`, `PERPETUAL`, USDT
quote AND margin asset, historical base classification, and tick-size metadata.
Current `exchangeInfo` is NOT historical evidence. The public kline archive does
not itself certify all those fields. Unless an authoritative dated metadata history
is obtained, **no exact primary result is claimed**. Separately label all produced
results `archive_eligibility_proxy` (potential eligibility/availability bias).

Proxy universe: enumerate ALL archived USD-M symbols, including retired names,
retain plain `*USDT` symbols without delivery suffixes, exclude the live project's
frozen stablecoin-base set. On each UTC midnight, rank the previous complete UTC
day's quote volume across that archive universe (descending, symbol ascending on
ties); retain top 20 for that day. This is a prior-only rolling 24-hour volume at
the daily membership boundary, not today's top 20 projected backward. Require
prior-day coverage; never use the current day's final volume for selection. Do not
require future data to select a market; missing future data is recorded as missing
execution coverage, with no replacement market. Continuous historical trading
status, margin asset and exact delisting instant remain uncertified.

Historical ticks: absent dated official filters, use the GCD lattice of prior UTC
day's 1-minute OHLC prices represented as integers at 1e-8 precision. This is an
explicit **inferred tick**, not a verified exchange tick; changes intraday may be
missed. Missing/invalid prior-day data prevents trading that symbol-day. Strictly
positive breach must be at least this inferred tick. Verify the live predicate
against Decimal arithmetic in pilot fixtures; equality uses integer price units.

## Reference levels (copied semantics, no live-module imports)

All timestamps UTC; candle timestamp = opening time. Ranges are start-inclusive,
end-exclusive; use completed, midnight-aligned daily candles with exactly one row
per day. High=max(high), low=min(low); incomplete reference coverage makes that
reference unavailable. Monday high/low: current week's Monday 00:00–Tuesday 00:00,
**unavailable all Monday**, active Tuesday through Sunday; previous Monday expires
at the next Monday 00:00. Previous week: previous Monday through current Monday.
Previous month: previous calendar month. Previous quarter: previous calendar
Jan–Mar / Apr–Jun / Jul–Sep / Oct–Dec quarter. This produces eight possible levels.

Test BOTH short and long at EACH of those eight levels (16 watch/direction pairs).
Shorts at low levels and longs at high levels are explicitly research extensions;
the monitor normally labels breach direction by the high/low side of its watch.
The 1-minute breach/reclaim boolean itself is reproduced exactly for the chosen
direction; no claim that this larger strategy is the live monitor's signal logic.

## Events, confirmation and expiry

A watch identity is `(symbol, reference name, source start/end, high/low, direction)`.
At most one pending setup per identity, sweep definition and confirmation timeframe.
Every setup has an immutable event ID including its sweep-minute opening timestamp.
The immediately previous minute must exist. A new event begins only on an inside
(or equal) previous close; repeated beyond-level prices do not create extra events.

Three independently evaluated definitions:
1. `live_1m`: short: previous close <= level, minute high >= level+tick, minute close
   < level; long: previous close >= level, low <= level-tick, close > level. Closed
   one-minute candles only. This event has already reclaimed, and must subsequently
   satisfy the SAME confirmation sequence below; a later above-close is therefore
   required. This deliberately separates the monitor predicate from the new setup.
2. `delayed_reclaim`: first one-minute breach >= one inferred tick beyond the level,
   with previous close inside/equal; same-minute reclaim is not required.
3. `close_beyond`: previous close inside/equal and a completed one-minute close at
   least one inferred tick beyond the level. Confirmation below still uses 5m/1h.

UTC-aligned 5-minute and 1-hour bars are derived from complete sets of base minutes.
For a short, after the event becomes observable, find the FIRST completed confirmation
bar closing strictly above the level. It may contain the event if its close is at
or after the event minute's end. Its IMMEDIATELY NEXT bar must close strictly below.
For a long, invert above/below. Thus the two confirmation bars are consecutive;
equality does not confirm. If the next bar fails, expire the setup (do not slide the
first bar forward). No additional events arm while a setup is pending. A setup
expires after six hours from the sweep minute's end, on source-period change,
on loss of universe membership, or on any missing base minute. Confirmation and
entry must occur strictly before the period/split end. Every discarded reason is
counted. A new setup can arm on a later qualifying minute after expiry/confirmation.

Control: `reclaim_only` starts on the delayed-breach event and enters on the first
strict one-minute reclaim, including the breach minute, without 5m/1h confirmation.
Its baseline stop is that breach minute's extreme. It is a descriptive control,
not a matched randomized experiment; signal incidence and risk can differ.

## Entries, exits, overlap and full grid

Baseline entry = next 1-minute open at the confirmation bar's exclusive ending
timestamp. Sensitivity = confirmation bar's last 1-minute close, decided only once
the bar is completed, with exposure starting in the next minute. This zero-latency
last-price fill is an idealized sensitivity, not a guaranteed executable close fill;
it never receives profit from the already observed signal candle.

Baseline short stop = high of the first above-close confirmation bar; baseline
long stop = low of the first below-close bar. Exclude nonpositive risk. Compare
1.00x, 1.25x, 1.50x and 2.00x entry-to-baseline-stop distance, and a stop one inferred
tick beyond the full excursion from sweep minute through reclaim confirmation.
Widened stops round away from entry to the inferred tick. Targets are SEPARATE:
(a) fixed 2R; (b) nearest valid low reference below entry for shorts / high reference
above entry for longs, among references available at entry. No valid opposing
reference => excluded for that target rule only. Target and stop remain frozen
through later reference updates. No trailing stops or partial exits.

Maximum holding = 24 hours; exit at the last completed minute close at that
boundary. Force close at the final available close before a data gap or chronological
split end, labeling that reason. Missing candles are never filled. Every entry is
simulated on 1-minute OHLC. Stop and target in same minute => stop first, counted.
At an opening gap through a stop, fill at the worse open; through a target, cap at
target (no favorable slippage credit). If open establishes an unambiguous exit,
use it before checking intraminute extremes. Signal-close sensitivity uses the same
future path. Exit timestamp for intraminute touch is the candle's exclusive end
(OHLC cannot provide the exact second); gap-open exits use opening timestamp.

Independent event-study trades may overlap across symbols, levels AND events;
no portfolio capital, leverage, position sizing, netting, or capacity model. All
variants share the same eligible signals; no suppression based on future exits.
Report correlated exposure explicitly; cumulative fixed-1R closed-trade drawdown
is descriptive and is NOT an account-equity drawdown.

Main grid: 3 sweep definitions × 2 timeframes × 5 stops × 2 targets × 2 entries =
120 variants, each evaluated for all 8 levels and 2 directions, separately by
chronological split. Control: 1 × 1 × 5 × 2 × 2 = 20 variants. Retain zero-trade cells
in the full grid. Fixed six-hour wait and 24-hour holding default are never selected
from holdout. No additional wait/hold tuning is planned in this bounded first run.

## Data validation and provenance

Only official ZIPs with matching official SHA-256 `.CHECKSUM` are admitted.
Manifest: URL, symbol, interval/type, month, archive bytes, expected/actual SHA-256,
download UTC, upstream last-modified, row counts, observed boundaries, duplicates,
missing and malformed rows, timestamp units, quality notes. Record failures too.
Previously cached downloads retain their original provenance. Archive revisions
are possible; pin the actual downloaded hash, do not silently replace files.

Validate row width, numeric values, integral times, nonnegative volume/trade count,
positive OHLC, price ordering, exact interval alignment and close-time boundary.
Remove ALL rows at duplicate timestamps, malformed rows, and out-of-file-period
rows. Never copy prices forward. Missing minutes break higher bars, pending setups,
prior-24h features and contiguous trade paths. Quantify missing and excluded data
separately from normal no-signal periods. Native daily archives supply completed
long reference history; compare their OHLC and selected archived 5m/1h bars with
minute-derived values during pilot. Any mismatch blocks expansion until explained.

## Costs, funding, features and statistics

Unit-risk gross R = directional price P&L / initial stop distance. Hypothetical
per-leg fee/slippage scenarios (basis points, NOT account-specific fee claims):
zero 0/0, low 2/1, base 5/2, stress 10/5. Charge entry and exit notionals; subtract
adverse slippage as a cost without changing touch rules or retuning stops/targets.
Also report funding-omitted net. Download verified official monthly fundingRate
archives when available. Apply signed published rates at timestamps strictly after
entry and through exit; use last fully observed 1-minute trade close as a documented
proxy for unavailable mark notional. Funding P&L is approximate even if rates are
verified. Missing funding coverage is labeled omitted, never invented as zero known
funding. Sensitivity: an additional cost OR credit of 1 and 5 bp of entry notional
per eight elapsed hours, prorated, independent of historical funding availability.

Features available at sweep: lagged previous-24h quote volume (excludes sweep
minute); prior daily cross-universe rank group 1–5 / 6–10 / 11–20; sweep quote volume
relative to lagged mean minute volume; directional overshoot / mean prior 60-minute
true range; sweep directional wick/body ratio (zero-body flagged); prior 24h realized
volatility from minute returns. All rolling features use only completed earlier
minutes. Overshoot and sweep OHLC are available once sweep minute completes.
Time-to-reclaim uses elapsed event-to-confirmation time, known only at confirmation.
Feature bins are fixed, except volatility low/middle/high uses training-only 33/67%
cut points frozen before holdout results are read. Report missing feature groups.

Trade audit rows include all identifiers, source boundaries, sweep/confirmation/
entry/exit times, prices, risk, target source, stop/target rules, exit reason, costs,
funding coverage, gross/net R, sweep features, and ambiguity flags. MAE/MFE use
complete pre-exit minutes plus exit price; exit-candle extrema are omitted when
ordering is unknowable (conservative lower-bound excursion measures).

Report signals, attempted/valid trades, exclusions, win rate (R>0), expectancy,
median R, profit factor (sum positive / abs sum negative; undefined if no losses),
fixed-1R closed-trade maximum drawdown ordered by exit time and deterministic ID,
holding time, MAE/MFE, before/after costs. Break down by each requested dimension
and produce the entire strategy grid including weak/zero variants. Do not pool
multiple variants as independent observations or describe their total as distinct
market opportunities. For uncertainty use 500 deterministic crossed symbol × UTC
calendar-week bootstrap replicates (seed 20261012), independently resampling symbols
and weeks and applying product multiplicities to trades, for mean net R per variant
and split. Report cluster counts; intervals with <5 symbols or <8 weeks are fragile.
CI comparisons and feature slicing are exploratory, unadjusted for multiple testing.

## Official references (facts; strategy and cost choices above are assumptions)

- Archive format, checksums and possible corrections:
  https://github.com/binance/binance-public-data
- Public archives: https://data.binance.vision/
- Current exchange filters and public funding-history API:
  https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
- Funding mechanics:
  https://www.binance.com/en/support/faq/detail/360033525031
- Example proving tick sizes change over time:
  https://www.binance.com/en/support/announcement/detail/8764f0d84e6b4c7abc97f036bb6f9ebc
