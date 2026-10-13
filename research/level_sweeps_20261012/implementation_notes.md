# Implementation audit before holdout

The first development run revealed millisecond jitter in official funding calculation
timestamps (for example BTC November 2022 includes +19 ms and +11 ms offsets).
The initial research implementation overreported funding gaps when adjacent jitter
exceeded one millisecond. It also used an open-time insertion index, which could
select the not-yet-completed settlement-minute close as its notional proxy.

Before any holdout run, corrected the implementation to follow the frozen protocol:

- Schedule continuity compares funding times rounded to the nearest minute against
  the published interval hours. Actual archived millisecond timestamps remain in
  the cashflow entry/exit test.
- Funding notional looks up the last candle whose **ending** timestamp is no later
  than the funding timestamp; the current incomplete minute is not used.
- Added a regression fixture with a +19 ms funding timestamp and a deliberately
  extreme future close, proving the notional uses only the completed prior candle.

Signals, price paths, stop/target rules, entry rules, costs excluding funding,
parameter defaults, and chronological splits were not changed. Development was
rerun; unchanged trade counts and gross/funding-omitted mean returns are checked
against `development_price_results_before_funding_fix.csv`. The initial lock record
is retained as `development_lock_before_funding_fix.json`; a new development lock
is written before the first holdout execution. The report separately flags known
SRM delisting overlaps without changing or filtering the frozen proxy grid.
