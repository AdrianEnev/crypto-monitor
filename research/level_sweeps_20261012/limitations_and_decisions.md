# Decisions and evidence limitations

These notes clarify the frozen protocol; they do not change its strategy parameters.

- Exact historical exchange eligibility and tick-size filters were **not reconstructed**
  in this run. Official exchange-information documentation describes current rules;
  an official [tick-size adjustment](https://www.binance.com/en/support/announcement/detail/8764f0d84e6b4c7abc97f036bb6f9ebc)
  confirms that projecting current filters backward is unsafe. This is a limitation
  of this study, not a claim that no historical records can exist. Every trade is
  labeled `archive_eligibility_proxy` and uses an explicitly inferred prior-day tick.
  No exact point-in-time primary performance result is delivered.
- The pilot's FTT case is a **distressed-market** example. The protocol's shorthand
  “distressed/delisted-contract case” does not establish FTT's delisting status.
  Actual evidence of a delisted constituent is SRMUSDT: Binance announced settlement
  at 2022-11-15 04:30 UTC, following publication on November 14. The archive-volume
  proxy selects SRM on November 14 and 15. Source:
  https://www.binance.com/en/support/announcement/detail/9a6676662bd5417c92267953f3819dd0
  The primary proxy is not patched ad hoc with one announcement; incomplete metadata
  and missing minute coverage are reported explicitly. A comprehensive future primary
  run should construct a dated eligibility table from such notices and historical filters.
- Raw market data is outside the checkout. Public listing requests use only the
  Binance-owned archive bucket, as explicitly authorized by the user. There are no
  AWS credentials, AWS SDK calls, private account APIs, email sends, or order requests.
- Official funding-rate archives are present. Rates are verified; position notional
  uses the last completed trade candle as a proxy because historical mark-price
  archives are outside this bounded first data collection. Funding omission and
  sensitivity columns remain separately available. Rates available after an event
  are used only to cost a realized holding period, never to generate a signal.
- Missing full-month rows in the manifest include ordinary prelisting/postdelisting
  periods. These are not all corrupt data. Selected-market execution coverage and
  reference coverage give the relevant excluded observation denominators.
- The isolated parity audit reads and executes only the original pure level-sweep
  source in a temporary namespace. It validates the research copy without importing
  any monitor adapter, persistence or runtime code. The research simulator itself
  has no live-project imports.
- This is a bounded nine-month first study, not exhaustive exchange history. The
  resource ceiling is a planning guardrail, not a claim that every remaining byte
  or possible period was exhausted. Further dates require a new frozen experiment
  and a new untouched holdout; the current holdout cannot be reused for tuning.
- Intraminute touches are timestamped at the minute's exclusive end. Actual touches
  may have occurred earlier; funding settlement exactly on that boundary is therefore
  uncertain. The funding-omitted comparison and funding sensitivities avoid treating
  this minute-resolution allocation as exact historical account bookkeeping.
