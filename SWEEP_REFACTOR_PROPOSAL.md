# Sweep monitor: proposed behavior and review questions

Prepared: October 13, 2026. Status: proposal for review; runtime changes have not begun.

This document records the latest decisions, explains the remaining choices, and proposes a cleaner terminal. Values used in examples are illustrative. They are not configuration defaults.

## 1. A simple example of the intended behavior

You launch the monitor on Wednesday and select 100 coins. It first shows a loading screen while it calculates the current levels and quietly checks the necessary history.

Suppose it discovers the following:

| Coin and level | What happened | What you should see |
|---|---|---|
| Coin A: previous-week low | Touched Tuesday; its configured delay has not expired. | A quiet swept indicator. No startup email or urgent message. |
| Coin A: previous-week high | Has not been touched during its current watch period. | An untouched indicator. Touching the low does not affect the high. |
| Coin B: previous-month high | Three separate visits occurred this month, with a clear move away between visits. | A count of three, if the proposed meaningful-visit counting rule is approved. Its notification eligibility is calculated separately. |
| Coin C: previous-quarter high | Touched two months ago, with no recent accepted sweep. | No old swept badge. It is treated as untouched/eligible again because that touch is outside the configured recent window. |
| Coin D: quarterly low | Historical data is incomplete. | An explicit unknown indicator rather than a claim that it is untouched. |

After loading finishes, Coin A reaches its previous-week high. Equality counts, so a high at 100 is touched when price reaches 100; it does not have to reach 100.01 or close below 100. The monitor marks that high swept and creates the chosen fresh notification. The low keeps its own state and deadline.

If `PWLH_delay` is five days, that high cannot produce another eligible alert until five full 24-hour days after its accepted sweep. A touch on day four does not move the deadline. A fresh revisit after the deadline can alert again, subject to the repeat-visit rule we still need to choose.

You stop the monitor and return a day later. It quietly records touches that occurred while it was off, restores the remaining delays and monthly counts, and sends no flood of historical alerts.

At a reference-period change, the new watch starts fresh. A new monthly reference does not inherit an old monthly delay, even if the two prices happen to match.

## 2. Decisions already made

| Decision | Agreed behavior |
|---|---|
| Sweep definition | Touching or passing a level counts. Equality counts. There is no one-minute closing or return-inside requirement for the first touch. |
| Live detection | Evaluate incoming prices as they arrive instead of waiting for a completed minute. |
| Independence | Each coin, named level, high/low side, and reference period has independent state. |
| Weekly repeats | Previous-week high/low may alert again after `PWLH_delay`. This replaces the earlier proposal to alert only once per week. |
| Higher timeframes | Monthly and quarterly watches may repeat after their own configured delays. |
| Fixed delay | Ignored touches during a delay do not extend it. The anchor is the accepted sweep's occurrence time. |
| Duration | A delay of X days means X times 24 elapsed hours, calculated in UTC. |
| New reference | A reference-period change clears the previous reference's delay and repeat state. |
| Historical discovery | Touches found while checking time the script was off are recorded quietly. |
| Quarterly visibility | Old quarterly swept markers should disappear when outside the `PQLH_delay` lookback. A two-month-old touch should not clutter the terminal. |
| Monthly count | Show a meaningful sweep count for the current monthly watch period, even after its delay expires. The exact counting rule is proposed in section 6. |
| State after delay | Treat a repeatable level as untouched/eligible again. Monthly counts remain a separate piece of information. |
| Configuration | Require an environment file and explicit values. Missing settings must not silently use hardcoded delay values. |
| Time | Store and calculate in UTC. Convert all terminal timestamps to `Europe/Sofia`. |
| Startup display | Show a temporary loading screen until startup preparation and the live handoff are complete. |

Monday levels still use the completed Monday range. Whether Monday watches remain first-only or also use a repeat delay needs one final answer: the latest reply explicitly changed the weekly rule but did not explicitly change Monday's repeat policy.

## 3. What the script currently does

Immediate touching is still not implemented. The earlier change added persistent duplicate suppression while retaining the old candle rule.

| Area | Current code | Required change |
|---|---|---|
| Detection | Ignores unfinished candles; requires a one-tick breach and a close back inside the level. | Accept a live touch with equality; remove the closing/reclaim conditions. |
| Repeat suppression | Saves only the first alert per coin, named level, side, and reference period. | Replace this with the agreed per-group delay policies and independent repeat state. |
| Historical coverage | A new coin's first live candle establishes a baseline. Existing coins recover missing minutes after their last saved candle. | Establish current watch status for every selected coin, including newly selected coins. |
| Historical notifications | Newly accepted recovered hits follow the same email path as live hits. | Historical discoveries update state without creating urgent notifications or email. |
| Terminal | Shows ten ranked alerts selected from the newest 100 saved alerts, including older periods. | Show current watch states, monthly counts, recent quarterly status, and a small fresh-event area. |
| Time display | Dashboard and plain terminal logs use UTC. | Display Sofia time while keeping calculations in UTC. |
| Startup | Dashboard starts after preparation finishes. | Start a loading view before network preparation. |
| Configuration | No sweep-delay environment variables or environment-file loader. | Add explicit required file loading and validation during implementation. |
| Email | Sends five-minute digests and retains pending mail, including while email is disabled. | Choose a fresh-email schedule and a policy for stale pending mail. |

Relevant code: [feed](crypto_monitor/adapters/binance_feed.py), [sweep rule](crypto_monitor/core/level_sweep.py), [engine](crypto_monitor/core/monitoring_engine.py), [startup](crypto_monitor/runtime/service.py), [recovery](crypto_monitor/runtime/gap_recovery.py), [SQLite state](crypto_monitor/persistence/sqlite_store.py), [terminal](crypto_monitor/ui/terminal_dashboard.py), and [email](crypto_monitor/adapters/ses_email.py).

Existing tests confirm the present behavior: `test_level_sweep.py` rejects exact touches and unfinished candles; `test_sqlite_store.py` checks first-only persistent claims; `test_gap_recovery.py` checks restart and chronological recovery; `test_terminal_dashboard.py` checks the ten-row display; `test_ses_email.py` checks five-minute scheduling and persisted retries. Those tests will need meaningful changes when implementation begins. They were inspected for this proposal, not rerun.

## 4. Which history belongs to a level?

The candles that establish a level and the time when that level is watched are different.

| Watch | Price is calculated from | Look for subsequent touches starting at | Reference expires |
|---|---|---|---|
| Previous-week low/high | Last week's completed range | This Monday, 00:00 UTC | Next Monday, 00:00 UTC |
| Current Monday low/high | This Monday's completed range | Tuesday, 00:00 UTC | Next Monday, 00:00 UTC |
| Previous-month low/high | Last month's completed range | This month's first day, 00:00 UTC | Next month's first day, 00:00 UTC |
| Previous-quarter low/high | Last quarter's completed range | This quarter's first day, 00:00 UTC | Next quarter's first day, 00:00 UTC |

Searching last week's candles for a touch of last week's high would always find the price that created the high. The useful search is this week's activity against that already-established price. The same distinction applies to months and quarters.

A touch from a prior, expired watch period does not carry into the current watch merely because the numeric price matches.

### Monday closure: checked against Binance

The completed Monday range becomes available at **Tuesday 00:00 UTC**. Binance's last included candle timestamp is Monday 23:59:59.999 UTC; Tuesday 00:00 UTC is the end-exclusive completion boundary.

I checked Binance's official USD-M BTCUSDT daily archive for Monday, October 20, 2025. Its single daily row opened at `2025-10-20 00:00:00 UTC` and ended at `2025-10-20 23:59:59.999 UTC`. Adding one millisecond gives Tuesday `2025-10-21 00:00:00 UTC`. This directly checks the daily alignment rather than relying only on the code's assumption. [Official archive sample](https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2025-10-20.zip).

Binance documents UTC as its exchange timezone and exposes candle opening/closing timestamps in its futures market-data reference. Its official archive repository identifies USD-M candles as `/fapi/v1/klines` data. [Binance futures market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data), [Binance public-data documentation](https://github.com/binance/binance-public-data#futures).

The terminal conversion does not move this boundary. Tuesday 00:00 UTC appears as 03:00 in Sofia during summer time and 02:00 during winter time. The code should use `Europe/Sofia` so daylight-saving changes are handled automatically, rather than adding a fixed number of hours.

The interval is complete at that boundary, but the exchange response can arrive later. Keep Monday's status checking until its completed candle is actually received and validated.

## 5. Fixed delays and quarterly visibility

There are three separate concepts:

1. **A touch:** price reaches or passes the level.
2. **A meaningful new visit:** price returns after a sufficient move away, according to the rule chosen in section 6.
3. **An accepted sweep:** the first touch, or a later meaningful visit that is eligible under the delay rule. It starts the next fixed delay, even if reconstructed silently from history.

Only an accepted fresh live sweep would create an urgent notification. A historical accepted sweep establishes the same deadline using its historical time, without sending email.

### Fixed five-day example

The example display times below are Sofia times during a week with no daylight-saving change; the actual calculation uses UTC.

| Event | Result |
|---|---|
| Monday, 10:00: accepted touch | Deadline becomes Saturday, 10:00. |
| Thursday, 10:00: another touch | No repeat alert. Saturday's deadline stays unchanged. |
| Saturday, 10:00: deadline passes | The level becomes untouched/eligible again. Time passing does not itself produce an alert. |
| Saturday, 12:00: a new eligible visit | Accept another sweep and start a new delay. |

If price is still parked above a high when the deadline expires, I recommend waiting for a fresh visit rather than issuing a new alert for the old visit. That remains a review decision.

Under that recommendation, the requested untouched label means the delay has expired. A later alert still requires the chosen fresh-visit rule. A detail view could say “delay ready; waiting for price to move away” when the price remains beyond the level, without adding another column to the main screen.

### Quarterly terminal window

Proposed interpretation: calculate `now in UTC minus PQLH_delay times 24 hours`, and show a quarterly swept badge only while its latest accepted sweep is recent enough. At the exact expiry boundary, remove the badge and treat the level as eligible again.

This means a quarterly touch two months ago disappears from the current terminal view when the delay is much shorter than two months. Its historical record can remain stored without being shown in the main terminal.

**Open choice:** should the quarterly window use the latest accepted sweep, or the latest meaningful touch even if it was ignored during cooldown? I recommend the accepted sweep. Otherwise a day-four ignored touch could keep the badge visible after the day-one sweep's five-day delay expired. That would be a separate visibility rule, even though the notification delay stayed fixed.

Your phrase “current day minus delay days” could also mean a calendar-date window. I recommend the exact rolling-hour window above, to match your delay calculations; the final questions ask you to confirm.

The display window does not necessarily limit the history required to establish correct state. Suppose day 1 was accepted, day 4 was ignored, and you restart on day 7 with a five-day delay. Looking only at the last five days could incorrectly treat day 4 as the anchor and delay alerts until day 9. The saved anchor or an earlier chronological replay must establish that the real deadline was day 6. Only then should the short quarterly display filter be applied.

## 6. Monthly counts without counting consolidation as thousands of sweeps

A fixed delay limits notifications, but it does not explain which price movements deserve to be counted as separate visits. The monthly count needs an additional rule.

### Recommended idea: move away, then return

For a high at 100, an illustrative retreat of 0.5% means price must fall to 99.50 or lower before another return to 100 counts as a new visit. For a low at 100, price must rise to 100.50 or higher before returning to 100. The low and high each track their own movement.

The initial touch still counts immediately. The move-away requirement governs later visits; it does not add a closing requirement to the first touch. Moving farther above a high does not prepare another high sweep; the retreat should be back below the high. The rule is mirrored for lows.

**The 0.5% value is an example, not a default.** A chosen threshold would have to be explicitly configured.

| Method | Why it might help | Main limitation |
|---|---|---|
| Percentage of the level price | Easy to understand; scales across differently priced coins. | The same percentage can feel large for a quiet coin and small for a volatile one. |
| Number of price ticks | Uses each market's smallest permitted price increment. | A few ticks may still count ordinary consolidation as many visits. |
| Recent-volatility distance | Adjusts to each coin's recent movement. | Adds more data, moving thresholds, and configuration. |

My recommendation for an initial implementation is an explicitly configured percentage retreat. A possible monthly name is `PMLH_rearm_pct`. Whether quarterly, weekly, or Monday repeats use the same kind of minimum retreat is still open.

### Worked monthly example

Assume a monthly high of 100, a five-day alert delay, and the illustrative 0.5% retreat requirement:

1. Monday: price first touches 100. Monthly count becomes one; a fresh eligible event can notify. The delay ends Saturday at the corresponding time.
2. Tuesday: price oscillates between 99.95 and 100.05 hundreds of times. It never reaches the required 99.50 retreat. The count stays one.
3. Wednesday: price falls to 99.40 and later returns to 100. This is a meaningful second visit. Under the recommended counting interpretation, count becomes two, but no email is sent because the fixed delay is still active. The Saturday deadline does not move.
4. Saturday: if price stays above 100 as the delay expires, no new visit or alert is manufactured.
5. Later: price retreats to 99.50 or below and returns to 100 after eligibility resumes. Count becomes three, an alert is allowed, and a new delay begins.

A retreat may happen before the delay expires, provided its subsequent return happens after expiry and represents a new visit. If a return happens during cooldown, that visit is consumed; it must not linger as a reason to alert automatically at expiry.

### Count and notification eligibility

Your request to count monthly sweeps “regardless of the delay variable” suggests this proposed split:

- Count every meaningful retreat/return during the current monthly watch period.
- During the delay, suppress notifications and leave the deadline unchanged.
- After the delay, display the level as untouched/eligible while retaining the month's count.

For example, `PMH: untouched · 4 visits` means it is eligible again and has had four meaningful visits this month. It does not claim that price has never visited it. The terminal can use a compact symbol for eligibility to keep this distinction clear.

This needs your confirmation because the earlier statement that ignored touches are “simply ignored” could alternatively mean they should not increase the count either. Under that alternative, the Wednesday visit in the example would leave the count at one.

Counts reset when the monthly reference changes. Restarting within the same month restores or quietly reconstructs the count.

### What historical candles can establish

Historical candle highs/lows can establish that a level was reached within that candle. They cannot always reveal how many retreat/return sequences occurred or their order. A one-minute candle containing both 99.40 and 100.05 does not say which came first or whether there were several visits.

For older data, the choices are a conservative minute-based count, an explicitly approximate count, or replaying finer trade history where it is available. The proposal should not promise an exact monthly count from candle ranges alone. Live counts can use the sequence of incoming prices.

## 7. Required environment file

During implementation, create and load the project's environment file before any network startup work. The delay settings below are required and have no hidden numeric fallback:

| Variable | Meaning | Value still needed |
|---|---|---|
| `PWLH_delay` | Previous-week low/high delay, applied separately to each side and coin. | Number of days. |
| `PMLH_delay` | Previous-month low/high delay. | Number of days. |
| `PQLH_delay` | Previous-quarter low/high delay and quarterly display lookback. | Number of days. |
| `MONLH_delay` | Monday low/high delay, if Monday repeats are approved. | Conditional on the Monday policy. |
| `PMLH_rearm_pct` | Minimum retreat before another monthly visit counts, if the percentage proposal is approved. | Percentage. |

Illustrative file skeleton; blank values deliberately indicate unanswered settings:

```dotenv
PWLH_delay=
PMLH_delay=
PQLH_delay=

# Add if Monday repeats are selected:
# MONLH_delay=

# Add if percentage retreat is selected:
# PMLH_rearm_pct=
```

This skeleton is not a runnable configuration. During implementation the real file must contain the values you choose. A missing file, blank required value, or invalid number should produce a clear configuration error before scanning begins.

We still need to decide whether fractional day values are allowed and whether zero is allowed. Zero has a special consequence for quarterly visibility: a zero-duration recent window would ordinarily show no persistent quarterly swept badges.

A later configuration change also needs a policy: recompute deadlines from the original accepted times using the new values, or retain deadlines already established. I recommend recomputing from saved occurrence times on restart, but that is not yet approved.

## 8. Missed touches, startup, and restart

### The process was off

Confirmed behavior: quietly discover touches during the offline interval. Update the level states, accepted-sweep anchors, and relevant counts using their historical times. Do not email or add them to the fresh-event display.

The same quiet preparation applies to a coin entering the monitored list later. A newly selected coin should not be assumed untouched just because this process has no saved candles for it.

### The process was open but disconnected

Recommended behavior: show one short connection/recovery status, then silently restore touches from the missing interval. Individual missed touches should not become urgent merely because the socket reconnects. This needs your confirmation because the process technically remained running.

### A live event arrives or is processed late

This differs from an offline scan. A delayed event may still arrive through the live feed or sit in a processing queue. Without an age rule, a several-minute-old touch could look urgent now.

Proposal: define a maximum fresh-event age. Events older than that still update state quietly, but do not create new urgent messages. The age is measured using UTC event time, not the time the terminal finally renders the row. You need to choose the limit.

### Touches while the loading screen is visible

Proposed boundary: active alerts begin when the loading view changes to `LIVE`. All touches up to that handoff are quiet preparation. Capture live data while checking history and reconcile it in chronological order so there is no uncovered gap between the scan and live monitoring.

Alternative: count touches received after the live subscription opens as fresh, even during loading, then report them after each coin's history check finishes. This provides earlier coverage but can produce notifications as startup finishes. You need to choose which moment means “actively running.”

## 9. Fresh email and existing pending email

### Fresh email

Current email is a five-minute digest. Immediate price detection will not make email immediate unless the dispatcher also changes.

Recommended option: start sending promptly after an eligible fresh live touch. Historical scans, expired quarterly history, and suppressed visits create no email entry. Actual inbox arrival remains dependent on email delivery.

If one movement touches both a monthly and quarterly high at the same price, their states remain separate. One email could list both levels. Combining nearby events for one coin reduces duplicate-looking messages but requires a small gathering window; keeping the current five-minute digest is another option. The scheduling and grouping choice remains open.

The new email wording would say “touched at” rather than “confirmed at candle close.” You can choose whether email times also use Sofia; the confirmed requirement currently covers the terminal.

### Pending mail already in the database

The current queue survives restart and can contain old events. Historical recovery and email-disabled runs can also add to it. Quiet startup scanning alone will not stop old queued messages from being sent.

Recommended migration policy: retain old records as history, but mute existing pre-change pending notifications. This avoids receiving a backlog when the new logic starts. It needs explicit confirmation in the final review.

For new fresh events that fail to send, proposed policy: retry only while the event is within a chosen useful age. After that, retain its history and stop treating the notification as urgent. The same age rule could apply to pending messages across restarts, or all pre-restart messages could be muted instead.

When email is disabled, I recommend storing the touch state and history without accumulating mail for later enablement. Otherwise enabling email could recreate the historical-alert flood you want to avoid.

These are notification policies; none should erase level state or start a delay from the date a delayed email finally sends. The existing delivery system can still duplicate an email if acceptance succeeds but acknowledgement is lost; changing the touch detector does not itself change that delivery property.

## 10. A cleaner terminal

The default screen should answer: which current levels are swept, what monthly counts exist, whether monitoring is healthy, and what was just touched.

Proposed layout, with example data:

```text
CRYPTO MONITOR | LIVE | 100 coins | Email on
13 Oct 16:32:04 Europe/Sofia

Fresh touches
16:31:58  FILUSDT  Previous-week high  1.231

Coin       Monday L/H   Week L/H   Month L/H   Quarter L/H
FILUSDT    S / -        - / S      S(3) / -(1) - / -
ADAUSDT    - / -        S / -      -(2) / -(0) - / S
LINKUSDT   - / S        - / -      -(1) / S(2) ? / ?

S = swept; - = untouched/eligible; ? = unverified
Month parentheses = meaningful visits this watch period
Swept view | Page 1/3 | 20 shown | 47 matching | 2 unverified
```

`S(3)` means the monthly side has three meaningful visits and its swept/delay indicator is active. `-(3)` means the delay has expired and it is treated as untouched/eligible, while the count of three is retained. This legend needs approval alongside the counting rule.

Recommended behavior:

- One row per coin, with separate low/high positions inside each level group.
- A small fresh-touch area; historical discoveries never enter it.
- Quarterly badges disappear after the chosen recent window. Two-month-old quarterly touches do not occupy the default view.
- Monthly counts stay visible for the current monthly watch, independent of the badge's expiry.
- Expired reference periods leave the current view. Old records remain available as history.
- Pages or scrolling make every matching coin accessible instead of silently cutting the list off at ten.
- A coin-detail view can show exact prices, touch time, next eligible time, reference period, and provider without putting all those fields in every row.
- One compact health line. Connection trouble, incomplete history, delayed data, or email problems add a brief banner when relevant. Routine queue counters and “no error” lines need not occupy the normal screen.
- All terminal timestamps, including plain terminal log messages if that mode is used, convert to Sofia. Local offsets can appear in details when needed to disambiguate daylight-saving clock changes.

Recommended default filter: swept coins, with an accessible all-coins view and an always-visible count of unverified watches. Showing all selected coins by default is also reasonable; this remains a review choice.

For “urgent,” I suggest a highlighted fresh row and a short fresh-event panel. A beep, highlight duration, and whether to print a persistent event line are optional choices for you. No sounds or durations have been selected.

### Loading screen

```text
CRYPTO MONITOR | LOADING
Preparing current level status

Reference prices: 100 / 100 coins complete
Touch history:    42 / 100 coins complete

Historical touches are being recorded quietly.
Live alerts begin when preparation is complete.
```

The view should appear before market discovery and replace itself with the normal dashboard after preparation finishes. Suggested stages are configuration validation, selecting coins, reference calculation, historical checks, live connection, and reconciling the scan-to-stream handoff.

Show actual completed counts when they are known, and a spinner otherwise. Avoid invented overall percentages or completion estimates. A count of completed coins should only increase after that coin's required checks have genuinely completed.

If some history remains unavailable, the program must either wait, stop with a useful explanation, or enter live mode for verified watches with an explicit incomplete-history status. It should not stay on an endless loading screen or claim that all watches are verified. The preferred partial-startup policy is still a question.

## 11. Incomplete history and other providers

There are two different missing-data cases:

1. **Missing reference history:** there is not enough history to calculate the previous month's or quarter's exact Binance high/low.
2. **Missing touch history:** the reference price is known, but some later activity is missing, so we cannot confirm whether it was touched or count its visits completely.

A missing quarter does not automatically make an independently verified weekly watch unusable. The terminal should show uncertainty on the affected watch.

### Recommended recovery order

1. Retry or complete pagination from Binance's own candle endpoints.
2. Look for Binance's official historical archives for older missing ranges, validate their checksums and requested coverage, and use current REST data for the latest ranges.
3. If exact Binance coverage remains incomplete, keep the affected result unknown.
4. Use MEXC or BingX only as explicitly labelled comparison estimates if you approve that mode.

Binance's official archives contain USD-M candle data from its futures candle endpoint. Daily files become available the next day, so they cannot replace the latest live data. Archives can also be corrected, which is why validation and provenance matter. [Binance public-data documentation](https://github.com/binance/binance-public-data#futures).

### Provider research

| Provider | Official capability reviewed | Practical implication |
|---|---|---|
| Binance USD-M | Candles with opening and closing timestamps; up to 1,500 per REST request. | Page through the required ranges and check coverage. This is the matching venue for the monitor's levels. |
| MEXC contracts | Ordinary contract candles support minute/daily intervals, seconds-based start/end times, and up to 2,000 candles per request. Index and fair-price candles have separate endpoints. | A separate adapter and correct instrument/price mapping are required. Coverage must be verified for each coin. |
| BingX swaps | Swap candles support minute/daily intervals, milliseconds-based start/end times, and up to 1,440 candles per request. Mark-price candles are separate. | A separate adapter, symbol mapping, and coverage checks are required. |

Sources: [Binance futures market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data), [MEXC official contract candle documentation](https://mexcdevelop.github.io/apidocs/contract_v1_en/#k-line-data), [BingX official swap candle reference](https://github.com/BingX-API/api-ai-skills/blob/main/skills/swap-market/api-reference.md#6-kline--candlestick-data).

Neither reviewed alternative-provider reference guarantees complete history for every coin.

**Inference from those being separate exchange markets:** a touch on MEXC does not establish a touch on Binance. For example, MEXC could trade at 100.05 while Binance reaches only 99.98. Substituting MEXC would incorrectly mark a Binance high of 100 swept. Likewise, another venue's failure to reach 100 does not prove Binance remained untouched.

The same problem applies when building a reference price: an older MEXC quarter high is not an exact Binance quarter high. A newly listed Binance contract cannot have Binance trading history before its listing, even if the asset traded elsewhere for years.

If you approve other-exchange estimates, keep their provider and estimated status explicit. Do not silently convert them into authoritative Binance swept/untouched states. We also need to decide whether such estimates can trigger alerts or are display-only.

Validation needs to distinguish the same asset and contract from similarly named instruments, ordinary traded prices from mark/index/fair prices, spot from perpetual futures, seconds from milliseconds, chronological ordering, missing/duplicate candles, and complete UTC coverage.

## 12. Implementation outline after review

The work would involve the live feed and engine, persistent touch/delay/count state, silent history reconstruction, email eligibility, required environment loading, and the loading/dashboard views. The old closed-candle alert records can be preserved as old-rule history; they alone cannot reconstruct every touch or monthly count.

Immediate live state must be saved independently from a candle that has not closed yet. A later candle completion should not create a duplicate touch. Historical and live data must meet at a clear boundary without missing or double-counting the same movement.

Weekly/monthly/quarterly rollover and restart must preserve each watch's correct identity. High and low remain independent even if several level names share the same numeric price.

Once the decisions below are settled, the implementation checks should cover equality, no waiting for a close, independent sides, fixed deadlines, retreat/return counts, quiet history, restart, period reset, quarterly expiry, missing configuration, Sofia display, and incomplete-history states. This document is the review artifact; it does not implement those changes.

## 13. Questions to finish the specification

You can answer by number. The decisions already confirmed above do not need to be repeated.

1. **Monday repeats:** should Monday low/high stay first-only until next Monday, or repeat after `MONLH_delay` like the previous-week watches?
2. **Required values:** what day values should the actual environment file contain for `PWLH_delay`, `PMLH_delay`, `PQLH_delay`, and `MONLH_delay` if applicable? Should fractional days be allowed? Should zero be rejected to avoid an empty quarterly display window?
3. **Repeat distance:** do you approve a percentage retreat before another meaningful monthly visit? What percentage should be explicitly configured? Should this minimum retreat also apply to quarterly, weekly, or Monday repeats?
4. **Monthly count:** count all meaningful retreat/return visits, including those during cooldown with no email, or count only delay-eligible accepted sweeps? The former best matches your request for a count regardless of delay.
5. **Historical count accuracy:** is a conservative minute-based count acceptable when the within-minute sequence is unknown, clearly identified as such, or do you require finer trade-history reconstruction before showing a count?
6. **Quarterly display window:** base the badge on the most recent accepted sweep, or on every meaningful recent touch including those suppressed during cooldown? Should the lookback be exact elapsed hours, as recommended, or midnight-based calendar dates?
7. **Price parked beyond a level:** when a delay expires, should we wait for a fresh retreat/return rather than alert merely because price remains beyond the level?
8. **Start of active alerts:** begin when loading finishes and the screen becomes `LIVE`, or notify qualifying touches received during loading once their checks finish?
9. **Reconnect and freshness:** should touches recovered after a connection failure be quiet like offline touches? What maximum event age should still qualify as a fresh urgent notification?
10. **Fresh email:** initiate sending promptly, keep five-minute digests, or use a small grouping window? May simultaneous levels for one coin share one email? Should email timestamps also use Sofia?
11. **Old pending email:** approve muting existing pre-change pending mail while retaining its records? For future failed sends/restarts, use the same maximum fresh age or mute all pre-restart mail? When email is disabled, approve keeping state without accumulating later email?
12. **Terminal view:** approve a paginated swept-coins view with an all-coins option and coin details, or prefer all coins by default? Does the grouped low/high layout and monthly-count notation suit you?
13. **Urgency:** is a highlighted fresh-event panel enough, or do you want a persistent event line or sound too? How long should fresh emphasis remain?
14. **Incomplete history:** wait for every required watch, or enter live mode for verified watches while clearly marking and disabling uncertain watches? Should MEXC/BingX estimates be excluded entirely, display-only, or permitted to influence alerts with an explicit provider label?
15. **Changing configuration:** on a later restart with different delay/retreat settings, recompute current state from saved times and history, or preserve deadlines/counts established under the old settings? Recomputing is the recommended consistent interpretation, but counts may change if the retreat threshold changes.
