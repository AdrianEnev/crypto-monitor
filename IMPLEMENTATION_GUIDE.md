# How the updated monitor works

This guide describes the implemented service launched with `crypto-monitor`
(also available through `python -m crypto_monitor`).

## 1. Before your first launch

Open the project's `.env` file and fill in these three values yourself:

```dotenv
PWLH_delay=
PMLH_delay=
PQLH_delay=
```

They mean previous-week, previous-month, and previous-quarter delays, in days. The empty fields are intentional because you chose to set your own values. Startup stops with a clear message until all are positive numbers. Zero and negative values are rejected; fractions are accepted. For example, `0.1` days is 2 hours 24 minutes. There are no numeric delay fallbacks in the code.

The file also contains editable notification timings in seconds:

- `NOTIFICATION_MAX_AGE_SECONDS=60`: an older incoming event is quiet history, and an unsent email older than this is muted.
- `EMAIL_RETRY_SECONDS=30`: how long to wait before trying a failed fresh email again, while it is still useful.
- `FRESH_HIGHLIGHT_SECONDS=60`: how long a new live activation gets fresh emphasis in the terminal.

Monday watches have no delay variable. Each Monday low and high can activate only once for its reference.

Run from any directory using the installed command:

```sh
crypto-monitor
```

The defaults are top 100 markets, table layout, email on, and the existing
project database and `.env`. Use `--no-email` to start with mail off, `-n 50`
for a different market count, or `-l cards` for the other layout. The long
`--top-n` and `--layout` options and all advanced options remain available.
Default database and environment paths stay anchored to the project directory;
explicit relative paths resolve from the invocation directory.

Use `--env-file /path/to/settings.env` for a different required file, or
`--no-tui` for plain logs. Selection still uses eligible Binance USDT perpetual
markets, with optional `--include` and `--exclude` rules.

## 2. Loading and quiet history

The loading screen appears while the monitor selects coins, calculates reference prices, and checks their current watch histories. Completed coins are counted honestly; no invented estimated completion time is shown.

Loading has separate steps: **reference levels**, **past touches**, **loading catch-up**, and **current minutes**. The catch-up step checks the time spent doing the earlier work; it resumes saved history rather than starting the whole research over. Each step has its own counter, so a new step beginning at zero does not erase the completed steps.

For example, if you start on Thursday and a coin touched its previous-week low on Wednesday, that low appears swept earlier if its delay remains active. The historical discovery sends no email and does not appear in the fresh-event panel.

Initial checks cover the current week for previous-week levels, Tuesday onward for the completed Monday range, the current month for previous-month levels, and the current quarter for previous-quarter levels. The program searches activity after each reference became a watched level, rather than the candles that originally created it.

The scanner uses daily price ranges to skip days that could not produce an eligible activation. Where necessary, it checks minute history. Saved progress reduces repeated work after restart. The first scan of many coins and a long quarterly period can still take time.

Historical minute data is useful, not automatically unavailable. A candle can establish that price touched during a minute without revealing the exact second. The saved historical activation time uses that minute's ending boundary and is labelled approximate, so it can be up to one minute later than the actual touch. This can affect activation counts or eligibility near a precise delay boundary; the monitor does not claim exact historical seconds.

Genuinely missing or malformed required coverage is **unavailable**. That watch is ignored for live alerts until verified; other usable watches continue. No MEXC or BingX prices are substituted for Binance prices. Unavailable watches are retried during refreshes.

Before switching to LIVE, the monitor also checks the forming minute and joins the live feed quietly. A price already beyond a level during startup is recorded without emailing. Events from before the live handoff remain quiet even if they arrive afterward.

The live connection keeps reading prices while those checks run. Prices received during loading are buffered and processed quietly before the LIVE screen appears. This prevents a long history check from leaving the incoming price feed unread. Ordinary prices that cannot activate a verified level are checked without unnecessary database writes.

## 3. What activates a level now?

The live service receives Binance aggregate trade prices. It evaluates those updates as they are processed; it does not wait for a candle to close.

- A high at 100 activates when the reported price reaches or exceeds 100.
- A low at 90 activates when the reported price reaches or falls below 90.
- No one-tick excess, return inside, minimum retreat, or candle closing condition is required.

These prices arrive through Binance's live aggregate-trade stream; “immediate” means reacting to the received price update, subject to network and processing time. [Binance's official trade-stream documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market#aggregate-trade-streams).

Every coin and every named low/high has independent state. Touching a monthly low does not disable its high, another coin, or a different reference name. If monthly and quarterly highs share the same price, each remains a separate watch and can produce a separate email.

## 4. Repeats and counts

Suppose the weekly delay is five days and a high activates Monday at 10:00:

1. Its next eligible time is Saturday at 10:00, calculated as 120 elapsed hours in UTC.
2. A touch Thursday is ignored and does not extend Saturday's deadline.
3. Once the delay ends, the next fresh live update can activate it again if price is still at or beyond the level. Price does not have to leave and cross again.
4. If price is inside the level when the delay ends, it waits until price reaches the level.

A level continuously beyond its price can therefore activate once per configured delay. That is an accepted activation count, rather than a count of separate physical crossings.

Monthly numbers count **accepted activations** during the current monthly watch period, including accepted ones discovered quietly. Ignored crossings during a delay do not inflate the count. A monthly count stays visible after the swept badge clears, and resets for the new monthly reference.

Quarterly badges use the most recent accepted activation's fixed delay. An ignored touch does not extend the badge. Once that delay expires, the old quarterly touch disappears from the current display. A touch two months ago does not clutter the main screen merely because the quarter is still active.

## 5. Period changes and Monday

All period calculations use UTC:

| Watch | New reference becomes available | Old reference expires |
|---|---|---|
| Previous week | Monday 00:00 UTC | Following Monday 00:00 UTC |
| Monday | Tuesday 00:00 UTC, after Monday is complete | Following Monday 00:00 UTC |
| Previous month | First day of each month, 00:00 UTC | Next month's first day |
| Previous quarter | January, April, July, and October 1, 00:00 UTC | Next quarter's first day |

During Monday's 24 hours, Monday high and low are undefined and shown unavailable. Tuesday introduces the new completed Monday range. Its low and high are each first-only. A reference change clears that watch's prior suppression, even when its numeric price happens to match the previous reference.

The confirmed Monday daily interval is Monday 00:00 UTC through Monday 23:59:59.999 UTC, complete Tuesday 00:00 UTC. Verification against an official Binance daily archive is recorded in the [proposal](SWEEP_REFACTOR_PROPOSAL.md).

## 6. Stop, restart, and connection trouble

SQLite remembers the current references, accepted times, counts, and historical progress. Restarting restores that information and quietly checks the time you missed.

Recovery after a socket disconnection is also quiet. Old incoming events are not made urgent simply because they arrive late. No notification is created from a cached stale price.

If you change a delay in `.env`, eligibility is recalculated from the saved accepted time using the new delay. Recorded counts and history stay intact. Startup history and settings changes do not themselves send email.

Ctrl+C, SIGTERM, or the terminal's `q` key requests shutdown. In-flight network work may need to return or time out before cleanup finishes. An email already being sent is allowed to finish and save its result.

The terminal keyboard reader leaves screen output in its normal blocking mode. Stopping during preparation is handled as an ordinary shutdown, including on Python 3.14; the expected preparation interruption is not displayed as a failure.

## 7. Email

Press `e` in the running dashboard to toggle email alerts. The top of both
layouts shows **Email ON (e)** or **Email OFF (e)**. The toggle affects only the
current run; email starts on unless `--no-email` is supplied. Disabling
email stops new notifications, lets any send already underway finish, and
discards pending retries. Enabling it again sends only new eligible live touches.
Invalid settings when pressing `e` keep email off and show an attention message.
Invalid enabled-email settings at startup stop the run; `--no-email` allows
starting without email.

Email starts enabled; use `--no-email` to disable it at startup. The existing `--email-enabled` option explicitly selects the default. Startup loads shared credentials from `Trading_NEW/.env` alongside the required project `.env`; process variables take precedence, followed by project values, then shared defaults. Required monitor timings still come directly from the project file. Shared `SES_SMTP_*` credentials select encrypted SES SMTP delivery, with `EMAIL_FROM` and `ALERT_EMAIL_TO` supplying the addresses. Sender and recipient arguments override those defaults. SMTP delivery uses the region in `SES_SMTP_HOST`. Without that host, the SES API continues to use Boto3's standard credential chain, with `--email-region` overriding `AWS_REGION` or `AWS_DEFAULT_REGION`. See the README for variable names and recipient fallbacks.

Each eligible fresh live activation gets a separate message, sent promptly. There is no five-minute combined digest in the executable touch service. Messages show the level, touched time in Sofia and UTC, and a stable activation ID.

Historical discoveries never enter this email queue. With email disabled, events remain available to the terminal but do not accumulate mail to send later. Prior-run pending notifications are muted when starting a new run; legacy closed-candle pending records are preserved but not consumed by the new sender.

Provider failures retry using the environment timings while the message remains fresh. Expired pending messages are muted. A local database failure stops the service visibly rather than silently losing bookkeeping. SES acceptance is not proof of inbox delivery, and a lost acceptance response can still lead to duplicate delivery on retry.

## 8. Terminal designs you can switch

Table view provides a broad overview. Card view spells out each coin's low/high statuses, prices, and relevant times. All selected coins are accessible through pages; there is no ten-alert cutoff.

| Key | Action |
|---|---|
| `t` | Table layout |
| `c` | Card layout |
| `n` / `p` | Next / previous page |
| `a` / `s` | All coins / currently swept coins |
| `q` | Orderly shutdown |

In the table, `S` means an active live-swept badge, `E` means swept earlier, `-` means eligible again, and `N/A` means unavailable. Monthly parentheses contain the accepted count: `-(2)` means eligible again with two accepted activations this month.

Fresh live activations get a small highlighted panel for the configured time. Historical discoveries do not. No sound was added. Routine logs are suppressed while the interactive display is active; important errors remain visible. Plain logs remain available with `--no-tui`.

Terminal times use `Europe/Sofia`, including daylight-saving changes. Internal UTC calculations are unaffected by the local display.

## 9. Where the current implementation lives

| File | Plain-language responsibility |
|---|---|
| `runtime/service.py` | Reads command-line options and starts the executable service. |
| `runtime/touch_settings.py` | Reads and validates the required environment file. |
| `runtime/touch_service.py` | Connects startup, history, live processing, refresh, display, and shutdown. |
| `adapters/binance_trades.py` | Receives live trade prices and reconnects when needed. |
| `core/touch_monitor.py` | Defines equality touches, watch periods, and delay arithmetic. |
| `persistence/touch_store.py` | Saves independent level states, accepted counts, and fresh email work. |
| `runtime/touch_history.py` | Quietly checks past prices and records unavailable coverage. |
| `ui/touch_dashboard.py` | Loading screen, table/cards, paging, keys, and Sofia display. |
| `adapters/touch_email.py` | Separate fresh emails, retry expiry, and safe send cleanup. |

Existing market-selection and daily reference-calculation modules are reused. The older closed-minute engine, recovery harness, dashboard, and sender remain as compatibility code with their previous tests; they are not the executable live monitor's detection/email path. Old database records are preserved, and the new touch tables are added when the new service starts.

The `research/` studies remain separate historical research. They are not imported to generate live touch alerts.

## 10. What was checked

Before the October 13 terminal/shutdown fixes, all 378 automated tests passed, including 84 new checks for the touch service. These cover equality, independent sides, fractional delays, restart, quiet history, unavailable data, period resets, separate email, safe shutdown, and selectable terminal layouts. That result describes the initial implementation; the later fixes have not had another test run.

The checks used temporary databases, simulated Binance responses, and mocked email clients. No real email was sent or live monitor session started during verification. Your existing database was not opened for mutation during development. It receives the additive touch tables when you start the configured service.
