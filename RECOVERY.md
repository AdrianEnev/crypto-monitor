# Closed-minute recovery

> This document describes the preserved closed-candle compatibility modules.
> The executable monitor now uses live trade touches and quiet historical discovery.
> For current startup, recovery, alerts, and settings, read [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md).

`GapRecoveryProcessor(markets, store, rest, references).process_live(event)`
uses the existing `DiscoveredMarket`, `SQLiteStore`, `BinanceRESTAdapter` and
`ClosedKline` models. It returns newly committed sweep hits. All hits, including
those committed before a later failure, remain readable through `store.load_hits()`.

The store accepts only the first alert per symbol, named level, side, and source
reference period. The processor returns only those accepted alerts; later sweep
candles still commit and advance the cursor. `alert_claims` is saved atomically
with the candle, hit, and outbox entry, so recovery and restart cannot rearm the
same watch. A change to that watch's source period permits a new alert. Migration
keeps old hit/outbox history but hides suppressed repeats from the dashboard and
email dispatcher; see the README's alert-frequency policy.

The reference callback is synchronous: `references(symbol, candle_open_time)`
returns a mapping of the existing watch names to `ReferenceLevels`. Supply the
periods and calculated levels valid at that **historical opening time**. The service
uses `DurableReferences` from `crypto_monitor/persistence/reference_snapshots.py` for this lookup. A stale
period raises an error. An omitted watch or `levels=None` remains unavailable,
as in the existing engine. The first candle for a new symbol needs no reference
lookup and only establishes a baseline.

For a live feed, let `WebSocketSupervisor` own the feed and bounded worker. For example,
inside an async function with selected `markets`, `rest`, `references`, and an
`asyncio.Event` named `stop`:

```python
from crypto_monitor.runtime.gap_recovery import RecoveryWorker
from crypto_monitor.runtime.websocket_supervisor import WebSocketSupervisor

worker = RecoveryWorker(markets, "monitor.sqlite3", rest, references, capacity=256)
supervisor = WebSocketSupervisor(worker)
await supervisor.run(stop)
```

Do not also start `worker.run()` yourself. Use a fresh, initially unset `stop`
event. Setting it, or cancelling `supervisor.run()`, closes the feed and interrupts
the worker between candle commits. An in-flight synchronous REST request must
return or time out first. Pending/queued events remain on the worker object for
an explicit restart; only committed state survives a process exit. The standalone
worker's own stop event still drains accepted events, but the supervisor uses
cancellation for faster shutdown. REST and SQLite run on one background thread; the
receiver callback never waits for queue space. Capacity counts queued events,
plus at most one event in progress. Overflow raises a monitoring failure rather
than silently dropping an event.

Transient socket failures and remote disconnects reconnect automatically. Recovery
errors (including incomplete REST coverage, SQLite failures, and queue overflow) stop the
supervisor as unhealthy and require explicit retry after resolving their cause.
Re-running the same supervisor retains the worker's pending/queued events.
A new process has only committed state and needs a new closed live candle to
trigger recovery. Each retry reconstructs the engine from SQLite and starts
after its committed cursor. Only one processing worker may write this database.

## Refresh coordination in the service

`crypto_monitor/runtime/service.py` initializes `RefreshCoordinator` alongside the supervisor. Its clock,
sleep function, and synchronous ticker/exchange/level fetch functions are
injectable. The scheduler uses aware UTC time, runs quote-volume refreshes at
`:00/:05/...`, and runs membership plus reference-level refreshes at midnight.
A late wakeup refreshes the current snapshot once rather than replaying every
missed timer tick. HTTP staging runs off the event loop.

Daily membership selection uses the existing market-universe rules and the latest
validated ticker response. Five-minute ticks never fetch exchange information.
Membership and all its reference levels form one bundle: a failed/partial bundle
keeps the active set and previous references. An independently successful volume
refresh can still update all current markets. Even one missing active volume
rejects the entire volume update; failures retain the good values and their age.
Unavailable required levels reject a refreshed bundle, including insufficient
daily coverage for recently listed contracts. Startup retains its existing
behavior of fetching current levels and logging unavailable levels; initialization
persists that seed before starting processing. Missing required levels must be
resolved before the service commits even a new symbol's baseline.

`worker.apply_configuration(markets, references)` publishes the complete snapshot
under a lock shared with processing. One operation includes an incoming candle
and all of its preceding backfill; none of it sees a partially refreshed snapshot.
Fetched data is validated before that lock is acquired. `supervisor.update_symbols`
then rotates only if the normalized symbol set changed, closing the old session
before connecting its replacement. The worker and database cursors remain intact.
Queued events for removed symbols are explicitly logged and skipped without
changing those cursors. Re-included symbols recover from their saved cursors.

`coordinator.status` exposes last successful volume/membership/level update times,
`volume_age_seconds`, separate errors, `unresolved`, and `degraded`. Membership
failures keep current subscriptions and retry the next midnight. Volume failures
retry on the next five-minute tick. Failed levels retry on those ticks for the
retained active set; `refresh(force_daily=True)` permits an explicit full retry.

### Durable reference snapshots

SQLite's additive `reference_snapshots` table stores snapshots by symbol and
start-inclusive/end-exclusive UTC validity range. Each snapshot records all four
watches, their source-period boundaries, and Decimal high/low values as strings.
Inactive Monday is explicit; an active watch with incomplete daily coverage is
recorded as unavailable. The existing UTC rules and sweep predicate are unchanged.

`reference_regime(timestamp)` identifies the maximal range with identical source
periods: boundaries occur on Monday (Monday expires and the previous week changes),
Tuesday (Monday activates), and the first of each month (also quarter boundaries
when applicable). Ordinary midnight boundaries do not create extra regimes.
Daily refresh validates and stores its entire selected-market bundle in one
transaction before making it available to the worker. Immutable mappings and a
provider lock prevent recovery from observing partially published bundles;
SQLite connections belong to their calling threads. HTTP holds no SQLite transaction.

For **each recovered candle's opening timestamp**, the provider checks its cache
and then SQLite. If required levels are absent, it calls the existing daily-kline
adapter with that historical timestamp as `as_of`, calculates levels from completed
daily candles, and persists the result. Complete snapshots are cached by symbol
and regime, so minutes sharing a regime reuse one lookup/fetch. Partial snapshots
retain available levels but cannot authorize candle processing. No snapshot is
automatically pruned; all remain usable across restarts. Startup still fetches
current levels before initializing this provider, so it requires REST availability.

Accepted non-null levels within a snapshot are immutable: repeated identical
writes are idempotent, unavailable levels can be filled, and partial writes cannot
downgrade known values. Conflicting values reject the whole transaction. This
keeps concurrent refresh/recovery consistent; exchange corrections to already
stored daily values require explicit reconciliation, which is not automated.

Recovery resolves references chronologically, committing each valid candle and
its hits before proceeding. Missing/duplicate daily coverage, malformed history,
or failure fetching/storing a needed snapshot raises `ReferenceUnavailable`.
The worker retains its pending event and committed prefix until configuration is
republished (normally the next five-minute refresh), then retries from SQLite.
No candle requiring unavailable levels is committed and today's levels are never
substituted for an older regime. Already committed duplicates need no snapshot.
All symbols share this ordered worker, so an unresolved reference blocks the
queue; prolonged failure may cause the existing explicit overflow failure.
Shutdown preserves snapshots and the committed prefix, not pending queue entries.

Binance documents weight **1** for
[exchange information](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#exchange-information)
and **40** for the all-symbol
[24-hour ticker](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#24hr-ticker-price-change-statistics)
(single-symbol weight 1). Daily levels use the existing exact-range daily-kline
requests and kline tiers below. This scheduler does not implement shared-IP
request budgeting or automatic HTTP retries within a refresh.

## WebSocket lifecycle

Each connection uses the existing routed `/market/stream?streams=...` URL, selected
symbols, and unchanged heartbeat settings. A successful reconnect resubscribes
to the same streams. Its closed candles enter the same `worker.submit` queue;
the recovery processor fills missing minutes using SQLite's committed cursor.
Duplicate REST/live timestamps are handled by that processor and the store's
unique keys, not by another in-memory deduplication cache.

Defaults are `base_delay=1`, `max_delay=60`, `stable_after=60`, and
`rotation_interval=23*60*60` (seconds). Retry ceilings double up to `max_delay`;
jitter selects between half the ceiling and the ceiling. A handshake alone does
not reset backoff: the connection must remain open for `stable_after` seconds,
or complete a planned rotation. Rotation starts after the successful handshake
and closes the old session before opening a new one. The interval must be positive
and below 24 hours. Configure a comfortable margin below the limit.

Binance documents a 24-hour lifetime, server pings every three minutes and a
ten-minute pong deadline in its [official connection guidance](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect).
The existing WebSocket library answers server pings automatically; the feed's
20-second client ping/pong timeout remains unchanged.

Network/timeouts, normal remote closes, transient server close codes, and HTTP
429/500/502/503/504 handshakes retry. Malformed messages, callback failures, policy
or protocol failures, certificate errors, and other handshake/configuration errors
surface without retrying forever. Worker failures stop the feed even if no further
candle arrives. Socket reconnection does not automatically retry failed REST
backfill or guarantee that history is available.

`supervisor.status` is an immutable snapshot exposing `state` (`stopped`,
`connecting`, `connected`, `backoff`, `rotating`, or `unhealthy`), `last_disconnect`,
`last_error`, `reconnect_attempts`, `consecutive_failures`, and `retry_delay`.
Reconnect attempts count actual attempts after the first, including planned
rotations; they reset on an explicit `run()`. The last error remains visible after
successful recovery. `connected` describes the socket, not completion of backfill.
Tests can inject `connector`, `sleep`, `random_value`, and `monotonic` without live
network connections or real retry delays.

Backfill requests use nonoverlapping windows of at most 499 minutes and
`endTime = exclusive_end - 1 millisecond`. Each page is fully validated before
any of its candles are processed. Malformed, duplicate, out-of-order or
out-of-window rows fail. Missing or unclosed rows cause `UnresolvedGapError`,
which exposes the symbol and unresolved interval. Previously completed pages
remain committed. The incoming live candle never crosses an unresolved gap.

Limits and caveats:

- Binance currently allows 1,500 klines per request; weights are 1 for limits
  1–99, 2 for 100–499, 5 for 500–1,000, and 10 above 1,000. See the
  [official USDⓈ-M kline documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#klinecandlestick-data).
  HTTP/rate-limit failures surface; this module does not coordinate other
  clients' shared IP budget or automatically back off.
- REST has no historical WebSocket event time. Recovered alerts use the
  triggering live event's timestamp and the selected metadata's captured quote
  volume. Neither represents historical volume or the original event timestamp.
- Both minute history and complete daily source periods must remain available
  from Binance. A recently listed market without a full previous quarter can
  remain unresolved. Recovery begins only when another live candle arrives;
  a stalled feed alone cannot trigger it.
- The latest stored candle is trusted as the cursor. This does not audit or
  repair gaps created earlier by other software, and first launch deliberately
  does not replay alerts before the first saved baseline.
- Queued events are not durable. After a process crash, a later live candle can
  trigger recovery from committed state. Detected hits enter the SQLite email
  outbox atomically; optional SES delivery is described in [README.md](README.md).
