"""Ordered recovery from committed cursors; no feed reconnection or retry timer.

GapRecoveryProcessor is synchronous and owns its engine, not its store. Retry
process_live() with the same live event after a failure. Successfully committed
prefixes stay saved; every attempt rebuilds engine state from SQLite.

RecoveryWorker.submit is an async BinanceKlineFeed callback. Run the worker
alongside the feed and await/supervise BOTH tasks: a worker failure must stop
monitoring even if no further live events arrive. One worker is the sole writer
for a database. Read hits from SQLite; the queue isn't an alert delivery system.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from os import PathLike
from threading import Event
from typing import Callable, Iterable, Mapping

from crypto_monitor.adapters.binance_feed import ClosedKline
from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter, DiscoveredMarket, UnresolvedGapError
from crypto_monitor.core.level_sweep import MINUTE, reference_periods
from crypto_monitor.core.market_universe import rank_alerts
from crypto_monitor.core.monitoring_engine import CandleBaseline, MonitoringEngine, ReferenceLevels, SweepAlert
from crypto_monitor.persistence.sqlite_store import SQLiteStore


ReferenceProvider = Callable[[str, datetime], Mapping[str, ReferenceLevels]]


class MonitoringFailure(RuntimeError):
    """Monitoring must stop; retry explicitly after resolving the cause."""


class ReferenceUnavailable(MonitoringFailure):
    """Leave the pending candle uncommitted until its correct references exist."""


class GapRecoveryProcessor:
    """References must describe the supplied historical candle opening time.

    A provider may intentionally return unavailable levels (None) or omit
    watches, as with the engine. Supplied periods must match that candle;
    stale snapshots raise an error instead of silently suppressing sweeps.
    Recovered hits capture selected market volume and the triggering live
    event's event_time; REST cannot reconstruct historical event timestamps.
    """

    def __init__(self, markets: Iterable[DiscoveredMarket], store: SQLiteStore,
                 rest: BinanceRESTAdapter, references: ReferenceProvider,
                 *, cancelled: Callable[[], bool] = lambda: False):
        self.markets = tuple(markets)
        self.store, self.rest, self.references = store, rest, references
        self._cancelled = cancelled
        self._restore()

    def _restore(self) -> None:
        # restore_baseline deliberately cannot rewind an existing engine.
        self._engine = MonitoringEngine(self.markets)
        self._baselines = self.store.load_baselines()
        for market in self.markets:
            if market.symbol in self._baselines:
                self._engine.restore_baseline(market.symbol, self._baselines[market.symbol])

    def _check_cancelled(self) -> None:
        if self._cancelled():
            raise MonitoringFailure("recovery interrupted; resume from the committed cursor")

    def _commit(self, event: ClosedKline, *, first: bool = False) -> list[SweepAlert]:
        self._check_cancelled()
        refs = {} if first else self.references(event.symbol, event.candle.open_time)
        self._check_cancelled()  # Reference lookup can now perform historical REST I/O.
        periods = reference_periods(event.candle.open_time)
        for name, reference in refs.items():
            if name not in periods or reference.period != periods[name]:
                raise MonitoringFailure(f"incorrect reference period for {event.symbol}: {name}")
        hits = self._engine.process(event, refs)
        if not self.store.commit_candle(event, hits):
            raise MonitoringFailure("another writer committed this candle; reload and retry")
        return rank_alerts(self.store.load_candle_alerts(event.symbol, event.candle.open_time))

    def process_live(self, event: ClosedKline) -> list[SweepAlert]:
        """Commit backfill chronologically, then live; return newly committed hits.

        An exception means the live candle was not fully handled. Already
        committed hits remain in SQLite even when no result can be returned.
        Duplicate/older live events are ignored without moving any baseline.
        """
        self._restore()
        if event.candle.closed is False:
            return []
        if event.symbol not in {market.symbol for market in self.markets}:
            raise ValueError(f"unselected symbol: {event.symbol}")
        if event.candle.closed is not True or event.candle.interval != MINUTE:
            raise ValueError("expected a closed one-minute candle")
        CandleBaseline(event.candle.open_time, event.candle.close)
        if event.event_time.utcoffset() != timedelta(0):
            raise ValueError("event time must be timezone-aware UTC")
        if (event.close_time.utcoffset() != timedelta(0)
                or event.close_time != event.candle.open_time + MINUTE - timedelta(milliseconds=1)):
            raise ValueError("invalid live candle close time")
        previous = self._baselines.get(event.symbol)
        if previous is not None and event.candle.open_time <= previous.open_time:
            return []
        try:
            if previous is None:
                # No first-launch historical replay, even if this candle sweeps.
                return self._commit(event, first=True)
            expected = previous.open_time + MINUTE
            hits = []
            if expected < event.candle.open_time:
                logging.getLogger(__name__).info("recovery started symbol=%s start=%s end=%s",
                    event.symbol, expected.isoformat(), event.candle.open_time.isoformat())
                self._check_cancelled()
                recovered = self.rest.iter_minute_klines(
                    event.symbol, start=expected, end=event.candle.open_time,
                    as_of=event.candle.open_time,
                )
                for item in recovered:
                    self._check_cancelled()
                    if (item.symbol != event.symbol or item.candle.open_time != expected
                            or expected >= event.candle.open_time or not item.candle.closed
                            or item.candle.interval != MINUTE):
                        raise MonitoringFailure("invalid backfill sequence; refusing to cross a gap")
                    hits.extend(self._commit(replace(item, event_time=event.event_time)))
                    expected += MINUTE
                if expected != event.candle.open_time:
                    raise UnresolvedGapError(event.symbol, expected, event.candle.open_time)
                logging.getLogger(__name__).info("recovery committed symbol=%s through=%s",
                    event.symbol, (expected - MINUTE).isoformat())
            hits.extend(self._commit(event))
            return hits
        except Exception as exc:
            self._restore()
            logging.getLogger(__name__).info("processing failed symbol=%s committed_baseline=%s error=%s",
                event.symbol, self._baselines.get(event.symbol), exc)
            raise


@dataclass(frozen=True)
class RecoveryStatus:
    queue_depth: int
    queue_capacity: int
    processing: bool
    running: bool
    latest_closed_at: datetime | None
    unresolved: str | None
    error: str | None


class RecoveryWorker:
    """Bounded live queue and one processing thread.

    Start run(stop), await ready.wait(), then use submit as the feed callback.
    stop drains accepted events; cancellation stops between candle commits.
    Failed/pending and queued events remain for a subsequent run() on this
    object. Queue overflow rejects the new event and latches a failure. The
    caller must resubmit rejected events or use a later live candle to recover.
    SQLite connections are created/closed inside the worker thread per event.
    Missing reference snapshots pause the pending event until configuration is
    republished; other processing failures still require explicit retry.
    """

    def __init__(self, markets: Iterable[DiscoveredMarket], database: str | PathLike[str],
                 rest: BinanceRESTAdapter, references: ReferenceProvider, *, capacity: int = 256):
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("queue capacity must be a positive integer")
        self.markets, self.database = tuple(markets), database
        self.rest, self.references = rest, references
        self._queue: asyncio.Queue[ClosedKline] = asyncio.Queue(maxsize=capacity)
        self._wake = asyncio.Event()
        self.ready = asyncio.Event()
        self._halt = Event()
        self._pending: ClosedKline | None = None
        self.failure: Exception | None = None
        self._running = self._accepting = False
        self._configuration_lock = asyncio.Lock()
        self._configured = False
        self.unresolved: str | None = None
        self._latest_closed_at: datetime | None = None

    @property
    def status(self) -> RecoveryStatus:
        """Read on the event loop; latest_closed_at is accepted live candle end."""
        error = str(self.failure) if self.failure is not None else None
        unresolved = self.unresolved
        if isinstance(self.failure, UnresolvedGapError):
            unresolved = error
        return RecoveryStatus(self._queue.qsize(), self._queue.maxsize, self._pending is not None,
                              self._running, self._latest_closed_at, unresolved, error)

    async def apply_configuration(self, markets, references) -> None:
        """Publish a complete snapshot between operations, including their backfill.

        The lock is held until the processing thread finishes; it never covers
        refresh HTTP calls. The coordinator supplies immutable reference data.
        """
        markets = tuple(markets)
        if not markets or not callable(references):
            raise ValueError("configuration requires markets and a reference provider")
        MonitoringEngine(markets)  # Validate before replacing good configuration.
        async with self._configuration_lock:
            self.markets, self.references = markets, references
            self._configured = True
            self.unresolved = None
            self._wake.set()

    async def submit(self, event: ClosedKline) -> None:
        """Never wait for REST/SQLite or queue space; failure is explicit."""
        if self.failure is not None:
            raise MonitoringFailure("worker failed; monitoring must stop") from self.failure
        if not self._accepting:
            raise MonitoringFailure("worker is not accepting candles")
        if event.candle.closed is False:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull as exc:
            self.failure = MonitoringFailure("live candle queue overflow; monitoring must stop")
            self._halt.set()
            self._wake.set()
            raise self.failure from exc
        closed_at = event.candle.open_time + MINUTE
        if self._latest_closed_at is None or closed_at > self._latest_closed_at:
            self._latest_closed_at = closed_at
        self._wake.set()

    def _process(self, event: ClosedKline) -> None:
        if self._configured:
            if event.symbol not in {market.symbol for market in self.markets}:
                logging.getLogger(__name__).info("inactive symbol event skipped symbol=%s time=%s",
                                                event.symbol, event.candle.open_time)
                return  # Preserve its stored cursor for possible future re-inclusion.
        with SQLiteStore(self.database) as store:
            if self._configured and event.candle.closed:
                baseline = store.load_baselines().get(event.symbol)
                if baseline is None:
                    # Check a first baseline too. For recovery, look up each
                    # missing candle in order inside _commit, not the live
                    # candle in advance of a recoverable historical prefix.
                    self.references(event.symbol, event.candle.open_time)
            GapRecoveryProcessor(self.markets, store, self.rest, self.references,
                                 cancelled=self._halt.is_set).process_live(event)

    async def run(self, stop: asyncio.Event) -> None:
        if self._running:
            raise RuntimeError("recovery worker is already running")
        if stop.is_set():
            return
        self._running = self._accepting = True
        self.failure = None
        self._halt.clear()
        self.ready.set()

        async def stopping():
            await stop.wait()
            self._accepting = False
            self._wake.set()

        stopping_task = asyncio.create_task(stopping())
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="candle-recovery")
        try:
            while True:
                if self.failure is not None:
                    raise self.failure
                if self.unresolved is not None:
                    if stop.is_set():
                        return  # Pending event remains available for explicit retry.
                    await self._wake.wait()
                    self._wake.clear()
                    continue
                if self._pending is None:
                    try:
                        self._pending = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        if stop.is_set():
                            return
                        await self._wake.wait()
                        self._wake.clear()
                        continue
                async with self._configuration_lock:
                    work = asyncio.get_running_loop().run_in_executor(executor, self._process, self._pending)
                    try:
                        await asyncio.wait((work,))
                        work.result()
                    except ReferenceUnavailable as exc:
                        self.unresolved = str(exc)
                        logging.getLogger(__name__).info("recovery unresolved: %s", exc)
                        continue
                    except asyncio.CancelledError:
                        self._halt.set()
                        await asyncio.gather(work, return_exceptions=True)
                        raise
                self._pending = None
                self._queue.task_done()
        except Exception as exc:
            if self.failure is None:
                self.failure = exc
            raise self.failure
        finally:
            self._accepting = self._running = False
            self.ready.clear()
            stopping_task.cancel()
            await asyncio.gather(stopping_task, return_exceptions=True)
            executor.shutdown(wait=True)
