"""Live trade-touch service and quiet historical preparation."""

import asyncio
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Event

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter, KLINES_URL, parse_quote_volumes
from crypto_monitor.adapters.binance_trades import TradeSupervisor
from crypto_monitor.adapters.touch_email import TouchEmailDispatcher
from crypto_monitor.core.level_sweep import reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.persistence.touch_store import TouchStore
from crypto_monitor.runtime.touch_history import TouchHistoryScanner


LOG = logging.getLogger(__name__)


def live_fresh_events(events, live_started):
    """Earlier sessions never re-enter the urgent panel after restart/reconnect."""
    return tuple(event for event in events if live_started is not None
                 and event["observed_at"] > live_started)


class TouchRuntime:
    """All mutable SQLite work belongs to one background thread."""

    def __init__(self, settings, policy, progress, cancelled, *, rest_factory=BinanceRESTAdapter,
                 now=lambda: datetime.now(timezone.utc)):
        self.settings, self.policy, self.progress = settings, policy, progress
        self.cancelled, self.rest_factory = cancelled, rest_factory
        self.now = now
        self.store = self.rest = None
        self.markets = ()
        self.error = None
        self.configured_day = None
        self.last_daily_attempt = None

    def check_stop(self):
        if self.cancelled.is_set():
            raise InterruptedError("monitor preparation stopped")

    def prepare(self, asof):
        self.check_stop()
        self.progress["stage"] = "Selecting coins"
        self.rest = self.rest_factory()
        self.markets = tuple(self.rest.discover_markets(top_n=self.settings.top_n,
            include_symbols=self.settings.include_symbols, exclude_symbols=self.settings.exclude_symbols))
        if not self.markets:
            raise ValueError("no eligible markets selected")
        self.store = TouchStore(self.settings.database)
        self.store.begin_run(asof)
        self.progress.update(total=len(self.markets), references_done=0, history_done=0)
        self.configure(asof)

    def configure(self, asof):
        self.progress.update(stage="Calculating reference levels", references_done=0, history_done=0,
                             total=len(self.markets))
        periods = reference_periods(asof)
        for index, market in enumerate(self.markets):
            self.check_stop()
            self.progress["symbol"] = market.symbol
            try:
                levels = self.rest.fetch_reference_levels(market.symbol, as_of=asof)
                if set(levels) != set(periods):
                    raise ValueError("incomplete reference response")
            except Exception as exc:
                levels = {name: None for name in periods}
                LOG.info("reference levels unavailable symbol=%s category=%s", market.symbol, type(exc).__name__)
            references = {name: ReferenceLevels(period, levels[name])
                          for name, period in periods.items() if period is not None}
            self.store.initialize_watches(market.symbol, references, asof)
            self.progress["references_done"] = index + 1
        self.catch_up(asof)
        self.configured_day = asof.date()

    def catch_up(self, asof):
        self.progress.update(stage="Checking past touches", history_done=0)
        scanner = TouchHistoryScanner(self.store, self.rest, self.policy.delays,
                                      cancelled=self.cancelled.is_set)
        for index, market in enumerate(self.markets):
            self.check_stop()
            self.progress["symbol"] = market.symbol
            scanner.scan(market.symbol, asof)
            self.progress["history_done"] = index + 1

    def seed_partial_minutes(self, asof):
        """Cover the currently forming minute quietly; never claim a final close."""
        self.progress.update(stage="Joining live prices", history_done=0)
        for index, market in enumerate(self.markets):
            self.check_stop()
            current = self.now()
            opened = current.replace(second=0, microsecond=0)
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            try:
                start = (opened - epoch) // timedelta(milliseconds=1)
                payload = self.rest._get_json(
                    f"{KLINES_URL}?symbol={market.symbol}&interval=1m&startTime={start}&limit=1")
                if not isinstance(payload, list) or len(payload) != 1:
                    raise ValueError("current-minute coverage unavailable")
                row = payload[0]
                if len(row) < 7 or type(row[0]) is not int or row[0] != start:
                    raise ValueError("invalid current minute")
                if row[6] != start + 59999:
                    raise ValueError("invalid current minute ending time")
                high, low = Decimal(row[2]), Decimal(row[3])
                if not high.is_finite() or not low.is_finite() or low < 0 or high < low:
                    raise ValueError("invalid current minute prices")
                self.store.record_range(market.symbol, low, high, current, self.policy.delays,
                    origin="history", notify=False, quote_volume=market.quote_volume, verified_only=True)
            except sqlite3.Error:
                raise
            except Exception as exc:
                # A missing handoff minute must not be mistaken for untouched.
                for watch in self.store.history_jobs(market.symbol):
                    self.store.mark_history_unavailable(market.symbol, watch.name, watch.source_start,
                                                       f"live handoff unavailable: {type(exc).__name__}")
            self.progress["history_done"] = index + 1

    def process(self, tick, *, quiet, email_enabled, observed):
        market = next((market for market in self.markets if market.symbol == tick.symbol), None)
        if market is None:
            return []
        return self.store.record_price(tick.symbol, tick.price, tick.at, self.policy.delays,
            origin="history" if quiet else "live", notify=email_enabled and not quiet,
            quote_volume=market.quote_volume, observed_at=observed, verified_only=True)

    def refresh(self, asof, daily):
        self.check_stop()
        if daily:
            self.last_daily_attempt = asof.date()
        try:
            if daily:
                selected = tuple(self.rest.discover_markets(top_n=self.settings.top_n,
                    include_symbols=self.settings.include_symbols, exclude_symbols=self.settings.exclude_symbols))
                if not selected:
                    raise ValueError("empty market refresh")
                self.markets = selected
                self.configure(asof)
            else:
                volumes = parse_quote_volumes(self.rest.fetch_tickers())
                self.markets = tuple(replace(m, quote_volume=volumes[m.symbol]) for m in self.markets)
                # Retry only watches with unavailable reference/history.
                missing = {row["symbol"] for row in self.store.dashboard_snapshot(
                    self.markets, asof, self.policy.delays)["watches"]
                    if row["status"] == "unavailable" and row.get("reason") != "Monday candle still forming"}
                periods = reference_periods(asof)
                for symbol in missing:
                    self.check_stop()
                    levels = self.rest.fetch_reference_levels(symbol, as_of=asof)
                    refs = {name: ReferenceLevels(period, levels[name])
                            for name, period in periods.items() if period is not None}
                    self.store.initialize_watches(symbol, refs, asof)
                    TouchHistoryScanner(self.store, self.rest, self.policy.delays).scan(symbol, asof)
            self.error = None
        except InterruptedError:
            raise
        except sqlite3.Error:
            raise
        except Exception as exc:
            self.error = f"Refresh unavailable: {type(exc).__name__}"
            LOG.warning("%s", self.error)
        return tuple(m.symbol for m in self.markets)

    def close(self):
        if self.store is not None:
            self.store.close()


async def run_touch_service(settings, *, policy, stop=None, rest_factory=BinanceRESTAdapter,
                            supervisor_factory=TradeSupervisor, email_factory=TouchEmailDispatcher,
                            dashboard_factory=None, now=lambda: datetime.now(timezone.utc)):
    from crypto_monitor.runtime.service import should_use_dashboard
    from crypto_monitor.ui.touch_dashboard import TouchDashboard
    stop = stop if stop is not None else asyncio.Event()
    if stop.is_set():
        return
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="touch-monitor")
    cancelled = Event()
    progress = {"stage": "Preparing", "total": 0, "references_done": 0, "history_done": 0}
    runtime = TouchRuntime(settings, policy, progress, cancelled, rest_factory=rest_factory, now=now)
    loop = asyncio.get_running_loop()
    lock = asyncio.Lock()
    tasks = []
    loading_stop = asyncio.Event()
    loading_task = None
    live_started = None
    email = None
    supervisor = None
    last_received = None

    async def work(function, *args, **kwargs):
        future = loop.run_in_executor(executor, lambda: function(*args, **kwargs))
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            cancelled.set()
            await asyncio.gather(future, return_exceptions=True)
            raise

    async def collector():
        stamp = now()
        def read():
            with TouchStore.open_readonly(settings.database) as store:
                return store.dashboard_snapshot(runtime.markets, stamp, policy.delays)
        try:
            data = await asyncio.to_thread(read)
        except Exception as exc:
            data = {"watches": (), "fresh": (), "pending_email_count": None,
                    "error": f"Status unavailable: {type(exc).__name__}"}
        error = data.get("error") or runtime.error or (supervisor.error if supervisor else None)
        data["fresh"] = live_fresh_events(data.get("fresh", ()), live_started)
        if email is not None and email.status.last_error:
            error = error or "Email delivery needs attention"
        if last_received is not None and (stamp - last_received).total_seconds() > float(policy.max_age_seconds):
            error = error or "Waiting for fresh prices"
        return dict(data, now=stamp, markets=runtime.markets,
                    connection=supervisor.state if supervisor else "loading", email_enabled=settings.email.enabled,
                    error=error, highlight_seconds=float(policy.highlight_seconds))

    display = None
    if should_use_dashboard(settings):
        try:
            display = (dashboard_factory or TouchDashboard)(collector, layout=settings.layout,
                highlight_seconds=float(policy.highlight_seconds))
        except Exception as exc:
            LOG.warning("Terminal unavailable; continuing plain logs (%s)", type(exc).__name__)

    async def on_connect():
        nonlocal live_started
        async with lock:
            if runtime.configured_day != now().date():
                await work(runtime.configure, now())
            else:
                await work(runtime.catch_up, now())
            await work(runtime.seed_partial_minutes, now())
            live_started = now()
            loading_stop.set()
            LOG.info("LIVE: %s selected coins; historical touches recorded quietly", len(runtime.markets))

    async def process(tick):
        nonlocal last_received
        async with lock:
            observed = now()
            if (runtime.configured_day != observed.date()
                    and runtime.last_daily_attempt != observed.date()):
                await work(runtime.refresh, observed, True)
                supervisor.update_symbols([m.symbol for m in runtime.markets])
                observed = now()
            last_received = observed
            quiet = (live_started is None or tick.at <= live_started
                     or tick.at > observed + timedelta(seconds=1)
                     or (observed - tick.at).total_seconds() >= float(policy.max_age_seconds))
            activations = await work(runtime.process, tick, quiet=quiet,
                                     email_enabled=settings.email.enabled, observed=observed)
        if not quiet:
            for activation in activations:
                LOG.info("LIVE TOUCH %s %s %s price=%s", activation.symbol,
                         activation.level_name, activation.side, activation.level_price)
            if activations and email is not None:
                email.notify()

    async def refreshing():
        while not stop.is_set():
            stamp = now()
            minute = stamp.replace(second=0, microsecond=0)
            boundary = minute.replace(minute=minute.minute // 5 * 5) + timedelta(minutes=5)
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0, (boundary - stamp).total_seconds()))
                return
            except asyncio.TimeoutError:
                pass
            daily = ((boundary.hour == 0 and boundary.minute == 0)
                     or runtime.configured_day != now().date())
            async with lock:
                symbols = await work(runtime.refresh, now(), daily)
            supervisor.update_symbols(symbols)

    async def displays():
        try:
            if loading_task is not None:
                await loading_task
            if not stop.is_set():
                await display.run(stop)
        except Exception as exc:
            LOG.warning("Terminal unavailable; continuing plain logs (%s)", type(exc).__name__)
            await stop.wait()

    async def stopping():
        await stop.wait()
        cancelled.set()

    try:
        if display is not None:
            loading_task = asyncio.create_task(display.run_loading(loading_stop, progress))
        tasks.append(asyncio.create_task(stopping()))
        await work(runtime.prepare, now())
        if stop.is_set():
            return
        supervisor = supervisor_factory([m.symbol for m in runtime.markets], process, on_connect)
        if settings.email.enabled:
            email = email_factory(settings.database, settings.email,
                retry_seconds=policy.email_retry_seconds, max_age_seconds=policy.max_age_seconds, now=now)
            tasks.append(asyncio.create_task(email.run(stop)))
        tasks.extend((asyncio.create_task(supervisor.run(stop)), asyncio.create_task(refreshing())))
        if display is not None:
            tasks.append(asyncio.create_task(displays()))
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if stop.is_set():
            return
        for task in done:
            await task
        if not stop.is_set():
            raise RuntimeError("touch service component stopped unexpectedly")
    except InterruptedError:
        if not stop.is_set():
            raise
    finally:
        cancelled.set()
        loading_stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if loading_task is not None:
            await asyncio.gather(loading_task, return_exceptions=True)
        await loop.run_in_executor(executor, runtime.close)
        executor.shutdown(wait=True)
        LOG.info("Touch monitor stopped; state saved")
