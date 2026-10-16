"""Live trade-touch service and quiet historical preparation."""

import asyncio
import logging
import os
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Event

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter, KLINES_URL, parse_quote_volumes
from crypto_monitor.adapters.binance_trades import TradeSupervisor
from crypto_monitor.adapters.touch_email import TouchEmailDispatcher
from crypto_monitor.core.level_sweep import reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.core.touch_monitor import crosses, next_eligible
from crypto_monitor.persistence.touch_store import TouchStore
from crypto_monitor.runtime.touch_history import TouchHistoryScanner


LOG = logging.getLogger(__name__)


def live_fresh_events(events, live_started):
    """Earlier sessions never re-enter the urgent panel after restart/reconnect."""
    return tuple(event for event in events if live_started is not None
                 and event["observed_at"] > live_started)


@dataclass(frozen=True)
class PriceState:
    """Last accepted price; a REST snapshot has no aggregate trade ID."""
    price: Decimal
    at: datetime
    trade_id: int | None


def is_newer(tick, state):
    """Aggregate trade IDs order one symbol; times order snapshot handoffs."""
    if state.trade_id is not None and tick.trade_id is not None:
        return tick.trade_id > state.trade_id
    return tick.at > state.at


class TouchRuntime:
    """All mutable SQLite work belongs to one background thread.

    Ordered price state is in memory and shared by every watch of a symbol.
    History replay and the handoff minute establish it quietly before alerts.
    """

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
        self._market_cache = {}
        self._watch_cache = {}
        self._prices = {}
        # Fresh panel events belong to this run, independent of clock changes.
        self.run_id = uuid.uuid4().hex

    def _cache_watches(self, symbol):
        """Publish immutable eligibility data after this worker changes state.

        SQLite remains authoritative for accepting a contact. This cache only
        avoids durable work for prices that cannot produce an activation.
        """
        watches = []
        for watch in self.store.history_jobs(symbol):
            if not watch.available or watch.level_price is None:
                continue
            eligible = (next_eligible(watch.name, watch.accepted_at, watch.watch_end,
                                      self.policy.delays)
                        if watch.accepted_at is not None else watch.watch_start)
            if watch.name == "monday" and watch.accepted_at is not None:
                continue
            watches.append((watch.level_price, watch.watch_start, watch.watch_end, eligible))
        self._watch_cache[symbol] = tuple(watches)

    def ready(self, symbol):
        """Whether observe() can run on the event loop without database work."""
        return symbol in self._market_cache and symbol in self._watch_cache

    def observe(self, tick):
        """Advance this symbol's ordered price; return the prior price if it may contact.

        No I/O: the service calls this while holding its processing lock.
        Every newer trade advances state, including trades during a delay,
        so a later contact compares consecutive prices. Older or repeated
        trades are ignored and never rewind state. A None result needs no
        durable work; SQLite still decides acceptance for the others.
        """
        state = self._prices.get(tick.symbol)
        if state is not None and not is_newer(tick, state):
            return None
        self._prices[tick.symbol] = PriceState(tick.price, tick.at, tick.trade_id)
        previous = state.price if state is not None else None
        if previous is None:
            return None
        for level, start, end, eligible in self._watch_cache.get(tick.symbol, ()):
            if start <= tick.at < end and eligible <= tick.at and crosses(previous, tick.price, level):
                return previous
        return None

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
        self._market_cache = {market.symbol: market for market in self.markets}
        self._watch_cache = {symbol: watches for symbol, watches in self._watch_cache.items()
                             if symbol in self._market_cache}
        # A coin that leaves and later returns must not compare with an old price.
        self._prices = {symbol: state for symbol, state in self._prices.items()
                        if symbol in self._market_cache}
        self.progress.update(stage="Calculating reference levels", references_done=0, history_done=0,
                             total=len(self.markets))
        self.progress.pop("catchup_done", None)
        self.progress.pop("handoff_done", None)
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
            self._cache_watches(market.symbol)
            self.progress["references_done"] = index + 1
        self.catch_up(asof)
        self.configured_day = asof.date()

    def catch_up(self, asof, *, stage="Checking past touches", counter="history_done"):
        self.progress.update(stage=stage, **{counter: 0})
        scanner = TouchHistoryScanner(self.store, self.rest, self.policy.delays,
                                      cancelled=self.cancelled.is_set)
        for index, market in enumerate(self.markets):
            self.check_stop()
            self.progress["symbol"] = market.symbol
            scanner.scan(market.symbol, asof)
            self._cache_watches(market.symbol)
            self.progress[counter] = index + 1

    def seed_partial_minutes(self, asof):
        """Cover the currently forming minute quietly; never claim a final close."""
        self.progress.update(stage="Checking current minutes before live alerts", handoff_done=0)
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
                opening, high, low, close = (Decimal(value) for value in row[1:5])
                if (not all(value.is_finite() for value in (opening, high, low, close))
                        or low < 0 or not low <= min(opening, close)
                        or max(opening, close) > high):
                    raise ValueError("invalid current minute prices")
                self.store.record_candle(market.symbol, opened=opened, opening=opening, low=low,
                    high=high, close=close, at=current, delays=self.policy.delays,
                    origin="history", notify=False, quote_volume=market.quote_volume,
                    verified_only=True, run_id=self.run_id)
                # Buffered trades up to this snapshot are already in its range.
                self._prices[market.symbol] = PriceState(close, current, None)
            except sqlite3.Error:
                raise
            except Exception as exc:
                # A missing handoff minute must not be mistaken for untouched.
                self._prices.pop(market.symbol, None)
                for watch in self.store.history_jobs(market.symbol):
                    self.store.mark_history_unavailable(market.symbol, watch.name, watch.source_start,
                                                       f"live handoff unavailable: {type(exc).__name__}")
            self._cache_watches(market.symbol)
            self.progress["handoff_done"] = index + 1

    def process(self, tick, *, quiet, email_enabled, observed):
        market = self._market_cache.get(tick.symbol)
        if market is None:
            market = next((market for market in self.markets if market.symbol == tick.symbol), None)
            if market is not None:
                self._market_cache[tick.symbol] = market
        if market is None:
            return []
        if tick.symbol not in self._watch_cache:
            self._cache_watches(tick.symbol)
        previous = self.observe(tick)
        if previous is None:
            return []
        return self.record(tick, previous, quiet=quiet, email_enabled=email_enabled,
                           observed=observed)

    def record(self, tick, previous, *, quiet, email_enabled, observed):
        """Durably accept a contact that observe() found between two trades."""
        market = self._market_cache[tick.symbol]
        activations = self.store.record_price(tick.symbol, tick.price, tick.at, self.policy.delays,
            previous=previous, origin="history" if quiet else "live",
            notify=email_enabled and not quiet, quote_volume=market.quote_volume,
            observed_at=observed, verified_only=True, trade_id=tick.trade_id, run_id=self.run_id)
        self._cache_watches(tick.symbol)
        return activations

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
                self._market_cache = {market.symbol: market for market in self.markets}
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
                    self._cache_watches(symbol)
                    TouchHistoryScanner(self.store, self.rest, self.policy.delays,
                                        cancelled=self.cancelled.is_set).scan(symbol, asof)
                    self._cache_watches(symbol)
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
    email_enabled = settings.email.enabled
    email_enabled_since = None
    email_error = None
    email_task = None
    email_stop = None
    email_failure = loop.create_future()
    email_toggle_lock = asyncio.Lock()
    supervisor = None
    last_received = None
    shutting_down = False

    async def work(function, *args, **kwargs):
        future = loop.run_in_executor(executor, lambda: function(*args, **kwargs))
        try:
            # wait() leaves the executor future intact if this coroutine is
            # cancelled. Unlike shield() on Python 3.14, it does not log an
            # expected InterruptedError before cleanup can retrieve it.
            await asyncio.wait((future,))
            return future.result()
        except asyncio.CancelledError:
            cancelled.set()
            try:
                await asyncio.gather(future, return_exceptions=True)
            finally:
                # A failed socket may cancel its preparation before reconnecting.
                # The owning worker has finished before this flag is reused.
                if not stop.is_set() and not shutting_down:
                    cancelled.clear()
            raise

    async def collector():
        stamp = now()
        def read():
            with TouchStore.open_readonly(settings.database) as store:
                return store.dashboard_snapshot(runtime.markets, stamp, policy.delays,
                                                run_id=runtime.run_id)
        try:
            data = await asyncio.to_thread(read)
        except Exception as exc:
            data = {"watches": (), "fresh": (), "pending_email_count": None,
                    "error": f"Status unavailable: {type(exc).__name__}"}
        error = data.get("error") or runtime.error or (supervisor.error if supervisor else None)
        data["fresh"] = live_fresh_events(data.get("fresh", ()), live_started)
        error = error or email_error
        if email_enabled and email is not None and email.status.last_error:
            error = error or "Email delivery needs attention"
        if last_received is not None and (stamp - last_received).total_seconds() > float(policy.max_age_seconds):
            error = error or "Waiting for fresh prices"
        return dict(data, now=stamp, markets=runtime.markets,
                    connection=supervisor.state if supervisor else "loading", email_enabled=email_enabled,
                    error=error, highlight_seconds=float(policy.highlight_seconds))

    def report_email_failure(failure):
        if not stop.is_set() and not email_failure.done():
            email_failure.set_result(failure)

    def start_email(configuration):
        nonlocal email, email_task, email_stop
        email = email_factory(settings.database, configuration,
            retry_seconds=policy.email_retry_seconds, max_age_seconds=policy.max_age_seconds, now=now)
        email_stop = asyncio.Event()
        sender_stop = email_stop
        email_task = asyncio.create_task(email.run(sender_stop))

        def finished(task):
            if task.cancelled():
                failure = RuntimeError("email sender cancelled unexpectedly")
            else:
                failure = task.exception()
                if failure is None and not sender_stop.is_set():
                    failure = RuntimeError("email sender stopped unexpectedly")
            if failure is not None:
                report_email_failure(failure)

        email_task.add_done_callback(finished)

    def request_email_stop():
        if email_stop is not None:
            email_stop.set()
        if email is not None:
            request_stop = getattr(email, "request_stop", None)
            if request_stop is not None:
                request_stop()

    async def watch_email():
        # Normal ON/OFF transitions finish sender tasks without ending monitoring.
        # Provider bookkeeping failures still stop the service visibly.
        raise await email_failure

    async def change_email():
        nonlocal email_enabled, email_enabled_since, email_error, email, email_task, email_stop
        async with email_toggle_lock:
            if stop.is_set() or shutting_down:
                return email_enabled
            if email_enabled:
                async with lock:
                    email_enabled = False
                    email_enabled_since = None
                    email_error = None
                    request_email_stop()
                # Keep prices flowing while an in-flight send saves its result.
                if email_task is not None:
                    await email_task
                async with lock:
                    await work(runtime.store.mute_pending_touch_outbox, now(), reason="email disabled")
                    email = email_task = email_stop = None
                LOG.info("Email OFF; pending notifications muted")
            else:
                try:
                    configuration = replace(settings.email, enabled=True)
                    if os.environ.get("SES_SMTP_HOST"):
                        from crypto_monitor.adapters.ses_smtp import SESEmailSMTPSettings
                        SESEmailSMTPSettings.from_env()
                except ValueError as exc:
                    email_error = f"Email stays off: {exc}"
                    LOG.warning("%s", email_error)
                    return False
                async with lock:
                    if stop.is_set() or shutting_down:
                        return False
                    start_email(configuration)
                    email_enabled = True
                    email_enabled_since = now()
                    email_error = None
                LOG.info("Email ON; only new live activations will notify")
            return email_enabled

    async def toggle_email():
        try:
            return await change_email()
        except Exception as exc:
            report_email_failure(exc)
            raise

    display = None
    if should_use_dashboard(settings):
        try:
            display = (dashboard_factory or TouchDashboard)(collector, layout=settings.layout,
                highlight_seconds=float(policy.highlight_seconds), on_toggle_email=toggle_email)
        except Exception as exc:
            LOG.warning("Terminal unavailable; continuing plain logs (%s)", type(exc).__name__)

    async def on_connect():
        nonlocal live_started
        live_started = None
        async with lock:
            if runtime.configured_day != now().date():
                await work(runtime.configure, now())
            else:
                await work(runtime.catch_up, now(), stage="Catching up on time spent loading",
                           counter="catchup_done")
            await work(runtime.seed_partial_minutes, now())
            progress.update(stage="Recording prices received while loading", symbol="")
        # Alternate supervisors used by embedders may have no readiness hook.
        if not hasattr(supervisor, "on_ready"):
            await on_ready()

    async def on_ready():
        nonlocal live_started
        if stop.is_set():
            return
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
                     or (observed - tick.at).total_seconds() >= float(policy.max_age_seconds))
            notify = email_enabled and (email_enabled_since is None or tick.at > email_enabled_since)
            if tick.at > observed + timedelta(seconds=1):
                # A trade dated ahead of this clock still orders prices, but
                # its time cannot start a delay or count as an activation.
                if runtime.ready(tick.symbol):
                    runtime.observe(tick)
                activations = []
            elif runtime.ready(tick.symbol):
                previous = runtime.observe(tick)
                activations = (await work(runtime.record, tick, previous, quiet=quiet,
                                          email_enabled=notify, observed=observed)
                               if previous is not None else [])
            else:
                activations = await work(runtime.process, tick, quiet=quiet,
                                         email_enabled=notify, observed=observed)
        if not quiet:
            for activation in activations:
                LOG.info("LIVE TOUCH %s %s %s level=%s price=%s previous=%s", activation.symbol,
                         activation.level_name, activation.side, activation.level_price,
                         activation.trigger_price, activation.previous_price)
            if activations and notify and email_enabled and email is not None:
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
        if hasattr(supervisor, "on_ready"):
            supervisor.on_ready = on_ready
        tasks.append(asyncio.create_task(watch_email()))
        if email_enabled:
            start_email(settings.email)
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
        shutting_down = True
        cancelled.set()
        loading_stop.set()
        request_email_stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if email_task is not None:
            await asyncio.gather(email_task, return_exceptions=True)
        if loading_task is not None:
            await asyncio.gather(loading_task, return_exceptions=True)
        await loop.run_in_executor(executor, runtime.close)
        executor.shutdown(wait=True)
        LOG.info("Touch monitor stopped; state saved")
