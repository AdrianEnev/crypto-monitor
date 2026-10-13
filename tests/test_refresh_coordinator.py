"""Offline UTC scheduling, transactional configuration and refresh failure tests."""

import asyncio
from dataclasses import replace
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from contextlib import closing

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter, DiscoveredMarket, EXCHANGE_INFO_URL, TICKER_URL
from crypto_monitor.runtime.gap_recovery import RecoveryWorker, ReferenceUnavailable
from crypto_monitor.core.level_sweep import MINUTE, Levels, reference_periods
from crypto_monitor.core.market_universe import rank_alerts
from crypto_monitor.runtime.refresh_coordinator import RefreshCoordinator, next_boundaries
from crypto_monitor.persistence.reference_snapshots import DurableReferences
from crypto_monitor.runtime.service import Settings, StartupReferences
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from crypto_monitor.runtime.websocket_supervisor import WebSocketSupervisor
from tests.test_binance_feed import FakeSocket
from tests.test_binance_rest import contract, response, ticker
from tests.test_minute_klines import START, window_response
from tests.test_sqlite_store import D, event
from tests.test_websocket_supervisor import ManualSleep


def levels(symbol, *, as_of):
    return {name: Levels(D("100"), D("90")) if period else None
            for name, period in reference_periods(as_of).items()}


def initial(markets, as_of):
    from crypto_monitor.core.monitoring_engine import ReferenceLevels
    return StartupReferences(as_of, {m.symbol: {
        name: ReferenceLevels(period, levels(m.symbol, as_of=as_of)[name])
        for name, period in reference_periods(as_of).items() if period is not None
    } for m in markets})


async def until(predicate):
    async def waiting():
        while not predicate():
            await asyncio.sleep(0)
    await asyncio.wait_for(waiting(), 3)


class BoundaryTests(unittest.TestCase):
    def test_utc_five_minute_and_midnight_boundaries(self):
        for value, volume, daily in [
            ("2026-10-11T23:59:59.999999", "2026-10-12T00:00", "2026-10-12T00:00"),
            ("2026-10-12T00:00", "2026-10-12T00:05", "2026-10-13T00:00"),
            ("2026-12-31T23:59", "2027-01-01T00:00", "2027-01-01T00:00"),
            ("2026-10-09T12:04:59", "2026-10-09T12:05", "2026-10-10T00:00"),
        ]:
            utc = lambda text: datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
            with self.subTest(value=value):
                self.assertEqual(next_boundaries(utc(value)), (utc(volume), utc(daily)))
        with self.assertRaises(ValueError):
            next_boundaries(START.replace(tzinfo=None))

    def test_reference_history_keeps_correct_rollover_snapshots_only(self):
        markets = [DiscoveredMarket("BTCUSDT", "BTC", D("10"), D(".1"))]
        for before, after in [("2026-10-12T23:59", "2026-10-13T00:00"),
                              ("2026-10-11T23:59", "2026-10-12T00:00"),
                              ("2026-09-30T23:59", "2026-10-01T00:00"),
                              ("2026-12-31T23:59", "2027-01-01T00:00")]:
            a, b = (datetime.fromisoformat(v).replace(tzinfo=timezone.utc) for v in (before, after))
            with tempfile.TemporaryDirectory() as directory:
                history = DurableReferences(Path(directory) / "refs.sqlite3",
                    MagicMock(side_effect=ValueError("history unavailable")))
                history.persist(a, initial(markets, a).snapshots)
                history.persist(b, initial(markets, b).snapshots)
                for opened in (a, b):
                    actual = history("BTCUSDT", opened)
                    self.assertEqual({name: ref.period for name, ref in actual.items()},
                        {name: period for name, period in reference_periods(opened).items() if period is not None})
                with self.assertRaises(ReferenceUnavailable):
                    history("BTCUSDT", a.replace(year=2025))


class CoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "refresh.sqlite3"
        self.markets = (DiscoveredMarket("BTCUSDT", "BTC", D("100"), D(".1")),
                        DiscoveredMarket("ETHUSDT", "ETH", D("50"), D(".1")))
        self.clock = [START]
        self.tickers = [ticker("BTCUSDT", "200"), ticker("ETHUSDT", "300"), ticker("SOLUSDT", "1")]
        self.exchange = {"symbols": [contract(), contract("ETH"), contract("SOL")]}
        def opener(request, *, timeout):
            if request.full_url == TICKER_URL:
                return response(self.tickers)
            if request.full_url == EXCHANGE_INFO_URL:
                return response(self.exchange)
            return window_response(request, timeout=timeout)
        self.opener = MagicMock(side_effect=opener)
        self.rest = BinanceRESTAdapter(opener=self.opener)
        self.fetch_levels = MagicMock(side_effect=levels)
        self.refs = initial(self.markets, START)
        self.worker = RecoveryWorker(self.markets, self.path, self.rest, self.refs)
        self.supervisor = MagicMock()
        self.coordinator = RefreshCoordinator(self.worker, self.supervisor, self.rest,
            Settings(self.path, top_n=2), self.refs, now=lambda: self.clock[0], fetch_levels=self.fetch_levels)
        await self.coordinator.initialize()

    async def advance(self, when):
        self.clock[0] = when
        return await self.coordinator.refresh()

    def urls(self):
        return [call.args[0].full_url for call in self.opener.call_args_list]

    async def test_five_minute_volumes_only_and_age(self):
        self.assertFalse(await self.advance(START + timedelta(minutes=4, seconds=59)))
        self.opener.assert_not_called()
        self.assertTrue(await self.advance(START + 5 * MINUTE))
        self.assertEqual(self.urls(), [TICKER_URL])
        self.assertEqual({m.symbol: m.quote_volume for m in self.worker.markets},
                         {"BTCUSDT": D("200"), "ETHUSDT": D("300")})
        self.fetch_levels.assert_not_called()
        self.clock[0] += MINUTE
        self.assertEqual(self.coordinator.status.volume_age_seconds, 60)
        self.assertFalse(await self.coordinator.refresh())
        await self.advance(START + 10 * MINUTE)
        self.assertEqual(self.urls(), [TICKER_URL, TICKER_URL])
        self.assertEqual(self.coordinator.status.volume_age_seconds, 0)

    async def test_refreshed_volumes_are_captured_by_existing_alert_ranking(self):
        with SQLiteStore(self.path) as store:
            for market in self.markets:
                store.commit_candle(event(symbol=market.symbol), [])
        await self.advance(START + 5 * MINUTE)
        for market in self.markets:
            self.worker._process(event(1, symbol=market.symbol))
        with SQLiteStore(self.path) as store:
            ranked = rank_alerts(store.load_hits())
        self.assertEqual([hit.symbol for hit in ranked[:4]], ["ETHUSDT"] * 4)
        self.assertEqual(ranked[0].captured_quote_volume, D("300"))

    async def test_daily_unchanged_membership_fetches_exchange_once_and_levels(self):
        midnight = START.replace(day=10, hour=0)
        await self.advance(midnight)
        self.assertEqual(self.urls(), [TICKER_URL, EXCHANGE_INFO_URL])
        self.assertEqual({m.symbol for m in self.worker.markets}, {"BTCUSDT", "ETHUSDT"})
        self.assertEqual(self.fetch_levels.call_count, 2)
        self.assertEqual(self.coordinator.status.membership_updated_at, midnight)
        await self.advance(midnight + 5 * MINUTE)
        self.assertEqual(self.urls().count(EXCHANGE_INFO_URL), 1)
        self.assertEqual(self.fetch_levels.call_count, 2)

    async def test_changed_membership_has_levels_and_metadata_before_rotation(self):
        self.tickers[2] = ticker("SOLUSDT", "999")
        def rotate(symbols):
            self.assertEqual(set(symbols), {m.symbol for m in self.worker.markets})
            for symbol in symbols:
                self.assertTrue(self.worker.references(symbol, self.clock[0]))
        self.supervisor.update_symbols.side_effect = rotate
        await self.advance(START.replace(day=10, hour=0))
        self.assertEqual({m.symbol for m in self.worker.markets}, {"ETHUSDT", "SOLUSDT"})
        self.assertEqual({call.args[0] for call in self.fetch_levels.call_args_list}, {"ETHUSDT", "SOLUSDT"})

    async def test_malformed_or_partial_volume_refresh_keeps_all_good_values(self):
        original = self.worker.markets
        for payload in ([ticker("BTCUSDT", "500")], [ticker(), ticker()],
                        [ticker("BTCUSDT", "NaN"), ticker("ETHUSDT", "20")]):
            self.tickers = payload
            await self.advance(self.clock[0] + 5 * MINUTE)
            self.assertEqual(self.worker.markets, original)
            self.assertIsNotNone(self.coordinator.status.volume_error)
            self.assertEqual(self.coordinator.status.volume_updated_at, START)
        self.assertEqual(self.coordinator.status.volume_age_seconds, 900)
        self.tickers = [ticker("BTCUSDT", "400"), ticker("ETHUSDT", "40")]
        await self.advance(self.clock[0] + 5 * MINUTE)
        self.assertIsNone(self.coordinator.status.volume_error)
        self.assertEqual(self.coordinator.status.volume_age_seconds, 0)

    async def test_ticker_transport_failure_retains_values_and_exposes_age(self):
        self.coordinator.fetch_tickers = MagicMock(side_effect=TimeoutError("ticker timeout"))
        await self.advance(START + 5 * MINUTE)
        self.assertEqual(self.worker.markets, self.markets)
        self.assertEqual(self.coordinator.status.volume_age_seconds, 300)
        self.assertIn("ticker timeout", self.coordinator.status.volume_error)
        self.assertTrue(self.coordinator.status.degraded)

    async def test_membership_failure_keeps_subscriptions_but_refreshes_current_levels(self):
        self.coordinator.fetch_exchange = MagicMock(side_effect=RuntimeError("exchange unavailable"))
        midnight = START.replace(day=12, hour=0)
        await self.advance(midnight)
        self.assertEqual({m.symbol for m in self.worker.markets}, {"BTCUSDT", "ETHUSDT"})
        self.assertEqual(self.coordinator.status.membership_updated_at, START)
        self.assertIn("exchange unavailable", self.coordinator.status.membership_error)
        self.assertEqual(self.coordinator.status.levels_updated_at, midnight)
        self.assertNotIn("monday", self.worker.references("BTCUSDT", midnight))
        self.assertTrue(self.coordinator.status.degraded)

    async def test_invalid_exchange_or_missing_eligible_ticker_does_not_publish_membership(self):
        for bad_exchange in ({}, {"symbols": [contract(filters=[])]}):
            self.exchange = bad_exchange
            await self.coordinator.refresh(START.replace(day=10, hour=0), force_daily=True)
            self.assertEqual({m.symbol for m in self.worker.markets}, {"BTCUSDT", "ETHUSDT"})
            self.assertIsNotNone(self.coordinator.status.membership_error)
        self.exchange = {"symbols": [contract(), contract("ETH"), contract("SOL")]}
        self.tickers = self.tickers[:2]
        await self.coordinator.refresh(START.replace(day=10, hour=0), force_daily=True)
        self.assertIn("missing eligible symbol SOLUSDT", self.coordinator.status.membership_error)

    async def test_daily_can_use_last_successful_tickers_without_faking_freshness(self):
        await self.advance(START + 5 * MINUTE)
        self.coordinator.fetch_tickers = MagicMock(side_effect=TimeoutError("offline"))
        midnight = START.replace(day=10, hour=0)
        await self.advance(midnight)
        self.assertEqual(self.coordinator.status.membership_updated_at, midnight)
        self.assertEqual(self.coordinator.status.volume_updated_at, START + 5 * MINUTE)
        self.assertIn("offline", self.coordinator.status.volume_error)

    async def test_bad_levels_reject_whole_bundle_and_retry_current_set_at_next_tick(self):
        previous = self.worker.references
        self.tickers[2] = ticker("SOLUSDT", "999")
        def bad(symbol, *, as_of):
            if symbol == "SOLUSDT":
                return {name: None for name in reference_periods(as_of)}
            return levels(symbol, as_of=as_of)
        self.fetch_levels.side_effect = bad
        midnight = START.replace(day=12, hour=0)
        await self.advance(midnight)
        self.assertIs(self.worker.references, previous)
        self.assertEqual({m.symbol for m in self.worker.markets}, {"BTCUSDT", "ETHUSDT"})
        self.assertIsNotNone(self.coordinator.status.levels_error)
        with SQLiteStore(self.path) as store:
            self.assertIsNone(store.load_reference_snapshot("BTCUSDT", midnight))
        self.fetch_levels.side_effect = levels
        await self.advance(midnight + 5 * MINUTE)
        self.assertIsNone(self.coordinator.status.levels_error)
        self.assertEqual(self.urls().count(EXCHANGE_INFO_URL), 1)
        self.assertTrue(self.worker.references("BTCUSDT", midnight))
        self.assertIsNotNone(self.coordinator.status.membership_error)  # Retry daily selection next day.

    async def test_partial_and_malformed_levels_preserve_previous_snapshot(self):
        old = self.worker.references
        for bad in ({}, {name: Levels(D("90"), D("100")) for name in reference_periods(START)}):
            self.fetch_levels.side_effect = None
            self.fetch_levels.return_value = bad
            await self.coordinator.refresh(START + 5 * MINUTE, force_daily=True)
            self.assertIs(self.worker.references, old)
            self.assertTrue(self.coordinator.status.degraded)

    async def test_level_transport_failure_preserves_complete_snapshot(self):
        old = self.worker.references
        self.fetch_levels.side_effect = TimeoutError("daily candles timed out")
        await self.advance(START.replace(day=12, hour=0))
        self.assertIs(self.worker.references, old)
        self.assertIn("daily candles timed out", self.coordinator.status.levels_error)
        self.assertEqual(self.coordinator.status.levels_updated_at, START)

    async def test_daily_snapshot_storage_failure_does_not_publish_bundle(self):
        midnight = START.replace(day=12, hour=0)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TRIGGER reject_refresh BEFORE INSERT ON reference_snapshots
                          WHEN NEW.symbol = 'ETHUSDT'
                          BEGIN SELECT RAISE(ABORT, 'storage failure'); END""")
        await self.advance(midnight)
        self.assertIn("storage failure", self.coordinator.status.levels_error)
        self.assertEqual(self.coordinator.status.levels_updated_at, START)
        with SQLiteStore(self.path) as store:
            for market in self.markets:
                self.assertIsNone(store.load_reference_snapshot(market.symbol, midnight))
                self.assertIsNotNone(store.load_reference_snapshot(market.symbol, START))

    async def test_daily_snapshot_available_after_restart_without_historical_fetch(self):
        monday, tuesday = START.replace(day=12, hour=0), START.replace(day=13, hour=0)
        await self.advance(monday)
        await self.advance(tuesday)
        restarted = DurableReferences(self.path, MagicMock(side_effect=AssertionError("no network")))
        for opened in (START, monday, tuesday):
            self.assertEqual(restarted("BTCUSDT", opened), self.worker.references("BTCUSDT", opened))

    async def test_cancelled_fetch_finishes_without_publishing_or_rotating(self):
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        def fetch():
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("test release missing")
            return self.tickers
        self.coordinator.fetch_tickers = fetch
        task = asyncio.create_task(self.advance(START + 5 * MINUTE))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(self.worker.markets, self.markets)
            self.assertEqual(self.coordinator.status.volume_updated_at, START)
            self.supervisor.update_symbols.assert_not_called()
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_committed_duplicate_needs_no_expired_reference_snapshot(self):
        ancient = event(-14 * 24 * 60)
        with SQLiteStore(self.path) as store:
            store.commit_candle(ancient, [])
        self.worker._process(ancient)
        self.worker._process(event(-15 * 24 * 60))
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, ancient.candle.open_time)
            self.assertEqual(store.load_hits(), [])

    async def test_scheduler_uses_injected_utc_clock_and_sleep_and_skips_missed_ticks(self):
        timer, stop = ManualSleep(), asyncio.Event()
        self.coordinator.sleep = timer
        task = asyncio.create_task(self.coordinator.run(stop))
        try:
            delay, wake = await timer.next()
            self.assertEqual(delay, 300)
            self.clock[0] = START + timedelta(minutes=16)
            wake.set_result(None)
            delay, _ = await timer.next()
            self.assertEqual(delay, 240)
            self.assertEqual(self.urls(), [TICKER_URL])
            stop.set()
            await asyncio.wait_for(task, 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_stale_pending_candle_not_committed_and_resumes_after_reference_refresh(self):
        sunday = START.replace(day=11, hour=23, minute=59)
        monday = sunday + MINUTE
        self.fetch_levels.side_effect = TimeoutError("daily history unavailable")
        def at(opened):
            return event((opened - START) // MINUTE)
        with SQLiteStore(self.path) as store:
            store.commit_candle(at(sunday), [])
        stop = asyncio.Event()
        task = asyncio.create_task(self.worker.run(stop))
        try:
            await asyncio.wait_for(self.worker.ready.wait(), 2)
            await self.worker.submit(at(monday))
            await until(lambda: self.worker.unresolved is not None)
            self.assertIsNotNone(self.coordinator.status.unresolved)
            with SQLiteStore(self.path) as store:
                self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, sunday)
                self.assertEqual(store.load_hits(), [])
            self.fetch_levels.side_effect = levels
            await self.advance(monday)
            await until(lambda: self.worker._pending is None)
            with SQLiteStore(self.path) as store:
                self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, monday)
                self.assertEqual(len(store.load_hits()), 3)  # Monday range is inactive.
            self.assertFalse(task.done())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_uncached_historical_backfill_remains_unresolved(self):
        # First missing candle uses the previous week's references, never loaded here.
        before = START.replace(day=4, hour=23, minute=59)
        incoming = START.replace(day=5, hour=0, minute=1)
        self.fetch_levels.side_effect = lambda symbol, *, as_of: {name: None for name in reference_periods(as_of)}
        with SQLiteStore(self.path) as store:
            store.commit_candle(event((before - START) // MINUTE), [])
        stop = asyncio.Event()
        task = asyncio.create_task(self.worker.run(stop))
        try:
            await asyncio.wait_for(self.worker.ready.wait(), 2)
            await self.worker.submit(event((incoming - START) // MINUTE))
            await until(lambda: self.worker.unresolved is not None)
            with SQLiteStore(self.path) as store:
                self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, before)
            self.assertIn("daily history unavailable", self.worker.unresolved)
            self.assertIsNotNone(self.worker._pending)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_backfill_uses_each_candles_cached_period_across_midnight(self):
        sunday = START.replace(day=11, hour=23, minute=58)
        monday = START.replace(day=12, hour=0)
        def at(opened):
            return event((opened - START) // MINUTE)
        with SQLiteStore(self.path) as store:
            store.commit_candle(at(sunday), [])
        self.fetch_levels.side_effect = lambda symbol, *, as_of: {
            name: Levels(D("110"), D("90")) if period else None
            for name, period in reference_periods(as_of).items()}
        await self.advance(monday)
        self.worker._process(at(monday + MINUTE))
        with SQLiteStore(self.path) as store:
            hits = store.load_hits()
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, monday + MINUTE)
        # Sunday's recovered candle sweeps its old 100 levels; Monday candles
        # cannot breach the refreshed 110 levels with the fixture's high of 101.
        self.assertEqual(len(hits), 4)
        self.assertEqual({hit.candle_time for hit in hits}, {sunday + MINUTE})
        self.assertEqual({hit.level_price for hit in hits}, {D("100")})

    async def test_current_live_period_does_not_allow_uncached_older_backfill(self):
        sunday = START.replace(day=4, hour=23, minute=58)
        self.fetch_levels.side_effect = lambda symbol, *, as_of: {name: None for name in reference_periods(as_of)}
        with SQLiteStore(self.path) as store:
            store.commit_candle(event((sunday - START) // MINUTE), [])
        # The live timestamp matches today's snapshot, but the first missing
        # minute requires a previous-week snapshot that was never loaded.
        with self.assertRaises(ReferenceUnavailable):
            self.worker._process(event(1))
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, sunday)
            self.assertEqual(store.load_hits(), [])

    async def test_configuration_swap_waits_until_current_candle_is_committed(self):
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(), [])
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        old = self.worker.references
        def blocked(symbol, opened):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("test release missing")
            return old(symbol, opened)
        await self.worker.apply_configuration(self.markets, blocked)
        task = asyncio.create_task(self.worker.run(asyncio.Event()))
        update = None
        try:
            await asyncio.wait_for(self.worker.ready.wait(), 2)
            await self.worker.submit(event(1))
            await asyncio.wait_for(entered.wait(), 2)
            update = asyncio.create_task(self.advance(START + 5 * MINUTE))
            await until(lambda: self.opener.called)
            self.assertFalse(update.done())
            self.assertEqual(self.worker.markets, self.markets)
            release.set()
            await asyncio.wait_for(update, 2)
            # An unswept low remains eligible after the volume update.
            low_event = event(2)
            low_event = replace(low_event, candle=replace(low_event.candle, low=D("89")))
            await self.worker.submit(low_event)
            await until(lambda: self.worker._pending is None and self.worker._queue.empty())
            with SQLiteStore(self.path) as store:
                hits = store.load_hits()
            self.assertEqual({h.captured_quote_volume for h in hits if h.candle_time == START + MINUTE}, {D("100")})
            self.assertEqual({h.captured_quote_volume for h in hits if h.candle_time == START + 2 * MINUTE}, {D("200")})
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, *([update] if update else []), return_exceptions=True)

    async def test_removed_and_resumed_symbol_retains_sqlite_cursor(self):
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(symbol="ETHUSDT"), [])
        await self.worker.apply_configuration(self.markets[:1], self.worker.references)
        self.worker._process(event(1, symbol="ETHUSDT"))
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["ETHUSDT"].open_time, START)
        await self.worker.apply_configuration(self.markets, self.worker.references)
        self.worker._process(event(3, symbol="ETHUSDT"))
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["ETHUSDT"].open_time, START + 3 * MINUTE)
            self.assertEqual(len(store.load_hits()), 4)

    async def test_membership_rotation_keeps_actual_worker_alive_and_closes_old_socket_first(self):
        first, second = FakeSocket(), FakeSocket()
        def connect(*args, **kwargs):
            if connector.call_count == 1:
                return first
            self.assertTrue(first.closed)
            return second
        connector = MagicMock(side_effect=connect)
        supervisor = WebSocketSupervisor(self.worker, connector=connector)
        self.coordinator.supervisor = supervisor
        stop = asyncio.Event()
        with patch.object(self.worker, "run", wraps=self.worker.run) as worker_run:
            task = asyncio.create_task(supervisor.run(stop))
            try:
                await asyncio.wait_for(first.entered.wait(), 2)
                # Volume-only and unchanged daily selection must not reconnect.
                await self.advance(START + 5 * MINUTE)
                await self.advance(START.replace(day=10, hour=0))
                self.assertEqual(connector.call_count, 1)
                self.tickers[2] = ticker("SOLUSDT", "999")
                await self.advance(START.replace(day=11, hour=0))
                await asyncio.wait_for(second.entered.wait(), 2)
                self.assertIn("ethusdt@kline_1m/solusdt@kline_1m", connector.call_args.args[0])
                self.assertTrue(self.worker.ready.is_set())
                self.assertEqual(worker_run.call_count, 1)
                self.assertEqual(supervisor.status.last_disconnect, "market membership changed")
                stop.set()
                await asyncio.wait_for(task, 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
