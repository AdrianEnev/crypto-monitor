"""Offline tests of the new runtime with real, temporary touch-state databases."""

import asyncio
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from unittest.mock import MagicMock, patch

from crypto_monitor.adapters.binance_rest import DiscoveredMarket
from crypto_monitor.adapters.binance_trades import PriceTick
from crypto_monitor.adapters.ses_email import DeliveryStatus, EmailSettings
from crypto_monitor.core.level_sweep import DAY, MINUTE, Candle, Levels, reference_periods
from crypto_monitor.persistence.touch_store import TouchStore
from crypto_monitor.runtime.service import Settings
from crypto_monitor.runtime.touch_service import TouchRuntime, run_touch_service
from crypto_monitor.runtime.touch_settings import TouchSettings, load_touch_settings


START = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
ENABLED_EMAIL = EmailSettings(True, "monitor@example.com", "recipient@example.com", "eu-west-1")
DELAYS = {name: Decimal("5") for name in
          ("previous_week", "previous_month", "previous_quarter")}
POLICY = TouchSettings(DELAYS, Decimal("30"), Decimal("3"), Decimal("60"))


def tick(price, at, *, symbol="FILUSDT", trade_id=1):
    return PriceTick(symbol, Decimal(price), at, at, trade_id)


class FakeREST:
    """Completed ranges avoid unnecessary minute downloads, as production does."""

    def __init__(self, *, symbols=("FILUSDT",), touches=None, reference_failures=(),
                 minute_failure=False, invalid_partial=False, partial_range=("99", "95")):
        self.markets = tuple(DiscoveredMarket(symbol, symbol.removesuffix("USDT"),
                                             Decimal("1000000"), Decimal("0.001"))
                             for symbol in symbols)
        self.touches = dict(touches or {})
        self.reference_failures = set(reference_failures)
        self.minute_failure, self.invalid_partial = minute_failure, invalid_partial
        self.partial_range = partial_range
        self.partial_starts = []

    def discover_markets(self, **kwargs):
        return self.markets

    def fetch_reference_levels(self, symbol, *, as_of):
        if symbol in self.reference_failures:
            raise TimeoutError("fake reference fetch timed out")
        return {name: None if period is None else Levels(Decimal("100"), Decimal("90"))
                for name, period in reference_periods(as_of).items()}

    def fetch_daily_klines(self, symbol, *, start, end, as_of):
        rows = []
        while start < end:
            touches = [Decimal(price) for (touched_symbol, at), price in self.touches.items()
                       if touched_symbol == symbol and start <= at < start + DAY]
            rows.append(Candle(start, DAY, max([Decimal("99")] + touches),
                               min([Decimal("95")] + touches), Decimal("97"), True))
            start += DAY
        return rows

    def iter_minute_klines(self, symbol, *, start, end, as_of):
        if self.minute_failure:
            raise ValueError("historical minute unavailable")
        while start < end:
            price = Decimal(self.touches.get((symbol, start), "97"))
            high, low = max(Decimal("99"), price), min(Decimal("95"), price)
            yield SimpleNamespace(symbol=symbol, candle=Candle(start, MINUTE, high, low, price, True))
            start += MINUTE

    def _get_json(self, url):
        start = int(parse_qs(urlsplit(url).query)["startTime"][0])
        self.partial_starts.append(start)
        if self.invalid_partial:
            return []
        return [[start, "97", self.partial_range[0], self.partial_range[1], "97", "1", start + 59999]]

    def fetch_tickers(self):
        return [{"symbol": market.symbol, "quoteVolume": str(market.quote_volume)}
                for market in self.markets]


class RecordingEmail:
    def __init__(self, database, settings, **kwargs):
        self.database, self.settings, self.kwargs = database, settings, kwargs
        self.status = DeliveryStatus()
        self.notifications = 0

    def notify(self):
        self.notifications += 1

    async def run(self, stop):
        await stop.wait()


class TouchRuntimeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "runtime.sqlite3"

    def runtime(self, rest=None, *, email=EmailSettings()):
        runtime = TouchRuntime(Settings(database=self.path, no_tui=True, email=email),
                               POLICY, {}, threading.Event(), rest_factory=lambda: rest or FakeREST(),
                               now=lambda: START)
        self.addCleanup(runtime.close)
        return runtime

    def test_complete_history_seeds_earlier_touches_without_creating_emails(self):
        touched = START.replace(hour=0, minute=5)
        runtime = self.runtime(FakeREST(touches={("FILUSDT", touched): "90"}))
        runtime.prepare(START)
        snapshot = runtime.store.dashboard_snapshot(runtime.markets, START, DELAYS)
        lows = [row for row in snapshot["watches"] if row["side"] == "low"]
        self.assertEqual(len(lows), 4)
        self.assertTrue(all(row["status"] == "swept" and row["source"] == "history" for row in lows))
        self.assertTrue(all(row["accepted_at"] == touched + MINUTE for row in lows))
        self.assertEqual(snapshot["fresh"], ())
        self.assertEqual(snapshot["pending_email_count"], 0)
        self.assertTrue(all(row["status"] == "untouched" for row in snapshot["watches"]
                            if row["side"] == "high"))

    def test_partial_handoff_uses_the_supplied_clock_and_rejects_missing_coverage(self):
        rest = FakeREST(invalid_partial=True)
        runtime = self.runtime(rest)
        runtime.prepare(START)
        runtime.seed_partial_minutes(START + timedelta(seconds=12))
        expected = int((START - EPOCH).total_seconds() * 1000)
        self.assertEqual(rest.partial_starts, [expected])
        snapshot = runtime.store.dashboard_snapshot(runtime.markets, START, DELAYS)
        self.assertTrue(all(row["status"] == "unavailable" for row in snapshot["watches"]))
        self.assertEqual(runtime.process(tick("100", START + timedelta(seconds=13)), quiet=False,
                                        email_enabled=True, observed=START + timedelta(seconds=13)), [])

    def test_restart_discovers_offline_touch_and_mutes_previous_run_pending(self):
        rest = FakeREST()
        first = self.runtime(rest)
        first.prepare(START)
        first.process(tick("100", START + timedelta(seconds=1)), quiet=False,
                      email_enabled=True, observed=START + timedelta(seconds=1))
        self.assertEqual(len(first.store.load_touch_outbox(START + timedelta(seconds=2))), 4)
        first.close()
        offline_touch = START + timedelta(hours=1)
        rest.touches[("FILUSDT", offline_touch)] = "90"
        second = self.runtime(rest)
        second.prepare(START + DAY)
        snapshot = second.store.dashboard_snapshot(second.markets, START + DAY, DELAYS)
        lows = [row for row in snapshot["watches"] if row["side"] == "low"]
        self.assertTrue(all(row["source"] == "history" and row["status"] == "swept" for row in lows))
        self.assertEqual(snapshot["pending_email_count"], 0)
        self.assertEqual(second.store.load_touch_outbox(START + DAY), ())

    def test_unavailable_history_cannot_be_overwritten_by_quiet_quote_or_partial_range(self):
        runtime = self.runtime(FakeREST(minute_failure=True, partial_range=("100", "90")))
        runtime.prepare(START)
        before = runtime.store.dashboard_snapshot(runtime.markets, START, DELAYS)
        self.assertTrue(all(row["status"] == "unavailable" and row["count"] == 0
                            and row["accepted_at"] is None for row in before["watches"]))
        runtime.seed_partial_minutes(START)
        self.assertEqual(runtime.process(tick("100", START + timedelta(seconds=1)), quiet=True,
                                         email_enabled=True, observed=START + timedelta(seconds=1)), [])
        after = runtime.store.dashboard_snapshot(runtime.markets, START + timedelta(seconds=1), DELAYS)
        self.assertTrue(all(row["status"] == "unavailable" and row["count"] == 0
                            and row["accepted_at"] is None and row["source"] is None
                            for row in after["watches"]))
        self.assertEqual(after["fresh"], ())
        self.assertEqual(after["pending_email_count"], 0)
        self.assertEqual(runtime.store.load_touch_outbox(START + timedelta(seconds=1)), ())

    def test_new_month_resets_only_that_reference(self):
        before = START.replace(day=31, hour=0)
        runtime = self.runtime()
        runtime.prepare(before)
        accepted = runtime.process(tick("100", before + timedelta(seconds=1)), quiet=False,
                                   email_enabled=False, observed=before + timedelta(seconds=1))
        old_month = next(hit for hit in accepted if hit.level_name == "previous_month")
        after = before.replace(month=11, day=1)
        runtime.refresh(after, daily=True)
        current = runtime.process(tick("100", after + timedelta(seconds=1)), quiet=False,
                                  email_enabled=False, observed=after + timedelta(seconds=1))
        self.assertEqual([hit.level_name for hit in current], ["previous_month"])
        self.assertNotEqual(current[0].reference_period_start, old_month.reference_period_start)

    def test_monday_is_unavailable_for_24_hours_then_new_range_appears(self):
        monday = START.replace(day=19, hour=0)
        runtime = self.runtime()
        runtime.prepare(monday)
        snapshot = runtime.store.dashboard_snapshot(runtime.markets, monday, DELAYS)
        monday_rows = [row for row in snapshot["watches"] if row["name"] == "monday"]
        self.assertEqual(len(monday_rows), 2)
        self.assertTrue(all(row["status"] == "unavailable" for row in monday_rows))
        runtime.refresh(monday + DAY, daily=True)
        snapshot = runtime.store.dashboard_snapshot(runtime.markets, monday + DAY, DELAYS)
        self.assertTrue(all(row["status"] == "untouched" for row in snapshot["watches"]
                            if row["name"] == "monday"))

    def test_one_reference_failure_isolated_from_other_coin(self):
        runtime = self.runtime(FakeREST(symbols=("FILUSDT", "BTCUSDT"), reference_failures=("FILUSDT",)))
        with self.assertLogs("crypto_monitor.runtime.touch_service", level="INFO"):
            runtime.prepare(START)
        snapshot = runtime.store.dashboard_snapshot(runtime.markets, START, DELAYS)
        self.assertTrue(all(row["status"] == "unavailable" for row in snapshot["watches"]
                            if row["symbol"] == "FILUSDT"))
        self.assertTrue(all(row["status"] == "untouched" for row in snapshot["watches"]
                            if row["symbol"] == "BTCUSDT"))

    def test_database_failure_during_refresh_is_fatal(self):
        runtime = self.runtime()
        runtime.prepare(START)
        with patch.object(runtime.store, "dashboard_snapshot", side_effect=sqlite3.OperationalError("disk failed")):
            with self.assertRaises(sqlite3.OperationalError):
                runtime.refresh(START, daily=False)


class TouchServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "service.sqlite3"

    async def test_first_genuinely_new_live_touch_after_loading_notifies(self):
        clock, emails = [START], []
        rest = FakeREST(touches={("FILUSDT", START.replace(hour=0, minute=5)): "90"})

        class Supervisor:
            state, error = "loading", None

            def __init__(self, symbols, callback, on_connect):
                self.callback, self.on_connect = callback, on_connect

            async def run(self, stop):
                await self.on_connect()
                self.state = "connected"
                clock[0] += timedelta(seconds=1)
                await self.callback(tick("100", clock[0]))
                stop.set()

            def update_symbols(self, symbols):
                pass

        def email_factory(*args, **kwargs):
            sender = RecordingEmail(*args, **kwargs)
            emails.append(sender)
            return sender

        env = Path(self.directory.name) / "test.env"
        env.write_text("PWLH_delay=5\nPMLH_delay=5\nPQLH_delay=5\n"
                       "NOTIFICATION_MAX_AGE_SECONDS=30\nEMAIL_RETRY_SECONDS=3\nFRESH_HIGHLIGHT_SECONDS=60\n")
        policy = load_touch_settings(env)
        settings = Settings(database=self.path, no_tui=True, email=ENABLED_EMAIL, env_file=env)
        await asyncio.wait_for(run_touch_service(settings, policy=policy, rest_factory=lambda: rest,
            supervisor_factory=Supervisor, email_factory=email_factory, now=lambda: clock[0]), 5)
        self.assertEqual(emails[0].notifications, 1)
        self.assertEqual(emails[0].kwargs["retry_seconds"], Decimal("3"))
        with TouchStore.open_readonly(self.path) as store:
            snapshot = store.dashboard_snapshot(rest.markets, clock[0], DELAYS)
            self.assertEqual(len(snapshot["fresh"]), 4)
            self.assertEqual(snapshot["pending_email_count"], 4)
            self.assertTrue(all(row["source"] == "history" for row in snapshot["watches"]
                                if row["side"] == "low"))

    async def test_stop_while_selecting_coins_returns_cleanly_after_request_finishes(self):
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()

        class SlowREST(FakeREST):
            def discover_markets(self, **kwargs):
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(3):
                    raise TimeoutError("test release missing")
                return super().discover_markets(**kwargs)

        stop = asyncio.Event()
        factory = MagicMock(side_effect=AssertionError("must not start live after stop"))
        task = asyncio.create_task(run_touch_service(Settings(database=self.path, no_tui=True),
            policy=POLICY, stop=stop, rest_factory=SlowREST, supervisor_factory=factory, now=lambda: START))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            stop.set()
            await asyncio.sleep(0)
            release.set()
            await asyncio.wait_for(task, 3)
            factory.assert_not_called()
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_already_stopped_service_performs_no_startup_work(self):
        stop = asyncio.Event()
        stop.set()
        factory = MagicMock(side_effect=AssertionError("startup must stay stopped"))
        await run_touch_service(Settings(database=self.path, no_tui=True), policy=POLICY,
                                stop=stop, rest_factory=factory, now=lambda: START)
        factory.assert_not_called()
        self.assertFalse(self.path.exists())


if __name__ == "__main__":
    unittest.main()
