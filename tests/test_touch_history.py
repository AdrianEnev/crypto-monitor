"""History discovery is quiet, resumable and independent per reference side."""

import tempfile
import unittest
import sqlite3
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from crypto_monitor.adapters.binance_feed import ClosedKline, MinuteCandle
from crypto_monitor.core.level_sweep import Candle, DAY, MINUTE
from crypto_monitor.persistence.touch_store import TouchStore
from crypto_monitor.runtime.touch_history import HistoryScanCancelled, TouchHistoryScanner
from tests.test_touch_monitor import D, START, DELAYS, references


MONTH_START = START.replace(day=1, hour=0)


def event(opened, *, low="95", high="101"):
    return ClosedKline("TESTUSDT", MinuteCandle(opened, MINUTE, D(high), D(low),
        D("96"), True, D("96")), opened + MINUTE, opened + MINUTE - timedelta(milliseconds=1))


class FakeREST:
    def __init__(self, *, daily_low="95", daily_high="99", minute_low="95", minute_high="101", fail_at=None):
        self.daily_low, self.daily_high = D(daily_low), D(daily_high)
        self.minute_low, self.minute_high = minute_low, minute_high
        self.fail_at = fail_at
        self.daily_calls, self.minute_calls = [], []

    def fetch_daily_klines(self, symbol, *, start, end, as_of):
        self.daily_calls.append((start, end))
        result = []
        while start < end:
            result.append(Candle(start, DAY, self.daily_high, self.daily_low, D("96"), True))
            start += DAY
        return result

    def iter_minute_klines(self, symbol, *, start, end, as_of):
        self.minute_calls.append((start, end))
        while start < end:
            if self.fail_at is not None and start >= self.fail_at:
                raise RuntimeError("missing candle")
            yield event(start, low=self.minute_low, high=self.minute_high)
            start += MINUTE


class TouchHistoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "history.sqlite3"
        self.store = TouchStore(self.path)
        self.addCleanup(self.store.close)

    def initialize(self, asof, names=None):
        self.store.initialize_watches("TESTUSDT", references(asof, only=names or {"previous_month"}), asof)

    def test_historical_touch_is_quiet_has_containing_minute_end_and_no_email(self):
        asof = MONTH_START + 5 * MINUTE
        self.initialize(asof)
        result = TouchHistoryScanner(self.store, FakeREST(), DELAYS).scan("TESTUSDT", asof)
        self.assertIn(("previous_month", "high"), result.verified)
        high = next(w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
        self.assertEqual(high.accepted_at, MONTH_START + MINUTE)
        self.assertEqual(high.accepted_count, 1)
        self.assertTrue(high.approximate)
        snapshot = self.store.dashboard_snapshot(["TESTUSDT"], asof, DELAYS)
        self.assertEqual(snapshot["fresh"], ())
        self.assertEqual(self.store.load_touch_outbox(asof), ())

    def test_restart_fetches_only_after_durable_history_cursor(self):
        first = MONTH_START + 5 * MINUTE
        self.initialize(first)
        rest = FakeREST()
        TouchHistoryScanner(self.store, rest, DELAYS).scan("TESTUSDT", first)
        second = first + 5 * MINUTE
        with TouchStore(self.path) as reopened:
            reopened.initialize_watches("TESTUSDT", references(second, only={"previous_month"}), second)
            resumed = FakeREST()
            TouchHistoryScanner(reopened, resumed, DELAYS).scan("TESTUSDT", second)
            # High is within its delay and needs no minute fetch. Low resumes
            # at the prior cursor, rather than downloading the whole month.
            self.assertEqual(resumed.minute_calls, [(first, second)])
            high = next(w for w in reopened.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
            self.assertEqual(high.accepted_count, 1)

    def test_accepted_history_counts_respect_delay_not_every_cross(self):
        asof = MONTH_START + 5 * MINUTE
        self.initialize(asof)
        delays = {name: D("0.001") for name in DELAYS}  # 86.4 seconds
        TouchHistoryScanner(self.store, FakeREST(), delays).scan("TESTUSDT", asof)
        high = next(w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
        self.assertEqual(high.accepted_count, 3)
        self.assertEqual(high.accepted_at, MONTH_START + 5 * MINUTE)

    def test_complete_daily_no_touch_proof_skips_all_minute_requests(self):
        asof = MONTH_START + 2 * DAY
        self.initialize(asof)
        rest = FakeREST()
        result = TouchHistoryScanner(self.store, rest, DELAYS).scan("TESTUSDT", asof)
        self.assertEqual(rest.minute_calls, [])
        self.assertEqual(result.activation_count, 0)
        self.assertEqual(set(result.verified), {("previous_month", "high"), ("previous_month", "low")})

    def test_missing_minute_history_disables_only_side_that_needs_it(self):
        asof = MONTH_START + DAY
        self.initialize(asof)
        rest = FakeREST(daily_low="89", daily_high="99", fail_at=MONTH_START)
        result = TouchHistoryScanner(self.store, rest, DELAYS).scan("TESTUSDT", asof)
        self.assertIn(("previous_month", "high"), result.verified)
        unavailable = {(name, side) for name, side, _ in result.unavailable}
        self.assertIn(("previous_month", "low"), unavailable)
        self.assertNotIn(("previous_month", "high"), unavailable)
        self.assertEqual(self.store.record_price("TESTUSDT", D("89"), asof, DELAYS, notify=True), [])
        self.assertEqual(len(self.store.record_price("TESTUSDT", D("100"), asof, DELAYS)), 1)

    def test_daily_endpoint_failure_falls_back_to_validated_minutes(self):
        asof = MONTH_START + DAY
        self.initialize(asof)

        class NoDaily(FakeREST):
            def fetch_daily_klines(self, *args, **kwargs):
                raise RuntimeError("daily endpoint unavailable")

        rest = NoDaily(minute_high="99")
        result = TouchHistoryScanner(self.store, rest, DELAYS).scan("TESTUSDT", asof)
        self.assertIn(("previous_month", "high"), result.verified)
        self.assertIn(("previous_month", "low"), result.verified)

    def test_daily_cooldown_avoids_replaying_every_minute_beyond_level(self):
        asof = MONTH_START + 3 * DAY
        self.initialize(asof)
        rest = FakeREST(daily_high="101")
        result = TouchHistoryScanner(self.store, rest, DELAYS).scan("TESTUSDT", asof)
        self.assertEqual(result.activation_count, 1)
        self.assertEqual(len(rest.minute_calls), 1)
        high = next(w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
        self.assertEqual(high.history_cursor, asof)

    def test_duplicate_or_skipped_injected_minutes_are_not_declared_untouched(self):
        asof = MONTH_START + 3 * MINUTE
        self.initialize(asof)

        class BadMinutes(FakeREST):
            def iter_minute_klines(self, *args, **kwargs):
                yield event(MONTH_START + MINUTE)

        result = TouchHistoryScanner(self.store, BadMinutes(), DELAYS).scan("TESTUSDT", asof)
        self.assertNotIn(("previous_month", "high"), result.verified)
        self.assertNotIn(("previous_month", "low"), result.verified)

    def test_shutdown_stops_before_history_network_and_keeps_durable_cursor(self):
        asof = MONTH_START + 3 * MINUTE
        self.initialize(asof)
        rest = FakeREST()
        with self.assertRaises(HistoryScanCancelled):
            TouchHistoryScanner(self.store, rest, DELAYS, cancelled=lambda: True).scan("TESTUSDT", asof)
        self.assertEqual(rest.daily_calls, [])
        self.assertEqual(rest.minute_calls, [])
        high = next(w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
        self.assertEqual(high.history_cursor, MONTH_START)

    def test_sqlite_write_failure_propagates_instead_of_hiding_as_missing_market_history(self):
        asof = MONTH_START + 3 * MINUTE
        self.initialize(asof)
        with patch.object(self.store, "replay_history_candles", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaisesRegex(sqlite3.OperationalError, "disk full"):
                TouchHistoryScanner(self.store, FakeREST(), DELAYS).scan("TESTUSDT", asof)


if __name__ == "__main__":
    unittest.main()
