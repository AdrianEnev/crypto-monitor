"""Offline persistence and restart tests using real temporary SQLite files."""

import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from crypto_monitor.adapters.binance_feed import ClosedKline, MinuteCandle
from crypto_monitor.adapters.binance_rest import DiscoveredMarket
from crypto_monitor.core.level_sweep import MINUTE, Levels, reference_periods
from crypto_monitor.core.monitoring_engine import CandleBaseline, MonitoringEngine, ReferenceLevels
from crypto_monitor.persistence.sqlite_store import SQLiteStore


D = Decimal
START = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
MARKETS = [DiscoveredMarket(symbol, symbol[:-4], D("1234567890.12345678901234567890"),
                           D("0.01")) for symbol in ("BTCUSDT", "ETHUSDT")]
REFERENCES = {
    name: ReferenceLevels(period, Levels(D("100.00"), D("90.00")))
    for name, period in reference_periods(START).items()
}


def event(minute=0, symbol="BTCUSDT", close="99"):
    opened = START + minute * MINUTE
    return ClosedKline(
        symbol, MinuteCandle(opened, MINUTE, D("101"), D("98"), D(close), True, D(close)),
        opened + MINUTE, opened + MINUTE - timedelta(milliseconds=1),
    )


def hits_for(current):
    engine = MonitoringEngine(MARKETS)
    previous = replace(current, candle=replace(current.candle,
                       open_time=current.candle.open_time - MINUTE),
                       event_time=current.event_time - MINUTE,
                       close_time=current.close_time - MINUTE)
    engine.process(previous, REFERENCES)
    return engine.process(current, REFERENCES)


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "monitor.sqlite3"
        self.store = SQLiteStore(self.path)
        self.addCleanup(self.store.close)

    def rows(self, query):
        # Independent reader also verifies that writes were committed.
        connection = sqlite3.connect(self.path)
        try:
            return connection.execute(query).fetchall()
        finally:
            connection.close()

    def test_reopen_preserves_all_hits_and_exact_decimal_strings(self):
        current = event(close="99.123456789012345678901234567890")
        hits = hits_for(current)
        self.assertEqual(len(hits), 4)
        self.assertTrue(self.store.commit_candle(current, hits))
        self.store.close()
        with SQLiteStore(self.path) as reopened:
            self.assertEqual(reopened.load_hits(), sorted(hits, key=lambda hit: hit.alert_id))
            baseline = reopened.load_baselines()["BTCUSDT"]
            self.assertEqual(baseline.open_time, START)
            self.assertEqual(baseline.open_time.tzinfo, timezone.utc)
            self.assertEqual(str(baseline.close), str(current.candle.close))
        self.assertEqual(self.rows("SELECT close_price, typeof(close_price) FROM processed_candles"),
                         [(str(current.candle.close), "text")])
        stored = self.rows("SELECT level_price, captured_quote_volume, "
                           "typeof(level_price), typeof(captured_quote_volume) FROM sweep_hits")
        self.assertEqual(stored, [("100.00", str(MARKETS[0].quote_volume), "text", "text")] * 4)

    def test_no_hit_candle_is_persisted(self):
        self.assertTrue(self.store.commit_candle(event(), []))
        with SQLiteStore(self.path) as reopened:
            self.assertEqual(reopened.load_baselines(), {"BTCUSDT": CandleBaseline(START, D("99"))})
            self.assertEqual(reopened.load_hits(), [])

    def test_duplicate_candle_is_noop_and_first_commit_wins(self):
        current = event()
        hits = hits_for(current)
        self.store.commit_candle(current, hits)
        self.assertFalse(self.store.commit_candle(event(close="98"), hits))
        self.assertFalse(self.store.commit_candle(current, []))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM processed_candles"), [(1,)])
        self.assertEqual(len(self.store.load_hits()), 4)
        self.assertEqual(self.store.load_baselines()["BTCUSDT"].close, D("99"))

    def test_duplicate_no_hit_commit_cannot_append_late_hits(self):
        self.store.commit_candle(event(), [])
        self.assertFalse(self.store.commit_candle(event(), hits_for(event())))
        self.assertEqual(self.store.load_hits(), [])

    def test_duplicate_alert_ids_are_stored_once(self):
        hits = hits_for(event())
        self.store.commit_candle(event(), hits + hits + [replace(hits[0], level_price=D("101"))])
        self.assertEqual(self.store.load_hits(), sorted(hits, key=lambda hit: hit.alert_id))

    def test_named_level_suppression_survives_reopen_and_advances_candles(self):
        first = hits_for(event())
        self.store.commit_candle(event(), first)
        with SQLiteStore(self.path) as reopened:
            # Changed prices/volume do not rearm a previously claimed level.
            repeats = [replace(hit, level_price=D("100.01"), captured_quote_volume=D("999"))
                       for hit in hits_for(event(1))]
            self.assertTrue(reopened.commit_candle(event(1), repeats))
            self.assertEqual(reopened.load_candle_alerts("BTCUSDT", event(1).candle.open_time), [])
            self.assertEqual(reopened.load_baselines()["BTCUSDT"].open_time, START + MINUTE)
            self.assertEqual(set(reopened.load_hits()), set(first))
            self.assertEqual(len(reopened.load_outbox()), 4)
            self.assertEqual(len(reopened.dashboard_data().alerts), 4)

    def test_high_low_symbols_and_named_levels_have_independent_claims(self):
        # All four named highs at the same price each deserve one alert.
        self.store.commit_candle(event(), hits_for(event()))
        other = event(symbol="ETHUSDT")
        self.store.commit_candle(other, hits_for(other))
        current = event(1)
        lows = [replace(hit, side="low", level_price=D("90"), alert_id=f"low:{hit.alert_id}")
                for hit in hits_for(current)]
        self.store.commit_candle(current, lows)
        self.assertEqual(len(self.store.load_hits()), 12)
        self.assertEqual(len(self.store.load_outbox()), 12)

    def test_failed_outbox_write_does_not_consume_level_claim(self):
        self.store._connection.execute("""
            CREATE TRIGGER fail_outbox BEFORE INSERT ON email_outbox
            BEGIN SELECT RAISE(ABORT, 'test failure'); END
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.commit_candle(event(), hits_for(event()))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM alert_claims"), [(0,)])
        self.store._connection.execute("DROP TRIGGER fail_outbox")
        self.store.commit_candle(event(), hits_for(event()))
        self.assertEqual(len(self.store.load_outbox()), 4)

    def legacy_repeats(self, *, sent=False, unqueued=False):
        """Build the old schema's repeated hits without the new claim table."""
        self.store.commit_candle(event(), hits_for(event()))
        self.store.close()
        with sqlite3.connect(self.path) as db:
            db.execute("DROP TABLE alert_claims")
            for minute in (1, 2):
                current = event(minute)
                db.execute("INSERT INTO processed_candles VALUES (?, ?, ?)",
                           (current.symbol, current.candle.open_time.isoformat(timespec="microseconds"), "99"))
                for hit in hits_for(current):
                    db.execute("INSERT INTO sweep_hits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (hit.alert_id, hit.symbol, hit.candle_time.isoformat(timespec="microseconds"),
                                hit.level_name, hit.side, hit.reference_period_start.isoformat(timespec="microseconds"),
                                str(hit.level_price), str(hit.captured_quote_volume),
                                hit.detected_at.isoformat(timespec="microseconds")))
                    db.execute("INSERT INTO email_outbox (alert_id) VALUES (?)", (hit.alert_id,))
            if sent:
                for hit in hits_for(event(1)):
                    db.execute("""UPDATE email_outbox SET sent_at = ?, message_id = 'already-sent',
                               attempt_count = 1 WHERE alert_id = ?""", (START.isoformat(), hit.alert_id))
            if unqueued:
                db.execute("DROP TABLE email_outbox")
        db.close()

    def test_legacy_pending_repeats_migrate_once_without_erasing_history(self):
        self.legacy_repeats()
        for _ in range(2):
            with SQLiteStore(self.path) as migrated:
                self.assertEqual(len(migrated.load_hits()), 12)
                self.assertEqual(len(migrated.load_outbox()), 4)
                self.assertEqual({row.hit.candle_time for row in migrated.load_outbox()}, {START})
                self.assertEqual(len(migrated.dashboard_data().alerts), 4)
                self.assertEqual(migrated.dashboard_data().pending_email_count, 4)
                migrated.commit_candle(event(3), hits_for(event(3)))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM email_outbox"), [(12,)])

    def test_legacy_sent_repeat_prevents_older_pending_alerts_from_sending(self):
        self.legacy_repeats(sent=True)
        with SQLiteStore(self.path) as migrated:
            self.assertEqual(migrated.load_outbox(), [])
            self.assertEqual(len(migrated.load_outbox(include_sent=True)), 4)
            self.assertEqual({row.email_status for row in migrated.dashboard_data().alerts}, {"sent"})
            self.assertEqual(migrated.dashboard_data().pending_email_count, 0)
            self.assertEqual(len(migrated.load_hits()), 12)

    def test_legacy_unqueued_hits_claim_levels_without_retroactive_email(self):
        self.legacy_repeats(unqueued=True)
        with SQLiteStore(self.path) as migrated:
            self.assertEqual(migrated.load_outbox(), [])
            self.assertEqual({row.email_status for row in migrated.dashboard_data().alerts}, {"unqueued"})
            migrated.commit_candle(event(3), hits_for(event(3)))
            self.assertEqual(migrated.load_outbox(), [])
            self.assertEqual(len(migrated.load_hits()), 12)

    def test_alert_id_collision_with_another_candle_rolls_back(self):
        first = hits_for(event())[0]
        self.store.commit_candle(event(), [first])
        later = event(1)
        collision = replace(hits_for(later)[0], alert_id=first.alert_id)
        with self.assertRaisesRegex(ValueError, "another candle"):
            self.store.commit_candle(later, [collision])
        self.assertEqual(self.store.load_hits(), [first])
        self.assertEqual(self.store.load_baselines()["BTCUSDT"].open_time, START)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM processed_candles"), [(1,)])

    def test_latest_per_symbol_uses_open_time_and_keeps_out_of_order_history(self):
        for minute, symbol, close in [(9, "BTCUSDT", "100"), (3, "ETHUSDT", "99"),
                                      (2, "BTCUSDT", "98"), (1, "ETHUSDT", "98"),
                                      (10, "BTCUSDT", "99")]:
            self.store.commit_candle(event(minute, symbol, close), [])
        expected = {"BTCUSDT": CandleBaseline(START + 10 * MINUTE, D("99")),
                    "ETHUSDT": CandleBaseline(START + 3 * MINUTE, D("99"))}
        self.assertEqual(self.store.load_baselines(), expected)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM processed_candles"), [(5,)])
        self.store.close()
        with SQLiteStore(self.path) as reopened:
            self.assertEqual(reopened.load_baselines(), expected)

    def test_invalid_later_hit_rolls_back_candle_and_prior_valid_hit(self):
        self.store.commit_candle(event(), [])
        current = event(1)
        hits = hits_for(current)
        invalid_changes = [
            {"side": "invalid"}, {"level_price": D("NaN")}, {"level_price": 100.0},
            {"symbol": "ETHUSDT"}, {"candle_time": START}, {"level_name": ""},
            {"reference_period_start": START.replace(tzinfo=None)},
            {"reference_period_start": START},
        ]
        for changes in invalid_changes:
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    self.store.commit_candle(current, [hits[0], replace(hits[1], **changes)])
                self.assertEqual(self.store.load_hits(), [])
                self.assertEqual(self.store.load_baselines()["BTCUSDT"].open_time, START)
                self.assertEqual(self.rows("SELECT COUNT(*) FROM processed_candles"), [(1,)])
        # Failed attempts did not claim the candle key; corrected input can commit.
        self.assertTrue(self.store.commit_candle(current, hits))
        self.assertEqual(len(self.store.load_hits()), 4)

    def test_database_error_rolls_back_whole_transaction(self):
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("""CREATE TRIGGER reject_hit BEFORE INSERT ON sweep_hits
                                  BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END""")
        finally:
            connection.close()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "simulated write failure"):
            self.store.commit_candle(event(), hits_for(event()))
        self.assertEqual(self.store.load_baselines(), {})
        self.assertEqual(self.store.load_hits(), [])

    def test_failing_hit_iterator_rolls_back(self):
        def broken_hits():
            yield hits_for(event())[0]
            raise RuntimeError("broken input")

        with self.assertRaisesRegex(RuntimeError, "broken input"):
            self.store.commit_candle(event(), broken_hits())
        self.assertEqual(self.store.load_baselines(), {})
        self.assertEqual(self.store.load_hits(), [])

    def test_unclosed_nonminute_and_unaligned_candles_are_rejected(self):
        for changes in ({"closed": False}, {"interval": 2 * MINUTE},
                        {"open_time": START + timedelta(seconds=1)}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.store.commit_candle(replace(event(), candle=replace(event().candle, **changes)), [])
        self.assertEqual(self.store.load_baselines(), {})

    def test_values_are_bound_as_parameters(self):
        symbol = "coin'); DROP TABLE processed_candles; --"
        self.store.commit_candle(event(symbol=symbol), [])
        self.assertEqual(self.store.load_baselines(), {symbol: CandleBaseline(START, D("99"))})

    def test_restart_restores_consecutive_sweep_but_not_across_gap(self):
        self.store.commit_candle(event(), [])
        self.store.close()
        for next_minute in (1, 3):
            with self.subTest(next_minute=next_minute), SQLiteStore(self.path) as reopened:
                engine = MonitoringEngine(MARKETS)
                for symbol, baseline in reopened.load_baselines().items():
                    engine.restore_baseline(symbol, baseline)
                hits = engine.process(event(next_minute), REFERENCES)
                self.assertEqual(len(hits), 4 if next_minute == 1 else 0)

    def test_restore_does_not_replace_newer_or_equal_engine_baseline(self):
        engine = MonitoringEngine(MARKETS)
        engine.process(event(5, close="101"), REFERENCES)
        for minute in (3, 5):
            engine.restore_baseline("BTCUSDT", CandleBaseline(START + minute * MINUTE, D("99")))
        # Prior close 101 is above the level, so this cannot be a high sweep.
        self.assertEqual(engine.process(event(6), REFERENCES), [])

    def test_baseline_validation_and_unselected_symbols(self):
        for opened, close in [(START.replace(tzinfo=None), D("99")),
                              (START + timedelta(seconds=1), D("99")),
                              (START, D("NaN")), (START, 99.0)]:
            with self.subTest(opened=opened, close=close), self.assertRaises((ValueError, TypeError)):
                CandleBaseline(opened, close)
        with self.assertRaisesRegex(ValueError, "unselected symbol"):
            MonitoringEngine(MARKETS).restore_baseline("UNKNOWN", CandleBaseline(START, D("99")))


if __name__ == "__main__":
    unittest.main()
