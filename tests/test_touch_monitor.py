"""Contact semantics, durable delays and fresh-only delivery use temporary files."""

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from crypto_monitor.core.level_sweep import Levels, reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.core.touch_monitor import (
    RULES_VERSION, candle_contacts, contact_possible, crosses, delay_duration,
)
from crypto_monitor.persistence.touch_store import TouchStore


D = Decimal
START = datetime(2026, 10, 13, 10, tzinfo=timezone.utc)
DELAYS = {"previous_week": D("5"), "previous_month": D("5"), "previous_quarter": D("5")}


def references(asof, *, only=None):
    return {name: ReferenceLevels(period, Levels(D("100"), D("90"))
                if only is None or name in only else None)
            for name, period in reference_periods(asof).items() if period is not None}


def ready(store, asof=START, *, only=None):
    store.initialize_watches("TESTUSDT", references(asof, only=only), asof)
    for watch in store.history_jobs("TESTUSDT"):
        if watch.level_price is not None:
            store.advance_history(watch, asof.replace(second=0, microsecond=0))
            store.mark_history_complete(watch.symbol, watch.name, watch.source_start,
                                        asof.replace(second=0, microsecond=0))


def move(store, previous, price, at, delays=DELAYS, **kwargs):
    """One trade following another; prices are strings for readability."""
    return store.record_price("TESTUSDT", D(price), at, delays,
                              previous=None if previous is None else D(previous), **kwargs)


class ContactRuleTests(unittest.TestCase):
    def test_trade_contacts_from_either_direction(self):
        level = D("100")
        for previous, current, expected in (
            ("99", "100", True), ("101", "100", True),     # arrive from below / above
            ("99", "101", True), ("101", "99", True),      # cross without equality
            ("101", "102", False), ("99", "98", False),    # stay beyond either side
            ("100", "100", False), ("100", "101", False),  # equality run, departure
            (None, "100", False),                          # unknown prior price
        ):
            with self.subTest(previous=previous, current=current):
                prior = None if previous is None else D(previous)
                self.assertIs(crosses(prior, D(current), level), expected)

    def test_range_proof_and_conservative_candles(self):
        level = D("100")
        self.assertFalse(contact_possible(D("101"), D("101"), D("105"), level))
        self.assertTrue(contact_possible(D("99"), D("101"), D("105"), level))  # gap up
        self.assertFalse(contact_possible(D("100"), D("100"), D("100"), level))
        self.assertTrue(contact_possible(None, D("99"), D("100"), level))
        # A whole candle above the level is no contact, unless it gapped across.
        self.assertFalse(candle_contacts(D("102"), D("101"), D("101"), D("103"), D("102"), level))
        self.assertTrue(candle_contacts(D("99"), D("101"), D("101"), D("103"), D("102"), level))
        # During an equality run, only proven departures and returns count.
        self.assertFalse(candle_contacts(D("100"), D("100"), D("100"), D("102"), D("101"), level))
        self.assertTrue(candle_contacts(D("100"), D("101"), D("100"), D("102"), D("100"), level))
        self.assertTrue(candle_contacts(D("100"), D("99"), D("98"), D("102"), D("101"), level))
        # Unknown history falls back to the candle's open.
        self.assertFalse(candle_contacts(None, D("100"), D("100"), D("101"), D("101"), level))
        self.assertTrue(candle_contacts(None, D("101"), D("99"), D("101"), D("99"), level))


class TouchMonitorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "touch.sqlite3"
        self.store = TouchStore(self.path)
        self.addCleanup(self.store.close)

    def test_equality_is_immediate_and_high_low_remain_independent(self):
        ready(self.store)
        highs = move(self.store, "99", "100", START, notify=True)
        self.assertEqual({hit.side for hit in highs}, {"high"})
        self.assertEqual({hit.level_name for hit in highs}, set(reference_periods(START)))
        self.assertEqual({(hit.previous_price, hit.trigger_price) for hit in highs}, {(D("99"), D("100"))})
        lows = move(self.store, "91", "90", START + timedelta(seconds=1))
        self.assertEqual(len(lows), 4)
        self.assertEqual({hit.side for hit in lows}, {"low"})
        self.assertEqual(len(self.store.load_touch_outbox(START + timedelta(seconds=1))), 4)

    def test_staying_beyond_or_at_level_after_expiry_is_not_a_new_contact(self):
        ready(self.store, only={"previous_month"})
        self.assertEqual(len(move(self.store, "99", "101", START)), 1)
        later = START + timedelta(days=5)
        self.assertEqual(move(self.store, "101", "102", later), [])
        self.assertEqual(move(self.store, "100", "100", later), [])
        self.assertEqual(move(self.store, "100", "101", later), [])
        self.assertEqual(len(self.store.load_touch_outbox(later)), 0)
        # Returning from above, or crossing down without equality, is contact.
        again = move(self.store, "102", "99", later + timedelta(seconds=1))
        self.assertEqual([hit.side for hit in again], ["high"])
        monthly = [w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high"][0]
        self.assertEqual(monthly.accepted_count, 2)

    def test_crossings_count_from_either_side_of_each_level(self):
        ready(self.store, only={"previous_month"})
        self.assertEqual([hit.side for hit in move(self.store, "91", "89", START)], ["low"])
        later = START + timedelta(days=5)
        self.assertEqual([hit.side for hit in move(self.store, "89", "91", later)], ["low"])
        self.assertEqual([hit.side for hit in move(self.store, "101", "100", later)], ["high"])

    def test_ignored_contact_during_delay_does_not_extend_it_or_alert_later(self):
        ready(self.store, only={"previous_month"})
        move(self.store, "99", "100", START)
        self.assertEqual(move(self.store, "101", "99", START + timedelta(days=4), notify=True), [])
        expired = START + timedelta(days=5)
        self.assertEqual(move(self.store, "99", "98", expired, notify=True), [])
        self.assertEqual(self.store.load_touch_outbox(expired), ())
        self.assertEqual(len(move(self.store, "98", "100", expired)), 1)

    def test_fractional_days_and_invalid_delays(self):
        ready(self.store, only={"previous_month"})
        delays = {name: D("0.1") for name in DELAYS}
        self.assertEqual(delay_duration(D("0.1")), timedelta(hours=2, minutes=24))
        move(self.store, "99", "100", START, delays)
        self.assertEqual(move(self.store, "99", "100", START + timedelta(hours=2), delays), [])
        self.assertEqual(len(move(self.store, "99", "100", START + timedelta(hours=2, minutes=24), delays)), 1)
        for invalid in (D(0), D(-1), D("NaN"), D("Infinity")):
            with self.assertRaises(ValueError):
                delay_duration(invalid)

    def test_restart_keeps_activation_time_not_restart_time(self):
        ready(self.store, only={"previous_month"})
        move(self.store, "99", "100", START)
        with TouchStore(self.path) as restarted:
            ready(restarted, START + timedelta(days=1), only={"previous_month"})
            self.assertEqual(move(restarted, "99", "100", START + timedelta(days=4)), [])
            self.assertEqual(len(move(restarted, "99", "100", START + timedelta(days=5))), 1)

    def test_new_month_resets_even_identical_numeric_level(self):
        before = START.replace(day=31)
        ready(self.store, before, only={"previous_month"})
        move(self.store, "99", "100", before)
        after = before.replace(month=11, day=1)
        ready(self.store, after, only={"previous_month"})
        self.assertEqual(len(move(self.store, "99", "100", after)), 1)
        state = next(w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
        self.assertEqual(state.accepted_count, 1)

    def test_monday_first_only_then_undefined_for_next_monday(self):
        ready(self.store, only={"monday"})
        self.assertEqual(len(move(self.store, "99", "100", START)), 1)
        short = {name: D("0.1") for name in DELAYS}
        self.assertEqual(move(self.store, "99", "100", START + timedelta(days=3), short), [])
        monday = START.replace(day=19, hour=0)
        self.store.initialize_watches("TESTUSDT", references(monday), monday)
        states = self.store.dashboard_snapshot(["TESTUSDT"], monday, DELAYS)["watches"]
        monday_rows = [row for row in states if row["name"] == "monday"]
        self.assertEqual(len(monday_rows), 2)
        self.assertTrue(all(row["status"] == "unavailable" for row in monday_rows))
        tuesday = monday + timedelta(days=1)
        ready(self.store, tuesday, only={"monday"})
        hits = move(self.store, "99", "100", tuesday)
        self.assertEqual(len([hit for hit in hits if hit.level_name == "monday"]), 1)

    def test_month_count_survives_expiry_while_quarter_badge_and_old_time_disappear(self):
        ready(self.store, only={"previous_month", "previous_quarter"})
        move(self.store, "99", "100", START, origin="history")
        state = self.store.dashboard_snapshot(["TESTUSDT"], START + timedelta(days=6), DELAYS)
        month = next(row for row in state["watches"] if row["name"] == "previous_month" and row["side"] == "high")
        quarter = next(row for row in state["watches"] if row["name"] == "previous_quarter" and row["side"] == "high")
        self.assertEqual((month["status"], month["count"]), ("untouched", 1))
        self.assertEqual((quarter["status"], quarter["count"], quarter["accepted_at"]), ("untouched", 0, None))
        self.assertEqual(state["fresh"], ())

    def test_history_and_disabled_email_do_not_stack_and_prior_run_pending_is_muted(self):
        ready(self.store, only={"previous_month", "previous_quarter"})
        move(self.store, "99", "100", START, origin="history", notify=True)
        self.assertEqual(self.store.load_touch_outbox(START), ())
        move(self.store, "91", "90", START, notify=False)
        self.assertEqual(self.store.load_touch_outbox(START), ())
        move(self.store, "91", "90", START + timedelta(days=5), notify=True)
        self.assertEqual(len(self.store.load_touch_outbox(START + timedelta(days=5))), 2)
        self.store.begin_run(START + timedelta(days=5, seconds=1))
        self.assertEqual(self.store.load_touch_outbox(START + timedelta(days=5, seconds=1)), ())

    def test_unverified_watch_does_not_create_live_activation(self):
        self.store.initialize_watches("TESTUSDT", references(START), START)
        self.assertEqual(move(self.store, "99", "100", START, notify=True), [])
        self.assertEqual(self.store.load_touch_outbox(START), ())

    def test_disabling_email_mutes_pending_but_preserves_sent_notifications(self):
        ready(self.store, only={"previous_month", "previous_quarter"})
        move(self.store, "99", "100", START, notify=True)
        entries = self.store.load_touch_outbox(START)
        self.assertEqual(len(entries), 2)
        self.store.mark_touch_outbox_sent(entries[0].activation.activation_id,
                                         sent_at=START, message_id="accepted")
        self.store.mute_pending_touch_outbox(START + timedelta(seconds=1))
        self.assertEqual(self.store.load_touch_outbox(START + timedelta(seconds=2)), ())
        rows = self.store._connection.execute(
            "SELECT sent_at,message_id,muted_reason FROM touch_email_outbox ORDER BY activation_id"
        ).fetchall()
        self.assertEqual(sum(row[1] == "accepted" and row[2] is None for row in rows), 1)
        self.assertEqual(sum(row[0] is None and row[2] == "email disabled" for row in rows), 1)

    def test_outbox_claim_is_atomic_between_senders(self):
        ready(self.store, only={"previous_month"})
        move(self.store, "99", "100", START, notify=True)
        entry, = self.store.load_touch_outbox(START)
        identity, lease = entry.activation.activation_id, START + timedelta(seconds=60)
        with TouchStore(self.path) as other:
            self.assertTrue(self.store.begin_touch_outbox_attempt(
                identity, START, expected_attempts=entry.attempt_count, lease_until=lease))
            # A second sender that loaded the same row cannot claim it, and
            # the lease hides the in-progress row from new loads.
            self.assertFalse(other.begin_touch_outbox_attempt(
                identity, START, expected_attempts=entry.attempt_count, lease_until=lease))
            self.assertEqual(other.load_touch_outbox(START + timedelta(seconds=1)), ())
            self.store.mute_pending_touch_outbox(START)
            self.assertFalse(other.begin_touch_outbox_attempt(identity, START))

    def test_clock_rollback_mutes_events_dated_ahead_of_the_clock(self):
        ready(self.store, only={"previous_month"})
        move(self.store, "99", "100", START + timedelta(hours=1), notify=True)
        self.store.expire_touch_outbox(START - timedelta(seconds=60), after=START + timedelta(seconds=60))
        self.assertEqual(self.store.load_touch_outbox(START + timedelta(hours=1)), ())


class ContactMigrationTests(unittest.TestCase):
    def test_threshold_era_state_is_rebuilt_while_old_records_remain(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            with TouchStore(path) as store:
                ready(store, only={"previous_month"})
                move(store, "99", "100", START - timedelta(minutes=1), notify=True)
                connection = store._connection
                with connection:
                    # Recreate a version 1 database: threshold counts, no new columns.
                    connection.execute("UPDATE touch_watches SET accepted_count=13, rules_version=1")
                    connection.execute("UPDATE touch_activations SET rules_version=1")
                    for column in ("cursor_price", "rules_version"):
                        connection.execute(f"ALTER TABLE touch_watches DROP COLUMN {column}")
                    for column in ("rules_version", "previous_price", "trigger_price", "trade_id", "run_id"):
                        connection.execute(f"ALTER TABLE touch_activations DROP COLUMN {column}")
            with TouchStore(path) as migrated:
                watches = migrated.history_jobs("TESTUSDT")
                self.assertTrue(all(w.accepted_count == 0 and w.accepted_at is None
                                    and w.history_cursor == w.watch_start and not w.available
                                    and w.cursor_price is None for w in watches))
                versions = migrated._connection.execute(
                    "SELECT DISTINCT rules_version FROM touch_watches").fetchall()
                self.assertEqual(versions, [(RULES_VERSION,)])
                # Audit rows survive; the old live event is no longer fresh.
                self.assertEqual(migrated._connection.execute(
                    "SELECT COUNT(*) FROM touch_activations WHERE rules_version=1").fetchone()[0], 1)
                self.assertEqual(migrated.dashboard_snapshot(["TESTUSDT"], START, DELAYS)["fresh"], ())
                ready(migrated, only={"previous_month"})
                self.assertEqual(len(move(migrated, "99", "100", START)), 1)
            with TouchStore(path) as reopened:
                self.assertEqual(sum(w.accepted_count for w in reopened.history_jobs("TESTUSDT")), 1)


if __name__ == "__main__":
    unittest.main()
