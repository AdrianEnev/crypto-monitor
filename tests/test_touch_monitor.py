"""Touch semantics, durable delays and fresh-only delivery use temporary files."""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from crypto_monitor.core.level_sweep import Levels, reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.core.touch_monitor import delay_duration
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


class TouchMonitorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "touch.sqlite3"
        self.store = TouchStore(self.path)
        self.addCleanup(self.store.close)

    def test_equality_is_immediate_and_high_low_remain_independent(self):
        ready(self.store)
        highs = self.store.record_price("TESTUSDT", D("100"), START, DELAYS, notify=True)
        self.assertEqual({hit.side for hit in highs}, {"high"})
        self.assertEqual({hit.level_name for hit in highs}, set(reference_periods(START)))
        lows = self.store.record_price("TESTUSDT", D("90"), START + timedelta(seconds=1), DELAYS)
        self.assertEqual(len(lows), 4)
        self.assertEqual({hit.side for hit in lows}, {"low"})
        self.assertEqual(len(self.store.load_touch_outbox(START + timedelta(seconds=1))), 4)

    def test_ignored_touch_does_not_extend_delay_and_expiry_repeats_beyond_level(self):
        ready(self.store, only={"previous_month"})
        first = self.store.record_price("TESTUSDT", D("101"), START, DELAYS)
        self.assertEqual(len(first), 1)
        self.assertEqual(self.store.record_price("TESTUSDT", D("102"), START + timedelta(days=4), DELAYS), [])
        again = self.store.record_price("TESTUSDT", D("102"), START + timedelta(days=5), DELAYS)
        self.assertEqual(len(again), 1)
        monthly = [w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high"][0]
        self.assertEqual(monthly.accepted_count, 2)

    def test_fractional_days_and_invalid_delays(self):
        ready(self.store, only={"previous_month"})
        delays = {name: D("0.1") for name in DELAYS}
        self.assertEqual(delay_duration(D("0.1")), timedelta(hours=2, minutes=24))
        self.store.record_price("TESTUSDT", D("100"), START, delays)
        self.assertEqual(self.store.record_price("TESTUSDT", D("100"), START + timedelta(hours=2), delays), [])
        self.assertEqual(len(self.store.record_price("TESTUSDT", D("100"), START + timedelta(hours=2, minutes=24), delays)), 1)
        for invalid in (D(0), D(-1), D("NaN"), D("Infinity")):
            with self.assertRaises(ValueError):
                delay_duration(invalid)

    def test_restart_keeps_activation_time_not_restart_time(self):
        ready(self.store, only={"previous_month"})
        self.store.record_price("TESTUSDT", D("100"), START, DELAYS)
        with TouchStore(self.path) as restarted:
            ready(restarted, START + timedelta(days=1), only={"previous_month"})
            self.assertEqual(restarted.record_price("TESTUSDT", D("100"), START + timedelta(days=4), DELAYS), [])
            self.assertEqual(len(restarted.record_price("TESTUSDT", D("100"), START + timedelta(days=5), DELAYS)), 1)

    def test_new_month_resets_even_identical_numeric_level(self):
        before = START.replace(day=31)
        ready(self.store, before, only={"previous_month"})
        self.store.record_price("TESTUSDT", D("100"), before, DELAYS)
        after = before.replace(month=11, day=1)
        ready(self.store, after, only={"previous_month"})
        self.assertEqual(len(self.store.record_price("TESTUSDT", D("100"), after, DELAYS)), 1)
        state = next(w for w in self.store.history_jobs("TESTUSDT") if w.name == "previous_month" and w.side == "high")
        self.assertEqual(state.accepted_count, 1)

    def test_monday_first_only_then_undefined_for_next_monday(self):
        ready(self.store, only={"monday"})
        self.assertEqual(len(self.store.record_price("TESTUSDT", D("100"), START, DELAYS)), 1)
        short = {name: D("0.1") for name in DELAYS}
        self.assertEqual(self.store.record_price("TESTUSDT", D("100"), START + timedelta(days=3), short), [])
        monday = START.replace(day=19, hour=0)
        self.store.initialize_watches("TESTUSDT", references(monday), monday)
        states = self.store.dashboard_snapshot(["TESTUSDT"], monday, DELAYS)["watches"]
        monday_rows = [row for row in states if row["name"] == "monday"]
        self.assertEqual(len(monday_rows), 2)
        self.assertTrue(all(row["status"] == "unavailable" for row in monday_rows))
        tuesday = monday + timedelta(days=1)
        ready(self.store, tuesday, only={"monday"})
        hits = self.store.record_price("TESTUSDT", D("100"), tuesday, DELAYS)
        self.assertEqual(len([hit for hit in hits if hit.level_name == "monday"]), 1)

    def test_month_count_survives_expiry_while_quarter_badge_and_old_time_disappear(self):
        ready(self.store, only={"previous_month", "previous_quarter"})
        self.store.record_price("TESTUSDT", D("100"), START, DELAYS, origin="history")
        state = self.store.dashboard_snapshot(["TESTUSDT"], START + timedelta(days=6), DELAYS)
        month = next(row for row in state["watches"] if row["name"] == "previous_month" and row["side"] == "high")
        quarter = next(row for row in state["watches"] if row["name"] == "previous_quarter" and row["side"] == "high")
        self.assertEqual((month["status"], month["count"]), ("untouched", 1))
        self.assertEqual((quarter["status"], quarter["count"], quarter["accepted_at"]), ("untouched", 0, None))
        self.assertEqual(state["fresh"], ())

    def test_history_and_disabled_email_do_not_stack_and_prior_run_pending_is_muted(self):
        ready(self.store, only={"previous_month", "previous_quarter"})
        self.store.record_price("TESTUSDT", D("100"), START, DELAYS, origin="history", notify=True)
        self.assertEqual(self.store.load_touch_outbox(START), ())
        self.store.record_price("TESTUSDT", D("90"), START, DELAYS, notify=False)
        self.assertEqual(self.store.load_touch_outbox(START), ())
        self.store.record_price("TESTUSDT", D("90"), START + timedelta(days=5), DELAYS, notify=True)
        self.assertEqual(len(self.store.load_touch_outbox(START + timedelta(days=5))), 2)
        self.store.begin_run(START + timedelta(days=5, seconds=1))
        self.assertEqual(self.store.load_touch_outbox(START + timedelta(days=5, seconds=1)), ())

    def test_unverified_watch_does_not_create_live_activation(self):
        self.store.initialize_watches("TESTUSDT", references(START), START)
        self.assertEqual(self.store.record_price("TESTUSDT", D("100"), START, DELAYS, notify=True), [])
        self.assertEqual(self.store.load_touch_outbox(START), ())

    def test_disabling_email_mutes_pending_but_preserves_sent_notifications(self):
        ready(self.store, only={"previous_month", "previous_quarter"})
        self.store.record_price("TESTUSDT", D("100"), START, DELAYS, notify=True)
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


if __name__ == "__main__":
    unittest.main()
