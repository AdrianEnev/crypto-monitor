"""Restart keeps level status without re-showing old events as urgent."""

import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from crypto_monitor.persistence.touch_store import TouchStore
from crypto_monitor.runtime.touch_service import live_fresh_events
from tests.test_sqlite_store import START, MARKETS, REFERENCES


class FreshnessTests(unittest.TestCase):
    def test_old_live_activation_remains_swept_but_not_fresh_after_handoff(self):
        delays = {name: Decimal("0.1") for name in
                  ("previous_week", "previous_month", "previous_quarter")}
        with tempfile.TemporaryDirectory() as directory, TouchStore(Path(directory) / "monitor.sqlite3") as store:
            store.initialize_watches("BTCUSDT", REFERENCES, START)
            for watch in store.history_jobs("BTCUSDT"):
                store.advance_history(watch, START)
                store.mark_history_complete("BTCUSDT", watch.name, watch.source_start, START)
            store.record_price("BTCUSDT", Decimal("100"), START, delays, previous=Decimal("99"),
                               origin="live", run_id="earlier")
            store.record_price("BTCUSDT", Decimal("90"), START + timedelta(seconds=10), delays,
                               previous=Decimal("100"), origin="live", run_id="current")
            snapshot = store.dashboard_snapshot(MARKETS[:1], START + timedelta(seconds=12), delays)
            fresh = live_fresh_events(snapshot["fresh"], START + timedelta(seconds=5))
            self.assertTrue(fresh)
            self.assertEqual({event["side"] for event in fresh}, {"low"})
            self.assertTrue(all(row["status"] == "swept" for row in snapshot["watches"]))
            self.assertEqual(live_fresh_events(snapshot["fresh"], None), ())
            # A previous run stays out of the urgent panel even if the clock
            # moves backward and its times look newer than this run's start.
            this_run = store.dashboard_snapshot(MARKETS[:1], START + timedelta(seconds=12), delays,
                                                run_id="current")
            self.assertEqual({event["side"] for event in live_fresh_events(
                this_run["fresh"], START - timedelta(hours=1))}, {"low"})
