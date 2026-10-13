"""Durable snapshots and recovery using fixture daily/minute REST responses only."""

import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter
from crypto_monitor.runtime.gap_recovery import GapRecoveryProcessor, ReferenceUnavailable
from crypto_monitor.core.level_sweep import DAY, MINUTE, Levels, Period, reference_periods, reference_regime
from crypto_monitor.persistence.reference_snapshots import DurableReferences, complete, references_from_levels
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from tests.test_daily_klines import row as daily_row, utc
from tests.test_minute_klines import EPOCH, MS, query, response, row as minute_row
from tests.test_sqlite_store import D, MARKETS, START, event


def daily_response(request, *, timeout):
    params = query(request)
    start = EPOCH + int(params["startTime"]) * MS
    interval = DAY if params["interval"] == "1d" else MINUTE
    rows = []
    for i in range(int(params["limit"])):
        opened = start + i * interval
        if interval == DAY:
            rows.append(daily_row(opened, opening="99", high=str(100 + opened.day), low="90", close="99"))
        else:
            raw = minute_row(opened)
            raw[2] = "130"
            rows.append(raw)
    return response(rows)


def live(opened):
    value = event((opened - START) // MINUTE)
    return replace(value, candle=replace(value.candle, high=D("130")))


class RegimeTests(unittest.TestCase):
    def test_monday_activation_expiry_and_calendar_boundaries(self):
        for instant, start, end in [
            ("2026-10-05T23:59", "2026-10-05", "2026-10-06"),
            ("2026-10-06", "2026-10-06", "2026-10-12"),
            ("2026-10-12", "2026-10-12", "2026-10-13"),
            ("2026-09-30T23:59", "2026-09-29", "2026-10-01"),
            ("2026-10-01", "2026-10-01", "2026-10-05"),
            ("2026-12-31T23:59", "2026-12-29", "2027-01-01"),
            ("2027-01-01", "2027-01-01", "2027-01-04"),
        ]:
            with self.subTest(instant=instant):
                regime = reference_regime(utc(instant))
                self.assertEqual(regime, Period(utc(start), utc(end)))
                self.assertEqual(reference_periods(regime.start), reference_periods(regime.end - MINUTE))
                self.assertNotEqual(reference_periods(regime.start), reference_periods(regime.start - MINUTE))
                self.assertNotEqual(reference_periods(regime.start), reference_periods(regime.end))

    def test_regimes_cover_leap_year_without_changing_sweep_calendar(self):
        start, end = utc("2024-01-01"), utc("2025-01-01")
        opened = start
        previous = None
        while opened < end:
            regime = reference_regime(opened)
            self.assertLessEqual(regime.start, opened)
            self.assertGreater(regime.end, opened)
            if previous is not None and regime != previous:
                self.assertEqual(previous.end, regime.start)
            self.assertEqual(reference_periods(opened), reference_periods(regime.start))
            previous = regime
            opened += DAY
        with self.assertRaises(ValueError):
            reference_regime(start.replace(tzinfo=None))


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "reference.sqlite3"
        self.opener = MagicMock(side_effect=daily_response)
        self.rest = BinanceRESTAdapter(opener=self.opener)
        self.provider = DurableReferences(self.path, self.rest.fetch_reference_levels)

    def refs(self, when=START):
        return references_from_levels(when, self.rest.fetch_reference_levels("BTCUSDT", as_of=when))

    def test_persistence_reopen_valid_range_and_decimal_source_boundaries(self):
        refs = self.refs()
        refs["monday"] = replace(refs["monday"], levels=Levels(D("125.0000000000000000001"), D("90.0000000000000000001")))
        self.provider.persist(START, {"BTCUSDT": refs})
        regime = reference_regime(START)
        with SQLiteStore(self.path) as reopened:
            self.assertEqual(reopened.load_reference_snapshot("BTCUSDT", regime.start), refs)
            self.assertEqual(reopened.load_reference_snapshot("BTCUSDT", regime.end - MINUTE), refs)
            self.assertIsNone(reopened.load_reference_snapshot("BTCUSDT", regime.end))
            self.assertIsNone(reopened.load_reference_snapshot("BTCUSDT", regime.start - MINUTE))
            self.assertIsNone(reopened.load_reference_snapshot("ETHUSDT", START))
        with closing(sqlite3.connect(self.path)) as db:
            raw = json.loads(db.execute("SELECT levels_json FROM reference_snapshots").fetchone()[0])
        self.assertEqual(set(raw), set(reference_periods(START)))
        self.assertEqual(raw["monday"]["high"], "125.0000000000000000001")
        self.assertEqual(raw["monday"]["start"], refs["monday"].period.start.isoformat(timespec="microseconds"))
        forbidden = MagicMock(side_effect=AssertionError("saved history must not refetch"))
        restarted = DurableReferences(self.path, forbidden)
        self.assertEqual(restarted("BTCUSDT", START), refs)
        forbidden.assert_not_called()
        with self.assertRaises(TypeError):
            restarted("BTCUSDT", START)["monday"] = None

    def test_one_daily_request_per_distinct_symbol_and_regime(self):
        for minute in range(120):
            refs = self.provider("BTCUSDT", START + minute * MINUTE)
            self.assertTrue(complete(refs))
        self.assertEqual(self.opener.call_count, 1)
        params = query(self.opener.call_args.args[0])
        self.assertEqual(params["interval"], "1d")
        self.assertEqual(params["limit"], "97")
        self.provider("ETHUSDT", START)
        self.assertEqual(self.opener.call_count, 2)

    def test_more_than_two_regimes_survive_provider_restart(self):
        times = [utc(day) for day in ("2026-09-28", "2026-09-29", "2026-10-01", "2026-10-05", "2026-10-06")]
        expected = [self.provider("BTCUSDT", when) for when in times]
        self.assertEqual(self.opener.call_count, 5)
        forbidden = MagicMock(side_effect=AssertionError("unexpected HTTP"))
        restarted = DurableReferences(self.path, forbidden)
        for when, refs in zip(times, expected):
            self.assertEqual(restarted("BTCUSDT", when), refs)
        forbidden.assert_not_called()

    def test_missing_or_duplicate_daily_history_persists_available_levels_but_blocks(self):
        for corruption in ("missing", "duplicate", "malformed"):
            path = self.path.with_name(corruption + ".sqlite3")
            def corrupt(request, *, timeout):
                payload = json.loads(daily_response(request, timeout=timeout).read())
                if corruption == "missing":
                    payload.pop()  # Monday candle is missing, older periods still available.
                elif corruption == "duplicate":
                    payload.append(payload[-1])
                else:
                    payload[-1][2] = "NaN"
                return response(payload)
            provider = DurableReferences(path, BinanceRESTAdapter(opener=corrupt).fetch_reference_levels)
            with self.subTest(corruption=corruption), self.assertRaises(ReferenceUnavailable):
                provider("BTCUSDT", START)
            with SQLiteStore(path) as store:
                saved = store.load_reference_snapshot("BTCUSDT", START)
            if corruption == "malformed":
                self.assertIsNone(saved)
            else:
                self.assertIsNone(saved["monday"].levels)
                self.assertIsNotNone(saved["previous_quarter"].levels)

    def test_incomplete_snapshot_upgrades_but_complete_values_cannot_change(self):
        good = self.refs()
        partial = {name: replace(ref, levels=None) if name == "monday" else ref for name, ref in good.items()}
        self.provider.persist(START, {"BTCUSDT": partial})
        self.assertEqual(self.provider("BTCUSDT", START), good)
        self.provider.persist(START, {"BTCUSDT": partial})  # No downgrade.
        changed = {**good, "monday": replace(good["monday"], levels=Levels(D("500"), D("90")))}
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.provider.persist(START, {"ETHUSDT": good, "BTCUSDT": changed})
        with SQLiteStore(self.path) as store:
            self.assertIsNone(store.load_reference_snapshot("ETHUSDT", START))
            self.assertEqual(store.load_reference_snapshot("BTCUSDT", START), good)

    def test_database_failure_rolls_back_entire_bundle_and_cache_publication(self):
        refs = self.refs()
        with SQLiteStore(self.path):
            pass
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TRIGGER reject_snapshot BEFORE INSERT ON reference_snapshots
                          WHEN NEW.symbol = 'ETHUSDT'
                          BEGIN SELECT RAISE(ABORT, 'snapshot write failed'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "snapshot write failed"):
            self.provider.persist(START, {"BTCUSDT": refs, "ETHUSDT": refs})
        with SQLiteStore(self.path) as store:
            self.assertIsNone(store.load_reference_snapshot("BTCUSDT", START))
        self.opener.side_effect = TimeoutError("offline")
        with self.assertRaises(ReferenceUnavailable):
            self.provider("BTCUSDT", START)  # A failed batch was not published into RAM.

    def test_bad_source_period_or_missing_watch_rejected_before_storage(self):
        refs = self.refs()
        for bad in ({name: ref for name, ref in refs.items() if name != "monday"},
                    {**refs, "monday": replace(refs["monday"], period=refs["previous_month"].period)}):
            with SQLiteStore(self.path) as store:
                with self.assertRaises(ValueError):
                    store.save_reference_snapshots(START, {"BTCUSDT": bad})
                self.assertIsNone(store.load_reference_snapshot("BTCUSDT", START))

    def test_concurrent_lookup_coalesces_fetch_and_allows_other_database_writes(self):
        entered, release = threading.Event(), threading.Event()
        def blocked(request, *, timeout):
            # The provider must not retain a SQLite transaction during REST.
            with SQLiteStore(self.path) as store:
                store.commit_candle(event(), [])
            entered.set()
            if not release.wait(3):
                raise TimeoutError("test did not release fetch")
            return daily_response(request, timeout=timeout)
        self.opener.side_effect = blocked
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.provider, "BTCUSDT", START)
            try:
                self.assertTrue(entered.wait(2))
                second = pool.submit(self.provider, "BTCUSDT", START + MINUTE)
                release.set()
                self.assertEqual(first.result(3), second.result(3))
            finally:
                release.set()
        self.assertEqual(self.opener.call_count, 1)

    def test_reader_never_observes_half_of_refresh_bundle(self):
        refs = self.refs()
        with SQLiteStore(self.path):
            pass
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TRIGGER pause_snapshot BEFORE INSERT ON reference_snapshots
                          WHEN NEW.symbol = 'ETHUSDT' BEGIN SELECT pause(); END""")
        entered, release = threading.Event(), threading.Event()
        def pause():
            entered.set()
            if not release.wait(3):
                raise TimeoutError("test release missing")
            return 0
        def factory(path):
            store = SQLiteStore(path)
            store._connection.create_function("pause", 0, pause)
            return store
        publisher = DurableReferences(self.path, self.rest.fetch_reference_levels, store_factory=factory)
        with ThreadPoolExecutor(max_workers=1) as pool:
            write = pool.submit(publisher.persist, START, {"BTCUSDT": refs, "ETHUSDT": refs})
            try:
                self.assertTrue(entered.wait(2))
                with SQLiteStore.open_readonly(self.path) as reader:
                    self.assertIsNone(reader.load_reference_snapshot("BTCUSDT", START))
                    self.assertIsNone(reader.load_reference_snapshot("ETHUSDT", START))
                release.set()
                write.result(3)
            finally:
                release.set()
        with SQLiteStore.open_readonly(self.path) as reader:
            self.assertEqual(reader.load_reference_snapshot("BTCUSDT", START), refs)
            self.assertEqual(reader.load_reference_snapshot("ETHUSDT", START), refs)

    def test_backfill_across_week_month_quarter_and_monday_activation(self):
        # Sunday -> Monday (new week/month/quarter) -> Tuesday activation.
        before, after = utc("2024-03-31T23:58"), utc("2024-04-02T00:01")
        with SQLiteStore(self.path) as store:
            store.commit_candle(live(before), [])
            processor = GapRecoveryProcessor(MARKETS[:1], store, self.rest, self.provider)
            hits = processor.process_live(live(after))
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, after)
            self.assertEqual(len(hits), 4)
            for hit in hits:
                period = reference_periods(hit.candle_time)[hit.level_name]
                self.assertEqual(hit.reference_period_start, period.start)
            self.assertEqual({hit.level_price for hit in hits if hit.candle_time == before + MINUTE},
                             {D("124"), D("125"), D("129")})
            self.assertEqual({hit.level_price for hit in hits if hit.candle_time >= utc("2024-04-02")}, {D("101")})
        daily = [call for call in self.opener.call_args_list if query(call.args[0])["interval"] == "1d"]
        self.assertEqual(len(daily), 3)  # Not 1,443 requests for 1,443 processed minutes.
        restarted = DurableReferences(self.path, MagicMock(side_effect=AssertionError("no daily refetch")))
        with SQLiteStore(self.path) as store:
            processor = GapRecoveryProcessor(MARKETS[:1], store, self.rest, restarted)
            self.assertEqual(processor.process_live(live(after + MINUTE)), [])

    def test_missing_history_preserves_committed_prefix_and_retry_resumes(self):
        before, after = utc("2024-03-31T23:58"), utc("2024-04-01T00:01")
        def missing(request, *, timeout):
            params = query(request)
            reply = daily_response(request, timeout=timeout)
            if params["interval"] == "1d" and params["startTime"] == str((utc("2024-01-01") - EPOCH) // MS):
                return response(json.loads(reply.read())[:-1])
            return reply
        self.opener.side_effect = missing
        with SQLiteStore(self.path) as store:
            store.commit_candle(live(before), [])
            processor = GapRecoveryProcessor(MARKETS[:1], store, self.rest, self.provider)
            with self.assertRaisesRegex(ReferenceUnavailable, "daily history unavailable"):
                processor.process_live(live(after))
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, before + MINUTE)
            self.assertEqual(len(store.load_hits()), 3)
            self.opener.side_effect = daily_response
            processor.process_live(live(after))
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, after)
            self.assertEqual(len(store.load_hits()), 3)


if __name__ == "__main__":
    unittest.main()
