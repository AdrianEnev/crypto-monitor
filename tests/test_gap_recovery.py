"""Recovery tests use mocked HTTP and real temporary SQLite files."""

import asyncio
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter, BinanceResponseError, BinanceTransportError, UnresolvedGapError
from crypto_monitor.runtime.gap_recovery import GapRecoveryProcessor, MonitoringFailure, RecoveryWorker
from crypto_monitor.core.level_sweep import MINUTE, Levels, reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from tests.test_minute_klines import START, millis, query, response, row, window_response
from tests.test_sqlite_store import D, MARKETS, event


def references(symbol, opened):
    return {name: ReferenceLevels(period, Levels(D("100"), D("90")))
            for name, period in reference_periods(opened).items() if period is not None}


class GapRecoveryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "recovery.sqlite3"
        self.store = SQLiteStore(self.path)
        self.addCleanup(self.store.close)
        self.opener = MagicMock(side_effect=window_response)
        self.rest = BinanceRESTAdapter(opener=self.opener)
        self.provider = MagicMock(side_effect=references)
        self.processor = GapRecoveryProcessor(MARKETS, self.store, self.rest, self.provider)

    def cursor(self, symbol="BTCUSDT"):
        return self.store.load_baselines()[symbol].open_time

    def test_first_launch_baselines_then_consecutive_live_sweep(self):
        self.assertEqual(self.processor.process_live(event()), [])
        self.provider.assert_not_called()
        self.assertEqual(self.cursor(), START)
        hits = self.processor.process_live(event(1))
        self.assertEqual(len(hits), 4)
        self.assertEqual(set(self.store.load_hits()), set(hits))
        self.assertEqual(self.cursor(), START + MINUTE)
        self.opener.assert_not_called()

    def test_backfill_commits_every_candle_in_order_before_live(self):
        self.processor.process_live(event())
        with patch.object(self.store, "commit_candle", wraps=self.store.commit_candle) as commit:
            hits = self.processor.process_live(event(4))
        self.assertEqual([call.args[0].candle.open_time for call in commit.call_args_list],
                         [START + i * MINUTE for i in (1, 2, 3, 4)])
        self.assertEqual(len(hits), 4)
        self.assertEqual({h.detected_at for h in hits}, {event(4).event_time})
        params = query(self.opener.call_args.args[0])
        self.assertEqual(int(params["startTime"]), millis(START + MINUTE))
        self.assertEqual(int(params["endTime"]), millis(START + 4 * MINUTE) - 1)
        self.assertEqual(self.cursor(), START + 4 * MINUTE)

    def test_restart_suppresses_repeat_and_month_rollover_rearms_only_month(self):
        before = START.replace(day=31, hour=23, minute=58)
        offset = (before - START) // MINUTE
        self.processor.process_live(event(offset))
        first = self.processor.process_live(event(offset + 1))
        self.assertEqual(len(first), 4)
        with SQLiteStore(self.path) as reopened:
            restarted = GapRecoveryProcessor(MARKETS, reopened, self.rest, self.provider)
            new_month = restarted.process_live(event(offset + 2))
            self.assertEqual([hit.level_name for hit in new_month], ["previous_month"])
            self.assertEqual(new_month[0].reference_period_start, before.replace(day=1, hour=0, minute=0))
            self.assertEqual(restarted.process_live(event(offset + 3)), [])
            self.assertEqual(len(reopened.load_outbox()), 5)

    def test_paginated_fetch_failure_retry_starts_at_committed_prefix(self):
        self.processor.process_live(event())

        def fail_second(request, *, timeout):
            if self.opener.call_count == 2:
                raise TimeoutError()
            return window_response(request, timeout=timeout)

        self.opener.side_effect = fail_second
        with self.assertRaises(BinanceTransportError):
            self.processor.process_live(event(502))
        self.assertEqual(self.cursor(), START + 499 * MINUTE)
        self.opener.side_effect = window_response
        self.processor.process_live(event(502))
        params = query(self.opener.call_args.args[0])
        self.assertEqual(int(params["startTime"]), millis(START + 500 * MINUTE))
        self.assertEqual(params["limit"], "2")
        self.assertEqual(self.cursor(), START + 502 * MINUTE)
        self.assertEqual(len(self.store.load_hits()), 4)

    def test_partial_commit_failure_restores_memory_and_retries_failed_minute(self):
        self.processor.process_live(event())
        real_commit = self.store.commit_candle

        def fail_minute_two(current, hits):
            if current.candle.open_time == START + 2 * MINUTE:
                raise sqlite3.OperationalError("simulated disk failure")
            return real_commit(current, hits)

        with patch.object(self.store, "commit_candle", side_effect=fail_minute_two):
            with self.assertRaises(sqlite3.OperationalError):
                self.processor.process_live(event(4))
        self.assertEqual(self.cursor(), START + MINUTE)
        # Directly prove the failed candle is no longer the in-memory baseline.
        self.assertEqual(self.processor._engine._previous["BTCUSDT"].open_time, self.cursor())
        retry_hits = self.processor.process_live(event(4))
        self.assertEqual(query(self.opener.call_args.args[0])["startTime"], str(millis(START + 2 * MINUTE)))
        self.assertEqual(retry_hits, [])
        self.assertEqual(len(self.store.load_hits()), 4)
        self.assertEqual(self.cursor(), START + 4 * MINUTE)

    def test_first_candle_commit_failure_does_not_seed_memory(self):
        with patch.object(self.store, "commit_candle", side_effect=sqlite3.OperationalError("disk")):
            with self.assertRaises(sqlite3.OperationalError):
                self.processor.process_live(event())
        self.assertEqual(self.processor._engine._previous, {})
        self.assertEqual(self.processor.process_live(event()), [])

    def test_live_commit_failure_retry_retains_sweep(self):
        self.processor.process_live(event())
        with patch.object(self.store, "commit_candle", side_effect=sqlite3.OperationalError("disk")):
            with self.assertRaises(sqlite3.OperationalError):
                self.processor.process_live(event(1))
        self.assertEqual(self.cursor(), START)
        self.assertEqual(len(self.processor.process_live(event(1))), 4)

    def test_exception_after_successful_commit_reloads_actual_database_cursor(self):
        self.processor.process_live(event())
        real_commit = self.store.commit_candle

        def commit_then_fail(current, hits):
            real_commit(current, hits)
            raise RuntimeError("failure after commit")

        with patch.object(self.store, "commit_candle", side_effect=commit_then_fail):
            with self.assertRaisesRegex(RuntimeError, "after commit"):
                self.processor.process_live(event(1))
        self.assertEqual(self.cursor(), START + MINUTE)
        self.assertEqual(self.processor.process_live(event(1)), [])
        self.assertEqual(self.processor.process_live(event(2)), [])
        self.assertEqual(len(self.store.load_hits()), 4)

    def test_incomplete_gap_does_not_process_partial_page_or_live(self):
        self.processor.process_live(event())
        self.opener.side_effect = None
        self.opener.return_value = response([row(START + MINUTE), row(START + 3 * MINUTE)])
        with self.assertRaises(UnresolvedGapError) as caught:
            self.processor.process_live(event(4))
        self.assertEqual(caught.exception.start, START + 2 * MINUTE)
        self.assertEqual(self.cursor(), START)
        self.assertEqual(self.store.load_hits(), [])
        self.opener.side_effect = window_response
        self.assertEqual(len(self.processor.process_live(event(4))), 4)

    def test_duplicate_out_of_order_or_malformed_backfill_never_commits_page(self):
        self.processor.process_live(event())
        one, two = row(START + MINUTE), row(START + 2 * MINUTE)
        for payload in ([one, one], [two, one], [one, two[:-1]]):
            with self.subTest(payload=payload):
                self.opener.side_effect = None
                self.opener.return_value = response(payload)
                with self.assertRaises(BinanceResponseError):
                    self.processor.process_live(event(3))
                self.assertEqual(self.cursor(), START)
                self.assertEqual(self.store.load_hits(), [])

    def test_duplicate_and_late_live_events_never_reset_baseline(self):
        self.processor.process_live(event())
        self.processor.process_live(event(2))
        self.opener.reset_mock()
        for current in (event(2), event(1, close="101"), event(-1)):
            self.assertEqual(self.processor.process_live(current), [])
        self.assertEqual(self.processor.process_live(event(3)), [])
        self.assertEqual(len(self.store.load_hits()), 4)
        self.opener.assert_not_called()

    def test_unclosed_live_ignored_and_symbols_have_separate_cursors(self):
        self.assertEqual(self.processor.process_live(replace(event(), candle=replace(event().candle, closed=False))), [])
        self.assertEqual(self.store.load_baselines(), {})
        self.processor.process_live(event())
        self.assertEqual(self.processor.process_live(event(10, "ETHUSDT")), [])
        self.opener.assert_not_called()
        self.assertEqual(len(self.processor.process_live(event(1))), 4)
        self.assertEqual(self.cursor("ETHUSDT"), START + 10 * MINUTE)

    def test_reopen_and_reference_rollover_use_historical_open_times(self):
        # Cross UTC midnight into the next calendar month.
        start = START.replace(month=10, day=31, hour=23, minute=58)
        shift = start - START
        def shifted(minute):
            current = event(minute)
            return replace(current, candle=replace(current.candle, open_time=current.candle.open_time + shift),
                           close_time=current.close_time + shift, event_time=current.event_time + shift)

        self.processor.process_live(shifted(0))
        self.store.close()
        with SQLiteStore(self.path) as reopened:
            processor = GapRecoveryProcessor(MARKETS, reopened, self.rest, self.provider)
            hits = processor.process_live(shifted(3))
        calls = [call.args[1] for call in self.provider.call_args_list]
        self.assertEqual(calls, [start + i * MINUTE for i in (1, 2, 3)])
        months = {h.candle_time: h.reference_period_start for h in hits if h.level_name == "previous_month"}
        self.assertEqual(months[start + MINUTE].month, 9)
        self.assertEqual(months[start + 2 * MINUTE].month, 10)

    def test_stale_references_stop_before_commit(self):
        self.processor.process_live(event())
        self.provider.side_effect = None
        self.provider.return_value = references("BTCUSDT", START.replace(month=9))
        with self.assertRaisesRegex(MonitoringFailure, "incorrect reference period"):
            self.processor.process_live(event(1))
        self.assertEqual(self.cursor(), START)


class RecoveryWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "worker.sqlite3"
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(), [])
        self.opener = MagicMock(side_effect=window_response)
        self.worker = RecoveryWorker(MARKETS, self.path, BinanceRESTAdapter(opener=self.opener), references, capacity=2)

    async def start(self):
        self.stop = asyncio.Event()
        self.task = asyncio.create_task(self.worker.run(self.stop))
        await asyncio.wait_for(self.worker.ready.wait(), 2)
        self.addAsyncCleanup(self.cleanup_task)

    async def cleanup_task(self):
        if not self.task.done():
            self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)

    async def test_receiver_stays_responsive_during_backfill_and_stop_drains(self):
        entered = asyncio.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        loop = asyncio.get_running_loop()

        def blocked(request, *, timeout):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(2):
                raise TimeoutError()
            return window_response(request, timeout=timeout)

        self.opener.side_effect = blocked
        await self.start()
        await self.worker.submit(event(3))
        await asyncio.wait_for(entered.wait(), 2)
        # These callbacks complete while the REST worker thread is blocked.
        await asyncio.wait_for(self.worker.submit(event(4)), .5)
        await asyncio.wait_for(self.worker.submit(event(5)), .5)
        release.set()
        self.stop.set()
        await asyncio.wait_for(self.task, 3)
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START + 5 * MINUTE)
            self.assertEqual(len(store.load_hits()), 4)

    async def test_overflow_latches_failure_instead_of_dropping_silently(self):
        self.worker = RecoveryWorker(MARKETS, self.path, BinanceRESTAdapter(opener=self.opener), references, capacity=1)
        await self.start()
        # No scheduling point inside submit: deterministically fill the queue.
        await self.worker.submit(event(1))
        with self.assertRaisesRegex(MonitoringFailure, "overflow"):
            await self.worker.submit(event(2))
        with self.assertRaisesRegex(MonitoringFailure, "overflow"):
            await self.task
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START)
        with self.assertRaises(MonitoringFailure):
            await self.worker.submit(event(3))

    async def test_failure_keeps_pending_and_queued_events_for_explicit_retry(self):
        self.opener.side_effect = TimeoutError()
        await self.start()
        await self.worker.submit(event(3))
        await self.worker.submit(event(4))
        with self.assertRaises(BinanceTransportError):
            await self.task
        self.assertIsInstance(self.worker.failure, BinanceTransportError)
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START)
        self.opener.side_effect = window_response
        await self.start()
        self.stop.set()
        await asyncio.wait_for(self.task, 3)
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START + 4 * MINUTE)
            self.assertEqual(len(store.load_hits()), 4)

    async def test_cancellation_waits_for_inflight_request_and_retries_pending(self):
        entered = asyncio.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        loop = asyncio.get_running_loop()

        def blocked(request, *, timeout):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(2):
                raise TimeoutError()
            return window_response(request, timeout=timeout)

        self.opener.side_effect = blocked
        await self.start()
        await self.worker.submit(event(3))
        await asyncio.wait_for(entered.wait(), 2)
        with patch.object(loop, "call_exception_handler") as unhandled:
            self.task.cancel()
            await asyncio.sleep(0)  # Deliver cancellation before releasing the mock request.
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await self.task
            unhandled.assert_not_called()
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START)
        self.opener.side_effect = window_response
        await self.start()
        self.stop.set()
        await asyncio.wait_for(self.task, 3)
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START + 3 * MINUTE)

    async def test_overflow_during_backfill_stops_before_more_commits(self):
        entered = asyncio.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        loop = asyncio.get_running_loop()

        def blocked(request, *, timeout):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(2):
                raise TimeoutError()
            return window_response(request, timeout=timeout)

        self.opener.side_effect = blocked
        await self.start()
        await self.worker.submit(event(3))
        await asyncio.wait_for(entered.wait(), 2)
        await self.worker.submit(event(4))
        await self.worker.submit(event(5))
        with self.assertRaisesRegex(MonitoringFailure, "overflow"):
            await self.worker.submit(event(6))
        release.set()
        with self.assertRaisesRegex(MonitoringFailure, "overflow"):
            await self.task
        with SQLiteStore(self.path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START)
        self.opener.side_effect = window_response
        await self.start()
        self.stop.set()
        await asyncio.wait_for(self.task, 3)
        with SQLiteStore(self.path) as store:
            # The rejected sixth minute was reported to the caller, never accepted.
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START + 5 * MINUTE)


if __name__ == "__main__":
    unittest.main()
