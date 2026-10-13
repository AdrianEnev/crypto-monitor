"""Deterministic socket/timer mocks; no connections to Binance."""

import asyncio
import sqlite3
import ssl
import tempfile
import unittest
from pathlib import Path
from contextlib import closing
from unittest.mock import AsyncMock, MagicMock, patch

from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidHandshake, InvalidStatus
from websockets.frames import Close
from websockets.http11 import Response

from crypto_monitor.adapters.binance_feed import FeedCallbackError, FeedConnectionError, FeedMessageError
from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter
from crypto_monitor.runtime.gap_recovery import MonitoringFailure, RecoveryWorker
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from crypto_monitor.runtime.websocket_supervisor import WebSocketSupervisor
from tests.test_binance_feed import FakeSocket, message, payload
from tests.test_gap_recovery import references
from tests.test_minute_klines import START, millis, query, window_response
from tests.test_sqlite_store import MARKETS
from crypto_monitor.core.level_sweep import MINUTE


class ManualSleep:
    def __init__(self):
        self.calls = asyncio.Queue()

    async def __call__(self, delay):
        future = asyncio.get_running_loop().create_future()
        self.calls.put_nowait((delay, future))
        await future

    async def next(self):
        while True:
            delay, future = await asyncio.wait_for(self.calls.get(), 2)
            if not future.cancelled():
                return delay, future


class FakeWorker:
    markets = MARKETS

    def __init__(self):
        self.ready = asyncio.Event()
        self.failed = asyncio.Event()
        self.submit = AsyncMock()
        self.starts = 0
        self.closed = False

    async def run(self, stop):
        self.starts += 1
        self.ready.set()
        try:
            await self.failed.wait()
            raise MonitoringFailure("unresolved recovery failure")
        finally:
            self.ready.clear()
            self.closed = True


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.worker = FakeWorker()
        self.timer = ManualSleep()
        self.stop = asyncio.Event()
        self.socket = FakeSocket()
        self.connector = MagicMock(return_value=self.socket)

    def supervisor(self, **kwargs):
        return WebSocketSupervisor(self.worker, connector=self.connector,
                                   sleep=self.timer, random_value=lambda: 1, **kwargs)

    async def start(self, supervisor):
        task = asyncio.create_task(supervisor.run(self.stop))
        async def cleanup():
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.addAsyncCleanup(cleanup)
        return task

    async def finish(self, task):
        return await asyncio.wait_for(task, 2)

    async def test_transient_failure_reconnects_and_preserves_feed_settings(self):
        self.connector.side_effect = [OSError("network down"), self.socket]
        supervisor = self.supervisor()
        task = await self.start(supervisor)
        delay, wake = await self.timer.next()
        self.assertEqual(delay, 1)
        self.assertEqual(supervisor.status.state, "backoff")
        wake.set_result(None)
        await self.finish(self.socket.entered.wait())
        self.assertEqual(supervisor.status.state, "connected")
        self.assertEqual(supervisor.status.reconnect_attempts, 1)
        self.assertIn("network down", supervisor.status.last_error)
        self.assertEqual(self.worker.starts, 1)
        self.socket.queue.put_nowait(message())
        await self.finish(self.socket.waiting.wait())
        self.stop.set()
        await self.finish(task)
        self.assertTrue(self.socket.closed)
        self.assertTrue(self.worker.closed)
        self.assertEqual(supervisor.status.state, "stopped")
        first, second = self.connector.call_args_list
        self.assertEqual(first, second)
        self.assertEqual(first.args[0], "wss://fstream.binance.com/market/stream?streams="
                         "btcusdt@kline_1m/ethusdt@kline_1m")
        self.assertEqual(first.kwargs, dict(open_timeout=10, close_timeout=5,
                         ping_interval=20, ping_timeout=20, max_size=65_536,
                         max_queue=16, compression=None))

    async def test_exponential_backoff_progression_and_cap(self):
        self.connector.side_effect = [OSError("offline")] * 4 + [self.socket]
        supervisor = self.supervisor(base_delay=2, max_delay=5)
        task = await self.start(supervisor)
        for failures, expected in enumerate((2, 4, 5, 5), start=1):
            delay, wake = await self.timer.next()
            self.assertEqual(delay, expected)
            self.assertEqual(supervisor.status.consecutive_failures, failures)
            self.assertEqual(supervisor.status.reconnect_attempts, failures - 1)
            wake.set_result(None)
        await self.finish(self.socket.entered.wait())
        self.stop.set()
        await self.finish(task)
        self.assertEqual(supervisor.status.reconnect_attempts, 4)

    async def test_jitter_is_bounded_and_injectable(self):
        self.connector.side_effect = OSError("offline")
        supervisor = WebSocketSupervisor(self.worker, connector=self.connector,
                        sleep=self.timer, random_value=lambda: .25, base_delay=4, max_delay=4)
        task = await self.start(supervisor)
        delay, _ = await self.timer.next()
        self.assertEqual(delay, 2.5)
        self.stop.set()
        await self.finish(task)
        self.connector.assert_called_once()

    async def test_remote_close_retries_and_stable_connection_resets_backoff(self):
        second = FakeSocket()
        clock = [0.0]
        self.connector.side_effect = [OSError("offline"), self.socket, second]
        supervisor = self.supervisor(base_delay=2, monotonic=lambda: clock[0])
        task = await self.start(supervisor)
        _, wake = await self.timer.next()
        wake.set_result(None)
        await self.finish(self.socket.entered.wait())
        rotation_delay, rotation = await self.timer.next()
        self.assertEqual(rotation_delay, 23 * 3600)
        clock[0] = 61
        self.socket.queue.put_nowait(None)
        delay, wake = await self.timer.next()
        self.assertTrue(rotation.cancelled())
        self.assertEqual(delay, 2)
        self.assertEqual(supervisor.status.consecutive_failures, 1)
        wake.set_result(None)
        await self.finish(second.entered.wait())
        self.assertIn("closed remotely", supervisor.status.last_disconnect)
        self.stop.set()
        await self.finish(task)

    async def test_rotation_closes_old_socket_before_resubscribing(self):
        second = FakeSocket()
        def connector(*args, **kwargs):
            if self.connector.call_count == 1:
                return self.socket
            self.assertTrue(self.socket.closed)
            return second
        self.connector.side_effect = connector
        supervisor = self.supervisor(rotation_interval=10)
        task = await self.start(supervisor)
        delay, rotate = await self.timer.next()
        self.assertEqual(delay, 10)
        rotate.set_result(None)
        await self.finish(second.entered.wait())
        self.assertEqual(supervisor.status.reconnect_attempts, 1)
        self.assertEqual(supervisor.status.last_disconnect, "planned connection rotation")
        self.assertIsNone(supervisor.status.last_error)
        self.assertEqual(supervisor.status.consecutive_failures, 0)
        self.assertEqual(self.worker.starts, 1)
        self.stop.set()
        await self.finish(task)

    async def test_short_lived_connection_does_not_reset_backoff(self):
        self.connector.side_effect = [OSError("offline"), self.socket]
        supervisor = self.supervisor(monotonic=lambda: 0.0)
        task = await self.start(supervisor)
        _, wake = await self.timer.next()
        wake.set_result(None)
        await self.finish(self.socket.entered.wait())
        await self.timer.next()  # Pending rotation timer.
        self.socket.queue.put_nowait(None)
        delay, _ = await self.timer.next()
        self.assertEqual(delay, 2)
        self.assertEqual(supervisor.status.consecutive_failures, 2)
        self.stop.set()
        await self.finish(task)

    async def test_malformed_message_is_fatal(self):
        self.socket.queue.put_nowait("bad JSON")
        supervisor = self.supervisor()
        with self.assertRaises(FeedMessageError):
            await self.finish(supervisor.run(self.stop))
        self.assertEqual(supervisor.status.state, "unhealthy")
        self.assertIn("JSON", supervisor.status.last_error)
        self.connector.assert_called_once()
        self.assertTrue(self.socket.closed)
        self.assertTrue(self.worker.closed)

    async def test_permanent_handshake_and_policy_errors_are_not_retried(self):
        errors = [InvalidHandshake("invalid handshake"),
                  InvalidStatus(Response(403, "Forbidden", Headers())),
                  ssl.SSLCertVerificationError(1, "invalid certificate"),
                  ConnectionClosedError(Close(1008, "policy"), None)]
        for error in errors:
            with self.subTest(error=error):
                self.worker = FakeWorker()
                self.connector.reset_mock()
                self.connector.side_effect = error
                supervisor = self.supervisor()
                with self.assertRaises(FeedConnectionError):
                    await self.finish(supervisor.run(self.stop))
                self.assertEqual(supervisor.status.state, "unhealthy")
                self.connector.assert_called_once()

    async def test_callback_failure_is_fatal_without_socket_retries(self):
        self.worker.submit.side_effect = MonitoringFailure("queue overflow")
        self.socket.queue.put_nowait(message())
        supervisor = self.supervisor()
        with self.assertRaises(FeedCallbackError) as caught:
            await self.finish(supervisor.run(self.stop))
        self.assertIn("queue overflow", str(caught.exception.__cause__))
        self.assertEqual(supervisor.status.state, "unhealthy")
        self.connector.assert_called_once()

    async def test_server_handshake_failure_is_transient(self):
        self.connector.side_effect = [InvalidStatus(Response(503, "Unavailable", Headers())), self.socket]
        supervisor = self.supervisor()
        task = await self.start(supervisor)
        _, wake = await self.timer.next()
        wake.set_result(None)
        await self.finish(self.socket.entered.wait())
        self.stop.set()
        await self.finish(task)
        self.assertEqual(supervisor.status.reconnect_attempts, 1)

    async def test_worker_failure_is_fatal_even_without_another_candle(self):
        supervisor = self.supervisor()
        task = await self.start(supervisor)
        await self.finish(self.socket.entered.wait())
        self.worker.failed.set()
        with self.assertRaisesRegex(MonitoringFailure, "unresolved recovery"):
            await self.finish(task)
        self.assertEqual(supervisor.status.state, "unhealthy")
        self.assertTrue(self.socket.closed)
        self.connector.assert_called_once()

    async def test_cancellation_during_backoff_is_prompt(self):
        self.connector.side_effect = OSError("network")
        supervisor = self.supervisor()
        task = await self.start(supervisor)
        _, sleeper = await self.timer.next()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.finish(task)
        self.assertTrue(sleeper.cancelled())
        self.assertTrue(self.worker.closed)
        self.assertEqual(supervisor.status.state, "stopped")
        self.connector.assert_called_once()

    async def test_cancellation_connected_closes_socket_and_worker(self):
        supervisor = self.supervisor()
        task = await self.start(supervisor)
        await self.finish(self.socket.entered.wait())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.finish(task)
        self.assertTrue(self.socket.closed)
        self.assertTrue(self.worker.closed)

    async def test_stop_during_handshake_cancels_setup_without_retry(self):
        entered, cancelled = asyncio.Event(), asyncio.Event()
        async def opening():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        context = MagicMock()
        context.__aenter__ = AsyncMock(side_effect=opening)
        self.connector.return_value = context
        supervisor = self.supervisor()
        task = await self.start(supervisor)
        await self.finish(entered.wait())
        self.assertTrue(self.timer.calls.empty())  # Rotation waits for handshake.
        self.stop.set()
        await self.finish(task)
        self.assertTrue(cancelled.is_set())
        self.connector.assert_called_once()

    async def test_pre_requested_stop_and_invalid_configuration(self):
        supervisor = self.supervisor()
        self.stop.set()
        await supervisor.run(self.stop)
        self.connector.assert_not_called()
        self.assertEqual(self.worker.starts, 0)
        for settings in ({"base_delay": 0}, {"max_delay": .1}, {"max_delay": float("inf")},
                         {"rotation_interval": 86400}, {"rotation_interval": True}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                self.supervisor(**settings)


class RecoveryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_backfills_and_duplicate_rest_live_timestamp_is_not_reprocessed(self):
        import json
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "monitor.sqlite3"
        rest_opener = MagicMock(side_effect=window_response)
        worker = RecoveryWorker(MARKETS, path, BinanceRESTAdapter(opener=rest_opener), references)
        def live(minute):
            raw = payload()
            opened = millis(START + minute * MINUTE)
            raw["data"]["E"] = opened + 60_000
            raw["data"]["k"].update(t=opened, T=opened + 59_999, o="99", h="101", l="98", c="99")
            return json.dumps(raw)
        first = FakeSocket([live(0)])
        second = FakeSocket([live(3), live(2), live(4)])
        connector = MagicMock(side_effect=[first, second])
        timer, stop = ManualSleep(), asyncio.Event()
        supervisor = WebSocketSupervisor(worker, connector=connector, sleep=timer, random_value=lambda: 1)
        baseline, finished = asyncio.Event(), asyncio.Event()
        loop = asyncio.get_running_loop()
        original = SQLiteStore.commit_candle
        def commit(store, current, hits):
            result = original(store, current, hits)
            if current.candle.open_time == START:
                loop.call_soon_threadsafe(baseline.set)
            if current.candle.open_time == START + 4 * MINUTE:
                loop.call_soon_threadsafe(finished.set)
            return result
        with patch.object(SQLiteStore, "commit_candle", commit):
            task = asyncio.create_task(supervisor.run(stop))
            try:
                await asyncio.wait_for(baseline.wait(), 2)
                _, rotation = await timer.next()
                first.queue.put_nowait(None)
                _, wake = await timer.next()
                self.assertTrue(rotation.cancelled())
                wake.set_result(None)
                await asyncio.wait_for(finished.wait(), 3)
                stop.set()
                await asyncio.wait_for(task, 3)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        with SQLiteStore(path) as store:
            self.assertEqual(store.load_baselines()["BTCUSDT"].open_time, START + 4 * MINUTE)
            self.assertEqual(len(store.load_hits()), 4)
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM processed_candles").fetchone(), (5,))
        rest_opener.assert_called_once()
        params = query(rest_opener.call_args.args[0])
        self.assertEqual(params["startTime"], str(millis(START + MINUTE)))
        self.assertEqual(params["endTime"], str(millis(START + 3 * MINUTE) - 1))
        self.assertEqual(connector.call_count, 2)


if __name__ == "__main__":
    unittest.main()
