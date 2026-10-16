"""Live email switching with temporary state and entirely offline senders."""

import asyncio
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from crypto_monitor.adapters.ses_email import EmailSettings
from crypto_monitor.adapters.touch_email import TouchEmailDispatcher
from crypto_monitor.persistence.touch_store import TouchStore
from crypto_monitor.runtime.service import Settings
from crypto_monitor.runtime.touch_service import TouchRuntime, run_touch_service
from tests.test_touch_service import (
    ENABLED_EMAIL, POLICY, START, FakeREST, RecordingEmail, tick,
)


class WaitingEmail(RecordingEmail):
    """Leave committed outbox entries pending until explicitly stopped."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_requests = 0
        self.stopped = asyncio.Event()

    def request_stop(self):
        self.stop_requests += 1

    async def run(self, stop):
        try:
            await stop.wait()
        finally:
            self.stopped.set()


class ServiceHarness:
    """Expose the real service's dashboard callback and trade callback."""

    def __init__(self, testcase, path, *, email=None, sender_factory=WaitingEmail):
        self.path = path
        self.clock = START
        self.rest = FakeREST(symbols=("FILUSDT", "BTCUSDT"))
        self.stop = asyncio.Event()
        self.ready = asyncio.Event()
        self.senders = []
        self.trade_ids = {}
        self.display = self.supervisor = self.task = None
        harness = self

        class Dashboard:
            def __init__(self, collector, *, on_toggle_email, **kwargs):
                self.collector, self.toggle = collector, on_toggle_email
                self.pending_toggle = None
                harness.display = self

            async def run_loading(self, stop, progress):
                await stop.wait()

            async def run(self, stop):
                harness.ready.set()
                try:
                    await stop.wait()
                finally:
                    if self.pending_toggle is not None:
                        await asyncio.shield(self.pending_toggle)

        class Supervisor:
            state, error = "loading", None

            def __init__(self, symbols, callback, on_connect):
                self.callback, self.on_connect = callback, on_connect
                self.stopped = asyncio.Event()
                harness.supervisor = self

            async def run(self, stop):
                try:
                    await self.on_connect()
                    self.state = "connected"
                    await stop.wait()
                finally:
                    self.stopped.set()

            def update_symbols(self, symbols):
                pass

        def email_factory(*args, **kwargs):
            sender = sender_factory(*args, **kwargs)
            self.senders.append(sender)
            return sender

        self.settings = Settings(database=path,
            email=replace(ENABLED_EMAIL, enabled=False) if email is None else email)
        self.arguments = dict(policy=POLICY, stop=self.stop, rest_factory=lambda: self.rest,
            supervisor_factory=Supervisor, dashboard_factory=Dashboard,
            email_factory=email_factory, now=lambda: self.clock)
        testcase.addAsyncCleanup(self.cleanup)

    async def start(self):
        self.task = asyncio.create_task(run_touch_service(self.settings, **self.arguments))
        await asyncio.wait_for(self.ready.wait(), 3)
        return self

    async def emit(self, price, *, symbol="FILUSDT"):
        # Aggregate trade IDs increase per symbol, as on the exchange.
        self.clock += timedelta(seconds=1)
        self.trade_ids[symbol] = self.trade_ids.get(symbol, 0) + 1
        await self.supervisor.callback(tick(price, self.clock, symbol=symbol,
                                            trade_id=self.trade_ids[symbol]))

    async def snapshot(self):
        return await self.display.collector()

    def outbox_rows(self):
        with closing(sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)) as connection:
            return connection.execute(
                "SELECT sent_at,muted_at,muted_reason FROM touch_email_outbox"
            ).fetchall()

    async def finish(self):
        self.stop.set()
        await asyncio.wait_for(self.task, 3)

    async def cleanup(self):
        self.stop.set()
        if self.task is not None:
            await asyncio.wait_for(asyncio.gather(self.task, return_exceptions=True), 4)


class EmailToggleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "toggle.sqlite3"
        self.enterContext(patch.dict(os.environ, {}, clear=True))
        self.enterContext(patch("crypto_monitor.runtime.service.should_use_dashboard", return_value=True))

    async def test_live_toggle_mutes_pending_and_only_notifies_new_enabled_touches(self):
        service = await ServiceHarness(self, self.path).start()
        self.assertFalse((await service.snapshot())["email_enabled"])
        self.assertEqual(service.senders, [])

        self.assertTrue(await service.display.toggle())
        self.assertTrue((await service.snapshot())["email_enabled"])
        await service.emit("100")
        first = service.senders[0]
        self.assertTrue(first.settings.enabled)
        self.assertEqual(first.notifications, 1)
        self.assertEqual((await service.snapshot())["pending_email_count"], 4)

        self.assertFalse(await service.display.toggle())
        self.assertFalse(service.task.done())
        self.assertTrue(first.stopped.is_set())
        self.assertEqual(first.stop_requests, 1)
        disabled = await service.snapshot()
        self.assertFalse(disabled["email_enabled"])
        self.assertEqual(disabled["pending_email_count"], 0)
        self.assertEqual(len(service.outbox_rows()), 4)
        self.assertTrue(all(sent is None and muted is not None and reason == "email disabled"
                            for sent, muted, reason in service.outbox_rows()))

        await service.emit("90")
        quiet_email = await service.snapshot()
        self.assertEqual(len(quiet_email["fresh"]), 8)
        self.assertEqual(quiet_email["pending_email_count"], 0)
        self.assertEqual(first.notifications, 1)
        self.assertEqual(len(service.outbox_rows()), 4)

        self.assertTrue(await service.display.toggle())
        second = service.senders[1]
        self.assertEqual(second.notifications, 0)
        with TouchStore.open_readonly(self.path) as store:
            self.assertEqual(store.load_touch_outbox(service.clock), ())
        await service.emit("100", symbol="BTCUSDT")
        enabled = await service.snapshot()
        self.assertEqual(len(enabled["fresh"]), 12)
        self.assertEqual(enabled["pending_email_count"], 4)
        self.assertEqual(second.notifications, 1)
        self.assertFalse(service.task.done())
        await service.finish()

    async def test_startup_enabled_sender_can_be_disabled_without_stopping_monitor(self):
        service = await ServiceHarness(self, self.path, email=ENABLED_EMAIL).start()
        self.assertEqual(len(service.senders), 1)
        self.assertTrue((await service.snapshot())["email_enabled"])
        await service.emit("100")
        self.assertFalse(await service.display.toggle())
        await service.emit("90")
        current = await service.snapshot()
        self.assertFalse(current["email_enabled"])
        self.assertEqual(current["pending_email_count"], 0)
        self.assertEqual(len(current["fresh"]), 8)
        self.assertEqual(service.senders[0].notifications, 1)
        self.assertFalse(service.task.done())
        await service.finish()

    async def test_delayed_off_period_tick_stays_silent_after_enabling(self):
        service = await ServiceHarness(self, self.path).start()
        touched_while_off = START + timedelta(seconds=1)
        service.clock = START + timedelta(seconds=2)
        self.assertTrue(await service.display.toggle())
        service.clock += timedelta(seconds=1)
        await service.supervisor.callback(tick("100", touched_while_off))
        delayed = await service.snapshot()
        self.assertTrue(delayed["email_enabled"])
        self.assertEqual(len(delayed["fresh"]), 4)
        self.assertTrue(all(event["touched_at"] == touched_while_off for event in delayed["fresh"]))
        self.assertEqual(delayed["pending_email_count"], 0)
        self.assertEqual(service.senders[0].notifications, 0)
        self.assertEqual(service.outbox_rows(), [])

        await service.emit("100", symbol="BTCUSDT")
        current = await service.snapshot()
        self.assertEqual(len(current["fresh"]), 8)
        self.assertEqual(current["pending_email_count"], 4)
        self.assertEqual(service.senders[0].notifications, 1)
        await service.finish()

    async def test_enable_waiting_for_processing_does_not_start_sender_during_shutdown(self):
        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        # The durable contact write runs on the worker thread.
        original_process = TouchRuntime.record

        def blocked_process(runtime, *args, **kwargs):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("offline processing was not released")
            return original_process(runtime, *args, **kwargs)

        with patch.object(TouchRuntime, "record", blocked_process):
            service = await ServiceHarness(self, self.path).start()
            self.addCleanup(release.set)
            processing = asyncio.create_task(service.emit("100"))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                service.display.pending_toggle = asyncio.create_task(service.display.toggle())
                await asyncio.sleep(0)
                self.assertFalse(service.display.pending_toggle.done())
                service.task.cancel()
                await asyncio.wait_for(service.supervisor.stopped.wait(), 1)
                release.set()
                await asyncio.wait_for(processing, 2)
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(service.task, 2)
                self.assertFalse(await service.display.pending_toggle)
                self.assertEqual(service.senders, [])
            finally:
                release.set()
                await asyncio.gather(processing, return_exceptions=True)

    async def test_missing_email_addresses_stays_off_and_reports_attention(self):
        service = await ServiceHarness(self, self.path, email=EmailSettings()).start()
        with self.assertLogs("crypto_monitor.runtime.touch_service", level="WARNING"):
            self.assertFalse(await service.display.toggle())
        current = await service.snapshot()
        self.assertFalse(current["email_enabled"])
        self.assertIn("Email stays off", current["error"])
        self.assertIn("valid sender address", current["error"])
        self.assertEqual(service.senders, [])
        await service.emit("100")
        current = await service.snapshot()
        self.assertEqual(len(current["fresh"]), 4)
        self.assertEqual(current["pending_email_count"], 0)
        self.assertFalse(service.task.done())
        await service.finish()

    async def test_incomplete_smtp_configuration_stays_off_without_constructing_sender(self):
        with patch.dict(os.environ, {"SES_SMTP_HOST": "email-smtp.example.test"}):
            service = await ServiceHarness(self, self.path).start()
            with self.assertLogs("crypto_monitor.runtime.touch_service", level="WARNING"):
                self.assertFalse(await service.display.toggle())
            current = await service.snapshot()
            self.assertFalse(current["email_enabled"])
            self.assertIn("Email stays off", current["error"])
            self.assertIn("SES_SMTP_USER", current["error"])
            self.assertEqual(service.senders, [])
            self.assertFalse(service.task.done())
            await service.finish()

    async def test_disable_finishes_inflight_ack_then_mutes_pending_while_prices_continue(self):
        entered, stopping = asyncio.Event(), asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        sent = []

        class BlockingClient:
            def send_email(self, **kwargs):
                sent.append(kwargs)
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(3):
                    raise TimeoutError("offline send was not released")
                return {"MessageId": "offline-accepted"}

        class BlockingDispatcher(TouchEmailDispatcher):
            def request_stop(self):
                super().request_stop()
                stopping.set()

        def email_factory(*args, **kwargs):
            return BlockingDispatcher(*args, client=BlockingClient(), **kwargs)

        service = await ServiceHarness(self, self.path, sender_factory=email_factory).start()
        self.addCleanup(release.set)
        await service.display.toggle()
        await service.emit("100")
        await asyncio.wait_for(entered.wait(), 2)
        disabling = asyncio.create_task(service.display.toggle())
        try:
            await asyncio.wait_for(stopping.wait(), 1)
            self.assertFalse(disabling.done())
            self.assertFalse((await service.snapshot())["email_enabled"])
            await asyncio.wait_for(service.emit("90"), 1)
            current = await service.snapshot()
            self.assertEqual(len(current["fresh"]), 8)
            self.assertEqual(current["pending_email_count"], 4)
            self.assertEqual(len(service.outbox_rows()), 4)

            release.set()
            self.assertFalse(await asyncio.wait_for(disabling, 2))
            rows = service.outbox_rows()
            self.assertEqual(sum(sent_at is not None and muted_at is None
                                 for sent_at, muted_at, _ in rows), 1)
            self.assertEqual(sum(sent_at is None and muted_at is not None and reason == "email disabled"
                                 for sent_at, muted_at, reason in rows), 3)
            self.assertEqual(len(sent), 1)
            self.assertEqual((await service.snapshot())["pending_email_count"], 0)
            self.assertFalse(service.task.done())
            await service.finish()
        finally:
            release.set()
            await asyncio.gather(disabling, return_exceptions=True)

    async def test_unexpected_sender_bookkeeping_failure_stops_service(self):
        fail = asyncio.Event()

        class FailingSender(WaitingEmail):
            async def run(self, stop):
                await fail.wait()
                raise sqlite3.OperationalError("offline bookkeeping failure")

        service = await ServiceHarness(self, self.path, sender_factory=FailingSender).start()
        await service.display.toggle()
        await service.emit("100")
        fail.set()
        with self.assertRaisesRegex(sqlite3.OperationalError, "offline bookkeeping failure"):
            await asyncio.wait_for(service.task, 2)
        self.assertTrue(service.supervisor.stopped.is_set())


if __name__ == "__main__":
    unittest.main()
