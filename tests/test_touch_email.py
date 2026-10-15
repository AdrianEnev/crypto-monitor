"""Fresh separate-email policy and safe asynchronous sends, without AWS."""

import asyncio
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from crypto_monitor.adapters.ses_email import EmailSettings
from crypto_monitor.adapters.touch_email import TouchEmailDispatcher, touch_email_content
from crypto_monitor.core.level_sweep import Levels, reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.persistence.touch_store import TouchStore


START = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
SETTINGS = EmailSettings(True, "monitor@example.com", "recipient@example.com", "eu-west-1")


def activation(identity="activation-1", *, level="previous_month", side="high",
               touched_at=START, origin="live"):
    return SimpleNamespace(
        activation_id=identity, symbol="FILUSDT", level_name=level, side=side,
        reference_period_start=START.replace(day=1, hour=0),
        level_price=Decimal("1.231000000000000001"), touched_at=touched_at,
        observed_at=touched_at, origin=origin,
        captured_quote_volume=Decimal("280180000.123456789"), approximate=False,
    )


class MemoryOutbox:
    """Dispatcher boundary fake; persistence has its own SQLite tests."""

    def __init__(self, *activations):
        self.rows = {}
        for value in activations:
            self.add(value)
        self.failure = None

    def add(self, value):
        self.rows[value.activation_id] = SimpleNamespace(
            activation=value, attempt_count=0, next_attempt_at=None,
            last_error=None, sent_at=None, message_id=None, muted=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def load_touch_outbox(self, now, limit=100):
        if self.failure == "load":
            raise sqlite3.OperationalError("database unavailable")
        return tuple(SimpleNamespace(**vars(row)) for row in self.rows.values()
                     if not row.muted and row.sent_at is None
                     and (row.next_attempt_at is None or row.next_attempt_at <= now))[:limit]

    def expire_touch_outbox(self, before):
        for row in self.rows.values():
            if row.sent_at is None and row.activation.touched_at <= before:
                row.muted = True

    def begin_touch_outbox_attempt(self, identity, attempted_at):
        if self.failure == "begin":
            raise sqlite3.OperationalError("attempt persistence unavailable")
        self.rows[identity].attempt_count += 1

    def record_touch_outbox_failure(self, identity, error, next_attempt_at):
        if self.failure == "failure":
            raise sqlite3.OperationalError("failure persistence unavailable")
        row = self.rows[identity]
        row.last_error, row.next_attempt_at = error, next_attempt_at

    def mark_touch_outbox_sent(self, identity, sent_at, message_id=None):
        if self.failure == "ack":
            raise sqlite3.OperationalError("acceptance persistence unavailable")
        row = self.rows[identity]
        row.sent_at, row.message_id, row.last_error = sent_at, message_id, None

    def mute_touch_outbox(self, identity, reason="expired"):
        self.rows[identity].muted = True


class TouchEmailTests(unittest.TestCase):
    def setUp(self):
        self.clock = [START]
        self.store = MemoryOutbox(activation())
        self.client = MagicMock()
        self.client.send_email.return_value = {"MessageId": "ses-accepted"}
        self.dispatcher = TouchEmailDispatcher(
            "unused", SETTINGS, retry_seconds=3, max_age_seconds=30,
            client=self.client, store_factory=lambda path: self.store,
            now=lambda: self.clock[0])

    def test_single_message_uses_touch_time_sofia_utc_and_exact_price(self):
        subject, body = touch_email_content(activation())
        self.assertIn("FILUSDT", subject)
        self.assertIn("high", subject)
        self.assertIn("2026-10-13 15:00:00 EEST", body)
        self.assertIn("2026-10-13T12:00:00Z", body)
        self.assertIn("1.231000000000000001 USDT", body)
        self.assertIn("Activation ID: activation-1", body)
        self.assertNotIn("confirmed:", body)

    def test_each_named_level_and_side_is_a_separate_message(self):
        self.store.add(activation("q-high", level="previous_quarter"))
        self.store.add(activation("m-low", side="low"))
        self.assertEqual(self.dispatcher.send_pending(), 3)
        self.assertEqual(self.client.send_email.call_count, 3)
        bodies = [call.kwargs["Content"]["Simple"]["Body"]["Text"]["Data"]
                  for call in self.client.send_email.call_args_list]
        for identity in self.store.rows:
            self.assertEqual(sum(f"Activation ID: {identity}" in body for body in bodies), 1)
        self.assertEqual(self.dispatcher.send_pending(), 0)
        self.client.close.assert_not_called()

    def test_due_retry_uses_fixed_delay_without_extending_event_freshness(self):
        self.client.send_email.side_effect = TimeoutError("secret request details")
        with self.assertLogs("crypto_monitor.adapters.touch_email", level="WARNING") as logs:
            self.assertEqual(self.dispatcher.send_pending(), 0)
        self.assertNotIn("secret", "\n".join(logs.output))
        row = self.store.rows["activation-1"]
        self.assertEqual(row.attempt_count, 1)
        self.assertEqual(row.last_error, "TimeoutError")
        self.assertEqual(row.next_attempt_at, START + timedelta(seconds=3))
        self.clock[0] += timedelta(seconds=2)
        self.assertEqual(self.dispatcher.send_pending(), 0)
        self.client.send_email.assert_called_once()
        self.clock[0] += timedelta(seconds=1)
        self.client.send_email.side_effect = None
        self.assertEqual(self.dispatcher.send_pending(), 1)
        self.assertEqual(row.attempt_count, 2)
        self.assertIsNone(self.dispatcher.status.last_error)

    def test_expired_and_historical_rows_cannot_send(self):
        self.clock[0] += timedelta(seconds=30)
        self.store.add(activation("history", touched_at=self.clock[0], origin="history"))
        self.assertEqual(self.dispatcher.send_pending(), 0)
        self.assertTrue(all(row.muted for row in self.store.rows.values()))
        self.client.send_email.assert_not_called()

    def test_expiry_removes_failed_rows_even_if_their_retry_is_not_due(self):
        self.client.send_email.side_effect = TimeoutError()
        self.dispatcher.retry_delay = timedelta(seconds=60)
        with self.assertLogs("crypto_monitor.adapters.touch_email", level="WARNING"):
            self.dispatcher.send_pending()
        self.clock[0] += timedelta(seconds=30)
        self.assertEqual(self.dispatcher.send_pending(), 0)
        self.assertTrue(self.store.rows["activation-1"].muted)
        self.client.send_email.assert_called_once()

    def test_provider_errors_are_sanitized(self):
        error = RuntimeError("signed credentials")
        error.response = {"Error": {"Code": "MessageRejected", "Message": "secret"}}
        self.client.send_email.side_effect = error
        with self.assertLogs("crypto_monitor.adapters.touch_email", level="WARNING") as logs:
            self.dispatcher.send_pending()
        self.assertEqual(self.store.rows["activation-1"].last_error, "SES MessageRejected")
        self.assertNotIn("secret", "\n".join(logs.output))

    def test_database_errors_surface_instead_of_becoming_provider_failures(self):
        for failure in ("load", "begin", "ack"):
            with self.subTest(failure=failure):
                self.store.failure = failure
                with self.assertRaises(sqlite3.OperationalError):
                    self.dispatcher.send_pending()
        self.store.failure = "failure"
        self.client.send_email.side_effect = TimeoutError()
        with self.assertRaises(sqlite3.OperationalError):
            self.dispatcher.send_pending()

    def test_ambiguous_response_remains_pending_and_keeps_stable_identity(self):
        self.client.send_email.return_value = {}
        with self.assertLogs("crypto_monitor.adapters.touch_email", level="WARNING"):
            self.assertEqual(self.dispatcher.send_pending(), 0)
        first = self.client.send_email.call_args
        self.clock[0] += timedelta(seconds=3)
        self.client.send_email.return_value = {"MessageId": "ses-accepted"}
        self.assertEqual(self.dispatcher.send_pending(), 1)
        self.assertEqual(first, self.client.send_email.call_args)

    def test_owned_client_closes_after_send_and_failure(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                client = MagicMock()
                client.send_email.return_value = {"MessageId": "ses-accepted"}
                client.send_email.side_effect = TimeoutError() if fail else None
                store = MemoryOutbox(activation())
                sender = TouchEmailDispatcher(
                    "unused", SETTINGS, retry_seconds=3, max_age_seconds=30,
                    client_factory=lambda region: client, store_factory=lambda path: store,
                    now=lambda: START)
                with self.assertLogs("crypto_monitor.adapters.touch_email", level="INFO"):
                    sender.send_pending()
                client.close.assert_called_once()

    def test_disabled_sender_does_no_database_or_provider_work(self):
        factory = MagicMock(side_effect=AssertionError("must not perform I/O"))
        sender = TouchEmailDispatcher(
            "unused", EmailSettings(), retry_seconds=3, max_age_seconds=30,
            client_factory=factory, store_factory=factory)
        self.assertEqual(sender.send_pending(), 0)
        factory.assert_not_called()

    def test_required_positive_finite_timing_values(self):
        for value in (0, -1, True, float("inf"), float("nan"), "", None):
            for name in ("retry_seconds", "max_age_seconds"):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    kwargs = {"retry_seconds": 3, "max_age_seconds": 30, name: value}
                    TouchEmailDispatcher("unused", SETTINGS, **kwargs)

    def test_stop_request_finishes_current_send_and_skips_remaining_batch(self):
        self.store.add(activation("second"))

        def accepted(**kwargs):
            self.dispatcher.request_stop()
            return {"MessageId": "accepted-before-disable"}

        self.client.send_email.side_effect = accepted
        self.assertEqual(self.dispatcher.send_pending(), 1)
        self.client.send_email.assert_called_once()
        self.assertEqual(self.store.rows["activation-1"].message_id, "accepted-before-disable")
        self.assertIsNone(self.store.rows["second"].sent_at)


class AsyncTouchEmailTests(unittest.IsolatedAsyncioTestCase):
    async def test_notify_sends_without_a_digest_boundary(self):
        store, client, sent = MemoryOutbox(), MagicMock(), asyncio.Event()
        loop = asyncio.get_running_loop()

        def accepted(**kwargs):
            loop.call_soon_threadsafe(sent.set)
            return {"MessageId": "accepted-now"}

        client.send_email.side_effect = accepted
        sender = TouchEmailDispatcher("unused", SETTINGS, retry_seconds=3, max_age_seconds=30,
                                      client=client, store_factory=lambda path: store, now=lambda: START)
        stop = asyncio.Event()
        task = asyncio.create_task(sender.run(stop))
        try:
            # Allow the first empty dispatch to complete before notifying it.
            await asyncio.sleep(0.02)
            store.add(activation())
            sender.notify()
            await asyncio.wait_for(sent.wait(), 0.3)
            stop.set()
            await asyncio.wait_for(task, 2)
            self.assertEqual(store.rows["activation-1"].message_id, "accepted-now")
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_finishes_current_send_and_skips_remaining(self):
        store = MemoryOutbox(activation(), activation("second"))
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()

        def accepted(**kwargs):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("test release missing")
            return {"MessageId": "accepted-during-stop"}

        client = MagicMock()
        client.send_email.side_effect = accepted
        sender = TouchEmailDispatcher("unused", SETTINGS, retry_seconds=3, max_age_seconds=30,
                                      client=client, store_factory=lambda path: store, now=lambda: START)
        task = asyncio.create_task(sender.run(asyncio.Event()))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(store.rows["activation-1"].message_id, "accepted-during-stop")
            self.assertIsNone(store.rows["second"].message_id)
            self.assertEqual(client.send_email.call_count, 1)
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_disabled_run_only_waits_for_stop(self):
        factory = MagicMock(side_effect=AssertionError("no I/O while disabled"))
        sender = TouchEmailDispatcher("unused", EmailSettings(), retry_seconds=3, max_age_seconds=30,
                                      client_factory=factory, store_factory=factory)
        stop = asyncio.Event()
        task = asyncio.create_task(sender.run(stop))
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        stop.set()
        await task
        factory.assert_not_called()


class TouchSQLiteEmailTests(unittest.TestCase):
    def test_dispatcher_uses_real_touch_outbox_and_old_run_pending_is_muted(self):
        delays = {name: Decimal("5") for name in
                  ("previous_week", "previous_month", "previous_quarter")}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "touch.sqlite3"
            with TouchStore(path) as store:
                periods = reference_periods(START)
                refs = {name: ReferenceLevels(periods[name], Levels(Decimal("100"), Decimal("90")))
                        for name in ("previous_month", "previous_quarter")}
                store.initialize_watches("FILUSDT", refs, START)
                for watch in store.history_jobs("FILUSDT"):
                    if watch.level_price is not None:
                        store.advance_history(watch, START)
                        store.mark_history_complete("FILUSDT", watch.name, watch.source_start, START)
                quiet = store.record_price("FILUSDT", Decimal("90"), START, delays,
                                           origin="history", notify=True)
                self.assertEqual(len(quiet), 2)
                self.assertEqual(store.load_touch_outbox(START), ())
                live = store.record_price("FILUSDT", Decimal("100"), START, delays,
                                          notify=True)
                self.assertEqual(len(live), 2)
            client = MagicMock()
            client.send_email.return_value = {"MessageId": "real-queue-accepted"}
            sender = TouchEmailDispatcher(path, SETTINGS, retry_seconds=3, max_age_seconds=30,
                                          client=client, now=lambda: START, batch_size=1)
            self.assertEqual(sender.send_pending(), 1)
            with TouchStore(path) as store:
                self.assertEqual(len(store.load_touch_outbox(START)), 1)
                store.begin_run(START + timedelta(seconds=1))
            self.assertEqual(sender.send_pending(), 0)
            client.send_email.assert_called_once()


if __name__ == "__main__":
    unittest.main()
