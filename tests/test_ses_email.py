"""Offline outbox and SES tests: real temporary SQLite, injected mock clients."""

import asyncio
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from crypto_monitor.adapters.ses_email import (EmailDispatcher, EmailSettings, create_ses_client, digest_content,
                       next_digest_time, retry_delay)
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from tests.test_sqlite_store import START, D, event, hits_for
from tests.test_websocket_supervisor import ManualSleep


SETTINGS = EmailSettings(True, "monitor@example.com", "recipient@example.com", "eu-west-1")


class OutboxTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "outbox.sqlite3"
        self.store = SQLiteStore(self.path)
        self.addCleanup(self.store.close)

    def test_outbox_is_atomic_idempotent_and_survives_reopen(self):
        current, hits = event(), hits_for(event())
        self.store.commit_candle(current, hits + hits)
        self.assertFalse(self.store.commit_candle(current, hits))
        self.store.commit_candle(event(1), [])
        with SQLiteStore(self.path) as reopened:
            rows = reopened.load_outbox()
            self.assertEqual([row.hit for row in rows], sorted(hits, key=lambda hit: hit.alert_id))
            self.assertEqual([row.attempt_count for row in rows], [0] * 4)
            self.assertTrue(all(row.sent_at is None and row.message_id is None for row in rows))

    def test_later_invalid_hit_rolls_back_candle_hits_and_outbox(self):
        hits = hits_for(event())
        with self.assertRaises(ValueError):
            self.store.commit_candle(event(), [hits[0], replace(hits[1], side="bad")])
        self.assertEqual(self.store.load_baselines(), {})
        self.assertEqual(self.store.load_hits(), [])
        self.assertEqual(self.store.load_outbox(), [])

    def test_outbox_insert_failure_rolls_back_candle_and_hits(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TRIGGER reject_outbox BEFORE INSERT ON email_outbox
                          BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "outbox unavailable"):
            self.store.commit_candle(event(), hits_for(event()))
        self.assertEqual(self.store.load_baselines(), {})
        self.assertEqual(self.store.load_hits(), [])
        self.assertEqual(self.store.load_outbox(), [])

    def test_existing_database_migration_does_not_requeue_old_hits(self):
        self.store.commit_candle(event(), hits_for(event()))
        self.store.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DROP TABLE email_outbox")
        with SQLiteStore(self.path) as migrated:
            self.assertEqual(len(migrated.load_hits()), 4)
            self.assertEqual(migrated.load_outbox(), [])
            migrated.commit_candle(event(1), hits_for(event(1)))
            self.assertEqual(migrated.load_outbox(), [])  # Old levels stay claimed.
            migrated.commit_candle(event(1, "ETHUSDT"), hits_for(event(1, "ETHUSDT")))
            self.assertEqual(len(migrated.load_outbox()), 4)

    def test_invalid_acknowledgement_rolls_back_entire_digest(self):
        self.store.commit_candle(event(), hits_for(event()))
        ids = [row.hit.alert_id for row in self.store.load_outbox()]
        self.store.begin_email_attempt(ids, now=START, next_attempt_at=START + 5 * timedelta(minutes=1))
        with self.assertRaises(ValueError):
            self.store.mark_email_sent(ids + ["missing"], message_id="ses-id", sent_at=START)
        self.assertEqual(len(self.store.load_outbox()), 4)
        self.assertTrue(all(row.message_id is None for row in self.store.load_outbox()))


class DigestTests(unittest.TestCase):
    def test_order_content_exact_decimals_and_utc_confirmation(self):
        low = replace(hits_for(event())[0], captured_quote_volume=D("9.123456789012345678901"))
        newest = replace(hits_for(event(2, "ETHUSDT"))[0], side="low", level_price=D("90.00000000000000001"),
                         captured_quote_volume=D("10"))
        older = replace(hits_for(event(1))[0], captured_quote_volume=D("10"))
        tie = replace(older, alert_id="zzzz-tie")
        subject, body = digest_content([low, tie, newest, older])
        self.assertEqual(subject, "Crypto level sweeps: 4 alerts")
        self.assertLess(body.index(newest.alert_id), body.index(older.alert_id))
        self.assertLess(body.index(older.alert_id), body.index(tie.alert_id))
        self.assertLess(body.index(tie.alert_id), body.index(low.alert_id))
        for expected in ["1. ETHUSDT", newest.level_name + " low sweep",
                         "level price: 90.00000000000000001 USDT", "confirmed: 2026-10-09T12:03:00Z",
                         "captured 24h quote volume: 9.123456789012345678901 USDT", "observed:"]:
            self.assertIn(expected, body)

    def test_boundaries_and_bounded_backoff(self):
        self.assertEqual(next_digest_time(START), START + timedelta(minutes=5))
        self.assertEqual(next_digest_time(START.replace(hour=23, minute=59, second=59)),
                         START.replace(day=10, hour=0))
        with self.assertRaises(ValueError):
            next_digest_time(START.replace(tzinfo=None))
        self.assertEqual([retry_delay(n).total_seconds() for n in range(1, 7)],
                         [300, 600, 1200, 2400, 3600, 3600])

    def test_default_client_uses_provider_chain_finite_timeouts_and_no_sdk_retries(self):
        # Mock entire lazy imports: no installed SDK or credential lookup is needed.
        boto, config = MagicMock(), MagicMock()
        with patch.dict("sys.modules", {"boto3": boto, "botocore": MagicMock(),
                                       "botocore.config": SimpleNamespace(Config=config)}):
            self.assertIs(create_ses_client("eu-west-1"), boto.client.return_value)
        boto.client.assert_called_once_with("sesv2", region_name="eu-west-1", config=config.return_value)
        config.assert_called_once_with(connect_timeout=5, read_timeout=15,
                                      retries={"mode": "standard", "total_max_attempts": 1})


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "delivery.sqlite3"
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(), hits_for(event()))
        self.clock = [START + timedelta(minutes=5)]
        self.client = MagicMock()
        self.client.send_email.return_value = {"MessageId": "ses-message-123"}
        self.dispatcher = EmailDispatcher(self.path, SETTINGS, client=self.client, now=lambda: self.clock[0])

    def rows(self, include_sent=False):
        with SQLiteStore(self.path) as store:
            return store.load_outbox(include_sent=include_sent)

    def test_success_stores_message_id_for_exact_batch_and_preserves_new_hits(self):
        def accepted(**kwargs):
            with SQLiteStore(self.path) as store:
                store.commit_candle(event(1, "ETHUSDT"), hits_for(event(1, "ETHUSDT")))
            return {"MessageId": "ses-message-123"}
        self.client.send_email.side_effect = accepted
        self.assertEqual(self.dispatcher.send_pending(), 4)
        request = self.client.send_email.call_args.kwargs
        self.assertEqual(request["FromEmailAddress"], SETTINGS.sender)
        self.assertEqual(request["Destination"], {"ToAddresses": [SETTINGS.recipient]})
        self.assertEqual(request["Content"]["Simple"]["Body"]["Text"]["Charset"], "UTF-8")
        self.assertEqual(len(self.rows()), 4)
        sent = [row for row in self.rows(True) if row.sent_at is not None]
        self.assertEqual(len(sent), 4)
        self.assertTrue(all(row.message_id == "ses-message-123" and row.attempt_count == 1
                            and row.last_error is None for row in sent))
        self.client.close.assert_not_called()  # Caller owns injected clients.

    def test_sent_rows_are_never_resent_and_empty_queue_does_not_call_ses(self):
        self.assertEqual(self.dispatcher.send_pending(), 4)
        self.assertEqual(self.dispatcher.send_pending(), 0)
        self.client.send_email.assert_called_once()

    def test_owned_client_is_closed_after_success_or_failure(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                self.client.send_email.side_effect = TimeoutError() if fail else None
                # Keep each case's independent outbox pending until its own send.
                with SQLiteStore(self.path) as store:
                    current = event(1 if fail else 2, "ETHUSDT" if fail else "BTCUSDT")
                    store.commit_candle(current, hits_for(current))
                sender = EmailDispatcher(self.path, SETTINGS, client_factory=lambda region: self.client,
                                         now=lambda: self.clock[0])
                with patch("crypto_monitor.adapters.ses_email.LOG"):
                    sender.send_pending()
                self.assertEqual(self.client.close.call_count, 2 if fail else 1)

    def test_ses_error_code_is_recorded_without_raw_error_text(self):
        error = RuntimeError("raw signed request or credentials must not be copied")
        error.response = {"Error": {"Code": "MessageRejected", "Message": "sensitive details"}}
        self.client.send_email.side_effect = error
        with self.assertLogs("crypto_monitor.adapters.ses_email", level="WARNING") as logs:
            self.assertEqual(self.dispatcher.send_pending(), 0)
        self.assertTrue(all(row.last_error == "SES MessageRejected" for row in self.rows()))
        self.assertNotIn("sensitive", "\n".join(logs.output))

    def test_failure_retry_and_restart_preserve_attempts_and_deadlines(self):
        self.client.send_email.side_effect = TimeoutError("sensitive SDK request text must not persist")
        with self.assertLogs("crypto_monitor.adapters.ses_email", level="WARNING") as logs:
            self.assertEqual(self.dispatcher.send_pending(), 0)
        self.assertNotIn("sensitive", "\n".join(logs.output))
        self.assertTrue(all(row.attempt_count == 1 and row.last_error == "TimeoutError"
                            and row.message_id is None for row in self.rows()))
        restarted = EmailDispatcher(self.path, SETTINGS, client=self.client, now=lambda: self.clock[0])
        self.clock[0] += timedelta(minutes=4, seconds=59)
        self.assertEqual(restarted.send_pending(), 0)
        self.client.send_email.assert_called_once()
        self.clock[0] += timedelta(seconds=1)
        with self.assertLogs("crypto_monitor.adapters.ses_email", level="WARNING"):
            self.assertEqual(restarted.send_pending(), 0)
        self.assertTrue(all(row.attempt_count == 2 and row.next_attempt_at == self.clock[0] + timedelta(minutes=10)
                            for row in self.rows()))
        self.client.send_email.side_effect = None
        self.clock[0] += timedelta(minutes=10)
        self.assertEqual(restarted.send_pending(), 4)
        self.assertTrue(all(row.attempt_count == 3 for row in self.rows(True)))

    def test_persisted_attempt_before_send_survives_ambiguous_lost_response(self):
        accepted = []
        def lost(**kwargs):
            self.assertTrue(all(row.attempt_count == 1 for row in self.rows()))
            accepted.append(kwargs)
            raise TimeoutError("SES accepted, response lost")
        self.client.send_email.side_effect = lost
        with self.assertLogs("crypto_monitor.adapters.ses_email", level="WARNING"):
            self.dispatcher.send_pending()
        self.clock[0] += timedelta(minutes=5)
        self.client.send_email.side_effect = None
        self.assertEqual(self.dispatcher.send_pending(), 4)
        self.assertEqual(accepted[0], self.client.send_email.call_args.kwargs)
        self.assertEqual(self.client.send_email.call_count, 2)  # At least once, not exactly once.

    def test_failed_local_acknowledgement_stays_pending_and_surfaces(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TRIGGER reject_ack BEFORE UPDATE OF sent_at ON email_outbox
                          BEGIN SELECT RAISE(ABORT, 'ack failed'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "ack failed"):
            self.dispatcher.send_pending()
        self.assertTrue(all(row.sent_at is None and row.attempt_count == 1 for row in self.rows()))
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DROP TRIGGER reject_ack")
        self.clock[0] += timedelta(minutes=5)
        self.assertEqual(self.dispatcher.send_pending(), 4)

    def test_malformed_response_and_client_creation_failure_keep_pending(self):
        for response in (None, {}, {"MessageId": ""}):
            self.client.send_email.return_value = response
            with self.assertLogs("crypto_monitor.adapters.ses_email", level="WARNING"):
                self.assertEqual(self.dispatcher.send_pending(), 0)
            self.assertEqual(len(self.rows()), 4)
            self.clock[0] += timedelta(hours=1)
        factory = MagicMock(side_effect=RuntimeError("secret details"))
        sender = EmailDispatcher(self.path, SETTINGS, client_factory=factory, now=lambda: self.clock[0])
        with self.assertLogs("crypto_monitor.adapters.ses_email", level="WARNING"):
            self.assertEqual(sender.send_pending(), 0)
        self.assertTrue(all(row.last_error == "RuntimeError" for row in self.rows()))

    def test_bounded_batch_keeps_remaining_alerts_pending(self):
        self.dispatcher.batch_size = 2
        self.assertEqual(self.dispatcher.send_pending(), 2)
        self.assertEqual(len(self.rows()), 2)

    def test_disabled_has_no_database_or_aws_side_effects_even_when_configured(self):
        for settings in (EmailSettings(), EmailSettings(False, "partial"), replace(SETTINGS, enabled=False)):
            store, client = MagicMock(side_effect=AssertionError("database")), MagicMock(side_effect=AssertionError("AWS"))
            sender = EmailDispatcher(self.path, settings, store_factory=store, client_factory=client)
            self.assertEqual(sender.send_pending(), 0)
            store.assert_not_called()
            client.assert_not_called()
        self.assertEqual(len(self.rows()), 4)

    def test_enabled_incomplete_or_invalid_configuration_rejected(self):
        for fields in ({}, {"sender": "a@example.com"}, {"sender": "a@example.com", "recipient": "b@example.com"},
                       {"sender": "a@example.com\nBcc: x@y.com", "recipient": "b@example.com", "region": "eu-west-1"}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                EmailSettings(enabled=True, **fields)


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_five_minute_schedule_and_clean_stop_without_real_sends(self):
        clock, timer, stop = [START], ManualSleep(), asyncio.Event()
        sender = EmailDispatcher("unused", SETTINGS, now=lambda: clock[0], sleep=timer)
        sender.send_pending = MagicMock(return_value=0)
        task = asyncio.create_task(sender.run(stop))
        try:
            delay, wake = await timer.next()
            self.assertEqual(delay, 300)
            sender.send_pending.assert_not_called()
            clock[0] += timedelta(minutes=5)
            wake.set_result(None)
            delay, _ = await timer.next()
            self.assertEqual(delay, 300)
            sender.send_pending.assert_called_once()
            stop.set()
            await asyncio.wait_for(task, 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_waits_for_send_and_persists_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cancel.sqlite3"
            with SQLiteStore(path) as store:
                store.commit_candle(event(), hits_for(event()))
            clock, timer = [START], ManualSleep()
            entered, release = asyncio.Event(), threading.Event()
            loop = asyncio.get_running_loop()
            def send(**kwargs):
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(3):
                    raise TimeoutError("test release missing")
                return {"MessageId": "accepted-during-shutdown"}
            client = MagicMock()
            client.send_email.side_effect = send
            sender = EmailDispatcher(path, SETTINGS, client=client, now=lambda: clock[0], sleep=timer)
            task = asyncio.create_task(sender.run(asyncio.Event()))
            try:
                _, wake = await timer.next()
                clock[0] += timedelta(minutes=5)
                wake.set_result(None)
                await asyncio.wait_for(entered.wait(), 2)
                task.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                with SQLiteStore(path) as store:
                    self.assertEqual(store.load_outbox(), [])
                    self.assertTrue(all(row.message_id == "accepted-during-shutdown"
                                        for row in store.load_outbox(include_sent=True)))
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
