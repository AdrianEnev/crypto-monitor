"""Prompt, separate SES emails for fresh live level touches.

The touch store queues only notifications enabled when a live activation was
accepted. Startup mutes mail from earlier runs; failed sends may retry only while
their original live event remains fresh. SES acceptance is at least once: a lost
response or failed database acknowledgement can still cause a duplicate email.
"""

import asyncio
import logging
import math
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from crypto_monitor.adapters.ses_email import (
    DeliveryStatus, EmailSettings, InvalidSESResponse, create_ses_client,
    error_category, utc_text,
)
from crypto_monitor.persistence.touch_store import TouchStore


LOG = logging.getLogger(__name__)
SOFIA = ZoneInfo("Europe/Sofia")


def touch_email_content(activation):
    """One named level and one side per message, preserving exact prices."""
    touched = activation.touched_at
    utc_text(touched)
    local = touched.astimezone(SOFIA).strftime("%Y-%m-%d %H:%M:%S %Z")
    subject = f"Crypto level touch: {activation.symbol} {activation.level_name} {activation.side}"
    body = "\n".join([
        f"{activation.symbol}: {activation.level_name} {activation.side} touched",
        "",
        f"Level price: {activation.level_price} USDT",
        f"Touched at: {local} (Europe/Sofia)",
        f"UTC time: {utc_text(touched)}",
        f"Reference start: {utc_text(activation.reference_period_start)}",
        f"Captured 24h quote volume: {activation.captured_quote_volume} USDT",
        f"Activation ID: {activation.activation_id}",
        "",
        "Price reached or passed this level. No candle close was required.",
    ])
    return subject, body


class TouchEmailDispatcher:
    """Bounded separate sends, woken immediately after live activations.

    SDK work runs in a thread. A stop or cancellation finishes the current send
    and its bookkeeping, then skips the rest of the batch. Database errors
    propagate; provider errors are recorded without copying sensitive messages.
    """

    def __init__(self, database, settings: EmailSettings, *, retry_seconds,
                 max_age_seconds, client=None, client_factory=create_ses_client,
                 store_factory=TouchStore, now=lambda: datetime.now(timezone.utc),
                 batch_size=100):
        for label, value in (("retry_seconds", retry_seconds),
                             ("max_age_seconds", max_age_seconds)):
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"{label} must be a positive finite number") from exc
            if isinstance(value, bool) or not math.isfinite(number) or number <= 0:
                raise ValueError(f"{label} must be a positive finite number")
        if type(batch_size) is not int or not 1 <= batch_size <= 100:
            raise ValueError("email batch size must be between 1 and 100")
        self.database, self.settings = database, settings
        self.retry_delay = timedelta(seconds=float(retry_seconds))
        self.max_age = timedelta(seconds=float(max_age_seconds))
        self.client, self.client_factory, self.store_factory = client, client_factory, store_factory
        self.now, self.batch_size = now, batch_size
        self.status = DeliveryStatus()
        self.wake = asyncio.Event()
        self._shutdown_requested = threading.Event()
        self._running = False

    def notify(self):
        """Called on the event loop after a fresh notification is committed."""
        self.wake.set()

    def send_pending(self, *, max_count=None):
        """Send each due live activation separately; return accepted count."""
        if not self.settings.enabled:
            return 0
        now = self.now()
        utc_text(now)
        with self.store_factory(self.database) as store:
            store.expire_touch_outbox(now - self.max_age)
            entries = store.load_touch_outbox(
                now, limit=self.batch_size if max_count is None else min(max_count, self.batch_size))

        client, owned = self.client, self.client is None
        accepted, failed = 0, False
        try:
            for entry in entries:
                if self._shutdown_requested.is_set():
                    break
                activation = entry.activation
                attempted_at = self.now()
                utc_text(attempted_at)
                if activation.origin != "live" or attempted_at - activation.touched_at >= self.max_age:
                    with self.store_factory(self.database) as store:
                        store.mute_touch_outbox(activation.activation_id, reason="expired")
                    continue
                subject, body = touch_email_content(activation)
                with self.store_factory(self.database) as store:
                    store.begin_touch_outbox_attempt(activation.activation_id, attempted_at)
                try:
                    if client is None:
                        client = self.client_factory(self.settings.region)
                    response = client.send_email(
                        FromEmailAddress=self.settings.sender,
                        Destination={"ToAddresses": [self.settings.recipient]},
                        Content={"Simple": {
                            "Subject": {"Data": subject, "Charset": "UTF-8"},
                            "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
                        }})
                    message_id = response.get("MessageId") if isinstance(response, dict) else None
                    if not isinstance(message_id, str) or not message_id.strip():
                        raise InvalidSESResponse("SES acceptance response lacks MessageId")
                except Exception as exc:
                    error = error_category(exc)
                    failure_at = self.now()
                    with self.store_factory(self.database) as store:
                        store.record_touch_outbox_failure(
                            activation.activation_id, error, failure_at + self.retry_delay)
                    self.status = DeliveryStatus(error, failure_at)
                    LOG.warning("touch email pending symbol=%s level=%s side=%s attempt=%s error=%s",
                                activation.symbol, activation.level_name, activation.side,
                                entry.attempt_count + 1, error)
                    failed = True
                    continue
                # A failed local acknowledgement must escape the provider catch.
                with self.store_factory(self.database) as store:
                    store.mark_touch_outbox_sent(
                        activation.activation_id, sent_at=self.now(), message_id=message_id)
                accepted += 1
                LOG.info("touch email accepted symbol=%s level=%s side=%s",
                         activation.symbol, activation.level_name, activation.side)
            if accepted and not failed:
                self.status = DeliveryStatus()
            return accepted
        finally:
            if owned and client is not None:
                try:
                    client.close()
                except Exception as exc:
                    LOG.warning("touch email client cleanup error=%s", error_category(exc))

    async def _dispatch(self, stop):
        # One send per async pass gives shutdown a chance between messages.
        work = asyncio.create_task(asyncio.to_thread(self.send_pending, max_count=1))
        stopping = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait((work, stopping), return_when=asyncio.FIRST_COMPLETED)
            if stopping in done:
                self._shutdown_requested.set()
            return await asyncio.shield(work)
        finally:
            if not work.done():
                self._shutdown_requested.set()
            if not stopping.done():
                stopping.cancel()
            # Never abandon an accepted send before its database acknowledgement.
            await asyncio.gather(work, stopping, return_exceptions=True)

    async def run(self, stop):
        if not self.settings.enabled:
            await stop.wait()
            return
        if self._running:
            raise RuntimeError("touch email dispatcher is already running")
        self._running = True
        self._shutdown_requested.clear()
        try:
            while not stop.is_set():
                # A notify during the thread's work stays set for the next pass.
                self.wake.clear()
                accepted = await self._dispatch(stop)
                if stop.is_set():
                    return
                if accepted or self.wake.is_set():
                    continue
                waking = asyncio.create_task(self.wake.wait())
                stopping = asyncio.create_task(stop.wait())
                polling = asyncio.create_task(asyncio.sleep(0.5))
                try:
                    await asyncio.wait((waking, stopping, polling), return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in (waking, stopping, polling):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(waking, stopping, polling, return_exceptions=True)
        finally:
            self._shutdown_requested.set()
            self._running = False
