"""Prompt, separate SES emails for fresh live level touches.

The touch store queues only notifications enabled when a live activation was
accepted. Startup mutes mail from earlier runs; failed sends may retry only while
their original live event remains fresh. SES acceptance is at least once: a lost
response or failed database acknowledgement can still cause a duplicate email.
"""

import asyncio
import html
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


LEVEL_NAMES = {"monday": "Monday", "previous_week": "Previous week",
               "previous_month": "Previous month", "previous_quarter": "Previous quarter"}


def _day(value):
    return f"{value.day} {value:%b %Y}"


def _reference(name, start):
    """Readable source period, from its UTC start."""
    if name == "previous_month":
        return f"{start:%B %Y}"
    if name == "previous_quarter":
        return f"Q{(start.month - 1) // 3 + 1} {start.year}"
    if name == "monday":
        return f"Monday {_day(start)}"
    last = start + timedelta(days=6)
    if start.year != last.year:
        return f"{_day(start)} – {_day(last)}"
    if start.month != last.month:
        return f"{start.day} {start:%b} – {_day(last)}"
    return f"{start.day}–{_day(last)}"


def _price(value):
    # Fixed notation keeps every exchange digit without scientific exponents.
    return format(value, "f")


def _market(symbol):
    base = symbol.removesuffix("USDT")
    return f"{base} / USDT" if base and base != symbol else symbol


def touch_email_content(activation):
    """Subject, plain text and HTML for one named level and side.

    Times are shown in Sofia local time, including daylight-saving changes.
    """
    touched = activation.touched_at
    utc_text(touched)
    local = touched.astimezone(SOFIA)
    touched_text = f"{_day(local)}, {local:%H:%M:%S}"
    level = f"{LEVEL_NAMES.get(activation.level_name, activation.level_name)} {activation.side}"
    reference = _reference(activation.level_name, activation.reference_period_start)
    price = _price(activation.level_price)
    subject = f"{activation.symbol} · {level} touched"
    text = "\n".join([
        f"{activation.symbol} · {level} touched",
        "",
        f"Level price: {price} USDT",
        f"Reference: {reference}",
        f"Touched at (Sofia time): {touched_text}",
    ])
    e = html.escape
    body = f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(subject)}</title></head>
<body style="margin:0;padding:0;background:#f1f4f4;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;background:#f1f4f4;">
<tr><td align="center" style="padding:32px 12px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:560px;border-collapse:separate;background:#ffffff;border:1px solid #e0e8e5;border-radius:18px;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
<tr><td height="5" style="height:5px;background:#139583;font-size:0;line-height:0;">&nbsp;</td></tr>
<tr><td style="padding:34px 36px 26px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">
<tr><td style="color:#52716a;font-size:11px;line-height:16px;font-weight:700;letter-spacing:2.5px;">CRYPTO MONITOR</td>
<td align="right" style="color:#148675;font-size:11px;line-height:16px;font-weight:600;">LEVEL TOUCH</td></tr>
</table>
<h1 style="margin:30px 0 8px;color:#153a31;font-size:27px;line-height:1.25;font-weight:650;letter-spacing:-0.7px;">{e(_market(activation.symbol))} <span style="color:#82948e;font-size:15px;font-weight:450;letter-spacing:0;white-space:nowrap;">Futures</span></h1>
<p style="margin:0;color:#647b73;font-size:16px;line-height:1.6;">{e(level)} touched</p>
</td></tr>
<tr><td style="padding:0 36px 30px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:separate;background:#f1f8f5;border:1px solid #e0eee7;border-radius:12px;">
<tr><td style="padding:22px 24px 24px;">
<p style="margin:0 0 8px;color:#668275;font-size:11px;line-height:17px;font-weight:650;letter-spacing:1.4px;text-transform:uppercase;">Level price</p>
<p style="margin:0;color:#105a46;font-size:18px;line-height:1.3;word-break:break-all;"><span style="font-size:40px;font-weight:650;letter-spacing:-1.5px;">{e(price)}</span> <span style="color:#749384;font-size:15px;font-weight:550;letter-spacing:.5px;">USDT</span></p>
</td></tr>
</table>
</td></tr>
<tr><td style="padding:0 36px 34px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;font-size:14px;line-height:1.6;">
<tr><td style="padding:0 0 16px;color:#789087;vertical-align:top;">Reference</td>
<td align="right" style="padding:0 0 16px;color:#294d3f;font-weight:550;vertical-align:top;">{e(reference)}</td></tr>
<tr><td colspan="2" height="1" style="height:1px;background:#edf1ee;font-size:0;line-height:0;">&nbsp;</td></tr>
<tr><td style="padding:16px 0 0;color:#789087;vertical-align:top;">Touched at<span style="display:block;color:#9aa9a1;font-size:11px;">Sofia time</span></td>
<td align="right" style="padding:16px 0 0;color:#294d3f;font-weight:550;vertical-align:top;">{e(touched_text)}</td></tr>
</table>
</td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""
    return subject, text, body


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

    def request_stop(self):
        """Prevent another send while allowing an in-flight send to be saved."""
        self._shutdown_requested.set()

    def send_pending(self, *, max_count=None):
        """Send each due live activation separately; return accepted count."""
        if not self.settings.enabled:
            return 0
        now = self.now()
        utc_text(now)
        with self.store_factory(self.database) as store:
            store.expire_touch_outbox(now - self.max_age, after=now + self.max_age)
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
                age = attempted_at - activation.touched_at
                # A large negative age means the clock moved backward; the
                # event's real age is unknown, so it is not sent as fresh.
                if activation.origin != "live" or not -self.max_age < age < self.max_age:
                    with self.store_factory(self.database) as store:
                        store.mute_touch_outbox(activation.activation_id, reason="expired")
                    continue
                subject, text, body = touch_email_content(activation)
                with self.store_factory(self.database) as store:
                    # The claim fails if another sender already took this row.
                    # Its lease lasts for the event's freshness window.
                    claimed = store.begin_touch_outbox_attempt(
                        activation.activation_id, attempted_at,
                        expected_attempts=entry.attempt_count,
                        lease_until=activation.touched_at + self.max_age)
                if not claimed:
                    continue
                try:
                    if client is None:
                        client = self.client_factory(self.settings.region)
                    response = client.send_email(
                        FromEmailAddress=self.settings.sender,
                        Destination={"ToAddresses": [self.settings.recipient]},
                        Content={"Simple": {
                            "Subject": {"Data": subject, "Charset": "UTF-8"},
                            "Body": {"Text": {"Data": text, "Charset": "UTF-8"},
                                     "Html": {"Data": body, "Charset": "UTF-8"}},
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
            await asyncio.wait((work,))
            return work.result()
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
