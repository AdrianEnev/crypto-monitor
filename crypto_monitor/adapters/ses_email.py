"""Optional SES v2 digests from a durable SQLite outbox; no import-time I/O.

One dispatcher per database. Delivery is at least once: acceptance followed by
a lost response or failed SQLite acknowledgement can cause duplicate email.
"""

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from crypto_monitor.core.level_sweep import MINUTE
from crypto_monitor.core.market_universe import rank_alerts
from crypto_monitor.persistence.sqlite_store import SQLiteStore


LOG = logging.getLogger(__name__)
INTERVAL = timedelta(minutes=5)
MAX_BACKOFF = timedelta(hours=1)


class InvalidSESResponse(RuntimeError):
    """No usable acceptance MessageId; retry without claiming successful delivery."""


@dataclass(frozen=True)
class DeliveryStatus:
    last_error: str | None = None
    last_error_at: datetime | None = None


@dataclass(frozen=True)
class EmailSettings:
    enabled: bool = False
    sender: str | None = None
    recipient: str | None = None
    region: str | None = None

    def __post_init__(self):
        if type(self.enabled) is not bool:
            raise ValueError("email enablement must be a boolean")
        if not self.enabled:
            return
        for label, value in (("sender", self.sender), ("recipient", self.recipient)):
            # A single bare ASCII address; no display names or address lists.
            if (not isinstance(value, str) or not value.isascii()
                    or not re.fullmatch(r"[^\s<>@,;]+@[^\s<>@,;]+\.[^\s<>@,;]+", value)):
                raise ValueError(f"enabled email requires a valid {label} address")
        if not isinstance(self.region, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)+-\d+", self.region):
            raise ValueError("enabled email requires an AWS region")


def create_ses_client(region):
    """Use shared SES SMTP credentials when configured, otherwise Boto3's chain."""
    if os.environ.get("SES_SMTP_HOST"):
        from crypto_monitor.adapters.ses_smtp import SESEmailSMTPClient
        return SESEmailSMTPClient()
    import boto3
    from botocore.config import Config
    return boto3.client("sesv2", region_name=region, config=Config(
        connect_timeout=5, read_timeout=15,
        retries={"mode": "standard", "total_max_attempts": 1}))


def utc_text(value):
    if value.utcoffset() != timedelta(0):
        raise ValueError("email times must be aware UTC")
    return value.isoformat().replace("+00:00", "Z")


def next_digest_time(now):
    utc_text(now)
    return now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0) + INTERVAL


def retry_delay(attempt):
    # Cap the exponent too: indefinitely failing outboxes cannot create huge integers.
    return min(INTERVAL * 2 ** min(max(attempt - 1, 0), 4), MAX_BACKOFF)


def digest_content(hits):
    """Reuse Decimal-safe ranking; price is the swept level, not an execution."""
    ranked = rank_alerts(hits)
    lines = [f"Cryptocurrency level sweeps — {len(ranked)} alerts", "",
             "Ranked by captured 24h quote volume, then newest detection.",
             "Confirmation is the closed candle's UTC end; observed time may be later during recovery.", ""]
    for index, hit in enumerate(ranked, 1):
        lines.append(
            f"{index}. {hit.symbol} | {hit.level_name} {hit.side} sweep | "
            f"level price: {hit.level_price} USDT | "
            f"confirmed: {utc_text(hit.candle_time + MINUTE)} | "
            f"captured 24h quote volume: {hit.captured_quote_volume} USDT\n"
            f"   Reference start: {utc_text(hit.reference_period_start)} | "
            f"observed: {utc_text(hit.detected_at)} | alert ID: {hit.alert_id}")
    return f"Crypto level sweeps: {len(ranked)} alerts", "\n".join(lines)


def error_category(exc):
    """Avoid logging/persisting raw SDK exceptions, request data, or credentials."""
    known_codes = {"AccessDenied", "AccessDeniedException", "AccountSuspendedException",
        "BadRequestException", "LimitExceededException", "MailFromDomainNotVerifiedException",
        "MessageRejected", "NotFoundException", "SendingPausedException", "TooManyRequestsException",
        "ExpiredToken", "InvalidClientTokenId", "SignatureDoesNotMatch"}
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        code = error.get("Code") if isinstance(error, dict) else None
        if isinstance(code, str) and code in known_codes:
            return f"SES {code}"
    return type(exc).__name__


class EmailDispatcher:
    """Send at most one 200-alert digest per UTC five-minute tick.

    Retry counts/deadlines are durable. SES errors leave rows pending; SQLite
    errors propagate so the service cannot silently lose delivery bookkeeping.
    Blocking I/O runs off-loop and is allowed to finish during orderly shutdown.
    """

    def __init__(self, database, settings: EmailSettings, *, client=None,
                 client_factory=create_ses_client, store_factory=SQLiteStore,
                 now=lambda: datetime.now(timezone.utc), sleep=asyncio.sleep,
                 batch_size=200):
        if type(batch_size) is not int or not 1 <= batch_size <= 200:
            raise ValueError("email batch size must be between 1 and 200")
        self.database, self.settings = database, settings
        self.client, self.client_factory, self.store_factory = client, client_factory, store_factory
        self.now, self.sleep, self.batch_size = now, sleep, batch_size
        self._running = False
        self.status = DeliveryStatus()

    def send_pending(self):
        """One synchronous attempt; return the accepted count. No I/O if disabled."""
        if not self.settings.enabled:
            return 0
        now = self.now()
        utc_text(now)
        with self.store_factory(self.database) as store:
            entries = store.load_outbox(due_at=now)
            by_id = {entry.hit.alert_id: entry for entry in entries}
            hits = rank_alerts(entry.hit for entry in entries)[:self.batch_size]
            if not hits:
                return 0
            subject, body = digest_content(hits)
            ids = [hit.alert_id for hit in hits]
            attempt = max(by_id[identity].attempt_count for identity in ids) + 1
            store.begin_email_attempt(ids, now=now, next_attempt_at=now + retry_delay(attempt))

        client, owned = self.client, self.client is None
        try:
            try:
                if owned:
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
                with self.store_factory(self.database) as store:
                    store.record_email_failure(ids, error)
                self.status = DeliveryStatus(error, self.now())
                LOG.warning("email pending count=%s attempt=%s error=%s", len(ids), attempt, error)
                return 0
            # Keep this outside the send catch: failed acknowledgement must surface.
            with self.store_factory(self.database) as store:
                store.mark_email_sent(ids, message_id=message_id, sent_at=self.now())
            LOG.info("email accepted count=%s", len(ids))
            return len(ids)
        finally:
            if owned and client is not None:
                try:
                    client.close()
                except Exception as exc:
                    LOG.warning("email client cleanup error=%s", error_category(exc))

    async def run(self, stop):
        if not self.settings.enabled:
            await stop.wait()
            return
        if self._running:
            raise RuntimeError("email dispatcher is already running")
        self._running = True
        try:
            due = next_digest_time(self.now())
            while not stop.is_set():
                sleeping = asyncio.create_task(self.sleep(max(0, (due - self.now()).total_seconds())))
                stopping = asyncio.create_task(stop.wait())
                try:
                    done, _ = await asyncio.wait((sleeping, stopping), return_when=asyncio.FIRST_COMPLETED)
                    if stopping in done:
                        return
                    await sleeping
                finally:
                    for task in (sleeping, stopping):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(sleeping, stopping, return_exceptions=True)
                if stop.is_set() or self.now() < due:
                    continue
                work = asyncio.create_task(asyncio.to_thread(self.send_pending))
                try:
                    await asyncio.wait((work,))
                    work.result()
                finally:
                    # Do not abandon a possibly accepted send before recording its result.
                    await asyncio.gather(work, return_exceptions=True)
                due = next_digest_time(self.now())
        finally:
            self._running = False
