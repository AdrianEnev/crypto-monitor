"""SES SMTP transport with the small client interface used by both dispatchers.

Credentials are read only when explicitly requested. Connections are lazy and
always encrypted; an accepted message stays accepted even if socket cleanup
fails. Returned IDs are local RFC Message-IDs, not SES API acceptance IDs.
"""

import math
import os
import re
import smtplib
import ssl
from collections.abc import Mapping
from dataclasses import dataclass, field
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import make_msgid


def _required(value, name):
    if not isinstance(value, str) or not value.strip() or "\r" in value or "\n" in value:
        raise ValueError(f"{name} must be configured for SES SMTP email")
    return value


@dataclass(frozen=True)
class SESEmailSMTPSettings:
    host: str
    username: str = field(repr=False)
    password: str = field(repr=False)
    port: int = 587
    timeout_seconds: float = 15

    def __post_init__(self):
        _required(self.host, "SES_SMTP_HOST")
        if any(character.isspace() for character in self.host):
            raise ValueError("SES_SMTP_HOST must be a hostname")
        _required(self.username, "SES_SMTP_USER or SES_IAM_USER")
        _required(self.password, "SES_SMTP_PASS")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("SES_SMTP_PORT must be an integer between 1 and 65535")
        if (isinstance(self.timeout_seconds, bool)
                or not isinstance(self.timeout_seconds, (int, float))
                or not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0):
            raise ValueError("SES SMTP timeout must be a finite positive number")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None):
        values = os.environ if environ is None else environ
        raw_port = values.get("SES_SMTP_PORT", "").strip() or "587"
        try:
            port = int(raw_port)
        except ValueError:
            raise ValueError("SES_SMTP_PORT must be an integer between 1 and 65535") from None
        return cls(
            host=values.get("SES_SMTP_HOST", "").strip(),
            username=(values.get("SES_SMTP_USER", "").strip()
                      or values.get("SES_IAM_USER", "").strip()),
            password=values.get("SES_SMTP_PASS", ""),
            port=port,
        )


def _bare_address(value, label):
    if (not isinstance(value, str) or not value.isascii()
            or not re.fullmatch(r"[^\s<>@,;]+@[^\s<>@,;]+\.[^\s<>@,;]+", value)):
        raise ValueError(f"SES SMTP requires one bare {label} address")
    return value


def _simple_text(content):
    try:
        simple = content["Simple"]
        subject = simple["Subject"]["Data"]
        body = simple["Body"]["Text"]
        text = body["Data"]
        charset = body.get("Charset", "utf-8")
    except (KeyError, TypeError, AttributeError):
        raise ValueError("SES SMTP requires Simple Subject and Text body content") from None
    if not all(isinstance(value, str) for value in (subject, text, charset)):
        raise ValueError("SES SMTP message content must be text")
    return subject, text, charset


class SESEmailSMTPClient:
    def __init__(self, settings=None, *, smtp_factory=None, smtp_ssl_factory=None,
                 context_factory=None):
        self.settings = SESEmailSMTPSettings.from_env() if settings is None else settings
        self._smtp_factory = smtp_factory
        self._smtp_ssl_factory = smtp_ssl_factory
        self._context_factory = context_factory
        self._connection = None

    def send_email(self, *, FromEmailAddress, Destination, Content):
        sender = _bare_address(FromEmailAddress, "sender")
        recipients = Destination.get("ToAddresses") if isinstance(Destination, Mapping) else None
        if not isinstance(recipients, (list, tuple)) or len(recipients) != 1:
            raise ValueError("SES SMTP requires exactly one recipient")
        recipient = _bare_address(recipients[0], "recipient")
        subject, text, charset = _simple_text(Content)
        message = EmailMessage(policy=SMTP)
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = subject
        message["Message-ID"] = make_msgid()
        message.set_content(text, charset=charset)
        context = (self._context_factory or ssl.create_default_context)()
        settings = self.settings
        try:
            if settings.port in (465, 2465):
                factory = self._smtp_ssl_factory or smtplib.SMTP_SSL
                self._connection = factory(settings.host, settings.port,
                                           timeout=settings.timeout_seconds, context=context)
                self._connection.ehlo()
            else:
                factory = self._smtp_factory or smtplib.SMTP
                self._connection = factory(settings.host, settings.port,
                                           timeout=settings.timeout_seconds)
                self._connection.ehlo()
                self._connection.starttls(context=context)
                self._connection.ehlo()
            self._connection.login(settings.username, settings.password)
            refused = self._connection.send_message(message, from_addr=sender,
                                                    to_addrs=[recipient])
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
            return {"MessageId": str(message["Message-ID"])}
        finally:
            self.close()

    def close(self):
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                # QUIT is unnecessary; failure after SMTP acceptance must not
                # turn a successful send into another dispatcher attempt.
                connection.close()
            except Exception:
                pass
