"""Offline SES SMTP configuration, encrypted transport, and acceptance checks."""

import importlib
import math
import smtplib
import ssl
import unittest
from dataclasses import replace
from unittest.mock import MagicMock, call, patch

from crypto_monitor.adapters import ses_smtp
from crypto_monitor.adapters.ses_smtp import SESEmailSMTPClient, SESEmailSMTPSettings


ENV = {"SES_SMTP_HOST": "email-smtp.eu-west-1.amazonaws.com",
       "SES_SMTP_USER": "test-username-sensitive", "SES_SMTP_PASS": "test-password-sensitive"}
REQUEST = {"FromEmailAddress": "monitor@example.com",
           "Destination": {"ToAddresses": ["recipient@example.com"]},
           "Content": {"Simple": {"Subject": {"Data": "Crypto test — level touch", "Charset": "UTF-8"},
                                    "Body": {"Text": {"Data": "A price touched its level.\nNo trades were placed.",
                                                      "Charset": "UTF-8"}}}}}


class SMTPSettingsTests(unittest.TestCase):
    def test_environment_settings_default_port_and_username_fallback(self):
        settings = SESEmailSMTPSettings.from_env(ENV)
        self.assertEqual(settings.host, ENV["SES_SMTP_HOST"])
        self.assertEqual(settings.username, ENV["SES_SMTP_USER"])
        self.assertEqual(settings.password, ENV["SES_SMTP_PASS"])
        self.assertEqual(settings.port, 587)
        self.assertEqual(settings.timeout_seconds, 15)
        fallback = {**ENV, "SES_SMTP_USER": " ", "SES_IAM_USER": "fallback-user", "SES_SMTP_PORT": "465"}
        self.assertEqual(SESEmailSMTPSettings.from_env(fallback).username, "fallback-user")
        self.assertEqual(SESEmailSMTPSettings.from_env(fallback).port, 465)

    def test_secrets_are_excluded_from_repr(self):
        rendered = repr(SESEmailSMTPSettings.from_env(ENV))
        self.assertNotIn(ENV["SES_SMTP_USER"], rendered)
        self.assertNotIn(ENV["SES_SMTP_PASS"], rendered)

    def test_missing_values_and_invalid_ports_fail_without_values(self):
        for key in ("SES_SMTP_HOST", "SES_SMTP_USER", "SES_SMTP_PASS"):
            with self.subTest(key=key), self.assertRaises(ValueError) as caught:
                SESEmailSMTPSettings.from_env({**ENV, key: ""})
            self.assertNotIn(ENV["SES_SMTP_PASS"], str(caught.exception))
            self.assertNotIn(ENV["SES_SMTP_USER"], str(caught.exception))
        for port in ("sensitive-invalid-port", "0", "65536", "1.5"):
            with self.subTest(port=port), self.assertRaises(ValueError) as caught:
                SESEmailSMTPSettings.from_env({**ENV, "SES_SMTP_PORT": port})
            self.assertNotIn(port, str(caught.exception).replace("65535", ""))

    def test_invalid_direct_settings_and_timeouts_are_rejected(self):
        settings = SESEmailSMTPSettings.from_env(ENV)
        for changes in ({"host": "hostname\nsecret"}, {"username": "\nsecret"}, {"password": ""},
                        {"port": True}, {"port": 0}, {"timeout_seconds": 0},
                        {"timeout_seconds": -1}, {"timeout_seconds": math.inf},
                        {"timeout_seconds": math.nan}, {"timeout_seconds": True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(settings, **changes)

    def test_import_and_client_construction_have_no_network_side_effects(self):
        with patch("smtplib.SMTP") as plain, patch("smtplib.SMTP_SSL") as encrypted, \
             patch("ssl.create_default_context") as context:
            importlib.reload(ses_smtp)
            client = ses_smtp.SESEmailSMTPClient(ses_smtp.SESEmailSMTPSettings.from_env(ENV))
            client.close()
        plain.assert_not_called()
        encrypted.assert_not_called()
        context.assert_not_called()


class SMTPClientTests(unittest.TestCase):
    def setUp(self):
        self.server = MagicMock()
        self.server.send_message.return_value = {}
        self.plain = MagicMock(return_value=self.server)
        self.encrypted = MagicMock(return_value=self.server)
        self.context = ssl.create_default_context()
        self.context_factory = MagicMock(return_value=self.context)

    def client(self, port=587):
        return SESEmailSMTPClient(replace(SESEmailSMTPSettings.from_env(ENV), port=port),
                                 smtp_factory=self.plain, smtp_ssl_factory=self.encrypted,
                                 context_factory=self.context_factory)

    def test_starttls_login_envelope_content_and_local_acceptance_id(self):
        result = self.client().send_email(**REQUEST)
        self.plain.assert_called_once_with(ENV["SES_SMTP_HOST"], 587, timeout=15)
        self.encrypted.assert_not_called()
        self.assertEqual(self.server.mock_calls[:4], [call.ehlo(), call.starttls(context=self.context),
                         call.ehlo(), call.login(ENV["SES_SMTP_USER"], ENV["SES_SMTP_PASS"])])
        self.assertEqual(self.context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(self.context.check_hostname)
        message = self.server.send_message.call_args.args[0]
        self.assertEqual(self.server.send_message.call_args.kwargs,
                         {"from_addr": "monitor@example.com", "to_addrs": ["recipient@example.com"]})
        self.assertEqual(str(message["Subject"]), REQUEST["Content"]["Simple"]["Subject"]["Data"])
        self.assertEqual(str(message["From"]), "monitor@example.com")
        self.assertEqual(str(message["To"]), "recipient@example.com")
        self.assertEqual(message.get_content().strip(), REQUEST["Content"]["Simple"]["Body"]["Text"]["Data"])
        self.assertRegex(result["MessageId"], r"^<[^<>]+@[^<>]+>$")
        self.assertEqual(result["MessageId"], str(message["Message-ID"]))
        self.server.close.assert_called_once()
        self.server.quit.assert_not_called()

    def test_implicit_tls_ports_and_starttls_alternative(self):
        for port in (465, 2465, 2587):
            with self.subTest(port=port):
                self.server.reset_mock()
                self.plain.reset_mock()
                self.encrypted.reset_mock()
                self.client(port).send_email(**REQUEST)
                if port in (465, 2465):
                    self.encrypted.assert_called_once_with(ENV["SES_SMTP_HOST"], port,
                                                           timeout=15, context=self.context)
                    self.plain.assert_not_called()
                    self.server.starttls.assert_not_called()
                else:
                    self.server.starttls.assert_called_once_with(context=self.context)
                    self.encrypted.assert_not_called()

    def test_default_context_factory_is_used(self):
        with patch("crypto_monitor.adapters.ses_smtp.ssl.create_default_context", return_value=self.context) as context:
            SESEmailSMTPClient(SESEmailSMTPSettings.from_env(ENV), smtp_factory=self.plain).send_email(**REQUEST)
        context.assert_called_once_with()

    def test_recipient_refusal_is_failure(self):
        self.server.send_message.return_value = {"recipient@example.com": (550, b"rejected")}
        with self.assertRaises(smtplib.SMTPRecipientsRefused):
            self.client().send_email(**REQUEST)
        self.server.close.assert_called_once()

    def test_transport_errors_are_preserved_and_connections_closed(self):
        for operation in ("ehlo", "starttls", "login", "send_message"):
            with self.subTest(operation=operation):
                self.server.reset_mock()
                error = smtplib.SMTPException("simulated failure")
                getattr(self.server, operation).side_effect = error
                try:
                    with self.assertRaises(smtplib.SMTPException) as caught:
                        self.client().send_email(**REQUEST)
                    self.assertIs(caught.exception, error)
                    self.server.close.assert_called_once()
                    self.server.quit.assert_not_called()
                finally:
                    getattr(self.server, operation).side_effect = None

    def test_failed_tls_prevents_login_and_send(self):
        self.server.starttls.side_effect = smtplib.SMTPNotSupportedError("TLS unavailable")
        with self.assertRaises(smtplib.SMTPNotSupportedError):
            self.client().send_email(**REQUEST)
        self.server.login.assert_not_called()
        self.server.send_message.assert_not_called()

    def test_cleanup_failure_after_acceptance_does_not_fail_the_send(self):
        self.server.close.side_effect = OSError("simulated close failure")
        client = self.client()
        result = client.send_email(**REQUEST)
        self.assertTrue(result["MessageId"])
        client.close()
        self.server.close.assert_called_once()
        self.server.send_message.assert_called_once()
        self.server.quit.assert_not_called()

    def test_invalid_requests_fail_before_connecting(self):
        requests = [
            {**REQUEST, "FromEmailAddress": "name <monitor@example.com>"},
            {**REQUEST, "Destination": {"ToAddresses": []}},
            {**REQUEST, "Destination": {"ToAddresses": ["one@example.com", "two@example.com"]}},
            {**REQUEST, "Destination": {"ToAddresses": ["bad\n@example.com"]}},
            {**REQUEST, "Content": {}},
            {**REQUEST, "Content": {"Simple": {"Subject": {"Data": None}, "Body": {"Text": {"Data": "x"}}}}},
        ]
        for request in requests:
            with self.subTest(request=request), self.assertRaises(ValueError):
                self.client().send_email(**request)
        self.plain.assert_not_called()
        self.encrypted.assert_not_called()


if __name__ == "__main__":
    unittest.main()
