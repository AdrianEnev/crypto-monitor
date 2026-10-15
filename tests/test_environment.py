"""Environment precedence and CLI email wiring, using disposable credentials."""

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from crypto_monitor.adapters.ses_email import create_ses_client
from crypto_monitor.adapters.touch_email import TouchEmailDispatcher
from crypto_monitor.runtime.environment import email_region, load_environment, read_env_values
from crypto_monitor.runtime.service import parse_settings
from crypto_monitor.runtime.touch_settings import load_touch_settings
from tests.test_touch_settings import CONFIG
from tests.test_touch_email import MemoryOutbox, START, activation


SMTP_CONFIG = """SES_SMTP_HOST=email-smtp.eu-west-1.amazonaws.com
SES_SMTP_PORT=587
SES_SMTP_USER=test-smtp-user
SES_SMTP_PASS=test-smtp-password
EMAIL_FROM=monitor@example.com
ALERT_EMAIL_TO=recipient@example.com
"""


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.project = self.root / "crypto-monitor"
        self.project.mkdir()
        self.shared = self.root / ".env"
        self.local = self.project / ".env"
        self.shared.write_text(SMTP_CONFIG)
        self.local.write_text(CONFIG)
        self.enterContext(patch.dict(os.environ, {}, clear=True))
        self.enterContext(patch("crypto_monitor.runtime.environment.SHARED_ENV_FILE", self.shared))
        self.enterContext(patch("crypto_monitor.runtime.service.DEFAULT_ENV_FILE", self.local))
        self.enterContext(patch("crypto_monitor.runtime.service.DEFAULT_DATABASE", self.project / "monitor.sqlite3"))

    def test_project_and_parent_load_without_changing_files(self):
        before = {path: path.read_bytes() for path in (self.shared, self.local)}
        load_environment(self.local)
        self.assertEqual(os.environ["SES_SMTP_USER"], "test-smtp-user")
        self.assertEqual(os.environ["PWLH_delay"], "0.1")
        self.assertEqual(os.environ["EMAIL_RETRY_SECONDS"], "30")
        self.assertEqual({path: path.read_bytes() for path in before}, before)

    def test_process_then_project_then_shared_precedence(self):
        self.local.write_text(CONFIG + "EMAIL_FROM=project@example.com\nALERT_EMAIL_TO=project-recipient@example.com\n")
        os.environ["EMAIL_FROM"] = "process@example.com"
        load_environment(self.local)
        self.assertEqual(os.environ["EMAIL_FROM"], "process@example.com")
        self.assertEqual(os.environ["ALERT_EMAIL_TO"], "project-recipient@example.com")
        self.assertEqual(os.environ["SES_SMTP_PASS"], "test-smtp-password")

    def test_shared_and_process_timings_do_not_replace_required_project_values(self):
        self.shared.write_text(SMTP_CONFIG + "PWLH_delay=20\n")
        os.environ["PWLH_delay"] = "30"
        load_environment(self.local)
        self.assertEqual(str(load_touch_settings(self.local).delays["previous_week"]), "0.1")
        self.local.write_text(CONFIG.replace("PWLH_delay=0.1\n", ""))
        with self.assertRaisesRegex(ValueError, "PWLH_delay"):
            load_touch_settings(self.local)

    def test_default_paths_are_anchored_even_from_another_working_directory(self):
        expected = parse_settings([])
        previous = Path.cwd()
        try:
            os.chdir(self.root)
            settings = parse_settings([])
        finally:
            os.chdir(previous)
        self.assertEqual(settings, expected)
        self.assertEqual(settings.database, self.project / "monitor.sqlite3")
        self.assertEqual(settings.env_file, self.local)
        self.assertTrue(settings.email.enabled)
        self.assertEqual(settings.email.sender, "monitor@example.com")
        self.assertEqual(settings.email.recipient, "recipient@example.com")
        self.assertEqual(settings.email.region, "eu-west-1")

    def test_custom_project_file_overrides_shared_defaults_and_cli_wins(self):
        custom = self.root / "custom.env"
        custom.write_text(CONFIG + "EMAIL_FROM=custom@example.com\n")
        settings = parse_settings(["--env-file", str(custom), "--email-enabled",
                                   "--email-recipient", "cli@example.com", "--email-region", "us-east-1"])
        self.assertEqual(settings.env_file, custom)
        self.assertEqual(settings.email.sender, "custom@example.com")
        self.assertEqual(settings.email.recipient, "cli@example.com")
        self.assertEqual(settings.email.region, "us-east-1")
        self.assertEqual(os.environ["SES_SMTP_USER"], "test-smtp-user")

    def test_email_defaults_on_and_can_be_disabled_explicitly(self):
        self.assertTrue(parse_settings([]).email.enabled)
        self.assertFalse(parse_settings(["--no-email"]).email.enabled)
        self.assertFalse(parse_settings(["--email-disabled"]).email.enabled)

    def test_shared_smtp_credentials_reach_the_production_touch_dispatcher(self):
        settings = parse_settings(["--email-enabled"])
        policy = load_touch_settings(settings.env_file)
        outbox = MemoryOutbox(activation())
        server = MagicMock()
        server.send_message.return_value = {}
        dispatcher = TouchEmailDispatcher("unused", settings.email,
            retry_seconds=policy.email_retry_seconds, max_age_seconds=policy.max_age_seconds,
            store_factory=lambda path: outbox, now=lambda: START)
        with patch("crypto_monitor.adapters.ses_smtp.smtplib.SMTP", return_value=server):
            self.assertEqual(dispatcher.send_pending(), 1)
        server.login.assert_called_once_with("test-smtp-user", "test-smtp-password")
        self.assertEqual(server.send_message.call_args.kwargs,
                         {"from_addr": "monitor@example.com", "to_addrs": ["recipient@example.com"]})
        self.assertEqual(outbox.rows["activation-1"].attempt_count, 1)
        self.assertIsNotNone(outbox.rows["activation-1"].sent_at)

    def test_smtp_factory_selection_does_not_load_boto3(self):
        load_environment(self.local)
        with patch.dict("sys.modules", {"boto3": None}), \
             patch("crypto_monitor.adapters.ses_smtp.SESEmailSMTPClient") as smtp:
            self.assertIs(create_ses_client("eu-west-1"), smtp.return_value)
        smtp.assert_called_once_with()

    def test_incomplete_smtp_credentials_fail_before_startup(self):
        self.shared.write_text(SMTP_CONFIG.replace("SES_SMTP_PASS=test-smtp-password\n", ""))
        with contextlib.redirect_stderr(io.StringIO()) as errors, self.assertRaises(SystemExit) as caught:
            parse_settings([])
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("SES_SMTP_PASS", errors.getvalue())
        self.assertNotIn("test-smtp-user", errors.getvalue())
        self.assertFalse(parse_settings(["--no-email"]).email.enabled)

    def test_api_credentials_and_region_load_without_smtp(self):
        self.shared.write_text("AWS_ACCESS_KEY_ID=fake-access\nAWS_SECRET_ACCESS_KEY=fake-secret\nAWS_DEFAULT_REGION=eu-central-1\n")
        load_environment(self.local)
        self.assertEqual(os.environ["AWS_ACCESS_KEY_ID"], "fake-access")
        self.assertEqual(os.environ["AWS_SECRET_ACCESS_KEY"], "fake-secret")
        self.assertEqual(email_region(), "eu-central-1")

    def test_optional_missing_shared_file_and_strict_parser(self):
        self.shared.unlink()
        self.local.write_text(CONFIG + "export LABEL='value # literal'\nOTHER=unquoted # comment\n")
        load_environment(self.local)
        self.assertEqual(os.environ["LABEL"], "value # literal")
        self.assertEqual(os.environ["OTHER"], "unquoted")
        self.assertEqual(read_env_values(self.shared), {})

    def test_invalid_project_file_does_not_export_shared_secrets(self):
        self.local.write_text(CONFIG + "BROKEN\n")
        with self.assertRaises(ValueError):
            load_environment(self.local)
        self.assertNotIn("SES_SMTP_PASS", os.environ)
