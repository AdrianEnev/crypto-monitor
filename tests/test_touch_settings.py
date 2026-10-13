"""Required file configuration and production entry point checks."""

import contextlib
import io
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

from crypto_monitor.runtime.service import main, parse_settings
from crypto_monitor.runtime.touch_settings import load_touch_settings


CONFIG = """PWLH_delay=0.1
PMLH_delay=3
PQLH_delay=5
NOTIFICATION_MAX_AGE_SECONDS=60
EMAIL_RETRY_SECONDS=30
FRESH_HIGHLIGHT_SECONDS=60
"""


class TouchSettingsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / ".env"
        self.path.write_text(CONFIG)

    def test_fractional_explicit_values_and_no_monday_delay(self):
        settings = load_touch_settings(self.path)
        self.assertEqual(settings.delays["previous_week"], Decimal("0.1"))
        self.assertEqual(set(settings.delays), {"previous_week", "previous_month", "previous_quarter"})
        self.assertEqual(settings.max_age_seconds, Decimal(60))
        with self.assertRaises(TypeError):
            settings.delays["previous_week"] = Decimal(1)

    def test_missing_file_missing_value_and_invalid_numbers_have_no_fallback(self):
        with self.assertRaisesRegex(ValueError, "not found"):
            load_touch_settings(self.path.parent / "missing")
        for value in ("", "0", "-1", "NaN", "Infinity", "abc"):
            with self.subTest(value=value):
                self.path.write_text(CONFIG.replace("PWLH_delay=0.1", f"PWLH_delay={value}"))
                with self.assertRaisesRegex(ValueError, "PWLH_delay"):
                    load_touch_settings(self.path)
        self.path.write_text(CONFIG.replace("EMAIL_RETRY_SECONDS=30\n", ""))
        with self.assertRaisesRegex(ValueError, "EMAIL_RETRY_SECONDS"):
            load_touch_settings(self.path)

    def test_duplicates_and_invalid_lines_rejected(self):
        for extra in ("PWLH_delay=2", "BROKEN", "FOO='unclosed"):
            self.path.write_text(CONFIG + extra)
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                load_touch_settings(self.path)

    def test_cli_layout_and_required_path_are_forwarded(self):
        settings = parse_settings(["--env-file", str(self.path), "--layout", "cards", "--top-n", "100"])
        self.assertEqual(settings.env_file, self.path.resolve())
        self.assertEqual(settings.layout, "cards")
        self.assertEqual(settings.top_n, 100)

    def test_production_main_uses_touch_service_with_validated_policy(self):
        runner = AsyncMock()
        with patch("crypto_monitor.runtime.touch_service.run_touch_service", runner), \
             patch("crypto_monitor.runtime.service.configure_logging"):
            self.assertEqual(main(["--env-file", str(self.path), "--no-tui"]), 0)
        runner.assert_awaited_once()
        self.assertEqual(runner.call_args.kwargs["policy"].delays["previous_week"], Decimal("0.1"))

    def test_configuration_failure_prevents_service_or_database_start(self):
        self.path.write_text(CONFIG.replace("PMLH_delay=3", "PMLH_delay="))
        runner = AsyncMock()
        with patch("crypto_monitor.runtime.touch_service.run_touch_service", runner), contextlib.redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(main(["--env-file", str(self.path)]), 2)
        runner.assert_not_called()
        self.assertIn("PMLH_delay", errors.getvalue())
