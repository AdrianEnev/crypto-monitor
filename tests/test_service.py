"""Entry-point lifecycle tests; all external clients are fixtures or mocks."""

import asyncio
import contextlib
import io
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter
from crypto_monitor.runtime.gap_recovery import MonitoringFailure
from crypto_monitor.core.level_sweep import MINUTE, Levels, reference_periods
from crypto_monitor.core.monitoring_engine import CandleBaseline
from crypto_monitor.runtime.service import (Settings, StartupReferences, configure_logging, main, parse_settings,
                     make_dashboard, run_service, shutdown_signals, should_use_dashboard)
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from crypto_monitor.adapters.ses_email import EmailSettings
from crypto_monitor.runtime.websocket_supervisor import WebSocketSupervisor
from tests.test_binance_feed import FakeSocket, payload
from tests.test_minute_klines import START, millis, window_response
from tests.test_sqlite_store import D, MARKETS, event


class SettingsTests(unittest.TestCase):
    def test_committed_freshness_threshold_configuration_and_wiring(self):
        self.assertEqual(parse_settings([]).stale_after_minutes, 3)
        settings = parse_settings(["--stale-after-minutes", "7"])
        self.assertEqual(settings.stale_after_minutes, 7)
        with patch("crypto_monitor.ui.terminal_dashboard.DashboardCollector") as collector:
            make_dashboard(settings, MagicMock(), MagicMock(), MagicMock(), None, now=lambda: START)
        self.assertEqual(collector.call_args.kwargs["stale_after_minutes"], 7)
        for value in ("0", "-1", "nan", "inf", "1.5"):
            with self.subTest(value=value), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_settings(["--stale-after-minutes", value])

    def test_no_tui_and_stdout_detection(self):
        terminal = MagicMock()
        terminal.isatty.return_value = True
        with patch.dict(os.environ, {"TERM": "xterm"}):
            self.assertTrue(should_use_dashboard(parse_settings([]), terminal))
            self.assertFalse(should_use_dashboard(parse_settings(["--no-tui"]), terminal))
            self.assertFalse(should_use_dashboard(parse_settings([]), io.StringIO()))
        with patch.dict(os.environ, {"TERM": "dumb"}):
            self.assertFalse(should_use_dashboard(parse_settings([]), terminal))

    def test_email_requires_explicit_enablement_and_complete_configuration(self):
        self.assertFalse(parse_settings([]).email.enabled)
        args = ["--email-sender", "monitor@example.com", "--email-recipient", "recipient@example.com",
                "--email-region", "eu-west-1"]
        self.assertFalse(parse_settings(args).email.enabled)
        configured = parse_settings(["--email-enabled"] + args)
        self.assertEqual(configured.email, EmailSettings(True, "monitor@example.com", "recipient@example.com", "eu-west-1"))
        for incomplete in (["--email-enabled"], ["--email-enabled"] + args[:2],
                           ["--email-enabled"] + args[:4]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                parse_settings(incomplete)
            self.assertEqual(error.exception.code, 2)

    def test_defaults_and_explicit_selection(self):
        defaults = parse_settings([])
        self.assertEqual(defaults.database, Path("monitor.sqlite3").resolve())
        self.assertEqual(defaults.top_n, 20)
        settings = parse_settings(["--database", "example.sqlite3", "--top-n", "0",
                                   "--include", "ethusdt, BTCUSDT,ethusdt", "--exclude", "ETHUSDT"])
        self.assertEqual(settings.include_symbols, ("BTCUSDT", "ETHUSDT"))
        self.assertEqual(settings.exclude_symbols, ("ETHUSDT",))
        self.assertEqual(settings.top_n, 0)

    def test_invalid_settings_fail_before_startup(self):
        for args in (["--top-n", "-1"], ["--database", ""], ["--database", ":memory:"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    parse_settings(args)
                self.assertEqual(caught.exception.code, 2)

    def test_fresh_import_has_no_database_network_signal_or_logging_setup(self):
        code = '''
from unittest.mock import patch
with patch("sqlite3.connect", side_effect=AssertionError("database on import")), \\
     patch("urllib.request.urlopen", side_effect=AssertionError("HTTP on import")), \\
     patch("websockets.asyncio.client.connect", side_effect=AssertionError("socket on import")), \\
     patch("signal.signal", side_effect=AssertionError("signals on import")), \\
     patch("logging.basicConfig", side_effect=AssertionError("logging setup on import")), \\
     patch("asyncio.run", side_effect=AssertionError("run on import")):
    import crypto_monitor.runtime.service
    import crypto_monitor.__main__
    import sys
    assert "boto3" not in sys.modules
'''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, "-B", "-c", code], cwd=directory,
                env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
                capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertEqual(list(Path(directory).iterdir()), [])


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trace = []
        self.stop = asyncio.Event()
        self.settings = Settings(Path("unused.sqlite3"), 3, ("BTCUSDT",), ("SOLUSDT",))
        self.rest = MagicMock()
        self.rest.discover_markets.side_effect = lambda **kwargs: self.record("discover", MARKETS)
        self.rest.fetch_reference_levels.side_effect = lambda symbol, **kwargs: self.record(
            "levels:" + symbol, {name: Levels(D("100"), D("90")) for name in reference_periods(START)})
        self.store = MagicMock()
        self.store.__enter__.side_effect = lambda: self.record("open", self.store)
        self.store.__exit__.side_effect = lambda *args: self.record("close", False)
        self.store.load_baselines.side_effect = lambda: self.record("load", {
            "BTCUSDT": CandleBaseline(START, D("99")), "UNSELECTED": CandleBaseline(START, D("10"))})
        self.engine = MagicMock()
        self.engine.restore_baseline.side_effect = lambda symbol, baseline: self.record("restore:" + symbol)
        self.worker = MagicMock()
        self.supervisor = MagicMock()
        async def run(stop):
            self.record("run")
            stop.set()
        self.supervisor.run = AsyncMock(side_effect=run)
        coordinator = MagicMock()
        coordinator.initialize = AsyncMock()
        async def refresh_run(stop):
            await stop.wait()
        coordinator.run = AsyncMock(side_effect=refresh_run)
        self.options = dict(stop=self.stop, rest_factory=lambda: self.record("rest", self.rest),
            store_factory=MagicMock(return_value=self.store),
            engine_factory=lambda markets: self.record("engine", self.engine),
            worker_factory=MagicMock(side_effect=lambda *args: self.record("worker", self.worker)),
            supervisor_factory=MagicMock(side_effect=lambda worker: self.record("supervisor", self.supervisor)),
            coordinator_factory=MagicMock(return_value=coordinator),
            now=lambda: START, stdout=io.StringIO())

    def record(self, name, result=None):
        self.trace.append(name)
        return result

    async def test_non_tty_and_no_tui_keep_plain_logging_without_dashboard(self):
        tty = MagicMock()
        tty.isatty.return_value = True
        for settings, stream in ((self.settings, io.StringIO()), (replace(self.settings, no_tui=True), tty)):
            self.stop.clear()
            factory = MagicMock(side_effect=AssertionError("dashboard should not start"))
            options = {**self.options, "stdout": stream}
            with patch.dict(os.environ, {"TERM": "xterm"}), self.assertLogs("crypto_monitor.runtime.service", level="INFO") as logs:
                await run_service(settings, dashboard_factory=factory, **options)
            factory.assert_not_called()
            self.assertIn("startup complete", "\n".join(logs.output))

    async def test_dashboard_construction_failure_keeps_monitoring_running(self):
        tty = MagicMock()
        tty.isatty.return_value = True
        factory = MagicMock(side_effect=ImportError("Rich unavailable"))
        with patch.dict(os.environ, {"TERM": "xterm"}), self.assertLogs("crypto_monitor.runtime.service", level="WARNING") as logs:
            await run_service(self.settings, **{**self.options, "stdout": tty, "dashboard_factory": factory})
        self.supervisor.run.assert_awaited_once()
        self.assertIn("continuing plain logs", "\n".join(logs.output))

    async def test_dashboard_stops_after_core_cleanup_on_stop_and_cancellation(self):
        for cancel in (False, True):
            self.stop.clear()
            entered, closed = asyncio.Event(), asyncio.Event()
            async def supervise(stop):
                try:
                    await stop.wait()
                finally:
                    closed.set()
            async def display(stop):
                entered.set()
                await stop.wait()
                self.assertTrue(closed.is_set())
            self.supervisor.run.side_effect = supervise
            dashboard = MagicMock()
            dashboard.run = AsyncMock(side_effect=display)
            factory = MagicMock(return_value=dashboard)
            tty = MagicMock()
            tty.isatty.return_value = True
            options = {**self.options, "stdout": tty, "dashboard_factory": factory}
            with patch.dict(os.environ, {"TERM": "xterm"}):
                task = asyncio.create_task(run_service(self.settings, **options))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    if cancel:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        self.stop.set()
                        await asyncio.wait_for(task, 2)
                    self.assertTrue(closed.is_set())
                    dashboard.run.assert_awaited_once()
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_disabled_email_never_constructs_dispatcher(self):
        factory = MagicMock(side_effect=AssertionError("disabled email constructed"))
        await run_service(self.settings, email_factory=factory, **self.options)
        factory.assert_not_called()

    async def test_enabled_email_is_injected_started_and_stopped(self):
        settings = replace(self.settings, email=EmailSettings(True, "a@example.com", "b@example.com", "eu-west-1"))
        email = MagicMock()
        async def run(stop):
            await stop.wait()
        email.run = AsyncMock(side_effect=run)
        factory = MagicMock(return_value=email)
        await run_service(settings, email_factory=factory, **self.options)
        factory.assert_called_once_with(settings.database, settings.email, now=self.options["now"])
        email.run.assert_awaited_once_with(self.stop)

    async def test_email_bookkeeping_failure_stops_service(self):
        settings = replace(self.settings, email=EmailSettings(True, "a@example.com", "b@example.com", "eu-west-1"))
        entered, closed = asyncio.Event(), asyncio.Event()
        async def supervise(stop):
            entered.set()
            try:
                await stop.wait()
            finally:
                closed.set()
        async def send(stop):
            await entered.wait()
            raise RuntimeError("outbox storage unavailable")
        self.supervisor.run.side_effect = supervise
        email = MagicMock()
        email.run = AsyncMock(side_effect=send)
        with self.assertLogs("crypto_monitor.runtime.service", level="ERROR"), self.assertRaisesRegex(RuntimeError, "outbox storage"):
            await run_service(settings, email_factory=lambda *a, **k: email, **self.options)
        self.assertTrue(closed.is_set())

    async def test_scheduler_failure_cancels_supervisor(self):
        coordinator = self.options["coordinator_factory"].return_value
        entered, closed = asyncio.Event(), asyncio.Event()
        async def supervise(stop):
            entered.set()
            try:
                await stop.wait()
            finally:
                closed.set()
        async def refresh(stop):
            await entered.wait()
            raise RuntimeError("scheduler failed")
        self.supervisor.run.side_effect = supervise
        coordinator.run.side_effect = refresh
        with self.assertLogs("crypto_monitor.runtime.service", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "scheduler failed"):
                await run_service(self.settings, **self.options)
        self.assertTrue(closed.is_set())
        coordinator.initialize.assert_awaited_once()

    async def test_startup_order_selection_restore_and_single_owner(self):
        with self.assertLogs("crypto_monitor.runtime.service", level="INFO") as logs:
            await run_service(self.settings, **self.options)
        self.assertEqual(self.trace, ["rest", "discover", "levels:BTCUSDT", "levels:ETHUSDT",
            "open", "engine", "load", "restore:BTCUSDT", "close", "worker", "supervisor", "run"])
        self.rest.discover_markets.assert_called_once_with(top_n=3, include_symbols=("BTCUSDT",),
                                                         exclude_symbols=("SOLUSDT",))
        self.options["store_factory"].assert_called_once_with(self.settings.database)
        markets, path, rest, refs = self.options["worker_factory"].call_args.args
        self.assertEqual(markets, tuple(MARKETS))
        self.assertIs(rest, self.rest)
        self.assertEqual(refs("BTCUSDT", START)["monday"].levels.high, D("100"))
        self.worker.run.assert_not_called()  # Only the supervisor owns this lifecycle.
        self.assertIn("restored baseline", "\n".join(logs.output))

    async def test_unavailable_levels_are_reported_and_preserved(self):
        self.rest.fetch_reference_levels.side_effect = lambda *args, **kwargs: {
            name: None for name in reference_periods(START)}
        with self.assertLogs("crypto_monitor.runtime.service", level="INFO") as logs:
            await run_service(self.settings, **self.options)
        refs = self.options["worker_factory"].call_args.args[3]
        self.assertIsNone(refs("BTCUSDT", START)["previous_week"].levels)
        self.assertIn("unavailable=['monday'", "\n".join(logs.output))

    async def test_startup_failure_never_starts_worker_or_opens_database(self):
        self.rest.fetch_reference_levels.side_effect = RuntimeError("history failed")
        with self.assertLogs("crypto_monitor.runtime.service", level="ERROR"), self.assertRaisesRegex(RuntimeError, "history failed"):
            await run_service(self.settings, **self.options)
        self.options["store_factory"].assert_not_called()
        self.options["worker_factory"].assert_not_called()

    async def test_bad_baseline_closes_startup_database_before_error(self):
        self.engine.restore_baseline.side_effect = ValueError("invalid saved baseline")
        with self.assertLogs("crypto_monitor.runtime.service", level="ERROR"), self.assertRaises(ValueError):
            await run_service(self.settings, **self.options)
        self.assertEqual(self.trace[-1], "close")
        self.options["worker_factory"].assert_not_called()

    async def test_supervisor_failure_surfaces_as_fatal(self):
        self.supervisor.run.side_effect = MonitoringFailure("unresolved gap")
        with self.assertLogs("crypto_monitor.runtime.service", level="ERROR") as logs:
            with self.assertRaisesRegex(MonitoringFailure, "unresolved gap"):
                await run_service(self.settings, **self.options)
        self.assertIn("fatal service error", "\n".join(logs.output))
        self.assertEqual(self.trace.count("close"), 1)

    async def test_stop_during_startup_finishes_inflight_call_without_starting_service(self):
        entered, release = threading.Event(), threading.Event()
        def discover(**kwargs):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("test did not release discovery")
            return MARKETS
        self.rest.discover_markets.side_effect = discover
        task = asyncio.create_task(run_service(self.settings, **self.options))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 2))
            self.stop.set()
            await asyncio.sleep(0)
            release.set()
            await asyncio.wait_for(task, 2)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
        self.rest.fetch_reference_levels.assert_not_called()
        self.options["worker_factory"].assert_not_called()

    async def test_cancellation_reaches_running_supervisor(self):
        entered, cleaned = asyncio.Event(), asyncio.Event()
        async def run(stop):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        self.supervisor.run.side_effect = run
        task = asyncio.create_task(run_service(self.settings, **self.options))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(cleaned.is_set())
        self.assertEqual(self.trace.count("close"), 1)

    async def test_pre_requested_stop_has_no_side_effects(self):
        self.stop.set()
        await run_service(self.settings, **self.options)
        self.assertEqual(self.trace, [])

    async def test_snapshot_rejects_rollover_and_older_backfill(self):
        await run_service(self.settings, **self.options)
        refs = self.options["worker_factory"].call_args.args[3]
        for opened in (START.replace(day=12), START.replace(month=9)):
            with self.assertRaisesRegex(MonitoringFailure, "snapshot does not cover"):
                refs("BTCUSDT", opened)
        with self.assertRaises(TypeError):
            refs("BTCUSDT", START)["monday"] = None


class SignalTests(unittest.IsolatedAsyncioTestCase):
    async def test_sigint_and_sigterm_request_stop_and_restore_prior_handlers(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(sig=sig):
                stop = asyncio.Event()
                prior = {signal.SIGINT: object(), signal.SIGTERM: object()}
                installed = {}
                with patch("crypto_monitor.runtime.service.signal.getsignal", side_effect=prior.__getitem__), \
                     patch("crypto_monitor.runtime.service.signal.signal", side_effect=lambda key, value: installed.__setitem__(key, value)):
                    with shutdown_signals(stop):
                        installed[sig](sig, None)
                        await asyncio.wait_for(stop.wait(), 1)
                    self.assertEqual(installed, prior)


class MainTests(unittest.TestCase):
    def test_exit_codes_and_signal_cleanup(self):
        for error, expected in ((None, 0), (RuntimeError("failure"), 1)):
            runner = AsyncMock(side_effect=error)
            signals = MagicMock()
            with self.subTest(error=error), patch("crypto_monitor.runtime.service.configure_logging"), \
                 patch("crypto_monitor.runtime.service.shutdown_signals", signals), patch("crypto_monitor.runtime.service.LOG"):
                self.assertEqual(main([], service_runner=runner), expected)
                runner.assert_awaited_once()
                signals.return_value.__exit__.assert_called_once()

    def test_logging_is_explicitly_configured_for_stdout(self):
        stream = io.StringIO()
        root = logging.getLogger()
        previous_handlers, previous_level = root.handlers[:], root.level
        try:
            with patch("crypto_monitor.runtime.service.sys.stdout", stream):
                configure_logging()
                logging.getLogger("crypto_monitor.runtime.service").info("startup log example")
            self.assertIn("Z INFO crypto_monitor.runtime.service startup log example", stream.getvalue())
        finally:
            for handler in root.handlers:
                handler.close()
            root.handlers = previous_handlers
            root.setLevel(previous_level)


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_pipeline_logs_connection_recovery_and_closes_database(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.sqlite3"
            with SQLiteStore(path) as store:
                store.commit_candle(event(), [])
            rest = BinanceRESTAdapter(opener=MagicMock(side_effect=window_response))
            rest.discover_markets = MagicMock(return_value=MARKETS)
            rest.fetch_reference_levels = MagicMock(return_value={
                name: Levels(D("100"), D("90")) for name in reference_periods(START)})
            raw = payload()
            opened = millis(START + 3 * MINUTE)
            raw["data"]["E"] = opened + 60_000
            raw["data"]["k"].update(t=opened, T=opened + 59_999, o="99", h="101", l="98", c="99")
            socket = FakeSocket([json.dumps(raw)])
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            original = SQLiteStore.commit_candle
            def commit(store, current, hits):
                saved = original(store, current, hits)
                if current.candle.open_time == START + 3 * MINUTE:
                    loop.call_soon_threadsafe(stop.set)
                return saved
            with patch.object(SQLiteStore, "commit_candle", commit), self.assertLogs(level="INFO") as logs:
                await asyncio.wait_for(run_service(Settings(path), stop=stop, rest_factory=lambda: rest,
                    supervisor_factory=lambda worker: WebSocketSupervisor(worker, connector=lambda *a, **k: socket),
                    now=lambda: START), 3)
            self.assertTrue(socket.closed)
            messages = "\n".join(logs.output)
            for expected in ("startup restored baseline", "connection state=connected", "recovery started",
                             "recovery committed", "service stopped"):
                self.assertIn(expected, messages)
            with SQLiteStore(path) as reopened:
                self.assertEqual(reopened.load_baselines()["BTCUSDT"].open_time, START + 3 * MINUTE)
                self.assertEqual(len(reopened.load_hits()), 4)


if __name__ == "__main__":
    unittest.main()
