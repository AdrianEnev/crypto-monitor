"""Rich rendering, read-only collection and lifecycle tests; no external clients."""

import asyncio
import io
import logging
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from rich.console import Console

from crypto_monitor.runtime.gap_recovery import RecoveryStatus, RecoveryWorker
from crypto_monitor.runtime.refresh_coordinator import RefreshStatus
from crypto_monitor.adapters.ses_email import DeliveryStatus
from crypto_monitor.persistence.sqlite_store import DashboardData, SQLiteStore, StoredAlert
from crypto_monitor.ui.terminal_dashboard import (DashboardCollector, DashboardSnapshot, TerminalDashboard,
                                committed_freshness, dashboard_logging, render_dashboard)
from crypto_monitor.runtime.websocket_supervisor import ConnectionStatus
from tests.test_sqlite_store import START, D, MARKETS, event, hits_for
from tests.test_websocket_supervisor import ManualSleep


def snapshot(**changes):
    value = DashboardSnapshot(START + timedelta(minutes=2), ConnectionStatus(state="connected"),
        RefreshStatus(START, START, START), RecoveryStatus(0, 256, False, True,
        START + timedelta(minutes=1), None, None), tuple(MARKETS), False, DeliveryStatus(),
        DashboardData(0, None, None, (), tuple((market.symbol, START)
            for market in changes.get("markets", MARKETS))))
    return replace(value, **changes)


def rendered(value, width=80, color=False):
    stream = io.StringIO()
    console = Console(file=stream, width=width, force_terminal=color, no_color=not color,
                      color_system="standard" if color else None)
    console.print(render_dashboard(value, width=width))
    return stream.getvalue()


class RenderTests(unittest.TestCase):
    def test_compact_status_content_top_markets_and_no_full_symbol_list(self):
        markets = tuple(replace(MARKETS[0], symbol=f"COIN{i:02}USDT", quote_volume=D(100-i)) for i in range(20))
        text = rendered(snapshot(markets=markets))
        for expected in ("HEALTHY", "WebSocket connected", "reconnects 0", "Markets 20",
                         "COIN00USDT", "COIN02USDT", "+17 more", "closed 1m", "volume 2m", "levels 2m",
                         "Recovery queue 0/256", "Email disabled", "pending 0", "Gap: none", "Recent alerts: none"):
            self.assertIn(expected, text)
        self.assertIn("COIN04USDT", text)  # Five freshness rows; still no full list.
        self.assertNotIn("COIN05USDT", text)

    def test_states_colors_and_resolved_historical_errors(self):
        healthy = snapshot(connection=ConnectionStatus(state="connected", last_error="old outage"),
                           delivery=DeliveryStatus("old email error", START))
        self.assertEqual(healthy.health, "healthy")
        self.assertIn("old outage", rendered(healthy))
        for value, label, color in ((healthy, "healthy", "32"),
                                    (replace(healthy, data_error="locked"), "degraded", "33"),
                                    (replace(healthy, stopped=True), "stopped", "31")):
            with self.subTest(label=label):
                self.assertEqual(value.health, label)
                self.assertIn(label.upper(), rendered(value))
                self.assertIn(f"\x1b[{color}m", rendered(value, color=True))

    def test_degraded_conditions_and_unknown_are_not_reported_healthy(self):
        base = snapshot()
        for value in (replace(base, connection=ConnectionStatus(state="backoff", reconnect_attempts=3)),
                      replace(base, refresh=replace(base.refresh, levels_error="history unavailable")),
                      replace(base, recovery=replace(base.recovery, unresolved="missing minute")),
                      replace(base, recovery=replace(base.recovery, queue_depth=3)),
                      replace(base, recovery=replace(base.recovery, latest_closed_at=None)),
                      replace(base, now=START + timedelta(hours=1)),
                      replace(base, stored=None, data_error="read failed"),
                      replace(base, email_enabled=True, stored=DashboardData(2, "SES MessageRejected", START, ()))):
            with self.subTest(value=value):
                self.assertEqual(value.health, "degraded")
        text = rendered(replace(base, stored=None, recovery=replace(base.recovery, latest_closed_at=None)))
        self.assertIn("closed unknown", text)
        self.assertIn("pending unknown", text)

    def test_ranked_alert_table_content_and_ten_row_limit(self):
        alerts = []
        for minute in range(12):
            hit = replace(hits_for(event(minute))[0], captured_quote_volume=D(minute),
                          level_price=D("100.001"))
            alerts.append(StoredAlert(hit, "sent" if minute == 11 else "pending"))
        value = snapshot(stored=DashboardData(11, None, None, tuple(alerts)))
        text = rendered(value)
        self.assertIn("Ranked alerts", text)
        self.assertIn("UTC", text)
        self.assertIn("100.001", text)
        self.assertIn("Mon high", text)
        self.assertIn("sent", text)
        self.assertEqual(text.count("BTCUSDT"), 12)  # Ten alerts, market summary, freshness.
        self.assertLess(text.index("10-09 12:12:00"), text.index("10-09 12:11:00"))
        self.assertNotIn("10-09 12:01:00", text)

    def test_narrow_rendering_keeps_all_fields_without_overflow(self):
        hit = hits_for(event())[0]
        value = snapshot(stored=DashboardData(1, None, None, (StoredAlert(hit, "retry"),)))
        for width in (40, 60, 80):
            with self.subTest(width=width):
                text = rendered(value, width)
                self.assertTrue(all(len(row) <= width for row in text.splitlines()))
                for word in ("BTCUSDT", "high", "retry", "100.00"):
                    self.assertIn(word, text)
                self.assertIn("Vol", text) if width < 76 else self.assertIn("24h vol", text)

    def test_errors_are_truncated_and_markup_is_literal(self):
        value = snapshot(connection=ConnectionStatus(state="backoff", last_error="[bold]literal[/bold] " + "X"*300),
            recovery=replace(snapshot().recovery, unresolved="gap " + "Y"*300),
            delivery=DeliveryStatus("SES MessageRejected", START))
        text = rendered(value)
        self.assertIn("[bold]literal[/bold]", text)
        self.assertNotIn("X"*100, text)
        self.assertIn("…", text)
        self.assertIn("Email last error: SES MessageRejected", text)


class FreshnessTests(unittest.TestCase):
    def value(self, ages):
        """Age fixtures are minutes since committed opening, independent of feed age."""
        base = snapshot()
        markets = tuple(replace(MARKETS[0], symbol=symbol) for symbol in ages)
        stamps = tuple((symbol, base.now - timedelta(minutes=minutes))
                       for symbol, minutes in ages.items() if minutes is not None)
        return replace(base, markets=markets, stored=replace(base.stored, latest_committed=stamps))

    def test_fresh_stale_missing_and_strict_threshold(self):
        fresh = self.value({"BTCUSDT": 3})
        self.assertEqual(fresh.health, "healthy")  # Exactly threshold remains fresh.
        self.assertIn("3.0m", rendered(fresh))
        self.assertIn("\x1b[32m3.0m", rendered(fresh, color=True))
        stale = self.value({"BTCUSDT": 3.5})
        self.assertEqual(stale.health, "degraded")
        self.assertIn("3.5m stale", rendered(stale))
        self.assertIn("\x1b[33m3.5m stale", rendered(stale, color=True))
        missing = self.value({"BTCUSDT": None})
        self.assertEqual(missing.health, "degraded")
        self.assertIn("no baseline", rendered(missing))
        self.assertIn("\x1b[33mno baseline", rendered(missing, color=True))
        # The setting only changes presentation/health, not processing.
        relaxed = replace(stale, stale_after_minutes=4)
        self.assertEqual(relaxed.health, "healthy")
        self.assertNotIn("3.5m stale", rendered(relaxed))

    def test_missing_first_oldest_then_symbol_ties_and_only_five_shown(self):
        value = self.value({"Z": 1, "D": 4, "B": 8, "N": None, "A": 8, "M": None, "C": 5})
        self.assertEqual(committed_freshness(value),
                         [("M", None), ("N", None), ("A", 480), ("B", 480), ("C", 300), ("D", 240), ("Z", 60)])
        reversed_value = replace(value, markets=tuple(reversed(value.markets)), stored=replace(
            value.stored, latest_committed=tuple(reversed(value.stored.latest_committed)) + (("REMOVED", START),)))
        self.assertEqual(committed_freshness(value), committed_freshness(reversed_value))
        text = rendered(value)
        section = text[text.index("Committed age"):text.index("WS last error")]
        self.assertIn("+2 more", section)
        self.assertEqual(section.count("no baseline"), 2)
        self.assertEqual(section.count("stale"), 4)  # Header plus three stale rows.
        self.assertNotIn("4.0m", section)
        self.assertNotIn("1.0m", section)
        self.assertNotIn("REMOVED", rendered(reversed_value))

    def test_minutes_remain_minutes_and_narrow_view_preserves_age_labels(self):
        value = self.value({"BTCUSDT": 90, "ETHUSDT": None, "SOLUSDT": 1.5})
        for width in (40, 60, 80):
            text = rendered(value, width)
            self.assertTrue(all(len(row) <= width for row in text.splitlines()))
            for expected in ("UTC min", "90.0m stale", "no baseline", "1.5m", "+0 more"):
                self.assertIn(expected, text)
        failed = replace(value, stored=None, data_error="read failed")
        self.assertIn("Committed age: unavailable", rendered(failed))
        self.assertNotIn("no baseline", rendered(failed))


class ReadOnlyTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "dashboard.sqlite3"

    def test_readonly_reader_never_creates_missing_database(self):
        with self.assertRaises(sqlite3.OperationalError):
            SQLiteStore.open_readonly(self.path)
        self.assertFalse(self.path.exists())

    def test_latest_committed_open_times_reopen_no_hits_and_out_of_order(self):
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(4), [])
            store.commit_candle(event(1), [])  # Older arrival must not win.
            store.commit_candle(replace(event(2), symbol="ETHUSDT"), [])
        before = self.path.read_bytes()
        with SQLiteStore.open_readonly(self.path) as store:
            data = store.dashboard_data()
        self.assertEqual(data.latest_committed, (("BTCUSDT", START + timedelta(minutes=4)),
                                                ("ETHUSDT", START + timedelta(minutes=2))))
        self.assertEqual(data.alerts, ())
        self.assertEqual(self.path.read_bytes(), before)

    def test_consistent_outbox_counts_statuses_and_read_only_enforcement(self):
        hits = hits_for(event())
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(), hits)
            store.begin_email_attempt([hits[0].alert_id, hits[1].alert_id], now=START,
                                      next_attempt_at=START + timedelta(minutes=5))
            store.mark_email_sent([hits[0].alert_id], message_id="fixture", sent_at=START)
            store.record_email_failure([hits[1].alert_id], "SES MessageRejected")
        before = self.path.read_bytes()
        with SQLiteStore.open_readonly(self.path) as store:
            statements = []
            store._connection.set_trace_callback(statements.append)
            data = store.dashboard_data()
            self.assertEqual(data.pending_email_count, 3)
            self.assertEqual(data.delivery_error, "SES MessageRejected")
            self.assertEqual({row.hit.alert_id: row.email_status for row in data.alerts},
                {hits[0].alert_id: "sent", hits[1].alert_id: "retry",
                 hits[2].alert_id: "pending", hits[3].alert_id: "pending"})
            self.assertFalse(any(sql.strip().startswith(("CREATE", "INSERT", "UPDATE", "DELETE")) for sql in statements))
            with self.assertRaises(sqlite3.OperationalError):
                store.commit_candle(event(1), [])
        self.assertEqual(before, self.path.read_bytes())

    def test_only_newest_hundred_hits_are_candidates(self):
        with SQLiteStore(self.path) as store:
            for minute in range(26):
                current = event(minute, symbol=f"COIN{minute}USDT")
                hits = [replace(hit, symbol=current.symbol, alert_id=f"{minute}:{hit.alert_id}")
                        for hit in hits_for(event(minute))]
                store.commit_candle(current, hits)
        with SQLiteStore.open_readonly(self.path) as store:
            rows = store.dashboard_data().alerts
        self.assertEqual(len(rows), 100)
        self.assertTrue(all(row.hit.candle_time > START for row in rows))

    def test_inflight_attempt_is_pending_not_a_delivery_failure(self):
        hit = hits_for(event())[0]
        with SQLiteStore(self.path) as store:
            store.commit_candle(event(), [hit])
            store.begin_email_attempt([hit.alert_id], now=START, next_attempt_at=START + timedelta(minutes=5))
        with SQLiteStore.open_readonly(self.path) as store:
            data = store.dashboard_data()
        self.assertIsNone(data.delivery_error)
        self.assertEqual(data.alerts[0].email_status, "pending")


class AsyncDashboardTests(unittest.IsolatedAsyncioTestCase):
    async def test_collection_runs_database_reader_off_loop(self):
        base = snapshot(stale_after_minutes=7)
        entered, release = asyncio.Event(), threading.Event()
        loop, main_thread = asyncio.get_running_loop(), threading.get_ident()
        def read():
            self.assertNotEqual(threading.get_ident(), main_thread)
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):
                raise TimeoutError("test release missing")
            return base.stored
        collector = DashboardCollector("unused", SimpleNamespace(status=base.recovery),
            SimpleNamespace(status=base.connection), SimpleNamespace(status=base.refresh, markets=base.markets),
            now=lambda: base.now, reader=read, stale_after_minutes=7)
        task = asyncio.create_task(collector())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            self.assertFalse(task.done())
            await asyncio.sleep(0)  # Event loop remains responsive while DB work is blocked.
            release.set()
            self.assertEqual(await task, base)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_failed_read_is_unknown_degraded_snapshot(self):
        base = snapshot()
        collector = DashboardCollector("unused", SimpleNamespace(status=base.recovery),
            SimpleNamespace(status=base.connection), SimpleNamespace(status=base.refresh, markets=base.markets),
            now=lambda: base.now, reader=MagicMock(side_effect=sqlite3.OperationalError("locked")))
        result = await collector()
        self.assertIsNone(result.stored)
        self.assertEqual(result.health, "degraded")
        self.assertIn("OperationalError", result.data_error)

    async def test_live_manual_refresh_interval_stop_and_cancellation(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                timer, stop = ManualSleep(), asyncio.Event()
                collector, live = AsyncMock(return_value=snapshot()), MagicMock()
                factory = MagicMock(return_value=live)
                console = Console(file=io.StringIO(), width=80)
                dashboard = TerminalDashboard(collector, console=console, live_factory=factory, sleep=timer)
                task = asyncio.create_task(dashboard.run(stop))
                try:
                    delay, wake = await timer.next()
                    self.assertEqual(delay, 2)
                    self.assertEqual(collector.await_count, 1)
                    self.assertFalse(factory.call_args.kwargs["auto_refresh"])
                    wake.set_result(None)
                    await timer.next()
                    self.assertEqual(collector.await_count, 2)
                    if cancel:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        stop.set()
                        await asyncio.wait_for(task, 2)
                    live.__exit__.assert_called_once()
                    final = live.__enter__.return_value.update.call_args.args[0]
                    console.print(final)
                    self.assertIn("STOPPED", console.file.getvalue())
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_presentation_failure_falls_back_instead_of_raising(self):
        collector = AsyncMock(side_effect=RuntimeError("render unavailable"))
        with self.assertLogs("crypto_monitor.ui.terminal_dashboard", level="WARNING") as logs:
            await TerminalDashboard(collector).run(asyncio.Event())
        self.assertIn("continuing plain logs", "\n".join(logs.output))

    async def test_recovery_status_is_observational_and_handles_replays(self):
        worker = RecoveryWorker(MARKETS, "unused", MagicMock(), MagicMock(), capacity=2)
        worker._accepting = True
        await worker.submit(event(2))
        await worker.submit(event(1))
        self.assertEqual(worker.status.queue_depth, 2)
        self.assertEqual(worker.status.latest_closed_at, START + timedelta(minutes=3))
        self.assertEqual(worker.status.queue_capacity, 2)
        self.assertFalse(worker.status.processing)
        self.assertEqual(worker.status, worker.status)  # No dequeue or mutation from reading.


class LoggingTests(unittest.TestCase):
    def test_live_logging_restores_plain_handler_and_keeps_warnings(self):
        root = logging.getLogger()
        handlers, level = root.handlers[:], root.level
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root.handlers, root.level = [handler], logging.INFO
        console = Console(file=stream, width=80, force_terminal=False)
        try:
            with patch("sys.stdout", stream), dashboard_logging(console):
                root.info("hidden routine log")
                root.warning("visible warning [literal]")
                self.assertIsNot(root.handlers[0], handler)
            self.assertIs(root.handlers[0], handler)
            root.info("plain restored")
            self.assertNotIn("hidden routine log", stream.getvalue())
            self.assertIn("visible warning [literal]", stream.getvalue())
            self.assertIn("plain restored", stream.getvalue())
        finally:
            root.handlers, root.level = handlers, level
            handler.close()


if __name__ == "__main__":
    unittest.main()
