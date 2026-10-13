"""Current-state presentation without live connections, databases, or real TTYs."""

import asyncio
import io
import termios
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

from rich.console import Console

from crypto_monitor.ui.touch_dashboard import (
    TouchDashboard, fresh_events, keyboard_input, local_time, matching_symbols,
    render_loading, render_touch_dashboard,
)


NOW = datetime(2026, 10, 13, 13, 30, tzinfo=timezone.utc)


def row(symbol="FILUSDT", name="previous_week", side="high", **changes):
    return dict(symbol=symbol, name=name, side=side, level_price=Decimal("1.231"),
                status="swept", accepted_at=NOW - timedelta(minutes=5),
                eligible_at=NOW + timedelta(days=5), count=1, source="history",
                available=True, **changes)


def snapshot(**changes):
    return dict(now=NOW, markets=("FILUSDT", "ADAUSDT"),
                watches=(row(),), fresh=(), connection="connected",
                email_enabled=False, **changes)


def render(renderable, width=120):
    output = io.StringIO()
    Console(file=output, width=width, no_color=True).print(renderable)
    return output.getvalue()


class RenderTests(unittest.TestCase):
    def test_live_header_uses_sofia_and_excludes_operational_clutter(self):
        value = render(render_touch_dashboard(snapshot(), width=120))
        for expected in ("LIVE", "2 coins", "Email off", "16:30:00", "Europe/Sofia",
                         "FILUSDT", "ADAUSDT", "N/A", "E earlier"):
            self.assertIn(expected, value)
        for unwanted in ("UTC", "queue", "pending", "13:30:00"):
            self.assertNotIn(unwanted, value)

    def test_independent_high_low_and_expired_monthly_count(self):
        monthly = dict(row(name="previous_month", side="low"), status="untouched", count=4)
        value = render(render_touch_dashboard(dict(snapshot(), watches=(row(), monthly)), width=120))
        self.assertIn("-(4)", value)
        self.assertIn("N/A / E", value)
        self.assertIn("4", value)

    def test_cards_show_quiet_history_and_price_in_local_time(self):
        value = render(render_touch_dashboard(snapshot(), layout="cards", width=120))
        for expected in ("FILUSDT", "Previous week", "swept earlier", "1.231", "~13 Oct 16:25:00", "unavailable"):
            self.assertIn(expected, value)
        self.assertNotIn("NEW LIVE ACTIVATION", value)

    def test_quarter_expired_row_has_no_old_history_in_cards(self):
        quarterly = dict(row(name="previous_quarter"), status="untouched",
                         accepted_at=NOW - timedelta(days=60))
        value = render(render_touch_dashboard(dict(snapshot(), watches=(quarterly,)), layout="cards", width=120))
        self.assertIn("High: ready", value)
        self.assertNotIn("Aug", value)
        self.assertNotIn("swept earlier", value)

    def test_every_selected_market_is_accessible_through_paging(self):
        value = dict(snapshot(), markets=tuple(f"COIN{i:03}USDT" for i in range(100)))
        seen = ""
        for page in range(10):
            current = render(render_touch_dashboard(value, page=page, page_size=10, width=120))
            self.assertIn(f"Page {page + 1}/10", current)
            seen += current
        for number in range(100):
            self.assertEqual(seen.count(f"COIN{number:03}USDT"), 1)

    def test_swept_filter_includes_either_independent_side_only(self):
        self.assertEqual(matching_symbols(snapshot(), "swept"), ["FILUSDT"])
        value = render(render_touch_dashboard(snapshot(), filter_mode="swept", width=120))
        self.assertNotIn("ADAUSDT", value)
        self.assertIn("1 swept coins", value)

    def test_only_recent_live_events_receive_fresh_emphasis(self):
        history = dict(row(), observed_at=NOW, source="history")
        live = dict(row(), observed_at=NOW - timedelta(seconds=3), source="live")
        expired = dict(row(symbol="OLDUSDT"), observed_at=NOW - timedelta(minutes=2), source="live")
        value = dict(snapshot(), fresh=(history, live, expired))
        self.assertEqual(fresh_events(value, 60), [live])
        printed = render(render_touch_dashboard(value, width=120))
        self.assertIn("NEW LIVE ACTIVATION", printed)
        self.assertIn("16:29:57", printed)
        self.assertNotIn("OLDUSDT", printed)

    def test_loading_counts_no_false_eta_or_alert_panel(self):
        value = render(render_loading(dict(total=100, references_done=100, history_done=37,
                                           stage="Checking history", symbol="FILUSDT"), width=120))
        for expected in ("LOADING", "100 / 100", "37 / 100", "FILUSDT", "quietly", "live handoff"):
            self.assertIn(expected, value)
        for unwanted in ("ETA", "%", "NEW LIVE ACTIVATION"):
            self.assertNotIn(unwanted, value)

    def test_external_markup_and_control_characters_are_literal(self):
        value = render(render_touch_dashboard(dict(snapshot(), error="[bold]literal[/bold]\x1b[31m"), width=120))
        self.assertIn("[bold]literal[/bold]", value)
        self.assertNotIn("\x1b", value)

    def test_narrow_layouts_fit_terminal_width(self):
        for layout in ("table", "cards"):
            for width in (40, 60, 100):
                with self.subTest(layout=layout, width=width):
                    value = render(render_touch_dashboard(snapshot(), layout=layout, width=width), width=width)
                    self.assertTrue(all(len(line) <= width for line in value.splitlines()))
                    self.assertIn("FILUSDT", value)

    def test_local_time_uses_sofia_daylight_saving_rules(self):
        self.assertEqual(local_time(NOW), "16:30:00")
        winter = NOW.replace(month=12)
        self.assertEqual(local_time(winter), "15:30:00")


class KeyboardTests(unittest.TestCase):
    def test_layout_filter_page_and_quit_switches(self):
        dashboard = TouchDashboard(None, console=Console(width=120, height=40), page_size=1)
        stop = asyncio.Event()
        dashboard.handle_key("n", stop, snapshot())
        self.assertEqual(dashboard.page, 1)
        dashboard.handle_key("c", stop, snapshot())
        self.assertEqual((dashboard.layout, dashboard.page), ("cards", 0))
        dashboard.handle_key("s", stop, snapshot())
        self.assertEqual(dashboard.filter_mode, "swept")
        dashboard.handle_key("t", stop, snapshot())
        dashboard.handle_key("a", stop, snapshot())
        self.assertEqual((dashboard.layout, dashboard.filter_mode), ("table", "all"))
        dashboard.handle_key("q", stop, snapshot())
        self.assertTrue(stop.is_set())

    def test_keyboard_restores_terminal_without_changing_blocking_when_body_fails(self):
        stream = MagicMock()
        stream.isatty.return_value = True
        stream.fileno.return_value = 17
        controls = [b"\x00"] * termios.NCCS
        controls[termios.VMIN] = b"\x01"
        old_settings = [1, 2, 3, 4, 5, 6, controls]
        with patch("crypto_monitor.ui.touch_dashboard.termios.tcgetattr", return_value=old_settings), \
             patch("crypto_monitor.ui.touch_dashboard.termios.tcsetattr") as restore, \
             patch("crypto_monitor.ui.touch_dashboard.tty.setcbreak") as cbreak, \
             patch("crypto_monitor.ui.touch_dashboard.os.set_blocking") as blocking:
            with self.assertRaisesRegex(RuntimeError, "fixture"):
                with keyboard_input(stream):
                    raise RuntimeError("fixture")
            cbreak.assert_called_once()
            self.assertEqual(restore.call_count, 2)
            polling_settings = restore.call_args_list[0].args[2]
            self.assertEqual(polling_settings[6][termios.VMIN], 0)
            self.assertEqual(polling_settings[6][termios.VTIME], 0)
            self.assertEqual(restore.call_args.args[2], old_settings)
            self.assertEqual(old_settings[6][termios.VMIN], b"\x01")
            blocking.assert_not_called()

    def test_non_tty_does_not_change_terminal(self):
        stream = io.StringIO()
        with patch("crypto_monitor.ui.touch_dashboard.termios.tcgetattr") as attributes:
            with keyboard_input(stream) as read:
                self.assertEqual(read(), "")
        attributes.assert_not_called()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_closes_live_and_renders_final_stopped_state(self):
        stop = asyncio.Event()
        calls = 0

        async def collector():
            nonlocal calls
            calls += 1
            if calls == 2:
                stop.set()
            return snapshot()

        live = MagicMock()
        live.__enter__.return_value = live
        factory = MagicMock(return_value=live)
        dashboard = TouchDashboard(collector, console=Console(width=120), input_stream=io.StringIO(), live_factory=factory)
        await dashboard.run(stop)
        live.__exit__.assert_called_once()
        final = render(live.update.call_args.args[0])
        self.assertIn("STOPPED", final)

    async def test_cancel_loading_closes_live(self):
        live = MagicMock()
        live.__enter__.return_value = live
        dashboard = TouchDashboard(None, console=Console(width=120), live_factory=MagicMock(return_value=live))
        task = asyncio.create_task(dashboard.run_loading(asyncio.Event(), {}))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        live.__exit__.assert_called_once()
