"""Current touch states, Sofia time, and selectable terminal layouts.

The renderers accept plain snapshots so presentation never opens a market
connection or writes monitor state. Keyboard settings are restored on exit.
"""

import asyncio
import inspect
import math
import os
import select
import sys
import termios
import tty
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from crypto_monitor.ui.terminal_dashboard import dashboard_logging


SOFIA = ZoneInfo("Europe/Sofia")
GROUPS = (
    ("monday", "Monday"),
    ("previous_week", "Previous week"),
    ("previous_month", "Previous month"),
    ("previous_quarter", "Previous quarter"),
)


def clean(value):
    """Render external values literally, excluding terminal control characters."""
    return "".join(character if character.isprintable() else " " for character in str(value))


def text(value, style=""):
    return Text(clean(value), style=style)


def local_time(value, *, seconds=True, date=False):
    if not isinstance(value, datetime):
        return "unavailable"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    pattern = "%d %b %H:%M:%S" if date and seconds else "%d %b %H:%M" if date else "%H:%M:%S" if seconds else "%H:%M"
    return value.astimezone(SOFIA).strftime(pattern)


def price(value):
    if value is None:
        return "unavailable"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    return clean(value)


def _row_name(row):
    return row.get("name", row.get("level_name", ""))


def _status(row):
    if row is None or row.get("available") is False:
        return "unavailable"
    value = row.get("status", "unavailable")
    return "untouched" if value in ("ready", "eligible") else value


def _source(row):
    return row.get("source", row.get("origin", "history"))


def _accepted(row):
    return row.get("accepted_at", row.get("last_accepted_at", row.get("touched_at")))


def _count(row):
    return row.get("count", row.get("activation_count", 0))


def _symbols(snapshot):
    symbols = []
    for market in snapshot.get("markets", ()):
        symbol = market if isinstance(market, str) else market.get("symbol") if isinstance(market, dict) else market.symbol
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        symbols = sorted({row["symbol"] for row in snapshot.get("watches", ())})
    return symbols


def _index(snapshot):
    return {(row["symbol"], _row_name(row), row["side"]): row for row in snapshot.get("watches", ())}


def sweep_rank(symbol, index):
    """Position of the highest timeframe currently swept: quarter 0 … Monday 3, none 4.

    Only the timeframe matters. Repeat counts and sweeping both sides do not
    raise a coin above another that swept the same timeframe once.
    """
    for rank, (name, _) in enumerate(reversed(GROUPS)):
        if any(_status(index.get((symbol, name, side))) == "swept" for side in ("low", "high")):
            return rank
    return len(GROUPS)


def matching_symbols(snapshot, filter_mode="all"):
    """Same coins, higher-timeframe sweeps first; selection order breaks ties."""
    symbols, index = _symbols(snapshot), _index(snapshot)
    ranked = sorted(symbols, key=lambda symbol: sweep_rank(symbol, index))
    if filter_mode == "swept":
        return [symbol for symbol in ranked if sweep_rank(symbol, index) < len(GROUPS)]
    return ranked


def _state_cell(row, name):
    status = _status(row)
    if status == "unavailable":
        return Text("N/A", style="dim")
    if status == "swept":
        label, style = ("S", "green") if _source(row) == "live" else ("E", "cyan")
    else:
        label, style = "-", "dim"
    if name == "previous_month" and _count(row):
        label += f"({_count(row)})"
    return Text(label, style=style)


def _state_words(row):
    status = _status(row)
    if status == "unavailable":
        return "unavailable", "dim"
    if status == "swept":
        return ("swept live", "green") if _source(row) == "live" else ("swept earlier", "cyan")
    return "ready", "dim"


def _table(symbols, index, *, width):
    narrow = width < 76
    table = Table(box=box.SIMPLE, expand=True, padding=(0, 1), show_edge=False)
    table.add_column("Coin", no_wrap=True)
    if narrow:
        table.add_column("Low / High", ratio=1)
    else:
        for label in ("Monday", "Week", "Month", "Quarter"):
            table.add_column(f"{label} L/H", ratio=1)
    for symbol in symbols:
        cells = []
        for name, label in GROUPS:
            cell = Text()
            if narrow:
                cell.append(f"{label}: ")
            cell.append_text(_state_cell(index.get((symbol, name, "low")), name))
            cell.append(" / ")
            cell.append_text(_state_cell(index.get((symbol, name, "high")), name))
            cells.append(cell)
        if narrow:
            joined = Text()
            for number, cell in enumerate(cells):
                if number:
                    joined.append("\n")
                joined.append_text(cell)
            table.add_row(text(symbol), joined)
        else:
            table.add_row(text(symbol), *cells)
    return table


def _cards(symbols, index):
    cards = []
    for symbol in symbols:
        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(no_wrap=True)
        table.add_column(ratio=1)
        for name, label in GROUPS:
            parts = Text()
            for number, side in enumerate(("low", "high")):
                if number:
                    parts.append("\n")
                row = index.get((symbol, name, side))
                words, style = _state_words(row)
                parts.append(f"{side.title()}: ")
                parts.append(words, style=style)
                if _status(row) != "unavailable":
                    parts.append(f"  {price(row.get('level_price'))}", style="dim")
                    if name == "previous_month":
                        parts.append(f"  count {_count(row)}", style="dim")
                    if _status(row) == "swept":
                        stamp = _accepted(row)
                        approximate = "~" if row.get("approximate", _source(row) != "live") else ""
                        parts.append(f"  {approximate}{local_time(stamp, date=True)}", style="dim")
            table.add_row(text(label), parts)
        cards.append(Panel(table, title=text(symbol, "bold"), border_style="dim", padding=(0, 1)))
    return Group(*cards)


def fresh_events(snapshot, highlight_seconds):
    now = snapshot.get("now")
    if not isinstance(now, datetime):
        return []
    result = []
    for event in snapshot.get("fresh", ()):
        if event.get("source", "live") not in ("live", "fresh"):
            continue
        stamp = event.get("observed_at", event.get("touched_at", event.get("accepted_at")))
        if isinstance(stamp, datetime) and 0 <= (now - stamp).total_seconds() <= highlight_seconds:
            result.append(event)
    return result


def _fresh(snapshot, highlight_seconds):
    events = fresh_events(snapshot, highlight_seconds)
    if not events:
        return Text("Fresh: no new activations", style="dim")
    rows = []
    # This panel is transient attention, not the complete current-state list.
    for event in events[:3]:
        stamp = event.get("observed_at", event.get("touched_at", event.get("accepted_at")))
        label = dict(GROUPS).get(_row_name(event), _row_name(event))
        rows.append(text(f"{local_time(stamp)}  {event['symbol']}  {label} {event['side']}  "
                         f"{price(event.get('level_price', event.get('price')))}", "bold green"))
    if len(events) > 3:
        rows.append(text(f"+{len(events) - 3} more fresh activations; all states are shown below", "dim"))
    return Panel(Group(*rows), title="NEW LIVE ACTIVATION", border_style="green", padding=(0, 1))


def page_capacity(layout, *, width=100, height=40):
    if layout == "cards":
        return max(1, (height - 19) // 10)
    return max(1, (height - 19) // (4 if width < 76 else 1))


def render_touch_dashboard(snapshot, *, layout="table", page=0, page_size=None,
                           filter_mode="all", width=100, height=40, highlight_seconds=60):
    """Render every market through pages; no alert-history truncation."""
    if layout not in ("table", "cards"):
        raise ValueError("layout must be table or cards")
    width = max(30, width)
    page_size = page_size or page_capacity(layout, width=width, height=height)
    if page_size < 1:
        raise ValueError("page_size must be positive")
    symbols, index = matching_symbols(snapshot, filter_mode), _index(snapshot)
    pages = max(1, math.ceil(len(symbols) / page_size))
    page = max(0, min(page, pages - 1))
    shown = symbols[page * page_size:(page + 1) * page_size]
    state = snapshot.get("connection", "connecting")
    state = getattr(state, "state", state)
    stopped = snapshot.get("stopped", False)
    label = "STOPPED" if stopped else "LIVE" if state == "connected" else "RECONNECTING" if state in ("backoff", "connecting", "disconnected") else clean(state).upper()
    style = "dim" if stopped else "green" if state == "connected" else "yellow"
    email_enabled = snapshot.get("email_enabled", False)
    header = Text.assemble(("CRYPTO MONITOR  ", "bold"), (label, f"bold {style}"),
                          "\n" if width < 76 else "  ",
                          (f"Email {'ON' if email_enabled else 'OFF'}", "bold green" if email_enabled else "bold red"),
                          (" (e)", "dim"), f"  {len(_symbols(snapshot))} coins")
    rows = [header, text(f"{local_time(snapshot.get('now'), date=True)} Europe/Sofia", "dim"),
            _fresh(snapshot, highlight_seconds)]
    if snapshot.get("error"):
        rows.append(text(f"Attention: {snapshot['error']}", "yellow"))
    if not shown:
        rows.append(text("No swept levels" if filter_mode == "swept" else "No selected coins", "dim"))
    else:
        rows.append(_cards(shown, index) if layout == "cards" else _table(shown, index, width=width))
    if layout == "table":
        rows.append(text("E earlier · S live · - ready · N/A unavailable · (n) monthly count · L/H low/high", "dim"))
    unavailable = sum(_status(index.get((symbol, name, side))) == "unavailable"
                      for symbol in _symbols(snapshot) for name, _ in GROUPS for side in ("low", "high"))
    rows.append(text(f"Page {page + 1}/{pages} · {len(symbols)} {'swept' if filter_mode == 'swept' else 'selected'} coins · {unavailable} unavailable levels", "dim"))
    rows.append(text("t table · c cards · n/p pages · a all · s swept · e email · q quit", "dim"))
    return Panel(Group(*rows), width=width, border_style=style, padding=(0, 1))


def render_loading(progress, *, width=100):
    """Loading shows completed work, never an invented ETA or percentage."""
    total = progress.get("total", progress.get("total_markets", progress.get("markets_total")))
    references = progress.get("references_done", progress.get("levels_done", progress.get("reference_done", 0)))
    history = progress.get("history_done", progress.get("history_completed", 0))
    rows = [text("CRYPTO MONITOR  LOADING", "bold cyan")]
    if progress.get("stage"):
        rows.append(text(progress["stage"]))
    if total is not None:
        rows.append(text(f"Reference levels   {references} / {total} coins checked"))
        rows.append(text(f"Past touches       {history} / {total} coins checked"))
        if "catchup_done" in progress:
            rows.append(text(f"Loading catch-up   {progress['catchup_done']} / {total} coins checked"))
        if "handoff_done" in progress:
            rows.append(text(f"Current minutes    {progress['handoff_done']} / {total} coins checked"))
    else:
        rows.append(text("Selecting markets and preparing reference levels…", "dim"))
    if progress.get("symbol"):
        rows.append(text(f"Checking {progress['symbol']}", "dim"))
    if progress.get("error"):
        rows.append(text(f"Attention: {progress['error']}", "yellow"))
    rows.extend((Text("Recording earlier touches quietly.", style="dim"),
                 Text("Fresh alerts begin when loading and the live handoff finish.", style="dim"),
                 Text("Ctrl+C to stop", style="dim")))
    return Panel(Group(*rows), width=max(30, width), border_style="cyan", padding=(1, 1))


@contextmanager
def keyboard_input(stream):
    """Poll real TTY input without changing shared descriptor blocking flags.

    A shell may duplicate stdin and stdout from one open terminal description.
    Setting O_NONBLOCK on stdin then changes stdout too, which can make Rich
    rendering fail with BlockingIOError. VMIN/VTIME affect input reads only.
    """
    if stream is None or not stream.isatty():
        yield lambda: ""
        return
    descriptor = stream.fileno()
    settings = termios.tcgetattr(descriptor)
    try:
        tty.setcbreak(descriptor, termios.TCSANOW)
        polling_settings = termios.tcgetattr(descriptor)
        polling_settings = polling_settings[:]
        polling_settings[6] = polling_settings[6][:]
        polling_settings[6][termios.VMIN] = 0
        polling_settings[6][termios.VTIME] = 0
        termios.tcsetattr(descriptor, termios.TCSANOW, polling_settings)

        def read():
            if select.select([descriptor], [], [], 0)[0]:
                try:
                    return os.read(descriptor, 64).decode("utf-8", errors="ignore")
                except (BlockingIOError, InterruptedError):
                    return ""
            return ""

        yield read
    finally:
        termios.tcsetattr(descriptor, termios.TCSANOW, settings)


class TouchDashboard:
    def __init__(self, collector, *, layout="table", highlight_seconds=60, console=None,
                 input_stream=None, interval=0.2, live_factory=Live, page_size=None,
                 on_toggle_email=None):
        if layout not in ("table", "cards"):
            raise ValueError("layout must be table or cards")
        if isinstance(highlight_seconds, bool) or not math.isfinite(highlight_seconds) or highlight_seconds <= 0:
            raise ValueError("highlight_seconds must be positive and finite")
        if not 0 < interval <= 0.25:
            raise ValueError("dashboard interval must be between zero and 0.25 seconds")
        self.collector = collector
        self.layout, self.highlight_seconds = layout, highlight_seconds
        self.console = console or Console(file=sys.stdout)
        self.input_stream = sys.stdin if input_stream is None else input_stream
        self.interval, self.live_factory, self.page_size = interval, live_factory, page_size
        self.on_toggle_email = on_toggle_email
        self.page, self.filter_mode = 0, "all"

    def handle_key(self, key, stop, snapshot):
        capacity = self.page_size or page_capacity(self.layout, width=self.console.width, height=self.console.height)
        pages = max(1, math.ceil(len(matching_symbols(snapshot, self.filter_mode)) / capacity))
        if key == "q":
            stop.set()
        elif key in ("t", "c"):
            self.layout, self.page = ("table" if key == "t" else "cards"), 0
        elif key == "n":
            self.page = (self.page + 1) % pages
        elif key == "p":
            self.page = (self.page - 1) % pages
        elif key in ("a", "s"):
            self.filter_mode, self.page = ("all" if key == "a" else "swept"), 0
        elif key == "e" and self.on_toggle_email is not None:
            return self.on_toggle_email()

    def render(self, snapshot):
        return render_touch_dashboard(snapshot, layout=self.layout, page=self.page, page_size=self.page_size,
                                      filter_mode=self.filter_mode, width=self.console.width,
                                      height=self.console.height, highlight_seconds=self.highlight_seconds)

    async def _tick(self, stop):
        try:
            await asyncio.wait_for(stop.wait(), timeout=self.interval)
        except asyncio.TimeoutError:
            pass

    async def run_loading(self, stop, progress):
        with dashboard_logging(self.console), self.live_factory(render_loading(progress, width=self.console.width), console=self.console,
                               auto_refresh=False, screen=True, redirect_stdout=False,
                               redirect_stderr=False, vertical_overflow="ellipsis") as live:
            while not stop.is_set():
                live.update(render_loading(progress, width=self.console.width), refresh=True)
                await self._tick(stop)

    async def run(self, stop):
        pending_toggle = None
        try:
            snapshot = await self.collector()
            with dashboard_logging(self.console), keyboard_input(self.input_stream) as read, self.live_factory(
                    self.render(snapshot), console=self.console, auto_refresh=False, screen=True,
                    redirect_stdout=False, redirect_stderr=False, vertical_overflow="ellipsis") as live:
                while not stop.is_set():
                    if pending_toggle is not None and pending_toggle.done():
                        pending_toggle.result()
                        pending_toggle = None
                    for key in read().lower():
                        if key == "e" and pending_toggle is not None:
                            continue
                        result = self.handle_key(key, stop, snapshot)
                        if inspect.isawaitable(result):
                            pending_toggle = asyncio.ensure_future(result)
                        if stop.is_set():
                            break
                    if stop.is_set():
                        break
                    snapshot = await self.collector()
                    live.update(self.render(snapshot), refresh=True)
                    await self._tick(stop)
                live.update(self.render(dict(snapshot, stopped=True)), refresh=True)
        finally:
            if pending_toggle is not None:
                # Disabling may finish an in-flight email before updating its
                # bookkeeping. Keep that work alive even when the UI is cancelled.
                cancelled = False
                while not pending_toggle.done():
                    try:
                        await asyncio.shield(pending_toggle)
                    except asyncio.CancelledError:
                        if pending_toggle.cancelled():
                            raise
                        cancelled = True
                pending_toggle.result()
                if cancelled:
                    raise asyncio.CancelledError


# Concise aliases for callers and standalone rendering previews.
Dashboard = TouchDashboard
render_dashboard = render_touch_dashboard
