"""Read-only snapshots and a compact Rich Live view. No I/O on import."""

import asyncio
import logging
import math
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from crypto_monitor.adapters.binance_rest import DiscoveredMarket
from crypto_monitor.runtime.gap_recovery import RecoveryStatus
from crypto_monitor.core.market_universe import rank_alerts
from crypto_monitor.runtime.refresh_coordinator import RefreshStatus
from crypto_monitor.adapters.ses_email import DeliveryStatus
from crypto_monitor.persistence.sqlite_store import DashboardData, SQLiteStore
from crypto_monitor.runtime.websocket_supervisor import ConnectionStatus


LOG = logging.getLogger(__name__)
HEALTH_COLORS = {"healthy": "green", "degraded": "yellow", "stopped": "red"}


def age_seconds(now, then):
    return None if then is None else max(0, (now - then).total_seconds())


@dataclass(frozen=True)
class DashboardSnapshot:
    now: datetime
    connection: ConnectionStatus
    refresh: RefreshStatus
    recovery: RecoveryStatus
    markets: tuple[DiscoveredMarket, ...]
    email_enabled: bool
    delivery: DeliveryStatus
    stored: DashboardData | None
    data_error: str | None = None
    stopped: bool = False
    stale_after_minutes: int = 3

    def __post_init__(self):
        if type(self.stale_after_minutes) is not int or self.stale_after_minutes <= 0:
            raise ValueError("stale_after_minutes must be a positive integer")

    @property
    def health(self):
        if self.stopped:
            return "stopped"
        closed_age = age_seconds(self.now, self.recovery.latest_closed_at)
        if (self.connection.state != "connected" or self.refresh.degraded
                or not self.recovery.running or self.recovery.error or self.recovery.unresolved
                or self.recovery.queue_depth or self.recovery.processing
                or closed_age is None or closed_age > 120
                or age_seconds(self.now, self.refresh.volume_updated_at) > 360
                or age_seconds(self.now, self.refresh.levels_updated_at) > 86520
                or self.data_error or self.stored is None
                or any(seconds is None or seconds > self.stale_after_minutes * 60
                       for _, seconds in committed_freshness(self))
                or (self.email_enabled and self.stored.delivery_error)):
            return "degraded"
        return "healthy"


class DashboardCollector:
    def __init__(self, database, worker, supervisor, coordinator, *, email_enabled=False,
                 email=None, now=lambda: datetime.now(timezone.utc), reader=None, stale_after_minutes=3):
        self.database, self.worker, self.supervisor, self.coordinator = database, worker, supervisor, coordinator
        self.email_enabled, self.email, self.now = email_enabled, email, now
        self.reader = reader or self._read
        self.stale_after_minutes = stale_after_minutes

    def _read(self):
        with SQLiteStore.open_readonly(self.database) as store:
            return store.dashboard_data()

    async def __call__(self):
        # Copy event-loop-owned status before awaiting the independent DB reader.
        snapshot = DashboardSnapshot(self.now(), self.supervisor.status, self.coordinator.status,
            self.worker.status, tuple(self.coordinator.markets), self.email_enabled,
            self.email.status if self.email is not None else DeliveryStatus(), None,
            stale_after_minutes=self.stale_after_minutes)
        work = asyncio.create_task(asyncio.to_thread(self.reader))
        try:
            await asyncio.wait((work,))
            try:
                return replace(snapshot, stored=work.result())
            except (sqlite3.Error, ValueError) as exc:
                return replace(snapshot, data_error=f"status read unavailable: {type(exc).__name__}")
        finally:
            await asyncio.gather(work, return_exceptions=True)


def clean(value):
    # All external text stays literal: no Rich markup or terminal control sequences.
    return "".join(character if character.isprintable() else " " for character in str(value))


def line(value, width, style=""):
    text = Text(clean(value), style=style, no_wrap=True, overflow="ellipsis")
    text.truncate(max(1, width), overflow="ellipsis")
    return text


def age(now, then):
    seconds = age_seconds(now, then)
    if seconds is None:
        return "unknown"
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds // 86400)}d"


def number(value, *, volume=False):
    if volume:
        for suffix, scale in (("T", Decimal("1e12")), ("B", Decimal("1e9")),
                              ("M", Decimal("1e6")), ("K", Decimal("1e3"))):
            if value >= scale:
                return f"{value / scale:.2f}{suffix}"
    text = str(value)
    return text if len(text) <= 12 else f"{value:.6g}"


def committed_freshness(snapshot):
    """Monitored symbols only; missing first, oldest next, alphabetical ties.

    Ages use committed opening times (not receipt times or candle ending times).
    A failed database read is unknown, not evidence that baselines are missing.
    """
    if snapshot.stored is None:
        return []
    latest = dict(snapshot.stored.latest_committed)
    rows = [(market.symbol, age_seconds(snapshot.now, latest.get(market.symbol)))
            for market in snapshot.markets]
    return sorted(rows, key=lambda row: (row[1] is not None,
                                         -row[1] if row[1] is not None else 0, row[0]))


def freshness_view(snapshot, width):
    inner = width - 4
    if snapshot.stored is None:
        return line("Committed age: unavailable", inner, "yellow")
    entries = committed_freshness(snapshot)
    remaining = max(0, len(entries) - 5)
    detail = f"stale >{snapshot.stale_after_minutes}m | +{remaining} more"
    title = "Committed age (UTC min)"
    rows = ([line(f"{title} | {detail}", inner, "dim")] if width >= 76 else
            [line(title, inner, "dim"), line(detail, inner, "dim")])
    if not entries:
        return Group(*rows, line("No monitored markets", inner, "dim"))
    pairs = 2 if width >= 76 else 1
    table = Table.grid(expand=True, padding=(0, 1))
    for _ in range(pairs):
        table.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
        table.add_column(no_wrap=True, overflow="ellipsis")
    cells = []
    for symbol, seconds in entries[:5]:
        stale = seconds is not None and seconds > snapshot.stale_after_minutes * 60
        label = "no baseline" if seconds is None else f"{seconds / 60:.1f}m" + (" stale" if stale else "")
        style = "yellow" if seconds is None or stale else "green"
        cells.extend((Text(clean(symbol)), Text(label, style=style)))
    columns = pairs * 2
    for offset in range(0, len(cells), columns):
        row = cells[offset:offset + columns]
        table.add_row(*row, *([""] * (columns - len(row))))
    return Group(*rows, table)


def render_dashboard(snapshot: DashboardSnapshot, *, width=80):
    """Pure layout: no clients, clocks, files or running service required."""
    width = max(20, min(width, 80))
    inner = width - 4
    status = Text("CRYPTO MONITOR  ", style="bold")
    status.append(snapshot.health.upper(), style=f"bold {HEALTH_COLORS[snapshot.health]}")
    if width >= 60:
        status.append(f"  {snapshot.now:%H:%M:%S} UTC", style="dim")
    status.truncate(inner, overflow="ellipsis")
    connection, recovery = snapshot.connection, snapshot.recovery
    markets = sorted(snapshot.markets, key=lambda item: (-item.quote_volume, item.symbol))
    shown = 3 if width >= 76 else 2 if width >= 60 else 1
    symbols = ", ".join(market.symbol for market in markets[:shown]) or "none"
    if len(markets) > shown:
        symbols += f"  +{len(markets) - shown} more"
    pending = str(snapshot.stored.pending_email_count) if snapshot.stored else "unknown"
    rows = [status,
        line(f"WebSocket {connection.state} | reconnects {connection.reconnect_attempts}", inner,
             "green" if connection.state == "connected" else "yellow"),
        line(f"Markets {len(markets)} | {symbols}", inner),
        line(f"Age: closed {age(snapshot.now, recovery.latest_closed_at)} | "
             f"volume {age(snapshot.now, snapshot.refresh.volume_updated_at)}" +
             (f" | levels {age(snapshot.now, snapshot.refresh.levels_updated_at)}" if width >= 60 else ""), inner),
        line(f"Recovery queue {recovery.queue_depth}/{recovery.queue_capacity}"
             f" | in progress {int(recovery.processing)}", inner),
        line(f"Email {'enabled' if snapshot.email_enabled else 'disabled'} | pending {pending}", inner)]
    if width < 60:
        rows.insert(1, line(f"{snapshot.now:%Y-%m-%d %H:%M:%S} UTC", inner, "dim"))
        rows.insert(5, line(f"Levels age {age(snapshot.now, snapshot.refresh.levels_updated_at)}", inner))
    rows.append(freshness_view(snapshot, width))
    delivery_error = snapshot.delivery.last_error
    if snapshot.stored and snapshot.stored.delivery_error and (
            snapshot.delivery.last_error_at is None or snapshot.stored.delivery_error_at is None
            or snapshot.stored.delivery_error_at >= snapshot.delivery.last_error_at):
        delivery_error = snapshot.stored.delivery_error
    errors = [("WS last error", connection.last_error),
              ("Gap", recovery.unresolved or snapshot.refresh.unresolved),
              ("Recovery error", recovery.error if not recovery.unresolved else None),
              ("Refresh", snapshot.refresh.volume_error or snapshot.refresh.membership_error or snapshot.refresh.levels_error),
              ("Email last error", delivery_error), ("View", snapshot.data_error)]
    # Always expose last error / gap fields; truncate rather than flooding the display.
    for label, error in errors:
        if error or label in ("WS last error", "Gap", "Email last error"):
            rows.append(line(f"{label}: {error or 'none'}", inner, "yellow" if error else "dim"))

    candidates = snapshot.stored.alerts if snapshot.stored else ()
    by_id = {entry.hit.alert_id: entry for entry in candidates}
    hits = rank_alerts(entry.hit for entry in candidates)[:10]
    table = Table(box=box.SIMPLE, expand=True, padding=(0, 1), show_edge=False,
                  title="Ranked alerts · newest 100 · once per level · UTC / USDT", title_style="dim")
    narrow = width < 76
    if narrow:
        table.add_column("Time / symbol", ratio=2, overflow="fold")
        table.add_column("Sweep / price / 24h quote / email", ratio=3, overflow="fold")
    else:
        for name, size in (("Detected UTC", 14), ("Symbol", 9), ("Level/side", 12),
                           ("Price", 8), ("24h vol", 8), ("Email", 7)):
            table.add_column(name, width=size, min_width=size if name in ("Detected UTC", "Email") else None,
                             no_wrap=True, overflow="ellipsis")
    names = {"monday": "Mon", "previous_week": "Prev wk", "previous_month": "Prev mo", "previous_quarter": "Prev qtr"}
    for hit in hits:
        state = by_id[hit.alert_id].email_status
        email_style = "green" if state == "sent" else "yellow" if state in ("pending", "retry") else "dim"
        level = f"{names.get(hit.level_name, hit.level_name)} {hit.side}"
        stamp = hit.detected_at.strftime("%m-%d %H:%M:%S")
        price, volume = number(hit.level_price), number(hit.captured_quote_volume, volume=True)
        if narrow:
            table.add_row(Text(f"{stamp}\n{clean(hit.symbol)}"),
                          Text.assemble(f"{clean(level)}\nPrice {price}\nVol {volume} | ", (state, email_style)))
        else:
            table.add_row(Text(stamp), Text(clean(hit.symbol)), Text(clean(level)), Text(price),
                          Text(volume), Text(state, style=email_style))
    if not hits:
        rows.append(line("Recent alerts: none" if snapshot.stored else "Recent alerts: unavailable", inner, "dim"))
    else:
        rows.append(table)
    return Panel(Group(*rows), width=width, padding=(0, 1), border_style=HEALTH_COLORS[snapshot.health])


@contextmanager
def dashboard_logging(console):
    """Warnings/errors above Live; retain file/capture handlers and restore stdout logs."""
    root, replacements = logging.getLogger(), []
    for index, handler in enumerate(root.handlers):
        if isinstance(handler, logging.StreamHandler) and getattr(handler, "stream", None) is sys.stdout:
            rich_handler = RichHandler(console=console, show_time=False, show_level=False, show_path=False,
                                       markup=False, highlighter=None)
            rich_handler.setLevel(max(handler.level, logging.WARNING))
            rich_handler.setFormatter(handler.formatter)
            replacements.append((handler, rich_handler))
            root.handlers[index] = rich_handler
    try:
        yield
    finally:
        for original, replacement in replacements:
            if replacement in root.handlers:
                root.handlers[root.handlers.index(replacement)] = original
            replacement.close()


class TerminalDashboard:
    def __init__(self, collector, *, console=None, live_factory=Live, sleep=asyncio.sleep, interval=2):
        if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval < 1:
            raise ValueError("dashboard interval must be at least one second")
        self.collector, self.console = collector, console or Console(file=sys.stdout)
        self.live_factory, self.sleep, self.interval = live_factory, sleep, interval

    async def run(self, stop):
        snapshot = None
        try:
            snapshot = await self.collector()
            with dashboard_logging(self.console), self.live_factory(
                    render_dashboard(snapshot, width=self.console.width), console=self.console,
                    auto_refresh=False, redirect_stdout=False, redirect_stderr=False, vertical_overflow="ellipsis") as live:
                try:
                    while not stop.is_set():
                        sleeping = asyncio.create_task(self.sleep(self.interval))
                        stopping = asyncio.create_task(stop.wait())
                        try:
                            done, _ = await asyncio.wait((sleeping, stopping), return_when=asyncio.FIRST_COMPLETED)
                            if stopping in done:
                                break
                            await sleeping
                        finally:
                            for task in (sleeping, stopping):
                                if not task.done():
                                    task.cancel()
                            await asyncio.gather(sleeping, stopping, return_exceptions=True)
                        snapshot = await self.collector()
                        live.update(render_dashboard(snapshot, width=self.console.width), refresh=True)
                finally:
                    snapshot = await self.collector()
                    live.update(render_dashboard(replace(snapshot, stopped=True), width=self.console.width), refresh=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Optional presentation must never bring down the monitoring pipeline.
            LOG.warning("dashboard unavailable; continuing plain logs (%s)", type(exc).__name__)
