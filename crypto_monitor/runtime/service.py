"""Command-line entry point for live touches, plus legacy candle test wiring.

main() runs touch_service.run_touch_service. run_service() is the preserved
closed-candle compatibility harness; it is not the executable monitor.
Importing this module performs no I/O.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from types import MappingProxyType

from crypto_monitor.adapters.binance_rest import BinanceRESTAdapter
from crypto_monitor.runtime.gap_recovery import MonitoringFailure, RecoveryWorker
from crypto_monitor.core.level_sweep import reference_periods
from crypto_monitor.core.monitoring_engine import MonitoringEngine, ReferenceLevels
from crypto_monitor.persistence.sqlite_store import SQLiteStore
from crypto_monitor.runtime.refresh_coordinator import RefreshCoordinator
from crypto_monitor.adapters.ses_email import EmailDispatcher, EmailSettings
from crypto_monitor.runtime.websocket_supervisor import WebSocketSupervisor
from crypto_monitor.runtime.environment import DEFAULT_DATABASE, DEFAULT_ENV_FILE, email_region, load_environment


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    database: Path = DEFAULT_DATABASE
    top_n: int = 100
    include_symbols: tuple[str, ...] = ()
    exclude_symbols: tuple[str, ...] = ()
    email: EmailSettings = EmailSettings()
    no_tui: bool = False
    stale_after_minutes: int = 3
    env_file: Path = DEFAULT_ENV_FILE
    layout: str = "table"

    def __post_init__(self):
        if str(self.database) == ":memory:":
            raise ValueError("database must be a persistent file, not :memory:")
        if type(self.top_n) is not int or self.top_n < 0:
            raise ValueError("top_n must be a nonnegative integer")
        if type(self.stale_after_minutes) is not int or self.stale_after_minutes <= 0:
            raise ValueError("stale_after_minutes must be a positive integer")


def parse_settings(argv=None) -> Settings:
    parser = argparse.ArgumentParser(prog="crypto-monitor", description="Binance USD-M price-touch monitor",
        epilog="Default: top 100 markets, table dashboard, email on. Press e to toggle email while running.")
    parser.add_argument("--database", default=str(DEFAULT_DATABASE),
                        help="SQLite file (default: crypto-monitor/monitor.sqlite3)")
    parser.add_argument("-n", "--top-n", type=int, default=100,
                        help="top eligible markets by quote volume (default: 100)")
    parser.add_argument("--include", default="", help="comma-separated additional eligible symbols")
    parser.add_argument("--exclude", default="", help="comma-separated excluded symbols (always win)")
    email = parser.add_mutually_exclusive_group()
    email.add_argument("--email-enabled", action="store_true", help="start with email alerts on (default)")
    email.add_argument("--no-email", "--email-disabled", action="store_false", dest="email_enabled",
                       help="start with email alerts off; press e to enable while running")
    parser.set_defaults(email_enabled=True)
    parser.add_argument("--email-sender", help="verified SES sender (default: EMAIL_FROM)")
    parser.add_argument("--email-recipient", help="single recipient (default: ALERT_EMAIL_TO)")
    parser.add_argument("--email-region", help="SES API region (default: AWS region; SMTP uses SES_SMTP_HOST)")
    parser.add_argument("--no-tui", action="store_true", help="plain logs even on an interactive terminal")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE),
                        help="required sweep settings file (default: crypto-monitor/.env)")
    parser.add_argument("-l", "--layout", choices=("table", "cards"), default="table",
                        help="terminal layout (default: table)")
    parser.add_argument("--stale-after-minutes", type=int, default=3, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    def symbols(value):
        return tuple(sorted({item.strip().upper() for item in value.split(",") if item.strip()}))
    try:
        load_environment(args.env_file)
        sender = args.email_sender if args.email_sender is not None else os.environ.get("EMAIL_FROM")
        recipient = args.email_recipient if args.email_recipient is not None else (
            os.environ.get("ALERT_EMAIL_TO") or os.environ.get("EMAIL_TO") or os.environ.get("EMAIL_RECIPIENT"))
        region = args.email_region if args.email_region is not None else email_region()
        if args.email_enabled and os.environ.get("SES_SMTP_HOST"):
            from crypto_monitor.adapters.ses_smtp import SESEmailSMTPSettings
            SESEmailSMTPSettings.from_env()
        if not args.database.strip():
            raise ValueError("database path cannot be empty")
        settings = Settings(Path(args.database).expanduser(), args.top_n,
                            symbols(args.include), symbols(args.exclude),
                            EmailSettings(args.email_enabled, sender, recipient, region),
                            args.no_tui, args.stale_after_minutes)
        return Settings(settings.database.resolve(), settings.top_n,
                        settings.include_symbols, settings.exclude_symbols, settings.email, settings.no_tui,
                        settings.stale_after_minutes, Path(args.env_file).expanduser().resolve(), args.layout)
    except ValueError as exc:
        parser.error(str(exc))


def should_use_dashboard(settings, stdout=None):
    stream = sys.stdout if stdout is None else stdout
    try:
        return not settings.no_tui and bool(stream.isatty()) and os.environ.get("TERM") != "dumb"
    except (AttributeError, OSError):
        return False


def make_dashboard(settings, worker, supervisor, coordinator, email, *, now):
    # Plain-log mode doesn't import Rich or initialize any terminal objects.
    from crypto_monitor.ui.terminal_dashboard import DashboardCollector, TerminalDashboard
    return TerminalDashboard(DashboardCollector(settings.database, worker, supervisor, coordinator,
        email_enabled=settings.email.enabled, email=email, now=now,
        stale_after_minutes=settings.stale_after_minutes))


class StartupReferences:
    """Read-only startup snapshots; never silently relabel or reuse stale levels."""

    def __init__(self, as_of, snapshots):
        self.as_of = as_of
        self.periods = reference_periods(as_of)
        self.snapshots = MappingProxyType({symbol: MappingProxyType(dict(refs))
                                          for symbol, refs in snapshots.items()})

    def __call__(self, symbol, opened):
        if reference_periods(opened) != self.periods:
            raise MonitoringFailure(
                f"startup reference snapshot does not cover {symbol} at {opened.isoformat()}; "
                "this startup-only provider cannot supply historical references"
            )
        return self.snapshots[symbol]


def _prepare(settings, cancelled, rest_factory, store_factory, engine_factory, now, interactive=False):
    """Blocking startup runs off the event loop; no connection crosses threads."""
    if cancelled.is_set():
        return None
    rest = rest_factory()
    LOG.info("startup discovering markets top_n=%s include=%s exclude=%s", settings.top_n,
             settings.include_symbols, settings.exclude_symbols)
    markets = tuple(rest.discover_markets(top_n=settings.top_n,
                    include_symbols=settings.include_symbols, exclude_symbols=settings.exclude_symbols))
    if cancelled.is_set():
        return None
    if not markets:
        raise MonitoringFailure("market discovery returned no selected markets")
    as_of = now()
    periods = reference_periods(as_of)
    if interactive:
        leaders = sorted(markets, key=lambda market: (-market.quote_volume, market.symbol))[:3]
        LOG.info("startup selected=%s top=%s remainder=%s reference_as_of=%s", len(markets),
                 ",".join(m.symbol for m in leaders), max(0, len(markets) - 3), as_of.isoformat())
    else:
        LOG.info("startup selected=%s reference_as_of=%s", ",".join(m.symbol for m in markets), as_of.isoformat())
    detail = LOG.debug if interactive else LOG.info
    unavailable_count = 0
    snapshots = {}
    for market in markets:
        if cancelled.is_set():
            return None
        levels = rest.fetch_reference_levels(market.symbol, as_of=as_of)
        if set(levels) != set(periods):
            raise MonitoringFailure(f"incomplete reference snapshot for {market.symbol}")
        snapshots[market.symbol] = {
            name: ReferenceLevels(period, levels[name]) for name, period in periods.items() if period is not None
        }
        unavailable = [name for name, ref in snapshots[market.symbol].items() if ref.levels is None]
        unavailable_count += len(unavailable)
        detail("startup levels symbol=%s unavailable=%s quote_volume=%s tick_size=%s", market.symbol,
                 unavailable, market.quote_volume, market.tick_size)
    if interactive and unavailable_count:
        LOG.warning("startup has %s unavailable reference levels; incomplete history", unavailable_count)
    if cancelled.is_set():
        return None
    references = StartupReferences(as_of, snapshots)
    LOG.info("startup opening database=%s", settings.database)
    with store_factory(settings.database) as store:
        engine = engine_factory(markets)
        baselines = store.load_baselines()
        for market in markets:
            baseline = baselines.get(market.symbol)
            if baseline is not None:
                engine.restore_baseline(market.symbol, baseline)
                detail("startup restored baseline symbol=%s open_time=%s", market.symbol, baseline.open_time.isoformat())
            else:
                detail("startup no baseline symbol=%s; first closed live candle will seed it", market.symbol)
    # The worker independently restores from this file in its own thread, as
    # it already does on every event/retry. Do not share a SQLite connection.
    LOG.info("startup database validation complete; startup connection closed")
    return markets, rest, references


async def run_service(
    settings: Settings, *, stop: asyncio.Event | None = None,
    rest_factory=BinanceRESTAdapter, store_factory=SQLiteStore, engine_factory=MonitoringEngine,
    worker_factory=RecoveryWorker, supervisor_factory=WebSocketSupervisor,
    coordinator_factory=RefreshCoordinator,
    email_factory=EmailDispatcher,
    dashboard_factory=make_dashboard, stdout=None,
    now=lambda: datetime.now(timezone.utc),
) -> None:
    """Inject factories for offline tests; the supervisor owns worker lifecycle."""
    stop = stop if stop is not None else asyncio.Event()
    if stop.is_set():
        return
    cancelled = Event()
    interactive = should_use_dashboard(settings, stdout)
    preparing = asyncio.create_task(asyncio.to_thread(
        _prepare, settings, cancelled, rest_factory, store_factory, engine_factory, now, interactive))
    async def stop_startup():
        await stop.wait()
        cancelled.set()  # Signal the startup thread before waking the main waiter.
    stopping = asyncio.create_task(stop_startup())
    running = []
    display_task, display_stop = None, asyncio.Event()
    try:
        done, _ = await asyncio.wait((preparing, stopping), return_when=asyncio.FIRST_COMPLETED)
        if stopping in done:
            cancelled.set()
        # wait() never cancels a thread future; let it close its own resources.
        await asyncio.wait((preparing,))
        prepared = preparing.result()
        if stop.is_set() or prepared is None:
            return
        markets, rest, references = prepared
        worker = worker_factory(markets, settings.database, rest, references)
        supervisor = supervisor_factory(worker)
        coordinator = coordinator_factory(worker, supervisor, rest, settings, references, now=now)
        await coordinator.initialize()
        LOG.info("startup complete; starting supervisor, recovery worker, and UTC refresh coordinator")
        running = [asyncio.create_task(supervisor.run(stop)), asyncio.create_task(coordinator.run(stop))]
        email = None
        if settings.email.enabled:
            email = email_factory(settings.database, settings.email, now=now)
            running.append(asyncio.create_task(email.run(stop)))
            LOG.info("email enabled; pending SQLite alerts scheduled on five-minute UTC boundaries")
        else:
            LOG.info("email disabled; detected hits remain queued in SQLite")
        if interactive:
            try:
                dashboard = dashboard_factory(settings, worker, supervisor, coordinator, email, now=now)
                display_task = asyncio.create_task(dashboard.run(display_stop))
            except Exception as exc:
                LOG.warning("dashboard unavailable; continuing plain logs (%s)", type(exc).__name__)
        done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            await task
        if not stop.is_set():
            raise MonitoringFailure("service component stopped unexpectedly")
    except asyncio.CancelledError:
        LOG.info("shutdown requested by task cancellation")
        raise
    except Exception:
        LOG.exception("fatal service error; monitoring stopped")
        raise
    finally:
        cancelled.set()
        stopping.cancel()
        for task in running:
            if not task.done():
                task.cancel()
        await asyncio.gather(preparing, stopping, *running, return_exceptions=True)
        # Keep showing cleanup progress until the worker and any SES send finish.
        display_stop.set()
        if display_task is not None:
            await asyncio.gather(display_task, return_exceptions=True)
        LOG.info("service stopped; owned database connections closed")


@contextmanager
def shutdown_signals(stop):
    """Install only while running on the main thread; restore prior handlers."""
    loop = asyncio.get_running_loop()
    previous = {}
    def request(signum):
        if not stop.is_set():
            LOG.info("shutdown requested signal=%s", signal.Signals(signum).name)
            stop.set()
    def handler(signum, frame):
        loop.call_soon_threadsafe(request, signum)
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, handler)
        yield
    finally:
        for signum, prior in previous.items():
            signal.signal(signum, prior)


def configure_logging(*, sofia=False):
    """Called only by the executable entry point, never at import time."""
    handler = logging.StreamHandler(sys.stdout)
    if sofia:
        from zoneinfo import ZoneInfo
        class LocalFormatter(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                return datetime.fromtimestamp(record.created, ZoneInfo("Europe/Sofia")).isoformat(timespec="seconds")
        formatter = LocalFormatter("%(asctime)s %(levelname)s %(message)s")
    else:
        formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
        formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


def main(argv=None, *, service_runner=None) -> int:
    settings = parse_settings(argv)
    production = service_runner is None
    if service_runner is None:
        from crypto_monitor.runtime.touch_settings import load_touch_settings
        from crypto_monitor.runtime.touch_service import run_touch_service
        try:
            policy = load_touch_settings(settings.env_file)
        except ValueError as exc:
            print(f"Configuration error: {exc}", file=sys.stderr)
            return 2
        async def service_runner(settings, **kwargs):
            await run_touch_service(settings, policy=policy, **kwargs)
    configure_logging(sofia=production)
    async def run():
        stop = asyncio.Event()
        with shutdown_signals(stop):
            await service_runner(settings, stop=stop)
    try:
        asyncio.run(run())
        return 0
    except KeyboardInterrupt:
        LOG.info("shutdown interrupted")
        return 130
    except Exception as exc:
        # Also covers signal-installation errors or injected runners.
        LOG.error("fatal service exit: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
