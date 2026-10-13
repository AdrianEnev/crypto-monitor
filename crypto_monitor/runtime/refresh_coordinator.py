"""UTC refresh scheduling and immutable, atomic runtime configuration snapshots."""

import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from crypto_monitor.adapters.binance_rest import parse_quote_volumes
from crypto_monitor.adapters.binance_feed import BinanceKlineFeed
from crypto_monitor.core.level_sweep import reference_periods
from crypto_monitor.core.monitoring_engine import MonitoringEngine, ReferenceLevels
from crypto_monitor.persistence.reference_snapshots import DurableReferences


LOG = logging.getLogger(__name__)
FIVE_MINUTES = timedelta(minutes=5)
DAY = timedelta(days=1)


def next_boundaries(now):
    reference_periods(now)  # Reuse the strict aware-UTC validation.
    minute = now.replace(second=0, microsecond=0)
    volume = minute.replace(minute=minute.minute // 5 * 5) + FIVE_MINUTES
    daily = now.replace(hour=0, minute=0, second=0, microsecond=0) + DAY
    return volume, daily


@dataclass(frozen=True)
class RefreshStatus:
    volume_updated_at: datetime
    membership_updated_at: datetime
    levels_updated_at: datetime
    volume_age_seconds: float = 0
    volume_error: str | None = None
    membership_error: str | None = None
    levels_error: str | None = None
    unresolved: str | None = None

    @property
    def degraded(self):
        return any((self.volume_error, self.membership_error, self.levels_error, self.unresolved))


class RefreshCoordinator:
    """Stage HTTP work off-loop, publish between worker operations, then rotate.

    UTC ticks: :00/:05/... for volumes; 00:00 daily for membership and levels.
    A delayed wakeup performs one current refresh, not a burst of missed ticks.
    Failed levels retry on volume ticks for the retained active set. Membership
    failures (including a rejected daily bundle) wait until the next daily tick.
    refresh() is also available for explicit, clock-controlled recovery/tests.
    """

    def __init__(self, worker, supervisor, rest, settings, initial_references, *,
                 now=lambda: datetime.now(timezone.utc), sleep=asyncio.sleep,
                 fetch_tickers=None, fetch_exchange=None, fetch_levels=None, select_members=None):
        self.worker, self.supervisor, self.settings = worker, supervisor, settings
        self.now, self.sleep = now, sleep
        self.fetch_tickers = fetch_tickers or rest.fetch_tickers
        self.fetch_exchange = fetch_exchange or rest.fetch_exchange_info
        self.fetch_levels = fetch_levels or rest.fetch_reference_levels
        self.select_members = select_members or rest.discover_markets
        self.markets = tuple(worker.markets)
        self.references = DurableReferences(settings.database,
            lambda symbol, **kwargs: self.fetch_levels(symbol, **kwargs))
        self._initial_references = initial_references
        as_of = initial_references.as_of
        self._status = RefreshStatus(as_of, as_of, as_of)
        self.next_volume, self.next_daily = next_boundaries(as_of)
        self._tickers = None
        self._tickers_at = as_of
        self._lock = asyncio.Lock()

    @property
    def status(self):
        now = self.now()
        reference_periods(now)
        return replace(self._status, volume_age_seconds=max(0, (now - self._status.volume_updated_at).total_seconds()),
                       unresolved=self.worker.unresolved)

    async def initialize(self):
        saving = asyncio.create_task(asyncio.to_thread(self.references.persist,
            self._initial_references.as_of, self._initial_references.snapshots))
        try:
            await asyncio.wait((saving,))
            saving.result()
        finally:
            await asyncio.gather(saving, return_exceptions=True)
        await self.worker.apply_configuration(self.markets, self.references)

    def _stage(self, as_of, daily):
        status = self._status
        tickers, tickers_at = self._tickers, self._tickers_at
        volume_markets = self.markets
        try:
            fresh = self.fetch_tickers()
            volumes = parse_quote_volumes(fresh)
            # Even daily selection uses the latest well-shaped ticker response.
            tickers, tickers_at = fresh, as_of
            volume_markets = tuple(replace(m, quote_volume=volumes[m.symbol]) for m in self.markets)
            status = replace(status, volume_updated_at=as_of, volume_error=None)
        except Exception as exc:
            status = replace(status, volume_error=str(exc))

        candidate, membership_ok = volume_markets, False
        if daily:
            try:
                exchange = self.fetch_exchange()
                if tickers is None:
                    raise ValueError("no successful ticker snapshot for membership selection")
                candidate = tuple(self.select_members(
                    exchange_info=exchange, tickers=tickers, top_n=self.settings.top_n,
                    include_symbols=self.settings.include_symbols, exclude_symbols=self.settings.exclude_symbols))
                if not candidate:
                    raise ValueError("membership refresh returned no eligible markets")
                MonitoringEngine(candidate)
                BinanceKlineFeed([m.symbol for m in candidate], self.worker.submit)
                membership_ok = True
            except Exception as exc:
                candidate = volume_markets
                status = replace(status, membership_error=str(exc))

        references = self.references
        if daily or status.levels_error is not None:
            try:
                periods, snapshots = reference_periods(as_of), {}
                for market in candidate:
                    levels = self.fetch_levels(market.symbol, as_of=as_of)
                    if set(levels) != set(periods):
                        raise ValueError(f"incomplete level response for {market.symbol}")
                    refs = {}
                    for name, period in periods.items():
                        if period is not None:
                            if levels[name] is None:
                                raise ValueError(f"unavailable refreshed level: {market.symbol} {name}")
                            refs[name] = ReferenceLevels(period, levels[name])
                    snapshots[market.symbol] = refs
                # Commit the whole bundle before publishing it to recovery.
                references.persist(as_of, snapshots)
                status = replace(status, levels_updated_at=as_of, levels_error=None)
                if membership_ok:
                    status = replace(status, membership_updated_at=as_of, membership_error=None,
                                     volume_updated_at=tickers_at,
                                     volume_error=None if tickers_at == as_of else status.volume_error)
            except Exception as exc:
                candidate = volume_markets  # Never publish membership without all its levels.
                status = replace(status, levels_error=str(exc))
                if membership_ok:
                    status = replace(status, membership_error="daily membership not applied: level refresh failed")
        return candidate, references, status, tickers, tickers_at

    async def refresh(self, as_of=None, *, force_daily=False):
        async with self._lock:
            as_of = self.now() if as_of is None else as_of
            reference_periods(as_of)
            daily = force_daily or as_of >= self.next_daily
            if not daily and as_of < self.next_volume:
                return False
            staged = asyncio.create_task(asyncio.to_thread(self._stage, as_of, daily))
            try:
                await asyncio.wait((staged,))
                markets, references, status, tickers, tickers_at = staged.result()
            finally:
                # Cancellation never publishes unfinished results or leaves a fetch thread running.
                await asyncio.gather(staged, return_exceptions=True)
            await self.worker.apply_configuration(markets, references)
            # No intervening await: publish metadata and signal rotation as one event-loop operation.
            self.markets, self.references, self._status = markets, references, status
            self._tickers, self._tickers_at = tickers, tickers_at
            self.supervisor.update_symbols([m.symbol for m in markets])
            self.next_volume = next_boundaries(as_of)[0]
            if daily:
                self.next_daily = next_boundaries(as_of)[1]
            LOG.info("refresh applied daily=%s degraded=%s volume_age_seconds=%s errors=%s",
                     daily, self.status.degraded, self.status.volume_age_seconds,
                     (status.volume_error, status.membership_error, status.levels_error))
            return True

    async def run(self, stop):
        while not stop.is_set():
            await self.refresh()
            delay = max(0, (min(self.next_volume, self.next_daily) - self.now()).total_seconds())
            sleeping = asyncio.create_task(self.sleep(delay))
            stopping = asyncio.create_task(stop.wait())
            try:
                done, _ = await asyncio.wait((sleeping, stopping), return_when=asyncio.FIRST_COMPLETED)
                if sleeping in done:
                    await sleeping
            finally:
                for task in (sleeping, stopping):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(sleeping, stopping, return_exceptions=True)
