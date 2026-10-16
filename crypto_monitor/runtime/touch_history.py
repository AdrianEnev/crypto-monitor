"""Quiet current-period discovery with independent coverage for each watch.

Complete daily ranges prove when no contact was possible, avoiding unnecessary
minute downloads. Other intervals use validated closed one-minute history.
The resulting time is the containing minute's end, rather than an invented
historical second. Failures make the affected side unavailable, never untouched.

Contacts need the price before each interval. Each side keeps the price at its
cursor; a skip records the daily close when it is known and otherwise marks
the price unknown, so a later minute is never compared with a stale price.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import islice
import sqlite3

from crypto_monitor.core.level_sweep import DAY, MINUTE
from crypto_monitor.core.touch_monitor import contact_possible, next_eligible, utc_time, validate_delays


def _key(watch):
    return watch.name, watch.side, watch.source_start


def _price_at(daily, day, through):
    """A completed day's close is the exact price at its end, else unknown."""
    return daily.close if daily is not None and through == day + DAY else None


@dataclass(frozen=True)
class HistoryScanResult:
    symbol: str
    verified: tuple[tuple[str, str], ...]
    unavailable: tuple[tuple[str, str, str], ...]
    activation_count: int


class HistoryScanCancelled(InterruptedError):
    """Shutdown requested; retain committed cursors for a later restart."""


class TouchHistoryScanner:
    def __init__(self, store, rest, delays, *, progress=None, cancelled=None):
        validate_delays(delays)
        self.store, self.rest, self.delays, self.progress = store, rest, delays, progress
        self.cancelled = cancelled

    def _check_cancelled(self):
        if self.cancelled is not None and self.cancelled():
            raise HistoryScanCancelled("history scan stopped for shutdown")

    def _needs_minutes(self, watch, daily, end):
        if watch.level_price is None or watch.history_cursor >= end:
            return False
        if watch.accepted_at is not None:
            if watch.name == "monday":
                return False
            # Only a minute starting at or after the deadline can be accepted.
            eligible = next_eligible(watch.name, watch.accepted_at, watch.watch_end, self.delays)
            if eligible > end - MINUTE:
                return False
        return daily is None or contact_possible(watch.cursor_price, daily.low, daily.high,
                                                 watch.level_price)

    def scan(self, symbol: str, as_of: datetime) -> HistoryScanResult:
        self._check_cancelled()
        utc_time(as_of)
        horizon = as_of.replace(second=0, microsecond=0)
        jobs = [watch for watch in self.store.history_jobs(symbol, as_of)
                if watch.level_price is not None and watch.history_cursor < horizon]
        failures = {}
        activation_count = 0
        if jobs:
            cursor = min(watch.history_cursor for watch in jobs)
            start_day = cursor.replace(hour=0, minute=0, second=0, microsecond=0)
            today = horizon.replace(hour=0, minute=0, second=0, microsecond=0)
            daily_ranges = {}
            if start_day < today:
                try:
                    rows = self.rest.fetch_daily_klines(symbol, start=start_day, end=today, as_of=as_of)
                    duplicate_days = set()
                    for candle in rows:
                        if (not candle.closed or candle.interval != DAY
                                or candle.open_time != candle.open_time.replace(hour=0, minute=0,
                                                                             second=0, microsecond=0)):
                            continue
                        if candle.open_time in daily_ranges:
                            duplicate_days.add(candle.open_time)
                        daily_ranges[candle.open_time] = candle
                    for duplicate in duplicate_days:
                        daily_ranges.pop(duplicate, None)
                except Exception:
                    # This is an optimization. Valid minute data can still prove
                    # coverage when the daily endpoint is temporarily unavailable.
                    daily_ranges = {}
                self._check_cancelled()
            total = max(1, (horizon - start_day + DAY - timedelta(microseconds=1)) // DAY)
            day = start_day
            completed = 0
            while day < horizon:
                self._check_cancelled()
                end = min(day + DAY, horizon)
                daily = daily_ranges.get(day)
                watches = [watch for watch in self.store.history_jobs(symbol)
                           if watch.level_price is not None and _key(watch) not in failures
                           and watch.history_cursor < end and watch.watch_start < end]
                candidates = []
                for watch in watches:
                    if self._needs_minutes(watch, daily, end):
                        # No accepted activation can occur before its fixed
                        # deadline. Replay from one minute earlier, so the
                        # first eligible minute has a known preceding price.
                        if watch.accepted_at is not None:
                            eligible = next_eligible(watch.name, watch.accepted_at,
                                                     watch.watch_end, self.delays)
                            prefix = eligible.replace(second=0, microsecond=0) - MINUTE
                            if prefix > watch.history_cursor:
                                self.store.advance_history(watch, min(prefix, end))
                        candidates.append(watch)
                    else:
                        self.store.advance_history(watch, end, _price_at(daily, day, end))
                if candidates:
                    keys = {_key(watch) for watch in candidates}
                    # Split at different side cursors. A gap in an older watch
                    # must not prevent a later-starting watch from validating.
                    boundaries = sorted({watch.history_cursor for watch in self.store.history_jobs(symbol)
                                         if _key(watch) in keys} | {end})
                    for interval_start, interval_end in zip(boundaries, boundaries[1:]):
                        current = [watch for watch in self.store.history_jobs(symbol)
                                   if _key(watch) in keys and _key(watch) not in failures
                                   and watch.history_cursor <= interval_start]
                        interval_keys = {_key(watch) for watch in current}
                        if not interval_keys or interval_start >= interval_end:
                            continue
                        iterator = None
                        try:
                            self._check_cancelled()
                            iterator = iter(self.rest.iter_minute_klines(symbol, start=interval_start,
                                end=interval_end, as_of=as_of))
                            expected = interval_start
                            while expected < interval_end:
                                self._check_cancelled()
                                batch = tuple(islice(iterator, 499))
                                self._check_cancelled()
                                if not batch:
                                    raise ValueError(f"missing minute history at {expected.isoformat()}")
                                for event in batch:
                                    if event.candle.open_time != expected:
                                        raise ValueError(f"missing minute history at {expected.isoformat()}")
                                    expected += MINUTE
                                activation_count += len(self.store.replay_history_candles(symbol, batch,
                                    self.delays, observed_at=as_of, watch_keys=interval_keys))
                                # The remainder of this interval is safely
                                # skippable once every active side is cooling down.
                                remaining = [watch for watch in self.store.history_jobs(symbol)
                                             if _key(watch) in interval_keys]
                                if expected < interval_end and all(
                                    not self._needs_minutes(watch, daily, interval_end) for watch in remaining
                                ):
                                    for watch in remaining:
                                        self.store.advance_history(watch, interval_end,
                                                                   _price_at(daily, day, interval_end))
                                    expected = interval_end
                        except HistoryScanCancelled:
                            raise
                        except sqlite3.Error:
                            # A failed durable write is a monitor failure, not
                            # evidence that exchange history is unavailable.
                            raise
                        except Exception as exc:
                            for watch in self.store.history_jobs(symbol):
                                if _key(watch) not in interval_keys or watch.history_cursor >= interval_end:
                                    continue
                                if not self._needs_minutes(watch, daily, interval_end):
                                    self.store.advance_history(watch, interval_end,
                                                               _price_at(daily, day, interval_end))
                                    continue
                                reason = f"minute history unavailable: {exc}"
                                failures[_key(watch)] = reason
                                self.store.mark_history_unavailable(symbol, watch.name, watch.source_start,
                                                                   reason, side=watch.side)
                        finally:
                            if iterator is not None and hasattr(iterator, "close"):
                                iterator.close()
                day += DAY
                completed += 1
                if self.progress:
                    self.progress({"stage": "checking sweep history", "symbol": symbol,
                                   "completed": completed, "total": total})
        for watch in self.store.history_jobs(symbol):
            if _key(watch) not in failures:
                self.store.mark_history_complete(symbol, watch.name, watch.source_start, horizon)
        final = self.store.history_jobs(symbol)
        return HistoryScanResult(symbol,
            tuple((watch.name, watch.side) for watch in final if watch.available),
            tuple((watch.name, watch.side, watch.reason or "unavailable") for watch in final if not watch.available),
            activation_count)
