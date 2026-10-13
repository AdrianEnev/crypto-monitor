"""Small price-touch rules shared by live monitoring and historical discovery.

A level is reached when price equals or passes it. A repeat delay measures
elapsed UTC time from an accepted activation; ignored touches never move it.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Mapping

from crypto_monitor.core.level_sweep import DAY, Period, reference_periods


REPEAT_NAMES = ("previous_week", "previous_month", "previous_quarter")
WATCH_NAMES = ("monday",) + REPEAT_NAMES


def utc_time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")
    return value


def price_value(value: Decimal) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError("prices must be Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError("prices must be finite and nonnegative")
    return value


def delay_duration(days: Decimal) -> timedelta:
    """Convert positive fractional days without using a floating-point clock."""
    if not isinstance(days, Decimal):
        raise TypeError("delay days must be Decimal")
    if not days.is_finite() or days <= 0:
        raise ValueError("delay days must be finite and positive")
    microseconds = int(days * Decimal(86_400_000_000))
    if microseconds < 1:
        raise ValueError("delay must be at least one microsecond")
    try:
        return timedelta(microseconds=microseconds)
    except OverflowError as exc:
        raise ValueError("delay is too large") from exc


def validate_delays(delays: Mapping[str, Decimal]) -> None:
    if set(delays) != set(REPEAT_NAMES):
        raise ValueError("provide previous_week, previous_month and previous_quarter delays")
    for value in delays.values():
        delay_duration(value)


def range_touches(side: str, level: Decimal, low: Decimal, high: Decimal) -> bool:
    for value in (level, low, high):
        price_value(value)
    if low > high:
        raise ValueError("low must not exceed high")
    if side == "high":
        return high >= level
    if side == "low":
        return low <= level
    raise ValueError("side must be high or low")


def watch_window(name: str, source: Period) -> Period:
    """Calendar during which a completed source range is watched."""
    if name == "monday":
        return Period(source.end, source.start + 7 * DAY)
    if name == "previous_week":
        return Period(source.end, source.end + 7 * DAY)
    if name == "previous_month":
        end = (source.end.replace(day=28) + 4 * DAY).replace(day=1)
        return Period(source.end, end)
    if name == "previous_quarter":
        month = source.end.month + 3
        end = source.end.replace(year=source.end.year + (month > 12),
                                 month=((month - 1) % 12) + 1)
        return Period(source.end, end)
    raise ValueError(f"unknown watch: {name}")


def next_eligible(name: str, touched_at: datetime, watch_end: datetime,
                  delays: Mapping[str, Decimal]) -> datetime:
    utc_time(touched_at)
    utc_time(watch_end)
    if name == "monday":
        return watch_end
    return min(touched_at + delay_duration(delays[name]), watch_end)


@dataclass(frozen=True)
class TouchWatch:
    symbol: str
    name: str
    side: Literal["high", "low"]
    source_start: datetime
    source_end: datetime
    watch_start: datetime
    watch_end: datetime
    level_price: Decimal | None
    history_cursor: datetime
    available: bool
    reason: str | None
    accepted_at: datetime | None
    accepted_count: int
    origin: str | None
    approximate: bool


@dataclass(frozen=True)
class TouchActivation:
    activation_id: str
    symbol: str
    level_name: str
    side: Literal["high", "low"]
    reference_period_start: datetime
    level_price: Decimal
    touched_at: datetime
    observed_at: datetime
    origin: Literal["live", "history"]
    captured_quote_volume: Decimal
    approximate: bool = False

    @property
    def watch(self):
        return self.level_name

    @property
    def source(self):
        return self.origin

    @property
    def reference_start(self):
        return self.reference_period_start


@dataclass(frozen=True)
class TouchOutboxEntry:
    activation: TouchActivation
    attempt_count: int
    next_attempt_at: datetime | None
    last_error: str | None
