"""Small price-contact rules shared by live monitoring and historical discovery.

A contact happens when consecutive ordered prices arrive at or pass through a
level, from either direction. Remaining beyond a level is not a contact, and a
run of prices exactly at the level is one contact. A repeat delay measures
elapsed UTC time from an accepted activation; it only suppresses contacts and
never creates one when it expires.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Mapping

from crypto_monitor.core.level_sweep import DAY, Period, reference_periods


REPEAT_NAMES = ("previous_week", "previous_month", "previous_quarter")
WATCH_NAMES = ("monday",) + REPEAT_NAMES
# Version 1 accepted any price at or beyond a level. Version 2 requires contact.
RULES_VERSION = 2


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


def crosses(previous: Decimal | None, current: Decimal, level: Decimal) -> bool:
    """Exact contact between two consecutive trades, from either direction.

    An unknown previous price establishes state without a contact. Leaving an
    equality run does not count; returning to the level later does.
    """
    price_value(current)
    price_value(level)
    if previous is None:
        return False
    price_value(previous)
    return previous != level and min(previous, current) <= level <= max(previous, current)


def contact_possible(previous: Decimal | None, low: Decimal, high: Decimal,
                     level: Decimal) -> bool:
    """Whether a range that followed ``previous`` may contain a contact.

    False is proof of no contact, so callers may skip finer history. True
    means finer prices are needed, or the range alone is evidence of contact.
    """
    for value in (level, low, high):
        price_value(value)
    if low > high:
        raise ValueError("low must not exceed high")
    if previous is None:
        return low <= level <= high and not low == high == level
    price_value(previous)
    if previous == level:
        return low < level or high > level
    return min(previous, low) <= level <= max(previous, high)


def candle_contacts(previous: Decimal | None, opening: Decimal | None, low: Decimal,
                    high: Decimal, close: Decimal, level: Decimal) -> bool:
    """Conservative contact evidence from one candle following ``previous``.

    OHLC cannot show the order or number of trades inside the candle. A gap
    from the previous close counts as a crossing. During an equality run only
    proven departures and returns count: prices on both sides of the level,
    or a close back at the level after leaving it. Unknown history uses the
    candle's own open, or else requires the range to reach the level.
    """
    price_value(close)
    if previous is None:
        previous = opening
    if previous is None:
        return contact_possible(None, low, high, level)
    if previous != level:
        return contact_possible(previous, low, high, level)
    for value in (level, low, high):
        price_value(value)
    return low < level < high or (close == level and (low < level or high > level))


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
    # Last known price at history_cursor, if the replay could establish one.
    cursor_price: Decimal | None = None


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
    # Diagnostic evidence; historical candles have no single trigger trade.
    previous_price: Decimal | None = None
    trigger_price: Decimal | None = None

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
