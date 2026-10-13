"""UTC reference levels and a provisional one-minute breach/reclaim rule.

Candle timestamps are opening times. Callers supply the closed flag; this
module does not consult the wall clock or infer whether a candle has closed.
Prices must be finite Decimal values, constructed from strings or integers.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable, Literal


DAY = timedelta(days=1)
MINUTE = timedelta(minutes=1)


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")


def _require_prices(*values: Decimal) -> None:
    for value in values:
        if not isinstance(value, Decimal):
            raise TypeError("prices must be Decimal values")
        if not value.is_finite():
            raise ValueError("prices must be finite")


def _midnight(value: datetime) -> datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class Period:
    """A UTC calendar period with an inclusive start and exclusive end."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        for boundary in (self.start, self.end):
            _require_utc(boundary)
            if boundary != _midnight(boundary):
                raise ValueError("calendar boundaries must be UTC midnight")
        if self.start >= self.end:
            raise ValueError("period start must precede end")


@dataclass(frozen=True)
class Candle:
    open_time: datetime
    interval: timedelta
    high: Decimal
    low: Decimal
    close: Decimal
    closed: bool

    def __post_init__(self) -> None:
        _require_utc(self.open_time)
        _require_prices(self.high, self.low, self.close)
        if self.interval <= timedelta(0):
            raise ValueError("candle interval must be positive")
        if not self.low <= self.close <= self.high:
            raise ValueError("candle must satisfy low <= close <= high")


@dataclass(frozen=True)
class Levels:
    high: Decimal
    low: Decimal


def reference_periods(as_of: datetime) -> dict[str, Period | None]:
    """Return reference periods; Monday is unavailable throughout Monday.

    The previous Monday range expires at the start of the next Monday.
    The new Monday range becomes available at Tuesday 00:00 UTC.
    """
    _require_utc(as_of)
    today = _midnight(as_of)
    monday = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    previous_month_start = (month_start - DAY).replace(day=1)
    quarter_month = ((today.month - 1) // 3) * 3 + 1
    quarter_start = month_start.replace(month=quarter_month)
    if quarter_month == 1:
        previous_quarter_start = quarter_start.replace(
            year=quarter_start.year - 1, month=10
        )
    else:
        previous_quarter_start = quarter_start.replace(month=quarter_month - 3)

    return {
        "monday": Period(monday, monday + DAY) if today.weekday() != 0 else None,
        "previous_week": Period(monday - 7 * DAY, monday),
        "previous_month": Period(previous_month_start, month_start),
        "previous_quarter": Period(previous_quarter_start, quarter_start),
    }


def reference_regime(as_of: datetime) -> Period:
    """Maximal UTC range with identical reference_periods (start in, end out).

    Quarter changes coincide with month changes. A Monday-only regime ends at
    Tuesday activation; the Tuesday regime ends at next Monday's expiry.
    """
    _require_utc(as_of)
    today = _midnight(as_of)
    monday = today - timedelta(days=today.weekday())
    month = today.replace(day=1)
    next_month = (month.replace(day=28) + 4 * DAY).replace(day=1)
    start, end = (monday, monday + DAY) if today.weekday() == 0 else (monday + DAY, monday + 7 * DAY)
    return Period(max(start, month), min(end, next_month))


def calculate_levels(period: Period, candles: Iterable[Candle]) -> Levels | None:
    """Return extrema, or None if complete daily history cannot be established.

    Each UTC day must have exactly one closed, midnight-aligned daily candle.
    Input order does not matter. Candles outside [start, end) are ignored.
    Duplicate or malformed in-period coverage is unavailable, not partial.
    """
    by_day: dict[datetime, Candle] = {}
    for candle in candles:
        if not period.start <= candle.open_time < period.end:
            continue
        if (
            not candle.closed
            or candle.interval != DAY
            or candle.open_time != _midnight(candle.open_time)
            or candle.open_time in by_day
        ):
            return None
        by_day[candle.open_time] = candle

    expected_days = (period.end - period.start).days
    if len(by_day) != expected_days:
        return None
    return Levels(
        high=max(candle.high for candle in by_day.values()),
        low=min(candle.low for candle in by_day.values()),
    )


def detect_sweep(
    side: Literal["high", "low"],
    level: Decimal,
    previous_close: Decimal,
    tick_size: Decimal,
    candle: Candle,
) -> bool:
    """Apply only the provisional closed-one-minute breach/reclaim rule.

    previous_close is supplied by the caller for the immediately preceding
    candle. No ordering, deduplication, or alert-frequency state is maintained.
    """
    if side not in ("high", "low"):
        raise ValueError("side must be 'high' or 'low'")
    _require_prices(level, previous_close, tick_size)
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    if not candle.closed or candle.interval != MINUTE:
        return False
    if side == "high":
        return (
            previous_close <= level
            and candle.high >= level + tick_size
            and candle.close < level
        )
    return (
        previous_close >= level
        and candle.low <= level - tick_size
        and candle.close > level
    )
