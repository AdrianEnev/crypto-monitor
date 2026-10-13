"""In-memory sweep evaluation; callers supply candles and reference snapshots.

No I/O, alert retention, frequency suppression or persistence. Call serially.
Reference names match reference_periods()/fetch_reference_levels(). A caller
pairs those Levels with the Period used to calculate them, per symbol.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable, Literal, Mapping, cast

from crypto_monitor.adapters.binance_feed import ClosedKline
from crypto_monitor.adapters.binance_rest import DiscoveredMarket
from crypto_monitor.core.level_sweep import (
    MINUTE, Candle, Levels, Period, detect_sweep, reference_periods,
)
from crypto_monitor.core.market_universe import Alert, rank_alerts


def _decimal(value: Decimal, label: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{label} must be Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{label} must be finite and nonnegative")


@dataclass(frozen=True)
class CandleBaseline:
    """Only the processed opening time and close are needed after restart."""

    open_time: datetime
    close: Decimal

    def __post_init__(self) -> None:
        if self.open_time.utcoffset() != timedelta(0):
            raise ValueError("baseline opening time must be timezone-aware UTC")
        if self.open_time.second or self.open_time.microsecond:
            raise ValueError("baseline opening time must be minute-aligned")
        _decimal(self.close, "baseline close")


@dataclass(frozen=True)
class ReferenceLevels:
    """A calculated high/low pair and its source period; None means unavailable.

    Omit inactive Monday from the supplied mapping. Do not relabel stale Levels
    with a new Period: the engine can check dates, not the provenance of prices.
    """

    period: Period
    levels: Levels | None

    def __post_init__(self) -> None:
        if self.levels is not None:
            _decimal(self.levels.high, "reference high")
            _decimal(self.levels.low, "reference low")
            if self.levels.low > self.levels.high:
                raise ValueError("reference low must be at or below high")


@dataclass(frozen=True)
class SweepAlert(Alert):
    """An Alert compatible with rank_alerts(), with a stable hit identity.

    alert_id is a compact JSON array of symbol, level_name, side,
    reference_period_start and candle_time (UTC ISO timestamps). It excludes
    volume and delivery/event time so replay produces the same identity.
    """

    level_name: str
    side: Literal["high", "low"]
    reference_period_start: datetime
    candle_time: datetime
    level_price: Decimal


class MonitoringEngine:
    """Evaluate feed events against supplied levels for the event's symbol.

    process(event, references) accepts the ClosedKline passed to the feed's
    callback. An async callback can call this synchronous method and collect
    the returned hits. process_batch additionally ranks hits across symbols.

    Every valid candle becomes that symbol's latest received baseline. Gaps,
    duplicates and backward timestamps produce no hits on that event. After
    any reset, only a candle exactly one minute after the new baseline can be
    evaluated. Replays may re-emit an ID; deduplication belongs to a later slice.
    """

    def __init__(self, markets: Iterable[DiscoveredMarket]):
        self._markets: dict[str, DiscoveredMarket] = {}
        for market in markets:
            if market.symbol in self._markets:
                raise ValueError(f"duplicate market: {market.symbol}")
            _decimal(market.tick_size, "tick size")
            _decimal(market.quote_volume, "quote volume")
            if market.tick_size == 0:
                raise ValueError("tick size must be positive")
            self._markets[market.symbol] = market
        self._previous: dict[str, Candle | CandleBaseline] = {}

    def restore_baseline(self, symbol: str, baseline: CandleBaseline) -> None:
        """Seed a selected symbol from storage, without replacing newer state.

        Intended for startup. This restores no missing candles; process() still
        requires the next opening time to be exactly one minute later.
        """
        if symbol not in self._markets:
            raise ValueError(f"unselected symbol: {symbol}")
        if not isinstance(baseline, CandleBaseline):
            raise TypeError("baseline must be CandleBaseline")
        previous = self._previous.get(symbol)
        if previous is None or baseline.open_time > previous.open_time:
            self._previous[symbol] = baseline

    def process(
        self, event: ClosedKline, references: Mapping[str, ReferenceLevels]
    ) -> list[SweepAlert]:
        if event.symbol not in self._markets:
            raise ValueError(f"unselected symbol: {event.symbol}")
        candle = event.candle
        # Such updates never arrive from the validated feed; ignore defensively
        # without corrupting the last closed-one-minute baseline.
        if not candle.closed or candle.interval != MINUTE:
            return []
        if candle.open_time.second or candle.open_time.microsecond:
            raise ValueError("candle opening time must be minute-aligned")
        if event.event_time.utcoffset() != timedelta(0):
            raise ValueError("event time must be timezone-aware UTC")
        if (
            event.close_time.utcoffset() != timedelta(0)
            or event.close_time != candle.open_time + MINUTE - timedelta(milliseconds=1)
        ):
            raise ValueError("event close time must be the candle's final UTC millisecond")

        active = reference_periods(candle.open_time)
        unknown = references.keys() - active.keys()
        if unknown:
            raise ValueError(f"unknown reference names: {sorted(unknown)}")

        previous = self._previous.get(event.symbol)
        self._previous[event.symbol] = candle
        if previous is None or candle.open_time - previous.open_time != MINUTE:
            return []

        market = self._markets[event.symbol]
        hits = []
        for name, reference in references.items():
            # The candle must belong to the watch period, not the source period.
            # This also prevents using stale or newly activated levels at rollover.
            if reference.levels is None or reference.period != active[name]:
                continue
            for side in ("high", "low"):
                price = getattr(reference.levels, side)
                if detect_sweep(side, price, previous.close, market.tick_size, candle):
                    identity = json.dumps([
                        event.symbol, name, side, reference.period.start.isoformat(),
                        candle.open_time.isoformat(),
                    ], separators=(",", ":"))
                    hits.append(SweepAlert(
                        symbol=event.symbol,
                        captured_quote_volume=market.quote_volume,
                        detected_at=event.event_time, alert_id=identity,
                        level_name=name, side=side,
                        reference_period_start=reference.period.start,
                        candle_time=candle.open_time, level_price=price,
                    ))
        return cast(list[SweepAlert], rank_alerts(hits))

    def process_batch(
        self, events: Iterable[ClosedKline],
        references: Mapping[str, Mapping[str, ReferenceLevels]],
    ) -> list[SweepAlert]:
        """Process in arrival order, then rank the combined hits without merging.

        Missing symbol reference mappings are configuration errors. This is not
        transactional: already processed candles remain baselines if a later
        event raises an error. No candles or alerts are retained beyond baselines.
        """
        hits = []
        for event in events:
            if event.symbol not in references:
                raise ValueError(f"missing references for {event.symbol}")
            hits.extend(self.process(event, references[event.symbol]))
        return cast(list[SweepAlert], rank_alerts(hits))
