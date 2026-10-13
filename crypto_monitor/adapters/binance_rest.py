"""Unauthenticated, read-only USDⓈ-M market data using standard-library HTTP.

One discovery uses exchangeInfo (weight 1) and all-symbol ticker/24hr (weight
40). Binance publishes current IP budgets in exchangeInfo.rateLimits and used
weight in response headers. This adapter neither schedules nor retries calls.

Official references:
https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information
https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics
https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
Kline fields, limits and weights:
https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#klinecandlestick-data
"""

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from http.client import HTTPException
from typing import Any, Callable, Iterable, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from crypto_monitor.adapters.binance_feed import ClosedKline, MinuteCandle
from crypto_monitor.core.level_sweep import DAY, MINUTE, Candle, Levels, Period, calculate_levels, reference_periods
from crypto_monitor.core.market_universe import (
    DEFAULT_STABLECOIN_BASES,
    Market,
    is_eligible_market,
    select_markets,
)


EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
# Binance permits up to 1500. Staying below 500 costs at most 2 weight/page,
# versus 5 for 500..1000 and 10 above 1000; limits 1..99 cost 1.
DAILY_PAGE_LIMIT = 499
MINUTE_PAGE_LIMIT = 499
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MILLISECOND = timedelta(milliseconds=1)


class BinanceDiscoveryError(RuntimeError):
    """Discovery failed; no partial result is available."""


class BinanceHTTPError(BinanceDiscoveryError):
    def __init__(self, url: str, status: int):
        self.url = url
        self.status = status
        super().__init__(f"GET {url}: HTTP {status}")


class BinanceTransportError(BinanceDiscoveryError):
    """Connection, timeout, or response-read failure."""


class BinanceResponseError(BinanceDiscoveryError):
    """Invalid JSON, malformed fields, or incomplete discovery data."""


class UnresolvedGapError(BinanceResponseError):
    """Expected closed minutes are absent. No processing may cross this gap."""

    def __init__(self, symbol: str, start: datetime, end: datetime):
        self.symbol, self.start, self.end = symbol, start, end
        super().__init__(f"unresolved gap for {symbol}: [{start.isoformat()}, {end.isoformat()})")


@dataclass(frozen=True)
class DiscoveredMarket(Market):
    tick_size: Decimal


@dataclass(frozen=True)
class DailyCandle(Candle):
    """A Candle compatible with level calculation, retaining the opening price."""

    open_price: Decimal


def parse_daily_klines(
    payload: Any, *, period: Period, as_of: datetime
) -> list[DailyCandle]:
    """Parse 12-field daily rows; return closed candles within [start, end).

    Empty/gapped history is valid input and yields unavailable levels later.
    Duplicate rows are preserved. Malformed rows raise BinanceResponseError.
    The inclusive exchange close time must be next UTC midnight minus 1 ms;
    a candle is complete at that next midnight, not during its final ms.
    """
    _validate_as_of(as_of)
    if not isinstance(payload, list):
        raise BinanceResponseError("klines: expected an array")
    candles = []
    for index, row in enumerate(payload):
        label = f"klines[{index}]"
        if not isinstance(row, list) or len(row) != 12:
            raise BinanceResponseError(f"{label}: expected a 12-field array")
        if any(type(row[i]) is not int or row[i] < 0 for i in (0, 6, 8)):
            raise BinanceResponseError(f"{label}: timestamps/trade count must be nonnegative integers")
        try:
            opened = _EPOCH + timedelta(milliseconds=row[0])
            end = opened + DAY
        except OverflowError as exc:
            raise BinanceResponseError(f"{label}: timestamp out of range") from exc
        if opened != opened.replace(hour=0, minute=0, second=0, microsecond=0):
            raise BinanceResponseError(f"{label}: daily open must be UTC midnight")
        if row[6] != row[0] + DAY // _MILLISECOND - 1:
            raise BinanceResponseError(f"{label}: invalid daily close time")
        opening, high, low, close = (
            _decimal_field(row[i], f"{label}.{name}")
            for i, name in enumerate(("open", "high", "low", "close"), start=1)
        )
        if not low <= opening <= high or not low <= close <= high:
            raise BinanceResponseError(f"{label}: inconsistent OHLC prices")
        for i in (5, 7, 9, 10):
            _decimal_field(row[i], f"{label}[{i}]")
        if not isinstance(row[11], str):
            raise BinanceResponseError(f"{label}[11]: expected a string")
        if period.start <= opened < period.end and end <= as_of:
            candles.append(DailyCandle(
                open_time=opened, interval=DAY, high=high, low=low, close=close,
                closed=True, open_price=opening,
            ))
    return sorted(candles, key=lambda candle: candle.open_time)


def _validate_as_of(as_of: datetime) -> None:
    if not isinstance(as_of, datetime) or as_of.utcoffset() != timedelta(0):
        raise ValueError("as_of must be timezone-aware UTC")


def _minute_range(start: datetime, end: datetime, as_of: datetime) -> None:
    for boundary in (start, end, as_of):
        _validate_as_of(boundary)
    if start >= end or start < _EPOCH:
        raise ValueError("minute range must have epoch <= start < end")
    if any(value.second or value.microsecond for value in (start, end)):
        raise ValueError("minute boundaries must be minute-aligned")


def parse_minute_klines(
    payload: Any, *, symbol: str, start: datetime, end: datetime, as_of: datetime
) -> list[ClosedKline]:
    """Validate one ordered page, without sorting or deduplicating bad data.

    Unclosed candles are excluded; completeness is checked by the iterator.
    REST has no event timestamp, so event_time uses the explicit as_of value.
    """
    _minute_range(start, end, as_of)
    if not isinstance(payload, list):
        raise BinanceResponseError("minute klines: expected an array")
    result = []
    previous = None
    for index, row in enumerate(payload):
        label = f"minute klines[{index}]"
        if not isinstance(row, list) or len(row) != 12:
            raise BinanceResponseError(f"{label}: expected a 12-field array")
        if any(type(row[i]) is not int or row[i] < 0 for i in (0, 6, 8)):
            raise BinanceResponseError(f"{label}: invalid timestamps or trade count")
        try:
            opened = _EPOCH + row[0] * _MILLISECOND
            closed_at = opened + MINUTE
        except OverflowError as exc:
            raise BinanceResponseError(f"{label}: timestamp out of range") from exc
        if row[0] % 60_000 or row[6] != row[0] + 59_999:
            raise BinanceResponseError(f"{label}: invalid one-minute timestamps")
        if not start <= opened < end:
            raise BinanceResponseError(f"{label}: candle outside requested window")
        if previous is not None and opened <= previous:
            raise BinanceResponseError(f"{label}: duplicate or out-of-order candle")
        previous = opened
        opening, high, low, close = (
            _decimal_field(row[i], f"{label}[{i}]") for i in (1, 2, 3, 4)
        )
        if not low <= opening <= high or not low <= close <= high:
            raise BinanceResponseError(f"{label}: inconsistent OHLC prices")
        for i in (5, 7, 9, 10):
            _decimal_field(row[i], f"{label}[{i}]")
        if not isinstance(row[11], str):
            raise BinanceResponseError(f"{label}[11]: expected a string")
        if closed_at <= as_of:
            result.append(ClosedKline(
                symbol, MinuteCandle(opened, MINUTE, high, low, close, True, opening),
                as_of, closed_at - _MILLISECOND,
            ))
    return result


def _decimal_field(value: Any, label: str, *, positive: bool = False) -> Decimal:
    # Binance documents these fields as decimal strings, not JSON floats.
    if not isinstance(value, str):
        raise BinanceResponseError(f"{label}: expected a decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise BinanceResponseError(f"{label}: invalid decimal") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        requirement = "positive" if positive else "nonnegative"
        raise BinanceResponseError(f"{label}: must be finite and {requirement}")
    return result


def _rows(payload: Any, label: str, fields: tuple[str, ...]) -> dict[str, dict]:
    if not isinstance(payload, list) or not payload:
        raise BinanceResponseError(f"{label}: expected a nonempty array")
    indexed = {}
    for index, row in enumerate(payload):
        if not isinstance(row, dict):
            raise BinanceResponseError(f"{label}[{index}]: expected an object")
        for field in ("symbol",) + fields:
            if not isinstance(row.get(field), str) or not row[field]:
                raise BinanceResponseError(
                    f"{label}[{index}].{field}: expected a nonempty string"
                )
        symbol = row["symbol"]
        if symbol in indexed:
            raise BinanceResponseError(f"{label}: duplicate symbol {symbol}")
        indexed[symbol] = row
    return indexed


def _tick_size(info: dict) -> Decimal:
    label = f"exchangeInfo {info['symbol']} PRICE_FILTER.tickSize"
    filters = info.get("filters")
    if not isinstance(filters, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("filterType"), str)
        for item in filters
    ):
        raise BinanceResponseError(f"{label}: malformed filters array")
    prices = [item for item in filters if item["filterType"] == "PRICE_FILTER"]
    if len(prices) != 1:
        raise BinanceResponseError(f"{label}: expected exactly one PRICE_FILTER")
    return _decimal_field(prices[0].get("tickSize"), label, positive=True)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"nonstandard JSON constant {value}")


class BinanceRESTAdapter:
    """A synchronous adapter with an injectable urllib-compatible opener.

    opener(request, timeout=seconds) must return a context manager exposing
    status and read(), or raise urllib/OS transport errors. The default uses
    urlopen. Timeout is a finite socket-operation timeout, not an overall
    deadline across both requests. No credentials or retries are used.
    """

    def __init__(
        self, *, timeout: float = 10.0, opener: Callable[..., Any] | None = None
    ):
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("timeout must be a finite positive number of seconds")
        self.timeout = timeout
        self._opener = opener if opener is not None else urlopen

    def _get_json(self, url: str) -> Any:
        request = Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with self._opener(request, timeout=self.timeout) as response:
                if not 200 <= response.status < 300:
                    raise BinanceHTTPError(url, response.status)
                body = response.read()
        except HTTPError as exc:
            exc.close()
            raise BinanceHTTPError(url, exc.code) from exc
        except TimeoutError as exc:
            raise BinanceTransportError(f"GET {url}: request timed out") from exc
        except URLError as exc:
            detail = (
                "request timed out" if isinstance(exc.reason, TimeoutError)
                else str(exc.reason)
            )
            raise BinanceTransportError(f"GET {url}: {detail}") from exc
        except (OSError, HTTPException) as exc:
            raise BinanceTransportError(f"GET {url}: transport failure: {exc}") from exc
        try:
            return json.loads(
                body, parse_float=Decimal, parse_constant=_reject_json_constant
            )
        except (ValueError, UnicodeError, TypeError) as exc:
            raise BinanceResponseError(f"GET {url}: invalid JSON response") from exc

    def fetch_daily_klines(
        self, symbol: str, *, start: datetime, end: datetime, as_of: datetime
    ) -> list[DailyCandle]:
        """Fetch explicit UTC midnight boundaries with an exclusive end.

        Use exact day counts for limits, capping pages at 499. Advance by each
        requested window even for short/empty responses: never fill gaps or
        deduplicate. Transport/parse failures abort rather than return partial
        data. Each page inherits the existing timeout and no-retry behavior.
        """
        period = Period(start, end)
        _validate_as_of(as_of)
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a nonempty string")
        symbol = symbol.strip().upper()
        candles = []
        cursor = period.start
        while cursor < period.end:
            days = min((period.end - cursor).days, DAILY_PAGE_LIMIT)
            page_end = cursor + days * DAY
            query = urlencode({
                "symbol": symbol, "interval": "1d",
                "startTime": (cursor - _EPOCH) // _MILLISECOND,
                "endTime": (page_end - _EPOCH) // _MILLISECOND - 1,
                "limit": days,
            })
            payload = self._get_json(f"{KLINES_URL}?{query}")
            candles.extend(parse_daily_klines(
                payload, period=Period(cursor, page_end), as_of=as_of
            ))
            cursor = page_end
        return candles

    def fetch_reference_levels(
        self, symbol: str, *, as_of: datetime
    ) -> dict[str, Levels | None]:
        """Calculate all current reference levels from their smallest enclosing range.

        This is one contiguous range, including any gaps between periods.
        Current reference windows fit below 499 days, so need one request.
        Inactive Monday, missing days and duplicate coverage yield None through
        the unchanged reference-period and complete-history calculation logic.
        """
        periods = reference_periods(as_of)
        active = [period for period in periods.values() if period is not None]
        candles = self.fetch_daily_klines(
            symbol, start=min(period.start for period in active),
            end=max(period.end for period in active), as_of=as_of,
        )
        return {
            name: calculate_levels(period, candles) if period is not None else None
            for name, period in periods.items()
        }

    def iter_minute_klines(
        self, symbol: str, *, start: datetime, end: datetime, as_of: datetime
    ) -> Iterator[ClosedKline]:
        """Yield complete, validated 1m pages over [start, end), at most 499 rows.

        Each endTime is page_end minus 1 ms. A bad/incomplete page yields
        nothing; earlier pages may already have been consumed and committed.
        No automatic retries. Resume using the caller's committed cursor.
        """
        _minute_range(start, end, as_of)
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a nonempty string")
        symbol = symbol.strip().upper()
        cursor = start
        while cursor < end:
            count = min((end - cursor) // MINUTE, MINUTE_PAGE_LIMIT)
            page_end = cursor + count * MINUTE
            query = urlencode({
                "symbol": symbol, "interval": "1m",
                "startTime": (cursor - _EPOCH) // _MILLISECOND,
                "endTime": (page_end - _EPOCH) // _MILLISECOND - 1,
                "limit": count,
            })
            candles = parse_minute_klines(
                self._get_json(f"{KLINES_URL}?{query}"), symbol=symbol,
                start=cursor, end=page_end, as_of=as_of,
            )
            expected = cursor
            for event in candles:
                if event.candle.open_time != expected:
                    raise UnresolvedGapError(symbol, expected, page_end)
                expected += MINUTE
            if expected != page_end:
                raise UnresolvedGapError(symbol, expected, page_end)
            yield from candles
            cursor = page_end

    def discover_markets(
        self,
        *,
        top_n: int = 20,
        include_symbols: Iterable[str] = (),
        exclude_symbols: Iterable[str] = (),
        stablecoin_bases: Iterable[str] = DEFAULT_STABLECOIN_BASES,
        exchange_info: dict | None = None,
        tickers: list | None = None,
    ) -> list[DiscoveredMarket]:
        """Fetch both snapshots and return a complete, ranked discovery result.

        All eligible symbols, even below the cutoff, must have valid ticks and
        tickers. Empty payloads or a selection with no markets raise an error.
        Excluded/ineligible symbols need no tick size or matching ticker.
        """
        if type(top_n) is not int or top_n < 0:
            raise ValueError("top_n must be a nonnegative integer")
        included = tuple(include_symbols)
        excluded = {name.strip().upper() for name in exclude_symbols}
        stablecoins = {name.strip().upper() for name in stablecoin_bases}

        exchange = self.fetch_exchange_info() if exchange_info is None else exchange_info
        if not isinstance(exchange, dict):
            raise BinanceResponseError("exchangeInfo: expected an object")
        symbols = _rows(exchange.get("symbols"), "exchangeInfo.symbols", (
            "baseAsset", "quoteAsset", "marginAsset", "contractType", "status",
        ))
        tickers = self.fetch_tickers() if tickers is None else tickers
        ticker_index = _rows(tickers, "ticker/24hr", ("quoteVolume",))
        for symbol, ticker in ticker_index.items():
            _decimal_field(ticker["quoteVolume"], f"ticker/24hr {symbol} quoteVolume")

        ticks = {}
        for symbol, info in symbols.items():
            if is_eligible_market(
                info, excluded_symbols=excluded, stablecoin_bases=stablecoins
            ):
                if symbol not in ticker_index:
                    raise BinanceResponseError(
                        f"ticker/24hr: missing eligible symbol {symbol}"
                    )
                ticks[symbol] = _tick_size(info)

        markets = select_markets(
            exchange, tickers, top_n=top_n, include_symbols=included,
            exclude_symbols=excluded, stablecoin_bases=stablecoins,
        )
        if not markets:
            raise BinanceResponseError(
                "market discovery selected no markets; check data and filters"
            )
        return [
            DiscoveredMarket(m.symbol, m.base_asset, m.quote_volume, ticks[m.symbol])
            for m in markets
        ]

    def fetch_exchange_info(self) -> dict:
        """Public exchange snapshot (weight 1); useful for daily membership refresh."""
        exchange = self._get_json(EXCHANGE_INFO_URL)
        if not isinstance(exchange, dict):
            raise BinanceResponseError("exchangeInfo: expected an object")
        _rows(exchange.get("symbols"), "exchangeInfo.symbols", (
            "baseAsset", "quoteAsset", "marginAsset", "contractType", "status",
        ))
        return exchange

    def fetch_tickers(self) -> list:
        """All-symbol 24h tickers (weight 40), without fetching exchange information."""
        tickers = self._get_json(TICKER_URL)
        parse_quote_volumes(tickers)
        return tickers


def parse_quote_volumes(tickers: list) -> dict[str, Decimal]:
    """Strict shared validation for refreshes, including duplicate symbols."""
    return {symbol: _decimal_field(row["quoteVolume"], f"ticker/24hr {symbol} quoteVolume")
            for symbol, row in _rows(tickers, "ticker/24hr", ("quoteVolume",)).items()}
