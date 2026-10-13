"""One connection of closed USDⓈ-M one-minute candles; no reconnect or recovery.

Binance: /market/stream, 24-hour lifetime, server ping every 3 minutes, pong
required within 10 minutes; at most 1024 streams and 10 client messages/second.
websockets answers protocol pings automatically. Its 20-second client ping and
pong timeout also detect dead connections. No JSON heartbeat messages are sent.

https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect
https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Iterable
from urllib.parse import quote

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from crypto_monitor.core.level_sweep import MINUTE, Candle


STREAM_URL = "wss://fstream.binance.com/market/stream?streams="
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class FeedError(RuntimeError):
    """The feed stopped and requires caller attention."""


class FeedMessageError(FeedError):
    """Invalid JSON, message shape, routing, or candle values."""


class FeedConnectionError(FeedError):
    """Connection failure or remote disconnect; missing candles are possible."""


class FeedCallbackError(FeedError):
    """The consumer callback failed; the cause is preserved."""


@dataclass(frozen=True)
class MinuteCandle(Candle):
    open_price: Decimal


@dataclass(frozen=True)
class ClosedKline:
    symbol: str
    candle: MinuteCandle
    event_time: datetime
    close_time: datetime  # Binance's inclusive final millisecond.


def _symbols(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError("symbols must be a collection of symbol names")
    normalized = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("each symbol must be a nonempty string")
        name = value.strip().upper()
        if not all(character.isalnum() or character == "_" for character in name):
            raise ValueError(f"invalid symbol: {value!r}")
        normalized.add(name)
    if not 1 <= len(normalized) <= 1024:
        raise ValueError("a feed requires 1..1024 distinct symbols")
    return tuple(sorted(normalized))


def _timestamp(value: Any, field: str) -> datetime:
    if type(value) is not int or value < 0:
        raise FeedMessageError(f"{field}: expected nonnegative integer milliseconds")
    try:
        return _EPOCH + timedelta(milliseconds=value)
    except OverflowError as exc:
        raise FeedMessageError(f"{field}: timestamp out of range") from exc


def _price(value: Any, field: str) -> Decimal:
    if not isinstance(value, str):
        raise FeedMessageError(f"{field}: expected a decimal string")
    try:
        price = Decimal(value)
    except InvalidOperation as exc:
        raise FeedMessageError(f"{field}: invalid decimal") from exc
    if not price.is_finite() or price < 0:
        raise FeedMessageError(f"{field}: expected finite nonnegative price")
    return price


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant {value}")


def parse_combined_message(
    message: str | bytes, selected_symbols: Iterable[str]
) -> ClosedKline | None:
    """Validate relevant event fields; ignore valid updates with x=false.

    Check the stream name, event symbol and nested candle symbol against the
    selected universe. Unknown extra fields are tolerated. Duplicates aren't
    suppressed and event times aren't compared with the machine's clock.
    """
    selected = _symbols(selected_symbols)
    try:
        payload = json.loads(message, parse_float=Decimal, parse_constant=_reject_constant)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise FeedMessageError("invalid combined-stream JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise FeedMessageError("expected a combined-stream object with data")
    data = payload["data"]
    symbol = data.get("s")
    if not isinstance(symbol, str) or symbol not in selected:
        raise FeedMessageError("event symbol is not in the selected universe")
    if payload.get("stream") != f"{symbol.lower()}@kline_1m":
        raise FeedMessageError("stream name does not match the selected 1m symbol")
    if data.get("e") != "kline" or not isinstance(data.get("k"), dict):
        raise FeedMessageError("expected a kline event and candle object")
    kline = data["k"]
    if kline.get("s") != symbol or kline.get("i") != "1m":
        raise FeedMessageError("candle symbol or interval does not match")
    if type(kline.get("x")) is not bool:
        raise FeedMessageError("candle closed flag must be boolean")
    event_time = _timestamp(data.get("E"), "E")
    opened = _timestamp(kline.get("t"), "k.t")
    closed = _timestamp(kline.get("T"), "k.T")
    if kline["t"] % 60_000 or kline["T"] != kline["t"] + 59_999:
        raise FeedMessageError("candle timestamps must span one aligned UTC minute")
    opening, high, low, close = (
        _price(kline.get(field), f"k.{field}") for field in ("o", "h", "l", "c")
    )
    if not low <= opening <= high or not low <= close <= high:
        raise FeedMessageError("inconsistent OHLC prices")
    if not kline["x"]:
        return None
    return ClosedKline(
        symbol=symbol, event_time=event_time, close_time=closed,
        candle=MinuteCandle(
            open_time=opened, interval=MINUTE, high=high, low=low, close=close,
            closed=True, open_price=opening,
        ),
    )


class BinanceKlineFeed:
    """Feed symbols from selected markets, e.g. [market.symbol for market in markets].

    on_candle is an async callback taking one ClosedKline. Calls are serial;
    keep the callback short and nonblocking so heartbeat handling can run.
    connector has websockets.connect's async-context-manager interface.

    await run(stop) ends on stop.set() or task cancellation, including during
    connection setup. A normal remote close is still a disconnect error.
    No reconnection, deduplication, or gap-free delivery guarantee is provided.
    """

    def __init__(
        self, symbols: Iterable[str], on_candle: Callable[[ClosedKline], Awaitable[None]],
        *, connector: Callable[..., Any] | None = None,
    ):
        self.symbols = _symbols(symbols)
        self.url = STREAM_URL + "/".join(
            quote(f"{symbol.lower()}@kline_1m", safe="@_") for symbol in self.symbols
        )
        if not callable(on_candle):
            raise TypeError("on_candle must be an async callback")
        self._on_candle = on_candle
        self._connector = connector if connector is not None else connect
        self._running = False

    async def _consume(self, stop: asyncio.Event) -> None:
        try:
            async with self._connector(
                self.url, open_timeout=10, close_timeout=5,
                ping_interval=20, ping_timeout=20,
                max_size=65_536, max_queue=16, compression=None,
            ) as socket:
                async for message in socket:
                    if stop.is_set():
                        return
                    event = parse_combined_message(message, self.symbols)
                    if event is not None:
                        try:
                            await self._on_candle(event)
                        except Exception as exc:
                            raise FeedCallbackError(f"callback failed for {event.symbol}") from exc
                    if stop.is_set():
                        return
                if not stop.is_set():
                    raise FeedConnectionError("Binance stream closed remotely; candle gaps are possible")
        except (WebSocketException, OSError, TimeoutError) as exc:
            raise FeedConnectionError(f"Binance connection failed or disconnected: {exc}") from exc

    async def run(self, stop: asyncio.Event) -> None:
        if self._running:
            raise RuntimeError("this feed is already running")
        if stop.is_set():
            return
        self._running = True
        session = asyncio.create_task(self._consume(stop))
        stopping = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait(
                (session, stopping), return_when=asyncio.FIRST_COMPLETED
            )
            if session in done:
                await session  # Surface parser, callback and connection failures.
        finally:
            for task in (session, stopping):
                if not task.done():
                    task.cancel()
            # Cancellation unwinds the connection context and awaits its close.
            results = await asyncio.gather(session, stopping, return_exceptions=True)
            self._running = False
        if isinstance(results[0], Exception):
            raise results[0]  # A close failure during requested shutdown isn't success.
