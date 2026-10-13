"""Validated Binance futures trade prices: no candle-close wait.

https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market
"""

import asyncio
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from crypto_monitor.adapters.binance_feed import FeedCallbackError, FeedConnectionError, FeedMessageError, _symbols
from crypto_monitor.runtime.websocket_supervisor import _transient


@dataclass(frozen=True)
class PriceTick:
    symbol: str
    price: Decimal
    at: datetime
    event_time: datetime
    trade_id: int


def parse_trade(message, symbols):
    try:
        value = json.loads(message)
        data = value["data"]
        symbol = data["s"]
        if symbol not in symbols or value["stream"] != f"{symbol.lower()}@aggTrade" or data["e"] != "aggTrade":
            raise ValueError("unexpected stream or symbol")
        if data.get("st", 1) != 1:
            raise ValueError("expected USD-M market")
        if not isinstance(data["p"], str):
            raise ValueError("price must be a decimal string")
        price = Decimal(data["p"])
        if not price.is_finite() or price <= 0:
            raise ValueError("invalid price")
        for key in ("T", "E", "a"):
            if type(data[key]) is not int or data[key] < 0:
                raise ValueError("invalid timestamp or trade identity")
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return PriceTick(symbol, price, epoch + timedelta(milliseconds=data["T"]),
                         epoch + timedelta(milliseconds=data["E"]), data["a"])
    except (ValueError, KeyError, TypeError, InvalidOperation, OverflowError) as exc:
        raise FeedMessageError("malformed aggregate trade message") from exc


class TradeSupervisor:
    """Reconnect with backoff; rebuild silent history before resuming live alerts."""

    def __init__(self, symbols, callback, on_connect, *, connector=connect,
                 rotation_seconds=23 * 3600, sleep=asyncio.sleep):
        self.symbols = _symbols(symbols)
        self.callback, self.on_connect, self.connector = callback, on_connect, connector
        self.rotation_seconds, self.sleep = rotation_seconds, sleep
        self.state, self.error = "stopped", None
        self.reconnects = 0
        self._changed = asyncio.Event()

    def update_symbols(self, symbols):
        selected = _symbols(symbols)
        if selected != self.symbols:
            self.symbols = selected
            self._changed.set()

    async def _session(self, stop):
        url = "wss://fstream.binance.com/market/stream?streams=" + "/".join(
            f"{symbol.lower()}@aggTrade" for symbol in self.symbols)
        symbols = self.symbols
        try:
            async with self.connector(url, ping_interval=20, ping_timeout=20,
                                      open_timeout=10, close_timeout=5, max_queue=1024) as socket:
                connected_at = time.monotonic()
                self.state = "checking"
                try:
                    await self.on_connect()
                except Exception as exc:
                    raise FeedCallbackError("historical preparation failed") from exc
                self.state, self.error = "connected", None
                async def consume():
                    async for message in socket:
                        tick = parse_trade(message, symbols)
                        try:
                            await self.callback(tick)
                        except Exception as exc:
                            raise FeedCallbackError("price processing failed") from exc
                    raise FeedConnectionError("remote connection closed")
                tasks = [asyncio.create_task(consume()), asyncio.create_task(stop.wait()),
                         asyncio.create_task(self._changed.wait()),
                         asyncio.create_task(self.sleep(max(0, self.rotation_seconds - (time.monotonic() - connected_at))))]
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    if tasks[0] in done:
                        await tasks[0]
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except (OSError, TimeoutError, WebSocketException) as exc:
            raise FeedConnectionError("trade socket disconnected") from exc

    async def run(self, stop):
        ceiling = 1.0
        try:
            while not stop.is_set():
                self._changed.clear()
                self.state = "connecting"
                started = time.monotonic()
                try:
                    session = asyncio.create_task(self._session(stop))
                    stopping = asyncio.create_task(stop.wait())
                    try:
                        done, _ = await asyncio.wait((session, stopping), return_when=asyncio.FIRST_COMPLETED)
                        if stopping in done:
                            return
                        await session
                    finally:
                        session.cancel()
                        stopping.cancel()
                        await asyncio.gather(session, stopping, return_exceptions=True)
                    ceiling = 1.0
                except Exception as exc:
                    self.error = str(exc)
                    if not _transient(exc):
                        self.state = "unhealthy"
                        raise
                    if time.monotonic() - started >= 60:
                        ceiling = 1.0
                    self.state = "backoff"
                    delay = ceiling * random.uniform(0.5, 1)
                    waiter = asyncio.create_task(stop.wait())
                    try:
                        await asyncio.wait((waiter,), timeout=delay)
                    finally:
                        waiter.cancel()
                        await asyncio.gather(waiter, return_exceptions=True)
                    ceiling = min(60.0, ceiling * 2)
                self.reconnects += 1
        finally:
            self.state = "stopped"
