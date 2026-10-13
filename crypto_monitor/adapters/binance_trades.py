"""Validated Binance futures trade prices: no candle-close wait.

https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market
"""

import asyncio
import json
import random
import struct
import tempfile
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


class _CheckingSpool:
    """FIFO for preparation prices, using at most 1 MiB before spilling to disk.

    One event-loop thread owns both positions. No timestamp or touch is dropped
    while REST preparation is running. Consumed disk space is reclaimed when
    the remaining tail fits in the bounded buffer; the file closes at handoff.
    """

    _MEMORY_LIMIT = 1024 * 1024
    _HEADER = struct.Struct("!I")
    _MAX_RECORD = 65_536

    def __init__(self):
        try:
            self._file = tempfile.SpooledTemporaryFile(max_size=self._MEMORY_LIMIT, mode="w+b")
        except OSError as exc:
            raise FeedCallbackError("temporary price backlog unavailable") from exc
        self._written = self._read = 0

    def append(self, tick):
        data = json.dumps([tick.symbol, str(tick.price), tick.at.isoformat(),
                           tick.event_time.isoformat(), tick.trade_id], separators=(",", ":")).encode("utf-8")
        if len(data) > self._MAX_RECORD:
            raise FeedMessageError("aggregate trade exceeds price backlog limit")
        frame = self._HEADER.pack(len(data)) + data
        try:
            self._file.seek(self._written)
            if self._file.write(frame) != len(frame):
                raise OSError("incomplete price backlog write")
            self._written += len(frame)
        except (OSError, ValueError) as exc:
            raise FeedCallbackError("temporary price backlog write failed") from exc

    def pop(self):
        if self._read == self._written:
            return None
        try:
            self._file.seek(self._read)
            header = self._file.read(self._HEADER.size)
            if len(header) != self._HEADER.size:
                raise ValueError("incomplete price backlog header")
            size = self._HEADER.unpack(header)[0]
            if not 0 < size <= self._MAX_RECORD:
                raise ValueError("invalid price backlog record")
            data = self._file.read(size)
            if len(data) != size:
                raise ValueError("incomplete price backlog record")
            self._read += self._HEADER.size + size
            symbol, price, at, event_time, trade_id = json.loads(data)
            tick = PriceTick(symbol, Decimal(price), datetime.fromisoformat(at),
                             datetime.fromisoformat(event_time), trade_id)
            remaining = self._written - self._read
            if self._read >= self._MEMORY_LIMIT and remaining <= self._MEMORY_LIMIT:
                # Copy only a bounded tail, never the entire accumulated backlog.
                self._file.seek(self._read)
                tail = self._file.read(remaining)
                if len(tail) != remaining:
                    raise ValueError("incomplete price backlog tail")
                self._file.seek(0)
                if self._file.write(tail) != remaining:
                    raise OSError("incomplete price backlog compaction")
                self._file.truncate(remaining)
                self._read, self._written = 0, remaining
            return tick
        except (OSError, ValueError, TypeError, InvalidOperation, struct.error) as exc:
            raise FeedCallbackError("temporary price backlog read failed") from exc

    def close(self):
        try:
            self._file.close()
        except OSError as exc:
            raise FeedCallbackError("temporary price backlog cleanup failed") from exc


class TradeSupervisor:
    """Reconnect with backoff; rebuild silent history before resuming live alerts."""

    def __init__(self, symbols, callback, on_connect, *, connector=connect,
                 rotation_seconds=23 * 3600, sleep=asyncio.sleep):
        self.symbols = _symbols(symbols)
        self.callback, self.on_connect, self.connector = callback, on_connect, connector
        self.rotation_seconds, self.sleep = rotation_seconds, sleep
        self.state, self.error = "stopped", None
        self.reconnects = 0
        self.on_ready = None
        self._connected_at = None
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
        self._connected_at = None
        try:
            async with self.connector(url, ping_interval=20, ping_timeout=20,
                                      open_timeout=10, close_timeout=5,
                                      max_size=65_536, max_queue=1024) as socket:
                connected_at = time.monotonic()
                self.state = "checking"
                spool = _CheckingSpool()
                live_prices = asyncio.Queue(maxsize=1024)
                live = False

                async def read_prices():
                    read_count = 0
                    async for message in socket:
                        tick = parse_trade(message, symbols)
                        if live:
                            await live_prices.put(tick)
                        else:
                            spool.append(tick)
                        read_count += 1
                        if read_count % 128 == 0:
                            # Even a hot socket with immediately ready messages
                            # must let preparation, stop and replay tasks run.
                            await asyncio.sleep(0)
                    raise FeedConnectionError("remote connection closed")

                async def deliver(tick):
                    try:
                        await self.callback(tick)
                    except Exception as exc:
                        raise FeedCallbackError("price processing failed") from exc

                async def prepare_and_deliver():
                    nonlocal live
                    try:
                        await self.on_connect()
                    except Exception as exc:
                        raise FeedCallbackError("historical preparation failed") from exc
                    replayed = 0
                    while True:
                        tick = spool.pop()
                        if tick is None:
                            # No await between checking the spool and switching
                            # the writer: no price can fall between the two paths.
                            live = True
                            spool.close()
                            break
                        await deliver(tick)
                        replayed += 1
                        if replayed % 128 == 0:
                            await asyncio.sleep(0)
                    if self.on_ready is not None:
                        try:
                            await self.on_ready()
                        except Exception as exc:
                            raise FeedCallbackError("live handoff failed") from exc
                    self._connected_at = time.monotonic()
                    self.state, self.error = "connected", None
                    while True:
                        await deliver(await live_prices.get())

                tasks = [asyncio.create_task(read_prices()), asyncio.create_task(prepare_and_deliver()),
                         asyncio.create_task(stop.wait()),
                         asyncio.create_task(self._changed.wait()),
                         asyncio.create_task(self.sleep(max(0, self.rotation_seconds - (time.monotonic() - connected_at))))]
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in tasks[:2]:
                        if task in done:
                            await task
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    spool.close()
        except (OSError, TimeoutError, WebSocketException) as exc:
            raise FeedConnectionError("trade socket disconnected") from exc

    async def run(self, stop):
        ceiling = 1.0
        try:
            while not stop.is_set():
                self._changed.clear()
                self.state = "connecting"
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
                    if self._connected_at is not None and time.monotonic() - self._connected_at >= 60:
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
