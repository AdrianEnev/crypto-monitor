"""Combined-stream parsing and lifecycle tests; connections are always mocked."""

import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from unittest.mock import AsyncMock, MagicMock, patch

from websockets.exceptions import ConnectionClosedError, InvalidHandshake
from websockets.frames import Close

from crypto_monitor.adapters.binance_feed import (
    BinanceKlineFeed,
    FeedCallbackError,
    FeedConnectionError,
    FeedMessageError,
    parse_combined_message,
)
from crypto_monitor.core.level_sweep import MINUTE, Candle


def payload(symbol="BTCUSDT", closed=True):
    return {
        "stream": f"{symbol.lower()}@kline_1m",
        "data": {
            "e": "kline", "E": 1609459260000, "s": symbol,
            "k": {
                "t": 1609459200000, "T": 1609459259999,
                "s": symbol, "i": "1m", "x": closed,
                "o": "100.00000000000000001", "h": "110.00000000000000001",
                "l": "90.00000000000000001", "c": "105.00000000000000001",
                "f": 100, "L": 109, "n": 10, "v": "1000", "q": "100000",
                "V": "500", "Q": "50000", "B": "0",
            },
        },
    }


def message(symbol="BTCUSDT", closed=True):
    return json.dumps(payload(symbol, closed))


class ParsingTests(unittest.TestCase):
    def parse(self, value):
        return parse_combined_message(json.dumps(value), ["BTCUSDT"])

    def test_closed_candle_decimal_prices_and_utc_timestamps(self):
        raw = payload()
        with localcontext() as context:
            context.prec = 6
            result = self.parse(raw)
        self.assertEqual(result.symbol, "BTCUSDT")
        self.assertIsInstance(result.candle, Candle)
        self.assertEqual(result.candle.interval, MINUTE)
        self.assertTrue(result.candle.closed)
        opened = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(result.candle.open_time, opened)
        self.assertEqual(result.close_time, opened + MINUTE - timedelta(milliseconds=1))
        self.assertEqual(result.event_time, opened + MINUTE)
        for value in [result.candle.open_time, result.close_time, result.event_time]:
            self.assertIs(value.tzinfo, timezone.utc)
        for field, key in [("open_price", "o"), ("high", "h"), ("low", "l"), ("close", "c")]:
            value = getattr(result.candle, field)
            self.assertIsInstance(value, Decimal)
            self.assertEqual(value, Decimal(raw["data"]["k"][key]))

    def test_bytes_and_extra_fields(self):
        raw = payload()
        raw["data"]["extra"] = "future field"
        result = parse_combined_message(json.dumps(raw).encode(), ["btcusdt"])
        self.assertEqual(result.symbol, "BTCUSDT")

    def test_unclosed_candle_is_ignored(self):
        self.assertIsNone(self.parse(payload(closed=False)))

    def test_invalid_json(self):
        for raw in ["{", "NaN", b"\xff", None]:
            with self.subTest(raw=raw), self.assertRaisesRegex(FeedMessageError, "JSON"):
                parse_combined_message(raw, ["BTCUSDT"])

    def test_malformed_combined_envelopes(self):
        for raw in [None, [], {}, {"data": []}, payload()["data"],
                    {"result": None, "id": 1}, {"data": payload()["data"]}]:
            with self.subTest(raw=raw), self.assertRaises(FeedMessageError):
                self.parse(raw)

    def test_wrong_or_conflicting_symbols(self):
        for path, value in [
            (("stream",), "ethusdt@kline_1m"),
            (("data", "s"), "ETHUSDT"),
            (("data", "k", "s"), "ETHUSDT"),
            (("data", "s"), []),
        ]:
            raw = payload()
            parent = raw
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(FeedMessageError):
                self.parse(raw)
        with self.assertRaises(FeedMessageError):
            self.parse(payload("ETHUSDT"))

    def test_wrong_event_and_interval(self):
        raw = payload()
        raw["data"]["e"] = "aggTrade"
        with self.assertRaises(FeedMessageError):
            self.parse(raw)
        raw = payload()
        raw["data"]["k"]["i"] = "5m"
        with self.assertRaises(FeedMessageError):
            self.parse(raw)
        raw = payload()
        raw["stream"] = "btcusdt@kline_5m"
        with self.assertRaises(FeedMessageError):
            self.parse(raw)

    def test_required_candle_fields_and_strict_boolean(self):
        for key in ["t", "T", "s", "i", "x", "o", "h", "l", "c"]:
            raw = payload()
            del raw["data"]["k"][key]
            with self.subTest(missing=key), self.assertRaises(FeedMessageError):
                self.parse(raw)
        for value in [None, "false", "true", 0, 1]:
            raw = payload(closed=value)
            with self.subTest(flag=value), self.assertRaises(FeedMessageError):
                self.parse(raw)
        for value in [None, [], "kline"]:
            raw = payload()
            raw["data"]["k"] = value
            with self.subTest(kline=value), self.assertRaises(FeedMessageError):
                self.parse(raw)

    def test_malformed_prices(self):
        for key in ("o", "h", "l", "c"):
            for value in [None, 100.0, "bad", "NaN", "Infinity", "-1"]:
                raw = payload()
                raw["data"]["k"][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(FeedMessageError):
                    self.parse(raw)
        for key, value in [("o", "120"), ("c", "80"), ("h", "85")]:
            raw = payload()
            raw["data"]["k"][key] = value
            with self.subTest(key=key), self.assertRaises(FeedMessageError):
                self.parse(raw)

    def test_invalid_timestamps_and_minute_alignment(self):
        for key in ("E", "t", "T"):
            for value in [None, True, -1, 1.5, "1609459200000", 10**30]:
                raw = payload()
                target = raw["data"] if key == "E" else raw["data"]["k"]
                target[key] = value
                with self.subTest(key=key, value=value), self.assertRaises(FeedMessageError):
                    self.parse(raw)
        for key, value in [("t", 1609459200001), ("T", 1609459260000)]:
            raw = payload()
            raw["data"]["k"][key] = value
            with self.subTest(key=key), self.assertRaises(FeedMessageError):
                self.parse(raw)

    def test_symbol_configuration_and_routed_url(self):
        feed = BinanceKlineFeed([" ethusdt ", "BTCUSDT", "btcusdt"], AsyncMock())
        self.assertEqual(feed.url,
            "wss://fstream.binance.com/market/stream?streams="
            "btcusdt@kline_1m/ethusdt@kline_1m")
        for symbols in [[], "BTCUSDT", [""], [None], ["BTCUSDT/evil"],
                        [f"COIN{i}USDT" for i in range(1025)]]:
            with self.subTest(symbols=symbols), self.assertRaises(ValueError):
                BinanceKlineFeed(symbols, AsyncMock())


class FakeSocket:
    """None ends the remote stream; exceptions simulate protocol/transport failure."""

    def __init__(self, messages=()):
        self.queue = asyncio.Queue()
        for item in messages:
            self.queue.put_nowait(item)
        self.entered = asyncio.Event()
        self.waiting = asyncio.Event()
        self.closed = False
        self.close_error = None

    async def __aenter__(self):
        self.entered.set()
        return self

    async def __aexit__(self, *args):
        self.closed = True
        if self.close_error:
            raise self.close_error

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.waiting.set()
        item = await self.queue.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
        return item


class FeedLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        patcher = patch("crypto_monitor.adapters.binance_feed.connect")
        self.connector = patcher.start()
        self.addCleanup(patcher.stop)
        self.stop = asyncio.Event()
        self.callback = AsyncMock()
        self.socket = FakeSocket()
        self.connector.return_value = self.socket

    def feed(self, callback=None):
        return BinanceKlineFeed(["BTCUSDT", "ETHUSDT"], callback or self.callback)

    async def finish(self, task):
        await asyncio.wait_for(task, timeout=1)

    async def test_dispatches_only_closed_candles_then_shuts_down(self):
        self.socket = FakeSocket([message(closed=False), message(), message("ETHUSDT")])
        self.connector.return_value = self.socket
        seen = []

        async def callback(event):
            seen.append(event)
            if len(seen) == 2:
                self.stop.set()

        feed = self.feed(callback)
        await self.finish(feed.run(self.stop))
        self.assertEqual([event.symbol for event in seen], ["BTCUSDT", "ETHUSDT"])
        self.assertTrue(self.socket.closed)
        self.connector.assert_called_once_with(
            feed.url, open_timeout=10, close_timeout=5, ping_interval=20,
            ping_timeout=20, max_size=65_536, max_queue=16, compression=None,
        )

    async def test_stop_while_waiting_for_data_closes_connection(self):
        task = asyncio.create_task(self.feed().run(self.stop))
        await self.finish(self.socket.waiting.wait())
        self.stop.set()
        await self.finish(task)
        self.assertTrue(self.socket.closed)
        self.callback.assert_not_awaited()

    async def test_pre_requested_stop_does_not_connect(self):
        self.stop.set()
        await self.feed().run(self.stop)
        self.connector.assert_not_called()

    async def test_task_cancellation_closes_connection(self):
        task = asyncio.create_task(self.feed().run(self.stop))
        await self.finish(self.socket.entered.wait())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.finish(task)
        self.assertTrue(self.socket.closed)

    async def test_stop_during_connection_setup_cancels_handshake(self):
        started, cancelled = asyncio.Event(), asyncio.Event()
        context = MagicMock()

        async def opening():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        context.__aenter__ = AsyncMock(side_effect=opening)
        context.__aexit__ = AsyncMock()
        self.connector.return_value = context
        task = asyncio.create_task(self.feed().run(self.stop))
        await self.finish(started.wait())
        self.stop.set()
        await self.finish(task)
        self.assertTrue(cancelled.is_set())
        context.__aexit__.assert_not_awaited()

    async def test_normal_remote_close_is_exposed_without_reconnect(self):
        self.socket.queue.put_nowait(None)
        with self.assertRaisesRegex(FeedConnectionError, "closed remotely"):
            await self.finish(self.feed().run(self.stop))
        self.assertTrue(self.socket.closed)
        self.connector.assert_called_once()

    async def test_connection_errors_are_exposed_without_retries(self):
        for error in [OSError("network"), TimeoutError("opening"), InvalidHandshake("bad handshake")]:
            with self.subTest(error=error):
                self.connector.reset_mock()
                self.connector.side_effect = error
                with self.assertRaises(FeedConnectionError) as caught:
                    await self.finish(self.feed().run(self.stop))
                self.assertIs(caught.exception.__cause__, error)
                self.connector.assert_called_once()

    async def test_abnormal_remote_close_preserves_cause(self):
        error = ConnectionClosedError(Close(1011, "heartbeat failure"), None)
        self.socket.queue.put_nowait(error)
        with self.assertRaises(FeedConnectionError) as caught:
            await self.finish(self.feed().run(self.stop))
        self.assertIs(caught.exception.__cause__, error)
        self.assertTrue(self.socket.closed)

    async def test_bad_message_stops_and_closes_feed(self):
        self.socket.queue.put_nowait("invalid JSON")
        with self.assertRaises(FeedMessageError):
            await self.finish(self.feed().run(self.stop))
        self.assertTrue(self.socket.closed)
        self.callback.assert_not_awaited()

    async def test_callback_error_is_distinct_from_connection_error(self):
        error = OSError("consumer failed")
        self.callback.side_effect = error
        self.socket.queue.put_nowait(message())
        with self.assertRaises(FeedCallbackError) as caught:
            await self.finish(self.feed().run(self.stop))
        self.assertIs(caught.exception.__cause__, error)
        self.assertTrue(self.socket.closed)

    async def test_shutdown_close_failure_is_reported(self):
        self.socket.close_error = OSError("close failed")
        task = asyncio.create_task(self.feed().run(self.stop))
        await self.finish(self.socket.waiting.wait())
        self.stop.set()
        with self.assertRaisesRegex(FeedConnectionError, "close failed"):
            await self.finish(task)

    async def test_connector_can_be_injected(self):
        transport = MagicMock(return_value=self.socket)
        feed = BinanceKlineFeed(["BTCUSDT"], self.callback, connector=transport)
        task = asyncio.create_task(feed.run(self.stop))
        await self.finish(self.socket.waiting.wait())
        self.stop.set()
        await self.finish(task)
        transport.assert_called_once()
        self.connector.assert_not_called()

    async def test_same_feed_cannot_run_concurrently(self):
        feed = self.feed()
        task = asyncio.create_task(feed.run(self.stop))
        await self.finish(self.socket.entered.wait())
        with self.assertRaisesRegex(RuntimeError, "already running"):
            await feed.run(self.stop)
        self.stop.set()
        await self.finish(task)


if __name__ == "__main__":
    unittest.main()
