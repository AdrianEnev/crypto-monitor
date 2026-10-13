"""Aggregate-trade parsing and reconnect lifecycle with mocked sockets only."""

import asyncio
import json
import unittest
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from unittest.mock import AsyncMock, patch

from crypto_monitor.adapters.binance_feed import FeedCallbackError, FeedMessageError
from crypto_monitor.adapters.binance_trades import TradeSupervisor, parse_trade


def payload(symbol="BTCUSDT"):
    return {"stream": f"{symbol.lower()}@aggTrade", "data": {
        "e": "aggTrade", "E": 1791892800123, "s": symbol, "a": 123456,
        "p": "100.000000000000000001", "q": "1.5", "f": 10, "l": 11,
        "T": 1791892800100, "m": False, "st": 1,
    }}


class TradeParsingTests(unittest.TestCase):
    def test_exact_price_and_trade_time_with_no_candle_fields(self):
        with localcontext() as context:
            context.prec = 6
            result = parse_trade(json.dumps(payload()), ("BTCUSDT",))
        self.assertEqual(result.price, Decimal("100.000000000000000001"))
        self.assertEqual(result.at, datetime(2026, 10, 13, 12, 0, 0, 100000, tzinfo=timezone.utc))
        self.assertEqual(result.event_time.microsecond, 123000)
        self.assertEqual(result.trade_id, 123456)

    def test_bytes_optional_market_type_and_additional_fields(self):
        raw = payload()
        del raw["data"]["st"]
        raw["data"]["futureField"] = "allowed"
        self.assertEqual(parse_trade(json.dumps(raw).encode(), ("BTCUSDT",)).symbol, "BTCUSDT")

    def test_invalid_json_and_envelopes_are_feed_errors(self):
        for raw in ("{", "null", "NaN", "[]", "{}", b"\xff", None,
                    json.dumps({"data": []}), json.dumps(payload()["data"])):
            with self.subTest(raw=raw), self.assertRaises(FeedMessageError):
                parse_trade(raw, ("BTCUSDT",))

    def test_each_required_field_is_checked(self):
        for field in ("e", "s", "p", "a", "T", "E"):
            raw = payload()
            del raw["data"][field]
            with self.subTest(field=field), self.assertRaises(FeedMessageError):
                parse_trade(json.dumps(raw), ("BTCUSDT",))

    def test_unselected_symbol_and_conflicting_stream_are_rejected(self):
        for location, key, value in (("data", "s", "ETHUSDT"), ("data", "s", []),
                                     ("data", "e", "kline"), ("data", "st", 2),
                                     (None, "stream", "btcusdt@trade"),
                                     (None, "stream", "ethusdt@aggTrade")):
            raw = payload()
            (raw if location is None else raw[location])[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(FeedMessageError):
                parse_trade(json.dumps(raw), ("BTCUSDT",))

    def test_price_must_be_positive_finite_decimal_string(self):
        for value in (True, 100, 100.0, None, "", "NaN", "Infinity", "-1", "0"):
            raw = payload()
            raw["data"]["p"] = value
            with self.subTest(value=value), self.assertRaises(FeedMessageError):
                parse_trade(json.dumps(raw), ("BTCUSDT",))

    def test_timestamps_and_identity_are_nonnegative_integers(self):
        for field in ("a", "T", "E"):
            for value in (True, -1, "123", 1.5, None):
                raw = payload()
                raw["data"][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(FeedMessageError):
                    parse_trade(json.dumps(raw), ("BTCUSDT",))
        raw = payload()
        raw["data"]["T"] = 10 ** 30
        with self.assertRaises(FeedMessageError):
            parse_trade(json.dumps(raw), ("BTCUSDT",))


class Socket:
    def __init__(self, messages=()):
        self.messages = asyncio.Queue()
        for message in messages:
            self.messages.put_nowait(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.messages.get()
        if value is None:
            raise StopAsyncIteration
        if isinstance(value, Exception):
            raise value
        return value


class Session:
    def __init__(self, socket=None, *, error=None, entered=None, block=None):
        self.socket = socket or Socket()
        self.error, self.entered, self.block = error, entered, block
        self.closed = False
        self.cancelled_during_open = False

    async def __aenter__(self):
        if self.entered is not None:
            self.entered.set()
        if self.error is not None:
            raise self.error
        if self.block is not None:
            try:
                await self.block.wait()
            except asyncio.CancelledError:
                self.cancelled_during_open = True
                raise
        return self.socket

    async def __aexit__(self, *args):
        self.closed = True


class Connector:
    def __init__(self, *sessions):
        self.sessions, self.calls = list(sessions), []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.sessions:
            raise AssertionError("unexpected reconnect")
        return self.sessions.pop(0)


class TradeSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_valid_trade_follows_history_handoff_and_socket_closes_on_stop(self):
        events, stop = [], asyncio.Event()
        session = Session(Socket((json.dumps(payload()),)))
        connector = Connector(session)

        async def prepare():
            events.append("prepared")

        async def receive(value):
            events.append(value)
            stop.set()

        supervisor = TradeSupervisor(("btcusdt",), receive, prepare, connector=connector)
        await asyncio.wait_for(supervisor.run(stop), 2)
        self.assertEqual(events[0], "prepared")
        self.assertEqual(events[1].symbol, "BTCUSDT")
        self.assertTrue(session.closed)
        self.assertEqual(supervisor.state, "stopped")
        self.assertIn("btcusdt@aggTrade", connector.calls[0][0])
        self.assertEqual(connector.calls[0][1]["open_timeout"], 10)

    async def test_malformed_price_is_fatal_instead_of_reconnecting(self):
        raw = payload()
        raw["data"]["p"] = "NaN"
        session = Session(Socket((json.dumps(raw),)))
        connector, receive = Connector(session), AsyncMock()
        supervisor = TradeSupervisor(("BTCUSDT",), receive, AsyncMock(), connector=connector)
        with self.assertRaises(FeedMessageError):
            await asyncio.wait_for(supervisor.run(asyncio.Event()), 2)
        receive.assert_not_awaited()
        self.assertTrue(session.closed)
        self.assertEqual(len(connector.calls), 1)

    async def test_callback_oserror_and_history_timeout_are_fatal(self):
        for preparing in (False, True):
            with self.subTest(preparing=preparing):
                session = Session(Socket((json.dumps(payload()),)))
                connector = Connector(session)
                receive = AsyncMock(side_effect=None if preparing else OSError("database failed"))
                prepare = AsyncMock(side_effect=TimeoutError("history worker failed") if preparing else None)
                supervisor = TradeSupervisor(("BTCUSDT",), receive, prepare, connector=connector)
                with self.assertRaises(FeedCallbackError) as raised:
                    await asyncio.wait_for(supervisor.run(asyncio.Event()), 2)
                self.assertIsInstance(raised.exception.__cause__, TimeoutError if preparing else OSError)
                self.assertTrue(session.closed)
                self.assertEqual(len(connector.calls), 1)

    async def test_network_failure_reconnects_then_checks_history_again(self):
        stop, prepare = asyncio.Event(), AsyncMock()
        first, second = Session(error=OSError("network unavailable")), Session(Socket((json.dumps(payload()),)))
        connector = Connector(first, second)

        async def receive(value):
            stop.set()

        supervisor = TradeSupervisor(("BTCUSDT",), receive, prepare, connector=connector)
        with patch("crypto_monitor.adapters.binance_trades.random.uniform", return_value=0):
            await asyncio.wait_for(supervisor.run(stop), 2)
        self.assertEqual(len(connector.calls), 2)
        prepare.assert_awaited_once()
        self.assertTrue(second.closed)
        self.assertIsNone(supervisor.error)

    async def test_normal_remote_close_reconnects(self):
        stop, prepare = asyncio.Event(), AsyncMock()
        first, second = Session(Socket((None,))), Session(Socket((json.dumps(payload()),)))
        connector = Connector(first, second)

        async def receive(value):
            stop.set()

        supervisor = TradeSupervisor(("BTCUSDT",), receive, prepare, connector=connector)
        with patch("crypto_monitor.adapters.binance_trades.random.uniform", return_value=0):
            await asyncio.wait_for(supervisor.run(stop), 2)
        self.assertEqual(prepare.await_count, 2)
        self.assertTrue(first.closed and second.closed)

    async def test_stop_interrupts_connection_setup(self):
        stop, entered = asyncio.Event(), asyncio.Event()
        session = Session(entered=entered, block=asyncio.Event())
        prepare = AsyncMock()
        supervisor = TradeSupervisor(("BTCUSDT",), AsyncMock(), prepare, connector=Connector(session))
        task = asyncio.create_task(supervisor.run(stop))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            stop.set()
            await asyncio.wait_for(task, 1)
            self.assertTrue(session.cancelled_during_open)
            prepare.assert_not_awaited()
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_stop_interrupts_history_handoff_and_closes_socket(self):
        stop, entered = asyncio.Event(), asyncio.Event()
        session = Session()

        async def prepare():
            entered.set()
            await asyncio.Event().wait()

        supervisor = TradeSupervisor(("BTCUSDT",), AsyncMock(), prepare, connector=Connector(session))
        task = asyncio.create_task(supervisor.run(stop))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            stop.set()
            await asyncio.wait_for(task, 1)
            self.assertTrue(session.closed)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_changed_symbols_reconnect_and_prepare_before_new_stream(self):
        stop, prepared = asyncio.Event(), asyncio.Queue()
        first, second = Session(), Session()
        connector = Connector(first, second)

        async def prepare():
            prepared.put_nowait(None)

        supervisor = TradeSupervisor(("BTCUSDT",), AsyncMock(), prepare, connector=connector)
        task = asyncio.create_task(supervisor.run(stop))
        try:
            await asyncio.wait_for(prepared.get(), 2)
            supervisor.update_symbols(("ETHUSDT",))
            await asyncio.wait_for(prepared.get(), 2)
            stop.set()
            await asyncio.wait_for(task, 1)
            self.assertEqual(len(connector.calls), 2)
            self.assertIn("ethusdt@aggTrade", connector.calls[1][0])
            self.assertTrue(first.closed and second.closed)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_already_stopped_does_not_connect(self):
        stop = asyncio.Event()
        stop.set()
        connector = Connector()
        supervisor = TradeSupervisor(("BTCUSDT",), AsyncMock(), AsyncMock(), connector=connector)
        await supervisor.run(stop)
        self.assertEqual(connector.calls, [])


if __name__ == "__main__":
    unittest.main()
