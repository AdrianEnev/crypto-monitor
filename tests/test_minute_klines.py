"""Mocked one-minute REST parsing, completeness and pagination tests."""

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlsplit

from crypto_monitor.adapters.binance_rest import (
    BinanceRESTAdapter, BinanceResponseError, BinanceTransportError,
    UnresolvedGapError, parse_minute_klines,
)
from crypto_monitor.core.level_sweep import MINUTE


START = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MS = timedelta(milliseconds=1)


def millis(value):
    return (value - EPOCH) // MS


def row(opened=START, close="99"):
    return [millis(opened), "99", "101", "98", close, "10",
            millis(opened + MINUTE) - 1, "1000", 3, "5", "500", "0"]


def response(payload):
    result = MagicMock()
    result.__enter__.return_value = result
    result.status = 200
    result.read.return_value = json.dumps(payload).encode()
    return result


def query(request):
    return {key: values[0] for key, values in parse_qs(urlsplit(request.full_url).query).items()}


def window_response(request, *, timeout):
    params = query(request)
    start = EPOCH + int(params["startTime"]) * MS
    count = int(params["limit"])
    return response([row(start + i * MINUTE) for i in range(count)])


class MinuteKlineTests(unittest.TestCase):
    def setUp(self):
        self.opener = MagicMock(side_effect=window_response)
        self.rest = BinanceRESTAdapter(opener=self.opener, timeout=4)

    def fetch(self, count=1, **kwargs):
        return list(self.rest.iter_minute_klines(
            "BTCUSDT", start=START, end=START + count * MINUTE,
            as_of=kwargs.get("as_of", START + count * MINUTE),
        ))

    def test_decimal_utc_and_synthetic_event_time(self):
        raw = row(close="99.12345678901234567890123456789")
        self.opener.side_effect = None
        self.opener.return_value = response([raw])
        with localcontext() as context:
            context.prec = 6
            result = self.fetch()[0]
        self.assertEqual(result.candle.close, Decimal(raw[4]))
        self.assertEqual(result.candle.open_time, START)
        self.assertIs(result.candle.open_time.tzinfo, timezone.utc)
        self.assertEqual(result.close_time, START + MINUTE - MS)
        self.assertEqual(result.event_time, START + MINUTE)
        self.assertTrue(result.candle.closed)

    def test_pagination_exact_inclusive_end_and_weight_tiers(self):
        candles = self.fetch(1001)
        self.assertEqual(len(candles), 1001)
        self.assertEqual([c.candle.open_time for c in candles],
                         [START + i * MINUTE for i in range(1001)])
        requests = [query(call.args[0]) for call in self.opener.call_args_list]
        self.assertEqual([p["limit"] for p in requests], ["499", "499", "3"])
        self.assertEqual([p["interval"] for p in requests], ["1m"] * 3)
        self.assertEqual(int(requests[0]["startTime"]), millis(START))
        self.assertEqual(int(requests[-1]["endTime"]), millis(START + 1001 * MINUTE) - 1)
        for before, after in zip(requests, requests[1:]):
            self.assertEqual(int(before["endTime"]) + 1, int(after["startTime"]))
        for call in self.opener.call_args_list:
            self.assertEqual(call.kwargs, {"timeout": 4})
            self.assertEqual(call.args[0].get_method(), "GET")
            self.assertEqual(call.args[0].headers, {"Accept": "application/json"})

    def test_short_windows_use_exact_limits(self):
        for count, limits in [(1, [1]), (99, [99]), (100, [100]), (499, [499]), (500, [499, 1])]:
            with self.subTest(count=count):
                self.opener.reset_mock()
                self.fetch(count)
                self.assertEqual([int(query(call.args[0])["limit"])
                                  for call in self.opener.call_args_list], limits)

    def test_unclosed_is_ignored_and_incomplete_coverage_is_explicit(self):
        cutoff = START + MINUTE - MS
        parsed = parse_minute_klines([row()], symbol="BTCUSDT", start=START,
                                    end=START + MINUTE, as_of=cutoff)
        self.assertEqual(parsed, [])
        with self.assertRaises(UnresolvedGapError) as caught:
            self.fetch(as_of=cutoff)
        self.assertEqual(caught.exception.start, START)

    def test_missing_minutes_do_not_yield_a_partial_page(self):
        for payload in ([], [row()], [row(START + MINUTE)], [row(), row(START + 2 * MINUTE)]):
            with self.subTest(payload=payload):
                self.opener.side_effect = None
                self.opener.return_value = response(payload)
                iterator = self.rest.iter_minute_klines(
                    "BTCUSDT", start=START, end=START + 3 * MINUTE, as_of=START + 3 * MINUTE)
                with self.assertRaises(UnresolvedGapError):
                    next(iterator)

    def test_rejects_malformed_duplicate_out_of_order_and_out_of_range_rows(self):
        invalid = [{}, [row()[:-1]], [row(), row()], [row(START + MINUTE), row()],
                   [row(START - MINUTE)], [row(START + 2 * MINUTE)]]
        for index, value in [(0, True), (0, 10**30), (0, millis(START) + 1),
                             (6, millis(START + MINUTE)), (8, -1), (1, 99.0),
                             (2, "NaN"), (3, "102"), (4, "Infinity"),
                             (5, "-1"), (7, None), (11, 0)]:
            bad = row()
            bad[index] = value
            invalid.append([bad])
        for payload in invalid:
            with self.subTest(payload=payload), self.assertRaises(BinanceResponseError):
                parse_minute_klines(payload, symbol="BTCUSDT", start=START,
                                    end=START + 2 * MINUTE, as_of=START + 2 * MINUTE)

    def test_bad_second_page_preserves_only_prior_page_yields(self):
        def fail_second(request, *, timeout):
            if len(self.opener.call_args_list) == 2:
                raise TimeoutError()
            return window_response(request, timeout=timeout)

        self.opener.side_effect = fail_second
        collected = []
        with self.assertRaises(BinanceTransportError):
            for item in self.rest.iter_minute_klines(
                "BTCUSDT", start=START, end=START + 500 * MINUTE, as_of=START + 500 * MINUTE
            ):
                collected.append(item)
        self.assertEqual(len(collected), 499)

    def test_invalid_boundaries_never_make_request(self):
        for start, end in [(START, START), (START.replace(tzinfo=None), START + MINUTE),
                           (START + MS, START + MINUTE)]:
            with self.subTest(start=start), self.assertRaises(ValueError):
                list(self.rest.iter_minute_klines("BTCUSDT", start=start, end=end, as_of=START))
        self.opener.assert_not_called()


if __name__ == "__main__":
    unittest.main()
