"""Daily history and level integration using only mocked HTTP responses."""

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

from crypto_monitor.adapters.binance_rest import (
    KLINES_URL,
    BinanceHTTPError,
    BinanceRESTAdapter,
    BinanceResponseError,
    BinanceTransportError,
)
from crypto_monitor.core.level_sweep import DAY, Candle, Levels, Period, calculate_levels


def utc(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def millis(value):
    return (value - utc("1970-01-01")) // timedelta(milliseconds=1)


def row(when, opening="100", high="110", low="90", close="105"):
    opened = utc(when) if isinstance(when, str) else when
    return [
        millis(opened), opening, high, low, close, "123.45",
        millis(opened + DAY) - 1, "12345.67", 10, "12.34", "1234.56", "0",
    ]


def history(start, end):
    return [row(start + i * DAY) for i in range((end - start).days)]


def response(payload):
    reply = MagicMock()
    reply.__enter__.return_value = reply
    reply.status = 200
    reply.read.return_value = json.dumps(payload).encode()
    return reply


class DailyKlineTests(unittest.TestCase):
    def setUp(self):
        patcher = patch("crypto_monitor.adapters.binance_rest.urlopen")
        self.opener = patcher.start()
        self.addCleanup(patcher.stop)
        self.adapter = BinanceRESTAdapter(timeout=4)

    def fetch(self, payload, start="2021-01-01", end="2021-01-02", as_of=None):
        self.opener.return_value = response(payload)
        return self.adapter.fetch_daily_klines(
            "BTCUSDT", start=utc(start), end=utc(end),
            as_of=as_of if as_of is not None else utc(end),
        )

    def query(self, index=-1):
        request = self.opener.call_args_list[index].args[0]
        return parse_qs(urlsplit(request.full_url).query)

    def test_parses_ohlc_with_exact_decimals_and_aware_utc(self):
        raw = row("2021-01-01", "100.00000000000000001", "110.123456789123456789",
                  "90.00000000000000001", "105.00000000000000001")
        self.assertEqual(raw[0], 1609459200000)
        with localcontext() as context:
            context.prec = 6
            candle = self.fetch([raw])[0]
        self.assertIsInstance(candle, Candle)
        self.assertEqual(candle.open_time, utc("2021-01-01"))
        self.assertIs(candle.open_time.tzinfo, timezone.utc)
        self.assertEqual(candle.interval, DAY)
        self.assertTrue(candle.closed)
        for name, index in [("open_price", 1), ("high", 2), ("low", 3), ("close", 4)]:
            self.assertIsInstance(getattr(candle, name), Decimal)
            self.assertEqual(getattr(candle, name), Decimal(raw[index]))

    def test_exact_request_boundaries_limit_timeout_and_public_get(self):
        self.fetch([row("2021-01-01")])
        request = self.opener.call_args.args[0]
        self.assertEqual(request.full_url.split("?")[0], KLINES_URL)
        self.assertEqual(self.query(), {
            "symbol": ["BTCUSDT"], "interval": ["1d"],
            "startTime": ["1609459200000"], "endTime": ["1609545599999"],
            "limit": ["1"],
        })
        self.assertEqual(request.get_method(), "GET")
        self.assertIsNone(request.data)
        self.assertEqual(request.header_items(), [("Accept", "application/json")])
        self.assertEqual(self.opener.call_args.kwargs, {"timeout": 4})
        self.opener.assert_called_once()

    def test_filters_start_inclusive_end_exclusive_and_sorts(self):
        candles = self.fetch(
            [row("2021-01-03"), row("2021-01-02"), row("2020-12-31"), row("2021-01-01")],
            end="2021-01-03",
        )
        self.assertEqual([c.open_time for c in candles], [utc("2021-01-01"), utc("2021-01-02")])

    def test_closed_only_at_next_midnight_not_final_millisecond(self):
        for as_of, count in [
            (utc("2021-01-01T12:00:00"), 0),
            (utc("2021-01-01T23:59:59.999000"), 0),
            (utc("2021-01-01T23:59:59.999999"), 0),
            (utc("2021-01-02"), 1),
        ]:
            with self.subTest(as_of=as_of):
                candles = self.fetch([row("2021-01-01")], as_of=as_of)
                self.assertEqual(len(candles), count)
                level = calculate_levels(Period(utc("2021-01-01"), utc("2021-01-02")), candles)
                self.assertEqual(level, Levels(Decimal("110"), Decimal("90")) if count else None)

    def test_excludes_current_and_future_daily_candles(self):
        candles = self.fetch(
            history(utc("2021-01-01"), utc("2021-01-04")), end="2021-01-04",
            as_of=utc("2021-01-02T12:00:00"),
        )
        self.assertEqual([c.open_time for c in candles], [utc("2021-01-01")])

    def test_preserves_duplicates_for_unavailable_level(self):
        candles = self.fetch([row("2021-01-01"), row("2021-01-01")])
        self.assertEqual(len(candles), 2)
        self.assertIsNone(calculate_levels(
            Period(utc("2021-01-01"), utc("2021-01-02")), candles
        ))

    def test_invalid_utc_inputs_fail_before_http(self):
        valid = dict(start=utc("2021-01-01"), end=utc("2021-01-02"), as_of=utc("2021-01-02"))
        for field in valid:
            for invalid in [valid[field].replace(tzinfo=None),
                            valid[field].astimezone(timezone(timedelta(hours=3)))]:
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    self.adapter.fetch_daily_klines("BTCUSDT", **{**valid, field: invalid})
        for changes in [{"end": valid["start"]}, {"start": valid["end"]},
                        {"start": utc("2021-01-01T01:00:00")},
                        {"end": utc("2021-01-02T01:00:00")}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.adapter.fetch_daily_klines("BTCUSDT", **{**valid, **changes})
        self.opener.assert_not_called()

    def test_malformed_payloads_and_row_lengths(self):
        for payload in [None, {}, {"code": -1121}, [None], [{}], [row("2021-01-01")[:-1]],
                        [row("2021-01-01") + ["extra"]]]:
            with self.subTest(payload=payload), self.assertRaisesRegex(BinanceResponseError, "klines"):
                self.fetch(payload)

    def test_malformed_ohlc_and_auxiliary_fields(self):
        for index in [1, 2, 3, 4, 5, 7, 9, 10]:
            for bad in [None, "bad", "NaN", "Infinity", "-1", 1.2, True]:
                raw = row("2021-01-01")
                raw[index] = bad
                with self.subTest(index=index, bad=bad), self.assertRaises(BinanceResponseError):
                    self.fetch([raw])
        for index, bad in [(1, "111"), (4, "89"), (2, "80"), (11, None)]:
            raw = row("2021-01-01")
            raw[index] = bad
            with self.subTest(index=index, bad=bad), self.assertRaises(BinanceResponseError):
                self.fetch([raw])

    def test_malformed_timestamps_and_daily_alignment(self):
        cases = [(index, bad) for index in (0, 6, 8) for bad in (True, "1", 1.5, -1)]
        cases += [(0, 10**30), (0, 1609459200001), (6, 1609545600000)]
        for index, bad in cases:
            raw = row("2021-01-01")
            raw[index] = bad
            with self.subTest(index=index, bad=bad), self.assertRaises(BinanceResponseError):
                self.fetch([raw])

    def test_weight_tier_limits_and_pagination(self):
        for days, expected_limits in [(99, [99]), (100, [100]), (499, [499]),
                                      (500, [499, 1]), (1501, [499, 499, 499, 4])]:
            with self.subTest(days=days):
                self.opener.reset_mock()
                # Empty responses never cause premature termination or retries.
                self.opener.return_value = response([])
                start, end = utc("2020-01-01"), utc("2020-01-01") + days * DAY
                self.assertEqual(self.adapter.fetch_daily_klines(
                    "BTCUSDT", start=start, end=end, as_of=end,
                ), [])
                cursor = start
                self.assertEqual(self.opener.call_count, len(expected_limits))
                for index, limit in enumerate(expected_limits):
                    query = self.query(index)
                    self.assertEqual(query["startTime"], [str(millis(cursor))])
                    cursor += limit * DAY
                    self.assertEqual(query["endTime"], [str(millis(cursor) - 1)])
                    self.assertEqual(query["limit"], [str(limit)])
                self.assertEqual(cursor, end)

    def test_paginated_history_has_no_artificial_boundary_duplicates(self):
        start = utc("2020-01-01")
        boundary, end = start + 499 * DAY, start + 500 * DAY
        self.opener.side_effect = [response(history(start, boundary)), response([row(boundary)])]
        candles = self.adapter.fetch_daily_klines("BTCUSDT", start=start, end=end, as_of=end)
        self.assertEqual(len(candles), 500)
        self.assertEqual(candles[-1].open_time, boundary)
        self.assertIsNotNone(calculate_levels(Period(start, end), candles))

    def test_later_page_failure_does_not_return_partial_history(self):
        start, end = utc("2020-01-01"), utc("2020-01-01") + 500 * DAY
        for failure, error in [
            (TimeoutError(), BinanceTransportError),
            (response({"code": -1}), BinanceResponseError),
        ]:
            with self.subTest(failure=failure):
                self.opener.reset_mock()
                self.opener.side_effect = [response([row(start)]), failure]
                with self.assertRaises(error):
                    self.adapter.fetch_daily_klines("BTCUSDT", start=start, end=end, as_of=end)
                self.assertEqual(self.opener.call_count, 2)

    def test_http_and_invalid_json_errors_are_preserved(self):
        reply = response({})
        reply.status = 429
        self.opener.return_value = reply
        with self.assertRaises(BinanceHTTPError):
            self.adapter.fetch_reference_levels("BTCUSDT", as_of=utc("2026-10-09"))
        self.opener.assert_called_once()
        self.opener.reset_mock()
        reply.status = 200
        reply.read.return_value = b"invalid json"
        with self.assertRaises(BinanceResponseError):
            self.adapter.fetch_reference_levels("BTCUSDT", as_of=utc("2026-10-09"))
        self.opener.assert_called_once()


class ReferenceLevelTests(unittest.TestCase):
    def setUp(self):
        patcher = patch("crypto_monitor.adapters.binance_rest.urlopen")
        self.opener = patcher.start()
        self.addCleanup(patcher.stop)
        self.adapter = BinanceRESTAdapter()
        self.as_of = utc("2026-10-09T12:00:00")
        self.rows = history(utc("2026-07-01"), utc("2026-10-06"))
        specials = {
            millis(utc("2026-07-01")): ("140", "60"),
            millis(utc("2026-09-10")): ("130", "70"),
            millis(utc("2026-10-01")): ("120", "80"),
            millis(utc("2026-10-05")): ("115", "85"),
        }
        for raw in self.rows:
            if raw[0] in specials:
                raw[2], raw[3] = specials[raw[0]]

    def fetch(self, rows):
        self.opener.return_value = response(rows)
        return self.adapter.fetch_reference_levels("BTCUSDT", as_of=self.as_of)

    def test_all_four_reference_levels_with_one_minimal_range(self):
        result = self.fetch(self.rows)
        self.assertEqual(result, {
            "monday": Levels(Decimal("115"), Decimal("85")),
            "previous_week": Levels(Decimal("120"), Decimal("80")),
            "previous_month": Levels(Decimal("130"), Decimal("70")),
            "previous_quarter": Levels(Decimal("140"), Decimal("60")),
        })
        self.opener.assert_called_once()
        query = parse_qs(urlsplit(self.opener.call_args.args[0].full_url).query)
        self.assertEqual(query["startTime"], [str(millis(utc("2026-07-01")))])
        self.assertEqual(query["endTime"], [str(millis(utc("2026-10-06")) - 1)])
        self.assertEqual(query["limit"], ["97"])

    def test_missing_day_only_invalidates_affected_periods(self):
        result = self.fetch([r for r in self.rows if r[0] != millis(utc("2026-09-10"))])
        self.assertIsNone(result["previous_month"])
        self.assertIsNone(result["previous_quarter"])
        self.assertIsNotNone(result["previous_week"])
        self.assertIsNotNone(result["monday"])

    def test_duplicate_day_only_invalidates_affected_periods(self):
        result = self.fetch(self.rows + [self.rows[-1]])
        self.assertIsNone(result["monday"])
        for name in ("previous_week", "previous_month", "previous_quarter"):
            self.assertIsNotNone(result[name])

    def test_empty_and_new_listing_histories(self):
        self.assertTrue(all(level is None for level in self.fetch([]).values()))
        result = self.fetch([self.rows[-1]])
        self.assertEqual(result["monday"], Levels(Decimal("115"), Decimal("85")))
        for name in ("previous_week", "previous_month", "previous_quarter"):
            self.assertIsNone(result[name])

    def test_monday_activation_expiry_and_calendar_rollover_ranges(self):
        for as_of, start, end, monday_active in [
            ("2026-10-05T23:59:59.999999", "2026-07-01", "2026-10-05", False),
            ("2026-10-06", "2026-07-01", "2026-10-06", True),
            ("2026-10-12", "2026-07-01", "2026-10-12", False),
            ("2026-01-01", "2025-10-01", "2026-01-01", True),
            ("2026-04-01", "2026-01-01", "2026-04-01", True),
            ("2026-07-01", "2026-04-01", "2026-07-01", True),
            ("2026-11-10", "2026-07-01", "2026-11-10", True),
        ]:
            with self.subTest(as_of=as_of):
                self.opener.reset_mock()
                self.opener.return_value = response(history(utc(start), utc(end)))
                result = self.adapter.fetch_reference_levels("BTCUSDT", as_of=utc(as_of))
                self.assertEqual(result["monday"] is not None, monday_active)
                self.opener.assert_called_once()
                query = parse_qs(urlsplit(self.opener.call_args.args[0].full_url).query)
                self.assertEqual(query["startTime"], [str(millis(utc(start)))])
                self.assertEqual(query["endTime"], [str(millis(utc(end)) - 1)])
                self.assertEqual(query["limit"], [str((utc(end) - utc(start)).days)])


if __name__ == "__main__":
    unittest.main()
