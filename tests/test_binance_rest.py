"""Mocked discovery tests: no live HTTP requests or credentials required."""

import json
import unittest
from decimal import Decimal, localcontext
from http.client import IncompleteRead
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

from crypto_monitor.adapters.binance_rest import (
    EXCHANGE_INFO_URL,
    TICKER_URL,
    BinanceHTTPError,
    BinanceRESTAdapter,
    BinanceResponseError,
    BinanceTransportError,
    DiscoveredMarket,
)
from crypto_monitor.core.market_universe import select_markets


def contract(base="BTC", tick="0.10", **overrides):
    return {
        "symbol": f"{base}USDT", "baseAsset": base,
        "quoteAsset": "USDT", "marginAsset": "USDT",
        "status": "TRADING", "contractType": "PERPETUAL",
        "pricePrecision": 8,  # Intentionally unrelated to the tick size.
        "filters": [
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            {"filterType": "PRICE_FILTER", "tickSize": tick},
        ],
        **overrides,
    }


def ticker(symbol="BTCUSDT", volume="1000.01"):
    return {"symbol": symbol, "quoteVolume": volume}


def raw_response(body, status=200):
    result = MagicMock()
    result.__enter__.return_value = result
    result.status = status
    result.read.return_value = body
    return result


def response(payload, status=200):
    return raw_response(json.dumps(payload).encode("utf-8"), status)


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        # Even an accidentally uninjected adapter cannot reach the network.
        patcher = patch("crypto_monitor.adapters.binance_rest.urlopen")
        self.opener = patcher.start()
        self.addCleanup(patcher.stop)
        self.exchange = {"symbols": [contract()]}
        self.tickers = [ticker()]
        self.configure(self.exchange, self.tickers)

    def configure(self, exchange, tickers):
        self.opener.reset_mock()
        self.opener.side_effect = [response(exchange), response(tickers)]

    def test_valid_result_tick_filter_and_existing_selector(self):
        with patch("crypto_monitor.adapters.binance_rest.select_markets", wraps=select_markets) as selector:
            result = BinanceRESTAdapter().discover_markets()
        self.assertEqual(result, [DiscoveredMarket(
            "BTCUSDT", "BTC", Decimal("1000.01"), Decimal("0.10")
        )])
        selector.assert_called_once_with(
            self.exchange, self.tickers, top_n=20, include_symbols=(),
            exclude_symbols=set(),
            stablecoin_bases=selector.call_args.kwargs["stablecoin_bases"],
        )
        self.assertIsInstance(result[0].tick_size, Decimal)
        self.assertIsInstance(result[0].quote_volume, Decimal)

    def test_public_get_urls_timeout_and_no_authentication(self):
        BinanceRESTAdapter(timeout=3.5).discover_markets()
        self.assertEqual(self.opener.call_count, 2)
        for call, url in zip(self.opener.call_args_list, [EXCHANGE_INFO_URL, TICKER_URL]):
            request = call.args[0]
            self.assertEqual(request.full_url, url)
            self.assertEqual(request.get_method(), "GET")
            self.assertIsNone(request.data)
            self.assertEqual(dict(request.header_items()), {"Accept": "application/json"})
            self.assertEqual(call.kwargs, {"timeout": 3.5})

    def test_injected_transport(self):
        transport = MagicMock(side_effect=[response(self.exchange), response(self.tickers)])
        self.assertEqual(len(BinanceRESTAdapter(opener=transport).discover_markets()), 1)
        self.opener.assert_not_called()
        self.assertEqual(transport.call_count, 2)

    def test_missing_tick_size_has_no_price_precision_fallback(self):
        del self.exchange["symbols"][0]["filters"][1]["tickSize"]
        self.configure(self.exchange, self.tickers)
        with self.assertRaisesRegex(BinanceResponseError, "BTCUSDT.*tickSize"):
            BinanceRESTAdapter().discover_markets()

    def test_invalid_or_missing_price_filter(self):
        for filters in [
            None, {}, [], [None], [{}],
            [{"filterType": "LOT_SIZE", "tickSize": "0.01"}],
            [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}] * 2,
        ]:
            with self.subTest(filters=filters):
                self.configure({"symbols": [contract(filters=filters)]}, self.tickers)
                with self.assertRaisesRegex(BinanceResponseError, "PRICE_FILTER"):
                    BinanceRESTAdapter().discover_markets()

    def test_invalid_tick_values(self):
        for value in [None, "", "bad", "NaN", "Infinity", "0", "-0.01", 0.1, True]:
            with self.subTest(value=value):
                self.configure({"symbols": [contract(tick=value)]}, self.tickers)
                with self.assertRaisesRegex(BinanceResponseError, "tickSize"):
                    BinanceRESTAdapter().discover_markets()

    def test_eligible_below_cutoff_still_requires_tick_size(self):
        self.configure(
            {"symbols": [contract(), contract("ETH", filters=[])]},
            [ticker(), ticker("ETHUSDT", "1")],
        )
        with self.assertRaisesRegex(BinanceResponseError, "ETHUSDT.*tickSize"):
            BinanceRESTAdapter().discover_markets(top_n=1)

    def test_missing_eligible_ticker_rejects_partial_snapshot(self):
        self.configure({"symbols": [contract(), contract("ETH")]}, self.tickers)
        with self.assertRaisesRegex(BinanceResponseError, "missing eligible symbol ETHUSDT"):
            BinanceRESTAdapter().discover_markets(top_n=1)

    def test_ineligible_and_excluded_symbols_need_no_tick_or_ticker(self):
        rows = [contract(), contract("ETH", filters=[]), contract("USDC", filters=[])]
        for base, overrides in [
            ("SOL", {"status": "SETTLING"}),
            ("BNB", {"contractType": "CURRENT_QUARTER"}),
            ("XRP", {"quoteAsset": "USDC"}),
            ("ADA", {"marginAsset": "USDC"}),
        ]:
            rows.append(contract(base, filters=[], **overrides))
        self.configure({"symbols": rows}, self.tickers)
        result = BinanceRESTAdapter().discover_markets(
            include_symbols=[row["symbol"] for row in rows], exclude_symbols=[" ethusdt "],
        )
        self.assertEqual([m.symbol for m in result], ["BTCUSDT"])

    def test_default_top20_inclusions_exclusions_and_ties(self):
        rows = [contract(f"COIN{i:02}") for i in range(25)]
        ticks = [ticker(row["symbol"], str(i)) for i, row in enumerate(rows)]
        self.configure({"symbols": rows[::-1]}, ticks)
        result = BinanceRESTAdapter().discover_markets(
            include_symbols=["coin00usdt", "coin24usdt"],
            exclude_symbols=["coin24usdt"],
        )
        self.assertEqual(
            [m.symbol for m in result],
            [f"COIN{i:02}USDT" for i in range(23, 3, -1)] + ["COIN00USDT"],
        )
        self.configure(
            {"symbols": [contract("ETH"), contract()]},
            [ticker("ETHUSDT", "100"), ticker("BTCUSDT", "100")],
        )
        self.assertEqual(
            [m.symbol for m in BinanceRESTAdapter().discover_markets()],
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_custom_stablecoin_policy_and_generator_options(self):
        self.configure(
            {"symbols": [contract(), contract("NEWUSD", filters=[])]}, self.tickers
        )
        result = BinanceRESTAdapter().discover_markets(
            top_n=0, include_symbols=iter(["btcusdt"]),
            stablecoin_bases=iter(["newusd"]), exclude_symbols=iter([]),
        )
        self.assertEqual([m.symbol for m in result], ["BTCUSDT"])

    def test_decimal_values_remain_exact(self):
        volume = "100000000000000000000.00000001"
        tick = "0.00000000000000000001"
        self.configure({"symbols": [contract(tick=tick)]}, [ticker(volume=volume)])
        with localcontext() as context:
            context.prec = 6
            result = BinanceRESTAdapter().discover_markets()[0]
        self.assertEqual(result.quote_volume, Decimal(volume))
        self.assertEqual(result.tick_size, Decimal(tick))

    def test_malformed_exchange_payloads(self):
        malformed = [None, [], {}, {"code": -1, "msg": "failure"},
                     {"symbols": None}, {"symbols": {}}, {"symbols": []},
                     {"symbols": [None]}, {"symbols": [{}]},
                     {"symbols": [contract(), contract()]}]
        for field in ["symbol", "baseAsset", "quoteAsset", "marginAsset",
                      "contractType", "status"]:
            row = contract()
            del row[field]
            malformed.append({"symbols": [row]})
            malformed.append({"symbols": [contract(**{field: []})]})
        for payload in malformed:
            with self.subTest(payload=payload):
                self.configure(payload, self.tickers)
                with self.assertRaisesRegex(BinanceResponseError, "exchangeInfo"):
                    BinanceRESTAdapter().discover_markets()
                self.assertEqual(self.opener.call_count, 1)

    def test_malformed_ticker_payloads(self):
        for payload in [None, {}, ticker(), [], [None], [{}], [ticker(), ticker()],
                        [{"symbol": "BTCUSDT"}], [ticker(volume=10.5)],
                        [ticker(volume="NaN")], [ticker(volume="-1")],
                        [ticker(volume="bad")]]:
            with self.subTest(payload=payload):
                self.configure(self.exchange, payload)
                with self.assertRaisesRegex(BinanceResponseError, "ticker/24hr"):
                    BinanceRESTAdapter().discover_markets()

    def test_empty_selection_is_an_explicit_error(self):
        with self.assertRaisesRegex(BinanceResponseError, "selected no markets"):
            BinanceRESTAdapter().discover_markets(exclude_symbols=["BTCUSDT"])

    def test_invalid_json_at_either_endpoint(self):
        for endpoint in [0, 1]:
            for body in [b"not json", b'{"symbols":', b"\xff", b"NaN"]:
                with self.subTest(endpoint=endpoint, body=body):
                    replies = [response(self.exchange), response(self.tickers)]
                    replies[endpoint] = raw_response(body)
                    self.opener.side_effect = replies
                    with self.assertRaisesRegex(BinanceResponseError, "invalid JSON"):
                        BinanceRESTAdapter().discover_markets()

    def test_http_errors_at_either_endpoint_without_retry(self):
        for endpoint, url in enumerate([EXCHANGE_INFO_URL, TICKER_URL]):
            for status in [403, 418, 429, 500, 503]:
                with self.subTest(endpoint=endpoint, status=status):
                    self.opener.reset_mock()
                    replies = [response(self.exchange), response(self.tickers)]
                    replies[endpoint] = HTTPError(url, status, "failure", {}, None)
                    self.opener.side_effect = replies
                    with self.assertRaises(BinanceHTTPError) as caught:
                        BinanceRESTAdapter().discover_markets()
                    self.assertEqual(caught.exception.status, status)
                    self.assertEqual(caught.exception.url, url)
                    self.assertEqual(self.opener.call_count, endpoint + 1)

    def test_non_success_status_returned_by_custom_transport(self):
        self.opener.side_effect = [raw_response(b"bad gateway", 502)]
        with self.assertRaisesRegex(BinanceHTTPError, "HTTP 502"):
            BinanceRESTAdapter().discover_markets()
        self.assertEqual(self.opener.call_count, 1)

    def test_timeouts_at_either_endpoint_without_retry(self):
        for endpoint in [0, 1]:
            for error in [TimeoutError("slow"), URLError(TimeoutError("slow"))]:
                with self.subTest(endpoint=endpoint, error=error):
                    self.opener.reset_mock()
                    replies = [response(self.exchange), response(self.tickers)]
                    replies[endpoint] = error
                    self.opener.side_effect = replies
                    with self.assertRaisesRegex(BinanceTransportError, "timed out"):
                        BinanceRESTAdapter().discover_markets()
                    self.assertEqual(self.opener.call_count, endpoint + 1)

    def test_connection_and_read_failures(self):
        self.opener.side_effect = URLError("DNS failure")
        with self.assertRaisesRegex(BinanceTransportError, "DNS failure"):
            BinanceRESTAdapter().discover_markets()
        for error in [TimeoutError(), ConnectionResetError(), IncompleteRead(b"{")]:
            with self.subTest(error=error):
                reply = response(self.exchange)
                reply.read.side_effect = error
                self.opener.side_effect = [reply]
                with self.assertRaises(BinanceTransportError):
                    BinanceRESTAdapter().discover_markets()
                reply.__exit__.assert_called_once()

    def test_invalid_timeout_and_limit_rejected_before_network(self):
        for timeout in [0, -1, float("inf"), float("nan"), None, True]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                BinanceRESTAdapter(timeout=timeout)
        for limit in [-1, 1.5, True]:
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                BinanceRESTAdapter().discover_markets(top_n=limit)
        self.opener.assert_not_called()


if __name__ == "__main__":
    unittest.main()
