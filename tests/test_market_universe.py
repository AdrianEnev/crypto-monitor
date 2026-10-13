"""Synthetic Binance-shaped snapshots; these tests never call Binance."""

import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext

from crypto_monitor.core.market_universe import (
    DEFAULT_STABLECOIN_BASES,
    Alert,
    rank_alerts,
    select_markets,
)


D = Decimal
NOW = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)


def contract(base="BTC", **overrides):
    return {
        "symbol": f"{base}USDT",
        "baseAsset": base,
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        **overrides,
    }


def ticker(symbol="BTCUSDT", quote_volume="1000", **overrides):
    return {"symbol": symbol, "quoteVolume": quote_volume, **overrides}


def names(markets):
    return [market.symbol for market in markets]


class UniverseTests(unittest.TestCase):
    def test_eligible_market_and_decimal_volume(self):
        result = select_markets({"symbols": [contract()]}, [ticker()])
        self.assertEqual(names(result), ["BTCUSDT"])
        self.assertEqual(result[0].base_asset, "BTC")
        self.assertIsInstance(result[0].quote_volume, Decimal)
        self.assertEqual(result[0].quote_volume, D("1000"))

    def test_inclusions_cannot_override_eligibility(self):
        for field, value in [
            ("status", "PENDING_TRADING"),
            ("status", "SETTLING"),
            ("contractType", "CURRENT_QUARTER"),
            ("contractType", "NEXT_QUARTER"),
            ("contractType", "TRADIFI_PERPETUAL"),
            ("quoteAsset", "USDC"),
            ("marginAsset", "BTC"),
            ("marginAsset", "USDC"),
        ]:
            with self.subTest(field=field, value=value):
                result = select_markets(
                    {"symbols": [contract(**{field: value})]}, [ticker()],
                    include_symbols=["BTCUSDT"],
                )
                self.assertEqual(result, [])

    def test_missing_eligibility_fields_fail_closed(self):
        for field in [
            "status", "contractType", "quoteAsset", "marginAsset", "baseAsset",
        ]:
            row = contract()
            del row[field]
            with self.subTest(field=field):
                self.assertEqual(
                    select_markets({"symbols": [row]}, [ticker()]), []
                )

    def test_stablecoin_bases_and_inclusions(self):
        for base in DEFAULT_STABLECOIN_BASES:
            row = contract(base)
            with self.subTest(base=base):
                self.assertEqual(select_markets(
                    {"symbols": [row]}, [ticker(row["symbol"])],
                    include_symbols=[row["symbol"]],
                ), [])

    def test_stablecoin_filter_uses_base_asset_and_is_configurable(self):
        data = {"symbols": [contract("NEWUSD", symbol="ALIASUSDT")]}
        ticks = [ticker("ALIASUSDT")]
        self.assertEqual(names(select_markets(data, ticks)), ["ALIASUSDT"])
        self.assertEqual(select_markets(
            data, ticks,
            stablecoin_bases=DEFAULT_STABLECOIN_BASES | {"NEWUSD"},
        ), [])

    def test_default_top_20_and_additive_inclusions(self):
        rows = [contract(f"COIN{i:02}") for i in range(25)]
        ticks = [ticker(row["symbol"], str(i)) for i, row in enumerate(rows)]
        data = {"symbols": rows}
        expected = [f"COIN{i:02}USDT" for i in range(24, 4, -1)]
        self.assertEqual(names(select_markets(data, ticks)), expected)
        self.assertEqual(names(select_markets(
            data, ticks,
            include_symbols=["COIN00USDT", "COIN24USDT", "COIN00USDT", "UNKNOWN"],
        )), expected + ["COIN00USDT"])

    def test_exclusion_wins_and_selection_backfills(self):
        data = {"symbols": [contract(base) for base in ["BTC", "ETH", "SOL"]]}
        ticks = [ticker("BTCUSDT", "300"), ticker("ETHUSDT", "200"),
                 ticker("SOLUSDT", "100")]
        self.assertEqual(names(select_markets(
            data, ticks, top_n=2,
            include_symbols=[" btcusdt "], exclude_symbols=["btcusdt"],
        )), ["ETHUSDT", "SOLUSDT"])

    def test_numeric_quote_volume_not_base_volume_or_string_order(self):
        data = {"symbols": [contract("BTC"), contract("ETH")]}
        ticks = [ticker("BTCUSDT", "9", volume="9999"),
                 ticker("ETHUSDT", "10", volume="1")]
        self.assertEqual(names(select_markets(data, ticks)), ["ETHUSDT", "BTCUSDT"])

    def test_market_ties_are_symbol_ascending_at_cutoff(self):
        rows = [contract(base) for base in ["SOL", "ETH", "BTC"]]
        ticks = [ticker(row["symbol"], "100.00") for row in rows]
        for snapshot, volumes in [(rows, ticks), (rows[::-1], ticks[::-1])]:
            with self.subTest(snapshot=snapshot):
                self.assertEqual(names(select_markets(
                    {"symbols": snapshot}, volumes, top_n=2,
                    include_symbols=[" solusdt "],
                )), ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
                self.assertEqual(names(select_markets(
                    {"symbols": snapshot}, volumes, top_n=2,
                )), ["BTCUSDT", "ETHUSDT"])

    def test_decimal_precision_is_not_lost_during_sorting(self):
        data = {"symbols": [contract("BTC"), contract("ETH")]}
        ticks = [ticker("BTCUSDT", "100000000000000000000.00000001"),
                 ticker("ETHUSDT", "100000000000000000000.00000002")]
        with localcontext() as context:
            context.prec = 6
            self.assertEqual(
                names(select_markets(data, ticks)), ["ETHUSDT", "BTCUSDT"]
            )

    def test_missing_ticker_is_omitted_even_if_included(self):
        self.assertEqual(select_markets(
            {"symbols": [contract()]}, [ticker("UNKNOWN")],
            include_symbols=["BTCUSDT"],
        ), [])

    def test_invalid_volume_is_rejected(self):
        for volume in [None, "bad", "NaN", "Infinity", "-1", 1.2, True]:
            with self.subTest(volume=volume), self.assertRaises(ValueError):
                select_markets(
                    {"symbols": [contract()]}, [ticker(quote_volume=volume)]
                )

    def test_duplicate_snapshot_symbols_are_rejected(self):
        for rows, ticks in [([contract(), contract()], [ticker()]),
                            ([contract()], [ticker(), ticker()])]:
            with self.subTest(rows=rows, ticks=ticks), self.assertRaises(ValueError):
                select_markets({"symbols": rows}, ticks)

    def test_zero_limit_empty_input_and_invalid_limits(self):
        data, ticks = {"symbols": [contract()]}, [ticker(quote_volume="0")]
        self.assertEqual(select_markets(data, ticks, top_n=0), [])
        self.assertEqual(names(select_markets(
            data, ticks, top_n=0, include_symbols=["BTCUSDT"],
        )), ["BTCUSDT"])
        self.assertEqual(select_markets({"symbols": []}, []), [])
        for limit in [-1, 1.5, True]:
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                select_markets(data, ticks, top_n=limit)

    def test_input_snapshots_are_unchanged(self):
        data, ticks = {"symbols": [contract()]}, [ticker()]
        original = deepcopy((data, ticks))
        select_markets(data, ticks)
        self.assertEqual((data, ticks), original)


class AlertRankingTests(unittest.TestCase):
    def test_volume_first_then_newest_then_symbol_and_id(self):
        alerts = [
            Alert("SOLUSDT", D("9"), NOW, "sol"),
            Alert("ETHUSDT", D("10"), NOW, "eth"),
            Alert("BTCUSDT", D("10"), NOW, "b"),
            Alert("BTCUSDT", D("10"), NOW, "a"),
            Alert("BTCUSDT", D("10"), NOW - timedelta(seconds=1), "old"),
            Alert("BTCUSDT", D("100"), NOW - timedelta(days=1), "big"),
        ]
        expected = ["big", "a", "b", "eth", "old", "sol"]
        for source in [alerts, alerts[::-1]]:
            with self.subTest(source=source):
                self.assertEqual([a.alert_id for a in rank_alerts(source)], expected)
        self.assertEqual(alerts[0].alert_id, "sol")

    def test_captured_volume_is_independent_of_later_ticker_changes(self):
        ticks = [ticker("BTCUSDT", "200"), ticker("ETHUSDT", "100")]
        markets = select_markets(
            {"symbols": [contract("BTC"), contract("ETH")]}, ticks
        )
        alerts = [Alert(m.symbol, m.quote_volume, NOW, m.symbol) for m in markets]
        ticks[0]["quoteVolume"] = "1"
        self.assertEqual(
            [a.symbol for a in rank_alerts(alerts)], ["BTCUSDT", "ETHUSDT"]
        )
        self.assertEqual(alerts[0].captured_quote_volume, D("200"))

    def test_microsecond_time_order_and_decimal_precision(self):
        alerts = [
            Alert("BTCUSDT", D("1.00000000000000001"), NOW, "less"),
            Alert("ETHUSDT", D("1.00000000000000002"), NOW, "older"),
            Alert("SOLUSDT", D("1.00000000000000002"),
                  NOW + timedelta(microseconds=1), "newer"),
        ]
        with localcontext() as context:
            context.prec = 6
            self.assertEqual(
                [a.alert_id for a in rank_alerts(alerts)], ["newer", "older", "less"]
            )

    def test_alert_requires_decimal_volume_and_utc_time(self):
        for volume, error in [(1.0, TypeError), ("1", TypeError),
                              (D("NaN"), ValueError), (D("-1"), ValueError)]:
            with self.subTest(volume=volume), self.assertRaises(error):
                Alert("BTCUSDT", volume, NOW, "a")
        for when in [NOW.replace(tzinfo=None),
                     NOW.astimezone(timezone(timedelta(hours=3)))]:
            with self.subTest(when=when), self.assertRaises(ValueError):
                Alert("BTCUSDT", D("1"), when, "a")

    def test_empty_alerts_and_exact_duplicates(self):
        self.assertEqual(rank_alerts([]), [])
        alert = Alert("BTCUSDT", D("1"), NOW, "a")
        self.assertEqual(rank_alerts([alert, alert]), [alert, alert])


if __name__ == "__main__":
    unittest.main()
