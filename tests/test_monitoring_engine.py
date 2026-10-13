"""Fixture-only monitoring engine tests; no transports are opened."""

import json
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from crypto_monitor.adapters.binance_feed import ClosedKline, MinuteCandle, parse_combined_message
from crypto_monitor.adapters.binance_rest import DiscoveredMarket
from crypto_monitor.core.level_sweep import MINUTE, Levels, Period, detect_sweep, reference_periods
from crypto_monitor.core.market_universe import Alert, rank_alerts
from crypto_monitor.core.monitoring_engine import MonitoringEngine, ReferenceLevels


D = Decimal
START = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)


def market(symbol="BTCUSDT", volume="1000", tick="0.01"):
    return DiscoveredMarket(symbol, symbol[:-4], D(volume), D(tick))


def event(minute=0, symbol="BTCUSDT", high="100", low="98", close="99", *, start=START):
    opened = start + minute * MINUTE
    return ClosedKline(
        symbol=symbol,
        candle=MinuteCandle(
            open_time=opened, interval=MINUTE, high=D(high), low=D(low),
            close=D(close), closed=True, open_price=D(close),
        ),
        close_time=opened + MINUTE - timedelta(milliseconds=1),
        event_time=opened + MINUTE,
    )


def references(*names, high="100", low="90", as_of=START):
    periods = reference_periods(as_of)
    return {
        name: ReferenceLevels(periods[name], Levels(D(high), D(low)))
        for name in (names or ("monday",))
    }


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = MonitoringEngine([market(), market("ETHUSDT", "500", "0.10")])
        self.refs = references()

    def test_first_candle_only_establishes_baseline(self):
        with patch("crypto_monitor.core.monitoring_engine.detect_sweep", wraps=detect_sweep) as predicate:
            self.assertEqual(self.engine.process(event(high="101"), self.refs), [])
        predicate.assert_not_called()

    def test_consecutive_high_sweep_reuses_predicate_and_tick(self):
        self.engine.process(event(close="100"), self.refs)
        current = event(1, high="100.01")
        with patch("crypto_monitor.core.monitoring_engine.detect_sweep", wraps=detect_sweep) as predicate:
            hits = self.engine.process(current, self.refs)
        self.assertEqual(len(hits), 1)
        predicate.assert_any_call("high", D("100"), D("100"), D("0.01"), current.candle)
        hit = hits[0]
        self.assertIsInstance(hit, Alert)
        self.assertEqual((hit.symbol, hit.level_name, hit.side), ("BTCUSDT", "monday", "high"))
        self.assertEqual(hit.level_price, D("100"))
        self.assertEqual(hit.captured_quote_volume, D("1000"))
        self.assertEqual(hit.candle_time, START + MINUTE)
        self.assertEqual(hit.detected_at, current.event_time)

    def test_low_sweep_and_strict_reclaim(self):
        refs = references(high="110", low="100")
        self.engine.process(event(high="102", low="100", close="100"), refs)
        hits = self.engine.process(event(1, high="102", low="99.99", close="101"), refs)
        self.assertEqual([hit.side for hit in hits], ["low"])
        # A close on the level isn't a reclaim.
        self.assertEqual(self.engine.process(
            event(2, high="102", low="99.99", close="100"), refs,
        ), [])

    def test_exact_touches_and_sub_tick_breaches_do_not_count(self):
        for high in ["100", "100.009"]:
            engine = MonitoringEngine([market()])
            engine.process(event(), self.refs)
            with self.subTest(high=high):
                self.assertEqual(engine.process(event(1, high=high), self.refs), [])

    def test_uses_immediately_previous_close_not_initial_close(self):
        self.engine.process(event(), self.refs)
        self.assertEqual(self.engine.process(event(1, high="102", close="101"), self.refs), [])
        # Previous close is now beyond the high level, despite initial close 99.
        self.assertEqual(self.engine.process(event(2, high="102", close="99"), self.refs), [])
        self.assertEqual(len(self.engine.process(event(3, high="101"), self.refs)), 1)

    def test_per_symbol_baselines_and_tick_sizes(self):
        self.engine.process(event(), self.refs)
        self.assertEqual(self.engine.process(event(1, "ETHUSDT", high="101"), self.refs), [])
        self.assertEqual(len(self.engine.process(event(1, high="100.01"), self.refs)), 1)
        self.assertEqual(self.engine.process(event(2, "ETHUSDT", high="100.09"), self.refs), [])
        self.assertEqual(len(self.engine.process(event(3, "ETHUSDT", high="100.10"), self.refs)), 1)

    def test_forward_gap_resets_baseline_without_sweep(self):
        self.engine.process(event(), self.refs)
        with patch("crypto_monitor.core.monitoring_engine.detect_sweep", wraps=detect_sweep) as predicate:
            self.assertEqual(self.engine.process(event(3, high="101"), self.refs), [])
        predicate.assert_not_called()
        self.assertEqual(len(self.engine.process(event(4, high="101"), self.refs)), 1)

    def test_out_of_order_and_duplicate_timestamps_reset_baseline(self):
        for reset_minute in [-2, 0]:
            with self.subTest(reset_minute=reset_minute):
                engine = MonitoringEngine([market()])
                engine.process(event(), self.refs)
                self.assertEqual(engine.process(event(reset_minute, high="101"), self.refs), [])
                # New baseline is the most recently received candle, even if older.
                hits = engine.process(event(reset_minute + 1, high="101"), self.refs)
                self.assertEqual(len(hits), 1)

    def test_late_candle_does_not_create_a_sweep_across_a_second_gap(self):
        self.engine.process(event(5), self.refs)
        self.engine.process(event(2), self.refs)
        self.assertEqual(self.engine.process(event(6, high="101"), self.refs), [])
        self.assertEqual(len(self.engine.process(event(7, high="101"), self.refs)), 1)

    def test_multiple_distinct_levels_including_equal_prices_are_all_returned(self):
        refs = references("monday", "previous_week", "previous_month", "previous_quarter")
        refs["previous_month"] = replace(refs["previous_month"], levels=Levels(D("100.5"), D("90")))
        self.engine.process(event(), refs)
        hits = self.engine.process(event(1, high="101"), refs)
        self.assertEqual(len(hits), 4)
        self.assertEqual({hit.level_name for hit in hits}, set(refs))
        self.assertEqual(len({hit.alert_id for hit in hits}), 4)

    def test_high_and_low_of_distinct_ranges_can_both_be_hit(self):
        refs = references(high="100", low="90")
        refs.update(references("previous_week", high="110", low="99"))
        self.engine.process(event(close="99.5"), refs)
        hits = self.engine.process(event(1, high="101", low="98", close="99.5"), refs)
        self.assertEqual({(hit.level_name, hit.side) for hit in hits},
                         {("monday", "high"), ("previous_week", "low")})

    def test_stable_alert_identity_uses_period_and_candle_not_event_time(self):
        def replay(current):
            engine = MonitoringEngine([market()])
            engine.process(event(), self.refs)
            return engine.process(current, self.refs)[0]

        current = event(1, high="101")
        first = replay(current)
        delayed = replay(replace(current, event_time=current.event_time + timedelta(seconds=5)))
        self.assertEqual(first.alert_id, delayed.alert_id)
        self.assertEqual(json.loads(first.alert_id), [
            "BTCUSDT", "monday", "high", self.refs["monday"].period.start.isoformat(),
            current.candle.open_time.isoformat(),
        ])
        # Distinct sweep candles aren't suppressed by this in-memory engine.
        self.engine.process(event(), self.refs)
        one = self.engine.process(current, self.refs)[0]
        two = self.engine.process(event(2, high="101"), self.refs)[0]
        self.assertNotEqual(one.alert_id, two.alert_id)

    def test_unavailable_and_empty_reference_sets_still_establish_baselines(self):
        self.assertEqual(self.engine.process(event(), {}), [])
        self.assertEqual(len(self.engine.process(event(1, high="101"), self.refs)), 1)
        unavailable = {"monday": replace(self.refs["monday"], levels=None)}
        self.assertEqual(self.engine.process(event(2, high="101"), unavailable), [])
        self.assertEqual(len(self.engine.process(event(3, high="101"), self.refs)), 1)

    def test_inactive_stale_or_future_reference_period_is_not_used(self):
        for start in [datetime(2026, 10, 5, 12, tzinfo=timezone.utc),
                      datetime(2026, 10, 12, 12, tzinfo=timezone.utc)]:
            with self.subTest(start=start):
                engine = MonitoringEngine([market()])
                engine.process(event(start=start), self.refs)
                self.assertEqual(engine.process(event(1, high="101", start=start), self.refs), [])

    def test_rollover_uses_period_active_during_candle_not_delivery_time(self):
        start = datetime(2026, 10, 11, 23, 58, tzinfo=timezone.utc)
        self.engine.process(event(start=start), self.refs)
        # Sunday's final candle arrives Monday; Sunday's Monday range still applies.
        hits = self.engine.process(event(1, high="101", start=start), self.refs)
        self.assertEqual(len(hits), 1)
        self.assertEqual(self.engine.process(event(2, high="101", start=start), self.refs), [])

    def test_unclosed_and_wrong_interval_do_not_change_baseline(self):
        self.engine.process(event(), self.refs)
        for changes in [{"closed": False}, {"interval": 5 * MINUTE}]:
            invalid = replace(event(1), candle=replace(event(1).candle, **changes))
            self.assertEqual(self.engine.process(invalid, self.refs), [])
        self.assertEqual(len(self.engine.process(event(1, high="101"), self.refs)), 1)

    def test_unknown_symbol_or_level_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unselected symbol"):
            self.engine.process(event(symbol="SOLUSDT"), self.refs)
        with self.assertRaisesRegex(ValueError, "unknown reference"):
            self.engine.process(event(), {"typo": self.refs["monday"]})
        self.assertEqual(self.engine.process(event(1, high="101"), self.refs), [])

    def test_invalid_metadata_and_reference_prices(self):
        for changes, error in [({"tick_size": 0.01}, TypeError),
                               ({"tick_size": D("0")}, ValueError),
                               ({"quote_volume": D("NaN")}, ValueError)]:
            with self.subTest(changes=changes), self.assertRaises(error):
                MonitoringEngine([replace(market(), **changes)])
        with self.assertRaisesRegex(ValueError, "duplicate market"):
            MonitoringEngine([market(), market()])
        with self.assertRaises(TypeError):
            ReferenceLevels(self.refs["monday"].period, Levels(100.0, D("90")))

    def test_actual_feed_parser_output_is_accepted(self):
        opened = (START - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)
        raw = {"stream": "btcusdt@kline_1m", "data": {
            "e": "kline", "s": "BTCUSDT", "E": opened + 60_000,
            "k": {"s": "BTCUSDT", "i": "1m", "t": opened, "T": opened + 59_999,
                  "x": True, "o": "99", "h": "100", "l": "98", "c": "99"},
        }}
        self.engine.process(parse_combined_message(json.dumps(raw), ["BTCUSDT"]), self.refs)
        raw["data"]["E"] += 60_000
        raw["data"]["k"].update(t=opened + 60_000, T=opened + 119_999, h="101")
        hits = self.engine.process(parse_combined_message(json.dumps(raw), ["BTCUSDT"]), self.refs)
        self.assertEqual(len(hits), 1)


class RankingTests(unittest.TestCase):
    def test_batch_uses_existing_volume_time_and_tie_ranking(self):
        markets = [market("BTCUSDT", "1000"), market("ETHUSDT", "2000"), market("SOLUSDT", "1000")]
        engine = MonitoringEngine(markets)
        refs = {m.symbol: references() for m in markets}
        events = [event(symbol=m.symbol) for m in markets]
        events += [event(1, "SOLUSDT", high="101"), event(1, "BTCUSDT", high="101"),
                   event(1, "ETHUSDT", high="101"), event(2, "BTCUSDT", high="101")]
        with patch("crypto_monitor.core.monitoring_engine.rank_alerts", wraps=rank_alerts) as ranking:
            hits = engine.process_batch(events, refs)
        self.assertEqual([h.symbol for h in hits], ["ETHUSDT", "BTCUSDT", "BTCUSDT", "SOLUSDT"])
        self.assertEqual([h.candle_time for h in hits],
                         [START + MINUTE, START + 2 * MINUTE, START + MINUTE, START + MINUTE])
        self.assertCountEqual(ranking.call_args.args[0], hits)
        self.assertEqual(hits, rank_alerts(hits[::-1]))

    def test_volume_is_captured_from_metadata(self):
        markets = [market()]
        engine = MonitoringEngine(markets)
        engine.process(event(), references())
        hit = engine.process(event(1, high="101"), references())[0]
        markets[0] = market(volume="1")
        self.assertEqual(hit.captured_quote_volume, D("1000"))


if __name__ == "__main__":
    unittest.main()
