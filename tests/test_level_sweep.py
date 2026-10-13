import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from crypto_monitor.core.level_sweep import (
    DAY,
    MINUTE,
    Candle,
    Levels,
    Period,
    calculate_levels,
    detect_sweep,
    reference_periods,
)


D = Decimal


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def period(start: str, end: str) -> Period:
    return Period(utc(start), utc(end))


def candle(
    when: str = "2026-10-09T12:00:00",
    high: str = "100.01",
    low: str = "99.98",
    close: str = "99.99",
    interval: timedelta = MINUTE,
    closed: bool = True,
) -> Candle:
    return Candle(utc(when), interval, D(high), D(low), D(close), closed)


class CalendarTests(unittest.TestCase):
    def test_monday_activation_and_expiry(self):
        cases = [
            ("2026-10-05T00:00:00", None),
            ("2026-10-05T23:59:59.999999", None),
            ("2026-10-06T00:00:00", period("2026-10-05", "2026-10-06")),
            ("2026-10-11T23:59:59.999999", period("2026-10-05", "2026-10-06")),
            ("2026-10-12T00:00:00", None),
            ("2026-10-13T00:00:00", period("2026-10-12", "2026-10-13")),
        ]
        for when, expected in cases:
            with self.subTest(when=when):
                self.assertEqual(reference_periods(utc(when))["monday"], expected)

    def test_week_rollover(self):
        for when, start, end in [
            ("2026-10-11T23:59:59.999999", "2026-09-28", "2026-10-05"),
            ("2026-10-12T00:00:00", "2026-10-05", "2026-10-12"),
            ("2026-01-05T00:00:00", "2025-12-29", "2026-01-05"),
        ]:
            with self.subTest(when=when):
                self.assertEqual(
                    reference_periods(utc(when))["previous_week"], period(start, end)
                )

    def test_month_rollovers_and_leap_year(self):
        for when, start, end in [
            ("2026-10-31T23:59:59.999999", "2026-09-01", "2026-10-01"),
            ("2026-11-01T00:00:00", "2026-10-01", "2026-11-01"),
            ("2026-01-01T00:00:00", "2025-12-01", "2026-01-01"),
            ("2024-03-01T00:00:00", "2024-02-01", "2024-03-01"),
        ]:
            with self.subTest(when=when):
                self.assertEqual(
                    reference_periods(utc(when))["previous_month"], period(start, end)
                )

    def test_every_quarter_rollover(self):
        for boundary, previous_start, older_start in [
            ("2026-01-01", "2025-10-01", "2025-07-01"),
            ("2026-04-01", "2026-01-01", "2025-10-01"),
            ("2026-07-01", "2026-04-01", "2026-01-01"),
            ("2026-10-01", "2026-07-01", "2026-04-01"),
        ]:
            with self.subTest(boundary=boundary):
                self.assertEqual(
                    reference_periods(utc(boundary))["previous_quarter"],
                    period(previous_start, boundary),
                )
                self.assertEqual(
                    reference_periods(utc(boundary) - timedelta(microseconds=1))[
                        "previous_quarter"
                    ],
                    period(older_start, previous_start),
                )

    def test_rejects_naive_and_non_utc_times(self):
        for when in [
            datetime(2026, 10, 9),
            datetime(2026, 10, 9, tzinfo=timezone(timedelta(hours=3))),
        ]:
            with self.subTest(when=when), self.assertRaises(ValueError):
                reference_periods(when)

    def test_rejects_invalid_periods(self):
        for start, end in [
            ("2026-10-05", "2026-10-05"),
            ("2026-10-06", "2026-10-05"),
            ("2026-10-05T01:00:00", "2026-10-06"),
        ]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                period(start, end)


class LevelTests(unittest.TestCase):
    def setUp(self):
        self.period = period("2026-10-05", "2026-10-12")
        self.history = [
            candle(f"2026-10-{day:02}", interval=DAY) for day in range(5, 12)
        ]

    def test_start_included_end_excluded_and_order_irrelevant(self):
        self.history[0] = candle("2026-10-05", "110", "90", "100", DAY)
        self.history[-1] = candle("2026-10-11", "120", "95", "100", DAY)
        outside = [
            candle("2026-10-04", "999", "1", "100", DAY),
            candle("2026-10-12", "999", "1", "100", DAY),
        ]
        self.assertEqual(
            calculate_levels(self.period, reversed(self.history + outside)),
            Levels(high=D("120"), low=D("90")),
        )

    def test_each_missing_day_makes_levels_unavailable(self):
        for index in range(7):
            with self.subTest(missing=index):
                partial = self.history[:index] + self.history[index + 1:]
                self.assertIsNone(
                    calculate_levels(self.period, partial)
                )

    def test_empty_history(self):
        self.assertIsNone(calculate_levels(self.period, []))

    def test_unclosed_daily_candle(self):
        self.history[-1] = replace(self.history[-1], closed=False)
        self.assertIsNone(calculate_levels(self.period, self.history))

    def test_duplicate_day_is_unavailable(self):
        self.assertIsNone(calculate_levels(self.period, self.history + self.history[:1]))

    def test_wrong_interval_or_non_midnight_is_unavailable(self):
        for replacement in [
            replace(self.history[0], interval=MINUTE),
            replace(self.history[0], open_time=utc("2026-10-05T01:00:00")),
        ]:
            with self.subTest(replacement=replacement):
                self.assertIsNone(
                    calculate_levels(self.period, [replacement] + self.history[1:])
                )

    def test_complete_leap_month_requires_february_29(self):
        window = period("2024-02-01", "2024-03-01")
        history = [
            candle(f"2024-02-{day:02}", interval=DAY) for day in range(1, 30)
        ]
        self.assertEqual(
            calculate_levels(window, history), Levels(D("100.01"), D("99.98"))
        )
        self.assertIsNone(calculate_levels(window, history[:-1]))


class SweepTests(unittest.TestCase):
    def test_high_sweep_cases(self):
        for high, close, previous, expected in [
            ("100.01", "99.99", "100", True),
            ("100.02", "99.99", "99.99", True),
            ("100", "99.99", "100", False),  # exact touch
            ("100.009", "99.99", "100", False),  # less than a tick
            ("100.01", "100", "100", False),  # no strict reclaim
            ("100.02", "100.01", "100", False),  # closes beyond level
            ("100.01", "99.99", "100.01", False),  # already beyond level
        ]:
            with self.subTest(high=high, close=close, previous=previous):
                self.assertEqual(
                    detect_sweep(
                        "high", D("100"), D(previous), D("0.01"),
                        candle(high=high, close=close),
                    ),
                    expected,
                )

    def test_low_sweep_cases(self):
        for low, close, previous, expected in [
            ("99.99", "100.01", "100", True),
            ("99.98", "100.01", "100.01", True),
            ("100", "100.01", "100", False),
            ("99.991", "100.01", "100", False),
            ("99.99", "100", "100", False),
            ("99.98", "99.99", "100", False),
            ("99.99", "100.01", "99.99", False),
        ]:
            with self.subTest(low=low, close=close, previous=previous):
                self.assertEqual(
                    detect_sweep(
                        "low", D("100"), D(previous), D("0.01"),
                        candle(high="100.02", low=low, close=close),
                    ),
                    expected,
                )

    def test_requires_closed_one_minute_candle(self):
        for bar in [candle(closed=False), candle(interval=5 * MINUTE)]:
            with self.subTest(bar=bar):
                self.assertFalse(
                    detect_sweep("high", D("100"), D("100"), D("0.01"), bar)
                )

    def test_decimal_tick_boundary(self):
        self.assertTrue(
            detect_sweep(
                "high", D("0.1"), D("0.1"), D("0.2"),
                candle(high="0.3", low="0.08", close="0.09"),
            )
        )

    def test_rejects_invalid_tick_and_side(self):
        for tick in [D("0"), D("-0.01")]:
            with self.subTest(tick=tick), self.assertRaises(ValueError):
                detect_sweep("high", D("100"), D("100"), tick, candle())
        with self.assertRaises(ValueError):
            detect_sweep("other", D("100"), D("100"), D("0.01"), candle())

    def test_rejects_float_and_nonfinite_prices(self):
        for bad, error in [
            (100.0, TypeError),
            (D("NaN"), ValueError),
            (D("Infinity"), ValueError),
        ]:
            with self.subTest(bad=bad):
                with self.assertRaises(error):
                    replace(candle(), high=bad)
                with self.assertRaises(error):
                    detect_sweep("high", bad, D("100"), D("0.01"), candle())


if __name__ == "__main__":
    unittest.main()
