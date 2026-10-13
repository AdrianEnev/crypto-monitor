"""B4: release pending state at the frozen six-hour expiry, before the next candle."""
import unittest
import numpy as np
import pandas as pd
from data import MS, SCALE
from engine import MarketData, make_signals
from test_research import fixture, ts


class TimeoutBoundaryTests(unittest.TestCase):
    def test_sweep_opening_at_six_hour_expiry_is_eligible_when_closed(self):
        minute, daily, members = fixture()
        # The first live sweep never receives an above-level confirmation close.
        minute.loc[1444, ['open', 'high', 'low', 'close']] = np.array([95, 96, 94, 95]) * SCALE
        first = 1441
        expiry = first + 1 + 360
        # An earlier qualifying sweep is still suppressed while the setup is pending.
        for i in [expiry - 1, expiry]:
            minute.loc[i, ['open', 'high', 'low', 'close']] = np.array([99, 101, 98, 99]) * SCALE
        # Supply valid consecutive 5m confirmations for the newly eligible sweep.
        minute.loc[1804, ['open', 'high', 'low', 'close']] = np.array([100, 102, 99, 101]) * SCALE
        minute.loc[1809, ['open', 'high', 'low', 'close']] = np.array([100, 100, 98, 99]) * SCALE
        market = MarketData('TESTUSDT', minute, daily, members, pd.DataFrame())
        events, _ = make_signals(market, ['train'])
        events = [e for e in events if e['level'] == 'monday_high' and e['side'] == 'short'
                  and e['sweep_type'] == 'live_1m' and e['timeframe'] == 5]
        first_event = next(e for e in events if e['sweep_time'] == int(market.t[first]))
        self.assertEqual(first_event['status'], 'confirmation_timeout')
        self.assertEqual(int(market.t[expiry]), first_event['sweep_observed_time'] + 360 * MS)
        self.assertFalse(any(e['sweep_time'] == int(market.t[expiry - 1]) for e in events))
        eligible = [e for e in events if e['sweep_time'] == int(market.t[expiry])]
        self.assertEqual(len(eligible), 1, 'Sweep opening exactly at expiry must be eligible once closed')
        self.assertEqual(eligible[0]['sweep_observed_time'], ts('2022-11-08 06:03'))
        self.assertEqual(eligible[0]['status'], 'confirmed')
        self.assertEqual(eligible[0]['confirmation_time'], ts('2022-11-08 06:10'))


if __name__ == '__main__':
    unittest.main()
