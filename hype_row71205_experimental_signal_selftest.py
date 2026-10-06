"""Offline frozen thresholds, causal windows and reference-level tests."""
import math
import unittest
import hype_row71205_experimental_signal as s


class SignalTests(unittest.TestCase):
    def fixture(self):
        d = 1791100800000
        d -= d % s.DECISION_STEP_MS
        hype = [[t, 100., 102., 98., 100.] for t in range(d-24*s.HOUR_MS, d, s.MINUTE_MS)]
        btc = [[t, 100., 101., 99., 100.1] for t in range(d-12*s.HOUR_MS, d, s.MINUTE_MS)]
        return d, hype, btc

    def test_exact_range_start_open_arithmetic_and_causal_features(self):
        d, h, b = self.fixture()
        h[0][1] = 101
        h[-240][1] = 99
        out = s.evaluate_signal(h, b, d)
        self.assertTrue(out['valid'])
        self.assertFalse(out['signal'])  # HYPE 4h outperformed BTC in this fixture.
        self.assertEqual(out['hype_range_24h_pct'], 100*(102-98)/101)
        self.assertEqual(out['range_4h_to_24h'], (100*(102-98)/99)/(100*(102-98)/101))
        self.assertEqual(out['relative_return_4h'], 100/99-1 - (100.1/100-1))

    def test_future_and_entry_bar_values_do_not_enter_features(self):
        d, h, b = self.fixture()
        original = s.evaluate_signal(h, b, d)
        future = [[d, float('nan'), 0, 0, 0], [d+s.MINUTE_MS, 10000, 0, 0, 0]]
        self.assertEqual(s.evaluate_signal(h+future, b+future, d), original)
        self.assertEqual(original['entry_ms'], d+s.MINUTE_MS)

    def test_each_strict_boundary_and_frozen_upper_float(self):
        def match(**kw):
            args = dict(btc_return_12h_pct=.1, hype_range_24h_pct=4.,
                        range_4h_to_24h=.6, relative_return_4h=0.)
            return s.predicate_values(**{**args, **kw})
        self.assertTrue(match())
        self.assertFalse(match(btc_return_12h_pct=0))
        self.assertFalse(match(btc_return_12h_pct=.25))
        self.assertTrue(match(btc_return_12h_pct=math.nextafter(.25, 0)))
        self.assertTrue(match(hype_range_24h_pct=s.RANGE_24H_MAX_PCT))
        self.assertFalse(match(hype_range_24h_pct=math.nextafter(s.RANGE_24H_MAX_PCT, math.inf)))
        self.assertFalse(match(range_4h_to_24h=.5))
        self.assertTrue(match(range_4h_to_24h=math.nextafter(.5, math.inf)))
        self.assertFalse(match(relative_return_4h=math.nextafter(0, math.inf)))

    def test_gaps_duplicates_wrong_grid_and_flat_range_fail_closed(self):
        d, h, b = self.fixture()
        for hx, bx, dx in [(h[1:],b,d), (h,b[:-1],d), (h[:4]+[h[3]]+h[4:],b,d),
                          (h,b,d+15*s.MINUTE_MS)]:
            out = s.evaluate_signal(hx,bx,dx)
            self.assertFalse(out['valid'])
            self.assertFalse(out['signal'])
        flat = [[r[0], 100, 100, 100, 100] for r in h]
        self.assertFalse(s.evaluate_signal(flat,b,d)['signal'])

    def test_static_levels_no_lock_or_notional_cap(self):
        levels = s.build_levels(100)
        self.assertEqual(levels['stop_loss'], 100*1.005)
        self.assertEqual(levels['take_profit'], 98)
        self.assertEqual(levels['reward_risk'], 4)
        self.assertEqual(5/levels['risk_fraction'], 1000)
        self.assertFalse(any('lock' in k or 'notional' in k for k in levels))


if __name__ == '__main__':
    unittest.main()
