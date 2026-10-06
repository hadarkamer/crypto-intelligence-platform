"""Network-free frozen U21 golden, causal-data, NY calendar and outcome tests."""
from datetime import datetime, timezone
from pathlib import Path
import json
import unittest

from u21_experimental_signal import (
    MINUTE_MS, DECISION_STEP_MS, build_levels, evaluate_signal, first_touch,
    last_closed_decision_ms, nonoverlap_allows, predicate_values,
    previous_ny_regular_session, required_history_start_ms,
)


def ms(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


class FrozenU21Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = ms("2026-09-25T07:15:00Z")
        starts = required_history_start_ms(cls.d)
        cls.xrp = [[t, 100, 101, 98, 100] for t in range(starts["XRP"], cls.d, MINUTE_MS)]
        cls.btc = [[t, 60000, 60001, 59999, 60000] for t in range(starts["BTC"], cls.d, MINUTE_MS)]

    def test_exact_archived_golden_features(self):
        cases = json.loads(Path(__file__).with_name("u21_experimental_signal_golden.json").read_text())
        self.assertEqual(len(cases), 40)
        for case in cases:
            with self.subTest(decision=case["decision_ms"]):
                if case["session_high"] is None or case["session_low"] is None:
                    self.assertFalse(case["expected_signal"])
                    self.assertFalse(case["common_valid"])
                    continue
                out = predicate_values(case["last_closed_price"], case["session_high"], case["session_low"])
                self.assertEqual(out["location"], case["location"])
                self.assertEqual(out["distance"], case["distance"])
                self.assertEqual(out["signal"] and case["common_valid"], case["expected_signal"])
                self.assertEqual(previous_ny_regular_session(case["decision_ms"]),
                                 (case["prior_ny_start_ms"], case["prior_ny_end_ms"]))

    def test_causal_predicate_and_separate_entry_observation(self):
        out = evaluate_signal(self.xrp, self.btc, self.d)
        self.assertTrue(out["valid"])
        self.assertTrue(out["signal"])
        self.assertEqual(out["entry_ms"], self.d + MINUTE_MS)
        self.assertEqual(out["last_closed_minute_ms"], self.d - MINUTE_MS)
        # The D and D+1 candles are not feature inputs, even if supplied.
        future = [[self.d, "bad", "future", None, 0], [self.d + MINUTE_MS, 1, 2, .1, .5]]
        self.assertEqual(evaluate_signal(self.xrp + future, self.btc, self.d), out)

    def test_missing_btc_or_xrp_never_becomes_valid_false(self):
        for x, b in ((self.xrp[:-1], self.btc), (self.xrp, self.btc[1:]),
                     (self.xrp[:100] + self.xrp[101:], self.btc)):
            out = evaluate_signal(x, b, self.d)
            self.assertFalse(out["valid"])
            self.assertFalse(out["signal"])

    def test_no_btc_directional_filter(self):
        declining = [[r[0], 60000, 60001, 99, 100] for r in self.btc]
        self.assertEqual(evaluate_signal(self.xrp, declining, self.d), evaluate_signal(self.xrp, self.btc, self.d))

    def test_flat_short_window_fails_frozen_common_coverage(self):
        flat = self.xrp[:-5] + [[r[0], 100, 100, 100, 100] for r in self.xrp[-5:]]
        out = evaluate_signal(flat, self.btc, self.d)
        self.assertFalse(out["valid"])
        self.assertEqual(out["reason"], "flat_common_window_5")

    def test_holidays_early_closes_dst_and_exact_session_end(self):
        self.assertEqual(previous_ny_regular_session(ms("2026-09-07T21:00:00Z")),
                         (ms("2026-09-04T13:30:00Z"), ms("2026-09-04T20:00:00Z")))
        self.assertEqual(previous_ny_regular_session(ms("2025-11-28T18:00:00Z")),
                         (ms("2025-11-28T14:30:00Z"), ms("2025-11-28T18:00:00Z")))
        self.assertEqual(previous_ny_regular_session(ms("2026-03-09T20:00:00Z")),
                         (ms("2026-03-09T13:30:00Z"), ms("2026-03-09T20:00:00Z")))
        with self.assertRaises(ValueError):
            previous_ny_regular_session(ms("2027-01-04T21:00:00Z"))

    def test_decision_grid_and_levels(self):
        self.assertEqual(last_closed_decision_ms(self.d + 59123), self.d)
        self.assertFalse(evaluate_signal(self.xrp, self.btc, self.d + MINUTE_MS)["valid"])
        levels = build_levels(2)
        self.assertEqual(levels["stop_loss"], 2 * 1.005)
        self.assertEqual(levels["take_profit"], 2 * .92)

    def test_strict_nonoverlap_release_is_candle_end(self):
        end = self.d + MINUTE_MS
        out = first_touch(100, [[self.d, 100, 101, 99, 100]], self.d, end)
        self.assertEqual((out["status"], out["exit_ms"], out["release_ms"]), ("SL", end, end))
        self.assertFalse(nonoverlap_allows(end, out["release_ms"]))
        self.assertTrue(nonoverlap_allows(end + DECISION_STEP_MS, out["release_ms"]))
        self.assertFalse(nonoverlap_allows(end + DECISION_STEP_MS, None, active=True))

    def test_samebar_is_ambiguous_not_assumed_stop_first(self):
        out = first_touch(100, [[self.d, 100, 101, 91, 99]], self.d, self.d + MINUTE_MS)
        self.assertEqual((out["status"], out["status_code"], out["release_ms"]), ("AMBIGUOUS", 4, None))

    def test_gap_stop_actual_open_and_gap_take_fixed_target(self):
        end = self.d + MINUTE_MS
        loss = first_touch(100, [[self.d, 102, 103, 90, 100]], self.d, end)
        win = first_touch(100, [[self.d, 90, 101, 89, 100]], self.d, end)
        self.assertEqual((loss["status"], loss["exit_price"], loss["reason_code"]), ("SL", 102, 10))
        self.assertLess(loss["R"], -1)
        self.assertEqual((win["status"], win["exit_price"], win["reason_code"]), ("TP", 92, 11))
        self.assertAlmostEqual(win["R"], 16)

    def test_missing_price_bar_reserves_capacity(self):
        out = first_touch(100, [[self.d + MINUTE_MS, 100, 101, 99, 100]], self.d, self.d + 2 * MINUTE_MS)
        self.assertEqual((out["status"], out["release_ms"]), ("UNKNOWN", None))
        future_only = first_touch(100, [[self.d, 100, 101, 91, 99]], self.d, self.d)
        self.assertEqual(future_only["status"], "OPEN")


if __name__ == "__main__":
    unittest.main()
