"""Network-free R2732 calendar, causality and archived-value regression tests."""
from datetime import datetime, timezone
import math
import unittest

import xrp_r2732_experimental_signal as signal


def ms(text):
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def bars_for(decision, close=99, high=100):
    start, end = signal.previous_ny_regular_session(decision)
    rows = [[t, high-1, high, high-2, high-1]
            for t in range(start, end, signal.MINUTE_MS)]
    if end < decision:
        rows.append([decision-signal.MINUTE_MS, close, close, close, close])
    else:
        rows[-1] = [rows[-1][0], close, max(high, close), min(high-2, close), close]
    return rows


# Frozen independently from the 2026-10-02 actual filtered replay ledger and
# its annual Binance Spot source: D, last closed price, prior high, ratio, close.
# These span the research history and include holidays, DST and midnight.
GOLDEN = (
    (1756843200000, 2.8282, 2.8455, -0.006079775083465133, 1756843200000),
    (1758064500000, 3.0442, 3.0586, -0.004708036356503076, 1758052800000),
    (1759436100000, 3.0644, 3.0998, -0.011420091618814188, 1759435200000),
    (1761657300000, 2.6813, 2.6975, -0.006005560704355872, 1761595200000),
    (1763519400000, 2.2101, 2.2413, -0.013920492571275478, 1763499600000),
    (1764759600000, 2.1738, 2.1831, -0.004259997251614744, 1764709200000),
    (1766171700000, 1.9005, 1.9341, -0.017372421281215966, 1766091600000),
    (1767924900000, 2.1351, 2.1689, -0.01558393655770196, 1767906000000),
    (1769580900000, 1.9174, 1.9292, -0.0061165249844494785, 1769547600000),
    (1772491500000, 1.4017, 1.423, -0.014968376669009187, 1772485200000),
    (1774264500000, 1.4276, 1.4471, -0.01347522631469844, 1774036800000),
    (1776267000000, 1.38, 1.3955, -0.011107130060910131, 1776196800000),
    (1779681600000, 1.3468, 1.367, -0.014776883686905662, 1779480000000),
    (1783373400000, 1.1524, 1.1593, -0.00595186750625365, 1783368000000),
    (1787621400000, 1.5086, 1.5304, -0.014244641923680135, 1787601600000),
    (1789632900000, 1.2992, 1.305, -0.004444444444444473, 1789588800000),
)


class R2732SignalTests(unittest.TestCase):
    def test_frozen_archived_values_and_reference_sessions(self):
        for decision, close, high, distance, session_end in GOLDEN:
            with self.subTest(decision=decision):
                values = signal.predicate_values(close, high)
                self.assertEqual(values["distance"], distance)
                self.assertTrue(values["signal"])
                self.assertEqual(signal.previous_ny_regular_session(decision)[1], session_end)

    def test_distance_only_and_no_btc_or_location_gate(self):
        decision = ms("2026-10-02T13:00:00Z")
        rows = bars_for(decision)
        # Session low 98 makes 99 location=.5. Moving it to 0.1 makes location
        # >.98, outside U21's range, but R2732's distance remains unchanged.
        baseline = signal.evaluate_signal(rows, decision)
        lowered = [[r[0], r[1], r[2], .1, r[4]] for r in rows]
        self.assertTrue(baseline["signal"])
        self.assertTrue(signal.evaluate_signal(lowered, decision)["signal"])
        self.assertEqual(set(signal.required_history_start_ms(decision)), {"XRP"})

    def test_required_window_completeness_and_no_future_leak(self):
        decision = ms("2026-10-02T13:00:00Z")
        rows = bars_for(decision)
        result = signal.evaluate_signal(rows, decision)
        self.assertTrue(result["valid"])
        self.assertEqual(result["last_closed_minute_ms"], decision-60000)
        self.assertEqual(result["entry_ms"], decision+60000)
        future = [[decision, None, "bad", 0, float("nan")],
                  [decision+60000, 50, 200, 1, 50]]
        self.assertEqual(signal.evaluate_signal(rows+future, decision), result)
        self.assertEqual(signal.evaluate_signal(rows[:-1], decision)["reason"], "missing_last_closed_minute")
        self.assertEqual(signal.evaluate_signal(rows[:2]+rows[3:], decision)["reason"], "incomplete_prior_ny_session")
        self.assertFalse(signal.evaluate_signal(rows[:3]+rows[2:], decision)["valid"])

    def test_ny_weekday_not_utc_or_israel_weekday(self):
        # Saturday UTC and Israel, still Friday 23:45 New York.
        friday = ms("2026-10-03T03:45:00Z")
        self.assertTrue(signal.evaluate_signal(bars_for(friday), friday)["signal"])
        saturday = ms("2026-10-03T04:00:00Z")
        self.assertEqual(signal.evaluate_signal([], saturday)["reason"], "ny_weekend")
        # Monday UTC/Israel, still Sunday New York; Monday 00:00 NY qualifies.
        sunday = ms("2026-10-05T03:45:00Z")
        self.assertEqual(signal.evaluate_signal([], sunday)["reason"], "ny_weekend")
        monday = ms("2026-10-05T04:00:00Z")
        self.assertTrue(signal.evaluate_signal(bars_for(monday), monday)["signal"])

    def test_holiday_is_eligible_weekday_with_friday_reference(self):
        decision = ms("2026-09-07T21:00:00Z")
        result = signal.evaluate_signal(bars_for(decision), decision)
        self.assertTrue(result["signal"])
        self.assertEqual(result["prior_ny_start_ms"], ms("2026-09-04T13:30:00Z"))
        self.assertEqual(result["prior_ny_end_ms"], ms("2026-09-04T20:00:00Z"))

    def test_early_close_exact_boundary_and_dst(self):
        cases = (
            ("2025-11-28T18:00:00Z", "2025-11-28T14:30:00Z", "2025-11-28T18:00:00Z"),
            ("2026-03-09T20:00:00Z", "2026-03-09T13:30:00Z", "2026-03-09T20:00:00Z"),
            ("2026-11-02T21:00:00Z", "2026-11-02T14:30:00Z", "2026-11-02T21:00:00Z"),
        )
        for decision, start, end in cases:
            d = ms(decision)
            self.assertEqual(signal.previous_ny_regular_session(d), (ms(start), ms(end)))
            self.assertTrue(signal.evaluate_signal(bars_for(d), d)["signal"])
        self.assertEqual(signal.previous_ny_regular_session(ms("2025-11-28T17:45:00Z"))[1],
                         ms("2025-11-26T21:00:00Z"))

    def test_unknown_calendar_and_non_grid_fail_closed(self):
        for decision in (ms("2027-01-02T15:00:00Z"), ms("2026-10-02T13:01:00Z")):
            self.assertFalse(signal.evaluate_signal([], decision)["valid"])
            with self.assertRaises(ValueError):
                signal.required_history_start_ms(decision)

    def test_real_double_precision_boundaries(self):
        # Threshold prices are rounded floating-point representations, so use
        # adjacent representable prices and independently evaluate exact ratio.
        for boundary in (signal.DISTANCE_LOWER, signal.DISTANCE_UPPER):
            price = 100*(1+boundary)
            for value in (math.nextafter(price, -math.inf), price, math.nextafter(price, math.inf)):
                distance = value/100-1
                self.assertEqual(signal.predicate_values(value, 100)["signal"],
                                 signal.DISTANCE_LOWER < distance <= signal.DISTANCE_UPPER)
        self.assertFalse(signal.predicate_values(98, 100)["signal"])
        self.assertFalse(signal.predicate_values(99.6, 100)["signal"])

    def test_price_geometry_and_strict_release(self):
        for entry in (100, 2.8183, 1.2992):
            levels = signal.build_levels(entry)
            original_risk = abs(entry-entry*1.005)
            self.assertEqual(levels["lock_trigger_price"], entry-2*original_risk)
            self.assertEqual(levels["locked_stop_loss"], entry-.5*original_risk)
            self.assertEqual(levels["take_profit"], entry*.92)
        decision = ms("2026-10-02T13:00:00Z")
        self.assertFalse(signal.nonoverlap_allows(decision, decision))
        self.assertTrue(signal.nonoverlap_allows(decision+15*60000, decision))
        self.assertFalse(signal.nonoverlap_allows(decision+15*60000, decision, active=True))


if __name__ == "__main__":
    unittest.main()
