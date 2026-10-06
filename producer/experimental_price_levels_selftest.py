"""Network-free regressions for symmetric experimental reference price levels."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import unittest

from experimental_price_levels import calculate_price_levels, render_price_levels


NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


class ExperimentalPriceLevelsTests(unittest.TestCase):
    def test_two_percent_long_and_short_are_symmetric(self):
        for direction, stop, target in (("LONG", "98", "102"), ("SHORT", "102", "98")):
            with self.subTest(direction=direction):
                levels = calculate_price_levels("100", NOW, 200, direction)
                self.assertEqual(levels.stop_loss, Decimal(stop))
                self.assertEqual(levels.take_profit, Decimal(target))
                self.assertEqual(levels.reference_price, Decimal("100"))

    def test_half_and_one_percent_use_their_own_threshold(self):
        for bps, stop, target in ((50, "99.5", "100.5"), (100, "99", "101")):
            levels = calculate_price_levels(100, NOW, bps, "LONG")
            self.assertEqual((levels.stop_loss, levels.take_profit), (Decimal(stop), Decimal(target)))

    def test_decimal_arithmetic_doge_and_btc_survives_low_external_precision(self):
        with localcontext() as context:
            context.prec = 5
            doge = calculate_price_levels(0.08329, NOW, 200, "LONG")
            btc = calculate_price_levels("113500.12", NOW, 100, "SHORT")
        self.assertEqual(doge.stop_loss, Decimal("0.0816242"))
        self.assertEqual(doge.take_profit, Decimal("0.0849558"))
        self.assertEqual(btc.stop_loss, Decimal("114635.1212"))
        self.assertEqual(btc.take_profit, Decimal("112365.1188"))
        text = render_price_levels(doge, reference_label="סגירת נתוני CVD")
        self.assertIn("0.0816242", text)
        self.assertIn("0.0849558", text)
        self.assertIn("15.09.2026 15:00:00 (שעון ישראל)", text)

    def test_small_price_and_small_threshold_do_not_collapse_displayed_levels(self):
        for price, threshold in (("0.00000001", 50), ("100", "0.000000001")):
            levels = calculate_price_levels(price, NOW, threshold, "LONG")
            text = render_price_levels(levels, reference_label="מחיר המקור", html=False)
            prices = [Decimal(line.split(": ", 1)[1]) for line in text.splitlines() if "שעת" not in line]
            self.assertEqual(len(set(prices)), 3)
            self.assertTrue(prices[1] < prices[0] < prices[2])

    def test_invalid_inputs_fail_closed(self):
        for bad in (None, True, False, 0, -1, "", "missing", "NaN", "sNaN", float("inf"), float("nan"), []):
            with self.subTest(price=repr(bad)):
                with self.assertRaises(ValueError):
                    calculate_price_levels(bad, NOW, 200, "LONG")
        for bad in (None, True, False, 0, -1, 10000, 10001, "NaN", float("inf"), []):
            with self.subTest(threshold=repr(bad)):
                with self.assertRaises(ValueError):
                    calculate_price_levels(100, NOW, bad, "LONG")
        for bad in (None, True, "UPPER", "BEARISH", "", "LONG SHORT"):
            with self.subTest(direction=repr(bad)):
                with self.assertRaises(ValueError):
                    calculate_price_levels(100, NOW, 200, bad)
        for bad in (None, True, 0, "not a date", "2026-09-15T12:00:00", NOW.replace(tzinfo=None)):
            with self.subTest(timestamp=repr(bad)):
                with self.assertRaises(ValueError):
                    calculate_price_levels(100, bad, 200, "LONG")

    def test_aware_timestamp_normalization_and_winter_israel_clock(self):
        alternate = NOW.astimezone(timezone(timedelta(hours=3))).isoformat()
        self.assertEqual(calculate_price_levels(100, alternate, 200, "LONG").reference_time, NOW)
        winter = calculate_price_levels(100, "2026-01-15T12:00:00Z", 200, "LONG")
        self.assertIn("15.01.2026 14:00:00", render_price_levels(winter, reference_label="מקור"))

    def test_html_escapes_reference_label_and_plain_mode_has_no_markup(self):
        levels = calculate_price_levels(100, NOW, 200, "LONG")
        label = "מקור <CVD> & מחיר"
        text = render_price_levels(levels, reference_label=label)
        self.assertIn("&lt;CVD&gt; &amp;", text)
        self.assertNotIn("<CVD>", text)
        self.assertIn("<b>סטופלוס:</b> 98", text)
        self.assertIn("<b>טייק פרופיט:</b> 102", text)
        plain = render_price_levels(levels, reference_label="מקור", html=False)
        self.assertNotIn("<b>", plain)
        self.assertIn("שער בסיס — מקור: 100", plain)
        for bad in (None, "", "   "):
            with self.assertRaises(ValueError):
                render_price_levels(levels, reference_label=bad)

    def test_render_rejects_tampered_direction_or_levels(self):
        levels = calculate_price_levels(100, NOW, 200, "LONG")
        for bad in (replace(levels, stop_loss=Decimal(99)), replace(levels, direction="SHORT"), None):
            with self.assertRaises(ValueError):
                render_price_levels(bad, reference_label="מקור")


if __name__ == "__main__":
    unittest.main()
