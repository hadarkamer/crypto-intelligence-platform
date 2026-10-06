"""Deterministic safety checks: python -m unittest maxpain_cvd_short_alert_selftest."""
from copy import deepcopy
from datetime import datetime, timezone
import unittest

from maxpain_cvd_short_alert import FORMULA_ID, FORMULA_VERSION, render_message, select_matches


def opportunity(symbol="BTC", side="SHORT", total=80.0, short=0.8):
    sign = 1.0 if side == "SHORT" else -1.0
    flow_direction = "BULLISH" if sign > 0 else "BEARISH"
    module = {
        "available": True,
        "quality_status": "PASS",
        "freshness_status": "FRESH",
        "direction": flow_direction,
        "score": total * sign,
    }
    futures = deepcopy(module)
    futures["time_families"] = {"short": {"quality": short, "direction": flow_direction}}
    return {
        "symbol": symbol, "side": side, "timeframe": "12h", "score": 70.0,
        "current_price": 62000.5, "price_source": "BINANCE_SPOT",
        "market_evidence": {"modules": {"futures_flow": futures, "spot_flow": deepcopy(module)}},
    }


class FormulaAlertTest(unittest.TestCase):
    def test_both_pain_sides_invert_exactly_once(self):
        for pain, expected in (("SHORT", "LONG"), ("LONG", "SHORT")):
            with self.subTest(pain=pain):
                item = opportunity(side=pain)
                item["direction"] = expected
                match, = select_matches([item])
                self.assertEqual(match.direction, expected)
                self.assertEqual(match.source_side, pain)
                self.assertIs(match.item, item)

    def test_total_strict_boundary_but_mp_and_short_include_65(self):
        for side in ("SHORT", "LONG"):
            with self.subTest(side=side):
                self.assertEqual(select_matches([opportunity(side=side, total=65)]), [])
                item = opportunity(side=side, total=65.0001, short=0.65)
                item["score"] = 65
                self.assertEqual(len(select_matches([item])), 1)
                item["score"] = 64.9999
                self.assertEqual(select_matches([item]), [])

    def test_high_window_scores_cannot_replace_low_total(self):
        item = opportunity(total=40, short=1)
        for module in item["market_evidence"]["modules"].values():
            module["windows"] = {"1h": {"score": 100}, "4h": {"score": 100}}
        self.assertEqual(select_matches([item]), [])

    def test_both_total_scores_required_aligned(self):
        for key in ("futures_flow", "spot_flow"):
            for score in (-90, 0, 65):
                with self.subTest(key=key, score=score):
                    item = opportunity()
                    item["market_evidence"]["modules"][key]["score"] = score
                    self.assertEqual(select_matches([item]), [])

    def test_short_wrong_or_missing_direction_and_subthreshold_rejected(self):
        for short in ({"quality": 1, "direction": "BEARISH"}, {"quality": 1},
                      {"quality": 0.64999, "direction": "BULLISH"}, {}):
            with self.subTest(short=short):
                item = opportunity()
                item["market_evidence"]["modules"]["futures_flow"]["time_families"]["short"] = short
                self.assertEqual(select_matches([item]), [])

    def test_first_both_total_match_is_frozen_before_short_filter(self):
        first = opportunity(short=0.4)
        later = opportunity(short=1)
        self.assertEqual(select_matches([first, later]), [])
        first["market_evidence"]["modules"]["futures_flow"]["time_families"] = {}
        self.assertEqual(select_matches([first, later]), [])

    def test_noneligible_earlier_card_does_not_consume_symbol(self):
        first = opportunity(total=65)
        later = opportunity(total=80)
        self.assertIs(select_matches([first, later])[0].item, later)

    def test_one_per_symbol_original_order_and_different_coins(self):
        first, repeated = opportunity("btc"), opportunity("BTC")
        other = opportunity("SOL", side="LONG")
        matches = select_matches([first, repeated, other])
        self.assertEqual([m.symbol for m in matches], ["BTC", "SOL"])
        self.assertIs(matches[0].item, first)

    def test_missing_stale_invalid_or_unavailable_totals_rejected(self):
        variants = (("available", False), ("available", "true"), ("available", 1),
                    ("quality_status", "INVALID"), ("quality_status", None),
                    ("freshness_status", "STALE"), ("freshness_status", "UNKNOWN"),
                    ("freshness_status", None), ("direction", "BEARISH"))
        for module_name in ("futures_flow", "spot_flow"):
            for key, value in variants:
                with self.subTest(module=module_name, key=key, value=value):
                    item = opportunity()
                    item["market_evidence"]["modules"][module_name][key] = value
                    self.assertEqual(select_matches([item]), [])

    def test_warning_uses_already_adjusted_score_without_second_penalty(self):
        item = opportunity(total=70)
        for module in item["market_evidence"]["modules"].values():
            module["quality_status"] = "WARNING"
        self.assertEqual(len(select_matches([item])), 1)

    def test_nonfinite_out_of_range_bool_and_missing_numbers_rejected(self):
        for value in (None, float("nan"), float("inf"), -float("inf"), 101, -101, True, "bad"):
            for field in ("score", "futures", "spot", "short"):
                with self.subTest(value=value, field=field):
                    item = opportunity()
                    modules = item["market_evidence"]["modules"]
                    if field == "score":
                        item["score"] = value
                    elif field == "short":
                        modules["futures_flow"]["time_families"]["short"]["quality"] = value
                    else:
                        modules[f"{field}_flow"]["score"] = value
                    self.assertEqual(select_matches([item]), [])

    def test_short_quality_is_fraction_not_percent(self):
        self.assertEqual(select_matches([opportunity(short=65)]), [])

    def test_reference_price_required_no_replacement(self):
        for price in (0, -1, None, float("nan"), float("inf")):
            item = opportunity()
            item["current_price"] = price
            self.assertEqual(select_matches([item, opportunity()]), [])

    def test_explicit_target_direction_must_agree_with_inverted_pain(self):
        for side, accepted, rejected in (
            ("SHORT", ("UP", "BULLISH", "LONG", "BUY"), ("DOWN", "BEARISH", "SHORT", "NEUTRAL")),
            ("LONG", ("DOWN", "BEARISH", "SHORT", "SELL"), ("UP", "BULLISH", "LONG", "unknown")),
        ):
            for target_direction in accepted + rejected:
                with self.subTest(side=side, target_direction=target_direction):
                    item = opportunity(side=side)
                    item["target_direction"] = target_direction
                    expected_count = 1 if target_direction in accepted else 0
                    self.assertEqual(len(select_matches([item])), expected_count)

    def test_target_price_must_not_contradict_inverted_pain_no_replacement(self):
        for side, good, bad in (("SHORT", 63000, 61000), ("LONG", 61000, 63000)):
            item = opportunity(side=side)
            item["target_price"] = good
            self.assertEqual(len(select_matches([item])), 1)
            item["target_price"] = bad
            self.assertEqual(select_matches([item, opportunity(side=side)]), [])
            item["target_direction"] = "LONG" if side == "SHORT" else "SHORT"
            self.assertEqual(select_matches([item]), [])

    def test_invalid_payloads_rejected_without_mutation(self):
        item = opportunity()
        original = deepcopy(item)
        self.assertEqual(select_matches([None, {}, {"side": "NEUTRAL"}]), [])
        select_matches([item])
        self.assertEqual(item, original)

    def test_hebrew_html_renderer_escapes_data_and_converts_israel_time(self):
        item = opportunity(symbol="BTC<&>", side="LONG", total=65.0001, short=0.65)
        item["timeframe"] = "<12h>"
        item["price_source"] = "source<&>"
        match, = select_matches([item])
        message = render_message(match, datetime(2026, 9, 10, 8, 33, tzinfo=timezone.utc))
        self.assertIn("BTC&lt;&amp;&gt;", message)
        self.assertIn("&lt;12h&gt;", message)
        self.assertIn("source&lt;&amp;&gt;", message)
        self.assertIn("10.09.2026 11:33:00", message)
        self.assertIn("שורט — ירידה", message)
        self.assertIn("-65.0001", message)
        self.assertIn("62000.5", message)
        self.assertIn("ניסיוני — התאמה לנוסחת מחקר", message)
        self.assertNotIn("80.49", message)
        self.assertEqual(FORMULA_ID, "FORMULA_MP65_CVD_SHORT")
        self.assertTrue(FORMULA_VERSION)
        with self.assertRaises(ValueError):
            render_message(match, datetime(2026, 9, 10, 8, 33))


if __name__ == "__main__":
    unittest.main()
