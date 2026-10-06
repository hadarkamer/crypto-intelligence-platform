"""Network-free regression tests for exact-target MaxPain cluster attribution."""
import copy
import math
import unittest

import alert_engine as engine
import counter_score


def row(tf, target, amount, *, opposite=90.0, symbol="HYPE"):
    return dict(symbol=symbol, timeframe=tf, rank=20, current_price=100.0,
                short_max_pain=target, long_max_pain=opposite,
                distance_short_pct=target-100.0, distance_long_pct=opposite-100.0,
                short_liquidation_amount=amount, long_liquidation_amount=100.0)


def fixture():
    return [row("12h", 101, 100), row("24h", 101.05, 120),
            row("48h", 105, 100), row("3d", 105.01, 140),
            row("1w", 110, 100)]


class TargetClusterScoreTests(unittest.TestCase):
    def test_unrelated_best_does_not_hide_weaker_relevant_cluster(self):
        rows = fixture()
        global_best = engine._cluster_for_side(rows, "SHORT")
        local = engine._cluster_for_target(rows[0], "SHORT", rows)
        self.assertEqual(global_best["members"], ["48h", "3d"])
        self.assertEqual(local["members"], ["12h", "24h"])
        self.assertGreater(global_best["points"], local["points"])
        self.assertGreater(local["points"], 0)
        self.assertTrue(local["target_is_member"])
        self.assertEqual(local["scoring_scope"], "TARGET_EXACT_PRICE_V1")
        self.assertEqual(local["member_targets"], [101, 101.05])

    def test_isolated_target_has_zero_all_cluster_components(self):
        rows = fixture()
        local = engine._cluster_for_target(rows[-1], "SHORT", rows)
        for key in ("points", "density_points", "coverage_points", "growth_points",
                    "liquidity_multiplier", "count", "candidate_cluster_count"):
            self.assertEqual(local[key], 0, key)
        self.assertEqual(local["members"], [])
        self.assertFalse(local["target_is_member"])

    def test_same_price_other_timeframe_qualifies_but_bounds_do_not(self):
        rows = fixture()
        # This TF supports LONG; its upper target is not in the SHORT universe.
        rows[-1] = row("1w", 101.0, 100, opposite=99.8)
        local = engine._cluster_for_target(rows[-1], "SHORT", rows)
        self.assertEqual(local["members"], ["12h", "24h"])
        self.assertNotIn("1w", local["members"])
        self.assertGreater(local["points"], 0)
        rows[-1]["short_max_pain"] = 101.025  # inside bounds, not a member quote
        local = engine._cluster_for_target(rows[-1], "SHORT", rows)
        self.assertEqual(local["points"], 0)
        self.assertFalse(local["target_is_member"])
        rows[-1]["short_max_pain"] = math.nextafter(101.0, math.inf)
        self.assertEqual(engine._cluster_for_target(rows[-1], "SHORT", rows)["points"], 0)

    def test_rank_all_subwindows_with_target_not_only_maximal_window(self):
        rows = [row("12h", 101, 100), row("24h", 101.05, 140),
                row("48h", 101.1, 141)]
        local = engine._cluster_for_target(rows[0], "SHORT", rows)
        self.assertEqual(local["members"], ["12h", "24h"])
        third = engine._cluster_for_target(rows[2], "SHORT", rows)
        self.assertIn("48h", third["members"])
        self.assertIn(101.1, third["member_targets"])
        self.assertLess(third["points"], local["points"])

    def test_unrelated_or_missing_evidence_fails_closed(self):
        rows = fixture()
        other_coin = [{**r, "symbol": "BTC"} for r in rows]
        self.assertEqual(engine._cluster_for_target(rows[0], "SHORT", other_coin)["points"], 0)
        self.assertEqual(engine._cluster_for_target(rows[0], "SHORT", [rows[0]])["points"], 0)
        for target in (None, "", "invalid", float("nan"), float("inf"), 0, -1):
            invalid = {**rows[0], "short_max_pain": target}
            self.assertEqual(engine._cluster_for_target(invalid, "SHORT", rows)["points"], 0)

    def test_opposite_direction_symmetric_and_counter_matches_full_score(self):
        rows = fixture()
        for item in rows:
            item["long_max_pain"] = 200-item["short_max_pain"]
            item["distance_long_pct"] = item["long_max_pain"]-100
            item["long_liquidation_amount"] = item["short_liquidation_amount"]
            item["short_max_pain"], item["distance_short_pct"] = 120, 20
        local = engine._cluster_for_target(rows[0], "LONG", rows)
        self.assertEqual(local["members"], ["12h", "24h"])
        counter = counter_score.calculate_counter_score(
            {"symbol": "HYPE", "timeframe": "12h", "side": "SHORT"}, rows)
        details = engine._score_details_for_side(rows[0], "LONG", engine._consensus_map(rows),
                                                {}, engine._cluster_map(rows), rows)
        self.assertEqual(counter["score"], details["score"])
        self.assertEqual(counter["components"]["cluster_confidence"], local["points"])
        self.assertEqual(counter["cluster_member_targets"], [99, 98.95])

    def test_capture_scalar_forced_side_averages_and_no_input_mutation(self):
        rows = fixture()
        original = copy.deepcopy(rows)
        captured = {"HYPE": []}
        items = engine.build_opportunities(rows, limit=100, forced_symbol="HYPE",
                                          forced_side="SHORT", capture_by_symbol=captured)
        consensus, clusters = engine._consensus_map(rows), engine._cluster_map(rows)
        scores = []
        for item in items:
            source = next(r for r in rows if r["timeframe"] == item["timeframe"])
            scalar = engine._score_explicit_side(source, "SHORT", consensus, clusters, rows)
            slot = next(s for s in captured["HYPE"] if s["timeframe"] == item["timeframe"]
                        and s["source_side"] == "SHORT")
            self.assertEqual(scalar, item["score"])
            self.assertEqual(slot["score"], item["score"])
            self.assertEqual(slot["components"], item["components"])
            self.assertEqual(slot["cluster_scoring_scope"], "TARGET_EXACT_PRICE_V1")
            self.assertEqual(slot["cluster_member_targets"], item["cluster_member_targets"])
            self.assertEqual(item["calculation_validation_errors"], [])
            if item["components"]["cluster_confidence"] > 0:
                self.assertIn(item["target_price"], item["cluster_member_targets"])
            scores.append(item["score"])
        self.assertEqual(items[0]["average_score_all_timeframes"], round(sum(scores)/len(scores), 2))
        self.assertEqual(rows, original)


if __name__ == "__main__":
    unittest.main()
