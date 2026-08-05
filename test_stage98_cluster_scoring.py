import unittest

import alert_engine


def row(tf, target, amount, side="SHORT", price=100.0):
    return {
        "symbol": "BTC",
        "timeframe": tf,
        "current_price": price,
        "rank": 1,
        "short_max_pain": target if side == "SHORT" else 105.0,
        "long_max_pain": target if side == "LONG" else 95.0,
        "distance_short_pct": target - price if side == "SHORT" else 5.0,
        "distance_long_pct": target - price if side == "LONG" else -5.0,
        "short_liquidation_amount": amount,
        "long_liquidation_amount": amount,
    }


class Stage98ClusterScoringTests(unittest.TestCase):
    def test_coverage_is_fully_linear_from_two_to_seven(self):
        expected = {
            0: 0.0,
            1: 0.0,
            2: 10.0 / 6.0,
            3: 20.0 / 6.0,
            4: 5.0,
            5: 40.0 / 6.0,
            6: 50.0 / 6.0,
            7: 10.0,
        }
        for count, value in expected.items():
            self.assertAlmostEqual(alert_engine._coverage_points(count), value, places=3)

    def test_two_timeframes_can_form_cluster(self):
        cluster = alert_engine._cluster_for_side([
            row("12h", 101.0, 100.0),
            row("24h", 101.4, 120.0),
        ], "SHORT")
        self.assertEqual(cluster["count"], 2)
        self.assertGreater(cluster["density_points"], 0.0)
        self.assertAlmostEqual(cluster["coverage_points"], 1.67, places=2)

    def test_cluster_uses_full_spread_not_median_membership(self):
        # Chained distances fit around neighbours, but full width exceeds 1%.
        cluster = alert_engine._cluster_for_side([
            row("12h", 101.0, 100.0),
            row("24h", 101.7, 120.0),
            row("48h", 102.4, 150.0),
        ], "SHORT")
        self.assertEqual(cluster["count"], 2)
        self.assertLessEqual(cluster["spread_pct"], 1.0)

    def test_density_is_continuous_out_of_ten(self):
        cluster = alert_engine._cluster_for_side([
            row("12h", 101.0, 100.0),
            row("24h", 101.5, 120.0),
            row("48h", 102.0, 150.0),
        ], "SHORT")
        expected = max(0.0, 1.0 - cluster["spread_pct"] / 1.0) * 10.0
        self.assertAlmostEqual(cluster["density_points"], round(expected, 2), places=2)


    def test_multiple_independent_clusters_are_reported(self):
        cluster = alert_engine._cluster_for_side([
            row("12h", 100.1, 100.0),
            row("24h", 100.4, 130.0),
            row("48h", 103.0, 200.0),
            row("3d", 103.5, 250.0),
        ], "SHORT")
        self.assertEqual(cluster["candidate_cluster_count"], 2)
        self.assertEqual(len(cluster["candidate_clusters"]), 2)

    def test_growth_only_applies_through_multiplier(self):
        cluster = alert_engine._cluster_for_side([
            row("12h", 101.0, 100.0),
            row("24h", 101.1, 115.0),
            row("48h", 101.2, 138.0),
        ], "SHORT")
        raw = cluster["density_points"] + cluster["coverage_points"]
        self.assertAlmostEqual(
            cluster["points"],
            round(min(30.0, raw * cluster["liquidity_multiplier"]), 2),
            places=2,
        )


if __name__ == "__main__":
    unittest.main()
