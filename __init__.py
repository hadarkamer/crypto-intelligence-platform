import unittest
import alert_engine


def btc_rows(with_duplicate=False):
    rows = []
    for index, timeframe in enumerate(alert_engine.TIMEFRAMES):
        side = "LONG" if index < 3 else "SHORT"
        row = {
            "symbol": "BTC",
            "timeframe": timeframe,
            "current_price": 100.0,
            "rank": 1,
            "short_max_pain": 103.0 if side == "SHORT" else 110.0,
            "long_max_pain": 99.0 if side == "LONG" else 90.0,
            "distance_short_pct": 3.0 if side == "SHORT" else 10.0,
            "distance_long_pct": -1.0 if side == "LONG" else -10.0,
            "short_liquidation_amount": 1_000_000 * (index + 1),
            "long_liquidation_amount": 900_000 * (index + 1),
        }
        rows.append(row)
    if with_duplicate:
        rows.append(dict(rows[0]))
    return rows


class Stage57ScoringTests(unittest.TestCase):
    def test_consensus_is_scored_per_alert_direction(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=20)
        by_tf = {item["timeframe"]: item for item in items}
        self.assertEqual(by_tf["12h"]["side"], "LONG")
        self.assertEqual(by_tf["12h"]["consensus_hits"], 3)
        self.assertEqual(by_tf["12h"]["components"]["consensus"], 9.86)
        self.assertEqual(by_tf["3d"]["side"], "SHORT")
        self.assertEqual(by_tf["3d"]["consensus_hits"], 4)
        self.assertEqual(by_tf["3d"]["components"]["consensus"], 13.14)

    def test_cluster_is_scoped_to_alert_direction(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=20)
        by_tf = {item["timeframe"]: item for item in items}
        self.assertEqual(by_tf["12h"]["cluster_same_direction_count"], 3)
        self.assertLessEqual(by_tf["12h"]["cluster_count"], 3)
        self.assertEqual(by_tf["3d"]["cluster_same_direction_count"], 4)
        self.assertLessEqual(by_tf["3d"]["cluster_count"], 4)

    def test_duplicate_symbol_timeframe_is_removed(self):
        items = alert_engine.build_opportunities(btc_rows(with_duplicate=True), limit=20)
        pairs = [(item["symbol"], item["timeframe"]) for item in items]
        self.assertEqual(len(pairs), 7)
        self.assertEqual(len(set(pairs)), 7)

    def test_component_sum_and_average_are_consistent(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=20)
        expected_average = round(sum(item["score"] for item in items) / 7, 2)
        for item in items:
            components = item["components"]
            component_sum = round(
                components["directional_alignment"]
                + components["target_proximity"]
                + components["cluster_confidence"]
                + components["relative_gap"],
                2,
            )
            self.assertEqual(component_sum, item["score"])
            self.assertEqual(item["average_score_all_timeframes"], expected_average)
            self.assertTrue(item["calculation_valid"])
            self.assertEqual(item["calculation_validation_errors"], [])


if __name__ == "__main__":
    unittest.main()
