import unittest

import alert_engine


def btc_rows():
    rows = []
    price = 100.0
    plan = [
        ("12h", "LONG", 99.0),
        ("24h", "LONG", 99.1),
        ("48h", "LONG", 98.9),
        ("3d", "SHORT", 103.0),
        ("1w", "SHORT", 103.1),
        ("2w", "SHORT", 102.3),
        ("1m", "SHORT", 102.5),
    ]
    for timeframe, side, target in plan:
        rows.append({
            "symbol": "BTC",
            "timeframe": timeframe,
            "current_price": price,
            "rank": 1,
            "short_max_pain": target if side == "SHORT" else 105.0,
            "long_max_pain": target if side == "LONG" else 95.0,
            "distance_short_pct": (target - price) if side == "SHORT" else 5.0,
            "distance_long_pct": (target - price) if side == "LONG" else -5.0,
            "short_liquidation_amount": 1_000_000.0,
            "long_liquidation_amount": 900_000.0,
        })
    return rows


class Stage57CalculationTests(unittest.TestCase):
    def test_consensus_is_side_specific(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=100)
        for item in items:
            if item["side"] == "LONG":
                self.assertEqual(item["consensus_hits"], 3)
                self.assertAlmostEqual(item["components"]["consensus"], 12.86, places=2)
            else:
                self.assertEqual(item["consensus_hits"], 4)
                self.assertAlmostEqual(item["components"]["consensus"], 17.14, places=2)

    def test_cluster_is_side_specific(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=100)
        for item in items:
            if item["side"] == "LONG":
                self.assertEqual(item["cluster_same_direction_count"], 3)
                self.assertEqual(set(item["cluster_members"]), {"12h", "24h", "48h"})
            else:
                self.assertEqual(item["cluster_same_direction_count"], 4)
                self.assertEqual(set(item["cluster_members"]), {"3d", "1w", "2w", "1m"})

    def test_duplicates_are_removed(self):
        rows = btc_rows()
        rows.append(dict(rows[0]))
        report = alert_engine.debug_symbol(rows, "BTC")
        self.assertEqual(report["duplicates_removed"], 1)
        self.assertEqual(len(report["items"]), 7)

    def test_component_sum_matches_score(self):
        for item in alert_engine.build_opportunities(btc_rows(), limit=100):
            self.assertEqual(item["calculation_validation_errors"], [])
            self.assertAlmostEqual(item["component_sum_check"], item["score"], places=2)


if __name__ == "__main__":
    unittest.main()
