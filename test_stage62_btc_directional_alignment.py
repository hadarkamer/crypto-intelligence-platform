import unittest

import alert_engine


class Stage62DirectionalAlignmentTests(unittest.TestCase):
    def test_aligned_btc_score_is_continuous(self):
        result = alert_engine._directional_alignment(
            "SOL", 5, 7, "LONG", {"side": "LONG", "score": 73.4}
        )
        self.assertAlmostEqual(result["consensus_points"], 10.71, places=2)
        self.assertAlmostEqual(result["btc_approval_points"], 11.01, places=2)
        self.assertAlmostEqual(result["total"], 21.72, places=2)

    def test_opposite_btc_score_applies_continuous_penalty(self):
        result = alert_engine._directional_alignment(
            "SOL", 5, 7, "SHORT", {"side": "LONG", "score": 73.4}
        )
        self.assertAlmostEqual(result["consensus_points"], 10.71, places=2)
        self.assertAlmostEqual(result["btc_conflict_penalty"], 7.34, places=2)
        self.assertAlmostEqual(result["total"], 3.37, places=2)

    def test_btc_uses_consensus_only(self):
        result = alert_engine._directional_alignment(
            "BTC", 4, 7, "SHORT", None
        )
        self.assertAlmostEqual(result["consensus_points"], 17.14, places=2)
        self.assertEqual(result["btc_relation"], "SELF")
        self.assertAlmostEqual(result["total"], 17.14, places=2)

    def test_market_is_not_part_of_directional_score(self):
        result = alert_engine._directional_alignment(
            "ETH", 4, 7, "LONG", {"side": "LONG", "score": 80}
        )
        self.assertNotIn("market_points", result)
        self.assertAlmostEqual(result["total"], 20.57, places=2)


if __name__ == "__main__":
    unittest.main()
