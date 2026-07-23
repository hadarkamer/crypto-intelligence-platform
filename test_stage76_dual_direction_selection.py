import unittest

import alert_engine
from test_stage57_calculations import btc_rows


class Stage76DualDirectionSelectionTests(unittest.TestCase):
    def test_selected_direction_has_highest_complete_score(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=100)
        self.assertTrue(items)
        for item in items:
            opposite = item.get("opposite_score")
            if opposite is not None:
                self.assertGreaterEqual(float(item["score"]), float(opposite))
                self.assertAlmostEqual(
                    float(item["directional_edge"]),
                    float(item["score"]) - float(opposite),
                    places=2,
                )

    def test_btc_reference_uses_selected_full_score_same_timeframe(self):
        rows = btc_rows()
        for row in list(rows):
            alt = dict(row)
            alt["symbol"] = "ETH"
            alt["rank"] = 2
            rows.append(alt)
        items = alert_engine.build_opportunities(rows, limit=100)
        btc_by_tf = {
            item["timeframe"]: item for item in items if item["symbol"] == "BTC"
        }
        for item in items:
            if item["symbol"] != "ETH":
                continue
            reference = btc_by_tf[item["timeframe"]]
            self.assertEqual(item["btc_reference_side"], reference["side"])
            self.assertEqual(item["btc_reference_score"], reference["score"])

    def test_opposite_direction_average_is_available(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=100)
        for item in items:
            self.assertIn("opposite_average_score_all_timeframes", item)
            self.assertIsNotNone(item["opposite_average_score_all_timeframes"])


if __name__ == "__main__":
    unittest.main()
