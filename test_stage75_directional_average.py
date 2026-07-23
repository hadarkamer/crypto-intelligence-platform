import unittest

import alert_engine
from test_stage57_calculations import btc_rows


class Stage75DirectionalAverageTests(unittest.TestCase):
    def test_average_is_separate_for_long_and_short(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=100)
        long_values = {
            item["average_score_all_timeframes"]
            for item in items if item["side"] == "LONG"
        }
        short_values = {
            item["average_score_all_timeframes"]
            for item in items if item["side"] == "SHORT"
        }
        self.assertEqual(len(long_values), 1)
        self.assertEqual(len(short_values), 1)
        self.assertNotEqual(long_values, short_values)
        for item in items:
            expected = (
                item["average_score_long"]
                if item["side"] == "LONG"
                else item["average_score_short"]
            )
            self.assertEqual(item["average_score_all_timeframes"], expected)

    def test_each_direction_has_scores_across_timeframes(self):
        items = alert_engine.build_opportunities(btc_rows(), limit=100)
        sample = items[0]
        matrix = sample["directional_scores_all_timeframes"]
        self.assertEqual(set(matrix), {"LONG", "SHORT"})
        self.assertEqual(len(matrix["LONG"]), 7)
        self.assertEqual(len(matrix["SHORT"]), 7)

    def test_btc_confirmation_uses_same_timeframe_score_not_average(self):
        rows = btc_rows()
        for row in list(rows):
            alt = dict(row)
            alt["symbol"] = "ETH"
            alt["rank"] = 2
            rows.append(alt)
        items = alert_engine.build_opportunities(rows, limit=100)
        btc_by_tf = {
            item["timeframe"]: item
            for item in items if item["symbol"] == "BTC"
        }
        for item in items:
            if item["symbol"] != "ETH":
                continue
            btc_item = btc_by_tf[item["timeframe"]]
            self.assertEqual(item["btc_reference_score"], btc_item["score"])
            # This explicitly guards against replacing the timeframe score
            # with the directional all-timeframe average.
            if btc_item["score"] != btc_item["average_score_all_timeframes"]:
                self.assertNotEqual(
                    item["btc_reference_score"],
                    btc_item["average_score_all_timeframes"],
                )


if __name__ == "__main__":
    unittest.main()
