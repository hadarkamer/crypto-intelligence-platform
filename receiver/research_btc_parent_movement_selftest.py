"""Causality, continuity and globally shared BTC evidence regressions."""

from datetime import datetime, timedelta, timezone
import json
import unittest

import research_btc_parent_movement as policy


START = datetime(2026, 9, 1, tzinfo=timezone.utc)


def bar(index, price):
    opened = START+timedelta(minutes=index)
    return {"open_time_utc": opened,
            "close_time_utc": opened+timedelta(minutes=1)-timedelta(milliseconds=1),
            "open": price,"high": price,"low": price,"close": price}


def advance(prices, previous=None):
    candles = [bar(i, p) for i, p in prices]
    return policy.advance_parents(candles, previous=previous,
                                  as_of_utc=candles[-1]["close_time_utc"])


class ParentMovementTests(unittest.TestCase):
    def test_reversal_is_causal_not_retroactive_pivot(self):
        parents = advance(enumerate([100, 102, 110, 109, 107.8]))
        self.assertEqual(len(parents), 2)
        left, down = parents
        self.assertFalse(left["evidence_eligible"])
        self.assertTrue(down["evidence_eligible"])
        self.assertEqual(down["direction"], "DOWN")
        self.assertEqual(down["start_time_utc"], bar(4, 107.8)["close_time_utc"])
        self.assertEqual(left["end_time_utc"], down["start_time_utc"])

    def test_one_extended_rise_never_adds_independent_parents(self):
        parents = advance([(0, 100),(1, 102),(2, 105),(3, 103.1),(4, 106),
                           (5, 104.1),(6, 110)])
        self.assertEqual(len(parents), 1)
        self.assertFalse(parents[0]["evidence_eligible"])

    def test_no_duration_gate_and_no_return_to_entry_requirement(self):
        parents = advance(enumerate([100, 102, 120, 117.6, 120]))
        self.assertEqual(len(parents), 3)
        self.assertEqual([x["direction"] for x in parents], ["UP","DOWN","UP"])
        self.assertTrue(all(x["evidence_eligible"] for x in parents[1:]))
        self.assertLess(parents[-1]["start_time_utc"]-parents[0]["start_time_utc"],
                        timedelta(minutes=5))

    def test_chunking_restart_and_json_checkpoint_preserve_identity(self):
        prices = list(enumerate([100,102,110,107.8,109,106,108.2]))
        expected = advance(prices)
        for split in range(1,len(prices)):
            first = advance(prices[:split])
            checkpoint = json.loads(json.dumps(first[-1], default=str))
            second = advance(prices[split:], previous=checkpoint)
            actual = {x["btc_parent_movement_id"]:x for x in first+second}
            self.assertEqual(set(actual), {x["btc_parent_movement_id"] for x in expected})
            for item in expected:
                other = actual[item["btc_parent_movement_id"]]
                self.assertEqual(other["evidence_eligible"],item["evidence_eligible"])
                self.assertEqual(other["state_json"],item["state_json"])

    def test_future_price_never_changes_prior_membership(self):
        candles = [bar(i,p) for i,p in enumerate([100,102,110,107.8,106,108.2])]
        prefix = policy.advance_parents(candles[:4], as_of_utc=candles[3]["close_time_utc"])
        extended = policy.advance_parents(candles, as_of_utc=candles[-1]["close_time_utc"])
        decision = candles[3]["close_time_utc"]+timedelta(seconds=15)
        event = {"event_id":1,"alert_time_utc":decision}
        before = policy.membership(event,parent=prefix[-1],btc_bar=candles[3])
        after = policy.membership(event,parent=extended[1],btc_bar=candles[3])
        self.assertEqual(before,after)
        self.assertEqual(before["membership_status"],"LIVE")

    def test_all_assets_directions_horizons_and_repeat_alerts_share_parent(self):
        parents = advance(enumerate([100,102,110,107.8,107,106.5]))
        parent = parents[-1]
        identities = set()
        for index in (3,4,5):
            for symbol in ("BTC","ETH","SOL"):
                for direction in ("LONG","SHORT"):
                    result = policy.membership(
                        {"event_id":index+1,"alert_time_utc":bar(index,100)["close_time_utc"],
                         "symbol":symbol,"direction":direction,"window_minutes":1440},
                        parent=parent,btc_bar=bar(index,100))
                    self.assertEqual(result["membership_status"],"LIVE")
                    identities.add(result["btc_parent_movement_id"])
        self.assertEqual(len(identities),1)

    def test_gap_does_not_manufacture_eligible_boundary(self):
        parents = advance([(0,100),(1,102),(2,110),(3,107.8),(10,80),(11,82),
                           (12,84),(13,82.32)])
        self.assertEqual(len(parents),4)
        self.assertTrue(parents[1]["evidence_eligible"])
        self.assertFalse(parents[2]["evidence_eligible"])
        self.assertEqual(parents[2]["boundary_reason"],"BTC_DATA_GAP")
        self.assertTrue(parents[3]["evidence_eligible"])
        missing = policy.membership({"event_id":1,"alert_time_utc":START+timedelta(minutes=8)},
                                    parent=parents[1],btc_bar=bar(3,107.8))
        self.assertEqual(missing["membership_status"],"BTC_DATA_MISSING")

    def test_left_edge_remains_ineligible_until_first_complete_reversal(self):
        parent = advance(enumerate([100,98,94]))[-1]
        result = policy.membership({"event_id":1,"alert_time_utc":bar(2,94)["close_time_utc"]},
                                   parent=parent,btc_bar=bar(2,94))
        self.assertEqual(result["membership_status"],"BOUNDARY_UNVERIFIED")
        self.assertEqual(parent["direction"],"DOWN")

    def test_future_duplicate_out_of_order_and_malformed_bars_rejected(self):
        examples = [
            ([bar(1,100)],bar(0,100)["close_time_utc"]),
            ([bar(0,100),bar(0,100)],bar(1,100)["close_time_utc"]),
            ([bar(1,100),bar(0,100)],bar(1,100)["close_time_utc"]),
            ([bar(0,100)|{"high":99}],bar(1,100)["close_time_utc"]),
            ([bar(0,100)|{"close":float("nan")}],bar(1,100)["close_time_utc"]),
            ([bar(0,100)|{"open":True}],bar(1,100)["close_time_utc"]),
        ]
        for candles,cutoff in examples:
            with self.assertRaises(ValueError):
                policy.advance_parents(candles,as_of_utc=cutoff)

    def test_exact_age_and_confirmed_boundary_fail_closed(self):
        parent = advance(enumerate([100,102,110,107.8]))[-1]
        candle = bar(3,107.8)
        for decision in (candle["close_time_utc"]-timedelta(milliseconds=1),
                         candle["close_time_utc"]+timedelta(minutes=1)):
            result = policy.membership({"event_id":1,"alert_time_utc":decision},
                                       parent=parent,btc_bar=candle)
            self.assertEqual(result["membership_status"],"BTC_DATA_MISSING")


if __name__ == "__main__":
    unittest.main()
