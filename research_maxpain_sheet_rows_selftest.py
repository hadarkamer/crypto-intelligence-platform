"""Frozen source-to-Sheet semantics, event identity and missing data checks."""
from copy import deepcopy
import unittest
import google_sheets_sync
from google_sheets_sync_selftest import Event
import research_maxpain_sheet_rows as rows
import research_snapshot_sync_worker as worker


class MaxPainRowsTests(unittest.TestCase):
    def event(self):
        event = Event().to_dict()
        event.update(event_id=1, event_kind="ALERT", delivery_status="DELIVERED", timeframe="24h")
        event["engine_snapshot"].update({
            "directional_scores_all_timeframes": {"LONG": {"24h": 40, "48h": 0}, "SHORT": {"24h": 78, "48h": 65}},
            "score_components": {"relative_gap": 12, "target_proximity": 20, "consensus": 26, "cluster_confidence": 20},
            "maxpain_timeframes": [
                {"timeframe": "24h", "source_side": "SHORT", "score": 78, "target_price": 101, "near_amount": 700, "far_amount": 300},
                {"timeframe": "24h", "source_side": "LONG", "score": 40, "target_price": 99, "near_amount": 300},
            ],
        })
        return event

    def test_every_observed_timeframe_with_actual_selected_components(self):
        event = self.event()
        frozen = deepcopy(event)
        upserts = rows.build_rows(event)
        self.assertEqual(event, frozen)
        self.assertEqual([item["row"]["timeframe"] for item in upserts], ["24h", "48h"])
        selected, other = [item["row"] for item in upserts]
        self.assertEqual(selected["long_score"], 78)
        self.assertEqual(selected["short_score"], 40)
        self.assertEqual(selected["long_maxpain_price"], 101)
        self.assertEqual(selected["short_maxpain_price"], 99)
        self.assertEqual(selected["source_side"], "SHORT")
        self.assertEqual(selected["selected_side"], "LONG")
        self.assertEqual(selected["gap_score"], 12)
        self.assertEqual(selected["consensus_score"], 26)
        self.assertTrue(selected["is_alert_timeframe"])
        self.assertEqual(selected["timestamp_utc"], event["alert_time_utc"])
        self.assertEqual(other["selected_score"], 65)
        self.assertEqual(other["opposite_score"], 0)
        self.assertIsNone(other["score_ratio"])
        self.assertIsNone(other["gap_score"])
        self.assertIsNone(other["selected_liquidity_usd"])
        self.assertIsNone(other["long_maxpain_price"])
        self.assertFalse(other["is_alert_timeframe"])
        self.assertEqual(other["quality_status"], "FROZEN_TOTALS_ONLY")

    def test_same_scan_does_not_collapse_independent_event_timeframes(self):
        first = self.event()
        second = deepcopy(first)
        second.update(event_id=2, event_fingerprint="second", alert_time_utc="2026-09-05T09:31:00Z")
        rebuilt = worker.rebuild_alert_group([first, second])
        tf = [item for item in rebuilt if item["sheet"] == "MaxPain_TF"]
        self.assertEqual(len(tf), 4)
        self.assertEqual(len({(item["row"]["event_id"], item["row"]["timeframe"]) for item in tf}), 4)
        self.assertEqual(len({item["row"]["snapshot_id"] for item in tf}), 1)
        self.assertTrue(all(item["key"] == "event_id,timeframe" for item in tf))

    def test_combined_selected_components_and_no_average_substitution(self):
        event = self.event()
        event["event_type"] = "COMBINED_CONFIRMATION"
        event["engine_snapshot"] = {"top_item_components": {"target_proximity": 11}, "top_item_average_score_all_timeframes": 99}
        selected = rows.build_rows(event)[0]["row"]
        self.assertEqual(selected["proximity_score"], 11)
        self.assertEqual(selected["selected_score"], 78)
        self.assertIsNone(selected["opposite_score"])
        self.assertIsNone(selected["cluster_score"])
        self.assertEqual(rows.build_rows(dict(event, event_type="OI_PRICE_HIGH")), [])

    def test_new_capture_absence_is_not_overwritten_by_legacy_scorer_zero(self):
        event = self.event()
        snapshot = event["engine_snapshot"]
        snapshot.update(near_amount=0, far_amount=0, near_share_pct=0)
        snapshot["maxpain_timeframes"] = [{"timeframe": "24h", "source_side": "SHORT", "score": 78}]
        selected = rows.build_rows(event)[0]["row"]
        self.assertIsNone(selected["selected_liquidity_usd"])
        self.assertIsNone(selected["opposite_liquidity_usd"])
        self.assertIsNone(selected["selected_liquidity_share_pct"])

    def test_normalization_preserves_original(self):
        self.assertEqual(rows.classification("WATCH_CANDIDATE", "🎯 Max Pain\n#1 BTC"), "MAX_PAIN_ALERT")
        self.assertEqual(rows.classification("WATCH_CANDIDATE", "unrelated"), "WATCH_CANDIDATE")
        self.assertEqual(rows.classification("MAGNET_ALERT", "Max Pain"), "MAGNET_ALERT")
        payload = google_sheets_sync.build_delivered_event_payload(self.event())
        telegram = next(item["row"] for item in payload["upserts"] if item["sheet"] == "Telegram_Events")
        self.assertEqual(telegram["source_record_type"], "MAX_PAIN_ALERT")
        self.assertEqual(telegram["record_type"], "MAX_PAIN_ALERT")

    def test_conflicting_duplicate_observations_fail_and_nonfinite_stays_blank(self):
        event = self.event()
        event["engine_snapshot"]["maxpain_timeframes"].append(event["engine_snapshot"]["maxpain_timeframes"][0])
        with self.assertRaises(ValueError):
            rows.build_rows(event)
        self.assertIsNone(rows.number(float("nan")))
        self.assertIsNone(rows.number(True))


if __name__ == "__main__":
    unittest.main()
