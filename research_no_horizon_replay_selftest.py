"""Integration checks for snapshot replay provenance and computation bounds."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import research_no_horizon_replay as replay


def fixture():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    route = {"exchange": "TEST_FIXTURE", "market": "spot", "instrument": "XRP/USDT",
             "price_type": "trade", "interval_seconds": 60}
    bars = [{"open_time_utc": (base + timedelta(minutes=i)).isoformat(),
             "close_time_utc": (base + timedelta(minutes=i+1)).isoformat(),
             "open": 100, "high": 101.5, "low": 99.5, "close": 100}
            for i in range(100)]
    events = []
    for i in range(5):
        decision = (base + timedelta(minutes=10*i)).isoformat()
        events.append({"contract": {
            "candidate_id": "FIXTURE_ONLY", "candidate_version": "v1", "cohort_id": "fixture-cohort",
            "dataset_id": "fixture-dataset", "entry_id": str(i), "symbol": "XRP", "direction": "LONG",
            "decision_time_utc": decision, "reference_price": 100, "threshold_pct": 1,
            "source_route": route, "parent_policy_version": "fixture-parent-policy"},
            "btc_parent_movement_id": "fixture-parent-" + str(i), "membership_status": "LIVE",
            "parent_evidence_eligible": True, "parent_start_time_utc": decision,
            "parent_confirmed_at_utc": decision, "features_observed_at_utc": decision})
    return {"snapshot_version": replay.SNAPSHOT_VERSION, "dataset_id": "fixture-dataset",
            "cohort_id": "fixture-cohort", "source_route": route, "source_coverage_complete": True,
            "cutoff_utc": (base + timedelta(minutes=100)).isoformat(),
            "candles": bars, "opportunities": events}


class ReplayChecks(unittest.TestCase):
    def test_same_receipt_evidence_across_batch_sizes_and_input_order(self):
        source = fixture()
        one = replay.replay_snapshot(source, batch_size=1)
        source["opportunities"].reverse()
        many = replay.replay_snapshot(source, batch_size=17)
        self.assertEqual(one["outcomes"], many["outcomes"])
        self.assertEqual(one["gate"], many["gate"])
        self.assertTrue(one["gate"]["experimental_eligible"])
        self.assertFalse(one["trading_authorized"])

    def test_budget_cannot_qualify_incomplete_population(self):
        result = replay.replay_snapshot(fixture(), candle_budget=4)
        self.assertFalse(result["computation_complete"])
        self.assertEqual(result["incomplete_entry_ids"], ["4"])
        self.assertFalse(result["gate"]["experimental_eligible"])

    def test_route_mismatch_rejected_before_evaluation(self):
        source = fixture()
        source["opportunities"][0] = copy.deepcopy(source["opportunities"][0])
        source["opportunities"][0]["contract"]["source_route"]["market"] = "perpetual"
        with self.assertRaisesRegex(ValueError, "snapshot source"):
            replay.replay_snapshot(source)

    def test_missing_parent_does_not_become_independent_evidence(self):
        source = fixture()
        del source["opportunities"][0]["btc_parent_movement_id"]
        result = replay.replay_snapshot(source)
        self.assertEqual(result["status_counts"]["SUCCESS"], 5)
        self.assertFalse(result["gate"]["experimental_eligible"])

    def test_source_tail_missing_is_incomplete_not_a_loss(self):
        source = fixture()
        for bar in source["candles"]:
            bar.update(high=100.1, low=99.9)
        source["candles"] = source["candles"][:80]
        result = replay.replay_snapshot(source)
        self.assertEqual(result["status_counts"]["OPEN"], 5)
        self.assertEqual(result["status_counts"]["FAILURE"], 0)
        self.assertFalse(result["computation_complete"])
        self.assertFalse(result["gate"]["experimental_eligible"])

    def test_duplicate_entries_and_source_times_rejected(self):
        source = fixture()
        source["opportunities"].append(copy.deepcopy(source["opportunities"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate opportunity"):
            replay.replay_snapshot(source)
        source = fixture()
        source["candles"][1] = dict(source["candles"][0])
        with self.assertRaisesRegex(ValueError, "chronological"):
            replay.replay_snapshot(source)

    def test_duplicate_json_and_existing_receipt_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "input.json"
            input_path.write_text('{"x":1,"x":2}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
                replay.load_snapshot(input_path)
            input_path.write_text(json.dumps(fixture()), encoding="utf-8")
            output_path = Path(directory) / "output.json"
            output_path.write_text("earlier experiment\n", encoding="utf-8")
            process = subprocess.run([sys.executable, replay.__file__, str(input_path),
                                      "--output", str(output_path)], capture_output=True, text=True)
            self.assertEqual(process.returncode, 2)
            self.assertEqual(output_path.read_text(), "earlier experiment\n")


if __name__ == "__main__":
    unittest.main()
