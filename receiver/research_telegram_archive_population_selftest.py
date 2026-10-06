"""Archive union ancestry, provenance, wave pooling and native isolation gates."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import unittest

from research_telegram_archive_hype_supplement_selftest import HypeSupplementTests, NOW, mark_bars
from research_telegram_archive_reconstruction_selftest import ENTRY, cache
from research_telegram_archive_backfill import calculate_event, canonical
from research_telegram_archive_runtime_importer_selftest import artifact
from research_telegram_archive_population import population
from research_archive_price_policy import POLICY_VERSION, aggregate_archive_wave_metrics, MARK_SOURCE, SPOT_SOURCE
from research_formula_ordered_v7 import ordered_outcome_evidence, summarize_scope
import research_telegram_archive_runtime_importer as importer
import research_telegram_archive_hype_supplement as hype


class ArchivePopulationTests(HypeSupplementTests):
    # Shared fixture only; inherited tests retain the existing supplement gates.
    def union(self, report):
        return population(self.base, run_key=self.base_run, expected_sha256=self.sha,
            supplement_database=Path(report["sqlite_artifact"]),
            supplement_run_key=report["run_key"], supplement_sha256=report["sqlite_sha256"])

    def test_exact_ancestor_replacement_preserves_unique_population(self):
        report = self.run_supplement(limit=2)
        with self.union(report) as (events, load, names, audit):
            self.assertEqual(audit["hype_ancestors_replaced"], 2)
            self.assertEqual(audit["duplicate_supplement_messages_added"], 0)
            self.assertEqual(audit["validated_supplement_labels"], 128)
            for original in self.originals:
                key = original["archive_event_key"]
                self.assertEqual(events[key]["base_archive_event_key"], key)
                self.assertEqual(names[key], MARK_SOURCE)
                self.assertEqual(len(load(key)[0]), 64)
            self.assertEqual(len(events), audit["effective_unique_source_messages"])
        self.assertEqual(importer.file_sha256(self.base), self.sha)

    def test_summary_writes_both_periods_and_tracks_replaced_population(self):
        from research_telegram_archive_summary import summarize
        report = self.run_supplement(limit=2)
        summary = summarize(self.base, run_key=self.base_run, expected_sha256=self.sha,
            supplement_database=Path(report["sqlite_artifact"]), supplement_run_key=report["run_key"],
            supplement_sha256=report["sqlite_sha256"], observed_at=NOW, output=self.root/"analysis")
        self.assertEqual(summary["population"]["hype_ancestors_replaced"], 2)
        self.assertEqual(len(summary["periods"]), 2)
        self.assertFalse(summary["research_ready"])
        self.assertEqual(summary["production_rows_written"], 0)

    def test_source_feature_mutation_is_rejected_even_with_updated_file_digest(self):
        report = self.run_supplement(limit=2)
        with sqlite3.connect(report["sqlite_artifact"]) as conn:
            key, encoded = conn.execute("SELECT event_key,event_json FROM archive_reconstructed_events LIMIT 1").fetchone()
            changed = json.loads(encoded)
            changed["features"]["maxpain.selected_score"] = 99
            conn.execute("UPDATE archive_reconstructed_events SET event_json=? WHERE event_key=?", (canonical(changed), key))
        report["sqlite_sha256"] = importer.file_sha256(Path(report["sqlite_artifact"]))
        with self.assertRaisesRegex(ValueError, "changed original source"):
            with self.union(report):
                pass

    def test_incomplete_population_is_not_silently_added(self):
        report = self.run_supplement(limit=1)
        with self.assertRaisesRegex(ValueError, "incomplete events"):
            with self.union(report):
                pass

    def test_archive_policy_accepts_mark_but_native_default_and_wrong_source_do_not(self):
        event = hype.derive_event(self.originals[0], base_run_key=self.base_run)
        event, labels, metrics = hype.calculate_event(event, mark_bars(), observed_at=NOW)
        label = next(row for row in labels if row["signal_variant"] == "NORMAL" and row["window_minutes"] == 60 and row["threshold_bps"] == 25)
        row = {**event, "event_id": 1, "direction": event["analysis_direction"],
            "alert_time_utc": event["entry_time_utc"], "ordered_outcome": {**label, "event_id": 1},
            "features_observed_at_utc": event["source_message_time_utc"], "membership_status": "LIVE",
            "parent_evidence_eligible": True, "episode_policy_version": hype.parent_policy.POLICY_VERSION}
        self.assertIn("UNVERIFIED_DATA_QUALITY", ordered_outcome_evidence(row, analysis_as_of_utc=NOW)["exclusion_reasons"])
        self.assertTrue(ordered_outcome_evidence(row, analysis_as_of_utc=NOW, archive_price_policy=POLICY_VERSION)["eligible"])
        # Repeating an event within the same wave never adds a second vote.
        repeated = {**row, "event_id": 2, "ordered_outcome": {**label, "event_id": 2}}
        result = summarize_scope([row, repeated], analysis_as_of_utc=NOW, archive_price_policy=POLICY_VERSION)
        self.assertEqual(result["decision_waves"], 1)
        self.assertEqual(len(result["episodes"]), 1)
        self.assertTrue(result["episodes"][0]["eligible"])
        for key, value in (("source_scope", "LIVE"), ("symbol", "BTC"), ("live_union_eligible", True)):
            bad = {**row, key: value}
            self.assertFalse(ordered_outcome_evidence(bad, analysis_as_of_utc=NOW, archive_price_policy=POLICY_VERSION)["eligible"])
        bad = deepcopy(row)
        bad["ordered_outcome"]["source_price_kind"] = "LAST"
        self.assertFalse(ordered_outcome_evidence(bad, analysis_as_of_utc=NOW, archive_price_policy=POLICY_VERSION)["eligible"])
        mark = next(m for m in metrics if m["signal_variant"] == "NORMAL" and m["window_minutes"] == 60)
        aggregate = aggregate_archive_wave_metrics([[mark, mark]], window_minutes=60, threshold_bps=25)
        self.assertEqual(aggregate["expected_representatives"], 1)
        self.assertEqual(aggregate["valid_representatives"], 1)
        self.assertEqual(aggregate["source_member_counts"], {MARK_SOURCE: 2})
        bad_metric = {**mark, "source": {**hype.SOURCE, "price_kind": "LAST"}}
        bad = aggregate_archive_wave_metrics([[mark, bad_metric]], window_minutes=60, threshold_bps=25)
        self.assertFalse(bad["coverage_complete"])


if __name__ == "__main__":
    unittest.main()
