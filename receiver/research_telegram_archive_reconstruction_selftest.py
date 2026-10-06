"""Causality, direction, source identity and ordered archive measurement gates."""
from datetime import datetime, timedelta, timezone
import json
import unittest
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest.mock import patch

from research_telegram_archive_features import extract_message, ENTRY_VERSION
from research_telegram_archive_backfill import SpotCache, calculate_event, canonical, evaluator_features, initialize, iter_evidence_rows

UTC = timezone.utc
ENTRY = datetime(2026, 8, 16, 0, 1, tzinfo=UTC)


def source(text, family="WATCH_CANDIDATE"):
    return {"message_text": text, "message_family": family, "source_identity_key": "source", "source_revision_sha256": "revision", "source_message_id": "message1", "source_chat_key": "chat1", "period_scope_ids": ["ALL_COMPATIBLE_SINCE_20260816"], "existing_source_links": [], "raw_time_title": "16.08.2026 03:00:15 UTC+02:00", "header_message_time_utc": "2026-08-16T01:00:15Z", "import_action": "STAGE_NEW_SOURCE", "identity_status": "EXACT_SOURCE_ID"}


MANIFEST = {"time_policy": "israel-wall-clock-v1", "embedded_utc_age_audit": {"source_messages_with_dated_utc_and_age": 2183, "normalized_residual_minutes_min": 0, "header_residual_minutes_min": 60}}


def cache(high=103.0, low=99.9):
    result = object.__new__(SpotCache)
    result.bars = {"BTC": [{"open_time_utc": ENTRY + timedelta(minutes=i), "close_time_utc": ENTRY + timedelta(minutes=i+1, milliseconds=-1), "open": 100.0, "close": 100.0, "high": high, "low": low, "volume": 1} for i in range(1440)]}
    result.opens = {"BTC": [bar["open_time_utc"] for bar in result.bars["BTC"]]}
    return result


class ReconstructionTests(unittest.TestCase):
    def test_archive_fetch_cannot_relabel_an_endpoint_override_as_binance(self):
        import research_telegram_archive_backfill as backfill
        with patch.object(backfill.binance_spot_price_path, "BINANCE_SPOT_BASE_URL", "https://alternative.invalid"):
            with self.assertRaisesRegex(ValueError, "reviewed official Binance Spot"):
                cache().extend("BTC", ENTRY, ENTRY+timedelta(minutes=1))

    def test_maxpain_inverts_once_and_total_is_never_tf_score(self):
        text = "#1 BTC / 24h | 🔴 SHORT | 78.1\nמחיר נוכחי: $100\nסיכום Futures: ציון +68.0/100\n🟢 30m: +1M$ | עוצמה 99/100\nקונצנזוס Gap: 5/6\nGap\n9.4 / 15"
        event = extract_message(source(text), MANIFEST)
        self.assertEqual(event["analysis_direction"], "LONG")
        self.assertEqual(event["features"]["futures_cvd.total_score"], 68)
        self.assertNotIn("spot_cvd.total_score", event["features"])
        self.assertEqual(event["features"]["maxpain.consensus_hits"], 5)
        self.assertEqual(event["features"]["maxpain.consensus_total"], 6)
        self.assertEqual(event["features"]["maxpain.gap_score"], 9.4)
        self.assertEqual(event["original_price_source"], "TELEGRAM_PRINTED_QUOTE_EXCHANGE_UNSPECIFIED")
        self.assertEqual(event["entry_time_utc"], ENTRY.isoformat())

    def test_standalone_spot_group_is_not_total(self):
        event = extract_message(source("ספוט 🚨 Spot CVD חזק\nנכס BTC\n🟢 עכשיו | עוצמה 90/100", "SPOT_CVD_STRONG"), MANIFEST)
        self.assertEqual(event["analysis_direction"], "LONG")
        self.assertEqual(event["features"]["spot_cvd.group_score"], 90)
        self.assertNotIn("spot_cvd.total_score", event["features"])

    def test_missing_symbol_and_future_embedded_time_stay_blocked(self):
        event = extract_message(source("מגנט עליון\nאיכות 80", "MAGNET_TARGET"), MANIFEST)
        self.assertEqual(event["reconstruction_status"], "BLOCKED_SOURCE_EVIDENCE")
        text = "#1 BTC / 24h | 🔴 SHORT | 78\nCVD עד: 2026-08-16 00:30 UTC | גיל בפועל 5"
        event = extract_message(source(text), MANIFEST)
        self.assertIn("EMBEDDED_CVD_TIME_AFTER_SOURCE_MESSAGE", event["conflicts"])
        self.assertEqual(event["reconstruction_status"], "BLOCKED_SOURCE_EVIDENCE")

    def test_new_entry_freezes_before_path_and_retests_inverse(self):
        event = extract_message(source("#1 BTC / 24h | 🔴 SHORT | 78\nמחיר נוכחי: $88"), MANIFEST)
        measured, outcomes, full = calculate_event(event, cache(), [], observed_at=ENTRY+timedelta(days=2))
        self.assertEqual(measured["entry_price"], 100)
        self.assertEqual(measured["original_printed_price"], 88)
        self.assertEqual(measured["entry_policy_version"], ENTRY_VERSION)
        self.assertEqual(len(outcomes), 64)
        self.assertEqual(len({label["outcome_id"] for label in outcomes}), 64)
        self.assertTrue(all(label["status"] == "SUCCESS" for label in outcomes if label["signal_variant"] == "NORMAL"))
        self.assertTrue(all(label["status"] == "FAILURE" for label in outcomes if label["signal_variant"] == "INVERSE"))
        self.assertEqual(len(full), 8)
        self.assertTrue(all(row["status"] == "READY" for row in full))
        self.assertEqual(measured["membership_status"], "BTC_DATA_MISSING")

    def test_both_touches_ambiguous_and_missing_tail_not_open(self):
        event = extract_message(source("#1 BTC / 24h | 🔴 SHORT | 78"), MANIFEST)
        _, outcomes, _ = calculate_event(event, cache(low=97), [], observed_at=ENTRY+timedelta(days=2))
        self.assertTrue(all(label["first_touch_side"] == "AMBIGUOUS" and label["success"] is None for label in outcomes))
        partial = cache()
        partial.bars["BTC"].pop()
        partial.opens["BTC"].pop()
        _, outcomes, metrics = calculate_event(event, partial, [], observed_at=ENTRY+timedelta(days=2))
        self.assertTrue(all(label["status"] == "DATA_MISSING" for label in outcomes if label["window_minutes"] == 1440))
        self.assertTrue(all(metric["status"] == "DATA_MISSING" and metric["mfe_pct"] is None for metric in metrics if metric["window_minutes"] == 1440))

    def test_future_entry_and_unsupported_hype_cannot_take_other_prices(self):
        event = extract_message(source("#1 BTC / 24h | 🔴 SHORT | 78"), MANIFEST)
        measured, labels, metrics = calculate_event(event, cache(), [], observed_at=ENTRY-timedelta(seconds=1))
        self.assertEqual(measured["calculation_status"], "NOT_YET_ENTRY")
        self.assertIsNone(measured["entry_price"])
        self.assertEqual(labels, [])
        event["symbol"] = "HYPE"
        measured, labels, _ = calculate_event(event, cache(), [], observed_at=ENTRY+timedelta(days=2))
        self.assertEqual(measured["calculation_status"], "UNSUPPORTED_SPOT_SYMBOL")
        self.assertEqual(labels, [])

    def test_inverse_preserves_source_predicates_and_missing_labels_are_visible(self):
        event = extract_message(source("#1 BTC / 24h | 🔴 SHORT | 78\nסיכום Futures: ציון +68/100"), MANIFEST)
        self.assertEqual(evaluator_features(event)["futures_cvd.aligned_score"], 68)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "empty-labels.sqlite"
            with sqlite3.connect(path) as conn:
                initialize(conn)
                conn.execute("INSERT INTO archive_reconstructed_events(run_key,event_key,source_time_utc,symbol,reconstruction_status,event_json) VALUES(?,?,?,?,?,?)", ("run", event["archive_event_key"], event["source_message_time_utc"], "BTC", "READY_FOR_SPOT_ENTRY_PATH", canonical(event)))
            rows = list(iter_evidence_rows(path, run_key="run"))
            self.assertEqual(len(rows), 64)
            self.assertEqual(len({row["event_id"] for row in rows}), 2)
            self.assertTrue(all(row["decision_features"]["futures_cvd.aligned_score"] == 68 for row in rows))
            self.assertTrue(all(row["ordered_outcome"]["status"] == "DATA_MISSING" for row in rows))
            self.assertTrue(all("method_version" not in row["ordered_outcome"] for row in rows))

    def test_candidate_summary_selects_wave_before_labels_and_keeps_period_scope(self):
        import research_telegram_archive_summary as summary
        import research_telegram_archive_runtime_importer as importer
        # The summary now validates the original archive contract before any
        # cohort selection. Keep this fixture versioned like a real artifact.
        contract = {"backfill_version": importer.BACKFILL_VERSION, "prepared_stage_digest": "a" * 64,
            "entry_policy_version": importer.ENTRY_VERSION, "feature_version": importer.FEATURE_VERSION,
            "direction_version": importer.DIRECTION_VERSION, "time_version": importer.TIME_VERSION,
            "parent_policy_version": importer.parent_policy.POLICY_VERSION, "source_scope": "ARCHIVE_ONLY",
            "threshold_bps": list(range(25, 201, 25)), "window_minutes": list(importer.WINDOWS),
            "variants": ["NORMAL", "INVERSE"], "cache_sha256": "b" * 64,
            "live_union_eligible": False, "phase": "DISCOVERY", "formula_relevance": "NOT_EVALUATED"}
        run_key = importer._hash(contract)
        event = extract_message(source("#1 BTC / 24h | 🔴 SHORT | 78\nסיכום Futures: ציון +68/100"), MANIFEST)
        event, labels, metrics = calculate_event(event, cache(), [], observed_at=ENTRY+timedelta(days=2))
        event.update({"membership_status":"LIVE", "parent_evidence_eligible":True, "parent_start_time_utc":ENTRY.isoformat(), "btc_parent_movement_id":"wave1"})
        candidate = {"formula_id":"FUTURES_CVD_TOTAL_65", "conditions":[{"feature":"futures_cvd.aligned_score","operator":">=","value":65}], "repeat_count":1}
        with TemporaryDirectory() as directory:
            root=Path(directory);path=root/"data.sqlite"
            with sqlite3.connect(path) as conn:
                initialize(conn)
                conn.execute("INSERT INTO archive_reconstruction_runs VALUES (?,?)", (run_key, canonical(contract)))
                conn.execute("INSERT INTO archive_reconstructed_events VALUES(?,?,?,?,?,?,?)", (run_key,event["archive_event_key"],event["source_message_time_utc"],"BTC","READY_FOR_SPOT_ENTRY_PATH",event["calculation_status"],canonical(event)))
                conn.executemany("INSERT INTO archive_delayed_entry_outcomes VALUES(?,?,?,?,?,?,?,?)", [(run_key,event["archive_event_key"],label["signal_variant"],label["window_minutes"],label["threshold_bps"],label["outcome_id"],label["status"],canonical(label)) for label in labels])
                conn.executemany("INSERT INTO archive_common_window_metrics VALUES(?,?,?,?,?,?)", [(run_key,event["archive_event_key"],metric["signal_variant"],metric["window_minutes"],metric["status"],canonical(metric)) for metric in metrics])
            with patch.object(summary,"candidate_catalog",return_value=[candidate]):
                report=summary.summarize(path,run_key=run_key,expected_sha256=importer.file_sha256(path),observed_at=ENTRY+timedelta(days=2),output=root/"summary")
            self.assertEqual(report["maximum_decisive_independent_waves"],1)
            self.assertEqual(report["nonempty_cells_tested"],128)
            self.assertFalse(report["research_ready"])
            from research_telegram_archive_audit import audit
            audit_report=audit(path,run_key=run_key,observed_at=ENTRY+timedelta(days=2),output=root/"audit.json")
            self.assertTrue(audit_report["valid"])
            self.assertEqual(audit_report["paired_normal_inverse_cells"],32)
            cells=[json.loads(line) for line in (root/"summary"/"archive_candidate_cells.jsonl").read_text().splitlines()]
            self.assertEqual(len({cell["scope_key"] for cell in cells}),128)
            self.assertTrue(all(cell["fresh_decision_waves"]==1 for cell in cells))


if __name__ == "__main__":
    unittest.main()
