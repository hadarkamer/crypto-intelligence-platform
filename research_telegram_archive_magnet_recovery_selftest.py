"""Causal Magnet recovery rejects adjacency, conflicts and altered provenance."""
from copy import deepcopy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from research_telegram_archive_features import extract_message, FEATURE_VERSION as BASE_FEATURE_VERSION
from research_telegram_archive_magnet_recovery import extract_magnet_message, run
from research_telegram_html_archive import stage_digest

MANIFEST = {"time_policy": "israel-wall-clock-v1", "embedded_utc_age_audit": {"source_messages_with_dated_utc_and_age": 2183, "normalized_residual_minutes_min": 0, "header_residual_minutes_min": 60}}


def source(extra="", heading="מגנט 🔺 עליון #1"):
    return {
        "message_text": heading + "\n" + extra + "\nאזור: $100 – $101\nטווחים: 12h, 24h\nאיכות Magnet Quality: 79.57/100\nפיזור Spread: 0.4085%\nנזילות Liquidity Edge: +9.21%\nמקור נזילות: הטווח הרחב בקלאסטר 24h\nPrice+OI: תומך (עולה) | ציון +65.00/100\nFutures CVD: סותר (יורד) | ציון -70.00/100\nSpot: ניטרלי (ניטרלי) | ציון +0.00/100\nמסקנה: ❌ Magnet עדיין לא מאומת",
        "message_family": "MAGNET_TARGET", "source_identity_key": "source", "source_revision_sha256": "revision",
        "source_message_id": "message1", "source_chat_key": "chat1", "period_scope_ids": ["ALL_COMPATIBLE_SINCE_20260816"],
        "existing_source_links": [], "raw_time_title": "16.08.2026 03:00:15 UTC+02:00",
        "header_message_time_utc": "2026-08-16T01:00:15Z", "import_action": "STAGE_NEW_SOURCE", "identity_status": "EXACT_SOURCE_ID",
    }


class MagnetRecoveryTests(unittest.TestCase):
    def test_upper_is_long_and_components_are_actual_same_message_totals(self):
        event = extract_magnet_message(source("נכס: BTC"), MANIFEST)
        self.assertEqual(event["symbol"], "BTC")
        self.assertEqual(event["analysis_direction"], "LONG")
        self.assertFalse(event["display_direction_inverted"])
        self.assertEqual(event["reconstruction_status"], "READY_FOR_SPOT_ENTRY_PATH")
        self.assertEqual(event["features"]["magnet.quality"], 79.57)
        self.assertEqual(event["features"]["magnet.timeframes"], ["12h", "24h"])
        self.assertEqual(event["features"]["futures_cvd.aligned_score"], -70)
        self.assertEqual(event["features"]["spot_cvd.total_score"], 0)
        self.assertIn("Price+OI:", event["field_source_lines"]["price_oi.signed_total_score"])

    def test_lower_is_short_without_maxpain_inversion(self):
        event = extract_magnet_message(source("נכס BTC", "מגנט 🔻 תחתון #2"), MANIFEST)
        self.assertEqual(event["analysis_direction"], "SHORT")
        self.assertEqual(event["features"]["futures_cvd.aligned_score"], 70)
        self.assertEqual(event["features"]["price_oi.aligned_score"], -65)

    def test_adjacent_header_joined_group_and_legacy_links_cannot_supply_asset(self):
        row = source()
        row.update({"previous_message_text": "מגנט 🧲 BTC — Magnet V1", "class": "message default clearfix joined", "symbol": "BTC", "existing_source_links": [{"existing_symbol": "BTC", "existing_event_id": "old-guessed-event"}]})
        event = extract_magnet_message(row, MANIFEST)
        self.assertIsNone(event["symbol"])
        self.assertEqual(event["analysis_direction"], "LONG")
        self.assertEqual(event["missing_evidence"], ["UNAMBIGUOUS_SYMBOL_IN_SAME_MESSAGE"])
        self.assertEqual(event["calculation_status"], "BLOCKED_SOURCE_EVIDENCE")

    def test_conflicting_assets_or_direction_stay_blocked(self):
        for extra, heading, reason in [
            ("נכס BTC\nנכס: ETH", "מגנט 🔺 עליון #1", "CONFLICTING_MAGNET_SYMBOLS"),
            ("נכס BTC | 🔴 SHORT", "מגנט 🔺 עליון #1", "MAGNET_HEADING_DIRECTION_CONFLICT"),
            ("נכס: BTC | 🔴 SHORT", "מגנט 🔺 עליון #1", "MAGNET_HEADING_DIRECTION_CONFLICT"),
            ("נכס BTC", "מגנט 🔺 תחתון #1", "MISSING_OR_CONFLICTING_MAGNET_HEADING"),
        ]:
            with self.subTest(reason=reason):
                event = extract_magnet_message(source(extra, heading), MANIFEST)
                self.assertIn(reason, event["conflicts"])
                self.assertEqual(event["reconstruction_status"], "BLOCKED_SOURCE_EVIDENCE")

    def test_conflicting_numeric_field_is_missing_not_arbitrarily_chosen(self):
        row = source("נכס BTC")
        row["message_text"] += "\nאיכות Magnet Quality: 99/100"
        event = extract_magnet_message(row, MANIFEST)
        self.assertNotIn("magnet.quality", event["features"])
        self.assertIn("CONFLICTING_PRINTED_FIELD:magnet.quality", event["conflicts"])

    def test_raw_source_and_old_feature_contract_are_immutable(self):
        row = source("נכס BTC")
        original = deepcopy(row)
        base = extract_message(row, MANIFEST)
        event = extract_magnet_message(row, MANIFEST)
        self.assertEqual(row, original)
        self.assertEqual(event["base_archive_event_key"], base["archive_event_key"])
        self.assertNotEqual(event["archive_event_key"], base["archive_event_key"])
        self.assertEqual(extract_message(row, MANIFEST)["feature_version"], BASE_FEATURE_VERSION)
        self.assertEqual(event["source_message_time_utc"], base["source_message_time_utc"])
        self.assertFalse(event["live_union_eligible"])
        self.assertFalse(event["candidate_eligible"])

    def test_impossible_component_or_target_range_cannot_enter_measurement(self):
        for old, new, reason in [("+65.00/100", "+165.00/100", "SIGNED_SCORE_OUT_OF_RANGE:price_oi.signed_total_score"), ("$100 – $101", "$102 – $101", "REVERSED_MAGNET_TARGET_RANGE")]:
            row = source("נכס: BTC")
            row["message_text"] = row["message_text"].replace(old, new)
            event = extract_magnet_message(row, MANIFEST)
            self.assertIn(reason, event["conflicts"])
            self.assertEqual(event["reconstruction_status"], "BLOCKED_SOURCE_EVIDENCE")

    def test_unknown_asset_future_time_or_quarantine_not_unblocked(self):
        for text in ("נכס: MISSING", "נכס: BTC\nCVD עד: 2026-08-16 00:30 UTC | גיל בפועל 5"):
            self.assertEqual(extract_magnet_message(source(text), MANIFEST)["reconstruction_status"], "BLOCKED_SOURCE_EVIDENCE")
        row = source("נכס BTC");row["identity_status"] = "SOURCE_ID_REVISION_CONFLICT"
        self.assertEqual(extract_magnet_message(row, MANIFEST)["reconstruction_status"], "BLOCKED_SOURCE_EVIDENCE")

    def test_audit_preserves_source_and_reports_residual_instead_of_price_labels(self):
        with TemporaryDirectory() as directory:
            root = Path(directory);stage = root / "stage";stage.mkdir()
            rows = [source()];manifest = deepcopy(MANIFEST)
            manifest["prepared_stage_digest"] = stage_digest(manifest, rows)
            source_path = stage / "archive_source_messages.jsonl"
            source_path.write_text(json.dumps(rows[0]) + "\n")
            (stage / "archive_manifest.json").write_text(json.dumps(manifest))
            before = source_path.read_bytes()
            report = run(stage_dir=stage, output_dir=root / "out", expected_stage_digest=manifest["prepared_stage_digest"])
            self.assertEqual(report["remaining_blocked_events"], 1)
            self.assertEqual(report["source_recovered_events"], 0)
            self.assertEqual(report["outcomes_created"], 0)
            self.assertEqual(source_path.read_bytes(), before)
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                run(stage_dir=stage, output_dir=root / "bad", expected_stage_digest="0" * 64)


if __name__ == "__main__":
    unittest.main()
