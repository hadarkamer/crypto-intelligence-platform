"""Source identity, timestamp, cutoff and existing-import safeguards (no network)."""

import html
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import research_telegram_html_archive as archive


def message(identifier, title, text):
    body = html.escape(text).replace("\n", "<br>")
    return f'<div class="message default clearfix" id="message{identifier}"><div class="body"><div class="date details" title="{title}"></div><div class="text">{body}</div></div></div>'


class ArchiveTests(unittest.TestCase):
    def prepare(self, documents, **kwargs):
        with TemporaryDirectory() as directory:
            paths = []
            for index, document in enumerate(documents):
                path = Path(directory) / f"messages{index}.html"
                path.write_text(document, encoding="utf-8")
                paths.append(path)
            return archive.prepare_archive(paths, chat_key=kwargs.pop("chat_key", "chat:123"), time_policy=kwargs.pop("time_policy", "israel-wall-clock-v1"), **kwargs)

    def test_two_periods_use_israel_midnight_and_exclude_aug15(self):
        documents = [
            message(1, "15.08.2026 23:59:59 UTC+03:00", "a")
            + message(2, "16.08.2026 00:00:00 UTC+03:00", "b")
            + message(3, "03.09.2026 23:59:59 UTC+03:00", "c")
            + message(4, "04.09.2026 00:00:00 UTC+03:00", "d")
        ]
        manifest, rows = self.prepare(documents)
        self.assertEqual(manifest["excluded_before_20260816"], 1)
        self.assertEqual(len(rows), 3)
        self.assertEqual([len(row["period_scope_ids"]) for row in rows], [1, 1, 2])
        self.assertEqual(rows[-1]["normalized_message_time_utc"], "2026-09-03T21:00:00Z")
        self.assertFalse(manifest["periods_are_independent_validation"])

    def test_header_offset_and_israel_interpretation_are_both_preserved(self):
        _, rows = self.prepare([message(1, "05.09.2026 08:38:27 UTC+02:00", "body")])
        row = rows[0]
        self.assertEqual(row["header_message_time_utc"], "2026-09-05T06:38:27Z")
        self.assertEqual(row["normalized_message_time_utc"], "2026-09-05T05:38:27Z")
        self.assertEqual(row["time_status"], "OFFSET_DISAGREEMENT_REVIEW_REQUIRED")
        self.assertFalse(row["event_time_verified"])
        _, alternative = self.prepare([message(1, "05.09.2026 08:38:27 UTC+02:00", "body")], time_policy="html-offset-v1")
        self.assertEqual(alternative[0]["normalized_message_time_utc"], "2026-09-05T06:38:27Z")
        self.assertEqual(row["source_identity_key"], alternative[0]["source_identity_key"])

    def test_reexport_dedup_identity_is_stable_and_content_changes_quarantine(self):
        original = message(1, "16.08.2026 10:00:00 UTC+03:00", "first")
        revision = message(1, "16.08.2026 10:00:00 UTC+03:00", "edited")
        manifest, rows = self.prepare([original, original, revision])
        self.assertEqual(manifest["unique_source_messages"], 1)
        self.assertEqual(manifest["exact_duplicate_copies_removed"], 1)
        self.assertEqual(manifest["conflicting_source_identities"], 1)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["import_action"] == "QUARANTINE_REVISION_CONFLICT" for row in rows))
        _, other_chat = self.prepare([original], chat_key="chat:456")
        self.assertNotEqual(rows[0]["source_identity_key"], other_chat[0]["source_identity_key"])

    def test_existing_sept_import_links_source_id_and_text_not_price_or_scan(self):
        payload = {"events": [["old-event", "old-snapshot", "message1,message2", "", "", "", "", "", "", "first\ncontinuation"]]}
        links = archive.existing_links(payload, "chat:123")
        manifest, rows = self.prepare([
            message(1, "04.09.2026 10:00:00 UTC+03:00", "first")
            + message(2, "04.09.2026 10:00:03 UTC+03:00", "continuation")
            + message(3, "04.09.2026 10:00:03 UTC+03:00", "continuation")
        ], known_links=links)
        self.assertEqual(manifest["existing_linked_snapshot_count"], 1)
        self.assertEqual(manifest["existing_linked_event_count"], 1)
        self.assertEqual([row["import_action"] for row in rows], ["LINK_EXISTING_SOURCE", "LINK_EXISTING_SOURCE", "STAGE_NEW_SOURCE"])
        _, conflict = self.prepare([message(1, "04.09.2026 10:00:00 UTC+03:00", "changed source")], known_links=links)
        self.assertEqual(conflict[0]["import_action"], "QUARANTINE_EXISTING_TEXT_CONFLICT")

    def test_missing_timestamp_is_never_open_or_in_a_period(self):
        manifest, rows = self.prepare([message(1, "bad timestamp", "unusable timing")])
        self.assertEqual(rows[0]["time_status"], "DATA_MISSING")
        self.assertEqual(rows[0]["import_action"], "QUARANTINE_MISSING_TIME")
        self.assertEqual(rows[0]["period_scope_ids"], [])
        self.assertEqual(manifest["v7_outcomes_created"], 0)

    def test_source_messages_do_not_gain_features_waves_or_eligibility(self):
        manifest, rows = self.prepare([
            message(1, "04.09.2026 10:00:00 UTC+03:00", "✅ סריקת Watch Top 8 #1")
            + message(2, "04.09.2026 10:00:03 UTC+03:00", "עוצמה 🚨 Futures CVD\nBTC 🔴 SHORT\nציון 90")
        ])
        self.assertEqual(manifest["signal_source_messages"], 1)
        self.assertTrue(all(row["btc_parent_movement_id"] is None for row in rows))
        self.assertTrue(all(row["watch_scan_id"] is None for row in rows))
        self.assertTrue(all(row["analysis_direction"] is None for row in rows))
        self.assertTrue(all(not row["candidate_eligible"] for row in rows))
        self.assertEqual(manifest["formula_evidence_rows"], 0)

    def test_html_line_breaks_and_entities_are_preserved(self):
        parser = archive._Messages()
        parser.feed('<div class="message default" id="message1"><div class="date details" title="x"></div><div class="text">A &amp; B<br/><strong>second</strong></div></div>')
        self.assertEqual(parser.rows[0]["message_text"], "A & B\nsecond")

    def test_embedded_age_is_evidence_without_promoting_time(self):
        manifest, rows = self.prepare([message(1, "16.08.2026 00:05:54 UTC+02:00", "CVD עד: 2026-08-15 21:00 UTC | גיל בפועל 5 דק׳")])
        audit = manifest["embedded_utc_age_audit"]
        self.assertEqual(audit["source_messages_with_dated_utc_and_age"], 1)
        self.assertAlmostEqual(audit["normalized_residual_minutes_min"], 0.9)
        self.assertAlmostEqual(audit["header_residual_minutes_min"], 60.9)
        self.assertFalse(audit["exact_emission_time_verified"])
        self.assertFalse(rows[0]["event_time_verified"])


if __name__ == "__main__":
    unittest.main()
