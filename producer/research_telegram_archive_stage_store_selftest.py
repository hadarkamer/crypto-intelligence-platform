"""Review-digest, eligibility, resumable intake and persistence guards."""

from copy import deepcopy
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import research_telegram_archive_stage_store as store
import research_telegram_html_archive as archive
from research_telegram_html_archive_selftest import message


class StageStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        html = self.path / "messages.html"
        html.write_text(message(1, "16.08.2026 00:05:00 UTC+02:00", "first") + message(2, "04.09.2026 10:00:00 UTC+02:00", "second"), encoding="utf-8")
        self.manifest, self.rows = archive.prepare_archive([html], chat_key="chat:123", time_policy="israel-wall-clock-v1")
        self.apply_args = dict(expected_digest=self.manifest["archive_revision_digest"], expected_stage_digest=self.manifest["prepared_stage_digest"], batch_size=1)

    def test_annotation_link_and_manifest_tampering_invalidates_review_digest(self):
        for field, value in (("existing_source_links", [{"existing_snapshot_id": "injected"}]), ("import_action", "LINK_EXISTING_SOURCE"), ("normalized_message_time_israel", "2026-09-04T10:00:00+03:00")):
            with self.subTest(field=field):
                rows = deepcopy(self.rows)
                rows[0][field] = value
                with self.assertRaisesRegex(ValueError, "Prepared stage digest"):
                    store.validate_stage(self.manifest, rows)
        manifest = deepcopy(self.manifest)
        manifest["existing_linked_event_count"] = 99
        with self.assertRaisesRegex(ValueError, "Prepared stage digest"):
            store.validate_stage(manifest, self.rows)

    def test_resealed_changed_intake_still_requires_new_explicit_review(self):
        manifest = deepcopy(self.manifest)
        manifest["existing_import_sha256"] = "different-proof"
        manifest["prepared_stage_digest"] = archive.stage_digest(manifest, self.rows)
        target = self.path / "must-not-exist.sqlite"
        with self.assertRaisesRegex(ValueError, "Expected prepared stage digest"):
            store.apply_sqlite_stage(target, manifest, self.rows, **self.apply_args)
        self.assertFalse(target.exists())

    def test_resealing_cannot_promote_evidence_or_change_computed_metadata(self):
        for field, value in (("candidate_eligible", True), ("training_eligible", True), ("record_mode", "LIVE"), ("event_time_verified", True), ("analysis_direction", "LONG"), ("message_family", "MADE_UP"), ("period_scope_ids", ["SINCE_20260904"]), ("normalized_message_time_utc", "2026-09-04T07:00:00Z")):
            with self.subTest(field=field):
                rows, manifest = deepcopy(self.rows), deepcopy(self.manifest)
                rows[0][field] = value
                manifest["prepared_stage_digest"] = archive.stage_digest(manifest, rows)
                with self.assertRaises(ValueError):
                    store.validate_stage(manifest, rows)

    def test_sqlite_is_idempotent_resumable_and_keeps_both_cutoffs(self):
        target = self.path / "sources.sqlite"
        with patch.object(store, "_database_url", side_effect=AssertionError("SQLite must not read production credentials")):
            first = store.apply_sqlite_stage(target, self.manifest, self.rows, **self.apply_args)
            second = store.apply_sqlite_stage(target, self.manifest, self.rows, **self.apply_args)
        self.assertEqual(first["source_rows_inserted"], 2)
        self.assertEqual(second["source_rows_inserted"], 0)
        self.assertEqual(second["batch_members_inserted"], 0)
        self.assertEqual(second["production_rows_inserted"], 0)
        with sqlite3.connect(target) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM archive_source_messages WHERE candidate_eligible != 0 OR canonical_message_time_utc IS NOT NULL").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM archive_intake_members WHERE period_scope_ids_json LIKE '%ALL_COMPATIBLE_SINCE_20260816%'").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM archive_intake_members WHERE period_scope_ids_json LIKE '%SINCE_20260904%'").fetchone()[0], 1)
            conn.execute("DELETE FROM archive_intake_members WHERE source_identity_key = ?", (self.rows[0]["source_identity_key"],))
            conn.execute("UPDATE archive_intake_batches SET intake_status = 'INGESTING'")
        resumed = store.apply_sqlite_stage(target, self.manifest, self.rows, **self.apply_args)
        self.assertEqual(resumed["source_rows_inserted"], 0)
        self.assertEqual(resumed["batch_members_inserted"], 1)
        self.assertEqual(resumed["persisted_batch_members"], 2)

    def test_existing_persisted_content_is_verified_not_silently_ignored(self):
        target = self.path / "sources.sqlite"
        store.apply_sqlite_stage(target, self.manifest, self.rows, **self.apply_args)
        with sqlite3.connect(target) as conn:
            conn.execute("UPDATE archive_source_messages SET message_text = 'corrupted'")
        with self.assertRaisesRegex(ValueError, "Persisted raw source"):
            store.apply_sqlite_stage(target, self.manifest, self.rows, **self.apply_args)

    def test_postgres_apply_is_opt_in_and_validates_review_before_connecting(self):
        with patch.dict("os.environ", {}, clear=True), patch.object(store, "_database_url", side_effect=AssertionError("must not connect")):
            with self.assertRaisesRegex(RuntimeError, "RESEARCH_ARCHIVE_IMPORT_APPLY"):
                store.apply_stage(self.manifest, self.rows, **self.apply_args)


if __name__ == "__main__":
    unittest.main()
