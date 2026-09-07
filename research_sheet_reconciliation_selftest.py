"""Actual readback proves coverage; successful write ACKs alone cannot."""
from datetime import datetime, timezone
from unittest.mock import patch
import unittest
import research_sheet_reconciliation as audit


class Result:
    rowcount = 1


class Connection:
    def __init__(self): self.calls = []
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, args):
        self.calls.append((sql, args))
        return Result()


class AuditTests(unittest.TestCase):
    def auditor(self):
        instance = audit.SheetReconciler()
        instance.start = datetime(2026, 9, 4, tzinfo=timezone.utc)
        instance.cutoff = datetime(2026, 9, 7, tzinfo=timezone.utc)
        instance.expected = {key: {"event_fingerprint": key, "event_type": kind} for key, kind in [
            ("present", "MAX_PAIN_ALERT"), ("duplicate", "COMBINED_CONFIRMATION"), ("missing", "MAX_PAIN_ALERT")]}
        return instance

    def row(self, key, status="DELIVERED", timestamp="2026-09-05T00:00:00Z"):
        return {"event_id": key, "verification_status": status, "timestamp_utc": timestamp}

    def test_missing_duplicate_extra_and_imported_are_distinguished(self):
        a = self.auditor()
        self.assertFalse(a.consume_page({"start_row": 2, "next_row": 5, "last_row": 8, "complete": False,
            "rows": [self.row("present"), self.row("duplicate"), self.row("missing", "IMPORTED")]}))
        self.assertTrue(a.consume_page({"start_row": 5, "next_row": 9, "last_row": 8, "complete": True,
            "rows": [self.row("duplicate"), self.row("unexpected"), self.row("demo", "DEMO"), self.row("new", timestamp="2026-09-08T00:00:00Z")]}))
        s = a.summarize()
        self.assertEqual((s["delivered"], s["sheet_unique"], s["missing"], s["duplicate_ids"], s["unexpected_delivered_ids"]), (3, 2, 1, 1, 1))
        self.assertEqual(s["by_type"]["MAX_PAIN_ALERT"], {"delivered": 2, "sheet_unique": 1, "missing": 1})
        conn = Connection()
        with patch.object(audit.research_sheet_outbox, "_connect", return_value=conn):
            a._finish("test")
        self.assertEqual(len(conn.calls), 1)
        self.assertEqual(conn.calls[0][1], ('["missing"]',))
        self.assertIn("sync_status='SYNCED'", conn.calls[0][0])
        self.assertNotIn("_missing_ids", a.status()["last_complete"])
        self.assertEqual(a.status()["status"], "GAPS_FOUND")

    def test_incomplete_or_discontinuous_page_never_certifies(self):
        a = self.auditor()
        for page in [
            {"start_row": 3, "next_row": 3, "last_row": 8, "complete": False, "rows": []},
            {"start_row": 2, "next_row": 3, "last_row": 8, "complete": True, "rows": [self.row("present")]},
        ]:
            with self.assertRaises(ValueError): a.consume_page(page)
        self.assertIsNone(a.status()["last_complete"])

    def test_remote_failure_does_not_repair_or_report_match(self):
        a = self.auditor()
        with patch.object(audit.google_sheets_sync, "read_telegram_audit_page", side_effect=ValueError("V3_REQUIRED")), \
             patch.object(a, "_finish") as finish:
            result = a.run_due("test")
        self.assertEqual(result["status"], "AUDIT_FAILED")
        self.assertEqual(result["last_error"], "V3_REQUIRED")
        finish.assert_not_called()
        self.assertIsNone(a.expected)

    def test_empty_sheet_is_true_missing_and_never_future_row(self):
        a = self.auditor()
        self.assertTrue(a.consume_page({"start_row": 2, "next_row": 2, "last_row": 1, "complete": True, "rows": []}))
        self.assertEqual(a.summarize()["missing"], 3)


if __name__ == "__main__": unittest.main()
