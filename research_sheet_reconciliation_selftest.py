"""Actual readback proves coverage; successful write ACKs alone cannot."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from unittest.mock import patch
import unittest
import research_sheet_reconciliation as audit


class Result:
    def __init__(self, rowcount=1): self.rowcount = rowcount


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
        self.assertEqual(conn.calls[0][1], (['["missing"]'],))
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

    def test_busy_after_partial_page_preserves_population_and_resumes_exact_cursor(self):
        a = self.auditor()
        population = a.expected
        a.consume_page({"start_row": 2, "next_row": 3, "last_row": 4, "complete": False,
                        "rows": [self.row("present")]})
        with patch.object(audit.google_sheets_sync, "read_telegram_audit_page",
                          side_effect=audit.google_sheets_sync.SheetReceiverBusy("SHEET_RECEIVER_BUSY")) as request, \
             patch.object(a, "_begin") as begin, patch.object(a, "_finish") as finish:
            status = a.run_due("test")
        request.assert_called_once_with(start_row=3, last_row=4)
        begin.assert_not_called()
        finish.assert_not_called()
        self.assertIs(a.expected, population)
        self.assertEqual(a.seen, {"present": 1})
        self.assertEqual((a.next_row, a.last_row), (3, 4))
        self.assertEqual(status["status"], "DEFERRED_RECEIVER_BUSY")
        self.assertIsNone(status["last_complete"])
        self.assertIsNone(status["last_error"])
        a.next_attempt = 0
        with patch.object(audit.google_sheets_sync, "read_telegram_audit_page", return_value={
                "start_row": 3, "next_row": 5, "last_row": 4, "complete": True,
                "rows": [self.row("duplicate"), self.row("missing")]}), \
             patch.object(audit.research_sheet_outbox, "_connect", return_value=Connection()), \
             patch.object(a, "_begin") as begin:
            status = a.run_due("test")
        begin.assert_not_called()
        self.assertEqual(status["status"], "MATCHED")
        self.assertEqual(status["last_complete"]["sheet_unique"], 3)
        self.assertEqual(status["last_complete"]["missing"], 0)
        self.assertEqual(status["last_complete"]["duplicate_ids"], 0)

    def test_invalid_remote_page_after_partial_scan_still_discards_population(self):
        a = self.auditor()
        a.consume_page({"start_row": 2, "next_row": 3, "last_row": 4, "complete": False,
                        "rows": [self.row("present")]})
        with patch.object(audit.google_sheets_sync, "read_telegram_audit_page", return_value={
                "start_row": 3, "next_row": 5, "last_row": 999, "complete": True,
                "rows": [self.row("duplicate"), self.row("missing")]}), \
             patch.object(a, "_finish") as finish:
            status = a.run_due("test")
        finish.assert_not_called()
        self.assertIsNone(a.expected)
        self.assertEqual(status["status"], "AUDIT_FAILED")
        self.assertIsNone(status["last_complete"])

    def test_transient_read_preserves_frozen_population_and_counts_only_resumed_page(self):
        for reason in ["SHEET_AUDIT_HTTP_404_CONTENT_RESPONSE", "SHEET_AUDIT_CONNECTION_INTERRUPTED"]:
            with self.subTest(reason=reason):
                a = self.auditor()
                population, start, cutoff = a.expected, a.start, a.cutoff
                a.consume_page({"start_row": 2, "next_row": 4, "last_row": 7, "complete": False,
                    "normalized_maxpain_rows": 1,
                    "rows": [self.row("present"), self.row("duplicate")]})
                with patch.object(audit.google_sheets_sync, "read_telegram_audit_page",
                        side_effect=audit.google_sheets_sync.SheetAuditTransportRetry(reason)) as request, \
                        patch.object(a, "_begin") as begin, patch.object(a, "_finish") as finish:
                    status = a.run_due("test")
                request.assert_called_once_with(start_row=4, last_row=7)
                begin.assert_not_called()
                finish.assert_not_called()
                self.assertIs(a.expected, population)
                self.assertEqual((a.start, a.cutoff, a.next_row, a.last_row, a.normalized), (start, cutoff, 4, 7, 1))
                self.assertEqual(a.seen, {"present": 1, "duplicate": 1})
                self.assertEqual(status["status"], "DEFERRED_TRANSPORT_RETRY")
                self.assertIsNone(status["last_complete"])
                a.next_attempt = 0
                with patch.object(audit.google_sheets_sync, "read_telegram_audit_page", return_value={
                        "start_row": 4, "next_row": 8, "last_row": 7, "complete": True,
                        "normalized_maxpain_rows": 2,
                        "rows": [self.row("duplicate"), self.row("missing", "IMPORTED"),
                                 self.row("demo", "DEMO"), self.row("unexpected")]}), \
                        patch.object(audit.research_sheet_outbox, "_connect", return_value=Connection()), \
                        patch.object(a, "_begin") as begin:
                    status = a.run_due("test")
                begin.assert_not_called()
                self.assertEqual(status["status"], "GAPS_FOUND")
                final = status["last_complete"]
                self.assertEqual((final["sheet_unique"], final["missing"], final["duplicate_extra_rows"],
                                  final["unexpected_delivered_ids"], final["normalized_historical_maxpain_rows"]),
                                 (2, 1, 1, 1, 3))
                self.assertEqual((final["from_utc"], final["through_utc"]), (start.isoformat(), cutoff.isoformat()))

    def test_corrupt_read_after_transient_retry_still_invalidates_scan(self):
        a = self.auditor()
        a.consume_page({"start_row": 2, "next_row": 3, "last_row": 4, "complete": False,
                        "rows": [self.row("present")]})
        with patch.object(audit.google_sheets_sync, "read_telegram_audit_page",
                          side_effect=audit.google_sheets_sync.SheetAuditTransportRetry("SHEET_AUDIT_CONNECTION_INTERRUPTED")):
            a.run_due("test")
        a.next_attempt = 0
        with patch.object(audit.google_sheets_sync, "read_telegram_audit_page",
                          side_effect=json.JSONDecodeError("invalid response", "", 0)), patch.object(a, "_finish") as finish:
            status = a.run_due("test")
        finish.assert_not_called()
        self.assertIsNone(a.expected)
        self.assertEqual(status["status"], "AUDIT_FAILED")
        self.assertIsNone(status["last_complete"])

    def test_pending_prefix_does_not_hide_later_synced_repairs(self):
        a = self.auditor()
        ids = [f"event-{n:04d}" for n in range(260)]
        a.expected = {key: {"event_fingerprint": key, "event_type": "MAX_PAIN_ALERT"} for key in ids}
        states = {audit.research_sheet_outbox._json([key]): ("RETRY" if n % 2 else "PENDING")
                  if n < 110 else "SYNCED" for n, key in enumerate(ids)}
        conn = Connection()
        submitted = []

        def apply_eligible_repair(sql, args):
            # Model the database result while checking the producer submits
            # the COMPLETE missing population, including IDs after the old
            # first-100 cut. State selection/locking is an SQL responsibility.
            conn.calls.append((sql, args))
            submitted.extend(args[0])
            eligible = sorted(key for key in args[0] if states.get(key) == "SYNCED")[:100]
            for key in eligible:
                states[key] = "PENDING"
            return Result(len(eligible))

        conn.execute = apply_eligible_repair
        with patch.object(audit.research_sheet_outbox, "_connect", return_value=conn):
            summary = a._finish("test")
        self.assertEqual(len(conn.calls), 1)
        self.assertEqual(len(submitted), 260)
        self.assertEqual(summary["previously_synced_rows_requeued"], 100)
        self.assertEqual(sum(state == "SYNCED" for state in states.values()), 50)
        self.assertIn("FOR UPDATE SKIP LOCKED", conn.calls[0][0])
        self.assertEqual(summary["missing"], 260, "queued repair is not proof of delivery")

    def test_empty_sheet_is_true_missing_and_never_future_row(self):
        a = self.auditor()
        self.assertTrue(a.consume_page({"start_row": 2, "next_row": 2, "last_row": 1, "complete": True, "rows": []}))
        self.assertEqual(a.summarize()["missing"], 3)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "TEST_DATABASE_URL required for PostgreSQL audit repair gate")
class PostgreSQLAuditRepairTests(unittest.TestCase):
    """Execute production repair SQL against a rollback-only temporary table."""

    def setUp(self):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        self.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(self.dsn)
        if (info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (info.get("dbname", "").startswith("test_") or info.get("dbname", "").endswith("_test"))):
            raise ValueError("Audit integration requires an explicit local/CI test database")
        self.conn = psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5,
                                   options="-c statement_timeout=10000 -c lock_timeout=1000")
        self.addCleanup(self.conn.close)
        self.addCleanup(self.conn.rollback)
        self.conn.execute("SET search_path TO pg_temp")
        migration = (Path(__file__).resolve().parent / "migrations" / "022_ordered_formula_research.sql").read_text()
        table_suffix = migration.split("CREATE TABLE IF NOT EXISTS research_sheet_upsert_outbox", 1)[1].split(";", 1)[0]
        # Use the real outbox column/check contract, isolated to this session.
        self.conn.execute("CREATE TEMP TABLE research_sheet_upsert_outbox" + table_suffix)

    def rows(self):
        return {(row["sheet_name"], row["row_key"]): dict(row) for row in
                self.conn.execute("SELECT * FROM research_sheet_upsert_outbox").fetchall()}

    def test_real_sql_limits_after_synced_filter_and_leaves_active_queue_untouched(self):
        ids = [f"event-{n:04d}" for n in range(230)]
        seed = []
        for n, key in enumerate(ids):
            state = "SYNCED" if n >= 110 else ("RETRY" if n % 2 else "PENDING")
            if n in {5, 6}:
                state = "IN_FLIGHT"
            seed.append(("Telegram_Events", key, state))
        seed += [("Snapshots", ids[110], "SYNCED"),
                 ("Telegram_Events", "outside-population", "SYNCED"),
                 ("Telegram_Events", "already-present", "SYNCED")]
        insert = """
            INSERT INTO research_sheet_upsert_outbox
                (sheet_name,row_key,payload,payload_sha256,sync_status,attempts,
                 claim_token,claimed_payload_sha256,lease_expires_at_utc,
                 synced_at_utc,last_error,updated_at_utc)
            VALUES (%s,%s,%s::jsonb,%s,%s,7,%s,%s,%s,%s,'KEEP_ORIGINAL_STATE',%s)
        """
        baseline = datetime(2026, 9, 1, tzinfo=timezone.utc)
        with self.conn.cursor() as cursor:
            cursor.executemany(insert, [
                (sheet, audit.research_sheet_outbox._json([key]), json.dumps({"event_id": key}), key,
                 state, "11111111-1111-4111-8111-111111111111" if state == "IN_FLIGHT" else None,
                 key if state == "IN_FLIGHT" else None, baseline if state == "IN_FLIGHT" else None,
                 baseline if state == "SYNCED" else None, baseline)
                for sheet, key, state in seed
            ])
        before = self.rows()

        @contextmanager
        def test_connection(url):
            self.assertEqual(url, "audit-local-test")
            # _finish uses its normal SQL. Keep the surrounding transaction
            # open for assertions, then rollback every fixture and write.
            yield self.conn

        def auditor():
            instance = AuditTests().auditor()
            instance.expected = {key: {"event_fingerprint": key, "event_type": "MAX_PAIN_ALERT"}
                                 for key in ids + ["no-outbox-row", "already-present"]}
            instance.seen["already-present"] = 1
            return instance

        with patch.object(audit.research_sheet_outbox, "_connect", side_effect=test_connection):
            first = auditor()._finish("audit-local-test")
        after = self.rows()
        repaired = {("Telegram_Events", audit.research_sheet_outbox._json([key])) for key in ids[110:210]}
        self.assertEqual(first["previously_synced_rows_requeued"], 100)
        self.assertEqual(first["missing"], 231, "repair scheduling cannot certify delivery")
        self.assertEqual(set(before), set(after), "repair must not invent absent outbox rows")
        for identity, original in before.items():
            if identity not in repaired:
                self.assertEqual(after[identity], original, "pending/retry/active/present/other-sheet rows are untouched")
            else:
                current = after[identity]
                self.assertEqual((current["sync_status"], current["attempts"], current["synced_at_utc"], current["last_error"]),
                                 ("PENDING", 0, None, "ACTUAL_SHEET_ROW_MISSING"))
                for name in ("payload", "payload_sha256", "claim_token", "claimed_payload_sha256", "lease_expires_at_utc"):
                    self.assertEqual(current[name], original[name])
        # A second cycle must reach the remaining eligible tail while all
        # first-cycle repairs are still PENDING ahead of it.
        with patch.object(audit.research_sheet_outbox, "_connect", side_effect=test_connection):
            second = auditor()._finish("audit-local-test")
        self.assertEqual(second["previously_synced_rows_requeued"], 20)
        self.assertEqual(second["missing"], 231)
        final = self.rows()
        for sheet, key, state in seed:
            if state in {"PENDING", "RETRY", "IN_FLIGHT"}:
                identity = (sheet, audit.research_sheet_outbox._json([key]))
                self.assertEqual(final[identity], before[identity])


if __name__ == "__main__": unittest.main()
