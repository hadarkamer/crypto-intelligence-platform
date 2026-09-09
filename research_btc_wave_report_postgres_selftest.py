"""Real PostgreSQL report checkpoint/publication atomicity and local archive reads."""
from datetime import timedelta
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_btc_wave_report_worker as worker
import research_price_archive as archive
from research_btc_wave_endpoint_report_selftest import START, candle
from research_btc_wave_report_worker_selftest import fixtures


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "TEST_DATABASE_URL required for real PostgreSQL")
class WaveReportPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(cls.dsn)
        if info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"} or "test" not in info.get("dbname", "").lower():
            raise RuntimeError("Only local/CI test databases are allowed")
        cls.conn = psycopg.connect(cls.dsn, row_factory=dict_row, autocommit=True)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def connect(self, url):
        return worker.psycopg.connect(self.dsn, row_factory=worker.dict_row,
            options=f"-c search_path={self.schema} -c statement_timeout=15000 -c lock_timeout=1000")

    def setUp(self):
        self.schema = "wave_report_test_"+uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        root = Path(__file__).parent
        for filename in ("044_continuous_price_archive.sql", "045_btc_wave_report_refresh.sql"):
            self.conn.execute((root/"migrations"/filename).read_text())
        self.conn.execute((root/"migrations/045_btc_wave_report_refresh.sql").read_text())
        self.conn.execute("""CREATE TABLE research_sheet_upsert_outbox (
            sheet_name text,row_key text,payload jsonb,payload_sha256 text,
            sync_status text DEFAULT 'PENDING',attempts integer DEFAULT 0,
            next_attempt_at_utc timestamptz DEFAULT NOW(),claim_token uuid,claimed_payload_sha256 text,
            lease_expires_at_utc timestamptz,synced_at_utc timestamptz,last_error text,
            updated_at_utc timestamptz DEFAULT NOW(),source_time_utc timestamptz,
            PRIMARY KEY(sheet_name,row_key))""")
        self.source = fixtures()
        archive.write_bars(self.conn, archive.BINANCE_SPOT, "BTC",
            [{**candle(i, high=101., low=99.5), "volume": 1.} for i in range(120)])

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def state(self):
        return self.conn.execute("SELECT * FROM research_btc_wave_report_state").fetchone()

    def run_worker(self, *, fail_staging=False, source_reader=None, now=None):
        self.last_worker = worker.ResearchBTCWaveReportWorker()
        with patch.object(worker, "_connect", self.connect), patch.object(worker, "_database_url", lambda:self.dsn), \
             patch.object(worker, "load_job_source", source_reader or (lambda conn, now:self.source)):
            if fail_staging:
                real_stage = worker.research_sheet_outbox.stage_upserts
                def fail(conn, rows):
                    real_stage(conn, rows)
                    raise RuntimeError("simulated failure after staging before report checkpoint")
                with patch.object(worker.research_sheet_outbox, "stage_upserts", fail):
                    return self.last_worker.run_once(now=now or START+timedelta(minutes=120))
            return self.last_worker.run_once(now=now or START+timedelta(minutes=120))

    def test_archive_only_report_and_outbox_commit_together(self):
        result = self.run_worker()
        self.assertTrue(result["published"])
        self.assertEqual(result["staged_rows"], 16)
        state = self.state()
        self.assertIsNone(state["pending_job"])
        self.assertEqual(state["report_observed_at_utc"], START+timedelta(minutes=120))
        rows = self.conn.execute("SELECT payload FROM research_sheet_upsert_outbox").fetchall()
        self.assertEqual(len(rows), 16)
        self.assertTrue(all(row["payload"]["row"]["coverage_status"] == "COMPLETE_OBSERVED_PREFIX" for row in rows))
        self.assertTrue(self.run_worker()["waiting_for_sheet_delivery"])

    def test_delayed_exact_sheet_ack_gates_next_generation(self):
        self.assertTrue(self.run_worker()["published"])
        original = self.state()
        later = START+timedelta(minutes=136)
        def no_source(conn, now):
            raise AssertionError("New source generation must wait for previous Sheet ACK")
        waiting = self.run_worker(now=later, source_reader=no_source)
        self.assertTrue(waiting["waiting_for_sheet_delivery"])
        json.dumps(self.last_worker.status(), allow_nan=False)
        json.dumps(waiting, allow_nan=False)
        self.assertEqual(waiting["delivery"]["pending_rows"], 16)
        self.assertEqual(self.state()["report"], original["report"])
        self.assertIsNone(self.state()["pending_job"])
        # The sender alone sets SYNCED after matching the current claimed
        # generation. Simulate those exact confirmed acknowledgments here.
        self.conn.execute("UPDATE research_sheet_upsert_outbox SET sync_status='SYNCED'")
        refreshed = self.run_worker(now=later)
        self.assertTrue(refreshed["published"])
        self.assertEqual(self.state()["report_observed_at_utc"], later)

    def test_pending_job_from_previous_deployment_waits_for_old_report_ack(self):
        self.assertTrue(self.run_worker()["published"])
        original = self.state()
        later = START+timedelta(minutes=136)
        pending = worker.prepare_job(self.source, later,
            previous_report=original["report"], previous_source=original["source"])
        self.conn.execute("UPDATE research_btc_wave_report_state SET pending_job=%s::jsonb",
                          (worker.canonical(pending),))
        checkpoint = self.state()["pending_job"]
        self.assertTrue(checkpoint["pending_event_ids"])
        def no_source(conn, now):
            raise AssertionError("Resumed job must wait for its previous report ACK")
        waiting = self.run_worker(now=later, source_reader=no_source)
        self.assertTrue(waiting["waiting_for_sheet_delivery"])
        json.dumps(self.last_worker.status(), allow_nan=False)
        json.dumps(waiting, allow_nan=False)
        self.assertTrue(waiting["pending_job_preserved"])
        self.assertEqual(self.state()["pending_job"], checkpoint)
        self.assertEqual(self.state()["report"], original["report"])
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM research_sheet_upsert_outbox").fetchone()["n"], 16)
        self.conn.execute("UPDATE research_sheet_upsert_outbox SET sync_status='SYNCED'")
        resumed = self.run_worker(now=later)
        self.assertTrue(resumed["published"])
        self.assertIsNone(self.state()["pending_job"])
        self.assertEqual(self.state()["report_observed_at_utc"], later)

    def test_hype_perpetual_archive_enters_only_explicit_mixed_report(self):
        self.source["events"][0].update(symbol="HYPE", current_price=99.,
            engine_snapshot={"price_source": "hyperliquid", "price_pair": "HYPEUSDT"})
        archive.write_bars(self.conn, archive.HYPERLIQUID_PERP, "HYPE",
            [{**candle(i, high=101., low=99.5), "volume": None} for i in range(1, 60)])
        self.assertTrue(self.run_worker()["published"])
        result = self.state()["report"]
        hype = [row for row in result["records"] if row["source_scope"] == worker.report.PERP_SCOPE]
        self.assertEqual(len(hype), 2)
        self.assertTrue(all(row["status"] == "READY" and row["reference_price"] == 100. and row["original_alert_reference_price"] == 99. for row in hype))
        self.assertTrue(all(row["source"]["market"] == "perpetual" and row["source"]["price_kind"] == "TRADE" for row in hype))
        self.assertFalse(any(row["source_scope"] == worker.report.MARK_SCOPE for row in result["records"]))

    def test_failure_rolls_back_staged_rows_and_report_together(self):
        with self.assertRaisesRegex(RuntimeError, "simulated failure"):
            self.run_worker(fail_staging=True)
        self.assertIsNone(self.state()["report"])
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM research_sheet_upsert_outbox").fetchone()["n"], 0)
        self.assertTrue(self.run_worker()["published"])

    def test_late_source_correction_restarts_before_publication(self):
        calls = []
        revised = fixtures()
        revised["events"][0]["current_price"] = 100.1
        def source(conn, now):
            calls.append(now)
            return self.source if len(calls) == 1 else revised
        result = self.run_worker(source_reader=source)
        self.assertFalse(result["published"])
        self.assertTrue(result["source_changed"])
        self.assertIsNone(self.state()["report"])
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM research_sheet_upsert_outbox").fetchone()["n"], 0)
        self.source = revised
        self.assertTrue(self.run_worker()["published"])


if __name__ == "__main__":
    unittest.main()
