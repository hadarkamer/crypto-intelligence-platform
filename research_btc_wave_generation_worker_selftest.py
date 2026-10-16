"""Controller checks plus real PostgreSQL >32-parent atomic publication tests."""
from contextlib import nullcontext
from datetime import timedelta
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_btc_wave_generation_worker as controller
import research_btc_wave_generation_store as store
import research_btc_wave_report_worker as worker
import research_btc_wave_endpoint_report as report
import research_price_archive as archive
from research_btc_wave_endpoint_report_selftest import START, candle, event, wave
from research_btc_wave_report_worker_selftest import fetch


class ControllerChecks(unittest.TestCase):
    def test_successor_context_reuses_closed_path_without_adding_a_parent(self):
        target = wave("first")
        successor = {**wave("second"), "start_time_utc": target["end_time_utc"],
                     "end_time_utc": None}
        source = {"waves": [target], "events": [event(1, wave_id="first")],
                  "successor_context": [successor]}
        now = START+timedelta(hours=2)
        old = worker.prepare_job(source, now)
        self.assertTrue(worker.advance_job(old, fetcher=fetch)["completed"])
        previous = {"source": source, "report": worker.finish_job(old)}
        fresh = controller.prepare_part_job(source, now+timedelta(minutes=15), previous=previous)
        self.assertEqual(fresh["pending_event_ids"], [])
        self.assertEqual(fresh["reused_closed_paths"], 1)
        self.assertEqual(len(worker.finish_job(fresh)["waves"]), 1)
        no_proof = {**source, "successor_context": []}
        unverified = controller.prepare_part_job(no_proof, now, previous=previous)
        self.assertEqual(unverified["pending_event_ids"], [1])

    def test_paths_budget_is_cumulative_and_checkpoint_is_resumable(self):
        class Conn:
            def transaction(self): return nullcontext()
            def execute(self, *args): return self
            def commit(self): pass
        generation = {"generation_id": str(uuid4()), "worker_key": worker.VERSION,
            "status": "CAPTURED", "observed_at_utc": START+timedelta(hours=2), "parent_count": 33}
        parts = [{"ordinal": i, "parent_id": f"p{i}", "job": None} for i in range(33)]
        calls, saved = [], []
        def read_source(conn, parent_id, observed_at):
            i = int(parent_id[1:])
            return {"waves": [wave(parent_id)], "events": [event(i+1, wave_id=parent_id)],
                    "successor_context": []}
        def save(conn, gen, ordinal, **kwargs):
            saved.append(ordinal)
            self.assertIsNotNone(kwargs["completed_report"])
            parts.pop(0)
        def read_price(conn, symbol, start, end):
            calls.append((symbol, start, end))
            return fetch(symbol, start, end)
        with patch.object(store, "get_generation", return_value=generation), \
             patch.object(store, "next_unfinished_part", side_effect=lambda *args: parts[0] if parts else None), \
             patch.object(store, "has_unfinished_parts", side_effect=lambda *args: bool(parts)), \
             patch.object(store, "load_part_source", side_effect=read_source), \
             patch.object(store, "get_previous_parent_cache", return_value=None), \
             patch.object(store, "save_checkpoint", side_effect=save), \
             patch.object(store, "validate_generation", side_effect=AssertionError("Incomplete generation validated")), \
             patch.object(controller, "_fetch", side_effect=read_price):
            progress = controller.run_generation(Conn(), state={"report": None},
                pending=controller.reference(generation), now=generation["observed_at_utc"], monotonic=lambda: 0.)
        self.assertEqual(progress["processed_paths"], 8)
        self.assertEqual(len(calls), 8)
        self.assertEqual(saved, list(range(8)))
        self.assertFalse(progress["published"])
        self.assertFalse(progress["completed"])
        self.assertEqual(len(parts), 25)


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "TEST_DATABASE_URL required for real PostgreSQL")
class ControllerPostgresChecks(unittest.TestCase):
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
        self.schema = "wave_parts_worker_test_"+uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        root = Path(__file__).parent
        for filename in ("044_continuous_price_archive.sql", "045_btc_wave_report_refresh.sql",
                         "057_btc_wave_report_parts.sql"):
            self.conn.execute((root/"migrations"/filename).read_text())
        self.conn.execute("""CREATE TABLE research_sheet_upsert_outbox (
            sheet_name text,row_key text,payload jsonb,payload_sha256 text,
            sync_status text DEFAULT 'PENDING',attempts integer DEFAULT 0,
            next_attempt_at_utc timestamptz DEFAULT NOW(),claim_token uuid,claimed_payload_sha256 text,
            lease_expires_at_utc timestamptz,synced_at_utc timestamptz,last_error text,
            updated_at_utc timestamptz DEFAULT NOW(),source_time_utc timestamptz,
            PRIMARY KEY(sheet_name,row_key))""")
        self.conn.execute("""CREATE TABLE research_btc_parent_movements (
            btc_parent_movement_id text PRIMARY KEY,episode_policy_version text,
            start_time_utc timestamptz,end_time_utc timestamptz,evidence_eligible boolean,
            boundary_reason text,observed_through_utc timestamptz);
            CREATE TABLE research_events (event_id bigint PRIMARY KEY,event_kind text,event_type text,
            alert_time_utc timestamptz,symbol text,direction text,score double precision,current_price double precision,
            delivery_status text,engine_snapshot jsonb,source_side text,timeframe text,target_price double precision,
            initial_target_distance_pct double precision,event_fingerprint text,strategy_version text,code_version text);
            CREATE TABLE research_event_btc_movements (event_id bigint,episode_policy_version text,
            btc_parent_movement_id text,membership_status text,decision_time_utc timestamptz,
            btc_observed_close_utc timestamptz,PRIMARY KEY(event_id,episode_policy_version))""")
        self.now = START+timedelta(minutes=100)
        self.parents, self.events = [], []
        for i in range(33):
            start = START+timedelta(minutes=3*i)-report.MILLISECOND
            end = START+timedelta(minutes=3*(i+1))-report.MILLISECOND if i < 32 else None
            parent = {**wave(f"parent-{i:03}"), "start_time_utc": start,
                "end_time_utc": end, "observed_through_utc": end or self.now-report.MILLISECOND,
                "closing_boundary_verified": i < 32}
            evt = event(i+1, seconds=180*i, wave_id=parent["btc_parent_movement_id"])
            self.parents.append(parent)
            self.events.append(evt)
            self.conn.execute("""INSERT INTO research_btc_parent_movements VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                tuple(parent[k] for k in ("btc_parent_movement_id", "episode_policy_version", "start_time_utc",
                    "end_time_utc", "evidence_eligible", "boundary_reason", "observed_through_utc")))
            self.conn.execute("""INSERT INTO research_events (event_id,event_kind,event_type,alert_time_utc,
                symbol,direction,score,current_price,delivery_status,engine_snapshot,event_fingerprint,
                strategy_version,code_version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)""",
                tuple(evt[k] for k in ("event_id", "event_kind", "event_type", "alert_time_utc", "symbol", "direction",
                    "score", "current_price", "delivery_status")) + (worker.canonical(evt["engine_snapshot"]),)
                + tuple(evt[k] for k in ("event_fingerprint", "strategy_version", "code_version")))
            self.conn.execute("""INSERT INTO research_event_btc_movements VALUES (%s,%s,%s,%s,%s,%s)""",
                tuple(evt[k] for k in ("event_id", "membership_policy_version", "membership_parent_id",
                    "membership_status", "decision_time_utc", "btc_observed_close_utc")))
        archive.write_bars(self.conn, archive.BINANCE_SPOT, "BTC",
            [{**candle(i, high=101., low=99.5), "volume": 1.} for i in range(120)])

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def state(self):
        return self.conn.execute("SELECT * FROM research_btc_wave_report_state").fetchone()

    def run_worker(self, *, now=None):
        with patch.object(worker, "_connect", self.connect), patch.object(worker, "_database_url", lambda: self.dsn):
            return worker.ResearchBTCWaveReportWorker().run_once(now=now or self.now)

    def complete(self):
        for _ in range(7):
            value = self.run_worker()
            self.assertLessEqual(value["processed_paths"], 8)
            if value["published"]:
                return value
        self.fail("33-parent generation failed to finish in its bounded passes")

    def test_thirty_three_parents_resume_publish_and_exact_ack_gate(self):
        first = self.run_worker()
        self.assertEqual(first["processed_paths"], 8)
        pending = self.state()["pending_job"]
        self.assertEqual(set(pending), {"storage_version", "generation_id"})
        self.assertIsNone(self.state()["report"])
        self.assertEqual(self.conn.execute("SELECT count(*) n FROM research_btc_wave_report_parts WHERE report IS NOT NULL").fetchone()["n"], 8)
        final = self.complete()
        state = self.state()
        self.assertTrue(final["published"])
        self.assertEqual(state["report"]["parent_count"], 33)
        self.assertEqual(state["report"]["storage_version"], controller.STORAGE_VERSION)
        self.assertIsNone(state["pending_job"])
        self.assertEqual(state["source"]["generation_id"], pending["generation_id"])
        parts = list(store.iter_parts(self.conn, pending))
        self.assertEqual(len(parts), 33)
        records = [row for part in parts for row in part["report"]["records"]]
        expected = report.aggregate_records(records)
        by_key = lambda row: (row["source_scope"], row["coin_scope"], row["direction"], row["threshold_pct"])
        actual = {by_key(row): row for row in state["report"]["summary_rows"]}
        for old in expected:
            for key, value in old.items():
                if key not in {"source_event_ids", "btc_parent_movement_ids"}:
                    self.assertEqual(actual[by_key(old)][key], value)
        self.assertTrue(self.run_worker(now=self.now+timedelta(minutes=16))["waiting_for_sheet_delivery"])
        self.conn.execute("UPDATE research_sheet_upsert_outbox SET sync_status='SYNCED',payload=jsonb_set(payload,'{row,report_digest}','\"older\"'::jsonb)")
        self.assertTrue(self.run_worker(now=self.now+timedelta(minutes=16))["waiting_for_sheet_delivery"])
        self.conn.execute("UPDATE research_sheet_upsert_outbox SET payload=jsonb_set(payload,'{row,report_digest}',to_jsonb(%s::text))",
            (worker.digest(state["report"]),))
        # Every complete closed source path is reused. The open wave is reread.
        next_generation = self.run_worker(now=self.now+timedelta(minutes=16))
        self.assertTrue(next_generation["published"])
        self.assertEqual(next_generation["processed_paths"], 1)
        self.assertNotEqual(self.state()["source"]["generation_id"], pending["generation_id"])

    def test_stage_failure_rolls_back_publication_and_source_revision_keeps_cutoff(self):
        for _ in range(4):
            self.assertFalse(self.run_worker()["published"])
        pointer = self.state()["pending_job"]
        real_stage = worker.research_sheet_outbox.stage_upserts
        def fail(conn, rows):
            real_stage(conn, rows)
            raise RuntimeError("simulated failure after outbox staging")
        with patch.object(worker.research_sheet_outbox, "stage_upserts", fail):
            with self.assertRaisesRegex(RuntimeError, "simulated failure"):
                self.run_worker()
        state = self.state()
        self.assertIsNone(state["report"])
        self.assertEqual(state["pending_job"], pointer)
        self.assertEqual(self.conn.execute("SELECT count(*) n FROM research_sheet_upsert_outbox").fetchone()["n"], 0)
        self.assertEqual(store.get_generation(self.conn, pointer)["status"], "CAPTURED")
        self.conn.execute("UPDATE research_events SET current_price=100.1 WHERE event_id=1")
        revised = self.run_worker(now=self.now+timedelta(hours=1))
        self.assertTrue(revised["source_changed"])
        self.assertFalse(revised["published"])
        self.assertIsNone(self.state()["report"])
        replacement = store.get_generation(self.conn, self.state()["pending_job"])
        self.assertEqual(replacement["observed_at_utc"], self.now)
        self.assertEqual(store.get_generation(self.conn, pointer)["status"], "ABANDONED")
        self.assertEqual(self.conn.execute("SELECT count(*) n FROM research_btc_wave_report_parts WHERE generation_id=%s::uuid", (pointer["generation_id"],)).fetchone()["n"], 33)
        final = self.complete()
        self.assertTrue(final["published"])
        self.assertEqual(self.state()["report_observed_at_utc"], self.now)

    def test_changed_part_report_cannot_publish(self):
        for _ in range(4):
            self.assertFalse(self.run_worker()["published"])
        pointer = self.state()["pending_job"]
        self.conn.execute("UPDATE research_btc_wave_report_parts SET report=jsonb_set(report::jsonb,'{candidate}','\"tampered\"'::jsonb)::json WHERE generation_id=%s::uuid AND ordinal=0", (pointer["generation_id"],))
        result = self.run_worker()
        self.assertTrue(result["source_changed"])
        self.assertEqual(result["validation_reason"], "STORED_PART_INTEGRITY_FAILED")
        self.assertIsNone(self.state()["report"])
        self.assertEqual(self.conn.execute("SELECT count(*) n FROM research_sheet_upsert_outbox").fetchone()["n"], 0)

    def test_empty_population_does_not_pin_a_future_population_to_old_cutoff(self):
        self.conn.execute("DELETE FROM research_event_btc_movements")
        self.conn.execute("DELETE FROM research_events")
        self.conn.execute("DELETE FROM research_btc_parent_movements")
        waiting = self.run_worker()
        self.assertEqual(waiting["waiting_for"], "BTC_PARENT_POPULATION")
        self.assertIsNone(self.state()["pending_job"])
        self.assertEqual(self.conn.execute("SELECT status FROM research_btc_wave_report_generations").fetchone()["status"], "ABANDONED")
        later = self.now+timedelta(hours=1)
        self.conn.execute("""INSERT INTO research_btc_parent_movements VALUES (%s,%s,%s,NULL,true,%s,%s)""",
            ("future-parent", report.POLICY_VERSION, self.now+timedelta(minutes=30),
             "CAUSAL_CLOSE_REVERSAL", later-report.MILLISECOND))
        published = self.run_worker(now=later)
        self.assertTrue(published["published"])
        self.assertEqual(self.state()["report_observed_at_utc"], later)
        self.assertEqual(self.state()["report"]["parent_count"], 1)

    def test_numeric_root_roundtrip_binds_exact_sheet_ack(self):
        original_aggregate = controller.aggregate.aggregate_generation
        before_storage = {}
        def aggregate_with_numeric_probe(*args, **kwargs):
            completed = original_aggregate(*args, **kwargs)
            completed["numeric_storage_probe"] = {"large_float": 1e16, "negative_zero": -0.0}
            # The serializer hash must be taken only after the JSONB roundtrip.
            # These scalars deliberately reproduce the PostgreSQL type/spelling
            # change without substituting any calculated research outcome.
            before_storage["probe"] = completed["numeric_storage_probe"]
            return completed
        with patch.object(controller.aggregate, "aggregate_generation", aggregate_with_numeric_probe):
            published = self.complete()
        state = self.state()
        root_hash = worker.digest(state["report"])
        self.assertEqual(published["report_digest"], root_hash)
        self.assertEqual(state["report"]["numeric_storage_probe"]["large_float"], 10**16)
        self.assertIsInstance(state["report"]["numeric_storage_probe"]["large_float"], int)
        self.assertNotEqual(worker.canonical(state["report"]["numeric_storage_probe"]),
                            worker.canonical(before_storage["probe"]))
        rows = self.conn.execute("SELECT payload->'row'->>'report_digest' AS digest FROM research_sheet_upsert_outbox").fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(row["digest"] == root_hash for row in rows))
        with patch.object(store, "freeze", side_effect=AssertionError("UnACKed root advanced")):
            waiting = self.run_worker(now=self.now+timedelta(minutes=16))
        self.assertTrue(waiting["waiting_for_sheet_delivery"])
        self.assertEqual(waiting["delivery"]["report_digest"], root_hash)
        self.conn.execute("UPDATE research_sheet_upsert_outbox SET sync_status='SYNCED'")
        delivery = worker.sheet_delivery_status(self.conn, self.state()["report"])
        self.assertTrue(delivery["complete"])
        self.assertEqual(delivery["missing_or_other_generation_rows"], 0)
        self.assertTrue(self.run_worker(now=self.now+timedelta(minutes=16))["published"])


if __name__ == "__main__":
    unittest.main()
