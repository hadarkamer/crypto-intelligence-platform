"""Real PostgreSQL gates for migrations 037/038 and durable worker transactions.

Run with TEST_DATABASE_URL=postgresql://...@localhost/crypto_research_test.
No DATABASE_URL fallback, Telegram transport, or production connection is used.
Each test applies real prerequisite migrations in its own temporary schema.
Absent TEST_DATABASE_URL skips this optional local gate; CI must supply it.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import importlib
import os
from pathlib import Path
import unittest
from uuid import uuid4

ROOT = Path(__file__).resolve().parent
MIGRATIONS = (
    "001_research_archive_v1.sql", "002_formula_research_v1.sql",
    "003_formula_autonomous_alerts_v1.sql", "021_btc_parent_movements_v1.sql",
    "022_ordered_formula_research.sql", "027_ordered_formula_research_periods.sql",
    "031_ordered_prospective_validation.sql", "037_ordered_experimental_delivery.sql",
    "038_runtime_event_scan_bounds.sql",
)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "TEST_DATABASE_URL is required for real PostgreSQL integration")
class PostgreSQLDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.psycopg, cls.sql, cls.dict_row = psycopg, sql, staticmethod(dict_row)
        cls.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(cls.dsn)
        if (info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (info.get("dbname", "").startswith("test_") or info.get("dbname", "").endswith("_test"))):
            raise ValueError("Integration requires an explicit local/CI test database")
        cls.store = importlib.import_module("research_ordered_experimental_store")
        cls.contract = importlib.import_module("research_ordered_experimental")
        cls.validation = importlib.import_module("research_ordered_validation")
        cls.acceptance = importlib.import_module("research_ordered_acceptance_policy")
        cls.formulas = importlib.import_module("research_formula_ordered_store")
        cls.factories = importlib.import_module("research_ordered_validation_selftest")
        cls.scan = importlib.import_module("research_event_scan")

    def connect(self):
        conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5,
            options="-c statement_timeout=10000 -c lock_timeout=1000")
        conn.execute(self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema = "test_delivery_" + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5)
        self.conn.execute(self.sql.SQL("CREATE SCHEMA {}").format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema)))
        for filename in MIGRATIONS:
            self.conn.execute((ROOT / "migrations" / filename).read_text(), prepare=False)
        self.conn.commit()
        self.assessed = self.conn.execute("SELECT date_trunc('minute',clock_timestamp())-interval '2 minutes' AS now").fetchone()["now"]
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, connect_timeout=5, autocommit=True) as cleanup:
            cleanup.execute(self.sql.SQL("DROP SCHEMA {} CASCADE").format(self.sql.Identifier(self.schema)))

    def seed_qualification(self):
        """Actual current acceptance policy, calculator labels and five waves."""
        v = self.validation
        candidate = next(c for c in v.evidence.candidate_catalog(include_extended=True)
            if c["formula_id"].endswith(":CORE_PRICE_OI_TOTAL_65"))
        candidate = {**candidate, "direction_mode": candidate.get("research_orientation", "NORMAL")}
        self.scope = {**self.factories.SCOPE, "candidate_key": candidate["formula_id"],
            "parent_policy_version": self.formulas.PARENT_POLICY}
        binding = v.binding(self.scope, candidate)
        self.registration = v.freeze(self.scope, candidate,
            frozen_at_utc=self.assessed-timedelta(hours=10),
            acceptance_policy=self.acceptance.policy_for(binding, candidate))
        rows = [{**self.factories.item(i, start=self.assessed-timedelta(hours=10-i)),
            "episode_policy_version": self.formulas.PARENT_POLICY} for i in range(1, 6)]
        windows = {row["event_id"]: self.factories.common(row) for row in rows}
        self.result = v.evaluate(self.registration, rows, analysis_as_of_utc=self.assessed,
            source_coverage_complete=True, common_window_rows=windows)
        self.result["representative_entries_frozen"] = True
        self.result["evidence_sha256"] = v.digest(self.result)
        self.assertTrue(self.result["research_ready"], self.result)
        self.conn.execute("""INSERT INTO research_ordered_formula_candidates
            (candidate_key,formula_version,definition_sha256,definition) VALUES(%s,%s,%s,%s::jsonb)""",
            (candidate["formula_id"], "integration", v.digest(candidate), v.canonical(candidate)))
        self.conn.execute("""INSERT INTO research_ordered_formula_scopes
            (scope_key,candidate_key,symbol,direction,window_minutes,threshold_bps,period_key,period_start_utc)
            VALUES(%s,%s,'BTC','LONG',60,50,%s,%s)""",
            (self.scope["scope_key"], candidate["formula_id"], self.scope["period_key"], self.scope["period_start_utc"]))
        self.conn.execute("""INSERT INTO research_ordered_validation_freezes
            (freeze_id,scope_key,definition_sha256,frozen_at_utc,registration) VALUES(%s,%s,%s,%s,%s::jsonb)""",
            (self.registration["freeze_id"], self.scope["scope_key"], self.registration["definition_sha256"],
             self.registration["frozen_at_utc"], v.canonical(self.registration)))
        self.add_evaluation(self.result, self.assessed)
        self.conn.execute("INSERT INTO research_formula_alert_subscriptions(chat_id) VALUES(42)")
        self.assertTrue(self.store.publish_evaluation(self.conn, self.scope, self.result,
            now=self.assessed, rows=rows, common_window_rows=windows))
        self.conn.commit()
        self.grant = self.conn.execute("SELECT * FROM research_ordered_experimental_eligibility").fetchone()
        self.conn.commit()
        return self.grant

    def add_evaluation(self, result, when):
        self.conn.execute("""INSERT INTO research_ordered_validation_evaluations
            (freeze_id,evidence_sha256,result,evaluated_at_utc) VALUES(%s,%s,%s::jsonb,%s)""",
            (result["freeze_id"], result["evidence_sha256"], self.validation.canonical(result), when))

    def add_event(self, event_id, when, *, wave=None, delivered=True, matching=True):
        """Store a native source and its real FK/trigger-validated BTC bar."""
        wave = wave or f"trigger-wave-{event_id}"
        close = when.replace(second=0, microsecond=0)-timedelta(milliseconds=1)
        opened = close.replace(second=0, microsecond=0)
        parent_start = opened
        self.conn.execute("""INSERT INTO research_events(event_id,schema_version,event_kind,event_type,
            alert_time_utc,symbol,direction,setup_key,event_fingerprint,strategy_version,code_version,
            runtime_session_id,delivery_status,current_price,engine_snapshot)
            VALUES(%s,'test','ALERT','COMBINED_CONFIRMATION',%s,'BTC','LONG',%s,%s,'test','test','test',%s,100,%s::jsonb)""",
            (event_id, when, f"{event_id:064x}", f"{event_id:064x}", "DELIVERED" if delivered else "UNKNOWN",
             '{"price_source":"binance_spot","price_pair":"BTCUSDT"}'))
        if not matching:
            return
        self.conn.execute("""INSERT INTO research_btc_price_bars(open_time_utc,close_time_utc,open,high,low,close)
            VALUES(%s,%s,100,101,99,100) ON CONFLICT DO NOTHING""", (opened, close))
        existing = self.conn.execute("SELECT btc_parent_movement_id FROM research_btc_parent_movements WHERE btc_parent_movement_id=%s", (wave,)).fetchone()
        if not existing:
            # Each fixture interval is closed, so independent fixture waves do
            # not contend for the production one-active-parent unique index.
            self.conn.execute("""INSERT INTO research_btc_parent_movements
                (btc_parent_movement_id,episode_policy_version,start_time_utc,end_time_utc,confirmed_at_utc,
                 direction,evidence_eligible,boundary_reason,observed_through_utc,price_source,state_json)
                VALUES(%s,%s,%s,%s,%s,'UP',true,'CAUSAL_CLOSE_REVERSAL',%s,'BINANCE_SPOT_BTCUSDT_1M','{}')""",
                (wave, self.formulas.PARENT_POLICY, parent_start, when+timedelta(minutes=5), parent_start, when))
        self.conn.execute("""INSERT INTO research_event_btc_movements
            (event_id,episode_policy_version,btc_parent_movement_id,decision_time_utc,btc_observed_close_utc,membership_status)
            VALUES(%s,%s,%s,%s,%s,'LIVE')""", (event_id, self.formulas.PARENT_POLICY, wave, when, close))
        self.conn.execute("""INSERT INTO research_ordered_formula_matches
            (candidate_key,event_id,symbol,direction,alert_time_utc,snapshot_id,entry_price,decision_features)
            VALUES(%s,%s,'BTC','LONG',%s,%s,100,%s::jsonb)""",
            (self.scope["candidate_key"], event_id, when, f"snapshot-{event_id}",
             '{"price_oi.aligned_score":70,"event.direction_mapping_valid":true,"event.analysis_direction":"LONG"}'))

    def queue(self, count=1):
        self.seed_qualification()
        start = self.grant["published_at_utc"]
        for i in range(count):
            # Separate minutes give each independently assigned parent a unique
            # causal start. No network request or external message is made.
            self.add_event(100+i, start+timedelta(minutes=i+1))
        self.now = start+timedelta(minutes=count+1)
        summary = self.store.enqueue(self.conn, now=self.now)
        self.assertEqual(summary["enqueued"], count, summary)
        self.conn.commit()

    def test_real_migrations_reapply_and_constraints_reject_invalid_states(self):
        self.queue()
        for filename in MIGRATIONS[-2:]:
            self.conn.execute((ROOT/"migrations"/filename).read_text(), prepare=False)
        self.conn.commit()
        self.assertTrue(self.store.available(self.conn))
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM research_ordered_experimental_deliveries").fetchone()["n"], 1)
        indices = {r["indexname"] for r in self.conn.execute("SELECT indexname FROM pg_indexes WHERE schemaname=%s", (self.schema,))}
        self.assertIn("idx_ordered_formula_matches_event_candidate", indices)
        for sql in (
            "UPDATE research_ordered_experimental_deliveries SET status='CLAIMED'",
            "UPDATE research_ordered_experimental_deliveries SET status='SENT'",
            "UPDATE research_ordered_experimental_deliveries SET direction='NEUTRAL'",
            "UPDATE research_ordered_experimental_deliveries SET payload='[]'::jsonb",
            "UPDATE research_ordered_experimental_eligibility SET ready=false",
            "INSERT INTO research_event_scan_cursors(queue_key,last_event_id,high_water_event_id) VALUES('bad',2,1)",
        ):
            with self.subTest(sql=sql), self.assertRaises(self.psycopg.errors.CheckViolation):
                with self.conn.transaction():
                    self.conn.execute(sql)
        with self.assertRaises(self.psycopg.errors.ForeignKeyViolation):
            with self.conn.transaction():
                self.conn.execute("UPDATE research_ordered_experimental_deliveries SET evidence_sha256='missing'")

    def test_database_publication_time_prevents_replay_and_wave_duplicates(self):
        grant = self.seed_qualification()
        self.assertGreater(grant["published_at_utc"], self.assessed)
        self.assertEqual(grant["evaluated_at_utc"], self.assessed)
        start = grant["published_at_utc"]
        self.add_event(100, start-timedelta(minutes=1))
        self.add_event(101, start+timedelta(minutes=1), wave="new-qualified-wave")
        self.add_event(102, start+timedelta(minutes=1, seconds=1), wave="new-qualified-wave")
        self.now = start+timedelta(minutes=2)
        result = self.store.enqueue(self.conn, now=self.now)
        self.assertEqual(result["enqueued"], 1, result)
        self.assertEqual(self.store.enqueue(self.conn, now=self.now)["enqueued"], 0)
        ids = [r["event_id"] for r in self.conn.execute("SELECT event_id FROM research_ordered_experimental_deliveries")]
        self.assertEqual(ids, [101])
        # A delayed older evaluation cannot overwrite newer published proof.
        older = deepcopy(self.result)
        older["research_ready"] = False
        older["evidence_sha256"] = self.validation.digest(older)
        self.add_evaluation(older, self.assessed-timedelta(seconds=1))
        self.store.publish_evaluation(self.conn, self.scope, older, now=self.assessed-timedelta(seconds=1), rows=[], common_window_rows={})
        retained = self.conn.execute("SELECT * FROM research_ordered_experimental_eligibility").fetchone()
        self.assertEqual(retained["evidence_sha256"], grant["evidence_sha256"])
        self.assertEqual(retained["published_at_utc"], grant["published_at_utc"])

    def test_claim_commit_send_ack_and_ambiguous_transport_are_durable(self):
        self.queue(2)
        first = self.store.claim(self.conn, now=self.now)
        self.assertEqual(first["status"], "CLAIMED")
        # Another real connection skips the row lock and claims a different item.
        with self.connect() as other:
            second = self.store.claim(other, now=self.now)
            self.assertNotEqual(first["delivery_id"], second["delivery_id"])
        self.conn.commit()
        self.assertTrue(self.store.begin_send(self.conn, first, now=self.now))
        self.conn.commit()
        with self.connect() as observer:
            row = observer.execute("SELECT status,attempts FROM research_ordered_experimental_deliveries WHERE delivery_id=%s", (first["delivery_id"],)).fetchone()
            self.assertEqual((row["status"], row["attempts"]), ("SENDING", 1))
        self.assertTrue(self.store.finish(self.conn, first, now=self.now, message_id=12345))
        self.assertFalse(self.store.finish(self.conn, first, now=self.now, message_id=12345))
        self.assertTrue(self.store.begin_send(self.conn, second, now=self.now))
        self.assertTrue(self.store.finish(self.conn, second, now=self.now, message_id=None, error="timeout; remote outcome unknown"))
        self.conn.commit()
        with self.connect() as observer:
            statuses = {r["status"] for r in observer.execute("SELECT status FROM research_ordered_experimental_deliveries")}
            self.assertEqual(statuses, {"SENT", "UNKNOWN"})
            self.assertIsNone(self.store.claim(observer, now=self.now+timedelta(minutes=2)))

    def test_claim_rollback_replays_but_sending_expiry_never_retries(self):
        self.queue()
        claimed = self.store.claim(self.conn, now=self.now)
        self.conn.rollback()
        replayed = self.store.claim(self.conn, now=self.now)
        self.assertEqual(replayed["delivery_id"], claimed["delivery_id"])
        self.assertNotEqual(replayed["claim_token"], claimed["claim_token"])
        self.conn.commit()
        # A stale CLAIMED lease is retryable because send has not begun.
        reclaimed = self.store.claim(self.conn, now=self.now+timedelta(seconds=91))
        self.assertEqual(reclaimed["delivery_id"], claimed["delivery_id"])
        self.assertFalse(self.store.begin_send(self.conn, replayed, now=self.now+timedelta(seconds=91)))
        self.assertTrue(self.store.begin_send(self.conn, reclaimed, now=self.now+timedelta(seconds=91)))
        self.conn.commit()
        self.assertIsNone(self.store.claim(self.conn, now=self.now+timedelta(seconds=182)))
        row = self.conn.execute("SELECT status,claim_token,last_error FROM research_ordered_experimental_deliveries").fetchone()
        self.assertEqual(row["status"], "UNKNOWN")
        self.assertIsNone(row["claim_token"])
        self.assertIn("RESULT_UNKNOWN", row["last_error"])

    def test_revocation_before_send_blocks_transport_and_cancels_pending(self):
        self.queue(2)
        claimed = self.store.claim(self.conn, now=self.now)
        self.conn.commit()
        with self.connect() as revoke:
            revoke.execute("UPDATE research_formula_alert_subscriptions SET active=false")
        self.assertFalse(self.store.begin_send(self.conn, claimed, now=self.now))
        self.assertIsNone(self.store.claim(self.conn, now=self.now+timedelta(seconds=91)))
        self.assertEqual({r["status"] for r in self.conn.execute("SELECT status FROM research_ordered_experimental_deliveries")}, {"CANCELLED"})

    def test_cursor_rollback_locking_finite_lap_and_unprocessed_suffix(self):
        when = self.assessed
        for event_id in range(1, 9):
            self.add_event(event_id, when, delivered=(event_id % 2 == 0), matching=False)
        self.conn.commit()
        def page(conn, limit=2):
            return self.scan.claim_event_page(conn, "integration-queue", limit=limit,
                predicate="e.delivery_status=%s", params=("DELIVERED",))
        self.assertEqual(page(self.conn), [2, 4])
        self.conn.commit()
        self.assertEqual(page(self.conn), [6, 8])
        self.conn.rollback()
        self.assertEqual(page(self.conn), [6, 8])
        with self.connect() as other:
            other.execute("SET LOCAL lock_timeout='100ms'")
            with self.assertRaises(self.psycopg.errors.LockNotAvailable):
                with other.transaction():
                    page(other)
        self.conn.commit()
        self.conn.execute("UPDATE research_events SET delivery_status='DELIVERED' WHERE event_id=1")
        self.add_event(9, when, matching=False)
        self.conn.commit()
        self.assertEqual(page(self.conn, limit=3), [1, 2, 4])
        self.scan.retain_unprocessed_tail(self.conn, "integration-queue", 2)
        self.conn.commit()
        self.assertEqual(page(self.conn, limit=3), [4, 6, 8])
        self.add_event(10, when, matching=False)
        self.assertEqual(page(self.conn, limit=3), [9])  # Current lap stops at 9.
        state = self.conn.execute("SELECT * FROM research_event_scan_cursors").fetchone()
        self.assertEqual((state["last_event_id"], state["high_water_event_id"]), (9, 9))
        self.conn.commit()
        self.assertEqual(page(self.conn, limit=20), [1, 2, 4, 6, 8, 9, 10])


if __name__ == "__main__":
    unittest.main(verbosity=2)
