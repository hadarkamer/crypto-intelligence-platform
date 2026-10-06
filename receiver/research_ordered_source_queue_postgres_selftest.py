"""Real PostgreSQL tests for finite ordered-v7 source selection.

Uses only an explicit local/CI TEST_DATABASE_URL and disposable schemas. No
price provider, Telegram transport, production URL, or production data is used.
The authorization view is a controlled fixture; its policy has separate tests.
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path
import unittest
from uuid import uuid4


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "TEST_DATABASE_URL is required for PostgreSQL integration")
class OrderedSourceQueuePostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row

        cls.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(cls.dsn)
        if (info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (info.get("dbname", "").startswith("test_")
                        or info.get("dbname", "").endswith("_test"))):
            raise ValueError("Integration requires an explicit local/CI test database")
        cls.psycopg, cls.sql, cls.dict_row = psycopg, sql, staticmethod(dict_row)
        cls.worker = importlib.import_module("research_outcome_worker")

    def connect(self):
        conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row,
            connect_timeout=5, options="-c statement_timeout=10000 -c lock_timeout=1000")
        conn.execute(self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema = "test_ordered_source_" + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5)
        self.conn.execute(self.sql.SQL("CREATE SCHEMA {}").format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema)))
        self.conn.execute("""
            CREATE TABLE research_events (
                event_id bigint PRIMARY KEY, event_fingerprint text,
                alert_time_utc timestamptz NOT NULL, symbol text DEFAULT 'BTC',
                direction text DEFAULT 'LONG', event_type text DEFAULT 'TEST',
                setup_key text, event_kind text, delivery_status text,
                current_price double precision DEFAULT 100,
                target_price double precision DEFAULT 101,
                engine_snapshot jsonb DEFAULT '{}'
            );
            CREATE TABLE research_outcome_event_rejections (
                event_id bigint, rejection_policy_version text,
                PRIMARY KEY(event_id,rejection_policy_version)
            );
            CREATE TABLE research_ordered_first_touch_outcomes (
                event_id bigint, window_minutes integer, threshold_bps integer,
                method_version text, status text,
                observed_through_utc timestamptz, updated_at_utc timestamptz,
                PRIMARY KEY(event_id,window_minutes,threshold_bps,method_version)
            );
            CREATE TABLE authorized_samples (event_id bigint PRIMARY KEY);
            CREATE VIEW research_prospective_shadow_events AS
                SELECT event_id FROM authorized_samples;
            CREATE TABLE research_ordered_formula_matches (
                event_id bigint, candidate_key text,
                PRIMARY KEY(candidate_key,event_id)
            );
        """, prepare=False)
        self.conn.execute((Path(__file__).resolve().parent / "migrations" /
            "038_runtime_event_scan_bounds.sql").read_text(), prepare=False)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute(self.sql.SQL("DROP SCHEMA {} CASCADE").format(self.sql.Identifier(self.schema)))

    def add_events(self, first, last=None, *, sample=False, authorized=True,
                   delivered=True, age_minutes=10):
        last = first if last is None else last
        self.conn.execute("""INSERT INTO research_events
            (event_id,event_fingerprint,alert_time_utc,event_kind,delivery_status)
            SELECT id,id::text,date_trunc('minute',NOW())-%s*INTERVAL '1 minute',%s,%s
            FROM generate_series(%s::bigint,%s::bigint) id""",
            (age_minutes, "DECISION_SAMPLE" if sample else "ALERT",
             "NOT_APPLICABLE" if sample else ("DELIVERED" if delivered else "UNKNOWN"),
             first, last))
        if sample and authorized:
            self.conn.execute("INSERT INTO authorized_samples SELECT generate_series(%s::bigint,%s::bigint)", (first, last))

    def finish(self, ids, *, status="SUCCESS"):
        if not ids:
            return
        self.conn.execute("""INSERT INTO research_ordered_first_touch_outcomes
            (event_id,window_minutes,threshold_bps,method_version,status,
             observed_through_utc,updated_at_utc)
            SELECT e.event_id,w,b,%s,%s,e.alert_time_utc,NOW()-INTERVAL '7 hours'
            FROM research_events e CROSS JOIN unnest(%s::integer[]) w
            CROSS JOIN unnest(%s::integer[]) b WHERE e.event_id=ANY(%s::bigint[])
            ON CONFLICT DO NOTHING""",
            (self.worker._ORDERED_FIRST_TOUCH_METHOD_VERSION, status,
             list(self.worker._ORDERED_FIRST_TOUCH_WINDOWS),
             [int(round(float(t)*100)) for t in
              self.worker.research_ordered_first_touch.SUPPORTED_THRESHOLDS_PCT],
             list(ids)))

    def due(self):
        rows = self.worker.ResearchOutcomeWorker._load_ordered_first_touch_due_events(self.conn, 8)
        self.assertLessEqual(len(rows), 8)
        self.assertTrue(all(not any(key.startswith("_ordered_") or key.endswith("_due_ids")
                                    for key in row) for row in rows))
        ids = [row["event_id"] for row in rows]
        self.assertEqual(len(ids), len(set(ids)))
        return ids

    def cursor(self, kind="samples"):
        return self.conn.execute("SELECT last_event_id,high_water_event_id FROM research_event_scan_cursors WHERE queue_key=%s",
            ("ordered-v7-new-" + kind + "-v1",)).fetchone()

    def test_single_sample_lane_retains_every_unselected_due_id_across_restart(self):
        self.add_events(1, 20, sample=True)
        self.conn.commit()
        self.assertEqual(self.due(), list(range(1, 9)))
        self.assertEqual(self.cursor()["last_event_id"], 8)
        self.finish(range(1, 9))
        self.conn.commit()
        self.conn.close()
        self.conn = self.connect()
        self.assertEqual(self.due(), list(range(9, 17)))
        self.assertEqual(self.cursor()["last_event_id"], 16)
        self.finish(range(9, 17))
        self.conn.commit()
        self.assertEqual(self.due(), list(range(17, 21)))

    def test_unauthorized_prefix_advances_without_delaying_recent_native(self):
        self.add_events(1, 256, sample=True, authorized=False)
        self.add_events(257, 259, sample=True)
        self.add_events(1000, age_minutes=2)
        self.conn.commit()
        self.assertEqual(self.due(), [1000])
        self.assertEqual(self.cursor()["last_event_id"], 128)
        self.finish([1000])
        self.conn.commit()
        self.assertEqual(self.due(), [])
        self.assertEqual(self.cursor()["last_event_id"], 256)
        self.conn.commit()
        self.assertEqual(self.due(), [257, 258, 259])

    def test_complete_history_uses_a_finite_page_before_new_native(self):
        self.add_events(1, 1024, sample=True)
        self.finish(range(1, 1025))
        self.add_events(10000, age_minutes=2)
        self.conn.commit()
        self.assertEqual(self.due(), [10000])
        self.assertEqual(self.cursor()["last_event_id"], 128)
        self.assertEqual(self.cursor()["high_water_event_id"], 10000)

    def test_recent_and_historical_native_and_sample_lanes_all_progress(self):
        self.add_events(1, 24, age_minutes=60)
        self.add_events(101, 124, sample=True, age_minutes=60)
        self.add_events(1001, 1130, age_minutes=2)
        self.conn.commit()
        first = self.due()
        self.assertEqual(len(first), 8)
        self.assertTrue({1, 101, 1130}.issubset(first), first)
        self.finish(first)
        self.conn.commit()
        self.add_events(1140, age_minutes=2)
        self.conn.commit()
        second = self.due()
        self.assertIn(1140, second)
        self.assertTrue(any(1 <= i <= 24 for i in second), second)
        self.assertTrue(any(101 <= i <= 124 for i in second), second)
        self.assertFalse(set(first) & set(second))

    def test_rejected_open_rows_cannot_consume_authorized_sample_slots(self):
        self.add_events(1, 8)
        self.finish(range(1, 9), status="OPEN")
        self.conn.execute("""INSERT INTO research_outcome_event_rejections
            SELECT generate_series(1,8),%s""",
            (self.worker._ALERT_REFERENCE_REJECTION_POLICY_VERSION,))
        self.add_events(101, 124, sample=True)
        self.conn.commit()
        self.assertEqual(self.due(), list(range(101, 109)))
        self.assertEqual(self.cursor()["last_event_id"], 108)

    def test_rollback_replays_selection_and_cursor_state(self):
        self.add_events(1, 20, sample=True)
        self.conn.commit()
        first = self.due()
        self.conn.rollback()
        self.assertIsNone(self.cursor())
        self.assertEqual(self.due(), first)

    def test_finite_next_lap_recovers_late_delivery(self):
        self.add_events(1, delivered=False)
        self.add_events(2, 129, sample=True, authorized=False)
        self.conn.commit()
        self.assertEqual(self.due(), [])
        self.assertEqual(self.cursor("alerts")["last_event_id"], 129)
        self.conn.commit()
        self.conn.execute("UPDATE research_events SET delivery_status='DELIVERED' WHERE event_id=1")
        self.conn.commit()
        self.assertEqual(self.due(), [1])


if __name__ == "__main__":
    unittest.main()
