"""Real PostgreSQL gates for bounded, causal BTC membership assignment.

Only an explicit local/CI TEST_DATABASE_URL is accepted. Each test applies the
actual archive, BTC membership and cursor migrations in a disposable schema.
No price request, Telegram transport or production database is used.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import os
from pathlib import Path
import unittest
from uuid import uuid4


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class BTCMembershipQueuePostgreSQLTests(unittest.TestCase):
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
        cls.worker = importlib.import_module("research_btc_episode_worker")
        cls.policy = importlib.import_module("research_btc_parent_movement")
        cls.queue_key = "btc-episode-membership:" + cls.policy.POLICY_VERSION

    def connect(self):
        conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row,
            connect_timeout=5, options="-c statement_timeout=10000 -c lock_timeout=1000")
        conn.execute(self.sql.SQL("SET search_path TO {}").format(
            self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema = "test_btc_membership_queue_" + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row,
            connect_timeout=5, options="-c statement_timeout=10000 -c lock_timeout=1000")
        self.conn.execute(self.sql.SQL("CREATE SCHEMA {}").format(
            self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL("SET search_path TO {}").format(
            self.sql.Identifier(self.schema)))
        migrations = Path(__file__).resolve().parent / "migrations"
        for filename in ("001_research_archive_v1.sql", "021_btc_parent_movements_v1.sql"):
            self.conn.execute((migrations / filename).read_text(), prepare=False)
        # Outcome calculation has separate integration coverage. Membership
        # admits any current-method row, including OPEN and DATA_MISSING.
        self.conn.execute("""
            CREATE TABLE research_ordered_first_touch_outcomes (
                event_id bigint NOT NULL REFERENCES research_events(event_id),
                method_version text NOT NULL, status text NOT NULL,
                PRIMARY KEY(event_id,method_version)
            );
            CREATE TABLE research_ordered_formula_matches (
                event_id bigint, candidate_key text,
                PRIMARY KEY(candidate_key,event_id)
            );
        """, prepare=False)
        self.conn.execute((migrations / "038_runtime_event_scan_bounds.sql").read_text(),
                          prepare=False)
        self.start = datetime(2026, 9, 7, tzinfo=timezone.utc)
        self.through = self.start + timedelta(hours=3)
        self.conn.execute("""INSERT INTO research_btc_parent_movements (
            btc_parent_movement_id,episode_policy_version,start_time_utc,
            confirmed_at_utc,direction,evidence_eligible,boundary_reason,
            observed_through_utc,price_source,state_json)
            VALUES ('test-parent',%s,%s,%s,'UP',TRUE,'CAUSAL_CLOSE_REVERSAL',
                    %s,%s,'{}'::jsonb)""",
            (self.policy.POLICY_VERSION, self.start, self.start,
             self.through, self.policy.SOURCE))
        self.add_bars()
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as cleanup:
            cleanup.execute(self.sql.SQL("DROP SCHEMA {} CASCADE").format(
                self.sql.Identifier(self.schema)))

    def add_bars(self):
        self.conn.execute("""INSERT INTO research_btc_price_bars
            (open_time_utc,close_time_utc,open,high,low,close)
            SELECT t,t+INTERVAL '1 minute'-INTERVAL '1 millisecond',100,101,99,100
            FROM generate_series(%s::timestamptz,%s::timestamptz,INTERVAL '1 minute') t
            ON CONFLICT DO NOTHING""", (self.start, self.through))

    def add_events(self, first, last=None, *, sample=False, delivered=True,
                   when=None, direction="LONG", event_kind=None):
        last = first if last is None else last
        kind = event_kind or ("DECISION_SAMPLE" if sample else "ALERT")
        status = "NOT_APPLICABLE" if sample else ("DELIVERED" if delivered else "UNKNOWN")
        self.conn.execute("""INSERT INTO research_events (
            event_id,schema_version,event_kind,event_type,alert_time_utc,symbol,
            direction,setup_key,event_fingerprint,strategy_version,code_version,
            runtime_session_id,delivery_status)
            SELECT id,'test',%s,'TEST',%s,'BTC',%s,repeat('s',64),
                   lpad(id::text,64,'0'),'test','test','test',%s
            FROM generate_series(%s::bigint,%s::bigint) id""",
            (kind, when or self.start + timedelta(minutes=10, seconds=30),
             direction, status, first, last))

    def add_outcomes(self, ids, *, method="ordered-first-touch-v7", status="OPEN"):
        self.conn.execute("""INSERT INTO research_ordered_first_touch_outcomes
            (event_id,method_version,status)
            SELECT event_id,%s,%s FROM unnest(%s::bigint[]) event_id
            ON CONFLICT DO NOTHING""", (method, status, list(ids)))

    def assign(self):
        result = self.worker._assign_due_events(self.conn, through=self.through)
        self.assertLessEqual(result["members_written"], 500)
        self.assertEqual(result["members_written"], sum(result[key] for key in
            ("live", "btc_data_missing", "boundary_unverified")))
        return result

    def members(self):
        return self.conn.execute("""SELECT * FROM research_event_btc_movements
            WHERE episode_policy_version=%s ORDER BY event_id""",
            (self.policy.POLICY_VERSION,)).fetchall()

    def member_ids(self):
        return {row["event_id"] for row in self.members()}

    def cursor(self):
        return self.conn.execute("""SELECT last_event_id,high_water_event_id
            FROM research_event_scan_cursors WHERE queue_key=%s""",
            (self.queue_key,)).fetchone()

    def test_recent_native_and_sample_bypass_long_unadmitted_history(self):
        self.add_events(1, 600, sample=True)
        recent = self.through - timedelta(seconds=30)
        self.add_events(9000, when=recent, direction="NEUTRAL")
        self.add_events(9001, sample=True, when=recent)
        self.add_outcomes([9001], status="DATA_MISSING")
        self.conn.commit()

        result = self.assign()
        self.assertEqual(result["members_written"], 2)
        self.assertEqual(self.member_ids(), {9000, 9001})
        self.assertEqual(self.cursor(), {"last_event_id": 244,
                                         "high_water_event_id": 9001})
        self.assertTrue(all(row["membership_status"] == "LIVE" for row in self.members()))

    def test_history_progress_survives_restart_and_growing_recent_tail(self):
        self.add_events(1, 900)
        self.conn.commit()
        self.assign()
        self.assertTrue(set(range(1, 245)).issubset(self.member_ids()))
        self.assertEqual(self.cursor(), {"last_event_id": 244,
                                         "high_water_event_id": 900})
        self.conn.commit()
        self.conn.close()
        self.conn = self.connect()

        for lap, expected_cursor in enumerate((488, 732, 900), start=1):
            self.add_events(10000 + lap,
                when=self.through - timedelta(seconds=30 - lap))
            self.conn.commit()
            self.assign()
            self.assertEqual(self.cursor(), {"last_event_id": expected_cursor,
                                             "high_water_event_id": 900})
            self.assertIn(10000 + lap, self.member_ids())
            self.conn.commit()
        self.assertTrue(set(range(1, 901)).issubset(self.member_ids()))

    def test_sample_admitted_after_its_page_is_recovered_on_the_next_lap(self):
        self.add_events(1, 600, sample=True)
        self.conn.commit()
        self.assertEqual(self.assign()["members_written"], 0)
        self.assertEqual(self.cursor()["last_event_id"], 244)
        self.conn.commit()
        self.add_outcomes([1])
        self.conn.commit()

        for expected_cursor in (488, 600):
            self.assertEqual(self.assign()["members_written"], 0)
            self.assertEqual(self.cursor()["last_event_id"], expected_cursor)
            self.conn.commit()
        self.assertEqual(self.assign()["members_written"], 1)
        self.assertEqual(self.member_ids(), {1})

    def test_assignment_and_cursor_roll_back_together(self):
        self.add_events(1, 600)
        self.conn.commit()
        first_result = self.assign()
        first_ids = self.member_ids()
        first_cursor = self.cursor()
        self.assertGreater(first_result["members_written"], 0)
        self.conn.rollback()
        self.assertEqual(self.member_ids(), set())
        self.assertIsNone(self.cursor())
        self.assertEqual(self.assign(), first_result)
        self.assertEqual(self.member_ids(), first_ids)
        self.assertEqual(self.cursor(), first_cursor)

    def test_already_assigned_samples_never_probe_ordered_outcomes_again(self):
        self.add_events(1, sample=True)
        self.add_outcomes([1])
        self.assertEqual(self.assign()["members_written"], 1)
        self.conn.commit()
        # Turn any admission lookup into an observable failure. The assigned
        # sample must leave the materialized queue before this view is touched;
        # the new native alert must also bypass sample-only outcome admission.
        self.conn.execute("""
            ALTER TABLE research_ordered_first_touch_outcomes RENAME TO test_outcome_rows;
            CREATE FUNCTION test_outcome_admission_probe() RETURNS text
            LANGUAGE plpgsql VOLATILE AS $$
            BEGIN RAISE EXCEPTION 'unexpected ordered admission probe'; END $$;
            CREATE VIEW research_ordered_first_touch_outcomes AS
                SELECT event_id,test_outcome_admission_probe() AS method_version,status
                FROM test_outcome_rows;
        """, prepare=False)
        self.add_events(2)
        self.assertEqual(self.assign()["members_written"], 1)
        self.assertEqual(self.member_ids(), {1, 2})
        self.conn.commit()
        # Prove the sentinel is active for an unassigned sample, rather than
        # a fixture whose expression PostgreSQL can remove as unused.
        self.add_events(3, sample=True)
        self.conn.execute("INSERT INTO test_outcome_rows VALUES (3,'ordered-first-touch-v7','OPEN')")
        with self.assertRaisesRegex(self.psycopg.errors.RaiseException,
                                    "unexpected ordered admission probe"):
            self.assign()
        self.conn.rollback()

    def test_existing_admission_and_exact_source_time_bounds_are_preserved(self):
        self.add_events(1, direction="NEUTRAL")
        self.add_events(2, delivered=False)
        self.add_outcomes([2])  # Outcomes do not authorize an undelivered alert.
        self.add_events(3, sample=True)
        self.conn.execute("UPDATE research_events SET delivery_status='UNKNOWN' WHERE event_id=3")
        self.add_outcomes([3], status="DATA_MISSING")
        self.add_events(4, sample=True)
        self.add_outcomes([4], method="ordered-first-touch-v6")
        self.add_events(5, when=self.start - timedelta(microseconds=1))
        self.add_events(6, when=self.through + timedelta(microseconds=1))
        self.add_events(7, when=self.through)
        self.add_events(8, when=self.start)
        self.add_events(9, event_kind="SIGNAL_STATE_CHANGE")
        self.add_outcomes([9])
        self.conn.commit()

        self.assertEqual(self.assign()["members_written"], 4)
        self.assertEqual(self.member_ids(), {1, 3, 7, 8})
        statuses = {row["event_id"]: row["membership_status"] for row in self.members()}
        self.assertEqual(statuses[8], "BTC_DATA_MISSING")
        self.assertEqual(statuses[7], "LIVE")

    def test_membership_uses_only_the_latest_already_closed_bar(self):
        decision = self.start + timedelta(minutes=10, seconds=30)
        self.add_events(1, when=decision)
        self.conn.commit()
        self.assign()
        member = self.members()[0]
        self.assertEqual(member["membership_status"], "LIVE")
        self.assertEqual(member["btc_observed_close_utc"],
            self.start + timedelta(minutes=10) - timedelta(milliseconds=1))
        self.assertLessEqual(member["btc_observed_close_utc"], decision)
        self.assertLess(decision - member["btc_observed_close_utc"], timedelta(minutes=1))

    def test_unverified_parent_keeps_its_existing_membership_status(self):
        self.conn.execute("""UPDATE research_btc_parent_movements SET
            evidence_eligible=FALSE,confirmed_at_utc=NULL,
            boundary_reason='LEFT_BOUNDARY_UNVERIFIED'""")
        self.add_events(1)
        self.conn.commit()
        self.assertEqual(self.assign()["boundary_unverified"], 1)
        member = self.members()[0]
        self.assertEqual(member["membership_status"], "BOUNDARY_UNVERIFIED")
        self.assertEqual(member["btc_parent_movement_id"], "test-parent")

    def test_missing_source_remains_recorded_after_later_source_backfill(self):
        decision = self.start + timedelta(minutes=10, seconds=30)
        self.add_events(1, when=decision)
        # A future bar exists, but no source has closed at the decision time.
        self.conn.execute("DELETE FROM research_btc_price_bars WHERE close_time_utc<=%s",
                          (decision,))
        self.conn.commit()
        self.assertEqual(self.assign()["btc_data_missing"], 1)
        member = self.members()[0]
        self.assertEqual(member["membership_status"], "BTC_DATA_MISSING")
        self.assertIsNone(member["btc_parent_movement_id"])
        self.assertIsNone(member["btc_observed_close_utc"])
        self.conn.commit()

        self.add_bars()
        self.conn.commit()
        self.assertEqual(self.assign()["members_written"], 0)
        self.assertEqual(self.members(), [member])


if __name__ == "__main__":
    unittest.main()
