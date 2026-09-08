"""Real PostgreSQL sequence-history completeness, snapshot and rollback gates.

Only an explicit local/CI TEST_DATABASE_URL is accepted. Every test creates a
disposable schema using the actual archive migration and runs the production
metadata query and JSON projection. No production data or transport is used.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import os
from pathlib import Path
import unittest
from uuid import uuid4


class ObservedConnection:
    """Observe page bounds and inject a committed concurrent source change."""

    def __init__(self, conn, *, after_first_page=None, before_projection=None):
        self.conn = conn
        self.after_first_page = after_first_page
        self.before_projection = before_projection
        self.source_cursors = []
        self.pages = []
        self.projections = []

    def cursor(self, *args, **kwargs):
        real = self.conn.cursor(*args, **kwargs)
        if not kwargs.get("name"):
            raise AssertionError("Sequence metadata must use a named server cursor")
        self.source_cursors.append(real)
        observer = self

        class ObservedCursor:
            def __enter__(self):
                real.__enter__()
                return self

            def __exit__(self, *exc):
                return real.__exit__(*exc)

            def execute(self, *execute_args, **execute_kwargs):
                return real.execute(*execute_args, **execute_kwargs)

            def fetchmany(self, size):
                if not 0 < size <= 512:
                    raise AssertionError("Metadata fetch exceeded 512 rows")
                rows = real.fetchmany(size)
                observer.pages.append([row["event_id"] for row in rows])
                if len(observer.pages) == 1 and observer.after_first_page:
                    observer.after_first_page()
                return rows

        return ObservedCursor()

    def execute(self, query, params=None, **kwargs):
        if "WITH picked AS MATERIALIZED" in query:
            ids = list(params[0])
            if not 0 < len(ids) <= 64:
                raise AssertionError("JSON projection exceeded 64 rows")
            self.projections.append(ids)
            if self.before_projection:
                self.before_projection(ids)
        return self.conn.execute(query, params, **kwargs)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class SequenceHistoryPostgreSQLTests(unittest.TestCase):
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
        cls.store = importlib.import_module("research_formula_ordered_store")

    def connect(self):
        conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row,
            connect_timeout=5, options="-c statement_timeout=10000 -c lock_timeout=1000")
        conn.execute(self.sql.SQL("SET search_path TO {}").format(
            self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema = "test_sequence_history_" + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row,
            connect_timeout=5, options="-c statement_timeout=10000 -c lock_timeout=1000")
        self.conn.execute(self.sql.SQL("CREATE SCHEMA {}").format(
            self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL("SET search_path TO {}").format(
            self.sql.Identifier(self.schema)))
        self.conn.execute((Path(__file__).resolve().parent / "migrations" /
            "001_research_archive_v1.sql").read_text(), prepare=False)
        self.conn.execute("CREATE TABLE pending_screen_marker (id integer PRIMARY KEY, value integer)")
        self.conn.execute("INSERT INTO pending_screen_marker VALUES (1,0)")
        self.conn.commit()
        self.now = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as cleanup:
            cleanup.execute(self.sql.SQL("DROP SCHEMA {} CASCADE").format(
                self.sql.Identifier(self.schema)))

    def add_events(self, first, last=None, *, when=None, symbol="BTC", direction="LONG",
                   kind="ALERT", delivered=True, conn=None):
        conn = conn or self.conn
        conn.execute("""INSERT INTO research_events (
            event_id,schema_version,event_kind,event_type,alert_time_utc,symbol,
            direction,setup_key,event_fingerprint,strategy_version,code_version,
            runtime_session_id,delivery_status,current_price,target_price,score,
            source_side,engine_snapshot)
            SELECT id,'test',%s,'MAGNET_ALERT',%s,%s,%s,repeat('s',64),
                lpad(id::text,64,'0'),'test','test','test',%s,100,101,70,%s,
                jsonb_build_object('watch_scan_id','scan-'||id,
                    'magnet',jsonb_build_object('side',%s::text),
                    'market_evidence',jsonb_build_object('modules',jsonb_build_object(
                        'futures_flow',jsonb_build_object('score',65,'direction',%s::text),
                        'spot_flow',jsonb_build_object('score',66,'direction',%s::text))),
                    'unused_large_payload',repeat('not-projected',100))
            FROM generate_series(%s::bigint,%s::bigint) id""",
            (kind, when or self.now - timedelta(hours=1), symbol, direction,
             "DELIVERED" if delivered else "UNKNOWN", direction,
             "UPPER" if direction == "LONG" else "LOWER", direction, direction,
             first, first if last is None else last))

    def changed(self, event_id, *, when=None, symbol="BTC", direction="LONG"):
        return {"event_id": event_id, "symbol": symbol, "direction": direction,
            "alert_time_utc": when or self.now, "event_type": "MAGNET_ALERT",
            "event_kind": "ALERT", "delivery_status": "DELIVERED", "current_price": 102,
            "engine_snapshot": {"watch_scan_id": "current-" + str(event_id),
                "magnet": {"side": "UPPER" if direction == "LONG" else "LOWER"},
                "market_evidence": {"modules": {
                    "futures_flow": {"score": 75, "direction": direction}}}}}

    def features(self, event):
        return (self.store.evaluator.extract_event_features(event)
                | self.store.questions.extended_features(event))

    def assert_cursor_closed(self, observed):
        self.assertEqual(len(observed.source_cursors), 1)
        self.assertTrue(observed.source_cursors[0].closed)
        self.assertEqual(self.conn.execute("""SELECT name FROM pg_cursors
            WHERE name LIKE 'ordered_sequence_%'""").fetchall(), [])

    def test_dense_window_keeps_every_scan_and_evidence_after_old_5000_cap(self):
        self.add_events(1, 6001, when=self.now - timedelta(hours=2))
        self.conn.execute("""UPDATE research_events SET
            alert_time_utc=%s,event_type='SPOT_CVD_HIGH',
            engine_snapshot=jsonb_set(engine_snapshot,
                '{market_evidence,modules,futures_flow,score}','88'::jsonb)
            WHERE event_id=6001""", (self.now - timedelta(minutes=1),))
        self.conn.commit()
        current = self.changed(9000)
        observed = ObservedConnection(self.conn)
        history, stats = self.store.load_sequence_history(observed, {9000: current})

        self.assertEqual([row["event_id"] for row in history], list(range(1, 6002)))
        self.assertEqual(stats["sequence_source_rows"], 6001)
        self.assertEqual(stats["sequence_projected_rows"], 6001)
        self.assertEqual(stats["sequence_source_pages"], 12)
        self.assertTrue(all("unused_large_payload" not in row["engine_snapshot"]
                            for row in history))
        raw = self.conn.execute("SELECT * FROM research_events ORDER BY event_id").fetchall()
        baseline = self.store.questions.sequence_features(current, self.features(current),
            [(row, self.features(row)) for row in raw])
        actual = self.store.questions.sequence_features(current, self.features(current),
            [(row, self.features(row)) for row in history])
        self.assertEqual(actual, baseline)
        self.assertEqual(actual["sequence.240m.prior_distinct_scans"], 6001)
        self.assertEqual(actual["sequence.240m.previous_primary_family"], "SPOT_CVD")
        self.assertEqual(actual["sequence.240m.futures_cvd.score_change"], -13)
        for candidate in self.store.evaluator.candidate_catalog(include_extended=True):
            self.assertEqual(
                self.store.evaluator.matches(candidate, self.features(current) | actual, "LONG"),
                self.store.evaluator.matches(candidate, self.features(current) | baseline, "LONG"))
        self.assert_cursor_closed(observed)

    def test_disjoint_and_overlapping_windows_exclude_large_gap_and_preserve_boundaries(self):
        early = self.now - timedelta(days=2)
        # Same symbol/direction gap rows caused the former global cap failure.
        self.add_events(10000, 16000, when=self.now - timedelta(days=1))
        self.add_events(50, when=self.now - timedelta(hours=4))
        self.add_events(10, when=self.now - timedelta(hours=2))
        self.add_events(90, when=self.now - timedelta(hours=6))
        self.add_events(20, when=self.now - timedelta(hours=6, microseconds=1))
        self.add_events(30, when=self.now)  # Upper endpoint is not prior evidence.
        self.add_events(60, when=early - timedelta(hours=4))
        self.add_events(70, when=early)
        self.add_events(80, when=self.now - timedelta(minutes=1), direction="SHORT")
        self.add_events(100, when=self.now - timedelta(minutes=1), symbol="ETH")
        self.add_events(110, when=self.now - timedelta(minutes=1), symbol="SOL")
        self.add_events(120, kind="SIGNAL_STATE_CHANGE")
        self.add_events(130, kind="DECISION_SAMPLE")
        self.add_events(140, delivered=False)
        self.conn.commit()
        changed = {row["event_id"]: row for row in (
            self.changed(9000), self.changed(10, when=self.now - timedelta(hours=2)),
            self.changed(9001, when=early), self.changed(9002, direction="SHORT"),
            self.changed(9003, symbol="ETH"))}
        observed = ObservedConnection(self.conn)
        history, stats = self.store.load_sequence_history(observed, changed)
        self.assertEqual([row["event_id"] for row in history], [10, 50, 60, 80, 90, 100])
        self.assertEqual(stats["sequence_source_rows"], 6)
        self.assertEqual(observed.projections, [[10, 50, 60, 80, 90, 100]])
        self.assert_cursor_closed(observed)

    def test_later_page_source_drift_closes_cursor_and_caller_rolls_back_screen_work(self):
        self.add_events(1, 600)
        self.conn.commit()
        changed = {9000: self.changed(9000)}
        altered = []

        def drift(ids):
            if 550 in ids:
                with self.connect() as other:
                    other.execute("UPDATE research_events SET symbol='ETH' WHERE event_id=550")
                altered.append(550)

        observed = ObservedConnection(self.conn, before_projection=drift)
        with self.assertRaisesRegex(RuntimeError, "lost or changed source rows"):
            with self.conn.transaction():
                self.conn.execute("UPDATE pending_screen_marker SET value=1 WHERE id=1")
                self.store.load_sequence_history(observed, changed)
        self.assertEqual(altered, [550])
        self.assertEqual(len(observed.pages), 2)
        self.assertEqual(self.conn.execute(
            "SELECT value FROM pending_screen_marker WHERE id=1").fetchone()["value"], 0)
        self.assert_cursor_closed(observed)
        # Committed external drift survives; only the caller's pending work rolls back.
        self.assertEqual(self.conn.execute(
            "SELECT symbol FROM research_events WHERE event_id=550").fetchone()["symbol"], "ETH")

    def test_server_cursor_keeps_source_snapshot_across_insert_and_late_delivery(self):
        self.add_events(1, 600)
        self.conn.execute("UPDATE research_events SET delivery_status='UNKNOWN' WHERE event_id=550")
        self.conn.commit()

        def concurrent_changes():
            with self.connect() as other:
                self.add_events(700, conn=other)
                other.execute("UPDATE research_events SET delivery_status='DELIVERED' WHERE event_id=550")

        current = {9000: self.changed(9000)}
        observed = ObservedConnection(self.conn, after_first_page=concurrent_changes)
        history, stats = self.store.load_sequence_history(observed, current)
        self.assertEqual([row["event_id"] for row in history],
                         [i for i in range(1, 601) if i != 550])
        self.assertEqual(stats["sequence_source_rows"], 599)
        self.assert_cursor_closed(observed)
        self.conn.commit()
        # A later complete load sees both changes, without extending the earlier pass.
        history, stats = self.store.load_sequence_history(self.conn, current)
        self.assertEqual([row["event_id"] for row in history], list(range(1, 601)) + [700])
        self.assertEqual(stats["sequence_source_rows"], 601)


if __name__ == "__main__":
    unittest.main()
