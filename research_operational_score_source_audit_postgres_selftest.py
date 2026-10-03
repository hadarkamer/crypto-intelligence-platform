"""Real PostgreSQL gates for the complete Stage-8 EVALUABLE source graph.

Requires an explicit local/CI TEST_DATABASE_URL; never uses a runtime URL.
Migrations 001–021 run in a random disposable schema. Their explicit public
regclass references are rebound to that schema, with no constraint/trigger
changes. Fixtures come from the production anchor, capture, outcome and BTC
builders. No worker, price request, publisher or Telegram transport is run.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_operational_score_source_audit as audit
import research_prospective_anchors as anchors
from research_operational_score_source_audit_selftest import (
    BASE, DECISION, MAX_AGE, _anchor, _block, _outcome, _parent, _snapshot,
)
from research_watch_score_capture_selftest import archive_payload


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class OperationalSourceAuditPostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb

        cls.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(cls.dsn)
        if (info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (info.get("dbname", "").startswith("test_")
                        or info.get("dbname", "").endswith("_test"))):
            raise ValueError("Integration requires an explicit local/CI test database")
        cls.psycopg, cls.sql, cls.Jsonb = psycopg, sql, Jsonb
        cls.dict_row = staticmethod(dict_row)

    def setUp(self):
        # Append-only guards forbid clearing/reusing an anchor corpus. Give
        # each test its own schema instead of disabling any guard.
        self.schema = "test_operational_source_audit_" + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5,
            prepare_threshold=None,
            options="-c statement_timeout=10000 -c lock_timeout=1000")
        self.addCleanup(self.cleanup_schema)
        self.conn.execute(self.sql.SQL("CREATE SCHEMA {}").format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema)))
        migrations = sorted((Path(__file__).parent / "migrations").glob("*.sql"))
        self.applied_migrations = []
        for path in migrations:
            if int(path.name[:3]) > 21:
                continue
            source = path.read_text().replace("'public.research_", f"'{self.schema}.research_")
            self.conn.execute(source, prepare=False)
            self.applied_migrations.append(path.name)
        self.conn.commit()
        self.attempt, self.slot, self.events = _anchor()
        self.snapshot = _snapshot()

    def cleanup_schema(self):
        self.conn.rollback()
        self.conn.close()
        with self.psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as cleanup:
            cleanup.execute(self.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                self.sql.Identifier(self.schema)))

    def insert(self, table, row):
        """Keep all persisted columns; ignore builder-only convenience fields."""
        columns = self.conn.execute("""SELECT column_name, data_type
            FROM information_schema.columns WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position""", (self.schema, table)).fetchall()
        values = {column["column_name"]: (
            self.Jsonb(row[column["column_name"]], dumps=anchors._canonical)
            if column["data_type"] in ("json", "jsonb") else row[column["column_name"]]
        ) for column in columns if column["column_name"] in row}
        self.assertTrue(values, table)
        self.conn.execute(self.sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            self.sql.Identifier(table),
            self.sql.SQL(",").join(map(self.sql.Identifier, values)),
            self.sql.SQL(",").join(self.sql.Placeholder() for _ in values),
        ), tuple(values.values()))

    def seed(self):
        self.insert("research_prospective_anchor_attempts", self.attempt)
        for event in self.events:
            self.insert("research_events", event | {"runtime_session_id": self.schema})
        self.insert("research_prospective_anchor_slots", self.slot)
        self.insert_snapshot(self.snapshot)
        membership, parent, bar = _parent(self.events[0])
        self.insert("research_btc_price_bars", bar)
        self.insert("research_btc_parent_movements", parent)
        for event in self.events:
            self.insert("research_event_btc_movements", _parent(event)[0])
            # Both directions observe the same upward first touch.
            status = "SUCCESS" if event["direction"] == "LONG" else "FAILURE"
            self.insert("research_ordered_first_touch_outcomes", _outcome(event, status=status))
        self.conn.commit()

    def insert_snapshot(self, snapshot):
        # Migration 007 defers a full set/manifest/row consistency check until
        # COMMIT; a set-only fixture is intentionally not sufficient.
        self.insert("research_max_pain_snapshot_sets", snapshot)
        payload = archive_payload(_block(snapshot))
        for table, key in (("research_max_pain_snapshot_symbols", "symbols"),
                           ("research_max_pain_snapshot_rows", "rows")):
            for row in payload[key]:
                self.insert(table, row | {"snapshot_set_id": snapshot["snapshot_set_id"]})

    def read_connection(self, *, mapping=True, read_only=True):
        conn = self.psycopg.connect(self.dsn, connect_timeout=5,
            prepare_threshold=None,
            row_factory=self.dict_row if mapping else None,
            options="-c statement_timeout=10000 -c lock_timeout=1000")
        self.addCleanup(conn.close)
        conn.execute(self.sql.SQL("SET search_path TO {}").format(
            self.sql.Identifier(self.schema)))
        conn.commit()
        conn.read_only = read_only
        conn.isolation_level = self.psycopg.IsolationLevel.REPEATABLE_READ
        return conn

    def page(self, *, conn=None, **overrides):
        options = dict(symbols=["BTC"], start_utc=BASE,
            end_utc=BASE + timedelta(hours=1), max_capture_age_seconds=MAX_AGE,
            windows=[60], thresholds_bps=[50], page_size=10)
        options.update(overrides)
        if conn is not None:
            return audit.audit_anchor_attempt_page_from_connection(conn, **options)
        with self.read_connection() as reader:
            return audit.audit_anchor_attempt_page_from_connection(reader, **options)

    def test_complete_evaluable_graph_roundtrips_with_real_sql_and_no_writes(self):
        self.seed()
        self.assertEqual(len(self.applied_migrations), 21)
        result = self.page()
        self.assertEqual(result["snapshot_consistency"], "CALLER_TRANSACTION_SNAPSHOT")
        self.assertEqual((result["examined"], result["emitted"]), (1, 1))
        self.assertTrue(result["population_page_complete"])
        self.assertIsNone(result["next_cursor"])
        row = result["rows"][0]
        self.assertEqual(row["anchor_authority"]["status"], "VALID", row["anchor_authority"])
        self.assertEqual(row["capture"]["status"], "VALID", row["capture"])
        self.assertEqual(row["capture"]["snapshot_reference"]["snapshot_set_id"], 101)
        self.assertEqual(len(row["outcome_cells"]), 2)
        for cell in row["outcome_cells"]:
            self.assertEqual(cell["status"], "VALID", cell)
            self.assertEqual(cell["source_status"],
                             "SUCCESS" if cell["direction"] == "LONG" else "FAILURE")
        for parent in row["parent_memberships"].values():
            self.assertEqual(parent["status"], "VALID", parent)
        self.assertEqual(row["delivery_state"], "UNKNOWN_NOT_AUDITED")
        # PostgreSQL itself, not a mock, enforces the caller's read-only gate.
        reader = self.read_connection()
        self.page(conn=reader)
        with self.assertRaises(self.psycopg.errors.ReadOnlySqlTransaction):
            reader.execute("DELETE FROM research_prospective_anchor_attempts")
        reader.rollback()

    def test_unevaluable_and_excluded_attempts_survive_bounded_pagination(self):
        self.seed()
        for attempt_id, kwargs in ((2, {"missing": True}), (3, {"eligible": False})):
            attempt, _, _ = _anchor(attempt_id=attempt_id, **kwargs)
            self.insert("research_prospective_anchor_attempts", attempt)
        self.conn.commit()
        paging = dict(page_size=1, symbols=["BTC", "ETH"])
        first = self.page(**paging)
        self.assertEqual(first["high_water_attempt_id"], 3)
        self.assertTrue(first["has_more"])
        # A new attempt committed between pages must not extend this run.
        attempt, _, _ = _anchor(attempt_id=4, symbol="ETH", missing=True)
        self.insert("research_prospective_anchor_attempts", attempt)
        self.conn.commit()
        second = self.page(**paging, cursor=first["next_cursor"])
        third = self.page(**paging, cursor=second["next_cursor"])
        self.assertEqual([first["rows"][0]["attempt"]["attempt_id"],
                          second["rows"][0]["attempt"]["attempt_id"],
                          third["rows"][0]["attempt"]["attempt_id"]], [1, 2, 3])
        self.assertFalse(third["has_more"])
        for result, status in ((second, "UNEVALUABLE"), (third, "COVERAGE_EXCLUDED")):
            row = result["rows"][0]
            self.assertEqual(row["attempt"]["evaluation_status"], status)
            self.assertEqual(row["anchor_authority"]["status"], "NOT_APPLICABLE")
            self.assertEqual(len(row["outcome_cells"]), 2)
            self.assertTrue(all(cell["status"] == "UNKNOWN" for cell in row["outcome_cells"]))
        with self.assertRaisesRegex(ValueError, "different query"):
            self.page(**paging, thresholds_bps=[25], cursor=first["next_cursor"])

    def test_latest_invalid_capture_does_not_fall_back_to_older_valid_capture(self):
        self.seed()
        newer = deepcopy(self.snapshot)
        newer.update(snapshot_set_id=102, snapshot_key="e" * 64,
                     created_at_utc=DECISION - timedelta(minutes=1))
        _block(newer)["version"] = "unsupported-capture-version"
        self.insert_snapshot(newer)
        self.conn.commit()
        row = self.page()["rows"][0]
        self.assertEqual(row["capture"]["snapshot_reference"]["snapshot_set_id"], 102)
        self.assertEqual(row["capture"]["status"], "UNKNOWN")
        self.assertEqual(row["anchor_authority"]["status"], "VALID")

    def test_late_persistence_cannot_supply_decision_time_capture(self):
        self.snapshot["created_at_utc"] = DECISION + timedelta(seconds=1)
        self.seed()
        captured = self.page()["rows"][0]["capture"]
        self.assertEqual(captured["status"], "UNKNOWN")
        self.assertIsNone(captured["snapshot_reference"])
        self.assertIn("NO_DURABLY_PRIOR_WATCH_CAPTURE_WITHIN_MAX_AGE", captured["reasons"])

    def test_missing_outcome_and_membership_leave_attempt_and_both_cells(self):
        self.seed()
        self.conn.execute("DELETE FROM research_ordered_first_touch_outcomes WHERE event_id=%s",
                          (self.events[0]["event_id"],))
        self.conn.execute("DELETE FROM research_event_btc_movements WHERE event_id=%s",
                          (self.events[1]["event_id"],))
        self.conn.commit()
        row = self.page()["rows"][0]
        cells = {cell["direction"]: cell for cell in row["outcome_cells"]}
        self.assertEqual(row["anchor_authority"]["status"], "VALID")
        self.assertIn("OUTCOME_ROW_MISSING", cells["LONG"]["reasons"])
        self.assertEqual(cells["SHORT"]["status"], "VALID")
        self.assertEqual(row["parent_memberships"]["LONG"]["status"], "VALID")
        self.assertIn("PARENT_MEMBERSHIP_MISSING", row["parent_memberships"]["SHORT"]["reasons"])

    def test_open_outcome_is_unknown_even_when_real_row_satisfies_schema(self):
        self.seed()
        self.conn.execute("DELETE FROM research_ordered_first_touch_outcomes WHERE event_id=%s",
                          (self.events[0]["event_id"],))
        self.insert("research_ordered_first_touch_outcomes", _outcome(self.events[0], status="OPEN"))
        self.conn.commit()
        cells = {cell["direction"]: cell for cell in self.page()["rows"][0]["outcome_cells"]}
        self.assertEqual(cells["LONG"]["source_status"], "OPEN")
        self.assertEqual(cells["LONG"]["status"], "UNKNOWN")
        self.assertEqual(cells["SHORT"]["status"], "VALID")

    def test_outcome_route_mismatch_is_unknown_without_dropping_its_cell(self):
        self.seed()
        self.conn.execute("""UPDATE research_ordered_first_touch_outcomes
            SET market_pair='ETHUSDT' WHERE event_id=%s""", (self.events[0]["event_id"],))
        self.conn.commit()
        row = self.page()["rows"][0]
        self.assertEqual(row["anchor_authority"]["status"], "VALID")
        cells = {cell["direction"]: cell for cell in row["outcome_cells"]}
        self.assertEqual(cells["LONG"]["status"], "UNKNOWN")
        self.assertIn("OUTCOME_MARKET_PAIR_MISMATCH", cells["LONG"]["reasons"])
        self.assertEqual(cells["SHORT"]["status"], "VALID")

    def test_missing_actual_btc_bar_does_not_promote_membership_metadata(self):
        self.seed()
        self.conn.execute("DELETE FROM research_btc_price_bars")
        self.conn.commit()
        row = self.page()["rows"][0]
        self.assertEqual(row["anchor_authority"]["status"], "VALID")
        self.assertEqual(row["capture"]["status"], "VALID")
        for member in row["parent_memberships"].values():
            self.assertEqual(member["status"], "UNKNOWN")
            self.assertEqual(member["membership"]["membership_status"], "LIVE")
            self.assertIn("BTC_SOURCE_BAR_MISSING", member["reasons"])

    def test_writable_connection_and_tuple_row_factory_are_rejected(self):
        self.seed()
        writable = self.read_connection(read_only=False)
        with self.assertRaisesRegex(ValueError, "read-only"):
            self.page(conn=writable)
        writable.rollback()
        tuples = self.read_connection(mapping=False)
        with self.assertRaisesRegex(ValueError, "dict_row"):
            self.page(conn=tuples)
        tuples.rollback()

    def test_real_anchor_append_only_guard_rejects_posthoc_feature_rewrite(self):
        self.seed()
        self.conn.execute("""DO $$ BEGIN
            BEGIN
                UPDATE research_prospective_anchor_slots SET feature_bundle_sha256=repeat('0',64);
                RAISE EXCEPTION 'append-only guard did not run';
            EXCEPTION WHEN raise_exception THEN
                IF SQLERRM <> 'research_prospective_anchor_slots is append-only' THEN
                    RAISE;
                END IF;
            END;
        END $$""")
        self.conn.commit()
        self.assertEqual(self.page()["rows"][0]["anchor_authority"]["status"], "VALID")


if __name__ == "__main__":
    unittest.main()
