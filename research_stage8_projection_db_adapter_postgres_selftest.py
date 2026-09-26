"""Optional real-PostgreSQL gate for the trusted Stage-8 projection adapter.

Requires an explicit local/CI ``TEST_DATABASE_URL``.  The test creates a
random disposable schema, applies migrations 001--021 there, and never uses a
runtime or production URL.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_prospective_anchors as anchors
from research_operational_score_source_audit_selftest import (
    DECISION,
    _anchor,
    _block,
    _outcome,
    _parent,
    _snapshot,
)
from research_watch_score_capture_selftest import archive_payload
import research_stage8_projection_db_adapter as adapter
import research_stage8_contract as contract


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class ProjectionAdapterPostgreSQLTests(unittest.TestCase):
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
        self.schema = "test_stage8_projection_adapter_" + uuid4().hex
        self.writer = self.psycopg.connect(
            self.dsn, row_factory=self.dict_row, connect_timeout=5,
            prepare_threshold=None,
            options="-c statement_timeout=10000 -c lock_timeout=1000",
        )
        self.writer.execute(
            self.sql.SQL("CREATE SCHEMA {}").format(self.sql.Identifier(self.schema))
        )
        self.writer.execute(
            self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema))
        )
        for path in sorted((Path(__file__).parent / "migrations").glob("*.sql")):
            if int(path.name[:3]) > 21:
                continue
            source = path.read_text().replace(
                "'public.research_", f"'{self.schema}.research_"
            )
            self.writer.execute(source, prepare=False)
        self.writer.commit()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.writer.rollback()
        self.writer.close()
        with self.psycopg.connect(
                self.dsn, autocommit=True, connect_timeout=5) as cleanup:
            cleanup.execute(
                self.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    self.sql.Identifier(self.schema)
                )
            )

    def _insert(self, table, row):
        columns = self.writer.execute(
            """SELECT column_name, data_type FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
            (self.schema, table),
        ).fetchall()
        values = {
            column["column_name"]: (
                self.Jsonb(row[column["column_name"]], dumps=anchors._canonical)
                if column["data_type"] in ("json", "jsonb")
                else row[column["column_name"]]
            )
            for column in columns if column["column_name"] in row
        }
        self.assertTrue(values, table)
        self.writer.execute(
            self.sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                self.sql.Identifier(table),
                self.sql.SQL(",").join(map(self.sql.Identifier, values)),
                self.sql.SQL(",").join(self.sql.Placeholder() for _ in values),
            ),
            tuple(values.values()),
        )

    def _insert_snapshot(self, snapshot):
        self._insert("research_max_pain_snapshot_sets", snapshot)
        payload = archive_payload(_block(snapshot))
        for table, key in (
            ("research_max_pain_snapshot_symbols", "symbols"),
            ("research_max_pain_snapshot_rows", "rows"),
        ):
            for row in payload[key]:
                self._insert(table, row | {"snapshot_set_id": snapshot["snapshot_set_id"]})

    def _seed(self):
        attempt, slot, events = _anchor()
        snapshot = deepcopy(_snapshot())
        snapshot["available_at_utc"] = DECISION - timedelta(minutes=2)
        snapshot["created_at_utc"] = DECISION - timedelta(minutes=1)
        self._insert("research_prospective_anchor_attempts", attempt)
        for event in events:
            self._insert(
                "research_events", event | {"runtime_session_id": self.schema}
            )
        self._insert("research_prospective_anchor_slots", slot)
        self._insert_snapshot(snapshot)
        membership, parent, bar = _parent(events[0])
        self._insert("research_btc_price_bars", bar)
        self._insert("research_btc_parent_movements", parent)
        self.writer.commit()
        return attempt, snapshot

    def _reader(self):
        conn = self.psycopg.connect(
            self.dsn, row_factory=self.dict_row, connect_timeout=5,
            prepare_threshold=None,
            options="-c statement_timeout=5000 -c lock_timeout=1000",
        )
        self.addCleanup(conn.close)
        conn.execute(
            self.sql.SQL("SET search_path TO {}").format(self.sql.Identifier(self.schema))
        )
        conn.execute("SET statement_timeout TO '5s'")
        conn.commit()
        conn.read_only = True
        conn.isolation_level = self.psycopg.IsolationLevel.REPEATABLE_READ
        return conn

    def test_real_sql_selects_complete_authority_without_outcomes(self):
        attempt, snapshot = self._seed()
        reader = self._reader()
        result = adapter.project_attempts_from_connection(
            reader, attempt_ids=[attempt["attempt_id"]]
        )
        self.assertEqual(result["population_receipt"]["query_count"], 8)
        self.assertTrue(result["population_receipt"]["population_complete"])
        self.assertEqual(result["archive_snapshot_high_water_id"], snapshot["snapshot_set_id"])
        row = result["rows"][0]
        self.assertEqual(row["projection"]["fact_count"], 96)
        self.assertTrue(any(
            item["fact"]["knowledge_status"] == "KNOWN"
            and item["parent_membership_evidence"]["validation_status"] == "VALID"
            for item in row["fact_ledger"]
        ))
        exact = adapter.project_exact_binding_attempts_from_connection(
            reader,
            exact_binding=contract.exact_binding(
                scope_id="BINANCE_BTC",
                candidate_id="FUTURES_FLOW_ALIGNED65_LONG",
                threshold_bps=50,
            ),
            attempt_ids=[attempt["attempt_id"]],
        )
        self.assertEqual(
            exact["projection_mode"], adapter.EXACT_BINDING_PROJECTION_MODE,
        )
        self.assertEqual(exact["population_receipt"]["query_count"], 8)
        self.assertEqual(exact["rows"][0]["projection"]["fact_count"], 1)
        self.assertEqual(len(exact["rows"][0]["fact_ledger"]), 1)
        # PostgreSQL, not the mock test, enforces that this same connection is
        # incapable of mutating the source graph.
        with self.assertRaises(self.psycopg.errors.ReadOnlySqlTransaction):
            reader.execute("DELETE FROM research_prospective_anchor_attempts")
        reader.rollback()

    def test_parent_selection_identity_is_independent_of_materialization_and_outcomes(self):
        attempt, _ = self._seed()
        binding = contract.exact_binding(
            scope_id="BINANCE_BTC", candidate_id="FUTURES_FLOW_ALIGNED65_LONG",
            threshold_bps=50,
        )

        def project_identity():
            reader = self._reader()
            ledger = adapter.project_exact_binding_attempts_from_connection(
                reader, exact_binding=binding, attempt_ids=[attempt["attempt_id"]],
            )["rows"][0]["fact_ledger"][0]
            reader.rollback()
            evidence = ledger["parent_membership_evidence"]
            self.assertEqual(evidence["validation_status"], "VALID")
            return evidence, adapter.representative_selector.canonical_selection_fact_identity(
                binding, ledger["fact"], source_attempt_evaluation_status="EVALUABLE",
                parent_authority_class="LIVE", parent_membership_evidence=evidence,
            )

        self.assertEqual(self.writer.execute(
            "SELECT count(*) AS n FROM research_event_btc_movements"
        ).fetchone()["n"], 0)
        self.assertEqual(self.writer.execute(
            "SELECT count(*) AS n FROM research_ordered_first_touch_outcomes"
        ).fetchone()["n"], 0)
        before = project_identity()
        for event in _anchor()[2]:
            self._insert("research_event_btc_movements", {
                "event_id": event["event_id"],
                "episode_policy_version": adapter.btc_parent.POLICY_VERSION,
                "decision_time_utc": event["alert_time_utc"],
                "btc_parent_movement_id": None,
                "btc_observed_close_utc": None,
                "membership_status": "BTC_DATA_MISSING",
            })
            self._insert("research_ordered_first_touch_outcomes", _outcome(event) | {
                "threshold_bps": 50,
            })
        self.writer.commit()
        self.assertEqual(project_identity(), before)


if __name__ == "__main__":
    unittest.main()
