"""Real disposable-PostgreSQL gate for the Stage-8 outcome DB adapter.

Only an explicit local/CI ``TEST_DATABASE_URL`` whose database name is visibly
test-only is accepted.  The test builds the real migrations 001--053, persists
five prospective facts and the deterministic selection, inserts authoritative
outcome rows, then replays the complete fact population and reads both outcome
routes in one caller-owned read-only REPEATABLE READ transaction. Caller output
remains diagnostic; only the independently replayed, persisted database-trigger
evaluation may qualify for research.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import os
import unittest

import research_stage8_contract as contract
import research_stage8_coverage_receipt as coverage
import research_stage8_outcome_db_adapter as adapter
from research_stage8_outcome_db_adapter_selftest import Fixture
import research_stage8_projection_db_adapter as projection_adapter
import research_stage8_registry as registry
import research_stage8_registry_postgres_selftest as registry_pg
import research_stage8_representative_selector as selector


class _CountingConnection:
    """Count adapter reads while transparently forwarding one real connection."""

    def __init__(self, connection):
        self._connection = connection
        self.executions = []

    @property
    def autocommit(self):
        return self._connection.autocommit

    def execute(self, statement, params=None, **kwargs):
        self.executions.append((str(statement), params))
        return self._connection.execute(statement, params, **kwargs)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class OutcomeAdapterPostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        registry_pg.Stage8RegistryPostgreSQLTests.setUpClass()

    def setUp(self):
        self.harness = registry_pg.Stage8RegistryPostgreSQLTests("runTest")
        self.harness.setUp()
        self.addCleanup(self.harness.doCleanups)
        self.binding = self.harness.binding
        self.selection = self._build_five_parent_path()

    def _set_disposable_test_clock(self, instant):
        """Set the trusted schema-local clock used only by migration triggers.

        Five distinct BTC representatives require five distinct 30-minute
        source slots.  Waiting for those slots would make the integration gate
        unbounded, so this disposable-schema test installs a schema-owner clock
        before registration.  Migration triggers explicitly put their trusted
        schema before ``pg_catalog``; ordinary adapter reads still use the real
        PostgreSQL transaction/clock authorities.
        """
        h = self.harness
        body = "SELECT TIMESTAMPTZ " + h.admin.execute(
            "SELECT quote_literal(%s::timestamptz) AS literal", (instant,),
        ).fetchone()["literal"]
        h.admin.execute(h.sql.SQL("""
            CREATE OR REPLACE FUNCTION {}.clock_timestamp()
            RETURNS timestamptz LANGUAGE sql IMMUTABLE AS {}
        """).format(h.sql.Identifier(h.schema), h.sql.Literal(body)))
        h.admin.commit()

    def _register_at_disposable_test_clock(self, frozen_at):
        h = self.harness
        self._set_disposable_test_clock(frozen_at)
        conn = h._connect(role=registry.REGISTRAR_ROLE)
        registered = registry.register_exact_binding_from_connection(
            conn, self.binding,
        )
        conn.commit()
        observed_freeze = registered["frozen_at_utc"]
        if not isinstance(observed_freeze, datetime):
            observed_freeze = datetime.fromisoformat(
                observed_freeze.replace("Z", "+00:00")
            )
        self.assertEqual(
            observed_freeze,
            frozen_at,
        )
        h._reset_admin()
        return registered

    def _build_five_parent_path(self):
        h = self.harness
        real_now = h.admin.execute(
            "SELECT pg_catalog.clock_timestamp() AS now"
        ).fetchone()["now"]
        # Leave ample room for five non-overlapping source slots and their
        # terminal outcome observations while keeping every timestamp in the
        # past relative to the real read snapshot.
        test_freeze = real_now - timedelta(days=3)
        registered = self._register_at_disposable_test_clock(test_freeze)
        frozen = registered["frozen_at_utc"]
        if not isinstance(frozen, datetime):
            frozen = datetime.fromisoformat(frozen.replace("Z", "+00:00"))
        attempts = [
            h._seed_attempt(
                attempt_id=index,
                decision=frozen + timedelta(hours=2 * index),
            )
            for index in range(1, 6)
        ]
        h.admin.commit()
        # All append-only trigger timestamps now follow the completed fixture,
        # while the adapter's transaction identity continues to use pg_catalog.
        self._set_disposable_test_clock(real_now - timedelta(hours=1))
        reader = h._connect(read_only=True, role=registry.READER_ROLE)
        handoff = coverage.read_bounded_attempt_cohort_from_connection(
            reader,
            start_utc=frozen,
            end_utc=attempts[-1]["source_candle_open_utc"] + timedelta(hours=1),
            symbols=tuple(self.binding["binding"]["scope"]["symbols"]),
        )
        self.assertEqual(handoff["attempt_ids"], [1, 2, 3, 4, 5])
        projected = projection_adapter.project_exact_binding_attempts_from_connection(
            reader, exact_binding=self.binding, attempt_ids=handoff["attempt_ids"],
        )
        reference = registry.registry_reference_from_connection(reader, self.binding)
        source_rows, authorities = [], []
        for projected_row in projected["rows"]:
            fact_item = projected_row["fact_ledger"][0]
            source_rows.append({
                "attempt_id": projected_row["attempt_id"],
                "fact": fact_item["fact"],
                "parent_membership_source": fact_item["parent_membership_source"],
                "noneligibility_proof": fact_item["noneligibility_proof"],
            })
            authorities.append({
                key: fact_item["fact_authority"].get(key)
                for key in selector._FACT_AUTHORITY_KEYS
            })
        selected = selector.select_representatives(
            self.binding, source_rows, fact_authorities=authorities,
            population_receipt=handoff["coverage_receipt"],
            expected_outcome_free_population_receipt_sha256=handoff[
                "outcome_free_population_receipt_sha256"
            ],
            registry_reference=reference,
            observed_source_hashes={
                "projection_source_sha256": reference[
                    "expected_projection_source_sha256"
                ],
                "selector_source_sha256": reference[
                    "expected_selector_source_sha256"
                ],
            },
        )
        self.assertEqual(selected["representative_count"], 5, selected)
        fact_conn = h._connect(role=registry.FACT_WRITER_ROLE)
        registry.append_projection_fact_batch_from_connection(
            fact_conn, self.binding, projection_result=projected,
            coverage_audit_receipt=handoff["coverage_receipt"],
            registry_reference=reference,
        )
        fact_conn.commit()
        selection_conn = h._connect(role=registry.SELECTOR_WRITER_ROLE)
        durable = registry.append_selection_receipt_from_connection(
            selection_conn, self.binding, selected,
        )
        selection_conn.commit()
        h._reset_admin()
        event_ids = [item["event_id"] for item in durable["representative_identities"]]
        events = h.admin.execute(
            "SELECT * FROM research_events WHERE event_id=ANY(%s) ORDER BY event_id",
            (event_ids,),
        ).fetchall()
        self.assertEqual(len(events), 5)
        for event in events:
            outcome = Fixture._outcome(None, event, "SUCCESS", 50)
            revised = event["alert_time_utc"] + timedelta(minutes=2)
            outcome.update(
                created_at_utc=revised,
                updated_at_utc=revised,
            )
            h._insert(
                "research_ordered_first_touch_outcomes",
                outcome,
            )
        h.admin.commit()
        return durable

    def _reader(self):
        h = self.harness
        # Use a fresh socket and change the session authorization before the
        # adapter starts.  Unlike the shared PGlite SET ROLE smoke path, this
        # gives current_user == session_user, carries no pre-existing TEMP
        # objects, and exercises only the dedicated reader's privileges.
        conn = h.psycopg.connect(
            h.dsn, row_factory=h.dict_row, connect_timeout=5,
            prepare_threshold=None,
            options=("-c statement_timeout=10000 -c lock_timeout=1000 "
                     "-c TimeZone=UTC"),
        )
        self.addCleanup(conn.close)
        conn.execute(h.sql.SQL("SET SESSION AUTHORIZATION {}").format(
            h.sql.Identifier(registry.READER_ROLE)
        ))
        conn.execute(h.sql.SQL(
            "SET search_path TO {}, pg_catalog, pg_temp"
        ).format(h.sql.Identifier(h.schema)))
        conn.execute("SET statement_timeout TO '10000ms'")
        conn.commit()
        conn.isolation_level = h.psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        return conn

    def test_only_persisted_server_replay_and_outcomes_qualify_research_only(self):
        reader = _CountingConnection(self._reader())
        result = adapter.evaluate_selection_outcomes_from_connection(
            reader, self.binding,
            selection_record_sha256=self.selection["selection_record_sha256"],
        )
        self.assertTrue(result["evaluation"]["atomic_gate_passed"], result)
        self.assertFalse(result["research_qualified"], result)
        self.assertFalse(result["evaluation"]["authoritative_fact_replay_verified"])
        self.assertFalse(result["fact_replay_receipt"]["durable_selection_server_recomputed"])
        self.assertIn("SERVER_DB_REPLAY_ATTESTATION_REQUIRED",
                      result["evaluation"]["qualification_blockers"])
        self.assertEqual(result["fact_replay_receipt"]["status"], "VERIFIED")
        self.assertEqual(result["fact_replay_receipt"]["attempt_ids"], [1, 2, 3, 4, 5])
        self.assertEqual(result["evidence_receipt"]["query_count"], adapter.MAX_QUERIES)
        self.assertEqual(len(reader.executions), adapter.MAX_QUERIES)
        self.assertEqual(
            sum("stage8-outcome:" in sql for sql, _ in reader.executions),
            adapter.OUTCOME_QUERY_COUNT,
        )
        self.assertEqual(
            sum("stage8-projection:" in sql for sql, _ in reader.executions),
            adapter.PROJECTION_REPLAY_QUERY_COUNT,
        )
        self.assertEqual(result["transaction"]["database_role"], registry.READER_ROLE)
        self.assertFalse(result["live_authorized"])
        self.assertFalse(result["telegram_authorized"])
        self.assertFalse(result["trade_authorized"])
        payload = dict(result["persistence_payload"])
        supplied = payload.pop("persistence_payload_sha256")
        self.assertEqual(contract.digest(payload), supplied)
        writer = self.harness._connect(role=registry.EVALUATOR_WRITER_ROLE)
        persisted = registry.append_evaluation_receipt_from_connection(
            writer, self.binding,
            persistence_payload=result["persistence_payload"],
        )
        writer.commit()
        self.assertTrue(persisted["server_replay_verified"], persisted)
        self.assertTrue(persisted["atomic_gate_passed"], persisted)
        self.assertTrue(persisted["research_qualified"], persisted)
        self.assertTrue(persisted["evaluation"]["research_qualified"])
        self.assertEqual(persisted["evaluation"]["status"],
                         "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY")
        self.assertEqual(persisted["evaluation"]["qualification_blockers"], [])
        self.assertEqual(persisted["persistence_payload"], result["persistence_payload"])
        self.assertFalse(persisted["persistence_payload"]["evaluation"]["research_qualified"])
        self.assertEqual(persisted["evaluation_sha256"],
                         contract.digest(persisted["evaluation"]))
        self.assertNotEqual(persisted["evaluation_sha256"], result["evaluation_sha256"])
        for document in (persisted, persisted["evaluation"]):
            for key in ("live_authorized", "telegram_authorized", "trade_authorized"):
                self.assertIs(document[key], False)
        self._assert_reader_acl_and_forged_input_rejected()
        self._assert_causal_parent_change_fails_closed()

    def _assert_causal_parent_change_fails_closed(self):
        h = self.harness
        h._reset_admin()
        parent_id = self.selection["representative_identities"][0][
            "btc_parent_movement_id"
        ]
        h.admin.execute(
            "UPDATE research_btc_parent_movements "
            "SET direction=CASE direction WHEN 'UP' THEN 'DOWN' ELSE 'UP' END "
            "WHERE btc_parent_movement_id=%s",
            (parent_id,),
        )
        h.admin.commit()
        result = adapter.evaluate_selection_outcomes_from_connection(
            self._reader(), self.binding,
            selection_record_sha256=self.selection["selection_record_sha256"],
        )
        self.assertFalse(result["research_qualified"])
        self.assertEqual(result["fact_replay_receipt"]["status"], "UNKNOWN")
        self.assertIn(
            "AUTHORITATIVE_FACT_SOURCE_REPLAY_NOT_VERIFIED",
            result["evaluation"]["qualification_blockers"],
        )
        writer = h._connect(role=registry.EVALUATOR_WRITER_ROLE)
        persisted = registry.append_evaluation_receipt_from_connection(
            writer, self.binding,
            persistence_payload=result["persistence_payload"],
        )
        writer.commit()
        self.assertIs(persisted["server_replay_verified"], False)
        self.assertIs(persisted["research_qualified"], False)
        self.assertIs(persisted["evaluation"]["research_qualified"], False)
        self.assertNotEqual(persisted["evaluation"]["status"],
                            "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY")
        self.assertTrue(persisted["evaluation"]["qualification_blockers"])

    def _assert_reader_acl_and_forged_input_rejected(self):
        h = self.harness
        relations = (
            "research_stage8_registry_read_v1", "research_stage8_selection_read_v1",
            "research_stage8_fact_batch_read_v1", "research_stage8_fact_read_v1",
            "research_stage8_fact_seal_read_v1", "research_events",
            "research_ordered_first_touch_outcomes", "research_common_window_metrics",
            "research_max_pain_snapshot_sets", "research_prospective_anchor_attempts",
            "research_prospective_anchor_slots", "research_event_btc_movements",
            "research_btc_parent_movements", "research_btc_price_bars",
        )
        h._reset_admin()
        for relation in relations:
            with self.subTest(relation=relation):
                privileges = h.admin.execute("""
                    SELECT has_table_privilege(%s,%s,'SELECT') AS can_select,
                           has_table_privilege(%s,%s,'INSERT') AS can_insert,
                           has_table_privilege(%s,%s,'UPDATE') AS can_update,
                           has_table_privilege(%s,%s,'DELETE') AS can_delete
                """, (
                    registry.READER_ROLE, relation, registry.READER_ROLE, relation,
                    registry.READER_ROLE, relation, registry.READER_ROLE, relation,
                )).fetchone()
                self.assertEqual(privileges, {
                    "can_select": True, "can_insert": False,
                    "can_update": False, "can_delete": False,
                })
        reader = self._reader()
        with self.assertRaises(self.harness.psycopg.errors.ReadOnlySqlTransaction):
            reader.execute("DELETE FROM research_common_window_metrics")
        reader.rollback()
        reader = self._reader()
        with self.assertRaises(TypeError):
            adapter.evaluate_selection_outcomes_from_connection(
                reader, self.binding,
                selection_record_sha256=self.selection["selection_record_sha256"],
                representatives=[], probability_outcomes=[], asymmetry_metrics=[],
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
