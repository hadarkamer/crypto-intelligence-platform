"""Dependency-light guards for the Stage-8 durable registry boundary."""
from __future__ import annotations

from copy import deepcopy
import inspect
import os
from pathlib import Path
import unittest
from unittest import mock

import research_stage8_contract as contract
import research_stage8_registry as registry


class _NeverConnect:
    def __init__(self):
        self.calls = []

    def connect(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise AssertionError("connection must not be attempted")


class _MappingCursor:
    def __init__(self, value=None):
        self.value = value

    def fetchone(self):
        return self.value


class _SchemaPinConnection:
    def __init__(self):
        self.calls = []

    def execute(self, statement, params=()):
        self.calls.append((statement, params))
        if "current_schemas(false)" in statement:
            return _MappingCursor({
                "current_user": registry.READER_ROLE,
                "current_schema": "public",
                "explicit_schemas": ["public", "pg_catalog"],
            })
        if "qualified_registry_oid" in statement:
            return _MappingCursor({
                "current_schema": "stage8_research",
                "resolved_schemas": [
                    "stage8_research", "pg_catalog", "pg_temp_7",
                ],
                "schema_usage": True, "schema_create": False,
                "qualified_registry_oid": 1234,
                "unqualified_registry_oid": 1234,
            })
        return _MappingCursor()


class Stage8RegistryTests(unittest.TestCase):
    def setUp(self):
        self.binding = contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="FUTURES_FLOW_ALIGNED65_LONG",
            threshold_bps=50,
        )

    def test_verifier_profile_hashes_the_full_trust_chain(self):
        manifest = registry.implementation_artifacts()
        self.assertEqual(manifest["version"], registry.ARTIFACT_MANIFEST_VERSION)
        self.assertEqual(set(manifest["files"]), {
            "canonical_price_path", "common_window_metrics", "contract",
            "coverage_receipt", "source_audit", "watch_capture",
            "projection", "projection_db_adapter", "selector", "acceptance",
            "outcome_db_adapter",
            "registry_adapter", "registry_migration",
        })
        for value in manifest["files"].values():
            path = Path(registry.__file__).resolve().parent / value["path"]
            self.assertTrue(path.is_file())
            self.assertRegex(value["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(set(registry.expected_watch_code_manifest()), {
            "alert_engine.py", "coinglass_flow_engine.py",
            "coinglass_oi_regime_service.py", "live_price_provider.py",
            "market_confidence_engine.py", "time_family_engine.py",
        })

    def test_runtime_version_monkeypatch_fails_before_any_registration(self):
        with mock.patch.object(registry.selector, "VERSION", "mutated"):
            with self.assertRaisesRegex(RuntimeError, "RUNTIME_VERSION_DRIFT:selector"):
                registry.implementation_artifacts()

    def test_generic_database_url_and_ambient_pg_variables_are_never_fallbacks(self):
        fake = _NeverConnect()
        environ = {
            "DATABASE_URL": "postgresql://ambient",
            "RESEARCH_DATABASE_URL": "postgresql://ambient-research",
            "PGHOST": "ambient-host", "PGUSER": "ambient-user",
            "PGPASSWORD": "ambient-secret", "PGDATABASE": "ambient-db",
        }
        with mock.patch.dict(os.environ, environ, clear=True), \
             mock.patch.object(registry, "psycopg", fake):
            with self.assertRaisesRegex(RuntimeError, "NOT_EXPLICITLY_CONFIGURED"):
                registry._connect("RESEARCH_STAGE8_READER_DATABASE_URL", read_only=True)
        self.assertEqual(fake.calls, [])

    def test_partial_conninfo_is_rejected_before_connect_even_with_ambient_pg(self):
        fake = _NeverConnect()
        environ = {
            "RESEARCH_STAGE8_READER_DATABASE_URL": "dbname=stage8",
            "PGHOST": "ambient-host", "PGUSER": "ambient-user",
            "PGPASSWORD": "ambient-secret", "PGPORT": "5432",
            "PGSSLMODE": "disable",
        }
        with mock.patch.dict(os.environ, environ, clear=True), \
             mock.patch.object(registry, "psycopg", fake), \
             mock.patch.object(registry, "conninfo_to_dict",
                               return_value={"dbname": "stage8"}):
            with self.assertRaisesRegex(RuntimeError, "TARGET_NOT_FULLY_EXPLICIT"):
                registry._connect("RESEARCH_STAGE8_READER_DATABASE_URL", read_only=True)
        self.assertEqual(fake.calls, [])

    def test_all_environment_entrypoints_use_distinct_dedicated_urls_and_roles(self):
        source = inspect.getsource(registry)
        self.assertIn("RESEARCH_STAGE8_DATABASE_SCHEMA", source)
        for name in (
            "RESEARCH_STAGE8_REGISTRAR_DATABASE_URL",
            "RESEARCH_STAGE8_FACT_DATABASE_URL",
            "RESEARCH_STAGE8_SELECTOR_DATABASE_URL",
            "RESEARCH_STAGE8_EVALUATOR_DATABASE_URL",
            "RESEARCH_STAGE8_READER_DATABASE_URL",
        ):
            self.assertIn(name, source)
        for role in (
            registry.REGISTRAR_ROLE, registry.FACT_WRITER_ROLE,
            registry.SELECTOR_WRITER_ROLE, registry.EVALUATOR_WRITER_ROLE,
            registry.READER_ROLE,
        ):
            self.assertIn(role, source)

    def test_environment_wrapper_requires_explicit_trusted_schema(self):
        fake = _NeverConnect()
        environ = {
            "RESEARCH_STAGE8_READER_DATABASE_URL":
                "postgresql://stage8:secret@localhost:5432/stage8",
        }
        explicit = {
            "host": "localhost", "port": "5432", "dbname": "stage8",
            "user": "stage8", "password": "secret", "sslmode": "disable",
        }
        with mock.patch.dict(os.environ, environ, clear=True), \
             mock.patch.object(registry, "psycopg", fake), \
             mock.patch.object(registry.coverage_receipt,
                               "_explicit_connection_fields",
                               return_value=explicit):
            with self.assertRaisesRegex(
                RuntimeError, "DATABASE_SCHEMA_NOT_EXPLICITLY_CONFIGURED"
            ):
                registry._connect(
                    "RESEARCH_STAGE8_READER_DATABASE_URL", read_only=True,
                )
        self.assertEqual(fake.calls, [])

    def test_explicit_custom_schema_need_not_be_on_ambient_search_path(self):
        conn = _SchemaPinConnection()
        result = registry._pin_trusted_schema(
            conn, expected_role=registry.READER_ROLE,
            trusted_schema="stage8_research",
        )
        self.assertEqual(result, "stage8_research")
        set_config = [
            params for statement, params in conn.calls
            if "set_config('search_path'" in statement
        ]
        self.assertEqual(
            set_config, [(r'"stage8_research",pg_catalog,pg_temp',)],
        )

    def test_registration_insert_has_no_caller_clock_or_server_owned_field(self):
        source = inspect.getsource(registry.register_exact_binding_from_connection)
        self.assertNotIn("frozen_at", source)
        self.assertNotIn("freeze_id", source)
        self.assertNotIn("registry_record_sha256", source)
        self.assertIn("implementation_artifacts", source)
        self.assertIn("expected_watch_code_manifest", source)

    def test_registry_row_tampering_is_detected(self):
        artifacts = registry.implementation_artifacts()
        watch = registry.expected_watch_code_manifest()
        manifest = contract.frozen_manifest()
        profile = {
            "version": registry.VERIFIER_PROFILE_VERSION,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "implementation_artifacts_sha256": contract.digest(artifacts),
            "expected_watch_code_manifest_sha256": contract.digest(watch),
        }
        record = {
            "version": registry.REGISTRY_RECORD_VERSION,
            "exact_binding": self.binding,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "hash_version": manifest["hash_version"],
            "source_audit_version": manifest["source"]["audit_version"],
            "candidate_version": manifest["candidates"]["version"],
            "parent_policy_version": manifest["independence"]["parent_policy_version"],
            "implementation_artifacts": artifacts,
            "expected_watch_code_manifest": watch,
            "verifier_profile": profile,
            "freeze_id": "a" * 64,
            "frozen_at_utc": "2026-09-13T00:00:00.000000Z",
            "registered_by": registry.REGISTRAR_ROLE,
        }
        inner = self.binding["binding"]
        row = {
            "exact_binding": self.binding,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "manifest_sha256": contract.MANIFEST_SHA256,
            "contract_version": manifest["version"],
            "hash_version": manifest["hash_version"],
            "source_version": manifest["source"]["version"],
            "source_audit_version": manifest["source"]["audit_version"],
            "projection_version": manifest["projection"]["version"],
            "candidate_version": manifest["candidates"]["version"],
            "label_version": manifest["labels"]["version"],
            "independence_version": manifest["independence"]["version"],
            "acceptance_version": manifest["acceptance"]["version"],
            "parent_policy_version": manifest["independence"]["parent_policy_version"],
            "scope_id": inner["scope"]["scope_id"],
            "candidate_id": inner["candidate"]["candidate_id"],
            "window_minutes": inner["window_minutes"],
            "threshold_bps": inner["threshold_bps"],
            "implementation_artifacts": artifacts,
            "implementation_artifacts_sha256": contract.digest(artifacts),
            "expected_watch_code_manifest": watch,
            "expected_watch_code_manifest_sha256": contract.digest(watch),
            "verifier_profile": profile,
            "verifier_profile_sha256": contract.digest(profile),
            "frozen_at_utc": "2026-09-13T00:00:00.000000Z",
            "freeze_id": "a" * 64,
            "registry_record": record,
            "registry_record_sha256": contract.digest(record),
        }
        registry._validate_registry_row(
            row, self.binding, require_current_implementation=True,
        )
        tampered = deepcopy(row)
        tampered["registry_record"]["freeze_id"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "REGISTRY_ROW_INVALID"):
            registry._validate_registry_row(
                tampered, self.binding, require_current_implementation=False,
            )

    def test_caller_supplied_outcomes_can_never_upgrade_qualification(self):
        source = inspect.getsource(registry.evaluate_verified_from_connection)
        self.assertIn("AUTHORITATIVE_OUTCOME_ADAPTER_REQUIRED", source)
        self.assertNotIn("acceptance.evaluate", source)
        self.assertNotIn('"research_qualified": True', source)

    def test_migration_has_sealed_full_ledger_and_closed_acls(self):
        sql = (Path(registry.__file__).resolve().parent / "migrations" /
               "056_stage8_durable_registry.sql").read_text()
        for token in (
            "research_stage8_projection_fact_batches",
            "research_stage8_projected_fact_ledger",
            "research_stage8_projection_fact_batch_seals",
            "research_stage8_fact_seal_insert_guard_v1",
            "REFERENCES research_prospective_anchor_attempts(attempt_id)",
            "representative is not backed by the sealed fact ledger",
            "research_stage8_fact_writer_v1",
            "REVOKE ALL ON research_stage8_projected_fact_ledger FROM PUBLIC",
            "outcome_free_population_receipt_sha256",
            "current_setting('transaction_isolation')",
            "CREATE CONSTRAINT TRIGGER trg_stage8_fact_batch_complete_at_commit",
            "DEFERRABLE INITIALLY DEFERRED",
        ):
            self.assertIn(token, sql)
        self.assertGreaterEqual(
            sql.count("item->'fact'->>'validation_status' = 'VALID'"), 2,
        )
        self.assertEqual(sql.count("research_event_btc_movements"), 1)
        self.assertIn(
            "GRANT SELECT ON research_event_btc_movements\n"
            "            TO research_stage8_reader_v1",
            sql,
        )
        self.assertNotIn("CREATE ROLE research_stage8_", sql)

    def test_selection_guard_uses_python_canonical_collation(self):
        sql = (Path(registry.__file__).resolve().parent / "migrations" /
               "056_stage8_durable_registry.sql").read_text()
        self.assertIn(
            'research_stage8_canonical_json_v1(finalized.identity) COLLATE "C"',
            sql,
        )
        self.assertIn(
            'research_stage8_canonical_json_v1(finalized.representative) '
            'COLLATE "C"',
            sql,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
