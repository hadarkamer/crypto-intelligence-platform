"""Network-free tests for the concrete Stage-8 Shadow PostgreSQL seam."""
from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import inspect
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import research_operational_score_source_audit as source_audit
import research_stage8_shadow_postgres as postgres


DSN = (
    "host=localhost port=5432 dbname=stage8_shadow "
    "user=research_stage8_reader_v1 password=secret sslmode=disable"
)
DSN_SHA256 = hashlib.sha256(DSN.encode("utf-8")).hexdigest()


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class FakeConnection:
    def __init__(self, owner):
        self.owner = owner
        self.autocommit = True
        self.read_only = False
        self.isolation_level = None
        self.rollback_count = 0
        self.close_count = 0

    def execute(self, statement, params=()):
        self.owner.queries.append((statement, params))
        row = deepcopy(self.owner.attestation)
        return FakeCursor([row])

    def rollback(self):
        self.rollback_count += 1
        if self.owner.rollback_fails:
            raise RuntimeError("driver detail must be hidden")

    def close(self):
        self.close_count += 1


class FakePsycopg:
    IsolationLevel = SimpleNamespace(REPEATABLE_READ="RR")

    def __init__(self):
        self.connect_calls = []
        self.queries = []
        self.rollback_fails = False
        self.attestation = {
            "read_only": "on",
            "isolation": "repeatable read",
            "statement_timeout": "10s",
            "lock_timeout": "1s",
            "database_role": postgres.EXPECTED_ROLE,
            "backend_pid": 73,
            "transaction_started_at_utc": datetime(
                2026, 9, 13, 12, 0, tzinfo=timezone.utc,
            ),
            "database_snapshot_id": "500:500:",
        }
        self.connection = FakeConnection(self)

    def connect(self, **kwargs):
        self.connect_calls.append(kwargs)
        return self.connection


class Delegate:
    def __init__(self, method_name, result):
        self.method_name = method_name
        self.result = result
        self.calls = []

    def __getattr__(self, name):
        if name != self.method_name:
            raise AttributeError(name)

        def invoke(*args, **kwargs):
            self.calls.append((args, kwargs))
            return deepcopy(self.result)

        return invoke


def _parser(value):
    return dict(item.split("=", 1) for item in value.split())


def _runtime():
    driver = FakePsycopg()
    registry = Delegate("registry_reference_from_connection", {"registry": True})
    coverage = Delegate(
        "read_bounded_attempt_cohort_from_connection", {"cohort": True},
    )
    adapter = Delegate(
        "project_exact_binding_attempts_from_connection", {"projection": True},
    )
    runtime = postgres._Runtime(
        psycopg=driver, conninfo_to_dict=_parser, dict_row=object(),
        registry=registry, coverage=coverage, projection_adapter=adapter,
        source_audit=source_audit,
    )
    return runtime, driver, registry, coverage, adapter


class ShadowPostgresDependenciesTests(unittest.TestCase):
    def test_import_is_inert_and_factory_is_the_only_runtime_loader(self):
        tree = ast.parse(inspect.getsource(postgres))
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")
        self.assertFalse(any(
            name == "psycopg" or name.startswith("research_")
            for name in imports
        ), imports)
        runtime, driver, *_ = _runtime()
        with patch.object(postgres, "_load_runtime", return_value=runtime) as load:
            dependencies = postgres.build_dependencies()
        load.assert_called_once_with()
        self.assertIsInstance(
            dependencies, postgres.PostgresReadOnlyShadowDependencies,
        )
        self.assertEqual(driver.connect_calls, [])

    def test_exact_dsn_opens_one_bounded_read_only_rr_session_and_cleans_up(self):
        runtime, driver, *_ = _runtime()
        dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
        manager = dependencies.open_read_only_session(
            database_url=DSN, expected_role=postgres.EXPECTED_ROLE,
            database_target_sha256=DSN_SHA256,
        )
        self.assertEqual(driver.connect_calls, [])
        with manager as connection:
            self.assertIs(connection, driver.connection)
            self.assertFalse(connection.autocommit)
            self.assertTrue(connection.read_only)
            self.assertEqual(connection.isolation_level, "RR")
            attestation = dependencies.verify_read_only_session(connection)
            expected = source_audit.transaction_identity_from_fields(
                backend_pid=73,
                transaction_started_at_utc=
                    driver.attestation["transaction_started_at_utc"],
                database_snapshot_id="500:500:",
            )
            self.assertEqual(
                attestation["transaction_identity_sha256"],
                expected["transaction_identity_sha256"],
            )
            self.assertEqual(attestation["database_target_sha256"], DSN_SHA256)
        self.assertEqual(driver.connection.rollback_count, 1)
        self.assertEqual(driver.connection.close_count, 1)
        call = driver.connect_calls[0]
        self.assertEqual(call["host"], "localhost")
        self.assertEqual(call["user"], postgres.EXPECTED_ROLE)
        self.assertEqual(call["connect_timeout"], 5)
        self.assertEqual(call["prepare_threshold"], None)
        self.assertIn("default_transaction_read_only=on", call["options"])
        self.assertIn("statement_timeout=10000", call["options"])
        self.assertIn("lock_timeout=1000", call["options"])

    def test_only_the_three_trusted_readers_are_delegated_on_same_connection(self):
        runtime, driver, registry, coverage, adapter = _runtime()
        dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
        binding = {"binding_sha256": "a" * 64}
        with dependencies.open_read_only_session(
            database_url=DSN, expected_role=postgres.EXPECTED_ROLE,
            database_target_sha256=DSN_SHA256,
        ) as connection:
            dependencies.verify_read_only_session(connection)
            self.assertEqual(
                dependencies.registry_reference_from_connection(
                    connection, binding,
                ),
                {"registry": True},
            )
            self.assertEqual(
                dependencies.read_bounded_attempt_cohort_from_connection(
                    connection, start_utc="start", end_utc="end",
                    symbols=["BTC"], page_size=25, max_pages=40,
                ),
                {"cohort": True},
            )
            self.assertEqual(
                dependencies.project_exact_binding_attempts_from_connection(
                    connection, exact_binding=binding, attempt_ids=[1, 2],
                ),
                {"projection": True},
            )
        for delegate in (registry, coverage, adapter):
            self.assertEqual(len(delegate.calls), 1)
            self.assertIs(delegate.calls[0][0][0], driver.connection)
        self.assertEqual(adapter.calls[0][1]["attempt_ids"], [1, 2])

    def test_delegation_requires_prior_attestation_and_active_session(self):
        runtime, driver, *_ = _runtime()
        dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
        with dependencies.open_read_only_session(
            database_url=DSN, expected_role=postgres.EXPECTED_ROLE,
            database_target_sha256=DSN_SHA256,
        ) as connection:
            with self.assertRaises(postgres.ShadowPostgresError):
                dependencies.registry_reference_from_connection(connection, {})
            dependencies.verify_read_only_session(connection)
        with self.assertRaises(postgres.ShadowPostgresError):
            dependencies.verify_read_only_session(driver.connection)

    def test_target_hash_role_and_connection_fallbacks_fail_before_connect(self):
        invalid_dsns = (
            DSN.replace(" password=secret", ""),
            DSN.replace("host=localhost", "host=one,two"),
            DSN.replace("sslmode=disable", "sslmode=prefer"),
            DSN + " passfile=/tmp/pgpass",
            DSN + " service=production",
            DSN + " options=-csearch_path=public",
            DSN.replace(postgres.EXPECTED_ROLE, "postgres"),
        )
        for value in invalid_dsns:
            with self.subTest(value=value):
                runtime, driver, *_ = _runtime()
                dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
                with self.assertRaises(postgres.ShadowPostgresError):
                    dependencies.open_read_only_session(
                        database_url=value, expected_role=postgres.EXPECTED_ROLE,
                        database_target_sha256=hashlib.sha256(
                            value.encode("utf-8")
                        ).hexdigest(),
                    )
                self.assertEqual(driver.connect_calls, [])
        runtime, driver, *_ = _runtime()
        dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
        for role, target_hash in (
            ("postgres", DSN_SHA256),
            (postgres.EXPECTED_ROLE, "f" * 64),
        ):
            with self.assertRaises(postgres.ShadowPostgresError):
                dependencies.open_read_only_session(
                    database_url=DSN, expected_role=role,
                    database_target_sha256=target_hash,
                )
        self.assertEqual(driver.connect_calls, [])

    def test_writable_wrong_role_or_unbounded_timeout_attestation_fails_closed(self):
        mutations = (
            {"read_only": "off"},
            {"isolation": "read committed"},
            {"database_role": "postgres"},
            {"statement_timeout": "11s"},
            {"statement_timeout": "0"},
            {"lock_timeout": "1001ms"},
            {"lock_timeout": "0"},
            {"database_snapshot_id": ""},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                runtime, driver, *_ = _runtime()
                driver.attestation.update(mutation)
                dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
                with dependencies.open_read_only_session(
                    database_url=DSN, expected_role=postgres.EXPECTED_ROLE,
                    database_target_sha256=DSN_SHA256,
                ) as connection:
                    with self.assertRaises(postgres.ShadowPostgresError):
                        dependencies.verify_read_only_session(connection)
                self.assertEqual(driver.connection.rollback_count, 1)
                self.assertEqual(driver.connection.close_count, 1)

    def test_body_error_is_preserved_while_cleanup_still_rolls_back_and_closes(self):
        runtime, driver, *_ = _runtime()
        driver.rollback_fails = True
        dependencies = postgres.PostgresReadOnlyShadowDependencies(runtime)
        with self.assertRaisesRegex(ValueError, "body failure"):
            with dependencies.open_read_only_session(
                database_url=DSN, expected_role=postgres.EXPECTED_ROLE,
                database_target_sha256=DSN_SHA256,
            ):
                raise ValueError("body failure")
        self.assertEqual(driver.connection.rollback_count, 1)
        self.assertEqual(driver.connection.close_count, 1)

    def test_public_surface_has_no_writer_outcome_or_delivery_method(self):
        methods = {
            name for name, value in vars(
                postgres.PostgresReadOnlyShadowDependencies
            ).items() if callable(value) and not name.startswith("_")
        }
        self.assertEqual(methods, {
            "open_read_only_session", "verify_read_only_session",
            "registry_reference_from_connection",
            "read_bounded_attempt_cohort_from_connection",
            "project_exact_binding_attempts_from_connection",
        })
        source = inspect.getsource(postgres).lower()
        for forbidden_import in (
            "import telegram", "import requests", "import socket",
            "import research_stage8_outcome", "import trading",
        ):
            self.assertNotIn(forbidden_import, source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
