"""Network-free safety and orchestration checks for the Stage-8 Shadow coordinator."""
from __future__ import annotations

from copy import deepcopy
import ast
import hashlib
import inspect
import unittest
from unittest.mock import patch

import research_operational_score_source_audit as source_audit
import research_stage8_contract as contract
import research_stage8_coverage_receipt as coverage_receipt
import research_stage8_projection_db_adapter as projection_adapter
from research_stage8_projection_db_adapter_selftest import FakeConnection
from research_stage8_representative_selector_selftest import (
    DECISION, HASH_F, _binding, _receipt, _registry,
)
from research_stage8_feature_projection_selftest import _fresh, _set_model, _snapshot
from research_operational_score_source_audit_selftest import _anchor
import research_stage8_shadow as shadow


DATABASE_URL = "postgresql://shadow-reader:secret@stage8.example/research"


def _environment():
    return {
        shadow.ENABLED_ENV: "TRUE",
        shadow.MODE_ENV: shadow.MODE,
        shadow.DATABASE_URL_ENV: DATABASE_URL,
        shadow.DATABASE_TARGET_SHA256_ENV: hashlib.sha256(
            DATABASE_URL.encode("utf-8")
        ).hexdigest(),
        shadow.ROLE_ENV: shadow.EXPECTED_ROLE,
        shadow.SCOPE_ENV: "BINANCE_BTC",
        shadow.CANDIDATE_ENV: "FUTURES_FLOW_ALIGNED65_SHORT",
        shadow.THRESHOLD_ENV: "50",
        shadow.START_ENV: "2026-08-29T11:34:00.000000Z",
        shadow.END_ENV: "2026-08-29T13:34:00.000000Z",
        shadow.MAX_ATTEMPTS_ENV: "1",
    }


_ADAPTER_FIXTURES = {}


def _non_evaluable_attempts(count):
    source, _, _ = _anchor(missing=True)
    rows = []
    for attempt_id in range(1, count + 1):
        row = deepcopy(source)
        row["attempt_id"] = attempt_id
        row["attempt_fingerprint"] = format(attempt_id, "064x")
        rows.append(row)
    return rows


def _adapter_result(*, unknown=False, count=1, missing_id=None):
    key = (unknown, count, missing_id)
    if key not in _ADAPTER_FIXTURES:
        binding = _binding()
        attempt_ids = list(range(1, count + 1))
        if missing_id is not None:
            attempt_ids.append(missing_id)
        if count == 1:
            snapshot = _set_model(
                _fresh(_snapshot()), score=0 if unknown else -70,
                available=not unknown,
                capture_status="UNAVAILABLE" if unknown else "AVAILABLE",
            )
            conn = FakeConnection(snapshots=[snapshot])
        else:
            conn = FakeConnection(
                attempts=_non_evaluable_attempts(count), slots=[], events=[],
                parent_sources={}, snapshots=[],
            )
        _ADAPTER_FIXTURES[key] = (
            projection_adapter.project_exact_binding_attempts_from_connection(
                conn, exact_binding=binding, attempt_ids=attempt_ids,
            )
        )
    return deepcopy(_ADAPTER_FIXTURES[key])


def _handoff(attempt_ids, binding, transaction_sha, *, page_size=1,
             attempt_status="EVALUABLE"):
    receipt = _receipt(attempt_ids, binding)
    query = dict(receipt["query_scope"])
    query.pop("query_sha256", None)
    query["page_size"] = page_size
    query["query_sha256"] = contract.digest(query)
    receipt["query_scope"] = query
    receipt["transaction_identity_sha256"] = transaction_sha
    receipt["counts"]["attempt_status"] = {
        attempt_status: len(attempt_ids),
    }
    population = {
        "version": "stage8-bounded-attempt-id-population-v1",
        "manifest_sha256": contract.MANIFEST_SHA256,
        "query_sha256": query["query_sha256"],
        "transaction_identity_sha256": transaction_sha,
        "high_water_attempt_id": max(attempt_ids, default=0),
        "attempt_ids": list(attempt_ids),
    }
    receipt["attempt_population_sha256"] = contract.digest(population)
    receipt["outcome_free_population_receipt_sha256"] = contract.digest(
        coverage_receipt.outcome_free_population_payload(receipt)
    )
    receipt.pop("receipt_sha256", None)
    receipt["receipt_sha256"] = contract.digest(receipt)
    result = {
        "version": coverage_receipt.ATTEMPT_COHORT_HANDOFF_VERSION,
        "status": "COMPLETE_BOUNDED_COHORT",
        "coverage_receipt": receipt,
        "attempt_ids": list(attempt_ids),
        "outcome_free_population_receipt_sha256":
            receipt["outcome_free_population_receipt_sha256"],
        "handoff_sha256": None,
    }
    result["handoff_sha256"] = contract.digest({
        "version": result["version"],
        "outcome_free_population_receipt_sha256":
            result["outcome_free_population_receipt_sha256"],
        "attempt_ids": result["attempt_ids"],
    })
    coverage_receipt.validate_attempt_cohort_handoff(result)
    return result


class _SessionManager:
    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        self.owner.calls.append("enter")
        return object()

    def __exit__(self, exc_type, exc, traceback):
        self.owner.calls.append("exit")
        return False


class FakeDependencies:
    def __init__(self, *, unknown=False, invalid_read=False,
                 cohort_incomplete=False, cohort_ids=None, missing_id=None):
        self.calls = []
        self.binding = _binding()
        self.cohort_ids = list(cohort_ids or [1])
        found_count = len(self.cohort_ids) - (1 if missing_id is not None else 0)
        self.adapter = _adapter_result(
            unknown=unknown, count=found_count, missing_id=missing_id,
        )
        source_manifest = self.adapter["projection_source_manifest"]
        known_adapter = _adapter_result()
        matching = known_adapter["rows"][0]["fact_ledger"]
        assert len(matching) == 1
        self.registry = _registry(
            self.binding,
            code_hash=matching[0]["expected_watch_code_manifest_sha256"],
        )
        self.registry["expected_projection_source_sha256"] = source_manifest[
            "research_stage8_feature_projection.py"
        ]
        self.registry["expected_selector_source_sha256"] = source_manifest[
            "research_stage8_representative_selector.py"
        ]
        self.invalid_read = invalid_read
        self.cohort_incomplete = cohort_incomplete
        self.forbidden_calls = 0

    def open_read_only_session(self, **kwargs):
        self.calls.append("open")
        self.open_arguments = kwargs
        return _SessionManager(self)

    def verify_read_only_session(self, session):
        self.calls.append("verify")
        return {
            "status": "VERIFIED_READ_ONLY_REPEATABLE_READ",
            "read_only": not self.invalid_read,
            "transaction_isolation": "REPEATABLE READ",
            "database_role": shadow.EXPECTED_ROLE,
            "database_target_sha256": hashlib.sha256(
                DATABASE_URL.encode("utf-8")
            ).hexdigest(),
            "backend_pid": self.adapter["transaction"]["backend_pid"],
            "transaction_started_at_utc":
                self.adapter["transaction"]["transaction_started_at_utc"],
            "transaction_identity_sha256":
                self.adapter["transaction"]["transaction_identity_sha256"],
            "database_snapshot_id":
                self.adapter["transaction"]["database_snapshot_id"],
        }

    def registry_reference_from_connection(self, session, exact_binding):
        self.calls.append("registry")
        self.asserted_binding = exact_binding
        return deepcopy(self.registry)

    def read_bounded_attempt_cohort_from_connection(self, session, **kwargs):
        self.calls.append("cohort")
        self.cohort_arguments = kwargs
        if self.cohort_incomplete:
            return {"status": "BOUNDED_PARTIAL"}
        return _handoff(
            self.cohort_ids, self.binding,
            self.adapter["transaction"]["transaction_identity_sha256"],
            page_size=kwargs["page_size"],
            attempt_status=("UNEVALUABLE" if len(self.cohort_ids) > 1
                            else "EVALUABLE"),
        )

    def project_exact_binding_attempts_from_connection(self, session, **kwargs):
        self.calls.append("adapter")
        self.adapter_arguments = kwargs
        return deepcopy(self.adapter)

    # Deliberately present to prove the coordinator has no path to them.
    def send_telegram(self, *args, **kwargs):
        self.forbidden_calls += 1
        raise AssertionError("must never send")

    def place_trade(self, *args, **kwargs):
        self.forbidden_calls += 1
        raise AssertionError("must never trade")

    def append_selection(self, *args, **kwargs):
        self.forbidden_calls += 1
        raise AssertionError("read-only coordinator must never persist")

    def evaluate_outcomes(self, *args, **kwargs):
        self.forbidden_calls += 1
        raise AssertionError("untrusted outcome API must never run")


class ShadowCoordinatorTests(unittest.TestCase):
    def test_default_is_disabled_without_importing_components_or_calling_dependencies(self):
        dependencies = FakeDependencies()
        with patch.object(shadow, "_load_components",
                          side_effect=AssertionError("must not import")), \
                patch.object(
                    shadow, "_load_default_dependencies",
                    side_effect=AssertionError("must not load database seam"),
                ):
            result = shadow.run_shadow_from_environment({}, dependencies=dependencies)
        self.assertEqual(result["status"], "DISABLED")
        self.assertEqual(result["reason"], "EXPLICIT_OPT_IN_ABSENT")
        self.assertEqual(dependencies.calls, [])
        self.assertFalse(result["research_qualified"])
        self.assertFalse(result["telegram_authorized"])
        self.assertFalse(result["live_authorized"])
        self.assertFalse(result["trade_authorized"])
        self.assertFalse(result["persistence_authorized"])

    def test_opt_in_token_is_exact_and_not_truthy_coerced(self):
        for value in ("true", " TRUE", "TRUE ", "1", "YES", "ON"):
            with self.subTest(value=value):
                dependencies = FakeDependencies()
                result = shadow.run_shadow_from_environment(
                    _environment() | {shadow.ENABLED_ENV: value},
                    dependencies=dependencies,
                )
                self.assertEqual(result["status"], "DISABLED")
                self.assertEqual(dependencies.calls, [])

    def test_generic_production_dsn_is_never_discovered(self):
        environ = _environment()
        environ.pop(shadow.DATABASE_URL_ENV)
        environ.pop(shadow.DATABASE_TARGET_SHA256_ENV)
        environ.update({
            "DATABASE_URL": "postgresql://production-secret",
            "RESEARCH_DATABASE_URL": "postgresql://also-production",
            "RESEARCH_STAGE8_READER_DATABASE_URL": "postgresql://registry-reader",
        })
        dependencies = FakeDependencies()
        result = shadow.run_shadow_from_environment(
            environ, dependencies=dependencies
        )
        self.assertEqual(result["status"], "CONFIGURATION_INCOMPLETE")
        self.assertIn(shadow.DATABASE_URL_ENV, result["missing_configuration"])
        self.assertEqual(dependencies.calls, [])
        self.assertNotIn("production", str(result).lower())

    def test_dangerous_flags_block_before_any_read_and_never_authorize(self):
        for key in shadow._DANGEROUS_ENABLES:
            with self.subTest(key=key):
                environ = _environment()
                environ[key] = "TRUE"
                dependencies = FakeDependencies()
                result = shadow.run_shadow_from_environment(
                    environ, dependencies=dependencies
                )
                self.assertEqual(result["status"], "BLOCKED_DANGEROUS_CONFIGURATION")
                self.assertEqual(dependencies.calls, [])
                self.assertFalse(result["telegram_authorized"])
                self.assertFalse(result["live_authorized"])
                self.assertFalse(result["trade_authorized"])
                self.assertFalse(result["outbox_authorized"])
                self.assertFalse(result["persistence_authorized"])

    def test_complete_known_read_stops_before_separate_persistence(self):
        dependencies = FakeDependencies()
        environ = _environment() | {
            "TELEGRAM_ENABLED": "true",
            "LIVE_TRADING_ENABLED": "true",
            "TRADING_ENABLED": "true",
        }
        result = shadow.run_shadow_from_environment(
            environ, dependencies=dependencies
        )
        self.assertEqual(result["status"], "AWAITING_SEPARATE_SELECTION_PERSISTENCE",
                         result)
        self.assertEqual(result["selector_summary"]["status"], "COMPLETE")
        self.assertEqual(result["selector_summary"]["representative_count"], 1)
        self.assertIsNotNone(result["selection_package"])
        self.assertIsNotNone(result["persistence_package"])
        shadow.validate_persistence_package(result["persistence_package"])
        persistence = result["persistence_package"]
        self.assertEqual(
            persistence["registry_reference"], dependencies.registry,
        )
        self.assertEqual(
            persistence["adapter_result"], dependencies.adapter,
        )
        self.assertEqual(
            persistence["selector_result"], result["selection_package"],
        )
        self.assertFalse(persistence["write_authority_granted"])
        self.assertFalse(persistence["research_qualified"])
        self.assertFalse(result["selection_persisted"])
        self.assertFalse(result["durable_outcome_evidence_verified"])
        self.assertFalse(result["research_qualified"])
        self.assertEqual(dependencies.forbidden_calls, 0)
        self.assertEqual(
            dependencies.calls,
            ["open", "enter", "verify", "registry", "cohort", "adapter",
             "verify", "exit"],
        )
        self.assertEqual(
            dependencies.open_arguments["database_target_sha256"],
            _environment()[shadow.DATABASE_TARGET_SHA256_ENV],
        )
        self.assertNotIn(DATABASE_URL, str(result))

    def test_partial_coverage_stops_before_projection_or_selection(self):
        dependencies = FakeDependencies(cohort_incomplete=True)
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=dependencies
        )
        self.assertEqual(
            result["status"], "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE", result,
        )
        self.assertEqual(result["reason"], "COHORT_HANDOFF_NOT_COMPLETE")
        self.assertIsNone(result["selector_summary"])
        self.assertIsNone(result["selection_package"])
        self.assertIsNone(result["persistence_package"])
        self.assertFalse(result["research_qualified"])
        self.assertEqual(
            dependencies.calls,
            ["open", "enter", "verify", "registry", "cohort", "verify", "exit"],
        )
        self.assertEqual(dependencies.forbidden_calls, 0)

    def test_exact_binding_full_cohort_handles_33_and_1000_without_chunking(self):
        for count in (33, 1000):
            with self.subTest(count=count):
                attempt_ids = list(range(1, count + 1))
                dependencies = FakeDependencies(cohort_ids=attempt_ids)
                environ = _environment() | {
                    shadow.MAX_ATTEMPTS_ENV: str(count),
                }
                result = shadow.run_shadow_from_environment(
                    environ, dependencies=dependencies
                )
                self.assertEqual(
                    result["status"],
                    "AWAITING_SEPARATE_SELECTION_PERSISTENCE", result,
                )
                self.assertEqual(
                    result["selector_summary"]["source_attempt_count"], count,
                )
                self.assertEqual(
                    len(result["selector_summary"]["proven_noneligible_attempt_ids"]),
                    count,
                )
                self.assertEqual(
                    dependencies.adapter_arguments["attempt_ids"], attempt_ids,
                )
                self.assertEqual(
                    dependencies.adapter_arguments["exact_binding"],
                    dependencies.binding,
                )
                self.assertEqual(dependencies.calls.count("adapter"), 1)
                expected_page = 1 if count == 33 else 25
                expected_pages = 33 if count == 33 else 40
                self.assertEqual(
                    dependencies.cohort_arguments["page_size"], expected_page,
                )
                self.assertEqual(
                    dependencies.cohort_arguments["max_pages"], expected_pages,
                )
                self.assertIsNotNone(result["persistence_package"])
                shadow.validate_persistence_package(result["persistence_package"])
                self.assertFalse(result["research_qualified"])

    def test_unknown_candidate_fact_remains_blocked_and_is_not_persisted(self):
        dependencies = FakeDependencies(unknown=True)
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=dependencies
        )
        self.assertEqual(result["status"], "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE",
                         result)
        self.assertEqual(result["selector_summary"]["status"], "BLOCKED")
        self.assertFalse(result["selector_summary"]["candidate_match_coverage_complete"])
        self.assertIsNone(result["selection_package"])
        self.assertIsNone(result["persistence_package"])
        self.assertFalse(result["research_qualified"])
        self.assertEqual(dependencies.forbidden_calls, 0)

    def test_read_only_attestation_failure_is_sanitized_and_fail_closed(self):
        dependencies = FakeDependencies(invalid_read=True)
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=dependencies
        )
        self.assertEqual(result["status"], "READ_ONLY_CHAIN_FAILED")
        self.assertEqual(result["reason"], "FAIL_CLOSED_READ_ERROR")
        self.assertFalse(result["research_qualified"])
        self.assertNotIn("secret", str(result))
        self.assertNotIn(DATABASE_URL, str(result))
        self.assertEqual(dependencies.forbidden_calls, 0)

    def test_adapter_cannot_smuggle_outcome_or_label_fields(self):
        for forbidden in ("outcome", "label"):
            with self.subTest(forbidden=forbidden):
                dependencies = FakeDependencies()
                ledger = next(
                    item for item in dependencies.adapter["rows"][0]["fact_ledger"]
                    if item["exact_binding_sha256"]
                    == dependencies.binding["binding_sha256"]
                )
                ledger[forbidden] = "MUST_NOT_BE_READ"
                unsigned = dict(dependencies.adapter)
                unsigned.pop("result_sha256")
                dependencies.adapter["result_sha256"] = contract.digest(unsigned)
                result = shadow.run_shadow_from_environment(
                    _environment(), dependencies=dependencies
                )
                self.assertEqual(result["status"], "READ_ONLY_CHAIN_FAILED")
                self.assertIsNone(result.get("selection_package"))
                self.assertFalse(result["research_qualified"])
                self.assertEqual(dependencies.forbidden_calls, 0)

    def test_shared_transaction_identity_is_recomputed_and_cross_layer_bound(self):
        dependencies = FakeDependencies()
        transaction = dependencies.adapter["transaction"]
        expected = source_audit.transaction_identity_from_fields(
            backend_pid=transaction["backend_pid"],
            transaction_started_at_utc=transaction["transaction_started_at_utc"],
            database_snapshot_id=transaction["database_snapshot_id"],
        )
        self.assertEqual(
            transaction["transaction_identity_sha256"],
            expected["transaction_identity_sha256"],
        )
        original_verify = dependencies.verify_read_only_session

        def mismatched(session):
            result = original_verify(session)
            changed = source_audit.transaction_identity_from_fields(
                backend_pid=result["backend_pid"] + 1,
                transaction_started_at_utc=result["transaction_started_at_utc"],
                database_snapshot_id=result["database_snapshot_id"],
            )
            result.update(changed)
            return result

        dependencies.verify_read_only_session = mismatched
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=dependencies,
        )
        self.assertEqual(result["status"], "READ_ONLY_CHAIN_FAILED")
        self.assertIsNone(result.get("persistence_package"))
        self.assertFalse(result["research_qualified"])

    def test_handoff_and_adapter_binding_mismatches_fail_closed(self):
        dependencies = FakeDependencies()
        original_cohort = dependencies.read_bounded_attempt_cohort_from_connection

        def changed_ids(session, **kwargs):
            result = original_cohort(session, **kwargs)
            result["attempt_ids"] = [2]
            result["handoff_sha256"] = contract.digest({
                "version": result["version"],
                "outcome_free_population_receipt_sha256":
                    result["outcome_free_population_receipt_sha256"],
                "attempt_ids": result["attempt_ids"],
            })
            return result

        dependencies.read_bounded_attempt_cohort_from_connection = changed_ids
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=dependencies,
        )
        self.assertEqual(result["status"], "READ_ONLY_CHAIN_FAILED")
        self.assertNotIn("adapter", dependencies.calls)

        dependencies = FakeDependencies()
        dependencies.adapter["exact_binding_sha256"] = HASH_F
        unsigned = dict(dependencies.adapter)
        unsigned.pop("result_sha256")
        dependencies.adapter["result_sha256"] = contract.digest(unsigned)
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=dependencies,
        )
        self.assertEqual(result["status"], "READ_ONLY_CHAIN_FAILED")
        self.assertIsNone(result.get("persistence_package"))

    def test_missing_adapter_row_stays_unknown_and_has_no_persistence_package(self):
        dependencies = FakeDependencies(
            cohort_ids=[1, 999], missing_id=999,
        )
        result = shadow.run_shadow_from_environment(
            _environment() | {shadow.MAX_ATTEMPTS_ENV: "2"},
            dependencies=dependencies,
        )
        self.assertEqual(
            result["status"], "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE", result,
        )
        self.assertEqual(
            result["selector_summary"]["missing_attempt_ids"], [999],
        )
        self.assertIsNone(result["selection_package"])
        self.assertIsNone(result["persistence_package"])
        self.assertFalse(result["research_qualified"])

    def test_persistence_package_is_tamper_evident_and_audit_metadata_is_not_identity(self):
        result = shadow.run_shadow_from_environment(
            _environment(), dependencies=FakeDependencies(),
        )
        original = result["persistence_package"]
        shadow.validate_persistence_package(original)

        injected = deepcopy(original)
        injected["outcome"] = "FORBIDDEN"
        unsigned = dict(injected)
        unsigned.pop("persistence_package_sha256")
        injected["persistence_package_sha256"] = contract.digest(unsigned)
        with self.assertRaises(ValueError):
            shadow.validate_persistence_package(injected)

        changed = deepcopy(original)
        for coverage in (
            changed["coverage_receipt"],
            changed["attempt_cohort_handoff"]["coverage_receipt"],
        ):
            coverage["counts"]["outcome_source_status"] = {"FAILURE": 16}
            coverage["reason_counts"]["outcome"] = {"AUDIT_ONLY": 16}
            coverage.pop("receipt_sha256")
            coverage["receipt_sha256"] = contract.digest(coverage)
        selector_result = changed["selector_result"]
        selector_result["source_audit_receipt_sha256"] = changed[
            "coverage_receipt"
        ]["receipt_sha256"]
        selector_result.pop("selector_receipt_sha256")
        selector_result["selector_receipt_sha256"] = contract.digest(
            selector_result
        )
        changed.pop("persistence_package_sha256")
        changed["persistence_package_sha256"] = contract.digest(changed)
        shadow.validate_persistence_package(changed)
        self.assertNotEqual(
            original["persistence_package_sha256"],
            changed["persistence_package_sha256"],
        )
        self.assertEqual(
            original["outcome_free_identity_sha256"],
            changed["outcome_free_identity_sha256"],
        )

    def test_missing_dependencies_never_fall_back_to_builtin_connectors(self):
        with patch.object(
                shadow, "_load_default_dependencies",
                side_effect=RuntimeError("psycopg absent")), patch.object(
                    shadow, "_load_components",
                    side_effect=AssertionError("must not import components"),
                ):
            result = shadow.run_shadow_from_environment(
                _environment(), dependencies=None
            )
        self.assertEqual(result["status"], "DEPENDENCIES_UNAVAILABLE")
        self.assertEqual(
            result["reason"],
            "READ_ONLY_SHADOW_POSTGRES_DEPENDENCIES_UNAVAILABLE",
        )
        self.assertFalse(result["research_qualified"])

    def test_complete_opt_in_can_load_the_concrete_dependency_factory(self):
        dependencies = FakeDependencies()
        with patch.object(
            shadow, "_load_default_dependencies", return_value=dependencies,
        ) as factory:
            result = shadow.run_shadow_from_environment(
                _environment(), dependencies=None,
            )
        factory.assert_called_once_with()
        self.assertEqual(
            result["status"], "AWAITING_SEPARATE_SELECTION_PERSISTENCE",
            result,
        )
        self.assertEqual(dependencies.calls.count("open"), 1)
        self.assertFalse(result["research_qualified"])

    def test_noncanonical_time_and_target_mismatch_fail_before_dependencies(self):
        for mutation in (
            {shadow.START_ENV: "2026-08-29T11:00:00Z"},
            {shadow.DATABASE_TARGET_SHA256_ENV: HASH_F},
            {shadow.MODE_ENV: "WRITE"},
            {shadow.MAX_ATTEMPTS_ENV: "1001"},
        ):
            with self.subTest(mutation=mutation):
                environ = _environment() | mutation
                dependencies = FakeDependencies()
                result = shadow.run_shadow_from_environment(
                    environ, dependencies=dependencies
                )
                self.assertEqual(result["status"], "CONFIGURATION_INVALID")
                self.assertEqual(dependencies.calls, [])

    def test_module_imports_and_dependency_surface_are_read_only_only(self):
        tree = ast.parse(inspect.getsource(shadow))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
        forbidden = ("telegram", "trading", "outbox", "psycopg", "requests", "socket")
        self.assertFalse(any(
            marker in name.lower() for name in imported for marker in forbidden
        ), imported)
        dependency_protocol = next(
            node for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "ReadOnlyShadowDependencies"
        )
        methods = {
            node.name for node in dependency_protocol.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertEqual(methods, {
            "open_read_only_session", "verify_read_only_session",
            "registry_reference_from_connection",
            "read_bounded_attempt_cohort_from_connection",
            "project_exact_binding_attempts_from_connection",
        })


if __name__ == "__main__":
    unittest.main(verbosity=2)
