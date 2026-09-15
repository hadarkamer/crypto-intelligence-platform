from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

import research_operational_score_source_audit as source_audit
import research_stage8_coverage_receipt as receipt


NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)


def _scope() -> dict:
    manifest = receipt.contract.frozen_manifest()
    return {
        "sampler_version": manifest["source"]["sampler_version"],
        "symbols": ["BTC"], "start_utc": NOW, "end_utc": NOW.replace(hour=13),
        "version": source_audit.VERSION,
        "max_capture_age_seconds": manifest["source"]["max_capture_age_seconds"],
        "windows": [manifest["labels"]["window_minutes"]],
        "thresholds_bps": manifest["labels"]["thresholds_bps"],
        "page_size": 1, "capture_version": manifest["source"]["watch_version"],
        "outcome_version": manifest["labels"]["method_version"],
        "parent_policy": manifest["independence"]["parent_policy_version"],
    }


def _row(attempt_id: int, *, valid: bool = True) -> dict:
    state = "VALID" if valid else "UNKNOWN"
    reasons = [] if valid else ["FIXTURE_UNKNOWN"]
    return {
        "attempt": {"attempt_id": attempt_id, "evaluation_status": "EVALUABLE"},
        "anchor_authority": {"status": state, "reasons": reasons},
        "capture": {"status": state, "reasons": reasons},
        "outcome_cells": [
            {"direction": direction, "window_minutes": 60, "threshold_bps": threshold,
             "status": state, "reasons": reasons,
             "source_status": ("SUCCESS" if direction == "LONG" else "FAILURE")
             if valid else "DATA_MISSING",
             "reported_status": ("SUCCESS" if direction == "LONG" else "FAILURE")
             if valid else "DATA_MISSING"}
            for direction in ("LONG", "SHORT")
            for threshold in range(25, 201, 25)
        ],
        "parent_memberships": {
            direction: {"status": state, "reasons": reasons,
                        "membership": {"btc_parent_movement_id": f"parent-{attempt_id}"}}
            for direction in ("LONG", "SHORT")
        },
    }


def _page(rows, *, cursor=None, has_more=False, high_water=None, page_size=1) -> dict:
    scope = _scope()
    scope["page_size"] = page_size
    if high_water is None:
        high_water = max((row["attempt"]["attempt_id"] for row in rows), default=0)
    return {
        "version": source_audit.VERSION, "scope": scope, "rows": rows,
        "high_water_attempt_id": high_water,
        "next_cursor": cursor if has_more else None,
        "examined": len(rows), "emitted": len(rows),
        "has_more": has_more,
        "read_started_at_utc": NOW, "population_page_complete": not has_more,
        "snapshot_consistency": "CALLER_TRANSACTION_SNAPSHOT",
        "transaction_identity_sha256": "a" * 64,
    }


class CoverageReceiptTests(unittest.TestCase):
    def _handoff(self, pages):
        reader = mock.Mock(side_effect=pages)
        result = receipt.read_bounded_attempt_cohort_from_connection(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            symbols=["BTC"], page_size=1, max_pages=len(pages),
            page_reader=reader, monotonic=lambda: 0.0)
        return result, reader

    @staticmethod
    def _rehash_handoff_audit_metadata(value):
        coverage = value["coverage_receipt"]
        outcome_free_hash = receipt.contract.digest(
            receipt.outcome_free_population_payload(coverage))
        coverage["outcome_free_population_receipt_sha256"] = outcome_free_hash
        value["outcome_free_population_receipt_sha256"] = outcome_free_hash
        coverage.pop("receipt_sha256", None)
        coverage["receipt_sha256"] = receipt.contract.digest(coverage)
        value["handoff_sha256"] = receipt.contract.digest(
            receipt._attempt_cohort_handoff_payload(value))

    def test_handoff_uses_exact_same_page_ids_without_second_read(self):
        pages = [
            _page([_row(2)], cursor={"next": 2}, has_more=True, high_water=8),
            _page([_row(8, valid=False)], high_water=8),
        ]
        result, reader = self._handoff(pages)
        self.assertEqual(reader.call_count, 2)
        self.assertIsNone(reader.call_args_list[0].kwargs["cursor"])
        self.assertEqual(reader.call_args_list[1].kwargs["cursor"], {"next": 2})
        self.assertEqual(result["attempt_ids"], [2, 8])
        self.assertEqual(result["status"], "COMPLETE_BOUNDED_COHORT")
        self.assertNotIn("attempt_ids", result["coverage_receipt"])
        receipt.validate_attempt_cohort_handoff(result,
            expected_handoff_sha256=result["handoff_sha256"])
        self.assertEqual(reader.call_count, 2)
        # The count-only API remains byte-for-byte identical for the same data.
        count_only = receipt.run_bounded_audit(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            symbols=["BTC"], page_size=1, max_pages=2,
            page_reader=mock.Mock(side_effect=pages), monotonic=lambda: 0.0)
        self.assertEqual(count_only, result["coverage_receipt"])

    def test_handoff_attempt_digest_binds_query_transaction_highwater_and_ids(self):
        result, _ = self._handoff([_page([_row(8)])])
        coverage = result["coverage_receipt"]
        population = {
            "version": "stage8-bounded-attempt-id-population-v1",
            "manifest_sha256": coverage["manifest_sha256"],
            "query_sha256": coverage["query_scope"]["query_sha256"],
            "transaction_identity_sha256": coverage["transaction_identity_sha256"],
            "high_water_attempt_id": coverage["high_water_attempt_id"],
            "attempt_ids": result["attempt_ids"],
        }
        self.assertEqual(coverage["attempt_population_sha256"], receipt.contract.digest(population))

    def test_handoff_changed_ids_rejected_even_with_rehashed_handoff(self):
        result, _ = self._handoff([
            _page([_row(2)], cursor={"next": 2}, has_more=True, high_water=8),
            _page([_row(8)], high_water=8),
        ])
        result["attempt_ids"] = [3, 8]
        result["handoff_sha256"] = receipt.contract.digest(
            receipt._attempt_cohort_handoff_payload(result))
        with self.assertRaisesRegex(ValueError, "attempt population hash"):
            receipt.validate_attempt_cohort_handoff(result)

    def test_handoff_query_transaction_and_count_tampering_rejected(self):
        original, _ = self._handoff([_page([_row(8)])])
        for mutation in ("transaction", "query", "high_water", "count", "status"):
            result = deepcopy(original)
            coverage = result["coverage_receipt"]
            if mutation == "transaction":
                coverage["transaction_identity_sha256"] = "b" * 64
            elif mutation == "query":
                query = coverage["query_scope"]
                query["symbols"] = ["ETH"]
                query.pop("query_sha256")
                query["query_sha256"] = receipt.contract.digest(query)
            elif mutation == "high_water":
                coverage["high_water_attempt_id"] = 9
            elif mutation == "count":
                coverage["attempts_examined"] = 2
            else:
                coverage["status"] = "UNKNOWN_NOT_QUERIED"
            self._rehash_handoff_audit_metadata(result)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                receipt.validate_attempt_cohort_handoff(result)

    def test_handoff_outcome_metadata_changes_do_not_change_identity(self):
        original, _ = self._handoff([_page([_row(8)])])
        changed = deepcopy(original)
        changed["coverage_receipt"]["counts"]["outcome_source_status"] = {"FAILURE": 16}
        changed["coverage_receipt"]["reason_counts"]["outcome"] = {"OUTCOME_INVALID": 16}
        self._rehash_handoff_audit_metadata(changed)
        self.assertNotEqual(original["coverage_receipt"]["receipt_sha256"],
                            changed["coverage_receipt"]["receipt_sha256"])
        self.assertEqual(original["handoff_sha256"], changed["handoff_sha256"])
        receipt.validate_attempt_cohort_handoff(changed,
            expected_handoff_sha256=original["handoff_sha256"])
        identity = receipt._attempt_cohort_handoff_payload(changed)
        self.assertEqual(set(identity), {"version", "attempt_ids",
                                        "outcome_free_population_receipt_sha256"})

    def test_handoff_expected_hash_pins_trusted_original(self):
        result, _ = self._handoff([_page([_row(8)])])
        with self.assertRaisesRegex(ValueError, "handoff hash"):
            receipt.validate_attempt_cohort_handoff(result, expected_handoff_sha256="b" * 64)
        result["coverage_receipt"]["counts"]["outcome_source_status"] = {}
        with self.assertRaisesRegex(ValueError, "full audit receipt hash"):
            receipt.validate_attempt_cohort_handoff(result)

    def test_partial_handoff_returns_no_ids_or_usable_identity(self):
        result, reader = self._handoff([
            _page([_row(2)], cursor={"next": 2}, has_more=True, high_water=8),
        ])
        self.assertEqual(reader.call_count, 1)
        self.assertEqual(result["status"], "BOUNDED_PARTIAL")
        self.assertIsNone(result["attempt_ids"])
        self.assertIsNone(result["handoff_sha256"])
        with self.assertRaisesRegex(ValueError, "incomplete"):
            receipt.validate_attempt_cohort_handoff(result)

        ticks = iter((0.0, 31.0))
        reader = mock.Mock(side_effect=AssertionError("no DB read after deadline"))
        result = receipt.read_bounded_attempt_cohort_from_connection(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            page_reader=reader, monotonic=lambda: next(ticks))
        reader.assert_not_called()
        self.assertEqual(result["coverage_receipt"]["pages_read"], 0)
        self.assertIsNone(result["attempt_ids"])
        with self.assertRaises(ValueError):
            receipt.validate_attempt_cohort_handoff(result)

    def test_non_atomic_and_unknown_handoffs_cannot_be_validated(self):
        page = _page([_row(8)])
        page["snapshot_consistency"] = "STATEMENT_SNAPSHOTS_NOT_ATOMIC"
        result, _ = self._handoff([page])
        self.assertIsNone(result["attempt_ids"])
        with self.assertRaises(ValueError):
            receipt.validate_attempt_cohort_handoff(result)
        unknown = receipt._unknown("EXPLICIT_AUDIT_CONFIGURATION_MISSING")
        result["coverage_receipt"] = unknown
        result["status"] = unknown["status"]
        with self.assertRaises(ValueError):
            receipt.validate_attempt_cohort_handoff(result)

    def test_handoff_omits_raw_outcomes_scores_and_source_payloads(self):
        row = _row(8)
        row["candidate_score"] = "private-score-marker"
        row["outcome_cells"][0]["reference_price"] = "private-price-marker"
        row["capture"]["payload"] = "private-capture-marker"
        result, _ = self._handoff([_page([row])])
        rendered = json.dumps(result)
        self.assertNotIn("private-", rendered)
        self.assertNotIn("outcome_cells", rendered)
        self.assertNotIn('"candidate_score":', rendered)
        self.assertEqual(result["attempt_ids"], [8])

    def test_missing_configuration_is_unknown_not_zero_and_never_connects(self):
        result = receipt.run_from_environment({})
        self.assertEqual(result["status"], "UNKNOWN_NOT_QUERIED")
        self.assertIsNone(result["counts"])
        self.assertFalse(result["database_connection_attempted"])
        self.assertEqual(result["missing_configuration"], sorted([
            receipt.DATABASE_URL_ENV, receipt.START_UTC_ENV, receipt.END_UTC_ENV]))

    def test_bounded_pages_reduce_only_structural_statuses(self):
        calls = []

        def reader(conn, **kwargs):
            calls.append(kwargs)
            if kwargs["cursor"] is None:
                return _page([_row(1)], cursor={"next": 1}, has_more=True,
                             high_water=2)
            return _page([_row(2, valid=False)], high_water=2)

        result = receipt.run_bounded_audit(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            symbols=["BTC"], page_size=1, max_pages=2,
            page_reader=reader, monotonic=lambda: 0.0)
        self.assertEqual(result["status"], "COMPLETE_BOUNDED_COHORT")
        self.assertEqual(result["attempts_examined"], 2)
        self.assertEqual(result["counts"]["attempt_status"], {"EVALUABLE": 2})
        self.assertEqual(result["counts"]["distinct_valid_btc_parent_movement_ids"], 1)
        self.assertEqual(
            result["counts"]["valid_anchor_capture_parent_outcome_cell_intersections"], 16)
        self.assertEqual(result["reason_counts"]["capture"],
                         {"CAPTURE_UNRECOGNIZED_REASON": 1})
        self.assertEqual(result["query_scope"]["symbols"], ["BTC"])
        self.assertEqual(result["execution_bounds"]["max_pages"], 2)
        unsigned = dict(result)
        supplied_receipt_sha256 = unsigned.pop("receipt_sha256")
        self.assertEqual(supplied_receipt_sha256, receipt.contract.digest(unsigned))
        self.assertRegex(result["attempt_population_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(
            result["outcome_free_population_receipt_sha256"],
            receipt.contract.digest(receipt.outcome_free_population_payload(result)),
        )
        self.assertFalse(result["formula_qualification_evaluated"])
        self.assertFalse(result["candidate_score_values_returned"])
        self.assertEqual(calls[0]["windows"], (60,))
        self.assertEqual(tuple(calls[0]["thresholds_bps"]), tuple(
            receipt.contract.frozen_manifest()["labels"]["thresholds_bps"]))

    def test_outcome_changes_do_not_change_selection_population_identity(self):
        original = receipt._aggregate_pages([_page([_row(1)])], truncated=False)
        changed = deepcopy(original)
        changed["counts"]["outcome_source_status"] = {"FAILURE": 16}
        changed["counts"]["outcome_reported_status"] = {"FAILURE": 16}
        changed["reason_counts"]["outcome"] = {"OUTCOME_INVALID": 16}
        changed.pop("receipt_sha256")
        changed["receipt_sha256"] = receipt.contract.digest(changed)
        self.assertNotEqual(original["receipt_sha256"], changed["receipt_sha256"])
        self.assertEqual(
            receipt.contract.digest(receipt.outcome_free_population_payload(original)),
            receipt.contract.digest(receipt.outcome_free_population_payload(changed)),
        )
        self.assertEqual(
            original["outcome_free_population_receipt_sha256"],
            changed["outcome_free_population_receipt_sha256"],
        )

    def test_max_pages_and_wall_clock_are_explicit_partial_results(self):
        endless = lambda conn, **kwargs: _page(
            [_row(1)], cursor={"again": True}, has_more=True)
        page_limited = receipt.run_bounded_audit(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            symbols=["BTC"], page_size=1, max_pages=1,
            page_reader=endless, monotonic=lambda: 0.0)
        self.assertEqual(page_limited["status"], "BOUNDED_PARTIAL")
        self.assertEqual(page_limited["stop_reason"], "MAX_PAGES")

        ticks = iter((0.0, 31.0))
        wall_limited = receipt.run_bounded_audit(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            max_wall_seconds=30, page_reader=lambda *args, **kwargs: self.fail(
                "reader must not run after the wall bound"), monotonic=lambda: next(ticks))
        self.assertEqual(wall_limited["status"], "BOUNDED_PARTIAL")
        self.assertEqual(wall_limited["stop_reason"], "MAX_WALL_SECONDS")
        self.assertEqual(wall_limited["pages_read"], 0)

        # Finishing one page after the bound is still partial, even if that
        # page says the keyset is exhausted.
        ticks = iter((0.0, 0.0, 31.0))
        late_page = receipt.run_bounded_audit(
            object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
            symbols=["BTC"], page_size=1, max_wall_seconds=30,
            page_reader=lambda *args, **kwargs: _page([_row(1)]),
            monotonic=lambda: next(ticks))
        self.assertEqual(late_page["status"], "BOUNDED_PARTIAL")
        self.assertEqual(late_page["stop_reason"], "MAX_WALL_SECONDS")

    def test_hard_bounds_cannot_be_enlarged_by_a_caller(self):
        base = dict(conn=object(), start_utc=NOW, end_utc=NOW.replace(hour=13),
                    page_reader=lambda *args, **kwargs: _page([]), monotonic=lambda: 0.0)
        for changed in ({"page_size": 26}, {"max_pages": 41},
                        {"max_wall_seconds": 30.0001}, {"max_pages": True},
                        {"max_wall_seconds": float("inf")}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                receipt.run_bounded_audit(**base, **changed)

    def test_changed_scope_or_high_water_is_rejected(self):
        changed = _page([_row(2)])
        changed["scope"] = {**_scope(), "symbols": ["ETH"]}
        first = _page([_row(1)], cursor={"next": 1}, has_more=True, high_water=2)
        with self.assertRaisesRegex(ValueError, "bounded cohort"):
            receipt._aggregate_pages([first, changed], truncated=False)

    def test_pages_from_different_database_transactions_are_rejected(self):
        first = _page([_row(1)], cursor={"next": 1}, has_more=True, high_water=2)
        second = _page([_row(2)], high_water=2)
        second["transaction_identity_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "one database transaction"):
            receipt._aggregate_pages([first, second], truncated=False)

    def test_page_scope_must_equal_every_frozen_manifest_axis(self):
        for key, value in (("windows", [240]),
                           ("max_capture_age_seconds", 999),
                           ("capture_version", "wrong"),
                           ("outcome_version", "wrong"),
                           ("parent_policy", "wrong")):
            page = _page([_row(1)])
            page["scope"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "manifest axes"):
                receipt._aggregate_pages([page], truncated=False)

    def test_runtime_source_audit_version_cannot_drift_from_frozen_manifest(self):
        page = _page([_row(1)])
        with mock.patch.object(source_audit, "VERSION", "mutated-after-import"):
            with self.assertRaisesRegex(ValueError, "frozen manifest"):
                receipt._aggregate_pages([page], truncated=False)

    def test_runtime_contract_drift_fails_before_driver_import_or_connect(self):
        values = {
            receipt.DATABASE_URL_ENV: (
                "host=db.example port=5432 dbname=audit user=reader "
                "password=secret sslmode=require"
            ),
            receipt.START_UTC_ENV: NOW.isoformat(),
            receipt.END_UTC_ENV: NOW.replace(hour=13).isoformat(),
        }
        original_import = __import__

        def blocked(name, *args, **kwargs):
            if name == "psycopg" or name.startswith("psycopg."):
                raise AssertionError("driver must not be imported after contract drift")
            return original_import(name, *args, **kwargs)

        with mock.patch.object(source_audit, "VERSION", "mutated-after-import"), \
                mock.patch("builtins.__import__", side_effect=blocked):
            result = receipt.run_from_environment(values)
        self.assertEqual(result["status"], "QUERY_FAILED")
        self.assertEqual(result["reason"], "RUNTIME_CONTRACT_MISMATCH")
        self.assertFalse(result["database_connection_attempted"])

    def test_malformed_page_protocol_and_keyset_fail_closed(self):
        page = _page([_row(1)])
        page["examined"] = 2
        with self.assertRaisesRegex(ValueError, "protocol"):
            receipt._aggregate_pages([page], truncated=False)
        duplicate = _page([_row(1), _row(1)], page_size=2)
        with self.assertRaisesRegex(ValueError, "increasing|duplicate"):
            receipt._aggregate_pages([duplicate], truncated=False)

    def test_complete_pages_cannot_omit_attempts_or_frozen_cells(self):
        omitted_attempt = _page([_row(1)], high_water=2)
        with self.assertRaisesRegex(ValueError, "high-water"):
            receipt._aggregate_pages([omitted_attempt], truncated=False)

        oversized = _page([_row(1), _row(2)], high_water=2, page_size=1)
        with self.assertRaisesRegex(ValueError, "protocol"):
            receipt._aggregate_pages([oversized], truncated=False)

        for mutation in ("missing_cell", "duplicate_cell", "missing_parent"):
            row = _row(1)
            if mutation == "missing_cell":
                row["outcome_cells"].pop()
            elif mutation == "duplicate_cell":
                row["outcome_cells"][-1] = dict(row["outcome_cells"][0])
            else:
                row["parent_memberships"].pop("SHORT")
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                    ValueError, "cell matrix|parent envelope"):
                receipt._aggregate_pages([_page([row])], truncated=False)

    def test_dynamic_reason_details_are_normalized_and_secret_free(self):
        row = _row(1, valid=False)
        row["capture"]["reasons"] = [
            "CAPTURE_INVALID:postgresql://secret-user:secret-pass@db/private"
        ]
        result = receipt._aggregate_pages([_page([row])], truncated=False)
        rendered = json.dumps(result)
        self.assertEqual(result["reason_counts"]["capture"], {"CAPTURE_INVALID": 1})
        self.assertNotIn("secret", rendered)
        self.assertNotIn("postgresql", rendered)

    def test_non_atomic_source_page_cannot_be_reported_complete(self):
        page = _page([_row(1)])
        page["snapshot_consistency"] = "STATEMENT_SNAPSHOTS_NOT_ATOMIC"
        result = receipt._aggregate_pages([page], truncated=False)
        self.assertEqual(result["status"], "BOUNDED_PARTIAL")
        self.assertEqual(result["stop_reason"], "NON_ATOMIC_DATABASE_SNAPSHOT")

    def test_environment_connection_is_read_only_repeatable_read_and_dsn_is_absent(self):
        class FakeConnection:
            read_only = None
            isolation_level = None

            def __init__(self):
                self.rollback = mock.Mock()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

        fake_conn = FakeConnection()
        connect = mock.Mock(return_value=fake_conn)
        psycopg = ModuleType("psycopg")
        psycopg.connect = connect
        psycopg.IsolationLevel = SimpleNamespace(REPEATABLE_READ="repeatable-read")
        conninfo = ModuleType("psycopg.conninfo")
        conninfo.conninfo_to_dict = mock.Mock(return_value={
            "host": "db.example", "port": "5432", "dbname": "private",
            "user": "secret-user", "password": "secret-pass", "sslmode": "require",
        })
        rows = ModuleType("psycopg.rows")
        rows.dict_row = object()
        values = {
            receipt.DATABASE_URL_ENV: "postgresql://secret-user:secret-pass@db.example/private",
            receipt.START_UTC_ENV: NOW.isoformat(),
            receipt.END_UTC_ENV: NOW.replace(hour=13).isoformat(),
        }
        completed = {"status": "COMPLETE_BOUNDED_COHORT", "counts": {}}
        with mock.patch.dict(sys.modules, {"psycopg": psycopg,
                                           "psycopg.conninfo": conninfo,
                                           "psycopg.rows": rows}), \
                mock.patch.object(receipt, "run_bounded_audit", return_value=completed):
            result = receipt.run_from_environment(values)
        self.assertEqual(result, completed)
        self.assertTrue(fake_conn.read_only)
        self.assertEqual(fake_conn.isolation_level, "repeatable-read")
        fake_conn.rollback.assert_called_once_with()
        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["host"], "db.example")
        self.assertEqual(kwargs["port"], "5432")
        self.assertEqual(kwargs["user"], "secret-user")
        self.assertEqual(kwargs["password"], "secret-pass")
        self.assertFalse(kwargs["autocommit"])
        self.assertIn("default_transaction_read_only=on", kwargs["options"])
        self.assertNotIn("secret", json.dumps(result))

    def test_connection_failure_reports_only_exception_type(self):
        psycopg = ModuleType("psycopg")
        psycopg.connect = mock.Mock(side_effect=RuntimeError(
            "failed postgresql://secret-user:secret-pass@db.example/private"))
        psycopg.IsolationLevel = SimpleNamespace(REPEATABLE_READ="repeatable-read")
        conninfo = ModuleType("psycopg.conninfo")
        conninfo.conninfo_to_dict = mock.Mock(return_value={
            "host": "db.example", "port": "5432", "dbname": "private",
            "user": "secret-user", "password": "secret-pass", "sslmode": "require",
        })
        rows = ModuleType("psycopg.rows")
        rows.dict_row = object()
        values = {
            receipt.DATABASE_URL_ENV: "postgresql://secret-user:secret-pass@db.example/private",
            receipt.START_UTC_ENV: NOW.isoformat(),
            receipt.END_UTC_ENV: NOW.replace(hour=13).isoformat(),
        }
        with mock.patch.dict(sys.modules, {"psycopg": psycopg,
                                           "psycopg.conninfo": conninfo,
                                           "psycopg.rows": rows}):
            result = receipt.run_from_environment(values)
        rendered = json.dumps(result)
        self.assertEqual(result["status"], "QUERY_FAILED")
        self.assertEqual(result["error_type"], "RuntimeError")
        self.assertNotIn("secret", rendered)
        self.assertNotIn("db.example", rendered)

    def test_explicit_connection_parser_rejects_fallback_and_ambiguous_routes(self):
        complete = {
            "host": "db.example", "port": "5432", "dbname": "audit",
            "user": "audit_reader", "password": "password", "sslmode": "require",
        }
        self.assertEqual(
            receipt._explicit_connection_fields("unused", lambda _: complete)["host"],
            "db.example",
        )
        for changed in (
                {**complete, "service": "ambient"},
                {**complete, "passfile": "/tmp/ambient"},
                {**complete, "host": "one,two"},
                {**complete, "port": "5432,5433"},
                {**complete, "sslmode": "prefer"},
                {**complete, "sslmode": "verify-full"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                receipt._explicit_connection_fields("unused", lambda _, value=changed: value)

    def test_partial_dsn_cannot_use_ambient_libpq_target_or_credentials(self):
        psycopg = ModuleType("psycopg")
        psycopg.connect = mock.Mock()
        psycopg.IsolationLevel = SimpleNamespace(REPEATABLE_READ="repeatable-read")
        conninfo = ModuleType("psycopg.conninfo")
        conninfo.conninfo_to_dict = mock.Mock(return_value={"dbname": "declared-only"})
        rows = ModuleType("psycopg.rows")
        rows.dict_row = object()
        values = {
            receipt.DATABASE_URL_ENV: "dbname=declared-only",
            receipt.START_UTC_ENV: NOW.isoformat(),
            receipt.END_UTC_ENV: NOW.replace(hour=13).isoformat(),
            "PGHOST": "ambient-host", "PGPORT": "5432", "PGUSER": "ambient-user",
            "PGPASSWORD": "ambient-secret",
        }
        with mock.patch.dict(sys.modules, {"psycopg": psycopg,
                                           "psycopg.conninfo": conninfo,
                                           "psycopg.rows": rows}):
            result = receipt.run_from_environment(values)
        self.assertEqual(result["status"], "UNKNOWN_NOT_QUERIED")
        self.assertEqual(result["reason"], "EXPLICIT_DATABASE_CONNECTION_FIELDS_INVALID")
        self.assertFalse(result["database_connection_attempted"])
        psycopg.connect.assert_not_called()

    def test_reversed_time_range_never_imports_driver_or_connects(self):
        psycopg = ModuleType("psycopg")
        psycopg.connect = mock.Mock()
        rows = ModuleType("psycopg.rows")
        rows.dict_row = object()
        values = {
            receipt.DATABASE_URL_ENV: "postgresql://unused",
            receipt.START_UTC_ENV: NOW.replace(hour=13).isoformat(),
            receipt.END_UTC_ENV: NOW.isoformat(),
        }
        with mock.patch.dict(sys.modules, {"psycopg": psycopg, "psycopg.rows": rows}):
            result = receipt.run_from_environment(values)
        self.assertEqual(result["status"], "UNKNOWN_NOT_QUERIED")
        self.assertEqual(result["reason"], "AUDIT_TIME_RANGE_INVALID")
        self.assertFalse(result["database_connection_attempted"])
        psycopg.connect.assert_not_called()

    def test_driver_import_failure_is_not_reported_as_a_connection_attempt(self):
        values = {
            receipt.DATABASE_URL_ENV: "postgresql://unused",
            receipt.START_UTC_ENV: NOW.isoformat(),
            receipt.END_UTC_ENV: NOW.replace(hour=13).isoformat(),
        }
        original_import = __import__

        def blocked(name, *args, **kwargs):
            if name == "psycopg" or name.startswith("psycopg."):
                raise ModuleNotFoundError("driver deliberately unavailable")
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=blocked):
            result = receipt.run_from_environment(values)
        self.assertEqual(result["status"], "QUERY_FAILED")
        self.assertEqual(result["reason"], "DATABASE_DRIVER_UNAVAILABLE")
        self.assertFalse(result["database_connection_attempted"])

    def test_cli_exit_codes_cannot_treat_unknown_or_partial_as_complete(self):
        for status, expected in (("COMPLETE_BOUNDED_COHORT", 0),
                                 ("QUERY_FAILED", 2),
                                 ("UNKNOWN_NOT_QUERIED", 3),
                                 ("BOUNDED_PARTIAL", 4)):
            with self.subTest(status=status), \
                    mock.patch.object(receipt, "run_from_environment",
                                      return_value={"status": status}), \
                    mock.patch("builtins.print"):
                self.assertEqual(receipt.main(), expected)


if __name__ == "__main__":
    unittest.main()
