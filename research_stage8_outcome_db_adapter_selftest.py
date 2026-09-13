"""Adversarial, network-free tests for the Stage-8 outcome DB adapter."""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import inspect
import re
import unittest
from unittest import mock

import canonical_price_path
import research_btc_parent_movement as btc_parent
import research_common_window_metrics as common_window
from research_operational_score_source_audit_selftest import (
    AS_OF, DECISION, _anchor, _bar, _outcome, _snapshot,
)
import research_stage8_acceptance as acceptance
import research_stage8_contract as contract
import research_stage8_feature_projection as projection
from research_stage8_feature_projection_selftest import (
    _fresh, _selection_attestation, _set_model,
)
import research_stage8_outcome_db_adapter as adapter
import research_stage8_projection_db_adapter as projection_adapter
import research_stage8_registry as registry_adapter
import research_stage8_representative_selector as selector


def _sha(label: str) -> str:
    return contract.digest({"fixture": label})


def _iso(value) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _selection_fact_identity(binding, attempt, fact, parent_evidence):
    identity = fact["identity"]
    return {
        "version": "stage8-selection-fact-identity-v1",
        "exact_binding_sha256": binding["binding_sha256"],
        "attempt_id": attempt["attempt_id"],
        "attempt_fingerprint": attempt["attempt_fingerprint"],
        "sampler_version": attempt["sampler_version"],
        "source_candle_open_utc": _iso(attempt["source_candle_open_utc"]),
        "source_attempt_evaluation_status": attempt["evaluation_status"],
        "anchor_slot_id": identity["anchor_slot_id"],
        "event_id": identity["event_id"],
        "event_fingerprint": identity["event_fingerprint"],
        "symbol": attempt["symbol"],
        "direction": binding["binding"]["candidate"]["direction"],
        "decision_time_utc": identity["decision_time_utc"],
        "knowledge_status": fact["knowledge_status"],
        "candidate_match": fact["candidate_match"],
        "parent_authority_class": "LIVE",
        "btc_parent_movement_id": parent_evidence["btc_parent_movement_id"],
        "parent_start_time_utc": parent_evidence["parent_start_time_utc"],
        "parent_policy_version": btc_parent.POLICY_VERSION,
        "membership_status": parent_evidence["membership_status"],
        "parent_evidence_eligible": parent_evidence[
            "parent_evidence_eligible"
        ],
    }


class _Result:
    def __init__(self, rows):
        self._rows = deepcopy(rows)

    def fetchall(self):
        return deepcopy(self._rows)


class FakeConnection:
    autocommit = False

    def __init__(self, fixture, *, read_only="on", isolation="repeatable read",
                 timeout="5s", change_transaction=False):
        self.fixture = deepcopy(fixture)
        self.read_only = read_only
        self.isolation = isolation
        self.timeout = timeout
        self.change_transaction = change_transaction
        self.transaction_reads = 0
        self.queries = []

    def execute(self, sql, params=None):
        params = params or {}
        self.queries.append((sql, deepcopy(params)))
        if "stage8-outcome:transaction" in sql:
            self.transaction_reads += 1
            changed = self.change_transaction and self.transaction_reads > 1
            return _Result([{
                "read_only": self.read_only,
                "isolation": self.isolation,
                "statement_timeout": self.timeout,
                "backend_pid": 1002 if changed else 1001,
                "transaction_started_at_utc": adapter._utc(
                    "2026-09-13T02:00:00.000000Z"
                ),
                "transaction_snapshot": "101:101:" if changed else "100:100:",
                "database_role": registry_adapter.READER_ROLE,
                "session_role": registry_adapter.READER_ROLE,
                "database_timezone": "UTC",
                "trusted_schema": "stage8_fixture",
                "effective_schemas": ["stage8_fixture", "pg_catalog"],
                "trusted_schema_create_allowed": False,
                "trusted_relations_resolved": True,
                "observed_at_utc": adapter._utc(
                    "2026-09-13T02:00:02.000000Z"
                    if self.transaction_reads > 1
                    else "2026-09-13T02:00:01.000000Z"
                ),
            }])
        if "stage8-outcome:registry-selection" in sql:
            if (params.get("selection_record_sha256")
                    != self.fixture["selection"]["selection_record_sha256"]
                    or params.get("exact_binding_sha256")
                    != self.fixture["registry"]["exact_binding_sha256"]):
                return _Result([])
            return _Result([{
                "adapter_registry_json": self.fixture["registry"],
                "adapter_selection_json": self.fixture["selection"],
            }])
        if "stage8-outcome:batch-seal" in sql:
            return _Result([{
                "adapter_batch_json": self.fixture["batch"],
                "adapter_seal_json": self.fixture["seal"],
            }])
        if "stage8-outcome:facts" in sql:
            return _Result([{
                "adapter_fact_json": item["fact"],
                "adapter_event_json": item["event"],
            } for item in self.fixture["facts"]][:params["limit"]])
        if "stage8-outcome:probability" in sql:
            ids = set(params["event_ids"])
            rows = [row for row in self.fixture["outcomes"]
                    if row.get("event_id") in ids
                    and row.get("window_minutes") == params["window_minutes"]
                    and row.get("threshold_bps") == params["threshold_bps"]
                    and row.get("method_version") == params["method_version"]]
            return _Result([{"adapter_outcome_json": row}
                            for row in rows[:params["limit"]]])
        if "stage8-outcome:asymmetry" in sql:
            ids = set(params["event_ids"])
            rows = [row for row in self.fixture["metrics"]
                    if row.get("event_id") in ids
                    and row.get("window_minutes") == params["window_minutes"]
                    and row.get("method_version") == params["method_version"]]
            return _Result([{"adapter_metric_json": row}
                            for row in rows[:params["limit"]]])
        raise AssertionError("unexpected SQL: " + sql)


class Fixture:
    def __init__(self, count=5, *, symbol="BTC", threshold_bps=50,
                 outcome_status="SUCCESS", include_outcomes=True,
                 include_metrics=True, zero_mae=False):
        scope = "HYPE_SPOT_107" if symbol == "HYPE" else "BINANCE_" + symbol
        self.binding = contract.exact_binding(
            scope_id=scope,
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=threshold_bps,
        )
        self.frozen_at = "2026-08-28T00:00:00.000000Z"
        self.registry = self._registry()
        facts, identities, outcomes, metrics = [], [], [], []
        for number in range(1, count + 1):
            attempt, slot, event_pair = _anchor(symbol=symbol, attempt_id=number)
            snapshot = _set_model(
                _fresh(_snapshot(snapshot_id=100 + number)), symbol=symbol,
            )
            watch = _selection_attestation(attempt, snapshot)
            fact = projection.project_binding_fact(
                attempt=attempt, anchor_slot=slot, anchor_events=event_pair,
                selected_watch_snapshot=snapshot,
                watch_selection_attestation=watch, binding=self.binding,
            )
            assert fact["knowledge_status"] == "KNOWN" and fact["candidate_match"] is True
            event = next(row for row in event_pair
                         if row["event_id"] == fact["identity"]["event_id"])
            event = deepcopy(event) | {
                "runtime_session_id": "stage8-outcome-fixture",
                "delivery_attempted_at_utc": None,
                "delivered_at_utc": None,
                "created_at": DECISION,
            }
            parent_start = DECISION - timedelta(minutes=30 - number)
            parent_id = btc_parent._identity(parent_start)
            parent_evidence = {
                "version": selector.MEMBERSHIP_EVIDENCE_VERSION,
                "validation_status": "VALID",
                "event_id": event["event_id"],
                "event_fingerprint": event["event_fingerprint"],
                "symbol": symbol, "direction": event["direction"],
                "decision_time_utc": _iso(DECISION),
                "parent_policy_version": btc_parent.POLICY_VERSION,
                "membership_status": "LIVE",
                "btc_parent_movement_id": parent_id,
                "btc_observed_close_utc": _iso(DECISION),
                "parent_start_time_utc": _iso(parent_start),
                "parent_end_time_utc": None,
                "parent_confirmed_at_utc": _iso(parent_start),
                "parent_direction": "UP",
                "parent_evidence_eligible": True,
                "parent_boundary_reason": "CAUSAL_CLOSE_REVERSAL",
                "parent_observed_through_utc": _iso(DECISION),
                "parent_price_source": btc_parent.SOURCE,
                "btc_bar": {
                    "open_time_utc": _iso(DECISION - timedelta(minutes=1)),
                    "close_time_utc": _iso(DECISION),
                    "open": 100.0, "high": 101.0, "low": 99.0,
                    "close": 100.0, "price_source": btc_parent.SOURCE,
                },
            }
            parent_sha = contract.digest(parent_evidence)
            watch_code_sha = projection._watch_code_manifest(snapshot)[1]
            authority = projection_adapter._fact_authority(
                fact,
                expected_selection_hash=watch["attestation_sha256"],
                expected_code_hash=watch_code_sha,
                watch_selection_observation_hash=_sha("watch-observation-" + str(number)),
                expected_parent_hash=parent_sha,
                expected_noneligibility_hash=None,
            )
            selection_fact_identity = _selection_fact_identity(
                self.binding, attempt, fact, parent_evidence,
            )
            selection_fact_identity_sha256 = contract.digest(
                selection_fact_identity
            )
            persisted = "2026-09-13T01:00:00.000000Z"
            fact_row = {
                "fact_record_sha256": None,
                "fact_batch_record_sha256": None,
                "exact_binding_sha256": self.binding["binding_sha256"],
                "attempt_id": attempt["attempt_id"],
                "attempt_fingerprint": fact["identity"]["attempt_fingerprint"],
                "anchor_slot_id": fact["identity"]["anchor_slot_id"],
                "event_id": fact["identity"]["event_id"],
                "event_fingerprint": fact["identity"]["event_fingerprint"],
                "symbol": symbol, "direction": fact["identity"]["direction"],
                "decision_time_utc": fact["identity"]["decision_time_utc"],
                "knowledge_status": "KNOWN", "candidate_match": True,
                "fact": fact, "fact_sha256": fact["fact_sha256"],
                "fact_authority": authority,
                "fact_authority_sha256": authority["fact_authority_sha256"],
                "watch_selection_attestation": watch,
                "watch_selection_attestation_sha256": watch["attestation_sha256"],
                "observed_watch_code_manifest_sha256": watch_code_sha,
                "parent_membership_evidence": parent_evidence,
                "parent_membership_evidence_sha256": parent_sha,
                "noneligibility_proof": None,
                "noneligibility_proof_sha256": None,
                "selection_fact_identity": selection_fact_identity,
                "selection_fact_identity_sha256":
                    selection_fact_identity_sha256,
                "persisted_at_utc": persisted,
                "fact_record": None,
            }
            facts.append({"fact": fact_row, "event": event})
            identities.append({
                "version": "stage8-outcome-free-representative-identity-v1",
                "exact_binding_sha256": self.binding["binding_sha256"],
                "btc_parent_movement_id": parent_id,
                "parent_start_time_utc": _iso(parent_start),
                "expected_selection_fact_identity_sha256":
                    selection_fact_identity_sha256,
                "attempt_fingerprint": fact["identity"]["attempt_fingerprint"],
                "anchor_slot_id": fact["identity"]["anchor_slot_id"],
                "event_id": fact["identity"]["event_id"],
                "event_fingerprint": fact["identity"]["event_fingerprint"],
                "symbol": symbol, "direction": fact["identity"]["direction"],
                "decision_time_utc": fact["identity"]["decision_time_utc"],
                "candidate_match_knowledge_status": "KNOWN",
                "candidate_match": True,
            })
            if include_outcomes:
                outcomes.append(self._outcome(event, outcome_status, threshold_bps))
            if include_metrics:
                metrics.append(self._metric(event, zero_mae=zero_mae))

        identities.sort(key=contract.canonical)
        attempt_ids = sorted(item["fact"]["attempt_id"] for item in facts)
        self.batch = self._batch(attempt_ids)
        batch_sha = self.batch["fact_batch_record_sha256"]
        for item in facts:
            fact_row = item["fact"]
            fact_row["fact_batch_record_sha256"] = batch_sha
            record = {
                "version": "stage8-durable-projected-fact-record-v1",
                "fact_batch_record_sha256": batch_sha,
                "exact_binding_sha256": fact_row["exact_binding_sha256"],
                "attempt_id": fact_row["attempt_id"],
                "attempt_fingerprint": fact_row["attempt_fingerprint"],
                "anchor_slot_id": fact_row["anchor_slot_id"],
                "event_id": fact_row["event_id"],
                "event_fingerprint": fact_row["event_fingerprint"],
                "symbol": fact_row["symbol"], "direction": fact_row["direction"],
                "decision_time_utc": fact_row["decision_time_utc"],
                "knowledge_status": "KNOWN", "candidate_match": True,
                "fact_sha256": fact_row["fact_sha256"],
                "fact_authority_sha256": fact_row["fact_authority_sha256"],
                "watch_selection_attestation_sha256":
                    fact_row["watch_selection_attestation_sha256"],
                "observed_watch_code_manifest_sha256":
                    fact_row["observed_watch_code_manifest_sha256"],
                "parent_membership_evidence_sha256":
                    fact_row["parent_membership_evidence_sha256"],
                "noneligibility_proof_sha256": None,
                "selection_fact_identity_sha256": fact_row[
                    "selection_fact_identity_sha256"
                ],
                "server_projection_attestation_sha256": _sha(
                    "server-projection-" + str(fact_row["attempt_id"])
                ),
                "server_projection_status": "VERIFIED",
                "persisted_at_utc": fact_row["persisted_at_utc"],
                "persisted_by": registry_adapter.FACT_WRITER_ROLE,
            }
            fact_row["fact_record"] = record
            fact_row["fact_record_sha256"] = contract.digest(record)
        facts.sort(key=lambda item: item["fact"]["attempt_id"])
        self.seal = self._seal(facts)
        self.selection = self._selection(identities, facts)
        self.data = {
            "registry": self.registry, "selection": self.selection,
            "batch": self.batch, "seal": self.seal, "facts": facts,
            "outcomes": outcomes, "metrics": metrics,
        }
        self.replay_result = self._replay_result(facts)

    def _replay_result(self, facts):
        artifacts = self.registry["implementation_artifacts"]["files"]
        source_manifest = {
            "research_stage8_contract.py": artifacts["contract"]["sha256"],
            "research_stage8_feature_projection.py":
                artifacts["projection"]["sha256"],
            "research_stage8_projection_db_adapter.py":
                artifacts["projection_db_adapter"]["sha256"],
            "research_operational_score_source_audit.py":
                artifacts["source_audit"]["sha256"],
            "research_stage8_representative_selector.py":
                artifacts["selector"]["sha256"],
            "research_watch_score_capture.py":
                artifacts["watch_capture"]["sha256"],
        }
        attempt_ids = [item["fact"]["attempt_id"] for item in facts]
        shared = adapter.source_audit.transaction_identity_from_fields(
            backend_pid=1001,
            transaction_started_at_utc="2026-09-13T02:00:00.000000Z",
            database_snapshot_id="100:100:",
        )
        transaction = {
            "backend_pid": 1001,
            "transaction_started_at_utc": shared["transaction_started_at_utc"],
            "database_snapshot_id": "100:100:", "read_only": True,
            "isolation": "repeatable read", "statement_timeout_ms": 5000.0,
            "transaction_identity_sha256": shared["transaction_identity_sha256"],
            "observed_at_utc": "2026-09-13T02:00:01.500000Z",
        }
        query = {
            "version": projection_adapter.VERSION,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "projection_version": projection.VERSION,
            "source_audit_version": adapter.source_audit.VERSION,
            "projection_mode": projection_adapter.EXACT_BINDING_PROJECTION_MODE,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "population_kind": "EXACT_BINDING_FULL_COHORT_ATTEMPT_IDS",
            "requested_attempt_ids": attempt_ids,
            "max_attempts": projection_adapter.MAX_EXACT_BINDING_ATTEMPTS,
            "max_queries": projection_adapter.MAX_QUERIES,
            "max_wall_seconds": float(projection_adapter.MAX_WALL_SECONDS),
            "max_statement_timeout_ms": projection_adapter.MAX_STATEMENT_TIMEOUT_MS,
            "max_capture_age_seconds": 300.0,
            "capture_selection_policy": projection.WATCH_SELECTION_POLICY,
        }
        query_sha = contract.digest(query)
        rows = []
        for item in facts:
            fact_row = item["fact"]
            ledger = {
                "attempt_id": fact_row["attempt_id"],
                "exact_binding_sha256": self.binding["binding_sha256"],
                "fact": deepcopy(fact_row["fact"]),
                "fact_authority": deepcopy(fact_row["fact_authority"]),
                "watch_selection_attestation": deepcopy(
                    fact_row["watch_selection_attestation"]
                ),
                "expected_watch_selection_attestation_sha256":
                    fact_row["watch_selection_attestation_sha256"],
                "watch_code_manifest": deepcopy(
                    self.registry["expected_watch_code_manifest"]
                ),
                "expected_watch_code_manifest_sha256":
                    fact_row["observed_watch_code_manifest_sha256"],
                "watch_selection_observation": None,
                "parent_membership_source": None,
                "parent_membership_evidence": deepcopy(
                    fact_row["parent_membership_evidence"]
                ),
                "noneligibility_proof": deepcopy(fact_row["noneligibility_proof"]),
            }
            fact_identity = fact_row["fact"]["identity"]
            rows.append({
                "attempt_id": fact_row["attempt_id"],
                "attempt_identity": {
                    "attempt_id": fact_row["attempt_id"],
                    "row_status": "FOUND",
                    "attempt_fingerprint": fact_identity[
                        "attempt_fingerprint"
                    ],
                    "sampler_version": fact_identity["sampler_version"],
                    "symbol": fact_identity["symbol"],
                    "evaluation_status": fact_row["selection_fact_identity"][
                        "source_attempt_evaluation_status"
                    ],
                    "source_candle_open_utc": fact_identity[
                        "source_candle_open_utc"
                    ],
                    "decision_time_utc": fact_identity["decision_time_utc"],
                },
                "fact_ledger": [ledger],
            })
        population_unsigned = {
            "version": projection_adapter.POPULATION_RECEIPT_VERSION,
            "projection_mode": projection_adapter.EXACT_BINDING_PROJECTION_MODE,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "query_binding_sha256": query_sha,
            "requested_attempt_ids": attempt_ids, "found_attempt_ids": attempt_ids,
            "missing_attempt_ids": [],
            "attempt_population_sha256": _sha("replay-attempt-population"),
            "outcome_free_authority_ledger_sha256": _sha("replay-authority-ledger"),
            "archive_snapshot_high_water_id": 999,
            "database_snapshot_id": transaction["database_snapshot_id"],
            "transaction_identity_sha256": transaction[
                "transaction_identity_sha256"
            ],
            "read_started_at_utc": "2026-09-13T02:00:01.000000Z",
            "read_finished_at_utc": "2026-09-13T02:00:01.900000Z",
            "read_only": True, "transaction_isolation": "repeatable read",
            "population_complete": True, "truncated": False,
            "query_count": projection_adapter.MAX_QUERIES,
        }
        exact_population_sha = contract.digest(population_unsigned)
        population = {
            **population_unsigned,
            "exact_attempt_population_receipt_sha256": exact_population_sha,
        }
        population_sha = contract.digest(population)
        population["population_receipt_sha256"] = population_sha
        authority_unsigned = {
            "version": projection_adapter.AUTHORITY_RECEIPT_VERSION,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "projection_mode": projection_adapter.EXACT_BINDING_PROJECTION_MODE,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "projection_source_manifest_sha256": contract.digest(source_manifest),
            "projection_module_sha256": artifacts["projection"]["sha256"],
            "db_adapter_module_sha256":
                artifacts["projection_db_adapter"]["sha256"],
            "parent_evidence_module_sha256": artifacts["selector"]["sha256"],
            "population_receipt_sha256": population_sha,
            "exact_attempt_population_receipt_sha256": exact_population_sha,
            "authority_rows_sha256": _sha("replay-authority-rows"),
        }
        authority = {**authority_unsigned,
                     "authority_receipt_sha256": contract.digest(authority_unsigned)}
        result = {
            "version": projection_adapter.VERSION,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "projection_version": projection.VERSION,
            "source_audit_version": adapter.source_audit.VERSION,
            "projection_mode": projection_adapter.EXACT_BINDING_PROJECTION_MODE,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "query_scope": query, "query_binding_sha256": query_sha,
            "projection_source_manifest": source_manifest,
            "projection_source_manifest_sha256": contract.digest(source_manifest),
            "projection_module_sha256": artifacts["projection"]["sha256"],
            "db_adapter_module_sha256": artifacts["projection_db_adapter"]["sha256"],
            "parent_evidence_module_sha256": artifacts["selector"]["sha256"],
            "transaction": transaction, "archive_snapshot_high_water_id": 999,
            "population_receipt": population,
            "exact_attempt_population_receipt_sha256": exact_population_sha,
            "authority_receipt": authority, "rows": rows,
            "interpretation": "selftest authoritative replay",
        }
        return {**result, "result_sha256": contract.digest(result)}

    def _registry(self):
        manifest = contract.frozen_manifest()
        artifacts = registry_adapter.implementation_artifacts()
        watch = registry_adapter.expected_watch_code_manifest()
        profile = {
            "version": registry_adapter.VERIFIER_PROFILE_VERSION,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "implementation_artifacts_sha256": contract.digest(artifacts),
            "expected_watch_code_manifest_sha256": contract.digest(watch),
        }
        record = {
            "version": registry_adapter.REGISTRY_RECORD_VERSION,
            "exact_binding": self.binding,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "hash_version": manifest["hash_version"],
            "source_audit_version": manifest["source"]["audit_version"],
            "candidate_version": manifest["candidates"]["version"],
            "parent_policy_version": manifest["independence"]["parent_policy_version"],
            "implementation_artifacts": artifacts,
            "expected_watch_code_manifest": watch,
            "verifier_profile": profile,
            "freeze_id": _sha("freeze"), "frozen_at_utc": self.frozen_at,
            "registered_by": registry_adapter.REGISTRAR_ROLE,
        }
        inner = self.binding["binding"]
        return {
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
            "frozen_at_utc": self.frozen_at, "freeze_id": _sha("freeze"),
            "registry_record": record,
            "registry_record_sha256": contract.digest(record),
        }

    def _batch(self, attempt_ids):
        artifacts = self.registry["implementation_artifacts"]["files"]
        query = {"version": "fixture-query-v1", "scope": self.binding["binding_sha256"]}
        population = {"version": "fixture-population-v1", "attempt_ids": attempt_ids}
        population["population_receipt_sha256"] = contract.digest(population)
        authority = {"version": "fixture-authority-v1", "attempt_ids": attempt_ids}
        authority["authority_receipt_sha256"] = contract.digest(authority)
        value = {
            "fact_batch_record_sha256": None,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "freeze_id": self.registry["freeze_id"],
            "registry_record_sha256": self.registry["registry_record_sha256"],
            "verifier_profile_sha256": self.registry["verifier_profile_sha256"],
            "registry_verification_receipt_sha256": _sha("registry-verification"),
            "projection_adapter_version": projection_adapter.VERSION,
            "observed_projection_source_sha256": artifacts["projection"]["sha256"],
            "observed_projection_adapter_source_sha256":
                artifacts["projection_db_adapter"]["sha256"],
            "observed_registry_adapter_source_sha256":
                artifacts["registry_adapter"]["sha256"],
            "observed_registry_migration_sha256": artifacts["registry_migration"]["sha256"],
            "projection_source_manifest": {"fixture.py": _sha("projection-source")},
            "projection_source_manifest_sha256": None,
            "adapter_query_binding_sha256": _sha("adapter-query"),
            "adapter_population_receipt": population,
            "adapter_population_receipt_sha256": population[
                "population_receipt_sha256"
            ],
            "adapter_authority_receipt": authority,
            "adapter_authority_receipt_sha256": authority[
                "authority_receipt_sha256"
            ],
            "adapter_result_sha256": _sha("adapter-result"),
            "coverage_query_scope": query,
            "coverage_query_sha256": _sha("cohort-query"),
            "outcome_free_population_receipt_sha256": _sha("outcome-free-population"),
            "coverage_attempt_population_sha256": _sha("attempt-population"),
            "coverage_source_high_water_attempt_id": max(attempt_ids),
            "watch_archive_high_water_snapshot_set_id": 999,
            "attempt_ids": attempt_ids, "attempt_count": len(attempt_ids),
            "persisted_at_utc": "2026-09-13T00:30:00.000000Z",
            "fact_batch_record": None,
        } | {
            "projection_source_manifest_sha256": contract.digest(
                {"fixture.py": _sha("projection-source")}
            )
        }
        record = {
            "version": "stage8-durable-projection-fact-batch-v1",
            **{key: deepcopy(value[key]) for key in adapter._FACT_BATCH_RECORD_KEYS
               if key not in {"version", "persisted_by"}},
            "persisted_by": registry_adapter.FACT_WRITER_ROLE,
        }
        value["fact_batch_record"] = record
        value["fact_batch_record_sha256"] = contract.digest(record)
        return value

    def _seal(self, facts):
        fact_set = contract.digest({
            "version": "stage8-durable-fact-record-set-v1",
            "fact_batch_record_sha256": self.batch["fact_batch_record_sha256"],
            "facts": [{"attempt_id": item["fact"]["attempt_id"],
                       "fact_record_sha256": item["fact"]["fact_record_sha256"]}
                      for item in facts],
        })
        record = {
            "version": "stage8-durable-projection-fact-seal-v1",
            "fact_batch_record_sha256": self.batch["fact_batch_record_sha256"],
            "fact_count": len(facts), "fact_records_sha256": fact_set,
            "sealed_at_utc": "2026-09-13T01:10:00.000000Z",
            "sealed_by": registry_adapter.FACT_WRITER_ROLE,
        }
        return {
            "fact_batch_record_sha256": self.batch["fact_batch_record_sha256"],
            "fact_count": len(facts), "fact_records_sha256": fact_set,
            "sealed_at_utc": record["sealed_at_utc"], "seal_record": record,
            "seal_record_sha256": contract.digest(record),
        }

    def _selection(self, identities, facts):
        representative_binding = acceptance.representative_binding(self.binding)
        rows = []
        for identity in identities:
            row = {
                "binding": deepcopy(representative_binding),
                "btc_parent_movement_id": identity["btc_parent_movement_id"],
                "parent_start_time_utc": identity["parent_start_time_utc"],
                "representative_status": "VALID",
                "parent_policy_version": btc_parent.POLICY_VERSION,
                "membership_status": "LIVE", "parent_evidence_eligible": True,
                "freeze_id": self.registry["freeze_id"],
                "registry_record_sha256": self.registry["registry_record_sha256"],
                "registry_verification_receipt_sha256":
                    self.batch["registry_verification_receipt_sha256"],
                "selection_attestation_sha256": None,
                "representative": {key: identity[key] for key in (
                    "expected_selection_fact_identity_sha256",
                    "attempt_fingerprint",
                    "anchor_slot_id", "event_id", "event_fingerprint", "symbol",
                    "direction", "decision_time_utc",
                    "candidate_match_knowledge_status", "candidate_match",
                )},
                "representative_identity_sha256": contract.digest(identity),
            }
            rows.append(row)
        _, set_sha = acceptance._representative_set(self.binding, rows)
        batch = {
            "version": selector.BATCH_VERSION, "selector_version": selector.VERSION,
            "structural_authority": selector.STRUCTURAL_AUTHORITY,
            "status": "COMPLETE",
            "exact_binding_sha256": self.binding["binding_sha256"],
            "manifest_sha256": contract.MANIFEST_SHA256,
            "freeze_id": self.registry["freeze_id"],
            "frozen_at_utc": self.registry["frozen_at_utc"],
            "registry_record_sha256": self.registry["registry_record_sha256"],
            "registry_verification_receipt_sha256":
                self.batch["registry_verification_receipt_sha256"],
            "verifier_profile_sha256": self.registry["verifier_profile_sha256"],
            "expected_projection_source_sha256":
                self.registry["implementation_artifacts"]["files"]["projection"]["sha256"],
            "expected_selector_source_sha256":
                self.registry["implementation_artifacts"]["files"]["selector"]["sha256"],
            "expected_watch_code_manifest_sha256":
                self.registry["expected_watch_code_manifest_sha256"],
            "cohort_query_sha256": self.batch["coverage_query_sha256"],
            "outcome_free_population_receipt_sha256":
                self.batch["outcome_free_population_receipt_sha256"],
            "attempt_population_sha256":
                self.batch["coverage_attempt_population_sha256"],
            "source_transaction_identity_sha256": _sha("source-transaction"),
            "source_high_water_attempt_id":
                self.batch["coverage_source_high_water_attempt_id"],
            "source_attempt_count": len(facts),
            "source_authority_ledger_count": len(facts),
            "source_authority_ledger_sha256": _sha("source-authority-ledger"),
            "representative_count": len(identities),
            "representative_set_sha256": set_sha,
            "population_coverage_complete": True,
            "candidate_match_coverage_complete": True,
            "outcome_blind_selection": True,
            "outcome_or_label_fields_accepted": False,
            "truncated": False, "global_blockers": [], "blocked_parents": [],
            "excluded_pre_freeze_parent_ids": [],
            "proven_noneligible_attempt_ids": [],
            "deduplicated_exact_anchor_event_count": 0,
            "qualification_evaluated": False,
            "database_verification_asserted_by_selector": False,
        }
        persisted = "2026-09-13T01:30:00.000000Z"
        identity_sha = contract.digest({
            "version": "stage8-durable-representative-identities-v1",
            "exact_binding_sha256": self.binding["binding_sha256"],
            "representatives": identities,
        })
        selection = {
            "selection_record_sha256": None,
            "fact_batch_record_sha256": self.batch["fact_batch_record_sha256"],
            "exact_binding_sha256": self.binding["binding_sha256"],
            "freeze_id": self.registry["freeze_id"],
            "registry_record_sha256": self.registry["registry_record_sha256"],
            "verifier_profile_sha256": self.registry["verifier_profile_sha256"],
            "registry_verification_receipt_sha256":
                self.batch["registry_verification_receipt_sha256"],
            "selector_version": selector.VERSION,
            "observed_projection_source_sha256":
                self.registry["implementation_artifacts"]["files"]["projection"]["sha256"],
            "observed_selector_source_sha256":
                self.registry["implementation_artifacts"]["files"]["selector"]["sha256"],
            "observed_watch_code_manifest_sha256":
                self.registry["expected_watch_code_manifest_sha256"],
            "cohort_query_sha256": self.batch["coverage_query_sha256"],
            "outcome_free_population_receipt_sha256":
                self.batch["outcome_free_population_receipt_sha256"],
            "source_high_water_attempt_id":
                self.batch["coverage_source_high_water_attempt_id"],
            "representative_count": len(identities),
            "representative_set_sha256": set_sha,
            "representative_identities": identities,
            "representative_identities_sha256": identity_sha,
            "selection_attestation": batch,
            "selection_attestation_sha256": contract.digest(batch),
            "persisted_at_utc": persisted, "selection_record": None,
        }
        record = {
            "version": registry_adapter.SELECTION_RECORD_VERSION,
            **{key: selection[key] for key in (
                "fact_batch_record_sha256", "exact_binding_sha256", "freeze_id",
                "registry_record_sha256", "verifier_profile_sha256",
                "registry_verification_receipt_sha256", "selector_version",
                "observed_projection_source_sha256",
                "observed_selector_source_sha256",
                "observed_watch_code_manifest_sha256", "cohort_query_sha256",
                "outcome_free_population_receipt_sha256",
                "source_high_water_attempt_id", "representative_count",
                "representative_set_sha256", "representative_identities_sha256",
                "selection_attestation_sha256", "persisted_at_utc",
            )},
            "persisted_by": registry_adapter.SELECTOR_WRITER_ROLE,
        }
        selection["selection_record"] = record
        selection["selection_record_sha256"] = contract.digest(record)
        return selection

    def _outcome(self, event, status, threshold_bps):
        generated = _outcome(event, status=status, threshold_bps=threshold_bps)
        result = {key: generated.get(key) for key in adapter._OUTCOME_KEYS}
        result["threshold_policy"] = {}
        result["calculation_audit"] = {}
        if event["symbol"] == "HYPE":
            result.update(
                price_source=(
                    "reference=hyperliquid_spot_@107|"
                    "path=hyperliquid_spot:HYPE/USDT:1m|provenance=SELFTEST"
                ),
                market_pair="HYPE/USDT",
                data_quality_status=canonical_price_path.HYPERLIQUID_COMPLETE,
                calculation_audit={"price_provenance": {"instrument": "@107"}},
            )
        return result

    def _metric(self, event, *, zero_mae=False):
        price = float(event["current_price"])
        decision = adapter._utc(event["alert_time_utc"])
        candles = []
        for number in range(60):
            opened = decision + timedelta(minutes=number)
            if event["direction"] == "SHORT":
                high, low = (price if zero_mae else price * 1.01), price * 0.98
            else:
                high, low = price * 1.02, (price if zero_mae else price * 0.99)
            candles.append(_bar(opened, price, high=high, low=low))
        hype = event["symbol"] == "HYPE"
        path = {
            "symbol": event["symbol"],
            "exchange": "hyperliquid" if hype else "binance",
            "market": "spot",
            "pair": "HYPE/USDT" if hype else event["symbol"] + "USDT",
            "api_coin": "@107" if hype else None,
            "interval": "1m", "interval_seconds": 60,
            "complete": True, "provenance": "SELFTEST",
        }
        result = common_window.calculate_common_window_metrics(
            symbol=event["symbol"], reference_price=price,
            direction=event["direction"], event_time=decision,
            window_minutes=60, candles=candles,
            observed_at=decision + timedelta(minutes=61), path_result=path,
        )
        return {
            "event_id": event["event_id"], "window_minutes": 60,
            "method_version": common_window.METHOD_VERSION, "status": "READY",
            "measurement_start_utc": decision,
            "window_end_utc": decision + timedelta(minutes=60),
            "next_attempt_at_utc": decision + timedelta(minutes=61),
            "result": result,
            "created_at_utc": AS_OF, "updated_at_utc": AS_OF,
        }


class OutcomeDatabaseAdapterTests(unittest.TestCase):
    def evaluate(self, fixture, **connection_kwargs):
        conn = FakeConnection(fixture.data, **connection_kwargs)
        replay = mock.Mock(return_value=deepcopy(fixture.replay_result))
        with mock.patch.object(adapter, "_PROJECT_EXACT_BINDING", replay), \
             mock.patch.object(
                 adapter.projection_db_adapter,
                 "project_exact_binding_attempts_from_connection", replay,
             ):
            result = adapter.evaluate_selection_outcomes_from_connection(
                conn, fixture.binding,
                selection_record_sha256=fixture.selection["selection_record_sha256"],
            )
        for evaluation in (result["evaluation"],
                           result["persistence_payload"]["evaluation"]):
            self.assertIs(evaluation["research_qualified"], False)
            self.assertNotEqual(evaluation["status"],
                                "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY")
            self.assertIn("SERVER_DB_REPLAY_ATTESTATION_REQUIRED",
                          evaluation["qualification_blockers"])
            for key in ("authoritative_fact_replay_verified",
                        "durable_fact_source_authority_verified",
                        "durable_outcome_atomic_gate_evidence_verified"):
                self.assertIs(evaluation[key], False)
        self.assertIs(result["research_qualified"], False)
        self.assertIs(result["fact_replay_receipt"]
                      ["durable_selection_server_recomputed"], False)
        for document in (result, result["evaluation"],
                         result["persistence_payload"],
                         result["persistence_payload"]["evaluation"]):
            for key in ("live_authorized", "telegram_authorized", "trade_authorized"):
                self.assertIs(document[key], False)
        return result, conn

    def test_same_snapshot_fact_replay_is_diagnostic_until_server_persistence(self):
        fixture = Fixture()
        result, conn = self.evaluate(fixture)
        evaluation = result["evaluation"]
        self.assertTrue(evaluation["atomic_gate_passed"], evaluation)
        self.assertEqual(evaluation["routes"]["PROBABILITY"]["status"], "PASS")
        self.assertEqual(evaluation["routes"]["ASYMMETRY"]["status"], "PASS")
        self.assertFalse(result["research_qualified"])
        self.assertFalse(evaluation["research_qualified"])
        self.assertFalse(evaluation["authoritative_fact_replay_verified"])
        self.assertFalse(evaluation["durable_fact_source_authority_verified"])
        self.assertFalse(evaluation["durable_outcome_atomic_gate_evidence_verified"])
        self.assertTrue(result["fact_replay_receipt"]["all_causal_fact_semantics_verified"])
        self.assertFalse(result["fact_replay_receipt"]["durable_selection_server_recomputed"])
        self.assertEqual(evaluation["qualification_blockers"], [
            "SERVER_DB_REPLAY_ATTESTATION_REQUIRED",
        ])
        self.assertEqual(evaluation["status"], "AWAITING_DURABLE_REGISTRY_VERIFICATION")
        self.assertNotIn("DURABLE_OUTCOME_EVIDENCE_NOT_VERIFIED",
                         evaluation["qualification_blockers"])
        self.assertFalse(result["live_authorized"])
        self.assertFalse(result["telegram_authorized"])
        self.assertFalse(result["trade_authorized"])
        self.assertEqual(result["evidence_receipt"]["query_count"], adapter.MAX_QUERIES)
        self.assertEqual(len(conn.queries), adapter.OUTCOME_QUERY_COUNT)
        self.assertEqual(result["evidence_receipt"]["btc_parent_movement_ids"], [
            item["btc_parent_movement_id"]
            for item in result["evidence_receipt"]["representatives"]
        ])
        unsigned = dict(result)
        supplied = unsigned.pop("result_sha256")
        self.assertEqual(contract.digest(unsigned), supplied)
        persistence = deepcopy(result["persistence_payload"])
        persistence_sha = persistence.pop("persistence_payload_sha256")
        self.assertEqual(contract.digest(persistence), persistence_sha)
        validated = adapter.registry_adapter._validate_outcome_persistence_payload(
            fixture.binding, result["persistence_payload"],
            selection_record_sha256=fixture.selection["selection_record_sha256"],
            registry_row=fixture.registry,
            selection_row=fixture.selection,
            durable_fact_rows=[
                item["fact"] for item in fixture.data["facts"]
            ],
        )
        self.assertEqual(
            validated["persistence_payload_sha256"],
            result["persistence_payload_sha256"],
        )

    def test_rehashed_qualification_flag_tamper_is_not_persistable(self):
        for count in (4, 5):
            with self.subTest(distinct_parent_count=count):
                fixture = Fixture(count=count)
                result, _ = self.evaluate(fixture)
                payload = deepcopy(result["persistence_payload"])
                evaluation = payload["evaluation"]
                self.assertEqual(evaluation["atomic_gate_passed"], count >= 5)
                evaluation["research_qualified"] = True
                evaluation["status"] = "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY"
                evaluation["qualification_blockers"] = []
                payload["evaluation_sha256"] = contract.digest(evaluation)
                payload.pop("persistence_payload_sha256")
                payload["persistence_payload_sha256"] = contract.digest(payload)
                with self.assertRaisesRegex(
                    ValueError, "EVALUATION_INVALID_OR_UNSAFE",
                ):
                    adapter.registry_adapter._validate_outcome_persistence_payload(
                        fixture.binding, payload,
                        selection_record_sha256=fixture.selection[
                            "selection_record_sha256"
                        ],
                        registry_row=fixture.registry,
                        selection_row=fixture.selection,
                        durable_fact_rows=[
                            item["fact"] for item in fixture.data["facts"]
                        ],
                    )

    def test_rehashed_server_owned_authority_flags_are_not_persistable(self):
        fixture = Fixture()
        result, _ = self.evaluate(fixture)
        for section, key in (
            ("fact_replay_receipt", "durable_selection_server_recomputed"),
            ("evaluation", "authoritative_fact_replay_verified"),
            ("evaluation", "durable_fact_source_authority_verified"),
            ("evaluation", "durable_outcome_atomic_gate_evidence_verified"),
        ):
            with self.subTest(section=section, key=key):
                payload = deepcopy(result["persistence_payload"])
                payload[section][key] = True
                replay = payload["fact_replay_receipt"]
                replay.pop("fact_replay_receipt_sha256")
                replay_sha = contract.digest(replay)
                replay["fact_replay_receipt_sha256"] = replay_sha
                payload["fact_replay_receipt_sha256"] = replay_sha
                payload["evaluation"]["fact_replay_receipt_sha256"] = replay_sha
                payload["evaluation_sha256"] = contract.digest(payload["evaluation"])
                payload.pop("persistence_payload_sha256")
                payload["persistence_payload_sha256"] = contract.digest(payload)
                with self.assertRaises(ValueError):
                    adapter.registry_adapter._validate_outcome_persistence_payload(
                        fixture.binding, payload,
                        selection_record_sha256=fixture.selection[
                            "selection_record_sha256"
                        ],
                        registry_row=fixture.registry,
                        selection_row=fixture.selection,
                        durable_fact_rows=[
                            item["fact"] for item in fixture.data["facts"]
                        ],
                    )

    def test_predicted_selection_identity_is_verified_then_normalized(self):
        fixture = Fixture()
        result, _ = self.evaluate(fixture)
        durable_by_event = {
            item["event_id"]: item
            for item in fixture.selection["representative_identities"]
        }
        for evidence in result["evidence_receipt"]["representatives"]:
            durable = durable_by_event[evidence["event_id"]]
            self.assertEqual(
                evidence["expected_selection_fact_identity_sha256"],
                durable["expected_selection_fact_identity_sha256"],
            )
            self.assertEqual(
                evidence["selection_fact_identity_sha256"],
                durable["expected_selection_fact_identity_sha256"],
            )
            # The durable pre-persistence identity and authoritative
            # post-database acceptance identity intentionally use different
            # closed field names and therefore different record digests.
            self.assertNotEqual(
                evidence["durable_representative_identity_sha256"],
                evidence["representative_identity_sha256"],
            )

    def test_causal_parent_replay_mismatch_is_unknown_and_hard_false(self):
        fixture = Fixture()
        ledger = fixture.replay_result["rows"][0]["fact_ledger"][0]
        ledger["parent_membership_evidence"]["parent_direction"] = "DOWN"
        parent_sha = contract.digest(ledger["parent_membership_evidence"])
        authority = ledger["fact_authority"]
        authority["expected_parent_membership_evidence_sha256"] = parent_sha
        authority.pop("fact_authority_sha256")
        authority["fact_authority_sha256"] = contract.digest(authority)
        fixture.replay_result.pop("result_sha256")
        fixture.replay_result["result_sha256"] = contract.digest(
            fixture.replay_result
        )
        result, _ = self.evaluate(fixture)
        self.assertFalse(result["research_qualified"])
        replay = result["fact_replay_receipt"]
        self.assertEqual(replay["status"], "UNKNOWN")
        self.assertFalse(replay["all_causal_fact_semantics_verified"])
        self.assertIn("AUTHORITATIVE_FACT_SOURCE_REPLAY_NOT_VERIFIED",
                      result["evaluation"]["qualification_blockers"])

    def test_transaction_audit_fields_do_not_change_causal_fact_semantics(self):
        fixture = Fixture(count=1)
        original = fixture.data["facts"][0]["fact"]["fact"]
        replayed = deepcopy(original)
        source = replayed["source"]
        attestation = source["watch_selection_attestation"]
        attestation["query_scope"]["database_snapshot_id"] = "later:snapshot:"
        unsigned_query = deepcopy(attestation["query_scope"])
        attestation["query_binding_sha256"] = contract.digest(unsigned_query)
        attestation["read_started_at_utc"] = "2026-09-13T03:00:00.000000Z"
        attestation.pop("attestation_sha256")
        attestation["attestation_sha256"] = contract.digest(attestation)
        source["watch_selection_query_binding_sha256"] = attestation[
            "query_binding_sha256"
        ]
        source["watch_selection_attestation_sha256"] = attestation[
            "attestation_sha256"
        ]
        replayed.pop("fact_sha256")
        replayed["fact_sha256"] = contract.digest(replayed)
        self.assertEqual(
            contract.digest(adapter._fact_semantic_projection(original)),
            contract.digest(adapter._fact_semantic_projection(replayed)),
        )

    def test_sql_is_bounded_select_only_and_reads_no_delivery_runtime(self):
        _, conn = self.evaluate(Fixture())
        forbidden = re.compile(
            r"\b(insert|update|delete|merge|alter|drop|truncate|copy|call)\b",
            re.I,
        )
        for sql, _ in conn.queries:
            self.assertIn("select", sql.lower())
            self.assertIsNone(forbidden.search(sql), sql)
            self.assertIsNone(
                re.search(r"\bcreate\s+(table|view|function|schema|role)\b", sql, re.I),
                sql,
            )
            self.assertNotIn("telegram", sql.lower())
            self.assertNotIn("outbox", sql.lower())
            self.assertNotIn("trade", sql.lower())

    def test_public_api_cannot_accept_caller_representatives_or_outcomes(self):
        signature = inspect.signature(
            adapter.evaluate_selection_outcomes_from_connection
        )
        self.assertEqual(list(signature.parameters), [
            "conn", "exact_binding", "selection_record_sha256",
        ])
        with self.assertRaises(TypeError):
            adapter.evaluate_selection_outcomes_from_connection(
                object(), {}, selection_record_sha256="a" * 64,
                representatives=[], outcomes=[],  # type: ignore[call-arg]
            )

    def test_probability_passes_when_asymmetry_is_unavailable(self):
        result, _ = self.evaluate(Fixture(include_metrics=False))
        evaluation = result["evaluation"]
        self.assertTrue(evaluation["atomic_gate_passed"], evaluation)
        self.assertEqual(evaluation["routes"]["PROBABILITY"]["status"], "PASS")
        self.assertEqual(evaluation["routes"]["ASYMMETRY"]["status"], "UNAVAILABLE")

    def test_asymmetry_passes_when_probability_is_open(self):
        result, _ = self.evaluate(Fixture(outcome_status="OPEN"))
        evaluation = result["evaluation"]
        self.assertTrue(evaluation["atomic_gate_passed"], evaluation)
        self.assertEqual(evaluation["routes"]["PROBABILITY"]["status"], "UNAVAILABLE")
        self.assertEqual(evaluation["routes"]["ASYMMETRY"]["status"], "PASS")
        self.assertEqual(evaluation["routes"]["PROBABILITY"]["failures"], 0)

    def test_four_distinct_parents_do_not_pass(self):
        result, _ = self.evaluate(Fixture(count=4))
        evaluation = result["evaluation"]
        self.assertFalse(evaluation["atomic_gate_passed"])
        self.assertEqual(evaluation["routes"]["PROBABILITY"]["status"], "INSUFFICIENT")
        self.assertEqual(evaluation["routes"]["ASYMMETRY"]["status"], "INSUFFICIENT")

    def test_duplicate_outcome_is_unknown_and_never_substituted(self):
        fixture = Fixture(include_metrics=False)
        fixture.data["outcomes"].append(deepcopy(fixture.data["outcomes"][0]))
        result, _ = self.evaluate(fixture)
        route = result["evaluation"]["routes"]["PROBABILITY"]
        self.assertEqual(route["distinct_parent_count"], 4)
        self.assertEqual(route["failures"], 0)
        self.assertFalse(route["passed"])
        evidence = result["evidence_receipt"]["representatives"]
        duplicate = next(item for item in evidence
                         if "ORDERED_V7_ROW_DUPLICATED" in item["probability"]["reasons"])
        self.assertEqual(duplicate["probability"]["validation_status"], "UNKNOWN")

    def test_wrong_threshold_is_missing_not_cross_cell_fallback(self):
        fixture = Fixture(include_metrics=False)
        fixture.data["outcomes"][0]["threshold_bps"] = 75
        result, conn = self.evaluate(fixture)
        route = result["evaluation"]["routes"]["PROBABILITY"]
        self.assertEqual(route["distinct_parent_count"], 4)
        probability_query = next(params for sql, params in conn.queries
                                 if "stage8-outcome:probability" in sql)
        self.assertEqual(probability_query["threshold_bps"], 50)

    def test_nondecisive_states_remain_unknown_not_failures(self):
        fixture = Fixture(count=5, include_metrics=False)
        statuses = ["SUCCESS", "OPEN", "UNRESOLVED", "DATA_MISSING", "FAILURE"]
        fixture.data["outcomes"] = [
            fixture._outcome(item["event"], status, 50)
            for item, status in zip(fixture.data["facts"], statuses)
        ]
        result, _ = self.evaluate(fixture)
        route = result["evaluation"]["routes"]["PROBABILITY"]
        self.assertEqual(route["distinct_parent_count"], 2)
        self.assertEqual(route["successes"], 1)
        self.assertEqual(route["failures"], 1)

    def test_common_window_route_mismatch_is_unknown(self):
        fixture = Fixture(include_outcomes=False)
        fixture.data["metrics"][0]["result"]["source"]["exchange"] = "hyperliquid"
        result, _ = self.evaluate(fixture)
        route = result["evaluation"]["routes"]["ASYMMETRY"]
        self.assertEqual(route["distinct_parent_count"], 4)
        self.assertFalse(route["passed"])

    def test_hype_requires_exact_spot_107_instrument(self):
        fixture = Fixture(symbol="HYPE", include_outcomes=False)
        good, _ = self.evaluate(fixture)
        self.assertTrue(good["evaluation"]["routes"]["ASYMMETRY"]["passed"], good)
        fixture.data["metrics"][0]["result"]["source"]["instrument"] = None
        bad, _ = self.evaluate(fixture)
        self.assertEqual(bad["evaluation"]["routes"]["ASYMMETRY"]
                         ["distinct_parent_count"], 4)

    def test_zero_mae_stays_unavailable_not_infinite(self):
        result, _ = self.evaluate(Fixture(
            include_outcomes=False, zero_mae=True,
        ))
        route = result["evaluation"]["routes"]["ASYMMETRY"]
        self.assertEqual(route["status"], "UNAVAILABLE")
        self.assertEqual(route["common_window_asymmetry_state"], "ZERO_DENOMINATOR")
        self.assertIsNone(route["common_window_asymmetry_ratio"])

    def test_fact_substitution_blocks_both_routes(self):
        fixture = Fixture()
        identity = fixture.selection["representative_identities"][0]
        original = next(item for item in fixture.data["facts"]
                        if item["fact"]["event_id"] == identity["event_id"])
        original["fact"]["fact_sha256"] = "f" * 64
        # Preserve the sealed outer record so this simulates a conflicting
        # returned row rather than manufacturing a new valid durable fact.
        with self.assertRaisesRegex(adapter.OutcomeAdapterError,
                                    "SEALED_FACT_POPULATION_INVALID"):
            self.evaluate(fixture)

    def test_read_only_repeatable_read_and_stable_snapshot_are_mandatory(self):
        fixture = Fixture()
        for kwargs in (
            {"read_only": "off"}, {"isolation": "read committed"},
            {"timeout": "0"}, {"timeout": "20s"},
            {"change_transaction": True},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(adapter.OutcomeAdapterError):
                self.evaluate(fixture, **kwargs)
        conn = FakeConnection(fixture.data)
        conn.autocommit = True
        with self.assertRaises(adapter.OutcomeAdapterError):
            adapter.evaluate_selection_outcomes_from_connection(
                conn, fixture.binding,
                selection_record_sha256=fixture.selection["selection_record_sha256"],
            )

    def test_reader_role_and_shadow_free_schema_are_mandatory(self):
        fixture = Fixture()
        cases = (
            {"database_role": "privileged_application"},
            {"session_role": "privileged_application"},
            {"database_timezone": "America/New_York"},
            {"effective_schemas": ["pg_temp_9", "stage8_fixture", "pg_catalog"]},
            {"trusted_schema_create_allowed": True},
            {"trusted_relations_resolved": False},
        )
        for override in cases:
            conn = FakeConnection(fixture.data)
            original_execute = conn.execute

            def execute(sql, params=None, *, override=override):
                result = original_execute(sql, params)
                if "stage8-outcome:transaction" not in sql:
                    return result
                rows = result.fetchall()
                rows[0].update(override)
                return _Result(rows)

            conn.execute = execute
            with self.subTest(override=override), self.assertRaisesRegex(
                adapter.OutcomeAdapterError,
                "REQUIRES_READ_ONLY_REPEATABLE_READ_TRANSACTION",
            ):
                adapter.evaluate_selection_outcomes_from_connection(
                    conn, fixture.binding,
                    selection_record_sha256=fixture.selection[
                        "selection_record_sha256"
                    ],
                )

    def test_source_manifest_drift_during_read_fails_closed(self):
        fixture = Fixture()
        first_manifest, first_sha = adapter._source_manifest()
        changed_manifest = deepcopy(first_manifest)
        changed_manifest["files"]["research_stage8_acceptance.py"] = "f" * 64
        changed = (changed_manifest, contract.digest(changed_manifest))
        conn = FakeConnection(fixture.data)
        replay = mock.Mock(return_value=deepcopy(fixture.replay_result))
        with mock.patch.object(adapter, "_source_manifest",
                               side_effect=[(first_manifest, first_sha), changed]), \
             mock.patch.object(adapter, "_PROJECT_EXACT_BINDING", replay), \
             mock.patch.object(
                 adapter.projection_db_adapter,
                 "project_exact_binding_attempts_from_connection", replay,
             ), self.assertRaisesRegex(
                 adapter.OutcomeAdapterError,
                 "SOURCE_CODE_CHANGED_DURING_READ",
             ):
            adapter.evaluate_selection_outcomes_from_connection(
                conn, fixture.binding,
                selection_record_sha256=fixture.selection[
                    "selection_record_sha256"
                ],
            )

    def test_selection_hash_and_binding_cannot_be_swapped(self):
        fixture = Fixture()
        other = contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="POSITIONING_ALIGNED65_SHORT",
            threshold_bps=50,
        )
        with self.assertRaises(LookupError):
            adapter.evaluate_selection_outcomes_from_connection(
                FakeConnection(fixture.data), other,
                selection_record_sha256=fixture.selection["selection_record_sha256"],
            )
        with self.assertRaises(adapter.OutcomeAdapterError):
            adapter.evaluate_selection_outcomes_from_connection(
                FakeConnection(fixture.data), fixture.binding,
                selection_record_sha256="caller-fabricated",
            )

    def test_receipt_hash_is_deterministic_and_source_sensitive(self):
        fixture = Fixture()
        first, _ = self.evaluate(fixture)
        second, _ = self.evaluate(fixture)
        self.assertEqual(first["result_sha256"], second["result_sha256"])
        changed = Fixture()
        changed.data["outcomes"][0] = changed._outcome(
            changed.data["facts"][0]["event"], "FAILURE", 50,
        )
        third, _ = self.evaluate(changed)
        self.assertNotEqual(first["evidence_receipt_sha256"],
                            third["evidence_receipt_sha256"])

    def test_projection_batch_receipt_self_hashes_exclude_hash_field(self):
        fixture = Fixture()
        for receipt_key, column_key, hash_key in (
            ("adapter_population_receipt",
             "adapter_population_receipt_sha256",
             "population_receipt_sha256"),
            ("adapter_authority_receipt",
             "adapter_authority_receipt_sha256",
             "authority_receipt_sha256"),
        ):
            receipt = fixture.batch[receipt_key]
            stored = fixture.batch[column_key]
            self.assertTrue(adapter._embedded_receipt_hash_matches(
                receipt, hash_key=hash_key, stored_sha256=stored,
            ))
            self.assertNotEqual(contract.digest(receipt), stored)

            corrupted = Fixture()
            corrupted.data["batch"][receipt_key][hash_key] = "f" * 64
            with self.subTest(receipt=receipt_key), self.assertRaisesRegex(
                adapter.OutcomeAdapterError,
                "FACT_BATCH_OR_SEAL_INVALID",
            ):
                self.evaluate(corrupted)

    def test_server_watch_archive_high_water_is_nullable_positive_int64_audit(self):
        fixture = Fixture(count=1)
        for high_water in (None, 1, 999, 2 ** 63 - 1, True, False,
                           0, -1, 2 ** 63, 1.0, "999"):
            with self.subTest(high_water=high_water):
                batch = deepcopy(fixture.batch)
                seal = deepcopy(fixture.seal)
                selection = deepcopy(fixture.selection)
                key = "watch_archive_high_water_snapshot_set_id"
                batch[key] = batch["fact_batch_record"][key] = high_water
                batch_sha = contract.digest(batch["fact_batch_record"])
                batch["fact_batch_record_sha256"] = batch_sha
                selection["fact_batch_record_sha256"] = batch_sha
                seal["fact_batch_record_sha256"] = batch_sha
                seal["seal_record"]["fact_batch_record_sha256"] = batch_sha
                seal["seal_record_sha256"] = contract.digest(seal["seal_record"])
                if high_water is None or (
                        type(high_water) is int and 0 < high_water < 2 ** 63):
                    self.assertEqual(adapter._validate_batch_and_seal(
                        batch, seal, fixture.registry, selection,
                    ), [1])
                else:
                    with self.assertRaisesRegex(adapter.OutcomeAdapterError,
                                                "FACT_BATCH_OR_SEAL_INVALID"):
                        adapter._validate_batch_and_seal(
                            batch, seal, fixture.registry, selection,
                        )

    def test_server_projection_record_audit_fields_are_closed_and_hash_bound(self):
        fixture = Fixture(count=1)
        original = fixture.data["facts"][0]["fact"]
        self.assertTrue(adapter._fact_record_valid(
            original, fixture.batch["fact_batch_record_sha256"], fixture.binding,
        ))
        for key, value in (
            ("server_projection_attestation_sha256", None),
            ("server_projection_attestation_sha256", "not-a-hash"),
            ("server_projection_status", None),
            ("server_projection_status", "CALLER_VERIFIED"),
        ):
            with self.subTest(key=key, value=value):
                row = deepcopy(original)
                row["fact_record"][key] = value
                row["fact_record_sha256"] = contract.digest(row["fact_record"])
                self.assertFalse(adapter._fact_record_valid(
                    row, fixture.batch["fact_batch_record_sha256"], fixture.binding,
                ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
