"""Bounded same-snapshot outcome reader for one durable Stage-8 selection.

The only authority inputs are a caller-owned PostgreSQL connection, one exact
frozen binding, and the SHA-256 identity of a durable selection record.  The
adapter reconstructs representatives from the append-only selection and fact
ledgers; it never accepts representatives, labels, excursions, source hashes,
or a caller-authored receipt.

The connection must already be in a read-only REPEATABLE READ transaction.
This module does not open connections, inspect environment variables, change
transaction state, write receipts, send Telegram messages, authorize LIVE, or
trade.  It replays the complete sealed fact population from the original
database authorities in that same snapshot, but its replay and atomic outcome
gate are caller-side diagnostics only.  ``research_qualified`` remains false;
only the persisted database-trigger result can establish experimental research
qualification.  Every delivery and trading authorization remains hard-false.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import time
from typing import Any, Mapping, Sequence

import canonical_price_path
import research_common_window_metrics as common_window
import research_operational_score_source_audit as source_audit
import research_stage8_acceptance as acceptance
import research_stage8_contract as contract
import research_stage8_feature_projection as projection
import research_stage8_projection_db_adapter as projection_db_adapter
import research_stage8_registry as registry_adapter
import research_stage8_representative_selector as selector


VERSION = "stage8-durable-outcome-db-adapter-v1"
EVIDENCE_RECEIPT_VERSION = "stage8-durable-outcome-evidence-receipt-v1"
# Keep the original wire identities for the frozen closed request schema.
# These names are not server attestations: the entire request is caller-owned.
FACT_REPLAY_RECEIPT_VERSION = "stage8-authoritative-fact-replay-receipt-v1"
PERSISTENCE_PAYLOAD_VERSION = (
    "stage8-authoritative-outcome-evaluation-persistence-v1"
)
SOURCE_MANIFEST_VERSION = "stage8-outcome-reader-source-manifest-v1"
RESULT_SCOPE = "EXPERIMENTAL_RESEARCH_ONLY"
MAX_REPRESENTATIVES = 1_000
MAX_FACTS = 1_000
OUTCOME_QUERY_COUNT = 7
PROJECTION_REPLAY_QUERY_COUNT = projection_db_adapter.MAX_QUERIES
MAX_QUERIES = OUTCOME_QUERY_COUNT + PROJECTION_REPLAY_QUERY_COUNT
MAX_WALL_SECONDS = 30.0
MAX_STATEMENT_TIMEOUT_MS = 10_000.0
_INT64_MAX = 9_223_372_036_854_775_807
_HASH = re.compile(r"[0-9a-f]{64}\Z")

_STARTUP_MANIFEST = contract.frozen_manifest()
contract.validate_manifest(_STARTUP_MANIFEST)
_MANIFEST_SHA256 = contract.MANIFEST_SHA256
_ACCEPTANCE_VERSION = acceptance.VERSION
_REGISTRY_VERSION = registry_adapter.VERSION
_SELECTOR_VERSION = selector.VERSION
_PROJECTION_VERSION = projection.VERSION
_PROJECTION_ADAPTER_VERSION = projection_db_adapter.VERSION
_SOURCE_AUDIT_VERSION = source_audit.VERSION
_COMMON_WINDOW_VERSION = common_window.METHOD_VERSION
_PROJECT_EXACT_BINDING = (
    projection_db_adapter.project_exact_binding_attempts_from_connection
)

_REGISTRY_VIEW_KEYS = frozenset({
    "exact_binding", "exact_binding_sha256", "manifest_sha256",
    "contract_version", "hash_version", "source_version",
    "source_audit_version", "projection_version", "candidate_version",
    "label_version", "independence_version", "acceptance_version",
    "parent_policy_version", "scope_id", "candidate_id", "window_minutes",
    "threshold_bps", "implementation_artifacts",
    "implementation_artifacts_sha256", "expected_watch_code_manifest",
    "expected_watch_code_manifest_sha256", "verifier_profile",
    "verifier_profile_sha256", "frozen_at_utc", "freeze_id",
    "registry_record", "registry_record_sha256",
})
_SELECTION_VIEW_KEYS = frozenset({
    "selection_record_sha256", "fact_batch_record_sha256",
    "exact_binding_sha256", "freeze_id", "registry_record_sha256",
    "verifier_profile_sha256", "registry_verification_receipt_sha256",
    "selector_version", "observed_projection_source_sha256",
    "observed_selector_source_sha256",
    "observed_watch_code_manifest_sha256", "cohort_query_sha256",
    "outcome_free_population_receipt_sha256",
    "source_high_water_attempt_id", "representative_count",
    "representative_set_sha256", "representative_identities",
    "representative_identities_sha256", "selection_attestation",
    "selection_attestation_sha256", "persisted_at_utc", "selection_record",
})
_SELECTION_RECORD_KEYS = frozenset({
    "version", "fact_batch_record_sha256", "exact_binding_sha256",
    "freeze_id", "registry_record_sha256", "verifier_profile_sha256",
    "registry_verification_receipt_sha256", "selector_version",
    "observed_projection_source_sha256", "observed_selector_source_sha256",
    "observed_watch_code_manifest_sha256", "cohort_query_sha256",
    "outcome_free_population_receipt_sha256",
    "source_high_water_attempt_id", "representative_count",
    "representative_set_sha256", "representative_identities_sha256",
    "selection_attestation_sha256", "persisted_at_utc", "persisted_by",
})
_SELECTION_ATTESTATION_KEYS = frozenset({
    "version", "selector_version", "structural_authority", "status",
    "exact_binding_sha256", "manifest_sha256", "freeze_id", "frozen_at_utc",
    "registry_record_sha256", "registry_verification_receipt_sha256",
    "verifier_profile_sha256", "expected_projection_source_sha256",
    "expected_selector_source_sha256", "expected_watch_code_manifest_sha256",
    "cohort_query_sha256", "outcome_free_population_receipt_sha256",
    "attempt_population_sha256", "source_transaction_identity_sha256",
    "source_high_water_attempt_id", "source_attempt_count",
    "source_authority_ledger_count", "source_authority_ledger_sha256",
    "representative_count", "representative_set_sha256",
    "population_coverage_complete", "candidate_match_coverage_complete",
    "outcome_blind_selection", "outcome_or_label_fields_accepted",
    "truncated", "global_blockers", "blocked_parents",
    "excluded_pre_freeze_parent_ids", "proven_noneligible_attempt_ids",
    "deduplicated_exact_anchor_event_count", "qualification_evaluated",
    "database_verification_asserted_by_selector",
})
_REPRESENTATIVE_IDENTITY_KEYS = frozenset({
    "version", "exact_binding_sha256", "btc_parent_movement_id",
    "parent_start_time_utc", "expected_selection_fact_identity_sha256",
    "attempt_fingerprint", "anchor_slot_id", "event_id", "event_fingerprint",
    "symbol", "direction", "decision_time_utc",
    "candidate_match_knowledge_status", "candidate_match",
})
_FACT_BATCH_VIEW_KEYS = frozenset({
    "fact_batch_record_sha256", "exact_binding_sha256", "freeze_id",
    "registry_record_sha256", "verifier_profile_sha256",
    "registry_verification_receipt_sha256", "projection_adapter_version",
    "observed_projection_source_sha256",
    "observed_projection_adapter_source_sha256",
    "observed_registry_adapter_source_sha256",
    "observed_registry_migration_sha256", "projection_source_manifest",
    "projection_source_manifest_sha256", "adapter_query_binding_sha256",
    "adapter_population_receipt", "adapter_population_receipt_sha256",
    "adapter_authority_receipt", "adapter_authority_receipt_sha256",
    "adapter_result_sha256", "coverage_query_scope", "coverage_query_sha256",
    "outcome_free_population_receipt_sha256",
    "coverage_attempt_population_sha256",
    "coverage_source_high_water_attempt_id", "attempt_ids", "attempt_count",
    "watch_archive_high_water_snapshot_set_id",
    "persisted_at_utc", "fact_batch_record",
})
_FACT_BATCH_RECORD_KEYS = frozenset({
    "version", "exact_binding_sha256", "freeze_id", "registry_record_sha256",
    "verifier_profile_sha256", "registry_verification_receipt_sha256",
    "projection_adapter_version", "observed_projection_source_sha256",
    "observed_projection_adapter_source_sha256",
    "observed_registry_adapter_source_sha256",
    "observed_registry_migration_sha256", "projection_source_manifest_sha256",
    "adapter_query_binding_sha256", "adapter_population_receipt_sha256",
    "adapter_authority_receipt_sha256", "adapter_result_sha256",
    "coverage_query_scope", "coverage_query_sha256",
    "outcome_free_population_receipt_sha256",
    "coverage_attempt_population_sha256",
    "coverage_source_high_water_attempt_id", "attempt_ids", "attempt_count",
    "watch_archive_high_water_snapshot_set_id",
    "persisted_at_utc", "persisted_by",
})
_FACT_SEAL_VIEW_KEYS = frozenset({
    "fact_batch_record_sha256", "fact_count", "fact_records_sha256",
    "sealed_at_utc", "seal_record", "seal_record_sha256",
})
_FACT_SEAL_RECORD_KEYS = frozenset({
    "version", "fact_batch_record_sha256", "fact_count",
    "fact_records_sha256", "sealed_at_utc", "sealed_by",
})
_FACT_VIEW_KEYS = frozenset({
    "fact_record_sha256", "fact_batch_record_sha256", "exact_binding_sha256",
    "attempt_id", "attempt_fingerprint", "anchor_slot_id", "event_id",
    "event_fingerprint", "symbol", "direction", "decision_time_utc",
    "knowledge_status", "candidate_match", "fact", "fact_sha256",
    "fact_authority", "fact_authority_sha256",
    "watch_selection_attestation", "watch_selection_attestation_sha256",
    "observed_watch_code_manifest_sha256", "parent_membership_evidence",
    "parent_membership_evidence_sha256", "noneligibility_proof",
    "noneligibility_proof_sha256", "selection_fact_identity",
    "selection_fact_identity_sha256", "persisted_at_utc", "fact_record",
})
_FACT_RECORD_KEYS = frozenset({
    "version", "fact_batch_record_sha256", "exact_binding_sha256",
    "attempt_id", "attempt_fingerprint", "anchor_slot_id", "event_id",
    "event_fingerprint", "symbol", "direction", "decision_time_utc",
    "knowledge_status", "candidate_match", "fact_sha256",
    "fact_authority_sha256", "watch_selection_attestation_sha256",
    "observed_watch_code_manifest_sha256", "parent_membership_evidence_sha256",
    "noneligibility_proof_sha256", "selection_fact_identity_sha256",
    "server_projection_attestation_sha256", "server_projection_status",
    "persisted_at_utc", "persisted_by",
})
_SELECTION_FACT_IDENTITY_KEYS = frozenset({
    "version", "exact_binding_sha256", "attempt_id", "attempt_fingerprint",
    "sampler_version", "source_candle_open_utc",
    "source_attempt_evaluation_status", "anchor_slot_id", "event_id",
    "event_fingerprint", "symbol", "direction", "decision_time_utc",
    "knowledge_status", "candidate_match", "parent_authority_class",
    "btc_parent_movement_id", "parent_start_time_utc",
    "parent_policy_version", "membership_status",
    "parent_evidence_eligible",
})
_EVENT_KEYS = frozenset({
    "event_id", "schema_version", "event_kind", "event_type", "alert_time_utc",
    "symbol", "direction", "source_side", "timeframe", "score",
    "current_price", "target_price", "initial_target_distance_pct",
    "categories", "setup_key", "event_fingerprint", "strategy_version",
    "code_version", "runtime_session_id", "capture_stage", "delivery_status",
    "delivery_attempted_at_utc", "delivered_at_utc", "engine_snapshot",
    "created_at",
})
_OUTCOME_KEYS = frozenset({
    "event_id", "window_minutes", "threshold_bps", "method_version",
    "direction", "status", "first_touch_side", "terminal_reason", "success",
    "measurement_start_utc", "first_observed_open_utc",
    "observed_through_utc", "decision_time_utc", "time_to_decision_seconds",
    "initial_gap_seconds", "initial_gap_unobserved", "reference_price",
    "favorable_barrier_price", "adverse_barrier_price",
    "favorable_touch_price", "adverse_touch_price", "max_favorable_price",
    "max_adverse_price", "mfe_pct", "mae_pct", "candle_interval_seconds",
    "path_samples", "observation_closed", "input_path_complete",
    "path_complete", "price_source", "market_pair", "data_quality_status",
    "data_quality_note", "threshold_policy", "calculation_audit",
    "created_at_utc", "updated_at_utc",
})
_COMMON_WINDOW_ROW_KEYS = frozenset({
    "event_id", "window_minutes", "method_version", "status",
    "measurement_start_utc", "window_end_utc", "next_attempt_at_utc", "result",
    "created_at_utc", "updated_at_utc",
})
_COMMON_RESULT_BASE_KEYS = frozenset({
    "method_version", "measurement_kind", "window_minutes", "symbol",
    "direction", "measurement_start_utc", "window_end_utc",
    "observed_from_utc", "observed_through_utc", "observed_at_utc",
    "reference_price", "status", "observation_closed", "path_complete",
    "observed_prefix_complete", "expected_candles", "path_samples",
    "initial_gap_seconds", "trailing_partial_minute_seconds", "boundary_policy",
    "candle_interval_seconds", "source", "data_quality_status",
    "missing_reason", "mfe_pct", "mae_pct", "asymmetry_ratio",
    "asymmetry_method", "asymmetry_status", "path_sha256",
})
_COMMON_RESULT_READY_KEYS = _COMMON_RESULT_BASE_KEYS | {
    "max_favorable_price", "max_adverse_price",
}

_TRANSACTION_SQL = """/* stage8-outcome:transaction */ SELECT
    pg_catalog.current_setting('transaction_read_only') AS read_only,
    pg_catalog.current_setting('transaction_isolation') AS isolation,
    pg_catalog.current_setting('statement_timeout') AS statement_timeout,
    pg_catalog.current_setting('TimeZone') AS database_timezone,
    pg_catalog.pg_backend_pid() AS backend_pid,
    pg_catalog.transaction_timestamp() AS transaction_started_at_utc,
    pg_catalog.pg_current_snapshot()::text AS transaction_snapshot,
    current_user AS database_role,
    session_user AS session_role,
    pg_catalog.current_schema() AS trusted_schema,
    pg_catalog.current_schemas(true) AS effective_schemas,
    pg_catalog.has_schema_privilege(
        current_user, pg_catalog.current_schema(), 'CREATE'
    ) AS trusted_schema_create_allowed,
    (SELECT pg_catalog.count(*) = 14
            AND pg_catalog.bool_and(
                namespace.nspname = pg_catalog.current_schema()
            )
       FROM pg_catalog.unnest(ARRAY[
            'research_stage8_registry_read_v1',
            'research_stage8_selection_read_v1',
            'research_stage8_fact_batch_read_v1',
            'research_stage8_fact_read_v1',
            'research_stage8_fact_seal_read_v1',
            'research_events',
            'research_ordered_first_touch_outcomes',
            'research_common_window_metrics',
            'research_max_pain_snapshot_sets',
            'research_prospective_anchor_attempts',
            'research_prospective_anchor_slots',
            'research_event_btc_movements',
            'research_btc_parent_movements',
            'research_btc_price_bars'
       ]::text[]) AS relation(name)
       JOIN pg_catalog.pg_class AS class
         ON class.oid = pg_catalog.to_regclass(relation.name)
       JOIN pg_catalog.pg_namespace AS namespace
         ON namespace.oid = class.relnamespace
    ) AS trusted_relations_resolved,
    pg_catalog.clock_timestamp() AS observed_at_utc"""

_REGISTRY_SELECTION_SQL = """/* stage8-outcome:registry-selection */
    SELECT to_jsonb(r)::text AS adapter_registry_json,
           to_jsonb(s)::text AS adapter_selection_json
    FROM research_stage8_selection_read_v1 AS s
    JOIN research_stage8_registry_read_v1 AS r
      ON r.exact_binding_sha256=s.exact_binding_sha256
    WHERE s.selection_record_sha256=%(selection_record_sha256)s
      AND s.exact_binding_sha256=%(exact_binding_sha256)s
    ORDER BY s.selection_record_sha256
    LIMIT 2"""

_BATCH_SEAL_SQL = """/* stage8-outcome:batch-seal */
    SELECT to_jsonb(b)::text AS adapter_batch_json,
           to_jsonb(z)::text AS adapter_seal_json
    FROM research_stage8_fact_batch_read_v1 AS b
    JOIN research_stage8_fact_seal_read_v1 AS z
      USING (fact_batch_record_sha256)
    WHERE b.fact_batch_record_sha256=%(fact_batch_record_sha256)s
      AND b.exact_binding_sha256=%(exact_binding_sha256)s
    ORDER BY b.fact_batch_record_sha256
    LIMIT 2"""

_FACTS_SQL = """/* stage8-outcome:facts */
    SELECT to_jsonb(f)::text AS adapter_fact_json,
           CASE WHEN e.event_id IS NULL THEN NULL ELSE to_jsonb(e)::text END
             AS adapter_event_json
    FROM research_stage8_fact_read_v1 AS f
    LEFT JOIN research_events AS e ON e.event_id=f.event_id
    WHERE f.fact_batch_record_sha256=%(fact_batch_record_sha256)s
    ORDER BY f.attempt_id
    LIMIT %(limit)s"""

_OUTCOMES_SQL = """/* stage8-outcome:probability */
    SELECT to_jsonb(o)::text AS adapter_outcome_json
    FROM research_ordered_first_touch_outcomes AS o
    WHERE o.event_id=ANY(%(event_ids)s::bigint[])
      AND o.window_minutes=%(window_minutes)s
      AND o.threshold_bps=%(threshold_bps)s
      AND o.method_version=%(method_version)s
    ORDER BY o.event_id
    LIMIT %(limit)s"""

_COMMON_METRICS_SQL = """/* stage8-outcome:asymmetry */
    SELECT to_jsonb(m)::text AS adapter_metric_json
    FROM research_common_window_metrics AS m
    WHERE m.event_id=ANY(%(event_ids)s::bigint[])
      AND m.window_minutes=%(window_minutes)s
      AND m.method_version=%(method_version)s
    ORDER BY m.event_id
    LIMIT %(limit)s"""


class OutcomeAdapterError(ValueError):
    """Stable fail-closed boundary for invalid inputs or durable envelopes."""


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH.fullmatch(value) is not None


def _positive_int64(value: Any) -> bool:
    return type(value) is int and 0 < value <= _INT64_MAX


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _same_time(left: Any, right: Any) -> bool:
    try:
        return _utc(left) == _utc(right)
    except (TypeError, ValueError, OverflowError):
        return False


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _json_safe(value: Any) -> Any:
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("nonfinite database value")
        return 0.0 if value == 0.0 else value
    if isinstance(value, Decimal):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("nonfinite database value")
        return 0.0 if number == 0.0 else number
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError("database mapping keys must be strings")
        return {key: _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(child) for child in value]
    raise ValueError("database value is not strict JSON")


def _decoded(value: Any, *, reason: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value) if isinstance(value, str) else deepcopy(value)
        safe = _json_safe(parsed)
    except (TypeError, ValueError, json.JSONDecodeError, OverflowError) as exc:
        raise OutcomeAdapterError(reason) from exc
    if type(safe) is not dict:
        raise OutcomeAdapterError(reason)
    return safe


def _strict(value: Any, keys: frozenset[str], reason: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        raise OutcomeAdapterError(reason)
    return value


def _raw_hash(value: Any) -> str:
    return contract.digest(_json_safe(value))


def _embedded_receipt_hash_matches(
    receipt: Any, *, hash_key: str, stored_sha256: Any,
) -> bool:
    """Validate a receipt whose digest excludes its own hash field."""
    if type(receipt) is not dict or not _valid_hash(stored_sha256):
        return False
    unsigned = deepcopy(receipt)
    supplied = unsigned.pop(hash_key, None)
    return bool(
        supplied == stored_sha256
        and _valid_hash(supplied)
        and contract.digest(unsigned) == supplied
    )


def _timeout_ms(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("invalid statement timeout")
    if isinstance(value, (int, float)):
        amount, unit = float(value), "ms"
    else:
        match = re.fullmatch(
            r"\s*([0-9]+(?:\.[0-9]+)?)\s*(ms|s|min|h)?\s*", str(value), re.I
        )
        if match is None:
            raise ValueError("invalid statement timeout")
        amount, unit = float(match.group(1)), (match.group(2) or "ms").lower()
    timeout = amount * {"ms": 1.0, "s": 1_000.0,
                        "min": 60_000.0, "h": 3_600_000.0}[unit]
    if not math.isfinite(timeout):
        raise ValueError("invalid statement timeout")
    return timeout


def _assert_runtime_contract() -> None:
    try:
        manifest = contract.frozen_manifest()
        contract.validate_manifest(manifest)
        if (contract.MANIFEST_SHA256 != _MANIFEST_SHA256
                or contract.digest(manifest) != _MANIFEST_SHA256
                or acceptance.VERSION != _ACCEPTANCE_VERSION
                or registry_adapter.VERSION != _REGISTRY_VERSION
                or selector.VERSION != _SELECTOR_VERSION
                or projection.VERSION != _PROJECTION_VERSION
                or projection_db_adapter.VERSION != _PROJECTION_ADAPTER_VERSION
                or projection_db_adapter.project_exact_binding_attempts_from_connection
                is not _PROJECT_EXACT_BINDING
                or source_audit.VERSION != _SOURCE_AUDIT_VERSION
                or common_window.METHOD_VERSION != _COMMON_WINDOW_VERSION
                or manifest["labels"]["method_version"] != "ordered-first-touch-v7"
                or manifest["labels"]["asymmetry_source_version"]
                != _COMMON_WINDOW_VERSION):
            raise ValueError("version mismatch")
    except (KeyError, TypeError, ValueError) as exc:
        raise OutcomeAdapterError("STAGE8_OUTCOME_RUNTIME_CONTRACT_MISMATCH") from exc


def _module_source_path(module: Any) -> Path:
    path = Path(str(module.__file__)).resolve()
    if path.suffix in {".pyc", ".pyo"} and path.with_suffix(".py").is_file():
        path = path.with_suffix(".py")
    if not path.is_file():
        raise OutcomeAdapterError("STAGE8_OUTCOME_SOURCE_CODE_UNAVAILABLE")
    return path


def _source_manifest() -> tuple[dict[str, Any], str]:
    paths = {
        "canonical_price_path.py": _module_source_path(canonical_price_path),
        "research_common_window_metrics.py": _module_source_path(common_window),
        "research_operational_score_source_audit.py": _module_source_path(source_audit),
        "research_stage8_acceptance.py": _module_source_path(acceptance),
        "research_stage8_contract.py": _module_source_path(contract),
        "research_stage8_feature_projection.py": _module_source_path(projection),
        "research_stage8_projection_db_adapter.py":
            _module_source_path(projection_db_adapter),
        "research_stage8_outcome_db_adapter.py": Path(__file__).resolve(),
        "research_stage8_registry.py": _module_source_path(registry_adapter),
        "research_stage8_representative_selector.py": _module_source_path(selector),
    }
    files = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in sorted(paths.items())
    }
    if any(not _valid_hash(value) for value in files.values()):
        raise OutcomeAdapterError("STAGE8_OUTCOME_SOURCE_CODE_UNAVAILABLE")
    value = {"version": SOURCE_MANIFEST_VERSION, "files": files}
    return value, contract.digest(value)


class _Reader:
    def __init__(self, conn: Any, *, started: float):
        self.conn = conn
        self.started = started
        self.query_count = 0

    def rows(self, statement: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        if time.monotonic() - self.started > MAX_WALL_SECONDS:
            raise OutcomeAdapterError("STAGE8_OUTCOME_READ_WALL_BOUND_EXCEEDED")
        self.query_count += 1
        if self.query_count > MAX_QUERIES:
            raise OutcomeAdapterError("STAGE8_OUTCOME_QUERY_BOUND_EXCEEDED")
        raw = self.conn.execute(statement, params or {}).fetchall()
        if any(not isinstance(row, Mapping) for row in raw):
            raise OutcomeAdapterError("STAGE8_OUTCOME_REQUIRES_MAPPING_ROWS")
        return [dict(row) for row in raw]


def _transaction(row: Mapping[str, Any], conn: Any) -> dict[str, Any]:
    try:
        timeout = _timeout_ms(row.get("statement_timeout"))
        read_only = row.get("read_only") in (True, "on", "true")
        isolation = str(row.get("isolation") or "").lower().replace("_", " ")
        role = row.get("database_role")
        session_role = row.get("session_role")
        database_timezone = row.get("database_timezone")
        trusted_schema = row.get("trusted_schema")
        effective_schemas = row.get("effective_schemas")
        snapshot = row.get("transaction_snapshot")
        if (not read_only or isolation != "repeatable read"
                or getattr(conn, "autocommit", None) is not False
                or not 0 < timeout <= MAX_STATEMENT_TIMEOUT_MS
                or not _positive_int64(row.get("backend_pid"))
                or role != registry_adapter.READER_ROLE
                or session_role != registry_adapter.READER_ROLE
                or database_timezone != "UTC"
                or not isinstance(trusted_schema, str) or not trusted_schema
                or not isinstance(effective_schemas, (list, tuple))
                or list(effective_schemas[:2]) != [trusted_schema, "pg_catalog"]
                or any(not isinstance(item, str)
                       or not item.startswith("pg_temp_")
                       for item in effective_schemas[2:])
                or row.get("trusted_schema_create_allowed") is not False
                or row.get("trusted_relations_resolved") is not True
                or not isinstance(snapshot, str) or not snapshot.strip()):
            raise ValueError("transaction mismatch")
        shared = source_audit.transaction_identity_from_fields(
            backend_pid=row["backend_pid"],
            transaction_started_at_utc=row["transaction_started_at_utc"],
            database_snapshot_id=snapshot,
        )
        value = {
            "backend_pid": shared["backend_pid"],
            "database_role": role,
            "session_role": session_role,
            "database_timezone": database_timezone,
            "trusted_schema": trusted_schema,
            "effective_schemas": list(effective_schemas),
            "transaction_started_at_utc": shared["transaction_started_at_utc"],
            "database_snapshot_id": shared["database_snapshot_id"],
            "read_only": True,
            "isolation": "repeatable read",
            "statement_timeout_ms": timeout,
        }
        value["transaction_identity_sha256"] = shared[
            "transaction_identity_sha256"
        ]
        value["observed_at_utc"] = _iso(row["observed_at_utc"])
        return value
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OutcomeAdapterError(
            "STAGE8_OUTCOME_REQUIRES_READ_ONLY_REPEATABLE_READ_TRANSACTION"
        ) from exc


def _stable_transaction(first: Mapping[str, Any], last: Mapping[str, Any]) -> bool:
    keys = (
        "backend_pid", "database_role", "session_role", "database_timezone",
        "trusted_schema", "effective_schemas", "transaction_started_at_utc",
        "database_snapshot_id", "read_only", "isolation", "statement_timeout_ms",
        "transaction_identity_sha256",
    )
    try:
        return (all(first.get(key) == last.get(key) for key in keys)
                and _utc(last["observed_at_utc"]) >= _utc(first["observed_at_utc"]))
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def _selection_envelope(
    registry: Mapping[str, Any], selection: Mapping[str, Any],
    exact_binding: Mapping[str, Any], selection_record_sha256: str,
) -> list[dict[str, Any]]:
    _strict(dict(registry), _REGISTRY_VIEW_KEYS, "STAGE8_DURABLE_REGISTRY_SHAPE_INVALID")
    _strict(dict(selection), _SELECTION_VIEW_KEYS, "STAGE8_DURABLE_SELECTION_SHAPE_INVALID")
    try:
        verified_registry = registry_adapter._validate_registry_row(
            registry, exact_binding, require_current_implementation=True,
        )
        # PostgreSQL's to_jsonb timestamp spelling may use ``+00:00`` while
        # the frozen Stage-8 codec uses fixed-microsecond ``Z``.  Retain the
        # same instant but use the contract codec for downstream identities.
        if isinstance(registry, dict):
            registry.update(verified_registry)
        record = _strict(
            dict(selection["selection_record"]), _SELECTION_RECORD_KEYS,
            "STAGE8_DURABLE_SELECTION_RECORD_SHAPE_INVALID",
        )
        batch = _strict(
            dict(selection["selection_attestation"]), _SELECTION_ATTESTATION_KEYS,
            "STAGE8_DURABLE_SELECTION_ATTESTATION_SHAPE_INVALID",
        )
        identities = selection["representative_identities"]
        if (not isinstance(identities, list)
                or len(identities) > MAX_REPRESENTATIVES
                or any(type(item) is not dict or set(item) != _REPRESENTATIVE_IDENTITY_KEYS
                       for item in identities)):
            raise ValueError("representative identity shape")
        ordered = sorted(deepcopy(identities), key=contract.canonical)
        parent_ids = [item.get("btc_parent_movement_id") for item in ordered]
        event_ids = [item.get("event_id") for item in ordered]
        selection_fact_hashes = [
            item.get("expected_selection_fact_identity_sha256")
            for item in ordered
        ]
        if (identities != ordered or len(set(parent_ids)) != len(parent_ids)
                or len(set(event_ids)) != len(event_ids)
                or len(set(selection_fact_hashes)) != len(selection_fact_hashes)
                or any(not _valid_hash(item.get("btc_parent_movement_id"))
                       or not _valid_hash(
                           item.get("expected_selection_fact_identity_sha256")
                       )
                       or not _valid_hash(item.get("attempt_fingerprint"))
                       or not _valid_hash(item.get("event_fingerprint"))
                       or not _positive_int64(item.get("anchor_slot_id"))
                       or not _positive_int64(item.get("event_id"))
                       for item in ordered)):
            raise ValueError("representative identity cardinality")
        artifacts = verified_registry["implementation_artifacts"]["files"]
        selection_expected = {
            "selection_record_sha256": selection_record_sha256,
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "freeze_id": verified_registry["freeze_id"],
            "registry_record_sha256": verified_registry["registry_record_sha256"],
            "verifier_profile_sha256": verified_registry["verifier_profile_sha256"],
            "selector_version": selector.VERSION,
            "observed_projection_source_sha256": artifacts["projection"]["sha256"],
            "observed_selector_source_sha256": artifacts["selector"]["sha256"],
            "observed_watch_code_manifest_sha256": verified_registry[
                "expected_watch_code_manifest_sha256"
            ],
        }
        if any(selection.get(key) != value for key, value in selection_expected.items()):
            raise ValueError("selection axes")
        if (selection["selection_record_sha256"]
                != contract.digest(selection["selection_record"])
                or selection["selection_attestation_sha256"]
                != contract.digest(selection["selection_attestation"])
                or selection["representative_count"] != len(ordered)
                or selection["representative_identities_sha256"] != contract.digest({
                    "version": "stage8-durable-representative-identities-v1",
                    "exact_binding_sha256": exact_binding["binding_sha256"],
                    "representatives": ordered,
                })):
            raise ValueError("selection hashes")
        record_expected = {
            key: selection[key] for key in _SELECTION_RECORD_KEYS
            if key not in {"version", "persisted_by", "persisted_at_utc"}
        }
        if (record.get("version") != registry_adapter.SELECTION_RECORD_VERSION
                or any(record.get(key) != value
                       for key, value in record_expected.items())
                or not _same_time(record.get("persisted_at_utc"),
                                  selection.get("persisted_at_utc"))
                or record.get("persisted_by")
                != registry_adapter.SELECTOR_WRITER_ROLE):
            raise ValueError("selection record axes")
        batch_expected = {
            "version": selector.BATCH_VERSION,
            "selector_version": selector.VERSION,
            "status": "COMPLETE",
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "manifest_sha256": contract.MANIFEST_SHA256,
            "freeze_id": verified_registry["freeze_id"],
            "frozen_at_utc": verified_registry["frozen_at_utc"],
            "registry_record_sha256": verified_registry["registry_record_sha256"],
            "registry_verification_receipt_sha256": selection[
                "registry_verification_receipt_sha256"
            ],
            "verifier_profile_sha256": verified_registry["verifier_profile_sha256"],
            "expected_projection_source_sha256": artifacts["projection"]["sha256"],
            "expected_selector_source_sha256": artifacts["selector"]["sha256"],
            "expected_watch_code_manifest_sha256": verified_registry[
                "expected_watch_code_manifest_sha256"
            ],
            "cohort_query_sha256": selection["cohort_query_sha256"],
            "outcome_free_population_receipt_sha256": selection[
                "outcome_free_population_receipt_sha256"
            ],
            "source_high_water_attempt_id": selection[
                "source_high_water_attempt_id"
            ],
            "representative_count": len(ordered),
            "representative_set_sha256": selection["representative_set_sha256"],
            "population_coverage_complete": True,
            "candidate_match_coverage_complete": True,
            "outcome_blind_selection": True,
            "outcome_or_label_fields_accepted": False,
            "truncated": False,
            "global_blockers": [],
            "blocked_parents": [],
            "qualification_evaluated": False,
            "database_verification_asserted_by_selector": False,
        }
        if any(batch.get(key) != value for key, value in batch_expected.items()):
            raise ValueError("selection attestation axes")
        if (not _valid_hash(batch.get("attempt_population_sha256"))
                or not _valid_hash(batch.get("source_transaction_identity_sha256"))
                or not _valid_hash(batch.get("source_authority_ledger_sha256"))
                or type(batch.get("source_attempt_count")) is not int
                or not 0 < batch["source_attempt_count"] <= MAX_FACTS
                or batch.get("source_authority_ledger_count")
                != batch["source_attempt_count"]
                or type(batch.get("deduplicated_exact_anchor_event_count")) is not int
                or batch["deduplicated_exact_anchor_event_count"] < 0
                or not isinstance(batch.get("excluded_pre_freeze_parent_ids"), list)
                or not isinstance(batch.get("proven_noneligible_attempt_ids"), list)):
            raise ValueError("selection source completeness")
    except OutcomeAdapterError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OutcomeAdapterError("STAGE8_DURABLE_SELECTION_INVALID") from exc
    return ordered


def _validate_batch_and_seal(
    batch: Mapping[str, Any], seal: Mapping[str, Any],
    registry: Mapping[str, Any], selection: Mapping[str, Any],
) -> list[int]:
    _strict(dict(batch), _FACT_BATCH_VIEW_KEYS, "STAGE8_FACT_BATCH_SHAPE_INVALID")
    _strict(dict(seal), _FACT_SEAL_VIEW_KEYS, "STAGE8_FACT_SEAL_SHAPE_INVALID")
    try:
        record = _strict(
            dict(batch["fact_batch_record"]), _FACT_BATCH_RECORD_KEYS,
            "STAGE8_FACT_BATCH_RECORD_SHAPE_INVALID",
        )
        seal_record = _strict(
            dict(seal["seal_record"]), _FACT_SEAL_RECORD_KEYS,
            "STAGE8_FACT_SEAL_RECORD_SHAPE_INVALID",
        )
        attempt_ids = batch["attempt_ids"]
        if (not isinstance(attempt_ids, list)
                or attempt_ids != sorted(set(attempt_ids))
                or not attempt_ids or len(attempt_ids) > MAX_FACTS
                or any(not _positive_int64(item) for item in attempt_ids)):
            raise ValueError("fact attempt population")
        expected = {
            "fact_batch_record_sha256": selection["fact_batch_record_sha256"],
            "exact_binding_sha256": selection["exact_binding_sha256"],
            "freeze_id": selection["freeze_id"],
            "registry_record_sha256": selection["registry_record_sha256"],
            "verifier_profile_sha256": selection["verifier_profile_sha256"],
            "registry_verification_receipt_sha256": selection[
                "registry_verification_receipt_sha256"
            ],
            "observed_projection_source_sha256": selection[
                "observed_projection_source_sha256"
            ],
            "observed_registry_adapter_source_sha256": registry[
                "implementation_artifacts"
            ]["files"]["registry_adapter"]["sha256"],
            "coverage_query_sha256": selection["cohort_query_sha256"],
            "outcome_free_population_receipt_sha256": selection[
                "outcome_free_population_receipt_sha256"
            ],
            "coverage_source_high_water_attempt_id": selection[
                "source_high_water_attempt_id"
            ],
            "attempt_count": len(attempt_ids),
        }
        if (any(batch.get(key) != value for key, value in expected.items())
                or (batch["watch_archive_high_water_snapshot_set_id"] is not None
                    and not _positive_int64(
                        batch["watch_archive_high_water_snapshot_set_id"]))
                or batch["fact_batch_record_sha256"]
                != contract.digest(record)
                or batch["projection_source_manifest_sha256"]
                != contract.digest(batch["projection_source_manifest"])
                or not _embedded_receipt_hash_matches(
                    batch["adapter_population_receipt"],
                    hash_key="population_receipt_sha256",
                    stored_sha256=batch["adapter_population_receipt_sha256"],
                )
                or not _embedded_receipt_hash_matches(
                    batch["adapter_authority_receipt"],
                    hash_key="authority_receipt_sha256",
                    stored_sha256=batch["adapter_authority_receipt_sha256"],
                )
                or batch["coverage_attempt_population_sha256"]
                != selection["selection_attestation"]["attempt_population_sha256"]
                or selection["selection_attestation"]["source_attempt_count"]
                != len(attempt_ids)):
            raise ValueError("fact batch axes")
        record_expected = {
            key: batch[key] for key in _FACT_BATCH_RECORD_KEYS
            if key not in {"version", "persisted_at_utc", "persisted_by"}
        }
        if (record.get("version") != "stage8-durable-projection-fact-batch-v1"
                or any(record.get(key) != value
                       for key, value in record_expected.items())
                or not _same_time(record.get("persisted_at_utc"),
                                  batch.get("persisted_at_utc"))
                or record.get("persisted_by") != registry_adapter.FACT_WRITER_ROLE):
            raise ValueError("fact batch record axes")
        if (seal.get("fact_batch_record_sha256") != batch["fact_batch_record_sha256"]
                or seal.get("fact_count") != len(attempt_ids)
                or not _valid_hash(seal.get("fact_records_sha256"))
                or seal.get("seal_record_sha256") != contract.digest(seal_record)
                or seal_record.get("version")
                != "stage8-durable-projection-fact-seal-v1"
                or seal_record.get("fact_batch_record_sha256")
                != batch["fact_batch_record_sha256"]
                or seal_record.get("fact_count") != len(attempt_ids)
                or seal_record.get("fact_records_sha256")
                != seal.get("fact_records_sha256")
                or not _same_time(seal_record.get("sealed_at_utc"),
                                  seal.get("sealed_at_utc"))
                or seal_record.get("sealed_by")
                != registry_adapter.FACT_WRITER_ROLE):
            raise ValueError("fact seal axes")
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OutcomeAdapterError("STAGE8_FACT_BATCH_OR_SEAL_INVALID") from exc
    return attempt_ids


def _selection_fact_identity_payload(
    exact_binding: Mapping[str, Any], *, attempt_identity: Mapping[str, Any],
    fact: Mapping[str, Any], parent: Any, proof: Any,
) -> dict[str, Any]:
    """Rebuild the migration-owned, outcome-free selection identity."""
    try:
        identity = fact["identity"]
        if (type(identity) is not dict
                or attempt_identity.get("row_status") != "FOUND"
                or not _positive_int64(attempt_identity.get("attempt_id"))
                or attempt_identity.get("evaluation_status") not in {
                    "EVALUABLE", "UNEVALUABLE", "COVERAGE_EXCLUDED"
                }
                or attempt_identity.get("attempt_id") != identity.get("attempt_id")
                or attempt_identity.get("attempt_fingerprint")
                != identity.get("attempt_fingerprint")
                or attempt_identity.get("sampler_version")
                != identity.get("sampler_version")
                or attempt_identity.get("symbol") != identity.get("symbol")):
            raise ValueError("attempt identity mismatch")
        evaluation_status = attempt_identity["evaluation_status"]
        evaluable = evaluation_status == "EVALUABLE"
        raw_slot = (
            identity.get("anchor_slot_id"), identity.get("event_id"),
            identity.get("event_fingerprint"),
        )
        slot_complete = (
            _positive_int64(raw_slot[0]) and _positive_int64(raw_slot[1])
            and _valid_hash(raw_slot[2])
        )
        slot_absent = raw_slot == (None, None, None)
        if evaluable:
            if not (slot_complete or slot_absent):
                raise ValueError("selection fact slot identity is partial")
            anchor_slot_id, event_id, event_fingerprint = raw_slot
            decision_time_utc = (
                _iso(identity.get("decision_time_utc"))
                if slot_complete else None
            )
        else:
            if not slot_absent or identity.get("decision_time_utc") is not None:
                raise ValueError("non-evaluable selection fact has slot identity")
            anchor_slot_id = event_id = event_fingerprint = None
            decision_time_utc = None
        if (parent is None) == (proof is None):
            raise ValueError("parent authority is not exclusive")
        if proof is not None:
            if evaluable:
                raise ValueError("evaluable selection fact requires parent evidence")
            parent_class = "PROVEN_NOT_CANDIDATE_ELIGIBLE"
            parent_id = parent_start = parent_policy = membership = eligible = None
        elif type(parent) is dict:
            if not evaluable:
                raise ValueError("non-evaluable selection fact requires proof")
            status = parent.get("validation_status")
            parent_class = {
                "VALID": "LIVE",
                "PROVEN_NOT_EVIDENCE_ELIGIBLE":
                    "PROVEN_NOT_EVIDENCE_ELIGIBLE",
            }.get(status, "UNKNOWN")
            if status in {"VALID", "PROVEN_NOT_EVIDENCE_ELIGIBLE"}:
                parent_id = parent.get("btc_parent_movement_id")
                parent_start = _iso(parent.get("parent_start_time_utc"))
                parent_policy = _STARTUP_MANIFEST["independence"][
                    "parent_policy_version"
                ]
                membership = parent.get("membership_status")
                eligible = parent.get("parent_evidence_eligible")
            else:
                parent_id = parent_start = parent_policy = membership = eligible = None
        else:
            raise ValueError("parent authority malformed")
        payload = {
            "version": "stage8-selection-fact-identity-v1",
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "attempt_id": attempt_identity["attempt_id"],
            "attempt_fingerprint": attempt_identity["attempt_fingerprint"],
            "sampler_version": attempt_identity["sampler_version"],
            "source_candle_open_utc": _iso(
                attempt_identity["source_candle_open_utc"]
            ),
            "source_attempt_evaluation_status": evaluation_status,
            "anchor_slot_id": anchor_slot_id,
            "event_id": event_id,
            "event_fingerprint": event_fingerprint,
            "symbol": attempt_identity["symbol"],
            "direction": exact_binding["binding"]["candidate"]["direction"],
            "decision_time_utc": decision_time_utc,
            "knowledge_status": fact.get("knowledge_status"),
            "candidate_match": fact.get("candidate_match"),
            "parent_authority_class": parent_class,
            "btc_parent_movement_id": parent_id,
            "parent_start_time_utc": parent_start,
            "parent_policy_version": parent_policy,
            "membership_status": membership,
            "parent_evidence_eligible": eligible,
        }
        _strict(
            payload, _SELECTION_FACT_IDENTITY_KEYS,
            "STAGE8_SELECTION_FACT_IDENTITY_SHAPE_INVALID",
        )
        predicted = selector.canonical_selection_fact_identity(
            exact_binding, fact,
            source_attempt_evaluation_status=evaluation_status,
            parent_authority_class=parent_class,
            parent_membership_evidence=(parent if type(parent) is dict else None),
        )
        if payload != predicted:
            raise ValueError("selection fact identity semantic drift")
        return payload
    except OutcomeAdapterError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OutcomeAdapterError(
            "STAGE8_SELECTION_FACT_IDENTITY_RECONSTRUCTION_INVALID"
        ) from exc


def _validated_durable_selection_fact_identity(
    row: Mapping[str, Any], exact_binding: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _strict(
        dict(row.get("selection_fact_identity") or {}),
        _SELECTION_FACT_IDENTITY_KEYS,
        "STAGE8_DURABLE_SELECTION_FACT_IDENTITY_SHAPE_INVALID",
    )
    supplied = row.get("selection_fact_identity_sha256")
    fact = row.get("fact")
    if (not _valid_hash(supplied) or contract.digest(payload) != supplied
            or type(fact) is not dict):
        raise OutcomeAdapterError("STAGE8_DURABLE_SELECTION_FACT_IDENTITY_INVALID")
    identity = fact.get("identity")
    if type(identity) is not dict:
        raise OutcomeAdapterError("STAGE8_DURABLE_SELECTION_FACT_IDENTITY_INVALID")
    attempt_identity = {
        "attempt_id": row.get("attempt_id"),
        "row_status": "FOUND",
        "attempt_fingerprint": identity.get("attempt_fingerprint"),
        "sampler_version": identity.get("sampler_version"),
        "symbol": identity.get("symbol"),
        "evaluation_status": payload.get("source_attempt_evaluation_status"),
        "source_candle_open_utc": identity.get("source_candle_open_utc"),
        "decision_time_utc": identity.get("decision_time_utc"),
    }
    rebuilt = _selection_fact_identity_payload(
        exact_binding, attempt_identity=attempt_identity, fact=fact,
        parent=row.get("parent_membership_evidence"),
        proof=row.get("noneligibility_proof"),
    )
    if rebuilt != payload:
        raise OutcomeAdapterError("STAGE8_DURABLE_SELECTION_FACT_IDENTITY_INVALID")
    return payload


def _fact_record_valid(
    row: Mapping[str, Any], batch_sha: str,
    exact_binding: Mapping[str, Any],
) -> bool:
    try:
        record = _strict(dict(row["fact_record"]), _FACT_RECORD_KEYS,
                         "fact record shape")
        expected = {
            key: row[key] for key in (
                "fact_batch_record_sha256", "exact_binding_sha256", "attempt_id",
                "attempt_fingerprint", "anchor_slot_id", "event_id",
                "event_fingerprint", "symbol", "direction", "knowledge_status",
                "candidate_match", "fact_sha256", "fact_authority_sha256",
                "watch_selection_attestation_sha256",
                "observed_watch_code_manifest_sha256",
                "parent_membership_evidence_sha256", "noneligibility_proof_sha256",
                "selection_fact_identity_sha256",
            )
        }
        expected["decision_time_utc"] = (
            None if row["decision_time_utc"] is None else _iso(row["decision_time_utc"])
        )
        expected["persisted_at_utc"] = _iso(row["persisted_at_utc"])
        return bool(
            row["fact_batch_record_sha256"] == batch_sha
            and row["fact_record_sha256"] == contract.digest(record)
            and record.get("version") == "stage8-durable-projected-fact-record-v1"
            and record.get("persisted_by") == registry_adapter.FACT_WRITER_ROLE
            and _valid_hash(record.get("server_projection_attestation_sha256"))
            and record.get("server_projection_status")
            in {"VERIFIED", "PROVEN_NONELIGIBLE", "UNKNOWN"}
            and all(record.get(key) == value for key, value in expected.items())
            and _validated_durable_selection_fact_identity(row, exact_binding)
            is not None
        )
    except (KeyError, TypeError, ValueError, OverflowError, OutcomeAdapterError):
        return False


def _representative_fact_reasons(
    identity: Mapping[str, Any], fact_row: Mapping[str, Any] | None,
    event: Mapping[str, Any] | None, registry: Mapping[str, Any],
    exact_binding: Mapping[str, Any], batch_sha: str,
) -> list[str]:
    reasons: list[str] = []
    if fact_row is None:
        return ["DURABLE_REPRESENTATIVE_FACT_MISSING"]
    try:
        _strict(dict(fact_row), _FACT_VIEW_KEYS, "fact shape")
        if event is None:
            reasons.append("REPRESENTATIVE_EVENT_MISSING")
        else:
            _strict(dict(event), _EVENT_KEYS, "event shape")
        expected = {
            "fact_batch_record_sha256": batch_sha,
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "attempt_fingerprint": identity["attempt_fingerprint"],
            "anchor_slot_id": identity["anchor_slot_id"],
            "event_id": identity["event_id"],
            "event_fingerprint": identity["event_fingerprint"],
            "symbol": identity["symbol"],
            "direction": identity["direction"],
            "knowledge_status": "KNOWN",
            "candidate_match": True,
            "selection_fact_identity_sha256": identity[
                "expected_selection_fact_identity_sha256"
            ],
        }
        if any(fact_row.get(key) != value for key, value in expected.items()):
            reasons.append("REPRESENTATIVE_FACT_IDENTITY_MISMATCH")
        if not _same_time(fact_row.get("decision_time_utc"), identity["decision_time_utc"]):
            reasons.append("REPRESENTATIVE_FACT_DECISION_TIME_MISMATCH")
        if not _fact_record_valid(fact_row, batch_sha, exact_binding):
            reasons.append("REPRESENTATIVE_FACT_RECORD_HASH_INVALID")
        authority = fact_row.get("fact_authority")
        if type(authority) is not dict:
            reasons.append("REPRESENTATIVE_FACT_AUTHORITY_MISSING")
        else:
            unsigned = dict(authority)
            authority_sha = unsigned.pop("fact_authority_sha256", None)
            authority_expected = {
                "expected_fact_sha256": fact_row.get("fact_sha256"),
                "attempt_fingerprint": identity["attempt_fingerprint"],
                "anchor_slot_id": identity["anchor_slot_id"],
                "event_id": identity["event_id"],
                "event_fingerprint": identity["event_fingerprint"],
                "symbol": identity["symbol"], "direction": identity["direction"],
                "knowledge_status": "KNOWN", "candidate_match": True,
            }
            if (authority_sha != fact_row.get("fact_authority_sha256")
                    or authority_sha != contract.digest(unsigned)
                    or any(authority.get(key) != value
                           for key, value in authority_expected.items())):
                reasons.append("REPRESENTATIVE_FACT_AUTHORITY_HASH_INVALID")
        parent = fact_row.get("parent_membership_evidence")
        if type(parent) is not dict:
            reasons.append("REPRESENTATIVE_PARENT_EVIDENCE_MISSING")
        else:
            parent_expected = {
                "validation_status": "VALID", "event_id": identity["event_id"],
                "event_fingerprint": identity["event_fingerprint"],
                "symbol": identity["symbol"], "direction": identity["direction"],
                "parent_policy_version": _STARTUP_MANIFEST["independence"][
                    "parent_policy_version"
                ],
                "membership_status": "LIVE",
                "btc_parent_movement_id": identity["btc_parent_movement_id"],
                "parent_start_time_utc": identity["parent_start_time_utc"],
                "parent_evidence_eligible": True,
            }
            if (fact_row.get("parent_membership_evidence_sha256")
                    != contract.digest(parent)
                    or any(parent.get(key) != value
                           for key, value in parent_expected.items())
                    or not _same_time(parent.get("decision_time_utc"),
                                      identity["decision_time_utc"])):
                reasons.append("REPRESENTATIVE_PARENT_EVIDENCE_MISMATCH")
        watch = fact_row.get("watch_selection_attestation")
        if type(watch) is not dict:
            reasons.append("REPRESENTATIVE_WATCH_ATTESTATION_MISSING")
        else:
            unsigned_watch = dict(watch)
            supplied_watch = unsigned_watch.pop("attestation_sha256", None)
            if (supplied_watch != fact_row.get("watch_selection_attestation_sha256")
                    or supplied_watch != contract.digest(unsigned_watch)):
                reasons.append("REPRESENTATIVE_WATCH_ATTESTATION_HASH_INVALID")
        try:
            projection.validate_fact(
                fact_row["fact"],
                expected_fact_sha256=fact_row["fact_sha256"],
                expected_watch_selection_attestation_sha256=fact_row[
                    "watch_selection_attestation_sha256"
                ],
                expected_watch_code_manifest_sha256=registry[
                    "expected_watch_code_manifest_sha256"
                ],
            )
        except (TypeError, ValueError, KeyError):
            reasons.append("REPRESENTATIVE_PROJECTED_FACT_INVALID")
        if event is not None:
            price = _finite(event.get("current_price"))
            event_expected = {
                "event_id": identity["event_id"],
                "event_fingerprint": identity["event_fingerprint"],
                "symbol": identity["symbol"], "direction": identity["direction"],
            }
            if (any(event.get(key) != value for key, value in event_expected.items())
                    or not _same_time(event.get("alert_time_utc"),
                                      identity["decision_time_utc"])
                    or price is None or price <= 0.0):
                reasons.append("REPRESENTATIVE_EVENT_IDENTITY_MISMATCH")
    except (KeyError, TypeError, ValueError, OverflowError, OutcomeAdapterError):
        reasons.append("REPRESENTATIVE_FACT_OR_EVENT_MALFORMED")
    return sorted(set(reasons))


def _base_representatives(
    exact_binding: Mapping[str, Any], identities: Sequence[Mapping[str, Any]],
    registry: Mapping[str, Any], selection: Mapping[str, Any], *,
    verified_selection_fact_identities: Mapping[int, str | None],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    binding = acceptance.representative_binding(exact_binding)
    rows: list[dict[str, Any]] = []
    for identity in identities:
        actual_selection_sha = verified_selection_fact_identities.get(
            identity["event_id"]
        )
        representative = {
            key: identity[key] for key in (
                "attempt_fingerprint",
                "anchor_slot_id", "event_id", "event_fingerprint", "symbol",
                "direction", "decision_time_utc",
                "candidate_match_knowledge_status", "candidate_match",
            )
        }
        representative["selection_fact_identity_sha256"] = actual_selection_sha
        row = {
            "binding": deepcopy(binding),
            "btc_parent_movement_id": identity["btc_parent_movement_id"],
            "parent_start_time_utc": identity["parent_start_time_utc"],
            "representative_status": "VALID",
            "parent_policy_version": binding["parent_policy_version"],
            "membership_status": "LIVE",
            "parent_evidence_eligible": True,
            "freeze_id": registry["freeze_id"],
            "registry_record_sha256": registry["registry_record_sha256"],
            "registry_verification_receipt_sha256": selection[
                "registry_verification_receipt_sha256"
            ],
            "selection_attestation_sha256": None,
            "representative": representative,
            "representative_identity_sha256": None,
        }
        try:
            row["representative_identity_sha256"] = (
                acceptance.representative_identity_sha256(exact_binding, row)
            )
        except (TypeError, ValueError, KeyError, OverflowError):
            row["representative_status"] = "UNKNOWN"
        rows.append(row)
    count, _ = acceptance._representative_set(exact_binding, rows)
    if count != selection["representative_count"]:
        raise OutcomeAdapterError("STAGE8_DURABLE_REPRESENTATIVE_SET_INVALID")
    provenance = acceptance.bind_registry_selection_receipt(
        exact_binding, rows,
        freeze_id=registry["freeze_id"],
        frozen_at_utc=registry["frozen_at_utc"],
        registry_record_sha256=registry["registry_record_sha256"],
        registry_verification_receipt_sha256=selection[
            "registry_verification_receipt_sha256"
        ],
        cohort_query_sha256=selection["cohort_query_sha256"],
        population_receipt_sha256=selection[
            "outcome_free_population_receipt_sha256"
        ],
        source_high_water_attempt_id=selection["source_high_water_attempt_id"],
    )
    for row in rows:
        row["selection_attestation_sha256"] = provenance["attestation_sha256"]
    return rows, provenance


def _outcome_evidence(
    exact_binding: Mapping[str, Any], row: Mapping[str, Any],
    identity_sha: str, event: Mapping[str, Any] | None,
    candidates: Sequence[Mapping[str, Any]], *, as_of_utc: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    identity = row["representative"]
    reasons: list[str] = []
    source = candidates[0] if len(candidates) == 1 else None
    if not candidates:
        reasons.append("ORDERED_V7_ROW_MISSING")
    elif len(candidates) != 1:
        reasons.append("ORDERED_V7_ROW_DUPLICATED")
    raw_sha = _raw_hash(source) if source is not None else None
    reported = "UNKNOWN"
    source_status = None
    terminal_reason = None
    if source is not None:
        source_status = source.get("status")
        terminal_reason = source.get("terminal_reason")
        try:
            _strict(dict(source), _OUTCOME_KEYS, "outcome shape")
            expected = {
                "event_id": identity["event_id"],
                "window_minutes": exact_binding["binding"]["window_minutes"],
                "threshold_bps": exact_binding["binding"]["threshold_bps"],
                "method_version": _STARTUP_MANIFEST["labels"]["method_version"],
                "direction": identity["direction"],
            }
            if any(source.get(key) != value for key, value in expected.items()):
                reasons.append("ORDERED_V7_EXACT_CELL_MISMATCH")
            if not _same_time(source.get("measurement_start_utc"),
                              identity["decision_time_utc"]):
                reasons.append("ORDERED_V7_MEASUREMENT_START_MISMATCH")
            audit = source_audit.validate_outcome_cell(
                event, source,
                window_minutes=exact_binding["binding"]["window_minutes"],
                threshold_bps=exact_binding["binding"]["threshold_bps"],
                analysis_as_of_utc=as_of_utc,
            )
            reported = audit.get("reported_status") or "UNKNOWN"
            reasons.extend("ORDERED_V7:" + reason for reason in audit.get("reasons", []))
        except (OutcomeAdapterError, TypeError, ValueError, KeyError, OverflowError) as exc:
            reasons.append("ORDERED_V7_ROW_INVALID:" + type(exc).__name__)
    valid = not reasons and reported in {"SUCCESS", "FAILURE"}
    evidence = {
        "validation_status": "VALID" if valid else "UNKNOWN",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "btc_parent_movement_id": row["btc_parent_movement_id"],
        "representative_identity_sha256": identity_sha,
        "event_id": identity["event_id"],
        "selection_fact_identity_sha256": identity[
            "selection_fact_identity_sha256"
        ],
        "direction": identity["direction"],
        "method_version": _STARTUP_MANIFEST["labels"]["method_version"],
        "window_minutes": exact_binding["binding"]["window_minutes"],
        "threshold_bps": exact_binding["binding"]["threshold_bps"],
        "reported_status": reported,
        "source_status": source_status,
        "terminal_reason": terminal_reason,
        "source_row_sha256": raw_sha,
        "reasons": sorted(set(reasons)),
    }
    audit_entry = {
        "source_status": source_status,
        "reported_status": reported,
        "validation_status": evidence["validation_status"],
        "source_row_sha256": raw_sha,
        "reasons": evidence["reasons"],
    }
    return evidence, audit_entry


def _scope_route_matches(scope_route: str, symbol: str, route: Mapping[str, Any]) -> bool:
    if scope_route == "BINANCE_SPOT_1M":
        return symbol != "HYPE" and route.get("exchange") == "binance"
    if scope_route == "HYPERLIQUID_SPOT_@107_1M":
        return (symbol == "HYPE" and route.get("exchange") == "hyperliquid"
                and route.get("instrument") == "@107")
    return False


def _asymmetry_evidence(
    exact_binding: Mapping[str, Any], row: Mapping[str, Any],
    identity_sha: str, event: Mapping[str, Any] | None,
    candidates: Sequence[Mapping[str, Any]], *, as_of_utc: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    identity = row["representative"]
    reasons: list[str] = []
    source_row = candidates[0] if len(candidates) == 1 else None
    if not candidates:
        reasons.append("COMMON_WINDOW_ROW_MISSING")
    elif len(candidates) != 1:
        reasons.append("COMMON_WINDOW_ROW_DUPLICATED")
    raw_sha = _raw_hash(source_row) if source_row is not None else None
    status = source_row.get("status") if source_row is not None else None
    mfe = mae = None
    if source_row is not None:
        try:
            _strict(dict(source_row), _COMMON_WINDOW_ROW_KEYS, "common row shape")
            result = source_row.get("result")
            if type(result) is not dict:
                raise ValueError("common result missing")
            expected_outer = {
                "event_id": identity["event_id"],
                "window_minutes": exact_binding["binding"]["window_minutes"],
                "method_version": _COMMON_WINDOW_VERSION,
            }
            if any(source_row.get(key) != value for key, value in expected_outer.items()):
                reasons.append("COMMON_WINDOW_EXACT_CELL_MISMATCH")
            if not _same_time(source_row.get("measurement_start_utc"),
                              identity["decision_time_utc"]):
                reasons.append("COMMON_WINDOW_MEASUREMENT_START_MISMATCH")
            if source_row.get("status") != "READY":
                reasons.append("COMMON_WINDOW_NONREADY:" + str(status or "UNKNOWN"))
            if status == "READY":
                _strict(dict(result), _COMMON_RESULT_READY_KEYS,
                        "common READY result shape")
                start = _utc(identity["decision_time_utc"])
                end = start + timedelta(
                    minutes=exact_binding["binding"]["window_minutes"]
                )
                expected_result = {
                    "method_version": _COMMON_WINDOW_VERSION,
                    "measurement_kind": "FIXED_WINDOW",
                    "window_minutes": exact_binding["binding"]["window_minutes"],
                    "symbol": identity["symbol"], "direction": identity["direction"],
                    "status": "READY", "observation_closed": True,
                    "path_complete": True, "observed_prefix_complete": True,
                    "boundary_policy":
                        "EXCLUDE_PARTIAL_MINUTES_USE_IMMUTABLE_ALERT_PRICE",
                    "candle_interval_seconds": 60,
                    "asymmetry_method": common_window.ASYMMETRY_METHOD,
                    "missing_reason": None,
                }
                if any(result.get(key) != value for key, value in expected_result.items()):
                    reasons.append("COMMON_WINDOW_RESULT_CONTRACT_MISMATCH")
                for left, right, reason in (
                    (result.get("measurement_start_utc"), start,
                     "COMMON_WINDOW_RESULT_START_MISMATCH"),
                    (result.get("window_end_utc"), end,
                     "COMMON_WINDOW_RESULT_END_MISMATCH"),
                    (source_row.get("window_end_utc"), end,
                     "COMMON_WINDOW_OUTER_END_MISMATCH"),
                ):
                    if not _same_time(left, right):
                        reasons.append(reason)
                if (source_row.get("status") != result.get("status")
                        or not _same_time(source_row.get("measurement_start_utc"),
                                          result.get("measurement_start_utc"))
                        or not _same_time(source_row.get("window_end_utc"),
                                          result.get("window_end_utc"))):
                    reasons.append("COMMON_WINDOW_OUTER_RESULT_MISMATCH")
                expected_candles = result.get("expected_candles")
                samples = result.get("path_samples")
                if (type(expected_candles) is not int or type(samples) is not int
                        or not 0 < expected_candles <= exact_binding["binding"]["window_minutes"]
                        or samples != expected_candles):
                    reasons.append("COMMON_WINDOW_COVERAGE_MISMATCH")
                if (not _same_time(result.get("observed_at_utc"),
                                   result.get("observed_at_utc"))
                        or _utc(result.get("observed_at_utc")) < end
                        or _utc(result.get("observed_at_utc")) > _utc(as_of_utc)):
                    reasons.append("COMMON_WINDOW_OBSERVATION_TIME_INVALID")
                if (result.get("data_quality_status")
                        not in canonical_price_path.COMPLETE_QUALITIES
                        or not _valid_hash(result.get("path_sha256"))):
                    reasons.append("COMMON_WINDOW_PATH_QUALITY_INVALID")
                source = result.get("source")
                try:
                    source_dict = dict(source)
                    route = canonical_price_path.validated_route(
                        identity["symbol"],
                        {**source_dict, "api_coin": source_dict.get("instrument"),
                         "provenance": source_dict.get("provider_provenance"),
                         "complete": True},
                        require_complete=True,
                    )
                    if (contract.canonical(route) != contract.canonical(source_dict)
                            or not _scope_route_matches(
                                exact_binding["binding"]["scope"]["price_route"],
                                identity["symbol"], route,
                            )):
                        reasons.append("COMMON_WINDOW_SCOPE_PRICE_ROUTE_MISMATCH")
                    expected_quality = canonical_price_path.quality_status(
                        {**source_dict, "api_coin": source_dict.get("instrument"),
                         "provenance": source_dict.get("provider_provenance")},
                        complete=True,
                    )
                    if expected_quality != result.get("data_quality_status"):
                        reasons.append("COMMON_WINDOW_ROUTE_QUALITY_MISMATCH")
                except (TypeError, ValueError, KeyError):
                    reasons.append("COMMON_WINDOW_PRICE_ROUTE_INVALID")
                mfe, mae = _finite(result.get("mfe_pct")), _finite(result.get("mae_pct"))
                reference = _finite(result.get("reference_price"))
                event_reference = _finite(event.get("current_price")) if event else None
                if (mfe is None or mae is None or min(mfe, mae) < 0.0
                        or reference is None or reference <= 0.0
                        or event_reference is None
                        or not math.isclose(reference, event_reference, rel_tol=1e-12)):
                    reasons.append("COMMON_WINDOW_METRICS_OR_REFERENCE_INVALID")
                ratio = _finite(result.get("asymmetry_ratio"))
                if mae is not None and mae > 0.0:
                    if (ratio is None or mfe is None
                            or not math.isclose(ratio, mfe / mae, rel_tol=1e-12)
                            or result.get("asymmetry_status") != "DEFINED"):
                        reasons.append("COMMON_WINDOW_ASYMMETRY_DIAGNOSTIC_MISMATCH")
                elif (result.get("asymmetry_ratio") is not None
                      or result.get("asymmetry_status") != "UNDEFINED_ZERO_MAE"):
                    reasons.append("COMMON_WINDOW_ZERO_DENOMINATOR_MISMATCH")
            elif frozenset(result) not in {
                    _COMMON_RESULT_BASE_KEYS, _COMMON_RESULT_READY_KEYS}:
                reasons.append("COMMON_WINDOW_NONREADY_RESULT_SHAPE_INVALID")
            for key in ("created_at_utc", "updated_at_utc"):
                if _utc(source_row[key]) > _utc(as_of_utc):
                    reasons.append("COMMON_WINDOW_REVISION_AFTER_SNAPSHOT:" + key)
            if _utc(source_row["created_at_utc"]) > _utc(source_row["updated_at_utc"]):
                reasons.append("COMMON_WINDOW_REVISION_ORDER_INVALID")
        except (OutcomeAdapterError, TypeError, ValueError, KeyError, OverflowError) as exc:
            reasons.append("COMMON_WINDOW_ROW_INVALID:" + type(exc).__name__)
    valid = not reasons and status == "READY" and mfe is not None and mae is not None
    evidence = {
        "validation_status": "VALID" if valid else "UNKNOWN",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "btc_parent_movement_id": row["btc_parent_movement_id"],
        "representative_identity_sha256": identity_sha,
        "event_id": identity["event_id"],
        "selection_fact_identity_sha256": identity[
            "selection_fact_identity_sha256"
        ],
        "direction": identity["direction"],
        "method_version": _COMMON_WINDOW_VERSION,
        "measurement_kind": "FIXED_WINDOW",
        "reported_status": status or "UNKNOWN",
        "window_minutes": exact_binding["binding"]["window_minutes"],
        "scope_price_route": exact_binding["binding"]["scope"]["price_route"],
        "path_complete": valid,
        "observation_closed": valid,
        "coverage_complete": valid,
        "mfe_pct": mfe if valid else None,
        "mae_pct": mae if valid else None,
        "source_row_sha256": raw_sha,
        "reasons": sorted(set(reasons)),
    }
    audit_entry = {
        "source_status": status,
        "validation_status": evidence["validation_status"],
        "source_row_sha256": raw_sha,
        "mfe_pct": evidence["mfe_pct"], "mae_pct": evidence["mae_pct"],
        "zero_denominator": bool(valid and mae == 0.0),
        "reasons": evidence["reasons"],
    }
    return evidence, audit_entry


_WATCH_SELECTION_SEMANTIC_KEYS = (
    "version", "source_audit_version", "selection_policy", "attempt_id",
    "attempt_fingerprint", "symbol", "decision_time_utc",
    "max_capture_age_seconds", "selection_status", "selected_snapshot_set_id",
    "selected_snapshot_key", "selected_payload_sha256",
    "selected_durably_available_at_utc", "watch_code_manifest_sha256",
)
_PARENT_CAUSAL_KEYS = (
    "version", "validation_status", "event_id", "event_fingerprint", "symbol",
    "direction", "decision_time_utc", "parent_policy_version",
    "membership_status", "btc_parent_movement_id", "btc_observed_close_utc",
    "parent_start_time_utc", "parent_direction", "parent_evidence_eligible",
    "parent_price_source", "btc_bar",
)


def _fact_semantic_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Remove only transaction/audit identity from a validated projected fact."""
    fact = _decoded(value, reason="STAGE8_FACT_REPLAY_FACT_INVALID")
    supplied = fact.pop("fact_sha256", None)
    if not _valid_hash(supplied) or contract.digest(fact) != supplied:
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_FACT_HASH_INVALID")
    source = fact.get("source")
    if type(source) is not dict:
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_SOURCE_INVALID")
    source = deepcopy(source)
    attestation = source.pop("watch_selection_attestation", None)
    source.pop("watch_selection_attestation_sha256", None)
    source.pop("watch_selection_query_binding_sha256", None)
    if attestation is None:
        selection_semantics = None
    elif type(attestation) is dict:
        selection_semantics = {
            key: _json_safe(attestation.get(key))
            for key in _WATCH_SELECTION_SEMANTIC_KEYS
        }
    else:
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_WATCH_ATTESTATION_INVALID")
    source["watch_selection_causal_semantics"] = selection_semantics
    fact["source"] = source
    return fact


def _parent_semantic_projection(value: Any) -> tuple[str, dict[str, Any]]:
    if type(value) is not dict:
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_PARENT_EVIDENCE_INVALID")
    status = value.get("validation_status")
    if status in {"VALID", "PROVEN_NOT_EVIDENCE_ELIGIBLE"}:
        projected = {key: _json_safe(value.get(key)) for key in _PARENT_CAUSAL_KEYS}
        return ("LIVE" if status == "VALID" else status), projected
    # UNKNOWN is not evidence, but its full reason/identity must not metamorphose.
    return "UNKNOWN", _json_safe(value)


def _proof_semantic_projection(value: Any) -> tuple[str, dict[str, Any]]:
    if type(value) is not dict:
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_NONELIGIBILITY_INVALID")
    proof = deepcopy(value)
    supplied = proof.pop("proof_sha256", None)
    if not _valid_hash(supplied) or contract.digest(proof) != supplied:
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_NONELIGIBILITY_HASH_INVALID")
    return "PROVEN_NOT_CANDIDATE_ELIGIBLE", proof


def _fact_source_semantics(
    *, fact: Mapping[str, Any], parent: Any, proof: Any,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    if (parent is None) == (proof is None):
        raise OutcomeAdapterError("STAGE8_FACT_REPLAY_PARENT_STATE_INVALID")
    fact_semantics = _fact_semantic_projection(fact)
    if parent is not None:
        parent_class, parent_semantics = _parent_semantic_projection(parent)
    else:
        parent_class, parent_semantics = _proof_semantic_projection(proof)
    return fact_semantics, parent_class, parent_semantics


def _durable_fact_source_semantics(
    row: Mapping[str, Any], *, exact_binding_sha256: str,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    try:
        fact = row["fact"]
        authority = deepcopy(row["fact_authority"])
        supplied_authority = authority.pop("fact_authority_sha256", None)
        if (row.get("exact_binding_sha256") != exact_binding_sha256
                or supplied_authority != row.get("fact_authority_sha256")
                or not _valid_hash(supplied_authority)
                or contract.digest(authority) != supplied_authority
                or authority.get("expected_fact_sha256") != row.get("fact_sha256")
                or authority.get("attempt_id") != row.get("attempt_id")
                or authority.get("attempt_fingerprint")
                != row.get("attempt_fingerprint")):
            raise ValueError("durable authority mismatch")
        parent = row.get("parent_membership_evidence")
        proof = row.get("noneligibility_proof")
        if parent is not None:
            if (row.get("parent_membership_evidence_sha256")
                    != contract.digest(parent)
                    or authority.get("expected_parent_membership_evidence_sha256")
                    != row.get("parent_membership_evidence_sha256")
                    or row.get("noneligibility_proof_sha256") is not None):
                raise ValueError("durable parent hash mismatch")
        elif proof is not None:
            proof_unsigned = deepcopy(proof)
            proof_sha = proof_unsigned.pop("proof_sha256", None)
            if (proof_sha != row.get("noneligibility_proof_sha256")
                    or proof_sha
                    != authority.get("expected_noneligibility_proof_sha256")
                    or contract.digest(proof_unsigned) != proof_sha
                    or row.get("parent_membership_evidence_sha256") is not None):
                raise ValueError("durable proof hash mismatch")
        return _fact_source_semantics(fact=fact, parent=parent, proof=proof)
    except OutcomeAdapterError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OutcomeAdapterError("STAGE8_DURABLE_FACT_REPLAY_ENVELOPE_INVALID") from exc


def _caller_fact_replay(
    conn: Any, exact_binding: Mapping[str, Any], *,
    attempt_ids: Sequence[int], durable_facts: Sequence[Mapping[str, Any]],
    registry: Mapping[str, Any], selection: Mapping[str, Any],
    transaction: Mapping[str, Any],
) -> tuple[dict[str, Any], bool, dict[int, list[str]]]:
    """Compare all sealed facts with causal source reads as caller diagnostics."""
    try:
        projected = _PROJECT_EXACT_BINDING(
            conn, exact_binding=exact_binding, attempt_ids=list(attempt_ids),
            max_wall_seconds=MAX_WALL_SECONDS,
        )
        replay, replay_ids, ledger = registry_adapter._validate_projection_adapter_result(
            exact_binding, projected, registry,
        )
    except (projection_db_adapter.ProjectionAdapterError, OutcomeAdapterError,
            KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OutcomeAdapterError("STAGE8_AUTHORITATIVE_FACT_REPLAY_INVALID") from exc
    replay_transaction = replay.get("transaction")
    population = replay.get("population_receipt")
    if (type(replay_transaction) is not dict or type(population) is not dict
            or replay_ids != list(attempt_ids)
            or replay_transaction.get("transaction_identity_sha256")
            != transaction["transaction_identity_sha256"]
            or population.get("transaction_identity_sha256")
            != transaction["transaction_identity_sha256"]
            or population.get("query_count") != PROJECTION_REPLAY_QUERY_COUNT):
        raise OutcomeAdapterError("STAGE8_AUTHORITATIVE_FACT_REPLAY_SNAPSHOT_INVALID")
    durable_by_attempt = {row.get("attempt_id"): row for row in durable_facts}
    replay_by_attempt = {item.get("attempt_id"): item for item in ledger}
    replay_rows = replay.get("rows")
    if not isinstance(replay_rows, list):
        raise OutcomeAdapterError("STAGE8_AUTHORITATIVE_FACT_REPLAY_POPULATION_INVALID")
    replay_row_by_attempt = {
        item.get("attempt_id"): item for item in replay_rows
        if isinstance(item, Mapping)
    }
    if (set(durable_by_attempt) != set(attempt_ids)
            or set(replay_by_attempt) != set(attempt_ids)
            or set(replay_row_by_attempt) != set(attempt_ids)
            or len(durable_by_attempt) != len(attempt_ids)
            or len(replay_by_attempt) != len(attempt_ids)
            or len(replay_row_by_attempt) != len(attempt_ids)):
        raise OutcomeAdapterError("STAGE8_AUTHORITATIVE_FACT_REPLAY_POPULATION_INVALID")
    comparisons: list[dict[str, Any]] = []
    reasons_by_event: dict[int, list[str]] = {}
    complete = True
    for attempt_id in attempt_ids:
        durable = durable_by_attempt[attempt_id]
        replayed = replay_by_attempt[attempt_id]
        replay_row = replay_row_by_attempt[attempt_id]
        reasons: list[str] = []
        try:
            durable_fact, durable_class, durable_parent = _durable_fact_source_semantics(
                durable, exact_binding_sha256=exact_binding["binding_sha256"],
            )
            replay_fact, replay_class, replay_parent = _fact_source_semantics(
                fact=replayed["fact"],
                parent=replayed.get("parent_membership_evidence"),
                proof=replayed.get("noneligibility_proof"),
            )
            durable_fact_sha = contract.digest(durable_fact)
            replay_fact_sha = contract.digest(replay_fact)
            durable_parent_sha = contract.digest(durable_parent)
            replay_parent_sha = contract.digest(replay_parent)
            durable_selection_identity = (
                _validated_durable_selection_fact_identity(
                    durable, exact_binding,
                )
            )
            replay_selection_identity = _selection_fact_identity_payload(
                exact_binding,
                attempt_identity=replay_row.get("attempt_identity"),
                fact=replayed["fact"],
                parent=replayed.get("parent_membership_evidence"),
                proof=replayed.get("noneligibility_proof"),
            )
            durable_selection_identity_sha = contract.digest(
                durable_selection_identity
            )
            replay_selection_identity_sha = contract.digest(
                replay_selection_identity
            )
            if durable_fact_sha != replay_fact_sha:
                reasons.append("PROJECTED_FACT_CAUSAL_SEMANTICS_MISMATCH")
            if durable_class != replay_class or durable_parent_sha != replay_parent_sha:
                reasons.append("PARENT_CAUSAL_SEMANTICS_MISMATCH")
            if (durable_selection_identity_sha
                    != replay_selection_identity_sha):
                reasons.append("SELECTION_FACT_IDENTITY_REPLAY_MISMATCH")
        except (OutcomeAdapterError, KeyError, TypeError, ValueError, OverflowError):
            durable_fact_sha = replay_fact_sha = None
            durable_parent_sha = replay_parent_sha = None
            durable_selection_identity_sha = replay_selection_identity_sha = None
            durable_class = replay_class = "UNKNOWN"
            reasons.append("FACT_REPLAY_SEMANTICS_INVALID")
        if reasons:
            complete = False
            event_id = durable.get("event_id")
            if _positive_int64(event_id):
                reasons_by_event[event_id] = sorted(set(reasons))
        comparisons.append({
            "attempt_id": attempt_id,
            "durable_fact_record_sha256": durable.get("fact_record_sha256"),
            "durable_fact_sha256": durable.get("fact_sha256"),
            "replayed_fact_sha256": replayed.get("fact", {}).get("fact_sha256"),
            "durable_fact_semantic_sha256": durable_fact_sha,
            "replayed_fact_semantic_sha256": replay_fact_sha,
            "durable_parent_class": durable_class,
            "replayed_parent_class": replay_class,
            "durable_parent_semantic_sha256": durable_parent_sha,
            "replayed_parent_semantic_sha256": replay_parent_sha,
            "durable_selection_fact_identity_sha256":
                durable_selection_identity_sha,
            "replayed_selection_fact_identity_sha256":
                replay_selection_identity_sha,
            "status": "VERIFIED" if not reasons else "UNKNOWN",
            "reasons": sorted(set(reasons)),
        })
    receipt_unsigned = {
        "version": FACT_REPLAY_RECEIPT_VERSION,
        "status": "VERIFIED" if complete else "UNKNOWN",
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "selection_record_sha256": selection["selection_record_sha256"],
        "fact_batch_record_sha256": selection["fact_batch_record_sha256"],
        "transaction_identity_sha256": transaction["transaction_identity_sha256"],
        "projection_adapter_version": projection_db_adapter.VERSION,
        "projection_result_sha256": replay["result_sha256"],
        "projection_source_manifest_sha256": replay[
            "projection_source_manifest_sha256"
        ],
        "projection_population_receipt_sha256": replay[
            "population_receipt_sha256"
        ],
        "projection_authority_receipt_sha256": replay[
            "authority_receipt_sha256"
        ],
        "attempt_count": len(attempt_ids),
        "attempt_ids": list(attempt_ids),
        "attempt_population_sha256": contract.digest({
            "version": FACT_REPLAY_RECEIPT_VERSION,
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "attempt_ids": list(attempt_ids),
        }),
        "regenerated_fact_count": len(ledger),
        "all_causal_fact_semantics_verified": complete,
        "durable_selection_server_recomputed": False,
        "outcome_free": True,
        "complete": complete,
        "truncated": False,
        "comparisons": comparisons,
    }
    receipt_sha = contract.digest(receipt_unsigned)
    return ({**receipt_unsigned, "fact_replay_receipt_sha256": receipt_sha},
            complete, reasons_by_event)


def evaluate_selection_outcomes_from_connection(
    conn: Any, exact_binding: Mapping[str, Any], *,
    selection_record_sha256: str,
) -> dict[str, Any]:
    """Read one durable selection and return non-qualifying caller diagnostics."""
    started = time.monotonic()
    _assert_runtime_contract()
    source_manifest_before, source_manifest_sha_before = _source_manifest()
    try:
        contract.validate_exact_binding(exact_binding)
    except (TypeError, ValueError, KeyError) as exc:
        raise OutcomeAdapterError("STAGE8_OUTCOME_EXACT_BINDING_INVALID") from exc
    if not _valid_hash(selection_record_sha256):
        raise OutcomeAdapterError("STAGE8_OUTCOME_SELECTION_RECORD_SHA256_INVALID")
    reader = _Reader(conn, started=started)
    transaction_rows = reader.rows(_TRANSACTION_SQL)
    if len(transaction_rows) != 1:
        raise OutcomeAdapterError("STAGE8_OUTCOME_TRANSACTION_IDENTITY_INVALID")
    transaction = _transaction(transaction_rows[0], conn)

    rows = reader.rows(_REGISTRY_SELECTION_SQL, {
        "selection_record_sha256": selection_record_sha256,
        "exact_binding_sha256": exact_binding["binding_sha256"],
    })
    if not rows:
        raise LookupError("STAGE8_DURABLE_SELECTION_NOT_FOUND_FOR_EXACT_BINDING")
    if len(rows) != 1:
        raise OutcomeAdapterError("STAGE8_DURABLE_SELECTION_CARDINALITY_INVALID")
    registry = _decoded(rows[0].get("adapter_registry_json"),
                        reason="STAGE8_DURABLE_REGISTRY_INVALID")
    selection = _decoded(rows[0].get("adapter_selection_json"),
                         reason="STAGE8_DURABLE_SELECTION_INVALID")
    identities = _selection_envelope(
        registry, selection, exact_binding, selection_record_sha256,
    )

    batch_rows = reader.rows(_BATCH_SEAL_SQL, {
        "fact_batch_record_sha256": selection["fact_batch_record_sha256"],
        "exact_binding_sha256": exact_binding["binding_sha256"],
    })
    if len(batch_rows) != 1:
        raise OutcomeAdapterError("STAGE8_FACT_BATCH_OR_SEAL_NOT_EXACTLY_ONE")
    batch = _decoded(batch_rows[0].get("adapter_batch_json"),
                     reason="STAGE8_FACT_BATCH_INVALID")
    seal = _decoded(batch_rows[0].get("adapter_seal_json"),
                    reason="STAGE8_FACT_SEAL_INVALID")
    attempt_ids = _validate_batch_and_seal(batch, seal, registry, selection)

    fact_rows = reader.rows(_FACTS_SQL, {
        "fact_batch_record_sha256": batch["fact_batch_record_sha256"],
        "limit": MAX_FACTS + 1,
    })
    if len(fact_rows) > MAX_FACTS:
        raise OutcomeAdapterError("STAGE8_FACT_BATCH_EXCEEDS_BOUND")
    facts: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
    for raw in fact_rows:
        fact = _decoded(raw.get("adapter_fact_json"), reason="STAGE8_FACT_ROW_INVALID")
        event_raw = raw.get("adapter_event_json")
        event = None if event_raw is None else _decoded(
            event_raw, reason="STAGE8_EVENT_ROW_INVALID",
        )
        facts.append((fact, event))
    if ([fact.get("attempt_id") for fact, _ in facts] != attempt_ids
            or len({fact.get("fact_record_sha256") for fact, _ in facts}) != len(facts)
                or any(set(fact) != _FACT_VIEW_KEYS
                   or not _fact_record_valid(
                       fact, batch["fact_batch_record_sha256"], exact_binding,
                   )
                   for fact, _ in facts)):
        raise OutcomeAdapterError("STAGE8_SEALED_FACT_POPULATION_INVALID")
    fact_records_sha = contract.digest({
        "version": "stage8-durable-fact-record-set-v1",
        "fact_batch_record_sha256": batch["fact_batch_record_sha256"],
        "facts": [{"attempt_id": fact["attempt_id"],
                   "fact_record_sha256": fact["fact_record_sha256"]}
                  for fact, _ in facts],
    })
    if fact_records_sha != seal["fact_records_sha256"]:
        raise OutcomeAdapterError("STAGE8_FACT_SEAL_POPULATION_HASH_INVALID")

    fact_replay_receipt, fact_replay_verified, replay_reasons = (
        _caller_fact_replay(
            conn, exact_binding, attempt_ids=attempt_ids,
            durable_facts=[fact for fact, _ in facts], registry=registry,
            selection=selection, transaction=transaction,
        )
    )

    fact_matches: dict[int, list[tuple[dict[str, Any], dict[str, Any] | None]]] = {}
    for fact, event in facts:
        if fact.get("event_id") is not None:
            fact_matches.setdefault(fact["event_id"], []).append((fact, event))
    fact_audit: dict[int, dict[str, Any]] = {}
    events: dict[int, Mapping[str, Any] | None] = {}
    verified_selection_fact_identities: dict[int, str | None] = {}
    fact_reasons: dict[int, list[str]] = {}
    for identity in identities:
        matches = [item for item in fact_matches.get(identity["event_id"], [])
                   if item[0].get("selection_fact_identity_sha256")
                   == identity["expected_selection_fact_identity_sha256"]]
        if len(matches) != 1:
            reasons = (["DURABLE_REPRESENTATIVE_FACT_MISSING"] if not matches
                       else ["DURABLE_REPRESENTATIVE_FACT_DUPLICATED"])
            fact = event = None
        else:
            fact, event = matches[0]
            reasons = _representative_fact_reasons(
                identity, fact, event, registry, exact_binding,
                batch["fact_batch_record_sha256"],
            )
        structural_reasons = list(reasons)
        actual_selection_sha = (
            fact.get("selection_fact_identity_sha256") if fact else None
        )
        verified_selection_fact_identities[identity["event_id"]] = (
            actual_selection_sha
            if not structural_reasons
            and actual_selection_sha
            == identity["expected_selection_fact_identity_sha256"]
            else None
        )
        reasons.extend(replay_reasons.get(identity["event_id"], []))
        if not fact_replay_verified:
            reasons.append("SEALED_FACT_POPULATION_AUTHORITY_UNKNOWN")
        reasons = sorted(set(reasons))
        fact_reasons[identity["event_id"]] = reasons
        fact_audit[identity["event_id"]] = {
            "validation_status": "VALID" if not reasons else "UNKNOWN",
            "fact_record_sha256": fact.get("fact_record_sha256") if fact else None,
            "expected_selection_fact_identity_sha256": identity[
                "expected_selection_fact_identity_sha256"
            ],
            "selection_fact_identity_sha256": actual_selection_sha,
            "fact_sha256": fact.get("fact_sha256") if fact else None,
            "fact_row_sha256": _raw_hash(fact) if fact else None,
            "event_row_sha256": _raw_hash(event) if event else None,
            "reasons": reasons,
        }
        events[identity["event_id"]] = event

    representatives, provenance = _base_representatives(
        exact_binding, identities, registry, selection,
        verified_selection_fact_identities=verified_selection_fact_identities,
    )
    for identity, row in zip(identities, representatives):
        if fact_reasons[identity["event_id"]]:
            # A stored identity without exact durable fact/event backing is a
            # shared provenance failure, not a route-specific missing label.
            row["representative_status"] = "UNKNOWN"

    event_ids = [identity["event_id"] for identity in identities]
    query_limit = len(event_ids) + 1
    outcomes_raw = reader.rows(_OUTCOMES_SQL, {
        "event_ids": event_ids,
        "window_minutes": exact_binding["binding"]["window_minutes"],
        "threshold_bps": exact_binding["binding"]["threshold_bps"],
        "method_version": _STARTUP_MANIFEST["labels"]["method_version"],
        "limit": query_limit,
    })
    metrics_raw = reader.rows(_COMMON_METRICS_SQL, {
        "event_ids": event_ids,
        "window_minutes": exact_binding["binding"]["window_minutes"],
        "method_version": _COMMON_WINDOW_VERSION,
        "limit": query_limit,
    })
    outcomes = [_decoded(item.get("adapter_outcome_json"),
                         reason="STAGE8_ORDERED_V7_ROW_INVALID")
                for item in outcomes_raw]
    metrics = [_decoded(item.get("adapter_metric_json"),
                        reason="STAGE8_COMMON_WINDOW_ROW_INVALID")
               for item in metrics_raw]
    selected_event_ids = set(event_ids)
    if (any(not _positive_int64(item.get("event_id"))
            or item.get("event_id") not in selected_event_ids for item in outcomes)
            or any(not _positive_int64(item.get("event_id"))
                   or item.get("event_id") not in selected_event_ids
                   for item in metrics)):
        raise OutcomeAdapterError("STAGE8_OUTCOME_QUERY_POPULATION_INVALID")
    outcomes_by_event: dict[int, list[dict[str, Any]]] = {}
    metrics_by_event: dict[int, list[dict[str, Any]]] = {}
    for source, target in ((outcomes, outcomes_by_event), (metrics, metrics_by_event)):
        for item in source:
            target.setdefault(item.get("event_id"), []).append(item)

    evidence_entries: list[dict[str, Any]] = []
    for identity, row in zip(identities, representatives):
        # The durable selection row carries a *predicted* identity whose
        # digest is over the ``expected_*`` transport schema.  Evidence must
        # instead bind to the post-DB authoritative acceptance identity that
        # was reconstructed above with the trigger-owned
        # ``selection_fact_identity_sha256``.  Reusing the transport digest
        # here would make every otherwise-valid route fail closed because the
        # two schemas are intentionally different.
        identity_sha = row.get("representative_identity_sha256")
        probability, probability_audit = _outcome_evidence(
            exact_binding, row, identity_sha, events.get(identity["event_id"]),
            outcomes_by_event.get(identity["event_id"], []),
            as_of_utc=transaction["observed_at_utc"],
        )
        asymmetry, asymmetry_audit = _asymmetry_evidence(
            exact_binding, row, identity_sha, events.get(identity["event_id"]),
            metrics_by_event.get(identity["event_id"], []),
            as_of_utc=transaction["observed_at_utc"],
        )
        row["probability_evidence"] = probability
        row["asymmetry_evidence"] = asymmetry
        evidence_entries.append({
            "btc_parent_movement_id": identity["btc_parent_movement_id"],
            "parent_start_time_utc": identity["parent_start_time_utc"],
            "representative_identity_sha256": identity_sha,
            "event_id": identity["event_id"],
            "event_fingerprint": identity["event_fingerprint"],
            "expected_selection_fact_identity_sha256": identity[
                "expected_selection_fact_identity_sha256"
            ],
            "selection_fact_identity_sha256": row["representative"].get(
                "selection_fact_identity_sha256"
            ),
            "durable_representative_identity_sha256": contract.digest(identity),
            "fact": fact_audit[identity["event_id"]],
            "probability": probability_audit,
            "asymmetry": asymmetry_audit,
        })

    final_rows = reader.rows(_TRANSACTION_SQL)
    if len(final_rows) != 1:
        raise OutcomeAdapterError("STAGE8_OUTCOME_TRANSACTION_IDENTITY_INVALID")
    final_transaction = _transaction(final_rows[0], conn)
    if not _stable_transaction(transaction, final_transaction):
        raise OutcomeAdapterError("STAGE8_OUTCOME_TRANSACTION_CHANGED_DURING_READ")
    if reader.query_count != OUTCOME_QUERY_COUNT:
        raise OutcomeAdapterError("STAGE8_OUTCOME_QUERY_PLAN_CHANGED")
    if time.monotonic() - started > MAX_WALL_SECONDS:
        raise OutcomeAdapterError("STAGE8_OUTCOME_READ_WALL_BOUND_EXCEEDED")

    source_manifest, source_manifest_sha = _source_manifest()
    if (source_manifest != source_manifest_before
            or source_manifest_sha != source_manifest_sha_before):
        raise OutcomeAdapterError("STAGE8_OUTCOME_SOURCE_CODE_CHANGED_DURING_READ")
    probability_valid_count = sum(
        item["probability"]["validation_status"] == "VALID"
        for item in evidence_entries
    )
    asymmetry_valid_count = sum(
        item["asymmetry"]["validation_status"] == "VALID"
        for item in evidence_entries
    )
    all_fact_rows_valid = all(
        item["fact"]["validation_status"] == "VALID"
        for item in evidence_entries
    )
    evidence_unsigned = {
        "version": EVIDENCE_RECEIPT_VERSION,
        "adapter_version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "selection_record_sha256": selection_record_sha256,
        "fact_batch_record_sha256": batch["fact_batch_record_sha256"],
        "fact_seal_record_sha256": seal["seal_record_sha256"],
        "transaction_identity_sha256": transaction["transaction_identity_sha256"],
        "read_started_at_utc": transaction["observed_at_utc"],
        "read_finished_at_utc": final_transaction["observed_at_utc"],
        "query_count": MAX_QUERIES,
        "outcome_query_count": reader.query_count,
        "projection_replay_query_count": PROJECTION_REPLAY_QUERY_COUNT,
        "source_manifest_sha256": source_manifest_sha,
        "raw_source_hashes": {
            "registry_row_sha256": _raw_hash(registry),
            "selection_row_sha256": _raw_hash(selection),
            "fact_batch_row_sha256": _raw_hash(batch),
            "fact_seal_row_sha256": _raw_hash(seal),
            "sealed_fact_population_sha256": contract.digest([
                _raw_hash(fact) for fact, _ in facts
            ]),
        },
        "parent_count": len(evidence_entries),
        "btc_parent_movement_ids": [
            item["btc_parent_movement_id"] for item in evidence_entries
        ],
        "representatives": evidence_entries,
        "outcome_authority": "DURABLE_ROWS_READ_IN_CALLER_OWNED_RO_RR_SNAPSHOT",
        "fact_authority": (
            "SAME_SNAPSHOT_CAUSAL_SOURCE_REPLAY_VERIFIED"
            if fact_replay_verified else "SAME_SNAPSHOT_CAUSAL_SOURCE_REPLAY_UNKNOWN"
        ),
        "selection_population_read_complete": True,
        "all_representative_facts_valid": all_fact_rows_valid,
        "probability_evidence_valid_count": probability_valid_count,
        "asymmetry_evidence_valid_count": asymmetry_valid_count,
        "population_read_complete": True,
        "complete": True,
        "truncated": False,
    }
    evidence_sha = contract.digest(evidence_unsigned)
    evidence_receipt = {**evidence_unsigned, "evidence_receipt_sha256": evidence_sha}

    diagnostic = acceptance.evaluate(
        exact_binding, representatives, selection_provenance=provenance,
    )
    result = deepcopy(diagnostic)
    blockers = [item for item in result.get("qualification_blockers", [])
                if item != "DURABLE_REGISTRY_PERSISTENCE_NOT_VERIFIED"]
    # A caller-owned repeatable-read connection is useful for diagnostics but
    # cannot create a server-owned replay attestation or persist qualification.
    blockers.append("SERVER_DB_REPLAY_ATTESTATION_REQUIRED")
    if not fact_replay_verified:
        blockers.append("AUTHORITATIVE_FACT_SOURCE_REPLAY_NOT_VERIFIED")
    if not all_fact_rows_valid:
        blockers.append("DURABLE_FACT_IDENTITY_INTEGRITY_UNKNOWN")
    atomic_gate_passed = bool(diagnostic.get("atomic_gate_passed"))
    research_qualified = False
    if not atomic_gate_passed:
        blockers.append("DURABLE_OUTCOME_ACCEPTANCE_GATE_NOT_PASSED")
    result.update({
        "research_qualified": research_qualified,
        "status": diagnostic.get("status"),
        "qualification_blockers": sorted(set(blockers)),
        "durable_registry_persistence_verified": True,
        "registry_selection_persistence_verified": True,
        "authoritative_fact_replay_verified": False,
        "durable_outcome_source_read_verified": True,
        "durable_outcome_atomic_gate_evidence_verified": False,
        "durable_fact_source_authority_verified": False,
        "selection_record_sha256": selection_record_sha256,
        "evidence_receipt_sha256": evidence_sha,
        "fact_replay_receipt_sha256": fact_replay_receipt[
            "fact_replay_receipt_sha256"
        ],
        "result_scope": RESULT_SCOPE,
        "live_authorized": False,
        "telegram_authorized": False,
        "trade_authorized": False,
    })
    receipt = result.get("registry_selection_receipt")
    if isinstance(receipt, dict):
        receipt["persistence_verified_by_evaluator"] = True
        receipt["verification_boundary"] = (
            "OUTCOME_ADAPTER_VERIFIED_DURABLE_SELECTION_AND_SAME_SNAPSHOT_OUTCOMES"
        )
    evaluation_sha = contract.digest(result)
    persistence_unsigned = {
        "version": PERSISTENCE_PAYLOAD_VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "selection_record_sha256": selection_record_sha256,
        "outcome_adapter_version": VERSION,
        "outcome_adapter_source_sha256": source_manifest["files"][
            "research_stage8_outcome_db_adapter.py"
        ],
        "outcome_source_manifest": source_manifest,
        "outcome_source_manifest_sha256": source_manifest_sha,
        "transaction_identity_sha256": transaction["transaction_identity_sha256"],
        "fact_replay_receipt": fact_replay_receipt,
        "fact_replay_receipt_sha256": fact_replay_receipt[
            "fact_replay_receipt_sha256"
        ],
        "evidence_receipt": evidence_receipt,
        "evidence_receipt_sha256": evidence_sha,
        "evaluation": result,
        "evaluation_sha256": evaluation_sha,
        "result_scope": RESULT_SCOPE,
        "live_authorized": False,
        "telegram_authorized": False,
        "trade_authorized": False,
    }
    persistence_sha = contract.digest(persistence_unsigned)
    persistence_payload = {
        **persistence_unsigned,
        "persistence_payload_sha256": persistence_sha,
    }
    unsigned_result = {
        "version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "selection_record_sha256": selection_record_sha256,
        "transaction": transaction,
        "source_manifest": source_manifest,
        "source_manifest_sha256": source_manifest_sha,
        "fact_replay_receipt": fact_replay_receipt,
        "fact_replay_receipt_sha256": fact_replay_receipt[
            "fact_replay_receipt_sha256"
        ],
        "evidence_receipt": evidence_receipt,
        "evidence_receipt_sha256": evidence_sha,
        "evaluation": result,
        "evaluation_sha256": evaluation_sha,
        "persistence_payload": persistence_payload,
        "persistence_payload_sha256": persistence_sha,
        "research_qualified": research_qualified,
        "result_scope": RESULT_SCOPE,
        "live_authorized": False,
        "telegram_authorized": False,
        "trade_authorized": False,
    }
    return {**unsigned_result, "result_sha256": contract.digest(unsigned_result)}


__all__ = [
    "VERSION", "EVIDENCE_RECEIPT_VERSION", "FACT_REPLAY_RECEIPT_VERSION",
    "PERSISTENCE_PAYLOAD_VERSION", "MAX_REPRESENTATIVES", "MAX_FACTS",
    "OUTCOME_QUERY_COUNT", "PROJECTION_REPLAY_QUERY_COUNT", "MAX_QUERIES",
    "OutcomeAdapterError", "evaluate_selection_outcomes_from_connection",
]
