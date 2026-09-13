"""Durable, least-privilege boundary for Stage-8 experimental research.

Registration computes implementation and Watch-code hashes from fixed local
files and lets PostgreSQL assign the freeze time.  Selection and evaluation
receipts are append-only.  Registry/selection reads use one caller-owned,
read-only REPEATABLE READ transaction; a caller-supplied receipt is never an
authority token.  Research qualification can be persisted only from the
closed same-snapshot outcome-adapter payload; runtime authorizations remain
hard-false.

This module has no worker, deployment, Telegram, LIVE, or trading integration.
It never falls back to a generic application database URL.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from statistics import median

try:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - exercised by dependency-free selftests
    psycopg = None
    conninfo_to_dict = None
    dict_row = None

import research_operational_score_source_audit as source_audit
import research_stage8_acceptance as acceptance
import research_stage8_contract as contract
import research_stage8_coverage_receipt as coverage_receipt
import research_stage8_feature_projection as projection
import research_stage8_projection_db_adapter as projection_db_adapter
import research_stage8_representative_selector as selector
import research_watch_score_capture as watch_capture
import canonical_price_path
import research_common_window_metrics as common_window_metrics


VERSION = "stage8-durable-registry-adapter-v1"
REGISTRY_RECORD_VERSION = "stage8-durable-registry-record-v1"
REGISTRY_VERIFICATION_VERSION = "stage8-durable-registry-verification-v1"
ARTIFACT_MANIFEST_VERSION = "stage8-implementation-artifact-manifest-v1"
VERIFIER_PROFILE_VERSION = "stage8-durable-verifier-profile-v1"
SELECTION_RECORD_VERSION = "stage8-durable-selection-record-v1"
EVALUATION_RECORD_VERSION = "stage8-durable-evaluation-record-v1"
RESEARCH_RESULT_SCOPE = "EXPERIMENTAL_RESEARCH_ONLY"
OUTCOME_ADAPTER_VERSION = "stage8-durable-outcome-db-adapter-v1"
OUTCOME_PERSISTENCE_VERSION = (
    "stage8-authoritative-outcome-evaluation-persistence-v1"
)

REGISTRAR_ROLE = "research_stage8_registrar_v1"
SELECTOR_WRITER_ROLE = "research_stage8_selector_writer_v1"
FACT_WRITER_ROLE = "research_stage8_fact_writer_v1"
EVALUATOR_WRITER_ROLE = "research_stage8_evaluator_writer_v1"
READER_ROLE = "research_stage8_reader_v1"

_ROOT = Path(__file__).resolve().parent
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_SCHEMA_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
_INT64_MAX = 9223372036854775807
_DATABASE_SCHEMA_ENV = "RESEARCH_STAGE8_DATABASE_SCHEMA"
_ROLE_BY_DATABASE_URL = {
    "RESEARCH_STAGE8_REGISTRAR_DATABASE_URL": REGISTRAR_ROLE,
    "RESEARCH_STAGE8_FACT_DATABASE_URL": FACT_WRITER_ROLE,
    "RESEARCH_STAGE8_SELECTOR_DATABASE_URL": SELECTOR_WRITER_ROLE,
    "RESEARCH_STAGE8_EVALUATOR_DATABASE_URL": EVALUATOR_WRITER_ROLE,
    "RESEARCH_STAGE8_READER_DATABASE_URL": READER_ROLE,
}
_ARTIFACT_SPECS = {
    "canonical_price_path": (
        "canonical_price_path.py", canonical_price_path.METHOD_VERSION,
    ),
    "common_window_metrics": (
        "research_common_window_metrics.py", common_window_metrics.METHOD_VERSION,
    ),
    "contract": ("research_stage8_contract.py", contract.VERSION),
    "coverage_receipt": ("research_stage8_coverage_receipt.py", coverage_receipt.VERSION),
    "source_audit": ("research_operational_score_source_audit.py", source_audit.VERSION),
    "watch_capture": ("research_watch_score_capture.py", watch_capture.VERSION),
    "projection": ("research_stage8_feature_projection.py", projection.VERSION),
    "projection_db_adapter": (
        "research_stage8_projection_db_adapter.py", projection_db_adapter.VERSION,
    ),
    # Literal declaration avoids importing the outcome adapter, which itself
    # imports this registry module for durable read-side validation.
    "outcome_db_adapter": (
        "research_stage8_outcome_db_adapter.py",
        "stage8-durable-outcome-db-adapter-v1",
    ),
    "registry_adapter": ("research_stage8_registry.py", VERSION),
    "registry_migration": (
        "migrations/051_stage8_durable_registry.sql", "051-stage8-durable-registry-v1",
    ),
    "selector": ("research_stage8_representative_selector.py", selector.VERSION),
    "acceptance": ("research_stage8_acceptance.py", acceptance.VERSION),
}
_EXPECTED_VERSIONS = {
    "canonical_price_path": "canonical-spot-1m-ohlc-path-v3",
    "common_window_metrics": "common-window-spot-1m-v1",
    "contract": "stage8-operational-model-contract-v1",
    "coverage_receipt": "stage8-bounded-coverage-receipt-v1",
    "source_audit": "operational-score-source-audit-v2",
    "watch_capture": "watch-operational-scores-v2",
    "projection": "stage8-watch-signed-model-sidecar-v1",
    "projection_db_adapter": "stage8-projection-postgres-adapter-v1",
    "outcome_db_adapter": "stage8-durable-outcome-db-adapter-v1",
    "registry_adapter": "stage8-durable-registry-adapter-v1",
    "registry_migration": "051-stage8-durable-registry-v1",
    "selector": "stage8-outcome-blind-representative-selector-v1",
    "acceptance": "stage8-experimental-acceptance-evaluator-v1",
}
_RUNTIME_VERSION_GETTERS = {
    "canonical_price_path": lambda: canonical_price_path.METHOD_VERSION,
    "common_window_metrics": lambda: common_window_metrics.METHOD_VERSION,
    "contract": lambda: contract.VERSION,
    "coverage_receipt": lambda: coverage_receipt.VERSION,
    "source_audit": lambda: source_audit.VERSION,
    "watch_capture": lambda: watch_capture.VERSION,
    "projection": lambda: projection.VERSION,
    "projection_db_adapter": lambda: projection_db_adapter.VERSION,
    "outcome_db_adapter": lambda: "stage8-durable-outcome-db-adapter-v1",
    "registry_adapter": lambda: VERSION,
    "registry_migration": lambda: "051-stage8-durable-registry-v1",
    "selector": lambda: selector.VERSION,
    "acceptance": lambda: acceptance.VERSION,
}
_WATCH_CODE_FILES = (
    "alert_engine.py",
    "coinglass_flow_engine.py",
    "coinglass_oi_regime_service.py",
    "live_price_provider.py",
    "market_confidence_engine.py",
    "time_family_engine.py",
)
_SERVER_FIELDS = frozenset({
    "frozen_at_utc", "freeze_id", "registry_record", "registry_record_sha256",
    "registered_by", "persisted_at_utc", "persisted_by",
})
_SELECTOR_BATCH_KEYS = frozenset({
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
    "outcome_blind_selection", "outcome_or_label_fields_accepted", "truncated",
    "global_blockers", "blocked_parents", "excluded_pre_freeze_parent_ids",
    "proven_noneligible_attempt_ids", "deduplicated_exact_anchor_event_count",
    "qualification_evaluated", "database_verification_asserted_by_selector",
})
_OUTCOME_PERSISTENCE_KEYS = frozenset({
    "version", "manifest_sha256", "exact_binding_sha256",
    "selection_record_sha256", "outcome_adapter_version",
    "outcome_adapter_source_sha256", "outcome_source_manifest",
    "outcome_source_manifest_sha256", "transaction_identity_sha256",
    "fact_replay_receipt", "fact_replay_receipt_sha256",
    "evidence_receipt", "evidence_receipt_sha256", "evaluation",
    "evaluation_sha256", "result_scope", "live_authorized",
    "telegram_authorized", "trade_authorized", "persistence_payload_sha256",
})
_FACT_REPLAY_KEYS = frozenset({
    "version", "status", "manifest_sha256", "exact_binding_sha256",
    "selection_record_sha256", "fact_batch_record_sha256",
    "transaction_identity_sha256", "projection_adapter_version",
    "projection_result_sha256", "projection_source_manifest_sha256",
    "projection_population_receipt_sha256",
    "projection_authority_receipt_sha256", "attempt_count", "attempt_ids",
    "attempt_population_sha256", "regenerated_fact_count",
    "all_causal_fact_semantics_verified",
    "durable_selection_server_recomputed", "outcome_free", "complete",
    "truncated", "comparisons", "fact_replay_receipt_sha256",
})
_OUTCOME_EVIDENCE_KEYS = frozenset({
    "version", "adapter_version", "manifest_sha256", "exact_binding_sha256",
    "selection_record_sha256", "fact_batch_record_sha256",
    "fact_seal_record_sha256", "transaction_identity_sha256",
    "read_started_at_utc", "read_finished_at_utc", "query_count",
    "outcome_query_count", "projection_replay_query_count",
    "source_manifest_sha256", "raw_source_hashes", "parent_count",
    "btc_parent_movement_ids", "representatives", "outcome_authority",
    "fact_authority", "selection_population_read_complete",
    "all_representative_facts_valid", "probability_evidence_valid_count",
    "asymmetry_evidence_valid_count", "population_read_complete", "complete",
    "truncated", "evidence_receipt_sha256",
})
_AUTHORITATIVE_EVALUATION_KEYS = frozenset({
    "evaluator_version", "manifest_sha256", "exact_binding_sha256",
    "acceptance_policy_version", "acceptance_policy_sha256",
    "registry_selection_receipt", "atomic_expression", "common", "routes",
    "atomic_gate_passed", "structurally_eligible", "research_qualified",
    "qualification_blockers", "status", "maximum_result",
    "excluded_representatives", "representative_rows_received",
    "delivery_status_required", "fresh_three_parent_route", "live_effect",
    "trade_execution_effect", "durable_registry_persistence_verified",
    "registry_selection_persistence_verified", "authoritative_fact_replay_verified",
    "durable_outcome_source_read_verified",
    "durable_outcome_atomic_gate_evidence_verified",
    "durable_fact_source_authority_verified", "selection_record_sha256",
    "evidence_receipt_sha256", "fact_replay_receipt_sha256", "result_scope",
    "live_authorized", "telegram_authorized", "trade_authorized",
})
_OUTCOME_SOURCE_FILES = frozenset({
    "canonical_price_path.py", "research_common_window_metrics.py",
    "research_operational_score_source_audit.py", "research_stage8_acceptance.py",
    "research_stage8_contract.py", "research_stage8_feature_projection.py",
    "research_stage8_outcome_db_adapter.py",
    "research_stage8_projection_db_adapter.py", "research_stage8_registry.py",
    "research_stage8_representative_selector.py",
})
_EVIDENCE_REPRESENTATIVE_KEYS = frozenset({
    "btc_parent_movement_id", "parent_start_time_utc",
    "representative_identity_sha256", "event_id", "event_fingerprint",
    "expected_selection_fact_identity_sha256",
    "selection_fact_identity_sha256",
    "durable_representative_identity_sha256", "fact", "probability",
    "asymmetry",
})
_FACT_AUDIT_KEYS = frozenset({
    "validation_status", "fact_record_sha256",
    "expected_selection_fact_identity_sha256",
    "selection_fact_identity_sha256", "fact_sha256", "fact_row_sha256",
    "event_row_sha256", "reasons",
})
_PROBABILITY_AUDIT_KEYS = frozenset({
    "source_status", "reported_status", "validation_status",
    "source_row_sha256", "reasons",
})
_ASYMMETRY_AUDIT_KEYS = frozenset({
    "source_status", "validation_status", "source_row_sha256", "mfe_pct",
    "mae_pct", "zero_denominator", "reasons",
})
_FACT_REPLAY_COMPARISON_KEYS = frozenset({
    "attempt_id", "durable_fact_record_sha256", "durable_fact_sha256",
    "replayed_fact_sha256", "durable_fact_semantic_sha256",
    "replayed_fact_semantic_sha256", "durable_parent_class",
    "replayed_parent_class", "durable_parent_semantic_sha256",
    "replayed_parent_semantic_sha256",
    "durable_selection_fact_identity_sha256",
    "replayed_selection_fact_identity_sha256", "status", "reasons",
})
_RAW_SOURCE_HASH_KEYS = frozenset({
    "registry_row_sha256", "selection_row_sha256", "fact_batch_row_sha256",
    "fact_seal_row_sha256", "sealed_fact_population_sha256",
})
_SERVER_REPLAY_ATTESTATION_KEYS = frozenset({
    "version", "exact_binding_sha256", "fact_batch_record_sha256",
    "selection_record_sha256", "attempt_count", "entries",
    "selected_representatives_verified", "all_projection_semantics_verified",
})
_SERVER_REPLAY_ENTRY_KEYS = frozenset({
    "attempt_id", "persisted_projection_attestation_sha256",
    "recomputed_projection_attestation_sha256", "projection_semantics_sha256",
    "derived_knowledge_status", "derived_candidate_match",
    "derived_parent_authority_class", "status",
})
_EVALUATION_RECORD_KEYS = frozenset({
    "version", "exact_binding_sha256", "selection_record_sha256",
    "outcome_adapter_version", "observed_outcome_adapter_source_sha256",
    "outcome_source_manifest_sha256", "transaction_identity_sha256",
    "fact_replay_receipt_sha256", "evidence_receipt_sha256",
    "caller_evaluation_sha256", "evaluation_sha256",
    "persistence_payload_sha256", "server_replay_attestation_sha256",
    "server_replay_verified", "atomic_gate_passed", "research_qualified",
    "result_scope", "live_authorized", "telegram_authorized",
    "trade_authorized", "persisted_at_utc", "persisted_by",
})
_REGISTRY_SELECTION_RECEIPT_KEYS = frozenset({
    "registration_evidence", "freeze_id", "frozen_at_utc",
    "registry_record_sha256", "registry_verification_receipt_sha256",
    "cohort_query_sha256", "population_receipt_sha256",
    "source_high_water_attempt_id", "representative_count",
    "representative_set_sha256", "attestation_sha256",
    "attestation_structurally_valid", "persistence_verified_by_evaluator",
    "verification_boundary",
})


def _json(value: Any) -> str:
    return contract.canonical(value)


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH.fullmatch(value) is not None


def _file_sha256(name: str) -> str:
    path = (_ROOT / name).resolve()
    if (not path.is_file() or _ROOT != path and _ROOT not in path.parents
            or name.startswith("/") or ".." in Path(name).parts):
        raise RuntimeError("STAGE8_IMPLEMENTATION_ARTIFACT_MISSING:" + name)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def implementation_artifacts() -> dict[str, Any]:
    """Hash fixed implementation files; no caller-selected path is accepted."""
    for key, expected in _EXPECTED_VERSIONS.items():
        if (_ARTIFACT_SPECS[key][1] != expected
                or _RUNTIME_VERSION_GETTERS[key]() != expected):
            raise RuntimeError("STAGE8_RUNTIME_VERSION_DRIFT:" + key)
    value = {
        "version": ARTIFACT_MANIFEST_VERSION,
        "files": {
            key: {"path": path, "version": version, "sha256": _file_sha256(path)}
            for key, (path, version) in sorted(_ARTIFACT_SPECS.items())
        },
    }
    # Strict canonicalization also rejects any accidental non-JSON value.
    contract.digest(value)
    return value


def expected_watch_code_manifest() -> dict[str, str]:
    """Freeze the producer files themselves, not a hash copied from a fact."""
    return {name: _file_sha256(name) for name in _WATCH_CODE_FILES}


def _iso_utc(value: Any) -> str:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("STAGE8_TIMESTAMP_REQUIRES_OFFSET")
    return parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


def _row(cursor: Any, raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    names = [item.name if hasattr(item, "name") else item[0]
             for item in cursor.description]
    return dict(zip(names, raw))


def _query_one(conn: Any, statement: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
    cursor = conn.execute(statement, tuple(params))
    return _row(cursor, cursor.fetchone())


def _transaction_characteristics(conn: Any) -> dict[str, str]:
    return {
        "read_only": str(_query_one(conn, "SHOW transaction_read_only")["transaction_read_only"]),
        "isolation": str(_query_one(conn, "SHOW transaction_isolation")["transaction_isolation"]),
    }


def _require_verified_read_transaction(conn: Any) -> None:
    state = _transaction_characteristics(conn)
    if state["read_only"].lower() not in {"on", "true"}:
        raise RuntimeError("STAGE8_REGISTRY_READ_REQUIRES_READ_ONLY_TRANSACTION")
    if state["isolation"].lower().replace("_", " ") != "repeatable read":
        raise RuntimeError("STAGE8_REGISTRY_READ_REQUIRES_REPEATABLE_READ")


def _database_url(name: str) -> str:
    # Deliberately no DATABASE_URL / RESEARCH_DATABASE_URL fallback.
    return os.getenv(name, "").strip()


def _connect(name: str, *, read_only: bool):
    url = _database_url(name)
    if not url:
        raise RuntimeError("STAGE8_DATABASE_URL_NOT_EXPLICITLY_CONFIGURED:" + name)
    if psycopg is None or conninfo_to_dict is None:
        raise RuntimeError("psycopg is unavailable")
    try:
        parameters = coverage_receipt._explicit_connection_fields(
            url, conninfo_to_dict,
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError("STAGE8_DATABASE_TARGET_NOT_FULLY_EXPLICIT:" + name) from exc
    trusted_schema = os.getenv(_DATABASE_SCHEMA_ENV, "").strip()
    if not _valid_trusted_schema_name(trusted_schema):
        raise RuntimeError(
            "STAGE8_DATABASE_SCHEMA_NOT_EXPLICITLY_CONFIGURED:"
            + _DATABASE_SCHEMA_ENV
        )
    expected_role = _ROLE_BY_DATABASE_URL.get(name)
    if expected_role is None:
        raise RuntimeError("STAGE8_DATABASE_ENTRYPOINT_UNKNOWN:" + name)
    options = "-c statement_timeout=10000 -c lock_timeout=1000 -c TimeZone=UTC"
    if read_only:
        options += " -c default_transaction_read_only=on"
    conn = psycopg.connect(
        **parameters, row_factory=dict_row, connect_timeout=5, options=options,
        prepare_threshold=None,
    )
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    conn.read_only = read_only
    _pin_trusted_schema(
        conn, expected_role=expected_role, trusted_schema=trusted_schema,
    )
    return conn


def _valid_trusted_schema_name(value: Any) -> bool:
    return (
        isinstance(value, str)
        and _SCHEMA_IDENTIFIER.fullmatch(value) is not None
        and value.lower() not in {
            "pg_catalog", "information_schema", "pg_temp",
        }
        and not value.lower().startswith("pg_temp_")
    )


def _pin_trusted_schema(
    conn: Any, *, expected_role: str, trusted_schema: str | None = None,
) -> str:
    """Pin one non-writable Stage-8 schema before any relation is touched.

    Environment wrappers supply the schema explicitly.  Caller-owned
    ``*_from_connection`` APIs may use the connection's one configured
    application schema, but never an implicit/explicit temp namespace.  The
    explicit ``pg_temp`` tail is essential: omitting it makes PostgreSQL search
    the temporary schema *before* named schemas for relations and types.
    """
    state = _query_one(conn, """
        SELECT current_user::text AS current_user,
               pg_catalog.current_schema()::text AS current_schema,
               pg_catalog.current_schemas(false) AS explicit_schemas
    """)
    if state is None or state.get("current_user") != expected_role:
        raise RuntimeError("STAGE8_DATABASE_ROLE_MISMATCH:" + expected_role)
    explicit_schemas = [str(value) for value in state.get("explicit_schemas") or []]
    if trusted_schema is None:
        application_schemas = [
            value for value in explicit_schemas
            if value not in {"pg_catalog", "information_schema", "pg_temp"}
            and not value.startswith("pg_temp_")
        ]
        if len(application_schemas) != 1:
            raise RuntimeError("STAGE8_TRUSTED_SCHEMA_NOT_UNAMBIGUOUS")
        trusted_schema = application_schemas[0]
    if not _valid_trusted_schema_name(trusted_schema):
        raise RuntimeError("STAGE8_TRUSTED_SCHEMA_INVALID")
    quoted_schema = '"' + trusted_schema.replace('"', '""') + '"'
    pinned_path = quoted_schema + ",pg_catalog,pg_temp"
    conn.execute(
        "SELECT pg_catalog.set_config('search_path', %s, true)",
        (pinned_path,),
    )
    qualified_registry = quoted_schema + ".research_stage8_binding_registry"
    verified = _query_one(conn, """
        SELECT pg_catalog.current_schema()::text AS current_schema,
               pg_catalog.current_schemas(true) AS resolved_schemas,
               pg_catalog.has_schema_privilege(
                   current_user, %s, 'USAGE') AS schema_usage,
               pg_catalog.has_schema_privilege(
                   current_user, %s, 'CREATE') AS schema_create,
               pg_catalog.to_regclass(%s)::oid AS qualified_registry_oid,
               pg_catalog.to_regclass(
                   'research_stage8_binding_registry')::oid
                    AS unqualified_registry_oid
    """, (trusted_schema, trusted_schema, qualified_registry))
    resolved = [str(value) for value in verified.get("resolved_schemas") or []]
    temp_positions = [
        index for index, value in enumerate(resolved)
        if value == "pg_temp" or value.startswith("pg_temp_")
    ]
    if (verified.get("current_schema") != trusted_schema
            or verified.get("schema_usage") is not True
            or verified.get("schema_create") is not False
            or verified.get("qualified_registry_oid") is None
            or verified.get("qualified_registry_oid")
                != verified.get("unqualified_registry_oid")
            or not resolved or resolved[0] != trusted_schema
            or any(index < resolved.index(trusted_schema)
                   for index in temp_positions)):
        raise RuntimeError("STAGE8_TRUSTED_SCHEMA_VERIFICATION_FAILED")
    return trusted_schema


def _require_env_role(conn: Any, expected: str) -> None:
    row = _query_one(conn, "SELECT current_user AS current_user")
    if row is None or row["current_user"] != expected:
        raise RuntimeError("STAGE8_DATABASE_ROLE_MISMATCH:" + expected)


def _registry_row_from_connection(
    conn: Any, exact_binding_sha256: str,
) -> dict[str, Any] | None:
    return _query_one(conn, """
        SELECT * FROM research_stage8_registry_read_v1
        WHERE exact_binding_sha256 = %s
    """, (exact_binding_sha256,))


def _validate_registry_row(
    row: Mapping[str, Any], exact_binding: Mapping[str, Any], *,
    require_current_implementation: bool,
) -> dict[str, Any]:
    contract.validate_exact_binding(exact_binding)
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    try:
        exact_match = contract.canonical(row["exact_binding"]) == contract.canonical(exact_binding)
        record_valid = (
            row["registry_record_sha256"] == contract.digest(row["registry_record"])
            and row["implementation_artifacts_sha256"]
            == contract.digest(row["implementation_artifacts"])
            and row["expected_watch_code_manifest_sha256"]
            == contract.digest(row["expected_watch_code_manifest"])
            and row["verifier_profile_sha256"] == contract.digest(row["verifier_profile"])
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("STAGE8_DURABLE_REGISTRY_ROW_INVALID") from exc
    expected_axes = {
        "exact_binding_sha256": exact_binding["binding_sha256"],
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
        "scope_id": exact_binding["binding"]["scope"]["scope_id"],
        "candidate_id": exact_binding["binding"]["candidate"]["candidate_id"],
        "window_minutes": exact_binding["binding"]["window_minutes"],
        "threshold_bps": exact_binding["binding"]["threshold_bps"],
    }
    if (not exact_match or not record_valid
            or any(row.get(key) != value for key, value in expected_axes.items())
            or not _valid_hash(row.get("freeze_id"))
            or not _valid_hash(row.get("registry_record_sha256"))
            or not _valid_hash(row.get("verifier_profile_sha256"))):
        raise ValueError("STAGE8_DURABLE_REGISTRY_ROW_INVALID")
    if require_current_implementation:
        current_artifacts = implementation_artifacts()
        current_watch = expected_watch_code_manifest()
        if (contract.canonical(row["implementation_artifacts"])
                != contract.canonical(current_artifacts)
                or contract.canonical(row["expected_watch_code_manifest"])
                != contract.canonical(current_watch)):
            raise ValueError("STAGE8_REGISTERED_IMPLEMENTATION_NO_LONGER_CURRENT")
    result = deepcopy(dict(row))
    result["frozen_at_utc"] = _iso_utc(row["frozen_at_utc"])
    return result


def register_exact_binding_from_connection(
    conn: Any, exact_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Register one exact binding; PostgreSQL alone supplies the freeze time."""
    _pin_trusted_schema(conn, expected_role=REGISTRAR_ROLE)
    contract.validate_exact_binding(exact_binding)
    artifacts = implementation_artifacts()
    watch_manifest = expected_watch_code_manifest()
    conn.execute("""
        INSERT INTO research_stage8_binding_registry
            (exact_binding, implementation_artifacts, expected_watch_code_manifest)
        VALUES (%s::jsonb, %s::jsonb, %s::jsonb)
        ON CONFLICT (exact_binding_sha256) DO NOTHING
    """, (_json(exact_binding), _json(artifacts), _json(watch_manifest)))
    row = _registry_row_from_connection(conn, exact_binding["binding_sha256"])
    if row is None:
        raise RuntimeError("STAGE8_REGISTRY_INSERT_NOT_VISIBLE")
    return _validate_registry_row(row, exact_binding, require_current_implementation=True)


def register_exact_binding(exact_binding: Mapping[str, Any]) -> dict[str, Any]:
    """Environment entrypoint restricted to the dedicated registrar role."""
    with _connect("RESEARCH_STAGE8_REGISTRAR_DATABASE_URL", read_only=False) as conn:
        _require_env_role(conn, REGISTRAR_ROLE)
        result = register_exact_binding_from_connection(conn, exact_binding)
        conn.commit()
        return result


def registry_reference_from_connection(
    conn: Any, exact_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Emit a selector reference from a real read-only RR snapshot.

    The returned checksum is audit evidence, not a bearer capability.  The
    trusted acceptance adapter never accepts this dictionary as an input and
    instead performs the same durable read itself.
    """
    _pin_trusted_schema(conn, expected_role=READER_ROLE)
    _require_verified_read_transaction(conn)
    contract.validate_exact_binding(exact_binding)
    row = _registry_row_from_connection(conn, exact_binding["binding_sha256"])
    if row is None:
        raise LookupError("STAGE8_EXACT_BINDING_NOT_DURABLY_REGISTERED")
    registry = _validate_registry_row(
        row, exact_binding, require_current_implementation=True,
    )
    snapshot = _query_one(conn, """
        SELECT txid_current_snapshot()::text AS database_snapshot_id,
               clock_timestamp() AS read_started_at_utc,
               current_user AS database_role
    """)
    artifacts = registry["implementation_artifacts"]["files"]
    unsigned = {
        "version": REGISTRY_VERIFICATION_VERSION,
        "status": "VERIFIED_READ_ONLY_REPEATABLE_READ",
        "transaction_read_only": True,
        "transaction_isolation": "REPEATABLE READ",
        "database_snapshot_id": snapshot["database_snapshot_id"],
        "read_started_at_utc": _iso_utc(snapshot["read_started_at_utc"]),
        "database_role": snapshot["database_role"],
        "exact_binding_sha256": registry["exact_binding_sha256"],
        "manifest_sha256": registry["manifest_sha256"],
        "freeze_id": registry["freeze_id"],
        "frozen_at_utc": registry["frozen_at_utc"],
        "registry_record_sha256": registry["registry_record_sha256"],
        "verifier_profile_sha256": registry["verifier_profile_sha256"],
        "expected_projection_source_sha256": artifacts["projection"]["sha256"],
        "expected_selector_source_sha256": artifacts["selector"]["sha256"],
        "expected_watch_code_manifest_sha256": registry[
            "expected_watch_code_manifest_sha256"
        ],
    }
    verification_sha = contract.digest(unsigned)
    # Exact closed shape consumed by the pure selector.
    return {
        "status": unsigned["status"],
        "exact_binding_sha256": unsigned["exact_binding_sha256"],
        "manifest_sha256": unsigned["manifest_sha256"],
        "freeze_id": unsigned["freeze_id"],
        "frozen_at_utc": unsigned["frozen_at_utc"],
        "registry_record_sha256": unsigned["registry_record_sha256"],
        "registry_verification_receipt_sha256": verification_sha,
        "verifier_profile_sha256": unsigned["verifier_profile_sha256"],
        "expected_projection_source_sha256": unsigned[
            "expected_projection_source_sha256"
        ],
        "expected_selector_source_sha256": unsigned[
            "expected_selector_source_sha256"
        ],
        "expected_watch_code_manifest_sha256": unsigned[
            "expected_watch_code_manifest_sha256"
        ],
    }


def registry_reference(exact_binding: Mapping[str, Any]) -> dict[str, Any]:
    """Environment entrypoint restricted to the dedicated SELECT-only role."""
    with _connect("RESEARCH_STAGE8_READER_DATABASE_URL", read_only=True) as conn:
        _require_env_role(conn, READER_ROLE)
        return registry_reference_from_connection(conn, exact_binding)


def _validate_registry_reference_input(
    value: Mapping[str, Any], registry: Mapping[str, Any],
) -> None:
    expected = {
        "status": "VERIFIED_READ_ONLY_REPEATABLE_READ",
        "exact_binding_sha256": registry["exact_binding_sha256"],
        "manifest_sha256": registry["manifest_sha256"],
        "freeze_id": registry["freeze_id"],
        "frozen_at_utc": registry["frozen_at_utc"],
        "registry_record_sha256": registry["registry_record_sha256"],
        "verifier_profile_sha256": registry["verifier_profile_sha256"],
        "expected_projection_source_sha256": registry["implementation_artifacts"]
            ["files"]["projection"]["sha256"],
        "expected_selector_source_sha256": registry["implementation_artifacts"]
            ["files"]["selector"]["sha256"],
        "expected_watch_code_manifest_sha256": registry[
            "expected_watch_code_manifest_sha256"
        ],
    }
    if (not isinstance(value, Mapping) or set(value) != set(expected) | {
            "registry_verification_receipt_sha256"
        } or any(value.get(key) != expected_value
                 for key, expected_value in expected.items())
            or not _valid_hash(value.get("registry_verification_receipt_sha256"))):
        raise ValueError("STAGE8_REGISTRY_REFERENCE_INPUT_INVALID")


def _validate_outcome_free_coverage(
    value: Mapping[str, Any], *, attempt_ids: Sequence[int],
    exact_binding: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("STAGE8_OUTCOME_FREE_COVERAGE_RECEIPT_INVALID")
    receipt = deepcopy(dict(value))
    full_hash = receipt.pop("receipt_sha256", None)
    if not _valid_hash(full_hash) or contract.digest(receipt) != full_hash:
        raise ValueError("STAGE8_FULL_COVERAGE_AUDIT_REFERENCE_INVALID")
    payload = coverage_receipt.outcome_free_population_payload(value)
    outcome_free_hash = value.get("outcome_free_population_receipt_sha256")
    query = payload.get("query_scope")
    if (not _valid_hash(outcome_free_hash)
            or contract.digest(payload) != outcome_free_hash
            or payload.get("version")
            != coverage_receipt.OUTCOME_FREE_POPULATION_RECEIPT_VERSION
            or payload.get("status") != "COMPLETE_BOUNDED_COHORT"
            or payload.get("manifest_sha256") != contract.MANIFEST_SHA256
            or payload.get("manifest_axes_verified") is not True
            or payload.get("database_connection_attempted") is not True
            or payload.get("stop_reason") is not None
            or not isinstance(query, Mapping)
            or query.get("symbols") != sorted(exact_binding["binding"]["scope"]["symbols"])
            or payload.get("attempts_examined") != len(attempt_ids)
            or payload.get("high_water_attempt_id") != (attempt_ids[-1] if attempt_ids else 0)
            or not _valid_hash(payload.get("transaction_identity_sha256"))):
        raise ValueError("STAGE8_OUTCOME_FREE_COVERAGE_RECEIPT_INVALID")
    population = {
        "version": "stage8-bounded-attempt-id-population-v1",
        "manifest_sha256": contract.MANIFEST_SHA256,
        "query_sha256": query.get("query_sha256"),
        "transaction_identity_sha256": payload["transaction_identity_sha256"],
        "high_water_attempt_id": payload["high_water_attempt_id"],
        "attempt_ids": list(attempt_ids),
    }
    if (payload.get("attempt_population_version") != population["version"]
            or payload.get("attempt_population_sha256") != contract.digest(population)):
        raise ValueError("STAGE8_COVERAGE_ATTEMPT_POPULATION_INVALID")
    return {
        "query_scope": deepcopy(dict(query)),
        "query_sha256": query["query_sha256"],
        "outcome_free_population_receipt_sha256": outcome_free_hash,
        "attempt_population_sha256": payload["attempt_population_sha256"],
        "source_high_water_attempt_id": payload["high_water_attempt_id"],
        "full_audit_receipt_sha256": full_hash,
    }


def _validate_projection_adapter_result(
    exact_binding: Mapping[str, Any], value: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> tuple[dict[str, Any], list[int], list[dict[str, Any]]]:
    if not isinstance(value, Mapping):
        raise ValueError("STAGE8_PROJECTION_ADAPTER_RESULT_INVALID")
    result = deepcopy(dict(value))
    supplied_result_hash = result.pop("result_sha256", None)
    if not _valid_hash(supplied_result_hash) or contract.digest(result) != supplied_result_hash:
        raise ValueError("STAGE8_PROJECTION_ADAPTER_RESULT_HASH_INVALID")
    source_manifest = result.get("projection_source_manifest")
    population = result.get("population_receipt")
    authority_receipt = result.get("authority_receipt")
    rows = result.get("rows")
    query = result.get("query_scope")
    artifacts = registry["implementation_artifacts"]["files"]
    expected_source_files = {
        "research_stage8_contract.py": artifacts["contract"]["sha256"],
        "research_stage8_feature_projection.py": artifacts["projection"]["sha256"],
        "research_stage8_projection_db_adapter.py": artifacts[
            "projection_db_adapter"
        ]["sha256"],
        "research_operational_score_source_audit.py": artifacts["source_audit"]["sha256"],
        "research_stage8_representative_selector.py": artifacts["selector"]["sha256"],
        "research_watch_score_capture.py": artifacts["watch_capture"]["sha256"],
    }
    if (result.get("version") != projection_db_adapter.VERSION
            or result.get("manifest_sha256") != contract.MANIFEST_SHA256
            or result.get("projection_version") != projection.VERSION
            or result.get("source_audit_version") != source_audit.VERSION
            or result.get("projection_mode")
            != projection_db_adapter.EXACT_BINDING_PROJECTION_MODE
            or result.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or not isinstance(source_manifest, Mapping)
            or contract.canonical(source_manifest) != contract.canonical(expected_source_files)
            or result.get("projection_source_manifest_sha256")
            != contract.digest(source_manifest)
            or not isinstance(query, Mapping)
            or query.get("projection_mode")
            != projection_db_adapter.EXACT_BINDING_PROJECTION_MODE
            or query.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or query.get("population_kind")
            != "EXACT_BINDING_FULL_COHORT_ATTEMPT_IDS"
            or query.get("max_attempts")
            != projection_db_adapter.MAX_EXACT_BINDING_ATTEMPTS
            or result.get("query_binding_sha256") != contract.digest(query)
            or not isinstance(population, Mapping)
            or not isinstance(authority_receipt, Mapping)
            or not isinstance(rows, list)):
        raise ValueError("STAGE8_PROJECTION_ADAPTER_RESULT_INVALID")
    query_ids = query.get("requested_attempt_ids")
    if (not isinstance(query_ids, list) or not query_ids
            or query_ids != sorted(set(query_ids))
            or any(type(item) is not int or not 0 < item <= _INT64_MAX
                   for item in query_ids)):
        raise ValueError("STAGE8_PROJECTION_ATTEMPT_IDS_INVALID")
    population_unsigned = deepcopy(dict(population))
    population_sha = population_unsigned.pop("population_receipt_sha256", None)
    exact_population_sha = population_unsigned.pop(
        "exact_attempt_population_receipt_sha256", None,
    )
    if (not _valid_hash(population_sha) or not _valid_hash(exact_population_sha)
            or contract.digest(population_unsigned) != exact_population_sha
            or contract.digest({
                **population_unsigned,
                "exact_attempt_population_receipt_sha256": exact_population_sha,
            }) != population_sha
            or population.get("requested_attempt_ids") != query_ids
            or population.get("projection_mode")
            != projection_db_adapter.EXACT_BINDING_PROJECTION_MODE
            or population.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or population.get("found_attempt_ids") != query_ids
            or population.get("missing_attempt_ids") != []
            or population.get("population_complete") is not True
            or population.get("truncated") is not False
            or population.get("read_only") is not True
            or population.get("transaction_isolation") != "repeatable read"):
        raise ValueError("STAGE8_PROJECTION_POPULATION_RECEIPT_INVALID")
    authority_unsigned = deepcopy(dict(authority_receipt))
    authority_sha = authority_unsigned.pop("authority_receipt_sha256", None)
    if (not _valid_hash(authority_sha) or contract.digest(authority_unsigned) != authority_sha
            or authority_receipt.get("population_receipt_sha256") != population_sha
            or authority_receipt.get("projection_mode")
            != projection_db_adapter.EXACT_BINDING_PROJECTION_MODE
            or authority_receipt.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or authority_receipt.get("exact_attempt_population_receipt_sha256")
            != exact_population_sha
            or authority_receipt.get("projection_source_manifest_sha256")
            != result["projection_source_manifest_sha256"]):
        raise ValueError("STAGE8_PROJECTION_AUTHORITY_RECEIPT_INVALID")
    if ([row.get("attempt_id") for row in rows if isinstance(row, Mapping)] != query_ids
            or len(rows) != len(query_ids)):
        raise ValueError("STAGE8_PROJECTION_ROWS_DO_NOT_COVER_ATTEMPTS")
    ledger: list[dict[str, Any]] = []
    for row in rows:
        entries = row.get("fact_ledger") if isinstance(row, Mapping) else None
        if not isinstance(entries, list):
            raise ValueError("STAGE8_PROJECTION_FACT_LEDGER_INVALID")
        matching = [entry for entry in entries if isinstance(entry, Mapping)
                    and entry.get("exact_binding_sha256")
                    == exact_binding["binding_sha256"]]
        if len(matching) != 1:
            raise ValueError("STAGE8_PROJECTION_FACT_LEDGER_BINDING_CARDINALITY_INVALID")
        item = deepcopy(dict(matching[0]))
        fact, fact_authority = item.get("fact"), item.get("fact_authority")
        if not isinstance(fact, Mapping) or not isinstance(fact_authority, Mapping):
            raise ValueError("STAGE8_PROJECTION_FACT_LEDGER_INVALID")
        authority_unsigned = deepcopy(dict(fact_authority))
        authority_item_sha = authority_unsigned.pop("fact_authority_sha256", None)
        if (not _valid_hash(authority_item_sha)
                or contract.digest(authority_unsigned) != authority_item_sha
                or fact_authority.get("expected_fact_sha256") != fact.get("fact_sha256")):
            raise ValueError("STAGE8_PROJECTION_FACT_AUTHORITY_INVALID")
        validation = {"expected_fact_sha256": fact_authority["expected_fact_sha256"]}
        selection_sha = item.get("expected_watch_selection_attestation_sha256")
        watch_sha = item.get("expected_watch_code_manifest_sha256")
        if fact.get("knowledge_status") == projection.KNOWN:
            validation.update(
                expected_watch_selection_attestation_sha256=selection_sha,
                expected_watch_code_manifest_sha256=watch_sha,
            )
        projection.validate_fact(fact, **validation)
        watch_attestation = item.get("watch_selection_attestation")
        if watch_attestation is not None:
            watch_unsigned = deepcopy(dict(watch_attestation))
            supplied = watch_unsigned.pop("attestation_sha256", None)
            if supplied != selection_sha or contract.digest(watch_unsigned) != supplied:
                raise ValueError("STAGE8_WATCH_SELECTION_ATTESTATION_INVALID")
        watch_manifest = item.get("watch_code_manifest")
        if watch_manifest is not None and contract.digest(watch_manifest) != watch_sha:
            raise ValueError("STAGE8_WATCH_CODE_MANIFEST_INVALID")
        parent_evidence = item.get("parent_membership_evidence")
        noneligible = item.get("noneligibility_proof")
        parent_sha = fact_authority.get("expected_parent_membership_evidence_sha256")
        noneligible_sha = fact_authority.get("expected_noneligibility_proof_sha256")
        if (parent_evidence is None) == (noneligible is None):
            raise ValueError("STAGE8_FACT_PARENT_STATE_NOT_EXACTLY_ONE")
        if parent_evidence is not None:
            if contract.digest(parent_evidence) != parent_sha or noneligible_sha is not None:
                raise ValueError("STAGE8_FACT_PARENT_MEMBERSHIP_HASH_INVALID")
        else:
            proof_unsigned = deepcopy(dict(noneligible))
            proof_sha = proof_unsigned.pop("proof_sha256", None)
            if proof_sha != noneligible_sha or contract.digest(proof_unsigned) != proof_sha:
                raise ValueError("STAGE8_FACT_NONELIGIBILITY_HASH_INVALID")
        ledger.append(item)
    return {
        **result,
        "result_sha256": supplied_result_hash,
        "population_receipt_sha256": population_sha,
        "authority_receipt_sha256": authority_sha,
        "exact_attempt_population_receipt_sha256": exact_population_sha,
    }, query_ids, ledger


def append_projection_fact_batch_from_connection(
    conn: Any, exact_binding: Mapping[str, Any], *,
    projection_result: Mapping[str, Any],
    coverage_audit_receipt: Mapping[str, Any],
    registry_reference: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist and seal every fact state for one exact binding/population."""
    _pin_trusted_schema(conn, expected_role=FACT_WRITER_ROLE)
    contract.validate_exact_binding(exact_binding)
    raw = _registry_row_from_connection(conn, exact_binding["binding_sha256"])
    if raw is None:
        raise LookupError("STAGE8_EXACT_BINDING_NOT_DURABLY_REGISTERED")
    registered = _validate_registry_row(
        raw, exact_binding, require_current_implementation=True,
    )
    _validate_registry_reference_input(registry_reference, registered)
    adapter, attempt_ids, ledger = _validate_projection_adapter_result(
        exact_binding, projection_result, registered,
    )
    coverage = _validate_outcome_free_coverage(
        coverage_audit_receipt, attempt_ids=attempt_ids, exact_binding=exact_binding,
    )
    artifacts = implementation_artifacts()["files"]
    population = adapter["population_receipt"]
    authority = adapter["authority_receipt"]
    conn.execute("""
        INSERT INTO research_stage8_projection_fact_batches (
            exact_binding_sha256, freeze_id, registry_record_sha256,
            verifier_profile_sha256, registry_verification_receipt_sha256,
            projection_adapter_version, observed_projection_source_sha256,
            observed_projection_adapter_source_sha256,
            observed_registry_adapter_source_sha256,
            observed_registry_migration_sha256, projection_source_manifest,
            projection_source_manifest_sha256, adapter_query_binding_sha256,
            adapter_population_receipt, adapter_population_receipt_sha256,
            adapter_authority_receipt, adapter_authority_receipt_sha256,
            adapter_result_sha256, coverage_query_scope, coverage_query_sha256,
            outcome_free_population_receipt_sha256,
            coverage_attempt_population_sha256,
            coverage_source_high_water_attempt_id, attempt_ids, attempt_count
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s::jsonb,%s,
            %s::jsonb,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s
        ) ON CONFLICT (
            exact_binding_sha256, outcome_free_population_receipt_sha256,
            adapter_population_receipt_sha256
        ) DO NOTHING
    """, (
        exact_binding["binding_sha256"], registered["freeze_id"],
        registered["registry_record_sha256"], registered["verifier_profile_sha256"],
        registry_reference["registry_verification_receipt_sha256"],
        projection_db_adapter.VERSION, artifacts["projection"]["sha256"],
        artifacts["projection_db_adapter"]["sha256"],
        artifacts["registry_adapter"]["sha256"],
        artifacts["registry_migration"]["sha256"],
        _json(adapter["projection_source_manifest"]),
        adapter["projection_source_manifest_sha256"],
        adapter["query_binding_sha256"], _json(population),
        adapter["population_receipt_sha256"], _json(authority),
        adapter["authority_receipt_sha256"], adapter["result_sha256"],
        _json(coverage["query_scope"]), coverage["query_sha256"], coverage[
            "outcome_free_population_receipt_sha256"
        ], coverage["attempt_population_sha256"],
        coverage["source_high_water_attempt_id"], _json(attempt_ids),
        len(attempt_ids),
    ))
    batch = _query_one(conn, """
        SELECT * FROM research_stage8_fact_batch_read_v1
        WHERE exact_binding_sha256=%s
          AND outcome_free_population_receipt_sha256=%s
          AND adapter_population_receipt_sha256=%s
    """, (exact_binding["binding_sha256"],
           coverage["outcome_free_population_receipt_sha256"],
           adapter["population_receipt_sha256"]))
    if batch is None or batch["fact_batch_record_sha256"] != contract.digest(
        batch["fact_batch_record"]
    ):
        raise RuntimeError("STAGE8_FACT_BATCH_INSERT_OR_VERIFICATION_FAILED")
    batch_sha = batch["fact_batch_record_sha256"]
    for item in ledger:
        fact = item["fact"]
        authority_item = item["fact_authority"]
        identity = fact["identity"]
        parent_evidence = item.get("parent_membership_evidence")
        noneligible = item.get("noneligibility_proof")
        parent_sha = authority_item.get("expected_parent_membership_evidence_sha256")
        noneligible_sha = authority_item.get("expected_noneligibility_proof_sha256")
        conn.execute("""
            INSERT INTO research_stage8_projected_fact_ledger (
                fact_batch_record_sha256, exact_binding_sha256, attempt_id,
                attempt_fingerprint, anchor_slot_id, event_id, event_fingerprint,
                symbol, direction, decision_time_utc, knowledge_status,
                candidate_match, fact, fact_sha256, fact_authority,
                fact_authority_sha256, watch_selection_attestation,
                watch_selection_attestation_sha256,
                observed_watch_code_manifest_sha256,
                parent_membership_evidence, parent_membership_evidence_sha256,
                noneligibility_proof, noneligibility_proof_sha256
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,
                %s::jsonb,%s,%s::jsonb,%s,%s,%s::jsonb,%s,%s::jsonb,%s
            ) ON CONFLICT (fact_batch_record_sha256, attempt_id) DO NOTHING
        """, (
            batch_sha, exact_binding["binding_sha256"], item["attempt_id"],
            identity["attempt_fingerprint"], identity.get("anchor_slot_id"),
            identity.get("event_id"), identity.get("event_fingerprint"),
            identity.get("symbol"), identity.get("direction"),
            identity.get("decision_time_utc"), fact["knowledge_status"],
            fact.get("candidate_match"), _json(fact), fact["fact_sha256"],
            _json(authority_item), authority_item["fact_authority_sha256"],
            (_json(item["watch_selection_attestation"])
             if item.get("watch_selection_attestation") is not None else None),
            item.get("expected_watch_selection_attestation_sha256"),
            item.get("expected_watch_code_manifest_sha256"),
            _json(parent_evidence) if parent_evidence is not None else None,
            parent_sha,
            _json(noneligible) if noneligible is not None else None,
            noneligible_sha,
        ))
    stored_facts = conn.execute("""
        SELECT * FROM research_stage8_fact_read_v1
        WHERE fact_batch_record_sha256=%s ORDER BY attempt_id
    """, (batch_sha,)).fetchall()
    stored_facts = [dict(row) if isinstance(row, Mapping) else row for row in stored_facts]
    if len(stored_facts) != len(attempt_ids):
        raise RuntimeError("STAGE8_FACT_LEDGER_NOT_POPULATION_COMPLETE")
    fact_records = [{
        "attempt_id": row["attempt_id"],
        "fact_record_sha256": row["fact_record_sha256"],
    } for row in stored_facts]
    fact_records_sha = contract.digest({
        "version": "stage8-durable-fact-record-set-v1",
        "fact_batch_record_sha256": batch_sha,
        "facts": fact_records,
    })
    conn.execute("""
        INSERT INTO research_stage8_projection_fact_batch_seals
            (fact_batch_record_sha256, fact_count, fact_records_sha256)
        VALUES (%s,%s,%s) ON CONFLICT (fact_batch_record_sha256) DO NOTHING
    """, (batch_sha, len(attempt_ids), fact_records_sha))
    seal = _query_one(conn, """
        SELECT * FROM research_stage8_fact_seal_read_v1
        WHERE fact_batch_record_sha256=%s
    """, (batch_sha,))
    if (seal is None or seal["fact_count"] != len(attempt_ids)
            or seal["fact_records_sha256"] != fact_records_sha
            or seal["seal_record_sha256"] != contract.digest(seal["seal_record"])):
        raise RuntimeError("STAGE8_FACT_BATCH_SEAL_FAILED")
    return {"batch": deepcopy(batch), "seal": deepcopy(seal)}


def append_projection_fact_batch(
    exact_binding: Mapping[str, Any], *,
    projection_result: Mapping[str, Any],
    coverage_audit_receipt: Mapping[str, Any],
    registry_reference: Mapping[str, Any],
) -> dict[str, Any]:
    with _connect("RESEARCH_STAGE8_FACT_DATABASE_URL", read_only=False) as conn:
        _require_env_role(conn, FACT_WRITER_ROLE)
        result = append_projection_fact_batch_from_connection(
            conn, exact_binding, projection_result=projection_result,
            coverage_audit_receipt=coverage_audit_receipt,
            registry_reference=registry_reference,
        )
        conn.commit()
        return result


def _validate_selector_result(
    exact_binding: Mapping[str, Any], selector_result: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(selector_result, Mapping):
        raise ValueError("STAGE8_SELECTOR_RESULT_INVALID")
    value = deepcopy(dict(selector_result))
    supplied_receipt_sha = value.pop("selector_receipt_sha256", None)
    if not _valid_hash(supplied_receipt_sha) or contract.digest(value) != supplied_receipt_sha:
        raise ValueError("STAGE8_SELECTOR_RECEIPT_HASH_INVALID")
    representatives = value.pop("representatives", None)
    selection_sha = value.pop("selection_attestation_sha256", None)
    source_audit_sha = value.pop("source_audit_receipt_sha256", None)
    if (not isinstance(representatives, list) or not _valid_hash(selection_sha)
            or contract.digest(value) != selection_sha
            or not _valid_hash(source_audit_sha)):
        raise ValueError("STAGE8_SELECTION_ATTESTATION_HASH_INVALID")
    expected = {
        "version": selector.BATCH_VERSION,
        "selector_version": selector.VERSION,
        "structural_authority": selector.STRUCTURAL_AUTHORITY,
        "status": "COMPLETE",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "manifest_sha256": contract.MANIFEST_SHA256,
        "freeze_id": registry["freeze_id"],
        "frozen_at_utc": registry["frozen_at_utc"],
        "registry_record_sha256": registry["registry_record_sha256"],
        "verifier_profile_sha256": registry["verifier_profile_sha256"],
        "expected_projection_source_sha256": registry["implementation_artifacts"]
            ["files"]["projection"]["sha256"],
        "expected_selector_source_sha256": registry["implementation_artifacts"]
            ["files"]["selector"]["sha256"],
        "expected_watch_code_manifest_sha256": registry[
            "expected_watch_code_manifest_sha256"
        ],
        "population_coverage_complete": True,
        "candidate_match_coverage_complete": True,
        "outcome_blind_selection": True,
        "outcome_or_label_fields_accepted": False,
        "truncated": False,
        "qualification_evaluated": False,
        "database_verification_asserted_by_selector": False,
    }
    if (set(value) != _SELECTOR_BATCH_KEYS
            or any(value.get(key) != expected_value
                   for key, expected_value in expected.items())
            or value.get("global_blockers") != []
            or value.get("blocked_parents") != []
            or value.get("representative_count") != len(representatives)
            or not _valid_hash(value.get("registry_verification_receipt_sha256"))
            or not _valid_hash(value.get("cohort_query_sha256"))
            or not _valid_hash(value.get("outcome_free_population_receipt_sha256"))
            or not _valid_hash(value.get("attempt_population_sha256"))
            or not _valid_hash(value.get("source_transaction_identity_sha256"))
            or not _valid_hash(value.get("source_authority_ledger_sha256"))
            or value.get("source_authority_ledger_count")
            != value.get("source_attempt_count")
            or type(value.get("source_high_water_attempt_id")) is not int
            or not 0 <= value["source_high_water_attempt_id"] <= _INT64_MAX
            or value.get("deduplicated_exact_anchor_event_count") != 0
            or not isinstance(value.get("excluded_pre_freeze_parent_ids"), list)
            or value["excluded_pre_freeze_parent_ids"]
                != sorted(set(value["excluded_pre_freeze_parent_ids"]))
            or any(not _valid_hash(item)
                   for item in value["excluded_pre_freeze_parent_ids"])
            or not isinstance(value.get("proven_noneligible_attempt_ids"), list)
            or value["proven_noneligible_attempt_ids"]
                != sorted(set(value["proven_noneligible_attempt_ids"]))
            or any(type(item) is not int or not 0 < item <= _INT64_MAX
                   for item in value["proven_noneligible_attempt_ids"])):
        raise ValueError("STAGE8_SELECTOR_RESULT_NOT_COMPLETE_OR_NOT_DURABLY_BOUND")
    representative_set_sha = selector._representative_set_sha256(
        exact_binding["binding_sha256"], representatives,
    )
    if (len(representatives) != value["representative_count"]
            or representative_set_sha != value.get("representative_set_sha256")):
        raise ValueError("STAGE8_REPRESENTATIVE_SET_HASH_INVALID")
    identities = [selector._representative_identity(row)
                  for row in representatives]
    identities.sort(key=contract.canonical)
    return value, representatives, identities


def _sealed_fact_batch_for_selector(
    conn: Any, exact_binding: Mapping[str, Any], batch: Mapping[str, Any],
) -> dict[str, Any]:
    cursor = conn.execute("""
        SELECT b.* FROM research_stage8_fact_batch_read_v1 AS b
        JOIN research_stage8_fact_seal_read_v1 AS seal
          USING (fact_batch_record_sha256)
        WHERE b.exact_binding_sha256=%s
          AND b.coverage_query_sha256=%s
          AND b.outcome_free_population_receipt_sha256=%s
          AND b.coverage_attempt_population_sha256=%s
          AND b.coverage_source_high_water_attempt_id=%s
          AND b.attempt_count=%s
          AND b.registry_verification_receipt_sha256=%s
        ORDER BY b.persisted_at_utc, b.fact_batch_record_sha256
    """, (
        exact_binding["binding_sha256"], batch["cohort_query_sha256"],
        batch["outcome_free_population_receipt_sha256"],
        batch["attempt_population_sha256"], batch["source_high_water_attempt_id"],
        batch["source_attempt_count"],
        batch["registry_verification_receipt_sha256"],
    ))
    raw_candidates = cursor.fetchall()
    candidates = [dict(row) if isinstance(row, Mapping)
                  else _row(cursor, row) for row in raw_candidates]
    matches = []
    for candidate in candidates:
        fact_cursor = conn.execute("""
            SELECT * FROM research_stage8_fact_read_v1
            WHERE fact_batch_record_sha256=%s ORDER BY attempt_id
        """, (candidate["fact_batch_record_sha256"],))
        raw_facts = fact_cursor.fetchall()
        facts = [dict(row) if isinstance(row, Mapping) else _row(fact_cursor, row)
                 for row in raw_facts]
        if any(
            not isinstance(fact_row.get("selection_fact_identity"), Mapping)
            or not _valid_hash(fact_row.get("selection_fact_identity_sha256"))
            or contract.digest(fact_row["selection_fact_identity"])
                != fact_row["selection_fact_identity_sha256"]
            for fact_row in facts
        ):
            continue
        ledger = [{
            "attempt_id": fact_row["attempt_id"],
            "expected_selection_fact_identity_sha256": fact_row[
                "selection_fact_identity_sha256"
            ],
        } for fact_row in facts]
        ledger_sha = contract.digest({
            "version": "stage8-selector-source-authority-ledger-v1",
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "attempt_population_sha256": batch["attempt_population_sha256"],
            "entries": ledger,
        })
        if (ledger_sha == batch["source_authority_ledger_sha256"]
                and len(ledger) == batch["source_authority_ledger_count"]):
            matches.append(candidate)
    if len(matches) != 1:
        raise ValueError("STAGE8_SELECTOR_NOT_BOUND_TO_ONE_SEALED_FACT_BATCH")
    return matches[0]


def append_selection_receipt_from_connection(
    conn: Any, exact_binding: Mapping[str, Any],
    selector_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist one complete pure-selector result under the frozen profile."""
    _pin_trusted_schema(conn, expected_role=SELECTOR_WRITER_ROLE)
    contract.validate_exact_binding(exact_binding)
    row = _registry_row_from_connection(conn, exact_binding["binding_sha256"])
    if row is None:
        raise LookupError("STAGE8_EXACT_BINDING_NOT_DURABLY_REGISTERED")
    registry = _validate_registry_row(
        row, exact_binding, require_current_implementation=True,
    )
    batch, _, identities = _validate_selector_result(
        exact_binding, selector_result, registry,
    )
    fact_batch = _sealed_fact_batch_for_selector(conn, exact_binding, batch)
    artifacts = implementation_artifacts()["files"]
    params = (
        fact_batch["fact_batch_record_sha256"], exact_binding["binding_sha256"],
        registry["freeze_id"],
        registry["registry_record_sha256"], registry["verifier_profile_sha256"],
        batch["registry_verification_receipt_sha256"], selector.VERSION,
        artifacts["projection"]["sha256"], artifacts["selector"]["sha256"],
        registry["expected_watch_code_manifest_sha256"], batch["cohort_query_sha256"],
        batch["outcome_free_population_receipt_sha256"],
        batch["source_high_water_attempt_id"],
        batch["representative_count"], batch["representative_set_sha256"],
        _json(identities), _json(batch),
    )
    conn.execute("""
        INSERT INTO research_stage8_selection_receipts (
            fact_batch_record_sha256, exact_binding_sha256, freeze_id,
            registry_record_sha256,
            verifier_profile_sha256, registry_verification_receipt_sha256,
            selector_version, observed_projection_source_sha256,
            observed_selector_source_sha256, observed_watch_code_manifest_sha256,
            cohort_query_sha256, outcome_free_population_receipt_sha256,
            source_high_water_attempt_id, representative_count,
            representative_set_sha256, representative_identities,
            selection_attestation
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb
        ) ON CONFLICT (
            exact_binding_sha256, cohort_query_sha256,
            outcome_free_population_receipt_sha256, source_high_water_attempt_id
        ) DO NOTHING
    """, params)
    stored = _query_one(conn, """
        SELECT * FROM research_stage8_selection_read_v1
        WHERE exact_binding_sha256=%s AND cohort_query_sha256=%s
          AND outcome_free_population_receipt_sha256=%s
          AND source_high_water_attempt_id=%s
    """, (exact_binding["binding_sha256"], batch["cohort_query_sha256"],
           batch["outcome_free_population_receipt_sha256"],
           batch["source_high_water_attempt_id"]))
    if stored is None:
        raise RuntimeError("STAGE8_SELECTION_INSERT_NOT_VISIBLE")
    _validate_stored_selection(
        stored, exact_binding, registry, identities, batch,
        fact_batch_record_sha256=fact_batch["fact_batch_record_sha256"],
    )
    result = deepcopy(stored)
    result["persisted_at_utc"] = _iso_utc(result["persisted_at_utc"])
    return result


def append_selection_receipt(
    exact_binding: Mapping[str, Any], selector_result: Mapping[str, Any],
) -> dict[str, Any]:
    with _connect("RESEARCH_STAGE8_SELECTOR_DATABASE_URL", read_only=False) as conn:
        _require_env_role(conn, SELECTOR_WRITER_ROLE)
        result = append_selection_receipt_from_connection(
            conn, exact_binding, selector_result,
        )
        conn.commit()
        return result


def _validate_stored_selection(
    stored: Mapping[str, Any], exact_binding: Mapping[str, Any],
    registry: Mapping[str, Any], identities: Sequence[Mapping[str, Any]],
    batch: Mapping[str, Any], *, fact_batch_record_sha256: str,
) -> None:
    try:
        valid = (
            stored["exact_binding_sha256"] == exact_binding["binding_sha256"]
            and stored["fact_batch_record_sha256"]
            == fact_batch_record_sha256
            and stored["freeze_id"] == registry["freeze_id"]
            and stored["registry_record_sha256"] == registry["registry_record_sha256"]
            and stored["verifier_profile_sha256"] == registry["verifier_profile_sha256"]
            and stored["registry_verification_receipt_sha256"]
            == batch["registry_verification_receipt_sha256"]
            and stored["selection_attestation_sha256"] == contract.digest(batch)
            and stored["selection_record_sha256"] == contract.digest(stored["selection_record"])
            and contract.canonical(stored["representative_identities"])
            == contract.canonical(list(identities))
            and stored["representative_identities_sha256"] == contract.digest({
                "version": "stage8-durable-representative-identities-v1",
                "exact_binding_sha256": exact_binding["binding_sha256"],
                "representatives": list(identities),
            })
            and stored["representative_set_sha256"] == batch["representative_set_sha256"]
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("STAGE8_DURABLE_SELECTION_INVALID") from exc
    if not valid:
        raise ValueError("STAGE8_DURABLE_SELECTION_INVALID")


def _selection_from_connection(
    conn: Any, selection_record_sha256: str,
) -> dict[str, Any] | None:
    if not _valid_hash(selection_record_sha256):
        raise ValueError("STAGE8_SELECTION_RECORD_SHA256_INVALID")
    return _query_one(conn, """
        SELECT * FROM research_stage8_selection_read_v1
        WHERE selection_record_sha256 = %s
    """, (selection_record_sha256,))


def evaluate_verified_from_connection(
    conn: Any, exact_binding: Mapping[str, Any],
    representatives: Sequence[Mapping[str, Any]], *,
    selection_record_sha256: str,
) -> dict[str, Any]:
    """Verify durable rows and produce a non-qualifying atomic diagnostic.

    No receipt argument can bypass the reads.  The result remains experimental
    research and explicitly carries no delivery or trade authority.
    """
    _pin_trusted_schema(conn, expected_role=READER_ROLE)
    _require_verified_read_transaction(conn)
    contract.validate_exact_binding(exact_binding)
    registry_raw = _registry_row_from_connection(conn, exact_binding["binding_sha256"])
    selection_row = _selection_from_connection(conn, selection_record_sha256)
    if registry_raw is None or selection_row is None:
        raise LookupError("STAGE8_DURABLE_REGISTRY_OR_SELECTION_NOT_FOUND")
    registry = _validate_registry_row(
        registry_raw, exact_binding, require_current_implementation=True,
    )
    rows = deepcopy(list(representatives))
    try:
        identities = [selector._representative_identity(row) for row in rows]
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("STAGE8_CALLER_REPRESENTATIVE_IDENTITY_INVALID") from exc
    identities.sort(key=contract.canonical)
    batch = selection_row["selection_attestation"]
    _validate_stored_selection(
        selection_row, exact_binding, registry, identities, batch,
        fact_batch_record_sha256=selection_row["fact_batch_record_sha256"],
    )
    if len(rows) != selection_row["representative_count"]:
        raise ValueError("STAGE8_DURABLE_SELECTION_REPRESENTATIVE_COUNT_MISMATCH")
    fact_rows = conn.execute("""
        SELECT attempt_id, selection_fact_identity,
               selection_fact_identity_sha256
        FROM research_stage8_fact_read_v1
        WHERE fact_batch_record_sha256=%s ORDER BY attempt_id
    """, (selection_row["fact_batch_record_sha256"],)).fetchall()
    durable_hashes = {
        row["selection_fact_identity_sha256"]
        for row in fact_rows
        if isinstance(row, Mapping)
        and isinstance(row.get("selection_fact_identity"), Mapping)
        and contract.digest(row["selection_fact_identity"])
            == row.get("selection_fact_identity_sha256")
    }
    expected_hashes = {
        item["expected_selection_fact_identity_sha256"] for item in identities
    }
    if (len(durable_hashes) != len(fact_rows)
            or not expected_hashes.issubset(durable_hashes)):
        raise ValueError("STAGE8_DURABLE_SELECTION_FACT_IDENTITY_MISMATCH")
    # Caller-supplied outcomes are never evaluated.  Only the dedicated
    # same-snapshot outcome adapter can produce the closed persistence payload.
    return {
        "evaluator_version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "selection_record_sha256": selection_record_sha256,
        "atomic_gate_passed": False,
        "research_qualified": False,
        "status": "CALLER_OUTCOME_EVALUATION_DISABLED",
        "qualification_blockers": ["AUTHORITATIVE_OUTCOME_ADAPTER_REQUIRED"],
        "durable_registry_persistence_verified": True,
        "registry_selection_persistence_verified": True,
        "result_scope": RESEARCH_RESULT_SCOPE,
        "live_authorized": False,
        "telegram_authorized": False,
        "trade_authorized": False,
    }


def _validate_outcome_persistence_payload(
    exact_binding: Mapping[str, Any], payload: Mapping[str, Any],
    *, selection_record_sha256: str, registry_row: Mapping[str, Any],
    selection_row: Mapping[str, Any],
    durable_fact_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate the closed payload against the exact durable fact population.

    ``durable_fact_rows`` must come from the sealed Stage-8 fact view in the
    same connection used for the append.  Caller-recomputable JSON hashes are
    integrity checks only; they never substitute for this database binding.
    """
    if not isinstance(payload, Mapping):
        raise ValueError("STAGE8_OUTCOME_PERSISTENCE_PAYLOAD_INVALID")
    value = deepcopy(dict(payload))
    if set(value) != _OUTCOME_PERSISTENCE_KEYS:
        raise ValueError("STAGE8_OUTCOME_PERSISTENCE_PAYLOAD_NOT_CLOSED")
    replay = value.get("fact_replay_receipt")
    evidence = value.get("evidence_receipt")
    evaluation = value.get("evaluation")
    source_manifest = value.get("outcome_source_manifest")
    if (not isinstance(replay, Mapping) or set(replay) != _FACT_REPLAY_KEYS
            or not isinstance(evidence, Mapping)
            or set(evidence) != _OUTCOME_EVIDENCE_KEYS
            or not isinstance(evaluation, Mapping)
            or set(evaluation) != _AUTHORITATIVE_EVALUATION_KEYS
            or not isinstance(source_manifest, Mapping)
            or set(source_manifest) != {"version", "files"}
            or source_manifest.get("version")
            != "stage8-outcome-reader-source-manifest-v1"
            or not isinstance(source_manifest.get("files"), Mapping)
            or set(source_manifest["files"]) != _OUTCOME_SOURCE_FILES
            or any(not _valid_hash(item)
                   for item in source_manifest["files"].values())):
        raise ValueError("STAGE8_OUTCOME_PERSISTENCE_INNER_SCHEMA_INVALID")
    artifacts = registry_row["implementation_artifacts"]["files"]
    if (isinstance(durable_fact_rows, (str, bytes))
            or not isinstance(durable_fact_rows, Sequence)
            or any(not isinstance(item, Mapping) for item in durable_fact_rows)):
        raise ValueError("STAGE8_OUTCOME_DURABLE_FACT_POPULATION_INVALID")
    durable_facts = [deepcopy(dict(item)) for item in durable_fact_rows]
    durable_facts.sort(key=lambda item: item.get("attempt_id", -1))
    durable_attempt_ids = [item.get("attempt_id") for item in durable_facts]
    if (not durable_facts
            or any(type(item) is not int or not 0 < item <= 9223372036854775807
                   for item in durable_attempt_ids)
            or durable_attempt_ids != sorted(set(durable_attempt_ids))
            or any(item.get("fact_batch_record_sha256")
                   != selection_row.get("fact_batch_record_sha256")
                   or item.get("exact_binding_sha256")
                   != exact_binding["binding_sha256"]
                   for item in durable_facts)):
        raise ValueError("STAGE8_OUTCOME_DURABLE_FACT_POPULATION_INVALID")
    durable_by_attempt = {item["attempt_id"]: item for item in durable_facts}
    durable_by_event: dict[int, Mapping[str, Any]] = {}
    for item in durable_facts:
        event_id = item.get("event_id")
        if event_id is None:
            continue
        if (type(event_id) is not int or not 0 < event_id <= 9223372036854775807
                or event_id in durable_by_event):
            raise ValueError("STAGE8_OUTCOME_DURABLE_FACT_EVENT_POPULATION_INVALID")
        durable_by_event[event_id] = item
    replay_unsigned = dict(replay)
    replay_sha = replay_unsigned.pop("fact_replay_receipt_sha256", None)
    evidence_unsigned = dict(evidence)
    evidence_sha = evidence_unsigned.pop("evidence_receipt_sha256", None)
    payload_unsigned = dict(value)
    payload_sha = payload_unsigned.pop("persistence_payload_sha256", None)
    expected_files = {
        "canonical_price_path.py":
            artifacts["canonical_price_path"]["sha256"],
        "research_common_window_metrics.py":
            artifacts["common_window_metrics"]["sha256"],
        "research_operational_score_source_audit.py":
            artifacts["source_audit"]["sha256"],
        "research_stage8_outcome_db_adapter.py":
            artifacts["outcome_db_adapter"]["sha256"],
        "research_stage8_acceptance.py": artifacts["acceptance"]["sha256"],
        "research_stage8_contract.py": artifacts["contract"]["sha256"],
        "research_stage8_feature_projection.py":
            artifacts["projection"]["sha256"],
        "research_stage8_projection_db_adapter.py":
            artifacts["projection_db_adapter"]["sha256"],
        "research_stage8_registry.py": artifacts["registry_adapter"]["sha256"],
        "research_stage8_representative_selector.py": artifacts["selector"]["sha256"],
    }
    if (value.get("version") != OUTCOME_PERSISTENCE_VERSION
            or value.get("manifest_sha256") != contract.MANIFEST_SHA256
            or value.get("exact_binding_sha256") != exact_binding["binding_sha256"]
            or value.get("selection_record_sha256") != selection_record_sha256
            or value.get("outcome_adapter_version") != OUTCOME_ADAPTER_VERSION
            or value.get("outcome_adapter_source_sha256")
            != artifacts["outcome_db_adapter"]["sha256"]
            or value.get("result_scope") != RESEARCH_RESULT_SCOPE
            or any(value.get(key) is not False for key in (
                "live_authorized", "telegram_authorized", "trade_authorized"
            ))
            or contract.digest(source_manifest)
            != value.get("outcome_source_manifest_sha256")
            or any(source_manifest["files"].get(key) != expected
                   for key, expected in expected_files.items())
            or not _valid_hash(payload_sha) or contract.digest(payload_unsigned) != payload_sha
            or not _valid_hash(replay_sha) or contract.digest(replay_unsigned) != replay_sha
            or value.get("fact_replay_receipt_sha256") != replay_sha
            or not _valid_hash(evidence_sha)
            or contract.digest(evidence_unsigned) != evidence_sha
            or value.get("evidence_receipt_sha256") != evidence_sha
            or value.get("evaluation_sha256") != contract.digest(evaluation)):
        raise ValueError("STAGE8_OUTCOME_PERSISTENCE_HASH_OR_BINDING_INVALID")
    transaction_sha = value.get("transaction_identity_sha256")
    if (not _valid_hash(transaction_sha)
            or replay.get("version")
            != "stage8-authoritative-fact-replay-receipt-v1"
            or replay.get("manifest_sha256") != contract.MANIFEST_SHA256
            or replay.get("exact_binding_sha256") != exact_binding["binding_sha256"]
            or replay.get("selection_record_sha256") != selection_record_sha256
            or replay.get("fact_batch_record_sha256")
            != selection_row.get("fact_batch_record_sha256")
            or replay.get("transaction_identity_sha256") != transaction_sha
            or replay.get("projection_adapter_version")
            != projection_db_adapter.VERSION
            or replay.get("outcome_free") is not True
            or replay.get("truncated") is not False
            or type(replay.get("attempt_count")) is not int
            or replay.get("attempt_count") <= 0
            or replay.get("regenerated_fact_count") != replay.get("attempt_count")
            or not isinstance(replay.get("attempt_ids"), list)
            or len(replay["attempt_ids"]) != replay["attempt_count"]
            or replay.get("attempt_ids") != sorted(set(replay["attempt_ids"]))
            or replay.get("attempt_ids") != durable_attempt_ids
            or replay.get("attempt_population_sha256") != contract.digest({
                "version": "stage8-authoritative-fact-replay-receipt-v1",
                "exact_binding_sha256": exact_binding["binding_sha256"],
                "attempt_ids": durable_attempt_ids,
            })
            or any(not _valid_hash(replay.get(key)) for key in (
                "projection_result_sha256",
                "projection_source_manifest_sha256",
                "projection_population_receipt_sha256",
                "projection_authority_receipt_sha256",
            ))
            or not isinstance(replay.get("comparisons"), list)
            or len(replay["comparisons"]) != replay["attempt_count"]):
        raise ValueError("STAGE8_OUTCOME_FACT_REPLAY_INVALID")
    replay_verified = True
    for attempt_id, comparison in zip(durable_attempt_ids, replay["comparisons"]):
        durable_fact = durable_by_attempt[attempt_id]
        if (not isinstance(comparison, Mapping)
                or set(comparison) != _FACT_REPLAY_COMPARISON_KEYS
                or comparison.get("attempt_id") != attempt_id
                or comparison.get("durable_fact_record_sha256")
                    != durable_fact.get("fact_record_sha256")
                or comparison.get("durable_fact_sha256")
                    != durable_fact.get("fact_sha256")
                or comparison.get("durable_selection_fact_identity_sha256")
                    != durable_fact.get("selection_fact_identity_sha256")
                or comparison.get("status") not in {"VERIFIED", "UNKNOWN"}
                or not isinstance(comparison.get("reasons"), list)
                or any(not isinstance(reason, str) or not reason
                       for reason in comparison.get("reasons", []))
                or comparison.get("reasons")
                    != sorted(set(comparison.get("reasons", [])))):
            raise ValueError("STAGE8_OUTCOME_FACT_REPLAY_COMPARISON_INVALID")
        comparison_valid = (
            comparison.get("status") == "VERIFIED"
            and comparison.get("reasons") == []
            and _valid_hash(comparison.get("durable_fact_semantic_sha256"))
            and comparison.get("durable_fact_semantic_sha256")
                == comparison.get("replayed_fact_semantic_sha256")
            and isinstance(comparison.get("durable_parent_class"), str)
            and comparison.get("durable_parent_class")
                == comparison.get("replayed_parent_class")
            and _valid_hash(comparison.get("durable_parent_semantic_sha256"))
            and comparison.get("durable_parent_semantic_sha256")
                == comparison.get("replayed_parent_semantic_sha256")
            and _valid_hash(
                comparison.get("durable_selection_fact_identity_sha256"))
            and comparison.get("durable_selection_fact_identity_sha256")
                == comparison.get("replayed_selection_fact_identity_sha256")
        )
        if comparison.get("status") == "VERIFIED" and not comparison_valid:
            raise ValueError("STAGE8_OUTCOME_FACT_REPLAY_COMPARISON_INVALID")
        if comparison.get("status") == "UNKNOWN" and not comparison["reasons"]:
            raise ValueError("STAGE8_OUTCOME_FACT_REPLAY_COMPARISON_INVALID")
        replay_verified = replay_verified and comparison_valid
    if (replay.get("all_causal_fact_semantics_verified") is not replay_verified
            or replay.get("durable_selection_server_recomputed") is not False
            or replay.get("complete") is not replay_verified
            or replay.get("status") != ("VERIFIED" if replay_verified else "UNKNOWN")):
        raise ValueError("STAGE8_OUTCOME_FACT_REPLAY_SUMMARY_INVALID")
    if (evidence.get("version") != "stage8-durable-outcome-evidence-receipt-v1"
            or evidence.get("adapter_version") != OUTCOME_ADAPTER_VERSION
            or evidence.get("manifest_sha256") != contract.MANIFEST_SHA256
            or evidence.get("exact_binding_sha256") != exact_binding["binding_sha256"]
            or evidence.get("selection_record_sha256") != selection_record_sha256
            or evidence.get("fact_batch_record_sha256")
            != selection_row.get("fact_batch_record_sha256")
            or evidence.get("transaction_identity_sha256") != transaction_sha
            or evidence.get("source_manifest_sha256")
            != value.get("outcome_source_manifest_sha256")
            or evidence.get("query_count") != 15
            or evidence.get("outcome_query_count") != 7
            or evidence.get("projection_replay_query_count") != 8
            or not _valid_hash(evidence.get("fact_seal_record_sha256"))
            or not isinstance(evidence.get("raw_source_hashes"), Mapping)
            or set(evidence.get("raw_source_hashes", {})) != _RAW_SOURCE_HASH_KEYS
            or any(not _valid_hash(item)
                   for item in evidence.get("raw_source_hashes", {}).values())
            or evidence.get("outcome_authority")
                != "DURABLE_ROWS_READ_IN_CALLER_OWNED_RO_RR_SNAPSHOT"
            or evidence.get("fact_authority") != (
                "SAME_SNAPSHOT_CAUSAL_SOURCE_REPLAY_VERIFIED"
                if replay_verified
                else "SAME_SNAPSHOT_CAUSAL_SOURCE_REPLAY_UNKNOWN"
            )
            or evidence.get("selection_population_read_complete") is not True
            or evidence.get("population_read_complete") is not True
            or evidence.get("complete") is not True
            or evidence.get("truncated") is not False
            or type(evidence.get("parent_count")) is not int
            or evidence["parent_count"] < 0
            or evidence["parent_count"] != selection_row.get("representative_count")
            or not isinstance(evidence.get("btc_parent_movement_ids"), list)
            or len(evidence["btc_parent_movement_ids"]) != evidence["parent_count"]
            or not isinstance(evidence.get("representatives"), list)
            or len(evidence["representatives"]) != evidence["parent_count"]):
        raise ValueError("STAGE8_OUTCOME_EVIDENCE_INVALID")
    try:
        read_started = _iso_utc(evidence.get("read_started_at_utc"))
        read_finished = _iso_utc(evidence.get("read_finished_at_utc"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("STAGE8_OUTCOME_EVIDENCE_READ_TIME_INVALID") from exc
    if (read_started != evidence.get("read_started_at_utc")
            or read_finished != evidence.get("read_finished_at_utc")
            or read_finished < read_started):
        raise ValueError("STAGE8_OUTCOME_EVIDENCE_READ_TIME_INVALID")
    selection_identities = selection_row.get("representative_identities")
    evidence_representatives = evidence.get("representatives")
    if (not isinstance(selection_identities, list)
            or not isinstance(evidence_representatives, list)):
        raise ValueError("STAGE8_OUTCOME_REPRESENTATIVE_POPULATION_INVALID")
    selected_by_parent: dict[str, Mapping[str, Any]] = {}
    selected_parent_ids_in_order: list[str] = []
    selected_identity_keys = {
        "version", "exact_binding_sha256", "btc_parent_movement_id",
        "parent_start_time_utc", "expected_selection_fact_identity_sha256",
        "attempt_fingerprint", "anchor_slot_id", "event_id",
        "event_fingerprint", "symbol", "direction", "decision_time_utc",
        "candidate_match_knowledge_status", "candidate_match",
    }
    for item in selection_identities:
        if not isinstance(item, Mapping):
            raise ValueError("STAGE8_OUTCOME_SELECTION_IDENTITY_INVALID")
        parent_id = item.get("btc_parent_movement_id")
        expected_sha = item.get("expected_selection_fact_identity_sha256")
        if (set(item) != selected_identity_keys
                or item.get("version")
                    != "stage8-outcome-free-representative-identity-v1"
                or item.get("exact_binding_sha256")
                    != exact_binding["binding_sha256"]
                or not _valid_hash(parent_id) or not _valid_hash(expected_sha)
                or parent_id in selected_by_parent):
            raise ValueError("STAGE8_OUTCOME_SELECTION_IDENTITY_INVALID")
        try:
            if (_iso_utc(item.get("parent_start_time_utc"))
                    != item.get("parent_start_time_utc")
                    or _iso_utc(item.get("decision_time_utc"))
                    != item.get("decision_time_utc")):
                raise ValueError("noncanonical time")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("STAGE8_OUTCOME_SELECTION_IDENTITY_INVALID") from exc
        selected_by_parent[parent_id] = item
        selected_parent_ids_in_order.append(parent_id)
    evidence_by_parent: dict[str, Mapping[str, Any]] = {}
    probability_values: dict[str, bool] = {}
    asymmetry_values: dict[str, tuple[float, float]] = {}
    probability_valid_count = 0
    asymmetry_valid_count = 0
    normalized_rows: list[dict[str, Any]] = []
    representative_binding = acceptance.representative_binding(exact_binding)
    for item in evidence_representatives:
        if (not isinstance(item, Mapping)
                or set(item) != _EVIDENCE_REPRESENTATIVE_KEYS):
            raise ValueError("STAGE8_OUTCOME_EVIDENCE_REPRESENTATIVE_INVALID")
        parent_id = item.get("btc_parent_movement_id")
        selected = selected_by_parent.get(parent_id)
        expected_sha = item.get("expected_selection_fact_identity_sha256")
        actual_sha = item.get("selection_fact_identity_sha256")
        probability_item = item.get("probability")
        asymmetry_item = item.get("asymmetry")
        fact_item = item.get("fact")
        durable_fact = durable_by_event.get(item.get("event_id"))
        if (selected is None or parent_id in evidence_by_parent
                or expected_sha != selected.get(
                    "expected_selection_fact_identity_sha256")
                or actual_sha != expected_sha or not _valid_hash(actual_sha)
                or item.get("parent_start_time_utc")
                    != selected.get("parent_start_time_utc")
                or item.get("event_id") != selected.get("event_id")
                or item.get("event_fingerprint")
                    != selected.get("event_fingerprint")
                or item.get("durable_representative_identity_sha256")
                    != contract.digest(selected)
                or not _valid_hash(item.get("representative_identity_sha256"))
                or durable_fact is None
                or durable_fact.get("attempt_fingerprint")
                    != selected.get("attempt_fingerprint")
                or durable_fact.get("anchor_slot_id")
                    != selected.get("anchor_slot_id")
                or durable_fact.get("event_fingerprint")
                    != selected.get("event_fingerprint")
                or durable_fact.get("symbol") != selected.get("symbol")
                or durable_fact.get("direction") != selected.get("direction")
                or durable_fact.get("knowledge_status")
                    != selected.get("candidate_match_knowledge_status")
                or durable_fact.get("candidate_match")
                    is not selected.get("candidate_match")
                or _iso_utc(durable_fact.get("decision_time_utc"))
                    != selected.get("decision_time_utc")
                or durable_fact.get("selection_fact_identity_sha256")
                    != expected_sha
                or not isinstance(fact_item, Mapping)
                or set(fact_item) != _FACT_AUDIT_KEYS
                or not isinstance(probability_item, Mapping)
                or set(probability_item) != _PROBABILITY_AUDIT_KEYS
                or not isinstance(asymmetry_item, Mapping)
                or set(asymmetry_item) != _ASYMMETRY_AUDIT_KEYS):
            raise ValueError("STAGE8_OUTCOME_EVIDENCE_REPRESENTATIVE_INVALID")
        for audit in (fact_item, probability_item, asymmetry_item):
            reasons = audit.get("reasons")
            if (audit.get("validation_status") not in {"VALID", "UNKNOWN"}
                    or not isinstance(reasons, list)
                    or any(not isinstance(reason, str) or not reason
                           for reason in reasons)
                    or reasons != sorted(set(reasons))):
                raise ValueError("STAGE8_OUTCOME_EVIDENCE_AUDIT_INVALID")
        fact_valid = fact_item.get("validation_status") == "VALID"
        try:
            durable_fact_row_sha256 = contract.digest(durable_fact)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("STAGE8_OUTCOME_DURABLE_FACT_ROW_INVALID") from exc
        if (fact_item.get("expected_selection_fact_identity_sha256") != expected_sha
                or fact_item.get("selection_fact_identity_sha256") != actual_sha
                or fact_item.get("fact_record_sha256")
                    != durable_fact.get("fact_record_sha256")
                or fact_item.get("fact_sha256") != durable_fact.get("fact_sha256")
                or fact_item.get("fact_row_sha256") != durable_fact_row_sha256
                or (fact_valid and (
                    fact_item.get("reasons") != []
                    or not _valid_hash(fact_item.get("event_row_sha256"))))
                or (not fact_valid and not fact_item.get("reasons"))
                or (fact_item.get("event_row_sha256") is not None
                    and not _valid_hash(fact_item.get("event_row_sha256")))):
            raise ValueError("STAGE8_OUTCOME_FACT_AUDIT_INVALID")
        probability_valid = probability_item.get("validation_status") == "VALID"
        if probability_valid:
            status = probability_item.get("reported_status")
            if (status not in {"SUCCESS", "FAILURE"}
                    or probability_item.get("source_status") != status
                    or probability_item.get("reasons") != []
                    or not _valid_hash(
                        probability_item.get("source_row_sha256"))):
                raise ValueError("STAGE8_OUTCOME_PROBABILITY_EVIDENCE_INVALID")
            probability_valid_count += 1
            if fact_valid:
                probability_values[parent_id] = status == "SUCCESS"
        elif (not probability_item.get("reasons")
              or (probability_item.get("source_row_sha256") is not None
                  and not _valid_hash(
                      probability_item.get("source_row_sha256")))):
            raise ValueError("STAGE8_OUTCOME_PROBABILITY_EVIDENCE_INVALID")
        asymmetry_valid = asymmetry_item.get("validation_status") == "VALID"
        if asymmetry_valid:
            mfe, mae = asymmetry_item.get("mfe_pct"), asymmetry_item.get("mae_pct")
            if (asymmetry_item.get("source_status") != "READY"
                    or asymmetry_item.get("reasons") != []
                    or not _valid_hash(asymmetry_item.get("source_row_sha256"))
                    or isinstance(mfe, bool) or not isinstance(mfe, (int, float))
                    or isinstance(mae, bool) or not isinstance(mae, (int, float))
                    or not math.isfinite(float(mfe))
                    or not math.isfinite(float(mae))
                    or min(float(mfe), float(mae)) < 0.0
                    or asymmetry_item.get("zero_denominator")
                        is not (float(mae) == 0.0)):
                raise ValueError("STAGE8_OUTCOME_ASYMMETRY_EVIDENCE_INVALID")
            asymmetry_valid_count += 1
            if fact_valid:
                asymmetry_values[parent_id] = (float(mfe), float(mae))
        elif (not asymmetry_item.get("reasons")
              or asymmetry_item.get("mfe_pct") is not None
              or asymmetry_item.get("mae_pct") is not None
              or asymmetry_item.get("zero_denominator") is not False
              or (asymmetry_item.get("source_row_sha256") is not None
                  and not _valid_hash(asymmetry_item.get("source_row_sha256")))):
            raise ValueError("STAGE8_OUTCOME_ASYMMETRY_EVIDENCE_INVALID")

        representative = {
            key: selected[key] for key in (
                "attempt_fingerprint", "anchor_slot_id", "event_id",
                "event_fingerprint", "symbol", "direction",
                "decision_time_utc", "candidate_match_knowledge_status",
                "candidate_match",
            )
        }
        representative["selection_fact_identity_sha256"] = actual_sha
        normalized = {
            "binding": deepcopy(representative_binding),
            "btc_parent_movement_id": parent_id,
            "parent_start_time_utc": selected["parent_start_time_utc"],
            "representative_status": "VALID" if fact_valid else "UNKNOWN",
            "parent_policy_version": representative_binding[
                "parent_policy_version"],
            "membership_status": "LIVE",
            "parent_evidence_eligible": True,
            "freeze_id": registry_row["freeze_id"],
            "registry_record_sha256": registry_row["registry_record_sha256"],
            "registry_verification_receipt_sha256": selection_row[
                "registry_verification_receipt_sha256"],
            "selection_attestation_sha256": None,
            "representative": representative,
            "representative_identity_sha256": None,
        }
        normalized["representative_identity_sha256"] = (
            acceptance.representative_identity_sha256(exact_binding, normalized)
        )
        if item.get("representative_identity_sha256") != normalized[
                "representative_identity_sha256"]:
            raise ValueError("STAGE8_OUTCOME_REPRESENTATIVE_IDENTITY_INVALID")
        normalized["probability_evidence"] = {
            "validation_status": probability_item["validation_status"],
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "btc_parent_movement_id": parent_id,
            "representative_identity_sha256": normalized[
                "representative_identity_sha256"],
            "event_id": selected["event_id"],
            "selection_fact_identity_sha256": actual_sha,
            "direction": selected["direction"],
            "method_version": contract.frozen_manifest()["labels"][
                "method_version"],
            "window_minutes": exact_binding["binding"]["window_minutes"],
            "threshold_bps": exact_binding["binding"]["threshold_bps"],
            "reported_status": probability_item["reported_status"],
        }
        normalized["asymmetry_evidence"] = {
            "validation_status": asymmetry_item["validation_status"],
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "btc_parent_movement_id": parent_id,
            "representative_identity_sha256": normalized[
                "representative_identity_sha256"],
            "event_id": selected["event_id"],
            "selection_fact_identity_sha256": actual_sha,
            "direction": selected["direction"],
            "method_version": contract.frozen_manifest()["labels"][
                "asymmetry_source_version"],
            "measurement_kind": "FIXED_WINDOW",
            "reported_status": asymmetry_item["source_status"] or "UNKNOWN",
            "window_minutes": exact_binding["binding"]["window_minutes"],
            "scope_price_route": exact_binding["binding"]["scope"][
                "price_route"],
            "path_complete": asymmetry_valid,
            "observation_closed": asymmetry_valid,
            "coverage_complete": asymmetry_valid,
            "mfe_pct": asymmetry_item["mfe_pct"],
            "mae_pct": asymmetry_item["mae_pct"],
        }
        normalized_rows.append(normalized)
        evidence_by_parent[parent_id] = item
    if (set(evidence_by_parent) != set(selected_by_parent)
            or evidence.get("btc_parent_movement_ids")
                != selected_parent_ids_in_order
            or evidence.get("all_representative_facts_valid")
                is not all(row["representative_status"] == "VALID"
                           for row in normalized_rows)
            or evidence.get("probability_evidence_valid_count")
                != probability_valid_count
            or evidence.get("asymmetry_evidence_valid_count")
                != asymmetry_valid_count):
        raise ValueError("STAGE8_OUTCOME_EVIDENCE_POPULATION_MISMATCH")
    # The durable selection attestation was frozen before the outcome reader
    # replayed causal source rows.  A later replay failure downgrades the
    # representative for acceptance, but it must not rewrite that historical
    # selection receipt.  Rebuild the original all-valid selection population,
    # then evaluate the fact-downgraded rows against it exactly as the adapter
    # does.
    selection_rows = deepcopy(normalized_rows)
    for row in selection_rows:
        row["representative_status"] = "VALID"
    provenance = acceptance.bind_registry_selection_receipt(
        exact_binding, selection_rows,
        freeze_id=registry_row["freeze_id"],
        frozen_at_utc=registry_row["frozen_at_utc"],
        registry_record_sha256=registry_row["registry_record_sha256"],
        registry_verification_receipt_sha256=selection_row[
            "registry_verification_receipt_sha256"],
        cohort_query_sha256=selection_row["cohort_query_sha256"],
        population_receipt_sha256=selection_row[
            "outcome_free_population_receipt_sha256"],
        source_high_water_attempt_id=selection_row[
            "source_high_water_attempt_id"],
    )
    for row in normalized_rows:
        row["selection_attestation_sha256"] = provenance["attestation_sha256"]
    selection_receipt = evaluation.get("registry_selection_receipt")
    if (not isinstance(selection_receipt, Mapping)
            or set(selection_receipt) != _REGISTRY_SELECTION_RECEIPT_KEYS):
        raise ValueError("STAGE8_OUTCOME_SELECTION_PROVENANCE_INVALID")
    common = evaluation.get("common")
    routes = evaluation.get("routes")
    probability = routes.get("PROBABILITY") if isinstance(routes, Mapping) else None
    asymmetry = routes.get("ASYMMETRY") if isinstance(routes, Mapping) else None
    probability_checks = (probability.get("checks")
                          if isinstance(probability, Mapping) else None)
    asymmetry_checks = (asymmetry.get("checks")
                        if isinstance(asymmetry, Mapping) else None)
    if (not isinstance(common, Mapping) or set(common) != {
            "passed", "blockers", "selection_provenance_complete",
            "duplicate_btc_parent_movement_ids",
        } or not isinstance(routes, Mapping)
            or set(routes) != {"PROBABILITY", "ASYMMETRY"}
            or not isinstance(probability, Mapping) or set(probability) != {
                "status", "passed", "distinct_parent_count",
                "btc_parent_movement_ids", "successes", "failures",
                "hit_rate_pct", "wilson_95_lower_pct", "checks", "exclusions",
            } or not isinstance(probability_checks, Mapping)
            or set(probability_checks) != {
                "minimum_distinct_parents", "hit_rate_pct_gte_70",
                "wilson_95_lower_pct_gte_40",
            } or not isinstance(asymmetry, Mapping) or set(asymmetry) != {
                "status", "passed", "distinct_parent_count",
                "btc_parent_movement_ids", "sum_mfe_pct", "sum_mae_pct",
                "common_window_asymmetry_ratio", "common_window_asymmetry_state",
                "common_window_favorable_dominance_pct",
                "common_window_median_paired_edge_pct", "checks", "exclusions",
            } or not isinstance(asymmetry_checks, Mapping)
            or set(asymmetry_checks) != {
                "minimum_distinct_parents",
                "common_window_asymmetry_ratio_gte_1_5",
                "common_window_favorable_dominance_pct_gte_60",
                "common_window_median_paired_edge_pct_gt_0",
            }):
        raise ValueError("STAGE8_OUTCOME_ROUTE_SCHEMA_INVALID")
    def metric_same(observed: Any, expected: float | None) -> bool:
        if expected is None:
            return observed is None
        return (not isinstance(observed, bool)
                and isinstance(observed, (int, float))
                and math.isfinite(float(observed))
                and math.isclose(float(observed), expected,
                                 rel_tol=1e-12, abs_tol=1e-12))

    probability_ids = sorted(probability_values)
    probability_count = len(probability_ids)
    successes = sum(probability_values.values())
    hit_rate = 100.0 * successes / probability_count if probability_count else None
    wilson = acceptance._frozen_wilson(
        successes, probability_count, z=1.959963984540054,
    )
    expected_probability_checks = {
        "minimum_distinct_parents": probability_count >= 5,
        "hit_rate_pct_gte_70": hit_rate is not None and hit_rate >= 70.0,
        "wilson_95_lower_pct_gte_40": wilson is not None and wilson >= 40.0,
    }
    probability_pass = all(expected_probability_checks.values())
    expected_probability_status = (
        "PASS" if probability_pass else "UNAVAILABLE" if not probability_count
        else "INSUFFICIENT" if probability_count < 5 else "FAIL"
    )
    if (probability.get("btc_parent_movement_ids") != probability_ids
            or probability.get("distinct_parent_count") != probability_count
            or probability.get("successes") != successes
            or probability.get("failures") != probability_count - successes
            or not metric_same(probability.get("hit_rate_pct"), hit_rate)
            or not metric_same(probability.get("wilson_95_lower_pct"), wilson)
            or dict(probability_checks) != expected_probability_checks
            or probability.get("passed") is not probability_pass
            or probability.get("status") != expected_probability_status):
        raise ValueError("STAGE8_OUTCOME_PROBABILITY_ROUTE_MISMATCH")

    asymmetry_ids = sorted(asymmetry_values)
    asymmetry_count = len(asymmetry_ids)
    pairs = [asymmetry_values[parent_id] for parent_id in asymmetry_ids]
    try:
        total_mfe = math.fsum(pair[0] for pair in pairs) if pairs else None
        total_mae = math.fsum(pair[1] for pair in pairs) if pairs else None
    except OverflowError as exc:
        raise ValueError("STAGE8_OUTCOME_ASYMMETRY_EVIDENCE_INVALID") from exc
    ratio = (total_mfe / total_mae
             if pairs and total_mae is not None and total_mae > 0.0 else None)
    dominance = (100.0 * sum(mfe > mae for mfe, mae in pairs) / len(pairs)
                 if pairs else None)
    paired_edge = median(mfe - mae for mfe, mae in pairs) if pairs else None
    expected_asymmetry_checks = {
        "minimum_distinct_parents": asymmetry_count >= 5,
        "common_window_asymmetry_ratio_gte_1_5": ratio is not None and ratio >= 1.5,
        "common_window_favorable_dominance_pct_gte_60":
            dominance is not None and dominance >= 60.0,
        "common_window_median_paired_edge_pct_gt_0":
            paired_edge is not None and paired_edge > 0.0,
    }
    asymmetry_pass = all(expected_asymmetry_checks.values())
    expected_asymmetry_status = (
        "PASS" if asymmetry_pass else "UNAVAILABLE" if ratio is None
        else "INSUFFICIENT" if asymmetry_count < 5 else "FAIL"
    )
    expected_asymmetry_state = (
        "FINITE" if ratio is not None
        else "ZERO_DENOMINATOR" if pairs and total_mae == 0.0
        else "DATA_MISSING"
    )
    if (asymmetry.get("btc_parent_movement_ids") != asymmetry_ids
            or asymmetry.get("distinct_parent_count") != asymmetry_count
            or not metric_same(asymmetry.get("sum_mfe_pct"), total_mfe)
            or not metric_same(asymmetry.get("sum_mae_pct"), total_mae)
            or not metric_same(asymmetry.get("common_window_asymmetry_ratio"), ratio)
            or asymmetry.get("common_window_asymmetry_state")
                != expected_asymmetry_state
            or not metric_same(
                asymmetry.get("common_window_favorable_dominance_pct"), dominance)
            or not metric_same(
                asymmetry.get("common_window_median_paired_edge_pct"), paired_edge)
            or dict(asymmetry_checks) != expected_asymmetry_checks
            or asymmetry.get("passed") is not asymmetry_pass
            or asymmetry.get("status") != expected_asymmetry_status):
        raise ValueError("STAGE8_OUTCOME_ASYMMETRY_ROUTE_MISMATCH")
    common_pass = bool(
        common.get("passed") is True and common.get("blockers") == []
        and common.get("selection_provenance_complete") is True
        and common.get("duplicate_btc_parent_movement_ids") == []
    )
    computed_atomic = common_pass and (probability_pass or asymmetry_pass)
    if (evaluation.get("atomic_gate_passed") is not computed_atomic
            or evaluation.get("structurally_eligible") is not computed_atomic):
        raise ValueError("STAGE8_OUTCOME_ROUTE_GATE_MISMATCH")
    qualified = evaluation.get("research_qualified")
    replay_verified = replay.get("all_causal_fact_semantics_verified") is True
    if (type(qualified) is not bool
            or type(evaluation.get("atomic_gate_passed")) is not bool
            or not isinstance(evaluation.get("qualification_blockers"), list)
            or evaluation.get("manifest_sha256") != contract.MANIFEST_SHA256
            or evaluation.get("exact_binding_sha256") != exact_binding["binding_sha256"]
            or evaluation.get("selection_record_sha256") != selection_record_sha256
            or evaluation.get("evaluator_version") != acceptance.VERSION
            or evaluation.get("fact_replay_receipt_sha256") != replay_sha
            or evaluation.get("evidence_receipt_sha256") != evidence_sha
            or evaluation.get("result_scope") != RESEARCH_RESULT_SCOPE
            or any(evaluation.get(key) is not False for key in (
                "live_authorized", "telegram_authorized", "trade_authorized",
                "delivery_status_required",
            ))
            or evaluation.get("live_effect") != "NONE"
            or evaluation.get("trade_execution_effect") != "NONE"
            or evaluation.get("durable_registry_persistence_verified") is not True
            or evaluation.get("registry_selection_persistence_verified") is not True
            or evaluation.get("durable_outcome_source_read_verified") is not True
            or evaluation.get("authoritative_fact_replay_verified") is not False
            or evaluation.get("durable_fact_source_authority_verified") is not False
            or evaluation.get("durable_outcome_atomic_gate_evidence_verified")
            is not False
            or qualified is not False
            or "SERVER_DB_REPLAY_ATTESTATION_REQUIRED"
                not in evaluation["qualification_blockers"]):
        raise ValueError("STAGE8_OUTCOME_EVALUATION_INVALID_OR_UNSAFE")
    # Re-run the frozen pure acceptance evaluator from the closed, validated
    # audits.  This derives common provenance, route exclusions, policy fields,
    # all summaries and the unqualified status instead of trusting declarative
    # copies inside the persistence payload.
    expected_evaluation = acceptance.evaluate(
        exact_binding, normalized_rows, selection_provenance=provenance,
    )
    expected_blockers = [
        item for item in expected_evaluation.get("qualification_blockers", [])
        if item != "DURABLE_REGISTRY_PERSISTENCE_NOT_VERIFIED"
    ]
    all_facts_valid = all(
        row["representative_status"] == "VALID" for row in normalized_rows
    )
    if not replay_verified:
        expected_blockers.append("AUTHORITATIVE_FACT_SOURCE_REPLAY_NOT_VERIFIED")
    if not all_facts_valid:
        expected_blockers.append("DURABLE_FACT_IDENTITY_INTEGRITY_UNKNOWN")
    expected_atomic = bool(expected_evaluation.get("atomic_gate_passed"))
    expected_qualified = False
    if not expected_atomic:
        expected_blockers.append("DURABLE_OUTCOME_ACCEPTANCE_GATE_NOT_PASSED")
    expected_blockers.append("SERVER_DB_REPLAY_ATTESTATION_REQUIRED")
    expected_evaluation.update({
        "research_qualified": expected_qualified,
        "status": (
            "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY"
            if expected_qualified else expected_evaluation.get("status")
        ),
        "qualification_blockers": sorted(set(expected_blockers)),
        "durable_registry_persistence_verified": True,
        "registry_selection_persistence_verified": True,
        "authoritative_fact_replay_verified": False,
        "durable_outcome_source_read_verified": True,
        "durable_outcome_atomic_gate_evidence_verified": False,
        "durable_fact_source_authority_verified": False,
        "selection_record_sha256": selection_record_sha256,
        "evidence_receipt_sha256": evidence_sha,
        "fact_replay_receipt_sha256": replay_sha,
        "result_scope": RESEARCH_RESULT_SCOPE,
        "live_authorized": False,
        "telegram_authorized": False,
        "trade_authorized": False,
    })
    expected_receipt = expected_evaluation.get("registry_selection_receipt")
    if not isinstance(expected_receipt, dict):
        raise ValueError("STAGE8_OUTCOME_SELECTION_PROVENANCE_INVALID")
    expected_receipt["persistence_verified_by_evaluator"] = True
    expected_receipt["verification_boundary"] = (
        "OUTCOME_ADAPTER_VERIFIED_DURABLE_SELECTION_AND_SAME_SNAPSHOT_OUTCOMES"
    )
    try:
        evaluation_matches = (
            contract.canonical(evaluation)
            == contract.canonical(expected_evaluation)
        )
    except (TypeError, ValueError, OverflowError):
        evaluation_matches = False
    if not evaluation_matches:
        raise ValueError("STAGE8_OUTCOME_EVALUATION_RECOMPUTATION_MISMATCH")
    return value


def append_evaluation_receipt_from_connection(
    conn: Any, exact_binding: Mapping[str, Any], *,
    persistence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Append only a closed authoritative outcome-adapter persistence payload."""
    _pin_trusted_schema(conn, expected_role=EVALUATOR_WRITER_ROLE)
    contract.validate_exact_binding(exact_binding)
    selection_record_sha256 = (
        persistence_payload.get("selection_record_sha256")
        if isinstance(persistence_payload, Mapping) else None
    )
    if not _valid_hash(selection_record_sha256):
        raise ValueError("STAGE8_EVALUATION_SELECTION_IDENTITY_INVALID")
    registry_raw = _registry_row_from_connection(
        conn, exact_binding["binding_sha256"],
    )
    selection_row = _selection_from_connection(conn, selection_record_sha256)
    if registry_raw is None or selection_row is None:
        raise LookupError("STAGE8_DURABLE_REGISTRY_OR_SELECTION_NOT_FOUND")
    registry_row = _validate_registry_row(
        registry_raw, exact_binding, require_current_implementation=True,
    )
    if selection_row.get("exact_binding_sha256") != exact_binding["binding_sha256"]:
        raise ValueError("STAGE8_EVALUATION_SELECTION_BINDING_MISMATCH")
    fact_cursor = conn.execute("""
        SELECT to_jsonb(f)::text AS durable_fact_json
        FROM research_stage8_fact_read_v1 AS f
        WHERE f.fact_batch_record_sha256=%s
        ORDER BY f.attempt_id
    """, (selection_row["fact_batch_record_sha256"],))
    durable_fact_rows: list[dict[str, Any]] = []
    for raw in fact_cursor.fetchall():
        result_row = _row(fact_cursor, raw)
        try:
            decoded = json.loads(result_row["durable_fact_json"])
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("STAGE8_EVALUATION_DURABLE_FACT_ROW_INVALID") from exc
        if not isinstance(decoded, dict):
            raise ValueError("STAGE8_EVALUATION_DURABLE_FACT_ROW_INVALID")
        durable_fact_rows.append(decoded)
    document = _validate_outcome_persistence_payload(
        exact_binding, persistence_payload,
        selection_record_sha256=selection_record_sha256,
        registry_row=registry_row, selection_row=selection_row,
        durable_fact_rows=durable_fact_rows,
    )
    conn.execute("""
        INSERT INTO research_stage8_evaluation_receipts (
            exact_binding_sha256, selection_record_sha256, persistence_payload
        ) VALUES (%s,%s,%s::jsonb)
        ON CONFLICT (selection_record_sha256, persistence_payload_sha256)
        DO NOTHING
    """, (
        exact_binding["binding_sha256"], selection_record_sha256,
        _json(document),
    ))
    stored = _query_one(conn, """
        SELECT * FROM research_stage8_evaluation_read_v1
        WHERE selection_record_sha256=%s AND persistence_payload_sha256=%s
    """, (selection_record_sha256, document["persistence_payload_sha256"]))
    server_attestation = (
        stored.get("server_replay_attestation")
        if isinstance(stored, Mapping) else None
    )
    durable_attempt_ids = sorted(
        row.get("attempt_id") for row in durable_fact_rows
        if type(row.get("attempt_id")) is int
    )
    server_entries = (
        server_attestation.get("entries")
        if isinstance(server_attestation, Mapping) else None
    )
    server_attestation_valid = (
        isinstance(server_attestation, Mapping)
        and set(server_attestation) == _SERVER_REPLAY_ATTESTATION_KEYS
        and server_attestation.get("version")
            == "stage8-db-owned-projection-replay-attestation-v1"
        and server_attestation.get("exact_binding_sha256")
            == exact_binding["binding_sha256"]
        and server_attestation.get("fact_batch_record_sha256")
            == selection_row["fact_batch_record_sha256"]
        and server_attestation.get("selection_record_sha256")
            == selection_record_sha256
        and type(server_attestation.get("attempt_count")) is int
        and server_attestation.get("attempt_count") == len(durable_fact_rows)
        and type(server_attestation.get("selected_representatives_verified"))
            is bool
        and type(server_attestation.get("all_projection_semantics_verified"))
            is bool
        and isinstance(server_entries, list)
        and len(server_entries) == len(durable_fact_rows)
        and [entry.get("attempt_id") for entry in server_entries
             if isinstance(entry, Mapping)] == durable_attempt_ids
        and all(
            isinstance(entry, Mapping)
            and set(entry) == _SERVER_REPLAY_ENTRY_KEYS
            and type(entry.get("attempt_id")) is int
            and 0 < entry["attempt_id"] <= _INT64_MAX
            and _valid_hash(entry.get(
                "persisted_projection_attestation_sha256"))
            and _valid_hash(entry.get(
                "recomputed_projection_attestation_sha256"))
            and _valid_hash(entry.get("projection_semantics_sha256"))
            and entry.get("derived_knowledge_status") in {"KNOWN", "UNKNOWN"}
            and (type(entry.get("derived_candidate_match")) is bool
                 or entry.get("derived_candidate_match") is None)
            and entry.get("derived_parent_authority_class") in {
                "LIVE", "PROVEN_NOT_CANDIDATE_ELIGIBLE",
                "PROVEN_NOT_EVIDENCE_ELIGIBLE", "UNKNOWN",
            }
            and entry.get("status") in {"VERIFIED", "UNKNOWN"}
            and (entry.get("status") != "VERIFIED"
                 or entry.get("persisted_projection_attestation_sha256")
                    == entry.get("recomputed_projection_attestation_sha256"))
            for entry in server_entries
        )
        and (server_attestation.get("all_projection_semantics_verified")
             is not True
             or (server_attestation.get("selected_representatives_verified")
                 is True
                 and all(entry.get("status") == "VERIFIED"
                         for entry in server_entries)))
    )
    if (stored is None
            or stored["persistence_payload_sha256"]
            != document["persistence_payload_sha256"]
            or contract.canonical(stored["persistence_payload"])
            != contract.canonical(document)
            or not server_attestation_valid
            or stored.get("server_replay_attestation_sha256")
            != contract.digest(server_attestation)
            or type(stored.get("server_replay_verified")) is not bool
            or server_attestation.get("all_projection_semantics_verified")
                is not stored.get("server_replay_verified")
            or type(stored.get("atomic_gate_passed")) is not bool
            or type(stored.get("research_qualified")) is not bool
            or not isinstance(stored.get("evaluation"), Mapping)
            or stored.get("evaluation_sha256")
            != contract.digest(stored.get("evaluation"))
            or stored["evaluation"].get("authoritative_fact_replay_verified")
            is not stored.get("server_replay_verified")
            or stored["evaluation"].get("durable_fact_source_authority_verified")
            is not stored.get("server_replay_verified")
            or stored["evaluation"].get("atomic_gate_passed")
            is not stored.get("atomic_gate_passed")
            or stored["evaluation"].get("research_qualified")
            is not stored.get("research_qualified")
            or stored.get("result_scope") != RESEARCH_RESULT_SCOPE
            or any(stored.get(key) is not False for key in (
                "live_authorized", "telegram_authorized", "trade_authorized",
            ))
            or any(stored["evaluation"].get(key) is not False for key in (
                "live_authorized", "telegram_authorized", "trade_authorized",
            ))
            or not isinstance(stored.get("evaluation_record"), Mapping)
            or set(stored["evaluation_record"]) != _EVALUATION_RECORD_KEYS
            or stored["evaluation_record"].get("version")
                != EVALUATION_RECORD_VERSION
            or stored["evaluation_record"].get("exact_binding_sha256")
                != exact_binding["binding_sha256"]
            or stored["evaluation_record"].get("selection_record_sha256")
                != selection_record_sha256
            or stored["evaluation_record"].get("caller_evaluation_sha256")
                != document["evaluation_sha256"]
            or stored["evaluation_record"].get("outcome_adapter_version")
                != document["outcome_adapter_version"]
            or stored["evaluation_record"].get(
                "observed_outcome_adapter_source_sha256")
                != document["outcome_adapter_source_sha256"]
            or stored["evaluation_record"].get(
                "outcome_source_manifest_sha256")
                != document["outcome_source_manifest_sha256"]
            or stored["evaluation_record"].get("transaction_identity_sha256")
                != document["transaction_identity_sha256"]
            or stored["evaluation_record"].get("fact_replay_receipt_sha256")
                != document["fact_replay_receipt_sha256"]
            or stored["evaluation_record"].get("evidence_receipt_sha256")
                != document["evidence_receipt_sha256"]
            or stored["evaluation_record"].get("evaluation_sha256")
                != stored.get("evaluation_sha256")
            or stored["evaluation_record"].get("persistence_payload_sha256")
                != document["persistence_payload_sha256"]
            or stored["evaluation_record"].get(
                "server_replay_attestation_sha256")
                != stored.get("server_replay_attestation_sha256")
            or stored["evaluation_record"].get("server_replay_verified")
                is not stored.get("server_replay_verified")
            or stored["evaluation_record"].get("atomic_gate_passed")
                is not stored.get("atomic_gate_passed")
            or stored["evaluation_record"].get("research_qualified")
                is not stored.get("research_qualified")
            or stored["evaluation_record"].get("result_scope")
                != RESEARCH_RESULT_SCOPE
            or stored["evaluation_record"].get("persisted_by")
                != EVALUATOR_WRITER_ROLE
            or stored["evaluation_record"].get("persisted_at_utc")
                != _iso_utc(stored.get("persisted_at_utc"))
            or any(stored["evaluation_record"].get(key) is not False for key in (
                "live_authorized", "telegram_authorized", "trade_authorized",
            ))
            or (stored.get("research_qualified") and (
                stored.get("server_replay_verified") is not True
                or stored.get("atomic_gate_passed") is not True
                or stored["evaluation"].get("status")
                    != "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY"
                or stored["evaluation"].get("qualification_blockers") != []
            ))
            or stored["evaluation_record_sha256"]
            != contract.digest(stored["evaluation_record"])):
        raise RuntimeError("STAGE8_EVALUATION_INSERT_OR_VERIFICATION_FAILED")
    result = deepcopy(stored)
    result["persisted_at_utc"] = _iso_utc(result["persisted_at_utc"])
    return result


def append_evaluation_receipt(
    exact_binding: Mapping[str, Any], *,
    persistence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    with _connect("RESEARCH_STAGE8_EVALUATOR_DATABASE_URL", read_only=False) as conn:
        _require_env_role(conn, EVALUATOR_WRITER_ROLE)
        result = append_evaluation_receipt_from_connection(
            conn, exact_binding, persistence_payload=persistence_payload,
        )
        conn.commit()
        return result


__all__ = [
    "VERSION", "implementation_artifacts", "expected_watch_code_manifest",
    "register_exact_binding", "register_exact_binding_from_connection",
    "registry_reference", "registry_reference_from_connection",
    "append_projection_fact_batch", "append_projection_fact_batch_from_connection",
    "append_selection_receipt", "append_selection_receipt_from_connection",
    "evaluate_verified_from_connection", "append_evaluation_receipt",
    "append_evaluation_receipt_from_connection",
]
