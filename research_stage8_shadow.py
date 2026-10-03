"""Disabled-by-default, read-only coordinator for Stage-8 Shadow research.

The coordinator has no database driver and no messaging, delivery, outbox or
trading dependency.  Its default path imports no Stage-8 component and opens
nothing.  An enabled caller must inject a small read-only dependency surface;
selection persistence is intentionally a separate command and authorization.
"""
from __future__ import annotations

from copy import deepcopy
from contextlib import AbstractContextManager
from datetime import datetime, timezone
import hashlib
import importlib
import json
import math
import os
import re
from typing import Any, Mapping, Protocol, Sequence


VERSION = "stage8-read-only-shadow-coordinator-v1"
ENABLED_ENV = "RESEARCH_STAGE8_SHADOW_ENABLED"
MODE_ENV = "RESEARCH_STAGE8_SHADOW_MODE"
DATABASE_URL_ENV = "RESEARCH_STAGE8_SHADOW_READER_DATABASE_URL"
DATABASE_TARGET_SHA256_ENV = "RESEARCH_STAGE8_SHADOW_DATABASE_TARGET_SHA256"
ROLE_ENV = "RESEARCH_STAGE8_SHADOW_EXPECTED_ROLE"
SCOPE_ENV = "RESEARCH_STAGE8_SHADOW_SCOPE_ID"
CANDIDATE_ENV = "RESEARCH_STAGE8_SHADOW_CANDIDATE_ID"
THRESHOLD_ENV = "RESEARCH_STAGE8_SHADOW_THRESHOLD_BPS"
START_ENV = "RESEARCH_STAGE8_SHADOW_START_UTC"
END_ENV = "RESEARCH_STAGE8_SHADOW_END_UTC"
MAX_ATTEMPTS_ENV = "RESEARCH_STAGE8_SHADOW_MAX_ATTEMPTS"

MODE = "READ_ONLY"
EXPECTED_ROLE = "research_stage8_reader_v1"
MAX_COHORT_ATTEMPTS = 1000
ADAPTER_MAX_ATTEMPTS = 1000
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_INT64_MAX = 9223372036854775807
_DANGEROUS_ENABLES = frozenset({
    "RESEARCH_STAGE8_SHADOW_TELEGRAM_ENABLED",
    "RESEARCH_STAGE8_SHADOW_LIVE_ENABLED",
    "RESEARCH_STAGE8_SHADOW_TRADING_ENABLED",
    "RESEARCH_STAGE8_SHADOW_OUTBOX_ENABLED",
    "RESEARCH_STAGE8_SHADOW_PERSIST_ENABLED",
})
_REQUIRED = (
    MODE_ENV, DATABASE_URL_ENV, DATABASE_TARGET_SHA256_ENV, ROLE_ENV,
    SCOPE_ENV, CANDIDATE_ENV, THRESHOLD_ENV, START_ENV, END_ENV,
    MAX_ATTEMPTS_ENV,
)
_READ_ATTESTATION_KEYS = frozenset({
    "status", "read_only", "transaction_isolation", "database_role",
    "database_target_sha256", "transaction_identity_sha256",
    "backend_pid", "transaction_started_at_utc", "database_snapshot_id",
})
_ADAPTER_LEDGER_KEYS = frozenset({
    "attempt_id", "exact_binding_sha256", "fact", "fact_authority",
    "watch_selection_attestation",
    "expected_watch_selection_attestation_sha256", "watch_code_manifest",
    "expected_watch_code_manifest_sha256", "watch_selection_observation",
    "parent_membership_source",
    "parent_membership_evidence", "noneligibility_proof",
})
_ADAPTER_AUTHORITY_KEYS = frozenset({
    "exact_binding_sha256", "expected_fact_sha256", "projection_version",
    "source_audit_version", "attempt_id", "attempt_fingerprint",
    "anchor_slot_id", "event_id", "event_fingerprint", "symbol", "direction",
    "decision_time_utc", "knowledge_status", "candidate_match",
    "watch_selection_attestation_sha256", "watch_code_manifest_sha256",
    "expected_watch_selection_attestation_sha256",
    "expected_watch_code_manifest_sha256",
    "watch_selection_observation_sha256",
    "expected_parent_membership_evidence_sha256",
    "expected_noneligibility_proof_sha256",
    "observed_fact_selection_attestation_sha256",
    "observed_fact_code_manifest_sha256", "fact_authority_sha256",
})
_ADAPTER_RESULT_KEYS = frozenset({
    "version", "manifest_sha256", "projection_version",
    "source_audit_version", "projection_mode", "exact_binding_sha256",
    "query_scope", "query_binding_sha256",
    "projection_source_manifest", "projection_source_manifest_sha256",
    "projection_module_sha256", "db_adapter_module_sha256",
    "parent_evidence_module_sha256", "transaction",
    "archive_snapshot_high_water_id", "population_receipt",
    "exact_attempt_population_receipt_sha256", "authority_receipt", "rows",
    "interpretation", "result_sha256",
})
_ADAPTER_ROW_KEYS = frozenset({
    "attempt_id", "source_status", "knowledge_status", "reasons",
    "attempt_identity", "attempt_identity_sha256",
    "watch_selection_attestation",
    "expected_watch_selection_attestation_sha256", "watch_code_manifest",
    "expected_watch_code_manifest_sha256", "watch_selection_observation",
    "projection", "fact_authorities", "fact_ledger",
})
_ADAPTER_ATTEMPT_IDENTITY_KEYS = frozenset({
    "attempt_id", "row_status", "attempt_fingerprint", "sampler_version",
    "symbol", "evaluation_status", "source_candle_open_utc",
    "decision_time_utc",
})
_ADAPTER_EXACT_PROJECTION_KEYS = frozenset({
    "version", "manifest_sha256", "projection_mode", "exact_binding_sha256",
    "symbol", "attempt_id", "attempt_fingerprint",
    "scope_resolution_status", "reasons", "fact_count", "facts",
    "facts_sha256",
})
_ADAPTER_SOURCE_FILES = frozenset({
    "research_operational_score_source_audit.py",
    "research_stage8_contract.py",
    "research_stage8_feature_projection.py",
    "research_stage8_projection_db_adapter.py",
    "research_stage8_representative_selector.py",
    "research_watch_score_capture.py",
})
_ADAPTER_QUERY_KEYS = frozenset({
    "version", "manifest_sha256", "projection_version",
    "source_audit_version", "projection_mode", "exact_binding_sha256",
    "population_kind", "requested_attempt_ids",
    "max_attempts", "max_queries", "max_wall_seconds",
    "max_statement_timeout_ms", "max_capture_age_seconds",
    "capture_selection_policy",
})
_ADAPTER_POPULATION_KEYS = frozenset({
    "version", "projection_mode", "exact_binding_sha256",
    "query_binding_sha256", "requested_attempt_ids",
    "found_attempt_ids", "missing_attempt_ids", "attempt_population_sha256",
    "outcome_free_authority_ledger_sha256", "archive_snapshot_high_water_id",
    "database_snapshot_id", "transaction_identity_sha256",
    "read_started_at_utc", "read_finished_at_utc", "read_only",
    "transaction_isolation", "population_complete", "truncated",
    "query_count", "exact_attempt_population_receipt_sha256",
    "population_receipt_sha256",
})
_ADAPTER_AUTHORITY_RECEIPT_KEYS = frozenset({
    "version", "manifest_sha256", "projection_mode", "exact_binding_sha256",
    "projection_source_manifest_sha256",
    "projection_module_sha256", "db_adapter_module_sha256",
    "parent_evidence_module_sha256", "population_receipt_sha256",
    "exact_attempt_population_receipt_sha256", "authority_rows_sha256",
    "authority_receipt_sha256",
})
_ADAPTER_TRANSACTION_KEYS = frozenset({
    "backend_pid", "transaction_started_at_utc", "database_snapshot_id",
    "read_only", "isolation", "statement_timeout_ms",
    "transaction_identity_sha256", "observed_at_utc",
})
_WATCH_OBSERVATION_KEYS = frozenset({
    "version", "selection_policy", "attempt_id", "attempt_fingerprint",
    "decision_time_utc", "archive_snapshot_high_water_id",
    "database_snapshot_id", "selection_status", "selected_snapshot_set_id",
    "selected_snapshot_key", "selected_payload_sha256",
    "selected_durably_available_at_utc", "selected_source_metadata_sha256",
    "watch_selection_observation_sha256",
})
_ADAPTER_VERSION = "stage8-projection-postgres-adapter-v1"
_ADAPTER_PROJECTION_MODE = "EXACT_BINDING_FULL_COHORT"
_ADAPTER_MAX_QUERIES = 8
_ADAPTER_POPULATION_VERSION = "stage8-projection-attempt-population-receipt-v1"
_ADAPTER_AUTHORITY_VERSION = "stage8-projection-authority-receipt-v1"
_WATCH_OBSERVATION_VERSION = "stage8-db-watch-selection-observation-v1"
_COHORT_STATUS = "COMPLETE_BOUNDED_COHORT"
_PERSISTENCE_PACKAGE_VERSION = "stage8-shadow-persistence-handoff-v1"
_PERSISTENCE_PACKAGE_KEYS = frozenset({
    "version", "status", "internal_only", "outcome_free_selection_identity",
    "raw_outcome_or_label_values_included",
    "full_coverage_receipt_is_audit_only", "write_authority_granted",
    "selection_persisted", "research_qualified", "exact_binding",
    "registry_reference", "coverage_receipt", "attempt_cohort_handoff",
    "adapter_result", "selector_result", "outcome_free_identity",
    "outcome_free_identity_sha256", "persistence_package_sha256",
})


class ReadOnlyShadowDependencies(Protocol):
    """Only methods the coordinator can invoke; there is no writer method."""

    def open_read_only_session(
        self, *, database_url: str, expected_role: str,
        database_target_sha256: str,
    ) -> AbstractContextManager[Any]: ...

    def verify_read_only_session(self, session: Any) -> Mapping[str, Any]: ...

    def registry_reference_from_connection(
        self, session: Any, exact_binding: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    def read_bounded_attempt_cohort_from_connection(
        self, session: Any, *, start_utc: str, end_utc: str,
        symbols: Sequence[str], page_size: int, max_pages: int,
    ) -> Mapping[str, Any]: ...

    def project_exact_binding_attempts_from_connection(
        self, session: Any, *, exact_binding: Mapping[str, Any],
        attempt_ids: Sequence[int],
    ) -> Mapping[str, Any]: ...


def _canonical(value: Any) -> str:
    def check(item: Any) -> None:
        if item is None or type(item) in (str, bool, int, float):
            if type(item) is float and not math.isfinite(item):
                raise ValueError("nonfinite JSON number")
            return
        if type(item) is list:
            for child in item:
                check(child)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for child in item.values():
                check(child)
            return
        raise ValueError("strict JSON required")
    check(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH.fullmatch(value) is not None


def _canonical_utc(value: str) -> tuple[str, datetime]:
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires UTC offset")
    utc = parsed.astimezone(timezone.utc)
    canonical = utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if value != canonical:
        raise ValueError("timestamp is not canonical UTC")
    return canonical, utc


def _utc_instant(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires UTC offset")
    return parsed.astimezone(timezone.utc)


def _base(status: str, *, reason: str | None = None) -> dict[str, Any]:
    value = {
        "version": VERSION,
        "status": status,
        "reason": reason,
        "enabled": status != "DISABLED",
        "mode": MODE,
        "research_qualified": False,
        "selection_persisted": False,
        "durable_outcome_evidence_verified": False,
        "telegram_authorized": False,
        "live_authorized": False,
        "trade_authorized": False,
        "outbox_authorized": False,
        "persistence_authorized": False,
    }
    return {**value, "receipt_sha256": _digest(value)}


def _finish(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result.pop("receipt_sha256", None)
    # These are invariants, not caller-selectable flags.
    result.update({
        "research_qualified": False,
        "selection_persisted": False,
        "durable_outcome_evidence_verified": False,
        "telegram_authorized": False,
        "live_authorized": False,
        "trade_authorized": False,
        "outbox_authorized": False,
        "persistence_authorized": False,
    })
    return {**result, "receipt_sha256": _digest(result)}


def _truthy(value: Any) -> bool:
    return isinstance(value, str) and value.strip().upper() in {
        "1", "TRUE", "YES", "ON", "ENABLED",
    }


def _configuration(environ: Mapping[str, str]) -> tuple[dict[str, Any] | None, dict]:
    if environ.get(ENABLED_ENV) != "TRUE":
        return None, _base("DISABLED", reason="EXPLICIT_OPT_IN_ABSENT")
    enabled_dangerous = sorted(
        name for name in _DANGEROUS_ENABLES if _truthy(environ.get(name))
    )
    if enabled_dangerous:
        return None, _finish({
            **_base("BLOCKED_DANGEROUS_CONFIGURATION",
                    reason="SHADOW_RUNTIME_AUTHORITY_FORBIDDEN"),
            "dangerous_keys": enabled_dangerous,
        })
    missing = sorted(name for name in _REQUIRED if not environ.get(name, "").strip())
    if missing:
        return None, _finish({
            **_base("CONFIGURATION_INCOMPLETE",
                    reason="EXPLICIT_SHADOW_CONFIGURATION_REQUIRED"),
            "missing_configuration": missing,
        })
    try:
        if environ[MODE_ENV] != MODE or environ[ROLE_ENV] != EXPECTED_ROLE:
            raise ValueError("mode or role mismatch")
        database_url = environ[DATABASE_URL_ENV]
        target_sha = environ[DATABASE_TARGET_SHA256_ENV]
        if database_url != database_url.strip():
            raise ValueError("database URL contains surrounding whitespace")
        if not _valid_hash(target_sha) or hashlib.sha256(
                database_url.encode("utf-8")).hexdigest() != target_sha:
            raise ValueError("database target fingerprint mismatch")
        start_text, start = _canonical_utc(environ[START_ENV])
        end_text, end = _canonical_utc(environ[END_ENV])
        threshold = int(environ[THRESHOLD_ENV])
        max_attempts = int(environ[MAX_ATTEMPTS_ENV])
        if (start >= end or not 1 <= max_attempts <= MAX_COHORT_ATTEMPTS
                or str(threshold) != environ[THRESHOLD_ENV]
                or str(max_attempts) != environ[MAX_ATTEMPTS_ENV]):
            raise ValueError("range or integer bound invalid")
    except (TypeError, ValueError, OverflowError):
        return None, _base("CONFIGURATION_INVALID",
                           reason="SHADOW_CONFIGURATION_CONTRACT_MISMATCH")
    return {
        "database_url": database_url,
        "database_target_sha256": target_sha,
        "expected_role": EXPECTED_ROLE,
        "scope_id": environ[SCOPE_ENV],
        "candidate_id": environ[CANDIDATE_ENV],
        "threshold_bps": threshold,
        "start_utc": start_text,
        "end_utc": end_text,
        "max_attempts": max_attempts,
    }, {}


def _validate_read_attestation(
    value: Mapping[str, Any], *, config: Mapping[str, Any], source_audit: Any,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _READ_ATTESTATION_KEYS:
        raise ValueError("read-only attestation shape invalid")
    try:
        identity = source_audit.transaction_identity_from_fields(
            backend_pid=value.get("backend_pid"),
            transaction_started_at_utc=value.get("transaction_started_at_utc"),
            database_snapshot_id=value.get("database_snapshot_id"),
        )
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise ValueError("read-only transaction identity invalid") from exc
    if (value.get("status") != "VERIFIED_READ_ONLY_REPEATABLE_READ"
            or value.get("read_only") is not True
            or value.get("transaction_isolation") != "REPEATABLE READ"
            or value.get("database_role") != config["expected_role"]
            or value.get("database_target_sha256")
            != config["database_target_sha256"]
            or value.get("backend_pid") != identity["backend_pid"]
            or value.get("transaction_started_at_utc")
            != identity["transaction_started_at_utc"]
            or value.get("database_snapshot_id")
            != identity["database_snapshot_id"]
            or value.get("transaction_identity_sha256")
            != identity["transaction_identity_sha256"]):
        raise ValueError("read-only attestation invalid")
    return dict(value)


def _load_components() -> tuple[Any, Any, Any, Any]:
    # Reached only after opt-in, complete configuration and dependency injection.
    contract = importlib.import_module("research_stage8_contract")
    selector = importlib.import_module("research_stage8_representative_selector")
    coverage = importlib.import_module("research_stage8_coverage_receipt")
    source_audit = importlib.import_module(
        "research_operational_score_source_audit"
    )
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    if (selector.VERSION != "stage8-outcome-blind-representative-selector-v1"
            or coverage.ATTEMPT_COHORT_HANDOFF_VERSION
            != "stage8-bounded-attempt-cohort-handoff-v1"
            or not callable(coverage.validate_attempt_cohort_handoff)
            or not callable(source_audit.transaction_identity_from_fields)):
        raise ValueError("selector version mismatch")
    return contract, selector, coverage, source_audit


def _load_default_dependencies() -> ReadOnlyShadowDependencies:
    """Load the concrete PostgreSQL seam only after exact opt-in/configuration."""
    module = importlib.import_module("research_stage8_shadow_postgres")
    factory = getattr(module, "build_dependencies", None)
    if not callable(factory):
        raise RuntimeError("Stage-8 Shadow PostgreSQL factory unavailable")
    return factory()


def _adapter_selector_inputs(
    adapter_result: Mapping[str, Any], *, exact_binding: Mapping[str, Any],
    attempt_ids: Sequence[int], contract: Any,
    transaction_attestation: Mapping[str, Any], source_audit: Any,
) -> tuple[list[dict], list[dict], dict[str, str], list[int]]:
    if (not isinstance(adapter_result, Mapping)
            or set(adapter_result) != _ADAPTER_RESULT_KEYS):
        raise ValueError("adapter result missing")
    detached = dict(adapter_result)
    result_hash = detached.pop("result_sha256", None)
    if not _valid_hash(result_hash) or contract.digest(detached) != result_hash:
        raise ValueError("adapter result hash invalid")
    query_scope = adapter_result.get("query_scope")
    population = adapter_result.get("population_receipt")
    authority_receipt = adapter_result.get("authority_receipt")
    transaction = adapter_result.get("transaction")
    rows = adapter_result.get("rows")
    manifest = adapter_result.get("projection_source_manifest")
    manifest_contract = contract.frozen_manifest()
    if (adapter_result.get("version") != _ADAPTER_VERSION
            or adapter_result.get("manifest_sha256") != contract.MANIFEST_SHA256
            or adapter_result.get("projection_version")
            != manifest_contract["projection"]["version"]
            or adapter_result.get("source_audit_version")
            != manifest_contract["source"]["audit_version"]
            or adapter_result.get("projection_mode")
            != _ADAPTER_PROJECTION_MODE
            or adapter_result.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or not isinstance(query_scope, Mapping)
            or set(query_scope) != _ADAPTER_QUERY_KEYS
            or query_scope.get("version") != _ADAPTER_VERSION
            or query_scope.get("manifest_sha256") != contract.MANIFEST_SHA256
            or query_scope.get("projection_version")
            != adapter_result.get("projection_version")
            or query_scope.get("source_audit_version")
            != adapter_result.get("source_audit_version")
            or query_scope.get("projection_mode")
            != _ADAPTER_PROJECTION_MODE
            or query_scope.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or query_scope.get("population_kind")
            != "EXACT_BINDING_FULL_COHORT_ATTEMPT_IDS"
            or query_scope.get("max_attempts") != ADAPTER_MAX_ATTEMPTS
            or query_scope.get("max_queries") != _ADAPTER_MAX_QUERIES
            or query_scope.get("requested_attempt_ids") != list(attempt_ids)
            or adapter_result.get("query_binding_sha256")
            != contract.digest(query_scope)
            or not isinstance(population, Mapping)
            or set(population) != _ADAPTER_POPULATION_KEYS
            or population.get("requested_attempt_ids") != list(attempt_ids)
            or population.get("projection_mode") != _ADAPTER_PROJECTION_MODE
            or population.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or population.get("population_complete") is not True
            or population.get("truncated") is not False
            or population.get("read_only") is not True
            or population.get("transaction_isolation") != "repeatable read"
            or type(population.get("query_count")) is not int
            or not 1 <= population["query_count"] <= _ADAPTER_MAX_QUERIES
            or not isinstance(authority_receipt, Mapping)
            or set(authority_receipt) != _ADAPTER_AUTHORITY_RECEIPT_KEYS
            or authority_receipt.get("projection_mode")
            != _ADAPTER_PROJECTION_MODE
            or authority_receipt.get("exact_binding_sha256")
            != exact_binding["binding_sha256"]
            or not isinstance(transaction, Mapping)
            or set(transaction) != _ADAPTER_TRANSACTION_KEYS
            or not isinstance(rows, list) or len(rows) != len(attempt_ids)
            or not isinstance(manifest, Mapping)
            or set(manifest) != _ADAPTER_SOURCE_FILES
            or any(not _valid_hash(value) for value in manifest.values())
            or adapter_result.get("projection_source_manifest_sha256")
            != contract.digest(manifest)):
        raise ValueError("adapter population incomplete")
    found_ids = population.get("found_attempt_ids")
    missing_ids = population.get("missing_attempt_ids")
    if (not isinstance(found_ids, list) or not isinstance(missing_ids, list)
            or found_ids != sorted(set(found_ids))
            or missing_ids != sorted(set(missing_ids))
            or set(found_ids) & set(missing_ids)
            or sorted(found_ids + missing_ids) != list(attempt_ids)):
        raise ValueError("adapter found/missing population partition invalid")
    try:
        adapter_identity = source_audit.transaction_identity_from_fields(
            backend_pid=transaction.get("backend_pid"),
            transaction_started_at_utc=transaction.get(
                "transaction_started_at_utc"
            ),
            database_snapshot_id=transaction.get("database_snapshot_id"),
        )
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise ValueError("adapter transaction identity invalid") from exc
    if (transaction.get("read_only") is not True
            or transaction.get("isolation") != "repeatable read"
            or transaction.get("backend_pid") != adapter_identity["backend_pid"]
            or transaction.get("transaction_started_at_utc")
            != adapter_identity["transaction_started_at_utc"]
            or transaction.get("database_snapshot_id")
            != adapter_identity["database_snapshot_id"]
            or transaction.get("transaction_identity_sha256")
            != adapter_identity["transaction_identity_sha256"]
            or transaction.get("transaction_identity_sha256")
            != transaction_attestation["transaction_identity_sha256"]
            or transaction.get("database_snapshot_id")
            != transaction_attestation["database_snapshot_id"]
            or population.get("transaction_identity_sha256")
            != transaction["transaction_identity_sha256"]
            or population.get("database_snapshot_id")
            != transaction["database_snapshot_id"]
            or population.get("archive_snapshot_high_water_id")
            != adapter_result.get("archive_snapshot_high_water_id")):
        raise ValueError("adapter transaction authority invalid")
    population_unsigned = dict(population)
    population_sha = population_unsigned.pop("population_receipt_sha256", None)
    exact_population_sha = population_unsigned.pop(
        "exact_attempt_population_receipt_sha256", None,
    )
    if (population.get("version") != _ADAPTER_POPULATION_VERSION
            or not _valid_hash(population_sha)
            or not _valid_hash(exact_population_sha)
            or contract.digest(population_unsigned) != exact_population_sha
            or contract.digest({
                **population_unsigned,
                "exact_attempt_population_receipt_sha256": exact_population_sha,
            }) != population_sha
            or adapter_result.get("exact_attempt_population_receipt_sha256")
            != exact_population_sha
            or population.get("query_binding_sha256")
            != adapter_result.get("query_binding_sha256")
            or type(adapter_result.get("archive_snapshot_high_water_id")) is not int
            or adapter_result["archive_snapshot_high_water_id"] < 0):
        raise ValueError("adapter population receipt invalid")
    authority_unsigned = dict(authority_receipt)
    authority_sha = authority_unsigned.pop("authority_receipt_sha256", None)
    if (authority_receipt.get("version") != _ADAPTER_AUTHORITY_VERSION
            or authority_receipt.get("manifest_sha256")
            != contract.MANIFEST_SHA256
            or not _valid_hash(authority_sha)
            or contract.digest(authority_unsigned) != authority_sha
            or authority_receipt.get("population_receipt_sha256") != population_sha
            or authority_receipt.get("exact_attempt_population_receipt_sha256")
            != exact_population_sha
            or authority_receipt.get("projection_source_manifest_sha256")
            != adapter_result["projection_source_manifest_sha256"]
            or authority_receipt.get("projection_module_sha256")
            != adapter_result.get("projection_module_sha256")
            or authority_receipt.get("db_adapter_module_sha256")
            != adapter_result.get("db_adapter_module_sha256")
            or authority_receipt.get("parent_evidence_module_sha256")
            != adapter_result.get("parent_evidence_module_sha256")):
        raise ValueError("adapter authority receipt invalid")
    projection_sha = manifest.get("research_stage8_feature_projection.py")
    selector_sha = manifest.get("research_stage8_representative_selector.py")
    if (not _valid_hash(projection_sha) or not _valid_hash(selector_sha)
            or adapter_result.get("projection_module_sha256") != projection_sha
            or adapter_result.get("db_adapter_module_sha256")
            != manifest["research_stage8_projection_db_adapter.py"]
            or adapter_result.get("parent_evidence_module_sha256")
            != selector_sha):
        raise ValueError("adapter source manifest invalid")

    source_rows: list[dict] = []
    authorities: list[dict] = []
    seen: set[int] = set()
    if [row.get("attempt_id") if isinstance(row, Mapping) else None
            for row in rows] != list(attempt_ids):
        raise ValueError("adapter rows are not exact ordered attempt population")
    for adapter_row in rows:
        if (not isinstance(adapter_row, Mapping)
                or set(adapter_row) != _ADAPTER_ROW_KEYS):
            raise ValueError("adapter row invalid")
        attempt_id = adapter_row.get("attempt_id")
        ledgers = adapter_row.get("fact_ledger")
        attempt_identity = adapter_row.get("attempt_identity")
        fact_authorities = adapter_row.get("fact_authorities")
        if (type(attempt_id) is not int or attempt_id not in attempt_ids
                or attempt_id in seen or not isinstance(ledgers, list)
                or not isinstance(fact_authorities, list)
                or not isinstance(attempt_identity, Mapping)
                or set(attempt_identity) != _ADAPTER_ATTEMPT_IDENTITY_KEYS
                or attempt_identity.get("attempt_id") != attempt_id
                or contract.digest(attempt_identity)
                != adapter_row.get("attempt_identity_sha256")):
            raise ValueError("adapter row identity invalid")
        if attempt_id in missing_ids:
            if (attempt_identity.get("row_status") != "MISSING"
                    or adapter_row.get("source_status") != "MISSING"
                    or adapter_row.get("knowledge_status") != "UNKNOWN"
                    or ledgers != [] or fact_authorities != []
                    or adapter_row.get("projection") is not None
                    or adapter_row.get("watch_selection_attestation") is not None
                    or adapter_row.get(
                        "expected_watch_selection_attestation_sha256"
                    ) is not None
                    or adapter_row.get("watch_code_manifest") is not None
                    or adapter_row.get(
                        "expected_watch_code_manifest_sha256"
                    ) is not None
                    or adapter_row.get("watch_selection_observation") is not None):
                raise ValueError("missing adapter row fabricated authority")
            seen.add(attempt_id)
            continue
        if (attempt_id not in found_ids
                or attempt_identity.get("row_status") != "FOUND"
                or adapter_row.get("source_status") != "FOUND"
                or len(ledgers) != 1 or len(fact_authorities) != 1):
            raise ValueError("found adapter row cardinality invalid")
        projection_result = adapter_row.get("projection")
        if (not isinstance(projection_result, Mapping)
                or set(projection_result) != _ADAPTER_EXACT_PROJECTION_KEYS
                or projection_result.get("projection_mode")
                != _ADAPTER_PROJECTION_MODE
                or projection_result.get("exact_binding_sha256")
                != exact_binding["binding_sha256"]
                or projection_result.get("attempt_id") != attempt_id
                or projection_result.get("fact_count") != 1
                or not isinstance(projection_result.get("facts"), list)
                or len(projection_result["facts"]) != 1):
            raise ValueError("exact binding projection envelope invalid")
        unsigned_projection = dict(projection_result)
        facts_sha = unsigned_projection.pop("facts_sha256", None)
        if not _valid_hash(facts_sha) or contract.digest(unsigned_projection) != facts_sha:
            raise ValueError("exact binding projection hash invalid")
        ledger = ledgers[0]
        if (not isinstance(ledger, Mapping)
                or ledger.get("exact_binding_sha256")
                != exact_binding["binding_sha256"]
                or contract.canonical(projection_result["facts"][0])
                != contract.canonical(ledger.get("fact"))):
            raise ValueError("exact binding fact missing or duplicated")
        if set(ledger) != _ADAPTER_LEDGER_KEYS:
            raise ValueError("adapter ledger shape invalid")
        authority = ledger.get("fact_authority")
        if not isinstance(authority, Mapping) or set(authority) != _ADAPTER_AUTHORITY_KEYS:
            raise ValueError("adapter fact authority shape invalid")
        unsigned_authority = dict(authority)
        authority_sha = unsigned_authority.pop("fact_authority_sha256", None)
        if (not _valid_hash(authority_sha)
                or contract.digest(unsigned_authority) != authority_sha
                or authority.get("attempt_id") != attempt_id
                or authority.get("exact_binding_sha256")
                != exact_binding["binding_sha256"]
                or authority.get("projection_version")
                != adapter_result.get("projection_version")
                or authority.get("source_audit_version")
                != adapter_result.get("source_audit_version")
                or authority.get("expected_fact_sha256")
                != ledger.get("fact", {}).get("fact_sha256")
                or authority.get("expected_watch_selection_attestation_sha256")
                != ledger.get("expected_watch_selection_attestation_sha256")
                or authority.get("expected_watch_code_manifest_sha256")
                != ledger.get("expected_watch_code_manifest_sha256")
                or authority.get("watch_selection_attestation_sha256")
                != ledger.get("expected_watch_selection_attestation_sha256")
                or authority.get("watch_code_manifest_sha256")
                != ledger.get("expected_watch_code_manifest_sha256")):
            raise ValueError("adapter fact authority hash invalid")
        if (not isinstance(fact_authorities[0], Mapping)
                or contract.canonical(fact_authorities[0])
                != contract.canonical(authority)):
            raise ValueError("adapter row authority ledger mismatch")
        attestation = ledger.get("watch_selection_attestation")
        expected_selection = ledger.get(
            "expected_watch_selection_attestation_sha256"
        )
        if attestation is not None:
            if not isinstance(attestation, Mapping):
                raise ValueError("watch selection attestation invalid")
            unsigned_attestation = dict(attestation)
            supplied_attestation = unsigned_attestation.pop(
                "attestation_sha256", None
            )
            if (supplied_attestation != expected_selection
                    or contract.digest(unsigned_attestation)
                    != supplied_attestation):
                raise ValueError("watch selection attestation invalid")
        elif expected_selection is not None:
            raise ValueError("watch selection authority missing")
        watch_manifest = ledger.get("watch_code_manifest")
        expected_watch_manifest = ledger.get(
            "expected_watch_code_manifest_sha256"
        )
        if watch_manifest is not None:
            if (not isinstance(watch_manifest, Mapping)
                    or contract.digest(watch_manifest) != expected_watch_manifest):
                raise ValueError("watch code manifest invalid")
        elif expected_watch_manifest is not None:
            raise ValueError("watch code authority missing")
        observation = ledger.get("watch_selection_observation")
        if (not isinstance(observation, Mapping)
                or set(observation) != _WATCH_OBSERVATION_KEYS
                or observation.get("version") != _WATCH_OBSERVATION_VERSION
                or observation.get("attempt_id") != attempt_id
                or observation.get("database_snapshot_id")
                != transaction["database_snapshot_id"]
                or observation.get("archive_snapshot_high_water_id")
                != adapter_result["archive_snapshot_high_water_id"]):
            raise ValueError("watch selection observation invalid")
        unsigned_observation = dict(observation)
        observation_sha = unsigned_observation.pop(
            "watch_selection_observation_sha256", None
        )
        if (not _valid_hash(observation_sha)
                or contract.digest(unsigned_observation) != observation_sha
                or authority.get("watch_selection_observation_sha256")
                != observation_sha):
            raise ValueError("watch selection observation hash invalid")
        parent_evidence = ledger.get("parent_membership_evidence")
        noneligibility = ledger.get("noneligibility_proof")
        parent_source = ledger.get("parent_membership_source")
        expected_parent = authority.get(
            "expected_parent_membership_evidence_sha256"
        )
        expected_noneligible = authority.get(
            "expected_noneligibility_proof_sha256"
        )
        if (parent_evidence is None) == (noneligibility is None):
            raise ValueError("adapter parent state is not exactly one")
        if parent_evidence is not None:
            if (not isinstance(parent_source, Mapping)
                    or expected_noneligible is not None
                    or contract.digest(parent_evidence) != expected_parent):
                raise ValueError("adapter parent evidence invalid")
        else:
            if parent_source is not None or not isinstance(noneligibility, Mapping):
                raise ValueError("adapter noneligibility source invalid")
            unsigned_proof = dict(noneligibility)
            proof_sha = unsigned_proof.pop("proof_sha256", None)
            if (proof_sha != expected_noneligible
                    or contract.digest(unsigned_proof) != proof_sha
                    or expected_parent is not None):
                raise ValueError("adapter noneligibility proof invalid")
        source_rows.append({
            "attempt_id": attempt_id,
            "fact": ledger["fact"],
            "parent_membership_source": parent_source,
            "noneligibility_proof": noneligibility,
        })
        authorities.append({
            "attempt_id": attempt_id,
            "expected_fact_sha256": authority["expected_fact_sha256"],
            "expected_watch_selection_attestation_sha256":
                authority["expected_watch_selection_attestation_sha256"],
            "expected_parent_membership_evidence_sha256":
                authority["expected_parent_membership_evidence_sha256"],
            "expected_noneligibility_proof_sha256":
                authority["expected_noneligibility_proof_sha256"],
        })
        seen.add(attempt_id)
    if sorted(seen) != list(attempt_ids):
        raise ValueError("adapter attempt set mismatch")
    authority_rows = [{
        "attempt_id": row["attempt_id"],
        "attempt_identity_sha256": row["attempt_identity_sha256"],
        "expected_watch_selection_attestation_sha256":
            row["expected_watch_selection_attestation_sha256"],
        "expected_watch_code_manifest_sha256":
            row["expected_watch_code_manifest_sha256"],
        "fact_authorities": row["fact_authorities"],
    } for row in rows]
    population_entries = [{
        "attempt_id": row["attempt_id"],
        "row_status": row["attempt_identity"]["row_status"],
        "attempt_identity_sha256": row["attempt_identity_sha256"],
    } for row in rows]
    authority_rows_sha = contract.digest(authority_rows)
    if (population.get("attempt_population_sha256")
            != contract.digest(population_entries)
            or population.get("outcome_free_authority_ledger_sha256")
            != authority_rows_sha
            or authority_receipt.get("authority_rows_sha256")
            != authority_rows_sha):
        raise ValueError("adapter authority rows digest invalid")
    source_rows.sort(key=lambda item: item["attempt_id"])
    authorities.sort(key=lambda item: item["attempt_id"])
    return source_rows, authorities, {
        "projection_source_sha256": projection_sha,
        "selector_source_sha256": selector_sha,
    }, list(missing_ids)


def _safe_summary(selector_result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: selector_result.get(key) for key in (
            "status", "exact_binding_sha256", "selection_attestation_sha256",
            "selector_receipt_sha256", "outcome_free_population_receipt_sha256",
            "attempt_population_sha256", "source_high_water_attempt_id",
            "source_attempt_count", "source_authority_ledger_count",
            "source_authority_ledger_sha256", "representative_count",
            "representative_set_sha256", "population_coverage_complete",
            "candidate_match_coverage_complete", "global_blockers",
            "blocked_parents", "excluded_pre_freeze_parent_ids",
            "proven_noneligible_attempt_ids",
        )
    }


def _persistence_identity(
    *, exact_binding: Mapping[str, Any], registry_reference: Mapping[str, Any],
    cohort_handoff: Mapping[str, Any], adapter_result: Mapping[str, Any],
    selector_result: Mapping[str, Any],
) -> dict[str, Any]:
    coverage = cohort_handoff["coverage_receipt"]
    return {
        "version": "stage8-shadow-outcome-free-persistence-identity-v1",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "registry_record_sha256": registry_reference["registry_record_sha256"],
        "registry_verification_receipt_sha256":
            registry_reference["registry_verification_receipt_sha256"],
        "cohort_handoff_sha256": cohort_handoff["handoff_sha256"],
        "outcome_free_population_receipt_sha256":
            cohort_handoff["outcome_free_population_receipt_sha256"],
        "attempt_population_sha256": coverage["attempt_population_sha256"],
        "adapter_result_sha256": adapter_result["result_sha256"],
        "adapter_population_receipt_sha256":
            adapter_result["population_receipt"]["population_receipt_sha256"],
        "adapter_authority_receipt_sha256":
            adapter_result["authority_receipt"]["authority_receipt_sha256"],
        "selection_attestation_sha256":
            selector_result["selection_attestation_sha256"],
        "representative_set_sha256":
            selector_result["representative_set_sha256"],
        "source_authority_ledger_sha256":
            selector_result["source_authority_ledger_sha256"],
    }


def _build_persistence_package(
    *, exact_binding: Mapping[str, Any], registry_reference: Mapping[str, Any],
    cohort_handoff: Mapping[str, Any], adapter_result: Mapping[str, Any],
    selector_result: Mapping[str, Any], contract: Any,
) -> dict[str, Any]:
    identity = _persistence_identity(
        exact_binding=exact_binding, registry_reference=registry_reference,
        cohort_handoff=cohort_handoff, adapter_result=adapter_result,
        selector_result=selector_result,
    )
    value = {
        "version": _PERSISTENCE_PACKAGE_VERSION,
        "status": "READY_FOR_SEPARATE_APPEND_ONLY_WRITER",
        "internal_only": True,
        "outcome_free_selection_identity": True,
        "raw_outcome_or_label_values_included": False,
        "full_coverage_receipt_is_audit_only": True,
        "write_authority_granted": False,
        "selection_persisted": False,
        "research_qualified": False,
        "exact_binding": deepcopy(dict(exact_binding)),
        "registry_reference": deepcopy(dict(registry_reference)),
        "coverage_receipt": deepcopy(dict(cohort_handoff["coverage_receipt"])),
        "attempt_cohort_handoff": deepcopy(dict(cohort_handoff)),
        "adapter_result": deepcopy(dict(adapter_result)),
        "selector_result": deepcopy(dict(selector_result)),
        "outcome_free_identity": identity,
        "outcome_free_identity_sha256": contract.digest(identity),
    }
    return {**value, "persistence_package_sha256": contract.digest(value)}


def validate_persistence_package(value: Mapping[str, Any]) -> None:
    """Verify transport and outcome-free identity hashes; grant no write right."""
    if (not isinstance(value, Mapping)
            or set(value) != _PERSISTENCE_PACKAGE_KEYS):
        raise ValueError("Stage-8 persistence package is missing")
    unsigned = deepcopy(dict(value))
    supplied = unsigned.pop("persistence_package_sha256", None)
    identity = value.get("outcome_free_identity")
    if (value.get("version") != _PERSISTENCE_PACKAGE_VERSION
            or value.get("status") != "READY_FOR_SEPARATE_APPEND_ONLY_WRITER"
            or value.get("internal_only") is not True
            or value.get("outcome_free_selection_identity") is not True
            or value.get("raw_outcome_or_label_values_included") is not False
            or value.get("full_coverage_receipt_is_audit_only") is not True
            or value.get("write_authority_granted") is not False
            or value.get("selection_persisted") is not False
            or value.get("research_qualified") is not False
            or not _valid_hash(supplied) or _digest(unsigned) != supplied
            or not isinstance(identity, Mapping)
            or _digest(identity) != value.get("outcome_free_identity_sha256")):
        raise ValueError("Stage-8 persistence package hash or boundary is invalid")
    try:
        binding = value["exact_binding"]
        registry = value["registry_reference"]
        handoff = value["attempt_cohort_handoff"]
        coverage = value["coverage_receipt"]
        adapter = value["adapter_result"]
        selector = value["selector_result"]
        expected = _persistence_identity(
            exact_binding=binding, registry_reference=registry,
            cohort_handoff=handoff, adapter_result=adapter,
            selector_result=selector,
        )
        cross_layer_valid = (
            _canonical(identity) == _canonical(expected)
            and _canonical(coverage) == _canonical(handoff["coverage_receipt"])
            and binding["binding_sha256"] == registry["exact_binding_sha256"]
            and binding["binding_sha256"] == adapter["exact_binding_sha256"]
            and binding["binding_sha256"] == selector["exact_binding_sha256"]
            and handoff["status"] == _COHORT_STATUS
            and handoff["attempt_ids"]
            == adapter["query_scope"]["requested_attempt_ids"]
            and handoff["outcome_free_population_receipt_sha256"]
            == selector["outcome_free_population_receipt_sha256"]
            and coverage["receipt_sha256"]
            == selector["source_audit_receipt_sha256"]
        )
    except (TypeError, KeyError, AttributeError):
        cross_layer_valid = False
    if not cross_layer_valid:
        raise ValueError("Stage-8 persistence package components are mismatched")


def _run(config: Mapping[str, Any], dependencies: ReadOnlyShadowDependencies) -> dict:
    contract, selector, coverage, source_audit = _load_components()
    exact_binding = contract.exact_binding(
        scope_id=config["scope_id"], candidate_id=config["candidate_id"],
        threshold_bps=config["threshold_bps"],
    )
    page_size = max(1, min(
        coverage.MAX_PAGE_SIZE,
        math.ceil(config["max_attempts"] / coverage.MAX_PAGES),
    ))
    max_pages = math.ceil(config["max_attempts"] / page_size)
    calls: list[str] = []
    manager = dependencies.open_read_only_session(
        database_url=config["database_url"],
        expected_role=config["expected_role"],
        database_target_sha256=config["database_target_sha256"],
    )
    calls.append("OPEN_READ_ONLY_SESSION")
    with manager as session:
        before = _validate_read_attestation(
            dependencies.verify_read_only_session(session), config=config,
            source_audit=source_audit,
        )
        calls.append("VERIFY_READ_ONLY_BEFORE")
        registry_reference = dependencies.registry_reference_from_connection(
            session, exact_binding,
        )
        calls.append("READ_AND_VERIFY_REGISTRY")
        cohort = dependencies.read_bounded_attempt_cohort_from_connection(
            session, start_utc=config["start_utc"], end_utc=config["end_utc"],
            symbols=sorted(exact_binding["binding"]["scope"]["symbols"]),
            page_size=page_size, max_pages=max_pages,
        )
        calls.append("READ_BOUNDED_ATTEMPT_COHORT_HANDOFF")
        if not isinstance(cohort, Mapping):
            raise ValueError("cohort result invalid")
        if cohort.get("status") != _COHORT_STATUS:
            after = _validate_read_attestation(
                dependencies.verify_read_only_session(session), config=config,
                source_audit=source_audit,
            )
            calls.append("VERIFY_READ_ONLY_AFTER")
            if before != after:
                raise ValueError("read transaction changed")
            return _finish({
                "version": VERSION,
                "status": "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE",
                "reason": "COHORT_HANDOFF_NOT_COMPLETE",
                "enabled": True,
                "mode": MODE,
                "exact_binding_sha256": exact_binding["binding_sha256"],
                "database_target_sha256": config["database_target_sha256"],
                "transaction_identity_sha256":
                    before["transaction_identity_sha256"],
                "read_chain": calls,
                "selector_summary": None,
                "selection_package": None,
                "persistence_package": None,
                "selection_persisted": False,
                "durable_outcome_evidence_verified": False,
            })
        expected_handoff = cohort.get("handoff_sha256")
        if not _valid_hash(expected_handoff):
            raise ValueError("trusted cohort handoff hash missing")
        coverage.validate_attempt_cohort_handoff(
            cohort, expected_handoff_sha256=expected_handoff,
        )
        calls.append("VALIDATE_COMPLETE_COHORT_HANDOFF")
        attempt_ids = cohort.get("attempt_ids")
        receipt = cohort.get("coverage_receipt")
        expected_population = cohort.get(
            "outcome_free_population_receipt_sha256"
        )
        if (not isinstance(attempt_ids, list)
                or attempt_ids != sorted(set(attempt_ids))
                or len(attempt_ids) > config["max_attempts"]
                or any(type(item) is not int or not 0 < item <= _INT64_MAX
                       for item in attempt_ids)
                or not isinstance(receipt, Mapping)
                or not _valid_hash(expected_population)
                or receipt.get("outcome_free_population_receipt_sha256")
                != expected_population):
            raise ValueError("cohort is empty, partial or malformed")
        query_scope = receipt.get("query_scope")
        if (not isinstance(query_scope, Mapping)
                or query_scope.get("symbols")
                != sorted(exact_binding["binding"]["scope"]["symbols"])
                or _utc_instant(query_scope.get("start_utc"))
                != _utc_instant(config["start_utc"])
                or _utc_instant(query_scope.get("end_utc"))
                != _utc_instant(config["end_utc"])
                or receipt.get("transaction_identity_sha256")
                != before["transaction_identity_sha256"]):
            raise ValueError("cohort query or transaction mismatch")
        if not attempt_ids:
            after = _validate_read_attestation(
                dependencies.verify_read_only_session(session), config=config,
                source_audit=source_audit,
            )
            calls.append("VERIFY_READ_ONLY_AFTER")
            if before != after:
                raise ValueError("read transaction changed")
            return _finish({
                "version": VERSION,
                "status": "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE",
                "reason": "COMPLETE_COHORT_IS_EMPTY",
                "enabled": True,
                "mode": MODE,
                "exact_binding_sha256": exact_binding["binding_sha256"],
                "database_target_sha256": config["database_target_sha256"],
                "transaction_identity_sha256":
                    before["transaction_identity_sha256"],
                "read_chain": calls,
                "selector_summary": None,
                "selection_package": None,
                "persistence_package": None,
                "selection_persisted": False,
                "durable_outcome_evidence_verified": False,
            })
        adapter_result = (
            dependencies.project_exact_binding_attempts_from_connection(
                session, exact_binding=exact_binding, attempt_ids=attempt_ids,
            )
        )
        calls.append("READ_EXACT_BINDING_FACTS_AND_PARENT_EVIDENCE")
        source_rows, authorities, observed_hashes, missing_ids = (
            _adapter_selector_inputs(
                adapter_result, exact_binding=exact_binding,
                attempt_ids=attempt_ids, contract=contract,
                transaction_attestation=before, source_audit=source_audit,
            )
        )
        if missing_ids:
            selector_result = None
        else:
            selector_result = selector.select_representatives(
                exact_binding, source_rows, fact_authorities=authorities,
                population_receipt=receipt,
                expected_outcome_free_population_receipt_sha256=
                    expected_population,
                registry_reference=registry_reference,
                observed_source_hashes=observed_hashes,
            )
            calls.append("SELECT_OUTCOME_BLIND_REPRESENTATIVES")
        after = _validate_read_attestation(
            dependencies.verify_read_only_session(session), config=config,
            source_audit=source_audit,
        )
        calls.append("VERIFY_READ_ONLY_AFTER")
        if before != after:
            raise ValueError("read transaction changed")

    if missing_ids:
        summary = {
            "status": "BLOCKED",
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "source_attempt_count": len(attempt_ids),
            "global_blockers": ["ADAPTER_ATTEMPT_ROWS_MISSING"],
            "missing_attempt_ids": missing_ids,
            "population_coverage_complete": False,
            "candidate_match_coverage_complete": False,
        }
        return _finish({
            "version": VERSION,
            "status": "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE",
            "reason": "ADAPTER_DID_NOT_FIND_EVERY_COHORT_ATTEMPT",
            "enabled": True,
            "mode": MODE,
            "exact_binding_sha256": exact_binding["binding_sha256"],
            "database_target_sha256": config["database_target_sha256"],
            "transaction_identity_sha256": before["transaction_identity_sha256"],
            "read_chain": calls,
            "selector_summary": summary,
            "selection_package": None,
            "persistence_package": None,
            "selection_persisted": False,
            "durable_outcome_evidence_verified": False,
        })
    summary = _safe_summary(selector_result)
    if selector_result.get("status") != "COMPLETE":
        status = "BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE"
        reason = "SELECTOR_DID_NOT_PRODUCE_A_COMPLETE_OUTCOME_BLIND_BATCH"
        package = None
        persistence_package = None
    else:
        status = "AWAITING_SEPARATE_SELECTION_PERSISTENCE"
        reason = "READ_ONLY_SHADOW_CANNOT_PERSIST_OR_VERIFY_OUTCOMES"
        package = selector_result
        persistence_package = _build_persistence_package(
            exact_binding=exact_binding,
            registry_reference=registry_reference,
            cohort_handoff=cohort,
            adapter_result=adapter_result,
            selector_result=selector_result,
            contract=contract,
        )
        validate_persistence_package(persistence_package)
    return _finish({
        "version": VERSION,
        "status": status,
        "reason": reason,
        "enabled": True,
        "mode": MODE,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "database_target_sha256": config["database_target_sha256"],
        "transaction_identity_sha256": before["transaction_identity_sha256"],
        "read_chain": calls,
        "selector_summary": summary,
        "selection_package": package,
        "persistence_package": persistence_package,
        "selection_persisted": False,
        "durable_outcome_evidence_verified": False,
    })


def run_shadow_from_environment(
    environ: Mapping[str, str] | None = None, *,
    dependencies: ReadOnlyShadowDependencies | None = None,
) -> dict[str, Any]:
    """Run only after exact opt-in/config; otherwise perform no external work."""
    values = os.environ if environ is None else environ
    config, early = _configuration(values)
    if config is None:
        return early
    if dependencies is None:
        try:
            dependencies = _load_default_dependencies()
        except Exception:
            return _finish({
                **_base(
                    "DEPENDENCIES_UNAVAILABLE",
                    reason="READ_ONLY_SHADOW_POSTGRES_DEPENDENCIES_UNAVAILABLE",
                ),
                "enabled": True,
            })
    try:
        return _run(config, dependencies)
    except Exception:
        # Driver errors and DSNs are deliberately never copied into a receipt.
        return _finish({
            **_base("READ_ONLY_CHAIN_FAILED", reason="FAIL_CLOSED_READ_ERROR"),
            "enabled": True,
        })


def main() -> int:
    result = run_shadow_from_environment()
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "DISABLED" else 3


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "VERSION", "MAX_COHORT_ATTEMPTS", "ReadOnlyShadowDependencies",
    "validate_persistence_package", "run_shadow_from_environment", "main",
]
