"""Trusted, bounded PostgreSQL producer for Stage-8 projection facts.

The caller supplies an already-open mapping-row PostgreSQL connection whose
current transaction is read-only and REPEATABLE READ.  This module neither
opens a connection nor changes transaction state.  It reads only the v4
attempt/slot/event authority, the Watch archive, and exact-policy BTC parents
and source bars. Membership is derived at the event's decision time. Outcome,
delivery, Telegram, LIVE and trading tables are outside this adapter.

Population completeness here means completeness of the exact, bounded list of
attempt IDs supplied by the caller.  It is deliberately not a claim that the
list is the complete Stage-8 corpus.  Missing and malformed source rows remain
explicit UNKNOWN records.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import time
from typing import Any, Callable, Mapping, Sequence

import research_operational_score_source_audit as source_audit
import research_btc_parent_movement as btc_parent
import research_stage8_contract as contract
import research_stage8_feature_projection as projection
import research_stage8_representative_selector as representative_selector
import research_watch_score_capture as watch_capture


VERSION = "stage8-projection-postgres-adapter-v1"
POPULATION_RECEIPT_VERSION = "stage8-projection-attempt-population-receipt-v1"
AUTHORITY_RECEIPT_VERSION = "stage8-projection-authority-receipt-v1"
WATCH_OBSERVATION_VERSION = "stage8-db-watch-selection-observation-v1"
FIRST_TRANCHE_PROJECTION_MODE = "FIRST_TRANCHE_ALL_BINDINGS"
EXACT_BINDING_PROJECTION_MODE = "EXACT_BINDING_FULL_COHORT"
MAX_ATTEMPTS = 32
MAX_EXACT_BINDING_ATTEMPTS = 1_000
MAX_QUERIES = 8
MAX_WALL_SECONDS = 30.0
MAX_STATEMENT_TIMEOUT_MS = 10_000.0
_INT64_MAX = 9223372036854775807
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_STARTUP_MANIFEST = contract.frozen_manifest()
contract.validate_manifest(_STARTUP_MANIFEST)
_MANIFEST_SHA256 = contract.MANIFEST_SHA256
_SOURCE_AUDIT_VERSION = _STARTUP_MANIFEST["source"]["audit_version"]
_PROJECTION_VERSION = _STARTUP_MANIFEST["projection"]["version"]
_MAX_CAPTURE_AGE_SECONDS = _STARTUP_MANIFEST["source"]["max_capture_age_seconds"]

_TRANSACTION_SQL = """/* stage8-projection:transaction */ SELECT
    current_setting('transaction_read_only') AS read_only,
    current_setting('transaction_isolation') AS isolation,
    current_setting('statement_timeout') AS statement_timeout,
    pg_backend_pid() AS backend_pid,
    transaction_timestamp() AS transaction_started_at_utc,
    pg_current_snapshot()::text AS transaction_snapshot,
    clock_timestamp() AS observed_at_utc"""

_ARCHIVE_HIGH_WATER_SQL = """/* stage8-projection:archive-high-water */
    SELECT COALESCE(MAX(snapshot_set_id), 0)::bigint AS archive_snapshot_high_water_id
    FROM research_max_pain_snapshot_sets
    WHERE source='WATCH_SHARED'"""

_ATTEMPTS_SQL = """/* stage8-projection:attempts */ SELECT a.*
    FROM research_prospective_anchor_attempts AS a
    WHERE a.attempt_id=ANY(%(attempt_ids)s::bigint[])
    ORDER BY a.attempt_id ASC
    LIMIT %(limit)s"""

_SLOTS_SQL = """/* stage8-projection:slots */
    SELECT a.attempt_id AS adapter_attempt_id, s.*
    FROM research_prospective_anchor_attempts AS a
    JOIN research_prospective_anchor_slots AS s
      ON s.sampler_version=a.sampler_version
     AND s.symbol=a.symbol
     AND s.source_candle_open_utc=a.source_candle_open_utc
    WHERE a.attempt_id=ANY(%(attempt_ids)s::bigint[])
    ORDER BY a.attempt_id ASC, s.anchor_slot_id ASC
    LIMIT %(limit)s"""

_EVENTS_SQL = """/* stage8-projection:events */ SELECT e.*
    FROM research_events AS e
    WHERE e.event_id=ANY(%(event_ids)s::bigint[])
    ORDER BY e.event_id ASC
    LIMIT %(limit)s"""

_PARENT_SOURCES_SQL = """/* stage8-projection:parent-sources */
    SELECT e.event_id AS adapter_event_id,
           jsonb_build_object(
               'event_id', e.event_id,
               'episode_policy_version', %(parent_policy_version)s,
               'decision_time_utc', e.alert_time_utc,
               'btc_parent_movement_id', CASE
                   WHEN authority.compatible AND NOT (
                       p.evidence_eligible IS TRUE
                       AND p.confirmed_at_utc IS NOT NULL
                       AND p.confirmed_at_utc > e.alert_time_utc
                   ) THEN p.btc_parent_movement_id ELSE NULL END,
               'btc_observed_close_utc', CASE
                   WHEN authority.compatible THEN b.close_time_utc ELSE NULL END,
               'membership_status', CASE
                   WHEN NOT authority.compatible OR (
                       p.evidence_eligible IS TRUE
                       AND p.confirmed_at_utc IS NOT NULL
                       AND p.confirmed_at_utc > e.alert_time_utc
                   ) THEN 'BTC_DATA_MISSING'
                   WHEN p.evidence_eligible IS TRUE
                        AND p.confirmed_at_utc IS NOT NULL THEN 'LIVE'
                   ELSE 'BOUNDARY_UNVERIFIED' END
           )::text AS adapter_membership_json,
           to_jsonb(p)::text AS adapter_parent_json,
           to_jsonb(b)::text AS adapter_btc_bar_json
    FROM research_events AS e
    LEFT JOIN LATERAL (
        SELECT parent.* FROM research_btc_parent_movements AS parent
        WHERE parent.episode_policy_version=%(parent_policy_version)s
          AND parent.start_time_utc <= e.alert_time_utc
          AND (parent.end_time_utc IS NULL OR e.alert_time_utc < parent.end_time_utc)
        ORDER BY parent.start_time_utc DESC, parent.btc_parent_movement_id COLLATE "C"
        LIMIT 1
    ) AS p ON TRUE
    LEFT JOIN LATERAL (
        SELECT bar.* FROM research_btc_price_bars AS bar
        WHERE bar.close_time_utc <= e.alert_time_utc
        ORDER BY bar.close_time_utc DESC
        LIMIT 1
    ) AS b ON TRUE
    CROSS JOIN LATERAL (
        SELECT COALESCE(
            p.btc_parent_movement_id IS NOT NULL
            AND b.close_time_utc IS NOT NULL
            AND b.close_time_utc > e.alert_time_utc - interval '1 minute'
            AND b.close_time_utc >= p.start_time_utc, FALSE
        ) AS compatible
    ) AS authority
    WHERE e.event_id=ANY(%(event_ids)s::bigint[])
    ORDER BY e.event_id ASC
    LIMIT %(limit)s"""

_WATCH_SELECTION_SQL = """/* stage8-projection:watch-selection */
    SELECT a.attempt_id AS adapter_attempt_id,
           (s.snapshot_set_id IS NOT NULL) AS adapter_has_snapshot,
           s.*
    FROM research_prospective_anchor_attempts AS a
    LEFT JOIN LATERAL (
        SELECT w.*
        FROM research_max_pain_snapshot_sets AS w
        WHERE w.source='WATCH_SHARED'
          AND w.snapshot_set_id <= %(archive_snapshot_high_water_id)s
          AND w.available_at_utc IS NOT NULL
          AND w.created_at_utc IS NOT NULL
          AND a.decision_time_utc IS NOT NULL
          AND GREATEST(w.available_at_utc, w.created_at_utc)
              <= a.decision_time_utc
          AND GREATEST(w.available_at_utc, w.created_at_utc)
              >= a.decision_time_utc - %(max_capture_age)s
        ORDER BY GREATEST(w.available_at_utc, w.created_at_utc) DESC,
                 w.snapshot_set_id DESC
        LIMIT 1
    ) AS s ON TRUE
    WHERE a.attempt_id=ANY(%(attempt_ids)s::bigint[])
    ORDER BY a.attempt_id ASC
    LIMIT %(limit)s"""


class ProjectionAdapterError(ValueError):
    """Stable fail-closed boundary for adapter contract violations."""


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _positive_id(value: Any) -> bool:
    return type(value) is int and 0 < value <= _INT64_MAX


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _timeout_ms(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("invalid statement_timeout")
    if isinstance(value, (int, float)):
        amount, unit = float(value), "ms"
    else:
        match = re.fullmatch(
            r"\s*([0-9]+(?:\.[0-9]+)?)\s*(ms|s|min|h)?\s*", str(value), re.I
        )
        if match is None:
            raise ValueError("invalid statement_timeout")
        amount = float(match.group(1))
        unit = (match.group(2) or "ms").lower()
    multiplier = {"ms": 1.0, "s": 1_000.0, "min": 60_000.0, "h": 3_600_000.0}[unit]
    result = amount * multiplier
    if not math.isfinite(result):
        raise ValueError("invalid statement_timeout")
    return result


def _assert_runtime_contract() -> None:
    try:
        contract.validate_manifest(contract.frozen_manifest())
        if (contract.MANIFEST_SHA256 != _MANIFEST_SHA256
                or source_audit.VERSION != _SOURCE_AUDIT_VERSION
                or projection.VERSION != _PROJECTION_VERSION
                or projection.WATCH_SELECTION_ATTESTATION_VERSION
                != "stage8-watch-db-selection-attestation-v1"
                or projection.WATCH_SELECTION_QUERY_VERSION
                != "stage8-watch-db-selection-query-v1"
                or projection.WATCH_SELECTION_POLICY
                != _STARTUP_MANIFEST["source"]["capture_selection"]):
            raise ValueError("version mismatch")
    except (TypeError, ValueError, KeyError) as exc:
        raise ProjectionAdapterError("STAGE8_PROJECTION_RUNTIME_CONTRACT_MISMATCH") from exc


def _module_path(module: Any) -> Path:
    path = Path(str(module.__file__)).resolve()
    if path.suffix in (".pyc", ".pyo") and path.with_suffix(".py").is_file():
        path = path.with_suffix(".py")
    if not path.is_file():
        raise ProjectionAdapterError("STAGE8_PROJECTION_SOURCE_CODE_UNAVAILABLE")
    return path


def _projection_source_manifest() -> tuple[dict[str, str], str]:
    paths = {
        "research_stage8_projection_db_adapter.py": Path(__file__).resolve(),
        "research_stage8_feature_projection.py": _module_path(projection),
        "research_stage8_contract.py": _module_path(contract),
        "research_operational_score_source_audit.py": _module_path(source_audit),
        "research_stage8_representative_selector.py": _module_path(
            representative_selector
        ),
        "research_watch_score_capture.py": _module_path(watch_capture),
    }
    manifest = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in sorted(paths.items())
    }
    if any(not _valid_hash(value) for value in manifest.values()):
        raise ProjectionAdapterError("STAGE8_PROJECTION_SOURCE_CODE_UNAVAILABLE")
    return manifest, contract.digest(manifest)


def _attempt_identity(attempt_id: int, row: Mapping[str, Any] | None) -> dict:
    if row is None:
        return {
            "attempt_id": attempt_id,
            "row_status": "MISSING",
            "attempt_fingerprint": None,
            "sampler_version": None,
            "symbol": None,
            "evaluation_status": None,
            "source_candle_open_utc": None,
            "decision_time_utc": None,
        }
    identity = {
        "attempt_id": attempt_id,
        "row_status": "FOUND",
        "attempt_fingerprint": (row.get("attempt_fingerprint")
                                if isinstance(row.get("attempt_fingerprint"), str) else None),
        "sampler_version": (row.get("sampler_version")
                            if isinstance(row.get("sampler_version"), str) else None),
        "symbol": row.get("symbol") if isinstance(row.get("symbol"), str) else None,
        "evaluation_status": (row.get("evaluation_status")
                              if isinstance(row.get("evaluation_status"), str) else None),
        "source_candle_open_utc": None,
        "decision_time_utc": None,
    }
    for key in ("source_candle_open_utc", "decision_time_utc"):
        try:
            identity[key] = _iso(row.get(key)) if row.get(key) is not None else None
        except (TypeError, ValueError, OverflowError):
            identity[key] = None
    return identity


def _json_mapping(value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    try:
        decoded = json.loads(value) if isinstance(value, str) else deepcopy(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_SOURCE_INVALID") from exc
    if not isinstance(decoded, Mapping):
        raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_SOURCE_INVALID")
    return dict(decoded)


def _json_safe(value: Any) -> Any:
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_SOURCE_INVALID")
        return 0.0 if value == 0.0 else value
    if isinstance(value, Decimal):
        result = float(value)
        if not math.isfinite(result):
            raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_SOURCE_INVALID")
        return 0.0 if result == 0.0 else result
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_SOURCE_INVALID")
        return {key: _json_safe(child) for key, child in value.items()}
    if type(value) in (list, tuple):
        return [_json_safe(child) for child in value]
    raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_SOURCE_INVALID")


def _exact_parent_source(*, event: Mapping[str, Any] | None,
                         membership: Mapping[str, Any] | None,
                         parent: Mapping[str, Any] | None,
                         btc_bar: Mapping[str, Any] | None) -> dict:
    def exact(value: Mapping[str, Any] | None, keys: Sequence[str]):
        return ({key: _json_safe(value.get(key)) for key in keys}
                if isinstance(value, Mapping) else None)

    return {
        "event": exact(event, (
            "event_id", "event_fingerprint", "alert_time_utc", "symbol", "direction",
        )),
        "membership": exact(membership, (
            "event_id", "episode_policy_version", "btc_parent_movement_id",
            "decision_time_utc", "btc_observed_close_utc", "membership_status",
        )),
        "parent": exact(parent, (
            "btc_parent_movement_id", "episode_policy_version", "start_time_utc",
            "end_time_utc", "confirmed_at_utc", "direction", "evidence_eligible",
            "boundary_reason", "observed_through_utc", "price_source", "state_json",
        )),
        "btc_bar": exact(btc_bar, (
            "open_time_utc", "close_time_utc", "open", "high", "low", "close",
            "price_source",
        )),
    }


def _noneligibility_proof(attempt: Mapping[str, Any]) -> dict | None:
    status = attempt.get("evaluation_status")
    reason = {
        "UNEVALUABLE": "ATTEMPT_UNEVALUABLE_NO_DECISION",
        "COVERAGE_EXCLUDED": "ATTEMPT_COVERAGE_EXCLUDED_NO_DECISION",
    }.get(status)
    if reason is None:
        return None
    value = {
        "version": representative_selector.NONELIGIBILITY_VERSION,
        "attempt_id": attempt.get("attempt_id"),
        "attempt_fingerprint": attempt.get("attempt_fingerprint"),
        "sampler_version": attempt.get("sampler_version"),
        "symbol": attempt.get("symbol"),
        "evaluation_status": status,
        "decision_time_utc": None,
        "reason": reason,
    }
    value["proof_sha256"] = contract.digest(value)
    return value


def _transaction_identity(row: Mapping[str, Any], conn: Any) -> dict:
    try:
        timeout_ms = _timeout_ms(row.get("statement_timeout"))
        isolation = str(row.get("isolation") or "").strip().lower()
        read_only = row.get("read_only") in (True, "on")
        if (not read_only or isolation != "repeatable read"
                or getattr(conn, "autocommit", None) is not False
                or not 0 < timeout_ms <= MAX_STATEMENT_TIMEOUT_MS
                or not _positive_id(row.get("backend_pid"))
                or not isinstance(row.get("transaction_snapshot"), str)
                or not row["transaction_snapshot"].strip()):
            raise ValueError("transaction contract mismatch")
        shared_identity = source_audit.transaction_identity_from_fields(
            backend_pid=row["backend_pid"],
            transaction_started_at_utc=row.get("transaction_started_at_utc"),
            database_snapshot_id=row["transaction_snapshot"],
        )
        identity = {
            "backend_pid": shared_identity["backend_pid"],
            "transaction_started_at_utc": shared_identity["transaction_started_at_utc"],
            "database_snapshot_id": shared_identity["database_snapshot_id"],
            "read_only": True,
            "isolation": "repeatable read",
            "statement_timeout_ms": timeout_ms,
        }
        identity["transaction_identity_sha256"] = shared_identity["transaction_identity_sha256"]
        identity["observed_at_utc"] = _iso(row.get("observed_at_utc"))
        return identity
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise ProjectionAdapterError(
            "STAGE8_PROJECTION_REQUIRES_READ_ONLY_REPEATABLE_READ_TRANSACTION"
        ) from exc


def _selection_attestation(*, attempt: Mapping[str, Any],
                           snapshot: Mapping[str, Any] | None,
                           archive_high_water: int,
                           transaction: Mapping[str, Any]) -> dict:
    decision = _iso(attempt.get("decision_time_utc"))
    query_scope = {
        "version": projection.WATCH_SELECTION_QUERY_VERSION,
        "source_audit_version": _SOURCE_AUDIT_VERSION,
        "source": "WATCH_SHARED",
        "selection_policy": projection.WATCH_SELECTION_POLICY,
        "attempt_id": attempt.get("attempt_id"),
        "attempt_fingerprint": attempt.get("attempt_fingerprint"),
        "symbol": attempt.get("symbol"),
        "decision_time_utc": decision,
        "max_capture_age_seconds": _MAX_CAPTURE_AGE_SECONDS,
        "archive_snapshot_high_water_id": archive_high_water,
        "database_snapshot_id": transaction["database_snapshot_id"],
    }
    selected = snapshot is not None
    code_manifest, code_digest = projection._watch_code_manifest(snapshot)
    durable = None
    if selected:
        durable = max(_utc(snapshot.get("available_at_utc")),
                      _utc(snapshot.get("created_at_utc")))
    receipt = {
        "version": projection.WATCH_SELECTION_ATTESTATION_VERSION,
        "source_audit_version": _SOURCE_AUDIT_VERSION,
        "query_scope": query_scope,
        "query_binding_sha256": contract.digest(query_scope),
        "transaction_snapshot": "CALLER_TRANSACTION_SNAPSHOT",
        "population_complete": True,
        "selection_policy": projection.WATCH_SELECTION_POLICY,
        "attempt_id": attempt.get("attempt_id"),
        "attempt_fingerprint": attempt.get("attempt_fingerprint"),
        "symbol": attempt.get("symbol"),
        "decision_time_utc": decision,
        "max_capture_age_seconds": _MAX_CAPTURE_AGE_SECONDS,
        "read_started_at_utc": transaction["observed_at_utc"],
        "selection_status": "SELECTED" if selected else "NO_MATCH",
        "selected_snapshot_set_id": snapshot.get("snapshot_set_id") if selected else None,
        "selected_snapshot_key": snapshot.get("snapshot_key") if selected else None,
        "selected_payload_sha256": snapshot.get("payload_sha256") if selected else None,
        "selected_durably_available_at_utc": _iso(durable) if selected else None,
        "watch_code_manifest_sha256": code_digest if selected else None,
    }
    receipt["attestation_sha256"] = contract.digest(receipt)
    projection.validate_watch_selection_attestation(
        receipt, attempt=attempt, selected_snapshot=snapshot,
    )
    return {
        "attestation": receipt,
        "attestation_sha256": receipt["attestation_sha256"],
        "query_binding_sha256": receipt["query_binding_sha256"],
        "watch_code_manifest": code_manifest,
        "watch_code_manifest_sha256": code_digest,
    }


def _watch_selection_observation(*, attempt: Mapping[str, Any],
                                 snapshot: Mapping[str, Any] | None,
                                 archive_high_water: int,
                                 transaction: Mapping[str, Any]) -> dict:
    selected = snapshot is not None
    durable = None
    metadata_hash = None
    if selected:
        try:
            durable = _iso(max(
                _utc(snapshot.get("available_at_utc")),
                _utc(snapshot.get("created_at_utc")),
            ))
            metadata_hash = contract.digest(_json_safe(snapshot.get("source_metadata")))
        except (TypeError, ValueError, KeyError, OverflowError) as exc:
            raise ProjectionAdapterError(
                "STAGE8_PROJECTION_WATCH_OBSERVATION_INVALID"
            ) from exc
    value = {
        "version": WATCH_OBSERVATION_VERSION,
        "selection_policy": projection.WATCH_SELECTION_POLICY,
        "attempt_id": attempt.get("attempt_id"),
        "attempt_fingerprint": attempt.get("attempt_fingerprint"),
        "decision_time_utc": (
            _iso(attempt.get("decision_time_utc"))
            if attempt.get("decision_time_utc") is not None else None
        ),
        "archive_snapshot_high_water_id": archive_high_water,
        "database_snapshot_id": transaction["database_snapshot_id"],
        "selection_status": "SELECTED" if selected else "NO_MATCH",
        "selected_snapshot_set_id": snapshot.get("snapshot_set_id") if selected else None,
        "selected_snapshot_key": snapshot.get("snapshot_key") if selected else None,
        "selected_payload_sha256": snapshot.get("payload_sha256") if selected else None,
        "selected_durably_available_at_utc": durable,
        "selected_source_metadata_sha256": metadata_hash,
    }
    value["watch_selection_observation_sha256"] = contract.digest(value)
    return value


def _fact_authority(fact: Mapping[str, Any], *,
                    expected_selection_hash: str | None,
                    expected_code_hash: str | None,
                    watch_selection_observation_hash: str,
                    expected_parent_hash: str | None,
                    expected_noneligibility_hash: str | None) -> dict:
    if not isinstance(fact, Mapping):
        raise ProjectionAdapterError("STAGE8_PROJECTION_EMITTED_INVALID_FACT")
    detached = deepcopy(dict(fact))
    supplied_hash = detached.pop("fact_sha256", None)
    try:
        expected_hash = contract.digest(detached)
        if supplied_hash != expected_hash:
            raise ValueError("fact self hash mismatch")
        validation = {"expected_fact_sha256": expected_hash}
        if (fact.get("applicability_status") == projection.APPLICABLE
                and fact.get("knowledge_status") == projection.KNOWN):
            if not (_valid_hash(expected_selection_hash) and _valid_hash(expected_code_hash)):
                raise ValueError("known fact has no independent source authority")
            validation.update(
                expected_watch_selection_attestation_sha256=expected_selection_hash,
                expected_watch_code_manifest_sha256=expected_code_hash,
            )
        projection.validate_fact(fact, **validation)
        binding = _mapping(fact.get("binding"))
        contract.validate_exact_binding(binding)
        identity = _mapping(fact.get("identity"))
        source = _mapping(fact.get("source"))
        authority = {
            "exact_binding_sha256": binding.get("binding_sha256"),
            "expected_fact_sha256": expected_hash,
            "projection_version": projection.VERSION,
            "source_audit_version": _SOURCE_AUDIT_VERSION,
            "attempt_id": identity.get("attempt_id"),
            "attempt_fingerprint": identity.get("attempt_fingerprint"),
            "anchor_slot_id": identity.get("anchor_slot_id"),
            "event_id": identity.get("event_id"),
            "event_fingerprint": identity.get("event_fingerprint"),
            "symbol": identity.get("symbol"),
            "direction": identity.get("direction"),
            "decision_time_utc": identity.get("decision_time_utc"),
            "knowledge_status": fact.get("knowledge_status"),
            "candidate_match": fact.get("candidate_match"),
            "watch_selection_attestation_sha256": expected_selection_hash,
            "watch_code_manifest_sha256": expected_code_hash,
            "expected_watch_selection_attestation_sha256": expected_selection_hash,
            "expected_watch_code_manifest_sha256": expected_code_hash,
            "watch_selection_observation_sha256":
                watch_selection_observation_hash,
            "expected_parent_membership_evidence_sha256": expected_parent_hash,
            "expected_noneligibility_proof_sha256": expected_noneligibility_hash,
            "observed_fact_selection_attestation_sha256":
                source.get("watch_selection_attestation_sha256"),
            "observed_fact_code_manifest_sha256":
                source.get("watch_code_manifest_sha256"),
        }
        if (not _valid_hash(authority["exact_binding_sha256"])
                or not _valid_hash(authority["expected_fact_sha256"])
                or not _valid_hash(watch_selection_observation_hash)
                or (_valid_hash(expected_parent_hash)
                    == _valid_hash(expected_noneligibility_hash))):
            raise ValueError("fact authority hash missing")
        authority["fact_authority_sha256"] = contract.digest(authority)
        return authority
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise ProjectionAdapterError("STAGE8_PROJECTION_EMITTED_INVALID_FACT") from exc


def _project_attempts_from_connection(
    conn: Any, *, attempt_ids: Sequence[int], max_attempts: int,
    projection_mode: str, exact_binding: Mapping[str, Any] | None,
    max_wall_seconds: float = MAX_WALL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Project a complete, exact, bounded attempt-ID cohort in one DB snapshot.

    The result's expected hashes are computed by this producer from raw rows;
    no caller-supplied fact, receipt, selection hash or code hash is accepted.
    Result hashes are deterministic integrity bindings, not signatures.  A
    durable registry must compare them with independently stored expectations.
    """
    _assert_runtime_contract()
    if projection_mode == EXACT_BINDING_PROJECTION_MODE:
        try:
            contract.validate_exact_binding(exact_binding)
        except (TypeError, ValueError, KeyError) as exc:
            raise ProjectionAdapterError(
                "STAGE8_PROJECTION_EXACT_BINDING_INVALID"
            ) from exc
    elif (projection_mode != FIRST_TRANCHE_PROJECTION_MODE
          or exact_binding is not None):
        raise ProjectionAdapterError("STAGE8_PROJECTION_MODE_INVALID")
    if max_attempts not in (MAX_ATTEMPTS, MAX_EXACT_BINDING_ATTEMPTS):
        raise ProjectionAdapterError("STAGE8_PROJECTION_MODE_INVALID")
    if (isinstance(attempt_ids, (str, bytes)) or not isinstance(attempt_ids, Sequence)):
        raise ProjectionAdapterError("STAGE8_PROJECTION_ATTEMPT_IDS_INVALID")
    requested = list(attempt_ids)
    if (not requested or len(requested) > max_attempts
            or any(not _positive_id(value) for value in requested)
            or requested != sorted(set(requested))):
        raise ProjectionAdapterError("STAGE8_PROJECTION_ATTEMPT_IDS_INVALID")
    if (isinstance(max_wall_seconds, bool)
            or not isinstance(max_wall_seconds, (int, float))
            or not math.isfinite(float(max_wall_seconds))
            or not 0 < float(max_wall_seconds) <= MAX_WALL_SECONDS):
        raise ProjectionAdapterError("STAGE8_PROJECTION_WALL_BOUND_INVALID")
    try:
        started = float(monotonic())
        if not math.isfinite(started):
            raise ValueError("invalid monotonic clock")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProjectionAdapterError("STAGE8_PROJECTION_MONOTONIC_CLOCK_INVALID") from exc
    deadline = started + float(max_wall_seconds)
    query_count = 0

    def check_deadline() -> float:
        try:
            current = float(monotonic())
        except (TypeError, ValueError, OverflowError) as exc:
            raise ProjectionAdapterError(
                "STAGE8_PROJECTION_MONOTONIC_CLOCK_INVALID"
            ) from exc
        if not math.isfinite(current) or current < started or current >= deadline:
            raise ProjectionAdapterError("STAGE8_PROJECTION_WALL_BOUND_EXCEEDED")
        return current

    def read(sql: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        nonlocal query_count
        query_count += 1
        if query_count > MAX_QUERIES:
            raise ProjectionAdapterError("STAGE8_PROJECTION_QUERY_BOUND_EXCEEDED")
        try:
            before = check_deadline()
            values = conn.execute(sql, params or {}).fetchall()
            after = check_deadline()
            if after < before:
                raise ProjectionAdapterError(
                    "STAGE8_PROJECTION_MONOTONIC_CLOCK_INVALID"
                )
            if any(not isinstance(row, Mapping) for row in values):
                raise TypeError("mapping rows required")
            return [dict(row) for row in values]
        except ProjectionAdapterError:
            raise
        except Exception as exc:
            # Do not embed driver messages: they may contain DSNs or values.
            raise ProjectionAdapterError("STAGE8_PROJECTION_DATABASE_READ_FAILED") from exc

    source_manifest_before, source_manifest_hash = _projection_source_manifest()
    check_deadline()
    tx_rows = read(_TRANSACTION_SQL)
    if len(tx_rows) != 1:
        raise ProjectionAdapterError("STAGE8_PROJECTION_TRANSACTION_IDENTITY_INVALID")
    transaction = _transaction_identity(tx_rows[0], conn)

    # The archive high-water is acquired before any archive payload/value row
    # is inspected.  Selection below is explicitly bounded by this ID.
    high_water_rows = read(_ARCHIVE_HIGH_WATER_SQL)
    if (len(high_water_rows) != 1
            or type(high_water_rows[0].get("archive_snapshot_high_water_id")) is not int
            or not 0 <= high_water_rows[0]["archive_snapshot_high_water_id"] <= _INT64_MAX):
        raise ProjectionAdapterError("STAGE8_PROJECTION_ARCHIVE_HIGH_WATER_INVALID")
    archive_high_water = high_water_rows[0]["archive_snapshot_high_water_id"]

    attempt_rows = read(_ATTEMPTS_SQL, {
        "attempt_ids": requested, "limit": max_attempts + 1,
    })
    found_ids = [row.get("attempt_id") for row in attempt_rows]
    if (len(attempt_rows) > max_attempts or any(not _positive_id(value) for value in found_ids)
            or found_ids != sorted(set(found_ids))
            or any(value not in requested for value in found_ids)):
        raise ProjectionAdapterError("STAGE8_PROJECTION_ATTEMPT_QUERY_INVALID")
    attempts = {row["attempt_id"]: row for row in attempt_rows}

    slot_rows = read(_SLOTS_SQL, {
        "attempt_ids": requested, "limit": max_attempts + 1,
    })
    slots: dict[int, dict] = {}
    for row in slot_rows:
        attempt_id = row.pop("adapter_attempt_id", None)
        if (not _positive_id(attempt_id) or attempt_id not in attempts
                or attempt_id in slots):
            raise ProjectionAdapterError("STAGE8_PROJECTION_SLOT_QUERY_INVALID")
        slots[attempt_id] = row
    if len(slot_rows) > max_attempts:
        raise ProjectionAdapterError("STAGE8_PROJECTION_SLOT_QUERY_INVALID")

    event_ids: list[int] = []
    for slot in slots.values():
        for key in ("long_event_id", "short_event_id"):
            value = slot.get(key)
            if _positive_id(value):
                event_ids.append(value)
    if len(event_ids) != len(set(event_ids)) or len(event_ids) > 2 * max_attempts:
        raise ProjectionAdapterError("STAGE8_PROJECTION_EVENT_IDS_INVALID")
    event_rows = read(_EVENTS_SQL, {
        "event_ids": sorted(event_ids), "limit": 2 * max_attempts + 1,
    }) if event_ids else []
    returned_event_ids = [row.get("event_id") for row in event_rows]
    if (len(event_rows) > 2 * max_attempts
            or any(not _positive_id(value) for value in returned_event_ids)
            or returned_event_ids != sorted(set(returned_event_ids))
            or any(value not in event_ids for value in returned_event_ids)):
        raise ProjectionAdapterError("STAGE8_PROJECTION_EVENT_QUERY_INVALID")
    events = {row["event_id"]: row for row in event_rows}

    parent_rows = read(_PARENT_SOURCES_SQL, {
        "event_ids": sorted(event_ids),
        "parent_policy_version": btc_parent.POLICY_VERSION,
        "limit": 2 * max_attempts + 1,
    }) if event_ids else []
    parent_sources: dict[int, dict] = {}
    for row in parent_rows:
        event_id = row.get("adapter_event_id")
        if (not _positive_id(event_id) or event_id not in event_ids
                or event_id in parent_sources
                or set(row) != {
                    "adapter_event_id", "adapter_membership_json",
                    "adapter_parent_json", "adapter_btc_bar_json",
                }):
            raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_QUERY_INVALID")
        parent_sources[event_id] = _exact_parent_source(
            event=events.get(event_id),
            membership=_json_mapping(row.get("adapter_membership_json")),
            parent=_json_mapping(row.get("adapter_parent_json")),
            btc_bar=_json_mapping(row.get("adapter_btc_bar_json")),
        )
    if (len(parent_rows) > 2 * max_attempts
            or set(parent_sources) != set(events)):
        raise ProjectionAdapterError("STAGE8_PROJECTION_PARENT_QUERY_INVALID")

    snapshot_rows = read(_WATCH_SELECTION_SQL, {
        "attempt_ids": requested,
        "archive_snapshot_high_water_id": archive_high_water,
        "max_capture_age": timedelta(seconds=_MAX_CAPTURE_AGE_SECONDS),
        "limit": max_attempts + 1,
    })
    snapshots: dict[int, Mapping[str, Any] | None] = {}
    for row in snapshot_rows:
        attempt_id = row.pop("adapter_attempt_id", None)
        has_snapshot = row.pop("adapter_has_snapshot", None)
        if (not _positive_id(attempt_id) or attempt_id not in attempts
                or attempt_id in snapshots or type(has_snapshot) is not bool):
            raise ProjectionAdapterError("STAGE8_PROJECTION_WATCH_QUERY_INVALID")
        snapshots[attempt_id] = row if has_snapshot else None
    if len(snapshot_rows) > max_attempts or set(snapshots) != set(attempts):
        raise ProjectionAdapterError("STAGE8_PROJECTION_WATCH_QUERY_INVALID")

    # Re-read transaction identity after all source reads.  A changed snapshot,
    # backend or transaction start makes every would-be fact unauthoritative.
    final_tx_rows = read(_TRANSACTION_SQL)
    if len(final_tx_rows) != 1:
        raise ProjectionAdapterError("STAGE8_PROJECTION_TRANSACTION_IDENTITY_INVALID")
    final_transaction = _transaction_identity(final_tx_rows[0], conn)
    identity_keys = (
        "backend_pid", "transaction_started_at_utc", "database_snapshot_id",
        "read_only", "isolation", "statement_timeout_ms",
        "transaction_identity_sha256",
    )
    if any(transaction[key] != final_transaction[key] for key in identity_keys):
        raise ProjectionAdapterError("STAGE8_PROJECTION_TRANSACTION_CHANGED_DURING_READ")

    result_rows = []
    population_entries = []
    for attempt_id in requested:
        check_deadline()
        attempt = attempts.get(attempt_id)
        identity = _attempt_identity(attempt_id, attempt)
        identity_hash = contract.digest(identity)
        population_entries.append({
            "attempt_id": attempt_id,
            "row_status": identity["row_status"],
            "attempt_identity_sha256": identity_hash,
        })
        if attempt is None:
            result_rows.append({
                "attempt_id": attempt_id,
                "source_status": "MISSING",
                "knowledge_status": "UNKNOWN",
                "reasons": ["ATTEMPT_ROW_MISSING"],
                "attempt_identity": identity,
                "attempt_identity_sha256": identity_hash,
                "watch_selection_attestation": None,
                "expected_watch_selection_attestation_sha256": None,
                "watch_code_manifest": None,
                "expected_watch_code_manifest_sha256": None,
                "watch_selection_observation": None,
                "projection": None,
                "fact_authorities": [],
                "fact_ledger": [],
            })
            continue

        slot = slots.get(attempt_id)
        anchor_events = []
        if slot is not None:
            for key in ("long_event_id", "short_event_id"):
                event = events.get(slot.get(key))
                if event is not None:
                    anchor_events.append(event)
        snapshot = snapshots.get(attempt_id)
        watch_observation = _watch_selection_observation(
            attempt=attempt, snapshot=snapshot,
            archive_high_water=archive_high_water,
            transaction=transaction,
        )
        selection = None
        selection_reason = None
        try:
            selection = _selection_attestation(
                attempt=attempt, snapshot=snapshot,
                archive_high_water=archive_high_water,
                transaction=transaction,
            )
        except (TypeError, ValueError, KeyError, OverflowError):
            selection_reason = "WATCH_SELECTION_ATTESTATION_INVALID"

        attestation = selection["attestation"] if selection is not None else {}
        if projection_mode == EXACT_BINDING_PROJECTION_MODE:
            fact = projection.project_binding_fact(
                attempt=attempt, anchor_slot=slot, anchor_events=anchor_events,
                selected_watch_snapshot=snapshot,
                watch_selection_attestation=attestation,
                binding=exact_binding,
            )
            tranche = {
                "version": projection.VERSION,
                "manifest_sha256": _MANIFEST_SHA256,
                "projection_mode": projection_mode,
                "exact_binding_sha256": exact_binding["binding_sha256"],
                "symbol": attempt.get("symbol"),
                "attempt_id": attempt_id,
                "attempt_fingerprint": attempt.get("attempt_fingerprint"),
                "scope_resolution_status": fact.get("knowledge_status"),
                "reasons": deepcopy(fact.get("reasons", [])),
                "fact_count": 1,
                "facts": [fact],
            }
            tranche["facts_sha256"] = contract.digest(tranche)
        else:
            tranche = projection.project_first_tranche(
                attempt=attempt, anchor_slot=slot, anchor_events=anchor_events,
                selected_watch_snapshot=snapshot,
                watch_selection_attestation=attestation,
            )
        expected_selection_hash = (
            selection["attestation_sha256"] if selection is not None else None
        )
        expected_code_hash = (
            selection["watch_code_manifest_sha256"] if selection is not None else None
        )
        proof = _noneligibility_proof(attempt)
        fact_ledger = []
        authorities = []
        for fact in tranche.get("facts", []):
            identity_for_fact = _mapping(fact.get("identity"))
            parent_source = None
            parent_evidence = None
            expected_parent_hash = None
            expected_noneligibility_hash = None
            if proof is not None:
                expected_noneligibility_hash = proof["proof_sha256"]
            else:
                event_id = identity_for_fact.get("event_id")
                parent_source = parent_sources.get(event_id)
                if parent_source is None:
                    parent_source = _exact_parent_source(
                        event=events.get(event_id), membership=None,
                        parent=None, btc_bar=None,
                    )
                try:
                    parent_evidence, _ = (
                        representative_selector.canonical_parent_membership_evidence(
                            parent_source, fact=fact,
                            direction=str(identity_for_fact.get("direction") or ""),
                        )
                    )
                    expected_parent_hash = contract.digest(parent_evidence)
                except (TypeError, ValueError, KeyError, OverflowError) as exc:
                    raise ProjectionAdapterError(
                        "STAGE8_PROJECTION_PARENT_EVIDENCE_INVALID"
                    ) from exc
            authority = _fact_authority(
                fact, expected_selection_hash=expected_selection_hash,
                expected_code_hash=expected_code_hash,
                watch_selection_observation_hash=
                    watch_observation["watch_selection_observation_sha256"],
                expected_parent_hash=expected_parent_hash,
                expected_noneligibility_hash=expected_noneligibility_hash,
            )
            authorities.append(authority)
            fact_ledger.append({
                "attempt_id": attempt_id,
                "exact_binding_sha256": authority["exact_binding_sha256"],
                "fact": deepcopy(dict(fact)),
                "fact_authority": deepcopy(authority),
                "watch_selection_attestation": (
                    deepcopy(selection["attestation"])
                    if selection is not None else None
                ),
                "expected_watch_selection_attestation_sha256":
                    expected_selection_hash,
                "watch_code_manifest": (
                    deepcopy(selection["watch_code_manifest"])
                    if selection is not None else None
                ),
                "expected_watch_code_manifest_sha256": expected_code_hash,
                "watch_selection_observation": deepcopy(watch_observation),
                "parent_membership_source": deepcopy(parent_source),
                "parent_membership_evidence": deepcopy(parent_evidence),
                "noneligibility_proof": deepcopy(proof),
            })
        if tranche.get("fact_count") != len(authorities):
            raise ProjectionAdapterError("STAGE8_PROJECTION_TRANCHE_CARDINALITY_INVALID")
        reasons = []
        if selection_reason:
            reasons.append(selection_reason)
        if selection is not None and selection["attestation"]["selection_status"] == "NO_MATCH":
            reasons.append("WATCH_CAPTURE_NO_MATCH")
        if slot is None:
            reasons.append("ANCHOR_SLOT_MISSING")
        if len(anchor_events) != 2:
            reasons.append("ANCHOR_EXACT_EVENT_PAIR_MISSING")
        if tranche.get("scope_resolution_status") != projection.KNOWN:
            reasons.append("PROJECTION_SCOPE_UNKNOWN")
        unknown_count = sum(
            authority["knowledge_status"] != projection.KNOWN
            for authority in authorities
        )
        if unknown_count:
            reasons.append("PROJECTED_FACTS_INCLUDE_UNKNOWN")
        result_rows.append({
            "attempt_id": attempt_id,
            "source_status": "FOUND",
            "knowledge_status": "KNOWN" if not reasons else "UNKNOWN",
            "reasons": sorted(set(reasons)),
            "attempt_identity": identity,
            "attempt_identity_sha256": identity_hash,
            "watch_selection_attestation": (
                deepcopy(selection["attestation"]) if selection is not None else None
            ),
            "expected_watch_selection_attestation_sha256": expected_selection_hash,
            "watch_code_manifest": (
                deepcopy(selection["watch_code_manifest"]) if selection is not None else None
            ),
            "expected_watch_code_manifest_sha256": expected_code_hash,
            "watch_selection_observation": deepcopy(watch_observation),
            "projection": tranche,
            "fact_authorities": authorities,
            "fact_ledger": fact_ledger,
        })
        check_deadline()

    source_manifest_after, source_manifest_hash_after = _projection_source_manifest()
    check_deadline()
    if (source_manifest_before != source_manifest_after
            or source_manifest_hash != source_manifest_hash_after):
        raise ProjectionAdapterError("STAGE8_PROJECTION_SOURCE_CHANGED_DURING_READ")

    found = sorted(attempts)
    missing = [attempt_id for attempt_id in requested if attempt_id not in attempts]
    query_scope = {
        "version": VERSION,
        "manifest_sha256": _MANIFEST_SHA256,
        "projection_version": _PROJECTION_VERSION,
        "source_audit_version": _SOURCE_AUDIT_VERSION,
        "projection_mode": projection_mode,
        "exact_binding_sha256": (
            exact_binding["binding_sha256"]
            if exact_binding is not None else None
        ),
        "population_kind": (
            "EXACT_BINDING_FULL_COHORT_ATTEMPT_IDS"
            if projection_mode == EXACT_BINDING_PROJECTION_MODE
            else "EXACT_CALLER_SUPPLIED_ATTEMPT_IDS_NOT_COMPLETE_STAGE8_CORPUS"
        ),
        "requested_attempt_ids": requested,
        "max_attempts": max_attempts,
        "max_queries": MAX_QUERIES,
        "max_wall_seconds": float(max_wall_seconds),
        "max_statement_timeout_ms": MAX_STATEMENT_TIMEOUT_MS,
        "max_capture_age_seconds": _MAX_CAPTURE_AGE_SECONDS,
        "capture_selection_policy": projection.WATCH_SELECTION_POLICY,
    }
    query_binding_sha256 = contract.digest(query_scope)
    authority_rows = [{
        "attempt_id": row["attempt_id"],
        "attempt_identity_sha256": row["attempt_identity_sha256"],
        "expected_watch_selection_attestation_sha256":
            row["expected_watch_selection_attestation_sha256"],
        "expected_watch_code_manifest_sha256":
            row["expected_watch_code_manifest_sha256"],
        "fact_authorities": row["fact_authorities"],
    } for row in result_rows]
    population_receipt = {
        "version": POPULATION_RECEIPT_VERSION,
        "projection_mode": projection_mode,
        "exact_binding_sha256": (
            exact_binding["binding_sha256"]
            if exact_binding is not None else None
        ),
        "query_binding_sha256": query_binding_sha256,
        "requested_attempt_ids": requested,
        "found_attempt_ids": found,
        "missing_attempt_ids": missing,
        "attempt_population_sha256": contract.digest(population_entries),
        "outcome_free_authority_ledger_sha256": contract.digest(authority_rows),
        "archive_snapshot_high_water_id": archive_high_water,
        "database_snapshot_id": transaction["database_snapshot_id"],
        "transaction_identity_sha256": transaction["transaction_identity_sha256"],
        "read_started_at_utc": transaction["observed_at_utc"],
        "read_finished_at_utc": final_transaction["observed_at_utc"],
        "read_only": True,
        "transaction_isolation": "repeatable read",
        "population_complete": True,
        "truncated": False,
        "query_count": query_count,
    }
    population_receipt["exact_attempt_population_receipt_sha256"] = contract.digest(
        population_receipt
    )
    population_receipt["population_receipt_sha256"] = contract.digest(population_receipt)
    authority_receipt = {
        "version": AUTHORITY_RECEIPT_VERSION,
        "manifest_sha256": _MANIFEST_SHA256,
        "projection_mode": projection_mode,
        "exact_binding_sha256": (
            exact_binding["binding_sha256"]
            if exact_binding is not None else None
        ),
        "projection_source_manifest_sha256": source_manifest_hash,
        "projection_module_sha256":
            source_manifest_before["research_stage8_feature_projection.py"],
        "db_adapter_module_sha256":
            source_manifest_before["research_stage8_projection_db_adapter.py"],
        "parent_evidence_module_sha256":
            source_manifest_before["research_stage8_representative_selector.py"],
        "population_receipt_sha256": population_receipt["population_receipt_sha256"],
        "exact_attempt_population_receipt_sha256":
            population_receipt["exact_attempt_population_receipt_sha256"],
        "authority_rows_sha256": contract.digest(authority_rows),
    }
    authority_receipt["authority_receipt_sha256"] = contract.digest(authority_receipt)
    result = {
        "version": VERSION,
        "manifest_sha256": _MANIFEST_SHA256,
        "projection_version": _PROJECTION_VERSION,
        "source_audit_version": _SOURCE_AUDIT_VERSION,
        "projection_mode": projection_mode,
        "exact_binding_sha256": (
            exact_binding["binding_sha256"]
            if exact_binding is not None else None
        ),
        "query_scope": query_scope,
        "query_binding_sha256": query_binding_sha256,
        "projection_source_manifest": source_manifest_before,
        "projection_source_manifest_sha256": source_manifest_hash,
        "projection_module_sha256":
            source_manifest_before["research_stage8_feature_projection.py"],
        "db_adapter_module_sha256":
            source_manifest_before["research_stage8_projection_db_adapter.py"],
        "parent_evidence_module_sha256":
            source_manifest_before["research_stage8_representative_selector.py"],
        "transaction": transaction,
        "archive_snapshot_high_water_id": archive_high_water,
        "population_receipt": population_receipt,
        "exact_attempt_population_receipt_sha256":
            population_receipt["exact_attempt_population_receipt_sha256"],
        "authority_receipt": authority_receipt,
        "rows": result_rows,
        "interpretation": (
            "Projection authority for the exact requested attempt IDs only; "
            "not a corpus-completeness, representative, label, qualification, "
            "delivery or deployment claim."
        ),
    }
    check_deadline()
    result["result_sha256"] = contract.digest(result)
    check_deadline()
    return result


def project_attempts_from_connection(
    conn: Any, *, attempt_ids: Sequence[int],
    max_wall_seconds: float = MAX_WALL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Project every first-tranche binding for at most 32 exact attempt IDs."""
    return _project_attempts_from_connection(
        conn, attempt_ids=attempt_ids, max_attempts=MAX_ATTEMPTS,
        projection_mode=FIRST_TRANCHE_PROJECTION_MODE, exact_binding=None,
        max_wall_seconds=max_wall_seconds, monotonic=monotonic,
    )


def project_exact_binding_attempts_from_connection(
    conn: Any, *, exact_binding: Mapping[str, Any],
    attempt_ids: Sequence[int], max_wall_seconds: float = MAX_WALL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Project one exact binding for each found ID in a full bounded cohort.

    Up to 1,000 sorted unique IDs are read with the same eight-SELECT source
    graph as the small first-tranche API. Missing source IDs remain explicit
    UNKNOWN rows and never receive a fabricated projected fact.
    """
    return _project_attempts_from_connection(
        conn, attempt_ids=attempt_ids,
        max_attempts=MAX_EXACT_BINDING_ATTEMPTS,
        projection_mode=EXACT_BINDING_PROJECTION_MODE,
        exact_binding=exact_binding, max_wall_seconds=max_wall_seconds,
        monotonic=monotonic,
    )
