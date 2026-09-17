"""Outcome-blind Stage-8 representative selection.

This module consumes one complete, externally hash-bound attempt population
for one exact frozen candidate binding.  It selects at most one candidate
observation per causal BTC parent, pooling symbols before it orders matches.

The selector is deliberately pure: it opens no database, reads no files,
inspects no label/outcome fields, and cannot assert database verification or
research qualification.  Hashes supplied by a registry/reader are authority
inputs, not signatures.  A caller must persist and independently verify the
returned batch before it can become acceptance evidence.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import math
import re
from typing import Any, Mapping, Sequence

import research_btc_parent_movement as btc_parent
import research_operational_score_source_audit as source_audit
import research_stage8_contract as contract
import research_stage8_coverage_receipt as coverage_receipt
import research_stage8_feature_projection as projection


VERSION = "stage8-outcome-blind-representative-selector-v1"
POPULATION_VERSION = "stage8-bounded-attempt-id-population-v1"
BATCH_VERSION = "stage8-outcome-blind-representative-batch-v1"
MEMBERSHIP_EVIDENCE_VERSION = "stage8-canonical-parent-membership-evidence-v1"
NONELIGIBILITY_VERSION = "stage8-attempt-noneligibility-proof-v1"
SELECTION_FACT_IDENTITY_VERSION = "stage8-selection-fact-identity-v1"
REGISTRY_STATUS = "VERIFIED_READ_ONLY_REPEATABLE_READ"
STRUCTURAL_AUTHORITY = (
    "PREDICTED_SELECTION_FACT_IDENTITIES_REQUIRE_DURABLE_DB_MATCH"
)

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_INT64_MAX = 9223372036854775807
_SOURCE_ROW_KEYS = frozenset({
    "attempt_id", "fact", "parent_membership_source", "noneligibility_proof",
})
_FACT_AUTHORITY_KEYS = frozenset({
    "attempt_id", "expected_fact_sha256",
    "expected_watch_selection_attestation_sha256",
    "expected_parent_membership_evidence_sha256",
    "expected_noneligibility_proof_sha256",
})
_PARENT_SOURCE_KEYS = frozenset({"event", "membership", "parent", "btc_bar"})
_EVENT_KEYS = frozenset({
    "event_id", "event_fingerprint", "alert_time_utc", "symbol", "direction",
})
_MEMBERSHIP_KEYS = frozenset({
    "event_id", "episode_policy_version", "btc_parent_movement_id",
    "decision_time_utc", "btc_observed_close_utc", "membership_status",
})
_PARENT_KEYS = frozenset({
    "btc_parent_movement_id", "episode_policy_version", "start_time_utc",
    "end_time_utc", "confirmed_at_utc", "direction", "evidence_eligible",
    "boundary_reason", "observed_through_utc", "price_source", "state_json",
})
_BAR_KEYS = frozenset({
    "open_time_utc", "close_time_utc", "open", "high", "low", "close",
    "price_source",
})
_NONELIGIBILITY_KEYS = frozenset({
    "version", "attempt_id", "attempt_fingerprint", "sampler_version", "symbol",
    "evaluation_status", "decision_time_utc", "reason", "proof_sha256",
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
_REGISTRY_KEYS = frozenset({
    "status", "exact_binding_sha256", "manifest_sha256", "freeze_id",
    "frozen_at_utc", "registry_record_sha256",
    "registry_verification_receipt_sha256", "verifier_profile_sha256",
    "expected_projection_source_sha256", "expected_selector_source_sha256",
    "expected_watch_code_manifest_sha256",
})
_OBSERVED_SOURCE_HASH_KEYS = frozenset({
    "projection_source_sha256", "selector_source_sha256",
})
_ALLOWED_NONELIGIBILITY = {
    "UNEVALUABLE": "ATTEMPT_UNEVALUABLE_NO_DECISION",
    "COVERAGE_EXCLUDED": "ATTEMPT_COVERAGE_EXCLUDED_NO_DECISION",
}
_STARTUP_MANIFEST = contract.frozen_manifest()
contract.validate_manifest(_STARTUP_MANIFEST)
_MANIFEST_SHA256 = contract.digest(_STARTUP_MANIFEST)
_COVERAGE_VERSION = coverage_receipt.VERSION
_OUTCOME_FREE_RECEIPT_VERSION = (
    coverage_receipt.OUTCOME_FREE_POPULATION_RECEIPT_VERSION
)
_PROJECTION_VERSION = _STARTUP_MANIFEST["projection"]["version"]
_SOURCE_AUDIT_VERSION = _STARTUP_MANIFEST["source"]["audit_version"]
_VALIDATE_FACT = projection.validate_fact
_VALIDATE_PARENT_MEMBERSHIP = source_audit.validate_parent_membership
_OUTCOME_FREE_PAYLOAD = coverage_receipt.outcome_free_population_payload


def _assert_runtime_contract() -> None:
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    if (contract.digest(manifest) != _MANIFEST_SHA256
            or contract.MANIFEST_SHA256 != _MANIFEST_SHA256
            or coverage_receipt.VERSION != _COVERAGE_VERSION
            or coverage_receipt.OUTCOME_FREE_POPULATION_RECEIPT_VERSION
            != _OUTCOME_FREE_RECEIPT_VERSION
            or projection.VERSION != _PROJECTION_VERSION
            or source_audit.VERSION != _SOURCE_AUDIT_VERSION
            or projection.validate_fact is not _VALIDATE_FACT
            or source_audit.validate_parent_membership
            is not _VALIDATE_PARENT_MEMBERSHIP
            or coverage_receipt.outcome_free_population_payload
            is not _OUTCOME_FREE_PAYLOAD):
        raise ValueError("Stage-8 selector dependency differs from frozen runtime contract")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH.fullmatch(value) is not None


def _positive_int64(value: Any) -> bool:
    return type(value) is int and 0 < value <= _INT64_MAX


def _nonnegative_int64(value: Any) -> bool:
    return type(value) is int and 0 <= value <= _INT64_MAX


def _canonical_utc(value: Any) -> tuple[str, datetime]:
    if not isinstance(value, str):
        raise ValueError("timestamp must be canonical UTC text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("timestamp must be canonical UTC text") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must have an explicit UTC offset")
    utc = parsed.astimezone(timezone.utc)
    canonical = utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if value != canonical:
        raise ValueError("timestamp is not canonical UTC")
    return canonical, utc


def _source_utc(value: Any) -> tuple[str, datetime]:
    """Canonicalize a trusted raw-source timestamp for an evidence identity."""
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("source timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("source timestamp must have an explicit UTC offset")
    utc = parsed.astimezone(timezone.utc)
    return utc.isoformat(timespec="microseconds").replace("+00:00", "Z"), utc


def _finite(value: Any) -> float:
    if isinstance(value, bool) or type(value) not in (int, float):
        raise ValueError("source price must be a JSON number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("source price must be finite")
    return 0.0 if number == 0.0 else number


def _strict_keys(value: Any, expected: frozenset[str], reason: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(reason)
    return value


def _representative_binding(exact_binding: Mapping[str, Any]) -> dict[str, Any]:
    contract.validate_exact_binding(exact_binding)
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    inner = exact_binding["binding"]
    scope, candidate = inner["scope"], inner["candidate"]
    return {
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "manifest_sha256": inner["manifest_sha256"],
        "contract_version": inner["version"],
        "source_version": inner["source_version"],
        "projection_version": inner["projection_version"],
        "label_version": inner["label_version"],
        "independence_version": inner["independence_version"],
        "acceptance_version": inner["acceptance_version"],
        "scope_id": scope["scope_id"],
        "scope_symbols": list(scope["symbols"]),
        "scope_price_route": scope["price_route"],
        "candidate_id": candidate["candidate_id"],
        "candidate_model": candidate["model"],
        "direction": candidate["direction"],
        "window_minutes": inner["window_minutes"],
        "threshold_bps": inner["threshold_bps"],
        "parent_policy_version": manifest["independence"]["parent_policy_version"],
    }


def _validate_registry_reference(
    value: Mapping[str, Any], *, exact_binding: Mapping[str, Any],
    observed_source_hashes: Mapping[str, Any],
) -> tuple[dict[str, Any], datetime]:
    registry = _strict_keys(value, _REGISTRY_KEYS, "registry reference shape invalid")
    observed = _strict_keys(
        observed_source_hashes, _OBSERVED_SOURCE_HASH_KEYS,
        "observed source hash shape invalid",
    )
    for key in _REGISTRY_KEYS - {"status", "frozen_at_utc"}:
        if not _hash(registry.get(key)):
            raise ValueError("registry reference hash invalid: " + key)
    for key in _OBSERVED_SOURCE_HASH_KEYS:
        if not _hash(observed.get(key)):
            raise ValueError("observed source hash invalid: " + key)
    if (registry["status"] != REGISTRY_STATUS
            or registry["manifest_sha256"] != _MANIFEST_SHA256
            or registry["exact_binding_sha256"] != exact_binding["binding_sha256"]
            or observed["projection_source_sha256"]
            != registry["expected_projection_source_sha256"]
            or observed["selector_source_sha256"]
            != registry["expected_selector_source_sha256"]):
        raise ValueError("registry reference does not bind this exact implementation")
    frozen_at, frozen = _canonical_utc(registry["frozen_at_utc"])
    detached = deepcopy(dict(registry))
    detached["frozen_at_utc"] = frozen_at
    return detached, frozen


def _validate_population_receipt(
    value: Mapping[str, Any], *, exact_binding: Mapping[str, Any],
    attempt_ids: Sequence[int], expected_outcome_free_receipt_sha256: str,
) -> dict[str, Any]:
    if (not isinstance(value, Mapping)
            or not _hash(expected_outcome_free_receipt_sha256)):
        raise ValueError("population receipt or expected hash invalid")
    # Project only the outcome-free view.  In particular, do not copy, hash or
    # branch on outcome/reason counts in the wider audit receipt.
    safe_payload = _OUTCOME_FREE_PAYLOAD(value)
    supplied_outcome_free_hash = value.get("outcome_free_population_receipt_sha256")
    if (not _hash(supplied_outcome_free_hash)
            or supplied_outcome_free_hash
            != expected_outcome_free_receipt_sha256
            or contract.digest(safe_payload) != supplied_outcome_free_hash
            or safe_payload.get("version")
            != _OUTCOME_FREE_RECEIPT_VERSION):
        raise ValueError("outcome-free population receipt hash mismatch")
    query = _mapping(safe_payload.get("query_scope"))
    supplied_query_hash = query.get("query_sha256")
    unsigned_query = dict(query)
    unsigned_query.pop("query_sha256", None)
    manifest = contract.frozen_manifest()
    expected_axes = {
        "adapter_version": manifest["source"]["audit_version"],
        "sampler_version": manifest["source"]["sampler_version"],
        "symbols": sorted(exact_binding["binding"]["scope"]["symbols"]),
        "max_capture_age_seconds": float(manifest["source"]["max_capture_age_seconds"]),
        "windows": [manifest["labels"]["window_minutes"]],
        "thresholds_bps": manifest["labels"]["thresholds_bps"],
        "capture_version": manifest["source"]["watch_version"],
        "outcome_version": manifest["labels"]["method_version"],
        "parent_policy": manifest["independence"]["parent_policy_version"],
    }
    if (not _hash(supplied_query_hash)
            or contract.digest(unsigned_query) != supplied_query_hash
            or any(unsigned_query.get(key) != expected for key, expected in expected_axes.items())
            or type(unsigned_query.get("page_size")) is not int
            or not 1 <= unsigned_query["page_size"] <= coverage_receipt.MAX_PAGE_SIZE):
        raise ValueError("population query does not match the exact frozen binding")
    try:
        start = datetime.fromisoformat(str(unsigned_query["start_utc"]))
        end = datetime.fromisoformat(str(unsigned_query["end_utc"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("population query time range invalid") from exc
    if (start.tzinfo is None or end.tzinfo is None or start >= end
            or set(unsigned_query) != set(expected_axes) | {
                "start_utc", "end_utc", "page_size",
            }):
        raise ValueError("population query shape or time range invalid")

    ordered_ids = sorted(attempt_ids)
    high_water = safe_payload.get("high_water_attempt_id")
    transaction_hash = safe_payload.get("transaction_identity_sha256")
    population = {
        "version": POPULATION_VERSION,
        "manifest_sha256": _MANIFEST_SHA256,
        "query_sha256": supplied_query_hash,
        "transaction_identity_sha256": transaction_hash,
        "high_water_attempt_id": high_water,
        "attempt_ids": ordered_ids,
    }
    attempts = safe_payload.get("attempts_examined")
    status_counts = _mapping(safe_payload.get("attempt_status_counts"))
    valid_status_counts = (
        set(status_counts).issubset({
            "EVALUABLE", "UNEVALUABLE", "COVERAGE_EXCLUDED", "UNKNOWN",
        })
        and all(type(count) is int and count >= 0 for count in status_counts.values())
    )
    if (safe_payload.get("coverage_receipt_version") != _COVERAGE_VERSION
            or safe_payload.get("manifest_sha256") != _MANIFEST_SHA256
            or safe_payload.get("manifest_axes_verified") is not True
            or safe_payload.get("status") != "COMPLETE_BOUNDED_COHORT"
            or safe_payload.get("database_connection_attempted") is not True
            or safe_payload.get("stop_reason") is not None
            or safe_payload.get("attempt_population_version") != POPULATION_VERSION
            or not _hash(transaction_hash)
            or not _nonnegative_int64(high_water)
            or type(attempts) is not int or attempts != len(ordered_ids)
            or not valid_status_counts
            or sum(status_counts.values()) != attempts
            or safe_payload.get("attempt_population_sha256") != contract.digest(population)
            or len(set(ordered_ids)) != len(ordered_ids)
            or any(not _positive_int64(item) or item > high_water for item in ordered_ids)
            or (ordered_ids and ordered_ids[-1] != high_water)
            or (not ordered_ids and high_water != 0)):
        raise ValueError("population receipt is incomplete or does not cover all source rows")
    full_audit_hash = value.get("receipt_sha256")
    if not _hash(full_audit_hash):
        raise ValueError("full audit receipt reference hash invalid")
    return {
        **safe_payload,
        "query_scope": deepcopy(dict(query)),
        "outcome_free_population_receipt_sha256": supplied_outcome_free_hash,
        "full_audit_receipt_sha256": full_audit_hash,
    }


def canonical_parent_membership_evidence(
    value: Mapping[str, Any], *, fact: Mapping[str, Any], direction: str,
) -> tuple[dict[str, Any], str]:
    """Return the exact hashable parent evidence projection and its class.

    ``UNKNOWN`` is itself hashable so a database adapter can bind every source
    attempt without pretending that missing membership proves noneligibility.
    Only ``LIVE`` and the one canonical boundary-unverified case can proceed.
    """
    _assert_runtime_contract()
    source = _strict_keys(value, _PARENT_SOURCE_KEYS, "parent source shape invalid")
    identity = _mapping(fact.get("identity"))
    raw_parts = {
        "event": source.get("event"), "membership": source.get("membership"),
        "parent": source.get("parent"), "btc_bar": source.get("btc_bar"),
    }
    expected_keys = {
        "event": _EVENT_KEYS, "membership": _MEMBERSHIP_KEYS,
        "parent": _PARENT_KEYS, "btc_bar": _BAR_KEYS,
    }
    # Extra fields are rejected rather than ignored; especially, this leaves
    # no nested channel through which outcome/label material can affect a hash.
    for name, raw in raw_parts.items():
        if isinstance(raw, Mapping) and set(raw) != expected_keys[name]:
            raise ValueError(name + " source shape invalid")
    if any(not isinstance(raw, Mapping) for raw in raw_parts.values()):
        return {
            "version": MEMBERSHIP_EVIDENCE_VERSION,
            "validation_status": "UNKNOWN",
            "reason": "PARENT_SOURCE_MISSING",
            "source_presence": {
                name: isinstance(raw, Mapping) for name, raw in raw_parts.items()
            },
            "attempt_id": identity.get("attempt_id"),
            "attempt_fingerprint": identity.get("attempt_fingerprint"),
            "anchor_slot_id": identity.get("anchor_slot_id"),
            "event_id": identity.get("event_id"),
            "event_fingerprint": identity.get("event_fingerprint"),
            "symbol": identity.get("symbol"),
            "direction": identity.get("direction"),
            "decision_time_utc": identity.get("decision_time_utc"),
        }, "UNKNOWN"
    event = raw_parts["event"]
    membership = raw_parts["membership"]
    parent = raw_parts["parent"]
    bar = raw_parts["btc_bar"]
    if (event.get("event_id") != identity.get("event_id")
            or event.get("event_fingerprint") != identity.get("event_fingerprint")
            or event.get("symbol") != identity.get("symbol")
            or event.get("direction") != direction):
        raise ValueError("parent event does not match projected fact")
    event_time, decision = _source_utc(event.get("alert_time_utc"))
    if event_time != identity.get("decision_time_utc"):
        raise ValueError("parent event time does not match projected fact")
    validated = _VALIDATE_PARENT_MEMBERSHIP(event, membership, parent, bar)
    reasons = validated.get("reasons")
    if validated.get("status") == "VALID" and reasons == []:
        membership_class = "LIVE"
    elif (validated.get("status") == "UNKNOWN"
          and reasons == ["BTC_PARENT_BOUNDARY_UNVERIFIED"]
          and membership.get("membership_status") == "BOUNDARY_UNVERIFIED"
          and parent.get("evidence_eligible") is False):
        membership_class = "PROVEN_NOT_EVIDENCE_ELIGIBLE"
    else:
        return {
            "version": MEMBERSHIP_EVIDENCE_VERSION,
            "validation_status": "UNKNOWN",
            "reason": "PARENT_SOURCE_VALIDATION_UNKNOWN",
            "source_reason_codes": [
                str(reason).split(":", 1)[0]
                for reason in reasons if isinstance(reason, str)
            ] if isinstance(reasons, list) else ["PARENT_SOURCE_RESULT_MALFORMED"],
            "source_presence": {name: True for name in raw_parts},
            "attempt_id": identity.get("attempt_id"),
            "attempt_fingerprint": identity.get("attempt_fingerprint"),
            "anchor_slot_id": identity.get("anchor_slot_id"),
            "event_id": identity.get("event_id"),
            "event_fingerprint": identity.get("event_fingerprint"),
            "symbol": identity.get("symbol"),
            "direction": identity.get("direction"),
            "decision_time_utc": identity.get("decision_time_utc"),
        }, "UNKNOWN"
    membership_decision, membership_dt = _source_utc(membership.get("decision_time_utc"))
    parent_start, parent_start_dt = _source_utc(parent.get("start_time_utc"))
    observed_close, _ = _source_utc(membership.get("btc_observed_close_utc"))
    bar_open, _ = _source_utc(bar.get("open_time_utc"))
    bar_close, _ = _source_utc(bar.get("close_time_utc"))
    parent_end = None if parent.get("end_time_utc") is None else _source_utc(
        parent["end_time_utc"]
    )[0]
    confirmed = None if parent.get("confirmed_at_utc") is None else _source_utc(
        parent["confirmed_at_utc"]
    )[0]
    observed_through, _ = _source_utc(parent.get("observed_through_utc"))
    if decision != membership_dt or decision < parent_start_dt:
        raise ValueError("parent membership causal time mismatch")
    evidence = {
        "version": MEMBERSHIP_EVIDENCE_VERSION,
        "validation_status": "VALID" if membership_class == "LIVE" else membership_class,
        "event_id": event["event_id"],
        "event_fingerprint": event["event_fingerprint"],
        "symbol": event["symbol"],
        "direction": event["direction"],
        "decision_time_utc": event_time,
        "parent_policy_version": membership["episode_policy_version"],
        "membership_status": membership["membership_status"],
        "btc_parent_movement_id": membership["btc_parent_movement_id"],
        "btc_observed_close_utc": observed_close,
        "parent_start_time_utc": parent_start,
        "parent_end_time_utc": parent_end,
        "parent_confirmed_at_utc": confirmed,
        "parent_direction": parent["direction"],
        "parent_evidence_eligible": parent["evidence_eligible"],
        "parent_boundary_reason": parent["boundary_reason"],
        "parent_observed_through_utc": observed_through,
        "parent_price_source": parent["price_source"],
        "btc_bar": {
            "open_time_utc": bar_open, "close_time_utc": bar_close,
            "open": _finite(bar["open"]), "high": _finite(bar["high"]),
            "low": _finite(bar["low"]), "close": _finite(bar["close"]),
            "price_source": bar["price_source"],
        },
    }
    return evidence, membership_class


def _validate_noneligibility(
    value: Mapping[str, Any], *, attempt_id: int, fact: Mapping[str, Any],
) -> dict[str, Any]:
    proof = _strict_keys(value, _NONELIGIBILITY_KEYS, "noneligibility proof shape invalid")
    unsigned = dict(proof)
    supplied = unsigned.pop("proof_sha256", None)
    identity = _mapping(fact.get("identity"))
    status = proof.get("evaluation_status")
    if (proof.get("version") != NONELIGIBILITY_VERSION
            or status not in _ALLOWED_NONELIGIBILITY
            or proof.get("reason") != _ALLOWED_NONELIGIBILITY.get(status)
            or proof.get("attempt_id") != attempt_id
            or proof.get("attempt_id") != identity.get("attempt_id")
            or proof.get("attempt_fingerprint") != identity.get("attempt_fingerprint")
            or proof.get("sampler_version") != identity.get("sampler_version")
            or proof.get("symbol") != identity.get("symbol")
            or proof.get("decision_time_utc") is not None
            or identity.get("decision_time_utc") is not None
            or not _hash(proof.get("attempt_fingerprint"))
            or not _hash(supplied)
            or contract.digest(unsigned) != supplied):
        raise ValueError("noneligibility proof invalid")
    return deepcopy(dict(proof))


def canonical_selection_fact_identity(
    exact_binding: Mapping[str, Any], fact: Mapping[str, Any], *,
    source_attempt_evaluation_status: str,
    parent_authority_class: str,
    parent_membership_evidence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Project the exact pre-persistence identity PostgreSQL will derive.

    This value is deterministic prediction, not database authority.  The fact
    writer must independently derive the same payload from source-linked rows,
    seal it, and require its stored digest to equal the selector's expected
    digest before a selection receipt can be persisted.
    """
    contract.validate_exact_binding(exact_binding)
    if not isinstance(fact, Mapping):
        raise ValueError("selection fact identity requires a projected fact")
    projected_binding = _mapping(fact.get("binding"))
    identity = _mapping(fact.get("identity"))
    expected_direction = exact_binding["binding"]["candidate"]["direction"]
    expected_symbols = exact_binding["binding"]["scope"]["symbols"]
    attempt_id = identity.get("attempt_id")
    attempt_fingerprint = identity.get("attempt_fingerprint")
    sampler_version = identity.get("sampler_version")
    symbol = identity.get("symbol")
    direction = identity.get("direction")
    if (projected_binding.get("binding_sha256")
            != exact_binding["binding_sha256"]
            or not _positive_int64(attempt_id)
            or not _hash(attempt_fingerprint)
            or sampler_version != _STARTUP_MANIFEST["source"]["sampler_version"]
            or symbol not in expected_symbols
            or direction != expected_direction
            or fact.get("knowledge_status") not in {"KNOWN", "UNKNOWN"}
            or (fact.get("knowledge_status") == "KNOWN")
            != isinstance(fact.get("candidate_match"), bool)
            or (fact.get("knowledge_status") == "UNKNOWN"
                and fact.get("candidate_match") is not None)):
        raise ValueError("selection fact causal identity is invalid")
    source_open, _ = _canonical_utc(identity.get("source_candle_open_utc"))
    if source_attempt_evaluation_status not in {
        "EVALUABLE", "UNEVALUABLE", "COVERAGE_EXCLUDED",
    }:
        raise ValueError("selection fact source attempt status is invalid")

    raw_slot = (
        identity.get("anchor_slot_id"), identity.get("event_id"),
        identity.get("event_fingerprint"),
    )
    slot_complete = (
        _positive_int64(raw_slot[0]) and _positive_int64(raw_slot[1])
        and _hash(raw_slot[2])
    )
    slot_absent = raw_slot == (None, None, None)
    if source_attempt_evaluation_status == "EVALUABLE":
        if not (slot_complete or slot_absent):
            raise ValueError("selection fact source slot identity is partial")
        anchor_slot_id, event_id, event_fingerprint = raw_slot
        # The server uses source_slot fields.  For an EVALUABLE attempt whose
        # authoritative slot is absent, all slot-derived values are JSON null,
        # even though the source attempt itself still has a decision time.
        decision_time_utc = (
            _canonical_utc(identity.get("decision_time_utc"))[0]
            if slot_complete else None
        )
    else:
        if (not slot_absent or identity.get("decision_time_utc") is not None
                or parent_authority_class
                != "PROVEN_NOT_CANDIDATE_ELIGIBLE"):
            raise ValueError("non-evaluable selection fact identity is invalid")
        anchor_slot_id = event_id = event_fingerprint = decision_time_utc = None

    evidence = (
        parent_membership_evidence
        if isinstance(parent_membership_evidence, Mapping) else None
    )
    if parent_authority_class == "LIVE":
        expected_evidence_status = "VALID"
    elif parent_authority_class == "PROVEN_NOT_EVIDENCE_ELIGIBLE":
        expected_evidence_status = "PROVEN_NOT_EVIDENCE_ELIGIBLE"
    elif parent_authority_class == "UNKNOWN":
        expected_evidence_status = "UNKNOWN"
    elif parent_authority_class == "PROVEN_NOT_CANDIDATE_ELIGIBLE":
        expected_evidence_status = None
    else:
        raise ValueError("selection fact parent authority class is invalid")
    if ((expected_evidence_status is None) != (evidence is None)
            or (evidence is not None
                and evidence.get("validation_status")
                != expected_evidence_status)
            or (source_attempt_evaluation_status == "EVALUABLE")
            != (evidence is not None)):
        raise ValueError("selection fact parent authority is inconsistent")

    parent_id = parent_start = parent_policy = membership_status = None
    parent_eligible = None
    if parent_authority_class in {"LIVE", "PROVEN_NOT_EVIDENCE_ELIGIBLE"}:
        parent_id = evidence.get("btc_parent_movement_id")
        parent_start = _canonical_utc(evidence.get("parent_start_time_utc"))[0]
        parent_policy = evidence.get("parent_policy_version")
        membership_status = evidence.get("membership_status")
        parent_eligible = evidence.get("parent_evidence_eligible")
        expected_membership = (
            ("LIVE", True) if parent_authority_class == "LIVE"
            else ("BOUNDARY_UNVERIFIED", False)
        )
        if (not _hash(parent_id)
                or parent_policy
                != _STARTUP_MANIFEST["independence"]["parent_policy_version"]
                or (membership_status, parent_eligible) != expected_membership):
            raise ValueError("selection fact durable parent identity is invalid")

    result = {
        "version": SELECTION_FACT_IDENTITY_VERSION,
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "attempt_id": attempt_id,
        "attempt_fingerprint": attempt_fingerprint,
        "sampler_version": sampler_version,
        "source_candle_open_utc": source_open,
        "source_attempt_evaluation_status":
            source_attempt_evaluation_status,
        "anchor_slot_id": anchor_slot_id,
        "event_id": event_id,
        "event_fingerprint": event_fingerprint,
        "symbol": symbol,
        "direction": direction,
        "decision_time_utc": decision_time_utc,
        "knowledge_status": fact.get("knowledge_status"),
        "candidate_match": fact.get("candidate_match"),
        "parent_authority_class": parent_authority_class,
        "btc_parent_movement_id": parent_id,
        "parent_start_time_utc": parent_start,
        "parent_policy_version": parent_policy,
        "membership_status": membership_status,
        "parent_evidence_eligible": parent_eligible,
    }
    if set(result) != _SELECTION_FACT_IDENTITY_KEYS:
        raise AssertionError("selection fact identity schema drift")
    return result


def selection_fact_identity_sha256(
    exact_binding: Mapping[str, Any], fact: Mapping[str, Any], *,
    source_attempt_evaluation_status: str,
    parent_authority_class: str,
    parent_membership_evidence: Mapping[str, Any] | None,
) -> str:
    """Hash the closed predicted identity, excluding all free-form fact JSON."""
    return contract.digest(canonical_selection_fact_identity(
        exact_binding, fact,
        source_attempt_evaluation_status=source_attempt_evaluation_status,
        parent_authority_class=parent_authority_class,
        parent_membership_evidence=parent_membership_evidence,
    ))


def _representative_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    source = row["representative"]
    return {
        "version": "stage8-outcome-free-representative-identity-v1",
        "exact_binding_sha256": row["binding"]["exact_binding_sha256"],
        "btc_parent_movement_id": row["btc_parent_movement_id"],
        "parent_start_time_utc": row["parent_start_time_utc"],
        "expected_selection_fact_identity_sha256":
            source["expected_selection_fact_identity_sha256"],
        "attempt_fingerprint": source["attempt_fingerprint"],
        "anchor_slot_id": source["anchor_slot_id"],
        "event_id": source["event_id"],
        "event_fingerprint": source["event_fingerprint"],
        "symbol": source["symbol"],
        "direction": source["direction"],
        "decision_time_utc": source["decision_time_utc"],
        "candidate_match_knowledge_status": "KNOWN",
        "candidate_match": True,
    }


def _representative_set_sha256(
    exact_binding_sha256: str, representatives: Sequence[Mapping[str, Any]],
) -> str:
    records = []
    for row in representatives:
        records.append({
            "binding": deepcopy(row["binding"]),
            "btc_parent_movement_id": row["btc_parent_movement_id"],
            "parent_start_time_utc": row["parent_start_time_utc"],
            "representative_status": row["representative_status"],
            "parent_policy_version": row["parent_policy_version"],
            "membership_status": row["membership_status"],
            "parent_evidence_eligible": row["parent_evidence_eligible"],
            "freeze_id": row["freeze_id"],
            "registry_record_sha256": row["registry_record_sha256"],
            "registry_verification_receipt_sha256":
                row["registry_verification_receipt_sha256"],
            "representative": deepcopy(row["representative"]),
            "representative_identity_sha256": row["representative_identity_sha256"],
        })
    records.sort(key=contract.canonical)
    return contract.digest({
        "version": "stage8-outcome-blind-representative-set-v1",
        "exact_binding_sha256": exact_binding_sha256,
        "representatives": records,
    })


def select_representatives(
    exact_binding: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
    *,
    fact_authorities: Sequence[Mapping[str, Any]],
    population_receipt: Mapping[str, Any],
    expected_outcome_free_population_receipt_sha256: str,
    registry_reference: Mapping[str, Any],
    observed_source_hashes: Mapping[str, Any],
) -> dict[str, Any]:
    """Select earliest valid matches, once per parent, without label access.

    ``source_rows`` has an intentionally closed schema and has no outcome or
    label slot.  The complete attempt-ID cohort is checked against the bounded
    database receipt before any representative can be marked complete.
    """
    _assert_runtime_contract()
    contract.validate_exact_binding(exact_binding)
    binding = _representative_binding(exact_binding)
    registry, frozen = _validate_registry_reference(
        registry_reference, exact_binding=exact_binding,
        observed_source_hashes=observed_source_hashes,
    )
    if (isinstance(source_rows, (str, bytes))
            or not isinstance(source_rows, Sequence)
            or isinstance(fact_authorities, (str, bytes))
            or not isinstance(fact_authorities, Sequence)):
        raise ValueError("source rows and fact authorities must be sequences")
    rows = list(source_rows)
    authorities = list(fact_authorities)
    if any(not isinstance(row, Mapping) or set(row) != _SOURCE_ROW_KEYS for row in rows):
        raise ValueError("source row shape invalid; outcome/label fields are forbidden")
    if any(not isinstance(item, Mapping) or set(item) != _FACT_AUTHORITY_KEYS
           for item in authorities):
        raise ValueError("fact authority shape invalid")
    attempt_ids = [row.get("attempt_id") for row in rows]
    authority_ids = [item.get("attempt_id") for item in authorities]
    if (any(not _positive_int64(item) for item in attempt_ids)
            or len(set(attempt_ids)) != len(attempt_ids)
            or any(not _positive_int64(item) for item in authority_ids)
            or len(set(authority_ids)) != len(authority_ids)
            or sorted(authority_ids) != sorted(attempt_ids)):
        raise ValueError("attempt rows and out-of-band authorities are not one-to-one")
    receipt = _validate_population_receipt(
        population_receipt, exact_binding=exact_binding,
        attempt_ids=attempt_ids,
        expected_outcome_free_receipt_sha256=
            expected_outcome_free_population_receipt_sha256,
    )
    authority_by_id = {item["attempt_id"]: item for item in authorities}

    global_blockers: list[str] = []
    parent_observations: dict[str, list[dict[str, Any]]] = {}
    parent_starts: dict[str, str] = {}
    excluded_pre_freeze: set[str] = set()
    proven_noneligible_attempt_ids: list[int] = []
    seen_fingerprints: dict[str, int] = {}
    exact_event_rows: dict[tuple[int, int], dict[str, Any]] = {}
    anchor_to_event: dict[int, int] = {}
    event_to_anchor: dict[int, int] = {}
    deduplicated_exact_events = 0
    authority_ledger: list[dict[str, Any]] = []

    for row in sorted(rows, key=lambda item: item["attempt_id"]):
        attempt_id = row["attempt_id"]
        authority = authority_by_id[attempt_id]
        fact = row.get("fact")
        if not isinstance(fact, Mapping):
            global_blockers.append("PROJECTED_FACT_MISSING_OR_INVALID")
            continue
        for key in ("expected_fact_sha256",):
            if not _hash(authority.get(key)):
                global_blockers.append("FACT_AUTHORITY_HASH_INVALID")
        selection_hash = authority.get("expected_watch_selection_attestation_sha256")
        if selection_hash is not None and not _hash(selection_hash):
            global_blockers.append("FACT_AUTHORITY_HASH_INVALID")
        try:
            _VALIDATE_FACT(
                fact,
                expected_fact_sha256=authority.get("expected_fact_sha256"),
                expected_watch_selection_attestation_sha256=selection_hash,
                expected_watch_code_manifest_sha256=
                    registry["expected_watch_code_manifest_sha256"],
            )
        except (TypeError, ValueError, KeyError, OverflowError):
            global_blockers.append("PROJECTED_FACT_AUTHORITY_INVALID")
            continue
        identity = _mapping(fact.get("identity"))
        if (identity.get("attempt_id") != attempt_id
                or fact.get("binding", {}).get("binding_sha256")
                != exact_binding["binding_sha256"]
                or identity.get("symbol") not in binding["scope_symbols"]
                or identity.get("direction") != binding["direction"]):
            global_blockers.append("PROJECTED_FACT_COHORT_OR_BINDING_MISMATCH")
            continue
        fingerprint = identity.get("attempt_fingerprint")
        if not _hash(fingerprint):
            global_blockers.append("ATTEMPT_FINGERPRINT_INVALID")
            continue
        if fingerprint in seen_fingerprints and seen_fingerprints[fingerprint] != attempt_id:
            global_blockers.append("ATTEMPT_FINGERPRINT_REUSED")
            continue
        seen_fingerprints[fingerprint] = attempt_id

        parent_source = row.get("parent_membership_source")
        noneligibility = row.get("noneligibility_proof")
        if (parent_source is None) == (noneligibility is None):
            global_blockers.append("EXACTLY_ONE_MEMBERSHIP_OR_NONELIGIBILITY_PROOF_REQUIRED")
            continue
        if noneligibility is not None:
            try:
                proof = _validate_noneligibility(
                    noneligibility, attempt_id=attempt_id, fact=fact,
                )
                if (authority.get("expected_parent_membership_evidence_sha256") is not None
                        or authority.get("expected_noneligibility_proof_sha256")
                        != proof["proof_sha256"]):
                    raise ValueError("noneligibility authority mismatch")
            except (TypeError, ValueError, KeyError, OverflowError):
                global_blockers.append("NONELIGIBILITY_PROOF_INVALID")
            else:
                try:
                    expected_identity_sha = selection_fact_identity_sha256(
                        exact_binding, fact,
                        source_attempt_evaluation_status=
                            proof["evaluation_status"],
                        parent_authority_class=
                            "PROVEN_NOT_CANDIDATE_ELIGIBLE",
                        parent_membership_evidence=None,
                    )
                except (TypeError, ValueError, KeyError, OverflowError):
                    global_blockers.append("SELECTION_FACT_IDENTITY_INVALID")
                else:
                    proven_noneligible_attempt_ids.append(attempt_id)
                    authority_ledger.append({
                        "attempt_id": attempt_id,
                        "expected_selection_fact_identity_sha256":
                            expected_identity_sha,
                    })
            continue
        try:
            evidence, membership_class = canonical_parent_membership_evidence(
                parent_source, fact=fact, direction=binding["direction"],
            )
            evidence_sha256 = contract.digest(evidence)
            if (authority.get("expected_noneligibility_proof_sha256") is not None
                    or authority.get("expected_parent_membership_evidence_sha256")
                    != evidence_sha256):
                raise ValueError("parent membership authority mismatch")
        except (TypeError, ValueError, KeyError, OverflowError):
            global_blockers.append("PARENT_MEMBERSHIP_UNKNOWN_OR_INVALID")
            continue
        try:
            expected_identity_sha = selection_fact_identity_sha256(
                exact_binding, fact,
                source_attempt_evaluation_status="EVALUABLE",
                parent_authority_class=membership_class,
                parent_membership_evidence=evidence,
            )
        except (TypeError, ValueError, KeyError, OverflowError):
            global_blockers.append("SELECTION_FACT_IDENTITY_INVALID")
            continue
        if membership_class == "UNKNOWN":
            global_blockers.append("PARENT_MEMBERSHIP_UNKNOWN_OR_INVALID")
            authority_ledger.append({
                "attempt_id": attempt_id,
                "expected_selection_fact_identity_sha256":
                    expected_identity_sha,
            })
            continue
        authority_ledger.append({
            "attempt_id": attempt_id,
            "expected_selection_fact_identity_sha256": expected_identity_sha,
        })
        if membership_class == "PROVEN_NOT_EVIDENCE_ELIGIBLE":
            proven_noneligible_attempt_ids.append(attempt_id)
            continue

        parent_id = evidence["btc_parent_movement_id"]
        parent_start = evidence["parent_start_time_utc"]
        if (not _hash(parent_id)
                or evidence["parent_policy_version"] != binding["parent_policy_version"]
                or parent_id != btc_parent._identity(_canonical_utc(parent_start)[1])):
            global_blockers.append("BTC_PARENT_IDENTITY_INVALID")
            continue
        if parent_id in parent_starts and parent_starts[parent_id] != parent_start:
            global_blockers.append("BTC_PARENT_START_CONFLICT")
            continue
        parent_starts[parent_id] = parent_start
        if _canonical_utc(parent_start)[1] <= frozen:
            excluded_pre_freeze.add(parent_id)
            continue

        order = (
            _canonical_utc(identity.get("decision_time_utc"))[1],
            identity.get("symbol"), identity.get("anchor_slot_id"), identity.get("event_id"),
        )
        if (not isinstance(order[1], str)
                or not _positive_int64(order[2]) or not _positive_int64(order[3])):
            global_blockers.append("REPRESENTATIVE_ORDER_IDENTITY_INVALID")
            continue
        exact_event = (order[2], order[3])
        observation = {
            "attempt_id": attempt_id,
            "attempt_fingerprint": fingerprint,
            "anchor_slot_id": order[2],
            "event_id": order[3],
            "event_fingerprint": identity.get("event_fingerprint"),
            "symbol": order[1],
            "direction": identity.get("direction"),
            "decision_time_utc": identity.get("decision_time_utc"),
            "expected_selection_fact_identity_sha256":
                expected_identity_sha,
            "candidate_match_knowledge_status": fact.get("knowledge_status"),
            "candidate_match": fact.get("candidate_match"),
            "parent_id": parent_id,
            "parent_start_time_utc": parent_start,
            "order": order,
        }
        prior = exact_event_rows.get(exact_event)
        comparable = {key: value for key, value in observation.items()
                      if key not in {"attempt_id", "order"}}
        if prior is not None:
            prior_comparable = {key: value for key, value in prior.items()
                                if key not in {"attempt_id", "order"}}
            if contract.canonical(comparable) != contract.canonical(prior_comparable):
                global_blockers.append("CONFLICTING_EXACT_ANCHOR_EVENT")
            else:
                deduplicated_exact_events += 1
            continue
        if (order[2] in anchor_to_event and anchor_to_event[order[2]] != order[3]):
            global_blockers.append("ANCHOR_REUSED_WITH_DIFFERENT_DIRECTION_EVENT")
            continue
        if (order[3] in event_to_anchor and event_to_anchor[order[3]] != order[2]):
            global_blockers.append("EVENT_REUSED_ACROSS_ANCHORS")
            continue
        anchor_to_event[order[2]] = order[3]
        event_to_anchor[order[3]] = order[2]
        exact_event_rows[exact_event] = observation
        parent_observations.setdefault(parent_id, []).append(observation)

    representatives: list[dict[str, Any]] = []
    blocked_parents: list[dict[str, Any]] = []
    for parent_id in sorted(parent_observations):
        observations = sorted(parent_observations[parent_id], key=lambda item: item["order"])
        matches = [item for item in observations
                   if item["candidate_match_knowledge_status"] == "KNOWN"
                   and item["candidate_match"] is True]
        earliest = matches[0] if matches else None
        unknown_before = [item for item in observations
                          if item["candidate_match_knowledge_status"] != "KNOWN"
                          and (earliest is None or item["order"] < earliest["order"])]
        if unknown_before:
            blocked_parents.append({
                "btc_parent_movement_id": parent_id,
                "reason": "EARLIER_CANDIDATE_MATCH_ELIGIBILITY_UNKNOWN",
                "blocking_attempt_ids": sorted(item["attempt_id"] for item in unknown_before),
                "later_known_match_attempt_id": earliest["attempt_id"] if earliest else None,
            })
            continue
        if earliest is None:
            continue
        representative = {
            "binding": deepcopy(binding),
            "btc_parent_movement_id": parent_id,
            "parent_start_time_utc": earliest["parent_start_time_utc"],
            "representative_status": "VALID",
            "parent_policy_version": binding["parent_policy_version"],
            "membership_status": "LIVE",
            "parent_evidence_eligible": True,
            "freeze_id": registry["freeze_id"],
            "registry_record_sha256": registry["registry_record_sha256"],
            "registry_verification_receipt_sha256":
                registry["registry_verification_receipt_sha256"],
            "selection_attestation_sha256": None,
            "representative": {
                key: earliest[key] for key in (
                    "expected_selection_fact_identity_sha256",
                    "attempt_fingerprint", "anchor_slot_id",
                    "event_id", "event_fingerprint", "symbol", "direction",
                    "decision_time_utc", "candidate_match_knowledge_status",
                    "candidate_match",
                )
            },
        }
        representative["representative_identity_sha256"] = contract.digest(
            _representative_identity(representative)
        )
        representatives.append(representative)

    representatives.sort(key=lambda item: item["btc_parent_movement_id"])
    blockers = sorted(set(global_blockers))
    if len(authority_ledger) != len(rows):
        blockers = sorted(set(blockers) | {"SOURCE_AUTHORITY_LEDGER_INCOMPLETE"})
    selection_complete = not blockers and not blocked_parents
    representative_set_sha256 = _representative_set_sha256(
        exact_binding["binding_sha256"], representatives
    )
    authority_ledger.sort(key=lambda item: item["attempt_id"])
    source_authority_ledger_sha256 = contract.digest({
        "version": "stage8-selector-source-authority-ledger-v1",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "attempt_population_sha256": receipt["attempt_population_sha256"],
        "entries": authority_ledger,
    })
    batch = {
        "version": BATCH_VERSION,
        "selector_version": VERSION,
        "structural_authority": STRUCTURAL_AUTHORITY,
        "status": "COMPLETE" if selection_complete else "BLOCKED",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "manifest_sha256": _MANIFEST_SHA256,
        "freeze_id": registry["freeze_id"],
        "frozen_at_utc": registry["frozen_at_utc"],
        "registry_record_sha256": registry["registry_record_sha256"],
        "registry_verification_receipt_sha256":
            registry["registry_verification_receipt_sha256"],
        "verifier_profile_sha256": registry["verifier_profile_sha256"],
        "expected_projection_source_sha256":
            registry["expected_projection_source_sha256"],
        "expected_selector_source_sha256": registry["expected_selector_source_sha256"],
        "expected_watch_code_manifest_sha256":
            registry["expected_watch_code_manifest_sha256"],
        "cohort_query_sha256": receipt["query_scope"]["query_sha256"],
        "outcome_free_population_receipt_sha256":
            receipt["outcome_free_population_receipt_sha256"],
        "attempt_population_sha256": receipt["attempt_population_sha256"],
        "source_transaction_identity_sha256": receipt["transaction_identity_sha256"],
        "source_high_water_attempt_id": receipt["high_water_attempt_id"],
        "source_attempt_count": len(rows),
        "source_authority_ledger_count": len(authority_ledger),
        "source_authority_ledger_sha256": source_authority_ledger_sha256,
        "representative_count": len(representatives),
        "representative_set_sha256": representative_set_sha256,
        "population_coverage_complete": selection_complete,
        "candidate_match_coverage_complete": selection_complete,
        "outcome_blind_selection": True,
        "outcome_or_label_fields_accepted": False,
        "truncated": False,
        "global_blockers": blockers,
        "blocked_parents": blocked_parents,
        "excluded_pre_freeze_parent_ids": sorted(excluded_pre_freeze),
        "proven_noneligible_attempt_ids": sorted(proven_noneligible_attempt_ids),
        "deduplicated_exact_anchor_event_count": deduplicated_exact_events,
        "qualification_evaluated": False,
        "database_verification_asserted_by_selector": False,
    }
    # This is a pre-persistence structural batch hash, not an acceptance
    # provenance claim.  The append-only registry must independently derive
    # each database-owned selection-fact identity from sealed source rows,
    # compare it to every expected digest above, and hash the stored batch.
    # Only a later readback adapter may normalize those stored identities for
    # the authoritative acceptance evaluator.
    selection_attestation_sha256 = contract.digest(batch)
    result = {
        **batch,
        # Audit-only reference.  It is intentionally outside the selection
        # batch hash because the full receipt contains
        # label/outcome coverage counts.
        "source_audit_receipt_sha256": receipt["full_audit_receipt_sha256"],
        "selection_attestation_sha256": selection_attestation_sha256,
        "representatives": representatives,
    }
    return {**result, "selector_receipt_sha256": contract.digest(result)}


__all__ = [
    "VERSION", "POPULATION_VERSION", "BATCH_VERSION",
    "MEMBERSHIP_EVIDENCE_VERSION", "NONELIGIBILITY_VERSION",
    "SELECTION_FACT_IDENTITY_VERSION",
    "canonical_parent_membership_evidence",
    "canonical_selection_fact_identity", "selection_fact_identity_sha256",
    "select_representatives",
]
