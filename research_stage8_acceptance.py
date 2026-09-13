"""Exact, fail-closed Stage-8 experimental research acceptance evaluator.

The caller supplies one already selected, outcome-blind representative per BTC
parent movement.  This module never selects or replaces a representative,
reads a database, sends a notification, approves LIVE delivery, or executes a
trade.  Each route has its own complete-evidence parent set and must satisfy
the five-parent floor itself; counts are never borrowed across routes.

The public API can establish structural and atomic diagnostic results only and
always leaves ``research_qualified`` false.  The separate read-only Stage-8
outcome adapter also produces caller-side diagnostics only.  Research-only
qualification requires the persisted database-trigger result after server-owned
fact replay and atomic-gate verification.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import math
import re
from statistics import median
from typing import Any, Mapping, Sequence

import research_btc_parent_movement as btc_parent
import research_stage8_contract as contract


VERSION = "stage8-experimental-acceptance-evaluator-v1"
# Informational compatibility exports only. Runtime gates are always read from
# the hash-validated frozen manifest; mutating these names cannot change a gate.
POLICY_VERSION = "stage8-five-parent-probability-or-asymmetry-v1"
MINIMUM_DISTINCT_PARENTS = 5
PROBABILITY_HIT_RATE_FLOOR_PCT = 70.0
PROBABILITY_WILSON_FLOOR_PCT = 40.0
ASYMMETRY_RATIO_FLOOR = 1.5
ASYMMETRY_DOMINANCE_FLOOR_PCT = 60.0
PARENT_POLICY_VERSION = btc_parent.POLICY_VERSION
COMMON_WINDOW_METHOD_VERSION = "common-window-spot-1m-v1"
_PARENT_ID = re.compile(r"[0-9a-f]{64}\Z")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite_number(value: Any) -> float | None:
    """Strict JSON-number validation; strings and booleans are not evidence."""
    if isinstance(value, bool) or type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _canonical_utc(value: Any) -> tuple[str, datetime]:
    """Require the sampler/projection fixed-microsecond ``Z`` UTC codec."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be a canonical UTC string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("timestamp must be a canonical UTC string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    normalized = parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")
    if value != normalized:
        raise ValueError("timestamp is not canonical UTC")
    return normalized, parsed.astimezone(timezone.utc)


def _manifest() -> dict[str, Any]:
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    return manifest


def _expected_policy_core() -> dict[str, Any]:
    return {
        "version": "stage8-five-parent-probability-or-asymmetry-v1",
        "atomic_expression": (
            "N_ROUTE_DISTINCT_BTC_PARENTS >= 5 AND (PROBABILITY OR ASYMMETRY)"
        ),
        "minimum_distinct_parents_per_passing_route": 5,
        "combination": "PROBABILITY_OR_ASYMMETRY",
        "route_cohort": "ONE_OUTCOME_BLIND_FROZEN_REPRESENTATIVE_SET_PER_EXACT_BINDING",
        "route_eligible_subsets": "DISCLOSE_EXACT_PARENT_IDS_NO_CROSS_ROUTE_COUNT_BORROWING",
        "probability": {
            "hit_rate_pct_gte": 70,
            "wilson_95_lower_pct_gte": 40,
        },
        "asymmetry": {
            "common_window_asymmetry_ratio_gte": 1.5,
            "common_window_favorable_dominance_pct_gte": 60,
            "common_window_median_paired_edge_pct_gt": 0,
        },
        "metric_definitions": {
            "weighting": "ONE_UNWEIGHTED_VOTE_PER_FROZEN_PARENT_REPRESENTATIVE",
            "hit_rate_pct": "100*successes/(successes+failures)",
            "wilson_method": "TWO_SIDED_95_PERCENT_WILSON_SCORE_LOWER_BOUND_NO_CONTINUITY_CORRECTION",
            "wilson_z": 1.959963984540054,
            "wilson_95_lower_pct": "100*max(0,(p+z*z/(2*n)-z*sqrt((p*(1-p)+z*z/(4*n))/n))/(1+z*z/n))",
            "wilson_p": "successes/n",
            "wilson_n": "successes+failures",
            "no_decisive_labels": "UNKNOWN_NOT_ZERO_PERCENT",
            "excursion_unit": "NONNEGATIVE_FINITE_PERCENT_OF_SAME_REPRESENTATIVE_ENTRY_PRICE",
            "within_parent_excursions": "SINGLE_FROZEN_EARLIEST_REPRESENTATIVE_NO_MULTI_COIN_MIN_MAX_COLLAPSE",
            "common_window_asymmetry_ratio": "sum(representative_mfe_pct)/sum(representative_mae_pct)",
            "positive_mfe_zero_mae": "ZERO_DENOMINATOR_RATIO_NULL_ASYMMETRY_ROUTE_UNAVAILABLE",
            "zero_mfe_zero_mae": "UNDEFINED_ZERO_ZERO_RATIO_NULL_DOES_NOT_PASS",
            "common_window_favorable_dominance_pct": "100*count(mfe_pct>mae_pct)/full_window_parent_count",
            "common_window_median_paired_edge_pct": "median(mfe_pct-mae_pct)",
            "even_sample_median": "ARITHMETIC_MEAN_OF_TWO_MIDDLE_SORTED_VALUES",
            "json_nonfinite_allowed": False,
        },
        "fresh_three_parent_route": False,
        "unavailable_other_route_blocks_passing_route": False,
        "shared_coverage_or_provenance_failure_blocks_all_routes": True,
        "maximum_result": "EXPERIMENTAL_RESEARCH_ONLY",
    }


def validate_policy_compatibility(policy: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return the exact frozen policy or reject any caller/manifest mismatch."""
    manifest_policy = _manifest()["acceptance"]
    expected = _expected_policy_core()
    if any(
        contract.canonical(manifest_policy.get(key))
        != contract.canonical(value)
        for key, value in expected.items()
    ):
        raise ValueError("STAGE8_MANIFEST_ACCEPTANCE_POLICY_INCOMPATIBLE")
    supplied = manifest_policy if policy is None else policy
    try:
        same = contract.canonical(supplied) == contract.canonical(manifest_policy)
    except (TypeError, ValueError):
        same = False
    if not same:
        raise ValueError("STAGE8_ACCEPTANCE_POLICY_MISMATCH")
    return deepcopy(manifest_policy)


def representative_binding(exact_binding: Mapping[str, Any]) -> dict[str, Any]:
    """Exact identity every supplied representative must carry."""
    contract.validate_exact_binding(exact_binding)
    binding = exact_binding["binding"]
    manifest = _manifest()
    scope = binding["scope"]
    candidate = binding["candidate"]
    return {
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "manifest_sha256": binding["manifest_sha256"],
        "contract_version": binding["version"],
        "source_version": binding["source_version"],
        "projection_version": binding["projection_version"],
        "label_version": binding["label_version"],
        "independence_version": binding["independence_version"],
        "acceptance_version": binding["acceptance_version"],
        "scope_id": scope["scope_id"],
        "scope_symbols": list(scope["symbols"]),
        "scope_price_route": scope["price_route"],
        "candidate_id": candidate["candidate_id"],
        "candidate_model": candidate["model"],
        "direction": candidate["direction"],
        "window_minutes": binding["window_minutes"],
        "threshold_bps": binding["threshold_bps"],
        "parent_policy_version": manifest["independence"]["parent_policy_version"],
    }


def _valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and _PARENT_ID.fullmatch(value) is not None


def _positive_int64(value: Any) -> bool:
    return type(value) is int and 0 < value <= 9223372036854775807


def representative_identity(
    exact_binding: Mapping[str, Any], row: Mapping[str, Any]
) -> dict[str, Any]:
    """Return the outcome-free structural identity of one representative.

    ``selection_fact_identity_sha256`` is the database-derived digest of the
    closed selection identity.  The free-form projected fact hash is
    deliberately absent: it remains replay/audit evidence and cannot steer
    representative selection.
    """
    expected = representative_binding(exact_binding)
    if not isinstance(row, Mapping) or not _row_binding_matches(row, expected):
        raise ValueError("STAGE8_REPRESENTATIVE_BINDING_INVALID")
    source = _mapping(row.get("representative"))
    parent_id = row.get("btc_parent_movement_id")
    if not _valid_sha256(parent_id):
        raise ValueError("STAGE8_REPRESENTATIVE_PARENT_INVALID")
    _, parent_start = _canonical_utc(row.get("parent_start_time_utc"))
    _, decision = _canonical_utc(source.get("decision_time_utc"))
    if (parent_id != btc_parent._identity(parent_start)
            or decision < parent_start
            or not _positive_int64(source.get("anchor_slot_id"))
            or not _positive_int64(source.get("event_id"))
            or not _valid_sha256(source.get("selection_fact_identity_sha256"))
            or not _valid_sha256(source.get("attempt_fingerprint"))
            or not _valid_sha256(source.get("event_fingerprint"))
            or source.get("symbol") not in expected["scope_symbols"]
            or source.get("direction") != expected["direction"]
            or source.get("candidate_match_knowledge_status") != "KNOWN"
            or source.get("candidate_match") is not True):
        raise ValueError("STAGE8_REPRESENTATIVE_EVENT_FACT_INVALID")
    return {
        "version": "stage8-outcome-free-representative-identity-v1",
        "exact_binding_sha256": expected["exact_binding_sha256"],
        "btc_parent_movement_id": parent_id,
        "parent_start_time_utc": row["parent_start_time_utc"],
        "selection_fact_identity_sha256": source[
            "selection_fact_identity_sha256"
        ],
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


def representative_identity_sha256(
    exact_binding: Mapping[str, Any], row: Mapping[str, Any]
) -> str:
    return contract.digest(representative_identity(exact_binding, row))


def _outcome_free_set_record(row: Mapping[str, Any]) -> dict[str, Any]:
    """Project only selection-time fields; outcomes cannot alter this digest."""
    return {
        "binding": deepcopy(row.get("binding")),
        "btc_parent_movement_id": row.get("btc_parent_movement_id"),
        "parent_start_time_utc": row.get("parent_start_time_utc"),
        "representative_status": row.get("representative_status"),
        "parent_policy_version": row.get("parent_policy_version"),
        "membership_status": row.get("membership_status"),
        "parent_evidence_eligible": row.get("parent_evidence_eligible"),
        "freeze_id": row.get("freeze_id"),
        "registry_record_sha256": row.get("registry_record_sha256"),
        "registry_verification_receipt_sha256": row.get(
            "registry_verification_receipt_sha256"
        ),
        "representative": deepcopy(row.get("representative")),
        "representative_identity_sha256": row.get(
            "representative_identity_sha256"
        ),
    }


def _representative_set(
    exact_binding: Mapping[str, Any], representatives: Sequence[Mapping[str, Any]]
) -> tuple[int, str]:
    contract.validate_exact_binding(exact_binding)
    if (isinstance(representatives, (str, bytes))
            or not isinstance(representatives, Sequence)
            or any(not isinstance(row, Mapping) for row in representatives)):
        raise ValueError("STAGE8_REPRESENTATIVE_SET_INVALID")
    records = [_outcome_free_set_record(row) for row in representatives]
    records.sort(key=contract.canonical)
    payload = {
        "version": "stage8-outcome-blind-representative-set-v1",
        "exact_binding_sha256": exact_binding["binding_sha256"],
        "representatives": records,
    }
    return len(records), contract.digest(payload)


def bind_registry_selection_receipt(
    exact_binding: Mapping[str, Any],
    representatives: Sequence[Mapping[str, Any]],
    *,
    freeze_id: str,
    frozen_at_utc: str,
    registry_record_sha256: str,
    registry_verification_receipt_sha256: str,
    cohort_query_sha256: str,
    population_receipt_sha256: str,
    source_high_water_attempt_id: int,
) -> dict[str, Any]:
    """Bind a selector result to caller-supplied registry receipt references.

    This pure helper does not verify persistence. The separate read-only
    outcome adapter must obtain and verify both registry hashes from the
    durable row before it can rely on this structure.
    The representative-set digest only uses outcome-free identities, and the
    population receipt/query high-water prevents a subset from masquerading as
    the selector's complete cohort.
    """
    identity = representative_binding(exact_binding)
    manifest = _manifest()
    independence = manifest["independence"]
    if not _valid_sha256(freeze_id):
        raise ValueError("freeze_id must be a 64-character lowercase hex identity")
    for name, value in (
        ("registry_record_sha256", registry_record_sha256),
        ("registry_verification_receipt_sha256", registry_verification_receipt_sha256),
        ("cohort_query_sha256", cohort_query_sha256),
        ("population_receipt_sha256", population_receipt_sha256),
    ):
        if not _valid_sha256(value):
            raise ValueError(name + " must be lowercase SHA-256")
    if (type(source_high_water_attempt_id) is not int
            or not 0 <= source_high_water_attempt_id <= 9223372036854775807):
        raise ValueError("source_high_water_attempt_id must be a nonnegative int64")
    frozen, _ = _canonical_utc(frozen_at_utc)
    representative_count, representative_set_sha256 = _representative_set(
        exact_binding, representatives
    )
    value = {
        "registration_evidence": "CALLER_SUPPLIED_REGISTRY_REFERENCES_NOT_DB_VERIFIED",
        "freeze_id": freeze_id,
        "frozen_at_utc": frozen,
        "registry_record_sha256": registry_record_sha256,
        "registry_verification_receipt_sha256": registry_verification_receipt_sha256,
        "status": "COMPLETE",
        "exact_binding_sha256": identity["exact_binding_sha256"],
        "manifest_sha256": identity["manifest_sha256"],
        "acceptance_policy_version": manifest["acceptance"]["version"],
        "acceptance_policy_sha256": contract.digest(manifest["acceptance"]),
        "independence_version": identity["independence_version"],
        "parent_policy_version": identity["parent_policy_version"],
        "representative_policy": independence["representative"],
        "eligible_parent_rule": manifest["prospective_registration"]["eligible_parent_rule"],
        "prospective_clock_basis": "DURABLE_REGISTRY_FROZEN_AT_RECEIPT_NOT_LOCAL_CLOCK",
        "cohort_query_sha256": cohort_query_sha256,
        "population_receipt_sha256": population_receipt_sha256,
        "source_high_water_attempt_id": source_high_water_attempt_id,
        "representative_count": representative_count,
        "representative_set_sha256": representative_set_sha256,
        "population_coverage_complete": True,
        "candidate_match_coverage_complete": True,
        "outcome_blind_selection": True,
        "truncated": False,
    }
    return {**value, "attestation_sha256": contract.digest(value)}


def _selection_matches(
    selection_provenance: Mapping[str, Any] | None,
    exact_binding: Mapping[str, Any],
    representatives: Sequence[Mapping[str, Any]],
) -> bool:
    try:
        supplied = _mapping(selection_provenance)
        return contract.canonical(supplied) == contract.canonical(
            bind_registry_selection_receipt(
                exact_binding,
                representatives,
                freeze_id=supplied["freeze_id"],
                frozen_at_utc=supplied["frozen_at_utc"],
                registry_record_sha256=supplied["registry_record_sha256"],
                registry_verification_receipt_sha256=supplied[
                    "registry_verification_receipt_sha256"
                ],
                cohort_query_sha256=supplied["cohort_query_sha256"],
                population_receipt_sha256=supplied["population_receipt_sha256"],
                source_high_water_attempt_id=supplied["source_high_water_attempt_id"],
            )
        )
    except (TypeError, ValueError, KeyError):
        return False


def _row_binding_matches(row: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    try:
        return contract.canonical(row.get("binding")) == contract.canonical(expected)
    except (TypeError, ValueError):
        return False


def _valid_parent(
    row: Mapping[str, Any], selection_provenance: Mapping[str, Any] | None,
    identity: Mapping[str, Any], exact_binding: Mapping[str, Any],
) -> tuple[str | None, str | None, list[str]]:
    reasons: list[str] = []
    parent_id = row.get("btc_parent_movement_id")
    if not isinstance(parent_id, str) or not _PARENT_ID.fullmatch(parent_id):
        reasons.append("MISSING_OR_INVALID_BTC_PARENT_MOVEMENT_ID")
        parent_id = None
    if row.get("representative_status") != "VALID":
        reasons.append("REPRESENTATIVE_NOT_VALID")
    if row.get("parent_policy_version") != identity["parent_policy_version"]:
        reasons.append("PARENT_POLICY_VERSION_MISMATCH")
    if row.get("membership_status") != "LIVE":
        reasons.append("PARENT_MEMBERSHIP_NOT_LIVE")
    if row.get("parent_evidence_eligible") is not True:
        reasons.append("PARENT_NOT_EVIDENCE_ELIGIBLE")
    provenance = _mapping(selection_provenance)
    if (row.get("freeze_id") != provenance.get("freeze_id")
            or row.get("registry_record_sha256") != provenance.get("registry_record_sha256")
            or row.get("registry_verification_receipt_sha256")
            != provenance.get("registry_verification_receipt_sha256")
            or row.get("selection_attestation_sha256") != provenance.get("attestation_sha256")):
        reasons.append("REPRESENTATIVE_REGISTRATION_BINDING_MISMATCH")
    representative_sha256 = None
    try:
        representative_sha256 = representative_identity_sha256(exact_binding, row)
        if row.get("representative_identity_sha256") != representative_sha256:
            reasons.append("REPRESENTATIVE_IDENTITY_HASH_MISMATCH")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("REPRESENTATIVE_EVENT_FACT_IDENTITY_INVALID")
    try:
        _, frozen = _canonical_utc(provenance.get("frozen_at_utc"))
        _, parent_start = _canonical_utc(row.get("parent_start_time_utc"))
        if parent_start <= frozen:
            reasons.append("PARENT_NOT_STRICTLY_AFTER_DURABLE_FREEZE")
        if parent_id is not None and parent_id != btc_parent._identity(parent_start):
            reasons.append("BTC_PARENT_CANONICAL_ID_MISMATCH")
    except ValueError:
        reasons.append("FREEZE_OR_PARENT_START_TIME_INVALID")
    return parent_id, representative_sha256, reasons


def _probability_value(
    row: Mapping[str, Any], identity: Mapping[str, Any], parent_id: str,
    representative_sha256: str, manifest: Mapping[str, Any],
) -> tuple[bool | None, list[str]]:
    item = _mapping(row.get("probability_evidence"))
    reasons: list[str] = []
    if not item:
        return None, ["PROBABILITY_EVIDENCE_MISSING"]
    if item.get("validation_status") != "VALID":
        reasons.append("PROBABILITY_EVIDENCE_NOT_VALID")
    if item.get("exact_binding_sha256") != identity["exact_binding_sha256"]:
        reasons.append("PROBABILITY_BINDING_MISMATCH")
    if item.get("btc_parent_movement_id") != parent_id:
        reasons.append("PROBABILITY_PARENT_MISMATCH")
    representative = _mapping(row.get("representative"))
    if item.get("representative_identity_sha256") != representative_sha256:
        reasons.append("PROBABILITY_REPRESENTATIVE_IDENTITY_MISMATCH")
    if item.get("event_id") != representative.get("event_id"):
        reasons.append("PROBABILITY_REPRESENTATIVE_EVENT_MISMATCH")
    if (item.get("selection_fact_identity_sha256")
            != representative.get("selection_fact_identity_sha256")):
        reasons.append("PROBABILITY_SELECTION_FACT_IDENTITY_MISMATCH")
    if item.get("direction") != identity["direction"]:
        reasons.append("PROBABILITY_DIRECTION_MISMATCH")
    if item.get("method_version") != manifest["labels"]["method_version"]:
        reasons.append("PROBABILITY_METHOD_VERSION_MISMATCH")
    if item.get("window_minutes") != identity["window_minutes"]:
        reasons.append("PROBABILITY_WINDOW_MISMATCH")
    if item.get("threshold_bps") != identity["threshold_bps"]:
        reasons.append("PROBABILITY_THRESHOLD_MISMATCH")
    status = item.get("reported_status")
    decisive = frozenset(manifest["labels"]["probability_decisive_statuses"])
    if status not in decisive:
        reasons.append("PROBABILITY_LABEL_NONDECISIVE:" + str(status or "UNKNOWN"))
    if reasons:
        return None, reasons
    return status == "SUCCESS", []


def _asymmetry_value(
    row: Mapping[str, Any], identity: Mapping[str, Any], parent_id: str,
    representative_sha256: str, manifest: Mapping[str, Any],
) -> tuple[tuple[float, float] | None, list[str]]:
    item = _mapping(row.get("asymmetry_evidence"))
    reasons: list[str] = []
    if not item:
        return None, ["ASYMMETRY_EVIDENCE_MISSING"]
    representative = _mapping(row.get("representative"))
    expected = {
        "validation_status": "VALID",
        "exact_binding_sha256": identity["exact_binding_sha256"],
        "btc_parent_movement_id": parent_id,
        "representative_identity_sha256": representative_sha256,
        "event_id": representative.get("event_id"),
        "selection_fact_identity_sha256": representative.get(
            "selection_fact_identity_sha256"
        ),
        "direction": identity["direction"],
        "method_version": manifest["labels"]["asymmetry_source_version"],
        "measurement_kind": "FIXED_WINDOW",
        "reported_status": "READY",
        "window_minutes": identity["window_minutes"],
        "scope_price_route": identity["scope_price_route"],
        "path_complete": True,
        "observation_closed": True,
        "coverage_complete": True,
    }
    for key, value in expected.items():
        if item.get(key) != value:
            reasons.append("ASYMMETRY_EVIDENCE_MISMATCH:" + key)
    mfe = _finite_number(item.get("mfe_pct"))
    mae = _finite_number(item.get("mae_pct"))
    if mfe is None or mae is None or min(mfe, mae) < 0.0:
        reasons.append("ASYMMETRY_METRICS_INVALID")
    if reasons:
        return None, reasons
    return (mfe, mae), []


def _frozen_wilson(successes: int, total: int, *, z: float) -> float | None:
    if not total:
        return None
    proportion = successes / total
    return 100.0 * max(0.0, (
        proportion + z * z / (2 * total)
        - z * math.sqrt(
            (proportion * (1.0 - proportion) + z * z / (4 * total)) / total
        )
    ) / (1.0 + z * z / total))


def _probability_route(
    values: Mapping[str, bool], policy: Mapping[str, Any]
) -> dict[str, Any]:
    parents = sorted(values)
    successes = sum(values.values())
    total = len(parents)
    hit_rate = 100.0 * successes / total if total else None
    gates = policy["probability"]
    minimum = policy["minimum_distinct_parents_per_passing_route"]
    z = float(policy["metric_definitions"]["wilson_z"])
    wilson = _frozen_wilson(successes, total, z=z)
    checks = {
        "minimum_distinct_parents": total >= minimum,
        "hit_rate_pct_gte_70": hit_rate is not None
        and hit_rate >= float(gates["hit_rate_pct_gte"]),
        "wilson_95_lower_pct_gte_40": wilson is not None
        and wilson >= float(gates["wilson_95_lower_pct_gte"]),
    }
    return {
        "status": (
            "PASS"
            if all(checks.values())
            else "UNAVAILABLE"
            if not total
            else "INSUFFICIENT"
            if not checks["minimum_distinct_parents"]
            else "FAIL"
        ),
        "passed": all(checks.values()),
        "distinct_parent_count": total,
        "btc_parent_movement_ids": parents,
        "successes": successes,
        "failures": total - successes,
        "hit_rate_pct": hit_rate,
        "wilson_95_lower_pct": wilson,
        "checks": checks,
    }


def _asymmetry_route(
    values: Mapping[str, tuple[float, float]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    parents = sorted(values)
    pairs = [values[parent_id] for parent_id in parents]
    try:
        total_mfe = math.fsum(value[0] for value in pairs)
        total_mae = math.fsum(value[1] for value in pairs)
    except OverflowError:
        total_mfe = total_mae = None
    ratio = (
        total_mfe / total_mae
        if pairs and total_mfe is not None and total_mae is not None and total_mae > 0.0
        else None
    )
    if ratio is not None and not math.isfinite(ratio):
        ratio = None
    dominance = (
        100.0 * sum(mfe > mae for mfe, mae in pairs) / len(pairs)
        if pairs
        else None
    )
    paired_edge = median(mfe - mae for mfe, mae in pairs) if pairs else None
    gates = policy["asymmetry"]
    minimum = policy["minimum_distinct_parents_per_passing_route"]
    checks = {
        "minimum_distinct_parents": len(parents) >= minimum,
        "common_window_asymmetry_ratio_gte_1_5": ratio is not None
        and ratio >= float(gates["common_window_asymmetry_ratio_gte"]),
        "common_window_favorable_dominance_pct_gte_60": dominance is not None
        and dominance >= float(gates["common_window_favorable_dominance_pct_gte"]),
        "common_window_median_paired_edge_pct_gt_0": paired_edge is not None
        and paired_edge > float(gates["common_window_median_paired_edge_pct_gt"]),
    }
    metrics_available = bool(pairs) and ratio is not None
    return {
        "status": (
            "PASS"
            if all(checks.values())
            else "UNAVAILABLE"
            if not metrics_available
            else "INSUFFICIENT"
            if not checks["minimum_distinct_parents"]
            else "FAIL"
        ),
        "passed": all(checks.values()),
        "distinct_parent_count": len(parents),
        "btc_parent_movement_ids": parents,
        "sum_mfe_pct": total_mfe if pairs else None,
        "sum_mae_pct": total_mae if pairs else None,
        "common_window_asymmetry_ratio": ratio,
        "common_window_asymmetry_state": (
            "FINITE"
            if ratio is not None
            else "ZERO_DENOMINATOR"
            if pairs and total_mae == 0.0
            else "DATA_MISSING"
        ),
        "common_window_favorable_dominance_pct": dominance,
        "common_window_median_paired_edge_pct": paired_edge,
        "checks": checks,
    }


def evaluate(
    exact_binding: Mapping[str, Any],
    representatives: Sequence[Mapping[str, Any]],
    *,
    selection_provenance: Mapping[str, Any] | None,
    acceptance_policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate the exact atomic research gate over frozen representatives.

    A route only sees parents with complete evidence for that route. Invalid
    parents and nondecisive labels are disclosed and excluded. Duplicate valid
    parent IDs, row-binding mismatches, an incompatible policy, or incomplete
    selection provenance are common blockers and therefore block both routes.
    """
    common_blockers: list[str] = []
    try:
        manifest = _manifest()
        contract.validate_exact_binding(exact_binding)
        identity = representative_binding(exact_binding)
    except (TypeError, ValueError, KeyError):
        manifest = {
            "labels": {
                "method_version": "ordered-first-touch-v7",
                "asymmetry_source_version": "common-window-spot-1m-v1",
                "probability_decisive_statuses": ["SUCCESS", "FAILURE"],
            }
        }
        identity = None
        common_blockers.append("EXACT_BINDING_INVALID")
    try:
        policy = validate_policy_compatibility()
    except (TypeError, ValueError, KeyError):
        policy = None
        common_blockers.append("FROZEN_ACCEPTANCE_POLICY_INCOMPATIBLE")
    if acceptance_policy is not None:
        try:
            validate_policy_compatibility(acceptance_policy)
        except (TypeError, ValueError, KeyError):
            common_blockers.append("ACCEPTANCE_POLICY_MISMATCH")
    if isinstance(representatives, (str, bytes)) or not isinstance(representatives, Sequence):
        rows: list[Any] = []
        common_blockers.append("REPRESENTATIVES_NOT_A_SEQUENCE")
    else:
        rows = list(representatives)
    selection_valid = (
        identity is not None
        and _selection_matches(selection_provenance, exact_binding, rows)
    )
    if not selection_valid:
        common_blockers.append("REGISTRY_SELECTION_ATTESTATION_INVALID")
        common_blockers.append("SELECTION_PROVENANCE_INCOMPLETE_OR_MISMATCH")

    probability_values: dict[str, bool] = {}
    asymmetry_values: dict[str, tuple[float, float]] = {}
    exclusions: list[dict[str, Any]] = []
    all_valid_parent_ids: list[str] = []
    route_exclusions = {"PROBABILITY": Counter(), "ASYMMETRY": Counter()}
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            common_blockers.append("REPRESENTATIVE_ROW_NOT_A_MAPPING")
            exclusions.append({"index": index, "btc_parent_movement_id": None,
                               "reasons": ["REPRESENTATIVE_ROW_NOT_A_MAPPING"]})
            continue
        row = raw
        if identity is None or not _row_binding_matches(row, identity):
            common_blockers.append("REPRESENTATIVE_BINDING_MISMATCH")
            exclusions.append({"index": index,
                               "btc_parent_movement_id": row.get("btc_parent_movement_id"),
                               "reasons": ["REPRESENTATIVE_BINDING_MISMATCH"]})
            continue
        parent_id, representative_sha256, parent_reasons = _valid_parent(
            row, selection_provenance if selection_valid else None,
            identity, exact_binding,
        )
        if parent_reasons:
            common_blockers.append("SHARED_REPRESENTATIVE_PROVENANCE_INVALID")
            exclusions.append({"index": index, "btc_parent_movement_id": parent_id,
                               "reasons": parent_reasons})
            continue
        assert parent_id is not None
        assert representative_sha256 is not None
        all_valid_parent_ids.append(parent_id)
        probability, probability_reasons = _probability_value(
            row, identity, parent_id, representative_sha256, manifest
        )
        if probability is not None:
            probability_values[parent_id] = probability
        else:
            route_exclusions["PROBABILITY"].update(probability_reasons)
        asymmetry, asymmetry_reasons = _asymmetry_value(
            row, identity, parent_id, representative_sha256, manifest
        )
        if asymmetry is not None:
            asymmetry_values[parent_id] = asymmetry
        else:
            route_exclusions["ASYMMETRY"].update(asymmetry_reasons)

    duplicates = sorted(
        parent_id for parent_id, count in Counter(all_valid_parent_ids).items() if count > 1
    )
    if duplicates:
        common_blockers.append("DUPLICATE_BTC_PARENT_MOVEMENT_ID")
        # The evaluator never chooses which duplicate survives.
        for parent_id in duplicates:
            probability_values.pop(parent_id, None)
            asymmetry_values.pop(parent_id, None)

    runtime_policy = policy or _expected_policy_core()
    probability_route = _probability_route(probability_values, runtime_policy)
    asymmetry_route = _asymmetry_route(asymmetry_values, runtime_policy)
    probability_route["exclusions"] = dict(sorted(route_exclusions["PROBABILITY"].items()))
    asymmetry_route["exclusions"] = dict(sorted(route_exclusions["ASYMMETRY"].items()))
    common_blockers = sorted(set(common_blockers))
    common_passed = not common_blockers
    atomic_gate_passed = common_passed and (
        probability_route["passed"] or asymmetry_route["passed"]
    )
    # This public pure layer cannot prove that caller-supplied hashes name an
    # actually persisted registry row. The read-only outcome adapter also
    # remains diagnostic; only the database trigger can establish research
    # qualification, which must then be read back from the persisted row.
    research_qualified = False
    qualification_blockers = ["DURABLE_REGISTRY_PERSISTENCE_NOT_VERIFIED"]
    if atomic_gate_passed:
        status = "AWAITING_DURABLE_REGISTRY_VERIFICATION"
    elif common_blockers or (
        probability_route["status"] == "UNAVAILABLE"
        or asymmetry_route["status"] == "UNAVAILABLE"
    ):
        status = "UNKNOWN"
    elif (
        probability_route["status"] == "INSUFFICIENT"
        and asymmetry_route["status"] == "INSUFFICIENT"
    ):
        status = "INSUFFICIENT_EVIDENCE"
    else:
        status = "BELOW_ACCEPTANCE_GATE"
    return {
        "evaluator_version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": (
            exact_binding.get("binding_sha256")
            if isinstance(exact_binding, Mapping)
            else None
        ),
        "acceptance_policy_version": policy.get("version") if policy else None,
        "acceptance_policy_sha256": (
            contract.digest(policy) if policy is not None else None
        ),
        "registry_selection_receipt": {
            key: _mapping(selection_provenance).get(key)
            for key in ("registration_evidence", "freeze_id", "frozen_at_utc",
                        "registry_record_sha256", "registry_verification_receipt_sha256",
                        "cohort_query_sha256", "population_receipt_sha256",
                        "source_high_water_attempt_id", "representative_count",
                        "representative_set_sha256", "attestation_sha256")
        } | {
            "attestation_structurally_valid": selection_valid,
            "persistence_verified_by_evaluator": False,
            "verification_boundary": "REGISTRY_ADAPTER_MUST_VERIFY_DURABLE_RECORD_AND_RECEIPT",
        },
        "atomic_expression": runtime_policy["atomic_expression"],
        "common": {
            "passed": common_passed,
            "blockers": common_blockers,
            "selection_provenance_complete": (
                selection_valid
            ),
            "duplicate_btc_parent_movement_ids": duplicates,
        },
        "routes": {
            "PROBABILITY": probability_route,
            "ASYMMETRY": asymmetry_route,
        },
        "atomic_gate_passed": atomic_gate_passed,
        "structurally_eligible": atomic_gate_passed,
        "research_qualified": research_qualified,
        "qualification_blockers": qualification_blockers,
        "status": status,
        "maximum_result": runtime_policy["maximum_result"],
        "excluded_representatives": exclusions,
        "representative_rows_received": len(rows),
        "delivery_status_required": False,
        "fresh_three_parent_route": runtime_policy["fresh_three_parent_route"],
        "live_effect": "NONE",
        "trade_execution_effect": "NONE",
    }
