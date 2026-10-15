"""One atomic >=5-parent AND (probability OR compatible asymmetry) gate.

This is a descriptive research heuristic, not statistical independence proof,
profitability validation, prospective validation, or authorization to deliver.
The earlier common-window asymmetry is deliberately not relabeled here.
"""
from __future__ import annotations

from collections import Counter
import math
from typing import Any, Mapping, Sequence

import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch

VERSION = "no-horizon-five-parent-probability-or-asymmetry-v1"
POLICY_VERSION = "no-horizon-descriptive-p70-wilson40-v1"


def make_policy(*, policy_version: str = POLICY_VERSION, minimum_waves: int = 5,
                hit_rate_min_pct: float = 70, wilson_lower_min_pct: float = 40) -> dict[str, Any]:
    """Changed numeric heuristics require a new caller-declared policy version."""
    if not isinstance(policy_version, str) or not policy_version.strip():
        raise ValueError("explicit policy_version required")
    p = contracts.number(hit_rate_min_pct, "hit_rate_min_pct")
    w = contracts.number(wilson_lower_min_pct, "wilson_lower_min_pct")
    if type(minimum_waves) is not int or minimum_waves < 5 or not 0 <= p <= 100 or not 0 <= w <= 100:
        raise ValueError("minimum must be >=5 and percentage gates in [0,100]")
    if policy_version == POLICY_VERSION and (minimum_waves, p, w) != (5, 70, 40):
        raise ValueError("changed thresholds require a new policy_version")
    return {"policy_version": policy_version, "minimum_waves": minimum_waves,
        "combination": "PROBABILITY_OR_ASYMMETRY", "hit_rate_min_pct": p,
        "wilson_lower_min_pct": w, "statistical_claim": "DESCRIPTIVE_RESEARCH_HEURISTIC",
        "asymmetry_definition": None}


def _wilson(successes: int, n: int) -> float | None:
    if not n:
        return None
    z = 1.959963984540054
    p = successes / n
    return 100 * (p + z*z/(2*n) - z*math.sqrt(p*(1-p)/n + z*z/(4*n*n))) / (1+z*z/n)


def evaluate_gate(rows: Sequence[Mapping[str, Any]], *, as_of_utc: Any,
                  source_coverage_complete: bool, policy: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Select earliest (decision time, entry_id) per parent before reading labels.

    Rows contain contract, outcome (checkpoint or None), btc_parent_movement_id,
    membership_status='LIVE', parent_evidence_eligible=True, parent_start_time_utc,
    parent_confirmed_at_utc and features_observed_at_utc. All metadata must have
    been knowable by the signal decision. 'LIVE' denotes the causal membership
    policy; it does not itself turn replay data into prospective evidence.
    Missing outcomes retain their earliest representative and are never replaced
    by a later winner. Exact duplicate entries collapse; conflicts block the gate.
    """
    if type(source_coverage_complete) is not bool:
        raise ValueError("source_coverage_complete must be explicit boolean")
    if len(rows) > 100000:
        raise ValueError("bounded gate row budget exceeded")
    as_of = contracts.utc(as_of_utc)
    p = make_policy() if policy is None else dict(policy)
    try:
        expected = make_policy(**{key: p[key] for key in (
            "policy_version", "minimum_waves", "hit_rate_min_pct", "wilson_lower_min_pct")})
        if contracts.canonical(p) != contracts.canonical(expected):
            raise ValueError("unsupported or altered gate policy")
    except (KeyError, TypeError) as exc:
        raise ValueError("incomplete gate policy") from exc
    blockers = [] if source_coverage_complete else ["INCOMPLETE_DECISION_POPULATION"]
    groups: dict[str, list[tuple[Any, str, Mapping[str, Any], dict[str, Any]]]] = {}
    scope = None
    seen = {}
    for row in rows:
        try:
            c = contracts.validate_contract(row["contract"])
            identity = contracts.scope_identity(c)
            if scope is None:
                scope = identity
            if identity != scope:
                blockers.append("HETEROGENEOUS_CANDIDATE_COHORT_OR_SOURCE")
            parent = row.get("btc_parent_movement_id")
            if not isinstance(parent, str) or not parent or parent != parent.strip():
                raise ValueError("unassignable BTC parent")
            signature = contracts.canonical(row)
            if c["entry_id"] in seen:
                if seen[c["entry_id"]] != signature:
                    blockers.append("CONFLICTING_DUPLICATE_ENTRY")
                continue
            seen[c["entry_id"]] = signature
            # Only contract time and immutable entry ID influence selection.
            groups.setdefault(parent, []).append((contracts.utc(c["decision_time_utc"]), c["entry_id"], row, c))
        except (KeyError, TypeError, ValueError):
            blockers.append("INVALID_OR_UNASSIGNABLE_DECISION_ROW")
    representatives = []
    for parent, members in sorted(groups.items()):
        decision, entry_id, row, c = min(members, key=lambda item: (item[0], item[1]))
        status, reason = "DATA_MISSING", None
        try:
            if (decision > as_of or contracts.utc(row["features_observed_at_utc"]) > decision
                    or row.get("membership_status") != "LIVE" or row.get("parent_evidence_eligible") is not True
                    or row.get("parent_policy_version", c["parent_policy_version"]) != c["parent_policy_version"]
                    or not contracts.utc(row["parent_start_time_utc"]) <= contracts.utc(row["parent_confirmed_at_utc"]) <= decision):
                raise ValueError("UNVERIFIED_CAUSAL_PARENT_OR_FEATURES")
            if row.get("outcome") is None:
                raise ValueError("MISSING_EARLIEST_OUTCOME")
            result = first_touch.validate_state(row["outcome"])
            if (result["contract"] != c or contracts.utc(result["cutoff_utc"]) > as_of):
                raise ValueError("OUTCOME_CONTRACT_MISMATCH_OR_FUTURE_DATA")
            status = result["status"]
            if status == "OPEN" and (result["coverage_complete_through_cutoff"] is not True
                    or contracts.utc(result["cutoff_utc"]) < as_of.replace(second=0, microsecond=0)):
                status, reason = "DATA_MISSING", "OPEN_PATH_NOT_CAUGHT_UP_TO_ANALYSIS_CUTOFF"
            if status in ("DATA_MISSING", "BLOCKED_ENTRY"):
                reason = reason or result["terminal_reason"] or "INCOMPLETE_SELECTED_OUTCOME"
        except (KeyError, TypeError, ValueError) as exc:
            reason = str(exc)
        if reason is not None:
            blockers.append("INCOMPLETE_SELECTED_OUTCOME_EVIDENCE")
        representatives.append({"btc_parent_movement_id": parent, "entry_id": entry_id,
            "decision_time_utc": decision.isoformat(), "status": status, "exclusion_reason": reason})
    counts = Counter(item["status"] for item in representatives)
    n = counts["SUCCESS"] + counts["FAILURE"]
    hit_rate = 100 * counts["SUCCESS"] / n if n else None
    lower = _wilson(counts["SUCCESS"], n)
    enough = n >= p["minimum_waves"]
    probability_passes = bool(enough and hit_rate is not None and hit_rate >= p["hit_rate_min_pct"]
                              and lower is not None and lower >= p["wilson_lower_min_pct"])
    if not enough:
        blockers.append("FEWER_THAN_REQUIRED_RESOLVED_PARENTS")
    if not probability_passes:
        blockers.append("NO_COMPATIBLE_METRIC_ROUTE_PASSES")
    return {"gate_version": VERSION, "policy": p, "scope": scope,
        "source_coverage_complete": source_coverage_complete,
        "selected_parents": len(representatives), "resolved_parents": n,
        "successes": counts["SUCCESS"], "failures": counts["FAILURE"],
        "status_counts": {key: counts[key] for key in first_touch.STATUSES},
        "probability": {"available": n > 0, "sample_size": n, "sample_minimum": p["minimum_waves"],
            "hit_rate_pct": hit_rate, "wilson_95_lower_pct": lower, "passes": probability_passes},
        "asymmetry": {"available": False, "sample_size": 0, "passes": None,
            "reason": "NO_COMPATIBLE_VERSIONED_NO_HORIZON_ASYMMETRY_DEFINITION"},
        "experimental_eligible": not blockers and probability_passes,
        "blockers": sorted(set(blockers)), "representatives": representatives,
        "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False,
        "limitations": ["BTC_PARENT_GROUPING_IS_NOT_A_STATISTICAL_INDEPENDENCE_PROOF",
            "PROVENANCE_AND_POPULATION_COMPLETENESS_REQUIRE_EXTERNAL_ATTESTATION",
            "NO_PROFITABILITY_OR_PROSPECTIVE_VALIDATION_CLAIM"]}
