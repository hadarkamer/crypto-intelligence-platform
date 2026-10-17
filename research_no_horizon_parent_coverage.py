"""Outcome-free feasibility bound for each exact frozen research scope.

Complete source decisions and causal membership can prove that too few matched
BTC parents exist. Enough parents never proves resolved outcomes, independence,
probability, asymmetry, or research qualification. No entry prices are read.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
import research_no_horizon_preflight as features
import research_no_horizon_source as source

VERSION = "no-horizon-matched-parent-coverage-v1"
REQUIRE_ALL_SCOPES = "REQUIRE_ALL_SCOPES_MATCHED_PARENT_COVERAGE_V1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}


def _policy(value):
    if value is None:
        return gate.make_policy()
    if not isinstance(value, Mapping):
        raise ValueError("COMPLETE_VERSIONED_GATE_POLICY_REQUIRED")
    try:
        expected = gate.make_policy(**{key: value[key] for key in (
            "policy_version", "minimum_waves", "hit_rate_min_pct", "wilson_lower_min_pct")})
    except (KeyError, TypeError) as exc:
        raise ValueError("COMPLETE_VERSIONED_GATE_POLICY_REQUIRED") from exc
    if contracts.canonical(value) != contracts.canonical(expected):
        raise ValueError("UNSUPPORTED_OR_ALTERED_GATE_POLICY")
    return expected


def _matched_parent(raw, record, symbol, direction, cutoff):
    decision = contracts.utc(record["usable_from_utc"])
    result = {**record, "decision_time_utc": decision.isoformat(),
        "entry_id": f"watch:{record['snapshot_set_id']}:{symbol}:{direction}",
        "btc_parent_movement_id": None, "membership_status": "BTC_DATA_MISSING",
        "parent_blockers": [], "parent_evaluation": "CAUSAL_MEMBERSHIP_ONLY"}
    parent, prior = source._parent_evidence(raw.get("btc_parent"), raw.get("btc_prior_bar"), decision)
    member = source.measurement._membership(decision, parent, prior)
    result["membership_status"] = member["membership_status"]
    try:
        if (member["membership_status"] != "LIVE" or parent is None
                or parent.get("evidence_eligible") is not True
                or member["btc_parent_movement_id"] != parent["btc_parent_movement_id"]
                or not contracts.utc(record["source_observed_at_utc"]) <= decision <= cutoff
                or not contracts.utc(parent["start_time_utc"]) <= contracts.utc(parent["confirmed_at_utc"]) <= decision):
            raise ValueError("UNVERIFIED_MATCHED_BTC_PARENT")
        result.update(btc_parent_movement_id=member["btc_parent_movement_id"],
            parent_start_time_utc=contracts.utc(parent["start_time_utc"]).isoformat(),
            parent_confirmed_at_utc=contracts.utc(parent["confirmed_at_utc"]).isoformat(),
            parent_end_time_utc=contracts.utc(parent["end_time_utc"]).isoformat() if parent.get("end_time_utc") else None,
            parent_policy_version=parent["episode_policy_version"], parent_evidence_eligible=True,
            parent_price_source=parent["price_source"], parent_direction=parent["direction"],
            parent_boundary_reason=parent["boundary_reason"])
    except (KeyError, TypeError, ValueError, OverflowError):
        result["parent_blockers"].append("UNVERIFIED_MATCHED_BTC_PARENT")
    return result


def _conflicting_groups(groups):
    conflicts = []
    keys = ("parent_start_time_utc", "parent_confirmed_at_utc", "parent_policy_version",
            "parent_price_source", "parent_direction", "parent_evidence_eligible", "parent_boundary_reason")
    for parent_id, members in sorted(groups.items()):
        signatures = {tuple(row[key] for key in keys) for row in members}
        ends = {row["parent_end_time_utc"] for row in members if row["parent_end_time_utc"] is not None}
        # An open checkpoint and its later closed version are compatible only
        # if every decision still lies before that unique closing boundary.
        if len(signatures) != 1 or len(ends) > 1 or (ends and any(
                contracts.utc(row["decision_time_utc"]) >= contracts.utc(min(ends)) for row in members)):
            conflicts.append(parent_id)
    return conflicts


def _coverage_from_preflight(frozen, receipt, *, gate_policy=None):
    """Internal composition of freshly computed feature evidence; no caller bypass.

    Public APIs compute that receipt themselves. Reusing it here avoids running
    capture validation twice during one admission. All source rows are retained.
    """
    policy = _policy(gate_policy)
    if (receipt["preflight_version"] != features.VERSION
            or receipt["source_export_sha256"] != contracts.digest(frozen)
            or receipt["receipt_sha256"] != contracts.digest({k: v for k, v in receipt.items() if k != "receipt_sha256"})):
        raise ValueError("SOURCE_PREFLIGHT_BINDING_MISMATCH")
    cutoff = contracts.utc(frozen["cutoff_utc"])
    scopes = []
    for scope in receipt["scopes"]:
        ledger, groups = [], {}
        for record in scope["decision_ledger"]:
            if record["match_status"] == "MATCH":
                row = _matched_parent(frozen["source_rows"][record["ordinal"]], record,
                    frozen["symbol"], scope["base_direction"], cutoff)
                if row["btc_parent_movement_id"] is not None:
                    groups.setdefault(row["btc_parent_movement_id"], []).append(row)
            else:
                row = {**record, "btc_parent_movement_id": None, "membership_status": None,
                    "parent_blockers": [], "parent_evaluation": "NOT_EVALUATED_NON_MATCH"
                        if record["match_status"] == "NO_MATCH" else "NOT_EVALUATED_UNKNOWN_DECISION"}
            ledger.append(row)
        conflicts = _conflicting_groups(groups)
        for row in ledger:
            if row["btc_parent_movement_id"] in conflicts:
                row["parent_blockers"].append("CONFLICTING_MATCHED_BTC_PARENT_EVIDENCE")
        parent_blockers = sorted({error for row in ledger for error in row["parent_blockers"]})
        source_complete = scope["ready_for_outcome_research"]
        parent_complete = not parent_blockers
        complete = source_complete and parent_complete
        bound = len(groups) if complete else None
        status = "BLOCKED" if not complete else "POSSIBLE" if bound >= policy["minimum_waves"] else "INSUFFICIENT"
        representatives = []
        if complete:
            for parent_id, members in sorted(groups.items()):
                first = min(members, key=lambda row: (contracts.utc(row["decision_time_utc"]), row["entry_id"]))
                representatives.append({key: first[key] for key in (
                    "btc_parent_movement_id", "snapshot_set_id", "ordinal", "entry_id", "decision_time_utc")})
        blockers = sorted(set(scope["source_blockers"] + parent_blockers))
        if status == "INSUFFICIENT":
            blockers.append("FEWER_THAN_REQUIRED_MATCHED_PARENTS")
        scopes.append({**{key: scope[key] for key in (
                "scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct", "candidate")},
            "source_counts": dict(scope["counts"]), "source_decisions_complete": source_complete,
            "parent_membership_complete": parent_complete, "matched_parent_count": len(groups),
            "verified_matched_rows": sum(len(members) for members in groups.values()),
            "unverified_matched_rows": sum(row["match_status"] == "MATCH" and row["btc_parent_movement_id"] is None for row in ledger),
            "conflicting_parent_ids": conflicts, "matched_parent_upper_bound": bound,
            "minimum_required_parents": policy["minimum_waves"], "coverage_status": status,
            "potentially_sufficient": status == "POSSIBLE", "blockers": blockers,
            "decision_ledger": ledger, "representatives": representatives,
            "representative_selection": "EARLIEST_DECISION_THEN_ENTRY_ID_WITHOUT_OUTCOMES_V1" if complete else "NOT_SELECTED_INCOMPLETE_POPULATION",
            "outcome_evaluation": "NOT_EVALUATED", "gate_evaluation": "NOT_EVALUATED", "gate": None,
            **_AUTHORITY})
    counts = Counter(scope["coverage_status"] for scope in scopes)
    all_possible = all(scope["potentially_sufficient"] for scope in scopes)
    result = {"coverage_version": VERSION, "source_export_sha256": receipt["source_export_sha256"],
        "source_canonical_bytes": receipt["source_canonical_bytes"], "symbol": receipt["symbol"],
        "source_start_utc": receipt["source_start_utc"], "source_end_utc": receipt["source_end_utc"],
        "cutoff_utc": receipt["cutoff_utc"], "source_preflight": receipt,
        "source_preflight_receipt_sha256": receipt["receipt_sha256"], "gate_policy": policy,
        "parent_policy_version": source.btc.POLICY_VERSION, "scopes": scopes,
        "coverage_status_counts": {status: counts[status] for status in ("BLOCKED", "INSUFFICIENT", "POSSIBLE")},
        "all_scopes_potentially_sufficient": all_possible,
        "any_scope_potentially_sufficient": any(scope["potentially_sufficient"] for scope in scopes),
        "ready_for_outcome_research": all_possible, "source_decisions_complete": receipt["ready_for_outcome_research"],
        "entry_evaluation": "NOT_EVALUATED", "parent_evaluation": "CAUSAL_MEMBERSHIP_ONLY",
        "outcome_evaluation": "NOT_EVALUATED", "gate_evaluation": "NOT_EVALUATED",
        "outcomes_evaluated": False, "gate": None, "scope_pooling": False,
        "research_classification": "RETROSPECTIVE_MATCHED_PARENT_COVERAGE_FEASIBILITY",
        "is_prospective_formula_evidence": False, "db_origin_authenticated_by_this_tool": False,
        "limitations": ["MATCHED_PARENT_BOUND_IS_NOT_RESOLVED_OUTCOME_COUNT",
            "BTC_PARENT_GROUPING_IS_NOT_A_STATISTICAL_INDEPENDENCE_PROOF",
            "POSSIBLE_COVERAGE_DOES_NOT_EVALUATE_PROBABILITY_ASYMMETRY_OR_GATE",
            "ENTRY_PRICE_AND_OUTCOME_PATH_COVERAGE_NOT_EVALUATED"], **_AUTHORITY}
    return {**result, "receipt_sha256": contracts.digest(result)}


def preflight_parent_coverage(export: Mapping[str, Any], scopes, *, gate_policy=None) -> dict[str, Any]:
    """Revalidate a complete population and bound matched parents before outcomes.

    A null bound denotes missing/conflicting evidence, never zero opportunities.
    Standalone custom policies must be complete and versioned; experiment
    admission uses its unchanged child gate policy. No supplied receipt is trusted.
    """
    from research_no_horizon_experiment import _admit_export
    policy = _policy(gate_policy)
    frozen, _ = _admit_export(export)
    receipt = features.preflight_source_features(frozen, scopes)
    return _coverage_from_preflight(frozen, receipt, gate_policy=policy)
