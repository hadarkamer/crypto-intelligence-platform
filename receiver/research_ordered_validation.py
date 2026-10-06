"""Frozen, prospective ordered-v7 validation; no implicit acceptance thresholds.

Discovery and later complete BTC parent movements never share evidence.  A
registration is immutable; changing any criterion requires a new registration
and a new real freeze time. This module is research-only and sends no alerts.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
import math
from statistics import median
from typing import Any, Mapping, Sequence

import research_formula_ordered_v7 as evidence
import canonical_price_path

VERSION = "ordered-v7-prospective-freeze-v1"
COMMON_WINDOW_VERSION = "common-window-spot-1m-v1"
COMMON_WINDOW_METRIC = "RATIO_SUM_MFE_TO_SUM_MAE_FULL_COMMON_WINDOW"
METRICS = {
    "hit_rate_pct", "wilson_95_lower_pct", "common_window_asymmetry_ratio",
    "common_window_median_mfe_pct", "common_window_median_mae_pct",
    "common_window_favorable_dominance_pct", "common_window_median_paired_edge_pct",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str,
                      ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def binding(scope: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Exact compatibility boundary; never inherit legacy acceptance by name."""
    conditions = evidence._conditions(candidate)
    repeat = candidate.get("repeat_count", 1)
    if type(repeat) is not int or repeat != 1:
        raise ValueError("validation v1 needs already registered single-completion candidates")
    if scope.get("direction") not in evidence.DIRECTIONS:
        raise ValueError("one explicit research direction is required")
    if type(scope.get("threshold_bps")) is not int or scope["threshold_bps"] not in evidence.THRESHOLDS_BPS:
        raise ValueError("unsupported threshold")
    if type(scope.get("window_minutes")) is not int or scope["window_minutes"] not in evidence.HORIZONS_MINUTES:
        raise ValueError("unsupported window")
    required = ("scope_key", "candidate_key", "symbol", "period_key", "period_version",
                "source_scope", "parent_policy_version")
    if any(not isinstance(scope.get(key), str) or not scope[key] for key in required):
        raise ValueError("explicit scope, period, source and parent policy identities required")
    return {
        **{key: scope[key] for key in required},
        "conditions": conditions, "repeat_count": repeat,
        "candidate_definition": json.loads(canonical(candidate)),
        "candidate_version": candidate.get("candidate_version") or candidate.get("catalog_version"),
        "direction_mode": candidate.get("direction_mode", "NORMAL"),
        "direction": scope["direction"], "threshold_bps": scope["threshold_bps"],
        "window_minutes": scope["window_minutes"],
        "period_start_utc": evidence._utc(scope["period_start_utc"]).isoformat(),
        "outcome_method_version": evidence.METHOD_VERSION,
        "common_window_method_version": COMMON_WINDOW_VERSION,
        "common_window_metric": COMMON_WINDOW_METRIC, "validation_version": VERSION,
        "wave_policy": "WHOLE_PARENT_STARTS_STRICTLY_AFTER_FREEZE",
        "representative_policy": "EARLIEST_COMPLETION_ALL_SIMULTANEOUS_MEMBERS_REQUIRED",
        "representative_fingerprint_version": "ENTRY_AND_FROZEN_PREDICATE_FEATURES_V1",
        "fresh_policy": "ALL_WAVE_EVIDENCE_WITHIN_ROLLING_14_DAYS",
    }


def validate_acceptance(policy: Mapping[str, Any] | None,
                        exact_binding: Mapping[str, Any]) -> dict[str, Any] | None:
    """Only an explicit, documented, exactly compatible policy can activate.

    No existing v6/one-sided policy is adapted to ordered v7 by reusing a few
    favorable numeric thresholds. Missing configuration is a named blocker.
    """
    if policy is None:
        return None
    if (not isinstance(policy, Mapping) or not policy.get("policy_version")
            or not policy.get("documented_source") or not policy.get("rationale")
            or policy.get("binding_sha256") != digest(exact_binding)):
        raise ValueError("acceptance policy needs documented exact version/cell binding")
    if policy.get("combination") not in {"PROBABILITY_OR_ASYMMETRY", "PROBABILITY_AND_ASYMMETRY"}:
        raise ValueError("explicit acceptance route combination required")
    multiplicity = policy.get("multiplicity")
    if (not isinstance(multiplicity, Mapping) or not multiplicity.get("family_id")
            or multiplicity.get("method") != "PRESPECIFIED_FIXED_FAMILY_DISCLOSURE"
            or type(multiplicity.get("registered_attempts")) is not int
            or multiplicity["registered_attempts"] < 1
            or not multiplicity.get("justification")):
        raise ValueError("explicit fixed-family multiplicity accounting is required")
    if not isinstance(policy.get("routes"), Mapping):
        raise ValueError("explicit acceptance paths required")
    for route in ("PROBABILITY", "ASYMMETRY"):
        gates = policy["routes"].get(route)
        if not isinstance(gates, list) or not gates:
            raise ValueError("both acceptance paths need explicit documented metric gates")
        for gate in gates:
            if (not isinstance(gate, Mapping) or gate.get("metric") not in METRICS or gate.get("operator") not in {">=", ">", "<=", "<"}
                    or evidence._number(gate.get("value")) is None):
                raise ValueError("invalid acceptance metric gate")
        required_metric = "hit_rate_pct" if route == "PROBABILITY" else "common_window_asymmetry_ratio"
        if not any(gate["metric"] == required_metric for gate in gates):
            raise ValueError("route omits its defining metric")
    return json.loads(canonical(policy))


def freeze(scope: Mapping[str, Any], candidate: Mapping[str, Any], *, frozen_at_utc: Any,
           acceptance_policy: Mapping[str, Any] | None = None) -> dict[str, Any]:
    contract = binding(scope, candidate)
    if not contract["candidate_version"]:
        raise ValueError("candidate/catalog version required")
    policy = validate_acceptance(acceptance_policy, contract)
    identity = {"binding": contract, "acceptance_policy": policy}
    when = evidence._utc(frozen_at_utc)
    return {**identity, "definition_sha256": digest(identity),
            "frozen_at_utc": when.isoformat(),
            "freeze_id": digest([scope["scope_key"], digest(identity), when.isoformat()])}


def assert_same_registration(existing: Mapping[str, Any], scope: Mapping[str, Any],
                             candidate: Mapping[str, Any]) -> None:
    if canonical(existing["binding"]) != canonical(binding(scope, candidate)):
        raise ValueError("frozen registration changed; use a new candidate/scope version")
    validate_acceptance(existing.get("acceptance_policy"), existing["binding"])
    if existing["definition_sha256"] != digest({key: existing.get(key) for key in ("binding", "acceptance_policy")}):
        raise ValueError("frozen definition digest mismatch")


def _record_metric(record: Mapping[str, Any]) -> Mapping[str, Any]:
    # The sidecar store returns its immutable identity plus calculated details.
    details = record.get("details") or record.get("metrics") or {}
    return {**record, **details} if isinstance(details, Mapping) else record


def _full_window(row: Mapping[str, Any], record: Mapping[str, Any] | None,
                 contract: Mapping[str, Any], as_of: datetime) -> tuple[float, float] | None:
    if not record:
        return None
    item = _record_metric(record)
    try:
        start = evidence._utc(item.get("measurement_start_utc"))
        end = evidence._utc(item.get("observed_through_utc"))
        window_end = start + timedelta(minutes=contract["window_minutes"])
        closed_grid_end = (window_end + timedelta(milliseconds=1)).replace(second=0, microsecond=0)
        expected_last_close = closed_grid_end - timedelta(milliseconds=1)
        expected_first = start.replace(second=0, microsecond=0)
        if expected_first < start:
            expected_first += timedelta(minutes=1)
        expected_samples = int((closed_grid_end - expected_first).total_seconds() // 60)
        mfe, mae = evidence._number(item.get("mfe_pct")), evidence._number(item.get("mae_pct"))
        if (item.get("method_version") != COMMON_WINDOW_VERSION
                or item.get("window_minutes") != contract["window_minutes"]
                or item.get("measurement_kind") != "FIXED_WINDOW"
                or item.get("status") != "READY" or item.get("path_complete") is not True
                or item.get("observation_closed") is not True
                or start != evidence._decision_time(row)
                or evidence._utc(item.get("window_end_utc")) != window_end
                or window_end > as_of or evidence._utc(item.get("observed_at_utc")) > as_of
                or end != expected_last_close
                or evidence._utc(item.get("observed_from_utc")) != expected_first
                or item.get("path_samples") != expected_samples or item.get("candle_interval_seconds") != 60
                or end > as_of or item.get("direction") != contract["direction"]
                or mfe is None or mae is None or min(mfe, mae) < 0):
            return None
        if item.get("data_quality_status") not in evidence.VERIFIED_DATA_QUALITY:
            return None
        label = evidence._outcome(row)
        reference = evidence._number(row.get("entry_price", label.get("reference_price")))
        common_reference = evidence._number(item.get("reference_price"))
        if reference is None or common_reference is None or not math.isclose(reference, common_reference, rel_tol=1e-12):
            return None
        label_reference = evidence._number(label.get("reference_price"))
        if label_reference is not None and not math.isclose(label_reference, common_reference, rel_tol=1e-12):
            return None
        measured_id = row.get("outcome_event_id", row.get("event_id"))
        if item.get("event_id") is not None and item["event_id"] != measured_id:
            return None
        route = dict(item.get("source") or {})
        route["api_coin"] = route.get("instrument") or ""
        verified = canonical_price_path.validated_route(row.get("symbol"), route, require_complete=False)
        canonical_price_path.validated_route(item.get("symbol"), route, require_complete=False)
        if canonical_price_path.quality_status(route, complete=True) != item["data_quality_status"]:
            return None
        if route.get("symbol") != item.get("symbol"):
            return None
        if label.get("market_pair") and label["market_pair"] != verified["pair"]:
            return None
        if label.get("price_source") and f"path={verified['exchange']}_{verified['market']}:{verified['pair']}:1m" not in label["price_source"]:
            return None
    except (TypeError, ValueError, OverflowError):
        return None
    return mfe, mae


def _metrics(episodes: Sequence[Mapping[str, Any]], row_by_id: Mapping[Any, Mapping[str, Any]],
             windows: Mapping[Any, Mapping[str, Any]], contract: Mapping[str, Any],
             as_of: datetime, complete: bool) -> dict[str, Any]:
    statuses = Counter(episode["status"] for episode in episodes)
    successes, failures = statuses["SUCCESS"], statuses["FAILURE"]
    resolved = successes + failures
    full = []
    for episode in episodes:
        values = [_full_window(row_by_id[event_id], windows.get(event_id), contract, as_of)
                  for event_id in episode["event_ids"]]
        if values and all(value is not None for value in values):
            # Same conservative all-members wave aggregation as First Touch:
            # min favorable and max adverse; every status stays in this cohort.
            full.append((min(value[0] for value in values), max(value[1] for value in values)))
    window_complete = bool(episodes) and len(full) == len(episodes) and complete
    sum_mfe = sum(item[0] for item in full)
    sum_mae = sum(item[1] for item in full)
    return {
        "status_counts": {status: statuses[status] for status in evidence.WAVE_STATUSES},
        "selected_waves": len(episodes), "resolved_waves": resolved,
        "successes": successes, "failures": failures,
        "hit_rate_pct": 100 * successes / resolved if resolved and complete else None,
        "wilson_95_lower_pct": evidence._wilson(successes, resolved) if complete else None,
        "common_window_waves": len(full), "common_window_complete": window_complete,
        "common_window_asymmetry_ratio": sum_mfe / sum_mae if window_complete and sum_mae > 0 else None,
        "common_window_asymmetry_state": "FINITE" if window_complete and sum_mae > 0 else "ZERO_DENOMINATOR" if window_complete else "DATA_MISSING",
        "common_window_median_mfe_pct": median(value[0] for value in full) if window_complete else None,
        "common_window_median_mae_pct": median(value[1] for value in full) if window_complete else None,
        "common_window_favorable_dominance_pct": 100 * sum(mfe > mae for mfe, mae in full) / len(full) if window_complete else None,
        "common_window_median_paired_edge_pct": median(mfe - mae for mfe, mae in full) if window_complete else None,
        "common_window_method_version": COMMON_WINDOW_VERSION, "asymmetry_method": COMMON_WINDOW_METRIC,
        "metric_window_minutes": contract["window_minutes"], "threshold_bps": contract["threshold_bps"],
        "btc_parent_movement_ids": sorted(episode["btc_parent_movement_id"] for episode in episodes),
        "source_coverage_complete": complete,
    }


def _accept(metrics: Mapping[str, Any], policy: Mapping[str, Any] | None,
            *, minimum: int, complete: bool, attempts: int) -> dict[str, Any]:
    blockers = []
    if not complete:
        blockers.append("INCOMPLETE_DECISION_POPULATION")
    if metrics["resolved_waves"] < minimum:
        blockers.append("INSUFFICIENT_INDEPENDENT_FUTURE_WAVES")
    if policy is None:
        blockers.append("MISSING_EXACT_ORDERED_V7_ACCEPTANCE_CONTRACT")
    elif attempts > policy["multiplicity"]["registered_attempts"]:
        blockers.append("MULTIPLICITY_FAMILY_EXPANDED_AFTER_FREEZE")
    paths = {}
    for route in ("PROBABILITY", "ASYMMETRY"):
        gates = []
        for gate in (policy or {}).get("routes", {}).get(route, []):
            actual = evidence._number(metrics.get(gate["metric"]))
            target = float(gate["value"])
            passed = False if actual is None else {">=": actual >= target, ">": actual > target,
                                                   "<=": actual <= target, "<": actual < target}[gate["operator"]]
            gates.append({**gate, "actual": actual, "passed": passed})
        paths[route] = {"metric_gates": gates, "metrics_pass": bool(gates) and all(gate["passed"] for gate in gates),
                        "metric_blockers": ["MISSING_METRIC:" + gate["metric"] for gate in gates if gate["actual"] is None]}
    p, a = (paths[key]["metrics_pass"] for key in ("PROBABILITY", "ASYMMETRY"))
    combined = (p and a) if (policy or {}).get("combination") == "PROBABILITY_AND_ASYMMETRY" else (p or a)
    return {"sample_minimum": minimum, "sample_count_eligible": complete and metrics["resolved_waves"] >= minimum,
            "paths": paths, "both_metrics_pass": p and a,
            "research_ready": not blockers and combined,
            "blockers": blockers, "acceptance_policy_version": (policy or {}).get("policy_version")}


def evaluate(registration: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], *,
             analysis_as_of_utc: Any, source_coverage_complete: bool,
             common_window_rows: Mapping[Any, Mapping[str, Any]] | None = None,
             registered_attempts: int = 1, truncated: bool = False,
             representative_conflicts: Sequence[str] = ()) -> dict[str, Any]:
    """Re-evaluate mutable labels against an immutable registration and entries."""
    as_of = evidence._utc(analysis_as_of_utc)
    frozen = evidence._utc(registration["frozen_at_utc"])
    if as_of < frozen or type(source_coverage_complete) is not bool:
        raise ValueError("invalid evaluation clock or population coverage")
    if type(registered_attempts) is not int or registered_attempts < 1:
        raise ValueError("positive actual tested-family count required")
    contract = registration["binding"]
    policy = validate_acceptance(registration.get("acceptance_policy"), contract)
    rejected = Counter()
    partitions = {"DISCOVERY": [], "PROSPECTIVE": []}
    for raw in rows:
        row = dict(raw)
        label = evidence._outcome(row)
        if (row.get("source_scope") != contract["source_scope"]
                or row.get("period_key") != contract["period_key"]
                or row.get("period_version") != contract["period_version"]):
            rejected["SOURCE_OR_PERIOD_MISMATCH"] += 1
            continue
        if (row.get("direction") != contract["direction"]
                or row.get("episode_policy_version") != contract["parent_policy_version"]
                or label.get("window_minutes") != contract["window_minutes"]
                or label.get("threshold_bps") != contract["threshold_bps"]
                or (contract["symbol"] != "ALL" and row.get("symbol") != contract["symbol"])):
            rejected["FROZEN_CELL_MISMATCH"] += 1
            continue
        try:
            start = evidence._utc(row.get("parent_start_time_utc"))
            decision = evidence._decision_time(row)
            if start < evidence._utc(contract["period_start_utc"]):
                rejected["WHOLE_WAVE_CROSSES_PERIOD_BOUNDARY"] += 1
                continue
            if start > decision:
                raise ValueError("parent begins after event")
        except (TypeError, ValueError, OverflowError):
            rejected["MISSING_OR_INVALID_PARENT_START"] += 1
            continue
        if not evidence._matches(row.get("decision_features") or {}, contract["conditions"]):
            rejected["FROZEN_CONDITIONS_NOT_MET"] += 1
            continue
        row["_phase"] = "PROSPECTIVE" if start > frozen else "DISCOVERY"
        expected_label_event_id = row.get("outcome_event_id", row.get("event_id"))
        label_identity_invalid = label.get("event_id") != expected_label_event_id
        if row.get("btc_parent_movement_id") in representative_conflicts or label_identity_invalid:
            row["ordered_outcome"] = {"window_minutes": contract["window_minutes"], "threshold_bps": contract["threshold_bps"]}
        partitions[row["_phase"]].append(row)
    complete = source_coverage_complete and not truncated and not representative_conflicts
    # Unexpected input rejection means the caller did not provide its promised
    # complete exact universe. Explicitly excluded period-boundary waves alone
    # do not invalidate the intended period cohort.
    if any(value for key, value in rejected.items() if key != "WHOLE_WAVE_CROSSES_PERIOD_BOUNDARY"):
        complete = False
    summary = {}
    all_episodes = []
    phase_completeness = []
    for phase, selected in partitions.items():
        audited = evidence.summarize_scope(selected, analysis_as_of_utc=as_of,
                                          source_coverage_complete=complete, truncated=truncated)
        episodes = audited["episodes"]
        row_by_id = {row["event_id"]: row for row in selected}
        parent_starts = {row["btc_parent_movement_id"]: evidence._utc(row["parent_start_time_utc"]) for row in selected}
        phase_complete = complete and audited["source_coverage_complete"]
        phase_completeness.append(phase_complete)
        for item in episodes:
            item["phase"] = phase
            item["parent_start_time_utc"] = parent_starts[item["btc_parent_movement_id"]].isoformat()
        fresh = [item for item in episodes if parent_starts[item["btc_parent_movement_id"]] >= as_of - timedelta(days=14)]
        summary[phase] = _metrics(episodes, row_by_id, common_window_rows or {}, contract, as_of, phase_complete)
        summary[phase]["fresh"] = _metrics(fresh, row_by_id, common_window_rows or {}, contract, as_of, phase_complete)
        all_episodes.extend(episodes)
    complete = complete and all(phase_completeness)
    future = summary["PROSPECTIVE"]
    all_period_metrics = _metrics(all_episodes, {row["event_id"]: row for selected in partitions.values() for row in selected},
                                  common_window_rows or {}, contract, as_of, complete)
    all_period_metrics["fresh"] = _metrics(
        [item for item in all_episodes if evidence._utc(item["parent_start_time_utc"]) >= as_of - timedelta(days=14)],
        {row["event_id"]: row for selected in partitions.values() for row in selected},
        common_window_rows or {}, contract, as_of, complete)
    standard = _accept(future, policy, minimum=5, complete=complete, attempts=registered_attempts)
    fresh = _accept(future["fresh"], policy, minimum=3, complete=complete, attempts=registered_attempts)
    ready = standard["research_ready"] or fresh["research_ready"]
    return {"validation_version": VERSION, "freeze_id": registration["freeze_id"],
            "frozen_at_utc": registration["frozen_at_utc"], "definition_sha256": registration["definition_sha256"],
            "discovery": summary["DISCOVERY"], "prospective": future,
            "all_period_metrics": all_period_metrics,
            "standard": standard, "fresh": {**fresh, "experimental_early_only": True, "window_days": 14},
            "research_ready": ready,
            "validation_status": "RELEVANT_RESEARCH" if standard["research_ready"] else "FRESH_EARLY_EXPERIMENT" if fresh["research_ready"] else "VALIDATING_PROSPECTIVE",
            "registered_attempts": registered_attempts, "multiplicity_note": "Overlapping formulas and periods are not independent validation; no probability multiplication.",
            "excluded_rows": dict(rejected), "representative_conflicts": sorted(representative_conflicts),
            "source_coverage_complete": complete, "episodes": all_episodes,
            "live_effect": "NONE"}
