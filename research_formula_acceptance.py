"""Research-only formula acceptance with probability and asymmetry paths.

This policy never approves LIVE delivery.  It classifies whether a formula is
ready to be presented later as an explicitly experimental research indication.
The immutable owner approval and Telegram delivery gates remain separate.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional

import research_mfe_mae_efficiency


POLICY_VERSION = "research-acceptance-v1-probability-or-asymmetry"


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _metric(metrics: Mapping[str, Any], name: str, default: float = 0.0) -> float:
    value = _number(metrics.get(name))
    return default if value is None else value


def _count(value: Any) -> int:
    number = _number(value)
    if number is None or number < 0.0 or not number.is_integer():
        return 0
    return int(number)


def evaluate(
    metrics: Mapping[str, Any],
    *,
    phase: str,
    minimum_matches: int,
    minimum_controls: int,
    minimum_recent_matches: int = 3,
    minimum_recent_effective_samples: float = 6.0,
    minimum_recent_control_effective_samples: float = 6.0,
    maximum_last_match_age_hours: float = 24.0 * 21.0,
    probability_q_value: Any = None,
    asymmetry_q_value: Any = None,
    require_multiple_testing: bool = False,
    mandatory_checks: Optional[Mapping[str, bool]] = None,
    early_mandatory_checks: Optional[Mapping[str, bool]] = None,
    probability_checks: Optional[Mapping[str, bool]] = None,
    asymmetry_checks: Optional[Mapping[str, bool]] = None,
) -> Dict[str, Any]:
    """Evaluate two independent research acceptance paths.

    Common evidence gates prove that the sample is usable and current.  A
    formula then qualifies through either a high-enough probability path or a
    stronger favorable/adverse movement-asymmetry path.  Tail MAE is disclosed
    but is deliberately not a hard rejection by itself.
    """

    sample_size = _count(metrics.get("sample_size"))
    controls = _count(metrics.get("control_sample_size"))
    recent = _count(metrics.get("recent_sample_size"))
    recent_effective = _metric(metrics, "recency_effective_sample_size")
    recent_control_effective = _metric(
        metrics, "recency_control_effective_sample_size"
    )
    age_hours = _metric(metrics, "last_sample_age_hours", 10**9)
    width_floor = _metric(metrics, "movement_width_floor_effective_pct")
    normalized_phase = str(phase or "").strip().upper()
    if normalized_phase not in {"HISTORICAL", "PROSPECTIVE"}:
        raise ValueError("phase must be HISTORICAL or PROSPECTIVE")
    probability_q = _number(probability_q_value)
    asymmetry_q = _number(asymmetry_q_value)

    common = {
        "independent matched market episodes": sample_size >= int(minimum_matches),
        "independent control market episodes": controls >= int(minimum_controls),
        "recent matched market episodes": recent >= int(minimum_recent_matches),
        "recent effective evidence": recent_effective
        >= float(minimum_recent_effective_samples),
        "recent effective control evidence": recent_control_effective
        >= float(minimum_recent_control_effective_samples),
        "latest match is current": age_hours <= float(maximum_last_match_age_hours),
        "current core metrics complete": False,
        "wide favorable movement floor": False,
    }
    supplied_common_checks = {
        str(name): type(passed) is bool and passed
        for name, passed in (mandatory_checks or {}).items()
    }
    collisions = sorted(set(common) & set(supplied_common_checks))
    if collisions:
        raise ValueError(
            "mandatory_checks may not override built-in gates: "
            + ", ".join(collisions)
        )
    common.update(supplied_common_checks)
    supplied_early_checks = {
        str(name): type(passed) is bool and passed
        for name, passed in (
            early_mandatory_checks
            if early_mandatory_checks is not None
            else supplied_common_checks
        ).items()
    }
    current_hit_rate = _number(metrics.get("recency_weighted_hit_rate_pct"))
    current_wilson = _number(
        metrics.get("recency_weighted_wilson_95_lower_approx_pct")
    )
    current_median_mfe = _number(
        metrics.get("recency_weighted_median_mfe_pct")
    )
    current_median_mae = _number(
        metrics.get("recency_weighted_median_mae_pct")
    )
    current_mae_p90 = _number(metrics.get("recency_weighted_mae_p90_pct"))
    current_mae_p95 = _number(metrics.get("recency_weighted_mae_p95_pct"))
    hit_rate = current_hit_rate if current_hit_rate is not None else 0.0
    wilson = current_wilson if current_wilson is not None else 0.0
    improvement_value = _number(
        metrics.get("recency_weighted_hit_rate_improvement_pct_points")
    )
    median_mfe = current_median_mfe if current_median_mfe is not None else 0.0
    median_mae = current_median_mae if current_median_mae is not None else 0.0
    common["current core metrics complete"] = all(
        value is not None
        for value in (
            current_hit_rate,
            current_wilson,
            current_median_mfe,
            current_median_mae,
        )
    )
    common["current adverse-excursion disclosure complete"] = all(
        value is not None for value in (current_mae_p90, current_mae_p95)
    )
    common["wide favorable movement floor"] = (
        median_mfe >= width_floor and width_floor > 0.0
    )
    efficiency = research_mfe_mae_efficiency.classify(median_mfe, median_mae)
    efficiency_ratio = efficiency.ratio
    if efficiency.state == research_mfe_mae_efficiency.UNBOUNDED_ZERO_MAE:
        efficiency_ratio = float("inf")
    probability_hit_floor = 60.0 if normalized_phase == "HISTORICAL" else 65.0
    probability_wilson_floor = 45.0 if normalized_phase == "HISTORICAL" else 50.0
    dominance_wilson_floor = 40.0 if normalized_phase == "HISTORICAL" else 45.0
    probability = {
        "hit rate": hit_rate >= probability_hit_floor,
        "Wilson lower bound": wilson >= probability_wilson_floor,
        "improvement over controls": (
            improvement_value is not None and improvement_value >= 5.0
        ),
        "minimum favorable/adverse efficiency": efficiency.meets_threshold(1.10),
        "probability multiple-testing correction": (
            not require_multiple_testing
            or (
                probability_q is not None
                and 0.0 <= probability_q <= 0.20
            )
        ),
    }
    current_dominance_rate = _number(
        metrics.get("recency_weighted_favorable_dominance_rate_pct")
    )
    current_dominance_wilson = _number(
        metrics.get(
            "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct"
        )
    )
    current_dominance_improvement = _number(
        metrics.get(
            "recency_weighted_favorable_dominance_improvement_pct_points"
        )
    )
    current_paired_edge = _number(
        metrics.get(
            "recency_weighted_median_paired_favorable_minus_adverse_pct"
        )
    )
    dominance_rate = (
        current_dominance_rate if current_dominance_rate is not None else 0.0
    )
    dominance_wilson = (
        current_dominance_wilson
        if current_dominance_wilson is not None
        else 0.0
    )
    dominance_improvement = (
        current_dominance_improvement
        if current_dominance_improvement is not None
        else -100.0
    )
    paired_edge = current_paired_edge if current_paired_edge is not None else 0.0
    dominance_effective = _metric(
        metrics,
        "recency_favorable_dominance_effective_sample_size",
        0.0,
    )
    control_dominance_effective = _metric(
        metrics,
        "recency_control_favorable_dominance_effective_sample_size",
        0.0,
    )
    probability["recent favorable/adverse movement evidence"] = (
        dominance_effective >= float(minimum_recent_effective_samples)
    )
    movement_balance_heuristic = (
        median_mfe * hit_rate / 100.0
        - median_mae * (100.0 - hit_rate) / 100.0
    )
    asymmetry = {
        "current asymmetry metrics complete": all(
            value is not None
            for value in (
                current_dominance_rate,
                current_dominance_wilson,
                current_dominance_improvement,
                current_paired_edge,
            )
        ),
        "minimum directional hit rate": hit_rate >= 45.0,
        "favorable dominance rate": dominance_rate >= 70.0,
        "favorable dominance Wilson lower bound": dominance_wilson
        >= dominance_wilson_floor,
        "favorable dominance improvement over controls": dominance_improvement
        >= 5.0,
        "strong favorable/adverse efficiency": efficiency.meets_threshold(2.0),
        "positive paired favorable/adverse edge": paired_edge > 0.0,
        "recent paired favorable/adverse evidence": dominance_effective
        >= float(minimum_recent_effective_samples),
        "recent paired control evidence": control_dominance_effective
        >= float(minimum_recent_control_effective_samples),
        "asymmetry multiple-testing correction": (
            not require_multiple_testing
            or (
                asymmetry_q is not None
                and 0.0 <= asymmetry_q <= 0.20
            )
        ),
    }
    for route_name, gates, supplied in (
        ("PROBABILITY", probability, probability_checks or {}),
        ("ASYMMETRY", asymmetry, asymmetry_checks or {}),
    ):
        normalized = {
            str(name): type(passed) is bool and passed
            for name, passed in supplied.items()
        }
        collisions = sorted(set(gates) & set(normalized))
        if collisions:
            raise ValueError(
                f"{route_name.lower()}_checks may not override built-in gates: "
                + ", ".join(collisions)
            )
        gates.update(normalized)

    common_passed = all(common.values())
    probability_passed = common_passed and all(probability.values())
    asymmetry_passed = common_passed and all(asymmetry.values())
    accepted_paths = [
        name
        for name, passed in (
            ("PROBABILITY", probability_passed),
            ("ASYMMETRY", asymmetry_passed),
        )
        if passed
    ]
    missing_by_path = {
        "COMMON": [name for name, passed in common.items() if not passed],
        "PROBABILITY": [name for name, passed in probability.items() if not passed],
        "ASYMMETRY": [name for name, passed in asymmetry.items() if not passed],
    }
    early_probability_signal = bool(
        hit_rate >= probability_hit_floor
        and improvement_value is not None
        and improvement_value >= 5.0
        and efficiency.meets_threshold(1.10)
        and dominance_effective >= 3.0
    )
    early_asymmetry_signal = bool(
        hit_rate >= 45.0
        and dominance_rate >= 70.0
        and dominance_improvement >= 5.0
        and efficiency.meets_threshold(2.0)
        and paired_edge > 0.0
        and dominance_effective >= 3.0
        and control_dominance_effective >= 3.0
    )
    early_current_paths = [
        name
        for name, passed in (
            ("PROBABILITY", early_probability_signal),
            ("ASYMMETRY", early_asymmetry_signal),
        )
        if passed
    ]
    early_current_edge = bool(
        not accepted_paths
        and recent >= 3
        and 3.0 <= recent_effective < float(minimum_recent_effective_samples)
        and recent_control_effective >= 3.0
        and age_hours <= float(maximum_last_match_age_hours)
        and width_floor > 0.0
        and median_mfe >= width_floor
        and early_current_paths
        and all(supplied_early_checks.values())
    )
    if accepted_paths:
        maturity = "RESEARCH_READY"
    elif early_current_edge:
        maturity = "EARLY_CURRENT_EDGE"
    elif sample_size < int(minimum_matches) or controls < int(minimum_controls):
        maturity = "ACCUMULATING_EVIDENCE"
    elif (
        recent < int(minimum_recent_matches)
        or recent_effective < float(minimum_recent_effective_samples)
        or recent_control_effective
        < float(minimum_recent_control_effective_samples)
        or age_hours > maximum_last_match_age_hours
    ):
        maturity = "STALE_OR_NOT_RECENT"
    else:
        maturity = "EVIDENCE_PRESENT_EDGE_NOT_ESTABLISHED"

    tail_exceeds_favorable = bool(
        current_mae_p90 is not None
        and median_mfe > 0.0
        and current_mae_p90 > median_mfe
    )
    return {
        "policy_version": POLICY_VERSION,
        "research_ready": bool(accepted_paths),
        "accepted_paths": accepted_paths,
        "early_current_paths": early_current_paths,
        "early_integrity_gates": supplied_early_checks,
        "maturity": maturity,
        "common_gates": common,
        "paths": {
            "PROBABILITY": {"passed": probability_passed, "gates": probability},
            "ASYMMETRY": {"passed": asymmetry_passed, "gates": asymmetry},
        },
        "missing_by_path": missing_by_path,
        "metrics": {
            "probability_weighted_movement_balance_pct": round(
                movement_balance_heuristic, 6
            ),
            "median_mfe_mae_ratio": (
                None if efficiency_ratio in (None, float("inf")) else efficiency_ratio
            ),
            "median_mfe_mae_ratio_state": efficiency.state,
            "favorable_dominance_rate_pct": dominance_rate,
            "favorable_dominance_wilson_95_lower_pct": dominance_wilson,
            "favorable_dominance_improvement_pct_points": dominance_improvement,
            "median_paired_favorable_minus_adverse_pct": paired_edge,
            "current_mae_p90_pct": current_mae_p90,
            "current_mae_p95_pct": current_mae_p95,
            "adverse_excursion_disclosure_complete": (
                current_mae_p90 is not None and current_mae_p95 is not None
            ),
            "p90_adverse_exceeds_median_favorable": tail_exceeds_favorable,
        },
        "tail_risk_treatment": (
            "p90 adverse movement is a mandatory disclosure, not a standalone "
            "research rejection"
        ),
        "movement_balance_treatment": (
            "descriptive ranking heuristic from recency-weighted MFE/MAE and "
            "hit rate; it is not an expected trade return"
        ),
        "live_effect": "NONE; explicit owner approval and LIVE gates are unchanged",
    }
