"""Deterministic checks for the two-path rolling research contract."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import math

import research_formula_acceptance as acceptance
import research_formula_engine as engine


def _metrics() -> dict:
    return {
        "sample_size": 12,
        "control_sample_size": 12,
        "recent_sample_size": 6,
        "recent_control_sample_size": 6,
        "recency_effective_sample_size": 6.0,
        "recency_control_effective_sample_size": 6.0,
        "last_sample_age_hours": 1.0,
        "movement_width_floor_effective_pct": 0.5,
        "recency_weighted_hit_rate_pct": 70.0,
        "recency_weighted_wilson_95_lower_approx_pct": 55.0,
        "recency_weighted_hit_rate_improvement_pct_points": 12.0,
        "recency_weighted_median_mfe_pct": 2.0,
        "recency_weighted_median_mae_pct": 0.8,
        "recency_weighted_mae_p90_pct": 3.0,
        "recency_weighted_mae_p95_pct": 4.0,
        "recency_weighted_favorable_dominance_rate_pct": 60.0,
        "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct": 35.0,
        "recency_weighted_favorable_dominance_improvement_pct_points": 2.0,
        "recency_weighted_median_paired_favorable_minus_adverse_pct": 1.2,
        "recency_favorable_dominance_effective_sample_size": 6.0,
        "recency_control_favorable_dominance_effective_sample_size": 6.0,
        "median_mfe_pct": 2.0,
        "median_mae_pct": 0.8,
        "mae_p90_pct": 3.0,
    }


def _evaluate(metrics: dict, **kwargs) -> dict:
    return acceptance.evaluate(
        metrics,
        phase=kwargs.pop("phase", "PROSPECTIVE"),
        minimum_matches=kwargs.pop("minimum_matches", 12),
        minimum_controls=kwargs.pop("minimum_controls", 12),
        minimum_recent_matches=kwargs.pop("minimum_recent_matches", 3),
        minimum_recent_effective_samples=kwargs.pop(
            "minimum_recent_effective_samples", 6.0
        ),
        minimum_recent_control_effective_samples=kwargs.pop(
            "minimum_recent_control_effective_samples", 6.0
        ),
        **kwargs,
    )


def _asymmetry_only() -> dict:
    metrics = _metrics()
    metrics.update(
        {
            "recency_weighted_hit_rate_pct": 50.0,
            "recency_weighted_wilson_95_lower_approx_pct": 30.0,
            "recency_weighted_hit_rate_improvement_pct_points": 2.0,
            "recency_weighted_median_mfe_pct": 3.0,
            "recency_weighted_median_mae_pct": 1.0,
            "recency_weighted_favorable_dominance_rate_pct": 80.0,
            "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct": 50.0,
            "recency_weighted_favorable_dominance_improvement_pct_points": 15.0,
            "recency_weighted_median_paired_favorable_minus_adverse_pct": 2.0,
        }
    )
    return metrics


def _row(event_id: int, at: datetime, *, success: bool, active: float = 1.0) -> dict:
    return {
        "event": {
            "event_id": event_id,
            "alert_time_utc": at,
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "ROLLING_ACCEPTANCE_SELFTEST",
            "current_price": 100.0,
        },
        "outcome_label": {
            "horizon_minutes": 60,
            "path_success": success,
            "first_touch_status": "HIT" if success else "MISS",
            "mfe_pct": 2.0 if success else 0.3,
            "mae_pct": 0.3 if success else 2.0,
            "session_active_ratio": active,
            "session_weekend_ratio": 1.0 - active,
            "movement_width_reference": {"floor_scale_factor": 1.0},
        },
    }


def run() -> None:
    probability = _evaluate(_metrics())
    assert probability["research_ready"] is True
    assert probability["accepted_paths"] == ["PROBABILITY"]
    assert probability["metrics"]["p90_adverse_exceeds_median_favorable"] is True
    assert "not a standalone" in probability["tail_risk_treatment"]

    asymmetry = _evaluate(_asymmetry_only())
    assert asymmetry["research_ready"] is True
    assert asymmetry["accepted_paths"] == ["ASYMMETRY"]

    both_metrics = _metrics()
    both_metrics.update(
        {
            "recency_weighted_favorable_dominance_rate_pct": 80.0,
            "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct": 50.0,
            "recency_weighted_favorable_dominance_improvement_pct_points": 15.0,
        }
    )
    assert _evaluate(both_metrics)["accepted_paths"] == [
        "PROBABILITY",
        "ASYMMETRY",
    ]
    neither = _metrics()
    neither.update(
        {
            "recency_weighted_hit_rate_pct": 40.0,
            "recency_weighted_wilson_95_lower_approx_pct": 20.0,
            "recency_weighted_hit_rate_improvement_pct_points": 0.0,
        }
    )
    assert _evaluate(neither)["research_ready"] is False

    probability_q = _evaluate(
        _metrics(),
        phase="HISTORICAL",
        require_multiple_testing=True,
        probability_q_value=0.10,
        asymmetry_q_value=0.90,
    )
    assert probability_q["accepted_paths"] == ["PROBABILITY"]
    asymmetry_q = _evaluate(
        _asymmetry_only(),
        phase="HISTORICAL",
        require_multiple_testing=True,
        probability_q_value=0.90,
        asymmetry_q_value=0.10,
    )
    assert asymmetry_q["accepted_paths"] == ["ASYMMETRY"]
    assert not _evaluate(
        _asymmetry_only(),
        phase="HISTORICAL",
        require_multiple_testing=True,
        probability_q_value=0.10,
        asymmetry_q_value=0.90,
    )["research_ready"]
    for invalid_q in (-0.01, 1.01, float("nan")):
        assert not _evaluate(
            _metrics(),
            phase="HISTORICAL",
            require_multiple_testing=True,
            probability_q_value=invalid_q,
            asymmetry_q_value=invalid_q,
        )["research_ready"]

    historical_edge = _metrics()
    historical_edge.update(
        {
            "recency_weighted_hit_rate_pct": 60.0,
            "recency_weighted_wilson_95_lower_approx_pct": 45.0,
            "recency_weighted_hit_rate_improvement_pct_points": 5.0,
            "recency_weighted_median_mfe_pct": 1.1,
            "recency_weighted_median_mae_pct": 1.0,
        }
    )
    historical_edge_result = _evaluate(
        historical_edge,
        phase="HISTORICAL",
        require_multiple_testing=True,
        probability_q_value=0.20,
    )
    assert historical_edge_result["accepted_paths"] == ["PROBABILITY"]
    for key, below in (
        ("recency_weighted_hit_rate_pct", 59.999),
        ("recency_weighted_wilson_95_lower_approx_pct", 44.999),
        ("recency_weighted_hit_rate_improvement_pct_points", 4.999),
    ):
        just_below = deepcopy(historical_edge)
        just_below[key] = below
        assert "PROBABILITY" not in _evaluate(
            just_below,
            phase="HISTORICAL",
            require_multiple_testing=True,
            probability_q_value=0.20,
        )["accepted_paths"]
    assert "PROBABILITY" not in _evaluate(
        historical_edge,
        phase="HISTORICAL",
        require_multiple_testing=True,
        probability_q_value=0.200001,
    )["accepted_paths"]

    prospective_edge = deepcopy(historical_edge)
    prospective_edge.update(
        {
            "recency_weighted_hit_rate_pct": 65.0,
            "recency_weighted_wilson_95_lower_approx_pct": 50.0,
        }
    )
    assert "PROBABILITY" in _evaluate(prospective_edge)["accepted_paths"]
    prospective_edge["recency_weighted_hit_rate_pct"] = 64.999
    assert "PROBABILITY" not in _evaluate(prospective_edge)["accepted_paths"]

    asymmetry_edge = _asymmetry_only()
    asymmetry_edge.update(
        {
            "recency_weighted_hit_rate_pct": 45.0,
            "recency_weighted_favorable_dominance_rate_pct": 70.0,
            "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct": 45.0,
            "recency_weighted_favorable_dominance_improvement_pct_points": 5.0,
            "recency_weighted_median_mfe_pct": 2.0,
            "recency_weighted_median_mae_pct": 1.0,
            "recency_weighted_median_paired_favorable_minus_adverse_pct": 0.001,
        }
    )
    assert "ASYMMETRY" in _evaluate(
        asymmetry_edge,
        require_multiple_testing=True,
        asymmetry_q_value=0.20,
    )["accepted_paths"]
    asymmetry_zero_edge = deepcopy(asymmetry_edge)
    asymmetry_zero_edge[
        "recency_weighted_median_paired_favorable_minus_adverse_pct"
    ] = 0.0
    assert "ASYMMETRY" not in _evaluate(
        asymmetry_zero_edge,
        require_multiple_testing=True,
        asymmetry_q_value=0.20,
    )["accepted_paths"]
    assert "ASYMMETRY" not in _evaluate(
        asymmetry_edge,
        require_multiple_testing=True,
        asymmetry_q_value=0.200001,
    )["accepted_paths"]

    blocked = _evaluate(
        both_metrics,
        mandatory_checks={"complete provenance": False},
        early_mandatory_checks={"complete provenance": False},
    )
    assert blocked["research_ready"] is False
    try:
        _evaluate(
            both_metrics,
            mandatory_checks={"independent matched market episodes": True},
        )
    except ValueError as exc:
        assert "override" in str(exc)
    else:
        raise AssertionError("caller overrode a built-in acceptance gate")
    try:
        _evaluate(
            both_metrics,
            probability_checks={"hit rate": True},
        )
    except ValueError as exc:
        assert "override" in str(exc)
    else:
        raise AssertionError("caller overrode a built-in route gate")
    assert "PROBABILITY" not in _evaluate(
        _metrics(), probability_checks={"external quality check": 1}
    )["accepted_paths"]

    for key in (
        "sample_size",
        "control_sample_size",
        "recent_sample_size",
        "recency_effective_sample_size",
        "recency_control_effective_sample_size",
        "last_sample_age_hours",
        "movement_width_floor_effective_pct",
        "recency_weighted_hit_rate_pct",
        "recency_weighted_wilson_95_lower_approx_pct",
        "recency_weighted_median_mfe_pct",
        "recency_weighted_median_mae_pct",
        "recency_weighted_mae_p90_pct",
        "recency_weighted_mae_p95_pct",
    ):
        for invalid in (float("nan"), float("inf"), float("-inf")):
            invalid_metrics = deepcopy(both_metrics)
            invalid_metrics[key] = invalid
            assert not _evaluate(invalid_metrics)["research_ready"], (
                key,
                invalid,
            )

    for key in (
        "recency_weighted_hit_rate_pct",
        "recency_weighted_wilson_95_lower_approx_pct",
        "recency_weighted_median_mfe_pct",
        "recency_weighted_median_mae_pct",
        "recency_weighted_mae_p90_pct",
        "recency_weighted_mae_p95_pct",
    ):
        incomplete = deepcopy(both_metrics)
        incomplete.pop(key)
        assert not _evaluate(incomplete)["research_ready"], key
    probability_incomplete = deepcopy(_metrics())
    probability_incomplete.pop(
        "recency_weighted_hit_rate_improvement_pct_points"
    )
    probability_result = _evaluate(probability_incomplete)
    assert "PROBABILITY" not in probability_result["accepted_paths"]
    assert "ASYMMETRY" not in probability_result["accepted_paths"]
    asymmetry_incomplete = deepcopy(_asymmetry_only())
    asymmetry_incomplete.pop(
        "recency_weighted_favorable_dominance_improvement_pct_points"
    )
    asymmetry_result = _evaluate(asymmetry_incomplete)
    assert "ASYMMETRY" not in asymmetry_result["accepted_paths"]

    missing_tail = deepcopy(_metrics())
    missing_tail.pop("recency_weighted_mae_p90_pct")
    missing_tail.pop("mae_p90_pct")
    missing_tail_result = _evaluate(missing_tail)
    assert missing_tail_result["research_ready"] is False
    assert (
        missing_tail_result["metrics"]["adverse_excursion_disclosure_complete"]
        is False
    )
    assert (
        missing_tail_result["metrics"]["p90_adverse_exceeds_median_favorable"]
        is False
    )
    thin_movement = deepcopy(both_metrics)
    thin_movement["recency_favorable_dominance_effective_sample_size"] = 1.0
    assert not _evaluate(thin_movement)["research_ready"]

    early_metrics = _metrics()
    early_metrics.update(
        {
            "sample_size": 5,
            "control_sample_size": 5,
            "recent_sample_size": 5,
            "recent_control_sample_size": 5,
            "recency_effective_sample_size": 5.0,
            "recency_control_effective_sample_size": 5.0,
            "recency_favorable_dominance_effective_sample_size": 5.0,
            "recency_control_favorable_dominance_effective_sample_size": 5.0,
        }
    )
    early = _evaluate(
        early_metrics,
        early_mandatory_checks={"complete provenance": True},
    )
    assert early["research_ready"] is False
    assert early["maturity"] == "EARLY_CURRENT_EDGE"
    early_blocked = _evaluate(
        early_metrics,
        early_mandatory_checks={"complete provenance": False},
    )
    assert early_blocked["maturity"] != "EARLY_CURRENT_EDGE"
    stale_metrics = deepcopy(neither)
    stale_metrics["last_sample_age_hours"] = 22.0 * 24.0
    assert _evaluate(stale_metrics)["maturity"] == "STALE_OR_NOT_RECENT"

    zero_mae = _metrics()
    zero_mae["recency_weighted_median_mae_pct"] = 0.0
    serialized = json.dumps(_evaluate(zero_mae), allow_nan=False)
    assert "Infinity" not in serialized and "NaN" not in serialized

    as_of = datetime(2026, 8, 30, tzinfo=timezone.utc)
    recent = _row(1, as_of, success=True)
    half_life = _row(2, as_of - timedelta(days=14), success=False)
    future = _row(3, as_of + timedelta(minutes=1), success=True)
    controls = [
        _row(10, as_of - timedelta(days=1), success=False),
        _row(11, as_of - timedelta(days=2), success=False),
    ]
    rolling = engine._metrics(
        [recent, half_life, future],
        [recent, half_life, future, *controls],
        evidence_as_of_utc=as_of,
        already_independent_episodes=True,
    )
    assert rolling["future_selected_rows_excluded"] == 1
    assert rolling["recency_effective_sample_size"] == 1.8
    assert rolling["recency_effective_sample_size"] <= rolling["recent_sample_size"]
    assert rolling["recency_weighted_hit_rate_pct"] > 50.0
    priority = engine.rank_prospective_metrics(rolling, horizon_minutes=60)
    assert math.isclose(
        priority["score"],
        0.70 * priority["historical_score"]
        + 0.30 * priority["current_relevance_score"],
        abs_tol=1e-4,
    )
    stale_partition = engine._metrics(
        [half_life],
        [half_life, *controls],
        evidence_as_of_utc=as_of + timedelta(days=30),
        already_independent_episodes=True,
    )
    assert stale_partition["recent_sample_size"] == 0
    assert stale_partition["last_sample_age_hours"] > 21.0 * 24.0

    route_rank_metrics = _asymmetry_only()
    route_rank_metrics.update(
        {
            "hit_rate_pct": 45.0,
            "wilson_95_lower_pct": 20.0,
            "session_hit_rate_improvement_pct_points": -5.0,
            "favorable_dominance_rate_pct": 80.0,
            "favorable_dominance_wilson_95_lower_pct": 50.0,
            "favorable_dominance_improvement_pct_points": 15.0,
            "median_paired_favorable_minus_adverse_pct": 2.0,
            "median_mfe_pct": 3.0,
            "median_mae_pct": 1.0,
            "session_adjusted_mfe_percentile_pct": 80.0,
            "mae_p90_pct": 4.0,
            "rarity_class": "UNCOMMON",
        }
    )
    route_priority = engine.rank_prospective_metrics(
        route_rank_metrics, horizon_minutes=60
    )
    assert route_priority["selected_historical_route"] == "ASYMMETRY"

    stable_asymmetry_discovery = {
        **route_rank_metrics,
        "hit_rate_pct": 10.0,
        "favorable_dominance_rate_pct": 80.0,
    }
    changed_probability_discovery = {
        **stable_asymmetry_discovery,
        "hit_rate_pct": 45.0,
    }
    stable_asymmetry_holdout = {
        **route_rank_metrics,
        "hit_rate_pct": 45.0,
        "favorable_dominance_rate_pct": 80.0,
    }
    score_with_probability_shift = engine._final_score(
        stable_asymmetry_discovery,
        stable_asymmetry_holdout,
        horizon_minutes=60,
        q_value=0.90,
        asymmetry_q_value=0.10,
        complexity=1,
    )
    score_without_probability_shift = engine._final_score(
        changed_probability_discovery,
        stable_asymmetry_holdout,
        horizon_minutes=60,
        q_value=0.90,
        asymmetry_q_value=0.10,
        complexity=1,
    )
    assert score_with_probability_shift == score_without_probability_shift

    print("research formula acceptance self-test: PASS")


if __name__ == "__main__":
    run()
