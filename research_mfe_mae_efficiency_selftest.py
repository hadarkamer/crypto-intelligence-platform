"""Deterministic checks for zero-MAE efficiency semantics and consumers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import ai_alert_research
import research_formula_approval
import research_formula_engine as engine
import research_formula_store as store
import research_formula_worker
import research_mfe_mae_efficiency as efficiency


def _summary_row(event_id: int, *, mfe: float, mae: float) -> dict:
    return {
        "event": {
            "event_id": event_id,
            "alert_time_utc": datetime(2026, 8, 29, tzinfo=timezone.utc)
            + timedelta(hours=event_id),
            "symbol": "BTC",
            "event_type": "ZERO_MAE_SELFTEST",
        },
        "outcome_label": {
            "horizon_minutes": 240,
            "session_active_ratio": 1.0,
            "session_weekend_ratio": 0.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "directional_return_pct": 1.0,
            "mfe_pct": mfe,
            "mae_pct": mae,
            "time_to_first_progress_seconds": 60,
            "time_to_mfe_seconds": 300,
            "target_progress_ratio": 1.0,
            "target_reached": True,
        },
    }


def _gate_metrics(*, mfe: object, mae: object) -> dict:
    classified = efficiency.classify(mfe, mae)
    return {
        "sample_size": 20,
        "control_sample_size": 20,
        "sample_share_pct": 10.0,
        "rarity_class": "UNCOMMON",
        "time_span_hours": 96.0,
        "distinct_utc_dates": 4,
        "session_baseline_complete": True,
        "hit_rate_pct": 75.0,
        "wilson_95_lower_pct": 55.0,
        "session_hit_rate_improvement_pct_points": 15.0,
        "median_mfe_pct": mfe,
        "median_mae_pct": mae,
        "mae_p90_pct": 0.0 if mae == 0.0 else mae,
        "favorable_minus_p90_adverse_pct": (
            float(mfe) - float(mae)
            if isinstance(mfe, (int, float)) and isinstance(mae, (int, float))
            else None
        ),
        "median_mfe_percentile_pct": 85.0,
        "session_adjusted_mfe_percentile_pct": 85.0,
        "universe_p90_mfe_pct": 3.0,
        "median_time_to_first_progress_seconds": 60,
        "avg_target_progress_ratio": 1.0,
        # A stale persisted number must not override the underlying medians.
        "median_mfe_mae_ratio": classified.ratio,
        "median_mfe_mae_ratio_state": classified.state,
    }


def _shadow_row(*, mfe: float, mae: float) -> dict:
    return {
        "event_id": 100,
        "alert_time_utc": datetime(2026, 8, 29, tzinfo=timezone.utc),
        "symbol": "BTC",
        "event_type": "ZERO_MAE_SELFTEST",
        "evaluation_status": "MATCHED",
        "outcome_available": True,
        "outcome_due": True,
        "full_horizon_outcome_available": True,
        "first_touch_available": True,
        "first_touch_hit": True,
        "directional_return_pct": 1.0,
        "path_success": True,
        "first_touch_status": "HIT",
        "mfe_pct": mfe,
        "mae_pct": mae,
        "full_horizon_mae_pct": 5.0,
        "time_to_first_progress_seconds": 60,
        "time_to_mfe_seconds": 300,
        "target_progress_ratio": 1.0,
        "target_reached": True,
    }


def run() -> None:
    legacy_override = store._bind_efficiency_policy("shadow-monitoring-v1-old")
    assert legacy_override.endswith(efficiency.POLICY_VERSION)
    assert store._bind_efficiency_policy(legacy_override) == legacy_override
    assert efficiency.POLICY_VERSION in store._SHADOW_MONITORING_POLICY_VERSION

    for serializer in (
        store._json,
        store._json_safe,
        engine._json_safe,
        ai_alert_research._json_safe,
        research_formula_approval._canonical_json,
    ):
        try:
            serializer({"not_json": float("nan")})
        except ValueError:
            pass
        else:
            raise AssertionError("non-finite JSON value was accepted")

    finite = efficiency.classify(2.0, 0.5)
    assert finite.state == efficiency.FINITE and finite.ratio == 4.0
    assert finite.meets_threshold(1.25) is True
    assert finite.capped_quality(3.0) == 1.0
    json.dumps(finite.evidence(), allow_nan=False)

    unbounded = efficiency.classify(2.0, 0.0)
    assert unbounded.state == efficiency.UNBOUNDED_ZERO_MAE
    assert unbounded.ratio is None
    assert unbounded.meets_threshold(1_000_000.0) is True
    assert unbounded.capped_quality(5.0) == 1.0
    json.dumps(unbounded.evidence(), allow_nan=False)

    undefined = efficiency.classify(0.0, 0.0)
    assert undefined.state == efficiency.UNDEFINED_ZERO_ZERO
    assert undefined.ratio is None
    assert undefined.meets_threshold(0.0) is False
    assert undefined.capped_quality(3.0) == 0.0
    json.dumps(undefined.evidence(), allow_nan=False)

    for invalid in (
        efficiency.classify(None, 0.0),
        efficiency.classify(-1.0, 0.0),
        efficiency.classify(1.0, -0.1),
        efficiency.classify(float("nan"), 1.0),
        efficiency.classify(1.0, float("inf")),
        efficiency.classify(10**10000, 1.0),
        efficiency.classify(1.0, 10**10000),
    ):
        assert invalid.state == efficiency.INVALID_OR_MISSING
        assert invalid.ratio is None
        assert invalid.meets_threshold(0.0) is False
        assert invalid.capped_quality(3.0) == 0.0
        json.dumps(invalid.evidence(), allow_nan=False)
    assert finite.meets_threshold(10**10000) is False
    assert finite.capped_quality(10**10000) == 0.0

    zero_favorable = efficiency.classify(0.0, 1.0)
    assert zero_favorable.state == efficiency.FINITE
    assert zero_favorable.ratio == 0.0
    assert zero_favorable.meets_threshold(1.25) is False
    json.dumps(zero_favorable.evidence(), allow_nan=False)

    overflow = efficiency.classify(1e308, 1e-308)
    assert overflow.state == efficiency.INVALID_OR_MISSING
    assert overflow.ratio is None
    json.dumps(overflow.evidence(), allow_nan=False)

    rows = [_summary_row(index, mfe=2.0, mae=0.0) for index in range(1, 4)]
    metrics = engine.summarize_outcomes(rows, rows)
    assert metrics["median_mfe_mae_ratio"] is None
    assert metrics["median_mfe_mae_ratio_state"] == efficiency.UNBOUNDED_ZERO_MAE
    assert (
        metrics["median_mfe_mae_ratio_policy_version"]
        == efficiency.POLICY_VERSION
    )
    json.dumps(metrics, allow_nan=False, default=str)

    gate_metrics = _gate_metrics(mfe=2.5, mae=0.0)
    stage, reasons = engine._recommended_stage(
        gate_metrics,
        gate_metrics,
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
    )
    assert stage == "SHADOW", reasons
    assert "MFE/MAE efficiency" not in reasons
    priority = engine.rank_prospective_metrics(
        gate_metrics, horizon_minutes=240
    )
    assert priority["components"]["mfe_mae_ratio"] == 10.0
    assert priority["policy_version"].startswith(
        "prospective-shadow-priority-v2"
    )
    assert (
        priority["mfe_mae_efficiency_policy_version"]
        == efficiency.POLICY_VERSION
    )

    # Beam and final ranking must award the full capped efficiency component
    # without relying on a synthetic epsilon or a persisted Infinity.
    preliminary_unbounded = engine._preliminary_score(gate_metrics, 1)
    preliminary_invalid_metrics = {
        **gate_metrics,
        "median_mae_pct": None,
    }
    preliminary_invalid = engine._preliminary_score(
        preliminary_invalid_metrics, 1
    )
    assert round(preliminary_unbounded - preliminary_invalid, 8) == 32.0
    final_unbounded = engine._final_score(
        gate_metrics,
        gate_metrics,
        horizon_minutes=240,
        q_value=0.01,
        complexity=1,
    )
    final_invalid = engine._final_score(
        preliminary_invalid_metrics,
        preliminary_invalid_metrics,
        horizon_minutes=240,
        q_value=0.01,
        complexity=1,
    )
    assert round(final_unbounded - final_invalid, 4) == 8.0

    adverse_tail = {**gate_metrics, "mae_p90_pct": 3.0}
    _, adverse_reasons = engine._recommended_stage(
        adverse_tail,
        adverse_tail,
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
    )
    assert "MFE/MAE efficiency" not in adverse_reasons
    assert (
        "favorable excursion exceeds p90 adverse excursion"
        in adverse_reasons
    )

    stale_undefined = {**_gate_metrics(mfe=0.0, mae=0.0)}
    stale_undefined["median_mfe_mae_ratio"] = 999.0
    stale_undefined["median_mfe_mae_ratio_state"] = efficiency.FINITE
    assert efficiency.from_metrics(stale_undefined).state == (
        efficiency.UNDEFINED_ZERO_ZERO
    )
    _, undefined_reasons = engine._recommended_stage(
        stale_undefined,
        stale_undefined,
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
    )
    assert "MFE/MAE efficiency" in undefined_reasons

    validation = store._build_shadow_validation(
        {"horizon_minutes": 240},
        [_shadow_row(mfe=2.0, mae=0.0)],
        evaluated_at_utc=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert validation["gates"]["future MFE/MAE efficiency"] is True
    assert validation["metrics"]["median_mfe_mae_ratio"] is None
    assert validation["metrics"]["median_mfe_mae_ratio_state"] == (
        efficiency.UNBOUNDED_ZERO_MAE
    )
    assert validation["mfe_mae_efficiency_policy_version"] == (
        efficiency.POLICY_VERSION
    )
    json.dumps(validation, allow_nan=False, default=str)
    frozen_approval_evidence = research_formula_approval._canonical_json(
        validation
    )
    assert efficiency.UNBOUNDED_ZERO_MAE in frozen_approval_evidence
    assert efficiency.POLICY_VERSION in frozen_approval_evidence

    invalid_validation = store._build_shadow_validation(
        {"horizon_minutes": 240},
        [_shadow_row(mfe=0.0, mae=0.0)],
        evaluated_at_utc=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert invalid_validation["gates"]["future MFE/MAE efficiency"] is False

    delivery = {
        "shadow_validation_metrics": {"metrics": gate_metrics},
        "holdout_metrics": {"rarity_class": "UNCOMMON"},
        "horizon_minutes": 240,
        "direction": "LONG",
        "symbol": "BTC",
        "event_id": 100,
        "event_type": "ZERO_MAE_SELFTEST",
        "formula_id": 1,
        "formula_version": 1,
        "formula_text": "LONG WHEN selftest",
        "current_price": 100.0,
        "target_price": 102.0,
    }
    text = research_formula_worker.FormulaResearchWorker._live_alert_text(delivery)
    assert "MFE/MAE: בלתי־חסום (MAE חציוני 0)" in text
    assert "inf" not in text.lower() and "nan" not in text.lower()

    group = ai_alert_research._mfe_mae_group_evidence(
        {"median_mfe_pct": 2.0, "median_mae_pct": 0.0}
    )
    assert group["median_mfe_to_mae_ratio"] is None
    assert group["median_mfe_to_mae_ratio_state"] == (
        efficiency.UNBOUNDED_ZERO_MAE
    )
    json.dumps(group, allow_nan=False)

    legacy_registry_metrics = {
        "median_mfe_pct": 2.0,
        "median_mae_pct": 0.0,
        "median_mfe_mae_ratio": None,
    }
    registry_row = store._formula_registry_row(
        {
            "formula_id": 2852,
            "discovery_metrics": legacy_registry_metrics,
            "holdout_metrics": json.dumps(legacy_registry_metrics),
        }
    )
    for key in ("discovery_metrics", "holdout_metrics"):
        assert registry_row[key]["median_mfe_mae_ratio"] is None
        assert registry_row[key]["median_mfe_mae_ratio_state"] == (
            efficiency.UNBOUNDED_ZERO_MAE
        )
        assert registry_row[key]["median_mfe_mae_ratio_policy_version"] == (
            efficiency.POLICY_VERSION
        )
    json.dumps(registry_row, allow_nan=False)

    print("research MFE/MAE efficiency self-test: PASS")


if __name__ == "__main__":
    run()
