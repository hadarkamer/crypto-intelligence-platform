"""Deterministic checks for automatic formula discovery and holdout safety."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from statistics import mean

import research_formula_engine as engine


def _row(index: int):
    event_time = datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(minutes=index * 30)
    input_active, input_weekend, _ = engine.market_session_baseline.session_ratios(
        event_time - timedelta(minutes=60), event_time
    )
    outcome_active, outcome_weekend, _ = engine.market_session_baseline.session_ratios(
        event_time, event_time + timedelta(minutes=240)
    )
    market_session = (
        "ACTIVE"
        if engine.market_session_baseline.is_active_market(event_time)
        else "WEEKEND"
    )
    direction = "LONG" if index % 2 == 0 else "SHORT"
    signal = 2.0 if index % 5 in {0, 1, 2} else -2.0
    directional_return = 1.0 if signal > 0 else -0.4
    snapshot = {f"snapshot.synthetic.feature_{feature}": float((index + feature) % 11) for feature in range(24)}
    return {
        "feature_schema_version": "selftest-matrix-v2",
        "event": {
            "event_id": index + 1,
            "alert_time_utc": event_time,
            "symbol": "BTC" if index % 3 else "ETH",
            "direction": direction,
            "source_side": "SHORT" if direction == "LONG" else "LONG",
            "timeframe": "1h",
            "event_type": "SELFTEST_SIGNAL",
            "strategy_version": "selftest-v1",
            "code_version": "abc123",
        },
        "time_features": {
            "utc_hour": event_time.hour,
            "utc_weekday": event_time.weekday(),
            "utc_weekday_name": event_time.strftime("%A").upper(),
            "is_calendar_weekend_utc": event_time.weekday() >= 5,
            "is_market_weekend": market_session == "WEEKEND",
            "market_session": market_session,
            "market_regime": market_session,
            "fixed_utc_session_bucket": "SELFTEST",
        },
        "historical_context": {
            "event_market_session": market_session,
            "windows": {
                "60m": {
                    "session_active_ratio": input_active,
                    "session_weekend_ratio": input_weekend,
                    "session_composition": (
                        "ACTIVE_ONLY"
                        if input_active == 1.0
                        else "WEEKEND_ONLY"
                        if input_active == 0.0
                        else "MIXED"
                    ),
                    "prior_points": 120,
                    "sufficient_history": True,
                    "price_change_pct_percentile_session_matched": (
                        85.0 if signal > 0 else 15.0
                    ),
                    "price_change_pct_abs_percentile_session_matched": 80.0,
                    "oi_change_pct_percentile_session_matched": float(index % 100),
                }
            },
        },
        "raw_features": {
            "captured_event_inputs": {
                "event_initial_target_distance_pct": 1.0 + (index % 4) / 10.0,
                "snapshot_inputs": {"near_share_pct": 60.0 + index % 20},
            },
            "latest_at_or_before_alert": {
                "price_oi": {"available": True, "age_minutes": 1.0},
                "futures_cvd": {"available": True, "age_minutes": 1.0, "buy_sell_ratio": 1.2},
                "spot_cvd": {"available": True, "age_minutes": 1.0, "buy_sell_ratio": 1.4},
            },
            "windows": {
                "60m": {
                    "session_active_ratio": input_active,
                    "session_weekend_ratio": input_weekend,
                    "session_composition": (
                        "ACTIVE_ONLY"
                        if input_active == 1.0
                        else "WEEKEND_ONLY"
                        if input_active == 0.0
                        else "MIXED"
                    ),
                    "price_change_pct": signal if direction == "LONG" else -signal,
                    "oi_change_pct": float(index % 7) - 3.0,
                    "futures_continuous_cvd_change_usd": signal * 1_000_000,
                    "spot_continuous_cvd_change_usd": signal * 500_000,
                    "futures_api_cvd_change_usd": signal * 800_000,
                    "spot_api_cvd_change_usd": signal * 400_000,
                    "spot_to_futures_abs_cvd_ratio": 0.5,
                    "price_oi_state": "PRICE_UP__OI_UP" if signal > 0 else "PRICE_DOWN__OI_UP",
                    "spot_futures_alignment": "ALIGNED",
                    "price_spot_alignment": "ALIGNED",
                    "price_futures_alignment": "ALIGNED",
                    "complete": True,
                }
            },
        },
        "model_features": {
            "alert_score": 60.0 + index % 35,
            "initial_target_distance_pct": 1.0,
            "categories": ["SYNTHETIC"],
            "snapshot_features": snapshot,
        },
        "sequence_features": {
            "30m": {
                "same_symbol_alerts": index % 4,
                "same_symbol_same_direction": index % 3,
                "same_symbol_distinct_event_types": 1,
                "same_setup_repetitions": index % 5,
                "market_alerts": index % 9,
                "market_distinct_symbols": 2,
                "market_long_alerts": index % 6,
                "market_short_alerts": (index + 2) % 6,
                "market_direction_balance_pct": float((index % 11) - 5) * 10.0,
            }
        },
        "outcome_label": {
            "horizon_minutes": 240,
            "session_active_ratio": outcome_active,
            "session_weekend_ratio": outcome_weekend,
            "session_composition": (
                "ACTIVE_ONLY"
                if outcome_active == 1.0
                else "WEEKEND_ONLY"
                if outcome_active == 0.0
                else "MIXED"
            ),
            "movement_width_reference": {"floor_scale_factor": 1.0},
            "directional_return_pct": directional_return,
            "path_success": directional_return > 0,
            "first_touch_status": (
                "HIT" if directional_return > 0 else "MISS"
            ),
            "mfe_pct": 1.8 if directional_return > 0 else 0.4,
            "mae_pct": 0.2 if directional_return > 0 else 1.1,
            "full_horizon_mae_pct": 8.0 if directional_return > 0 else 9.0,
            "time_to_first_progress_seconds": 120 if directional_return > 0 else 1800,
            "time_to_mfe_seconds": 900,
            "target_progress_ratio": 0.9 if directional_return > 0 else 0.2,
            "target_reached": directional_return > 0,
        },
    }


def _hierarchy_row(index: int) -> dict:
    """Five bounded feature families with stable incremental information."""
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
        minutes=index * 30
    )
    residue = index % 17
    false_residues = (
        {0, 1},
        {2, 3},
        {4, 5},
        {6, 7},
        {8, 9},
    )
    factors = [residue not in blocked for blocked in false_residues]
    success = all(factors)
    return {
        "feature_schema_version": "hierarchy-selftest-v1",
        "event": {
            "event_id": 100_000 + index,
            "alert_time_utc": event_time,
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "HIERARCHY_SELFTEST",
        },
        "model_features": {
            "alert_score": None,
            "initial_target_distance_pct": None,
            "categories": [],
            "snapshot_features": {
                f"hierarchy.factor_{offset}": float(value)
                for offset, value in enumerate(factors)
            },
        },
        "outcome_label": {
            "horizon_minutes": 240,
            "session_active_ratio": 1.0,
            "session_weekend_ratio": 0.0,
            "session_composition": "ACTIVE_ONLY",
            "movement_width_reference": {"floor_scale_factor": 1.0},
            "directional_return_pct": 2.0 if success else -0.5,
            "path_success": success,
            "first_touch_status": "HIT" if success else "MISS",
            "mfe_pct": 3.0 if success else 0.2,
            "mae_pct": 0.2 if success else 2.0,
            "time_to_first_progress_seconds": 60 if success else 1800,
            "time_to_mfe_seconds": 600,
            "target_progress_ratio": 1.0 if success else 0.1,
            "target_reached": success,
        },
    }


def run() -> None:
    assert engine.condition_matches(
        {"x": 1.0}, {"feature": "x", "operator": "==", "value": 1}
    )
    assert not engine.condition_matches(
        {"x": "1"}, {"feature": "x", "operator": "==", "value": 1}
    )
    assert not engine.condition_matches(
        {"x": "0.8"}, {"feature": "x", "operator": ">=", "value": 0.8}
    )
    assert not engine.condition_matches(
        {"x": True}, {"feature": "x", "operator": "==", "value": 1}
    )
    assert engine.MIN_MEDIAN_MFE_BY_HORIZON == dict(
        engine.research_no_dwell_outcome.BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON
    )
    rows = [_row(index) for index in range(140)]

    # Success is the explicit final first-touch label, never the endpoint
    # return.  A later negative close cannot erase a prior touch, while a
    # positive close cannot fabricate one.  Full-horizon MAE is diagnostic;
    # ranking risk is the pre-qualifying `mae_pct` supplied by the matrix.
    endpoint_positive_miss = _row(0)
    endpoint_positive_miss["outcome_label"].update(
        {
            "directional_return_pct": 9.0,
            "path_success": False,
            "first_touch_status": "MISS",
            "mae_pct": 0.25,
            "full_horizon_mae_pct": 25.0,
        }
    )
    endpoint_negative_hit = _row(1)
    endpoint_negative_hit["outcome_label"].update(
        {
            "directional_return_pct": -9.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "mae_pct": 0.75,
            "full_horizon_mae_pct": 50.0,
        }
    )
    explicit_metrics = engine.summarize_outcomes(
        [endpoint_positive_miss, endpoint_negative_hit],
        [endpoint_positive_miss, endpoint_negative_hit],
    )
    assert explicit_metrics["successes"] == 1
    assert explicit_metrics["hit_rate_pct"] == 50.0
    assert explicit_metrics["median_mae_pct"] == 0.5
    positive_miss_metrics = engine.summarize_outcomes(
        [endpoint_positive_miss], [endpoint_positive_miss]
    )
    negative_hit_metrics = engine.summarize_outcomes(
        [endpoint_negative_hit], [endpoint_negative_hit]
    )
    assert positive_miss_metrics["successes"] == 0
    assert positive_miss_metrics["hit_rate_pct"] == 0.0
    assert negative_hit_metrics["successes"] == 1
    assert negative_hit_metrics["hit_rate_pct"] == 100.0

    session_selected = _row(500)
    session_selected["event"]["event_id"] = 900_000
    session_selected["outcome_label"].update(
        {
            "directional_return_pct": -5.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "session_active_ratio": 1.0,
            "session_weekend_ratio": 0.0,
            "session_composition": "ACTIVE_ONLY",
        }
    )
    endpoint_positive_controls = []
    for index in range(30):
        control = _row(600 + index)
        control["event"]["event_id"] = 901_000 + index
        control["outcome_label"].update(
            {
                "directional_return_pct": 5.0,
                "path_success": False,
                "first_touch_status": "MISS",
                "session_active_ratio": 1.0,
                "session_weekend_ratio": 0.0,
                "session_composition": "ACTIVE_ONLY",
            }
        )
        endpoint_positive_controls.append(control)
    baseline_metrics = engine.summarize_outcomes(
        [session_selected],
        [session_selected, *endpoint_positive_controls],
    )
    assert baseline_metrics["control_sample_size"] == 30
    assert baseline_metrics["control_hit_rate_pct"] == 0.0
    assert baseline_metrics["session_matched_hit_rate_baseline_pct"] == 0.0
    assert baseline_metrics["hit_rate_improvement_pct_points"] == 100.0
    assert baseline_metrics["session_hit_rate_improvement_pct_points"] == 100.0
    assert baseline_metrics["unadjusted_one_sided_p_value"] is not None
    assert baseline_metrics["one_sided_p_value"] is not None

    missing_label = _row(2)
    missing_label["outcome_label"].pop("path_success")
    missing_metrics = engine.summarize_outcomes([missing_label], [missing_label])
    assert missing_metrics["successes"] == 0
    assert missing_metrics["hit_rate_pct"] is None

    malformed_labels = []
    for event_id, path_success, status in (
        (20, "true", "HIT"),
        (21, None, "PENDING"),
        (22, True, "MISS"),
        (23, False, "HIT"),
    ):
        malformed = _row(event_id)
        malformed["outcome_label"].update(
            {"path_success": path_success, "first_touch_status": status}
        )
        malformed_labels.append(malformed)
    malformed_metrics = engine.summarize_outcomes(
        malformed_labels, malformed_labels
    )
    assert malformed_metrics["successes"] == 0
    assert malformed_metrics["hit_rate_pct"] is None

    eligibility_rows = [_row(index) for index in range(8)]
    eligibility_rows[2]["outcome_label"].pop("path_success")
    eligibility_rows[3]["outcome_label"].update(
        {"path_success": True, "first_touch_status": "MISS"}
    )
    eligibility = engine.discover_formulas(
        eligibility_rows,
        horizon_minutes=240,
        feature_schema_version="explicit-first-touch-selftest",
        config=engine.DiscoveryConfig(
            discovery_fraction=0.5,
            min_discovery_samples=1,
            min_holdout_samples=1,
            max_single_predicates=2,
            max_pair_candidates=0,
            max_triple_candidates=0,
            max_candidates_evaluated=2,
            max_formulas_returned=1,
        ),
    )
    assert (
        eligibility["discovery_sample_size"]
        + eligibility["holdout_sample_size"]
        == 6
    ), "a non-terminal or inconsistent first-touch label entered Discovery"
    corrected = engine._bh_q_values([0.01, None, 0.02])
    assert corrected[1] is None
    assert corrected[0] is not None and corrected[0] >= 0.03 - 1e-12
    assert corrected[2] is not None and corrected[2] >= 0.03 - 1e-12

    # Prospective matching is explicit about all three possible states. It
    # reads decision-time features only: the guarded outcome mapping must never
    # be inspected while evaluating a formula.
    class _OutcomeGuard(dict):
        def get(self, key, default=None):
            assert key != "outcome_label", "Shadow matching read an outcome"
            return super().get(key, default)

        def __getitem__(self, key):
            assert key != "outcome_label", "Shadow matching read an outcome"
            return super().__getitem__(key)

    guarded = _OutcomeGuard(_row(0))
    condition = {
        "feature": "aligned.60m.price_change_pct",
        "operator": ">=",
        "value": 1.0,
    }
    matched = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[condition],
    )
    assert matched["status"] == "MATCHED" and matched["matched"] is True
    assert matched["condition_results"][0]["actual"] == 2.0
    assert all(not key.startswith("outcome") for key in matched["features"])

    unmatched = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[{**condition, "value": 3.0}],
    )
    assert unmatched["status"] == "UNMATCHED"
    assert unmatched["matched"] is False

    unevaluable = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[
            {
                "feature": "raw.60m.feature_that_was_not_captured",
                "operator": ">=",
                "value": 1.0,
            }
        ],
    )
    assert unevaluable["status"] == "UNEVALUABLE"
    assert unevaluable["condition_results"][0]["available"] is False
    assert engine.evaluate_formula(
        None, direction="LONG", conditions=[condition]
    )["status"] == "UNEVALUABLE"
    paired_rows = []
    for index in range(60):
        original = _row(index)
        opposite = _row(index)
        opposite["event"] = dict(opposite["event"])
        opposite["event"]["event_id"] = 10_000 + index
        opposite["event"]["direction"] = (
            "SHORT" if original["event"]["direction"] == "LONG" else "LONG"
        )
        paired_rows.extend((original, opposite))
    paired_result = engine.discover_formulas(
        paired_rows,
        horizon_minutes=240,
        feature_schema_version="paired-time-selftest",
        config=engine.DiscoveryConfig(
            max_single_predicates=8,
            max_pair_candidates=4,
            max_triple_candidates=0,
            max_candidates_evaluated=20,
            max_formulas_returned=5,
        ),
    )
    assert paired_result["discovery_sample_size"] == 84
    assert paired_result["holdout_sample_size"] == 36
    assert "identical timestamps never split" in paired_result["split_policy"]
    selected_ratios = [0.0, 0.2, 0.5, 0.8, 1.0]
    optimized_profile = engine._composition_profile_weights(
        rows[:12], selected_ratios
    )
    optimized_by_id = {
        int(row["event"]["event_id"]): weight
        for row, weight in optimized_profile
    }
    for row in rows[:12]:
        historical_ratio = engine._row_outcome_active_ratio(row)
        brute = mean(
            engine.market_session_baseline.composition_weight(
                selected_ratio, historical_ratio
            )
            for selected_ratio in selected_ratios
        )
        optimized = optimized_by_id.get(int(row["event"]["event_id"]), 0.0)
        assert abs(brute - optimized) < 1e-12

    result = engine.discover_formulas(
        rows,
        horizon_minutes=240,
        feature_schema_version="selftest-matrix-v2",
    )
    assert result["available"] is True
    assert result["discovery_sample_size"] == 98
    assert result["holdout_sample_size"] == 42
    assert result["candidates_evaluated"] >= 1000
    assert result["formulas"]
    assert result["automatic_stage_ceiling"] == "SHADOW"
    assert "future-Shadow validation" in result["live_activation"]

    formula = result["formulas"][0]
    assert formula["conditions"]
    assert formula["recommended_stage"] in {
        "DISCOVERED", "BACKTESTED", "HOLDOUT_PASSED", "SHADOW"
    }
    assert formula["live_alert_approved"] is False
    assert "sample_size" in formula["discovery_metrics"]
    assert "mae_p95_pct" in formula["holdout_metrics"]
    assert "median_mfe_percentile_pct" in formula["holdout_metrics"]
    assert "session_adjusted_mfe_percentile_pct" in formula["holdout_metrics"]
    assert "outcome_session_composition_counts" in formula["holdout_metrics"]
    assert "movement_width_floor_effective_pct" in formula["holdout_metrics"]
    assert "favorable_minus_p90_adverse_pct" in formula["holdout_metrics"]
    assert "q_value" in formula["multiple_testing"]
    assert all(
        candidate["recommended_stage"] != "SHADOW"
        for candidate in result["formulas"]
    ), "a sub-day holdout must never enter Shadow"

    features = engine.extract_decision_features(rows[0])
    assert all(not key.startswith("outcome") for key in features)
    assert "aligned.60m.price_change_pct" in features
    assert "raw.60m.session_active_ratio" in features
    assert "historical.60m.price_change_pct_percentile_session_matched" in features

    max_pain_row = _row(0)
    max_pain_row["max_pain_features"] = {
        "evaluation_status": "EVALUABLE",
        "available_at_utc": max_pain_row["event"]["alert_time_utc"]
        - timedelta(minutes=5),
        "features": {
            "max_pain.12h.short_target_signed_distance_pct": 2.5,
            "max_pain.aggregate.short_long_liquidity_ratio": 1.8,
            # A malformed unnamespaced value must never become a predicate.
            "source_snapshot_set_id": 123,
        },
    }
    max_pain_decision = engine.extract_decision_features(max_pain_row)
    assert max_pain_decision[
        "max_pain.12h.short_target_signed_distance_pct"
    ] == 2.5
    assert max_pain_decision[
        "max_pain.aggregate.short_long_liquidity_ratio"
    ] == 1.8
    assert "source_snapshot_set_id" not in max_pain_decision
    max_pain_row["max_pain_features"]["evaluation_status"] = "UNEVALUABLE"
    assert not any(
        key.startswith("max_pain.")
        for key in engine.extract_decision_features(max_pain_row)
    )
    assert engine.candidate_feature_allowed(
        "historical.60m.price_change_pct_percentile_session_matched"
    )
    for technical_feature in (
        "historical.60m.price_change_pct_history_samples",
        "historical.60m.price_change_pct_session_matched_samples",
        "historical.60m.prior_points",
        "historical.60m.sufficient_history",
        "latest.price_oi.age_minutes",
        "time.utc_hour",
        "time.market_utc_offset_minutes",
    ):
        assert not engine.candidate_feature_allowed(technical_feature)
    assert all(
        engine.candidate_feature_allowed(condition["feature"])
        for candidate in result["formulas"]
        for condition in candidate["conditions"]
    )
    assert result["config"]["hierarchical_search_enabled"] is False
    assert all(candidate["condition_count"] <= 3 for candidate in result["formulas"])
    assert all(
        direction["hierarchical_search"]["quad_candidates_attempted"] == 0
        and direction["hierarchical_search"]["quint_candidates_attempted"] == 0
        for direction in result["directions"]
    )
    assert engine.formula_key(
        direction=formula["direction"],
        horizon_minutes=240,
        feature_schema_version="selftest-matrix-v2",
        conditions=list(reversed(formula["conditions"])),
    ) == formula["formula_key"]

    # A tiny 100%-hit move must not outrank a materially wider, still reliable
    # move. This guards the production issue observed in formula v1.
    common = {
        "sample_size": 20,
        "sample_share_pct": 10.0,
        "median_time_to_first_progress_seconds": 300,
        "avg_target_progress_ratio": 0.5,
    }
    narrow = {
        **common,
        "hit_rate_pct": 100.0,
        "wilson_95_lower_pct": 83.0,
        "hit_rate_improvement_pct_points": 30.0,
        "median_mfe_pct": 0.385,
        "universe_p90_mfe_pct": 3.0,
        "median_mfe_percentile_pct": 30.0,
        "median_mae_pct": 0.10,
        "mae_p90_pct": 0.25,
        "median_mfe_mae_ratio": 3.85,
    }
    wide = {
        **common,
        "hit_rate_pct": 75.0,
        "wilson_95_lower_pct": 55.0,
        "hit_rate_improvement_pct_points": 15.0,
        "median_mfe_pct": 2.50,
        "universe_p90_mfe_pct": 3.0,
        "median_mfe_percentile_pct": 85.0,
        "median_mae_pct": 0.40,
        "mae_p90_pct": 0.80,
        "median_mfe_mae_ratio": 6.25,
    }
    narrow_score = engine._final_score(
        narrow, narrow, horizon_minutes=240, q_value=0.01, complexity=2
    )
    wide_score = engine._final_score(
        wide, wide, horizon_minutes=240, q_value=0.01, complexity=2
    )
    assert wide_score > narrow_score
    assert engine._final_score(
        wide, wide, horizon_minutes=240, q_value=0.01, complexity=5
    ) < engine._final_score(
        wide, wide, horizon_minutes=240, q_value=0.01, complexity=3
    )
    narrow_stage, narrow_reasons = engine._recommended_stage(
        {
            **narrow,
            "time_span_hours": 96,
            "distinct_utc_dates": 4,
        },
        {
            **narrow,
            "time_span_hours": 48,
            "distinct_utc_dates": 3,
        },
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
    )
    assert narrow_stage == "BACKTESTED"
    assert "wide favorable movement floor" in narrow_reasons
    _, hierarchical_sample_reasons = engine._recommended_stage(
        {
            **wide,
            "sample_size": 20,
            "time_span_hours": 96,
            "distinct_utc_dates": 4,
        },
        {
            **wide,
            "sample_size": 10,
            "time_span_hours": 48,
            "distinct_utc_dates": 3,
        },
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
        complexity=4,
    )
    assert "hierarchical discovery sample" in hierarchical_sample_reasons
    assert "hierarchical holdout sample" in hierarchical_sample_reasons

    # Weekend calibration changes only the absolute width floor. The hit,
    # Wilson, improvement, risk and percentile gates remain unchanged.
    assert engine.minimum_wide_move_pct(
        240, {"movement_width_floor_scale_factor": 0.60}
    ) == 0.60

    # A missing prior-only reference must never be reconstructed from future
    # control MFE. This population deliberately has small weekend future MFE
    # and large active future MFE, which used to imply a lower fallback scale.
    def _outcome_row(
        event_id: int,
        *,
        active_ratio: float,
        mfe_pct: float,
        width_scale=None,
    ):
        label = {
            "horizon_minutes": 240,
            "session_active_ratio": active_ratio,
            "session_weekend_ratio": 1.0 - active_ratio,
            "session_composition": (
                "ACTIVE_ONLY" if active_ratio == 1.0 else "WEEKEND_ONLY"
            ),
            "directional_return_pct": 1.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "mfe_pct": mfe_pct,
            "mae_pct": 0.20,
            "time_to_first_progress_seconds": 60,
            "time_to_mfe_seconds": 600,
            "target_progress_ratio": 1.0,
            "target_reached": True,
        }
        if width_scale is not None:
            label["movement_width_reference"] = {
                "floor_scale_factor": width_scale,
                "source": "frozen prior-only self-test calibration",
            }
        return {
            "event": {
                "event_id": event_id,
                "alert_time_utc": datetime(2026, 8, 29, tzinfo=timezone.utc)
                + timedelta(minutes=event_id),
                "symbol": "BTC",
                "event_type": "SELFTEST_SIGNAL",
            },
            "outcome_label": label,
        }

    selected_without_reference = [
        _outcome_row(50_000, active_ratio=0.0, mfe_pct=1.25)
    ]
    weekend_controls = [
        _outcome_row(50_100 + index, active_ratio=0.0, mfe_pct=0.10)
        for index in range(30)
    ]
    active_controls = [
        _outcome_row(50_200 + index, active_ratio=1.0, mfe_pct=5.0)
        for index in range(30)
    ]
    missing_reference_metrics = engine.summarize_outcomes(
        selected_without_reference,
        [*selected_without_reference, *weekend_controls, *active_controls],
    )
    assert missing_reference_metrics["session_matched_mfe_effective_samples"] >= 30
    assert missing_reference_metrics["active_reference_mfe_effective_samples"] >= 30
    assert (
        missing_reference_metrics["session_matched_control_p90_mfe_pct"]
        < missing_reference_metrics["active_reference_mfe_p90_pct"]
    )
    assert missing_reference_metrics["movement_width_floor_scale_factor"] == 1.0
    assert (
        missing_reference_metrics["movement_width_floor_effective_pct"]
        == missing_reference_metrics["movement_width_floor_base_pct"]
    )
    assert "no relaxation" in missing_reference_metrics["movement_width_floor_source"]

    selected_with_reference = [
        _outcome_row(
            50_000,
            active_ratio=0.0,
            mfe_pct=1.25,
            width_scale=0.60,
        )
    ]
    frozen_reference_metrics = engine.summarize_outcomes(
        selected_with_reference,
        [*selected_with_reference, *weekend_controls, *active_controls],
    )
    assert frozen_reference_metrics["movement_width_floor_scale_factor"] == 0.60
    assert frozen_reference_metrics["movement_width_floor_effective_pct"] == 0.60
    for probability_or_risk_metric in (
        "hit_rate_pct",
        "wilson_95_lower_pct",
        "median_mae_pct",
        "mae_p90_pct",
        "median_mfe_mae_ratio",
    ):
        assert (
            frozen_reference_metrics[probability_or_risk_metric]
            == missing_reference_metrics[probability_or_risk_metric]
        )

    # Four/five-condition search is opt-in and hierarchical: only stable triple
    # parents enter the bounded beam, and every child must improve both the
    # discovery score and the later chronological holdout score.
    hierarchy_rows = [_hierarchy_row(index) for index in range(816)]
    hierarchy_config = engine.DiscoveryConfig(
        numeric_quantiles=(0.50,),
        max_single_predicates=10,
        max_pair_candidates=20,
        max_triple_candidates=30,
        max_candidates_evaluated=100,
        max_formulas_returned=40,
        hierarchical_search_enabled=True,
        hierarchical_beam_width=10,
        hierarchical_extension_predicates=10,
        max_quad_candidates=30,
        max_quint_candidates=20,
        hierarchical_min_discovery_samples=40,
        hierarchical_min_holdout_samples=20,
        hierarchical_discovery_sample_increment=0,
        hierarchical_holdout_sample_increment=0,
        hierarchical_min_parent_gain=0.25,
        evidence_family_overlap_threshold=0.95,
    )
    hierarchy_result = engine.discover_formulas(
        hierarchy_rows,
        horizon_minutes=240,
        feature_schema_version="hierarchy-selftest-v1",
        config=hierarchy_config,
    )
    long_diagnostics = next(
        direction
        for direction in hierarchy_result["directions"]
        if direction["direction"] == "LONG"
    )
    hierarchy_diagnostics = long_diagnostics["hierarchical_search"]
    assert hierarchy_diagnostics["stable_triple_parents"] > 0
    assert hierarchy_diagnostics["quad_candidates_passed_gain"] > 0
    assert hierarchy_diagnostics["quint_candidates_passed_gain"] > 0
    assert hierarchy_diagnostics["quad_candidates_attempted"] <= 30
    assert hierarchy_diagnostics["quint_candidates_attempted"] <= 20
    assert long_diagnostics["candidates_evaluated"] <= 100
    hierarchical_formulas = [
        formula
        for formula in long_diagnostics["formulas"]
        if formula["condition_count"] >= 4
    ]
    assert all(
        formula["condition_count"] <= hierarchy_config.hierarchical_max_conditions
        for formula in long_diagnostics["formulas"]
    )
    assert any(formula["condition_count"] == 5 for formula in hierarchical_formulas)
    for formula in hierarchical_formulas:
        validation = formula["hierarchical_validation"]
        assert validation["passed"] is True
        assert validation["discovery_incremental_score_gain"] >= 0.25
        assert validation["holdout_incremental_score_gain"] >= 0.25
        families = [
            engine.research_formula_families.feature_correlation_family(
                condition["feature"]
            )
            for condition in formula["conditions"]
        ]
        assert len(families) == len(set(families))
        assert (
            formula["multiple_testing"]["hypotheses_tested"]
            == long_diagnostics["statistical_hypotheses_tested"]
        )
        assert "evidence_family" in formula["multiple_testing"]

    four_only_result = engine.discover_formulas(
        hierarchy_rows,
        horizon_minutes=240,
        feature_schema_version="hierarchy-four-only-selftest-v1",
        config=replace(
            hierarchy_config,
            hierarchical_max_conditions=4,
            max_quad_candidates=10,
            max_quint_candidates=20,
        ),
    )
    four_only_long = next(
        direction
        for direction in four_only_result["directions"]
        if direction["direction"] == "LONG"
    )
    assert four_only_long["hierarchical_search"]["quad_candidates_attempted"] <= 10
    assert four_only_long["hierarchical_search"]["quint_candidates_attempted"] == 0
    assert all(
        formula["condition_count"] <= 4
        for formula in four_only_long["formulas"]
    )

    print("research formula engine self-test: PASS")


if __name__ == "__main__":
    run()
