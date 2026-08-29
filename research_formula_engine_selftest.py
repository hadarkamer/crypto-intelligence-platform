"""Deterministic checks for automatic formula discovery and holdout safety."""

from datetime import datetime, timedelta, timezone

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
            "mfe_pct": 1.8 if directional_return > 0 else 0.4,
            "mae_pct": 0.2 if directional_return > 0 else 1.1,
            "time_to_first_progress_seconds": 120 if directional_return > 0 else 1800,
            "time_to_mfe_seconds": 900,
            "target_progress_ratio": 0.9 if directional_return > 0 else 0.2,
            "target_reached": directional_return > 0,
        },
    }


def run() -> None:
    rows = [_row(index) for index in range(140)]
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

    # Weekend calibration changes only the absolute width floor. The hit,
    # Wilson, improvement, risk and percentile gates remain unchanged.
    assert engine.minimum_wide_move_pct(
        240, {"movement_width_floor_scale_factor": 0.60}
    ) == 0.60

    print("research formula engine self-test: PASS")


if __name__ == "__main__":
    run()
