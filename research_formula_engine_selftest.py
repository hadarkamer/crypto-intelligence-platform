"""Deterministic checks for automatic formula discovery and holdout safety."""

from datetime import datetime, timedelta, timezone

import research_formula_engine as engine


def _row(index: int):
    event_time = datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(minutes=index * 30)
    direction = "LONG" if index % 2 == 0 else "SHORT"
    signal = 2.0 if index % 5 in {0, 1, 2} else -2.0
    directional_return = 1.0 if signal > 0 else -0.4
    snapshot = {f"snapshot.synthetic.feature_{feature}": float((index + feature) % 11) for feature in range(24)}
    return {
        "feature_schema_version": "selftest-matrix-v1",
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
            "is_weekend_utc": event_time.weekday() >= 5,
            "fixed_utc_session_bucket": "SELFTEST",
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
        feature_schema_version="selftest-matrix-v1",
    )
    assert result["available"] is True
    assert result["discovery_sample_size"] == 98
    assert result["holdout_sample_size"] == 42
    assert result["candidates_evaluated"] >= 1000
    assert result["formulas"]
    assert result["automatic_stage_ceiling"] == "SHADOW"
    assert result["live_activation"] == "never automatic"

    formula = result["formulas"][0]
    assert formula["conditions"]
    assert formula["recommended_stage"] in {
        "DISCOVERED", "BACKTESTED", "HOLDOUT_PASSED", "SHADOW"
    }
    assert formula["live_alert_approved"] is False
    assert "sample_size" in formula["discovery_metrics"]
    assert "mae_p95_pct" in formula["holdout_metrics"]
    assert "q_value" in formula["multiple_testing"]

    features = engine.extract_decision_features(rows[0])
    assert all(not key.startswith("outcome") for key in features)
    assert "aligned.60m.price_change_pct" in features
    assert engine.formula_key(
        direction=formula["direction"],
        horizon_minutes=240,
        feature_schema_version="selftest-matrix-v1",
        conditions=list(reversed(formula["conditions"])),
    ) == formula["formula_key"]

    print("research formula engine self-test: PASS")


if __name__ == "__main__":
    run()
