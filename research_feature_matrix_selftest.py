"""Deterministic checks for the no-lookahead research feature matrix."""

from datetime import datetime, timedelta, timezone

import research_feature_matrix as matrix


def _price(symbol, timestamp, price, oi, source="selftest"):
    return {
        "symbol": symbol,
        "candle_time": timestamp,
        "price_close": price,
        "oi_close_usd": oi,
        "price_exchange": "Binance",
        "price_pair": f"{symbol}USDT",
        "source": source,
    }


def _flow(symbol, timestamp, continuous, api, buy=60.0, sell=40.0):
    return {
        "symbol": symbol,
        "candle_time": timestamp,
        "buy_volume_usd": buy,
        "sell_volume_usd": sell,
        "api_cum_vol_delta_usd": api,
        "continuous_cum_vol_delta_usd": continuous,
        "exchange_list": "Binance,OKX,Bybit",
        "source": "selftest",
    }


def run() -> None:
    # The production session contract is New York local time, including DST.
    # Summer: Friday 20:00 ET = Saturday 00:00 UTC; Sunday 18:00 ET = 22:00 UTC.
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 28, 23, 59, tzinfo=timezone.utc)
    )
    assert not matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
    )
    assert not matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 30, 21, 59, tzinfo=timezone.utc)
    )
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 30, 22, 0, tzinfo=timezone.utc)
    )
    active_ratio, weekend_ratio, _ = matrix.market_session_baseline.session_ratios(
        datetime(2026, 8, 28, 23, 30, tzinfo=timezone.utc),
        datetime(2026, 8, 29, 0, 30, tzinfo=timezone.utc),
    )
    assert active_ratio == 0.5 and weekend_ratio == 0.5
    boundary_event = datetime(2026, 8, 29, 0, 30, tzinfo=timezone.utc)
    boundary_windows = {
        minutes: matrix._window_features(
            event_time=boundary_event,
            minutes=minutes,
            price_series=None,
            futures_series=None,
            spot_series=None,
        )
        for minutes in matrix.CORE_WINDOWS_MINUTES
    }
    assert boundary_windows[30]["session_active_ratio"] == 0.0
    assert boundary_windows[60]["session_active_ratio"] == 0.5
    assert boundary_windows[240]["session_active_ratio"] == 0.875
    assert boundary_windows[720]["session_active_ratio"] == 0.958333
    assert boundary_windows[1440]["session_active_ratio"] == 0.979167

    # Winter boundaries move one UTC hour later and must remain exact.
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 12, 5, 0, 59, tzinfo=timezone.utc)
    )
    assert not matrix.market_session_baseline.is_active_market(
        datetime(2026, 12, 5, 1, 0, tzinfo=timezone.utc)
    )
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 12, 6, 23, 0, tzinfo=timezone.utc)
    )

    class _Fetched:
        def fetchall(self):
            return [{"event_id": 1, "symbol": "BTC"}]

    class _Connection:
        def execute(self, query, params):
            assert query.count("%s") == len(params)
            assert "e.symbol=%s" in query
            assert "e.event_type=%s" in query
            assert "e.direction=%s" in query
            return _Fetched()

    loaded = matrix._load_verified_events(
        _Connection(),
        symbol="BTC",
        event_type="SELFTEST",
        direction="LONG",
        lookback_days=30,
        horizon_minutes=240,
        limit=100,
    )
    assert loaded == [{"event_id": 1, "symbol": "BTC"}]

    event_time = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    reference_time = event_time - timedelta(minutes=61)
    current_time = event_time - timedelta(minutes=1)
    future_time = event_time + timedelta(minutes=1)

    event = {
        "event_id": 77,
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "source_side": "SHORT",
        "timeframe": "1h",
        "event_type": "COMBINED_CONFIRMATION",
        "score": 82.5,
        "current_price": 102.0,
        "target_price": 104.0,
        "initial_target_distance_pct": 1.960784,
        "categories": ["OI_PRICE", "FUTURES_CVD_HIGH", "SPOT_CVD_HIGH"],
        "setup_key": "same-setup",
        "strategy_version": "selftest-v1",
        "code_version": "abc123",
        "engine_snapshot": {
            "component_scores": {
                "oi_price": 70.0,
                "futures_cvd": 65.0,
                "spot_cvd": 75.0,
            },
            "market_evidence": {
                "classification": "CORE_CONFIRMATION",
                "supporting_families": 3,
            },
        },
        "horizon_minutes": 240,
        "measured_at_utc": event_time + timedelta(hours=4),
        "reference_price": 102.0,
        "price_at_horizon": 105.0,
        "raw_return_pct": 2.941176,
        "directional_return_pct": 2.941176,
        "mfe_pct": 4.0,
        "mae_pct": 0.4,
        "time_to_first_progress_seconds": 120,
        "time_to_mfe_seconds": 1800,
        "time_to_closest_target_seconds": 900,
        "time_to_target_seconds": 1200,
        "target_progress_ratio": 1.5,
        "target_reached": True,
        "path_samples": 240,
        "outcome_method_version": matrix.VERIFIED_OUTCOME_METHOD,
        "data_quality_status": matrix.VERIFIED_OUTCOME_QUALITY,
    }

    historical_price_rows = []
    historical_futures_rows = []
    historical_spot_rows = []
    for index in range(1, matrix.HISTORICAL_BASELINE_MIN_SAMPLES + 1):
        week = (index + 1) // 2
        anchor = event_time - timedelta(days=7 * week) + timedelta(
            hours=2 * (index % 2)
        )
        prior = anchor - timedelta(minutes=60)
        historical_price_rows.extend(
            [
                _price("BTC", prior, 100.0 + index, 1000.0 + index),
                _price("BTC", anchor, 101.0 + index, 1010.0 + index),
            ]
        )
        historical_futures_rows.extend(
            [
                _flow("BTC", prior, 10.0 * index, 5.0 * index),
                _flow("BTC", anchor, 10.0 * index + 5.0, 5.0 * index + 2.0),
            ]
        )
        historical_spot_rows.extend(
            [
                _flow("BTC", prior, 8.0 * index, 4.0 * index),
                _flow("BTC", anchor, 8.0 * index + 3.0, 4.0 * index + 1.0),
            ]
        )

    price_rows = historical_price_rows + [
        _price("BTC", reference_time, 100.0, 1000.0, "oi_price_history"),
        _price(
            "BTC",
            current_time,
            102.0,
            1100.0,
            "oi_regime_snapshots:coinglass",
        ),
        _price("BTC", future_time, 999.0, 9999.0),
    ]
    futures_rows = historical_futures_rows + [
        _flow("BTC", reference_time, 100.0, 10.0),
        _flow("BTC", current_time, 200.0, 30.0),
        _flow("BTC", future_time, 9999.0, 9999.0),
    ]
    spot_rows = historical_spot_rows + [
        _flow("BTC", reference_time, 50.0, 5.0),
        _flow("BTC", current_time, 90.0, 15.0),
        _flow("BTC", future_time, -9999.0, -9999.0),
    ]
    prior_events = [
        {
            "event_id": 70,
            "alert_time_utc": event_time - timedelta(minutes=20),
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "COMBINED_CONFIRMATION",
            "setup_key": "same-setup",
        },
        {
            "event_id": 71,
            "alert_time_utc": event_time - timedelta(minutes=10),
            "symbol": "ETH",
            "direction": "SHORT",
            "event_type": "MAX_PAIN_CONFIRMATION",
            "setup_key": "other",
        },
        {
            "event_id": 72,
            "alert_time_utc": event_time,
            "symbol": "SOL",
            "direction": "LONG",
            "event_type": "SAME_TIMESTAMP_MUST_NOT_COUNT",
            "setup_key": "other",
        },
        {
            "event_id": 73,
            "alert_time_utc": future_time,
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "FUTURE_MUST_NOT_COUNT",
            "setup_key": "same-setup",
        },
    ]

    rows = matrix.build_feature_rows(
        [event],
        price_oi_rows=price_rows,
        futures_rows=futures_rows,
        spot_rows=spot_rows,
        prior_events=prior_events,
        windows_minutes=(60, 240),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["feature_schema_version"] == matrix.FEATURE_SCHEMA_VERSION

    latest = row["raw_features"]["latest_at_or_before_alert"]
    assert latest["price_oi"]["price_close"] == 102.0
    assert latest["price_oi"]["oi_close_usd"] == 1100.0
    assert latest["price_oi"]["source"] == "oi_regime_snapshots:coinglass"
    assert latest["futures_cvd"]["continuous_cvd_usd"] == 200.0
    assert latest["spot_cvd"]["continuous_cvd_usd"] == 90.0

    one_hour = row["raw_features"]["windows"]["60m"]
    assert one_hour["price_change_pct"] == 2.0
    assert one_hour["oi_change_pct"] == 10.0
    assert one_hour["futures_continuous_cvd_change_usd"] == 100.0
    assert one_hour["spot_continuous_cvd_change_usd"] == 40.0
    assert one_hour["spot_futures_alignment"] == "ALIGNED"
    assert one_hour["price_oi_state"] == "PRICE_UP__OI_UP"
    assert one_hour["session_active_ratio"] == 0.0
    assert one_hour["session_weekend_ratio"] == 1.0
    assert one_hour["session_composition"] == "WEEKEND_ONLY"
    assert one_hour["complete"] is True
    assert row["raw_features"]["windows"]["240m"]["complete"] is False

    # The values from 12:01 are intentionally extreme; none may enter a
    # decision-time feature for the alert at 12:00.
    serialized_inputs = str(
        {
            "raw": row["raw_features"],
            "historical": row["historical_context"],
            "model": row["model_features"],
            "sequence": row["sequence_features"],
        }
    )
    assert "9999" not in serialized_inputs and "-9999" not in serialized_inputs

    model = row["model_features"]
    assert model["alert_score"] == 82.5
    assert model["snapshot_features"]["snapshot.component_scores.spot_cvd"] == 75.0
    assert (
        model["snapshot_features"]["snapshot.market_evidence.classification"]
        == "CORE_CONFIRMATION"
    )

    sequence = row["sequence_features"]["30m"]
    assert sequence["same_symbol_alerts"] == 1
    assert sequence["same_symbol_same_direction"] == 1
    assert sequence["same_setup_repetitions"] == 1
    assert sequence["market_alerts"] == 2
    assert sequence["market_distinct_symbols"] == 2
    assert sequence["market_direction_balance_pct"] == 0.0

    assert row["time_features"]["utc_hour"] == 12
    assert row["time_features"]["market_session"] == "WEEKEND"
    assert row["time_features"]["market_regime"] == "WEEKEND"
    historical_60m = row["historical_context"]["windows"]["60m"]
    assert historical_60m["session_composition"] == "WEEKEND_ONLY"
    assert historical_60m["price_change_pct_history_samples"] >= 30
    assert historical_60m["price_change_pct_percentile_session_matched"] is not None
    assert (
        historical_60m["price_change_pct_session_matched_effective_samples"]
        >= 30
    )
    assert historical_60m["sufficient_history"] is True
    assert row["outcome_label"]["mfe_pct"] == 4.0
    assert row["outcome_label"]["mae_pct"] == 0.4
    assert row["outcome_label"]["session_active_ratio"] == 0.0
    assert row["outcome_label"]["session_weekend_ratio"] == 1.0
    assert row["outcome_label"]["session_composition"] == "WEEKEND_ONLY"
    assert (
        row["outcome_label"]["movement_width_reference"]["floor_scale_factor"]
        == 1.0
    )
    assert "outcome_label" not in row["model_features"]

    prepared = matrix._prepare_series(price_rows, time_column="candle_time")
    prior, age = matrix._prior_point(prepared["BTC"], event_time)
    assert prior["price_close"] == 102.0 and age == 1.0
    missing, missing_age = matrix._prior_point(
        prepared["BTC"], event_time + timedelta(hours=2)
    )
    assert missing is None and missing_age is None

    print("research feature matrix self-test: OK")


if __name__ == "__main__":
    run()
