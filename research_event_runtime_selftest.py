"""Deterministic checks for the dry-run live alert mapper."""
from datetime import datetime, timedelta, timezone

import research_event_runtime as runtime


def _item(score=84.0, confirmation_status="STRONG_CONFIRMED"):
    return {
        "symbol": "BTC",
        "timeframe": "24h",
        "side": "LONG",  # liquidation side; price direction is SHORT
        "current_price": 70000.0,
        "target_price": 66500.0,
        "target_direction": "DOWN",
        "score": score,
        "priority": score,
        "average_score_all_timeframes": 80.0,
        "directional_scores_all_timeframes": {"LONG": {"24h": score}},
        "types": ["NEAR_MAX_PAIN", "TARGET_CLUSTER", "RELATIVE_GAP_ADVANTAGE"],
        "near_share_pct": 64.0,
        "components": {
            "consensus": 24.0,
            "target_proximity": 20.0,
            "cluster_confidence": 25.0,
            "relative_gap": 15.0,
        },
        "maxpain_confirmation": {
            "status": confirmation_status,
            "supporting_families": 2,
            "opposing_families": 0,
            "strong_core": confirmation_status in {"CONFIRMED", "STRONG_CONFIRMED"},
        },
        "market_evidence": {
            "modules": {
                "positioning": {
                    "family": "Price+OI", "available": True,
                    "direction": "BEARISH", "relation": "SUPPORT",
                    "score": -70.0, "quality": 0.70, "state": "BEARISH_BUILDUP",
                    "time_families": {"now": {"direction": "BEARISH", "score": -72.0, "quality": 0.8}},
                },
                "futures_flow": {
                    "family": "Futures Flow", "available": True,
                    "direction": "BEARISH", "relation": "SUPPORT",
                    "score": -72.0, "quality": 0.72, "state": "BEARISH",
                    "time_families": {"now": {"direction": "BEARISH", "score": -74.0, "quality": 0.82}},
                },
                "spot_flow": {
                    "family": "Spot Flow", "available": True,
                    "direction": "BEARISH", "relation": "SUPPORT",
                    "score": -40.0, "quality": 0.40, "state": "BEARISH",
                    "time_families": {
                        "now": {"label": "NOW", "direction": "BEARISH", "quality": 0.70, "windows": ["30m"]},
                        "short": {"label": "SHORT", "direction": "NEUTRAL", "quality": 0.20, "windows": ["1h", "4h"]},
                        "medium": {"label": "MEDIUM", "direction": "NEUTRAL", "quality": 0.10, "windows": ["12h", "24h"]},
                        "long": {"label": "LONG", "direction": "NEUTRAL", "quality": 0.10, "windows": ["48h", "72h", "7d"]},
                    },
                },
            },
        },
    }


def run():
    runtime.reset()
    t0 = datetime(2026, 8, 20, 10, 0, 0, 123456, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=30)
    t2 = t1 + timedelta(minutes=30)
    t3 = t2 + timedelta(minutes=30)

    item = _item()
    count0 = runtime.capture_special_transitions([item], event_time=t0)
    assert count0 >= 6, count0
    types0 = [event["event_type"] for event in runtime.events(30)]
    assert "MAX_PAIN_SCORE_65" in types0
    assert "STRONG_MAX_PAIN_CONFIRMATION" in types0
    assert "MAX_PAIN_SCORE_83" in types0
    assert "OI_PRICE_HIGH" in types0
    assert "FUTURES_CVD_HIGH" in types0
    assert "SPOT_CVD_HIGH" in types0

    before = len(runtime.events(200))
    runtime.capture_special_transitions([item], event_time=t1)
    after = len(runtime.events(200))
    assert after == before

    weak = _item(score=55.0, confirmation_status="UNCONFIRMED")
    weak["market_evidence"]["modules"]["positioning"]["score"] = -20.0
    weak["market_evidence"]["modules"]["futures_flow"]["score"] = -15.0
    weak["market_evidence"]["modules"]["spot_flow"]["time_families"]["now"]["quality"] = 0.30
    runtime.capture_special_transitions([weak], event_time=t2)
    latest = runtime.events(100)
    assert any(e["event_kind"] == "SIGNAL_STATE_CHANGE" for e in latest)

    # The caller can freeze decision time before Telegram network latency and
    # pass it into the post-delivery capture.
    runtime.capture_sent_maxpain(item, event_time=t0)
    runtime.capture_sent_maxpain(item, event_time=t1)
    sent = [e for e in runtime.events(200) if e["event_type"] == "MAX_PAIN_ALERT"]
    assert len(sent) == 2
    assert sent[0]["setup_key"] == sent[1]["setup_key"]
    assert sent[0]["event_fingerprint"] != sent[1]["event_fingerprint"]
    assert sent[0]["alert_time_utc"].startswith("2026-08-20T10:00:00.123456")

    token = runtime.set_watch_context(
        watch_scan_id="shared-watch:test", watch_cycle_number=7
    )
    try:
        runtime.capture_sent_maxpain(item, event_time=t3)
    finally:
        runtime.reset_watch_context(token)
    contextual = [
        e for e in runtime.events(200)
        if e["event_type"] == "MAX_PAIN_ALERT"
        and e["alert_time_utc"].startswith("2026-08-20T11:30:00")
    ][-1]
    assert contextual["engine_snapshot"]["watch_scan_id"] == "shared-watch:test"
    assert contextual["engine_snapshot"]["watch_cycle_number"] == 7
    assert len(contextual["engine_snapshot"]["sheet_snapshot_id"]) == 64

    combined = {
        "key": "BTC|LONG",
        "symbol": "BTC",
        "side": "LONG",
        "signal_keys": {"strong_confirmation:24h", "score_over_80:24h", "derivatives_high:futures_flow"},
        "signal_count": 3,
        "normal_confirmations": [],
        "strong_confirmations": [{"timeframe": "24h", "score": 84.0}],
        "high_scores": [{"timeframe": "24h", "score": 84.0}],
        "anomaly_setups": [],
        "liquidity_imbalances": [],
        "derivatives_high": [{"title": "Futures CVD", "score": -72.0}],
        "magnet": None,
        "top_item": item,
    }

    # First observation seeds the research state without inventing a weakening event.
    assert runtime.capture_combined_state_changes([combined], event_time=t1) == 0
    assert runtime.capture_combined_confirmation(combined, event_time=t1)
    combined_event = [e for e in runtime.events(200) if e["event_type"] == "COMBINED_CONFIRMATION"][-1]
    assert combined_event["direction"] == "SHORT"
    assert combined_event["source_side"] == "LONG"
    assert combined_event["engine_snapshot"]["signal_count"] == 3

    # Losing one component while still active must be research-visible even though
    # Telegram does not emit a new Combined alert for weakening.
    weakened = dict(combined)
    weakened["signal_keys"] = {"strong_confirmation:24h", "score_over_80:24h"}
    weakened["signal_count"] = 2
    assert runtime.capture_combined_state_changes([weakened], event_time=t2) == 1
    state_events = [e for e in runtime.events(300) if e["event_type"] == "COMBINED_CONFIRMATION_STATE"]
    assert state_events[-1]["engine_snapshot"]["evidence"]["removed"] == ["derivatives_high:futures_flow"]

    # Complete disappearance must also be recorded.
    assert runtime.capture_combined_state_changes([], event_time=t3) == 1
    state_events = [e for e in runtime.events(300) if e["event_type"] == "COMBINED_CONFIRMATION_STATE"]
    assert state_events[-1]["engine_snapshot"]["new_state"]["active"] is False

    original_build = runtime.magnet_v1.build_magnets
    original_expected = runtime.magnet_v1.expected_price_direction
    original_evaluate = runtime.magnet_v1.evaluate_confirmation
    original_combine = runtime.market_confidence_engine.combine
    try:
        runtime.magnet_v1.build_magnets = lambda rows, symbol=None: [{
            "symbol": "SOL", "side": "UPPER", "count": 3,
            "members": ["12h", "24h", "48h"],
            "min_target": 180.0, "max_target": 181.0, "average_target": 180.4,
            "spread_pct": 0.55, "magnet_quality": 82.5,
            "liquidity_edge_pct": 18.0,
        }]
        runtime.magnet_v1.expected_price_direction = lambda side: "BULLISH"
        runtime.market_confidence_engine.combine = lambda *args, **kwargs: {"modules": {}, "confirmation": {}}
        runtime.magnet_v1.evaluate_confirmation = lambda magnet, evidence: {
            "status": "STRONG_CONFIRMED",
            "magnet_quality": 82.5,
            "liquidity_edge_pct": 18.0,
            "liquidity_status": "SUPPORT",
            "derivatives": {"status": "CONFIRMED", "positioning_score": 50.0, "futures_score": 55.0},
        }
        rows = [{"symbol": "SOL", "current_price": 170.0}]
        snapshot = {"SOL": {"regime": {}, "flow": {}}}
        assert runtime.capture_magnet_watch_symbol("SOL", rows, snapshot, event_time=t0) >= 1
        magnet_events = [e for e in runtime.events(300) if e["event_type"] == "STRONG_MAGNET_CONFIRMATION"]
        assert magnet_events
        assert magnet_events[-1]["source_side"] == "UPPER"
    finally:
        runtime.magnet_v1.build_magnets = original_build
        runtime.magnet_v1.expected_price_direction = original_expected
        runtime.magnet_v1.evaluate_confirmation = original_evaluate
        runtime.market_confidence_engine.combine = original_combine

    status = runtime.status()
    assert status["database_writes"] is False
    assert status["mode"] == "DRY_RUN_MEMORY_ONLY"
    assert status["tracked_combined_states"] == 0
    print("Research runtime mapper self-test: PASS")
    print("events:", status["events"])
    print("database writes:", status["database_writes"])


if __name__ == "__main__":
    run()
