"""Deterministic self-test for the candidate Research Event capture layer."""
from datetime import datetime, timedelta, timezone

from research_event_capture import (
    DryRunResearchCapture,
    build_generic_alert_event,
    build_magnet_event,
    build_maxpain_event,
    build_signal_state_change,
)


def _maxpain_item():
    return {
        "symbol": "BTC",
        "timeframe": "24h",
        "side": "LONG",  # liquidation side; target is below price
        "current_price": 70000.0,
        "target_price": 66500.0,
        "target_direction": "DOWN",
        "score": 82.5,
        "priority": 82.5,
        "types": ["RELATIVE_GAP_ADVANTAGE", "LIQUIDITY_BALANCE_SUPPORT"],
        "components": {
            "consensus": 24.0,
            "target_proximity": 20.0,
            "cluster_confidence": 25.0,
            "relative_gap": 13.5,
        },
        "opposite_score": 41.0,
        "directional_edge": 41.5,
        "consensus_hits": 5,
        "consensus_total": 6,
        "balance": {"near_share_pct": 64.0},
        "cluster": {
            "points": 25.0,
            "count": 4,
            "spread_pct": 0.65,
            "average_target": 66520.0,
            "members": ["12h", "24h", "48h", "3d"],
        },
        "gap": {"points": 13.5, "advantage": 0.9, "near_distance": 5.0, "far_distance": 50.0},
        "maxpain_confirmation": {
            "status": "STRONG_CONFIRMED",
            "score_threshold": 65.0,
            "strong_score_threshold": 75.0,
            "supporting_families": 2,
            "opposing_families": 0,
            "strong_core": True,
        },
        "market_evidence": {
            "expected_price_direction": "BEARISH",
            "classification": "CORE_CONFIRMATION",
            "relation_to_alert": "SUPPORT",
            "core_supporting_families": 2,
            "core_opposing_families": 0,
            "modules": {
                "positioning": {
                    "family": "Price+OI", "direction": "BEARISH", "relation": "SUPPORT",
                    "score": -58.0, "state": "BEARISH_BUILDUP", "time_families": {"huge": "excluded"},
                },
                "futures_flow": {
                    "family": "Futures Flow", "direction": "BEARISH", "relation": "SUPPORT",
                    "score": -62.0, "state": "BEARISH", "windows": {"huge": "excluded"},
                },
                "spot_flow": {
                    "family": "Spot Flow", "direction": "BULLISH", "relation": "OPPOSE",
                    "score": 31.0, "state": "BULLISH",
                },
            },
        },
        "price_source": "binance",
        "price_pair": "BTCUSDT",
    }


def run() -> None:
    t0 = datetime(2026, 8, 20, 10, 0, 0, 123456, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=30)

    event0 = build_maxpain_event(
        _maxpain_item(),
        event_type="STRONG_MAX_PAIN_CONFIRMATION",
        event_time=t0,
        strategy_version="candidate-test",
        code_version="abc123",
    )
    event1 = build_maxpain_event(
        _maxpain_item(),
        event_type="STRONG_MAX_PAIN_CONFIRMATION",
        event_time=t1,
        strategy_version="candidate-test",
        code_version="abc123",
    )
    event0_replay = build_maxpain_event(
        _maxpain_item(),
        event_type="STRONG_MAX_PAIN_CONFIRMATION",
        event_time=t0,
        strategy_version="candidate-test",
        code_version="abc123",
    )

    assert event0.direction == "SHORT", "Max-Pain expected PRICE direction must be stored, not liquidation side"
    assert event0.engine_snapshot["alert_side"] == "LONG"
    assert event0.current_price == 70000.0 and event0.target_price == 66500.0
    assert round(event0.initial_target_distance_pct or 0, 3) == 5.0
    assert event0.engine_snapshot["score_components"]["consensus"] == 24.0
    assert event0.engine_snapshot["maxpain_confirmation"]["status"] == "STRONG_CONFIRMED"
    assert event0.engine_snapshot["market_evidence"]["modules"]["positioning"]["score"] == -58.0
    assert "time_families" not in event0.engine_snapshot["market_evidence"]["modules"]["positioning"]
    assert "windows" not in event0.engine_snapshot["market_evidence"]["modules"]["futures_flow"]

    # Repeated occurrences must group together but remain distinct events.
    assert event0.setup_key == event1.setup_key
    assert event0.event_fingerprint != event1.event_fingerprint
    assert event0.event_fingerprint == event0_replay.event_fingerprint
    assert event0.alert_time_utc.endswith("Z") and ".123456Z" in event0.alert_time_utc

    magnet = {
        "symbol": "SOL",
        "side": "UPPER",
        "count": 4,
        "members": ["12h", "24h", "48h", "3d"],
        "min_target": 180.0,
        "max_target": 181.0,
        "average_target": 180.4,
        "spread_pct": 0.55,
        "magnet_quality": 82.5,
        "liquidity_edge_pct": 18.0,
        "gross_liquidity_timeframe": "3d",
    }
    magnet_conf = {
        "status": "STRONG_CONFIRMED",
        "magnet_quality": 82.5,
        "liquidity_edge_pct": 18.0,
        "liquidity_status": "SUPPORT",
        "derivatives": {"status": "CONFIRMED", "positioning_score": 44.0, "futures_score": 52.0},
    }
    magnet_event = build_magnet_event(
        magnet,
        confirmation=magnet_conf,
        current_price=170.0,
        event_time=t0,
        strategy_version="candidate-test",
        code_version="abc123",
    )
    assert magnet_event.event_type == "STRONG_MAGNET_CONFIRMATION"
    assert magnet_event.direction == "LONG"
    assert magnet_event.score == 82.5
    assert magnet_event.target_price == 180.4
    assert magnet_event.engine_snapshot["magnet_confirmation"]["derivatives"]["futures_score"] == 52.0

    state_change = build_signal_state_change(
        symbol="BTC",
        signal_name="FUTURES_CVD_HIGH",
        old_state={"direction": "LONG", "score": 68.0},
        new_state={"direction": "NEUTRAL", "score": 20.0},
        direction="SHORT",
        score=20.0,
        current_price=69900.0,
        event_time=t1,
        evidence={"reason": "decayed below reset threshold"},
        strategy_version="candidate-test",
        code_version="abc123",
    )
    assert state_change.event_kind == "SIGNAL_STATE_CHANGE"
    assert state_change.engine_snapshot["old_state"]["score"] == 68.0
    assert state_change.engine_snapshot["new_state"]["score"] == 20.0

    combined = build_generic_alert_event(
        symbol="BTC",
        event_type="COMBINED_CONFIRMATION",
        direction="SHORT",
        event_time=t0,
        score=84.0,
        categories=["MAX_PAIN_CONFIRMATION", "FUTURES_CVD_HIGH", "OI_PRICE"],
        engine_snapshot={
            "signal_keys": ["MAX_PAIN_CONFIRMATION", "FUTURES_CVD_HIGH", "OI_PRICE"],
            "component_scores": {"maxpain": 84.0, "futures_cvd": -68.0, "oi_price": -55.0},
        },
        setup_identity={"signal_families": ["MAX_PAIN", "FUTURES_CVD", "OI_PRICE"]},
        strategy_version="candidate-test",
        code_version="abc123",
    )
    assert "COMBINED_CONFIRMATION" == combined.event_type
    assert len(combined.categories) == 3

    sink = DryRunResearchCapture(max_events=10)
    assert sink.status()["database_writes"] is False
    assert sink.emit(event0) is True
    assert sink.emit(event0_replay) is False, "exact replay must be idempotent"
    assert sink.emit(event1) is True, "later repeated occurrence must be retained"
    assert sink.emit(magnet_event) is True
    assert sink.emit(state_change) is True
    assert sink.emit(combined) is True
    assert len(sink.events()) == 5

    # State changes with no actual transition are invalid.
    try:
        build_signal_state_change(
            symbol="BTC", signal_name="OI_PRICE", old_state="LONG", new_state="LONG", event_time=t0
        )
    except ValueError:
        pass
    else:
        raise AssertionError("equal old/new state must not produce a state-change event")

    print("Research Event capture self-test: PASS")
    print("Dry-run events:", len(sink.events()))
    print("Repeated setup preserved:", event0.setup_key == event1.setup_key)
    print("Distinct occurrence fingerprints:", event0.event_fingerprint != event1.event_fingerprint)
    print("Database writes:", sink.status()["database_writes"])


if __name__ == "__main__":
    run()
