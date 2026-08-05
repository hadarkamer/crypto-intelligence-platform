from datetime import datetime, timedelta, timezone

import coinglass_flow_engine as engine
import coinglass_flow_foundation as foundation
import market_confidence_engine as confidence


def _row(open_time, delta=100.0, cumulative=100.0):
    return {
        "time": open_time,
        "buy": max(delta, 0.0),
        "sell": max(-delta, 0.0),
        "delta": delta,
        "api_cvd": cumulative,
        "continuous_cvd": cumulative,
    }


def test_exact_30_minute_ceiling_from_candle_close(monkeypatch):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    candle_open = now - timedelta(minutes=60)
    assert foundation.candle_age_minutes(candle_open, now) == 30.0
    assert foundation.candle_age_minutes(candle_open - timedelta(seconds=1), now) > 30.0


def test_stale_quality_is_not_usable_for_confirmation(monkeypatch):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    rows = []
    cumulative = 0.0
    for i in range(120):
        cumulative += 100.0
        rows.append(_row(now - timedelta(minutes=30 * (120 - i + 2)), 100.0, cumulative))
    quality = engine._quality(rows)
    assert quality["stale"] is True
    assert quality["freshness_status"] == "STALE"
    assert quality["usable_for_confirmation"] is False


def test_flow_module_excludes_stale_data():
    data = {
        "available": True,
        "quality": {
            "status": "PASS",
            "freshness_status": "STALE",
            "usable_for_confirmation": False,
            "age_minutes": 31.0,
        },
        "windows": {},
        "overall": {"direction": "BULLISH", "state": "BULLISH_EVIDENCE"},
    }
    module = confidence._flow_module(data, "Futures Flow", "BULLISH")
    assert module["available"] is False
    assert module["relation"] == "NEUTRAL"
    assert module["freshness_status"] == "STALE"
