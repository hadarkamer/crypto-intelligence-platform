from datetime import datetime, timedelta, timezone

import coinglass_flow_engine as engine


def _rows(deltas, start=None):
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    cumulative = 0.0
    rows = []
    for i, delta in enumerate(deltas):
        cumulative += delta
        rows.append({
            "time": start + timedelta(minutes=30*i),
            "buy": max(delta, 0) + 100,
            "sell": max(-delta, 0) + 100,
            "delta": delta,
            "api_cvd": cumulative,
            "continuous_cvd": cumulative,
        })
    return rows


def test_percentiles_and_magnitude():
    p = engine.Percentiles(100, 10, 20, 30, 40)
    assert engine._magnitude_label(5, p) == ("NOISE", 0)
    assert engine._magnitude_label(20, p) == ("NORMAL", 1)
    assert engine._magnitude_label(35, p) == ("MEANINGFUL", 2)
    assert engine._magnitude_label(50, p) == ("STRONG", 3)


def test_group_does_not_double_count_normal_windows():
    windows = {
        "30m": {"available": True, "evidence_level": 1, "direction": "BULLISH"},
        "1h": {"available": True, "evidence_level": 1, "direction": "BULLISH"},
    }
    state = engine._group_state("short", windows)
    assert state["state"] == "NEUTRAL"


def test_group_confirmed_requires_two_significant_windows():
    windows = {
        "30m": {"available": True, "evidence_level": 2, "direction": "BULLISH"},
        "1h": {"available": True, "evidence_level": 3, "direction": "BULLISH"},
    }
    state = engine._group_state("short", windows)
    assert state["state"] == "BULLISH_CONFIRMED"


def test_group_conflict_is_mixed():
    windows = {
        "4h": {"available": True, "evidence_level": 2, "direction": "BULLISH"},
        "12h": {"available": True, "evidence_level": 2, "direction": "BEARISH"},
        "24h": {"available": True, "evidence_level": 0, "direction": "NEUTRAL"},
    }
    assert engine._group_state("medium", windows)["state"] == "MIXED"


def test_early_shift_against_established_flow():
    groups = {
        "short": {"state": "BEARISH_CONFIRMED", "direction": "BEARISH"},
        "medium": {"state": "BULLISH_CONFIRMED", "direction": "BULLISH"},
        "broad": {"state": "BULLISH_CONFIRMED", "direction": "BULLISH"},
    }
    early = engine._early_shift(groups)
    assert early and early["new_direction"] == "BEARISH"


def test_quality_validates_continuous_cvd():
    rows = _rows([10] * 120)
    assert engine._quality(rows)["status"] == "PASS"
    rows[-1]["continuous_cvd"] += 100
    assert engine._quality(rows)["status"] == "WARNING"
