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
    state = engine._group_state("momentum", windows)
    assert state["state"] == "NEUTRAL"


def test_group_confirmed_requires_two_significant_windows():
    windows = {
        "30m": {"available": True, "evidence_level": 2, "direction": "BULLISH"},
        "1h": {"available": True, "evidence_level": 3, "direction": "BULLISH"},
    }
    state = engine._group_state("momentum", windows)
    assert state["state"] == "BULLISH_CONFIRMED"


def test_group_conflict_is_mixed():
    windows = {
        "4h": {"available": True, "evidence_level": 2, "direction": "BULLISH"},
        "12h": {"available": True, "evidence_level": 2, "direction": "BEARISH"},
        "24h": {"available": True, "evidence_level": 0, "direction": "NEUTRAL"},
    }
    assert engine._group_state("trend", windows)["state"] == "MIXED"


def test_early_shift_against_established_flow():
    groups = {
        "momentum": {"state": "BEARISH_CONFIRMED", "direction": "BEARISH"},
        "trend": {"state": "BULLISH_CONFIRMED", "direction": "BULLISH"},
        "structure": {"state": "BULLISH_CONFIRMED", "direction": "BULLISH"},
    }
    early = engine._early_shift(groups)
    assert early and early["new_direction"] == "BEARISH"


def test_quality_validates_continuous_cvd():
    rows = _rows([10] * 120)
    assert engine._quality(rows)["status"] == "PASS"
    rows[-1]["continuous_cvd"] += 2_000
    assert engine._quality(rows)["status"] == "WARNING"


def test_directional_baselines_keep_positive_and_negative_separate():
    rows = _rows(([10, 20, -100, -200] * 80))
    baseline = engine._baseline(rows, 1)
    assert baseline is not None
    assert baseline.positive is not None
    assert baseline.negative is not None
    assert baseline.negative.p50 > baseline.positive.p50


def test_quality_explains_warning_reason():
    rows = _rows([10] * 120)
    rows[-1]["continuous_cvd"] += 2_000
    quality = engine._quality(rows)
    assert quality["status"] == "WARNING"
    assert quality["reasons"]
    assert "mismatch" in quality["reasons"][0]


def test_quality_uses_practical_absolute_and_relative_tolerance():
    rows = _rows([10_000_000.0] * 400)
    independent = sum(r["delta"] for r in rows)
    # 0.005% mismatch: below the new 0.01% relative tolerance.
    rows[-1]["continuous_cvd"] = independent + independent * 0.00005
    quality = engine._quality(rows)
    assert quality["continuous_cvd_check"] is True
    assert quality["continuous_cvd_tolerance_usd"] == max(1000.0, abs(independent) * 0.0001)


def test_quality_still_rejects_large_stale_series_mismatch():
    rows = _rows([10_000_000.0] * 400)
    independent = sum(r["delta"] for r in rows)
    rows[-1]["continuous_cvd"] = 0.0
    quality = engine._quality(rows)
    assert quality["status"] == "WARNING"
    assert quality["continuous_cvd_check"] is False
