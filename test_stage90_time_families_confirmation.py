import market_confidence_engine as confidence
import time_family_engine as families


def _oi_window(state):
    return {"available": True, "state": state}


def _flow_window(direction, level=3):
    return {"available": True, "direction": direction, "evidence_level": level}


def test_fixed_time_family_layout_and_weights():
    assert families.TIME_FAMILIES["now"]["windows"] == ("30m",)
    assert families.TIME_FAMILIES["short"]["windows"] == ("1h", "4h")
    assert families.TIME_FAMILIES["medium"]["windows"] == ("12h", "24h")
    assert families.TIME_FAMILIES["long"]["windows"] == ("48h", "72h", "7d")
    assert sum(x["weight"] for x in families.TIME_FAMILIES.values()) == 100.0


def test_internal_conflict_reduces_family_quality():
    windows = {"1h": _flow_window("BULLISH"), "4h": _flow_window("BEARISH")}
    result = families.aggregate(windows, families.flow_window_evaluator)
    short = result["families"]["short"]
    assert short["direction"] == "NEUTRAL"
    assert short["quality"] == 0.0
    assert short["agreement"] == 0.0


def test_confirmation_two_of_three_without_opposition():
    regime = {
        "available": True,
        "data_quality_status": "PASS",
        "windows": {"30m": _oi_window("BULLISH_BUILDUP")},
        "overall": {"state": "BULLISH_BUILDUP", "label": "Bullish Build-up"},
        "early_transition": False,
    }
    flow = {
        "futures": {
            "available": True, "quality": {"status": "PASS"},
            "windows": {"30m": _flow_window("BULLISH")}, "overall": {}, "early_shift": None,
        },
        "spot": {"available": False, "quality": {"status": "NO_DATA"}, "windows": {}, "overall": {}},
    }
    out = confidence.combine("BTC", "LONG", regime, flow, maxpain_score=72)
    assert out["supporting_families"] == 2
    assert out["opposing_families"] == 0
    assert out["confirmation"]["status"] == "STRONG_CONFIRMED"


def test_confirmation_requires_score_70():
    out = confidence.combine("BTC", "LONG", {"available": False}, {"futures": {}, "spot": {}}, maxpain_score=69.9)
    assert out["confirmation"]["status"] == "BELOW_SCORE"
