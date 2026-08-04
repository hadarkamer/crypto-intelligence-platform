import market_confidence_engine as m
import time_family_engine as t


def test_extreme_rank_keeps_full_oi_quality():
    direction, strength = t.oi_window_evaluator({
        "state": "LONG_UNWINDING",
        "price_strength": {"rank": 4},
        "oi_strength": {"rank": 4},
    })
    assert direction == "BEARISH"
    assert strength == 0.65


def _module(relation, direction, score, families=None):
    return {
        "relation": relation,
        "direction": direction,
        "score": score,
        "time_families": families or {},
    }


def test_weak_spot_opposition_does_not_veto_two_core_supports():
    modules = {
        "positioning": _module("SUPPORT", "BULLISH", 40),
        "futures_flow": _module("SUPPORT", "BULLISH", 35),
        "spot_flow": _module("OPPOSE", "BEARISH", -12),
    }
    out = m._conclusion(modules, "BULLISH")
    assert out["classification"] == "CORE_CONFIRMATION"
    assert out["opposing_families"] == 0
    assert out["spot_context"]["status"] == "DIVERGING"


def test_strong_short_and_medium_spot_opposition_is_secondary_context_only():
    families = {
        "short": {"direction": "BEARISH", "members": [{"strength": 0.75}]},
        "medium": {"direction": "BEARISH", "members": [{"strength": 0.35}]},
    }
    modules = {
        "positioning": _module("SUPPORT", "BULLISH", 40),
        "futures_flow": _module("SUPPORT", "BULLISH", 35),
        "spot_flow": _module("OPPOSE", "BEARISH", -29, families),
    }
    out = m._conclusion(modules, "BULLISH")
    assert out["classification"] == "CORE_CONFIRMATION"
    assert out["opposing_families"] == 0
    assert out["spot_context"]["status"] == "DIVERGING"
