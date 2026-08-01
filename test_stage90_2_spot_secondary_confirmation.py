import market_confidence_engine as m


def _module(relation, direction, score, early_shift=None):
    return {
        "relation": relation,
        "direction": direction,
        "score": score,
        "time_families": {},
        "early_shift": early_shift,
    }


def test_spot_opposition_never_vetoes_core_confirmation():
    modules = {
        "positioning": _module("SUPPORT", "BULLISH", 40),
        "futures_flow": _module("SUPPORT", "BULLISH", 35),
        "spot_flow": _module("OPPOSE", "BEARISH", -90),
    }
    out = m._conclusion(modules, "BULLISH")
    assert out["classification"] == "CORE_CONFIRMATION"
    assert out["supporting_families"] == 2
    assert out["opposing_families"] == 0
    assert out["spot_context"]["status"] == "DIVERGING"


def test_strong_confirmation_uses_two_strong_core_engines():
    modules = {
        "positioning": _module("SUPPORT", "BULLISH", 40),
        "futures_flow": _module("SUPPORT", "BULLISH", 35),
        "spot_flow": _module("NEUTRAL", "NEUTRAL", 0),
    }
    conclusion = m._conclusion(modules, "BULLISH")
    out = m._confirmation(72, "BULLISH", modules, conclusion)
    assert out["status"] == "STRONG_CONFIRMED"


def test_regular_confirmation_when_core_support_is_not_both_strong():
    modules = {
        "positioning": _module("SUPPORT", "BULLISH", 40),
        "futures_flow": _module("SUPPORT", "BULLISH", 18),
        "spot_flow": _module("SUPPORT", "BULLISH", 90),
    }
    conclusion = m._conclusion(modules, "BULLISH")
    out = m._confirmation(72, "BULLISH", modules, conclusion)
    assert out["status"] == "CONFIRMED"


def test_spot_early_shift_does_not_create_conflict():
    modules = {
        "positioning": _module("SUPPORT", "BULLISH", 40),
        "futures_flow": _module("SUPPORT", "BULLISH", 35),
        "spot_flow": _module("OPPOSE", "BEARISH", -90, {"new_direction": "BEARISH"}),
    }
    conclusion = m._conclusion(modules, "BULLISH")
    out = m._confirmation(72, "BULLISH", modules, conclusion)
    assert out["status"] == "STRONG_CONFIRMED"
