import coinglass_oi_regime_service as svc


def w(state, available=True):
    return {"state": state, "available": available}


def test_four_neutral_one_directional_is_neutral_not_mixed():
    windows = {
        "30m": w("NEUTRAL_INCONCLUSIVE"),
        "1h": w("NEUTRAL_INCONCLUSIVE"),
        "4h": w("SHORT_COVERING"),
        "12h": w("NEUTRAL_INCONCLUSIVE"),
        "24h": w("NEUTRAL_INCONCLUSIVE"),
    }
    o = svc._overall(windows)
    assert o["state"] == "NEUTRAL_INCONCLUSIVE"
    assert o["agreement"] == 4
    assert o["valid_windows"] == 5


def test_three_same_directional_is_confirmed():
    windows = {
        "30m": w("BULLISH_BUILDUP"), "1h": w("BULLISH_BUILDUP"),
        "4h": w("BULLISH_BUILDUP"), "12h": w("NEUTRAL_INCONCLUSIVE"),
        "24h": w("NEUTRAL_INCONCLUSIVE"),
    }
    o=svc._overall(windows)
    assert o["state"] == "BULLISH_BUILDUP"
    assert o["agreement"] == 3


def test_split_directional_without_majority_is_mixed():
    windows = {
        "30m": w("BULLISH_BUILDUP"), "1h": w("BULLISH_BUILDUP"),
        "4h": w("BEARISH_BUILDUP"), "12h": w("BEARISH_BUILDUP"),
        "24h": w("NEUTRAL_INCONCLUSIVE"),
    }
    o=svc._overall(windows)
    assert o["state"] == "MIXED_TRANSITION"
    assert o["agreement"] == 2


def test_composite_neutral_does_not_change_alert_score():
    regime={"overall":{"state":"NEUTRAL_INCONCLUSIVE","label":"Neutral / Inconclusive","agreement":4}}
    text=svc.composite_conclusion(regime,"LONG")
    assert "הציון הקיים נשאר עצמאי" in text
