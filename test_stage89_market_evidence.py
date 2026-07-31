import market_confidence_engine as m


def _regime(state="BULLISH_BUILDUP", agreement=5, quality="PASS"):
    labels={
        "BULLISH_BUILDUP":"Bullish Build-up",
        "BEARISH_BUILDUP":"Bearish Build-up",
        "SHORT_COVERING":"Short Covering",
        "LONG_UNWINDING":"Long Unwinding",
    }
    return {
        "available": True,
        "data_quality_status": quality,
        "overall": {"state":state,"label":labels.get(state,state),"agreement":agreement,"valid_windows":5},
    }


def _market(state="BULLISH_CONFIRMED", direction="BULLISH", quality="PASS"):
    return {
        "available": True,
        "quality": {"status": quality, "reasons": []},
        "overall": {"state":state,"direction":direction},
    }


def test_all_three_support_long_is_strong_support():
    flow={"futures":_market(),"spot":_market()}
    out=m.combine("BTC","LONG",_regime(),flow)
    assert out["alignment_score"] == 100.0
    assert out["classification"] == "STRONG_SUPPORT"


def test_all_three_oppose_long_is_strong_conflict():
    flow={
        "futures":_market("BEARISH_CONFIRMED","BEARISH"),
        "spot":_market("BEARISH_CONFIRMED","BEARISH"),
    }
    out=m.combine("BTC","LONG",_regime("BEARISH_BUILDUP"),flow)
    assert out["alignment_score"] == -100.0
    assert out["classification"] == "STRONG_CONFLICT"


def test_neutral_flow_is_not_counted_as_extra_vote():
    flow={
        "futures":_market("NEUTRAL","NEUTRAL"),
        "spot":_market("NEUTRAL","NEUTRAL"),
    }
    out=m.combine("BTC","LONG",_regime(),flow)
    assert out["alignment_score"] == 40.0
    assert out["classification"] == "SUPPORT"


def test_max_pain_side_is_inverted_to_price_direction():
    assert m.max_pain_side_to_price_direction("SHORT") == "LONG"
    assert m.max_pain_side_to_price_direction("LONG") == "SHORT"
