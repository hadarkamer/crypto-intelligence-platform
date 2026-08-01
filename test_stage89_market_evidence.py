import market_confidence_engine as m

def _regime(state="BULLISH_BUILDUP", agreement=8, quality="PASS"):
    labels={"BULLISH_BUILDUP":"Bullish Build-up","BEARISH_BUILDUP":"Bearish Build-up"}
    return {
        "available":True,"data_quality_status":quality,
        "overall":{"state":state,"label":labels.get(state,state),"agreement":agreement,"valid_windows":8},
    }

def _market(state="BULLISH_CONFIRMED", direction="BULLISH", quality="PASS"):
    return {
        "available":True,
        "quality":{"status":quality,"reasons":[]},
        "overall":{"state":state,"direction":direction},
    }

def test_all_three_support_long_is_full_confirmation():
    out=m.combine("BTC","LONG",_regime(),{"futures":_market(),"spot":_market()})
    assert out["counts"]["BULLISH"] == 3
    assert out["classification"] == "FULL_CONFIRMATION"
    assert out["relation_to_alert"] == "SUPPORT"

def test_all_three_oppose_long_is_conflict():
    flow={"futures":_market("BEARISH_CONFIRMED","BEARISH"),"spot":_market("BEARISH_CONFIRMED","BEARISH")}
    out=m.combine("BTC","LONG",_regime("BEARISH_BUILDUP"),flow)
    assert out["counts"]["BEARISH"] == 3
    assert out["classification"] == "FULL_CONFIRMATION"
    assert out["relation_to_alert"] == "CONFLICT"

def test_one_support_two_neutral_is_weak_evidence():
    flow={"futures":_market("NEUTRAL","NEUTRAL"),"spot":_market("NEUTRAL","NEUTRAL")}
    out=m.combine("BTC","LONG",_regime(),flow)
    assert out["counts"] == {"BULLISH":1,"BEARISH":0,"NEUTRAL":2,"MIXED":0}
    assert out["classification"] == "WEAK_EVIDENCE"

def test_max_pain_side_is_inverted_to_price_direction():
    assert m.max_pain_side_to_price_direction("SHORT") == "LONG"
    assert m.max_pain_side_to_price_direction("LONG") == "SHORT"
