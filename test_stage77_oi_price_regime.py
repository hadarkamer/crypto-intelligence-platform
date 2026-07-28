import coinglass_oi_regime_service as regime


def test_five_price_oi_states_use_direction_only():
    assert regime.classify("BTC", 0.000001, 0.000001).state == "BULLISH_BUILDUP"
    assert regime.classify("BTC", -0.000001, 0.000001).state == "BEARISH_BUILDUP"
    assert regime.classify("BTC", 0.000001, -0.000001).state == "SHORT_COVERING"
    assert regime.classify("BTC", -0.000001, -0.000001).state == "LONG_UNWINDING"
    assert regime.classify("BTC", 0.0, 1.0).state == "NEUTRAL_INCONCLUSIVE"
    assert regime.classify("BTC", 1.0, 0.0).state == "NEUTRAL_INCONCLUSIVE"


def test_no_arbitrary_intensity_or_market_threshold_fields():
    result = regime.classify("BTC", 0.25, 0.50).to_dict()
    assert "intensity" not in result
    assert "price_threshold_pct" not in result
    assert "oi_threshold_pct" not in result


def test_first_snapshot_cannot_claim_a_regime():
    result = regime.classify("ETH", None, None)
    assert result.available is False
    assert result.state == "UNAVAILABLE"


def test_build_up_composite_uses_inverse_max_pain_price_direction():
    bullish = {"available": True, "state": "BULLISH_BUILDUP"}
    # Max-Pain SHORT means shorts are expected to be hurt, hence price up.
    assert "תומך" in regime.composite_conclusion(bullish, "SHORT")
    # Max-Pain LONG means longs are expected to be hurt, hence price down.
    assert "מנוגד" in regime.composite_conclusion(bullish, "LONG")


def test_covering_does_not_claim_new_long_build_up():
    covering = {"available": True, "state": "SHORT_COVERING"}
    text = regime.composite_conclusion(covering, "SHORT")
    assert "ללא אישור" in text


def test_attach_never_changes_existing_score(monkeypatch):
    monkeypatch.setattr(
        regime,
        "latest",
        lambda symbol: {
            "symbol": symbol,
            "available": True,
            "state": "BEARISH_BUILDUP",
        },
    )
    items = [{"symbol": "BTC", "side": "LONG", "score": 82.5, "priority": 82.5}]
    regime.attach_to_opportunities(items)
    assert items[0]["score"] == 82.5
    assert items[0]["priority"] == 82.5
    assert items[0]["market_regime"]["state"] == "BEARISH_BUILDUP"
