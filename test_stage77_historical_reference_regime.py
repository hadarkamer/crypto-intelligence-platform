import coinglass_oi_regime_service as r


def _ref(p25=0.10, p75=0.50, p90=1.00, p95=1.50):
    d={"p25":p25,"median":0.25,"p75":p75,"p90":p90,"p95":p95,"count":1400}
    return {"samples":1400,"price_abs_change_pct":dict(d),"oi_abs_change_pct":dict(d)}


def test_below_p25_becomes_neutral(monkeypatch):
    monkeypatch.setattr(r.history_reference, "reference_for_window", lambda symbol,label: _ref())
    d=r._classify_with_historical_reference("BTC","1h",0.05,0.80)
    assert d["state"] == "NEUTRAL_INCONCLUSIVE"
    assert d["price_strength"]["label"] == "Weak / Noise"
    assert d["oi_strength"]["label"] == "Elevated"
    assert d["price_minimum_valid"] is False
    assert d["oi_minimum_valid"] is True


def test_both_above_p25_keep_directional_state(monkeypatch):
    monkeypatch.setattr(r.history_reference, "reference_for_window", lambda symbol,label: _ref())
    d=r._classify_with_historical_reference("BTC","1h",0.20,1.20)
    assert d["state"] == "BULLISH_BUILDUP"
    assert d["price_strength"]["label"] == "Normal"
    assert d["oi_strength"]["label"] == "Strong"


def test_strength_bands_are_percentile_based():
    dist=_ref()["oi_abs_change_pct"]
    fn=r.history_reference.strength_from_distribution
    assert fn(0.05,dist)["label"] == "Weak / Noise"
    assert fn(0.10,dist)["label"] == "Normal"
    assert fn(0.70,dist)["label"] == "Elevated"
    assert fn(1.20,dist)["label"] == "Strong"
    assert fn(1.70,dist)["label"] == "Extreme"


def test_no_reference_preserves_existing_classifier(monkeypatch):
    monkeypatch.setattr(r.history_reference, "reference_for_window", lambda symbol,label: {})
    d=r._classify_with_historical_reference("OTHER","1h",0.000001,0.000001)
    assert d["state"] == "BULLISH_BUILDUP"
    assert d["historical_reference_available"] is False


def test_significance_observation_for_strong_oi_weak_price():
    windows={
        "1h": {
            "historical_reference_available": True,
            "price_strength": {"rank":0,"label":"Weak / Noise"},
            "oi_strength": {"rank":3,"label":"Strong"},
        }
    }
    obs=r._significance_observations(windows)
    assert obs and obs[0]["type"] == "OI_WITHOUT_PRICE_CONFIRMATION"
