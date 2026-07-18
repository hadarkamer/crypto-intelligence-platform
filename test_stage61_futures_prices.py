import live_price_provider as lp


def test_hype_exact_pair_first():
    pairs = lp._candidate_pairs("HYPE", {"HYPE": ["HYPEUSDT"]})
    assert pairs[0] == ("HYPEUSDT", 1.0)
    assert pairs.count(("HYPEUSDT", 1.0)) == 1


def test_1000_contract_exact_before_alias():
    pairs = lp._candidate_pairs("1000PEPE", {})
    assert pairs[0] == ("1000PEPEUSDT", 1.0)
    assert ("PEPEUSDT", 1000.0) in pairs


def test_bulk_hype_price(monkeypatch):
    monkeypatch.setattr(lp, "_fetch_exchange_info", lambda: ({"HYPE": ["HYPEUSDT"]}, "host", []))
    monkeypatch.setattr(lp, "_fetch_bulk_mark_prices", lambda: ({"HYPEUSDT": 42.5}, "host", []))
    result = lp.fetch_binance_usdt_prices(["HYPE"])
    assert result["prices"]["HYPE"]["pair"] == "HYPEUSDT"
    assert result["prices"]["HYPE"]["price"] == 42.5
    assert result["prices"]["HYPE"]["source"] == "binance_futures_mark"


def test_direct_fallback_for_new_contract(monkeypatch):
    monkeypatch.setattr(lp, "_fetch_exchange_info", lambda: ({"HYPE": ["HYPEUSDT"]}, "host", []))
    monkeypatch.setattr(lp, "_fetch_bulk_mark_prices", lambda: ({}, None, ["bulk failed"]))
    monkeypatch.setattr(lp, "_fetch_direct_mark_price", lambda pair, preferred_host=None: (37.25, "host2", []))
    result = lp.fetch_binance_usdt_prices(["HYPE"])
    assert result["found_count"] == 1
    assert result["prices"]["HYPE"]["price"] == 37.25
