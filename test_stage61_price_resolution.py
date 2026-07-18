from live_price_provider import _resolve_futures_pair


def test_hype_exchange_info_resolution():
    info = {
        "by_pair": {
            "HYPEUSDT": {
                "symbol": "HYPEUSDT",
                "baseAsset": "HYPE",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
            }
        },
        "by_base": {"HYPE": ["HYPEUSDT"]},
    }
    pair, multiplier, candidates = _resolve_futures_pair(
        "HYPE", info, {"HYPEUSDT": 68.42}
    )
    assert pair == "HYPEUSDT"
    assert multiplier == 1.0
    assert "HYPEUSDT" in candidates


def test_exact_pair_is_preferred():
    info = {
        "by_pair": {"BTCUSDT": {"status": "TRADING"}},
        "by_base": {"BTC": ["BTCUSDT"]},
    }
    pair, _, _ = _resolve_futures_pair("BTC", info, {"BTCUSDT": 64000.0})
    assert pair == "BTCUSDT"
