from unittest.mock import patch

import counter_score
import live_price_provider
from alert_summary import format_alert_count_summary


def test_alert_summary_by_coin_and_side():
    items = [
        {"symbol": "BTC", "side": "LONG"},
        {"symbol": "BTC", "side": "LONG"},
        {"symbol": "BTC", "side": "SHORT"},
        {"symbol": "ETH", "side": "SHORT"},
    ]
    text = format_alert_count_summary(items)
    assert "BTC: 2 LONG, 1 SHORT" in text
    assert "ETH: 1 SHORT" in text
    assert "סה\"כ" not in text


def test_hype_uses_bybit_futures_first():
    with patch.object(live_price_provider, "_fetch_futures_mark_prices", return_value={}), \
         patch.object(live_price_provider, "_fetch_spot_prices", return_value={}), \
         patch.object(live_price_provider, "_fetch_bybit_hype_price", return_value=42.5) as bybit:
        result = live_price_provider.fetch_binance_usdt_prices(["HYPE"])
    assert result["prices"]["HYPE"]["price"] == 42.5
    assert result["prices"]["HYPE"]["source"] == "bybit_futures_mark"
    bybit.assert_called_once_with("linear")


def test_hype_falls_back_to_bybit_spot():
    def fetch(category):
        if category == "linear":
            raise RuntimeError("linear unavailable")
        return 41.25

    with patch.object(live_price_provider, "_fetch_futures_mark_prices", return_value={}), \
         patch.object(live_price_provider, "_fetch_spot_prices", return_value={}), \
         patch.object(live_price_provider, "_fetch_bybit_hype_price", side_effect=fetch):
        result = live_price_provider.fetch_binance_usdt_prices(["HYPE"])
    assert result["prices"]["HYPE"]["price"] == 41.25
    assert result["prices"]["HYPE"]["source"] == "bybit_spot"
    assert "linear unavailable" in result["bybit_futures_error"]


def test_hype_falls_back_to_hyperliquid_after_bybit():
    with patch.object(live_price_provider, "_fetch_futures_mark_prices", return_value={}), \
         patch.object(live_price_provider, "_fetch_spot_prices", return_value={}), \
         patch.object(live_price_provider, "_fetch_bybit_hype_price", side_effect=RuntimeError("blocked")), \
         patch.object(live_price_provider, "_fetch_hyperliquid_hype_price", return_value=44.0), \
         patch.object(live_price_provider, "_fetch_coingecko_hype_price") as cg:
        result = live_price_provider.fetch_binance_usdt_prices(["HYPE"])
    assert result["prices"]["HYPE"]["price"] == 44.0
    assert result["prices"]["HYPE"]["source"] == "hyperliquid"
    cg.assert_not_called()


def test_hype_uses_coinglass_row_as_final_fallback():
    row = {
        "symbol": "HYPE", "timeframe": "12h", "current_price": 45.5,
        "short_max_pain": 46.0, "long_max_pain": 44.0,
    }
    with patch.object(live_price_provider, "fetch_binance_usdt_prices", return_value={"prices": {}}):
        result = live_price_provider.enrich_snapshot_rows([row])
    assert len(result["rows"]) == 1
    assert result["rows"][0]["current_price"] == 45.5
    assert result["rows"][0]["price_source"] == "coinglass_dom"


def test_counter_score_only_for_requested_item():
    rows = []
    price = 100.0
    for tf in ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]:
        rows.append({
            "symbol": "BTC", "timeframe": tf, "rank": 1,
            "current_price": price,
            "short_max_pain": 101.0,
            "long_max_pain": 98.0,
            "distance_short_pct": 1.0,
            "distance_long_pct": -2.0,
            "short_liquidation_amount": 1_000_000,
            "long_liquidation_amount": 900_000,
        })
    primary = {"symbol": "BTC", "timeframe": "12h", "side": "SHORT", "score": 80}
    result = counter_score.calculate_counter_score(primary, rows)
    assert result["available"] is True
    assert result["side"] == "LONG"
    assert 0 <= result["score"] <= 100
