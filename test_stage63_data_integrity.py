from unittest.mock import patch

import coinglass_dom_reader as dom
import live_price_provider as prices


def test_valid_coinglass_row_schema_passes():
    cells = [
        "1", "BTC", "$64,000", "$65,000", "49.57M", "$1,000", "+1.56%", "💥",
        "$63,000", "41.25M", "-$1,000", "-1.56%", "💥",
    ]
    parsed = {
        "price": 64000.0,
        "max_short_price": 65000.0,
        "short_amount_usd": 49_570_000.0,
        "short_distance_usd": 1000.0,
        "short_distance_pct": 1.56,
        "max_long_price": 63000.0,
        "long_amount_usd": 41_250_000.0,
        "long_distance_usd": -1000.0,
        "long_distance_pct": -1.56,
    }
    ok, errors = dom._validate_maxpain_row(cells, parsed)
    assert ok, errors


def test_shifted_amount_percentage_cell_is_rejected():
    cells = [
        "1", "BTC", "$64,000", "$65,000", "+1.56%", "$1,000", "49.57M", "💥",
        "$63,000", "-1.56%", "-$1,000", "41.25M", "💥",
    ]
    parsed = {
        "price": 64000.0,
        "max_short_price": 65000.0,
        "short_amount_usd": 1.56,
        "short_distance_usd": 1000.0,
        "short_distance_pct": 49_570_000.0,
        "max_long_price": 63000.0,
        "long_amount_usd": -1.56,
        "long_distance_usd": -1000.0,
        "long_distance_pct": 41_250_000.0,
    }
    ok, errors = dom._validate_maxpain_row(cells, parsed)
    assert not ok
    assert errors


def test_price_provider_uses_futures_mark_only():
    exchange_info = {
        "by_pair": {"BTCUSDT": {"status": "TRADING", "contractType": "PERPETUAL"}},
        "by_base": {"BTC": ["BTCUSDT"]},
    }
    with patch.object(prices, "_fetch_futures_exchange_info", return_value=exchange_info), \
         patch.object(prices, "_fetch_futures_mark_prices", return_value={"BTCUSDT": 64123.5}):
        result = prices.fetch_binance_usdt_prices(["BTC"])
    assert result["source"] == "binance_futures_mark"
    assert result["prices"]["BTC"]["source"] == "binance_futures_mark"
    assert result["prices"]["BTC"]["price"] == 64123.5
