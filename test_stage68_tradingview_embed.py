import technical_signal_store as store


def _payload(title: str, score: str = "82.5", timeframe: str = "15"):
    return {
        "embeds": [{
            "title": title,
            "fields": [
                {"name": "🎯 Score", "value": f"**{score}/100**"},
                {"name": "📊 AVG Score", "value": "74/100"},
                {"name": "💰 Price", "value": "$118,250.50"},
                {"name": "⏰ TF", "value": timeframe},
                {"name": "⚡ Exit", "value": "31/100"},
                {"name": "🛡️ ATR Stop", "value": "$116,900"},
            ],
        }]
    }


def test_bullish_embed():
    adapted = store.normalize_tradingview_webhook(
        _payload("BTCUSDT:BINANCE | 🟢 ANY BULLISH SIGNAL")
    )
    signal = store.normalize_payload(adapted)
    assert signal.symbol == "BTC"
    assert signal.exchange == "BINANCE"
    assert signal.timeframe == "15m"
    assert signal.direction == "LONG"
    assert signal.technical_score == 82.5
    assert adapted["event_type"] == "bullish_signal"
    assert adapted["price"] == 118250.5


def test_bearish_embed():
    adapted = store.normalize_tradingview_webhook(
        _payload("BTCUSDT:BINANCE | 🔴 ANY BEARISH SIGNAL", "21", "1D")
    )
    signal = store.normalize_payload(adapted)
    assert signal.direction == "SHORT"
    assert signal.timeframe == "1d"
    assert adapted["event_type"] == "bearish_signal"


def test_strong_zone_embed():
    adapted = store.normalize_tradingview_webhook(
        _payload("BTCUSDT:BINANCE | 🟢 GOAT Score — Strong Zone", "88", "4H")
    )
    signal = store.normalize_payload(adapted)
    assert signal.direction == "NEUTRAL"
    assert signal.timeframe == "4h"
    assert adapted["event_type"] == "strong_zone"
