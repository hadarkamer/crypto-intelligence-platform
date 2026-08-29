"""Network-free checks for the official Research price overlay."""

from __future__ import annotations

from datetime import datetime, timezone

from binance_spot_price_path import SpotCandle
import live_price_provider as provider


BASE = datetime(2026, 8, 29, 12, 3, 20, tzinfo=timezone.utc)


def _candles() -> list[SpotCandle]:
    return [
        SpotCandle(
            open_time_utc=BASE.replace(minute=1, second=0, microsecond=0),
            close_time_utc=BASE.replace(minute=1, second=59, microsecond=999000),
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1.0,
        ),
        SpotCandle(
            open_time_utc=BASE.replace(minute=2, second=0, microsecond=0),
            close_time_utc=BASE.replace(minute=2, second=59, microsecond=999000),
            open=101.0,
            high=104.0,
            low=100.0,
            close=103.0,
            volume=2.0,
        ),
    ]


def run() -> None:
    calls = []

    def fetcher(symbol, start, end):
        calls.append((symbol, start, end))
        if symbol == "BAD":
            raise RuntimeError("official route unavailable")
        hype = symbol == "HYPE"
        return {
            "symbol": symbol,
            "pair": "HYPE/USDT" if hype else f"{symbol}USDT",
            "api_coin": "@107" if hype else None,
            "exchange": "hyperliquid" if hype else "binance",
            "market": "spot",
            "interval": "1m",
            "candles": _candles(),
            "complete": True,
        }

    result = provider.fetch_research_spot_1m_prices(
        ("BTC", "HYPE", "BAD"),
        observed_at_utc=BASE,
        candle_fetcher=fetcher,
    )
    assert result["fallback_used"] is False
    assert result["missing_symbols"] == ["BAD"]
    assert result["prices"]["BTC"]["source"] == "binance_spot"
    assert result["prices"]["BTC"]["pair"] == "BTCUSDT"
    assert result["prices"]["HYPE"]["source"] == "hyperliquid"
    assert result["prices"]["HYPE"]["pair"] == "HYPE/USDT"
    assert result["prices"]["HYPE"]["instrument"] == "@107"
    assert all(item[1] == BASE.replace(minute=1, second=0, microsecond=0) for item in calls)
    assert all(item[2] == BASE.replace(minute=3, second=0, microsecond=0) for item in calls)

    def wrong_hype(symbol, start, end):
        value = fetcher(symbol, start, end)
        value["api_coin"] = "HYPE"
        return value

    rejected = provider.fetch_research_spot_1m_prices(
        ("HYPE",), observed_at_utc=BASE, candle_fetcher=wrong_hype
    )
    assert rejected["prices"] == {}
    assert rejected["missing_symbols"] == ["HYPE"]
    assert "non-canonical" in rejected["errors"]["HYPE"]

    original = provider.fetch_research_spot_1m_prices
    provider.fetch_research_spot_1m_prices = lambda symbols: result
    try:
        overlaid = provider.enrich_research_snapshot_rows(
            [
                {
                    "symbol": symbol,
                    "current_price": 90.0,
                    "short_max_pain": 110.0,
                    "long_max_pain": 95.0,
                }
                for symbol in ("BTC", "HYPE", "BAD")
            ]
        )
    finally:
        provider.fetch_research_spot_1m_prices = original
    assert overlaid["fallback_used"] is False
    assert overlaid["skipped_symbols"] == ["BAD"]
    by_symbol = {row["symbol"]: row for row in overlaid["rows"]}
    assert by_symbol["BTC"]["price_interval"] == "1m"
    assert by_symbol["BTC"]["price_exchange"] == "binance"
    assert by_symbol["HYPE"]["price_instrument"] == "@107"
    assert by_symbol["HYPE"]["current_price"] == 103.0

    print("Official Research price overlay self-test: PASS")


if __name__ == "__main__":
    run()
