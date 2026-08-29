"""Deterministic, network-free checks for the HYPE Hyperliquid spot path."""

from __future__ import annotations

from datetime import datetime

import hyperliquid_spot_price_path as price_path


def _ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _row(open_time: str, open_: float, high: float, low: float, close: float):
    opened = _ms(open_time)
    return {
        "t": opened,
        "T": opened + 59_999,
        "s": "@107",
        "i": "1m",
        "o": str(open_),
        "h": str(high),
        "l": str(low),
        "c": str(close),
        "v": "123.45",
        "n": 10,
    }


class _Response:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def run() -> None:
    calls = []
    payload = [
        _row("2026-08-28T10:00:00Z", 80.0, 80.8, 79.8, 80.5),
        _row("2026-08-28T10:01:00Z", 80.5, 81.2, 80.4, 81.0),
        _row("2026-08-28T10:02:00Z", 81.0, 82.0, 80.9, 81.8),
    ]

    def fake_post(url, *, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _Response(payload)

    result = price_path.fetch_closed_candles(
        "HYPE",
        "2026-08-28T10:00:30Z",
        "2026-08-28T10:03:00Z",
        request_post=fake_post,
    )
    assert result["exchange"] == "hyperliquid"
    assert result["market"] == "spot"
    assert result["pair"] == "HYPE/USDT"
    assert result["api_coin"] == "@107"
    assert result["expected_candles"] == 2
    assert result["complete"] is True
    assert len(result["candles"]) == 2
    assert calls[0]["json"]["type"] == "candleSnapshot"
    assert calls[0]["json"]["req"]["coin"] == "@107"
    assert calls[0]["json"]["req"]["startTime"] == _ms(
        "2026-08-28T10:01:00Z"
    )

    print("Hyperliquid HYPE spot price-path self-test: PASS")


if __name__ == "__main__":
    run()
