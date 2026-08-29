"""Deterministic, network-free checks for the Binance Spot Research path."""

from __future__ import annotations

from datetime import datetime, timezone

import binance_spot_price_path as price_path


def _ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _row(open_time: str, open_: float, high: float, low: float, close: float):
    open_ms = _ms(open_time)
    return [
        open_ms,
        str(open_),
        str(high),
        str(low),
        str(close),
        "123.45",
        open_ms + 59_999,
    ]


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
        _row("2026-08-28T10:01:00Z", 100, 102, 99, 101),
        _row("2026-08-28T10:02:00Z", 101, 105, 100, 104),
    ]

    def fake_get(url, *, params, timeout):
        calls.append({"url": url, "params": dict(params), "timeout": timeout})
        return _Response(payload)

    result = price_path.fetch_closed_candles(
        "BTC",
        "2026-08-28T10:00:30Z",
        "2026-08-28T10:03:00Z",
        request_get=fake_get,
    )
    assert result["pair"] == "BTCUSDT"
    assert result["interval"] == "1m"
    assert result["expected_candles"] == 2
    assert result["complete"] is True
    assert len(result["candles"]) == 2
    # The 10:00 candle is excluded because half of it predates the alert.
    assert calls[0]["params"]["startTime"] == _ms("2026-08-28T10:01:00Z")

    long_metrics = price_path.calculate_path_metrics(
        reference_price=100,
        direction="LONG",
        event_time="2026-08-28T10:00:30Z",
        candles=result["candles"],
        target_price=104,
    )
    assert round(long_metrics["raw_return_pct"], 8) == 4.0
    assert round(long_metrics["directional_return_pct"], 8) == 4.0
    assert round(long_metrics["mfe_pct"], 8) == 5.0
    assert round(long_metrics["mae_pct"], 8) == 1.0
    assert long_metrics["target_reached"] is True
    assert long_metrics["target_progress_ratio"] == 1.0
    assert long_metrics["time_to_target_seconds"] == 149

    short_candles = [
        price_path.SpotCandle(
            open_time_utc=datetime(2026, 8, 28, 10, 1, tzinfo=timezone.utc),
            close_time_utc=datetime(2026, 8, 28, 10, 1, 59, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=98,
            close=99,
            volume=1,
        ),
        price_path.SpotCandle(
            open_time_utc=datetime(2026, 8, 28, 10, 2, tzinfo=timezone.utc),
            close_time_utc=datetime(2026, 8, 28, 10, 2, 59, tzinfo=timezone.utc),
            open=99,
            high=100,
            low=95,
            close=96,
            volume=1,
        ),
    ]
    short_metrics = price_path.calculate_path_metrics(
        reference_price=100,
        direction="SHORT",
        event_time="2026-08-28T10:00:30Z",
        candles=short_candles,
        target_price=96,
    )
    assert round(short_metrics["directional_return_pct"], 8) == 4.0
    assert round(short_metrics["mfe_pct"], 8) == 5.0
    assert round(short_metrics["mae_pct"], 8) == 1.0
    assert short_metrics["target_reached"] is True

    pair, multiplier = price_path.resolve_pair("1000PEPE")
    assert pair == "PEPEUSDT"
    assert multiplier == 1000.0

    print("Binance Spot price-path self-test: PASS")


if __name__ == "__main__":
    run()
