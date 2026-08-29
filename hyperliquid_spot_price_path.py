"""Canonical HYPE/USDT spot candle reader backed by Hyperliquid.

Hyperliquid exposes spot candles through its public ``/info`` endpoint.  The
HYPE spot market is addressed by the documented mainnet spot index ``@107``.
As with the Binance research path, the first partial minute after an alert is
excluded so a candle high/low from before the alert cannot leak into outcomes.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Callable, Dict

import requests

from binance_spot_price_path import (
    INTERVAL,
    INTERVAL_MS,
    INTERVAL_SECONDS,
    MAX_CANDLES,
    SpotCandle,
)


HYPERLIQUID_INFO_URL = os.getenv(
    "HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info"
).strip()
HYPE_SPOT_COIN = os.getenv("HYPERLIQUID_HYPE_SPOT_COIN", "@107").strip() or "@107"
REQUEST_TIMEOUT_SECONDS = max(
    3, int(os.getenv("HYPERLIQUID_PRICE_PATH_TIMEOUT_SECONDS", "15"))
)


class HyperliquidSpotPathError(RuntimeError):
    """Raised when the canonical Hyperliquid HYPE spot path is unusable."""


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _epoch_ms(value: Any) -> int:
    return int(_utc(value).timestamp() * 1000)


def _datetime_ms(value: Any) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)


def _ceil_interval_ms(value: int) -> int:
    return ((int(value) + INTERVAL_MS - 1) // INTERVAL_MS) * INTERVAL_MS


def _parse_candle(raw: Any) -> SpotCandle:
    if not isinstance(raw, dict):
        raise HyperliquidSpotPathError("Hyperliquid returned an invalid candle row")
    try:
        candle = SpotCandle(
            open_time_utc=_datetime_ms(raw["t"]),
            close_time_utc=_datetime_ms(raw["T"]),
            open=float(raw["o"]),
            high=float(raw["h"]),
            low=float(raw["l"]),
            close=float(raw["c"]),
            volume=float(raw.get("v") or 0.0),
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise HyperliquidSpotPathError("Hyperliquid returned a malformed candle") from exc
    if min(candle.open, candle.high, candle.low, candle.close) <= 0:
        raise HyperliquidSpotPathError("Hyperliquid returned a non-positive price")
    if candle.high < max(candle.open, candle.close, candle.low):
        raise HyperliquidSpotPathError("Hyperliquid candle high is inconsistent")
    if candle.low > min(candle.open, candle.close, candle.high):
        raise HyperliquidSpotPathError("Hyperliquid candle low is inconsistent")
    return candle


def fetch_closed_candles(
    symbol: str,
    start_time: Any,
    end_time: Any,
    *,
    request_post: Callable[..., Any] = requests.post,
) -> Dict[str, Any]:
    """Fetch closed post-alert HYPE spot candles at one-minute resolution."""
    normalized = str(symbol or "").strip().upper()
    if normalized != "HYPE":
        raise HyperliquidSpotPathError(
            "Hyperliquid canonical spot routing is configured only for HYPE"
        )
    start = _utc(start_time)
    end = _utc(end_time)
    if end <= start:
        raise ValueError("end_time must be after start_time")

    start_ms = _ceil_interval_ms(_epoch_ms(start))
    end_ms = _epoch_ms(end)
    if start_ms + INTERVAL_MS - 1 > end_ms:
        return {
            "symbol": normalized,
            "pair": "HYPE/USDT",
            "api_coin": HYPE_SPOT_COIN,
            "exchange": "hyperliquid",
            "market": "spot",
            "interval": INTERVAL,
            "interval_seconds": INTERVAL_SECONDS,
            "candles": [],
            "expected_candles": 0,
            "complete": True,
            "provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
        }

    last_eligible_open = ((end_ms - (INTERVAL_MS - 1)) // INTERVAL_MS) * INTERVAL_MS
    expected = (
        max(0, ((last_eligible_open - start_ms) // INTERVAL_MS) + 1)
        if last_eligible_open >= start_ms
        else 0
    )
    if expected > MAX_CANDLES:
        raise HyperliquidSpotPathError(
            f"Requested Hyperliquid path exceeds {MAX_CANDLES} candles"
        )

    response = request_post(
        HYPERLIQUID_INFO_URL,
        json={
            "type": "candleSnapshot",
            "req": {
                "coin": HYPE_SPOT_COIN,
                "interval": INTERVAL,
                "startTime": start_ms,
                "endTime": end_ms,
            },
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    try:
        response.raise_for_status()
    except Exception as exc:
        status = getattr(response, "status_code", "unknown")
        raise HyperliquidSpotPathError(
            f"Hyperliquid candle request failed for HYPE/USDT (HTTP {status})"
        ) from exc
    payload = response.json()
    if not isinstance(payload, list):
        raise HyperliquidSpotPathError("Hyperliquid returned a non-list candle payload")

    candles_by_open: Dict[int, SpotCandle] = {}
    for raw in payload:
        candle = _parse_candle(raw)
        open_ms = _epoch_ms(candle.open_time_utc)
        close_ms = _epoch_ms(candle.close_time_utc)
        if open_ms < start_ms or close_ms > end_ms:
            continue
        candles_by_open[open_ms] = candle
    candles = [candles_by_open[key] for key in sorted(candles_by_open)]
    return {
        "symbol": normalized,
        "pair": "HYPE/USDT",
        "api_coin": HYPE_SPOT_COIN,
        "exchange": "hyperliquid",
        "market": "spot",
        "interval": INTERVAL,
        "interval_seconds": INTERVAL_SECONDS,
        "candles": candles,
        "expected_candles": expected,
        "complete": len(candles) == expected,
        "provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
    }
