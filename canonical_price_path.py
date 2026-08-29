"""Canonical historical price routing for Research outcome labels.

The route is explicit and provenance-preserving: Binance Spot USDT is used for
symbols listed there, while HYPE uses Hyperliquid's HYPE/USDT spot candles.
No futures path or silent exchange fallback is permitted.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import binance_spot_price_path
import hyperliquid_spot_price_path


METHOD_VERSION = "canonical-spot-1m-ohlc-path-v3"
BINANCE_COMPLETE = "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES"
BINANCE_PARTIAL = "PARTIAL_BINANCE_SPOT_1M_CLOSED_CANDLES"
HYPERLIQUID_COMPLETE = "VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES"
HYPERLIQUID_PARTIAL = "PARTIAL_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES"
COMPLETE_QUALITIES = (BINANCE_COMPLETE, HYPERLIQUID_COMPLETE)
INTERVAL = binance_spot_price_path.INTERVAL
INTERVAL_SECONDS = binance_spot_price_path.INTERVAL_SECONDS
INTERVAL_MS = binance_spot_price_path.INTERVAL_MS


def provider_for_symbol(symbol: Any) -> str:
    return "hyperliquid" if str(symbol or "").strip().upper() == "HYPE" else "binance"


def fetch_closed_candles(symbol: str, start_time: Any, end_time: Any) -> Dict[str, Any]:
    provider = provider_for_symbol(symbol)
    if provider == "hyperliquid":
        result = hyperliquid_spot_price_path.fetch_closed_candles(
            symbol, start_time, end_time
        )
    else:
        result = binance_spot_price_path.fetch_closed_candles(
            symbol, start_time, end_time
        )
        result = dict(result)
        result["provenance"] = "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
    result["retrieved_at_utc"] = datetime.now(timezone.utc).isoformat()
    return result


def quality_status(path_result: Dict[str, Any], *, complete: bool) -> str:
    exchange = str(path_result.get("exchange") or "").lower()
    if exchange == "hyperliquid":
        return HYPERLIQUID_COMPLETE if complete else HYPERLIQUID_PARTIAL
    if exchange == "binance":
        return BINANCE_COMPLETE if complete else BINANCE_PARTIAL
    raise ValueError(f"Unsupported canonical outcome exchange: {exchange or 'missing'}")


def canonical_source_description() -> str:
    return (
        "Binance Spot USDT 1m closed candles; "
        "HYPE uses Hyperliquid HYPE/USDT spot (@107) 1m closed candles"
    )
