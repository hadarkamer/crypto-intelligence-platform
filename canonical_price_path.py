"""Canonical historical price routing for Research outcome labels.

The route is explicit and provenance-preserving: Binance Spot USDT is used for
symbols listed there, while HYPE uses Hyperliquid's HYPE/USDT spot candles.
No futures path or silent exchange fallback is permitted.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
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
PRICE_PROVENANCE_VERSION = "canonical-spot-reference-provenance-v1"


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


def validated_route(
    symbol: Any,
    path_result: Dict[str, Any],
    *,
    require_complete: bool = True,
) -> Dict[str, Any]:
    """Validate and normalize the exact official one-minute Spot route."""
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("canonical price route requires a symbol")
    exchange = str(path_result.get("exchange") or "").strip().lower()
    market = str(path_result.get("market") or "").strip().lower()
    pair = str(path_result.get("pair") or "").strip().upper()
    interval = str(path_result.get("interval") or INTERVAL).strip().lower()
    interval_seconds = path_result.get("interval_seconds")
    if type(interval_seconds) is not int:
        raise ValueError("canonical path interval_seconds must be a JSON integer")
    instrument = str(path_result.get("api_coin") or "").strip()
    if require_complete and path_result.get("complete") is not True:
        raise ValueError("canonical one-minute Spot path is incomplete")
    if market != "spot" or interval != "1m" or interval_seconds != 60:
        raise ValueError("canonical path must be closed Spot 1m")
    if normalized == "HYPE":
        if (
            exchange != "hyperliquid"
            or pair != "HYPE/USDT"
            or instrument != hyperliquid_spot_price_path.HYPE_SPOT_COIN
        ):
            raise ValueError(
                "HYPE canonical path must be Hyperliquid HYPE/USDT Spot @107"
            )
    else:
        expected_pair, _multiplier = binance_spot_price_path.resolve_pair(
            normalized
        )
        if exchange != "binance" or pair != expected_pair or instrument:
            raise ValueError(
                f"{normalized} canonical path must be Binance Spot {expected_pair}"
            )
    return {
        "provenance_version": PRICE_PROVENANCE_VERSION,
        "method_version": METHOD_VERSION,
        "symbol": normalized,
        "exchange": exchange,
        "market": market,
        "pair": pair,
        "instrument": instrument or None,
        "interval": "1m",
        "interval_seconds": 60,
        "provider_provenance": str(
            path_result.get("provenance")
            or "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
        ),
    }


def canonical_provenance_text(symbol: Any, path_result: Dict[str, Any]) -> str:
    """Return stable persisted provenance, including HYPE instrument @107."""
    return json.dumps(
        validated_route(symbol, path_result),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def persisted_reference_is_canonical(
    row: Dict[str, Any],
    *,
    required_hype_replay_version: str = "",
) -> bool:
    """Validate a persisted canonical reference-price row fail-closed.

    Older Binance rows retain enough relational route metadata to prove the
    Spot USDT route.  Older HYPE rows did not persist ``@107`` and are therefore
    rejected; HYPE requires the new structured provenance and, when supplied,
    the exact replay version.
    """
    symbol = str(row.get("symbol") or "").strip().upper()
    exchange = str(row.get("exchange") or "").strip().lower()
    market = str(row.get("market") or "").strip().lower()
    pair = str(row.get("pair") or "").strip().upper()
    interval_seconds = row.get("interval_seconds")
    if type(interval_seconds) is not int:
        return False
    if (
        not symbol
        or market != "spot"
        or interval_seconds != 60
        or str(row.get("outcome_method_version") or "") != METHOD_VERSION
        or str(row.get("data_quality_status") or "") not in COMPLETE_QUALITIES
    ):
        return False
    if symbol != "HYPE":
        try:
            expected_pair, _multiplier = binance_spot_price_path.resolve_pair(
                symbol
            )
        except (TypeError, ValueError):
            return False
        return exchange == "binance" and pair == expected_pair
    if exchange != "hyperliquid" or pair != "HYPE/USDT":
        return False
    if required_hype_replay_version and str(
        row.get("replay_version") or ""
    ) != required_hype_replay_version:
        return False
    try:
        provenance = json.loads(str(row.get("provenance") or ""))
    except (TypeError, json.JSONDecodeError):
        return False
    return bool(
        isinstance(provenance, dict)
        and provenance.get("provenance_version") == PRICE_PROVENANCE_VERSION
        and provenance.get("method_version") == METHOD_VERSION
        and provenance.get("symbol") == "HYPE"
        and provenance.get("exchange") == "hyperliquid"
        and provenance.get("market") == "spot"
        and provenance.get("pair") == "HYPE/USDT"
        and provenance.get("instrument")
        == hyperliquid_spot_price_path.HYPE_SPOT_COIN
        and provenance.get("interval") == "1m"
        and type(provenance.get("interval_seconds")) is int
        and provenance.get("interval_seconds") == 60
    )


def canonical_source_description() -> str:
    return (
        "Binance Spot USDT 1m closed candles; "
        "HYPE uses Hyperliquid HYPE/USDT spot (@107) 1m closed candles"
    )
