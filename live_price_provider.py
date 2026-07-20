from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


BINANCE_FUTURES_BASE_URL = os.getenv(
    "BINANCE_FUTURES_BASE_URL",
    "https://fapi.binance.com",
).rstrip("/")
BINANCE_FUTURES_MARK_ENDPOINT = os.getenv(
    "BINANCE_FUTURES_MARK_ENDPOINT",
    "/fapi/v1/premiumIndex",
)

BINANCE_SPOT_BASE_URL = os.getenv(
    "BINANCE_MARKET_DATA_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")
BINANCE_SPOT_PRICE_ENDPOINT = os.getenv(
    "BINANCE_PRICE_ENDPOINT",
    "/api/v3/ticker/price",
)

REQUEST_TIMEOUT_SECONDS = int(os.getenv("BINANCE_PRICE_TIMEOUT_SECONDS", "15"))
HYPERLIQUID_INFO_URL = os.getenv(
    "HYPERLIQUID_INFO_URL",
    "https://api.hyperliquid.xyz/info",
)
HYPERLIQUID_TIMEOUT_SECONDS = int(
    os.getenv("HYPERLIQUID_PRICE_TIMEOUT_SECONDS", "10")
)

# symbol -> (Binance base symbol, multiplier)
# 1000PEPE on CoinGlass represents 1000 PEPE units.
SYMBOL_ALIASES: Dict[str, Tuple[str, float]] = {
    "1000PEPE": ("PEPE", 1000.0),
}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _fetch_futures_mark_prices() -> Dict[str, float]:
    url = BINANCE_FUTURES_BASE_URL + BINANCE_FUTURES_MARK_ENDPOINT
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()

    result: Dict[str, float] = {}
    for item in payload:
        pair = str(item.get("symbol", "")).upper()
        raw_price = item.get("markPrice")
        if not pair or raw_price is None:
            continue
        try:
            result[pair] = float(raw_price)
        except (TypeError, ValueError):
            continue
    return result


def _fetch_spot_prices() -> Dict[str, float]:
    url = BINANCE_SPOT_BASE_URL + BINANCE_SPOT_PRICE_ENDPOINT
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()

    result: Dict[str, float] = {}
    for item in payload:
        pair = str(item.get("symbol", "")).upper()
        raw_price = item.get("price")
        if not pair or raw_price is None:
            continue
        try:
            result[pair] = float(raw_price)
        except (TypeError, ValueError):
            continue
    return result



def _fetch_hyperliquid_mids() -> Dict[str, float]:
    """Fetch public Hyperliquid mid prices via the official Info API."""
    response = requests.post(
        HYPERLIQUID_INFO_URL,
        json={"type": "allMids", "dex": ""},
        timeout=HYPERLIQUID_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Hyperliquid allMids returned a non-object payload")

    result: Dict[str, float] = {}
    for symbol, raw_price in payload.items():
        normalized = _normalize_symbol(symbol)
        if not normalized or normalized.startswith("@"):
            continue
        try:
            result[normalized] = float(raw_price)
        except (TypeError, ValueError):
            continue
    return result


def fetch_binance_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    """Fetch Binance prices once.

    Priority:
    1. Binance Spot last price — matches the regular Binance market price.
    2. USD-M Futures mark price as fallback when Spot is unavailable.
    """
    requested = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    fetched_at = datetime.now(timezone.utc)

    futures_error = None
    spot_error = None
    hyperliquid_error = None

    try:
        futures_prices = _fetch_futures_mark_prices()
    except Exception as exc:
        futures_prices = {}
        futures_error = repr(exc)

    try:
        spot_prices = _fetch_spot_prices()
    except Exception as exc:
        spot_prices = {}
        spot_error = repr(exc)

    hyperliquid_prices: Dict[str, float] = {}
    if "HYPE" in requested:
        try:
            hyperliquid_prices = _fetch_hyperliquid_mids()
        except Exception as exc:
            hyperliquid_error = repr(exc)

    prices: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []

    for symbol in requested:
        base, multiplier = SYMBOL_ALIASES.get(symbol, (symbol, 1.0))
        pair = f"{base}USDT"

        if pair in spot_prices:
            raw_price = spot_prices[pair]
            source = "binance_spot"
        elif pair in futures_prices:
            raw_price = futures_prices[pair]
            source = "binance_futures_mark"
        elif symbol == "HYPE" and "HYPE" in hyperliquid_prices:
            raw_price = hyperliquid_prices["HYPE"]
            source = "hyperliquid_all_mids"
            pair = "HYPE/USD"
        else:
            missing.append(symbol)
            continue

        prices[symbol] = {
            "symbol": symbol,
            "pair": pair,
            "price": raw_price * multiplier,
            "raw_price": raw_price,
            "multiplier": multiplier,
            "source": source,
            "fetched_at_utc": fetched_at.isoformat(),
        }

    return {
        "ok": bool(prices),
        "source": "binance_spot_then_futures_mark",
        "fetched_at_utc": fetched_at.isoformat(),
        "requested_count": len(requested),
        "found_count": len(prices),
        "missing_count": len(missing),
        "prices": prices,
        "missing_symbols": missing,
        "futures_error": futures_error,
        "spot_error": spot_error,
        "hyperliquid_error": hyperliquid_error,
    }


def recalculate_distances(
    live_price: float,
    short_max_pain: Optional[float],
    long_max_pain: Optional[float],
) -> Dict[str, Optional[float]]:
    """Recalculate all Max Pain distances from the Binance live price."""
    if not live_price:
        return {
            "short_signed_pct": None,
            "long_signed_pct": None,
            "short_abs_pct": None,
            "long_abs_pct": None,
            "short_abs_usd": None,
            "long_abs_usd": None,
            "closest_side": None,
        }

    short_signed = (
        (short_max_pain - live_price) / live_price * 100
        if short_max_pain is not None else None
    )
    long_signed = (
        (long_max_pain - live_price) / live_price * 100
        if long_max_pain is not None else None
    )

    short_abs = abs(short_signed) if short_signed is not None else None
    long_abs = abs(long_signed) if long_signed is not None else None
    short_abs_usd = abs(short_max_pain - live_price) if short_max_pain is not None else None
    long_abs_usd = abs(long_max_pain - live_price) if long_max_pain is not None else None

    if short_abs is None and long_abs is None:
        closest = None
    elif long_abs is None or (short_abs is not None and short_abs <= long_abs):
        closest = "SHORT"
    else:
        closest = "LONG"

    return {
        "short_signed_pct": short_signed,
        "long_signed_pct": long_signed,
        "short_abs_pct": short_abs,
        "long_abs_pct": long_abs,
        "short_abs_usd": short_abs_usd,
        "long_abs_usd": long_abs_usd,
        "closest_side": closest,
    }


def enrich_snapshot_rows(rows: Iterable[Any], excluded_symbols: Iterable[str] = ()) -> Dict[str, Any]:
    """Overlay Binance live prices on raw CoinGlass rows.

    CoinGlass remains the source for:
    - Short/Long Max Pain targets
    - Short/Long liquidation amounts

    Binance becomes the source for:
    - current_price
    - distance_short_pct / distance_long_pct
    - distance_short_abs / distance_long_abs

    Symbols without a supported live price are excluded from live calculations. HYPE uses the Hyperliquid public Info API when Binance does not list it.
    """
    excluded = {str(x).upper() for x in excluded_symbols}
    raw_rows = [dict(row) for row in rows]
    symbols = sorted({
        str(row.get("symbol", "")).upper()
        for row in raw_rows
        if row.get("symbol") and str(row.get("symbol")).upper() not in excluded
    })

    price_result = fetch_binance_usdt_prices(symbols)
    enriched = []
    skipped = []

    for row in raw_rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or symbol in excluded:
            continue

        live = price_result["prices"].get(symbol)
        if not live:
            skipped.append(symbol)
            continue

        calc = recalculate_distances(
            live["price"],
            row.get("short_max_pain"),
            row.get("long_max_pain"),
        )

        row["coinglass_price"] = row.get("current_price")
        row["current_price"] = live["price"]
        row["price_source"] = live["source"]
        row["price_pair"] = live["pair"]
        row["price_fetched_at_utc"] = live["fetched_at_utc"]

        row["distance_short_pct"] = calc["short_signed_pct"]
        row["distance_long_pct"] = calc["long_signed_pct"]
        row["distance_short_abs"] = calc["short_abs_usd"]
        row["distance_long_abs"] = calc["long_abs_usd"]
        row["closest_side"] = calc["closest_side"]

        enriched.append(row)

    return {
        "rows": enriched,
        "price_result": price_result,
        "skipped_symbols": sorted(set(skipped)),
    }
