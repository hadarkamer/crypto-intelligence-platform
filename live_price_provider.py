from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


BYBIT_API_BASE_URL = os.getenv(
    "BYBIT_API_BASE_URL",
    "https://api.bybit.com",
).rstrip("/")
BYBIT_TICKERS_ENDPOINT = os.getenv(
    "BYBIT_TICKERS_ENDPOINT",
    "/v5/market/tickers",
)

REQUEST_TIMEOUT_SECONDS = int(os.getenv("BYBIT_PRICE_TIMEOUT_SECONDS", "15"))

# Optional fallbacks for symbols whose CoinGlass notation differs from Bybit.
# Direct SYMBOLUSDT lookup is always attempted first.
SYMBOL_ALIASES: Dict[str, Tuple[str, float]] = {
    "1000PEPE": ("PEPE", 1000.0),
}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _fetch_linear_mark_prices() -> Dict[str, float]:
    """Fetch all Bybit USDT-linear tickers and return pair -> mark price."""
    url = BYBIT_API_BASE_URL + BYBIT_TICKERS_ENDPOINT
    response = requests.get(
        url,
        params={"category": "linear"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()

    if int(payload.get("retCode", -1)) != 0:
        raise RuntimeError(
            f"Bybit API error retCode={payload.get('retCode')}: "
            f"{payload.get('retMsg', 'unknown error')}"
        )

    items = payload.get("result", {}).get("list", [])
    if not isinstance(items, list):
        raise RuntimeError("Bybit API returned an unexpected ticker payload")

    result: Dict[str, float] = {}
    for item in items:
        pair = str(item.get("symbol", "")).upper()
        raw_price = item.get("markPrice")
        if not pair or raw_price in (None, ""):
            continue
        try:
            result[pair] = float(raw_price)
        except (TypeError, ValueError):
            continue
    return result


def fetch_bybit_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    """Fetch Bybit USDT-linear mark prices only.

    No Spot fallback is used. A symbol without an active USDT-linear mark
    price is excluded from calculations and alerts.
    """
    requested = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    fetched_at = datetime.now(timezone.utc)

    provider_error = None
    try:
        futures_prices = _fetch_linear_mark_prices()
    except Exception as exc:
        futures_prices = {}
        provider_error = repr(exc)

    prices: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    for symbol in requested:
        # Prefer Bybit's exact pair (for example 1000PEPEUSDT).
        direct_pair = f"{symbol}USDT"
        pair = direct_pair
        multiplier = 1.0

        if direct_pair not in futures_prices:
            base, multiplier = SYMBOL_ALIASES.get(symbol, (symbol, 1.0))
            pair = f"{base}USDT"

        if pair not in futures_prices:
            missing.append(symbol)
            continue

        raw_price = futures_prices[pair]
        prices[symbol] = {
            "symbol": symbol,
            "pair": pair,
            "price": raw_price * multiplier,
            "raw_price": raw_price,
            "multiplier": multiplier,
            "source": "bybit_linear_mark",
            "fetched_at_utc": fetched_at.isoformat(),
        }

    return {
        "ok": bool(prices),
        "source": "bybit_linear_mark",
        "fetched_at_utc": fetched_at.isoformat(),
        "requested_count": len(requested),
        "found_count": len(prices),
        "missing_count": len(missing),
        "prices": prices,
        "missing_symbols": missing,
        "provider_error": provider_error,
        # Kept for compatibility with any existing diagnostics.
        "futures_error": provider_error,
    }


# Backward-compatible name so older callers do not break during deployment.
def fetch_binance_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    return fetch_bybit_usdt_prices(symbols)


def recalculate_distances(
    live_price: float,
    short_max_pain: Optional[float],
    long_max_pain: Optional[float],
) -> Dict[str, Optional[float]]:
    """Recalculate all Max Pain distances from the Bybit live price."""
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
    """Overlay Bybit USDT-linear mark prices on raw CoinGlass rows."""
    excluded = {str(x).upper() for x in excluded_symbols}
    raw_rows = [dict(row) for row in rows]
    symbols = sorted({
        str(row.get("symbol", "")).upper()
        for row in raw_rows
        if row.get("symbol") and str(row.get("symbol")).upper() not in excluded
    })

    price_result = fetch_bybit_usdt_prices(symbols)
    provider_error = price_result.get("provider_error")
    if provider_error and not price_result.get("prices"):
        raise RuntimeError(f"Bybit live-price request failed: {provider_error}")

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
