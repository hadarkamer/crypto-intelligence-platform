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
REQUEST_TIMEOUT_SECONDS = int(os.getenv("BINANCE_PRICE_TIMEOUT_SECONDS", "12"))

# CoinGlass symbol -> (Binance futures base symbol, price multiplier).
# Binance quotes 1000PEPE per 1000 tokens, while CoinGlass uses PEPE.
SYMBOL_ALIASES: Dict[str, Tuple[str, float]] = {
    "PEPE": ("1000PEPE", 0.001),
    "1000PEPE": ("1000PEPE", 1.0),
}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _fetch_futures_mark_prices() -> Dict[str, float]:
    """Fetch one coherent Binance USD-M Futures mark-price snapshot."""
    url = BINANCE_FUTURES_BASE_URL + BINANCE_FUTURES_MARK_ENDPOINT
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError("Binance Futures premiumIndex returned a non-list payload")

    result: Dict[str, float] = {}
    for item in payload:
        pair = str(item.get("symbol", "")).upper()
        raw_price = item.get("markPrice")
        if not pair or raw_price is None:
            continue
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            continue
        if price > 0:
            result[pair] = price
    if not result:
        raise RuntimeError("Binance Futures returned no usable mark prices")
    return result


def fetch_binance_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    """Fetch Binance USD-M Futures mark prices only.

    One bulk request is used for the entire scan so every timeframe of a symbol
    receives the same live price and timestamp. Spot is intentionally excluded.
    """
    requested = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    fetched_at = datetime.now(timezone.utc)
    error = None
    try:
        futures_prices = _fetch_futures_mark_prices()
    except Exception as exc:
        futures_prices = {}
        error = repr(exc)

    prices: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    for symbol in requested:
        base, multiplier = SYMBOL_ALIASES.get(symbol, (symbol, 1.0))
        pair = f"{base}USDT"
        raw_price = futures_prices.get(pair)
        if raw_price is None:
            missing.append(symbol)
            continue
        prices[symbol] = {
            "symbol": symbol,
            "pair": pair,
            "price": raw_price * multiplier,
            "raw_price": raw_price,
            "multiplier": multiplier,
            "source": "binance_futures_mark",
            "fetched_at_utc": fetched_at.isoformat(),
        }

    return {
        "ok": bool(prices),
        "source": "binance_futures_mark",
        "fetched_at_utc": fetched_at.isoformat(),
        "requested_count": len(requested),
        "found_count": len(prices),
        "missing_count": len(missing),
        "prices": prices,
        "missing_symbols": missing,
        "futures_error": error,
    }


def recalculate_distances(
    live_price: float,
    short_max_pain: Optional[float],
    long_max_pain: Optional[float],
) -> Dict[str, Optional[float]]:
    """Recalculate all Max Pain distances from the Futures mark price."""
    if not live_price:
        return {
            "short_signed_pct": None, "long_signed_pct": None,
            "short_abs_pct": None, "long_abs_pct": None,
            "short_abs_usd": None, "long_abs_usd": None,
            "closest_side": None,
        }
    short_signed = ((short_max_pain-live_price)/live_price*100 if short_max_pain is not None else None)
    long_signed = ((long_max_pain-live_price)/live_price*100 if long_max_pain is not None else None)
    short_abs = abs(short_signed) if short_signed is not None else None
    long_abs = abs(long_signed) if long_signed is not None else None
    short_abs_usd = abs(short_max_pain-live_price) if short_max_pain is not None else None
    long_abs_usd = abs(long_max_pain-live_price) if long_max_pain is not None else None

    # A crossed target is no longer active.
    active = []
    if short_signed is not None and short_signed > 0:
        active.append(("SHORT", short_abs))
    if long_signed is not None and long_signed < 0:
        active.append(("LONG", long_abs))
    closest = min(active, key=lambda item: item[1])[0] if active else None
    return {
        "short_signed_pct": short_signed, "long_signed_pct": long_signed,
        "short_abs_pct": short_abs, "long_abs_pct": long_abs,
        "short_abs_usd": short_abs_usd, "long_abs_usd": long_abs_usd,
        "closest_side": closest,
    }


def enrich_snapshot_rows(rows: Iterable[Any], excluded_symbols: Iterable[str] = ()) -> Dict[str, Any]:
    """Overlay one Futures mark-price snapshot on all CoinGlass rows."""
    excluded = {str(x).upper() for x in excluded_symbols}
    raw_rows = [dict(row) for row in rows]
    symbols = sorted({str(r.get("symbol", "")).upper() for r in raw_rows
                      if r.get("symbol") and str(r.get("symbol")).upper() not in excluded})
    price_result = fetch_binance_usdt_prices(symbols)
    enriched, skipped = [], []
    for row in raw_rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or symbol in excluded:
            continue
        live = price_result["prices"].get(symbol)
        if not live:
            skipped.append(symbol)
            continue
        calc = recalculate_distances(live["price"], row.get("short_max_pain"), row.get("long_max_pain"))
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
    return {"rows": enriched, "price_result": price_result, "skipped_symbols": sorted(set(skipped))}
