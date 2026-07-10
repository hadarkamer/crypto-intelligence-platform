from __future__ import annotations
from datetime import datetime, timezone
import os
from typing import Any, Dict, Iterable, List, Optional
import requests

BINANCE_MARKET_DATA_BASE_URL = os.getenv(
    "BINANCE_MARKET_DATA_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")
BINANCE_PRICE_ENDPOINT = os.getenv(
    "BINANCE_PRICE_ENDPOINT",
    "/api/v3/ticker/price",
)
BINANCE_ALL_PRICES_URL = BINANCE_MARKET_DATA_BASE_URL + BINANCE_PRICE_ENDPOINT
REQUEST_TIMEOUT_SECONDS = int(os.getenv("BINANCE_PRICE_TIMEOUT_SECONDS", "15"))
SYMBOL_ALIASES: Dict[str, str] = {}

def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()

def fetch_binance_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    requested = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    fetched_at = datetime.now(timezone.utc)
    response = requests.get(BINANCE_ALL_PRICES_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()

    pair_prices: Dict[str, float] = {}
    for item in payload:
        pair = str(item.get("symbol", "")).upper()
        try:
            pair_prices[pair] = float(item.get("price"))
        except Exception:
            continue

    prices = {}
    missing: List[str] = []
    for symbol in requested:
        base = SYMBOL_ALIASES.get(symbol, symbol)
        pair = f"{base}USDT"
        price = pair_prices.get(pair)
        if price is None:
            missing.append(symbol)
        else:
            prices[symbol] = {
                "symbol": symbol,
                "pair": pair,
                "price": price,
                "source": "binance",
                "fetched_at_utc": fetched_at.isoformat(),
            }

    return {
        "ok": True,
        "source": "binance",
        "fetched_at_utc": fetched_at.isoformat(),
        "requested_count": len(requested),
        "found_count": len(prices),
        "missing_count": len(missing),
        "prices": prices,
        "missing_symbols": missing,
    }

def recalculate_distances(live_price: float, short_max_pain: Optional[float], long_max_pain: Optional[float]) -> Dict[str, Optional[float]]:
    if not live_price:
        return {"short_signed_pct": None, "long_signed_pct": None, "short_abs_pct": None, "long_abs_pct": None, "closest_side": None}

    short_signed = ((short_max_pain - live_price) / live_price * 100) if short_max_pain is not None else None
    long_signed = ((long_max_pain - live_price) / live_price * 100) if long_max_pain is not None else None
    short_abs = abs(short_signed) if short_signed is not None else None
    long_abs = abs(long_signed) if long_signed is not None else None

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
        "closest_side": closest,
    }
