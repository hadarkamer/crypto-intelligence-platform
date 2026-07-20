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
BYBIT_BASE_URL = os.getenv(
    "BYBIT_BASE_URL",
    "https://api.bybit.com",
).rstrip("/")
BYBIT_TICKERS_ENDPOINT = os.getenv(
    "BYBIT_TICKERS_ENDPOINT",
    "/v5/market/tickers",
)
BYBIT_TIMEOUT_SECONDS = int(os.getenv("BYBIT_PRICE_TIMEOUT_SECONDS", "10"))
HYPERLIQUID_INFO_URL = os.getenv(
    "HYPERLIQUID_INFO_URL",
    "https://api.hyperliquid.xyz/info",
).rstrip("/")
HYPERLIQUID_TIMEOUT_SECONDS = int(os.getenv("HYPERLIQUID_PRICE_TIMEOUT_SECONDS", "10"))
COINGECKO_SIMPLE_PRICE_URL = os.getenv(
    "COINGECKO_SIMPLE_PRICE_URL",
    "https://api.coingecko.com/api/v3/simple/price",
)
COINGECKO_TIMEOUT_SECONDS = int(os.getenv("COINGECKO_PRICE_TIMEOUT_SECONDS", "10"))
COINPAPRIKA_BASE_URL = os.getenv(
    "COINPAPRIKA_BASE_URL",
    "https://api.coinpaprika.com/v1",
).rstrip("/")
COINPAPRIKA_TIMEOUT_SECONDS = int(os.getenv("COINPAPRIKA_PRICE_TIMEOUT_SECONDS", "10"))

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



def _fetch_bybit_hype_price(category: str) -> float:
    """Fetch HYPEUSDT from Bybit V5 tickers.

    ``linear`` is the preferred USDT perpetual market. ``spot`` is used only
    as a fallback. For linear contracts the mark price is preferred; when it
    is unavailable the latest traded price is used.
    """
    normalized_category = str(category or "").strip().lower()
    if normalized_category not in {"linear", "spot"}:
        raise ValueError(f"unsupported Bybit category: {category!r}")

    response = requests.get(
        BYBIT_BASE_URL + BYBIT_TICKERS_ENDPOINT,
        params={"category": normalized_category, "symbol": "HYPEUSDT"},
        timeout=BYBIT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Bybit tickers returned a non-object payload")
    if int(payload.get("retCode", -1)) != 0:
        raise ValueError(
            f"Bybit API error retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')!r}"
        )

    result = payload.get("result") or {}
    items = result.get("list") or []
    if not isinstance(items, list) or not items:
        raise ValueError(f"Bybit {normalized_category} returned no HYPEUSDT ticker")

    ticker = items[0] or {}
    candidate_fields = (
        ("markPrice", "lastPrice")
        if normalized_category == "linear"
        else ("lastPrice",)
    )
    for field in candidate_fields:
        raw_price = ticker.get(field)
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price

    raise ValueError(
        f"Bybit {normalized_category} HYPEUSDT ticker has no valid price"
    )


def _fetch_hyperliquid_hype_price() -> float:
    response = requests.post(
        HYPERLIQUID_INFO_URL,
        json={"type": "allMids"},
        timeout=HYPERLIQUID_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Hyperliquid allMids returned a non-object payload")
    raw_price = payload.get("HYPE")
    price = float(raw_price)
    if price <= 0:
        raise ValueError("Hyperliquid allMids returned an invalid HYPE price")
    return price


def _fetch_coingecko_hype_price() -> float:
    response = requests.get(
        COINGECKO_SIMPLE_PRICE_URL,
        params={"ids": "hyperliquid", "vs_currencies": "usd"},
        timeout=COINGECKO_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    raw_price = (payload.get("hyperliquid") or {}).get("usd") if isinstance(payload, dict) else None
    price = float(raw_price)
    if price <= 0:
        raise ValueError("CoinGecko returned an invalid HYPE price")
    return price


def _fetch_coinpaprika_hype_price() -> float:
    search_response = requests.get(
        COINPAPRIKA_BASE_URL + "/search",
        params={"q": "HYPE", "c": "currencies", "limit": 20},
        timeout=COINPAPRIKA_TIMEOUT_SECONDS,
    )
    search_response.raise_for_status()
    search_payload = search_response.json()
    currencies = search_payload.get("currencies") or [] if isinstance(search_payload, dict) else []
    coin_id = None
    for item in currencies:
        symbol = str(item.get("symbol", "")).upper()
        name = str(item.get("name", "")).lower()
        if symbol == "HYPE" and "hyperliquid" in name:
            coin_id = item.get("id")
            break
    if not coin_id:
        raise ValueError("CoinPaprika could not resolve the Hyperliquid HYPE coin id")

    ticker_response = requests.get(
        COINPAPRIKA_BASE_URL + f"/tickers/{coin_id}",
        timeout=COINPAPRIKA_TIMEOUT_SECONDS,
    )
    ticker_response.raise_for_status()
    ticker_payload = ticker_response.json()
    raw_price = ((ticker_payload.get("quotes") or {}).get("USD") or {}).get("price") if isinstance(ticker_payload, dict) else None
    price = float(raw_price)
    if price <= 0:
        raise ValueError("CoinPaprika returned an invalid HYPE price")
    return price


def _fetch_hype_fallback_price() -> Tuple[Optional[float], Optional[str], Dict[str, Optional[str]]]:
    errors: Dict[str, Optional[str]] = {
        "hyperliquid_error": None,
        "coingecko_error": None,
        "coinpaprika_error": None,
    }
    sources = (
        ("hyperliquid", _fetch_hyperliquid_hype_price),
        ("coingecko", _fetch_coingecko_hype_price),
        ("coinpaprika", _fetch_coinpaprika_hype_price),
    )
    for source, fetcher in sources:
        try:
            price = fetcher()
            print(f"[price] HYPE source={source} price={price}", flush=True)
            return price, source, errors
        except Exception as exc:
            key = f"{source}_error"
            errors[key] = repr(exc)
            print(f"[price] HYPE {source} failed: {errors[key]}", flush=True)
    return None, None, errors


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
    bybit_futures_error = None
    bybit_spot_error = None
    hype_fallback_errors: Dict[str, Optional[str]] = {
        "hyperliquid_error": None,
        "coingecko_error": None,
        "coinpaprika_error": None,
    }

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

    bybit_hype_price: Optional[float] = None
    bybit_hype_source: Optional[str] = None
    if "HYPE" in requested:
        try:
            bybit_hype_price = _fetch_bybit_hype_price("linear")
            bybit_hype_source = "bybit_futures_mark"
            print(
                f"[price] HYPE fetched from Bybit Futures: {bybit_hype_price}",
                flush=True,
            )
        except Exception as exc:
            bybit_futures_error = repr(exc)
            print(
                f"[price] HYPE Bybit Futures failed: {bybit_futures_error}",
                flush=True,
            )
            try:
                bybit_hype_price = _fetch_bybit_hype_price("spot")
                bybit_hype_source = "bybit_spot"
                print(
                    f"[price] HYPE fetched from Bybit Spot: {bybit_hype_price}",
                    flush=True,
                )
            except Exception as spot_exc:
                bybit_spot_error = repr(spot_exc)
                print(
                    f"[price] HYPE Bybit Spot failed: {bybit_spot_error}",
                    flush=True,
                )
                bybit_hype_price, bybit_hype_source, hype_fallback_errors = _fetch_hype_fallback_price()

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
        elif symbol == "HYPE" and bybit_hype_price is not None:
            raw_price = bybit_hype_price
            source = bybit_hype_source or "bybit_futures_mark"
            pair = "HYPEUSDT"
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
        "bybit_futures_error": bybit_futures_error,
        "bybit_spot_error": bybit_spot_error,
        **hype_fallback_errors,
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

    Symbols without a supported live price are excluded from live calculations. HYPE uses Bybit Futures, Bybit Spot, Hyperliquid, CoinGecko, CoinPaprika, and finally its CoinGlass row price.
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
        if not live and symbol == "HYPE":
            try:
                coinglass_price = float(row.get("current_price"))
            except (TypeError, ValueError):
                coinglass_price = 0.0
            if coinglass_price > 0:
                live = {
                    "symbol": "HYPE",
                    "pair": "HYPEUSDT",
                    "price": coinglass_price,
                    "raw_price": coinglass_price,
                    "multiplier": 1.0,
                    "source": "coinglass_dom",
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                print(f"[price] HYPE source=coinglass_dom price={coinglass_price}", flush=True)
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
