from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


BINANCE_FUTURES_BASE_URL = os.getenv(
    "BINANCE_FUTURES_BASE_URL",
    "https://fapi.binance.com",
).rstrip("/")

# Binance exposes several public USD-M Futures API edge hosts. Render can
# occasionally fail against one edge while another remains available. Keep the
# configured host first, then try only equivalent Futures endpoints.
BINANCE_FUTURES_BASE_URLS: List[str] = []
for _base in [
    BINANCE_FUTURES_BASE_URL,
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
    "https://www.binance.com",
]:
    _base = _base.rstrip("/")
    if _base and _base not in BINANCE_FUTURES_BASE_URLS:
        BINANCE_FUTURES_BASE_URLS.append(_base)
BINANCE_FUTURES_MARK_ENDPOINT = os.getenv(
    "BINANCE_FUTURES_MARK_ENDPOINT",
    "/fapi/v1/premiumIndex",
)
BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT = os.getenv(
    "BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT",
    "/fapi/v1/exchangeInfo",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("BINANCE_PRICE_TIMEOUT_SECONDS", "15"))

# Optional explicit overrides for CoinGlass names that do not match Binance's
# futures base asset. The tuple is: (Binance futures pair, multiplier applied
# to the returned mark price). Keep this table small; the exchange-info resolver
# handles normal symbols such as BTC, ETH and HYPE automatically.
FUTURES_PAIR_OVERRIDES: Dict[str, Tuple[str, float]] = {
    # Example only: "LUNA2": ("LUNA2USDT", 1.0),
}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _request_json(url: str, *, params: Optional[Dict[str, str]] = None) -> Any:
    response = requests.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; CoinGlassTracker/1.0)",
            "Accept": "application/json",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("code") not in (None, 0, 200):
        raise RuntimeError(f"Binance API error: {payload}")
    return payload


def _request_futures_path(
    path: str,
    *,
    params: Optional[Dict[str, str]] = None,
) -> Tuple[Any, str]:
    errors: List[str] = []
    for base in BINANCE_FUTURES_BASE_URLS:
        try:
            payload = _request_json(base + path, params=params)
            return payload, base
        except Exception as exc:
            errors.append(f"{base}: {exc!r}")
    raise RuntimeError("All Binance Futures API hosts failed: " + " | ".join(errors))


def _fetch_futures_exchange_info() -> Dict[str, Any]:
    """Return Binance USD-M perpetual symbol metadata.

    The resolver uses exchangeInfo instead of assuming every CoinGlass symbol is
    exactly SYMBOLUSDT. This is important for newly listed contracts and symbols
    whose exchange ticker differs from their display name.
    """
    payload, _ = _request_futures_path(BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT)

    by_pair: Dict[str, Dict[str, Any]] = {}
    by_base: Dict[str, List[str]] = {}

    for item in payload.get("symbols", []):
        pair = _normalize_symbol(item.get("symbol"))
        base = _normalize_symbol(item.get("baseAsset"))
        quote = _normalize_symbol(item.get("quoteAsset"))
        contract_type = _normalize_symbol(item.get("contractType"))
        status = _normalize_symbol(item.get("status"))

        if not pair or not base:
            continue
        if quote != "USDT" or contract_type != "PERPETUAL":
            continue
        # Keep TRADING first, but retain non-trading metadata for diagnostics.
        by_pair[pair] = dict(item)
        by_base.setdefault(base, []).append(pair)

    for base, pairs in by_base.items():
        pairs.sort(key=lambda p: (
            _normalize_symbol(by_pair[p].get("status")) != "TRADING",
            p != f"{base}USDT",
            p,
        ))

    return {"by_pair": by_pair, "by_base": by_base}


def _fetch_futures_mark_prices() -> Dict[str, float]:
    payload, _ = _request_futures_path(BINANCE_FUTURES_MARK_ENDPOINT)

    result: Dict[str, float] = {}
    for item in payload:
        pair = _normalize_symbol(item.get("symbol"))
        raw_price = item.get("markPrice")
        if not pair or raw_price is None:
            continue
        try:
            result[pair] = float(raw_price)
        except (TypeError, ValueError):
            continue
    return result


def _fetch_single_futures_mark_price(pair: str) -> Optional[float]:
    """Direct fallback for a contract missing from the bulk premiumIndex reply."""
    try:
        payload, _ = _request_futures_path(
            BINANCE_FUTURES_MARK_ENDPOINT,
            params={"symbol": pair},
        )
        value = payload.get("markPrice") if isinstance(payload, dict) else None
        return float(value) if value is not None else None
    except Exception:
        return None


def _resolve_futures_pair(
    symbol: str,
    exchange_info: Dict[str, Any],
    available_marks: Dict[str, float],
) -> Tuple[Optional[str], float, List[str]]:
    """Resolve a CoinGlass symbol to the best Binance USD-M perpetual pair.

    Resolution order:
    1. Explicit override.
    2. Exact SYMBOLUSDT contract.
    3. exchangeInfo baseAsset match (handles HYPE and future listings).
    4. Unique mark-price pair beginning with the symbol (defensive fallback).
    """
    normalized = _normalize_symbol(symbol)
    candidates: List[str] = []

    override = FUTURES_PAIR_OVERRIDES.get(normalized)
    if override:
        candidates.append(_normalize_symbol(override[0]))

    exact = f"{normalized}USDT"
    candidates.append(exact)
    candidates.extend(exchange_info.get("by_base", {}).get(normalized, []))

    # Defensive fallback. Only accept a unique match to avoid mapping e.g. ETH
    # to an unrelated prefixed contract.
    prefix_matches = sorted(
        pair for pair in available_marks
        if pair.startswith(normalized) and pair.endswith("USDT")
    )
    if len(prefix_matches) == 1:
        candidates.extend(prefix_matches)

    seen = set()
    ordered = []
    for pair in candidates:
        if pair and pair not in seen:
            seen.add(pair)
            ordered.append(pair)

    by_pair = exchange_info.get("by_pair", {})
    ordered.sort(key=lambda pair: (
        pair not in available_marks,
        _normalize_symbol(by_pair.get(pair, {}).get("status")) != "TRADING",
        pair != exact,
        pair,
    ))

    multiplier = override[1] if override else 1.0
    for pair in ordered:
        if pair in available_marks or pair in by_pair:
            return pair, multiplier, ordered

    # If exchangeInfo or the bulk response is temporarily unavailable, still
    # try the canonical Futures contract directly. The previous code returned
    # None here, skipped every symbol and made an otherwise valid CoinGlass
    # collection fail with zero complete symbols.
    return (ordered[0] if ordered else exact), multiplier, ordered


def fetch_binance_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    """Fetch Binance USD-M Futures Mark Prices.

    Mark Price is the sole canonical price source for scoring and alerts. Symbol
    resolution is based on Binance Futures exchangeInfo, rather than relying only
    on a hard-coded SYMBOLUSDT guess. This allows contracts such as HYPEUSDT to
    be discovered automatically when Binance lists them.
    """
    requested = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    fetched_at = datetime.now(timezone.utc)

    exchange_info_error = None
    futures_error = None

    try:
        exchange_info = _fetch_futures_exchange_info()
    except Exception as exc:
        exchange_info = {"by_pair": {}, "by_base": {}}
        exchange_info_error = repr(exc)

    try:
        futures_prices = _fetch_futures_mark_prices()
    except Exception as exc:
        futures_prices = {}
        futures_error = repr(exc)

    prices: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    diagnostics: Dict[str, Dict[str, Any]] = {}

    for symbol in requested:
        pair, multiplier, candidates = _resolve_futures_pair(
            symbol, exchange_info, futures_prices
        )
        raw_price = futures_prices.get(pair) if pair else None

        # A newly listed contract can occasionally be absent from the bulk reply
        # for a short period. Query it directly once before declaring it missing.
        if pair and raw_price is None:
            raw_price = _fetch_single_futures_mark_price(pair)

        diagnostics[symbol] = {
            "resolved_pair": pair,
            "candidate_pairs": candidates,
            "exchange_info_match": bool(pair and pair in exchange_info.get("by_pair", {})),
            "bulk_mark_match": bool(pair and pair in futures_prices),
        }

        if not pair or raw_price is None:
            missing.append(symbol)
            continue

        metadata = exchange_info.get("by_pair", {}).get(pair, {})
        prices[symbol] = {
            "symbol": symbol,
            "pair": pair,
            "price": raw_price * multiplier,
            "raw_price": raw_price,
            "multiplier": multiplier,
            "source": "binance_futures_mark",
            "contract_status": metadata.get("status"),
            "contract_type": metadata.get("contractType"),
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
        "symbol_diagnostics": diagnostics,
        "exchange_info_error": exchange_info_error,
        "futures_error": futures_error,
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
    """Overlay Binance Futures Mark Prices on raw CoinGlass rows."""
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
