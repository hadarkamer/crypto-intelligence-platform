from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import websocket



# Binance USD-M Futures Mark Price websocket. This is the primary live-price source.
_DEFAULT_FUTURES_WS_URLS = (
    "wss://fstream.binance.com/ws/!markPrice@arr@1s",
)
BINANCE_FUTURES_WS_URLS = tuple(
    item.strip()
    for item in os.getenv(
        "BINANCE_FUTURES_WS_URLS",
        ",".join(_DEFAULT_FUTURES_WS_URLS),
    ).split(",")
    if item.strip()
)

# Binance USD-M Futures market-data hosts. The first reachable host is used.
# Multiple official hosts make collection more resilient to a temporary DNS/CDN issue.
_DEFAULT_FUTURES_HOSTS = (
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
)
BINANCE_FUTURES_BASE_URLS = tuple(
    item.strip().rstrip("/")
    for item in os.getenv(
        "BINANCE_FUTURES_BASE_URLS",
        ",".join(_DEFAULT_FUTURES_HOSTS),
    ).split(",")
    if item.strip()
)
BINANCE_FUTURES_MARK_ENDPOINT = os.getenv(
    "BINANCE_FUTURES_MARK_ENDPOINT",
    "/fapi/v1/premiumIndex",
)
BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT = os.getenv(
    "BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT",
    "/fapi/v1/exchangeInfo",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("BINANCE_PRICE_TIMEOUT_SECONDS", "15"))

# CoinGlass symbol -> fallback Binance base symbol and multiplier.
# Exact Futures pair (for example 1000PEPEUSDT) is always tried first.
SYMBOL_ALIASES: Dict[str, Tuple[str, float]] = {
    "1000PEPE": ("PEPE", 1000.0),
}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _get_json(url: str, params: Optional[Dict[str, str]] = None) -> Any:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _fetch_websocket_mark_prices() -> Tuple[Dict[str, float], Optional[str], List[str]]:
    """Read one complete Binance USD-M Futures Mark Price array from websocket."""
    errors: List[str] = []
    for url in BINANCE_FUTURES_WS_URLS:
        ws = None
        try:
            ws = websocket.create_connection(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                suppress_origin=True,
            )
            raw = ws.recv()
            payload = json.loads(raw)
            if isinstance(payload, dict) and "data" in payload:
                payload = payload["data"]
            if not isinstance(payload, list):
                raise ValueError("mark-price websocket response is not a list")

            prices: Dict[str, float] = {}
            for item in payload:
                if not isinstance(item, dict):
                    continue
                pair = str(item.get("s") or item.get("symbol") or "").upper()
                raw_price = item.get("p") if item.get("p") is not None else item.get("markPrice")
                if not pair or raw_price is None:
                    continue
                try:
                    price = float(raw_price)
                except (TypeError, ValueError):
                    continue
                if price > 0:
                    prices[pair] = price

            if not prices:
                raise ValueError("mark-price websocket returned no valid prices")
            return prices, url, errors
        except Exception as exc:
            errors.append(f"{url}: {exc!r}")
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
    return {}, None, errors


def _fetch_exchange_info() -> Tuple[Dict[str, List[str]], Optional[str], List[str]]:
    """Return baseAsset -> tradable USDT perpetual symbols.

    Exchange info is helpful but not mandatory: direct SYMBOLUSDT matching still works
    if this endpoint is temporarily unavailable.
    """
    errors: List[str] = []
    for host in BINANCE_FUTURES_BASE_URLS:
        try:
            payload = _get_json(host + BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT)
            mapping: Dict[str, List[str]] = {}
            for item in payload.get("symbols", []):
                pair = str(item.get("symbol", "")).upper()
                base_asset = str(item.get("baseAsset", "")).upper()
                quote_asset = str(item.get("quoteAsset", "")).upper()
                status = str(item.get("status", "")).upper()
                contract_type = str(item.get("contractType", "")).upper()
                if (
                    pair
                    and base_asset
                    and quote_asset == "USDT"
                    and status == "TRADING"
                    and contract_type == "PERPETUAL"
                ):
                    mapping.setdefault(base_asset, []).append(pair)
            return mapping, host, errors
        except Exception as exc:  # network/API fallback across official hosts
            errors.append(f"{host}: {exc!r}")
    return {}, None, errors


def _fetch_bulk_mark_prices() -> Tuple[Dict[str, float], Optional[str], List[str]]:
    errors: List[str] = []
    for host in BINANCE_FUTURES_BASE_URLS:
        try:
            payload = _get_json(host + BINANCE_FUTURES_MARK_ENDPOINT)
            if not isinstance(payload, list):
                raise ValueError("premiumIndex bulk response is not a list")
            prices: Dict[str, float] = {}
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
                    prices[pair] = price
            if not prices:
                raise ValueError("premiumIndex returned no valid prices")
            return prices, host, errors
        except Exception as exc:
            errors.append(f"{host}: {exc!r}")
    return {}, None, errors


def _fetch_direct_mark_price(pair: str, preferred_host: Optional[str] = None) -> Tuple[Optional[float], Optional[str], List[str]]:
    errors: List[str] = []
    hosts = list(BINANCE_FUTURES_BASE_URLS)
    if preferred_host and preferred_host in hosts:
        hosts.remove(preferred_host)
        hosts.insert(0, preferred_host)

    for host in hosts:
        try:
            payload = _get_json(
                host + BINANCE_FUTURES_MARK_ENDPOINT,
                params={"symbol": pair},
            )
            raw_price = payload.get("markPrice") if isinstance(payload, dict) else None
            price = float(raw_price)
            if price <= 0:
                raise ValueError("non-positive mark price")
            return price, host, errors
        except Exception as exc:
            errors.append(f"{host}/{pair}: {exc!r}")
    return None, None, errors


def _candidate_pairs(symbol: str, exchange_map: Dict[str, List[str]]) -> List[Tuple[str, float]]:
    """Return ordered Futures-pair candidates for a CoinGlass symbol."""
    symbol = _normalize_symbol(symbol)
    candidates: List[Tuple[str, float]] = []

    def add(pair: str, multiplier: float = 1.0) -> None:
        pair = pair.upper()
        entry = (pair, float(multiplier))
        if pair and entry not in candidates:
            candidates.append(entry)

    # Exact contract first. This resolves HYPE -> HYPEUSDT and preserves 1000-token contracts.
    add(f"{symbol}USDT", 1.0)

    # Official exchangeInfo matches by baseAsset.
    for pair in exchange_map.get(symbol, []):
        add(pair, 1.0)

    # Controlled fallback aliases only after exact matching.
    alias = SYMBOL_ALIASES.get(symbol)
    if alias:
        base, multiplier = alias
        add(f"{base}USDT", multiplier)
        for pair in exchange_map.get(base, []):
            add(pair, multiplier)

    return candidates


def fetch_binance_usdt_prices(symbols: Iterable[str]) -> Dict[str, Any]:
    """Fetch Binance USD-M Futures Mark Prices.

    Primary source: official Futures Mark Price websocket.
    Fallback: official REST premiumIndex bulk endpoint.
    Spot prices are never used.
    """
    requested = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    fetched_at = datetime.now(timezone.utc)

    websocket_prices, websocket_url, websocket_errors = _fetch_websocket_mark_prices()

    rest_prices: Dict[str, float] = {}
    rest_host: Optional[str] = None
    rest_errors: List[str] = []
    if not websocket_prices:
        rest_prices, rest_host, rest_errors = _fetch_bulk_mark_prices()

    bulk_prices = websocket_prices or rest_prices
    source_transport = "websocket" if websocket_prices else ("rest" if rest_prices else None)

    # Exact SYMBOLUSDT and controlled aliases resolve nearly all CoinGlass symbols.
    # exchangeInfo is queried only when at least one symbol remains unresolved, avoiding
    # long REST delays when the websocket already contains the needed contracts.
    exchange_map: Dict[str, List[str]] = {}
    exchange_host: Optional[str] = None
    exchange_errors: List[str] = []

    prelim_unresolved = []
    for symbol in requested:
        if not any(pair in bulk_prices for pair, _ in _candidate_pairs(symbol, {})):
            prelim_unresolved.append(symbol)
    if prelim_unresolved and bulk_prices:
        exchange_map, exchange_host, exchange_errors = _fetch_exchange_info()

    prices: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    diagnostics: Dict[str, Any] = {}

    for symbol in requested:
        candidates = _candidate_pairs(symbol, exchange_map)
        selected_pair: Optional[str] = None
        selected_multiplier = 1.0
        raw_price: Optional[float] = None
        direct_errors: List[str] = []
        direct_host: Optional[str] = None

        for pair, multiplier in candidates:
            if pair in bulk_prices:
                selected_pair = pair
                selected_multiplier = multiplier
                raw_price = bulk_prices[pair]
                break

        # A successful bulk source can occasionally omit a newly listed contract.
        # In that case only, try the exact candidates directly over REST.
        if raw_price is None and bulk_prices:
            for pair, multiplier in candidates:
                direct_price, host, errors = _fetch_direct_mark_price(pair, rest_host)
                direct_errors.extend(errors)
                if direct_price is not None:
                    selected_pair = pair
                    selected_multiplier = multiplier
                    raw_price = direct_price
                    direct_host = host
                    break

        diagnostics[symbol] = {
            "candidate_pairs": [pair for pair, _ in candidates],
            "exchange_info_pairs": exchange_map.get(symbol, []),
            "selected_pair": selected_pair,
            "bulk_match": bool(selected_pair and selected_pair in bulk_prices),
            "direct_host": direct_host,
            "direct_errors": direct_errors,
        }

        if raw_price is None or selected_pair is None:
            missing.append(symbol)
            continue

        prices[symbol] = {
            "symbol": symbol,
            "pair": selected_pair,
            "price": raw_price * selected_multiplier,
            "raw_price": raw_price,
            "multiplier": selected_multiplier,
            "source": "binance_futures_mark",
            "transport": source_transport if selected_pair in bulk_prices else "rest_direct",
            "fetched_at_utc": fetched_at.isoformat(),
        }

    return {
        "ok": bool(prices),
        "source": "binance_futures_mark",
        "transport": source_transport,
        "fetched_at_utc": fetched_at.isoformat(),
        "requested_count": len(requested),
        "found_count": len(prices),
        "missing_count": len(missing),
        "prices": prices,
        "missing_symbols": missing,
        "websocket_url": websocket_url,
        "websocket_errors": websocket_errors,
        "exchange_info_host": exchange_host,
        "exchange_info_errors": exchange_errors,
        "mark_price_host": rest_host,
        "mark_price_errors": rest_errors,
        "diagnostics": diagnostics,
    }


def recalculate_distances(
    live_price: float,
    short_max_pain: Optional[float],
    long_max_pain: Optional[float],
) -> Dict[str, Optional[float]]:
    """Recalculate all Max Pain distances from the Binance Futures Mark Price."""
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
    """Overlay Binance Futures Mark Prices on CoinGlass Max Pain rows."""
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
