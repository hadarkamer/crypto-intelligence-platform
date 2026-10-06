"""Bounded HYPE USD-M Futures MARK candles, with explicit source provenance.

This is an independent archive supplement. It is never a Binance Spot route.
Official schema: https://developers.binance.com/en/docs/catalog/
core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
#mark-price-klinecandlestick-data
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Callable
from urllib.parse import urlencode
import urllib.request

from research_btc_parent_movement import validate_candle

METHOD_VERSION = "binance-futures-hype-mark-1m-v1"
SOURCE_URL = "https://fapi.binance.com/fapi/v1/markPriceKlines"
PROVENANCE = "OFFICIAL_BINANCE_USDM_HYPEUSDT_MARK_PRICE_KLINES_1M"
MAX_WINDOW_MINUTES = 1440
PAGE_LIMIT = 1500
REQUEST_TIMEOUT_SECONDS = 15
MAX_RESPONSE_BYTES = 2_000_000
INTERVAL = "1m"
INTERVAL_SECONDS = 60
INTERVAL_MS = 60_000
_MINUTE = timedelta(minutes=1)
_MILLISECOND = timedelta(milliseconds=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class BinanceFuturesMarkPathError(RuntimeError):
    """The fixed Futures MARK source returned unsafe or unavailable data."""


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise BinanceFuturesMarkPathError("Futures MARK source redirects are prohibited")


class _StdlibResponse:
    def __init__(self, status_code: int, body: bytes):
        self.status_code = status_code
        self._body = body

    def raise_for_status(self):
        if self.status_code != 200:
            raise BinanceFuturesMarkPathError("Futures MARK source did not return HTTP 200")

    def json(self):
        return json.loads(self._body)


def _stdlib_get(url: str, *, params: dict, timeout: int, allow_redirects: bool = False):
    """Match the injectable GET interface using only the standard library."""
    if url != SOURCE_URL or allow_redirects:
        raise BinanceFuturesMarkPathError("Only the fixed non-redirecting MARK source is supported")
    request = urllib.request.Request(url + "?" + urlencode(params), method="GET")
    opener = urllib.request.build_opener(_RejectRedirects())
    with opener.open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise BinanceFuturesMarkPathError("Futures MARK response exceeds the byte budget")
        return _StdlibResponse(response.status, body)


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Futures MARK boundaries require an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def _milliseconds(value: datetime) -> int:
    return (value - _EPOCH) // _MILLISECOND


def _parse_candle(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, (list, tuple)) or len(raw) != 12:
        raise BinanceFuturesMarkPathError("Futures MARK kline must have 12 fields")
    if type(raw[0]) is not int or type(raw[6]) is not int:
        raise BinanceFuturesMarkPathError("Futures MARK timestamps must be integer milliseconds")
    try:
        # Only reuse OHLC/time validation; no BTC or Spot source metadata.
        return validate_candle({
            "open_time_utc": _EPOCH + raw[0] * _MILLISECOND,
            "close_time_utc": _EPOCH + raw[6] * _MILLISECOND,
            **dict(zip(("open", "high", "low", "close"), raw[1:5])),
        })
    except (TypeError, ValueError, OverflowError) as exc:
        raise BinanceFuturesMarkPathError("Futures MARK kline has invalid one-minute OHLC data") from exc


def fetch_closed_candles(
    symbol: str,
    start_time: Any,
    end_time: Any,
    *,
    request_get: Callable[..., Any] = _stdlib_get,
) -> dict[str, Any]:
    """Return closed HYPE MARK candles for a timezone-aware window of at most 1 day.

    A partial first minute is excluded. The end is an inclusive close-time
    cutoff, and must not be in the future. One bounded HTTP request is enough
    for the maximum 1,440 eligible bars. Gaps stay missing; prices are never
    filled, scaled, or substituted. Identical duplicate bars are counted once.
    """
    if not isinstance(symbol, str) or symbol.strip().upper() != "HYPE":
        raise ValueError("The Futures MARK supplement supports HYPE only")
    start, end = _utc(start_time), _utc(end_time)
    if end <= start or end - start > timedelta(minutes=MAX_WINDOW_MINUTES):
        raise ValueError("Futures MARK window must be positive and at most 1440 minutes")
    if start < _EPOCH or end > datetime.now(timezone.utc):
        raise ValueError("Futures MARK window must be historical")
    first = start.replace(second=0, microsecond=0)
    if first < start:
        first += _MINUTE
    first_ms = _milliseconds(first)
    last_open_ms = ((_milliseconds(end) + 1) // INTERVAL_MS - 1) * INTERVAL_MS
    expected = max(0, (last_open_ms - first_ms) // INTERVAL_MS + 1)
    result = {
        "symbol": "HYPE", "pair": "HYPEUSDT", "exchange": "binance",
        "market": "futures", "price_kind": "MARK", "interval": INTERVAL,
        "interval_seconds": INTERVAL_SECONDS, "source_url": SOURCE_URL,
        "method_version": METHOD_VERSION, "provenance": PROVENANCE,
        "candles": [], "expected_candles": expected, "missing_candles": expected,
        "duplicate_candles": 0, "request_count": 0, "complete": expected == 0,
    }
    if not expected:
        return result
    last_close_ms = last_open_ms + INTERVAL_MS - 1
    try:
        response = request_get(
            SOURCE_URL,
            params={"symbol": "HYPEUSDT", "interval": INTERVAL,
                    "startTime": first_ms, "endTime": last_close_ms, "limit": PAGE_LIMIT},
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        response.raise_for_status()
        if response.status_code != 200:
            raise BinanceFuturesMarkPathError("Futures MARK source did not return HTTP 200")
        payload = response.json()
    except Exception as exc:
        raise BinanceFuturesMarkPathError("Official Futures MARK request failed") from exc
    if not isinstance(payload, list) or len(payload) > PAGE_LIMIT:
        raise BinanceFuturesMarkPathError("Futures MARK returned an invalid or oversized API payload")
    by_open: dict[int, dict[str, Any]] = {}
    duplicates = 0
    for raw in payload:
        candle = _parse_candle(raw)
        opened = _milliseconds(candle["open_time_utc"])
        closed = _milliseconds(candle["close_time_utc"])
        if opened < first_ms or opened > last_open_ms or closed > last_close_ms:
            raise BinanceFuturesMarkPathError("Futures MARK returned a candle outside the closed window")
        if opened in by_open:
            if by_open[opened] != candle:
                raise BinanceFuturesMarkPathError("Futures MARK returned conflicting duplicate candles")
            duplicates += 1
        else:
            by_open[opened] = candle
    candles = [by_open[key] for key in sorted(by_open)]
    missing = expected - len(candles)
    result.update({"candles": candles, "missing_candles": missing,
                   "duplicate_candles": duplicates, "request_count": 1,
                   "complete": missing == 0})
    return result
