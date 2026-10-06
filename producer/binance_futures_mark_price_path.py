"""Bounded HYPE USD-M Futures MARK candles, with explicit source provenance.

This is an independent archive supplement. It is never a Binance Spot route.
Official schema: https://developers.binance.com/en/docs/catalog/
core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
#mark-price-klinecandlestick-data
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from threading import Lock
from typing import Any, Callable
import urllib.error
from urllib.parse import urlencode
from urllib.parse import urlsplit
import urllib.request

import requests

from research_btc_parent_movement import validate_candle

METHOD_VERSION = "binance-futures-hype-mark-1m-v1"
SOURCE_URL = "https://fapi.binance.com/fapi/v1/markPriceKlines"
PROVENANCE = "OFFICIAL_BINANCE_USDM_HYPEUSDT_MARK_PRICE_KLINES_1M"
TRANSPORT_VERSION = "binance-futures-mark-egress-v1"
HTTPS_PROXY_ENV = "BINANCE_FUTURES_MARK_HTTPS_PROXY"
MAX_WINDOW_MINUTES = 1440
PAGE_LIMIT = 1500
REQUEST_TIMEOUT_SECONDS = 15
MAX_RESPONSE_BYTES = 2_000_000
RESPONSE_CHUNK_BYTES = 64 * 1024
INTERVAL = "1m"
INTERVAL_SECONDS = 60
INTERVAL_MS = 60_000
_MINUTE = timedelta(minutes=1)
_MILLISECOND = timedelta(milliseconds=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_STATUS_LOCK = Lock()
_ATTEMPT_SEQUENCE = 0
_STATUS = {
    "version": TRANSPORT_VERSION,
    "target": "OFFICIAL_BINANCE_USDM_MARK_PRICE_KLINES",
    "proxy_configured": False,
    "last_transport": None,
    "last_attempt_at_utc": None,
    "last_http_status": None,
    "last_error_type": None,
    "last_attempt_sequence": 0,
}


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


def _begin_attempt(*, proxy_configured: bool, attempted_at: str) -> int:
    global _ATTEMPT_SEQUENCE
    with _STATUS_LOCK:
        _ATTEMPT_SEQUENCE += 1
        sequence = _ATTEMPT_SEQUENCE
        _STATUS.update(
            proxy_configured=proxy_configured,
            last_transport="DEDICATED_HTTPS_PROXY" if proxy_configured else "DIRECT",
            last_attempt_at_utc=attempted_at,
            last_http_status=None,
            last_error_type=None,
            last_attempt_sequence=sequence,
        )
        return sequence


def _finish_attempt(sequence: int, **changes: Any) -> None:
    with _STATUS_LOCK:
        if _STATUS["last_attempt_sequence"] == sequence:
            _STATUS.update(changes)


def transport_status() -> dict[str, Any]:
    """Return diagnostics without exposing the proxy address or credentials."""
    with _STATUS_LOCK:
        value = deepcopy(_STATUS)
    value["proxy_configured"] = bool(os.getenv(HTTPS_PROXY_ENV, "").strip())
    return value


def _https_proxy() -> tuple[str | None, str | None]:
    raw = os.getenv(HTTPS_PROXY_ENV, "").strip()
    if not raw:
        return None, None
    invalid = False
    try:
        parsed = urlsplit(raw)
        # A path, query or fragment can silently turn a forward-proxy setting
        # into a request-rewriting endpoint. Only an authority is accepted.
        if (parsed.scheme != "https" or not parsed.hostname
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
            invalid = True
        _ = parsed.port
    except (TypeError, ValueError):
        invalid = True
    if invalid:
        return None, "INVALID_PROXY_CONFIGURATION"
    if urllib.request.proxy_bypass("fapi.binance.com"):
        return None, "PROXY_BYPASS_CONFIGURATION"
    return raw.rstrip("/"), None


def _requests_proxy_fetch(
    url: str, *, params: dict, timeout: int, proxy: str
) -> tuple[int | None, bytes, str | None, bool]:
    """Read one bounded response through an explicitly configured proxy.

    Requests/urllib3 supports TLS to an ``https://`` forward proxy. Disabling
    ``trust_env`` ensures ambient proxy and bypass variables cannot alter the
    selected route. Exceptions stay inside this helper so their messages,
    which can contain proxy credentials, are never retained on the public
    error's exception chain.
    """
    session = None
    response = None
    status_code = None
    body = b""
    transport_error_type = None
    response_too_large = False
    try:
        session = requests.Session()
        session.trust_env = False
        response = session.get(
            url,
            params=params,
            timeout=timeout,
            allow_redirects=False,
            stream=True,
            proxies={"https": proxy},
            verify=True,
        )
        status_code = int(response.status_code)
        if status_code == 200:
            chunks = []
            total_bytes = 0
            for chunk in response.iter_content(chunk_size=RESPONSE_CHUNK_BYTES):
                if not chunk:
                    continue
                total_bytes += len(chunk)
                if total_bytes > MAX_RESPONSE_BYTES:
                    response_too_large = True
                    break
                chunks.append(chunk)
            if not response_too_large:
                body = b"".join(chunks)
    except Exception as exc:
        transport_error_type = type(exc).__name__

    # Close outside the request handler. If either close fails, keep only its
    # class name; never retain a proxy-bearing exception object.
    if response is not None:
        try:
            response.close()
        except Exception as exc:
            if transport_error_type is None:
                transport_error_type = type(exc).__name__
    if session is not None:
        try:
            session.close()
        except Exception as exc:
            if transport_error_type is None:
                transport_error_type = type(exc).__name__
    return status_code, body, transport_error_type, response_too_large


def _stdlib_get(url: str, *, params: dict, timeout: int, allow_redirects: bool = False):
    """Match the injectable GET interface with a fixed, explicit transport."""
    if url != SOURCE_URL or allow_redirects:
        raise BinanceFuturesMarkPathError("Only the fixed non-redirecting MARK source is supported")
    attempted_at = datetime.now(timezone.utc).isoformat()
    proxy_configured = bool(os.getenv(HTTPS_PROXY_ENV, "").strip())
    sequence = _begin_attempt(proxy_configured=proxy_configured, attempted_at=attempted_at)
    proxy, configuration_error = _https_proxy()
    if configuration_error:
        _finish_attempt(sequence, last_error_type=configuration_error)
        if configuration_error == "PROXY_BYPASS_CONFIGURATION":
            raise BinanceFuturesMarkPathError(
                "Configured Futures MARK HTTPS proxy is bypassed"
            )
        raise BinanceFuturesMarkPathError(
            "Configured Futures MARK HTTPS proxy is invalid"
        )
    http_error_status = None
    transport_error_type = None
    response_too_large = False
    status_code = None
    body = b""
    if proxy:
        status_code, body, transport_error_type, response_too_large = (
            _requests_proxy_fetch(url, params=params, timeout=timeout, proxy=proxy)
        )
        # Do not retain a credential-bearing URL in a frame that can raise.
        proxy = None
    else:
        request = urllib.request.Request(url + "?" + urlencode(params), method="GET")
        try:
            # An explicit empty handler prevents ambient HTTPS_PROXY settings
            # from changing a route reported as DIRECT.
            opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({}), _RejectRedirects()
            )
            with opener.open(request, timeout=timeout) as response:
                status_code = int(response.status)
                body = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            http_error_status = int(exc.code)
        except Exception as exc:
            transport_error_type = type(exc).__name__
    # Raise only after leaving the handler so proxy-bearing exceptions are not
    # retained in __context__ or __cause__.
    if http_error_status is not None:
        _finish_attempt(sequence, last_http_status=http_error_status,
                        last_error_type="HTTPError")
        raise BinanceFuturesMarkPathError(
            f"Futures MARK source returned HTTP {http_error_status}"
        )
    if transport_error_type is not None:
        _finish_attempt(sequence, last_http_status=status_code,
                        last_error_type=transport_error_type)
        raise BinanceFuturesMarkPathError("Futures MARK transport failed")
    if status_code != 200:
        _finish_attempt(sequence, last_http_status=status_code,
                        last_error_type="UNEXPECTED_HTTP_STATUS")
        raise BinanceFuturesMarkPathError(
            f"Futures MARK source returned HTTP {status_code}"
        )
    if response_too_large or len(body) > MAX_RESPONSE_BYTES:
        _finish_attempt(sequence, last_http_status=status_code,
                        last_error_type="RESPONSE_TOO_LARGE")
        raise BinanceFuturesMarkPathError("Futures MARK response exceeds the byte budget")
    _finish_attempt(sequence, last_http_status=status_code, last_error_type=None)
    return _StdlibResponse(status_code, body)


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
    except BinanceFuturesMarkPathError:
        raise
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
