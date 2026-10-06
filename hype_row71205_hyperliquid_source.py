"""Bounded public HYPE perpetual TRADE minutes for the ROW71205 source variant.

This is a different market from the original Binance HYPEUSDT research.
There is no exchange fallback and no order/account API.  Rows cover exactly
``[start, end)``.  Callers request only closed minutes for features/monitoring;
an explicitly requested current minute may supply its OPEN or a live veto.
That unfinished row must never enter the closed-minute cache.
"""
from math import isfinite
import time

import requests

MINUTE_MS = 60_000
MAX_WINDOW_MINUTES = 2_000
RETENTION_CANDLES = 5_000
MAX_RESPONSE_BYTES = 2_000_000
TIMEOUT_SECONDS = 15
SOURCE_URL = "https://api.hyperliquid.xyz/info"
PRICE_SOURCE = "HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M"


class HyperliquidSourceError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _fail(suffix):
    raise HyperliquidSourceError("HYPE_HYPERLIQUID_" + suffix)


def _price(value):
    if isinstance(value, bool):
        _fail("INVALID_PRICE")
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        _fail("INVALID_PRICE")
    if not isfinite(value) or value <= 0:
        _fail("INVALID_PRICE")
    return value


def fetch_rows(symbol, start, end, *, request_post=None, clock_ms=None):
    """Return complete canonical ``[open_ms, open, high, low, close]`` rows.

    A current-minute request is explicit when ``end`` is the next minute
    boundary. Future opens, missing minutes and expired retention fail closed.
    The rolling 5,000-candle limit cannot be extended by pagination.
    """
    if symbol != "HYPE":
        _fail("INVALID_SYMBOL")
    if (type(start) is not int or type(end) is not int or start < 0 or
            start % MINUTE_MS or end % MINUTE_MS or end < start):
        _fail("INVALID_WINDOW")
    expected = (end - start) // MINUTE_MS
    if expected > MAX_WINDOW_MINUTES:
        _fail("WINDOW_TOO_LARGE")
    moment = clock_ms() if clock_ms else int(time.time() * 1000)
    current_open = moment // MINUTE_MS * MINUTE_MS
    if end > current_open + MINUTE_MS:
        _fail("FUTURE_MINUTE")
    if not expected:
        return []
    # Conservatively include the current candle in the provider's 5,000 rows.
    if start < current_open - (RETENTION_CANDLES - 1) * MINUTE_MS:
        _fail("RETENTION_EXPIRED")
    try:
        response = (request_post or requests.post)(
            SOURCE_URL,
            json={"type": "candleSnapshot", "req": {
                "coin": "HYPE", "interval": "1m", "startTime": start,
                "endTime": end - 1}},
            timeout=TIMEOUT_SECONDS, allow_redirects=False,
        )
        if type(response.status_code) is not int:
            _fail("INVALID_HTTP_STATUS")
        if response.status_code != 200:
            _fail("HTTP_" + str(response.status_code))
        if len(response.content) > MAX_RESPONSE_BYTES:
            _fail("RESPONSE_TOO_LARGE")
        payload = response.json()
    except HyperliquidSourceError:
        raise
    except Exception:
        # Do not expose response bodies, request details or arbitrary exceptions.
        _fail("REQUEST_FAILED")
    # The official endpoint can return one surrounding candle on either side.
    if not isinstance(payload, list) or len(payload) > expected + 2:
        _fail("INVALID_PAYLOAD")
    rows, seen = [], set()
    for raw in payload:
        if (not isinstance(raw, dict) or raw.get("s") != "HYPE" or
                raw.get("i") != "1m"):
            _fail("INVALID_INSTRUMENT")
        opened, closed = raw.get("t"), raw.get("T")
        if (type(opened) is not int or type(closed) is not int or
                opened % MINUTE_MS or closed != opened + MINUTE_MS - 1):
            _fail("INVALID_CANDLE_TIME")
        if opened > current_open:
            _fail("FUTURE_MINUTE")
        if opened < start - MINUTE_MS or opened > end:
            _fail("UNEXPECTED_CANDLE")
        if opened in seen:
            _fail("DUPLICATE_MINUTE")
        seen.add(opened)
        o, h, l, c = (_price(raw.get(key)) for key in ("o", "h", "l", "c"))
        if h < max(o, l, c) or l > min(o, h, c):
            _fail("INVALID_OHLC")
        if start <= opened < end:
            rows.append([opened, o, h, l, c])
    if [row[0] for row in rows] != list(range(start, end, MINUTE_MS)):
        _fail("INCOMPLETE_MINUTES")
    return rows
