"""Shared bounded public Hyperliquid PERPETUAL TRADE minute source.

Only the six experimental-market inputs use this adapter.  No order, account,
Spot, MARK or Binance fallback API exists here.  Each request is at most 1,000
minutes and one HTTP attempt.  Closed candles are shared across workers under
per-symbol locks.  Current-minute rows are explicit, transient observations;
they never enter the closed cache or optional archive writer.

The provider's rolling 5,000-candle window cannot be extended by pagination.
Cached/archived older candles can be read, but missing old minutes fail closed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
import threading
import time

import requests

MINUTE_MS = 60_000
MAX_WINDOW_MINUTES = 1_000
RETENTION_CANDLES = 5_000
CACHE_CANDLES_PER_SYMBOL = 10_080
MAX_RESPONSE_BYTES = 2_000_000
TIMEOUT_SECONDS = 15
MAX_QUOTE_DECISION_AGE_MS = 5 * MINUTE_MS
SOURCE_URL = "https://api.hyperliquid.xyz/info"
SYMBOLS = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "XRP")
METHOD_VERSION = "experimental-hyperliquid-perpetual-trade-1m-v1"


class HyperliquidSourceError(ValueError):
    def __init__(self, code):
        self.code = "EXPERIMENTAL_HYPERLIQUID_" + code
        super().__init__(self.code)


def _fail(code):
    raise HyperliquidSourceError(code)


def _symbol(symbol):
    if symbol not in SYMBOLS:
        _fail("INVALID_SYMBOL")
    return symbol


def price_source(symbol):
    return "HYPERLIQUID_" + _symbol(symbol) + "_PERPETUAL_TRADE_1M"


def source_metadata(symbol):
    symbol = _symbol(symbol)
    return {"symbol": symbol, "price_source": price_source(symbol),
            "price_exchange": "hyperliquid", "price_market": "perpetual",
            "price_pair": symbol + "-PERP", "price_instrument": symbol,
            "price_kind": "TRADE", "interval": "1m",
            "method_version": METHOD_VERSION,
            "source_url": SOURCE_URL,
            "mark_price_equivalent": False}


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


def _canonical_row(raw, *, current_open):
    if (not isinstance(raw, (list, tuple)) or len(raw) != 5 or
            type(raw[0]) is not int or raw[0] < 0 or raw[0] % MINUTE_MS):
        _fail("INVALID_CANDLE_TIME")
    opened = raw[0]
    if opened > current_open:
        _fail("FUTURE_MINUTE")
    o, h, l, c = (_price(value) for value in raw[1:])
    if h < max(o, l, c) or l > min(o, h, c):
        _fail("INVALID_OHLC")
    return (opened, o, h, l, c)


class HyperliquidCandleSource:
    """Thread-safe source cache with explicit optional closed-bar persistence.

    Optional ``archive_read(symbol,start_ms,end_ms)`` returns any already
    archived canonical rows inside that interval (missing rows are allowed).
    ``archive_write(symbol,rows)`` receives only validated CLOSED tuples and
    must reject revisions.  Both callbacks must use this source's own namespace;
    this module never reads or writes another market's archive.  Archive errors
    fail closed, with no API or source fallback that could hide them.
    """

    def __init__(self, *, request_post=None, clock_ms=None, archive_read=None,
                 archive_write=None):
        self._post = request_post
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._archive_read = archive_read
        self._archive_write = archive_write
        self._rows = {symbol: {} for symbol in SYMBOLS}
        self._locks = {symbol: threading.Lock() for symbol in SYMBOLS}
        # Serialise actual HTTP calls across coins; cache hits never acquire it.
        self._request_lock = threading.Lock()
        self._rate_limit_until_ms = 0

    def _remember(self, symbol, rows):
        data = self._rows[symbol]
        for row in rows:
            existing = data.get(row[0])
            if existing is not None and existing != row:
                _fail("CLOSED_CANDLE_REVISION")
        data.update((row[0], row) for row in rows)

    def _prune(self, symbol):
        data = self._rows[symbol]
        if len(data) > CACHE_CANDLES_PER_SYMBOL:
            for opened in sorted(data)[:-CACHE_CANDLES_PER_SYMBOL]:
                del data[opened]

    def _load_archive(self, symbol, start, end, current_open):
        if not self._archive_read:
            return
        try:
            raw_rows = self._archive_read(symbol, start, end)
            if (not isinstance(raw_rows, (list, tuple)) or
                    len(raw_rows) > (end - start) // MINUTE_MS):
                _fail("INVALID_ARCHIVE_PAYLOAD")
            rows, seen = [], set()
            for raw in raw_rows:
                row = _canonical_row(raw, current_open=current_open)
                if not start <= row[0] < min(end, current_open):
                    _fail("INVALID_ARCHIVE_MINUTE")
                if row[0] in seen:
                    _fail("DUPLICATE_ARCHIVE_MINUTE")
                seen.add(row[0])
                rows.append(row)
            self._remember(symbol, rows)
        except HyperliquidSourceError:
            raise
        except Exception:
            _fail("ARCHIVE_READ_FAILED")

    def _request(self, symbol, start, end, current_open):
        # Cache/archive availability is checked before the retention restriction.
        if start < current_open - (RETENTION_CANDLES - 1) * MINUTE_MS:
            _fail("RETENTION_EXPIRED")
        with self._request_lock:
            if self._clock() < self._rate_limit_until_ms:
                _fail("RATE_LIMIT_COOLDOWN")
            try:
                response = (self._post or requests.post)(
                    SOURCE_URL, json={"type": "candleSnapshot", "req": {
                        "coin": symbol, "interval": "1m", "startTime": start,
                        "endTime": end - 1}},
                    timeout=TIMEOUT_SECONDS, allow_redirects=False)
                if type(response.status_code) is not int:
                    _fail("INVALID_HTTP_STATUS")
                if response.status_code != 200:
                    if response.status_code == 429:
                        self._rate_limit_until_ms = self._clock() + MINUTE_MS
                    _fail("HTTP_" + str(response.status_code))
                if len(response.content) > MAX_RESPONSE_BYTES:
                    _fail("RESPONSE_TOO_LARGE")
                payload = response.json()
            except HyperliquidSourceError:
                raise
            except Exception:
                _fail("REQUEST_FAILED")
        expected = (end - start) // MINUTE_MS
        if not isinstance(payload, list) or len(payload) > expected + 2:
            _fail("INVALID_PAYLOAD")
        rows, seen = [], set()
        for raw in payload:
            if (not isinstance(raw, dict) or raw.get("s") != symbol or
                    raw.get("i") != "1m"):
                _fail("INVALID_INSTRUMENT")
            opened, closed = raw.get("t"), raw.get("T")
            if (type(opened) is not int or type(closed) is not int or
                    closed != opened + MINUTE_MS - 1):
                _fail("INVALID_CANDLE_TIME")
            row = _canonical_row([opened] + [raw.get(k) for k in ("o", "h", "l", "c")],
                                 current_open=current_open)
            if opened < start - MINUTE_MS or opened > end:
                _fail("UNEXPECTED_CANDLE")
            if opened in seen:
                _fail("DUPLICATE_MINUTE")
            seen.add(opened)
            if start <= opened < end:
                rows.append(row)
        if [row[0] for row in rows] != list(range(start, end, MINUTE_MS)):
            _fail("INCOMPLETE_MINUTES")
        return rows

    def fetch_rows(self, symbol, start, end):
        """Return exact ``[open_ms,o,h,l,c]`` copies for aligned ``[start,end)``.

        ``end=current_minute_open+60_000`` explicitly permits a transient live
        candle. It is usable only as a live veto or its already observed OPEN,
        never as a closed feature, historical fill, or completed exit minute.
        """
        _symbol(symbol)
        if (type(start) is not int or type(end) is not int or start < 0 or
                start % MINUTE_MS or end % MINUTE_MS or end < start):
            _fail("INVALID_WINDOW")
        if end - start > MAX_WINDOW_MINUTES * MINUTE_MS:
            _fail("WINDOW_TOO_LARGE")
        moment = self._clock()
        if type(moment) is not int or moment < 0:
            _fail("INVALID_CLOCK")
        current_open = moment // MINUTE_MS * MINUTE_MS
        if end > current_open + MINUTE_MS:
            _fail("FUTURE_MINUTE")
        if start == end:
            return []
        with self._locks[symbol]:
            data = self._rows[symbol]
            wanted = range(start, end, MINUTE_MS)
            if any(t not in data for t in wanted if t < current_open):
                self._load_archive(symbol, start, min(end, current_open), current_open)
            missing = [t for t in wanted if t not in data or t == current_open]
            fetched = []
            if missing:
                # One bounded request spans missing minutes, even across cache
                # islands. It cannot turn a fragmented gap into many API calls.
                fetched = self._request(symbol, missing[0], missing[-1] + MINUTE_MS, current_open)
                closed = [row for row in fetched if row[0] < current_open]
                for row in closed:
                    existing = data.get(row[0])
                    if existing is not None and existing != row:
                        _fail("CLOSED_CANDLE_REVISION")
                if self._archive_write and closed:
                    try:
                        self._archive_write(symbol, closed)
                    except Exception:
                        _fail("ARCHIVE_WRITE_FAILED")
                self._remember(symbol, closed)
            result = {**data, **{row[0]: row for row in fetched}}
            if any(t not in result for t in wanted):
                _fail("INCOMPLETE_MINUTES")
            answer = [list(result[t]) for t in wanted]
            self._prune(symbol)
            return answer


def _archive_read(symbol, start, end):
    # Lazy imports prevent the archive's source wrapper from creating a cycle.
    from research_price_archive import experimental_hyperliquid_read
    return experimental_hyperliquid_read(symbol, start, end)


def _archive_write(symbol, rows):
    from research_price_archive import experimental_hyperliquid_write
    return experimental_hyperliquid_write(symbol, rows)


_SHARED_SOURCE = HyperliquidCandleSource(
    archive_read=_archive_read, archive_write=_archive_write)


def fetch_rows(symbol, start_ms, end_ms):
    return _SHARED_SOURCE.fetch_rows(symbol, start_ms, end_ms)


def closed_quote(symbol, decision_ms, *, now_ms=None, fetch=None):
    """Causal last CLOSED minute close at an explicitly fresh decision time.

    The quote's as-of time is the candle close, not the later retrieval time.
    At 12:34:30 the price is the close of 12:33, known by 12:34:00. No current
    candle close or eventual final OHLC can leak into the decision price.
    """
    _symbol(symbol)
    moment = int(time.time() * 1000) if now_ms is None else now_ms
    if (type(decision_ms) is not int or type(moment) is not int or
            decision_ms < MINUTE_MS or decision_ms > moment or
            moment - decision_ms > MAX_QUOTE_DECISION_AGE_MS):
        _fail("STALE_OR_FUTURE_DECISION")
    end = decision_ms // MINUTE_MS * MINUTE_MS
    rows = (fetch or fetch_rows)(symbol, end - MINUTE_MS, end)
    if len(rows) != 1:
        _fail("INCOMPLETE_QUOTE")
    row = _canonical_row(rows[0], current_open=moment // MINUTE_MS * MINUTE_MS)
    if row[0] != end - MINUTE_MS:
        _fail("INCORRECT_QUOTE_MINUTE")
    asof = end - 1
    return {**source_metadata(symbol), "price": row[4], "current_price": row[4],
            "asof_ms": asof, "candle_open_ms": row[0], "retrieved_at_ms": moment,
            "price_fetched_at_utc": datetime.fromtimestamp(asof / 1000, timezone.utc).isoformat(),
            "quote_basis": "LAST_CLOSED_TRADE_1M_CLOSE"}
