"""Official HYPE perpetual TRADE candles; distinct from Spot @107 and MARK.

Uses Hyperliquid's public info endpoint and the native perpetual instrument
HYPE. Only closed one-minute candles within a bounded requested window pass.
The provider's rolling 5,000-candle limit cannot be extended by pagination.
"""
from datetime import datetime, timedelta, timezone
from typing import Callable

import requests

from research_btc_parent_movement import validate_candle

SOURCE_URL = "https://api.hyperliquid.xyz/info"
METHOD_VERSION = "hyperliquid-hype-perp-trade-1m-v1"
PROVENANCE = "OFFICIAL_HYPERLIQUID_HYPE_PERPETUAL_TRADE_CANDLES_1M"
SOURCE = {"symbol": "HYPE", "pair": "HYPE-PERP", "exchange": "hyperliquid",
    "market": "perpetual", "price_kind": "TRADE", "instrument": "HYPE",
    "margin_currency": "USDC", "interval": "1m", "interval_seconds": 60,
    "source_url": SOURCE_URL, "method_version": METHOD_VERSION, "provenance": PROVENANCE}
MAX_WINDOW_MINUTES = 1440
RETENTION_CANDLES = 5000
MAX_RESPONSE_BYTES = 2_000_000
TIMEOUT_SECONDS = 15


class HyperliquidPerpPathError(RuntimeError):
    pass


def _utc(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Hyperliquid perpetual boundaries require explicit timezone")
    return value.astimezone(timezone.utc)


def fetch_closed_candles(symbol, start_time, end_time, *, request_post: Callable = requests.post):
    if symbol != "HYPE":
        raise ValueError("The explicit perpetual route accepts HYPE only")
    start, end = _utc(start_time), _utc(end_time)
    if end <= start or end - start > timedelta(minutes=MAX_WINDOW_MINUTES):
        raise ValueError("Perpetual candle window must be positive and at most 1440 minutes")
    if end > datetime.now(timezone.utc):
        raise ValueError("Perpetual path must be historical")
    first = start.replace(second=0, microsecond=0)
    if first < start:
        first += timedelta(minutes=1)
    end_ms = int(end.timestamp() * 1000)
    last_open_ms = ((end_ms + 1) // 60000 - 1) * 60000
    first_ms = int(first.timestamp() * 1000)
    expected = max(0, (last_open_ms - first_ms) // 60000 + 1)
    result = {**SOURCE, "candles": [], "expected_candles": expected,
        "missing_candles": expected, "duplicate_candles": 0, "request_count": 0,
        "retention_candles": RETENTION_CANDLES, "complete": expected == 0}
    if not expected:
        return result
    try:
        response = request_post(SOURCE_URL,
            json={"type": "candleSnapshot", "req": {"coin": "HYPE", "interval": "1m",
                "startTime": first_ms, "endTime": last_open_ms + 59999}},
            timeout=TIMEOUT_SECONDS, allow_redirects=False)
        if response.status_code != 200:
            raise HyperliquidPerpPathError(f"Official perpetual source HTTP {response.status_code}")
        content = getattr(response, "content", b"")
        if len(content) > MAX_RESPONSE_BYTES:
            raise HyperliquidPerpPathError("Perpetual candle response exceeds byte budget")
        payload = response.json()
    except HyperliquidPerpPathError:
        raise
    except Exception as exc:
        raise HyperliquidPerpPathError(f"Official perpetual request failed ({type(exc).__name__})") from exc
    # Some responses include one surrounding candle; only verified instrument
    # rows fully inside the requested closed window are eligible.
    if not isinstance(payload, list) or len(payload) > MAX_WINDOW_MINUTES + 2:
        raise HyperliquidPerpPathError("Invalid or oversized perpetual candle payload")
    bars, duplicates = {}, 0
    for raw in payload:
        if (not isinstance(raw, dict) or raw.get("s") != "HYPE" or raw.get("i") != "1m"
            or type(raw.get("t")) is not int or type(raw.get("T")) is not int):
            raise HyperliquidPerpPathError("Candle does not prove HYPE perpetual 1m instrument")
        try:
            bar = validate_candle({"open_time_utc": datetime.fromtimestamp(raw["t"] / 1000, timezone.utc),
                "close_time_utc": datetime.fromtimestamp(raw["T"] / 1000, timezone.utc),
                "open": raw["o"], "high": raw["h"], "low": raw["l"], "close": raw["c"]})
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise HyperliquidPerpPathError("Invalid perpetual closed one-minute OHLC candle") from exc
        if raw["t"] < first_ms or raw["t"] > last_open_ms or raw["T"] > last_open_ms + 59999:
            continue
        if raw["t"] in bars:
            if bars[raw["t"]] != bar:
                raise HyperliquidPerpPathError("Conflicting duplicate perpetual candle")
            duplicates += 1
        bars[raw["t"]] = bar
    candles = [bars[key] for key in sorted(bars)]
    result.update(candles=candles, missing_candles=expected-len(candles), duplicate_candles=duplicates,
        request_count=1, complete=len(candles)==expected)
    return result
