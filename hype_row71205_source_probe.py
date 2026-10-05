"""Bounded, read-only capability probe; its prices never enter ROW71205.

A valid sample proves only that the current CoinGlass key can obtain three
recent minute bars labelled Binance/HYPEUSDT. It does not establish candle
parity with the researched Binance trade feed, or approve a source switch.
The caller owns scheduling/cooldown. No retries or durable state writes occur.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from math import isfinite
import os
import re

import requests

MINUTE = 60_000
MAX_RESPONSE_BYTES = 2_000_000
ENDPOINT = "https://open-api-v4.coinglass.com/api/futures/price/history"


def _safe_api_code(value):
    """Never forward arbitrary provider strings into health/log output."""
    if isinstance(value, bool):
        return "UNRECOGNIZED"
    candidate = str(value) if isinstance(value, (str, int)) else ""
    return candidate if re.fullmatch(r"-?[0-9]{1,10}", candidate) else "UNRECOGNIZED"


def _access_not_granted(payload):
    # Provider prose is used only for classification and is never returned.
    messages = [payload.get(k) for k in ("msg", "message", "error")]
    message = " ".join(x[:512].lower() for x in messages if isinstance(x, str))
    explicit = ("upgrade", "permission denied", "no permission", "access denied",
                "not authorized", "unauthorized", "subscription required",
                "subscription expired", "insufficient permissions")
    return any(x in message for x in explicit) or bool(re.search(
        r"(?:plan|package|subscription).{0,60}(?:not support|not available|does not|limited|required)",
        message,
    ))


def _minute_timestamp(value):
    if isinstance(value, bool):
        raise ValueError("Invalid timestamp")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,16}", value):
        return int(value)
    raise ValueError("Invalid timestamp")


def _validate_rows(rows, start, end):
    if not isinstance(rows, list) or len(rows) != 3:
        return False
    expected = set(range(start, end, MINUTE))
    seen = set()
    try:
        for row in rows:
            if not isinstance(row, dict):
                return False
            timestamp = _minute_timestamp(row.get("time"))
            if timestamp not in expected or timestamp in seen or timestamp % MINUTE:
                return False
            prices = []
            for name in ("open", "high", "low", "close"):
                value = row.get(name)
                if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                    return False
                price = float(value)
                if not isfinite(price) or price <= 0:
                    return False
                prices.append(price)
            opening, high, low, close = prices
            if high < max(opening, low, close) or low > min(opening, high, close):
                return False
            seen.add(timestamp)
    except (TypeError, ValueError, OverflowError):
        return False
    return seen == expected


def probe(now_ms):
    """Make at most one official API request and return sanitized metadata only.

    The requested interval ends two minutes before the current UTC minute,
    leaving a publication buffer. No current/partially closed candle is used.
    Missing entitlement is reported without inferring the account's plan.
    """
    result = {
        "provider": "COINGLASS",
        "market": "BINANCE_USDM_FUTURES_HYPEUSDT",
        "interval": "1m",
        "status": "NOT_CHECKED",
        "checked_at": None,
        "http_status": None,
        "api_code": None,
        "rows_validated": 0,
        "approved_for_formula": False,
    }
    if isinstance(now_ms, bool) or not isinstance(now_ms, int) or now_ms < 5 * MINUTE:
        result["status"] = "INVALID_CLOCK"
        return result
    try:
        result["checked_at"] = datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    except (ValueError, OverflowError, OSError):
        result["status"] = "INVALID_CLOCK"
        return result
    key = os.getenv("COINGLASS_API_KEY", "").strip()
    if not key:
        result["status"] = "KEY_NOT_CONFIGURED"
        return result
    end = (now_ms // MINUTE - 2) * MINUTE
    start = end - 3 * MINUTE
    response = None
    try:
        response = requests.get(
            ENDPOINT,
            params={"exchange": "Binance", "symbol": "HYPEUSDT", "interval": "1m",
                    "start_time": start, "end_time": end - 1, "limit": 3},
            headers={"CG-API-KEY": key, "accept": "application/json"},
            timeout=10, allow_redirects=False, stream=True,
        )
        status = response.status_code
        if not isinstance(status, int) or isinstance(status, bool) or not 100 <= status <= 599:
            result["status"] = "INVALID_HTTP_STATUS"
            return result
        result["http_status"] = status
        if status != 200:
            result["status"] = {
                401: "KEY_REJECTED", 403: "ACCESS_NOT_GRANTED", 451: "ACCESS_RESTRICTED",
                429: "RATE_LIMITED",
            }.get(status, "HTTP_REDIRECT" if 300 <= status <= 399 else "HTTP_REJECTED")
            return result
        body = bytearray()
        for chunk in response.iter_content(chunk_size=65_536):
            if not chunk:
                continue
            if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                result["status"] = "RESPONSE_TOO_LARGE"
                return result
            body.extend(chunk)
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            result["status"] = "INVALID_RESPONSE"
            return result
        if not isinstance(payload, dict):
            result["status"] = "INVALID_RESPONSE"
            return result
        result["api_code"] = _safe_api_code(payload.get("code"))
        if result["api_code"] != "0":
            result["status"] = ("KEY_REJECTED" if result["api_code"] == "401" else
                                "RATE_LIMITED" if result["api_code"] == "429" else
                                "ACCESS_NOT_GRANTED" if _access_not_granted(payload) else
                                "API_REJECTED")
            return result
        if not _validate_rows(payload.get("data"), start, end):
            result["status"] = "INVALID_MINUTE_SAMPLE"
            return result
        result.update(status="VALID_MINUTE_SAMPLE", rows_validated=3)
        return result
    except Exception as error:
        result["status"] = "REQUEST_FAILED"
        # Do not stringify exceptions: requests embeds URLs/headers in errors.
        name = type(error).__name__
        result["error_type"] = name if name in {
            "Timeout", "ConnectTimeout", "ReadTimeout", "ConnectionError", "SSLError",
            "ProxyError", "ChunkedEncodingError", "ContentDecodingError", "OSError",
            "ValueError", "TypeError", "RequestException",
        } else "Exception"
        return result
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
