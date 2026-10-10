"""Deterministic, network-free checks for the Binance Spot Research path."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from unittest.mock import patch

import binance_spot_price_path as price_path


def _ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _row(open_time: str, open_: float, high: float, low: float, close: float):
    open_ms = _ms(open_time)
    return [
        open_ms,
        str(open_),
        str(high),
        str(low),
        str(close),
        "123.45",
        open_ms + 59_999,
    ]


class _Response:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


_UNSAFE = "UNSAFE_BODY_URL_HEADER_OR_CAUSE"


class _ErrorResponse:
    def __init__(self, body, status=400):
        self.body = body
        self.status = status
        self.failure = price_path.requests.HTTPError(_UNSAFE)
        self.reads = {"content": 0, "json": 0, "other": 0}

    @property
    def status_code(self):
        if isinstance(self.status, Exception):
            raise self.status
        return self.status

    @property
    def content(self):
        self.reads["content"] += 1
        if isinstance(self.body, Exception):
            raise self.body
        return self.body

    def raise_for_status(self):
        raise self.failure

    def json(self):
        self.reads["json"] += 1
        raise AssertionError("Failed HTTP must not use response.json()")

    def __getattr__(self, name):
        if name in {"text", "url", "headers"}:
            self.reads["other"] += 1
            return _UNSAFE
        raise AttributeError(name)


def _assert_http_failure(body, expected_code, *, status=400, expected_status=400,
                         later_page=False, symbol="BTC"):
    response = _ErrorResponse(body, status)
    calls = []

    def fake_get(url, *, params, timeout):
        calls.append((url, dict(params), timeout))
        if later_page and len(calls) == 1:
            return _Response([
                _row("2026-08-28T10:01:00Z", 100, 102, 99, 101),
                _row("2026-08-28T10:02:00Z", 101, 105, 100, 104),
            ])
        return response

    try:
        price_path.fetch_closed_candles(
            symbol, "2026-08-28T10:00:30Z", "2026-08-28T10:05:30Z",
            request_get=fake_get,
        )
    except price_path.BinanceSpotPathError as exc:
        error = exc
    else:
        raise AssertionError("HTTP failure unexpectedly returned a price path")

    pair = "PEPEUSDT" if symbol == "1000PEPE" else symbol + "USDT"
    expected = {
        "diagnostic_version": 1,
        "http_status": expected_status,
        "binance_code": expected_code,
        "pair": pair,
        "request_start_ms": _ms("2026-08-28T10:01:00Z"),
        "request_end_ms": _ms("2026-08-28T10:05:30Z"),
        "page_cursor_ms": _ms(
            "2026-08-28T10:03:00Z" if later_page else "2026-08-28T10:01:00Z"
        ),
    }
    assert type(error) is price_path.BinanceSpotPathError
    assert error.__cause__ is response.failure
    assert error.diagnostic == expected
    assert set(vars(error)) == {"diagnostic"}
    status_text = expected_status if expected_status is not None else "unknown"
    prefix = f"Binance Spot kline request failed for {pair} (HTTP {status_text})"
    expected_text = prefix + " diagnostic=" + json.dumps(
        expected, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    assert error.args == (expected_text,)
    assert _UNSAFE not in str(error) and _UNSAFE not in repr(error)
    # This bound is for the plain str(error) carrier, not a prefixed traceback.
    assert len(str(error)) <= 300
    assert json.loads(str(error)[:300].split(" diagnostic=", 1)[1]) == expected
    assert response.reads == {"content": 1, "json": 0, "other": 0}
    assert len(calls) == (2 if later_page else 1), "No retry or fallback"
    for index, (url, params, timeout) in enumerate(calls):
        assert url == price_path.BINANCE_SPOT_BASE_URL + price_path.BINANCE_SPOT_KLINES_ENDPOINT
        assert timeout == price_path.REQUEST_TIMEOUT_SECONDS
        assert params == {
            "symbol": pair,
            "interval": "1m",
            "startTime": _ms(
                "2026-08-28T10:01:00Z" if index == 0 else "2026-08-28T10:03:00Z"
            ),
            "endTime": _ms("2026-08-28T10:05:30Z"),
            "limit": 1000,
        }
    return error


def _test_http_diagnostics():
    for code in (-1121, -2**31, 2**31 - 1, 0):
        body = json.dumps({"code": code, "msg": _UNSAFE, "extra": {"url": _UNSAFE}}).encode()
        _assert_http_failure(body, code)
    _assert_http_failure(b'{"code":-1003}', -1003, status=429, expected_status=429)
    _assert_http_failure(b'{"code":-1121}', -1121, later_page=True, symbol="1000PEPE")
    _assert_http_failure(b'{"code":-2147483648}', -2**31, status=None,
                         expected_status=None, symbol="\u00c9" * 20)

    invalid_codes = (True, False, "-1121", -1121.0, None, [], {},
                     -(2**31) - 1, 2**31)
    for code in invalid_codes:
        _assert_http_failure(json.dumps({"code": code, "msg": _UNSAFE}).encode(), None)
    invalid_bodies = (
        b"", b"<html>UNSAFE_BODY_URL_HEADER_OR_CAUSE</html>", b'{"code":',
        b"\xff", b"[]", b"null", b"true", b"1", b'"text"',
        b'{"msg":"UNSAFE_BODY_URL_HEADER_OR_CAUSE"}',
        b'{"code":-1121,"code":-1003}',
        b'{"code":-1121,"extra":{"same":1,"same":2}}',
        b"[" * 1100 + b"]" * 1100,
        '{"code":-1121}', bytearray(b'{"code":-1121}'),
        memoryview(b'{"code":-1121}'), None, RuntimeError(_UNSAFE),
    )
    for body in invalid_bodies:
        _assert_http_failure(body, None)
    for status in ("400", 400.0, True, None, 99, 600, RuntimeError(_UNSAFE)):
        # A bad status must not prevent independent bounded body decoding.
        _assert_http_failure(b'{"code":-1121}', -1121,
                             status=status, expected_status=None)

    prefix, suffix = b'{"code":-1121,"msg":"', b'"}'
    boundary = prefix + b"x" * (4096 - len(prefix) - len(suffix)) + suffix
    assert len(boundary) == 4096
    _assert_http_failure(boundary, -1121)
    oversized = boundary[:-2] + b"x" + boundary[-2:]
    assert len(oversized) == 4097
    # Even valid JSON with a useful code must not be parsed above the cap.
    # Exclude this helper's own message readback from the parser-call count.
    with patch.object(price_path.json, "loads", wraps=json.loads) as decoder:
        error = _assert_http_failure(oversized, None)
    assert decoder.call_count == 1, "Oversized HTTP body was passed to json.loads"
    assert decoder.call_args.args == (str(error).split(" diagnostic=", 1)[1],)


def _test_http_success_and_transport():
    class SuccessResponse(_Response):
        content_reads = 0

        @property
        def content(self):
            self.content_reads += 1
            raise AssertionError("Successful responses need no HTTP diagnostic")

    responses = [
        SuccessResponse([
            _row("2026-08-28T10:01:00Z", 100, 102, 99, 101),
            _row("2026-08-28T10:02:00Z", 101, 105, 100, 104),
        ]),
        SuccessResponse([]),
    ]
    calls = []

    def fake_get(url, *, params, timeout):
        calls.append(dict(params))
        return responses[len(calls) - 1]

    result = price_path.fetch_closed_candles(
        "BTC", "2026-08-28T10:00:30Z", "2026-08-28T10:03:00Z",
        request_get=fake_get,
    )
    assert {key: value for key, value in result.items() if key != "candles"} == {
        "symbol": "BTC", "pair": "BTCUSDT", "exchange": "binance",
        "market": "spot", "interval": "1m", "interval_seconds": 60,
        "multiplier": 1.0, "expected_candles": 2, "complete": True,
    }
    assert [(item.open, item.high, item.low, item.close) for item in result["candles"]] == [
        (100, 102, 99, 101), (101, 105, 100, 104),
    ]
    assert len(calls) == 2
    assert all(response.content_reads == 0 for response in responses)

    failure = price_path.requests.Timeout("synthetic transport timeout")
    transport_calls = []

    def failed_get(*args, **kwargs):
        transport_calls.append(kwargs)
        raise failure

    try:
        price_path.fetch_closed_candles(
            "BTC", "2026-08-28T10:00:30Z", "2026-08-28T10:03:00Z",
            request_get=failed_get,
        )
    except price_path.requests.Timeout as exc:
        assert exc is failure and not hasattr(exc, "diagnostic")
    else:
        raise AssertionError("Transport failure was swallowed")
    assert len(transport_calls) == 1


def _test_research_price_error_carrier():
    import live_price_provider

    response = _ErrorResponse(b'{"code":-1121,"msg":"UNSAFE_BODY_URL_HEADER_OR_CAUSE"}')
    calls = []

    def fake_get(url, *, params, timeout):
        calls.append(dict(params))
        return response

    def fetcher(symbol, start, end):
        return price_path.fetch_closed_candles(symbol, start, end, request_get=fake_get)

    result = live_price_provider.fetch_research_spot_1m_prices(
        ["BTC"], observed_at_utc="2026-08-28T10:05:30Z", candle_fetcher=fetcher,
    )
    assert result["ok"] is False
    assert result["prices"] == {} and result["missing_symbols"] == ["BTC"]
    assert (result["requested_count"], result["found_count"], result["missing_count"]) == (1, 0, 1)
    assert result["fallback_used"] is False
    assert result["source"] == "official_closed_spot_1m_no_fallback"
    assert set(result["errors"]) == {"BTC"}
    message = result["errors"]["BTC"]
    assert message.startswith(
        "BinanceSpotPathError: Binance Spot kline request failed for BTCUSDT (HTTP 400) diagnostic="
    )
    assert _UNSAFE not in message
    diagnostic = json.loads(message.split(" diagnostic=", 1)[1])
    assert diagnostic == {
        "diagnostic_version": 1, "http_status": 400, "binance_code": -1121,
        "pair": "BTCUSDT",
        "request_start_ms": _ms("2026-08-28T10:03:00Z"),
        "request_end_ms": _ms("2026-08-28T10:05:00Z"),
        "page_cursor_ms": _ms("2026-08-28T10:03:00Z"),
    }
    assert calls == [{
        "symbol": "BTCUSDT", "interval": "1m",
        "startTime": _ms("2026-08-28T10:03:00Z"),
        "endTime": _ms("2026-08-28T10:05:00Z"), "limit": 1000,
    }]
    assert response.reads == {"content": 1, "json": 0, "other": 0}


def run() -> None:
    calls = []
    payload = [
        _row("2026-08-28T10:01:00Z", 100, 102, 99, 101),
        _row("2026-08-28T10:02:00Z", 101, 105, 100, 104),
    ]

    def fake_get(url, *, params, timeout):
        calls.append({"url": url, "params": dict(params), "timeout": timeout})
        return _Response(payload)

    result = price_path.fetch_closed_candles(
        "BTC",
        "2026-08-28T10:00:30Z",
        "2026-08-28T10:03:00Z",
        request_get=fake_get,
    )
    assert result["pair"] == "BTCUSDT"
    assert result["interval"] == "1m"
    assert result["expected_candles"] == 2
    assert result["complete"] is True
    assert len(result["candles"]) == 2
    # The 10:00 candle is excluded because half of it predates the alert.
    assert calls[0]["params"]["startTime"] == _ms("2026-08-28T10:01:00Z")

    long_metrics = price_path.calculate_path_metrics(
        reference_price=100,
        direction="LONG",
        event_time="2026-08-28T10:00:30Z",
        candles=result["candles"],
        target_price=104,
    )
    assert round(long_metrics["raw_return_pct"], 8) == 4.0
    assert round(long_metrics["directional_return_pct"], 8) == 4.0
    assert round(long_metrics["mfe_pct"], 8) == 5.0
    assert round(long_metrics["mae_pct"], 8) == 1.0
    assert long_metrics["target_reached"] is True
    assert long_metrics["target_progress_ratio"] == 1.0
    assert long_metrics["time_to_target_seconds"] == 149

    short_candles = [
        price_path.SpotCandle(
            open_time_utc=datetime(2026, 8, 28, 10, 1, tzinfo=timezone.utc),
            close_time_utc=datetime(2026, 8, 28, 10, 1, 59, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=98,
            close=99,
            volume=1,
        ),
        price_path.SpotCandle(
            open_time_utc=datetime(2026, 8, 28, 10, 2, tzinfo=timezone.utc),
            close_time_utc=datetime(2026, 8, 28, 10, 2, 59, tzinfo=timezone.utc),
            open=99,
            high=100,
            low=95,
            close=96,
            volume=1,
        ),
    ]
    short_metrics = price_path.calculate_path_metrics(
        reference_price=100,
        direction="SHORT",
        event_time="2026-08-28T10:00:30Z",
        candles=short_candles,
        target_price=96,
    )
    assert round(short_metrics["directional_return_pct"], 8) == 4.0
    assert round(short_metrics["mfe_pct"], 8) == 5.0
    assert round(short_metrics["mae_pct"], 8) == 1.0
    assert short_metrics["target_reached"] is True

    pair, multiplier = price_path.resolve_pair("1000PEPE")
    assert pair == "PEPEUSDT"
    assert multiplier == 1000.0

    _test_http_diagnostics()
    _test_http_success_and_transport()
    _test_research_price_error_carrier()

    print("Binance Spot price-path self-test: PASS")


if __name__ == "__main__":
    run()
