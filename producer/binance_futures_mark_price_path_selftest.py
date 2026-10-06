"""Network-free contract checks for the isolated HYPE Futures MARK reader."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import unittest
from unittest.mock import MagicMock, patch
import urllib.error
from urllib.parse import parse_qs, urlsplit

import binance_futures_mark_price_path as path

START = datetime(2026, 8, 16, tzinfo=timezone.utc)


def row(offset=0, *, opened=100, high=102, low=99, closed=101):
    timestamp = int((START + timedelta(minutes=offset)).timestamp() * 1000)
    # Ignored fields intentionally are not valid volume/turnover evidence.
    return [timestamp, str(opened), str(high), str(low), str(closed), "ignored",
            timestamp + 59_999, "ignored", 0, "ignored", "ignored", "ignored"]


class Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FuturesMarkPathTests(unittest.TestCase):
    def fetch(self, payload, *, start=START, end=START + timedelta(minutes=3), symbol="HYPE"):
        self.calls = []

        def get(url, **kwargs):
            self.calls.append((url, kwargs))
            return Response(payload)

        return path.fetch_closed_candles(symbol, start, end, request_get=get)

    def test_stdlib_default_transport_and_redirect_rejection(self):
        raw_response = MagicMock()
        raw_response.status = 200
        raw_response.read.return_value = json.dumps([row()]).encode()
        raw_response.__enter__.return_value = raw_response
        opener = MagicMock()
        opener.open.return_value = raw_response
        with patch.object(path.urllib.request, "build_opener", return_value=opener) as build:
            result = path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        self.assertTrue(result["complete"])
        self.assertEqual(opener.open.call_args.kwargs, {"timeout": 15})
        request = opener.open.call_args.args[0]
        parsed = urlsplit(request.full_url)
        self.assertEqual(parsed.scheme + "://" + parsed.netloc + parsed.path, path.SOURCE_URL)
        self.assertEqual(parse_qs(parsed.query)["symbol"], ["HYPEUSDT"])
        raw_response.read.assert_called_once_with(path.MAX_RESPONSE_BYTES + 1)
        proxy_handler, handler = build.call_args.args
        self.assertEqual(proxy_handler.proxies, {})
        with self.assertRaises(path.BinanceFuturesMarkPathError):
            handler.redirect_request(None, None, 302, "Found", {}, "https://example.com")
        for status, body in ((302, b"[]"), (200, b"invalid JSON"),
                             (200, b" " * (path.MAX_RESPONSE_BYTES + 1))):
            raw_response.status, raw_response.read.return_value = status, body
            with self.subTest(status=status, body_bytes=len(body)), \
                    patch.object(path.urllib.request, "build_opener", return_value=opener), \
                    self.assertRaises(path.BinanceFuturesMarkPathError):
                path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))

    def test_dedicated_https_proxy_preserves_fixed_binance_target(self):
        raw_response = MagicMock()
        raw_response.status_code = 200
        raw_response.iter_content.return_value = [json.dumps([row()]).encode()]
        session = MagicMock()
        session.get.return_value = raw_response
        proxy = "https://user:secret@proxy.example:8443"
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: proxy}), \
                patch.object(path.requests, "Session", return_value=session) as session_factory, \
                patch.object(path.urllib.request, "proxy_bypass", return_value=False), \
                patch.object(path.urllib.request, "build_opener") as build:
            result = path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
            status = path.transport_status()
        self.assertTrue(result["complete"])
        session_factory.assert_called_once_with()
        self.assertIs(session.trust_env, False)
        build.assert_not_called()
        self.assertEqual(session.get.call_args.args, (path.SOURCE_URL,))
        self.assertEqual(session.get.call_args.kwargs, {
            "params": {"symbol": "HYPEUSDT", "interval": "1m",
                       "startTime": row()[0], "endTime": row()[6], "limit": 1500},
            "timeout": 15,
            "allow_redirects": False,
            "stream": True,
            "proxies": {"https": proxy},
            "verify": True,
        })
        raw_response.iter_content.assert_called_once_with(
            chunk_size=path.RESPONSE_CHUNK_BYTES
        )
        raw_response.close.assert_called_once_with()
        session.close.assert_called_once_with()
        self.assertTrue(status["proxy_configured"])
        self.assertEqual(status["last_transport"], "DEDICATED_HTTPS_PROXY")
        self.assertEqual(status["last_http_status"], 200)
        self.assertNotIn("proxy.example", json.dumps(status).lower())
        self.assertNotIn("secret", json.dumps(status).lower())

    def test_invalid_or_failed_proxy_is_sanitized_and_fails_closed(self):
        for proxy in ("socks5://proxy.example:1080", "https://proxy.example/path",
                      "https://proxy.example:bad", "proxy.example:3128",
                      "http://proxy.example:3128",
                      "http://user:secret@proxy.example:3128"):
            with self.subTest(proxy=proxy), patch.dict(
                    os.environ, {path.HTTPS_PROXY_ENV: proxy}), \
                    self.assertRaisesRegex(path.BinanceFuturesMarkPathError, "invalid") as caught:
                path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
            self.assertNotIn(proxy, str(caught.exception))

        secret_proxy = "https://user:do-not-leak@proxy.example:8443"
        session = MagicMock()
        session.get.side_effect = path.requests.exceptions.ProxyError(secret_proxy)
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: secret_proxy}), \
                patch.object(path.requests, "Session", return_value=session), \
                patch.object(path.urllib.request, "proxy_bypass", return_value=False), \
                patch.object(path.urllib.request, "build_opener") as build, \
                self.assertRaises(path.BinanceFuturesMarkPathError) as caught:
            path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        build.assert_not_called()
        chain = caught.exception
        rendered = []
        seen = set()
        while chain is not None and id(chain) not in seen:
            seen.add(id(chain))
            rendered.append(str(chain))
            chain = chain.__cause__ or chain.__context__
        self.assertNotIn("do-not-leak", " ".join(rendered))
        self.assertEqual(path.transport_status()["last_error_type"], "ProxyError")
        session.close.assert_called_once_with()

    def test_proxy_response_body_is_streamed_and_bounded(self):
        raw_response = MagicMock()
        raw_response.status_code = 200

        def chunks():
            yield b"x" * path.MAX_RESPONSE_BYTES
            yield b"y"
            raise AssertionError("the bounded reader must stop immediately")

        raw_response.iter_content.return_value = chunks()
        session = MagicMock()
        session.get.return_value = raw_response
        proxy = "https://proxy.example:8443"
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: proxy}), \
                patch.object(path.requests, "Session", return_value=session), \
                patch.object(path.urllib.request, "proxy_bypass", return_value=False), \
                self.assertRaisesRegex(path.BinanceFuturesMarkPathError, "byte budget"):
            path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        raw_response.iter_content.assert_called_once_with(
            chunk_size=path.RESPONSE_CHUNK_BYTES
        )
        raw_response.close.assert_called_once_with()
        session.close.assert_called_once_with()
        status = path.transport_status()
        self.assertEqual(status["last_http_status"], 200)
        self.assertEqual(status["last_error_type"], "RESPONSE_TOO_LARGE")

    def test_proxy_redirect_or_auth_failure_never_falls_back_to_direct(self):
        proxy = "https://proxy.example:8443"
        for code in (302, 407):
            raw_response = MagicMock(status_code=code)
            session = MagicMock()
            session.get.return_value = raw_response
            with self.subTest(code=code), \
                    patch.dict(os.environ, {path.HTTPS_PROXY_ENV: proxy}), \
                    patch.object(path.requests, "Session", return_value=session), \
                    patch.object(path.urllib.request, "proxy_bypass", return_value=False), \
                    patch.object(path.urllib.request, "build_opener") as build, \
                    self.assertRaisesRegex(path.BinanceFuturesMarkPathError,
                                           f"HTTP {code}"):
                path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
            build.assert_not_called()
            self.assertFalse(session.get.call_args.kwargs["allow_redirects"])
            raw_response.iter_content.assert_not_called()
            raw_response.close.assert_called_once_with()
            session.close.assert_called_once_with()
            status = path.transport_status()
            self.assertEqual(status["last_http_status"], code)
            self.assertEqual(status["last_error_type"], "UNEXPECTED_HTTP_STATUS")

    def test_stream_or_close_error_with_secret_is_not_chained(self):
        proxy = "https://user:p%40ss%3Aword@proxy.example:8443"
        for failure in ("stream", "close"):
            raw_response = MagicMock(status_code=200)
            raw_response.iter_content.return_value = [json.dumps([row()]).encode()]
            if failure == "stream":
                raw_response.iter_content.side_effect = path.requests.exceptions.ProxyError(proxy)
            else:
                raw_response.close.side_effect = OSError(proxy)
            session = MagicMock()
            session.get.return_value = raw_response
            with self.subTest(failure=failure), \
                    patch.dict(os.environ, {path.HTTPS_PROXY_ENV: proxy}), \
                    patch.object(path.requests, "Session", return_value=session), \
                    patch.object(path.urllib.request, "proxy_bypass", return_value=False), \
                    self.assertRaises(path.BinanceFuturesMarkPathError) as caught:
                path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
            rendered = str(caught.exception) + repr(caught.exception)
            chain = caught.exception.__cause__ or caught.exception.__context__
            while chain is not None:
                rendered += str(chain) + repr(chain)
                chain = chain.__cause__ or chain.__context__
            self.assertNotIn("p%40ss%3Aword", rendered)
            self.assertNotIn("proxy.example", json.dumps(path.transport_status()))

    def test_no_proxy_cannot_bypass_dedicated_egress(self):
        proxy = "https://proxy.example:8443"
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: proxy,
                                     "NO_PROXY": "*", "no_proxy": "*"}), \
                self.assertRaisesRegex(path.BinanceFuturesMarkPathError, "bypassed"):
            path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        status = path.transport_status()
        self.assertEqual(status["last_error_type"], "PROXY_BYPASS_CONFIGURATION")

    def test_ambient_proxy_is_disabled_for_direct_transport(self):
        raw_response = MagicMock()
        raw_response.status = 200
        raw_response.read.return_value = json.dumps([row()]).encode()
        raw_response.__enter__.return_value = raw_response
        opener = MagicMock()
        opener.open.return_value = raw_response
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: "",
                                     "HTTPS_PROXY": "http://ambient.example:8080"}), \
                patch.object(path.urllib.request, "build_opener", return_value=opener) as build:
            result = path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        self.assertTrue(result["complete"])
        proxy_handler = build.call_args.args[0]
        self.assertIsInstance(proxy_handler, path.urllib.request.ProxyHandler)
        self.assertEqual(proxy_handler.proxies, {})
        self.assertEqual(path.transport_status()["last_transport"], "DIRECT")

    def test_direct_transport_failure_is_sanitized_and_updates_status(self):
        opener = MagicMock()
        opener.open.side_effect = urllib.error.URLError("direct route unavailable")
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: ""}), \
                patch.object(path.urllib.request, "build_opener", return_value=opener), \
                self.assertRaisesRegex(path.BinanceFuturesMarkPathError,
                                       "transport failed") as caught:
            path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)
        status = path.transport_status()
        self.assertEqual(status["last_transport"], "DIRECT")
        self.assertIsNone(status["last_http_status"])
        self.assertEqual(status["last_error_type"], "URLError")

    def test_unexpected_non_200_response_updates_status(self):
        raw_response = MagicMock()
        raw_response.status = 204
        raw_response.read.return_value = b""
        raw_response.__enter__.return_value = raw_response
        opener = MagicMock()
        opener.open.return_value = raw_response
        with patch.dict(os.environ, {path.HTTPS_PROXY_ENV: ""}), \
                patch.object(path.urllib.request, "build_opener", return_value=opener), \
                self.assertRaisesRegex(path.BinanceFuturesMarkPathError, "HTTP 204"):
            path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))
        status = path.transport_status()
        self.assertEqual(status["last_http_status"], 204)
        self.assertEqual(status["last_error_type"], "UNEXPECTED_HTTP_STATUS")

    def test_full_day_is_one_fixed_source_request(self):
        result = self.fetch([row(i) for i in range(1440)], end=START + timedelta(days=1))
        self.assertEqual((result["expected_candles"], result["missing_candles"], result["request_count"]), (1440, 0, 1))
        self.assertTrue(result["complete"])
        self.assertEqual({key: result[key] for key in ("symbol", "pair", "exchange", "market", "price_kind", "interval", "interval_seconds")},
                         {"symbol": "HYPE", "pair": "HYPEUSDT", "exchange": "binance", "market": "futures", "price_kind": "MARK", "interval": "1m", "interval_seconds": 60})
        self.assertEqual(result["source_url"], path.SOURCE_URL)
        self.assertEqual(result["provenance"], path.PROVENANCE)
        self.assertEqual(result["method_version"], path.METHOD_VERSION)
        self.assertEqual(self.calls, [(path.SOURCE_URL, {
            "params": {"symbol": "HYPEUSDT", "interval": "1m", "startTime": row()[0],
                       "endTime": row(1439)[6], "limit": 1500},
            "timeout": 15, "allow_redirects": False})])
        self.assertEqual(set(result["candles"][0]), {"open_time_utc", "close_time_utc", "open", "high", "low", "close"})

    def test_partial_minute_and_inclusive_close_cutoff(self):
        result = self.fetch([row(1)], start=START + timedelta(microseconds=1),
                            end=START + timedelta(minutes=2) - timedelta(milliseconds=1))
        self.assertTrue(result["complete"])
        self.assertEqual(result["expected_candles"], 1)
        self.assertEqual(self.calls[0][1]["params"]["startTime"], row(1)[0])
        empty = self.fetch([], start=START + timedelta(seconds=1), end=START + timedelta(seconds=59))
        self.assertEqual((empty["expected_candles"], empty["request_count"], empty["complete"]), (0, 0, True))
        self.assertEqual(self.calls, [])

    def test_gaps_and_duplicates_cannot_fake_complete(self):
        result = self.fetch([row(2), row(0), row(2)])
        self.assertEqual((result["expected_candles"], result["missing_candles"], result["duplicate_candles"]), (3, 1, 1))
        self.assertFalse(result["complete"])
        self.assertEqual([c["open_time_utc"] for c in result["candles"]], [START, START + timedelta(minutes=2)])
        self.assertEqual(self.fetch([])["missing_candles"], 3)
        with self.assertRaises(path.BinanceFuturesMarkPathError):
            self.fetch([row(0), row(0, closed=100)])

    def test_invalid_boundaries_or_other_symbols_do_not_fetch(self):
        for kwargs in ({"symbol": "BTC"}, {"symbol": "HYPEUSDT"},
                       {"start": START.replace(tzinfo=None)}, {"end": "2026-08-16T00:03:00"},
                       {"end": START}, {"end": START + timedelta(days=1, seconds=1)},
                       {"start": datetime.now(timezone.utc) + timedelta(days=1),
                        "end": datetime.now(timezone.utc) + timedelta(days=1, minutes=1)}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.fetch([], **kwargs)
            self.assertEqual(self.calls, [])
        result = self.fetch([row(i) for i in range(3)], start="2026-08-16T03:00:00+03:00", end="2026-08-16T03:03:00+03:00")
        self.assertTrue(result["complete"])

    def test_bad_schema_ohlc_and_nonminute_rows_fail(self):
        variants = [row()[:7], row() + [0], {"open": 100}]
        for position, value in ((0, row()[0] + 1), (6, row()[6] + 1), (0, float(row()[0])),
                                (6, True), (1, "NaN"), (2, "inf"), (3, "0"), (4, True),
                                (2, "98"), (3, "102")):
            changed = row()
            changed[position] = value
            variants.append(changed)
        for invalid in variants:
            with self.subTest(row=invalid), self.assertRaises(path.BinanceFuturesMarkPathError):
                self.fetch([invalid])

    def test_api_transport_and_out_of_window_fail(self):
        for payload in ({"code": -1121, "msg": "Invalid symbol"}, None, [row()] * 1501,
                        [row(-1)], [row(3)]):
            with self.subTest(payload_type=type(payload).__name__), self.assertRaises(path.BinanceFuturesMarkPathError):
                self.fetch(payload)

        def broken_get(*args, **kwargs):
            raise OSError("transport unavailable")

        with self.assertRaises(path.BinanceFuturesMarkPathError):
            path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1), request_get=broken_get)
        for failure in ("http", "json", "redirect"):
            class BrokenResponse(Response):
                status_code = 302 if failure == "redirect" else 200

                def raise_for_status(self):
                    if failure == "http":
                        raise OSError("HTTP 503")

                def json(self):
                    if failure == "json":
                        raise ValueError("invalid JSON")
                    return [row()]

            with self.subTest(failure=failure), self.assertRaises(path.BinanceFuturesMarkPathError):
                path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1),
                                          request_get=lambda *args, **kwargs: BrokenResponse([row()]))


if __name__ == "__main__":
    unittest.main()
