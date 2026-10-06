"""Network-free contract checks for the isolated HYPE Futures MARK reader."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import MagicMock, patch
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
        handler = build.call_args.args[0]
        with self.assertRaises(path.BinanceFuturesMarkPathError):
            handler.redirect_request(None, None, 302, "Found", {}, "https://example.com")
        for status, body in ((302, b"[]"), (200, b"invalid JSON"),
                             (200, b" " * (path.MAX_RESPONSE_BYTES + 1))):
            raw_response.status, raw_response.read.return_value = status, body
            with self.subTest(status=status, body_bytes=len(body)), \
                    patch.object(path.urllib.request, "build_opener", return_value=opener), \
                    self.assertRaises(path.BinanceFuturesMarkPathError):
                path.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1))

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
