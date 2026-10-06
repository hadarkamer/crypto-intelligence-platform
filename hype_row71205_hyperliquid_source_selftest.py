"""Offline source-contract tests; no network or trading calls."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import hype_row71205_hyperliquid_source as source

MINUTE = source.MINUTE_MS
CURRENT = 30_000_000 * MINUTE
NOW = CURRENT + 12_345


def candle(open_ms, **changes):
    result = {"s": "HYPE", "i": "1m", "t": open_ms, "T": open_ms + MINUTE - 1,
              "o": "40", "h": "41", "l": "39", "c": "40.5"}
    result.update(changes)
    return result


class SourceTests(unittest.TestCase):
    def fetch(self, start, end, payload, *, status=200, content=b"[]"):
        self.post = Mock(return_value=SimpleNamespace(
            status_code=status, content=content, json=Mock(return_value=payload)))
        return source.fetch_rows("HYPE", start, end, request_post=self.post,
                                 clock_ms=lambda: NOW)

    def assert_code(self, code, func):
        with self.assertRaises(source.HyperliquidSourceError) as caught:
            func()
        self.assertEqual(caught.exception.code, "HYPE_HYPERLIQUID_" + code)

    def test_closed_rows_and_request_contract(self):
        start, end = CURRENT-2*MINUTE, CURRENT
        rows = self.fetch(start, end, [candle(start), candle(start+MINUTE)])
        self.assertEqual(rows, [[start, 40., 41., 39., 40.5],
                                [start+MINUTE, 40., 41., 39., 40.5]])
        self.post.assert_called_once_with(source.SOURCE_URL,
            json={"type": "candleSnapshot", "req": {"coin": "HYPE", "interval": "1m",
                  "startTime": start, "endTime": end-1}},
            timeout=15, allow_redirects=False)

    def test_partial_current_minute_only_when_requested(self):
        rows = self.fetch(CURRENT, CURRENT+MINUTE, [candle(CURRENT)])
        self.assertEqual(rows[0][0], CURRENT)
        # A surrounding current candle does not contaminate a closed request.
        rows = self.fetch(CURRENT-MINUTE, CURRENT,
                          [candle(CURRENT-MINUTE), candle(CURRENT)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], CURRENT-MINUTE)

    def test_future_and_oversized_windows_fail_without_network(self):
        for start, end, code in [
                (CURRENT, CURRENT+2*MINUTE, "FUTURE_MINUTE"),
                (CURRENT-2001*MINUTE, CURRENT, "WINDOW_TOO_LARGE"),
                (CURRENT-5000*MINUTE, CURRENT-4999*MINUTE, "RETENTION_EXPIRED"),
                (CURRENT+1, CURRENT+MINUTE, "INVALID_WINDOW"),
                (float(CURRENT), CURRENT+MINUTE, "INVALID_WINDOW"),
                (True, CURRENT, "INVALID_WINDOW")]:
            with self.subTest(code=code, start=start):
                post = Mock()
                self.assert_code(code, lambda: source.fetch_rows(
                    "HYPE", start, end, request_post=post, clock_ms=lambda: NOW))
                post.assert_not_called()

    def test_other_instrument_and_empty_request(self):
        post = Mock()
        self.assert_code("INVALID_SYMBOL", lambda: source.fetch_rows(
            "BTC", CURRENT-MINUTE, CURRENT, request_post=post, clock_ms=lambda: NOW))
        self.assertEqual(source.fetch_rows("HYPE", CURRENT, CURRENT,
                         request_post=post, clock_ms=lambda: NOW), [])
        post.assert_not_called()

    def test_missing_duplicate_unordered_or_wrong_interval_rejected(self):
        start, end = CURRENT-2*MINUTE, CURRENT
        cases = [
            ([candle(start)], "INCOMPLETE_MINUTES"),
            ([candle(start), candle(start)], "DUPLICATE_MINUTE"),
            ([candle(start+MINUTE), candle(start)], "INCOMPLETE_MINUTES"),
            ([candle(start, s="@107")], "INVALID_INSTRUMENT"),
            ([candle(start, i="30m")], "INVALID_INSTRUMENT"),
            ([candle(start, t=start+1)], "INVALID_CANDLE_TIME"),
            ([candle(start, T=start+MINUTE)], "INVALID_CANDLE_TIME"),
            ([candle(start, t=True)], "INVALID_CANDLE_TIME"),
            ([candle(CURRENT+MINUTE)], "FUTURE_MINUTE"),
            ([candle(start-2*MINUTE)], "UNEXPECTED_CANDLE"),
        ]
        for payload, code in cases:
            with self.subTest(code=code, payload=payload):
                self.assert_code(code, lambda: self.fetch(start, end, payload))

    def test_bad_ohlc_and_nonfinite_prices_rejected(self):
        for key, value, code in [
                ("o", "nan", "INVALID_PRICE"), ("h", "inf", "INVALID_PRICE"),
                ("l", 0, "INVALID_PRICE"), ("c", -1, "INVALID_PRICE"),
                ("c", True, "INVALID_PRICE"), ("c", None, "INVALID_PRICE"),
                ("h", "39.9", "INVALID_OHLC"), ("l", "40.1", "INVALID_OHLC")]:
            with self.subTest(key=key, value=value):
                self.assert_code(code, lambda: self.fetch(CURRENT-MINUTE, CURRENT,
                                 [candle(CURRENT-MINUTE, **{key: value})]))

    def test_surrounding_rows_are_validated_but_excluded(self):
        start, end = CURRENT-2*MINUTE, CURRENT-MINUTE
        payload = [candle(start-MINUTE), candle(start), candle(end)]
        self.assertEqual(len(self.fetch(start, end, payload)), 1)
        payload[0]["s"] = "BTC"
        self.assert_code("INVALID_INSTRUMENT", lambda: self.fetch(start, end, payload))

    def test_http_payload_and_byte_failures(self):
        start, end = CURRENT-MINUTE, CURRENT
        self.assert_code("HTTP_429", lambda: self.fetch(start, end, [], status=429))
        self.assert_code("RESPONSE_TOO_LARGE", lambda: self.fetch(
            start, end, [], content=b"x" * (source.MAX_RESPONSE_BYTES+1)))
        for payload in [{"error": "x"}, "[]", None, [candle(start)]*4]:
            self.assert_code("INVALID_PAYLOAD", lambda: self.fetch(start, end, payload))

    def test_transport_failure_is_sanitized_and_not_retried(self):
        post = Mock(side_effect=RuntimeError("private detail"))
        self.assert_code("REQUEST_FAILED", lambda: source.fetch_rows(
            "HYPE", CURRENT-MINUTE, CURRENT, request_post=post, clock_ms=lambda: NOW))
        post.assert_called_once()

    def test_retention_boundary_and_maximum_request(self):
        start = CURRENT-(source.RETENTION_CANDLES-1)*MINUTE
        rows = self.fetch(start, start+MINUTE, [candle(start)])
        self.assertEqual(rows[0][0], start)
        start = CURRENT-source.MAX_WINDOW_MINUTES*MINUTE
        rows = self.fetch(start, CURRENT, [candle(t) for t in range(start, CURRENT, MINUTE)])
        self.assertEqual(len(rows), source.MAX_WINDOW_MINUTES)


if __name__ == "__main__":
    unittest.main()
