"""Offline CoinGlass capability-probe tests. No API key/network is required."""
import copy
import json
import os
import unittest
from unittest.mock import patch

import requests
import hype_row71205_source_probe as source

NOW = 1_791_170_123_000
END = (NOW // source.MINUTE - 2) * source.MINUTE
START = END - 3 * source.MINUTE
SECRET = "test-secret-never-returned"


def sample():
    return {"code": "0", "data": [
        {"time": t, "open": "100", "high": "101", "low": "99", "close": "100.5"}
        for t in range(START, END, source.MINUTE)
    ]}


class Response:
    def __init__(self, payload=None, status=200, raw=None):
        self.status_code = status
        self.body = raw if raw is not None else json.dumps(payload).encode()
        self.closed = False
        self.body_read = False

    def iter_content(self, chunk_size):
        self.body_read = True
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i:i + chunk_size]

    def close(self):
        self.closed = True


class ProbeTests(unittest.TestCase):
    def setUp(self):
        p = patch.dict(os.environ, {"COINGLASS_API_KEY": SECRET})
        p.start()
        self.addCleanup(p.stop)

    def run_probe(self, payload=None, status=200, raw=None):
        response = Response(sample() if payload is None else payload, status, raw)
        with patch.object(source.requests, "get", return_value=response) as get:
            result = source.probe(NOW)
        get.assert_called_once()
        self.assertTrue(response.closed)
        self.assertNotIn(SECRET, json.dumps(result))
        self.assertNotIn("data", result)
        self.assertFalse(result["approved_for_formula"])
        return result, get, response

    def test_valid_sample_and_exact_bounded_request(self):
        result, get, _ = self.run_probe()
        self.assertEqual(result["status"], "VALID_MINUTE_SAMPLE")
        self.assertEqual(result["rows_validated"], 3)
        self.assertEqual(result["api_code"], "0")
        self.assertEqual(get.call_args.args, (source.ENDPOINT,))
        kwargs = get.call_args.kwargs
        self.assertEqual(kwargs["params"], {
            "exchange": "Binance", "symbol": "HYPEUSDT", "interval": "1m",
            "start_time": START, "end_time": END - 1, "limit": 3,
        })
        self.assertEqual(kwargs["headers"]["CG-API-KEY"], SECRET)
        self.assertEqual(kwargs["timeout"], 10)
        self.assertFalse(kwargs["allow_redirects"])
        self.assertTrue(kwargs["stream"])

    def test_sample_order_is_irrelevant_and_numeric_strings_are_allowed(self):
        payload = sample()
        payload["data"].reverse()
        for row in payload["data"]:
            row["time"] = str(row["time"])
        result, _, _ = self.run_probe(payload)
        self.assertEqual(result["status"], "VALID_MINUTE_SAMPLE")

    def test_missing_duplicate_future_stale_and_30min_rejected(self):
        original = sample()
        cases = []
        missing = copy.deepcopy(original); missing["data"].pop(); cases.append(missing)
        duplicate = copy.deepcopy(original); duplicate["data"][1] = duplicate["data"][0]; cases.append(duplicate)
        future = copy.deepcopy(original); future["data"][2]["time"] = END; cases.append(future)
        stale = copy.deepcopy(original); stale["data"][0]["time"] -= source.MINUTE; cases.append(stale)
        thirty = copy.deepcopy(original)
        for i, row in enumerate(thirty["data"]): row["time"] = START + i * 30 * source.MINUTE
        cases.append(thirty)
        off_minute = copy.deepcopy(original); off_minute["data"][0]["time"] += 1; cases.append(off_minute)
        fractional = copy.deepcopy(original); fractional["data"][0]["time"] += .25; cases.append(fractional)
        for payload in cases:
            with self.subTest(payload=payload):
                result, _, _ = self.run_probe(payload)
                self.assertEqual(result["status"], "INVALID_MINUTE_SAMPLE")
                self.assertEqual(result["rows_validated"], 0)

    def test_invalid_ohlc(self):
        for field, value in [("open", 0), ("open", True), ("high", "NaN"),
                             ("low", "Infinity"), ("low", -1), ("high", 99),
                             ("low", 101), ("close", {}), ("open", None)]:
            with self.subTest(field=field, value=value):
                payload = sample(); payload["data"][0][field] = value
                result, _, _ = self.run_probe(payload)
                self.assertEqual(result["status"], "INVALID_MINUTE_SAMPLE")

    def test_http_rejections_do_not_read_body_and_never_retry(self):
        expected = {401: "KEY_REJECTED", 403: "ACCESS_NOT_GRANTED", 451: "ACCESS_RESTRICTED",
                    429: "RATE_LIMITED", 302: "HTTP_REDIRECT", 503: "HTTP_REJECTED"}
        for status, outcome in expected.items():
            with self.subTest(status=status):
                result, _, response = self.run_probe(status=status, raw=SECRET.encode())
                self.assertEqual(result["status"], outcome)
                self.assertEqual(result["http_status"], status)
                self.assertFalse(response.body_read)

    def test_entitlement_error_is_sanitized_without_guessing_plan(self):
        for message in ["Please upgrade your plan " + SECRET, "Permission denied " + SECRET,
                        "Your plan does not support 1m " + SECRET]:
            result, _, _ = self.run_probe({"code": "422", "msg": message})
            self.assertEqual(result["status"], "ACCESS_NOT_GRANTED")
            self.assertNotIn("plan", result)
            self.assertNotIn("msg", result)

    def test_other_api_rejection_and_untrusted_code_are_sanitized(self):
        for code, expected in [("422", "422"), (SECRET, "UNRECOGNIZED"),
                               ({"key": SECRET}, "UNRECOGNIZED"), (False, "UNRECOGNIZED")]:
            result, _, _ = self.run_probe({"code": code, "message": SECRET})
            self.assertEqual(result["status"], "API_REJECTED")
            self.assertEqual(result["api_code"], expected)
        for code, status in [(401, "KEY_REJECTED"), ("429", "RATE_LIMITED")]:
            result, _, _ = self.run_probe({"code": code, "msg": SECRET})
            self.assertEqual(result["status"], status)

    def test_response_size_and_json_shape(self):
        for raw, expected in [(b"x" * (source.MAX_RESPONSE_BYTES + 1), "RESPONSE_TOO_LARGE"),
                              (b"not-json", "INVALID_RESPONSE"), (b"[]", "INVALID_RESPONSE")]:
            result, _, _ = self.run_probe(raw=raw)
            self.assertEqual(result["status"], expected)

    def test_key_absent_means_no_request(self):
        with patch.dict(os.environ, {"COINGLASS_API_KEY": "  "}), patch.object(source.requests, "get") as get:
            result = source.probe(NOW)
        self.assertEqual(result["status"], "KEY_NOT_CONFIGURED")
        get.assert_not_called()

    def test_exception_message_and_url_never_escape(self):
        for error in [requests.ReadTimeout(SECRET + " https://example.invalid/?key=" + SECRET),
                      RuntimeError(SECRET)]:
            with patch.object(source.requests, "get", side_effect=error) as get:
                result = source.probe(NOW)
            get.assert_called_once()
            self.assertEqual(result["status"], "REQUEST_FAILED")
            self.assertEqual(result["error_type"], "ReadTimeout" if isinstance(error, requests.ReadTimeout) else "Exception")
            self.assertNotIn(SECRET, json.dumps(result))
            self.assertNotIn("http", json.dumps({k: v for k, v in result.items() if k != "http_status"}))

    def test_invalid_clock_means_no_request(self):
        for now in [True, 0, float("nan"), "123", 10 ** 30]:
            with patch.object(source.requests, "get") as get:
                result = source.probe(now)
            self.assertEqual(result["status"], "INVALID_CLOCK")
            get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
