"""Network-free perpetual instrument, closed-window and retention checks."""
from datetime import datetime, timedelta, timezone
import unittest

import hyperliquid_perp_price_path as provider

START = datetime(2026, 9, 7, 3, 11, tzinfo=timezone.utc)


def bar(index=0, **changes):
    opened = int((START + timedelta(minutes=index)).timestamp()*1000)
    return {"s": "HYPE", "i": "1m", "t": opened, "T": opened + 59999,
        "o": "86", "h": "86.1", "l": "85.9", "c": "86", **changes}


class Response:
    status_code = 200
    content = b"[]"
    def __init__(self, payload): self.payload = payload
    def json(self): return self.payload


class PerpProviderTests(unittest.TestCase):
    def fetch(self, rows, minutes=2, start=START):
        self.calls = []
        def post(url, **kwargs):
            self.calls.append((url, kwargs))
            return Response(rows)
        return provider.fetch_closed_candles("HYPE", start, START + timedelta(minutes=minutes), request_post=post)

    def test_exact_perpetual_instrument_and_source(self):
        result = self.fetch([bar(), bar(1)])
        self.assertTrue(result["complete"])
        self.assertEqual(result["price_kind"], "TRADE")
        self.assertEqual(result["market"], "perpetual")
        self.assertEqual(result["instrument"], "HYPE")
        self.assertEqual(result["retention_candles"], 5000)
        url, kwargs = self.calls[0]
        self.assertEqual(url, "https://api.hyperliquid.xyz/info")
        self.assertEqual(kwargs["json"]["req"]["coin"], "HYPE")
        self.assertFalse(kwargs["allow_redirects"])

    def test_spot_or_missing_instrument_is_rejected(self):
        for wrong in (bar(s="@107"), bar(s=None), bar(i="5m"), bar(t=True)):
            with self.subTest(wrong=wrong), self.assertRaises(provider.HyperliquidPerpPathError):
                self.fetch([wrong])

    def test_retention_gap_remains_missing(self):
        result = self.fetch([bar(1)])
        self.assertFalse(result["complete"])
        self.assertEqual(result["expected_candles"], 2)
        self.assertEqual(result["missing_candles"], 1)

    def test_partial_minute_and_surrounding_rows_are_excluded(self):
        result = self.fetch([bar(), bar(1), bar(2)], start=START + timedelta(seconds=12))
        self.assertTrue(result["complete"])
        self.assertEqual(len(result["candles"]), 1)
        self.assertEqual(result["candles"][0]["open_time_utc"], START + timedelta(minutes=1))

    def test_invalid_and_conflicting_candles_are_rejected(self):
        for rows in ([bar(h="85")], [bar(T=1)], [bar(), bar(c="86.05")], {"error": "invalid"}):
            with self.subTest(rows=rows), self.assertRaises(provider.HyperliquidPerpPathError):
                self.fetch(rows)

    def test_boundaries_and_failure_status(self):
        with self.assertRaises(ValueError): self.fetch([], minutes=1441)
        with self.assertRaises(ValueError): self.fetch([], start=START.replace(tzinfo=None))
        response = Response([])
        response.status_code = 429
        with self.assertRaisesRegex(provider.HyperliquidPerpPathError, "HTTP 429"):
            provider.fetch_closed_candles("HYPE", START, START + timedelta(minutes=1), request_post=lambda *a, **k: response)


if __name__ == "__main__": unittest.main()
