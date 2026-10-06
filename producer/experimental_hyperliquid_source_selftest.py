"""Offline contracts for shared experimental perpetual prices. No network."""
from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import experimental_hyperliquid_source as source

M = source.MINUTE_MS
CURRENT = 30_000_000 * M
NOW = CURRENT + 12_345


def candle(opened, symbol="ETH", **changes):
    result = {"s": symbol, "i": "1m", "t": opened, "T": opened+M-1,
              "o": "40", "h": "41", "l": "39", "c": "40.5"}
    result.update(changes)
    return result


def response(payload, status=200, content=b"[]"):
    return SimpleNamespace(status_code=status, content=content, json=lambda: payload)


class SourceTests(unittest.TestCase):
    def client(self, payload=None, **kwargs):
        self.post = Mock(return_value=response(payload))
        return source.HyperliquidCandleSource(request_post=self.post,
                                             clock_ms=lambda: NOW, **kwargs)

    def assert_code(self, code, func):
        with self.assertRaises(source.HyperliquidSourceError) as caught:
            func()
        self.assertEqual(caught.exception.code, "EXPERIMENTAL_HYPERLIQUID_"+code)

    def test_six_symbols_exact_source_and_http_contract(self):
        for symbol in source.SYMBOLS:
            with self.subTest(symbol=symbol):
                c = self.client([candle(CURRENT-M, symbol)])
                self.assertEqual(c.fetch_rows(symbol, CURRENT-M, CURRENT),
                                 [[CURRENT-M, 40., 41., 39., 40.5]])
                self.post.assert_called_once_with(source.SOURCE_URL,
                    json={"type": "candleSnapshot", "req": {"coin": symbol,
                        "interval": "1m", "startTime": CURRENT-M, "endTime": CURRENT-1}},
                    timeout=15, allow_redirects=False)
                meta = source.source_metadata(symbol)
                self.assertEqual(meta["price_kind"], "TRADE")
                self.assertEqual(meta["price_market"], "perpetual")
                self.assertFalse(meta["mark_price_equivalent"])
                self.assertEqual(source.price_source(symbol),
                                 f"HYPERLIQUID_{symbol}_PERPETUAL_TRADE_1M")

    def test_closed_cache_shared_by_calls_and_return_cannot_mutate_cache(self):
        c = self.client([candle(CURRENT-2*M), candle(CURRENT-M)])
        got = c.fetch_rows("ETH", CURRENT-2*M, CURRENT)
        got[0][4] = 99
        again = c.fetch_rows("ETH", CURRENT-2*M, CURRENT)
        self.assertEqual(again[0][4], 40.5)
        self.post.assert_called_once()

    def test_current_candle_never_cache_or_archive(self):
        writer = Mock()
        c = self.client([candle(CURRENT-M), candle(CURRENT)], archive_write=writer)
        c.fetch_rows("ETH", CURRENT-M, CURRENT+M)
        self.assertEqual(set(c._rows["ETH"]), {CURRENT-M})
        writer.assert_called_once_with("ETH", [(CURRENT-M, 40., 41., 39., 40.5)])
        self.post.return_value = response([candle(CURRENT, c="40.8")])
        live = c.fetch_rows("ETH", CURRENT, CURRENT+M)
        self.assertEqual(live[0][4], 40.8)
        self.assertEqual(self.post.call_count, 2)
        self.assertNotIn(CURRENT, c._rows["ETH"])

    def test_same_symbol_concurrent_fetch_is_deduplicated(self):
        entered, release = threading.Event(), threading.Event()
        c = self.client()
        def slow(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(3))
            return response([candle(CURRENT-M)])
        self.post.side_effect = slow
        with ThreadPoolExecutor(max_workers=4) as executor:
            jobs = [executor.submit(c.fetch_rows, "ETH", CURRENT-M, CURRENT) for _ in range(4)]
            self.assertTrue(entered.wait(3))
            release.set()
            rows = [j.result(3) for j in jobs]
        self.assertTrue(all(r == rows[0] for r in rows))
        self.post.assert_called_once()

    def test_caches_do_not_mix_coins(self):
        c = self.client([candle(CURRENT-M)])
        c.fetch_rows("ETH", CURRENT-M, CURRENT)
        self.post.return_value = response([candle(CURRENT-M, "BTC", o="50",h="51",l="49",c="50.5")])
        self.assertEqual(c.fetch_rows("BTC", CURRENT-M, CURRENT)[0][4], 50.5)
        self.assertEqual(self.post.call_count, 2)

    def test_missing_islands_use_one_bounded_request_and_revisions_fail(self):
        c = self.client([candle(CURRENT-2*M)])
        c.fetch_rows("ETH", CURRENT-2*M, CURRENT-M)
        self.post.return_value = response([candle(t) for t in range(CURRENT-3*M, CURRENT, M)])
        self.assertEqual(len(c.fetch_rows("ETH", CURRENT-3*M, CURRENT)), 3)
        self.assertEqual(self.post.call_count, 2)
        self.post.return_value = response([candle(t, c="40.8") for t in range(CURRENT-4*M, CURRENT+M, M)])
        self.assert_code("CLOSED_CANDLE_REVISION", lambda: c.fetch_rows("ETH", CURRENT-4*M, CURRENT+M))

    def test_invalid_windows_fail_without_http(self):
        c = self.client()
        for start, end, code in [
                (CURRENT, CURRENT+2*M, "FUTURE_MINUTE"),
                (CURRENT-1001*M, CURRENT, "WINDOW_TOO_LARGE"),
                (CURRENT-5000*M, CURRENT-4999*M, "RETENTION_EXPIRED"),
                (CURRENT+1, CURRENT+M, "INVALID_WINDOW"),
                (float(CURRENT), CURRENT+M, "INVALID_WINDOW"),
                (True, CURRENT, "INVALID_WINDOW")]:
            with self.subTest(code=code):
                self.assert_code(code, lambda: c.fetch_rows("ETH", start, end))
        self.assert_code("INVALID_SYMBOL", lambda: c.fetch_rows("@107", CURRENT-M, CURRENT))
        self.assert_code("INVALID_SYMBOL", lambda: c.fetch_rows("eth", CURRENT-M, CURRENT))
        self.assertEqual(c.fetch_rows("ETH", CURRENT, CURRENT), [])
        self.post.assert_not_called()

    def test_retention_boundary_and_maximum_window(self):
        start = CURRENT-(source.RETENTION_CANDLES-1)*M
        c = self.client([candle(start)])
        self.assertEqual(c.fetch_rows("ETH", start, start+M)[0][0], start)
        start = CURRENT-1000*M
        self.post.return_value = response([candle(t) for t in range(start, CURRENT, M)])
        self.assertEqual(len(c.fetch_rows("ETH", start, CURRENT)), 1000)

    def test_bad_payloads_cannot_enter_cache(self):
        start = CURRENT-2*M
        cases = [([candle(start)], "INCOMPLETE_MINUTES"),
                 ([candle(start), candle(start)], "DUPLICATE_MINUTE"),
                 ([candle(start+M), candle(start)], "INCOMPLETE_MINUTES"),
                 ([candle(start, s="@107")], "INVALID_INSTRUMENT"),
                 ([candle(start, i="30m")], "INVALID_INSTRUMENT"),
                 ([candle(start, t=start+1)], "INVALID_CANDLE_TIME"),
                 ([candle(start, T=start+M)], "INVALID_CANDLE_TIME"),
                 ([candle(start, t=True)], "INVALID_CANDLE_TIME"),
                 ([candle(CURRENT+M)], "FUTURE_MINUTE"),
                 ([candle(start-2*M)], "UNEXPECTED_CANDLE"),
                 ({"error": "something"}, "INVALID_PAYLOAD"),
                 ([candle(start)]*5, "INVALID_PAYLOAD")]
        for payload, code in cases:
            with self.subTest(code=code):
                c = self.client(payload)
                self.assert_code(code, lambda: c.fetch_rows("ETH", start, CURRENT))
                self.assertFalse(c._rows["ETH"])

    def test_ohlc_nonfinite_and_boolean_fail(self):
        for key, value, code in [
                ("o", "nan", "INVALID_PRICE"), ("h", "inf", "INVALID_PRICE"),
                ("l", 0, "INVALID_PRICE"), ("c", -1, "INVALID_PRICE"),
                ("c", True, "INVALID_PRICE"), ("c", None, "INVALID_PRICE"),
                ("h", "39.9", "INVALID_OHLC"), ("l", "40.1", "INVALID_OHLC")]:
            with self.subTest(key=key, value=value):
                c = self.client([candle(CURRENT-M, **{key: value})])
                self.assert_code(code, lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))

    def test_http_and_transport_are_bounded_and_sanitized(self):
        c = self.client()
        self.post.side_effect = RuntimeError("private detail")
        self.assert_code("REQUEST_FAILED", lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))
        self.post.assert_called_once()
        self.post.side_effect = None
        self.post.return_value = response([], status=451)
        self.assert_code("HTTP_451", lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))
        self.post.return_value = response([], content=b"x"*(source.MAX_RESPONSE_BYTES+1))
        self.assert_code("RESPONSE_TOO_LARGE", lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))

    def test_rate_limit_cooldown_shared_across_coins_without_sleep_or_retry(self):
        c = self.client()
        self.post.return_value = response([], status=429)
        self.assert_code("HTTP_429", lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))
        self.assert_code("RATE_LIMIT_COOLDOWN", lambda: c.fetch_rows("BTC", CURRENT-M, CURRENT))
        self.post.assert_called_once()

    def test_validated_surrounding_candles_are_excluded(self):
        start = CURRENT-M
        c = self.client([candle(start-M), candle(start), candle(CURRENT)])
        self.assertEqual(len(c.fetch_rows("ETH", start, CURRENT)), 1)
        self.assertEqual(set(c._rows["ETH"]), {start})

    def test_older_archived_rows_allowed_but_missing_old_minutes_fail_closed(self):
        start = CURRENT-6000*M
        archived = [(start, 40., 41., 39., 40.5)]
        reader = Mock(return_value=archived)
        c = self.client(archive_read=reader)
        self.assertEqual(c.fetch_rows("ETH", start, start+M), [list(archived[0])])
        self.post.assert_not_called()
        self.assert_code("RETENTION_EXPIRED", lambda: c.fetch_rows("ETH", start, start+2*M))
        self.post.assert_not_called()

    def test_archive_may_not_supply_live_wrong_or_duplicate_rows(self):
        for rows, code in [
                ([(CURRENT, 40,41,39,40)], "INVALID_ARCHIVE_MINUTE"),
                ([(CURRENT-2*M,40,41,39,40)]*2, "INVALID_ARCHIVE_PAYLOAD"),
                ([(CURRENT-M,40,41,39,float('nan'))], "INVALID_PRICE")]:
            with self.subTest(code=code):
                c = self.client(archive_read=Mock(return_value=rows))
                self.assert_code(code, lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))
                self.post.assert_not_called()

    def test_archive_errors_never_hidden_by_http_or_memory_cache(self):
        c = self.client(archive_read=Mock(side_effect=RuntimeError("private")))
        self.assert_code("ARCHIVE_READ_FAILED", lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))
        self.post.assert_not_called()
        c = self.client([candle(CURRENT-M)], archive_write=Mock(side_effect=RuntimeError("private")))
        self.assert_code("ARCHIVE_WRITE_FAILED", lambda: c.fetch_rows("ETH", CURRENT-M, CURRENT))
        self.assertFalse(c._rows["ETH"])

    def test_eviction_does_not_hide_old_archived_request(self):
        old = CURRENT-6000*M
        c = self.client(archive_read=Mock(return_value=[(old,40,41,39,40.5)]))
        c._rows["ETH"] = {CURRENT-i*M:(CURRENT-i*M,40.,41.,39.,40.5) for i in range(1,4)}
        with patch.object(source, "CACHE_CANDLES_PER_SYMBOL", 3):
            self.assertEqual(c.fetch_rows("ETH", old, old+M)[0][0], old)
        self.assertEqual(len(c._rows["ETH"]), 3)
        self.post.assert_not_called()

    def test_quote_causal_at_minute_boundary_and_mid_minute(self):
        fetch = Mock(return_value=[[CURRENT-M,40,41,39,40.5]])
        for decision in (CURRENT, NOW):
            quote = source.closed_quote("ETH", decision, now_ms=NOW, fetch=fetch)
            self.assertEqual(quote["price"], 40.5)
            self.assertEqual(quote["asof_ms"], CURRENT-1)
            self.assertEqual(quote["retrieved_at_ms"], NOW)
            self.assertEqual(quote["quote_basis"], "LAST_CLOSED_TRADE_1M_CLOSE")
            fetch.assert_called_with("ETH", CURRENT-M, CURRENT)

    def test_quote_stale_future_or_wrong_minute_rejected(self):
        fetch = Mock()
        for decision in (NOW+1, NOW-5*M-1, float(NOW), True):
            self.assert_code("STALE_OR_FUTURE_DECISION", lambda:
                             source.closed_quote("ETH", decision, now_ms=NOW, fetch=fetch))
        fetch.assert_not_called()
        fetch.return_value = [[CURRENT,40,41,39,40.5]]
        self.assert_code("INCORRECT_QUOTE_MINUTE", lambda:
                         source.closed_quote("ETH", NOW, now_ms=NOW, fetch=fetch))


if __name__ == "__main__":
    unittest.main()
