"""Offline regression checks for observation-only producer and cache hooks."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gc
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
import weakref
from unittest.mock import Mock, patch

try:
    import requests
except ImportError:  # The standalone numeric audit uses only the stdlib.
    sys.modules["requests"] = types.ModuleType("requests")

import research_consumed_inputs as capture
import coinglass_flow_engine as flow
import coinglass_history_backfill as history
import coinglass_oi_regime_service as regime
import market_confidence_engine as confidence


def payloads(collector, kind):
    return [body for _, body in capture.event_payloads(collector.package(), kind)]


class HistoryRequestTests(unittest.TestCase):
    def response(self, status=200, payload=None, headers=None):
        response = Mock(status_code=status, headers=headers or {})
        response.json.return_value = payload or {"code": "0", "data": []}
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(str(status), response=response)
        return response

    def test_permanent_failures_stop_without_backoff(self):
        cases = [self.response(status=401), self.response(status=403)]
        cases += [self.response(payload={"code": code, "msg": message}) for code, message in (
            ("401", "Unauthorized"), ("403", "Forbidden"),
            ("400", "The requested pair does not exist on the exchange."),
        )]
        for response in cases:
            with self.subTest(response=response), patch.object(history, '_api_key', return_value='fixture'), \
                 patch.object(history.requests, 'get', return_value=response) as get, \
                 patch.object(history.time, 'sleep') as sleep:
                with self.assertRaisesRegex(RuntimeError, 'after 1 attempts'):
                    history._request('/fixture', {})
                get.assert_called_once()
                sleep.assert_not_called()

    def test_transient_failures_keep_backoff_and_recover(self):
        for failure in (self.response(status=429, headers={'Retry-After': '12'}),
                        self.response(status=503), requests.Timeout('slow'),
                        self.response(payload={'code': '500', 'msg': 'temporary'})):
            with self.subTest(failure=failure), patch.object(history, '_api_key', return_value='fixture'), \
                 patch.object(history.requests, 'get', side_effect=[failure, self.response()]) as get, \
                 patch.object(history.time, 'sleep') as sleep:
                self.assertEqual(history._request('/fixture', {})['code'], '0')
                self.assertEqual(get.call_count, 2)
                sleep.assert_called_once_with(12.0 if getattr(failure, 'status_code', None) == 429 else 2.0)

    def test_exhausted_network_failures_are_bounded(self):
        with patch.object(history, '_api_key', return_value='fixture'), \
             patch.object(history.requests, 'get', side_effect=requests.Timeout('slow')) as get, \
             patch.object(history.time, 'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'after 4 attempts'):
                history._request('/fixture', {})
            self.assertEqual(get.call_count, 4)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [2.0, 5.0, 10.0])

    def test_unsupported_pair_preserves_market_fallback(self):
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        candles = [{'time': i + 1, 'close': 10.0} for i in range(100)]
        responses = [self.response(payload={'code': '400', 'msg': 'The requested pair does not exist on the exchange.'}),
                     self.response(payload={'code': '0', 'data': candles})]
        with patch.object(history, '_api_key', return_value='fixture'), \
             patch.object(history, '_price_candidates', return_value=[('first', 'XRPUSDT'), ('second', 'XRPUSDT')]), \
             patch.object(history.requests, 'get', side_effect=responses) as get, \
             patch.object(history.time, 'sleep') as sleep:
            rows, exchange, pair = history.fetch_price_history('XRP', start, start + timedelta(hours=1))
        self.assertEqual((len(rows), exchange, pair), (100, 'second', 'XRPUSDT'))
        self.assertEqual(get.call_count, 2)
        sleep.assert_called_once_with(history.REQUEST_PAUSE_SECONDS)


class CacheInputTests(unittest.TestCase):
    def setUp(self):
        confidence.clear_flow_cache()
        history._REFERENCE_CACHE.clear()
        history._REFERENCE_INPUT_RECEIPTS.clear()
        self.value = 10

    def analyze(self, symbol):
        value = self.value
        result = {"symbol": symbol, "futures": {"marker": value}, "spot": {"marker": -value}}
        for market in ("futures", "spot"):
            capture.record("flow_rows", {"arguments": {"symbol": symbol, "market": market},
                "result": [{"time": "2026-01-01T00:00:00+00:00", "delta": value}]})
        return result

    def baseline(self, symbol):
        value = self.value
        result = {"symbol": symbol, "available": True, "windows": {"30m": {"marker": value}}}
        capture.record("oi_baseline_rows", {"arguments": {"symbol": symbol},
            "result": [{"candle_time": "2026-01-01T00:00:00+00:00", "price_close": value}]})
        capture.record("oi_baseline_calculation", {"arguments": {"symbol": symbol}, "result": result})
        return result

    def test_flow_hit_uses_original_rows_after_source_changes(self):
        with patch.object(flow, "analyze_symbol", side_effect=self.analyze) as compute:
            with capture.collect() as first:
                result = confidence._cached_flow("ETH")
            self.value = 99
            with capture.collect() as second:
                cached = confidence._cached_flow("ETH")
        self.assertEqual(compute.call_count, 1)
        self.assertEqual(cached, result)
        self.assertIsNot(cached, result)
        self.assertEqual(payloads(second, "flow_rows")[0]["result"][0]["delta"], 10)
        self.assertTrue(payloads(second, "generation_use")[0]["cache_hit"])
        self.assertFalse(payloads(first, "generation_use")[0]["cache_hit"])

    def test_flow_refresh_binds_new_generation(self):
        with patch.object(flow, "analyze_symbol", side_effect=self.analyze):
            with capture.collect() as first:
                confidence._cached_flow("DOGE")
            confidence.clear_flow_cache("DOGE")
            self.value = 20
            with capture.collect() as second:
                result = confidence._cached_flow("DOGE")
        self.assertEqual(result["futures"]["marker"], 20)
        self.assertNotEqual(payloads(first, "generation_use")[0]["generation_id"],
                            payloads(second, "generation_use")[0]["generation_id"])

    def test_flow_missing_receipt_keeps_cached_result_and_marks_gap(self):
        confidence._FLOW_CACHE["ETH"] = (time.monotonic(), {"marker": 9})
        with patch.object(flow, "analyze_symbol", side_effect=AssertionError("must not recompute")):
            with capture.collect() as evidence:
                result = confidence._cached_flow("ETH")
        self.assertEqual(result, {"marker": 9})
        self.assertEqual(len(payloads(evidence, "capture_gap")), 1)
        self.assertTrue(evidence.package()["errors"])

    def test_flow_mutated_same_object_is_gap(self):
        with patch.object(flow, "analyze_symbol", side_effect=self.analyze):
            confidence._cached_flow("ETH")
            confidence._FLOW_CACHE["ETH"][1]["futures"]["marker"] = 77
            with capture.collect() as evidence:
                result = confidence._cached_flow("ETH")
        self.assertEqual(result["futures"]["marker"], 77)
        self.assertEqual(len(payloads(evidence, "capture_gap")), 1)
        self.assertFalse(payloads(evidence, "generation_use"))

    def test_baseline_hit_keeps_population_and_no_refetch(self):
        with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline) as compute:
            original = history.get_reference_ranges("ETH")
            self.value = 100
            with capture.collect() as evidence:
                result = history.get_reference_ranges("ETH")
        self.assertIs(original, result)
        self.assertEqual(compute.call_count, 1)
        self.assertEqual(payloads(evidence, "oi_baseline_rows")[0]["result"][0]["price_close"], 10)
        self.assertTrue(payloads(evidence, "generation_use")[0]["cache_hit"])

    def test_baseline_refresh_changes_generation(self):
        with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline):
            with capture.collect() as first:
                history.get_reference_ranges("ETH")
            self.value = 11
            with capture.collect() as second:
                result = history.get_reference_ranges("ETH", refresh=True)
        self.assertEqual(result["windows"]["30m"]["marker"], 11)
        self.assertNotEqual(payloads(first, "generation_use")[0]["generation_id"],
                            payloads(second, "generation_use")[0]["generation_id"])

    def test_baseline_missing_or_mutated_receipt_never_refetches(self):
        with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline) as compute:
            result = history.get_reference_ranges("DOGE")
            result["windows"]["30m"]["marker"] = 12
            with capture.collect() as evidence:
                cached = history.get_reference_ranges("DOGE")
        self.assertIs(cached, result)
        self.assertEqual(compute.call_count, 1)
        self.assertEqual(len(payloads(evidence, "capture_gap")), 1)

    def test_refresh_releases_unused_result_but_keeps_active_generation(self):
        with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline):
            old = history.get_reference_ranges("ETH")
            old_ref = weakref.ref(old)
            self.value = 11
            current = history.get_reference_ranges("ETH", refresh=True)
            with capture.collect() as evidence:
                history._use_reference_inputs("ETH", old, True)
            self.assertEqual(payloads(evidence, "oi_baseline_calculation")[0]["result"], old)
            self.assertFalse(evidence.errors)
            del old
            gc.collect()
            self.assertIsNone(old_ref())
            self.assertIs(history.get_reference_ranges("ETH"), current)

    def test_baseline_hit_without_capture_skips_serializing_guard(self):
        with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline):
            result = history.get_reference_ranges("ETH")
        with patch.object(history, "_local_input_guard", side_effect=AssertionError("unused guard")) as guard:
            self.assertIs(history.get_reference_ranges("ETH"), result)
        guard.assert_not_called()

    def test_concurrent_baseline_builds_bind_actual_returned_object(self):
        barrier = threading.Barrier(2)
        def calculate(symbol):
            value = threading.current_thread().name
            result = {"symbol": symbol, "marker": value}
            capture.record("oi_baseline_calculation", {"arguments": {"symbol": symbol}, "result": result})
            barrier.wait(timeout=5)
            return result
        def run():
            with capture.collect() as evidence:
                result = history.get_reference_ranges("ETH", refresh=True)
            body = payloads(evidence, "oi_baseline_calculation")[0]["result"]
            return result, body, payloads(evidence, "generation_use")[0]
        with patch.object(history, "calculate_reference_ranges", side_effect=calculate):
            with ThreadPoolExecutor(max_workers=2) as pool:
                outputs = list(pool.map(lambda _: run(), range(2)))
        for result, body, receipt in outputs:
            self.assertEqual(result, body)
            self.assertEqual(capture.digest(result), receipt["result_sha256"])
        self.assertNotEqual(outputs[0][2]["generation_id"], outputs[1][2]["generation_id"])

    def test_concurrent_flow_builds_bind_actual_returned_tuple(self):
        barrier = threading.Barrier(2)
        def analyze(symbol):
            result = {"symbol": symbol, "marker": threading.current_thread().name}
            capture.record("flow_rows", {"arguments": {"symbol": symbol}, "result": [result]})
            barrier.wait(timeout=5)
            return result
        def run():
            with capture.collect() as evidence:
                result = confidence._cached_flow("DOGE")
            return result, payloads(evidence, "flow_cached_generation")[0]["result"], payloads(evidence, "generation_use")[0]
        with patch.object(flow, "analyze_symbol", side_effect=analyze):
            with ThreadPoolExecutor(max_workers=2) as pool:
                outputs = list(pool.map(lambda _: run(), range(2)))
        for result, body, receipt in outputs:
            self.assertEqual(result, body)
            self.assertEqual(capture.digest(result), receipt["result_sha256"])

    def test_capture_context_failure_preserves_computation(self):
        with patch.object(capture, "collect", side_effect=RuntimeError("observer unavailable")):
            with patch.object(flow, "analyze_symbol", side_effect=self.analyze):
                flow_result = confidence._cached_flow("ETH")
            with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline):
                baseline_result = history.get_reference_ranges("ETH")
        self.assertEqual(flow_result["futures"]["marker"], 10)
        self.assertEqual(baseline_result["windows"]["30m"]["marker"], 10)

    def test_other_symbols_do_not_add_large_generation_captures(self):
        with capture.collect() as evidence:
            with patch.object(flow, "analyze_symbol", return_value={"symbol": "BTC"}):
                confidence._cached_flow("BTC")
            with patch.object(history, "calculate_reference_ranges", return_value={"symbol": "BTC"}):
                history.get_reference_ranges("BTC")
        self.assertEqual(evidence.package()["events"], [])

    def test_sidecar_entry_and_byte_limits_are_enforced(self):
        with patch.object(history, "calculate_reference_ranges", side_effect=self.baseline):
            for _ in range(10):
                history.get_reference_ranges("ETH", refresh=True)
        self.assertLessEqual(len(history._REFERENCE_INPUT_RECEIPTS), history._REFERENCE_INPUT_MAX_ENTRIES)
        self.assertLessEqual(sum(v[3] for v in history._REFERENCE_INPUT_RECEIPTS.values()), history._REFERENCE_INPUT_MAX_BYTES)


class ProducerInputTests(unittest.TestCase):
    def test_flow_sql_output_and_record_are_equal(self):
        test_root = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory(dir=test_root) as directory:
            self.assertTrue(Path(directory).resolve().is_relative_to(test_root))
            db = str(Path(directory) / "raw.db")
            with sqlite3.connect(db) as connection:
                connection.execute("CREATE TABLE futures_taker_history (symbol TEXT,candle_time TEXT,buy_volume_usd REAL,sell_volume_usd REAL,api_cum_vol_delta_usd REAL,continuous_cum_vol_delta_usd REAL)")
                connection.execute("INSERT INTO futures_taker_history VALUES (?,?,?,?,?,?)", ("ETH", "2026-01-01T00:00:00+00:00", 5, 2, 12, 3))
            connection.close()
            with patch.object(flow, "DATABASE_URL", ""), patch.object(flow, "DB_PATH", db):
                ordinary = flow._load_rows.__wrapped__("ETH", "futures")
                with capture.collect() as evidence:
                    observed = flow._load_rows("ETH", "futures")
            self.assertEqual(observed, ordinary)
            self.assertEqual(payloads(evidence, "flow_rows")[0]["result"], capture.normalized(ordinary))
            self.assertEqual(observed[0]["delta"], 3)
            gc.collect()

    def test_actual_selected_reference_is_recorded_without_row_population_copy(self):
        now = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)
        rows = [{"collected_at": now-timedelta(minutes=31), "price": 10, "open_interest_usd": 20}]
        candidate = {"collected_at": now-timedelta(minutes=30), "price": 11, "open_interest_usd": 22, "source": "historical_backfill"}
        with patch.object(history, "historical_point_nearest", return_value=candidate):
            ordinary = regime._reference_for_window.__wrapped__(rows, now, 30, "ETH")
            with capture.collect() as evidence:
                observed = regime._reference_for_window(rows, now, 30, "ETH")
        self.assertEqual(observed, ordinary)
        body = payloads(evidence, "oi_reference_selection")[0]
        self.assertEqual(body["result"], capture.normalized(observed))
        self.assertNotIn("rows", body["arguments"])
        self.assertEqual(body["result"]["price"], 11)

    def test_observer_exception_does_not_change_return_or_original_error(self):
        def value(symbol):
            return {"value": 0.125, "symbol": symbol}
        observed = regime._observe_inputs("test", ("symbol",))(value)
        with patch.object(capture, "record", side_effect=RuntimeError("capture failed")):
            self.assertEqual(observed("ETH"), value("ETH"))
        def error(symbol):
            raise ValueError("source failure")
        observed_error = regime._observe_inputs("test", ("symbol",))(error)
        with self.assertRaisesRegex(ValueError, "source failure"):
            observed_error("ETH")


if __name__ == "__main__":
    unittest.main()
