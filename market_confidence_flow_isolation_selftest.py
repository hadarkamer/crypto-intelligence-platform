"""Independent Flow reads, unchanged healthy caching and immutable captures.

All source reads are replaced at the analyzer or its row-loading boundary.
No database, network, application startup or provider credentials are required.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import call, patch

import coinglass_flow_engine as flow
import market_confidence_engine as market


BASE = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)


def healthy(market_name, *, direction="BEARISH"):
    return {
        "symbol": "BTC",
        "market": market_name,
        "available": True,
        "windows": {
            label: {
                "available": True,
                "direction": direction,
                "continuous_strength": 0.4,
                "latest_time": BASE.isoformat(),
                "reference_time": (BASE - timedelta(hours=8)).isoformat(),
            }
            for label, _ in flow.WINDOWS
        },
        "quality": {
            "status": "PASS",
            "usable_for_confirmation": True,
            "freshness_status": "FRESH",
            "candle_close": BASE.isoformat(),
        },
    }


def history(market_name):
    """Continuous closed-candle input for the unchanged real analyzer."""
    rows, cumulative = [], 0.0
    for index in range(600):
        delta = float((index % 17 + 1) * (100 if market_name == "futures" else -70))
        cumulative += delta
        rows.append({
            "time": BASE - timedelta(minutes=30 * (599 - index)),
            "buy": 10000.0 + delta,
            "sell": 10000.0,
            "delta": delta,
            "api_cvd": cumulative,
            "continuous_cvd": cumulative,
        })
    return rows


class FlowIsolationTests(unittest.TestCase):
    def setUp(self):
        market.clear_flow_cache()
        self.addCleanup(market.clear_flow_cache)
        self.clock = patch.object(market.time, "monotonic", return_value=1000.0).start()
        self.addCleanup(patch.stopall)

    def assert_unavailable(self, result, error):
        self.assertEqual(result, {
            "available": False,
            "windows": {},
            "quality": {"status": "NO_DATA", "reasons": [repr(error)]},
        })

    def test_futures_failure_still_reads_and_preserves_spot(self):
        error, spot = RuntimeError("futures read failed"), healthy("spot")
        with patch.object(flow, "analyze_market", side_effect=[error, spot]) as read:
            actual = market._cached_flow("btc")
        self.assertEqual(read.call_args_list, [call("BTC", "futures"), call("BTC", "spot")])
        self.assertEqual(actual["symbol"], "BTC")
        self.assertIs(actual["spot"], spot)
        self.assert_unavailable(actual["futures"], error)
        self.assertNotIn("BTC", market._FLOW_CACHE)

    def test_spot_failure_preserves_already_read_futures(self):
        futures, error = healthy("futures"), RuntimeError("spot read failed")
        with patch.object(flow, "analyze_market", side_effect=[futures, error]) as read:
            actual = market._cached_flow("BTC")
        self.assertEqual(read.call_args_list, [call("BTC", "futures"), call("BTC", "spot")])
        self.assertIs(actual["futures"], futures)
        self.assert_unavailable(actual["spot"], error)
        self.assertNotIn("BTC", market._FLOW_CACHE)

    def test_both_failures_retain_distinct_errors_without_retries(self):
        errors = [RuntimeError("futures failure"), ValueError("spot failure")]
        with patch.object(flow, "analyze_market", side_effect=errors) as read:
            actual = market._cached_flow("BTC")
        self.assertEqual(read.call_args_list, [call("BTC", "futures"), call("BTC", "spot")])
        for market_name, error in zip(("futures", "spot"), errors):
            self.assert_unavailable(actual[market_name], error)
        self.assertNotIn("BTC", market._FLOW_CACHE)

    def test_partial_result_is_not_cached_and_later_capture_can_recover(self):
        with patch.object(flow, "analyze_market", side_effect=[
            RuntimeError("first futures failure"), healthy("spot"),
            healthy("futures"), healthy("spot"),
        ]) as read:
            first = market._cached_flow("BTC")
            self.assertNotIn("BTC", market._FLOW_CACHE)
            second = market._cached_flow("BTC")
            third = market._cached_flow("BTC")
        self.assertEqual(read.call_args_list, [
            call("BTC", "futures"), call("BTC", "spot"),
            call("BTC", "futures"), call("BTC", "spot"),
        ])
        self.assertFalse(first["futures"]["available"])
        self.assertTrue(second["futures"]["available"])
        self.assertEqual(second, third)
        self.assertIn("BTC", market._FLOW_CACHE)

    def test_expired_cache_cannot_substitute_for_failed_read(self):
        error = RuntimeError("new futures failure")
        old_futures = healthy("futures", direction="BULLISH")
        new_spot = healthy("spot", direction="BEARISH")
        with patch.object(flow, "analyze_market", side_effect=[
            old_futures, healthy("spot", direction="BULLISH"), error, new_spot,
            error, new_spot,
        ]) as read:
            old = market._cached_flow("BTC")
            self.clock.return_value = 1300.001
            current = market._cached_flow("BTC")
            again = market._cached_flow("BTC")
        self.assertTrue(old["futures"]["available"])
        self.assert_unavailable(current["futures"], error)
        self.assertIs(current["spot"], new_spot)
        self.assertEqual(again, current)
        self.assertEqual(read.call_count, 6)

    def test_healthy_result_exactly_matches_public_analyze_symbol(self):
        with patch.object(flow, "_load_rows", side_effect=lambda symbol, name: history(name)), \
             patch.object(flow.flow_foundation, "candle_age_minutes", return_value=1.0):
            expected = flow.analyze_symbol("btc")
            actual = market._cached_flow("btc")
        self.assertTrue(expected["futures"]["available"])
        self.assertTrue(expected["spot"]["available"])
        self.assertEqual(actual, expected)
        self.assertEqual(market._FLOW_CACHE["BTC"], (1000.0, expected))

    def test_cache_copies_protect_from_caller_mutation_on_miss_and_hit(self):
        expected = {"symbol": "BTC", "futures": healthy("futures"), "spot": healthy("spot")}
        with patch.object(flow, "analyze_market", side_effect=[
            deepcopy(expected["futures"]), deepcopy(expected["spot"]),
        ]) as read:
            first = market._cached_flow("BTC")
            first["futures"]["windows"].clear()
            second = market._cached_flow("BTC")
            self.assertEqual(second, expected)
            second["spot"]["quality"]["status"] = "CALLER_MUTATION"
            self.assertEqual(market._cached_flow("BTC"), expected)
        self.assertEqual(read.call_count, 2)

    def test_ttl_boundary_and_existing_invalidation_are_preserved(self):
        with patch.object(flow, "analyze_market", side_effect=lambda symbol, name: healthy(name)) as read:
            market._cached_flow("BTC")
            self.clock.return_value = 1300.0
            market._cached_flow("btc")
            self.assertEqual(read.call_count, 2)
            self.clock.return_value = 1300.001
            market._cached_flow("BTC")
            self.assertEqual(read.call_count, 4)
            market._cached_flow("ETH")
            market.clear_flow_cache("btc")
            self.assertNotIn("BTC", market._FLOW_CACHE)
            self.assertIn("ETH", market._FLOW_CACHE)
            market._cached_flow("BTC")
            self.assertEqual(read.call_count, 8)
            market.clear_flow_cache()
            self.assertEqual(market._FLOW_CACHE, {})

    def test_successful_empty_data_remains_exact_and_cacheable(self):
        with patch.object(flow, "_load_rows", return_value=[]) as read:
            expected = flow.analyze_symbol("BTC")
            read.reset_mock()
            actual = market._cached_flow("BTC")
            self.assertEqual(market._cached_flow("BTC"), actual)
        self.assertEqual(actual, expected)
        self.assertEqual(read.call_args_list, [call("BTC", "futures"), call("BTC", "spot")])
        self.assertEqual(actual["futures"]["quality"]["reasons"], ["no stored rows"])
        self.assertFalse(actual["spot"]["available"])
        self.assertIn("BTC", market._FLOW_CACHE)

    def test_capture_to_combine_preserves_healthy_family_and_source_time(self):
        for failed in ("futures", "spot"):
            with self.subTest(failed=failed):
                market.clear_flow_cache()
                good = "spot" if failed == "futures" else "futures"
                error = RuntimeError(f"{failed} read failed")
                regime = {"available": False, "windows": {}, "data_quality_status": "NO_DATA"}
                results = [error if name == failed else healthy(name) for name in ("futures", "spot")]
                with patch.object(flow, "analyze_market", side_effect=results) as read, \
                     patch.object(market.coinglass_oi_regime_service, "latest", return_value=regime) as oi:
                    snapshot = market.capture_snapshot(["btc", "BTC"])["BTC"]
                    evidence = market.combine("BTC", "BEARISH", snapshot["regime"], snapshot["flow"], 71)
                self.assertEqual(read.call_count, 2)
                oi.assert_called_once_with("BTC")
                self.assertEqual(snapshot["regime"], regime)
                self.assertEqual(evidence["maxpain_score"], 71)
                self.assertTrue(evidence["modules"][f"{good}_flow"]["available"])
                self.assertEqual(evidence["modules"][f"{good}_flow"]["score"], -40)
                failed_module = evidence["modules"][f"{failed}_flow"]
                self.assertFalse(failed_module["available"])
                self.assertEqual(failed_module["score"], 0)
                self.assertEqual(failed_module["quality_reasons"], [repr(error)])
                timing = snapshot["timing_observation"]
                self.assertEqual(timing[f"{good}_source_close_at_utc"], BASE.isoformat())
                self.assertIsNone(timing[f"{failed}_source_close_at_utc"])
                self.assertIsNotNone(datetime.fromisoformat(timing["cvd_observed_at_utc"]).tzinfo)

    def test_later_recovery_cannot_repair_an_earlier_frozen_capture(self):
        regime = {"available": False, "windows": {}, "data_quality_status": "NO_DATA"}
        with patch.object(flow, "analyze_market", side_effect=[
            RuntimeError("earlier unavailable"), healthy("spot"),
            healthy("futures"), healthy("spot"),
        ]) as read, patch.object(market.coinglass_oi_regime_service, "latest", return_value=regime):
            earlier = market.capture_snapshot(["BTC"])["BTC"]
            frozen = deepcopy(earlier)
            before = market.combine("BTC", "BEARISH", earlier["regime"], earlier["flow"])
            later = market.capture_snapshot(["BTC"])["BTC"]
            after = market.combine("BTC", "BEARISH", earlier["regime"], earlier["flow"])
        self.assertEqual(read.call_count, 4)
        self.assertEqual(earlier, frozen)
        self.assertEqual(before, after)
        self.assertFalse(after["modules"]["futures_flow"]["available"])
        self.assertTrue(later["flow"]["futures"]["available"])


if __name__ == "__main__":
    unittest.main()
