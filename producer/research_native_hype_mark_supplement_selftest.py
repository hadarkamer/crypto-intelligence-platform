"""Behavioral source/entry/window checks for isolated native HYPE MARK repair."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

import research_native_hype_mark_supplement as supplement

DECISION = datetime(2026, 9, 6, 12, 33, 12, tzinfo=timezone.utc)
ENTRY = DECISION.replace(second=0) + timedelta(minutes=1)


def event():
    return {"event_id": 901, "event_kind": "ALERT", "event_type": "MAX_PAIN_ALERT",
        "alert_time_utc": DECISION, "symbol": "HYPE", "direction": "LONG", "source_side": "SHORT",
        "timeframe": "24h", "score": 78.5, "current_price": 88.0, "target_price": 89.0,
        "initial_target_distance_pct": 1.13, "event_fingerprint": "f" * 64,
        "strategy_version": "original", "code_version": "original",
        "delivery_status": "DELIVERED",
        "engine_snapshot": {"price_source": "hyperliquid", "price_pair": "HYPEUSDT"}}


def membership():
    return {"event_id": 901, "episode_policy_version": supplement.PARENT_POLICY,
        "btc_parent_movement_id": "original-wave-9", "decision_time_utc": DECISION,
        "btc_observed_close_utc": DECISION.replace(second=0) - timedelta(milliseconds=1),
        "membership_status": "LIVE"}


def path(minutes=1440):
    bars = [{"open_time_utc": ENTRY + timedelta(minutes=i),
        "close_time_utc": ENTRY + timedelta(minutes=i + 1) - timedelta(milliseconds=1),
        "open": 100.0, "high": 101.1 if i == 0 else 100.05, "low": 99.9,
        "close": 100.0} for i in range(minutes)]
    return {**supplement.archive.SOURCE, "candles": bars}


class NativeMarkTests(unittest.TestCase):
    def derive(self, minutes=1440, **kwargs):
        return supplement.derive_measurement(kwargs.get("source", event()),
            kwargs.get("member", membership()), kwargs.get("price_path", path(minutes)),
            observed_at=kwargs.get("now", ENTRY + timedelta(minutes=minutes)))

    def test_native_reference_is_retained_but_mark_open_is_used(self):
        original = event()
        original_copy = deepcopy(original)
        result = self.derive(source=original)
        self.assertEqual(original, original_copy)
        measured = result["event"]
        self.assertEqual(measured["entry_price"], 100.0)
        self.assertEqual(measured["original_reference_price"], 88.0)
        self.assertEqual(measured["original_event"], original)
        self.assertEqual(measured["original_btc_membership"], membership())
        self.assertEqual(measured["entry_delay_seconds"], 48)
        self.assertFalse(measured["live_union_eligible"])
        self.assertEqual(measured["source_scope"], "DERIVED_NATIVE_HYPE_MARK")
        self.assertEqual(measured["calculation_status"], "COMPLETE_32_LABELS")
        self.assertEqual(len(result["outcomes"]), 32)
        self.assertEqual(len(result["metrics"]), 4)
        for horizon in (60, 240, 720, 1440):
            selected = [row for row in result["outcomes"] if row["window_minutes"] == horizon]
            self.assertEqual({row["threshold_bps"] for row in selected}, set(range(25, 201, 25)))
            one = next(row for row in selected if row["threshold_bps"] == 100)
            self.assertEqual(one["status"], "SUCCESS")
            self.assertEqual(one["reference_price"], 100.0)
            self.assertEqual(one["source_price_market"], "futures")
            self.assertEqual(one["entry_policy_version"], supplement.ENTRY_VERSION)
        for metric in result["metrics"]:
            self.assertEqual(metric["status"], "READY")
            self.assertAlmostEqual(metric["mfe_pct"], 1.1)
            self.assertAlmostEqual(metric["mae_pct"], .1)

    def test_open_windows_advance_without_new_wave(self):
        early, later = self.derive(60), self.derive(240)
        self.assertEqual([m["status"] for m in early["metrics"]], ["READY", "OPEN", "OPEN", "OPEN"])
        self.assertEqual([m["status"] for m in later["metrics"]], ["READY", "READY", "OPEN", "OPEN"])
        self.assertEqual(early["event"]["derived_measurement_id"], later["event"]["derived_measurement_id"])
        self.assertEqual(early["event"]["btc_parent_movement_id"], later["event"]["btc_parent_movement_id"])
        self.assertEqual(later["metrics"][-1]["path_samples"], 240)

    def test_gap_is_not_filled_or_hidden(self):
        p = path()
        del p["candles"][30]
        result = self.derive(price_path=p)
        self.assertTrue(all(m["status"] == "DATA_MISSING" for m in result["metrics"]))
        self.assertTrue(all(not m["path_complete"] for m in result["metrics"]))
        self.assertNotEqual(result["event"]["calculation_status"], "COMPLETE_32_LABELS")

    def test_ineligible_event_and_wrong_route_are_rejected(self):
        for field, value in (("symbol", "SOL"), ("delivery_status", "FAILED"),
                ("score", 64.9), ("score", True), ("event_fingerprint", "missing")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.derive(source={**event(), field: value})
        for field, value in (("market", "spot"), ("price_kind", "LAST"), ("pair", "HYPE/USDC")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.derive(price_path={**path(), field: value})
        with self.assertRaises(ValueError):
            self.derive(member={**membership(), "decision_time_utc": ENTRY})

    def test_entry_missing_and_unclosed_candles_are_not_invented(self):
        p = path()
        del p["candles"][0]
        result = self.derive(price_path=p)
        self.assertIsNone(result["event"]["entry_price"])
        self.assertEqual(result["event"]["calculation_status"], "DATA_MISSING_ENTRY_CANDLE")
        self.assertEqual(result["outcomes"], [])
        with self.assertRaises(ValueError):
            self.derive(60, price_path=path(61))

    def test_full_wave_entry_accepts_long_path_but_does_not_qualify_it(self):
        full = path(1600)
        result = supplement.derive_entry(event(), membership(), full)
        self.assertEqual(result["entry_time_utc"], ENTRY)
        self.assertEqual(result["entry_price"], 100)
        self.assertEqual(result["original_reference_price"], 88)
        self.assertNotEqual(result["calculation_status"], "COMPLETE_32_LABELS")


if __name__ == "__main__":
    unittest.main()
