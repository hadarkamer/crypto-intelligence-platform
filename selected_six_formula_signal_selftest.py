"""Pure specification/boundary tests; no external I/O or product bootstrap."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
from decimal import Decimal
import math
import unittest

import selected_six_formula_signal as signal


AT = 1_794_672_000_000  # A fixed whole UTC minute; these are test-only observations.


def dated(value, observed=AT, received=None, expires=None):
    received = observed if received is None else received
    return {"value": value, "observed_at_ms": observed, "received_at_ms": received,
            "valid_until_ms": received + 5 * signal.MINUTE_MS if expires is None else expires,
            "complete": True}


def make_snapshot(symbol, values, *, direction="SHORT", event="current", observed=AT,
                  received=None, next_minute=True):
    received = observed if received is None else received
    reference_at = signal.next_full_minute_ms(received) if next_minute else received
    reference = dated(100.0, reference_at)
    reference["venue"] = "HYPERLIQUID_PERPETUAL"
    return {"source_event_id": event, "symbol": symbol, "direction": direction,
            "observed_at_ms": observed, "received_at_ms": received,
            "valid_until_ms": received + 5 * signal.MINUTE_MS,
            "inputs": {k: dated(v, observed, received) for k, v in values.items()},
            "reference": reference}


def make_sff():
    return make_snapshot("SOL", {"price_oi_30m_short_score": 40,
                                 "futures_cvd_30m_short_score": 40,
                                 "one_hour_price_change_fraction": .005,
                                 "price_history_complete_24h": True})


def make_ct009(symbol="BTC"):
    values = {"ordinary_alert_delivered": True, "timeframe": "3d", "target_price": 98}
    current = make_snapshot(symbol, values)
    previous = make_snapshot(symbol, dict(values, maxpain_score=72.8),
                             event="previous", observed=AT - signal.MINUTE_MS)
    return current, [previous]


def make_oconf(symbol="BTC"):
    values = {"confirmation_type": "MP", "confirmation_confirmed": True,
              "target_price": 98, "oi_now_short_score": 40, "oi_short_short_score": 40,
              "futures_now_short_score": 40, "futures_short_short_score": 40,
              "spot_now_short_score": 0, "spot_short_short_score": 0}
    current = make_snapshot(symbol, values, next_minute=False)
    previous = make_snapshot(symbol, {k: values[k] for k in
                                      ("confirmation_type", "confirmation_confirmed", "target_price")},
                             event="previous", observed=AT-signal.MINUTE_MS, next_minute=False)
    return current, [previous]


def make_inverse():
    return make_snapshot("XRP", {"source_state": "COMBINED_QUALIFIED", "target_price": 102,
                                 "supporting_liquidity": [{"timeframe": "3d", "upper_liquidation": 60,
                                                            "lower_liquidation": 40, "supports_source": True}]},
                         direction="LONG")


def change(row, key, value):
    row["inputs"][key]["value"] = value


class CalculationTests(unittest.TestCase):
    def test_ct_all_six_operational_symbols(self):
        for symbol in sorted(signal.CT009_SYMBOLS):
            with self.subTest(symbol=symbol):
                current, history = make_ct009(symbol)
                result = signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)
                self.assertTrue(result["signal"], result)
                d = result["decision"]
                self.assertEqual(d.symbol, symbol)
                self.assertEqual(d.source_event_ids, ("current", "previous"))
                self.assertEqual(Decimal(d.entry_price), Decimal('100.3629120588302612'))
                self.assertEqual(Decimal(d.stop_loss), Decimal('100.7651449721012717435297547'))
                self.assertEqual(d.pending_entry_expires_at_ms, AT+signal.MINUTE_MS+signal.DAY_MS)

    def test_ct_rejects_bnb_zec_unknown_before_other_valid_conditions(self):
        for symbol in ("BNB", "ZEC", "UNKNOWN"):
            with self.subTest(symbol=symbol):
                current, history = make_ct009(symbol)
                result = signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)
                self.assertFalse(result["signal"])
                self.assertEqual(result["reason"], "invalid_input:symbol_not_allowed")

    def test_ct_mean_boundary_and_current_excluded(self):
        current, history = make_ct009()
        change(current, "target_price", 98)
        current["inputs"]["maxpain_score"] = dated(100)
        change(history[0], "maxpain_score", math.nextafter(72.8, -math.inf))
        self.assertFalse(signal.evaluate_ct009(current, history+[current], AT+signal.MINUTE_MS)["signal"])
        change(history[0], "maxpain_score", 72.8)
        self.assertTrue(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])

    def test_ct_duplicate_history_does_not_reweight_mean(self):
        current, history = make_ct009()
        change(history[0], "maxpain_score", 80)
        lower = deepcopy(history[0])
        lower["source_event_id"] = "lower"
        change(lower, "maxpain_score", 60)
        # Unique mean is 70; duplicating the 80 record must not manufacture a pass.
        self.assertFalse(signal.evaluate_ct009(current, history*5+[lower], AT+signal.MINUTE_MS)["signal"])

    def test_ct_conflicting_duplicate_history_is_invalid(self):
        current, history = make_ct009()
        other = deepcopy(history[0])
        change(other, "maxpain_score", 90)
        self.assertFalse(signal.evaluate_ct009(current, history+[other], AT+signal.MINUTE_MS)["valid"])

    def test_ct_history_time_window_and_future_receipt(self):
        current, history = make_ct009()
        row = history[0]
        offset = signal.DAY_MS - signal.MINUTE_MS
        for record in [row] + list(row["inputs"].values()):
            for key in ("observed_at_ms", "received_at_ms", "valid_until_ms"):
                record[key] -= offset
        self.assertTrue(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])
        row["observed_at_ms"] -= 1
        self.assertFalse(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])
        current, history = make_ct009()
        history[0]["received_at_ms"] = AT + 1
        self.assertFalse(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])

    def test_ct_target_ratio_numeric_tolerance(self):
        current, history = make_ct009()
        change(history[0], "target_price", 98 * (1.004 + .5e-12))
        self.assertTrue(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])
        change(history[0], "target_price", 98 * (1.004 + 2e-12))
        self.assertFalse(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])

    def test_ct_requires_delivered_same_timeframe(self):
        for key, value in (("ordinary_alert_delivered", False), ("timeframe", "24h")):
            current, history = make_ct009()
            change(current, key, value)
            self.assertFalse(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])
        current, history = make_ct009()
        change(history[0], "timeframe", "48h")
        self.assertFalse(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])
        change(current, "timeframe", "48h")
        self.assertTrue(signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["signal"])

    def test_oconf_exact_geometry_and_fast_boundaries(self):
        current, history = make_oconf()
        result = signal.evaluate_oconf0034_13(current, history, AT)
        self.assertTrue(result["signal"], result)
        d = result["decision"]
        self.assertEqual(Decimal(d.entry_price), Decimal('100.4'))
        self.assertEqual(Decimal(d.stop_loss), Decimal('101.6048'))
        self.assertEqual(Decimal(d.take_profit), Decimal('98'))
        for key in ("oi_now_short_score", "futures_now_short_score", "spot_now_short_score"):
            bad = deepcopy(current)
            change(bad, key, -1 if key.startswith("spot") else 39)
            self.assertFalse(signal.evaluate_oconf0034_13(bad, history, AT)["signal"])

    def test_oconf_weighted_families_not_raw_average(self):
        current, history = make_oconf()
        change(current, "oi_now_short_score", 80)
        change(current, "oi_short_short_score", 0)
        self.assertTrue(signal.evaluate_oconf0034_13(current, history, AT)["signal"])
        change(current, "oi_now_short_score", 0)
        change(current, "oi_short_short_score", 80)
        self.assertFalse(signal.evaluate_oconf0034_13(current, history, AT)["signal"])

    def test_oconf_current_snapshot_timeframes_do_not_count_as_repeat(self):
        current, history = make_oconf()
        duplicate = deepcopy(current)
        duplicate["source_event_id"] = "another_timeframe_same_snapshot"
        self.assertFalse(signal.evaluate_oconf0034_13(current, [duplicate], AT)["signal"])
        self.assertTrue(signal.evaluate_oconf0034_13(current, history*3, AT)["signal"])

    def test_oconf_rejects_magnet_unconfirmed_and_missing_score(self):
        current, history = make_oconf()
        for key, value in (("confirmation_type", "MAGNET"), ("confirmation_confirmed", False)):
            bad = deepcopy(current)
            change(bad, key, value)
            self.assertFalse(signal.evaluate_oconf0034_13(bad, history, AT)["signal"])
        del current["inputs"]["spot_short_short_score"]
        self.assertFalse(signal.evaluate_oconf0034_13(current, history, AT)["valid"])

    def test_sff_inclusive_numeric_boundaries(self):
        current = make_sff()
        for value in (-.005, .005):
            change(current, "one_hour_price_change_fraction", value)
            self.assertTrue(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["signal"])
        for value in (math.nextafter(-.005, -math.inf), math.nextafter(.005, math.inf)):
            change(current, "one_hour_price_change_fraction", value)
            self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["signal"])
        for key in ("price_oi_30m_short_score", "futures_cvd_30m_short_score"):
            current = make_sff()
            change(current, key, math.nextafter(40, -math.inf))
            self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["signal"])

    def test_sff_complete_history_and_family_required(self):
        current = make_sff()
        change(current, "price_history_complete_24h", False)
        self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["valid"])
        current = make_sff()
        current["inputs"]["price_oi_30m_short_score"]["complete"] = False
        self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["valid"])

    def test_sff_geometry_and_unspecified_holding_not_invented(self):
        d = signal.evaluate_sff1361(make_sff(), AT+signal.MINUTE_MS)["decision"]
        self.assertEqual(tuple(Decimal(v) for v in (d.entry_price, d.stop_loss, d.take_profit)), (100, 101, 98))
        self.assertIsNone(d.pending_entry_expires_at_ms)
        self.assertIsNone(d.holding_time_limit_ms)
        self.assertFalse(d.holding_time_limit_specified)

    def test_xrp_inverse_reflects_original_reference(self):
        result = signal.evaluate_xrp_liquidity_inverse(make_inverse(), AT+signal.MINUTE_MS)
        self.assertTrue(result["signal"], result)
        d = result["decision"]
        self.assertEqual(d.source_direction, "LONG")
        self.assertEqual(d.direction, "SHORT")
        self.assertEqual(Decimal(d.source_target_price), 102)
        self.assertEqual(Decimal(d.cancel_target_price), 98)
        self.assertEqual(Decimal(d.entry_price), Decimal('100.468556908890605'))
        self.assertEqual(Decimal(d.take_profit), Decimal('98.32785429117448631404291959'))

    def test_xrp_liquidity_threshold_and_support(self):
        current = make_inverse()
        row = current["inputs"]["supporting_liquidity"]["value"][0]
        row["upper_liquidation"] = 59.999999
        self.assertFalse(signal.evaluate_xrp_liquidity_inverse(current, AT+signal.MINUTE_MS)["signal"])
        row["upper_liquidation"] = 60
        row["supports_source"] = False
        self.assertFalse(signal.evaluate_xrp_liquidity_inverse(current, AT+signal.MINUTE_MS)["signal"])
        row["supports_source"] = True
        change(current, "source_state", "NOT_QUALIFIED")
        self.assertFalse(signal.evaluate_xrp_liquidity_inverse(current, AT+signal.MINUTE_MS)["signal"])

    def test_xrp_invalid_target_and_liquidation(self):
        for target in (100, 99, 200, 201):
            current = make_inverse()
            change(current, "target_price", target)
            self.assertFalse(signal.evaluate_xrp_liquidity_inverse(current, AT+signal.MINUTE_MS)["signal"])
        current = make_inverse()
        change(current, "supporting_liquidity", [None])
        self.assertFalse(signal.evaluate_xrp_liquidity_inverse(current, AT+signal.MINUTE_MS)["valid"])

    def test_every_input_must_be_available_and_fresh(self):
        for field in make_sff()["inputs"]:
            for mode in ("expired", "future_received", "future_observed"):
                current = make_sff()
                row = current["inputs"][field]
                if mode == "expired": row["valid_until_ms"] = AT+signal.MINUTE_MS-1
                if mode == "future_received": row["received_at_ms"] = AT+signal.MINUTE_MS+1
                if mode == "future_observed": row["observed_at_ms"] = AT+1
                self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["valid"], (field, mode))
        current = make_sff()
        for row in [current]+list(current["inputs"].values())+[current["reference"]]:
            row["valid_until_ms"] = AT+signal.MINUTE_MS
        self.assertTrue(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["signal"])
        self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS+1)["signal"])

    def test_future_minute_price_never_used_early_or_substituted(self):
        self.assertFalse(signal.evaluate_sff1361(make_sff(), AT+signal.MINUTE_MS-1)["signal"])
        current = make_sff()
        current["reference"]["observed_at_ms"] += signal.MINUTE_MS
        current["reference"]["received_at_ms"] += signal.MINUTE_MS
        self.assertFalse(signal.evaluate_sff1361(current, AT+2*signal.MINUTE_MS)["signal"])
        current = make_sff()
        current["reference"]["venue"] = "BINANCE_SPOT"
        self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["signal"])

    def test_no_nonfinite_or_boolean_numeric_values(self):
        for value in (float("nan"), float("inf"), True, None):
            current = make_sff()
            change(current, "price_oi_30m_short_score", value)
            self.assertFalse(signal.evaluate_sff1361(current, AT+signal.MINUTE_MS)["valid"])

    def test_identity_stable_across_retry_and_decision_immutable(self):
        current, history = make_ct009()
        first = signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["decision"]
        second = signal.evaluate_ct009(deepcopy(current), deepcopy(history), AT+2*signal.MINUTE_MS)["decision"]
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(first.entry_price, second.entry_price)
        self.assertNotEqual(first.decision_ms, second.decision_ms)
        with self.assertRaises(FrozenInstanceError): first.entry_price = 1
        detached = first.to_dict()
        detached["entry_price"] = 1
        self.assertNotEqual(first.entry_price, 1)

    def test_pending_expiry_distinct_from_signal_or_position_lifetime(self):
        current, history = make_ct009()
        d = signal.evaluate_ct009(current, history, AT+signal.MINUTE_MS)["decision"]
        self.assertIsNone(signal.pending_cancel_reason(d, d.reference_at_ms+90_000))
        self.assertIsNone(signal.pending_cancel_reason(d, d.pending_entry_expires_at_ms-1))
        self.assertEqual(signal.pending_cancel_reason(d, d.pending_entry_expires_at_ms), "PENDING_ENTRY_EXPIRED")
        self.assertIsNone(signal.pending_cancel_reason(d, d.pending_entry_expires_at_ms, fully_filled=True))
        self.assertEqual(signal.pending_cancel_reason(d, d.reference_at_ms+1,
                         observed_price=98, observed_at_ms=d.reference_at_ms+1),
                         "ORIGINAL_TARGET_REACHED_BEFORE_FILL")

    def test_cumulative_target_episode_and_gap(self):
        self.assertTrue(signal.target_episode_continues([100,100.2],100.4,AT,AT+signal.EPISODE_GAP_MS,signal.EPISODE_GAP_MS))
        self.assertFalse(signal.target_episode_continues([100,100.2],100.6,AT,AT+1,signal.EPISODE_GAP_MS))
        self.assertFalse(signal.target_episode_continues([100],100,AT,AT+signal.EPISODE_GAP_MS+1,signal.EPISODE_GAP_MS))


if __name__ == "__main__":
    unittest.main()
