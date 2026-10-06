"""Network-free prospective leakage, mutation, state and metric regressions."""
from copy import deepcopy
from datetime import timedelta
import unittest

import research_ordered_validation as validation
import research_common_window_metrics as full_window
import research_ordered_first_touch as ordered
from research_formula_ordered_v7_selftest import START, row as v7row, nondecisive_row

SCOPE = {"scope_key": "test-scope", "candidate_key": "PRICE_OI_TOTAL_65", "symbol": "BTC",
         "direction": "LONG", "threshold_bps": 50, "window_minutes": 60,
         "period_key": "SINCE_20260904", "period_version": "live-israel-cutoffs-v1",
         "period_start_utc": "2026-09-03T21:00:00Z", "source_scope": "LIVE",
         "parent_policy_version": "btc-parent-test-v1"}
CANDIDATE = {"formula_id": "PRICE_OI_TOTAL_65", "catalog_version": "test-prespecified-v1",
             "conditions": [{"feature": "price_oi.aligned_score", "operator": ">=", "value": 65}]}


def item(event_id=1, *, start=None, parent_start=None, status="SUCCESS", wave=None):
    start = start or START + timedelta(hours=event_id)
    args = {"event_id": event_id, "wave": wave or f"wave-{event_id}", "start": start, "symbol": "BTC"}
    result = v7row(**args, success=status == "SUCCESS") if status in {"SUCCESS", "FAILURE"} else nondecisive_row(**args, status=status)
    return {**result, "parent_start_time_utc": parent_start or start - timedelta(minutes=1),
            "source_scope": "LIVE", "period_key": SCOPE["period_key"], "period_version": SCOPE["period_version"]}


def common(row, *, mfe=2.0, mae=1.0):
    start = row["alert_time_utc"]
    end = start + timedelta(minutes=60)
    return {"method_version": validation.COMMON_WINDOW_VERSION, "measurement_kind": "FIXED_WINDOW",
            "event_id": row["event_id"], "window_minutes": 60, "status": "READY", "direction": "LONG",
            "symbol": "BTC", "source": {"symbol": "BTC", "exchange": "binance", "market": "spot", "pair": "BTCUSDT", "interval": "1m", "interval_seconds": 60},
            "reference_price": 100, "measurement_start_utc": start, "window_end_utc": end,
            "observed_at_utc": end,
            "observed_from_utc": start, "path_samples": 60, "candle_interval_seconds": 60,
            "observed_through_utc": end.replace(second=0, microsecond=0) - timedelta(milliseconds=1),
            "observation_closed": True, "path_complete": True,
            "data_quality_status": "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES", "mfe_pct": mfe, "mae_pct": mae}


def policy():
    # These are deliberately test-only values, never installed in production.
    return {"policy_version": "TEST_ONLY_DO_NOT_DEPLOY", "documented_source": "selftest fixture",
            "rationale": "Exercise configurable acceptance; not trading criteria",
            "binding_sha256": validation.digest(validation.binding(SCOPE, CANDIDATE)),
            "combination": "PROBABILITY_OR_ASYMMETRY",
            "multiplicity": {"family_id": "TEST_ONLY", "method": "PRESPECIFIED_FIXED_FAMILY_DISCLOSURE",
                             "registered_attempts": 1, "justification": "one test fixture"},
            "routes": {"PROBABILITY": [{"metric": "hit_rate_pct", "operator": ">=", "value": 80}],
                       "ASYMMETRY": [{"metric": "common_window_asymmetry_ratio", "operator": ">=", "value": 3}]}}


def run(rows, *, now=None, registration=None, windows=None, **kwargs):
    registered = registration or validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START)
    return validation.evaluate(registered, rows, analysis_as_of_utc=now or START + timedelta(days=1),
                               source_coverage_complete=True, common_window_rows=windows, **kwargs)


class ProspectiveValidationTests(unittest.TestCase):
    def test_whole_wave_never_crosses_freeze(self):
        rows = [item(1, parent_start=START - timedelta(hours=1)),
                item(2, parent_start=START), item(3, parent_start=START + timedelta(seconds=1))]
        result = run(rows)
        self.assertEqual(result["discovery"]["selected_waves"], 2)
        self.assertEqual(result["prospective"]["selected_waves"], 1)
        self.assertEqual(result["prospective"]["btc_parent_movement_ids"], ["wave-3"])

    def test_late_decision_on_old_wave_is_discovery(self):
        result = run([item(start=START + timedelta(hours=8), parent_start=START - timedelta(minutes=1))])
        self.assertEqual(result["prospective"]["resolved_waves"], 0)
        self.assertEqual(result["discovery"]["resolved_waves"], 1)

    def test_definition_changes_cannot_reuse_freeze(self):
        registered = validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START)
        for field, value in (("threshold_bps", 75), ("direction", "SHORT"), ("period_version", "new-v2"),
                             ("source_scope", "ARCHIVE"), ("window_minutes", 240)):
            with self.assertRaises(ValueError):
                validation.assert_same_registration(registered, {**SCOPE, field: value}, CANDIDATE)
        changed = deepcopy(CANDIDATE)
        changed["conditions"][0]["value"] = 70
        with self.assertRaises(ValueError):
            validation.assert_same_registration(registered, SCOPE, changed)

    def test_each_status_kept_and_only_decisive_denominator(self):
        rows = [item(i, status=status) for i, status in enumerate(
            ("SUCCESS", "FAILURE", "OPEN", "AMBIGUOUS", "NO_TOUCH", "DATA_MISSING"), 1)]
        result = run(rows)
        self.assertEqual(result["prospective"]["status_counts"], dict.fromkeys(validation.evidence.WAVE_STATUSES, 1))
        self.assertEqual(result["prospective"]["hit_rate_pct"], 50)
        self.assertEqual(result["prospective"]["resolved_waves"], 2)

    def test_same_wave_repeats_and_updated_outcome_never_new_trials(self):
        first = item(status="OPEN")
        later = item(2, wave="wave-1", parent_start=first["parent_start_time_utc"])
        result = run([first, later])
        self.assertEqual(result["prospective"]["selected_waves"], 1)
        self.assertEqual(result["prospective"]["status_counts"]["OPEN"], 1)
        fixed = item(1, start=first["alert_time_utc"])
        updated = run([fixed, later])
        self.assertEqual(updated["prospective"]["selected_waves"], 1)
        self.assertEqual(updated["prospective"]["resolved_waves"], 1)

    def test_mixed_source_and_period_are_excluded_and_block_population(self):
        for field, value in (("source_scope", "ARCHIVE"), ("period_key", "ALL_COMPATIBLE_SINCE_20260816"),
                             ("period_version", "wrong-v0"), ("episode_policy_version", "old-policy")):
            result = run([{**item(), field: value}])
            self.assertEqual(result["prospective"]["selected_waves"], 0)
            self.assertFalse(result["source_coverage_complete"])

    def test_wave_crossing_period_stays_outside_both_partitions(self):
        result = run([item(parent_start=START - timedelta(days=4))])
        self.assertEqual(result["excluded_rows"]["WHOLE_WAVE_CROSSES_PERIOD_BOUNDARY"], 1)
        self.assertEqual(result["discovery"]["selected_waves"], 0)

    def test_fresh_expires_without_changing_frozen_registration(self):
        rows = [item(i) for i in range(1, 4)]
        registered = validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START, acceptance_policy=policy())
        windows = {row["event_id"]: common(row) for row in rows}
        result = run(rows, registration=registered, windows=windows)
        self.assertTrue(result["fresh"]["sample_count_eligible"])
        self.assertTrue(result["fresh"]["research_ready"])
        expired = run(rows, now=START + timedelta(days=15), registration=registered, windows=windows)
        self.assertFalse(expired["fresh"]["sample_count_eligible"])
        self.assertFalse(expired["research_ready"])
        self.assertEqual(result["freeze_id"], expired["freeze_id"])

    def test_minimum_is_not_acceptance_and_missing_contract_named(self):
        rows = [item(i) for i in range(1, 6)]
        result = run(rows, windows={row["event_id"]: common(row) for row in rows})
        self.assertTrue(result["standard"]["sample_count_eligible"])
        self.assertFalse(result["research_ready"])
        self.assertIn("MISSING_EXACT_ORDERED_V7_ACCEPTANCE_CONTRACT", result["standard"]["blockers"])

    def test_asymmetry_includes_failures_and_nontouch_same_horizon(self):
        rows = [item(1), item(2, status="FAILURE"), item(3, status="NO_TOUCH")]
        windows = {1: common(rows[0], mfe=9, mae=1), 2: common(rows[1], mfe=1, mae=9),
                   3: common(rows[2], mfe=1, mae=1)}
        result = run(rows, windows=windows)
        self.assertEqual(result["prospective"]["common_window_asymmetry_ratio"], 1)
        self.assertEqual(result["prospective"]["common_window_waves"], 3)
        windows.pop(2)
        self.assertIsNone(run(rows, windows=windows)["prospective"]["common_window_asymmetry_ratio"])

    def test_wrong_horizon_and_zero_mae_not_false_asymmetry(self):
        rows = [item()]
        bad = {1: {**common(rows[0]), "window_minutes": 240}}
        self.assertIsNone(run(rows, windows=bad)["prospective"]["common_window_asymmetry_ratio"])
        zero = {1: common(rows[0], mae=0)}
        self.assertEqual(run(rows, windows=zero)["prospective"]["common_window_asymmetry_state"], "ZERO_DENOMINATOR")

    def test_actual_nonminute_v7_and_full_window_join(self):
        start = START + timedelta(hours=1, seconds=30)
        first = start.replace(second=0) + timedelta(minutes=1)
        path = [{"open_time_utc": first + timedelta(minutes=i),
                 "close_time_utc": first + timedelta(minutes=i, seconds=59, milliseconds=999),
                 "open": 100, "close": 100, "high": 101 if i == 0 else 103, "low": 99.8}
                for i in range(59)]
        label = ordered.calculate_ordered_first_touch_outcome(reference_price=100, direction="LONG",
            event_time=start, candles=path, threshold_pct=.5, observation_closed=True, path_complete=True)
        row = {**item(start=start), **label}
        measured = full_window.calculate_common_window_metrics(symbol="BTC", reference_price=100,
            direction="LONG", event_time=start, window_minutes=60, candles=path,
            observed_at=START + timedelta(days=1), path_result={"symbol": "BTC", "exchange": "binance",
                "market": "spot", "pair": "BTCUSDT", "interval": "1m", "interval_seconds": 60, "complete": True})
        self.assertEqual(measured["status"], "READY")
        result = run([row], windows={1: measured})
        self.assertEqual(result["prospective"]["resolved_waves"], 1)
        self.assertTrue(result["prospective"]["common_window_complete"])
        self.assertAlmostEqual(result["prospective"]["common_window_asymmetry_ratio"], 15)
        # The final full candle closes before this non-minute window ends.
        # A READY record measured later must not leak into that earlier as-of.
        early = run([row], windows={1: measured}, now=measured["observed_through_utc"])
        self.assertFalse(early["prospective"]["common_window_complete"])
        early_observation = run([row], windows={1: measured}, now=measured["window_end_utc"])
        self.assertFalse(early_observation["prospective"]["common_window_complete"])

    def test_full_window_identity_binding(self):
        row = item()
        for changed in ({"reference_price": 101}, {"symbol": "ETHUSDT"}, {"event_id": 99},
                        {"source": {**common(row)["source"], "market": "futures"}}):
            self.assertIsNone(run([row], windows={1: {**common(row), **changed}})["prospective"]["common_window_asymmetry_ratio"])
        inverse_mapping = {**row, "outcome_event_id": 99, "ordered_outcome": {**row, "event_id": 99}}
        self.assertTrue(run([inverse_mapping], windows={1: {**common(row), "event_id": 99}})["prospective"]["common_window_complete"])

    def test_window_ending_exactly_at_exchange_candle_close_includes_that_candle(self):
        start = START + timedelta(hours=1, seconds=59, milliseconds=999)
        first = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
        path = [{"open_time_utc": first + timedelta(minutes=i),
                 "close_time_utc": first + timedelta(minutes=i, seconds=59, milliseconds=999),
                 "open": 100, "close": 100, "high": 103, "low": 99.8} for i in range(60)]
        measured = full_window.calculate_common_window_metrics(symbol="BTC", reference_price=100,
            direction="LONG", event_time=start, window_minutes=60, candles=path,
            observed_at=START + timedelta(days=1), path_result={"symbol": "BTC", "exchange": "binance",
                "market": "spot", "pair": "BTCUSDT", "interval": "1m", "interval_seconds": 60, "complete": True})
        self.assertEqual(measured["observed_through_utc"], measured["window_end_utc"])
        self.assertEqual(measured["path_samples"], 60)
        contract = validation.binding(SCOPE, CANDIDATE)
        self.assertIsNotNone(validation._full_window(item(start=start), measured, contract, START + timedelta(days=1)))

    def test_probability_asymmetry_and_joint_paths_are_distinct(self):
        rows = [item(i) for i in range(1, 6)]
        registered = validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START, acceptance_policy=policy())
        result = run(rows, registration=registered, windows={row["event_id"]: common(row) for row in rows})
        self.assertTrue(result["standard"]["paths"]["PROBABILITY"]["metrics_pass"])
        self.assertFalse(result["standard"]["paths"]["ASYMMETRY"]["metrics_pass"])
        self.assertFalse(result["standard"]["both_metrics_pass"])
        self.assertTrue(result["standard"]["research_ready"])

    def test_explicit_probability_only_path_does_not_require_unrelated_metrics(self):
        rows = [item(i) for i in range(1, 6)]
        configured = validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START, acceptance_policy=policy())
        result = run(rows, registration=configured, windows={})
        self.assertTrue(result["standard"]["research_ready"])
        self.assertFalse(result["prospective"]["common_window_complete"])
        self.assertEqual(result["standard"]["paths"]["ASYMMETRY"]["metric_blockers"], ["MISSING_METRIC:common_window_asymmetry_ratio"])
        conjunction = policy()
        conjunction["combination"] = "PROBABILITY_AND_ASYMMETRY"
        configured = validation.freeze({**SCOPE, "scope_key": "test-conjunction"}, CANDIDATE, frozen_at_utc=START,
            acceptance_policy={**conjunction, "binding_sha256": validation.digest(validation.binding({**SCOPE, "scope_key": "test-conjunction"}, CANDIDATE))})
        self.assertFalse(run(rows, registration=configured, windows={})["standard"]["research_ready"])

    def test_legacy_or_loose_acceptance_contract_rejected(self):
        broken = policy()
        broken["binding_sha256"] = "frozen_oos_protocol_v1"
        with self.assertRaises(ValueError):
            validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START, acceptance_policy=broken)
        broken = policy()
        broken.pop("documented_source")
        with self.assertRaises(ValueError):
            validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START, acceptance_policy=broken)

    def test_multiplicity_expansion_and_entry_conflict_block_readiness(self):
        rows = [item(i) for i in range(1, 6)]
        registered = validation.freeze(SCOPE, CANDIDATE, frozen_at_utc=START, acceptance_policy=policy())
        windows = {row["event_id"]: common(row) for row in rows}
        result = run(rows, registration=registered, windows=windows, registered_attempts=2)
        self.assertIn("MULTIPLICITY_FAMILY_EXPANDED_AFTER_FREEZE", result["standard"]["blockers"])
        self.assertFalse(result["research_ready"])
        result = run(rows, registration=registered, windows=windows, representative_conflicts=["wave-1"])
        self.assertEqual(result["prospective"]["status_counts"]["DATA_MISSING"], 1)
        self.assertFalse(result["research_ready"])


if __name__ == "__main__":
    unittest.main()
