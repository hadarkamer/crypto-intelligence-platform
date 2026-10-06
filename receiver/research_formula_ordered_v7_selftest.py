"""Network-free regressions for ordered v7 formula evidence and anti-leakage."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

import research_formula_ordered_v7 as formulas
import research_ordered_first_touch as ordered


START = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
AS_OF = START + timedelta(days=1)
CANDIDATE = {
    "formula_id": "PRICE_OI_65",
    "conditions": [{"feature": "price_oi.aligned_score", "operator": ">=", "value": 65}],
}


def row(
    event_id=1, *, wave="wave-1", start=START, success=True, symbol="BTCUSDT",
    direction="LONG", threshold=50, horizon=60,
):
    shift = threshold / 100.0 + 0.01
    up = success == (direction == "LONG")
    path = [{
        "open_time_utc": start,
        "close_time_utc": start + timedelta(seconds=59, milliseconds=999),
        "open": 100.0, "high": 100 + (shift if up else 0.01),
        "low": 100 - (0.01 if up else shift), "close": 100.0,
    }]
    label = ordered.calculate_ordered_first_touch_outcome(
        reference_price=100, direction=direction, event_time=start,
        candles=path, threshold_pct=threshold / 100,
        observation_closed=False, path_complete=True,
    )
    return {
        **label, "event_id": event_id, "symbol": symbol, "entry_price": 100.0,
        "alert_time_utc": start, "window_minutes": horizon,
        "btc_parent_movement_id": wave,
        "episode_policy_version": "btc-parent-test-v1",
        "membership_status": "LIVE", "parent_evidence_eligible": True,
        "decision_features": {"price_oi.aligned_score": 70},
        "features_observed_at_utc": start,
        "data_quality_status": "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
    }


def nondecisive_row(event_id=1, *, status="OPEN", wave="wave-1", start=START, symbol="BTCUSDT"):
    path = [{
        "open_time_utc": start + timedelta(minutes=i),
        "close_time_utc": start + timedelta(minutes=i, seconds=59, milliseconds=999),
        "open": 100.0, "close": 100.0,
        "high": 101.0 if status == "AMBIGUOUS" else 100.01,
        "low": 99.0 if status == "AMBIGUOUS" else 99.99,
    } for i in range(60 if status == "NO_TOUCH" else 1)]
    label = ordered.calculate_ordered_first_touch_outcome(
        reference_price=100, direction="LONG", event_time=start,
        candles=path, threshold_pct=0.5,
        observation_closed=status == "NO_TOUCH", path_complete=status != "DATA_MISSING",
    )
    return {**row(event_id, wave=wave, start=start, symbol=symbol), **label}


def cell(result, *, symbol="ALL", direction="LONG", horizon=60, threshold=50):
    return next(item for item in result["formulas"][0]["cells"] if (
        item["symbol"], item["direction"], item["window_minutes"], item["threshold_bps"]
    ) == (symbol, direction, horizon, threshold))


def evaluate(rows, candidate=CANDIDATE):
    return formulas.evaluate_formulas(rows, [candidate], analysis_as_of_utc=AS_OF)


class OrderedEvidenceTests(unittest.TestCase):
    def test_self_consistent_label_cannot_change_original_entry(self):
        source=row()
        for key in ('reference_price','favorable_barrier_price','adverse_barrier_price','favorable_touch_price'):
            source[key] *= 2
        audit=formulas.ordered_outcome_evidence(source,analysis_as_of_utc=AS_OF)
        self.assertFalse(audit['eligible'])
        self.assertIn('REFERENCE_PRICE_DIFFERS_FROM_ORIGINAL_ENTRY',audit['exclusion_reasons'])
        missing=row();missing.pop('entry_price')
        self.assertIn('MISSING_ORIGINAL_ENTRY_PRICE',formulas.ordered_outcome_evidence(missing,analysis_as_of_utc=AS_OF)['exclusion_reasons'])
        original=row();wrong={**original,'ordered_outcome':{**original,'event_id':99}}
        self.assertIn('OUTCOME_EVENT_ID_MISMATCH',formulas.ordered_outcome_evidence(wrong,analysis_as_of_utc=AS_OF)['exclusion_reasons'])
        # An explicit real derived inverse identity is allowed; its source ID
        # remains unchanged for the BTC cohort. Direction and price gates still apply.
        mapped={**wrong,'outcome_event_id':99}
        self.assertTrue(formulas.ordered_outcome_evidence(mapped,analysis_as_of_utc=AS_OF)['eligible'])
    def test_real_calculator_labels_and_fail_closed_audit(self):
        for direction in formulas.DIRECTIONS:
            for success in (True, False):
                item = row(direction=direction, success=success)
                audit = formulas.ordered_outcome_evidence(item, analysis_as_of_utc=AS_OF)
                self.assertTrue(audit["eligible"], audit)
                self.assertIs(audit["success"], success)
        for update in (
            {"method_version": "first-touch-v6"},
            {"status": "UNRESOLVED", "first_touch_side": "AMBIGUOUS", "success": None},
            {"status": "UNRESOLVED", "first_touch_side": "NONE", "success": None},
            {"status": "OPEN", "success": None},
            {"status": "DATA_MISSING", "path_complete": False},
            {"decision_time_utc": None},
            {"adverse_touch_price": None},
            {"first_touch_side": "FAVORABLE"},
            {"time_to_decision_seconds": 0},
            {"initial_gap_unobserved": True},
            {"observed_through_utc": AS_OF + timedelta(days=1)},
            {"measurement_start_utc": START - timedelta(minutes=1)},
            {"threshold_bps": True},
            {"data_quality_status": "PARTIAL_BINANCE_SPOT_1M_CLOSED_CANDLES"},
            {"data_quality_status": None},
        ):
            item = {**row(success=False), **update}
            self.assertFalse(formulas.ordered_outcome_evidence(item, analysis_as_of_utc=AS_OF)["eligible"], update)

    def test_all_thresholds_and_horizons_remain_separate(self):
        rows = [row(direction=d, event_id=1 if d == "LONG" else 2, threshold=t, horizon=h)
                for d in formulas.DIRECTIONS for t in formulas.THRESHOLDS_BPS for h in formulas.HORIZONS_MINUTES]
        result = evaluate(rows)
        all_cells = [item for item in result["formulas"][0]["cells"] if item["symbol"] == "ALL"]
        self.assertEqual(len(all_cells), 64)
        self.assertTrue(all(item["sample_size"] == 1 for item in all_cells))
        self.assertTrue(all(not item["count_eligible"] for item in all_cells))

    def test_same_wave_never_repeats_independent_proof(self):
        result = evaluate([row(i + 1, start=START + timedelta(minutes=i * 30)) for i in range(5)])
        self.assertEqual(cell(result)["sample_size"], 1)
        self.assertEqual(cell(result)["evidence_event_ids"], [1])

    def test_earliest_failure_cannot_be_replaced_by_later_win(self):
        result = evaluate([row(success=False), row(2, start=START + timedelta(minutes=30))])
        self.assertEqual(cell(result)["failures"], 1)
        self.assertEqual(cell(result)["successes"], 0)

    def test_pending_earliest_cohort_cannot_disappear(self):
        pending = nondecisive_row()
        result = evaluate([pending, row(2, start=START + timedelta(minutes=30))])
        self.assertEqual(cell(result)["sample_size"], 0)
        self.assertEqual(cell(result)["excluded_waves"], 1)
        self.assertEqual(cell(result)["open_waves"], 1)
        self.assertEqual(cell(result)["data_missing_waves"], 0)

    def test_every_nondecisive_status_has_its_own_wave_count(self):
        rows = [row(1, wave="success"), row(2, wave="failure", success=False)]
        rows += [nondecisive_row(i + 3, status=status, wave=status)
                 for i, status in enumerate(("OPEN", "AMBIGUOUS", "NO_TOUCH", "DATA_MISSING"))]
        for result in (
            formulas.summarize_scope(rows, analysis_as_of_utc=AS_OF),
            cell(evaluate(rows)),
        ):
            self.assertEqual(result["status_counts"], dict.fromkeys(formulas.WAVE_STATUSES, 1))
            self.assertEqual(result["decision_waves"], 6)
            self.assertEqual(result["excluded_waves"], 4)
            self.assertEqual(result["sample_size"], 2)
            self.assertEqual(result["hit_rate_pct"], 50)
            self.assertEqual(result["open_waves"], 1)
        incremental = formulas.summarize_scope(rows, analysis_as_of_utc=AS_OF)
        ambiguous = next(item for item in incremental["episodes"] if item["status"] == "AMBIGUOUS")
        self.assertFalse(ambiguous["eligible"])
        self.assertEqual(ambiguous["representative_outcome_statuses"][0]["source_status"], "UNRESOLVED")

    def test_missing_or_invalid_labels_are_never_open(self):
        malformed = [
            {**nondecisive_row(), "path_complete": False},
            {**nondecisive_row(), "data_quality_status": None},
            {**nondecisive_row(), "method_version": "first-touch-v6"},
            {**nondecisive_row(), "first_touch_side": "FAVORABLE"},
            {**nondecisive_row(), "observation_closed": True},
            {**row(), "decision_time_utc": None},
        ]
        for item in malformed:
            result = formulas.summarize_scope([item], analysis_as_of_utc=AS_OF)
            self.assertEqual(result["data_missing_waves"], 1, item)
            self.assertEqual(result["open_waves"], 0, item)
            self.assertEqual(result["sample_size"], 0, item)
        missing_cell = cell(evaluate([row()]), threshold=75)
        self.assertEqual(missing_cell["data_missing_waves"], 1)
        self.assertEqual(missing_cell["open_waves"], 0)

    def test_mixed_cohort_keeps_member_statuses_without_extra_votes(self):
        rows = [row(), nondecisive_row(2, symbol="ETHUSDT"),
                nondecisive_row(3, symbol="SOLUSDT", status="DATA_MISSING")]
        result = formulas.summarize_scope(rows + [rows[0]], analysis_as_of_utc=AS_OF)
        self.assertEqual(result["decision_waves"], 1)
        self.assertEqual(result["status_counts"]["DATA_MISSING"], 1)
        self.assertEqual(result["open_waves"], 0)
        self.assertEqual(result["sample_size"], 0)
        self.assertEqual(len(result["episodes"][0]["representative_outcome_statuses"]), 3)

    def test_fresh_status_counts_do_not_include_old_open_waves(self):
        rows = [row(i + 1, wave=f"recent-{i}", start=START - timedelta(days=i)) for i in range(3)]
        rows += [nondecisive_row(10, wave="old-open", start=START - timedelta(days=20)),
                 nondecisive_row(11, wave="recent-open")]
        result = formulas.summarize_scope(rows, analysis_as_of_utc=AS_OF)
        self.assertEqual(result["count_route"], "FRESH")
        self.assertEqual(result["open_waves"], 1)
        self.assertEqual(result["all_wave_status_counts"]["OPEN"], 2)
        self.assertEqual(result["status_count_scope"], "FRESH_14D_SELECTED_WAVES")

    def test_simultaneous_coins_one_conservative_global_vote(self):
        result = evaluate([row(), row(2, symbol="ETHUSDT", success=False)])
        self.assertEqual(cell(result)["sample_size"], 1)
        self.assertEqual(cell(result)["failures"], 1)
        self.assertEqual(cell(result, symbol="BTCUSDT")["successes"], 1)
        self.assertEqual(cell(result, symbol="ETHUSDT")["failures"], 1)

    def test_repeat_starts_at_third_distinct_alert(self):
        candidate = {**CANDIDATE, "repeat_count": 3}
        rows = [row(i + 1, start=START + timedelta(minutes=i * 30), success=i == 2) for i in range(3)]
        result = evaluate(rows, candidate)
        self.assertEqual(cell(result)["successes"], 1)
        self.assertEqual(cell(result)["evidence_event_ids"], [3])
        rows[2]["measurement_start_utc"] = START
        self.assertEqual(cell(evaluate(rows, candidate))["sample_size"], 0)
        self.assertEqual(cell(evaluate([row(), row(2), row(3)], candidate))["sample_size"], 0)

    def test_five_wave_gate_and_fresh_three_in_fourteen_days(self):
        recent = [row(i + 1, wave=f"recent-{i}", start=START - timedelta(days=i)) for i in range(3)]
        older = [row(10 + i, wave=f"old-{i}", start=START - timedelta(days=20 + i), success=False) for i in range(2)]
        fresh = cell(evaluate(recent + older[:1]))
        self.assertEqual(fresh["count_route"], "FRESH")
        self.assertEqual(fresh["sample_size"], 3)
        self.assertEqual(fresh["hit_rate_pct"], 100)
        self.assertFalse(fresh["research_ready"])
        standard = cell(evaluate(recent + older))
        self.assertEqual(standard["count_route"], "STANDARD")
        self.assertEqual(standard["sample_size"], 5)
        self.assertEqual(standard["hit_rate_pct"], 60)

    def test_future_features_and_unverified_parent_never_enter_sample(self):
        for update in (
            {"features_observed_at_utc": START + timedelta(seconds=1)},
            {"membership_status": "BOUNDARY_UNVERIFIED"},
            {"parent_evidence_eligible": False},
            {"btc_parent_movement_id": None},
        ):
            self.assertEqual(cell(evaluate([{**row(), **update}]))["sample_size"], 0)
        with self.assertRaises(ValueError):
            evaluate([row()], {"conditions": [{"feature": "model.mfe_pct", "operator": ">=", "value": 0}]})

    def test_conflicting_same_event_and_scope_mixing_are_rejected(self):
        item = row()
        conflict = deepcopy(item)
        conflict["success"] = False
        self.assertEqual(cell(evaluate([item, conflict]))["sample_size"], 0)
        with self.assertRaises(ValueError):
            formulas.summarize_scope([row(), row(2, threshold=75)], analysis_as_of_utc=AS_OF)
        with self.assertRaises(ValueError):
            formulas.summarize_scope([row(), {**row(2), "episode_policy_version": "another-policy"}], analysis_as_of_utc=AS_OF)

    def test_incremental_scope_preserves_missing_and_truncated_evidence(self):
        rows = [row(i + 1, wave=f"wave-{i}") for i in range(5)]
        result = formulas.summarize_scope(rows, analysis_as_of_utc=AS_OF)
        self.assertEqual(result["count_route"], "STANDARD")
        self.assertEqual(len(result["episodes"]), 5)
        self.assertEqual(result["metric_scope"], "STOP_AT_FIRST_TOUCH_V7")
        truncated = formulas.summarize_scope(rows, analysis_as_of_utc=AS_OF, truncated=True)
        self.assertFalse(truncated["count_eligible"])
        self.assertEqual(truncated["count_route"], "TRUNCATED_EVIDENCE")
        pending = {**row(8, wave="wave-8"), "method_version": None, "status": None}
        result = formulas.summarize_scope(rows + [pending], analysis_as_of_utc=AS_OF)
        self.assertEqual(result["decision_waves"], 6)
        self.assertEqual(result["excluded_waves"], 1)

    def test_catalog_alignment_and_versioned_identity(self):
        event = {
            "direction": "LONG", "symbol": "BTCUSDT",
            "engine_snapshot": {"market_evidence": {"modules": {
                "positioning": {"score": 75, "direction": "LONG"},
                "futures_flow": {"score": 75, "direction": "LONG"},
                "spot_flow": {"score": -75, "direction": "SHORT"},
            }}},
        }
        features = formulas.extract_event_features(event)
        catalog = formulas.candidate_catalog()
        self.assertEqual(len(catalog), 7)
        triple = catalog[-1]
        self.assertEqual(triple["formula_id"], "STRICT_TRIPLE_TOTAL_65")
        self.assertFalse(formulas.matches(triple, features, "LONG"))
        features["spot_cvd.aligned_score"] = 75
        self.assertTrue(formulas.matches(triple, features, "LONG"))
        key = evaluate([row()])["formulas"][0]["formula_key"]
        self.assertNotEqual(key, evaluate([row()], {**CANDIDATE, "catalog_version": "new"})["formulas"][0]["formula_key"])
        self.assertNotEqual(key, evaluate([row()], {**CANDIDATE, "frozen_at_utc": START})["formulas"][0]["formula_key"])
        self.assertNotEqual(key, evaluate([{**row(), "episode_policy_version": "new"}])["formulas"][0]["formula_key"])

    def test_missing_earlier_membership_blocks_usable_counts_and_rates(self):
        known = [row(i + 1, wave=f"known-{i}") for i in range(5)]
        missing = {
            **row(20, start=START - timedelta(minutes=30)),
            "btc_parent_movement_id": None, "membership_status": "BTC_DATA_MISSING",
        }
        result = cell(evaluate([missing, *known]))
        self.assertEqual(result["independent_waves"], 5)
        self.assertFalse(result["count_eligible"])
        self.assertEqual(result["count_route"], "INCOMPLETE_MEMBERSHIP_COVERAGE")
        self.assertEqual(result["diagnostic_hit_rate_pct"], 100)
        self.assertIsNone(result["hit_rate_pct"])
        self.assertEqual(result["usable_sample_size"], 0)
        # The initial, explicitly unverified boundary is outside the defined
        # policy's evidence period, unlike a missing mapping in observed data.
        warmup = {**missing, "membership_status": "BOUNDARY_UNVERIFIED"}
        self.assertTrue(cell(evaluate([warmup, *known]))["count_eligible"])
        result = formulas.summarize_scope(known, analysis_as_of_utc=AS_OF, source_coverage_complete=False)
        self.assertFalse(result["count_eligible"])
        self.assertIsNone(result["hit_rate_pct"])
        result = formulas.summarize_scope([missing, *known], analysis_as_of_utc=AS_OF)
        self.assertFalse(result["source_coverage_complete"])


if __name__ == "__main__":
    unittest.main()
