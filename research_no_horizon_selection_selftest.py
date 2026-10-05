"""Real two-window SQLite selection regressions over synthetic anchored data.

Each capture is rebuilt through the genuine archive/intake/causal-parent
fixtures. The second population uses fresh source IDs and shifted feature
timestamps; a deliberately continuing BTC parent tests cross-window overlap.
No market service or Production database is accessed.
"""
from contextlib import ExitStack
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage_selftest as coverage_fixture
import research_no_horizon_cohort_store as coordinator
import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
import research_no_horizon_selection as selection
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture, _seal_exports, _entry_time
from research_no_horizon_experiment import _scopes
from research_no_horizon_parent_coverage_selftest import scope, source_row
from research_watch_score_capture_selftest import BASE


def selection_fixture(*, parent_counts=(5, 5), directions=("SHORT", "LONG"),
                      thresholds=(.25, .2), repeat_parent=True, top_k=1,
                      required_eligible_windows=2, failing_window=None,
                      missing_window=None, gate_policy=None):
    """Return one frozen selection plan and genuine inputs for each window.

    This helper is shared with the PostgreSQL parity test. Only the coverage
    fixture's interval origin is patched; captured score template caches and
    production functions are untouched. source_row shifts scores and rebuilds
    their archive and intake identities using its parent_offset parameter.
    """
    duration = timedelta(hours=9) - timedelta(minutes=6)
    price_scopes = [scope("SHORT", threshold=threshold) for threshold in thresholds]
    sources = []
    originals = []
    for window, count in enumerate(parent_counts):
        delta = duration * window
        offset = delta.total_seconds() / timedelta(hours=2).total_seconds()
        rows = [source_row(parent_offset=offset + ordinal,
                           snapshot_set_id=window * 100 + ordinal + 1)
                for ordinal in range(count)]
        if repeat_parent and window and rows and sources[0]:
            # One still-active causal movement can span adjacent populations.
            # Its immutable start/confirmation/identity stay unchanged.
            parent = deepcopy(sources[0][-1]["btc_parent"])
            parent["observed_through_utc"] = rows[0]["btc_prior_bar"]["close_time_utc"]
            rows[0]["btc_parent"] = parent
        sources.append(rows)
        with patch.object(coverage_fixture, "BASE", BASE + delta):
            declaration, _, exports = outcome_fixture(
                [rows[:3], rows[3:]], scopes=price_scopes,
                mode="success", gate_policy=gate_policy)
        if window == failing_window:
            opened = _entry_time(rows[0]["intake"]["usable_from_utc"])
            direction = _scopes(price_scopes)[0]["analysis_direction"]
            width = max(thresholds) + .1
            for export in exports:
                for bar in export["candles"]:
                    if contracts.utc(bar["open_time_utc"]) == opened:
                        bar.update(high=100.01 if direction == "LONG" else 100. + width,
                                   low=100. - width if direction == "LONG" else 99.99)
        if window == missing_window:
            opened = _entry_time(rows[0]["intake"]["usable_from_utc"])
            for export in exports:
                export["candles"] = [bar for bar in export["candles"]
                                     if contracts.utc(bar["open_time_utc"]) != opened]
        originals.append((declaration, exports))
    first = deepcopy(originals[0][0])
    first["scopes"] = [scope(direction, threshold=threshold)
                       for direction in directions for threshold in thresholds]
    plan = selection.build_plan(first, base_directions=list(directions),
        thresholds_pct=list(thresholds), candidate_keys=["FUTURES_CVD_TOTAL_65"],
        window_count=len(parent_counts), top_k=top_k,
        required_eligible_windows=required_eligible_windows)
    windows = plan["discovery_plan"]["calendar_plan"]["windows"]
    fixtures = [_seal_exports(window["declaration"], exports)
                for window, (_, exports) in zip(windows, originals)]
    return plan, fixtures


def execute_fixture(plan, fixtures, *, pending_window=None):
    """Produce actual stored executor reports, with bounded resumable work."""
    reports = []
    for ordinal, (declaration, anchor, exports) in enumerate(fixtures):
        with coordinator.LocalCohortStore(":memory:") as store:
            plan_id = store.submit_cohort(declaration, anchor, lambda n: exports[n])
            if ordinal == pending_window:
                store.run_cohort(plan_id, "selection-fixture", scope_budget=1,
                    candle_budget=1, entry_budget=1, batch_size=1)
            else:
                for _ in range(8):
                    report = store.run_cohort(plan_id, "selection-fixture",
                        scope_budget=64, candle_budget=100000,
                        entry_budget=128, batch_size=128)
                    if report["all_scopes_processed"]:
                        break
                else:
                    raise AssertionError("bounded selection fixture did not finish")
            reports.append(store.report(plan_id))
    return reports


def rehash(value, key):
    value[key] = contracts.digest({name: item for name, item in value.items() if name != key})
    return value


class SelectionIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.good_plan, cls.good_fixtures = selection_fixture()
        cls.good = cls.good_plan, execute_fixture(cls.good_plan, cls.good_fixtures)
        cls.pending = cls.good_plan, execute_fixture(
            cls.good_plan, cls.good_fixtures, pending_window=1)
        plan, fixtures = selection_fixture(parent_counts=(3, 3))
        cls.low_n = plan, execute_fixture(plan, fixtures)
        plan, fixtures = selection_fixture(failing_window=1)
        cls.one_failure = plan, execute_fixture(plan, fixtures)
        plan, fixtures = selection_fixture(failing_window=1, required_eligible_windows=1)
        cls.one_required = plan, execute_fixture(plan, fixtures)
        plan, fixtures = selection_fixture(missing_window=1)
        cls.blocked = plan, execute_fixture(plan, fixtures)

    def select(self, name="good"):
        plan, reports = deepcopy(getattr(self, name))
        return selection.select_reports(plan, reports)

    def build(self, **changes):
        first = deepcopy(self.good_plan["template_declaration"])
        options = {"base_directions": ["SHORT", "LONG"],
                   "thresholds_pct": [.25, .2],
                   "candidate_keys": ["FUTURES_CVD_TOTAL_65"],
                   "window_count": 2, "top_k": 1,
                   "required_eligible_windows": 2}
        options.update(changes)
        return selection.build_plan(first, **options)

    def test_two_complete_windows_select_only_predeclared_top_k(self):
        result = self.select()
        self.assertTrue(result["selection_complete"])
        self.assertEqual(len(result["windows"]), 2)
        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual(len(result["ranked_scope_ids"]), 2)
        self.assertEqual(len(result["selected_scope_ids"]), 1)
        short = [row for row in result["rows"] if row["base_direction"] == "SHORT"]
        self.assertTrue(all(row["rankable"] and row["selectable"] for row in short))
        self.assertEqual(sum(row["selected"] for row in short), 1)
        self.assertTrue(all(row["eligible_window_count"] == 2 for row in short))
        self.assertEqual(result["denominator"]["declared_windows"], 2)
        self.assertEqual(result["denominator"]["complete_windows"], 2)
        self.assertEqual(result["denominator"]["declared_scopes"], 4)
        self.assertEqual(result["denominator"]["declared_scope_windows"], 8)
        self.assertEqual(result["denominator"]["selectable_scopes"], 2)
        self.assertEqual(result["denominator"]["selected_scopes"], 1)

    def test_selected_scope_definitions_are_full_versioned_normalized_candidates(self):
        result = self.select()
        declaration = self.good_plan["discovery_plan"]["calendar_plan"]["windows"][0]["declaration"]
        normalized = {item["scope_id"]: item for item in _scopes(declaration["scopes"])}
        self.assertEqual(result["selected_scopes"],
                         [normalized[key] for key in result["selected_scope_ids"]])
        rows = {item["scope_id"]: item for item in result["rows"]}
        for key in result["selected_scope_ids"]:
            self.assertEqual(rows[key]["candidate_version"],
                             normalized[key]["candidate"]["definition_sha256"])

    def test_equal_window_statistics_use_stable_scope_identity_tie_break(self):
        result = self.select()
        expected = sorted(row["scope_id"] for row in result["rows"] if row["rankable"])
        self.assertEqual(result["ranked_scope_ids"], expected)
        self.assertEqual(result["selected_scope_ids"], expected[:1])
        self.assertEqual(result, self.select())

    def test_empty_scopes_remain_in_every_window_and_selection_denominator(self):
        result = self.select()
        empty = [row for row in result["rows"] if row["base_direction"] == "LONG"]
        self.assertEqual(len(empty), 2)
        for row in empty:
            self.assertFalse(row["rankable"])
            self.assertFalse(row["selectable"])
            self.assertFalse(row["selected"])
            self.assertIsNone(row["rank"])
            self.assertEqual(row["eligible_window_count"], 0)
            self.assertEqual(len(row["window_statuses"]), 2)
            self.assertEqual(row["known_matched_parent_ids"], [])
        self.assertEqual(result["denominator"]["unranked_scopes"], 2)
        self.assertEqual(result["denominator"]["unselected_scopes"], 3)

    def test_window_metrics_are_kept_separate_and_selection_uses_the_worst_window(self):
        result = self.select("one_failure")
        self.assertTrue(result["selection_complete"])
        short = [row for row in result["rows"] if row["base_direction"] == "SHORT"]
        self.assertEqual(len(short), 2)
        for row in short:
            self.assertTrue(row["rankable"])
            self.assertEqual(row["eligible_window_count"], 1)
            self.assertFalse(row["selectable"])
            self.assertFalse(row["selected"])
            per_window = [next(item for item in window["rows"] if item["scope_id"] == row["scope_id"])
                          for window in result["windows"]]
            self.assertEqual([item["gate"]["successes"] for item in per_window], [5, 4])
            self.assertEqual([item["gate"]["resolved_parents"] for item in per_window], [5, 5])
            self.assertEqual(row["metrics"]["minimum_hit_rate_pct"], 80)
            self.assertEqual(row["metrics"]["minimum_resolved_parents"], 5)
            self.assertEqual(row["metrics"]["minimum_wilson_95_lower_pct"],
                min(item["gate"]["probability"]["wilson_95_lower_pct"] for item in per_window))
        self.assertEqual(result["selected_scope_ids"], [])

    def test_one_required_window_selects_without_changing_the_other_windows_failed_gate(self):
        result = self.select("one_required")
        self.assertTrue(result["selection_complete"])
        self.assertEqual(len(result["selected_scope_ids"]), 1)
        chosen = next(row for row in result["rows"] if row["selected"])
        self.assertEqual(chosen["eligible_window_count"], 1)
        self.assertEqual(chosen["required_eligible_windows"], 1)
        self.assertTrue(chosen["selectable"])
        gates = [next(row["gate"] for row in window["rows"]
                      if row["scope_id"] == chosen["scope_id"]) for window in result["windows"]]
        self.assertEqual([item["experimental_eligible"] for item in gates], [True, False])
        self.assertEqual([item["resolved_parents"] for item in gates], [5, 5])
        self.assertEqual([item["successes"] for item in gates], [5, 4])
        self.assertEqual(chosen["metrics"]["minimum_hit_rate_pct"], 80)

    def test_three_plus_three_resolved_parents_never_become_a_six_parent_gate(self):
        result = self.select("low_n")
        self.assertTrue(result["selection_complete"])
        self.assertEqual(result["matched_parent_overlap"]["distinct_matched_parent_count"], 5)
        for row in result["rows"]:
            if row["base_direction"] == "SHORT":
                self.assertTrue(row["rankable"])
                self.assertEqual(row["metrics"]["minimum_resolved_parents"], 3)
                self.assertEqual(row["metrics"]["minimum_hit_rate_pct"], 100)
                self.assertEqual(row["eligible_window_count"], 0)
                self.assertFalse(row["selectable"])
                self.assertFalse(row["selected"])
        self.assertEqual(result["selected_scope_ids"], [])
        self.assertEqual(result["denominator"]["selectable_scopes"], 0)

    def test_continuing_parent_is_reported_once_in_union_and_twice_in_window_membership(self):
        result = self.select()
        overlap = result["matched_parent_overlap"]
        self.assertEqual(overlap["basis"], "ALL_MATCHED_DECISIONS_ONLY")
        self.assertEqual(overlap["distinct_matched_parent_count"], 9)
        self.assertEqual(overlap["repeated_parent_count"], 1)
        self.assertFalse(overlap["cross_window_parent_independence_proven"])
        self.assertEqual(len(overlap["overlapping_parents"]), 1)
        repeated = overlap["overlapping_parents"][0]
        self.assertEqual(repeated["window_ordinals"], [0, 1])
        self.assertEqual(len(result["known_matched_parent_ids"]), 9)
        self.assertEqual(result["known_matched_parent_ids"], sorted(result["known_matched_parent_ids"]))
        for row in result["rows"]:
            if row["base_direction"] == "SHORT":
                self.assertEqual(row["matched_parent_overlap"]["repeated_parent_count"], 1)
                self.assertEqual(len(row["known_matched_parent_ids"]), 9)

    def test_partial_second_window_suppresses_global_ranks_and_selection(self):
        result = self.select("pending")
        self.assertFalse(result["selection_complete"])
        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual(len(result["windows"]), 2)
        self.assertEqual(result["ranked_scope_ids"], [])
        self.assertEqual(result["selected_scope_ids"], [])
        self.assertEqual(result["denominator"]["complete_windows"], 1)
        self.assertEqual(result["matched_parent_overlap"]["distinct_matched_parent_count"], 9)
        self.assertEqual(result["matched_parent_overlap"]["repeated_parent_count"], 1)
        self.assertTrue(all(row["rank"] is None and not row["selectable"] and not row["selected"]
                            for row in result["rows"]))

    def test_input_blocked_window_stays_visible_and_cannot_be_selected(self):
        result = self.select("blocked")
        self.assertTrue(result["selection_complete"])
        self.assertEqual(result["denominator"]["declared_scope_windows"], 8)
        self.assertEqual(result["selected_scope_ids"], [])
        for row in result["rows"]:
            if row["base_direction"] == "SHORT":
                self.assertFalse(row["rankable"])
                self.assertFalse(row["selectable"])
                self.assertIsNone(row["rank"])
                self.assertEqual(row["window_statuses"][1]["status"], "INPUT_BLOCKED")

    def test_missing_duplicate_or_reordered_window_reports_are_rejected(self):
        for mode in ("missing", "duplicate", "reverse", "extra", "tuple"):
            with self.subTest(mode=mode):
                plan, reports = deepcopy(self.good)
                if mode == "missing":
                    reports.pop()
                elif mode == "duplicate":
                    reports[1] = deepcopy(reports[0])
                elif mode == "reverse":
                    reports.reverse()
                elif mode == "extra":
                    reports.append(deepcopy(reports[-1]))
                else:
                    reports = tuple(reports)
                with self.assertRaises(ValueError):
                    selection.select_reports(plan, reports)

    def test_rehashed_executor_probability_forgery_is_recomputed_not_trusted(self):
        plan, reports = deepcopy(self.good)
        row = next(item for item in reports[1]["scopes"] if item["base_direction"] == "SHORT")
        row["gate"]["probability"]["wilson_95_lower_pct"] += 1
        rehash(reports[1], "report_sha256")
        with self.assertRaises(ValueError):
            selection.select_reports(plan, reports)

    def test_rehashed_dropped_scope_is_not_hidden_by_cross_window_selection(self):
        plan, reports = deepcopy(self.good)
        reports[1]["scopes"].pop()
        reports[1]["declared_scopes"] -= 1
        rehash(reports[1], "report_sha256")
        with self.assertRaises(ValueError):
            selection.select_reports(plan, reports)

    def test_selection_does_not_alias_or_modify_plans_reports_or_window_metrics(self):
        plan, reports = deepcopy(self.good)
        original = deepcopy((plan, reports))
        with patch("sqlite3.connect", side_effect=AssertionError("selection cannot open a database")):
            result = selection.select_reports(plan, reports)
        self.assertEqual((plan, reports), original)
        result["rows"][0]["known_matched_parent_ids"].append("caller-change")
        result["windows"][0]["rows"][0]["representatives"].clear()
        self.assertEqual((plan, reports), original)

    def test_result_hash_and_disabled_authority_are_explicit(self):
        result = self.select()
        self.assertEqual(result["plan_sha256"], self.good_plan["plan_sha256"])
        self.assertIs(result["policy_registration_verified"], False)
        self.assertIs(result["source_provenance_verified_by_this_tool"], False)
        self.assertEqual(result["report_sha256"], contracts.digest({
            key: value for key, value in result.items() if key != "report_sha256"}))
        for flag in ("runtime_authorized", "telegram_authorized", "trading_authorized",
                     "validated_discovery", "is_prospective_formula_evidence",
                     "multiple_testing_adjusted", "scope_pooling", "window_pooling",
                     "cross_window_outcome_pooling", "cross_window_parent_independence_proven"):
            self.assertIs(result[flag], False, flag)

    def test_planning_is_deterministic_outcome_free_and_database_free(self):
        original = deepcopy(self.good_plan["template_declaration"])
        with ExitStack() as stack:
            stack.enter_context(patch("sqlite3.connect", side_effect=AssertionError("no planner database")))
            stack.enter_context(patch("research_no_horizon_ranking.rank_report",
                                      side_effect=AssertionError("no planner outcomes")))
            stack.enter_context(patch("socket.create_connection", side_effect=AssertionError("no planner network")))
            plan = self.build()
            self.assertEqual(selection.validate_plan(plan), plan)
            self.assertEqual(self.build(), plan)
        self.assertEqual(self.good_plan["template_declaration"], original)

    def test_selection_parameters_bind_every_window_declaration_before_registration(self):
        reference = self.build()
        baseline = reference["discovery_plan"]["calendar_plan"]["windows"]
        for changes in ({"top_k": 2}, {"required_eligible_windows": 1}, {"window_count": 3}):
            with self.subTest(changes=changes):
                changed = self.build(**changes)
                self.assertNotEqual(changed["plan_sha256"], reference["plan_sha256"])
                windows = changed["discovery_plan"]["calendar_plan"]["windows"]
                for left, right in zip(baseline, windows):
                    self.assertNotEqual(left["declaration_sha256"], right["declaration_sha256"])
                    self.assertNotEqual(left["declaration"]["cohort_key"], right["declaration"]["cohort_key"])

    def test_invalid_selection_bounds_and_boolean_integers_reject(self):
        for field, values in (
                ("top_k", (0, -1, True, 1.5, "1", 5)),
                ("required_eligible_windows", (0, -1, True, 1.5, "1", 3)),
                ("window_count", (0, True, 1.5, "2", 33))):
            for value in values:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        self.build(**{field: value})

    def test_omitted_required_selection_policy_cannot_get_implicit_defaults(self):
        first = deepcopy(self.good_plan["template_declaration"])
        common = {"base_directions": ["SHORT"], "thresholds_pct": [.25],
                  "candidate_keys": ["FUTURES_CVD_TOTAL_65"], "window_count": 2}
        for options in (common, {**common, "top_k": 1},
                        {**common, "required_eligible_windows": 2}):
            with self.subTest(keys=sorted(options)):
                with self.assertRaises(TypeError):
                    selection.build_plan(first, **options)

    def test_custom_versioned_atomic_gate_survives_selection_planning(self):
        first = deepcopy(self.good_plan["template_declaration"])
        custom = gate.make_policy(policy_version="selection-fixture-six-parent-v1", minimum_waves=6)
        first["gate_policy"] = custom
        plan = selection.build_plan(first, base_directions=["SHORT"],
            thresholds_pct=[.25], candidate_keys=["FUTURES_CVD_TOTAL_65"],
            window_count=2, top_k=1, required_eligible_windows=2)
        for window in plan["discovery_plan"]["calendar_plan"]["windows"]:
            self.assertEqual(window["declaration"]["gate_policy"], custom)
        self.assertEqual(selection.validate_plan(plan), plan)

    def test_unknown_or_rehashed_mutated_policy_and_implementation_reject(self):
        for mutation in ("unknown", "policy", "implementation", "window"):
            with self.subTest(mutation=mutation):
                plan = deepcopy(self.good_plan)
                if mutation == "unknown":
                    plan["allow_pooling"] = True
                elif mutation == "policy":
                    plan["policy"]["top_k"] = 2
                elif mutation == "implementation":
                    plan["implementation"]["unrecognized_override"] = True
                else:
                    plan["discovery_plan"]["calendar_plan"]["windows"][1]["declaration"]["cohort_key"] = "changed"
                rehash(plan, "plan_sha256")
                with self.assertRaises(ValueError):
                    selection.validate_plan(plan)

    def test_different_valid_selection_policy_cannot_reuse_already_frozen_reports(self):
        _, reports = deepcopy(self.good)
        changed = self.build(top_k=2)
        with self.assertRaises(ValueError):
            selection.select_reports(changed, reports)


if __name__ == "__main__":
    unittest.main()
