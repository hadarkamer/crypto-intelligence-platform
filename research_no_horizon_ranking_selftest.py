"""Rank actual SQLite receipts without hiding failed, empty or unresolved scopes.

All captures, parent groups and minute prices are synthetic. Common-anchor
proofs are regenerated for the discovery calendar's exact declaration; no
historical market outcomes or external services are accessed by this suite.
"""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

import research_no_horizon_cohort_store as coordinator
import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
import research_no_horizon_discovery as discovery
import research_no_horizon_ranking as ranking
from research_no_horizon_cohort_coverage_selftest import cross_part_rows
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture, _seal_exports, _entry_time
from research_no_horizon_parent_coverage_selftest import scope
from research_no_horizon_experiment import _scopes


def rehash_report(report):
    report["report_sha256"] = contracts.digest({
        key: value for key, value in report.items() if key != "report_sha256"})
    return report


def rebind_coverage(report):
    """Remove mere outer-hash failures from deliberate semantic mutations."""
    receipt = report["coverage_receipt"]
    receipt["receipt_sha256"] = contracts.digest({
        key: value for key, value in receipt.items() if key != "receipt_sha256"})
    report["plan_identity"]["coverage_receipt_sha256"] = receipt["receipt_sha256"]
    report["plan_id"] = contracts.digest(report["plan_identity"])
    return rehash_report(report)


def case(*, mode="success", rows=None, directions=("SHORT",), thresholds=(.25,),
         missing=None, pending=False, window_count=1, wide_open=False, gate_policy=None):
    scopes = [scope(direction=direction, threshold=threshold)
              for direction in directions for threshold in thresholds]
    # Only SHORT matches these synthetic captures. Keep its actual prices when
    # adding the LONG no-match control; normalized scope ordering is unrelated
    # to the direction whose outcomes this fixture is intended to exercise.
    price_scopes = [scope(direction="SHORT", threshold=threshold) for threshold in thresholds]
    declaration, _, exports = outcome_fixture(rows, scopes=price_scopes, mode=mode,
                                              gate_policy=gate_policy)
    declaration["scopes"] = scopes
    if wide_open:
        direction = _scopes(declaration["scopes"])[0]["analysis_direction"]
        later_entries = {_entry_time(row["intake"]["usable_from_utc"])
                         for export in exports for row in export["source_rows"]
                         if row["intake"]["snapshot_set_id"] in (5, 6)}
        for export in exports:
            for bar in export["candles"]:
                if contracts.utc(bar["open_time_utc"]) in later_entries:
                    bar.update(high=100.3 if direction == "LONG" else 100.01,
                               low=99.99 if direction == "LONG" else 99.7)
    if missing is not None:
        selected = exports[0]["source_rows"][2 if missing == "entry" else 0]
        opened = _entry_time(selected["intake"]["usable_from_utc"])
        if missing == "gap":
            opened += timedelta(minutes=2)
        for export in exports:
            export["candles"] = [bar for bar in export["candles"]
                                 if contracts.utc(bar["open_time_utc"]) != opened]
    search = discovery.build_plan(declaration, base_directions=list(directions),
        thresholds_pct=list(thresholds), candidate_keys=["FUTURES_CVD_TOTAL_65"],
        window_count=window_count)
    frozen = search["calendar_plan"]["windows"][0]["declaration"]
    frozen, anchor, exports = _seal_exports(frozen, exports)
    with coordinator.LocalCohortStore(":memory:") as store:
        plan_id = store.submit_cohort(frozen, anchor, lambda ordinal: exports[ordinal])
        if pending:
            store.run_cohort(plan_id, "ranking-fixture", scope_budget=1,
                             candle_budget=1, entry_budget=1, batch_size=1)
        else:
            for _ in range(8):
                result = store.run_cohort(plan_id, "ranking-fixture", scope_budget=64,
                    candle_budget=100000, entry_budget=128, batch_size=128)
                if result["all_scopes_processed"]:
                    break
            else:
                raise AssertionError("bounded ranking fixture did not finish")
        return search, store.report(plan_id)


class RankingIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.good = case(directions=("SHORT", "LONG"), thresholds=(.25, .2))
        cls.mixed = case(mode="mixed")
        cls.comparison = case(thresholds=(.25, .4), wide_open=True)
        cls.custom_policy = case(gate_policy=gate.make_policy(
            policy_version="ranking-fixture-six-parent-v1", minimum_waves=6))
        low_rows = cross_part_rows()
        low_rows[1] = []
        cls.low_n = case(rows=low_rows)
        cls.open = case(mode="open")
        cls.ambiguous = case(mode="ambiguous")
        cls.missing = case(mode="mixed", directions=("SHORT", "LONG"), missing="entry")
        cls.gap = case(mode="open", missing="gap")
        cls.pending = case(thresholds=(.25, .2), pending=True, window_count=2)

    def rank(self, name="good", **kwargs):
        plan, report = deepcopy(getattr(self, name))
        return ranking.rank_report(plan, report, **kwargs)

    def corrupt(self, mutate, name="good", *, coverage=False):
        plan, report = deepcopy(getattr(self, name))
        mutate(report)
        (rebind_coverage if coverage else rehash_report)(report)
        with self.assertRaises(ValueError):
            ranking.rank_report(plan, report)

    def test_complete_report_ranks_only_resolved_scopes_and_retains_empty_ones(self):
        result = self.rank()
        self.assertTrue(result["global_ranking_complete"])
        self.assertEqual(result["declared_scope_count"], 4)
        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual(len(result["ranked_scope_ids"]), 2)
        short = [row for row in result["rows"] if row["base_direction"] == "SHORT"]
        empty = [row for row in result["rows"] if row["base_direction"] == "LONG"]
        self.assertEqual(sorted(row["rank"] for row in short), [1, 2])
        self.assertTrue(all(row["experimental_eligible"] for row in short))
        for row in empty:
            self.assertFalse(row["rankable"])
            self.assertIsNone(row["rank"])
            self.assertFalse(row["experimental_eligible"])
            self.assertEqual(row["gate"]["resolved_parents"], 0)
            self.assertEqual(row["representatives"], [])
        denominator = result["denominator"]
        self.assertEqual(denominator["declared_scopes"], 4)
        self.assertEqual(denominator["processed_scopes"], 4)
        self.assertEqual(denominator["ranked_scopes"], 2)
        self.assertEqual(denominator["unranked_scopes"], 2)

    def test_same_population_orders_five_resolved_above_three_without_pooling(self):
        result = self.rank("comparison")
        rows = {row["threshold_pct"]: row for row in result["rows"]}
        self.assertEqual(rows[.25]["gate"]["resolved_parents"], 5)
        self.assertEqual(rows[.4]["gate"]["resolved_parents"], 3)
        self.assertEqual(rows[.4]["outcome_status_counts"]["OPEN"], 2)
        self.assertEqual(rows[.25]["gate"]["probability"]["hit_rate_pct"], 100)
        self.assertEqual(rows[.4]["gate"]["probability"]["hit_rate_pct"], 100)
        self.assertGreater(rows[.25]["gate"]["probability"]["wilson_95_lower_pct"],
                           rows[.4]["gate"]["probability"]["wilson_95_lower_pct"])
        self.assertEqual(rows[.25]["rank"], 1)
        self.assertEqual(rows[.4]["rank"], 2)
        self.assertTrue(rows[.25]["experimental_eligible"])
        self.assertFalse(rows[.4]["experimental_eligible"])
        self.assertEqual(result["denominator"]["declared_scopes"], 2)
        self.assertFalse(result["scope_pooling"])

    def test_explicit_versioned_gate_policy_is_preserved_without_silent_defaulting(self):
        plan, report = deepcopy(self.custom_policy)
        result = ranking.rank_report(plan, report)
        row = result["rows"][0]
        declared_policy = plan["calendar_plan"]["windows"][0]["declaration"]["gate_policy"]
        self.assertEqual(row["gate"]["policy"], declared_policy)
        self.assertEqual(row["gate"]["policy"]["minimum_waves"], 6)
        self.assertEqual(row["gate"]["resolved_parents"], 5)
        self.assertEqual(row["gate"]["probability"]["hit_rate_pct"], 100)
        self.assertTrue(row["rankable"])
        self.assertFalse(row["experimental_eligible"])
        self.assertEqual(report["scopes"][0]["gate"], row["gate"])

    def test_probability_sample_count_and_gate_use_same_five_parents(self):
        row = next(row for row in self.rank()["rows"] if row["base_direction"] == "SHORT")
        evidence = row["gate"]
        self.assertEqual(evidence["selected_parents"], 5)
        self.assertEqual(evidence["resolved_parents"], 5)
        self.assertEqual(evidence["successes"], 5)
        self.assertEqual(evidence["probability"]["sample_size"], 5)
        self.assertEqual(evidence["probability"]["hit_rate_pct"], 100)
        self.assertTrue(evidence["probability"]["passes"])
        self.assertTrue(row["experimental_eligible"])
        self.assertFalse(evidence["asymmetry"]["available"])

    def test_small_perfect_sample_is_descriptive_without_experimental_eligibility(self):
        result = self.rank("low_n")
        row = result["rows"][0]
        self.assertTrue(result["global_ranking_complete"])
        self.assertTrue(row["rankable"])
        self.assertEqual(row["rank"], 1)
        self.assertEqual(row["gate"]["resolved_parents"], 3)
        self.assertEqual(row["gate"]["probability"]["hit_rate_pct"], 100)
        self.assertGreater(row["gate"]["probability"]["wilson_95_lower_pct"], 40)
        self.assertFalse(row["experimental_eligible"])
        self.assertIn("FEWER_THAN_REQUIRED_RESOLVED_PARENTS", row["gate"]["blockers"])

    def test_earliest_losing_parent_is_not_replaced_by_later_winning_capture(self):
        result = self.rank("mixed")
        row = result["rows"][0]
        ids = {item["entry_id"] for item in row["representatives"]}
        self.assertIn("watch:3:BTC:SHORT", ids)
        self.assertNotIn("watch:4:BTC:SHORT", ids)
        self.assertEqual(row["gate"]["successes"], 4)
        self.assertEqual(row["gate"]["failures"], 1)
        self.assertEqual(row["gate"]["probability"]["hit_rate_pct"], 80)
        self.assertLess(row["gate"]["probability"]["wilson_95_lower_pct"], 40)
        self.assertTrue(row["rankable"])
        self.assertFalse(row["experimental_eligible"])

    def test_ties_have_deterministic_lexical_scope_order(self):
        result = self.rank()
        expected = sorted(row["scope_id"] for row in result["rows"] if row["rankable"])
        self.assertEqual(result["ranked_scope_ids"], expected)
        self.assertEqual([row["scope_id"] for row in
            sorted((row for row in result["rows"] if row["rankable"]), key=lambda row: row["rank"])],
            expected)
        self.assertEqual(self.rank(), result)

    def test_incomplete_population_cannot_publish_partial_ranks(self):
        result = self.rank("pending")
        self.assertFalse(result["global_ranking_complete"])
        self.assertEqual(result["ranked_scope_ids"], [])
        self.assertTrue(all(row["rank"] is None for row in result["rows"]))
        self.assertEqual(result["denominator"]["declared_scopes"], 2)
        self.assertEqual(result["denominator"]["ranked_scopes"], 0)
        self.assertGreater(result["denominator"]["pending_scopes"], 0)

    def test_open_and_ambiguous_remain_explicit_unresolved_denominators(self):
        for name, status in (("open", "OPEN"), ("ambiguous", "AMBIGUOUS")):
            with self.subTest(status=status):
                result = self.rank(name)
                row = result["rows"][0]
                self.assertTrue(result["global_ranking_complete"])
                self.assertEqual(row["outcome_status_counts"][status], 5)
                self.assertEqual(row["gate"]["resolved_parents"], 0)
                self.assertEqual(row["gate"]["successes"], 0)
                self.assertIsNone(row["rank"])
                self.assertFalse(row["rankable"])
                self.assertFalse(row["experimental_eligible"])
                self.assertEqual(result["denominator"]["zero_resolved_scopes"], 1)

    def test_missing_selected_entry_and_zero_match_scope_remain_visible(self):
        result = self.rank("missing")
        rows = {row["base_direction"]: row for row in result["rows"]}
        self.assertTrue(result["global_ranking_complete"])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows["SHORT"]["status"], "INPUT_BLOCKED")
        self.assertEqual(len(rows["SHORT"]["representatives"]), 5)
        self.assertIsNone(rows["SHORT"]["gate"])
        self.assertIsNone(rows["SHORT"]["rank"])
        self.assertFalse(rows["SHORT"]["experimental_eligible"])
        self.assertEqual(rows["LONG"]["status"], "COMPLETE")
        self.assertEqual(rows["LONG"]["gate"]["selected_parents"], 0)
        self.assertEqual(result["denominator"]["input_blocked_scopes"], 1)
        self.assertEqual(result["denominator"]["ranked_scopes"], 0)

    def test_price_gap_is_evidence_blocked_never_ranked_as_failure_or_success(self):
        result = self.rank("gap")
        row = result["rows"][0]
        self.assertEqual(row["status"], "BLOCKED")
        self.assertEqual(row["outcome_status_counts"]["DATA_MISSING"], 1)
        self.assertEqual(row["gate"]["resolved_parents"], 0)
        self.assertIsNone(row["rank"])
        self.assertFalse(row["rankable"])
        self.assertFalse(row["experimental_eligible"])
        self.assertEqual(result["denominator"]["evidence_blocked_scopes"], 1)

    def test_rehashed_scalar_probability_and_eligibility_forgery_reject(self):
        for field in ("hit_rate_pct", "wilson_95_lower_pct", "sample_size"):
            with self.subTest(field=field):
                def mutate(report):
                    row = next(row for row in report["scopes"] if row["base_direction"] == "SHORT")
                    row["gate"]["probability"][field] += 1
                self.corrupt(mutate)
        self.corrupt(lambda report: report["scopes"][0]["gate"].update(experimental_eligible=True),
                     name="mixed")

    def test_rehashed_counts_and_gate_representative_status_forgery_reject(self):
        self.corrupt(lambda report: report["scopes"][0]["gate"].update(successes=5), name="mixed")
        def change_status(report):
            representative = next(row for row in report["scopes"][0]["gate"]["representatives"]
                                  if row["status"] == "FAILURE")
            representative["status"] = "SUCCESS"
        self.corrupt(change_status, name="mixed")
        self.corrupt(lambda report: report["scopes"][0]["status_counts"].update(SUCCESS=5),
                     name="mixed")

    def test_dropped_duplicated_or_reordered_scope_cannot_shrink_denominator(self):
        for mode in ("drop", "duplicate", "reverse"):
            with self.subTest(mode=mode):
                def mutate(report):
                    if mode == "drop":
                        report["scopes"].pop()
                        report["declared_scopes"] -= 1
                    elif mode == "duplicate":
                        report["scopes"][-1] = deepcopy(report["scopes"][0])
                    else:
                        report["scopes"].reverse()
                self.corrupt(mutate)

    def test_duplicate_or_dropped_outcome_cannot_change_parent_sample(self):
        for mode in ("duplicate", "drop"):
            with self.subTest(mode=mode):
                def mutate(report):
                    row = next(row for row in report["scopes"] if row["base_direction"] == "SHORT")
                    if mode == "duplicate":
                        row["outcomes"][-1] = deepcopy(row["outcomes"][0])
                    else:
                        row["outcomes"].pop()
                self.corrupt(mutate)

    def test_swapped_outcome_contracts_cannot_attach_a_winner_to_another_entry(self):
        def mutate(report):
            outcomes = report["scopes"][0]["outcomes"]
            loser = next(row for row in outcomes if row["outcome"]["status"] == "FAILURE")
            winner = next(row for row in outcomes if row["outcome"]["status"] == "SUCCESS")
            loser["outcome"], winner["outcome"] = winner["outcome"], loser["outcome"]
        self.corrupt(mutate, name="mixed")

    def test_rehashed_later_representative_is_rejected_against_complete_decision_ledger(self):
        def mutate(report):
            coverage = report["coverage_receipt"]["scopes"][0]
            later = next(row for row in coverage["decision_ledger"]
                         if row["entry_id"] == "watch:4:BTC:SHORT")
            for representatives in (coverage["representatives"], report["scopes"][0]["representatives"]):
                representative = next(row for row in representatives
                                      if row["entry_id"] == "watch:3:BTC:SHORT")
                for key in tuple(representative):
                    representative[key] = deepcopy(later[key])
        self.corrupt(mutate, name="mixed", coverage=True)

    def test_stale_executor_implementation_rejects_even_with_fresh_report_hash(self):
        def mutate(report):
            report["plan_identity"]["versions"]["implementation_sha256"] = "0" * 64
            report["plan_id"] = contracts.digest(report["plan_identity"])
        self.corrupt(mutate)

    def test_report_must_match_exact_calendar_window(self):
        plan, report = deepcopy(self.pending)
        with self.assertRaises(ValueError):
            ranking.rank_report(plan, report, window_ordinal=1)
        for ordinal in (-1, 2, True):
            with self.subTest(ordinal=ordinal):
                with self.assertRaises(ValueError):
                    ranking.rank_report(plan, report, window_ordinal=ordinal)

    def test_rehashed_changed_discovery_plan_cannot_reuse_original_population(self):
        plan, report = deepcopy(self.good)
        plan["calendar_plan"]["windows"][0]["declaration"]["cohort_key"] = "different-population"
        plan["plan_sha256"] = contracts.digest({
            key: value for key, value in plan.items() if key != "plan_sha256"})
        with self.assertRaises(ValueError):
            ranking.rank_report(plan, report)

    def test_ranking_is_pure_and_does_not_alias_or_modify_evidence(self):
        plan, report = deepcopy(self.good)
        before = deepcopy((plan, report))
        with patch("sqlite3.connect", side_effect=AssertionError("ranking must not reopen databases")):
            result = ranking.rank_report(plan, report)
        self.assertEqual((plan, report), before)
        selected = next(row for row in result["rows"] if row["base_direction"] == "SHORT")
        selected["gate"]["probability"]["hit_rate_pct"] = -1
        selected["representatives"].clear()
        self.assertEqual((plan, report), before)

    def test_ranking_is_descriptive_and_never_authorizes_delivery_or_trading(self):
        result = self.rank()
        for flag in ("runtime_authorized", "telegram_authorized", "trading_authorized",
                     "validated_discovery", "is_prospective_formula_evidence", "scope_pooling",
                     "multiple_testing_adjusted"):
            self.assertIs(result[flag], False, flag)
        self.assertEqual(result["report_sha256"], contracts.digest({
            key: value for key, value in result.items() if key != "report_sha256"}))
        plan, report = self.good
        self.assertEqual(result["source_report_sha256"], report["report_sha256"])
        self.assertEqual(result["search_plan_sha256"], plan["plan_sha256"])
        self.assertEqual(result["source_plan_id"], report["plan_id"])


    def test_rehashed_source_time_outside_declared_part_is_rejected(self):
        def mutate(report):
            row = report["coverage_receipt"]["scopes"][0]["decision_ledger"][0]
            row["usable_from_utc"] = report["coverage_receipt"]["cutoff_utc"]
        self.corrupt(mutate, coverage=True)

    def test_rehashed_cross_scope_source_projection_disagreement_is_rejected(self):
        def mutate(report):
            report["coverage_receipt"]["scopes"][1]["decision_ledger"][0]["feature_sha256"] = "0" * 64
        self.corrupt(mutate, coverage=True)



if __name__ == "__main__":
    unittest.main()
