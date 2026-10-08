"""Fresh-parent validation against genuine synthetic SQLite cohort reports.

Captures, causal parents, immutable anchors and outcome checkpoints use the
existing real builders. Only trusted database registration/admission receipts
are represented by explicit synthetic dictionaries in these pure API tests.
"""
from copy import deepcopy
from datetime import timedelta
from functools import lru_cache
import unittest
from unittest.mock import patch

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage_selftest as coverage_fixture
import research_no_horizon_cohort_store as coordinator
import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
import research_no_horizon_parent_coverage_selftest as parent_fixture
import research_no_horizon_selection as selection
import research_no_horizon_validation as validation
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture, _seal_exports, _entry_time
from research_no_horizon_experiment import _scopes
from research_no_horizon_parent_coverage_selftest import scope, source_row
from research_no_horizon_selection_selftest import execute_fixture
from research_watch_score_capture_selftest import BASE, bundle, derivatives


ALIASES = ("FUTURES_CVD_TOTAL_65",
           "captured-question-search-v3-experimental-binding:CORE_FUTURES_CVD_TOTAL_65")
AUTHORITY = dict(runtime_authorized=False, telegram_authorized=False, trading_authorized=False)


def seal(value, key):
    value[key] = contracts.digest({name: item for name, item in value.items() if name != key})
    return value


@lru_cache(maxsize=2)
def training_fixture(base=BASE):
    """One genuine complete search, a non-Cartesian shortlist, and a losing parent.

    The LONG-only parent matches no selected scope. Its ID must nevertheless be
    excluded from subsequent validation because every training MATCH counts.
    Callers must deepcopy before modifying this cached synthetic evidence.
    """
    offset = (base - BASE).total_seconds() / timedelta(hours=2).total_seconds()
    rows = [source_row(parent_offset=offset + i, snapshot_set_id=i + 1) for i in range(5)]
    market = derivatives()
    for coin in market.values():
        for window in coin["flow"]["futures"]["windows"].values():
            window.update(direction="BULLISH", continuous_strength=1.)
    bullish, *_ = bundle(snapshot=market)
    with patch.object(parent_fixture, "_template", return_value={"source_rows": [{"scores": bullish}]}):
        rows.append(source_row(parent_offset=offset + 4.2, snapshot_set_id=6))
    with patch.object(coverage_fixture, "BASE", base):
        declaration, _, exports = outcome_fixture([rows[:3], rows[3:]],
            scopes=[scope(threshold=.2), scope(threshold=.25)], mode="success")
    plan = selection.build_plan(declaration, base_directions=["SHORT", "LONG"],
        thresholds_pct=[.2, .25], candidate_keys=list(ALIASES), window_count=1,
        top_k=3, required_eligible_windows=1)
    frozen = plan["discovery_plan"]["calendar_plan"]["windows"][0]["declaration"]
    fixture = _seal_exports(frozen, exports)
    reports = execute_fixture(plan, [fixture])
    return plan, reports, fixture


def future_template(*, training=None, mode="success", old_count=0,
                    formerly_nonmatched=False, duplicate_loser=False, missing=False,
                    old_path_gap=False):
    """Build unbound future evidence; all unmodified parent starts are fresh."""
    selected_plan, training_reports, training_data = training or training_fixture()
    training_base = contracts.utc(training_data[0]["source_start_utc"]) - timedelta(minutes=6)
    future_base = training_base + timedelta(days=2)
    offset = (future_base - BASE).total_seconds() / timedelta(hours=2).total_seconds()
    offsets = [0, 1, 2, 3, 4, 4.2] if old_path_gap else range(5)
    rows = [source_row(parent_offset=offset + step, snapshot_set_id=1001 + i)
            for i, step in enumerate(offsets)]
    old_rows = [row for export in training_data[2] for row in export["source_rows"]]
    # The final old row is the unique LONG-only, unselected training parent.
    for index in range(old_count):
        parent = deepcopy(old_rows[-1 - index]["btc_parent"])
        parent["observed_through_utc"] = rows[index]["btc_prior_bar"]["close_time_utc"]
        rows[index]["btc_parent"] = parent
    if formerly_nonmatched:
        parent = source_row(parent_offset=offset - .5, snapshot_set_id=999)["btc_parent"]
        parent["observed_through_utc"] = rows[0]["btc_prior_bar"]["close_time_utc"]
        rows[0]["btc_parent"] = parent
    parts = [rows[:3], rows[3:]]
    if duplicate_loser:
        later = source_row(parent_offset=offset + 2, snapshot_set_id=1006, decision_delay_seconds=90)
        for key in ("open_time_utc", "close_time_utc"):
            later["btc_prior_bar"][key] = contracts.utc(later["btc_prior_bar"][key]) + timedelta(minutes=1)
        later["btc_parent"]["observed_through_utc"] = later["btc_prior_bar"]["close_time_utc"]
        rows[2]["btc_parent"] = deepcopy(later["btc_parent"])
        parts[1].insert(0, later)
    with patch.object(coverage_fixture, "BASE", future_base):
        declaration, _, exports = outcome_fixture(parts,
            scopes=[scope(threshold=.2), scope(threshold=.25)], mode=mode)
    declaration["source_start_utc"] = future_base.isoformat()
    declaration["parts"][0]["source_start_utc"] = future_base.isoformat()
    exports[0]["source_start_utc"] = future_base.isoformat()
    declaration["prior_outcomes_observed"] = False
    declaration["declared_at_utc"] = (training_base + timedelta(days=1)).isoformat()
    declaration["cohort_key"] = "synthetic-future-validation"
    if duplicate_loser:
        entry = _entry_time(rows[2]["intake"]["usable_from_utc"])
        direction = _scopes([scope()])[0]["analysis_direction"]
        for export in exports:
            for candle in export["candles"]:
                if contracts.utc(candle["open_time_utc"]) == entry:
                    candle.update(high=100.01 if direction == "LONG" else 100.35,
                                  low=99.65 if direction == "LONG" else 99.99)
    if missing:
        entry = _entry_time(rows[0]["intake"]["usable_from_utc"])
        for export in exports:
            export["candles"] = [bar for bar in export["candles"]
                                 if contracts.utc(bar["open_time_utc"]) != entry]
    if old_path_gap:
        entry = _entry_time(rows[0]["intake"]["usable_from_utc"])
        for export in exports:
            for candle in export["candles"]:
                if contracts.utc(candle["open_time_utc"]) == entry:
                    candle.update(high=100.01, low=99.99)
            export["candles"] = [bar for bar in export["candles"]
                if contracts.utc(bar["open_time_utc"]) != entry + timedelta(minutes=1)]
    return cohort.normalize_declaration(declaration), exports


def validation_fixture(*, training=None, pending=False, **options):
    """Shared fixture for pure evaluation and real PostgreSQL parity tests."""
    selection_plan, selection_reports, training_data = training or training_fixture()
    training = selection_plan, selection_reports, training_data
    template, exports = future_template(training=training, **options)
    plan = validation.build_plan(template, selection_plan=selection_plan,
                                 selection_reports=selection_reports)
    fixture = _seal_exports(plan["declaration"], exports)
    if pending:
        declared, anchor, parts = fixture
        with coordinator.LocalCohortStore(":memory:") as store:
            plan_id = store.submit_cohort(declared, anchor, lambda n: parts[n])
            store.run_cohort(plan_id, "partial-validation", scope_budget=1,
                             candle_budget=100000, entry_budget=128, batch_size=128)
            executor_report = store.report(plan_id)
    else:
        executor_report = execute_fixture(None, [fixture])[0]
    result = {"plan": plan, "selection_plan": selection_plan,
            "selection_reports": selection_reports, "fixture": fixture,
            "executor_report": executor_report}
    result.update(trusted_receipts(plan, executor_report))
    return result


def trusted_receipts(plan, executor_report):
    """Synthetic trusted DB timing/link evidence, never an origin attestation."""
    declaration = plan["declaration"]
    registered = contracts.utc(plan["training_max_cutoff_utc"]) + timedelta(hours=1)
    info = {"request_id": plan["request_id"], "declaration_sha256": plan["declaration_sha256"],
        "implementation_sha256": plan["request_identity"]["implementation_sha256"],
        "created_at_utc": registered.isoformat(), "source_start_utc": declaration["source_start_utc"],
        "registered_before_source_start": True}
    terminal = {"version": acquisition.VERSION, "request_id": plan["request_id"],
        "status": "ADMITTED", "declaration_sha256": plan["declaration_sha256"],
        "anchor_sha256": executor_report["plan_identity"]["anchor_sha256"],
        "executor_plan_id": executor_report["plan_id"], **AUTHORITY}
    seal(terminal, "receipt_sha256")
    acquired = {"version": acquisition.VERSION, "request_id": plan["request_id"],
        "identity": deepcopy(plan["request_identity"]), "declaration": deepcopy(declaration),
        "not_before_utc": max(contracts.utc(declaration["declared_at_utc"]),
                              contracts.utc(declaration["cutoff_utc"])).isoformat(),
        "status": "ADMITTED", "terminal_receipt": terminal,
        "anchor_sha256": terminal["anchor_sha256"], "executor_plan_id": executor_report["plan_id"],
        **AUTHORITY}
    return {"registration_info": info, "acquisition_report": seal(acquired, "report_sha256")}


def evaluate_fixture(value):
    return validation.evaluate_reports(value["plan"], **{key: value[key] for key in (
        "selection_plan", "selection_reports", "registration_info", "acquisition_report", "executor_report")})


class ValidationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.good = validation_fixture()
        cls.three_fresh = validation_fixture(old_count=2)
        cls.old_start = validation_fixture(formerly_nonmatched=True)
        cls.earliest_loss = validation_fixture(duplicate_loser=True)
        cls.pending = validation_fixture(pending=True)
        cls.blocked = validation_fixture(missing=True, old_count=1)
        cls.open = validation_fixture(mode="open")
        cls.ambiguous = validation_fixture(mode="ambiguous")
        cls.no_fresh = validation_fixture(old_count=5)

    def evaluate(self, name="good"):
        return evaluate_fixture(deepcopy(getattr(self, name)))

    def test_five_fresh_successes_pass_exact_original_atomic_gate(self):
        result = self.evaluate()
        self.assertTrue(result["validation_complete"])
        self.assertEqual(len(result["qualified_scope_ids"]), 3)
        self.assertEqual(result["denominator"]["declared_scopes"], 3)
        for row in result["rows"]:
            self.assertTrue(row["prospective_gate_passed"])
            self.assertEqual(row["fresh_parent_count"], 5)
            self.assertEqual(row["excluded_parent_count"], 0)
            self.assertEqual(row["fresh_gate"]["resolved_parents"], 5)
            self.assertEqual(row["fresh_gate"]["successes"], 5)
            self.assertEqual(row["fresh_gate"]["policy"], self.good["plan"]["declaration"]["gate_policy"])
        self.assertTrue(result["registration_timing_verified"])
        self.assertFalse(result["database_origin_authenticated_by_this_tool"])

    def test_selected_noncartesian_scope_tuples_are_not_expanded_or_dropped(self):
        plan = self.good["plan"]
        selected = selection.select_reports(self.good["selection_plan"], self.good["selection_reports"])
        exact = {(item["candidate_key"], item["base_direction"], item["threshold_pct"])
                 for item in selected["selected_scopes"]}
        actual = {(item["candidate_key"], item["base_direction"], item["threshold_pct"])
                  for item in plan["declaration"]["scopes"]}
        self.assertEqual(len(exact), 3)
        self.assertEqual(len({item[0] for item in exact}) * len({item[2] for item in exact}), 4)
        self.assertEqual(actual, exact)
        self.assertEqual({row["scope_id"] for row in self.evaluate()["rows"]},
                         {row["scope_id"] for row in selected["selected_scopes"]})

    def test_three_fresh_plus_two_training_parents_cannot_satisfy_five(self):
        result = self.evaluate("three_fresh")
        self.assertEqual(result["qualified_scope_ids"], [])
        for original, row in zip(result["original_verification"]["rows"], result["rows"]):
            self.assertEqual(original["gate"]["resolved_parents"], 5)
            self.assertTrue(original["experimental_eligible"])
            self.assertEqual(row["fresh_parent_count"], 3)
            self.assertEqual(row["excluded_parent_count"], 2)
            self.assertEqual(row["fresh_gate"]["resolved_parents"], 3)
            self.assertEqual(row["fresh_gate"]["probability"]["hit_rate_pct"], 100)
            self.assertFalse(row["prospective_gate_passed"])

    def test_training_blacklist_includes_matches_from_unselected_scopes(self):
        selected = selection.select_reports(self.good["selection_plan"], self.good["selection_reports"])
        selected_ids = set(selected["selected_scope_ids"])
        winners = {item["btc_parent_movement_id"] for full in self.good["selection_reports"][0]["coverage_receipt"]["scopes"]
                   if full["scope_id"] in selected_ids for item in full["representatives"]}
        known = set(self.good["plan"]["known_matched_training_parent_ids"])
        self.assertEqual(len(winners), 5)
        self.assertEqual(len(known), 6)
        loser_only = known - winners
        self.assertEqual(len(loser_only), 1)
        for row in self.evaluate("three_fresh")["rows"]:
            excluded = {item["btc_parent_movement_id"]: item for item in row["excluded_parents"]}
            self.assertTrue(loser_only <= set(excluded))
            for parent in loser_only:
                self.assertIn("PARENT_PRESENT_IN_TRAINING_MATCHED_POPULATION", excluded[parent]["reasons"])

    def test_preexisting_parent_not_seen_in_training_is_excluded_by_start_time(self):
        result = self.evaluate("old_start")
        training = set(self.good["plan"]["known_matched_training_parent_ids"])
        for row in result["rows"]:
            self.assertEqual(row["fresh_parent_count"], 4)
            self.assertEqual(row["excluded_parent_count"], 1)
            excluded = row["excluded_parents"][0]
            self.assertNotIn(excluded["btc_parent_movement_id"], training)
            self.assertEqual(excluded["reasons"], ["PARENT_STARTED_BEFORE_VALIDATION_SOURCE_START"])
            self.assertFalse(row["prospective_gate_passed"])

    def test_filtering_preserves_original_earliest_loser_instead_of_later_winner(self):
        for row in self.evaluate("earliest_loss")["rows"]:
            ids = {item["entry_id"] for item in row["fresh_representatives"]}
            self.assertIn("watch:1003:BTC:SHORT", ids)
            self.assertNotIn("watch:1006:BTC:SHORT", ids)
            self.assertEqual(row["fresh_gate"]["successes"], 4)
            self.assertEqual(row["fresh_gate"]["failures"], 1)
            self.assertFalse(row["prospective_gate_passed"])

    def test_pending_other_scope_suppresses_already_complete_gate_qualification(self):
        result = self.evaluate("pending")
        self.assertFalse(result["validation_complete"])
        complete = [row for row in result["rows"] if row["original_status"] == "COMPLETE"]
        self.assertEqual(len(complete), 1)
        self.assertTrue(complete[0]["fresh_gate"]["experimental_eligible"])
        self.assertTrue(all(not row["prospective_gate_passed"] for row in result["rows"]))
        self.assertEqual(result["qualified_scope_ids"], [])

    def test_original_input_blocked_is_not_salvaged_when_missing_parent_is_old(self):
        result = self.evaluate("blocked")
        self.assertTrue(result["validation_complete"])
        self.assertEqual(len(result["rows"]), 3)
        for row in result["rows"]:
            self.assertEqual(row["original_status"], "INPUT_BLOCKED")
            self.assertEqual(row["excluded_parent_count"], 1)
            self.assertIsNone(row["fresh_gate"])
            self.assertFalse(row["prospective_gate_passed"])
            self.assertIn("ORIGINAL_INPUT_BLOCKED", row["exclusion_reasons"])

    def test_old_parent_path_gap_does_not_erase_five_complete_fresh_successes(self):
        data = validation_fixture(old_count=1, old_path_gap=True)
        result = evaluate_fixture(data)
        self.assertTrue(result["validation_complete"])
        self.assertEqual(len(result["qualified_scope_ids"]), 3)
        for original, row in zip(result["original_verification"]["rows"], result["rows"]):
            self.assertEqual(original["status"], "BLOCKED")
            self.assertEqual(original["outcome_status_counts"]["DATA_MISSING"], 1)
            self.assertFalse(original["experimental_eligible"])
            self.assertEqual(row["excluded_parent_count"], 1)
            self.assertEqual(row["fresh_parent_count"], 5)
            self.assertEqual(row["fresh_gate"]["resolved_parents"], 5)
            self.assertTrue(row["prospective_gate_passed"])

    def test_open_and_ambiguous_are_unresolved_not_successes(self):
        for name, status in (("open", "OPEN"), ("ambiguous", "AMBIGUOUS")):
            with self.subTest(status=status):
                result = self.evaluate(name)
                self.assertTrue(result["validation_complete"])
                self.assertEqual(result["qualified_scope_ids"], [])
                for row in result["rows"]:
                    self.assertEqual(row["fresh_parent_count"], 5)
                    self.assertEqual(row["fresh_gate"]["resolved_parents"], 0)
                    self.assertEqual(row["fresh_gate"]["status_counts"][status], 5)

    def test_no_fresh_population_stays_visible_without_qualification(self):
        result = self.evaluate("no_fresh")
        self.assertEqual(result["qualified_scope_ids"], [])
        self.assertEqual(len(result["rows"]), 3)
        for row in result["rows"]:
            self.assertEqual(row["fresh_parent_count"], 0)
            self.assertEqual(row["excluded_parent_count"], 5)
            self.assertEqual(row["fresh_gate"]["selected_parents"], 0)
            self.assertFalse(row["prospective_gate_passed"])

    def test_rehashed_executor_metrics_and_dropped_scope_are_revalidated(self):
        for mutation in ("probability", "eligible", "drop"):
            with self.subTest(mutation=mutation):
                data = deepcopy(self.good)
                report = data["executor_report"]
                if mutation == "probability":
                    report["scopes"][0]["gate"]["probability"]["wilson_95_lower_pct"] += 1
                elif mutation == "eligible":
                    report["scopes"][0]["gate"]["experimental_eligible"] = False
                else:
                    report["scopes"].pop()
                    report["declared_scopes"] -= 1
                seal(report, "report_sha256")
                with self.assertRaises(ValueError):
                    evaluate_fixture(data)

    def test_late_and_pretraining_registration_timestamps_are_rejected(self):
        for stamp in (contracts.utc(self.good["plan"]["declaration"]["source_start_utc"]) + timedelta(microseconds=1),
                      contracts.utc(self.good["plan"]["training_max_cutoff_utc"]) - timedelta(microseconds=1)):
            with self.subTest(stamp=stamp):
                data = deepcopy(self.good)
                data["registration_info"]["created_at_utc"] = stamp.isoformat()
                with self.assertRaises(ValueError):
                    evaluate_fixture(data)

    def test_rehashed_foreign_request_anchor_or_executor_link_is_rejected(self):
        for key in ("request_id", "anchor_sha256", "executor_plan_id"):
            with self.subTest(key=key):
                data = deepcopy(self.good)
                acquired = data["acquisition_report"]
                acquired[key] = "f" * 64
                acquired["terminal_receipt"][key] = "f" * 64
                seal(acquired["terminal_receipt"], "receipt_sha256")
                seal(acquired, "report_sha256")
                with self.assertRaises(ValueError):
                    evaluate_fixture(data)

    def test_unadmitted_acquisition_retains_all_scopes_and_rejects_outcome_attachment(self):
        for status in ("WAITING", "FETCHING", "READY"):
            with self.subTest(status=status):
                data = deepcopy(self.good)
                acquired = data["acquisition_report"]
                acquired.update(status=status, terminal_receipt=None, executor_plan_id=None, anchor_sha256=None)
                seal(acquired, "report_sha256")
                with self.assertRaises(ValueError):
                    evaluate_fixture(data)
                data["executor_report"] = None
                result = evaluate_fixture(data)
                self.assertFalse(result["validation_complete"])
                self.assertEqual(len(result["rows"]), 3)
                self.assertEqual(result["qualified_scope_ids"], [])

    def test_blocked_acquisition_retains_diagnostic_receipt_without_any_qualification(self):
        data = validation_fixture()
        acquired = data["acquisition_report"]
        acquired.update(status="BLOCKED", executor_plan_id=None)
        terminal = acquired["terminal_receipt"]
        terminal.update(status="BLOCKED", executor_plan_id=None, reason="SYNTHETIC_SOURCE_PROOF_BLOCKED")
        seal(terminal, "receipt_sha256")
        seal(acquired, "report_sha256")
        data["executor_report"] = None
        result = evaluate_fixture(data)
        self.assertFalse(result["validation_complete"])
        self.assertEqual(len(result["rows"]), 3)
        self.assertEqual(result["qualified_scope_ids"], [])
        self.assertEqual(result["acquisition_evidence"]["terminal_receipt"]["reason"],
                         "SYNTHETIC_SOURCE_PROOF_BLOCKED")

    def test_training_reports_are_recomputed_before_plan_validation(self):
        for change in ("missing", "forged"):
            with self.subTest(change=change):
                data = deepcopy(self.good)
                if change == "missing":
                    data["selection_reports"] = []
                else:
                    report = data["selection_reports"][0]
                    report["scopes"][0]["gate"]["probability"]["hit_rate_pct"] = 999
                    seal(report, "report_sha256")
                with self.assertRaises(ValueError):
                    evaluate_fixture(data)

    def test_rehashed_frozen_plan_or_implementation_changes_are_rejected(self):
        for change in ("scope", "blacklist", "implementation"):
            with self.subTest(change=change):
                data = deepcopy(self.good)
                plan = data["plan"]
                if change == "scope":
                    plan["declaration"]["scopes"].pop()
                elif change == "blacklist":
                    plan["known_matched_training_parent_ids"].pop()
                else:
                    plan["implementation"]["validator_sha256"] = "0" * 64
                seal(plan, "plan_sha256")
                with self.assertRaises(ValueError):
                    evaluate_fixture(data)

    def test_future_template_rejects_known_outcomes_changed_gate_and_overlapping_start(self):
        for change in ("known", "gate", "overlap"):
            with self.subTest(change=change):
                template = deepcopy(self.good["plan"]["template_declaration"])
                if change == "known":
                    template["prior_outcomes_observed"] = True
                elif change == "gate":
                    template["gate_policy"] = gate.make_policy(policy_version="different-future-policy", minimum_waves=6)
                else:
                    start = contracts.utc(self.good["plan"]["training_max_cutoff_utc"])
                    old = contracts.utc(template["source_start_utc"])
                    delta = start - old
                    for key in ("source_start_utc", "source_end_utc", "cutoff_utc"):
                        template[key] = (contracts.utc(template[key]) + delta).isoformat()
                    for part in template["parts"]:
                        for key in ("source_start_utc", "source_end_utc"):
                            part[key] = (contracts.utc(part[key]) + delta).isoformat()
                    template["declared_at_utc"] = start.isoformat()
                with self.assertRaises(ValueError):
                    validation.build_plan(template, selection_plan=self.good["selection_plan"],
                                          selection_reports=self.good["selection_reports"])

    def test_validation_is_deterministic_and_does_not_mutate_or_alias_evidence(self):
        data = deepcopy(self.good)
        before = deepcopy(data)
        with patch("sqlite3.connect", side_effect=AssertionError("pure validation cannot reopen a database")):
            result = evaluate_fixture(data)
        self.assertEqual(data, before)
        self.assertEqual(result, self.evaluate())
        result["rows"][0]["fresh_representatives"].clear()
        result["original_verification"]["rows"][0]["representatives"].clear()
        self.assertEqual(data, before)

    def test_success_never_authorizes_runtime_delivery_or_trading(self):
        result = self.evaluate()
        for value in [result, *result["rows"]]:
            for key in AUTHORITY:
                self.assertIs(value[key], False, key)
        self.assertFalse(result["database_origin_authenticated_by_this_tool"])
        self.assertFalse(result["prior_outcomes_observed"])
        self.assertEqual(result["report_sha256"], contracts.digest({
            key: value for key, value in result.items() if key != "report_sha256"}))


if __name__ == "__main__":
    unittest.main()
