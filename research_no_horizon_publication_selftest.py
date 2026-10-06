"""Publication over genuine synthetic cohorts, with explicit trusted receipts.

SQLite builders produce the source, parent and outcome evidence. Synthetic
registration records stand in for a trusted database only in the pure API and
read-only store tests; hashes are never asserted to authenticate market origin.
"""
from copy import deepcopy
from datetime import timedelta
from unittest.mock import patch
import unittest

import research_no_horizon_contract as contracts
import research_no_horizon_postgres_store as executor
import research_no_horizon_publication as publication
import research_no_horizon_validation as validation
from research_no_horizon_validation_selftest import (
    AUTHORITY, seal, training_fixture, trusted_receipts, validation_fixture,
)


def publish_fixture(data):
    return publication.publish_reports(data["plan"], **{key: data[key] for key in (
        "selection_plan", "selection_reports", "registration_info", "acquisition_report", "executor_report")})


def waiting_fixture(data):
    value = deepcopy(data)
    acquired = value["acquisition_report"]
    acquired.update(status="WAITING", terminal_receipt=None,
                    executor_plan_id=None, anchor_sha256=None)
    seal(acquired, "report_sha256")
    value["executor_report"] = None
    return value


def native_fixture(data):
    """Synthetic native report identity for read-only dispatch verification.

    This does not claim PostgreSQL integration: CI covers the actual linked
    PostgreSQL path separately. Genuine cohort/outcome contents stay unchanged.
    """
    value = deepcopy(data)
    report = value["executor_report"]
    report["prepared_plan_id"] = report["plan_id"]
    versions = executor.implementation()
    report["runtime_identity"] = {"store_version": executor.VERSION,
        "prepared_plan_id": report["prepared_plan_id"],
        "implementation_sha256": contracts.digest(versions), "implementation": versions}
    report["cohort_store_version"] = executor.VERSION
    report["plan_id"] = contracts.digest(report["runtime_identity"])
    seal(report, "report_sha256")
    value.update(trusted_receipts(value["plan"], report))
    return value


class ReadOnlyAcquisition:
    def __init__(self, data):
        self.data = data
        self.versions = deepcopy(data["plan"]["implementation"]["acquisition"])
        self.calls = []

    def registration_info(self, request_id):
        self.calls.append(("registration_info", request_id))
        if request_id != self.data["plan"]["request_id"]:
            raise AssertionError("only the frozen request may be read")
        return deepcopy(self.data["registration_info"])

    def report(self, request_id):
        self.calls.append(("report", request_id))
        if request_id != self.data["plan"]["request_id"]:
            raise AssertionError("only the frozen request may be read")
        return deepcopy(self.data["acquisition_report"])

    def register_request(self, *args, **kwargs):
        raise AssertionError("publication must not register a request")

    def run_once(self, *args, **kwargs):
        raise AssertionError("publication must not acquire source data")


class ReadOnlyExecutor:
    def __init__(self, data):
        self.data = data
        self.versions = deepcopy(data["plan"]["implementation"]["acquisition"]["executor"])
        self.calls = []

    def report(self, plan_id):
        self.calls.append(("report", plan_id))
        if plan_id != self.data["acquisition_report"]["executor_plan_id"]:
            raise AssertionError("only the admitted executor plan may be read")
        return deepcopy(self.data["executor_report"])

    def submit(self, *args, **kwargs):
        raise AssertionError("publication must not admit a cohort")

    def run_once(self, *args, **kwargs):
        raise AssertionError("publication must not execute outcomes")


class PublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        training = training_fixture()
        cls.good = validation_fixture(training=training)
        cls.pending = validation_fixture(training=training, pending=True)
        cls.old = validation_fixture(training=training, old_count=2)
        cls.no_fresh = validation_fixture(training=training, old_count=5)
        cls.waiting = waiting_fixture(cls.good)
        cls.publications = {name: publish_fixture(getattr(cls, name))
                            for name in ("good", "pending", "old", "no_fresh", "waiting")}

    def test_full_plan_result_and_exact_candidate_definitions_are_preserved(self):
        value = self.publications["good"]
        self.assertEqual(value["validation_plan"], self.good["plan"])
        self.assertEqual(value["validation_result"], validation.evaluate_reports(
            self.good["plan"], **{key: self.good[key] for key in (
                "selection_plan", "selection_reports", "registration_info", "acquisition_report", "executor_report")}))
        selected = value["validation_plan"]["selected_scopes"]
        self.assertEqual(len(selected), 3)
        self.assertEqual([row["scope_id"] for row in value["scope_summary"]],
                         [row["scope_id"] for row in selected])
        self.assertEqual(len({row["candidate_key"] for row in selected}) *
                         len({row["threshold_pct"] for row in selected}), 4)
        for scope, row in zip(selected, value["scope_summary"]):
            self.assertEqual(row["candidate_version"], scope["candidate"]["definition_sha256"])
            self.assertEqual(scope, value["validation_result"]["selected_scopes"][row["ordinal"]])
            self.assertTrue(scope["candidate"])

    def test_global_state_keeps_the_complete_declared_denominator(self):
        for name, state in (("good", "COMPLETE_QUALIFIED"),
                            ("old", "COMPLETE_NO_QUALIFICATION"),
                            ("pending", "INCOMPLETE"), ("waiting", "INCOMPLETE")):
            with self.subTest(name=name):
                value = self.publications[name]
                self.assertEqual(value["state"], state)
                self.assertEqual(len(value["scope_summary"]), 3)
                self.assertEqual(value["validation_result"]["denominator"]["declared_scopes"], 3)
                self.assertEqual([row["ordinal"] for row in value["scope_summary"]], [0, 1, 2])
                if state == "INCOMPLETE":
                    self.assertEqual(value["validation_result"]["qualified_scope_ids"], [])
                    self.assertFalse(any(row["global_qualified"] for row in value["scope_summary"]))

    def test_unselected_training_matches_and_excluded_parents_remain_visible(self):
        value = self.publications["old"]
        selected_ids = {row["scope_id"] for row in self.old["plan"]["selected_scopes"]}
        winners = {representative["btc_parent_movement_id"]
            for scope in self.old["selection_reports"][0]["coverage_receipt"]["scopes"]
            if scope["scope_id"] in selected_ids for representative in scope["representatives"]}
        known = set(value["validation_plan"]["known_matched_training_parent_ids"])
        self.assertEqual(len(known), 6)
        self.assertEqual(len(winners), 5)
        unselected = known - winners
        for row in value["validation_result"]["rows"]:
            self.assertEqual(row["fresh_parent_count"], 3)
            self.assertEqual(row["excluded_parent_count"], 2)
            self.assertTrue(unselected <= {parent["btc_parent_movement_id"]
                                         for parent in row["excluded_parents"]})
            for parent in row["fresh_representatives"] + row["excluded_parents"]:
                self.assertIn("parent_start_time_utc", parent)
                self.assertIn("entry_id", parent)
            for parent in row["excluded_parents"]:
                self.assertIn("PARENT_PRESENT_IN_TRAINING_MATCHED_POPULATION", parent["reasons"])

    def test_original_five_parent_gate_cannot_be_displayed_as_fresh_qualification(self):
        for row in self.publications["old"]["scope_summary"]:
            self.assertEqual(row["original_gate"]["resolved_parents"], 5)
            self.assertTrue(row["original_gate"]["experimental_eligible"])
            self.assertEqual(row["fresh_gate"]["resolved_parents"], 3)
            self.assertEqual(row["fresh_gate"]["probability"]["hit_rate_pct"], 100)
            self.assertFalse(row["prospective_gate_passed"])
            self.assertFalse(row["global_qualified"])
            self.assertEqual(row["original_outcome_status_counts"]["SUCCESS"], 5)

    def test_unobserved_population_is_null_and_observed_empty_population_is_zero(self):
        for row in self.publications["waiting"]["scope_summary"]:
            self.assertIsNone(row["fresh_parent_count"])
            self.assertIsNone(row["excluded_parent_count"])
            self.assertIsNone(row["fresh_gate"])
            self.assertIsNone(row["original_gate"])
        for row in self.publications["no_fresh"]["scope_summary"]:
            self.assertEqual(row["fresh_parent_count"], 0)
            self.assertEqual(row["excluded_parent_count"], 5)
            self.assertEqual(row["fresh_gate"]["selected_parents"], 0)
            self.assertIsNone(row["fresh_gate"]["probability"]["hit_rate_pct"])
            self.assertFalse(row["global_qualified"])

    def test_publication_binds_the_exact_source_evidence_chain(self):
        value = self.publications["good"]
        hashes = value["source_hashes"]
        expected = {"validation_plan_sha256": self.good["plan"]["plan_sha256"],
            "selection_plan_sha256": self.good["selection_plan"]["plan_sha256"],
            "selection_report_sha256": self.good["plan"]["selection_report_sha256"],
            "declaration_sha256": self.good["plan"]["declaration_sha256"],
            "validation_result_sha256": value["validation_result"]["report_sha256"],
            "acquisition_report_sha256": self.good["acquisition_report"]["report_sha256"],
            "executor_report_sha256": self.good["executor_report"]["report_sha256"],
            "executor_plan_id": self.good["executor_report"]["plan_id"],
            "anchor_sha256": self.good["executor_report"]["plan_identity"]["anchor_sha256"]}
        self.assertEqual(hashes, expected)
        self.assertEqual(value["publication_sha256"], contracts.digest({
            key: item for key, item in value.items() if key != "publication_sha256"}))

    def test_json_and_markdown_are_deterministic_and_inputs_are_not_aliased(self):
        data = deepcopy(self.good)
        before = deepcopy(data)
        with patch("sqlite3.connect", side_effect=AssertionError("publication cannot reopen a database")):
            first, second = publish_fixture(data), publish_fixture(data)
            first_markdown = publication.render_markdown(first)
            second_markdown = publication.render_markdown(second)
        self.assertEqual(contracts.canonical(first), contracts.canonical(second))
        self.assertEqual(first_markdown, second_markdown)
        self.assertEqual(data, before)
        first["validation_plan"]["selected_scopes"][0]["candidate"].clear()
        first["validation_result"]["rows"][0]["fresh_representatives"].clear()
        first["scope_summary"][0]["fresh_gate"].clear()
        self.assertEqual(data, before)
        self.assertEqual(second, self.publications["good"])

    def test_markdown_keeps_all_scopes_evidence_hashes_and_proof_limits(self):
        for name in ("good", "pending", "old", "no_fresh", "waiting"):
            with self.subTest(name=name):
                value = self.publications[name]
                text = publication.render_markdown(value)
                plain = text.replace("\\", "")
                self.assertIn(value["state"], text)
                self.assertIn(value["publication_sha256"], text)
                offsets = [text.index(row["scope_id"]) for row in value["scope_summary"]]
                self.assertEqual(offsets, sorted(offsets))
                for row in value["validation_plan"]["selected_scopes"]:
                    self.assertIn(row["candidate_key"], plain)
                    self.assertIn(row["candidate"]["definition_sha256"], text)
                self.assertIn(value["source_hashes"]["validation_plan_sha256"], text)
                self.assertIn(value["source_hashes"]["acquisition_report_sha256"], text)
                self.assertIn("Runtime authorization: no.", text)
                self.assertIn("Telegram authorization: no.", text)
                self.assertIn("Trading authorization: no.", text)
                self.assertIn("TRUSTED_DATABASE_REPORTS_REQUIRED_HASHES_DO_NOT_AUTHENTICATE_ORIGIN", plain)

    def test_renderer_rejects_rehashed_summary_and_source_hash_alterations(self):
        for change in ("fresh_count", "global_qualified", "original_gate", "drop_scope", "source_hash"):
            with self.subTest(change=change):
                value = deepcopy(self.publications["good"])
                row = value["scope_summary"][0]
                if change == "fresh_count":
                    row["fresh_parent_count"] += 1
                elif change == "global_qualified":
                    row["global_qualified"] = False
                elif change == "original_gate":
                    row["original_gate"]["probability"]["hit_rate_pct"] = 999
                elif change == "drop_scope":
                    value["scope_summary"].pop()
                else:
                    value["source_hashes"]["executor_report_sha256"] = "f" * 64
                seal(value, "publication_sha256")
                with self.assertRaises(ValueError):
                    publication.render_markdown(value)

    def test_renderer_rejects_stale_implementation_and_any_granted_authority(self):
        for field in ("publisher_sha256", *AUTHORITY):
            with self.subTest(field=field):
                value = deepcopy(self.publications["good"])
                if field == "publisher_sha256":
                    value["implementation"][field] = "f" * 64
                else:
                    value[field] = True
                seal(value, "publication_sha256")
                with self.assertRaises(ValueError):
                    publication.render_markdown(value)

    def test_pure_publication_recomputes_rehashed_executor_and_training_evidence(self):
        for key in ("executor_report", "selection_reports"):
            with self.subTest(key=key):
                data = deepcopy(self.good)
                report = data[key][0] if key == "selection_reports" else data[key]
                report["scopes"][0]["gate"]["probability"]["hit_rate_pct"] = 999
                seal(report, "report_sha256")
                with self.assertRaises(ValueError):
                    publish_fixture(data)

    def test_foreign_registration_and_rehashed_acquisition_links_are_rejected(self):
        for change in ("late", "request_id", "executor_plan_id"):
            with self.subTest(change=change):
                data = deepcopy(self.good)
                if change == "late":
                    start = contracts.utc(data["plan"]["declaration"]["source_start_utc"])
                    data["registration_info"]["created_at_utc"] = (start + timedelta(microseconds=1)).isoformat()
                else:
                    acquired = data["acquisition_report"]
                    acquired[change] = "f" * 64
                    acquired["terminal_receipt"][change] = "f" * 64
                    seal(acquired["terminal_receipt"], "receipt_sha256")
                    seal(acquired, "report_sha256")
                with self.assertRaises(ValueError):
                    publish_fixture(data)

    def test_native_store_path_reads_only_the_derived_request_and_executor(self):
        data = native_fixture(self.good)
        acquired, executed = ReadOnlyAcquisition(data), ReadOnlyExecutor(data)
        with patch("sqlite3.connect", side_effect=AssertionError("read-only dispatch must not reopen a database")):
            actual = publication.publish_plan(acquired, executed, data["plan"],
                selection_plan=data["selection_plan"], selection_reports=data["selection_reports"])
        self.assertEqual(actual, publish_fixture(data))
        self.assertEqual(acquired.calls, [("registration_info", data["plan"]["request_id"]),
                                         ("report", data["plan"]["request_id"])])
        self.assertEqual(executed.calls, [("report", data["acquisition_report"]["executor_plan_id"])])

    def test_unadmitted_native_path_does_not_read_or_execute_an_outcome_plan(self):
        data = deepcopy(self.waiting)
        acquired, executed = ReadOnlyAcquisition(data), ReadOnlyExecutor(data)
        actual = publication.publish_plan(acquired, executed, data["plan"],
            selection_plan=data["selection_plan"], selection_reports=data["selection_reports"])
        self.assertEqual(actual, self.publications["waiting"])
        self.assertEqual(executed.calls, [])

    def test_registration_failure_stops_native_reads_before_acquisition_or_outcomes(self):
        data = native_fixture(self.good)
        data["registration_info"]["request_id"] = "f" * 64
        acquired, executed = ReadOnlyAcquisition(data), ReadOnlyExecutor(data)
        with self.assertRaises(ValueError):
            publication.publish_plan(acquired, executed, data["plan"],
                selection_plan=data["selection_plan"], selection_reports=data["selection_reports"])
        self.assertEqual(acquired.calls, [("registration_info", data["plan"]["request_id"])])
        self.assertEqual(executed.calls, [])

    def test_research_success_does_not_claim_origin_profitability_or_trading_authority(self):
        value = self.publications["good"]
        for record in (value, value["validation_plan"], value["validation_result"],
                       *value["validation_result"]["rows"]):
            for key in AUTHORITY:
                self.assertIs(record[key], False)
        result = value["validation_result"]
        for key in ("database_origin_authenticated_by_this_tool", "profitability_validated",
                    "statistical_independence_proven", "multiple_testing_adjusted", "validated_discovery"):
            self.assertIs(result[key], False)


if __name__ == "__main__":
    unittest.main()
