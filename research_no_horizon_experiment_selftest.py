"""Genuine-source tests for frozen experiment orchestration and fenced budgets."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import research_no_horizon_contract as contracts
import research_no_horizon_experiment as experiment
import research_no_horizon_source as source
import research_no_horizon_store as child
from research_no_horizon_source_selftest import fixture


def scope(direction="SHORT", threshold=1.5, candidate="FUTURES_CVD_TOTAL_65"):
    return {"candidate_key": candidate, "base_direction": direction, "threshold_pct": threshold}


class Experiments(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = fixture()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name)/"research.sqlite"
        self.data = deepcopy(self.original)
        self.store = experiment.LocalExperimentStore(self.path)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def submit(self, scopes=None, **kwargs):
        return self.store.submit_plan(self.data, scopes or [scope()], **kwargs)

    def finish(self, plan):
        for _ in range(20):
            result = self.store.run_plan(plan, "worker", scope_budget=64, candle_budget=20)
            if result["all_scopes_processed"]:
                return result
        self.fail("plan did not finish")

    def test_manifest_precedes_jobs_and_pending_metrics_are_null(self):
        plan = self.submit([scope(), scope("LONG")])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM local_jobs").fetchone()[0], 0)
        report = self.store.report(plan)
        self.assertFalse(report["all_scopes_processed"])
        self.assertEqual(len(report["scopes"]), 2)
        for row in report["scopes"]:
            self.assertEqual(row["status"], "NOT_SUBMITTED")
            for key in ("gate", "status_counts", "source_counts", "candle_evaluations", "computation_complete"):
                self.assertIsNone(row[key])
        self.assertFalse(report["validated_discovery"])
        self.assertFalse(report["multiple_testing_adjusted"])
        self.assertFalse(report["runtime_authorized"])

    def test_order_independent_idempotence_and_conflicting_plan_key(self):
        scopes = [scope(), scope("LONG")]
        plan = self.submit(scopes, plan_key="named")
        self.assertEqual(plan, self.submit(list(reversed(scopes)), plan_key="named"))
        self.assertEqual(plan, self.submit(scopes))
        with self.assertRaisesRegex(ValueError, "plan_key"):
            self.submit([scope(threshold=2)], plan_key="named")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM experiment_plans").fetchone()[0], 1)

    def test_normalized_duplicates_and_admission_bounds(self):
        for value in (1, 1.0, "1.000"):
            with self.assertRaisesRegex(ValueError, "DUPLICATE"):
                self.submit([scope(threshold=1), scope(threshold=value)])
        with self.assertRaises(ValueError):
            self.submit([scope(threshold=i+0.1) for i in range(65)])
        with patch.object(experiment, "MAX_EXPANDED_BYTES", 1):
            with self.assertRaisesRegex(ValueError, "EXPANDED"):
                self.submit()
        with patch.object(source, "MAX_BYTES", 1):
            with self.assertRaisesRegex(ValueError, "BYTE_LIMIT"):
                self.submit()
        with self.assertRaisesRegex(ValueError, "UNSUPPORTED"):
            self.submit([scope(candidate="made-up")])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM experiment_plans").fetchone()[0], 0)

    def test_restart_round_robin_and_global_candle_budget(self):
        plan = self.submit([scope(threshold=3), scope(threshold=4)])
        seen = []
        for _ in range(8):
            result = self.store.run_plan(plan, "worker", scope_budget=1, candle_budget=1, batch_size=1)
            self.assertEqual(result["attempted_scopes"], 1)
            self.assertEqual(result["candle_evaluations_this_run"], 1)
            seen.extend(result["attempted_ordinals"])
            self.store.close()
            self.store = experiment.LocalExperimentStore(self.path)
        self.assertEqual(seen[:4], [0, 1, 0, 1])
        report = self.store.report(plan)
        self.assertTrue(report["all_scopes_processed"])
        self.assertTrue(report["computation_complete"])
        self.assertTrue(all(row["status_counts"]["OPEN"] == 1 for row in report["scopes"]))
        self.assertTrue(all(row["gate"]["selected_parents"] == 1 for row in report["scopes"]))

    def test_scope_preparation_bound_and_shared_budget(self):
        plan = self.submit([scope(threshold=3), scope(threshold=4), scope(threshold=5)])
        with patch.object(source, "build_snapshot", wraps=source.build_snapshot) as build:
            report = self.store.run_plan(plan, "worker", scope_budget=2, candle_budget=3, batch_size=1)
            self.assertEqual(build.call_count, 2)
        self.assertEqual(report["candle_evaluations_this_run"], 3)
        self.assertEqual(sum(row["candle_evaluations"] or 0 for row in report["scopes"]), 3)
        self.assertEqual(report["scopes"][2]["status"], "NOT_SUBMITTED")
        self.assertFalse(report["all_scopes_processed"])

    def test_no_match_and_unknown_are_different_complete_processing_results(self):
        normal = self.submit([scope(), scope("LONG")])
        result = self.finish(normal)
        matched = next(row for row in result["scopes"] if row["base_direction"] == "SHORT")
        absent = next(row for row in result["scopes"] if row["base_direction"] == "LONG")
        self.assertEqual(matched["status_counts"]["SUCCESS"], 1)
        self.assertEqual(absent["source_counts"]["NO_MATCH"], 1)
        self.assertEqual(absent["emitted_opportunities"], 0)
        self.assertTrue(result["source_coverage_complete"])
        self.data = fixture(unavailable=True)
        unknown = self.finish(self.submit())
        self.assertTrue(unknown["all_scopes_processed"])
        self.assertTrue(unknown["computation_complete"])
        self.assertFalse(unknown["source_coverage_complete"])
        self.assertEqual(unknown["scopes"][0]["source_counts"]["UNKNOWN"], 1)
        self.assertTrue(unknown["scopes"][0]["source_blockers"])
        self.assertFalse(unknown["scopes"][0]["gate"]["experimental_eligible"])

    def test_missing_raw_source_is_preserved_not_omitted(self):
        self.data["source_rows"][0]["stored_source"] = None
        report = self.finish(self.submit())
        self.assertEqual(report["scopes"][0]["source_counts"]["UNKNOWN_SOURCE"], 1)
        self.assertFalse(report["source_coverage_complete"])
        self.assertTrue(report["all_scopes_processed"])

    def test_malformed_rows_and_bars_become_visible_input_blocked(self):
        for key in ("source_rows", "candles"):
            with self.subTest(key=key):
                self.data = deepcopy(self.original)
                self.data[key] = [None]
                report = self.finish(self.submit())
                self.assertEqual(len(report["scopes"]), 1)
                self.assertEqual(report["scopes"][0]["status"], "INPUT_BLOCKED")
                self.assertIsNone(report["scopes"][0]["gate"])
                self.assertFalse(report["computation_complete"])
                self.assertFalse(report["source_coverage_complete"])
                self.assertTrue(report["all_scopes_processed"])

    def test_one_preparation_failure_does_not_omit_other_scopes(self):
        plan = self.submit([scope(), scope("LONG")])
        original = source.build_snapshot
        def build(export, candidate, direction, symbol, threshold):
            if direction == "SHORT":
                raise ValueError("fixture input blocker")
            return original(export, candidate, direction, symbol, threshold)
        with patch.object(source, "build_snapshot", side_effect=build):
            report = self.finish(plan)
        self.assertEqual({row["status"] for row in report["scopes"]}, {"COMPLETE", "INPUT_BLOCKED"})
        self.assertFalse(report["computation_complete"])
        self.assertEqual(self.store.run_plan(plan, "worker")["attempted_scopes"], 0)

    def test_crash_between_submit_and_link_reuses_exact_child(self):
        plan = self.submit()
        with patch.object(self.store, "_link", side_effect=RuntimeError("crash after child submit")):
            with self.assertRaisesRegex(RuntimeError, "crash"):
                self.store.run_plan(plan, "worker")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM local_jobs").fetchone()[0], 1)
        self.assertEqual(self.store.report(plan)["scopes"][0]["status"], "NOT_SUBMITTED")
        self.store.close()
        self.store = experiment.LocalExperimentStore(self.path)
        report = self.finish(plan)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM local_jobs").fetchone()[0], 1)
        self.assertEqual(report["scopes"][0]["status_counts"]["SUCCESS"], 1)

    def test_unrelated_jobs_are_not_claimed_and_existing_exact_child_is_reused(self):
        unrelated = self.store.children.submit_snapshot(source.build_snapshot(self.data,
            "FUTURES_CVD_TOTAL_65", "SHORT", "BTC", 9))
        exact = self.store.children.submit_snapshot(source.build_snapshot(self.data,
            "FUTURES_CVD_TOTAL_65", "SHORT", "BTC", 1.5))
        report = self.finish(self.submit())
        self.assertEqual(report["scopes"][0]["job_id"], exact)
        self.assertEqual(self.store.children.get_job(unrelated)["status"], "PENDING")
        self.assertEqual(self.store.children.get_job(unrelated)["fencing_token"], 0)

    def test_leased_job_keeps_report_incomplete(self):
        plan = self.submit([scope(threshold=3)])
        self.store.run_plan(plan, "first", candle_budget=1)
        job = self.store.report(plan)["scopes"][0]["job_id"]
        self.store.children.claim_job("other", job_id=job)
        report = self.store.run_plan(plan, "blocked-worker")
        self.assertEqual(report["candle_evaluations_this_run"], 0)
        self.assertEqual(report["scopes"][0]["status"], "RUNNING")
        self.assertFalse(report["all_scopes_processed"])
        self.assertIsNone(report["scopes"][0]["gate"])

    def test_counter_return_race_counts_only_this_workers_fencing_token(self):
        plan = self.submit([scope(threshold=3)])
        original = self.store.children.get_job
        raced = []
        def racing_get(job_id):
            value = original(job_id)
            if value["status"] == "PENDING" and value["candle_evaluations"] == 1 and not raced:
                raced.append(True)
                # A separate connection commits after this call's checkpoint but
                # before its process_claim return reads get_job.
                with child.LocalResearchStore(self.path) as other:
                    other.run_once("racing-worker", job_id=job_id, candle_budget=1)
                return original(job_id)
            return value
        with patch.object(self.store.children, "get_job", side_effect=racing_get):
            report = self.store.run_plan(plan, "worker", candle_budget=1)
        self.assertTrue(raced)
        self.assertEqual(report["candle_evaluations_this_run"], 1)
        self.assertEqual(report["scopes"][0]["candle_evaluations"], 2)
        rows = self.store.connection.execute("SELECT * FROM experiment_work_commits").fetchall()
        self.assertEqual(len(rows), 2)

    def test_immutable_records_and_external_source_plan_tamper_fail_closed(self):
        plan = self.submit()
        for table in ("experiment_plans", "experiment_sources", "experiment_scopes"):
            with self.assertRaises(sqlite3.IntegrityError):
                self.store.connection.execute(f"DELETE FROM {table}")
        self.store.connection.execute("DROP TRIGGER immutable_experiment_sources_UPDATE")
        self.store.connection.execute("UPDATE experiment_sources SET payload_json='{}'")
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.store.run_plan(plan, "worker")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM local_jobs").fetchone()[0], 0)

    def test_policy_or_implementation_mutation_cannot_resume_but_report_remains_readable(self):
        plan = self.submit()
        original = experiment._implementation()
        modified = deepcopy(original)
        modified["source_version"] = "changed"
        with patch.object(experiment, "_implementation", return_value=modified):
            with self.assertRaisesRegex(ValueError, "implementation/policy mismatch"):
                self.store.run_plan(plan, "worker")
            self.assertEqual(self.store.report(plan)["plan_id"], plan)
        with patch.object(experiment.gate, "make_policy", return_value={"different": True}):
            with self.assertRaisesRegex(ValueError, "implementation/policy mismatch"):
                self.store.run_plan(plan, "worker")
        self.assertEqual(self.store.connection.execute("SELECT next_ordinal FROM experiment_schedule").fetchone()[0], 0)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM local_jobs").fetchone()[0], 0)

    def test_future_experiment_schema_rejected_before_child_tables_created(self):
        path = Path(self.directory.name)/"future.sqlite"
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE experiment_version(singleton INTEGER,version TEXT)")
            connection.execute("INSERT INTO experiment_version VALUES(1,'future')")
        before = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "unsupported experiment"):
            experiment.LocalExperimentStore(path)
        self.assertEqual(path.read_bytes(), before)
        with sqlite3.connect(path) as connection:
            self.assertIsNone(connection.execute("SELECT 1 FROM sqlite_master WHERE name='local_jobs'").fetchone())

    def test_inverse_scope_reports_base_and_effective_directions(self):
        candidate = next(row for row in experiment.formulas.catalog_records() if row["supported"] and row["orientation"] == "INVERSE")
        report = self.store.report(self.submit([scope("LONG", candidate=candidate["candidate_key"])]))
        self.assertEqual(report["scopes"][0]["base_direction"], "LONG")
        self.assertEqual(report["scopes"][0]["analysis_direction"], "SHORT")


if __name__ == "__main__":
    unittest.main()
