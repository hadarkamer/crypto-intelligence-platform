"""Exercise experiment commands with genuine captured-score source fixtures."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import research_no_horizon_experiment_cli as cli
from research_no_horizon_experiment import LocalExperimentStore
from research_no_horizon_source_selftest import fixture
import research_watch_scan_formula as formulas


class ExperimentCommands(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.base = Path(self.directory.name)
        self.database = self.base / "experiment.sqlite"
        self.export = self.base / "source.json"
        self.export.write_text(formulas.canonical(fixture()), encoding="utf-8")
        self.scopes = self.base / "scopes.json"
        self.declarations = [
            {"candidate_key": "FUTURES_CVD_TOTAL_65", "base_direction": "LONG", "threshold_pct": 1.5},
            {"candidate_key": "FUTURES_CVD_TOTAL_65", "base_direction": "SHORT", "threshold_pct": 1.5},
            {"candidate_key": "FUTURES_CVD_TOTAL_65", "base_direction": "SHORT", "threshold_pct": 3.0},
        ]
        self.scopes.write_text(json.dumps(self.declarations), encoding="utf-8")

    def tearDown(self):
        self.directory.cleanup()

    def command(self, *args):
        return subprocess.run([sys.executable, cli.__file__, "--database", str(self.database), *args],
            capture_output=True, text=True)

    def submit(self):
        result = self.command("submit", str(self.export), "--scopes", str(self.scopes), "--plan-key", "fixture-plan")
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)["plan_id"]

    def test_missing_database_run_and_report_never_create_it(self):
        output = self.base / "missing.json"
        for args in (("run", "unknown", "--worker-id", "test"),
                     ("report", "unknown", "--output", str(output))):
            result = self.command(*args)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(self.database.exists())
            self.assertFalse(output.exists())

    def test_genuine_source_bounded_restart_and_full_report_export(self):
        plan = self.submit()
        self.assertEqual(self.submit(), plan)
        before = self.base / "before.json"
        result = self.command("report", plan, "--output", str(before))
        self.assertEqual(result.returncode, 0, result.stderr)
        initial = json.loads(before.read_text(encoding="utf-8"))
        self.assertEqual(len(initial["scopes"]), 3)
        self.assertFalse(initial["all_scopes_processed"])
        self.assertTrue(all(row["status"] == "NOT_SUBMITTED" and row["gate"] is None
                            for row in initial["scopes"]))
        # Each command is a fresh process, exercising durable restart rather
        # than retaining an in-memory coordinator between bounded invocations.
        for _ in range(16):
            run = self.command("run", plan, "--worker-id", "fixture-worker",
                "--scope-budget", "1", "--candle-budget", "1", "--batch-size", "1")
            self.assertEqual(run.returncode, 0, run.stderr)
            progress = json.loads(run.stdout)
            self.assertLessEqual(progress["candle_evaluations_this_run"], 1)
            self.assertLessEqual(progress["attempted_scopes"], 1)
            self.assertEqual(progress["attempted_scopes"], len(progress["attempted_ordinals"]))
            self.assertEqual(len(progress["scopes"]), 3)
        output = self.base / "final.json"
        result = self.command("report", plan, "--output", str(output))
        self.assertEqual(result.returncode, 0, result.stderr)
        saved = output.read_bytes()
        actual = json.loads(saved)
        with LocalExperimentStore(self.database) as store:
            self.assertEqual(actual, store.report(plan))
        self.assertNotEqual(actual, json.loads(before.read_text(encoding="utf-8")))
        self.assertFalse(actual["trading_authorized"])
        self.assertTrue(actual["all_scopes_processed"])
        self.assertTrue(actual["source_coverage_complete"])
        self.assertTrue(actual["computation_complete"])
        rows = {(row["base_direction"], row["threshold_pct"]): row for row in actual["scopes"]}
        self.assertEqual(rows[("LONG", 1.5)]["source_counts"]["NO_MATCH"], 1)
        self.assertEqual(rows[("LONG", 1.5)]["gate"]["selected_parents"], 0)
        self.assertEqual(rows[("SHORT", 1.5)]["status_counts"]["SUCCESS"], 1)
        self.assertEqual(rows[("SHORT", 3.0)]["status_counts"]["OPEN"], 1)
        for row in actual["scopes"]:
            self.assertFalse(row["gate"]["experimental_eligible"])
            self.assertLessEqual(row["gate"]["selected_parents"], 1)
        # Both partial and completed reports survive subsequent export attempts.
        for path in (before, output):
            original = path.read_bytes()
            self.assertEqual(self.command("report", plan, "--output", str(path)).returncode, 2)
            self.assertEqual(path.read_bytes(), original)

    def test_duplicate_nonfinite_and_oversized_input_rejected_before_database(self):
        invalid = (
            ('[{"candidate_key":"a","candidate_key":"b"}]', "duplicate JSON key"),
            ('[{"threshold_pct":NaN}]', "nonfinite JSON value"),
            ('[{"threshold_pct":1e999}]', "nonfinite JSON value"),
            (" " * (cli.MAX_SCOPES_BYTES + 1), "bounded byte limit"),
            (json.dumps([self.declarations[0]] * 65), "between 1 and 64"),
        )
        for payload, message in invalid:
            with self.subTest(message=message):
                self.scopes.write_text(payload, encoding="utf-8")
                result = self.command("submit", str(self.export), "--scopes", str(self.scopes))
                self.assertEqual(result.returncode, 2)
                self.assertIn(message, result.stderr)
                self.assertFalse(self.database.exists())
        self.scopes.write_text(json.dumps(self.declarations), encoding="utf-8")
        self.export.write_text('{"symbol":"BTC","symbol":"ETH"}', encoding="utf-8")
        result = self.command("submit", str(self.export), "--scopes", str(self.scopes))
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate JSON key", result.stderr)
        self.assertFalse(self.database.exists())


if __name__ == "__main__":
    unittest.main()
