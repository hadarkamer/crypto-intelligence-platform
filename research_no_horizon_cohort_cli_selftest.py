"""Actual local CLI contracts over genuine capture/intake/parent fixtures.

These tests only create temporary local files. They never execute generated SQL,
open a research store, fetch network data, or evaluate historical outcomes.
"""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_cli as cli
import research_no_horizon_cohort_coverage as coverage
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
from research_no_horizon_cohort_coverage_selftest import cohort_fixture, cross_part_rows
from research_no_horizon_parent_coverage_selftest import scope, source_row


class CohortCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = cohort_fixture()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.install(deepcopy(self.original))

    def write_json(self, name, value):
        path = self.root / name
        path.write_text(contracts.canonical(value), encoding="utf-8")
        return path

    def install(self, fixture):
        self.declaration, self.anchor, self.exports = fixture
        self.declaration_path = self.write_json("declaration.json", self.declaration)
        self.anchor_path = self.write_json("anchor.json", self.anchor)
        self.parts = [self.write_json(f"part_{ordinal}.json", export)
            for ordinal, export in enumerate(self.exports)]
        raw = deepcopy(self.anchor)
        raw.pop("anchor_sha256")
        for receipt in [raw["extraction_receipt"]] + [part["manifest"]["receipt"] for part in raw["parts"]]:
            receipt.pop("query_sha256")
        self.raw_anchor_path = self.write_json("raw_anchor.json", raw)

    def arguments(self, command, output, *, parts=None, anchor=None):
        args = [command, "--declaration", str(self.declaration_path), "--output", str(output)]
        if command != "anchor-sql":
            default = self.raw_anchor_path if command == "seal-anchor" else self.anchor_path
            args += ["--anchor", str(default if anchor is None else anchor)]
        if command == "coverage":
            for path in self.parts if parts is None else parts:
                args += ["--part", str(path)]
        return args

    def invoke(self, args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = cli.main(args)
        return status, stdout.getvalue(), stderr.getvalue()

    def test_anchor_sql_writes_exact_single_statement_from_declaration(self):
        output = self.root / "anchor.sql"
        status, stdout, stderr = self.invoke(self.arguments("anchor-sql", output))
        self.assertEqual((status, stdout, stderr), (0, "", ""))
        self.assertEqual(output.read_text(), cohort.anchor_sql(self.declaration))
        self.assertEqual(output.read_text().count(";"), 1)
        self.assertIn("ONE_STATEMENT_SNAPSHOT", output.read_text())

    def test_seal_anchor_preserves_parts_and_matches_real_sealing_api(self):
        output = self.root / "sealed.json"
        before = self.raw_anchor_path.read_bytes()
        status, _, stderr = self.invoke(self.arguments("seal-anchor", output))
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(output.read_text()), self.anchor)
        self.assertEqual(self.raw_anchor_path.read_bytes(), before)
        self.assertTrue(output.read_bytes().endswith(b"\n"))

    def test_coverage_possible_writes_complete_deterministic_receipt(self):
        output = self.root / "coverage.json"
        status, _, stderr = self.invoke(self.arguments("coverage", output))
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(output.read_text())
        expected = coverage.preflight_cohort(self.declaration, self.anchor, lambda ordinal: self.exports[ordinal])
        self.assertEqual(result, expected)
        self.assertEqual(result["accepted_source_rows"], 6)
        self.assertEqual(result["scopes"][0]["matched_parent_count"], 5)
        self.assertEqual(len(result["scopes"][0]["decision_ledger"]), 6)
        self.assertTrue(result["all_scopes_potentially_sufficient"])
        self.assertFalse(result["outcomes_evaluated"])
        self.assertFalse(result["outcome_runner_available"])
        self.assertIsNone(result["gate"])

    def test_insufficient_coverage_returns_two_and_keeps_required_empty_part(self):
        rows = cross_part_rows()
        rows[1] = []
        self.install(cohort_fixture(rows))
        output = self.root / "insufficient.json"
        status, _, stderr = self.invoke(self.arguments("coverage", output))
        self.assertEqual((status, stderr), (2, ""))
        result = json.loads(output.read_text())
        self.assertEqual(result["part_count"], 2)
        self.assertEqual(result["parts"][1]["accepted_source_rows"], 0)
        self.assertEqual(result["scopes"][0]["coverage_status"], "INSUFFICIENT")
        self.assertEqual(result["scopes"][0]["matched_parent_upper_bound"], 3)

    def test_unknown_later_source_returns_two_with_all_rows_in_blocked_receipt(self):
        rows = cross_part_rows()
        rows[1][1] = source_row(parent_offset=3, snapshot_set_id=5, unavailable=True)
        self.install(cohort_fixture(rows))
        output = self.root / "blocked.json"
        status, _, stderr = self.invoke(self.arguments("coverage", output))
        self.assertEqual((status, stderr), (2, ""))
        result = json.loads(output.read_text())
        self.assertEqual(result["accepted_source_rows"], 6)
        self.assertEqual(result["scopes"][0]["source_counts"]["UNKNOWN"], 1)
        self.assertEqual(len(result["scopes"][0]["decision_ledger"]), 6)
        self.assertEqual(result["scopes"][0]["coverage_status"], "BLOCKED")
        self.assertFalse(result["all_scopes_potentially_sufficient"])
        self.assertIsNone(result["scopes"][0]["matched_parent_upper_bound"])

    def test_one_possible_scope_does_not_make_cli_return_success_for_all(self):
        self.install(cohort_fixture(scopes=[scope(), scope("LONG")]))
        output = self.root / "mixed.json"
        status, _, stderr = self.invoke(self.arguments("coverage", output))
        self.assertEqual((status, stderr), (2, ""))
        result = json.loads(output.read_text())
        self.assertTrue(result["any_scope_potentially_sufficient"])
        self.assertFalse(result["all_scopes_potentially_sufficient"])
        self.assertEqual(len(result["scopes"]), 2)

    def test_missing_foreign_reordered_and_extra_parts_never_create_output(self):
        invalid_sets = (
            [self.parts[0]],
            [self.parts[0], self.root / "missing.json"],
            [self.parts[0], self.parts[0]],
            list(reversed(self.parts)),
            self.parts + [self.parts[0]],
        )
        for number, paths in enumerate(invalid_sets):
            with self.subTest(paths=[path.name for path in paths]):
                output = self.root / f"rejected_{number}.json"
                status, _, stderr = self.invoke(self.arguments("coverage", output, parts=paths))
                self.assertEqual(status, 1)
                self.assertTrue(stderr)
                self.assertFalse(output.exists())

    def test_existing_output_and_input_output_collision_preserve_original_bytes(self):
        for command in ("anchor-sql", "seal-anchor", "coverage"):
            for collision in (False, True):
                with self.subTest(command=command, collision=collision):
                    output = self.declaration_path if collision else self.root / f"existing_{command}.txt"
                    if not collision:
                        output.write_bytes(b"previous immutable evidence\n")
                    before = output.read_bytes()
                    status, _, stderr = self.invoke(self.arguments(command, output))
                    self.assertEqual(status, 1)
                    self.assertTrue(stderr)
                    self.assertEqual(output.read_bytes(), before)

    def test_strict_json_duplicate_and_nonfinite_input_rejected_without_output(self):
        targets = (("anchor-sql", self.declaration_path),
            ("seal-anchor", self.raw_anchor_path), ("coverage", self.parts[1]))
        for command, path in targets:
            original = path.read_bytes()
            for number, bad in enumerate((b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}')):
                with self.subTest(command=command, bad=bad):
                    path.write_bytes(bad)
                    output = self.root / f"bad_json_{command}_{number}.txt"
                    status, _, stderr = self.invoke(self.arguments(command, output))
                    self.assertEqual(status, 1)
                    self.assertTrue("duplicate" in stderr or "nonfinite" in stderr)
                    self.assertFalse(output.exists())
            path.write_bytes(original)

    def test_invalid_utf8_and_oversized_declaration_are_rejected_before_output(self):
        for number, data in enumerate((b"\xff", b" " * (transport.MAX_MANIFEST_BYTES + 1))):
            with self.subTest(case=number):
                self.declaration_path.write_bytes(data)
                output = self.root / f"invalid_bytes_{number}.sql"
                status, _, stderr = self.invoke(self.arguments("anchor-sql", output))
                self.assertEqual(status, 1)
                self.assertTrue(stderr)
                self.assertFalse(output.exists())

    def test_changed_anchor_and_resealing_rejected_before_output(self):
        output = self.root / "already_sealed.json"
        status, _, stderr = self.invoke(self.arguments("seal-anchor", output, anchor=self.anchor_path))
        self.assertEqual(status, 1)
        self.assertIn("ALREADY_SEALED", stderr)
        self.assertFalse(output.exists())
        changed = deepcopy(self.anchor)
        changed["parts"][1]["manifest"]["receipt"]["mvcc_snapshot"] = "other-snapshot"
        self.write_json(self.anchor_path.name, changed)
        output = self.root / "changed_anchor.json"
        status, _, stderr = self.invoke(self.arguments("coverage", output))
        self.assertEqual(status, 1)
        self.assertTrue(stderr)
        self.assertFalse(output.exists())

    def test_all_commands_are_local_and_never_open_database_or_network(self):
        targets = ("sqlite3.connect", "psycopg.connect", "socket.socket", "socket.create_connection",
            "requests.sessions.Session.request")
        with ExitStack() as stack:
            mocks = [stack.enter_context(patch(target, side_effect=AssertionError("CLI attempted external I/O")))
                for target in targets]
            for command in ("anchor-sql", "seal-anchor", "coverage"):
                status, _, stderr = self.invoke(self.arguments(command, self.root / f"local_{command}.txt"))
                self.assertEqual((status, stderr), (0, ""))
            for mocked in mocks:
                mocked.assert_not_called()


if __name__ == "__main__":
    unittest.main()
