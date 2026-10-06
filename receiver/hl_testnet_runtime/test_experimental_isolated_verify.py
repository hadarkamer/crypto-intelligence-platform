"""Boundaries of the offline verification runner; no external I/O."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import experimental_isolated_verify as verify


class IsolatedVerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="isolated-runner-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "source"
        self.root.mkdir()
        (self.root / "sample.py").write_text("VALUE=1\n")

    def test_snapshot_copies_code_and_requirements_but_not_settings_or_data(self):
        (self.root / "requirements.txt").write_text("psycopg==3.2.3\n")
        (self.root / ".env").write_text("not-a-real-secret")
        (self.root / "data.json").write_text("{}")
        destination = self.root.parent / "snapshot"
        result = verify.snapshot_sources(self.root, destination)
        self.assertEqual(set(result["files"]), {"sample.py", "requirements.txt"})
        self.assertEqual(result["sha256"], verify.file_digest(result["files"]))
        self.assertFalse((destination / ".env").exists())
        self.assertFalse((destination / "data.json").exists())

    def test_snapshot_never_follows_source_symlink(self):
        (self.root / "alias.py").symlink_to(self.root / "sample.py")
        with self.assertRaisesRegex(verify.core.PreflightError, "SYMLINK_REFUSED"):
            verify.snapshot_sources(self.root, self.root.parent / "snapshot")

    def test_only_named_regression_fixture_is_copied_and_hashed(self):
        name = verify.TEST_FIXTURES[0]
        (self.root / name).write_text("[]")
        (self.root / "database-export.json").write_text("{}")
        result = verify.snapshot_sources(self.root, self.root.parent / "snapshot")
        self.assertIn(name, result["files"])
        self.assertNotIn("database-export.json", result["files"])
        self.assertTrue((self.root.parent / "snapshot" / name).is_file())

    def test_named_fixture_symlink_is_refused(self):
        (self.root / verify.TEST_FIXTURES[0]).symlink_to(self.root / "sample.py")
        with self.assertRaisesRegex(verify.core.PreflightError, "SYMLINK_REFUSED"):
            verify.candidate_files(self.root)

    def test_snapshot_detects_edit_during_copy(self):
        actual = verify.core.input_files(self.root)
        changed = {**actual, "added.py": "0" * 64}
        with patch.object(verify.core, "input_files", side_effect=[actual, changed]):
            with self.assertRaisesRegex(verify.core.PreflightError, "CHANGED_DURING_SNAPSHOT"):
                verify.snapshot_sources(self.root, self.root.parent / "snapshot")

    def test_producer_selection_keeps_required_tests_and_new_execution_tests(self):
        (self.root / "experimental_execution_transport_selftest.py").write_text("pass\n")
        selected = verify.producer_modules(self.root)
        self.assertIn("experimental_execution_transport_selftest", selected)
        self.assertIn("sol_g65_experimental_selftest", selected)
        self.assertIn("hype_row71205_experimental_store_selftest", selected)

    def test_skipped_postgres_never_reported_passed(self):
        row = dict(exit_code=0, successful=True, network_attempts_blocked=0, skipped=1)
        self.assertEqual(verify.final_status([row], True), "PARTIAL_POSTGRESQL_NOT_VERIFIED")
        self.assertEqual(verify.final_status([{**row, "skipped": 0}], True), "PASSED")

    def test_failure_network_attempt_or_changed_snapshot_cannot_report_success(self):
        row = dict(exit_code=0, successful=True, network_attempts_blocked=0, skipped=0)
        for bad in ({**row, "exit_code": 1}, {**row, "successful": False},
                    {**row, "network_attempts_blocked": 1}):
            self.assertEqual(verify.final_status([bad], True), "FAILED")
        self.assertEqual(verify.final_status([row], False), "FAILED")

    def test_inherited_secrets_and_database_settings_never_reach_child(self):
        with patch.dict(verify.os.environ, {"DATABASE_URL": "FAKE", "TEST_DATABASE_URL": "FAKE",
                                          "HL_TESTNET_AGENT_KEY": "FAKE"}):
            environment = verify.core.clean_environment(self.root)
        self.assertFalse({"DATABASE_URL", "TEST_DATABASE_URL", "HL_TESTNET_AGENT_KEY"} & set(environment))
        self.assertNotIn("FAKE", json.dumps(environment))

    def test_child_rejects_unexpected_environment_before_suite_discovery(self):
        with patch.dict(verify.os.environ, {"DATABASE_URL": "FAKE"}, clear=True):
            with self.assertRaisesRegex(verify.core.PreflightError, "UNEXPECTED_CHILD_ENVIRONMENT"):
                verify.run_child("runtime", self.root, self.root / "report.json")

    def test_optional_postgres_refuses_root_without_invoking_binary(self):
        postgres = verify.core.DisposablePostgres(self.root, self.root)
        with patch.object(verify.core.os, "geteuid", return_value=0), \
                patch.object(verify.core.subprocess, "run") as run:
            self.assertIsNone(postgres.start())
            run.assert_not_called()
        self.assertEqual(postgres.status, "NOT_RUN_POSTGRES_REQUIRES_NONROOT")

    def test_producer_database_created_only_for_started_fresh_server(self):
        url = "postgresql://preflight:synthetic@127.0.0.1:55432/hl_journal_ci"
        postgres = SimpleNamespace(started=True, ci_url=url, status="FRESH_LOOPBACK_POSTGRES18",
            directory=self.root, _command=lambda args: {"exit_code": 0, "failure_code": None})
        result = verify.create_producer_database(postgres, url)
        self.assertEqual(result, url.replace("/hl_journal_ci", "/test_experimental_ci"))
        postgres.started = False
        with self.assertRaisesRegex(verify.core.PreflightError, "FRESH_DATABASE_REQUIRED"):
            verify.create_producer_database(postgres, url)

    def test_producer_create_failure_is_not_configuration_success(self):
        url = "postgresql://preflight:synthetic@127.0.0.1:55432/hl_journal_ci"
        postgres = SimpleNamespace(started=True, ci_url=url, status="FRESH_LOOPBACK_POSTGRES18",
            directory=self.root, _command=lambda args: {"exit_code": 1, "failure_code": None})
        with self.assertRaisesRegex(verify.core.PreflightError, "PRODUCER_DATABASE_CREATE_FAILED"):
            verify.create_producer_database(postgres, url)

    def test_database_stops_before_temporary_directory_removed(self):
        directory = self.root / "temporary"
        directory.mkdir()
        postgres = SimpleNamespace(started=True)
        def stop():
            self.assertTrue(directory.exists())
            postgres.started = False
        postgres.stop = stop
        report = {"status": "PASSED"}
        verify.cleanup_database(postgres, directory, report)
        self.assertEqual(report, {"status": "PASSED", "postgresql_cleanup": "STOPPED"})
        self.assertFalse(directory.exists())

    def test_database_stop_failure_retains_directory_and_fails_report(self):
        directory = self.root / "temporary"
        directory.mkdir()
        def stop():
            raise RuntimeError("synthetic stop failure")
        postgres = SimpleNamespace(started=True, stop=stop)
        report = {"status": "PASSED"}
        verify.cleanup_database(postgres, directory, report)
        self.assertEqual(report["status"], "FAILED")
        self.assertEqual(report["postgresql_cleanup"], "FAILED")
        self.assertTrue(directory.exists())

    def test_runtime_child_rejects_producer_database_before_discovery(self):
        with patch.dict(verify.os.environ, {"TEST_DATABASE_URL": "FAKE"}, clear=True):
            with self.assertRaisesRegex(verify.core.PreflightError, "UNEXPECTED_PRODUCER_DATABASE"):
                verify.run_child("runtime", self.root, self.root / "report.json")
