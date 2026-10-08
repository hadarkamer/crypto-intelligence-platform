"""No-network acquisition CLI gates, output preservation and sanitized errors."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import research_no_horizon_acquisition_cli as cli
import research_no_horizon_cohort as cohort
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


class AcquisitionCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.declaration = cohort_fixture()[0]

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.input = self.root / "declaration.json"
        self.input.write_text(json.dumps(self.declaration), encoding="utf-8")
        self.output = self.root / "receipt.json"
        self.env = patch.dict(os.environ,
            {"RESEARCH_NO_HORIZON_DATABASE_URL": "postgresql://destination-test"}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def invoke(self, args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = cli.main([*args, "--output", str(self.output)])
        return status, out.getvalue(), err.getvalue()

    def backend(self):
        backend = MagicMock()
        backend.register_request.return_value = "request"
        backend.report.return_value = {"request_id": "request", "status": "WAITING"}
        return backend

    def test_register_normalizes_before_destination_and_does_not_read_source(self):
        backend = self.backend()
        with patch.object(cli, "_connect", return_value=MagicMock()) as connect, \
             patch.object(cli, "_connect_source", side_effect=AssertionError("source connected")), \
             patch.object(cli.acquisition, "schema_status", return_value={"schema_present": True}), \
             patch.object(cli.acquisition, "AcquisitionStore", return_value=backend):
            status, _, err = self.invoke(["register", "--declaration", str(self.input), "--request-key", "named"])
        self.assertEqual((status, err), (0, ""))
        connect.assert_called_once_with("postgresql://destination-test")
        backend.register_request.assert_called_once_with(
            cohort.normalize_declaration(self.declaration), request_key="named")
        self.assertEqual(json.loads(self.output.read_text())["request_id"], "request")

    def test_invalid_declaration_and_existing_output_fail_before_database(self):
        self.input.write_text("{}")
        with patch.object(cli, "_connect", side_effect=AssertionError("connected")) as connect:
            status, _, _ = self.invoke(["register", "--declaration", str(self.input)])
            self.assertEqual(status, 1)
            self.output.write_text("existing evidence")
            status, _, _ = self.invoke(["report", "request"])
            self.assertEqual(status, 1)
        connect.assert_not_called()
        self.assertEqual(self.output.read_text(), "existing evidence")

    def test_run_requires_read_database_and_rejects_unbounded_budget(self):
        with patch.object(cli, "_connect", side_effect=AssertionError("connected")) as connect:
            status, _, _ = self.invoke(["run", "--worker-id", "worker"])
            self.assertEqual(status, 1)
            os.environ["RESEARCH_NO_HORIZON_READ_DATABASE_URL"] = "configured"
            status, _, _ = self.invoke(["run", "--worker-id", "worker", "--leaf-budget", "33"])
            self.assertEqual(status, 1)
        connect.assert_not_called()

    def test_idle_queue_keeps_source_factory_lazy_and_records_no_work(self):
        backend = self.backend()
        backend.run_once.return_value = None
        os.environ["RESEARCH_NO_HORIZON_READ_DATABASE_URL"] = "postgresql://source-test"
        with patch.object(cli, "_connect", return_value=MagicMock()), \
             patch.object(cli, "_connect_source", side_effect=AssertionError("source connected")), \
             patch.object(cli.acquisition, "schema_status", return_value={"schema_present": True}), \
             patch.object(cli.acquisition, "AcquisitionStore", return_value=backend):
            status, _, err = self.invoke(["run", "--worker-id", "worker", "--request-id", "request"])
        self.assertEqual((status, err), (0, ""))
        self.assertFalse(json.loads(self.output.read_text())["claimed"])
        self.assertEqual(backend.run_once.call_args.kwargs["request_id"], "request")

    def test_blocked_report_exports_proofs_only_when_requested(self):
        backend = self.backend()
        backend.report.return_value = {"request_id": "request", "status": "BLOCKED", "proofs": []}
        with patch.object(cli, "_connect", return_value=MagicMock()), \
             patch.object(cli.acquisition, "schema_status", return_value={"schema_present": True}), \
             patch.object(cli.acquisition, "AcquisitionStore", return_value=backend):
            status, _, err = self.invoke(["report", "request", "--include-proofs"])
        self.assertEqual((status, err), (2, ""))
        backend.report.assert_called_once_with("request", include_proofs=True)

    def test_driver_failure_never_prints_credentials_or_source(self):
        with patch.object(cli, "_connect", side_effect=RuntimeError("postgresql://secret/private-payload")):
            status, out, err = self.invoke(["report", "request"])
        self.assertEqual(status, 1)
        self.assertEqual(out, "")
        self.assertEqual(err, "ACQUISITION_FAILED: RuntimeError\n")
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
