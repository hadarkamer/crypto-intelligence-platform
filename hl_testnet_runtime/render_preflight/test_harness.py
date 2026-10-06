"""Local checks of isolation and orchestration. No Render or exchange access."""
import hashlib
from http.server import ThreadingHTTPServer
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from . import core
from .server import handler_for


class HarnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="preflight-tests-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "candidate.py").write_text("VALUE = 1\n")
        self.manifest = self.root / "manifest.json"
        self.pin = core.create_manifest(self.root, self.manifest)

    def test_manifest_pins_exact_contents_and_base(self):
        identity = core.validate_manifest(self.root, self.manifest, self.pin)
        self.assertEqual(identity["file_count"], 1)
        self.assertEqual(identity["version_base"], core.BASE_VERSION)
        (self.root / "candidate.py").write_text("VALUE = 2\n")
        with self.assertRaisesRegex(core.PreflightError, "CANDIDATE_FILES_CHANGED"):
            core.validate_manifest(self.root, self.manifest, self.pin)

    def test_manifest_rejects_unpinned_and_added_code(self):
        with self.assertRaisesRegex(core.PreflightError, "MANIFEST_PIN_REQUIRED"):
            core.validate_manifest(self.root, self.manifest, "")
        (self.root / "new.py").write_text("pass\n")
        with self.assertRaisesRegex(core.PreflightError, "CANDIDATE_FILES_CHANGED"):
            core.validate_manifest(self.root, self.manifest, self.pin)

    def test_manifest_rejects_symlink_and_wrong_hash(self):
        with self.assertRaisesRegex(core.PreflightError, "MANIFEST_HASH_MISMATCH"):
            core.validate_manifest(self.root, self.manifest, "0" * 64)
        (self.root / "alias.py").symlink_to(self.root / "candidate.py")
        with self.assertRaisesRegex(core.PreflightError, "MANIFEST_SYMLINK_REFUSED"):
            core.input_files(self.root)

    def test_environment_has_no_copied_settings_or_secrets(self):
        with patch.dict(os.environ, {"RENDER_SERVICE_ID": "live", "HL_TESTNET_AGENT_KEY": "SECRET",
                                     "DATABASE_URL": "PRIVATE", "HOME": "/secret", "PATH": "/secret"}):
            env = core.clean_environment(self.root)
        self.assertFalse(set(env) & {"RENDER_SERVICE_ID", "HL_TESTNET_AGENT_KEY", "DATABASE_URL", "HOME"})
        self.assertNotIn("SECRET", json.dumps(env))
        self.assertNotIn("/secret", env["PATH"])

    def test_database_rejects_external_or_existing_local_name(self):
        valid = "postgresql://preflight:dummy@127.0.0.1:55432/hl_journal_ci"
        self.assertEqual(core.validate_ci_url(valid), valid)
        for invalid in (valid.replace("127.0.0.1", "db.render.com"),
                        valid.replace("hl_journal_ci", "production"),
                        valid + "?options=anything", valid.replace("preflight:", "owner:")):
            with self.assertRaises(core.PreflightError) as caught:
                core.clean_environment(self.root, invalid)
            self.assertNotIn("dummy", str(caught.exception))

    def test_no_postgres_root_bypass(self):
        pg = core.DisposablePostgres(self.root, self.root)
        with patch.object(core.os, "geteuid", return_value=0), patch.object(core.subprocess, "run") as run:
            self.assertIsNone(pg.start())
            run.assert_not_called()
        self.assertEqual(pg.status, "NOT_RUN_POSTGRES_REQUIRES_NONROOT")

    def test_postgres_missing_is_explicit_not_pass(self):
        pg = core.DisposablePostgres(self.root, self.root)
        with patch.object(core.os, "geteuid", return_value=1000):
            self.assertIsNone(pg.start())
        self.assertEqual(pg.status, "NOT_RUN_VERIFIED_POSTGRES186_UNAVAILABLE")

    def test_postgres_cleanup_failure_cannot_be_reported_as_stopped(self):
        pg = core.DisposablePostgres(self.root, self.root)
        pg.started = True
        with patch.object(pg, "_command", return_value={"exit_code": 1, "failure_code": "TIMEOUT"}):
            with self.assertRaisesRegex(core.PreflightError, "POSTGRES_CLEANUP_FAILED"):
                pg.stop()
        self.assertTrue(pg.started)

    def test_start_is_one_shot_under_concurrent_callers(self):
        runner = core.Runner(self.root, self.manifest, self.pin)
        entered, release = threading.Event(), threading.Event()
        calls = []
        def run():
            calls.append(1)
            entered.set()
            release.wait(2)
        with patch.object(runner, "_run", side_effect=run):
            threads = [threading.Thread(target=runner.start_once) for _ in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(2)
            self.assertTrue(entered.wait(2))
            release.set()
            runner.join(2)
            self.assertFalse(runner.start_once())
        self.assertEqual(calls, [1])

    def test_inherited_database_fails_before_child_or_postgres(self):
        runner = core.Runner(self.root, self.manifest, self.pin)
        with patch.dict(os.environ, {"DATABASE_URL": "NEVER_PRINT_THIS"}), \
                patch.object(core, "run_process") as run:
            runner.start_once()
            runner.join(5)
            run.assert_not_called()
        result = runner.snapshot()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["failure_code"], "INHERITED_DATABASE_SETTINGS_REFUSED")
        self.assertNotIn("NEVER_PRINT_THIS", json.dumps(result))

    def test_no_pg_path_runs_checks_serially_and_is_partial(self):
        runner = core.Runner(self.root, self.manifest, self.pin, allow_postgres=False)
        tasks = []
        def process(command, **kwargs):
            task = command[command.index("--task") + 1]
            tasks.append(task)
            self.assertNotIn("HL_JOURNAL_CI_URL", kwargs["env"])
            return {"exit_code": 0, "duration_seconds": 0.01, "failure_code": None}
        def report(task, _path):
            if task == "timing_tests":
                return {"tests_run": 28, "successful": True, "skipped": 0, "unexpected_successes": 0}
            return {"network_attempts_blocked": 0}
        with patch.dict(os.environ, {}, clear=True), patch.object(core, "run_process", side_effect=process), \
                patch.object(core, "summarize_report", side_effect=report):
            runner.start_once()
            runner.join(5)
        self.assertEqual(tasks, ["timing_tests", "recorder", "feed"])
        self.assertEqual(runner.snapshot()["status"], "PARTIAL")
        self.assertEqual(runner.snapshot()["durable_checks"], "NOT_RUN")

    def test_child_failure_stops_remaining_checks(self):
        runner = core.Runner(self.root, self.manifest, self.pin, allow_postgres=False)
        with patch.dict(os.environ, {}, clear=True), patch.object(core, "run_process", return_value={
                "exit_code": 1, "duration_seconds": 0.01, "failure_code": "CHILD_FAILED"}) as run:
            runner.start_once()
            runner.join(5)
            self.assertEqual(run.call_count, 1)
        self.assertEqual(runner.snapshot()["status"], "FAILED")

    def test_deadline_kills_child_without_printing_output(self):
        result = core.run_process([sys.executable, "-c", "import time; time.sleep(10)"],
            cwd=self.root, env=core.clean_environment(core.ROOT), log_path=self.root / "timeout.log", timeout=0.1)
        self.assertEqual(result["failure_code"], "TIMEOUT")
        self.assertLess(result["duration_seconds"], 3)

    def test_output_size_is_bounded(self):
        with patch.object(core, "MAX_LOG_BYTES", 2048):
            result = core.run_process([sys.executable, "-c",
                "import sys,time; sys.stdout.write('x'*4096); sys.stdout.flush(); time.sleep(10)"],
                cwd=self.root, env=core.clean_environment(core.ROOT), log_path=self.root / "limit.log", timeout=2)
        self.assertEqual(result["failure_code"], "OUTPUT_LIMIT")

    def test_summary_never_returns_unlisted_report_fields(self):
        path = self.root / "report.json"
        path.write_text(json.dumps({"tests_run": 28, "failures": 0, "errors": 0, "skipped": 0,
            "unexpected_successes": 0, "successful": True, "secret": "DONT_RETURN"}))
        summary = core.summarize_report("timing_tests", path)
        self.assertNotIn("DONT_RETURN", json.dumps(summary))
        self.assertEqual(summary["tests_run"], 28)


class GuardTests(unittest.TestCase):
    def child(self, code):
        return subprocess.run([sys.executable, "-c", code], cwd=core.ROOT,
                              env=core.clean_environment(core.ROOT), capture_output=True, text=True, timeout=5)

    def test_audit_guard_blocks_external_dns_and_connections_before_import(self):
        script = """from hl_testnet_runtime.render_preflight.child import install_network_guard
import socket
attempts=install_network_guard()
for operation in (lambda: socket.getaddrinfo('example.invalid',443),
                  lambda: socket.socket().connect(('203.0.113.7',443))):
    try: operation()
    except RuntimeError as exc: assert str(exc)=='PREFLIGHT_EXTERNAL_NETWORK_DISABLED'
    else: raise AssertionError('NETWORK_NOT_BLOCKED')
assert len(attempts)==2
print('BLOCKED')
"""
        result = self.child(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "BLOCKED")

    def test_guard_allows_only_loopback_test_socket(self):
        script = """from hl_testnet_runtime.render_preflight.child import install_network_guard
import socket
attempts=install_network_guard()
with socket.socket() as server:
    server.bind(('127.0.0.1',0)); server.listen(1)
    with socket.create_connection(server.getsockname(),timeout=1) as client:
        peer,_=server.accept();peer.close()
assert not attempts
"""
        result = self.child(script)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_replaced_env_python_descendant_keeps_network_guard(self):
        script = """from hl_testnet_runtime.render_preflight.child import install_network_guard
import subprocess,sys
install_network_guard()
program="import socket; socket.getaddrinfo('example.invalid',443)"
result=subprocess.run([sys.executable,'-c',program],env={},capture_output=True,text=True)
assert result.returncode != 0
assert 'PREFLIGHT_EXTERNAL_NETWORK_DISABLED' in result.stderr
"""
        result = self.child(script)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_psycopg_cannot_connect_without_fresh_ci_target(self):
        script = """from hl_testnet_runtime.render_preflight.child import install_network_guard
import psycopg
install_network_guard()
try: psycopg.connect(host='127.0.0.1',port=5432,dbname='production',user='owner')
except RuntimeError as exc: assert str(exc)=='PREFLIGHT_NONLOCAL_DATABASE_DISABLED'
else: raise AssertionError('DATABASE_NOT_BLOCKED')
"""
        result = self.child(script)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_platform_metadata_keeps_exact_local_uname_probe(self):
        script = """from hl_testnet_runtime.render_preflight.child import install_network_guard
import platform
attempts=install_network_guard()
assert platform.platform()
assert not attempts
"""
        result = self.child(script)
        self.assertEqual(result.returncode, 0, result.stderr)


class ServerTests(unittest.TestCase):
    def test_health_is_alive_on_failure_without_controls_or_file_serving(self):
        runner = core.Runner(core.ROOT, "unused", "unused")
        runner._update(status="FAILED", failure_code="SIMULATED")
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(runner))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for method, path, status in (("GET", "/healthz", 200), ("GET", "/summary", 200),
                    ("POST", "/run", 405), ("GET", "/../../AGENTS.md", 404), ("GET", "/summary?run=1", 404)):
                connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
                connection.request(method, path)
                response = connection.getresponse()
                self.assertEqual(response.status, status)
                response.read()
                connection.close()
            self.assertFalse(runner._started)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)


if __name__ == "__main__":
    unittest.main()
