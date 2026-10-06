"""Local checks of isolation and orchestration. No Render or exchange access."""
import hashlib
from http.server import ThreadingHTTPServer
import http.client
import io
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
from . import child
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
            Path(command[command.index("--output") + 1]).write_text('{}')
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

    def test_failed_suite_counts_and_ids_reach_summary_but_remain_failed(self):
        runner = core.Runner(self.root, self.manifest, self.pin, allow_postgres=False)
        def process(command, **_kwargs):
            output = Path(command[command.index('--output') + 1])
            output.write_text(json.dumps({'tests_run': 28, 'failures': 1, 'errors': 0, 'skipped': 0,
                'unexpected_successes': 0, 'successful': False, 'private': 'SECRET_DSN',
                'failure_details': [{'test_id': 'hl_testnet_runtime.test_fixture.Case.test_one',
                                     'exception_type': 'AssertionError', 'code': 'FILL_NOT_PROTECTED',
                                     'message': 'SECRET_DSN'}]}))
            return {'exit_code': 1, 'duration_seconds': 0.01, 'failure_code': 'CHILD_FAILED'}
        with patch.dict(os.environ, {}, clear=True), patch.object(core, 'run_process', side_effect=process), \
                patch.object(runner, '_log') as logs:
            runner.start_once()
            runner.join(5)
        report = runner.snapshot()
        self.assertEqual(report['status'], 'FAILED')
        stage = report['stages'][0]
        self.assertEqual(stage['failure_code'], 'CHILD_FAILED')
        self.assertEqual((stage['tests_run'], stage['failures']), (28, 1))
        self.assertEqual(stage['failure_details'][0]['code'], 'FILL_NOT_PROTECTED')
        self.assertNotIn('SECRET_DSN', json.dumps(report))
        self.assertNotIn('SECRET_DSN', str(logs.call_args_list))

    def test_benchmark_failure_report_uses_only_type_and_known_code(self):
        path = self.root / 'failure.json'
        child.write_failure_report(path, RuntimeError('PREFLIGHT_SUBPROCESS_REFUSED'))
        safe = core.summarize_report('runtime', path)
        self.assertFalse(safe['successful'])
        self.assertEqual(safe['exception_type'], 'RuntimeError')
        self.assertEqual(safe['code'], 'PREFLIGHT_SUBPROCESS_REFUSED')

    def test_exception_message_is_never_stringified_or_exported(self):
        class SensitiveError(Exception):
            def __str__(self):
                raise AssertionError('MUST_NOT_STRINGIFY')
        path = self.root / 'failure.json'
        child.write_failure_report(path, SensitiveError('postgresql://user:SECRET@host/db'))
        safe = core.summarize_report('runtime', path)
        self.assertEqual(safe['exception_type'], 'OtherError')
        self.assertEqual(safe['code'], 'UNCLASSIFIED')
        self.assertNotIn('SECRET', path.read_text())

    def test_failure_details_ids_codes_and_count_are_bounded(self):
        path = self.root / 'report.json'
        path.write_text(json.dumps({'tests_run': 100, 'failures': 100, 'errors': 0, 'skipped': 0,
            'unexpected_successes': 0, 'successful': False,
            'failure_details': [{'test_id': 'hl_testnet_runtime.Case.test_x(password=SECRET)',
                'exception_type': 'SECRET_CLASS', 'code': 'SECRET_CODE', 'traceback': 'SECRET'}] * 40}))
        safe = core.summarize_report('full_suite', path)
        self.assertEqual(len(safe['failure_details']), 16)
        self.assertEqual(safe['failure_details'][0], {'test_id': 'unidentified_test',
            'exception_type': 'OtherError', 'code': 'UNCLASSIFIED'})
        self.assertNotIn('SECRET', json.dumps(safe))

    def test_unittest_result_captures_safe_id_and_class_without_message(self):
        class FailingCase(unittest.TestCase):
            def test_failure(self):
                raise ValueError('SECRET_DSN')
        case = FailingCase('test_failure')
        case.id = lambda: 'hl_testnet_runtime.test_fixture.Case.test_failure'
        result = unittest.TextTestRunner(stream=io.StringIO(), resultclass=child.DiagnosticTestResult).run(
            unittest.TestSuite([case]))
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.failure_details, [{'test_id': 'hl_testnet_runtime.test_fixture.Case.test_failure',
            'exception_type': 'ValueError', 'code': 'UNCLASSIFIED'}])
        self.assertNotIn('SECRET_DSN', json.dumps(result.failure_details))

    def test_child_exception_creates_failure_report(self):
        path = self.root / 'failure.json'
        with patch.object(sys, 'argv', ['child', '--task', 'runtime', '--output', str(path)]), \
                patch.object(child, 'run_task', side_effect=RuntimeError('FRESH_DATABASE_REQUIRED')):
            self.assertEqual(child.main(), 1)
        self.assertEqual(json.loads(path.read_text())['code'], 'FRESH_DATABASE_REQUIRED')


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
