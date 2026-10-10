"""Actual local subprocess lifecycle tests; all receipts/children are synthetic."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import research_no_horizon_worker_supervisor as supervisor
import research_no_horizon_registered_runner as runner


HARNESS = r'''
import json, signal, sys
from pathlib import Path
from types import SimpleNamespace
import research_no_horizon_worker_supervisor as module
config=json.loads(Path(sys.argv[1]).read_text())
for key in ('checkpoint','adapter_dir','work_dir','state_dir','preflight_receipt'):
    config[key]=Path(config[key])
args=SimpleNamespace(**config)
stop=module.Stop()
signal.signal(signal.SIGTERM,stop.set)
signal.signal(signal.SIGINT,stop.set)
def identity(args, completed=False):
    if getattr(args,'fixture_preflight_retry',False):
        path=args.state_dir/'fixture-preflight-attempts'
        attempts=int(path.read_text())+1 if path.exists() else 1
        path.write_text(str(attempts))
        if attempts==1: raise module.PreflightRetry('OperationalError')
    if getattr(args,'fixture_preflight_failure',False):
        raise ValueError('synthetic permanent configuration failure')
    return {'fixture':'SYNTHETIC_SUBPROCESS_ONLY'}
raise SystemExit(module.supervise(args,stop,identity_validator=identity,
    command=[sys.executable,'-u',sys.argv[2],str(args.work_dir)]))
'''


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="no-horizon-supervisor-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.work = self.root / "work"
        self.work.mkdir()
        self.state = self.root / "supervisor"
        self.config = {"checkpoint": str(self.root / "missing-checkpoint"),
            "adapter_dir": str(self.root / "missing-adapter"), "work_dir": str(self.work),
            "state_dir": str(self.state), "preflight_receipt": str(self.root / "preflight.json"),
            "source_id": "SYNTHETIC", "source_url_env": "SUPERVISOR_TEST_SECRET_SOURCE",
            "node": "unused", "interval_seconds": 300, "heartbeat_seconds": .1,
            "shutdown_seconds": 1, "enable_worker": True}
        self.processes = []
        self.addCleanup(self.cleanup_processes)

    def cleanup_processes(self):
        for proc in self.processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            proc.stdout.close()
            proc.stderr.close()

    def launch(self, script, *, config=None):
        suffix = str(len(self.processes))
        script_path = self.root / ("child" + suffix + ".py")
        script_path.write_text(script)
        config_path = self.root / ("config" + suffix + ".json")
        config_path.write_text(json.dumps(config or self.config))
        proc = subprocess.Popen([sys.executable, "-u", "-c", HARNESS,
            str(config_path), str(script_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=Path(supervisor.__file__).parent)
        self.processes.append(proc)
        return proc

    def wait_for(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(.025)
        self.fail("Timed out waiting for bounded subprocess condition")

    def heartbeat(self):
        path = self.state / "heartbeat.json"
        return json.loads(path.read_bytes()) if path.exists() else {}

    def receipt(self, *, terminal=True, errors=0):
        receipts = self.work / "receipts"
        receipts.mkdir(exist_ok=True)
        snapshot = self.work / "working_registry_snapshot.tar.gz"
        snapshot.write_bytes(b"SYNTHETIC SNAPSHOT, NOT A REAL REGISTRY")
        groups = [{"group": str(number), "status": "SELECTED" if terminal else "WAITING_FOR_BOTH_WINDOW_REPORTS",
                   "requests": [{"status": "WAITING", "not_before_utc": "2099-01-01T00:00:00+00:00"}]}
                  for number in range(4)]
        tick = {"request_denominator": 8, "group_denominator": 4,
            "original_registration_times_preserved": True, "source_transport": runner.MODE,
            "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False,
            "groups": groups, "selection_complete": terminal,
            "errors": [{"error_type": "SyntheticOperationalError"}] * errors}
        tick["tick_sha256"] = supervisor.contracts.digest(tick)
        body = {"version": runner.VERSION, "transport": runner.MODE,
            "clean_registry_shutdown": True, "telegram_authorized": False, "trading_authorized": False,
            "working_snapshot_sha256": supervisor._sha_file(snapshot), "native_tick": tick}
        path = receipts / ("20261008T000000-" + "a" * 32 + ".json")
        runner.atomic_json(path, body)
        output = {"receipt": str(path), "terminal": terminal, "clean_registry_shutdown": True}
        return path, output

    def test_disabled_cli_parks_with_absent_inputs_and_exact_enable_value(self):
        command = [sys.executable, str(Path(supervisor.__file__)),
            "--checkpoint", self.config["checkpoint"], "--adapter-dir", self.config["adapter_dir"],
            "--work-dir", str(self.work), "--state-dir", str(self.state),
            "--source-id", "SYNTHETIC", "--heartbeat-seconds", "1"]
        env = dict(os.environ, NO_HORIZON_SUPERVISOR_ENABLED="true",
                   RESEARCH_NO_HORIZON_READ_DATABASE_URL="secret-must-not-be-read")
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        self.processes.append(proc)
        self.wait_for(lambda: self.heartbeat().get("state") == "DISABLED")
        self.assertIsNone(proc.poll())
        self.assertFalse(Path(self.config["checkpoint"]).exists())
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 0)
        self.assertEqual(self.heartbeat()["state"], "STOPPED")
        logs = [json.loads(line) for line in proc.stdout.read().splitlines()]
        self.assertEqual([row["state"] for row in logs], ["DISABLED", "STOPPED"])
        self.assertNotIn(b"secret", proc.stderr.read())

    def test_terminal_completion_parks_and_restart_does_not_start_another_tick(self):
        _, output = self.receipt()
        script = """
import json,sys
from pathlib import Path
work=Path(sys.argv[1])
with (work/'starts').open('a') as stream: stream.write('start\\n')
print(%r,flush=True)
""" % json.dumps(output)
        proc = self.launch(script)
        self.wait_for(lambda: self.heartbeat().get("state") == "TERMINAL_HOLD")
        self.assertIsNone(proc.poll())
        self.assertTrue((self.state / "terminal.json").exists())
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 0)
        second = self.launch("raise RuntimeError('MUST_NOT_RUN_AFTER_COMPLETION')")
        self.wait_for(lambda: self.heartbeat().get("state") == "TERMINAL_HOLD"
                      and self.heartbeat().get("supervisor_pid") == second.pid)
        self.assertEqual((self.work / "starts").read_text(), "start\n")
        self.assertIsNone(second.poll())
        second.send_signal(signal.SIGINT)
        self.assertEqual(second.wait(timeout=5), 0)

    def test_leader_term_drains_before_database_grandchild_exits(self):
        # The grandchild stands in for the DB. It would record an unsafe early
        # process-group TERM; the leader is responsible for stopping it after
        # durable work finishes.
        script = r'''
import json,signal,subprocess,sys,time
from pathlib import Path
work=Path(sys.argv[1])
database_code="""
import signal,sys,time
from pathlib import Path
root=Path(sys.argv[1])
def unsafe(*_):
    (root/'unsafe-group-term').write_text('unsafe')
    raise SystemExit(0)
signal.signal(signal.SIGTERM,unsafe)
(root/'database-ready').write_text('ready')
while True: time.sleep(.02)
"""
db=subprocess.Popen([sys.executable,'-u','-c',database_code,str(work)])
def drain(signum,*_):
    (work/'received-signal').write_text(str(signum))
    time.sleep(.15)
    (work/'durable-unit').write_text('committed')
    db.kill();db.wait()
    raise SystemExit(0)
signal.signal(signal.SIGTERM,drain)
signal.signal(signal.SIGINT,drain)
(work/'leader-ready').write_text('ready')
while True: time.sleep(.02)
'''
        proc = self.launch(script)
        self.wait_for(lambda: (self.work / "leader-ready").exists() and (self.work / "database-ready").exists())
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 0)
        self.assertEqual((self.work / "received-signal").read_text(), str(signal.SIGTERM))
        self.assertEqual((self.work / "durable-unit").read_text(), "committed")
        self.assertFalse((self.work / "unsafe-group-term").exists())
        self.assertEqual(self.heartbeat()["state"], "STOPPED")

    def test_uncooperative_child_is_killed_within_shutdown_budget(self):
        script = """
import signal,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path(sys.argv[1],'ready').write_text('ready')
while True: time.sleep(.02)
"""
        config = {**self.config, "shutdown_seconds": .15}
        proc = self.launch(script, config=config)
        self.wait_for(lambda: (self.work / "ready").exists())
        began = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 1)
        self.assertLess(time.monotonic() - began, 4)
        self.assertEqual(self.heartbeat()["state"], "STOPPED_UNCLEAN")
        self.assertEqual(self.heartbeat()["error_type"], "ShutdownDeadlineExceeded")

    def test_dead_leader_with_pipe_holding_descendant_cannot_hang_supervisor(self):
        script = r'''
import subprocess,sys,time
from pathlib import Path
work=Path(sys.argv[1])
code="""
import signal,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path(sys.argv[1],'descendant-ready').write_text('ready')
while True: time.sleep(.02)
"""
subprocess.Popen([sys.executable,'-u','-c',code,str(work)])
while not (work/'descendant-ready').exists(): time.sleep(.02)
# Exit without reaping descendant; it retains both inherited pipes.
'''
        proc = self.launch(script, config={**self.config, "shutdown_seconds": .15})
        self.wait_for(lambda: self.heartbeat().get("state") == "ERROR_HOLD", timeout=6)
        self.assertEqual(self.heartbeat()["error_type"], "ShutdownDeadlineExceeded")
        self.assertIsNone(proc.poll())
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 0)

    def test_duplicate_supervisor_lock_prevents_second_child(self):
        first = self.launch("raise RuntimeError('disabled must not launch')", config={**self.config, "enable_worker": False})
        self.wait_for(lambda: self.heartbeat().get("state") == "DISABLED")
        second = self.launch("raise RuntimeError('second child must not launch')")
        self.assertNotEqual(second.wait(timeout=5), 0)
        self.assertIsNone(first.poll())
        self.assertEqual(self.heartbeat()["supervisor_pid"], first.pid)

    def test_operational_error_observable_and_child_secrets_not_forwarded(self):
        _, output = self.receipt(terminal=False, errors=1)
        script = """
import signal,sys,time
signal.signal(signal.SIGTERM,lambda *_:sys.exit(0))
print(%r,flush=True)
print('source-private-password',file=sys.stderr,flush=True)
while True: time.sleep(.02)
""" % json.dumps(output)
        proc = self.launch(script)
        self.wait_for(lambda: self.heartbeat().get("state") == "OPERATIONAL_ERRORS")
        self.assertEqual(self.heartbeat()["progress"]["error_count"], 1)
        self.assertEqual(self.heartbeat()["progress"]["next_due_utc"], "2099-01-01T00:00:00+00:00")
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 0)
        logs = proc.stdout.read()
        self.assertNotIn(b"source-private-password", logs)
        self.assertTrue(all(json.loads(line)["event"] == "RESEARCH_WORKER_STATE" for line in logs.splitlines()))
        self.assertNotIn(b"source-private-password", proc.stderr.read())
        self.assertNotIn("password", (self.state / "heartbeat.json").read_text())

    def test_changed_terminal_snapshot_holds_error_without_reacquisition(self):
        _, output = self.receipt()
        proc = self.launch("print(%r,flush=True)" % json.dumps(output))
        self.wait_for(lambda: self.heartbeat().get("state") == "TERMINAL_HOLD")
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=5), 0)
        (self.work / "working_registry_snapshot.tar.gz").write_bytes(b"changed")
        second = self.launch("raise RuntimeError('must not reacquire after changed state')")
        self.wait_for(lambda: self.heartbeat().get("state") == "ERROR_HOLD"
                      and self.heartbeat().get("supervisor_pid") == second.pid)
        self.assertIsNone(second.poll())

    def test_bad_tick_hash_cannot_create_completion_latch(self):
        path, output = self.receipt()
        receipt = json.loads(path.read_bytes())
        receipt["native_tick"]["tick_sha256"] = "0" * 64
        runner.atomic_json(path, receipt)
        proc = self.launch("print(%r,flush=True)" % json.dumps(output))
        self.wait_for(lambda: self.heartbeat().get("state") == "ERROR_HOLD")
        self.assertFalse((self.state / "terminal.json").exists())
        self.assertIsNone(proc.poll())

    def test_transient_preflight_retries_then_starts_once(self):
        _, output = self.receipt()
        config = {**self.config, "fixture_preflight_retry": True, "interval_seconds": .2}
        proc = self.launch("print(%r,flush=True)" % json.dumps(output), config=config)
        self.wait_for(lambda: self.heartbeat().get("state") == "PREFLIGHT_RETRY")
        self.assertEqual(self.heartbeat()["error_type"], "OperationalError")
        self.wait_for(lambda: self.heartbeat().get("state") == "TERMINAL_HOLD")
        self.assertEqual((self.state / "fixture-preflight-attempts").read_text(), "2")
        self.assertIsNone(proc.poll())

    def test_permanent_preflight_failure_holds_without_starting_child(self):
        config = {**self.config, "fixture_preflight_failure": True}
        proc = self.launch("raise RuntimeError('must not run after failed access contract')", config=config)
        self.wait_for(lambda: self.heartbeat().get("state") == "ERROR_HOLD")
        self.assertEqual(self.heartbeat()["error_type"], "ValueError")
        self.assertIsNone(proc.poll())

    def test_enabled_gate_runs_metadata_and_retains_fresh_receipt_terminal_skips_source(self):
        import research_no_horizon_source_preflight as preflight
        import research_no_horizon_worker_bootstrap as bootstrap
        args = SimpleNamespace(**self.config)
        for key in ("work_dir", "state_dir", "checkpoint", "adapter_dir", "preflight_receipt"):
            setattr(args, key, Path(getattr(args, key)))
        (self.work / "registry_data").mkdir()
        (self.work / "working_registry_identity.json").write_text('{"fixture":"synthetic"}')
        (self.work / "working_registry_snapshot.tar.gz").write_bytes(b"synthetic")
        verified = {key: str(getattr(args, key)) for key in ("checkpoint", "adapter_dir", "work_dir")}
        verified.update(content_inventory_sha256="synthetic", original_bundle_sha256={"baseline": "synthetic"})
        receipt = {"status": "PASS", "sslmode": "verify-full",
                   "checked_at_utc": datetime.now(timezone.utc).isoformat(), "metadata_only": True}
        with patch.object(bootstrap, "verify_inputs", return_value=verified), \
                patch.object(preflight, "execute", return_value=receipt) as execute, \
                patch.object(preflight, "endpoint_identity", return_value={"endpoint_sha256": "a" * 64}), \
                patch.object(preflight, "validate_receipt", return_value=True), \
                patch.dict(os.environ, {args.source_url_env: "private-synthetic-credential"}):
            identity = supervisor.enable_identity(args)
            execute.assert_called_once_with("private-synthetic-credential", source_id=args.source_id)
            self.assertEqual(json.loads(args.preflight_receipt.read_bytes()), receipt)
            execute.reset_mock()
            with patch.object(supervisor.os, "getenv", side_effect=AssertionError("terminal read source env")):
                self.assertEqual(supervisor.enable_identity(args, completed=True), identity)
            execute.assert_not_called()
        self.assertNotIn("private-synthetic-credential", json.dumps(identity))

    def test_enabled_gate_retains_transient_receipt_for_retry_without_validating_pass(self):
        import research_no_horizon_source_preflight as preflight
        import research_no_horizon_worker_bootstrap as bootstrap
        args = SimpleNamespace(**self.config)
        for key in ("work_dir", "state_dir", "checkpoint", "adapter_dir", "preflight_receipt"):
            setattr(args, key, Path(getattr(args, key)))
        (self.work / "registry_data").mkdir()
        (self.work / "working_registry_identity.json").write_text('{}')
        (self.work / "working_registry_snapshot.tar.gz").write_bytes(b"synthetic")
        verified = {key: str(getattr(args, key)) for key in ("checkpoint", "adapter_dir", "work_dir")}
        receipt = {"status": "ERROR", "error_type": "OperationalError", "metadata_only": True}
        with patch.object(bootstrap, "verify_inputs", return_value=verified), \
                patch.object(preflight, "execute", return_value=receipt), \
                patch.object(preflight, "endpoint_identity") as endpoint, \
                patch.object(preflight, "validate_receipt") as validate:
            with self.assertRaises(supervisor.PreflightRetry):
                supervisor.enable_identity(args)
            self.assertEqual(json.loads(args.preflight_receipt.read_bytes()), receipt)
            endpoint.assert_not_called()
            validate.assert_not_called()


if __name__ == "__main__":
    unittest.main()
