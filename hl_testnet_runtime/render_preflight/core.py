"""Independent test orchestration; this module imports no trading modules."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit


BASE_VERSION = "3084c5c7cd53b70d0569086ba78a78f8fc53a9ac"
ROOT = Path(__file__).resolve().parents[2]
MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_LOG_BYTES = 4 * 1024 * 1024
STAGE_TIMEOUT = 1800
RUN_TIMEOUT = 4000
POSTGRES_SOURCE_SHA256 = "555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f"
TASKS = ("timing_tests", "recorder", "feed", "runtime", "full_suite")


class PreflightError(RuntimeError):
    """Only fixed, non-sensitive error codes leave the harness."""


def input_files(root):
    root = Path(root).resolve()
    files = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part.startswith(".") or part == "__pycache__" for part in relative.parts):
            continue
        if path.suffix != ".py" and not (path.name.startswith("requirements") and path.suffix == ".txt"):
            continue
        if path.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent != root):
            raise PreflightError("MANIFEST_SYMLINK_REFUSED")
        if path.is_file():
            files[relative.as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not files:
        raise PreflightError("MANIFEST_EMPTY")
    return files


def create_manifest(root, output):
    payload = {"schema": "render_offline_preflight_manifest_v1",
               "version_base": BASE_VERSION, "files": input_files(root)}
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    Path(output).write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def validate_manifest(root, manifest, expected_hash):
    if not isinstance(expected_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        raise PreflightError("MANIFEST_PIN_REQUIRED")
    try:
        path = Path(manifest)
        if path.is_symlink() or path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError()
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected_hash:
            raise PreflightError("MANIFEST_HASH_MISMATCH")
        payload = json.loads(raw)
        if (payload.get("schema") != "render_offline_preflight_manifest_v1"
                or payload.get("version_base") != BASE_VERSION):
            raise PreflightError("MANIFEST_VERSION_MISMATCH")
        current = input_files(root)
        if current != payload.get("files"):
            raise PreflightError("CANDIDATE_FILES_CHANGED")
        return {"sha256": expected_hash, "version_base": BASE_VERSION, "file_count": len(current)}
    except PreflightError:
        raise
    except Exception:
        raise PreflightError("MANIFEST_INVALID") from None


def validate_ci_url(value):
    try:
        u = urlsplit(value)
        if (u.scheme != "postgresql" or u.hostname != "127.0.0.1"
                or u.path != "/hl_journal_ci" or u.username != "preflight"
                or not u.password or u.port is None or not 1024 <= u.port <= 65535
                or u.query or u.fragment):
            raise ValueError()
    except Exception:
        raise PreflightError("ONLY_FRESH_LOCAL_CI_DATABASE_ALLOWED") from None
    return value


def clean_environment(root, ci_url=None):
    """Allowlist, never inherit Render service settings, tokens or database URLs."""
    env = {"PATH": os.defpath + os.pathsep + str(Path(sys.executable).parent),
           "LANG": "C.UTF-8", "TZ": "UTC", "PYTHONUNBUFFERED": "1",
           "PYTHONDONTWRITEBYTECODE": "1", "PYTHONHASHSEED": "0",
           "PYTHONPATH": str(Path(root).resolve())}
    if ci_url is not None:
        env["HL_JOURNAL_CI_URL"] = validate_ci_url(ci_url)
    return env


def run_process(command, *, cwd, env, log_path, timeout):
    """Bounded subprocess, output to a private file, entire process-group cleanup."""
    started = time.monotonic()
    reason = None
    with open(log_path, "wb") as log:
        os.chmod(log_path, 0o600)
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while process.poll() is None:
                if time.monotonic() - started >= timeout:
                    reason = "TIMEOUT"
                    break
                if os.fstat(log.fileno()).st_size > MAX_LOG_BYTES:
                    reason = "OUTPUT_LIMIT"
                    break
                threading.Event().wait(0.1)
        finally:
            if reason is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait(timeout=10)
    return {"exit_code": process.returncode, "duration_seconds": round(time.monotonic() - started, 3),
            "failure_code": reason or ("CHILD_FAILED" if process.returncode else None)}


def _numeric(value):
    if type(value) in (int, float) and math.isfinite(value):
        return value
    raise PreflightError("INVALID_RESULT_NUMBER")


def summarize_report(task, path):
    path = Path(path)
    if not path.is_file() or path.stat().st_size > MAX_REPORT_BYTES:
        raise PreflightError("REPORT_MISSING_OR_TOO_LARGE")
    raw = path.read_bytes()
    data = json.loads(raw)
    result = {"artifact_sha256": hashlib.sha256(raw).hexdigest()}
    if task in ("timing_tests", "full_suite"):
        for key in ("tests_run", "failures", "errors", "skipped", "unexpected_successes"):
            result[key] = _numeric(data[key])
        result["successful"] = data["successful"] is True
    elif task == "recorder":
        result.update(conditions=len(data["results"]), measured_calls=sum(
            _numeric(row["caller_duration_ns"]["count"]) for row in data["results"]),
            network_attempts_blocked=_numeric(data["network_attempts_blocked"]))
        result["comparisons"] = [{"payload": row["payload"], "producers": row["producers"],
            "added_mean_ns": _numeric(row["paired_round_mean_difference_ns"]["mean"])}
            for row in data["comparisons"] if row["payload"] in ("notification", "trade_projection")]
    elif task == "feed":
        result.update(exact_feed_state_parity_pairs=_numeric(data["exact_feed_state_parity_pairs"]),
                      network_attempts_blocked=_numeric(data["network_attempts_blocked"]))
        result["single_update_added_mean_ns"] = [_numeric(row["paired_added_duration_ns"]["mean"])
            for row in data["results"] if row["frame_size"] == 1 and row["duplicate"] is False]
    elif task == "runtime":
        result.update(rounds=_numeric(data["rounds"]), measured_scenarios=len(data["raw_samples"]),
            trace_equality_checks=_numeric(data["trace_equality_checks_including_warmups"]),
            simulated_order_requests_per_scenario=_numeric(data["simulated_order_requests_per_scenario"]),
            simulated_reader_calls_per_scenario=_numeric(data["simulated_reader_calls_per_scenario"]))
        result["runtime_mean_ms"] = {mode: _numeric(data["summary"][mode]["runtime_subtotal_ms"]["mean"])
                                     for mode in ("disabled", "enabled")}
        result["fill_to_projection_mean_ms"] = {mode: _numeric(data["summary"][mode][
            "simulated_fill_to_verified_projection_ms"]["mean"]) for mode in ("disabled", "enabled")}
    else:
        raise PreflightError("UNKNOWN_TASK")
    return result


class DisposablePostgres:
    """Unmodified PG18 binaries, fresh local database, no existing database accepted."""
    def __init__(self, root, directory):
        self.root, self.directory = Path(root), Path(directory)
        self.bin_dir = None
        self.started = False
        self.ci_url = None
        self.status = "NOT_STARTED"

    def _command(self, args, timeout=120):
        log = self.directory / ("postgres-" + args[0] + ".log")
        return run_process([str(self.bin_dir / args[0]), *args[1:]], cwd=self.root,
            env=clean_environment(self.root), log_path=log, timeout=timeout)

    def start(self):
        if os.geteuid() == 0:
            self.status = "NOT_RUN_POSTGRES_REQUIRES_NONROOT"
            return None
        prefix = self.root / ".render_preflight_pg"
        marker = prefix / "verified_source_sha256"
        if not marker.is_file() or marker.read_text().strip() != POSTGRES_SOURCE_SHA256:
            self.status = "NOT_RUN_VERIFIED_POSTGRES186_UNAVAILABLE"
            return None
        candidates = [prefix / "bin"]
        for directory in candidates:
            if not all((directory / name).is_file() and os.access(directory / name, os.X_OK)
                       for name in ("postgres", "initdb", "pg_ctl", "createdb")):
                continue
            probe = subprocess.run([str(directory / "postgres"), "--version"],
                env=clean_environment(self.root), capture_output=True, timeout=5, check=False)
            if probe.returncode == 0 and probe.stdout.strip() == b"postgres (PostgreSQL) 18.6":
                self.bin_dir = directory
                break
        if self.bin_dir is None:
            self.status = "NOT_RUN_VERIFIED_POSTGRES186_UNAVAILABLE"
            return None
        data = self.directory / "pgdata"
        password = secrets.token_urlsafe(24)
        pwfile = self.directory / "pgpassword"
        pwfile.write_text(password + "\n")
        pwfile.chmod(0o600)
        init = self._command(["initdb", "-D", str(data), "-U", "preflight", "--no-locale", "-E", "UTF8",
            "--auth-local=trust", "--auth-host=scram-sha-256", "--pwfile=" + str(pwfile)])
        pwfile.unlink(missing_ok=True)
        if init["exit_code"] != 0:
            raise PreflightError("POSTGRES_INIT_FAILED")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        options = (f"-h 127.0.0.1 -p {port} -k {self.directory} -c shared_buffers=16MB "
                   "-c work_mem=1MB -c max_connections=64 -c max_parallel_workers=0 "
                   "-c io_method=sync -c log_statement=none")
        started = self._command(["pg_ctl", "-D", str(data), "-l", str(self.directory / "postgres.log"),
                                 "-o", options, "-w", "-t", "60", "start"], timeout=75)
        if started["exit_code"] != 0:
            # pg_ctl may time out after the server starts; stop the exact data directory.
            self.started = True
            self.stop()
            raise PreflightError("POSTGRES_START_FAILED")
        self.started = True
        created = self._command(["createdb", "-h", str(self.directory), "-p", str(port),
                                 "-U", "preflight", "hl_journal_ci"])
        if created["exit_code"] != 0:
            raise PreflightError("POSTGRES_DATABASE_CREATE_FAILED")
        self.ci_url = validate_ci_url(f"postgresql://preflight:{password}@127.0.0.1:{port}/hl_journal_ci")
        self.status = "FRESH_LOOPBACK_POSTGRES18"
        return self.ci_url

    def stop(self):
        if self.started:
            result = self._command(["pg_ctl", "-D", str(self.directory / "pgdata"), "-w", "-t", "15",
                                    "-m", "immediate", "stop"], timeout=25)
            if result["exit_code"] != 0 or result["failure_code"]:
                raise PreflightError("POSTGRES_CLEANUP_FAILED")
            self.started = False


class Runner:
    def __init__(self, root, manifest, expected_hash, *, output_dir=None, allow_postgres=True):
        self.root, self.manifest = Path(root).resolve(), Path(manifest).resolve()
        self.expected_hash = expected_hash
        self.output_dir = output_dir
        self.allow_postgres = allow_postgres
        self._lock = threading.Lock()
        self._started = False
        self._thread = None
        self._summary = {"schema": "render_offline_preflight_v1", "status": "NOT_STARTED",
            "isolated": True, "exchange": "SYNTHETIC_ONLY", "stages": [],
            "limits": ["Free instance timing is not live-service latency or capacity evidence.",
                       "No live exchange, trading account or existing database is accessed.",
                       "Runtime benchmark uses four rounds; observed tails are not guarantees."]}

    def snapshot(self):
        with self._lock:
            return json.loads(json.dumps(self._summary))

    def _update(self, **fields):
        with self._lock:
            self._summary.update(fields)

    def _log(self, kind, value):
        # Only already-whitelisted summaries are emitted; no logs, env or raw fixtures.
        payload = json.dumps({"render_offline_preflight": {"kind": kind, **value}}, sort_keys=True)
        if len(payload) <= 65536:
            print(payload, flush=True)

    def start_once(self):
        with self._lock:
            if self._started:
                return False
            self._started = True
            self._summary["status"] = "RUNNING"
            self._thread = threading.Thread(target=self._run, name="isolated-preflight", daemon=True)
            self._thread.start()
            return True

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self):
        started = time.monotonic()
        pg = None
        temp = None
        try:
            identity = validate_manifest(self.root, self.manifest, self.expected_hash)
            self._update(candidate=identity)
            # Reject preconfigured DB URLs rather than accidentally treating them as test input.
            if any(key in os.environ for key in ("HL_JOURNAL_CI_URL", "HL_TESTNET_DATABASE_URL", "DATABASE_URL")):
                raise PreflightError("INHERITED_DATABASE_SETTINGS_REFUSED")
            if self.output_dir is None:
                temp = tempfile.TemporaryDirectory(prefix="render-offline-preflight-")
                directory = Path(temp.name)
            else:
                directory = Path(self.output_dir).resolve()
                directory.mkdir(mode=0o700, parents=True, exist_ok=False)
            directory.chmod(0o700)
            stages = []
            for task in TASKS:
                ci_url = None
                if task == "runtime":
                    if self.allow_postgres:
                        pg = DisposablePostgres(self.root, directory)
                        ci_url = pg.start()
                        self._update(postgres=pg.status)
                    else:
                        self._update(postgres="NOT_RUN_POSTGRES_EXPLICITLY_DISABLED")
                    if ci_url is None:
                        self._update(status="PARTIAL", durable_checks="NOT_RUN")
                        break
                elif task == "full_suite":
                    ci_url = pg.ci_url
                remaining = RUN_TIMEOUT - (time.monotonic() - started)
                if remaining <= 0:
                    raise PreflightError("RUN_TIMEOUT")
                self._update(current_stage=task)
                output = directory / (task + ".json")
                result = run_process([sys.executable, "-m", "hl_testnet_runtime.render_preflight.child",
                    "--task", task, "--output", str(output)], cwd=self.root,
                    env=clean_environment(self.root, ci_url), log_path=directory / (task + ".log"),
                    timeout=min(STAGE_TIMEOUT, remaining))
                row = {"task": task, **result}
                if result["exit_code"] == 0:
                    row.update(summarize_report(task, output))
                    if task in ("timing_tests", "full_suite") and (
                            row["successful"] is not True or row["skipped"] or row["unexpected_successes"]):
                        row["failure_code"] = "SUITE_NOT_COMPLETE"
                    if task == "timing_tests" and row["tests_run"] != 28:
                        row["failure_code"] = "EXPECTED_28_TIMING_TESTS"
                    if task == "full_suite" and row["tests_run"] < 2054:
                        row["failure_code"] = "FULL_SUITE_DISCOVERY_INCOMPLETE"
                    if row.get("network_attempts_blocked", 0) != 0:
                        row["failure_code"] = "UNEXPECTED_NETWORK_ATTEMPT"
                stages.append(row)
                self._update(stages=list(stages))
                self._log("stage_completed", row)
                if row["failure_code"]:
                    raise PreflightError(row["failure_code"])
            else:
                self._update(status="PASSED", durable_checks="COMPLETED")
        except Exception as exc:
            code = str(exc) if isinstance(exc, PreflightError) else "HARNESS_FAILED"
            self._update(status="FAILED", failure_code=code)
        finally:
            if pg is not None:
                try:
                    was_started = pg.started
                    pg.stop()
                    if was_started:
                        self._update(postgres_stopped=True)
                except Exception:
                    self._update(status="FAILED", failure_code="POSTGRES_CLEANUP_FAILED")
            self._update(current_stage=None, duration_seconds=round(time.monotonic() - started, 3))
            self._log("finished", self.snapshot())
            if self.output_dir is not None and Path(self.output_dir).is_dir():
                (Path(self.output_dir) / "summary.json").write_text(json.dumps(self.snapshot(), indent=2) + "\n")
            if temp is not None:
                temp.cleanup()
