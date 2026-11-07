#!/usr/bin/env python3
"""Run the frozen SERIAL fixture with an owned native PostgreSQL cluster.

This is a test adapter only. It neither starts on import nor accepts a database
URL, account session, API key, collector URL or production worker configuration.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid

RUN_BLOB = "3d8612558d9bde7a86d66540c82d87ae7fec1566"
HARNESS = Path("app/.github/labs/btc-full-flow")
SAFE_INHERITED = ("PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH")


def git_blob(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def beneath(path: Path, root: Path) -> Path:
    resolved, parent = path.resolve(), root.resolve()
    if resolved == parent or parent not in resolved.parents:
        raise ValueError("Path must be a child of its owned lab directory")
    return resolved


def local_base(port: int) -> str:
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("Invalid unprivileged local PostgreSQL port")
    return f"postgresql://postgres:postgres@127.0.0.1:{port}/"


def clean_environment(lab: Path, evidence: Path, inherited: dict[str, str]) -> dict[str, str]:
    env = {key: inherited[key] for key in SAFE_INHERITED if key in inherited}
    env.update({
        "HOME": str(lab / "home"), "TMPDIR": str(evidence / "temporary"),
        "LANG": "C.UTF-8", "TZ": "UTC", "CI": "true", "NO_COLOR": "1",
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1",
        "QUEUE_LAB_EVIDENCE": str(evidence), "QUEUE_LAB_COLLECTOR": str(lab / "collector"),
        "QUEUE_LAB_NODE": "/opt/lab-node",
        "QUEUE_LAB_FIXTURE_GUARD": str(lab / "packaging/serial_fixture_guard"),
        "QUEUE_LAB_ORIGINAL_FIXTURE": str(lab / HARNESS / "sitecustomize.py"),
        "QUEUE_LAB_NODE_GUARD": str(lab / "packaging/serial_node_guard.cjs"),
        "QUEUE_LAB_GUARD_AUDIT_DIR": str(evidence / "guards"),
    })
    return env


def replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError("Frozen lab adapter anchor changed")
    return source.replace(old, new, 1)


def adapt_harness(data: bytes, base: str, database_pid: int) -> str:
    if git_blob(data) != RUN_BLOB:
        raise ValueError("Unexpected serial harness Git blob")
    if not base.startswith("postgresql://postgres:postgres@127.0.0.1:") or not base.endswith("/"):
        raise ValueError("Only the owned loopback database is accepted")
    if base != local_base(int(base.rsplit(":", 1)[1][:-1])) or database_pid <= 1:
        raise ValueError("Invalid native database identity")
    source = data.decode("utf-8")
    source = replace_once(source, "BASE='postgresql://postgres:postgres@127.0.0.1:5432/'", "BASE=" + repr(base))
    start = "containers=subprocess.check_output(['docker','ps','--filter','ancestor=postgres:16.10','--format','{{.ID}}'],text=True).split()\nassert len(containers)==1,'Expected exactly one disposable PostgreSQL service'\ndatabase_pid=int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',containers[0]],text=True))"
    source = replace_once(source, start, "database_pid=" + repr(database_pid))
    source = replace_once(source, "('PATH','HOME','LANG','PLAYWRIGHT_BROWSERS_PATH')", "('PATH','HOME','LANG','PLAYWRIGHT_BROWSERS_PATH','TMPDIR')")
    source = replace_once(source, "PYTHONPATH=str(LAB)", "PYTHONPATH=os.environ['QUEUE_LAB_FIXTURE_GUARD'],QUEUE_LAB_ORIGINAL_FIXTURE=os.environ['QUEUE_LAB_ORIGINAL_FIXTURE'],QUEUE_LAB_GUARD_AUDIT_DIR=os.environ['QUEUE_LAB_GUARD_AUDIT_DIR'],PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1'")
    source = replace_once(source, "subprocess.run([sys.executable,str(LAB/'prepare.py')],check=True,cwd=ROOT)", "subprocess.run([sys.executable,str(LAB/'prepare.py')],check=True,cwd=ROOT,env=dict(os.environ))")
    source = replace_once(source, "subprocess.run(['bun',str(LAB/'build-ui.mjs')],check=True,cwd=ROOT)", "subprocess.run(['bun',str(LAB/'build-ui.mjs')],check=True,cwd=ROOT,env=dict(os.environ))")
    # os.environ here is the supervisor's complete allowlist, not hosting env.
    source = replace_once(source, "appenv=dict(os.environ,QUEUE_LAB_APP_DSN=BASE+'legacy_transaction_test')", "appenv=dict(os.environ,QUEUE_LAB_APP_DSN=BASE+'legacy_transaction_test',PYTHONPATH=os.environ['QUEUE_LAB_FIXTURE_GUARD'],NODE_OPTIONS='--require='+os.environ['QUEUE_LAB_NODE_GUARD'])")
    compile(source, "<native-serial-adapter>", "exec")
    return source


def verify_inputs(lab: Path) -> dict[str, str]:
    manifest = json.loads((Path(__file__).with_name("native_serial_manifest.json")).read_text())
    verified = {}
    for entry in manifest["pinned_inputs"]:
        path = beneath(lab / entry["path"], lab)
        data = path.read_bytes()
        digest = git_blob(data) if entry["algorithm"] == "git-blob-sha1" else hashlib.sha256(data).hexdigest()
        if digest != entry["digest"]:
            raise ValueError("Pinned fixture/product input mismatch: " + entry["path"])
        verified[entry["path"]] = digest
    return verified


def product_fingerprint(lab: Path) -> dict[str, str]:
    # Include all supplied source, including helper files not exercised by this run.
    paths = []
    for directory in (lab / "app/src", lab / "app/supabase", lab / "collector"):
        paths.extend(p for p in directory.rglob("*") if p.is_file()
                     and "__pycache__" not in p.parts and p.suffix != ".pyc")
    return {str(p.relative_to(lab)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def check_fixed_ports() -> None:
    for port in (21879, 21880):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", port))


def matching_process(pid: int, created: float):
    import psutil
    try:
        process = psutil.Process(pid)
        if process.create_time() == created and process.status() != psutil.STATUS_ZOMBIE:
            return process
    except psutil.Error:
        pass
    return None


class OwnedProcesses:
    def __init__(self):
        self.identities: dict[int, float] = {}
        self.lock = threading.Lock()

    def capture(self, pid: int) -> list:
        import psutil
        try:
            root = psutil.Process(pid)
            items = [root, *root.children(recursive=True)]
        except psutil.Error:
            return []
        with self.lock:
            for process in items:
                with contextlib.suppress(psutil.Error):
                    self.identities[process.pid] = process.create_time()
        return items

    def cleanup(self) -> dict:
        import psutil
        with self.lock:
            identities = dict(self.identities)
        live = [p for pid, born in identities.items() if (p := matching_process(pid, born))]
        for process in reversed(live):
            with contextlib.suppress(psutil.Error):
                process.terminate()
        _, remaining = psutil.wait_procs(live, timeout=5)
        for process in remaining:
            if matching_process(process.pid, identities[process.pid]):
                with contextlib.suppress(psutil.Error):
                    process.kill()
        _, remaining = psutil.wait_procs(remaining, timeout=5)
        return {"tracked": len(identities), "remaining_owned_pids": [p.pid for p in remaining]}


class NativePostgres:
    def __init__(self, binary: Path, directory: Path, env: dict[str, str], port: int):
        self.binary, self.directory, self.env, self.port = binary, directory, env, port
        self.data = directory / "data"
        self.pid = None
        self.created = None
        self.initialized = False
        self.last_command = None

    def command(self, args: list[str], timeout: int = 90):
        self.last_command = {"program": Path(args[0]).name, "timeout_seconds": timeout}
        with (self.directory / "bootstrap.log").open("ab") as output:
            try:
                completed = subprocess.run(args, env=self.env, stdout=output, stderr=subprocess.STDOUT,
                                           check=True, timeout=timeout)
                self.last_command["exit_code"] = completed.returncode
                return completed
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                self.last_command["error_type"] = type(error).__name__
                if isinstance(error, subprocess.CalledProcessError):
                    self.last_command["exit_code"] = error.returncode
                raise

    def start(self) -> None:
        if os.geteuid() == 0:
            raise ValueError("Native fixture PostgreSQL must run as the non-root lab user")
        if self.directory.stat().st_uid != os.geteuid():
            raise ValueError("Cluster directory is not owned by this lab user")
        sockets = self.directory / "socket"
        sockets.mkdir(mode=0o700)
        self.command([str(self.binary / "initdb"), "-D", str(self.data), "-U", "postgres",
                      "--encoding=UTF8", "--locale=C.UTF-8", "--auth-local=trust", "--auth-host=trust"])
        self.initialized = True
        options = (f"-c listen_addresses=127.0.0.1 -c port={self.port} -c unix_socket_directories={sockets} "
                   "-c timezone=UTC -c log_timezone=UTC -c wal_level=logical -c jit=off "
                   "-c shared_buffers=64MB -c max_connections=30")
        self.command([str(self.binary / "pg_ctl"), "-D", str(self.data), "-l", str(self.directory / "postgres.log"),
                      "-o", options, "-w", "-t", "45", "start"], 60)
        self.read_identity()

    def read_identity(self) -> None:
        import psutil
        lines = (self.data / "postmaster.pid").read_text().splitlines()
        if len(lines) < 4 or Path(lines[1]).resolve() != self.data.resolve() or int(lines[3]) != self.port:
            raise ValueError("Native PostgreSQL ownership/port receipt mismatch")
        process = psutil.Process(int(lines[0]))
        if process.uids().real != os.geteuid() or str(self.data) not in process.cmdline():
            raise ValueError("Unexpected PostgreSQL process identity")
        self.pid, self.created = process.pid, process.create_time()

    def cleanup(self) -> dict:
        receipt = {"initialized": self.initialized, "stopped": True, "pid": self.pid}
        # pg_ctl uses only this newly owned -D, even if start timed out before PID capture.
        if self.initialized:
            if self.pid is None and (self.data / "postmaster.pid").exists():
                with contextlib.suppress(Exception):
                    self.read_identity()
            try:
                result = subprocess.run([str(self.binary / "pg_ctl"), "-D", str(self.data),
                                         "-w", "-t", "15", "-m", "immediate", "stop"],
                                        env=self.env, capture_output=True, timeout=25, check=False)
                receipt["stop_exit"] = result.returncode
            except Exception as error:
                receipt["stop_error_type"] = type(error).__name__
            if self.pid and self.created and matching_process(self.pid, self.created):
                process = matching_process(self.pid, self.created)
                children = process.children(recursive=True)
                for child in [*reversed(children), process]:
                    with contextlib.suppress(Exception):
                        child.kill()
                with contextlib.suppress(Exception):
                    process.wait(timeout=5)
            receipt["stopped"] = not (self.pid and self.created and matching_process(self.pid, self.created))
            if not self.pid and (self.data / "postmaster.pid").exists():
                receipt["stopped"] = False
        return receipt


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n")


def sanitized_pg_tail(path: Path, limit: int = 8192) -> dict:
    """Read only a bounded regular-file tail; never inspect environment values."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {"present": False}
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Diagnostic source is not a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            stream.seek(max(0, info.st_size - limit))
            text = stream.read(limit).decode("utf-8", errors="replace")
        if info.st_size > limit:
            # Drop a partial leading line so truncation cannot remove a secret
            # field's label while leaving its value outside pattern redaction.
            text = text.split("\n", 1)[1] if "\n" in text else ""
        # Only static patterns are used: no secret values or ambient env are read.
        text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
        text = "".join(c for c in text if c in "\n\t" or ord(c) >= 32)
        text = re.sub(r"(?i)\b[a-z][a-z0-9+.-]*://\S+", "[URL_REDACTED]", text)
        credential = re.compile(r"(?i)(?:authorization|cookie|password|passwd|secret|token|api[_ -]?key|connection[_ -]?string|\bdsn\b)\s*[:=]")
        text = "\n".join("[CREDENTIAL_SHAPED_LINE_REDACTED]" if credential.search(line) else line
                         for line in text.splitlines())
        encoded = text.encode("utf-8")
        if len(encoded) > limit:
            text = encoded[-limit:].decode("utf-8", errors="ignore")
        return {"present": True, "source_bytes": info.st_size, "tail_truncated": info.st_size > limit,
                "retained_bytes": len(text.encode("utf-8")), "sanitized_tail": text}
    finally:
        os.close(descriptor)


def preserve_pg_diagnostic(directory: Path, evidence: Path, stage: str, last_command) -> None:
    value = {"format": "native-postgres-diagnostic.v1", "stage": stage,
             "last_command": last_command, "maximum_log_tail_bytes_each": 8192,
             "logs": {name: sanitized_pg_tail(directory / name) for name in ("bootstrap.log", "postgres.log")},
             "environment_values_included": False}
    target = evidence / "native-postgres-diagnostic.json"
    write_json(target, value)
    target.chmod(0o600)


def verify_guard_receipts(evidence: Path) -> dict:
    python = [json.loads(p.read_text()) for p in (evidence / "guards").glob("python-*.json")]
    node = [json.loads(p.read_text()) for p in (evidence / "guards").glob("node-*.json")]
    if len(python) < 20 or not node or any(r.get("canary_blocked") != 1 for r in python + node):
        raise RuntimeError("Missing successful fixture egress guard receipts")
    if sum(r.get("source_contexts_offline", 0) for r in python) != 19:
        raise RuntimeError("Expected all19 source fixture contexts to be offline")
    if any(r.get(key, 0) for r in python + node
           for key in ("denied_network", "denied_browser_http", "denied_browser_websocket")):
        raise RuntimeError("An unexpected external destination was blocked; inspect guard counts")
    return {"python_receipts": len(python), "node_receipts": len(node),
            "source_contexts_offline": 19, "canaries_passed": True,
            "unexpected_destinations": 0, "boundary": "software guards for the fixed fixtures, not an OS firewall"}


def supervise(lab: Path, evidence: Path, binary: Path, timeout: int) -> int:
    import psutil
    import collector_matrix as lifecycle
    env = clean_environment(lab, evidence, dict(os.environ))
    cluster_dir = None
    pg = None
    owned = OwnedProcesses()
    stop = threading.Event()
    samples = []
    monitor = None
    child = None
    before = {}
    result = {"status": "failed", "stage": "validate", "production_writes": 0,
              "external_source_calls": 0, "external_ai_calls": 0}
    cleanup = {}
    lifecycle_active = False
    started = time.monotonic()

    def interrupted(_signal, _frame):
        raise InterruptedError("Serial fixture interrupted")

    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
    try:
        result["pinned_input_count"] = len(verify_inputs(lab))
        before = product_fingerprint(lab)
        result["stage"] = "supervisor_preflight"
        # The shared, reviewed supervisor verifies /proc namespace identity,
        # pidfds and Linux subreaper ownership before any process is spawned.
        lifecycle.establish_subreaper()
        lifecycle_active = True
        (evidence / "temporary").mkdir(mode=0o700)
        (evidence / "guards").mkdir(mode=0o700)
        (lab / "home").mkdir(mode=0o700, exist_ok=True)
        cluster_dir = Path(tempfile.mkdtemp(prefix="serial-pg-", dir=evidence / "temporary"))
        os.chmod(cluster_dir, 0o700)
        pg = NativePostgres(binary, cluster_dir, env, reserve_port())
        check_fixed_ports()
        result["stage"] = "start_postgres"
        pg.start()
        result["postgres_version"] = subprocess.check_output([str(binary / "postgres"), "--version"], env=env, text=True).strip()
        result["postgres_major_matches_baseline"] = " 16." in result["postgres_version"]
        if not result["postgres_major_matches_baseline"]:
            raise ValueError("The serial lab requires PostgreSQL major16")
        source = adapt_harness((lab / HARNESS / "run.py").read_bytes(), local_base(pg.port), pg.pid)
        write_json(evidence / "adapter-receipt.json", {
            "original_git_blob": RUN_BLOB, "adapted_sha256": hashlib.sha256(source.encode()).hexdigest(),
            "changes": ["owned localhost database base", "owned native postmaster PID", "Python fixture guard", "explicit child allowlist and Node loopback guard"],
            "assertions": "Original serial19/18/failed-map/retry/hash/SQL/UIreload assertions retained verbatim"})
        # Only this allowlist becomes os.environ in the frozen runner and all its children.
        command = [sys.executable, "-c", "import signal; signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('fixture stopped'))); "
                   + "exec(compile(" + repr(source) + "," + repr(str(lab / HARNESS / "run.py"))
                   + ",'exec'), {'__name__':'__main__','__file__':" + repr(str(lab / HARNESS / "run.py")) + "})"]
        result["stage"] = "serial_full_flow"
        with (evidence / "supervised-harness.log").open("wb") as output:
            child = subprocess.Popen(command, cwd=lab / "app", env=env, stdout=output,
                                     stderr=subprocess.STDOUT, start_new_session=True)

            def measure():
                while not stop.wait(0.2):
                    processes = owned.capture(child.pid)
                    try:
                        parent = psutil.Process(pg.pid)
                        processes += [parent, *parent.children(recursive=True)]
                    except psutil.Error:
                        pass
                    unique = {p.pid: p for p in processes}
                    rss = 0
                    for process in unique.values():
                        with contextlib.suppress(psutil.Error):
                            rss += process.memory_info().rss
                    samples.append({"wall_seconds": round(time.monotonic() - started, 3),
                                    "summed_rss_bytes": rss, "processes": len(unique)})

            owned.capture(child.pid)
            monitor = threading.Thread(target=measure, daemon=True)
            monitor.start()
            exit_code = child.wait(timeout=timeout)
            if exit_code:
                raise RuntimeError("Frozen serial fixture failed; inspect supervised-harness.log")
        summary = json.loads((evidence / "summary.json").read_text())
        if not (summary.get("status") == "passed" and summary.get("jobs") == 19
                and summary.get("ready") == 18 and summary.get("peak_running_jobs") == 1):
            raise RuntimeError("Missing original full-flow acceptance receipt")
        result.update(status="passed", stage="complete", original_summary=summary,
                      guard_receipts=verify_guard_receipts(evidence))
    except Exception as error:
        result["error_type"] = type(error).__name__
        result["error"] = str(error)[:300]
    finally:
        # Do not allow a second interactive signal to interrupt owned-resource cleanup.
        for sig in handlers:
            signal.signal(sig, signal.SIG_IGN)
        cleanup_errors = []
        try:
            if child is not None:
                owned.capture(child.pid)
                if child.poll() is None:
                    with contextlib.suppress(ProcessLookupError):
                        child.terminate()
                    try:
                        child.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        with contextlib.suppress(ProcessLookupError):
                            child.kill()
                        with contextlib.suppress(subprocess.TimeoutExpired):
                            child.wait(timeout=5)
        except BaseException as error:
            cleanup_errors.append("harness:" + type(error).__name__)
        stop.set()
        if monitor is not None:
            try:
                monitor.join(timeout=5)
            except BaseException as error:
                cleanup_errors.append("monitor_join:" + type(error).__name__)
        try:
            cleanup["harness_children"] = owned.cleanup()
        except BaseException as error:
            cleanup_errors.append("sampled_children:" + type(error).__name__)
            cleanup["harness_children"] = {"tracking_cleanup_failed": True}
        try:
            cleanup["postgres"] = pg.cleanup() if pg is not None else {"initialized": False, "stopped": True}
        except BaseException as error:
            cleanup["postgres"] = {"stopped": False, "error_type": type(error).__name__}
        try:
            if lifecycle_active:
                clean, killed, fault = lifecycle.cleanup(child, time.monotonic() + 10)
                cleanup["final_owned_descendant_census"] = {"verified_empty": clean, "forced_signals": killed, "fault": fault}
            else:
                clean, fault = pg is None and child is None, False
                cleanup["final_owned_descendant_census"] = {"no_process_started": clean, "verified_empty": clean, "fault": False}
        except BaseException as error:
            clean, fault = False, True
            cleanup_errors.append("final_census:" + type(error).__name__)
            cleanup["final_owned_descendant_census"] = {"verified_empty": False, "fault": True}
        if clean and not fault:
            cleanup["postgres"]["no_owned_process_remains"] = True
        diagnostic_preserved = cluster_dir is None
        if cluster_dir is not None:
            try:
                preserve_pg_diagnostic(cluster_dir, evidence, result["stage"], pg.last_command if pg is not None else None)
                diagnostic_preserved = True
                result["postgres_diagnostic"] = "native-postgres-diagnostic.json"
            except BaseException as error:
                cleanup_errors.append("postgres_diagnostic:" + type(error).__name__)
        cleanup["postgres"]["diagnostic_preserved"] = diagnostic_preserved
        if cluster_dir is not None and clean and not fault and diagnostic_preserved:
            try:
                shutil.rmtree(cluster_dir)
            except BaseException as error:
                cleanup_errors.append("cluster_directory:" + type(error).__name__)
            cleanup["owned_cluster_directory_removed"] = not cluster_dir.exists()
        else:
            cleanup["owned_cluster_directory_removed"] = cluster_dir is None
        try:
            unchanged = not before or before == product_fingerprint(lab)
            verify_inputs(lab)
        except Exception:
            unchanged = False
        result["product_sources_unchanged"] = unchanged
        if not unchanged or not clean or fault or cleanup_errors:
            result["status"] = "failed"
            result["cleanup_or_source_integrity_failed"] = True
        cleanup["errors"] = cleanup_errors
        write_json(evidence / "native-cleanup.json", cleanup)
        write_json(evidence / "native-resources.json", {
            "metric": "sum of process RSS, sampled every0.2s; shared pages may be double-counted",
            "peak_sampled_rss_bytes": max((s["summed_rss_bytes"] for s in samples), default=0),
            "wall_seconds": round(time.monotonic() - started, 3), "samples": samples})
        write_json(evidence / "native-result.json", result)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
    print(json.dumps({"status": result["status"], "stage": result["stage"], "evidence": str(evidence)}))
    return 0 if result["status"] == "passed" else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-root", type=Path, default=Path("/lab"))
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--postgres-bin", type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=2400)
    args = parser.parse_args(argv)
    lab = args.lab_root.resolve(strict=True)
    if not 60 <= args.timeout_seconds <= 3600:
        parser.error("Timeout must be between60 and3600 seconds")
    evidence_root = lab / "evidence"
    if evidence_root.is_symlink():
        parser.error("Evidence root cannot be a symlink")
    evidence_root.mkdir(mode=0o700, exist_ok=True)
    evidence = beneath(args.evidence_dir, evidence_root)
    if args.evidence_dir.is_symlink():
        parser.error("Evidence path cannot be a symlink")
    binary = (args.postgres_bin or Path(shutil.which("initdb") or "/missing/initdb").parent).resolve()
    if any(not (binary / name).is_file() for name in ("initdb", "pg_ctl", "postgres")):
        parser.error("Native PostgreSQL binaries are missing")
    with (evidence_root / "native-serial.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another serial fixture owns this lab")
        evidence.mkdir(mode=0o700, parents=False, exist_ok=False)
        return supervise(lab, evidence, binary, args.timeout_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
