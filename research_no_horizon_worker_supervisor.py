"""Persistent host supervisor for the existing original-eight research runner.

Disabled by default. It delegates every bounded tick to the unchanged runner,
holds completion instead of exiting/restarting acquisition, and explicitly
checks source metadata once per enabled start. The host must provide persistent storage and 300s shutdown
grace. A heartbeat confirms process observation, not deployment or source truth.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import threading
import time

import research_no_horizon_contract as contracts
import research_no_horizon_registered_runner as runner

VERSION = "no-horizon-persistent-worker-supervisor-v1"
MAX_LINE_BYTES = 64 * 1024
MAX_RECEIPT_BYTES = 256 * 1024 * 1024
MAX_PREFLIGHT_AGE_SECONDS = 24 * 60 * 60
_RETRYABLE_PREFLIGHT_ERRORS = {"OperationalError", "InterfaceError", "QueryCanceled"}


class PreflightRetry(RuntimeError):
    def __init__(self, error_type):
        self.error_type = error_type
        super().__init__("RETRYABLE_METADATA_PREFLIGHT")


def _sha_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def _json_file(path, maximum=MAX_RECEIPT_BYTES):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise ValueError("BOUNDED_REGULAR_SUPERVISOR_INPUT_REQUIRED")
    return json.loads(path.read_bytes())


@contextmanager
def supervisor_lock(directory):
    directory.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(directory / "supervisor.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("SUPERVISOR_ALREADY_ACTIVE") from None
        yield
    finally:
        os.close(descriptor)


def enable_identity(args, *, completed=False):
    """Verify staged inputs and an explicit restore, then metadata preflight.

    Completed workers need no new credential read or fresh preflight: their
    retained preflight bytes remain bound to the terminal latch. They never
    reopen a source just to confirm completion.
    """
    import research_no_horizon_worker_bootstrap as bootstrap
    import research_no_horizon_source_preflight as preflight
    verified = bootstrap.verify_inputs(args.work_dir.parent)
    for key in ("checkpoint", "adapter_dir", "work_dir"):
        if Path(verified[key]).resolve() != getattr(args, key):
            raise ValueError("SUPERVISOR_VERIFIED_INPUT_PATH_MISMATCH")
    for name in ("working_registry_identity.json", "working_registry_snapshot.tar.gz"):
        path = args.work_dir / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("EXPLICIT_ORIGINAL_STATE_RESTORE_REQUIRED")
    if (args.work_dir / "registry_data").is_symlink() or not (args.work_dir / "registry_data").is_dir():
        raise ValueError("EXPLICIT_ORIGINAL_STATE_RESTORE_REQUIRED")
    if not completed:
        raw = os.getenv(args.source_url_env, "")
        receipt = preflight.execute(raw, source_id=args.source_id)
        args._preflight_status = receipt.get("status")
        args._preflight_checked_at = receipt.get("checked_at_utc")
        args._preflight_observed_this_start = True
        # Every attempt is metadata-only and explicitly enabled. Retain its
        # sanitized receipt without requiring an operator to refresh it later.
        runner.atomic_json(args.preflight_receipt, receipt)
        if receipt.get("status") == "ERROR" and receipt.get("error_type") in _RETRYABLE_PREFLIGHT_ERRORS:
            raise PreflightRetry(receipt["error_type"])
        endpoint = preflight.endpoint_identity(raw, source_id=args.source_id)
        preflight.validate_receipt(receipt, source_id=args.source_id,
                                   endpoint_sha256=endpoint["endpoint_sha256"])
        age = (datetime.now(timezone.utc) - contracts.utc(receipt["checked_at_utc"])).total_seconds()
        if not -300 <= age <= MAX_PREFLIGHT_AGE_SECONDS or receipt.get("sslmode") != "verify-full":
            raise ValueError("FRESH_REMOTE_SOURCE_PREFLIGHT_REQUIRED")
    else:
        receipt = _json_file(args.preflight_receipt, 4 * 1024 * 1024)
        preflight.validate_receipt(receipt, source_id=args.source_id,
                                   endpoint_sha256=receipt.get("endpoint_sha256"))
        args._preflight_status = receipt.get("status")
        args._preflight_checked_at = receipt.get("checked_at_utc")
        args._preflight_observed_this_start = False
    return {"version": VERSION, "source_id": args.source_id,
        "source_url_env": args.source_url_env,
        "input_inventory_sha256": verified["content_inventory_sha256"],
        "original_bundle_sha256": verified["original_bundle_sha256"],
        "working_registry_identity_sha256": _sha_file(args.work_dir / "working_registry_identity.json"),
        "preflight_receipt_sha256": _sha_file(args.preflight_receipt)}


def runner_command(args):
    return [sys.executable, str(Path(runner.__file__).resolve()), "run",
        "--checkpoint", str(args.checkpoint), "--adapter-dir", str(args.adapter_dir),
        "--work-dir", str(args.work_dir), "--source-id", args.source_id,
        "--source-url-env", args.source_url_env, "--node", args.node,
        "--interval-seconds", str(args.interval_seconds), "--request-budget", "8",
        "--leaf-budget", "1", "--passes", "1", "--candle-budget", "1024",
        "--enable-acquisition"]


def receipt_projection(args, output):
    """Read the durable receipt, independently bind its tick and snapshot."""
    path = Path(output["receipt"])
    if (not path.is_absolute() or path.is_symlink()
            or path.resolve().parent != (args.work_dir / "receipts").resolve()
            or not re.fullmatch(r"[0-9]{8}T[0-9]{6}-[a-f0-9]{32}\.json", path.name)):
        raise ValueError("RUNNER_RECEIPT_OUTSIDE_WORK_STATE")
    receipt = _json_file(path)
    tick = receipt["native_tick"]
    if (receipt.get("version") != runner.VERSION or receipt.get("transport") != runner.MODE
            or receipt.get("clean_registry_shutdown") is not True
            or receipt.get("telegram_authorized") is not False
            or receipt.get("trading_authorized") is not False
            or tick.get("request_denominator") != 8 or tick.get("group_denominator") != 4
            or tick.get("original_registration_times_preserved") is not True
            or tick.get("source_transport") != runner.MODE
            or any(tick.get(key) is not False for key in (
                "runtime_authorized", "telegram_authorized", "trading_authorized"))
            or tick.get("tick_sha256") != contracts.digest({key: value for key, value in tick.items()
                                                            if key != "tick_sha256"})):
        raise ValueError("RUNNER_RECEIPT_BINDING_MISMATCH")
    snapshot = args.work_dir / "working_registry_snapshot.tar.gz"
    if snapshot.is_symlink() or receipt["working_snapshot_sha256"] != _sha_file(snapshot):
        raise ValueError("RUNNER_RECEIPT_SNAPSHOT_MISMATCH")
    groups = tick["groups"]
    terminal = len(groups) == 4 and all(group["status"] in ("SELECTED", "BLOCKED_ACQUISITION") for group in groups)
    if output.get("terminal") is not terminal or output.get("clean_registry_shutdown") is not True:
        raise ValueError("RUNNER_STDOUT_TERMINAL_MISMATCH")
    pending_dates = [row["not_before_utc"] for group in groups for row in group.get("requests", [])
                     if row.get("status") not in ("ADMITTED", "BLOCKED") and row.get("not_before_utc")]
    return {"receipt_name": path.name, "receipt_sha256": _sha_file(path),
        "snapshot_sha256": receipt["working_snapshot_sha256"],
        "terminal": terminal, "selection_complete": tick["selection_complete"],
        "blocked_groups": sum(group["status"] == "BLOCKED_ACQUISITION" for group in groups),
        "error_count": len(tick["errors"]),
        "next_due_utc": min(pending_dates, key=contracts.utc) if pending_dates else None}


def terminal_latch(args, identity, progress=None):
    path = args.state_dir / "terminal.json"
    if progress is not None:
        if progress["terminal"] is not True:
            raise ValueError("ONLY_TERMINAL_RECEIPT_CAN_LATCH")
        body = {"version": VERSION, "identity": identity, "progress": progress}
        runner.atomic_json(path, {**body, "latch_sha256": contracts.digest(body)})
        return progress
    saved = _json_file(path, 64 * 1024)
    body = {key: value for key, value in saved.items() if key != "latch_sha256"}
    if (saved.get("version") != VERSION or saved.get("identity") != identity
            or saved.get("latch_sha256") != contracts.digest(body)):
        raise ValueError("TERMINAL_LATCH_IDENTITY_CHANGED")
    previous = saved["progress"]
    output = {"receipt": str(args.work_dir / "receipts" / previous["receipt_name"]),
              "terminal": True, "clean_registry_shutdown": True}
    checked = receipt_projection(args, output)
    if checked != previous:
        raise ValueError("TERMINAL_LATCH_EVIDENCE_CHANGED")
    return checked


class Stop:
    def __init__(self):
        self.event = threading.Event()
        self.signum = signal.SIGTERM

    def set(self, signum=signal.SIGTERM, _frame=None):
        self.signum = signum
        self.event.set()


def _heartbeat(args, state, *, progress=None, child_pid=None, error_type=None):
    observed = None
    if progress:
        if progress["terminal"]:
            observed = "TERMINAL"
        elif progress["error_count"]:
            observed = "OPERATIONAL_ERRORS"
        elif progress["next_due_utc"] and contracts.utc(progress["next_due_utc"]) > datetime.now(timezone.utc):
            observed = "WAITING_FOR_CUTOFF"
        else:
            observed = "DUE_OR_INCOMPLETE"
    value = {"version": VERSION, "state": state,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "supervisor_pid": os.getpid(), "child_pid": child_pid,
        "error_type": error_type, "progress": progress, "observed_work_state": observed,
        "metadata_preflight_status": getattr(args, "_preflight_status", None),
        "metadata_preflight_checked_at_utc": getattr(args, "_preflight_checked_at", None),
        "metadata_preflight_observed_this_start": getattr(args, "_preflight_observed_this_start", False),
        "host_durability_verified": False, "deployment_verified": False,
        "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
    runner.atomic_json(args.state_dir / "heartbeat.json", value)
    # Log only transitions/new receipts, never every heartbeat or child output.
    log_key = (state, error_type, observed, progress.get("receipt_sha256") if progress else None)
    if log_key != getattr(args, "_last_log_key", None):
        event = {"event": "RESEARCH_WORKER_STATE", "state": state,
            "observed_work_state": observed, "error_type": error_type,
            "metadata_preflight_status": value["metadata_preflight_status"]}
        if progress:
            event.update({key: progress[key] for key in (
                "terminal", "selection_complete", "blocked_groups", "error_count", "next_due_utc")})
        print(json.dumps(event, sort_keys=True), flush=True)
        args._last_log_key = log_key


def _hold(args, stop, state, *, progress=None, error_type=None):
    while not stop.event.is_set():
        _heartbeat(args, state, progress=progress, error_type=error_type)
        stop.event.wait(args.heartbeat_seconds)


def _preflight_wait(args, stop, error_type):
    deadline = time.monotonic() + args.interval_seconds
    while not stop.event.is_set():
        _heartbeat(args, "PREFLIGHT_RETRY", error_type=error_type)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        stop.event.wait(min(args.heartbeat_seconds, remaining))


def _signal_group(child, signum):
    try:
        os.killpg(child.pid, signum)
    except ProcessLookupError:
        pass


def _group_exists(child):
    try:
        os.killpg(child.pid, 0)
        return True
    except ProcessLookupError:
        return False


def _monitor(args, stop, command):
    """Leader-first drain leaves the DB alive; only the deadline hits the group."""
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, start_new_session=True)
    progress, failure, drain_at, forced = None, None, None, False
    buffer = bytearray()
    selector = selectors.DefaultSelector()
    for stream, label in ((child.stdout, "stdout"), (child.stderr, "stderr")):
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ, label)
    next_heartbeat = 0.0
    try:
        while selector.get_map() or child.poll() is None:
            now = time.monotonic()
            if (stop.event.is_set() or failure is not None) and drain_at is None:
                drain_at = now
                if child.poll() is None:
                    child.send_signal(stop.signum if stop.event.is_set() else signal.SIGTERM)
            if child.poll() is not None and selector.get_map() and drain_at is None:
                # A dead leader can leave descendants holding the pipes open.
                # Normal buffered output reaches EOF immediately; an orphan
                # group gets the same bounded drain deadline as shutdown.
                drain_at = now
            if drain_at is not None and not forced and now - drain_at >= args.shutdown_seconds:
                forced = True
                _signal_group(child, signal.SIGTERM)
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
                # The group can outlive its leader. Escalate descendants as well
                # instead of waiting forever on inherited stdout/stderr pipes.
                _signal_group(child, signal.SIGKILL)
                child.wait(timeout=3)
            if now >= next_heartbeat:
                state = "DRAINING" if drain_at is not None else (
                    "OPERATIONAL_ERRORS" if progress and progress["error_count"] else "RUNNING")
                _heartbeat(args, state, progress=progress, child_pid=child.pid,
                           error_type=failure)
                next_heartbeat = now + args.heartbeat_seconds
            for key, _ in selector.select(timeout=min(0.2, args.heartbeat_seconds)):
                chunk = os.read(key.fileobj.fileno(), MAX_LINE_BYTES + 1)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if key.data == "stderr":
                    # Child stderr can contain a driver message. Never retain,
                    # forward or publish it. Exit status is sufficient.
                    continue
                buffer.extend(chunk)
                if len(buffer) > MAX_LINE_BYTES:
                    failure = "RunnerOutputLimitExceeded"
                    buffer.clear()
                    continue
                while b"\n" in buffer:
                    raw, _, remainder = buffer.partition(b"\n")
                    buffer[:] = remainder
                    if failure is not None:
                        continue
                    try:
                        progress = receipt_projection(args, json.loads(raw))
                    except Exception as exc:
                        failure = type(exc).__name__
            if child.poll() is not None and not selector.get_map():
                break
        if buffer.strip() and failure is None:
            failure = "IncompleteRunnerOutput"
        if forced:
            failure = "ShutdownDeadlineExceeded"
        return child.returncode, progress, failure
    finally:
        selector.close()
        if child.poll() is None:
            # A heartbeat/storage error must not bypass the runner's drain.
            child.send_signal(signal.SIGTERM)
            remaining = args.shutdown_seconds if drain_at is None else max(
                0, args.shutdown_seconds - (time.monotonic() - drain_at))
            try:
                child.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                _signal_group(child, signal.SIGKILL)
                child.wait(timeout=3)
        if _group_exists(child):
            _signal_group(child, signal.SIGTERM)
            group_deadline = time.monotonic() + 2
            while _group_exists(child) and time.monotonic() < group_deadline:
                time.sleep(.02)
            if _group_exists(child):
                _signal_group(child, signal.SIGKILL)
        child.stdout.close()
        child.stderr.close()


def supervise(args, stop, *, identity_validator=enable_identity, command=None):
    """Internal test seams replace only startup validation and the child command."""
    with supervisor_lock(args.state_dir):
        progress = None
        if not args.enable_worker:
            _hold(args, stop, "DISABLED")
        else:
            try:
                completed = ((args.state_dir / "terminal.json").exists()
                             or (args.state_dir / "terminal.json").is_symlink())
                while not stop.event.is_set():
                    _heartbeat(args, "STARTING")
                    try:
                        identity = identity_validator(args, completed=completed)
                        break
                    except PreflightRetry as exc:
                        _preflight_wait(args, stop, exc.error_type)
                else:
                    _heartbeat(args, "STOPPED")
                    return 0
                if completed:
                    progress = terminal_latch(args, identity)
                    _hold(args, stop, "TERMINAL_HOLD", progress=progress)
                else:
                    code, progress, failure = _monitor(args, stop, command or runner_command(args))
                    if code == 0 and failure is None and progress and progress["terminal"]:
                        terminal_latch(args, identity, progress)
                        _hold(args, stop, "TERMINAL_HOLD", progress=progress)
                    elif not stop.event.is_set():
                        _hold(args, stop, "ERROR_HOLD", progress=progress,
                              error_type=failure or "UnexpectedRunnerExit")
                    elif failure is not None or code != 0:
                        _heartbeat(args, "STOPPED_UNCLEAN", progress=progress,
                                   error_type=failure or "NonzeroRunnerExit")
                        return 1
            except Exception as exc:
                if not stop.event.is_set():
                    _hold(args, stop, "ERROR_HOLD", progress=progress, error_type=type(exc).__name__)
                else:
                    _heartbeat(args, "STOPPED_UNCLEAN", progress=progress, error_type=type(exc).__name__)
                    return 1
        _heartbeat(args, "STOPPED", progress=progress)
        return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("checkpoint", "adapter-dir", "work-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--preflight-receipt", type=Path)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-url-env", default="RESEARCH_NO_HORIZON_READ_DATABASE_URL")
    parser.add_argument("--node", default=os.getenv("CODEX_PRIMARY_RUNTIME_NODE") or "node")
    parser.add_argument("--enable-worker", action="store_true",
                        default=os.getenv("NO_HORIZON_SUPERVISOR_ENABLED") == "1")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--heartbeat-seconds", type=int, default=30)
    parser.add_argument("--shutdown-seconds", type=int, default=240)
    args = parser.parse_args(argv)
    args.state_dir = args.state_dir or args.work_dir.with_name(args.work_dir.name + "-supervisor")
    args.preflight_receipt = args.preflight_receipt or args.work_dir.parent / "source-preflight.json"
    for name in ("checkpoint", "adapter_dir", "work_dir", "state_dir", "preflight_receipt"):
        path = getattr(args, name)
        if path.is_symlink():
            parser.error("symlink input rejected")
        setattr(args, name, path.resolve())
    for protected in (args.checkpoint, args.adapter_dir, args.work_dir,
                      args.work_dir.parent / "inputs", Path(__file__).resolve().parent):
        if (args.state_dir == protected or args.state_dir in protected.parents
                or protected in args.state_dir.parents):
            parser.error("supervisor state must be separate from code, inputs and research state")
        if args.preflight_receipt == protected or protected in args.preflight_receipt.parents:
            parser.error("preflight receipt must not overwrite code, inputs or research state")
    if args.preflight_receipt == args.state_dir or args.state_dir in args.preflight_receipt.parents:
        parser.error("preflight receipt must be separate from supervisor state")
    if (not 5 <= args.interval_seconds <= 86400 or not 1 <= args.heartbeat_seconds <= 60
            or not 1 <= args.shutdown_seconds <= 270
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", args.source_id)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.source_url_env)):
        parser.error("invalid supervisor configuration")
    stop = Stop()
    signal.signal(signal.SIGTERM, stop.set)
    signal.signal(signal.SIGINT, stop.set)
    return supervise(args, stop)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("WORKER_SUPERVISOR_FAILED: " + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
