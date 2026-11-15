#!/usr/bin/env python3
"""Isolated, disabled-by-default BTC/M1/12H source-only supervisor.

Private run_source_probe() is the sole source entry after review approval.
Linux userspace cannot guarantee cleanup
by a wall-clock deadline during kernel stalls, loss of its DB connection, or
external SIGKILL of this supervisor. Ordinary timeout/setsid/TERM-resistant
descendants are handled and independently testable without source requests.

Lock is held by a separate, read-only DB session. No schema, job, result,
image, environment, or application configuration is changed. This module
does not import app.py or initialize/claim/finish a job. Its approved child
can invoke exactly one browser using the pinned, unchanged source reader.
"""
import time

STARTED = time.monotonic()

import argparse
import asyncio
import base64
import ctypes
import errno
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import shutil
import hashlib
import math
from types import SimpleNamespace
from datetime import datetime, timezone
from uuid import uuid4

CRYPTO_WHEEL = 'pycryptodome-3.21.0-cp36-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl'
CRYPTO_SHA256 = '0714206d467fc911042d01ea3a1847c847bc10884cf674c82e12915cfe1649f8'


def activate_bundled_crypto():
    """Verify the prepared, private wheel tree; never install or fetch at runtime.

    Only this process's import path changes. The browser's allowlisted child
    environment never receives a dependency path, database access or key.
    """
    import importlib.metadata
    import platform
    import zipfile
    root = Path(__file__).resolve().parent
    vendor = root / 'vendor'
    wheel = root / 'dependencies' / CRYPTO_WHEEL
    if (sys.implementation.name != 'cpython' or sys.version_info < (3, 6)
            or sys.platform != 'linux' or platform.machine() != 'x86_64'):
        raise ValueError('session_crypto_platform')
    for path in (root, wheel.parent, vendor):
        if (path.is_symlink() or not path.is_dir() or path.stat().st_uid != os.geteuid()
                or path.stat().st_mode & 0o077):
            raise ValueError('session_crypto_permissions')
    if (wheel.is_symlink() or not wheel.is_file() or wheel.stat().st_uid != os.geteuid()
            or wheel.stat().st_mode & 0o077 or wheel.stat().st_size != 2254555
            or hashlib.sha256(wheel.read_bytes()).hexdigest() != CRYPTO_SHA256):
        raise ValueError('session_crypto_integrity')
    with zipfile.ZipFile(wheel) as archive:
        expected = {item.filename: item for item in archive.infolist() if not item.is_dir()}
        actual = set()
        for path in vendor.rglob('*'):
            if (path.is_symlink() or path.stat().st_uid != os.geteuid()
                    or path.stat().st_mode & 0o077):
                raise ValueError('session_crypto_permissions')
            if path.is_file():
                relative = path.relative_to(vendor).as_posix()
                if relative not in expected or path.read_bytes() != archive.read(relative):
                    raise ValueError('session_crypto_integrity')
                actual.add(relative)
            elif not path.is_dir():
                raise ValueError('session_crypto_integrity')
        if actual != set(expected):
            raise ValueError('session_crypto_incomplete')
    for name, module in tuple(sys.modules.items()):
        if name == 'Crypto' or name.startswith('Crypto.'):
            try: Path(module.__file__).resolve().relative_to(vendor)
            except (AttributeError, TypeError, ValueError):
                raise ValueError('session_crypto_preloaded_elsewhere') from None
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(vendor))
    distribution = importlib.metadata.distribution('pycryptodome')
    if distribution.version != '3.21.0' or Path(distribution.locate_file('')).resolve() != vendor:
        raise ValueError('session_crypto_import')
    from Crypto.Cipher import AES
    if Path(AES.__file__).resolve() != vendor / 'Crypto/Cipher/AES.py' or AES.MODE_GCM != 11:
        raise ValueError('session_crypto_import')
    # Exercise the packaged native library before any private input is read.
    key, nonce, plain = bytes(32), bytes(12), b'dummy-package-import-check'
    ciphertext, tag = AES.new(key, AES.MODE_GCM, nonce=nonce).encrypt_and_digest(plain)
    if AES.new(key, AES.MODE_GCM, nonce=nonce).decrypt_and_verify(ciphertext, tag) != plain:
        raise ValueError('session_crypto_native')
    return {'version': distribution.version, 'wheel_sha256': CRYPTO_SHA256,
            'module_relative_path': 'vendor/Crypto/Cipher/AES.py',
            'private_import_verified': True, 'native_aes_gcm_verified': True}

WORK_LOCK = 748230102
PR_SET_CHILD_SUBREAPER = 36
PR_GET_CHILD_SUBREAPER = 37
MAX_OUTPUT = 48 * 1024
TOTAL_SECONDS = 60.0
WORK_SECONDS = 40.0
CLEANUP_SECONDS = 55.0
LIVE_REVIEW_ACCEPTED = False  # Preparation only; a new live attempt needs separate owner approval.
SUBREAPER_READY = False
OWN_CHILDREN = []
SOURCE_OBSERVER_LAUNCHED = False
CANCELLED = False
CLEANUP_FAULT = False
COMMIT = "33bad18f8be322009108584a6b71f3d65fa9b351"
RUNTIME_HASHES = {
    "capture_readiness.py": "a21fcb00acfc4ba20bb1d54d6342e2f89521ad2d941792822cbbd20de7cc437a",
    "model1_execution.py": "5a82f410b089307d3ad465d3abb016f4616a0932c3f7c75a00a2584a95117693",
    "collection_bridge.py": "ccb98befa94c322888522771e435da32ceaa5ff16decf9882ef65c0e1e1f0543",
    "heatmap_models.py": "de2ed269648628bd124b83e159d4abb302cb024479721a7babfb0ad92ec7bf7e",
    "market_vision/coinglass_heatmap_capture.py": "3a8778e0c6de4b4c1e24dc8dea3292e2e6c5ce2e2163bad251c3edc12a2f129a",
}


class SupervisorFailure(Exception):
    """Only fixed codes, never exception messages from source/DB/processes."""


def emit(value):
    print(json.dumps(value, allow_nan=False, separators=(",", ":")), flush=True)


def proc_info(pid):
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        if int(raw.split(" ", 1)[0]) != pid:
            return None
        tail = raw[raw.rfind(")") + 2:].split()
        return {"pid": pid, "ppid": int(tail[1]), "start": int(tail[19]),
                "state": tail[0], "session": int(tail[3])}
    except (OSError, ValueError, IndexError):
        return None


def census():
    out = {}
    for name in os.listdir("/proc"):
        if name.isdecimal():
            info = proc_info(int(name))
            if info is not None:
                out[info["pid"]] = info
    return out


def descendants(exclude=None):
    """Current descendants only. Detached process groups are still descendants.

    A living lock-holder is the sole permitted exclusion. Its observer and
    every descendant remain included. Exclude only its exact PID/start identity.
    """
    rows = census()
    excluded = {item["pid"] for item in (exclude or ())
                if rows.get(item["pid"], {}).get("start") == item["start"]}
    owned = set()
    parents = {os.getpid()}
    while parents:
        found = {pid for pid, row in rows.items()
                 if row["ppid"] in parents and pid not in owned}
        owned.update(found)
        parents = found
    return [rows[pid] for pid in owned if pid not in excluded]


def kernel_children():
    try:
        found = set()
        for thread in Path("/proc/self/task").iterdir():
            found.update(int(value) for value in (thread / "children").read_text().split())
        return found
    except (OSError, ValueError):
        raise SupervisorFailure("kernel_children_unverifiable") from None


def signal_exact(info, signum):
    """Pin PID with pidfd and recheck generation before signaling."""
    fd = None
    try:
        fd = os.pidfd_open(info["pid"], 0)
        now = proc_info(info["pid"])
        if now is None or now["start"] != info["start"]:
            return False
        signal.pidfd_send_signal(fd, signum, None, 0)
        return True
    except ProcessLookupError:
        return False
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        raise SupervisorFailure("process_signal_unavailable") from None
    finally:
        if fd is not None:
            os.close(fd)


def reap(children):
    """Reap adopted descendants as well as our directly launched processes."""
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return
        for child in children:
            if child.pid == pid:
                child.returncode = os.waitstatus_to_exitcode(status)


def establish_subreaper():
    global SUBREAPER_READY
    if sys.platform != "linux" or not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise SupervisorFailure("linux_pidfd_required")
    # Tool containers can expose an ancestor namespace's procfs. Never signal
    # numeric /proc PIDs in that case: they mean different local processes.
    try:
        proc_pid = int(Path("/proc/self/stat").read_text().split(" ", 1)[0])
    except (OSError, ValueError):
        raise SupervisorFailure("proc_namespace_unverifiable") from None
    if proc_pid != os.getpid():
        raise SupervisorFailure("proc_namespace_mismatch")
    if descendants():
        raise SupervisorFailure("supervisor_already_has_children")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise SupervisorFailure("subreaper_unavailable")
    observed = ctypes.c_int()
    if libc.prctl(PR_GET_CHILD_SUBREAPER, ctypes.byref(observed), 0, 0, 0) != 0 or observed.value != 1:
        raise SupervisorFailure("subreaper_not_verified")
    # Verify pidfd permission/API using signal zero, which sends no signal.
    own = proc_info(os.getpid())
    if own is None or not signal_exact(own, 0):
        raise SupervisorFailure("pidfd_not_verified")
    SUBREAPER_READY = True


def confirm_worker_environment(*, compare_session=True):
    """Compare only authorized fields in memory; never emit values or hashes.

    Exact argv member excludes shell/-c strings which merely mention app.py.
    Procfs namespace consistency must already have passed.
    """
    matches = []
    wanted = b"standalone_collector/app.py"
    for name in os.listdir("/proc"):
        if not name.isdecimal():
            continue
        try:
            raw = Path(f"/proc/{name}/cmdline").read_bytes()
            if len(raw) <= 65536 and wanted in raw.rstrip(b"\0").split(b"\0"):
                info = proc_info(int(name))
                if info is not None:
                    matches.append(info)
        except OSError:
            continue
    if len(matches) != 1:
        raise SupervisorFailure("active_worker_not_unique")
    worker = matches[0]
    try:
        with Path(f"/proc/{worker['pid']}/environ").open("rb") as stream:
            raw = stream.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise SupervisorFailure("worker_environment_unverifiable")
        fields = (("COINGLASS_COOKIE_HEADER", "COINGLASS_STORAGE_STATE_JSON", "COINGLASS_SESSION_KEY", "DATABASE_URL", "COLLECTION_BRIDGE_ENABLED")
                  if compare_session else ("DATABASE_URL", "COLLECTION_BRIDGE_ENABLED"))
        selected = {}
        allowed = {name.encode(): name for name in fields}
        for item in raw.split(b"\0"):
            name, separator, value = item.partition(b"=")
            if separator and name in allowed:
                if allowed[name] in selected:
                    raise SupervisorFailure("worker_environment_unverifiable")
                selected[allowed[name]] = value
        equal = {name: selected.get(name) == (os.environ[name].encode() if name in os.environ else None)
                 for name in fields}
        now = proc_info(worker["pid"])
        if now is None or now["start"] != worker["start"]:
            raise SupervisorFailure("active_worker_changed")
        if not all(equal.values()):
            raise SupervisorFailure("worker_environment_differs")
        if (os.environ.get("COLLECTION_BRIDGE_ENABLED", "").strip().lower() != "false"
                or selected.get("COLLECTION_BRIDGE_ENABLED", b"").strip().lower() != b"false"):
            raise SupervisorFailure("maintenance_required")
        if not os.environ.get("DATABASE_URL", "").strip() or not os.environ.get("COINGLASS_SESSION_KEY", "").strip():
            raise SupervisorFailure("worker_environment_missing")
        return worker
    except (OSError, UnicodeError):
        raise SupervisorFailure("worker_environment_unverifiable") from None


def clean_owned(children, deadline, exclude=()):
    """Stop and kill only owned descendants, repeat after orphan adoption.

    Return False if the budget expires: never claim cleanup from a timeout
    exit code, a process-group kill, or the child report alone.
    """
    count = 0
    while time.monotonic() < deadline:
        reap(children)
        owned = descendants(exclude)
        if not owned:
            # Reaping + fresh census catches reparented/detached descendants.
            reap(children)
            excluded = {item["pid"] for item in exclude
                        if (proc_info(item["pid"]) or {}).get("start") == item["start"]}
            if not descendants(exclude) and not (kernel_children() - excluded):
                return True, count
        for info in owned:
            signal_exact(info, signal.SIGSTOP)
        # Rescan after stopping: catch children forked during first census.
        for info in descendants(exclude):
            count += int(signal_exact(info, signal.SIGKILL))
        time.sleep(0.01)
    reap(children)
    excluded = {item["pid"] for item in exclude
                if (proc_info(item["pid"]) or {}).get("start") == item["start"]}
    return not descendants(exclude) and not (kernel_children() - excluded), count


def safe_clean_owned(children, deadline, exclude=()):
    """A failure in one cleanup primitive must not skip later cleanup stages."""
    global CLEANUP_FAULT
    while time.monotonic() < deadline:
        try:
            return clean_owned(children, deadline, exclude)
        except BaseException:
            CLEANUP_FAULT = True
            # Direct children are independently enumerated by the kernel;
            # after root termination the subreaper adopts its descendants.
            try:
                excluded = {row["pid"] for row in exclude
                            if (proc_info(row["pid"]) or {}).get("start") == row["start"]}
                for pid in kernel_children() - excluded:
                    info = proc_info(pid)
                    if info is not None and info["ppid"] == os.getpid():
                        try:
                            signal_exact(info, signal.SIGKILL)
                        except BaseException:
                            pass
                reap(children)
            except BaseException:
                pass
            time.sleep(0.01)
    try:
        reap(children)
        excluded = {row["pid"] for row in exclude
                    if (proc_info(row["pid"]) or {}).get("start") == row["start"]}
        return not descendants(exclude) and not (kernel_children() - excluded), 0
    except BaseException:
        CLEANUP_FAULT = True
        return False, 0


def request_cancel(signum, frame):
    global CANCELLED
    CANCELLED = True


class OutputReader:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.buffers = {}
        self.overflow = False

    def add(self, child, label):
        for stream, suffix in ((child.stdout, "out"), (child.stderr, "err")):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, f"{label}_{suffix}")
            self.buffers[f"{label}_{suffix}"] = bytearray()

    def poll(self, seconds=0.02):
        for key, _ in self.selector.select(max(0.0, seconds)):
            try:
                chunk = os.read(key.fileobj.fileno(), 65536)
            except BlockingIOError:
                continue
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            buf = self.buffers[key.data]
            cap = MAX_OUTPUT if key.data.endswith("_out") else 4096
            if len(buf) + len(chunk) > cap:
                self.overflow = True
            if len(buf) < cap:
                buf.extend(chunk[:cap - len(buf)])

    def line(self, label):
        value = self.buffers.get(f"{label}_out", b"")
        if b"\n" not in value:
            return None
        try:
            parsed = json.loads(bytes(value).split(b"\n", 1)[0])
            return parsed if isinstance(parsed, dict) else None
        except (ValueError, UnicodeError):
            return None

    def messages(self, label):
        found = []
        for value in bytes(self.buffers.get(f"{label}_out", b"")).splitlines():
            try:
                item = json.loads(value)
                if isinstance(item, dict):
                    found.append(item)
            except (ValueError, UnicodeError):
                pass
        return found

    def close(self):
        self.selector.close()


def spawn(command, env=None, *, pass_fds=()):
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, close_fds=True,
                             start_new_session=True, env=env, pass_fds=pass_fds)
    OWN_CHILDREN.append(child)
    return child


def keeper_status(value):
    """A lost parent's output pipe is never permission to release its lock."""
    try:
        emit(value)
    except OSError:
        pass


def consume_keeper_control(ready, control):
    """Existing control pipe: C permanently prevents launches; R releases only later."""
    if control['input_closed'] or not ready.select(0): return
    try: command = os.read(sys.stdin.fileno(), 1)
    except OSError: command = b''
    if command in (b'C', b''):
        control['cancelled'] = True
        if command == b'':
            control['input_closed'] = True
            ready.unregister(sys.stdin)
        if not control['quiesced']:
            # This runs on the same event loop as the non-awaiting spawn
            # boundary: acknowledgement cannot race with a later fork.
            control['quiesced'] = True
            keeper_status({'observer': 'quiesced'})
    elif command == b'S':
        if not control['cancelled']: control['started'] = True
    elif command == b'R':
        if control['quiesced']: control['release'] = True
        else: keeper_status({'lock': 'release_denied'})


def await_lock_release(connection, owner, ready, control):
    """Only explicit R authorizes release; EOF is a fail-safe hold, not consent.

    On unverified descendant cleanup the parent can exit. Retaining this
    session then needs operator intervention and cannot satisfy a hard 60s
    cleanup guarantee. Live activation must accept that exceptional contract.
    """
    healthy = True
    while True:
        consume_keeper_control(ready, control)
        if control['release']: return
        time.sleep(0.25)
        if healthy:
            try:
                row = connection.execute("""SELECT pg_backend_pid() AS owner, EXISTS (
                    SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
                    AND pid=pg_backend_pid() AND classid=0 AND objid=%s
                    AND objsubid=1) AS held""", (WORK_LOCK,)).fetchone()
                if not row or row['owner'] != owner or row['held'] is not True:
                    raise SupervisorFailure('lock_lost')
            except BaseException:
                healthy = False
                keeper_status({'lock': 'unhealthy'})


async def managed_observer(connection, ready, control):
    """Keeper-owned candidate lifecycle; the existing pipe carries private state.

    Stdout relays only fixed status and the existing sanitized observer receipt.
    There is no queue claim, job write, DDL, AI call or alternate state store.
    """
    from session_runtime import ManagedSession, SessionError
    from session_state import PgSessionStore
    from collector_browser_fixture import sanitize_live_report
    child = transport = session = None
    reader = OutputReader()
    begun = False
    next_health = 0.0
    result = {'saved': False, 'outcome': None, 'revision': None, 'reason': None}
    async def watch_control():
        while True:
            consume_keeper_control(ready, control)
            await asyncio.sleep(0.02)
    control_task = asyncio.create_task(watch_control())
    try:
        while not control['started']:
            consume_keeper_control(ready, control)
            if control['cancelled']:
                raise SupervisorFailure('cancelled_before_source')
            await asyncio.sleep(0.02)
        mode = os.environ['COINGLASS_REUSE_MODE']
        if mode not in ('bootstrap', 'reuse'):
            raise SupervisorFailure('session_mode_invalid')
        key = base64.b64decode(os.environ.pop('COINGLASS_SESSION_KEY', ''), validate=True)
        if len(key) != 32:
            raise SupervisorFailure('session_key_invalid')
        store = PgSessionStore(connection, key)
        key = None
        raw = os.environ.pop('COINGLASS_STORAGE_STATE_JSON', None)
        if mode == 'bootstrap':
            state = json.loads(raw)
            try:
                store.bootstrap(state)
            finally:
                if isinstance(state, dict): state.clear()
                state = None
        else:
            expected = os.environ.get('COINGLASS_REUSE_EXPECTED_REVISION', '')
            if not expected.isdecimal() or not 0 < int(expected) < 2**63:
                raise SupervisorFailure('session_revision_unverified')
            row = connection.execute('SELECT revision,status FROM collector_private.coinglass_session_state WHERE slot=%s', ('coinglass',)).fetchone()
            if not row or row['status'] != 'ready' or row['revision'] != int(expected):
                raise SupervisorFailure('session_revision_changed')
        # Reuse never uses or falls back to the worker's environment snapshot.
        raw = None
        os.environ.pop('COINGLASS_COOKIE_HEADER', None)
        session = ManagedSession(store, str(uuid4()))
        await session.begin()
        begun = True
        environment = source_child_environment(
            Path(os.environ['COLLECTOR_SOURCE_RUNTIME']),
            Path(os.environ['COLLECTOR_SOURCE_OUTPUT']).parent,
            float(os.environ['COLLECTOR_LIVE_DEADLINE_MONOTONIC']))
        transport = await session.open_transport(environment)
        environment.clear()
        consume_keeper_control(ready, control)
        if control['cancelled']:
            raise SupervisorFailure('cancelled_before_source')
        # No await occurs between this check and spawn. The control task
        # cannot acknowledge quiescence until this boundary has completed.
        child = spawn([sys.executable, '-B', str(Path(__file__).with_name('collector_browser_fixture.py')),
                       '--live-worker'], transport.env, pass_fds=transport.pass_fds)
        transport.spawned()
        keeper_status({'observer': 'started'})
        reader.add(child, 'observer')
        while child.poll() is None:
            reader.poll(0)
            if reader.overflow:
                raise SupervisorFailure('diagnostic_output_limit')
            if time.monotonic() >= next_health:
                try: store._require_lock()
                except BaseException:
                    keeper_status({'lock': 'unhealthy'})
                    raise
                next_health = time.monotonic() + 0.25
            await asyncio.sleep(0.02)
        reader.poll(0)
        reports = [row for row in reader.messages('observer') if row.get('format') == 'collector-live-child.v1']
        report = sanitize_live_report(reports[0]) if len(reports) == 1 else None
        if report is not None:
            keeper_status(report)
    except BaseException as error:
        allowed = {'state_exists','state_missing','state_not_ready','state_expired','state_corrupt',
                   'state_store_failed','state_encrypt_failed','state_coordination_required',
                   'state_revision_conflict','session_transport_failed','session_access_requires_review',
                   'session_unconfirmed','diagnostic_output_limit','session_mode_invalid','session_key_invalid',
                   'session_revision_unverified','session_revision_changed','cancelled_before_source'}
        code = getattr(error, 'code', None)
        if isinstance(error, SupervisorFailure): code = str(error)
        result['reason'] = code if code in allowed else 'session_or_observer_failed'
        keeper_status({'observer': 'failed' if child is not None else 'not_started'})
    finally:
        # The supervisor owns termination/census. Never finalize while the
        # source child can still send an updated state, or release its lock.
        while child is not None and child.poll() is None:
            try: reader.poll(0)
            except BaseException: pass
            await asyncio.sleep(0.02)
        if begun:
            try:
                await session.finish(transport)
                row = connection.execute('SELECT revision,status FROM collector_private.coinglass_session_state WHERE slot=%s', ('coinglass',)).fetchone()
                if row and row['status'] == 'ready' and type(row['revision']) is int:
                    result.update(saved=True, outcome='ready', revision=row['revision'])
                else:
                    result['reason'] = 'session_not_ready'
            except BaseException as error:
                code = getattr(error, 'code', None)
                result['reason'] = code if code in {'state_store_failed','state_revision_conflict',
                    'session_transport_failed','session_access_requires_review','session_unconfirmed'} else 'session_save_failed'
        if child is not None:
            keeper_status({'observer': 'completed', 'returncode': child.returncode})
            for stream in (child.stdin, child.stdout, child.stderr):
                try: stream.close()
                except (OSError, ValueError): pass
        reader.close()
        keeper_status({'session_saved': result})
        control_task.cancel()
        try: await control_task
        except asyncio.CancelledError: pass


def lock_holder():
    """The existing keeper owns the lock and the candidate private-store writes."""
    connection = None
    try:
        crypto = activate_bundled_crypto()
        import psycopg
        from psycopg.rows import dict_row
        dsn = os.environ.get("DATABASE_URL", "")
        if not dsn:
            emit({"lock": "unconfigured"})
            return 1
        connection = psycopg.connect(
            dsn, autocommit=True, connect_timeout=3, row_factory=dict_row,
            options="-c statement_timeout=1000 -c lock_timeout=1000")
        if connection.execute("SELECT pg_try_advisory_lock(%s) AS held", (WORK_LOCK,)).fetchone()['held'] is not True:
            emit({"lock": "busy"})
            return 2
        if connection.execute("SELECT 1 FROM public.ai_collection_bridge_jobs WHERE status IN ('queued','running') LIMIT 1").fetchone():
            emit({"lock": "active_row"})
            return 2
        owner = connection.execute("SELECT pg_backend_pid() AS owner").fetchone()['owner']
        emit({"lock": "held_idle", "crypto_dependency": crypto})
        ready = selectors.DefaultSelector()
        ready.register(sys.stdin, selectors.EVENT_READ)
        control = {'started': False, 'cancelled': False, 'quiesced': False, 'release': False, 'input_closed': False}
        try:
            asyncio.run(managed_observer(connection, ready, control))
        except BaseException:
            # Preserve the original keeper contract even if orchestration
            # fails: only the supervisor's confirmed-cleanup R releases it.
            keeper_status({'observer': 'failed'})
        await_lock_release(connection, owner, ready, control)
        ready.close()
        connection.close()
        connection = None
        emit({"lock": "released"})
        return 0
    except BaseException:
        emit({"lock": "unavailable"})
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                pass


def fixture(code, reader, label):
    child = spawn([sys.executable, "-B", "-c", code])
    reader.add(child, label)
    return child


def _self_test_body():
    """No DB, browser, requests, secrets, source, or product file accesses."""
    establish_subreaper()
    results = []
    cases = {
        "normal_exit": "import json;print(json.dumps({'ready':True}),flush=True)",
        "term_ignoring_detached_descendant": """import os,signal,time,json
signal.signal(signal.SIGTERM,signal.SIG_IGN)
p=os.fork()
if p==0:
 os.setsid();signal.signal(signal.SIGTERM,signal.SIG_IGN)
 p2=os.fork()
 if p2==0:
  os.setsid();signal.signal(signal.SIGTERM,signal.SIG_IGN)
  while True:time.sleep(1)
 while True:time.sleep(1)
print(json.dumps({'ready':True}),flush=True)
while True:time.sleep(1)
""",
        "root_exits_leaving_detached_orphan": """import os,time,json
if os.fork()==0:
 os.setsid()
 while True:time.sleep(1)
print(json.dumps({'ready':True}),flush=True)
""",
        "setup_failure_with_orphan": """import os,time,json
if os.fork()==0:
 os.setsid()
 while True:time.sleep(1)
print(json.dumps({'ready':True}),flush=True)
raise RuntimeError('synthetic setup error')
""",
    }
    for label, code in cases.items():
        started = time.monotonic()
        reader = OutputReader()
        child = fixture(code, reader, label)
        while time.monotonic() - started < 0.3:
            reader.poll()
            reap([child])
        ready = reader.line(label) == {"ready": True}
        before = [row for row in descendants() if row["state"] != "Z"]
        if label == "normal_exit":
            exercised = ready and child.returncode == 0
        elif label == "term_ignoring_detached_descendant":
            exercised = ready and len(before) >= 3 and len({row["session"] for row in before}) >= 3
        else:
            exercised = ready and child.returncode is not None and any(
                row["session"] != child.pid for row in before)
        clean, signals = clean_owned([child], time.monotonic() + 2)
        reader.close()
        results.append({"case": label, "pass": exercised and clean and not descendants() and child.returncode is not None,
                        "scenario_exercised": exercised,
                        "root_reaped": child.returncode is not None,
                        "kill_signals": signals,
                        "elapsed_seconds": round(time.monotonic() - started, 3)})
        for stream in (child.stdin, child.stdout, child.stderr):
            stream.close()
    # A separately owned guard stands in for lock keeper and must survive.
    reader = OutputReader()
    guard = fixture("import time,json;print(json.dumps({'ready':True}),flush=True);time.sleep(30)", reader, "guard")
    guard_info = proc_info(guard.pid)
    worker = fixture(cases["term_ignoring_detached_descendant"], reader, "worker")
    until = time.monotonic() + 0.3
    while time.monotonic() < until:
        reader.poll()
    guard_ready = reader.line("guard") == {"ready": True}
    worker_ready = reader.line("worker") == {"ready": True}
    if guard_info is None:
        raise SupervisorFailure("fixture_guard_unverifiable")
    cleaned, _ = clean_owned([guard, worker], time.monotonic() + 2, [guard_info])
    guard_survived = guard.poll() is None and proc_info(guard.pid) is not None
    results.append({"case": "keeper_survives_until_observer_cleanup", "pass": cleaned and guard_survived and guard_ready and worker_ready})
    final_clean, _ = clean_owned([guard, worker], time.monotonic() + 2)
    reader.close()
    for child in (guard, worker):
        for stream in (child.stdin, child.stdout, child.stderr):
            stream.close()
    results.append({"case": "final_subtree_empty", "pass": final_clean and not descendants()})
    report = {"format_version": "observation-supervisor-offline-tests.v1",
              "network_requests": 0, "db_connections": 0, "browser_launches": 0,
              "product_writes": 0, "cases": results,
              "passed": all(item["pass"] for item in results),
              "elapsed_seconds": round(time.monotonic() - STARTED, 3),
              "live_entry_enabled": LIVE_REVIEW_ACCEPTED}
    emit(report)
    return 0 if report["passed"] else 1


def self_test():
    try:
        return _self_test_body()
    except BaseException as exc:
        if not SUBREAPER_READY:
            raise
        cleaned, _ = clean_owned(OWN_CHILDREN, time.monotonic() + 3)
        emit({"format_version": "observation-supervisor-offline-tests.v1",
              "passed": False, "reason": "selftest_exception",
              "own_children_cleaned": cleaned, "network_requests": 0,
              "db_connections": 0, "browser_launches": 0, "product_writes": 0})
        return 1


def verify_runtime(runtime):
    if os.environ.get("RENDER_GIT_COMMIT") != COMMIT:
        raise SupervisorFailure("runtime_commit_mismatch")
    for name, expected in RUNTIME_HASHES.items():
        try:
            actual = hashlib.sha256((runtime / name).read_bytes()).hexdigest()
        except OSError:
            raise SupervisorFailure("runtime_file_unavailable") from None
        if actual != expected:
            raise SupervisorFailure("runtime_hash_mismatch")


def read_child_report(reader, checkpoint):
    messages = reader.messages("observer")
    candidate = messages[0] if len(messages) == 1 else None
    origin = "child_after_cleanup"
    if candidate is None and checkpoint is not None:
        try:
            if checkpoint.is_symlink() or checkpoint.stat().st_size > MAX_OUTPUT:
                return None, None
            candidate = json.loads(checkpoint.read_text())
            origin = "checkpoint_before_cleanup"
        except (OSError, ValueError):
            return None, None
    if not isinstance(candidate, dict) or candidate.get("format_version") != "coinglass-observation.v1":
        return None, None
    return candidate, origin


def source_child_environment(runtime, temporary, deadline):
    """Use the current worker's exact session inputs, privately in process memory."""
    env = {key: os.environ[key] for key in ('PATH', 'HOME', 'LANG', 'LC_CTYPE',
           'PLAYWRIGHT_BROWSERS_PATH') if key in os.environ}
    env.update(PYTHONDONTWRITEBYTECODE='1', COLLECTION_HEATMAP_MODEL='1',
               COLLECTOR_SOURCE_SUPERVISED='1', COLLECTOR_SOURCE_RUNTIME=str(runtime),
               COLLECTOR_SOURCE_OUTPUT=str(temporary / 'capture'),
               COLLECTOR_LIVE_DEADLINE_MONOTONIC=str(deadline),
               COLLECTOR_ACCOUNT_AUTH_ACCEPTANCE='1')
    # Preserve/compare the real worker above; exclude its legacy header from
    # this restoration child only. No secret backup or worker mutation.
    env['COINGLASS_COOKIE_HEADER'] = ''
    return env


def read_source_report(reader):
    from collector_browser_fixture import sanitize_live_report
    values = reader.messages('holder')
    values = [value for value in values if value.get('format') == 'collector-live-child.v1']
    return (sanitize_live_report(values[0]), 'child_after_cleanup') if len(values) == 1 else (None, None)


def execute_once(args):
    """One source invocation. No retry path exists in this supervisor."""
    global SOURCE_OBSERVER_LAUNCHED
    if not LIVE_REVIEW_ACCEPTED:
        return {'format_version': 'coinglass-source-probe.v1', 'status': 'blocked',
                'reason': 'live_entry_disabled', 'source_observer_launches': 0,
                'ai_calls': 0, 'job_writes': 0}
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, request_cancel)
    checked_at = datetime.now(timezone.utc).isoformat()
    reader = OutputReader()
    holder = holder_info = checkpoint = temporary = None
    observer_code = None
    session_saved = None
    keeper_crypto = None
    reason = "preflight"
    source_started = False
    lock_was_healthy = True
    cleanup_confirmed = False
    release_confirmed = False
    cleanup_deadline_missed = False
    report = None
    try:
        establish_subreaper()
        runtime = Path(args.runtime).resolve(strict=True)
        observer_path = Path(args.observer).resolve(strict=True)
        verify_runtime(runtime)
        worker_identity = confirm_worker_environment(compare_session=True)
        if CANCELLED:
            raise SupervisorFailure("cancelled_before_source")
        if time.monotonic() >= STARTED + 8:
            raise SupervisorFailure("preflight_budget_exhausted")
        temporary = Path(tempfile.mkdtemp(prefix="coinglass-observation-", dir="/tmp"))
        temporary.chmod(0o700)
        holder_env = {key: os.environ[key] for key in ('PATH', 'HOME', 'LANG', 'LC_CTYPE', 'PLAYWRIGHT_BROWSERS_PATH',
                      'DATABASE_URL', 'COINGLASS_SESSION_KEY', 'COINGLASS_REUSE_MODE',
                      'COINGLASS_REUSE_EXPECTED_REVISION')
                      if key in os.environ}
        if os.environ.get('COINGLASS_REUSE_MODE') == 'bootstrap':
            holder_env['COINGLASS_STORAGE_STATE_JSON'] = os.environ['COINGLASS_STORAGE_STATE_JSON']
        holder_env.update(COLLECTOR_SOURCE_RUNTIME=str(runtime),
                          COLLECTOR_SOURCE_OUTPUT=str(temporary / 'capture'),
                          COLLECTOR_LIVE_DEADLINE_MONOTONIC=str(STARTED + WORK_SECONDS),
                          PYTHONDONTWRITEBYTECODE='1')
        holder_env['COINGLASS_SUPERVISOR_INTERNAL'] = 'lock-holder-v1'
        holder = spawn([sys.executable, "-B", str(Path(__file__).resolve()), "--lock-holder"], holder_env)
        holder_env.clear()
        reader.add(holder, "holder")
        while time.monotonic() < STARTED + 12:
            if CANCELLED:
                raise SupervisorFailure("cancelled_before_source")
            reader.poll()
            messages = reader.messages("holder")
            if messages:
                if messages[0].get("lock") != "held_idle":
                    raise SupervisorFailure("lock_not_available")
                keeper_crypto = messages[0].get('crypto_dependency')
                break
            if holder.poll() is not None:
                raise SupervisorFailure("lock_helper_failed")
        else:
            raise SupervisorFailure("lock_setup_timeout")
        holder_info = proc_info(holder.pid)
        if holder_info is None:
            raise SupervisorFailure("lock_holder_unverifiable")
        if holder.poll() is not None or any(item.get('lock') != 'held_idle' for item in reader.messages('holder') if 'lock' in item):
            lock_was_healthy = False
            raise SupervisorFailure('lock_unhealthy')
        if CANCELLED:
            raise SupervisorFailure("cancelled_before_source")
        holder.stdin.write(b'S')
        holder.stdin.flush()
        reason = "work_deadline"
        while time.monotonic() < STARTED + WORK_SECONDS:
            reader.poll()
            messages = reader.messages('holder')
            if any(item.get('observer') == 'started' for item in messages):
                source_started = SOURCE_OBSERVER_LAUNCHED = True
            if CANCELLED:
                reason = "cancelled"
                break
            if holder.poll() is not None or any(item.get("lock") != "held_idle" for item in messages if 'lock' in item):
                lock_was_healthy = False
                reason = "lock_unhealthy"
                break
            worker_now = proc_info(worker_identity["pid"])
            if worker_now is None or worker_now["start"] != worker_identity["start"]:
                reason = "active_worker_changed"
                break
            if reader.overflow:
                reason = "diagnostic_output_limit"
                break
            if any(item.get('observer') in ('failed', 'not_started') for item in messages):
                reason = 'observer_failed' if source_started else 'session_setup_failed'
                break
            completed = [item for item in messages if item.get('observer') == 'completed'
                         and type(item.get('returncode')) is int]
            if len(completed) == 1:
                observer_code = completed[0]['returncode']
                reason = "observer_completed"
                break
    except SupervisorFailure as exc:
        reason = str(exc)
    except BaseException:
        reason = "supervisor_interrupted_or_failed"
    finally:
        if holder_info is None and holder is not None and holder.poll() is None:
            holder_info = proc_info(holder.pid)
        excluded = [holder_info] if holder_info is not None and holder is not None and holder.poll() is None else []
        quiesced = holder is None or holder.poll() is not None
        if not quiesced:
            try:
                holder.stdin.write(b'C')
                holder.stdin.flush()
                while time.monotonic() < STARTED + CLEANUP_SECONDS:
                    reader.poll()
                    if any(item.get('observer') == 'quiesced' for item in reader.messages('holder')):
                        quiesced = True
                        break
                    if holder.poll() is not None:
                        quiesced = True
                        break
            except BaseException:
                pass
        cleanup_confirmed = not OWN_CHILDREN
        if SUBREAPER_READY:
            cleanup_confirmed, _ = safe_clean_owned(OWN_CHILDREN, STARTED + CLEANUP_SECONDS, excluded)
        cleanup_confirmed = bool(quiesced and cleanup_confirmed)
        if not cleanup_confirmed:
            # Never silently release a healthy lock with observation processes
            # still present. Report an actual deadline failure immediately.
            cleanup_deadline_missed = True
            emit({"format_version": "coinglass-supervisor-alert.v1",
                  "status": "failed", "reason": "cleanup_not_verified_within_budget",
                  "lock_release_requested": False,
                  "elapsed_seconds": round(time.monotonic() - STARTED, 3)})
            # One bounded final cleanup window, never an unbounded hold.
            # If the OS still cannot terminate a process, report failure;
            # do not claim that the deadline or cleanup guarantee succeeded.
            cleanup_confirmed, _ = safe_clean_owned(OWN_CHILDREN, STARTED + 58, excluded)
            cleanup_confirmed = bool(quiesced and cleanup_confirmed)
        origin = None
        try:
            reader.poll(0)
            if any(item.get('observer') == 'started' for item in reader.messages('holder')):
                source_started = SOURCE_OBSERVER_LAUNCHED = True
            report, origin = read_source_report(reader)
        except BaseException:
            reason = "diagnostic_report_failed"
        if cleanup_confirmed and holder is not None and holder.poll() is None:
            try:
                holder.stdin.write(b"R")
                holder.stdin.flush()
            except (OSError, ValueError):
                pass
            while holder.poll() is None and time.monotonic() < STARTED + 58:
                try:
                    reader.poll()
                except BaseException:
                    reason = "diagnostic_pipe_failed"
                    break
            try:
                reader.poll(0)
            except BaseException:
                reason = "diagnostic_pipe_failed"
            release_confirmed = any(item.get("lock") == "released" for item in reader.messages("holder"))
            completed = [item for item in reader.messages('holder') if item.get('observer') == 'completed'
                         and type(item.get('returncode')) is int]
            if len(completed) == 1: observer_code = completed[0]['returncode']
            saved = [item.get('session_saved') for item in reader.messages('holder') if 'session_saved' in item]
            if len(saved) == 1 and isinstance(saved[0], dict):
                value = saved[0]
                if (set(value) == {'saved','outcome','revision','reason'} and type(value['saved']) is bool
                        and value['outcome'] in (None,'ready')
                        and (value['revision'] is None or type(value['revision']) is int and 0 < value['revision'] < 2**63)
                        and value['reason'] in (None,'state_exists','state_missing','state_not_ready','state_expired',
                            'state_corrupt','state_store_failed','state_encrypt_failed','state_coordination_required',
                            'state_revision_conflict','session_transport_failed','session_access_requires_review',
                            'session_unconfirmed','diagnostic_output_limit','session_mode_invalid','session_key_invalid',
                            'session_or_observer_failed','session_not_ready','session_save_failed',
                            'session_revision_unverified','session_revision_changed','cancelled_before_source')):
                    session_saved = value
        # A failed observer cleanup must never intentionally kill the keeper
        # or request release. Keeper treats later EOF as a fault/hold, so this
        # exceptional path cannot promise completion inside 60 seconds.
        preserve_keeper = not cleanup_confirmed and bool(excluded)
        all_clean = not OWN_CHILDREN
        if SUBREAPER_READY:
            all_clean, _ = safe_clean_owned(OWN_CHILDREN, STARTED + 59,
                                           excluded if preserve_keeper else ())
        cleanup_confirmed = cleanup_confirmed and all_clean
        for child in OWN_CHILDREN:
            if preserve_keeper and child is holder:
                continue
            for stream in (child.stdin, child.stdout, child.stderr):
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass
        lock_was_healthy = lock_was_healthy and not any(
            item.get("lock") in ("unhealthy", "unavailable") for item in reader.messages("holder"))
        try:
            reader.close()
        except BaseException:
            reason = "diagnostic_pipe_failed"
        evidence_retained = False
        temporary_files_removed = temporary is None
        if temporary is not None and cleanup_confirmed:
            try:
                # The child has removed private profiles and only its strict,
                # filtered receipt and verified map artifacts may leave /tmp.
                if report is not None and report.get('private_output_deleted') is True:
                    allowed = report.get('artifact_files', [])
                    source = temporary / 'capture'
                    if allowed:
                        if allowed not in (['receipt.json'], ['map.png', 'hover.json', 'receipt.json'],
                                           ['receipt.json', 'site_code.json'],
                                           ['map.png', 'hover.json', 'receipt.json', 'site_code.json']):
                            raise ValueError('artifact_set_invalid')
                        if source.is_symlink() or set(p.name for p in source.iterdir()) != set(allowed):
                            raise ValueError('artifact_set_invalid')
                        for name in allowed:
                            path = source / name
                            limit = (8 * 1024 * 1024 if name == 'map.png' else
                                     3 * 1024 * 1024 if name == 'site_code.json' else 2 * 1024 * 1024)
                            if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
                                raise ValueError('artifact_invalid')
                            key = {'map.png': 'map_png_sha256', 'hover.json': 'hover_sha256',
                                   'site_code.json': 'site_code_sha256'}.get(name)
                            if key and hashlib.sha256(path.read_bytes()).hexdigest() != report.get(key):
                                raise ValueError('artifact_digest_mismatch')
                        destination = Path(args.observer).parent / ('source_evidence-' + os.environ['COINGLASS_REUSE_MODE'])
                        destination.mkdir(mode=0o700, exist_ok=False)
                        for name in allowed:
                            os.replace(source / name, destination / name)
                        evidence_retained = True
            except (OSError, ValueError):
                reason = "evidence_retention_failed"
            finally:
                try:
                    shutil.rmtree(temporary)
                    temporary_files_removed = not temporary.exists()
                except OSError:
                    reason = "temporary_cleanup_failed"
        elapsed = time.monotonic() - STARTED
        if source_started and reason == "observer_completed" and observer_code != 0:
            reason = "observer_failed"
        success = (source_started and reason == "observer_completed" and cleanup_confirmed
                   and release_confirmed and lock_was_healthy and elapsed < TOTAL_SECONDS
                   and not cleanup_deadline_missed and report is not None
                   and report.get('status') == 'passed'
                   and report.get('artifact_status') == 'retained' and evidence_retained and temporary_files_removed
                   and report.get('access_verified') is True
                   and all((report.get('session_restore') or {}).get(key) is True for key in
                           ('exclusive_input_validated', 'legacy_header_absent', 'context_state_applied',
                            'cookies_snapshot_observed', 'private_state_released'))
                   and observer_code == 0 and session_saved is not None and session_saved.get('saved') is True
                   and not CLEANUP_FAULT and not CANCELLED)
        receipt = {"format_version": "coinglass-source-probe.v1", "checked_at": checked_at,
              "status": "completed" if success else "failed" if source_started else "blocked",
              "reason": reason, "source_observer_launches": int(source_started),
              "ai_calls": 0, "job_writes": 0, "own_descendants_gone": cleanup_confirmed,
              "no_future_source_launch_confirmed": quiesced,
              "lock_healthy_during_observation": lock_was_healthy if source_started else None,
              "lock_release_confirmed": release_confirmed,
              "keeper_retained_for_safety": preserve_keeper,
              "operator_intervention_required": not cleanup_confirmed or not temporary_files_removed,
              "within_60_seconds": elapsed < TOTAL_SECONDS and cleanup_confirmed,
              "elapsed_seconds": round(elapsed, 3),
              "report_origin": origin, "observation_report": report,
              "session_mode": os.environ.get('COINGLASS_REUSE_MODE'), "session_persistence": session_saved,
              "keeper_crypto_dependency": keeper_crypto,
              "filtered_evidence_retained": evidence_retained,
              "temporary_files_removed": temporary_files_removed}
    return receipt


SOURCE_PROBE_CALLED = False


def run_source_probe(started_monotonic):
    """Private, one-shot entry using the already configured session, unchanged.

    The supplied clock begins AFTER private input, before this entry. No retry.
    Queue-idle is a snapshot after WORK_LOCK acquisition. New POSTs may queue;
    they cannot capture until this session releases the worker lock.
    """
    global STARTED, SOURCE_PROBE_CALLED
    blocked = {'format_version': 'coinglass-source-probe.v1', 'status': 'blocked',
               'reason': 'live_entry_disabled', 'source_observer_launches': 0,
               'ai_calls': 0, 'job_writes': 0}
    if not LIVE_REVIEW_ACCEPTED:
        return blocked
    if SOURCE_PROBE_CALLED:
        return {**blocked, 'reason': 'source_attempt_already_consumed'}
    if (type(started_monotonic) not in (int, float) or not math.isfinite(started_monotonic)
            or not 0 <= time.monotonic() - started_monotonic < WORK_SECONDS):
        return {**blocked, 'reason': 'invalid_deadline'}
    if os.environ.get('COLLECTION_BRIDGE_ENABLED', '').strip().lower() != 'false':
        return {**blocked, 'reason': 'maintenance_required'}
    if not os.environ.get('COINGLASS_SESSION_KEY', '').strip():
        return {**blocked, 'reason': 'configured_session_missing'}
    SOURCE_PROBE_CALLED = True
    STARTED = started_monotonic
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        args = SimpleNamespace(runtime=Path.cwd() / 'standalone_collector/runtime',
                               observer=Path(__file__).with_name('collector_browser_fixture.py'))
        return execute_once(args)
    except BaseException:
        return {**blocked, 'status': 'failed' if SOURCE_OBSERVER_LAUNCHED else 'blocked',
                'reason': 'probe_receipt_unavailable' if SOURCE_OBSERVER_LAUNCHED else 'probe_setup_failed',
                'source_observer_launches': int(SOURCE_OBSERVER_LAUNCHED),
                'own_descendants_gone': False, 'lock_release_confirmed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--source-once", action="store_true")
    parser.add_argument("--lock-holder", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--execute-once", action="store_true")
    parser.add_argument("--validated-self-test", action="store_true")
    parser.add_argument("--observer")
    parser.add_argument("--runtime")
    parser.add_argument("--measurement-js")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.source_once:
        report = run_source_probe(time.monotonic())
        emit(report)
        return 0 if report.get('status') == 'completed' else 2
    if args.lock_holder:
        # Cannot be called accidentally just by importing or running this file.
        if os.environ.get("COINGLASS_SUPERVISOR_INTERNAL") != "lock-holder-v1":
            emit({"status": "blocked", "reason": "private_mode"})
            return 2
        return lock_holder()
    if args.execute_once:
        emit({'status': 'blocked', 'reason': 'private_source_entry_required',
              'source_attempts': 0, 'ai_calls': 0, 'job_writes': 0})
        return 2
    parser.print_help()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SupervisorFailure as exc:
        emit({"status": "failed" if SOURCE_OBSERVER_LAUNCHED else "blocked",
              "reason": str(exc), "source_observer_launches": int(SOURCE_OBSERVER_LAUNCHED)})
        raise SystemExit(2) from None
