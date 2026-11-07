#!/usr/bin/env python3
"""Serial Render-only synthetic browser matrix with independent process cleanup.

Linux subreaper/pidfd/census helpers are adapted from the reviewed November 7
observation supervisor. No credentials, remote service, database or AI is used.
Each worker gets <=45 seconds; cleanup reserves <=4 seconds (total <=50).
"""
import argparse
import ctypes
import errno
import fcntl
import importlib.util
import json
import math
import os
from pathlib import Path
import resource
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
WORKER = HERE / 'collector_browser_fixture.py'
EVIDENCE = Path('/lab/evidence/collector')
MAX_OUTPUT = 64 * 1024
WORKER_SECONDS = 45
CLEANUP_SECONDS = 4
MAX_REPORT_BYTES = 64 * 1024
LIVE_REASONS = frozenset({'not_started', 'invalid_live_input', 'invalid_live_start',
    'live_budget_expired', 'core_dumps_not_disabled', 'serial_lock_busy',
    'serial_lock_unavailable', 'live_attempt_already_consumed', 'live_marker_unavailable',
    'worker_timeout', 'worker_output_limit', 'worker_result_missing', 'worker_result_invalid',
    'live_capture_verified', 'live_source_stopped', 'live_child_failed',
    'supervisor_interrupted', 'supervisor_failed', 'cleanup_unverified',
    'transient_cleanup_unverified', 'browser_process_not_observed', 'cleanup_deadline_exceeded',
    'receipt_write_failed',
    'linux_pidfd_required', 'proc_namespace_unverifiable', 'proc_namespace_mismatch',
    'supervisor_already_has_children', 'subreaper_unavailable', 'subreaper_not_verified',
    'pidfd_not_verified', 'process_signal_unavailable', 'kernel_children_unverifiable'})
# On an unverified cleanup, keep the lock descriptor alive until this supervisor
# exits. This is not a claim that a filesystem lock survives process termination.
_LIVE_RETAINED_LOCKS = []


class SupervisorFailure(Exception):
    pass


def emit_report(report):
    serialized = json.dumps(report, allow_nan=False, separators=(',', ':'))
    within_limit = len(serialized.encode('utf8')) + 1 <= MAX_REPORT_BYTES
    if not within_limit:
        serialized = json.dumps({'format': 'collector-browser-output.v1', 'status': 'failed',
                                 'reason': 'output_limit',
                                 'matrix_artifact': str(EVIDENCE / 'matrix.json')
                                 if (EVIDENCE / 'matrix.json').is_file() else None}, separators=(',', ':'))
    print(serialized, flush=True)
    return within_limit


def proc_info(pid):
    try:
        raw = Path(f'/proc/{pid}/stat').read_text()
        if int(raw.split(' ', 1)[0]) != pid:
            return None
        name = raw[raw.find('(') + 1:raw.rfind(')')]
        tail = raw[raw.rfind(')') + 2:].split()
        return {'pid': pid, 'ppid': int(tail[1]), 'start': int(tail[19]),
                'state': tail[0], 'browser': name in ('chrome', 'chromium', 'headless_shell')}
    except (OSError, ValueError, IndexError):
        return None


def descendants():
    rows = {}
    for name in os.listdir('/proc'):
        if name.isdecimal():
            value = proc_info(int(name))
            if value is not None:
                rows[value['pid']] = value
    owned, parents = set(), {os.getpid()}
    while parents:
        found = {pid for pid, row in rows.items() if row['ppid'] in parents and pid not in owned}
        owned.update(found)
        parents = found
    return [rows[pid] for pid in owned]


def kernel_children():
    try:
        found = set()
        for thread in Path('/proc/self/task').iterdir():
            found.update(int(value) for value in (thread / 'children').read_text().split())
        return found
    except (OSError, ValueError):
        raise SupervisorFailure('kernel_children_unverifiable') from None


def signal_exact(info, signum):
    descriptor = None
    try:
        descriptor = os.pidfd_open(info['pid'], 0)
        current = proc_info(info['pid'])
        if current is None or current['start'] != info['start']:
            return False
        signal.pidfd_send_signal(descriptor, signum, None, 0)
        return True
    except ProcessLookupError:
        return False
    except OSError as error:
        if error.errno == errno.ESRCH:
            return False
        raise SupervisorFailure('process_signal_unavailable') from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def reap(child):
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return
        if child is not None and child.pid == pid:
            child.returncode = os.waitstatus_to_exitcode(status)


def establish_subreaper():
    if sys.platform != 'linux' or not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise SupervisorFailure('linux_pidfd_required')
    try:
        proc_pid = int(Path('/proc/self/stat').read_text().split(' ', 1)[0])
    except (OSError, ValueError):
        raise SupervisorFailure('proc_namespace_unverifiable') from None
    if proc_pid != os.getpid():
        raise SupervisorFailure('proc_namespace_mismatch')
    if descendants() or kernel_children():
        raise SupervisorFailure('supervisor_already_has_children')
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise SupervisorFailure('subreaper_unavailable')
    observed = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(observed), 0, 0, 0) != 0 or observed.value != 1:
        raise SupervisorFailure('subreaper_not_verified')
    own = proc_info(os.getpid())
    if own is None or not signal_exact(own, 0):
        raise SupervisorFailure('pidfd_not_verified')


def cleanup(child, deadline):
    signals, fault = 0, False
    while time.monotonic() < deadline:
        try:
            reap(child)
            owned = descendants()
            if not owned:
                reap(child)
                if not descendants() and not kernel_children():
                    return True, signals, fault
            for info in owned:
                signal_exact(info, signal.SIGSTOP)
            for info in descendants():
                signals += int(signal_exact(info, signal.SIGKILL))
        except BaseException:
            fault = True
            try:
                for pid in kernel_children():
                    info = proc_info(pid)
                    if info is not None and info['ppid'] == os.getpid():
                        try:
                            signals += int(signal_exact(info, signal.SIGKILL))
                        except BaseException:
                            pass
                reap(child)
            except BaseException:
                pass
        time.sleep(0.01)
    try:
        reap(child)
        return not descendants() and not kernel_children(), signals, fault
    except BaseException:
        return False, signals, True


def child_environment(model):
    return {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8',
            'PLAYWRIGHT_BROWSERS_PATH': '/opt/playwright', 'PYTHONDONTWRITEBYTECODE': '1',
            'COLLECTION_HEATMAP_MODEL': str(model), 'COLLECTOR_LAB_SUPERVISED': '1'}


def single(model, timeframe, case):
    if model not in (1, 2, 3) or timeframe not in ('12H', '24H', '48H') or case not in ('capture', 'gates', 'failure', 'timeout'):
        raise SupervisorFailure('invalid_fixture_case')
    started = time.monotonic()
    child = None
    selector = selectors.DefaultSelector()
    data = bytearray()
    observed = set()
    marker_seen = False
    timed_out = False
    established = False
    output_bytes = 0
    stderr_bytes = 0
    reason = None
    report = {'format': 'collector-supervised-case.v1', 'model': model, 'timeframe': timeframe,
              'case': case, 'status': 'failed', 'reason': 'not_started',
              'cleanup_verified': False, 'real_source_requests': 0, 'ai_client_calls': 0,
              'database_connections': 0}
    old_handlers = {}
    def cancel(_signal, _frame):
        raise SupervisorFailure('supervisor_interrupted')
    try:
        establish_subreaper()
        established = True
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            old_handlers[signum] = signal.signal(signum, cancel)
        command = [sys.executable, '-I', '-B', '-u', str(WORKER), '--worker',
                   '--model', str(model), '--timeframe', timeframe, '--case', case]
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, env=child_environment(model),
                                 cwd=str(HERE), close_fds=True, start_new_session=True)
        for pipe in (child.stdout, child.stderr):
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, pipe is child.stdout)
        deadline = started + WORKER_SECONDS
        while True:
            for info in descendants():
                if info['browser']:
                    observed.add((info['pid'], info['start']))
            now = time.monotonic()
            if now >= deadline:
                timed_out = True
                reason = 'worker_timeout'
                break
            for key, _ in selector.select(min(0.05, deadline - now)):
                try:
                    chunk = os.read(key.fileobj.fileno(), 4096)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                output_bytes += len(chunk)
                if output_bytes > MAX_OUTPUT:
                    raise SupervisorFailure('worker_output_limit')
                # Never forward raw child stdout or stderr. Only a fixed JSON record is accepted.
                if key.data:
                    data.extend(chunk)
                else:
                    stderr_bytes += len(chunk)
                if case == 'timeout' and not marker_seen and b'FIXTURE_BROWSER_READY\n' in data:
                    marker_seen = True
                    deadline = min(deadline, time.monotonic() + 2)
            if child.poll() is not None and not selector.get_map():
                break
        if not timed_out:
            lines = [line[len(b'FIXTURE_RESULT '):] for line in bytes(data).splitlines()
                     if line.startswith(b'FIXTURE_RESULT ')]
            if len(lines) != 1:
                raise SupervisorFailure('worker_result_missing')
            result = json.loads(lines[0])
            if (not isinstance(result, dict) or result.get('format') != 'collector-browser-fixture.v1'
                    or result.get('model') != model or result.get('timeframe') != timeframe or result.get('case') != case):
                raise SupervisorFailure('worker_result_invalid')
            report['worker'] = result
            report['status'] = 'passed' if child.returncode == 0 and result.get('status') == 'passed' else 'failed'
            report['reason'] = 'fixture_verified' if report['status'] == 'passed' else 'fixture_failed'
    except SupervisorFailure as error:
        reason = error.args[0] if error.args else 'supervisor_failed'
    except BaseException:
        reason = 'supervisor_failed'
    finally:
        cleanup_started = time.monotonic()
        # Child is the only spawned workload. Subreaper adoption includes detached Chromium groups.
        for signum in old_handlers:
            signal.signal(signum, signal.SIG_IGN)
        clean, killed, fault = (cleanup(child, min(started + 49, cleanup_started + CLEANUP_SECONDS))
                                if established else (False, 0, False))
        report.update(cleanup_verified=clean, cleanup_fault=fault, forced_signals=killed,
                      forced_cleanup_used=killed > 0, stderr_bytes_suppressed=stderr_bytes,
                      observed_browser_processes=len(observed), worker_timed_out=timed_out)
        if child is not None:
            if child.poll() is None:
                try:
                    child.wait(timeout=max(0.01, min(0.1, started + 49 - time.monotonic())))
                except BaseException:
                    report['cleanup_verified'] = False
            for pipe in (child.stdout, child.stderr):
                if pipe is not None:
                    pipe.close()
        selector.close()
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
    if case == 'timeout' and timed_out and marker_seen and observed and report['cleanup_verified'] and not report['cleanup_fault']:
        report.update(status='passed', reason='timeout_browser_cleanup_verified')
    elif reason is not None:
        report.update(status='failed', reason=reason)
    if not report['cleanup_verified'] or report['cleanup_fault']:
        report.update(status='failed', reason='cleanup_unverified')
    if report['status'] == 'passed' and not observed:
        report.update(status='failed', reason='browser_process_not_observed')
    report['elapsed_seconds'] = round(time.monotonic() - started, 3)
    return report


def _live_contract():
    """Load only the reviewed worker's import-safe, standard-library contract."""
    specification = importlib.util.spec_from_file_location('_collector_live_contract', WORKER)
    if specification is None or specification.loader is None:
        raise SupervisorFailure('worker_result_invalid')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module.sanitize_live_report, module.LIVE_PHASES


def _empty_live_supervisor_report():
    return {'format': 'collector-live-supervised.v1', 'status': 'failed', 'reason': 'not_started',
            'child_started': False, 'attempt_consumed': False, 'cleanup_verified': False,
            'cleanup_fault': False, 'lock_acquired': False, 'lock_released': False,
            'transient_removed': True, 'forced_signals': 0, 'observed_browser_processes': 0,
            'worker_timed_out': False, 'stderr_bytes_suppressed': 0, 'last_phase': None,
            'elapsed_seconds': 0.0, 'ai_client_calls': 0, 'database_connections': 0,
            'worker': None}


def sanitize_live_supervisor_report(value):
    """Public projection for the secure TTY entry point; reject every unknown field."""
    template = _empty_live_supervisor_report()
    try:
        sanitizer, phases = _live_contract()
        if not isinstance(value, dict) or set(value) != set(template):
            return None
        if value['format'] != template['format'] or value['status'] not in ('passed', 'stopped', 'failed'):
            return None
        if value['reason'] not in LIVE_REASONS:
            return None
        for key in ('child_started', 'attempt_consumed', 'cleanup_verified', 'cleanup_fault',
                    'lock_acquired', 'lock_released', 'transient_removed', 'worker_timed_out'):
            if type(value[key]) is not bool:
                return None
        for key in ('forced_signals', 'observed_browser_processes', 'stderr_bytes_suppressed'):
            if type(value[key]) is not int or not 0 <= value[key] <= MAX_OUTPUT:
                return None
        if any(type(value[key]) is not int or value[key] != 0 for key in ('ai_client_calls', 'database_connections')):
            return None
        if value['last_phase'] is not None and value['last_phase'] not in phases:
            return None
        elapsed = value['elapsed_seconds']
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or not 0 <= elapsed <= 3600:
            return None
        result = dict(value)
        if value['worker'] is not None:
            result['worker'] = sanitizer(value['worker'])
            if result['worker'] is None:
                return None
        if value['status'] == 'passed' and not (value['cleanup_verified'] and not value['cleanup_fault']
                and value['transient_removed'] and value['lock_released']
                and value['child_started'] and value['attempt_consumed']
                and value['observed_browser_processes'] > 0 and result['worker'] is not None
                and result['worker']['status'] == 'passed'):
            return None
        return result
    except BaseException:
        return None


def _acquire_live_lock():
    """Use the same inode as native_serial_lab.py; never unlink the lock."""
    descriptor = None
    try:
        root = EVIDENCE.parent
        if root.is_symlink() or EVIDENCE.is_symlink() or not root.is_dir():
            raise SupervisorFailure('serial_lock_unavailable')
        descriptor = os.open(root / 'native-serial.lock',
                             os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise SupervisorFailure('serial_lock_unavailable')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SupervisorFailure('serial_lock_busy') from None
        return descriptor
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise


def _remove_live_transient():
    # The one-use live-attempt directory is deliberately retained. Only the
    # worker's owned temporary output is removed after its processes are gone.
    marker = EVIDENCE / 'live-attempt'
    transient = marker / 'transient'
    if marker.is_symlink() or EVIDENCE.is_symlink():
        return False
    try:
        if transient.is_symlink():
            transient.unlink()
        elif transient.exists():
            shutil.rmtree(transient)
        return not transient.exists() and not transient.is_symlink()
    except BaseException:
        return False


def _strict_live_json(data):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate_field')
            result[key] = value
        return result
    def constant(_value):
        raise ValueError('invalid_number')
    return json.loads(data, object_pairs_hook=object_pairs, parse_constant=constant)


def live_single(raw_header, started_monotonic):
    """One authorized model1/12H child; the clock starts BEFORE secure TTY input.

    This function never prompts, retries, writes the header, inherits hosting
    credentials, or forwards raw child output. No browser is started on import.
    """
    report = _empty_live_supervisor_report()
    child = None
    selector = None
    lock_descriptor = None
    established = False
    started_valid = False
    environment = None
    stdout = bytearray()
    phase_pending = bytearray()
    observed = set()
    old_handlers = {}
    output_bytes = 0
    marker_owned = False
    reason = None
    parsed = None
    def cancel(_signal, _frame):
        raise SupervisorFailure('supervisor_interrupted')
    try:
        now = time.monotonic()
        if (type(started_monotonic) not in (float, int) or not math.isfinite(started_monotonic)
                or started_monotonic < 0 or started_monotonic > now):
            raise SupervisorFailure('invalid_live_start')
        started_valid = True
        if now >= started_monotonic + 45:
            raise SupervisorFailure('live_budget_expired')
        if (not isinstance(raw_header, str) or not raw_header.strip() or len(raw_header) > 65536
                or any(ord(char) < 32 or ord(char) > 126 for char in raw_header)):
            raise SupervisorFailure('invalid_live_input')
        if resource.getrlimit(resource.RLIMIT_CORE) != (0, 0):
            raise SupervisorFailure('core_dumps_not_disabled')
        sanitizer, phases = _live_contract()
        establish_subreaper()
        established = True
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            old_handlers[signum] = signal.signal(signum, cancel)
        lock_descriptor = _acquire_live_lock()
        report['lock_acquired'] = True
        if time.monotonic() >= started_monotonic + 45:
            raise SupervisorFailure('live_budget_expired')
        try:
            EVIDENCE.mkdir(mode=0o700, exist_ok=True)
            (EVIDENCE / 'live-attempt').mkdir(mode=0o700, exist_ok=False)
        except FileExistsError:
            raise SupervisorFailure('live_attempt_already_consumed') from None
        except BaseException:
            raise SupervisorFailure('live_marker_unavailable') from None
        marker_owned = True
        report.update(attempt_consumed=True, transient_removed=False)
        if time.monotonic() >= started_monotonic + 45:
            raise SupervisorFailure('live_budget_expired')
        environment = child_environment(1)
        environment.update(COINGLASS_COOKIE_HEADER=raw_header,
                           COLLECTOR_LIVE_DEADLINE_MONOTONIC=str(started_monotonic + 45))
        command = [sys.executable, '-I', '-B', '-u', str(WORKER), '--live-worker']
        selector = selectors.DefaultSelector()
        # Pending cancellation must be delivered only AFTER child ownership is
        # assigned. No credential is ever placed in the command line.
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(old_handlers))
        try:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, env=environment, cwd=str(HERE),
                                     close_fds=True, start_new_session=True)
            report['child_started'] = True
            environment.clear()
            environment = None
            raw_header = None
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        for pipe in (child.stdout, child.stderr):
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, pipe is child.stdout)
        while True:
            for info in descendants():
                if info['browser']:
                    observed.add((info['pid'], info['start']))
            now = time.monotonic()
            if now >= started_monotonic + 45:
                report['worker_timed_out'] = True
                raise SupervisorFailure('worker_timeout')
            for key, _ in selector.select(min(0.05, started_monotonic + 45 - now)):
                try:
                    chunk = os.read(key.fileobj.fileno(), 4096)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                output_bytes += len(chunk)
                if output_bytes > MAX_OUTPUT:
                    raise SupervisorFailure('worker_output_limit')
                if not key.data:
                    report['stderr_bytes_suppressed'] += len(chunk)
                    continue
                stdout.extend(chunk)
                phase_pending.extend(chunk)
                while b'\n' in phase_pending:
                    line, _, rest = phase_pending.partition(b'\n')
                    phase_pending[:] = rest
                    if line.startswith(b'LIVE_PHASE '):
                        try:
                            phase = line[len(b'LIVE_PHASE '):].decode('ascii')
                        except UnicodeError:
                            continue
                        if phase in phases:
                            report['last_phase'] = phase
            if child.poll() is not None and not selector.get_map():
                break
        lines = [line[len(b'LIVE_RESULT '):] for line in stdout.splitlines()
                 if line.startswith(b'LIVE_RESULT ')]
        if len(lines) != 1:
            raise SupervisorFailure('worker_result_missing')
        try:
            parsed = _strict_live_json(lines[0])
            safe = sanitizer(parsed)
        except BaseException:
            safe = None
        parsed = None
        if safe is None:
            raise SupervisorFailure('worker_result_invalid')
        report['worker'] = safe
        report['last_phase'] = safe['phase']
        if child.returncode == 0 and safe['status'] == 'passed':
            report.update(status='passed', reason='live_capture_verified')
        elif safe['status'] == 'stopped':
            report.update(status='stopped', reason='live_source_stopped')
        else:
            report.update(status='failed', reason='live_child_failed')
    except SupervisorFailure as error:
        candidate = error.args[0] if error.args else None
        reason = candidate if isinstance(candidate, str) and candidate in LIVE_REASONS else 'supervisor_failed'
    except BaseException:
        reason = 'supervisor_failed'
    finally:
        # Atomically stop raising handlers before entering cleanup. This closes
        # the gap between changing the first and the last handled signal.
        cleanup_mask = None
        try:
            cleanup_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(old_handlers))
            for signum in old_handlers:
                signal.signal(signum, signal.SIG_IGN)
        except BaseException:
            report['cleanup_fault'] = True
        finally:
            if cleanup_mask is not None:
                try:
                    signal.pthread_sigmask(signal.SIG_SETMASK, cleanup_mask)
                except BaseException:
                    report['cleanup_fault'] = True
        raw_header = None
        parsed = None
        if environment is not None:
            environment.clear()
        stdout.clear()
        phase_pending.clear()
        # Cleanup has the remaining original 60-second budget, never a reset
        # timeout starting after the TTY or browser work has already elapsed.
        try:
            if established:
                clean, killed, fault = cleanup(child, started_monotonic + 59.5)
            else:
                clean, killed, fault = child is None, 0, False
        except BaseException:
            clean, killed, fault = False, 0, True
        report.update(cleanup_verified=clean, cleanup_fault=fault or report['cleanup_fault'], forced_signals=killed,
                      observed_browser_processes=len(observed))
        if child is not None:
            try:
                child_stopped = child.poll() is not None
            except BaseException:
                child_stopped = False
            if not child_stopped:
                report['cleanup_verified'] = False
            for pipe in (child.stdout, child.stderr):
                if pipe is not None:
                    try:
                        pipe.close()
                    except BaseException:
                        report['cleanup_fault'] = True
        if selector is not None:
            try:
                selector.close()
            except BaseException:
                report['cleanup_fault'] = True
        if marker_owned and report['cleanup_verified'] and not report['cleanup_fault']:
            try:
                report['transient_removed'] = _remove_live_transient()
            except BaseException:
                report['transient_removed'] = False
        if lock_descriptor is not None:
            if report['cleanup_verified'] and not report['cleanup_fault'] and report['transient_removed']:
                try:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                    os.close(lock_descriptor)
                    report['lock_released'] = True
                except BaseException:
                    report['cleanup_fault'] = True
                    _LIVE_RETAINED_LOCKS.append(lock_descriptor)
            else:
                _LIVE_RETAINED_LOCKS.append(lock_descriptor)
        for signum, handler in old_handlers.items():
            try:
                signal.signal(signum, handler)
            except BaseException:
                report['cleanup_fault'] = True
    if reason is not None:
        report.update(status='failed', reason=reason)
    if not report['cleanup_verified'] or report['cleanup_fault']:
        report.update(status='failed', reason='cleanup_unverified')
    elif not report['transient_removed']:
        report.update(status='failed', reason='transient_cleanup_unverified')
    if report['status'] == 'passed' and not observed:
        report.update(status='failed', reason='browser_process_not_observed')
    if started_valid:
        elapsed = max(0, time.monotonic() - started_monotonic)
        report['elapsed_seconds'] = round(min(elapsed, 3600), 3)
        if elapsed > 60:
            report.update(status='failed', reason='cleanup_deadline_exceeded')
    safe_report = sanitize_live_supervisor_report(report)
    if safe_report is None:
        # Do not serialize even an internally unexpected value on this path.
        safe_report = _empty_live_supervisor_report()
        safe_report.update(reason='worker_result_invalid', cleanup_verified=report['cleanup_verified'],
                           cleanup_fault=report['cleanup_fault'], child_started=report['child_started'],
                           attempt_consumed=report['attempt_consumed'], lock_acquired=report['lock_acquired'],
                           lock_released=report['lock_released'], transient_removed=report['transient_removed'])
    if marker_owned:
        descriptor = None
        try:
            serialized = json.dumps(safe_report, allow_nan=False, separators=(',', ':')).encode('utf8') + b'\n'
            if len(serialized) > MAX_REPORT_BYTES:
                raise ValueError('receipt_limit')
            target = EVIDENCE / 'live-attempt' / 'supervisor.json'
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            with os.fdopen(descriptor, 'wb') as stream:
                descriptor = None
                stream.write(serialized)
        except BaseException:
            safe_report.update(status='failed', reason='receipt_write_failed')
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except BaseException:
                    pass
    return safe_report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--single', action='store_true')
    parser.add_argument('--model', type=int, choices=(1, 2, 3), default=1)
    parser.add_argument('--timeframe', choices=('12H', '24H', '48H'), default='12H')
    parser.add_argument('--case', choices=('capture', 'gates', 'failure', 'timeout'), default='capture')
    args = parser.parse_args()
    if args.single:
        report = single(args.model, args.timeframe, args.case)
    else:
        specifications = [(model, timeframe, 'capture') for model in (1, 2, 3)
                          for timeframe in ('12H', '24H', '48H')]
        specifications += [(model, '12H', 'gates') for model in (1, 2, 3)]
        specifications += [(1, '12H', 'failure'), (1, '12H', 'timeout')]
        cases = []
        for model, timeframe, case in specifications:
            result = single(model, timeframe, case)
            cases.append(result)
            # An unconfirmed process tree must stop the suite before another worker starts.
            if not result['cleanup_verified']:
                break
        captures = [value for value in cases if value['case'] == 'capture']
        passed = len(cases) == len(specifications) and all(value['status'] == 'passed' for value in cases)
        report = {'format': 'collector-browser-matrix.v1', 'status': 'passed' if passed else 'failed',
                  'expected_cases': len(specifications), 'completed_cases': len(cases),
                  'capture_cases': len(captures),
                  'capture_passed_count': sum(value['status'] == 'passed' for value in captures),
                  'native_hover_validated': sum(value.get('worker', {}).get('native_hover_validated') is True for value in captures),
                  'png_validated': sum(value.get('worker', {}).get('png_validated') is True for value in captures),
                  'gate_check_count': sum(value.get('worker', {}).get('gate_check_count', 0)
                                          for value in cases if value['case'] == 'gates' and value['status'] == 'passed'),
                  'expected_gate_check_count': 72,
                  'injections_passed': sum(value['status'] == 'passed' for value in cases
                                           if value['case'] in ('failure', 'timeout')),
                  'cleanup_verified_count': sum(value['cleanup_verified'] is True for value in cases),
                  'all_cleanup_verified': all(value['cleanup_verified'] for value in cases),
                  'real_source_requests': 0, 'ai_client_calls': 0, 'database_connections': 0,
                  'scope': 'synthetic_capture_native_hover_and_dom_guards_only', 'cases': cases}
        EVIDENCE.mkdir(parents=True, exist_ok=True, mode=0o700)
        (EVIDENCE / 'matrix.json').write_text(json.dumps(report, allow_nan=False, indent=2), encoding='utf8')
    within_limit = emit_report(report)
    return 0 if within_limit and report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
