#!/usr/bin/env python3
"""Serial Render-only synthetic browser matrix with independent process cleanup.

Linux subreaper/pidfd/census helpers are adapted from the reviewed November 7
observation supervisor. No credentials, remote service, database or AI is used.
Each worker gets <=45 seconds; cleanup reserves <=4 seconds (total <=50).
"""
import argparse
import ctypes
import errno
import json
import os
from pathlib import Path
import selectors
import signal
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
