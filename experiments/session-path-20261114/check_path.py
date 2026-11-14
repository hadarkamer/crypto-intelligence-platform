#!/usr/bin/env python3
"""Two real collector commands and Chromium runs, with dummy HTTP boundaries.

The production app, queue claim, parent Session, generated task command, native
capture, browser events, pipe, encryption and PostgreSQL store remain real.
Only source document/account responses and the subsequent AI boundary are test
fixtures. This proves neither CoinGlass access nor its renewal policy.
"""
from __future__ import annotations

import argparse
import ast
import atexit
import base64
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RUNTIME = ROOT / 'standalone_collector/runtime'
PAGE_URL = 'https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'
ACCOUNT_URL = 'https://capi.coinglass.com/coin-community/api/userapi/info'
COOKIE_NAME = 'fixture_session'
KEY = bytes(range(32))
COLLECTOR_TOKEN = 'fixture-only-collector-authorization-00000001'


class CheckFailed(Exception):
    pass


def check(value, code):
    if not value:
        raise CheckFailed(code)


def write_report(path, report):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, sort_keys=True, allow_nan=False))
    temporary.chmod(0o600)
    temporary.replace(path)


def child_evidence(path):
    if not path.is_file() or path.is_symlink() or path.stat().st_size >= 16384:
        return {'evidence_missing':True}
    value = json.loads(path.read_text())
    check(isinstance(value, dict), 'child_evidence_shape_invalid')
    # Only known typed technical fields can enter the retained report.
    flags = {'account_cookie_matches','account_obe_absent','updated_cookie_before_close','context_closed',
        'browser_closed','ai_boundary_reached','native_12h_identity','native_png','native_hover',
        'parent_secrets_absent','legacy_header_absent','browser_processes_gone','profiles_removed',
        'state_environment_removed_before_driver','internal_sandbox_disabled_observed',
        'internal_sandbox_enabled','dummy_only_exception','fixture_bootstrap_failed'}
    counts = {'generation','child_pid','browser_launches','contexts','document_requests',
        'account_requests','other_requests_aborted','source_requests','ai_calls'}
    result = {key:item for key,item in value.items() if
        key in flags and type(item) is bool or key in counts and type(item) is int and 0 <= item < 2**31}
    if value.get('chromium_version') == '140.0.7339.16': result['chromium_version'] = value['chromium_version']
    if isinstance(value.get('browser_pids'), list) and all(type(pid) is int and 0 < pid < 2**31 for pid in value['browser_pids']):
        result['browser_pids'] = value['browser_pids'][:64]
    reasons = {'fixture_not_authorized','root_browser_forbidden','python_version_mismatch',
        'playwright_version_mismatch','parent_secret_in_child','legacy_header_in_child',
        'fixture_generation_invalid','fixture_report_scope_invalid','existing_html_fixture_missing','fixture_hook_setup_failed'}
    if value.get('reason_code') in reasons: result['reason_code'] = value['reason_code']
    if value.get('exception_kind') in {'CheckFailed','ImportError','ModuleNotFoundError','OSError','ValueError','KeyError','other'}:
        result['exception_kind'] = value['exception_kind']
    return result


def process_snapshot():
    rows = {}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            status = dict(line.split(':', 1) for line in (entry / 'status').read_text().splitlines() if ':' in line)
            rows[int(entry.name)] = (int(status['PPid']), (entry / 'cmdline').read_bytes().split(b'\0'))
        except (OSError, KeyError, ValueError):
            pass
    return rows


def descendants(pid, rows):
    owned = {pid}
    while True:
        expanded = owned | {child for child, (parent, _) in rows.items() if parent in owned}
        if expanded == owned:
            return owned - {pid}
        owned = expanded


def cookie_generation(number):
    return 'DUMMY_COIN_GLASS_GENERATION_' + str(number)


def state(number):
    return {'cookies': [{'name': COOKIE_NAME, 'value': cookie_generation(number),
        'domain': '.coinglass.com', 'path': '/', 'expires': -1,
        'httpOnly': True, 'secure': True, 'sameSite': 'Lax'}], 'origins': []}


def html_fixture():
    path = ROOT / 'experiments/model3-preparation-fix-20261031/check_capture.py'
    nodes = [node for node in ast.parse(path.read_text()).body if isinstance(node, ast.Assign)
             and any(isinstance(target, ast.Name) and target.id == 'HTML' for target in node.targets)]
    check(len(nodes) == 1, 'existing_html_fixture_missing')
    html = ast.literal_eval(nodes[0].value)
    html = html.replace('MODEL', '1').replace('STAMP', datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'))
    # The normal browser creates the request; no Python account request exists.
    # Existing controls become visible only after the dummy account response.
    return html + '''<script>
document.querySelector('#panel').style.visibility='hidden';
fetch('https://capi.coinglass.com/coin-community/api/userapi/info', {credentials:'include'})
 .then(response=>response.json()).then(value=>{
   if(value.success===true){document.querySelector('#panel').style.visibility='visible';window.fixtureAccountDone=true;}
 });
</script>'''


def install_child():
    """Called only by test sitecustomize in the real generated task process."""
    check(os.environ.get('CG_SESSION_PATH_TEST') == 'dummy-only', 'fixture_not_authorized')
    check(os.geteuid() != 0, 'root_browser_forbidden')
    check(sys.version_info[:2] == (3, 11), 'python_version_mismatch')
    check(importlib.metadata.version('playwright') == '1.55.0', 'playwright_version_mismatch')
    check('COINGLASS_SESSION_KEY' not in os.environ and 'DATABASE_URL' not in os.environ,
          'parent_secret_in_child')
    check(os.environ.get('COINGLASS_COOKIE_HEADER') == '', 'legacy_header_in_child')
    generation = int(os.environ['CG_SESSION_TEST_GENERATION'])
    check(generation in (1, 2), 'fixture_generation_invalid')
    report_path = Path(os.environ['CG_SESSION_TEST_REPORT'])
    check(report_path.parent.resolve() == Path(os.environ['TMPDIR']).resolve(), 'fixture_report_scope_invalid')
    sys.path.insert(0, str(RUNTIME))
    from market_vision import openai_heatmap_scanner as scanner
    from model1_execution import StageFailure
    from playwright.sync_api import Browser, BrowserType, BrowserContext
    report = {'generation': generation, 'child_pid': os.getpid(),
        'browser_launches': 0, 'contexts': 0, 'document_requests': 0,
        'account_requests': 0, 'other_requests_aborted': 0,
        'account_cookie_matches': False, 'account_obe_absent': False,
        'updated_cookie_before_close': False, 'context_closed': False,
        'browser_closed': False, 'ai_boundary_reached': False,
        'source_requests': 0, 'ai_calls': 0, 'native_12h_identity': False,
        'native_png': False, 'native_hover': False, 'parent_secrets_absent': True,
        'legacy_header_absent': True, 'internal_sandbox_enabled': False,
        'dummy_only_exception': True}
    browsers, contexts = [], []
    browser_pids, profile_paths = set(), set()
    original_launch = BrowserType.launch
    original_context = Browser.new_context
    original_context_close = BrowserContext.close
    original_browser_close = Browser.close
    html = html_fixture()

    def browser_processes():
        rows = process_snapshot()
        for pid in descendants(os.getpid(), rows):
            argv = rows[pid][1]
            if not argv or Path(os.fsdecode(argv[0])).name not in ('chrome', 'chromium', 'headless_shell', 'chrome-headless-shell'):
                continue
            browser_pids.add(pid)
            check(not any(arg in argv for arg in (b'--disable-web-security', b'--disable-site-isolation-trials',
                                                  b'--allow-file-access-from-files')), 'web_security_disabled')
            report['internal_sandbox_disabled_observed'] = b'--no-sandbox' in argv or report.get('internal_sandbox_disabled_observed', False)
            for arg in argv:
                if arg.startswith(b'--user-data-dir='):
                    path = Path(os.fsdecode(arg.partition(b'=')[2])).resolve()
                    check(path.is_relative_to(Path(os.environ['TMPDIR']).resolve()), 'profile_outside_test_directory')
                    profile_paths.add(path)

    def launch(browser_type, *args, **kwargs):
        check(browser_type.name == 'chromium' and kwargs.get('headless') is True, 'unexpected_browser_configuration')
        check(report['browser_launches'] == 0, 'second_browser_forbidden')
        # Same sandbox setting as the collector; no security/namespace change.
        kwargs['args'] = [*kwargs.get('args', []), '--disable-background-networking',
            '--disable-component-update', '--no-first-run', '--host-resolver-rules=MAP * ~NOTFOUND']
        browser = original_launch(browser_type, *args, **kwargs)
        browsers.append(browser); report['browser_launches'] += 1
        report['chromium_version'] = browser.version
        check(browser.version == '140.0.7339.16', 'chromium_version_mismatch')
        browser_processes()
        return browser

    def context(browser, *args, **kwargs):
        check(report['contexts'] == 0, 'second_context_forbidden')
        check('extra_http_headers' not in kwargs, 'global_header_unexpected')
        value = original_context(browser, *args, **kwargs)
        contexts.append(value); report['contexts'] += 1
        def route_request(route):
            request = route.request
            if (request.url == PAGE_URL and request.method == 'GET'
                    and request.resource_type == 'document' and request.is_navigation_request()):
                report['document_requests'] += 1
                check(report['document_requests'] == 1, 'unexpected_document_retry')
                route.fulfill(status=200, content_type='text/html', body=html)
            elif request.url == ACCOUNT_URL and request.method == 'GET':
                report['account_requests'] += 1
                headers = request.all_headers()
                parts = dict(item.strip().split('=', 1) for item in headers.get('cookie', '').split(';') if '=' in item)
                matches = parts.get(COOKIE_NAME) == cookie_generation(generation)
                no_obe = 'obe' not in headers
                headers = parts = None
                report['account_cookie_matches'] = matches
                report['account_obe_absent'] = no_obe
                check(matches and no_obe and report['account_requests'] == 1, 'dummy_account_request_invalid')
                route.fulfill(status=200, body='{"success":true}', headers={
                    'Content-Type': 'application/json', 'Cache-Control': 'no-store',
                    'Access-Control-Allow-Origin': 'https://www.coinglass.com',
                    'Access-Control-Allow-Credentials': 'true',
                    'Set-Cookie': COOKIE_NAME + '=' + cookie_generation(generation + 1) +
                        '; Domain=.coinglass.com; Path=/; Secure; HttpOnly; SameSite=Lax'})
            else:
                report['other_requests_aborted'] += 1
                route.abort()
        value.route('**/*', route_request)
        return value

    def context_close(value, *args, **kwargs):
        try:
            if value in contexts:
                cookies = value.cookies(urls=[ACCOUNT_URL])
                report['updated_cookie_before_close'] = any(cookie.get('name') == COOKIE_NAME
                    and cookie.get('value') == cookie_generation(generation + 1) for cookie in cookies)
                cookies = None
        finally:
            result = original_context_close(value, *args, **kwargs)
            if value in contexts: report['context_closed'] = True
        return result

    def browser_close(browser, *args, **kwargs):
        try: browser_processes()
        finally:
            result = original_browser_close(browser, *args, **kwargs)
            if browser in browsers: report['browser_closed'] = not browser.is_connected()
        return result

    def stop_at_ai(images, **kwargs):
        report['ai_boundary_reached'] = True
        if isinstance(images, list) and len(images) == 1:
            image = images[0]
            report['native_12h_identity'] = image.get('timeframe') == '12h'
            snapshot = image.get('hover_snapshot', {})
            report['native_hover'] = (snapshot.get('complete') is True and snapshot.get('heatmap_model') == 1
                and snapshot.get('timeframe') == '12H' and len(snapshot.get('rows', [])) == 40
                and snapshot.get('tooltip_reads', 0) > 0)
            path = Path(image.get('image', ''))
            if path.is_file():
                with path.open('rb') as stream: report['native_png'] = stream.read(8) == b'\x89PNG\r\n\x1a\n'
        check(all(not browser.is_connected() for browser in browsers), 'browser_open_at_ai_boundary')
        raise StageFailure('analysis_incomplete')

    def complete():
        try:
            end = time.monotonic() + 2
            while time.monotonic() < end and any(Path('/proc', str(pid)).exists() for pid in browser_pids):
                time.sleep(0.05)
            report['browser_processes_gone'] = bool(browser_pids) and all(not Path('/proc', str(pid)).exists() for pid in browser_pids)
            report['profiles_removed'] = bool(profile_paths) and all(not path.exists() for path in profile_paths)
            report['browser_pids'] = sorted(browser_pids)
            report['state_environment_removed_before_driver'] = all(name not in os.environ for name in
                ('COINGLASS_SESSION_KEY','COINGLASS_STORAGE_STATE_JSON','COINGLASS_COOKIE_HEADER','COINGLASS_SESSION_RETURN_FD'))
            write_report(report_path, report)
        except BaseException:
            os._exit(98)
    atexit.register(complete)
    BrowserType.launch = launch
    Browser.new_context = context
    BrowserContext.close = context_close
    Browser.close = browser_close
    scanner.analyze_heatmap_images = stop_at_ai


def http_json(port, path, *, data=None):
    request = urllib.request.Request('http://127.0.0.1:' + str(port) + path,
        data=None if data is None else json.dumps(data).encode(), headers={
            'Content-Type':'application/json', 'Authorization':'Bearer ' + COLLECTOR_TOKEN})
    with urllib.request.urlopen(request, timeout=2) as response:
        return json.loads(response.read(65536))


def stop_worker(process):
    if process is None: return True
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try: process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            return False
    return process.returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    report = {'status':'failed', 'phase':'preflight', 'runs':[], 'source_requests':0, 'ai_calls':0,
        'production_writes':0, 'full_command':'python -B standalone_collector/app.py'}
    worker = db = None
    cleanup = True
    work = Path(os.environ['TMPDIR']).resolve()
    try:
        check(os.geteuid() != 0, 'root_test_forbidden')
        check(sys.version_info[:2] == (3, 11), 'python_version_mismatch')
        check(importlib.metadata.version('playwright') == '1.55.0', 'playwright_version_mismatch')
        report.update(python_version=sys.version.split()[0], playwright_version=importlib.metadata.version('playwright'))
        from check_postgres import writer_dsn
        import psycopg
        from psycopg.rows import dict_row
        sys.path.insert(0, str(RUNTIME))
        from session_state import PgSessionStore, WORK_LOCK
        dsn = writer_dsn()
        db = psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
            options='-c statement_timeout=5000 -c lock_timeout=2000')
        check(db.execute('SELECT count(*) AS n FROM collector_private.coinglass_session_state').fetchone()['n'] == 0,
              'session_row_not_empty')
        db.execute('SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
        try: PgSessionStore(db, KEY).bootstrap(state(1))
        finally: db.execute('SELECT pg_advisory_unlock(%s)', (WORK_LOCK,))
        revisions = []
        for generation in (1, 2):
            report['phase'] = 'cycle_' + str(generation)
            child_report = work / ('browser-cycle-' + str(generation) + '.json')
            check(not child_report.exists(), 'cycle_already_attempted')
            with socket.socket() as available:
                available.bind(('127.0.0.1', 0)); port = available.getsockname()[1]
            env = {name:os.environ[name] for name in ('PATH','HOME','TMPDIR','PLAYWRIGHT_BROWSERS_PATH','LANG','LC_CTYPE') if name in os.environ}
            env.update(PYTHONDONTWRITEBYTECODE='1', PYTHONUTF8='1',
                PYTHONPATH=str(HERE) + os.pathsep + str(RUNTIME),
                DATABASE_URL=dsn, PORT=str(port), OPENAI_API_KEY='dummy-only-not-an-api-key',
                COINGLASS_COLLECTOR_TOKEN=COLLECTOR_TOKEN, COLLECTION_BRIDGE_ENABLED='true',
                COINGLASS_SESSION_STORE_ENABLED='true', COINGLASS_SESSION_KEY=base64.b64encode(KEY).decode(),
                COINGLASS_COOKIE_HEADER=COOKIE_NAME+'=DUMMY_STALE_HEADER',
                CG_SESSION_PATH_TEST='dummy-only', CG_SESSION_TEST_GENERATION=str(generation),
                CG_SESSION_TEST_REPORT=str(child_report), COINGLASS_ACCOUNT_DIAGNOSTIC_ON_START='false')
            worker = subprocess.Popen([sys.executable, '-B', 'standalone_collector/app.py'],
                cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            env.clear()
            cycle = {'generation':generation, 'worker_pid':worker.pid, 'worker_stopped':False}
            report['runs'].append(cycle)
            deadline = time.monotonic() + 20
            while True:
                check(worker.poll() is None, 'worker_start_exited')
                try:
                    health = http_json(port, '/health')
                    if health.get('mode') == 'collection': break
                except (OSError, ValueError): pass
                check(time.monotonic() < deadline, 'worker_start_timeout')
                time.sleep(0.1)
            result = http_json(port, '/api/collection/model1/jobs', data={'request_id':str(uuid4()),'timeframe':'12H'})
            job_id = str(result['job_id']); check(result['timeframe'] == '12H', 'job_timeframe_wrong')
            deadline = time.monotonic() + 60
            ownership = False
            while True:
                check(worker.poll() is None, 'worker_exited_during_job')
                row = db.execute('SELECT revision,status,run_id FROM collector_private.coinglass_session_state').fetchone()
                if row['status'] == 'in_use':
                    acquired = db.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (WORK_LOCK,)).fetchone()['acquired']
                    if acquired: db.execute('SELECT pg_advisory_unlock(%s)', (WORK_LOCK,))
                    ownership |= not acquired and str(row['run_id']) == job_id
                result = http_json(port, '/api/collection/model1/jobs/' + job_id)
                if result.get('status') in ('failed','ready'): break
                check(time.monotonic() < deadline, 'job_timeout')
                time.sleep(0.1)
            cycle['job_status'] = result.get('status')
            cycle['job_code'] = result.get('error', {}).get('code') if result.get('error') else None
            child = child_evidence(child_report)
            cycle['browser'] = child
            check(cycle['job_status'] == 'failed' and cycle['job_code'] == 'analysis_incomplete', 'unexpected_job_outcome')
            check(ownership, 'worker_lock_ownership_not_observed')
            cycle['existing_lock_ownership_observed'] = ownership
            row = db.execute('SELECT revision,status,run_id FROM collector_private.coinglass_session_state').fetchone()
            check(row['status'] == 'ready' and row['run_id'] is None, 'session_not_saved')
            stopped = stop_worker(worker); cycle['worker_stopped'] = stopped
            cleanup &= stopped; worker = None
            check(stopped, 'worker_cleanup_failed')
            deadline = time.monotonic() + 5
            while not db.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (WORK_LOCK,)).fetchone()['acquired']:
                check(time.monotonic() < deadline, 'worker_lock_not_released')
                time.sleep(0.05)
            current = None
            try:
                current = PgSessionStore(db, KEY).read()
                check(any(cookie['name'] == COOKIE_NAME and cookie['value'] == cookie_generation(generation + 1)
                          for cookie in current['cookies']), 'updated_state_not_saved')
            finally:
                if isinstance(current, dict): current.clear()
                db.execute('SELECT pg_advisory_unlock(%s)', (WORK_LOCK,))
            cycle['saved_revision'] = row['revision']; revisions.append(row['revision'])
            check(child.get('evidence_missing') is not True, 'child_evidence_missing')
            required = ('account_cookie_matches','account_obe_absent','updated_cookie_before_close','context_closed',
                'browser_closed','ai_boundary_reached','native_12h_identity','native_png','native_hover',
                'parent_secrets_absent','legacy_header_absent','browser_processes_gone','profiles_removed',
                'state_environment_removed_before_driver','internal_sandbox_disabled_observed')
            check(all(child.get(key) is True for key in required), 'child_evidence_failed')
            check(all(child.get(key) == 1 for key in ('browser_launches','contexts','document_requests','account_requests')),
                  'unexpected_browser_or_request_count')
            cycle['browser'] = child
        check(revisions == [3,5], 'revision_sequence_wrong')
        check(report['runs'][0]['worker_pid'] != report['runs'][1]['worker_pid'], 'worker_not_restarted')
        check(report['runs'][0]['browser']['child_pid'] != report['runs'][1]['browser']['child_pid'], 'child_not_restarted')
        check(set(report['runs'][0]['browser']['browser_pids']).isdisjoint(report['runs'][1]['browser']['browser_pids']),
              'browser_process_reused')
        report.update(status='passed', phase='complete', two_distinct_processes_and_browsers=True,
                      latest_state_reused=True, managed_source_not_legacy_header=True)
    except CheckFailed as error:
        report['reason'] = str(error)
    except BaseException as error:
        report['reason'] = 'test_execution_failed'
        report['exception_kind'] = type(error).__name__ if type(error).__name__ in (
            'OperationalError','ProgrammingError','IntegrityError','OSError','TimeoutError','KeyError','ValueError') else 'other'
    finally:
        cleanup &= stop_worker(worker)
        if db is not None:
            try: db.close()
            except Exception: cleanup = False
        report['cleanup'] = {'worker_stopped':cleanup, 'database_connection_closed':db is None or db.closed}
        report['elapsed_seconds'] = round(time.monotonic() - started, 3)
        if not cleanup: report['status'] = 'failed'
        write_report(args.output, report)
        print('SESSION_PATH_RESULT ' + json.dumps(report, sort_keys=True, allow_nan=False), flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
