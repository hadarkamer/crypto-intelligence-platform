#!/usr/bin/env python3
"""Focused, loopback-only real Chromium check; no source, account or product data.

Run only on the authorized Actions runner. The adapter never reads a response
body: this fixture page alone consumes the ordinary, public dummy response.
The existing Playwright routes remain installed (including their native
preflight handling). A dropped ExtraInfo event is explicitly a simulation.
"""
from __future__ import annotations

import argparse
import errno
import importlib.metadata
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'standalone_collector'))
from model1_execution import prepare_context
from account_cdp import AccountHeaderExperiment

TOKEN = 'fixture-only+decoded'
EXISTING = 'fixture-original'
ENV = {'COINGLASS_COOKIE_HEADER': 'obe=fixture-only%2Bdecoded'}
ACCOUNT_PATH = '/coin-community/api/userapi/info'
BODY = '{"fixture":"unchanged","success":false}'


class CheckFailed(Exception):
    pass


def check(value, code):
    if not value:
        raise CheckFailed(code)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = True
    allow_reuse_address = False

    def handle_error(self, request, client_address):
        # Dummy peer may close after a CORS rejection. Never log requests.
        self.state['handler_errors'] += 1


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'

    def log_message(self, *args):
        pass

    def answer(self, status, body, headers):
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def handle_request(self):
        state = self.server.state
        path = urlsplit(self.path).path
        label = ('account' if path == ACCOUNT_PATH else 'redirect_step'
                 if path == '/redirect-step' else 'sink'
                 if path == '/sink' else 'page' if path == '/page' else 'other')
        # Immediately reduce incoming headers; retain no headers or values.
        state['received'].append({'server': self.server.role,
            'label': label, 'method': self.command,
            'present': self.headers.get('obe') is not None,
            'matches': self.headers.get('obe') == TOKEN,
            'existing_matches': self.headers.get('obe') == EXISTING})
        if label == 'page':
            self.answer(200, b'<!doctype html><meta charset="utf-8"><link rel="icon" href="data:,"><title>Local fixture</title><body>Local fixture</body>',
                        {'Content-Type': 'text/html; charset=utf-8'})
            return
        origin = self.headers.get('Origin')
        headers = {'Content-Type': 'application/json'}
        if state['case'] != 'cors_missing':
            # The mock's own policy. The adapter does not alter these headers.
            headers['Access-Control-Allow-Origin'] = ('*' if state['case'] == 'cors_wildcard'
                else origin if origin in (state['page_origin'], 'null') else state['page_origin'])
            headers['Access-Control-Allow-Credentials'] = 'true'
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            headers['Access-Control-Allow-Headers'] = 'x-fixture, obe'
        if self.command == 'OPTIONS':
            self.answer(204, b'', headers)
            return
        if self.command == 'POST':
            # All fixture requests have no body. Do not ingest arbitrary data.
            self.answer(200, BODY.encode(), headers)
            return
        if label == 'account' and state['case'].startswith('redirect'):
            state['account_gets'] += 1
            if state['account_gets'] == 1:
                destination = (state['sink_origin'] + '/sink' if state['case'] == 'redirect_cross_origin'
                               else state['account_origin'] + '/redirect-step')
                headers['Location'] = destination
                self.answer(302, b'', headers)
                return
        if label == 'redirect_step':
            # Return to exactly the original matched endpoint: never re-inject.
            headers['Location'] = state['account_origin'] + ACCOUNT_PATH
            self.answer(302, b'', headers)
            return
        self.answer(200, BODY.encode(), headers)

    do_GET = handle_request
    do_POST = handle_request
    do_OPTIONS = handle_request


def process_snapshot():
    result = {}
    for directory in Path('/proc').iterdir():
        if not directory.name.isdecimal():
            continue
        try:
            status = dict(line.split(':', 1) for line in (directory / 'status').read_text().splitlines() if ':' in line)
            args = (directory / 'cmdline').read_bytes().split(b'\0')
            result[int(directory.name)] = (int(status['PPid']), args, status)
        except (OSError, ValueError, KeyError):
            continue
    return result


def descendants(snapshot):
    owned = {os.getpid()}
    while True:
        next_owned = owned | {pid for pid, item in snapshot.items() if item[0] in owned}
        if next_owned == owned:
            return owned - {os.getpid()}
        owned = next_owned


def browser_process_evidence():
    snapshot = process_snapshot()
    profiles = set()
    security_disabled = False
    sandbox_disabled = False
    browser_pids = set()
    for pid in descendants(snapshot):
        _, args, _ = snapshot[pid]
        if not args:
            continue
        name = Path(os.fsdecode(args[0])).name
        if name not in ('chrome', 'headless_shell', 'chrome-headless-shell', 'chromium'):
            continue
        browser_pids.add(pid)
        sandbox_disabled |= b'--no-sandbox' in args
        security_disabled |= any(arg in (b'--disable-web-security',
            b'--disable-site-isolation-trials', b'--allow-file-access-from-files') for arg in args)
        for arg in args:
            if arg.startswith(b'--user-data-dir='):
                profiles.add(Path(os.fsdecode(arg.partition(b'=')[2])))
    return {'browser_processes_seen': bool(browser_pids),
            'web_security_not_disabled': not security_disabled,
            'internal_sandbox_disabled_observed': sandbox_disabled}, profiles, browser_pids


def prelaunch_safety(report):
    # env -i in the existing runner wrapper is the boundary; fail if it changes.
    allowed = {'PATH', 'HOME', 'TMPDIR', 'PLAYWRIGHT_BROWSERS_PATH', 'PYTHONUTF8',
               'PYTHONDONTWRITEBYTECODE', 'COLLECTION_BRIDGE_ENABLED', 'LC_CTYPE'}
    unexpected = sorted(set(os.environ) - allowed)
    report['prelaunch'] = {'unexpected_environment_names': unexpected,
        'internal_sandbox_enabled': False, 'dummy_only_exception': True,
        'external_isolation_is_not_internal_sandbox': True}
    safety = report['prelaunch']
    check(not unexpected, 'unexpected_environment')
    status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines()
                  if ':' in line)
    safety['unprivileged_uid'] = os.geteuid() != 0
    safety['effective_capabilities_empty'] = int(status['CapEff'].strip(), 16) == 0
    check(safety['unprivileged_uid'] and safety['effective_capabilities_empty'], 'privileged_test_process')
    check(os.environ.get('COLLECTION_BRIDGE_ENABLED') == 'false', 'worker_not_disabled')
    links = json.loads(subprocess.run(['ip', '-json', 'link', 'show'], check=True,
        capture_output=True, text=True, timeout=3).stdout)
    safety['loopback_only'] = len(links) == 1 and links[0].get('ifname') == 'lo'
    check(safety['loopback_only'], 'network_interface_not_loopback_only')
    routes = []
    for family in ('-4', '-6'):
        routes.extend(json.loads(subprocess.run(['ip', family, '-json', 'route', 'show', 'table', 'all'],
            check=True, capture_output=True, text=True, timeout=3).stdout))
    safety['no_external_routes'] = all(row.get('dev') == 'lo' and row.get('dst') != 'default'
                                      for row in routes)
    check(safety['no_external_routes'], 'external_route_present')
    # TEST-NET is never contacted: with no external interface/route, require the
    # kernel's ENETUNREACH specifically, rather than accepting any socket error.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.25)
        try:
            probe.connect(('192.0.2.1', 443))
            blocked = False
        except OSError as error:
            blocked = error.errno == errno.ENETUNREACH
    safety['external_probe_network_unreachable'] = blocked
    check(blocked, 'network_block_not_proven')
    temporary_root = Path(os.environ['TMPDIR'])
    safety['fresh_owned_temporary_directory'] = (temporary_root.is_dir()
        and temporary_root.stat().st_uid == os.geteuid()
        and not any(temporary_root.iterdir()))
    check(safety['fresh_owned_temporary_directory'], 'temporary_directory_not_fresh')
    safety['dummy_credentials_only'] = True


def launch_failure_evidence(error):
    # Reduce only the launch exception. Never persist argv, paths, URLs, headers,
    # raw stderr, or arbitrary exception text; retain exact allowlisted phrases.
    message = str(error)
    lowered = message.lower()
    phrases = ('Operation not permitted', 'Permission denied', 'No usable sandbox',
        'Failed to move to new namespace', 'No such file or directory',
        'error while loading shared libraries', 'Exec format error',
        'Connection refused', 'Trace/breakpoint trap', 'Segmentation fault',
        'Target page, context or browser has been closed')
    matched = [phrase for phrase in phrases if phrase.lower() in lowered]
    exit_codes = [int(value) for value in re.findall(r'exitCode=(-?\d+)', message)]
    signals = re.findall(r'signal=(SIG[A-Z0-9]+)', message)
    pids = [int(value) for value in re.findall(r'(?:launched> pid=|\[pid=)(\d+)', message)]
    libraries = re.findall(r'error while loading shared libraries: ([A-Za-z0-9_.+-]{1,100}\.so(?:\.\d+)*)', message)
    source_locations = re.findall(
        r'\b(?:ERROR|FATAL|WARNING):(?:[A-Za-z0-9_.+-]+/)*([A-Za-z0-9_.+-]{1,100}\.(?:cc|cpp|c|h):\d{1,6})\]', message)
    reason = ('browser_executable_missing' if "executable doesn't exist" in lowered
        else 'shared_library_load_failed' if libraries
        else 'explicit_namespace_setup_failure' if 'failed to move to new namespace' in lowered
        else 'explicit_no_usable_sandbox' if 'no usable sandbox' in lowered
        else 'browser_startup_cause_unknown')
    return {'reason': reason, 'technical_phrases': matched,
        'exit_codes': exit_codes[:4], 'signals': sorted(set(signals))[:4],
        'process_ids': sorted(set(pids))[:8], 'missing_libraries': libraries[:4],
        'source_locations': sorted(set(source_locations))[:8],
        'raw_exception_retained': False}


def install_routes(context, page_url, ports, state):
    pages = []
    navigation = {'count': 0, 'blocked': 0, 'external': 0, 'handled': 0}

    def one_navigation(route):
        request = route.request
        navigation['handled'] += 1
        parsed = urlsplit(request.url)
        # Additional fixture-only egress allowlist; no source URL is reachable.
        if (parsed.scheme != 'http' or parsed.hostname != '127.0.0.1'
                or parsed.port not in ports or parsed.username or parsed.password):
            navigation['external'] += 1
            route.abort()
            return
        # Same navigation guard as the existing observation wrapper, with only
        # the known fixture page substituted for the approved source page.
        if request.is_navigation_request() and request.frame == request.frame.page.main_frame:
            expected_page = len(pages) == 1 and request.frame.page == pages[0]
            if (not expected_page or navigation['count'] or
                    request.url != page_url or request.method != 'GET'):
                navigation['blocked'] += 1
                route.abort()
                return
            navigation['count'] += 1
        route.fallback()

    context.route('**/*', one_navigation)
    prepare_context(context)  # The unmodified product advertising route.
    page = context.new_page()
    pages.append(page)
    page.set_default_timeout(7000)
    page.goto(page_url, wait_until='load', timeout=7000)
    return page, navigation


def browser_fetch(page, url, method='GET', headers=None):
    # Only the page reads the known dummy body, not the adapter/CDP observer.
    return page.evaluate('''async ({url, method, headers, expected}) => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);
      try {
        const r = await fetch(url, {method, headers, credentials: 'include', cache: 'no-store', signal: controller.signal});
        const body = await r.text();
        return {completed: true, status_ok: r.status === 200 || r.status === 204,
                body_unchanged: method === 'OPTIONS' ? body === '' : body === expected};
      } catch (error) { return {completed: false, status_ok: false, body_unchanged: false,
          network_rejected: error instanceof TypeError, timed_out: controller.signal.aborted}; }
      finally { clearTimeout(timer); }
    }''', {'url': url, 'method': method, 'headers': headers or {}, 'expected': BODY})


def received(state, label=None, method=None):
    return [dict(row) for row in state['received']
            if row['label'] != 'page' and (label is None or row['label'] == label)
            and (method is None or row['method'] == method)]


def run(output):
    report = {'format': 'account-cdp-isolated.v1', 'status': 'failed',
              'failure': None, 'cases': {}, 'source_requests': 0,
              'ai_calls': 0, 'product_writes': 0, 'cleanup': {}}
    servers, threads, contexts, adapters = [], [], [], []
    browser = playwright = None
    profiles, observed_pids = set(), set()
    started = time.monotonic()
    stage = 'preflight'
    state = {'case': 'startup', 'received': [], 'handler_errors': 0, 'account_gets': 0}
    try:
        check(not any(os.environ.get(key) for key in ('COINGLASS_COOKIE_HEADER',
              'COINGLASS_STORAGE_STATE_JSON', 'DATABASE_URL', 'OPENAI_API_KEY')),
              'unexpected_credentials')
        prelaunch_safety(report)
        report['target_playwright_from_build'] = '1.55.0'
        check(importlib.metadata.version('playwright') == '1.55.0', 'playwright_version')
        report['playwright_version'] = importlib.metadata.version('playwright')
        for role in ('page', 'account', 'sink'):
            server = Server(('127.0.0.1', 0), Handler)
            server.state, server.role = state, role
            servers.append(server)
            state[role + '_origin'] = 'http://127.0.0.1:' + str(server.server_port)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            threads.append(thread)
            thread.start()
        ports = {server.server_port for server in servers}
        page_url = state['page_origin'] + '/page'
        account_url = state['account_origin'] + ACCOUNT_PATH
        from playwright.sync_api import sync_playwright
        import playwright as playwright_package
        manifest = Path(playwright_package.__file__).parent / 'driver/package/browsers.json'
        declared = json.loads(manifest.read_text())
        expected = next(item['browserVersion'] for item in declared['browsers'] if item['name'] == 'chromium')
        report['declared_chromium_version'] = expected
        report['prelaunch']['fixture_origins_loopback_only'] = all(
            urlsplit(state[role + '_origin']).hostname == '127.0.0.1'
            and urlsplit(state[role + '_origin']).scheme == 'http'
            for role in ('page', 'account', 'sink'))
        check(report['prelaunch']['fixture_origins_loopback_only'], 'fixture_origin_not_local')
        stage = 'browser_startup'
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True, chromium_sandbox=False, timeout=15000)
        report['chromium_version'] = browser.version
        check(browser.version == expected, 'chromium_version')
        context = browser.new_context(service_workers='block')
        contexts.append(context)
        page, navigation = install_routes(context, page_url, ports, state)
        check(page.title() == 'Local fixture', 'startup_loopback')
        proc, paths, pids = browser_process_evidence()
        profiles |= paths
        observed_pids |= pids
        report['startup'] = {**proc, 'local_page_loaded': True}
        check(all(proc.values()), 'browser_launch_policy_not_proven')
        context.close()
        contexts.remove(context)

        for case in ('allowed', 'other_requests', 'redirect_same_and_return',
                     'redirect_cross_origin', 'cors_missing', 'cors_wildcard', 'missing_event'):
            stage = case
            state.update(case=case, received=[], account_gets=0)
            context = browser.new_context(service_workers='block')
            contexts.append(context)
            page, navigation = install_routes(context, page_url, ports, state)
            adapter = AccountHeaderExperiment(page, ENV, target_url=account_url,
                                               page_origin=state['page_origin'])
            adapters.append(adapter)
            adapter.start()
            result = {}
            if case == 'missing_event':
                # Explicit monitoring-fault simulation, real browser/server GET.
                adapter._session.remove_listener('Network.requestWillBeSentExtraInfo', adapter._on_extra_info)
                result['extra_info_drop_simulated'] = True
            if case == 'other_requests':
                result['other_path'] = browser_fetch(page, state['account_origin'] + '/other')
                result['post'] = browser_fetch(page, account_url, 'POST')
                result['existing_header'] = browser_fetch(page, account_url, headers={'obe': EXISTING})
                result['explicit_options'] = browser_fetch(page, account_url, 'OPTIONS')
                rows = received(state)
                check(all(value['completed'] and value['body_unchanged'] for value in result.values()), 'other_response')
                check(all(not row['present'] for row in rows if row['method'] == 'OPTIONS'), 'options_header')
                check(all(not row['present'] for row in rows if row['label'] == 'other' or row['method'] == 'POST'), 'other_header')
                gets = received(state, 'account', 'GET')
                check(len(gets) == 1 and gets[0]['existing_matches'], 'existing_header_changed')
                check(any(row['method'] == 'OPTIONS' for row in rows), 'explicit_options_not_received')
                check(adapter.evidence()['injections'] == 0, 'other_injected')
            else:
                result['page'] = browser_fetch(page, account_url, headers={'x-fixture': 'local'})
                page.wait_for_timeout(100)  # Dispatch already completed CDP metadata, not a retry.
                facts = adapter.evidence()
                rows = received(state)
                gets = received(state, 'account', 'GET')
                check(gets and gets[0]['present'] and gets[0]['matches'], 'server_header_not_matched')
                check(facts['injections'] == 1, 'injection_count')
                check(all(not row['present'] for row in rows if row['method'] == 'OPTIONS'), 'options_header')
                result['automatic_preflight_server_count'] = sum(row['method'] == 'OPTIONS' for row in rows)
                if case in ('cors_missing', 'cors_wildcard'):
                    check(not result['page']['completed'] and result['page']['network_rejected']
                          and not result['page']['timed_out'], 'cors_protection_not_proven')
                    result['browser_cors_rejection_preserved'] = True
                else:
                    check(result['page']['completed'] and result['page']['status_ok']
                          and result['page']['body_unchanged'], 'response_not_delivered')
                if case.startswith('redirect'):
                    downstream = [row for row in rows if row['method'] == 'GET'][1:]
                    check(downstream and all(not row['present'] for row in downstream), 'redirect_header_leak')
                    if case == 'redirect_same_and_return':
                        check(len(gets) == 2 and not gets[1]['present'], 'redirect_reinjected')
                    else:
                        check(any(row['server'] == 'sink' for row in downstream), 'cross_origin_sink_missing')
                    result['redirect_no_header'] = True
                elif case == 'missing_event':
                    check(facts['send_state'] == 'not_verified', 'missing_event_false_success')
                elif case == 'allowed':
                    check(facts['send_state'] == 'verified' and facts['header_present'] is True
                          and facts['header_matches'] is True, 'chromium_server_disagree')
                    result['chromium_and_server_agree'] = True
                    check(len(gets) == 1, 'duplicate_account_request')
            adapter.close()
            facts = adapter.evidence()
            cleanup = facts['cleanup']
            check(cleanup['paused_remaining'] == 0 and all(cleanup[k] for k in
                  ('listeners_removed', 'fetch_disabled', 'detached', 'reference_cleared')), 'adapter_cleanup')
            result['adapter'] = facts
            result['server'] = received(state)
            result['route_guard'] = dict(navigation)
            if case == 'allowed':
                before = len(received(state, 'account', 'GET'))
                check(browser_fetch(page, account_url)['body_unchanged'], 'after_detach_response')
                after = received(state, 'account', 'GET')
                check(len(after) == before + 1 and not after[-1]['present'], 'after_detach_header')
                result['request_after_detach_unmodified'] = True
            # Guard itself must still reject a second top-level navigation.
            try:
                page.goto(page_url, timeout=3000)
            except Exception:
                pass
            check(navigation['blocked'] == 1, 'navigation_guard_lost')
            result['navigation_guard_preserved'] = True
            check(navigation['external'] == 0, 'external_request_attempt')
            check(state['handler_errors'] == 0, 'mock_handler_failed')
            report['cases'][case] = {'status': 'passed', **result}
            context.close()
            contexts.remove(context)
            adapters.remove(adapter)
        report['status'] = 'passed'
    except BaseException as error:
        report['status'] = 'failed'
        # Never retain a raw browser exception (may include URLs or headers).
        report['error_class'] = type(error).__name__ if type(error).__name__ in {
            'CheckFailed', 'Error', 'TimeoutError', 'TargetClosedError', 'ValueError',
            'FileNotFoundError', 'PermissionError', 'RuntimeError', 'ImportError',
            'ModuleNotFoundError'} else 'Other'
        report['failure'] = str(error) if isinstance(error, CheckFailed) else 'stage_failed'
        if stage == 'browser_startup' and not isinstance(error, CheckFailed):
            report['launch_failure'] = launch_failure_evidence(error)
            report['failure'] = report['launch_failure']['reason']
        report['failed_stage'] = stage
        report['failure_evidence'] = {'server': received(state),
                                      'mock_handler_failed': state['handler_errors'] != 0}
        if adapters:
            report['failure_evidence']['adapter'] = adapters[-1].evidence()
    finally:
        signal.alarm(0)  # The outer 150s supervisor reserves 30s for cleanup.
        _, final_profiles, final_pids = browser_process_evidence()
        profiles |= final_profiles
        observed_pids |= final_pids
        clean = True
        for adapter in adapters:
            try:
                adapter.close()
                cleanup = adapter.evidence()['cleanup']
                clean &= cleanup['paused_remaining'] == 0 and all(cleanup[key] for key in
                    ('listeners_removed', 'fetch_disabled', 'detached', 'reference_cleared'))
            except BaseException:
                clean = False
        for context in contexts:
            try:
                context.close()
            except BaseException:
                clean = False
        if browser is not None:
            try:
                browser.close()
                clean &= not browser.is_connected()
            except BaseException:
                clean = False
        if playwright is not None:
            try:
                playwright.stop()
            except BaseException:
                clean = False
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(2)
        snapshot = process_snapshot()
        remaining = [pid for pid in observed_pids if pid in snapshot and snapshot[pid][2].get('State', '').strip()[:1] != 'Z']
        # Independently catch later contexts and processes orphaned on close.
        profile_markers = {b'--user-data-dir=' + os.fsencode(path) for path in profiles}
        for pid, (_, args, status) in snapshot.items():
            if status.get('State', '').strip()[:1] == 'Z':
                continue
            if (pid in descendants(snapshot) and args and Path(os.fsdecode(args[0])).name in
                    ('chrome', 'headless_shell', 'chrome-headless-shell', 'chromium')) or any(arg in profile_markers for arg in args):
                remaining.append(pid)
        try:
            # The CI runner supplies an owned TMPDIR. Also inspect startup-failure
            # leftovers when no browser command line was available to observe.
            temporary_root = Path(tempfile.gettempdir())
            leftover_profiles = [p for p in temporary_root.iterdir()
                                 if p.name.startswith('playwright_')]
            profile_check_complete = True
        except OSError:
            leftover_profiles, profile_check_complete = [], False
        report['profile_paths_observed'] = bool(profiles)
        if report.get('failure_evidence') is not None and adapters:
            report['failure_evidence']['adapter_after_cleanup'] = adapters[-1].evidence()
        report['cleanup'] = {'adapter_context_browser_close_succeeded': bool(clean),
            'server_threads_stopped': not any(t.is_alive() for t in threads),
            'server_sockets_closed': all(s.socket.fileno() == -1 for s in servers),
            'observed_browser_processes_gone': not remaining,
            'profile_check_complete': profile_check_complete,
            'profiles_removed': profile_check_complete and not leftover_profiles
                                and all(not path.exists() for path in profiles)}
        if not all(report['cleanup'].values()):
            report['status'], report['failure'] = 'failed', 'cleanup_failed'
        report['elapsed_seconds'] = round(time.monotonic() - started, 3)
        encoded = json.dumps(report, indent=2, sort_keys=True)
        if len(encoded.encode('utf-8')) > 131072:
            report = {'format': report['format'], 'status': 'failed',
                      'failure': 'report_limit', 'cleanup': report['cleanup']}
            encoded = json.dumps(report, indent=2, sort_keys=True)
        check(TOKEN not in encoded and EXISTING not in encoded and 'fixture-only%2Bdecoded' not in encoded,
              'report_secret_leak')
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded + '\n')
        print(json.dumps({'status': report['status'], 'failure': report['failure'],
                          'failed_stage': report.get('failed_stage'),
                          'completed_cases': len(report.get('cases', {})), 'cleanup': report['cleanup']}))
    return 0 if report['status'] == 'passed' else 1


def cancelled(signum, frame):
    raise CheckFailed('cancelled')


if __name__ == '__main__':
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGALRM):
        signal.signal(signum, cancelled)
    signal.alarm(120)
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=Path('cdp-evidence/result.json'))
    arguments = parser.parse_args()
    sys.exit(run(arguments.output))
