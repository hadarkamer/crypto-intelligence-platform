#!/usr/bin/env python3
"""Render-lab worker: synthetic HTML -> unchanged capture and native hover.

This is not a source/account, AI, persistence, or production-performance test.
The public HTML comes from the pinned collector's existing offline fixture.
Run through collector_matrix.py, which independently owns process cleanup.
"""
import argparse
import ast
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import time
from unittest.mock import patch

ROOT = Path('/lab/collector')
RUNTIME = ROOT / 'standalone_collector/runtime'
FIXTURE = ROOT / 'experiments/model3-preparation-fix-20261031/check_capture.py'
CHROMIUM = Path('/opt/playwright/chromium-1187/chrome-linux/chrome')
EVIDENCE = Path('/lab/evidence/collector')
COMMIT = '33bad18f8be322009108584a6b71f3d65fa9b351'
HTML_SHA = '566b3bb9bec4345df2a95f404c9ed8ba4a57fbbe92402e4a683c542f306e3a01'
HASHES = {
    'capture_readiness.py': 'a21fcb00acfc4ba20bb1d54d6342e2f89521ad2d941792822cbbd20de7cc437a',
    'model1_execution.py': '5a82f410b089307d3ad465d3abb016f4616a0932c3f7c75a00a2584a95117693',
    'heatmap_models.py': 'de2ed269648628bd124b83e159d4abb302cb024479721a7babfb0ad92ec7bf7e',
    'market_vision/coinglass_heatmap_capture.py': '3a8778e0c6de4b4c1e24dc8dea3292e2e6c5ce2e2163bad251c3edc12a2f129a',
    'heatmap_hover.py': '693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820',
    'model3_rendering.py': 'b3193d4cf58304637485a8aeb2852dff5dbb45be5ea2628396b31ebd5863ce8f',
    'model3_render_gate.js': '80b533ef6ad274f7fc2532964de308d7bb8a066fe386508551866f04f3dbda8a',
}
CASES = ('capture', 'gates', 'failure', 'timeout')

# The separately approved live path is fixed to one asset/model/range. It does
# not change any installed collector module; these guards exist only in this
# isolated lab process. Raw screenshots are temporary and never exported.
LIVE_PHASES = frozenset({'startup', 'capture', 'select_12h', 'model_selection',
    'symbol_selection', 'wait_for_chart', 'screenshot', 'navigation', 'browser_launch',
    'browser_context', 'source_session_setup', 'page_creation', 'overlay_dismissal',
    'timeframe_selection', 'legend_preparation', 'chart_geometry', 'render_settle',
    'context_close', 'browser_close', 'render_readiness', 'timeframe_trigger_lookup',
    'timeframe_open', 'timeframe_option_lookup', 'timeframe_select',
    'timeframe_confirmation', 'hover_collection'})
LIVE_REASONS = frozenset({'capture_verified', 'work_deadline', 'second_navigation_forbidden',
    'reload_forbidden', 'retry_forbidden', 'forced_click_forbidden', 'access_modal_blocked',
    'native_capture_rejected', 'live_setup_failed', 'live_capture_failed',
    'identity_failed', 'output_invalid', 'ephemeral_cleanup_failed',
    'browser_close_unverified', 'unexpected_environment', 'runtime_pin_mismatch'})
LIVE_NATIVE_CODES = frozenset({'source_capture_failed', 'source_timeout',
    'source_not_readable', 'source_login_required', 'screenshot_identity_mismatch',
    'image_evidence_invalid', 'price_uncertain', 'zones_invalid', 'worker_crashed'})
LIVE_NATIVE_REASONS = frozenset(('hover_deadline hover_geometry_invalid hover_geometry_missing '
    'hover_plot_clipped hover_plot_not_found hover_point_multiple_rows hover_pool_binding '
    'hover_pool_empty_gap hover_pool_overlap_or_gap hover_pool_rows_invalid hover_pool_uncertain '
    'hover_price_grid_gap hover_range_single_row hover_resolution_insufficient hover_row_count '
    'hover_row_order hover_rows_merged hover_snapshot_changed hover_source_stale hover_text_invalid '
    'hover_time_invalid hover_timezone_not_utc hover_update_timeout hover_candidates_unresolved '
    'hover_candidate_missing hover_candidate_disagreement hover_cookie_overlay hover_price_axis_changed '
    'hover_price_axis_mismatch hover_image_evidence_missing hover_image_changed hover_candidate_evidence_mismatch '
    'hover_pool_not_visible hover_candle_text_invalid hover_candle_time_invalid hover_candle_number_invalid '
    'hover_candle_evidence_mismatch hover_candle_time_mismatch hover_candle_not_found hover_candle_outside_axis '
    'hover_candle_changed hover_number_invalid').split())
LIVE_BOOLS = ('runtime_pins_verified', 'source_identity_verified', 'native_hover_validated',
              'png_validated', 'png_deleted', 'browsers_disconnected')
LIVE_COUNTS = {'screenshots': 1, 'browser_launches': 1, 'contexts': 1, 'navigation_calls': 1,
               'requests_seen': 100000, 'responses_seen': 100000,
               'blocked_navigation_requests': 100000, 'row_count': 1600,
               'tooltip_reads': 100000, 'png_width': 20000, 'png_height': 20000}
LIVE_ACCESS_MODAL = r'''() => [...document.querySelectorAll('[role="dialog"],.MuiModal-root')].some(e=>{
 const r=e.getBoundingClientRect();if(!r.width||!r.height)return false;
 for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);
 if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity||1)===0)return false;}
 return /\blog\s*in\b|\bsign\s*in\b|unlock\s+full\s+data/i.test(e.innerText||'');
})'''


class LiveStop(BaseException):
    """Not swallowed by the original best-effort controls or retry handler."""
    def __init__(self, reason):
        self.reason = reason if reason in LIVE_REASONS else 'live_capture_failed'


def empty_live_report():
    return {'format': 'collector-live-child.v1', 'model': 1, 'timeframe': '12H',
            'coin': 'BTC', 'commit': COMMIT, 'status': 'failed', 'reason': 'live_setup_failed',
            'phase': 'startup', 'native_code': None, 'native_reason': None,
            **{key: False for key in LIVE_BOOLS}, **{key: 0 for key in LIVE_COUNTS},
            'png_sha256': None, 'source_time': None, 'candle_close': None,
            'total_value': None, 'network': {'http_errors': [], 'network_errors': [], 'page_error_count': 0}}


def _sanitize_live_report(value):
    """Exact receipt schema; raw output, source text, headers and URLs cannot pass."""
    template = empty_live_report()
    if not isinstance(value, dict) or set(value) != set(template): return None
    for key in ('format', 'model', 'timeframe', 'coin', 'commit'):
        if type(value[key]) is not type(template[key]) or value[key] != template[key]: return None
    if value['status'] not in ('passed', 'stopped', 'failed') or value['reason'] not in LIVE_REASONS: return None
    if value['phase'] not in LIVE_PHASES: return None
    if value['native_code'] is not None and value['native_code'] not in LIVE_NATIVE_CODES: return None
    if value['native_reason'] is not None and value['native_reason'] not in LIVE_NATIVE_REASONS: return None
    if any(type(value[k]) is not bool for k in LIVE_BOOLS): return None
    if any(type(value[k]) is not int or not 0 <= value[k] <= maximum for k, maximum in LIVE_COUNTS.items()): return None
    if value['png_sha256'] is not None and not (isinstance(value['png_sha256'], str) and re.fullmatch('[0-9a-f]{64}', value['png_sha256'])): return None
    if value['source_time'] is not None and not (isinstance(value['source_time'], str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value['source_time'])): return None
    for key in ('candle_close', 'total_value'):
        number = value[key]
        if number is not None and (type(number) not in (float, int) or not math.isfinite(number) or not 0 <= number < 1e18): return None
    network = value['network']
    if not isinstance(network, dict) or set(network) != {'http_errors', 'network_errors', 'page_error_count'}: return None
    if type(network['page_error_count']) is not int or not 0 <= network['page_error_count'] <= 1000: return None
    for field in ('http_errors', 'network_errors'):
        rows = network[field]
        if not isinstance(rows, list) or len(rows) > 6: return None
        detail = 'status' if field == 'http_errors' else 'kind'
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'host_group', 'count', detail}: return None
            if row['host_group'] not in ('coinglass', 'other') or type(row['count']) is not int or not 1 <= row['count'] <= 1000: return None
            if detail == 'status' and (type(row[detail]) is not int or not 400 <= row[detail] <= 599): return None
            if detail == 'kind' and row[detail] not in ('timeout', 'connection', 'cancelled', 'blocked', 'other'): return None
    if value['status'] == 'passed':
        if value['reason'] != 'capture_verified' or not all(value[k] for k in LIVE_BOOLS): return None
        if any(value[k] != 1 for k in ('screenshots', 'browser_launches', 'contexts', 'navigation_calls')): return None
        if not value['row_count'] or not value['png_sha256'] or not value['source_time'] or not value['candle_close']: return None
    return dict(value)


def sanitize_live_report(value):
    try: return _sanitize_live_report(value)
    except (TypeError, ValueError, OverflowError): return None


class LiveGuard:
    """Trial envelope only. It never accepts data or changes reader thresholds."""
    def __init__(self, report, deadline, access_check):
        self.report, self.deadline, self.access_check = report, deadline, access_check
        self.navigation_requests = 0

    def check(self):
        if time.monotonic() >= self.deadline: raise LiveStop('work_deadline')
        if self.report['blocked_navigation_requests']: raise LiveStop('second_navigation_forbidden')

    def invoke(self, action, original, *args, **kwargs):
        self.check()
        if action == 'reload': raise LiveStop('reload_forbidden')
        if action == 'retry': raise LiveStop('retry_forbidden')
        if kwargs.get('force'): raise LiveStop('forced_click_forbidden')
        if action == 'goto':
            if self.report['navigation_calls']: raise LiveStop('second_navigation_forbidden')
            self.report['navigation_calls'] += 1
        else:
            self.access_check()
            # Escape is unnecessary in this fresh one-page trial. Do not send a
            # key that could close an access modal appearing between observation
            # and dispatch. Normal timeframe/candle/legend actions stay enabled.
            if action == 'key' and args and args[-1] == 'Escape': return None
        result = original(*args, **kwargs)
        self.check()
        if action != 'goto': self.access_check()
        return result


class FixtureFailure(Exception):
    pass


def check(condition, code):
    if not condition:
        raise FixtureFailure(code)


def apply_browser_close_check(report):
    """Keep object-state observations separate from the supervisor's process proof."""
    if report['browsers_disconnected']:
        report['browser_close_verification'] = 'playwright_object_disconnected'
        return
    # This exact injection occurs before capture's explicit try/finally. Its
    # sync_playwright manager has already unwound, so the cached Browser flag
    # is not an authoritative process-liveness observation. Preserve that flag;
    # only the unchanged parent census can accept cleanup for this case.
    expected_setup_failure = (
        report.get('case') == 'failure' and report.get('status') == 'passed'
        and report.get('reason') == 'injected_setup_failure_observed'
        and report.get('browser_launches') == 1 and report.get('contexts') == 1
        and report.get('screenshots') == 0 and report.get('fulfilled_documents') == 0)
    if expected_setup_failure:
        report['browser_close_verification'] = 'requires_supervisor_process_census'
        return
    report.update(status='failed', reason='browser_close_unverified',
                  browser_close_verification='unverified')


def deny_python_network(event, _args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind'):
        raise FixtureFailure('python_network_forbidden')


def fixture_html(model):
    for relative, expected in HASHES.items():
        check(hashlib.sha256((RUNTIME / relative).read_bytes()).hexdigest() == expected,
              'runtime_pin_mismatch')
    tree = ast.parse(FIXTURE.read_text(encoding='utf8'))
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'HTML'
                           for target in node.targets)]
    check(len(assignments) == 1, 'html_fixture_missing')
    original = ast.literal_eval(assignments[0].value)
    check(isinstance(original, str) and hashlib.sha256(original.encode()).hexdigest() == HTML_SHA,
          'html_fixture_pin_mismatch')
    stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    return original.replace('MODEL', str(model)).replace('STAMP', stamp)


def gate_html(html, timeframe, scenario):
    hours = int(timeframe[:-1])
    initial = 24 if hours == 12 else 12
    selected = initial if scenario == 'wrong-label' else hours
    content = html.replace('</script>', f'\nwindow.select({selected});\n</script>')
    notices = {
        'no-data': '<div role="status" style="position:absolute;left:60px;top:180px">No data</div>',
        'request-limit': '<div role="alert" style="position:absolute;left:60px;top:180px">Too many requests</div>',
        'loading-indicator': '<span role="progressbar" style="position:absolute;left:70px;top:180px;width:40px;height:40px"></span>',
    }
    if scenario in notices:
        content = content.replace('<canvas width="800" height="420"></canvas>',
                                  '<canvas width="800" height="420"></canvas>' + notices[scenario])
    if scenario == 'login-required':
        content += ('<div class="MuiModal-root" style="position:absolute;inset:0">'
                    '<div role="dialog" style="position:absolute;left:80px;top:180px;width:400px;height:120px">'
                    '<div>Log in to unlock full data</div><button>Login</button>'
                    '<button>Continue with Google</button></div></div>')
    return content


def exercise_gates(browser, args, capture, readiness, execution, rendering, html, active_html):
    context = browser.new_context(viewport={'width': 1600, 'height': 1300 if args.model == 3 else 1000})
    rendering.configure_context(context, args.model)
    page = context.new_page()
    cases = []
    url = capture.COINGLASS_HEATMAP_URL
    for hours in (12, 24, 48):
        timeframe = f'{hours}h'
        active_html[0] = html
        page.goto(url, wait_until='domcontentloaded', timeout=5000)
        check(readiness.valid_source(page.url, args.model), 'fixture_identity_failed')
        check(not readiness.valid_source(url.replace('coin=BTC', 'coin=ETH'), args.model),
              'wrong_coin_accepted')
        check(not readiness.valid_source(url + '&coin=BTC', args.model), 'duplicate_coin_accepted')
        execution.control(capture._select_timeframe, page, timeframe)
        state = readiness.current_state(page, timeframe, args.model)
        check(state['ready'] is True and state['label'] == f'{hours} hour', 'normal_control_failed')
        check(readiness.wait_for_render(page, timeframe, args.model, 1500)['ready'] is True,
              'normal_paint_failed')
        cases.append({'timeframe': timeframe.upper(), 'scenario': 'ready', 'passed': True})
        for reason in ('no-data', 'request-limit', 'loading-indicator', 'login-required', 'wrong-label'):
            active_html[0] = gate_html(html, timeframe, reason)
            page.goto(url, wait_until='domcontentloaded', timeout=5000)
            state = readiness.current_state(page, timeframe, args.model)
            check(state['ready'] is False and state['reason'] == reason, 'negative_gate_failed')
            if reason == 'login-required':
                calls = []
                def _select_timeframe(_page, value):
                    calls.append(value)
                try:
                    execution.control(_select_timeframe, page, timeframe)
                except execution.StageFailure as error:
                    check(error.code == 'source_login_required' and not calls, 'pre_control_gate_failed')
                else:
                    raise FixtureFailure('pre_control_gate_missing')
            cases.append({'timeframe': timeframe.upper(), 'scenario': reason, 'passed': True})
        # Same unchanged guard must reject a gate appearing after the control.
        active_html[0] = gate_html(html, timeframe, 'ready')
        page.goto(url, wait_until='domcontentloaded', timeout=5000)
        for raises in (False, True):
            page.locator('[data-fixture-gate]').evaluate_all('(nodes) => nodes.forEach(n => n.remove())')
            def _select_timeframe(target, _value):
                target.evaluate("""() => { const d=document.createElement('div');
                    d.dataset.fixtureGate='1';d.setAttribute('role','dialog');
                    d.style.cssText='position:absolute;left:80px;top:180px;width:400px;height:120px';
                    d.textContent='Log in to unlock full data';document.body.appendChild(d); }""")
                if raises:
                    raise TimeoutError('synthetic_control_timeout')
            try:
                execution.control(_select_timeframe, page, timeframe)
            except execution.StageFailure as error:
                check(error.code == 'source_login_required', 'post_control_gate_failed')
            else:
                raise FixtureFailure('post_control_gate_missing')
            cases.append({'timeframe': timeframe.upper(),
                          'scenario': 'gate_after_control_error' if raises else 'gate_after_control_success',
                          'passed': True})
    return {'gate_checks': cases, 'gate_check_count': len(cases)}


def worker(args):
    check(os.environ.get('COLLECTOR_LAB_SUPERVISED') == '1', 'supervisor_required')
    check(set(os.environ) <= {'PATH', 'LANG', 'LC_CTYPE', 'PLAYWRIGHT_BROWSERS_PATH',
                              'PYTHONDONTWRITEBYTECODE', 'COLLECTION_HEATMAP_MODEL',
                              'COLLECTOR_LAB_SUPERVISED'}, 'unexpected_environment')
    check(os.environ.get('COLLECTION_HEATMAP_MODEL') == str(args.model), 'model_environment_mismatch')
    sys.addaudithook(deny_python_network)
    html = fixture_html(args.model)
    check(CHROMIUM.is_file(), 'pinned_chromium_missing')
    sys.path.insert(0, str(RUNTIME))
    import capture_readiness as readiness
    import model1_execution as execution
    import model3_rendering as rendering
    from market_vision import coinglass_heatmap_capture as capture
    from playwright.sync_api import Browser, BrowserType, Page, sync_playwright
    check(capture.HEATMAP_MODEL == args.model, 'runtime_model_mismatch')
    check('market_vision.openai_heatmap_scanner' not in sys.modules, 'ai_module_loaded')
    output = EVIDENCE / f'm{args.model}-{args.timeframe.lower()}-{args.case}'
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    (output / 'fixture.html').write_text(html, encoding='utf8')
    browsers, contexts = [], []
    active_html = [html]
    counts = {'browser_launches': 0, 'contexts': 0, 'screenshots': 0,
              'fulfilled_documents': 0, 'aborted_requests': 0}
    original_launch, original_context, original_shot = BrowserType.launch, Browser.new_context, Page.screenshot
    original_preparation, original_observation = rendering.preparation, rendering.observation
    render_facts = {'early_active': 0, 'late_active': 0, 'restored': 0}
    def model3_enabled(page):
        return page.evaluate('() => globalThis.__cg29RenderLab?.snapshot().some(value => value.enabled) || false') is True
    @contextmanager
    def preparation(page, model):
        with original_preparation(page, model):
            if model == 3:
                render_facts['early_active'] += int(model3_enabled(page))
            yield
        if model == 3:
            render_facts['restored'] += int(not model3_enabled(page))
    @contextmanager
    def observation(page, model, geometry):
        with original_observation(page, model, geometry) as prepare:
            if model == 3:
                render_facts['late_active'] += int(model3_enabled(page))
            yield prepare
        if model == 3:
            render_facts['restored'] += int(not model3_enabled(page))
    def launch(browser_type, *a, **kw):
        check(browser_type.name == 'chromium', 'unexpected_browser')
        kw.update(executable_path=str(CHROMIUM), headless=True)
        kw['args'] = [*kw.get('args', []), '--disable-background-networking',
                      '--disable-component-update', '--no-first-run', '--host-resolver-rules=MAP * ~NOTFOUND']
        result = original_launch(browser_type, *a, **kw)
        browsers.append(result)
        counts['browser_launches'] += 1
        check(result.version.startswith('140.'), 'browser_version_mismatch')
        return result
    def context(browser, *a, **kw):
        check('storage_state' not in kw and 'extra_http_headers' not in kw, 'session_input_forbidden')
        kw.update(offline=True, service_workers='block')
        result = original_context(browser, *a, **kw)
        contexts.append(result)
        counts['contexts'] += 1
        def fulfill(route):
            request = route.request
            if (request.url == capture.COINGLASS_HEATMAP_URL and request.method == 'GET'
                    and request.resource_type == 'document' and request.is_navigation_request()
                    and request.frame == request.frame.page.main_frame):
                counts['fulfilled_documents'] += 1
                route.fulfill(status=200, content_type='text/html', body=active_html[0])
            else:
                counts['aborted_requests'] += 1
                route.abort()
        result.route('**/*', fulfill)
        if args.case in ('failure', 'timeout'):
            print('FIXTURE_BROWSER_READY', flush=True)
        if args.case == 'failure':
            raise FixtureFailure('injected_after_context')
        if args.case == 'timeout':
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                time.sleep(1)
        return result
    def screenshot(page, *a, **kw):
        counts['screenshots'] += 1
        return original_shot(page, *a, **kw)
    report = {'format': 'collector-browser-fixture.v1', 'model': args.model,
              'timeframe': args.timeframe, 'case': args.case, 'commit': COMMIT,
              'status': 'failed', 'reason': 'fixture_incomplete', 'runtime_pins_verified': True,
              'real_source_requests': 0, 'ai_client_calls': 0, 'database_connections': 0,
              'native_hover_validated': False, 'png_validated': False,
              'scope': 'synthetic_html_native_capture_hover' if args.case == 'capture' else 'synthetic_dom_guards'}
    try:
        with ExitStack() as stack:
            stack.enter_context(patch.object(BrowserType, 'launch', launch))
            stack.enter_context(patch.object(Browser, 'new_context', context))
            stack.enter_context(patch.object(Page, 'screenshot', screenshot))
            stack.enter_context(patch.object(rendering, 'preparation', preparation))
            stack.enter_context(patch.object(rendering, 'observation', observation))
            if args.case == 'gates':
                with sync_playwright() as driver:
                    browser = driver.chromium.launch(headless=True)
                    try:
                        report.update(exercise_gates(browser, args, capture, readiness, execution,
                                                     rendering, html, active_html))
                    finally:
                        browser.close()
            else:
                images = capture.capture_heatmaps(output, timeframes=(args.timeframe.lower(),))
                check(args.case == 'capture', 'injection_not_reached')
                check(all(not item.is_connected() for item in browsers), 'native_browser_close_failed')
                report['native_browser_disconnected_before_wrapper_cleanup'] = True
                check(len(images) == 1 and images[0]['timeframe'] == args.timeframe.lower(), 'capture_identity_failed')
                snapshot = images[0]['hover_snapshot']
                rows = snapshot.get('rows', [])
                check(snapshot.get('complete') is True and len(rows) == 40, 'native_hover_incomplete')
                check(snapshot.get('heatmap_model') == args.model and snapshot.get('timeframe') == args.timeframe,
                      'native_hover_identity_failed')
                check([row['price'] for row in rows] == [round(100 - i / 10, 1) for i in range(40)],
                      'native_hover_price_failed')
                check(snapshot.get('total_value') == 40_000_000, 'native_hover_total_failed')
                if args.model in (1, 2):
                    check(snapshot['source_candle']['close'] == 98.05, 'native_candle_failed')
                png = output / f'coinglass_btc_heatmap_{args.timeframe.lower()}.png'
                check(Path(images[0]['image']) == png and png.read_bytes()[:8] == b'\x89PNG\r\n\x1a\n',
                      'png_failed')
                check(counts['screenshots'] == 1 and counts['fulfilled_documents'] == 1,
                      'capture_count_failed')
                if args.model == 3:
                    check(render_facts == {'early_active': 1, 'late_active': 2, 'restored': 3},
                          'model3_render_fixture_gate_failed')
                (output / 'hover.json').write_text(json.dumps(snapshot, allow_nan=False, indent=2), encoding='utf8')
                report.update(native_hover_validated=True, png_validated=True, rows=40,
                              tooltip_reads=snapshot['tooltip_reads'],
                              png_sha256=hashlib.sha256(png.read_bytes()).hexdigest(),
                              artifacts=[str(output / 'fixture.html'), str(png), str(output / 'hover.json')])
            report.update(status='passed', reason='fixture_verified')
    except FixtureFailure as error:
        if args.case == 'failure' and error.args == ('injected_after_context',):
            report.update(status='passed', reason='injected_setup_failure_observed')
        else:
            report['reason'] = error.args[0] if error.args else 'fixture_assertion_failed'
    except BaseException as error:
        report['reason'] = 'native_capture_rejected' if getattr(error, 'code', None) else 'fixture_execution_failed'
        allowed = execution.CODES | execution.VALIDATION_REASONS
        code = getattr(error, 'code', None)
        reason = getattr(error, 'reason', None)
        if isinstance(code, str) and code in allowed:
            report['native_code'] = code
        if isinstance(reason, str) and reason in allowed:
            report['native_reason'] = reason
    finally:
        close_requests = 0
        for item in reversed(contexts):
            try:
                item.close()
                close_requests += 1
            except BaseException:
                pass
        for item in reversed(browsers):
            try:
                if item.is_connected():
                    item.close()
            except BaseException:
                pass
        report.update(counts, context_close_requests_completed=close_requests,
                      browsers_disconnected=all(not item.is_connected() for item in browsers),
                      browser_version=browsers[0].version if browsers else None,
                      model3_render_observations=render_facts,
                      ai_module_loaded='market_vision.openai_heatmap_scanner' in sys.modules)
    apply_browser_close_check(report)
    check(report['ai_module_loaded'] is False, 'ai_module_loaded')
    return report


def live_worker():
    """One approved source-only capture, using the installed unedited reader."""
    # The supervisor masks cancellation across fork/exec and child assignment.
    # Do not pass that temporary mask onward to the driver or Chromium.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM, signal.SIGHUP})
    report = empty_live_report()
    pages, browsers, contexts, diagnostics = [], [], [], []
    output = EVIDENCE / 'live-attempt' / 'transient'
    failure_phase = [None]
    current_phase = ['startup']
    try:
        allowed = {'PATH', 'LANG', 'LC_CTYPE', 'PLAYWRIGHT_BROWSERS_PATH', 'PYTHONDONTWRITEBYTECODE',
                   'COLLECTION_HEATMAP_MODEL', 'COLLECTOR_LAB_SUPERVISED',
                   'COINGLASS_COOKIE_HEADER', 'COLLECTOR_LIVE_DEADLINE_MONOTONIC'}
        if (set(os.environ) - allowed or os.environ.get('COLLECTOR_LAB_SUPERVISED') != '1'
                or os.environ.get('COLLECTION_HEATMAP_MODEL') != '1'
                or not os.environ.get('COINGLASS_COOKIE_HEADER')):
            raise LiveStop('unexpected_environment')
        import resource
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        deadline = float(os.environ['COLLECTOR_LIVE_DEADLINE_MONOTONIC'])
        if not math.isfinite(deadline) or not 0 < deadline - time.monotonic() <= 45:
            raise LiveStop('work_deadline')
        for relative, expected in HASHES.items():
            if hashlib.sha256((RUNTIME / relative).read_bytes()).hexdigest() != expected:
                raise LiveStop('runtime_pin_mismatch')
        report['runtime_pins_verified'] = True
        if not CHROMIUM.is_file() or output.parent.is_symlink() or not output.parent.is_dir():
            raise LiveStop('live_setup_failed')
        output.mkdir(mode=0o700, exist_ok=False)
        sys.path.insert(0, str(RUNTIME))
        import capture_readiness as readiness
        import model1_execution as execution
        import heatmap_hover as hover
        from market_vision import coinglass_heatmap_capture as capture
        from playwright.sync_api import Browser, BrowserType, Page, Locator, Keyboard, Mouse
        if capture.HEATMAP_MODEL != 1: raise LiveStop('identity_failed')
        # Convert with the exact installed loader, then remove the header BEFORE
        # Playwright starts its driver/browser subprocesses. No storage export.
        cookies = [capture._load_cookie_header()]
        os.environ.pop('COINGLASS_COOKIE_HEADER', None)
        if not cookies[0]: raise LiveStop('live_setup_failed')
        # Chromium/driver temporary profiles must remain in the same owned
        # subtree even if the supervisor has to kill them before normal close.
        browser_tmp = output / 'browser-tmp'
        browser_tmp.mkdir(mode=0o700, exist_ok=False)
        os.environ['TMPDIR'] = str(browser_tmp)
        sys.addaudithook(deny_python_network)
        def access_check():
            if not pages: return
            page = pages[0]
            if not readiness.valid_source(page.url, 1): raise LiveStop('identity_failed')
            try:
                readiness.require_unblocked_source(page, 1, phase=current_phase[0])
            except execution.StageFailure:
                report['native_code'] = 'source_login_required'
                raise LiveStop('access_modal_blocked') from None
            # Additional strict refusal, never a replacement for the original
            # canonical source gate. Header Login alone does not match this.
            if page.evaluate(LIVE_ACCESS_MODAL) is True:
                raise LiveStop('access_modal_blocked')
        guard = LiveGuard(report, deadline, access_check)
        original_operation = execution.capture_operation
        @contextmanager
        def operation(phase):
            previous = current_phase[0]
            current_phase[0] = phase if phase in LIVE_PHASES else 'capture'
            if phase not in ('context_close', 'browser_close'):
                guard.check()
                print('LIVE_PHASE ' + current_phase[0], flush=True)
            try:
                with original_operation(phase): yield
            except BaseException:
                if failure_phase[0] is None: failure_phase[0] = current_phase[0]
                raise
            finally:
                current_phase[0] = previous
        original_launch, original_context = BrowserType.launch, Browser.new_context
        original_goto, original_shot = Page.goto, Page.screenshot
        from playwright.sync_api import BrowserContext
        original_new_page = BrowserContext.new_page
        original_click, original_press = Locator.click, Locator.press
        original_key, original_mouse = Keyboard.press, Mouse.click
        original_wait = capture.wait_for_render
        original_hover = hover.capture_column
        original_select = capture.select_visible_control
        selected_controls = set()
        def launch(browser_type, *args, **kwargs):
            guard.check()
            if browsers or browser_type.name != 'chromium': raise LiveStop('live_setup_failed')
            kwargs.update(executable_path=str(CHROMIUM), headless=True)
            browser = original_launch(browser_type, *args, **kwargs)
            browsers.append(browser); report['browser_launches'] = 1
            if not browser.version.startswith('140.'): raise LiveStop('live_setup_failed')
            return browser
        def context(browser, *args, **kwargs):
            guard.check()
            if contexts or any(k in kwargs for k in ('storage_state', 'extra_http_headers', 'record_har_path', 'record_video_dir')):
                raise LiveStop('live_setup_failed')
            kwargs['service_workers'] = 'block'
            context = original_context(browser, *args, **kwargs)
            contexts.append(context); report['contexts'] = 1
            def one_navigation(route):
                request = route.request
                if request.is_navigation_request() and request.frame == request.frame.page.main_frame:
                    expected_page = len(pages) == 1 and request.frame.page == pages[0]
                    if (not expected_page or guard.navigation_requests or
                            request.url != capture.COINGLASS_HEATMAP_URL or request.method != 'GET'):
                        report['blocked_navigation_requests'] = min(100000, report['blocked_navigation_requests'] + 1)
                        route.abort(); return
                    guard.navigation_requests += 1
                route.fallback()
            # Includes browser-created popups; the unchanged advertising filter
            # is installed later and remains active. No bodies/headers are read.
            context.route('**/*', one_navigation)
            return context
        def new_page(context, *args, **kwargs):
            guard.check()
            if pages: raise LiveStop('second_navigation_forbidden')
            page = original_new_page(context, *args, **kwargs)
            pages.append(page)
            diagnostics.append(readiness.CaptureNetworkDiagnostics(page))
            page.on('request', lambda _request: report.__setitem__('requests_seen', min(100000, report['requests_seen'] + 1)))
            page.on('response', lambda _response: report.__setitem__('responses_seen', min(100000, report['responses_seen'] + 1)))
            return page
        def goto(page, url, *args, **kwargs):
            if url != capture.COINGLASS_HEATMAP_URL: raise LiveStop('identity_failed')
            return guard.invoke('goto', original_goto, page, url, *args, **kwargs)
        def reload(*args, **kwargs): return guard.invoke('reload', None, *args, **kwargs)
        def retry(*args, **kwargs): return guard.invoke('retry', None, *args, **kwargs)
        def click(*args, **kwargs): return guard.invoke('click', original_click, *args, **kwargs)
        def press(*args, **kwargs): return guard.invoke('key', original_press, *args, **kwargs)
        def key(*args, **kwargs): return guard.invoke('key', original_key, *args, **kwargs)
        def mouse(*args, **kwargs): return guard.invoke('click', original_mouse, *args, **kwargs)
        def dismiss(page):
            access_check()
            # Stop instead of entering the legacy login-close fallback even
            # when matching text is outside the canonical modal selectors.
            text = page.get_by_text(re.compile(r'log\s*in\s+to\s+unlock\s+full\s+data', re.I))
            if capture._first_visible(text) is not None: raise LiveStop('access_modal_blocked')
            capture._dismiss_common_overlays(page)
            access_check()
        def wait_for_render(*args, **kwargs):
            state = original_wait(*args, **kwargs)
            report['source_identity_verified'] = (readiness.valid_source(args[0].url, 1)
                                                  and selected_controls == {'Model 1', 'Symbol'})
            return state
        def select_control(page, label):
            result = original_select(page, label)
            if label not in ('Model 1', 'Symbol') or result is not True: raise LiveStop('identity_failed')
            selected_controls.add(label)
            return result
        def screenshot(page, *args, **kwargs):
            guard.check(); access_check()
            target = output / 'coinglass_btc_heatmap_12h.png'
            if report['screenshots'] or args or kwargs.get('path') != str(target): raise LiveStop('output_invalid')
            report['screenshots'] = 1
            return original_shot(page, **kwargs)
        def native_hover(*args, **kwargs):
            guard.check(); access_check()
            snapshot = original_hover(*args, **kwargs)
            guard.check(); access_check()
            final = readiness.current_state(pages[0], '12h', 1)
            if not final.get('ready') or final.get('label') != '12 hour': raise LiveStop('identity_failed')
            return snapshot
        with ExitStack() as stack:
            for owner, name, function in ((BrowserType, 'launch', launch), (Browser, 'new_context', context),
                    (BrowserContext, 'new_page', new_page), (Page, 'goto', goto), (Page, 'reload', reload),
                    (Page, 'screenshot', screenshot), (Locator, 'click', click), (Locator, 'press', press),
                    (Keyboard, 'press', key), (Mouse, 'click', mouse),
                    (capture, 'record_source_reload', retry), (capture, '_dismiss_capture_blockers', dismiss),
                    (capture, 'capture_operation', operation), (execution, 'capture_operation', operation),
                    (capture, 'select_visible_control', select_control),
                    (capture, 'wait_for_render', wait_for_render), (hover, 'capture_column', native_hover),
                    (capture, '_load_cookie_header', lambda: cookies.pop())):
                stack.enter_context(patch.object(owner, name, function))
            results = capture.capture_heatmaps(output, timeframes=('12h',))
        if len(results) != 1 or results[0]['timeframe'] != '12h' or not report['source_identity_verified']:
            raise LiveStop('identity_failed')
        snapshot = results[0]['hover_snapshot']
        if not (snapshot.get('complete') is True and snapshot.get('heatmap_model') == 1
                and snapshot.get('timeframe') == '12H' and isinstance(snapshot.get('rows'), list)):
            raise LiveStop('identity_failed')
        # Native capture_column already validates all rows, source timestamp,
        # distributed anchors and initial/final candle consistency.
        png = output / 'coinglass_btc_heatmap_12h.png'
        from PIL import Image
        with Image.open(png) as image:
            report['png_width'], report['png_height'] = image.size
            if image.format != 'PNG': raise LiveStop('output_invalid')
            image.verify()
        report.update(png_validated=True, png_sha256=hashlib.sha256(png.read_bytes()).hexdigest(),
            native_hover_validated=True, row_count=len(snapshot['rows']), tooltip_reads=snapshot['tooltip_reads'],
            source_time=snapshot['source_time'], candle_close=snapshot['source_candle']['close'],
            total_value=snapshot['total_value'], status='passed', reason='capture_verified')
    except LiveStop as error:
        report.update(status='stopped', reason=error.reason)
    except BaseException as error:
        code, reason = getattr(error, 'code', None), getattr(error, 'reason', None)
        report['reason'] = 'native_capture_rejected' if code in LIVE_NATIVE_CODES or reason in LIVE_NATIVE_REASONS else 'live_capture_failed'
        if code in LIVE_NATIVE_CODES: report['native_code'] = code
        if reason in LIVE_NATIVE_REASONS: report['native_reason'] = reason
    finally:
        os.environ.pop('COINGLASS_COOKIE_HEADER', None)
        report['phase'] = failure_phase[0] or ('hover_collection' if report['native_hover_validated'] else current_phase[0])
        for context in reversed(contexts):
            try: context.close()
            except BaseException: pass
        for browser in reversed(browsers):
            try:
                if browser.is_connected(): browser.close()
            except BaseException: pass
        report['browsers_disconnected'] = bool(browsers) and all(not item.is_connected() for item in browsers)
        if diagnostics:
            try: report['network'] = diagnostics[0].summary()
            except BaseException: pass
        try:
            if output.is_dir(): shutil.rmtree(output)
            report['png_deleted'] = not output.exists()
        except BaseException:
            report.update(status='failed', reason='ephemeral_cleanup_failed')
        if report['status'] == 'passed' and not report['browsers_disconnected']:
            report.update(status='failed', reason='browser_close_unverified')
    if 'market_vision.openai_heatmap_scanner' in sys.modules:
        report.update(status='failed', reason='live_capture_failed')
    return sanitize_live_report(report) or empty_live_report()


def main():
    if sys.argv[1:] == ['--live-worker']:
        report = live_worker()
        print('LIVE_RESULT ' + json.dumps(report, allow_nan=False, separators=(',', ':')), flush=True)
        return 0 if report['status'] == 'passed' else 1
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=int, choices=(1, 2, 3), required=True)
    parser.add_argument('--timeframe', choices=('12H', '24H', '48H'), required=True)
    parser.add_argument('--case', choices=CASES, default='capture')
    parser.add_argument('--worker', action='store_true')
    args = parser.parse_args()
    if not args.worker:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import collector_matrix
        report = collector_matrix.single(args.model, args.timeframe, args.case)
        within_limit = collector_matrix.emit_report(report)
        return 0 if within_limit and report['status'] == 'passed' else 1
    try:
        report = worker(args)
    except BaseException:
        report = {'format': 'collector-browser-fixture.v1', 'status': 'failed',
                  'reason': 'fixture_setup_failed', 'case': args.case,
                  'model': args.model, 'timeframe': args.timeframe}
    serialized = json.dumps(report, allow_nan=False, separators=(',', ':'))
    if len(serialized.encode('utf8')) > 32 * 1024:
        report = {'format': 'collector-browser-fixture.v1', 'status': 'failed', 'reason': 'output_limit',
                  'case': args.case, 'model': args.model, 'timeframe': args.timeframe}
        serialized = json.dumps(report, separators=(',', ':'))
    print('FIXTURE_RESULT ' + serialized, flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
