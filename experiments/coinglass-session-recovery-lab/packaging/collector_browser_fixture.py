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
import os
from pathlib import Path
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


def main():
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
