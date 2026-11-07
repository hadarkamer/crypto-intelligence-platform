#!/usr/bin/env python3
"""Pure changed-boundary tests. No browser, database or network is started."""
import ast
from contextlib import nullcontext
import hashlib
import importlib.util
from pathlib import Path
import re
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('live_worker_contract', HERE / 'collector_browser_fixture.py')
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
RUNTIME = Path('/lab/collector/standalone_collector/runtime')
if len(sys.argv) == 3 and sys.argv[1] == '--runtime':
    RUNTIME = Path(sys.argv[2]); sys.argv = sys.argv[:1]


class LiveGuardTests(unittest.TestCase):
    def setUp(self):
        self.report = worker.empty_live_report()
        self.access = Mock()
        self.guard = worker.LiveGuard(self.report, float('inf'), self.access)

    def stopped(self, reason, function):
        with self.assertRaises(worker.LiveStop) as caught: function()
        self.assertEqual(reason, caught.exception.reason)

    def test_one_navigation_and_no_reload_retry(self):
        action = Mock(return_value='first')
        self.assertEqual('first', self.guard.invoke('goto', action, 'page', 'fixed'))
        self.stopped('second_navigation_forbidden', lambda: self.guard.invoke('goto', action))
        self.stopped('reload_forbidden', lambda: self.guard.invoke('reload', action))
        self.stopped('retry_forbidden', lambda: self.guard.invoke('retry', action))
        self.assertEqual(1, action.call_count)

    def test_timeout_or_observed_extra_navigation_prevents_action(self):
        action = Mock()
        self.guard.deadline = 9
        with patch.object(worker.time, 'monotonic', return_value=10):
            self.stopped('work_deadline', lambda: self.guard.invoke('click', action))
        self.guard.deadline = float('inf')
        self.report['blocked_navigation_requests'] = 1
        self.stopped('second_navigation_forbidden', lambda: self.guard.invoke('click', action))
        action.assert_not_called()

    def test_access_modal_prevents_action_and_forced_click_never_dispatches(self):
        action = Mock()
        self.access.side_effect = worker.LiveStop('access_modal_blocked')
        self.stopped('access_modal_blocked', lambda: self.guard.invoke('click', action))
        self.stopped('forced_click_forbidden', lambda: self.guard.invoke('click', action, force=True))
        action.assert_not_called()

    def test_native_click_and_enter_retained_escape_not_dispatched(self):
        action = Mock(return_value=123)
        self.assertEqual(123, self.guard.invoke('click', action, 'candle', 4, 5))
        self.assertEqual(123, self.guard.invoke('key', action, 'consent', 'Enter'))
        self.assertIsNone(self.guard.invoke('key', action, 'keyboard', 'Escape'))
        self.assertEqual(2, action.call_count)
        self.assertEqual(5, self.access.call_count)

    def test_actual_timeframe_force_fallback_stops_before_second_click(self):
        source = (RUNTIME / 'market_vision/coinglass_heatmap_capture.py').read_text()
        self.assertEqual(worker.HASHES['market_vision/coinglass_heatmap_capture.py'], hashlib.sha256(source.encode()).hexdigest())
        node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == '_select_timeframe')
        native_click = Mock(side_effect=RuntimeError('public synthetic first-click timeout'))
        trigger = Mock()
        trigger.click.side_effect = lambda **kw: self.guard.invoke('click', native_click, **kw)
        page = Mock()
        page.keyboard.press.side_effect = lambda key: self.guard.invoke('key', Mock(), page.keyboard, key)
        scope = {'re': re, 'Page': object, 'Locator': object,
                 '_first_visible': Mock(side_effect=[None, trigger]),
                 'capture_operation': lambda _: nullcontext()}
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<pinned-timeframe>', 'exec'), scope)
        self.stopped('forced_click_forbidden', lambda: scope['_select_timeframe'](page, '12h'))
        self.assertEqual(1, native_click.call_count)
        self.assertNotIn('force', native_click.call_args.kwargs)

    def test_actual_retry_branch_stops_before_record_or_reload(self):
        source = (RUNTIME / 'market_vision/coinglass_heatmap_capture.py').read_text()
        tree = ast.parse(source)
        handler = next(n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler) and n.name == '_capture_error')
        error = RuntimeError('synthetic')
        error.code = 'source_not_readable'; error._model1_capture_phase = 'render_readiness'
        page = Mock()
        scope = {'_capture_attempt': 0, '_capture_error': error, 'out': None,
                 'timeframe': '12h', 'page': page,
                 'record_source_reload': lambda *a: self.guard.invoke('retry', Mock(), *a)}
        self.stopped('retry_forbidden', lambda: exec(compile(ast.Module(body=handler.body, type_ignores=[]), '<pinned-retry>', 'exec'), scope))
        page.reload.assert_not_called()

    def test_live_dismiss_refuses_legacy_login_branch(self):
        source = (HERE / 'collector_browser_fixture.py').read_text()
        parent = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == 'live_worker')
        function = next(n for n in ast.walk(parent) if isinstance(n, ast.FunctionDef) and n.name == 'dismiss')
        capture, page = Mock(), Mock()
        capture._first_visible.return_value = object()
        scope = {'access_check': self.access, 'capture': capture, 're': re, 'LiveStop': worker.LiveStop}
        exec(compile(ast.Module(body=[function], type_ignores=[]), '<live-dismiss>', 'exec'), scope)
        self.stopped('access_modal_blocked', lambda: scope['dismiss'](page))
        capture._dismiss_common_overlays.assert_not_called()

    def test_receipt_allowlist_rejects_raw_secret_error_or_unknown_field(self):
        self.assertIsNotNone(worker.sanitize_live_report(self.report))
        for key, value in [('cookie', 'PUBLIC_CANARY'), ('reason', 'PUBLIC_CANARY'),
                           ('native_code', 'PUBLIC_CANARY'), ('phase', 'PUBLIC_CANARY'),
                           ('native_reason', ['PUBLIC_CANARY']), ('png_sha256', 'PUBLIC_CANARY')]:
            candidate = {**self.report, key: value}
            self.assertIsNone(worker.sanitize_live_report(candidate))
        candidate = {**self.report, 'network': {**self.report['network'], 'url': 'PUBLIC_CANARY'}}
        self.assertIsNone(worker.sanitize_live_report(candidate))

    def test_actual_route_allows_first_document_blocks_redirect_and_popup(self):
        tree = ast.parse((HERE / 'collector_browser_fixture.py').read_text())
        parent = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'live_worker')
        function = next(n for n in ast.walk(parent) if isinstance(n, ast.FunctionDef) and n.name == 'one_navigation')
        class PageIdentity: pass
        class FrameIdentity:
            def __init__(self, page): self.page = page
        page = PageIdentity(); page.main_frame = FrameIdentity(page)
        capture = SimpleNamespace(COINGLASS_HEATMAP_URL='https://public.invalid/fixed')
        scope = {'pages': [page], 'guard': self.guard, 'report': self.report, 'capture': capture}
        exec(compile(ast.Module(body=[function], type_ignores=[]), '<live-route>', 'exec'), scope)
        request = SimpleNamespace(is_navigation_request=lambda: True, frame=page.main_frame,
                                  url=capture.COINGLASS_HEATMAP_URL, method='GET')
        first = Mock(request=request); scope['one_navigation'](first)
        first.fallback.assert_called_once(); first.abort.assert_not_called()
        again = Mock(request=request); scope['one_navigation'](again)
        again.abort.assert_called_once(); again.fallback.assert_not_called()
        other = PageIdentity(); other.main_frame = FrameIdentity(other)
        popup = Mock(request=SimpleNamespace(**{**request.__dict__, 'frame': other.main_frame}))
        scope['one_navigation'](popup); popup.abort.assert_called_once()
        subframe = Mock(request=SimpleNamespace(**{**request.__dict__, 'frame': FrameIdentity(page)}))
        scope['one_navigation'](subframe); subframe.fallback.assert_called_once()
        self.assertEqual(2, self.report['blocked_navigation_requests'])

    def test_child_unblocks_spawn_mask_even_when_setup_is_rejected(self):
        with patch.dict(worker.os.environ, {}, clear=True), patch.object(worker.signal, 'pthread_sigmask') as mask:
            result = worker.live_worker()
        mask.assert_called_once_with(worker.signal.SIG_UNBLOCK,
            {worker.signal.SIGINT, worker.signal.SIGTERM, worker.signal.SIGHUP})
        self.assertEqual('unexpected_environment', result['reason'])
        self.assertEqual(0, result['browser_launches'])

    def test_exact_conversion_and_temp_confinement_precede_browser_launch(self):
        tree = ast.parse((HERE / 'collector_browser_fixture.py').read_text())
        parent = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'live_worker')
        statements = next(n for n in parent.body if isinstance(n, ast.Try)).body
        start = next(i for i, n in enumerate(statements) if isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id == 'cookies' for t in n.targets))
        end = next(i for i, n in enumerate(statements) if isinstance(n, ast.Expr)
                   and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == 'sys.addaudithook')
        environment = {'COINGLASS_COOKIE_HEADER': 'PUBLIC_DUMMY_VALUE'}
        loader = Mock(return_value=[{'name': 'fixture', 'value': 'PUBLIC_DUMMY_VALUE'}])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            scope = {'capture': SimpleNamespace(_load_cookie_header=loader),
                     'os': SimpleNamespace(environ=environment), 'output': output, 'LiveStop': worker.LiveStop}
            exec(compile(ast.Module(body=statements[start:end], type_ignores=[]), '<live-private-setup>', 'exec'), scope)
            loader.assert_called_once_with()
            self.assertNotIn('COINGLASS_COOKIE_HEADER', environment)
            self.assertEqual(str(output / 'browser-tmp'), environment['TMPDIR'])
            self.assertEqual(0o700, (output / 'browser-tmp').stat().st_mode & 0o777)


if __name__ == '__main__': unittest.main(verbosity=2)
