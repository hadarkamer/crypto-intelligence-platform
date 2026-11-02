"""Decision/cleanup tests with no browser, source account or AI calls."""
import ast
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from install_capture_readiness import expand_source_reload as expand
from capture_readiness import record_source_reload as record_reload

SOURCE = '''
def capture_heatmaps(out, timeframes):
    results = []
    with sync_playwright() as p:
        try:
            page.goto('source')
            for timeframe in timeframes:
                read_map(timeframe)
                results.append(timeframe)
        except Exception:
            raise
        with capture_operation('context_close'):
            context.close()
        with capture_operation('browser_close'):
            browser.close()
    return results
'''


class Failure(RuntimeError):
    def __init__(self, code='source_not_readable', phase='render_readiness', reason='no-data'):
        self.code = code
        self._model1_capture_phase = phase
        self._model1_source_readiness = {'reason': reason}


class ReloadTests(unittest.TestCase):
    def execute(self, errors, *, reload_error=False):
        calls = []
        def read(tf):
            calls.append(('read', tf))
            error = errors.pop(0) if errors else None
            if error:
                raise error
        def refresh(**kwargs):
            calls.append(('reload', kwargs))
            if reload_error:
                raise RuntimeError('Reload failed')
        ns = dict(sync_playwright=nullcontext, capture_operation=lambda _: nullcontext(),
                  page=SimpleNamespace(goto=lambda _: calls.append(('goto',)), reload=refresh),
                  context=SimpleNamespace(close=lambda: calls.append(('context_close',))),
                  browser=SimpleNamespace(close=lambda: calls.append(('browser_close',))),
                  read_map=read, record_source_reload=lambda *a: calls.append(('record', a[1])),
                  _dismiss_capture_blockers=lambda p: None, _wait_for_heatmap=lambda p: None,
                  _select_model_one=lambda p: calls.append(('model',)),
                  _select_symbol_mode=lambda p: calls.append(('symbol',)))
        exec(compile(expand(SOURCE), '<test-capture>', 'exec'), ns)
        error = None
        result = None
        try:
            result = ns['capture_heatmaps']('.', ('12h', '24h', '48h'))
        except Exception as exc:
            error = exc
        self.assertEqual(calls[-2:], [('context_close',), ('browser_close',)])
        return calls, result, error

    def test_success_has_no_reload(self):
        calls, result, error = self.execute([])
        self.assertEqual(result, ['12h', '24h', '48h'])
        self.assertFalse(any(c[0] == 'reload' for c in calls))
        self.assertIsNone(error)

    def test_retry_only_failed_timeframe_and_preserve_completed(self):
        calls, result, error = self.execute([None, Failure()])
        self.assertEqual(result, ['12h', '24h', '48h'])
        self.assertEqual([c[1] for c in calls if c[0] == 'read'], ['12h', '24h', '24h', '48h'])
        self.assertEqual(sum(c[0] == 'reload' for c in calls), 1)
        self.assertIsNone(error)

    def test_persistent_error_stops_after_one_reload(self):
        calls, _, error = self.execute([Failure(), Failure()])
        self.assertIsInstance(error, Failure)
        self.assertEqual(sum(c[0] == 'reload' for c in calls), 1)
        self.assertEqual(sum(c[0] == 'read' for c in calls), 2)

    def test_reload_failure_does_not_loop(self):
        calls, _, error = self.execute([Failure()], reload_error=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(sum(c[0] == 'reload' for c in calls), 1)
        self.assertEqual(sum(c[0] == 'read' for c in calls), 1)

    def test_non_source_failures_are_not_retried(self):
        for error in (Failure('source_login_required'), Failure('analysis_timeout'),
                      Failure(phase='hover_collection'), Failure(reason='request-limit'),
                      ValueError('Unknown failure')):
            with self.subTest(error=repr(error)):
                calls, _, raised = self.execute([error])
                self.assertIs(raised, error)
                self.assertFalse(any(c[0] == 'reload' for c in calls))

    def test_unrecognized_source_structure_rejected(self):
        with self.assertRaises(RuntimeError):
            expand(SOURCE.replace('for timeframe in timeframes:', 'for timeframe in other:'))

    def test_first_evidence_retained_before_overwrite(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            for suffix in ('.png', '.render.json'):
                (root / ('coinglass_btc_heatmap_24h' + suffix)).write_bytes(b'first')
            record_reload(root, '24h', Failure())
            (root / 'coinglass_btc_heatmap_24h.png').write_bytes(b'second')
            self.assertEqual((root / 'before-reload/coinglass_btc_heatmap_24h.png').read_bytes(), b'first')


if __name__ == '__main__':
    unittest.main()
