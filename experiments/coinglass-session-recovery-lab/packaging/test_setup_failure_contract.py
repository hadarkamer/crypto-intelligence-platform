"""Pure source-extracted verdict regressions. No Playwright import or execution."""
import ast
import copy
import hashlib
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
WORKER = ast.parse((HERE / 'collector_browser_fixture.py').read_text())
MATRIX_BYTES = (HERE / 'collector_matrix.py').read_bytes()
assert hashlib.sha256(MATRIX_BYTES).hexdigest() == '830532450252de57538db641f24621331b8ce63fb4b7ab9f22ff7c90fad4d4b3'
helper = next(node for node in WORKER.body if isinstance(node, ast.FunctionDef)
              and node.name == 'apply_browser_close_check')
scope = {}
exec(compile(ast.Module(body=[helper], type_ignores=[]), '<fixture-verdict>', 'exec'), scope)
apply_check = scope['apply_browser_close_check']

# Exercise the actual unchanged matrix's final acceptance branches, with
# supplied process-proof facts; never execute its process or browser helpers.
matrix = ast.parse(MATRIX_BYTES)
single = next(node for node in matrix.body if isinstance(node, ast.FunctionDef) and node.name == 'single')
start = next(index for index, node in enumerate(single.body) if isinstance(node, ast.If)
             and ast.unparse(node.test).startswith("case == 'timeout'"))
verdict = ast.parse('def matrix_verdict(report, case, timed_out, marker_seen, observed, reason):\n    pass').body[0]
verdict.body = copy.deepcopy(single.body[start:start + 3]) + [ast.Return(value=ast.Name(id='report', ctx=ast.Load()))]
exec(compile(ast.fix_missing_locations(ast.Module(body=[verdict], type_ignores=[])),
             '<unchanged-matrix-verdict>', 'exec'), scope)
matrix_verdict = scope['matrix_verdict']


def expected():
    return {'case': 'failure', 'status': 'passed', 'reason': 'injected_setup_failure_observed',
            'browser_launches': 1, 'contexts': 1, 'screenshots': 0,
            'fulfilled_documents': 0, 'browsers_disconnected': False}


class SetupFailureContractTests(unittest.TestCase):
    def test_only_expected_setup_failure_defers_and_retains_false_observation(self):
        report = expected()
        apply_check(report)
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['browser_close_verification'], 'requires_supervisor_process_census')
        self.assertIs(report['browsers_disconnected'], False)

    def test_other_cases_or_missing_injection_evidence_still_fail(self):
        for field, value in [('case', 'capture'), ('case', 'gates'), ('case', 'timeout'),
                             ('status', 'failed'), ('reason', 'fixture_execution_failed'),
                             ('browser_launches', 0), ('contexts', 2),
                             ('screenshots', 1), ('fulfilled_documents', 1)]:
            with self.subTest(field=field, value=value):
                report = {**expected(), field: value}
                apply_check(report)
                self.assertEqual(report['status'], 'failed')
                self.assertEqual(report['reason'], 'browser_close_unverified')

    def test_actual_parent_verdict_requires_cleanup_no_fault_and_observed_browser(self):
        for clean, fault, observed, passes in [(True, False, {(123, 456)}, True),
                                              (False, False, {(123, 456)}, False),
                                              (True, True, {(123, 456)}, False),
                                              (True, False, set(), False)]:
            with self.subTest(clean=clean, fault=fault, observed=bool(observed)):
                report = expected()
                apply_check(report)
                report.update(cleanup_verified=clean, cleanup_fault=fault)
                result = matrix_verdict(report, 'failure', False, False, observed, None)
                self.assertEqual(result['status'] == 'passed', passes)

    def test_normal_disconnection_observation_is_retained(self):
        report = {**expected(), 'case': 'capture', 'browsers_disconnected': True}
        apply_check(report)
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['browser_close_verification'], 'playwright_object_disconnected')

    def test_native_capture_close_assertion_remains_before_wrapper_cleanup(self):
        worker = next(node for node in WORKER.body if isinstance(node, ast.FunctionDef) and node.name == 'worker')
        source = ast.unparse(worker)
        assertion = "check(all((not item.is_connected() for item in browsers)), 'native_browser_close_failed')"
        self.assertIn(assertion, source)
        self.assertLess(source.index(assertion), source.index('apply_browser_close_check(report)'))


if __name__ == '__main__':
    unittest.main()
