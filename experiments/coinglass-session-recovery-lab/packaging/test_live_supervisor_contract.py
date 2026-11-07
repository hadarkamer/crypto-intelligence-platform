"""Pure mocked live-supervisor tests: no child, browser, socket, source or DB.

Only the existing strict receipt sanitizer and supervisor control flow execute.
The public dummy cookie is never a real credential. Temporary files are local
test marker/receipt fixtures and are removed by TemporaryDirectory.
"""
import ast
from contextlib import ExitStack, contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('matrix_contract_test', HERE / 'collector_matrix.py')
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)
worker_spec = importlib.util.spec_from_file_location('worker_contract_test', HERE / 'collector_browser_fixture.py')
worker = importlib.util.module_from_spec(worker_spec)
worker_spec.loader.exec_module(worker)
DUMMY = 'obe=PUBLIC_LIVE_SUPERVISOR_DUMMY_ONLY'


def child_report(passed=False):
    result = worker.empty_live_report()
    result.update(status='stopped', reason='access_modal_blocked', phase='overlay_dismissal',
                  runtime_pins_verified=True, source_identity_verified=True,
                  png_deleted=True, browsers_disconnected=True,
                  browser_launches=1, contexts=1, navigation_calls=1)
    if passed:
        result.update(status='passed', reason='capture_verified', phase='browser_close',
                      native_hover_validated=True, png_validated=True, screenshots=1,
                      row_count=40, tooltip_reads=60, png_width=1600, png_height=1000,
                      png_sha256='0' * 64, source_time='2026-11-07T12:00:00Z',
                      candle_close=100.0, total_value=1_000_000.0)
    assert worker.sanitize_live_report(result) is not None
    return result


class Pipe:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.closed = False
    def fileno(self):
        return self.descriptor
    def close(self):
        self.closed = True


class Selector:
    def __init__(self):
        self.keys = {}
    def register(self, pipe, _event, is_stdout):
        self.keys[pipe] = SimpleNamespace(fileobj=pipe, data=is_stdout)
    def unregister(self, pipe):
        del self.keys[pipe]
    def select(self, _timeout):
        return [(key, 1) for key in list(self.keys.values())]
    def get_map(self):
        return self.keys
    def close(self):
        self.keys.clear()


@contextmanager
def fake_run(result=None, chunks=None, core=(0, 0), clean=True, cleanup_fault=False,
             lock_busy=False, clock=None, cancel_during_spawn=False):
    child = SimpleNamespace(stdout=Pipe(301), stderr=Pipe(302), returncode=0)
    child.poll = lambda: child.returncode
    receipt = result if result is not None else child_report()
    stdout_chunks = list(chunks) if chunks is not None else [
        b'LIVE_PHASE browser_launch\n',
        b'LIVE_RESULT ' + json.dumps(receipt).encode() + b'\n']
    pending = {301: stdout_chunks, 302: [DUMMY.encode()]}
    facts = {'events': [], 'spawned': [], 'cleanup_children': [], 'cleanup_deadlines': [],
             'pending_cancel': False, 'handlers': {}, 'mask': set()}
    moments = iter(clock or [])
    last_time = [100.0]
    def monotonic():
        last_time[0] = next(moments, last_time[0])
        return last_time[0]
    def read(descriptor, _limit):
        return pending[descriptor].pop(0) if pending[descriptor] else b''
    def install_handler(signum, handler):
        previous = facts['handlers'].get(signum, signal.SIG_DFL)
        facts['handlers'][signum] = handler
        return previous
    def sigmask(how, values):
        previous = set(facts['mask'])
        facts['mask'] = (facts['mask'] | set(values) if how == signal.SIG_BLOCK else set(values))
        if facts['pending_cancel'] and signal.SIGTERM not in facts['mask']:
            facts['pending_cancel'] = False
            handler = facts['handlers'].get(signal.SIGTERM)
            if callable(handler):
                handler(signal.SIGTERM, None)
        return previous
    def acquire():
        facts['events'].append('lock')
        if lock_busy:
            raise matrix.SupervisorFailure('serial_lock_busy')
        return 900
    def spawn(command, **kwargs):
        facts['events'].append('spawn')
        assert (matrix.EVIDENCE / 'live-attempt').is_dir()
        assert signal.SIGTERM in facts['mask']
        facts['spawned'].append({'command': list(command), 'environment': dict(kwargs['env'])})
        temporary = matrix.EVIDENCE / 'live-attempt/transient'
        temporary.mkdir()
        (temporary / 'synthetic.png').write_bytes(b'public dummy transient')
        facts['pending_cancel'] = cancel_during_spawn
        return child
    def cleanup(process, deadline):
        facts['events'].append('cleanup')
        facts['cleanup_children'].append(process)
        facts['cleanup_deadlines'].append(deadline)
        assert facts['handlers'].get(signal.SIGTERM) == signal.SIG_IGN
        child.returncode = 0
        return clean, 0, cleanup_fault
    def unlock(descriptor, operation):
        assert descriptor == 900
        assert operation == matrix.fcntl.LOCK_UN
        facts['events'].append('unlock')
    original_close = matrix.os.close
    def close(descriptor):
        if descriptor != 900:
            original_close(descriptor)
    with tempfile.TemporaryDirectory(prefix='live-supervisor-contract-') as directory:
        evidence = Path(directory) / 'collector'
        with ExitStack() as stack:
            for target, value in [('EVIDENCE', evidence), ('_LIVE_RETAINED_LOCKS', []),
                                  ('_live_contract', lambda: (worker.sanitize_live_report, worker.LIVE_PHASES)),
                                  ('establish_subreaper', lambda: None), ('_acquire_live_lock', acquire),
                                  ('descendants', lambda: [{'browser': True, 'pid': 123, 'start': 456}]),
                                  ('cleanup', cleanup)]:
                stack.enter_context(patch.object(matrix, target, value))
            stack.enter_context(patch.object(matrix.resource, 'getrlimit', return_value=core))
            stack.enter_context(patch.object(matrix.subprocess, 'Popen', side_effect=spawn))
            stack.enter_context(patch.object(matrix.selectors, 'DefaultSelector', Selector))
            stack.enter_context(patch.object(matrix.os, 'read', side_effect=read))
            stack.enter_context(patch.object(matrix.os, 'set_blocking'))
            stack.enter_context(patch.object(matrix.os, 'close', side_effect=close))
            stack.enter_context(patch.object(matrix.signal, 'signal', side_effect=install_handler))
            stack.enter_context(patch.object(matrix.signal, 'pthread_sigmask', side_effect=sigmask))
            stack.enter_context(patch.object(matrix.fcntl, 'flock', side_effect=unlock))
            stack.enter_context(patch.object(matrix.time, 'monotonic', side_effect=monotonic))
            facts.update(child=child, evidence=evidence)
            yield facts


class LiveSupervisorTests(unittest.TestCase):
    def test_header_only_child_environment_and_sanitized_persisted_receipt(self):
        with fake_run() as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['status'], 'stopped')
            self.assertTrue(report['cleanup_verified'])
            self.assertTrue(report['lock_released'])
            sent = facts['spawned'][0]
            self.assertNotIn(DUMMY, json.dumps(sent['command']))
            self.assertEqual(sent['environment']['COINGLASS_COOKIE_HEADER'], DUMMY)
            self.assertEqual(sent['environment']['COLLECTOR_LIVE_DEADLINE_MONOTONIC'], '135.0')
            self.assertEqual(set(sent['environment']), set(matrix.child_environment(1)) |
                             {'COINGLASS_COOKIE_HEADER', 'COLLECTOR_LIVE_DEADLINE_MONOTONIC'})
            self.assertEqual(facts['events'], ['lock', 'spawn', 'cleanup', 'unlock'])
            self.assertFalse((facts['evidence'] / 'live-attempt/transient').exists())
            receipt = (facts['evidence'] / 'live-attempt/supervisor.json').read_text()
            self.assertEqual(json.loads(receipt), report)
            self.assertNotIn(DUMMY, receipt)
            self.assertEqual(report['stderr_bytes_suppressed'], len(DUMMY))

    def test_one_shot_marker_prevents_second_spawn_and_preserves_first_receipt(self):
        with fake_run() as facts:
            first = matrix.live_single(DUMMY, 90.0)
            second = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(second['reason'], 'live_attempt_already_consumed')
            self.assertEqual(len(facts['spawned']), 1)
            receipt = json.loads((facts['evidence'] / 'live-attempt/supervisor.json').read_text())
            self.assertEqual(receipt, first)

    def test_expired_original_tty_budget_never_locks_or_spawns(self):
        with fake_run() as facts:
            report = matrix.live_single(DUMMY, 55.0)
            self.assertEqual(report['reason'], 'live_budget_expired')
            self.assertFalse(report['child_started'])
            self.assertEqual(facts['events'], [])

    def test_busy_lock_does_not_consume_attempt_or_spawn(self):
        with fake_run(lock_busy=True) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['reason'], 'serial_lock_busy')
            self.assertFalse(report['attempt_consumed'])
            self.assertFalse(facts['spawned'])

    def test_unknown_child_field_and_raw_stderr_never_reach_receipt(self):
        unsafe = {**child_report(), 'private': DUMMY}
        with fake_run(result=unsafe) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['reason'], 'worker_result_invalid')
            self.assertIsNone(report['worker'])
            self.assertNotIn(DUMMY, (facts['evidence'] / 'live-attempt/supervisor.json').read_text())

    def test_combined_output_cap_stops_without_retry(self):
        with fake_run(chunks=[b'x' * (matrix.MAX_OUTPUT + 1)]) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['reason'], 'worker_output_limit')
            self.assertEqual(len(facts['spawned']), 1)
            self.assertTrue(report['cleanup_verified'])

    def test_timeout_uses_original_deadline_and_deletes_transient_after_cleanup(self):
        with fake_run(clock=[100, 100, 100, 136, 136]) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['reason'], 'worker_timeout')
            self.assertTrue(report['worker_timed_out'])
            self.assertEqual(facts['cleanup_deadlines'], [149.5])
            self.assertTrue(report['transient_removed'])
            self.assertTrue((facts['evidence'] / 'live-attempt').is_dir())

    def test_pending_cancel_during_spawn_delivered_after_child_assignment(self):
        with fake_run(cancel_during_spawn=True) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['reason'], 'supervisor_interrupted')
            self.assertTrue(report['child_started'])
            self.assertEqual(facts['cleanup_children'], [facts['child']])
            self.assertEqual(facts['events'], ['lock', 'spawn', 'cleanup', 'unlock'])
            self.assertTrue(report['transient_removed'])

    def test_cleanup_failure_overrides_success_and_does_not_unlock_or_delete(self):
        with fake_run(result=child_report(passed=True), clean=False) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['reason'], 'cleanup_unverified')
            self.assertFalse(report['lock_released'])
            self.assertFalse(report['transient_removed'])
            self.assertNotIn('unlock', facts['events'])
            self.assertEqual(matrix._LIVE_RETAINED_LOCKS, [900])

    def test_enabled_coredumps_prevent_spawn(self):
        with fake_run(core=(1, 1)) as facts:
            report = matrix.live_single(DUMMY, 90.0)
            self.assertEqual(report['reason'], 'core_dumps_not_disabled')
            self.assertFalse(facts['spawned'])

    def test_strict_parser_and_public_projector_reject_unknown_or_invalid_values(self):
        with self.assertRaises(ValueError):
            matrix._strict_live_json('{"format":1,"format":2}')
        with self.assertRaises(ValueError):
            matrix._strict_live_json('{"value":NaN}')
        report = matrix._empty_live_supervisor_report()
        self.assertIsNone(matrix.sanitize_live_supervisor_report({**report, 'private': DUMMY}))
        self.assertIsNone(matrix.sanitize_live_supervisor_report({**report, 'reason': DUMMY}))

    def test_existing_fixture_matrix_functions_are_unchanged(self):
        wanted = {
            'single': 'aa896d6d10ae91884eb2390e4672c65f76e38f0fae4f581addc1af175d844b7e',
            'main': '925d82c3abdf99fb45bf42040fe9f3f93155815db4b8a4378bdcec6385e918d5',
            'cleanup': 'ff319cae2ed45722ed67cfac6020973b63c325e6af1a2ff231944d69341c44b2',
            'establish_subreaper': '2a2be11586c69d8692526b9e86430602201e8b1da00dc606b3f6ae9423f42373',
            'child_environment': '4b37feb91daf1df0967c5ca94def34c6eb8e69e5bd22ab218d5a8a5ee6f3e3c1',
        }
        tree = ast.parse((HERE / 'collector_matrix.py').read_text())
        found = {node.name: hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
                 for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted}
        self.assertEqual(found, wanted)


if __name__ == '__main__':
    unittest.main()
