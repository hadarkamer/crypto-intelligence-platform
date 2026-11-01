"""Extracted-loop ownership regressions; no producer imports, threads or GC.

The original loops are embedded from the exact 18ab5346 baseline. Fake queue
checks run inside the second get, before the caller's frame can unwind.
Artificial payloads/witnesses are never used to estimate live memory bytes.
"""
import ast
from copy import deepcopy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
import weakref

ROOT = Path(__file__).resolve().parent
BASELINE = {'research_alert_decision_store.py': {'baseline_source_canonical_sha256': 'f469643938d9e4e39d0d55a1289ab7415a57bce02e9636485580288719cca9b5',
                                      'persist_ast_sha256': '64fa01f27be8e9c19864cda498983b252cc06f62241a5a83054a57d6835e27a6',
                                      'run_ast_sha256': '2d330a8297a5ee121b83b89e6765e91963e987d0fe3234147f158e35b09bf878',
                                      'run_text': 'def _run():\n'
                                                  '    while True:\n'
                                                  '        prepared, enqueued_at = _QUEUE.get()\n'
                                                  '        try:\n'
                                                  '            _persist_one(prepared,enqueued_at)\n'
                                                  '        finally:\n'
                                                  '            _QUEUE.task_done()\n'},
 'research_combined_liquidity_shadow.py': {'baseline_source_canonical_sha256': '01c65aa397e7dc0af0c76b1fccb75971adb6c3023a51f3fa1d9caee052eab65f',
                                           'persist_ast_sha256': 'b89fbd5da9aa44606cac8554422e332fb5eab3cb05ff8492e844f89e5f158fde',
                                           'run_ast_sha256': 'db634cb9e6f025acb43173f4485b543eb54327071cb16edc44e5b74c1dfe6d2f',
                                           'run_text': 'def _run():\n'
                                                       '    while True:\n'
                                                       '        obs, size = _QUEUE.get()\n'
                                                       '        _persist_one(obs, size)\n'
                                                       '        _QUEUE.task_done()\n'},
 'research_doge_sequence_capture.py': {'baseline_source_canonical_sha256': '8844c211e5f933535720346ccdd353f584ee10148ddcf4cb66ebc67adcaa786d',
                                       'persist_ast_sha256': 'f22c6da934898e73a2fe358e9f78041202dc01bb15c719598cab3c80be5a91ad',
                                       'run_ast_sha256': 'aa08a7a6b87a75b14a7ac51e19464af5764bd57df4ee65377e419181da57ed1e',
                                       'run_text': 'def _run():\n'
                                                   '    while True:\n'
                                                   '        observation, size = _QUEUE.get()\n'
                                                   '        _persist_one(observation, size)\n'
                                                   '        _QUEUE.task_done()\n'}}
FILES = (
    ('research_doge_sequence_capture.py', 'observation', 'size'),
    ('research_combined_liquidity_shadow.py', 'obs', 'size'),
    ('research_alert_decision_store.py', 'prepared', 'enqueued_at'),
)


def dump(node):
    return ast.dump(node, include_attributes=False)


def function(text, name):
    return next(node for node in ast.parse(text).body
                if isinstance(node, ast.FunctionDef) and node.name == name)


def current(filename, name='_run'):
    return function((ROOT / filename).read_text(encoding='utf-8'), name)


class StopFixture(BaseException):
    pass


class Witness:
    pass


class DiagnosticFault(BaseException):
    pass


class Lock:
    def __init__(self, events, fail_after_ack=False):
        self.events, self.fail_after_ack = events, fail_after_ack

    def __enter__(self):
        if self.fail_after_ack and 'ACK' in self.events:
            raise DiagnosticFault()

    def __exit__(self, *args):
        return False


class Metrics(dict):
    def __init__(self, *args, fault=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fault = fault

    def get(self, key, default=None):
        if self.fault == 'get' and key == 'worker_payload_releases':
            raise DiagnosticFault()
        return super().get(key, default)

    def __setitem__(self, key, value):
        if self.fault == key:
            raise DiagnosticFault()
        return super().__setitem__(key, value)


class Queue:
    def __init__(self, first, events, on_second, ack_error=False, ack_exception=None):
        self.first, self.events, self.on_second = first, events, on_second
        self.ack_error = ack_error
        self.ack_exception = ack_exception
        self.gets = 0

    def get(self):
        self.gets += 1
        if self.gets == 1:
            item = self.first
            self.first = None
            return item
        self.events.append('SECOND_GET')
        self.on_second()
        raise StopFixture()

    def task_done(self):
        self.events.append('ACK')
        if self.ack_exception is not None:
            raise self.ack_exception
        if self.ack_error:
            raise RuntimeError('ACK_FAULT')


def make_payload(filename, size):
    if 'doge_sequence' in filename:
        source = ast.parse((ROOT / filename).read_text(encoding='utf-8'))
        original = next(n for n in source.body if isinstance(n, ast.ClassDef) and n.name == 'Observation')
        init = next(n for n in original.body if isinstance(n, ast.FunctionDef) and n.name == '__init__')
        scope = {'graph': SimpleNamespace(Collector=lambda **kw: SimpleNamespace(objects={}, events=[], errors=[])),
                 'MAX_RAW_BYTES': 67108864, 'MAX_COMPRESSED_BYTES': 16777216,
                 'uuid4': lambda: 'fixture', 'utc_now': lambda: '2000-01-01T00:00:00+00:00'}
        cls = ast.ClassDef(name='Observation', bases=[], keywords=[], body=[init], decorator_list=[])
        exec(compile(ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])), '<exact-observation-init>', 'exec'), scope)
        value = scope['Observation']()
        return value, weakref.ref(value)
    marker = Witness()
    # This child exists only in the fixture, keeping the envelope exact dict.
    value = {'_fixture_lifetime_witness': marker, 'observation_id': 'fixture',
             'manifest': {'capture_status': 'CAPTURED', 'event_fingerprint': 'fixture'},
             'cycle_id': 'fixture-cycle', 'payload_json': '{"fixture":true}',
             'payload_bytes': size}
    return value, weakref.ref(marker)


def exercise(filename, *, original=False, alias=False, real_persist=False,
             store_failure=False, persist_error=False, ack_error=False,
             malformed=False, fault=None, size=137, missing_size=False,
             persist_exception=None, ack_exception=None):
    events, observed = [], {}
    payload, witness = make_payload(filename, size)
    payload_id = id(payload)
    external_alias = payload if alias else None
    fingerprint = tuple((key, id(value)) for key, value in payload.items()) if type(payload) is dict else None
    if missing_size:
        del payload['payload_bytes']
        fingerprint = tuple((key, id(value)) for key, value in payload.items())
    auxiliary = 'fixture-enqueue-clock' if 'decision_store' in filename else size
    first = (payload, auxiliary, 'bad-unpack') if malformed else (payload, auxiliary)
    metrics = Metrics(dict(queued_items=1, queued_bytes=size if type(size) is int else 0,
                           persisted=0, partial=0, failed=0, linked=0, unlinked=0,
                           link_failures=0, idempotent_existing=0), fault=fault)
    def on_second():
        observed['alive_inside_second_get'] = witness() is not None
        observed['metrics'] = dict(metrics)
        observed['queue_dropped_payload'] = queue.first is None
        observed['events'] = list(events)
    queue = Queue(first, events, on_second, ack_error=ack_error, ack_exception=ack_exception)
    del payload, first
    def stub_persist(value, auxiliary):
        assert id(value) == payload_id
        events.append('PERSIST')
        if persist_exception is not None:
            raise persist_exception
        if persist_error:
            raise RuntimeError('PERSIST_FAULT')
    def failure(*args, **kwargs):
        metrics['failed'] += 1
        events.append('HANDLED_FAILURE')
    class Store:
        def persist(self, value):
            events.append('STORE')
            if store_failure:
                raise ValueError('CONTROLLED_STORE_FAILURE')
            return {'persisted': True}
        def persist_prepared(self, value, clock):
            assert id(value) == payload_id
            events.append('STORE')
            if store_failure:
                raise ValueError('CONTROLLED_STORE_FAILURE')
            return {'idempotent_existing': False}
        def find_source(self, fingerprint):
            return None
        def persist_link(self, receipt):
            events.append('LINK')
    scope = {'_QUEUE': queue, '_LOCK': Lock(events, fail_after_ack=fault == 'lock'),
             '_METRICS': metrics, '_STORE': Store(), '_persist_one': stub_persist,
             '_failure': failure, '_error_code': lambda e: type(e).__name__,
             'utc_now': lambda: 'fixture-clock', '_metric_clock': lambda: 'fixture-clock',
             'MAX_QUEUE_BYTES': 33554432 if 'doge_sequence' in filename else 8388608,
             'MAX_CAPSULE_BYTES': 262144, 'LINK_ATTEMPTS': 1,
             'link_receipt': lambda *a, **k: {'link_status': 'SOURCE_UNAVAILABLE', 'event_id': None},
             'time': SimpleNamespace(sleep=lambda n: (_ for _ in ()).throw(AssertionError('UNEXPECTED_SLEEP'))),
             'prepare': lambda value: {'manifest': {'summary': {'status': 'CAPTURED'}}}}
    nodes = [function(BASELINE[filename]['run_text'], '_run') if original else current(filename)]
    if real_persist:
        nodes.insert(0, current(filename, '_persist_one'))
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), '<exact-writer-loops>', 'exec'), scope)
    error = None
    caught_exception = None
    try:
        scope['_run']()
    except StopFixture:
        pass
    except BaseException as exc:
        error = type(exc).__name__ + ':' + str(exc)
        caught_exception = exc
        # Do not let this test harness become an extra traceback payload owner.
        exc.__traceback__ = None
    if alias:
        if type(external_alias) is dict:
            assert tuple((key, id(value)) for key, value in external_alias.items()) == fingerprint
        assert witness() is not None
    external_alias = None
    observed.update(error=error, caught_exception=caught_exception, events=events, final_metrics=dict(metrics),
                    released_after_all_fixture_owners=not bool(witness()),
                    remaining_queue_reference=queue.first is not None)
    return observed


class LifetimeTests(unittest.TestCase):
    def all_cases(self, **kwargs):
        return [exercise(name, **kwargs) for name, _, _ in FILES]

    def test_embedded_original_ast_pins(self):
        for filename, _, _ in FILES:
            self.assertEqual(hashlib.sha256(dump(function(BASELINE[filename]['run_text'], '_run')).encode()).hexdigest(), BASELINE[filename]['run_ast_sha256'])

    def test_current_loop_prefix_is_exact_original(self):
        for filename, _, _ in FILES:
            old = function(BASELINE[filename]['run_text'], '_run')
            fixed = deepcopy(current(filename))
            original_length = len(old.body[0].body)
            self.assertEqual(dump(ast.Module(body=fixed.body[0].body[:original_length], type_ignores=[])), dump(ast.Module(body=old.body[0].body, type_ignores=[])))
            fixed.body[0].body = fixed.body[0].body[:original_length]
            self.assertEqual(dump(fixed), dump(old))

    def test_persistence_functions_unchanged(self):
        for filename, _, _ in FILES:
            self.assertEqual(hashlib.sha256(dump(current(filename, '_persist_one')).encode()).hexdigest(), BASELINE[filename]['persist_ast_sha256'])

    def test_only_expected_post_ack_work(self):
        for filename, local, auxiliary in FILES:
            fn = current(filename)
            old = function(BASELINE[filename]['run_text'], '_run')
            tail = fn.body[0].body[len(old.body[0].body):]
            deletes = [n for n in tail if isinstance(n, ast.Delete)]
            self.assertEqual([[n.id for n in part.targets] for part in deletes], [[local, auxiliary]])
            calls = [n for part in tail for n in ast.walk(part) if isinstance(n, ast.Call)]
            self.assertTrue(all(isinstance(n.func, ast.Name) and n.func.id == 'type' or isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name) and n.func.value.id == '_METRICS' and n.func.attr == 'get' for n in calls))
            self.assertTrue(any(isinstance(n, ast.Try) and any(isinstance(h.type, ast.Name) and h.type.id == 'BaseException' for h in n.handlers) for n in tail))

    def test_doge_original_holds(self): self.assertTrue(exercise(FILES[0][0], original=True)['alive_inside_second_get'])
    def test_combined_original_holds(self): self.assertTrue(exercise(FILES[1][0], original=True)['alive_inside_second_get'])
    def test_decision_original_holds(self): self.assertTrue(exercise(FILES[2][0], original=True)['alive_inside_second_get'])
    def test_doge_fixed_releases(self): self.assertFalse(exercise(FILES[0][0])['alive_inside_second_get'])
    def test_combined_fixed_releases(self): self.assertFalse(exercise(FILES[1][0])['alive_inside_second_get'])
    def test_decision_fixed_releases(self): self.assertFalse(exercise(FILES[2][0])['alive_inside_second_get'])

    def test_shared_external_alias_still_retains(self):
        for result in self.all_cases(alias=True):
            self.assertTrue(result['alive_inside_second_get'])
            self.assertTrue(result['released_after_all_fixture_owners'])

    def test_existing_ack_precedes_release_witness(self):
        for result in self.all_cases():
            self.assertEqual(result['events'], ['PERSIST', 'ACK', 'SECOND_GET'])
            self.assertEqual(result['metrics']['worker_payload_releases'], 1)

    def test_actual_persist_success_releases(self):
        for result in self.all_cases(real_persist=True):
            self.assertIsNone(result['error'])
            self.assertFalse(result['alive_inside_second_get'])
            self.assertEqual(result['metrics']['queued_items'], 0)
            self.assertEqual(result['metrics']['queued_bytes'], 0)
            self.assertEqual(result['metrics']['persisted'], 1)

    def test_actual_handled_persist_failure_still_releases(self):
        for result in self.all_cases(real_persist=True, store_failure=True):
            self.assertIsNone(result['error'])
            self.assertFalse(result['alive_inside_second_get'])
            self.assertEqual(result['metrics']['failed'], 1)
            self.assertEqual(result['metrics']['queued_items'], 0)
            self.assertEqual(result['metrics']['worker_payload_releases'], 1)

    def test_unexpected_persist_exception_propagation_unchanged(self):
        for filename, _, _ in FILES:
            old, fixed = (exercise(filename, original=value, persist_error=True) for value in (True, False))
            self.assertEqual(old['error'], fixed['error'])
            self.assertEqual(old['events'], fixed['events'])
            self.assertNotIn('worker_payload_releases', fixed['final_metrics'])

    def test_ack_exception_propagation_unchanged(self):
        for filename, _, _ in FILES:
            old, fixed = (exercise(filename, original=value, ack_error=True) for value in (True, False))
            self.assertEqual(old['error'], fixed['error'])
            self.assertEqual(old['events'], fixed['events'])
            self.assertNotIn('worker_payload_releases', fixed['final_metrics'])

    def test_native_persist_abort_identity_and_ack_order(self):
        for filename, _, _ in FILES:
            abort = BaseException('NATIVE_PERSIST_ABORT')
            old, fixed = (exercise(filename, original=value, persist_exception=abort) for value in (True, False))
            self.assertIs(old['caught_exception'], abort)
            self.assertIs(fixed['caught_exception'], abort)
            expected = ['PERSIST', 'ACK'] if 'decision_store' in filename else ['PERSIST']
            self.assertEqual(old['events'], expected)
            self.assertEqual(fixed['events'], expected)
            self.assertNotIn('worker_payload_releases', fixed['final_metrics'])

    def test_native_ack_abort_identity_and_order(self):
        for filename, _, _ in FILES:
            abort = BaseException('NATIVE_ACK_ABORT')
            old, fixed = (exercise(filename, original=value, ack_exception=abort) for value in (True, False))
            self.assertIs(old['caught_exception'], abort)
            self.assertIs(fixed['caught_exception'], abort)
            self.assertEqual(old['events'], ['PERSIST', 'ACK'])
            self.assertEqual(fixed['events'], ['PERSIST', 'ACK'])
            self.assertNotIn('worker_payload_releases', fixed['final_metrics'])

    def test_malformed_queue_unpack_unchanged(self):
        for filename, _, _ in FILES:
            old, fixed = (exercise(filename, original=value, malformed=True) for value in (True, False))
            self.assertEqual(old['error'], fixed['error'])
            self.assertEqual(old['events'], fixed['events'])
            self.assertEqual(fixed['events'], [])

    def test_post_ack_diagnostic_lock_fault_cannot_retain_or_stop(self):
        for result in self.all_cases(fault='lock'):
            self.assertIsNone(result['error'])
            self.assertFalse(result['alive_inside_second_get'])

    def test_post_ack_metric_get_fault_cannot_retain_or_stop(self):
        for result in self.all_cases(fault='get'):
            self.assertIsNone(result['error'])
            self.assertFalse(result['alive_inside_second_get'])

    def test_post_ack_metric_write_fault_cannot_retain_or_stop(self):
        for key in ('worker_payload_releases', 'last_worker_payload_accounted_bytes'):
            for result in self.all_cases(fault=key):
                self.assertIsNone(result['error'])
                self.assertFalse(result['alive_inside_second_get'])

    def test_unknown_byte_annotations_are_none(self):
        for value in (True, -1, 1.5, '137', None):
            for result in self.all_cases(size=value):
                self.assertIsNone(result['error'])
                self.assertIsNone(result['metrics']['last_worker_payload_accounted_bytes'])
                self.assertFalse(result['alive_inside_second_get'])

    def test_zero_and_upper_bound_integer_annotation(self):
        for filename, _, _ in FILES:
            bound = 262144 if 'decision_store' in filename else 33554432 if 'doge_sequence' in filename else 8388608
            for size in (0, bound):
                self.assertEqual(exercise(filename, size=size)['metrics']['last_worker_payload_accounted_bytes'], size)
            self.assertIsNone(exercise(filename, size=bound + 1)['metrics']['last_worker_payload_accounted_bytes'])

    def test_decision_missing_optional_annotation_does_not_stop(self):
        result = exercise(FILES[2][0], missing_size=True)
        self.assertIsNone(result['error'])
        self.assertIsNone(result['metrics']['last_worker_payload_accounted_bytes'])
        self.assertFalse(result['alive_inside_second_get'])

    def test_payload_identity_contents_and_existing_units_preserved(self):
        for result in self.all_cases(alias=True):
            self.assertEqual(result['metrics']['last_worker_payload_accounted_bytes'], 137)
            self.assertTrue(result['queue_dropped_payload'])
            self.assertTrue(result['alive_inside_second_get'])

    def test_new_metrics_initialization_and_scope_labels(self):
        for filename, _, _ in FILES:
            tree = ast.parse((ROOT / filename).read_text(encoding='utf-8'))
            assignment = next(n for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '_METRICS' for t in n.targets))
            fields = {k.arg: ast.literal_eval(k.value) for k in assignment.value.keywords}
            self.assertEqual(fields['worker_payload_releases'], 0)
            self.assertIsNone(fields['last_worker_payload_accounted_bytes'])
            status = dump(next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'status'))
            self.assertIn('NOT_HEAP_OR_RSS_RECLAIMED', status)
            self.assertIn('OTHER_OWNERS_MAY_RETAIN_PAYLOAD', status)
            self.assertIn('CANONICAL_JSON_BYTES' if 'decision_store' in filename else 'QUEUE_ACCOUNTING', status)


if __name__ == '__main__':
    unittest.main()
