"""Diagnostic overload and failures cannot take execution authority or block it."""
import json
import sys
import threading
import unittest
from unittest.mock import patch

from . import passive_timing as timing


ENV = {'HL_TESTNET_TIMING_TELEMETRY': timing.OPT_IN,
       'RENDER_SERVICE_ID': timing.SERVICE,
       'HL_TESTNET_RUNTIME_MODE': timing.MODE}


class PassiveTimingTests(unittest.TestCase):
    def setUp(self):
        timing.stop()
        self.assertTrue(timing.set_recorder(None))
        self.recorders = []

    def tearDown(self):
        for recorder in self.recorders:
            recorder.stop()
        timing.stop()
        self.assertTrue(timing.set_recorder(None))

    def recorder(self, **kwargs):
        recorder = timing.Recorder(**kwargs)
        self.recorders.append(recorder)
        return recorder

    def exported(self, recorder, lines):
        recorder.start()
        recorder.stop(timeout=0.25)
        self.assertFalse(recorder.worker_alive())
        return [json.loads(line)['testnet_timing'] for line in lines]

    def test_disabled_does_no_clock_sink_serialization_or_thread_work(self):
        with patch.object(timing, '_clock', side_effect=AssertionError('CLOCK')), \
                patch.object(timing.json, 'dumps', side_effect=AssertionError('JSON')), \
                patch.object(timing.threading, 'Thread', side_effect=AssertionError('THREAD')):
            self.assertFalse(timing.record('event', status='OK'))
            self.assertIsNone(timing.stamp())
            self.assertFalse(timing.start({}))
            self.assertEqual(timing.health(), {'enabled': False})

    def test_only_exact_testnet_environment_can_activate(self):
        for changed in ({'HL_TESTNET_TIMING_TELEMETRY': 'true'},
                        {'RENDER_SERVICE_ID': 'other'},
                        {'HL_TESTNET_RUNTIME_MODE': 'mainnet'},
                        {'HL_TESTNET_TIMING_TELEMETRY': ''}):
            self.assertFalse(timing.start({**ENV, **changed}))
            self.assertFalse(timing.health()['enabled'])
        self.assertTrue(timing.start(ENV))
        self.assertIsNotNone(timing.stamp())
        self.assertTrue(timing.health()['enabled'])
        self.assertFalse(timing.start({}))
        self.assertFalse(timing.health()['enabled'])

    def test_two_accounts_share_bounded_queue_with_unique_sequences(self):
        lines = []
        recorder = self.recorder(capacity=256, sink=lines.append)
        barrier = threading.Barrier(3)
        def produce(role):
            barrier.wait()
            for i in range(100):
                recorder.record('notification', account_role=role, symbol='SOL',
                                order_id=i, exchange_at_ms=123)
        threads = [threading.Thread(target=produce, args=(role,))
                   for role in ('long_account', 'short_account')]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(1)
            self.assertFalse(thread.is_alive())
        events = self.exported(recorder, lines)
        self.assertEqual({e['account_role'] for e in events},
                         {'long_account', 'short_account'})
        self.assertEqual([e['sequence'] for e in events], list(range(1, len(events)+1)))
        self.assertEqual(len(events) + recorder.health()['dropped_busy'], 200)

    def test_contended_buffer_and_health_do_not_wait(self):
        recorder = self.recorder()
        finished = threading.Event()
        result = []
        def produce():
            result.append(recorder.record('notification'))
            result.append(recorder.health())
            finished.set()
        with recorder._lock:
            thread = threading.Thread(target=produce)
            thread.start()
            self.assertTrue(finished.wait(0.5))
        thread.join(0.5)
        self.assertFalse(result[0])
        self.assertTrue(result[1]['busy'])
        self.assertEqual(result[1]['dropped_busy'], 1)

    def test_full_buffer_drops_without_eviction_or_synchronous_export(self):
        lines = []
        recorder = self.recorder(capacity=2, sink=lines.append)
        with patch.object(timing.json, 'dumps', side_effect=AssertionError('ON_PRODUCER')):
            self.assertTrue(recorder.record('event', order_id=1))
            self.assertTrue(recorder.record('event', order_id=2))
            self.assertFalse(recorder.record('event', order_id=3))
        self.assertEqual(lines, [])
        self.assertEqual(recorder.health()['buffered'], 2)
        self.assertEqual(recorder.health()['dropped_full'], 1)
        self.assertEqual([e['order_id'] for e in self.exported(recorder, lines)], [1, 2])

    def test_raising_sink_is_counted_and_writer_continues(self):
        def fail(line):
            raise RuntimeError('SINK_FAILED')
        recorder = self.recorder(sink=fail)
        for i in range(3):
            self.assertTrue(recorder.record('event', order_id=i))
        self.exported(recorder, [])
        self.assertEqual(recorder.health()['sink_errors'], 3)
        self.assertEqual(recorder.health()['buffered'], 0)

    def test_default_archive_never_serializes_or_touches_shared_stdout(self):
        recorder = self.recorder()
        with patch.object(sys, 'stdout') as stdout, \
                patch.object(timing.json, 'dumps', side_effect=AssertionError('DEFAULT_IO')):
            self.assertTrue(recorder.record('notification_received', symbol='SOL'))
            self.exported(recorder, [])
            stdout.write.assert_not_called()
            stdout.flush.assert_not_called()
        health = recorder.health()
        self.assertEqual(health['exported'], 1)
        self.assertEqual(health['sink_errors'], 0)
        self.assertEqual(health['recent_events'][0]['symbol'], 'SOL')

    def test_health_archive_copy_cannot_mutate_internal_events(self):
        recorder = self.recorder()
        recorder.record('trade_observed', card_id='a'*64, account_role='long_account',
                        active_order_ids=(1, 2), issues=('KNOWN_CODE',))
        self.exported(recorder, [])
        snapshot = recorder.health()
        snapshot['recent_events'][0]['card_id'] = 'changed'
        snapshot['recent_events'][0]['active_order_ids'] += (3,)
        snapshot['recent_events'].clear()
        event, = recorder.health()['recent_events']
        self.assertEqual(event['card_id'], 'a'*64)
        self.assertEqual(event['active_order_ids'], (1, 2))
        self.assertEqual(event['issues'], ('KNOWN_CODE',))

    def test_public_module_health_excludes_all_event_payloads_without_archive_lock(self):
        recorder = self.recorder()
        self.assertTrue(timing.set_recorder(recorder))
        recorder.record('trade_observed', account_role='long_account', card_id='a'*64,
                        symbol='SOL', entry_order_ids=('secret-trade-identifier',))
        self.exported(recorder, [])
        with patch.object(recorder, '_archive_lock') as archive_lock:
            public = timing.health()
            archive_lock.acquire.assert_not_called()
        self.assertNotIn('recent_events', public)
        self.assertNotIn('recent_busy', public)
        self.assertNotIn('recent_capacity', public)
        public_text = json.dumps(public)
        for sensitive in ('long_account', 'SOL', 'a'*64, 'secret-trade-identifier',
                          'trade_observed', 'entry_order_ids'):
            self.assertNotIn(sensitive, public_text)
        detailed = timing.health(include_recent=True)
        self.assertEqual(detailed['recent_events'][0]['card_id'], 'a'*64)
        detailed['recent_events'][0]['symbol'] = 'CHANGED'
        self.assertEqual(timing.health(include_recent=True)['recent_events'][0]['symbol'], 'SOL')

    def test_archive_retains_fixed_number_of_recent_events(self):
        recorder = self.recorder(capacity=128)
        for i in range(timing.MAX_RECENT + 10):
            self.assertTrue(recorder.record('notification_received', order_id=i))
        self.exported(recorder, [])
        events = recorder.health()['recent_events']
        self.assertEqual(len(events), timing.MAX_RECENT)
        self.assertEqual(events[0]['order_id'], 10)
        self.assertEqual(events[-1]['order_id'], timing.MAX_RECENT + 9)
        self.assertEqual(recorder.health()['archive_evicted'], 10)

    def test_unchanged_trade_observations_deduplicate_only_in_worker(self):
        recorder = self.recorder()
        original = dict(account_role='long_account', card_id='a'*64,
                        evidence_at_ms=100, remaining_quantity='1.0',
                        verification_status='VERIFIED')
        for _ in range(10):
            self.assertTrue(recorder.record('trade_observed', **original))
        self.assertEqual(recorder.health()['accepted'], 10)
        self.assertEqual(recorder.health()['deduplicated'], 0)
        for change in ({'evidence_at_ms': 101}, {'remaining_quantity': '0.5'},
                       {'verification_status': 'REFRESHING'},
                       {'account_role': 'short_account'}):
            recorder.record('trade_observed', **{**original, **change})
        self.exported(recorder, [])
        health = recorder.health()
        self.assertEqual(len(health['recent_events']), 5)
        self.assertEqual(health['deduplicated'], 9)

    def test_dedupe_keys_are_bounded_and_diagnostics_never_wait_for_archive(self):
        recorder = self.recorder(capacity=128)
        for i in range(timing.MAX_DEDUPE + 10):
            recorder.record('trade_observed', card_id=str(i), evidence_at_ms=100)
        self.exported(recorder, [])
        self.assertEqual(len(recorder._dedupe), timing.MAX_DEDUPE)
        with recorder._archive_lock:
            result = recorder.health()
        self.assertTrue(result['recent_busy'])
        self.assertEqual(result['recent_events'], [])

    def test_hung_sink_bounds_memory_and_cannot_spawn_replacement_threads(self):
        entered, release, produced = threading.Event(), threading.Event(), threading.Event()
        def hang(line):
            entered.set()
            release.wait()
        recorder = self.recorder(capacity=3, batch_size=1, sink=hang)
        self.assertTrue(timing.set_recorder(recorder))
        recorder.record('event', order_id=0)
        recorder.start()
        self.assertTrue(entered.wait(0.5))
        thread = None
        try:
            def produce():
                for i in range(1000):
                    timing.record('event', order_id=i)
                produced.set()
            thread = threading.Thread(target=produce)
            thread.start()
            self.assertTrue(produced.wait(0.5))
            health = recorder.health()
            self.assertEqual(health['buffered'], 3)
            self.assertEqual(health['in_flight'], 1)
            self.assertEqual(health['dropped_full'], 997)
            original_thread = recorder._thread
            self.assertFalse(recorder.stop(timeout=0))
            for _ in range(10):
                self.assertFalse(timing.start(ENV))
                self.assertFalse(timing.set_recorder(self.recorder()))
            self.assertIs(recorder._thread, original_thread)
        finally:
            release.set()
            recorder.stop(timeout=0.25)
            if thread is not None:
                thread.join(0.5)

    def test_payload_is_immutable_and_rejects_nested_or_sensitive_data(self):
        lines = []
        recorder = self.recorder(sink=lines.append)
        fields = {'symbol': 'SOL', 'active_order_ids': (1, 2)}
        self.assertTrue(recorder.record('event', **fields))
        fields['symbol'] = 'DOGE'
        fields['active_order_ids'] = (9,)
        for payload in ({'private_key': 'secret'}, {'snapshot': {'x': 1}},
                        {'issues': ['mutable']}, {'issues': ({'x': 1},)},
                        {'reason': 'secret content with spaces'},
                        {'status': 'x' * 193}, {'issues': tuple('x' for _ in range(17))},
                        {'quantity': float('nan')}, {'quantity': 2**64}):
            self.assertFalse(recorder.record('event', **payload))
        events = self.exported(recorder, lines)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['symbol'], 'SOL')
        self.assertEqual(events[0]['active_order_ids'], [1, 2])
        self.assertEqual(recorder.health()['dropped_invalid'], 9)

    def test_total_text_and_field_caps_reject_unbounded_payloads(self):
        recorder = self.recorder()
        self.assertFalse(recorder.record('event', issues=tuple('x'*192 for _ in range(16))))
        self.assertFalse(recorder.record('event', **{key: 0 for key in timing.FIELDS}))
        self.assertFalse(recorder.record('E' * 100, status='OK'))
        self.assertEqual(recorder.health()['dropped_invalid'], 3)

    def test_restart_session_and_local_clocks_are_explicit(self):
        sessions = []
        for _ in range(2):
            lines = []
            recorder = self.recorder(sink=lines.append)
            self.assertTrue(timing.set_recorder(recorder))
            mark = timing.stamp()
            self.assertIsNotNone(mark)
            self.assertTrue(timing.record('cycle_completed', at_ms=mark[0], mono_ns=mark[1],
                                          bucket='a'*64, duration_ns=12))
            event, = self.exported(recorder, lines)
            self.assertEqual((event['at_ms'], event['mono_ns']), mark)
            self.assertEqual(event['sequence'], 1)
            self.assertEqual(event['duration_ns'], 12)
            sessions.append(event['session_id'])
        self.assertNotEqual(*sessions)

    def test_real_hook_event_fields_survive_validation_and_export(self):
        lines = []
        recorder = self.recorder(sink=lines.append)
        cases = [
            ('notification_received', dict(account_role='long_account', symbol='SOL',
                generation=2, revision=3, notification_type='userFills',
                exchange_at_ms=123, batch_count=2, captured_count=1,
                fill_id='789', order_id='456')),
            ('notification_received', dict(account_role='short_account', symbol='DOGE',
                generation=2, revision=4, notification_type='orderUpdates',
                exchange_at_ms=124, batch_count=1, captured_count=1,
                status='filled', order_id='456')),
            ('stream_result', dict(account_role='long_account', status='BLOCKED',
                failure_code='BUDGET_DEFERRED', symbol='SOL', retry_after_ms=1000,
                budget_stage='entry', order_requests_sent=0)),
            ('connection_gap', dict(account_role='short_account', generation=2,
                reason='STREAM_DISCONNECTED')),
            ('refresh_completed', dict(bucket='a'*64, status='RETURNED',
                failure_code=None, revision=4, evidence_at_ms=123, duration_ns=450)),
            ('cycle_completed', dict(bucket='a'*64, status='NO_ACTION_NEEDED',
                request_id=None, failure_code=None, order_requests_sent=0,
                duration_ns=500)),
            ('trade_observed', dict(account_role='short_account', card_id='b'*64,
                bucket='a'*64, revision=4, entry_order_ids=('789',),
                symbol='DOGE', state='OPEN', entry_quantity='5.52', exit_quantity='0',
                remaining_quantity='5.52', first_entry_at_ms=123, last_entry_at_ms=123,
                last_exit_at_ms=None, evidence_at_ms=124, protection_verified=True,
                closure_verified=False, verification_status='VERIFIED',
                stop_quantity='5.52', take_profit_quantity='5.52',
                stop_public_status_at_ms=124, stop_observed_at_ms=125,
                active_order_ids=('123', '456'), issues=())),
        ]
        for kind, fields in cases:
            with self.subTest(kind=kind, fields=fields):
                self.assertTrue(recorder.record(kind, at_ms=200, mono_ns=300, **fields))
        events = self.exported(recorder, lines)
        self.assertEqual(len(events), len(cases))
        self.assertEqual(recorder.health()['dropped_invalid'], 0)
        for event, (kind, fields) in zip(events, cases):
            self.assertEqual(event['kind'], kind)
            for name, value in fields.items():
                self.assertEqual(event[name], list(value) if type(value) is tuple else value)

    def test_invalid_clock_and_internal_ordinary_failure_are_fail_open(self):
        recorder = self.recorder()
        self.assertTrue(timing.set_recorder(recorder))
        self.assertFalse(timing.record('event', at_ms=1))
        with patch.object(timing, '_clock', side_effect=RuntimeError('CLOCK_FAILED')):
            self.assertIsNone(timing.stamp())
            self.assertFalse(timing.record('event'))
        with patch.object(recorder, 'record', side_effect=RuntimeError('FAULT')):
            self.assertFalse(timing.record('event'))
        with patch.object(recorder, 'health', side_effect=RuntimeError('FAULT')):
            self.assertEqual(timing.health(), {'enabled': False, 'health_error': True})
        self.assertEqual(recorder.health()['record_errors'], 1)


if __name__ == '__main__':
    unittest.main()
