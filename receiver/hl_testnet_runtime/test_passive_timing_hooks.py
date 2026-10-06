"""Offline production-hook parity: diagnostics never become trading work."""
from contextlib import contextmanager
from copy import deepcopy
import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

from . import fill_wakeups as wake, filled_quantity_dispatch as dispatch
from . import long_stream_runtime as stream, passive_timing as timing
from .postgres_journal import PostgresJournal
from .test_card_lifecycle import T
from .test_fill_wakeups import A, B, ROUTES, Clock, fill, fills, order
from .test_filled_quantity_dispatch import NoExternal, state_from_case


class PassiveTimingHookTests(NoExternal):
    def setUp(self):
        super().setUp()
        timing.stop()
        self.assertTrue(timing.set_recorder(None))
        self.recorders = []

    def tearDown(self):
        for recorder in self.recorders:
            recorder.stop(timeout=0.5)
        timing.stop()
        self.assertTrue(timing.set_recorder(None))

    def recorder(self, **kwargs):
        lines = []
        recorder = timing.Recorder(sink=kwargs.pop('sink', lines.append), **kwargs)
        self.recorders.append(recorder)
        self.assertTrue(timing.set_recorder(recorder))
        return recorder, lines

    def exported(self, recorder, lines):
        self.assertTrue(recorder.start())
        self.assertTrue(recorder.stop(timeout=0.5))
        self.assertFalse(recorder.worker_alive())
        return [json.loads(line)['testnet_timing'] for line in lines]

    def ready_feed(self):
        feed = wake.FillWakeups(ROUTES, clock=Clock())
        for account in (A, B):
            generation = feed._opened(account)
            for channel in sorted(wake.CHANNELS):
                message = dict(channel='subscriptionResponse', data=dict(
                    method='subscribe', subscription=dict(type=channel, user=account)))
                self.assertTrue(feed._receive(account, generation, json.dumps(message)))
            self.assertTrue(feed._receive(account, generation,
                json.dumps(fills(account, snapshot=True, rows=[]))))
            self.assertTrue(feed.finish_reconciliation(
                feed.begin_reconciliation(account), complete=True))
        feed.wake_event.clear()
        return feed

    def send(self, feed, message, account=A, generation=None):
        if generation is None:
            generation = feed._states[account]['generation']
        return feed._receive(account, generation, json.dumps(message))

    def trade_fixture(self, side='LONG'):
        state = state_from_case(q='40', stop='40', take='40', side=side,
                                expiry_seconds=600)
        snapshot = state['evidence']['snapshot']
        first = snapshot['fills'][0]
        first['quantity'] = '20'
        snapshot['fills'].append({**first, 'fill_id': 'later-fill', 'at_ms': T-1000})
        cid = state['bindings'][0]['card_id']
        state['protection_timing'] = {cid: dict(stop_public_status_at_ms=T-500,
                                               stop_observed_at_ms=T-100)}
        return state

    def projection(self, state, *, historical=False):
        store = Mock(spec=['for_account', 'load'])
        store.for_account.return_value = [state]
        store.load.side_effect = AssertionError('NO_NEW_STORE_READ')
        venue = Mock(spec=['now'])
        venue.now.return_value = T+1
        controller = SimpleNamespace(store=store, venue=venue)
        role = 'long_account' if state['account'] == A else 'short_account'
        result = stream.observed_trades(controller, {'account': state['account']},
                                        role=role, historical=historical)
        return result, store.method_calls, venue.method_calls

    def test_live_fill_and_order_export_identity_time_and_preserve_feed_state(self):
        disabled = self.ready_feed()
        enabled = self.ready_feed()
        messages = [(A, fills(rows=[fill('SOL', tid=41, oid=71, at=1200)])),
                    (B, order('DOGE', oid=82, at=1300, status='canceled'))]
        with patch.object(timing, '_clock', side_effect=AssertionError('DISABLED_CLOCK')):
            baseline = [self.send(disabled, message, account) for account, message in messages]
        recorder, lines = self.recorder()
        with patch.object(timing, '_clock', side_effect=[(2000, 100), (2100, 200)]):
            measured = [self.send(enabled, message, account) for account, message in messages]
        self.assertEqual(measured, baseline)
        self.assertEqual(enabled._states, disabled._states)
        self.assertEqual(enabled.health(), disabled.health())
        self.assertEqual(enabled.wake_event.is_set(), disabled.wake_event.is_set())
        events = self.exported(recorder, lines)
        self.assertEqual(len(events), 2)
        self.assertEqual([(e['kind'], e['account_role'], e['symbol'], e['at_ms'],
                           e['mono_ns'], e['exchange_at_ms'], e['order_id']) for e in events],
            [('notification_received', 'long_account', 'SOL', 2000, 100, 1200, '71'),
             ('notification_received', 'short_account', 'DOGE', 2100, 200, 1300, '82')])
        self.assertEqual(events[0]['fill_id'], '41')
        self.assertEqual(events[1]['status'], 'canceled')
        for event, account in zip(events, (A, B)):
            self.assertEqual(event['generation'], enabled._states[account]['generation'])
            self.assertEqual(event['revision'], enabled._states[account]['revision'])
            self.assertEqual((event['batch_count'], event['captured_count']), (1, 1))

    def test_snapshots_duplicates_stale_sessions_and_malformed_batches_are_not_events(self):
        feed = self.ready_feed()
        recorder, lines = self.recorder()
        self.assertTrue(self.send(feed, fills(snapshot=True, rows=[fill(tid=10)])))
        self.assertTrue(self.send(feed, fills(rows=[fill(tid=10)])))
        fresh = fills(rows=[fill(tid=20), fill(tid=20)])
        self.assertTrue(self.send(feed, fresh))
        self.assertTrue(self.send(feed, fresh))
        self.assertFalse(self.send(feed, fills(rows=[fill(tid=30)]), generation=0))
        malformed = fills(rows=[fill(tid=40), {**fill(tid=41), 'oid': True}])
        self.assertFalse(self.send(feed, malformed))
        self.assertFalse(feed.entry_allowed(A))
        events = self.exported(recorder, lines)
        self.assertEqual([(e['kind'], e.get('fill_id')) for e in events],
                         [('notification_received', '20'), ('connection_gap', None)])
        self.assertEqual((events[0]['batch_count'], events[0]['captured_count']), (2, 1))
        self.assertEqual(events[1]['reason'], 'MALFORMED_NOTIFICATION_RECONCILIATION_REQUIRED')

    def test_maximum_valid_batch_caps_diagnostics_but_processes_all_notification_hints(self):
        disabled = self.ready_feed()
        enabled = self.ready_feed()
        rows = [fill('SOL' if i < 64 else 'ETH', tid=i+1, oid=i+100, at=1000+i)
                for i in range(wake.MAX_BATCH_ROWS)]
        rows[-1]['coin'] = 'DOGE'
        message = fills(rows=rows)
        self.assertTrue(self.send(disabled, message))
        recorder, lines = self.recorder(capacity=128)
        self.assertTrue(self.send(enabled, message))
        self.assertEqual(enabled._states, disabled._states)
        self.assertEqual(enabled.dirty_symbols(A), ('DOGE', 'ETH', 'SOL'))
        self.assertFalse(enabled.entry_allowed(A))
        self.assertTrue(enabled.entry_allowed(B))
        self.assertEqual(len(enabled._states[A]['seen']), wake.MAX_SEEN)
        last = rows[-1]
        self.assertIn(('userFills', 'DOGE', last['time'], last['tid'], last['oid']),
                      enabled._states[A]['seen'])
        events = self.exported(recorder, lines)
        self.assertEqual(len(events), 64)
        self.assertEqual([e['fill_id'] for e in events], [str(i) for i in range(1, 65)])
        self.assertEqual({e['captured_count'] for e in events}, {64})
        self.assertEqual({e['batch_count'] for e in events}, {wake.MAX_BATCH_ROWS})
        self.assertEqual({e['symbol'] for e in events}, {'SOL'})

    def test_notification_and_disconnect_recorders_run_after_feed_lock_release(self):
        feed = self.ready_feed()
        recorder, lines = self.recorder()
        observed_locks = []
        record = recorder.record

        def check_lock(kind, **kwargs):
            acquired = []
            def probe():
                locked = feed._lock.acquire(False)
                acquired.append(locked)
                if locked:
                    feed._lock.release()
            thread = threading.Thread(target=probe)
            thread.start()
            thread.join(0.5)
            observed_locks.append((kind, acquired, thread.is_alive()))
            return record(kind, **kwargs)

        with patch.object(recorder, 'record', side_effect=check_lock):
            self.assertTrue(self.send(feed, fills()))
            generation = feed._states[B]['generation']
            feed._disconnected(B, generation)
            feed._disconnected(B, generation)  # No duplicate transition.
        self.assertEqual(observed_locks,
            [('notification_received', [True], False), ('connection_gap', [True], False)])
        self.assertFalse(feed.entry_allowed(B))
        events = self.exported(recorder, lines)
        self.assertEqual(events[-1]['account_role'], 'short_account')
        self.assertEqual(events[-1]['reason'], 'DISCONNECTED_RECONCILIATION_REQUIRED')

    def test_malformed_and_heartbeat_gap_exports_follow_lock_release_and_preserve_gates(self):
        for case in ('invalid_json', 'invalid_protocol', 'heartbeat_timeout'):
            with self.subTest(case=case):
                self.assertTrue(timing.set_recorder(None))
                disabled = self.ready_feed()
                enabled = self.ready_feed()
                def trigger(feed):
                    generation = feed._states[A]['generation']
                    if case == 'invalid_json':
                        return feed._receive(A, generation, '{')
                    if case == 'invalid_protocol':
                        return self.send(feed, fills(rows=[fill(), {**fill(tid=2), 'oid': True}]))
                    feed._clock.now += wake.IDLE_TIMEOUT
                    return feed._heartbeat(A, generation)
                baseline = trigger(disabled)
                recorder, lines = self.recorder()
                lock_probes = []
                record = recorder.record
                def observe(kind, **kwargs):
                    acquired = []
                    def probe():
                        locked = enabled._lock.acquire(False)
                        acquired.append(locked)
                        if locked:
                            enabled._lock.release()
                    thread = threading.Thread(target=probe)
                    thread.start()
                    thread.join(0.5)
                    lock_probes.append((kind, acquired, thread.is_alive()))
                    return record(kind, **kwargs)
                with patch.object(recorder, 'record', side_effect=observe):
                    self.assertEqual(trigger(enabled), baseline)
                    self.assertEqual(enabled._heartbeat(A, enabled._states[A]['generation']), 'CLOSE')
                    enabled._disconnected(A, enabled._states[A]['generation'])
                self.assertEqual(enabled._states, disabled._states)
                self.assertFalse(enabled.entry_allowed(A))
                self.assertEqual(lock_probes, [('connection_gap', [True], False)])
                event, = self.exported(recorder, lines)
                self.assertEqual(event['kind'], 'connection_gap')
                self.assertEqual(event['account_role'], 'long_account')
                self.assertEqual(event['reason'],
                    'NOTIFICATION_GAP_RECONCILIATION_REQUIRED' if case == 'heartbeat_timeout'
                    else 'MALFORMED_NOTIFICATION_RECONCILIATION_REQUIRED')

    def test_real_trade_projection_exports_existing_proof_without_extra_reads(self):
        for side in ('LONG', 'SHORT'):
            with self.subTest(side=side):
                self.assertTrue(timing.set_recorder(None))
                state = self.trade_fixture(side)
                original = deepcopy(state)
                disabled = self.projection(state)
                recorder, lines = self.recorder()
                with patch.object(timing, '_clock', return_value=(T+900, 900000)):
                    enabled = self.projection(state)
                self.assertEqual(enabled, disabled)
                self.assertEqual(state, original)
                trade = enabled[0][0]
                self.assertTrue(trade['protection_verified'])
                event, = self.exported(recorder, lines)
                self.assertEqual(event['kind'], 'trade_observed')
                for key in ('account_role', 'card_id', 'symbol', 'entry_quantity',
                            'remaining_quantity', 'stop_quantity', 'take_profit_quantity',
                            'first_entry_at_ms', 'last_entry_at_ms', 'evidence_at_ms',
                            'protection_verified', 'verification_status'):
                    self.assertEqual(event[key], trade[key])
                self.assertEqual((event['entry_quantity'], event['stop_quantity'],
                                  event['take_profit_quantity']), ('40', '40', '40'))
                self.assertEqual((event['first_entry_at_ms'], event['last_entry_at_ms'],
                                  event['evidence_at_ms'], event['at_ms']),
                                 (T-10000, T-1000, T, T+900))
                self.assertEqual(event['stop_public_status_at_ms'], T-500)
                self.assertEqual(event['stop_observed_at_ms'], T-100)
                self.assertEqual(event['bucket'], state['bucket'])
                self.assertEqual(event['revision'], state['revision'])
                self.assertEqual(event['entry_order_ids'], state['bindings'][0]['orders']['ENTRY'])

    def test_historical_projection_cannot_emit_new_live_verification(self):
        recorder, lines = self.recorder()
        result, _, venue_calls = self.projection(self.trade_fixture(), historical=True)
        self.assertEqual(result[0]['verification_status'], 'HISTORICAL')
        self.assertEqual(venue_calls, [])
        self.assertEqual(self.exported(recorder, lines), [])

    def test_shared_refresh_returns_original_evidence_timestamp_and_same_object(self):
        controller = object.__new__(dispatch.Controller)
        shared = self.trade_fixture()
        original = deepcopy(shared)
        with patch.object(controller, '_refresh', return_value=shared) as refresh:
            self.assertIs(controller.refresh(shared['bucket'], emergency=True), shared)
            recorder, lines = self.recorder()
            with patch.object(timing, '_clock', side_effect=[(T+500, 1000), (T+700, 1400)]):
                self.assertIs(controller.refresh(shared['bucket'], emergency=True), shared)
        self.assertEqual(refresh.call_args_list[0], refresh.call_args_list[1])
        self.assertEqual(shared, original)
        event, = self.exported(recorder, lines)
        self.assertEqual((event['kind'], event['status'], event['evidence_at_ms'],
                          event['at_ms'], event['duration_ns']),
                         ('refresh_completed', 'RETURNED', T, T+700, 400))

    def test_cycle_result_and_exception_are_unchanged_and_hook_follows_context_exit(self):
        for raises in (False, True):
            with self.subTest(raises=raises):
                self.assertTrue(timing.set_recorder(None))
                controller = object.__new__(dispatch.Controller)
                journal = object.__new__(PostgresJournal)
                controller.store = SimpleNamespace(journal=journal)
                active = [False]
                context_events = []
                @contextmanager
                def connection():
                    active[0] = True
                    context_events.append('enter')
                    try:
                        yield
                    finally:
                        active[0] = False
                        context_events.append('exit')
                result = dict(status='ACCEPTED_UNVERIFIED', request_id='b'*64,
                              order_requests_sent=1)
                failure = dispatch.DispatchError('OUTCOME_UNRESOLVED_NO_NEW_REQUEST')
                def work(*args, **kwargs):
                    self.assertTrue(active[0])
                    if raises:
                        raise failure
                    return result
                def invoke():
                    if raises:
                        with self.assertRaises(dispatch.DispatchError) as caught:
                            controller.cycle('a'*64, send=True, allow_new_entries=False)
                        self.assertIs(caught.exception, failure)
                    else:
                        self.assertIs(controller.cycle('a'*64, send=True,
                            allow_new_entries=False), result)
                with patch.object(journal, 'reuse_connection', side_effect=connection), \
                     patch.object(controller, '_cycle', side_effect=work) as cycle:
                    invoke()
                    recorder, lines = self.recorder()
                    hook_context = []
                    record = recorder.record
                    def observe(kind, **kwargs):
                        hook_context.append(active[0])
                        return record(kind, **kwargs)
                    with patch.object(recorder, 'record', side_effect=observe):
                        invoke()
                self.assertEqual(cycle.call_args_list[0], cycle.call_args_list[1])
                self.assertEqual(context_events, ['enter', 'exit', 'enter', 'exit'])
                self.assertEqual(hook_context, [False])
                event, = self.exported(recorder, lines)
                self.assertEqual(event['kind'], 'cycle_completed')
                self.assertEqual(event['status'], 'ERROR' if raises else result['status'])
                self.assertEqual(event['failure_code'], 'DispatchError' if raises else None)

    def test_saturated_or_raising_recorder_cannot_interrupt_original_hook_actions(self):
        for failure_mode in ('full', 'raises'):
            with self.subTest(failure_mode=failure_mode):
                self.assertTrue(timing.set_recorder(None))
                feed = self.ready_feed()
                state = self.trade_fixture()
                baseline = self.projection(state)
                recorder, lines = self.recorder(capacity=1)
                if failure_mode == 'full':
                    self.assertTrue(recorder.record('occupy'))
                record = (Mock(side_effect=RuntimeError('BROKEN_RECORDER'))
                          if failure_mode == 'raises' else recorder.record)
                controller = object.__new__(dispatch.Controller)
                controller.store = SimpleNamespace(journal=None)
                result = dict(status='NO_ACTION_NEEDED', order_requests_sent=0)
                with patch.object(recorder, 'record', side_effect=record), \
                     patch.object(controller, '_cycle', return_value=result), \
                     patch.object(controller, '_refresh', return_value=state):
                    self.assertTrue(self.send(feed, fills()))
                    self.assertFalse(feed.entry_allowed(A))
                    self.assertEqual(feed.dirty_symbols(A), ('SOL',))
                    self.assertIs(controller.cycle('a'*64), result)
                    self.assertIs(controller.refresh('a'*64), state)
                    self.assertEqual(self.projection(state), baseline)
                if failure_mode == 'full':
                    self.assertEqual(recorder.health()['dropped_full'], 4)
                else:
                    self.assertEqual(record.call_count, 4)
                self.exported(recorder, lines)
