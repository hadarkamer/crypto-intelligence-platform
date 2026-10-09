"""Durable websocket fill ingestion, crash boundaries and REST parity.

Only exchange I/O is simulated. The real feed and runtime are used throughout;
the PostgreSQL subset additionally checks the real transaction/rollback boundary.
No test may contact an exchange, obtain a key, or authorize a live submission.
"""
from copy import deepcopy
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from approved_alert_fixtures import maxpain_alert
from . import experimental_execution_runtime as isolated
from . import experimental_live_state as live_state
from . import fill_wakeups as wake
from .test_experimental_execution_runtime import T
from .test_experimental_live_runtime import RuntimeContract, CI


class IngestionFixture:
    # Deliberately borrow only fixture methods, not RuntimeContract's test suite.
    real_pg = False
    make_worker = RuntimeContract.make_worker
    cycle = RuntimeContract.cycle
    start = RuntimeContract.start
    restart = RuntimeContract.restart

    def setUp(self):
        RuntimeContract.setUp(self)
        self.account = self.store.load()['routes']['long_account']
        self.other = self.store.load()['routes']['short_account']
        self.new_feed()

    def new_feed(self):
        self.feed = wake.FillWakeups(self.store.load()['routes'],
            clock=lambda: self.venue.now() / 1000)
        self.provider.safety = SimpleNamespace(feed=self.feed)
        for account in (self.account, self.other):
            self.feed._opened(account)
            for channel in wake.CHANNELS:
                self.send(dict(channel='subscriptionResponse', data=dict(
                    method='subscribe', subscription=dict(type=channel, user=account))), account)
            self.send(dict(channel='userFills', data=dict(user=account,
                isSnapshot=True, fills=[])), account)
            token = self.feed.begin_reconciliation(account)
            self.assertTrue(self.feed.finish_reconciliation(token, complete=True))

    def send(self, message, account=None):
        account = account or self.account
        generation = self.feed._states[account]['generation']
        self.assertTrue(self.feed._receive(account, generation, json.dumps(message)))

    def enqueue(self, *rows, account=None):
        account = account or self.account
        self.send(dict(channel='userFills', data=dict(user=account,
            isSnapshot=False, fills=list(rows))), account)

    def raw(self, *, tid=7001, oid=900, symbol='SOL', quantity='2', price='95',
            side='B', at=None):
        return dict(coin=symbol, tid=tid, oid=int(oid), sz=quantity, px=price,
            fee='0.01', feeToken='USDC', side=side,
            time=self.venue.now() if at is None else at)

    def bound_entry(self):
        msg, result = self.start(maxpain_alert(approved_ms=T))
        self.assertEqual(result.get('operation'), 'ENTRY')
        self.cycle(False)
        oid = self.venue.oid('ENTRY')
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['orders'][oid]['status'], 'OPEN')
        self.assertFalse(trade['entry_fills'])
        return msg['occurrence_id'], oid

    def exchange_fill(self, oid, quantity='1', *, tid=7001):
        self.venue.fill(oid, quantity)
        item = self.venue.orders[oid]
        fill = item['view']['fills'][-1]
        fill['fill_id'] = 'hl:' + str(tid)
        wire = item['view']['wire_order']
        return self.raw(tid=tid, oid=oid, symbol=item['symbol'], quantity=quantity,
            price=fill['price'], side='B' if wire['b'] else 'A', at=fill['at_ms'])

    def saved_fills(self, account=None):
        return self.store.load().get('external_activity', {}).get('accounts', {}).get(
            account or self.account, {}).get('fills', [])

    def assert_pending(self, count, account=None):
        self.assertEqual(len(self.feed.pending_fills(account or self.account).fills), count)


class WsFillIngestionTests(IngestionFixture, unittest.TestCase):
    def test_unknown_fill_is_retained_without_position_or_ownership_invention(self):
        before = self.store.load()
        self.enqueue(self.raw())
        self.worker._ingest_pending_fills()
        after = self.store.load()
        self.assertEqual(len(self.saved_fills()), 1)
        self.assertEqual(self.saved_fills()[0]['origin'], 'UNATTRIBUTED')
        self.assertEqual(self.saved_fills()[0]['quantity'], '2')
        for key in ('trades', 'requests', 'snapshots'):
            self.assertEqual(after[key], before[key])
        observed = after['external_activity']['accounts'][self.account]
        self.assertFalse(observed.get('history_complete'))
        self.assertIsNone(observed.get('history_cursor_ms'))
        self.assertFalse(observed.get('positions'))
        self.assertFalse(self.feed.entry_allowed(self.account))
        self.assertTrue(self.feed.entry_allowed(self.other))
        self.assert_pending(0)
        self.assertFalse(self.venue.requests)

    def test_known_partial_entry_updates_both_fill_sets_without_certifying_state(self):
        cid, oid = self.bound_entry()
        before = self.store.load()
        self.enqueue(self.exchange_fill(oid))
        with patch.object(self.provider, 'collect', side_effect=AssertionError('NO_REST')), \
                patch.object(self.port, 'send', side_effect=AssertionError('NO_DISPATCH')):
            self.worker._ingest_pending_fills()
        after = self.store.load()
        trade = after['trades'][cid]
        self.assertEqual(isolated._remaining(trade), Decimal('1'))
        self.assertEqual(list(trade['entry_fills']), ['hl:7001'])
        self.assertEqual([f['fill_id'] for f in trade['orders'][oid]['fills']], ['hl:7001'])
        self.assertEqual(trade['phase'], before['trades'][cid]['phase'])
        self.assertEqual(trade['orders'][oid]['status'], before['trades'][cid]['orders'][oid]['status'])
        for key in ('requests', 'snapshots', 'collector_checkpoints', 'account_inventory_checkpoint'):
            self.assertEqual(after.get(key), before.get(key))
        self.assertEqual(self.saved_fills()[0]['origin'], 'BOT')
        self.assertFalse(self.feed.entry_allowed(self.account))

    def test_pending_fill_is_committed_before_failing_rest_and_not_dispatched(self):
        cid, oid = self.bound_entry()
        self.enqueue(self.exchange_fill(oid))
        requests = deepcopy(self.venue.requests)
        def fail_rest(value, **kwargs):
            self.assertEqual(isolated._remaining(value['trades'][cid]), Decimal('1'))
            raise OSError('REST_UNAVAILABLE')
        with patch.object(self.provider, 'collect', side_effect=fail_rest), \
                self.assertRaisesRegex(OSError, 'REST_UNAVAILABLE'):
            self.cycle(False)
        self.assertEqual(isolated._remaining(self.store.load()['trades'][cid]), Decimal('1'))
        self.assertEqual(self.venue.requests, requests)
        self.assert_pending(0)
        self.assertFalse(self.feed.entry_allowed(self.account))

    def test_commit_failure_keeps_queue_and_restart_replays_once(self):
        cid, oid = self.bound_entry()
        row = self.exchange_fill(oid)
        self.enqueue(row)
        before = self.store.load()
        with patch.object(self.store, 'mutate', side_effect=live_state.StateError('COMMIT_FAILED')), \
                self.assertRaisesRegex(live_state.StateError, 'COMMIT_FAILED'):
            self.worker._ingest_pending_fills()
        self.assertEqual(self.store.load(), before)
        self.assert_pending(1)
        self.restart()
        self.worker._ingest_pending_fills()
        self.assertEqual(isolated._remaining(self.store.load()['trades'][cid]), Decimal('1'))
        self.assertEqual(len(self.saved_fills()), 1)
        self.assert_pending(0)

    def test_crash_before_commit_lost_memory_queue_is_recovered_by_rest(self):
        cid, oid = self.bound_entry()
        self.enqueue(self.exchange_fill(oid))
        with patch.object(self.store, 'mutate', side_effect=live_state.StateError('COMMIT_FAILED')), \
                self.assertRaisesRegex(live_state.StateError, 'COMMIT_FAILED'):
            self.worker._ingest_pending_fills()
        self.new_feed()
        self.restart()
        self.assert_pending(0)
        self.cycle(False)
        trade = self.store.load()['trades'][cid]
        self.assertEqual(isolated._remaining(trade), Decimal('1'))
        self.assertEqual(list(trade['entry_fills']), ['hl:7001'])

    def test_lost_commit_ack_replays_durable_fill_without_double_count(self):
        cid, oid = self.bound_entry()
        self.enqueue(self.exchange_fill(oid))
        mutate = self.store.mutate
        def commit_then_lose_reply(fn):
            mutate(fn)
            raise live_state.StateError('COMMIT_ACK_LOST')
        with patch.object(self.store, 'mutate', side_effect=commit_then_lose_reply), \
                self.assertRaisesRegex(live_state.StateError, 'COMMIT_ACK_LOST'):
            self.worker._ingest_pending_fills()
        self.assert_pending(1)
        self.assertEqual(isolated._remaining(self.store.load()['trades'][cid]), Decimal('1'))
        self.restart()
        self.worker._ingest_pending_fills()
        self.assertEqual(isolated._remaining(self.store.load()['trades'][cid]), Decimal('1'))
        self.assertEqual(len(self.saved_fills()), 1)
        self.assert_pending(0)

    def test_ack_failure_after_commit_and_repeat_delivery_are_idempotent(self):
        cid, oid = self.bound_entry()
        row = self.exchange_fill(oid)
        self.enqueue(row)
        with patch.object(self.feed, 'acknowledge_fills', side_effect=RuntimeError('ACK_FAILED')), \
                self.assertRaisesRegex(RuntimeError, 'ACK_FAILED'):
            self.worker._ingest_pending_fills()
        self.assert_pending(1)
        self.restart()
        self.worker._ingest_pending_fills()
        self.enqueue(row)
        self.worker._ingest_pending_fills()
        trade = self.store.load()['trades'][cid]
        self.assertEqual(isolated._remaining(trade), Decimal('1'))
        self.assertEqual(len(trade['orders'][oid]['fills']), 1)
        self.assertEqual(len(self.saved_fills()), 1)
        self.assert_pending(0)

    def test_arrival_during_commit_is_not_removed_by_old_batch_ack(self):
        cid, oid = self.bound_entry()
        self.enqueue(self.exchange_fill(oid))
        mutate = self.store.mutate
        def commit_then_arrival(fn):
            result = mutate(fn)
            self.enqueue(self.exchange_fill(oid, tid=7002))
            return result
        with patch.object(self.store, 'mutate', side_effect=commit_then_arrival):
            self.worker._ingest_pending_fills()
        self.assert_pending(1)
        self.assertEqual(self.feed.pending_fills(self.account).fills[0]['fill_id'], 'hl:7002')
        self.assertEqual(isolated._remaining(self.store.load()['trades'][cid]), Decimal('1'))
        self.worker._ingest_pending_fills()
        self.assertEqual(isolated._remaining(self.store.load()['trades'][cid]), Decimal('2'))
        self.assertEqual(len(self.saved_fills()), 2)

    def test_reducer_failure_rolls_back_raw_and_allocated_fills_together(self):
        from .postgres_journal import JournalError
        cid, oid = self.bound_entry()
        self.enqueue(self.exchange_fill(oid))
        before = self.store.load()
        mutate = self.store.mutate
        reached_injection = []
        def fail_inside_transaction(fn):
            def failing(value):
                fn(value)
                self.assertEqual(isolated._remaining(value['trades'][cid]), Decimal('1'))
                reached_injection.append(True)
                raise ValueError('INJECTED_ROLLBACK')
            return mutate(failing)
        expected_type = JournalError if self.real_pg else ValueError
        expected_code = 'PERSISTENCE_UNAVAILABLE_NO_SEND' if self.real_pg else 'INJECTED_ROLLBACK'
        with patch.object(self.store, 'mutate', side_effect=fail_inside_transaction), \
                self.assertRaises(expected_type) as raised:
            self.worker._ingest_pending_fills()
        self.assertIs(type(raised.exception), expected_type)
        self.assertEqual(str(raised.exception), expected_code)
        self.assertEqual(reached_injection, [True])
        self.assertEqual(self.store.load(), before)
        self.assert_pending(1)

    def test_future_fill_is_rejected_without_committing_or_acknowledging(self):
        before = self.store.load()
        self.enqueue(self.raw(at=self.venue.now() + 60000))
        errors = self.worker._ingest_pending_fills()
        self.assertEqual(errors, {self.account: dict(reason='STREAM_FILL_TIME_INVALID', symbols=['SOL'])})
        self.assertEqual(self.store.load(), before)
        self.assert_pending(1)

    def test_changed_durable_fill_rejects_entire_batch(self):
        row = self.raw()
        self.enqueue(row)
        self.worker._ingest_pending_fills()
        before = self.store.load()
        # A process restart clears feed-level duplicate memory, so durable
        # validation must still reject a changed historical exchange identity.
        self.new_feed()
        self.restart()
        self.enqueue(self.raw(tid=7002), {**row, 'sz': '3'})
        errors = self.worker._ingest_pending_fills()
        self.assertEqual(errors, {self.account: dict(reason='EXTERNAL_FILL_FACT_CHANGED', symbols=['SOL'])})
        self.assertEqual(self.store.load(), before)
        self.assert_pending(2)

    def test_unbound_request_fill_waits_for_rest_to_assign_order(self):
        msg, result = self.start(maxpain_alert(approved_ms=T))
        self.assertEqual(result.get('operation'), 'ENTRY')
        oid = self.venue.oid('ENTRY')
        row = self.exchange_fill(oid)
        self.enqueue(row)
        self.worker._ingest_pending_fills()
        self.assertFalse(self.store.load()['trades'][msg['occurrence_id']]['entry_fills'])
        self.assertEqual(self.saved_fills()[0]['origin'], 'UNATTRIBUTED')
        self.cycle(False)
        self.assertEqual(isolated._remaining(self.store.load()['trades'][msg['occurrence_id']]), Decimal('1'))

    def test_same_order_number_other_account_and_manual_lane_never_allocate(self):
        cid, oid = self.bound_entry()
        row = self.raw(oid=oid, symbol='HYPE', price='93.544', quantity='1')
        self.enqueue(row, account=self.other)
        self.worker._ingest_pending_fills()
        self.assertFalse(self.store.load()['trades'][cid]['entry_fills'])
        self.assertEqual(self.saved_fills(self.other)[0]['origin'], 'UNATTRIBUTED')
        from .experimental_external_activity import lane
        self.store.mutate(lambda value: value.setdefault('external_activity', {}).setdefault(
            'human_managed', {}).update({lane(self.account, 'HYPE'): dict(
                account=self.account, symbol='HYPE', at_ms=self.venue.now(), reason='MANUAL')}))
        self.enqueue(self.exchange_fill(oid, tid=7002))
        self.worker._ingest_pending_fills()
        self.assertFalse(self.store.load()['trades'][cid]['entry_fills'])
        self.assertEqual(len(self.saved_fills()), 1)

    def test_invalid_known_order_allocation_is_raw_only_not_negative_or_overfilled(self):
        cid, oid = self.bound_entry()
        before = self.store.load()['trades'][cid]
        self.enqueue(self.raw(oid=oid, symbol='HYPE', price='93.544', quantity='100'))
        self.worker._ingest_pending_fills()
        self.assertEqual(self.store.load()['trades'][cid], before)
        self.assertEqual(len(self.saved_fills()), 1)
        self.assert_pending(0)

    def test_invalid_bound_fill_has_explicit_deferral_and_no_quantity_mutation(self):
        from .test_experimental_execution_runtime import SoftwareExchange
        for case in ('side', 'price', 'quantity', 'terminal_order'):
            with self.subTest(case=case):
                self.venue = SoftwareExchange(T + 10000)
                self.make_worker(not_before=T - 60000)
                self.new_feed()
                cid, oid = self.bound_entry()
                if case == 'terminal_order':
                    self.exchange_fill(oid, tid=6900)
                    self.venue.orders[oid]['view']['status'] = 'CANCELED'
                    self.cycle(False)
                    self.assertEqual(self.store.load()['trades'][cid]['orders'][oid]['status'], 'CANCELED')
                before = self.store.load()
                row = self.raw(oid=oid, symbol='HYPE', quantity='1', price='93.544')
                if case == 'side':
                    row['side'] = 'A'
                elif case == 'price':
                    row['px'] = '100'
                elif case == 'quantity':
                    row['sz'] = '100'
                self.enqueue(row)
                self.worker._ingest_pending_fills()
                after = self.store.load()
                self.assertEqual(after['trades'][cid], before['trades'][cid])
                self.assertEqual(after['requests'], before['requests'])
                self.assertEqual(after['snapshots'], before['snapshots'])
                self.assertEqual(after['blocked_lanes'][isolated._lane(self.account, 'HYPE')],
                                 'STREAM_FILL_RECONCILIATION_REQUIRED')
                events = [e for e in after['events'] if e['kind'] == 'STREAM_FILL_ALLOCATION_DEFERRED']
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]['occurrence_id'], cid)
                self.assertEqual(events[0]['fill_ids'], ['hl:7001'])
                self.assertTrue(events[0]['reason'])
                self.assertEqual(len(self.saved_fills()), 1)
                self.assert_pending(0)
                # Restart re-delivery cannot multiply the same diagnostic.
                self.new_feed()
                self.restart()
                self.enqueue(row)
                self.worker._ingest_pending_fills()
                self.assertEqual(len([e for e in self.store.load()['events']
                    if e['kind'] == 'STREAM_FILL_ALLOCATION_DEFERRED']), 1)

    def test_partial_exit_updates_quantity_but_rest_retains_status_authority(self):
        cid, oid = self.bound_entry()
        self.exchange_fill(oid, '2')
        for _ in range(3):
            self.cycle(False)
        exit_oid = self.venue.oid('TAKE_PROFIT')
        before = self.store.load()
        self.assertIn(exit_oid, before['trades'][cid]['orders'])
        self.enqueue(self.exchange_fill(exit_oid, '1', tid=7002))
        self.worker._ingest_pending_fills()
        trade = self.store.load()['trades'][cid]
        self.assertEqual(isolated._remaining(trade), Decimal('1'))
        self.assertEqual(list(trade['exit_fills']), ['hl:7002'])
        self.assertEqual(trade['orders'][exit_oid]['status'], 'OPEN')
        self.assertEqual(trade['phase'], before['trades'][cid]['phase'])
        self.cycle(False)
        trade = self.store.load()['trades'][cid]
        self.assertEqual(isolated._remaining(trade), Decimal('1'))
        self.assertEqual(list(trade['exit_fills']), ['hl:7002'])
        self.assertEqual(trade['phase'], 'PARTIALLY_CLOSED')

    def test_exit_arriving_before_missing_entry_defers_allocation_until_rest(self):
        cid, oid = self.bound_entry()
        self.exchange_fill(oid, '2')
        for _ in range(3):
            self.cycle(False)
        stop, take = self.venue.oid('STOP'), self.venue.oid('TAKE_PROFIT')
        # This additional entry occurred at the venue but its websocket frame
        # was lost. Two existing exits can then report more than saved exposure.
        self.exchange_fill(oid, '1', tid=7002)
        self.enqueue(self.exchange_fill(stop, '1.5', tid=7003),
                     self.exchange_fill(take, '1', tid=7004))
        self.worker._ingest_pending_fills()
        candidate = self.store.load()
        self.assertGreaterEqual(isolated._remaining(candidate['trades'][cid]), 0)
        self.assertLessEqual(isolated._sum(candidate['trades'][cid]['exit_fills']), Decimal('2'))
        self.assertEqual(len(self.saved_fills()), 2)
        self.assert_pending(0)
        # Complete REST reconstructs all three facts in the same snapshot.
        context = self.venue.collect(candidate)
        for snapshot in context['snapshots']:
            self.worker._snapshot(candidate, snapshot, self.venue.now())
        self.assertEqual(isolated._remaining(candidate['trades'][cid]), Decimal('0.5'))
        self.assertEqual(set(candidate['trades'][cid]['entry_fills']), {'hl:7001', 'hl:7002'})
        self.assertEqual(set(candidate['trades'][cid]['exit_fills']), {'hl:7003', 'hl:7004'})

    def test_duplicate_does_not_downgrade_existing_external_attribution(self):
        row = self.raw()
        self.enqueue(row)
        self.worker._ingest_pending_fills()
        self.store.mutate(lambda value: value['external_activity']['accounts'][
            self.account]['fills'][0].update(origin='EXTERNAL'))
        self.enqueue(row)
        self.worker._ingest_pending_fills()
        self.assertEqual(len(self.saved_fills()), 1)
        self.assertEqual(self.saved_fills()[0]['origin'], 'EXTERNAL')

    def test_rest_duplicate_after_ws_matches_rest_only_execution_facts(self):
        cid, oid = self.bound_entry()
        row = self.exchange_fill(oid)
        before = self.store.load()
        self.enqueue(row)
        self.worker._ingest_pending_fills()
        after = self.store.load()
        context_ws = self.venue.collect(after)
        context_rest = self.venue.collect(before)
        # Isolate the same authoritative snapshot transition in both paths.
        for snapshot in context_ws['snapshots']:
            self.worker._snapshot(after, snapshot, self.venue.now())
        for snapshot in context_rest['snapshots']:
            self.worker._snapshot(before, snapshot, self.venue.now())
        self.assertEqual(after['trades'], before['trades'])
        self.assertEqual(after['requests'], before['requests'])
        self.assertEqual(after['snapshots'], before['snapshots'])
        self.assertEqual(isolated._remaining(after['trades'][cid]), Decimal('1'))

    def test_real_raw_provider_reconciles_persisted_ws_partial_without_duplicate(self):
        from .test_experimental_live_service import FullConnectionTests
        # This separate fixture exercises the actual provider and dispatch
        # adapters. Its HTTPS/signing ports remain synthetic and fully isolated.
        h = FullConnectionTests()
        h.setUp()
        self.addCleanup(h.doCleanups)
        h.provider.safety.feed = self.feed
        msg = maxpain_alert(approved_ms=T)
        h.seed(msg)
        self.assertEqual(h.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
        h.worker.run_once(entries_enabled=False)
        oid = h.oracle.oid('ENTRY')
        cid = msg['occurrence_id']
        self.assertIn(oid, h.store.load()['trades'][cid]['orders'])
        h.oracle.fill(oid, '1')
        rows = h.raw.read('userFillsByTime', self.account, start=T, end=h.oracle.now())
        self.assertEqual(len(rows), 1)
        expected_id = 'hl:' + str(rows[0]['tid'])
        self.enqueue(*rows)
        h.worker._ingest_pending_fills()
        after_stream = h.store.load()
        self.assertEqual(isolated._remaining(after_stream['trades'][cid]), Decimal('1'))
        self.assertEqual(list(after_stream['trades'][cid]['entry_fills']), [expected_id])
        result = h.worker.run_once(entries_enabled=False)
        self.assertEqual(result.get('operation'), 'CREATE_EXIT')
        after_rest = h.store.load()
        self.assertEqual(isolated._remaining(after_rest['trades'][cid]), Decimal('1'))
        self.assertEqual(list(after_rest['trades'][cid]['entry_fills']), [expected_id])
        self.assertEqual(len(after_rest['external_activity']['accounts'][self.account]['fills']), 1)
        checkpoint = after_rest['collector_checkpoints'][isolated._lane(self.account, 'HYPE')]
        self.assertGreaterEqual(checkpoint['at_ms'], rows[0]['time'])

    def test_complete_checkpoint_rejects_changed_replay_after_recent_fact_pruned(self):
        from .test_experimental_live_service import FullConnectionTests
        h = FullConnectionTests()
        h.setUp()
        self.addCleanup(h.doCleanups)
        msg = maxpain_alert(approved_ms=T)
        h.seed(msg)
        self.assertEqual(h.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
        h.worker.run_once(entries_enabled=False)
        oid = h.oracle.oid('ENTRY')
        h.oracle.fill(oid, '1')
        h.worker.run_once(entries_enabled=False)
        row = h.raw.read('userFillsByTime', self.account, start=T, end=h.oracle.now())[0]
        fid = 'hl:' + str(row['tid'])
        lane = isolated._lane(self.account, 'HYPE')
        checkpoint = h.store.load()['collector_checkpoints'][lane]
        self.assertTrue(checkpoint['history_complete'])
        self.assertIn(fid, {f['fill_id'] for f in checkpoint['fills']})
        def prune_recent(value):
            account = value['external_activity']['accounts'][self.account]
            account['fills'] = [f for f in account['fills'] if f['fill_id'] != fid]
        h.store.mutate(prune_recent)
        before = h.store.load()
        for field, changed in (('sz', '2'), ('fee', '0.2'), ('oid', 998877), ('coin', 'SOL')):
            with self.subTest(changed_field=field):
                self.new_feed()
                h.provider.safety.feed = self.feed
                self.enqueue(self.raw(tid=999888, at=h.oracle.now()), {**row, field: changed})
                errors = h.worker._ingest_pending_fills()
                self.assertEqual(errors[self.account]['reason'], 'EXTERNAL_FILL_FACT_CHANGED')
                self.assertEqual(h.store.load(), before)
                self.assert_pending(2)
        # An exact replay remains harmless after restart and retention pruning.
        self.new_feed()
        h.provider.safety.feed = self.feed
        self.enqueue(row)
        self.assertEqual(h.worker._ingest_pending_fills(), {})
        self.assert_pending(0)
        after = h.store.load()
        self.assertEqual(after['trades'], before['trades'])
        self.assertEqual(after['collector_checkpoints'], before['collector_checkpoints'])
        self.assertEqual(isolated._remaining(after['trades'][msg['occurrence_id']]), Decimal('1'))

    def test_deferred_stream_lane_waits_for_complete_history_without_blocking_peer_protection(self):
        from experimental_execution_fixtures import r2732_message
        from .card_sync_evidence import SyncError
        from .test_experimental_live_service import FullConnectionTests
        for history_mode in ('absent', 'error', 'resolved_pruned'):
            with self.subTest(history=history_mode):
                h = FullConnectionTests()
                h.setUp()
                self.addCleanup(h.doCleanups)
                self.new_feed()
                h.provider.safety.feed = self.feed
                msg = maxpain_alert(approved_ms=T)
                cid = msg['occurrence_id']
                h.seed(msg)
                self.assertEqual(h.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
                h.worker.run_once(entries_enabled=False)
                entry = h.oracle.oid('ENTRY')
                h.oracle.fill(entry, '2')
                for _ in range(3):
                    h.worker.run_once(entries_enabled=False)
                stop, take = h.oracle.oid('STOP'), h.oracle.oid('TAKE_PROFIT')
                if history_mode == 'resolved_pruned':
                    # Model an earlier resolved episode whose canonical facts
                    # remain in the complete checkpoint after recent-observation
                    # retention has pruned its redundant account-level rows.
                    old_id = next(iter(h.store.load()['trades'][cid]['entry_fills']))
                    def old_resolved_episode(value):
                        value['events'].append(dict(kind='STREAM_FILL_ALLOCATION_DEFERRED',
                            occurrence_id=cid, account=self.account, symbol='HYPE',
                            fill_ids=[old_id], reason='EXACT_OBSERVED_STREAM_ORDER_REQUIRED',
                            at_ms=T))
                        account = value['external_activity']['accounts'][self.account]
                        account['fills'] = [f for f in account['fills'] if f['fill_id'] != old_id]
                    h.store.mutate(old_resolved_episode)
                peer = r2732_message(entry=2.3, decision_ms=T - 60000)
                h.seed(peer)
                self.assertEqual(h.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
                stale_orders = deepcopy(h.oracle.orders)
                # The stream delivers exits first; the venue really contains
                # another entry that makes both executions economically valid.
                h.oracle.fill(entry, '1')
                h.oracle.fill(stop, '1.5')
                h.oracle.fill(take, '1')
                complete_orders = deepcopy(h.oracle.orders)
                rows = h.raw.read('userFillsByTime', self.account,
                    start=T, end=h.oracle.now())
                exits = [row for row in rows if str(row['oid']) in (stop, take)]
                self.assertEqual(len(exits), 2)
                # REST temporarily exposes its previous, internally consistent
                # snapshot. This specifically tests the durable stream fence.
                h.oracle.orders = stale_orders
                if history_mode == 'resolved_pruned':
                    def prune_resolved(value):
                        account = value['external_activity']['accounts'][self.account]
                        account['fills'] = [f for f in account['fills'] if f['fill_id'] != old_id]
                    h.store.mutate(prune_resolved)
                    prior = h.store.load()
                    self.assertNotIn(old_id, {f['fill_id'] for f in
                        prior['external_activity']['accounts'][self.account]['fills']})
                    self.assertIn(old_id, {f['fill_id'] for f in prior['collector_checkpoints'][
                        isolated._lane(self.account, 'HYPE')]['fills']})
                self.enqueue(*exits)
                h.worker._ingest_pending_fills()
                lane = isolated._lane(self.account, 'HYPE')
                saved = h.store.load()
                self.assertEqual(saved['blocked_lanes'][lane], 'STREAM_FILL_RECONCILIATION_REQUIRED')
                self.assertEqual(isolated._remaining(saved['trades'][cid]), Decimal('2'))
                read = h.raw.read
                def unavailable(kind, account_arg=None, **kwargs):
                    if kind == 'userFillsByTime' and (account_arg or kwargs.get('user')) == self.account:
                        raise SyncError('PUBLIC_READ_UNAVAILABLE')
                    return read(kind, account_arg, **kwargs)
                if history_mode == 'error':
                    h.raw.read = unavailable
                checkpoint = deepcopy(saved.get('collector_checkpoints', {}).get(lane))
                for _ in range(3):
                    h.oracle.t += 100
                    h.worker.run_once(entries_enabled=False)
                    current = h.store.load()
                    self.assertEqual(current['blocked_lanes'][lane], 'STREAM_FILL_RECONCILIATION_REQUIRED')
                    self.assertEqual(isolated._remaining(current['trades'][cid]), Decimal('2'))
                    self.assertEqual(current.get('collector_checkpoints', {}).get(lane), checkpoint)
                peer_exits = [item for item in h.oracle.orders.values()
                    if item['account'] == self.other and item['leg'] in ('STOP', 'TAKE_PROFIT')]
                self.assertEqual({item['leg'] for item in peer_exits}, {'STOP', 'TAKE_PROFIT'})
                self.assertEqual(h.store.load()['trades'][peer['occurrence_id']]['phase'], 'OPEN')
                # Restore the complete long-account facts while retaining every
                # protective order created for the healthy peer in the interim.
                h.raw.read = read
                h.oracle.orders.update({oid: item for oid, item in complete_orders.items()
                    if item['account'] == self.account})
                h.oracle.t += 100
                h.worker.run_once(entries_enabled=False)
                restored = h.store.load()
                self.assertNotIn(lane, restored['blocked_lanes'])
                self.assertEqual(isolated._remaining(restored['trades'][cid]), Decimal('0.5'))
                self.assertEqual(len(restored['trades'][cid]['exit_fills']), 2)
                self.assertGreater(restored['collector_checkpoints'][lane]['at_ms'], checkpoint['at_ms'])

    def test_invalid_long_batch_preserves_short_fills_and_reaches_peer_protection(self):
        from experimental_execution_fixtures import r2732_message
        from .test_experimental_live_service import FullConnectionTests
        h = FullConnectionTests()
        h.setUp()
        self.addCleanup(h.doCleanups)
        h.provider.safety.feed = self.feed
        peer = r2732_message(entry=2.3, decision_ms=T - 60000)
        h.seed(peer)
        self.assertEqual(h.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
        valid = h.raw.read('userFillsByTime', self.other, start=T, end=h.oracle.now())
        self.assertEqual(len(valid), 1)
        self.enqueue(self.raw(at=h.oracle.now() + 60000))
        self.enqueue(*valid, account=self.other)
        with patch.object(h.provider, 'collect', wraps=h.provider.collect) as rest:
            result = h.worker.run_once(entries_enabled=False)
        self.assertTrue(rest.called)
        self.assertEqual(result.get('operation'), 'CREATE_EXIT')
        current = h.store.load()
        self.assertEqual(len(current['external_activity']['accounts'][self.other]['fills']), 1)
        self.assertEqual(current['blocked_lanes'][isolated._lane(self.account, 'SOL')],
                         'STREAM_FILL_TIME_INVALID')
        self.assertNotIn(isolated._lane(self.other, 'XRP'), current['blocked_lanes'])
        self.assertGreater(isolated._remaining(current['trades'][peer['occurrence_id']]), 0)
        self.assertNotIn('_pending_stream_fill_errors', current)
        self.assert_pending(1)
        self.assert_pending(0, self.other)

    def test_real_commit_failure_still_stops_before_rest_or_peer_processing(self):
        self.enqueue(self.raw())
        self.enqueue(self.raw(tid=7002), account=self.other)
        before = self.store.load()
        with patch.object(self.store, 'mutate', side_effect=live_state.StateError('DATABASE_UNAVAILABLE')), \
                patch.object(self.provider, 'collect', wraps=self.provider.collect) as rest, \
                self.assertRaisesRegex(live_state.StateError, 'DATABASE_UNAVAILABLE'):
            self.cycle(False)
        rest.assert_not_called()
        self.assertEqual(self.store.load(), before)
        self.assert_pending(1)
        self.assert_pending(1, self.other)

    def test_retention_capacity_keeps_queue_and_recovers_after_rest_prunes_old_facts(self):
        from . import experimental_external_activity as external
        from .test_experimental_live_service import FullConnectionTests
        h = FullConnectionTests()
        h.setUp()
        self.addCleanup(h.doCleanups)
        h.provider.safety.feed = self.feed
        old = self.raw(at=h.oracle.now())
        raw_fills = [old]
        read = h.raw.read
        def history(kind, account_arg=None, **kwargs):
            result = read(kind, account_arg, **kwargs)
            if kind == 'userFillsByTime' and (account_arg or kwargs.get('user')) == self.account:
                return result + [deepcopy(f) for f in raw_fills
                    if kwargs['start'] <= f['time'] <= kwargs['end']]
            return result
        h.raw.read = history
        with patch.object(external, 'MAX_RECENT_FILLS', 1):
            self.enqueue(old)
            self.assertEqual(h.worker._ingest_pending_fills(), {})
            # Complete cursor 90 seconds beyond the old fill retains it in
            # the two-minute window, but the next REST overlap excludes it.
            h.oracle.t += 90000
            h.worker.run_once(entries_enabled=False)
            first = h.store.load()['external_activity']['accounts'][self.account]
            self.assertEqual(first['history_cursor_ms'], h.oracle.now())
            self.assertEqual(len(first['fills']), 1)
            h.oracle.t += 90000
            new = self.raw(tid=7002, at=h.oracle.now())
            raw_fills.append(new)
            self.enqueue(new)
            errors = h.worker._ingest_pending_fills()
            self.assertEqual(errors[self.account]['reason'], 'EXTERNAL_FILL_RETENTION_CAPACITY_REVIEW_REQUIRED')
            self.assert_pending(1)
            with patch.object(h.provider, 'collect', wraps=h.provider.collect) as rest:
                h.worker.run_once(entries_enabled=False)
            self.assertTrue(rest.called)
            recovered = h.store.load()['external_activity']['accounts'][self.account]
            self.assertEqual(recovered['history_cursor_ms'], h.oracle.now())
            self.assertEqual([f['fill_id'] for f in recovered['fills']], ['hl:7002'])
            self.assert_pending(1)
            self.assertEqual(h.worker._ingest_pending_fills(), {})
            self.assert_pending(0)
            self.assertEqual(len(h.store.load()['external_activity']['accounts'][self.account]['fills']), 1)


@unittest.skipUnless(CI, 'requires isolated loopback HL_JOURNAL_CI_URL')
class WsFillIngestionPostgresTests(IngestionFixture, unittest.TestCase):
    """Only transaction-specific regressions are repeated on real PostgreSQL."""
    real_pg = True
    test_atomic_rollback = WsFillIngestionTests.test_reducer_failure_rolls_back_raw_and_allocated_fills_together
    test_unknown_commit_outcome = WsFillIngestionTests.test_lost_commit_ack_replays_durable_fill_without_double_count
    test_ack_after_commit = WsFillIngestionTests.test_ack_failure_after_commit_and_repeat_delivery_are_idempotent
    test_concurrent_arrival = WsFillIngestionTests.test_arrival_during_commit_is_not_removed_by_old_batch_ack
    test_fill_survives_rest_failure = WsFillIngestionTests.test_pending_fill_is_committed_before_failing_rest_and_not_dispatched


if __name__ == '__main__':
    unittest.main()
