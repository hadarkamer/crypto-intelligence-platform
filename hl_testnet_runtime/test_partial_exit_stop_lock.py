"""End-to-end residual/stop-lock regressions using SQLite and software I/O.

The software venue records explicit fills; it makes no claim about Hyperliquid
linked-order behavior. In particular these tests do not authorize two cards
to share one account/symbol. Network, signing and wallet construction are
blocked, and every state file lives in a disposable directory.
"""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import ExecutionState
from .test_approved_alert_lifecycle import alert
from .test_experimental_execution_runtime import ROUTES, SoftwareExchange, T


class PartialExitStopLockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name in ('socket.create_connection', 'socket.socket.connect',
                     'socket.socket.connect_ex', 'http.client.HTTPSConnection',
                     'hyperliquid_testnet_executor._wallet',
                     'hyperliquid_testnet_executor._signed_body'):
            guard = patch(name, side_effect=AssertionError('ISOLATED_NO_NETWORK_OR_SIGNER'))
            guard.start()
            self.addCleanup(guard.stop)
        self.path = Path(self.tmp.name) / 'partial-lock.isolated-experimental.sqlite3'
        self.venue = SoftwareExchange(T + 10000)
        self.store = ExecutionState(self.path)
        self.store.initialize(ROUTES, not_before_ms=T - 60000)
        self.restart()
        self.msg = r2732_message(entry=2.3, decision_ms=T - 60000)
        self.worker.receive([self.msg])
        self.worker.run_once()
        self.settle()

    def restart(self):
        self.store = ExecutionState(self.path)
        self.worker = runtime.IsolatedExecutionRuntime(self.store, self.venue, mode=runtime.MODE)

    def settle(self, cycles=3):
        for _ in range(cycles):
            self.worker.run_once(entries_enabled=False)

    def card(self, msg=None):
        return self.store.load()['trades'][(msg or self.msg)['occurrence_id']]

    def orders(self, msg, leg, *, active=True):
        cid = msg['occurrence_id']
        owned = {r['proposal']['action']['orders'][0]['c']
                 if r['proposal']['action']['type'] == 'order'
                 else r['proposal']['action']['modifies'][0]['order']['c']
                 for r in self.venue.requests
                 if r['proposal']['card_id'] == cid and r['proposal']['leg'] == leg
                 and r['proposal']['action']['type'] != 'cancel'}
        return [(oid, item['view']) for oid, item in self.venue.orders.items()
                if item['view']['cloid'] in owned
                and (not active or item['view']['status'] == 'OPEN')]

    def outstanding(self, row):
        return Decimal(row['wire_order']['s']) - sum(
            (Decimal(f['quantity']) for f in row['fills']), Decimal(0))

    def lock_bar(self):
        self.venue.t = T + 60000
        self.venue.mark[runtime._lane(ROUTES['short_account']['account'], 'XRP')] = '2.275'
        self.venue.bars[self.msg['occurrence_id']] = [dict(
            open_at_ms=T, open='2.3', high='2.305', low='2.27', close='2.275')]

    def assert_residual_protection(self, msg, expected):
        self.assertEqual(runtime._remaining(self.card(msg)), expected)
        for leg in ('STOP', 'TAKE_PROFIT'):
            rows = self.orders(msg, leg)
            self.assertEqual(len(rows), 1, leg)
            self.assertEqual(self.outstanding(rows[0][1]), expected, leg)
            self.assertTrue(rows[0][1]['wire_order']['r'], leg)

    def test_partial_take_and_lock_share_one_stop_amendment_without_rewriting_take(self):
        original = deepcopy(self.card())
        take_oid, take = self.orders(self.msg, 'TAKE_PROFIT')[0]
        self.venue.fill(take_oid, '30')
        self.lock_bar()
        before = len(self.venue.requests)
        result = self.worker.run_once()
        self.assertEqual(result['operation'], 'AMEND_EXIT')
        self.settle()
        expected = Decimal(original['quantity']) - 30
        self.assert_residual_protection(self.msg, expected)
        self.assertEqual(len(self.venue.requests) - before, 1)
        stop = self.orders(self.msg, 'STOP')[0][1]['wire_order']
        self.assertEqual(stop['p'], '2.2943')
        self.assertEqual(Decimal(stop['s']), expected)
        self.assertEqual(self.orders(self.msg, 'TAKE_PROFIT')[0][0], take_oid)
        self.assertEqual(self.orders(self.msg, 'TAKE_PROFIT')[0][1]['wire_order'], take['wire_order'])
        self.assertEqual(self.card()['source'], original['source'])
        self.assertEqual(self.card()['prices'], original['prices'])

    def test_partial_stop_lock_lost_modify_reply_and_restart_resize_each_exit_once(self):
        original = self.card()
        stop_oid, _ = self.orders(self.msg, 'STOP')[0]
        self.venue.fill(stop_oid, '30')
        self.lock_bar()
        before = len(self.venue.requests)
        self.venue.lose_reply = True
        result = self.worker.run_once()
        self.assertEqual(result['status'], 'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.restart()
        self.settle(4)
        expected = Decimal(original['quantity']) - 30
        self.assert_residual_protection(self.msg, expected)
        requests = self.venue.requests[before:]
        self.assertEqual([r['proposal']['leg'] for r in requests], ['STOP', 'TAKE_PROFIT'])
        self.assertTrue(all(r['proposal']['operation'] == 'AMEND_EXIT' for r in requests))
        self.assertEqual(self.orders(self.msg, 'STOP')[0][1]['wire_order']['p'], '2.2943')
        self.assertEqual(self.orders(self.msg, 'TAKE_PROFIT')[0][1]['wire_order']['p'],
                         original['prices']['take_profit'])
        # Replayed observations after restart never count the partial fill twice.
        self.assertEqual(len(self.card()['exit_fills']), 1)
        self.assertEqual(self.card()['source'], original['source'])

    def test_additional_fill_during_resize_is_reconciled_to_latest_card_residual(self):
        original = self.card()
        take_oid, _ = self.orders(self.msg, 'TAKE_PROFIT')[0]
        self.venue.fill(take_oid, '30')
        self.lock_bar()

        def fill_after_observation(request):
            self.venue.before_send = None
            self.venue.fill(take_oid, '20')

        self.venue.before_send = fill_after_observation
        before = len(self.venue.requests)
        self.worker.run_once()
        self.settle()
        self.assert_residual_protection(self.msg, Decimal(original['quantity']) - 50)
        self.assertEqual(len(self.card()['exit_fills']), 2)
        requests = self.venue.requests[before:]
        self.assertEqual([r['proposal']['leg'] for r in requests], ['STOP', 'STOP'])
        self.assertEqual(self.orders(self.msg, 'TAKE_PROFIT')[0][0], take_oid)
        # This verifies recovery in a single-owner lane, not a race-free venue
        # guarantee for pooled positions with independently executable exits.

    def test_repeated_unchanged_cycle_and_restart_send_nothing_after_lock_resize(self):
        take_oid, _ = self.orders(self.msg, 'TAKE_PROFIT')[0]
        self.venue.fill(take_oid, '30')
        self.lock_bar()
        self.settle()
        before = deepcopy(self.venue.requests)
        expected_fills = deepcopy(self.card()['exit_fills'])
        self.settle(4)
        self.restart()
        self.settle(4)
        self.assertEqual(self.venue.requests, before)
        self.assertEqual(self.card()['exit_fills'], expected_fills)

    def test_partial_then_final_take_retires_only_own_stop_and_preserves_fill_totals(self):
        original_quantity = Decimal(self.card()['quantity'])
        take_oid, _ = self.orders(self.msg, 'TAKE_PROFIT')[0]
        self.venue.fill(take_oid, '30')
        self.lock_bar()
        self.settle()
        self.venue.fill(take_oid, str(original_quantity - 30))
        before = len(self.venue.requests)
        self.settle()
        self.restart()
        self.settle()
        self.assertEqual(self.card()['phase'], 'CLOSED')
        self.assertEqual(runtime._remaining(self.card()), 0)
        requests = self.venue.requests[before:]
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]['proposal']['operation'], 'CANCEL')
        self.assertEqual(requests[0]['proposal']['card_id'], self.msg['occurrence_id'])
        self.assertEqual(requests[0]['proposal']['leg'], 'STOP')

    def test_unknown_stop_modify_does_not_block_existing_other_account_protection(self):
        other = alert(symbol='HYPE', side='LONG', approved_ms=T)
        key = runtime._lane(ROUTES['long_account']['account'], 'HYPE')
        self.venue.mark[key] = other['entry']
        self.worker.receive([other])
        self.worker.run_once()
        entry_oid, entry = self.orders(other, 'ENTRY')[0]
        self.venue.fill(entry_oid, entry['wire_order']['s'])
        self.settle()
        other_original = deepcopy(self.card(other))

        take_oid, _ = self.orders(self.msg, 'TAKE_PROFIT')[0]
        self.venue.fill(take_oid, '30')
        self.lock_bar()
        self.venue.lose_reply = True
        self.worker.run_once()
        # Hide only the unacknowledged new order; the previously observed
        # original remains in complete history as canceled. Its owner must wait.
        self.venue.omit_order = self.orders(self.msg, 'STOP')[0][0]
        other_take_oid, _ = self.orders(other, 'TAKE_PROFIT')[0]
        self.venue.fill(other_take_oid, '.1')
        before = len(self.venue.requests)
        self.restart()
        self.settle()
        requests = self.venue.requests[before:]
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]['proposal']['card_id'], other['occurrence_id'])
        self.assertEqual(requests[0]['proposal']['leg'], 'STOP')
        self.assert_residual_protection(other, Decimal(other_original['quantity']) - Decimal('.1'))
        self.assertEqual(self.card(other)['prices'], other_original['prices'])
        self.assertEqual(self.card(other)['source'], other_original['source'])

    def test_other_symbol_can_open_in_same_account_after_partial_exit_and_stop_lock(self):
        take_oid, _ = self.orders(self.msg, 'TAKE_PROFIT')[0]
        self.venue.fill(take_oid, '30')
        self.lock_bar()
        self.settle()
        original = deepcopy(self.card())
        other = alert(symbol='HYPE', side='SHORT', approved_ms=T + 60000)
        self.venue.t = T + 70000
        self.venue.mark[runtime._lane(ROUTES['short_account']['account'], 'HYPE')] = other['entry']
        self.worker.receive([other])
        result = self.worker.run_once()
        self.assertEqual(result['operation'], 'ENTRY')
        entry_oid, entry = self.orders(other, 'ENTRY')[0]
        self.venue.fill(entry_oid, entry['wire_order']['s'])
        self.settle()
        self.assertEqual(self.card(other)['account'], original['account'])
        self.assertEqual(self.card(other)['phase'], 'OPEN')
        self.assertEqual(self.card()['entry_fills'], original['entry_fills'])
        self.assertEqual(self.card()['exit_fills'], original['exit_fills'])
        self.assertEqual(self.card()['prices'], original['prices'])
        self.assert_residual_protection(self.msg, runtime._remaining(original))


if __name__ == '__main__':
    unittest.main()
