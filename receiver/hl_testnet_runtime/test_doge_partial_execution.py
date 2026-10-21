"""Real journal transitions with an isolated venue; no real account access."""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import BASE, doge_partial_alert
from . import doge_partial_execution as doge
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import ExecutionState
from .test_experimental_execution_runtime import ROUTES
from .test_approved_alert_lifecycle import CurrentOnlyExchange


class DogePartialRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name in ('http.client.HTTPSConnection', 'socket.create_connection',
                     'hyperliquid_testnet_executor._wallet',
                     'hyperliquid_testnet_executor._signed_body',
                     'hl_testnet_runtime.experimental_execution_runtime._range'):
            blocker = patch(name, side_effect=AssertionError('NO_REAL_VENUE_OR_SIGNER'))
            blocker.start(); self.addCleanup(blocker.stop)
        self.path = Path(self.tmp.name)/'doge.isolated-experimental.sqlite3'
        self.venue = CurrentOnlyExchange(BASE+1000)
        self.store = ExecutionState(self.path)
        self.store.initialize(ROUTES, not_before_ms=BASE-60000)
        self.restart()

    def restart(self):
        self.store = ExecutionState(self.path)
        self.worker = runtime.IsolatedExecutionRuntime(self.store, self.venue, mode=runtime.MODE)

    def run_cycles(self, count=6):
        return [self.worker.run_once(entries_enabled=False) for _ in range(count)]

    def start(self, side='LONG', fill=True):
        self.msg = doge_partial_alert(side=side)
        self.worker.receive([self.msg])
        result = self.worker.run_once()
        self.assertEqual(result['operation'], 'ENTRY')
        if fill:
            entry = self.venue.oid('ENTRY')
            self.venue.fill(entry, self.venue.orders[entry]['view']['wire_order']['s'])
            self.run_cycles()
        return self.trade()

    def trade(self):
        return self.store.load()['trades'][self.msg['occurrence_id']]

    def active(self, leg=None):
        return [(oid, row['view']) for oid, row in self.venue.orders.items()
                if row['view']['status']=='OPEN' and (leg is None or row['leg']==leg)]

    def take(self, partial):
        price = doge.partial_price(self.msg, 2) if partial else self.trade()['prices']['take_profit']
        return next((oid, row) for oid, row in self.active('TAKE_PROFIT')
                    if Decimal(row['wire_order']['p'])==Decimal(price))

    def mark(self, price):
        role = 'long_account' if self.msg['side']=='LONG' else 'short_account'
        self.venue.mark[runtime._lane(ROUTES[role]['account'], 'DOGE')] = str(price)

    def assert_protected(self):
        trade = self.trade()
        desired = {(leg, Decimal(price)):Decimal(quantity)
                   for leg, price, quantity in doge.desired_orders(trade)}
        actual = {}
        for oid, row in self.active():
            leg = trade['order_legs'][oid]
            if leg=='ENTRY':
                continue
            key = (leg, Decimal(row['wire_order']['p']))
            self.assertNotIn(key, actual)
            actual[key] = Decimal(row['wire_order']['s'])-sum(
                (Decimal(f['quantity']) for f in row['fills']), Decimal(0))
            self.assertTrue(row['wire_order']['r'])
        self.assertEqual(actual, desired)

    def test_both_directions_get_original_stop_and_two_exact_split_targets(self):
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                if side=='SHORT':
                    self.setUp()
                trade = self.start(side)
                self.assertEqual(trade['role'], 'long_account' if side=='LONG' else 'short_account')
                self.assertEqual(len(self.active('TAKE_PROFIT')), 2)
                q = Decimal(trade['quantity'])
                first = (q*Decimal('.75')).quantize(Decimal('.01'), rounding=ROUND_DOWN)
                self.assertEqual(Decimal(self.take(True)[1]['wire_order']['s']), first)
                self.assertEqual(Decimal(self.take(False)[1]['wire_order']['s']), q-first)
                self.assert_protected()

    def test_partial_fill_restart_resizes_original_stop_and_keeps_final_target(self):
        trade = self.start()
        partial, row = self.take(True)
        self.venue.fill(partial, row['wire_order']['s'])
        self.restart(); self.run_cycles()
        self.assertEqual(self.trade()['phase'], 'PARTIALLY_CLOSED')
        self.assertEqual(len(self.active('TAKE_PROFIT')), 1)
        self.assertEqual(self.active('STOP')[0][1]['wire_order']['p'], trade['prices']['stop'])
        self.assert_protected()
        final, row = self.take(False)
        self.venue.fill(final, row['wire_order']['s'])
        self.run_cycles(); self.restart(); self.run_cycles()
        self.assertEqual(self.trade()['phase'], 'CLOSED')
        self.assertFalse(self.active())
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests), 1)

    def test_partial_take_filled_in_chunks_does_not_repeat_consumed_quantity(self):
        self.start()
        partial, row = self.take(True)
        quantity = Decimal(row['wire_order']['s'])
        self.venue.fill(partial, str(quantity/2))
        self.restart(); self.run_cycles()
        self.assertEqual(self.take(True)[0], partial)
        self.assert_protected()
        self.venue.fill(partial, str(quantity/2))
        self.run_cycles()
        self.assertEqual(len(self.active('TAKE_PROFIT')), 1)
        self.assert_protected()

    def test_stop_first_cancels_both_profit_orders(self):
        trade = self.start('SHORT')
        self.venue.fill(self.venue.oid('STOP'), trade['quantity'])
        self.restart(); self.run_cycles()
        self.assertEqual(self.trade()['phase'], 'CLOSED')
        self.assertFalse(self.active())

    def test_partial_take_and_remaining_stop_fill_reconcile_as_one_closed_trade(self):
        trade = self.start()
        partial, row = self.take(True)
        self.venue.fill(partial, row['wire_order']['s'])
        remainder = Decimal(trade['quantity'])-Decimal(row['wire_order']['s'])
        self.venue.fill(self.venue.oid('STOP'), str(remainder))
        # A reduce-only stop's unfilled original size remains working until
        # explicitly canceled, even though this trade is now exchange-flat.
        self.run_cycles()
        self.assertEqual(self.trade()['phase'], 'CLOSED')
        self.assertFalse(self.active())

    def test_incremental_entry_fills_round_split_and_keep_all_quantity_protected(self):
        self.start(fill=False)
        entry = self.venue.oid('ENTRY')
        planned = Decimal(self.trade()['quantity'])
        self.venue.fill(entry, '.01')
        self.run_cycles()
        self.assertEqual(len(self.active('TAKE_PROFIT')), 1)
        self.assert_protected()
        self.venue.fill(entry, str(planned-Decimal('.01')))
        self.run_cycles()
        self.assertEqual(len(self.active('TAKE_PROFIT')), 2)
        self.assert_protected()

    def test_source_cancel_after_partial_entry_protects_and_retires_entry_remainder(self):
        self.start(fill=False)
        self.venue.fill(self.venue.oid('ENTRY'), '.37')
        canceled = deepcopy(self.msg)
        canceled.update(kind='CANCEL', source_state='CANCELED', cancel_reason='SOURCE_PARTIAL_TAKE',
            source_as_of=contract.iso_ms(self.venue.t), source_sequence=self.venue.t*10+3)
        self.worker.receive([contract.validate(canceled)])
        self.run_cycles()
        self.assertFalse(self.active('ENTRY'))
        self.assertEqual(runtime._remaining(self.trade()), Decimal('.37'))
        self.assertEqual(Decimal(self.take(True)[1]['wire_order']['s']), Decimal('.27'))
        self.assertEqual(Decimal(self.take(False)[1]['wire_order']['s']), Decimal('.10'))
        self.assert_protected()

    def test_deadline_closes_remainder_with_same_stop_and_no_new_entry(self):
        self.start('SHORT')
        partial, row = self.take(True)
        self.venue.fill(partial, row['wire_order']['s']); self.run_cycles()
        remainder = runtime._remaining(self.trade())
        self.venue.t = contract.moment_ms(self.msg['policy']['hold_until'])
        self.restart()
        result = self.worker.run_once(entries_enabled=False)
        self.assertEqual(result['operation'], 'TIME_EXIT')
        self.assertEqual(Decimal(self.venue.requests[-1]['proposal']['quantity']), remainder)
        self.run_cycles()
        self.assertEqual(self.trade()['phase'], 'CLOSED')
        self.assertFalse(self.active())

    def test_deadline_cancels_unfilled_entry_before_closing_partial_entry(self):
        self.start(fill=False)
        self.venue.fill(self.venue.oid('ENTRY'), '.37'); self.run_cycles()
        self.venue.t = contract.moment_ms(self.msg['policy']['hold_until'])
        result = self.worker.run_once(entries_enabled=False)
        self.assertEqual(result['operation'], 'CANCEL')
        result = self.worker.run_once(entries_enabled=False)
        self.assertEqual(result['operation'], 'TIME_EXIT')
        self.assertEqual(Decimal(self.venue.requests[-1]['proposal']['quantity']), Decimal('.37'))
        self.run_cycles()
        self.assertEqual(self.trade()['phase'], 'CLOSED')

    def test_lost_timeout_reply_restarts_without_replaying_ioc(self):
        self.start()
        self.venue.t = contract.moment_ms(self.msg['policy']['hold_until'])
        self.venue.lose_reply = True
        result = self.worker.run_once(entries_enabled=False)
        self.assertEqual(result['status'], 'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.restart(); self.run_cycles()
        self.assertEqual(sum(r['proposal']['operation']=='TIME_EXIT' for r in self.venue.requests), 1)
        self.assertEqual(self.trade()['phase'], 'CLOSED')

    def test_timeout_partial_ioc_retries_only_remaining_proven_quantity(self):
        self.start(); self.venue.instant_fraction=Decimal('.5')
        self.venue.t = contract.moment_ms(self.msg['policy']['hold_until'])
        initial = runtime._remaining(self.trade())
        self.worker.run_once(entries_enabled=False)
        self.venue.instant_fraction=Decimal(1)
        self.restart(); self.worker.run_once(entries_enabled=False)
        attempts=[r for r in self.venue.requests if r['proposal']['operation']=='TIME_EXIT']
        self.assertEqual(len(attempts),2)
        self.assertEqual(Decimal(attempts[1]['proposal']['quantity']), initial/2)
        self.run_cycles(); self.assertEqual(self.trade()['phase'],'CLOSED')

    def test_missing_partial_target_after_cross_uses_only_partial_quantity(self):
        self.start(fill=False)
        self.venue.fill(self.venue.oid('ENTRY'), self.trade()['quantity'])
        self.worker.run_once(entries_enabled=False)  # Original full stop first.
        self.mark(self.msg['policy']['partial_take_price'])
        result=self.worker.run_once(entries_enabled=False)
        self.assertEqual(result['operation'],'PARTIAL_TAKE')
        request=self.venue.requests[-1]['proposal']
        self.assertEqual(request['action']['orders'][0]['p'],doge.partial_price(self.msg,2))
        self.assertLess(Decimal(request['quantity']),Decimal(self.trade()['quantity']))
        self.restart();self.run_cycles();self.assert_protected()
        self.assertEqual(len(self.active('TAKE_PROFIT')),1)

    def test_canceled_before_entry_cannot_open(self):
        self.msg=doge_partial_alert(kind='CANCEL',cancel_reason='SOURCE_PARTIAL_TAKE')
        self.worker.receive([self.msg]);self.worker.run_once()
        self.assertFalse(self.venue.requests)

    def test_unrepresentable_full_split_is_rejected_before_submission(self):
        self.msg=doge_partial_alert();self.worker.receive([self.msg])
        with patch.object(runtime,'budget',return_value=Decimal('.01')):
            with self.assertRaisesRegex(ValueError,'OUTSIDE_LAB_SIZE_BOUNDS|DOGE_SPLIT_QUANTITY_UNREPRESENTABLE'):
                self.worker.run_once()
        self.assertFalse(self.venue.requests)


if __name__=='__main__':
    unittest.main()
