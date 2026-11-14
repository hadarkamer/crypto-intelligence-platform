"""Four fixed formula adapters through the existing worker and durable journal.

No new trading simulator: these tests reuse the product's controlled exchange
and optional disposable PostgreSQL contract. No live transport/signing allowed.
"""
from copy import deepcopy
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import selected_formula_alert
from . import experimental_execution_dispatch as boundary
from . import experimental_execution_prices as prices
from . import test_experimental_live_runtime as support
from . import test_experimental_execution_runtime as exchange_support
from .test_approved_alert_lifecycle import cancellation

T = support.T
SYMBOLS = ('BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'HYPE')


class SelectedContractTests(unittest.TestCase):
    def test_six_unique_formula_ids_and_ct009_six_operational_symbols(self):
        self.assertEqual(len({rule for _, rule, _ in contract.NEW_ENTRY_IDENTITIES}), 6)
        self.assertEqual({s for f, _, s in contract.NEW_ENTRY_IDENTITIES if f == 'ct009'}, set(SYMBOLS))
        for symbol in SYMBOLS:
            self.assertTrue(contract.new_entry_allowed(selected_formula_alert('ct009', symbol=symbol)))
        for symbol in ('BNB', 'ZEC'):
            msg = selected_formula_alert('ct009', symbol=symbol)
            self.assertFalse(contract.new_entry_allowed(msg))
            current = dict(source=msg, plan_digest=contract.plan_digest(msg),
                           entry_permission='WAITING', cancellation=None)
            with self.assertRaisesRegex(boundary.BoundaryError, 'FORMULA_NEW_ENTRIES_DISABLED'):
                boundary._source(msg, current, contract.moment_ms(msg['approved_at']), entry=True)
            # Historical protection and cancellation remain readable.
            boundary._source(msg, current, contract.moment_ms(msg['approved_at']), entry=False)
        with self.assertRaisesRegex(contract.ContractError, 'APPROVED_ALERT_IDENTITY'):
            selected_formula_alert('ct009', symbol='UNKNOWN')

    def test_frozen_prices_availability_and_cancel_shape_cannot_change(self):
        for family in contract.SELECTED_FORMULAS:
            msg = selected_formula_alert(family)
            for field in ('entry', 'stop', 'take_profit'):
                changed = deepcopy(msg); changed[field] = str(Decimal(changed[field]) + Decimal('.01'))
                with self.assertRaisesRegex(contract.ContractError, 'SELECTED_FORMULA_FROZEN_PRICES'):
                    contract.validate(changed)
            future = deepcopy(msg)
            future['proof']['data_available_at'] = contract.iso_ms(contract.moment_ms(msg['approved_at']) + 1)
            with self.assertRaisesRegex(contract.ContractError, 'SELECTED_FORMULA_AVAILABILITY'):
                contract.validate(future)
            canceled = cancellation(msg, contract.moment_ms(msg['approved_at']) + 5)
            self.assertEqual(canceled['occurrence_id'], msg['occurrence_id'])
            canceled['source_state'] = 'APPROVED'
            with self.assertRaisesRegex(contract.ContractError, 'APPROVED_ALERT_CANCEL_STATE'):
                contract.validate(canceled)

    def test_admission_ttl_order_expiry_and_holding_are_distinct(self):
        for family in contract.PENDING_FAMILIES:
            msg = selected_formula_alert(family)
            self.assertEqual(contract.moment_ms(msg['expires_at']) - contract.moment_ms(msg['approved_at']), 90000)
            self.assertEqual(contract.pending_entry_deadline(msg) - contract.moment_ms(msg['source_at']), 86400000)
            changed = deepcopy(msg)
            changed['policy']['pending_expires_at'] = msg['expires_at']
            with self.assertRaisesRegex(contract.ContractError, 'SELECTED_FORMULA_PENDING_TERMS'):
                contract.validate(changed)
        self.assertIsNone(contract.pending_entry_deadline(selected_formula_alert('sol_sff1361')))

    def test_inverse_target_is_reflected_around_reference_not_entry(self):
        msg = selected_formula_alert('xrp_liquidity_inverse')
        self.assertEqual(msg['original_target'], '98')
        changed = deepcopy(msg); changed['original_target'] = str(2 * Decimal(msg['entry']) - Decimal('102'))
        with self.assertRaisesRegex(contract.ContractError, 'SELECTED_FORMULA_TARGET'):
            contract.validate(changed)

    def test_pending_tp_is_not_the_prefill_cancel_barrier(self):
        msg = selected_formula_alert('ct009')
        midpoint = (Decimal(msg['original_target']) + Decimal(msg['take_profit'])) / 2
        levels = {k: msg[k] for k in ('entry', 'stop', 'take_profit')}
        self.assertTrue(prices.entry_mark_allowed(msg, levels, midpoint))
        self.assertFalse(prices.entry_mark_allowed(msg, levels, msg['original_target']))
        self.assertFalse(prices.entry_mark_allowed(msg, levels, msg['stop']))


class SelectedRuntimeContract:
    real_pg = False
    make_worker = support.RuntimeContract.make_worker
    cycle = support.RuntimeContract.cycle
    restart = support.RuntimeContract.restart

    def setUp(self):
        support.RuntimeContract.setUp(self)
        metadata = {'universe': [dict(name=s, szDecimals=2, maxLeverage=10)
                                  for s in (*SYMBOLS, 'BNB', 'ZEC')]}
        p = patch.object(exchange_support, 'META', metadata); p.start(); self.addCleanup(p.stop)

    def start_selected(self, family='ct009', symbol='XRP', cycle='selected-runtime'):
        msg = selected_formula_alert(family, symbol=symbol, reference_ms=T,
                                    approved_ms=T + 5000, cycle=cycle)
        self.venue.mark[self.venue._key(self.store.load()['routes']['short_account'], symbol)] = '100'
        self.worker.receive([msg]); self.cycle()
        return msg

    def test_every_ct009_authorized_symbol_enters_and_gets_correct_exits(self):
        for symbol in SYMBOLS:
            with self.subTest(symbol=symbol):
                self.venue = exchange_support.SoftwareExchange(T + 10000)
                self.make_worker(not_before=T - 60000)
                msg = self.start_selected(symbol=symbol, cycle='ct009-' + symbol)
                entry = self.venue.oid('ENTRY')
                self.venue.fill(entry, self.venue.orders[entry]['view']['wire_order']['s'])
                for _ in range(3): self.cycle(False)
                operations = [r['proposal']['leg'] for r in self.venue.requests]
                self.assertEqual(operations, ['ENTRY', 'STOP', 'TAKE_PROFIT'])
                trade = self.store.load()['trades'][msg['occurrence_id']]
                self.assertEqual(trade['role'], 'short_account')
                self.assertEqual(trade['source']['entry'], msg['entry'])
                self.assertEqual(Decimal(trade['entry_risk']['planned_usd']), Decimal('2'))

    def test_direct_disallowed_ct009_records_never_create_entry(self):
        for symbol in ('BNB', 'ZEC'):
            msg = self.start_selected(symbol=symbol, cycle='denied-' + symbol)
            self.assertEqual(self.store.load()['entry_decisions'][msg['occurrence_id']]['reason'],
                             'FORMULA_NEW_ENTRIES_DISABLED')
        self.assertEqual(self.venue.requests, [])
        self.restart(); self.cycle()
        self.assertEqual(self.venue.requests, [])
        with self.assertRaises(contract.ContractError):
            self.worker.receive([selected_formula_alert('ct009', symbol='UNKNOWN', reference_ms=T)])

    def test_each_new_formula_has_one_entry_and_restart_does_not_repeat(self):
        for family in contract.SELECTED_FORMULAS:
            with self.subTest(family=family):
                self.venue = exchange_support.SoftwareExchange(T + 10000)
                self.make_worker(not_before=T - 60000)
                msg = self.start_selected(family, symbol='SOL' if family == 'sol_sff1361' else 'XRP', cycle=family)
                self.worker.receive([msg, msg]); self.restart()
                for _ in range(2): self.cycle()
                self.assertEqual([r['proposal']['operation'] for r in self.venue.requests], ['ENTRY'])

    def test_pending_target_cancels_without_waiting_for_90_second_ttl(self):
        msg = self.start_selected()
        target = Decimal(msg['original_target'])
        key = self.venue._key(self.store.load()['routes']['short_account'], 'XRP')
        self.venue.mark[key] = '98.05'
        self.cycle(False)
        self.assertEqual(len(self.venue.requests), 1)
        self.venue.mark[key] = str(target)
        self.cycle(False)
        self.assertEqual(self.venue.requests[-1]['proposal']['operation'], 'CANCEL')
        self.assertEqual(len(self.venue.requests), 2)

    def test_pending_24_hour_deadline_survives_restart(self):
        msg = self.start_selected()
        self.venue.t = T + 90000 + 6000
        self.restart(); self.cycle(False)
        self.assertEqual(len(self.venue.requests), 1)
        self.venue.t = T + 86400000
        self.cycle(False)
        self.assertEqual(self.venue.requests[-1]['proposal']['operation'], 'CANCEL')

    def test_lost_reply_and_partial_fill_protect_before_pending_expiry_cancel(self):
        self.venue.lose_reply = True
        msg = self.start_selected()
        self.restart()
        for _ in range(3): self.cycle(False)
        self.assertEqual(len(self.venue.requests), 1)
        entry = self.venue.oid('ENTRY')
        quantity = str((Decimal(self.venue.orders[entry]['view']['wire_order']['s']) / 2).quantize(Decimal('.01')))
        self.venue.fill(entry, quantity)
        self.venue.t = T + 86400000
        for _ in range(4): self.cycle(False)
        self.assertEqual([r['proposal']['leg'] for r in self.venue.requests[:3]], ['ENTRY', 'STOP', 'TAKE_PROFIT'])
        self.assertEqual(self.venue.requests[-1]['proposal']['operation'], 'CANCEL')
        exits = [r for r in self.venue.requests if r['proposal']['operation'] == 'CREATE_EXIT']
        self.assertEqual([Decimal(r['proposal']['quantity']) for r in exits], [Decimal(quantity), Decimal(quantity)])
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY' for r in self.venue.requests), 1)


class SelectedRuntimeTests(SelectedRuntimeContract, unittest.TestCase):
    pass


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'), 'Disposable PostgreSQL required')
class SelectedPostgresTests(SelectedRuntimeContract, unittest.TestCase):
    real_pg = True
