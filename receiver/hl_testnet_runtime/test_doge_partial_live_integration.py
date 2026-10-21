"""DOGE through authenticated intake, live adapters and synthetic raw venue data.

No production planner, signer boundary or collector is replaced. The shared
fixture blocks external sockets and supplies disposable account/order facts.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
import unittest

import approved_alert_contract as contract
from doge_test_fixtures import doge_partial_alert
from . import experimental_execution_runtime as core
from . import experimental_execution_archive as archive
from . import experimental_execution_cards as cards
from . import test_experimental_live_service as support


class DogePartialLiveIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.h = support.FullConnectionTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)
        self.h.oracle.t = support.T + 10000

    def trade(self, msg):
        return self.h.store.load()['trades'][msg['occurrence_id']]

    def tick(self, count=1):
        return [self.h.service.tick() for _ in range(count)]

    def working(self, msg, leg=None):
        trade = self.trade(msg)
        return {oid: order for oid, order in trade['orders'].items()
                if order['status'] == 'OPEN'
                and (leg is None or trade['order_legs'][oid] == leg)}

    def take(self, msg, *, partial):
        price = Decimal(msg['policy']['partial_take_price'] if partial else msg['take_profit'])
        matches = [(oid, order) for oid, order in self.working(msg, 'TAKE_PROFIT').items()
                   if Decimal(order['wire_order']['p']) == price]
        self.assertEqual(len(matches), 1, self.trade(msg))
        return matches[0]

    def start(self, side='LONG', *, fraction=Decimal(1), protect=True):
        h = self.h
        msg = doge_partial_alert(side=side, approved_ms=h.oracle.t//60000*60000,
                                 cycle_id='live-test-'+side+'-'+str(h.oracle.t))
        h.seed(msg)
        first = h.service.tick()
        self.assertEqual(first.get('operation'), 'ENTRY', (first, h.store.load()))
        trade = self.trade(msg)
        self.assertEqual(trade['role'], 'long_account' if side == 'LONG' else 'short_account')
        oid = h.oracle.oid('ENTRY')
        quantity = (Decimal(trade['quantity'])*fraction).quantize(Decimal('.01'), rounding=ROUND_DOWN)
        h.oracle.fill(oid, str(quantity))
        if protect:
            self.tick(5)
            self.assert_protected(msg, quantity)
        return msg, quantity

    def assert_protected(self, msg, quantity):
        self.assertEqual(core._remaining(self.trade(msg)), quantity)
        stops = list(self.working(msg, 'STOP').values())
        self.assertEqual(len(stops), 1, self.trade(msg))
        self.assertEqual(Decimal(stops[0]['wire_order']['p']), Decimal(msg['stop']))
        self.assertEqual(Decimal(stops[0]['wire_order']['s']), quantity)
        tp = list(self.working(msg, 'TAKE_PROFIT').values())
        self.assertEqual(sum((Decimal(o['wire_order']['s'])-sum(
            (Decimal(f['quantity']) for f in o['fills']), Decimal(0)) for o in tp), Decimal(0)), quantity)
        self.assertTrue(all(o['wire_order']['r'] is True for o in stops+tp))

    def finish_partial_then_final(self, msg, quantity):
        h = self.h
        partial_oid, partial_order = self.take(msg, partial=True)
        expected_partial = (quantity*Decimal('.75')).quantize(Decimal('.01'), rounding=ROUND_DOWN)
        self.assertEqual(Decimal(partial_order['wire_order']['s']), expected_partial)
        _, final_order = self.take(msg, partial=False)
        self.assertEqual(Decimal(final_order['wire_order']['s']), quantity-expected_partial)
        h.oracle.fill(partial_oid, str(expected_partial))
        self.tick(4)
        self.assertEqual(self.trade(msg)['phase'], 'PARTIALLY_CLOSED')
        self.assert_protected(msg, quantity-expected_partial)
        self.assertEqual(len(self.working(msg, 'TAKE_PROFIT')), 1)
        final_oid, _ = self.take(msg, partial=False)
        h.reconnect()
        self.tick(2)
        self.assertEqual(len(self.working(msg, 'TAKE_PROFIT')), 1)
        h.oracle.fill(final_oid, str(quantity-expected_partial))
        self.tick(5)
        self.assertEqual(self.trade(msg)['phase'], 'CLOSED')
        self.assertFalse(self.working(msg))
        self.assertEqual(core._remaining(self.trade(msg)), Decimal(0))
        card = next(c for c in h.worker.report()['cards'] if c['card_id']==msg['occurrence_id'])
        self.assertEqual(Decimal(card['summary']['filled_entry_quantity']), quantity)
        self.assertEqual(Decimal(card['summary']['filled_exit_quantity']), quantity)
        self.assertEqual(len(card['exit_fills']), 2)
        state = h.store.load()
        self.assertTrue(archive._eligible(state, msg['occurrence_id'], h.oracle.t))
        archived = archive._bundle(state, msg['occurrence_id'], h.oracle.t)
        self.assertEqual(cards.card_from_record(archived, domain='mainnet'), card)
        return card

    def test_long_complete_partial_then_final_and_next_alert(self):
        msg, quantity = self.start('LONG')
        card = self.finish_partial_then_final(msg, quantity)
        self.assertEqual(card['actual_exit_legs'], ['TAKE_PROFIT'])
        # Net settlement uses the existing formula ledger once, including both exits.
        trade = self.trade(msg)
        self.assertIn('settlement', trade, trade.get('accounting_pending'))
        balance = self.h.store.load()['formula_accounting']['portfolios'][msg['rule_id']]['balance_usd']
        self.h.reconnect(); self.tick(2)
        self.assertEqual(self.h.store.load()['formula_accounting']['portfolios'][msg['rule_id']]['balance_usd'], balance)
        self.h.oracle.t = (self.h.oracle.t//60000+1)*60000+10000
        next_msg, _ = self.start('LONG')
        self.assertNotEqual(msg['occurrence_id'], next_msg['occurrence_id'])
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.h.oracle.requests), 2)
        self.assertEqual(Decimal(self.trade(next_msg)['entry_risk']['planned_usd']), Decimal(balance)*Decimal('.05'))

    def test_short_complete_partial_then_final(self):
        msg, quantity = self.start('SHORT')
        card = self.finish_partial_then_final(msg, quantity)
        self.assertGreater(Decimal(card['summary']['gross_pnl_before_costs']), 0)

    def test_partial_entry_source_cancel_protects_only_actual_quantity(self):
        msg, quantity = self.start('LONG', fraction=Decimal('.5'), protect=False)
        cancel = deepcopy(msg)
        cancel.update(kind='CANCEL', source_state='CANCELED', cancel_reason='SOURCE_PARTIAL_TAKE_OBSERVED',
            source_as_of=contract.iso_ms(self.h.oracle.t), source_sequence=self.h.oracle.t*10+3)
        self.h.ingest(contract.validate(cancel))
        self.tick(7)
        self.assertFalse(self.working(msg, 'ENTRY'))
        self.assert_protected(msg, quantity)
        self.finish_partial_then_final(msg, quantity)

    def test_stop_first_closes_and_cleans_both_profit_orders(self):
        msg, quantity = self.start('SHORT')
        stop = next(iter(self.working(msg, 'STOP')))
        self.h.oracle.fill(stop, str(quantity))
        self.tick(6)
        self.assertEqual(self.trade(msg)['phase'], 'CLOSED')
        self.assertFalse(self.working(msg))
        self.assertEqual(len(self.trade(msg)['exit_fills']), 1)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.h.oracle.requests), 1)

    def test_partially_filled_partial_take_keeps_exact_split_and_original_stop(self):
        msg, quantity = self.start('SHORT')
        oid, order = self.take(msg, partial=True)
        target = Decimal(order['wire_order']['s'])
        filled = (target/2).quantize(Decimal('.01'), rounding=ROUND_DOWN)
        self.h.oracle.fill(oid, str(filled))
        self.tick(4)
        self.assert_protected(msg, quantity-filled)
        current_oid, current = self.take(msg, partial=True)
        self.assertEqual(current_oid, oid)
        self.assertEqual(Decimal(current['wire_order']['s']), target)
        self.assertEqual(sum(Decimal(f['quantity']) for f in current['fills']), filled)
        self.h.reconnect(); self.tick(2)
        self.h.oracle.fill(oid, str(target-filled))
        self.tick(4)
        self.assert_protected(msg, quantity-target)
        self.assertEqual(len(self.working(msg, 'TAKE_PROFIT')), 1)

    def test_lost_partial_take_ack_recovers_its_oid_without_duplicate_profit_order(self):
        msg, quantity = self.start('LONG', protect=False)
        first = self.h.service.tick()
        self.assertEqual(first.get('operation'), 'CREATE_EXIT')
        self.assertEqual(self.h.oracle.requests[-1]['proposal']['leg'], 'STOP')
        self.h.fail_reply = True
        unknown = self.h.service.tick()
        self.assertIn('UNKNOWN', unknown['status'])
        self.assertEqual(self.h.oracle.requests[-1]['proposal']['leg'], 'TAKE_PROFIT')
        self.h.reconnect(); self.tick(5)
        self.assert_protected(msg, quantity)
        self.assertEqual(len(self.working(msg, 'TAKE_PROFIT')), 2)
        self.assertEqual(sum(r['proposal']['leg']=='TAKE_PROFIT'
                             for r in self.h.oracle.requests), 2)

    def test_timeout_closes_remaining_after_partial_while_entries_halted(self):
        msg, quantity = self.start('LONG')
        partial_oid, partial = self.take(msg, partial=True)
        self.h.oracle.fill(partial_oid, partial['wire_order']['s'])
        self.tick(4)
        remaining = core._remaining(self.trade(msg))
        self.h.release['entries_enabled'] = False
        self.h.oracle.t = contract.moment_ms(msg['policy']['hold_until'])-1
        self.h.provider._market = None
        self.h.raw.calls.clear()
        self.tick()
        self.assertFalse(any(r['proposal']['operation']=='TIME_EXIT' for r in self.h.oracle.requests))
        self.assertNotIn('metaAndAssetCtxs', [row[0] for row in self.h.raw.calls])
        self.h.oracle.t += 1
        self.h.provider._market = None
        self.h.raw.calls.clear()
        result = self.h.service.tick()
        self.assertEqual(result.get('operation'), 'TIME_EXIT', (result, self.h.store.load()))
        self.assertIn('metaAndAssetCtxs', [row[0] for row in self.h.raw.calls])
        self.tick(5)
        self.assertEqual(self.trade(msg)['phase'], 'CLOSED')
        exits = [r for r in self.h.oracle.requests if r['proposal']['operation']=='TIME_EXIT']
        self.assertEqual(len(exits), 1)
        self.assertEqual(Decimal(exits[0]['proposal']['quantity']), remaining)
        self.assertFalse(self.working(msg))

    def test_lost_entry_ack_and_restart_do_not_duplicate_entry(self):
        h = self.h
        msg = doge_partial_alert(side='SHORT', approved_ms=support.T)
        h.seed(msg); h.fail_reply = True
        first = h.service.tick()
        self.assertIn('UNKNOWN', first['status'])
        h.reconnect(); self.tick(2)
        self.assertEqual(h.ingest(msg)['status'], 'DUPLICATE')
        h.oracle.fill(h.oracle.oid('ENTRY'), self.trade(msg)['quantity'])
        self.tick(5)
        self.assert_protected(msg, Decimal(self.trade(msg)['quantity']))
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in h.oracle.requests), 1)
        self.assertEqual(len(self.working(msg, 'TAKE_PROFIT')), 2)

    def test_crossed_partial_before_order_creation_reconciles_ioc_and_preserves_remainder(self):
        msg, quantity = self.start('LONG', protect=False)
        self.tick()  # Actual stop is installed before the partial-take action.
        trade = self.trade(msg)
        self.h.oracle.mark[core._lane(trade['account'], trade['symbol'])] = str(
            Decimal(msg['policy']['partial_take_price'])+Decimal('.1'))
        self.h.oracle.t += 5001
        self.h.provider._market = None
        result = self.h.service.tick()
        self.assertEqual(result.get('operation'), 'PARTIAL_TAKE', (result, self.h.store.load()))
        self.tick(5)
        remaining = quantity-(quantity*Decimal('.75')).quantize(Decimal('.01'), rounding=ROUND_DOWN)
        self.assert_protected(msg, remaining)
        self.assertEqual(self.trade(msg)['phase'], 'PARTIALLY_CLOSED')
        self.assertEqual(len(self.working(msg, 'TAKE_PROFIT')), 1)
        self.take(msg, partial=False)
        partials = [r for r in self.h.oracle.requests if r['proposal']['operation']=='PARTIAL_TAKE']
        self.assertEqual(len(partials), 1)
        self.assertEqual(partials[0]['proposal']['action']['orders'][0]['p'], msg['policy']['partial_take_price'])

    def test_fully_protected_doge_does_not_poll_marks_before_timeout(self):
        msg, _ = self.start('LONG')
        self.h.oracle.t += 5001
        self.h.provider._market = None
        self.h.raw.calls.clear()
        sends = len(self.h.http)
        self.tick(2)
        self.assertNotIn('metaAndAssetCtxs', [row[0] for row in self.h.raw.calls])
        self.assertEqual(len(self.h.http), sends)
        self.assertEqual(self.trade(msg)['phase'], 'OPEN')

    def test_expired_doge_alert_is_recorded_without_a_trade(self):
        h = self.h
        msg = doge_partial_alert(side='LONG', approved_ms=support.T)
        h.oracle.t = contract.moment_ms(msg['expires_at'])
        h.seed(msg); self.tick(2)
        self.assertNotIn(msg['occurrence_id'], h.store.load()['trades'])
        self.assertFalse(h.http)


if __name__ == '__main__':
    unittest.main()
