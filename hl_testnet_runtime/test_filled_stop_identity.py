"""Terminal market-stop identity and independent fill proofs, with no I/O."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from . import card_sync_evidence as sync
from . import experimental_execution_evidence as evidence
from . import filled_quantity_dispatch as wire


T = 1792360600000
ACCOUNT = '0x' + '1' * 40
CID = 'a' * 64
OID = '578653942705'


def stop_case(*, buy=True):
    order = dict(a=5, b=buy, p='102.78', s='2.63', r=True,
        t=dict(trigger=dict(isMarket=True, triggerPx='102.78', tpsl='sl')),
        c='0x' + '3' * 32)
    request = dict(phase='OBSERVED', observed_oid=OID, attempt_at_ms=T, reply=None,
        proposal=dict(card_id=CID, account=ACCOUNT, symbol='SOL', leg='STOP',
            operation='CREATE_EXIT', action=dict(type='order', orders=[order], grouping='na')))
    raw = dict(status='order', order=dict(status='filled', statusTimestamp=T+30,
        order=dict(oid=int(OID), cloid=order['c'], coin='SOL', side='B' if buy else 'A',
            reduceOnly=True, origSz='2.63', sz='0', limitPx='113.05' if buy else '92.502',
            orderType='Stop Market', isPositionTpsl=False, isTrigger=False,
            triggerPx='0', timestamp=T)))
    return request, raw


class FilledStopIdentityTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.create_connection',
                       'socket.socket.connect', 'hl_testnet_runtime.two_account_execution.wallet_for_role'):
            guard = patch(target, side_effect=AssertionError('NO_NETWORK_OR_SIGNING'))
            guard.start(); self.addCleanup(guard.stop)

    def test_observed_market_stop_terminal_rewrite_for_both_sides(self):
        for buy in (True, False):
            with self.subTest(buy=buy):
                request, raw = stop_case(buy=buy)
                self.assertEqual(wire.identity(raw, request, T+100), OID)
                self.assertEqual(wire.requested_order(request['proposal']['action'])['p'], '102.78')

    def test_first_terminal_lookup_requires_matching_persisted_ack_oid(self):
        for phase in ('OUTCOME_UNKNOWN', 'ACK_UNVERIFIED'):
            with self.subTest(phase=phase):
                request, raw = stop_case()
                request.update(phase=phase, observed_oid=None,
                    reply=dict(state='ACCEPTED_UNVERIFIED', oid=OID))
                self.assertEqual(wire.identity(raw, request, T+100), OID)

    def test_unknown_or_conflicting_durable_binding_stays_blocked(self):
        for fields in (
            dict(phase='OUTCOME_UNKNOWN', observed_oid=None),
            dict(phase='OBSERVED', observed_oid=None),
            dict(observed_oid='999'),
            dict(observed_oid='999', reply=dict(state='ACCEPTED_UNVERIFIED', oid=OID)),
            dict(reply=dict(state='ACCEPTED_UNVERIFIED', oid='999')),
            dict(reply=dict(state='REJECTED', oid=OID)),
            dict(phase='ABORTED_UNSENT', reply=dict(state='ACCEPTED_UNVERIFIED', oid=OID)),
            dict(phase='PREPARED', reply=dict(state='ACCEPTED_UNVERIFIED', oid=OID)),
        ):
            with self.subTest(fields=fields):
                request, raw = stop_case(); request.update(fields)
                with self.assertRaises(ValueError): wire.identity(raw, request, T+100)

    def test_mutated_immutable_terms_and_invalid_execution_limit_stay_blocked(self):
        for fields in (
            dict(oid=999), dict(cloid='0x'+'4'*32), dict(coin='ETH'), dict(side='A'),
            dict(reduceOnly=False), dict(reduceOnly=1), dict(origSz='2.62'),
            dict(orderType='Limit'), dict(isPositionTpsl=True), dict(isTrigger=0),
            dict(triggerPx='102.78'), dict(sz='0.01'), dict(limitPx='0'),
            dict(limitPx='-1'), dict(limitPx='NaN'), dict(limitPx='Infinity'),
        ):
            with self.subTest(fields=fields):
                request, raw = stop_case(); raw['order']['order'].update(fields)
                with self.assertRaises(ValueError): wire.identity(raw, request, T+100)
        for stamp in (T-1, T+101):
            request, raw = stop_case(); raw['order']['statusTimestamp'] = stamp
            with self.assertRaises(ValueError): wire.identity(raw, request, T+100)

    def test_nonterminal_stop_and_other_order_types_gain_no_exception(self):
        for status in ('open', 'triggered', 'canceled', 'reduceOnlyCanceled', 'rejected'):
            with self.subTest(status=status):
                request, raw = stop_case(); raw['order']['status'] = status
                with self.assertRaises(ValueError): wire.identity(raw, request, T+100)
        for change in ('limit_stop', 'different_trigger', 'take', 'entry', 'immediate'):
            with self.subTest(change=change):
                request, raw = stop_case(); p = request['proposal']; order = wire.requested_order(p['action'])
                if change == 'limit_stop': order['t']['trigger']['isMarket'] = False
                elif change == 'different_trigger': order['t']['trigger']['triggerPx'] = '103'
                elif change == 'take':
                    p['leg'] = 'TAKE_PROFIT'; order['t']['trigger'].update(isMarket=False, tpsl='tp')
                    raw['order']['order']['orderType'] = 'Take Profit Limit'
                else:
                    p.update(leg='ENTRY' if change == 'entry' else 'STOP',
                        operation='ENTRY' if change == 'entry' else 'EMERGENCY_CLOSE')
                    order['t'] = dict(limit=dict(tif='Ioc'))
                    raw['order']['order']['orderType'] = 'Limit'
                with self.assertRaises(ValueError): wire.identity(raw, request, T+100)

    def test_open_stop_keeps_exact_price_and_trigger_checks(self):
        request, raw = stop_case(); raw['order']['status'] = 'open'
        raw['order']['order'].update(isTrigger=True, triggerPx='102.78', limitPx='102.78', sz='2.63')
        self.assertEqual(wire.identity(raw, request, T+100), OID)
        for fields in (dict(limitPx='113.05'), dict(triggerPx='103'), dict(isTrigger=False)):
            changed = deepcopy(raw); changed['order']['order'].update(fields)
            with self.assertRaises(ValueError): wire.identity(changed, request, T+100)

    def terminal_fixture(self):
        stop, raw_stop = stop_case()
        requests = {'STOP': stop}; responses = {OID: raw_stop}
        for leg, oid, price in (('ENTRY', '100', '102.02'), ('TAKE_PROFIT', '300', '100')):
            request = deepcopy(stop); p = request['proposal']; order = wire.requested_order(p['action'])
            request['observed_oid'] = oid; p.update(leg=leg, operation='ENTRY' if leg=='ENTRY' else 'CREATE_EXIT')
            order.update(b=leg!='ENTRY', r=leg!='ENTRY', p=price, c='0x'+('1' if leg=='ENTRY' else '2')*32,
                t=dict(limit=dict(tif='Gtc')) if leg=='ENTRY' else
                    dict(trigger=dict(isMarket=False, triggerPx=price, tpsl='tp')))
            raw = deepcopy(raw_stop); raw['order'].update(status='filled' if leg=='ENTRY' else 'reduceOnlyCanceled',
                statusTimestamp=T+10 if leg=='ENTRY' else T+30)
            raw['order']['order'].update(oid=int(oid), cloid=order['c'], side='A' if leg=='ENTRY' else 'B',
                reduceOnly=order['r'], limitPx=price, orderType='Limit' if leg=='ENTRY' else 'Take Profit Limit',
                isTrigger=leg!='ENTRY', triggerPx='0' if leg=='ENTRY' else price,
                sz='0' if leg=='ENTRY' else '2.63')
            requests[leg] = request; responses[oid] = raw
        binding = dict(card_id=CID, card_digest='b'*64, account=ACCOUNT, role='short_account',
            symbol='SOL', side='SHORT', planned_quantity='2.63', environment='mainnet',
            prices=dict(entry='102.02', stop='102.78', take_profit='100'),
            orders=dict(ENTRY=['100'], STOP=[OID], TAKE_PROFIT=['300']))
        previous = dict(environment='mainnet', account=ACCOUNT, symbol='SOL', at_ms=T,
            history_complete=True, orders_complete=True, position_quantity='0',
            fills=[], open_orders=[], terminal_orders=[])
        fills = [dict(coin='SOL', tid=1, oid=100, sz='2.63', px='102.02', fee='0', feeToken='USDC', side='A', time=T+10),
                 dict(coin='SOL', tid=2, oid=int(OID), sz='2.63', px='102.84', fee='0', feeToken='USDC', side='B', time=T+30)]
        class Reader:
            position = '0'
            def read(self, kind, account, *, oid=None, start=None, end=None):
                if kind=='orderStatus': return deepcopy(responses[oid])
                if kind=='userFillsByTime': return deepcopy(fills)
                if kind=='frontendOpenOrders': return []
                if kind=='clearinghouseState':
                    return dict(assetPositions=[] if self.position=='0' else [dict(position=dict(coin='SOL', szi=self.position))])
                raise AssertionError(kind)
        state = dict(routes=dict(short_account=ACCOUNT, long_account='0x'+'2'*40),
            requests=requests, trades={CID: dict(orders={})})
        lookups = {raw['order']['order']['cloid']: raw for raw in responses.values()}
        return state, binding, previous, Reader(), fills, lookups

    def test_complete_collector_proof_normalizes_original_intent_and_real_fills(self):
        state, binding, previous, reader, fills, lookups = self.terminal_fixture()
        result = sync.collect(dict(bindings=[binding], snapshot=previous), reader, clock=lambda:T+100)
        self.assertFalse(result['report']['bucket_issues'])
        self.assertEqual(result['report']['cards'][0]['state'], 'CLOSED')
        normalized = evidence.normalize_snapshot(state, result['snapshot'], lookups, now_ms=T+100)
        stop = next(row for row in normalized['orders'] if row['oid']==OID)
        self.assertEqual(stop['status'], 'FILLED')
        self.assertEqual(stop['wire_order']['p'], '102.78')
        self.assertEqual(stop['fills'][0]['price'], '102.84')
        self.assertEqual(stop['fills'][0]['quantity'], '2.63')

    def test_filled_label_without_complete_actual_fills_never_proves_finality(self):
        for size in (None, '1.00'):
            with self.subTest(size=size):
                state, binding, previous, reader, fills, lookups = self.terminal_fixture()
                if size is None: fills.pop()
                else: fills[-1]['sz'] = size
                with self.assertRaisesRegex(ValueError, 'FILLED_ORDER_HISTORY_INCOMPLETE'):
                    sync.collect(dict(bindings=[binding], snapshot=previous), reader, clock=lambda:T+100)

    def test_position_disagreement_and_missing_history_remain_unproven(self):
        state, binding, previous, reader, fills, lookups = self.terminal_fixture()
        reader.position = '-1'
        result = sync.collect(dict(bindings=[binding], snapshot=previous), reader, clock=lambda:T+100)
        self.assertIn('POSITION_DOES_NOT_MATCH_CARDS', result['report']['bucket_issues'])
        result['snapshot']['history_complete'] = False
        with self.assertRaisesRegex(ValueError, 'FRESH_COMPLETE_OWN_ACCOUNT_EVIDENCE_REQUIRED'):
            evidence.normalize_snapshot(state, result['snapshot'], lookups, now_ms=T+100)

    def test_normalizer_independently_rejects_missing_terminal_fill(self):
        state, binding, previous, reader, fills, lookups = self.terminal_fixture()
        result = sync.collect(dict(bindings=[binding], snapshot=previous), reader, clock=lambda:T+100)
        result['snapshot']['fills'] = [row for row in result['snapshot']['fills'] if row['oid']!=OID]
        with self.assertRaisesRegex(ValueError, 'TERMINAL_FILL_HISTORY_INCOMPLETE'):
            evidence.normalize_snapshot(state, result['snapshot'], lookups, now_ms=T+100)


if __name__ == '__main__':
    unittest.main()
