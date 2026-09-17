"""Offline normalized exchange fixtures only; no wallets, HTTP or real orders."""
from copy import deepcopy
import inspect
import json
import unittest
from unittest.mock import patch
from . import card_lifecycle as m

A='0x'+'1'*40
B='0x'+'2'*40
T=1789646400000


def binding(n=1,side='LONG',account=None,qty='100'):
    account=account or (A if side=='LONG' else B)
    return dict(card_id=f'{n:064x}',card_digest='f'*64,account=account,
        role='long_account' if side=='LONG' else 'short_account',symbol='DOGE',side=side,
        planned_quantity=qty,prices=dict(entry='100',stop='98' if side=='LONG' else '102',take_profit='104' if side=='LONG' else '96'),
        orders={leg:[str(n*10+i)] for i,leg in enumerate(m.LEGS)},environment='testnet')


def fill(b,leg='ENTRY',qty=None,px=None,fid=None,fee='0.1'):
    entry=leg=='ENTRY'
    return dict(account=b['account'],symbol=b['symbol'],oid=b['orders'][leg][0],
        fill_id=fid or str(int(b['orders'][leg][0])*100),quantity=qty or b['planned_quantity'],
        price=px or b['prices']['entry' if entry else 'stop' if leg=='STOP' else 'take_profit'],
        fee=fee,fee_token='USDC',side=('B' if b['side']=='LONG' else 'A') if entry else ('A' if b['side']=='LONG' else 'B'),at_ms=T-10000 if entry else T-1000)


def order(b,leg,qty=None,state='ACTIVE'):
    entry=leg=='ENTRY'
    p=b['prices']['entry' if entry else 'stop' if leg=='STOP' else 'take_profit']
    return dict(account=b['account'],symbol=b['symbol'],oid=b['orders'][leg][0],quantity=qty or b['planned_quantity'],
        price=p,trigger_price=None if entry else p,side=('B' if b['side']=='LONG' else 'A') if entry else ('A' if b['side']=='LONG' else 'B'),
        reduce_only=not entry,state=state,order_type='LIMIT' if entry else 'SL_MARKET' if leg=='STOP' else 'TP_LIMIT')


def terminal(b,leg,qty=None):
    return dict(account=b['account'],symbol=b['symbol'],oid=b['orders'][leg][0],
        state='CANCELED' if qty=='0' else 'FILLED',filled_quantity=qty or b['planned_quantity'],at_ms=T-500)


def snapshot(b,*,fills=None,opens=None,terms=None,position='0'):
    return dict(environment='testnet',account=b['account'],symbol=b['symbol'],at_ms=T,
        history_complete=True,orders_complete=True,position_quantity=position,
        fills=fills or [],open_orders=opens or [],terminal_orders=terms or [])


def opened(b):
    return snapshot(b,fills=[fill(b)],opens=[order(b,'STOP'),order(b,'TAKE_PROFIT')],terms=[terminal(b,'ENTRY')],position=('-' if b['side']=='SHORT' else '')+b['planned_quantity'])


def closed(b,leg='TAKE_PROFIT'):
    other='STOP' if leg=='TAKE_PROFIT' else 'TAKE_PROFIT'
    return snapshot(b,fills=[fill(b),fill(b,leg)],terms=[terminal(b,'ENTRY'),terminal(b,leg),terminal(b,other,'0')])


def run(b,s,**kw):
    return m.review(b if isinstance(b,list) else [b],s,now_ms=T,**kw)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        p=patch('socket.socket',side_effect=AssertionError('NETWORK_FORBIDDEN'))
        p.start();self.addCleanup(p.stop)

    def test_open_closed_both_directions_and_exit_types(self):
        for side in ('LONG','SHORT'):
            b=binding(side=side)
            self.assertEqual(run(b,opened(b))['cards'][0]['state'],'OPEN')
            for leg in ('STOP','TAKE_PROFIT'):
                r=run(b,closed(b,leg));v=r['cards'][0]
                self.assertFalse(r['needs_review']);self.assertTrue(v['closure_verified'])
                self.assertEqual(v['net_before_funding_usdc'],'399.8' if leg=='TAKE_PROFIT' else '-200.2')
                self.assertIsNone(v['funding_usdc']);self.assertIsNone(v['final_net_usdc'])

    def test_one_card_closes_other_card_remains_unchanged(self):
        a,b=binding(),binding(2,qty='60')
        b['prices']=dict(entry='110',stop='108',take_profit='116')
        s=closed(a);ob=opened(b)
        s['fills']+=ob['fills'];s['open_orders']+=ob['open_orders'];s['terminal_orders']+=ob['terminal_orders'];s['position_quantity']='60'
        result=run([a,b],s)
        self.assertTrue(result['cards'][0]['closure_verified'])
        self.assertEqual(result['cards'][1],run(b,ob)['cards'][0])
        self.assertFalse(result['needs_review'])

    def test_opposite_accounts_not_netted(self):
        a,b=binding(),binding(2,side='SHORT')
        for x in (a,b):
            r=run([a,b],opened(x));self.assertEqual(len(r['cards']),1)
            self.assertEqual(r['cards'][0]['card_id'],x['card_id']);self.assertFalse(r['needs_review'])

    def test_opposite_roles_same_account_rejected(self):
        with self.assertRaises(m.LifecycleError):run([binding(),binding(2,side='SHORT',account=A)],opened(binding()))

    def test_duplicate_fill_is_not_double_counted(self):
        b=binding();s=closed(b);s['fills']*=3
        self.assertEqual(run(b,s),run(b,closed(b)))

    def test_duplicate_fill_with_changed_data_rejected(self):
        b=binding();s=opened(b);s['fills'].append({**s['fills'][0],'quantity':'101'})
        with self.assertRaises(m.LifecycleError):run(b,s)

    def test_out_of_order_delivery_same_result(self):
        b=binding();s=closed(b);s['fills'].reverse();s['terminal_orders'].reverse()
        self.assertEqual(run(b,s),run(b,closed(b)))

    def test_same_oid_cannot_belong_to_two_cards_or_legs(self):
        a,b=binding(),binding(2);b['orders']['STOP']=a['orders']['ENTRY']
        with self.assertRaises(m.LifecycleError):run([a,b],opened(a))
        a['orders']['STOP']=a['orders']['ENTRY']
        with self.assertRaises(m.LifecycleError):run(a,opened(a))

    def test_same_oid_on_different_accounts_is_allowed(self):
        a,b=binding(),binding(2,side='SHORT');b['orders']=deepcopy(a['orders'])
        self.assertFalse(run([a,b],opened(a))['needs_review'])

    def test_fifty_same_symbol_cards_no_arbitrary_trade_cap(self):
        bs=[binding(n) for n in range(1,51)];s=snapshot(bs[0],position='5000')
        for b in bs:
            o=opened(b)
            for key in ('fills','open_orders','terminal_orders'):s[key]+=o[key]
        r=run(bs,s);self.assertEqual(len(r['cards']),50);self.assertFalse(r['needs_review'])

    def test_partial_entry_with_waiting_children_has_no_assumed_stop(self):
        b=binding();s=snapshot(b,position='30',fills=[fill(b,qty='30')],opens=[order(b,'ENTRY','70'),order(b,'STOP',state='WAITING_PARENT'),order(b,'TAKE_PROFIT',state='WAITING_PARENT')])
        v=run(b,s)['cards'][0]
        self.assertEqual(v['state'],'PARTIALLY_OPEN');self.assertIn('STOP_COVERAGE_MISSING',v['issues'])
        self.assertEqual(v['stop_quantity_observed'],'0');self.assertFalse(v['closure_verified'])

    def test_partial_entry_separate_correct_sized_exits_observed(self):
        b=binding();s=snapshot(b,position='30',fills=[fill(b,qty='30')],opens=[order(b,'ENTRY','70'),order(b,'STOP','30'),order(b,'TAKE_PROFIT','30')])
        self.assertFalse(run(b,s)['needs_review'])

    def test_partial_close_requires_remainder_exits(self):
        b=binding();s=opened(b);s['fills'].append(fill(b,'TAKE_PROFIT',qty='40'));s['position_quantity']='60'
        r=run(b,s);self.assertIn('STOP_EXCEEDS_CARD_REMAINDER',r['cards'][0]['issues'])
        for o in s['open_orders']:o['quantity']='60'
        r=run(b,s);self.assertEqual(r['cards'][0]['state'],'PARTIALLY_CLOSED');self.assertFalse(r['needs_review'])

    def test_orphan_stop_after_close_detected_without_touching_other_card(self):
        a,b=binding(),binding(2,qty='60');s=closed(a);s['terminal_orders']=[t for t in s['terminal_orders'] if t['oid']!=a['orders']['STOP'][0]];s['open_orders']=[order(a,'STOP')]
        ob=opened(b)
        for k in ('fills','open_orders','terminal_orders'):s[k]+=ob[k]
        s['position_quantity']='60';r=run([a,b],s)
        self.assertEqual(r['cards'][0]['leftover_order_ids'],a['orders']['STOP'])
        self.assertFalse(r['cards'][0]['closure_verified']);self.assertEqual(r['cards'][1]['remaining_quantity'],'60')
        self.assertTrue(r['needs_review']);self.assertEqual(r['order_requests_sent'],0)

    def test_empty_exchange_position_is_not_proof_of_closed_cards(self):
        b=binding();s=opened(b);s['position_quantity']='0';r=run(b,s)
        self.assertIn('POSITION_DOES_NOT_MATCH_CARDS',r['bucket_issues']);self.assertEqual(r['cards'][0]['remaining_quantity'],'100')

    def test_overclose_not_borrowed_from_another_card(self):
        a,b=binding(),binding(2);s=closed(a);s['fills'][-1]['quantity']='110';s['terminal_orders'][1]['filled_quantity']='110'
        for k in ('fills','open_orders','terminal_orders'):s[k]+=opened(b)[k]
        s['position_quantity']='90';r=run([a,b],s)
        self.assertIn('EXIT_EXCEEDS_CARD_QUANTITY',r['cards'][0]['issues']);self.assertEqual(r['cards'][1]['remaining_quantity'],'100')

    def test_acknowledgement_without_fills_is_not_execution(self):
        b=binding();s=snapshot(b,terms=closed(b)['terminal_orders']);r=run(b,s)
        self.assertFalse(r['cards'][0]['closure_verified']);self.assertIn('TERMINAL_FILL_TOTAL_MISMATCH',r['cards'][0]['issues'])

    def test_entry_canceled_without_fill_is_not_win_or_loss(self):
        b=binding();s=snapshot(b,terms=[terminal(b,leg,'0') for leg in m.LEGS]);v=run(b,s)['cards'][0]
        self.assertEqual(v['state'],'CANCELED_WITHOUT_FILL');self.assertIsNone(v['net_before_funding_usdc'])

    def test_flat_with_remaining_entry_is_not_closed(self):
        b=binding();s=closed(b)
        s['fills'][0]['quantity']=s['fills'][1]['quantity']='30'
        s['terminal_orders']=[terminal(b,'TAKE_PROFIT','30'),terminal(b,'STOP','0')];s['open_orders']=[order(b,'ENTRY','70')]
        v=run(b,s)['cards'][0];self.assertEqual(v['state'],'FLAT_AWAITING_FINALITY');self.assertIsNone(v['net_before_funding_usdc'])

    def test_late_fill_on_canceled_entry_is_recorded_not_ignored(self):
        b=binding();s=snapshot(b,position='30',fills=[fill(b,qty='30')],terms=[terminal(b,leg,'0') for leg in m.LEGS]);v=run(b,s)['cards'][0]
        self.assertEqual(v['remaining_quantity'],'30');self.assertIn('TERMINAL_FILL_TOTAL_MISMATCH',v['issues']);self.assertIn('STOP_COVERAGE_MISSING',v['issues'])

    def test_both_exit_legs_race_requires_review(self):
        b=binding();s=closed(b);s['fills'][-1]['quantity']='50';s['fills'].append(fill(b,'STOP',qty='50'));s['terminal_orders']=[terminal(b,'ENTRY'),terminal(b,'TAKE_PROFIT','50'),terminal(b,'STOP','50')]
        self.assertIn('BOTH_EXIT_LEGS_FILLED_REVIEW',run(b,s)['cards'][0]['issues'])

    def test_missing_history_order_inventory_or_stale_report_not_final(self):
        b=binding()
        for key,value in [('history_complete',False),('orders_complete',False),('at_ms',T-16000),('at_ms',T+1)]:
            s=closed(b);s[key]=value
            # Keep historical evidence older than a stale snapshot.
            if key=='at_ms' and value<T:
                for k in ('fills','terminal_orders'):
                    for row in s[k]:row['at_ms']=value-1
            r=run(b,s);self.assertTrue(r['needs_review']);self.assertFalse(r['cards'][0]['closure_verified'])

    def test_future_fill_rejected(self):
        b=binding();s=closed(b);s['fills'][0]['at_ms']=T+1
        with self.assertRaises(m.LifecycleError):run(b,s)

    def test_unknown_manual_trade_or_liquidation_not_assigned_by_symbol(self):
        b=binding();s=closed(b);s['fills'].append({**fill(b),'oid':'999','fill_id':'unmatched'})
        r=run(b,s);self.assertIn('UNASSIGNED_EXCHANGE_ACTIVITY',r['bucket_issues']);self.assertFalse(r['cards'][0]['closure_verified'])

    def test_wrong_account_or_symbol_evidence_rejected(self):
        b=binding()
        for key,value in [('account',B),('symbol','BTC')]:
            s=opened(b);s['fills'][0][key]=value
            with self.assertRaises(m.LifecycleError):run(b,s)

    def test_wrong_side_reduce_only_price_or_type_not_protection(self):
        b=binding()
        for key,value in [('side','B'),('reduce_only',False),('trigger_price','97'),('order_type','TP_LIMIT')]:
            s=opened(b);s['open_orders'][0][key]=value;v=run(b,s)['cards'][0]
            self.assertIn('ORDER_TERMS_MISMATCH',v['issues']);self.assertIn('STOP_COVERAGE_MISSING',v['issues'])

    def test_wrong_fill_side_is_not_final(self):
        b=binding();s=closed(b);s['fills'][1]['side']='B';v=run(b,s)['cards'][0]
        self.assertIn('FILL_SIDE_MISMATCH',v['issues']);self.assertFalse(v['closure_verified'])

    def test_extra_stop_is_not_harmless(self):
        b=binding();b['orders']['STOP'].append('99');s=opened(b);s['open_orders'].append({**order(b,'STOP'),'oid':'99'})
        self.assertIn('STOP_EXCEEDS_CARD_REMAINDER',run(b,s)['cards'][0]['issues'])

    def test_missing_order_status_not_assumed_canceled(self):
        b=binding();s=closed(b);s['terminal_orders'].pop()
        v=run(b,s)['cards'][0];self.assertIn('ORDER_STATUS_UNRESOLVED',v['issues']);self.assertFalse(v['closure_verified'])

    def test_open_and_terminal_same_order_requires_review(self):
        b=binding();s=closed(b);s['open_orders']=[order(b,'STOP')]
        self.assertIn('ORDER_OPEN_AND_TERMINAL',run(b,s)['bucket_issues'])

    def test_fee_rebate_and_foreign_fee_handled_explicitly(self):
        b=binding();s=closed(b);s['fills'][0]['fee']='-0.1'
        self.assertEqual(run(b,s)['cards'][0]['net_before_funding_usdc'],'400.0')
        s['fills'][0]['fee_token']='HYPE';v=run(b,s)['cards'][0]
        self.assertIn('NON_USDC_FEE_REQUIRES_CONVERSION',v['issues']);self.assertIsNone(v['net_before_funding_usdc'])

    def test_closed_pnl_from_actual_card_cashflows_not_account_average(self):
        b=binding();s=closed(b);s['fills'][0]['price']='99';s['fills'][1]['price']='103'
        self.assertEqual(run(b,s)['cards'][0]['gross_pnl_usdc'],'400')

    def test_original_inputs_not_modified_and_no_app_send(self):
        b=binding();s=closed(b);before=deepcopy([b,s]);r=run(b,s)
        self.assertEqual(before,[b,s]);self.assertFalse(r['dispatch_enabled']);self.assertFalse(r['app_delivery_enabled'])

    def test_invalid_numbers_and_extra_fields_rejected(self):
        b=binding()
        for value in ('NaN','Infinity','-1','1e-1000',1.1,True):
            s=opened(b);s['fills'][0]['quantity']=value
            with self.assertRaises(m.LifecycleError):run(b,s)
        s=opened(b);s['secret']='not accepted'
        with self.assertRaises(m.LifecycleError):run(b,s)

    def test_mainnet_rejected_before_processing(self):
        b=binding();s=opened(b);s['environment']='mainnet'
        with self.assertRaises(m.LifecycleError):run(b,s)

    def test_remaining_entry_cannot_overfill_plan(self):
        b=binding();s=opened(b);s['open_orders'].append(order(b,'ENTRY','1'));s['terminal_orders']=[]
        self.assertIn('ENTRY_PENDING_EXCEEDS_PLAN',run(b,s)['cards'][0]['issues'])

    def test_module_has_no_transport_signing_or_startup(self):
        src=inspect.getsource(m)
        for forbidden in ('import requests','import http','import socket','import os','/exchange','submit_persisted','start_read_only'):
            self.assertNotIn(forbidden,src)


if __name__=='__main__':unittest.main(verbosity=2)
