"""Real controller and PostgreSQL; ONLY venue I/O is substituted.

Never invoke real signing or HTTP. Database tests require the existing strictly
loopback CI DSN. Software rows cannot be loaded into the Testnet adapter.
"""
from copy import deepcopy
from contextlib import contextmanager, ExitStack
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import inspect
import json
import os
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch, Mock

from . import filled_quantity_dispatch as m, card_lifecycle as life, trade_cards
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore
from .test_filled_quantity_exits import original, case, META, ROUTES
from .test_card_lifecycle import T, A, B, terminal, order, fill

CI=os.environ.get('HL_JOURNAL_CI_URL')
AGENT='0x'+'3'*40
ROUTES2={k:{**v,'agent':AGENT} for k,v in ROUTES.items()}


def state_from_case(**kw):
    bs,s,c,originals=case(**kw)
    return dict(bucket=life.digest(['testnet',s['account'],s['symbol']]),account=s['account'],symbol=s['symbol'],
        revision=1,bindings=bs,originals=originals,pending=None,
        evidence=dict(bindings=bs,snapshot=s))


class Venue:
    """Deterministic external service double; no persistence or controller logic."""
    domain='software'
    def __init__(self):
        self.t=T;self.sent=0;self.orders={};self.fills=[];self.requests=[]
        self.lose_reply=False;self.cancel_race=None;self.calls=0;self.ioc_fill=None
    def now(self): return self.t
    def metadata(self): return META
    def sample(self,account,symbol): return dict(mark_price='10',at_ms=self.t)
    def empty_snapshot(self,account,symbol):
        return dict(environment='testnet',account=account,symbol=symbol,at_ms=self.t,
            history_complete=True,orders_complete=True,position_quantity='0',fills=[],open_orders=[],terminal_orders=[])
    def authorize(self,state,proposal,policy): pass
    def send(self,request):
        self.sent+=1;self.requests.append(deepcopy(request));self.t+=10
        p=request['proposal'];action=p['action']
        if action['type']=='cancel':
            oid=str(action['cancels'][0]['o'])
            if self.cancel_race:
                callback=self.cancel_race;self.cancel_race=None;callback(oid)
            if self.orders[oid]['status']=='open': self.orders[oid].update(status='canceled',statusTimestamp=self.t)
            reply=dict(status='ok',response=dict(type='cancel',data=dict(statuses=['success'])))
        else:
            o=m.requested_order(action);oid=str(1000+len(self.orders));leg=p['leg']
            if action['type']=='batchModify':
                prior=self.orders[str(action['modifies'][0]['oid'])]
                if prior['status']!='open':
                    return dict(status='ok',response=dict(type='order',data=dict(statuses=[dict(error='Cannot modify terminal order')])) )
                prior.update(status='canceled',statusTimestamp=self.t)
            immediate=p['operation']=='CLOSE_PASSED_TAKE'
            raw=dict(oid=int(oid),cloid=o['c'],coin=p['symbol'],side='B' if o['b'] else 'A',
                limitPx=o['p'],sz=o['s'],origSz=o['s'],reduceOnly=o['r'],isTrigger=leg!='ENTRY' and not immediate,
                triggerPx=o.get('t',{}).get('trigger',{}).get('triggerPx','0'),isPositionTpsl=False,
                orderType='Limit' if leg=='ENTRY' or immediate else 'Stop Market' if leg=='STOP' else 'Take Profit Limit')
            self.orders[oid]=dict(order=raw,status='open',statusTimestamp=self.t,account=p['account'])
            reply=dict(status='ok',response=dict(type='order',data=dict(statuses=[dict(resting=dict(oid=int(oid)))])))
            if immediate:
                quantity=o['s'] if self.ioc_fill is None else self.ioc_fill
                if Decimal(quantity)>0:self.fill(oid,quantity)
                if self.orders[oid]['status']=='open':
                    self.orders[oid].update(status='canceled',statusTimestamp=self.t)
                reply['response']['data']['statuses']=[dict(filled=dict(oid=int(oid)))]
        if self.lose_reply: self.lose_reply=False;raise TimeoutError()
        return reply
    def fill(self,oid,q):
        self.t+=10;row=self.orders[oid];o=row['order'];q=Decimal(q)
        left=Decimal(o['sz'])-q
        if left<0: raise AssertionError('DOUBLE_FILL')
        o['sz']=str(left)
        self.fills.append(dict(account=row['account'],coin=o['coin'],oid=int(oid),tid=len(self.fills)+1,
            time=self.t,sz=str(q),px=o['limitPx'],side=o['side'],fee='0.01',feeToken='USDC'))
        if left==0: row.update(status='filled',statusTimestamp=self.t)
    def lookup(self,account,cloid):
        rows=[r for r in self.orders.values() if r['account']==account and r['order']['cloid']==cloid]
        return dict(status='order',order=deepcopy(rows[0])) if rows else dict(status='unknownOid')
    def read(self,kind,account,*,oid=None,start=None,end=None):
        self.calls+=1
        if kind=='orderStatus':
            result=deepcopy(self.orders[oid])
            # Match the observed Testnet response: an open order's status can
            # retain its original size after frontendOpenOrders has shrunk.
            if result['status']=='open':
                result['order']['sz']=result['order']['origSz']
            return dict(status='order',order=result)
        if kind=='frontendOpenOrders': return [deepcopy(r['order']) for r in self.orders.values() if r['account']==account and r['status']=='open']
        if kind=='userFillsByTime': return [deepcopy(f) for f in self.fills if f['account']==account and start<=f['time']<=end]
        if kind=='clearinghouseState':
            q=sum((Decimal(f['sz'])*(1 if f['side']=='B' else -1) for f in self.fills if f['account']==account),Decimal(0))
            return dict(assetPositions=[dict(position=dict(coin='DOGE',szi=str(q)))])
        raise AssertionError(kind)
    def collect(self,value):
        return m.evidence.collect(value,self,clock=self.now,elapsed=lambda:0)


class NoExternal(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.create_connection',
                       'hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body',
                       'hl_testnet_runtime.two_account_execution.wallet_for_role'):
            guard=patch(target,side_effect=AssertionError('NO_NETWORK_OR_KEYS'))
            guard.start();self.addCleanup(guard.stop)


class DispatchPureTests(NoExternal):
    def select(self,s,**kw):
        return m.choose(s,ROUTES2,META,dict(mark_price='10',at_ms=T),now_ms=T,**kw)
    def further_partial(self,side='LONG'):
        state=state_from_case(q='40',side=side,stop='40',take='40')
        snap=state['evidence']['snapshot'];binding=state['bindings'][0]
        snap['fills'].append(fill(binding,qty='20',fid='later-partial-entry'))
        snap['position_quantity']='60' if side=='LONG' else '-60'
        next(o for o in snap['open_orders'] if o['oid'] in binding['orders']['ENTRY'])['quantity']='40'
        return state

    def test_crossed_take_allows_only_exact_owned_stop_resize_for_new_partial_fills_both_roles(self):
        for side,mark in (('LONG','10.3'),('SHORT','9.7')):
            with self.subTest(side=side):
                state=self.further_partial(side);before=deepcopy(state);binding=state['bindings'][0]
                proposal=m.choose(state,ROUTES2,META,dict(mark_price=mark,at_ms=T),now_ms=T,
                                  after_exit_policy=m.AFTER_EXIT)
                order=m.requested_order(proposal['action'])
                self.assertEqual((proposal['operation'],proposal['leg'],proposal['quantity']),('MODIFY_EXIT','STOP','60'))
                self.assertEqual(proposal['old_oid'],binding['orders']['STOP'][0])
                self.assertEqual((proposal['account'],proposal['role']),(binding['account'],binding['role']))
                self.assertEqual((order['p'],order['s'],order['r'],order['b']),
                                 (binding['prices']['stop'],'60',True,side=='SHORT'))
                self.assertEqual(order['t'],dict(trigger=dict(isMarket=True,
                    triggerPx=binding['prices']['stop'],tpsl='sl')))
                self.assertEqual(state,before)

    def test_crossed_take_stop_resize_keeps_ownership_freshness_and_quantity_fences(self):
        for invalid in ('stale','wrong-owner','wrong-stop-side','wrong-reduce-only','wrong-price','wrong-entry-total','wrong-position','crossed-stop'):
            with self.subTest(invalid=invalid):
                state=self.further_partial();snap=state['evidence']['snapshot'];binding=state['bindings'][0]
                stop=next(o for o in snap['open_orders'] if o['oid'] in binding['orders']['STOP'])
                now=T;mark='10.3'
                if invalid=='stale':now=T+15001
                elif invalid=='wrong-owner':state['originals'][binding['card_id']]['card']=original(7)[1]['card']
                elif invalid=='wrong-stop-side':stop['side']='B'
                elif invalid=='wrong-reduce-only':stop['reduce_only']=False
                elif invalid=='wrong-price':stop['trigger_price']='9.8'
                elif invalid=='wrong-entry-total':next(o for o in snap['open_orders'] if o['oid'] in binding['orders']['ENTRY'])['quantity']='60'
                elif invalid=='wrong-position':snap['position_quantity']='61'
                else:mark='9.8'
                with self.assertRaises(life.LifecycleError):
                    m.choose(state,ROUTES2,META,dict(mark_price=mark,at_ms=now),now_ms=now,
                             after_exit_policy=m.AFTER_EXIT)

    def test_partial_take_plus_later_entry_fill_resizes_only_stop_after_entry_remainder_final(self):
        for side,mark in (('LONG','10.3'),('SHORT','9.7')):
            with self.subTest(side=side):
                state=state_from_case(q='40',side=side,stop='40',take='40')
                snap=state['evidence']['snapshot'];binding=state['bindings'][0]
                snap['fills'] += [fill(binding,qty='40',fid='later-entry'),
                                  fill(binding,'TAKE_PROFIT',qty='20',fid='partial-take')]
                snap['open_orders']=[o for o in snap['open_orders'] if o['oid'] not in binding['orders']['ENTRY']]
                next(o for o in snap['open_orders'] if o['oid'] in binding['orders']['TAKE_PROFIT'])['quantity']='20'
                ending=terminal(binding,'ENTRY','80');ending['state']='CANCELED'
                snap['terminal_orders']=[ending];snap['position_quantity']='60' if side=='LONG' else '-60'
                p=m.choose(state,ROUTES2,META,dict(mark_price=mark,at_ms=T),now_ms=T,
                           after_exit_policy=m.AFTER_EXIT)
                self.assertEqual((p['operation'],p['leg'],p['quantity']),('MODIFY_EXIT','STOP','60'))
                self.assertEqual(p['old_oid'],binding['orders']['STOP'][0])

    def test_partial_take_shrinks_owned_stop_even_when_original_take_is_crossed(self):
        for side,mark in (('LONG','10.3'),('SHORT','9.7')):
            with self.subTest(side=side):
                state=state_from_case(q='100',side=side,stop='100',take='100')
                snap=state['evidence']['snapshot'];binding=state['bindings'][0]
                snap['fills'].append(fill(binding,'TAKE_PROFIT',qty='20',fid='partial-target'))
                next(o for o in snap['open_orders'] if o['oid'] in binding['orders']['TAKE_PROFIT'])['quantity']='80'
                snap['position_quantity']='80' if side=='LONG' else '-80'
                p=m.choose(state,ROUTES2,META,dict(mark_price=mark,at_ms=T),now_ms=T,
                           after_exit_policy=m.AFTER_EXIT)
                self.assertEqual((p['operation'],p['leg'],p['quantity']),('MODIFY_EXIT','STOP','80'))
                order=m.requested_order(p['action'])
                self.assertTrue(order['r'])
                self.assertEqual(order['p'],binding['prices']['stop'])
    def test_exact_short_trial_skips_older_unbound_card_in_same_market(self):
        older, chosen = 'a'*64, 'b'*64
        source_at=datetime.fromtimestamp((T-1000)/1000,timezone.utc).isoformat()
        original=lambda cid: dict(card=dict(prepared=dict(source=dict(at=source_at),
                                                       execution=dict(at=source_at))),
                                  draft=dict(role='short_account'))
        state=dict(account=A,symbol='DOGE',bucket='c'*64,revision=1,bindings=[],
                   originals={older:original(older),chosen:original(chosen)},
                   evidence=dict(snapshot=dict(at_ms=T)))
        draft=dict(entry_action=dict(type='order',orders=[dict(a=0)]),
                   size_decimals=2,prices=dict(stop='9',take_profit='11'),planned_quantity='1')
        with patch.object(m.selected,'validate_draft',return_value=draft):
            proposal=self.select(state,allowed_entry_card_id=chosen)
        self.assertEqual(proposal['card_id'],chosen)
        self.assertEqual(proposal['operation'],'ENTRY')

    def test_exact_short_trial_rejects_any_other_entry_at_final_gate(self):
        env=dict(HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1',
                 HL_TESTNET_FILLED_DISPATCH='approved_long_stream_v1',
                 HL_TESTNET_LONG_STREAM='approved_alerts_v1',
                 HL_TESTNET_SHORT_STREAM='approved_alerts_v1',
                 HL_TESTNET_SHORT_ENTRY_ENABLED='true',
                 HL_TESTNET_SHORT_NOT_BEFORE='2026-09-26T00:00:00+00:00',
                 HL_TESTNET_SHORT_TRIAL_CARD_ID='a'*64,
                 HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
                 HL_TESTNET_FILLED_AFTER_EXIT_POLICY=m.AFTER_EXIT,
                 RENDER_SERVICE_ID=m.roles.SERVICE)
        venue=m.TestnetVenue(env)
        proposal=dict(card_id='b'*64,role='short_account',account=B,
                      operation='ENTRY',source_at='2026-09-26T14:00:00+00:00')
        with self.assertRaisesRegex(DispatchError,'OUTSIDE_EXACT_TRIAL_CARD'):
            venue._gate(proposal,m.AFTER_EXIT)
    def test_first_stream_entry_has_no_predecessor_lifecycle_to_review(self):
        cid='b'*64
        at=datetime.fromtimestamp((T-1000)/1000,timezone.utc).isoformat()
        expires=datetime.fromtimestamp((T+60000)/1000,timezone.utc).isoformat()
        card=dict(record_kind='received_alert',account_role='short_account',
            source_expires_at=expires,prepared=dict(execution=dict(
                symbol='DOGE',side='SHORT',entry='10',stop='11',
                take_profit='9',at=at)))
        state=dict(bindings=[],originals={cid:dict(card=card)},
                   evidence=dict(snapshot=dict()))
        proposal=dict(card_id=cid,operation='ENTRY',role='short_account',account=B,observed_at_ms=T)
        venue=m.TestnetVenue({'HL_TESTNET_RUNTIME_MODE':'long_stream_testnet_v1'})
        store=type('Store',(),{'domain':'testnet',
            'for_account':lambda self,account:[]})()
        m.Controller(store,venue,ROUTES2)
        budget=dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',
                    test_plan_checked=True)
        with patch.object(venue,'now',return_value=T), \
                patch.object(venue,'_gate',return_value=ROUTES2['short_account']), \
                patch.object(m.roles,'wallet_for_role',return_value=object()), \
                patch.object(m.roles,'budget_for_role',return_value=budget), \
                patch.object(venue,'_fund_info_plan',return_value=Mock()), \
                patch.object(m.roles,'entry_action_headroom',return_value=100), \
                patch.object(m.life,'review',side_effect=AssertionError('NO_PREDECESSOR')), \
                patch('hl_testnet_runtime.long_stream_runtime._account_owned',return_value=True):
            venue.authorize(state,proposal,m.AFTER_EXIT)

    def test_40_of_100_creates_only_card_stop_and_preserves_entry(self):
        s=state_from_case();before=deepcopy(s);p=self.select(s)
        self.assertEqual((p['leg'],p['quantity']),('STOP','40'))
        self.assertEqual(p['action']['grouping'],'na');self.assertTrue(p['action']['orders'][0]['r'])
        self.assertEqual(s,before)
    def test_expired_partial_short_cancels_only_its_working_remainder(self):
        s=state_from_case(q='40',side='SHORT',expiry_seconds=10)
        before=deepcopy(s)
        p=m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T,
                   after_exit_policy=m.AFTER_EXIT)
        self.assertEqual((p['operation'],p['leg'],p['quantity']),
                         ('CANCEL_EXPIRED_ENTRY_REMAINDER','ENTRY','0'))
        self.assertEqual(p['action'],dict(type='cancel',cancels=[dict(a=0,o=10)]))
        self.assertEqual(s,before)
        # The same alert before expiry still follows normal stop-first recovery.
        future=state_from_case(q='40',side='SHORT',expiry_seconds=60)
        self.assertEqual(self.select(future)['leg'],'STOP')
        # An accepted but entirely unfilled entry also expires on this clock.
        unfilled=state_from_case(q='0',side='SHORT',expiry_seconds=10)
        self.assertEqual(self.select(unfilled)['operation'],'CANCEL_EXPIRED_ENTRY_REMAINDER')
    def test_crossed_old_target_closes_only_confirmed_partial_short_at_original_price(self):
        s=state_from_case(q='40',side='SHORT',expiry_seconds=10)
        b=s['bindings'][0];snap=s['evidence']['snapshot']
        snap['open_orders']=[]
        ending=terminal(b,'ENTRY','40');ending['state']='CANCELED'
        snap['terminal_orders']=[ending]
        before=deepcopy(s)
        p=m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
        self.assertEqual((p['leg'],p['operation'],p['quantity']),
                         ('TAKE_PROFIT','CLOSE_PASSED_TAKE','40'))
        self.assertEqual(p['action']['orders'][0]['t'],dict(limit=dict(tif='Ioc')))
        self.assertEqual(p['action']['orders'][0]['p'],'9.8')
        self.assertTrue(p['action']['orders'][0]['r'])
        from . import residual_exit_contract as contract, residual_exit_fence as fence
        self.assertTrue(contract.validate_wire_proposal(s,p,now_ms=T))
        self.assertTrue(fence.validate_proposal(s,p,now_ms=T))
        self.assertEqual(s,before)
        # A crossed STOP cannot be sent as if it were still protective.
        with self.assertRaisesRegex(DispatchError,'LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW'):
            m.choose(s,ROUTES2,META,dict(mark_price='11',at_ms=T),now_ms=T)
        # A correctly sized stop can remain working during the immediate try.
        b['orders']['STOP']=['11'];snap['open_orders']=[order(b,'STOP','40')]
        self.assertEqual(m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
                         ['operation'],'CLOSE_PASSED_TAKE')
        # If the exchange canceled the one immediate try, do not retry blindly;
        # retain or restore the stop based on a new complete observation.
        b['orders']['TAKE_PROFIT']=['12']
        snap['terminal_orders'].append(terminal(b,'TAKE_PROFIT','0'))
        with self.assertRaisesRegex(DispatchError,'LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW'):
            m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
    def test_crossed_target_missing_stop_requires_final_entry_but_owned_stop_can_be_amended(self):
        s=state_from_case(q='40',side='SHORT')
        snap=s['evidence']['snapshot'];b=s['bindings'][0]
        with self.assertRaisesRegex(DispatchError,'LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW'):
            m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
        snap['terminal_orders']=[terminal(b,'ENTRY','40')]
        snap['terminal_orders'][0]['state']='CANCELED'
        b['orders']['STOP']=['11']
        snap['open_orders']=[order(b,'STOP','1')]
        p=m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
        self.assertEqual((p['operation'],p['leg'],p['old_oid'],p['quantity']),('MODIFY_EXIT','STOP','11','40'))
    def test_canceled_immediate_take_does_not_retry_and_restores_missing_stop(self):
        s=state_from_case(q='40',side='SHORT')
        b=s['bindings'][0];snap=s['evidence']['snapshot']
        b['orders']['TAKE_PROFIT']=['12']
        snap['open_orders']=[]
        entry=terminal(b,'ENTRY','40');entry['state']='CANCELED'
        snap['terminal_orders']=[entry,terminal(b,'TAKE_PROFIT','0')]
        p=m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
        self.assertEqual((p['leg'],p['operation']),('STOP','CREATE_EXIT'))
    def test_external_manual_close_cannot_send_another_exit(self):
        s=state_from_case(q='40',side='SHORT')
        b=s['bindings'][0];snap=s['evidence']['snapshot']
        snap['open_orders']=[]
        entry=terminal(b,'ENTRY','40');entry['state']='CANCELED'
        snap['terminal_orders']=[entry]
        snap['position_quantity']='0'
        with self.assertRaisesRegex(DispatchError,'LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW'):
            m.choose(s,ROUTES2,META,dict(mark_price='9',at_ms=T),now_ms=T)
    def test_quantity_change_modifies_exact_old_exit_without_cancel_request(self):
        s=state_from_case(q='60',stop='40',take='40');p=self.select(s)
        self.assertEqual(p['operation'],'MODIFY_EXIT')
        self.assertEqual(p['action']['type'],'batchModify');self.assertEqual(p['old_oid'],s['bindings'][0]['orders']['STOP'][0])
        self.assertEqual(p['action']['modifies'][0]['oid'],int(p['old_oid']))
        self.assertEqual(m.requested_order(p['action'])['s'],'60')
        self.assertEqual(m.requested_order(p['action'])['r'],True)
    def test_quantity_and_price_precision_both_enforced(self):
        for px,q in (('9.987654','1'),('9.9','1.001')):
            with self.assertRaises(DispatchError):m.precise(px,q,2)
    def test_bad_asset_and_delisting_do_not_get_default_index(self):
        for meta in ({},{'universe':[]},{'universe':[dict(name='DOGE',szDecimals=2,isDelisted=True)]}):
            with self.assertRaises(DispatchError):m.asset(meta,'DOGE')
    def test_duplicate_fill_does_not_double_quantity(self):
        s=state_from_case();s['evidence']['snapshot']['fills']*=3
        self.assertEqual(self.select(s)['quantity'],'40')
    def test_untrusted_app_shape_cannot_be_execution_state(self):
        with self.assertRaises((KeyError,ValueError)):self.select(dict(display_copy=True))
    def test_stale_snapshot_blocks(self):
        s=state_from_case();s['evidence']['snapshot']['at_ms']=T-16000
        with self.assertRaises(DispatchError):self.select(s)
    def test_short_uses_buy_to_reduce_own_quantity(self):
        p=self.select(state_from_case(side='SHORT'))
        self.assertTrue(p['action']['orders'][0]['b']);self.assertEqual(p['account'],B)
    def test_rejection_cannot_look_like_acceptance(self):
        r=dict(status='ok',response=dict(type='order',data=dict(statuses=[dict(error='Reduce only order would increase position.')])) )
        self.assertEqual(m.normalized_reply(r,'order')['state'],'REJECTED')
    def test_rejection_reason_is_persistable_and_redacts_wallets_and_long_tokens(self):
        raw=dict(status='err',response='L1 error: User or API Wallet 0x'+'a'*40+' does not exist; token '+'Z'*64)
        result=m.normalized_reply(raw,'order')
        self.assertEqual(result['state'],'REJECTED')
        self.assertIn('User or API Wallet [address] does not exist',result['venue_reason'])
        self.assertNotIn('0x'+'a'*40,json.dumps(result))
        self.assertNotIn('Z'*64,json.dumps(result))
        self.assertNotIn('venue_reason',m.normalized_reply(dict(status='err',response='\nsecret'),'order'))
    def test_recovered_signer_is_classified_without_persisting_wallets(self):
        unexpected='0x'+'c'*40
        for address,expected in ((AGENT,'AGENT'),(A,'ACCOUNT'),(unexpected,'UNEXPECTED_SIGNER')):
            raw=dict(status='err',response='L1 error: User or API Wallet '+address+' does not exist.')
            result=m.normalized_reply(raw,'order',account=A,agent=AGENT)
            self.assertEqual(result['rejection_subject'],expected)
            self.assertNotIn(address.lower(),json.dumps(result).lower())
        self.assertIsNone(m.rejection_subject(
            dict(status='err',response='Order price too far from oracle'),A,AGENT))
    def test_jsonb_reordered_action_is_rebuilt_in_exact_sdk_wire_order(self):
        state=state_from_case();record=next(iter(state['originals'].values()))
        original=record['draft']['entry_action']
        reordered=json.loads(json.dumps(original,sort_keys=True))
        self.assertNotEqual(list(reordered),['type','orders','grouping'])
        wire=m.canonical_wire_action(reordered)
        self.assertEqual(list(wire),['type','orders','grouping'])
        self.assertEqual(list(wire['orders'][0]),['a','b','p','s','r','t','c'])
        self.assertEqual(list(wire['orders'][0]['t']),['limit'])
        self.assertEqual(wire,original)
        trailing={**original,'orders':[{**original['orders'][0],
            'p':'0.098700','s':'5927.0','t':{'trigger':{
                'tpsl':'sl','triggerPx':'0.098700','isMarket':True}}}]}
        normalized=m.canonical_wire_action(trailing)['orders'][0]
        self.assertEqual((normalized['p'],normalized['s'],
                          normalized['t']['trigger']['triggerPx']),
                         ('0.0987','5927','0.0987'))
        cancel=m.canonical_wire_action({'cancels':[{'o':123,'a':0}],'type':'cancel'})
        self.assertEqual(list(cancel),['type','cancels'])
        self.assertEqual(list(cancel['cancels'][0]),['a','o'])
        with self.assertRaisesRegex(DispatchError,'ACTION_WIRE_SHAPE_INVALID'):
            m.canonical_wire_action({'type':'order','orders':original['orders'],
                                     'grouping':'normalTpsl'})
    def test_rejected_entry_requires_two_public_empty_history_checks(self):
        venue=m.TestnetVenue({})
        state=dict(account=A,symbol='DOGE',bindings=[])
        request=dict(attempt_at_ms=T-121000,attempts=1,reply=dict(oid=None))
        snap=dict(at_ms=T,position_quantity='0',open_orders=[],fills=[])
        with patch.object(venue,'now',return_value=T), \
             patch.object(venue,'lookup',return_value={'status':'unknownOid'}) as lookup, \
             patch.object(venue,'empty_snapshot',return_value=snap) as empty, \
             patch('hl_testnet_runtime.card_sync_evidence.PublicReader'), \
             patch('hl_testnet_runtime.card_sync_evidence.history',return_value=[]) as history:
            self.assertEqual(venue.rejected_entry_evidence(state,request,'cloid'),snap)
            self.assertEqual((lookup.call_count,empty.call_count,history.call_count),(2,2,2))
            history.return_value=[dict(coin='DOGE')]
            with self.assertRaisesRegex(DispatchError,'REJECTION_FILL_FOUND_NO_RELEASE'):
                venue.rejected_entry_evidence(state,request,'cloid')
    def test_rejected_exit_signature_recovery_is_bounded_and_one_time(self):
        venue=m.TestnetVenue({});state=state_from_case()
        cid=next(iter(state['originals']))
        request=dict(attempt_at_ms=T-121000,attempts=1,
            reply=dict(oid=None,rejection_subject='UNEXPECTED_SIGNER'),
            proposal=dict(card_id=cid,leg='STOP'))
        observed=dict(bindings=state['bindings'],snapshot=state['evidence']['snapshot'])
        with patch.object(venue,'now',return_value=T), \
             patch.object(venue,'lookup',return_value={'status':'unknownOid'}) as lookup, \
             patch.object(venue,'collect',return_value=observed) as collect, \
             patch('hl_testnet_runtime.card_sync_evidence.PublicReader'), \
             patch('hl_testnet_runtime.card_sync_evidence.history',return_value=[]) as history:
            self.assertEqual(venue.rejected_exit_evidence(state,request,'cloid'),
                             state['evidence']['snapshot'])
            self.assertEqual((lookup.call_count,history.call_count,collect.call_count),(2,2,1))
            state['originals'][cid]['exit_signature_recovery_used']=True
            with self.assertRaisesRegex(DispatchError,'REJECTION_HISTORY_WINDOW_REQUIRES_REVIEW'):
                venue.rejected_exit_evidence(state,request,'cloid')
    def test_confirmed_entry_rejection_is_not_mislabeled_as_unknown(self):
        request=dict(reply=dict(state='REJECTED',code='ORACLE_PRICE',oid=None),
                     proposal=dict(leg='ENTRY'))
        with self.assertRaisesRegex(DispatchError,'ENTRY_REJECTED_NO_ORDER_NO_RETRY'):
            m.identity(dict(status='unknownOid'),request,T)
        request['reply']=dict(state='OUTCOME_UNKNOWN',code=None,oid=None)
        with self.assertRaisesRegex(DispatchError,'OUTCOME_UNRESOLVED_NO_NEW_REQUEST'):
            m.identity(dict(status='unknownOid'),request,T)

    def test_triggered_take_lookup_identifies_only_the_exact_signed_trigger_intent(self):
        venue=Venue()
        action=dict(type='order',orders=[dict(a=0,b=False,p='11',s='100',r=True,
            t=dict(trigger=dict(isMarket=False,triggerPx='11',tpsl='tp')),
            c='0x'+'a'*32)],grouping='na')
        request=dict(attempt_at_ms=T,reply=None,proposal=dict(account=A,symbol='DOGE',
            leg='TAKE_PROFIT',operation='CREATE_EXIT',action=action))
        venue.send(request)
        raw=venue.lookup(A,action['orders'][0]['c'])
        raw['order']['status']='triggered'
        self.assertEqual(m.identity(raw,request,venue.now()),'1000')
        for invalid in ('cloid','size','side','price','trigger','position_tpsl',
                        'rejected','early','future','stop','entry','immediate'):
            with self.subTest(invalid=invalid):
                changed=deepcopy(raw);intent=deepcopy(request);order=changed['order']['order']
                if invalid=='cloid':order['cloid']='0x'+'b'*32
                elif invalid=='size':order['origSz']='99'
                elif invalid=='side':order['side']='B'
                elif invalid=='price':order['limitPx']='12'
                elif invalid=='trigger':order['triggerPx']='12'
                elif invalid=='position_tpsl':order['isPositionTpsl']=True
                elif invalid=='rejected':intent['reply']=dict(state='REJECTED')
                elif invalid=='early':changed['order']['statusTimestamp']=T-1
                elif invalid=='future':changed['order']['statusTimestamp']=venue.now()+1
                elif invalid=='stop':
                    intent['proposal']['leg']='STOP';order['orderType']='Stop Market'
                elif invalid=='entry':
                    intent['proposal']['leg']='ENTRY';order.update(orderType='Limit',isTrigger=False,reduceOnly=False,side='A')
                    intent['proposal']['action']['orders'][0].update(b=False,r=False)
                else:
                    intent['proposal']['operation']='CLOSE_PASSED_TAKE'
                    intent['proposal']['action']['orders'][0]['t']=dict(limit=dict(tif='Ioc'))
                    order.update(orderType='Limit',isTrigger=False)
                with self.assertRaises(DispatchError):m.identity(changed,intent,venue.now())

    def test_malformed_cancel_responses_remain_unknown(self):
        for raw in (None,{},dict(status='ok',response=None),dict(status='ok',response='no')):
            self.assertEqual(m.normalized_reply(raw,'cancel')['state'],'OUTCOME_UNKNOWN')
    def test_cancel_success_is_unverified(self):
        raw=dict(status='ok',response=dict(type='cancel',data=dict(statuses=['success'])))
        self.assertEqual(m.normalized_reply(raw,'cancel')['state'],'ACCEPTED_UNVERIFIED')
    def test_default_adapter_blocks_before_key_or_http(self):
        v=m.TestnetVenue({});p=dict(card_id='a'*64)
        with self.assertRaises(DispatchError):v.send(dict(proposal=p))
        self.assertEqual(v.sent,0)
    def test_no_startup_hook_or_inbound_app_route(self):
        self.assertFalse(hasattr(m,'application'));self.assertFalse(hasattr(m,'start'))
        source=inspect.getsource(m)
        self.assertNotIn('scheduleCancel',source);self.assertNotIn('mainnet_enabled=True',source)
    def test_real_signature_source_explicitly_uses_testnet_domain(self):
        self.assertIn("sign_l1_action(wallet,action,None,request['nonce'],expires,False)",inspect.getsource(m.TestnetVenue.send))
    def test_two_same_symbol_cards_remain_separate(self):
        one=state_from_case();two=state_from_case(n=2,q='100',stop='100',take='100')
        one['bindings']+=two['bindings'];one['originals'].update(two['originals'])
        snap=one['evidence']['snapshot'];other=two['evidence']['snapshot']
        for name in ('fills','open_orders','terminal_orders'):snap[name]+=other[name]
        snap['position_quantity']='140';one['evidence']['bindings']=one['bindings']
        p=self.select(one);self.assertEqual(p['quantity'],'40')

    def test_same_symbol_alert_waits_for_verified_finality(self):
        from .test_card_lifecycle import fill, terminal
        one=state_from_case(q='100',stop='100',take='100')
        second,record=original(2)
        one['originals'][second['card_id']]=record
        self.assertIsNone(self.select(one))
        snap=one['evidence']['snapshot'];old=one['bindings'][0]
        snap['fills'].append(fill(old,'TAKE_PROFIT',qty='100'))
        snap['terminal_orders'] += [terminal(old,'STOP','0'),terminal(old,'TAKE_PROFIT','100')]
        snap['open_orders']=[];snap['position_quantity']='0'
        self.assertTrue(life.review(one['bindings'],snap,now_ms=T)['cards'][0]['closure_verified'])
        proposal=self.select(one)
        self.assertEqual((proposal['card_id'],proposal['leg']),(second['card_id'],'ENTRY'))
        self.assertTrue(m.residual.validate_proposal(one,proposal,now_ms=T))

    def test_pending_first_entry_also_blocks_new_card(self):
        one=state_from_case(q='0')
        second,record=original(2)
        one['originals'][second['card_id']]=record
        self.assertIsNone(self.select(one))


class ObservationCoordinationTests(NoExternal):
    def controller(self):
        state=state_from_case()
        store=Mock(domain='software');current=[deepcopy(state)]
        store.load.side_effect=lambda bucket:deepcopy(current[0])
        venue=Venue()
        controller=m.Controller(store,venue,ROUTES2)
        result=deepcopy(state);result['revision']+=1
        result['evidence']['snapshot']['at_ms']=T
        return controller,state,result,current

    def protected_controller(self):
        controller,_,_,current=self.controller()
        state=state_from_case(q='100',stop='100',take='100')
        current[0]=deepcopy(state)
        result=deepcopy(state);result['revision']+=1
        class Feed:
            generation=1
            revision=1
            healthy=True
            dirty=()
            def begin_reconciliation(self,account):
                return type('Token',(),dict(generation=self.generation,revision=self.revision))()
            def entry_allowed(self,account):return self.healthy
            def dirty_symbols(self,account):return self.dirty
        controller.venue.fill_wakeups=Feed()
        return controller,state,result,current

    def test_late_quiet_worker_shares_exact_completed_proof_in_both_directions(self):
        for owner_emergency in (False,True):
            with self.subTest(owner_emergency=owner_emergency):
                controller,state,result,current=self.protected_controller()
                def collect(bucket,prior):
                    current[0]=deepcopy(result)
                    return deepcopy(result)
                with patch.object(controller,'_refresh_once',side_effect=collect) as reads:
                    self.assertEqual(controller.refresh(state['bucket'],emergency=owner_emergency),result)
                    controller.venue.t+=500
                    actual=controller.refresh(state['bucket'],emergency=not owner_emergency)
                    self.assertEqual(actual,result)
                    reads.assert_called_once()
                self.assertEqual(actual['evidence']['snapshot']['at_ms'],T)
                self.assertEqual(controller._observations.completed[state['bucket']].completed_at_ms,T)
                self.assertEqual(controller.venue.sent,0)

    def test_worker_scheduled_before_peer_commit_does_not_restart_quiet_collection(self):
        controller,state,result,current=self.protected_controller()
        captured=threading.Event();release=threading.Event();actual=[]
        def load(bucket):
            value=deepcopy(current[0])
            if threading.current_thread().name=='late-quiet-refresh' and not captured.is_set():
                captured.set()
                if not release.wait(2):raise AssertionError('STALE_LOAD_NOT_RELEASED')
            return value
        controller.store.load.side_effect=load
        def collect(bucket,prior):
            current[0]=deepcopy(result)
            return deepcopy(result)
        worker=threading.Thread(target=lambda:actual.append(controller.refresh(state['bucket'],emergency=True)),
            name='late-quiet-refresh')
        with patch.object(controller,'_refresh_once',side_effect=collect) as reads:
            worker.start()
            try:
                self.assertTrue(captured.wait(1))
                self.assertEqual(controller.refresh(state['bucket']),result)
            finally:
                release.set();worker.join(2)
            self.assertFalse(worker.is_alive())
            self.assertEqual(actual,[result])
            reads.assert_called_once()
        self.assertFalse(controller._observation_flights)

    def test_completed_quiet_proof_never_hides_hints_uncertainty_or_old_clocks(self):
        for invalid in ('hint','generation','dirty','gap','missing-feed','pending',
                'emergency','missing-stop','missing-take','working-entry','changed-revision',
                'changed-bindings','incomplete','stale','future','share-expired','completion-future','failed'):
            with self.subTest(invalid=invalid):
                controller,state,result,current=self.protected_controller()
                feed=controller.venue.fill_wakeups
                completed=m._ObservationFlight(state['revision'],(1,1),False)
                completed.result=deepcopy(result);completed.done.set();completed.completed_at_ms=T
                controller._observations.completed[state['bucket']]=completed
                current[0]=deepcopy(result)
                if invalid=='hint':feed.revision+=1
                elif invalid=='generation':feed.generation+=1
                elif invalid=='dirty':feed.dirty=('DOGE',)
                elif invalid=='gap':feed.healthy=False
                elif invalid=='missing-feed':del controller.venue.fill_wakeups
                elif invalid=='pending':current[0]['pending']='a'*64
                elif invalid=='emergency':current[0]['emergency']={'phase':'ACTIVE'}
                elif invalid in ('missing-stop','missing-take','working-entry'):
                    kwargs={'missing-stop':dict(q='100',stop='40',take='100'),
                        'missing-take':dict(q='100',stop='100',take='40'),
                        'working-entry':dict(q='40',stop='40',take='40')}[invalid]
                    current[0]=state_from_case(**kwargs)
                    current[0]['revision']=result['revision']
                    completed.result=deepcopy(current[0])
                elif invalid=='changed-revision':current[0]['revision']+=1
                elif invalid=='changed-bindings':current[0]['bindings'][0]['orders']['STOP']=[]
                elif invalid=='incomplete':
                    current[0]['evidence']['snapshot']['history_complete']=False
                    completed.result=deepcopy(current[0])
                elif invalid=='stale':
                    controller.venue.t=T+5001;completed.completed_at_ms=T+5001
                elif invalid=='future':controller.venue.t=T-1;completed.completed_at_ms=T-1
                elif invalid=='share-expired':controller.venue.t=T+1001
                elif invalid=='completion-future':completed.completed_at_ms=T+1
                else:completed.error=DispatchError('PUBLIC_READ_FAILED')
                with patch.object(controller,'_refresh_once',return_value=deepcopy(current[0])) as reads:
                    controller.refresh(state['bucket'],emergency=True)
                    reads.assert_called_once()

    def test_quiet_share_window_is_not_renewed_by_the_late_worker(self):
        controller,state,result,current=self.protected_controller()
        completed=m._ObservationFlight(state['revision'],(1,1),False)
        completed.result=deepcopy(result);completed.done.set();completed.completed_at_ms=T
        controller._observations.completed[state['bucket']]=completed
        current[0]=deepcopy(result)
        controller.venue.t=T+1000
        with patch.object(controller,'_refresh_once',return_value=deepcopy(result)) as reads:
            self.assertEqual(controller.refresh(state['bucket']),result)
            reads.assert_not_called()
            self.assertEqual(completed.completed_at_ms,T)
            controller.venue.t+=1
            controller.refresh(state['bucket'])
            reads.assert_called_once()

    def test_peer_completion_after_flight_claim_keeps_its_original_share_deadline(self):
        controller,state,result,current=self.protected_controller()
        completed=m._ObservationFlight(state['revision'],(1,1),True)
        completed.result=deepcopy(result);completed.done.set();completed.completed_at_ms=T
        controller._observations.completed[state['bucket']]=completed
        current[0]=deepcopy(result);controller.venue.t=T+500
        original=controller._quiet_completed_observation
        calls=[]
        def checkpoint(*args):
            calls.append(1)
            return None if len(calls)==1 else original(*args)
        with patch.object(controller,'_quiet_completed_observation',side_effect=checkpoint), \
                patch.object(controller,'_refresh_once',side_effect=AssertionError('NO_DUPLICATE_READ')):
            self.assertEqual(controller.refresh(state['bucket'],emergency=True),result)
        self.assertEqual(len(calls),2)
        self.assertFalse(controller._observation_flights)
        self.assertEqual(controller._observations.completed[state['bucket']].completed_at_ms,T)
        controller.venue.t=T+1001
        with patch.object(controller,'_refresh_once',return_value=deepcopy(result)) as reads:
            controller.refresh(state['bucket'])
            reads.assert_called_once()

    def quiet_inflight(self,*,age=15001,started=100.0,now=100.0):
        controller,state,result,current=self.protected_controller()
        controller.venue.t=T+age
        result['evidence']['snapshot']['at_ms']=controller.venue.t
        clock=[now]
        with patch.object(m.time,'monotonic',return_value=started):
            flight=m._ObservationFlight(state['revision'],(1,1),False,
                state=state,now_ms=controller.venue.t)
        controller._observation_flights[state['bucket']]=[flight]
        return controller,state,result,current,flight,clock

    def test_quiet_due_proof_joins_normal_completion_at_600ms_without_second_read(self):
        controller,state,result,current,flight,clock=self.quiet_inflight()
        waits=[]
        def wait(seconds):
            waits.append(seconds);clock[0]+=seconds
            if clock[0]>=100.6:
                current[0]=deepcopy(result);flight.result=deepcopy(result);flight.done.set()
            return flight.done.is_set()
        with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]), \
                patch.object(flight.done,'wait',side_effect=wait), \
                patch.object(controller,'_refresh_once',side_effect=AssertionError('NO_DUPLICATE_READ')):
            self.assertEqual(controller.refresh(state['bucket'],emergency=True),result)
        self.assertGreater(sum(waits),.5)
        self.assertLessEqual(sum(waits),1.0)
        self.assertTrue(all(0<seconds<=.05 for seconds in waits))
        self.assertEqual(result['evidence']['snapshot']['at_ms'],T+15001)
        self.assertEqual(controller.venue.sent,0)
        controller._observation_flights.clear()

    def test_stalled_quiet_flight_uses_original_one_second_deadline_for_late_caller(self):
        controller,state,result,current,flight,clock=self.quiet_inflight(now=100.9)
        waits=[]
        def wait(seconds):waits.append(seconds);clock[0]+=seconds;return False
        def collect(bucket,prior):current[0]=deepcopy(result);return deepcopy(result)
        with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]), \
                patch.object(flight.done,'wait',side_effect=wait), \
                patch.object(controller,'_refresh_once',side_effect=collect) as reads:
            self.assertEqual(controller.refresh(state['bucket'],emergency=True),result)
            reads.assert_called_once()
        self.assertAlmostEqual(sum(waits),.1,places=6)
        self.assertEqual(flight.started,100.0)
        self.assertEqual(controller._observation_flights[state['bucket']],[flight])
        controller._observation_flights.clear()

    def test_hint_or_durable_change_interrupts_quiet_join_before_original_urgent_bound(self):
        for changed in ('hint','gap','durable-overdue-fill'):
            with self.subTest(changed=changed):
                controller,state,result,current,flight,clock=self.quiet_inflight()
                waits=[];observed=[]
                def wait(seconds):
                    waits.append(seconds);clock[0]+=seconds
                    if clock[0]>=100.05:
                        if changed=='hint':controller.venue.fill_wakeups.revision=2
                        elif changed=='gap':controller.venue.fill_wakeups.healthy=False
                        else:
                            current[0]=state_from_case(q='100',stop='40',take='100')
                            current[0]['revision']=state['revision']+1
                    return False
                def collect(bucket,prior):
                    observed.append(deepcopy(prior));current[0]=deepcopy(result)
                    return deepcopy(result)
                with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]), \
                        patch.object(flight.done,'wait',side_effect=wait), \
                        patch.object(controller,'_refresh_once',side_effect=collect) as reads:
                    controller.refresh(state['bucket'],emergency=True)
                    reads.assert_called_once()
                self.assertEqual(waits,[.05])
                if changed=='durable-overdue-fill':
                    self.assertEqual(observed[0]['revision'],state['revision']+1)
                    self.assertEqual(observed[0]['evidence']['snapshot']['position_quantity'],'100')
                else:self.assertEqual(observed[0]['revision'],state['revision'])
                controller._observation_flights.clear()

    def test_quiet_classification_never_extends_shorter_or_uncertain_join(self):
        for invalid in ('short-deadline','already-due','dirty','working-entry',
                'pending','emergency','missing-stop','missing-take','stale','future'):
            with self.subTest(invalid=invalid):
                controller,state,result,current,flight,clock=self.quiet_inflight()
                wait_ms=250
                if invalid=='short-deadline':wait_ms=100
                elif invalid=='already-due':wait_ms=0
                elif invalid=='dirty':controller.venue.fill_wakeups.dirty=('DOGE',)
                elif invalid in ('working-entry','missing-stop','missing-take'):
                    kw={'working-entry':dict(q='40',stop='40',take='40'),
                        'missing-stop':dict(q='100',stop='40',take='100'),
                        'missing-take':dict(q='100',stop='100',take='40')}[invalid]
                    current[0]=state_from_case(**kw)
                elif invalid=='pending':current[0]['pending']='a'*64
                elif invalid=='emergency':current[0]['emergency']={'phase':'ACTIVE'}
                elif invalid=='stale':controller.venue.t=T+17001
                else:controller.venue.t=T-1
                # Match the exact changed basis where possible: suppression is
                # still forbidden by its work/coverage/clock classification.
                flight.basis_digest=life.digest(current[0])
                waits=[]
                def wait(seconds):waits.append(seconds);clock[0]+=seconds;return False
                with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]), \
                        patch.object(flight.done,'wait',side_effect=wait), \
                        patch.object(controller,'_refresh_once',return_value=deepcopy(result)) as reads:
                    controller.refresh(state['bucket'],emergency=True,emergency_wait_ms=wait_ms)
                    reads.assert_called_once()
                self.assertEqual(waits,[wait_ms/1000])
                controller._observation_flights.clear()

    def joined(self,controller,state,result,current,*,failure=None,change=None):
        started=threading.Event();release=threading.Event();waiting=threading.Event()
        calls=[]
        def collect(bucket,original):
            calls.append(deepcopy(original))
            if len(calls)==1:
                started.set()
                if not release.wait(2):raise AssertionError('TEST_COLLECTION_NOT_RELEASED')
                if failure is not None:raise failure
            current[0]=deepcopy(result)
            return deepcopy(result)
        with patch.object(controller,'_refresh_once',side_effect=collect):
            with ThreadPoolExecutor(max_workers=2) as pool:
                owner=pool.submit(controller.refresh,state['bucket'])
                self.assertTrue(started.wait(1))
                flight=controller._observation_flights[state['bucket']][0]
                original_wait=flight.done.wait
                def wait(seconds):
                    waiting.set();return original_wait(seconds)
                with patch.object(flight.done,'wait',side_effect=wait):
                    safety=pool.submit(controller.refresh,state['bucket'],emergency=True)
                    self.assertTrue(waiting.wait(1))
                    if change:change()
                    release.set()
                    if failure is not None:
                        with self.assertRaises(type(failure)):owner.result(timeout=2)
                    else:self.assertEqual(owner.result(timeout=2),result)
                    self.assertEqual(safety.result(timeout=2),result)
        self.assertFalse(controller._observation_flights)
        return calls

    def test_overlapping_safety_and_normal_share_only_one_committed_collection(self):
        controller,state,result,current=self.controller()
        calls=self.joined(controller,state,result,current)
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0]['revision'],state['revision'])
        self.assertEqual(result['evidence']['snapshot']['at_ms'],T)

    def test_caller_loading_just_committed_revision_joins_its_exact_flight(self):
        for emergency in (False,True):
            with self.subTest(emergency=emergency):
                controller,state,result,current=self.controller()
                flight=m._ObservationFlight(state['revision'],None,False)
                # Model the interval after the checkpoint committed and result
                # was captured, before the owner's finalizer publishes done.
                flight.result=deepcopy(result);current[0]=deepcopy(result)
                controller._observation_flights[state['bucket']]=[flight]
                waited=[];wait=flight.done.wait
                def finish_owner(seconds):
                    waited.append(seconds);flight.done.set()
                    return wait(seconds)
                with patch.object(flight.done,'wait',side_effect=finish_owner), \
                        patch.object(controller,'_refresh_once',side_effect=AssertionError('NO_DUPLICATE_READ')):
                    actual=controller.refresh(state['bucket'],emergency=emergency)
                self.assertEqual(actual,result)
                self.assertEqual(waited,[0.25 if emergency else m.NORMAL_OBSERVATION_JOIN_SECONDS])
                self.assertEqual(actual['evidence']['snapshot']['at_ms'],T)
                controller._observation_flights.clear()

    def test_committed_revision_sharing_retains_done_identity_feed_and_freshness_guards(self):
        for invalid in ('not_done','basis','durable','feed','stale','future','incomplete'):
            with self.subTest(invalid=invalid):
                controller,state,result,current=self.controller()
                class Feed:
                    revision=1
                    def begin_reconciliation(self,account):
                        return type('Token',(),dict(generation=1,revision=self.revision))()
                feed=Feed();controller.venue.fill_wakeups=feed
                flight=m._ObservationFlight(state['revision'],(1,1),False)
                flight.result=deepcopy(result);flight.done.set()
                loaded=deepcopy(result);current[0]=deepcopy(result)
                if invalid=='not_done':flight.done.clear()
                elif invalid=='basis':loaded['pending']='a'*64
                elif invalid=='durable':current[0]['revision']+=1
                elif invalid=='feed':feed.revision+=1
                elif invalid=='stale':controller.venue.t=T+5001
                elif invalid=='future':controller.venue.t=T-1
                else:
                    flight.result['evidence']['snapshot']['history_complete']=False
                    loaded=deepcopy(flight.result);current[0]=deepcopy(flight.result)
                self.assertIsNone(controller._share_observation(state['bucket'],flight,loaded,(1,1),
                                                               emergency=True))

    def test_failed_background_collection_does_not_consume_independent_safety_attempt(self):
        from .request_budget import BudgetError
        controller,state,result,current=self.controller()
        calls=self.joined(controller,state,result,current,
            failure=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED'))
        self.assertEqual(len(calls),2)

    def test_new_feed_hint_forces_independent_safety_collection(self):
        controller,state,result,current=self.controller()
        class Feed:
            revision=1
            def begin_reconciliation(self,account):
                return type('Token',(),dict(generation=1,revision=self.revision))()
        feed=Feed();controller.venue.fill_wakeups=feed
        calls=self.joined(controller,state,result,current,
                          change=lambda:setattr(feed,'revision',2))
        self.assertEqual(len(calls),2)

    def test_safety_does_not_share_checkpoint_older_than_original_five_second_bound(self):
        controller,state,result,current=self.controller()
        controller.venue.t=T+5001
        calls=self.joined(controller,state,result,current)
        self.assertEqual(len(calls),2)

    def test_safety_can_take_over_stalled_normal_without_third_parallel_collection(self):
        controller,state,result,current=self.controller()
        normal_started=threading.Event();safety_started=threading.Event()
        normal_release=threading.Event();safety_release=threading.Event()
        lock=threading.Lock();calls=[]
        def collect(bucket,original):
            with lock:
                ordinal=len(calls);calls.append(original['revision'])
            started,release=((normal_started,normal_release) if ordinal==0
                             else (safety_started,safety_release))
            started.set()
            if not release.wait(2):raise AssertionError('TEST_COLLECTION_NOT_RELEASED')
            if ordinal==0:
                raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
            current[0]=deepcopy(result)
            return deepcopy(result)
        with patch.object(controller,'_refresh_once',side_effect=collect):
            with ThreadPoolExecutor(max_workers=2) as pool:
                normal=pool.submit(controller.refresh,state['bucket'])
                self.assertTrue(normal_started.wait(1))
                safety=pool.submit(controller.refresh,state['bucket'],
                                   emergency=True,emergency_wait_ms=0)
                self.assertTrue(safety_started.wait(1))
                try:
                    with self.assertRaisesRegex(DispatchError,'IN_PROGRESS_RETRY'):
                        controller.refresh(state['bucket'],emergency=True,emergency_wait_ms=0)
                    self.assertEqual(len(calls),2)
                    safety_release.set()
                    self.assertEqual(safety.result(timeout=1),result)
                finally:
                    normal_release.set();safety_release.set()
                with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED'):
                    normal.result(timeout=1)
        self.assertFalse(controller._observation_flights)

    def test_normal_timeout_never_starts_duplicate_public_collection(self):
        controller,state,result,current=self.controller()
        started=threading.Event();release=threading.Event()
        def collect(bucket,original):
            started.set()
            if not release.wait(2):raise AssertionError('TEST_COLLECTION_NOT_RELEASED')
            current[0]=deepcopy(result)
            return deepcopy(result)
        with patch.object(controller,'_refresh_once',side_effect=collect) as observation, \
                patch.object(m,'NORMAL_OBSERVATION_JOIN_SECONDS',0):
            with ThreadPoolExecutor(max_workers=1) as pool:
                first=pool.submit(controller.refresh,state['bucket'])
                self.assertTrue(started.wait(1))
                try:
                    with self.assertRaisesRegex(DispatchError,'IN_PROGRESS_RETRY'):
                        controller.refresh(state['bucket'])
                    self.assertEqual(observation.call_count,1)
                finally:release.set()
                first.result(timeout=1)
        self.assertFalse(controller._observation_flights)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class DispatchDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=Venue()
        self.c=m.Controller(self.store,self.v,ROUTES2)
        self.b,self.o=original();self.cards.record(self.o['card'])
        self.s=self.c.register(self.b['card_id']);self.bucket=self.s['bucket']
    def cycle(self,send=True):
        self.v.t+=1
        return self.c.cycle(self.bucket,send=send)
    def entry(self,q='40'):
        r=self.cycle();self.assertEqual(r['status'],'ACCEPTED_UNVERIFIED')
        oid=str(1000);self.v.fill(oid,q);return oid
    def protect(self,q='40'):
        self.entry(q);self.cycle();self.cycle();self.cycle(False)
    def remaining(self):
        s=self.store.load(self.bucket)
        return life.review(s['bindings'],s['evidence']['snapshot'],now_ms=self.v.now())['cards']
    def crossed_take_stop_modify_lost_reply_restart(self,side):
        if side=='SHORT':
            self.b,self.o=original(2,'SHORT');self.cards.record(self.o['card'])
            self.bucket=self.c.register(self.b['card_id'])['bucket']
        self.protect('40');self.v.fill('1000','20')
        mark='10.3' if side=='LONG' else '9.7'
        self.v.sample=lambda account,symbol:dict(mark_price=mark,at_ms=self.v.now())
        self.v.lose_reply=True;result=self.cycle()
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN')
        pending=self.store.load(self.bucket)['pending'];request=self.store.request(pending)
        self.assertEqual((request['proposal']['operation'],request['proposal']['leg'],request['proposal']['quantity']),
                         ('MODIFY_EXIT','STOP','60'))
        sent=self.v.sent
        reopened=DispatchStore(PostgresJournal.for_ci(CI));self.c=m.Controller(reopened,self.v,ROUTES2)
        state=self.c.refresh(self.bucket)
        self.assertIsNone(state['pending']);self.assertEqual(reopened.request(pending)['phase'],'OBSERVED')
        self.assertEqual(reopened.request(pending)['attempts'],1)
        view=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())['cards'][0]
        self.assertEqual((view['remaining_quantity'],view['stop_quantity_observed']),('60','60'))
        self.assertEqual(self.v.sent,sent)
        self.assertEqual(state['bindings'][0]['orders']['TAKE_PROFIT'],['1002'])
        self.assertEqual(self.v.orders['1002']['order']['origSz'],'40')
        # The crossed TAKE still cannot be replaced. Its refusal must not mint
        # another STOP intent or replay the already observed unknown attempt.
        with self.assertRaisesRegex(DispatchError,'LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW'):
            self.cycle()
        self.assertEqual(self.v.sent,sent)
        self.assertEqual(reopened.request(pending)['attempts'],1)

    def test_long_crossed_take_stop_modify_unknown_reply_is_reconciled_once_after_restart(self):
        self.crossed_take_stop_modify_lost_reply_restart('LONG')

    def test_short_crossed_take_stop_modify_unknown_reply_is_reconciled_once_after_restart(self):
        self.crossed_take_stop_modify_lost_reply_restart('SHORT')
    def test_preview_records_no_requests_and_no_attempts(self):
        r=self.cycle(False);self.assertEqual(r['status'],'PREVIEW_ONLY');self.assertEqual(self.v.sent,0)
        with self.j._transaction() as conn:self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],0)
    def test_confirmed_rejection_retires_once_and_never_replays_original_card(self):
        def reject(_):
            self.v.sent+=1;self.v.t+=10
            return dict(status='err',response='Order price too far from oracle')
        with patch.object(self.v,'send',side_effect=reject):
            result=self.cycle()
        self.assertEqual(result['status'],'REJECTED')
        rid=self.store.load(self.bucket)['pending']
        self.assertEqual(self.store.request(rid)['reply']['venue_reason'],'Order price too far from oracle')
        self.v.t+=121000
        with patch.object(self.v,'rejected_entry_evidence',create=True,
                          side_effect=lambda state,request,cloid:self.v.empty_snapshot(A,'DOGE')):
            self.cycle(False)
        state=self.store.load(self.bucket)
        self.assertIsNone(state['pending'])
        self.assertTrue(state['originals'][self.b['card_id']]['entry_rejected_no_retry'])
        self.assertEqual(self.store.request(rid)['terminal_state'],'REJECTED_NO_ORDER')
        self.assertEqual(self.v.sent,1)
        self.cycle(False)
        self.assertIsNone(m.choose(self.store.load(self.bucket),ROUTES2,META,
                                   self.v.sample(A,'DOGE'),now_ms=self.v.now()))
    def test_entry_stop_take_whole_chain_40_with_pending_60(self):
        self.protect();s=self.store.load(self.bucket);v=self.remaining()[0]
        self.assertEqual((v['entry_quantity'],v['stop_quantity_observed'],v['take_profit_quantity_observed']),('40','40','40'))
        self.assertEqual(self.v.orders['1000']['order']['sz'],'60')
        self.assertEqual(len(self.v.requests),3);self.assertEqual(s['pending'],None)
        self.assertIsNone(self.cards.load(self.b['card_id'])['actual_execution'])
    def test_short_full_controller_chain(self):
        b,o=original(2,'SHORT');self.cards.record(o['card']);s=self.c.register(b['card_id'])
        self.bucket=s['bucket'];self.b=b;self.protect()
        self.assertEqual(self.store.load(self.bucket)['account'],B)
        self.assertTrue(self.v.requests[1]['proposal']['action']['orders'][0]['b'])

    def test_take_activates_before_first_binding_and_partial_fill_resizes_only_stop(self):
        self.entry('100');self.cycle()  # Entry fill, then its verified stop.
        send=self.v.send;read=self.v.read;live=[]
        def activate(request):
            reply=send(request)
            if request['proposal']['leg']=='TAKE_PROFIT':
                self.v.t+=1
                row=self.v.orders['1002'];row['order']['timestamp']=self.v.t-1
                row.update(status='triggered',statusTimestamp=self.v.t)
                self.v.fill('1002','20')
                row['order']['sz']='100'  # Original orderStatus parent survives.
                live.append({**deepcopy(row['order']), 'sz':'80','isTrigger':False,
                    'triggerPx':'0','triggerCondition':'Triggered',
                    'timestamp':row['statusTimestamp']})
            return reply
        def observed(kind,account,**kwargs):
            result=read(kind,account,**kwargs)
            if kind=='frontendOpenOrders':result+=deepcopy(live)
            return result
        with patch.object(self.v,'send',side_effect=activate),patch.object(self.v,'read',side_effect=observed):
            self.cycle()
            request_id=self.store.load(self.bucket)['pending']
            self.assertNotIn('1002',self.store.load(self.bucket)['bindings'][0]['orders']['TAKE_PROFIT'])
            self.cycle(False)
            state=self.store.load(self.bucket)
            self.assertIsNone(state['pending'])
            self.assertEqual(self.store.request(request_id)['phase'],'OBSERVED')
            take=next(o for o in state['evidence']['snapshot']['open_orders'] if o['oid']=='1002')
            self.assertEqual((take['order_type'],take['quantity'],take['trigger_price']),
                ('TRIGGERED_TP_LIMIT','80',None))
            self.cycle();self.cycle(False)
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'MODIFY_EXIT')
        self.assertEqual((self.v.requests[-1]['proposal']['leg'],self.v.requests[-1]['proposal']['old_oid']),('STOP','1001'))
        self.assertEqual(self.v.sent,4)
        view=self.remaining()[0]
        self.assertEqual((view['remaining_quantity'],view['stop_quantity_observed'],
                          view['take_profit_quantity_observed']),('80','80','80'))
        self.assertFalse(view['issues']);self.assertFalse(view['closure_verified'])
        self.assertEqual(self.store.request(request_id)['attempts'],1)
    def test_resize_is_one_venue_action_and_recomputes_later_partial_fill(self):
        self.protect();self.v.fill('1000','20');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'MODIFY_EXIT')
        self.assertEqual(self.v.orders['1001']['status'],'canceled')
        self.assertEqual(self.v.orders['1003']['status'],'open')
        # Another fill while the single modification's result is reconciled.
        self.v.fill('1000','10');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'70')
        self.cycle();self.cycle();self.cycle(False)
        v=self.remaining()[0];self.assertEqual((v['stop_quantity_observed'],v['take_profit_quantity_observed']),('70','70'))
        self.assertEqual(self.v.orders['1000']['order']['sz'],'30')
        self.assertTrue(all(r['proposal']['action']['type']!='cancel' for r in self.v.requests))
    def test_lost_create_reply_reconciles_cloid_without_resending(self):
        self.entry();self.v.lose_reply=True;self.cycle();count=self.v.sent
        self.cycle(False);s=self.store.load(self.bucket)
        self.assertIsNone(s['pending']);self.assertEqual(self.v.sent,count)
        self.assertEqual(self.remaining()[0]['stop_quantity_observed'],'40')
    def test_unknown_create_cannot_be_bypassed_by_changed_quantity(self):
        self.entry()
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.cycle()
        self.v.fill('1000','20');before=len(self.v.requests)
        with self.assertRaises(DispatchError):self.cycle()
        self.assertEqual(len(self.v.requests),before)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f"SELECT count(*) FROM {SCHEMA}.requests WHERE phase='OUTCOME_UNKNOWN'").fetchone()[0],1)
    def test_lost_cancel_reply_requires_terminal_proof(self):
        self.protect();self.v.fill('1000','10');self.v.lose_reply=True;self.cycle();n=self.v.sent
        self.cycle(False);self.assertEqual(self.v.sent,n)
        self.assertIsNone(self.store.load(self.bucket)['pending'])
    def test_new_process_loads_unknown_request_without_second_attempt(self):
        self.entry()
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.cycle()
        rid=self.store.load(self.bucket)['pending']
        script="from hl_testnet_runtime.postgres_journal import PostgresJournal; from hl_testnet_runtime.filled_dispatch_store import DispatchStore; import os; r=DispatchStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])).request('"+rid+"'); print(r['phase'],r['attempts'])"
        result=subprocess.run([sys.executable,'-c',script],check=True,capture_output=True,text=True,timeout=15)
        self.assertEqual(result.stdout.strip(),'OUTCOME_UNKNOWN 1')
    def test_commit_ack_loss_never_reaches_sender(self):
        real=self.store.prepare_and_begin
        def lost(*a,**kw):real(*a,**kw);raise JournalError('SIMULATED_COMMIT_ACK_LOSS')
        with patch.object(self.store,'prepare_and_begin',side_effect=lost):
            with self.assertRaises(JournalError):self.cycle()
        self.assertEqual(self.v.sent,0)
        rid=self.store.load(self.bucket)['pending'];self.assertEqual(self.store.request(rid)['phase'],'OUTCOME_UNKNOWN')
    def test_concurrent_reservation_allows_only_one_request(self):
        state=self.c.refresh(self.bucket);p=m.choose(state,ROUTES2,META,self.v.sample(A,'DOGE'),now_ms=self.v.now())
        def reserve(_):
            try:self.store.reserve(state,p,self.v.now());return True
            except JournalError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(reserve,range(4)))
        self.assertEqual(sum(results),1)

    def atomic_entry_plan(self):
        self.v.t+=1
        state=self.c.refresh(self.bucket)
        pending=self.store.request(state['pending']) if state['pending'] else None
        proposal=m.choose(state,ROUTES2,META,self.v.sample(A,'DOGE'),now_ms=self.v.now(),
                          sequence=pending['proposal']['sequence'] if pending else None)
        return state,proposal

    def test_atomic_entry_commits_exact_request_without_postcommit_reload(self):
        state,proposal=self.atomic_entry_plan()
        with patch.object(self.store,'load',side_effect=AssertionError('NO_POSTCOMMIT_RELOAD')):
            committed,request=self.store.prepare_and_begin(state,proposal,AGENT,self.v.now())
        self.assertEqual(committed['revision'],state['revision']+1)
        self.assertEqual(committed['pending'],request['request_id'])
        self.assertEqual(request,self.store.request(request['request_id']))
        self.assertEqual((request['phase'],request['attempts']),('OUTCOME_UNKNOWN',1))
        self.assertEqual(request['nonce'],self.v.now())
        self.assertEqual(committed['entry_timing_armed'][self.b['card_id']],self.v.now())
        self.assertEqual(self.v.sent,0)

    def test_atomic_rebase_preserves_prepared_request_and_frozen_action(self):
        state,proposal=self.atomic_entry_plan()
        reserved=self.store.reserve(state,proposal,self.v.now())
        previous=self.store.request(reserved['pending'])
        state,fresh=self.atomic_entry_plan()
        self.assertNotEqual(fresh['basis'],proposal['basis'])
        committed,request=self.store.prepare_and_begin(state,fresh,AGENT,self.v.now())
        self.assertEqual(request['request_id'],previous['request_id'])
        self.assertEqual(request['prepared_at_ms'],previous['prepared_at_ms'])
        self.assertEqual(request['proposal']['action'],previous['proposal']['action'])
        self.assertEqual(request['proposal'],fresh)
        self.assertEqual((request['phase'],request['attempts']),('OUTCOME_UNKNOWN',1))
        self.assertEqual(committed['pending'],previous['request_id'])
        with self.assertRaisesRegex(DispatchError,'NO_REPEAT'):
            self.store.prepare_and_begin(committed,fresh,AGENT,self.v.now())

    def test_atomic_rebase_never_begins_ambiguous_or_attempted_prepared_record(self):
        state,proposal=self.atomic_entry_plan()
        state=self.store.reserve(state,proposal,self.v.now())
        original_request=self.store.request(state['pending'])
        for field,value in (('phase','OUTCOME_UNKNOWN'),('attempts',True),('attempts',1),
                ('nonce',self.v.now()),('attempt_at_ms',self.v.now()),
                ('reply',dict(state='OUTCOME_UNKNOWN')),('observed_oid','1000')):
            with self.subTest(field=field,value=value):
                def corrupt(conn,current):
                    request=deepcopy(original_request);request[field]=value
                    return request
                state=self.store.change(self.bucket,state['revision'],'FAULT_INJECTED_REQUEST',self.v.now(),corrupt)
                before=self.store.request(state['pending'])
                with self.assertRaisesRegex(DispatchError,'NO_REPEAT'):
                    self.store.prepare_and_begin(state,proposal,AGENT,self.v.now())
                self.assertEqual(self.store.load(self.bucket),state)
                self.assertEqual(self.store.request(state['pending']),before)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.nonces').fetchone()[0],0)

    def test_atomic_rebase_rejects_changed_quantity_or_wire_before_nonce(self):
        state,proposal=self.atomic_entry_plan()
        state=self.store.reserve(state,proposal,self.v.now())
        changed=deepcopy(proposal)
        changed['quantity']='99';changed['action']['orders'][0]['s']='99'
        with self.assertRaisesRegex(DispatchError,'UNSENT_PLAN_CHANGED'):
            self.store.prepare_and_begin(state,changed,AGENT,self.v.now())
        self.assertEqual(self.store.load(self.bucket),state)
        self.assertEqual(self.store.request(state['pending'])['attempts'],0)

    def test_atomic_concurrent_workers_return_only_one_sender_request(self):
        state,proposal=self.atomic_entry_plan()
        stores=[DispatchStore(PostgresJournal.for_ci(CI)) for _ in range(2)]
        def attempt(store):
            try:return store.prepare_and_begin(state,proposal,AGENT,self.v.now())[1]
            except DispatchError:return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            requests=list(pool.map(attempt,stores))
        winners=[request for request in requests if request is not None]
        self.assertEqual(len(winners),1)
        self.v.send(winners[0])
        self.assertEqual(self.v.sent,1)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],1)
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.nonces').fetchone()[0],1)

    def test_atomic_cross_process_checkpoint_cannot_observe_committed_prepared_gap(self):
        state,proposal=self.atomic_entry_plan();transaction=self.j._transaction
        script="""from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.filled_dispatch_store import DispatchStore, DispatchError
import os, sys
store=DispatchStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
state=store.load(sys.argv[1])
print('UNBOUND' if state['pending'] is None else 'PARTIAL_PREPARE',flush=True)
try:
    store.change(state['bucket'],state['revision'],'INDEPENDENT_CHECKPOINT',int(sys.argv[2]),lambda conn,state:None)
except DispatchError:
    print('STALE_CHECKPOINT_REJECTED',flush=True)
else:
    print('CHECKPOINT_COMMITTED',flush=True)
"""
        children=[];seen=[]
        class Connection:
            def __init__(proxy,conn):proxy.conn=conn
            def execute(proxy,query,*args,**kwargs):
                if query.startswith(f'INSERT INTO {SCHEMA}.nonces'):
                    child=subprocess.Popen([sys.executable,'-c',script,self.bucket,str(self.v.now())],
                        stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                    children.append(child)
                    seen.append(child.stdout.readline().strip())
                return proxy.conn.execute(query,*args,**kwargs)
        @contextmanager
        def intercepted():
            with transaction() as conn:yield Connection(conn)
        try:
            with patch.object(self.j,'_transaction',intercepted):
                committed,request=self.store.prepare_and_begin(state,proposal,AGENT,self.v.now())
            self.assertEqual(seen,['UNBOUND'])
            self.assertEqual(len(children),1)
            output,error=children[0].communicate(timeout=15)
            self.assertEqual(children[0].returncode,0,error)
            self.assertEqual(output.strip(),'STALE_CHECKPOINT_REJECTED')
            self.assertEqual(self.store.load(self.bucket),committed)
            self.assertEqual(self.store.request(request['request_id'])['attempts'],1)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill();child.communicate(timeout=5)

    def test_atomic_lost_commit_ack_returns_no_sender_and_never_replays(self):
        state,proposal=self.atomic_entry_plan();transaction=self.j._transaction
        @contextmanager
        def lost_ack():
            with transaction() as conn:yield conn
            raise JournalError('SIMULATED_COMMIT_ACK_LOSS')
        with patch.object(self.v,'send',side_effect=AssertionError('NO_SEND')) as sender:
            with patch.object(self.j,'_transaction',lost_ack):
                with self.assertRaisesRegex(JournalError,'COMMIT_ACK_LOSS'):
                    committed,request=self.store.prepare_and_begin(state,proposal,AGENT,self.v.now())
                    sender(request)
            sender.assert_not_called()
        committed=self.store.load(self.bucket)
        request=self.store.request(committed['pending'])
        self.assertEqual((request['phase'],request['attempts']),('OUTCOME_UNKNOWN',1))
        with self.assertRaisesRegex(DispatchError,'NO_REPEAT'):
            self.store.prepare_and_begin(committed,proposal,AGENT,self.v.now())

    def test_actionable_authorization_checkpoint_uses_winning_proof_and_one_frozen_send(self):
        other=m.Controller(DispatchStore(PostgresJournal.for_ci(CI)),self.v,ROUTES2)
        authorized=[];winning=[]
        def advance(state,proposal,policy):
            authorized.append(deepcopy(proposal))
            self.v.t+=1
            winning.append(other.refresh(self.bucket))
        with patch.object(self.v,'authorize',side_effect=advance):
            result=self.cycle()
        self.assertEqual((result['status'],result['order_requests_sent']),('ACCEPTED_UNVERIFIED',1))
        self.assertEqual((len(authorized),len(winning),self.v.sent),(1,1,1))
        request=self.v.requests[0];proposal=request['proposal']
        self.assertEqual(proposal['action'],authorized[0]['action'])
        self.assertEqual(proposal['sequence'],authorized[0]['sequence'])
        self.assertEqual(proposal['basis'],life.digest(winning[0]['evidence']))
        self.assertEqual(proposal['observed_at_ms'],winning[0]['evidence']['snapshot']['at_ms'])
        self.assertNotEqual(proposal['basis'],authorized[0]['basis'])
        self.assertEqual(request['attempts'],1)
        self.assertEqual(request['nonce'],request['attempt_at_ms'])
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],1)

    def test_authorization_checkpoint_with_larger_partial_fill_does_not_send_old_stop(self):
        self.entry('40')
        other=m.Controller(DispatchStore(PostgresJournal.for_ci(CI)),self.v,ROUTES2)
        authorized=[]
        def fill_during_authorization(state,proposal,policy):
            authorized.append(deepcopy(proposal))
            self.v.fill('1000','20')
            other.refresh(self.bucket)
        with patch.object(self.v,'authorize',side_effect=fill_during_authorization):
            with self.assertRaisesRegex(DispatchError,'AUTHORIZED_PLAN_CHANGED_REPREPARE_REQUIRED'):
                self.cycle()
        self.assertEqual((authorized[0]['leg'],authorized[0]['quantity']),('STOP','40'))
        self.assertEqual(self.v.sent,1)  # only the earlier entry, never the stale stop
        self.assertEqual(len(self.v.requests),1)
        self.assertEqual(self.remaining()[0]['entry_quantity'],'60')
        self.assertIsNone(self.store.load(self.bucket)['pending'])

    def test_authorization_checkpoint_retains_another_workers_unknown_attempt(self):
        other=DispatchStore(PostgresJournal.for_ci(CI));started=[]
        def uncertain_other_attempt(state,proposal,policy):
            started.append(other.prepare_and_begin(state,proposal,AGENT,self.v.now())[1])
        with patch.object(self.v,'authorize',side_effect=uncertain_other_attempt):
            with self.assertRaisesRegex(DispatchError,'AUTHORIZED_INTENT_CHANGED_REPREPARE_REQUIRED'):
                self.cycle()
        self.assertEqual((len(started),self.v.sent),(1,0))
        state=self.store.load(self.bucket)
        self.assertEqual(state['pending'],started[0]['request_id'])
        self.assertEqual(self.store.request(state['pending']),started[0])
        self.assertEqual((started[0]['phase'],started[0]['attempts']),('OUTCOME_UNKNOWN',1))
    def test_overlapping_workers_cannot_send_same_entry_twice(self):
        workers=[m.Controller(DispatchStore(PostgresJournal.for_ci(CI)),self.v,ROUTES2)
                 for _ in range(2)]
        def cycle(worker):
            try:return worker.cycle(self.bucket,send=True)
            except JournalError:return {'status':'RELOAD_REQUIRED','order_requests_sent':0}
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(cycle,workers))
        self.assertEqual(self.v.sent,1)
        self.assertEqual(len(self.v.requests),1)
        self.assertEqual(sum(r['order_requests_sent'] for r in results),1)
        state=self.store.load(self.bucket)
        request=self.store.request(state['last_request'])
        self.assertEqual(request['attempts'],1)
        self.assertIn(request['phase'],('ACK_UNVERIFIED','OBSERVED','OUTCOME_UNKNOWN'))
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],1)
    def test_stale_before_begin_blocks_even_after_reservation(self):
        s=self.c.refresh(self.bucket);p=m.choose(s,ROUTES2,META,self.v.sample(A,'DOGE'),now_ms=self.v.now())
        s=self.store.reserve(s,p,self.v.now())
        with self.assertRaises(JournalError):self.store.begin(s,p,AGENT,self.v.now()+16000)
        self.assertEqual(self.v.sent,0)
    def test_second_card_registration_does_not_bypass_pending(self):
        self.entry();b,o=original(2);self.cards.record(o['card'])
        with self.assertRaises(JournalError):self.c.register(b['card_id'])
    def test_expired_general_alert_cannot_register(self):
        _,o=original(3)
        card=o['card']
        source=card['prepared']['source']
        expired=datetime.fromtimestamp((T-1000)/1000,timezone.utc).isoformat()
        card=trade_cards.prepare_card(source,META,rule_id='SOFTWARE_TEST',
            threshold_pct='1.5',record_kind='synthetic_test',source_expires_at=expired)
        self.cards.record(card)
        with self.assertRaisesRegex(DispatchError,'ORIGINAL_SOURCE_EXPIRED'):
            self.c.register(card['card_id'])
        self.assertEqual(self.v.sent,0)
    def test_receipt_without_correct_order_terms_cannot_bind(self):
        self.entry();self.v.orders['1000']['order']['reduceOnly']=True
        with self.assertRaises((JournalError,ValueError)):self.cycle(False)
        self.assertEqual(self.store.load(self.bucket)['bindings'],[])
    def test_old_observation_cannot_confirm_new_attempt(self):
        self.entry();self.v.orders['1000']['statusTimestamp']=T-1
        with self.assertRaises(DispatchError):self.cycle(False)
        self.assertEqual(self.store.load(self.bucket)['bindings'],[])
    def test_orphan_stop_cancellation_after_full_take(self):
        self.protect('100');self.v.fill('1002','100');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_ORPHAN_EXIT')
        self.cycle(False);self.assertTrue(self.remaining()[0]['closure_verified'])
        self.assertEqual(self.v.orders['1001']['status'],'canceled')
    def test_partial_exit_policy_not_silently_chosen(self):
        self.protect();self.v.fill('1002','40')
        with self.assertRaises(DispatchError):self.cycle()
        self.assertEqual(self.v.orders['1000']['status'],'open')
    def test_explicit_candidate_policy_cancels_only_own_pending_entry(self):
        self.protect();self.v.fill('1002','40');self.c.after_exit_policy=m.AFTER_EXIT
        self.cycle();self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.v.orders['1000']['status'],'canceled')
        self.cycle();self.cycle(False);self.assertTrue(self.remaining()[0]['closure_verified'])
    def test_modifying_display_copy_cannot_change_persistent_source(self):
        before=self.cards.load(self.b['card_id']);self.protect()
        s=self.store.load(self.bucket);s['originals'][self.b['card_id']]['card']['planning']['quantity']='999'
        self.assertEqual(self.cards.load(self.b['card_id']),before)
        self.assertEqual(self.store.load(self.bucket)['originals'][self.b['card_id']]['draft']['planned_quantity'],'100')
    def test_domain_mismatch_blocks_real_adapter_using_ci_records(self):
        with self.assertRaises(DispatchError):m.Controller(self.store,m.TestnetVenue({}),ROUTES2)
    def test_delayed_poll_records_nonzero_fill_to_protection_delay(self):
        self.entry();fill_at=self.v.fills[0]['time'];self.v.t+=30000
        self.cycle();request=self.v.requests[-1]
        self.assertGreaterEqual(request['attempt_at_ms']-fill_at,30000)
        self.cycle(False);saved=self.store.request(request['request_id'])
        self.assertGreater(saved['observed_at_ms'],saved['attempt_at_ms'])
    def test_rejected_reply_with_existing_order_is_a_conflict_not_success(self):
        self.entry();rid=self.store.load(self.bucket)['pending'];s=self.store.load(self.bucket)
        self.store.reply(s,dict(state='REJECTED',code='OTHER_REJECTION',oid=None),self.v.now())
        with self.assertRaises(DispatchError):self.cycle(False)
        self.assertEqual(self.store.request(rid)['phase'],'CONFLICT')
    def test_default_real_gate_and_no_configuration_write(self):
        v=m.TestnetVenue({});self.protect();s=self.store.load(self.bucket)
        with self.assertRaises(DispatchError):v.authorize(s,self.v.requests[0]['proposal'],'NOT_SELECTED')
    def test_second_same_market_card_waits_without_creating_an_exit_race(self):
        self.protect('100');b,o=original(2);self.cards.record(o['card']);self.c.register(b['card_id'])
        sent=self.v.sent
        self.cycle()
        self.assertEqual(self.v.sent,sent)
        state=self.store.load(self.bucket)
        self.assertIn(b['card_id'],state['originals'])
        self.assertEqual({binding['card_id'] for binding in state['bindings']},
                         {self.b['card_id']})


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ExpiredEntryDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();cards=CardStore(self.j);cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize()
        self.v=Venue();self.v.t=T-15000
        self.c=m.Controller(self.store,self.v,ROUTES2)
        self.b,record=original(side='SHORT',expiry_seconds=10)
        cards.record(record['card'])
        self.bucket=self.c.register(self.b['card_id'])['bucket']

    def test_partial_fill_racing_with_expiry_cancel_is_reconciled(self):
        first=self.c.cycle(self.bucket,send=True)
        self.assertEqual(first['status'],'ACCEPTED_UNVERIFIED')
        self.v.fill('1000','40')
        self.v.t=T+1
        self.v.cancel_race=lambda oid:self.v.fill(oid,'10')
        result=self.c.cycle(self.bucket,send=True)
        self.assertEqual(result['status'],'ACCEPTED_UNVERIFIED')
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_EXPIRED_ENTRY_REMAINDER')
        self.assertEqual(self.v.orders['1000']['status'],'canceled')
        self.assertEqual(self.v.orders['1000']['order']['sz'],'50')
        state=self.c.refresh(self.bucket)
        self.assertIsNone(state['pending'])
        self.assertEqual(state['evidence']['snapshot']['position_quantity'],'-50')
        self.assertFalse(state['evidence']['snapshot']['open_orders'])
        self.assertEqual(len(self.v.requests),2)

    def test_missed_target_ioc_closes_confirmed_amount_once(self):
        self.c.cycle(self.bucket,send=True)
        self.v.fill('1000','40');self.v.t=T+1
        self.c.cycle(self.bucket,send=True)  # Cancel the expired 60 first.
        self.c.refresh(self.bucket)
        self.v.t+=1  # The next public observation must be newer than the cancellation.
        self.v.sample=lambda account,symbol:dict(mark_price='9',at_ms=self.v.now())
        result=self.c.cycle(self.bucket,send=True)
        self.assertEqual(result['status'],'ACCEPTED_UNVERIFIED')
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CLOSE_PASSED_TAKE')
        self.assertEqual(self.v.requests[-1]['proposal']['action']['orders'][0]['t'],
                         {'limit':{'tif':'Ioc'}})
        self.c.cycle(self.bucket,send=False)
        state=self.store.load(self.bucket)
        view=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())
        self.assertTrue(view['cards'][0]['closure_verified'])
        self.assertEqual(self.v.orders['1001']['status'],'filled')
        sent=self.v.sent
        self.v.t+=1
        self.c.cycle(self.bucket,send=True)
        self.assertEqual(self.v.sent,sent)

    def test_partial_ioc_is_not_repeated_and_remaining_amount_is_protected(self):
        self.c.cycle(self.bucket,send=True)
        self.v.fill('1000','40');self.v.t=T+1
        self.c.cycle(self.bucket,send=True)
        self.c.refresh(self.bucket)
        self.v.t+=1
        self.v.ioc_fill='15'
        self.v.sample=lambda account,symbol:dict(mark_price='9',at_ms=self.v.now())
        self.c.cycle(self.bucket,send=True)
        self.assertEqual(self.v.orders['1001']['status'],'canceled')
        self.c.cycle(self.bucket,send=True)
        self.assertEqual((self.v.requests[-1]['proposal']['leg'],
                          self.v.requests[-1]['proposal']['quantity']),('STOP','25'))
        sent=self.v.sent
        with self.assertRaisesRegex(DispatchError,'LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW'):
            self.c.cycle(self.bucket,send=True)
        self.assertEqual(self.v.sent,sent)


class EntryPreflightBoundaryTests(NoExternal):
    def setup_case(self,side='LONG',expiry_seconds=600):
        b,record=original(side=side,expiry_seconds=expiry_seconds)
        snapshot=Venue().empty_snapshot(b['account'],'DOGE')
        state=dict(bucket=life.digest(['testnet',b['account'],'DOGE']),revision=1,
            bindings=[],originals={b['card_id']:record},pending=None,
            account=b['account'],symbol='DOGE',evidence=dict(bindings=[],snapshot=snapshot))
        self.clock=T
        env=dict(HL_TESTNET_RUNTIME_MODE='filled_card_controlled_v1',
            HL_TESTNET_FILLED_DISPATCH='approved_single_card_v1',
            HL_TESTNET_FILLED_CARD_ID=b['card_id'],
            HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS=str(T+60000),
            HL_TESTNET_FILLED_AFTER_EXIT_POLICY=m.AFTER_EXIT,
            HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',RENDER_SERVICE_ID=m.roles.SERVICE,
            HL_TESTNET_LONG_ACCOUNT_ADDRESS=A,HL_TESTNET_LONG_AGENT_ADDRESS=AGENT,
            HL_TESTNET_SHORT_ACCOUNT_ADDRESS=B,HL_TESTNET_SHORT_AGENT_ADDRESS='0x'+'4'*40)
        venue=m.TestnetVenue(env);venue.now=lambda:self.clock
        proposal=m.choose(state,ROUTES2,META,dict(mark_price='10',at_ms=T),now_ms=T)
        return state,venue,proposal
    @contextmanager
    def public_checks(self,budget=None,headroom=None):
        report=dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',test_plan_checked=True)
        with ExitStack() as stack:
            stack.enter_context(patch.object(m.TestnetVenue,'_fund_info_plan',return_value=Mock()))
            reader=stack.enter_context(patch.object(m.checks,'InfoReader',return_value=object()))
            budget_mock=stack.enter_context(patch.object(m.roles,'budget_for_role',
                side_effect=budget,return_value=report))
            headroom_mock=stack.enter_context(patch.object(m.roles,'entry_action_headroom',
                side_effect=headroom,return_value=100))
            yield reader,budget_mock,headroom_mock
    def test_both_roles_pass_fresh_preflight_without_state_mutation(self):
        for side in ('LONG','SHORT'):
            state,venue,proposal=self.setup_case(side)
            before=deepcopy((state,proposal,venue.env))
            with self.public_checks() as (_,budget,headroom):
                venue.authorize(state,proposal,m.AFTER_EXIT)
                self.assertEqual(budget.call_args.args[1],proposal['role'])
                self.assertEqual(headroom.call_args.args[0],proposal['account'])
            self.assertEqual(before,(state,proposal,venue.env));self.assertEqual(venue.sent,0)
    def test_parallel_sample_and_metadata_join_before_planning(self):
        state,venue,_=self.setup_case()
        barrier=threading.Barrier(2)
        def sample(*args):
            barrier.wait(timeout=2)
            return dict(mark_price='10',at_ms=T)
        def metadata():
            barrier.wait(timeout=2)
            return META
        store=Mock(domain='testnet');store.load.return_value=state
        controller=m.Controller(store,venue,ROUTES2,after_exit_policy=m.AFTER_EXIT)
        with patch.object(controller,'refresh',return_value=state), \
                patch.object(venue,'sample',side_effect=sample), \
                patch.object(venue,'metadata',side_effect=metadata), \
                patch.object(controller,'_plan_cycle',return_value=dict(status='NO_ACTION_NEEDED')) as plan:
            result=controller._prepare_cycle(state['bucket'],send=True,
                allow_new_entries=True,allowed_entry_card_id=None)
        self.assertEqual(result['status'],'NO_ACTION_NEEDED')
        self.assertEqual(plan.call_args.args[2:],(dict(mark_price='10',at_ms=T),META))
        self.assertEqual(venue.sent,0)
    def test_latched_emergency_yields_before_every_public_preparation_read(self):
        state,venue,_=self.setup_case()
        state['emergency']=dict(phase='ACTIVE')
        store=Mock(domain='testnet');store.load.return_value=state
        controller=m.Controller(store,venue,ROUTES2,after_exit_policy=m.AFTER_EXIT)
        with patch.object(controller,'refresh',side_effect=AssertionError('NO_PUBLIC_REFRESH')) as refresh, \
                patch.object(venue,'sample',side_effect=AssertionError('NO_PUBLIC_SAMPLE')) as sample, \
                patch.object(venue,'metadata',side_effect=AssertionError('NO_PUBLIC_META')) as metadata, \
                patch.object(venue,'authorize',side_effect=AssertionError('NO_AUTHORIZATION')) as authorize, \
                patch.object(venue,'send',side_effect=AssertionError('NO_SEND')) as send:
            result=controller.cycle(state['bucket'],send=True)
        self.assertEqual(result,dict(status='EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE',order_requests_sent=0))
        for check in (refresh,sample,metadata,authorize,send):check.assert_not_called()
        store.prepare_and_begin.assert_not_called()
    def test_empty_snapshot_overlaps_each_inventory_pair_but_preserves_two_passes(self):
        _,venue,_=self.setup_case()
        barrier=threading.Barrier(2);lock=threading.Lock();calls=[]
        class Reader:
            def read(self,kind,account):
                with lock:
                    calls.append((kind,account))
                barrier.wait(timeout=2)
                return [] if kind=='frontendOpenOrders' else dict(assetPositions=[])
        with patch.object(m.evidence,'PublicReader',return_value=Reader()), \
                patch.object(venue,'request_budget',return_value=object()):
            snap=venue.empty_snapshot(A,'DOGE')
        self.assertEqual(snap['at_ms'],T)
        self.assertEqual(snap['position_quantity'],'0')
        self.assertEqual(len(calls),4)
        for start in (0,2):
            self.assertEqual(set(calls[start:start+2]),
                {('frontendOpenOrders',A),('clearinghouseState',A)})
    def stream_authorization_case(self):
        state,venue,proposal=self.setup_case()
        venue.env['HL_TESTNET_RUNTIME_MODE']='long_stream_testnet_v1'
        card=state['originals'][proposal['card_id']]['card']
        card.update(record_kind='received_alert',account_role=proposal['role'])
        venue.store=Mock()
        venue.store.for_account.return_value=[]
        return state,venue,proposal
    def test_ownership_and_budget_overlap_and_both_finish_before_final_gate(self):
        state,venue,proposal=self.stream_authorization_case()
        barrier=threading.Barrier(2);finished=[]
        def owned(*args,**kwargs):
            barrier.wait(timeout=2);finished.append('owned');return True
        def budget(*args):
            barrier.wait(timeout=2);finished.append('budget')
            return dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',test_plan_checked=True)
        def headroom(*args):
            self.assertCountEqual(finished,['owned','budget']);return 100
        with self.public_checks(budget=budget,headroom=headroom), \
                patch.object(venue,'_gate',return_value=ROUTES2[proposal['role']]), \
                patch.object(m.roles,'wallet_for_role',return_value=object()), \
                patch('hl_testnet_runtime.long_stream_runtime._account_owned',side_effect=owned):
            venue.authorize(state,proposal,m.AFTER_EXIT)
        self.assertCountEqual(finished,['owned','budget']);self.assertEqual(venue.sent,0)
    def test_failed_parallel_ownership_joins_budget_before_returning(self):
        state,venue,proposal=self.stream_authorization_case()
        barrier=threading.Barrier(2);release=threading.Event();ended=threading.Event()
        failed=threading.Event()
        def owned(*args,**kwargs):
            barrier.wait(timeout=2)
            failed.set()
            raise DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')
        def budget(*args):
            barrier.wait(timeout=2)
            if not release.wait(timeout=2):raise AssertionError('JOIN_RELEASE_MISSING')
            ended.set()
            return dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',test_plan_checked=True)
        with self.public_checks(budget=budget) as (_,_,headroom), \
                patch.object(venue,'_gate',return_value=ROUTES2[proposal['role']]), \
                patch.object(m.roles,'wallet_for_role',return_value=object()), \
                patch('hl_testnet_runtime.long_stream_runtime._account_owned',side_effect=owned), \
                ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(venue.authorize,state,proposal,m.AFTER_EXIT)
            try:
                self.assertTrue(failed.wait(timeout=2))
                with self.assertRaises(TimeoutError):
                    future.result(timeout=.05)
                self.assertFalse(ended.is_set())
            finally:
                release.set()
            with self.assertRaisesRegex(DispatchError,'^UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY$'):
                future.result(timeout=3)
            self.assertTrue(ended.is_set());headroom.assert_not_called()
        self.assertEqual(venue.sent,0)
    def test_parallel_account_inventory_retains_unknown_order_block(self):
        from . import long_stream_runtime as stream
        _,venue,_=self.setup_case()
        barrier=threading.Barrier(2)
        class Reader:
            def read(self,kind,account):
                barrier.wait(timeout=2)
                return ([dict(coin='DOGE',oid=100)] if kind=='frontendOpenOrders'
                    else dict(assetPositions=[]))
        with patch.object(m.evidence,'PublicReader',return_value=Reader()), \
                self.assertRaisesRegex(DispatchError,'^UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY$'):
            stream._account_owned(venue,A,[],role='long_account')
        self.assertEqual(venue.sent,0)
    def test_old_future_and_missing_evidence_block_before_account_reads(self):
        for observed in (T-15001,T+1,None,True):
            state,venue,proposal=self.setup_case();proposal['observed_at_ms']=observed
            with self.public_checks() as (reader,budget,headroom),self.assertRaises(DispatchError):
                venue.authorize(state,proposal,m.AFTER_EXIT)
            reader.assert_not_called();budget.assert_not_called();headroom.assert_not_called()
    def test_slow_budget_blocks_before_controller_reservation_or_send(self):
        state,venue,proposal=self.setup_case()
        store=Mock(domain='testnet')
        store.load.return_value=state
        controller=m.Controller(store,venue,ROUTES2,after_exit_policy=m.AFTER_EXIT)
        def slow(*args):
            self.clock=T+15001
            return dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',test_plan_checked=True)
        with self.public_checks(budget=slow), \
                patch.object(controller,'refresh',return_value=state), \
                patch.object(m.residual,'retire_obsolete_unsent',return_value=state), \
                patch.object(m.half_cancel,'checkpoint',return_value=state), \
                patch.object(venue,'sample',return_value=dict(mark_price='10',at_ms=T)), \
                patch.object(venue,'metadata',return_value=META):
            with self.assertRaisesRegex(DispatchError,'^ENTRY_EVIDENCE_EXPIRED_BEFORE_RESERVATION$'):
                controller.cycle(state['bucket'],send=True)
        store.reserve.assert_not_called();store.begin.assert_not_called()
        self.assertEqual(venue.sent,0)
    def test_original_alert_expiry_rechecked_after_budget(self):
        state,venue,proposal=self.setup_case(expiry_seconds=21)
        def slow(*args):
            self.clock=T+1000
            return dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',test_plan_checked=True)
        with self.public_checks(budget=slow),self.assertRaisesRegex(DispatchError,'^NEW_TRIAL_SOURCE_NOT_FRESH$'):
            venue.authorize(state,proposal,m.AFTER_EXIT)
        self.assertEqual(venue.sent,0)
    def test_slow_allowance_read_and_expired_operator_approval_block(self):
        for delay,code in ((15001,'ENTRY_EVIDENCE_EXPIRED_BEFORE_RESERVATION'),
                           (60000,'TRIAL_ENTRY_APPROVAL_EXPIRED')):
            state,venue,proposal=self.setup_case()
            def slow(*args):
                self.clock=T+delay
                return 100
            with self.public_checks(headroom=slow),self.assertRaisesRegex(DispatchError,'^'+code+'$'):
                venue.authorize(state,proposal,m.AFTER_EXIT)
            self.assertEqual(venue.sent,0)
    def test_capacity_failure_blocks_and_does_not_touch_exit_authorization(self):
        state,venue,proposal=self.setup_case()
        with self.public_checks(headroom=m.checks.Blocked('ENTRY_ACTION_HEADROOM_INSUFFICIENT')):
            with self.assertRaisesRegex(m.checks.Blocked,'^ENTRY_ACTION_HEADROOM_INSUFFICIENT$'):
                venue.authorize(state,proposal,m.AFTER_EXIT)
        proposal['operation']='CREATE_EXIT'
        with self.public_checks() as (reader,budget,headroom):
            venue.authorize(state,proposal,m.AFTER_EXIT)
        reader.assert_not_called();budget.assert_not_called();headroom.assert_not_called()
    def test_low_budget_never_reads_extra_capacity(self):
        state,venue,proposal=self.setup_case()
        with self.public_checks(budget=lambda *args:dict(status='NO_USABLE_CAPACITY_OBSERVED')) as (_,_,headroom):
            with self.assertRaisesRegex(DispatchError,'^EXACT_ENTRY_BUDGET_NOT_VERIFIED$'):
                venue.authorize(state,proposal,m.AFTER_EXIT)
        headroom.assert_not_called()


class EntryInfoAdmissionTests(NoExternal):
    """Real finite permits and adapter coordination; only public venue I/O is doubled."""
    setup_case=EntryPreflightBoundaryTests.setup_case
    stream_authorization_case=EntryPreflightBoundaryTests.stream_authorization_case
    def trial_case(self):
        state,venue,proposal=self.stream_authorization_case()
        venue.env['HL_TESTNET_PROTECTION_TIMING_CARD_ID']=proposal['card_id']
        return state,venue,proposal

    def test_whole_trial_refusal_precedes_every_read_and_durable_attempt(self):
        from .request_budget import BudgetError
        state,venue,proposal=self.trial_case()
        venue.store.domain='testnet'
        controller=m.Controller(venue.store,venue,ROUTES2,after_exit_policy=m.AFTER_EXIT)
        venue.store.load.return_value=state
        with patch.object(venue,'_fund_info_plan',side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')) as fund, \
                patch.object(controller,'refresh',side_effect=AssertionError('NO_READ')) as refresh, \
                self.assertRaisesRegex(BudgetError,'^TESTNET_REQUEST_BUDGET_EXHAUSTED$'):
            controller.cycle(state['bucket'],send=True,allowed_entry_card_id=proposal['card_id'])
        from .request_budget import request_weight
        self.assertEqual(sum(request_weight('/info',b) for b in fund.call_args.args[0]),330)
        refresh.assert_not_called();venue.store.prepare_and_begin.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_atomic_superset_covers_every_account_mode_without_reusing_a_response(self):
        from . import request_budget as quota
        for mode in ('default','disabled','unifiedAccount'):
            state,venue,proposal=self.trial_case()
            account=m.roles.PHANTOM
            venue.env['HL_TESTNET_LONG_ACCOUNT_ADDRESS']=account
            state['account']=proposal['account']=account
            route={**ROUTES2['long_account'],'account':account}
            funded=[];claimed=[];released=[];read=[]
            owner=Mock()
            owner._claim_observation.side_effect=lambda token,weight,deadline:claimed.append((token,weight))
            owner._release_unclaimed_observation.side_effect=lambda entries:released.extend(entries)
            def fund(bodies):
                funded.append(deepcopy(bodies))
                entries=[(quota._info_plan_key(b),format(i+1,'032x'),quota.request_weight('/info',b))
                         for i,b in enumerate(bodies)]
                return quota.InfoPlan(owner,entries,'background',time.monotonic_ns()+15_000_000_000)
            def response(body):
                read.append(deepcopy(body));kind=body['type']
                if kind=='userAbstraction':return mode
                if kind=='userRole':return ({'role':'user'} if body['user']==account
                    else {'role':'agent','data':{'user':account}})
                if kind=='frontendOpenOrders':return []
                if kind=='clearinghouseState':return dict(assetPositions=[],withdrawable='100000',
                    marginSummary=dict(accountValue='100000',totalRawUsd='100000',
                        totalMarginUsed='0',totalNtlPos='0'))
                if kind=='spotClearinghouseState':return {'balances':([] if mode=='default'
                    else [dict(coin='USDC',token=0,total='100000',hold='0')])}
                if kind=='activeAssetData':return dict(user=account,coin='DOGE',markPx='10',
                    availableToTrade=['100000','100000'],maxTradeSzs=['100000','100000'],
                    leverage=dict(type='cross',value=1))
                if kind=='meta':return {'universe':[dict(name='DOGE',szDecimals=2,maxLeverage=50)]}
                if kind=='userRateLimit':return dict(nRequestsCap=100,nRequestsUsed=0,nRequestsSurplus=0)
                raise AssertionError('UNPLANNED_READ')
            class Info:
                def __init__(self,*,budget,parallel=False,priority='background',reuse=False):
                    self.budget,self.parallel,self.priority=budget,parallel,priority;self.calls=0
                def read(self,kind,*,user=None,coin=None):
                    body=dict(type=kind)
                    if user is not None:body['user']=user
                    if coin is not None:body['coin']=coin
                    permit=self.budget.acquire('/info',body,priority=self.priority)
                    permit.check();self.calls+=1;return response(body)
                def read_many(self,requests):
                    return [self.read(kind,**kw) for kind,kw in requests]
            class Public:
                def __init__(self,*,budget,priority='background',reuse_cycle=False):self.reader=Info(budget=budget,priority=priority)
                def read(self,kind,account):return self.reader.read(kind,user=account)
            with patch.object(venue,'_fund_info_plan',side_effect=fund), \
                    patch.object(venue,'_gate',return_value=route), \
                    patch.object(m.roles,'wallet_for_role',return_value=object()), \
                    patch.object(m.checks,'InfoReader',Info),patch.object(m.evidence,'PublicReader',Public), \
                    patch.object(m.roles,'route_for',return_value=route):
                with venue.entry_read_admission(state,send=True,allow_new_entries=True,
                        allowed_entry_card_id=proposal['card_id']):
                    venue.empty_snapshot(account,'DOGE')
                    m.joined_public_reads(lambda:venue.sample(account,'DOGE'),venue.metadata)
                    venue.authorize(state,proposal,m.AFTER_EXIT)
            self.assertEqual(len(funded),1)
            self.assertEqual(sum(quota.request_weight('/info',b) for b in funded[0]),330)
            self.assertEqual(sum(b['type']=='userAbstraction' for b in read),2)
            self.assertEqual(sum(b['type']=='frontendOpenOrders' for b in read),3)
            self.assertEqual(sum(b['type']=='userRateLimit' for b in read),1)
            self.assertEqual(len(claimed),len(read));self.assertEqual(len(claimed)+len(released),len(funded[0]))
            self.assertEqual(venue.sent,0)

    def test_public_plan_is_local_to_its_venue_and_exchange_has_a_separate_permit(self):
        state,venue,proposal=self.trial_case();batch=Mock();raw=Mock();raw.acquire.return_value=Mock()
        other=m.TestnetVenue({})
        with patch.object(venue,'_fund_info_plan',return_value=batch), \
                patch.object(m.roles,'route_for',return_value=ROUTES2['long_account']), \
                patch('hl_testnet_runtime.request_budget.Budget.from_env',return_value=raw):
            with venue.entry_read_admission(state,send=True,allow_new_entries=True,
                    allowed_entry_card_id=proposal['card_id']):
                self.assertEqual(m.joined_public_reads(venue.request_budget,other.request_budget),(batch,raw))
                self.assertEqual(venue._public_priority(),'background')
                admission=venue.reserve_transport(proposal)
                self.assertIsInstance(admission,m.TransportAdmission)
                batch.acquire.assert_not_called()
                raw.acquire.assert_called_once()
                self.assertEqual(raw.acquire.call_args.kwargs['priority'],'background')
            self.assertIs(venue.request_budget(),raw)
        batch.close.assert_called_once()

    def test_preview_disabled_entries_and_other_cards_never_fund_trial_admission(self):
        state,venue,proposal=self.trial_case()
        for send,allowed,cid in ((False,True,proposal['card_id']),
                (True,False,proposal['card_id']),(True,True,'f'*64)):
            with patch.object(venue,'_fund_info_plan',side_effect=AssertionError('NO_TRIAL_ADMISSION')):
                with venue.entry_read_admission(state,send=send,allow_new_entries=allowed,
                                                allowed_entry_card_id=cid):pass

    def test_coordinator_shares_only_same_store_routes_and_notification_feed_before_start(self):
        state=state_from_case();store=Mock(domain='software');store.load.return_value=state
        first=m.Controller(store,Venue(),ROUTES2);second=m.Controller(store,Venue(),ROUTES2)
        feed=object();first.venue.fill_wakeups=second.venue.fill_wakeups=feed
        first.venue.env={'entry_enabled':'false'};second.venue.env={'exact_trial':'true'}
        second.share_observation_coordinator(first)
        self.assertIs(second._observation_flights,first._observation_flights)
        self.assertEqual(first.venue.env,{'entry_enabled':'false'})
        self.assertEqual(second.venue.env,{'exact_trial':'true'})
        with patch.object(second,'_refresh_once',return_value=state):second.refresh(state['bucket'])
        with self.assertRaisesRegex(DispatchError,'^OBSERVATION_COORDINATOR_ALREADY_STARTED$'):
            second.share_observation_coordinator(first)
        third=m.Controller(Mock(domain='software'),Venue(),ROUTES2);third.venue.fill_wakeups=feed
        with self.assertRaisesRegex(DispatchError,'^OBSERVATION_COORDINATOR_SCOPE_MISMATCH$'):
            third.share_observation_coordinator(first)

    def test_separate_trial_and_supervisor_join_one_committed_observation(self):
        first,state,result,current=ObservationCoordinationTests.controller(self)
        second=m.Controller(first.store,Venue(),ROUTES2)
        first.venue.fill_wakeups=second.venue.fill_wakeups=object()
        second.share_observation_coordinator(first)
        started=threading.Event();release=threading.Event();joining=threading.Event()
        def collect(bucket,prior):
            started.set()
            if not release.wait(2):raise AssertionError('TEST_COLLECTION_NOT_RELEASED')
            current[0]=deepcopy(result);return deepcopy(result)
        with patch.object(first,'_refresh_once',side_effect=collect) as reads, \
                patch.object(second,'_refresh_once',side_effect=AssertionError('NO_DUPLICATE_READ')), \
                ThreadPoolExecutor(max_workers=2) as pool:
            owner=pool.submit(first.refresh,state['bucket'])
            self.assertTrue(started.wait(1))
            flight=first._observation_flights[state['bucket']][0];wait=flight.done.wait
            def joined(seconds):joining.set();return wait(seconds)
            with patch.object(flight.done,'wait',side_effect=joined):
                observer=pool.submit(second.refresh,state['bucket'],emergency=True)
                self.assertTrue(joining.wait(1));release.set()
                self.assertEqual(owner.result(2),result);self.assertEqual(observer.result(2),result)
            reads.assert_called_once()
        self.assertEqual(current[0]['evidence']['snapshot']['at_ms'],T)
        self.assertFalse(first._observation_flights)


if __name__=='__main__':unittest.main()
