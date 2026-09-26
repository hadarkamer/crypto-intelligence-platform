"""Real controller and PostgreSQL; ONLY venue I/O is substituted.

Never invoke real signing or HTTP. Database tests require the existing strictly
loopback CI DSN. Software rows cannot be loaded into the Testnet adapter.
"""
from copy import deepcopy
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import inspect
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from . import filled_quantity_dispatch as m, card_lifecycle as life
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore
from .test_filled_quantity_exits import original, case, META, ROUTES
from .test_card_lifecycle import T, A, B

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
        self.lose_reply=False;self.cancel_race=None;self.calls=0
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
            o=action['orders'][0];oid=str(1000+len(self.orders));leg=p['leg']
            raw=dict(oid=int(oid),cloid=o['c'],coin=p['symbol'],side='B' if o['b'] else 'A',
                limitPx=o['p'],sz=o['s'],origSz=o['s'],reduceOnly=o['r'],isTrigger=leg!='ENTRY',
                triggerPx=o.get('t',{}).get('trigger',{}).get('triggerPx','0'),isPositionTpsl=False,
                orderType='Limit' if leg=='ENTRY' else 'Stop Market' if leg=='STOP' else 'Take Profit Limit')
            self.orders[oid]=dict(order=raw,status='open',statusTimestamp=self.t,account=p['account'])
            reply=dict(status='ok',response=dict(type='order',data=dict(statuses=[dict(resting=dict(oid=int(oid)))])))
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
        if kind=='orderStatus': return dict(status='order',order=deepcopy(self.orders[oid]))
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
    def test_40_of_100_creates_only_card_stop_and_preserves_entry(self):
        s=state_from_case();before=deepcopy(s);p=self.select(s)
        self.assertEqual((p['leg'],p['quantity']),('STOP','40'))
        self.assertEqual(p['action']['grouping'],'na');self.assertTrue(p['action']['orders'][0]['r'])
        self.assertEqual(s,before)
    def test_quantity_change_is_cancel_exact_old_order_not_create_duplicate(self):
        s=state_from_case(q='60',stop='40',take='40');p=self.select(s)
        self.assertEqual(p['operation'],'CANCEL_FOR_RESIZE')
        self.assertEqual(p['action']['type'],'cancel');self.assertEqual(p['old_oid'],s['bindings'][0]['orders']['STOP'][0])
        self.assertNotIn('always_place',json.dumps(p));self.assertNotIn('modify',json.dumps(p))
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
    def test_preview_records_no_requests_and_no_attempts(self):
        r=self.cycle(False);self.assertEqual(r['status'],'PREVIEW_ONLY');self.assertEqual(self.v.sent,0)
        with self.j._transaction() as conn:self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],0)
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
    def test_resize_waits_for_cancel_evidence_and_recomputes(self):
        self.protect();self.v.fill('1000','20');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_FOR_RESIZE')
        # Another 10 fills while the old stop's cancellation is being reconciled.
        self.v.fill('1000','10');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'70')
        self.cycle();self.cycle();self.cycle(False)
        v=self.remaining()[0];self.assertEqual((v['stop_quantity_observed'],v['take_profit_quantity_observed']),('70','70'))
        self.assertEqual(self.v.orders['1000']['order']['sz'],'30')
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
        real=self.store.begin
        def lost(*a,**kw):real(*a,**kw);raise JournalError('SIMULATED_COMMIT_ACK_LOSS')
        with patch.object(self.store,'begin',side_effect=lost):
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
    def test_stale_before_begin_blocks_even_after_reservation(self):
        s=self.c.refresh(self.bucket);p=m.choose(s,ROUTES2,META,self.v.sample(A,'DOGE'),now_ms=self.v.now())
        s=self.store.reserve(s,p,self.v.now())
        with self.assertRaises(JournalError):self.store.begin(s,p,AGENT,self.v.now()+16000)
        self.assertEqual(self.v.sent,0)
    def test_second_card_registration_does_not_bypass_pending(self):
        self.entry();b,o=original(2);self.cards.record(o['card'])
        with self.assertRaises(JournalError):self.c.register(b['card_id'])
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
    def test_raw_double_exit_race_is_detected_not_charged_to_second_card(self):
        self.protect('100');b,o=original(2);self.cards.record(o['card']);self.c.register(b['card_id'])
        # The new storage fence blocks constructing this unsafe overlap normally.
        with self.assertRaisesRegex(DispatchError,'SHARED_MARKET_INDEPENDENT_PAIR_NOT_ISOLATED'):
            self.cycle()
        # Arrange PRE-EXISTING legacy exposure in the SOFTWARE venue only.
        # Restore the guard before the cancellation race and retain all original
        # incident/attribution assertions; do not call this race "prevented".
        with patch('hl_testnet_runtime.residual_exit_fence.validate_proposal',return_value=True):
            self.cycle();self.v.fill('1003','100');self.cycle();self.cycle();self.cycle(False)
        self.v.fill('1002','100')
        self.v.cancel_race=lambda oid:self.v.fill(oid,'100')
        self.cycle() # venue fills first card stop before cancellation can take effect
        with self.assertRaises(DispatchError):self.cycle(False)
        views=self.remaining();first=next(v for v in views if v['card_id']==self.b['card_id'])
        second=next(v for v in views if v['card_id']==b['card_id'])
        self.assertIn('EXIT_EXCEEDS_CARD_QUANTITY',first['issues'])
        self.assertEqual(second['remaining_quantity'],'100');self.assertFalse(first['closure_verified'])


if __name__=='__main__':unittest.main()
