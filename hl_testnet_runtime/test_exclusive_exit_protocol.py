"""Allocation protocol tests, not an exchange execution or trading-policy test."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import inspect
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from . import exclusive_exit_protocol as m, card_lifecycle as life
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore
from .test_card_lifecycle import T, fill, order, terminal
from .test_filled_quantity_dispatch import NoExternal, state_from_case

CI = os.environ.get('HL_JOURNAL_CI_URL')


def state(side='LONG', second=True):
    s = state_from_case(q='100', stop='100', side=side)
    s['domain'] = 'software'
    if second:
        other = state_from_case(n=2, q='100', stop='100', side=side)
        s['bindings'] += other['bindings']; s['originals'].update(other['originals'])
        for key in ('fills', 'open_orders', 'terminal_orders'):
            s['evidence']['snapshot'][key] += other['evidence']['snapshot'][key]
        s['evidence']['snapshot']['position_quantity'] = '-200' if side=='SHORT' else '200'
    s['evidence']['bindings'] = s['bindings']
    return s


def targets(s, switch=True):
    result = {b['card_id']:'STOP' for b in s['bindings']}
    if switch: result[s['bindings'][0]['card_id']] = 'TAKE_PROFIT'
    return result


def cancel_snapshot(s, *, late='0', at=T+4):
    snap = deepcopy(s['evidence']['snapshot']); b = s['bindings'][0]
    oid = b['orders']['STOP'][0]
    snap['open_orders'] = [o for o in snap['open_orders'] if o['oid']!=oid]
    if Decimal(late):
        snap['fills'].append({**fill(b, 'STOP', qty=late, fid='race-fill'), 'at_ms':at-1})
        signed = Decimal(late) * (-1 if b['side']=='LONG' else 1)
        snap['position_quantity'] = str(Decimal(snap['position_quantity'])+signed)
    total = sum((Decimal(f['quantity']) for f in snap['fills'] if f['oid']==oid), Decimal(0))
    snap['terminal_orders'].append({**terminal(b, 'STOP', str(total)),
        'state':'FILLED' if total==100 else 'CANCELED', 'at_ms':at-1})
    snap['at_ms'] = at
    return snap


def after_cancel(s, **kw):
    new = deepcopy(s); snap=cancel_snapshot(s, **kw)
    new['evidence'] = dict(bindings=new['bindings'], snapshot=snap)
    new['revision'] += 1
    return new


def placement(s, req, *, at, fill_quantity='0'):
    p=req['proposal']; wire=p['wire_action']['orders'][0]
    oid='9001'; snap=deepcopy(s['evidence']['snapshot'])
    b=deepcopy(next(b for b in s['bindings'] if b['card_id']==p['card_id']))
    b['orders'][p['leg']] = [oid]
    q=Decimal(wire['s']); done=Decimal(fill_quantity)
    if done:
        snap['fills'].append({**fill(b,p['leg'],qty=str(done),fid='new-exit-fill'),'at_ms':at-1})
        snap['position_quantity']=str(Decimal(snap['position_quantity'])+done*(-1 if b['side']=='LONG' else 1))
    if done==q:
        snap['terminal_orders'].append({**terminal(b,p['leg'],str(done)),'at_ms':at-1})
    else:
        snap['open_orders'].append(order(b,p['leg'],str(q-done)))
    snap['at_ms']=at
    raw=dict(status='order',order=dict(status='filled' if done==q else 'open',statusTimestamp=at-1,
        order=dict(oid=int(oid),cloid=wire['c'],coin=p['symbol'],side='B' if wire['b'] else 'A',
            origSz=wire['s'],sz=str(q-done),limitPx=wire['p'],reduceOnly=True,isTrigger=p['leg']=='STOP',
            isPositionTpsl=False,orderType='Stop Market' if p['leg']=='STOP' else 'Limit',
            triggerPx=wire['p'] if p['leg']=='STOP' else '0')))
    return snap, raw


class ExclusivePureTests(NoExternal):
    def test_switch_first_cancels_exact_owner_stop(self):
        s=state(); before=deepcopy(s); p=m.plan(s,targets(s),now_ms=T)
        self.assertEqual(p['step']['operation'],'CANCEL_BEFORE_SWITCH')
        self.assertEqual(p['step']['old_oid'],s['bindings'][0]['orders']['STOP'][0])
        self.assertEqual(p['step']['wire_action']['cancels'][0]['o'],int(p['step']['old_oid']))
        self.assertEqual(s,before);self.assertFalse(p['dispatch_enabled'])
        self.assertFalse(p['trading_policy_selected']);self.assertFalse(p['gap_free_guaranteed'])

    def test_holding_stop_does_not_create_simultaneous_take(self):
        s=state();self.assertIsNone(m.plan(s,targets(s,False),now_ms=T)['step'])

    def test_after_terminal_stop_plan_is_plain_tp_at_original_price(self):
        s=after_cancel(state());p=m.plan(s,targets(s),now_ms=T+4)['step']
        self.assertEqual(p['operation'],'PLACE_ONLY_EXIT')
        wire=p['wire_action']['orders'][0]
        self.assertEqual(wire['t'],{'limit':{'tif':'Gtc'}})
        self.assertEqual(wire['p'],s['bindings'][0]['prices']['take_profit'])
        self.assertEqual(wire['s'],'100');self.assertTrue(wire['r'])

    def test_30_unit_cancel_race_leaves_only_70_for_replacement(self):
        s=after_cancel(state(),late='30');p=m.plan(s,targets(s),now_ms=T+4)
        self.assertEqual(p['step']['quantity'],'70')
        self.assertTrue(p['original_lifecycle_requires_review'])

    def test_full_fill_before_cancel_has_no_replacement(self):
        s=after_cancel(state(),late='100')
        self.assertIsNone(m.plan(s,targets(s),now_ms=T+4)['step'])

    def test_pending_cannot_be_bypassed_by_changing_desired_leg(self):
        s=state();s['pending']='a'*64
        for choices in (targets(s),targets(s,False)):
            r=m.plan(s,choices,now_ms=T)
            self.assertEqual(r['status'],'PENDING_REQUEST_MUST_BE_RECONCILED');self.assertIsNone(r['step'])

    def test_initial_two_full_size_siblings_are_not_silently_adopted(self):
        s=state(second=False);b=s['bindings'][0];b['orders']['TAKE_PROFIT']=['12']
        s['evidence']['snapshot']['open_orders'].append(order(b,'TAKE_PROFIT'))
        with self.assertRaisesRegex(DispatchError,'^EXISTING_EXIT_CAPACITY_NOT_EXCLUSIVE$'):
            m.plan(s,targets(s),now_ms=T)

    def test_short_is_exact_buy_to_reduce_after_old_exit_terminal(self):
        s=after_cancel(state('SHORT'))
        p=m.plan(s,targets(s),now_ms=T+4)['step']
        self.assertTrue(p['wire_action']['orders'][0]['b'])
        self.assertEqual(p['account'],s['bindings'][0]['account'])

    def test_stale_or_incomplete_evidence_rejected(self):
        for field,val in (('history_complete',False),('orders_complete',False),('at_ms',T-16000)):
            s=state();s['evidence']['snapshot'][field]=val
            with self.assertRaises(ValueError):m.plan(s,targets(s),now_ms=T)

    def test_testnet_domain_and_display_payload_rejected(self):
        s=state();s['domain']='testnet'
        for bad in (s,dict(display_copy=True),None):
            with self.assertRaises(DispatchError):m.plan(bad,{},now_ms=T)

    def test_foreign_or_missing_card_target_rejected(self):
        s=state()
        for bad in ({},{'a'*64:'STOP'}, {k:'BUY' for k in targets(s)}, None):
            with self.assertRaises(DispatchError):m.plan(s,bad,now_ms=T)

    def test_duplicate_fill_reports_do_not_multiply_exit_allocation(self):
        s=after_cancel(state(),late='30');s['evidence']['snapshot']['fills']*=2
        self.assertEqual(m.plan(s,targets(s),now_ms=T+4)['step']['quantity'],'70')

    def test_all_racing_fills_bound_total_exit_to_owner_allocation(self):
        # 101 separate cancellation outcomes, not 101 independently counted tests.
        for spent in range(101):
            s=after_cancel(state(),late=str(spent));p=m.plan(s,targets(s),now_ms=T+4)['step']
            reserved=Decimal(p['quantity']) if p else Decimal(0)
            self.assertLessEqual(Decimal(spent)+reserved,100)
            untouched=[o for o in s['evidence']['snapshot']['open_orders']
                       if o['oid'] in s['bindings'][1]['orders']['STOP']]
            self.assertEqual(untouched[0]['quantity'],'100')

    def test_counterexample_to_two_native_independent_full_size_exits(self):
        owner=100;another=100;tp_fill=100;remaining_account=owner+another-tp_fill
        stop_fill=min(100,remaining_account)  # Account reduce-only still allows it.
        self.assertGreater(tp_fill+stop_fill,owner)
        # This is a rejected architecture, not a claim the protocol repaired it.

    def test_source_contains_no_transport_keys_worker_or_app_entrypoint(self):
        src=inspect.getsource(m)
        for text in ('import os','import http','import requests','sign_l1_action','def send(','def start(','def application('):
            self.assertNotIn(text,src)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ExclusiveDurabilityTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for schema in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.r=m.Rehearsal(self.store)
        initial=state();empty=self.store.create_bucket(initial['account'],initial['symbol'])
        def seed(conn,v):
            for name in ('bindings','originals','evidence'):v[name]=deepcopy(initial[name])
        self.s=self.store.change(empty['bucket'],empty['revision'],'SOFTWARE_FIXTURE',T,seed)
        self.bucket=self.s['bucket'];self.choices=targets(self.s)

    def begun(self):
        prepared=self.r.prepare(self.s,self.choices,now_ms=T)
        return self.r.begin(prepared,now_ms=T+1)

    def canceled(self,late='0'):
        s=self.begun();return self.r.confirm(s,cancel_snapshot(self.s,late=late),now_ms=T+4)

    def test_full_handoff_on_real_store_keeps_other_card_unchanged(self):
        other=deepcopy(self.s['bindings'][1]);s=self.canceled('30')
        p=self.r.prepare(s,self.choices,now_ms=T+5);b=self.r.begin(p,now_ms=T+6)
        req=self.store.request(b['pending']);self.assertEqual(req['proposal']['quantity'],'70')
        snap,raw=placement(b,req,at=T+9,fill_quantity='70')
        done=self.r.confirm(b,snap,now_ms=T+9,order_lookup=raw)
        self.assertIsNone(done['pending']);self.assertEqual(done['bindings'][1],other)
        rows=life.review(done['bindings'],done['evidence']['snapshot'],now_ms=T+9)['cards']
        byid={c['card_id']:c for c in rows}
        self.assertEqual(byid[self.s['bindings'][0]['card_id']]['remaining_quantity'],'0')
        self.assertEqual(byid[other['card_id']]['remaining_quantity'],'100')
        self.assertEqual(done['evidence']['snapshot']['position_quantity'],'100')
        self.assertEqual(self.store.request(done['last_request'])['nonce'],None)

    def test_lost_cancel_reply_and_new_process_cannot_release_next_leg(self):
        s=self.begun();rid=s['pending']
        script="from hl_testnet_runtime.postgres_journal import PostgresJournal; from hl_testnet_runtime.filled_dispatch_store import DispatchStore; import os; r=DispatchStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])).request('"+rid+"'); print(r['phase'],r['attempts'],r['nonce'])"
        out=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,check=True,timeout=15)
        self.assertEqual(out.stdout.strip(),'OUTCOME_UNKNOWN 1 None')
        restored=m.Rehearsal(DispatchStore(PostgresJournal.for_ci(CI)))
        with self.assertRaises(DispatchError):restored.begin(s,now_ms=T+86400000)
        with self.assertRaises(DispatchError):restored.prepare(s,self.choices,now_ms=T+2)
        final=restored.confirm(s,cancel_snapshot(self.s),now_ms=T+4)
        self.assertIsNone(final['pending'])

    def test_receipt_alone_does_not_free_capacity(self):
        s=self.begun();s=self.store.reply(s,dict(state='ACCEPTED_UNVERIFIED',code=None,oid=None),T+2)
        snap=deepcopy(s['evidence']['snapshot']);snap['at_ms']=T+4
        with self.assertRaises(DispatchError):self.r.confirm(s,snap,now_ms=T+4)
        self.assertEqual(self.store.load(self.bucket)['pending'],s['pending'])

    def test_allocation_consumed_before_cancel_cannot_reopen_card(self):
        s=self.canceled('100');p=m.plan(s,self.choices,now_ms=T+4)
        self.assertIsNone(p['step'])
        self.assertEqual(self.store.request(s['last_request'])['confirmation']['terminal_state'],'FILLED')
        self.assertFalse(self.store.request(s['last_request'])['confirmation']['cancellation_caused_terminal_state'])

    def test_wrong_card_order_missing_finality_does_not_unlock(self):
        s=self.begun();snap=cancel_snapshot(self.s)
        snap['terminal_orders']=[o for o in snap['terminal_orders'] if o['oid']!=self.s['bindings'][0]['orders']['STOP'][0]]
        with self.assertRaises(ValueError):self.r.confirm(s,snap,now_ms=T+4)
        self.assertIsNotNone(self.store.load(self.bucket)['pending'])

    def test_concurrent_preparations_allow_one_request(self):
        def prep(_):
            try:self.r.prepare(self.s,self.choices,now_ms=T);return True
            except JournalError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:values=list(pool.map(prep,range(4)))
        self.assertEqual(sum(values),1)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],1)

    def test_changed_old_fill_cannot_validate_a_handoff(self):
        s=self.begun();snap=cancel_snapshot(self.s);snap['fills'][0]['price']='9.8'
        with self.assertRaises(DispatchError):self.r.confirm(s,snap,now_ms=T+4)
        self.assertEqual(self.store.request(s['pending'])['phase'],'OUTCOME_UNKNOWN')

    def test_stale_prepared_evidence_is_not_sent_after_restart(self):
        p=self.r.prepare(self.s,self.choices,now_ms=T)
        with self.assertRaises(DispatchError):self.r.begin(p,now_ms=T+15001)
        self.assertEqual(self.store.request(p['pending'])['attempts'],0)

    def test_duplicate_confirmation_does_not_repeat_execution(self):
        s=self.begun();snap=cancel_snapshot(self.s)
        final=self.r.confirm(s,snap,now_ms=T+4)
        with self.assertRaises(DispatchError):self.r.confirm(s,snap,now_ms=T+5)
        self.assertEqual(self.store.load(self.bucket)['revision'],final['revision'])

    def test_place_confirmation_requires_frozen_cloid_and_original_terms(self):
        s=self.canceled();p=self.r.prepare(s,self.choices,now_ms=T+5);b=self.r.begin(p,now_ms=T+6)
        req=self.store.request(b['pending']);snap,raw=placement(b,req,at=T+9)
        for key,bad in (('cloid','0x'+'a'*32),('limitPx','99'),('origSz','999'),('reduceOnly',False),('isTrigger',True)):
            corrupted=deepcopy(raw);corrupted['order']['order'][key]=bad
            with self.assertRaises(DispatchError):self.r.confirm(b,snap,now_ms=T+9,order_lookup=corrupted)
        done=self.r.confirm(b,snap,now_ms=T+9,order_lookup=raw)
        self.assertIsNone(done['pending'])

    def test_protocol_cannot_use_actual_account_store(self):
        with patch.object(self.store,'domain','testnet'):
            with self.assertRaises(DispatchError):m.Rehearsal(self.store)

    def test_protocol_request_is_rejected_by_actual_sender_before_wallet(self):
        from .filled_quantity_dispatch import TestnetVenue
        s=self.begun();req=self.store.request(s['pending']);v=TestnetVenue({})
        with self.assertRaises(DispatchError):v._fresh_attempt(req)
        self.assertEqual(v.sent,0)
