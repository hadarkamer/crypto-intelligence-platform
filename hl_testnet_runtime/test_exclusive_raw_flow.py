"""Raw venue-shaped evidence + existing PostgreSQL protocol. NO external I/O.

The venue double only applies submitted quantities and serves /info-shaped data.
It does not enforce internal card ownership: that must be proved by the protocol.
The same production parser is used for every reconciliation, including a resting
plain TP; no fabricated triggerPrice or synthetic closure is passed to it.
"""
from copy import deepcopy
from decimal import Decimal
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from . import exclusive_exit_protocol as m, card_lifecycle as life
from . import card_sync_evidence as evidence
from . import test_exclusive_exit_protocol as fixture
from .test_filled_quantity_dispatch import NoExternal, state_from_case
from .test_card_lifecycle import T, order, fill, terminal
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore

CI=os.environ.get('HL_JOURNAL_CI_URL')


def source(side='LONG', partial=False):
    s=fixture.state(side)
    if partial:
        one=state_from_case(q='40',stop='40',side=side)
        other=state_from_case(n=2,q='100',stop='100',side=side)
        one['domain']='software';one['bindings']+=other['bindings'];one['originals'].update(other['originals'])
        for key in ('fills','open_orders','terminal_orders'):
            one['evidence']['snapshot'][key]+=other['evidence']['snapshot'][key]
        one['evidence']['snapshot']['position_quantity']='140' if side=='LONG' else '-140'
        one['evidence']['bindings']=one['bindings'];s=one
    for f in s['evidence']['snapshot']['fills']:
        f['fill_id']='hl:'+f['fill_id']
    return s


class Book:
    domain='software'
    def __init__(self,s):
        self.t=T;self.rows={};self.fills=[];self.requests=[];self.calls=0;self.next_oid=9001
        snap=s['evidence']['snapshot'];self.account=s['account']
        for f in snap['fills']:
            self.fills.append(dict(coin=f['symbol'],oid=int(f['oid']),tid=int(f['fill_id'].split(':')[-1]),
                time=f['at_ms'],sz=f['quantity'],px=f['price'],side=f['side'],fee=f['fee'],feeToken=f['fee_token']))
        opens={o['oid']:o for o in snap['open_orders']};terms={o['oid']:o for o in snap['terminal_orders']}
        for b in s['bindings']:
            for leg,ids in b['orders'].items():
                for oid in ids:
                    active=opens.get(oid);done=sum((Decimal(f['sz']) for f in self.fills if str(f['oid'])==oid),Decimal(0))
                    qty=done+Decimal(active['quantity']) if active else max(done,Decimal(b['planned_quantity']))
                    px=b['prices']['entry' if leg=='ENTRY' else 'stop' if leg=='STOP' else 'take_profit']
                    raw=dict(oid=int(oid),cloid='0x'+format(int(oid),'032x'),coin=b['symbol'],
                        side=('B' if b['side']=='LONG' else 'A') if leg=='ENTRY' else ('A' if b['side']=='LONG' else 'B'),
                        limitPx=px,sz=active['quantity'] if active else '0',origSz=str(qty),
                        reduceOnly=leg!='ENTRY',isTrigger=leg!='ENTRY',isPositionTpsl=False,
                        orderType='Limit' if leg=='ENTRY' else 'Stop Market' if leg=='STOP' else 'Take Profit Limit',
                        triggerPx='0' if leg=='ENTRY' else px)
                    self.rows[oid]=dict(order=raw,status='open' if active else terms[oid]['state'].lower(),
                                        statusTimestamp=T-500 if active else terms[oid]['at_ms'])
    def now(self):return self.t
    def advance(self):self.t+=1;return self.t
    def lookup(self,cloid):
        found=[r for r in self.rows.values() if r['order']['cloid']==cloid]
        return dict(status='order',order=deepcopy(found[0])) if len(found)==1 else dict(status='unknownOid')
    def apply(self,req):
        if req['domain']!='software' or req['nonce'] is not None:raise AssertionError('SOFTWARE_ONLY')
        self.requests.append(deepcopy(req));p=req['proposal'];action=p['wire_action'];self.t+=2
        if action['type']=='cancel':
            oid=str(action['cancels'][0]['o'])
            if self.rows[oid]['status']=='open':self.rows[oid].update(status='canceled',statusTimestamp=self.t)
            return oid
        o=action['orders'][0];oid=str(self.next_oid);self.next_oid+=1;trigger='trigger' in o['t']
        raw=dict(oid=int(oid),cloid=o['c'],coin=p['symbol'],side='B' if o['b'] else 'A',
            limitPx=o['p'],sz=o['s'],origSz=o['s'],reduceOnly=o['r'],isTrigger=trigger,
            isPositionTpsl=False,orderType='Stop Market' if trigger else 'Limit',
            triggerPx=o['t']['trigger']['triggerPx'] if trigger else '0')
        self.rows[oid]=dict(order=raw,status='open',statusTimestamp=self.t)
        return oid
    def fill(self,oid,qty):
        row=self.rows[oid];o=row['order'];q=Decimal(qty);self.t+=1
        if row['status']!='open' or not 0<q<=Decimal(o['sz']):raise AssertionError('INVALID_DOUBLE_FILL')
        o['sz']=str(Decimal(o['sz'])-q)
        tid=max([f['tid'] for f in self.fills]+[0])+1
        self.fills.append(dict(coin=o['coin'],oid=int(oid),tid=tid,time=self.t,sz=str(q),
            px=o['limitPx'],side=o['side'],fee='0.01',feeToken='USDC'))
        if Decimal(o['sz'])==0:row.update(status='filled',statusTimestamp=self.t)
    def read(self,kind,account,*,oid=None,start=None,end=None):
        self.calls+=1
        if account!=self.account:raise AssertionError('WRONG_ACCOUNT')
        if kind=='orderStatus':return dict(status='order',order=deepcopy(self.rows[oid]))
        if kind=='frontendOpenOrders':return [deepcopy(r['order']) for r in self.rows.values() if r['status']=='open']
        if kind=='userFillsByTime':return [deepcopy(f) for f in self.fills if start<=f['time']<=end]
        if kind=='clearinghouseState':
            unique={f['tid']:f for f in self.fills}
            q=sum((Decimal(f['sz'])*(1 if f['side']=='B' else -1) for f in unique.values()),Decimal(0))
            return dict(assetPositions=[dict(position=dict(coin='DOGE',szi=str(q)))])
        raise AssertionError('UNEXPECTED_READ')


class ExclusiveReadModelTests(NoExternal):
    def test_plain_limit_requires_explicit_owned_read_model(self):
        s=fixture.state(second=False);b=s['bindings'][0];snap=s['evidence']['snapshot']
        b['orders']['TAKE_PROFIT']=['12'];snap['open_orders']=[{**order(b,'TAKE_PROFIT'),
            'order_type':'LIMIT','trigger_price':None}]
        snap['terminal_orders'].append(terminal(b,'STOP','0'))
        old=life.review([b],snap,now_ms=T)['cards'][0]
        self.assertIn('ORDER_TERMS_MISMATCH',old['issues'])
        actual=life.review([b],snap,now_ms=T,plain_take_profit_oids=['12'])['cards'][0]
        self.assertNotIn('ORDER_TERMS_MISMATCH',actual['issues'])
        self.assertEqual(actual['take_profit_quantity_observed'],'100')
        self.assertIn('STOP_COVERAGE_MISSING',actual['issues'])
    def test_stop_or_foreign_id_cannot_be_relabeled_plain_take(self):
        s=fixture.state();b=s['bindings'][0]
        for ids in ([b['orders']['STOP'][0]],['999999'],'12',None):
            with self.assertRaises(ValueError):
                life.review(s['bindings'],s['evidence']['snapshot'],now_ms=T,plain_take_profit_oids=ids)
    def test_trigger_cannot_disguise_itself_as_registered_plain_limit(self):
        s=fixture.state(second=False);b=s['bindings'][0];snap=s['evidence']['snapshot']
        b['orders']['TAKE_PROFIT']=['12'];snap['open_orders']=[order(b,'TAKE_PROFIT')]
        snap['terminal_orders'].append(terminal(b,'STOP','0'))
        r=life.review([b],snap,now_ms=T,plain_take_profit_oids=['12'])['cards'][0]
        self.assertIn('ORDER_TERMS_MISMATCH',r['issues'])
    def test_closed_partial_entry_remainder_is_not_skipped(self):
        s=state_from_case(q='40');s['domain']='software';b=s['bindings'][0];snap=s['evidence']['snapshot']
        b['orders']['STOP']=['11'];snap['fills'].append(fill(b,'STOP',qty='40'))
        snap['terminal_orders'].append(terminal(b,'STOP','40'));snap['position_quantity']='0'
        p=m.plan(s,{b['card_id']:'STOP'},now_ms=T)['step']
        self.assertEqual(p['operation'],'CANCEL_ENTRY_AFTER_EXIT');self.assertEqual(p['old_oid'],b['orders']['ENTRY'][0])
    def test_any_exit_cancels_entry_even_when_remaining_stop_is_correct_size(self):
        s=state_from_case(q='40',stop='40');s['domain']='software';b=s['bindings'][0];snap=s['evidence']['snapshot']
        snap['fills'].append(fill(b,'STOP',qty='10'));snap['position_quantity']='30'
        for o in snap['open_orders']:
            if o['oid'] in b['orders']['STOP']:o['quantity']='30'
        p=m.plan(s,{b['card_id']:'STOP'},now_ms=T)['step']
        self.assertEqual(p['operation'],'CANCEL_ENTRY_AFTER_EXIT')
    def test_fills_after_terminal_are_not_treated_as_fresh_capacity(self):
        s=fixture.after_cancel(fixture.state(),late='30')
        s['evidence']['snapshot']['fills'][-1]['at_ms']=T+4
        with self.assertRaises(DispatchError):m.plan(s,fixture.targets(s),now_ms=T+4)
    def test_all_integer_cancel_races_both_directions_preserve_other_card(self):
        for side in ('LONG','SHORT'):
            for q in range(101):
                s=fixture.after_cancel(fixture.state(side),late=str(q))
                p=m.plan(s,fixture.targets(s),now_ms=T+4)['step']
                self.assertEqual(Decimal(q)+(Decimal(p['quantity']) if p else 0),100)
                own=s['bindings'][1]['orders']['STOP'][0]
                self.assertEqual(next(o['quantity'] for o in s['evidence']['snapshot']['open_orders'] if o['oid']==own),'100')
    def test_twenty_same_market_cards_are_not_a_one_card_fixture(self):
        s=fixture.state(second=False)
        for n in range(2,21):
            other=state_from_case(n=n,q='100',stop='100')
            s['bindings']+=other['bindings'];s['originals'].update(other['originals'])
            for name in ('fills','open_orders','terminal_orders'):
                s['evidence']['snapshot'][name]+=other['evidence']['snapshot'][name]
        s['evidence']['snapshot']['position_quantity']='2000';before=deepcopy(s)
        p=m.plan(s,fixture.targets(s),now_ms=T)['step']
        self.assertEqual(p['card_id'],s['bindings'][0]['card_id']);self.assertEqual(s,before)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ExclusiveRawFlowTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for schema in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        self.j.bootstrap();CardStore(self.j).initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.r=m.Rehearsal(self.store)
        self.seed(source())
    def seed(self,s):
        self.book=Book(s);empty=self.store.create_bucket(s['account'],s['symbol'])
        def copy(conn,v):
            for field in ('bindings','originals','evidence'):v[field]=deepcopy(s[field])
        self.s=self.store.change(empty['bucket'],empty['revision'],'RAW_SOFTWARE_FIXTURE',T,copy)
        self.cid=self.s['bindings'][0]['card_id'];self.other=self.s['bindings'][1]['card_id']
        self.choices=fixture.targets(self.s)
    def prepare(self,choices=None):
        self.book.advance()
        self.s=self.r.prepare(self.s,choices or self.choices,now_ms=self.book.now())
        self.book.advance();self.s=self.r.begin(self.s,now_ms=self.book.now())
        return self.store.request(self.s['pending'])
    def confirm(self,req):
        self.book.advance();raw=None
        if req['proposal']['operation']=='PLACE_ONLY_EXIT':
            raw=self.book.lookup(req['proposal']['wire_action']['orders'][0]['c'])
        snap=self.r.read_snapshot(self.s,self.book,clock=self.book.now,elapsed=lambda:0,order_lookup=raw)
        self.s=self.r.confirm(self.s,snap,now_ms=self.book.now(),order_lookup=raw)
        return snap
    def step(self,choices=None,race=None):
        req=self.prepare(choices)
        if race:race(req)
        oid=self.book.apply(req);self.confirm(req)
        return oid,req
    def refresh(self):
        self.book.advance();snap=self.r.read_snapshot(self.s,self.book,clock=self.book.now,elapsed=lambda:0)
        self.s=self.r.observe(self.s,snap,now_ms=self.book.now());return snap
    def views(self):
        _,_,rows,_=m._read(self.s,self.book.now());return rows
    def to_take(self):
        self.step();oid,_=self.step();return oid
    def test_raw_stop_to_take_partial_take_back_to_stop_closes_only_owner(self):
        other_before=deepcopy(self.s['bindings'][1]);tp=self.to_take()
        observed=next(o for o in self.s['evidence']['snapshot']['open_orders'] if o['oid']==tp)
        self.assertEqual(observed['order_type'],'LIMIT');self.assertIsNone(observed['trigger_price'])
        self.book.fill(tp,'20');self.refresh()
        stop_choices=fixture.targets(self.s,False);self.step(stop_choices);sl,_=self.step(stop_choices)
        self.assertEqual(self.book.rows[sl]['order']['origSz'],'80')
        self.book.fill(sl,'80');self.refresh();rows=self.views()
        self.assertEqual(rows[self.cid]['remaining_quantity'],'0')
        self.assertEqual(rows[self.other]['remaining_quantity'],'100')
        self.assertEqual(self.s['bindings'][1],other_before)
        self.assertEqual(self.s['evidence']['snapshot']['position_quantity'],'100')
        self.assertIn('BOTH_EXIT_LEGS_FILLED_REVIEW',rows[self.cid]['issues'])
    def test_short_round_trip_uses_buy_to_reduce_only_own_remaining(self):
        self.seed(source('SHORT'));tp=self.to_take();self.book.fill(tp,'30');self.refresh()
        choices=fixture.targets(self.s,False);self.step(choices);sl,_=self.step(choices)
        self.assertEqual(self.book.rows[sl]['order']['side'],'B')
        self.assertEqual(self.book.rows[sl]['order']['origSz'],'70')
        self.book.fill(sl,'70');self.refresh()
        self.assertEqual(self.views()[self.other]['remaining_quantity'],'100')
        self.assertEqual(self.s['evidence']['snapshot']['position_quantity'],'-100')
    def test_stop_fill_during_cancel_reduces_take_capacity_before_placement(self):
        self.step(race=lambda req:self.book.fill(req['proposal']['old_oid'],'30'))
        tp,_=self.step();self.assertEqual(self.book.rows[tp]['order']['origSz'],'70')
        self.book.fill(tp,'70');self.refresh()
        self.assertEqual(self.views()[self.other]['remaining_quantity'],'100')
    def test_full_stop_fill_during_cancel_never_reopens_closed_owner(self):
        self.step(race=lambda req:self.book.fill(req['proposal']['old_oid'],'100'))
        self.assertIsNone(m.plan(self.s,self.choices,now_ms=self.book.now())['step'])
        self.assertEqual(len(self.book.requests),1);self.assertEqual(self.views()[self.other]['remaining_quantity'],'100')
    def test_pending_take_cannot_be_overridden_by_another_card_target(self):
        self.step();req=self.prepare();self.book.apply(req)
        choices={k:'TAKE_PROFIT' for k in self.choices}
        self.assertIsNone(m.plan(self.s,choices,now_ms=self.book.now())['step'])
        with self.assertRaises(DispatchError):self.r.prepare(self.s,choices,now_ms=self.book.now())
        self.confirm(req);self.assertEqual(len(self.book.requests),2)
    def test_restart_reloads_plain_ownership_and_unknown_attempt(self):
        tp=self.to_take();choices=fixture.targets(self.s,False);req=self.prepare(choices)
        script="import os; from hl_testnet_runtime.postgres_journal import PostgresJournal; from hl_testnet_runtime.filled_dispatch_store import DispatchStore; s=DispatchStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])); r=s.request('"+req['request_id']+"'); b=s.load('"+self.s['bucket']+"'); print(r['phase'],r['attempts'],len(b['exclusive_exit_contracts']))"
        out=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,check=True,timeout=15)
        self.assertEqual(out.stdout.strip(),'OUTCOME_UNKNOWN 1 1')
        self.r=m.Rehearsal(DispatchStore(PostgresJournal.for_ci(CI)))
        with self.assertRaises(DispatchError):self.r.begin(self.s,now_ms=self.book.now()+1)
        self.book.apply(req);self.confirm(req)
        self.assertEqual(self.book.rows[tp]['status'],'canceled')
    def test_receipt_cannot_substitute_for_terminal_order(self):
        req=self.prepare();self.book.advance()
        self.s=self.store.reply(self.s,dict(state='ACCEPTED_UNVERIFIED',code=None,oid=None),self.book.now())
        with self.assertRaises(ValueError):self.confirm(req)
        self.assertIsNotNone(self.store.load(self.s['bucket'])['pending'])
        self.book.apply(req);self.confirm(req)
    def test_unattempted_plan_can_be_aborted_but_uncertain_one_cannot(self):
        self.book.advance();self.s=self.r.prepare(self.s,self.choices,now_ms=self.book.now())
        first=self.s['pending'];self.book.advance();self.s=self.r.abort_unsent(self.s,now_ms=self.book.now())
        self.assertEqual(self.store.request(first)['phase'],'ABORTED_UNSENT')
        req=self.prepare()
        with self.assertRaises(DispatchError):self.r.abort_unsent(self.s,now_ms=self.book.now())
        self.assertEqual(self.store.request(req['request_id'])['attempts'],1)
        self.assertEqual(self.book.requests,[])
    def test_new_fills_invalidate_unsent_plan_without_erasing_it(self):
        self.book.advance();self.s=self.r.prepare(self.s,self.choices,now_ms=self.book.now())
        self.book.fill(self.s['bindings'][0]['orders']['STOP'][0],'10');self.book.advance()
        snap=evidence.collect(self.s['evidence'],self.book,clock=self.book.now,elapsed=lambda:0)['snapshot']
        self.s=self.r.observe(self.s,snap,now_ms=self.book.now())
        with self.assertRaises(DispatchError):self.r.begin(self.s,now_ms=self.book.now())
        self.assertEqual(self.store.request(self.s['pending'])['attempts'],0)
        self.s=self.r.abort_unsent(self.s,now_ms=self.book.now())
        self.assertIsNone(self.s['pending'])
    def test_corrupted_frozen_wire_rejected_even_with_recomputed_db_digest(self):
        self.book.advance();self.s=self.r.prepare(self.s,self.choices,now_ms=self.book.now())
        rid=self.s['pending'];req=self.store.request(rid)
        req['proposal']['wire_action']['cancels'][0]['o']=int(self.s['bindings'][1]['orders']['STOP'][0])
        with self.j._transaction() as conn:
            conn.execute(f'UPDATE {SCHEMA}.requests SET value=%s::jsonb,digest=%s WHERE request_id=%s',
                         (life.encoded(req),life.digest(req),rid))
        with self.assertRaises(DispatchError):self.r.begin(self.s,now_ms=self.book.now()+1)
        self.assertEqual(self.book.requests,[])
    def test_raw_unknown_order_does_not_create_ownership(self):
        self.step();req=self.prepare()
        with self.assertRaises(DispatchError):
            self.r.read_snapshot(self.s,self.book,clock=self.book.now,elapsed=lambda:0,order_lookup={'status':'unknownOid'})
        self.assertEqual(len(self.s['bindings'][0]['orders']['TAKE_PROFIT']),0)
    def test_plain_take_has_durable_three_way_ownership(self):
        tp=self.to_take()
        with self.j._transaction() as conn:
            row=conn.execute(f'SELECT card_id,leg,cloid FROM {SCHEMA}.ownership WHERE account=%s AND oid=%s',
                             (self.s['account'],tp)).fetchone()
        self.assertEqual(row,(self.cid,'TAKE_PROFIT',self.book.rows[tp]['order']['cloid']))
    def test_ownership_collision_rolls_back_adoption(self):
        self.step();req=self.prepare();oid=self.book.apply(req)
        with self.j._transaction() as conn:
            conn.execute(f'INSERT INTO {SCHEMA}.ownership VALUES(%s,%s,%s,%s,%s,%s)',
                (self.s['account'],oid,'0x'+'e'*32,self.other,'STOP',req['request_id']))
        with self.assertRaises(DispatchError):self.confirm(req)
        current=self.store.load(self.s['bucket'])
        self.assertIsNotNone(current['pending']);self.assertNotIn(oid,current.get('exclusive_exit_contracts',{}))
    def test_raw_inventory_type_cannot_disagree_with_order_status(self):
        self.step();req=self.prepare();self.book.apply(req);self.book.advance()
        raw=self.book.lookup(req['proposal']['wire_action']['orders'][0]['c']);real=self.book.read
        def wrong(kind,*args,**kw):
            out=real(kind,*args,**kw)
            if kind=='frontendOpenOrders':
                for o in out:
                    if o['oid']==raw['order']['order']['oid']:o['isTrigger']=True
            return out
        with patch.object(self.book,'read',side_effect=wrong):
            with self.assertRaises(ValueError):
                self.r.read_snapshot(self.s,self.book,clock=self.book.now,elapsed=lambda:0,order_lookup=raw)
    def test_plain_limit_is_not_accepted_by_default_legacy_collector(self):
        self.to_take();self.book.advance()
        with self.assertRaises(ValueError):
            evidence.collect(self.s['evidence'],self.book,clock=self.book.now,elapsed=lambda:0)
        snap=self.r.read_snapshot(self.s,self.book,clock=self.book.now,elapsed=lambda:0)
        self.assertTrue(any(o['order_type']=='LIMIT' and o['reduce_only'] for o in snap['open_orders']))
    def test_duplicate_raw_fill_reports_do_not_multiply_close(self):
        tp=self.to_take();self.book.fill(tp,'20');self.book.fills*=2;self.refresh()
        self.assertEqual(self.views()[self.cid]['remaining_quantity'],'80')
        self.assertEqual(self.views()[self.other]['remaining_quantity'],'100')
    def test_failure_after_begin_commit_cannot_be_retried(self):
        self.book.advance();self.s=self.r.prepare(self.s,self.choices,now_ms=self.book.now());real=self.store.change
        def lost(*a,**kw):real(*a,**kw);raise JournalError('SIMULATED_LOST_COMMIT_ACK')
        self.book.advance()
        with patch.object(self.store,'change',side_effect=lost):
            with self.assertRaises(JournalError):self.r.begin(self.s,now_ms=self.book.now())
        self.s=self.store.load(self.s['bucket'])
        with self.assertRaises(DispatchError):self.r.begin(self.s,now_ms=self.book.now()+1)
        self.assertEqual(self.book.requests,[])
    def test_real_public_reader_cannot_be_injected_into_software_run(self):
        with self.assertRaises(DispatchError):
            self.r.read_snapshot(self.s,evidence.PublicReader(),clock=self.book.now,elapsed=lambda:0)
    def test_partial_entry_then_exit_cancels_remainder_before_switch(self):
        self.seed(source('SHORT',partial=True));sl=self.s['bindings'][0]['orders']['STOP'][0]
        self.book.fill(sl,'10');self.refresh();entry=self.s['bindings'][0]['orders']['ENTRY'][0]
        _,req=self.step(race=lambda req:self.book.fill(entry,'5'))
        self.assertEqual(req['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.book.rows[entry]['status'],'canceled')
        self.assertEqual(self.views()[self.cid]['remaining_quantity'],'35')
        self.step();tp,_=self.step()
        self.assertEqual(self.book.rows[tp]['order']['origSz'],'35')
        self.assertEqual(self.views()[self.other]['remaining_quantity'],'100')
    def test_full_exit_with_waiting_entry_still_cancels_and_preserves_other(self):
        self.seed(source('SHORT',partial=True));sl=self.s['bindings'][0]['orders']['STOP'][0]
        entry=self.s['bindings'][0]['orders']['ENTRY'][0];self.book.fill(sl,'40');self.refresh()
        _,req=self.step();self.assertEqual(req['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.book.rows[entry]['status'],'canceled')
        self.assertEqual(self.views()[self.other]['remaining_quantity'],'100')
        self.assertIsNone(m.plan(self.s,self.choices,now_ms=self.book.now())['step'])
