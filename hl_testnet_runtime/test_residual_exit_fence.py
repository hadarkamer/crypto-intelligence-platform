"""Residual-order regression and adversarial shared-position tests.

All exchange events are synthetic. Database tests use disposable loopback
PostgreSQL and the actual controller/journal. No user keys or real HTTP.
The double-fill counterexample is intentionally NOT called a prevented fill.
"""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import inspect
import os
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch
from . import residual_exit_fence as fence, filled_quantity_dispatch as dispatch
from . import card_lifecycle as life
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore
from .test_filled_quantity_dispatch import NoExternal, Venue, ROUTES2, META, state_from_case, AGENT
from .test_filled_quantity_exits import original
from .test_card_lifecycle import fill, terminal, T

CI = os.environ.get('HL_JOURNAL_CI_URL')


def merged(one, two):
    result = deepcopy(one); other = deepcopy(two)
    result['bindings'] += other['bindings']; result['originals'].update(other['originals'])
    s = result['evidence']['snapshot']; o = other['evidence']['snapshot']
    for key in ('fills','open_orders','terminal_orders'): s[key] += o[key]
    s['position_quantity'] = str(Decimal(s['position_quantity']) + Decimal(o['position_quantity']))
    result['evidence']['bindings'] = result['bindings']
    return result


def orphan(side='LONG', leg='TAKE_PROFIT', *, second=True):
    s = state_from_case(q='100', side=side, stop='100', take='100')
    b = s['bindings'][0]; snap = s['evidence']['snapshot']
    snap['fills'].append(fill(b,leg,qty='100'))
    snap['terminal_orders'].append(terminal(b,leg,'100'))
    snap['open_orders'] = [o for o in snap['open_orders'] if o['oid'] != b['orders'][leg][0]]
    snap['position_quantity'] = '0'
    if second: s = merged(s,state_from_case(n=2,q='60',side=side,stop='60',take='60'))
    return s


def select(state, mark='10'):
    at = state['evidence']['snapshot']['at_ms']
    return dispatch.choose(state,ROUTES2,META,dict(mark_price=mark,at_ms=at),now_ms=at,
                           after_exit_policy=dispatch.AFTER_EXIT)


def new_entry(state, n=3):
    b, rec = original(n)
    state['originals'][b['card_id']] = rec
    return dict(card_id=b['card_id'],leg='ENTRY',operation='ENTRY',account=state['account'],
        symbol=state['symbol'],role=b['role'],quantity=rec['draft']['planned_quantity'],
        action=deepcopy(rec['draft']['entry_action']),basis=life.digest(state['evidence']))


class ResidualPureTests(NoExternal):
    def unsent_entry_case(self):
        binding, record = original(7, expiry_seconds=30)
        account = binding['account']
        state = dict(bucket=life.digest(['testnet',account,'DOGE']), account=account,
            symbol='DOGE',revision=2,bindings=[],originals={binding['card_id']:record},
            evidence=dict(bindings=[],snapshot=Venue().empty_snapshot(account,'DOGE')),
            pending=None)
        proposal = dispatch.choose(state,ROUTES2,META,dict(mark_price='10',at_ms=T),
            now_ms=T,after_exit_policy=dispatch.AFTER_EXIT)
        request = dict(request_id='f'*64,bucket=state['bucket'],phase='PREPARED',
            attempts=0,nonce=None,attempt_at_ms=None,reply=None,observed_oid=None,
            prepared_at_ms=T,proposal=proposal)
        state['pending'] = request['request_id']
        state['evidence']['snapshot']['at_ms'] = T+10000
        store = Mock();store.request.return_value=request
        return store,state,request

    def test_unsent_entry_retirement_does_not_use_operator_grant_or_old_evidence(self):
        for at,now in ((T+9999,T+9999),(T+9999,T+10000),(T,T+10000)):
            store,state,_=self.unsent_entry_case()
            state['evidence']['snapshot']['at_ms']=at
            self.assertEqual(fence.retire_obsolete_unsent(store,state,now_ms=now),state)
            store.change.assert_not_called()

    def test_unsent_entry_retirement_preserves_every_attempt_or_ack_barrier(self):
        mutations=({'attempts':1},{'attempts':False},{'nonce':T},
            {'attempt_at_ms':T},{'reply':{'state':'OUTCOME_UNKNOWN'}},
            {'observed_oid':'1000'},{'phase':'OUTCOME_UNKNOWN'},
            {'phase':'ACK_UNVERIFIED'},{'phase':'REJECTED'},{'phase':'CONFLICT'})
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                store,state,request=self.unsent_entry_case();request.update(mutation)
                self.assertEqual(fence.retire_obsolete_unsent(store,state,now_ms=T+10000),state)
                store.change.assert_not_called()

    def test_unsent_entry_retirement_requires_complete_fresh_public_empty_market(self):
        for mutation in ({'history_complete':False},{'orders_complete':False},
                         {'position_quantity':'1'},{'at_ms':T-6000}):
            with self.subTest(mutation=mutation):
                store,state,_=self.unsent_entry_case()
                state['evidence']['snapshot'].update(mutation)
                with self.assertRaises(life.LifecycleError):
                    fence.retire_obsolete_unsent(store,state,now_ms=T+10000)
                store.change.assert_not_called()

    def test_unsent_entry_retirement_rejects_changed_owner_source_or_wire(self):
        for field,value in (('account','0x'+'4'*40),('symbol','BTC'),
                            ('role','short_account'),('source_expires_at','changed'),
                            ('source_at','changed'),('quantity','999')):
            with self.subTest(field=field):
                store,state,request=self.unsent_entry_case()
                request['proposal'][field]=value
                with self.assertRaisesRegex(life.LifecycleError,'OWNER_OR_SOURCE_MISMATCH'):
                    fence.retire_obsolete_unsent(store,state,now_ms=T+10000)
                store.change.assert_not_called()
        store,state,request=self.unsent_entry_case()
        request['proposal']['action']['orders'][0]['p']='11'
        with self.assertRaisesRegex(life.LifecycleError,'OWNER_OR_SOURCE_MISMATCH'):
            fence.retire_obsolete_unsent(store,state,now_ms=T+10000)
        store.change.assert_not_called()

    def test_tp_and_stop_cleanup_in_both_accounts_targets_owner_only(self):
        for side in ('LONG','SHORT'):
            for leg in ('TAKE_PROFIT','STOP'):
                s=orphan(side,leg);before=deepcopy(s);p=select(s)
                b=s['bindings'][0];other='STOP' if leg=='TAKE_PROFIT' else 'TAKE_PROFIT'
                self.assertEqual(p['operation'],'CANCEL_ORPHAN_EXIT')
                self.assertEqual(p['old_oid'],b['orders'][other][0]);self.assertEqual(p['card_id'],b['card_id'])
                self.assertTrue(fence.validate_proposal(s,p,now_ms=T));self.assertEqual(s,before)

    def test_another_cards_crossed_target_does_not_suppress_cleanup(self):
        s=orphan(second=False)
        s=merged(s,state_from_case(n=2,q='60'))
        p=select(s,mark='9')
        self.assertEqual(p['operation'],'CANCEL_ORPHAN_EXIT')
        self.assertEqual(p['card_id'],s['bindings'][0]['card_id'])

    def test_original_card_cannot_be_changed_by_display_data(self):
        s=orphan();s['bindings'][0]['prices']['stop']='9.7'
        with self.assertRaises(life.LifecycleError): select(s)
        with self.assertRaises(life.LifecycleError): fence.exposure(dict(display_copy=True),now_ms=T)

    def test_missing_history_or_stale_snapshot_is_not_closure(self):
        for field in ('history_complete','orders_complete'):
            s=orphan();s['evidence']['snapshot'][field]=False
            with self.assertRaises(life.LifecycleError): select(s)
        with self.assertRaises(life.LifecycleError): fence.exposure(orphan(),now_ms=T+16000)

    def test_flat_exchange_position_alone_does_not_cancel_a_card_stop(self):
        s=state_from_case(q='100',stop='100',take='100');s['evidence']['snapshot']['position_quantity']='0'
        self.assertIsNone(fence.cleanup(s,now_ms=T))

    def test_new_entry_is_blocked_while_old_card_has_a_residual(self):
        s=orphan();p=new_entry(s)
        with self.assertRaisesRegex(life.LifecycleError,'RESIDUAL_EXITS_MUST_BE_RETIRED'):
            fence.validate_proposal(s,p,now_ms=T)

    def test_independent_pair_is_counted_twice_not_as_an_oco(self):
        s=state_from_case(q='100',stop='100',take='100')
        report=fence.exposure(s,now_ms=T);row=report['cards'][0]
        self.assertEqual(row['outstanding_exit_quantity'],'200')
        self.assertEqual(row['excess_independent_capacity'],'100')
        self.assertFalse(report['native_per_card_oco_verified'])
        p=new_entry(s)
        with self.assertRaisesRegex(life.LifecycleError,'SHARED_SYMBOL_PREDECESSOR_NOT_FINAL'):
            fence.validate_proposal(s,p,now_ms=T)

    def test_even_a_single_resting_exit_blocks_next_card_entry(self):
        s=state_from_case(q='100',stop='100');p=new_entry(s)
        self.assertEqual(select(s)['leg'],'TAKE_PROFIT')
        with self.assertRaisesRegex(life.LifecycleError,'SHARED_SYMBOL_PREDECESSOR_NOT_FINAL'):
            fence.validate_proposal(s,p,now_ms=T)

    def test_guard_does_not_change_single_card_tp_sl_policy(self):
        s=state_from_case(q='100',stop='100');p=select(s)
        self.assertEqual(p['leg'],'TAKE_PROFIT')
        self.assertTrue(fence.validate_proposal(s,p,now_ms=T))

    def test_shared_card_cannot_reserve_two_independent_full_size_exits(self):
        s=merged(state_from_case(q='100',stop='100'),state_from_case(n=2,q='60',stop='60',take='60'))
        p=select(s)
        self.assertEqual(p['leg'],'TAKE_PROFIT')
        with self.assertRaisesRegex(life.LifecycleError,'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED'):
            fence.validate_proposal(s,p,now_ms=T)

    def test_no_arbitrary_card_count_is_added_to_the_data_model(self):
        s=state_from_case(q='100',stop='100',take='100')
        for n in range(2,51):s=merged(s,state_from_case(n=n,q='100',stop='100',take='100'))
        self.assertEqual(len(fence.exposure(s,now_ms=T)['cards']),50)

    def test_cancel_cannot_be_redirected_to_another_card(self):
        s=orphan();p=select(s);p['action']['cancels'][0]['o']=int(s['bindings'][1]['orders']['STOP'][0])
        with self.assertRaisesRegex(life.LifecycleError,'CANCEL_TARGET_NOT_OWNED'):
            fence.validate_proposal(s,p,now_ms=T)

    def test_cancel_finality_requires_exact_final_fill_total(self):
        s=orphan(second=False);p=select(s);b=s['bindings'][0];snap=deepcopy(s['evidence']['snapshot'])
        snap['at_ms']=T+10;snap['open_orders']=[]
        snap['terminal_orders'].append({**terminal(b,'STOP','0'),'at_ms':T+5})
        req=dict(proposal=p,attempt_at_ms=T+1)
        checked=fence.confirm_cancel(s,req,snap,s['bindings'],now_ms=T+10)
        self.assertTrue(checked['exact_order_terminal']);self.assertFalse(checked['cancellation_caused_terminal_state'])
        snap['terminal_orders'][-1]['filled_quantity']='1'
        with self.assertRaisesRegex(life.LifecycleError,'CANCEL_FINAL_FILL_TOTAL_NOT_VERIFIED'):
            fence.confirm_cancel(s,req,snap,s['bindings'],now_ms=T+10)

    def test_same_order_open_and_terminal_is_not_cancel_confirmation(self):
        s=orphan(second=False);p=select(s);b=s['bindings'][0];snap=deepcopy(s['evidence']['snapshot']);snap['at_ms']=T+10
        snap['terminal_orders'].append({**terminal(b,'STOP','0'),'at_ms':T+5})
        with self.assertRaisesRegex(life.LifecycleError,'CANCEL_TARGET_STILL_WORKING'):
            fence.confirm_cancel(s,dict(proposal=p),snap,s['bindings'],now_ms=T+10)

    def test_missing_sibling_does_not_mean_it_was_canceled(self):
        s=orphan(second=False);p=select(s);snap=deepcopy(s['evidence']['snapshot']);snap['at_ms']=T+10;snap['open_orders']=[]
        with self.assertRaisesRegex(life.LifecycleError,'CANCEL_TERMINAL_EVIDENCE_REQUIRED'):
            fence.confirm_cancel(s,dict(proposal=p),snap,s['bindings'],now_ms=T+10)

    def test_double_fill_counterexample_preserves_damage_not_fake_isolation(self):
        s=orphan();b=s['bindings'][0];snap=s['evidence']['snapshot']
        snap['fills'].append(fill(b,'STOP',qty='40',fid='race-stop'))
        next(o for o in snap['open_orders'] if o['oid']==b['orders']['STOP'][0])['quantity']='60'
        snap['position_quantity']='20'
        report=fence.exposure(s,now_ms=T)
        owner=next(c for c in report['cards'] if c['card_id']==b['card_id'])
        other=next(c for c in report['cards'] if c['card_id']!=b['card_id'])
        self.assertEqual(owner['remaining_quantity'],'-40');self.assertEqual(other['remaining_quantity'],'60')
        self.assertFalse(report['shared_market_release_authorized'])
        self.assertEqual(select(s)['card_id'],b['card_id'])

    def test_one_budget_invariant_holds_for_arbitrary_two_fill_sequences(self):
        # Mathematical software model only: fills consume both remaining card
        # quantity and outstanding order quantity. No venue OCO is assumed.
        cases=0
        for remaining in range(1,12):
            for a in range(remaining+1):
                for b in range(remaining-a+1):
                    for fa in range(a+1):
                        for fb in range(b+1):
                            self.assertGreaterEqual(remaining-fa-fb,0)
                            self.assertLessEqual((a-fa)+(b-fb),remaining-fa-fb)
                            cases+=1
        self.assertGreater(cases,1000)

    def test_module_does_not_add_sender_app_controls_or_new_scheduler(self):
        code=inspect.getsource(fence)
        for text in ('import requests','import http','import threading','sign_l1_action','def application','def start('):
            self.assertNotIn(text,code)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ResidualDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=Venue()
        self.c=dispatch.Controller(self.store,self.v,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        self.b,self.o=original();self.cards.record(self.o['card'])
        self.bucket=self.c.register(self.b['card_id'])['bucket']

    def cycle(self,send=True):
        self.v.t+=1
        return self.c.cycle(self.bucket,send=send)

    def protect(self):
        self.cycle();self.v.fill('1000','100');self.cycle();self.cycle();self.cycle(False)

    def unsafe_legacy_two_card_setup(self):
        """Build the PRE-EXISTING unsafe starting state in the exchange DOUBLE.

        The new fence is bypassed ONLY while arranging legacy evidence, never
        during the cleanup, reconciliation or attack being tested afterward.
        """
        self.protect();b,o=original(2);self.cards.record(o['card']);self.c.register(b['card_id'])
        with patch.object(fence,'validate_proposal',return_value=True):
            self.cycle();self.v.fill('1003','100');self.cycle();self.cycle();self.cycle(False)
        return b

    def expiring_entry(self, expiry_seconds=30):
        b,original_record=original(7,expiry_seconds=expiry_seconds)
        self.cards.record(original_record['card']);self.c.register(b['card_id'])
        state=self.c.refresh(self.bucket)
        proposal=dispatch.choose(state,ROUTES2,META,dict(mark_price='10',at_ms=self.v.now()),
            now_ms=self.v.now(),after_exit_policy=dispatch.AFTER_EXIT,
            allowed_entry_card_id=b['card_id'])
        return b,original_record,state,proposal

    def test_reservation_commit_ack_loss_retires_only_after_original_source_expires(self):
        b,original_record,_,_=self.expiring_entry()
        reserve=self.store.reserve
        def commit_without_ack(*args):
            reserve(*args)
            raise DispatchError('COMMIT_ACK_UNCERTAIN')
        with patch.object(self.store,'reserve',side_effect=commit_without_ack):
            with self.assertRaisesRegex(DispatchError,'COMMIT_ACK_UNCERTAIN'):
                self.c.cycle(self.bucket,send=True,allowed_entry_card_id=b['card_id'])
        state=self.store.load(self.bucket);rid=state['pending']
        self.assertEqual(self.store.request(rid)['attempts'],0);self.assertEqual(self.v.sent,0)
        self.v.t=T+9999
        state=self.c.refresh(self.bucket)
        self.assertEqual(fence.retire_obsolete_unsent(self.store,state,now_ms=self.v.now())['pending'],rid)
        self.v.t=T+10000
        self.c.cycle(self.bucket,send=False,allowed_entry_card_id=b['card_id'])
        state=self.store.load(self.bucket);request=self.store.request(rid)
        self.assertIsNone(state['pending']);self.assertEqual(request['phase'],'ABORTED_UNSENT')
        self.assertEqual(request['retired_reason'],'ENTRY_SOURCE_EXPIRED_WITHOUT_ATTEMPT')
        self.assertEqual(request['attempts'],0);self.assertIsNone(request['nonce'])
        self.assertEqual(state['originals'][b['card_id']],original_record)
        self.assertEqual(self.cards.load(b['card_id']),original_record['card'])
        self.assertEqual(state['bindings'],[]);self.assertEqual(self.v.sent,0)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.nonces').fetchone()[0],0)
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.ownership').fetchone()[0],0)

    def test_started_entry_remains_unknown_after_source_expiry_without_public_identity(self):
        _,_,state,proposal=self.expiring_entry()
        state=self.store.reserve(state,proposal,self.v.now())
        state=self.store.begin(state,proposal,AGENT,self.v.now());rid=state['pending']
        self.v.t=T+10000
        self.assertEqual(fence.retire_obsolete_unsent(self.store,state,now_ms=self.v.now()),state)
        with self.assertRaises(DispatchError):self.c.cycle(self.bucket,send=False)
        self.assertEqual(self.store.load(self.bucket)['pending'],rid)
        self.assertEqual(self.store.request(rid)['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.store.request(rid)['attempts'],1);self.assertEqual(self.v.sent,0)

    def test_expired_unsent_entry_preserves_final_predecessor_fills_and_ownership(self):
        self.protect();self.v.fill('1002','100');self.cycle();self.cycle(False)
        predecessor=self.store.load(self.bucket)
        report=life.review(predecessor['bindings'],predecessor['evidence']['snapshot'],now_ms=self.v.now())
        self.assertTrue(report['cards'][0]['closure_verified'])
        b,original_record,state,proposal=self.expiring_entry(expiry_seconds=600)
        state=self.store.reserve(state,proposal,self.v.now());rid=state['pending'];count=self.v.sent
        with self.j._transaction() as conn:
            owned=conn.execute(f'SELECT * FROM {SCHEMA}.ownership ORDER BY oid').fetchall()
        self.v.t=T+580000
        state=self.c.refresh(self.bucket)
        final=fence.retire_obsolete_unsent(self.store,state,now_ms=self.v.now())
        self.assertIsNone(final['pending']);self.assertEqual(self.store.request(rid)['phase'],'ABORTED_UNSENT')
        self.assertEqual(final['bindings'],predecessor['bindings'])
        for key in ('fills','terminal_orders'):
            self.assertEqual(final['evidence']['snapshot'][key],predecessor['evidence']['snapshot'][key])
        self.assertEqual(final['originals'][self.b['card_id']],predecessor['originals'][self.b['card_id']])
        self.assertEqual(final['originals'][b['card_id']],original_record)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT * FROM {SCHEMA}.ownership ORDER BY oid').fetchall(),owned)
        self.assertEqual(self.v.sent,count)

    def test_entry_retirement_cannot_overtake_concurrent_begin(self):
        _,_,state,proposal=self.expiring_entry()
        state=self.store.reserve(state,proposal,self.v.now())
        stale=deepcopy(state);self.v.t=T+1
        prepared=self.store.request(stale['pending'])
        self.store.begin(state,prepared['proposal'],AGENT,self.v.now())
        self.v.t=T+10000
        stale['evidence']['snapshot']['at_ms']=self.v.now()
        with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED'):
            fence.retire_obsolete_unsent(self.store,stale,now_ms=self.v.now())
        self.assertEqual(self.store.request(stale['pending'])['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.v.sent,0)

    def test_concurrent_unsent_entry_retirement_has_one_durable_owner(self):
        _,_,state,proposal=self.expiring_entry()
        state=self.store.reserve(state,proposal,self.v.now());rid=state['pending']
        self.v.t=T+10000;state=self.c.refresh(self.bucket)
        def retire(_):
            try:
                fence.retire_obsolete_unsent(self.store,state,now_ms=self.v.now())
                return True
            except DispatchError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(retire,range(4)))
        self.assertEqual(sum(results),1);self.assertIsNone(self.store.load(self.bucket)['pending'])
        self.assertEqual(self.store.request(rid)['phase'],'ABORTED_UNSENT')
        self.assertEqual(self.v.sent,0)

    def test_new_shared_entry_fails_before_reservation_or_nonce(self):
        self.protect();b,o=original(2);self.cards.record(o['card']);self.c.register(b['card_id']);before=self.v.sent
        with self.assertRaisesRegex(DispatchError,'SHARED_MARKET_INDEPENDENT_PAIR_NOT_ISOLATED'):
            self.cycle()
        self.assertEqual(self.v.sent,before);self.assertIsNone(self.store.load(self.bucket)['pending'])

    def test_full_controller_cleanup_keeps_other_card_orders_and_fills(self):
        b=self.unsafe_legacy_two_card_setup();other={oid:deepcopy(self.v.orders[oid]) for oid in ('1003','1004','1005')}
        self.v.fill('1002','100');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['old_oid'],'1001')
        self.cycle(False)
        state=self.store.load(self.bucket);report=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())
        a=next(c for c in report['cards'] if c['card_id']==self.b['card_id'])
        second=next(c for c in report['cards'] if c['card_id']==b['card_id'])
        self.assertTrue(a['closure_verified']);self.assertEqual(second['remaining_quantity'],'100')
        self.assertEqual(other,{oid:self.v.orders[oid] for oid in other})
        request=self.store.request(state['last_request']);self.assertTrue(request['residual_verification']['exact_order_terminal'])

    def test_lost_cancel_reply_and_restart_do_not_repeat(self):
        self.unsafe_legacy_two_card_setup();self.v.fill('1002','100');self.v.lose_reply=True
        self.cycle();state=self.store.load(self.bucket);rid=state['pending'];count=self.v.sent
        script="from hl_testnet_runtime.postgres_journal import PostgresJournal; from hl_testnet_runtime.filled_dispatch_store import DispatchStore; import os; r=DispatchStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])).request('"+rid+"'); print(r['phase'],r['attempts'])"
        loaded=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,check=True,timeout=15)
        self.assertEqual(loaded.stdout.strip(),'OUTCOME_UNKNOWN 1')
        self.c=dispatch.Controller(DispatchStore(PostgresJournal.for_ci(CI)),self.v,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        self.cycle(False);self.assertEqual(self.v.sent,count);self.assertIsNone(self.store.load(self.bucket)['pending'])

    def test_cancel_ack_without_terminal_state_keeps_barrier(self):
        self.protect();self.v.fill('1002','100')
        def ack_only(request):
            self.v.sent+=1;self.v.t+=10
            return dict(status='ok',response=dict(type='cancel',data=dict(statuses=['success'])))
        with patch.object(self.v,'send',side_effect=ack_only):self.cycle()
        count=self.v.sent;out=self.cycle(False)
        self.assertEqual(out['status'],'ACK_UNVERIFIED');self.assertEqual(self.v.sent,count)
        self.assertIsNotNone(self.store.load(self.bucket)['pending'])

    def test_pre_cancel_double_fill_is_recorded_as_incident_not_borrowed(self):
        b=self.unsafe_legacy_two_card_setup();self.v.fill('1002','100')
        self.v.cancel_race=lambda oid:self.v.fill(oid,'40')
        self.cycle();count=self.v.sent
        with self.assertRaises(DispatchError):self.cycle(False)
        state=self.store.load(self.bucket);r=self.store.request(state['last_request'])
        self.assertEqual(r['residual_verification']['own_remaining_quantity'],'-40')
        self.assertFalse(r['residual_verification']['other_cards_reassigned'])
        self.assertTrue(r['residual_verification']['owner_requires_review'])
        self.assertEqual(self.v.sent,count)
        report=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())
        self.assertEqual(next(c for c in report['cards'] if c['card_id']==b['card_id'])['remaining_quantity'],'100')

    def test_never_sent_tp_is_retired_after_stop_closes_card(self):
        self.cycle();self.v.fill('1000','100');self.cycle();self.cycle(False)
        state=self.store.load(self.bucket);p=select(state);self.assertEqual(p['leg'],'TAKE_PROFIT')
        state=self.store.reserve(state,p,self.v.now());rid=state['pending']
        self.v.fill('1001','100');count=self.v.sent;self.cycle(False)
        r=self.store.request(rid)
        self.assertEqual(r['phase'],'ABORTED_UNSENT');self.assertEqual(r['attempts'],0)
        self.assertIsNone(r['nonce']);self.assertEqual(self.v.sent,count)
        self.assertIsNone(self.cards.load(self.b['card_id'])['actual_execution'])

    def test_attempted_unknown_tp_is_never_retired_after_stop_closes(self):
        self.cycle();self.v.fill('1000','100');self.cycle();self.cycle(False)
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.cycle()
        rid=self.store.load(self.bucket)['pending'];self.v.fill('1001','100');count=self.v.sent
        with self.assertRaises(DispatchError):self.cycle(False)
        self.assertEqual(self.store.request(rid)['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.store.load(self.bucket)['pending'],rid);self.assertEqual(self.v.sent,count)

    def test_concurrent_cleanup_reservations_still_have_one_owner(self):
        self.protect();self.v.fill('1002','100');self.v.t+=1
        state=self.c.refresh(self.bucket);p=select(state)
        def reserve(_):
            try:self.store.reserve(state,p,self.v.now());return True
            except DispatchError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(reserve,range(4)))
        self.assertEqual(sum(results),1)
        rid=self.store.load(self.bucket)['pending'];self.assertEqual(self.store.request(rid)['attempts'],0)

    def test_direct_storage_reservation_cannot_cancel_second_cards_stop(self):
        self.unsafe_legacy_two_card_setup();self.v.fill('1002','100');self.v.t+=1
        s=self.c.refresh(self.bucket);p=select(s);p['action']['cancels'][0]['o']=1004
        with self.assertRaisesRegex(DispatchError,'CANCEL_TARGET_NOT_OWNED'):
            self.store.reserve(s,p,self.v.now())
        self.assertIsNone(self.store.load(self.bucket)['pending'])
