"""Wire-boundary regressions for residual cleanup; no real venue or keys.

Uses the existing controller and disposable PostgreSQL for lock-boundary tests.
Raw payload mutations model a stale/incorrect internal request, not app authority.
No test treats delayed cancellation as exchange-atomic per-card OCO.
"""
from copy import deepcopy
import os
import unittest
from . import residual_exit_contract as contract, residual_exit_fence as fence
from . import card_lifecycle as life, filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore
from .test_filled_quantity_dispatch import NoExternal, Venue, ROUTES2, META, AGENT, state_from_case
from .test_filled_quantity_exits import original
from .test_residual_exit_fence import orphan, select, merged, new_entry
from .test_card_lifecycle import T, fill, terminal

CI = os.environ.get('HL_JOURNAL_CI_URL')


def validate(state, proposal):
    return contract.validate_wire_proposal(state, proposal, now_ms=state['evidence']['snapshot']['at_ms'])


def own_cancel(state, leg, operation):
    b=state['bindings'][0];oid=b['orders'][leg][0]
    return dict(card_id=b['card_id'],leg=leg,operation=operation,account=state['account'],
        symbol=state['symbol'],role=b['role'],quantity='0',old_oid=oid,
        action=dict(type='cancel',cancels=[dict(a=0,o=int(oid))]),
        basis=life.digest(state['evidence']))


class ResidualWireTests(NoExternal):
    def test_correct_cleanup_in_both_directions_preserves_other_card(self):
        for side in ('LONG','SHORT'):
            for leg in ('TAKE_PROFIT','STOP'):
                s=orphan(side,leg);before=deepcopy(s);p=select(s)
                self.assertTrue(validate(s,p));self.assertEqual(s,before)

    def test_cancel_wrong_asset_or_boolean_asset_rejected(self):
        for value in (1,False,True,'0',-1):
            s=orphan();p=select(s);p['action']['cancels'][0]['a']=value
            with self.assertRaisesRegex(contract.ContractError,'^CANCEL_ASSET_DIFFERS_FROM_OWN_CARD$'):
                validate(s,p)

    def test_cancel_wrong_card_order_or_out_of_range_id_rejected(self):
        for value in (0,2**64,True,'11',999):
            s=orphan();p=select(s);p['action']['cancels'][0]['o']=value
            with self.assertRaises(contract.ContractError):validate(s,p)

    def test_batch_cancel_extra_fields_and_wrong_reason_rejected(self):
        for mutation in ('batch','extra','reason','leg'):
            s=orphan();p=select(s)
            if mutation=='batch':p['action']['cancels'].append(dict(a=0,o=21))
            if mutation=='extra':p['action']['f']=True
            if mutation=='reason':p['operation']='CANCEL_EVERYTHING'
            if mutation=='leg':p['leg']='ENTRY'
            with self.assertRaises(contract.ContractError):validate(s,p)

    def test_cancellation_uses_validated_original_not_display_or_changed_draft(self):
        s=orphan();p=select(s);s['originals'][p['card_id']]['draft']['prices']['stop']='1'
        with self.assertRaises(contract.ContractError):validate(s,p)
        with self.assertRaises(contract.ContractError):validate(dict(display_copy=True),p)

    def test_correct_exit_payloads_keep_exact_card_prices_and_ids(self):
        for side in ('LONG','SHORT'):
            for stop in (None,'100'):
                s=state_from_case(q='100',side=side,stop=stop);p=select(s);before=deepcopy(p)
                self.assertTrue(validate(s,p));self.assertEqual(p,before)
                self.assertTrue(p['action']['orders'][0]['r'])

    def test_wrong_exit_prices_trigger_kind_or_cloid_rejected(self):
        for mutation in ('price','trigger','kind','cloid','quantity','grouping','extra','side','reduce'):
            s=state_from_case(q='100');p=select(s);o=p['action']['orders'][0]
            if mutation=='price':o['p']='1'
            if mutation=='trigger':o['t']['trigger']['triggerPx']='1'
            if mutation=='kind':o['t']['trigger']['tpsl']='tp'
            if mutation=='cloid':o['c']='0x'+'f'*32
            if mutation=='quantity':o['s']='90'
            if mutation=='grouping':p['action']['grouping']='positionTpsl'
            if mutation=='extra':p['action']['builder']=dict(b='0x'+'1'*40,f=1)
            if mutation=='side':o['b']=True
            if mutation=='reduce':o['r']=False
            with self.subTest(mutation=mutation):
                with self.assertRaises(contract.ContractError):validate(s,p)

    def test_numeric_booleans_cannot_compare_equal_to_wire_flags(self):
        for field in ('b','r','isMarket'):
            s=state_from_case(q='100');p=select(s);o=p['action']['orders'][0]
            if field=='isMarket':o['t']['trigger'][field]=1
            else:o[field]=int(o[field])
            with self.assertRaises(contract.ContractError):validate(s,p)

    def test_self_consistent_but_changed_entry_size_is_not_original_plan(self):
        s=state_from_case(q='100',stop='100');p=new_entry(s)
        p['quantity']=p['action']['orders'][0]['s']='90'
        with self.assertRaisesRegex(contract.ContractError,'^ENTRY_WIRE_DIFFERS_FROM_IMMUTABLE_DRAFT$'):
            validate(s,p)

    def test_after_exit_cancel_requires_actual_exit_not_just_label(self):
        s=state_from_case(q='40',stop='40',take='40')
        p=own_cancel(s,'ENTRY','CANCEL_ENTRY_AFTER_EXIT')
        with self.assertRaisesRegex(contract.ContractError,'^CLEANUP_REQUIRES_CONFIRMED_OWN_EXIT$'):
            validate(s,p)

    def test_filled_entry_cannot_be_canceled_using_half_threshold_label(self):
        s=state_from_case(q='40',stop='40',take='40')
        p=own_cancel(s,'ENTRY','CANCEL_UNFILLED_HALF_THRESHOLD')
        with self.assertRaisesRegex(contract.ContractError,'^HALF_THRESHOLD_CANNOT_CANCEL_FILLED_ENTRY$'):
            validate(s,p)

    def test_correct_sized_stop_cannot_be_removed_as_fake_resize_or_orphan(self):
        s=state_from_case(q='40',stop='40',take='40')
        for operation in ('CANCEL_FOR_RESIZE','CANCEL_ORPHAN_EXIT'):
            with self.assertRaises(contract.ContractError):validate(s,own_cancel(s,'STOP',operation))

    def test_quantity_incident_is_not_permission_to_open_a_third_card(self):
        s=orphan();b=s['bindings'][0];snap=s['evidence']['snapshot']
        snap['fills'].append(fill(b,'STOP',qty='40',fid='overclose'))
        snap['terminal_orders'].append(terminal(b,'STOP','40'))
        snap['open_orders']=[o for o in snap['open_orders'] if o['oid'] not in b['orders']['STOP']]
        snap['position_quantity']='20'
        p=new_entry(s)
        with self.assertRaisesRegex(contract.ContractError,'^QUANTITY_INCIDENT_REQUIRES_RECONCILIATION$'):
            validate(s,p)
        self.assertEqual(life.review(s['bindings'],snap,now_ms=T)['cards'][0]['closure_verified'],False)

    def test_flat_card_pending_entry_must_retire_before_new_entry(self):
        s=state_from_case(q='40',stop='40',take='40');b=s['bindings'][0];snap=s['evidence']['snapshot']
        snap['fills'].append(fill(b,'TAKE_PROFIT',qty='40'))
        snap['terminal_orders'] += [terminal(b,'TAKE_PROFIT','40'),terminal(b,'STOP','0')]
        snap['open_orders']=[o for o in snap['open_orders'] if o['oid'] in b['orders']['ENTRY']]
        snap['position_quantity']='0';p=new_entry(s)
        with self.assertRaisesRegex(contract.ContractError,'^CLOSED_CARD_ENTRY_REMAINDER_NOT_RETIRED$'):
            validate(s,p)
        # The permitted owner-only cancellation still passes.
        self.assertTrue(validate(s,own_cancel(s,'ENTRY','CANCEL_ENTRY_AFTER_EXIT')))

    def test_no_sender_scheduler_or_application_path_added(self):
        import inspect
        text=inspect.getsource(contract)
        for forbidden in ('import http','import requests','import threading','wallet_for_role','sign_l1_action','def application','def start('):
            self.assertNotIn(forbidden,text)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ResidualWireDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();cards=CardStore(self.j);cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=Venue()
        self.c=dispatch.Controller(self.store,self.v,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        self.b,self.o=original();cards.record(self.o['card']);self.bucket=self.c.register(self.b['card_id'])['bucket']

    def cycle(self,send=True):
        self.v.t+=1;return self.c.cycle(self.bucket,send=send)

    def counts(self):
        with self.j._transaction() as conn:
            return tuple(conn.execute(f'SELECT count(*) FROM {SCHEMA}.{t}').fetchone()[0] for t in ('requests','nonces','events'))

    def stop_proposal(self):
        self.cycle();self.v.fill('1000','100')
        out=self.cycle(False);return self.store.load(self.bucket),out['proposal']

    def test_bad_exit_wire_fails_inside_reservation_before_any_new_record(self):
        state,p=self.stop_proposal();p['action']['orders'][0]['p']='1';before=self.counts();sent=self.v.sent
        with self.assertRaisesRegex(DispatchError,'^EXIT_WIRE_DIFFERS_FROM_OWN_CARD_CONTRACT$'):
            self.store.reserve(state,p,self.v.now())
        self.assertEqual(before,self.counts());self.assertEqual(sent,self.v.sent)
        self.assertIsNone(self.store.load(self.bucket)['pending'])

    def test_rechecks_changed_wire_inside_attempt_lock_before_nonce(self):
        state,p=self.stop_proposal();state=self.store.reserve(state,p,self.v.now())
        bad=deepcopy(p);bad['action']['orders'][0]['t']['trigger']['triggerPx']='1'
        def corrupt_internal_record(conn,value):
            req=self.store.pending_record(conn,value);req['proposal']=deepcopy(bad);return req
        state=self.store.change(self.bucket,state['revision'],'TEST_BAD_INTERNAL_REBASE',self.v.now(),corrupt_internal_record)
        before=self.counts();sent=self.v.sent
        with self.assertRaisesRegex(DispatchError,'^EXIT_WIRE_DIFFERS_FROM_OWN_CARD_CONTRACT$'):
            self.store.begin(state,bad,AGENT,self.v.now()+1)
        req=self.store.request(state['pending'])
        self.assertEqual((req['phase'],req['attempts'],req['nonce']),('PREPARED',0,None))
        self.assertEqual(before,self.counts());self.assertEqual(sent,self.v.sent)

    def test_wrong_asset_cleanup_fails_before_reservation(self):
        self.cycle();self.v.fill('1000','100');self.cycle();self.cycle();self.cycle(False)
        self.v.fill('1002','100');out=self.cycle(False);state=self.store.load(self.bucket)
        p=out['proposal'];self.assertEqual(p['operation'],'CANCEL_ORPHAN_EXIT')
        p['action']['cancels'][0]['a']=1;before=self.counts();sent=self.v.sent
        with self.assertRaisesRegex(DispatchError,'^CANCEL_ASSET_DIFFERS_FROM_OWN_CARD$'):
            self.store.reserve(state,p,self.v.now())
        self.assertEqual(before,self.counts());self.assertEqual(sent,self.v.sent)

    def test_full_controller_still_cleans_correct_orphan(self):
        self.cycle();self.v.fill('1000','100');self.cycle();self.cycle();self.cycle(False)
        self.v.fill('1002','100');self.cycle();sent=self.v.sent;self.cycle(False)
        state=self.store.load(self.bucket)
        self.assertTrue(life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())['cards'][0]['closure_verified'])
        self.assertIsNone(state['pending']);self.assertEqual(self.v.sent,sent)


if __name__=='__main__':unittest.main()
