"""Exact admission refusals, durable aborts and transport uncertainty; no trades."""
from copy import deepcopy
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import time
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

from . import filled_quantity_dispatch as dispatch, emergency_close as emergency
from . import request_budget as quota, card_lifecycle as life
from .filled_dispatch_store import DispatchStore, DispatchError, DefinitelyUnsent, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore
from .test_filled_quantity_dispatch import NoExternal, Venue, state_from_case, ROUTES2
from .test_filled_quantity_exits import original, META
from .test_card_lifecycle import T, binding, closed, terminal
from .test_emergency_close import Venue as EmergencyVenue

CI=os.environ.get('HL_JOURNAL_CI_URL')


def permit(*, expired=False):
    return quota.Permit(Mock(), 'a'*32, 'exchange', 1,
        time.monotonic_ns()+(-1 if expired else 10_000_000_000))


def plan():
    state=state_from_case()
    proposal=dispatch.choose(state,ROUTES2,META,dict(mark_price='10',at_ms=T),now_ms=T)
    return state,proposal


def request(proposal):
    return dict(request_id='a'*64,bucket='b'*64,domain='software',phase='OUTCOME_UNKNOWN',
        proposal=deepcopy(proposal),nonce=T,attempt_at_ms=T,prepared_at_ms=T,attempts=1,
        reply=None,observed_oid=None,updated_at_ms=T)


class PureTests(NoExternal):
    def test_quota_denial_precedes_normal_durable_begin(self):
        state,proposal=plan()
        store=Mock(domain='software');store.load.return_value=state
        class Denied(Venue):
            def reserve_transport(self,proposal):
                raise quota.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        venue=Denied();controller=dispatch.Controller(store,venue,ROUTES2)
        with self.assertRaisesRegex(quota.BudgetError,'EXHAUSTED'):
            controller._begin_cycle(state['bucket'],state,None,proposal)
        store.prepare_and_begin.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_quota_denial_precedes_emergency_attempt_or_action_slot(self):
        state,_=plan()
        state['emergency']=dict(phase='ACTIVE',card_id=state['bindings'][0]['card_id'],
            requests=[],pending_close=None,pending_cancel=None)
        store=Mock(domain='software');store.load.return_value=state
        class Denied(EmergencyVenue):
            def reserve_transport(self,proposal):
                raise quota.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        venue=Denied();normal=dispatch.Controller(store,Venue(),ROUTES2)
        controller=emergency.Controller(normal,venue)
        proposal=controller.proposal(state,metadata=META)
        before=deepcopy(state)
        for _ in range(3):
            with self.assertRaisesRegex(quota.BudgetError,'EXHAUSTED'):
                controller._begin(state,proposal)
        store.change.assert_not_called()
        self.assertEqual(state,before)
        self.assertEqual(venue.sent,0)

    def test_only_expired_exact_commit_local_admission_certifies_unsent(self):
        _,proposal=plan();record=request(proposal)
        admission=dispatch.TransportAdmission(proposal,permit(expired=True))
        admission.bind(record)
        with self.assertRaises(DefinitelyUnsent) as caught:
            admission.validate(record)
        self.assertTrue(caught.exception.matches(record))
        # A second invocation may not issue another release certificate.
        with self.assertRaises(DispatchError) as twice:
            admission.consume(record)
        self.assertNotIsInstance(twice.exception,DefinitelyUnsent)
        fallback=dispatch.TransportAdmission(proposal,permit(expired=True))
        fallback.bind(record,certifiable=False)
        with self.assertRaises(DispatchError) as standalone:
            fallback.consume(record)
        self.assertNotIsInstance(standalone.exception,DefinitelyUnsent)

    def test_certificate_cannot_release_a_different_or_acknowledged_attempt(self):
        _,proposal=plan();record=request(proposal)
        certificate=DefinitelyUnsent(record,'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        for field,value in (('request_id','c'*64),('bucket','d'*64),('nonce',T+1),
                ('attempt_at_ms',T+1),('attempts',2),('phase','ACK_UNVERIFIED'),
                ('reply',dict(state='OUTCOME_UNKNOWN')),('observed_oid','10')):
            changed=deepcopy(record);changed[field]=value
            self.assertFalse(certificate.matches(changed),field)
        changed=deepcopy(record);changed['proposal']['quantity']='39'
        self.assertFalse(certificate.matches(changed))
        changed=deepcopy(record);changed['updated_at_ms']=T+1
        self.assertTrue(certificate.matches(changed))

    def test_binding_and_consumption_are_exact_and_single_use_under_concurrency(self):
        _,proposal=plan();record=request(proposal)
        admission=dispatch.TransportAdmission(proposal,permit());admission.bind(record)
        other=deepcopy(record);other['nonce']+=1
        with self.assertRaises(DispatchError):admission.consume(other)
        def consume(_):
            try:admission.consume(record);return True
            except DispatchError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(consume,range(12))),1)
        with self.assertRaises(DispatchError):admission.bind(record)

    def test_mock_dynamic_attributes_do_not_create_admission_protocol(self):
        venue=Mock()
        self.assertIsNone(dispatch.reserve_transport(venue,plan()[1]))
        venue.reserve_transport.assert_not_called()

    def test_normal_controller_records_exact_unsent_or_retains_commit_uncertainty(self):
        state,proposal=plan();record=request(proposal)
        for commit_lost in (False,True):
            with self.subTest(commit_lost=commit_lost):
                store=Mock(domain='software');venue=Venue()
                controller=dispatch.Controller(store,venue,ROUTES2)
                admission=dispatch.TransportAdmission(proposal,permit(expired=True));admission.bind(record)
                prepared=dispatch.AdmittedAttempt((state,record,proposal,ROUTES2[proposal['role']]),admission)
                if commit_lost:store.abort_definitely_unsent.side_effect=JournalError('COMMIT_UNKNOWN')
                with patch.object(controller,'_prepare_cycle',return_value=prepared), \
                        patch.object(venue,'send',side_effect=lambda r,admission:admission.consume(r)):
                    result=controller.cycle(state['bucket'],send=True)
                self.assertEqual(result['status'],'OUTCOME_UNKNOWN' if commit_lost else 'ABORTED_UNSENT')
                self.assertEqual(result['order_requests_sent'],0)
                store.abort_definitely_unsent.assert_called_once()
                store.reply.assert_not_called()

    def test_real_http_request_exception_never_issues_unsent_certificate(self):
        _,proposal=plan();record=request(proposal)
        record['domain']='testnet'
        admission=dispatch.TransportAdmission(proposal,permit());admission.bind(record)
        venue=dispatch.TestnetVenue({})
        connection=Mock();connection.request.side_effect=TimeoutError('WIRE_UNCERTAINTY')
        signing=ModuleType('hyperliquid.utils.signing');signing.sign_l1_action=Mock(return_value={})
        with patch.object(venue,'_gate',return_value=ROUTES2[proposal['role']]), \
                patch.object(venue,'now',return_value=T), \
                patch.object(dispatch.roles,'wallet_for_role',return_value=object()), \
                patch.dict(sys.modules,{'hyperliquid.utils.signing':signing}), \
                patch.object(dispatch.http.client,'HTTPSConnection',return_value=connection):
            with self.assertRaises(TimeoutError):venue.send(record,admission=admission)
        self.assertEqual(venue.sent,1);connection.request.assert_called_once()
        with self.assertRaises(DispatchError) as used:admission.consume(record)
        self.assertNotIsInstance(used.exception,DefinitelyUnsent)

    def test_controller_generic_transport_failure_preserves_unknown_attempt(self):
        state,proposal=plan();record=request(proposal)
        store=Mock(domain='software');venue=Venue();controller=dispatch.Controller(store,venue,ROUTES2)
        prepared=dispatch.AdmittedAttempt((state,record,proposal,ROUTES2[proposal['role']]),None)
        with patch.object(controller,'_prepare_cycle',return_value=prepared), \
                patch.object(venue,'send',side_effect=TimeoutError('WIRE_UNKNOWN')):
            result=controller.cycle(state['bucket'],send=True)
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN')
        store.abort_definitely_unsent.assert_not_called()

    def test_old_final_flat_history_background_active_or_unknown_protection(self):
        b=binding();value=dict(bindings=[b],snapshot=closed(b))
        self.assertEqual(dispatch._collection_priority(value,T+86_400_000),'background')
        for invalid in ('position','orders','history','ownership'):
            changed=deepcopy(value)
            if invalid=='position':changed['snapshot']['position_quantity']='1'
            elif invalid=='orders':changed['snapshot']['orders_complete']=False
            elif invalid=='history':changed['snapshot']['history_complete']=False
            else:changed['bindings'][0]['orders']['STOP']=['999']
            self.assertEqual(dispatch._collection_priority(changed,T),'protection')
        self.assertEqual(dispatch._collection_priority(plan()[0]['evidence'],T),'protection')

    def test_only_quiet_current_full_stop_and_take_with_continuous_feed_poll_background(self):
        state=state_from_case(q='100',stop='100',take='100')
        class Feed:
            def dirty_symbols(self,account):return ()
            def entry_allowed(self,account):return True
        feed=Feed()
        self.assertEqual(dispatch._collection_priority(state['evidence'],T,fill_wakeups=feed,pending_clear=True),'background')
        self.assertEqual(dispatch._collection_priority(state['evidence'],T,fill_wakeups=feed),'protection')
        self.assertEqual(dispatch._collection_priority(state['evidence'],T),'protection')
        self.assertEqual(dispatch._collection_priority(state['evidence'],T+15001,fill_wakeups=feed,pending_clear=True),'protection')
        for changed in (state_from_case(q='40',stop='40',take='40'),state_from_case(q='100',stop='100')):
            self.assertEqual(dispatch._collection_priority(changed['evidence'],T,fill_wakeups=feed,pending_clear=True),'protection')
        for dirty in (None,('DOGE',)):
            with patch.object(feed,'dirty_symbols',return_value=dirty):
                self.assertEqual(dispatch._collection_priority(state['evidence'],T,fill_wakeups=feed,pending_clear=True),'protection')
        with patch.object(feed,'entry_allowed',return_value=False):
            self.assertEqual(dispatch._collection_priority(state['evidence'],T,fill_wakeups=feed,pending_clear=True),'protection')

    def test_collection_adapter_preserves_actual_pending_and_emergency_priority(self):
        state=state_from_case(q='100',stop='100',take='100')
        class Feed:
            def dirty_symbols(self,account):return ()
            def entry_allowed(self,account):return True
        venue=dispatch.TestnetVenue({});venue.fill_wakeups=Feed()
        for pending,active,expected in ((None,False,'background'),('a'*64,False,'protection'),
                                       (None,True,'protection')):
            with patch.object(venue,'now',return_value=T), \
                    patch.object(venue,'request_budget',return_value=None), \
                    patch.object(dispatch.evidence,'PublicReader') as reader, \
                    patch.object(dispatch.evidence,'collect',return_value=state['evidence']):
                venue.collect_checkpoint(state['evidence'],pending=pending,emergency_active=active)
            self.assertEqual(reader.call_args.kwargs['priority'],expected)

    def test_quiet_fully_protected_feed_extends_only_duplicate_probe_to_fifteen_seconds(self):
        state=state_from_case(q='100',stop='100',take='100')
        class Feed:
            def dirty_symbols(self,account):return ()
            def entry_allowed(self,account):return True
        feed=Feed()
        self.assertTrue(emergency.recent_normal_checkpoint(state,now_ms=T+14999,fill_wakeups=feed))
        self.assertFalse(emergency.recent_normal_checkpoint(state,now_ms=T+15000,fill_wakeups=feed))
        self.assertFalse(emergency.recent_normal_checkpoint(state,now_ms=T+5000))
        state['pending']='a'*64
        self.assertFalse(emergency.recent_normal_checkpoint(state,now_ms=T+5000,fill_wakeups=feed))
        partial=state_from_case(q='40',stop='40',take='40')
        self.assertFalse(emergency.recent_normal_checkpoint(partial,now_ms=T+5000,fill_wakeups=feed))
        missing_take=state_from_case(q='100',stop='100')
        self.assertFalse(emergency.recent_normal_checkpoint(missing_take,now_ms=T+5000,fill_wakeups=feed))
        with patch.object(feed,'entry_allowed',return_value=False):
            state['pending']=None
            self.assertFalse(emergency.recent_normal_checkpoint(state,now_ms=T+5000,fill_wakeups=feed))
        with patch.object(feed,'dirty_symbols',return_value=None):
            self.assertFalse(emergency.recent_normal_checkpoint(state,now_ms=T+1,fill_wakeups=feed))


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class DatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();cards=CardStore(self.j);cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=Venue()
        self.c=dispatch.Controller(self.store,self.v,ROUTES2)
        self.b,self.o=original();cards.record(self.o['card'])
        self.state=self.c.register(self.b['card_id']);self.bucket=self.state['bucket']

    def count(self):
        with self.j._transaction() as conn:
            return conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0]

    def admission(self,*,expired=False):
        self.v.reserve_transport=lambda p:dispatch.TransportAdmission(p,permit(expired=expired))
        original_send=self.v.send
        def admitted_send(record,*,admission):
            admission.consume(record)
            return original_send(record)
        self.v.send=admitted_send

    def test_normal_denied_quota_has_no_request_nonce_or_pending(self):
        def denied(_):raise quota.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.v.reserve_transport=denied
        with self.assertRaises(quota.BudgetError):self.c.cycle(self.bucket,send=True)
        self.assertIsNone(self.store.load(self.bucket)['pending']);self.assertEqual(self.count(),0)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.nonces').fetchone()[0],0)
        self.assertEqual(self.v.sent,0)

    def test_expired_permit_retains_nonce_audit_and_never_retries_entry(self):
        self.admission(expired=True)
        result=self.c.cycle(self.bucket,send=True)
        self.assertEqual(result['status'],'ABORTED_UNSENT')
        record=self.store.request(result['request_id']);state=self.store.load(self.bucket)
        self.assertEqual((record['phase'],record['attempts'],record['nonce']),('ABORTED_UNSENT',1,T))
        self.assertIsNone(state['pending'])
        self.assertTrue(state['originals'][self.b['card_id']]['entry_unsent_no_retry'])
        self.c.cycle(self.bucket,send=True)
        self.assertEqual(self.count(),1);self.assertEqual(self.v.sent,0)

    def test_forged_changed_nonce_cannot_retire_durable_attempt(self):
        with patch.object(self.v,'send',side_effect=TimeoutError('WIRE_UNKNOWN')):
            self.c.cycle(self.bucket,send=True)
        state=self.store.load(self.bucket);record=self.store.request(state['pending'])
        changed=deepcopy(record);changed['nonce']+=1
        certificate=DefinitelyUnsent(changed,'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        with self.assertRaisesRegex(DispatchError,'CERTIFICATE_REQUIRED'):
            self.store.abort_definitely_unsent(state,record,certificate,T)
        self.assertEqual(self.store.request(record['request_id'])['phase'],'OUTCOME_UNKNOWN')

    def test_abort_commit_failure_keeps_original_unknown_fence(self):
        self.admission(expired=True)
        with patch.object(self.store,'abort_definitely_unsent',side_effect=JournalError('COMMIT_UNKNOWN')):
            result=self.c.cycle(self.bucket,send=True)
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN')
        state=self.store.load(self.bucket);record=self.store.request(state['pending'])
        self.assertEqual((record['phase'],record['attempts']),('OUTCOME_UNKNOWN',1))
        self.assertEqual(self.count(),1);self.assertEqual(self.v.sent,0)

    def test_lost_abort_commit_ack_returns_unknown_and_never_replays(self):
        self.admission(expired=True)
        original_abort=self.store.abort_definitely_unsent;transaction=self.j._transaction
        @contextmanager
        def lost_ack():
            with transaction() as conn:yield conn
            raise JournalError('COMMIT_ACK_UNKNOWN')
        def abort(*args):
            with patch.object(self.j,'_transaction',lost_ack):return original_abort(*args)
        with patch.object(self.store,'abort_definitely_unsent',side_effect=abort):
            result=self.c.cycle(self.bucket,send=True)
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN')
        self.assertIsNone(self.store.load(self.bucket)['pending'])
        self.c.cycle(self.bucket,send=True)
        self.assertEqual(self.count(),1);self.assertEqual(self.v.sent,0)

    def emergency_state(self):
        self.c.cycle(self.bucket,send=True);self.v.fill('1000','40')
        state=self.c.refresh(self.bucket)
        venue=EmergencyVenue();venue.__dict__.update(self.v.__dict__)
        controller=emergency.Controller(self.c,venue)
        state=controller.latch(state,dict(card_id=self.b['card_id'],reason='STOP_VERIFICATION_DEADLINE',
            uncovered_since_ms=venue.t,uncovered_quantity='40'))
        return controller,state,controller.proposal(state,metadata=META)

    def test_emergency_denial_consumes_no_durable_action_slot(self):
        controller,state,proposal=self.emergency_state()
        def denied(_):raise quota.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        controller.venue.reserve_transport=denied
        for _ in range(3):
            with self.assertRaises(quota.BudgetError):controller._begin(state,proposal)
        current=self.store.load(self.bucket)
        self.assertEqual(current['emergency']['requests'],[])
        self.assertIsNone(current['emergency']['pending_cancel'])

    def test_exact_emergency_unsent_retirement_keeps_circuit_and_nonce(self):
        controller,state,proposal=self.emergency_state()
        controller.venue.reserve_transport=lambda p:dispatch.TransportAdmission(p,permit(expired=True))
        prepared=controller._begin(state,proposal);state,record=prepared
        with self.assertRaises(DefinitelyUnsent) as caught:prepared.admission.consume(record)
        current=controller._abort_unsent(state,record,caught.exception)
        saved=current['emergency']['requests'][-1]
        self.assertEqual(saved['phase'],'ABORTED_UNSENT')
        self.assertEqual(saved['nonce'],record['nonce']);self.assertEqual(saved['attempts'],1)
        self.assertIsNone(current['emergency']['pending_cancel'])
        self.assertEqual(current['emergency']['phase'],'ACTIVE')


if __name__=='__main__':unittest.main()
