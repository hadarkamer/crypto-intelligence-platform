"""Long stream release boundaries; no network, wallet or real order."""
from datetime import datetime, timezone
from contextlib import redirect_stdout
from unittest.mock import Mock, patch
from contextlib import nullcontext
import os
import io
import json
import unittest

from . import long_stream_runtime as stream, filled_quantity_dispatch as dispatch
from . import alert_cards_intake as intake, app, gunicorn_conf
from . import approved_alert_selection as selection
from . import app_card_delivery as app_delivery
from .filled_dispatch_store import DispatchError
from .test_card_lifecycle import A, B
from .test_filled_quantity_dispatch import NoExternal
from .test_filled_quantity_dispatch import Venue, ROUTES2
from .test_filled_quantity_exits import original, META
from . import trade_cards
from . import card_lifecycle as life
from .filled_dispatch_store import DispatchStore, SCHEMA
from .trade_card_store import CardStore
from .postgres_journal import PostgresJournal
from .test_card_lifecycle import T

AGENT='0x'+'3'*40
SECRET='a'*64


def env():
    return dict(RENDER_SERVICE_ID=stream.roles.SERVICE,
        HL_TESTNET_RUNTIME_MODE=stream.MODE,
        HL_TESTNET_FILLED_DISPATCH=stream.RELEASE,
        HL_TESTNET_LONG_STREAM=stream.SOURCE,
        HL_TESTNET_LONG_ENTRY_ENABLED='false',
        HL_TESTNET_LONG_NOT_BEFORE='2026-09-26T14:00:00+00:00',
        HL_TESTNET_LONG_ACCOUNT_ADDRESS=A,
        HL_TESTNET_LONG_AGENT_ADDRESS=AGENT,
        HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',
        HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
        HL_TESTNET_FILLED_AFTER_EXIT_POLICY=dispatch.AFTER_EXIT,
        HL_TESTNET_CARDS_INTAKE='record_only_v1',
        HL_TESTNET_CARDS_PHASE1='record_only_v1',
        HL_TESTNET_CARDS_INTAKE_SECRET=SECRET)


class ConfigurationTests(NoExternal):
    def test_maintenance_reconciles_between_actions_and_cannot_open_entries(self):
        controller=Mock()
        controller.cycle.side_effect=[
            dict(status='ACCEPTED_UNVERIFIED',order_requests_sent=1),
            dict(status='ACCEPTED_UNVERIFIED',order_requests_sent=1),
            dict(status='NO_ACTION_NEEDED',order_requests_sent=0)]
        results=list(stream._maintain_bucket(controller,'owned'))
        self.assertEqual(sum(r['order_requests_sent'] for r in results),2)
        self.assertEqual(controller.cycle.call_count,3)
        for call in controller.cycle.call_args_list:
            self.assertEqual(call.args,('owned',))
            self.assertEqual(call.kwargs,dict(send=True,allow_new_entries=False))

    def test_maintenance_stops_on_unknown_rejected_or_unobserved_request(self):
        for status in ('OUTCOME_UNKNOWN','REJECTED','ATTEMPTED','ACCEPTED_UNVERIFIED'):
            controller=Mock()
            controller.cycle.return_value=dict(status=status,order_requests_sent=0)
            self.assertEqual(len(list(stream._maintain_bucket(controller,'owned'))),1)
            controller.cycle.assert_called_once()

    def test_maintenance_pass_is_bounded_even_as_fills_keep_growing(self):
        controller=Mock()
        controller.cycle.return_value=dict(status='ACCEPTED_UNVERIFIED',order_requests_sent=1)
        self.assertEqual(len(list(stream._maintain_bucket(controller,'owned'))),3)
        self.assertEqual(controller.cycle.call_count,3)

    def test_entry_disabled_maintenance_keeps_fast_poll_and_flat_accounts_wait(self):
        for active, delay in ((1,2),(0,10)):
            controller=Mock()
            controller.venue.env={'enabled':'false'}
            controller.venue.sent=0
            stop=Mock()
            stop.is_set.side_effect=[False,True]
            result=dict(status='ENTRIES_DISABLED',maintenance_active=active,
                        active_buckets=0,order_requests_sent=0,new_cards_registered=0)
            with patch.object(stream,'tick',return_value=result), \
                 patch.object(stream,'_stop',stop),redirect_stdout(io.StringIO()):
                stream._loop(controller,[('long_account',{},None,'enabled')])
            stop.wait.assert_called_once_with(delay)

    def test_aged_closed_short_reconciles_with_entries_disabled(self):
        controller=Mock()
        controller.venue.now.return_value=T+2*60*60*1000
        controller.store.for_account.return_value=[dict(bucket='saved-short',bindings=[{}],
            evidence=dict(snapshot=dict(at_ms=T)),pending=None)]
        controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        with patch.object(stream,'_unfinished',return_value=False):
            result=stream.tick(controller,{'account':B},datetime.now(timezone.utc),
                               new_entries=False,role='short_account')
        self.assertEqual(result['status'],'ENTRIES_DISABLED')
        controller.cycle.assert_called_once_with('saved-short',send=True,allow_new_entries=False)
        controller.venue.send.assert_not_called()

    def test_short_pending_diagnostic_reads_public_state_without_order_or_identifiers(self):
        controller=Mock()
        controller.store.for_account.return_value=[dict(symbol='DOGE',pending='private-request-id')]
        controller.store.request.return_value=dict(phase='OUTCOME_UNKNOWN',
            reply=dict(state='OUTCOME_UNKNOWN',code=None),attempt_at_ms=T,
            proposal=dict(leg='STOP',action=dict(type='order',orders=[dict(c='private-cloid')])))
        controller.venue.now.return_value=T+1000
        controller.venue.lookup.return_value=dict(status='unknownOid',
                                                  secret='private-venue-field')
        reader=Mock()
        reader.read.side_effect=[[],dict(assetPositions=[]),[]]
        output=io.StringIO()
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=reader), \
             redirect_stdout(output):
            stream._short_pending_readiness(controller,{'account':B})
        report=json.loads(output.getvalue())['testnet_short_pending_readiness']
        self.assertEqual(report['status'],'PENDING_PUBLIC_EVIDENCE_OBSERVED')
        self.assertEqual(report['pending'],[dict(symbol='DOGE',phase='OUTCOME_UNKNOWN',
            reply_state='OUTCOME_UNKNOWN',leg='STOP',reply_code=None,lookup_status='unknownOid',
            symbol_open_orders_present=False,symbol_position_present=False,
            fill_window_complete=True,symbol_fills_since_attempt=False)])
        self.assertEqual(report['order_requests_sent'],0)
        self.assertNotIn('private-',output.getvalue())
        controller.venue.send.assert_not_called()
        controller.store.request.return_value['phase']='REJECTED'
        controller.store.request.return_value['reply']=dict(state='REJECTED',
            code='OTHER_REJECTION',venue_reason='Order price too far from oracle',
            rejection_subject='AGENT')
        reader.read.side_effect=[[],dict(assetPositions=[]),[]]
        output=io.StringIO()
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=reader), \
             redirect_stdout(output):
            stream._short_pending_readiness(controller,{'account':B})
        rejected=json.loads(output.getvalue())['testnet_short_pending_readiness']['pending'][0]
        self.assertEqual(rejected['rejection_reason'],'Order price too far from oracle')
        self.assertEqual(rejected['rejection_subject'],'AGENT')
        reader.read.side_effect=[[dict(other='malformed')],dict(assetPositions=[])]
        output=io.StringIO()
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=reader), \
             redirect_stdout(output):
            stream._short_pending_readiness(controller,{'account':B})
        invalid=json.loads(output.getvalue())['testnet_short_pending_readiness']
        self.assertEqual(invalid['status'],'READ_ONLY_REVIEW_UNAVAILABLE')
        self.assertEqual(invalid['pending'],[])

    def test_recent_short_cards_check_current_budget_without_dispatch(self):
        controller=Mock()
        controller.venue.env={}
        controller.store.for_account.return_value=[dict(symbol='BTC',originals={'c':{}},
                                                         bindings=[],pending=None,
                                                         evidence=dict(snapshot=dict(at_ms=1234,fills=[])))]
        conn=Mock()
        conn.execute.return_value.fetchall.return_value=[('c',)]
        controller.store.journal._transaction.return_value=nullcontext(conn)
        card=dict(prepared=dict(source=dict(at='2026-09-26T17:05:00+00:00'),
                                execution=dict(symbol='BTC',side='SHORT',entry='100',
                                               stop='102',take_profit='98')))
        output=io.StringIO()
        with patch.object(stream,'CardStore') as store, \
             patch.object(stream.roles,'budget_for_role',
                side_effect=stream.roles.checks.Blocked('ESTIMATED_MARGIN_EXCEEDS_EXCHANGE_AVAILABLE')), \
             redirect_stdout(output):
            store.return_value.load.return_value=card
            stream._short_card_readiness(controller,{'account':B,'agent':AGENT})
        report=json.loads(output.getvalue())['testnet_short_card_readiness']
        self.assertEqual(report['buckets'][0]['registered_cards'],1)
        self.assertEqual(report['buckets'][0]['evidence_at_ms'],1234)
        self.assertEqual(report['buckets'][0]['evidence_fill_count'],0)
        self.assertEqual(report['cards'][0]['current_budget_status'],
                         'ESTIMATED_MARGIN_EXCEEDS_EXCHANGE_AVAILABLE')
        self.assertEqual(report['order_requests_sent'],0)
        controller.venue.send.assert_not_called()

    def test_second_account_readiness_is_read_only_and_redacted(self):
        for failure, status in (
                (stream.roles.checks.Blocked('AGENT_ACCOUNT_MISMATCH'), 'AGENT_ACCOUNT_MISMATCH'),
                (ValueError('private credential'), 'READ_ONLY_REVIEW_UNAVAILABLE')):
            output=io.StringIO()
            with patch.object(stream.roles.checks,'InfoReader') as reader, \
                 patch.object(stream.roles,'default_native_snapshot',side_effect=failure),redirect_stdout(output):
                reader.return_value.read.return_value='default'
                stream._short_account_readiness({'account':B,'agent':AGENT})
            report=json.loads(output.getvalue())['testnet_short_account_readiness']
            self.assertEqual(report['status'],status)
            self.assertEqual(report['order_requests_sent'],0)
            self.assertNotIn('credential',output.getvalue())
        output=io.StringIO()
        observation=dict(status='NATIVE_CAPACITY_OBSERVED_NO_ORDER_CHECKED',
                         account_mode='default',balance_usd='100',
                         exchange_reported_available_usd='100',account_mapping_verified=True,
                         unrelated_private_field='must not log')
        with patch.object(stream.roles.checks,'InfoReader') as reader, \
             patch.object(stream.roles,'default_native_snapshot',return_value=observation),redirect_stdout(output):
            reader.return_value.read.return_value='default'
            stream._short_account_readiness({'account':B,'agent':AGENT})
        report=json.loads(output.getvalue())['testnet_short_account_readiness']
        self.assertEqual(report['balance_usd'],'100')
        self.assertNotIn('unrelated_private_field',output.getvalue())

    def test_unified_short_readiness_uses_mode_aware_public_check(self):
        output=io.StringIO()
        observation=dict(status='ACCOUNT_CHECKED_WAITING_FOR_TEST_PLAN',
                         account_mapping_verified=True, positive_usdc_observed=True,
                         unheld_balance_observed=True, exchange_capacity_observed=True,
                         private_field='must not log')
        with patch.object(stream.roles.checks,'InfoReader') as reader, \
             patch.object(stream.roles.checks,'run_check',return_value=observation) as check, \
             patch.object(stream.roles,'default_native_snapshot',side_effect=AssertionError('default only')), \
             redirect_stdout(output):
            reader.return_value.read.return_value='unifiedAccount'
            stream._short_account_readiness({'account':B,'agent':AGENT})
        report=json.loads(output.getvalue())['testnet_short_account_readiness']
        self.assertEqual(report['account_mode'],'unifiedAccount')
        self.assertEqual(report['status'],'ACCOUNT_CHECKED_WAITING_FOR_TEST_PLAN')
        self.assertEqual(report['order_requests_sent'],0)
        self.assertNotIn('private_field',output.getvalue())
        check.assert_called_once()

    def test_stream_failure_reports_safe_code_without_exception_details(self):
        controller=Mock()
        controller.venue.env={'HL_TESTNET_SHORT_ENTRY_ENABLED':'true'}
        controller.venue.sent=0
        stop=Mock()
        stop.is_set.side_effect=[False,True]
        for failure, expected in (
                (DispatchError('PRICE_OUTSIDE_ORIGINAL_EXITS'), 'PRICE_OUTSIDE_ORIGINAL_EXITS'),
                (ValueError('private account credential must not appear'), 'ValueError')):
            stop.is_set.side_effect=[False,True]
            output=io.StringIO()
            with patch.object(stream,'tick',side_effect=failure),patch.object(stream,'_stop',stop),redirect_stdout(output):
                stream._loop(controller,[('short_account',{},None,'HL_TESTNET_SHORT_ENTRY_ENABLED')])
            report=json.loads(output.getvalue().strip())['testnet_short_stream']
            self.assertEqual(report['status'],'RECONCILIATION_REQUIRED_NO_BLIND_RETRY')
            self.assertEqual(report['order_requests_sent'],0)
            self.assertEqual(report['failure_code'],expected)
            self.assertRegex(report['failure_origin'],r'^[\w.]+:\d+$')
            self.assertNotIn('credential',output.getvalue())

    def test_exact_long_route_and_record_only_intake(self):
        route, start=stream.configuration(env())
        self.assertEqual(route['account'],A)
        self.assertEqual(start,datetime(2026,9,26,14,tzinfo=timezone.utc))
        self.assertTrue(intake.enabled(env()))
        self.assertIsNone(stream.short_configuration(env()))

    def test_short_release_requires_all_fields_and_own_route(self):
        configured={**env(),
            'HL_TESTNET_SHORT_ACCOUNT_ADDRESS':B,
            'HL_TESTNET_SHORT_AGENT_ADDRESS':'0x'+'4'*40,
            'HL_TESTNET_SHORT_STREAM':stream.SOURCE,
            'HL_TESTNET_SHORT_ENTRY_ENABLED':'false',
            'HL_TESTNET_SHORT_NOT_BEFORE':'2026-09-26T14:00:00+00:00'}
        route,start=stream.short_configuration(configured)
        self.assertEqual(route['account'],B)
        self.assertEqual(start,datetime(2026,9,26,14,tzinfo=timezone.utc))
        for key in ('HL_TESTNET_SHORT_ACCOUNT_ADDRESS','HL_TESTNET_SHORT_STREAM',
                    'HL_TESTNET_SHORT_ENTRY_ENABLED','HL_TESTNET_SHORT_NOT_BEFORE'):
            with self.subTest(key=key),self.assertRaises(Exception):
                stream.short_configuration({**configured,key:''})
        with patch.object(stream.roles,'wallet_for_role',
                          side_effect=stream.roles.checks.Blocked('ROLE_AGENT_KEY_NOT_CONFIGURED')):
            with self.assertRaisesRegex(stream.roles.checks.Blocked,'NOT_CONFIGURED'):
                stream.short_configuration({**configured,'HL_TESTNET_SHORT_ENTRY_ENABLED':'true',
                                            'HL_TESTNET_SHORT_TRIAL_CARD_ID':'b'*64})
        with patch.object(stream.roles,'wallet_for_role',return_value=object()) as signer:
            stream.short_configuration({**configured,'HL_TESTNET_SHORT_ENTRY_ENABLED':'true',
                                        'HL_TESTNET_SHORT_TRIAL_CARD_ID':'b'*64})
            signer.assert_called_once_with({**configured,'HL_TESTNET_SHORT_ENTRY_ENABLED':'true',
                                            'HL_TESTNET_SHORT_TRIAL_CARD_ID':'b'*64},
                'short_account',B,'0x'+'4'*40)
        venue=dispatch.TestnetVenue(configured)
        proposal=dict(card_id='b'*64,role='short_account',account=B,
            operation='CREATE_EXIT',source_at='2026-09-26T14:00:00+00:00')
        self.assertEqual(venue._gate(proposal,dispatch.AFTER_EXIT)['account'],B)
        with self.assertRaisesRegex(DispatchError,'STREAM_ENTRIES_DISABLED'):
            venue._gate({**proposal,'operation':'ENTRY'},dispatch.AFTER_EXIT)
        with self.assertRaises(stream.roles.checks.Blocked):
            venue._gate({**proposal,'account':A},dispatch.AFTER_EXIT)

    def test_all_other_release_modes_fail_closed(self):
        for key,value in (
                ('HL_TESTNET_RUNTIME_MODE','filled_card_controlled_v1'),
                ('HL_TESTNET_FILLED_DISPATCH','approved_single_card_v1'),
                ('HL_TESTNET_LONG_STREAM',''),
                ('HL_TESTNET_LONG_ENTRY_ENABLED','maybe'),
                ('HL_TESTNET_FILLED_AUTOWAIT','approved_next_u21_once_v1'),
                ('HL_TESTNET_FILLED_CARD_ID','b'*64),
                ('HL_TESTNET_TWO_ACCOUNT_EXECUTION','approved_single_attempt_v1'),
                ('HL_TESTNET_CARD_SYNC','registered_readonly_v1'),
                ('HL_TESTNET_CARDS_INTAKE_SECRET',''),
                ('HL_TESTNET_SAFETY_PIPELINE','integrated_readonly_v1')):
            with self.subTest(key=key),self.assertRaises(DispatchError):
                stream.configuration({**env(),key:value})
        self.assertFalse(intake.enabled({**env(),'HL_TESTNET_RUNTIME_MODE':'read_only',
                                         'HL_TESTNET_CARDS_INTAKE_SECRET':''}))

    def test_disabled_entry_keeps_exit_gate_available(self):
        venue=dispatch.TestnetVenue(env())
        proposal=dict(card_id='b'*64,role='long_account',account=A,
            operation='CREATE_EXIT',source_at='2026-09-26T14:00:00+00:00')
        self.assertEqual(venue._gate(proposal,dispatch.AFTER_EXIT)['account'],A)
        with self.assertRaisesRegex(DispatchError,'LONG_ENTRIES_DISABLED'):
            venue._gate({**proposal,'operation':'ENTRY'},dispatch.AFTER_EXIT)
        with self.assertRaises(DispatchError):
            venue._gate({**proposal,'role':'short_account'},dispatch.AFTER_EXIT)
        with self.assertRaises(stream.roles.checks.Blocked):
            venue._gate({**proposal,'account':AGENT},dispatch.AFTER_EXIT)

    def test_startup_is_single_worker_and_health_has_no_controls(self):
        with patch.dict(stream.os.environ,env(),clear=True),\
             patch.object(stream,'start') as starter,\
             patch('hl_testnet_runtime.filled_trial_runtime.start',
                   side_effect=AssertionError('SHORT_WORKER_STARTED')):
            gunicorn_conf.post_worker_init(None)
            starter.assert_called_once()
            output=[]
            body=b''.join(app.application(dict(REQUEST_METHOD='GET',PATH_INFO='/healthz',
                QUERY_STRING=''),lambda status,headers:output.append(status)))
            report=json.loads(body)
            self.assertEqual(output,['200 OK'])
            self.assertFalse(report['read_only'])
            self.assertFalse(report['continuous_trading'])
            self.assertFalse(report['public_order_controls'])
            self.assertIn('long_stream',report)

    def test_account_inventory_rejects_unowned_order_and_position(self):
        class Reader:
            orders=[]
            positions=[]
            def read(self,kind,account):
                return (self.orders if kind=='frontendOpenOrders' else
                    dict(assetPositions=[dict(position=p) for p in self.positions]))
        fake=Reader()
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=fake):
            self.assertTrue(stream._account_owned(None,A,[]))
            fake.orders=[dict(coin='BTC',oid=100)]
            with self.assertRaisesRegex(DispatchError,'UNOWNED_ACCOUNT_ORDER'):
                stream._account_owned(None,A,[])
            fake.orders=[];fake.positions=[dict(coin='BTC',szi='2')]
            with self.assertRaisesRegex(DispatchError,'UNOWNED_ACCOUNT_POSITION'):
                stream._account_owned(None,A,[])
            fake.positions=[]
            state=dict(symbol='BTC',pending=None,bindings=[dict(orders={
                'ENTRY':[], 'STOP':[], 'TAKE_PROFIT':[]})],
                evidence=dict(snapshot=dict(position_quantity='0')))
            fake.positions=[dict(coin='BTC',szi='2')]
            with self.assertRaisesRegex(DispatchError,'UNOWNED_ACCOUNT_POSITION'):
                stream._account_owned(None,A,[state])
            fake.positions=[dict(coin='BTC',szi='2'),dict(coin='ETH',szi='-3')]
            owned=[{**state,'symbol':'BTC',
                    'evidence':dict(snapshot=dict(position_quantity='2'))},
                   {**state,'symbol':'ETH',
                    'evidence':dict(snapshot=dict(position_quantity='-3'))}]
            self.assertTrue(stream._account_owned(None,A,owned))

    def test_account_inventory_rejects_opposite_direction_even_when_journal_agrees(self):
        class Reader:
            position='0'
            def read(self,kind,account):
                if kind=='frontendOpenOrders': return []
                return dict(assetPositions=[dict(position=dict(coin='BTC',szi=self.position))])
        reader=Reader()
        state=dict(symbol='BTC',pending=None,bindings=[],
                   evidence=dict(snapshot=dict(position_quantity='-2')))
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=reader):
            reader.position='-2'
            with self.assertRaisesRegex(DispatchError,'OPPOSITE_DIRECTION_ACCOUNT_EXPOSURE'):
                stream._account_owned(None,A,[state],role='long_account')
            self.assertTrue(stream._account_owned(None,B,[state],role='short_account'))
            state['evidence']['snapshot']['position_quantity']='2'
            reader.position='2'
            with self.assertRaisesRegex(DispatchError,'OPPOSITE_DIRECTION_ACCOUNT_EXPOSURE'):
                stream._account_owned(None,B,[state],role='short_account')
            state['bindings']=[dict(role='short_account',orders={
                'ENTRY':[],'STOP':[],'TAKE_PROFIT':[]})]
            with self.assertRaisesRegex(DispatchError,'ACCOUNT_BINDING_ROLE_MISMATCH'):
                stream._account_owned(None,A,[state],role='long_account')

    def test_disabled_entries_still_service_existing_buckets(self):
        class Store:
            journal=object()
            def for_account(self,account):
                return [dict(bucket='a'*64,pending='request',
                             bindings=[],originals={})]
        class Venue:
            @staticmethod
            def now(): return 1790433900000
        class Controller:
            store=Store()
            venue=Venue()
            calls=0
            def cycle(self,bucket,*,send,allow_new_entries):
                self.args=(bucket,send,allow_new_entries)
                self.calls+=1
                return (dict(order_requests_sent=1,status='ACCEPTED_UNVERIFIED')
                        if self.calls==1 else dict(order_requests_sent=0,status='OUTCOME_UNKNOWN'))
        c=Controller()
        result=stream.tick(c,dict(account=A),
            datetime(2026,9,26,14,tzinfo=timezone.utc),new_entries=False)
        self.assertEqual(c.args,('a'*64,True,False))
        self.assertEqual(result['order_requests_sent'],1)
        self.assertEqual(result['status'],'ENTRIES_DISABLED')
        self.assertEqual(result['maintenance_active'],1)
        self.assertEqual(c.calls,2)

    def test_existing_reconciliation_reports_fixed_failure_code(self):
        class Store:
            journal=object()
            def for_account(self,account):
                return [dict(bucket='a'*64,pending='request',
                             bindings=[],originals={})]
        class Venue:
            @staticmethod
            def now(): return 1790433900000
        class Controller:
            store=Store()
            venue=Venue()
            def cycle(self,*args,**kwargs):
                raise life.LifecycleError('BINDINGS_REQUIRED')
        result=stream.tick(Controller(),dict(account=A),
            datetime(2026,9,26,14,tzinfo=timezone.utc),new_entries=True)
        self.assertEqual(result['status'],'EXISTING_RECONCILIATION_REQUIRED')
        self.assertEqual(result['failure_code'],'BINDINGS_REQUIRED')
        self.assertRegex(result['failure_origin'],r'test_long_stream_runtime.py:[0-9]+')
        self.assertEqual(result['order_requests_sent'],0)

    def test_unowned_account_blocks_new_entry_before_registration(self):
        class Store:
            journal=object()
            def for_account(self,account): return []
        class Venue:
            @staticmethod
            def now(): return 1790433900000
        class Controller:
            store=Store()
            venue=Venue()
            def cycle(self,*args,**kwargs):
                raise AssertionError('NO_CARD_WAS_REGISTERED')
        with patch.object(stream,'_account_owned',
                          side_effect=DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')),\
             patch.object(stream.selection,'page',
                          side_effect=AssertionError('SELECTION_MUST_WAIT')):
            with self.assertRaisesRegex(DispatchError,'UNOWNED_ACCOUNT_ORDER'):
                stream.tick(Controller(),dict(account=A),
                    datetime(2026,9,26,14,tzinfo=timezone.utc),new_entries=True)

    def test_account_snapshot_is_market_scoped_only_for_long_stream(self):
        class Reader:
            def read(self,kind,account):
                return ([dict(coin='BTC',oid=100)] if kind=='frontendOpenOrders'
                    else dict(assetPositions=[dict(position=dict(coin='BTC',szi='2'))]))
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=Reader()), \
             patch.object(dispatch.TestnetVenue,'request_budget',return_value=object()):
            venue=dispatch.TestnetVenue(env())
            snap=venue.empty_snapshot(A,'DOGE')
            self.assertEqual(snap['position_quantity'],'0')
            venue.env={**env(),'HL_TESTNET_RUNTIME_MODE':'filled_card_controlled_v1'}
            with self.assertRaises(DispatchError):
                venue.empty_snapshot(A,'DOGE')


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),
    'Disposable loopback PostgreSQL required')
class DurableLongStreamTests(NoExternal):
    def setUp(self):
        super().setUp()
        self.j=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1',
                         'hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap()
        self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize()
        self.v=Venue()
        # Public reads consume time even when the in-memory venue is otherwise
        # deterministic; consecutive reconciliations require newer evidence.
        collect=self.v.collect
        def moving_collect(value):
            result=collect(value)
            self.v.t += 1
            return result
        self.v.collect=moving_collect
        self.c=dispatch.Controller(self.store,self.v,ROUTES2,
            after_exit_policy=dispatch.AFTER_EXIT)
        self.route=ROUTES2['long_account']
        self.start=datetime.fromtimestamp((T-30000)/1000,timezone.utc)

    def card(self,n,side='LONG'):
        _,record=original(n,side)
        source=record['card']['prepared']['source']
        expiry=datetime.fromtimestamp((T+30000)/1000,timezone.utc).isoformat()
        card=trade_cards.prepare_card(source,META,rule_id='FORMULA_'+str(n),
            threshold_pct='1.5',record_kind='received_alert',
            source_expires_at=expiry)
        self.cards.record(card)
        return card

    def sweep(self,ids,*,enabled=True):
        with patch.object(stream.selection,'page',return_value=(
                    [(cid,'long_account') for cid in ids],None)),\
             patch.object(stream,'_account_owned',return_value=True):
            return stream.tick(self.c,self.route,self.start,new_entries=enabled)

    def test_two_identical_market_alerts_keep_distinct_ids_and_one_entry(self):
        a,b=self.card(91),self.card(92)
        first=self.sweep([a['card_id'],b['card_id']])
        self.assertEqual(first['new_cards_registered'],2)
        state=self.store.for_account(self.route['account'])[0]
        self.assertEqual(set(state['originals']),{a['card_id'],b['card_id']})
        self.assertEqual(self.v.sent,1)
        # Restart the controller and repeat the same source page: no new
        # registration or second entry while the first market is unresolved.
        self.c=dispatch.Controller(self.store,self.v,ROUTES2,
            after_exit_policy=dispatch.AFTER_EXIT)
        self.v.t += 1000  # a fresh public observation after worker restart
        second=self.sweep([a['card_id'],b['card_id']])
        self.assertEqual(second['new_cards_registered'],0)
        self.assertEqual(self.v.sent,1)

    def test_entry_off_does_not_discard_a_recorded_alert(self):
        card=self.card(93)
        first=self.sweep([card['card_id']],enabled=False)
        self.assertEqual(first['new_cards_registered'],0)
        self.assertEqual(self.v.sent,0)
        self.assertEqual(self.sweep([card['card_id']])['new_cards_registered'],1)
        self.assertEqual(self.v.sent,1)

    def test_paged_selector_reads_only_receipted_fresh_rows(self):
        old=self.card(94)
        fresh=self.card(95)
        intake.initialize(self.j)
        intake.ReceiptStore(self.j).save('a'*64,'RECORDED',fresh['card_id'],
                                          {'source':'fixture'})
        now=datetime.fromtimestamp(T/1000,timezone.utc)
        rows,cursor=selection.page(self.j,not_before=self.start.isoformat(),now=now)
        self.assertEqual(rows,[(fresh['card_id'],'long_account')])
        self.assertIsNone(cursor)
        self.assertNotEqual(old['card_id'],fresh['card_id'])

    def test_app_projection_uses_only_receipted_card_and_durable_entry_state(self):
        card=self.card(96)
        intake.initialize(self.j)
        intake.ReceiptStore(self.j).save('b'*64,'RECORDED',card['card_id'],
                                          {'source':'fixture'})
        found=list(app_delivery.records(self.j))
        self.assertEqual([row['card_id'] for row,_ in found],[card['card_id']])
        self.sweep([card['card_id']])
        state=self.store.for_account(self.route['account'])[0]
        # The immediate maintenance cycle has observed the resting entry.
        # Reconciled acceptance still is not a fill and must remain waiting.
        self.assertIsNone(state['pending'])
        result=app_delivery.project(found[0][0],state,created_at=found[0][1])
        self.assertGreater(result['revision'],1)
        self.assertEqual(result['payload']['status'],'WAITING_ENTRY')
        self.assertEqual(result['payload']['quantity_entered'],0)
        self.assertFalse(result['payload']['pnl_verified'])

    def test_receipt_to_entry_protection_take_close_and_restart(self):
        card=self.card(97)
        intake.initialize(self.j)
        intake.ReceiptStore(self.j).save('c'*64,'RECORDED',card['card_id'],
                                          {'source':'fixture'})
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=self.v):
            first=stream.tick(self.c,self.route,self.start,new_entries=True)
            self.assertEqual(first['new_cards_registered'],1)
            self.assertEqual(first['order_requests_sent'],1)
            self.assertEqual(self.v.requests[0]['proposal']['operation'],'ENTRY')
            self.v.fill('1000','100')
            self.v.t+=1
            protected=stream.tick(self.c,self.route,self.start,new_entries=False)
            self.assertEqual(protected['order_requests_sent'],2)
        self.assertEqual([r['proposal']['leg'] for r in self.v.requests],
                         ['ENTRY','STOP','TAKE_PROFIT'])
        opened=stream.observed_trades(self.c,self.route)
        self.assertEqual(len(opened),1)
        self.assertEqual(opened[0]['card_id'],card['card_id'])
        self.assertEqual(opened[0]['source_event_id'],card['prepared']['source']['event_id'])
        self.assertEqual(opened[0]['entry_quantity'],'100')
        self.assertEqual(opened[0]['account_role'],'long_account')
        self.assertEqual(opened[0]['actual_entry_price'],self.v.orders['1000']['order']['limitPx'])
        self.assertIsInstance(opened[0]['first_entry_at_ms'],int)
        self.assertIsNone(opened[0]['last_exit_at_ms'])
        self.assertEqual(opened[0]['remaining_quantity'],'100')
        self.assertEqual(set(opened[0]['active_order_ids']),{'1001','1002'})
        self.assertFalse(opened[0]['issues'])
        self.assertTrue(opened[0]['protection_verified'])
        self.assertFalse(opened[0]['closure_verified'])
        self.v.fill('1002','100')
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=self.v):
            for _ in range(3):
                self.v.t+=1
                stream.tick(self.c,self.route,self.start,new_entries=False)
        state=self.store.for_account(self.route['account'])[0]
        view=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())
        self.assertTrue(view['cards'][0]['closure_verified'])
        self.assertEqual(view['cards'][0]['state'],'CLOSED')
        self.assertIsNotNone(view['cards'][0]['gross_pnl_usdc'])
        self.assertIsNotNone(view['cards'][0]['net_before_funding_usdc'])
        self.assertIsNone(view['cards'][0]['final_net_usdc'])
        self.assertIsNone(state['pending'])
        self.assertEqual(self.v.orders['1001']['status'],'canceled')
        self.assertEqual(self.v.sent,4)  # entry, stop, take profit, orphan stop cancel
        closed=stream.observed_trades(self.c,self.route)
        self.assertEqual(len(closed),1)
        self.assertEqual(closed[0]['card_id'],card['card_id'])
        self.assertTrue(closed[0]['closure_verified'])
        self.assertIsNotNone(closed[0]['actual_exit_price'])
        self.assertIsInstance(closed[0]['last_exit_at_ms'],int)
        self.assertEqual(closed[0]['remaining_quantity'],'0')
        # The immutable source card and its per-card evidence survive a new controller.
        restarted=dispatch.Controller(self.store,self.v,ROUTES2,
            after_exit_policy=dispatch.AFTER_EXIT)
        self.assertEqual(CardStore(restarted.store.journal).load(card['card_id']),card)
        saved=restarted.store.for_account(self.route['account'])[0]
        self.assertEqual(saved['bindings'][0]['card_id'],card['card_id'])
        self.assertEqual(life.review(saved['bindings'],saved['evidence']['snapshot'],
            now_ms=self.v.now())['cards'][0]['state'],'CLOSED')

    def test_short_receipt_to_fill_exits_closure_and_restart(self):
        card=self.card(98,side='SHORT')
        route=ROUTES2['short_account']
        intake.initialize(self.j)
        intake.ReceiptStore(self.j).save('d'*64,'RECORDED',card['card_id'],
                                          {'source':'fixture'})
        def short_tick(enabled):
            return stream.tick(self.c,route,self.start,new_entries=enabled,
                               role='short_account')
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=self.v):
            first=short_tick(True)
            self.assertEqual(first['new_cards_registered'],1)
            self.assertEqual(self.v.requests[0]['proposal']['role'],'short_account')
            self.assertFalse(self.v.requests[0]['proposal']['action']['orders'][0]['b'])
            self.v.fill('1000','100')
            self.v.t+=1
            protected=short_tick(False)
            self.assertEqual(protected['order_requests_sent'],2)
        self.assertEqual([r['proposal']['leg'] for r in self.v.requests],
                         ['ENTRY','STOP','TAKE_PROFIT'])
        opened=stream.observed_trades(self.c,route,role='short_account')
        self.assertEqual(opened[0]['card_id'],card['card_id'])
        self.assertEqual(opened[0]['account_role'],'short_account')
        self.assertTrue(opened[0]['protection_verified'])
        self.assertFalse(opened[0]['closure_verified'])
        self.assertTrue(all(self.v.orders[oid]['order']['side']=='B'
            for oid in ('1001','1002')))
        self.v.fill('1002','100')
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=self.v):
            for _ in range(3):
                self.v.t+=1
                short_tick(False)
        restarted=dispatch.Controller(self.store,self.v,ROUTES2,
            after_exit_policy=dispatch.AFTER_EXIT)
        saved=restarted.store.for_account(route['account'])[0]
        view=life.review(saved['bindings'],saved['evidence']['snapshot'],now_ms=self.v.now())
        self.assertEqual(view['cards'][0]['state'],'CLOSED')
        self.assertTrue(view['cards'][0]['closure_verified'])
        self.assertEqual(CardStore(self.j).load(card['card_id']),card)
        self.assertTrue(stream.observed_trades(restarted,route,role='short_account')[0]['closure_verified'])

    def test_one_maintenance_sweep_protects_partial_growth_using_native_modify(self):
        card=self.card(99)
        self.c.register(card['card_id'])
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=self.v):
            self.sweep([],enabled=True)
            self.v.fill('1000','40')
            first=self.sweep([],enabled=False)
            self.assertEqual(first['order_requests_sent'],2)
            self.assertTrue(stream.observed_trades(self.c,self.route)[0]['protection_verified'])
            self.v.fill('1000','30')
            second=self.sweep([],enabled=False)
            self.assertEqual(second['order_requests_sent'],2)
        changed=self.v.requests[-2:]
        self.assertEqual([r['proposal']['operation'] for r in changed],['MODIFY_EXIT']*2)
        self.assertTrue(all(r['proposal']['action']['type']=='batchModify' for r in changed))
        self.assertTrue(all(r['proposal']['quantity']=='70' for r in changed))
        trade=stream.observed_trades(self.c,self.route)[0]
        self.assertEqual(trade['entry_quantity'],'70')
        self.assertTrue(trade['protection_verified'])
        self.assertEqual(self.v.orders['1000']['order']['sz'],'30')

    def test_lost_stop_reply_ends_pass_and_next_pass_reconciles_without_duplicate(self):
        card=self.card(100)
        self.c.register(card['card_id'])
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=self.v):
            self.sweep([],enabled=True)
            self.v.fill('1000','100')
            self.v.lose_reply=True
            uncertain=self.sweep([],enabled=False)
            self.assertEqual(uncertain['order_requests_sent'],1)
            self.assertEqual([r['proposal']['leg'] for r in self.v.requests],['ENTRY','STOP'])
            recovered=self.sweep([],enabled=False)
            self.assertEqual(recovered['order_requests_sent'],1)
        self.assertEqual([r['proposal']['leg'] for r in self.v.requests],
                         ['ENTRY','STOP','TAKE_PROFIT'])
        self.assertTrue(stream.observed_trades(self.c,self.route)[0]['protection_verified'])


if __name__=='__main__':
    unittest.main()
