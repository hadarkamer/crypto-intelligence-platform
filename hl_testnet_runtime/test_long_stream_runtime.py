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
    def test_recent_short_cards_check_current_budget_without_dispatch(self):
        controller=Mock()
        controller.venue.env={}
        controller.store.for_account.return_value=[dict(symbol='BTC',originals={'c':{}},
                                                         bindings=[],pending=None)]
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
        self.assertEqual(report['cards'][0]['current_budget_status'],
                         'ESTIMATED_MARGIN_EXCEEDS_EXCHANGE_AVAILABLE')
        self.assertEqual(report['order_requests_sent'],0)
        controller.venue.send.assert_not_called()

    def test_second_account_readiness_is_read_only_and_redacted(self):
        for failure, status in (
                (stream.roles.checks.Blocked('AGENT_ACCOUNT_MISMATCH'), 'AGENT_ACCOUNT_MISMATCH'),
                (ValueError('private credential'), 'READ_ONLY_REVIEW_UNAVAILABLE')):
            output=io.StringIO()
            with patch.object(stream.roles,'default_native_snapshot',side_effect=failure),redirect_stdout(output):
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
        with patch.object(stream.roles,'default_native_snapshot',return_value=observation),redirect_stdout(output):
            stream._short_account_readiness({'account':B,'agent':AGENT})
        report=json.loads(output.getvalue())['testnet_short_account_readiness']
        self.assertEqual(report['balance_usd'],'100')
        self.assertNotIn('unrelated_private_field',output.getvalue())

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
                stream.short_configuration({**configured,'HL_TESTNET_SHORT_ENTRY_ENABLED':'true'})
        with patch.object(stream.roles,'wallet_for_role',return_value=object()) as signer:
            stream.short_configuration({**configured,'HL_TESTNET_SHORT_ENTRY_ENABLED':'true'})
            signer.assert_called_once_with({**configured,'HL_TESTNET_SHORT_ENTRY_ENABLED':'true'},
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
            def cycle(self,bucket,*,send,allow_new_entries):
                self.args=(bucket,send,allow_new_entries)
                return dict(order_requests_sent=1,status='ACCEPTED_UNVERIFIED')
        c=Controller()
        result=stream.tick(c,dict(account=A),
            datetime(2026,9,26,14,tzinfo=timezone.utc),new_entries=False)
        self.assertEqual(c.args,('a'*64,True,False))
        self.assertEqual(result['order_requests_sent'],1)
        self.assertEqual(result['status'],'ENTRIES_DISABLED')

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
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader',return_value=Reader()):
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
        request=self.store.request(state['pending'])
        result=app_delivery.project(found[0][0],state,created_at=found[0][1],
                                    pending_request=request)
        self.assertGreater(result['revision'],1)
        self.assertEqual(result['payload']['status'],'WAITING_ENTRY')
        self.assertIsNone(result['payload']['quantity_entered'])
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
            for _ in range(4):
                self.v.t+=1
                stream.tick(self.c,self.route,self.start,new_entries=False)
        self.assertEqual([r['proposal']['leg'] for r in self.v.requests],
                         ['ENTRY','STOP','TAKE_PROFIT'])
        opened=stream.observed_trades(self.c,self.route)
        self.assertEqual(len(opened),1)
        self.assertEqual(opened[0]['card_id'],card['card_id'])
        self.assertEqual(opened[0]['source_event_id'],card['prepared']['source']['event_id'])
        self.assertEqual(opened[0]['entry_quantity'],'100')
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
            for _ in range(4):
                self.v.t+=1
                short_tick(False)
        self.assertEqual([r['proposal']['leg'] for r in self.v.requests],
                         ['ENTRY','STOP','TAKE_PROFIT'])
        opened=stream.observed_trades(self.c,route,role='short_account')
        self.assertEqual(opened[0]['card_id'],card['card_id'])
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


if __name__=='__main__':
    unittest.main()
