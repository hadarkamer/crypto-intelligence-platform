"""Approved remainder policy and complete driver tests; no exchange or keys.

Use the existing real controller/collector and disposable PostgreSQL. Only the
external venue is replaced. Runtime flags are tested separately from policy.
"""
from copy import deepcopy
import io
import json
import os
import unittest
from unittest.mock import patch

from . import filled_trial_runtime as m, filled_quantity_dispatch as dispatch
from . import test_filled_quantity_dispatch as fixture
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore
from .test_filled_quantity_exits import original
from . import app, gunicorn_conf

CI = os.environ.get('HL_JOURNAL_CI_URL')
T = fixture.T


def read_only_env():
    return dict(RENDER_SERVICE_ID=m.roles.SERVICE,
        HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',
        HL_TESTNET_RUNTIME_MODE='read_only',
        HL_TESTNET_FILLED_DISPATCH='disabled',
        HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
        HL_TESTNET_SAFETY_PIPELINE='integrated_readonly_v1',
        HL_TESTNET_FILLED_AFTER_EXIT_POLICY=m.POLICY,
        HL_TESTNET_FILLED_PREPARATION=m.PREPARE)


class PreparationBoundaryTests(fixture.NoExternal):
    def test_policy_choice_alone_cannot_start_trading(self):
        env = read_only_env()
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(DispatchError): m.start()
        self.assertFalse(m.health()['running'])

    def test_preparation_disabled_has_no_storage_or_network_access(self):
        with patch.object(m.PostgresJournal, 'from_env', side_effect=AssertionError('NO_DB')):
            self.assertEqual(m.inspect_preparation({})['status'], 'DISABLED')

    def test_preparation_requires_read_only_and_explicit_disabled_dispatch(self):
        for field, value in (('HL_TESTNET_RUNTIME_MODE', m.EXECUTE),
                ('HL_TESTNET_FILLED_DISPATCH', 'approved_single_card_v1'),
                ('HL_TESTNET_SAFETY_PIPELINE', ''),
                ('HL_TESTNET_TWO_ACCOUNT_EXECUTION', 'approved_single_attempt_v1')):
            env = read_only_env(); env[field] = value
            with self.subTest(field=field):
                with self.assertRaises(DispatchError): m.configuration(env)

    def test_unknown_policy_or_wrong_service_is_not_approval(self):
        for field, value in (('HL_TESTNET_FILLED_AFTER_EXIT_POLICY', 'NOT_SELECTED'),
                ('HL_TESTNET_FILLED_AFTER_EXIT_POLICY', ''),
                ('RENDER_SERVICE_ID', 'some-other-service')):
            env=read_only_env();env[field]=value
            with self.assertRaises(DispatchError):m.configuration(env)

    def test_selected_role_cannot_drift_to_the_first_account(self):
        env=read_only_env();route=dict(account=m.roles.PHANTOM,agent=fixture.AGENT)
        with patch.object(m.roles,'route_for',return_value=route) as select:
            self.assertEqual(m.configuration(env),route)
            select.assert_called_once_with(env,'short_account',account=m.roles.PHANTOM,side='SHORT')

    def test_future_worker_rejects_parallel_legacy_sync(self):
        env=read_only_env();env.update(HL_TESTNET_RUNTIME_MODE=m.EXECUTE,
            HL_TESTNET_FILLED_DISPATCH='approved_single_card_v1',HL_TESTNET_SAFETY_PIPELINE='',
            HL_TESTNET_FILLED_CARD_ID='a'*64,HL_TESTNET_CARD_SYNC='registered_readonly_v1')
        with self.assertRaises(DispatchError):m.configuration(env,sending=True)

    def test_future_worker_branch_does_not_start_legacy_tasks(self):
        with patch.dict(os.environ, {'HL_TESTNET_RUNTIME_MODE':m.EXECUTE}, clear=True),\
             patch.object(m,'start') as new,\
             patch('hl_testnet_runtime.card_sync.start',side_effect=AssertionError('NO_SECOND_SCHEDULER')),\
             patch.object(app,'start_read_only_check',side_effect=AssertionError('NO_LEGACY_START')):
            gunicorn_conf.post_worker_init(None)
            new.assert_called_once()

    def test_health_is_not_falsely_read_only_in_future_execution_mode(self):
        response=[]
        with patch.dict(os.environ, {'HL_TESTNET_RUNTIME_MODE':m.EXECUTE},clear=True):
            body=b''.join(app.application(dict(REQUEST_METHOD='GET',PATH_INFO='/healthz',QUERY_STRING=''),
                lambda status,headers:response.append(status)))
        result=json.loads(body)
        self.assertEqual(response,['200 OK']);self.assertFalse(result['read_only'])
        self.assertFalse(result['public_order_controls']);self.assertFalse(result['controlled_card']['app_controls'])

    def test_application_still_cannot_start_cancel_or_alter_a_trade(self):
        with patch.dict(os.environ,read_only_env(),clear=True):
            for path in ('/','/healthz','/execute','/cancel','/internal/filled-card','/internal/policy'):
                responses=[]
                app.application(dict(REQUEST_METHOD='POST',PATH_INFO=path,QUERY_STRING='',
                    CONTENT_LENGTH='2',**{'wsgi.input':io.BytesIO(b'{}')}),
                    lambda status,headers:responses.append(status))
                self.assertEqual(responses,['405 Method Not Allowed'])

    def test_health_returns_an_independent_display_copy(self):
        snapshot=m.health();snapshot['running']=True;snapshot['app_controls']=True
        self.assertFalse(m.health()['app_controls']);self.assertFalse(m.health()['running'])


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ApprovedRemainderDriverTests(fixture.NoExternal):
    def setUp(self):
        super().setUp()
        self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=fixture.Venue()
        self.c=dispatch.Controller(self.store,self.v,fixture.ROUTES2,after_exit_policy=m.POLICY)
        self.b,self.o=original(91,'SHORT');self.cards.record(self.o['card'])
        self.bucket=self.c.register(self.b['card_id'])['bucket']

    def step(self,send=True):
        self.v.t+=1
        return m.tick(self.c,self.bucket,send=send)

    def protect(self):
        self.step();self.v.fill('1000','40');self.step();self.step();self.step(False)

    def views(self):
        state=self.store.load(self.bucket)
        return m.life.review(state['bindings'],state['evidence']['snapshot'],now_ms=self.v.now())['cards']

    def test_selected_policy_preview_never_reserves_or_sends(self):
        result=self.step(False)
        self.assertEqual(result['status'],'PREVIEW_ONLY');self.assertFalse(result['finished'])
        self.assertEqual(self.v.sent,0)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],0)

    def test_no_exit_keeps_all_sixty_waiting(self):
        self.protect();before=self.v.sent;self.step();self.step()
        self.assertEqual(self.v.sent,before)
        self.assertEqual(self.v.orders['1000']['status'],'open')
        self.assertEqual(self.v.orders['1000']['order']['sz'],'60')

    def test_take_closes_forty_cancels_sixty_then_only_old_stop(self):
        self.protect();self.v.fill('1002','40');self.step()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.v.requests[-1]['proposal']['old_oid'],'1000')
        self.assertEqual(self.v.orders['1000']['status'],'canceled')
        self.step();result=self.step(False)
        self.assertTrue(result['finished']);self.assertTrue(self.views()[0]['closure_verified'])
        self.assertEqual(self.v.orders['1001']['status'],'canceled')

    def test_stop_closes_forty_cancels_sixty_then_only_old_take(self):
        self.protect();self.v.fill('1001','40');self.step()
        self.assertEqual(self.v.requests[-1]['proposal']['old_oid'],'1000')
        self.step();self.assertTrue(self.step(False)['finished'])
        self.assertEqual(self.v.orders['1002']['status'],'canceled')

    def test_first_ten_unit_exit_already_cancels_sixty_not_waiting_for_full_close(self):
        self.protect();self.v.fill('1002','10');self.step()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.v.orders['1000']['status'],'canceled')
        self.step();self.step();self.step(False)
        view=self.views()[0]
        self.assertEqual(view['remaining_quantity'],'30')
        self.assertEqual(view['stop_quantity_observed'],'30')
        self.assertEqual(view['take_profit_quantity_observed'],'30')
        self.assertFalse(view['closure_verified'])

    def test_fill_during_cancellation_is_counted_not_erased(self):
        self.protect();self.v.fill('1002','10')
        self.v.cancel_race=lambda oid:self.v.fill(oid,'20')
        self.step();self.step();self.step();self.step();self.step();self.step(False)
        view=self.views()[0]
        self.assertEqual(view['entry_quantity'],'60');self.assertEqual(view['exit_quantity'],'10')
        self.assertEqual(view['remaining_quantity'],'50')
        self.assertEqual(view['stop_quantity_observed'],'50')
        self.assertEqual(self.v.orders['1000']['status'],'canceled')
        self.assertEqual(self.v.orders['1000']['order']['sz'],'40')

    def test_unknown_cancellation_is_not_retried_even_after_more_entry_fills(self):
        self.protect();self.v.fill('1002','10')
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.step()
        pending=self.store.load(self.bucket)['pending'];before=len(self.v.requests)
        self.v.fill('1000','10');self.step();self.step()
        self.assertEqual(self.store.load(self.bucket)['pending'],pending)
        self.assertEqual(self.store.request(pending)['attempts'],1)
        self.assertEqual(len(self.v.requests),before)

    def test_lost_cancel_reply_reconciles_without_resubmitting_it(self):
        self.protect();self.v.fill('1002','40');self.v.lose_reply=True;self.step()
        count=self.v.sent;self.step(False)
        self.assertEqual(self.v.sent,count);self.assertIsNone(self.store.load(self.bucket)['pending'])
        self.assertEqual(sum(r['proposal']['operation']=='CANCEL_ENTRY_AFTER_EXIT' for r in self.v.requests),1)

    def test_restart_after_exit_does_not_reopen_original_entry(self):
        self.protect();self.v.fill('1002','40');self.step();self.step();self.step(False)
        self.c=dispatch.Controller(DispatchStore(PostgresJournal.for_ci(CI)),self.v,fixture.ROUTES2,after_exit_policy=m.POLICY)
        before=self.v.sent
        self.assertTrue(self.step()['finished']);self.assertEqual(self.v.sent,before)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.v.requests),1)

    def preparation(self):
        route=fixture.ROUTES2['short_account']
        with patch.object(m.roles,'route_for',return_value=route),\
             patch.object(m.roles,'default_native_snapshot',return_value=dict(balance_usd='999',account_mode='default')):
            return m.inspect_preparation(read_only_env(),journal=self.j,public_venue=self.v,capacity_reader=object())

    def test_preparation_initialization_is_repeatable_without_requests(self):
        before=deepcopy(self.cards.load(self.b['card_id']))
        first=self.preparation();second=self.preparation()
        self.assertEqual(first['status'],'PREPARED_FOR_SPECIFIC_PLAN_REVIEW_ONLY')
        self.assertEqual(second['status'],first['status']);self.assertFalse(second['specific_trade_checked'])
        self.assertFalse(second['dispatch_enabled']);self.assertEqual(self.v.sent,0)
        self.assertEqual(self.cards.load(self.b['card_id']),before)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],0)

    def test_preparation_cannot_ignore_an_existing_pending_attempt(self):
        self.step()
        result=self.preparation()
        self.assertEqual(result['status'],'EXISTING_EXECUTION_REQUIRES_RECONCILIATION')
        self.assertEqual(result['unresolved_requests'],1)

    def test_finished_card_retains_same_source_prices_and_ten_dollar_size(self):
        before=deepcopy(self.o['card']);self.protect();self.v.fill('1002','40')
        self.step();self.step();self.step(False)
        original_record=self.store.load(self.bucket)['originals'][self.b['card_id']]['card']
        self.assertEqual(original_record,before)
        self.assertEqual(self.o['draft']['planned_quantity'],'100')
        self.assertEqual(self.b['account'],fixture.B)


if __name__=='__main__':unittest.main()
