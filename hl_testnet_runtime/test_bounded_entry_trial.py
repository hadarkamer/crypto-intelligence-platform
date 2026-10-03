"""One-attempt safety cap tests, including real PostgreSQL races and restart."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime,timezone
import os
import unittest
from unittest.mock import Mock,patch

from . import bounded_entry_trial as cap, filled_quantity_dispatch as dispatch
from . import card_lifecycle as life,trade_cards
from .filled_dispatch_store import DispatchStore,DispatchError,SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore
from . import test_emergency_close as emergency_fixtures
from .test_filled_quantity_dispatch import NoExternal,Venue,ROUTES2,AGENT
from .test_card_lifecycle import T,A,B

CI=os.environ.get('HL_JOURNAL_CI_URL')


def environment():
    env=emergency_fixtures.ContinuousReleaseTests.environment()
    epoch=datetime.fromtimestamp((T-1000)/1000,timezone.utc).isoformat()
    env.update(HL_TESTNET_ENTRY_ATTEMPT_CAP=cap.MODE,
               HL_TESTNET_LONG_NOT_BEFORE=epoch,HL_TESTNET_SHORT_NOT_BEFORE=epoch)
    return env


class CapPureTests(NoExternal):
    def test_absent_cap_changes_no_execution_policy_or_database_reads(self):
        store=Mock()
        self.assertFalse(cap.cap_reached(store,{},'long_account'))
        self.assertIsNone(cap.entry_guard({},dict(operation='ENTRY',role='long_account',account=A)))
        store.journal._transaction.assert_not_called()

    def test_scope_requires_exact_service_continuous_mode_role_and_original_epoch(self):
        env=environment()
        self.assertEqual(cap.configuration(env,'long_account',A),dict(role='long_account',account=A,epoch_ms=T-1000))
        self.assertEqual(cap.configuration(env,'short_account',B)['account'],B)
        for key,value in (('HL_TESTNET_ENTRY_ATTEMPT_CAP','unlimited'),
                          ('HL_TESTNET_EMERGENCY_RELEASE',''),('RENDER_SERVICE_ID','production'),
                          ('HL_TESTNET_LONG_NOT_BEFORE','invalid')):
            with self.subTest(key=key),self.assertRaises(ValueError):
                cap.configuration({**env,key:value},'long_account',A)
        with self.assertRaises(ValueError):cap.configuration(env,'long_account',B)

    def test_guard_uses_same_read_scope_and_blocks_every_begun_outcome(self):
        env=environment();proposal=dict(operation='ENTRY',role='long_account',account=A)
        scope=cap.configuration(env,'long_account',A)
        for phase in ('OUTCOME_UNKNOWN','REJECTED','OBSERVED','ABORTED_UNSENT'):
            request=dict(phase=phase,attempts=1,attempt_at_ms=T,proposal=proposal)
            conn=Mock();conn.execute.return_value.fetchall.return_value=[(request,life.digest(request))]
            with self.subTest(phase=phase),self.assertRaisesRegex(DispatchError,'CAP_REACHED'):
                cap.entry_guard(env,proposal)(conn,dict(account=A),proposal)
            self.assertEqual(conn.execute.call_args.args[1],(scope['role'],scope['account'],T-1000))

    def test_corrupted_attempt_cannot_release_the_cap(self):
        conn=Mock();conn.execute.return_value.fetchall.return_value=[({'attempts':1},'0'*64)]
        with self.assertRaisesRegex(DispatchError,'INTEGRITY_REQUIRED'):
            cap._attempts(conn,dict(role='long_account',account=A,epoch_ms=T))

    def test_management_has_no_cap_check(self):
        store=Mock();env=environment()
        proposal=dict(operation='CREATE_EXIT',role='long_account',account=A)
        cap.check_entry(store,env,proposal)
        self.assertIsNone(cap.entry_guard(env,proposal))
        store.journal._transaction.assert_not_called()

    def test_stored_unknown_entry_cannot_mint_another_transport_admission(self):
        venue=dispatch.TestnetVenue.__new__(dispatch.TestnetVenue)
        venue.env={'HL_TESTNET_ENTRY_ATTEMPT_CAP':cap.MODE}
        request=dict(proposal=dict(operation='ENTRY'),phase='OUTCOME_UNKNOWN',attempts=1)
        with patch.object(venue,'_gate',side_effect=AssertionError('NO_GATE_OR_STORE_READ')), \
                patch.object(venue,'reserve_transport',side_effect=AssertionError('NO_NEW_PERMIT')), \
                patch.object(venue,'_fresh_attempt',side_effect=AssertionError('NO_PREPARATION')):
            with self.assertRaisesRegex(DispatchError,'COMMIT_LOCAL_ADMISSION_REQUIRED') as refused:
                venue.send(request)
            self.assertNotIsInstance(refused.exception,dispatch.DefinitelyUnsent)


@unittest.skipUnless(CI,'disposable PostgreSQL required')
class CapPostgresTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for schema in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.env=environment()
        self.meta=dict(universe=[dict(name='DOGE',szDecimals=2),dict(name='ETH',szDecimals=2)])
        self.venue=Venue();self.venue.metadata=lambda:self.meta
        self.controller=dispatch.Controller(self.store,self.venue,ROUTES2)

    def planned(self,symbol,role):
        side='LONG' if role=='long_account' else 'SHORT'
        source=dict(kind='SIGNAL',event_id=role+symbol,symbol=symbol,side=side,
            entry='10',stop='9.9' if side=='LONG' else '10.1',
            take_profit='10.2' if side=='LONG' else '9.8',
            at=datetime.fromtimestamp((T-20000)/1000,timezone.utc).isoformat())
        card=trade_cards.prepare_card(source,self.meta,rule_id='SOFTWARE_TEST',threshold_pct='1.5',record_kind='synthetic_test')
        self.cards.record(card);state=self.controller.register(card['card_id'])
        state=self.controller.refresh(state['bucket'])
        proposal=dispatch.choose(state,ROUTES2,self.meta,dict(mark_price='10',at_ms=T),now_ms=T)
        return state,proposal

    def begin(self,plan):
        state,proposal=plan
        return self.store.prepare_and_begin(state,proposal,AGENT,T,
                                           entry_guard=cap.entry_guard(self.env,proposal))

    def test_concurrent_cross_symbol_attempts_commit_exactly_one_per_role(self):
        plans=[self.planned(symbol,'long_account') for symbol in ('DOGE','ETH')]
        def begin(plan):
            try:return self.begin(plan)[1]
            except DispatchError as exc:
                self.assertEqual(str(exc),'BOUNDED_ENTRY_TRIAL_CAP_REACHED');return None
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(begin,plans))
        self.assertEqual(sum(result is not None for result in results),1)
        self.assertTrue(cap.cap_reached(self.store,self.env,'long_account'))
        self.assertFalse(cap.cap_reached(self.store,self.env,'short_account'))
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],1)

    def test_two_accounts_get_one_independent_attempt_each(self):
        for role in ('long_account','short_account'):
            self.begin(self.planned('DOGE',role))
            self.assertTrue(cap.cap_reached(self.store,self.env,role))
            with self.assertRaisesRegex(DispatchError,'CAP_REACHED'):
                self.begin(self.planned('ETH',role))
        self.assertEqual(self.venue.sent,0)

    def test_unknown_reply_survives_restart_and_only_own_first_send_gate_can_continue(self):
        plan=self.planned('DOGE','long_account');state,request=self.begin(plan)
        restarted=DispatchStore(PostgresJournal.for_ci(CI))
        self.assertTrue(cap.cap_reached(restarted,self.env,'long_account'))
        cap.check_entry(restarted,self.env,request['proposal'])
        _,other=self.planned('ETH','long_account')
        with self.assertRaisesRegex(DispatchError,'CAP_REACHED'):
            cap.check_entry(restarted,self.env,other)
        with self.assertRaisesRegex(DispatchError,'CAP_REACHED'):
            self.begin(self.planned('ETH','long_account'))
        # A terminal HTTP rejection consumes the same cap permanently.
        self.store.reply(state,dict(state='REJECTED',code='VENUE_REJECTED',oid=None),T+1)
        self.assertTrue(cap.cap_reached(restarted,self.env,'long_account'))
        with self.assertRaisesRegex(DispatchError,'CAP_REACHED'):
            cap.check_entry(restarted,self.env,request['proposal'])

    def test_definitely_unsent_and_lost_commit_ack_cannot_rearm(self):
        state,request=self.begin(self.planned('DOGE','long_account'))
        certificate=dispatch.DefinitelyUnsent(request,'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        self.store.abort_definitely_unsent(state,request,certificate,T+1)
        self.assertTrue(cap.cap_reached(DispatchStore(PostgresJournal.for_ci(CI)),self.env,'long_account'))
        with self.assertRaisesRegex(DispatchError,'CAP_REACHED'):
            self.begin(self.planned('ETH','long_account'))
        plan=self.planned('DOGE','short_account');change=self.store.change
        def lost(*args,**kw):
            result=change(*args,**kw)
            raise DispatchError('COMMIT_ACKNOWLEDGEMENT_LOST')
        with patch.object(self.store,'change',side_effect=lost):
            with self.assertRaisesRegex(DispatchError,'ACKNOWLEDGEMENT_LOST'):self.begin(plan)
        self.assertTrue(cap.cap_reached(DispatchStore(PostgresJournal.for_ci(CI)),self.env,'short_account'))
        self.assertEqual(self.venue.sent,0)
