"""Final dispatch-boundary checks; no exchange I/O, signatures or account keys."""
import os
import unittest
from unittest.mock import patch
from . import filled_quantity_dispatch as m
from . import test_filled_quantity_dispatch as fixture
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore
from .test_filled_quantity_exits import original

T,A=fixture.T,fixture.A
ROUTES2=fixture.ROUTES2
CI=os.environ.get('HL_JOURNAL_CI_URL')


class FinalBoundaryTests(fixture.NoExternal):
    def test_final_gate_rechecks_original_source_after_budget_delay(self):
        from datetime import datetime, timezone
        cid='a'*64
        env=dict(RENDER_SERVICE_ID=m.roles.SERVICE,HL_TESTNET_RUNTIME_MODE='filled_card_controlled_v1',
            HL_TESTNET_FILLED_DISPATCH='approved_single_card_v1',HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
            HL_TESTNET_FILLED_CARD_ID=cid,HL_TESTNET_FILLED_AFTER_EXIT_POLICY=m.AFTER_EXIT,
            HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS=str(T+120000))
        p=dict(card_id=cid,operation='ENTRY',role='long_account',account=A,
               source_at=datetime.fromtimestamp((T-20000)/1000,timezone.utc).isoformat())
        v=m.TestnetVenue(env)
        with patch.object(v,'now',return_value=T),patch.object(m.roles,'route_for',return_value=ROUTES2['long_account']):
            self.assertEqual(v._gate(p,m.AFTER_EXIT)['account'],A)
        with patch.object(v,'now',return_value=T+40000),patch.object(m.roles,'route_for',side_effect=AssertionError('STALE_BEFORE_ROUTE')):
            with self.assertRaisesRegex(DispatchError,'^NEW_TRIAL_SOURCE_NOT_FRESH$'):
                v._gate(p,m.AFTER_EXIT)
        self.assertEqual(v.sent,0)

    def test_expired_entry_window_does_not_disable_exit_management(self):
        env=dict(RENDER_SERVICE_ID=m.roles.SERVICE,HL_TESTNET_RUNTIME_MODE='filled_card_controlled_v1',
            HL_TESTNET_FILLED_DISPATCH='approved_single_card_v1',HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
            HL_TESTNET_FILLED_CARD_ID='a'*64,HL_TESTNET_FILLED_AFTER_EXIT_POLICY=m.AFTER_EXIT,
            HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS=str(T-1000))
        p=dict(card_id='a'*64,operation='CREATE_EXIT',role='long_account',account=A,source_at='old')
        with patch.object(m.roles,'route_for',return_value=ROUTES2['long_account']):
            self.assertEqual(m.TestnetVenue(env)._gate(p,m.AFTER_EXIT)['account'],A)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class FinalBoundaryDatabaseTests(fixture.NoExternal):
    def test_each_cycle_reports_only_its_own_request_count(self):
        journal=PostgresJournal.for_ci(CI)
        with journal._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        journal.bootstrap();cards=CardStore(journal);cards.initialize()
        store=DispatchStore(journal);store.initialize();venue=fixture.Venue()
        controller=m.Controller(store,venue,ROUTES2)
        binding,original_record=original();cards.record(original_record['card'])
        bucket=controller.register(binding['card_id'])['bucket']
        def cycle(send=True):
            venue.t+=1
            return controller.cycle(bucket,send=send)
        self.assertEqual(cycle()['order_requests_sent'],1)
        venue.fill('1000','40')
        self.assertEqual(cycle()['order_requests_sent'],1)
        self.assertEqual(cycle()['order_requests_sent'],1)
        self.assertEqual(venue.sent,3)
        self.assertEqual(cycle(False)['order_requests_sent'],0)
