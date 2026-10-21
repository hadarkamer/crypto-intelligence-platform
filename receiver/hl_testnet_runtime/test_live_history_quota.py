"""Active service recovery with PostgreSQL and local quota ledgers.

Public readers and quota calculations are real; exchange replies, fixture
signing and market clocks are synthetic. Recovery forbids /exchange. The
PostgreSQL cases check CI identity before SQL; local cases replace SQL only.
"""
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
import json
import os
import time
import uuid
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from approved_alert_fixtures import maxpain_alert, formula_alert
from experimental_execution_fixtures import r2732_message
from . import test_experimental_live_service as service_tests
from . import test_history_gap_budget as budget_tests
from . import experimental_execution_runtime as runtime
from . import experimental_live_state as state_module
from . import card_sync_evidence as sync
from . import request_budget as quota
from .experimental_live_provider import LiveEvidenceProvider
from .test_experimental_live_provider import RawTestnetFixture, LegacyFixture
from .test_experimental_live_state import LIVE_ROUTES
from .filled_dispatch_store import DispatchStore
from .postgres_journal import PostgresJournal

CI = os.environ.get('HL_JOURNAL_CI_URL')


class MemoryBudget(quota.Budget):
    """In-memory SQL substitute; production quota math, batches and permits."""
    def __init__(self,clock):self.clock=clock;self.tickets={}
    def live(self):
        self.tickets={k:v for k,v in self.tickets.items()
                      if v[0]>self.clock()-quota.WINDOW_MS}
        return self.tickets
    def capacity(self,*,requested_weight=1,priority='background'):
        if not 1<=requested_weight<=quota.LIMIT:
            raise quota.BudgetError('TESTNET_REQUEST_BUDGET_WEIGHT_INVALID')
        ceiling=quota.LIMIT if priority=='protection' else quota.BACKGROUND_LIMIT
        rows=[(v[1],v[0]+quota.WINDOW_MS-self.clock()) for v in sorted(self.live().values())]
        conn=SimpleNamespace(execute=lambda *a:SimpleNamespace(fetchall=lambda:rows))
        return quota._quota_capacity(conn,requested_weight,ceiling)
    def admit(self,weight,priority):
        cap=self.capacity(requested_weight=weight,priority=priority)
        if not cap['eligible']:
            raise quota.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
                **{k:cap[k] for k in ('used_weight','requested_weight','ceiling','retry_after_ms')})
    def acquire(self,path,body,*,priority='background',host=quota.HOST):
        if path!='/info':raise AssertionError('NO_EXCHANGE_SENDS')
        weight=quota.request_weight(path,body,host=host);self.admit(weight,priority)
        token=uuid.uuid4().hex;self.tickets[token]=[self.clock(),weight,False]
        return quota.Permit(self,token,body['type'],weight,time.monotonic_ns()+1000000000)
    def _fund_observation(self,entries,priority,deadline):
        self.admit(sum(w for _,_,w in entries),priority)
        for _,token,w in entries:self.tickets[token]=[self.clock(),w,False]
    def _claim_observation(self,token,weight,deadline):
        assert token in self.live() and self.tickets[token][1:]==[weight,False]
        self.tickets[token][0]=self.clock()
    def _release_unclaimed_observation(self,entries):
        for token,weight in entries:
            if token in self.tickets and self.tickets[token][1:]==[weight,False]:
                self.tickets.pop(token)
        return True
    def _settle(self,token,weight):
        row=self.tickets[token];assert row[1]>=weight and row[2] is False
        row[1:]=[weight,True];return True


def completed_fixture():
    h = service_tests.FullConnectionTests()
    h.setUp()
    try:
        messages = [r2732_message(entry=2.3, decision_ms=service_tests.T-60000)]
        h.seed(messages[-1])
        for _ in range(4): h.service.tick()
        h.oracle.t += 900000
        messages.append(maxpain_alert(approved_ms=h.oracle.t//60000*60000))
        h.seed(messages[-1]); h.service.tick()
        entry = h.oracle.oid('ENTRY')
        h.oracle.fill(entry,h.oracle.orders[entry]['view']['wire_order']['s'])
        for _ in range(3): h.service.tick()
        h.oracle.t = (h.oracle.t//1800000+1)*1800000+130000
        msg = formula_alert('sol_g65',approved_ms=h.oracle.t//1800000*1800000+120000)
        h.seed(msg); h.reject_next_entry=True; h.service.tick()
        state=h.store.load()
        request=next(r for r in state['requests'].values()
                     if r['proposal']['card_id']==msg['occurrence_id'])
        h.oracle.instant_fraction=Decimal(0)
        h.oracle.send(request,admission=SimpleNamespace(consume=lambda r:None))
        cloid=request['proposal']['action']['orders'][0]['c']
        sol=next(o for o in h.oracle.orders.values() if o['view']['cloid']==cloid)
        sol['view'].update(status='REJECTED',at_ms=h.oracle.t)
        for source in messages:
            trade=state['trades'][source['occurrence_id']]
            take=next(oid for oid,leg in trade['order_legs'].items() if leg=='TAKE_PROFIT')
            stop=next(oid for oid,leg in trade['order_legs'].items() if leg=='STOP')
            h.oracle.fill(take,trade['quantity'])
            h.oracle.orders[stop]['view'].update(status='CANCELED',at_ms=h.oracle.t)
        h.exchange_requests=deepcopy(state['requests'])
        h.oracle.t+=51*3600000
        return h
    except BaseException:
        h.doCleanups()
        raise


@contextmanager
def public_transport(h, *, transform=None):
    """Transport-only replacement: all permits/refunds run in production code."""
    calls=[]
    def exchange_state():
        state=h.store.load()
        # Venue order metadata outlives the worker's active request records.
        state['requests']={**h.exchange_requests,**state['requests']}
        return state
    raw=RawTestnetFixture(h.oracle,exchange_state,h.budget)
    class Connection:
        def __init__(self,host,**kwargs):
            if host!='api.hyperliquid.xyz':raise AssertionError('UNEXPECTED_HOST')
        def request(self,method,path,body,headers):
            if method!='POST' or path!='/info':raise AssertionError('NO_EXCHANGE_SENDS')
            self.body=json.loads(body); calls.append(deepcopy(self.body))
        def getresponse(self):
            b=self.body;kind=b['type'];account=b.get('user')
            if kind=='orderStatus' and str(b['oid']).startswith('0x'):
                value=raw.lookup(account,b['oid'])
            else:
                value=raw.read(kind,account,coin=b.get('coin'),
                    oid=str(b['oid']) if 'oid' in b else None,
                    start=b.get('startTime'),end=b.get('endTime'))
            if transform is not None:value=transform(b,value)
            h.oracle.t+=300
            return SimpleNamespace(status=200,read=lambda n:json.dumps(value).encode()[:n])
        def close(self):pass
    with patch('http.client.HTTPSConnection',Connection):yield calls


@unittest.skipUnless(CI,'Requires disposable PostgreSQL; no quota mocks')
class LiveHistoryQuotaPostgresTests(unittest.TestCase):
    def setUp(self):
        budget_tests.HistoryGapBudgetPostgresTests.setUp(self)
        self.h=completed_fixture();self.addCleanup(self.h.doCleanups)
        before=self.h.store.load()
        DispatchStore(self.journal).initialize()
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {state_module.PG_SCHEMA} CASCADE')
        self.h.store=state_module.TestnetExecutionState.for_ci(self.journal)
        self.h.store.initialize(LIVE_ROUTES,not_before_ms=before['not_before_ms'])
        self.h.store.initialize_history()
        def retain_fixture(state):
            for key,value in before.items():
                if key not in ('version','domain','routes','agents','not_before_ms','revision'):
                    state[key]=deepcopy(value)
        self.h.store.mutate(retain_fixture)
        self.reconnect()

    seed=budget_tests.HistoryGapBudgetPostgresTests.seed

    def reconnect(self):
        h=self.h
        h.store=state_module.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        h.budget=quota.Budget(PostgresJournal.for_ci(CI))
        h.provider=LiveEvidenceProvider(h.env,legacy_store=LegacyFixture(),
            experimental_store=h.store,
            price_evidence=service_tests.SyntheticPrices(h.oracle,h.store),
            budget=h.budget,clock=h.oracle.now,
            safety_provider=service_tests.SyntheticSafety(h.release))
        h.reconnect()

    def expire_tickets(self):
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {quota.TICKETS} SET admitted_at=clock_timestamp()-interval '70 seconds'")

    def assert_final(self):
        state=self.h.store.load()
        self.assertEqual({t['symbol']:t['phase'] for t in state['trades'].values()},
                         {'XRP':'CLOSED','HYPE':'CLOSED','SOL':'CANCELED_WITHOUT_FILL'})
        self.assertFalse(state.get('blocked_lanes'))
        return state

    def recover_loaded(self,hours):
        self.h.oracle.t+=(hours-51)*3600000
        self.seed(592)
        with public_transport(self.h) as calls:self.h.service.tick()
        self.assert_final()
        counts=Counter(b['type'] for b in calls)
        self.assertEqual(counts['userFillsByTime'],6)
        self.assertEqual(counts['metaAndAssetCtxs'],0)
        self.assertEqual(counts['orderStatus'],5)

    def test_51_hour_live_load_reconciles_both_accounts_without_resending(self):
        self.recover_loaded(51)

    def test_five_day_live_load_does_not_request_more_than_the_ceiling(self):
        self.recover_loaded(120)

    def test_seven_day_live_load_uses_same_bounded_reads(self):
        self.recover_loaded(168)

    def test_restart_preserves_facts_and_reconciles_with_real_shared_quota(self):
        before=self.h.store.load();self.reconnect()
        self.assertEqual(self.h.store.load(),before)
        self.recover_loaded(120)
        final=self.h.store.load();self.reconnect()
        self.assertEqual(self.h.store.load(),final)

    def test_exhaustion_preserves_checkpoint_then_recovers_without_price_reads(self):
        self.seed(1080);before=self.h.store.load()
        with public_transport(self.h) as calls:self.h.service.tick()
        after=self.h.store.load()
        self.assertEqual(after['collector_checkpoints'],before['collector_checkpoints'])
        self.assertTrue(after['blocked_lanes'])
        self.assertFalse(any(b['type'] in ('metaAndAssetCtxs','userFillsByTime') for b in calls))
        self.expire_tickets();self.h.oracle.t+=70000
        with public_transport(self.h):self.h.service.tick()
        self.assert_final()

    def test_failed_history_read_cannot_commit_partial_evidence(self):
        before=self.h.store.load()
        def fail(body,value):
            if body['type']=='userFillsByTime' and body['startTime']!=body['endTime']:
                raise TimeoutError('SYNTHETIC_HISTORY_TIMEOUT')
            return value
        with public_transport(self.h,transform=fail):self.h.service.tick()
        after=self.h.store.load()
        self.assertEqual(after['collector_checkpoints'],before['collector_checkpoints'])
        self.assertEqual({cid:t['phase'] for cid,t in after['trades'].items()},
                         {cid:t['phase'] for cid,t in before['trades'].items()})
        self.expire_tickets()
        with public_transport(self.h):self.h.service.tick()
        self.assert_final()

    def test_lost_retention_anchor_keeps_owned_trades_unresolved(self):
        def lost(body,value):
            if body['type']=='userFillsByTime' and body['startTime']==body['endTime']:return []
            return value
        with public_transport(self.h,transform=lost):self.h.service.tick()
        phases={t['symbol']:t['phase'] for t in self.h.store.load()['trades'].values()}
        self.assertEqual(phases,{'XRP':'OPEN','HYPE':'OPEN','SOL':'CANCELED_WITHOUT_FILL'})

    def test_settlement_is_once_after_recovery_and_restart(self):
        def settlements():
            trades=list(self.h.store.load()['trades'].values())
            trades.extend(row['trade'] for row in self.h.store.history_page()['records']
                          if row.get('trade'))
            settled=[t for t in trades if t.get('entry_risk')]
            self.assertEqual(len(settled),2)
            self.assertEqual({t['symbol'] for t in settled},{'HYPE','SOL'})
            self.assertTrue(all(t.get('settlement') for t in settled))
            return {t['symbol']:deepcopy(t['settlement']) for t in settled}
        self.recover_loaded(120);self.expire_tickets()
        self.h.oracle.t+=60001  # Existing funding retry follows a denied background page.
        with public_transport(self.h):self.h.service.tick()
        before=deepcopy(self.h.store.load().get('formula_accounting'))
        self.assertIsNotNone(before)
        before_settlements=settlements()
        self.reconnect()
        with public_transport(self.h):
            for _ in range(3):self.h.service.tick()
        self.assertEqual(self.h.store.load().get('formula_accounting'),before)
        self.assertEqual(settlements(),before_settlements)


class LiveHistoryQuotaMemoryTests(LiveHistoryQuotaPostgresTests):
    """Same behavioral cases locally; SQL persistence is covered above in CI."""
    __unittest_skip__=False
    def setUp(self):
        self.h=completed_fixture();self.addCleanup(self.h.doCleanups)
        self.budget=MemoryBudget(self.h.oracle.now)
        self.reconnect()

    def reconnect(self):
        h=self.h;h.budget=self.budget
        h.provider=LiveEvidenceProvider(h.env,legacy_store=LegacyFixture(),
            experimental_store=h.store,
            price_evidence=service_tests.SyntheticPrices(h.oracle,h.store),
            budget=h.budget,clock=h.oracle.now,
            safety_provider=service_tests.SyntheticSafety(h.release))
        # The local ledger substitutes SQL only. Reuse the fixture's already
        # constructed dispatch boundary; every send remains forbidden by HTTP.
        h.port.context_loader=h.provider.dispatch_context
        h.port.budget=self.budget
        h.worker=service_tests.runtime.TestnetExecutionRuntime(h.store,h.provider,h.port,
            release_loader=lambda:deepcopy(h.release),mode=service_tests.runtime.MODE)
        h.service=service_tests.ConnectionService(h.worker,key=h.key,
            release_loader=lambda:deepcopy(h.release))

    def seed(self,weight):
        self.budget.tickets['seed']=[self.h.oracle.t,weight,False]

    def expire_tickets(self):self.budget.tickets.clear()

    def test_exchange_fixture_retains_fills_after_local_requests_are_archived(self):
        state=self.h.store.load();rows=[]
        state.update(requests={},trades={},sources={})
        with patch.object(self.h.store,'load',side_effect=lambda:deepcopy(state)),public_transport(self.h):
            for account in state['routes'].values():
                rows.extend(sync.PublicReader(budget=self.budget).read('userFillsByTime',account,
                    start=service_tests.T,end=self.h.oracle.t))
        self.assertEqual(len(rows),4)
        self.assertEqual({r['coin'] for r in rows},{'XRP','HYPE'})

    def test_forty_cycles_of_rolling_load_make_progress_without_duplicate_orders(self):
        h=self.h
        for i in range(1,9):self.budget.tickets[str(i)]=[h.oracle.t-i*8000,74,False]
        with public_transport(h) as calls:
            for _ in range(40):
                h.service.tick();h.oracle.t+=5000
                self.assert_final()
        self.assertFalse(any(b['type']=='metaAndAssetCtxs' for b in calls))
