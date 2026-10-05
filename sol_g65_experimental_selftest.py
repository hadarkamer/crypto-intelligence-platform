"""Offline causal execution, durable outbox and price-contract regression tests."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4
import os
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import sol_g65_experimental_signal as signal
import sol_g65_experimental_store as store
import sol_g65_experimental_worker as worker
from xrp_r2732_experimental_store_selftest import MemoryDatabase

BASE=datetime(2026,10,5,12,0,tzinfo=timezone.utc)
D=BASE+timedelta(minutes=30)
REF=D+timedelta(minutes=1)
CONFIG='frozen-sol-g65-test'


def bar(when=REF,*,open=100,high=100.6,low=100,close=100.4):
    return dict(open_at=when,open=open,high=high,low=low,close=close)


class SignalTests(unittest.TestCase):
    def test_frozen_geometry(self):
        p=signal.build_levels(100)
        for key,val in [('entry_price',100.5),('stop_loss',101.25),('take_profit',98.25),
                        ('pending_cancel_price',99),('lock_trigger_price',98.8125),('locked_stop_loss',99.9375)]:
            self.assertAlmostEqual(p[key],val,places=10)
        self.assertAlmostEqual(p['reward_risk'],3)

    def test_exact_predicate_boundaries(self):
        self.assertFalse(signal.predicate_values(-.02))
        self.assertTrue(signal.predicate_values(-.0199999))
        self.assertTrue(signal.predicate_values(-.01))
        self.assertFalse(signal.predicate_values(-.0099999))

    def test_closed_btc_window_and_missing_data(self):
        d=worker.ms(D);start=d-720*60000
        rows=[[t,100,100,98.5,98.5] for t in range(start,d,60000)]
        self.assertTrue(signal.evaluate_signal(rows,d)['signal'])
        future=[d,100,200,1,1]
        self.assertEqual(signal.evaluate_signal(rows+[future],d),signal.evaluate_signal(rows,d))
        self.assertFalse(signal.evaluate_signal(rows[:-1],d)['valid'])
        self.assertFalse(signal.evaluate_signal(rows[:10]+rows[11:],d)['valid'])
        self.assertFalse(signal.evaluate_signal(rows,d+60000)['valid'])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.db=MemoryDatabase()
        self.patcher=patch.object(store,'_connect',self.db.connect)
        self.patcher.start();self.addCleanup(self.patcher.stop)
        store.initialize_scope('scope',BASE,config_sha256=CONFIG)

    def reserve(self,now=None):
        return store.reserve_signal('scope',D,REF,100,{'valid':True,'signal':True},
                                    now or REF+timedelta(seconds=10),config_sha256=CONFIG)

    def state(self):
        return store.snapshot('scope')

    def advance(self,rows,now=None):
        return store.advance_position('scope',rows,now or rows[-1]['open_at']+timedelta(minutes=1),config_sha256=CONFIG)

    def test_activation_no_backfill_and_restart(self):
        self.assertEqual(self.reserve(now=REF-timedelta(seconds=1))['status'],'REFERENCE_NOT_AVAILABLE')
        self.assertIsNone(self.state()['decision_cursor'])
        self.assertEqual(self.reserve(now=REF+timedelta(seconds=90))['status'],'STALE_SIGNAL')
        self.assertIsNone(self.state()['active'])
        self.assertEqual(self.reserve()['status'],'ALREADY_PROCESSED')
        with self.assertRaisesRegex(ValueError,'hash mismatch'):
            store.initialize_scope('scope',BASE,config_sha256='wrong')

    def test_pending_reserves_cap_and_no_notification_until_fill(self):
        self.assertEqual(self.reserve()['status'],'RESERVED_PENDING')
        self.assertEqual(self.state()['intents'],[])
        self.assertIsNone(store.claim_pending('scope',REF+timedelta(seconds=20),config_sha256=CONFIG))
        later=D+timedelta(minutes=30)
        r=store.reserve_signal('scope',later,later+timedelta(minutes=1),100,{'valid':True,'signal':True},later+timedelta(minutes=1,seconds=10),config_sha256=CONFIG)
        self.assertEqual(r['status'],'ACTIVE_POSITION')
        store.initialize_scope('scope',REF+timedelta(days=10),config_sha256=CONFIG)
        self.assertEqual(self.state()['active']['phase'],'PENDING')
        self.advance([bar()])
        self.assertEqual(self.state()['active']['phase'],'OPEN')
        self.assertEqual(len(self.state()['intents']),1)

    def test_cancel_before_fill_does_not_emit(self):
        self.reserve()
        result=self.advance([bar(high=100.2,low=98.9,close=99.5)])
        self.assertEqual(result['status'],'CANCELLED_BEFORE_FILL')
        self.assertIsNone(self.state()['active'])
        self.assertEqual(self.state()['intents'],[])

    def test_ambiguous_entry_cancel_retains_cap(self):
        self.reserve()
        result=self.advance([bar(high=100.6,low=98.9,close=100.2)])
        self.assertEqual(result['status'],'AMBIGUOUS')
        self.assertEqual(self.state()['active']['phase'],'PENDING')
        self.assertEqual(self.state()['active']['ambiguity_reason'],'ENTRY_CANCEL_ORDER_UNKNOWN')
        result=self.advance([bar(REF+timedelta(minutes=1))])
        self.assertEqual(result['processed_bars'],0)
        self.assertEqual(self.state()['intents'],[])

    def test_open_determines_cancel_first(self):
        self.reserve()
        result=self.advance([bar(open=98.8,high=101,low=98.5,close=100.5)])
        self.assertEqual(result['status'],'CANCELLED_BEFORE_FILL')

    def test_open_determines_fill_then_take(self):
        self.reserve()
        result=self.advance([bar(open=100.6,high=100.7,low=98.2,close=98.3)])
        self.assertEqual(result['status'],'CLOSED')
        self.assertEqual(result['position']['outcome'],'TP')
        self.assertEqual(self.state()['intents'],[])

    def test_fill_bar_uses_surviving_close_for_promotion(self):
        self.reserve()
        self.advance([bar(open=100.6,high=100.7,low=98.7,close=99.5)])
        self.assertIsNone(self.state()['active']['lock_effective_at'])
        self.assertAlmostEqual(self.state()['active']['stop_price'],101.25)

    def test_entry_close_promotes_only_next_minute(self):
        self.reserve()
        self.advance([bar(open=100.6,high=100.7,low=98.7,close=98.8)])
        active=self.state()['active']
        self.assertEqual(active['phase'],'OPEN')
        self.assertAlmostEqual(active['stop_price'],99.9375)
        self.assertEqual(active['lock_effective_at'],store.iso(REF+timedelta(minutes=1)))
        result=self.advance([bar(REF+timedelta(minutes=1),open=98.8,high=100,low=98.7,close=99.5)])
        self.assertEqual(result['position']['exit_reason'],'PROFIT_LOCK_STOP')
        self.assertAlmostEqual(result['position']['exit_price'],99.9375)

    def test_promotion_uses_low_later_and_does_not_stop_same_bar(self):
        self.reserve();self.advance([bar()])
        trigger=REF+timedelta(minutes=1)
        self.advance([bar(trigger,open=100.4,high=100.6,low=98.7,close=99.3)])
        self.assertIsNotNone(self.state()['active'])
        self.assertEqual(self.state()['active']['lock_trigger_bar_at'],store.iso(trigger))
        result=self.advance([bar(trigger+timedelta(minutes=1),open=100,high=100.2,low=99.5,close=100)])
        self.assertEqual(result['position']['exit_reason'],'PROFIT_LOCK_STOP')
        self.assertEqual(result['position']['exit_price'],100) # adverse opening gap

    def test_old_stop_resolves_before_promotion(self):
        self.reserve();self.advance([bar()])
        result=self.advance([bar(REF+timedelta(minutes=1),open=100.4,high=101.3,low=98.7,close=99)])
        self.assertEqual(result['position']['exit_reason'],'INITIAL_STOP')
        self.assertIsNone(result['position']['lock_effective_at'])

    def test_dual_barrier_unknown_not_win(self):
        self.reserve();self.advance([bar()])
        result=self.advance([bar(REF+timedelta(minutes=1),open=100.4,high=101.3,low=98,close=99)])
        self.assertEqual(result['status'],'AMBIGUOUS')
        self.assertIsNotNone(self.state()['active'])

    def test_nonclosed_bars_and_gap_cannot_release(self):
        self.reserve()
        self.advance([bar()],now=REF+timedelta(seconds=59))
        self.assertEqual(self.state()['active']['phase'],'PENDING')
        result=self.advance([bar(REF+timedelta(minutes=1))])
        self.assertEqual(result['status'],'PRICE_GAP')
        self.assertEqual(self.state()['active']['bar_cursor'],store.iso(REF-timedelta(minutes=1)))

    def test_old_fills_do_not_replay_alerts(self):
        self.reserve();self.advance([bar()],now=REF+timedelta(hours=12))
        self.assertEqual(self.state()['intents'][0]['status'],'EXPIRED')
        self.assertIsNone(store.claim_pending('scope',REF+timedelta(hours=12),config_sha256=CONFIG))
        self.assertIsNotNone(self.state()['active'])

    def test_at_most_once_unknown_keeps_capacity(self):
        self.reserve();self.advance([bar()])
        now=REF+timedelta(minutes=1,seconds=5)
        intent=store.claim_pending('scope',now,config_sha256=CONFIG)
        self.assertIsNone(store.claim_pending('scope',now,config_sha256=CONFIG))
        ok=store.finish_attempt('scope',intent['intent_id'],intent['attempt_token'],'UNKNOWN',now,config_sha256=CONFIG)
        self.assertTrue(ok)
        self.assertIsNone(store.claim_pending('scope',now,config_sha256=CONFIG))
        self.assertIsNotNone(self.state()['active'])
        self.assertEqual(self.state()['intents'][0]['status'],'UNKNOWN')

    def test_claim_position_identity_fence(self):
        self.reserve();self.advance([bar()])
        self.assertIsNone(store.claim_pending('scope',REF+timedelta(minutes=1),expected_position_id='wrong',config_sha256=CONFIG))
        self.assertEqual(self.state()['intents'][0]['status'],'PENDING')


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.database=MemoryDatabase()
        self.dbpatch=patch.object(store,'_connect',self.database.connect)
        self.dbpatch.start();self.addCleanup(self.dbpatch.stop)
        self.policy=patch.object(worker.policy,'sol_g65_experimental_enabled',return_value=True,create=True)
        self.policy.start();self.addCleanup(self.policy.stop)
        self.current=worker.ms(BASE)+1000
        self.calls=[]
        def fetch(symbol,start,end):
            self.calls.append((symbol,start,end))
            if symbol=='BTC': return [[t,100,100,98.5,98.5] for t in range(start,end,60000)]
            return [[t,100,100.2,99.8,100] for t in range(start,end,60000)]
        self.w=worker.SolG65Worker(cache=worker.PriceCache(fetch),clock=lambda:self.current)
        self.w.subscription=lambda:(True,1234)
        self.sent=[]
        async def send(**kwargs):
            self.sent.append(kwargs);return SimpleNamespace(message_id=1)
        self.w.bot=SimpleNamespace(send_message=send)

    async def test_bootstrap_and_decision_only_btc_queries(self):
        await self.w.tick()
        self.assertEqual(self.calls,[])
        self.current=worker.ms(REF)+10000
        await self.w.tick()
        self.assertEqual([r[0] for r in self.calls],['BTC','SOL'])
        self.assertEqual(self.w.runtime['state'],'RESERVED_PENDING')
        self.assertEqual(self.sent,[])
        self.current=worker.ms(REF)+70000
        await self.w.tick()
        self.assertEqual([r[0] for r in self.calls],['BTC','SOL','SOL'])
        self.assertEqual(self.sent,[])

    async def test_verified_fill_sends_once_and_restart_does_not_replay(self):
        await self.w.tick()
        self.current=worker.ms(REF)+10000
        await self.w.tick()
        def filling_fetch(symbol,start,end):
            return [[t,100,100.6,100,100.4] for t in range(start,end,60000)]
        self.w.cache.fetch=filling_fetch
        self.current=worker.ms(REF)+70000
        await self.w.tick()
        self.assertEqual(len(self.sent),1)
        self.assertIn('g65/k49',self.sent[0]['text'])
        self.assertEqual(self.w.runtime['active_position']['phase'],'OPEN')
        self.assertEqual(self.w.runtime['last_delivery_status'],'DELIVERED')
        await self.w.tick()
        self.assertEqual(len(self.sent),1)
        self.w.scopes.clear() # process bootstrap of the same durable scope
        self.current+=60000
        await self.w.tick()
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.w.runtime['active_position']['phase'],'OPEN')

    async def test_unconfirmed_telegram_delivery_not_retried(self):
        await self.w.tick()
        self.current=worker.ms(REF)+10000
        await self.w.tick()
        self.w.cache.fetch=lambda symbol,start,end:[[t,100,100.6,100,100.4] for t in range(start,end,60000)]
        attempts=[]
        async def unknown_send(**kwargs):
            attempts.append(kwargs)
            raise TimeoutError('transport outcome unknown')
        self.w.bot=SimpleNamespace(send_message=unknown_send)
        self.current=worker.ms(REF)+70000
        await self.w.tick()
        self.assertEqual(len(attempts),1)
        self.assertEqual(self.w.runtime['last_delivery_status'],'UNKNOWN')
        self.current+=60000
        await self.w.tick()
        self.assertEqual(len(attempts),1)
        self.assertEqual(self.w.runtime['active_position']['phase'],'OPEN')

    async def test_closed_cache_guard_and_exchange_backoff(self):
        with self.assertRaisesRegex(ValueError,'unfinished'):
            await self.w.prices('BTC',worker.ms(BASE)-60000,worker.ms(BASE)+60000)
        self.assertEqual(self.calls,[])
        self.w.evidence_error(ValueError('SOL_BINANCE_HTTP_451'))
        self.assertEqual(self.w.retry_ms-self.current,6*60*60000)

    async def test_source_contract_strict_coverage(self):
        with self.assertRaises(ValueError): worker.fetch_rows('HYPE',0,60000)
        with self.assertRaises(ValueError): worker.fetch_rows('SOL',0,1001*60000)
        response=SimpleNamespace(status_code=200,content=b'[]',json=lambda:[])
        with patch.object(worker.requests,'get',return_value=response) as get:
            with self.assertRaisesRegex(ValueError,'Incomplete'): worker.fetch_rows('SOL',0,60000)
            self.assertFalse(get.call_args.kwargs['allow_redirects'])


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict,make_conninfo
        test_url=os.environ['TEST_DATABASE_URL']
        info=conninfo_to_dict(test_url)
        if info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or not (
                info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test')):
            raise ValueError('Explicit local test database required')
        cls.dbname='test_sol_g65_'+uuid4().hex
        cls.admin=psycopg.connect(test_url,autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.dbname)))
        cls.dsn=make_conninfo(test_url,dbname=cls.dbname)
        try:
            with store._connect(cls.dsn) as conn:
                conn.execute('CREATE TABLE bot_settings(key text PRIMARY KEY,value text NOT NULL)')
        except BaseException:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        try: cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
        finally: cls.admin.close()

    def test_concurrent_pending_fill_claim_restart(self):
        scope='test-'+uuid4().hex
        store.initialize_scope(scope,BASE,config_sha256=CONFIG,database_url=self.dsn)
        barrier=Barrier(6)
        def reserve(_):
            barrier.wait(timeout=5)
            return store.reserve_signal(scope,D,REF,100,{'valid':True,'signal':True},REF+timedelta(seconds=10),
                                        config_sha256=CONFIG,database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=6) as pool:
            results=list(pool.map(reserve,range(6)))
        self.assertEqual(sum(r['status']=='RESERVED_PENDING' for r in results),1)
        barrier=Barrier(6)
        def fill(_):
            barrier.wait(timeout=5)
            return store.advance_position(scope,[bar()],REF+timedelta(minutes=1),config_sha256=CONFIG,database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=6) as pool:
            results=list(pool.map(fill,range(6)))
        self.assertEqual(sum(r['processed_bars'] for r in results),1)
        state=store.snapshot(scope,database_url=self.dsn)
        self.assertEqual(len(state['intents']),1)
        barrier=Barrier(6)
        def claim(_):
            barrier.wait(timeout=5)
            return store.claim_pending(scope,REF+timedelta(minutes=1,seconds=5),config_sha256=CONFIG,database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=6) as pool:
            claims=[r for r in pool.map(claim,range(6)) if r]
        self.assertEqual(len(claims),1)
        state=store.initialize_scope(scope,REF+timedelta(minutes=5),config_sha256=CONFIG,database_url=self.dsn)
        self.assertEqual(state['intents'][0]['status'],'UNKNOWN')
        self.assertEqual(state['active']['phase'],'OPEN')
        self.assertEqual(state['activated_at'],store.iso(BASE))
        self.assertIsNone(store.claim_pending(scope,REF+timedelta(minutes=5),config_sha256=CONFIG,database_url=self.dsn))


if __name__=='__main__':
    unittest.main()
