"""Retired formula lifecycle on actual product stores; optional isolated PostgreSQL.

No live credentials, market requests or Telegram transport are used. The same
cases run against the existing in-memory fixture and TEST_DATABASE_URL.
"""
import ast
import asyncio
from copy import deepcopy
from datetime import timedelta
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import alert_delivery_policy as policy
import sol_g65_experimental_store as store
import sol_g65_experimental_worker as worker
import doge_partial_experimental_worker as doge
import sol_proximity_experimental_store as mp_store
import sol_proximity_experimental_worker as mp_worker
from sol_g65_experimental_selftest import BASE, D, REF, bar
from doge_partial_experimental_selftest import pending as doge_pending, filled as doge_filled
from sol_proximity_experimental_selftest import BASE as MP_BASE, M, bar as mp_bar
from hype_row71205_experimental_store_selftest import MemoryDatabase
import research_watch_decision_capture_watch_selftest as watch_fixture


class RetirementTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if os.environ.get('TEST_DATABASE_URL'):
            with store._connect(os.environ['TEST_DATABASE_URL']) as conn:
                conn.execute('CREATE TABLE IF NOT EXISTS bot_settings(key text PRIMARY KEY,value text NOT NULL)')
        else:
            self.database = MemoryDatabase()
            for module in (store, mp_store):
                p = patch.object(module, '_connect', self.database.connect)
                p.start(); self.addCleanup(p.stop)
        self.chat = int(uuid4().hex[:12], 16)
        self.now = worker.ms(REF) + 70_000
        self.fetches = []
        self.high, self.low = 100.7, 100.3
        self.bot = SimpleNamespace(send_message=AsyncMock())
        self.g65 = self.new_g65()
        self.scope = worker.subscription_scope(self.chat)
        self.assertIn(worker.signal.RULE_ID, policy.RETIRED_EXPERIMENTAL_RULES)
        self.assertFalse(policy.sol_g65_experimental_enabled())

    def new_g65(self):
        def fetch(symbol, start, end):
            self.fetches.append((symbol, start, end))
            return [[t, 100.5, self.high, self.low, 100.5] for t in range(start, end, worker.MINUTE)]
        w = worker.SolG65Worker(cache=worker.PriceCache(fetch),
            legacy_cache=worker.PriceCache(fetch), clock=lambda: self.now)
        w.bot, w.subscription = self.bot, lambda: (True, self.chat)
        return w

    def reserve(self, config=None):
        config = config or worker.CONFIG_SHA256
        store.initialize_scope(self.scope, BASE, config_sha256=config)
        return store.reserve_signal(self.scope, D, REF, 100, {'valid':True,'signal':True},
            REF+timedelta(seconds=10), config_sha256=config)

    def put(self, state):
        with store._connect(os.environ.get('TEST_DATABASE_URL')) as conn:
            conn.execute('UPDATE bot_settings SET value=%s WHERE key=%s',
                (json.dumps(state), store.key_for(self.scope)))

    async def test_pending_retires_before_fill_and_restart_cannot_revive(self):
        original = self.reserve()['position']
        with patch.object(worker.signal, 'evaluate_signal') as evaluate:
            await self.g65.tick()
            before = store.snapshot(self.scope)
            self.assertIsNone(before['active'])
            self.assertEqual(before['history'][-1]['position_id'], original['position_id'])
            self.assertEqual(before['history'][-1]['outcome'], 'CANCELLED_FORMULA_RETIRED')
            self.now += worker.MINUTE
            await self.new_g65().tick()
            self.assertEqual(store.snapshot(self.scope), before)
            evaluate.assert_not_called()
        self.assertEqual(self.fetches, [])
        self.bot.send_message.assert_not_awaited()

    async def test_open_unattempted_notification_cancels_but_original_exit_continues(self):
        self.reserve()
        store.advance_position(self.scope, [bar()], REF+timedelta(minutes=1), config_sha256=worker.CONFIG_SHA256)
        before = store.snapshot(self.scope)
        await self.g65.tick()
        saved = store.snapshot(self.scope)
        self.assertEqual(saved['active'], before['active'])
        self.assertEqual(saved['intents'][0]['status'], 'CANCELLED')
        self.assertEqual(saved['intents'][0]['error_type'], 'FORMULA_RETIRED')
        self.assertEqual(self.g65.status()['state'], 'RETIRED_MONITORING')
        self.now += 2*worker.MINUTE
        self.high = 102
        await self.new_g65().tick()
        saved = store.snapshot(self.scope)
        self.assertIsNone(saved['active'])
        self.assertEqual(saved['history'][-1]['outcome'], 'SL')
        self.assertEqual(saved['history'][-1]['original_stop_price'], before['active']['original_stop_price'])
        self.assertEqual({f[0] for f in self.fetches}, {'SOL'})
        self.bot.send_message.assert_not_awaited()

    async def test_ambiguous_pending_and_unknown_attempt_never_reopen_or_resend(self):
        self.reserve()
        store.advance_position(self.scope, [bar(high=100.6,low=98.9,close=100.2)],
            REF+timedelta(minutes=1), config_sha256=worker.CONFIG_SHA256)
        before = store.snapshot(self.scope)
        await self.new_g65().tick()
        self.assertEqual(store.snapshot(self.scope)['active'], before['active'])
        self.assertEqual(self.fetches, [])
        # A separate filled position with a committed uncertain delivery.
        self.chat += 1; self.scope = worker.subscription_scope(self.chat)
        self.reserve()
        store.advance_position(self.scope, [bar()], REF+timedelta(minutes=1), config_sha256=worker.CONFIG_SHA256)
        claimed = store.claim_pending(self.scope, REF+timedelta(minutes=1,seconds=5), config_sha256=worker.CONFIG_SHA256)
        store.finish_attempt(self.scope, claimed['intent_id'], claimed['attempt_token'], 'UNKNOWN',
            REF+timedelta(minutes=1,seconds=6), config_sha256=worker.CONFIG_SHA256)
        receipt = deepcopy(store.snapshot(self.scope)['intents'][0])
        await self.new_g65().tick()
        self.assertEqual(store.snapshot(self.scope)['intents'][0], receipt)
        self.bot.send_message.assert_not_awaited()

    async def test_missing_scope_never_creates_or_fetches(self):
        with patch.object(store, 'initialize_scope') as initialize:
            await self.g65.tick()
            await self.new_g65().tick()
            initialize.assert_not_called()
        self.assertIsNone(store.snapshot(self.scope))
        self.assertEqual(self.fetches, [])
        self.bot.send_message.assert_not_awaited()

    async def test_legacy_open_stays_on_original_source_without_migration(self):
        old = store.LEGACY_MIXED_SOURCE_CONFIG_SHA256
        self.reserve(old)
        store.advance_position(self.scope,[bar()],REF+timedelta(minutes=1),config_sha256=old)
        state = store.snapshot(self.scope)
        state['active']['price_source'] = store.LEGACY_POSITION_PRICE_SOURCE
        self.put(state)
        old_fetch = Mock(side_effect=self.g65.legacy_cache.fetch)
        self.g65.legacy_cache.fetch = old_fetch
        self.g65.cache.fetch = Mock(side_effect=AssertionError('must retain old venue'))
        self.now += worker.MINUTE
        await self.g65.tick()
        after = store.snapshot(self.scope)
        old_fetch.assert_called_once()
        self.assertEqual(after['config_sha256'], old)
        self.assertEqual(after['active']['price_source'], store.LEGACY_POSITION_PRICE_SOURCE)
        self.assertNotIn('all_prices_source_migration', after)
        self.bot.send_message.assert_not_awaited()

    async def test_unknown_configuration_is_not_migrated_or_cancelled(self):
        self.reserve('unrecognized')
        before = store.snapshot(self.scope)
        with self.assertRaisesRegex(ValueError, 'configuration hash mismatch'):
            await self.g65.tick()
        self.assertEqual(store.snapshot(self.scope), before)
        self.assertEqual(self.fetches, [])

    async def test_doge_pending_no_intake_and_open_partial_exit_is_preserved(self):
        clock = [MP_BASE+3*M+10_000]
        high = [99.2]
        def fetch(symbol,start,end):
            self.fetches.append((symbol,start,end))
            return [mp_bar(t,99,high[0],98.9,99) for t in range(start,end,M)]
        def make():
            obj=doge.DogePartialWorker(cache=mp_worker.SolPriceCache(fetch,'DOGE'),clock=lambda:clock[0])
            obj.bot,self_subscription=self.bot,lambda:(True,self.chat)
            obj.subscription=self_subscription
            return obj
        obj=make(); scope=obj.scope_for(self.chat)
        mp_store.initialize_scope(scope,clock[0],config_sha256=obj.config_sha256)
        mp_store.transact(scope,clock[0],obj.config_sha256,lambda s:s.update(doge_pending()))
        await obj.observe({'must_not_decode':True},self.chat)
        await obj.tick()
        self.assertFalse(mp_store.snapshot(scope)['active'])
        self.assertEqual(self.fetches,[])
        # Historical filled source survives restart, still uses partial reducer.
        mp_store.transact(scope,clock[0],obj.config_sha256,lambda s:s.update(doge_filled()))
        clock[0]+=M; high[0]=101
        await make().tick()
        saved=mp_store.snapshot(scope)
        self.assertEqual(saved['active'][0]['remaining_fraction'],.25)
        self.assertEqual(saved['active'][0]['stop_price'],98)
        self.assertEqual(saved['intents'][0]['status'],'CANCELLED_FORMULA_RETIRED')
        self.bot.send_message.assert_not_awaited()


class StartupAndWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_liquidity_autostart_disabled_without_disabling_independent_workers(self):
        tree=ast.parse(Path(__file__).with_name('main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='_start_ordered_research_workers')
        names={n.id for n in ast.walk(node) if isinstance(n,ast.Name)}
        scope={'Dict':dict,'Any':object,'DATABASE_URL':'isolated-fixture','os':os,'print':lambda *a,**k:None}
        for name in names:
            if name.startswith('research_'):
                start=AsyncMock(return_value=True)
                scope[name]=SimpleNamespace(WORKER=SimpleNamespace(start=start,status=lambda:{}),
                    start_worker=start,status=lambda:{})
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),scope)
        for _ in range(2):
            result=await scope['_start_ordered_research_workers'](schema_ready=True)
            self.assertEqual(result['liquidity-forward']['reason'],'independent_research_autostart_retired_20261114')
        calls=scope['research_liquidity_forward_worker'].start_worker.await_args_list
        self.assertEqual(len(calls),2)
        self.assertTrue(all(c.kwargs['enabled'] is False for c in calls))
        self.assertTrue(scope['research_forward_paper_worker'].start_worker.await_args.kwargs['enabled'])
        scope['research_price_archive_worker'].WORKER.start.assert_awaited()

    async def test_ordinary_watch_runs_without_consumerless_reference_fetch(self):
        with patch.dict(os.environ,{'ALERT_DELIVERY_PROFILE':'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
            fixture=watch_fixture.SharedWatchCaptureTests()
            scope,result,*_=await fixture.run_cycle()
        self.assertTrue(result['ok'],result)
        self.assertGreater(result['sent'],0)
        scope['experimental_reference_price'].prepare_reference_prices.assert_not_awaited()
        scope['_send_alert_with_confirmation'].assert_awaited()


if __name__=='__main__':
    unittest.main()
