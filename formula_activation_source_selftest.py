"""2026-11-15 source policy and historical HYPE drain, offline or isolated PG."""
from copy import deepcopy
from datetime import timedelta
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import alert_delivery_policy as policy
import hype_row71205_experimental_store as store
import hype_row71205_experimental_worker as worker
from hype_row71205_experimental_store_selftest import MemoryDatabase, BASE, DECISION, ENTRY


class SourceActivationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if os.environ.get('TEST_DATABASE_URL'):
            with store._connect(os.environ['TEST_DATABASE_URL']) as conn:
                conn.execute('CREATE TABLE IF NOT EXISTS bot_settings(key text PRIMARY KEY,value text NOT NULL)')
        else:
            self.database = MemoryDatabase()
            p = patch.object(store, '_connect', self.database.connect)
            p.start(); self.addCleanup(p.stop)
        self.chat = int(uuid4().hex[:12], 16)
        self.scope = worker.subscription_scope(self.chat)
        self.clock = int((ENTRY+timedelta(seconds=70)).timestamp()*1000)
        self.fetches = []
        self.high, self.low = 100.2, 99.7
        self.bot = SimpleNamespace(send_message=AsyncMock())

    def new_worker(self):
        def fetch(symbol, start, end):
            self.fetches.append((symbol, start, end))
            if symbol != 'HYPE':
                raise AssertionError('retirement may not fetch BTC signal inputs')
            return [[t, 100., self.high, self.low, 100.] for t in range(start, end, worker.MINUTE)]
        obj = worker.Row71205Worker(cache=worker.PriceCache(fetch), clock=lambda:self.clock)
        obj.bot, obj.subscription = self.bot, lambda:(True, self.chat)
        return obj

    def reserve_historical(self, *, config=None):
        config = config or worker.CONFIG_SHA256
        store.initialize_scope(self.scope, BASE, config_sha256=config)
        with patch.object(policy, 'hype_row71205_experimental_enabled', return_value=True):
            return store.reserve_signal(self.scope, DECISION, ENTRY, 100., {'valid':True,'signal':True},
                ENTRY+timedelta(seconds=10), config_sha256=config, text='historical notice')

    def state(self):
        return store.snapshot(self.scope)

    async def test_four_roster_all_profiles_pause_is_not_a_symbol_ban(self):
        wanted = {'R2732_XRP_SHORT_NY_WEEKDAYS_LOCK','SFF1361_SOL_RECENT','XRP_LIQUIDITY_RAW_INVERSE',
                  'HYPE_WATCH_LIQ4050_24H_INVERSE'}
        for profile in ('ALL','SELECTED_EXPERIMENTAL_ONLY','ORDINARY_AND_SELECTED_EXPERIMENTAL'):
            with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE':profile}):
                self.assertEqual(set(policy.status()['selected_experimental_rule_ids']), wanted)
                self.assertEqual(set(policy.status()['paused_experimental_rule_ids']), {'CT009_FROZEN_3D','OCONF0034_13'})
                self.assertIn('HYPE_ROW71205_SHORT', policy.status()['retired_experimental_rule_ids'])
                self.assertFalse(policy.hype_row71205_experimental_enabled())
                self.assertEqual(policy.ordinary_alerts_enabled(), profile!='SELECTED_EXPERIMENTAL_ONLY')
        self.assertIn('HYPE_WATCH_LIQ4050_24H_INVERSE', policy.SELECTED_EXPERIMENTAL_RULES)

    async def test_retired_restart_never_creates_scope_or_collects_signal_history(self):
        with patch.object(store, 'initialize_scope') as initialize, patch.object(worker.signal, 'evaluate_signal') as evaluate:
            for _ in range(2):
                obj = self.new_worker()
                await obj.tick()
                self.assertEqual(obj.status()['state'], 'RETIRED')
            initialize.assert_not_called(); evaluate.assert_not_called()
        self.assertIsNone(self.state())
        self.assertEqual(self.fetches, [])
        self.bot.send_message.assert_not_awaited()

    async def test_unsent_notice_retires_but_old_position_and_exit_continue(self):
        self.reserve_historical()
        before = self.state()
        with patch.object(worker.signal, 'evaluate_signal') as evaluate:
            obj = self.new_worker(); await obj.tick()
            after = self.state()
            self.assertEqual(after['intents'][0]['status'],'CANCELLED')
            self.assertEqual(after['intents'][0]['error_type'],'FORMULA_RETIRED')
            for k in ('position_id','entry_at','entry_price','stop_price','take_price'):
                self.assertEqual(after['active'][k], before['active'][k])
            self.assertEqual(after['activated_at'], before['activated_at'])
            self.assertEqual(after['decision_cursor'], before['decision_cursor'])
            self.assertEqual(obj.status()['state'],'RETIRED_MONITORING')
            self.clock += 2*worker.MINUTE; self.high = 101.
            await self.new_worker().tick()
            end=self.state()
            self.assertIsNone(end['active'])
            self.assertEqual(end['history'][-1]['outcome'],'SL')
            self.assertEqual(end['history'][-1]['position_id'],before['active']['position_id'])
            evaluate.assert_not_called()
        self.assertEqual({s for s,_,_ in self.fetches},{'HYPE'})
        self.bot.send_message.assert_not_awaited()

    async def test_uncertain_delivery_is_not_claimed_or_rewritten_after_restart(self):
        self.reserve_historical()
        with patch.object(policy,'hype_row71205_experimental_enabled',return_value=True):
            claimed=store.claim_pending(self.scope,ENTRY+timedelta(seconds=11),config_sha256=worker.CONFIG_SHA256)
        self.assertTrue(store.finish_attempt(self.scope,claimed['intent_id'],claimed['attempt_token'],
            'UNKNOWN',ENTRY+timedelta(seconds=12),config_sha256=worker.CONFIG_SHA256))
        before=deepcopy(self.state()['intents'][0])
        await self.new_worker().tick()
        self.clock+=worker.MINUTE
        await self.new_worker().tick()
        self.assertEqual(self.state()['intents'][0],before)
        self.assertIsNotNone(self.state()['active'])
        self.assertIsNone(store.claim_pending(self.scope,worker.dt(self.clock),config_sha256=worker.CONFIG_SHA256))
        self.bot.send_message.assert_not_awaited()

    async def test_inflight_attempt_remains_unresolved_until_normal_orphan_rule(self):
        self.reserve_historical()
        with patch.object(policy,'hype_row71205_experimental_enabled',return_value=True):
            claimed=store.claim_pending(self.scope,ENTRY+timedelta(seconds=11),config_sha256=worker.CONFIG_SHA256)
        await self.new_worker().tick()
        self.assertEqual(self.state()['intents'][0]['status'],'IN_FLIGHT')
        self.assertEqual(self.state()['intents'][0]['attempt_token'],claimed['attempt_token'])
        self.clock+=2*worker.MINUTE
        await self.new_worker().tick()
        self.assertEqual(self.state()['intents'][0]['status'],'UNKNOWN')
        self.assertEqual(self.state()['intents'][0]['error_type'],'ORPHANED_ATTEMPT')
        self.bot.send_message.assert_not_awaited()

    async def test_direct_reservation_and_claim_cannot_bypass_roster(self):
        store.initialize_scope(self.scope,BASE,config_sha256=worker.CONFIG_SHA256)
        before=self.state()
        result=store.reserve_signal(self.scope,DECISION,ENTRY,100.,{'valid':True,'signal':True},
            ENTRY+timedelta(seconds=10),config_sha256=worker.CONFIG_SHA256)
        self.assertEqual(result['status'],'FORMULA_RETIRED')
        self.assertEqual(self.state(),before)
        self.assertIsNone(store.claim_pending(self.scope,ENTRY+timedelta(seconds=11),config_sha256=worker.CONFIG_SHA256))
        self.assertEqual(self.state(),before)

    async def test_known_old_configuration_keeps_source_identity_and_no_migration(self):
        self.reserve_historical(config=store.LEGACY_MIXED_SOURCE_CONFIG_SHA256)
        before=self.state()
        await self.new_worker().tick()
        after=self.state()
        self.assertEqual(after['config_sha256'],before['config_sha256'])
        self.assertEqual(after['active']['position_id'],before['active']['position_id'])
        self.assertEqual(after['active']['price_source'],before['active']['price_source'])
        self.assertNotIn('all_prices_source_migration',after)
        self.bot.send_message.assert_not_awaited()

    async def test_unrecognized_configuration_is_not_migrated_or_modified(self):
        self.reserve_historical(config='unrecognized')
        before=self.state()
        with self.assertRaisesRegex(ValueError,'configuration hash mismatch'):
            await self.new_worker().tick()
        self.assertEqual(self.state(),before)
        self.assertEqual(self.fetches,[])
        self.bot.send_message.assert_not_awaited()


if __name__=='__main__':
    unittest.main()
