"""Offline retirement fixtures: no new alerts, retained positions keep exits."""
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import alert_delivery_policy as policy
import doge_partial_experimental_worker as doge
import sol_proximity_experimental_signal as signal
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as worker
from hype_row71205_experimental_store_selftest import MemoryDatabase
from maxpain_experimental_specs import SPECS
from sol_proximity_experimental_selftest import BASE, M, bar, pending


class RetirementTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = MemoryDatabase()
        self.clock = BASE+3*M
        self.fetches = []
        self.high, self.low = 97., 95.
        self.bot = SimpleNamespace(send_message=AsyncMock())
        self.patchers = [
            patch.object(store, '_connect', self.db.connect),
            patch.dict('os.environ', {'ALERT_DELIVERY_PROFILE': 'ALL'}),
            patch.object(worker.pending_source_restore, 'schedule'),
        ]
        self.restore = self.patchers[-1].start()
        for p in self.patchers[:-1]:
            p.start()
        for p in self.patchers:
            self.addCleanup(p.stop)

    def make_worker(self, coin='SOL'):
        def fetch(symbol, start, end):
            self.fetches.append((symbol, start, end))
            return [bar(t, 96, self.high, self.low, 96) for t in range(start, end, M)]
        w = worker.SolProximityWorker(spec=SPECS[coin], clock=lambda: self.clock,
            cache=worker.SolPriceCache(fetch, coin), legacy_cache=worker.SolPriceCache(fetch, coin))
        w.bot, w.subscription = self.bot, lambda: (True, 123)
        return w

    def install(self, w, state):
        scope = w.scope_for(123)
        store.initialize_scope(scope, self.clock, config_sha256=w.config_sha256)
        store.transact(scope, self.clock, w.config_sha256, lambda st: st.update(deepcopy(state)))
        return scope

    def open_state(self):
        state = pending()
        signal.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 100, 95, 96)], self.clock)
        return state

    async def test_all_five_retire_pending_before_price_can_fill(self):
        for coin in SPECS:
            with self.subTest(coin=coin):
                w = self.make_worker(coin)
                self.assertIn(w.spec.rule_id, policy.RETIRED_EXPERIMENTAL_RULES)
                state = pending()
                original = deepcopy(state['active'][0])
                scope = self.install(w, state)
                await w.tick()
                saved = store.snapshot(scope)
                self.assertFalse(saved['active'])
                self.assertEqual(saved['history'][-1]['position_id'], original['position_id'])
                self.assertEqual(saved['history'][-1]['status'], 'CANCELLED_FORMULA_RETIRED')
                self.assertEqual(saved['config_sha256'], w.config_sha256)
                self.assertFalse(saved['counts'].get('FILLED', 0))
                self.assertEqual(w.status()['state'], 'RETIRED')
                before = deepcopy(saved)
                restarted = self.make_worker(coin)
                await restarted.tick()
                self.assertEqual(store.snapshot(scope), before)
        self.assertEqual(self.fetches, [])
        self.bot.send_message.assert_not_awaited()
        self.restore.assert_not_called()

    async def test_unknown_and_open_levels_preserved_and_open_can_exit(self):
        w = self.make_worker()
        state = self.open_state()
        opened = deepcopy(state['active'][0])
        unknown = dict(deepcopy(opened), position_id='uncertain', status='UNKNOWN',
                       unknown_reason='BARRIER_ORDER_UNKNOWN')
        state['active'].append(unknown)
        state['intents'].append(dict(state['intents'][0], intent_id='in-flight',
                                    status='IN_FLIGHT', attempt_ms=self.clock))
        scope = self.install(w, state)
        await w.tick()
        saved = store.snapshot(scope)
        self.assertEqual(saved['active'], [opened, unknown])
        self.assertEqual(saved['intents'][0]['status'], 'CANCELLED_FORMULA_RETIRED')
        self.assertEqual(saved['intents'][1]['status'], 'IN_FLIGHT')
        self.assertEqual(w.status()['state'], 'RETIRED_DRAINING')
        self.clock += M
        self.high = 103.
        await w.tick()
        saved = store.snapshot(scope)
        self.assertEqual(saved['active'], [unknown])
        self.assertEqual(saved['history'][-1]['status'], 'TAKE_TOUCHED')
        self.assertEqual(saved['history'][-1]['stop_price'], opened['stop_price'])
        self.assertEqual(saved['history'][-1]['take_price'], opened['take_price'])
        self.assertEqual(w.status()['active_unknown'], 1)
        self.bot.send_message.assert_not_awaited()
        self.restore.assert_not_called()

    async def test_legacy_pending_retires_and_legacy_open_keeps_original_route(self):
        w = self.make_worker()
        state = self.open_state()
        state['active'], state['intents'] = [], []
        legacy = self.open_state()
        opened = deepcopy(legacy['active'][0])
        waiting = dict(pending()['active'][0], position_id='legacy-wait')
        legacy['active'].append(waiting)
        legacy['source_route'] = 'BINANCE_SPOT_SOLUSDT_TRADE_1M'
        state['legacy_source_state'] = legacy
        scope = self.install(w, state)
        await w.tick()
        saved = store.snapshot(scope)
        self.assertEqual(saved['legacy_source_state']['active'], [opened])
        self.assertEqual(saved['legacy_source_state']['source_route'], legacy['source_route'])
        self.assertEqual(saved['legacy_source_state']['history'][-1]['status'], 'CANCELLED_FORMULA_RETIRED')
        self.clock += M
        self.low = 89.
        await w.tick()
        saved = store.snapshot(scope)
        self.assertFalse(saved['legacy_source_state']['active'])
        self.assertEqual(saved['legacy_source_state']['history'][-1]['status'], 'STOP_TOUCHED')
        self.assertEqual(w.status()['state'], 'RETIRED')

    async def test_absent_scope_does_not_create_or_repeatedly_poll(self):
        w = self.make_worker()
        await w.tick()
        calls = len(self.db.calls)
        self.clock += M
        await w.tick()
        self.assertEqual(len(self.db.calls), calls)
        self.assertEqual(self.db.values, {})
        self.assertEqual(self.fetches, [])
        self.assertEqual(w.status()['state'], 'RETIRED')

    async def test_retired_observation_and_delivery_are_blocked(self):
        w = self.make_worker()
        scope = self.install(w, self.open_state())
        before = store.snapshot(scope)
        await w.observe({'must_not_be_decoded': True}, 123)
        self.assertFalse(await w.deliver(scope, 123))
        self.assertEqual(store.snapshot(scope), before)
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.fetches, [])

    async def test_configuration_mismatch_does_not_migrate_or_cancel(self):
        w = self.make_worker()
        scope = self.install(w, pending())
        original = store.snapshot(scope)
        store.transact(scope, self.clock, w.config_sha256, lambda st: st.update(config_sha256='different'))
        before = deepcopy(self.db.values)
        with self.assertRaisesRegex(ValueError, 'configuration mismatch'):
            await w.tick()
        self.assertEqual(self.db.values, before)
        self.assertEqual(store.snapshot(scope)['active'], original['active'])

    def test_retained_doge_worker_and_cache_unchanged(self):
        self.assertTrue(doge.WORKER.policy_enabled())
        self.assertNotIn(doge.RULE_ID, policy.RETIRED_EXPERIMENTAL_RULES)
        self.assertIs(doge.WORKER.cache, worker.ADDITIONAL_WORKERS['DOGE'].cache)
        self.assertEqual(worker.config_hash(SPECS['SOL']), worker.CONFIG_SHA256)


if __name__ == '__main__':
    unittest.main()
