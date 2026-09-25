"""Offline orchestration tests across actual durable-state transitions."""
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import u21_experimental_worker as worker
import u21_experimental_store as store
from u21_experimental_store_selftest import MemoryDatabase, BASE, DECISION, ENTRY, NOW


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database = MemoryDatabase()
        self.clock = [int(NOW.timestamp() * 1000)]
        self.fetches = []
        self.entry_high = 100.2
        self.bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=123)))
        self.enabled = True
        self.scope = worker.subscription_scope(1234)
        for p in (patch.object(store, '_connect', self.database.connect),
                  patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}),
                  patch.object(worker.signal, 'evaluate_signal', return_value={'valid': True, 'signal': True})):
            p.start(); self.addCleanup(p.stop)
        store.initialize_scope(self.scope, BASE, config_sha256=worker.CONFIG_SHA256)
        self.work = self.new_worker()

    def fetch(self, symbol, start, end):
        self.fetches.append((symbol, start, end))
        return [[t, 100., self.entry_high if t == int(ENTRY.timestamp()*1000) else 100.2, 99., 100.]
                for t in range(start, end, worker.MINUTE)]

    def new_worker(self):
        obj = worker.U21Worker(cache=worker.PriceCache(self.fetch), clock=lambda: self.clock[0])
        obj.bot = self.bot
        obj.subscription = lambda: (self.enabled, 1234)
        return obj

    def state(self):
        return store.snapshot(self.scope)

    async def test_fresh_reference_once_and_restart_preserves_cap(self):
        await self.work.tick()
        self.assertEqual(self.bot.send_message.await_count, 1)
        active = self.state()['active']
        self.assertEqual(active['entry_price'], 100)
        self.assertEqual(active['entry_at'], store.iso(ENTRY))
        self.assertEqual(active['stop_price'], worker.signal.build_levels(100)['stop_loss'])
        self.assertEqual(active['take_price'], 92)
        self.assertNotIn(int(ENTRY.timestamp()*1000), self.work.cache.rows['XRP'])
        self.clock[0] += 15 * worker.MINUTE
        await self.new_worker().tick()
        self.assertEqual(self.bot.send_message.await_count, 1)
        self.assertEqual(self.state()['active']['position_id'], active['position_id'])
        self.assertEqual(self.state()['last_decision']['status'], 'ACTIVE_POSITION')

    async def test_unknown_send_is_never_retried(self):
        self.bot.send_message.side_effect = TimeoutError('uncertain result')
        await self.work.tick()
        self.assertEqual(self.state()['intents'][0]['status'], 'UNKNOWN')
        self.clock[0] += worker.MINUTE
        await self.new_worker().tick()
        self.assertEqual(self.bot.send_message.await_count, 1)
        self.assertIsNotNone(self.state()['active'])

    async def test_expired_slot_and_new_activation_do_not_backfill(self):
        self.clock[0] = int(ENTRY.timestamp()*1000) + 90000
        await self.work.tick()
        self.assertIsNone(self.state()['active'])
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.state()['last_decision']['status'], 'STALE_SLOT_SKIPPED')
        self.database.values.clear()
        await self.new_worker().tick()
        self.assertIsNone(self.state()['active'])
        self.bot.send_message.assert_not_awaited()

    async def test_late_closed_entry_bar_exit_suppresses_message(self):
        self.clock[0] = int(ENTRY.timestamp()*1000) + 65000
        self.entry_high = 101
        await self.work.tick()
        self.bot.send_message.assert_not_awaited()
        self.assertIsNone(self.state()['active'])
        self.assertEqual(self.state()['history'][0]['outcome'], 'SL')

    async def test_live_barrier_veto_holds_capacity_until_closed_bar(self):
        self.entry_high = 101
        await self.work.tick()
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.state()['intents'][0]['status'], 'CANCELLED')
        self.assertIsNotNone(self.state()['active'])
        self.clock[0] += worker.MINUTE
        await self.work.tick()
        self.assertIsNone(self.state()['active'])

    async def test_watch_off_suppresses_new_entry(self):
        self.enabled = False
        await self.work.tick()
        self.bot.send_message.assert_not_awaited()
        self.assertIsNone(self.state()['active'])
        self.assertEqual(self.fetches, [])

    async def test_subscription_change_during_claim_blocks_transport(self):
        original = store.claim_pending
        def claim(*args, **kwargs):
            intent = original(*args, **kwargs)
            if intent: self.enabled = False
            return intent
        with patch.object(store, 'claim_pending', claim):
            await self.work.tick()
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.state()['intents'][0]['status'], 'FAILED')


class CacheTests(unittest.TestCase):
    def test_incremental_reuse_and_gap_rejection(self):
        calls = []
        def fetch(symbol, start, end):
            calls.append((start, end))
            return [[t, 100, 101, 99, 100] for t in range(start, end, worker.MINUTE)]
        cache = worker.PriceCache(fetch)
        cache.fill('XRP', 0, 3*worker.MINUTE)
        cache.fill('XRP', 0, 4*worker.MINUTE)
        self.assertEqual(calls, [(0, 3*worker.MINUTE), (3*worker.MINUTE, 4*worker.MINUTE)])
        cache.fetch = lambda *_: []
        with self.assertRaisesRegex(ValueError, 'Incomplete'):
            cache.fill('XRP', 0, 5*worker.MINUTE)


if __name__ == '__main__':
    unittest.main()
