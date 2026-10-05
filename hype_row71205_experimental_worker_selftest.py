"""Offline orchestration tests across actual durable-state transitions."""
import os
import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import hype_row71205_experimental_worker as worker
import hype_row71205_experimental_store as store
from hype_row71205_experimental_store_selftest import MemoryDatabase, BASE, DECISION, ENTRY, NOW


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database = MemoryDatabase()
        self.clock = [int(NOW.timestamp() * 1000)]
        self.fetches = []
        self.entry_high = 100.2
        self.entry_low = 99.7
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
        return [[t, 100., self.entry_high if t == int(ENTRY.timestamp()*1000) else 100.2, self.entry_low if t == int(ENTRY.timestamp()*1000) else 99.7, 100.]
                for t in range(start, end, worker.MINUTE)]

    def new_worker(self):
        obj = worker.Row71205Worker(cache=worker.PriceCache(self.fetch), clock=lambda: self.clock[0])
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
        self.assertEqual(active['take_price'], 98)
        self.assertNotIn(int(ENTRY.timestamp()*1000), self.work.cache.rows['HYPE'])
        self.clock[0] += 30 * worker.MINUTE
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

    async def test_matching_sources_and_distinct_namespace(self):
        import u21_experimental_store as old_store
        import xrp_r2732_experimental_store as xrp_store
        await self.work.tick()
        self.assertEqual({s for s, *_ in self.fetches}, {'HYPE', 'BTC'})
        self.assertNotEqual(store.key_for(self.scope), old_store.key_for(self.scope))
        self.assertNotEqual(store.key_for(self.scope), xrp_store.key_for(self.scope))
        text = self.bot.send_message.call_args.kwargs['text']
        for label in ['ROW71205', 'Binance Futures', 'ללא נעילת רווח', '1:4', 'לא למסחר']:
            self.assertIn(label, text)
        self.assertNotIn('U21', text)
        self.assertEqual(self.work.status()['position_notional_cap'], None)
        self.assertFalse(self.work.status()['live_order_execution'])

    async def test_already_reached_take_vetoes_live_reference(self):
        self.entry_low = 97.9
        await self.work.tick()
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.state()['intents'][0]['status'], 'CANCELLED')
        self.assertIsNotNone(self.state()['active'])

    async def test_restricted_source_fails_closed_with_six_hour_backoff(self):
        checks = []
        async def tick():
            checks.append(self.clock[0])
            raise worker.PriceEvidenceError('HYPE_BINANCE_HTTP_451')
        steps = [10000, worker.RESTRICTED_SOURCE_RETRY_MS-10000, None]
        async def sleep(_):
            delta = steps.pop(0)
            if delta is None:
                raise asyncio.CancelledError()
            self.clock[0] += delta
        with patch.object(self.work, 'tick', side_effect=tick), patch.object(worker.asyncio, 'sleep', side_effect=sleep), \
                patch.object(worker.source_probe, 'probe', return_value={'status': 'ACCESS_NOT_GRANTED'}) as probe:
            with self.assertRaises(asyncio.CancelledError):
                await self.work.run()
        self.assertEqual(len(checks), 2)
        self.assertEqual(checks[1]-checks[0], worker.RESTRICTED_SOURCE_RETRY_MS)
        self.assertEqual(probe.call_count, 2)
        self.assertEqual(self.work.status()['source_diagnostic']['status'], 'ACCESS_NOT_GRANTED')
        self.assertFalse(self.work.status()['ready'])
        self.assertFalse(self.work.status()['hype_source_available'])
        self.assertEqual(self.work.status()['last_error_code'], 'HYPE_BINANCE_HTTP_451')
        self.bot.send_message.assert_not_awaited()

    async def test_transient_source_failure_keeps_five_minute_backoff_no_probe(self):
        async def tick():
            raise worker.PriceEvidenceError('HYPE_BINANCE_HTTP_503')
        with patch.object(self.work, 'tick', side_effect=tick), \
                patch.object(worker.asyncio, 'sleep', side_effect=asyncio.CancelledError), \
                patch.object(worker.source_probe, 'probe') as probe:
            with self.assertRaises(asyncio.CancelledError):
                await self.work.run()
        self.assertEqual(self.work.next_retry_ms, self.clock[0] + 5*worker.MINUTE)
        probe.assert_not_called()

    async def test_diagnostic_exception_cannot_kill_worker_or_mask_original_error(self):
        with patch.object(self.work, 'tick', side_effect=worker.PriceEvidenceError('HYPE_BINANCE_HTTP_451')), \
                patch.object(worker.asyncio, 'sleep', side_effect=asyncio.CancelledError), \
                patch.object(worker.source_probe, 'probe', side_effect=RuntimeError('private diagnostic')):
            with self.assertRaises(asyncio.CancelledError):
                await self.work.run()
        self.assertEqual(self.work.status()['last_error_code'], 'HYPE_BINANCE_HTTP_451')
        self.assertEqual(self.work.status()['source_diagnostic']['status'], 'PROBE_INTERNAL_ERROR')
        self.assertNotIn('private diagnostic', str(self.work.status()))

    async def test_readiness_stays_false_during_failing_warmup(self):
        seen = []
        def failing_fetch(*_):
            seen.append(self.work.status()['ready'])
            raise worker.PriceEvidenceError('HYPE_BINANCE_HTTP_451')
        self.work.cache.fetch = failing_fetch
        with self.assertRaises(worker.PriceEvidenceError):
            await self.work.tick()
        self.assertEqual(seen, [False])
        self.bot.send_message.assert_not_awaited()

    async def test_warm_cache_cannot_falsely_report_recovery(self):
        await self.work.tick()
        self.work.runtime.update(ready=False, last_error_code='HYPE_BINANCE_HTTP_451')
        self.clock[0] += worker.MINUTE
        def failing_fetch(*_):
            raise worker.PriceEvidenceError('HYPE_BINANCE_HTTP_451')
        self.work.cache.fetch = failing_fetch
        with self.assertRaises(worker.PriceEvidenceError):
            await self.work.tick()
        self.assertFalse(self.work.status()['ready'])
        self.assertEqual(self.bot.send_message.await_count, 1)

    async def test_source_recovery_resets_readiness_error_without_replaying_activation(self):
        self.work.runtime.update(ready=False, last_error_code='HYPE_BINANCE_HTTP_451',
                                 hype_source_available=False)
        await self.work.tick()
        self.assertTrue(self.work.status()['ready'])
        self.assertTrue(self.work.status()['hype_source_available'])
        self.assertIsNone(self.work.status()['last_error_code'])

    async def test_warm_cache_monitor_recovery_refreshes_source_health(self):
        await self.work.tick()
        self.work.runtime.update(ready=False, last_error_code='HYPE_BINANCE_HTTP_451',
                                 hype_source_available=False)
        self.clock[0] += worker.MINUTE
        await self.work.tick()
        self.assertTrue(self.work.status()['ready'])
        self.assertTrue(self.work.status()['hype_source_available'])
        self.assertIsNone(self.work.status()['last_error_code'])


class CacheTests(unittest.TestCase):
    def test_incremental_reuse_and_gap_rejection(self):
        calls = []
        def fetch(symbol, start, end):
            calls.append((start, end))
            return [[t, 100, 101, 99, 100] for t in range(start, end, worker.MINUTE)]
        cache = worker.PriceCache(fetch)
        cache.fill('HYPE', 0, 3*worker.MINUTE)
        cache.fill('HYPE', 0, 4*worker.MINUTE)
        self.assertEqual(calls, [(0, 3*worker.MINUTE), (3*worker.MINUTE, 4*worker.MINUTE)])
        cache.fetch = lambda *_: []
        with self.assertRaisesRegex(ValueError, 'Incomplete'):
            cache.fill('HYPE', 0, 5*worker.MINUTE)


class SourceTests(unittest.TestCase):
    def response(self, status=200, rows=None):
        return SimpleNamespace(status_code=status, content=b'[]',
                               raise_for_status=lambda: None,
                               json=lambda: rows or [[0,'100','101','99','100','4',59999]])

    def test_explicit_trade_and_spot_routes_and_no_redirects(self):
        with patch.object(worker.requests, 'get', return_value=self.response()) as request:
            for symbol, url in [('HYPE','https://fapi.binance.com/fapi/v1/klines'),
                                ('BTC',worker.source.BINANCE_SPOT_BASE_URL+worker.source.BINANCE_SPOT_KLINES_ENDPOINT)]:
                self.assertEqual(worker.fetch_rows(symbol,0,worker.MINUTE), [[0,100,101,99,100]])
                self.assertEqual(request.call_args.args[0], url)
                self.assertEqual(request.call_args.kwargs['params']['symbol'], symbol+'USDT')
                self.assertFalse(request.call_args.kwargs['allow_redirects'])

    def test_http_restriction_never_tries_another_route(self):
        with patch.object(worker.requests, 'get', return_value=self.response(status=451)) as request:
            with self.assertRaises(worker.PriceEvidenceError) as error:
                worker.fetch_rows('HYPE',0,worker.MINUTE)
            self.assertEqual(error.exception.code, 'HYPE_BINANCE_HTTP_451')
            self.assertEqual(request.call_count,1)

    def test_bad_ohlc_and_wrong_minute_fail_closed(self):
        for row in [[60000,'100','101','99','100','4',119999],
                    [0,'100','99','98','100','4',59999],
                    [0,'NaN','101','99','100','4',59999]]:
            with patch.object(worker.requests, 'get', return_value=self.response(rows=[row])):
                with self.assertRaises(ValueError):
                    worker.fetch_rows('HYPE',0,worker.MINUTE)


if __name__ == '__main__':
    unittest.main()
