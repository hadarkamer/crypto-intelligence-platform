"""Fault injection against real XRP worker/store; no network or Telegram.

The dated replay reproduces the durable timing facts, then explicitly assumes
the next source request succeeds. It does not assert what the historical API
would have returned on a request that production never made.
"""
import asyncio
from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, patch

import xrp_r2732_experimental_worker as worker
import xrp_r2732_experimental_store as store
import xrp_r2732_experimental_worker_selftest as fixtures
from u21_experimental_store_selftest import DECISION, ENTRY


class DeliveryRetryTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.WorkerTests.setUp
    fetch = fixtures.WorkerTests.fetch
    new_worker = fixtures.WorkerTests.new_worker
    state = fixtures.WorkerTests.state

    def pending(self, *, entry=ENTRY, elapsed=65_757, gated=True):
        entry_ms = int(entry.timestamp() * 1000)
        self.clock[0] = entry_ms + elapsed
        result = store.reserve_signal(
            self.scope, worker.dt(entry_ms-worker.MINUTE), entry, 100,
            {'valid': True, 'signal': True}, worker.dt(self.clock[0]),
            text='offline fixture', config_sha256=worker.CONFIG_SHA256)
        self.assertEqual(result['status'], 'RESERVED')
        self.work.scopes.add(self.scope)
        if gated:
            self.work.last_poll_minute = (self.scope, self.clock[0] // worker.MINUTE)
        return entry_ms

    async def test_dated_case_source_recovers_next_loop_inside_24_seconds(self):
        entry = datetime(2026, 10, 28, 22, 16, tzinfo=timezone.utc)
        entry_ms = self.pending(entry=entry)
        expiry = self.state()['intents'][0]['expires_at']
        self.assertEqual(worker.ms(expiry)-self.clock[0], 24_243)
        def outage(*args):
            self.clock[0] = entry_ms + 66_274
            raise worker.hyperliquid.HyperliquidSourceError('REQUEST_FAILED')
        with patch.object(self.work.cache, 'fetch', side_effect=outage):
            with self.assertRaises(worker.hyperliquid.HyperliquidSourceError):
                await self.work.tick()
        self.assertIsNone(self.state()['intents'][0]['attempted_at'])
        self.clock[0] += 10_000
        await self.work.tick()
        self.assertEqual(self.state()['intents'][0]['status'], 'DELIVERED')
        self.assertEqual(self.state()['intents'][0]['expires_at'], expiry)
        self.assertLess(self.clock[0], worker.ms(expiry))
        await self.work.tick()
        self.assertEqual(self.bot.send_message.await_count, 1)

    async def test_pending_is_delivered_before_independent_monitor_failure(self):
        self.pending(gated=False)
        with patch.object(self.work, 'monitor', side_effect=ValueError('monitor unavailable')):
            with self.assertRaisesRegex(ValueError, 'monitor unavailable'):
                await self.work.tick()
        self.assertEqual(self.state()['intents'][0]['status'], 'DELIVERED')
        self.assertEqual(self.bot.send_message.await_count, 1)

    async def test_new_reservation_delivers_before_post_reservation_monitor(self):
        original = self.work.monitor
        async def monitor(scope, state, closed_end):
            if state.get('active'):
                raise ValueError('post-reservation monitor unavailable')
            return await original(scope, state, closed_end)
        with patch.object(self.work, 'monitor', side_effect=monitor):
            with self.assertRaisesRegex(ValueError, 'post-reservation'):
                await self.work.tick()
        self.assertEqual(self.state()['intents'][0]['status'], 'DELIVERED')
        self.assertEqual(self.bot.send_message.await_count, 1)

    async def test_persistent_source_failure_expires_without_attempt_or_ttl_extension(self):
        entry_ms = self.pending()
        expiry = self.state()['intents'][0]['expires_at']
        with patch.object(self.work.cache, 'fetch', side_effect=worker.hyperliquid.HyperliquidSourceError('REQUEST_FAILED')):
            for elapsed in [66_274, 76_274, 86_274]:
                self.clock[0] = entry_ms + elapsed
                with self.assertRaises(worker.hyperliquid.HyperliquidSourceError):
                    await self.work.tick()
            self.clock[0] = entry_ms + 96_274
            await self.work.tick()
        intent = self.state()['intents'][0]
        self.assertEqual(intent['status'], 'EXPIRED')
        self.assertEqual(intent['expires_at'], expiry)
        self.assertIsNone(intent['attempted_at'])
        self.assertIsNotNone(self.state()['active'])
        self.bot.send_message.assert_not_awaited()

    async def test_incomplete_pre_send_coverage_cannot_claim(self):
        self.pending()
        with patch.object(self.work.cache, 'fetch', return_value=[]):
            with self.assertRaisesRegex(ValueError, 'Incomplete pre-send'):
                await self.work.tick()
        self.assertEqual(self.state()['intents'][0]['status'], 'PENDING')
        self.assertIsNone(self.state()['intents'][0]['attempted_at'])
        self.bot.send_message.assert_not_awaited()

    async def test_crossing_minute_requires_fresh_live_coverage(self):
        entry_ms = self.pending(elapsed=59_000)
        def slow_fetch(symbol, start, end):
            rows = self.fetch(symbol, start, end)
            self.clock[0] = entry_ms + 61_000
            return rows
        with patch.object(self.work.cache, 'fetch', side_effect=slow_fetch):
            await self.work.deliver(self.scope, 1234)
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.state()['intents'][0]['status'], 'PENDING')
        self.clock[0] = entry_ms + 71_000
        await self.work.deliver(self.scope, 1234)
        self.assertEqual(self.state()['intents'][0]['status'], 'DELIVERED')
        self.assertEqual(self.fetches[-1][2], entry_ms + 2 * worker.MINUTE)

    async def test_pre_send_fetch_reaching_deadline_does_not_send(self):
        entry_ms = self.pending(elapsed=80_000)
        def slow_fetch(symbol, start, end):
            rows = self.fetch(symbol, start, end)
            self.clock[0] = entry_ms + 90_000
            return rows
        with patch.object(self.work.cache, 'fetch', side_effect=slow_fetch):
            await self.work.deliver(self.scope, 1234)
        self.assertEqual(self.state()['intents'][0]['status'], 'EXPIRED')
        self.assertIsNone(self.state()['intents'][0]['attempted_at'])
        self.bot.send_message.assert_not_awaited()

    async def test_claim_crossing_minute_fails_without_send_or_retry(self):
        entry_ms = self.pending(elapsed=59_000)
        expiry = self.state()['intents'][0]['expires_at']
        original = store.claim_pending
        def slow_claim(*args, **kwargs):
            intent = original(*args, **kwargs)
            self.clock[0] = entry_ms + 61_000
            return intent
        with patch.object(store, 'claim_pending', side_effect=slow_claim):
            await self.work.deliver(self.scope, 1234)
        intent = self.state()['intents'][0]
        self.assertEqual(self.fetches[-1][2], entry_ms + worker.MINUTE)
        self.assertEqual(intent['status'], 'FAILED')
        self.assertEqual(intent['error_type'], 'PRE_SEND_COVERAGE_EXPIRED_DURING_CLAIM')
        self.assertEqual(intent['expires_at'], expiry)
        self.assertIsNotNone(intent['attempt_token'])
        self.assertIsNotNone(self.state()['active'])
        self.bot.send_message.assert_not_awaited()
        self.clock[0] = entry_ms + 71_000
        await self.work.deliver(self.scope, 1234)
        self.assertEqual(self.state()['counts']['attempted'], 1)
        self.assertEqual(self.state()['intents'][0]['status'], 'FAILED')
        self.bot.send_message.assert_not_awaited()

    async def test_transport_timeout_is_bounded_by_original_remaining_deadline(self):
        self.pending(elapsed=85_000)
        original = asyncio.wait_for
        timeouts = []
        async def wait_for(awaitable, timeout):
            timeouts.append(timeout)
            return await original(awaitable, timeout)
        with patch.object(worker.asyncio, 'wait_for', side_effect=wait_for):
            await self.work.deliver(self.scope, 1234)
        self.assertEqual(timeouts, [5])

    async def test_same_minute_recovery_still_vetoes_observed_stop_or_lock(self):
        self.pending(entry=ENTRY)
        self.entry_low = 98.9
        await self.work.tick()
        self.assertEqual(self.state()['intents'][0]['status'], 'CANCELLED')
        self.assertIsNone(self.state()['intents'][0]['attempted_at'])
        self.bot.send_message.assert_not_awaited()

    async def test_concurrent_delivery_claim_still_sends_once(self):
        self.pending()
        await asyncio.gather(self.work.deliver(self.scope, 1234),
                             self.work.deliver(self.scope, 1234))
        self.assertEqual(self.bot.send_message.await_count, 1)
        self.assertEqual(self.state()['intents'][0]['status'], 'DELIVERED')

    async def test_cancelled_inflight_send_is_not_retried(self):
        self.pending()
        self.bot.send_message.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await self.work.deliver(self.scope, 1234)
        self.assertEqual(self.state()['intents'][0]['status'], 'IN_FLIGHT')
        await self.work.deliver(self.scope, 1234)
        self.assertEqual(self.bot.send_message.await_count, 1)


if __name__ == '__main__':
    unittest.main()
