"""Offline regression checks for known-unsent DOGE price-evidence retries."""
import asyncio
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import doge_partial_experimental_worker as doge
from doge_partial_experimental_selftest import BASE, M, bar, filled
from experimental_hyperliquid_source import HyperliquidSourceError
from hype_row71205_experimental_store_selftest import MemoryDatabase
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as parent


class DeliveryRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.database = MemoryDatabase()
        self.clock = [BASE + 3*M + 10_000]
        self.fetch = Mock(side_effect=self.safe_rows)
        self.bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=55)))
        for guard in (patch.object(store, '_connect', self.database.connect),
                      patch.object(parent.policy, 'maxpain_component_experimental_enabled', return_value=True),
                      patch('requests.post', side_effect=AssertionError('Network forbidden')),
                      patch('requests.get', side_effect=AssertionError('Network forbidden'))):
            guard.start()
            self.addCleanup(guard.stop)
        self.worker = self.make_worker()
        self.scope = self.worker.scope_for(123)
        await self.worker.initialize(self.scope)
        store.transact(self.scope, self.clock[0], self.worker.config_sha256,
                       lambda state: state.update(filled()))
        self.original = self.intent()

    def make_worker(self):
        worker = doge.DogePartialWorker(cache=parent.SolPriceCache(self.fetch, symbol='DOGE'),
                                        clock=lambda: self.clock[0])
        worker.bot, worker.subscription = self.bot, lambda: (True, 123)
        return worker

    def safe_rows(self, symbol, start, end):
        return [bar(t, 99, 99.5, 98.9, 99) for t in range(start, end, M)]

    def intent(self):
        return store.snapshot(self.scope)['intents'][0]

    def claim(self):
        return store.claim_pending(self.scope, self.clock[0], config_sha256=self.worker.config_sha256)

    def release(self, intent, *, token=None):
        return store.release_unattempted(self.scope, intent['intent_id'],
            token or intent['attempt_token'], self.clock[0], config_sha256=self.worker.config_sha256)

    async def fail_evidence(self, code='INCOMPLETE_MINUTES'):
        self.fetch.side_effect = HyperliquidSourceError(code)
        self.assertFalse(await self.worker.deliver(self.scope, 123))
        self.bot.send_message.assert_not_awaited()

    async def test_transient_retries_once_after_recovery_with_original_deadline(self):
        await self.fail_evidence()
        waiting = self.intent()
        self.assertEqual(waiting['status'], 'PENDING')
        self.assertIsNone(waiting['attempt_token'])
        self.assertEqual(waiting['payload'], self.original['payload'])
        self.assertEqual(waiting['expires_ms'], self.original['expires_ms'])
        self.assertEqual(self.worker.retry_ms, self.clock[0] + 10_000)
        self.assertEqual(self.fetch.call_count, 1)
        self.clock[0] = self.worker.retry_ms
        self.fetch.side_effect = self.safe_rows
        restarted = self.make_worker()
        await restarted.initialize(self.scope)
        self.assertTrue(await restarted.deliver(self.scope, 123))
        self.assertFalse(await restarted.deliver(self.scope, 123))
        self.assertEqual(self.intent()['status'], 'DELIVERED')
        self.assertEqual(self.intent()['expires_ms'], self.original['expires_ms'])
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.fetch.call_count, 2)

    async def test_rate_limit_keeps_existing_cooldown_without_extending_expiry(self):
        await self.fail_evidence('HTTP_429')
        self.assertEqual(self.intent()['status'], 'PENDING')
        self.assertEqual(self.worker.retry_ms, self.clock[0] + M)
        self.assertEqual(self.intent()['expires_ms'], self.original['expires_ms'])
        self.clock[0] = self.original['expires_ms']
        self.fetch.side_effect = self.safe_rows
        self.assertFalse(await self.worker.deliver(self.scope, 123))
        self.assertEqual(self.intent()['status'], 'EXPIRED')
        self.assertEqual(self.fetch.call_count, 1)
        self.bot.send_message.assert_not_awaited()

    async def test_failure_that_crosses_deadline_is_terminal(self):
        def delayed_failure(*args):
            self.clock[0] = self.original['expires_ms']
            raise HyperliquidSourceError('REQUEST_FAILED')
        self.fetch.side_effect = delayed_failure
        self.assertFalse(await self.worker.deliver(self.scope, 123))
        self.assertEqual(self.intent()['status'], 'EXPIRED')
        self.assertEqual(self.intent()['expires_ms'], self.original['expires_ms'])
        self.assertFalse(await self.make_worker().deliver(self.scope, 123))
        self.assertEqual(self.fetch.call_count, 1)
        self.bot.send_message.assert_not_awaited()

    async def test_post_send_timeout_remains_unknown_and_never_retries(self):
        self.bot.send_message.side_effect = TimeoutError('ambiguous send')
        self.assertTrue(await self.worker.deliver(self.scope, 123))
        intent = self.intent()
        self.assertEqual(intent['status'], 'UNKNOWN')
        self.assertIsNone(self.release(intent))
        self.clock[0] += 10_000
        self.assertFalse(await self.make_worker().deliver(self.scope, 123))
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.fetch.call_count, 1)

    async def test_definitive_send_failure_stays_terminal(self):
        self.bot.send_message.side_effect = type('Forbidden', (Exception,), {})('blocked')
        self.assertTrue(await self.worker.deliver(self.scope, 123))
        self.assertEqual(self.intent()['status'], 'FAILED')
        self.assertFalse(await self.make_worker().deliver(self.scope, 123))
        self.bot.send_message.assert_awaited_once()

    async def test_nontransient_evidence_failure_stays_terminal(self):
        await self.fail_evidence('INVALID_OHLC')
        self.assertEqual(self.intent()['status'], 'FAILED')
        self.assertEqual(self.worker.retry_ms, self.clock[0] + 5*M)
        self.assertFalse(await self.make_worker().deliver(self.scope, 123))
        self.assertEqual(self.fetch.call_count, 1)

    async def test_recovered_price_still_vetoes_partial_take(self):
        await self.fail_evidence()
        self.clock[0] = self.worker.retry_ms
        self.fetch.side_effect = lambda coin, start, end: [bar(t, 99, 101, 98.9, 99)
                                                          for t in range(start, end, M)]
        self.assertTrue(await self.worker.deliver(self.scope, 123))
        self.assertEqual(self.intent()['status'], 'CANCELLED_STALE_PRICE')
        self.bot.send_message.assert_not_awaited()

    async def test_exact_token_cannot_release_newer_or_terminal_claim(self):
        first = self.claim()
        self.assertIsNone(self.release(first, token='wrong-token'))
        self.assertEqual(self.intent()['status'], 'IN_FLIGHT')
        self.assertEqual(self.release(first), 'PENDING')
        second = self.claim()
        self.assertNotEqual(first['attempt_token'], second['attempt_token'])
        self.assertIsNone(self.release(first))
        self.assertEqual(self.intent()['attempt_token'], second['attempt_token'])
        store.finish_attempt(self.scope, second['intent_id'], second['attempt_token'], 'DELIVERED',
                             self.clock[0], message_id=55, config_sha256=self.worker.config_sha256)
        self.assertIsNone(self.release(second))
        self.assertEqual(self.intent()['status'], 'DELIVERED')

    async def test_ended_source_cannot_be_released_into_pending(self):
        intent = self.claim()
        store.transact(self.scope, self.clock[0], self.worker.config_sha256,
                       lambda state: state.update(active=[]))
        self.assertEqual(self.release(intent), 'CANCELLED_POSITION_ENDED')
        self.assertIsNone(self.claim())

    async def test_orphaned_claim_is_not_released(self):
        intent = self.claim()
        self.clock[0] += 120_000
        self.assertIsNone(self.release(intent))
        self.assertEqual(self.intent()['status'], 'UNKNOWN')
        self.assertIsNone(self.claim())

    async def test_concurrent_retry_claims_cannot_duplicate_fetch_or_send(self):
        await self.fail_evidence()
        self.clock[0] = self.worker.retry_ms
        started, finish = Event(), Event()
        self.addCleanup(finish.set)
        def blocked_fetch(*args):
            started.set()
            if not finish.wait(2):
                raise AssertionError('Concurrent test stalled')
            return self.safe_rows(*args)
        self.fetch.side_effect = blocked_fetch
        other = self.make_worker()
        first = asyncio.create_task(self.worker.deliver(self.scope, 123))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 2))
            self.assertFalse(await other.deliver(self.scope, 123))
        finally:
            finish.set()
            result = await first
        self.assertTrue(result)
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.fetch.call_count, 2)  # Initial failure, then one retry.
        self.assertEqual(self.intent()['status'], 'DELIVERED')


if __name__ == '__main__':
    unittest.main()
