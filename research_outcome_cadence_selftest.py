"""Deterministic async cadence checks with no network, threads or real sleeps."""
import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import research_outcome_worker as worker


class OutcomeCadenceTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, durations, *, failure_at=None, cancel_work=False,
                       cancel_sleep=False):
        service = worker.ResearchOutcomeWorker()
        clock = SimpleNamespace(now=1000.0)
        starts, delays = [], []

        def run_once():
            index = len(starts)
            starts.append(clock.now)
            clock.now += durations[index]
            if cancel_work:
                raise asyncio.CancelledError()
            if index == failure_at:
                raise RuntimeError("controlled cadence failure")
            return {}

        async def to_thread(function):
            return function()

        async def sleep(delay):
            delays.append(delay)
            if cancel_sleep:
                raise asyncio.CancelledError()
            clock.now += delay
            if len(delays) == len(durations):
                service._stopping = True

        fake_asyncio = SimpleNamespace(to_thread=to_thread, sleep=sleep,
                                       CancelledError=asyncio.CancelledError)
        with patch.object(worker, "_POLL_SECONDS", 60), \
             patch.object(worker, "time", SimpleNamespace(monotonic=lambda: clock.now)), \
             patch.object(worker, "asyncio", fake_asyncio), \
             patch.object(service, "run_once", side_effect=run_once):
            if cancel_work or cancel_sleep:
                with self.assertRaises(asyncio.CancelledError):
                    await service._run()
            else:
                await service._run()
        return service, starts, delays

    async def test_long_success_removes_idle_and_always_yields(self):
        service, starts, delays = await self.exercise([190, 211])
        self.assertEqual(starts, [1000, 1190])
        self.assertEqual(delays, [0, 0])
        self.assertEqual(service.metrics.failures, 0)

    async def test_short_success_waits_only_remaining_poll_time(self):
        _, starts, delays = await self.exercise([5, 59])
        self.assertEqual(starts, [1000, 1060])
        self.assertEqual(delays, [55, 1])

    async def test_exact_poll_duration_still_yields(self):
        _, _, delays = await self.exercise([60])
        self.assertEqual(delays, [0])

    async def test_slow_exception_keeps_full_backoff_then_recovers(self):
        service, starts, delays = await self.exercise([200, 1], failure_at=0)
        self.assertEqual(starts, [1000, 1260])
        self.assertEqual(delays, [60, 59])
        self.assertEqual(service.metrics.failures, 1)
        self.assertEqual(service.metrics.last_error, "RuntimeError: controlled cadence failure")

    async def test_work_cancellation_propagates_without_backoff(self):
        service, starts, delays = await self.exercise([5], cancel_work=True)
        self.assertEqual(starts, [1000])
        self.assertEqual(delays, [])
        self.assertEqual(service.metrics.failures, 0)

    async def test_cancellation_at_zero_delay_yield_propagates(self):
        service, starts, delays = await self.exercise([190], cancel_sleep=True)
        self.assertEqual(starts, [1000])
        self.assertEqual(delays, [0])
        self.assertEqual(service.metrics.failures, 0)


if __name__ == "__main__":
    unittest.main()
