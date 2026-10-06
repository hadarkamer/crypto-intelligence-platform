"""Restart cadence, cross-instance claims and partial Telegram delivery checks."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from telegram.error import BadRequest, TimedOut
import main

BASE = datetime(2026, 9, 13, 11, 58, tzinfo=timezone.utc)
SLOT = BASE.replace(hour=12, minute=2, second=15)


class ScheduleTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, start=BASE, *, immediate=False, claims=(True,), crash=False):
        clock = [start]
        sleeps, scans = [], []

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock[0]

        async def sleep(delay):
            self.assertGreater(delay, 0)
            sleeps.append(delay)
            clock[0] += timedelta(seconds=delay)

        async def scan(*args, **kwargs):
            scans.append(clock[0])
            clock[0] += timedelta(minutes=3)
            if not crash or len(scans) == 2:
                main.WATCH_GENERAL_ENABLED = False
            if crash and len(scans) == 1:
                raise RuntimeError('fixture cycle error')

        with patch.object(main, 'datetime', Clock), \
             patch.object(main, 'WATCH_GENERAL_ENABLED', True), \
             patch.object(main, 'MAGNET_V1_WATCHES', {}), \
             patch.object(main, 'WATCH_RUNTIME', {}), \
             patch.object(main, 'WATCH_INTERVAL_MINUTES', 30), \
             patch.object(main, 'WATCH_SYNC_GRACE_SECONDS', 135), \
             patch.object(main.asyncio, 'sleep', sleep), \
             patch.object(main, '_claim_watch_slot', side_effect=claims) as claim, \
             patch.object(main, 'run_watch_cycle', scan):
            await main.watch_loop(SimpleNamespace(), 1, run_immediately=immediate)
            self.assertIsNone(main.WATCH_SCAN_TASK)
            self.assertFalse(main.WATCH_RUNTIME['scan_in_progress'])
            return sleeps, scans, claim.call_args_list

    async def test_restore_waits_for_new_closed_candle(self):
        sleeps, scans, claims = await self.exercise()
        self.assertEqual(sleeps, [255])
        self.assertEqual(scans, [SLOT])
        self.assertEqual(claims[0].args, (SLOT,))
        flow = main.coinglass_flow_foundation
        with patch.object(flow, 'CVD_TIMESTAMP_MODE', 'open'):
            self.assertEqual(flow.candle_close_time(flow.latest_eligible_candle_time(SLOT)), SLOT.replace(minute=0, second=0))
            self.assertEqual(flow.candle_close_time(flow.latest_eligible_candle_time(BASE)), BASE.replace(minute=30, second=0))

    async def test_repeated_restarts_do_not_replay_previous_slot(self):
        for minute in (3, 10, 28, 31):
            _, scans, _ = await self.exercise(SLOT.replace(minute=minute, second=0))
            self.assertEqual(scans, [SLOT + timedelta(minutes=30)])

    async def test_claim_collision_or_database_failure_skips_that_slot(self):
        for first in (False, RuntimeError('fixture DB unavailable')):
            _, scans, claims = await self.exercise(claims=(first, True))
            self.assertEqual(scans, [SLOT + timedelta(minutes=30)])
            self.assertEqual(len(claims), 2)

    async def test_cycle_failure_preserves_next_aligned_slot(self):
        _, scans, _ = await self.exercise(claims=(True, True), crash=True)
        self.assertEqual(scans, [SLOT, SLOT + timedelta(minutes=30)])

    async def test_explicit_start_still_allows_immediate_scan(self):
        sleeps, scans, claims = await self.exercise(immediate=True)
        self.assertEqual((sleeps, scans, claims), ([], [BASE], []))

    async def test_stop_during_wait_never_claims_or_sends(self):
        with patch.object(main, 'WATCH_GENERAL_ENABLED', True), \
             patch.object(main, 'MAGNET_V1_WATCHES', {}), \
             patch.object(main, 'WATCH_RUNTIME', {}), \
             patch.object(main.asyncio, 'sleep', AsyncMock(side_effect=asyncio.CancelledError)), \
             patch.object(main, '_claim_watch_slot') as claim, \
             patch.object(main, 'run_watch_cycle', AsyncMock()) as scan:
            with self.assertRaises(asyncio.CancelledError):
                await main.watch_loop(SimpleNamespace(), 1)
            claim.assert_not_called()
            scan.assert_not_awaited()


def assert_claims(test):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(main._claim_watch_slot, [SLOT] * 4))
    test.assertEqual(results.count(True), 1)
    test.assertFalse(main._claim_watch_slot(SLOT - timedelta(minutes=30)))
    test.assertFalse(main._claim_watch_slot(SLOT))
    test.assertTrue(main._claim_watch_slot(SLOT + timedelta(minutes=30)))


class ClaimTests(unittest.TestCase):
    def test_sqlite_atomic_claims_survive_new_connections(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'claims.db')
            with sqlite3.connect(path) as conn:
                conn.execute('CREATE TABLE bot_settings(key TEXT PRIMARY KEY, value TEXT)')
            with patch.object(main, 'init_db'), patch.object(main, 'use_postgres', return_value=False), patch.object(main, 'DB_PATH', path):
                assert_claims(self)
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute('SELECT count(*) FROM bot_settings').fetchone()[0], 1)

    @unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
    def test_postgres_atomic_claims_survive_new_connections(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        source = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(source)
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or not (info.get('dbname', '').startswith('test_') or info.get('dbname', '').endswith('_test')):
            raise ValueError('Explicit local test database required')
        name = 'test_watch_claim_' + uuid4().hex
        with psycopg.connect(source, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
            try:
                dsn = make_conninfo(source, dbname=name)
                with psycopg.connect(dsn) as conn:
                    conn.execute('CREATE TABLE bot_settings(key TEXT PRIMARY KEY, value TEXT)')
                with patch.object(main, 'init_db'), patch.object(main, 'DATABASE_URL', dsn):
                    assert_claims(self)
                with psycopg.connect(dsn) as conn:
                    self.assertEqual(conn.execute('SELECT count(*) FROM bot_settings').fetchone()[0], 1)
            finally:
                admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_delivery_preserves_scan_and_reports_precise_failure(self):
        async def collect(**kwargs):
            result = {}
            await kwargs['prepare_watch']([], result)
            return [], result

        async def deliver(*args, **kwargs):
            main.WATCH_RUNTIME['last_delivery_errors'].append({'symbol': 'SOL', 'error_type': 'TimedOut', 'delivery_status': 'UNKNOWN'})
            return 2

        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        with patch.object(main, 'WATCH_RUNTIME', {}), \
             patch.object(main, '_get_scrape_lock', return_value=asyncio.Lock()), \
             patch.object(main, 'collect_live_rows_for_watch', collect), \
             patch.object(main, '_ensure_watch_derivatives_ready', AsyncMock(return_value={'core_ready': True})), \
             patch.object(main.market_confidence_engine, 'capture_snapshot', return_value={}), \
             patch.object(main.research_watch_score_capture, 'prepare', return_value=([], {}, {})), \
             patch.object(main.research_watch_score_capture, 'build_bundle', return_value={}), \
             patch.object(main, '_send_magnet_watch_reports', deliver):
            result = await main.run_watch_cycle(bot, 1, general_enabled=False)
            self.assertTrue(result['ok'])
            self.assertEqual(result['magnet_sent'], 2)
            self.assertEqual(result['delivery_errors'][0]['symbol'], 'SOL')
            self.assertEqual(main.WATCH_RUNTIME['last_cycle_result'], 'completed_with_delivery_errors')
            self.assertIsNone(main.WATCH_RUNTIME['cycle_stage'])
            bot.bot.send_message.assert_awaited_once()
            self.assertIn('SOL', bot.bot.send_message.call_args.kwargs['text'])
            self.assertNotIn('❌', bot.bot.send_message.call_args.kwargs['text'])

    async def test_timeout_or_rejection_continues_other_symbols_without_retry(self):
        for failure, expected_status in ((TimedOut(), 'UNKNOWN'), (BadRequest('fixture rejection'), 'DELIVERY_FAILED')):
            sent = []
            watches = {s: {'chat_id': 1} for s in ('BTC', 'SOL', 'ETH')}

            async def send(**kwargs):
                sent.append(kwargs['text'])
                if kwargs['text'] == 'report SOL':
                    raise failure

            async def report(symbol, *args, **kwargs):
                return ['report ' + symbol]

            with patch.object(main, 'MAGNET_V1_WATCHES', watches), \
                 patch.object(main, 'WATCH_RUNTIME', {}), \
                 patch.object(main, '_build_magnet_report', report), \
                 patch.object(main.research_event_runtime, 'capture_magnet_watch_symbol') as capture:
                result = await main._send_magnet_watch_reports(
                    SimpleNamespace(bot=SimpleNamespace(send_message=send)), [], {'generation': 'fixture'}, 1, {})
                self.assertEqual(result, 2)
                self.assertEqual(sent.count('report SOL'), 1)
                self.assertIn('report ETH', sent)
                self.assertNotIn('last_generation', watches['SOL'])
                self.assertEqual(watches['ETH']['last_generation'], 'fixture')
                self.assertEqual(main.WATCH_RUNTIME['last_delivery_errors'][0]['delivery_status'], expected_status)
                self.assertEqual([(c.args[0], c.kwargs['delivery_status']) for c in capture.call_args_list],
                                 [('BTC', 'DELIVERED'), ('SOL', expected_status), ('ETH', 'DELIVERED')])

    async def test_shutdown_cancellation_is_not_swallowed(self):
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock(side_effect=asyncio.CancelledError)))
        with patch.object(main, 'MAGNET_V1_WATCHES', {'BTC': {'chat_id': 1}}), \
             patch.object(main.research_event_runtime, 'capture_magnet_watch_symbol') as capture:
            with self.assertRaises(asyncio.CancelledError):
                await main._send_magnet_watch_reports(bot, [], {}, 1, {})
            capture.assert_not_called()


if __name__ == '__main__':
    unittest.main()
