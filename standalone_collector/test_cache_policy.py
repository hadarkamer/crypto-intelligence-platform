"""Offline cache/HTTP fixtures; no CoinGlass, model, production DB or sheet writes."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock
from uuid import uuid4
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE/'runtime'))
import collection_bridge as bridge
from model1_cache_policy import latest_ready, capture_limit_error, error_response

TOKEN = 'synthetic-test-bridge-token-with-no-real-access'
NOW = datetime(2026, 9, 15, 20, 38, tzinfo=timezone.utc)


def row(age=12, timeframe='12H'):
    jid = str(uuid4())
    return {'id': jid, 'timeframe': timeframe, 'status': 'ready', 'error_code': None,
            'result': {'captured_at': (NOW-timedelta(minutes=age)).isoformat(), 'run_id': jid}}


class Connection:
    """SQL-observing fixture for start's cache-before-paid-work ordering."""
    def __init__(self, cached=None, existing=None, captures=2):
        self.cached, self.existing, self.captures = cached, existing, captures
        self.commands, self.new_jobs, self.new_requests = [], 0, 0
        self.answer = None
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, args=None):
        self.commands.append((sql, args))
        self.answer = None
        if 'requested_timeframe' in sql:
            self.answer = self.existing
        elif 'SELECT count(*)' in sql:
            self.answer = {'n': 0 if 'bridge_requests' in sql else self.captures}
        elif 'SELECT *' in sql and 'WHERE timeframe=' in sql:
            if self.cached and self.cached['timeframe'] == args[0]:
                age = (NOW-datetime.fromisoformat(self.cached['result']['captured_at'])).total_seconds()
                self.answer = self.cached if 0 <= age <= 900 else None
        elif "created_at + interval '1 hour'" in sql:
            self.answer = {'retry_at': NOW+timedelta(minutes=36), 'checked_at': NOW}
        elif 'INSERT INTO public.ai_collection_bridge_jobs' in sql:
            self.new_jobs += 1
            self.answer = {'id': args[0], 'timeframe': args[1], 'status': 'queued', 'result': None}
        elif 'INSERT INTO public.ai_collection_bridge_requests' in sql:
            self.new_requests += 1
        return self
    def fetchone(self): return self.answer


def store_with(connection):
    store = bridge.JobStore('synthetic-not-a-dsn', hourly_limit=2)
    store.hourly_limit = 2
    store.connect = Mock(return_value=connection)
    return store


class StoreTests(unittest.TestCase):
    def test_12minute_result_reused_before_capture_budget(self):
        saved = row(12)
        c = Connection(cached=saved)
        result = store_with(c).start(str(uuid4()), '12H')
        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['job_id'], saved['id'])
        self.assertEqual(result['result']['captured_at'], saved['result']['captured_at'])
        self.assertEqual(c.new_jobs, 0)
        self.assertEqual(c.new_requests, 1)
        queries = '\n'.join(sql for sql, _ in c.commands)
        self.assertIn("interval '15 minutes'", queries)
        self.assertIn("(result->>'captured_at')::timestamptz <= now()", queries)
        self.assertNotIn('Hourly capture', queries)
        self.assertFalse(any('SELECT count(*)' in sql and 'bridge_jobs' in sql for sql, _ in c.commands))
    def test_stale_result_does_not_bypass_automatic_limit(self):
        c = Connection(cached=row(16))
        with self.assertRaises(bridge.BridgeError) as ctx:
            store_with(c).start(str(uuid4()), '12H')
        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(c.new_jobs, 0)
        self.assertEqual(c.new_requests, 0)
        self.assertEqual(ctx.exception.retry_after_seconds, 2161)
    def test_other_timeframe_is_not_reused(self):
        c = Connection(cached=row(1, '48H'))
        with self.assertRaises(bridge.BridgeError):
            store_with(c).start(str(uuid4()), '12H')
        self.assertEqual(c.new_jobs, 0)
    def test_known_request_reuses_original_even_after_cache_expiry(self):
        saved = row(30)
        c = Connection(existing={**saved, 'requested_timeframe': '12H'})
        result = store_with(c).start(str(uuid4()), '12H')
        self.assertEqual(result['job_id'], saved['id'])
        self.assertEqual(c.new_jobs, 0)
        self.assertEqual(c.new_requests, 0)
    def test_idempotency_timeframe_conflict_preserved(self):
        c = Connection(existing={**row(1), 'requested_timeframe': '12H'})
        with self.assertRaises(bridge.BridgeError) as ctx:
            store_with(c).start(str(uuid4()), '48H')
        self.assertEqual(ctx.exception.status, 409)
    def test_retry_has_db_time_and_correct_order_for_reduced_limit(self):
        c = Connection()
        err = capture_limit_error(c, 2)
        self.assertEqual(c.commands[0][1], (1,))
        self.assertIn('ORDER BY created_at DESC', c.commands[0][0])
        body, headers = error_response(err)
        self.assertEqual(headers['Retry-After'], '2161')
        self.assertEqual(body['error']['retry_at'], '2026-09-15T21:14:01Z')
        self.assertEqual(body['error']['code'], 'rate_limited')
    def test_no_retry_timestamp_is_invented_if_missing(self):
        err = bridge.BridgeError('rate_limited', 'Request limit', 429)
        body, headers = error_response(err)
        self.assertNotIn('retry_at', body['error'])
        self.assertNotIn('Retry-After', headers)
    def test_latest_lookup_is_select_only_and_preserves_timestamp(self):
        saved = row(50)
        c = Mock()
        c.__enter__ = Mock(return_value=c)
        c.__exit__ = Mock(return_value=False)
        c.execute.return_value.fetchone.return_value = saved
        result = latest_ready(store_with(c), '12H')
        self.assertEqual(result['result'], saved['result'])
        sql, args = c.execute.call_args.args
        self.assertEqual(args, ('12H',))
        self.assertTrue(sql.strip().startswith('SELECT'))
        self.assertIn("interval '6 hours'", sql)
        self.assertNotIn('INSERT', sql)
        c.execute.assert_called_once()


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.saved = row(40)
        self.store = Mock()
        self.store.latest.return_value = bridge.envelope(self.saved)
        self.store.start.side_effect = capture_limit_error(Connection(), 2)
        app = web.Application()
        bridge.register_collection_routes(app, store=self.store, token=TOKEN, enabled=True, start_worker=False)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        self.headers = {'Authorization': 'Bearer '+TOKEN}
    async def asyncTearDown(self): await self.client.close()
    async def test_latest_requires_existing_authorization(self):
        response = await self.client.get('/api/collection/model1/jobs/latest?timeframe=12H')
        self.assertEqual(response.status, 401)
        self.store.latest.assert_not_called()
    async def test_latest_works_without_a_paid_job(self):
        response = await self.client.get('/api/collection/model1/jobs/latest?timeframe=12H', headers=self.headers)
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assertEqual(data['job_id'], self.saved['id'])
        self.assertEqual(data['result']['captured_at'], self.saved['result']['captured_at'])
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.store.start.assert_not_called()
        self.store.claim.assert_not_called()
    async def test_unknown_timeframe_rejected_without_lookup(self):
        response = await self.client.get('/api/collection/model1/jobs/latest?timeframe=72H', headers=self.headers)
        self.assertEqual(response.status, 400)
        self.store.latest.assert_not_called()
    async def test_no_saved_data_is_404_not_start(self):
        self.store.latest.return_value = None
        response = await self.client.get('/api/collection/model1/jobs/latest?timeframe=48H', headers=self.headers)
        self.assertEqual(response.status, 404)
        self.assertEqual((await response.json())['error']['code'], 'no_cached_result')
        self.store.start.assert_not_called()
    async def test_rate_limit_http_contains_computed_retry_after(self):
        response = await self.client.post('/api/collection/model1/jobs', headers=self.headers,
            json={'request_id': str(uuid4()), 'timeframe': '12H'})
        self.assertEqual(response.status, 429)
        self.assertEqual(response.headers['Retry-After'], '2161')
        self.assertEqual((await response.json())['error']['retry_at'], '2026-09-15T21:14:01Z')

if __name__ == '__main__': unittest.main()
