"""Real PostgreSQL integration tests; synthetic work, no scanner/AI imports.

Run after standalone_collector/prepare.py, against the disposable CI service.
No fallback to production credentials and no skipped tests on missing database.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import sys
from threading import Barrier
import unittest
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'standalone_collector/runtime'),
                str(ROOT / 'standalone_collector'), str(ROOT / 'experiments')]
from collection_bridge import JobStore
from work_identity import WorkSpec, build_batch
from work_queue_candidate import WorkQueue

DSN = 'host=127.0.0.1 port=5432 dbname=coinglass_queue_test user=queue_test password=local-test-only'
TENANT, SOURCE, BATCH = [str(uuid4()) for _ in range(3)]


class QueuePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.queue = WorkQueue(JobStore(DSN))
        cls.queue.initialize()
        with cls.queue.connect() as c:
            print('DATABASE_EVIDENCE', c.execute('SELECT version() AS version').fetchone(), flush=True)

    def setUp(self):
        with self.queue.connect() as c:
            c.execute('TRUNCATE public.ai_collection_bridge_requests, public.ai_collection_bridge_jobs')
        self.command = WorkSpec(TENANT, SOURCE, BATCH, 'TESTA', 1, '12H', 'a' * 64)

    def parallel(self, n, operation):
        barrier = Barrier(n)
        def invoke(i):
            # Hold distinct sessions across the barrier; the store operation
            # also opens its own independent transaction (no shared connection).
            with self.queue.connect() as c:
                pid = c.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
                barrier.wait(timeout=15)
                return pid, operation(i)
        with ThreadPoolExecutor(max_workers=n) as pool:
            pairs = list(pool.map(invoke, range(n)))
        self.assertEqual(len({p[0] for p in pairs}), n)
        return [p[1] for p in pairs]

    def expire(self, claim):
        # Deterministic server-side expiry, not a fake DB or an application clock.
        with self.queue.connect() as c:
            c.execute('''UPDATE public.ai_collection_bridge_jobs
                SET lease_until=clock_timestamp()-interval '1 second' WHERE id=%s''',
                (claim.command.work_id,))

    def count(self):
        with self.queue.connect() as c:
            return c.execute('SELECT count(*) AS n FROM public.ai_collection_bridge_jobs').fetchone()['n']

    def test_01_sixteen_simultaneous_duplicates_create_one_job(self):
        rows = self.parallel(16, lambda _: self.queue.enqueue(self.command))
        self.assertEqual({str(r['id']) for r in rows}, {self.command.work_id})
        self.assertEqual(self.count(), 1)
        print('SCENARIO_1 PASS: 16 concurrent requests, 1 persisted job', flush=True)

    def test_02_two_workers_one_job_one_owner(self):
        self.queue.enqueue(self.command)
        claims = self.parallel(2, lambda _: self.queue.claim(str(uuid4())))
        winners = [c for c in claims if c is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.queue.get(self.command.work_id)['generation'], 1)
        self.assertTrue(self.queue.finish(winners[0], {'synthetic': 'winner'}))
        print('SCENARIO_2 PASS: 2 concurrent workers, 1 owner', flush=True)

    def test_03_old_owner_cannot_renew_start_or_overwrite_new_owner(self):
        self.queue.enqueue(self.command)
        # Even reusing a worker UUID cannot reuse an earlier ownership generation.
        owner = str(uuid4())
        old = self.queue.claim(owner)
        self.expire(old)
        self.assertFalse(self.queue.renew(old))
        self.assertFalse(self.queue.finish(old, {'synthetic': 'expired'}))
        new = self.queue.claim(owner)
        self.assertEqual(new.generation, old.generation + 1)
        self.assertFalse(self.queue.renew(old))
        self.assertFalse(self.queue.begin_external(old))
        self.assertFalse(self.queue.finish(old, {'synthetic': 'old'}, b'old-image'))
        self.assertTrue(self.queue.finish(new, {'synthetic': 'new'}, b'new-image'))
        self.assertFalse(self.queue.finish(old, error='late-failure'))
        row = self.queue.get(self.command.work_id)
        self.assertEqual((row['status'], row['result'], bytes(row['image_png']), row['error_code']),
                         ('ready', {'synthetic': 'new'}, b'new-image', None))
        print('SCENARIO_3 PASS: stale writes rejected before/after replacement completion', flush=True)

    def test_two_workers_take_two_different_jobs(self):
        other = replace(self.command, timeframe='24H')
        for command in (self.command, other):
            self.queue.enqueue(command)
        claims = self.parallel(2, lambda _: self.queue.claim(str(uuid4())))
        self.assertEqual({c.command.work_id for c in claims}, {self.command.work_id, other.work_id})

    def test_locked_job_is_skipped_and_sibling_is_claimed(self):
        other = replace(self.command, coin='TESTB')
        for command in (self.command, other):
            self.queue.enqueue(command)
        with self.queue.connect() as locked:
            locked.execute('SELECT id FROM public.ai_collection_bridge_jobs WHERE id=%s FOR UPDATE',
                           (self.command.work_id,)).fetchone()
            claim = self.queue.claim(str(uuid4()))
            self.assertEqual(claim.command, other)
        self.assertEqual(self.queue.claim(str(uuid4())).command, self.command)

    def test_72_distinct_jobs_and_new_batch_not_deduplicated(self):
        commands = build_batch(tenant_id=TENANT, source_scope_id=SOURCE, batch_id=BATCH,
                               coins=tuple('TEST' + c for c in 'ABCDEFGH'), config_digest='a' * 64)
        for command in commands:
            self.queue.enqueue(command)
            self.queue.enqueue(command)
        self.assertEqual(self.count(), 72)
        self.queue.enqueue(replace(commands[0], batch_id=str(uuid4())))
        self.queue.enqueue(replace(commands[0], tenant_id=str(uuid4())))
        self.queue.enqueue(replace(commands[0], source_scope_id=str(uuid4())))
        self.assertEqual(self.count(), 75)

    def test_conflicting_config_rejected_without_resetting_job(self):
        self.queue.enqueue(self.command)
        claim = self.queue.claim(str(uuid4()))
        self.assertTrue(self.queue.finish(claim, {'synthetic': 'saved'}))
        with self.assertRaises(ValueError):
            self.queue.enqueue(replace(self.command, config_digest='b' * 64))
        duplicate = self.queue.enqueue(self.command)
        self.assertEqual((duplicate['status'], duplicate['result']), ('ready', {'synthetic': 'saved'}))
        self.assertEqual(self.count(), 1)

    def test_external_reservation_once_and_unknown_outcome_not_reclaimed(self):
        self.queue.enqueue(self.command)
        claim = self.queue.claim(str(uuid4()))
        decisions = self.parallel(2, lambda _: self.queue.begin_external(claim))
        self.assertEqual(sorted(decisions), [False, True])
        self.expire(claim)
        self.assertIsNone(self.queue.claim(str(uuid4())))
        self.assertFalse(self.queue.finish(claim, {'synthetic': 'late'}))
        self.assertTrue(self.queue.get(self.command.work_id)['external_started'])

    def test_failure_is_local_and_successful_sibling_remains_saved(self):
        commands = [replace(self.command, timeframe=tf) for tf in ('12H', '24H', '48H')]
        for command in commands:
            self.queue.enqueue(command)
        claims = [self.queue.claim(str(uuid4())) for _ in commands]
        for i, claim in enumerate(claims):
            self.assertTrue(self.queue.finish(claim, {'synthetic': i}, error='synthetic-failure' if i == 1 else None))
        self.assertEqual([self.queue.get(c.command.work_id)['status'] for c in claims],
                         ['ready', 'failed', 'ready'])
        self.assertIsNone(self.queue.claim(str(uuid4())))

    def test_renewal_and_wrong_owner_guard(self):
        self.queue.enqueue(self.command)
        claim = self.queue.claim(str(uuid4()))
        self.assertTrue(self.queue.renew(claim))
        wrong = replace(claim, owner=str(uuid4()))
        self.assertFalse(self.queue.renew(wrong))
        self.assertFalse(self.queue.finish(wrong, {'synthetic': 'wrong'}))
        self.assertTrue(self.queue.finish(claim, {'synthetic': 'right'}))
        self.assertFalse(self.queue.finish(claim, {'synthetic': 'duplicate'}))

    def test_real_disconnect_rolls_back_uncommitted_claim(self):
        self.queue.enqueue(self.command)
        c = self.queue.connect()
        try:
            c.execute("UPDATE public.ai_collection_bridge_jobs SET status='running' WHERE id=%s",
                      (self.command.work_id,))
        finally:
            c.close()  # server rolls back; no application retry logic involved
        self.assertEqual(self.queue.claim(str(uuid4())).generation, 1)

    def test_guard_rejects_non_fixture_database(self):
        for dsn in ('host=production.invalid dbname=prod user=owner', '', 'dbname=postgres'):
            with self.assertRaises(ValueError):
                WorkQueue(JobStore(dsn))


if __name__ == '__main__':
    unittest.main(verbosity=2)
