"""Synthetic failure proofs plus separately gated disposable-PostgreSQL tests."""
from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from . import experimental_runtime_owner as owner
from .postgres_journal import PostgresJournal, STAGING_DB, STAGING_HOST

CI = os.environ.get('HL_JOURNAL_CI_URL')
FAKE_CI = 'postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci'


class Connection:
    def __init__(self):
        self.database, self.backend = 'hl_journal_ci', 123
        self.locked, self.busy = False, False
        self.queries = []
        self.closes = 0
        self.fail = None
        self.unlock_result = True

    def execute(self, query, params=None):
        self.queries.append((query, params))
        if self.fail is not None and self.fail in query:
            raise OSError('PRIVATE_DATABASE_PASSWORD')
        if query == 'SELECT current_database(), pg_backend_pid()':
            row = (self.database, self.backend)
        elif query.startswith('SELECT pg_try_advisory_lock'):
            self.locked = not self.busy
            row = (self.locked,)
        elif query == owner._VERIFY:
            row = (self.backend, self.locked)
        elif query.startswith('SELECT pg_advisory_unlock'):
            row = (self.unlock_result,)
            self.locked = False
        else:
            raise AssertionError('Unexpected SQL')
        class Cursor:
            def fetchone(self): return row
        return Cursor()

    def close(self):
        self.closes += 1
        self.locked = False
        if self.fail == 'close':
            raise OSError('PRIVATE_CLOSE_DETAIL')


class OwnerUnitTests(unittest.TestCase):
    def setUp(self):
        self.journal = PostgresJournal.for_ci(FAKE_CI)
        self.lease = owner.ProcessLease.for_ci(self.journal)
        self.connection = Connection()
        self.patch = patch.object(self.journal, '_connect', return_value=self.connection)
        self.connect = self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_construction_is_offline_and_exact_domain_required(self):
        self.connect.assert_not_called()
        with self.assertRaisesRegex(owner.OwnerError, 'EXACT_TESTNET'):
            owner.ProcessLease(self.journal)
        with self.assertRaisesRegex(owner.OwnerError, 'EXACT_LOOPBACK'):
            owner.ProcessLease.for_ci(object())
        production_shape = PostgresJournal(dict(host=STAGING_HOST, dbname=STAGING_DB))
        with patch.object(production_shape, '_connect', side_effect=AssertionError('No IO')):
            self.assertEqual(owner.ProcessLease(production_shape).health()['status'], 'NOT_ACQUIRED')

    def test_acquire_verify_do_not_stack_session_locks(self):
        self.assertTrue(self.lease.acquire())
        for _ in range(3):
            self.assertTrue(self.lease.acquire())
            self.assertTrue(self.lease.verify())
        self.assertEqual(sum('pg_try_advisory_lock' in q for q,_ in self.connection.queries), 1)
        self.connect.assert_called_once_with(autocommit=True)
        self.assertFalse(any(token in q for q,_ in self.connection.queries
                             for token in ('CREATE ', 'ALTER ', 'INSERT ', 'UPDATE ', 'DELETE ')))

    def test_confirmed_never_held_contention_allows_later_explicit_attempt(self):
        self.connection.busy = True
        with self.assertRaisesRegex(owner.OwnerError, 'ALREADY_HELD'):
            self.lease.acquire()
        self.assertEqual(self.connection.closes, 1)
        self.assertEqual(self.lease.health()['status'], 'CONTENDED')
        self.connect.assert_called_once()
        self.connect.return_value = Connection()
        self.assertTrue(self.lease.acquire())
        self.assertEqual(self.connect.call_count, 2)

    def test_contention_with_failed_close_cannot_retry(self):
        self.connection.busy = True; self.connection.fail = 'close'
        with self.assertRaisesRegex(owner.OwnerError, 'SESSION_LOST'):
            self.lease.acquire()
        with self.assertRaisesRegex(owner.OwnerError, 'REACQUIRE_REFUSED'):
            self.lease.acquire()
        self.connect.assert_called_once()

    def test_connection_failure_is_redacted_and_permanently_fenced(self):
        self.connect.side_effect = OSError('PRIVATE_DATABASE_PASSWORD')
        with self.assertRaisesRegex(owner.OwnerError, '^EXPERIMENTAL_OWNER_SESSION_LOST$'):
            self.lease.acquire()
        with self.assertRaisesRegex(owner.OwnerError, 'REACQUIRE_REFUSED'):
            self.lease.acquire()
        self.connect.assert_called_once()

    def test_wrong_database_identity_refused_before_lock(self):
        self.connection.database = 'other_database'
        with self.assertRaisesRegex(owner.OwnerError, 'DATABASE_IDENTITY_INVALID'):
            self.lease.acquire()
        self.assertEqual(len(self.connection.queries), 1)
        self.assertEqual(self.connection.closes, 1)

    def test_unknown_lock_reply_closes_session_without_retry(self):
        self.connection.fail = 'pg_try_advisory_lock'
        with self.assertRaisesRegex(owner.OwnerError, 'SESSION_LOST'):
            self.lease.acquire()
        self.assertEqual(self.connection.closes, 1)
        self.assertFalse(self.lease.release())

    def test_verify_lost_lock_or_replaced_backend_poison_owner(self):
        for change in ('lock', 'backend'):
            with self.subTest(change=change):
                self.lease = owner.ProcessLease.for_ci(self.journal)
                self.connection = Connection()
                self.connect.return_value = self.connection
                self.lease.acquire()
                if change == 'lock': self.connection.locked = False
                else: self.connection.backend += 1
                with self.assertRaisesRegex(owner.OwnerError, 'SESSION_LOST'):
                    self.lease.verify()
                self.assertEqual(self.connection.closes, 1)
                with self.assertRaisesRegex(owner.OwnerError, 'NOT_HELD'):
                    self.lease.verify()

    def test_verify_connection_failure_never_reconnects(self):
        self.lease.acquire()
        self.connection.fail = 'pg_locks'
        with self.assertRaisesRegex(owner.OwnerError, 'SESSION_LOST'):
            self.lease.verify()
        self.assertFalse(self.lease.health()['held'])
        self.connect.assert_called_once()

    def test_inherited_process_cannot_query_unlock_or_close_parent_session(self):
        self.lease.acquire()
        before = len(self.connection.queries)
        with patch.object(owner.os, 'getpid', return_value=self.lease._pid + 1):
            with self.assertRaisesRegex(owner.OwnerError, 'FOREIGN_PROCESS'):
                self.lease.verify()
            self.assertFalse(self.lease.release())
            self.assertFalse(self.lease.health()['held'])
        self.assertEqual(len(self.connection.queries), before)
        self.assertEqual(self.connection.closes, 0)

    def test_release_is_confirmed_idempotent_and_forbids_reacquisition(self):
        self.lease.acquire()
        self.assertTrue(self.lease.release())
        queries = len(self.connection.queries)
        self.assertTrue(self.lease.release())
        self.assertEqual(len(self.connection.queries), queries)
        self.assertEqual(self.connection.closes, 1)
        with self.assertRaisesRegex(owner.OwnerError, 'REACQUIRE_REFUSED'):
            self.lease.acquire()

    def test_unknown_unlock_or_close_never_reports_success(self):
        for failure in ('pg_advisory_unlock', 'close', 'negative_reply'):
            with self.subTest(failure=failure):
                self.lease = owner.ProcessLease.for_ci(self.journal)
                self.connection = Connection()
                self.connect.return_value = self.connection
                self.lease.acquire()
                if failure == 'negative_reply': self.connection.unlock_result = False
                else: self.connection.fail = failure
                self.assertFalse(self.lease.release())
                self.assertEqual(self.connection.closes, 1)
                self.assertFalse(self.lease.release())
                with self.assertRaisesRegex(owner.OwnerError, 'NOT_HELD'):
                    self.lease.verify()

    def test_verify_before_start_cannot_claim_ownership(self):
        with self.assertRaisesRegex(owner.OwnerError, 'NOT_HELD'):
            self.lease.verify()
        self.connect.assert_not_called()
        self.assertTrue(self.lease.release())


@unittest.skipUnless(CI, 'Requires fresh disposable loopback PostgreSQL')
class OwnerPostgresTests(unittest.TestCase):
    def new_owner(self):
        lease = owner.ProcessLease.for_ci(PostgresJournal.for_ci(CI))
        self.addCleanup(lease.release)
        return lease

    def test_two_connections_cannot_own_same_candidate_runtime(self):
        first, second = self.new_owner(), self.new_owner()
        self.assertTrue(first.acquire())
        with self.assertRaisesRegex(owner.OwnerError, 'ALREADY_HELD'):
            second.acquire()
        self.assertTrue(first.verify())
        self.assertTrue(first.release())
        self.assertTrue(second.acquire())

    def test_verification_does_not_increment_postgres_reentrant_lock_count(self):
        first = self.new_owner()
        first.acquire()
        for _ in range(5):
            first.acquire(); first.verify()
        self.assertTrue(first.release())
        self.assertTrue(self.new_owner().acquire())

    def test_concurrent_processes_are_fenced_and_clean_release_allows_successor(self):
        first = self.new_owner()
        first.acquire()
        program = '''import os,sys
from hl_testnet_runtime.experimental_runtime_owner import ProcessLease,OwnerError
from hl_testnet_runtime.postgres_journal import PostgresJournal
p=ProcessLease.for_ci(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
try:
 p.acquire()
except OwnerError as e:
 assert str(e)=='EXPERIMENTAL_OWNER_ALREADY_HELD'
 assert sys.argv[1]=='blocked'
 print('BLOCKED')
else:
 assert sys.argv[1]=='acquire'
 assert p.verify() and p.release()
 print('ACQUIRED')
'''
        for expected in ('blocked', 'acquire'):
            if expected == 'acquire': self.assertTrue(first.release())
            result = subprocess.run([sys.executable, '-c', program, expected],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('BLOCKED' if expected == 'blocked' else 'ACQUIRED', result.stdout)

    def test_parallel_acquisition_yields_exactly_one_owner(self):
        participants = [self.new_owner() for _ in range(3)]
        gate = threading.Barrier(len(participants))
        def acquire(lease):
            gate.wait(timeout=5)
            try: return lease.acquire()
            except owner.OwnerError as error:
                self.assertEqual(str(error), 'EXPERIMENTAL_OWNER_ALREADY_HELD')
                return False
        with ThreadPoolExecutor(max_workers=3) as pool:
            self.assertEqual(sum(pool.map(acquire, participants)), 1)

    def test_external_unlock_is_detected_and_old_owner_never_reacquires(self):
        first = self.new_owner()
        first.acquire()
        self.assertTrue(first._connection.execute(
            'SELECT pg_advisory_unlock(%s::bigint)', (owner.LOCK,)).fetchone()[0])
        with self.assertRaisesRegex(owner.OwnerError, 'SESSION_LOST'):
            first.verify()
        with self.assertRaisesRegex(owner.OwnerError, 'REACQUIRE_REFUSED'):
            first.acquire()
        self.assertTrue(self.new_owner().acquire())

    def test_closed_backend_connection_fences_dispatch_proof(self):
        first = self.new_owner()
        first.acquire()
        first._connection.close()
        with self.assertRaisesRegex(owner.OwnerError, 'SESSION_LOST'):
            first.verify()
        self.assertFalse(first.health()['held'])
        self.assertTrue(self.new_owner().acquire())
