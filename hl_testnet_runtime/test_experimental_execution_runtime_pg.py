"""Candidate worker real-PG integration, never substituted by SQLite results."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from copy import deepcopy
import threading
import os
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import PostgresExecutionState, PG_SCHEMA, initial
from .postgres_journal import PostgresJournal, JournalError
from . import card_lifecycle as life
from .test_experimental_execution_runtime import SoftwareExchange, ROUTES, T

CI=os.environ.get('HL_JOURNAL_CI_URL')


class JournalDomainErrorTests(unittest.TestCase):
    """Exercise real journal exception policy; only its connection is simulated.

    This is not PostgreSQL evidence. It verifies that controlled domain rejections
    survive rollback while unknown database/commit failures stay fail-closed.
    """
    class Connection:
        def __init__(self, *, fail_commit=False):
            self.value=initial(ROUTES,T-60000)
            self.fail_commit=fail_commit
            self.writes=0
            self.rolled_back=False
            self.closed=False
        def __enter__(self):return self
        def __exit__(self,typ,value,traceback):
            if typ is not None:self.rolled_back=True
            elif self.fail_commit:raise RuntimeError('SIMULATED_PRIVATE_COMMIT_DETAILS')
            return False
        def close(self):self.closed=True
        def transaction(self):return self
        def execute(self,query,args=None):
            class Cursor:
                def __init__(self,row):self.row=row
                def fetchone(self):return self.row
            if query=='SELECT current_database()':return Cursor(('hl_journal_ci',))
            if query.startswith('SELECT pg_advisory_xact_lock'):return Cursor((None,))
            if query.startswith('SELECT value,checksum'):
                value=deepcopy(self.value)
                return Cursor((value,life.digest(value)))
            if query.startswith('UPDATE '):
                self.writes+=1
                return Cursor(None)
            raise AssertionError('UNEXPECTED_TEST_QUERY')

    def journal(self):
        return PostgresJournal.for_ci('postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci')

    def test_controlled_revision_rejection_survives_actual_journal_rollback(self):
        for leased in (False,True):
            journal=self.journal();conn=self.Connection()
            store=PostgresExecutionState(journal)
            rejection=runtime.RuntimeError('CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
            def reject(_state):raise rejection
            with self.subTest(leased=leased),patch.object(journal,'_connect',return_value=conn):
                with journal.reuse_connection() if leased else nullcontext():
                    with self.assertRaises(runtime.RuntimeError) as captured:store.mutate(reject)
                self.assertIs(captured.exception,rejection)
                self.assertEqual(str(captured.exception),'CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
                self.assertEqual(conn.writes,0)
                self.assertTrue(conn.rolled_back)

    def test_unexpected_callback_failure_keeps_generic_persistence_fence(self):
        journal=self.journal();conn=self.Connection();store=PostgresExecutionState(journal)
        def fail(_state):raise ValueError('SIMULATED_PRIVATE_CALLBACK_DETAILS')
        with patch.object(journal,'_connect',return_value=conn),self.assertRaises(JournalError) as captured:
            store.mutate(fail)
        self.assertIs(type(captured.exception),JournalError)
        self.assertEqual(str(captured.exception),'PERSISTENCE_UNAVAILABLE_NO_SEND')
        self.assertEqual(conn.writes,0)
        self.assertTrue(conn.rolled_back)

    def test_unknown_commit_still_raises_no_result_or_dispatch_authority(self):
        for leased in (False,True):
            journal=self.journal();conn=self.Connection(fail_commit=True);store=PostgresExecutionState(journal)
            with self.subTest(leased=leased),patch.object(journal,'_connect',return_value=conn):
                with journal.reuse_connection() if leased else nullcontext():
                    with self.assertRaises(JournalError) as captured:
                        store.mutate(lambda state:dict(unsigned_proposal='never-returned'))
                self.assertIs(type(captured.exception),JournalError)
                self.assertEqual(str(captured.exception),'PERSISTENCE_UNAVAILABLE_NO_SEND')
                self.assertEqual(conn.writes,1)

    def test_unknown_connect_failure_stays_redacted(self):
        journal=self.journal();store=PostgresExecutionState(journal)
        with patch.object(journal,'_connect',side_effect=OSError('SIMULATED_PRIVATE_CONNECTION_DETAILS')),self.assertRaises(JournalError) as captured:
            store.load()
        self.assertIs(type(captured.exception),JournalError)
        self.assertEqual(str(captured.exception),'PERSISTENCE_UNAVAILABLE_NO_SEND')


@unittest.skipUnless(CI,'Requires disposable loopback PostgreSQL; SQLite is not PostgreSQL evidence')
class ExperimentalWorkerPostgresTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.create_connection','hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body'):
            p=patch(target,side_effect=AssertionError('NO_EXCHANGE_IO'));p.start();self.addCleanup(p.stop)
        self.journal=PostgresJournal.for_ci(CI)
        self.journal.bootstrap()
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {PG_SCHEMA} CASCADE')
        self.store=PostgresExecutionState(self.journal);self.store.initialize(ROUTES,not_before_ms=T-60000)
        self.venue=SoftwareExchange(T+10000)
        self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        self.msg=r2732_message(entry=2.3,decision_ms=T-60000)
        self.worker.receive([self.msg])

    def test_postgres_complete_entry_exits_finality_and_reopen(self):
        for _ in range(4):self.worker.run_once()
        trade=self.store.load()['trades'][self.msg['occurrence_id']]
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),trade['quantity'])
        for _ in range(3):self.worker.run_once()
        reopened=PostgresExecutionState(PostgresJournal.for_ci(CI))
        self.assertEqual(reopened.load()['trades'][self.msg['occurrence_id']]['phase'],'CLOSED')
        self.assertEqual(len(self.venue.requests),4)

    def test_postgres_cross_connection_entry_admission_is_atomic(self):
        other=runtime.IsolatedExecutionRuntime(PostgresExecutionState(PostgresJournal.for_ci(CI)),self.venue,mode=runtime.MODE)
        # Both workers must collect the same revision before either may enter
        # the SQL lock. One admits; one must report the exact reload fence.
        barrier=threading.Barrier(2)
        original_collect=self.venue.collect
        def collect(state):
            result=original_collect(state)
            barrier.wait(timeout=10)
            return result
        def run(worker):
            try:return worker.run_once()['status']
            except runtime.RuntimeError as exc:
                if str(exc)!='CONCURRENT_OBSERVATION_RELOAD_REQUIRED':raise
                return str(exc)
        with patch.object(self.venue,'collect',side_effect=collect):
            with ThreadPoolExecutor(2) as pool:outcomes=list(pool.map(run,[self.worker,other]))
        self.assertCountEqual(outcomes,['SOFTWARE_ATTEMPT_RECORDED_AWAITING_OBSERVATION',
                                       'CONCURRENT_OBSERVATION_RELOAD_REQUIRED'])
        entries=[r for r in self.store.load()['requests'].values() if r['proposal']['operation']=='ENTRY']
        self.assertEqual(len(entries),1)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)

    def test_postgres_attempt_commit_ack_loss_leaves_uncertain_fence(self):
        original=self.store.mutate
        def lose(fn):
            original(fn);raise RuntimeError('SIMULATED_COMMIT_REPLY_LOST')
        with patch.object(self.store,'mutate',side_effect=lose),self.assertRaisesRegex(RuntimeError,'COMMIT_REPLY_LOST'):
            self.worker.run_once()
        reopened=runtime.IsolatedExecutionRuntime(PostgresExecutionState(PostgresJournal.for_ci(CI)),self.venue,mode=runtime.MODE)
        reopened.run_once()
        self.assertEqual(len(self.store.load()['requests']),1);self.assertFalse(self.venue.requests)


if __name__=='__main__':unittest.main()
