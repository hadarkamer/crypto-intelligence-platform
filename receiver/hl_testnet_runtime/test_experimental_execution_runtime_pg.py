"""Candidate worker real-PG integration, never substituted by SQLite results."""
from concurrent.futures import ThreadPoolExecutor
import os
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import PostgresExecutionState, PG_SCHEMA
from .postgres_journal import PostgresJournal
from .test_experimental_execution_runtime import SoftwareExchange, ROUTES, T

CI=os.environ.get('HL_JOURNAL_CI_URL')


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
        def run(worker):
            try:return worker.run_once()
            except runtime.RuntimeError as exc:return str(exc)
        with ThreadPoolExecutor(2) as pool:list(pool.map(run,[self.worker,other]))
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
