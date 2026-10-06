"""Transaction isolation and fail-closed socket reuse; no exchange requests."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
import os
import unittest
from unittest.mock import Mock, patch

from .postgres_journal import PostgresJournal, JournalError
from . import filled_quantity_dispatch as dispatch, request_budget as budget
from .test_transport_unsent import plan, request
from .test_filled_quantity_dispatch import ROUTES2, Venue


class Connection:
    def __init__(self):
        self.active=False;self.commits=0;self.checks=0;self.closed=False;self.fail_commit=False
    @contextmanager
    def transaction(self):
        if self.active:raise AssertionError('NESTED_TRANSACTION')
        self.active=True
        try:
            yield
            if self.fail_commit:raise RuntimeError('PRIVATE_DATABASE_DETAIL')
            self.commits+=1
        finally:self.active=False
    def execute(self,sql):
        assert self.active and sql=='SELECT current_database()'
        self.checks+=1
        return Mock(fetchone=lambda:('hl_journal_ci',))
    def close(self):self.closed=True


class ScopeTests(unittest.TestCase):
    def journal(self):return PostgresJournal.for_ci('postgresql://fake@localhost/hl_journal_ci')

    def test_each_transaction_commits_and_rechecks_database_on_one_socket(self):
        journal=self.journal();conn=Connection()
        with patch.object(journal,'_connect',return_value=conn) as connect:
            with journal.reuse_connection():
                for _ in range(3):
                    with journal._transaction() as actual:self.assertIs(actual,conn)
                    self.assertFalse(conn.active)
                self.assertFalse(conn.closed)
            connect.assert_called_once_with(autocommit=True)
        self.assertEqual((conn.commits,conn.checks),(3,3));self.assertTrue(conn.closed)

    def test_uncertain_commit_poison_prevents_retry_or_follow_on_transaction(self):
        journal=self.journal();conn=Connection();conn.fail_commit=True
        with patch.object(journal,'_connect',return_value=conn) as connect:
            with journal.reuse_connection():
                with self.assertRaisesRegex(JournalError,'PERSISTENCE_UNAVAILABLE_NO_SEND'):
                    with journal._transaction():pass
                with self.assertRaisesRegex(JournalError,'PERSISTENCE_UNAVAILABLE_NO_SEND'):
                    with journal._transaction():self.fail('Cannot continue after uncertain COMMIT')
            connect.assert_called_once()
        self.assertEqual(conn.checks,1);self.assertTrue(conn.closed)

    def test_context_copy_to_other_thread_never_shares_the_socket(self):
        journal=self.journal();parent=Connection();child=Connection()
        with patch.object(journal,'_connect',side_effect=[parent,child]):
            with journal.reuse_connection():
                def worker():
                    with journal.reuse_connection():
                        with journal._transaction() as actual:self.assertIs(actual,child)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pool.submit(copy_context().run,worker).result()
                with journal._transaction() as actual:self.assertIs(actual,parent)
                self.assertFalse(parent.closed);self.assertTrue(child.closed)

    def test_wrong_database_and_changed_process_never_yield_a_connection(self):
        for mode in ('database','process'):
            journal=self.journal();conn=Connection()
            if mode=='database':conn.execute=lambda sql:Mock(fetchone=lambda:('other',))
            with patch.object(journal,'_connect',return_value=conn):
                with journal.reuse_connection():
                    with patch('os.getpid',return_value=os.getpid()+1) if mode=='process' else patch('os.getpid',wraps=os.getpid):
                        with self.assertRaises(JournalError):
                            with journal._transaction():self.fail('Untrusted connection yielded')

    def test_dispatch_warms_before_permit_and_releases_transactions_before_transport(self):
        journal=self.journal();conn=Connection();clock=[0]
        store=Mock(domain='software',journal=journal)
        venue=Venue();controller=dispatch.Controller(store,venue,ROUTES2)
        state,proposal=plan();record=request(proposal);route=ROUTES2[proposal['role']]
        def connect(**kwargs):clock[0]+=2_000_000_000;return conn
        def transaction():
            with journal._transaction():pass
            self.assertFalse(conn.active)
        def prepare(*args,**kwargs):
            transaction()
            permit=budget.Permit(Mock(),'a'*32,'exchange',1,clock[0]+1_000_000_000)
            admission=dispatch.TransportAdmission(proposal,permit);admission.bind(record)
            transaction();transaction()
            return dispatch.AdmittedAttempt((state,record,proposal,route),admission)
        def send(venue,request,admission):
            transaction();transaction()
            admission.consume(request)
            self.assertFalse(conn.active)
            return {}
        with patch.object(journal,'_connect',side_effect=connect) as dial, \
             patch.object(controller,'_prepare_cycle',side_effect=prepare), \
             patch.object(dispatch,'send_admitted',side_effect=send), \
             patch.object(dispatch,'normalized_reply',return_value={'state':'OUTCOME_UNKNOWN'}), \
             patch('time.monotonic_ns',side_effect=lambda:clock[0]):
            result=controller.cycle(state['bucket'],send=True)
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN')
        self.assertEqual((dial.call_count,conn.commits),(1,5))
        self.assertTrue(conn.closed)


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Disposable PostgreSQL required')
class PostgresScopeTests(unittest.TestCase):
    def test_commits_visible_from_independent_connection_between_scoped_transactions(self):
        journal=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        other=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        with patch.object(journal,'_connect',wraps=journal._connect) as connect:
            with journal.reuse_connection():
                with journal._transaction() as conn:
                    conn.execute('CREATE TEMP TABLE lease_proof(value integer)')
                    conn.execute('INSERT INTO lease_proof VALUES(7)')
                    pid=conn.execute('SELECT pg_backend_pid()').fetchone()[0]
                with other._transaction() as independent:
                    self.assertNotEqual(independent.execute('SELECT pg_backend_pid()').fetchone()[0],pid)
                    self.assertEqual(independent.execute('SELECT state FROM pg_stat_activity WHERE pid=%s',(pid,)).fetchone()[0],'idle')
                with journal._transaction() as conn:
                    self.assertEqual(conn.execute('SELECT value FROM lease_proof').fetchone()[0],7)
            self.assertEqual(connect.call_count,1)
