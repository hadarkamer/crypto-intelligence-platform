"""Card reads use real snapshots without borrowing execution writer locks.

SQLite WAL is enabled explicitly in its disposable fixture, not by the read
path. Default SQLite rollback mode still needs readers to finish before a
writer can commit. PostgreSQL cases exercise the actual Testnet store and are
skipped unless an explicitly disposable loopback CI database is supplied.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from copy import deepcopy
import os
import sqlite3
import threading
import unittest
from unittest.mock import patch

from . import card_lifecycle as life
from .experimental_execution_state import PostgresExecutionState, StateError, PG_SCHEMA, initial
from .postgres_journal import PostgresJournal, JournalError
from . import test_execution_history_lifecycle as sqlite_support
from . import test_execution_history_pg as pg_support
from .test_approved_alert_lifecycle import cancellation
from .test_experimental_execution_runtime import ROUTES, T

CI = os.environ.get('HL_JOURNAL_CI_URL')


class SnapshotCases:
    def hold_snapshot(self, entered, release):
        original = self.f.store._history_snapshot
        @contextmanager
        def held():
            with original() as (db, state):
                entered.set()
                if not release.wait(4):
                    raise AssertionError('READER_NOT_RELEASED')
                yield db, state
        return held

    def while_reader_held(self, read, write):
        entered, release = threading.Event(), threading.Event()
        with patch.object(self.f.store, '_history_snapshot',
                          self.hold_snapshot(entered, release)):
            with ThreadPoolExecutor(2) as pool:
                reader = pool.submit(read)
                try:
                    self.assertTrue(entered.wait(2), 'reader did not acquire snapshot')
                    written = pool.submit(write).result(timeout=2)
                finally:
                    release.set()
                return reader.result(timeout=3), written

    def journal_rows(self):
        with self.f.store._history_snapshot() as (db, state):
            return deepcopy(state), {
                name: db.execute(f'SELECT * FROM {db.table(name)} ORDER BY 1,2').fetchall()
                for name in ('history', 'history_updates', 'history_orders')}

    def test_read_does_not_write_journal_or_use_mutation_path(self):
        value = self.close_trade(); cid = value['occurrence_id']
        for archived in (False, True):
            if archived:
                self.compact()
                self.f.worker.receive([cancellation(value, self.now())])
            before = self.journal_rows()
            with patch.object(self.f.store, '_history_transaction',
                              side_effect=AssertionError('WRITER_PATH_USED')):
                result = self.f.store.trade_card(cid)
                self.assertEqual(result['location'], 'archived' if archived else 'active')
                self.assertEqual(result['card']['card_id'], cid)
                self.assertIsNone(self.f.store.trade_card('f'*64))
            self.assertEqual(self.journal_rows(), before)

    def test_archive_commits_during_active_reader_and_card_never_disappears(self):
        value = self.close_trade(); cid = value['occurrence_id']
        before = self.f.store.trade_card(cid)
        seen, written = self.while_reader_held(
            lambda: self.f.store.trade_card(cid), self.compact)
        self.assertEqual(written['archived'], 1)
        self.assertEqual(seen, before)
        after = self.f.store.trade_card(cid)
        self.assertEqual(after['location'], 'archived')
        self.assertEqual(after['card'], before['card'])
        state = self.f.store.load()
        self.assertNotIn(cid, state['trades'])
        self.assertEqual(state['history']['archived_trades'], 1)
        self.assertEqual(len(self.f.store.history_page()['records']), 1)

    def test_late_source_update_commits_but_reader_keeps_one_snapshot(self):
        value = self.close_trade(); cid = value['occurrence_id']
        self.compact()
        before = self.f.store.trade_card(cid)
        cancel = cancellation(value, self.now())
        seen, _ = self.while_reader_held(
            lambda: self.f.store.trade_card(cid),
            lambda: self.f.worker.receive([cancel]))
        self.assertEqual(seen, before)
        after = self.f.store.trade_card(cid)
        self.assertEqual(after['card'], before['card'])
        self.assertEqual(after['current_source']['cancellation'], cancel)
        self.assertEqual(len(after['source_updates']), 1)
        self.assertEqual(after['source_updates'][0]['source'], after['current_source'])
        self.assertGreater(after['snapshot_revision'], before['snapshot_revision'])

    def test_card_read_completes_while_execution_writer_holds_lock(self):
        value = self.close_trade(); cid = value['occurrence_id']
        before = self.f.store.trade_card(cid)
        entered, release = threading.Event(), threading.Event()
        def write():
            with self.f.store._history_transaction() as (_db, _state, save):
                save()
                entered.set()
                if not release.wait(4):
                    raise AssertionError('WRITER_NOT_RELEASED')
        with ThreadPoolExecutor(2) as pool:
            writer = pool.submit(write)
            try:
                self.assertTrue(entered.wait(2))
                result = pool.submit(self.f.store.trade_card, cid).result(timeout=2)
                self.assertEqual(result, before)
            finally:
                release.set()
            writer.result(timeout=3)

    def test_database_enforces_read_only_snapshot(self):
        before = self.journal_rows()
        with self.assertRaises((sqlite3.OperationalError, JournalError)):
            with self.f.store._history_snapshot() as (db, _state):
                db.execute(f'UPDATE {db.table("state")} SET checksum=checksum')
        self.assertEqual(self.journal_rows(), before)

    def test_corrupted_active_state_still_rejected(self):
        value = self.close_trade()
        with self.f.store._history_transaction() as (db, _state, _save):
            db.execute(f'UPDATE {db.table("state")} SET checksum=?', ('bad',))
        with self.assertRaisesRegex(StateError, 'STATE_INTEGRITY_FAILURE'):
            self.f.store.trade_card(value['occurrence_id'])

    def test_corrupted_archive_source_still_rejected(self):
        value = self.close_trade(); self.compact()
        with self.f.store._history_transaction() as (db, _state, _save):
            db.execute(f'UPDATE {db.table("history")} SET source_checksum=?', ('bad',))
        with self.assertRaisesRegex(StateError, 'ARCHIVED_HISTORY_INTEGRITY_FAILURE'):
            self.f.store.trade_card(value['occurrence_id'])

    def test_corrupted_source_update_still_rejected(self):
        value = self.close_trade(); self.compact()
        self.f.worker.receive([cancellation(value, self.now())])
        with self.f.store._history_transaction() as (db, _state, _save):
            db.execute(f'UPDATE {db.table("history_updates")} SET checksum=?', ('bad',))
        with self.assertRaisesRegex(StateError, 'ARCHIVED_HISTORY_INTEGRITY_FAILURE'):
            self.f.store.trade_card(value['occurrence_id'])


class SQLiteSnapshotTests(SnapshotCases, unittest.TestCase):
    def setUp(self):
        self.f = sqlite_support.ExecutionHistoryLifecycleTests()
        self.f.setUp(); self.addCleanup(self.f.doCleanups)
        with closing(self.f.store._connect()) as conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
        self.close_trade = lambda: self.f.close()[0]
        self.compact = self.f.compact
        self.now = self.f.venue.now

    def test_reader_preserves_default_rollback_journal_mode(self):
        with closing(self.f.store._connect()) as conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode=DELETE').fetchone()[0], 'delete')
        # A reader may coexist with a RESERVED writer lock even without WAL.
        self.test_card_read_completes_while_execution_writer_holds_lock()
        with closing(self.f.store._connect()) as conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode').fetchone()[0], 'delete')


@unittest.skipUnless(CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class TestnetPostgresSnapshotTests(SnapshotCases, unittest.TestCase):
    def setUp(self):
        self.f = pg_support.ExecutionHistoryPostgresTests()
        self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.close_trade = lambda: self.f.close_first()[0]
        self.compact = self.f.compact
        self.now = self.f.oracle.now

    def test_reader_does_not_borrow_active_journal_connection_scope(self):
        value = self.close_trade(); cid = value['occurrence_id']
        journal = self.f.store.journal
        with journal.reuse_connection():
            with journal._transaction() as writer:
                with self.f.store._history_snapshot() as (db, _state):
                    self.assertIsNot(db.conn, writer)
                    self.assertEqual(db.execute('SHOW transaction_isolation').fetchone()[0], 'repeatable read')
                    self.assertEqual(db.execute('SHOW transaction_read_only').fetchone()[0], 'on')
            self.assertEqual(self.f.store.trade_card(cid)['card']['card_id'], cid)


@unittest.skipUnless(CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class SoftwarePostgresSnapshotTests(unittest.TestCase):
    def test_software_postgres_card_preserves_schema_domain_and_archive(self):
        fixture = sqlite_support.ExecutionHistoryLifecycleTests()
        fixture.setUp(); self.addCleanup(fixture.doCleanups)
        value, _ = fixture.close(); cid = value['occurrence_id']
        before = fixture.store.load()
        expected = fixture.store.trade_card(cid)['card']
        journal = PostgresJournal.for_ci(CI)
        journal.bootstrap()
        with journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {PG_SCHEMA} CASCADE')
        store = PostgresExecutionState(journal)
        store.initialize(ROUTES, not_before_ms=before['not_before_ms'])
        store.initialize_history()
        store.mutate(lambda state: state.update(before))
        self.assertEqual(store.trade_card(cid)['card'], expected)
        self.assertEqual(store.compact_history(now_ms=fixture.venue.now(), force=True)['archived'], 1)
        result = store.trade_card(cid)
        self.assertEqual(result['card'], expected)
        self.assertEqual(result['location'], 'archived')
        self.assertEqual(result['card']['domain'], 'software')


class PostgresSnapshotFailureTests(unittest.TestCase):
    """Error-policy unit checks only; these are not PostgreSQL integration evidence."""
    def store(self):
        journal = PostgresJournal.for_ci('postgresql://fixture:unused@127.0.0.1/hl_journal_ci')
        return PostgresExecutionState(journal)

    def test_connection_failure_is_redacted(self):
        store = self.store()
        with patch.object(store.journal, '_connect', side_effect=OSError('PRIVATE_CONNECTION_DETAILS')):
            with self.assertRaisesRegex(JournalError, '^PERSISTENCE_UNAVAILABLE_NO_SEND$'):
                store.trade_card('f'*64)

    def test_wrong_database_is_rejected_before_reading_state(self):
        store = self.store()
        class WrongDatabase:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql):
                if sql == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY':
                    return self
                if sql == 'SELECT current_database()':
                    return self
                raise AssertionError('STATE_READ_BEFORE_DATABASE_CHECK')
            def fetchone(self): return ('wrong_database',)
        with patch.object(store.journal, '_connect', return_value=WrongDatabase()):
            with self.assertRaisesRegex(JournalError, '^WRONG_DATABASE$'):
                store.trade_card('f'*64)

    def test_connection_exit_failure_never_returns_a_card(self):
        store = self.store()
        class LostCommit:
            def __enter__(self): return self
            def __exit__(self, *_args): raise OSError('PRIVATE_CONNECTION_EXIT_DETAILS')
            def execute(self, sql):
                self.sql = sql
                return self
            def fetchone(self):
                if self.sql == 'SELECT current_database()':
                    return ('hl_journal_ci',)
                value = initial(ROUTES, T-60000)
                return value, life.digest(value)
        with patch.object(store.journal, '_connect', return_value=LostCommit()):
            with self.assertRaisesRegex(JournalError, '^PERSISTENCE_UNAVAILABLE_NO_SEND$'):
                store.trade_card('f'*64)


if __name__ == '__main__':
    unittest.main()
