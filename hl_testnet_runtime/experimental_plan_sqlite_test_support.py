"""Software-only SQLite adapter for exercising unchanged PlanStore transactions.

This is test support, not a runtime backend. It has no environment constructor,
network access, scheduler or exchange capability. SQLite BEGIN IMMEDIATE is a
stronger writer lock than PostgreSQL's advisory/row locks. Passing these tests
therefore does not verify PostgreSQL SQL, migrations, locking or libpq behavior.
Only explicitly initialized local software databases are accepted.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import stat

from .experimental_plan_store import LOCK, SCHEMA, VERSION, PlanStore, PlanStoreError

MARKER = 'software-only-planstore-sqlite-test-v1'


class SQLiteTestError(PlanStoreError):
    pass


def _normalized(sql):
    return ' '.join(sql.split())


class _Result:
    def __init__(self, cursor, *, decode=False):
        self.cursor, self.decode = cursor, decode

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None and self.decode:
            return json.loads(row[0]), row[1], row[2]
        return row


class _Connection:
    """An exact statement adapter, not a PostgreSQL parser or emulator."""
    def __init__(self, owner, connection):
        self.owner, self.connection = owner, connection
        self.write_lock_requested = False

    def execute(self, sql, params=()):
        query = _normalized(sql)
        if query == f'SELECT version,domain FROM {SCHEMA}.metadata WHERE singleton':
            return _Result(self.connection.execute('SELECT version,domain FROM metadata WHERE singleton=1'))
        if query == 'SELECT pg_advisory_xact_lock(%s)' and params == (LOCK,):
            # The real SQLite writer transaction was acquired before yielding.
            # This does not test PostgreSQL's advisory-lock implementation.
            self.write_lock_requested = True
            return _Result(self.connection.execute('SELECT NULL'))
        select = f'SELECT value,digest,revision FROM {SCHEMA}.plans WHERE occurrence_id=%s'
        if query in (select, select + ' FOR UPDATE'):
            return _Result(self.connection.execute(
                'SELECT value,digest,revision FROM plans WHERE occurrence_id=?', params), decode=True)
        insert_plan = _normalized(f'''INSERT INTO {SCHEMA}.plans VALUES(%s,%s,%s::jsonb,%s)
            ON CONFLICT(occurrence_id) DO UPDATE SET revision=EXCLUDED.revision,value=EXCLUDED.value,digest=EXCLUDED.digest''')
        if query == insert_plan:
            if not self.write_lock_requested:
                raise SQLiteTestError('SQLITE_TEST_EXPECTED_PLANSTORE_WRITE_LOCK')
            value = json.loads(params[2])
            if value.get('domain') != 'software':
                raise SQLiteTestError('SQLITE_TEST_SOFTWARE_DOMAIN_REQUIRED')
            return _Result(self.connection.execute('''INSERT INTO plans VALUES(?,?,?,?)
                ON CONFLICT(occurrence_id) DO UPDATE SET revision=excluded.revision,
                value=excluded.value,digest=excluded.digest''', params))
        if query == f'INSERT INTO {SCHEMA}.events VALUES(%s,%s,%s,%s,%s)':
            if not self.write_lock_requested:
                raise SQLiteTestError('SQLITE_TEST_EXPECTED_PLANSTORE_WRITE_LOCK')
            if self.owner.fail_event_once:
                self.owner.fail_event_once = False
                raise SQLiteTestError('SQLITE_TEST_EVENT_WRITE_FAILURE')
            return _Result(self.connection.execute('INSERT INTO events VALUES(?,?,?,?,?)', params))
        raise SQLiteTestError('SQLITE_TEST_UNSUPPORTED_POSTGRES_STATEMENT')


class SoftwareSQLiteJournal:
    """A real file-backed software fixture; never selected from runtime settings."""
    @property
    def _ci(self):
        return True

    def __init__(self, path):
        if not isinstance(path, (str, Path)) or str(path).startswith('file:') or str(path) == ':memory:':
            raise SQLiteTestError('SQLITE_TEST_EXPLICIT_LOCAL_PATH_REQUIRED')
        candidate = Path(path)
        if not candidate.is_absolute() or not candidate.parent.is_dir():
            raise SQLiteTestError('SQLITE_TEST_EXPLICIT_LOCAL_PATH_REQUIRED')
        self.path = candidate
        self.fail_event_once = False
        self.lose_commit_ack_once = False

    def _file(self):
        try:
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or self.path.is_symlink():
                raise ValueError()
        except (OSError, ValueError):
            raise SQLiteTestError('SQLITE_TEST_REGULAR_INITIALIZED_FILE_REQUIRED') from None

    def _connect(self):
        self._file()
        connection = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True,
            timeout=10, isolation_level=None)
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA synchronous=FULL')
        return connection

    def initialize(self):
        """Create test tables explicitly. This does not run PlanStore's PG DDL."""
        try:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            self._file()
            with self._transaction():
                pass
            return False
        else:
            os.close(descriptor)
        connection = self._connect()
        try:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('BEGIN IMMEDIATE')
            connection.execute('''CREATE TABLE metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version TEXT NOT NULL,domain TEXT NOT NULL CHECK(domain='software'),marker TEXT NOT NULL)''')
            connection.execute('INSERT INTO metadata VALUES(1,?,?,?)', (VERSION, 'software', MARKER))
            connection.execute('''CREATE TABLE plans(occurrence_id TEXT PRIMARY KEY CHECK(length(occurrence_id)=64),
                revision INTEGER NOT NULL,value TEXT NOT NULL,digest TEXT NOT NULL)''')
            connection.execute('''CREATE TABLE events(occurrence_id TEXT NOT NULL REFERENCES plans(occurrence_id),
                revision INTEGER NOT NULL,event TEXT NOT NULL,at_utc TEXT NOT NULL,
                record_hash TEXT NOT NULL,PRIMARY KEY(occurrence_id,revision))''')
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        return True

    def _ready(self, conn):
        raise SQLiteTestError('SQLITE_TEST_CANNOT_VERIFY_POSTGRES_BOOTSTRAP')

    @contextmanager
    def _transaction(self):
        connection = self._connect()
        committed = False
        try:
            connection.execute('BEGIN IMMEDIATE')
            try:
                marker = connection.execute('SELECT version,domain,marker FROM metadata WHERE singleton=1').fetchone()
            except sqlite3.DatabaseError:
                raise SQLiteTestError('SQLITE_TEST_INITIALIZED_SOFTWARE_SCHEMA_REQUIRED') from None
            if marker != (VERSION, 'software', MARKER):
                raise SQLiteTestError('SQLITE_TEST_INITIALIZED_SOFTWARE_SCHEMA_REQUIRED')
            yield _Connection(self, connection)
            connection.commit()
            committed = True
            if self.lose_commit_ack_once:
                self.lose_commit_ack_once = False
                raise SQLiteTestError('SQLITE_TEST_COMMIT_ACK_UNKNOWN')
        except BaseException:
            if not committed and connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()


def open_software_store(path, *, create=False):
    journal = SoftwareSQLiteJournal(path)
    if create:
        journal.initialize()
    return PlanStore(journal)
