"""Durable *isolated* experimental worker state; never a live order journal.

One aggregate transaction serializes source cancellation, cross-occurrence
admission and uncertain attempts across both accounts. No callback may perform
external I/O. SQLite is an actual process-safe local backend, not a SQL mock.
A separate loopback-only PostgreSQL backend exercises the identical transitions.
Neither backend can be selected by any existing startup or environment variable.
"""
from contextlib import closing
from copy import deepcopy
import json
import os
from pathlib import Path
import sqlite3

from . import card_lifecycle as life
from .postgres_journal import JournalError

VERSION = 'experimental-isolated-execution-state-v1'
PG_SCHEMA = 'hl_experimental_isolated_worker_v1'


class StateError(JournalError):
    """Preserve fixed domain rejections through the journal rollback boundary.

    Unexpected SQL, connection and commit failures still use the journal's
    generic persistence error and can never release an attempted request.
    """


def initial(routes, not_before_ms):
    life.moment(not_before_ms)
    accounts = {r: life.address(routes[r]['account']) for r in ('long_account', 'short_account')}
    if len(set(accounts.values())) != 2:
        raise StateError('INDEPENDENT_SOFTWARE_ACCOUNTS_REQUIRED')
    return dict(version=VERSION, domain='software', revision=0, not_before_ms=not_before_ms,
                routes=accounts, sources={}, trades={}, requests={}, snapshots={}, budget=[], events=[])


def checked(raw, checksum):
    value = json.loads(raw) if isinstance(raw, str) else raw
    if (life.digest(value) != checksum or value.get('version') != VERSION
            or value.get('domain') != 'software'):
        raise StateError('ISOLATED_STATE_INTEGRITY_FAILURE')
    return deepcopy(value)


def encode(value):
    result = life.encoded(value)
    if len(result.encode()) > 16*1024*1024:
        raise StateError('ISOLATED_STATE_CAPACITY_REVIEW_REQUIRED')
    return result


class ExecutionState:
    domain = 'software'

    def __init__(self, path):
        self.path = Path(path).absolute()
        if (not self.path.name.endswith('.isolated-experimental.sqlite3')
                or self.path.is_symlink() or not self.path.parent.is_dir()):
            raise StateError('EXPLICIT_ISOLATED_SQLITE_PATH_REQUIRED')

    def _connect(self):
        conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        conn.execute('PRAGMA busy_timeout=10000')
        conn.execute('PRAGMA synchronous=FULL')
        return conn

    def initialize(self, routes, *, not_before_ms):
        expected = initial(routes, not_before_ms)
        with closing(self._connect()) as conn:
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('CREATE TABLE IF NOT EXISTS experimental_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value TEXT NOT NULL, checksum TEXT NOT NULL)')
            row = conn.execute('SELECT value,checksum FROM experimental_state WHERE singleton=1').fetchone()
            if row is None:
                conn.execute('INSERT INTO experimental_state VALUES(1,?,?)', (encode(expected), life.digest(expected)))
            else:
                actual = checked(*row)
                if actual['routes'] != expected['routes'] or actual['not_before_ms'] != not_before_ms:
                    raise StateError('ISOLATED_ROUTE_OR_ACTIVATION_CHANGED')
            conn.commit()
        os.chmod(self.path, 0o600)

    def load(self):
        with closing(self._connect()) as conn:
            row = conn.execute('SELECT value,checksum FROM experimental_state WHERE singleton=1').fetchone()
            if row is None:
                raise StateError('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
            return checked(*row)

    def mutate(self, transition):
        conn = self._connect()
        try:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT value,checksum FROM experimental_state WHERE singleton=1').fetchone()
            if row is None:
                raise StateError('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
            value = checked(*row)
            result = transition(value)
            value['revision'] += 1
            conn.execute('UPDATE experimental_state SET value=?,checksum=? WHERE singleton=1', (encode(value), life.digest(value)))
            conn.commit()
            # A failed/unknown COMMIT acknowledgment must escape without result.
            return deepcopy(result)
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()


class PostgresExecutionState:
    """Only disposable CI PostgreSQL on loopback; cannot use production DSNs."""
    domain = 'software'

    def __init__(self, journal):
        from .postgres_journal import PostgresJournal
        if type(journal) is not PostgresJournal or journal._ci is not True:
            raise StateError('LOOPBACK_CI_POSTGRES_REQUIRED')
        p = journal._parameters
        if p.get('host') not in ('127.0.0.1', 'localhost') or p.get('dbname') != 'hl_journal_ci' or p.get('sslmode') != 'disable':
            raise StateError('LOOPBACK_CI_POSTGRES_REQUIRED')
        self.journal = journal

    def initialize(self, routes, *, not_before_ms):
        value = initial(routes, not_before_ms)
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (1729048361,))
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}')
            conn.execute(f'CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.state(singleton boolean PRIMARY KEY CHECK(singleton), value jsonb NOT NULL, checksum text NOT NULL)')
            conn.execute(f'REVOKE ALL ON SCHEMA {PG_SCHEMA} FROM PUBLIC')
            conn.execute(f'REVOKE ALL ON {PG_SCHEMA}.state FROM PUBLIC')
            row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton').fetchone()
            if row is None:
                conn.execute(f'INSERT INTO {PG_SCHEMA}.state VALUES(true,%s::jsonb,%s)', (encode(value), life.digest(value)))
            else:
                actual = checked(*row)
                if actual['routes'] != value['routes'] or actual['not_before_ms'] != not_before_ms:
                    raise StateError('ISOLATED_ROUTE_OR_ACTIVATION_CHANGED')

    def load(self):
        with self.journal._transaction() as conn:
            row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton').fetchone()
            if row is None:
                raise StateError('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
            return checked(*row)

    def mutate(self, transition):
        with self.journal._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (1729048361,))
            row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton FOR UPDATE').fetchone()
            if row is None:
                raise StateError('EXPLICIT_ISOLATED_INITIALIZATION_REQUIRED')
            value = checked(*row)
            result = transition(value)
            value['revision'] += 1
            conn.execute(f'UPDATE {PG_SCHEMA}.state SET value=%s::jsonb,checksum=%s WHERE singleton', (encode(value), life.digest(value)))
        return deepcopy(result)
