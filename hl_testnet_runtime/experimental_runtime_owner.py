"""Cooperative process ownership for the exact experimental Testnet journal.

A dedicated PostgreSQL session holds one advisory lock across transactions.
This neither migrates schemas nor proves that a pre-existing, noncooperating
binary stopped. Callers must join all their workers before releasing ownership.
Verification never reacquires a lock: any loss permanently fences this object.
A confirmed never-held contention may be tried again by the startup scheduler.
"""
import os
import threading

from .postgres_journal import PostgresJournal, JournalError, STAGING_DB, STAGING_HOST

LOCK = 1729048529
_VERIFY = """SELECT pg_backend_pid(), EXISTS (
    SELECT 1 FROM pg_locks
    WHERE locktype='advisory' AND pid=pg_backend_pid()
      AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
      AND classid=0::oid AND objid=%s::oid AND objsubid=1
      AND mode='ExclusiveLock' AND granted)"""


class OwnerError(JournalError):
    """Fixed ownership error codes, never connection details."""


class ProcessLease:
    def __init__(self, journal):
        if (type(journal) is not PostgresJournal or journal._ci is not False
                or journal._parameters.get('host') != STAGING_HOST
                or journal._parameters.get('dbname') != STAGING_DB):
            raise OwnerError('EXACT_TESTNET_OWNER_JOURNAL_REQUIRED')
        self._initialize(journal)

    @classmethod
    def for_ci(cls, journal):
        if (type(journal) is not PostgresJournal or journal._ci is not True
                or journal._parameters.get('host') not in ('127.0.0.1', 'localhost')
                or journal._parameters.get('dbname') != 'hl_journal_ci'
                or journal._parameters.get('sslmode') != 'disable'):
            raise OwnerError('EXACT_LOOPBACK_OWNER_JOURNAL_REQUIRED')
        self = cls.__new__(cls)
        self._initialize(journal)
        return self

    def _initialize(self, journal):
        self.journal = journal
        self._pid = os.getpid()
        self._mutex = threading.RLock()
        self._connection = None
        self._backend_pid = None
        self._status = 'NOT_ACQUIRED'

    def _close(self):
        # Never touch an inherited libpq socket from a different process: even
        # closing it may send a termination message on the parent's session.
        if os.getpid() != self._pid:
            return False
        connection, self._connection = self._connection, None
        if connection is None:
            return True
        try:
            connection.close()
            return True
        except Exception:
            return False

    def _lost(self, code='EXPERIMENTAL_OWNER_SESSION_LOST'):
        self._status = 'LOST'
        self._close()
        raise OwnerError(code) from None

    def _same_process(self):
        if os.getpid() != self._pid:
            self._status = 'FOREIGN_PROCESS'
            raise OwnerError('EXPERIMENTAL_OWNER_FOREIGN_PROCESS')

    def acquire(self):
        """One nonblocking acquisition; no implicit retries or lost-owner reuse."""
        self._same_process()
        with self._mutex:
            if self._status == 'HELD':
                return self.verify()
            if self._status not in ('NOT_ACQUIRED', 'CONTENDED'):
                raise OwnerError('EXPERIMENTAL_OWNER_REACQUIRE_REFUSED')
            self._status = 'ACQUIRING'
            try:
                self._connection = self.journal._connect(autocommit=True)
                identity = self._connection.execute(
                    'SELECT current_database(), pg_backend_pid()').fetchone()
                if (not isinstance(identity, tuple) or len(identity) != 2
                        or identity[0] != self.journal._parameters['dbname']
                        or type(identity[1]) is not int or identity[1] <= 0):
                    self._lost('EXPERIMENTAL_OWNER_DATABASE_IDENTITY_INVALID')
                self._backend_pid = identity[1]
                locked = self._connection.execute(
                    'SELECT pg_try_advisory_lock(%s::bigint)', (LOCK,)).fetchone()
                if isinstance(locked, tuple) and len(locked) == 1 and locked[0] is False:
                    # This exact response proves this session never acquired
                    # the lock. A later scheduler pass may try a NEW session
                    # only after this one was successfully closed.
                    if not self._close():
                        self._lost()
                    self._backend_pid = None
                    self._status = 'CONTENDED'
                    raise OwnerError('EXPERIMENTAL_OWNER_ALREADY_HELD')
                if not isinstance(locked, tuple) or len(locked) != 1 or locked[0] is not True:
                    self._lost()
                self._status = 'HELD'
                return self.verify()
            except OwnerError:
                raise
            except Exception:
                self._lost()

    def verify(self):
        """Fresh same-session ownership proof; no reconnect or lock stacking."""
        self._same_process()
        with self._mutex:
            if self._status != 'HELD' or self._connection is None:
                raise OwnerError('EXPERIMENTAL_OWNER_NOT_HELD')
            try:
                observed = self._connection.execute(_VERIFY, (LOCK,)).fetchone()
            except Exception:
                self._lost()
            if (not isinstance(observed, tuple) or len(observed) != 2
                    or type(observed[0]) is not int or observed[0] != self._backend_pid
                    or observed[1] is not True):
                self._lost()
            return True

    def release(self):
        """Release only after worker shutdown; ambiguity is never a success."""
        if os.getpid() != self._pid:
            return False
        with self._mutex:
            if self._status == 'RELEASED':
                return True
            if self._status in ('NOT_ACQUIRED', 'CONTENDED'):
                self._status = 'RELEASED'
                return True
            if self._status != 'HELD' or self._connection is None:
                return False
            confirmed = False
            try:
                self.verify()
                unlocked = self._connection.execute(
                    'SELECT pg_advisory_unlock(%s::bigint)', (LOCK,)).fetchone()
                confirmed = isinstance(unlocked, tuple) and len(unlocked) == 1 and unlocked[0] is True
            except Exception:
                confirmed = False
            closed = self._close()
            self._status = 'RELEASED' if confirmed and closed else 'LOST'
            return self._status == 'RELEASED'

    def health(self):
        """Local last-result status only; dispatch must still call verify()."""
        if os.getpid() != self._pid:
            return dict(status='FOREIGN_PROCESS', held=False)
        with self._mutex:
            return dict(status=self._status, held=self._status == 'HELD')
