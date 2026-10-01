"""Shared, fail-closed REST weight admission for the fixed Testnet host.

Hyperliquid's documented IP allowance is 1200 REST weight per minute:
https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
Fill responses have at most 2000 rows and add weight per 20 returned rows:
https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint

All configured Testnet processes share one staging PostgreSQL bucket, across
both accounts. This does not measure other clients behind the same NAT. It is
an admission bound, never evidence, an exchange retry, or trade authorization.
Background admission leaves 400 weight for protection/reconciliation/exits.
No transaction survives into HTTP; reservations commit before transport. A
one-second permit and conservative 69-second ledger window include allowance
for the existing four-second TCP and TLS socket timeouts. This is conservative
request accounting, not a hard end-to-end network deadline: DNS, other NAT
clients and a stalled OS are not measured. All original evidence clocks apply.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import atexit
import hashlib
import json
import os
import time
import threading
import uuid

from .postgres_journal import JournalError, PostgresJournal, SCHEMA

HOST = 'api.hyperliquid-testnet.xyz'
VERSION = 'testnet-shared-rest-weight-v1'
LIMIT = 1200
BACKGROUND_LIMIT = 800
WINDOW_MS = 69000
PERMIT_MS = 1000
LOCK = 1729048160
LOCK_WAIT_MS = 250
MAX_COORDINATORS = 4
POLICY = SCHEMA + '.request_budget_policy'
TICKETS = SCHEMA + '.request_budget_tickets'
PRIORITIES = frozenset(('background', 'protection'))
LIGHT = frozenset(('l2Book', 'allMids', 'clearinghouseState', 'orderStatus',
                   'spotClearinghouseState', 'exchangeStatus'))
NORMAL = frozenset(('meta', 'metaAndAssetCtxs', 'spotMeta', 'spotMetaAndAssetCtxs',
                    'frontendOpenOrders', 'openOrders', 'userAbstraction',
                    'userRateLimit', 'activeAssetData', 'userNonFundingLedgerUpdates'))
FILLS = frozenset(('userFills', 'userFillsByTime'))
SIZED = FILLS | frozenset(('userFunding',))


class BudgetError(ValueError):
    """Fixed redacted codes only, never SQL, credentials, or remote text."""
    def __init__(self, code, *, stage=None, elapsed_ms=None):
        super().__init__(code)
        self.stage = stage if stage in {'LOCAL_WAIT','CONNECT','READY','GLOBAL_LOCK',
            'ACCOUNTING','REFUND','COMMIT','AFTER_COMMIT'} else None
        self.elapsed_ms = elapsed_ms if type(elapsed_ms) is int and elapsed_ms >= 0 else None


@dataclass
class _Coordinator:
    lock: object = field(default_factory=threading.Lock, repr=False)
    connection: object = field(default=None, repr=False)


_coordinators = {}
_coordinators_lock = threading.Lock()


def _coordinator(journal):
    # Keep credentials out of registry identities and all diagnostic messages.
    key=(os.getpid(),hashlib.sha256(json.dumps(journal._parameters,
        sort_keys=True,separators=(',',':')).encode()).digest())
    with _coordinators_lock:
        if key not in _coordinators:
            if len(_coordinators)>=MAX_COORDINATORS:
                raise BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE',stage='LOCAL_WAIT')
            _coordinators[key]=_Coordinator()
        return _coordinators[key]


def _close_coordinators():
    for coordinator in tuple(_coordinators.values()):
        if coordinator.lock.acquire(blocking=False):
            try:
                if coordinator.connection is not None:
                    try:coordinator.connection.close()
                    except Exception:pass
                    coordinator.connection=None
            finally:
                coordinator.lock.release()


atexit.register(_close_coordinators)


def _after_fork():
    global _coordinators,_coordinators_lock
    inherited=tuple(_coordinators.values())
    _coordinators={}
    _coordinators_lock=threading.Lock()
    for coordinator in inherited:
        conn=coordinator.connection
        if conn is not None:
            try:
                # Close the CHILD's descriptor before libpq cleanup, so its
                # Terminate message cannot affect the parent's shared session.
                fd=conn.pgconn.socket
                if type(fd) is int and fd>=0:os.close(fd)
                conn.close()
            except Exception:pass


if hasattr(os,'register_at_fork'):
    os.register_at_fork(after_in_child=_after_fork)


def request_weight(path, body, *, host=HOST):
    """Conservative maximum charged before any HTTP request is attempted."""
    if host != HOST or path not in ('/info', '/exchange') or not isinstance(body, dict):
        raise BudgetError('TESTNET_REQUEST_BUDGET_TYPE_NOT_ALLOWED')
    if path == '/info':
        kind = body.get('type')
        if not isinstance(kind, str):
            raise BudgetError('TESTNET_REQUEST_BUDGET_TYPE_NOT_ALLOWED')
        if kind in LIGHT:
            return 2
        if kind == 'userRole':
            return 60
        if kind in NORMAL:
            return 20
        if kind in SIZED:
            # Hold the largest possible fill response before its size is known.
            # Failure, an oversized response or invalid JSON retains this charge.
            return 20 + 2000 // 20
        raise BudgetError('TESTNET_REQUEST_BUDGET_TYPE_NOT_ALLOWED')
    action = body.get('action')
    if not isinstance(action, dict):
        raise BudgetError('TESTNET_REQUEST_BUDGET_TYPE_NOT_ALLOWED')
    arrays = {'order': 'orders', 'cancel': 'cancels',
              'cancelByCloid': 'cancels', 'batchModify': 'modifies'}
    kind = action.get('type')
    key = arrays.get(kind) if isinstance(kind, str) else None
    rows = action.get(key) if key is not None else None
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1000 or any(
            not isinstance(row, dict) for row in rows):
        raise BudgetError('TESTNET_REQUEST_BUDGET_TYPE_NOT_ALLOWED')
    return 1 + len(rows) // 40


def initialize(conn):
    """Explicit runtime startup only. No hot-path repairs or migrations."""
    conn.execute(f'''CREATE TABLE IF NOT EXISTS {POLICY} (
        singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL,
        host text NOT NULL, maximum integer NOT NULL, background_maximum integer NOT NULL,
        window_ms integer NOT NULL)''')
    conn.execute(f'''CREATE TABLE IF NOT EXISTS {TICKETS} (
        token text PRIMARY KEY CHECK(length(token)=32),
        admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        weight integer NOT NULL CHECK(weight BETWEEN 1 AND 1200),
        settled boolean NOT NULL DEFAULT false)''')
    conn.execute(f'CREATE INDEX IF NOT EXISTS request_budget_admitted_at_idx ON {TICKETS}(admitted_at)')
    conn.execute(f'''INSERT INTO {POLICY}(singleton,version,host,maximum,background_maximum,window_ms)
        VALUES(true,%s,%s,%s,%s,%s) ON CONFLICT(singleton) DO NOTHING''',
        (VERSION, HOST, LIMIT, BACKGROUND_LIMIT, WINDOW_MS))
    for table in (POLICY, TICKETS):
        conn.execute(f'REVOKE ALL ON {table} FROM PUBLIC')
    ready(conn)


def ready(conn, *, expected_database=None):
    row = conn.execute(f'''SELECT current_database(),version,host,maximum,
        background_maximum,window_ms,to_regclass(%s)
        FROM {POLICY} WHERE singleton=true''',(TICKETS,)).fetchone()
    if row is None or row[1:6] != (VERSION, HOST, LIMIT, BACKGROUND_LIMIT, WINDOW_MS) or row[6] is None:
        raise BudgetError('TESTNET_REQUEST_BUDGET_SCHEMA_REQUIRES_REVIEW')
    if expected_database is not None and row[0] != expected_database:
        raise BudgetError('TESTNET_REQUEST_BUDGET_WRONG_DATABASE')


@dataclass(frozen=True)
class Permit:
    """One locally consumed transport permission; never transferable evidence."""
    _budget: object = field(repr=False)
    _token: str = field(repr=False)
    _kind: str = field(repr=False)
    _weight: int
    _deadline_ns: int = field(repr=False)
    _used: list = field(default_factory=lambda: [False], repr=False, compare=False)
    _lock: object = field(default_factory=threading.Lock, repr=False, compare=False)

    def validate(self):
        """Reject an already stale permission without consuming or extending it."""
        with self._lock:
            if self._used[0]:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_ALREADY_USED')
            if time.monotonic_ns() > self._deadline_ns:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')

    def check(self):
        """Consume immediately before HTTP. Expiry never releases a reservation."""
        with self._lock:
            if self._used[0]:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_ALREADY_USED')
            if time.monotonic_ns() > self._deadline_ns:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
            self._used[0] = True

    def finish(self, response):
        """Refund only proven response-size excess; a refund failure is harmless.

        Fixed-size request weights need no database round trip. Invalid or
        unbounded responses retain their full reservation, as do failed requests.
        """
        if not self._used[0] or self._kind not in SIZED:
            return False
        if (not isinstance(response, list) or len(response) > 2000
                or any(not isinstance(row, dict) for row in response)):
            return False
        weight = 20 + (len(response) + 19) // 20
        return self._budget._settle(self._token, weight)


class Budget:
    def __init__(self, journal):
        if not isinstance(journal, PostgresJournal):
            raise BudgetError('TESTNET_SHARED_REQUEST_JOURNAL_REQUIRED')
        self.journal = journal

    @classmethod
    def from_env(cls, env):
        # Legacy isolated read-only tools may have no runtime configuration.
        # Actual Testnet workers and any configured journal must fail closed.
        configured = (env.get('HL_TESTNET_JOURNAL_BACKEND') is not None
            or env.get('HL_TESTNET_DATABASE_URL') is not None
            or env.get('HL_TESTNET_RUNTIME_MODE') not in (None, '', 'read_only')
            or env.get('RENDER_SERVICE_ID') == 'srv-dakptbh594qs7395460g')
        if not configured:
            return None
        if env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1':
            raise BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        try:
            return cls(PostgresJournal.from_env(env))
        except JournalError:
            raise BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE') from None

    def initialize(self):
        """Once at explicit runtime startup, before any public read workers."""
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            initialize(conn)
        # Warm the single process connection before parallel public workers.
        # Policy/database identity is still checked on every later transaction.
        with self._transaction():
            pass

    @contextmanager
    def _transaction(self, *, deadline_ns=None, diagnostic=None):
        """Serialized short SQL only; reusable connection, no HTTP or retries.

        Local serialization avoids four same-process clients competing for the
        database lock. The advisory lock still coordinates OTHER processes.
        Every success commits before release; every failure discards the socket.
        """
        coordinator=_coordinator(self.journal)
        diagnostic={} if diagnostic is None else diagnostic
        started=time.monotonic()
        diagnostic['stage']='LOCAL_WAIT'
        remaining=(PERMIT_MS/1000 if deadline_ns is None else
            (deadline_ns-time.monotonic_ns())/1_000_000_000)
        if remaining<=0:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',stage='LOCAL_WAIT')
        if not coordinator.lock.acquire(timeout=min(PERMIT_MS/1000,remaining)):
            raise BudgetError('TESTNET_REQUEST_BUDGET_BUSY',stage='LOCAL_WAIT',
                              elapsed_ms=int((time.monotonic()-started)*1000))
        try:
            import psycopg
            diagnostic['stage']='CONNECT'
            conn=coordinator.connection
            if conn is None or conn.closed:
                conn=psycopg.connect(**self.journal._parameters,connect_timeout=2,
                    options=f'-c statement_timeout=500 -c lock_timeout={LOCK_WAIT_MS} -c idle_in_transaction_session_timeout=2000 -c synchronous_commit=on')
                coordinator.connection=conn
                # READY runs before the lock. Each subsequent statement needs
                # a fresh snapshot, regardless of database/role defaults.
                conn.isolation_level=psycopg.IsolationLevel.READ_COMMITTED
            if deadline_ns is not None and time.monotonic_ns()>deadline_ns:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
            diagnostic['stage']='READY'
            ready(conn,expected_database=self.journal._parameters['dbname'])
            yield conn
            diagnostic['stage']='COMMIT'
            conn.commit()
        except BaseException as exc:
            keep=False
            if isinstance(exc,BudgetError) and str(exc)=='TESTNET_REQUEST_BUDGET_EXHAUSTED':
                # A quota refusal is not a broken connection. A positively
                # acknowledged rollback makes it reusable without any retry.
                try:coordinator.connection.rollback();keep=True
                except Exception:pass
            if coordinator.connection is not None and not keep:
                try:coordinator.connection.close()
                except Exception:pass
                coordinator.connection=None
            if not isinstance(exc,Exception):
                raise
            code=(str(exc) if isinstance(exc,BudgetError) else
                'TESTNET_REQUEST_BUDGET_BUSY' if getattr(exc,'sqlstate',None)=='55P03'
                else 'TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
            raise BudgetError(code,stage=diagnostic['stage'],
                elapsed_ms=int((time.monotonic()-started)*1000)) from None
        finally:
            coordinator.lock.release()

    def acquire(self, path, body, *, priority='background', host=HOST):
        weight = request_weight(path, body, host=host)
        if not isinstance(priority, str) or priority not in PRIORITIES:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PRIORITY_INVALID')
        token = uuid.uuid4().hex
        # Start the local clock before database admission, not after COMMIT.
        # A slow database must not mint an apparently fresh transport permit.
        deadline = time.monotonic_ns() + PERMIT_MS * 1000000
        diagnostic={}
        with self._transaction(deadline_ns=deadline,diagnostic=diagnostic) as conn:
            # Separate statement: the accounting snapshot MUST follow this lock
            # acquisition, including a wait for another process's COMMIT.
            diagnostic['stage']='GLOBAL_LOCK'
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            ceiling = LIMIT if priority == 'protection' else BACKGROUND_LIMIT
            diagnostic['stage']='ACCOUNTING'
            row=conn.execute(f'''WITH stamp AS MATERIALIZED (
                    SELECT clock_timestamp() AS at),
                pruned AS (DELETE FROM {TICKETS} USING stamp
                    WHERE admitted_at <= stamp.at-(%s*interval '1 millisecond') RETURNING token),
                used AS (SELECT COALESCE(sum(weight),0) AS total FROM {TICKETS},stamp
                    WHERE admitted_at > stamp.at-(%s*interval '1 millisecond'))
                INSERT INTO {TICKETS}(token,admitted_at,weight)
                SELECT %s,stamp.at,%s FROM stamp,used WHERE used.total+%s<=%s
                RETURNING token''',(WINDOW_MS,WINDOW_MS,token,weight,weight,ceiling)).fetchone()
            if row is None:
                raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        permit = Permit(self, token, body.get('type') if path == '/info' else 'exchange', weight, deadline)
        if time.monotonic_ns() > deadline:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',stage='AFTER_COMMIT',
                elapsed_ms=PERMIT_MS+int((time.monotonic_ns()-deadline)/1_000_000))
        return permit

    def _settle(self, token, weight):
        try:
            diagnostic={}
            with self._transaction(diagnostic=diagnostic) as conn:
                diagnostic['stage']='REFUND'
                # Smaller weight is safe concurrently with admission. Repeated
                # finish calls cannot lower an already settled ticket again.
                row = conn.execute(f'''UPDATE {TICKETS} SET weight=%s,settled=true
                    WHERE token=%s AND settled=false AND weight >= %s RETURNING token''',
                    (weight, token, weight)).fetchone()
            return row is not None
        except BudgetError:
            # Full charge stays in place; response evidence is not lost just
            # because a best-effort refund could not be committed.
            return False


def default_budget():
    """Configured public readers use the common journal without touching keys."""
    import os
    return Budget.from_env(os.environ)
