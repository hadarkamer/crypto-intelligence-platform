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


def ready(conn):
    row = conn.execute(f'''SELECT version,host,maximum,background_maximum,window_ms
        FROM {POLICY} WHERE singleton=true''').fetchone()
    if row != (VERSION, HOST, LIMIT, BACKGROUND_LIMIT, WINDOW_MS):
        raise BudgetError('TESTNET_REQUEST_BUDGET_SCHEMA_REQUIRES_REVIEW')
    if conn.execute('SELECT to_regclass(%s)', (TICKETS,)).fetchone()[0] is None:
        raise BudgetError('TESTNET_REQUEST_BUDGET_SCHEMA_REQUIRES_REVIEW')


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

    @contextmanager
    def _transaction(self):
        """Separate short transaction; no HTTP, sleeps or admission retries."""
        try:
            import psycopg
            with psycopg.connect(**self.journal._parameters, connect_timeout=2,
                    options='-c statement_timeout=500 -c lock_timeout=50 -c idle_in_transaction_session_timeout=2000 -c synchronous_commit=on') as conn:
                if conn.execute('SELECT current_database()').fetchone()[0] != self.journal._parameters['dbname']:
                    raise BudgetError('TESTNET_REQUEST_BUDGET_WRONG_DATABASE')
                yield conn
                # Acknowledged synchronous COMMIT precedes return of the permit.
        except BudgetError:
            raise
        except Exception as exc:
            if getattr(exc, 'sqlstate', None) == '55P03':
                raise BudgetError('TESTNET_REQUEST_BUDGET_BUSY') from None
            raise BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE') from None

    def acquire(self, path, body, *, priority='background', host=HOST):
        weight = request_weight(path, body, host=host)
        if not isinstance(priority, str) or priority not in PRIORITIES:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PRIORITY_INVALID')
        token = uuid.uuid4().hex
        # Start the local clock before database admission, not after COMMIT.
        # A slow database must not mint an apparently fresh transport permit.
        deadline = time.monotonic_ns() + PERMIT_MS * 1000000
        with self._transaction() as conn:
            ready(conn)
            # Serialize the four healthy read workers for at most 50ms. This
            # waits only for the admission transaction, never for quota refill.
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            stamp = conn.execute('SELECT clock_timestamp()').fetchone()[0]
            conn.execute(f'''DELETE FROM {TICKETS}
                WHERE admitted_at <= %s - (%s * interval '1 millisecond')''', (stamp, WINDOW_MS))
            used = conn.execute(f'SELECT COALESCE(sum(weight),0) FROM {TICKETS}').fetchone()[0]
            ceiling = LIMIT if priority == 'protection' else BACKGROUND_LIMIT
            if used + weight > ceiling:
                raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
            conn.execute(f'INSERT INTO {TICKETS}(token,admitted_at,weight) VALUES(%s,%s,%s)',
                         (token, stamp, weight))
        permit = Permit(self, token, body.get('type') if path == '/info' else 'exchange', weight, deadline)
        if time.monotonic_ns() > deadline:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        return permit

    def _settle(self, token, weight):
        try:
            with self._transaction() as conn:
                ready(conn)
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
