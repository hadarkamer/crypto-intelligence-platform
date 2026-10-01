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
import re
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
OBSERVATION_MS = 15000
OBSERVATION_TYPES = frozenset(('userFillsByTime', 'frontendOpenOrders',
                             'clearinghouseState', 'orderStatus'))
INFO_PLAN_TYPES = frozenset(('meta','userRole','userAbstraction',
    'spotClearinghouseState','clearinghouseState','activeAssetData',
    'userRateLimit','frontendOpenOrders'))
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
    _pid: int = field(default_factory=os.getpid, repr=False, compare=False)
    _guard: object = field(default=None, repr=False, compare=False)

    def _local_guard(self):
        if os.getpid() != self._pid:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_PROCESS_CHANGED')
        if self._guard is not None:
            self._guard()

    def validate(self):
        """Reject an already stale permission without consuming or extending it."""
        self._local_guard()
        with self._lock:
            if self._used[0]:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_ALREADY_USED')
            if time.monotonic_ns() > self._deadline_ns:
                raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')

    def check(self):
        """Consume immediately before HTTP. Expiry never releases a reservation."""
        self._local_guard()
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
        if os.getpid() != self._pid or not self._used[0] or self._kind not in SIZED:
            return False
        if (not isinstance(response, list) or len(response) > 2000
                or any(not isinstance(row, dict) for row in response)):
            return False
        weight = 20 + (len(response) + 19) // 20
        return self._budget._settle(self._token, weight)


def _observation_key(body):
    if (not isinstance(body, dict) or not isinstance(body.get('type'),str)
            or body['type'] not in OBSERVATION_TYPES):
        raise BudgetError('TESTNET_OBSERVATION_BATCH_TYPE_NOT_ALLOWED')
    try:
        return json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError):
        raise BudgetError('TESTNET_OBSERVATION_BATCH_TYPE_NOT_ALLOWED') from None


def _info_plan_key(body):
    """Only exact read-only entry-preflight requests; no arbitrary /info body."""
    if (not isinstance(body,dict) or not isinstance(body.get('type'),str)
            or body['type'] not in INFO_PLAN_TYPES):
        raise BudgetError('TESTNET_INFO_PLAN_TYPE_NOT_ALLOWED')
    kind=body['type']
    fields={'type'} if kind=='meta' else {'type','user'}
    if kind=='activeAssetData':fields.add('coin')
    if set(body)!=fields:
        raise BudgetError('TESTNET_INFO_PLAN_BODY_NOT_ALLOWED')
    if kind!='meta' and (not isinstance(body['user'],str)
            or not re.fullmatch(r'0x[0-9a-fA-F]{40}',body['user'])
            or int(body['user'][2:],16)==0):
        raise BudgetError('TESTNET_INFO_PLAN_BODY_NOT_ALLOWED')
    if kind=='activeAssetData' and (not isinstance(body['coin'],str)
            or not re.fullmatch(r'[A-Z][A-Z0-9]{0,19}',body['coin'])):
        raise BudgetError('TESTNET_INFO_PLAN_BODY_NOT_ALLOWED')
    return json.dumps(body,sort_keys=True,separators=(',',':'),allow_nan=False)


class ObservationBatch:
    """Exact finite observation credits, never a pre-issued HTTP permission.

    Each ticket is funded before the first read, then claimed once immediately
    before its own HTTP. A failed/uncertain claim stays charged. Closing can
    release only acknowledged credits still present in this local object that
    never began a claim; a lost funding COMMIT cannot produce such an object.
    """
    _key=staticmethod(_observation_key)
    def __init__(self, owner, entries, priority, deadline_ns):
        self._owner, self._priority = owner, priority
        self._deadline_ns, self._pid = deadline_ns, os.getpid()
        self._lock, self._closed = threading.Lock(), False
        self._entries = {}
        self._claimed, self._extended = {}, {}
        self._count = len(entries)
        for key, token, weight in entries:
            self._entries.setdefault(key, []).append((token, weight))

    def _guard(self):
        # Check the PID before touching an inherited lock after fork.
        if os.getpid() != self._pid:
            raise BudgetError('TESTNET_OBSERVATION_BATCH_PROCESS_CHANGED')
        with self._lock:
            if self._closed:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_CLOSED')
            if time.monotonic_ns() > self._deadline_ns:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_EXPIRED')

    def acquire(self, path, body, *, priority='background', host=HOST):
        # This child's clock starts before local locking and every SQL step.
        deadline = min(time.monotonic_ns() + PERMIT_MS * 1000000,
                       self._deadline_ns)
        if path != '/info' or host != HOST or priority != self._priority:
            raise BudgetError('TESTNET_OBSERVATION_BATCH_REQUEST_MISMATCH')
        key = self._key(body)
        self._guard()
        with self._lock:
            if self._closed or time.monotonic_ns() > self._deadline_ns:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_EXPIRED_OR_CLOSED')
            entries = self._entries.get(key)
            if not entries:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_UNDECLARED_REQUEST')
            # Burn before SQL. Unknown/failed COMMIT cannot reissue this child.
            token, weight = entries.pop()
            self._claimed[key] = self._claimed.get(key,0)+1
        self._owner._claim_observation(token, weight, deadline)
        self._guard()
        if time.monotonic_ns() > deadline:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',stage='AFTER_COMMIT')
        return Permit(self._owner, token, body['type'], weight, deadline,
                      _guard=self._guard)

    def reserve_extra(self, bodies):
        """Fund both exact siblings of one claimed history page atomically.

        Recursive history keeps its existing 200-read/depth bounds; ordinary
        admission is never a fallback. An uncertain extension COMMIT consumes
        this local parent slot and leaves any inserted tickets charged.
        """
        deadline=min(time.monotonic_ns()+PERMIT_MS*1000000,self._deadline_ns)
        self._guard()
        if not isinstance(bodies,list) or len(bodies)!=2:
            raise BudgetError('TESTNET_OBSERVATION_BATCH_EXTENSION_INVALID')
        fields={'type','user','startTime','endTime','aggregateByTime'}
        for body in bodies:
            if (not isinstance(body,dict) or set(body)!=fields
                    or body['type']!='userFillsByTime' or body['aggregateByTime'] is not False
                    or type(body['startTime']) is not int or type(body['endTime']) is not int
                    or not 0<=body['startTime']<=body['endTime']):
                raise BudgetError('TESTNET_OBSERVATION_BATCH_EXTENSION_INVALID')
        left,right=sorted(bodies,key=lambda body:body['startTime'])
        if (left['user']!=right['user'] or left['endTime']+1!=right['startTime']):
            raise BudgetError('TESTNET_OBSERVATION_BATCH_EXTENSION_INVALID')
        parent={**left,'endTime':right['endTime']}
        key=_observation_key(parent)
        entries=[(_observation_key(body),uuid.uuid4().hex,
                  request_weight('/info',body)) for body in (left,right)]
        with self._lock:
            if self._closed or time.monotonic_ns()>self._deadline_ns:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_EXPIRED_OR_CLOSED')
            if (self._extended.get(key,0)>=self._claimed.get(key,0)
                    or self._count+len(entries)>200):
                raise BudgetError('TESTNET_OBSERVATION_BATCH_EXTENSION_UNDECLARED')
            self._extended[key]=self._extended.get(key,0)+1
            self._count+=len(entries)
        self._owner._fund_observation(entries,self._priority,deadline)
        self._guard()
        if time.monotonic_ns()>deadline:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',stage='AFTER_COMMIT')
        with self._lock:
            if self._closed or time.monotonic_ns()>self._deadline_ns:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_EXPIRED_OR_CLOSED')
            for child_key,token,weight in entries:
                self._entries.setdefault(child_key,[]).append((token,weight))

    def close(self):
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._closed:
                return
            self._closed = True
            # Fence every future acquire before identifying definite non-use.
            # acquire() removes a ticket before its SQL claim, so a racing,
            # failed or uncertain claim is excluded even if no HTTP followed.
            # Issued permits also retain their charge, regardless of whether
            # their local check happened before or after this close.
            unused=[entry for entries in self._entries.values() for entry in entries]
            self._entries.clear()
        if unused:
            self._owner._release_unclaimed_observation(unused)

    def __enter__(self):
        self._guard()
        return self

    def __exit__(self, *exc):
        self.close()


class InfoPlan(ObservationBatch):
    """Finite exact pre-entry reads, separate from lifecycle observation plans.

    These credits are accounting only, never account evidence or permission to
    enter. All child clocks and body multiplicities remain independently fenced.
    There is no pagination or undeclared work in this preflight plan.
    """
    _key=staticmethod(_info_plan_key)

    def reserve_extra(self, bodies):
        raise BudgetError('TESTNET_INFO_PLAN_EXTENSION_NOT_ALLOWED')


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

    def reserve_observation(self, bodies, *, priority='background', host=HOST):
        """Fund every declared two-pass/catchup read atomically before HTTP.

        Undeclared pagination cannot fall back to ordinary admission. A caller
        must plan its bounded work first; a plan too large for the unchanged
        ceiling fails without inserting any partial tickets.
        """
        return self._reserve_plan(bodies,priority,host,_observation_key,ObservationBatch)

    def reserve_info_plan(self, bodies, *, priority='background', host=HOST):
        """Atomically admit an exact finite entry preflight before any HTTP.

        Background keeps its existing 800 ceiling and 400 protection reserve.
        This plan contains no exchange action or lifecycle/history observation;
        missing headroom returns no batch and spends no partial tickets.
        """
        return self._reserve_plan(bodies,priority,host,_info_plan_key,InfoPlan)

    def _reserve_plan(self, bodies, priority, host, key, batch_type):
        started = time.monotonic_ns()
        deadline = started + PERMIT_MS * 1000000
        batch_deadline = started + OBSERVATION_MS * 1000000
        if host != HOST or not isinstance(priority,str) or priority not in PRIORITIES:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PRIORITY_INVALID')
        if not isinstance(bodies,list) or not 1 <= len(bodies) <= 200:
            raise BudgetError('TESTNET_OBSERVATION_BATCH_PLAN_INVALID')
        entries = [(key(body), uuid.uuid4().hex,
                    request_weight('/info',body,host=host)) for body in bodies]
        self._fund_observation(entries,priority,deadline)
        if time.monotonic_ns() > deadline:
            raise BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',stage='AFTER_COMMIT')
        return batch_type(self,entries,priority,batch_deadline)

    def _fund_observation(self, entries, priority, deadline):
        total = sum(weight for _,_,weight in entries)
        ceiling = LIMIT if priority == 'protection' else BACKGROUND_LIMIT
        if total > ceiling:
            raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',stage='ACCOUNTING')
        payload = json.dumps([dict(token=token,weight=weight) for _,token,weight in entries])
        diagnostic = {}
        with self._transaction(deadline_ns=deadline,diagnostic=diagnostic) as conn:
            diagnostic['stage']='GLOBAL_LOCK'
            conn.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK,))
            diagnostic['stage']='ACCOUNTING'
            rows = conn.execute(f'''WITH stamp AS MATERIALIZED (
                    SELECT clock_timestamp() AS at),
                pruned AS (DELETE FROM {TICKETS} USING stamp
                    WHERE admitted_at <= stamp.at-(%s*interval '1 millisecond') RETURNING token),
                used AS (SELECT COALESCE(sum(weight),0) AS total FROM {TICKETS},stamp
                    WHERE admitted_at > stamp.at-(%s*interval '1 millisecond')),
                planned AS (SELECT * FROM jsonb_to_recordset(%s::jsonb)
                    AS child(token text,weight integer))
                INSERT INTO {TICKETS}(token,admitted_at,weight)
                SELECT planned.token,stamp.at,planned.weight FROM stamp,used,planned
                WHERE used.total+%s<=%s RETURNING token''',
                (WINDOW_MS,WINDOW_MS,payload,total,ceiling)).fetchall()
            if len(rows) != len(entries):
                raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')

    def _claim_observation(self, token, weight, deadline):
        diagnostic = {}
        with self._transaction(deadline_ns=deadline,diagnostic=diagnostic) as conn:
            diagnostic['stage']='GLOBAL_LOCK'
            conn.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK,))
            diagnostic['stage']='ACCOUNTING'
            # Refresh only an existing LIVE credit under the admission lock.
            # Its original reservation was already included in the total; the
            # later timestamp keeps the final child covered for a full window.
            row = conn.execute(f'''WITH stamp AS MATERIALIZED (
                    SELECT clock_timestamp() AS at)
                UPDATE {TICKETS} SET admitted_at=stamp.at FROM stamp
                WHERE token=%s AND settled=false AND weight=%s
                    AND admitted_at > stamp.at-(%s*interval '1 millisecond')
                RETURNING token''',(token,weight,WINDOW_MS)).fetchone()
            if row is None:
                raise BudgetError('TESTNET_OBSERVATION_BATCH_CREDIT_UNAVAILABLE')

    def _release_unclaimed_observation(self, entries):
        """Best-effort removal of closed, acknowledged, never-claimed credits.

        No claim, permission or transport can use these exact local tokens after
        the batch close fence. Removal only lowers accounting, so admission's
        advisory lock is unnecessary. Unknown/failed cleanup is not retried and
        cannot make a closed batch usable or erase an issued request's charge.
        """
        try:
            payload=json.dumps([dict(token=token,weight=weight) for token,weight in entries])
            diagnostic={}
            with self._transaction(diagnostic=diagnostic) as conn:
                diagnostic['stage']='REFUND'
                conn.execute(f'''WITH unused AS (
                        SELECT * FROM jsonb_to_recordset(%s::jsonb)
                        AS child(token text,weight integer))
                    DELETE FROM {TICKETS} AS ticket USING unused
                    WHERE ticket.token=unused.token
                        AND ticket.weight=unused.weight
                        AND ticket.settled=false''',(payload,))
            return True
        except BudgetError:
            return False

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
