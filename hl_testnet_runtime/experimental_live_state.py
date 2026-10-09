"""Explicit TESTNET-domain state, distinct from all software execution fixtures.

Initialization is an administrative operation, never performed by load/mutate.
Transactions hold a single advisory lock and cannot contain exchange callbacks.
A missing/uncertain commit does not return an attempt or signing authority.
"""
from copy import deepcopy
import json

from . import card_lifecycle as life
from .experimental_execution_state import StateError, encode
from .experimental_execution_archive import HistoryMixin
from .postgres_journal import PostgresJournal, STAGING_DB, STAGING_HOST
from .filled_dispatch_store import DispatchStore, SCHEMA as DISPATCH_SCHEMA

VERSION = 'experimental-testnet-execution-state-v1'
PG_SCHEMA = 'hl_experimental_testnet_worker_v1'
LOCK = 1729048461


def initial(routes, not_before_ms):
    life.moment(not_before_ms)
    accounts = {r: life.address(routes[r]['account']) for r in ('long_account', 'short_account')}
    if len(set(accounts.values())) != 2:
        raise StateError('INDEPENDENT_TESTNET_ACCOUNTS_REQUIRED')
    return dict(version=VERSION, domain='testnet', revision=0,
                not_before_ms=not_before_ms, routes=accounts,
                agents={r: life.address(routes[r]['agent']) for r in accounts},
                sources={}, trades={}, requests={}, snapshots={}, events=[])


def checked(raw, checksum):
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
        if (life.digest(value) != checksum or value.get('version') != VERSION
                or value.get('domain') != 'testnet'
                or type(value.get('revision')) is not int or value['revision'] < 0
                or any(s.get('domain') != 'testnet' for s in value['sources'].values())
                or any(r.get('domain') != 'testnet' for r in value['requests'].values())):
            raise ValueError()
        return deepcopy(value)
    except (ValueError, KeyError, TypeError):
        raise StateError('TESTNET_STATE_INTEGRITY_FAILURE') from None


class TestnetExecutionState(HistoryMixin):
    """Persist only into the selected Testnet journal's separate schema.

    for_ci is explicit loopback support for tests of the TESTNET data format;
    it never reopens or translates a software-domain state.
    """
    domain = 'testnet'

    def __init__(self, journal):
        if (type(journal) is not PostgresJournal or journal._ci is not False
                or journal._parameters.get('host') != STAGING_HOST
                or journal._parameters.get('dbname') != STAGING_DB):
            raise StateError('EXPLICIT_TESTNET_POSTGRES_JOURNAL_REQUIRED')
        self.journal = journal
        self.ci_only = False

    @classmethod
    def for_ci(cls, journal):
        if (type(journal) is not PostgresJournal or journal._ci is not True
                or journal._parameters.get('host') not in ('localhost', '127.0.0.1')
                or journal._parameters.get('dbname') != 'hl_journal_ci'
                or journal._parameters.get('sslmode') != 'disable'):
            raise StateError('LOOPBACK_CI_POSTGRES_REQUIRED')
        self = cls.__new__(cls)
        self.journal, self.ci_only = journal, True
        return self

    def _ready(self, conn):
        row = conn.execute(f'SELECT version,domain FROM {PG_SCHEMA}.metadata WHERE singleton').fetchone()
        if row != (VERSION, 'testnet'):
            raise StateError('EXPLICIT_TESTNET_INITIALIZATION_REQUIRED')

    def initialize(self, routes, *, not_before_ms):
        expected = initial(routes, not_before_ms)
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            DispatchStore(self.journal).ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            exists = conn.execute('SELECT to_regnamespace(%s)', (PG_SCHEMA,)).fetchone()[0]
            if exists is None:
                conn.execute(f'CREATE SCHEMA {PG_SCHEMA}')
                conn.execute(f'CREATE TABLE {PG_SCHEMA}.metadata(singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL, domain text NOT NULL)')
                conn.execute(f'CREATE TABLE {PG_SCHEMA}.state(singleton boolean PRIMARY KEY CHECK(singleton), value jsonb NOT NULL, checksum text NOT NULL)')
                conn.execute(f'INSERT INTO {PG_SCHEMA}.metadata VALUES(true,%s,%s)', (VERSION, 'testnet'))
                conn.execute(f'INSERT INTO {PG_SCHEMA}.state VALUES(true,%s::jsonb,%s)', (encode(expected), life.digest(expected)))
                conn.execute(f'REVOKE ALL ON SCHEMA {PG_SCHEMA} FROM PUBLIC')
                conn.execute(f'REVOKE ALL ON ALL TABLES IN SCHEMA {PG_SCHEMA} FROM PUBLIC')
            else:
                self._ready(conn)
                row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton').fetchone()
                if row is None:
                    raise StateError('EXPLICIT_TESTNET_INITIALIZATION_REQUIRED')
                actual = checked(*row)
                if (actual['routes'] != expected['routes'] or actual['agents'] != expected['agents']
                        or actual['not_before_ms'] != not_before_ms):
                    raise StateError('TESTNET_ROUTE_OR_ACTIVATION_CHANGED')

    def load(self):
        with self.journal._transaction() as conn:
            self._ready(conn)
            row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton').fetchone()
            if row is None:
                raise StateError('EXPLICIT_TESTNET_INITIALIZATION_REQUIRED')
            return checked(*row)

    def mutate(self, transition):
        return self._mutate(transition)

    def commit_attempt(self, transition, *, role, now_ms):
        """Allocate from the same per-agent nonce table as existing dispatch."""
        if role not in ('long_account', 'short_account'):
            raise StateError('EXACT_TESTNET_ROLE_REQUIRED')
        life.moment(now_ms)
        return self._mutate(transition, nonce_role=role, nonce_floor_ms=now_ms)

    def _mutate(self, transition, *, nonce_role=None, nonce_floor_ms=None):
        with self.journal._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            self._ready(conn)
            row = conn.execute(f'SELECT value,checksum FROM {PG_SCHEMA}.state WHERE singleton FOR UPDATE').fetchone()
            if row is None:
                raise StateError('EXPLICIT_TESTNET_INITIALIZATION_REQUIRED')
            value = checked(*row)
            immutable = {k: deepcopy(value[k]) for k in ('version', 'domain', 'routes', 'agents', 'not_before_ms', 'revision')}
            if nonce_role is None:
                result = transition(value)
            else:
                DispatchStore(self.journal).ready(conn)
                nonce = conn.execute(f'''INSERT INTO {DISPATCH_SCHEMA}.nonces VALUES(%s,%s)
                    ON CONFLICT(agent) DO UPDATE SET nonce=GREATEST({DISPATCH_SCHEMA}.nonces.nonce+1,EXCLUDED.nonce)
                    RETURNING nonce''', (value['agents'][nonce_role], nonce_floor_ms)).fetchone()[0]
                if type(nonce) is not int or not nonce_floor_ms <= nonce <= nonce_floor_ms + 1000:
                    raise StateError('DURABLE_NONCE_WINDOW_EXHAUSTED_DEFER')
                result = transition(value, nonce)
            if any(value.get(k) != v for k, v in immutable.items()):
                raise StateError('TESTNET_STATE_IDENTITY_CHANGED')
            value['revision'] += 1
            checksum = life.digest(value)
            checked(value, checksum)
            conn.execute(f'UPDATE {PG_SCHEMA}.state SET value=%s::jsonb,checksum=%s WHERE singleton', (encode(value), checksum))
        return deepcopy(result)

    def request(self, request_id):
        """Exact committed attempt for the dispatch boundary's repeated check."""
        value = self.load()['requests'].get(request_id)
        if value is None:
            raise StateError('EXACT_DURABLE_ATTEMPT_REQUIRED')
        return value

    def claim_transport(self, request, claim_token):
        """One durable wire claim, including across processes and restarts.

        Only a successful committed return may authorize signing. A lost COMMIT
        reply leaves the claim set and prevents re-use of this attempt forever.
        """
        life.ident(claim_token, r'[0-9a-f]{32}')
        if request.get('domain') != 'testnet' or 'transport_claim' in request:
            raise StateError('UNCLAIMED_TESTNET_ATTEMPT_REQUIRED')
        from .filled_dispatch_store import DefinitelyUnsent
        DefinitelyUnsent.identity(request)
        def transition(value):
            current = value['requests'].get(request['request_id'])
            if current != request or 'transport_claim' in current:
                raise StateError('EXACT_UNCLAIMED_DURABLE_ATTEMPT_REQUIRED')
            current['transport_claim'] = claim_token
            return current
        return self.mutate(transition)
