"""Durable prospective experimental plans, separate from executable legacy cards.

This inbox has no exchange, signer, scheduler or implicit schema initialization.
Source cancellation retires *entry permission*, never an owned filled position.
The software domain used by local tests cannot be reopened as a Testnet journal.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re

from .postgres_journal import JournalError

SCHEMA = 'hl_testnet_experimental_plans_v1'
VERSION = 'experimental-plan-store-v1'
LOCK = 1729048291
HEX = re.compile(r'[0-9a-f]{64}\Z')


class PlanStoreError(JournalError):
    """Fixed local error codes; do not return source or infrastructure details."""


def encoded(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        if len(raw.encode()) > 262144:
            raise ValueError()
        return raw
    except (ValueError, TypeError, RecursionError):
        raise PlanStoreError('EXPERIMENTAL_RECORD_INVALID') from None


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def moment(value):
    if not isinstance(value, str) or len(value) > 40:
        raise PlanStoreError('EXPERIMENTAL_TIME_REQUIRED')
    try:
        at = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if at.utcoffset() is None:
            raise ValueError()
        return at.astimezone(timezone.utc)
    except ValueError:
        raise PlanStoreError('EXPERIMENTAL_TIME_REQUIRED') from None


def immutable(message):
    return {key: deepcopy(value) for key, value in message.items()
            if key not in ('kind', 'source_sequence', 'source_as_of', 'source_state',
                           'valid_until', 'cancel_reason')}


def source_digest(message):
    from experimental_execution_contract import immutable as contract_immutable
    value = contract_immutable(message)
    value.update({key: message[key] for key in ('kind', 'source_sequence', 'source_as_of',
                 'source_state', 'valid_until', 'cancel_reason')})
    return digest(value)


def reduce_source(previous, message, *, now, not_before, domain):
    """Atomic input transition; retries do not reset clocks or resume a latch."""
    from experimental_execution_contract import validate, plan_digest
    message = validate(message)
    if domain not in ('software', 'testnet'):
        raise PlanStoreError('EXPERIMENTAL_DOMAIN_INVALID')
    now, not_before = moment(now), moment(not_before)
    if not_before > now or moment(message['source_as_of']) > now:
        raise PlanStoreError('EXPERIMENTAL_FUTURE_SOURCE')
    immutable_digest = plan_digest(message)
    sequence = message['source_sequence']
    if previous is None:
        if message['kind'] == 'HEARTBEAT':
            raise PlanStoreError('EXPERIMENTAL_PLAN_NOT_RECEIVED_BEFORE_ARM')
        source_fresh = (moment(message['created_at']) >= not_before
                        and moment(message['valid_until']) > now
                        and (message['expires_at'] is None
                             or moment(message['expires_at']) > now))
        if message['kind'] == 'PLAN':
            if not source_fresh:
                raise PlanStoreError('EXPERIMENTAL_PLAN_STALE')
            if message['family'] == 'maxpain' and now >= moment(message['arm_at']):
                raise PlanStoreError('EXPERIMENTAL_PLAN_NOT_RECEIVED_BEFORE_ARM')
            if (message['family'] == 'sol_g65'
                    and now >= moment(message['source_at']) + timedelta(minutes=1)):
                # This is a first-delivery freshness boundary, not a holding
                # or pending timeout. Already recorded g65 plans have no
                # formula expiry and continue only through source heartbeats.
                raise PlanStoreError('EXPERIMENTAL_PLAN_NOT_RECEIVED_BEFORE_ARM')
        # A cancellation arriving first writes a tombstone. An old PLAN can
        # never win the race by arriving after it, even with a newer sequence.
        state = dict(version=VERSION, domain=domain,
            occurrence_id=message['occurrence_id'], revision=0,
            plan=immutable(message), plan_digest=immutable_digest,
            initial_source=deepcopy(message), cancellation=None,
            source=deepcopy(message), source_digest=source_digest(message),
            source_sequence=sequence, entry_permission='WAITING',
            terminal_reason=None, strategy=None, created_at=now.isoformat(),
            updated_at=now.isoformat())
    else:
        state = deepcopy(previous)
        if (state['version'] != VERSION or state['domain'] != domain
                or state['occurrence_id'] != message['occurrence_id']
                or state['plan_digest'] != immutable_digest):
            raise PlanStoreError('EXPERIMENTAL_IMMUTABLE_PLAN_CHANGED')
        if now < moment(state['updated_at']):
            raise PlanStoreError('EXPERIMENTAL_CLOCK_REGRESSION')
        if message['kind'] == 'CANCEL' and state.get('cancellation') is not None:
            # The first validated cancellation is a permanent tombstone. A
            # retry (including an older sequence after a lost COMMIT reply)
            # cannot manufacture revisions or change its recorded reason.
            return state, False
        lease_retired = False
        if (state['entry_permission'] == 'WAITING'
                and now >= moment(state['source']['valid_until'])):
            state['entry_permission'] = 'RETIRED'
            state['terminal_reason'] = 'SOURCE_LEASE_EXPIRED'
            lease_retired = True
        if sequence == state['source_sequence']:
            if state['source_digest'] != source_digest(message):
                raise PlanStoreError('EXPERIMENTAL_SOURCE_SEQUENCE_CONFLICT')
            if lease_retired:
                state['revision'] += 1
                state['updated_at'] = now.isoformat()
            return state, lease_retired
        if sequence < state['source_sequence'] and message['kind'] != 'CANCEL':
            if lease_retired:
                state['revision'] += 1
                state['updated_at'] = now.isoformat()
            return state, lease_retired
        # Permission is not restored after a lease gap. Restoring an original
        # source plan needs a separately audited protocol, not a fresh receipt.
        if sequence > state['source_sequence']:
            state['source'] = deepcopy(message)
            state['source_digest'] = source_digest(message)
            state['source_sequence'] = sequence
    if message['kind'] == 'CANCEL':
        state['entry_permission'] = 'RETIRED'
        state['terminal_reason'] = message['cancel_reason']
        state['cancellation'] = deepcopy(message)
    elif message['expires_at'] is not None and now >= moment(message['expires_at']):
        state['entry_permission'] = 'RETIRED'
        state['terminal_reason'] = 'PLAN_EXPIRED'
    elif now >= moment(message['valid_until']):
        state['entry_permission'] = 'RETIRED'
        state['terminal_reason'] = 'SOURCE_LEASE_EXPIRED'
    state['revision'] += 1
    state['updated_at'] = now.isoformat()
    return state, True


class PlanStore:
    def __init__(self, journal):
        self.journal = journal
        self.domain = 'software' if journal._ci else 'testnet'

    def ready(self, conn):
        if conn.execute(f'SELECT version,domain FROM {SCHEMA}.metadata WHERE singleton').fetchone() != (VERSION, self.domain):
            raise PlanStoreError('EXPERIMENTAL_SCHEMA_OR_DOMAIN_MISMATCH')

    def initialize(self):
        """Explicit startup/migration only; never called from HTTP or hot path."""
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            if conn.execute('SELECT to_regnamespace(%s)', (SCHEMA,)).fetchone()[0] is not None:
                self.ready(conn)
                return False
            conn.execute(f'CREATE SCHEMA {SCHEMA}')
            conn.execute(f'CREATE TABLE {SCHEMA}.metadata(singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL, domain text NOT NULL)')
            conn.execute(f'INSERT INTO {SCHEMA}.metadata VALUES(true,%s,%s)', (VERSION, self.domain))
            conn.execute(f'''CREATE TABLE {SCHEMA}.plans(
                occurrence_id text PRIMARY KEY CHECK(length(occurrence_id)=64),
                revision bigint NOT NULL, value jsonb NOT NULL, digest text NOT NULL)''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.events(
                occurrence_id text NOT NULL REFERENCES {SCHEMA}.plans,
                revision bigint NOT NULL, event text NOT NULL, at_utc timestamptz NOT NULL,
                record_hash text NOT NULL, PRIMARY KEY(occurrence_id,revision))''')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
            for name in ('metadata', 'plans', 'events'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{name} FROM PUBLIC')
        return True

    def _checked(self, row):
        if row is None:
            return None
        state, checksum, revision = row
        if (digest(state) != checksum or state.get('version') != VERSION
                or state.get('domain') != self.domain or state.get('revision') != revision):
            raise PlanStoreError('EXPERIMENTAL_STATE_INTEGRITY_FAILURE')
        return deepcopy(state)

    def load(self, identity):
        if not isinstance(identity, str) or not HEX.fullmatch(identity):
            raise PlanStoreError('EXPERIMENTAL_IDENTITY_INVALID')
        with self.journal._transaction() as conn:
            self.ready(conn)
            return self._checked(conn.execute(f'SELECT value,digest,revision FROM {SCHEMA}.plans WHERE occurrence_id=%s', (identity,)).fetchone())

    def _save(self, conn, state, event, now):
        conn.execute(f'''INSERT INTO {SCHEMA}.plans VALUES(%s,%s,%s::jsonb,%s)
            ON CONFLICT(occurrence_id) DO UPDATE SET revision=EXCLUDED.revision,value=EXCLUDED.value,digest=EXCLUDED.digest''',
            (state['occurrence_id'], state['revision'], encoded(state), digest(state)))
        conn.execute(f'INSERT INTO {SCHEMA}.events VALUES(%s,%s,%s,%s,%s)',
            (state['occurrence_id'], state['revision'], event, now, digest(state)))

    def ingest(self, message, *, now, not_before):
        from experimental_execution_contract import validate
        message = validate(message)
        with self.journal._transaction() as conn:
            self.ready(conn)
            # Global inbox serialization covers the absent-row race as well
            # as existing rows. No exchange work is done while it is held.
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            previous = self._checked(conn.execute(f'SELECT value,digest,revision FROM {SCHEMA}.plans WHERE occurrence_id=%s FOR UPDATE', (message['occurrence_id'],)).fetchone())
            state, changed = reduce_source(previous, message, now=now, not_before=not_before, domain=self.domain)
            if changed:
                self._save(conn, state, 'SOURCE_' + message['kind'], now)
        # Return only after the transaction positively acknowledges COMMIT.
        return dict(occurrence_id=state['occurrence_id'], revision=state['revision'],
                    status='RECORDED' if changed else 'DUPLICATE', record_only=True,
                    entry_permission=state['entry_permission'])

    def change_strategy(self, identity, revision, strategy, *, now):
        """CAS for locally tested reducers; cannot grant source permission."""
        moment(now)
        if not isinstance(identity, str) or not HEX.fullmatch(identity):
            raise PlanStoreError('EXPERIMENTAL_IDENTITY_INVALID')
        if type(revision) is not int or revision < 1 or not isinstance(strategy, dict):
            raise PlanStoreError('EXPERIMENTAL_STRATEGY_REVISION_REQUIRED')
        encoded(strategy)
        with self.journal._transaction() as conn:
            self.ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            state = self._checked(conn.execute(f'SELECT value,digest,revision FROM {SCHEMA}.plans WHERE occurrence_id=%s FOR UPDATE', (identity,)).fetchone())
            if state is None or state['revision'] != revision:
                raise PlanStoreError('EXPERIMENTAL_CONCURRENT_RELOAD_REQUIRED')
            if moment(now) < moment(state['updated_at']):
                raise PlanStoreError('EXPERIMENTAL_CLOCK_REGRESSION')
            state['strategy'] = deepcopy(strategy)
            state['revision'] += 1
            state['updated_at'] = now
            self._save(conn, state, 'LOCAL_STRATEGY_STATE', now)
        return state
