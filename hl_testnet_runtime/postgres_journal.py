"""Durable TESTNET-only preparation and at-most-one dispatch reservation.

Dedicated tables in the existing STAGING database, never the production DB.
No exchange imports, credentials in records, deletion or automatic replay.
A failed/uncertain commit is NOT permission to sign. The Free staging database
expires: surviving application restarts is not a backup or indefinite retention.
"""
from __future__ import annotations
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import unquote, urlsplit, parse_qs

SCHEMA = 'hl_testnet_execution_v1'
VERSION = 'testnet-postgres-journal-v1'
STAGING_HOST = 'dpg-dab7rc2d0e5s73dkb9l0-a'
STAGING_DB = 'crypto_intelligence_staging_db'
EXPIRES = datetime(2026, 10, 1, 7, 24, 32, tzinfo=timezone.utc)
ADDRESS = re.compile(r'0x[0-9a-fA-F]{40}\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')
LOCK = 1729048159


class JournalError(ValueError):
    """Fixed codes only. Never attach SQL, connection URL or remote exceptions."""


def canonical(value):
    try:
        text = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        if len(text.encode()) > 32768:
            raise ValueError()
        return text
    except (ValueError, TypeError):
        raise JournalError('INVALID_JOURNAL_RECORD') from None


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def account_address(value):
    if not isinstance(value, str) or not ADDRESS.fullmatch(value) or int(value[2:], 16) == 0:
        raise JournalError('INVALID_JOURNAL_ACCOUNT')
    return value.lower()


def connection_parameters(value):
    """Only the internal URL of the explicitly selected staging DB is allowed."""
    try:
        if not isinstance(value, str) or not 1 <= len(value) <= 2048:
            raise ValueError()
        u = urlsplit(value)
        query = parse_qs(u.query, strict_parsing=True)
        if (u.scheme not in ('postgres', 'postgresql') or u.hostname != STAGING_HOST
                or u.port not in (None, 5432) or u.path != '/' + STAGING_DB
                or u.username != STAGING_DB + '_user' or not u.password or u.fragment
                or query not in ({}, {'sslmode': ['require']})):
            raise ValueError()
        return dict(host=STAGING_HOST, port=5432, dbname=STAGING_DB,
                    user=unquote(u.username), password=unquote(u.password), sslmode='require')
    except (ValueError, TypeError):
        raise JournalError('STAGING_INTERNAL_DATABASE_URL_REQUIRED') from None


def validate_prepared(prepared):
    from .price_precision import prepare_signal
    try:
        if not isinstance(prepared, dict) or set(prepared) != {'source', 'execution', 'audit'}:
            raise ValueError()
        source = prepared['source']
        audit = prepared['audit']
        metadata = {'universe': [{'name': source['symbol'], 'szDecimals': audit['sz_decimals']}]}
        expected = prepare_signal(source, metadata)
        if canonical(expected) != canonical(prepared):
            raise ValueError()
        return json.loads(canonical(prepared))
    except Exception:
        raise JournalError('INVALID_OR_CHANGED_PREPARED_RECORD') from None


def safe_result(value):
    """Store only receipt status/flags; full immutable action is stored separately."""
    if not isinstance(value, dict):
        raise JournalError('INVALID_RECEIPT_SUMMARY')
    status = value.get('status')
    if not isinstance(status, str) or not re.fullmatch(r'[A-Z_]{1,80}', status):
        raise JournalError('INVALID_RECEIPT_SUMMARY')
    if type(value.get('verified')) is not bool:
        raise JournalError('INVALID_RECEIPT_SUMMARY')
    result = {'status': status, 'verified': value['verified']}
    if 'protection_active' in value:
        if type(value['protection_active']) is not bool:
            raise JournalError('INVALID_RECEIPT_SUMMARY')
        result['protection_active'] = value['protection_active']
    return result


def validate_action(action, prepared, account):
    """Exact current OR historical reconstruction, never resize a stored order.

    Retain the old $20 profile only to read/inspect/cancel already stored actions.
    A new reservation, signature or transport still enforces the current $10.
    """
    import hyperliquid_testnet_executor as sender
    from .risk_policy import LEGACY_RISK_USD
    try:
        orders = action['orders']
        asset = orders[0]['a']
        if type(asset) is not int or not 0 <= asset < 10000:
            raise ValueError()
        tp_market = orders[1]['t']['trigger']['isMarket']
        sl_market = orders[2]['t']['trigger']['isMarket']
        if type(tp_market) is not bool or type(sl_market) is not bool:
            raise ValueError()
        modes = {(False, False): 'limit', (True, True): 'market',
                 (False, True): 'tp_limit_sl_market'}
        exit_type = modes.get((tp_market, sl_market))
        if exit_type is None:
            raise ValueError()
        universe = [{'name': '_unused'} for _ in range(asset)]
        universe.append({'name': prepared['execution']['symbol'],
                         'szDecimals': prepared['audit']['sz_decimals']})
        for profile in (None, LEGACY_RISK_USD):
            try:
                expected = sender.build_action(prepared['execution'], {'universe': universe}, account,
                    exit_type=exit_type, historical_risk_usd=profile)
            except sender.TestnetError:
                continue
            if canonical(expected) == canonical(action):
                return json.loads(canonical(action))
        raise ValueError()
    except Exception:
        raise JournalError('ACTION_NOT_BOUND_TO_ROUNDED_RECORD') from None


class PostgresJournal:
    def __init__(self, parameters, *, _ci=False):
        self._parameters = dict(parameters)
        self._ci = _ci
        if not _ci and (parameters.get('host') != STAGING_HOST or parameters.get('dbname') != STAGING_DB):
            raise JournalError('STAGING_DATABASE_REQUIRED')

    @classmethod
    def from_env(cls, env):
        return cls(connection_parameters(env.get('HL_TESTNET_DATABASE_URL', '')))

    @classmethod
    def for_ci(cls, dsn):
        """Disposable loopback PostgreSQL only; cannot point to a user database."""
        u = urlsplit(dsn)
        if u.scheme != 'postgresql' or u.hostname not in ('127.0.0.1', 'localhost') or u.path != '/hl_journal_ci':
            raise JournalError('DISPOSABLE_CI_DATABASE_REQUIRED')
        return cls(dict(host=u.hostname, port=u.port or 5432, dbname='hl_journal_ci',
                        user=u.username, password=u.password, sslmode='disable'), _ci=True)

    @contextmanager
    def _transaction(self):
        if not self._ci and datetime.now(timezone.utc) >= EXPIRES:
            raise JournalError('STAGING_STORAGE_EXPIRED_DO_NOT_SEND')
        try:
            import psycopg
            with psycopg.connect(**self._parameters, connect_timeout=4,
                    options='-c statement_timeout=5000 -c lock_timeout=3000 -c idle_in_transaction_session_timeout=5000 -c synchronous_commit=on') as conn:
                actual = conn.execute('SELECT current_database()').fetchone()[0]
                if actual != self._parameters['dbname']:
                    raise JournalError('WRONG_DATABASE')
                yield conn
                # Connection context COMMIT is completed before the method returns.
        except JournalError:
            raise
        except Exception:
            raise JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND') from None

    def _ready(self, conn):
        row = conn.execute(f'SELECT version FROM {SCHEMA}.metadata WHERE singleton=true').fetchone()
        if row != (VERSION,):
            raise JournalError('JOURNAL_SCHEMA_REQUIRES_REVIEW')
        # Never repair/recreate missing tables while a dispatcher is running.
        for table in ('prepared', 'attempts'):
            if conn.execute('SELECT to_regclass(%s)', (SCHEMA + '.' + table,)).fetchone()[0] is None:
                raise JournalError('JOURNAL_SCHEMA_REQUIRES_REVIEW')

    def bootstrap(self):
        """Explicit startup setup of our namespace only. No existing schema changes."""
        with self._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            exists = conn.execute('SELECT to_regnamespace(%s)', (SCHEMA,)).fetchone()[0]
            if exists is not None:
                self._ready(conn)
                return False
            conn.execute(f'CREATE SCHEMA {SCHEMA}')
            conn.execute(f'''CREATE TABLE {SCHEMA}.metadata (
                singleton boolean PRIMARY KEY CHECK (singleton), version text NOT NULL,
                probe_visits bigint NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
            conn.execute(f'INSERT INTO {SCHEMA}.metadata(singleton,version) VALUES(true,%s)', (VERSION,))
            conn.execute(f'''CREATE TABLE {SCHEMA}.prepared (
                plan_key text PRIMARY KEY CHECK(length(plan_key)=64), account text NOT NULL,
                source_id text NOT NULL, digest text NOT NULL, manifest jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(account,source_id))''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.attempts (
                account text PRIMARY KEY, plan_key text UNIQUE NOT NULL REFERENCES {SCHEMA}.prepared(plan_key),
                action jsonb NOT NULL, nonce bigint NOT NULL, result jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
            for table in ('metadata', 'prepared', 'attempts'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
        return True

    def readiness_probe(self):
        """Commit a harmless counter then verify from a separate connection."""
        with self._transaction() as conn:
            self._ready(conn)
            visits = conn.execute(f'UPDATE {SCHEMA}.metadata SET probe_visits=probe_visits+1 WHERE singleton=true RETURNING probe_visits').fetchone()[0]
        with self._transaction() as conn:
            self._ready(conn)
            actual = conn.execute(f'SELECT probe_visits FROM {SCHEMA}.metadata WHERE singleton=true').fetchone()[0]
            if actual < visits:
                raise JournalError('COMMITTED_RECORD_NOT_VISIBLE')
        return {'status': 'STORAGE_WRITE_READ_VERIFIED', 'previous_probe_seen': visits > 1,
                'separate_connection_verified': True, 'order_requests_sent': 0}

    def save_prepared(self, account, prepared):
        account = account_address(account)
        manifest = validate_prepared(prepared)
        source_id = manifest['source']['event_id']
        key = digest(['testnet', account, source_id])
        checksum = digest(manifest)
        with self._transaction() as conn:
            self._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            row = conn.execute(f'SELECT digest FROM {SCHEMA}.prepared WHERE plan_key=%s', (key,)).fetchone()
            if row:
                if row[0] != checksum:
                    raise JournalError('SOURCE_OR_ROUNDING_CHANGED_NO_OVERWRITE')
                return key, False
            if conn.execute(f'SELECT count(*) FROM {SCHEMA}.prepared').fetchone()[0] >= 1000:
                raise JournalError('TEST_JOURNAL_CAPACITY_REACHED')
            conn.execute(f'INSERT INTO {SCHEMA}.prepared(plan_key,account,source_id,digest,manifest) VALUES(%s,%s,%s,%s,%s::jsonb)',
                         (key, account, source_id, checksum, canonical(manifest)))
        return key, True

    def load(self, key):
        if not isinstance(key, str) or not HEX.fullmatch(key):
            raise JournalError('INVALID_RECORD_ID')
        with self._transaction() as conn:
            self._ready(conn)
            row = conn.execute(f'''SELECT p.account,p.manifest,p.digest,a.action,a.nonce,a.result
                FROM {SCHEMA}.prepared p LEFT JOIN {SCHEMA}.attempts a ON a.plan_key=p.plan_key
                WHERE p.plan_key=%s''', (key,)).fetchone()
            if row is None:
                raise JournalError('UNKNOWN_PREPARED_RECORD')
            if digest(row[1]) != row[2]:
                raise JournalError('STORED_RECORD_CHECKSUM_MISMATCH')
            return {'account': row[0], 'prepared': validate_prepared(row[1]),
                    'action': row[3], 'nonce': row[4], 'result': row[5]}

    def reserve(self, key, action):
        record = self.load(key)
        account = record['account']
        action = validate_action(action, record['prepared'], account)
        with self._transaction() as conn:
            self._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            row = conn.execute(f'SELECT plan_key,action,nonce,result FROM {SCHEMA}.attempts WHERE account=%s', (account,)).fetchone()
            if row:
                if row[0] != key:
                    raise JournalError('TEST_ACCOUNT_ALREADY_RESERVED')
                if canonical(row[1]) != canonical(action):
                    raise JournalError('RESERVED_ACTION_CHANGED_NO_RESEND')
                return False, row[2], row[3]
            from .risk_policy import assert_new_entry_budget, RiskError
            try:
                assert_new_entry_budget(action)
            except RiskError:
                raise JournalError('NEW_ENTRY_EXCEEDS_CURRENT_RISK_POLICY') from None
            nonce = conn.execute('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint').fetchone()[0]
            result = {'status': 'RESERVED_OR_UNCERTAIN', 'verified': False}
            conn.execute(f'INSERT INTO {SCHEMA}.attempts(account,plan_key,action,nonce,result) VALUES(%s,%s,%s::jsonb,%s,%s::jsonb)',
                         (account, key, canonical(action), nonce, canonical(result)))
        return True, nonce, result

    def save_result(self, key, result):
        result = safe_result(result)
        with self._transaction() as conn:
            self._ready(conn)
            changed = conn.execute(f'UPDATE {SCHEMA}.attempts SET result=%s::jsonb,updated_at=clock_timestamp() WHERE plan_key=%s',
                                   (canonical(result), key)).rowcount
            if changed != 1:
                raise JournalError('MISSING_RESERVATION')
