"""Actual-loader BIGINT rejection/recovery on caller-owned disposable connections.

audit(connect, window_minutes) opens two fresh connections through the supplied
zero-argument factory. That factory owns the native-host/marker/schema guard;
event 1 must already have common-window metrics in the selected schema. This
module never chooses a DSN, creates data, changes the loader or rewrites SQL.
Every loader invocation runs in a server-enforced read-only transaction.
"""
from __future__ import annotations

import hashlib
import json

VERSION = 'ordered-binding-range-check-v1'


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


def _digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'),
                     default=str, ensure_ascii=False, allow_nan=False).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _read_only(conn):
    conn.execute('SET TRANSACTION READ ONLY')
    row = conn.execute('SHOW transaction_read_only').fetchone()
    _require(row == {'transaction_read_only':'on'}, 'Read-only transaction was not established')


def audit(connect, window_minutes):
    """Return only bounded evidence after both actual-loader cases succeed.

Each connection must start IDLE with autocommit disabled. Expected statement
errors are caught inside its context so rollback/reuse can be verified. Any
unexpected exception propagates through context exit, which closes the owned
connection. Full row dictionaries are compared transiently, never returned.
"""
    import psycopg
    from psycopg.pq import TransactionStatus
    import research_common_window_metrics_store as common

    cases, reference_sha, backend_pids = [], None, set()
    for name, value in (('overflow_above_int64', 2**63),
                        ('overflow_below_int64', -(2**63)-1)):
        with connect() as conn:
            _require(not conn.autocommit, 'Range check requires transactional connections')
            before = conn.pgconn.transaction_status
            _require(before == TransactionStatus.IDLE, 'Connection did not start IDLE')
            backend_pid = int(conn.info.backend_pid)
            _require(backend_pid > 0 and backend_pid not in backend_pids,
                     'Each range case requires a fresh backend connection')
            backend_pids.add(backend_pid)

            _read_only(conn)
            reference = common.load_by_event_ids(conn, [1], window_minutes)
            _require(set(reference) == {1}, 'Seeded reference event 1 is missing')
            case_reference_sha = _digest(reference)
            if reference_sha is None:
                reference_sha = case_reference_sha
            _require(case_reference_sha == reference_sha, 'Reference changed between range cases')
            conn.rollback()
            _require(conn.pgconn.transaction_status == TransactionStatus.IDLE,
                     'Reference rollback did not restore IDLE')

            _read_only(conn)
            try:
                common.load_by_event_ids(conn, [value], window_minutes)
            except psycopg.Error as error:
                _require(error.sqlstate == '22003', 'Expected BIGINT overflow SQLSTATE 22003')
                observed_sqlstate, error_class = error.sqlstate, type(error).__name__
            else:
                raise AssertionError('Out-of-range ID was accepted')
            failed_state = conn.pgconn.transaction_status
            _require(failed_state == TransactionStatus.INERROR,
                     'Expected statement error did not leave transaction INERROR')
            conn.rollback()
            recovered_state = conn.pgconn.transaction_status
            _require(recovered_state == TransactionStatus.IDLE,
                     'Error rollback did not restore IDLE')

            _read_only(conn)
            recovered = common.load_by_event_ids(conn, [1], window_minutes)
            _require(recovered == reference and _digest(recovered) == case_reference_sha,
                     'Actual-loader result changed after error recovery')
            conn.rollback()
            _require(conn.pgconn.transaction_status == TransactionStatus.IDLE,
                     'Recovery read did not finish IDLE')
            result = {'case':name, 'input_decimal':str(value), 'backend_pid':backend_pid,
                'before_transaction_state':TransactionStatus(before).name, 'sqlstate':observed_sqlstate,
                'error_class':error_class, 'error_transaction_state':TransactionStatus(failed_state).name,
                'after_rollback_state':TransactionStatus(recovered_state).name, 'final_transaction_state':'IDLE',
                'read_only_transactions_verified':3, 'reference_rows':1,
                'reference_sha256':case_reference_sha, 'recovered_sha256':_digest(recovered),
                'full_result_equal':True}
        _require(conn.closed, 'Owned range connection remained open')
        result['connection_closed'] = True
        cases.append(result)
    return {'version':VERSION, 'result':'PASS', 'actual_loader':True, 'proxy_used':False,
        'connection_count':2, 'fresh_backend_connections':True,
        'database_writes':False, 'read_only_enforced':True,
        'window_minutes':window_minutes, 'reference_sha256':reference_sha,
        'cases':cases}
