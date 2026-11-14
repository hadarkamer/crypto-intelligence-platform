#!/usr/bin/env python3
"""Focused real-PostgreSQL ownership checks; synthetic state only.

The runner starts/stops its private cluster. This file never installs PostgreSQL,
contacts a remote database, changes product code, or creates a permanent service.
It leaves the empty candidate table and temporary roles for check_path.py; the
runner must stop and delete the whole temporary cluster at the end of the job.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from uuid import uuid4

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'standalone_collector'))
from session_state import Lease, PgSessionStore, SessionStateError, WORK_LOCK

DATABASE = 'cg_session_test'
WRITER = 'cg_session_writer'
FRONTEND = 'cg_session_frontend'
KEY = bytes(range(32))  # Public synthetic fixture, never a deployment key.
STATE = {'cookies': [{'name': 'dummy_session', 'value': 'DUMMY_INITIAL_STATE',
    'domain': '.coinglass.com', 'path': '/', 'expires': -1,
    'httpOnly': True, 'secure': True, 'sameSite': 'Lax'}], 'origins': []}


class CheckFailure(Exception):
    pass


def require(condition, code):
    if not condition:
        raise CheckFailure(code)


def _private_directory(raw):
    require(isinstance(raw, str) and raw.startswith('/'), 'local_directory_required')
    original = Path(raw)
    require(not original.is_symlink(), 'local_directory_symlink')
    path = original.resolve(strict=True)
    info = path.stat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) & 0o077 == 0, 'local_directory_not_private')
    return path


def _parameters():
    require(os.geteuid() != 0, 'nonroot_test_user_required')
    require(not any(os.environ.get(name) for name in
                    ('PGPASSWORD', 'PGPASSFILE', 'PGSERVICE', 'PGSERVICEFILE')),
            'ambient_database_credentials_forbidden')
    raw = os.environ.get('CG_TEST_DATABASE_URL', '')
    require(bool(raw), 'local_database_configuration_missing')
    try:
        params = conninfo_to_dict(raw)
    except Exception:
        raise CheckFailure('local_database_configuration_invalid') from None
    require(set(params) <= {'host', 'port', 'dbname', 'user', 'application_name',
                            'connect_timeout', 'options', 'sslmode'},
            'unexpected_database_parameter')
    require(params.get('dbname') == DATABASE and bool(params.get('user')),
            'local_database_identity_invalid')
    socket = _private_directory(params.get('host'))
    _private_directory(os.environ.get('CG_TEST_PGDATA'))
    params['host'] = str(socket)
    require(str(params.get('port', '5432')).isdecimal(), 'local_database_port_invalid')
    return params


def writer_dsn():
    """Return only to the test parent; never print or pass into its browser child."""
    params = _parameters()
    params['user'] = WRITER
    return make_conninfo(**params)


def _connect(role, application_name, *, autocommit=False):
    params = _parameters()
    if role != 'admin':
        require(role in (WRITER, FRONTEND), 'unexpected_test_role')
        params['user'] = role
    return psycopg.connect(make_conninfo(**params), row_factory=dict_row,
        autocommit=autocommit, connect_timeout=3, application_name=application_name,
        options='-c statement_timeout=3000 -c lock_timeout=1000')


def prepare_database(application_name='cg-session-setup'):
    """Create fixtures only after verifying the exact disposable local cluster.

    Returns an admin connection owned by the caller. It deliberately refuses an
    existing candidate table; this is a single setup, not a cleanup/reset command.
    """
    admin = _connect('admin', application_name, autocommit=True)
    try:
        identity = admin.execute("""SELECT current_database() AS db,
            inet_server_addr() IS NULL AS unix_socket,
            current_setting('data_directory') AS directory,
            current_setting('server_version') AS version""").fetchone()
        require(identity['db'] == DATABASE and identity['unix_socket'] is True
                and Path(identity['directory']).resolve() ==
                    _private_directory(os.environ.get('CG_TEST_PGDATA')),
                'unexpected_database_target')
        require(admin.execute("SELECT to_regclass('collector_private.coinglass_session_state') AS table_id")
                .fetchone()['table_id'] is None, 'fixture_already_exists')
        for role in (WRITER, FRONTEND):
            require(admin.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone() is None,
                    'test_role_already_exists')
            admin.execute(f'CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS')
        admin.execute((ROOT / 'standalone_collector/session_state.sql').read_text())
        admin.execute(f'GRANT USAGE ON SCHEMA collector_private TO {WRITER}')
        admin.execute(f'GRANT SELECT,INSERT,UPDATE ON collector_private.coinglass_session_state TO {WRITER}')
        # The real app initializes its existing public job tables. This privilege
        # is confined to this disposable database, not the private state schema.
        admin.execute(f'GRANT USAGE,CREATE ON SCHEMA public TO {WRITER}')
        admin.execute(f'REVOKE ALL ON SCHEMA collector_private FROM {FRONTEND}')
        admin.execute(f'REVOKE ALL ON collector_private.coinglass_session_state FROM {FRONTEND}')
        return admin
    except BaseException:
        admin.close()
        raise


def _lock(connection):
    row = connection.execute('SELECT pg_try_advisory_lock(%s) AS held', (WORK_LOCK,)).fetchone()
    connection.commit()  # Match the existing worker's committed claim connection.
    return row['held'] is True


def _wait_for_released_lock(connection):
    deadline = time.monotonic() + 2
    while True:
        if _lock(connection): return True
        if time.monotonic() >= deadline: return False
        time.sleep(0.05)


def _expect_session(code, action):
    try:
        action()
    except SessionStateError as error:
        require(error.code == code and str(error) == code, 'unexpected_session_error')
    else:
        raise CheckFailure('expected_session_rejection_missing')


def _expect_permission(connection, query):
    try:
        connection.execute(query)
    except psycopg.errors.InsufficientPrivilege:
        connection.rollback()
    else:
        connection.rollback()
        raise CheckFailure('unexpected_database_permission')


def run_checks():
    prefix = 'cg-session-pgcheck-' + uuid4().hex[:12]
    result = {'status': 'failed', 'independent_connections': False,
        'writer_exclusion': False, 'ownership_transfer': False, 'stale_writer_rejected': False,
        'crash_releases_lock_without_reusing_state': False, 'role_boundaries': False,
        'owner_backend_terminated': False, 'client_cleanup': False, 'fixture_rows_cleared': False}
    connections = []
    admin = None
    failure = None
    phase = 'setup'
    cleanup_errors = []

    def connection(role=WRITER):
        value = _connect(role, prefix + '-' + str(len(connections)))
        connections.append(value)
        return value

    def metadata():
        return admin.execute('SELECT revision,status,run_id FROM collector_private.coinglass_session_state').fetchone()

    try:
        admin = prepare_database(prefix + '-admin')
        result['postgresql_version'] = admin.execute('SHOW server_version').fetchone()['server_version']
        a, b, observer = connection(), connection(), connection()
        backend_ids = [c.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
                       for c in (a, b, observer)]
        for c in (a, b, observer): c.commit()
        require(len(set(backend_ids)) == 3, 'connections_share_backend')
        result['backend_ids'] = backend_ids
        result['independent_connections'] = True
        phase = 'permissions'
        frontend = connection(FRONTEND)
        _expect_permission(frontend, 'SELECT encrypted_payload FROM collector_private.coinglass_session_state')
        _expect_permission(frontend, "UPDATE collector_private.coinglass_session_state SET status='ready' WHERE false")
        _expect_permission(b, 'DELETE FROM collector_private.coinglass_session_state WHERE false')
        _expect_permission(b, 'CREATE TABLE collector_private.test_forbidden_create (id int)')
        result['role_boundaries'] = True

        phase = 'competing_writer'
        require(_lock(a), 'first_writer_lock_unavailable')
        store_a, store_b = PgSessionStore(a, KEY), PgSessionStore(b, KEY)
        store_a.bootstrap(STATE)
        first = store_a.begin(str(uuid4()))
        require(a.info.transaction_status == psycopg.pq.TransactionStatus.IDLE,
                'begin_not_committed')
        before = metadata()
        visible = observer.execute('SELECT revision,status FROM collector_private.coinglass_session_state').fetchone()
        observer.commit()
        require(visible['status'] == 'in_use' and visible['revision'] == first.revision,
                'claim_not_visible_to_independent_connection')
        require(not _lock(b), 'second_writer_acquired_held_lock')
        _expect_session('state_coordination_required', lambda: store_b.begin(str(uuid4())))
        require(metadata() == before, 'competing_writer_changed_state')
        result['writer_exclusion'] = True

        phase = 'ownership_transfer'
        updated = json.loads(json.dumps(STATE))
        updated['cookies'][0]['value'] = 'DUMMY_UPDATED_STATE'
        stored_revision = store_a.complete(first, 'ready', updated)
        a.close()  # Test natural connection cleanup, not explicit unlock masking it.
        require(_wait_for_released_lock(b), 'lock_not_released_after_owner_close')
        second = store_b.begin(str(uuid4()))
        require(second.state == updated and second.revision > stored_revision,
                'new_owner_did_not_load_latest_state')
        result['ownership_transfer'] = True
        phase = 'stale_writer'
        before = metadata()
        _expect_session('state_revision_conflict', lambda: store_b.complete(first, 'ready', STATE))
        wrong_owner = Lease(second.revision, str(uuid4()), second.state)
        _expect_session('state_revision_conflict', lambda: store_b.complete(wrong_owner, 'ready', STATE))
        require(metadata() == before, 'stale_writer_changed_state')
        result['stale_writer_rejected'] = True

        phase = 'owner_backend_termination'
        # Terminate only the known backend opened by this check in the verified
        # private cluster. This is actual database connection loss, not a claim
        # that an entire worker/host crash was simulated.
        require(b.info.backend_pid == backend_ids[1], 'backend_identity_changed')
        require(admin.execute('SELECT pg_terminate_backend(%s) AS stopped',
                              (backend_ids[1],)).fetchone()['stopped'] is True,
                'owned_backend_not_terminated')
        result['owner_backend_terminated'] = True
        b.close()
        require(_wait_for_released_lock(observer), 'crashed_owner_lock_not_released')
        _expect_session('state_not_ready', lambda: PgSessionStore(observer, KEY).begin(str(uuid4())))
        require(metadata() == before, 'crash_marker_lost')
        result['crash_releases_lock_without_reusing_state'] = True
    except CheckFailure as error:
        failure = str(error)
    except SessionStateError as error:
        failure = error.code
    except BaseException as error:
        # No SQL, DSN, traceback or library error text is retained.
        failure = 'test_interrupted' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'postgres_check_failed'
        sqlstate = getattr(error, 'sqlstate', None)
        if isinstance(sqlstate, str) and re.fullmatch(r'[A-Z0-9]{5}', sqlstate):
            result['sqlstate'] = sqlstate
    finally:
        for connection_value in reversed(connections):
            try: connection_value.close()
            except Exception: cleanup_errors.append('client_close_failed')
        if admin is not None:
            try:
                deadline = time.monotonic() + 2
                while True:
                    remaining = admin.execute("""SELECT count(*) AS n FROM pg_stat_activity
                        WHERE application_name LIKE %s AND pid<>pg_backend_pid()""", (prefix + '%',)).fetchone()['n']
                    locks = admin.execute("""SELECT count(*) AS n FROM pg_locks l
                        JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name LIKE %s
                        AND l.locktype='advisory' AND l.granted""", (prefix + '%',)).fetchone()['n']
                    if remaining == 0 and locks == 0 or time.monotonic() >= deadline: break
                    time.sleep(0.05)
                result['client_cleanup'] = remaining == 0 and locks == 0
                if not result['client_cleanup']: cleanup_errors.append('client_or_lock_remaining')
                admin.execute('DELETE FROM collector_private.coinglass_session_state')
                result['fixture_rows_cleared'] = admin.execute(
                    'SELECT count(*) AS n FROM collector_private.coinglass_session_state').fetchone()['n'] == 0
                if not result['fixture_rows_cleared']: cleanup_errors.append('dummy_row_remaining')
            except Exception: cleanup_errors.append('database_cleanup_failed')
            finally:
                try: admin.close()
                except Exception: cleanup_errors.append('admin_close_failed')
        if failure is not None:
            result['reason'] = failure
            result['failure_phase'] = phase
        if cleanup_errors: result['cleanup_errors'] = sorted(set(cleanup_errors))
        if failure is None and not cleanup_errors:
            result['status'] = 'passed'
    return result


if __name__ == '__main__':
    answer = run_checks()
    print('CG_PG_CHECK_RESULT ' + json.dumps(answer, sort_keys=True, separators=(',', ':')), flush=True)
    raise SystemExit(0 if answer['status'] == 'passed' else 1)
