"""Apply the existing private migration transactionally from the approved QA shell."""
from hashlib import sha256
import os
from pathlib import Path
import sys


def apply(connection, migration):
    tables = ['collector_private.' + suffix for suffix in (
        'coinglass_dedicated_sessions', 'coinglass_dedicated_auth',
        'coinglass_dedicated_auth_jobs')]
    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout = '3s'")
        connection.execute("SET LOCAL statement_timeout = '8s'")
        connection.execute(migration, prepare=False)
        row = connection.execute('''
            SELECT count(*) = 3 AND bool_and(
                has_table_privilege(current_user, target, 'SELECT') AND
                has_table_privilege(current_user, target, 'INSERT') AND
                has_table_privilege(current_user, target, 'UPDATE') AND
                NOT EXISTS (SELECT FROM pg_roles r WHERE rolname IN ('anon','authenticated') AND
                            (has_table_privilege(r.oid, target, 'SELECT') OR
                             has_table_privilege(r.oid, target, 'INSERT') OR
                             has_table_privilege(r.oid, target, 'UPDATE') OR
                             has_table_privilege(r.oid, target, 'DELETE'))) AND
                NOT EXISTS (SELECT FROM pg_class c,
                     LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
                     WHERE c.oid=to_regclass(target) AND a.grantee=0))
            FROM unnest(%s::text[]) AS t(target)
        ''', (tables,)).fetchone()
        if not row or row[0] is not True:
            raise RuntimeError('private_permissions_failed')


if __name__ == '__main__':
    # Require the exact manual QA service and branch plus an independently verified QA commit.
    from native_pilot_host import require_service
    require_service(os.environ)
    if len(sys.argv) != 2 or os.environ.get('RENDER_GIT_COMMIT') != sys.argv[1]:
        raise SystemExit('qa_service_or_commit_mismatch')
    if not all(os.environ.get(k) for k in ('DATABASE_URL','COINGLASS_SESSION_KEY',
                                          'COINGLASS_LOGIN_EMAIL','COINGLASS_LOGIN_PASSWORD')):
        raise SystemExit('qa_configuration_incomplete')
    script = Path(__file__).with_name('dedicated_auth_state.sql').read_bytes()
    if sha256(script).hexdigest() != '5fcfe08797ccde65f77b69ecec75a61fe007127325097f448d777dc9fd6bcdbc':
        raise SystemExit('migration_digest_mismatch')
    try:
        import psycopg
        with psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5) as conn:
            apply(conn, script.decode())
    except Exception:
        raise SystemExit('private_storage_migration_failed') from None
    print('private_storage_tables=3 private_permissions=verified')
