"""Real PostgreSQL SQL/encryption in PGlite, with dummy state only.

PGlite is one backend: these tests do not claim multi-backend advisory-lock
contention. They exercise lock ownership, CAS, durability and SQL rollback.
No listener, browser, remote database, account or source request is opened.
"""
import contextlib
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from uuid import uuid4

from session_state import Lease, PgSessionStore, SessionStateError, WORK_LOCK, validate_state

HERE = Path(__file__).resolve().parent
PGLITE = Path(os.getenv('PGLITE_MODULE', str(HERE.parents[1] / 'test-runtime/node_modules/@electric-sql/pglite')))
KEY = bytes(range(32))  # Synthetic test key. No deployment key is generated here.
STATE = {'cookies': [{'name': 'dummy_session', 'value': 'DUMMY_AUTH_STATE_NOT_A_SECRET',
    'domain': '.coinglass.com', 'path': '/', 'expires': -1, 'httpOnly': True,
    'secure': True, 'sameSite': 'Lax'}], 'origins': []}
BRIDGE = r'''
const { PGlite } = require(process.argv[1]);
const readline = require('node:readline');
(async () => {
 const db = new PGlite(process.argv[2]); await db.waitReady;
 process.stdout.write(JSON.stringify({ready:true})+'\n');
 const input = readline.createInterface({input:process.stdin,crlfDelay:Infinity});
 for await (const line of input) {
  try {
   const request=JSON.parse(line);
   if(request.close){await db.close();process.stdout.write('{"closed":true}\n',()=>process.exit(0));break;}
   const answer=request.exec ? await db.exec(request.sql) : await db.query(request.sql,request.params||[]);
   process.stdout.write(JSON.stringify({ok:true,rows:answer.rows||[],affected:answer.affectedRows||0})+'\n');
  } catch(error) {process.stdout.write(JSON.stringify({ok:false,code:error.code||'unknown'})+'\n');}
 }
})().catch(()=>{process.stderr.write('isolated_pg_start_failed\n');process.exitCode=1;});
'''


class Cursor:
    def __init__(self, rows): self.rows = rows
    def fetchone(self): return self.rows[0] if self.rows else None


class IsolatedPg:
    """Only the DB-API surface used by the product store, over process pipes."""
    def __init__(self, directory):
        self.process = subprocess.Popen(['node', '-e', BRIDGE, str(PGLITE), str(directory)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if json.loads(self.process.stdout.readline()).get('ready') is not True:
            raise RuntimeError('isolated_pg_unavailable')

    def request(self, value):
        self.process.stdin.write(json.dumps(value, separators=(',', ':')) + '\n')
        self.process.stdin.flush()
        answer = json.loads(self.process.stdout.readline())
        if answer.get('ok') is False:
            raise RuntimeError('isolated_sql_error:' + answer['code'])
        return answer

    def execute(self, sql, params=()):
        parts = sql.split('%s')
        if len(parts) != len(params) + 1: raise AssertionError('parameter_count')
        query = parts[0] + ''.join('$' + str(i + 1) + part for i, part in enumerate(parts[1:]))
        rows = self.request({'sql': query, 'params': list(params)})['rows']
        # psycopg's dict_row returns bigint as Python int, while PGlite serializes
        # bigint as decimal text. Match that DB-API representation, not SQL logic.
        for row in rows:
            if 'revision' in row: row['revision'] = int(row['revision'])
        return Cursor(rows)

    def script(self, sql): self.request({'sql': sql, 'exec': True})

    @contextlib.contextmanager
    def transaction(self):
        self.execute('BEGIN')
        try:
            yield
        except BaseException:
            self.execute('ROLLBACK')
            raise
        else:
            self.execute('COMMIT')

    def close(self):
        if self.process.poll() is None:
            self.request({'close': True})
            self.process.wait(timeout=10)
        self.process.stdin.close(); self.process.stdout.close()
        error = self.process.stderr.read(); self.process.stderr.close()
        if self.process.returncode != 0 or error: raise AssertionError('isolated_pg_cleanup_failed')


@unittest.skipUnless(PGLITE.is_dir() and shutil.which('node'), 'existing isolated PGlite unavailable')
class StoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='coinglass-state-pg-')
        cls.path = Path(cls.temporary.name) / 'db'
        cls.db = IsolatedPg(cls.path)
        cls.db.script((HERE / 'session_state.sql').read_text())

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        cls.temporary.cleanup()
        assert not Path(cls.temporary.name).exists()

    def setUp(self):
        self.db.execute('SELECT pg_advisory_unlock_all()')
        self.db.execute('TRUNCATE collector_private.coinglass_session_state')
        self.db.execute('SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
        self.store = PgSessionStore(self.db, KEY)

    def assert_code(self, code, action):
        with self.assertRaises(SessionStateError) as caught: action()
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(str(caught.exception), code)
        self.assertNotIn(STATE['cookies'][0]['value'], str(caught.exception))

    def row(self):
        return self.db.execute('SELECT revision,status,run_id,encrypted_payload FROM collector_private.coinglass_session_state').fetchone()

    def test_load_capture_save_and_next_run_use_latest_without_plaintext(self):
        self.assertEqual(self.store.bootstrap(STATE), 1)
        lease = self.store.begin(str(uuid4()))
        self.assertEqual(lease.state, STATE)
        self.assertEqual(self.row()['status'], 'in_use')
        self.assertNotIn(STATE['cookies'][0]['value'], self.row()['encrypted_payload'])
        updated = copy.deepcopy(STATE); updated['cookies'][0]['value'] = 'DUMMY_ROTATED'
        saved = self.store.complete(lease, 'ready', updated)
        later = PgSessionStore(self.db, KEY).begin(str(uuid4()))
        self.assertEqual(later.state, updated)
        self.assertGreater(later.revision, saved)
        self.assertNotIn('DUMMY_ROTATED', self.row()['encrypted_payload'])
        self.assertNotIn('DUMMY_ROTATED', repr(later))

    def test_sql_write_failure_is_not_success_and_does_not_restore_old_ready_state(self):
        self.store.bootstrap(STATE); lease = self.store.begin(str(uuid4()))
        before = self.row()
        self.db.script("""CREATE FUNCTION collector_private.reject_state_save() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'dummy write failure'; END $$;
            CREATE TRIGGER reject_state_save BEFORE UPDATE ON collector_private.coinglass_session_state
            FOR EACH ROW EXECUTE FUNCTION collector_private.reject_state_save();""")
        try:
            self.assert_code('state_store_failed', lambda: self.store.complete(lease, 'ready', STATE))
            self.assertEqual(self.row(), before)
            self.assert_code('state_not_ready', lambda: PgSessionStore(self.db, KEY).begin(str(uuid4())))
        finally:
            self.db.script('DROP TRIGGER reject_state_save ON collector_private.coinglass_session_state; DROP FUNCTION collector_private.reject_state_save();')

    def test_crypto_import_and_provider_failures_are_sanitized_without_writes(self):
        import builtins
        from unittest.mock import patch
        real_import = builtins.__import__
        def unavailable(name, *args, **kwargs):
            if name == 'Crypto.Cipher': raise ImportError('DUMMY_PRIVATE_PROVIDER_DETAIL')
            return real_import(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=unavailable):
            self.assert_code('state_encrypt_failed', lambda: self.store.bootstrap(STATE))
        self.assertIsNone(self.row())
        self.store.bootstrap(STATE); lease = self.store.begin(str(uuid4()))
        before = self.row()
        with patch('Crypto.Cipher.AES.new', side_effect=RuntimeError('DUMMY_PRIVATE_PROVIDER_DETAIL')):
            self.assert_code('state_encrypt_failed', lambda: self.store.complete(lease, 'ready', STATE))
        self.assertEqual(self.row(), before)
        self.assertEqual(self.row()['status'], 'in_use')

    def test_corrupted_ciphertext_wrong_key_and_revision_replay_fail_closed(self):
        self.store.bootstrap(STATE)
        self.assert_code('state_corrupt', lambda: PgSessionStore(self.db, bytes(reversed(KEY))).read())
        original = self.row()['encrypted_payload']
        damaged = json.loads(original); damaged['tag'] = 'AAAAAAAAAAAAAAAAAAAAAA=='
        self.db.execute('UPDATE collector_private.coinglass_session_state SET encrypted_payload=%s', (json.dumps(damaged),))
        self.assert_code('state_corrupt', lambda: self.store.begin(str(uuid4())))
        self.db.execute('UPDATE collector_private.coinglass_session_state SET encrypted_payload=%s,revision=revision+1', (original,))
        self.assert_code('state_corrupt', lambda: self.store.begin(str(uuid4())))

    def test_all_expired_rejected_but_auxiliary_expiry_preserved(self):
        expired = copy.deepcopy(STATE); expired['cookies'][0]['expires'] = 1
        self.assert_code('state_expired', lambda: self.store.bootstrap(expired))
        mixed = copy.deepcopy(STATE); mixed['cookies'].append({**expired['cookies'][0], 'name': 'dummy_auxiliary'})
        self.store.bootstrap(mixed)
        self.assertEqual(self.store.read(), mixed)
        # A once-valid cookie can expire while stored; do not load old fallback.
        with self.db.transaction():
            expired_cipher = self.store._seal(mixed, 1)
            self.db.execute('UPDATE collector_private.coinglass_session_state SET encrypted_payload=%s', (expired_cipher,))
        from unittest.mock import patch
        with patch('session_state.time.time', return_value=10**12):
            self.assertEqual(self.store.read(), mixed)  # session cookie has no inferred server TTL
        self.db.execute('TRUNCATE collector_private.coinglass_session_state')
        expiring = copy.deepcopy(STATE); expiring['cookies'][0]['expires'] = 2
        with patch('session_state.time.time', return_value=1):
            self.store.bootstrap(expiring)
        with patch('session_state.time.time', return_value=3):
            self.assert_code('state_expired', lambda: self.store.begin(str(uuid4())))

    def test_competing_begin_and_stale_completion_cannot_overwrite(self):
        self.store.bootstrap(STATE); old = self.store.begin(str(uuid4()))
        self.assert_code('state_not_ready', lambda: PgSessionStore(self.db, KEY).begin(str(uuid4())))
        self.store.complete(old, 'ready', STATE)
        newer = self.store.begin(str(uuid4())); before = self.row()
        self.assert_code('state_revision_conflict', lambda: self.store.complete(old, 'ready', STATE))
        wrong_owner = Lease(newer.revision, str(uuid4()), newer.state)
        self.assert_code('state_revision_conflict', lambda: self.store.complete(wrong_owner, 'ready', STATE))
        self.assertEqual(self.row(), before)

    def test_blocked_and_unconfirmed_state_cannot_be_reused_or_bootstrapped_over(self):
        for outcome in ('blocked', 'unconfirmed'):
            self.db.execute('TRUNCATE collector_private.coinglass_session_state')
            self.store.bootstrap(STATE); lease = self.store.begin(str(uuid4()))
            self.store.complete(lease, outcome)
            self.assertEqual(self.row()['status'], outcome)
            self.assertIsNone(self.row()['encrypted_payload'])
            self.assert_code('state_not_ready', lambda: self.store.begin(str(uuid4())))
            self.assert_code('state_exists', lambda: self.store.bootstrap(STATE))
            self.assert_code('state_revision_conflict', lambda: self.store.complete(lease, 'ready', STATE))

    def test_existing_lock_required_for_begin_and_complete(self):
        self.store.bootstrap(STATE)
        self.db.execute('SELECT pg_advisory_unlock_all()')
        self.assert_code('state_coordination_required', lambda: self.store.begin(str(uuid4())))
        self.db.execute('SELECT pg_advisory_lock_shared(%s)', (WORK_LOCK,))
        self.assert_code('state_coordination_required', lambda: self.store.begin(str(uuid4())))
        self.db.execute('SELECT pg_advisory_unlock_all()')
        self.db.execute('SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
        lease = self.store.begin(str(uuid4()))
        self.db.execute('SELECT pg_advisory_unlock_all()')
        self.assert_code('state_coordination_required', lambda: self.store.complete(lease, 'ready', STATE))
        self.assertEqual(self.row()['status'], 'in_use')

    def test_process_restart_keeps_latest_and_crash_marker_is_not_reused(self):
        self.store.bootstrap(STATE); lease = self.store.begin(str(uuid4()))
        changed = copy.deepcopy(STATE); changed['cookies'][0]['value'] = 'DUMMY_AFTER_RESTART'
        self.store.complete(lease, 'ready', changed)
        type(self).db.close(); type(self).db = IsolatedPg(self.path)
        self.db.execute('SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
        reopened = PgSessionStore(self.db, KEY)
        self.assertEqual(reopened.read(), changed)
        reopened.begin(str(uuid4()))  # crash/suspension before final save
        type(self).db.close(); type(self).db = IsolatedPg(self.path)
        self.db.execute('SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
        self.assert_code('state_not_ready', lambda: PgSessionStore(self.db, KEY).begin(str(uuid4())))

    def test_no_public_privileges(self):
        row = self.db.execute("""SELECT NOT EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,
            LATERAL aclexplode(c.relacl) a WHERE n.nspname='collector_private'
            AND c.relname='coinglass_session_state' AND a.grantee=0) AS private""").fetchone()
        self.assertTrue(row['private'])
        schema = self.db.execute("""SELECT NOT EXISTS (
            SELECT 1 FROM pg_namespace n, LATERAL aclexplode(n.nspacl) a
            WHERE n.nspname='collector_private' AND a.grantee=0) AS private""").fetchone()
        self.assertTrue(schema['private'])


class ValidationTests(unittest.TestCase):
    def test_structure_scope_and_attributes(self):
        self.assertEqual(validate_state(STATE), STATE)
        bad = copy.deepcopy(STATE); bad['cookies'][0]['domain'] = 'coinglass.com.attacker.invalid'
        for state in (bad, {}, {'cookies': [], 'origins': []}, {**STATE, 'origins': [{}]}):
            with self.assertRaises(SessionStateError): validate_state(state)
        self.assertEqual(validate_state(STATE)['cookies'][0], STATE['cookies'][0])


if __name__ == '__main__': unittest.main()
