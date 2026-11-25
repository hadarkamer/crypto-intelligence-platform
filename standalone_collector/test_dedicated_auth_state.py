"""Dedicated state integration against a new, private PostgreSQL cluster. No provider or service DSN."""
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import time
from uuid import uuid4

from dedicated_auth_state import DedicatedSessionStore, incident_status
from session_state import PgSessionStore, SessionStateError, WORK_LOCK

HERE = Path(__file__).resolve().parent


class DisposablePostgres:
    def __enter__(self):
        postgres_bin = os.environ.get('COINGLASS_TEST_POSTGRES_BIN')
        if not postgres_bin and shutil.which('pg_config'):
            postgres_bin = subprocess.check_output(['pg_config','--bindir'], text=True).strip()
        self.bin = Path(postgres_bin or '/nonexistent')
        if not (self.bin/'initdb').is_file():
            raise RuntimeError('temporary PostgreSQL server programs are required')
        self.tmp = tempfile.TemporaryDirectory(prefix='cg-dedicated-pg-')
        self.root = Path(self.tmp.name)
        self.data = self.root/'data'
        self.password = secrets.token_urlsafe(32)
        pw = self.root/'pw'
        pw.write_text(self.password+'\n')
        pw.chmod(0o600)
        try:
            self.command('initdb','-U','fixture','-D',str(self.data),'-A','scram-sha-256',
                         '--pwfile',str(pw),'--no-locale','--encoding','UTF8')
            pw.unlink()
            # Loopback only, with fixture-only SCRAM credentials. Some runners prohibit Unix sockets.
            with socket.socket(socket.AF_INET) as allocated:
                allocated.bind(('127.0.0.1',0))
                self.port = allocated.getsockname()[1]
            self.command('pg_ctl','-D',str(self.data),'-o',
                         f"-c unix_socket_directories='' -h 127.0.0.1 -p {self.port}",
                         '-l',str(self.root/'pg.log'),'-w','start')
        except BaseException:
            self.tmp.cleanup()
            raise
        return self

    def command(self, exe, *args):
        subprocess.run([str(self.bin/exe),*args], check=True, capture_output=True, timeout=35)

    def connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(host='127.0.0.1',port=self.port,dbname='postgres',user='fixture',
                               password=self.password,connect_timeout=3,row_factory=dict_row,
                               options='-c statement_timeout=3000 -c lock_timeout=1000')

    def initialize(self):
        with self.connect() as conn:
            conn.execute((HERE/'session_state.sql').read_text())
            conn.execute((HERE/'dedicated_auth_state.sql').read_text())
            conn.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_jobs
                            (id uuid PRIMARY KEY,status text NOT NULL)""")

    def __exit__(self, *exc):
        try:
            self.command('pg_ctl','-D',str(self.data),'-m','immediate','-w','stop')
        finally:
            self.tmp.cleanup()
            self.password = None


def snapshot(value):
    return {'cookies':[{'name':'fixture','value':value,'domain':'www.coinglass.com','path':'/',
            'httpOnly':True,'secure':True,'expires':-1}], 'origins':[]}


def lock(connection):
    with connection.transaction():
        return connection.execute(
            'SELECT pg_try_advisory_lock(%s) AS held',(WORK_LOCK,)).fetchone()['held']


class DedicatedAuthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = DisposablePostgres().__enter__()
        cls.addClassCleanup(cls.fixture.__exit__,None,None,None)
        cls.fixture.initialize()

    def setUp(self):
        self.conn = self.fixture.connect()
        self.addCleanup(lambda: self.conn.close())
        self.email = f'{uuid4()}@example.invalid'
        self.key = secrets.token_bytes(32)
        self.store = DedicatedSessionStore(self.conn,self.key,self.email)
        self.assertTrue(lock(self.conn))

    def begin(self):
        return self.store.begin(str(uuid4()),allow_initial_probe=True,recover_orphan=True)

    def reconnect(self):
        self.conn.close()
        self.conn = self.fixture.connect()
        self.assertTrue(lock(self.conn))
        self.store = DedicatedSessionStore(self.conn,self.key,self.email)

    def test_no_legacy_read_even_if_valid_and_account_bindings_differ(self):
        legacy = PgSessionStore(self.conn,self.key)
        legacy.bootstrap(snapshot('old-account-only'))
        self.assertEqual(self.begin().state,{'cookies':[],'origins':[]})
        other = DedicatedSessionStore(self.conn,self.key,'another@example.invalid')
        self.assertNotEqual(other.slot,self.store.slot)
        self.assertNotIn(self.email,self.store.slot)
        self.assertEqual(other.revision(),0)
        with self.assertRaisesRegex(SessionStateError,'auth_protected_proof_required'):
            self.store.bootstrap(snapshot('old-account-only'))

    def test_two_connections_and_noncommitted_fence_are_rejected(self):
        lease = self.begin()
        reserved = self.store.auth_start(lease)
        event = reserved['incident_id']
        self.assertEqual(reserved['code'],'allowed')
        self.assertEqual(self.store.auth_start(lease)['code'],'auth_in_progress')
        with self.fixture.connect() as secondary:
            self.assertFalse(lock(secondary))
            rival = DedicatedSessionStore(secondary,self.key,self.email)
            with self.assertRaisesRegex(SessionStateError,'state_coordination_required'):
                rival.auth_submit(lease,event)
        with self.conn.transaction():
            with self.assertRaisesRegex(SessionStateError,'state_transaction_not_idle'):
                self.store.auth_submit(lease,event)
        self.store.auth_submit(lease,event)
        with self.assertRaisesRegex(SessionStateError,'auth_attempt_not_reserved'):
            self.store.auth_submit(lease,event)

    def test_crash_after_submit_retains_one_incident_and_never_resubmits(self):
        lease = self.begin()
        event = self.store.auth_start(lease)['incident_id']
        self.store.auth_submit(lease,event)
        self.reconnect()
        fresh = self.begin()
        verdict = self.store.auth_start(fresh)
        self.assertEqual((verdict['incident_id'],verdict['code'],verdict['notify']),
                         (event,'auth_unconfirmed',True))
        self.assertFalse(self.store.auth_start(fresh)['notify'])
        with self.assertRaisesRegex(SessionStateError,'auth_attempt_not_reserved'):
            self.store.auth_submit(fresh,event)
        self.assertEqual(incident_status(self.conn,self.store.slot,job_id=lease.run_id)['reason'],
                         'auth_unconfirmed')
        self.assertEqual(incident_status(self.conn,self.store.slot,job_id=fresh.run_id)['incident_id'],event)

    def test_preclick_retry_budget_and_timeout(self):
        lease = self.begin()
        event = self.store.auth_start(lease)['incident_id']
        self.assertEqual(self.store.auth_result(lease,event,'auth_network_transient')['code'],'auth_retry_pending')
        self.reconnect()
        fresh = self.begin()
        self.assertEqual(self.store.auth_start(fresh)['incident_id'],event)
        denied = self.store.auth_result(fresh,event,'auth_network_transient')
        self.assertEqual((denied['code'],denied['notify']),('auth_retry_exhausted',True))
        self.assertFalse(self.store.auth_start(fresh)['notify'])
        next_event = self.store.acknowledge_remediation(event)
        self.store.auth_start(fresh)
        with self.conn.transaction():
            self.conn.execute("UPDATE collector_private.coinglass_dedicated_auth SET retry_until=now()-interval '1 second' WHERE id=%s",(next_event,))
        with self.assertRaisesRegex(SessionStateError,'auth_attempt_not_reserved'):
            self.store.auth_submit(fresh,next_event)
        self.assertEqual(self.store.auth_result(fresh,next_event,'auth_network_transient')['code'],
                         'auth_retry_exhausted')

    def test_post_submit_transient_ambiguous_no_retry(self):
        lease = self.begin()
        event = self.store.auth_start(lease)['incident_id']
        self.store.auth_submit(lease,event)
        self.assertEqual(self.store.auth_result(lease,event,'auth_network_transient')['code'],'auth_unconfirmed')

    def test_map_http_429_is_durable_across_jobs_without_any_auth_event(self):
        lease = self.begin()
        self.assertEqual(self.store.source_cooldown(lease)['code'],'allowed')
        self.assertEqual(self.store.source_cooldown(lease,limited=True)['code'],'source_rate_limited')
        self.store.source_cooldown(lease,limited=True,retry_after_seconds=600)
        self.store.source_cooldown(lease,limited=True,retry_after_seconds=2)
        self.store.complete(lease,'unconfirmed')
        self.reconnect()
        fresh = self.begin()
        result = self.store.source_cooldown(fresh)
        self.assertEqual(result['code'],'source_rate_limited')
        self.assertGreaterEqual(result['retry_after_seconds'],599)
        self.assertEqual(incident_status(self.conn,self.store.slot)['status'],'none')
        with self.conn.transaction():
            self.conn.execute(f"UPDATE {self.store.table} SET map_retry_after=now()-interval '1 second' WHERE slot=%s",
                              (self.store.slot,))
        self.assertEqual(self.store.source_cooldown(fresh),{'code':'allowed','retry_after_seconds':None})

    def test_rate_limit_and_explicit_rearm_preserve_status_by_old_id(self):
        lease = self.begin()
        event = self.store.auth_start(lease)['incident_id']
        verdict = self.store.auth_result(lease,event,'auth_rate_limited',3600)
        self.assertGreaterEqual(verdict['retry_after_seconds'],3599)
        with self.assertRaisesRegex(SessionStateError,'auth_remediation_not_available'):
            self.store.acknowledge_remediation(event)
        with self.conn.transaction():
            self.conn.execute("UPDATE collector_private.coinglass_dedicated_auth SET retry_after=now()-interval '1 second' WHERE id=%s",(event,))
        replaced = self.store.acknowledge_remediation(event)
        with self.assertRaisesRegex(SessionStateError,'auth_remediation_not_available'):
            self.store.acknowledge_remediation(event)
        self.assertEqual(incident_status(self.conn,self.store.slot,incident_id=event)['incident_id'],event)
        self.assertEqual(incident_status(self.conn,self.store.slot,job_id=lease.run_id)['incident_id'],replaced)
        self.store.checkpoint(lease,snapshot('verified'))
        self.assertEqual(incident_status(self.conn,self.store.slot,incident_id=event)['status'],'resolved')

    def test_explicit_challenge_after_verified_keeps_reason_and_largest_retry_after(self):
        lease = self.begin()
        event = self.store.auth_start(lease)['incident_id']
        self.store.auth_submit(lease,event)
        verified = self.store.checkpoint(lease,snapshot('verified'),incident_id=event)
        observed = self.store.auth_start(verified,observed_code='auth_rate_limited',retry_after_seconds=7200)
        self.assertEqual((observed['code'],observed['notify']),('auth_rate_limited',True))
        later = self.store.auth_start(verified,observed_code='auth_rate_limited',retry_after_seconds=30)
        self.assertGreaterEqual(later['retry_after_seconds'],7199)
        self.assertFalse(later['notify'])
        huge = self.store.auth_start(verified,observed_code='auth_rate_limited',retry_after_seconds=10**17)
        self.assertGreater(huge['retry_after_seconds'],7200)
        self.assertEqual(self.store.auth_start(verified,observed_code='auth_challenge')['code'],'auth_challenge')
        with self.assertRaisesRegex(SessionStateError,'auth_remediation_not_available'):
            self.store.acknowledge_remediation(observed['incident_id'])
        with self.assertRaisesRegex(SessionStateError,'auth_attempt_not_reserved'):
            self.store.auth_submit(verified,observed['incident_id'])

    def test_verified_checkpoint_survives_worker_death_and_late_write(self):
        lease = self.begin()
        event = self.store.auth_start(lease)['incident_id']
        self.store.auth_submit(lease,event)
        saved = self.store.checkpoint(lease,snapshot('fresh-verified'),incident_id=event)
        with self.assertRaisesRegex(SessionStateError,'state_revision_conflict'):
            self.store.complete(lease,'ready',snapshot('stale'))
        self.assertEqual(incident_status(self.conn,self.store.slot,job_id=lease.run_id)['status'],'resolved')
        self.reconnect()
        next_lease = self.begin()
        self.assertGreater(next_lease.revision,saved.revision)
        self.assertEqual(next_lease.state,snapshot('fresh-verified'))
        self.store.complete(next_lease,'blocked')
        self.assertEqual(self.store.read(),snapshot('fresh-verified'))

    def test_receipt_public_or_uncertain_cannot_authenticate(self):
        lease = self.begin()
        self.store.complete(lease,'ready',snapshot('public-session'))
        next_lease = self.begin()
        self.assertEqual(next_lease.state['cookies'],[])
        event = self.store.auth_start(next_lease)['incident_id']
        self.store.auth_submit(next_lease,event)
        self.store.complete(next_lease,'unconfirmed')
        self.assertEqual(incident_status(self.conn,self.store.slot,incident_id=event)['reason'],'auth_unconfirmed')

    def test_clean_browser_copy_and_expiry_during_work_do_not_strand_lease(self):
        lease = self.begin()
        original = snapshot('working')
        expiry = time.time()+120
        original['cookies'][0]['expires'] = expiry
        saved = self.store.checkpoint(lease,original)
        saved.state.clear()
        self.store.complete(saved,'unconfirmed')
        fresh = self.begin()
        self.assertEqual(fresh.state['cookies'][0]['value'],'working')
        with patch('session_state.time.time',return_value=expiry+10):
            self.store.complete(fresh,'unconfirmed')
        final = self.begin()
        self.assertEqual(final.state['cookies'],[])

    def test_challenges_and_immediate_regate_emit_one_alert(self):
        for code in ('auth_challenge','auth_code_required','auth_invalid_credentials','auth_form_unavailable'):
            with self.subTest(code=code):
                self.store = DedicatedSessionStore(self.conn,self.key,f'{uuid4()}@example.invalid')
                lease = self.begin()
                event = self.store.auth_start(lease)['incident_id']
                self.assertTrue(self.store.auth_result(lease,event,code)['notify'])
                self.assertEqual(self.store.auth_start(lease)['code'],code)
                self.assertFalse(self.store.auth_start(lease)['notify'])
                verified = self.store.checkpoint(lease,snapshot('verified'))
                gate = self.store.auth_start(verified)
                self.assertEqual(gate['code'],'auth_gate_after_verification')
                self.assertTrue(gate['notify'])
                self.assertFalse(self.store.auth_start(verified)['notify'])
                self.assertEqual(incident_status(self.conn,self.store.slot,incident_id=event)['status'],'resolved')
