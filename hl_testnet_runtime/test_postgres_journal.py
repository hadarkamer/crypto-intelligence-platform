"""Real PostgreSQL persistence tests against a disposable loopback CI database.

No user database credentials, Hyperliquid credentials, or market requests.
Every server-side effect in these tests is restricted to hl_journal_ci.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from . import postgres_journal as pg

CI_URL = os.environ.get('HL_JOURNAL_CI_URL')
A, B = '0x' + '1' * 40, '0x' + '2' * 40


class PureTests(unittest.TestCase):
    def test_only_selected_staging_internal_url(self):
        url = 'postgresql://' + pg.STAGING_DB + '_user:FAKE%40VALUE@' + pg.STAGING_HOST + '/' + pg.STAGING_DB
        actual = pg.connection_parameters(url)
        self.assertEqual(actual['sslmode'], 'require')
        self.assertEqual(actual['password'], 'FAKE@VALUE')
        self.assertEqual(actual['dbname'], pg.STAGING_DB)
    def test_production_external_other_db_and_options_refused(self):
        valid = 'postgresql://' + pg.STAGING_DB + '_user:FAKE@' + pg.STAGING_HOST + '/' + pg.STAGING_DB
        for value in ('', None, valid.replace(pg.STAGING_HOST, 'dpg-d94d641kh4rs73evvih0-a'),
                      valid.replace(pg.STAGING_HOST, pg.STAGING_HOST+'.oregon-postgres.render.com'),
                      valid+'?sslmode=disable', valid+'?options=-c+log_statement=all',
                      valid+'#secret', valid.replace('/'+pg.STAGING_DB, '/production')):
            with self.assertRaises(pg.JournalError): pg.connection_parameters(value)
    def test_errors_never_echo_connection_details(self):
        with self.assertRaises(pg.JournalError) as caught:
            pg.connection_parameters('postgresql://owner:DO_NOT_EXPOSE@wrong/production')
        self.assertNotIn('DO_NOT_EXPOSE', str(caught.exception))
    def test_address_and_record_bounds(self):
        for value in ('', '0x'+'0'*40, '0x'+'a'*64, None):
            with self.assertRaises(pg.JournalError): pg.account_address(value)
        for value in ({'x':float('nan')}, {'x':'a'*40000}):
            with self.assertRaises(pg.JournalError): pg.canonical(value)
    def test_result_summary_drops_untrusted_details(self):
        result = pg.safe_result({'status':'UNCERTAIN_REQUIRES_REVIEW','verified':False,
                                  'private_key':'DO_NOT_EXPOSE','response':'RAW'})
        self.assertEqual(result, {'status':'UNCERTAIN_REQUIRES_REVIEW','verified':False})
        with self.assertRaises(pg.JournalError): pg.safe_result({'status':'raw: SECRET','verified':False})
    def test_ci_url_cannot_target_render(self):
        with self.assertRaises(pg.JournalError):
            pg.PostgresJournal.for_ci('postgresql://x:y@'+pg.STAGING_HOST+'/hl_journal_ci')
    def test_disabled_dispatch_has_no_import_or_io(self):
        from .persistent_execution import submit_persisted
        for enabled in (False, 'true', 1, None):
            with patch.object(pg.PostgresJournal, 'from_env', side_effect=AssertionError('No DB')):
                self.assertEqual(submit_persisted({}, account='', agent='', enable_testnet=enabled)['status'], 'DISABLED')
    def test_storage_missing_connection_does_not_read_key_or_send(self):
        from .persistent_execution import startup_storage_check
        from io import StringIO
        with patch.dict(os.environ, {'HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1'}, clear=True), \
             patch('sys.stdout', new_callable=StringIO) as output:
            startup_storage_check()
        self.assertEqual(json.loads(output.getvalue())['testnet_journal']['status'], 'WAITING_FOR_DATABASE_CONNECTION')
    def test_staging_expiry_blocks_before_connect(self):
        from types import SimpleNamespace
        journal = pg.PostgresJournal({'host':pg.STAGING_HOST,'dbname':pg.STAGING_DB})
        with patch.object(pg, 'datetime', SimpleNamespace(now=lambda tz:pg.EXPIRES+timedelta(seconds=1))):
            with self.assertRaisesRegex(pg.JournalError,'EXPIRED'):
                journal.readiness_probe()


@unittest.skipUnless(CI_URL, 'Requires disposable PostgreSQL CI service')
class PostgresTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from .price_precision import prepare_signal
        self.journal = pg.PostgresJournal.for_ci(CI_URL)
        # The test constructor above restricts host AND database name before DDL.
        with psycopg.connect(**self.journal._parameters) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {pg.SCHEMA} CASCADE')
        self.assertTrue(self.journal.bootstrap())
        self.source = {'kind':'SIGNAL','event_id':'fixture-1','symbol':'DEMO','side':'LONG',
                       'entry':'100.00','stop':'98.123456','take_profit':'104.123456',
                       'at':(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}
        self.meta = {'universe':[{'name':'OTHER','szDecimals':2}, {'name':'DEMO','szDecimals':2}]}
        self.prepared = prepare_signal(self.source,self.meta)
    def action(self, prepared=None):
        import hyperliquid_testnet_executor as sender
        return sender.build_action((prepared or self.prepared)['execution'],self.meta,A,exit_type='limit')
    def save(self):
        return self.journal.save_prepared(A,self.prepared)[0]
    def test_separate_connection_probe_and_repeated_boot(self):
        first = self.journal.readiness_probe()
        other = pg.PostgresJournal.for_ci(CI_URL)
        self.assertFalse(other.bootstrap())
        self.assertTrue(other.readiness_probe()['previous_probe_seen'])
        self.assertTrue(first['separate_connection_verified'])
    def test_original_rounded_and_audit_survive_new_python_process(self):
        key = self.save()
        self.journal.reserve(key,self.action())
        expected = pg.digest(self.prepared)
        program = '''import os,sys
from hl_testnet_runtime.postgres_journal import PostgresJournal,digest
j=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
r=j.load(sys.argv[1]); assert digest(r['prepared'])==sys.argv[2]
fresh,nonce,result=j.reserve(sys.argv[1],r['action']); assert fresh is False
assert result['status']=='RESERVED_OR_UNCERTAIN'
print('PERSISTENCE_AND_REPLAY_GUARD_PASSED')'''
        run = subprocess.run([sys.executable,'-c',program,key,expected],capture_output=True,text=True,timeout=10)
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertIn('PERSISTENCE_AND_REPLAY_GUARD_PASSED',run.stdout)
    def test_changed_original_even_same_rounding_cannot_overwrite(self):
        from .price_precision import prepare_signal
        self.save()
        changed = deepcopy(self.source);changed['stop']='98.123457'
        with self.assertRaisesRegex(pg.JournalError,'NO_OVERWRITE'):
            self.journal.save_prepared(A,prepare_signal(changed,self.meta))
    def test_new_id_is_preserved_but_one_shot_account_cannot_resend(self):
        from .price_precision import prepare_signal
        key=self.save();self.journal.reserve(key,self.action())
        changed={**self.source,'event_id':'fixture-2'}
        prep=prepare_signal(changed,self.meta)
        key2,_=self.journal.save_prepared(A,prep)
        self.assertNotEqual(key,key2)
        with self.assertRaisesRegex(pg.JournalError,'ALREADY_RESERVED'):
            self.journal.reserve(key2,self.action(prep))
    def test_concurrent_workers_only_one_committed_reservation(self):
        key=self.save();act=self.action()
        def reserve(_):return pg.PostgresJournal.for_ci(CI_URL).reserve(key,act)[0]
        with ThreadPoolExecutor(max_workers=6) as pool:
            answers=list(pool.map(reserve,range(6)))
        self.assertEqual(sum(answers),1)
    def test_unknown_response_survives_restart_no_replay(self):
        key=self.save();act=self.action();self.journal.reserve(key,act)
        self.journal.save_result(key,{'status':'UNCERTAIN_REQUIRES_REVIEW','verified':False})
        fresh,_,reply=pg.PostgresJournal.for_ci(CI_URL).reserve(key,act)
        self.assertFalse(fresh);self.assertEqual(reply['status'],'UNCERTAIN_REQUIRES_REVIEW')
    def test_changed_exit_mode_or_action_refused(self):
        import hyperliquid_testnet_executor as sender
        key=self.save();self.journal.reserve(key,self.action())
        different=sender.build_action(self.prepared['execution'],self.meta,A,exit_type='market')
        with self.assertRaisesRegex(pg.JournalError,'NO_RESEND'):self.journal.reserve(key,different)
        malicious=self.action();malicious['private_key']='SECRET'
        with self.assertRaisesRegex(pg.JournalError,'NOT_BOUND'):self.journal.reserve(key,malicious)
    def test_invalid_audit_and_secret_never_stored(self):
        changed=deepcopy(self.prepared);changed['audit']['changes']={}
        with self.assertRaises(pg.JournalError):self.journal.save_prepared(A,changed)
        changed=deepcopy(self.prepared);changed['source']['private_key']='SECRET'
        with self.assertRaises(pg.JournalError):self.journal.save_prepared(A,changed)
    def test_tampered_manifest_detected(self):
        import psycopg
        key=self.save()
        with psycopg.connect(**self.journal._parameters) as conn:
            conn.execute(f"UPDATE {pg.SCHEMA}.prepared SET digest=%s WHERE plan_key=%s",('f'*64,key))
        with self.assertRaisesRegex(pg.JournalError,'CHECKSUM'):self.journal.load(key)
    def test_no_database_means_no_signature(self):
        from . import persistent_execution as dispatch
        from .test_guarded_execution import Reader
        import hyperliquid_testnet_executor as sender
        with patch.object(self.journal,'save_prepared',side_effect=pg.JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND')), \
             patch('hl_testnet_runtime.checks.InfoReader',return_value=Reader()), \
             patch.object(sender,'_wallet') as wallet:
            r=dispatch.submit_persisted(self.source,account=A,agent=B,journal=self.journal,exit_type='limit',enable_testnet=True)
        self.assertEqual(r['order_requests_sent'],0);wallet.assert_not_called()
    def _dispatch_fixture(self):
        from contextlib import ExitStack
        from types import SimpleNamespace
        from .test_guarded_execution import Reader
        from hyperliquid_testnet_executor_selftest import FakeExchange
        import hyperliquid_testnet_executor as sender
        stack=ExitStack(); self.addCleanup(stack.close)
        exchange=FakeExchange()
        stack.enter_context(patch('hl_testnet_runtime.checks.InfoReader',return_value=Reader()))
        stack.enter_context(patch.object(sender.http.client,'HTTPSConnection',side_effect=exchange.connection))
        stack.enter_context(patch.object(sender,'_wallet',return_value=SimpleNamespace(address=B)))
        signer=stack.enter_context(patch.object(sender,'_signed_body',side_effect=lambda w,a,n:{
            'action':a,'nonce':n,'signature':{'r':'0x1','s':'0x2','v':27},'expiresAfter':n+30000}))
        return exchange,signer
    def test_real_pg_rounded_sender_and_fake_exchange(self):
        from .persistent_execution import submit_persisted
        exchange,signer=self._dispatch_fixture()
        r=submit_persisted(self.source,account=A,agent=B,journal=self.journal,exit_type='limit',enable_testnet=True)
        self.assertEqual(r['status'],'VERIFIED_OPEN_PROTECTED',r)
        self.assertEqual(len([c for c in exchange.calls if c[0]=='/exchange']),1)
        signer.assert_called_once()
        r2=submit_persisted(self.source,account=A,agent=B,journal=pg.PostgresJournal.for_ci(CI_URL),exit_type='limit',enable_testnet=True)
        self.assertTrue(r2['replayed'])
        self.assertEqual(len([c for c in exchange.calls if c[0]=='/exchange']),1)
    def test_lost_commit_ack_never_signs_or_retries(self):
        from .persistent_execution import submit_persisted
        exchange,signer=self._dispatch_fixture()
        reserve=self.journal.reserve
        def lost_ack(*args):
            reserve(*args)
            raise pg.JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND')
        with patch.object(self.journal,'reserve',side_effect=lost_ack):
            r=submit_persisted(self.source,account=A,agent=B,journal=self.journal,exit_type='limit',enable_testnet=True)
        signer.assert_not_called();self.assertEqual(r['order_requests_sent'],0)
        r=submit_persisted(self.source,account=A,agent=B,journal=self.journal,exit_type='limit',enable_testnet=True)
        self.assertTrue(r['replayed']);signer.assert_not_called()
    def test_stale_record_saved_but_not_signed(self):
        from .persistent_execution import submit_persisted
        exchange,signer=self._dispatch_fixture()
        self.source['at']=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
        r=submit_persisted(self.source,account=A,agent=B,journal=self.journal,exit_type='limit',enable_testnet=True)
        self.assertEqual(r['status'],'SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST');signer.assert_not_called()
    def test_missing_exit_type_saved_but_not_signed(self):
        from .persistent_execution import submit_persisted
        exchange,signer=self._dispatch_fixture()
        r=submit_persisted(self.source,account=A,agent=B,journal=self.journal,enable_testnet=True)
        self.assertEqual(r['status'],'EXPLICIT_EXIT_TYPE_REQUIRED');signer.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
