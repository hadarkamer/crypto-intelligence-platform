"""No real account or market I/O. Controlled authorization boundary tests."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import json
import unittest
from unittest.mock import patch,Mock
from . import controlled_attempt as task
from .source_window import source_fresh
from .postgres_journal import JournalError,digest
from .test_postgres_journal import PostgresTests,CI_URL

NOW=datetime(2026,9,16,9,0,tzinfo=timezone.utc)
A,B='0x'+'1'*40,'0x'+'2'*40


def fixture():
    signal={'kind':'SIGNAL','event_id':'fixture-one','symbol':'DEMO','side':'LONG',
            'entry':'100','stop':'98','take_profit':'104','at':(NOW-timedelta(minutes=2)).isoformat()}
    ticket={'version':task.MODE,'run_id':'controlled-fixture-one','signal':signal,
            'source_expires_at':(NOW+timedelta(minutes=8)).isoformat(),
            'issued_at':(NOW-timedelta(seconds=2)).isoformat(),'expires_at':(NOW+timedelta(seconds=120)).isoformat(),
            'message_id':'1','rule_id':'FIXTURE','signal_sha256':digest(signal),'delivery_verified':True}
    env={'RENDER_SERVICE_ID':task.SERVICE,'HL_TESTNET_RUNTIME_MODE':task.MODE,
         'HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1','HL_TESTNET_PRICE_ROUNDING':'nearest-half-up-perp-v1',
         'HL_TESTNET_EXIT_TYPE':'tp_limit_sl_market','HL_TESTNET_ACCOUNT_ADDRESS':A,'HL_TESTNET_AGENT_ADDRESS':B,
         'HL_TESTNET_ATTEMPT_TICKET':json.dumps(ticket)}
    return ticket,env


class AuthorizationTests(unittest.TestCase):
    def setUp(self):
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No network'))
        p.start();self.addCleanup(p.stop)
    def test_default_readonly_cannot_submit(self):
        _,env=fixture();env['HL_TESTNET_RUNTIME_MODE']='read_only'
        with patch.object(task.PostgresJournal,'from_env',side_effect=AssertionError('No DB')):
            self.assertEqual(task.run_configured(env,now=NOW)['status'],'DISABLED')
    def test_complete_ticket_is_bound(self):
        ticket,env=fixture();self.assertEqual(task.validate_ticket(json.dumps(ticket),env,now=NOW),ticket)
    def test_original_expiry_keeps_at_unchanged(self):
        t,e=fixture();original=deepcopy(t);task.validate_ticket(json.dumps(t),e,now=NOW)
        self.assertEqual(t,original)
    def test_foreign_service_or_policy_refused(self):
        t,e=fixture()
        for key in ('RENDER_SERVICE_ID','HL_TESTNET_PRICE_ROUNDING','HL_TESTNET_EXIT_TYPE','HL_TESTNET_JOURNAL_BACKEND'):
            env={**e,key:'wrong'}
            with self.assertRaises(JournalError):task.validate_ticket(json.dumps(t),env,now=NOW)
    def test_missing_or_extra_ticket_fields(self):
        t,e=fixture()
        for bad in ({**t,'secret':'NEVER_ECHO'},{k:v for k,v in t.items() if k!='message_id'}):
            with self.assertRaises(JournalError):task.validate_ticket(json.dumps(bad),e,now=NOW)
    def test_digest_changed_or_delivery_not_verified(self):
        t,e=fixture()
        for bad in ({**t,'signal_sha256':'bad'},{**t,'delivery_verified':False}):
            with self.assertRaises(JournalError):task.validate_ticket(json.dumps(bad),e,now=NOW)
    def test_ticket_expiry_and_future_issued(self):
        t,e=fixture()
        for when in (NOW+timedelta(minutes=3),NOW-timedelta(minutes=1)):
            with self.assertRaises(JournalError):task.validate_ticket(json.dumps(t),e,now=when)
    def test_overlong_source_window_refused(self):
        t,e=fixture();t['source_expires_at']=(NOW+timedelta(hours=1)).isoformat()
        with self.assertRaises(JournalError):task.validate_ticket(json.dumps(t),e,now=NOW)
    def test_inspection_allowed_when_expired_but_cannot_submit(self):
        t,e=fixture();e['HL_TESTNET_RUNTIME_MODE']=task.INSPECT
        self.assertEqual(task.validate_ticket(json.dumps(t),e,now=NOW+timedelta(days=1),inspection=True),t)
        with patch.object(task.PostgresJournal,'from_env',return_value=Mock()),patch('hl_testnet_runtime.persistent_execution.inspect_persisted',return_value={'status':'PREPARED_NOT_SUBMITTED','verified':False,'order_requests_sent':0}) as reader,patch('hl_testnet_runtime.persistent_execution.submit_persisted',side_effect=AssertionError('No send')):
            result=task.run_configured(e,now=NOW+timedelta(days=1))
        reader.assert_called_once();self.assertEqual(result['order_requests_sent'],0)
    def test_call_passes_exact_source_and_both_deadlines(self):
        t,e=fixture()
        with patch.object(task.PostgresJournal,'from_env',return_value=Mock()) as factory,patch('hl_testnet_runtime.persistent_execution.submit_persisted',return_value={'status':'FIXTURE','order_requests_sent':0}) as submit:
            task.run_configured(e,now=NOW)
        submit.assert_called_once_with(t['signal'],account=A,agent=B,exit_type='tp_limit_sl_market',enable_testnet=True,journal=factory.return_value,source_expires_at=t['source_expires_at'],approval_expires_at=t['expires_at'])
    def test_invalid_ticket_does_not_open_database_or_echo(self):
        _,e=fixture();e['HL_TESTNET_ATTEMPT_TICKET']='SECRET_INVALID'
        with patch.object(task.PostgresJournal,'from_env',side_effect=AssertionError('No DB')):
            result=task.run_configured(e,now=NOW)
        self.assertEqual(result['order_requests_sent'],0);self.assertNotIn('SECRET_INVALID',json.dumps(result))
    def test_legacy_window_stays_one_minute(self):
        self.assertFalse(source_fresh(NOW-timedelta(seconds=61),now=NOW))
        self.assertTrue(source_fresh(NOW-timedelta(seconds=59),now=NOW))
    def test_native_window_and_operator_deadline_both_enforced(self):
        at=NOW-timedelta(minutes=2);end=(at+timedelta(minutes=10)).isoformat()
        self.assertTrue(source_fresh(at,end,(NOW+timedelta(seconds=10)).isoformat(),now=NOW))
        self.assertFalse(source_fresh(at,end,(NOW-timedelta(seconds=1)).isoformat(),now=NOW))
        self.assertFalse(source_fresh(at,end,now=NOW+timedelta(minutes=8)))
    def test_bad_timestamps_fail_closed(self):
        for end in ('bad',NOW.replace(tzinfo=None).isoformat(),(NOW+timedelta(hours=1)).isoformat()):
            self.assertFalse(source_fresh(NOW-timedelta(minutes=2),end,now=NOW))


@unittest.skipUnless(CI_URL,'Requires disposable PostgreSQL')
class NativeExpiryPostgresTests(unittest.TestCase):
    setUp=PostgresTests.setUp
    _dispatch_fixture=PostgresTests._dispatch_fixture
    def test_original_outbox_expiry_dispatch_once_and_replay_zero(self):
        from .persistent_execution import submit_persisted
        from .postgres_journal import PostgresJournal
        now=datetime.now(timezone.utc);self.source['at']=(now-timedelta(minutes=2)).isoformat()
        expiry=(now+timedelta(minutes=8)).isoformat();approval=(now+timedelta(seconds=120)).isoformat()
        exchange,signer=self._dispatch_fixture()
        result=submit_persisted(self.source,account=A,agent=B,exit_type='tp_limit_sl_market',enable_testnet=True,journal=self.journal,source_expires_at=expiry,approval_expires_at=approval)
        self.assertEqual(result['status'],'VERIFIED_OPEN_PROTECTED',result)
        repeat=submit_persisted(self.source,account=A,agent=B,exit_type='tp_limit_sl_market',enable_testnet=True,journal=PostgresJournal.for_ci(CI_URL),source_expires_at=expiry,approval_expires_at=approval)
        self.assertTrue(repeat['replayed']);self.assertEqual(repeat['order_requests_sent'],0);signer.assert_called_once()
    def test_expired_authorization_never_signs(self):
        from .persistent_execution import submit_persisted
        exchange,signer=self._dispatch_fixture()
        now=datetime.now(timezone.utc)
        result=submit_persisted(self.source,account=A,agent=B,exit_type='tp_limit_sl_market',enable_testnet=True,journal=self.journal,approval_expires_at=(now-timedelta(seconds=1)).isoformat())
        self.assertEqual(result['status'],'SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST');signer.assert_not_called()


if __name__=='__main__':unittest.main()
