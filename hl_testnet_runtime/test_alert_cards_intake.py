"""Receiver safety and real disposable PostgreSQL; no real keys or orders."""
from copy import deepcopy
from datetime import datetime,timezone
import hashlib
from io import BytesIO,StringIO
import json
import os
import time
import unittest
from unittest.mock import patch,Mock

import alert_cards_wire as w
from alert_cards_forwarder_selftest import delivery,KEY
from . import alert_cards_intake as mod
from . import test_trade_cards_phase1 as fixtures
from .postgres_journal import JournalError

ENV=dict(HL_TESTNET_CARDS_INTAKE='record_only_v1',HL_TESTNET_CARDS_INTAKE_SECRET=KEY,
    RENDER_SERVICE_ID='srv-dakptbh594qs7395460g',HL_TESTNET_CARDS_PHASE1='record_only_v1',
    HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',HL_TESTNET_RUNTIME_MODE='cancel_monitor_testnet_v1')


def request(value=None):
    raw=w.encoded(delivery() if value is None else value);stamp=str(int(time.time()))
    return dict(REQUEST_METHOD='POST',PATH_INFO=w.PATH,QUERY_STRING='',CONTENT_TYPE='application/json',
        CONTENT_LENGTH=str(len(raw)),HTTP_X_CARD_TIMESTAMP=stamp,HTTP_X_CARD_SIGNATURE=w.signature(KEY,stamp,raw),
        **{'wsgi.input':BytesIO(raw)})


def invoke(env):
    status=[]
    body=b''.join(mod.application(env,lambda s,h:status.append(s)))
    return status[0],json.loads(body)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        p=patch.dict(mod.os.environ,ENV,clear=True);p.start();self.addCleanup(p.stop)
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No exchange request'))
        p.start();self.addCleanup(p.stop)
    def test_valid_auth_can_only_call_record_receiver(self):
        with patch.object(mod.PostgresJournal,'from_env',return_value=object()),patch.object(mod,'ReceiptStore'),\
             patch.object(mod,'accept',return_value={'status':'RECORDED','record_only':True}) as accept:
            self.assertEqual(invoke(request())[0],'200 OK')
        accept.assert_called_once()
    def test_default_disabled_cannot_read_db_or_key(self):
        with patch.dict(mod.os.environ,{},clear=True),patch.object(mod.PostgresJournal,'from_env',side_effect=AssertionError('No DB')):
            self.assertEqual(invoke(request())[0],'404 Not Found')
    def test_wrong_service_or_execution_mode_disabled(self):
        for k,v in [('RENDER_SERVICE_ID','other'),('HL_TESTNET_RUNTIME_MODE','single_testnet_attempt_v1')]:
            with patch.dict(mod.os.environ,{k:v}):self.assertEqual(invoke(request())[0],'404 Not Found')
    def test_missing_wrong_expired_or_modified_auth_has_no_db_access(self):
        with patch.object(mod.PostgresJournal,'from_env',side_effect=AssertionError('No DB')):
            for k,v in [('HTTP_X_CARD_SIGNATURE','00'*32),('HTTP_X_CARD_TIMESTAMP','1'),('HTTP_X_CARD_SIGNATURE','')]:
                r=request();r[k]=v;self.assertEqual(invoke(r)[0],'403 Forbidden')
    def test_public_query_get_and_oversize_refused(self):
        for k,v,prefix in [('QUERY_STRING','key=x','405'),('REQUEST_METHOD','GET','405'),
                           ('CONTENT_LENGTH','9999999','413'),('CONTENT_TYPE','text/plain','415')]:
            r=request();r[k]=v;self.assertTrue(invoke(r)[0].startswith(prefix))
    def test_failure_redacted_retryable_not_success(self):
        with patch.object(mod.PostgresJournal,'from_env',side_effect=ValueError('SECRET_DATABASE_URL')):
            s,result=invoke(request())
        self.assertTrue(s.startswith('503'));self.assertNotIn('SECRET',str(result))
    def test_incomplete_body_is_not_accepted(self):
        r=request();r['CONTENT_LENGTH']=str(int(r['CONTENT_LENGTH'])+1)
        self.assertTrue(invoke(r)[0].startswith('400'))
    def test_no_signing_or_trading_import(self):
        import inspect
        src=inspect.getsource(mod)
        for text in ('import hyperliquid_testnet_executor','_wallet(',"'/exchange'",'submit_persisted(', 'AGENT_KEY'):
            self.assertNotIn(text,src)


@unittest.skipUnless(fixtures.CI,'Requires disposable localhost PostgreSQL')
class IntakePostgresTests(unittest.TestCase):
    def setUp(self):
        fixtures.CardPostgresTests.setUp(self)
        mod.initialize(self.journal);self.receipts=mod.ReceiptStore(self.journal)
    def accept(self,value=None):
        return mod.accept(w.encoded(delivery() if value is None else value),self.receipts,read_metadata=lambda:fixtures.META)
    def counts(self):
        with self.journal._transaction() as conn:
            return (conn.execute('SELECT count(*) FROM hl_testnet_cards_v1.cards').fetchone()[0],
                conn.execute('SELECT count(*) FROM hl_testnet_cards_v1.delivery_receipts').fetchone()[0],
                conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0])
    def test_delivered_to_card_to_immutable_receipt_no_trade(self):
        r=self.accept();self.assertEqual(r['status'],'RECORDED');self.assertEqual(self.counts(),(1,1,0))
        rec=self.receipts.get(r['receipt_id']);card=self.store.load(rec['card_id'])
        self.assertEqual(card['state'],'RECORDED_ONLY');self.assertEqual(card['account_role'],'long_account')
        self.assertIsNone(card['actual_execution']);self.assertEqual(card['risk']['planned_usd'],'10')
        self.assertEqual(card['prepared']['source']['at'],delivery()['source_at'])
    def test_both_sources_both_directions_are_supported_without_accounts(self):
        for side in ('LONG','SHORT'):
            for family in ('manual','dual_cvd65'):
                self.accept(delivery(side,identity=side+family,family=family))
        self.assertEqual(self.counts(),(4,4,0))
    def test_replay_after_restart_creates_nothing(self):
        self.accept()
        from .postgres_journal import PostgresJournal
        r=mod.accept(w.encoded(delivery()),mod.ReceiptStore(PostgresJournal.for_ci(fixtures.CI)),read_metadata=lambda:fixtures.META)
        self.assertEqual(r['status'],'DUPLICATE');self.assertEqual(self.counts(),(1,1,0))
    def test_changed_source_same_intent_does_not_overwrite(self):
        old=self.accept();v=delivery();v['text']=v['text'].replace('טייק פרופיט:</b> 102','טייק פרופיט:</b> 103')
        self.assertEqual(self.accept(v)['status'],'REJECTED');self.assertEqual(self.counts(),(1,2,0))
        card=self.store.load(self.receipts.get(old['receipt_id'])['card_id'])
        self.assertEqual(card['prepared']['source']['take_profit'],'102')
    def test_malformed_authenticated_record_quarantined_not_fabricated(self):
        v=delivery();v['text']=v['text'].split('\nסטופלוס')[0].replace('סטופלוס','חסר')
        self.assertEqual(self.accept(v)['status'],'REJECTED');self.assertEqual(self.counts(),(0,1,0))
    def test_unknown_extra_secret_is_not_persisted(self):
        v={**delivery(),'secret':'DO_NOT_PERSIST'}
        self.assertEqual(self.accept(v)['status'],'REJECTED')
        with self.journal._transaction() as conn:
            rows=conn.execute('SELECT source,reason FROM hl_testnet_cards_v1.delivery_receipts').fetchall()
        self.assertNotIn('DO_NOT_PERSIST',str(rows))
    def test_metadata_failure_retains_retry_no_fake_receipt(self):
        with self.assertRaises(RuntimeError):
            mod.accept(w.encoded(delivery()),self.receipts,read_metadata=Mock(side_effect=RuntimeError('offline')))
        self.assertEqual(self.counts(),(0,0,0));self.assertEqual(self.accept()['status'],'RECORDED')
    def test_failure_between_card_and_receipt_can_resume_without_duplicate(self):
        with patch.object(self.receipts,'save',side_effect=JournalError('UNAVAILABLE')):
            with self.assertRaises(JournalError):self.accept()
        self.assertEqual(self.counts(),(1,0,0))
        self.assertEqual(self.accept()['status'],'DUPLICATE');self.assertEqual(self.counts(),(1,1,0))
    def test_concurrent_delivery_one_card_and_one_receipt(self):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:answers=list(pool.map(lambda _:self.accept(),range(4)))
        self.assertEqual(sum(x['status']=='RECORDED' for x in answers),1);self.assertEqual(self.counts(),(1,1,0))
    def test_logs_never_contain_messages_prices_or_auth(self):
        with patch('sys.stdout',new_callable=StringIO) as output:self.accept()
        value=output.getvalue()
        for secret in (KEY,delivery()['text'],'DATABASE_URL','"entry"'):self.assertNotIn(secret,value)
        self.assertIn('"order_requests_sent": 0',value)
    def test_schema_repeated_no_overwrites(self):
        self.accept();mod.initialize(self.journal);self.assertEqual(self.counts(),(1,1,0))


if __name__=='__main__':unittest.main()
