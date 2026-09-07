"""Real SQL lifecycle checks in a disposable schema of an explicit test DB.

Set RESEARCH_TEST_POSTGRES_URL in CI. Never falls back to the production URL.
The fixture schema is random and removed on completion; no Telegram is bound.
"""
import os
from pathlib import Path
from datetime import timedelta
import unittest
from uuid import uuid4

import research_ordered_experimental_store as store
import research_ordered_validation as validation
from research_ordered_experimental_selftest import fixture

TEST_URL = os.getenv('RESEARCH_TEST_POSTGRES_URL','').strip()


@unittest.skipUnless(TEST_URL,'explicit isolated PostgreSQL test URL not configured')
class PostgreSQLDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.rows import dict_row
        from psycopg import sql
        cls.schema='ordered_delivery_test_'+uuid4().hex
        cls.conn=psycopg.connect(TEST_URL,row_factory=dict_row,autocommit=True)
        cls.conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(cls.schema)))
        cls.conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(cls.schema)))
        cls.conn.execute('''
          CREATE TABLE research_events(event_id BIGINT PRIMARY KEY,event_kind TEXT,delivery_status TEXT,
              event_fingerprint TEXT,event_type TEXT,direction TEXT,symbol TEXT,alert_time_utc TIMESTAMPTZ,
              current_price DOUBLE PRECISION,engine_snapshot JSONB);
          CREATE TABLE research_ordered_formula_scopes(scope_key TEXT PRIMARY KEY,candidate_key TEXT);
          CREATE TABLE research_ordered_validation_freezes(freeze_id TEXT PRIMARY KEY,registration JSONB);
          CREATE TABLE research_ordered_validation_evaluations(freeze_id TEXT,evidence_sha256 TEXT,result JSONB,
              PRIMARY KEY(freeze_id,evidence_sha256));
          CREATE TABLE research_formula_alert_subscriptions(chat_id BIGINT PRIMARY KEY,active BOOLEAN);
          CREATE TABLE research_ordered_formula_matches(candidate_key TEXT,event_id BIGINT,symbol TEXT,
              direction TEXT,alert_time_utc TIMESTAMPTZ,snapshot_id TEXT,entry_price DOUBLE PRECISION,decision_features JSONB,
              PRIMARY KEY(candidate_key,event_id));
          CREATE TABLE research_event_btc_movements(event_id BIGINT,btc_parent_movement_id TEXT,membership_status TEXT,
              episode_policy_version TEXT,decision_time_utc TIMESTAMPTZ,btc_observed_close_utc TIMESTAMPTZ);
          CREATE TABLE research_btc_parent_movements(btc_parent_movement_id TEXT,episode_policy_version TEXT,
              evidence_eligible BOOLEAN,start_time_utc TIMESTAMPTZ,confirmed_at_utc TIMESTAMPTZ);
        ''')
        cls.conn.execute((Path(__file__).parent/'migrations/037_ordered_experimental_delivery.sql').read_text())

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        cls.conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(cls.schema)))
        cls.conn.close()

    def setUp(self):
        self.conn.execute('''TRUNCATE research_ordered_experimental_deliveries,research_ordered_experimental_eligibility,
            research_events,research_ordered_formula_scopes,research_ordered_validation_freezes,
            research_ordered_validation_evaluations,research_formula_alert_subscriptions,
            research_ordered_formula_matches,research_event_btc_movements,research_btc_parent_movements CASCADE''')
        scope,reg,result,event,assessed,*_=fixture()
        self.now=event['alert_time_utc']+timedelta(seconds=1)
        self.conn.execute('INSERT INTO research_ordered_formula_scopes VALUES(%s,%s)',(scope['scope_key'],scope['candidate_key']))
        self.conn.execute('INSERT INTO research_ordered_validation_freezes VALUES(%s,%s::jsonb)',(reg['freeze_id'],validation.canonical(reg)))
        self.conn.execute('INSERT INTO research_ordered_validation_evaluations VALUES(%s,%s,%s::jsonb)',
            (reg['freeze_id'],result['evidence_sha256'],validation.canonical(result)))
        self.conn.execute('''INSERT INTO research_ordered_experimental_eligibility
            (freeze_id,scope_key,evidence_sha256,evaluated_at_utc,published_at_utc,eligible_until_utc,ready)
            VALUES(%s,%s,%s,%s,%s,%s,TRUE)''',(reg['freeze_id'],scope['scope_key'],result['evidence_sha256'],
                assessed,assessed+timedelta(seconds=1),assessed+timedelta(minutes=30)))
        self.conn.execute('INSERT INTO research_formula_alert_subscriptions VALUES(42,TRUE)')
        self.conn.execute('INSERT INTO research_events VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)',
            tuple(event[k] for k in ('event_id','event_kind','delivery_status','event_fingerprint','event_type','source_direction','symbol','alert_time_utc','current_price'))+
            (validation.canonical(event['engine_snapshot']),))
        self.conn.execute('INSERT INTO research_ordered_formula_matches VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb)',
            tuple(event[k] for k in ('candidate_key','event_id','symbol','direction','alert_time_utc'))+
            ('test-snapshot',event['entry_price'],validation.canonical(event['decision_features'])))
        self.conn.execute('INSERT INTO research_event_btc_movements VALUES(%s,%s,%s,%s,%s,%s)',
            tuple(event[k] for k in ('event_id','btc_parent_movement_id','membership_status','episode_policy_version','decision_time_utc','btc_observed_close_utc')))
        self.conn.execute('INSERT INTO research_btc_parent_movements VALUES(%s,%s,%s,%s,%s)',
            tuple(event[k] for k in ('btc_parent_movement_id','episode_policy_version','parent_evidence_eligible','parent_start_time_utc','parent_confirmed_at_utc')))

    def enqueue_one(self):
        self.assertEqual(store.enqueue(self.conn,now=self.now)['enqueued'],1)
        return store.claim(self.conn,now=self.now)

    def test_migration_queue_dedup_claim_and_positive_ack(self):
        self.assertTrue(store.available(self.conn))
        item=self.enqueue_one()
        self.assertEqual(store.enqueue(self.conn,now=self.now)['enqueued'],0)
        self.assertIsNone(store.claim(self.conn,now=self.now))
        self.assertTrue(store.begin_send(self.conn,item,now=self.now))
        self.assertTrue(store.finish(self.conn,item,now=self.now,message_id=123))
        self.assertFalse(store.finish(self.conn,item,now=self.now,message_id=123))
        self.assertIsNone(store.claim(self.conn,now=self.now))

    def test_cancelled_definitely_unsent_reservation_is_reusable(self):
        self.assertEqual(store.enqueue(self.conn,now=self.now)['enqueued'],1)
        self.conn.execute('UPDATE research_formula_alert_subscriptions SET active=FALSE')
        self.assertIsNone(store.claim(self.conn,now=self.now))
        self.conn.execute('UPDATE research_formula_alert_subscriptions SET active=TRUE')
        self.assertEqual(store.enqueue(self.conn,now=self.now)['enqueued'],1)
        self.assertIsNotNone(store.claim(self.conn,now=self.now))

    def test_expired_claim_retries_but_expired_transport_never_retries(self):
        item=self.enqueue_one()
        later=self.now+timedelta(seconds=91)
        reclaimed=store.claim(self.conn,now=later)
        self.assertEqual(reclaimed['delivery_id'],item['delivery_id'])
        self.assertNotEqual(reclaimed['claim_token'],item['claim_token'])
        self.assertFalse(store.begin_send(self.conn,item,now=later))
        self.assertTrue(store.begin_send(self.conn,reclaimed,now=later))
        self.assertIsNone(store.claim(self.conn,now=later+timedelta(seconds=91)))
        row=self.conn.execute('SELECT status FROM research_ordered_experimental_deliveries').fetchone()
        self.assertEqual(row['status'],'UNKNOWN')
        self.assertFalse(store.finish(self.conn,reclaimed,now=later+timedelta(seconds=92),message_id=321))

    def test_qualification_revocation_is_checked_after_claim(self):
        item=self.enqueue_one()
        self.conn.execute('UPDATE research_ordered_experimental_eligibility SET ready=FALSE,eligible_until_utc=NULL')
        self.assertFalse(store.begin_send(self.conn,item,now=self.now))


if __name__=='__main__':
    unittest.main()
