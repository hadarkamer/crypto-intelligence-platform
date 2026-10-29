"""Real PostgreSQL18 checks for the new isolated observer and source query.

Only a local test database is accepted. All market rows/quotes are synthetic,
and every table belongs to a randomly named test schema, never production.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlparse
import uuid

import research_forward_paper_store as persistence
import research_liquidity_forward_engine as core
import research_liquidity_forward_source as source
import research_liquidity_forward_source_selftest as fixtures
import research_liquidity_forward_worker as worker

ROOT = Path(__file__).resolve().parent
DSN = os.getenv('RESEARCH_TEST_POSTGRES_URL') or os.getenv('TEST_DATABASE_URL')
OLD_ID = 'doge-eth-first-observed-paper-render-v1'


def text(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


SCHEMA_DDL = (
    '''CREATE TABLE research_events (
      event_id BIGINT PRIMARY KEY,schema_version TEXT,event_kind TEXT,event_type TEXT,
      alert_time_utc TIMESTAMPTZ,symbol TEXT,direction TEXT,source_side TEXT,timeframe TEXT,
      score DOUBLE PRECISION,current_price DOUBLE PRECISION,target_price DOUBLE PRECISION,
      initial_target_distance_pct DOUBLE PRECISION,categories JSONB,setup_key TEXT,
      event_fingerprint TEXT,strategy_version TEXT,code_version TEXT,runtime_session_id TEXT,
      capture_stage TEXT,delivery_status TEXT,delivery_attempted_at_utc TIMESTAMPTZ,
      delivered_at_utc TIMESTAMPTZ,created_at TIMESTAMPTZ,engine_snapshot JSONB)''',
    '''CREATE TABLE research_event_btc_movements (
      event_id BIGINT,episode_policy_version TEXT,btc_parent_movement_id TEXT,
      created_at_utc TIMESTAMPTZ,decision_time_utc TIMESTAMPTZ,
      btc_observed_close_utc TIMESTAMPTZ,membership_status TEXT,
      PRIMARY KEY(event_id,episode_policy_version))''',
    '''CREATE TABLE research_btc_parent_movements (
      btc_parent_movement_id TEXT,episode_policy_version TEXT,start_time_utc TIMESTAMPTZ,
      end_time_utc TIMESTAMPTZ,confirmed_at_utc TIMESTAMPTZ,created_at_utc TIMESTAMPTZ,
      evidence_eligible BOOLEAN,boundary_reason TEXT,
      PRIMARY KEY(btc_parent_movement_id,episode_policy_version))''',
    '''CREATE TABLE research_btc_price_bars (
      close_time_utc TIMESTAMPTZ PRIMARY KEY,open_time_utc TIMESTAMPTZ,created_at_utc TIMESTAMPTZ)''',
    '''CREATE TABLE research_combined_shadow_objects (
      sha256 TEXT PRIMARY KEY,codec TEXT,raw_bytes BIGINT,payload BYTEA,created_at_utc TIMESTAMPTZ DEFAULT now())''',
    '''CREATE TABLE research_combined_shadow_observations (
      observation_id TEXT PRIMARY KEY,root_sha256 TEXT,manifest_json TEXT,created_at_utc TIMESTAMPTZ)''',
    '''CREATE TABLE research_combined_shadow_links (
      link_receipt_id TEXT PRIMARY KEY,observation_id TEXT,receipt_json TEXT,created_at_utc TIMESTAMPTZ)''',
)


class CommitAcknowledgementLost:
    """Execute a real COMMIT, then lose only its acknowledgement once."""
    def __init__(self, connection):
        self.connection = connection
        self.armed = True

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def cursor(self):
        parent = self
        original = self.connection.cursor()
        class Cursor:
            def __getattr__(self, name):
                return getattr(original, name)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                original.close()
            def execute(self, sql, params=None):
                result = original.execute(sql, params)
                if isinstance(sql, str) and sql.strip().upper() == 'COMMIT' and parent.armed:
                    parent.armed = False
                    raise RuntimeError('Synthetic lost COMMIT acknowledgement')
                return result
        return Cursor()


@unittest.skipUnless(DSN, 'Real PostgreSQL18 integration requires TEST_DATABASE_URL')
class PostgreSQL18ObserverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parsed = urlparse(DSN)
        if parsed.scheme not in ('postgres', 'postgresql') or parsed.hostname not in ('localhost', '127.0.0.1', 'postgres') or 'test' not in parsed.path.lower():
            raise RuntimeError('Integration self-test accepts only a named local test database')
        try:
            import psycopg2 as driver
            from psycopg2 import sql
        except ImportError:
            import psycopg as driver
            from psycopg import sql
        cls.driver, cls.sql = driver, sql
        connection = driver.connect(DSN)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_setting('server_version_num')::integer,clock_timestamp()")
                version, now = cursor.fetchone()
            if not 180000 <= version < 190000:
                raise RuntimeError('Integration self-test requires real PostgreSQL18')
            freeze = datetime.fromisoformat('2026-10-29T12:30:58+00:00')
            if now < freeze + timedelta(minutes=70):
                raise unittest.SkipTest('Real causal warm-up fixture needs a server clock after protocol freeze plus70minutes')
            print('[LIQUIDITY_PG18] real PostgreSQL18 source/checkpoint integration', flush=True)
        finally:
            connection.close()

    def setUp(self):
        self.schema = 'liquidity_forward_test_' + uuid.uuid4().hex
        assert re.fullmatch(r'liquidity_forward_test_[0-9a-f]{32}', self.schema)
        self.stores, self.engines = [], []
        admin = self.driver.connect(DSN)
        admin.autocommit = True
        try:
            with admin.cursor() as cursor:
                cursor.execute(self.sql.SQL('CREATE SCHEMA {}').format(self.sql.Identifier(self.schema)))
        finally:
            admin.close()
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                for statement in SCHEMA_DDL + persistence.DDL:
                    cursor.execute(statement)
                cursor.execute("INSERT INTO research_forward_paper_studies(study_id,config_sha256,policy_sha256,policy_json,state_json,last_status) VALUES(%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)",
                    (OLD_ID, 'a'*64, 'b'*64, '{"sentinel":"old-policy"}', '{"untouched":true}', '{"state":"old-study-sentinel"}'))
                cursor.execute('SELECT clock_timestamp()')
                self.now = cursor.fetchone()[0]
            connection.commit()
        finally:
            connection.close()
        self.config = worker.build_config()
        self.pins = fixtures.registry()[fixtures.COMMIT]
        self.epoch = self.now - timedelta(hours=1)
        self.alert = self.now - timedelta(seconds=6)
        self._old_control = self.scalar("SELECT row_to_json(s)::text FROM research_forward_paper_studies s WHERE study_id=%s", (OLD_ID,))

    def tearDown(self):
        for engine in reversed(self.engines):
            try:
                engine.close()
            except Exception:
                pass
        for store in reversed(self.stores):
            try:
                store.close()
            except Exception:
                pass
        try:
            if hasattr(self, '_old_control'):
                self.assertEqual(self.scalar("SELECT row_to_json(s)::text FROM research_forward_paper_studies s WHERE study_id=%s", (OLD_ID,)), self._old_control)
        finally:
            assert re.fullmatch(r'liquidity_forward_test_[0-9a-f]{32}', self.schema)
            admin = self.driver.connect(DSN)
            admin.autocommit = True
            try:
                with admin.cursor() as cursor:
                    cursor.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))
            finally:
                admin.close()

    def connect(self, dsn=None):
        return self.driver.connect(DSN, options='-c search_path=' + self.schema)

    def scalar(self, statement, params=None):
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement, params)
                return cursor.fetchone()[0]
        finally:
            connection.close()

    def seeded_store(self):
        # This is a pre-existing synthetic study fixture, not a fake live activation.
        policy = {**deepcopy(self.config), 'study_start_utc': self.epoch.isoformat()}
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute('INSERT INTO research_forward_paper_studies(study_id,config_sha256,policy_sha256,policy_json,last_status) VALUES(%s,%s,%s,%s::jsonb,%s::jsonb)',
                    (core.STUDY_ID, persistence._hash(self.config), persistence._hash(policy), text(policy), '{"state":"SYNTHETIC_FIXTURE"}'))
            connection.commit()
        finally:
            connection.close()
        return self.open_store()

    def open_store(self):
        store = persistence.PostgresCheckpoint(DSN, core.STUDY_ID, self.config, connect=self.connect)
        self.stores.append(store)
        self.assertTrue(store.acquire())
        engine = store.restore_engine(core.PaperStudy)
        self.engines.append(engine)
        engine.observe_runtime_identity({'runtime_commit': fixtures.COMMIT, 'code_versions': self.pins,
            'python_implementation': 'CPython', 'python_version': '3.11.9',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'protocol_sha256': core.PROTOCOL_SHA256})
        store.checkpoint(engine, {'state': 'READY_FOR_SOURCE', 'synthetic_integration': True})
        return store, engine

    def read_source(self, engine, bootstrap=False):
        return source.fetch_candidates(DSN, engine.policy, bootstrap=bootstrap,
            eligible_keys=engine.eligible_event_keys(), code_registry=engine.source_code_registry(), connect=self.connect)

    def apply(self, store, engine, packet):
        summary = engine.observe_candidates(packet)
        self.assertIsNone(summary['last_observation_rejected'])
        store.checkpoint(engine, {'state': 'OBSERVING', 'synthetic_integration': True})
        return summary

    def fixture(self, kind, event_id, when, **kwargs):
        with patch.object(fixtures, 'EPOCH', self.epoch), patch.object(fixtures.shadow.platform, 'python_version', return_value='3.11.9'):
            record = fixtures.native(event_id, when=when, **kwargs) if kind == 'native' else fixtures.combined(event_id, when=when, **kwargs)
        parent = json.loads(record['parent_json_text'])
        closed = when.replace(second=0, microsecond=0) - timedelta(milliseconds=1)
        parent['btc_observed_close_utc'] = closed.isoformat()
        parent['btc_bar_created_at_utc'] = (closed + timedelta(milliseconds=100)).isoformat()
        record['parent_json_text'] = text(parent)
        return record

    def insert_record(self, record, *, include_links=True):
        event = json.loads(record['event_json_text'])
        fields = ('event_id','schema_version','event_kind','event_type','alert_time_utc','symbol','direction','source_side','timeframe','score','current_price','target_price','initial_target_distance_pct','categories','setup_key','event_fingerprint','strategy_version','code_version','runtime_session_id','capture_stage','delivery_status','delivery_attempted_at_utc','delivered_at_utc','created_at','engine_snapshot')
        values = [text(event.get(key, [] if key == 'categories' else {})) if key in ('categories','engine_snapshot') else event.get(key) for key in fields]
        placeholders = ','.join('%s::jsonb' if key in ('categories','engine_snapshot') else '%s' for key in fields)
        parent = json.loads(record['parent_json_text'])
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute('INSERT INTO research_events(' + ','.join(fields) + ') VALUES(' + placeholders + ')', values)
                cursor.execute('INSERT INTO research_btc_parent_movements(btc_parent_movement_id,episode_policy_version,start_time_utc,confirmed_at_utc,created_at_utc,evidence_eligible,boundary_reason) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                    (parent['btc_parent_movement_id'],parent['episode_policy_version'],parent['parent_start_time_utc'],parent['parent_confirmed_at_utc'],parent['parent_created_at_utc'],True,'SYNTHETIC_FULL_REVERSAL'))
                cursor.execute('INSERT INTO research_event_btc_movements(event_id,episode_policy_version,btc_parent_movement_id,created_at_utc,decision_time_utc,btc_observed_close_utc,membership_status) VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (event['event_id'],parent['episode_policy_version'],parent['btc_parent_movement_id'],parent['membership_created_at_utc'],event['alert_time_utc'],parent['btc_observed_close_utc'],'LIVE'))
                closed = datetime.fromisoformat(parent['btc_observed_close_utc'])
                cursor.execute('INSERT INTO research_btc_price_bars(close_time_utc,open_time_utc,created_at_utc) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                    (closed,closed-timedelta(minutes=1)+timedelta(milliseconds=1),parent['btc_bar_created_at_utc']))
                for observation in record['combined_observations']:
                    cursor.execute('INSERT INTO research_combined_shadow_observations(observation_id,root_sha256,manifest_json,created_at_utc) VALUES(%s,%s,%s,%s)',
                        (observation['observation_id'],observation['root_sha256'],observation['manifest_json_text'],observation['created_at_utc']))
                    for item in observation['objects']:
                        cursor.execute('INSERT INTO research_combined_shadow_objects(sha256,codec,raw_bytes,payload) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                            (item['sha256'],item['codec'],item['raw_bytes'],base64.b64decode(item['compressed_b64'])))
                    if include_links:
                        self.insert_links(cursor, observation)
            connection.commit()
        finally:
            connection.close()

    def insert_links(self, cursor, observation):
        for link in observation['links']:
            cursor.execute('INSERT INTO research_combined_shadow_links(link_receipt_id,observation_id,receipt_json,created_at_utc) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                (link['link_receipt_id'],observation['observation_id'],link['receipt_json_text'],link['created_at_utc']))

    def add_links(self, record):
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                for observation in record['combined_observations']:
                    self.insert_links(cursor, observation)
            connection.commit()
        finally:
            connection.close()

    def bootstrap(self, store, engine):
        packet = self.read_source(engine, bootstrap=True)
        self.assertEqual(packet['source_population_count'], 0)
        self.apply(store, engine, packet)

    def test_real_registration_lock_and_epoch_survive_restore(self):
        before = datetime.now(timezone.utc)
        store, engine = self.open_store()
        epoch = engine.policy['study_start_utc']
        self.assertGreaterEqual(datetime.fromisoformat(epoch), before)
        other = persistence.PostgresCheckpoint(DSN, core.STUDY_ID, self.config, connect=self.connect)
        self.stores.append(other)
        self.assertFalse(other.acquire())
        self.bootstrap(store, engine)
        sequence = store.journal_sequence
        engine.close()
        store.close()
        resumed, restored = self.open_store()
        self.assertEqual(restored.policy['study_start_utc'], epoch)
        self.assertTrue(restored.summary()['bootstrap_complete'])
        self.assertGreater(resumed.journal_sequence, sequence)
        self.assertEqual(restored.source_code_registry()[fixtures.COMMIT], self.pins)
        self.assertEqual(self.scalar("SELECT count(*) FROM research_forward_paper_journal WHERE study_id=%s AND kind='SOURCE_CLOSURE'", (core.STUDY_ID,)), 1)

    def test_native_sql_entry_quote_gap_and_restart_preserve_case(self):
        store, engine = self.seeded_store()
        self.bootstrap(store, engine)
        self.insert_record(self.fixture('native', 100, self.alert-timedelta(minutes=15), scan='prior'))
        self.insert_record(self.fixture('native', 101, self.alert, scan='current'))
        packet = self.read_source(engine)
        self.assertEqual([row['event_id'] for row in packet['rows']], [101])
        summary = self.apply(store, engine, packet)
        first_seen = engine._load()['first_seen'][self.config['scopes'][0]['scope_key']+':101']['first_eligible_seen_utc']
        again = self.read_source(engine)
        self.assertEqual(again['source_closure_sha256'], packet['source_closure_sha256'])
        self.apply(store, engine, again)
        self.assertEqual(engine._load()['first_seen'][self.config['scopes'][0]['scope_key']+':101']['first_eligible_seen_utc'], first_seen)
        self.assertEqual(self.scalar("SELECT count(*) FROM research_forward_paper_journal WHERE study_id=%s AND kind='SOURCE_CLOSURE'", (core.STUDY_ID,)), 2)
        choice = summary['books']['DOGE_only']['choices']['synthetic-parent']
        planned = datetime.fromisoformat(choice['planned_entry_utc'].replace('Z','+00:00'))
        def quote(identifier, seconds, price):
            started = planned+timedelta(seconds=seconds)
            return {'packet_id':identifier,'request_started_utc':started.isoformat(),
                    'response_received_utc':(started+timedelta(milliseconds=100)).isoformat(),
                    'source':'BINANCE_SPOT_BOOK_TICKER','monotonic_elapsed_seconds':0.1,
                    'quotes':[{'symbol':'DOGE','bid':str(price),'ask':str(price)}]}
        engine.observe_quotes(quote('entry',1,100))
        store.checkpoint(engine, {'state':'OBSERVING'})
        engine.observe_quotes(quote('gap',35,100))
        store.checkpoint(engine, {'state':'OBSERVING'})
        engine.close()
        store.close()
        resume_time = planned+timedelta(seconds=40)
        class SyntheticClock(datetime):
            @classmethod
            def now(cls,tz=None):
                return resume_time if tz is None else resume_time.astimezone(tz)
        with patch.object(core,'datetime',SyntheticClock):
            resumed, restored = self.open_store()
        position = restored.summary()['books']['DOGE_only']['active']
        self.assertTrue(position['path_uncertain'])
        self.assertIn('QUOTE_GAP', position['uncertainty_reasons'])
        self.assertIn('PROCESS_RESTART', position['uncertainty_reasons'])
        result = restored.observe_quotes(quote('exit',45,99.4))
        resumed.checkpoint(restored, {'state':'OBSERVING'})
        closed = result['books']['DOGE_only']['closed']
        self.assertEqual(len(closed),1)
        self.assertEqual(closed[0]['status'],'OBSERVED_TARGET')
        self.assertAlmostEqual(float(closed[0]['net_return_pct']),0.5)

    def test_combined_source_real_sql_link_is_pending_then_retryable(self):
        store, engine = self.seeded_store()
        self.bootstrap(store,engine)
        self.insert_record(self.fixture('native',100,self.alert-timedelta(minutes=15),scan='prior'))
        combined = self.fixture('combined',201,self.alert)
        self.insert_record(combined,include_links=False)
        pending = self.read_source(engine)
        self.assertEqual(pending['rows'],[])
        self.assertEqual(pending['pending_liquidity_points'],1)
        self.apply(store,engine,pending)
        self.add_links(combined)
        complete = self.read_source(engine)
        self.assertEqual([row['event_id'] for row in complete['rows']],[201])
        self.assertEqual(complete['rows'][0]['source_contract'],'doge-combined-selected-slot-shadow-v1')
        self.assertGreaterEqual(datetime.fromisoformat(complete['response_received_utc']),datetime.fromisoformat(complete['rows'][0]['liquidity_evidence_created_at_utc']))
        result = self.apply(store,engine,complete)
        self.assertEqual(result['books']['DOGE_only']['choices']['synthetic-parent']['event_id'],201)

    def test_late_prior_proof_does_not_replace_frozen_parent(self):
        store,engine = self.seeded_store()
        self.bootstrap(store,engine)
        self.insert_record(self.fixture('native',99,self.alert-timedelta(minutes=20),scan='prior'))
        combined = self.fixture('combined',200,self.alert-timedelta(minutes=10))
        self.insert_record(combined,include_links=False)
        self.insert_record(self.fixture('native',101,self.alert,scan='current'))
        pending = self.read_source(engine)
        match = next(row for row in pending['rows'] if row['event_id']==101)
        self.assertEqual(match['pending_prior_count'],1)
        self.assertTrue(match['source_history_uncertain'])
        self.apply(store,engine,pending)
        self.add_links(combined)
        later = self.read_source(engine)
        self.assertNotIn(101,[row['event_id'] for row in later['rows']])
        result = self.apply(store,engine,later)
        choice = result['books']['DOGE_only']['choices']['synthetic-parent']
        self.assertEqual(choice['event_id'],101)
        self.assertTrue(choice['source_history_uncertain'])

    def test_real_source_population_overflow_is_never_truncated(self):
        store,engine = self.seeded_store()
        self.bootstrap(store,engine)
        self.insert_record(self.fixture('native',100,self.alert-timedelta(minutes=15),scan='prior'))
        self.insert_record(self.fixture('native',101,self.alert,scan='current'))
        with patch.object(source,'MAX_SOURCE_EVENTS',1),self.assertRaises(source.SourceRejected):
            self.read_source(engine)
        self.assertEqual(engine.summary()['raw_first_seen_count'],0)

    def test_real_commit_ack_loss_restores_complete_new_receipt_once(self):
        store,engine = self.seeded_store()
        packet = self.read_source(engine,bootstrap=True)
        engine.observe_candidates(packet)
        store._conn = CommitAcknowledgementLost(store._conn)
        with self.assertRaises(persistence.CheckpointError):
            store.checkpoint(engine, {'state':'OBSERVING'})
        self.assertIsNone(store._conn)
        engine.close()
        store.close()
        resumed,restored = self.open_store()
        self.assertTrue(restored.summary()['bootstrap_complete'])
        duplicate = restored.observe_candidates(packet)
        self.assertTrue(duplicate['idempotent_duplicate'])
        resumed.checkpoint(restored,{'state':'OBSERVING'})
        self.assertEqual(self.scalar("SELECT count(*) FROM research_forward_paper_journal WHERE study_id=%s AND input_key=%s",(core.STUDY_ID,'CANDIDATES:'+packet['poll_id'])),1)
        self.assertEqual(self.scalar("SELECT count(*) FROM research_forward_paper_journal WHERE study_id=%s AND kind='SOURCE_CLOSURE'",(core.STUDY_ID,)),1)


if __name__=='__main__':
    unittest.main(verbosity=2)
