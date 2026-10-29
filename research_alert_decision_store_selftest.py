"""Finite offline store tests; PostgreSQL writes require an explicit local test DSN."""
from copy import deepcopy
from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_alert_decision_capture as capture
import research_alert_decision_store as store

BASE = 'shared-watch:2026-10-29T17:02:15.112702+00:00'


def capsule(cycle=BASE):
    value = dict(version=store.CAPTURE_VERSION,cycle_id=cycle,status='COMPLETE',payload={
        'source_code_sha256':{'main.py':'1'*64,'research_alert_decision_capture.py':'2'*64},
        'score_payload_sha256':'3'*64,'records':[],
        'test_scalars':{'zero':-0.0,'truth':False,'number':1.0,'label':'בדיקה'}})
    value['payload_sha256'] = store.digest(value)
    return value


def rehash(value):
    value['payload_sha256'] = store.digest({k:v for k,v in value.items() if k != 'payload_sha256'})
    return value


def fresh_module():
    spec = importlib.util.spec_from_file_location('isolated_decision_store_'+uuid4().hex,Path(store.__file__))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def wait_for(predicate, seconds=3.):
    deadline = time.monotonic()+seconds
    while not predicate() and time.monotonic() < deadline:
        time.sleep(.005)
    if not predicate():
        raise AssertionError('bounded worker wait timed out')


class StoreTests(unittest.TestCase):
    def test_real_collector_contract_and_exact_scalar_hash(self):
        token = capture.begin(BASE,{'scope':'OFFLINE_SELFTEST'})
        try:
            capture.bind(score_payload_sha256='3'*64,source_code_sha256={'main.py':'1'*64})
            capture.stage('special_native_batch','REACHED')
            state = {'before':False}
            def native():
                state['before'] = True
                return ['message never retained']
            original = capture.observe_call('special_native_batch',native,state=state,source={'score':65.})
            value = capture.seal()
        finally:
            capture.reset(token)
        self.assertEqual(original,['message never retained'])
        self.assertTrue(capture.validate_capsule(value))
        prepared = store.prepare(value)
        self.assertEqual(prepared['payload_json'].encode(),capture.canonical_bytes(value))
        self.assertNotIn('message never retained',prepared['payload_json'])
        fixture = capsule()
        self.assertIn('-0.0',store.prepare(fixture)['payload_json'])
        self.assertEqual(store.prepare(fixture)['payload_bytes'],len(store.canonical(fixture).encode()))

    def test_sqlite_roundtrip_duplicate_and_collision_are_append_only(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'capsules.sqlite3'
            backend = store.SQLiteStore(path)
            original = capsule()
            first = backend.persist(original)
            second = backend.persist(deepcopy(original))
            self.assertTrue(first['persisted'])
            self.assertFalse(first['idempotent_existing'])
            self.assertTrue(second['idempotent_existing'])
            changed = rehash({**deepcopy(original),'status':'PARTIAL'})
            with self.assertRaisesRegex(store.CapsuleValidationError,'CYCLE_IDENTITY_COLLISION'):
                backend.persist(changed)
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute('SELECT payload_sha256,payload_json FROM research_alert_decision_cycles').fetchall()
            self.assertEqual(len(rows),1)
            self.assertEqual(rows[0][0],original['payload_sha256'])
            self.assertEqual(rows[0][1],store.canonical(original))
            self.assertEqual(store.prepare(json.loads(rows[0][1]))['payload_sha256'],original['payload_sha256'])

    def test_strict_bounds_hash_and_sensitive_fields(self):
        invalid = [None,[],{'unknown':True}]
        for member in (float('nan'),float('inf'),object(),10**10000):
            value = capsule(); value['payload']['bad'] = member; invalid.append(value)
        value = capsule(); value['payload']['chat_id'] = 123; invalid.append(value)
        value = capsule(); value['payload']['attempt_token'] = 'private'; invalid.append(value)
        value = capsule(); value['payload_sha256'] = '0'*64; invalid.append(value)
        value = capsule(); value['cycle_id'] = 'shared-watch:not-a-clock'; invalid.append(value)
        value = capsule(); value['payload']['source_code_sha256']['main.py'] = True; invalid.append(value)
        value = capsule(); value['payload']['bad'] = {1:'integer key'}; invalid.append(value)
        value = capsule(); value['payload']['bad'] = ['x'*16384]*17; invalid.append(value)
        value = capsule(); value['payload']['bad'] = [None]*store.MAX_NODES; invalid.append(value)
        value = capsule(); deep = {}
        for _ in range(store.MAX_DEPTH+1): deep = {'deeper':deep}
        value['payload']['bad'] = deep; invalid.append(value)
        value = capsule(); cycle = []; cycle.append(cycle); value['payload']['bad'] = cycle; invalid.append(value)
        for value in invalid:
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(store.CapsuleValidationError): store.prepare(value)
        # Validation does not mutate caller-owned evidence.
        original = capsule(); before = deepcopy(original)
        store.prepare(original)
        self.assertEqual(original,before)

    def test_nonblocking_queue_has_eight_outstanding_slots_and_freezes_inputs(self):
        module = fresh_module()
        entered, release = threading.Event(),threading.Event()
        retained = []
        class SlowBackend:
            def persist_prepared(self,prepared,enqueued_at):
                entered.set()
                if not release.wait(3.): raise RuntimeError('selftest release deadline')
                retained.append(deepcopy(prepared))
                return dict(persisted=True,idempotent_existing=False,cycle_id=prepared['cycle_id'],
                    capture_version=prepared['capture_version'],capture_status=prepared['capture_status'],
                    payload_sha256=prepared['payload_sha256'],created_at_utc=enqueued_at)
        module._STORE = SlowBackend()
        first = capsule()
        try:
            response = module.submit(first)
            self.assertEqual(response['status'],'QUEUED')
            self.assertFalse(response['durable'])
            self.assertTrue(entered.wait(2.))
            self.assertEqual(module.status()['persisted'],0)
            first['payload']['records'].append({'changed':True})
            for i in range(1,8):
                value = capsule('shared-watch:2026-10-29T17:02:'+str(20+i)+'.000000+00:00')
                self.assertEqual(module.submit(value)['status'],'QUEUED')
            ninth = module.submit(capsule('shared-watch:2026-10-29T17:03:00+00:00'))
            self.assertEqual(ninth['status'],'DROPPED')
            state = module.status()
            self.assertEqual(state['queued_items'],8)
            self.assertLessEqual(state['queued_bytes'],module.MAX_QUEUE_BYTES)
            self.assertEqual(state['enqueued'],8)
            self.assertEqual(state['dropped'],1)
            self.assertFalse(state['shared_research_event_queue_used'])
        finally:
            release.set()
        wait_for(lambda:module.status()['queued_items'] == 0)
        self.assertEqual(module.status()['persisted'],8)
        self.assertEqual(json.loads(retained[0]['payload_json'])['payload']['records'],[])
        self.assertEqual(module.status()['last_durable']['status'],'PERSISTED')

    def test_submit_and_worker_failures_are_gaps_without_exception_text(self):
        module = fresh_module()
        with patch.object(module,'prepare',side_effect=RuntimeError('sensitive credentials must not be retained')):
            self.assertEqual(module.submit(capsule())['reason_code'],'RuntimeError')
        with patch.object(module,'_ensure_worker',return_value=False):
            self.assertEqual(module.submit(capsule())['status'],'UNAVAILABLE')
        class BrokenBackend:
            def persist_prepared(self,*args):
                raise RuntimeError('private endpoint and credentials')
        module._STORE = BrokenBackend()
        self.assertEqual(module.submit(capsule())['status'],'QUEUED')
        wait_for(lambda:module.status()['queued_items'] == 0)
        state = module.status()
        self.assertEqual(state['persisted'],0)
        self.assertEqual(state['failed'],3)
        self.assertIsNone(state['last_durable'])
        self.assertNotIn('credentials',json.dumps(state))
        self.assertNotIn('private endpoint',json.dumps(state))

    def test_diagnostic_clock_failures_do_not_escape_or_mask_commit(self):
        module = fresh_module()
        failure = RuntimeError('private clock message')
        with patch.object(module,'_utc_now',side_effect=failure), patch.object(module,'_ensure_worker',return_value=True):
            result = module.submit(capsule())
        self.assertEqual(result['status'],'FAILED')
        self.assertIsNone(module.status()['last']['at_utc'])
        self.assertEqual(module.status()['queued_items'],0)
        with patch.object(module,'_utc_now',side_effect=failure), patch.object(module,'_ensure_worker',return_value=False):
            self.assertEqual(module.submit(capsule())['status'],'UNAVAILABLE')
        class Backend:
            def __init__(self, broken): self.broken = broken
            def persist_prepared(self,prepared,enqueued_at):
                if self.broken: raise RuntimeError('private backend message')
                return dict(persisted=True,idempotent_existing=False,cycle_id=prepared['cycle_id'],
                    capture_version=prepared['capture_version'],capture_status=prepared['capture_status'],
                    payload_sha256=prepared['payload_sha256'],created_at_utc=enqueued_at)
        for broken in (True,False):
            isolated = fresh_module()
            isolated._STORE = Backend(broken)
            prepared = isolated.prepare(capsule())
            isolated._METRICS['queued_items'] = 1
            isolated._METRICS['queued_bytes'] = prepared['payload_bytes']
            with patch.object(isolated,'_utc_now',side_effect=failure):
                isolated._persist_one(prepared,'2026-10-29T17:07:16+00:00')
            status = isolated.status()
            self.assertEqual(status['queued_items'],0)
            self.assertEqual(status['failed'],int(broken))
            self.assertEqual(status['persisted'],int(not broken))
            self.assertIsNone(status['last']['at_utc'])
            self.assertNotIn('private',json.dumps(status))
        with patch.object(module,'prepare',side_effect=SystemExit('diagnostic only')):
            self.assertEqual(module.submit(capsule())['reason_code'],'SystemExit')


_TEST_URL = os.getenv('TEST_DATABASE_URL') or os.getenv('RESEARCH_TEST_POSTGRES_URL')


@unittest.skipUnless(_TEST_URL,'Explicit local/CI PostgreSQL test DSN required')
class PostgreSQLTests(unittest.TestCase):
    def test_jsonb_and_canonical_preimage_survive_independent_table(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict,make_conninfo
        info = conninfo_to_dict(_TEST_URL)
        if info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or not (
                info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test')):
            raise ValueError('Explicit local test database required')
        schema = 'alert_decision_test_'+uuid4().hex
        admin = psycopg.connect(_TEST_URL,autocommit=True)
        created = False
        try:
            admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            created = True
            options = (info.get('options','')+' -c search_path='+schema).strip()
            backend = store.PostgresStore(make_conninfo(_TEST_URL,options=options))
            original = capsule()
            self.assertFalse(backend.persist(original)['idempotent_existing'])
            self.assertTrue(backend.persist(original)['idempotent_existing'])
            changed = rehash({**deepcopy(original),'status':'PARTIAL'})
            with self.assertRaisesRegex(store.CapsuleValidationError,'CYCLE_IDENTITY_COLLISION'):
                backend.persist(changed)
            with backend._connect() as connection:
                row = connection.execute('SELECT payload,payload_json,payload_sha256,writer_version,writer_code_sha256 FROM research_alert_decision_cycles').fetchone()
                count = connection.execute('SELECT count(*) AS n FROM research_alert_decision_cycles').fetchone()['n']
            self.assertEqual(count,1)
            self.assertEqual(row['payload'],original)
            self.assertEqual(row['payload_json'],store.canonical(original))
            self.assertIn('-0.0',row['payload_json'])
            self.assertEqual(store.prepare(json.loads(row['payload_json']))['payload_sha256'],row['payload_sha256'])
            self.assertEqual(row['writer_version'],store.VERSION)
            self.assertEqual(len(row['writer_code_sha256']),64)
        finally:
            if created:
                # Exact generated schema in the explicitly guarded test DB.
                if not schema.startswith('alert_decision_test_') or len(schema) != len('alert_decision_test_')+32:
                    raise AssertionError('unexpected cleanup target')
                admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            admin.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
