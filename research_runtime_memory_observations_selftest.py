"""Finite passive-stream tests; real PostgreSQL requires an explicit test DSN."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import ModuleType
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_runtime_memory_observations as observations
import research_runtime_memory as memory

BASE='shared-watch:2026-10-31T12:02:15.000001+00:00'
STAMP='2026-10-31T12:08:15.000002+00:00'
SESSION='a'*32
COMMIT='b'*40


def fresh():
    spec=importlib.util.spec_from_file_location('isolated_memory_observations_'+uuid4().hex,Path(observations.__file__))
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module


def pins(module):
    return {'main.py':'1'*64,'research_runtime_memory.py':'2'*64,'research_watch_runtime.py':'3'*64,'research_runtime_journal.py':'4'*64,'research_runtime_memory_observations.py':module._writer_code_sha256()}


def sample_args(module=observations,index=2):
    sample=memory._empty(); sample['elapsed_ms']=-0.0
    return dict(process_session_id=SESSION,origin_cycle_id=BASE,cleanup_tail_at_utc=STAMP,runtime_git_commit=COMMIT,source_code_sha256=pins(module),observation_index=index,scheduled_delay_seconds=module.DELAYS[index],observed_delay_ms=module.DELAYS[index]*1000+25.5,sample_started_at_utc=STAMP,sample_completed_at_utc=STAMP,watch_active_before=0,watch_active_after=0,watch_activity_revision_before=1,watch_activity_revision_after=1,memory=sample)


def marker_args(module=observations):
    return dict(process_session_id=SESSION,origin_cycle_id=BASE,cleanup_tail_at_utc=STAMP,runtime_git_commit=COMMIT,source_code_sha256=pins(module))


def event(module=observations,marker=False):
    sink=[]
    with patch.object(module,'submit',side_effect=lambda value:sink.append(deepcopy(value)) or {'status':'QUEUED','durable':False}), patch.object(module,'_utc_now',return_value=STAMP):
        if marker: module.record_marker(**marker_args(module))
        else: module.record_sample(**sample_args(module))
    return sink[0]


def wait_for(predicate,seconds=3.):
    deadline=time.monotonic()+seconds
    while not predicate() and time.monotonic()<deadline: time.sleep(.005)
    if not predicate(): raise AssertionError('bounded worker wait timed out')


class Backend:
    def __init__(self): self.items=[]
    def persist_prepared(self,prepared,enqueued_at):
        self.items.append(deepcopy(prepared))
        return {**observations._summary(prepared),'persisted':True,'idempotent_existing':False,'created_at_utc':enqueued_at or STAMP}


class ObservationTests(unittest.TestCase):
    def test_import_performs_no_io_thread_database_or_watch_import(self):
        code=compile(Path(observations.__file__).read_text(encoding='utf-8'),'<lazy_stream_import>','exec')
        namespace={'__file__':observations.__file__,'__name__':'lazy_stream_import'}
        with patch('threading.Thread',side_effect=AssertionError('thread on import')), patch.object(Path,'read_bytes',side_effect=AssertionError('read on import')), patch('builtins.open',side_effect=AssertionError('open on import')):
            exec(code,namespace)
        self.assertIsNone(namespace['_WORKER']); self.assertIsNone(namespace['_STORE'])
        self.assertNotIn('research_runtime_journal',namespace)
        self.assertNotIn('research_watch_runtime',namespace)

    def test_marker_is_separate_tail_annotation_not_watch_finish(self):
        value=event(marker=True); prepared=observations.prepare(value)
        self.assertEqual(value['observation_index'],1)
        self.assertEqual(value['observation_kind'],'CLEANUP_TAIL_REACHED')
        self.assertEqual(value['activity_scope'],'CLEANUP_TAIL_NOT_RETURN_OR_IDLE_PROOF')
        self.assertEqual(value['memory'],{'status':'NOT_REQUESTED'})
        self.assertEqual(prepared['process_session_id'],SESSION)
        self.assertNotIn('sequence',value); self.assertNotIn('finish_scope',value)

    def test_sample_preserves_text_scalar_hash_and_unavailable_diagnostics(self):
        value=event(); prepared=observations.prepare(value)
        self.assertEqual(prepared['payload_json'],observations.canonical(value))
        self.assertIn('-0.0',prepared['payload_json'])
        self.assertEqual(value['memory']['sample_sha256'],observations.digest(value['memory']['sample']))
        self.assertEqual(value['capture_flags'],['MEMORY_SAMPLE_UNAVAILABLE'])
        self.assertEqual(value['activity_scope'],'NO_ACTIVE_WATCH_HANDLE_OBSERVED')
        self.assertEqual(value['activity_count_scope'],observations.ACTIVITY_COUNT_SCOPE)
        self.assertFalse(observations._jsonb_equal(True,1)); self.assertTrue(observations._jsonb_equal(-0.0,0))

    def test_all_four_fixed_offsets_and_exact_indices(self):
        module=fresh(); got=[]
        with patch.object(module,'submit',side_effect=lambda value:got.append(value) or {'status':'QUEUED'}):
            for index in range(2,6): module.record_sample(**sample_args(module,index))
        self.assertEqual([v['scheduled_delay_seconds'] for v in got],[30,120,300,600])
        for value in got: module.prepare(value)

    def test_begin_and_end_between_probe_are_changed_even_counts_return_zero(self):
        module=fresh(); args=sample_args(module); args['watch_activity_revision_after']=2; got=[]
        with patch.object(module,'submit',side_effect=lambda value:got.append(value) or {'status':'QUEUED'}): module.record_sample(**args)
        self.assertEqual(got[0]['watch_active_before'],0); self.assertEqual(got[0]['watch_active_after'],0)
        self.assertEqual(got[0]['activity_scope'],'WATCH_ACTIVITY_OBSERVED_OR_CHANGED')
        module.prepare(got[0])

    def test_positive_before_or_after_handles_prevent_idle_label(self):
        module=fresh(); got=[]
        with patch.object(module,'submit',side_effect=lambda value:got.append(value) or {'status':'QUEUED'}):
            for before,after in ((1,0),(0,1),(1,1),(2,2)):
                args=sample_args(module); args.update(watch_active_before=before,watch_active_after=after)
                module.record_sample(**args)
        self.assertTrue(all(value['activity_scope']=='WATCH_ACTIVITY_OBSERVED_OR_CHANGED' for value in got))

    def test_false_idle_annotation_rejected_by_strict_prepare(self):
        value=event(); value.update(watch_activity_revision_after=2)
        with self.assertRaisesRegex(observations.ObservationValidationError,'ACTIVITY_SCOPE'): observations.prepare(value)

    def test_late_offset_cannot_be_recorded_at_zero_elapsed(self):
        module=fresh(); args=sample_args(module,5); args['observed_delay_ms']=0
        self.assertEqual(module.record_sample(**args)['status'],'INVALID')
        args['observed_delay_ms']=600000-module.SCHEDULE_TOLERANCE_MS/2; got=[]
        with patch.object(module,'submit',side_effect=lambda value:got.append(value) or {'status':'QUEUED'}): module.record_sample(**args)
        module.prepare(got[0])
        got[0]['observed_delay_ms']=600000-module.SCHEDULE_TOLERANCE_MS*2
        with self.assertRaisesRegex(module.ObservationValidationError,'OBSERVATION_BEFORE_SCHEDULE'): module.prepare(got[0])

    def test_closed_schema_schedule_scope_and_privacy(self):
        invalid=[]
        for key,bad in [('observation_index',True),('observation_index',6),('scheduled_delay_seconds',31),('process_session_id','private-session'),('origin_cycle_id','shared-watch:bad'),('observed_delay_ms',True),('watch_active_before',True),('watch_active_after',3),('watch_activity_revision_after',0),('activity_count_scope','PROCESS_IS_IDLE'),('scope','COROUTINE_RETURN'),('event_time_utc','2026-10-31')]:
            value=event(); value[key]=bad; invalid.append(value)
        value=event(); value['memory']['sample']['pid']=123; invalid.append(value)
        value=event(); value['memory']['sample_sha256']='0'*64; invalid.append(value)
        value=event(); value['source_code_sha256']['private.py']='f'*64; invalid.append(value)
        value=event(); value['source_code_sha256']['research_runtime_memory_observations.py']='0'*64; invalid.append(value)
        value=event(); value['extra']='x'*20000; invalid.append(value)
        for value in invalid:
            with self.subTest(field=list(value)):
                with self.assertRaises(observations.ObservationValidationError): observations.prepare(value)

    def test_nan_or_custom_raw_objects_do_not_enter_queue(self):
        module=fresh()
        class Dangerous:
            def __str__(self): raise AssertionError('must not stringify')
            def __repr__(self): raise AssertionError('must not repr')
        with patch.object(module,'_ensure_worker',side_effect=AssertionError('must validate first')):
            for value in (float('nan'),Dangerous(),{'message':'private endpoint and token'}):
                args=sample_args(module); args['memory']=value
                self.assertEqual(module.record_sample(**args)['status'],'INVALID')
        self.assertNotIn('private endpoint',json.dumps(module.status()))

    def test_frozen_payload_and_source_pins_do_not_change_with_caller(self):
        module=fresh(); backend=Backend(); module._STORE=backend; args=sample_args(module)
        self.assertEqual(module.record_sample(**args)['status'],'QUEUED')
        args['memory']['elapsed_ms']=999; args['source_code_sha256']['main.py']='f'*64
        wait_for(lambda:module.status()['queued_items']==0)
        stored=json.loads(backend.items[0]['payload_json'])
        self.assertEqual(stored['memory']['sample']['elapsed_ms'],-0.0)
        self.assertEqual(stored['source_code_sha256']['main.py'],'1'*64)

    def test_four_outstanding_includes_inflight_and_has_no_durable_enqueue_ack(self):
        module=fresh(); entered,release=threading.Event(),threading.Event()
        class Slow(Backend):
            def persist_prepared(self,prepared,enqueued_at):
                entered.set()
                if not release.wait(3.): raise RuntimeError('bounded fixture deadline')
                return super().persist_prepared(prepared,enqueued_at)
        module._STORE=Slow()
        try:
            first=module.submit(event(module)); self.assertFalse(first['durable']); self.assertEqual(first['status'],'QUEUED')
            self.assertTrue(entered.wait(2.))
            for _ in range(3): self.assertEqual(module.submit(event(module))['status'],'QUEUED')
            self.assertEqual(module.submit(event(module))['status'],'DROPPED')
            self.assertEqual(module.status()['queued_items'],4); self.assertEqual(module.status()['persisted'],0)
        finally: release.set()
        wait_for(lambda:module.status()['queued_items']==0)
        self.assertEqual(module.status()['persisted'],4); self.assertTrue(module.status()['last_durable']['durable'])

    def test_independent_byte_bound_and_queue_failure_rollback(self):
        module=fresh(); module._STORE=Backend(); value=event(module); size=module.prepare(value)['payload_bytes']
        with patch.object(module,'_ensure_worker',return_value=True),patch.object(module,'MAX_QUEUE_BYTES',size-1):
            self.assertEqual(module.submit(value)['reason_code'],'QUEUE_BOUND'); self.assertEqual(module.status()['queued_items'],0)
        with patch.object(module,'_ensure_worker',return_value=True),patch.object(module._QUEUE,'put_nowait',side_effect=SystemExit('private queue error')):
            self.assertEqual(module.submit(value)['status'],'UNAVAILABLE'); self.assertEqual(module.status()['queued_items'],0); self.assertEqual(module.status()['queued_bytes'],0)

    def test_failed_worker_does_not_retry_or_expose_exception_text(self):
        module=fresh()
        class Broken:
            calls=0
            def persist_prepared(self,*args): self.calls+=1; raise SystemExit('private database password')
        module._STORE=backend=Broken(); self.assertEqual(module.submit(event(module))['status'],'QUEUED')
        wait_for(lambda:module.status()['queued_items']==0)
        self.assertEqual(backend.calls,1); self.assertEqual(module.status()['failed'],1); self.assertEqual(module.status()['persisted'],0)
        self.assertTrue(module.status()['worker_alive']); self.assertNotIn('private database',json.dumps(module.status()))

    def test_unconfigured_marker_is_not_retried_or_reconstructed(self):
        module=fresh()
        with patch.dict(os.environ,{'DATABASE_URL':'','RESEARCH_DATABASE_URL':''}): self.assertEqual(module.record_marker(**marker_args(module))['status'],'UNAVAILABLE')
        self.assertIsNone(module._WORKER); module._STORE=backend=Backend(); module.record_sample(**sample_args(module))
        wait_for(lambda:module.status()['queued_items']==0)
        self.assertEqual(len(backend.items),1); self.assertEqual(backend.items[0]['observation_index'],2)

    def test_status_uses_cached_metrics_and_does_not_read_source_or_probe(self):
        module=fresh()
        with patch.object(Path,'read_bytes',side_effect=AssertionError('health must not read')),patch.object(memory,'sample_memory',side_effect=AssertionError('health must not probe')):
            state=module.status()
        self.assertEqual(state['queued_items'],0); self.assertFalse(state['shared_watch_journal_queue_used'])

    def test_idempotent_identity_checks_exact_text_jsonb_and_writer(self):
        prepared=observations.prepare(event()); row={**prepared,'payload':json.loads(prepared['payload_json']),'writer_version':observations.VERSION,'writer_code_sha256':observations._writer_code_sha256()}
        observations._verify_existing(row,prepared)
        for key,bad in [('payload_json',prepared['payload_json']+' '),('writer_version','other'),('writer_code_sha256','0'*64)]:
            changed=deepcopy(row); changed[key]=bad
            with self.assertRaises(observations.ObservationValidationError): observations._verify_existing(changed,prepared)
        row['payload']['memory']['sample']['coverage']['budget_exhausted']=0
        with self.assertRaisesRegex(observations.ObservationValidationError,'JSONB_MISMATCH'): observations._verify_existing(row,prepared)

    def test_prepared_tamper_rejected_before_driver_import(self):
        prepared=observations.prepare(event()); prepared['payload_sha256']='0'*64
        with self.assertRaisesRegex(observations.ObservationValidationError,'PREPARED_OBSERVATION_CHANGED'): observations.PostgresStore('unused').persist_prepared(prepared,STAMP)

    def test_connect_timeout_preserves_isolated_search_path(self):
        psycopg,rows=ModuleType('psycopg'),ModuleType('psycopg.rows'); rows.dict_row=object(); calls=[]
        psycopg.connect=lambda *args,**kwargs:calls.append((args,kwargs)) or 'connection'
        with patch.dict(sys.modules,{'psycopg':psycopg,'psycopg.rows':rows}): self.assertEqual(observations.PostgresStore("host=postgres dbname=test_memory options='-c search_path=fixture'")._connect(),'connection')
        self.assertEqual(calls[0][1]['connect_timeout'],3); self.assertIn('search_path=fixture',calls[0][0][0])

    def test_commit_failure_cannot_be_acknowledged_durable(self):
        module=fresh(); prepared=module.prepare(event(module)); calls=[]
        class Result:
            def __init__(self,value): self.value=value
            def fetchone(self): return self.value
        class Connection:
            def __enter__(self): return self
            def __exit__(self,*args): raise SystemExit('private commit uncertainty')
            def execute(self,text,params=None):
                calls.append(text)
                if text.startswith('INSERT'): return Result({'observation_index':2})
                if text.startswith('SELECT process_session_id'): return Result({**prepared,'payload':json.loads(prepared['payload_json']),'writer_version':module.VERSION,'writer_code_sha256':module._writer_code_sha256(),'created_at_utc':datetime.now(timezone.utc)})
                return Result(None)
        fake_json=ModuleType('psycopg.types.json'); fake_json.Jsonb=lambda value:value
        backend=module.PostgresStore('unused'); module._STORE=backend; module._METRICS['queued_items']=1; module._METRICS['queued_bytes']=prepared['payload_bytes']
        with patch.dict(sys.modules,{'psycopg.types.json':fake_json}),patch.object(backend,'_connect',return_value=Connection()): module._persist_one(prepared,STAMP)
        self.assertEqual(calls[:2],['SET LOCAL statement_timeout = 3000','SET LOCAL lock_timeout = 1000'])
        self.assertTrue(any('PRIMARY KEY(process_session_id,origin_cycle_id,observation_index)' in sql for sql in calls))
        self.assertEqual(module.status()['persisted'],0); self.assertEqual(module.status()['failed'],1); self.assertFalse(backend.initialized)


_TEST_URL=os.getenv('TEST_DATABASE_URL') or os.getenv('RESEARCH_TEST_POSTGRES_URL')


@unittest.skipUnless(_TEST_URL,'Explicit local/CI PostgreSQL test DSN required')
class PostgreSQLTests(unittest.TestCase):
    def test_real_immutable_observations_exact_text_jsonb_and_identity(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict,make_conninfo
        info=conninfo_to_dict(_TEST_URL)
        if info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or not (info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test')): raise ValueError('Explicit local test database required')
        schema='runtime_memory_observation_test_'+uuid4().hex; admin=psycopg.connect(_TEST_URL,autocommit=True); created=False
        try:
            admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema))); created=True
            isolated=dict(info); isolated['options']=(info.get('options','')+' -c search_path='+schema).strip()
            backend=observations.PostgresStore(make_conninfo(**isolated)); value=event()
            first=backend.persist(value,STAMP); duplicate=backend.persist(deepcopy(value),STAMP)
            self.assertTrue(first['persisted']); self.assertFalse(first['idempotent_existing']); self.assertTrue(duplicate['idempotent_existing'])
            changed=deepcopy(value); changed['observed_delay_ms']+=1
            with self.assertRaisesRegex(observations.ObservationValidationError,'IDENTITY_COLLISION'): backend.persist(changed,STAMP)
            with backend._connect() as connection: rows=connection.execute('SELECT payload_json,payload,payload_sha256 FROM research_runtime_memory_observations').fetchall()
            self.assertEqual(len(rows),1); self.assertEqual(rows[0]['payload_json'],observations.canonical(value)); self.assertIn('-0.0',rows[0]['payload_json'])
            self.assertEqual(rows[0]['payload_sha256'],observations.prepare(value)['payload_sha256']); self.assertTrue(observations._jsonb_equal(rows[0]['payload'],value))
            with backend._connect() as connection: connection.execute('UPDATE research_runtime_memory_observations SET writer_code_sha256=%s WHERE process_session_id=%s AND origin_cycle_id=%s AND observation_index=2',('f'*64,SESSION,BASE))
            with self.assertRaisesRegex(observations.ObservationValidationError,'WRITER_IDENTITY_MISMATCH'): backend.persist(deepcopy(value),STAMP)
        finally:
            if created: admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            admin.close()


if __name__=='__main__': unittest.main()
