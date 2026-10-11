"""History maintenance against actual disposable stores; no venue/network calls."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from . import card_lifecycle as life
from . import experimental_execution_archive as history
from .experimental_execution_state import ExecutionState, PostgresExecutionState, StateError, PG_SCHEMA
from .experimental_plan_store import reduce_source
from .postgres_journal import PostgresJournal
from .test_approved_alert_lifecycle import alert, cancellation, BASE
from .test_experimental_execution_runtime import ROUTES

CI = os.environ.get('HL_JOURNAL_CI_URL')


class HistoryCases:
    """The same persistence assertions run against SQLite and real PostgreSQL."""
    def receive(self, msg, *, store=None, now=BASE+1000):
        store = store or self.store
        def transition(state):
            cid = msg['occurrence_id']
            source,changed = reduce_source(state['sources'].get(cid),msg,
                now=contract.iso_ms(now),not_before=contract.iso_ms(BASE-60000),domain=state['domain'])
            if changed:
                state['sources'][cid] = source
                state['events'].append(dict(at_ms=now,kind='SOURCE_'+msg['kind'],occurrence_id=cid))
            return dict(status='RECORDED' if changed else 'DUPLICATE',permission=source['entry_permission'])
        return store.mutate_sources(transition,[msg['occurrence_id']])
    def seed(self, count=1):
        msgs = [alert(cycle='history-'+str(i)) for i in range(count)]
        def transition(state):
            for msg in msgs:
                source,_ = reduce_source(None,msg,now=contract.iso_ms(BASE+1000),
                    not_before=contract.iso_ms(BASE-60000),domain=state['domain'])
                state['sources'][msg['occurrence_id']] = source
                state['events'].append(dict(at_ms=BASE+1000,kind='SOURCE_ALERT',occurrence_id=msg['occurrence_id']))
        self.store.mutate(transition)
        return msgs
    def compact(self, **kwargs):
        return self.store.compact_history(now_ms=BASE+100000,force=True,**kwargs)

    def test_explicit_migration_only_and_reopen(self):
        self.assertFalse(self.store.initialize_history())
        self.assertEqual(self.reopen().load()['history']['version'],history.VERSION)
    def test_expired_source_lossless_bundle_and_permanent_duplicate(self):
        msg = self.seed()[0]; cid = msg['occurrence_id']; before=self.store.load()
        self.assertEqual(self.compact()['archived'],1)
        after=self.store.load();record=self.reopen().archive_record(cid)
        self.assertNotIn(cid,after['sources']);self.assertEqual(after['events'],[])
        self.assertEqual(record['source'],before['sources'][cid])
        self.assertEqual(record['events'],before['events'])
        self.assertEqual(record['current_source']['entry_permission'],'RETIRED')
        result=self.receive(msg,store=self.reopen(),now=BASE+100001)
        self.assertEqual(result,dict(status='DUPLICATE',permission='RETIRED'))
        self.assertNotIn(cid,self.store.load()['sources'])
    def test_cancel_first_survives_archive_and_fresh_original_cannot_resume(self):
        msg=alert();cancel=cancellation(msg,BASE+500)
        self.receive(cancel)
        self.assertEqual(self.store.compact_history(now_ms=BASE+1100,force=True)['archived'],1)
        self.assertEqual(self.receive(msg,now=BASE+1200)['permission'],'RETIRED')
        self.assertEqual(self.store.archive_record(msg['occurrence_id'])['current_source']['cancellation'],cancel)
    def test_late_cancellation_audit_is_durable_and_does_not_reenter_hot_state(self):
        msg=self.seed()[0];cid=msg['occurrence_id'];self.compact()
        original=self.store.archive_record(cid)['source']
        cancel=cancellation(msg,BASE+110000)
        self.assertEqual(self.receive(cancel,now=BASE+110001)['status'],'RECORDED')
        record=self.reopen().archive_record(cid)
        self.assertEqual(record['source'],original)
        self.assertEqual(record['current_source']['cancellation'],cancel)
        updates=self.store.history_updates_page(cid)['records']
        self.assertEqual(len(updates),1);self.assertEqual(updates[0]['source'],record['current_source'])
        self.assertNotIn(cid,self.store.load()['sources'])
        self.assertEqual(self.receive(cancel,now=BASE+110002)['status'],'DUPLICATE')
        self.assertEqual(len(self.store.history_updates_page(cid)['records']),1)
    def test_changed_immutable_plan_after_archive_is_rejected(self):
        msg=self.seed()[0];self.compact();bad=deepcopy(msg);bad['entry']='93.543'
        with self.assertRaisesRegex(ValueError,'IMMUTABLE_PLAN_CHANGED'):
            self.receive(bad,now=BASE+110000)
        self.assertEqual(self.store.history_page()['archived_count'],1)
    def test_bounded_batches_pages_and_maintenance_interval(self):
        self.seed(70)
        first=self.store.compact_history(now_ms=BASE+100000)
        self.assertEqual(first['archived'],32)
        with patch.object(self.store, '_history_transaction', side_effect=AssertionError('Unneeded archive transaction')):
            self.assertEqual(self.store.compact_history(now_ms=BASE+100001)['status'],'HISTORY_INTERVAL_NOT_DUE')
        self.assertEqual(self.compact()['archived'],32)
        self.assertEqual(self.compact()['archived'],6)
        page=self.store.history_page(limit=30);ids={r['occurrence_id'] for r in page['records']}
        self.assertEqual(len(ids),30);self.assertTrue(page['next_cursor'])
        while page['next_cursor']:
            page=self.store.history_page(after=page['next_cursor'],limit=30)
            ids.update(r['occurrence_id'] for r in page['records'])
        self.assertEqual(len(ids),70);self.assertFalse(self.store.load()['sources'])
    def test_cursor_passes_more_than_scan_limit_ineligible_sources(self):
        msgs=self.seed(260)
        ids=sorted(m['occurrence_id'] for m in msgs);terminal=ids[-1]
        def pin(state):
            for cid in ids[:-1]:
                state['sources'][cid]['source']['expires_at']=contract.iso_ms(BASE+1000000)
        self.store.mutate(pin)
        self.assertEqual(self.compact()['archived'],0)
        self.assertEqual(self.compact()['archived'],0)
        self.assertEqual(self.compact()['archived'],1)
        self.assertIsNotNone(self.store.archive_record(terminal))
    def test_pending_actions_and_notifications_are_retained(self):
        self.seed()
        for key in ('pending_actions','pending_notifications','notification_outbox','outbox'):
            self.store.mutate(lambda state:state.update({key:[{'occurrence_id':'pending'}]}))
            self.assertEqual(self.compact()['archived'],0)
            self.store.mutate(lambda state:state.pop(key))
        self.assertEqual(self.compact()['archived'],1)
    def test_archive_and_receive_concurrent_same_id_never_revives(self):
        msg=self.seed()[0];other=self.reopen();barrier=threading.Barrier(2)
        def archive():
            barrier.wait();return self.compact()
        def duplicate():
            barrier.wait();return self.receive(msg,store=other,now=BASE+100000)
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(archive);b=pool.submit(duplicate);a.result();b.result()
        self.assertNotIn(msg['occurrence_id'],self.store.load()['sources'])
        self.assertEqual(self.store.history_page()['archived_count'],1)
    def test_failed_second_archive_rolls_back_first_and_hot_pruning(self):
        self.seed(2);before=self.store.load();original=history._SQL.insert;calls=[]
        def fail(db,bundle):
            calls.append(bundle['occurrence_id'])
            if len(calls)==2:raise StateError('SIMULATED_MAINTENANCE_FAILURE')
            return original(db,bundle)
        with patch.object(history._SQL,'insert',fail),self.assertRaisesRegex(StateError,'SIMULATED_MAINTENANCE_FAILURE'):
            self.compact()
        self.assertEqual(self.store.load(),before)
        self.assertEqual(self.store.history_page()['records'],[])
    def test_checksum_corruption_never_reported_as_history(self):
        msg=self.seed()[0];self.compact()
        with self.store._history_transaction() as (db,_state,_save):
            db.execute(f'UPDATE {db.table("history")} SET checksum=? WHERE occurrence_id=?',('bad',msg['occurrence_id']))
        for operation in (lambda:self.store.archive_record(msg['occurrence_id']),self.store.history_page):
            with self.assertRaisesRegex(StateError,'HISTORY_INTEGRITY'):operation()
    def test_large_terminal_history_is_removed_from_hot_document(self):
        self.seed(12)
        def enlarge(state):
            for event in state['events']:event['audit_detail']='x'*300000
        self.store.mutate(enlarge);before=len(life.encoded(self.store.load()))
        for _ in range(4):self.compact()
        self.assertGreater(before,3*1024*1024)
        self.assertLess(len(life.encoded(self.store.load())),4096)
        self.assertEqual(sum(len(r['events'][0]['audit_detail']) for r in self.store.history_page()['records']),3600000)
    def test_history_receive_cannot_mutate_state_identity(self):
        before=self.store.load()
        changes={'version':'changed','domain':'changed','routes':{},'not_before_ms':BASE,'revision':123}
        if 'agents' in before:changes['agents']={}
        for key,replacement in changes.items():
            with self.subTest(key=key),self.assertRaisesRegex(StateError,'IDENTITY_CHANGED'):
                self.store.mutate_sources(lambda value:value.update({key:replacement}),[])
            self.assertEqual(self.store.load(),before)
    def test_single_record_larger_than_normal_batch_is_not_starved(self):
        self.seed()
        self.store.mutate(lambda state:state['events'][0].update(audit_detail='y'*(3*1024*1024)))
        result=self.compact()
        self.assertEqual(result['archived'],1)
        self.assertGreater(result['bytes'],history.MAX_BATCH_BYTES)
        self.assertLess(len(life.encoded(self.store.load())),4096)
        self.assertEqual(len(self.store.history_page()['records'][0]['events'][0]['audit_detail']),3*1024*1024)

    def test_parameter_bounds_and_clock_regression_fail_closed(self):
        for kwargs in (dict(batch_size=33),dict(scan_limit=129),dict(batch_size=0)):
            with self.assertRaisesRegex(StateError,'BOUNDED'):self.compact(**kwargs)
        self.compact()
        with self.assertRaisesRegex(StateError,'CLOCK_REGRESSION'):
            self.store.compact_history(now_ms=BASE,force=True)


class SQLiteHistoryTests(HistoryCases,unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'history.isolated-experimental.sqlite3'
        self.store=ExecutionState(self.path);self.store.initialize(ROUTES,not_before_ms=BASE-60000)
        self.store.initialize_history()
    def reopen(self):return ExecutionState(self.path)
    def test_hot_path_executes_no_schema_creation(self):
        self.seed();queries=[];original=self.store._connect
        def connect():
            conn=original();conn.set_trace_callback(queries.append);return conn
        with patch.object(self.store,'_connect',side_effect=connect):
            self.compact();self.store.history_page();self.store.load()
        self.assertFalse(any(q.startswith(('CREATE ','ALTER ','DROP ')) for q in queries))
    def test_lost_commit_ack_keeps_archive_and_pruning_atomic(self):
        msg=self.seed()[0];original=self.store._connect
        class Connection:
            def __init__(self,raw):self.raw=raw
            def __getattr__(self,key):return getattr(self.raw,key)
            def commit(self):
                self.raw.commit();raise OSError('SIMULATED_LOST_COMMIT_REPLY')
        with patch.object(self.store,'_connect',side_effect=lambda:Connection(original())),self.assertRaisesRegex(OSError,'LOST_COMMIT'):
            self.compact()
        other=self.reopen()
        self.assertNotIn(msg['occurrence_id'],other.load()['sources'])
        self.assertIsNotNone(other.archive_record(msg['occurrence_id']))
        self.assertEqual(other.compact_history(now_ms=BASE+100001,force=True)['archived'],0)
    def test_concurrent_history_activation_cannot_bypass_tombstone_lookup(self):
        fresh=ExecutionState(Path(self.tmp.name)/'migration-race.isolated-experimental.sqlite3')
        fresh.initialize(ROUTES,not_before_ms=BASE-60000)
        msg=alert();self.receive(msg,store=fresh)
        other=ExecutionState(fresh.path);original=fresh.mutate
        def activate_then_mutate(transition):
            other.initialize_history()
            other.compact_history(now_ms=BASE+100000,force=True)
            return original(transition)
        with patch.object(fresh,'mutate',side_effect=activate_then_mutate),self.assertRaisesRegex(StateError,'INITIALIZATION_CHANGED_RETRY'):
            self.receive(msg,store=fresh,now=BASE+100000)
        self.assertNotIn(msg['occurrence_id'],fresh.load()['sources'])
        self.assertEqual(fresh.history_page()['archived_count'],1)

    def test_uninitialized_existing_store_stays_compatible(self):
        fresh=ExecutionState(Path(self.tmp.name)/'unmigrated.isolated-experimental.sqlite3')
        fresh.initialize(ROUTES,not_before_ms=BASE-60000)
        self.assertEqual(fresh.compact_history(now_ms=BASE)['status'],'EXPLICIT_HISTORY_INITIALIZATION_REQUIRED')
        self.assertEqual(self.receive(alert(),store=fresh)['status'],'RECORDED')
        with closing(fresh._connect()) as conn:
            names={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(names,{'experimental_state'})


@unittest.skipUnless(CI,'Requires actual disposable loopback PostgreSQL')
class PostgresHistoryTests(HistoryCases,unittest.TestCase):
    def setUp(self):
        self.journal=PostgresJournal.for_ci(CI);self.journal.bootstrap()
        with self.journal._transaction() as conn:conn.execute(f'DROP SCHEMA IF EXISTS {PG_SCHEMA} CASCADE')
        self.store=PostgresExecutionState(self.journal)
        self.store.initialize(ROUTES,not_before_ms=BASE-60000);self.store.initialize_history()
    def reopen(self):return PostgresExecutionState(PostgresJournal.for_ci(CI))


@unittest.skipUnless(CI,'Requires actual disposable loopback PostgreSQL')
class TestnetPostgresHistoryTests(HistoryCases,unittest.TestCase):
    def setUp(self):
        from .experimental_live_state import TestnetExecutionState, PG_SCHEMA as LIVE_SCHEMA
        from .filled_dispatch_store import DispatchStore
        self.journal=PostgresJournal.for_ci(CI);self.journal.bootstrap()
        DispatchStore(self.journal).initialize()
        with self.journal._transaction() as conn:conn.execute(f'DROP SCHEMA IF EXISTS {LIVE_SCHEMA} CASCADE')
        self.store=TestnetExecutionState.for_ci(self.journal)
        routes={role:dict(**value,agent='0x'+str(index+3)*40) for index,(role,value) in enumerate(ROUTES.items())}
        self.store.initialize(routes,not_before_ms=BASE-60000);self.store.initialize_history()
    def reopen(self):
        from .experimental_live_state import TestnetExecutionState
        return TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
