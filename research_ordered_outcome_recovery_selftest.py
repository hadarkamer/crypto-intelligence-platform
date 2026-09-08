"""Recovery fairness, coverage and real-PG retry/rollback regression tests."""
from __future__ import annotations

from copy import deepcopy
from contextlib import ExitStack
from datetime import datetime,timedelta,timezone
import os
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from uuid import uuid4

import research_ordered_outcome_recovery_store as store

START=datetime(2026,9,6,23,9,55,tzinfo=timezone.utc)


def grid(*,now,complete=False,start=START):
    labels=[]
    metrics=[]
    for window in store.WINDOWS:
        end=start+timedelta(minutes=window)
        observed=min(end,now).replace(second=0,microsecond=0)-timedelta(milliseconds=1)
        for bps in store.THRESHOLDS:
            labels.append(dict(window_minutes=window,threshold_bps=bps,method_version=store.V7,
                status='SUCCESS' if complete else 'OPEN',path_complete=True,
                observed_through_utc=start+timedelta(minutes=1) if complete else observed))
        metrics.append(dict(window_minutes=window,method_version=store.COMMON_VERSION,
            status='READY' if now>=end else 'OPEN',result={
                'observed_prefix_complete':True,'observed_through_utc':observed}))
    return labels,metrics


class RecoveryTests(unittest.TestCase):
    def test_terminal_first_touch_does_not_need_full_horizon_but_common_does(self):
        now=START+timedelta(days=2)
        labels,metrics=grid(now=now,complete=True)
        event={'alert_time_utc':START}
        self.assertEqual(store.coverage_state(event,labels,metrics,now=now),'COMPLETE')
        metrics[-1]['result']['observed_through_utc']=START+timedelta(minutes=1)
        self.assertEqual(store.coverage_state(event,labels,metrics,now=now),'RETRY')

    def test_all_32_labels_and_four_windows_are_required(self):
        now=START+timedelta(days=2)
        labels,metrics=grid(now=now,complete=True)
        event={'alert_time_utc':START}
        self.assertEqual(store.coverage_state(event,labels[:-1],metrics,now=now),'RETRY')
        self.assertEqual(store.coverage_state(event,labels,metrics[:-1],now=now),'RETRY')
        labels[0]['path_complete']=False
        self.assertEqual(store.coverage_state(event,labels,metrics,now=now),'RETRY')

    def test_live_refresh_remains_open_exact_and_partial_minute(self):
        for now in (START+timedelta(minutes=12),START.replace(minute=22,second=0),
                    START.replace(minute=22,second=59,microsecond=999999)):
            labels,metrics=grid(now=now)
            self.assertEqual(store.coverage_state({'alert_time_utc':START},labels,metrics,now=now),'OPEN')
            labels[-1]['observed_through_utc']-=timedelta(minutes=1)
            self.assertEqual(store.coverage_state({'alert_time_utc':START},labels,metrics,now=now),'RETRY')

    def test_queue_interleave_keeps_regular_work_and_request_tokens(self):
        requested=[{'event_id':i,'_recovery_requested_through':START} for i in (1,2,3,4)]
        ordinary=[{'event_id':i} for i in (2,10,11,12)]
        selected=store.combine(requested,ordinary,limit=8)
        self.assertEqual([row['event_id'] for row in selected],[1,2,10,3,11,4,12])
        self.assertEqual(selected[1]['_recovery_requested_through'],START)
        for invalid in ([],[0],range(1,1002)):
            with self.assertRaises(ValueError):store.normalize_ids(invalid)

    def test_bounded_capacity_reserves_half_for_ordinary_work(self):
        for cap in (2,8,32,64,1000):
            requested_cap=store.next_requested_budget(None,cap)
            effective=min(cap,64)
            self.assertEqual(requested_cap,effective//2)
            requested=[{'event_id':i,'_recovery_requested_through':START}
                for i in range(1,requested_cap+1)]
            ordinary=[{'event_id':i+100} for i in range(effective-requested_cap)]
            selected=store.combine(requested,ordinary,limit=effective)
            self.assertEqual(len(selected),effective)
            self.assertEqual(sum('_recovery_requested_through' not in row for row in selected),
                effective-requested_cap)

    def test_open_refresh_obeys_cadence_and_actual_entry_boundaries(self):
        self.assertEqual(store.next_open_refresh_at(START,START+timedelta(minutes=5)),
            START+timedelta(minutes=35))
        for horizon in store.WINDOWS:
            boundary=START+timedelta(minutes=horizon)
            self.assertEqual(store.next_open_refresh_at(START,boundary-timedelta(milliseconds=1)),boundary)
            self.assertEqual(store.next_open_refresh_at(START,boundary),boundary+timedelta(minutes=30))
        # HYPE enters at the next full minute, not at the immutable alert's seconds.
        derived_entry=START.replace(second=0,microsecond=0)+timedelta(minutes=1)
        original_boundary=START+timedelta(minutes=60)
        self.assertEqual(store.next_open_refresh_at(derived_entry,original_boundary),
            derived_entry+timedelta(minutes=60))

    def test_v7_pass_capacity_is_independent_of_legacy_closed_limit(self):
        import research_outcome_worker as worker
        class EndAfterOrderedPass(Exception):pass
        service=worker.ResearchOutcomeWorker()
        with patch.object(worker,'_ENABLED',True), patch.object(worker,'_database_url',return_value='test'), \
                patch.object(worker,'_ORDERED_FIRST_TOUCH_EVENT_LIMIT',32), \
                patch.object(worker,'psycopg',SimpleNamespace(connect=lambda *a,**k:(_ for _ in ()).throw(EndAfterOrderedPass()))), \
                patch.object(service,'_run_ordered_first_touch_once',return_value={}) as ordered:
            with self.assertRaises(EndAfterOrderedPass):service.run_once(limit_per_horizon=8)
            ordered.assert_called_once_with('test',event_limit=32)

    def test_matured_open_v7_cannot_be_called_complete(self):
        now=START+timedelta(days=2)
        labels,metrics=grid(now=now)
        self.assertEqual(store.coverage_state({'alert_time_utc':START},labels,metrics,now=now),'RETRY')

    def test_derived_open_and_stale_retained_prefix_are_distinguished(self):
        now=START+timedelta(minutes=30)
        event={'event_id':1,'_recovery_requested_through':now}
        class Connection:
            def execute(self,query,params=()):return SimpleNamespace(fetchone=lambda:{'event_id':1})
        item={'event_id':1,'calculation_status':'RETRY_INCOMPLETE_PATH','complete':False,
            'current_prefix_complete':True,'source_scope':'DERIVED_NATIVE_HYPE_MARK',
            'live_union_eligible':False,'observed_at_utc':now,
            'entry_time_utc':START.replace(second=0)+timedelta(minutes=1)}
        self.assertEqual(store.finish_derived(Connection(),event,result={'measurements':[item]},now=now),'DERIVED_OPEN')
        item['observed_at_utc']=now-timedelta(minutes=1)
        self.assertEqual(store.finish_derived(Connection(),event,result={'measurements':[item]},now=now),'RETRY')
        item['complete']=True
        item['calculation_status']='COMPLETE_32_LABELS'
        item['ready_windows']=list(store.WINDOWS)
        self.assertEqual(store.finish_derived(Connection(),event,result={'measurements':[item]},now=now),'SUPPLEMENTED')

    def test_derived_refresh_waits_for_derived_horizon_and_retry_keeps_backoff(self):
        now=START+timedelta(minutes=60)
        event={'event_id':1,'_recovery_requested_through':now,'alert_time_utc':START}
        class Connection:
            def execute(self,query,params=()):
                self.params=params
                return SimpleNamespace(fetchone=lambda:{'event_id':1})
        entry=START.replace(second=0,microsecond=0)+timedelta(minutes=1)
        item={'event_id':1,'calculation_status':'RETRY_INCOMPLETE_PATH','complete':False,
            'current_prefix_complete':True,'source_scope':'DERIVED_NATIVE_HYPE_PERP',
            'live_union_eligible':False,'observed_at_utc':now,'entry_time_utc':entry}
        conn=Connection()
        self.assertEqual(store.finish_derived(conn,event,result={'measurements':[item]},now=now,
            expected_source_scope='DERIVED_NATIVE_HYPE_PERP'),'DERIVED_OPEN')
        self.assertEqual(conn.params[-3],entry+timedelta(minutes=60))
        del item['entry_time_utc']
        self.assertEqual(store.finish_derived(conn,event,result={'measurements':[item]},now=now,
            expected_source_scope='DERIVED_NATIVE_HYPE_PERP'),'RETRY')
        self.assertEqual(conn.params[-3],now+timedelta(minutes=15))

    def test_worker_hype_branch_calls_actual_keyword_only_adapter_contract(self):
        import research_outcome_worker as worker
        import research_native_hype_perp_supplement as hype
        import research_ordered_inverse_worker as inverse
        import research_past_price_features_worker as past
        import research_common_window_metrics_store as common
        event={'event_id':1,'alert_time_utc':START,'symbol':'HYPE','direction':'SHORT',
            'event_kind':'ALERT','delivery_status':'DELIVERED','engine_snapshot':{},
            '_recovery_requested_through':START}
        class Connection:
            def __enter__(self):return self
            def __exit__(self,*args):return False
        service=worker.ResearchOutcomeWorker()
        with ExitStack() as stack:
            stack.enter_context(patch.object(worker,'psycopg',SimpleNamespace(connect=lambda *a,**k:Connection())))
            for module,name,value in ((common,'available',True),(common,'seed_bounded_history',0),
                (store,'available',True),(store,'intake',1),(store,'next_requested_budget',1),
                (store,'load_due',[event]),
                (inverse,'run_pending',{}),(past,'run_pending',{})):
                stack.enter_context(patch.object(module,name,return_value=value))
            finish=stack.enter_context(patch.object(store,'finish_derived',return_value='DERIVED_OPEN'))
            stack.enter_context(patch.object(service,'_write_alert_reference_rejections',return_value=1))
            stack.enter_context(patch.object(service,'_run_common_window_due',return_value={}))
            stack.enter_context(patch.object(service,'_drain_ordered_first_touch_outbox',return_value={'synced':0,'failed':0}))
            native=stack.enter_context(patch.object(service,'_write_ordered_first_touch_outcome'))
            adapter=stack.enter_context(patch.object(hype,'run',autospec=True,return_value={'measurements':[]}))
            result=service._run_ordered_first_touch_locked('test',event_limit=1)
            self.assertEqual(result['recovery_derived_open'],1)
            adapter.assert_called_once()
            self.assertEqual(adapter.call_args.kwargs['database_url'],'test')
            self.assertEqual(adapter.call_args.kwargs['event_ids'],[1])
            self.assertEqual(finish.call_args.kwargs['expected_source_scope'],'DERIVED_NATIVE_HYPE_PERP')
            native.assert_not_called()
            health=service.status()
            self.assertEqual(health['ordered_outcome_recovery_open_refresh_minutes'],30)
            self.assertEqual(health['ordered_outcome_recovery_retry_minutes'],15)
            self.assertGreaterEqual(health['metrics']['ordered_first_touch_last_pass_duration_seconds'],0)

    def test_derived_source_contract_is_exact_and_not_a_canonical_ack(self):
        now=START+timedelta(days=2)
        event={'event_id':1,'_recovery_requested_through':now}
        class Connection:
            def __init__(self):self.calls=[]
            def execute(self,query,params=()):
                self.calls.append((query,params))
                return SimpleNamespace(fetchone=lambda:{'event_id':1})
        item={'event_id':1,'calculation_status':'COMPLETE_32_LABELS','complete':True,
            'current_prefix_complete':True,'source_scope':'DERIVED_NATIVE_HYPE_PERP',
            'live_union_eligible':False,'observed_at_utc':now,'ready_windows':list(store.WINDOWS)}
        conn=Connection()
        self.assertEqual(store.finish_derived(conn,event,result={'measurements':[item]},now=now,
            expected_source_scope='DERIVED_NATIVE_HYPE_PERP'),'SUPPLEMENTED')
        self.assertIn('DERIVED_NATIVE_HYPE_PERP:1',conn.calls[-1][1])
        self.assertTrue(all('research_ordered_first_touch_outcomes' not in query for query,_ in conn.calls))
        self.assertEqual(store.finish_derived(conn,event,result={'measurements':[item]},now=now,
            expected_source_scope='DERIVED_NATIVE_HYPE_MARK'),'BLOCKED')
        with self.assertRaises(ValueError):
            store.finish_derived(conn,event,result={'measurements':[item]},now=now,
                expected_source_scope='UNKNOWN_PRICE_SOURCE')


DSN=os.environ.get('TEST_DATABASE_URL') or os.environ.get('RESEARCH_TEST_POSTGRES_URL')


@unittest.skipUnless(DSN,'Explicit local/CI test PostgreSQL required')
class RecoveryPostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row
        from psycopg.conninfo import conninfo_to_dict
        info=conninfo_to_dict(DSN)
        if (info.get('host') not in {'localhost','127.0.0.1','::1','postgres'}
            or not(info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test'))):
            raise ValueError('Recovery tests require a disposable local/CI test DB')
        cls.pg,cls.sql,cls.rows=psycopg,sql,staticmethod(dict_row)

    def setUp(self):
        self.schema='test_recovery_'+uuid4().hex
        self.conn=self.pg.connect(DSN,row_factory=self.rows,connect_timeout=5)
        self.conn.execute(self.sql.SQL('CREATE SCHEMA {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute("""CREATE TABLE research_events(event_id BIGINT PRIMARY KEY,
            event_kind TEXT DEFAULT 'ALERT',delivery_status TEXT DEFAULT 'DELIVERED',
            event_type TEXT DEFAULT 'MAX_PAIN_ALERT',score FLOAT DEFAULT 70,
            direction TEXT DEFAULT 'LONG',symbol TEXT DEFAULT 'BTC',
            alert_time_utc TIMESTAMPTZ DEFAULT NOW()-INTERVAL '2 days');
            CREATE TABLE research_ordered_first_touch_outcomes(event_id BIGINT,window_minutes INT,
            threshold_bps INT,method_version TEXT,status TEXT,path_complete BOOLEAN,
            observed_through_utc TIMESTAMPTZ);
            CREATE TABLE research_common_window_metrics(event_id BIGINT,window_minutes INT,
            method_version TEXT,status TEXT,result JSONB);""",prepare=False)
        self.conn.execute((Path(__file__).parent/'migrations/041_ordered_outcome_recovery_queue.sql').read_text(),prepare=False)
        self.conn.commit()
        self.now=datetime.now(timezone.utc)

    def tearDown(self):
        self.conn.close()
        with self.pg.connect(DSN,autocommit=True) as conn:
            conn.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))

    def insert(self,*ids):
        self.conn.execute('INSERT INTO research_events(event_id) SELECT unnest(%s::bigint[])',(list(ids),))

    def test_idempotent_retry_is_durable_and_not_a_hot_loop(self):
        self.insert(1,2)
        self.assertEqual(store.enqueue(self.conn,[1,2],as_of=self.now,request_key='incident'),2)
        selected=store.load_due(self.conn,limit=1)[0]
        self.assertTrue(store.finish_error(self.conn,selected,error='temporary HTTP failure'))
        self.conn.commit()
        self.assertEqual([row['event_id'] for row in store.load_due(self.conn,limit=8)],[2])
        self.assertEqual(store.enqueue(self.conn,[1,2],as_of=self.now,request_key='incident'),0)
        row=self.conn.execute('SELECT * FROM research_ordered_outcome_recovery WHERE event_id=1').fetchone()
        self.assertEqual((row['status'],row['attempts']),('RETRY',1))

    def test_intake_pages_new_alerts_and_recovers_late_lower_ids(self):
        self.insert(1,3,4,5,6)
        self.assertEqual(store.intake(self.conn,now=self.now,backfill_days=14,limit=2),2)
        self.insert(2)
        for _ in range(6):store.intake(self.conn,now=self.now,backfill_days=14,limit=2)
        rows=self.conn.execute('SELECT event_id,attempts FROM research_ordered_outcome_recovery ORDER BY event_id').fetchall()
        self.assertEqual([r['event_id'] for r in rows],list(range(1,7)))
        self.assertTrue(all(r['attempts']==0 for r in rows))
        # A later lap does not extend the original target or reopen completed work.
        self.conn.execute("UPDATE research_ordered_outcome_recovery SET status='COMPLETE'")
        for _ in range(4):store.intake(self.conn,now=self.now+timedelta(hours=1),backfill_days=14,limit=2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS n FROM research_ordered_outcome_recovery WHERE status='COMPLETE'").fetchone()['n'],6)

    def test_invalid_id_is_rejected_without_partial_request(self):
        self.insert(1)
        self.conn.commit()
        with self.assertRaises(ValueError):store.enqueue(self.conn,[1,999],as_of=self.now,request_key='bad')
        self.conn.rollback()
        self.assertEqual(self.conn.execute('SELECT COUNT(*) AS n FROM research_ordered_outcome_recovery').fetchone()['n'],0)

    def test_single_slot_rotation_and_stale_ack_guard(self):
        self.assertEqual([store.next_requested_budget(self.conn,1) for _ in range(4)],[1,0,1,0])
        self.assertEqual(store.next_requested_budget(self.conn,8),4)
        self.insert(1)
        store.enqueue(self.conn,[1],as_of=self.now,request_key='first')
        old=store.load_due(self.conn,limit=1)[0]
        store.enqueue(self.conn,[1],as_of=self.now+timedelta(minutes=1),request_key='extended')
        self.assertFalse(store.finish_error(self.conn,old,error='old failure',blocked=True))
        self.assertEqual(self.conn.execute('SELECT status FROM research_ordered_outcome_recovery').fetchone()['status'],'PENDING')

    def test_requested_representatives_precede_automatic_history(self):
        self.insert(1,2,3)
        store.intake(self.conn,now=self.now,backfill_days=14)
        store.enqueue(self.conn,[3],as_of=self.now,request_key='representative',priority=2)
        self.assertEqual(store.load_due(self.conn,limit=1)[0]['event_id'],3)

    def test_ack_checks_stored_grid_and_rolls_back_with_writes(self):
        self.insert(1)
        store.enqueue(self.conn,[1],as_of=self.now,request_key='atomic')
        event=store.load_due(self.conn,limit=1)[0]
        self.conn.commit()
        self.assertEqual(store.finish_success(self.conn,event,now=self.now),'RETRY')
        self.conn.rollback()
        self.assertEqual(self.conn.execute('SELECT attempts,status FROM research_ordered_outcome_recovery').fetchone(),{'attempts':0,'status':'PENDING'})

    def test_complete_grid_ack_and_rollback_use_real_postgres(self):
        self.insert(1)
        start=self.now-timedelta(days=2)
        self.conn.execute('UPDATE research_events SET alert_time_utc=%s',(start,))
        store.enqueue(self.conn,[1],as_of=self.now,request_key='complete')
        event=store.load_due(self.conn,limit=1)[0]
        self.conn.commit()
        labels,metrics=grid(now=self.now,complete=True,start=start)
        with self.conn.cursor() as cur:
            cur.executemany('INSERT INTO research_ordered_first_touch_outcomes VALUES(%s,%s,%s,%s,%s,%s,%s)',
                [(1,row['window_minutes'],row['threshold_bps'],row['method_version'],row['status'],row['path_complete'],row['observed_through_utc']) for row in labels])
            cur.executemany('INSERT INTO research_common_window_metrics VALUES(%s,%s,%s,%s,%s::jsonb)',
                [(1,row['window_minutes'],row['method_version'],row['status'],json.dumps(row['result'],default=str)) for row in metrics])
        self.assertEqual(store.finish_success(self.conn,event,now=self.now),'COMPLETE')
        row=self.conn.execute('SELECT status,sheet_pending FROM research_ordered_outcome_recovery').fetchone()
        self.assertEqual(row,{'status':'COMPLETE','sheet_pending':True})
        self.conn.rollback()
        self.assertEqual(self.conn.execute('SELECT status FROM research_ordered_outcome_recovery').fetchone()['status'],'PENDING')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) AS n FROM research_ordered_first_touch_outcomes').fetchone()['n'],0)


@unittest.skipUnless(DSN,'Explicit local/CI test PostgreSQL required')
class RecoveryDeliveryPostgreSQLTests(unittest.TestCase):
    # Reuse actual canonical-writer fixtures, without inheriting/repeating
    # their unrelated tests. No transport or production connection is used.
    from research_ordered_first_touch_fresh_postgres_selftest import PostgreSQLOutcomeFreshnessTests as Fixture
    setUpClass=classmethod(lambda cls:cls.Fixture.setUpClass.__func__(cls))
    tearDown=Fixture.tearDown
    write_outcome=Fixture.write_outcome

    def setUp(self):
        self.Fixture.setUp(self)
        self.conn.execute((Path(__file__).parent/'migrations/041_ordered_outcome_recovery_queue.sql').read_text(),prepare=False)
        self.conn.commit()

    def test_requested_outbox_claim_keeps_normal_capacity_and_generation_ack(self):
        for event_id in (1,2,3,4):self.write_outcome(event_id,self.now-timedelta(days=1))
        store.enqueue(self.conn,[4],as_of=self.now,request_key='representative',priority=2)
        self.conn.execute('UPDATE research_ordered_outcome_recovery SET sheet_pending=TRUE')
        ids=store.delivery_ids(self.conn)
        requested=self.worker._claim_ordered_first_touch_lane(self.conn,
            store.delivery_budget(self.conn,2),recent=False,event_ids=ids)
        normal=self.worker._claim_ordered_first_touch_outbox(self.conn,2-len(requested))
        self.assertEqual([row['event_id'] for row in requested],[4])
        self.assertEqual(len(normal),1)
        self.assertNotEqual(normal[0]['event_id'],4)
        # Canonical recalculation replaces the queued generation. A stale ACK
        # cannot confirm that newer payload; reclaim and ACK its actual hash.
        self.write_outcome(4,self.now-timedelta(days=1),candles=2)
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn,requested,delivered=True),0)
        fresh=self.worker._claim_ordered_first_touch_lane(self.conn,1,recent=False,event_ids=[4])
        self.assertEqual(len(fresh),1)
        self.assertNotEqual(fresh[0]['claimed_payload_sha256'],requested[0]['claimed_payload_sha256'])
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn,fresh,delivered=True),1)
        self.assertEqual(self.conn.execute("SELECT sync_status FROM research_ordered_first_touch_sync_outbox WHERE event_id=4").fetchone()['sync_status'],'SYNCED')
        # One ACK is insufficient to report the full32-grid Sheet checkpoint.
        self.assertEqual(store.delivery_ids(self.conn),[4])

    def test_sheet_checkpoint_only_clears_after_all32_current_generations_synced(self):
        self.write_outcome(4,self.now-timedelta(days=1))
        store.enqueue(self.conn,[4],as_of=self.now,request_key='grid')
        self.conn.execute('UPDATE research_ordered_outcome_recovery SET sheet_pending=TRUE')
        import research_ordered_first_touch_worker_selftest as fixtures
        import research_ordered_first_touch as ordered
        start=self.now-timedelta(days=1)
        event={**fixtures._event(start),'event_id':4,'event_fingerprint':f'{4:064x}'}
        path=[fixtures._candle(start)]
        path_result={'symbol':'BTC','pair':'BTCUSDT','exchange':'binance','market':'spot',
            'interval':'1m','interval_seconds':60,'complete':True,'expected_candles':1,
            'provenance':'SELFTEST','candles':path}
        for window in store.WINDOWS:
            for bps in store.THRESHOLDS:
                outcome=ordered.calculate_ordered_first_touch_outcome(reference_price=100,direction='LONG',
                    event_time=start,candles=path,threshold_pct=bps/100,observation_closed=False)
                self.worker._write_ordered_first_touch_outcome(self.conn,event=event,window_minutes=window,
                    reference_source='binance_spot',path_result=path_result,outcome=outcome,expected_candles=1)
        self.assertEqual(store.delivery_ids(self.conn),[4])
        claimed=self.worker._claim_ordered_first_touch_lane(self.conn,32,recent=False,event_ids=[4])
        self.assertEqual(len(claimed),32)
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn,claimed[:-1],delivered=True),31)
        self.assertEqual(store.delivery_ids(self.conn),[4])
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn,claimed[-1:],delivered=True),1)
        self.assertEqual(store.delivery_ids(self.conn),[])
        self.assertFalse(self.conn.execute('SELECT sheet_pending FROM research_ordered_outcome_recovery').fetchone()['sheet_pending'])


if __name__=='__main__':unittest.main()
