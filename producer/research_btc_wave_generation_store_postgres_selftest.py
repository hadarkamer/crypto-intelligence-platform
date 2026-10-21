"""Real PostgreSQL source parts, snapshot validation and bounded legacy reuse."""
from datetime import timedelta
import json
import math
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_btc_wave_generation_store as store
import research_btc_wave_report_worker as worker
from research_btc_wave_endpoint_report_selftest import START, event
from research_btc_source_compaction_selftest import fetch_fixture


def install_source_schema(conn):
    """Isolated native source fixture shared with controller integration tests."""
    conn.execute('''CREATE TABLE research_btc_parent_movements (
        btc_parent_movement_id text PRIMARY KEY,episode_policy_version text,
        start_time_utc timestamptz,end_time_utc timestamptz,evidence_eligible boolean,
        boundary_reason text,observed_through_utc timestamptz,
        UNIQUE(episode_policy_version,start_time_utc))''')
    conn.execute('''CREATE TABLE research_events (
        event_id bigint PRIMARY KEY,event_kind text,event_type text,alert_time_utc timestamptz,
        symbol text,direction text,score double precision,current_price double precision,
        delivery_status text,engine_snapshot jsonb,source_side text,timeframe text,
        target_price double precision,initial_target_distance_pct double precision,
        event_fingerprint text,strategy_version text,code_version text)''')
    conn.execute('''CREATE TABLE research_event_btc_movements (
        event_id bigint,episode_policy_version text,btc_parent_movement_id text,
        membership_status text,decision_time_utc timestamptz,btc_observed_close_utc timestamptz,
        PRIMARY KEY(event_id,episode_policy_version))''')


def insert_event(conn, row):
    fields = ('event_id','event_kind','event_type','alert_time_utc','symbol','direction',
        'score','current_price','delivery_status','engine_snapshot','source_side','timeframe',
        'target_price','initial_target_distance_pct','event_fingerprint','strategy_version','code_version')
    conn.execute('INSERT INTO research_events ('+','.join(fields)+') VALUES ('+
        ','.join('%s::jsonb' if key=='engine_snapshot' else '%s' for key in fields)+')',
        tuple(json.dumps(row.get(key), ensure_ascii=False, allow_nan=False) if key=='engine_snapshot'
              else row.get(key) for key in fields))
    conn.execute('''INSERT INTO research_event_btc_movements VALUES (%s,%s,%s,%s,%s,%s)''',
        (row['event_id'],worker.report.POLICY_VERSION,row['btc_parent_movement_id'],
         row['membership_status'],row['decision_time_utc'],row['btc_observed_close_utc']))


def populate_source(conn, parent_count=33, *, include_events=True, ineligible_first=False):
    observed_at = START+timedelta(hours=parent_count)
    # Reverse physical insertion order must not affect the manifest or selection.
    for index in reversed(range(parent_count)):
        start = START+timedelta(hours=index)
        end = start+timedelta(hours=1) if index+1<parent_count else None
        eligible = not (ineligible_first and index==0)
        conn.execute('''INSERT INTO research_btc_parent_movements VALUES (%s,%s,%s,%s,%s,%s,%s)''',
            (f'wave-{index:04d}',worker.report.POLICY_VERSION,start,end,eligible,
             'CAUSAL_CLOSE_REVERSAL' if eligible else 'LEFT_BOUNDARY_UNVERIFIED',
             start+timedelta(hours=1)-worker.report.MILLISECOND))
        if include_events:
            row = event(index+1, seconds=index*3600+10, wave_id=f'wave-{index:04d}')
            insert_event(conn,row)
    return observed_at


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'TEST_DATABASE_URL required for real PostgreSQL')
class GenerationStorePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.dsn = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(cls.dsn)
        if info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or 'test' not in info.get('dbname','').lower():
            raise RuntimeError('Only local/CI test databases are allowed')
        cls.conn = psycopg.connect(cls.dsn,row_factory=dict_row,autocommit=True)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.schema = 'wave_generation_test_'+uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        install_source_schema(self.conn)
        root = Path(__file__).parent/'migrations'
        self.conn.execute((root/'045_btc_wave_report_refresh.sql').read_text())
        self.conn.execute((root/'057_btc_wave_report_parts.sql').read_text())
        self.conn.execute((root/'057_btc_wave_report_parts.sql').read_text())
        self.assertTrue(store.schema_ready(self.conn))

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def freeze(self, observed_at):
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            return store.freeze(self.conn,worker.VERSION,observed_at)

    def complete_parts(self, generation):
        for part in store.iter_parts(self.conn,generation,include_payload=False):
            with self.conn.transaction():
                self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                source = store.load_part_source(self.conn,part['parent_id'],generation['observed_at_utc'])
                job = worker.prepare_job(source,generation['observed_at_utc'])
                while not worker.advance_job(job,fetcher=fetch_fixture)['completed']:
                    pass
                completed = worker.finish_job(job)
                store.save_checkpoint(self.conn,generation,part['ordinal'],source=source,job=None,completed_report=completed)

    def validate(self,generation):
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            return store.validate_generation(self.conn,generation)

    def test_freeze_retains_thirty_three_parents_empty_and_ineligible(self):
        observed = populate_source(self.conn,33,include_events=False,ineligible_first=True)
        generation = self.freeze(observed)
        self.assertEqual(generation['parent_count'],33)
        parts = list(store.iter_parts(self.conn,generation,include_payload=False))
        self.assertEqual([row['ordinal'] for row in parts],list(range(33)))
        self.assertEqual([row['parent_id'] for row in parts],[f'wave-{i:04d}' for i in range(33)])
        self.assertTrue(all(not row['has_source'] for row in parts))
        self.assertTrue(store.has_unfinished_parts(self.conn,generation))
        self.assertEqual(store.next_unfinished_part(self.conn,generation)['ordinal'],0)
        self.complete_parts(generation)
        checked = self.validate(generation)
        self.assertTrue(checked['valid'])
        self.assertEqual(checked['manifest']['parent_count'],33)
        self.assertEqual(checked['source_digest'],checked['manifest']['source_manifest_digest'])
        self.assertFalse(store.has_unfinished_parts(self.conn,generation))
        self.assertIsNone(store.next_unfinished_part(self.conn,generation))

    def test_native_projection_and_selected_hype_exactness_with_frozen_successor(self):
        observed = populate_source(self.conn,3)
        for eid, seconds in ((100,61),(101,62)):
            row = event(eid,symbol='HYPE',seconds=seconds,wave_id='wave-0000')
            row['engine_snapshot']={'price_source':'hyperliquid','price_pair':'HYPEUSDT','body':'x'*12000}
            insert_event(self.conn,row)
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            native = worker.load_job_source(self.conn,observed)
            parts = [store.load_part_source(self.conn,f'wave-{i:04d}',observed) for i in range(3)]
        combined = {'waves':[part['waves'][0] for part in parts],
                    'events':[row for part in parts for row in part['events']]}
        self.assertEqual(store.canonical(combined),store.canonical(native))
        hype = [row for row in parts[0]['events'] if row['symbol']=='HYPE']
        self.assertEqual([row['event_id'] for row in hype],[100,101])
        self.assertIn('body',hype[0]['engine_snapshot'])
        self.assertNotIn('body',hype[1]['engine_snapshot'])
        self.assertEqual(set(parts[0]['successor_context'][0]),
            {'btc_parent_movement_id','episode_policy_version','start_time_utc','evidence_eligible','boundary_reason'})
        before=store.digest(parts[0])
        self.conn.execute("UPDATE research_btc_parent_movements SET observed_through_utc=%s WHERE btc_parent_movement_id='wave-0001'",
                          (observed+timedelta(days=1),))
        self.assertEqual(store.digest(store.load_part_source(self.conn,'wave-0000',observed)),before)
        early=store.load_part_source(self.conn,'wave-0000',START+timedelta(minutes=30))
        self.assertIsNone(early['waves'][0]['end_time_utc'])
        self.assertEqual(early['successor_context'],[])

    def test_final_snapshot_detects_late_earlier_event_removal_and_parent_addition(self):
        observed=populate_source(self.conn,3)
        generation=self.freeze(observed)
        self.complete_parts(generation)
        self.assertTrue(self.validate(generation)['valid'])
        insert_event(self.conn,event(100,seconds=5,wave_id='wave-0000'))
        self.assertEqual(self.validate(generation)['reason'],'SOURCE_CHANGED')
        self.conn.execute('DELETE FROM research_event_btc_movements WHERE event_id=100')
        self.conn.execute('DELETE FROM research_events WHERE event_id=100')
        self.assertTrue(self.validate(generation)['valid'])
        self.conn.execute('DELETE FROM research_event_btc_movements WHERE event_id=1')
        self.conn.execute('DELETE FROM research_events WHERE event_id=1')
        self.assertEqual(self.validate(generation)['reason'],'SOURCE_CHANGED')
        insert_event(self.conn,event(1,seconds=10,wave_id='wave-0000'))
        self.conn.execute('''INSERT INTO research_btc_parent_movements VALUES (%s,%s,%s,%s,%s,%s,%s)''',
            ('late-parent',worker.report.POLICY_VERSION,START+timedelta(minutes=30),START+timedelta(minutes=40),
             False,'BTC_DATA_GAP',START+timedelta(minutes=39)))
        self.assertEqual(self.validate(generation)['reason'],'PARENT_POPULATION_CHANGED')

    def test_stored_source_and_report_tamper_rejects_and_sql_null_proof_is_not_allowed(self):
        observed=populate_source(self.conn,1)
        generation=self.freeze(observed)
        self.complete_parts(generation)
        for field in ('source','report'):
            with self.subTest(field=field):
                self.conn.execute(f"UPDATE research_btc_wave_report_parts SET {field}=({field}::jsonb||'{{\"tampered\":true}}'::jsonb)::json")
                self.assertEqual(self.validate(generation)['reason'],'STORED_PART_INTEGRITY_FAILED')
                # Restore the exact persisted part for the next independent check.
                self.conn.execute(f"UPDATE research_btc_wave_report_parts SET {field}=({field}::jsonb-'tampered')::json")
        import psycopg
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.conn.execute('UPDATE research_btc_wave_report_parts SET source_digest=NULL')
        with self.assertRaisesRegex(ValueError,'REPEATABLE READ'):
            store.validate_generation(self.conn,generation)

    def test_restart_checkpoint_and_per_part_sixteen_mib_limits(self):
        observed=populate_source(self.conn,1)
        generation=self.freeze(observed)
        source=store.load_part_source(self.conn,'wave-0000',observed)
        job=worker.prepare_job(source,observed)
        before=store.canonical(job)
        store.save_checkpoint(self.conn,generation,0,source=source,job=job)
        self.assertEqual(store.canonical(job),before)
        stored=store.get_part(self.conn,generation,0)
        self.assertNotIn('source',stored['job'])
        resumed={**stored['job'],'source':stored['source']}
        self.assertEqual(store.canonical(resumed),before)
        too_large={**source,'oversized':'x'*store.MAX_PART_BYTES}
        with self.assertRaisesRegex(ValueError,'SOURCE_PART_EXCEEDS'):
            store.save_checkpoint(self.conn,generation,0,source=too_large,job=None)
        report_large={'source_digest':store.digest(source),'oversized':'x'*store.MAX_PART_BYTES}
        with self.assertRaisesRegex(ValueError,'REPORT_PART_EXCEEDS'):
            store.save_checkpoint(self.conn,generation,0,source=source,job=None,completed_report=report_large)
        self.assertEqual(store.get_part(self.conn,generation,0)['job'],stored['job'])

    def test_source_checkpoint_and_report_preserve_original_numeric_literals_and_hashes(self):
        observed=populate_source(self.conn,1)
        self.conn.execute('UPDATE research_events SET current_price=%s',(1e16,))
        generation=self.freeze(observed)
        source=store.load_part_source(self.conn,'wave-0000',observed)
        self.assertIs(type(source['events'][0]['current_price']),float)
        numbers={'large':1e16,'negative_zero':-0.0,'small':1e-16,'integer':10**16}
        source['numeric_roundtrip_probe']=numbers
        job=worker.prepare_job(source,observed)
        job['numeric_roundtrip_probe']=numbers
        original_job=store.canonical(job)
        source_hash=store.digest(source)
        store.save_checkpoint(self.conn,generation,0,source=source,job=job)
        persisted=store.get_part(self.conn,generation,0)
        self.assertEqual(store.canonical(persisted['source']),store.canonical(source))
        self.assertEqual(persisted['source_digest'],source_hash)
        self.assertEqual(store.digest(persisted['source']),source_hash)
        self.assertEqual(store.canonical({**persisted['job'],'source':persisted['source']}),original_job)
        completed={'source_digest':source_hash,'method_version':worker.report.METHOD_VERSION,
                   'observed_at_utc':observed,'numeric_roundtrip_probe':numbers,'records':[]}
        store.save_checkpoint(self.conn,generation,0,source=source,job=None,completed_report=completed)
        final=store.get_part(self.conn,generation,0)
        self.assertEqual(store.canonical(final['report']),store.canonical(completed))
        self.assertEqual(final['report_digest'],store.digest(completed))
        for payload in (persisted['source'],persisted['job'],final['report']):
            probe=payload['numeric_roundtrip_probe']
            self.assertIs(type(probe['large']),float)
            self.assertIs(type(probe['small']),float)
            self.assertIs(type(probe['integer']),int)
            self.assertEqual(math.copysign(1.,probe['negative_zero']),-1.)
        self.assertEqual(final['source_bytes'],len(store.canonical(source).encode()))
        self.assertEqual(final['report_bytes'],len(store.canonical(completed).encode()))

    def test_legacy_and_published_part_cache_are_filtered_to_one_parent_with_successor(self):
        observed=populate_source(self.conn,3)
        source=worker.load_job_source(self.conn,observed)
        job=worker.prepare_job(source,observed)
        self.assertTrue(worker.advance_job(job,fetcher=fetch_fixture)['completed'])
        completed=worker.finish_job(job)
        self.conn.execute('''UPDATE research_btc_wave_report_state SET source=%s::jsonb,report=%s::jsonb''',
                          (store.canonical(source),store.canonical(completed)))
        previous=store.get_previous_parent_cache(self.conn,worker.VERSION,'wave-0000')
        self.assertEqual([row['btc_parent_movement_id'] for row in previous['source']['waves']],['wave-0000','wave-0001'])
        self.assertEqual([row['event_id'] for row in previous['source']['events']],[1])
        self.assertTrue(all(row['btc_parent_movement_id']=='wave-0000' for row in previous['report']['records']))
        incoming=store.cache_source(store.load_part_source(self.conn,'wave-0000',observed))
        cache=worker.report.verified_closed_cache(previous_report=previous['report'],previous_source=previous['source'],
            **incoming,observed_at=observed,include_hype_perp=True)
        self.assertEqual(set(cache),{(worker.report.SPOT_SCOPE,1)})
        generation=self.freeze(observed)
        self.complete_parts(generation)
        manifest=self.validate(generation)['manifest']
        store.mark_generation(self.conn,generation,'PUBLISHED')
        self.conn.execute('UPDATE research_btc_wave_report_state SET source=%s::jsonb',(store.canonical(manifest),))
        published=store.get_previous_parent_cache(self.conn,worker.VERSION,'wave-0000')
        self.assertEqual(published['source']['waves'][0],previous['source']['waves'][0])
        self.assertEqual(published['source']['events'],previous['source']['events'])
        self.assertEqual(published['source']['waves'][1]['btc_parent_movement_id'],'wave-0001')
        self.assertTrue(all(row['btc_parent_movement_id']=='wave-0000' for row in published['report']['records']))


if __name__=='__main__':
    unittest.main()
