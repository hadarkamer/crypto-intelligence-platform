"""Offline source-part parity, strict hashing, bounded lifetime and manifests."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4
import weakref

import research_btc_wave_generation_store as store
import research_btc_wave_report_worker as worker
from research_btc_source_compaction_selftest import SourceConnection, Result, OBSERVED, legacy_compacted_source
from research_btc_wave_endpoint_report_selftest import START, event, wave


class PartSourceConnection(SourceConnection):
    def execute(self, sql, params=()):
        normalized=' '.join(sql.split())
        if normalized.startswith('SELECT btc_parent_movement_id, episode_policy_version,start_time_utc,evidence_eligible,boundary_reason'):
            policy,start,observed=params
            return Result([{key: row[key] for key in ('btc_parent_movement_id','episode_policy_version',
                    'start_time_utc','evidence_eligible','boundary_reason')}
                for row in self.waves if row['episode_policy_version']==policy and row['start_time_utc']==start
                and row['start_time_utc']<=observed and row['evidence_eligible'] is True
                and row['boundary_reason']=='CAUSAL_CLOSE_REVERSAL'][:2])
        return super().execute(sql,params)


def source_fixture(index=0):
    parent=wave(f'wave-{index:04d}')
    parent['start_time_utc']=START+timedelta(hours=index)
    parent['end_time_utc']=START+timedelta(hours=index+1)
    parent['observed_through_utc']=parent['end_time_utc']-worker.report.MILLISECOND
    row=event(index+1,seconds=index*3600+10,wave_id=parent['btc_parent_movement_id'])
    return {'waves':[parent],'events':[row],'successor_context':[]}


class GenerationStoreTests(unittest.TestCase):
    def test_one_parent_projection_matches_legacy_population_and_frozen_boundaries(self):
        legacy=legacy_compacted_source(SourceConnection(),OBSERVED)
        conn=PartSourceConnection()
        parts=[store.load_part_source(conn,parent['btc_parent_movement_id'],OBSERVED) for parent in legacy['waves']]
        combined={'waves':[part['waves'][0] for part in parts],
                  'events':[row for part in parts for row in part['events']]}
        self.assertEqual(worker.canonical(combined),worker.canonical(legacy))
        self.assertEqual(parts[1]['events'],[])  # Ineligible parent is retained.
        self.assertEqual(parts[-1]['successor_context'],[])
        self.assertIsNone(parts[-1]['waves'][0]['end_time_utc'])
        self.assertEqual(parts[2]['successor_context'][0]['btc_parent_movement_id'],'wave-2')
        self.assertEqual(len(store.cache_source(parts[2])['waves']),2)
        self.assertEqual(len(parts[2]['waves']),1)

    def test_source_caps_are_checked_before_projection(self):
        with patch.object(worker.report,'MAX_EVENTS_PER_WAVE',2):
            with self.assertRaisesRegex(ValueError,'per-wave budget'):
                store.load_part_source(PartSourceConnection(),'wave-0',OBSERVED)
        self.assertEqual(worker.report.MAX_WAVES,32)
        self.assertEqual(store.MAX_PART_BYTES,16*1024*1024)

    def test_payload_exact_canonical_hash_and_byte_cap(self):
        source=source_fixture()
        text,checksum,count=store._payload(source,'SOURCE')
        self.assertEqual(text,worker.canonical(source))
        self.assertEqual(checksum,worker.digest(source))
        self.assertEqual(count,len(text.encode()))
        with self.assertRaisesRegex(ValueError,'SOURCE_PART_EXCEEDS_16_MIB'):
            store._payload({'body':'x'*store.MAX_PART_BYTES},'SOURCE')

    def validation_fixture(self,count=33):
        generation={'generation_id':str(uuid4()),'worker_key':worker.VERSION,
            'observed_at_utc':START+timedelta(hours=count),'parent_count':count,'status':'CAPTURED'}
        sources=[source_fixture(index) for index in range(count)]
        parts=[]
        for index,source in enumerate(sources):
            _,source_hash,source_bytes=store._payload(source,'SOURCE')
            result={'method_version':worker.report.METHOD_VERSION,'source_digest':source_hash,
                    'observed_at_utc':generation['observed_at_utc'],'records':[]}
            _,report_hash,report_bytes=store._payload(result,'REPORT')
            parts.append({'ordinal':index,'parent_id':source['waves'][0]['btc_parent_movement_id'],
                'parent_start_utc':source['waves'][0]['start_time_utc'],'source':source,
                'source_digest':source_hash,'source_bytes':source_bytes,'report':result,
                'report_digest':report_hash,'report_bytes':report_bytes})
        parents=[{'btc_parent_movement_id':part['parent_id'],'start_time_utc':part['parent_start_utc']} for part in parts]
        return generation,sources,parts,parents

    def validate_fixture(self,generation,sources,parts,parents,*,get_part=None,loader=None):
        metadata=[{key:value for key,value in part.items() if key not in {'source','report'}} for part in parts]
        with patch.object(store,'_require_snapshot'),patch.object(store,'get_generation',return_value=generation), \
             patch.object(store,'iter_parts',side_effect=lambda conn,gen,include_payload:iter(metadata)) as iterator, \
             patch.object(store,'_iter_native_parents',side_effect=lambda conn,observed:iter(parents)), \
             patch.object(store,'get_part',side_effect=get_part or (lambda conn,gen,index:deepcopy(parts[index]))), \
             patch.object(store,'load_part_source',side_effect=loader or (lambda conn,parent,observed:deepcopy(
                 sources[next(i for i,row in enumerate(parts) if row['parent_id']==parent)]))):
            result=store.validate_generation(Mock(),generation)
        iterator.assert_called_once_with(unittest.mock.ANY,generation,include_payload=False)
        return result

    def test_thirty_three_part_manifest_is_deterministic_and_binds_empty_parent(self):
        generation,sources,parts,parents=self.validation_fixture()
        first=self.validate_fixture(generation,sources,parts,parents)
        second=self.validate_fixture(generation,sources,parts,parents)
        self.assertTrue(first['valid'])
        self.assertEqual(first,second)
        self.assertEqual(first['manifest']['parent_count'],33)
        self.assertEqual(first['manifest']['storage_version'],'btc-wave-parts-v1')
        self.assertNotIn('events',first['manifest'])
        self.assertNotIn('parts',first['manifest'])
        self.assertEqual(self.validate_fixture(generation,sources,parts,parents[:-1])['reason'],'PARENT_POPULATION_CHANGED')
        changed=deepcopy(parents)
        changed[4]['btc_parent_movement_id']='inserted-earlier'
        self.assertEqual(self.validate_fixture(generation,sources,parts,changed)['reason'],'PARENT_POPULATION_CHANGED')

    def test_sources_reports_and_full_snapshot_digest_are_rechecked(self):
        for field in ('source','report'):
            with self.subTest(field=field):
                generation,sources,parts,parents=self.validation_fixture(2)
                parts[0][field]['tampered']=True
                checked=self.validate_fixture(generation,sources,parts,parents)
                self.assertEqual(checked['reason'],'STORED_PART_INTEGRITY_FAILED')
        generation,sources,parts,parents=self.validation_fixture(2)
        sources=deepcopy(sources)
        sources[0]['events'][0]['engine_snapshot']['changed_original_body']=True
        self.assertEqual(self.validate_fixture(generation,sources,parts,parents)['reason'],'SOURCE_CHANGED')
        generation,sources,parts,parents=self.validation_fixture(2)
        parts[0]['report']=None
        self.assertEqual(self.validate_fixture(generation,sources,parts,parents)['reason'],'INCOMPLETE_PART')

    def test_stored_payload_released_before_next_native_body_is_hydrated(self):
        generation,sources,parts,parents=self.validation_fixture(3)
        class Graph(dict): pass
        refs=[]
        def stored(conn,gen,index):
            part=deepcopy(parts[index])
            part['source']=Graph(part['source'])
            part['report']=Graph(part['report'])
            refs.extend((weakref.ref(part['source']),weakref.ref(part['report'])))
            return part
        def native(conn,parent,observed):
            self.assertTrue(all(ref() is None for ref in refs),'Stored payload survived into native hydration')
            return deepcopy(sources[next(i for i,row in enumerate(parts) if row['parent_id']==parent)])
        self.assertTrue(self.validate_fixture(generation,sources,parts,parents,get_part=stored,loader=native)['valid'])

    def test_validation_requires_single_consistent_transaction(self):
        conn=Mock()
        conn.execute.return_value.fetchone.return_value={'transaction_isolation':'read committed'}
        with self.assertRaisesRegex(ValueError,'one REPEATABLE READ transaction'):
            store.validate_generation(conn,{'generation_id':str(uuid4())})
        self.assertEqual(conn.execute.call_count,1)


if __name__=='__main__':
    unittest.main()
