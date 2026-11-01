"""Pure controlled trial tests. No /proc, real sleeps or native functions."""
from datetime import datetime,timedelta,timezone
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import research_resident_memory as m
import research_resident_smaps as maps

IDENTITY={'session':'a'*32,'runtime_commit':'b'*40,'origin_cycle_id':'shared-watch:2026-11-01T15:32:15+00:00','observation_index':5}
INFO={'platform':'linux','implementation':'cpython','python_version':[3,11,9],'pointer_bytes':8,'size_t_bytes':8,'int_bytes':4,'glibc_version':'2.36','debug_build':False}


def raw_map(rss=8):
    values={'Size':16,'Rss':rss,'Pss':rss,'Shared_Clean':0,'Shared_Dirty':0,'Private_Clean':0,'Private_Dirty':rss,'Anonymous':rss,'Swap':0,'Shared_Hugetlb':0,'Private_Hugetlb':0,'AnonHugePages':0,'LazyFree':0,'KernelPageSize':4,'MMUPageSize':4}
    return b'1000-5000 rw-p 00000000 00:00 0 [heap]\n'+''.join(f'{k}: {v} kB\n' for k,v in values.items()).encode()


def memory():
    return {'version':'runtime-memory-sample-v1','status':'COMPLETE','self':{'rss_bytes':100*1024*1024},
        'descendants':{'browser_named':{'process_count':0},'other':{'process_count':0}},
        'cgroup':{'current_bytes':200*1024*1024,'max_bytes':2*1024*1024*1024},'limits':{'budget_ms':500},'errors':{}}


class Harness:
    def __init__(self):
        self.t=0.;self.sleeps=[];self.reads=[];self.trim_calls=[];self.emits=[];self.context_calls=0;self.process_calls=0
        self.factory_calls=0;self.open_calls=0;self.return_value=1;self.trim_cost=0.01;self.trim_error=None
        self.process_cost=0.01;self.read_cost=0.001;self.parse_cost=0.002
        self.info=copy.deepcopy(INFO);self.process_mutator=None;self.parser_mutator=None;self.admission_mutator=None;self.context_override=None
        self.reader_override=None;self.admission_error=None;self.emitter_error=False
    def clock(self):return self.t
    def utc(self):return (datetime(2026,11,1,15,48,tzinfo=timezone.utc)+timedelta(seconds=self.t)).isoformat()
    def sleep(self,seconds):self.sleeps.append(seconds);self.t+=seconds
    def context(self):
        self.context_calls+=1
        return self.context_override(self) if self.context_override else True
    def process(self):
        self.process_calls+=1
        cost=self.process_cost(self) if callable(self.process_cost) else self.process_cost;self.t+=cost
        rss=100*1024*1024+self.process_calls*4096-(40960 if self.trim_calls else 0)
        fields={key:0 for key in m._ROLLUP};fields.update(Rss=rss,Pss=rss,Private_Dirty=rss,Anonymous=rss)
        mall={key:0 for key in m._MALL};mall.update(arena=100000,uordblks=60000,fordblks=40000)
        p={'version':'process-memory-diagnostics-v1','status':'COMPLETE','platform':'LINUX_PROCFS','sample_started_at_utc':self.utc(),'sample_completed_at_utc':self.utc(),'elapsed_ms':cost*1000,'rss_bytes':rss,
            'status_read':{'status':'COMPLETE','fields_bytes':{'VmRSS':rss},'missing_required':[],'errors':[]},
            'smaps_rollup':{'status':'COMPLETE','fields_bytes':fields,'missing_required':[],'errors':[]},
            'python_counters':{'allocated_blocks_count':1000+self.process_calls,'gc_count_tuple':[1,2,3],'errors':[],'scope':'COUNTS_NOT_HEAP_BYTES_LIVE_OBJECTS_OR_COLLECTION_CAUSE'},
            'allocator':{'status':'COMPLETE','glibc_version':'2.36','size_t_bytes':8,'fields':mall,'field_units':{k:'COUNT' if k in ('ordblks','smblks','hblks') else 'BYTES' for k in m._MALL},'coherence':True,'scope':'GLIBC_REPORTED_ALLOCATOR_TOTALS_VERSION_DEPENDENT_ARENA_COVERAGE_NOT_RSS_ALL_MAPPINGS_OR_PYTHON_CACHE_OWNERSHIP','errors':[]},
            'limits':{'status_bytes':16384,'smaps_rollup_bytes':65536,'status_read_calls_max':1,'smaps_read_calls_max':1},'errors':[],
            'scope':'PROCESS_MAPPING_AND_ALLOCATOR_GAUGES_NONATOMIC_NOT_CACHE_BYTES_OR_ALLOCATION_OWNERSHIP','observer_effect':'Sequential readings are not atomic.'}
        if self.process_mutator:self.process_mutator(p,self)
        return p
    def opener(self):
        self.open_calls+=1
        if self.reader_override:return self.reader_override(self)
        owner=self
        class Stream(io.BytesIO):
            def read(self,n=-1):
                owner.reads.append(n);owner.t+=owner.read_cost
                return super().read(n)
        return Stream(raw_map(4 if self.trim_calls else 8))
    def parser(self,raw,**kwargs):
        self.t+=self.parse_cost;p=maps.parse_smaps(raw,**kwargs)
        if self.parser_mutator:self.parser_mutator(p,self)
        return p
    def factory(self):
        self.factory_calls+=1
        return {'status':'READY','info':copy.deepcopy(self.info),'trim':self.trim}
    def trim(self,pad):
        self.trim_calls.append((pad,self.t));self.t+=self.trim_cost
        if self.trim_error:raise self.trim_error
        return self.return_value
    def admission(self):
        if self.admission_error:raise self.admission_error
        started=self.utc();self.t+=0.1
        p=memory();trace={'version':'fresh-retained-root-admission-v1','status':'ADMITTED','reason':'FRESH_COMPLETE_SAFE','samples':[{'index':1,'started_at_utc':started,'completed_at_utc':self.utc(),'elapsed_ms':100.,'sample':p,'decision':'ADMIT'}],
            'selected_sample_index':1,'cached_tail_memory':memory(),'max_samples':3,'budget_seconds':2.5,'scope':'FRESH_COMPLETE_SAFETY_ONLY_CACHED_PARTIAL_CONTEXT_NOT_RELABELLED','elapsed_ms':100.}
        if self.admission_mutator:self.admission_mutator(trace,self)
        return trace
    def emit(self,label,identity):
        self.emits.append((label,copy.deepcopy(identity)))
        if self.emitter_error:raise RuntimeError('private text must not be retained')
        return {'status':'EMITTED_NOT_YET_INGESTED','label':label,'started_at_utc':self.utc(),'completed_at_utc':self.utc()}
    def run(self,**overrides):
        options=dict(smaps_opener=self.opener,process_snapshot=self.process,parse_smaps=self.parser,monotonic=self.clock,utc_now=self.utc,sleep=self.sleep,native_factory=self.factory,context_check=self.context,admission=self.admission,emit=self.emit,identity=IDENTITY)
        options.update(overrides);return m.run_resident_probe(**options)


class TrialTests(unittest.TestCase):
    def assert_no_trim(self,h,result):
        self.assertEqual(h.trim_calls,[]);self.assertFalse(result['trim']['attempted']);self.assertEqual(result['trim']['call_count'],0)
    def test_complete_five_slots_once_and_fixed_timing(self):
        h=Harness();r=h.run();self.assertEqual(r['status'],'COMPLETE');self.assertTrue(r['protocol_complete']);self.assertTrue(r['all_five_snapshots_complete'])
        self.assertEqual([s['name'] for s in r['snapshots']],list(m.SNAPSHOT_NAMES));self.assertEqual(len(h.trim_calls),1);self.assertEqual(h.trim_calls[0][0],0)
        self.assertEqual(r['trim']['return_value'],1);self.assertFalse(r['native']['function_serialized']);self.assertEqual([x[0] for x in h.emits],['BEFORE_SHAM','AFTER_SETTLED'])
        parse=lambda s:datetime.fromisoformat(s)
        self.assertGreaterEqual((parse(r['snapshots'][1]['started_at_utc'])-parse(r['snapshots'][0]['completed_at_utc'])).total_seconds(),1.)
        self.assertGreaterEqual((parse(r['snapshots'][3]['started_at_utc'])-parse(r['trim']['started_at_utc'])).total_seconds(),1.)
        self.assertGreaterEqual((parse(r['snapshots'][4]['started_at_utc'])-parse(r['trim']['completed_at_utc'])).total_seconds(),5.)
        self.assertLess(r['elapsed_ms'],15000);self.assertTrue(all(s['smaps_read']['eof'] for s in r['snapshots']))
    def test_zero_return_is_not_bytes_or_failure(self):
        h=Harness();h.return_value=0;r=h.run();self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['trim']['return_value'],0)
        self.assertIn('NOT_BYTES',r['trim']['return_semantics']);self.assertIsNone(r['conditional_resident_free_bytes'])
    def test_native_baseexception_consumes_one_attempt_and_keeps_post(self):
        class Stop(BaseException):pass
        h=Harness();h.trim_error=Stop('secret');r=h.run();self.assertEqual(len(h.trim_calls),1);self.assertEqual(r['trim']['outcome'],'EXCEPTION_NOT_RETRIED')
        self.assertTrue(r['protocol_complete']);self.assertEqual(r['status'],'PARTIAL');self.assertEqual(r['snapshots'][4]['status'],'COMPLETE');self.assertNotIn('secret',json.dumps(r))
    def test_boolean_native_result_is_not_int_return(self):
        h=Harness();h.return_value=True;r=h.run();self.assertEqual(r['trim']['call_count'],1);self.assertFalse(r['trim']['valid_return']);self.assertIsNone(r['trim']['return_value'])
    def test_unexpected_native_result_is_retained_as_invalid_not_retried(self):
        h=Harness();h.return_value=42;r=h.run();self.assertEqual(r['trim']['call_count'],1);self.assertIn('TRIM_INVALID_RETURN',r['reason_codes'])
    def test_each_unsupported_runtime_abi_dimension_no_reads_or_trim(self):
        for key,value in (('platform','darwin'),('implementation','pypy'),('python_version',[3,11,8]),('pointer_bytes',4),('size_t_bytes',4),('int_bytes',8),('glibc_version','2.37'),('debug_build',True),('size_t_bytes',True)):
            with self.subTest(key=key,value=value):
                h=Harness();h.info[key]=value;r=h.run();self.assert_no_trim(h,r);self.assertEqual(h.open_calls,0);self.assertIn('UNSUPPORTED_RUNTIME_ABI_GLIBC',r['reason_codes'])
    def test_factory_exception_does_not_trim(self):
        h=Harness()
        def fail():raise RuntimeError('private native library path')
        r=h.run(native_factory=fail);self.assert_no_trim(h,r);self.assertNotIn('private',json.dumps(r))
    def test_initial_unknown_or_unsafe_context_never_opens(self):
        for value in (False,None,1,'True'):
            with self.subTest(value=value):
                h=Harness();r=h.run(context_check=lambda:value);self.assert_no_trim(h,r);self.assertEqual(h.factory_calls,0);self.assertEqual(h.open_calls,0)
    def test_context_change_during_pretrim_snapshot_blocks_trim(self):
        h=Harness();h.context_override=lambda x:x.process_calls<6;r=h.run();self.assert_no_trim(h,r);self.assertEqual(r['snapshots'][2]['status'],'PARTIAL')
    def test_context_change_immediately_before_trim_blocks(self):
        h=Harness();h.context_override=lambda x:x.context_calls<10;r=h.run();self.assert_no_trim(h,r)
    def test_partial_sham_maps_retained_and_no_trim(self):
        h=Harness();h.parser_mutator=lambda p,_:p.update(status='PARTIAL',reason_codes=['READ_NOT_COMPLETE']);r=h.run();self.assert_no_trim(h,r)
        self.assertEqual(r['snapshots'][0]['status'],'PARTIAL');self.assertIsNotNone(r['snapshots'][0]['smaps']);self.assertEqual(r['snapshots'][1]['status'],'NOT_CAPTURED')
    def test_partial_process_prevents_trim(self):
        h=Harness();h.process_mutator=lambda p,_:p.update(status='PARTIAL');r=h.run();self.assert_no_trim(h,r);self.assertEqual(r['snapshots'][0]['status'],'PARTIAL')
    def test_process_unknown_raw_key_is_never_persisted(self):
        h=Harness();h.process_mutator=lambda p,_:p.update(raw_smaps='private path');r=h.run();self.assert_no_trim(h,r);self.assertNotIn('private path',json.dumps(r));self.assertIn('PROCESS_SCHEMA',r['snapshots'][0]['reason_codes'])
    def test_nested_process_payload_key_rejected(self):
        h=Harness();h.process_mutator=lambda p,_:p['status_read'].update(path='/private/path');r=h.run();self.assert_no_trim(h,r);self.assertNotIn('/private/path',json.dumps(r))
    def test_malformed_nested_process_complete_cannot_authorize_trim(self):
        h=Harness();h.process_mutator=lambda p,_:p.update(python_counters={});r=h.run();self.assert_no_trim(h,r)
    def test_reader_type_and_exception_are_closed_failures(self):
        for output in ('not bytes',RuntimeError('private maps')):
            h=Harness()
            class Stream:
                def __enter__(self):return self
                def __exit__(self,*_):pass
                def read(self,n):
                    if isinstance(output,BaseException):raise output
                    return output
            r=h.run(smaps_opener=Stream);self.assert_no_trim(h,r);self.assertNotIn('private maps',json.dumps(r))
    def test_byte_cap_and_eof_required(self):
        h=Harness();r=h.run(smaps_opener=lambda:io.BytesIO(b'x'*(m.MAX_SMAPS_BYTES+1)));self.assert_no_trim(h,r)
        self.assertEqual(r['snapshots'][0]['smaps_read']['reason'],'SMAPS_BYTE_BOUND');self.assertFalse(r['snapshots'][0]['smaps_read']['eof'])
    def test_read_call_cap_never_marks_prefix_complete(self):
        h=Harness()
        class OneByte:
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def read(self,n):return b'x'
        r=h.run(smaps_opener=OneByte);self.assert_no_trim(h,r);s=r['snapshots'][0];self.assertEqual(s['smaps_read']['read_calls'],1024);self.assertFalse(s['smaps_read']['eof']);self.assertEqual(s['smaps_read']['reason'],'SMAPS_READ_CALL_BOUND')
    def test_reader_cannot_return_more_than_chunk_request(self):
        h=Harness()
        class TooMuch:
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def read(self,n):return b'x'*(n+1)
        r=h.run(smaps_opener=TooMuch);self.assert_no_trim(h,r);self.assertIn('SMAPS_READ_CHUNK_BOUND',r['snapshots'][0]['reason_codes'])
    def test_read_time_overrun_is_cooperative_no_trim(self):
        h=Harness();h.read_cost=0.6;r=h.run();self.assert_no_trim(h,r);self.assertIn('SMAPS_READ_PARSE_TIME_BOUND',r['snapshots'][0]['reason_codes'])
        self.assertGreater(r['snapshots'][0]['smaps_read']['read_parse_elapsed_ms'],500)
    def test_parse_time_overrun_retains_aggregate_but_no_trim(self):
        h=Harness();h.parse_cost=0.6;r=h.run();self.assert_no_trim(h,r);self.assertIsNotNone(r['snapshots'][0]['smaps']);self.assertEqual(r['snapshots'][0]['status'],'PARTIAL')
    def test_parser_exception_no_raw_payload(self):
        h=Harness()
        def fail(*_,**__):raise RuntimeError('1000-5000 private file')
        r=h.run(parse_smaps=fail);self.assert_no_trim(h,r);self.assertNotIn('private file',json.dumps(r))
    def test_unsafe_cached_admission_cannot_be_overridden(self):
        h=Harness();h.admission_mutator=lambda t,_:t['cached_tail_memory']['descendants']['other'].update(process_count=1);r=h.run();self.assert_no_trim(h,r);self.assertIsNotNone(r['admission'])
    def test_every_prior_admission_sample_known_unsafe_vetoes(self):
        h=Harness()
        def mutate(t,_):
            bad=copy.deepcopy(t['samples'][0]);bad['decision']='RETRY_TRANSIENT_COVERAGE';bad['sample']['status']='PARTIAL';bad['sample']['errors']={'TIME_BUDGET':1};bad['sample']['descendants']['browser_named']['process_count']=1
            t['samples'].insert(0,bad);t['samples'][1]['index']=2;t['selected_sample_index']=2
        h.admission_mutator=mutate;r=h.run();self.assert_no_trim(h,r);self.assertEqual(len(r['admission']['samples']),2)
    def test_transient_safe_retry_trace_allowed(self):
        h=Harness()
        def mutate(t,_):
            first=copy.deepcopy(t['samples'][0]);first.update(decision='RETRY_TRANSIENT_COVERAGE',elapsed_ms=0.);first['completed_at_utc']=first['started_at_utc'];first['sample'].update(status='PARTIAL',errors={'TIME_BUDGET':1})
            t['samples'].insert(0,first);t['samples'][1]['index']=2;t['selected_sample_index']=2
        h.admission_mutator=mutate;r=h.run();self.assertEqual(r['status'],'COMPLETE');self.assertEqual(len(r['admission']['samples']),2)
    def test_source_done_elapsed_overshoot_alone_not_rejected(self):
        h=Harness();h.admission_mutator=lambda t,_:t.update(elapsed_ms=3000.);r=h.run();self.assertEqual(r['status'],'COMPLETE')
    def test_impossible_completed_call_budget_no_trim(self):
        h=Harness();h.admission_mutator=lambda t,_:t['samples'][0].update(elapsed_ms=2501.);r=h.run();self.assert_no_trim(h,r);self.assertIn('ADMISSION_COMPLETED_CALL_BUDGET',r['reason_codes'])
    def test_admission_exception_retained_without_raw_text(self):
        h=Harness();h.admission_error=RuntimeError('private detail');r=h.run();self.assert_no_trim(h,r);self.assertEqual(r['admission']['reason'],'ADMISSION_EXCEPTION');self.assertNotIn('private detail',json.dumps(r))
    def test_pretrim_rss_veto(self):
        h=Harness()
        def mutate(p,x):
            if x.process_calls>=5:p['rss_bytes']=2**30+4096;p['status_read']['fields_bytes']['VmRSS']=p['rss_bytes']
        h.process_mutator=mutate;r=h.run();self.assert_no_trim(h,r);self.assertIn('PRE_TRIM_SELF_RSS_GUARD',r['reason_codes'])
    def test_stale_admission_veto_without_retry(self):
        h=Harness();h.process_cost=lambda x:1.3 if x.process_calls in (5,6) else 0.01;r=h.run();self.assert_no_trim(h,r);self.assertIn('FRESH_ADMISSION_STALE',r['reason_codes']);self.assertGreater(r['admission_return_to_trim_start_ms'],2500)
    def test_exact_freshness_boundary_allowed(self):
        h=Harness();h.read_cost=h.parse_cost=0.;h.process_cost=lambda x:1.25 if x.process_calls in (5,6) else 0.
        r=h.run();self.assertEqual(r['admission_return_to_trim_start_ms'],2500.);self.assertEqual(r['status'],'COMPLETE');self.assertEqual(len(h.trim_calls),1)
    def test_native_overrun_cannot_be_preempted_and_is_never_retried(self):
        h=Harness();h.trim_cost=20.;r=h.run();self.assertEqual(len(h.trim_calls),1);self.assertGreater(r['elapsed_ms'],15000);self.assertIn('TRIAL_TIME_BOUND',r['reason_codes']);self.assertEqual(r['snapshots'][3]['status'],'NOT_CAPTURED')
    def test_post_snapshot_failure_preserves_attempt_and_settled(self):
        h=Harness()
        def mutate(p,x):
            if x.process_calls in (7,8):p['status']='PARTIAL'
        h.process_mutator=mutate;r=h.run();self.assertTrue(r['trim']['attempted']);self.assertEqual(r['snapshots'][3]['status'],'PARTIAL');self.assertEqual(r['snapshots'][4]['status'],'COMPLETE');self.assertEqual(r['status'],'PARTIAL')
    def test_last_process_overrun_keeps_five_quality_separate_from_protocol(self):
        h=Harness();h.process_cost=lambda x:10. if x.process_calls==10 else 0.01;r=h.run()
        self.assertTrue(r['all_five_snapshots_complete']);self.assertFalse(r['protocol_complete']);self.assertEqual(r['status'],'PARTIAL');self.assertIn('TRIAL_TIME_BOUND',r['reason_codes'])
    def test_noop_sleep_return_early_is_not_elapsed_sham(self):
        h=Harness();r=h.run(sleep=lambda _:None);self.assert_no_trim(h,r);self.assertIn('WAIT_RETURNED_EARLY',r['reason_codes'])
    def test_emission_failures_explicit_no_raw_exception(self):
        h=Harness();h.emitter_error=True;r=h.run();self.assertEqual(len(h.trim_calls),1);self.assertEqual(r['emissions']['BEFORE_SHAM']['status'],'UNAVAILABLE');self.assertNotIn('private text',json.dumps(r))
    def test_raw_deltas_and_sham_are_not_causal_correction(self):
        h=Harness();r=h.run();self.assertEqual(r['deltas']['SHAM_AFTER_MINUS_SHAM_BEFORE']['process_rss_bytes'],8192)
        self.assertEqual(r['deltas']['POST_TRIM_MINUS_PRE_TRIM']['process_rss_bytes'],-32768);self.assertEqual(r['deltas']['SETTLED_MINUS_PRE_TRIM']['process_rss_bytes'],-24576)
        self.assertIn('NO_SHAM_SUBTRACTION',r['deltas']['POST_TRIM_MINUS_PRE_TRIM']['scope']);self.assertIsNone(r['conditional_resident_free_bytes'])
    def test_identity_invalid_never_prepares_native(self):
        h=Harness();r=h.run(identity=None);self.assert_no_trim(h,r);self.assertEqual(h.factory_calls,0)
    def test_all_output_is_bounded_primitives_without_raw_maps(self):
        h=Harness();r=h.run();raw=json.dumps(r,allow_nan=False);self.assertLess(len(raw),m.MAX_RESULT_BYTES);self.assertNotIn('1000-5000',raw);self.assertNotIn('[heap]',raw);self.assertFalse(r['raw_smaps_persisted'])
    def test_result_bound_fallback_preserves_consumed_attempt_and_known_return(self):
        h=Harness()
        with patch.object(m,'MAX_RESULT_BYTES',1000):r=h.run()
        self.assertTrue(r['components_omitted']);self.assertEqual(r['status'],'PARTIAL');self.assertTrue(r['trim']['attempted']);self.assertEqual(r['trim']['call_count'],1)
        self.assertEqual(r['trim']['return_value'],1);self.assertEqual(len(h.trim_calls),1);self.assertLess(len(json.dumps(r)),192*1024)
    def test_final_serialization_baseexception_preserves_attempt(self):
        h=Harness();original=json.dumps
        def serial(value,*a,**k):
            if type(value) is dict and value.get('version')==m.VERSION:raise KeyboardInterrupt('private serialization detail')
            return original(value,*a,**k)
        with patch.object(m.json,'dumps',side_effect=serial):r=h.run()
        self.assertTrue(r['trim']['attempted']);self.assertEqual(r['trim']['call_count'],1);self.assertEqual(r['trim']['return_value'],1);self.assertTrue(r['components_omitted'])
        self.assertNotIn('private serialization',json.dumps(r))
    def test_invalid_partial_admission_counts_cannot_be_retried_into_trim(self):
        h=Harness()
        def mutate(t,_):t['cached_tail_memory']['descendants']['other']['process_count']=-1
        h.admission_mutator=mutate;r=h.run();self.assert_no_trim(h,r)
    def test_default_native_factory_binds_size_t_and_int_without_real_calls(self):
        calls=[]
        class Fn:
            def __init__(self,value):self.value=value;self.calls=0
            def __call__(self,*args):self.calls+=1;return self.value
        library=SimpleNamespace(gnu_get_libc_version=Fn(b'2.36'),malloc_trim=Fn(1))
        size=object();pointer=object();cint=object();charp=object()
        fake=SimpleNamespace(c_size_t=size,c_void_p=pointer,c_int=cint,c_char_p=charp,
            sizeof=lambda t:4 if t is cint else 8,CDLL=lambda *a,**k:(calls.append((a,k)) or library))
        with patch.dict(sys.modules,{'ctypes':fake}),patch.object(m.sys,'platform','linux'),patch.object(m.sys,'version_info',(3,11,9)),patch.object(m.sys,'implementation',SimpleNamespace(name='cpython')),patch.object(m.sys,'abiflags','',create=True):
            backend=m._native_factory()
        self.assertEqual(backend['status'],'READY');self.assertEqual(library.malloc_trim.argtypes,[size]);self.assertIs(library.malloc_trim.restype,cint)
        self.assertEqual(library.malloc_trim.calls,0);self.assertEqual(library.gnu_get_libc_version.calls,1);self.assertEqual(calls[0][0],(None,))
    def test_default_native_factory_rejects_bad_abi_before_library_load(self):
        calls=[];cint=object();other=object()
        fake=SimpleNamespace(c_int=cint,c_size_t=other,c_void_p=other,sizeof=lambda t:2 if t is cint else 8,CDLL=lambda *a,**k:calls.append(True))
        with patch.dict(sys.modules,{'ctypes':fake}),patch.object(m.sys,'platform','linux'),patch.object(m.sys,'version_info',(3,11,9)),patch.object(m.sys,'implementation',SimpleNamespace(name='cpython')),patch.object(m.sys,'abiflags','',create=True):
            backend=m._native_factory()
        self.assertEqual(backend['status'],'UNAVAILABLE');self.assertEqual(calls,[])
    def test_import_does_not_open_start_threads_sleep_or_import_native(self):
        source=(HERE/'research_resident_memory.py').read_bytes();original=__import__
        def guarded(name,*a,**k):
            if name in ('ctypes','research_process_memory','research_resident_smaps'):raise AssertionError('import-time active dependency')
            return original(name,*a,**k)
        with patch('builtins.open',side_effect=AssertionError('import I/O')),patch('threading.Thread',side_effect=AssertionError('thread')),patch('time.sleep',side_effect=AssertionError('sleep')),patch('builtins.__import__',side_effect=guarded):
            namespace={'__name__':'passive_import_test'};exec(compile(source,'passive_source','exec'),namespace)
        self.assertTrue(callable(namespace['run_resident_probe']))


if __name__=='__main__':unittest.main()
