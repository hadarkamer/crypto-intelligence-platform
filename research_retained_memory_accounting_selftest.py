"""Small pure fixtures for named graph accounting and fail-open capture."""
import importlib.util
import ast
from collections import OrderedDict, Counter, defaultdict, deque
import hashlib
import json
from pathlib import Path
import pickle
import sys
import threading
import queue
from datetime import datetime, timezone
from types import ModuleType
import unittest
import weakref
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('history_memory_under_test',Path(__file__).with_name('research_retained_memory_accounting.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class _ReferenceRanges(dict):
    __slots__=('__weakref__',)
class _CapturedPackage(dict):
    pass
class Evil:
    calls=0
    def __sizeof__(self):
        Evil.calls+=1
        raise RuntimeError('must not run')

def roots(value=None):
    return {'paper_checkpoint_loaded':{} if value is None else value,'liquidity_checkpoint_loaded':OrderedDict(),
            'sheets_snapshot_cache':{},'sheets_queue_payloads':OrderedDict()}
def run(values=None,**options):
    return m.measure_named_roots(roots() if values is None else values,clock=lambda:0.,yield_fn=lambda:None,**options)

class Tests(unittest.TestCase):
    def test_empty_original_headers(self):
        values=roots();r=run(values)
        self.assertEqual(r['status'],'COMPLETE')
        expected=sum(sys.getsizeof(v) for v in values.values())+sum(sys.getsizeof(v.__dict__) for v in values.values() if type(v) is OrderedDict)
        self.assertEqual(r['covered_union_bytes'],expected)
    def test_known_shared_root(self):
        values=roots();values['sheets_snapshot_cache']=values['paper_checkpoint_loaded'];r=run(values)
        self.assertEqual(r['covered_shared_unique_bytes'],sys.getsizeof(values['sheets_snapshot_cache']))
        self.assertEqual(r['repeated_reachable_bytes'],sys.getsizeof(values['sheets_snapshot_cache']))
    def test_shared_nested_value(self):
        shared=[float('1.234')];values=roots({'a':shared});values['sheets_snapshot_cache']={'b':shared};r=run(values)
        expected=sys.getsizeof(shared)+sys.getsizeof(shared[0])
        self.assertEqual(r['pairwise_overlap_bytes']['paper_checkpoint_loaded|sheets_snapshot_cache'],expected)
    def test_cycle_finite(self):
        item=[];item.append(item);r=run(roots({'x':item}));self.assertTrue(r['complete'])
        self.assertLess(r['covered_unique_objects'],12)
    def test_reference_subclass(self):
        r=run(roots({'ETH':_ReferenceRanges(rows=4)}),reference_ranges_type=_ReferenceRanges)
        self.assertTrue(r['complete']);self.assertEqual(r['roots']['paper_checkpoint_loaded']['type_counts']['reference_ranges'],1)
    def test_package_instance_dict(self):
        item=_CapturedPackage(objects={});item._seal_fingerprint='fingerprint'
        r=run(roots({'package':item}),captured_package_type=_CapturedPackage)
        self.assertTrue(r['complete']);self.assertGreaterEqual(r['roots']['paper_checkpoint_loaded']['type_counts']['dict'],3)
    def test_weak_reference_is_not_followed(self):
        item=_CapturedPackage();item.large=[float(i) for i in range(100)]
        ref=weakref.ref(item);r=run(roots({'weak':ref}))
        self.assertTrue(r['complete']);self.assertNotIn('list',r['roots']['paper_checkpoint_loaded']['type_counts'])
    def test_evil_size_not_called(self):
        Evil.calls=0;r=run(roots({'bad':Evil()}));self.assertEqual(r['status'],'PARTIAL');self.assertEqual(Evil.calls,0)
    def test_fake_known_subclass_rejected(self):
        cls=type('_ReferenceRanges',(dict,),{'__sizeof__':lambda _:1,'__slots__':('__weakref__',)})
        result=run(roots({'ETH':cls()}),reference_ranges_type=cls)
        self.assertEqual(result['status'],'PARTIAL');self.assertIn('UNSAFE_KNOWN_DICT_TYPE',result['reason_codes'])
    def test_unregistered_subclass_partial(self):
        self.assertEqual(run(roots({'x':_ReferenceRanges()}))['status'],'PARTIAL')
    def test_node_bound(self):
        r=run(roots({'x':[float(i) for i in range(20)]}),max_nodes=6)
        self.assertEqual(r['status'],'PARTIAL');self.assertIn('NODE_BOUND',r['reason_codes'])
    def test_workspace_bound(self):
        r=run(roots({'x':1}),max_workspace_bytes=1024)
        self.assertIn(r['status'],('PARTIAL','UNAVAILABLE'));self.assertFalse(r['complete'])
    def test_time_bound(self):
        values=roots({'x':list(range(1000))});ticks=iter([0.,1.,2.,3.])
        r=m.measure_named_roots(values,clock=lambda:next(ticks),yield_fn=lambda:None,max_elapsed_seconds=.5)
        self.assertEqual(r['status'],'PARTIAL');self.assertIn('TIME_BOUND',r['reason_codes'])
    def test_yields(self):
        yields=[];r=m.measure_named_roots(roots({'x':list(range(20))}),clock=lambda:0.,yield_fn=lambda:yields.append(1),yield_every=1)
        self.assertTrue(r['complete']);self.assertGreater(len(yields),20)
    def test_yield_failure_fail_open(self):
        def bad():raise KeyboardInterrupt()
        r=m.measure_named_roots(roots({'x':1}),clock=lambda:0.,yield_fn=bad,yield_every=1)
        self.assertEqual(r['status'],'UNAVAILABLE')
    def test_root_mutation_invalid(self):
        values=roots({'x':list(range(10))})
        def mutate():values['paper_checkpoint_loaded']['new']=1
        r=m.measure_named_roots(values,clock=lambda:0.,yield_fn=mutate,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT');self.assertFalse(r['measurement_valid'])
    def test_nested_size_mutation_invalid(self):
        child=list(range(10));values=roots({'x':child});done=[]
        def mutate():
            if not done:child.append(42);done.append(1)
        r=m.measure_named_roots(values,clock=lambda:0.,yield_fn=mutate,yield_every=5)
        self.assertEqual(r['status'],'CONCURRENT')
    def test_depth_bound(self):
        value=[]
        for _ in range(150):value=[value]
        r=run(roots({'x':value}));self.assertIn('DEPTH_BOUND',r['reason_codes'])
    def test_edge_bound(self):
        with patch.object(m,'MAX_EDGE_VISITS',20):r=run(roots({'x':[0]*50}))
        self.assertIn('EDGE_BOUND',r['reason_codes'])
    def test_json_only_no_addresses(self):
        item=_ReferenceRanges(rows=2);r=run(roots({'ETH':item}),reference_ranges_type=_ReferenceRanges)
        text=json.dumps(r,allow_nan=False);self.assertNotIn(str(id(item)),text);self.assertNotIn('_ReferenceRanges',text)
    def test_no_graph_retained(self):
        item=_ReferenceRanges(rows=2);ref=weakref.ref(item);values=roots({'ETH':item})
        r=run(values,reference_ranges_type=_ReferenceRanges);values.clear();del item
        self.assertIsNone(ref());self.assertIsInstance(r,dict)
    def test_invalid_options(self):
        for options in ({'max_nodes':True},{'max_elapsed_seconds':float('nan')},{'max_workspace_bytes':True},{'yield_every':0}):
            self.assertEqual(run(**options)['status'],'UNAVAILABLE')
    def test_root_count_bound(self):
        r=run(roots({str(i):i for i in range(50001)}));self.assertEqual(r['status'],'UNAVAILABLE')
    def test_unrelated_flow_change_keeps_history_valid(self):
        values=roots({'ETH':_ReferenceRanges(rows=4)})
        def mutate():values['sheets_snapshot_cache']['new']=1
        r=m.measure_named_roots(values,reference_ranges_type=_ReferenceRanges,clock=lambda:0.,yield_fn=mutate,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT');self.assertFalse(r['measurement_valid'])
        self.assertTrue(r['roots']['paper_checkpoint_loaded']['complete'])
        self.assertTrue(r['roots']['paper_checkpoint_loaded']['measurement_valid'])
        self.assertFalse(r['roots']['sheets_snapshot_cache']['measurement_valid'])
    def test_first_seen_object_pinned_against_nested_replacement(self):
        item=_ReferenceRanges();reference=weakref.ref(item);nested=[item,1.234];del item
        values=roots({'outer':nested});calls=[];alive=[]
        def mutate():
            calls.append(1)
            if len(calls)==5:
                nested[0]=None;alive.append(reference() is not None)
        r=m.measure_named_roots(values,reference_ranges_type=_ReferenceRanges,clock=lambda:0.,yield_fn=mutate,yield_every=1)
        self.assertEqual(alive,[True]);self.assertIsNone(reference())
        self.assertTrue(r['temporary_graph_pins_released_on_return'])
    def test_shared_mutation_invalidates_earlier_root(self):
        shared=[1.234,2.345];values=roots({'a':shared});values['sheets_snapshot_cache']={'b':shared};calls=[]
        original=m._children
        def mutate(value,kind):
            if value is shared:
                calls.append(1)
                if len(calls)==2:shared.append(3.456)
            yield from original(value,kind)
        with patch.object(m,'_children',mutate):r=run(values)
        self.assertEqual(r['status'],'CONCURRENT')
        self.assertFalse(r['roots']['paper_checkpoint_loaded']['measurement_valid'])
        self.assertFalse(r['roots']['sheets_snapshot_cache']['measurement_valid'])

    def exact_producer_fixture(self):
        """Only inert class/guard AST, never the producer imports or startup."""
        module=ModuleType('history_accounting_exact_producer_fixture')
        module.__dict__.update(hashlib=hashlib,pickle=pickle)
        root=Path(__file__).parent
        for filename,names in (
            ('coinglass_history_backfill.py',('_ReferenceRanges','_local_input_guard')),
            ('research_consumed_inputs.py',('_CapturedPackage',))):
            source=ast.parse((root/filename).read_text(encoding='utf-8'))
            nodes=[node for node in source.body if isinstance(node,(ast.ClassDef,ast.FunctionDef)) and node.name in names]
            self.assertEqual({node.name for node in nodes},set(names))
            exec(compile(ast.Module(body=nodes,type_ignores=[]),filename,'exec'),module.__dict__)
        return module

    def test_exact_producer_pickle_lifecycle_admitted(self):
        producer=self.exact_producer_fixture()
        history=producer._ReferenceRanges(rows=3,windows={'30m':{'price_composition_samples':[(1.25,.5)],'oi_composition_samples':[(2.5,.5)]}})
        package=producer._CapturedPackage(objects={});package._seal_fingerprint='seal'
        with patch.dict(sys.modules,{producer.__name__:producer}):
            for item in (history,package):
                cls=type(item)
                self.assertTrue(m._known_dict_type(cls,cls.__name__))
                self.assertEqual(len(producer._local_input_guard(item)),32)
                self.assertIs(type(cls.__dict__['__slotnames__']),list)
                self.assertEqual(cls.__dict__['__slotnames__'],[])
                self.assertTrue(m._known_dict_type(cls,cls.__name__))
            values=roots({'ETH':history});values['liquidity_checkpoint_loaded']['receipt']=package
            result=run(values,reference_ranges_type=producer._ReferenceRanges,captured_package_type=producer._CapturedPackage)
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['roots']['paper_checkpoint_loaded']['type_counts']['reference_ranges'],1)
        self.assertEqual(result['roots']['liquidity_checkpoint_loaded']['type_counts']['captured_package'],1)
        self.assertEqual(result['version'],'retained-supported-memory-v1')


    def test_unused_unsafe_package_does_not_veto_history(self):
        calls=[]
        unsafe=type('_CapturedPackage',(dict,),{'__sizeof__':lambda self:calls.append(1) or 1})
        result=run(roots({'ETH':_ReferenceRanges(rows=4)}),reference_ranges_type=_ReferenceRanges,captured_package_type=unsafe)
        self.assertEqual(result['status'],'COMPLETE');self.assertEqual(calls,[])

    def test_encountered_unsafe_package_only_incompletes_own_root(self):
        calls=[]
        unsafe=type('_CapturedPackage',(dict,),{'__sizeof__':lambda self:calls.append(1) or 1})
        values=roots({'ETH':_ReferenceRanges(rows=4)});values['sheets_queue_payloads']['bad']=unsafe()
        result=run(values,reference_ranges_type=_ReferenceRanges,captured_package_type=unsafe)
        self.assertEqual(result['status'],'PARTIAL');self.assertFalse(result['complete']);self.assertEqual(calls,[])
        self.assertTrue(result['roots']['paper_checkpoint_loaded']['complete'])
        self.assertTrue(result['roots']['paper_checkpoint_loaded']['measurement_valid'])
        self.assertFalse(result['roots']['sheets_queue_payloads']['complete'])
        self.assertIn('UNSAFE_KNOWN_DICT_TYPE',result['reason_codes'])

    def test_slotnames_cache_is_exact_empty_list_only(self):
        class ListSubclass(list):pass
        for cached in (['payload'],(),ListSubclass(),None,0):
            cls=type('_CapturedPackage',(dict,),{'__slotnames__':cached})
            self.assertFalse(m._known_dict_type(cls,'_CapturedPackage'))
            result=run(roots({'item':cls()}),captured_package_type=cls)
            self.assertFalse(result['roots']['paper_checkpoint_loaded']['complete'])

    def test_empty_slot_cache_does_not_hide_other_hooks(self):
        for attributes in ({'__sizeof__':lambda _:1},{'payload':[]},{'__iter__':lambda _:iter(())}):
            cls=type('_CapturedPackage',(dict,),{'__slotnames__':[],**attributes})
            self.assertFalse(m._known_dict_type(cls,'_CapturedPackage'))

    def test_foreign_metaclass_hash_is_not_called(self):
        calls=[]
        class Meta(type):
            def __hash__(cls):calls.append('hash');return 1
            def __eq__(cls,other):calls.append('eq');return False
        class Foreign(metaclass=Meta):
            def __sizeof__(self):calls.append('sizeof');return 1
        result=run(roots({'foreign':Foreign()}))
        self.assertEqual(result['status'],'PARTIAL');self.assertIn('UNSUPPORTED_TYPE',result['reason_codes'])
        self.assertEqual(calls,[])

    def test_foreign_root_metaclass_comparison_is_not_called(self):
        calls=[]
        class Meta(type):
            def __eq__(cls,other):calls.append('eq');return False
            def __hash__(cls):calls.append('hash');return 1
        class Foreign(dict,metaclass=Meta):
            def __len__(self):calls.append('len');return 0
        result=run(roots(Foreign()))
        self.assertEqual(result['status'],'UNAVAILABLE');self.assertEqual(calls,[])


    def test_known_class_descriptors_must_be_native_and_owned(self):
        calls=[]
        def trap(self):calls.append('descriptor');return {}
        for field in ('__dict__','__weakref__'):
            cls=type('_CapturedPackage',(dict,),{field:property(trap)})
            self.assertFalse(m._known_dict_type(cls,'_CapturedPackage'))
            result=run(roots({'item':cls()}),captured_package_type=cls)
            self.assertFalse(result['roots']['paper_checkpoint_loaded']['complete'])
        cls=type('_CapturedPackage',(dict,),{'__dict__':_CapturedPackage.__dict__['__dict__']})
        self.assertFalse(m._known_dict_type(cls,'_CapturedPackage'))
        self.assertEqual(calls,[])

    def test_slot_shape_never_compares_foreign_tuple(self):
        calls=[]
        class ForeignTuple(tuple):
            def __eq__(self,other):calls.append('eq');return True
        cls=type('_ReferenceRanges',(dict,),{'__slots__':('__weakref__',)})
        cls.__slots__=ForeignTuple(('__weakref__',))
        self.assertFalse(m._known_dict_type(cls,'_ReferenceRanges'));self.assertEqual(calls,[])


    def test_all32_masks_pairs_and_shared_conservation(self):
        shared={};r=run({name:shared for name in m.ROOT_NAMES});size=sys.getsizeof(shared)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(len(r['roots']),32)
        self.assertEqual(r['covered_union_bytes'],size);self.assertEqual(r['repeated_reachable_bytes'],31*size)
        self.assertEqual(len(r['pairwise_overlap_bytes']),496);self.assertTrue(all(v==size for v in r['pairwise_overlap_bytes'].values()))
        self.assertGreaterEqual(r['workspace_mask_bytes_per_unique_node'],sys.getsizeof(2**32-1))
        self.assertEqual(r['workspace_masks_counted_bytes'],r['workspace_mask_bytes_per_unique_node']*r['covered_unique_objects'])

    def test_unknown_root_name_rejected(self):
        self.assertEqual(run({'not_declared':{}})['status'],'UNAVAILABLE')

    def test_direct_and_nested_counter_attributes_counted(self):
        counter=Counter(a=2);counter.extra=[float('3.456')]
        for values in ({'reconciler_seen':counter},{'paper_checkpoint_loaded':{'counter':counter}}):
            r=run(values);self.assertEqual(r['status'],'COMPLETE')
            stats=next(iter(r['roots'].values()));self.assertEqual(stats['type_counts']['counter'],1)
            self.assertEqual(stats['type_counts']['list'],1);self.assertIn('float',stats['type_counts'])

    def test_direct_and_nested_ordered_attributes_counted(self):
        ordered=OrderedDict(a=2);ordered.extra=[float('3.456')]
        for values in ({'past_price_lru':ordered},{'paper_checkpoint_loaded':{'ordered':ordered}}):
            r=run(values);self.assertEqual(r['status'],'COMPLETE')
            stats=next(iter(r['roots'].values()));self.assertEqual(stats['type_counts']['ordered_dict'],1)
            self.assertEqual(stats['type_counts']['list'],1)

    def test_exact_deque_and_defaultdict_without_factory_callback(self):
        calls=[]
        def factory():calls.append(1);raise RuntimeError('not called')
        values={'sheets_queue_payloads':deque([1.25]),'ai_conversation_messages':defaultdict(factory,one=deque([{'content':'PRIVATE_TEST_VALUE'}]))}
        r=run(values);self.assertEqual(r['status'],'COMPLETE');self.assertEqual(calls,[])
        self.assertNotIn('PRIVATE_TEST_VALUE',json.dumps(r))

    def test_container_subclass_never_sized(self):
        calls=[]
        class CustomCounter(Counter):
            def __sizeof__(self):calls.append(1);return 1
        r=run({'paper_checkpoint_loaded':{'bad':CustomCounter()}})
        self.assertEqual(r['status'],'PARTIAL');self.assertEqual(calls,[])

    def test_datetime_leaf_does_not_call_timezone(self):
        from datetime import tzinfo
        calls=[]
        class Zone(tzinfo):
            def utcoffset(self,dt):calls.append(1);raise RuntimeError('not called')
        r=run({'paper_checkpoint_loaded':{'time':datetime(2026,1,1,tzinfo=Zone())}})
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(calls,[])
        self.assertEqual(r['roots']['paper_checkpoint_loaded']['type_counts']['datetime_leaf'],1)
        self.assertIn('tzinfo',r['native_leaf_scope'])

    def test_exact_research_event_ast_fields_without_to_dict(self):
        from dataclasses import dataclass,asdict
        from typing import Any,Dict,List,Optional
        source=ast.parse(Path(__file__).with_name('research_event_capture.py').read_text(encoding='utf-8'))
        node=next(n for n in source.body if isinstance(n,ast.ClassDef) and n.name=='ResearchEvent')
        module=ModuleType('retained_exact_event_fixture');module.__dict__.update(dataclass=dataclass,asdict=asdict,Any=Any,Dict=Dict,List=List,Optional=Optional)
        with patch.dict(sys.modules,{module.__name__:module}):
            exec(compile(ast.Module(body=[node],type_ignores=[]),'event_ast','exec'),module.__dict__)
            cls=module.ResearchEvent;fields={name:None for name in cls.__dataclass_fields__};fields.update(engine_snapshot={'nested':[1.25]},categories=[])
            event=cls(**fields);r=m.measure_named_roots({'event_sink_events':OrderedDict(key=event)},research_event_type=cls,clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['roots']['event_sink_events']['type_counts']['research_event'],1)
        self.assertEqual(r['roots']['event_sink_events']['type_counts']['list'],2)

    def test_revalidation_workspace_reserved_before_snapshot_allocation(self):
        values={'paper_checkpoint_loaded':{str(i):i for i in range(5000)}}
        with patch.object(m,'_shape',side_effect=AssertionError('allocation must be rejected first')):
            result=run(values,max_workspace_bytes=3*1024*1024)
        self.assertEqual(result['reason_codes'],['SNAPSHOT_WORKSPACE_BOUND'])
        result=run({'paper_checkpoint_loaded':{'small':1}})
        self.assertEqual(result['workspace_revalidation_reserve_bytes'],2*(128+4096))

    def test_spoofed_research_event_slots_and_destructor_rejected(self):
        slotted=type('ResearchEvent',(object,),{'__dataclass_fields__':{},'__slots__':('payload','__dict__')})
        self.assertFalse(m._safe_event_type(slotted))
        ordinary=type('ResearchEvent',(object,),{'__dataclass_fields__':{},'__del__':lambda self:None})
        self.assertFalse(m._safe_event_type(ordinary))

    def test_grown_root_recheck_does_not_allocate_large_second_snapshot(self):
        value={'first':1};values={'paper_checkpoint_loaded':value};grew=[];calls=[];original=m._shape
        def grow():
            if not grew:
                grew.append(True);value.update({str(i):i for i in range(5000)})
        def observe(value,*args,**kwargs):calls.append(len(value));return original(value,*args,**kwargs)
        with patch.object(m,'_shape',observe):result=m.measure_named_roots(values,clock=lambda:0.,yield_fn=grow,yield_every=1)
        self.assertEqual(result['status'],'CONCURRENT')
        self.assertEqual(calls,[1])
        with self.assertRaises(ValueError):m._shape(value,expected_count=1)

    def test_supplied_snapshots_reject_before_any_walk_or_revalidation(self):
        value={};snapshots={'paper_checkpoint_loaded':m._shape(value)}
        with patch.object(m,'_shape',side_effect=AssertionError('must not revalidate')),patch.object(m,'_children',side_effect=AssertionError('must not walk')):
            result=m.measure_named_roots({'paper_checkpoint_loaded':value},_snapshots=snapshots,max_workspace_bytes=1024)
        self.assertEqual(result['status'],'UNAVAILABLE');self.assertEqual(result['reason_codes'],['SNAPSHOT_WORKSPACE_BOUND'])

    def fixture_modules(self):
        def box(**values):
            class Box:pass
            value=Box();value.__dict__.update(values);return value
        modules={}
        def module(name,**values):
            value=ModuleType(name);value.__dict__.update(values);modules[name]=value;return value
        for name in ('research_forward_paper_worker','research_liquidity_forward_worker'):
            module(name,WORKER=box(_store=box(_loaded={'state':{'first_seen':{}}})))
        module('google_sheets_sync',_SNAPSHOT_CACHE=OrderedDict(),_SNAPSHOT_LOCK=threading.Lock(),_QUEUE=queue.Queue())
        module('research_snapshot_sync_worker',WORKER=box(_sheet_reconciler=box(expected={},seen=Counter(),unexpected=Counter())))
        module('research_past_price_features_worker',_CACHE=OrderedDict())
        module('research_event_runtime',SINK=box(_events=OrderedDict()))
        module('research_event_store',WRITER=box(queue=box(_queue=deque())))
        for name in ('research_consumed_inputs','research_alert_decision_store','research_doge_sequence_capture','research_combined_liquidity_shadow'):
            module(name,_QUEUE=queue.Queue())
        module('experimental_hyperliquid_source',_SHARED_SOURCE=box(_rows={},_locks={coin:threading.Lock() for coin in ('BTC','ETH','SOL','HYPE','DOGE','XRP')}))
        for name in ('hype_row71205_experimental_worker','u21_experimental_worker','sol_g65_experimental_worker','xrp_r2732_experimental_worker','sol_proximity_experimental_worker'):
            module(name,WORKER=box(cache=box(rows={}),legacy_cache=box(rows={})))
        modules['sol_proximity_experimental_worker'].ADDITIONAL_WORKERS={coin:box(cache=box(rows={}),legacy_cache=box(rows={})) for coin in ('HYPE','DOGE','XRP','ETH')}
        module('ai_agent',AGENT=box(memory=box(_messages=defaultdict(list))))
        return modules

    def test_all32_actual_adapter_paths(self):
        with patch.dict(sys.modules,self.fixture_modules()):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(len(r['roots']),32)
        self.assertTrue(r['declared_coverage_complete']);self.assertTrue(r['declared_union_complete_valid'])

    def test_missing_roots_not_synthetic_empty(self):
        modules=self.fixture_modules();modules['google_sheets_sync']=None
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertNotIn('sheets_snapshot_cache',r['roots']);self.assertFalse(r['declared_coverage_complete'])
        self.assertEqual(r['root_presence']['sheets_snapshot_cache']['status'],'NOT_LOADED')

    def test_busy_lock_only_excludes_its_root(self):
        modules=self.fixture_modules();lock=modules['google_sheets_sync']._SNAPSHOT_LOCK;lock.acquire()
        try:
            with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        finally:lock.release()
        self.assertEqual(r['root_presence']['sheets_snapshot_cache']['status'],'LOCK_UNAVAILABLE')
        self.assertEqual(len(r['roots']),31);self.assertFalse(r['declared_union_complete_valid'])

    def test_queue_locks_released_before_walk(self):
        modules=self.fixture_modules();observed=[]
        def observe():
            lock=modules['google_sheets_sync']._QUEUE.mutex;ok=lock.acquire(False);observed.append(ok)
            if ok:lock.release()
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=observe,yield_every=1)
        self.assertTrue(r['complete']);self.assertTrue(observed);self.assertTrue(all(observed))

    def test_adapter_property_not_called(self):
        modules=self.fixture_modules();calls=[]
        class BadWorker:
            @property
            def _store(self):calls.append(1);return None
        modules['research_forward_paper_worker'].WORKER=BadWorker()
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(calls,[]);self.assertEqual(r['root_presence']['paper_checkpoint_loaded']['status'],'NOT_READY')

    def test_adapter_replacement_invalidation(self):
        modules=self.fixture_modules()
        def change():modules['u21_experimental_worker'].WORKER.cache.rows={}
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=change,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT');self.assertFalse(r['roots']['u21_price_rows']['measurement_valid'])

    def test_no_available_roots_is_unavailable(self):
        modules={name:None for name in self.fixture_modules()}
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure()
        self.assertEqual(r['status'],'UNAVAILABLE');self.assertEqual(r['available_root_names'],[])
        self.assertNotIn('covered_union_bytes',r)

if __name__=='__main__':unittest.main()
