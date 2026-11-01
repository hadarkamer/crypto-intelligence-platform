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

def fixture_root():
    here=Path(__file__).parent
    return here/'fixtures' if (here/'fixtures'/'coinglass_history_backfill.py').is_file() else here


spec=importlib.util.spec_from_file_location('history_memory_under_test',Path(__file__).with_name('research_current_history_memory.py'))
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
    return {'history_reference_cache':{} if value is None else value,'history_input_receipts':OrderedDict(),
            'flow_cache':{},'flow_input_receipts':OrderedDict()}
def run(values=None,**options):
    return m.measure_named_roots(roots() if values is None else values,clock=lambda:0.,yield_fn=lambda:None,**options)

class Tests(unittest.TestCase):
    def test_empty_original_headers(self):
        values=roots();r=run(values)
        self.assertEqual(r['status'],'COMPLETE')
        expected=sum(sys.getsizeof(v) for v in values.values())+sum(sys.getsizeof(v.__dict__) for v in values.values() if type(v) is OrderedDict)
        self.assertEqual(r['covered_union_bytes'],expected)
    def test_known_shared_root(self):
        values=roots();values['flow_cache']=values['history_reference_cache'];r=run(values)
        self.assertEqual(r['covered_shared_unique_bytes'],sys.getsizeof(values['flow_cache']))
        self.assertEqual(r['repeated_reachable_bytes'],sys.getsizeof(values['flow_cache']))
    def test_shared_nested_value(self):
        shared=[float('1.234')];values=roots({'a':shared});values['flow_cache']={'b':shared};r=run(values)
        expected=sys.getsizeof(shared)+sys.getsizeof(shared[0])
        self.assertEqual(r['pairwise_overlap_bytes']['history_reference_cache|flow_cache'],expected)
    def test_cycle_finite(self):
        item=[];item.append(item);r=run(roots({'x':item}));self.assertTrue(r['complete'])
        self.assertLess(r['covered_unique_objects'],12)
    def test_reference_subclass(self):
        r=run(roots({'ETH':_ReferenceRanges(rows=4)}),reference_ranges_type=_ReferenceRanges)
        self.assertTrue(r['complete']);self.assertEqual(r['roots']['history_reference_cache']['type_counts']['reference_ranges'],1)
    def test_package_instance_dict(self):
        item=_CapturedPackage(objects={});item._seal_fingerprint='fingerprint'
        r=run(roots({'package':item}),captured_package_type=_CapturedPackage)
        self.assertTrue(r['complete']);self.assertGreaterEqual(r['roots']['history_reference_cache']['type_counts']['dict'],3)
    def test_weak_reference_is_not_followed(self):
        item=_CapturedPackage();item.large=[float(i) for i in range(100)]
        ref=weakref.ref(item);r=run(roots({'weak':ref}))
        self.assertTrue(r['complete']);self.assertNotIn('list',r['roots']['history_reference_cache']['type_counts'])
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
        def mutate():values['history_reference_cache']['new']=1
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
        r=run(roots({str(i):i for i in range(65)}));self.assertEqual(r['status'],'UNAVAILABLE')
    def test_unrelated_flow_change_keeps_history_valid(self):
        values=roots({'ETH':_ReferenceRanges(rows=4)})
        def mutate():values['flow_cache']['new']=1
        r=m.measure_named_roots(values,reference_ranges_type=_ReferenceRanges,clock=lambda:0.,yield_fn=mutate,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT');self.assertFalse(r['measurement_valid'])
        self.assertTrue(r['roots']['history_reference_cache']['complete'])
        self.assertTrue(r['roots']['history_reference_cache']['measurement_valid'])
        self.assertFalse(r['roots']['flow_cache']['measurement_valid'])
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
        shared=[1.234,2.345];values=roots({'a':shared});values['flow_cache']={'b':shared};calls=[]
        original=m._children
        def mutate(value,kind):
            if value is shared:
                calls.append(1)
                if len(calls)==2:shared.append(3.456)
            yield from original(value,kind)
        with patch.object(m,'_children',mutate):r=run(values)
        self.assertEqual(r['status'],'CONCURRENT')
        self.assertFalse(r['roots']['history_reference_cache']['measurement_valid'])
        self.assertFalse(r['roots']['flow_cache']['measurement_valid'])

    def exact_producer_fixture(self):
        """Only inert class/guard AST, never the producer imports or startup."""
        module=ModuleType('history_accounting_exact_producer_fixture')
        module.__dict__.update(hashlib=hashlib,pickle=pickle)
        root=fixture_root()
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
            values=roots({'ETH':history});values['history_input_receipts']['receipt']=package
            result=run(values,reference_ranges_type=producer._ReferenceRanges,captured_package_type=producer._CapturedPackage)
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['roots']['history_reference_cache']['type_counts']['reference_ranges'],1)
        self.assertEqual(result['roots']['history_input_receipts']['type_counts']['captured_package'],1)
        self.assertEqual(result['version'],'current-history-supported-memory-pooled-v1')


    def test_unused_unsafe_package_does_not_veto_history(self):
        calls=[]
        unsafe=type('_CapturedPackage',(dict,),{'__sizeof__':lambda self:calls.append(1) or 1})
        result=run(roots({'ETH':_ReferenceRanges(rows=4)}),reference_ranges_type=_ReferenceRanges,captured_package_type=unsafe)
        self.assertEqual(result['status'],'COMPLETE');self.assertEqual(calls,[])

    def test_encountered_unsafe_package_only_incompletes_own_root(self):
        calls=[]
        unsafe=type('_CapturedPackage',(dict,),{'__sizeof__':lambda self:calls.append(1) or 1})
        values=roots({'ETH':_ReferenceRanges(rows=4)});values['flow_input_receipts']['bad']=unsafe()
        result=run(values,reference_ranges_type=_ReferenceRanges,captured_package_type=unsafe)
        self.assertEqual(result['status'],'PARTIAL');self.assertFalse(result['complete']);self.assertEqual(calls,[])
        self.assertTrue(result['roots']['history_reference_cache']['complete'])
        self.assertTrue(result['roots']['history_reference_cache']['measurement_valid'])
        self.assertFalse(result['roots']['flow_input_receipts']['complete'])
        self.assertIn('UNSAFE_KNOWN_DICT_TYPE',result['reason_codes'])

    def test_slotnames_cache_is_exact_empty_list_only(self):
        class ListSubclass(list):pass
        for cached in (['payload'],(),ListSubclass(),None,0):
            cls=type('_CapturedPackage',(dict,),{'__slotnames__':cached})
            self.assertFalse(m._known_dict_type(cls,'_CapturedPackage'))
            result=run(roots({'item':cls()}),captured_package_type=cls)
            self.assertFalse(result['roots']['history_reference_cache']['complete'])

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
            self.assertFalse(result['roots']['history_reference_cache']['complete'])
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


    def test_all4_masks_pairs_and_shared_conservation(self):
        shared={};r=run({name:shared for name in m.ROOT_NAMES});size=sys.getsizeof(shared)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(len(r['roots']),4)
        self.assertEqual(r['covered_union_bytes'],size);self.assertEqual(r['repeated_reachable_bytes'],3*size)
        self.assertEqual(len(r['pairwise_overlap_bytes']),6);self.assertTrue(all(v==size for v in r['pairwise_overlap_bytes'].values()))
        self.assertEqual(r['workspace_owner_mask_mode'],m.OWNER_MASK_MODE)
        self.assertEqual(r['workspace_mask_pool_count'],7)
        self.assertLessEqual(r['workspace_mask_pool_count'],16)

    def test_unknown_root_name_rejected(self):
        self.assertEqual(run({'not_declared':{}})['status'],'UNAVAILABLE')

    def test_root_counter_rejected_nested_counter_attributes_counted(self):
        counter=Counter(a=2);counter.extra=[float('3.456')]
        self.assertEqual(run({'history_input_receipts':counter})['status'],'UNAVAILABLE')
        r=run({'history_reference_cache':{'counter':counter}})
        self.assertEqual(r['status'],'COMPLETE');stats=r['roots']['history_reference_cache']
        self.assertEqual(stats['type_counts']['counter'],1);self.assertEqual(stats['type_counts']['list'],1);self.assertIn('float',stats['type_counts'])

    def test_direct_and_nested_ordered_attributes_counted(self):
        ordered=OrderedDict(a=2);ordered.extra=[float('3.456')]
        for values in ({'flow_cache':ordered},{'history_reference_cache':{'ordered':ordered}}):
            r=run(values);self.assertEqual(r['status'],'COMPLETE')
            stats=next(iter(r['roots'].values()));self.assertEqual(stats['type_counts']['ordered_dict'],1)
            self.assertEqual(stats['type_counts']['list'],1)

    def test_nested_exact_deque_and_defaultdict_without_factory_callback(self):
        calls=[]
        def factory():calls.append(1);raise RuntimeError('not called')
        values={'history_reference_cache':{'queue':deque([1.25])},'flow_cache':{'messages':defaultdict(factory,one=deque([{'content':'PRIVATE_TEST_VALUE'}]))}}
        r=run(values);self.assertEqual(r['status'],'COMPLETE');self.assertEqual(calls,[])
        self.assertNotIn('PRIVATE_TEST_VALUE',json.dumps(r))

    def test_container_subclass_never_sized(self):
        calls=[]
        class CustomCounter(Counter):
            def __sizeof__(self):calls.append(1);return 1
        r=run({'history_reference_cache':{'bad':CustomCounter()}})
        self.assertEqual(r['status'],'PARTIAL');self.assertEqual(calls,[])

    def test_datetime_leaf_does_not_call_timezone(self):
        from datetime import tzinfo
        calls=[]
        class Zone(tzinfo):
            def utcoffset(self,dt):calls.append(1);raise RuntimeError('not called')
        r=run({'history_reference_cache':{'time':datetime(2026,1,1,tzinfo=Zone())}})
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(calls,[])
        self.assertEqual(r['roots']['history_reference_cache']['type_counts']['datetime_leaf'],1)
        self.assertIn('tzinfo',r['native_leaf_scope'])

    def test_exact_research_event_ast_fields_without_to_dict(self):
        from dataclasses import dataclass,asdict
        from typing import Any,Dict,List,Optional
        source=ast.parse((fixture_root()/'research_event_capture.py').read_text(encoding='utf-8'))
        node=next(n for n in source.body if isinstance(n,ast.ClassDef) and n.name=='ResearchEvent')
        module=ModuleType('retained_exact_event_fixture');module.__dict__.update(dataclass=dataclass,asdict=asdict,Any=Any,Dict=Dict,List=List,Optional=Optional)
        with patch.dict(sys.modules,{module.__name__:module}):
            exec(compile(ast.Module(body=[node],type_ignores=[]),'event_ast','exec'),module.__dict__)
            cls=module.ResearchEvent;fields={name:None for name in cls.__dataclass_fields__};fields.update(engine_snapshot={'nested':[1.25]},categories=[])
            event=cls(**fields);r=m.measure_named_roots({'flow_cache':OrderedDict(key=event)},research_event_type=cls,clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['roots']['flow_cache']['type_counts']['research_event'],1)
        self.assertEqual(r['roots']['flow_cache']['type_counts']['list'],2)

    def test_revalidation_workspace_reserved_before_snapshot_allocation(self):
        values={'history_reference_cache':{str(i):i for i in range(64)}}
        with patch.object(m,'_shape',side_effect=AssertionError('allocation must be rejected first')):
            result=run(values,max_workspace_bytes=2*1024*1024+10000)
        self.assertEqual(result['reason_codes'],['SNAPSHOT_WORKSPACE_BOUND'])
        result=run({'history_reference_cache':{'small':1}})
        self.assertEqual(result['workspace_revalidation_reserve_bytes'],2*(128+4096))

    def test_spoofed_research_event_slots_and_destructor_rejected(self):
        slotted=type('ResearchEvent',(object,),{'__dataclass_fields__':{},'__slots__':('payload','__dict__')})
        self.assertFalse(m._safe_event_type(slotted))
        ordinary=type('ResearchEvent',(object,),{'__dataclass_fields__':{},'__del__':lambda self:None})
        self.assertFalse(m._safe_event_type(ordinary))

    def test_grown_root_recheck_does_not_allocate_large_second_snapshot(self):
        value={'first':1};values={'history_reference_cache':value};grew=[];calls=[];original=m._shape
        def grow():
            if not grew:
                grew.append(True);value.update({str(i):i for i in range(5000)})
        def observe(value,*args,**kwargs):calls.append(len(value));return original(value,*args,**kwargs)
        with patch.object(m,'_shape',observe):result=m.measure_named_roots(values,clock=lambda:0.,yield_fn=grow,yield_every=1)
        self.assertEqual(result['status'],'CONCURRENT')
        self.assertEqual(calls,[1])
        with self.assertRaises(ValueError):m._shape(value,expected_count=1)

    def test_supplied_snapshots_reject_before_any_walk_or_revalidation(self):
        value={};snapshots={'history_reference_cache':m._shape(value)}
        with patch.object(m,'_shape',side_effect=AssertionError('must not revalidate')),patch.object(m,'_children',side_effect=AssertionError('must not walk')):
            result=m.measure_named_roots({'history_reference_cache':value},_snapshots=snapshots,max_workspace_bytes=1024)
        self.assertEqual(result['status'],'UNAVAILABLE');self.assertEqual(result['reason_codes'],['SNAPSHOT_WORKSPACE_BOUND'])

    def fixture_modules(self):
        modules={}
        def module(name,**values):
            value=ModuleType(name);value.__dict__.update(values);modules[name]=value;return value
        module('coinglass_history_backfill',_REFERENCE_CACHE={},_REFERENCE_INPUT_RECEIPTS=OrderedDict(),_REFERENCE_INPUT_LOCK=threading.Lock(),_ReferenceRanges=_ReferenceRanges)
        module('market_confidence_engine',_FLOW_CACHE={},_FLOW_INPUT_RECEIPTS=OrderedDict(),_FLOW_INPUT_LOCK=threading.Lock())
        module('research_consumed_inputs',_CapturedPackage=_CapturedPackage)
        return modules

    def test_all4_actual_adapter_paths(self):
        with patch.dict(sys.modules,self.fixture_modules()):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(len(r['roots']),4)
        self.assertTrue(r['declared_coverage_complete']);self.assertTrue(r['declared_union_complete_valid'])

    def test_missing_roots_not_synthetic_empty(self):
        modules=self.fixture_modules();modules['market_confidence_engine']=None
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertNotIn('flow_cache',r['roots']);self.assertFalse(r['declared_coverage_complete'])
        self.assertEqual(r['root_presence']['flow_cache']['status'],'NOT_LOADED')

    def test_busy_lock_only_excludes_its_root(self):
        modules=self.fixture_modules();lock=modules['market_confidence_engine']._FLOW_INPUT_LOCK;lock.acquire()
        try:
            with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        finally:lock.release()
        self.assertEqual(r['root_presence']['flow_input_receipts']['status'],'LOCK_UNAVAILABLE')
        self.assertEqual(len(r['roots']),3);self.assertFalse(r['declared_union_complete_valid'])

    def test_receipt_locks_released_before_walk(self):
        modules=self.fixture_modules();observed=[]
        def observe():
            lock=modules['market_confidence_engine']._FLOW_INPUT_LOCK;ok=lock.acquire(False);observed.append(ok)
            if ok:lock.release()
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=observe,yield_every=1)
        self.assertTrue(r['complete']);self.assertTrue(observed);self.assertTrue(all(observed))

    def test_adapter_property_not_called(self):
        modules=self.fixture_modules();calls=[]
        class BadModule(ModuleType):
            @property
            def _FLOW_CACHE(self):calls.append(1);return {}
        modules['market_confidence_engine']=BadModule('market_confidence_engine')
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(calls,[]);self.assertEqual(r['root_presence']['flow_cache']['status'],'NOT_LOADED')

    def test_adapter_replacement_invalidation(self):
        modules=self.fixture_modules()
        def change():modules['market_confidence_engine']._FLOW_CACHE={}
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=change,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT');self.assertFalse(r['roots']['flow_cache']['measurement_valid'])

    def test_no_available_roots_is_unavailable(self):
        modules={name:None for name in self.fixture_modules()}
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure()
        self.assertEqual(r['status'],'UNAVAILABLE');self.assertEqual(r['available_root_names'],[])
        self.assertNotIn('covered_union_bytes',r)

    def spot_fixture(self):
        from dataclasses import dataclass,asdict
        from typing import Any,Dict
        module=ModuleType('retained_spot_exact_fixture')
        module.__dict__.update(dataclass=dataclass,asdict=asdict,Any=Any,Dict=Dict,datetime=datetime)
        source=ast.parse((fixture_root()/'binance_spot_price_path.py').read_text(encoding='utf-8'))
        node=next(n for n in source.body if isinstance(n,ast.ClassDef) and n.name=='SpotCandle')
        with patch.dict(sys.modules,{module.__name__:module}):
            exec(compile(ast.Module(body=[node],type_ignores=[]),'exact_spot_ast','exec'),module.__dict__)
        return module

    def candle(self,module):
        return module.SpotCandle(datetime(2026,11,1,tzinfo=timezone.utc),datetime(2026,11,1,0,0,59,tzinfo=timezone.utc),1.25,2.5,.5,1.5,3.75)

    def spot_measure(self,roots,cls):
        return m.measure_named_roots(roots,spot_candle_type=cls,clock=lambda:0.,yield_fn=lambda:None)

    def test_spot_header_native_dictionary_and_fields(self):
        module=self.spot_fixture();candle=self.candle(module);root={'candle':candle}
        result=self.spot_measure({'flow_cache':root},module.SpotCandle)
        self.assertEqual(result['status'],'COMPLETE');self.assertEqual(result['roots']['flow_cache']['type_counts']['spot_candle'],1)
        attrs=object.__getattribute__(candle,'__dict__')
        objects=[root,next(iter(root)),candle,attrs,*attrs.keys(),*attrs.values()]
        expected=sum(sys.getsizeof(v) for v in {id(value):value for value in objects}.values())
        self.assertEqual(result['covered_union_bytes'],expected)

    def test_spot_entire_native_attribute_dictionary_is_counted(self):
        module=self.spot_fixture();candle=self.candle(module)
        object.__getattribute__(candle,'__dict__')['safe_extra']=[float('9.876')]
        result=self.spot_measure({'flow_cache':{'candle':candle}},module.SpotCandle)
        self.assertEqual(result['status'],'COMPLETE');self.assertEqual(result['roots']['flow_cache']['type_counts']['list'],1)

    def test_spot_alias_dedup_within_and_across_roots(self):
        module=self.spot_fixture();candle=self.candle(module)
        values={'history_reference_cache':{'one':candle},'flow_cache':{'two':candle,'three':candle}}
        result=self.spot_measure(values,module.SpotCandle)
        self.assertEqual(result['status'],'COMPLETE')
        for stats in result['roots'].values():self.assertEqual(stats['type_counts']['spot_candle'],1)
        attrs=object.__getattribute__(candle,'__dict__');objects=[candle,attrs,*attrs.keys(),*attrs.values()]
        expected=sum(sys.getsizeof(v) for v in {id(value):value for value in objects}.values())
        self.assertEqual(result['pairwise_overlap_bytes']['history_reference_cache|flow_cache'],expected)

    def test_spot_exact_archive_both_routes(self):
        module=self.spot_fixture();source=ast.parse((fixture_root()/'research_price_archive.py').read_text(encoding='utf-8'))
        node=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name=='_result')
        env=dict(BINANCE_SPOT='BINANCE',HYPERLIQUID_SPOT='HYPE_SPOT',HYPERLIQUID_PERP='PERP',HYPERLIQUID_PERP_GENERIC='GENERIC',BINANCE_MARK='MARK',source_metadata=lambda route,symbol:{'route':route,'symbol':symbol},missing_ranges=lambda *args:[])
        with patch.dict(sys.modules,{'binance_spot_price_path':module}):
            exec(compile(ast.Module(body=[node],type_ignores=[]),'exact_archive_result_ast','exec'),env)
            for route in ('BINANCE','HYPE_SPOT'):
                attrs=object.__getattribute__(self.candle(module),'__dict__')
                path=env['_result'](route,'BTC',[attrs],attrs['open_time_utc'],attrs['close_time_utc'],1,0,1)
                self.assertIs(type(path['candles'][0]),module.SpotCandle)
                result=self.spot_measure({'flow_cache':OrderedDict(path=path)},module.SpotCandle)
                self.assertEqual(result['status'],'COMPLETE')

    def test_spot_pickle_lifecycle_remains_admitted(self):
        module=self.spot_fixture();candle=self.candle(module)
        with patch.dict(sys.modules,{module.__name__:module}):pickle.dumps(candle,protocol=5)
        self.assertEqual(module.SpotCandle.__dict__.get('__slotnames__'),[])
        self.assertTrue(m._safe_spot_candle_type(module.SpotCandle))
        self.assertEqual(self.spot_measure({'flow_cache':{'candle':candle}},module.SpotCandle)['status'],'COMPLETE')

    def test_spot_to_dict_and_field_properties_never_called(self):
        module=self.spot_fixture();candle=self.candle(module);calls=[]
        def trap(self):calls.append(1);raise AssertionError('callback must not run')
        module.SpotCandle.to_dict=property(trap);module.SpotCandle.open=property(trap)
        result=self.spot_measure({'flow_cache':{'candle':candle}},module.SpotCandle)
        self.assertEqual(result['status'],'COMPLETE');self.assertEqual(calls,[])

    def test_spot_without_exact_registration_is_partial(self):
        module=self.spot_fixture();candle=self.candle(module)
        result=run({'flow_cache':{'candle':candle}})
        self.assertEqual(result['status'],'PARTIAL');self.assertEqual(result['unsupported_visits'],1)

    def test_spot_already_loaded_capture(self):
        module=self.spot_fixture();modules=self.fixture_modules();modules['binance_spot_price_path']=module
        modules['market_confidence_engine']._FLOW_CACHE['path']={'candles':[self.candle(module)]}
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(result['status'],'COMPLETE');self.assertTrue(result['roots']['flow_cache']['complete'])
        self.assertEqual(result['roots']['flow_cache']['type_counts']['spot_candle'],1)

    def test_spot_missing_module_does_not_import_provider(self):
        module=self.spot_fixture();modules=self.fixture_modules();modules['binance_spot_price_path']=None
        modules['market_confidence_engine']._FLOW_CACHE['path']={'candles':[self.candle(module)]}
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(result['status'],'PARTIAL');self.assertFalse(result['roots']['flow_cache']['complete'])

    def test_spot_hostile_hooks_slots_finalizer_rejected(self):
        calls=[];fields={name:None for name in m.SPOT_CANDLE_FIELDS}
        for hook in ('__sizeof__','__getattribute__','__len__','__iter__'):
            cls=type('SpotCandle',(object,),{'__dataclass_fields__':fields,hook:lambda *args:calls.append(1) or 1})
            self.assertFalse(m._safe_spot_candle_type(cls))
            result=self.spot_measure({'flow_cache':{'bad':object.__new__(cls)}},cls)
            self.assertEqual(result['status'],'PARTIAL')
        for extra in ({'__slots__':('payload','__dict__')},{'__del__':lambda self:None}):
            cls=type('SpotCandle',(object,),{'__dataclass_fields__':fields,**extra})
            self.assertFalse(m._safe_spot_candle_type(cls))
        self.assertEqual(calls,[])

    def test_spot_foreign_metaclass_and_descriptor_rejected(self):
        calls=[];fields={name:None for name in m.SPOT_CANDLE_FIELDS}
        class Meta(type):
            def __hash__(cls):calls.append('hash');return 1
            def __eq__(cls,other):calls.append('eq');return True
        cls=Meta('SpotCandle',(object,),{'__dataclass_fields__':fields})
        result=self.spot_measure({'flow_cache':{'bad':object.__new__(cls)}},cls)
        self.assertEqual(result['status'],'PARTIAL');self.assertEqual(calls,[])
        for descriptor in (property(lambda self:calls.append('descriptor')),self.spot_fixture().SpotCandle.__dict__['__dict__']):
            cls=type('SpotCandle',(object,),{'__dataclass_fields__':fields,'__dict__':descriptor})
            self.assertFalse(m._safe_spot_candle_type(cls))
        self.assertEqual(calls,[])

    def test_spot_fieldmap_validation_never_calls_foreign_hooks(self):
        calls=[]
        class Fields(dict):
            def __iter__(self):calls.append('iter');return super().__iter__()
        for fields in ({},Fields({name:None for name in m.SPOT_CANDLE_FIELDS})):
            cls=type('SpotCandle',(object,),{'__dataclass_fields__':fields})
            self.assertFalse(m._safe_spot_candle_type(cls))
        self.assertEqual(calls,[])

class CurrentHistoryTests(unittest.TestCase):
    def modules(self):
        return Tests().fixture_modules()

    def test_fixed_history4_only_and_unchanged_budgets(self):
        self.assertEqual(m.ROOT_NAMES,('history_reference_cache','history_input_receipts','flow_cache','flow_input_receipts'))
        self.assertEqual(len(m.ROOT_NAMES),4);self.assertEqual(len(set(m.ROOT_NAMES)),4)
        self.assertEqual((m.MAX_NODES,m.MAX_EDGE_VISITS,m.MAX_WORKSPACE_BYTES,m.MAX_SECONDS,m.MAX_STACK_DEPTH),(4000000,16000000,268435456,30.,128))
        self.assertEqual((m.MAX_OWNER_MASK_PATTERNS,m._MAX_OWNER_MASK),(16,15))

    def test_history_top64_allowed65_rejected(self):
        for name in m.HISTORY_ROOT_NAMES:
            self.assertEqual(run({name:{str(i):i for i in range(64)}})['status'],'COMPLETE')
            result=run({name:{str(i):i for i in range(65)}})
            self.assertEqual(result['status'],'UNAVAILABLE')
            self.assertEqual(result['reason_codes'],['ROOT_TYPE_OR_ITEM_BOUND'])

    def test_additional32_names_are_not_admitted(self):
        for name in ('paper_checkpoint_loaded','past_price_lru','ai_conversation_messages'):
            result=run({name:{}})
            self.assertEqual(result['reason_codes'],['INVALID_OPTIONS_OR_ROOT_NAMES'])
        with self.assertRaisesRegex(ValueError,'UNSAFE_ADAPTER'):m._resolve('paper_checkpoint_loaded')

    def test_history_top_admits_only_exact_mapping_shapes(self):
        for value in ([],deque(),Counter(),defaultdict(list)):
            result=run({'history_reference_cache':value})
            self.assertEqual(result['status'],'UNAVAILABLE')
        self.assertEqual(run({'history_reference_cache':OrderedDict()})['status'],'COMPLETE')

    def test_supplied_snapshot_cannot_bypass_history_cap(self):
        value={str(i):i for i in range(65)}
        snapshot=m._shape(value,max_items=65)
        with patch.object(m,'_children',side_effect=AssertionError('must not walk')):
            result=run({'history_reference_cache':value},_snapshots={'history_reference_cache':snapshot})
        self.assertEqual(result['reason_codes'],['ROOT_TYPE_OR_ITEM_BOUND'])

    def test_history_reference_and_receipt_types_loaded_lazily(self):
        modules=self.modules()
        reference=_ReferenceRanges(rows=4,windows={'30m':{'pairs':[(1.25,2.5)]}})
        package=_CapturedPackage(objects={'a':'sealed'},events=[])
        package.extra=reference
        modules['coinglass_history_backfill']._REFERENCE_CACHE['ETH']=reference
        modules['coinglass_history_backfill']._REFERENCE_INPUT_RECEIPTS['ETH']=package
        modules['research_consumed_inputs']._CapturedPackage=_CapturedPackage
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertTrue(result['declared_union_complete_valid'])
        self.assertEqual(result['roots']['history_reference_cache']['type_counts']['reference_ranges'],1)
        self.assertEqual(result['roots']['history_input_receipts']['type_counts']['captured_package'],1)
        self.assertEqual(result['root_item_limits']['history_reference_cache'],64)
        self.assertEqual(result['root_item_limits']['flow_cache'],64)

    def test_busy_history_receipt_lock_excludes_only_that_root(self):
        modules=self.modules();lock=modules['coinglass_history_backfill']._REFERENCE_INPUT_LOCK;lock.acquire()
        try:
            with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        finally:lock.release()
        self.assertEqual(result['root_presence']['history_input_receipts']['status'],'LOCK_UNAVAILABLE')
        self.assertNotIn('history_input_receipts',result['roots'])
        self.assertTrue(result['roots']['history_reference_cache']['complete'])
        self.assertEqual(len(result['available_root_names']),3)
        self.assertFalse(result['declared_union_complete_valid'])

    def test_both_history_receipt_locks_released_before_walk(self):
        modules=self.modules();observed=[]
        locks=(modules['coinglass_history_backfill']._REFERENCE_INPUT_LOCK,modules['market_confidence_engine']._FLOW_INPUT_LOCK)
        def observe():
            for lock in locks:
                acquired=lock.acquire(False);observed.append(acquired)
                if acquired:lock.release()
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=observe,yield_every=1)
        self.assertTrue(result['declared_union_complete_valid']);self.assertTrue(observed);self.assertTrue(all(observed))

    def test_history_to_flow_receipt_shared_value_and_high_bit(self):
        shared=[float('1.23456789')];values={name:{} for name in m.ROOT_NAMES}
        values['history_reference_cache']={'h':shared};values['flow_input_receipts']={'r':shared}
        result=run(values);expected=sys.getsizeof(shared)+sys.getsizeof(shared[0])
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['pairwise_overlap_bytes']['history_reference_cache|flow_input_receipts'],expected)
        self.assertEqual(result['covered_shared_unique_bytes'],expected)
        self.assertEqual(result['repeated_reachable_bytes'],expected)
        self.assertEqual(result['workspace_mask_int_transient_reserve_bytes'],3*sys.getsizeof(15))

    def test_shared_mutation_invalidates_history_and_receipt_owner(self):
        shared=[1.23];values={'history_reference_cache':{'h':shared},'history_input_receipts':{'r':shared}}
        original=m._children;seen=[]
        def children(value,kind):
            if value is shared:
                seen.append(1)
                if len(seen)==2:shared.append(4.56)
            yield from original(value,kind)
        with patch.object(m,'_children',children):result=run(values)
        self.assertEqual(result['status'],'CONCURRENT')
        self.assertFalse(result['roots']['history_reference_cache']['measurement_valid'])
        self.assertFalse(result['roots']['history_input_receipts']['measurement_valid'])

    def test_missing_history_does_not_veto_flow2(self):
        modules=self.modules();modules['coinglass_history_backfill']=None
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(result['root_presence']['history_reference_cache']['status'],'NOT_LOADED')
        self.assertEqual(result['root_presence']['history_input_receipts']['status'],'NOT_LOADED')
        self.assertEqual(len(result['available_root_names']),2)
        self.assertTrue(result['roots']['flow_cache']['complete'])
        self.assertFalse(result['declared_union_complete_valid'])

    def test_history_root_replacement_only_invalidates_its_binding(self):
        modules=self.modules();changed=[]
        def replace():
            if not changed:
                changed.append(True);modules['coinglass_history_backfill']._REFERENCE_CACHE={}
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=replace,yield_every=1)
        self.assertEqual(result['status'],'CONCURRENT')
        self.assertFalse(result['roots']['history_reference_cache']['measurement_valid'])
        self.assertTrue(result['roots']['flow_cache']['measurement_valid'])

    def test_capture_history_cap_reports_missing_not_empty(self):
        modules=self.modules();modules['coinglass_history_backfill']._REFERENCE_CACHE={str(i):i for i in range(65)}
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(result['root_presence']['history_reference_cache']['status'],'ROOT_BOUND')
        self.assertNotIn('history_reference_cache',result['roots'])
        self.assertEqual(result['root_presence']['history_reference_cache']['entry_count'],None)

    def test_subset_still_reserves_three_max4bit_transients(self):
        result=run({'flow_cache':{}})
        expected=sys.getsizeof(15)
        self.assertEqual(result['workspace_mask_int_transient_reserve_bytes'],3*expected)
        self.assertEqual(result['workspace_mask_pool_count'],1)
        self.assertGreater(result['workspace_mask_pool_accounted_bytes'],result['workspace_mask_pool_value_bytes'])

    def test_no_available_history_root_has_no_fabricated_union(self):
        modules={name:None for name in self.modules()}
        with patch.dict(sys.modules,modules):result=m.capture_live_roots_and_measure()
        self.assertEqual(result['status'],'UNAVAILABLE')
        self.assertEqual(len(result['root_presence']),4)
        self.assertNotIn('covered_union_bytes',result)


class OwnerMaskPoolTests(unittest.TestCase):
    def test_equal_mask_reuses_same_canonical_key_and_value(self):
        pool=m._OwnerMaskPool();first=15;second=int('15')
        self.assertIs(pool.intern(first,100000),first)
        self.assertIs(pool.intern(second,100000),first)
        self.assertEqual(len(pool.values),1)
        self.assertTrue(all(key is value for key,value in pool.values.items()))

    def test_values_counted_once_and_capacity_is_actual(self):
        pool=m._OwnerMaskPool()
        for value in (1,2,3,5,8,15):pool.intern(value,1000000)
        before=(len(pool.values),pool.value_bytes,pool.capacity_bytes)
        for _ in range(100):pool.intern(int('5'),0)
        self.assertEqual(before,(len(pool.values),pool.value_bytes,pool.capacity_bytes))
        self.assertEqual(pool.value_bytes,sum(sys.getsizeof(key) for key in pool.values))
        self.assertEqual(pool.capacity_bytes,sys.getsizeof(pool.values));self.assertEqual(pool.last_reserve_bytes,0)

    def test_all15_valid_patterns_fit16_cap_and_invalid16_is_rejected(self):
        pool=m._OwnerMaskPool()
        for value in range(1,16):pool.intern(value,10000000)
        before=(pool.value_bytes,pool.capacity_bytes,len(pool.values))
        self.assertEqual(len(pool.values),15);self.assertEqual(pool.max_patterns,16)
        with self.assertRaisesRegex(ValueError,'OWNER_MASK_INVALID'):pool.intern(16,10000000)
        self.assertEqual(before,(pool.value_bytes,pool.capacity_bytes,len(pool.values)))
        self.assertNotIn(16,pool.values)

    def test_pattern_cap_marks_graph_partial(self):
        shared={}
        with patch.object(m,'MAX_OWNER_MASK_PATTERNS',2):
            result=run({'history_reference_cache':shared,'flow_cache':shared})
        self.assertEqual(result['status'],'PARTIAL')
        self.assertIn('OWNER_MASK_BOUND',result['reason_codes'])
        self.assertFalse(result['complete']);self.assertEqual(result['workspace_mask_pool_count'],2)
        self.assertEqual(result['workspace_mask_pool_max_patterns'],2)

    def test_pool_workspace_bound_precedes_first_mutation(self):
        pool=m._OwnerMaskPool();before=(pool.value_bytes,pool.capacity_bytes)
        with self.assertRaisesRegex(ValueError,'WORKSPACE_BOUND'):pool.intern(5,0)
        self.assertEqual(pool.values,{})
        self.assertEqual(before,(pool.value_bytes,pool.capacity_bytes))
        self.assertGreater(pool.last_reserve_bytes,0)

    def test_every_growth_has_preinsert_reserve_and_exact_capacity(self):
        pool=m._OwnerMaskPool();peak=0;growths=0
        for value in range(1,16):
            before=pool.capacity_bytes;reserve=3*before+512+sys.getsizeof(value)
            pool.intern(value,reserve)
            self.assertEqual(pool.last_reserve_bytes,reserve)
            self.assertLessEqual(pool.capacity_bytes,before+reserve)
            self.assertEqual(pool.capacity_bytes,sys.getsizeof(pool.values))
            self.assertIs(pool.values[value],next(key for key in pool.values if key==value))
            peak=max(peak,reserve);growths+=pool.capacity_bytes!=before
        self.assertGreaterEqual(growths,3);self.assertEqual(pool.resize_reserve_peak,peak)

    def test_resize_boundary_rejected_without_partial_insert(self):
        pool=m._OwnerMaskPool()
        for value in range(1,6):pool.intern(value,1000000)
        before=(dict(pool.values),pool.value_bytes,pool.capacity_bytes)
        reserve=3*pool.capacity_bytes+512+sys.getsizeof(6)
        with self.assertRaisesRegex(ValueError,'WORKSPACE_BOUND'):pool.intern(6,reserve-1)
        self.assertEqual(before,(pool.values,pool.value_bytes,pool.capacity_bytes))
        self.assertIs(pool.intern(6,reserve),pool.values[6])

    def test_mask_domain_rejects_hooks_subclasses_and_out_of_range(self):
        calls=[]
        class BadInt(int):
            def __hash__(self):calls.append('hash');return 1
            def __sizeof__(self):calls.append('size');return 1
        class Meta(type):
            def __hash__(self):calls.append('meta_hash');return 1
        class Foreign(metaclass=Meta):pass
        pool=m._OwnerMaskPool()
        for value in (True,0,-1,1<<4,BadInt(7),Foreign()):
            with self.assertRaisesRegex(ValueError,'OWNER_MASK_INVALID'):pool.intern(value,1000000)
        self.assertEqual(calls,[]);self.assertEqual(pool.values,{})
        self.assertEqual(pool.intern(15,1000000),15)

    def test_receipt_pool_accounting_formula_and_transient_masks(self):
        result=run({'history_reference_cache':{'a':[1.25]},'flow_cache':{'b':[1.25]}})
        self.assertEqual(result['workspace_owner_mask_mode'],m.OWNER_MASK_MODE)
        self.assertEqual(result['workspace_mask_pool_accounted_bytes'],sum(result[key] for key in
            ('workspace_mask_pool_value_bytes','workspace_mask_pool_dict_capacity_bytes','workspace_mask_pool_state_bytes','workspace_mask_temporary_reserve_bytes')))
        self.assertEqual(result['workspace_mask_temporary_reserve_bytes'],result['workspace_mask_int_transient_reserve_bytes']+result['workspace_mask_operation_scratch_bytes'])
        self.assertEqual(result['workspace_mask_int_transient_reserve_bytes'],3*sys.getsizeof(15))
        self.assertGreater(result['workspace_memo_resize_reserve_peak_bytes'],0)
        self.assertIsNone(result['workspace_next_operation_counted_upper_bound_bytes'])
        self.assertNotIn('workspace_mask_bytes_per_unique_node',result)

    def test_memo_assignment_paths_only_use_interned_values(self):
        source=ast.parse(Path(m.__file__).read_text())
        fn=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name=='measure_named_roots')
        values=[]
        for n in ast.walk(fn):
            if isinstance(n,ast.Assign) and any(isinstance(t,ast.Subscript) and isinstance(t.value,ast.Name) and t.value.id=='shard' for t in n.targets):values.append(ast.unparse(n.value))
        self.assertCountEqual(values,['bit','combined'])
        for name in ('bit','combined'):
            assigned=[n for n in ast.walk(fn) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets)]
            self.assertTrue(all(isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='intern_mask' for n in assigned))

    def test_pool_has_no_global_lifetime_after_success(self):
        references=[];original=m._OwnerMaskPool
        def create():
            pool=original();references.append(weakref.ref(pool));return pool
        with patch.object(m,'_OwnerMaskPool',create):result=run({'flow_cache':{'a':[1.25]}})
        self.assertEqual(result['status'],'COMPLETE');self.assertTrue(references)
        self.assertTrue(all(ref() is None for ref in references))
        json.dumps(result,allow_nan=False)

    def test_pool_exception_releases_temporary_input_pins(self):
        item=_ReferenceRanges(value=1.25);ref=weakref.ref(item)
        values={'history_reference_cache':{'a':item},'flow_cache':{'b':item}};del item
        original=m._OwnerMaskPool.intern
        def fail_on_union(pool,value,headroom):
            if value==3:raise BaseException('fixture-only failure')
            return original(pool,value,headroom)
        with patch.object(m._OwnerMaskPool,'intern',fail_on_union):result=run(values,reference_ranges_type=_ReferenceRanges)
        self.assertEqual(result['status'],'UNAVAILABLE')
        del values
        self.assertIsNone(ref())

    def test_empty_pool_does_not_preallocate_mask_space(self):
        pool=m._OwnerMaskPool()
        self.assertEqual(len(pool.values),0);self.assertEqual(pool.value_bytes,0)
        self.assertEqual(pool.capacity_bytes,sys.getsizeof({}))
        self.assertEqual(pool.max_patterns,16)

    def test_producer_ast_fixtures_are_selfcontained(self):
        for name in ('coinglass_history_backfill.py','research_consumed_inputs.py','research_event_capture.py','binance_spot_price_path.py','research_price_archive.py'):
            self.assertTrue((fixture_root()/name).is_file())
        self.assertIn(fixture_root(),(Path(__file__).parent,Path(__file__).parent/'fixtures'))


if __name__=='__main__':unittest.main()
