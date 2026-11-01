"""Small pure fixtures for named graph accounting and fail-open capture."""
import importlib.util
import ast
from collections import OrderedDict
import hashlib
import json
from pathlib import Path
import pickle
import sys
import threading
from types import ModuleType
import unittest
import weakref
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('history_memory_under_test',Path(__file__).with_name('research_history_memory_accounting.py'))
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
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['covered_union_bytes'],sum(sys.getsizeof(v) for v in values.values()))
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
        self.assertEqual(r['status'],'PARTIAL');self.assertIn('WORKSPACE_BOUND',r['reason_codes'])
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
    def modules(self):
        history=ModuleType('coinglass_history_backfill');flow=ModuleType('market_confidence_engine');consumed=ModuleType('research_consumed_inputs')
        history._REFERENCE_CACHE={'ETH':_ReferenceRanges(rows=4,windows={'30m':{'price_composition_samples':[(1.,.5)],'oi_composition_samples':[(2.,.5)]}})}
        history._REFERENCE_INPUT_RECEIPTS=OrderedDict();history._REFERENCE_INPUT_LOCK=threading.Lock();history._ReferenceRanges=_ReferenceRanges
        flow._FLOW_CACHE={};flow._FLOW_INPUT_RECEIPTS=OrderedDict();flow._FLOW_INPUT_LOCK=threading.Lock();consumed._CapturedPackage=_CapturedPackage
        return {history.__name__:history,flow.__name__:flow,consumed.__name__:consumed}
    def test_loaded_only_missing(self):
        with patch.dict(sys.modules,{'coinglass_history_backfill':None}):r=m.capture_live_roots_and_measure()
        self.assertEqual(r['reason_codes'],['PRODUCER_MODULE_NOT_LOADED'])
    def test_capture_actual_original_roots(self):
        modules=self.modules()
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertTrue(r['complete']);self.assertEqual(r['history_entries'][0]['rows_scalar'],4)
        self.assertEqual(r['history_entries'][0]['windows'][0]['price_pair_count'],1)
    def test_busy_lock(self):
        modules=self.modules();lock=modules['coinglass_history_backfill']._REFERENCE_INPUT_LOCK;lock.acquire()
        try:
            with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure()
        finally:lock.release()
        self.assertEqual(r['reason_codes'],['RECEIPT_LOCK_UNAVAILABLE'])
    def test_locks_released_before_walk(self):
        modules=self.modules();observations=[]
        def observe():
            locks=(modules['coinglass_history_backfill']._REFERENCE_INPUT_LOCK,modules['market_confidence_engine']._FLOW_INPUT_LOCK)
            for lock in locks:
                ok=lock.acquire(False);observations.append(ok)
                if ok:lock.release()
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=observe,yield_every=1)
        self.assertTrue(r['complete']);self.assertTrue(all(observations))
    def test_global_root_replacement(self):
        modules=self.modules()
        def mutate():modules['market_confidence_engine']._FLOW_CACHE={}
        with patch.dict(sys.modules,modules):r=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=mutate,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT');self.assertIn('ROOT_REPLACED',r['reason_codes'])
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
        def mutate():
            calls.append(1)
            if len(calls)==10: shared.append(3.456)
        r=m.measure_named_roots(values,clock=lambda:0.,yield_fn=mutate,yield_every=1)
        self.assertEqual(r['status'],'CONCURRENT')
        self.assertFalse(r['roots']['history_reference_cache']['measurement_valid'])
        self.assertFalse(r['roots']['flow_cache']['measurement_valid'])

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
            values=roots({'ETH':history});values['history_input_receipts']['receipt']=package
            result=run(values,reference_ranges_type=producer._ReferenceRanges,captured_package_type=producer._CapturedPackage)
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['roots']['history_reference_cache']['type_counts']['reference_ranges'],1)
        self.assertEqual(result['roots']['history_input_receipts']['type_counts']['captured_package'],1)
        self.assertEqual(result['version'],'named-history-memory-v2')

    def test_exact_producer_guard_capture_integration(self):
        producer=self.exact_producer_fixture();modules=self.modules()
        modules['coinglass_history_backfill']._ReferenceRanges=producer._ReferenceRanges
        history=producer._ReferenceRanges(rows=4,windows={'30m':{'price_composition_samples':[(1.,.5)],'oi_composition_samples':[(2.,.5)]}})
        modules['coinglass_history_backfill']._REFERENCE_CACHE={'ETH':history}
        modules['research_consumed_inputs']._CapturedPackage=producer._CapturedPackage
        with patch.dict(sys.modules,{**modules,producer.__name__:producer}):
            producer._local_input_guard(history)
            result=m.capture_live_roots_and_measure(clock=lambda:0.,yield_fn=lambda:None)
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['history_entries'][0]['rows_scalar'],4)
        self.assertEqual(result['history_entries'][0]['windows'][0]['price_pair_count'],1)

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

    def test_cardinality_helper_does_not_consult_custom_class_property(self):
        calls=[]
        class Foreign:
            @property
            def __class__(self):calls.append('class');return dict
        self.assertEqual(m._history_entries((('ETH',Foreign()),)),[])
        self.assertEqual(calls,[])

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

    def test_metadata_sequences_never_compare_foreign_metaclass(self):
        calls=[]
        class Meta(type):
            def __eq__(cls,other):calls.append('eq');return True
            def __hash__(cls):calls.append('hash');return 1
        class Foreign(metaclass=Meta):
            def __len__(self):calls.append('len');return 2
        value={'rows':4,'windows':{'30m':{'price_composition_samples':Foreign(),'oi_composition_samples':Foreign()}}}
        result=m._history_entries((('ETH',value),))
        self.assertIsNone(result[0]['windows'][0]['price_pair_count'])
        self.assertIsNone(result[0]['windows'][0]['oi_pair_count']);self.assertEqual(calls,[])

if __name__=='__main__':unittest.main()
