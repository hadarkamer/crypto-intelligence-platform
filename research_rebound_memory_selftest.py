"""Pure fixed fake modules/containers/callables; no producer/native/process IO."""
import asyncio
from collections import OrderedDict,Counter,deque
import json
import sys
from types import ModuleType
import unittest
from unittest.mock import patch
import weakref
import research_rebound_memory as m

class Clock:
    def __init__(self):self.value=0.
    def __call__(self):return self.value
NOW=lambda:'2026-11-01T17:00:00+00:00'

def history(samples=None):
    samples=[]if samples is None else samples
    return {'BTC':{'windows':{label:{'price_composition_samples':samples,'oi_composition_samples':samples}for label in m._WINDOWS}}}

def shells(values=None,clock=None):
    roots={name:{}for name in m.ROOT_NAMES};roots.update(values or{})
    with patch.object(m,'_resolve',side_effect=lambda name:(roots[name],())):
        return m.capture_shells_and_operations(monotonic=clock or(lambda:0.),utc_now=NOW)

class HeaderTests(unittest.TestCase):
    def test_exact_order_and_scope(self):
        self.assertEqual(len(m.ROOT_NAMES),36);self.assertEqual(m.ROOT_NAMES[:4],m.HISTORY_ROOT_NAMES)
        r=shells();self.assertFalse(r['full_graph_measured']);self.assertIsNone(r['deep_bytes_total']);self.assertEqual(r['limits']['workspace_bytes'],131072)
    def test_absent_is_not_empty(self):
        with patch.object(m,'_resolve',side_effect=ValueError('NOT_LOADED')):r=m.capture_shells_and_operations(monotonic=lambda:0.,utc_now=NOW)
        self.assertEqual(len(r['roots']),36);self.assertIsNone(r['roots'][m.ROOT_NAMES[0]]['entry_count']);self.assertEqual(r['status'],'UNAVAILABLE')
    def test_complete_all36_empty_roots(self):
        r=shells();self.assertEqual(r['status'],'COMPLETE');self.assertTrue(r['projection_complete']);self.assertEqual(r['covered_union_containers'],36)
    def test_alias_identity_deduplicates(self):
        shared={};r=shells(dict.fromkeys(m.ROOT_NAMES,shared));self.assertEqual(r['covered_union_containers'],1);self.assertEqual(r['covered_union_shallow_bytes'],sys.getsizeof(shared))
    def test_history_never_walks_million_sample_leaves(self):
        class Bad:
            def __sizeof__(self):raise AssertionError('leaf')
        samples=[Bad()]*1000000;r=shells({'history_reference_cache':history(samples)})
        self.assertEqual(r['status'],'COMPLETE');self.assertLess(r['covered_union_containers'],70);self.assertGreater(r['roots']['history_reference_cache']['covered_shallow_bytes'],8000000)
    def test_values_and_addresses_not_persisted(self):
        samples=['PRIVATE_SAVED_DATA'];r=shells({'history_reference_cache':history(samples)});text=json.dumps(r)
        self.assertNotIn('PRIVATE_SAVED_DATA',text);self.assertNotIn(str(id(samples)),text)
    def test_foreign_list_hooks_not_called(self):
        calls=[]
        class Bad(list):
            def __len__(self):calls.append('len');return 0
            def __sizeof__(self):calls.append('size');return 1
        r=shells({'sheets_queue_payloads':Bad()});self.assertNotEqual(r['roots']['sheets_queue_payloads']['status'],'CAPTURED');self.assertEqual(calls,[])
    def test_foreign_metaclass_not_hashed_or_compared(self):
        calls=[]
        class Meta(type):
            def __hash__(self):calls.append('hash');return 1
            def __eq__(self,x):calls.append('eq');return False
        class Bad(metaclass=Meta):pass
        shells({'sheets_queue_payloads':Bad()});self.assertEqual(calls,[])
    def test_foreign_key_not_compared(self):
        calls=[]
        class Key:
            def __hash__(self):return hash('BTC')
            def __eq__(self,x):calls.append('eq');return False
        value={Key():[]};calls.clear();r=shells({'history_reference_cache':value});self.assertEqual(calls,[]);self.assertIn('UNSAFE_ROUTE_KEY',r['reason_codes'])
    def test_large_reconciler_no_entry_walk(self):
        class Leaf:
            def __sizeof__(self):raise AssertionError('leaf')
        root={str(i):Leaf()for i in range(50000)};r=shells({'reconciler_expected':root});self.assertEqual(r['roots']['reconciler_expected']['covered_containers'],1);self.assertEqual(r['roots']['reconciler_expected']['entry_count'],50000)
    def test_busy_existing_lock_not_waited_or_released(self):
        lock=m.threading.Lock();lock.acquire()
        with patch.object(m,'_resolve',side_effect=lambda name:({},(lock,))):r=m.capture_shells_and_operations(monotonic=lambda:0.,utc_now=NOW)
        self.assertTrue(lock.locked());lock.release();self.assertIn('LOCK_UNAVAILABLE',r['reason_codes'])
    def test_lock_released_after_bad_root(self):
        lock=m.threading.Lock()
        with patch.object(m,'_resolve',side_effect=lambda name:(object(),(lock,))):m.capture_shells_and_operations(monotonic=lambda:0.,utc_now=NOW)
        self.assertFalse(lock.locked())
    def test_replacement_invalidates_header(self):
        calls={};values={n:{}for n in m.ROOT_NAMES}
        def resolve(n):
            calls[n]=calls.get(n,0)+1
            return(values[n]if calls[n]==1 else{},())
        with patch.object(m,'_resolve',side_effect=resolve):r=m.capture_shells_and_operations(monotonic=lambda:0.,utc_now=NOW)
        self.assertIn('ROOT_REPLACED',r['reason_codes']);self.assertFalse(r['projection_complete'])
    def test_nested_bound_keeps_all36_root_headers(self):
        many={f'x{i}':(0,{})for i in range(65)};r=shells({'flow_cache':many})
        self.assertTrue(all(x['status']=='CAPTURED'for x in r['roots'].values()));self.assertFalse(r['roots']['flow_cache']['fixed_routes_complete']);self.assertIn('ROUTE_ITEM_BOUND',r['reason_codes'])
    def test_container_bound_does_not_fake_deep_total(self):
        with patch.object(m,'MAX_CONTAINERS',40):r=shells({'history_reference_cache':history()})
        self.assertIn('CONTAINER_BOUND',r['reason_codes']);self.assertIsNone(r['deep_bytes_total']);self.assertTrue(all(x['status']=='CAPTURED'for x in r['roots'].values()))
    def test_workspace_bound_before_traversal(self):
        with patch.object(m,'MAX_WORKSPACE_BYTES',1000):r=shells()
        self.assertNotEqual(r['status'],'COMPLETE');self.assertEqual(r['covered_union_containers'],0)
    def test_cooperative_time_partial_explicit(self):
        c=Clock()
        def tick():c.value+=.005;return c.value
        r=shells(clock=tick);self.assertIn('TIME_BUDGET',r['reason_codes']);self.assertFalse(r['projection_complete'])
    def test_same_size_nested_replacement_detected(self):
        root=history([1]);calls={};values={n:{}for n in m.ROOT_NAMES};values['history_reference_cache']=root
        def resolve(n):
            calls[n]=calls.get(n,0)+1
            if n=='history_reference_cache'and calls[n]==3:root['BTC']['windows']['30m']['price_composition_samples']=[2]
            return values[n],()
        with patch.object(m,'_resolve',side_effect=resolve):r=m.capture_shells_and_operations(monotonic=lambda:0.,utc_now=NOW)
        self.assertFalse(r['roots']['history_reference_cache']['header_shapes_stable']);self.assertIn('CONCURRENT_OR_UNVERIFIED',r['reason_codes'])
    def test_safe_loaded_module_only(self):
        with patch.dict(sys.modules,{'google_sheets_sync':object()}):
            with self.assertRaises(ValueError):m._resolve('sheets_snapshot_cache')
    def test_instance_dict_property_not_called(self):
        calls=[]
        class Bad:
            @property
            def __dict__(self):calls.append(1);return{}
        with self.assertRaises(ValueError):m._field(Bad(),'_store')
        self.assertEqual(calls,[])
    def test_native_descriptor_reads_only_named_field(self):
        class Worker:pass
        w=Worker();w._store={};w.secret='PRIVATE'
        self.assertIs(m._field(w,'_store'),w._store)
    def test_reference_class_real_shape_no_size_hooks(self):
        class _ReferenceRanges(dict):__slots__=('__weakref__',)
        module=ModuleType('coinglass_history_backfill');module._ReferenceRanges=_ReferenceRanges
        root={'BTC':_ReferenceRanges(windows={})}
        with patch.dict(sys.modules,{'coinglass_history_backfill':module}):r=shells({'history_reference_cache':root})
        self.assertTrue(r['roots']['history_reference_cache']['fixed_routes_complete'])
    def test_return_does_not_retain_container_owner(self):
        class _ReferenceRanges(dict):__slots__=('__weakref__',)
        module=ModuleType('coinglass_history_backfill');module._ReferenceRanges=_ReferenceRanges
        root={'BTC':_ReferenceRanges(windows={})};weak=weakref.ref(root['BTC'])
        with patch.dict(sys.modules,{'coinglass_history_backfill':module}):r=shells({'history_reference_cache':root})
        root.clear();self.assertIsNone(weak());self.assertIsInstance(r['roots'],dict)
    def test_past_candle_list_header_not_candles(self):
        r=shells({'past_price_lru':OrderedDict([(('BTC',1,2),{'candles':[object()]*1000})])});self.assertEqual(r['roots']['past_price_lru']['covered_containers'],3)
    def test_checkpoint_book_routes(self):
        r=shells({'paper_checkpoint_loaded':{'state':{'books':{'x':{'closed':[],'choices':{}}},'first_seen':{}}}});self.assertGreater(r['roots']['paper_checkpoint_loaded']['covered_containers'],5)
    def test_operation_status_failure_is_closed(self):
        with patch.object(m,'operation_status',side_effect=BaseException('PRIVATE')):r=shells()
        self.assertEqual(r['operations']['status'],'UNAVAILABLE');self.assertNotIn('PRIVATE',json.dumps(r))
    def test_operation_status_oversize_not_repeated_in_fallback(self):
        with patch.object(m,'operation_status',return_value={'private':'x'*100000}):r=shells()
        self.assertEqual(r['operations']['status'],'UNAVAILABLE');self.assertLess(len(json.dumps(r).encode()),32768)

class OperationTests(unittest.TestCase):
    def setUp(self):self.clock=Clock();self.o=m.OperationObserver(deadline_monotonic=500,monotonic=self.clock,utc_now=NOW,blocks=lambda:100)
    def test_return_identity_and_no_rss(self):
        value=object();f=self.o.wrap('OI_LATEST',lambda:value);self.assertIs(f(),value);self.assertFalse(self.o.status()['per_operation_rss_measured'])
    def test_baseexception_identity(self):
        error=BaseException('PRIVATE')
        def business():raise error
        try:self.o.wrap('OI_LATEST',business)()
        except BaseException as caught:self.assertIs(caught,error)
        self.assertEqual(self.o.totals['OI_LATEST']['exceptions'],1);self.assertNotIn('PRIVATE',json.dumps(self.o.status()))
    def test_native_cancellation_identity(self):
        error=asyncio.CancelledError()
        def business():raise error
        try:self.o.wrap('OI_LATEST',business)()
        except asyncio.CancelledError as caught:self.assertIs(caught,error)
    def test_begin_fault_never_blocks_business(self):
        value=object();self.o.begin=lambda *a:(_ for _ in()).throw(BaseException());self.assertIs(self.o.wrap('OI_LATEST',lambda:value)(),value)
    def test_finish_fault_never_masks_return(self):
        value=object();self.o.finish=lambda *a:(_ for _ in()).throw(BaseException());self.assertIs(self.o.wrap('OI_LATEST',lambda:value)(),value)
    def test_finish_fault_never_masks_business_exception(self):
        error=BaseException();self.o.finish=lambda *a:(_ for _ in()).throw(BaseException())
        def business():raise error
        try:self.o.wrap('OI_LATEST',business)()
        except BaseException as caught:self.assertIs(caught,error)
    def test_arguments_and_result_not_retained(self):
        class Payload:pass
        value=Payload();weak=weakref.ref(value);self.o.wrap('OI_LATEST',lambda x:None)(value);del value;self.assertIsNone(weak())
    def test_inclusive_nested_counts_not_exclusive(self):
        inner=self.o.wrap('HISTORY_WINDOW',lambda:None);self.o.wrap('OI_LATEST',inner)();status=self.o.status();self.assertEqual(status['totals']['OI_LATEST']['completed'],1);self.assertEqual(status['totals']['HISTORY_WINDOW']['completed'],1);self.assertIn('INCLUSIVE',status['scope'])
    def test_busy_start_lock_sticky_drop(self):
        self.o.lock.acquire();self.o.wrap('OI_LATEST',lambda:None)();self.o.lock.release();s=self.o.status();self.assertEqual(s['dropped_updates'],1);self.assertFalse(s['tracked_edge_coverage_complete'])
    def test_busy_finish_lock_never_infers_idle(self):
        token=self.o.begin('OI_LATEST');self.o.lock.acquire();self.o.finish('OI_LATEST',token,False);self.o.lock.release();s=self.o.status();self.assertEqual(s['active_tracked_calls'],1);self.assertFalse(s['tracked_edge_coverage_complete'])
    def test_active_bound_not_business_limit(self):
        self.o.active=64;calls=[];self.o.wrap('OI_LATEST',lambda:calls.append(1))();self.assertEqual(calls,[1]);self.assertEqual(self.o.dropped,1)
    def test_stop_inflight_is_honest(self):
        token=self.o.begin('OI_LATEST');self.o.stop();self.o.finish('OI_LATEST',token,False);self.assertEqual(self.o.status()['completed_after_window'],1)
    def test_expired_no_business_probe(self):
        self.clock.value=500;calls=[];self.o.wrap('OI_LATEST',lambda:calls.append(1))();self.assertEqual(calls,[1]);self.assertEqual(self.o.totals['OI_LATEST']['started'],0)
    def test_four_bucket_bound_and_eviction_disclosed(self):
        for i in range(8):self.clock.value=i*16;self.o.wrap('OI_LATEST',lambda:None)()
        s=self.o.status();self.assertEqual(len(s['buckets']),4);self.assertGreater(s['evicted_buckets'],0);self.assertFalse(s['retained_bucket_coverage_complete'])
    def test_elapsed_counter_saturates(self):
        self.o.totals['OI_LATEST']['started']=m._MAX_INT;self.o.wrap('OI_LATEST',lambda:None)();self.assertEqual(self.o.totals['OI_LATEST']['started'],m._MAX_INT);self.assertTrue(self.o.overflow)
    def test_partial_install_and_exact_restore(self):
        module=ModuleType('coinglass_oi_regime_service');original=lambda:object();module.latest=original
        with patch.dict(sys.modules,{'coinglass_oi_regime_service':module}):
            s=self.o.install();self.assertEqual(s['presence']['OI_LATEST'],'INSTALLED');self.assertIsNot(module.latest,original);self.o.stop();self.assertIs(module.latest,original)
    def test_other_replacement_not_overwritten_on_stop(self):
        module=ModuleType('coinglass_oi_regime_service');module.latest=lambda:None
        with patch.dict(sys.modules,{'coinglass_oi_regime_service':module}):
            self.o.install();other=lambda:None;module.latest=other;self.o.stop();self.assertIs(module.latest,other);self.assertGreater(self.o.dropped,0)
    def test_method_binding_identity_preserved(self):
        module=ModuleType('research_forward_paper_store');value=object()
        class PostgresCheckpoint:
            def checkpoint(self,a):return a
        module.PostgresCheckpoint=PostgresCheckpoint;original=PostgresCheckpoint.checkpoint
        with patch.dict(sys.modules,{'research_forward_paper_store':module}):self.o.install();self.assertIs(PostgresCheckpoint().checkpoint(value),value);self.o.stop();self.assertIs(PostgresCheckpoint.checkpoint,original)
    def test_global_install_once_never_rearms(self):
        with patch.object(m,'_OBSERVER',None),patch.object(m,'_INSTALL_USED',False):
            m.install_operation_observer(deadline_monotonic=100,monotonic=self.clock,utc_now=NOW);m.stop_operation_observer();s=m.install_operation_observer(deadline_monotonic=200,monotonic=self.clock,utc_now=NOW);self.assertEqual(s['status'],'NOT_REARMED')
    def test_invalid_option_foreign_metaclass_not_called(self):
        calls=[]
        class Meta(type):
            def __eq__(self,x):calls.append(1);return False
        class Bad(metaclass=Meta):pass
        with patch.object(m,'_OBSERVER',None),patch.object(m,'_INSTALL_USED',False):m.install_operation_observer(deadline_monotonic=Bad(),monotonic=self.clock,utc_now=NOW)
        self.assertEqual(calls,[])
    def test_no_business_sink_or_process_read(self):
        with patch('builtins.open',side_effect=AssertionError('process read')):
            self.o.wrap('OI_LATEST',lambda:None)()
        self.assertFalse(self.o.status()['payload_values_or_exceptions_retained'])

if __name__=='__main__':unittest.main()
