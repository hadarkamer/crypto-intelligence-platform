"""Finite cheap named-container shells and scalar operation edges.

No import-time work, production imports, payload/heap traversal, process read,
GC, trim or sink on a business call. Shell bytes are not deep bytes or RSS.
"""
from array import array
from collections import Counter,OrderedDict,defaultdict,deque
from datetime import datetime,timezone
from functools import wraps
import _thread
import hashlib
import json
import math
import sys
import threading
import time
from types import ModuleType,GetSetDescriptorType,FunctionType

VERSION='rebound-memory-cheap-v1'
EXPIRES_AT_UTC='2026-11-01T22:00:00+00:00'
HISTORY_ROOT_NAMES=('history_reference_cache','history_input_receipts','flow_cache','flow_input_receipts')
RETAINED_ROOT_NAMES=('paper_checkpoint_loaded', 'liquidity_checkpoint_loaded', 'sheets_snapshot_cache', 'sheets_queue_payloads', 'reconciler_expected', 'reconciler_seen', 'reconciler_unexpected', 'past_price_lru', 'event_sink_events', 'event_writer_queue', 'consumed_inputs_queue', 'decision_store_queue', 'doge_sequence_queue', 'combined_shadow_queue', 'experimental_shared_prices', 'hype_price_rows', 'u21_price_rows', 'sol_g65_price_rows', 'sol_g65_legacy_rows', 'xrp_r2732_price_rows', 'xrp_r2732_legacy_rows', 'sol_proximity_price_rows', 'sol_proximity_legacy_rows', 'hype_proximity_price_rows', 'hype_proximity_legacy_rows', 'doge_proximity_price_rows', 'doge_proximity_legacy_rows', 'xrp_proximity_price_rows', 'xrp_proximity_legacy_rows', 'eth_proximity_price_rows', 'eth_proximity_legacy_rows', 'ai_conversation_messages')
ROOT_NAMES=HISTORY_ROOT_NAMES+RETAINED_ROOT_NAMES

def _known_dict_type(cls, name):
    if type(cls) is not type or cls.__bases__ != (dict,) or cls.__name__ != name:
        return False
    allowed = {'__module__','__doc__','__dict__','__weakref__','__slots__','__slotnames__'}
    fields=tuple(cls.__dict__)
    if any(type(field) is not str for field in fields) or not set(fields) <= allowed:
        return False
    for field in ('__dict__','__weakref__'):
        if field in cls.__dict__:
            descriptor=cls.__dict__[field]
            if (type(descriptor) is not GetSetDescriptorType or
                descriptor.__objclass__ is not cls or descriptor.__name__ != field):
                return False
    # Normal protocol5 pickle caches this exact empty built-in list on both
    # producer classes. It adds no lookup/sizing hook or traversable instance slot.
    if '__slotnames__' in cls.__dict__:
        slotnames = cls.__dict__['__slotnames__']
        if type(slotnames) is not list or len(slotnames) != 0:
            return False
    if name == '_ReferenceRanges':
        slots=cls.__dict__.get('__slots__')
        return type(slots) is tuple and len(slots)==1 and type(slots[0]) is str and slots[0]=='__weakref__'
    return name == '_CapturedPackage' and '__slots__' not in cls.__dict__

def _instance_dict(value):
    """Use only a native owned instance-dict descriptor, never a property."""
    cls=type(value)
    if type(cls) is not type:return None
    for base in cls.__mro__[:16]:
        if type(base) is not type:return None
        descriptor=base.__dict__.get('__dict__')
        if descriptor is None:continue
        if (type(descriptor) is not GetSetDescriptorType or
            descriptor.__objclass__ is not base or descriptor.__name__!='__dict__'):
            return None
        result=descriptor.__get__(value,cls)
        return result if type(result) is dict else None
    return None

def _field(value,name):
    namespace=_instance_dict(value)
    if namespace is None:raise ValueError('UNSAFE_ADAPTER')
    if name not in namespace:raise ValueError('NOT_READY')
    return dict.__getitem__(namespace,name)

def _loaded_module(name):
    value=sys.modules.get(name)
    if type(value) is not ModuleType:raise ValueError('NOT_LOADED')
    return value.__dict__

def _global(module,name):
    namespace=_loaded_module(module)
    if name not in namespace:raise ValueError('NOT_READY')
    return dict.__getitem__(namespace,name)

def _resolve_retained(name):
    """Return an actual named container plus existing locks; no method calls."""
    if name in ('paper_checkpoint_loaded','liquidity_checkpoint_loaded'):
        module='research_forward_paper_worker' if name=='paper_checkpoint_loaded' else 'research_liquidity_forward_worker'
        store=_field(_global(module,'WORKER'),'_store')
        if store is None:raise ValueError('NOT_READY')
        root=_field(store,'_loaded')
        if root is None:raise ValueError('NOT_READY')
        return root,()
    if name=='sheets_snapshot_cache':
        return _global('google_sheets_sync','_SNAPSHOT_CACHE'),(_global('google_sheets_sync','_SNAPSHOT_LOCK'),)
    if name=='sheets_queue_payloads':
        queue=_global('google_sheets_sync','_QUEUE')
        return _field(queue,'queue'),(_field(queue,'mutex'),)
    if name.startswith('reconciler_'):
        reconciler=_field(_global('research_snapshot_sync_worker','WORKER'),'_sheet_reconciler')
        root=_field(reconciler,{'reconciler_expected':'expected','reconciler_seen':'seen','reconciler_unexpected':'unexpected'}[name])
        if root is None:raise ValueError('NOT_READY')
        return root,()
    if name=='past_price_lru':return _global('research_past_price_features_worker','_CACHE'),()
    if name=='event_sink_events':return _field(_global('research_event_runtime','SINK'),'_events'),()
    if name=='event_writer_queue':
        writer=_global('research_event_store','WRITER')
        return _field(_field(writer,'queue'),'_queue'),()
    queues={'consumed_inputs_queue':'research_consumed_inputs','decision_store_queue':'research_alert_decision_store',
            'doge_sequence_queue':'research_doge_sequence_capture','combined_shadow_queue':'research_combined_liquidity_shadow'}
    if name in queues:
        queue=_global(queues[name],'_QUEUE')
        return _field(queue,'queue'),(_field(queue,'mutex'),)
    if name=='experimental_shared_prices':
        source=_global('experimental_hyperliquid_source','_SHARED_SOURCE')
        locks=_field(source,'_locks')
        if type(locks) is not dict:raise ValueError('UNSAFE_ADAPTER')
        return _field(source,'_rows'),tuple(dict.__getitem__(locks,symbol) for symbol in ('BTC','ETH','SOL','HYPE','DOGE','XRP'))
    workers={
        'hype_price_rows':('hype_row71205_experimental_worker','cache'),
        'u21_price_rows':('u21_experimental_worker','cache'),
        'sol_g65_price_rows':('sol_g65_experimental_worker','cache'),
        'sol_g65_legacy_rows':('sol_g65_experimental_worker','legacy_cache'),
        'xrp_r2732_price_rows':('xrp_r2732_experimental_worker','cache'),
        'xrp_r2732_legacy_rows':('xrp_r2732_experimental_worker','legacy_cache'),
        'sol_proximity_price_rows':('sol_proximity_experimental_worker','cache'),
        'sol_proximity_legacy_rows':('sol_proximity_experimental_worker','legacy_cache')}
    if name in workers:
        module,field=workers[name]
        return _field(_field(_global(module,'WORKER'),field),'rows'),()
    for coin in ('HYPE','DOGE','XRP','ETH'):
        prefix=coin.lower()+'_proximity_'
        if name in (prefix+'price_rows',prefix+'legacy_rows'):
            workers=_global('sol_proximity_experimental_worker','ADDITIONAL_WORKERS')
            if type(workers) is not dict:raise ValueError('UNSAFE_ADAPTER')
            if coin not in workers:raise ValueError('NOT_READY')
            worker=dict.__getitem__(workers,coin)
            return _field(_field(worker,'legacy_cache' if name.endswith('legacy_rows') else 'cache'),'rows'),()
    if name=='ai_conversation_messages':
        return _field(_field(_global('ai_agent','AGENT'),'memory'),'_messages'),()
    raise ValueError('UNSAFE_ADAPTER')

MAX_SECONDS=.05
MAX_WORKSPACE_BYTES=128*1024
MAX_CONTAINERS=1024
MAX_OUTPUT_BYTES=32768
MAX_ROUTE_ITEMS=64
_OUTPUT_AND_SCRATCH_RESERVE=96*1024
_COINS=('BTC','ETH','SOL','HYPE','DOGE','ZEC','BNB','XRP')
_WINDOWS=('30m','1h','4h','12h','24h','48h','72h','7d')
_CONTAINERS=(dict,OrderedDict,Counter,defaultdict,deque,list,tuple,set,frozenset)
_MISSING=object()
_OBSERVER=None
_INSTALL_USED=False
_INSTALL_LOCK=threading.Lock()


def _utc():return datetime.now(timezone.utc).isoformat()
def _expiry(now):
    value=datetime.fromisoformat(now.replace('Z','+00:00'))
    return value.utcoffset()is not None and value<datetime.fromisoformat(EXPIRES_AT_UTC)
def _kind(value):
    cls=type(value)
    if type(cls)is not type:return None
    for expected,label in ((dict,'dict'),(OrderedDict,'ordered_dict'),(Counter,'counter'),(defaultdict,'default_dict'),(deque,'deque'),(list,'list'),(tuple,'tuple'),(set,'set'),(frozenset,'frozenset')):
        if cls is expected:
            if cls is Counter and any(k in cls.__dict__ for k in ('__sizeof__','__len__')):return None
            return label
    for module,field,name in (('coinglass_history_backfill','_ReferenceRanges','_ReferenceRanges'),('research_consumed_inputs','_CapturedPackage','_CapturedPackage')):
        m=sys.modules.get(module)
        if type(m)is ModuleType and dict.get(m.__dict__,field)is cls and _known_dict_type(cls,name):return 'known_dict'
    return None

def _mapping(value):return _kind(value)in('dict','ordered_dict','counter','default_dict','known_dict')
def _get(value,key):
    if not _mapping(value):return _MISSING
    if len(value)>MAX_ROUTE_ITEMS:raise ValueError('ROUTE_ITEM_BOUND')
    # Never compare a foreign key or call its hash/equality hook.
    for old,child in dict.items(value):
        if type(old)is not str:raise ValueError('UNSAFE_ROUTE_KEY')
        if old==key:return child
    return _MISSING

def _values(value):
    if not _mapping(value)or len(value)>MAX_ROUTE_ITEMS:raise ValueError('ROUTE_ITEM_BOUND')
    for _key,child in dict.items(value):yield child

def _resolve(name):
    routes={'history_reference_cache':('coinglass_history_backfill','_REFERENCE_CACHE',None),'history_input_receipts':('coinglass_history_backfill','_REFERENCE_INPUT_RECEIPTS','_REFERENCE_INPUT_LOCK'),'flow_cache':('market_confidence_engine','_FLOW_CACHE',None),'flow_input_receipts':('market_confidence_engine','_FLOW_INPUT_RECEIPTS','_FLOW_INPUT_LOCK')}
    if name in routes:
        module,field,lock=routes[name]
        return _global(module,field),((_global(module,lock),)if lock else())
    return _resolve_retained(name)


def _route(name,root):
    """Fixed shallow routes; no series element, payload row or candle walk."""
    yield root
    def children(value,keys):
        if value is _MISSING or value is None:return
        if not _mapping(value):raise ValueError('UNSAFE_ROUTE_TYPE')
        for key in keys:
            child=_get(value,key)
            if child is not _MISSING and child is not None:
                if _kind(child)is None:raise ValueError('UNSAFE_ROUTE_TYPE')
                yield child
    if name=='history_reference_cache':
        for coin in _COINS:
            item=_get(root,coin)
            if item is _MISSING:continue
            yield item
            windows=_get(item,'windows')
            if windows is _MISSING:continue
            yield windows
            for label in _WINDOWS:
                window=_get(windows,label)
                if window is _MISSING:continue
                yield window
                for child in children(window,('price_composition_samples','oi_composition_samples')):
                    yield child
    elif name in('history_input_receipts','flow_input_receipts'):
        for receipt in _values(root):
            if type(receipt)is not tuple or len(receipt)!=5:raise ValueError('UNSAFE_RECEIPT_SHAPE')
            yield receipt
            package=receipt[1];yield package
            for child in children(package,('objects','events','errors')):yield child
    elif name=='flow_cache':
        for entry in _values(root):
            if type(entry)is not tuple or len(entry)!=2:raise ValueError('UNSAFE_FLOW_ENTRY')
            yield entry;result=entry[1];yield result
            for market in ('futures','spot'):
                part=_get(result,market)
                if part is _MISSING:continue
                yield part
                for child in children(part,('windows','quality','baseline')):yield child
    elif name in('paper_checkpoint_loaded','liquidity_checkpoint_loaded'):
        state=_get(root,'state')
        if state is not _MISSING:
            yield state
            for child in children(state,('bootstrap_event_keys','bootstrap_parent_ids','source_code_registries','first_seen','last_quotes','books')):yield child
            books=_get(state,'books')
            if books is not _MISSING:
                for book in _values(books):
                    yield book
                    for child in children(book,('choices','reservation','active','closed')):yield child
    elif name=='past_price_lru':
        for path in _values(root):
            yield path
            for child in children(path,('candles','archive_missing_ranges')):yield child
    elif name=='experimental_shared_prices'or name.endswith('_rows'):
        for coin in _COINS:
            rows=_get(root,coin)
            if rows is not _MISSING:
                if _kind(rows)is None:raise ValueError('UNSAFE_ROUTE_TYPE')
                yield rows


def capture_shells_and_operations(*,monotonic=time.monotonic,utc_now=None):
    """All36 root shells first, optional fixed nested routes second."""
    now=utc_now or _utc;start=monotonic();began=now();objects=[];sizes=array('Q');counts=array('Q');owners=array('Q');roots={};values={};initial={};digests={};codes=set();peak=[0];next_operation=[None]
    def workspace():return _OUTPUT_AND_SCRATCH_RESERVE+sum(sys.getsizeof(v)for v in(objects,sizes,counts,owners))
    def bounded():
        peak[0]=max(peak[0],workspace())
        if monotonic()-start>=MAX_SECONDS:raise ValueError('TIME_BUDGET')
        if workspace()+512>MAX_WORKSPACE_BYTES:raise ValueError('WORKSPACE_BOUND')
    def record_obj(obj,index):
        bounded()
        if _kind(obj)is None:raise ValueError('UNSAFE_ROUTE_TYPE')
        found=next((i for i,old in enumerate(objects)if old is obj),None)
        if found is None:
            if len(objects)>=MAX_CONTAINERS:raise ValueError('CONTAINER_BOUND')
            allocated=sum(sys.getsizeof(v)for v in(objects,sizes,counts,owners))
            prospective=workspace()+allocated+1024
            if prospective>MAX_WORKSPACE_BYTES:next_operation[0]=prospective;raise ValueError('WORKSPACE_BOUND')
            peak[0]=max(peak[0],prospective)
            objects.append(obj);sizes.append(sys.getsizeof(obj));counts.append(len(obj));owners.append(1<<index)
        else:owners[found]|=1<<index
    def walk(name,value,index,record):
        fingerprint=hashlib.blake2b(digest_size=16)
        for obj in _route(name,value):
            bounded()
            if _kind(obj)is None:raise ValueError('UNSAFE_ROUTE_TYPE')
            for number in(id(obj),len(obj),sys.getsizeof(obj)):fingerprint.update(number.to_bytes(8,'little',signed=False))
            if record:record_obj(obj,index)
        return fingerprint.digest()
    def fail(info,error):
        code=error.args[0]if isinstance(error,ValueError)and error.args and type(error.args[0])is str else'CAPTURE_ERROR'
        if code not in {'TIME_BUDGET','WORKSPACE_BOUND','CONTAINER_BOUND','NOT_LOADED','NOT_READY','LOCK_UNAVAILABLE','UNSAFE_ROOT_TYPE','UNSAFE_ROUTE_TYPE','UNSAFE_ROUTE_KEY','UNSAFE_RECEIPT_SHAPE','UNSAFE_FLOW_ENTRY','ROUTE_ITEM_BOUND','UNSAFE_ADAPTER','ROOT_REPLACED'}:code='CAPTURE_ERROR'
        codes.add(code)
        if info['status']!='CAPTURED':info['status']=code
    try:
        # Phase1 never enumerates a large root or its children.
        for index,name in enumerate(ROOT_NAMES):
            info=dict(status='NOT_LOADED',root_kind=None,entry_count=None,root_shallow_bytes=None,covered_containers=0,covered_shallow_bytes=0,covered_container_items=0,fixed_routes_complete=False,header_shapes_stable=None)
            roots[name]=info;held=[]
            try:
                bounded();value,locks=_resolve(name)
                for lock in locks:
                    if type(lock)is not _thread.LockType or not lock.acquire(False):raise ValueError('LOCK_UNAVAILABLE')
                    held.append(lock)
                if _kind(value)is None:raise ValueError('UNSAFE_ROOT_TYPE')
                record_obj(value,index);values[name]=value;initial[name]=(len(value),sys.getsizeof(value))
                info.update(status='CAPTURED',root_kind=_kind(value),entry_count=initial[name][0],root_shallow_bytes=initial[name][1])
            except BaseException as error:fail(info,error)
            finally:
                for lock in reversed(held):lock.release()
        # Phase2 may be partial without starving the later root headers.
        exhausted=False
        for index,name in enumerate(ROOT_NAMES):
            if name not in values or exhausted:continue
            held=[]
            try:
                bounded();value,locks=_resolve(name)
                if value is not values[name]:raise ValueError('ROOT_REPLACED')
                for lock in locks:
                    if type(lock)is not _thread.LockType or not lock.acquire(False):raise ValueError('LOCK_UNAVAILABLE')
                    held.append(lock)
                digests[name]=walk(name,value,index,True);roots[name]['fixed_routes_complete']=True
            except BaseException as error:
                fail(roots[name],error)
                exhausted=bool(codes&{'TIME_BUDGET','WORKSPACE_BOUND','CONTAINER_BOUND'})
            finally:
                for lock in reversed(held):lock.release()
        # Only revalidate the fixed routes actually completed; no new pins.
        for index,name in enumerate(ROOT_NAMES):
            info=roots[name]
            if name in values:
                try:
                    bounded();value,_locks=_resolve(name)
                    stable=value is values[name]and _kind(value)is not None and(len(value),sys.getsizeof(value))==initial[name]
                    if name in digests and stable:stable=walk(name,value,index,False)==digests[name]
                    info['header_shapes_stable']=stable
                    if not stable:codes.add('CONCURRENT_OR_UNVERIFIED')
                except BaseException:info['header_shapes_stable']=None;codes.add('RECHECK_UNAVAILABLE')
            for i,mask in enumerate(owners):
                if mask&(1<<index):
                    info['covered_containers']+=1;info['covered_shallow_bytes']+=sizes[i];info['covered_container_items']+=counts[i]
        complete=not codes and all(v['status']=='CAPTURED'and v['fixed_routes_complete']and v['header_shapes_stable']is True for v in roots.values())
        result=dict(version=VERSION,status='COMPLETE'if complete else'PARTIAL'if objects else'UNAVAILABLE',started_at_utc=began,completed_at_utc=now(),elapsed_ms=round(max(0.,monotonic()-start)*1000,3),declared_root_names=list(ROOT_NAMES),roots=roots,reason_codes=sorted(codes),covered_union_containers=len(objects),covered_union_shallow_bytes=sum(sizes),projection_complete=complete,full_graph_measured=False,deep_bytes_total=None,operations=_safe_operation_status(),workspace_accounted_bytes=workspace(),workspace_peak_accounted_bytes=peak[0],workspace_next_operation_bytes=next_operation[0],workspace_scope='96K_RESERVED_OUTPUT_JSON_AND_FIXED_SCRATCH_PLUS_NATIVE_LIST_ARRAY_CAPACITIES_ACCEPTED_RESIZE_OVERLAP_AND_1024B_TRANSIENTS',limits=dict(elapsed_ms=50,workspace_bytes=MAX_WORKSPACE_BYTES,containers=MAX_CONTAINERS,output_bytes=MAX_OUTPUT_BYTES,route_items=64),scope='FIXED_CONTAINER_SHELL_ALLOCATED_SLOTS_AND_CARDINALITIES_NOT_REFERENTS_DEEP_UNION_RSS_OR_ALLOCATION_OWNER',privacy=dict(values_or_addresses_persisted=False,worker_frames_inspected=False,source_imports=False),observer_effect='All36root headers precede optional nested routes. Covered containers are pinned only through this call. Shell sizes include allocated slots; History distributions/scalar leaves/data rows/candles/native instance attributes are excluded. Same-size changes outside fixed routes are not detected; no atomic snapshot or resident-byte attribution.')
        raw=json.dumps(result,separators=(',',':'),allow_nan=False).encode()
        if len(raw)>MAX_OUTPUT_BYTES:raise ValueError('OUTPUT_BOUND')
        return result
    except BaseException:
        return dict(version=VERSION,status='UNAVAILABLE',reason_codes=['CHEAP_CAPTURE_BOUND_OR_EXCEPTION'],roots={},declared_root_names=list(ROOT_NAMES),full_graph_measured=False,deep_bytes_total=None,operations=_safe_operation_status(),limits=dict(elapsed_ms=50,workspace_bytes=MAX_WORKSPACE_BYTES,containers=MAX_CONTAINERS,output_bytes=MAX_OUTPUT_BYTES,route_items=64))


OPERATIONS=(('OI_COLLECT','coinglass_oi_regime_service',None,'collect_many'),('OI_LATEST','coinglass_oi_regime_service',None,'latest'),('FLOW_DOWNLOAD','coinglass_flow_foundation',None,'backfill_all'),('FLOW_ANALYZE','coinglass_flow_engine',None,'analyze_symbol'),('HISTORY_BUILD','coinglass_history_backfill',None,'calculate_reference_ranges'),('HISTORY_WINDOW','coinglass_history_backfill',None,'reference_for_window'),('CHECKPOINT','research_forward_paper_store','PostgresCheckpoint','checkpoint'),('SNAPSHOT_SYNC','research_snapshot_sync_worker','SnapshotSyncWorker','run_once'))
_OPERATION_NAMES=tuple(row[0]for row in OPERATIONS)
_MAX_INT=(1<<63)-1

def _row():return dict(started=0,completed=0,exceptions=0,active=0,max_active=0,total_us=0,max_us=0,block_delta_min=None,block_delta_max=None,first_started_at_utc=None,last_completed_at_utc=None)
def _target(module,cls,field):
    namespace=_loaded_module(module)
    if cls is None:return namespace,dict.get(namespace,field),False
    klass=dict.get(namespace,cls)
    if type(klass)is not type:raise ValueError('UNSAFE_TARGET_CLASS')
    return klass,klass.__dict__.get(field),True

def _set_target(target,field,value,isclass):
    if isclass:type.__setattr__(target,field,value)
    else:dict.__setitem__(target,field,value)


class OperationObserver:
    def __init__(self,*,deadline_monotonic,monotonic=time.monotonic,utc_now=None,blocks=None):
        self.clock=monotonic;self.now=utc_now or _utc;self.blocks=blocks or sys.getallocatedblocks
        self.start=self.clock();self.deadline=deadline_monotonic;self.lock=threading.Lock();self.state='ACTIVE';self.bindings=[];self.presence={};self.totals={n:_row()for n in _OPERATION_NAMES};self.buckets=OrderedDict();self.dropped=0;self.overflow=False;self.active=0;self.ended_inflight=0;self.evicted_buckets=0
    def stamp(self):
        value=self.now()
        if type(value)is not str or len(value)>64 or not _expiry(value):raise ValueError('INVALID_OR_EXPIRED_UTC')
        return value
    def valid(self):
        try:return self.state=='ACTIVE'and self.clock()<self.deadline and bool(self.stamp())
        except BaseException:return False
    def _drop(self):self.dropped=min(_MAX_INT,self.dropped+1)
    def _add(self,row,key,value=1):
        new=row[key]+value
        if new>_MAX_INT:self.overflow=True;new=_MAX_INT
        row[key]=new
    def _bucket(self,tick):
        index=max(0,int((tick-self.start)//15))
        if index not in self.buckets:
            self.buckets[index]={}
            while len(self.buckets)>4:self.buckets.popitem(last=False);self.evicted_buckets+=1
        return self.buckets[index]
    def begin(self,name):
        try:
            if not self.valid():return None
            tick=self.clock();stamp=self.stamp();blocks=self.blocks()
            if type(blocks)is not int or not 0<=blocks<=_MAX_INT:blocks=None
            if not self.lock.acquire(False):self._drop();return None
            try:
                if self.active>=64:self._drop();return None
                row=self.totals[name];self._add(row,'started');self._add(row,'active');self.active+=1;row['max_active']=max(row['max_active'],row['active']);row['first_started_at_utc']=row['first_started_at_utc']or stamp
                bucket=self._bucket(tick).setdefault(name,dict(started=0,completed=0,exceptions=0,total_us=0,spanning=0));self._add(bucket,'started')
                return tick,stamp,blocks,int((tick-self.start)//15)
            finally:self.lock.release()
        except BaseException:self._drop();return None
    def finish(self,name,token,failed):
        if token is None:return
        try:
            tick=self.clock();stamp=self.stamp();blocks=self.blocks()
            if not self.lock.acquire(False):self._drop();return
            try:
                row=self.totals[name];row['active']=max(0,row['active']-1);self.active=max(0,self.active-1)
                if not self.valid():self.ended_inflight+=1;return
                elapsed=max(0,int((tick-token[0])*1e6))
                if elapsed>_MAX_INT:self.overflow=True;elapsed=_MAX_INT
                self._add(row,'completed');self._add(row,'exceptions',int(failed));self._add(row,'total_us',elapsed);row['max_us']=max(row['max_us'],elapsed);row['last_completed_at_utc']=stamp
                if type(blocks)is int and 0<=blocks<=_MAX_INT and token[2]is not None:
                    delta=blocks-token[2];row['block_delta_min']=delta if row['block_delta_min']is None else min(row['block_delta_min'],delta);row['block_delta_max']=delta if row['block_delta_max']is None else max(row['block_delta_max'],delta)
                bucket=self._bucket(tick).setdefault(name,dict(started=0,completed=0,exceptions=0,total_us=0,spanning=0));self._add(bucket,'completed');self._add(bucket,'exceptions',int(failed));self._add(bucket,'total_us',elapsed);self._add(bucket,'spanning',int(int((tick-self.start)//15)!=token[3]))
            finally:self.lock.release()
        except BaseException:self._drop()
    def wrap(self,name,original):
        @wraps(original,updated=())
        def observed(*args,**kwargs):
            try:token=self.begin(name)
            except BaseException:token=None
            failed=False
            try:return original(*args,**kwargs)
            except BaseException:failed=True;raise
            finally:
                try:self.finish(name,token,failed)
                except BaseException:pass
        return observed
    def install(self):
        for name,module,cls,field in OPERATIONS:
            try:
                target,original,isclass=_target(module,cls,field)
                if type(original)is not FunctionType or original.__code__.co_flags&(0x20|0x80|0x200):raise ValueError('NOT_SYNC_FUNCTION')
                wrapped=self.wrap(name,original);_set_target(target,field,wrapped,isclass);self.bindings.append((target,field,original,wrapped,isclass));self.presence[name]='INSTALLED'
            except BaseException:self.presence[name]='NOT_LOADED_OR_UNSAFE'
        return self.status()
    def stop(self):
        self.state='STOPPED'
        for target,field,original,wrapped,isclass in self.bindings:
            try:
                current=target.__dict__.get(field)if isclass else dict.get(target,field)
                if current is wrapped:_set_target(target,field,original,isclass)
                else:self._drop()
            except BaseException:self._drop()
        self.bindings.clear();return self.status()
    def status(self):
        if not self.lock.acquire(False):return dict(status='UNAVAILABLE',reason='COUNTER_LOCK_BUSY',coverage_complete=False)
        try:return dict(version='rebound-operation-scalars-v1',status=self.state if self.valid()or self.state=='STOPPED'else'EXPIRED',presence=dict(self.presence),totals={k:dict(v)for k,v in self.totals.items()},buckets=[dict(index=k,operations={n:dict(v)for n,v in values.items()})for k,values in self.buckets.items()],dropped_updates=self.dropped,overflow=self.overflow,active_tracked_calls=self.active,completed_after_window=self.ended_inflight,evicted_buckets=self.evicted_buckets,retained_bucket_coverage_complete=self.evicted_buckets==0,coverage_complete=False,tracked_edge_coverage_complete=self.dropped==0 and not self.overflow,pre_install_inflight_unknown=True,per_operation_rss_measured=False,scope='SYNC_INCLUSIVE_WALL_TIMES_AND_PROCESS_WIDE_BLOCK_ENDPOINTS_NOT_EXCLUSIVE_CPU_ALLOCATIONS_OR_RSS_OWNERSHIP',limits=dict(operations=8,active_calls=64,buckets=4,bucket_seconds=15),payload_values_or_exceptions_retained=False)
        finally:self.lock.release()


def install_operation_observer(*,deadline_monotonic,monotonic=time.monotonic,utc_now=None):
    global _OBSERVER,_INSTALL_USED
    with _INSTALL_LOCK:
        if _INSTALL_USED:return dict(status='NOT_REARMED',reason='INSTALL_ALREADY_USED')
        _INSTALL_USED=True
        try:
            if not(type(deadline_monotonic)is int or type(deadline_monotonic)is float)or not math.isfinite(deadline_monotonic)or not monotonic()<deadline_monotonic<=monotonic()+5400:raise ValueError('INVALID_DEADLINE')
            _OBSERVER=OperationObserver(deadline_monotonic=deadline_monotonic,monotonic=monotonic,utc_now=utc_now)
            if not _OBSERVER.valid():raise ValueError('EXPIRED')
            return _OBSERVER.install()
        except BaseException:return dict(status='UNAVAILABLE',reason='INSTALL_FAILED_NO_REARM')
def stop_operation_observer():return _OBSERVER.stop()if _OBSERVER is not None else dict(status='INACTIVE')
def operation_status():return _OBSERVER.status()if _OBSERVER is not None else dict(status='INACTIVE',coverage_complete=False)


def _safe_operation_status():
    try:
        result=operation_status()
        if len(json.dumps(result,separators=(',',':'),allow_nan=False).encode())>16384:raise ValueError('COUNTER_STATUS_BOUND')
        return result
    except BaseException:return dict(status='UNAVAILABLE',reason='COUNTER_STATUS_EXCEPTION_OR_BOUND',coverage_complete=False)
