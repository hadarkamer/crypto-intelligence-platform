"""Bounded named-cache observations. No import-time probes or producer imports.

Counts unique shallow sys.getsizeof bytes of supported reachable Python objects.
It is not RSS, allocator usage, native memory, exclusive ownership or a heap walk.
The caller must enforce idle-interval/RSS/absolute-expiry admission separately.
"""
from collections import OrderedDict, Counter, defaultdict, deque
from datetime import datetime, timezone
from itertools import islice
import _thread
import sys
import time
from types import ModuleType, GetSetDescriptorType
import weakref

VERSION = 'current-history-supported-memory-pooled-v1'
HISTORY_ROOT_NAMES = ('history_reference_cache', 'history_input_receipts', 'flow_cache', 'flow_input_receipts')
ROOT_NAMES = HISTORY_ROOT_NAMES
HISTORY_MAX_ROOT_ITEMS = 64
MAX_ROOT_ITEMS = 64
MAX_NODES = 4_000_000
MAX_WORKSPACE_BYTES = 256 * 1024 * 1024
MAX_SECONDS = 30.0
YIELD_EVERY = 2048
MAX_EDGE_VISITS = 16_000_000
MAX_STACK_DEPTH = 128
_AUXILIARY_ALLOWANCE_BYTES = 2 * 1024 * 1024
_SCALARS = {str:'str',bytes:'bytes',int:'int',float:'float',bool:'bool',type(None):'none',weakref.ReferenceType:'weakref',datetime:'datetime_leaf'}
_CONTAINERS = {dict:'dict',list:'list',tuple:'tuple',set:'set',frozenset:'frozenset',OrderedDict:'ordered_dict',Counter:'counter',defaultdict:'default_dict',deque:'deque'}

MAX_OWNER_MASK_PATTERNS = 16
OWNER_MASK_MODE = 'LOCAL_CANONICAL_EXACT_INT_POOL_V1'
_MAX_OWNER_MASK = (1 << len(ROOT_NAMES)) - 1


class _OwnerMaskPool:
    """Per-call exact-int canonical owners; no payload references or registry.

    Keys and values are the same canonical object. The transient headroom
    supplied by the caller already includes three maximum-size mask integers.
    A new pattern reserves a conservative old/new dict-table overlap before
    insertion, not merely its eventual net capacity growth.
    """
    __slots__ = ('values', 'value_bytes', 'capacity_bytes', 'resize_reserve_peak',
                 'last_reserve_bytes', 'max_patterns', '__weakref__')

    def __init__(self):
        self.values = {}
        self.value_bytes = 0
        self.capacity_bytes = sys.getsizeof(self.values)
        self.resize_reserve_peak = 0
        self.last_reserve_bytes = 0
        self.max_patterns = MAX_OWNER_MASK_PATTERNS

    def intern(self, value, available_headroom):
        if type(value) is not int or not 0 < value <= _MAX_OWNER_MASK:
            raise ValueError('OWNER_MASK_INVALID')
        existing = self.values.get(value)
        if existing is not None:
            self.last_reserve_bytes = 0
            return existing
        if len(self.values) >= self.max_patterns:
            self.last_reserve_bytes = 0
            raise ValueError('OWNER_MASK_BOUND')
        # Same conservative CPython dictionary-growth model as the memo shards:
        # current table is already counted; reserve three current capacities
        # plus fixed scratch for old/new table overlap before any insertion.
        reserve = 3 * self.capacity_bytes + 512 + sys.getsizeof(value)
        self.last_reserve_bytes = reserve
        if reserve > available_headroom:
            raise ValueError('WORKSPACE_BOUND')
        self.resize_reserve_peak = max(self.resize_reserve_peak, reserve)
        self.values[value] = value
        self.value_bytes += sys.getsizeof(value)
        self.capacity_bytes = sys.getsizeof(self.values)
        return value



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


def _root_item_limit(name):
    return HISTORY_MAX_ROOT_ITEMS if name in HISTORY_ROOT_NAMES else MAX_ROOT_ITEMS


def _root_supported(name,value):
    cls=type(value)
    if name in HISTORY_ROOT_NAMES:return cls is dict or cls is OrderedDict
    return any(cls is allowed for allowed in _CONTAINERS)


def _shape(value,expected_count=None,max_items=MAX_ROOT_ITEMS):
    cls=type(value)
    if not any(cls is allowed for allowed in (dict,OrderedDict,Counter,defaultdict,deque,list,tuple,set,frozenset)):
        raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
    count=len(value)
    if expected_count is not None and count!=expected_count:raise ValueError('ROOT_CHANGED')
    if count>max_items:raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
    limit=count*2+1
    if cls in (dict,OrderedDict,Counter,defaultdict):
        children=tuple(islice((child for pair in dict.items(value) for child in pair),limit+1))
    else:children=tuple(islice(value,limit+1))
    if len(children)>limit:raise ValueError('ROOT_CHANGED')
    if cls is Counter or cls is OrderedDict:
        attributes=_instance_dict(value)
        if attributes is None:raise ValueError('UNSAFE_COUNTER_ATTRIBUTES')
        children=children+(attributes,)
    if len(children)>max_items*2+1:raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
    signature=(id(value),count,sys.getsizeof(value),tuple(id(child) for child in children))
    return signature,children


def _children(value, kind):
    if kind in ('dict','counter','default_dict','reference_ranges','captured_package'):
        for key, child in dict.items(value):
            yield key
            yield child
        if kind == 'captured_package':
            yield object.__getattribute__(value,'__dict__')
        if kind == 'counter':
            attributes=_instance_dict(value)
            if attributes is None:raise ValueError('UNSAFE_COUNTER_ATTRIBUTES')
            yield attributes
    elif kind == 'ordered_dict':
        for key, child in OrderedDict.items(value):
            yield key
            yield child
        attributes=_instance_dict(value)
        if attributes is None:raise ValueError('UNSAFE_ORDERED_ATTRIBUTES')
        yield attributes
    elif kind in ('research_event','spot_candle'):
        yield _instance_dict(value)
    else:
        yield from value


def _base(status, codes):
    return {'version':VERSION,'status':status,'reason_codes':sorted(set(codes)),
        'measurement_valid':False,'complete':False,'atomic':False,
        'scope':'NAMED_SUPPORTED_PYTHON_GRAPH_SHALLOW_GETSIZEOF_NOT_RSS_NATIVE_ALLOCATOR_OR_EXCLUSIVE_OWNERSHIP',
        'observation_scope':'CURRENT_PROCESS_HISTORY_FLOW4_ONLY_ADDITIONAL32_NOT_REVISITED_NO_CROSS_PROCESS_SUM',
        'weak_references_followed':False,'object_values_or_addresses_persisted':False,
        'observer_effect':'First-seen objects are temporarily pinned through the probe to prevent id reuse. Memo tables and pins allocate temporary memory. Accessing native Counter/OrderedDict instance dictionaries may materialize an empty dict that is then counted. No owner or RSS-effect subtraction is inferred.',
        'caller_gate_required':'NO_ACTIVE_WATCH_AND_LOW_RSS_AND_UNEXPIRED_DIAGNOSTIC_WINDOW'}


def measure_named_roots(roots, *, reference_ranges_type=None, captured_package_type=None, research_event_type=None, spot_candle_type=None,
                        max_nodes=MAX_NODES, max_workspace_bytes=MAX_WORKSPACE_BYTES,
                        max_elapsed_seconds=MAX_SECONDS, yield_every=YIELD_EVERY,
                        clock=None, yield_fn=None, _snapshots=None):
    """Return a compact primitive-only receipt; never retain graph references.

    COMPLETE means the supported graph traversal finished within these bounds,
    with no *observed* mutation. It does not make a racing live graph atomic.
    Partial covered_bytes are not a total-cache estimate. CONCURRENT invalidates
    the observation rather than labelling a mixed generation a valid total.
    """
    started = None
    try:
        if (type(roots) is not dict or not roots or any(type(name) is not str for name in roots) or not set(roots) <= set(ROOT_NAMES) or
            type(max_nodes) is not int or not 1 <= max_nodes <= MAX_NODES or
            type(max_workspace_bytes) is not int or not 1024 <= max_workspace_bytes <= MAX_WORKSPACE_BYTES or
            type(max_elapsed_seconds) not in (int,float) or not 0 < max_elapsed_seconds <= MAX_SECONDS or
            type(yield_every) is not int or not 1 <= yield_every <= YIELD_EVERY):
            return _base('UNAVAILABLE',['INVALID_OPTIONS_OR_ROOT_NAMES'])
        names=tuple(name for name in ROOT_NAMES if name in roots)
        known = {}
        for cls,name,kind in ((reference_ranges_type,'_ReferenceRanges','reference_ranges'),
                              (captured_package_type,'_CapturedPackage','captured_package')):
            # Admission is root-dependent: an unrelated loaded class must not
            # veto another root. Check its full safe shape only if encountered.
            if type(cls) is type:
                known[cls] = (name,kind)
        clock = time.monotonic if clock is None else clock
        yield_fn = (lambda: time.sleep(0)) if yield_fn is None else yield_fn
        started = clock()
        snapshots = {} if _snapshots is None else _snapshots
        # Enforce each fixed root's own cap even for internal supplied snapshots.
        for name in names:
            if not _root_supported(name,roots[name]) or len(roots[name])>_root_item_limit(name):
                return _base('UNAVAILABLE',['ROOT_TYPE_OR_ITEM_BOUND'])
        if not snapshots:
            reservations={}
            for name in names:
                size=len(roots[name]) if any(type(roots[name]) is cls for cls in _CONTAINERS) else MAX_ROOT_ITEMS+1
                if size>_root_item_limit(name):return _base('UNAVAILABLE',['ROOT_TYPE_OR_ITEM_BOUND'])
                reservations[name]=size*128+4096
            if _AUXILIARY_ALLOWANCE_BYTES+sum(reservations.values())+2*max(reservations.values())>max_workspace_bytes:
                return _base('UNAVAILABLE',['SNAPSHOT_WORKSPACE_BOUND'])
            for name in names:snapshots[name]=_shape(roots[name],max_items=_root_item_limit(name))
        initial_shapes={name:snapshots[name][0] for name in names}
        root_children={id(roots[name]):snapshots[name][1] for name in names}
        # Shards bound one memo-table resize; no per-object tuple/size record.
        memo = [{} for _ in range(256)]
        # Prevent address reuse even if another thread removes a nested object.
        # Pins exist only for this walk and are released before the call returns.
        keepalive=[]
        keepalive_size=sys.getsizeof(keepalive)
        shard_sizes = [sys.getsizeof(part) for part in memo]
        memo_size = sys.getsizeof(memo) + sum(shard_sizes)
        id_size = 0
        snapshot_size=sys.getsizeof(snapshots)+sys.getsizeof(root_children)
        for name in names:
            signature,children=snapshots[name]
            snapshot_size+=sys.getsizeof(snapshots[name])+sys.getsizeof(signature)+sys.getsizeof(children)+sys.getsizeof(signature[3])
            snapshot_size+=sum(sys.getsizeof(identity) for identity in signature[3])
        owner_masks=_OwnerMaskPool()
        mask_state_bytes=sys.getsizeof(owner_masks)
        # Three transient maximum4-bit integers plus bounded call scratch.
        mask_int_transient_bytes=3*sys.getsizeof(_MAX_OWNER_MASK)
        mask_operation_scratch_bytes=256
        mask_temporary_bytes=mask_int_transient_bytes+mask_operation_scratch_bytes
        memo_resize_reserve_peak=0
        next_operation_bound=None
        revalidation_reserve=2*max(initial_shapes[name][1]*128+4096 for name in names)
        snapshot_size+=revalidation_reserve
        stats = {name:{'inclusive_unique_bytes':0,'unique_objects':0,'root_items':initial_shapes[name][1],
                       'complete':False,'measurement_valid':False,'observed_mutation':False,
                       'root_shape_stable':None,'type_counts':{},'type_bytes':{}} for name in names}
        pairwise = {names[i]+'|'+names[j]:0 for i in range(len(names)) for j in range(i+1,len(names))}
        union_bytes=union_objects=shared_bytes=shared_objects=visits=unsupported=0
        # Bounded128 frames/iterators, root signatures, type and result counters.
        # This conservative allowance avoids walking the workspace per graph node.
        snapshot_size += _AUXILIARY_ALLOWANCE_BYTES
        workspace_peak=(memo_size+snapshot_size+keepalive_size+mask_state_bytes+
                        owner_masks.capacity_bytes+owner_masks.value_bytes+mask_temporary_bytes)
        if workspace_peak>max_workspace_bytes:return _base('UNAVAILABLE',['SNAPSHOT_WORKSPACE_BOUND'])
        codes=[]; concurrent=False; stop=False; stack=[]
        changed_roots=set()
        def mark_changed(value, current_name):
            identity=id(value)
            mask=memo[(identity>>4)&255].get(identity,0)
            changed_roots.add(current_name)
            for index,root_name in enumerate(names):
                if mask & (1<<index): changed_roots.add(root_name)
        def workspace():
            return (memo_size+id_size+snapshot_size+keepalive_size+mask_state_bytes+
                    owner_masks.capacity_bytes+owner_masks.value_bytes+mask_temporary_bytes)
        def expired():
            elapsed=clock()-started
            if elapsed < 0: raise ValueError('CLOCK_BACKWARDS')
            return elapsed >= max_elapsed_seconds
        def intern_mask(value):
            nonlocal stop,workspace_peak,next_operation_bound
            before=workspace()
            try:
                canonical=owner_masks.intern(value,max_workspace_bytes-before)
            except ValueError as error:
                code=error.args[0] if error.args else 'OWNER_MASK_INVALID'
                codes.append(code);stop=True
                if code=='WORKSPACE_BOUND':next_operation_bound=before+owner_masks.last_reserve_bytes
                return None
            # Include the accepted prospective resize bound in the peak;
            # a refused insertion is not presented as an allocated peak.
            workspace_peak=max(workspace_peak,before+owner_masks.last_reserve_bytes,workspace())
            return canonical
        for root_index,name in enumerate(names):
            if stop: break
            bit=intern_mask(1<<root_index)
            if bit is None:break
            unsupported_before=unsupported
            stack=[['pending',roots[name]]]
            while stack:
                if visits % 256 == 0 and expired(): codes.append('TIME_BOUND'); stop=True; break
                frame=stack[-1]
                if frame[0]=='iter':
                    try:
                        child=next(frame[2])
                    except StopIteration:
                        if len(frame[1]) != frame[3] or sys.getsizeof(frame[1]) != frame[4]:
                            concurrent=True; mark_changed(frame[1],name); codes.append('CONTAINER_CHANGED')
                        stack.pop(); continue
                    except RuntimeError:
                        concurrent=True; mark_changed(frame[1],name); codes.append('CONTAINER_CHANGED'); stop=True; break
                    if len(stack)>=MAX_STACK_DEPTH: codes.append('DEPTH_BOUND'); stop=True; break
                    stack.append(['pending',child]); continue
                value=frame[1]; stack.pop(); visits+=1
                if visits>MAX_EDGE_VISITS: codes.append('EDGE_BOUND'); stop=True; break
                if visits % yield_every == 0:
                    yield_fn()
                    if expired(): codes.append('TIME_BOUND'); stop=True; break
                cls=type(value)
                # A foreign metaclass can override class hashing/equality.
                # Reject it before any class-keyed lookup or object sizing.
                if type(cls) is not type:
                    unsupported+=1; codes.append('UNSUPPORTED_TYPE'); continue
                kind=_SCALARS.get(cls) or _CONTAINERS.get(cls)
                if kind is None and cls is research_event_type and _safe_event_type(cls):kind='research_event'
                if kind is None and cls is spot_candle_type and _safe_spot_candle_type(cls):kind='spot_candle'
                if kind is None and cls in known:
                    known_name,known_kind=known[cls]
                    if _known_dict_type(cls,known_name):
                        kind=known_kind
                    else:
                        unsupported+=1; codes.append('UNSAFE_KNOWN_DICT_TYPE'); continue
                if kind is None:
                    unsupported+=1; codes.append('UNSUPPORTED_TYPE'); continue
                identity=id(value); shard_index=(identity>>4)&255; shard=memo[shard_index]; previous=shard.get(identity,0)
                if previous & bit: continue
                size=sys.getsizeof(value)
                if not previous:
                    if union_objects >= max_nodes: codes.append('NODE_BOUND'); stop=True; break
                    # Conservative room for a single shard resize before insert.
                    resize_reserve=3*shard_sizes[shard_index]+512
                    possible=workspace()+resize_reserve+sys.getsizeof(identity)+keepalive_size//4+256
                    if possible > max_workspace_bytes:
                        next_operation_bound=possible;codes.append('WORKSPACE_BOUND');stop=True;break
                    memo_resize_reserve_peak=max(memo_resize_reserve_peak,resize_reserve)
                    workspace_peak=max(workspace_peak,possible)
                    before_size=shard_sizes[shard_index]; shard[identity]=bit
                    keepalive.append(value); keepalive_size=sys.getsizeof(keepalive)
                    after_size=sys.getsizeof(shard); shard_sizes[shard_index]=after_size
                    memo_size+=after_size-before_size; id_size+=sys.getsizeof(identity)
                    union_objects+=1; union_bytes+=size
                else:
                    combined=intern_mask(previous|bit)
                    if combined is None:break
                    shard[identity]=combined
                    if previous & (previous-1)==0: shared_objects+=1; shared_bytes+=size
                    for earlier in range(root_index):
                        if previous & (1<<earlier): pairwise[names[earlier]+'|'+name]+=size
                current=stats[name]; current['unique_objects']+=1; current['inclusive_unique_bytes']+=size
                current['type_counts'][kind]=current['type_counts'].get(kind,0)+1
                current['type_bytes'][kind]=current['type_bytes'].get(kind,0)+size
                if cls not in _SCALARS:
                    children=iter(root_children[identity]) if identity in root_children else _children(value,kind)
                    container=_instance_dict(value) if kind in ('research_event','spot_candle') else value
                    stack.append(['iter',container,children,len(container),sys.getsizeof(container)])
                used=workspace();workspace_peak=max(workspace_peak,used)
                if used > max_workspace_bytes: codes.append('WORKSPACE_BOUND'); stop=True; break
            stats[name]['complete']=not stop and unsupported==unsupported_before and name not in changed_roots
        # Re-read roots only; no addresses/fingerprints enter the receipt.
        stable=True
        for name in names:
            try:
                value=roots[name]
                if len(value)!=initial_shapes[name][1] or sys.getsizeof(value)!=initial_shapes[name][2]:
                    stable=False;changed_roots.add(name)
                else:
                    shape,_items=_shape(value,expected_count=initial_shapes[name][1],max_items=_root_item_limit(name))
                    if shape != initial_shapes[name]:stable=False;changed_roots.add(name)
            except BaseException:
                stable=False; changed_roots.add(name)
            stats[name]['root_shape_stable']=name not in changed_roots
            stats[name]['observed_mutation']=name in changed_roots
            stats[name]['measurement_valid']=name not in changed_roots
            if name in changed_roots: stats[name]['complete']=False
        if not stable: concurrent=True; codes.append('ROOT_CHANGED')
        elapsed=max(0.,clock()-started)
        complete=not stop and not unsupported and not concurrent and all(item['complete'] for item in stats.values())
        result=_base('CONCURRENT' if concurrent else ('COMPLETE' if complete else 'PARTIAL'),codes)
        result.update(measurement_valid=not concurrent,complete=complete,root_shapes_stable=stable,available_root_names=list(names),declared_root_names=list(ROOT_NAMES),
            native_leaf_scope='Exact datetime shallow bytes only; tzinfo references/native backing excluded. defaultdict factory and container implementation backing are not traversed.',
            consistency='BEST_EFFORT_NONATOMIC_OBSERVED_ROOT_AND_CONTAINER_SIZE_CHECKS',
            covered_union_bytes=union_bytes,covered_unique_objects=union_objects,
            covered_shared_unique_bytes=shared_bytes,covered_shared_unique_objects=shared_objects,
            inclusive_bytes_sum=sum(item['inclusive_unique_bytes'] for item in stats.values()),
            repeated_reachable_bytes=sum(item['inclusive_unique_bytes'] for item in stats.values())-union_bytes,
            roots=stats,pairwise_overlap_bytes=pairwise,unsupported_visits=unsupported,
            traversal_visits=visits,elapsed_ms=round(elapsed*1000,3),workspace_peak_counted_bytes=workspace_peak,
            workspace_owner_mask_mode=OWNER_MASK_MODE,
            workspace_mask_pool_count=len(owner_masks.values),
            workspace_mask_pool_max_patterns=owner_masks.max_patterns,
            workspace_mask_pool_value_bytes=owner_masks.value_bytes,
            workspace_mask_pool_dict_capacity_bytes=owner_masks.capacity_bytes,
            workspace_mask_pool_state_bytes=mask_state_bytes,
            workspace_mask_pool_resize_reserve_peak_bytes=owner_masks.resize_reserve_peak,
            workspace_mask_pool_last_reserve_bytes=owner_masks.last_reserve_bytes,
            workspace_mask_int_transient_reserve_bytes=mask_int_transient_bytes,
            workspace_mask_operation_scratch_bytes=mask_operation_scratch_bytes,
            workspace_mask_temporary_reserve_bytes=mask_temporary_bytes,
            workspace_mask_pool_accounted_bytes=(owner_masks.capacity_bytes+owner_masks.value_bytes+mask_state_bytes+mask_temporary_bytes),
            workspace_memo_resize_reserve_peak_bytes=memo_resize_reserve_peak,
            workspace_next_operation_counted_upper_bound_bytes=next_operation_bound,
            workspace_revalidation_reserve_bytes=revalidation_reserve,
            limits={'unique_nodes':max_nodes,'workspace_counted_bytes':max_workspace_bytes,
                    'elapsed_seconds':max_elapsed_seconds,'yield_every_visits':yield_every,'edge_visits':MAX_EDGE_VISITS,'stack_depth':MAX_STACK_DEPTH},
            workspace_scope='SHARDED_MEMO_ID_KEYS_LOCAL_CANONICAL_MASK_POOL_ACTUAL_INTS_DICT_STATE_TRANSIENT_AND_RESIZE_RESERVES_KEEPALIVE_SNAPSHOTS_REVALIDATION_PLUS2MIB_AUXILIARY_NOT_TOTAL_RSS',
            temporary_graph_pins_released_on_return=True,
            partial_bytes_are_total_estimate=False)
        return result
    except BaseException:
        return _base('UNAVAILABLE',['MEASUREMENT_EXCEPTION'])


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


def _safe_event_type(cls):
    if type(cls) is not type or cls.__bases__!=(object,) or cls.__name__!='ResearchEvent':return False
    fields=cls.__dict__
    if any(name in fields for name in ('__sizeof__','__getattribute__','__len__','__iter__','__slots__','__del__')):return False
    descriptor=fields.get('__dict__')
    return (type(descriptor) is GetSetDescriptorType and descriptor.__objclass__ is cls and
            descriptor.__name__=='__dict__' and type(fields.get('__dataclass_fields__')) is dict)


SPOT_CANDLE_FIELDS=('open_time_utc','close_time_utc','open','high','low','close','volume')


def _safe_spot_candle_type(cls):
    """Admit the registered source record without invoking its methods."""
    if type(cls) is not type or cls.__bases__!=(object,) or cls.__name__!='SpotCandle':return False
    namespace=cls.__dict__
    if any(name in namespace for name in ('__sizeof__','__getattribute__','__len__','__iter__','__slots__','__del__')):return False
    descriptor=namespace.get('__dict__')
    if not (type(descriptor) is GetSetDescriptorType and descriptor.__objclass__ is cls and descriptor.__name__=='__dict__'):return False
    fields=namespace.get('__dataclass_fields__')
    if type(fields) is not dict or any(type(name) is not str for name in fields):return False
    return set(fields)==set(SPOT_CANDLE_FIELDS)


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


def _resolve(name):
    """Return an actual named container plus existing locks; no method calls."""
    history={
        'history_reference_cache':('coinglass_history_backfill','_REFERENCE_CACHE',None),
        'history_input_receipts':('coinglass_history_backfill','_REFERENCE_INPUT_RECEIPTS','_REFERENCE_INPUT_LOCK'),
        'flow_cache':('market_confidence_engine','_FLOW_CACHE',None),
        'flow_input_receipts':('market_confidence_engine','_FLOW_INPUT_RECEIPTS','_FLOW_INPUT_LOCK')}
    if name in history:
        module,field,lock_field=history[name]
        return _global(module,field),((_global(module,lock_field),) if lock_field else ())
    raise ValueError('UNSAFE_ADAPTER')


EXCLUDED_ROOT_SCOPES=(
    'Only the fixed4 current-process History/Flow roots are traversed. The32 additional retained roots are not revisited; no cross-process sum is made.',
    'Worker thread/coroutine locals, including last sidecar/Sheets payloads and retry batches, are not inspected.',
    'Database/SQLite/browser/native objects, locks, callables, credentials-bearing worker/queue/agent objects are not traversed.',
    'Small runtime writer queues, metadata/status/transition maps and diagnostic snapshots are outside these4 named roots.',
    'defaultdict default_factory and exact datetime tzinfo/native backing are excluded; shallow native-leaf bytes are not a complete heap graph.')


def capture_live_roots_and_measure(**options):
    """Read already-loaded fixed adapters, releasing every lock before walking."""
    roots={};presence={};snapshots={};snapshot_estimate=_AUXILIARY_ALLOWANCE_BYTES;largest_reservation=0
    try:
        workspace_limit=options.get('max_workspace_bytes',MAX_WORKSPACE_BYTES)
        if type(workspace_limit) is not int or not 1024<=workspace_limit<=MAX_WORKSPACE_BYTES:
            return _base('UNAVAILABLE',['INVALID_OPTIONS_OR_ROOT_NAMES'])
        for name in ROOT_NAMES:
            held=[]
            try:
                root,locks=_resolve(name)
                for lock in locks:
                    if type(lock) is not _thread.LockType or not lock.acquire(timeout=.05):
                        raise ValueError('LOCK_UNAVAILABLE')
                    held.append(lock)
                if not _root_supported(name,root):raise ValueError('UNSAFE_ADAPTER')
                if len(root)>_root_item_limit(name):raise ValueError('ROOT_BOUND')
                reservation=len(root)*128+4096
                if snapshot_estimate+reservation+2*max(largest_reservation,reservation)>workspace_limit:raise ValueError('ROOT_BOUND')
                snapshots[name]=_shape(root,max_items=_root_item_limit(name))
                snapshot_estimate+=reservation
                largest_reservation=max(largest_reservation,reservation)
                roots[name]=root
                presence[name]={'status':'CAPTURED','entry_count':len(root)}
            except ValueError as error:
                code=error.args[0] if error.args and type(error.args[0]) is str else 'UNSAFE_ADAPTER'
                if code not in {'NOT_LOADED','NOT_READY','LOCK_UNAVAILABLE','ROOT_BOUND'}:code='UNSAFE_ADAPTER'
                presence[name]={'status':code,'entry_count':None}
            except BaseException:
                presence[name]={'status':'UNSAFE_ADAPTER','entry_count':None}
            finally:
                for lock in reversed(held):
                    try:lock.release()
                    except BaseException:pass
        if not roots:result=_base('UNAVAILABLE',['NO_AVAILABLE_ROOTS'])
        else:
            history=sys.modules.get('coinglass_history_backfill')
            ranges_type=history.__dict__.get('_ReferenceRanges') if type(history) is ModuleType else None
            consumed=sys.modules.get('research_consumed_inputs')
            known=consumed.__dict__.get('_CapturedPackage') if type(consumed) is ModuleType else None
            events=sys.modules.get('research_event_capture')
            event_type=events.__dict__.get('ResearchEvent') if type(events) is ModuleType else None
            candles=sys.modules.get('binance_spot_price_path')
            candle_type=candles.__dict__.get('SpotCandle') if type(candles) is ModuleType else None
            result=measure_named_roots(roots,reference_ranges_type=ranges_type,captured_package_type=known,research_event_type=event_type,spot_candle_type=candle_type,_snapshots=snapshots,**options)
            replaced=[]
            for name in roots:
                try:current,_locks=_resolve(name)
                except BaseException:current=None
                if current is not roots[name]:replaced.append(name)
            if replaced:
                result.update(status='CONCURRENT',measurement_valid=False,complete=False,root_shapes_stable=False)
                result['reason_codes']=sorted(set(result['reason_codes']+['ROOT_REPLACED']))
                for name in replaced:
                    if name in result.get('roots',{}):
                        result['roots'][name].update(complete=False,measurement_valid=False,observed_mutation=True,root_shape_stable=False)
        available=[name for name in ROOT_NAMES if presence[name]['status']=='CAPTURED']
        result.update(declared_root_names=list(ROOT_NAMES),available_root_names=available,root_presence=presence,
            declared_coverage_complete=len(available)==len(ROOT_NAMES),
            declared_union_complete_valid=len(available)==len(ROOT_NAMES) and result['complete'] and result['measurement_valid'],
            root_item_limits={name:_root_item_limit(name) for name in ROOT_NAMES},
            excluded_root_scopes=list(EXCLUDED_ROOT_SCOPES),captured_at_utc=datetime.now(timezone.utc).isoformat())
        return result
    except BaseException:
        return _base('UNAVAILABLE',['CAPTURE_EXCEPTION'])


