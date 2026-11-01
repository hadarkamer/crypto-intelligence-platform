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

VERSION = 'retained-supported-memory-v1'
ROOT_NAMES = ('paper_checkpoint_loaded', 'liquidity_checkpoint_loaded', 'sheets_snapshot_cache', 'sheets_queue_payloads', 'reconciler_expected', 'reconciler_seen', 'reconciler_unexpected', 'past_price_lru', 'event_sink_events', 'event_writer_queue', 'consumed_inputs_queue', 'decision_store_queue', 'doge_sequence_queue', 'combined_shadow_queue', 'experimental_shared_prices', 'hype_price_rows', 'u21_price_rows', 'sol_g65_price_rows', 'sol_g65_legacy_rows', 'xrp_r2732_price_rows', 'xrp_r2732_legacy_rows', 'sol_proximity_price_rows', 'sol_proximity_legacy_rows', 'hype_proximity_price_rows', 'hype_proximity_legacy_rows', 'doge_proximity_price_rows', 'doge_proximity_legacy_rows', 'xrp_proximity_price_rows', 'xrp_proximity_legacy_rows', 'eth_proximity_price_rows', 'eth_proximity_legacy_rows', 'ai_conversation_messages')
MAX_ROOT_ITEMS = 50_000
MAX_NODES = 4_000_000
MAX_WORKSPACE_BYTES = 256 * 1024 * 1024
MAX_SECONDS = 30.0
YIELD_EVERY = 2048
MAX_EDGE_VISITS = 16_000_000
MAX_STACK_DEPTH = 128
_AUXILIARY_ALLOWANCE_BYTES = 2 * 1024 * 1024
_SCALARS = {str:'str',bytes:'bytes',int:'int',float:'float',bool:'bool',type(None):'none',weakref.ReferenceType:'weakref',datetime:'datetime_leaf'}
_CONTAINERS = {dict:'dict',list:'list',tuple:'tuple',set:'set',frozenset:'frozenset',OrderedDict:'ordered_dict',Counter:'counter',defaultdict:'default_dict',deque:'deque'}


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


def _shape(value,expected_count=None):
    cls=type(value)
    if not any(cls is allowed for allowed in (dict,OrderedDict,Counter,defaultdict,deque,list,tuple,set,frozenset)):
        raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
    count=len(value)
    if expected_count is not None and count!=expected_count:raise ValueError('ROOT_CHANGED')
    if count>MAX_ROOT_ITEMS:raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
    limit=count*2+1
    if cls in (dict,OrderedDict,Counter,defaultdict):
        children=tuple(islice((child for pair in dict.items(value) for child in pair),limit+1))
    else:children=tuple(islice(value,limit+1))
    if len(children)>limit:raise ValueError('ROOT_CHANGED')
    if cls is Counter or cls is OrderedDict:
        attributes=_instance_dict(value)
        if attributes is None:raise ValueError('UNSAFE_COUNTER_ATTRIBUTES')
        children=children+(attributes,)
    if len(children)>MAX_ROOT_ITEMS*2+1:raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
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
    elif kind == 'research_event':
        yield _instance_dict(value)
    else:
        yield from value


def _base(status, codes):
    return {'version':VERSION,'status':status,'reason_codes':sorted(set(codes)),
        'measurement_valid':False,'complete':False,'atomic':False,
        'scope':'NAMED_SUPPORTED_PYTHON_GRAPH_SHALLOW_GETSIZEOF_NOT_RSS_NATIVE_ALLOCATOR_OR_EXCLUSIVE_OWNERSHIP',
        'weak_references_followed':False,'object_values_or_addresses_persisted':False,
        'observer_effect':'First-seen objects are temporarily pinned through the probe to prevent id reuse. Memo tables and pins allocate temporary memory. Accessing native Counter/OrderedDict instance dictionaries may materialize an empty dict that is then counted. No owner or RSS-effect subtraction is inferred.',
        'caller_gate_required':'NO_ACTIVE_WATCH_AND_LOW_RSS_AND_UNEXPIRED_DIAGNOSTIC_WINDOW'}


def measure_named_roots(roots, *, reference_ranges_type=None, captured_package_type=None, research_event_type=None,
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
        if not snapshots:
            reservations={}
            for name in names:
                size=len(roots[name]) if any(type(roots[name]) is cls for cls in _CONTAINERS) else MAX_ROOT_ITEMS+1
                if size>MAX_ROOT_ITEMS:return _base('UNAVAILABLE',['ROOT_TYPE_OR_ITEM_BOUND'])
                reservations[name]=size*128+4096
            if _AUXILIARY_ALLOWANCE_BYTES+sum(reservations.values())+2*max(reservations.values())>max_workspace_bytes:
                return _base('UNAVAILABLE',['SNAPSHOT_WORKSPACE_BOUND'])
            for name in names:snapshots[name]=_shape(roots[name])
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
        mask_allowance=sys.getsizeof((1<<len(names))-1)
        mask_size=0
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
        workspace_peak=memo_size+snapshot_size+keepalive_size
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
            return memo_size + id_size + mask_size + snapshot_size + keepalive_size
        def expired():
            elapsed=clock()-started
            if elapsed < 0: raise ValueError('CLOCK_BACKWARDS')
            return elapsed >= max_elapsed_seconds
        for root_index,name in enumerate(names):
            if stop: break
            bit=1<<root_index
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
                    possible=(memo_size+id_size+mask_size+snapshot_size+keepalive_size+mask_allowance+
                              3*shard_sizes[shard_index]+512+sys.getsizeof(identity)+keepalive_size//4+256)
                    if possible > max_workspace_bytes: codes.append('WORKSPACE_BOUND'); stop=True; break
                    before_size=shard_sizes[shard_index]; shard[identity]=bit
                    keepalive.append(value); keepalive_size=sys.getsizeof(keepalive)
                    after_size=sys.getsizeof(shard); shard_sizes[shard_index]=after_size
                    memo_size+=after_size-before_size; id_size+=sys.getsizeof(identity)
                    union_objects+=1; union_bytes+=size; mask_size+=mask_allowance
                else:
                    shard[identity]=previous|bit
                    if previous & (previous-1)==0: shared_objects+=1; shared_bytes+=size
                    for earlier in range(root_index):
                        if previous & (1<<earlier): pairwise[names[earlier]+'|'+name]+=size
                current=stats[name]; current['unique_objects']+=1; current['inclusive_unique_bytes']+=size
                current['type_counts'][kind]=current['type_counts'].get(kind,0)+1
                current['type_bytes'][kind]=current['type_bytes'].get(kind,0)+size
                if cls not in _SCALARS:
                    children=iter(root_children[identity]) if identity in root_children else _children(value,kind)
                    container=_instance_dict(value) if kind=='research_event' else value
                    stack.append(['iter',container,children,len(container),sys.getsizeof(container)])
                used=memo_size+id_size+mask_size+snapshot_size+keepalive_size; workspace_peak=max(workspace_peak,used)
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
                    shape,_items=_shape(value,expected_count=initial_shapes[name][1])
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
            workspace_mask_bytes_per_unique_node=mask_allowance,workspace_masks_counted_bytes=mask_size,
            workspace_revalidation_reserve_bytes=revalidation_reserve,
            limits={'unique_nodes':max_nodes,'workspace_counted_bytes':max_workspace_bytes,
                    'elapsed_seconds':max_elapsed_seconds,'yield_every_visits':yield_every,'edge_visits':MAX_EDGE_VISITS,'stack_depth':MAX_STACK_DEPTH},
            workspace_scope='SHARDED_MEMO_ID_KEYS_PER_NODE_MAX_ROOT_MASK_ALLOWANCE_KEEPALIVE_CAPACITY_SNAPSHOTS_REVALIDATION_RESERVE_PLUS2MIB_AUXILIARY_ALLOWANCE_NOT_TOTAL_RSS',
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


EXCLUDED_ROOT_SCOPES=(
    'Previously measured History/Flow roots are not revisited.',
    'Worker thread/coroutine locals, including last sidecar/Sheets payloads and retry batches, are not inspected.',
    'Database/SQLite/browser/native objects, locks, callables, credentials-bearing worker/queue/agent objects are not traversed.',
    'Small runtime writer queues, metadata/status/transition maps and diagnostic snapshots are outside these32 named roots.',
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
                if not any(type(root) is cls for cls in _CONTAINERS):raise ValueError('UNSAFE_ADAPTER')
                if len(root)>MAX_ROOT_ITEMS:raise ValueError('ROOT_BOUND')
                reservation=len(root)*128+4096
                if snapshot_estimate+reservation+2*max(largest_reservation,reservation)>workspace_limit:raise ValueError('ROOT_BOUND')
                snapshots[name]=_shape(root)
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
            consumed=sys.modules.get('research_consumed_inputs')
            known=consumed.__dict__.get('_CapturedPackage') if type(consumed) is ModuleType else None
            events=sys.modules.get('research_event_capture')
            event_type=events.__dict__.get('ResearchEvent') if type(events) is ModuleType else None
            result=measure_named_roots(roots,captured_package_type=known,research_event_type=event_type,_snapshots=snapshots,**options)
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
            excluded_root_scopes=list(EXCLUDED_ROOT_SCOPES),captured_at_utc=datetime.now(timezone.utc).isoformat())
        return result
    except BaseException:
        return _base('UNAVAILABLE',['CAPTURE_EXCEPTION'])


