"""Bounded named-cache observations. No import-time probes or producer imports.

Counts unique shallow sys.getsizeof bytes of supported reachable Python objects.
It is not RSS, allocator usage, native memory, exclusive ownership or a heap walk.
The caller must enforce idle-interval/RSS/absolute-expiry admission separately.
"""
from collections import OrderedDict
from datetime import datetime, timezone
import _thread
import sys
import time
from types import ModuleType
import weakref

VERSION = 'named-history-memory-v1'
ROOT_NAMES = ('history_reference_cache', 'history_input_receipts', 'flow_cache', 'flow_input_receipts')
MAX_ROOT_ITEMS = 64
MAX_NODES = 4_000_000
MAX_WORKSPACE_BYTES = 256 * 1024 * 1024
MAX_SECONDS = 30.0
YIELD_EVERY = 2048
MAX_EDGE_VISITS = 16_000_000
MAX_STACK_DEPTH = 128
_AUXILIARY_ALLOWANCE_BYTES = 2 * 1024 * 1024
_SCALARS = {str:'str',bytes:'bytes',int:'int',float:'float',bool:'bool',type(None):'none',weakref.ReferenceType:'weakref'}
_CONTAINERS = {dict:'dict',list:'list',tuple:'tuple',set:'set',frozenset:'frozenset',OrderedDict:'ordered_dict'}


def _known_dict_type(cls, name):
    if type(cls) is not type or cls.__bases__ != (dict,) or cls.__name__ != name:
        return False
    allowed = {'__module__','__doc__','__dict__','__weakref__','__slots__'}
    if not set(cls.__dict__) <= allowed:
        return False
    if name == '_ReferenceRanges':
        return cls.__dict__.get('__slots__') == ('__weakref__',)
    return name == '_CapturedPackage' and '__slots__' not in cls.__dict__


def _shape(mapping):
    kind = type(mapping)
    if kind not in (dict, OrderedDict) or len(mapping) > MAX_ROOT_ITEMS:
        raise ValueError('ROOT_TYPE_OR_ITEM_BOUND')
    items = tuple(mapping.items())
    return (id(mapping), len(mapping), sys.getsizeof(mapping), tuple((id(k),id(v)) for k,v in items)), items


def _children(value, kind):
    if kind in ('dict','reference_ranges','captured_package'):
        for key, child in dict.items(value):
            yield key
            yield child
        if kind == 'captured_package':
            yield object.__getattribute__(value,'__dict__')
    elif kind == 'ordered_dict':
        for key, child in OrderedDict.items(value):
            yield key
            yield child
    else:
        yield from value


def _base(status, codes):
    return {'version':VERSION,'status':status,'reason_codes':sorted(set(codes)),
        'measurement_valid':False,'complete':False,'atomic':False,
        'scope':'NAMED_SUPPORTED_PYTHON_GRAPH_SHALLOW_GETSIZEOF_NOT_RSS_NATIVE_ALLOCATOR_OR_EXCLUSIVE_OWNERSHIP',
        'weak_references_followed':False,'object_values_or_addresses_persisted':False,
        'observer_effect':'First-seen objects are temporarily pinned through the probe to prevent id reuse. Memo tables and pins allocate temporary memory; no owner or RSS-effect subtraction is inferred.',
        'caller_gate_required':'NO_ACTIVE_WATCH_AND_LOW_RSS_AND_UNEXPIRED_DIAGNOSTIC_WINDOW'}


def measure_named_roots(roots, *, reference_ranges_type=None, captured_package_type=None,
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
        if (type(roots) is not dict or set(roots) != set(ROOT_NAMES) or
            type(max_nodes) is not int or not 1 <= max_nodes <= MAX_NODES or
            type(max_workspace_bytes) is not int or not 1024 <= max_workspace_bytes <= MAX_WORKSPACE_BYTES or
            type(max_elapsed_seconds) not in (int,float) or not 0 < max_elapsed_seconds <= MAX_SECONDS or
            type(yield_every) is not int or not 1 <= yield_every <= YIELD_EVERY):
            return _base('UNAVAILABLE',['INVALID_OPTIONS_OR_ROOT_NAMES'])
        known = {}
        for cls,name,kind in ((reference_ranges_type,'_ReferenceRanges','reference_ranges'),
                              (captured_package_type,'_CapturedPackage','captured_package')):
            if cls is not None:
                if not _known_dict_type(cls,name):
                    return _base('UNAVAILABLE',['UNSAFE_KNOWN_DICT_TYPE'])
                known[cls] = kind
        clock = time.monotonic if clock is None else clock
        yield_fn = (lambda: time.sleep(0)) if yield_fn is None else yield_fn
        started = clock()
        snapshots = {} if _snapshots is None else _snapshots
        if not snapshots:
            for name in ROOT_NAMES:
                signature, items = _shape(roots[name])
                snapshots[name] = (signature,items)
        initial_shapes = {name:snapshots[name][0] for name in ROOT_NAMES}
        root_children = {id(roots[name]):tuple(child for pair in snapshots[name][1] for child in pair) for name in ROOT_NAMES}
        # Shards bound one memo-table resize; no per-object tuple/size record.
        memo = [{} for _ in range(256)]
        # Prevent address reuse even if another thread removes a nested object.
        # Pins exist only for this walk and are released before the call returns.
        keepalive=[]
        keepalive_size=sys.getsizeof(keepalive)
        shard_sizes = [sys.getsizeof(part) for part in memo]
        memo_size = sys.getsizeof(memo) + sum(shard_sizes)
        id_size = 0
        snapshot_size = sys.getsizeof(snapshots) + sys.getsizeof(root_children)
        for name in ROOT_NAMES:
            snapshot_size += sys.getsizeof(snapshots[name]) + sys.getsizeof(snapshots[name][0]) + sys.getsizeof(snapshots[name][1])
            snapshot_size += sum(sys.getsizeof(pair) for pair in snapshots[name][1])
            snapshot_size += sys.getsizeof(root_children[id(roots[name])])
        stats = {name:{'inclusive_unique_bytes':0,'unique_objects':0,'root_items':initial_shapes[name][1],
                       'complete':False,'measurement_valid':False,'observed_mutation':False,
                       'root_shape_stable':None,'type_counts':{},'type_bytes':{}} for name in ROOT_NAMES}
        pairwise = {ROOT_NAMES[i]+'|'+ROOT_NAMES[j]:0 for i in range(4) for j in range(i+1,4)}
        union_bytes=union_objects=shared_bytes=shared_objects=visits=unsupported=0
        # Bounded128 frames/iterators, root signatures, type and result counters.
        # This conservative allowance avoids walking the workspace per graph node.
        snapshot_size += _AUXILIARY_ALLOWANCE_BYTES
        workspace_peak=memo_size+snapshot_size+keepalive_size
        codes=[]; concurrent=False; stop=False; stack=[]
        changed_roots=set()
        def mark_changed(value, current_name):
            identity=id(value)
            mask=memo[(identity>>4)&255].get(identity,0)
            changed_roots.add(current_name)
            for index,root_name in enumerate(ROOT_NAMES):
                if mask & (1<<index): changed_roots.add(root_name)
        def workspace():
            return memo_size + id_size + snapshot_size + keepalive_size
        def expired():
            elapsed=clock()-started
            if elapsed < 0: raise ValueError('CLOCK_BACKWARDS')
            return elapsed >= max_elapsed_seconds
        for root_index,name in enumerate(ROOT_NAMES):
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
                cls=type(value); kind=_SCALARS.get(cls) or _CONTAINERS.get(cls) or known.get(cls)
                if kind is None:
                    unsupported+=1; codes.append('UNSUPPORTED_TYPE'); continue
                identity=id(value); shard_index=(identity>>4)&255; shard=memo[shard_index]; previous=shard.get(identity,0)
                if previous & bit: continue
                size=sys.getsizeof(value)
                if not previous:
                    if union_objects >= max_nodes: codes.append('NODE_BOUND'); stop=True; break
                    # Conservative room for a single shard resize before insert.
                    possible=(memo_size+id_size+snapshot_size+keepalive_size+
                              3*shard_sizes[shard_index]+512+sys.getsizeof(identity)+keepalive_size//4+256)
                    if possible > max_workspace_bytes: codes.append('WORKSPACE_BOUND'); stop=True; break
                    before_size=shard_sizes[shard_index]; shard[identity]=bit
                    keepalive.append(value); keepalive_size=sys.getsizeof(keepalive)
                    after_size=sys.getsizeof(shard); shard_sizes[shard_index]=after_size
                    memo_size+=after_size-before_size; id_size+=sys.getsizeof(identity)
                    union_objects+=1; union_bytes+=size
                else:
                    shard[identity]=previous|bit
                    if previous & (previous-1)==0: shared_objects+=1; shared_bytes+=size
                    for earlier in range(root_index):
                        if previous & (1<<earlier): pairwise[ROOT_NAMES[earlier]+'|'+name]+=size
                current=stats[name]; current['unique_objects']+=1; current['inclusive_unique_bytes']+=size
                current['type_counts'][kind]=current['type_counts'].get(kind,0)+1
                current['type_bytes'][kind]=current['type_bytes'].get(kind,0)+size
                if cls not in _SCALARS:
                    children=iter(root_children[identity]) if identity in root_children else _children(value,kind)
                    stack.append(['iter',value,children,len(value),size])
                used=memo_size+id_size+snapshot_size+keepalive_size; workspace_peak=max(workspace_peak,used)
                if used > max_workspace_bytes: codes.append('WORKSPACE_BOUND'); stop=True; break
            stats[name]['complete']=not stop and unsupported==unsupported_before and name not in changed_roots
        # Re-read roots only; no addresses/fingerprints enter the receipt.
        stable=True
        for name in ROOT_NAMES:
            try:
                shape,_items=_shape(roots[name])
                if shape != initial_shapes[name]: stable=False; changed_roots.add(name)
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
        result.update(measurement_valid=not concurrent,complete=complete,root_shapes_stable=stable,
            consistency='BEST_EFFORT_NONATOMIC_OBSERVED_ROOT_AND_CONTAINER_SIZE_CHECKS',
            covered_union_bytes=union_bytes,covered_unique_objects=union_objects,
            covered_shared_unique_bytes=shared_bytes,covered_shared_unique_objects=shared_objects,
            inclusive_bytes_sum=sum(item['inclusive_unique_bytes'] for item in stats.values()),
            repeated_reachable_bytes=sum(item['inclusive_unique_bytes'] for item in stats.values())-union_bytes,
            roots=stats,pairwise_overlap_bytes=pairwise,unsupported_visits=unsupported,
            traversal_visits=visits,elapsed_ms=round(elapsed*1000,3),workspace_peak_counted_bytes=workspace_peak,
            limits={'unique_nodes':max_nodes,'workspace_counted_bytes':max_workspace_bytes,
                    'elapsed_seconds':max_elapsed_seconds,'yield_every_visits':yield_every,'edge_visits':MAX_EDGE_VISITS,'stack_depth':MAX_STACK_DEPTH},
            workspace_scope='SHARDED_MEMO_ID_KEYS_KEEPALIVE_CAPACITY_SNAPSHOTS_PLUS2MIB_AUXILIARY_ALLOWANCE_NOT_TOTAL_RSS',
            temporary_graph_pins_released_on_return=True,
            partial_bytes_are_total_estimate=False)
        return result
    except BaseException:
        return _base('UNAVAILABLE',['MEASUREMENT_EXCEPTION'])


def _history_entries(items):
    entries=[]
    allowed={'BTC','ETH','SOL','HYPE','DOGE','ZEC','BNB','XRP'}
    for symbol,value in items:
        if type(symbol) is not str or symbol not in allowed or not isinstance(value,dict): continue
        windows=dict.get(value,'windows'); rows=dict.get(value,'rows')
        if type(windows) is not dict: continue
        counts=[]
        for label in ('30m','1h','4h','12h','24h','48h','72h','7d'):
            window=windows.get(label)
            if type(window) is not dict: continue
            price=window.get('price_composition_samples'); oi=window.get('oi_composition_samples')
            counts.append({'window':label,'price_pair_count':len(price) if type(price) in (list,tuple) else None,
                           'oi_pair_count':len(oi) if type(oi) in (list,tuple) else None})
        entries.append({'symbol':symbol,'rows_scalar':rows if type(rows) is int and 0<=rows<2**31 else None,'windows':counts})
    return entries


def capture_live_roots_and_measure(**options):
    """Use already-loaded producer modules only; receipt locks never span walk."""
    held=[]
    try:
        modules={name:sys.modules.get(name) for name in ('coinglass_history_backfill','market_confidence_engine','research_consumed_inputs')}
        if any(type(module) is not ModuleType for module in modules.values()):
            return _base('UNAVAILABLE',['PRODUCER_MODULE_NOT_LOADED'])
        globals_by_name={name:module.__dict__ for name,module in modules.items()}
        history=globals_by_name['coinglass_history_backfill']; flow=globals_by_name['market_confidence_engine']; consumed=globals_by_name['research_consumed_inputs']
        specifications=(('history_reference_cache',history,'_REFERENCE_CACHE',None),
            ('history_input_receipts',history,'_REFERENCE_INPUT_RECEIPTS','_REFERENCE_INPUT_LOCK'),
            ('flow_cache',flow,'_FLOW_CACHE',None),('flow_input_receipts',flow,'_FLOW_INPUT_RECEIPTS','_FLOW_INPUT_LOCK'))
        roots={}; snapshots={}
        for name,namespace,key,lock_key in specifications:
            if lock_key:
                lock=namespace.get(lock_key)
                if type(lock) is not _thread.LockType or not lock.acquire(timeout=.05):
                    return _base('UNAVAILABLE',['RECEIPT_LOCK_UNAVAILABLE'])
                held.append(lock)
            try:
                root=namespace.get(key); signature,items=_shape(root)
                roots[name]=root; snapshots[name]=(signature,items)
            finally:
                if lock_key: held.pop().release()
        result=measure_named_roots(roots,reference_ranges_type=history.get('_ReferenceRanges'),
            captured_package_type=consumed.get('_CapturedPackage'),_snapshots=snapshots,**options)
        # A module-global root replacement also invalidates the captured graph.
        replaced=[name for name,namespace,key,_ in specifications if namespace.get(key) is not roots[name]]
        if replaced:
            result.update(status='CONCURRENT',measurement_valid=False,complete=False,root_shapes_stable=False)
            result['reason_codes']=sorted(set(result['reason_codes']+['ROOT_REPLACED']))
            for name in replaced:
                if name in result.get('roots',{}):
                    result['roots'][name].update(complete=False,measurement_valid=False,observed_mutation=True,root_shape_stable=False)
        result['history_entries']=_history_entries(snapshots['history_reference_cache'][1])
        result['captured_at_utc']=datetime.now(timezone.utc).isoformat()
        return result
    except BaseException:
        return _base('UNAVAILABLE',['CAPTURE_EXCEPTION'])
    finally:
        for lock in held:
            try: lock.release()
            except BaseException: pass
