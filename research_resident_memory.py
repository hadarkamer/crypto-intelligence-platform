"""Bounded, one-attempt malloc_trim response trial; never runs at import.

The controller owns outer admission, a durable original-slot claim and expiry.
This module cannot make its per-invocation once flag durable across restarts.
No cache clear, GC, heap walk, tuning, raw mapping persistence or resident-free
capacity calculation occurs here. All external operations are injectable.
"""
from datetime import datetime, timezone
import json
import math
import re
import sys
import threading
import time

VERSION='resident-memory-trim-response-v1'
SNAPSHOT_NAMES=('SHAM_BEFORE','SHAM_AFTER','PRE_TRIM','POST_TRIM','SETTLED')
MAX_SECONDS=15.0
MAX_SMAPS_BYTES=2*1024*1024
MAX_READ_CALLS=1024
READ_CHUNK_BYTES=64*1024
MAX_SMAPS_SECONDS=0.5
MAX_COMPONENT_BYTES=16*1024
MAX_ADMISSION_BYTES=64*1024
MAX_RESULT_BYTES=192*1024
FRESH_AGE_SECONDS=2.5
MAX_SELF_RSS_BYTES=1024*1024*1024
SCOPE='ONE_MALLOC_TRIM_RESPONSE_NOT_CACHE_CLEAR_GC_ALLOCATOR_OWNERSHIP_RESIDENT_FREE_POINT_OR_ISOLATED_CAUSAL_SAVING'
PROCESS_KEYS=frozenset(('version','status','platform','sample_started_at_utc','sample_completed_at_utc','elapsed_ms','rss_bytes','status_read','smaps_rollup','python_counters','allocator','limits','errors','scope','observer_effect'))
SMAPS_KEYS=frozenset(('version','status','reason_codes','input_bytes','read_complete','mapping_count','covered_mapping_count','classes','covered_totals_bytes','field_available_counts','page_sizes_bytes','candidate','hugetlb_present','addresses_or_paths_persisted','raw_mapping_payload_persisted','allocator_containment_established','atomic','scope','limits','interval_scope'))
ADMISSION_KEYS=frozenset(('version','status','reason','samples','selected_sample_index','cached_tail_memory','max_samples','budget_seconds','scope','elapsed_ms'))
_FORBIDDEN_KEYS=frozenset(('raw','raw_bytes','raw_smaps','path','paths','address','addresses','pid','pids','cmdline','environ','environment','inode','device','traceback','exception_text'))
_ROLLUP=('Rss','Pss','Pss_Anon','Pss_File','Pss_Shmem','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty','Referenced','Anonymous','LazyFree','AnonHugePages','ShmemPmdMapped','FilePmdMapped','Shared_Hugetlb','Private_Hugetlb','Swap','SwapPss','Locked')
_MAP_FIELDS=('Size','Rss','Pss','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty','Anonymous','Swap','Shared_Hugetlb','Private_Hugetlb','AnonHugePages','LazyFree')
_MALL=('arena','ordblks','smblks','hblks','hblkhd','usmblks','fsmblks','uordblks','fordblks','keepcost')


class _Stop(Exception):
    pass


def _copy(value,cap=MAX_COMPONENT_BYTES):
    """Exact builtin primitives only; callback exceptions/text are never saved."""
    count=[0]
    def visit(x,depth):
        count[0]+=1
        if depth>16 or count[0]>4096:raise _Stop('COMPONENT_STRUCTURE_BOUND')
        t=type(x)
        if x is None or t is bool:return x
        if t is int:
            if not -(2**63)<=x<=2**64-1:raise _Stop('COMPONENT_NUMBER_BOUND')
            return x
        if t is float:
            if not math.isfinite(x):raise _Stop('COMPONENT_NUMBER_BOUND')
            return x
        if t is str:
            if len(x)>4096:raise _Stop('COMPONENT_STRING_BOUND')
            return x
        if t in (list,tuple):
            if len(x)>512:raise _Stop('COMPONENT_SEQUENCE_BOUND')
            return [visit(v,depth+1) for v in x]
        if t is dict:
            if len(x)>256:raise _Stop('COMPONENT_OBJECT_BOUND')
            result={}
            for k,v in x.items():
                if type(k) is not str or len(k)>96 or k in _FORBIDDEN_KEYS:raise _Stop('UNSAFE_COMPONENT_KEY')
                result[k]=visit(v,depth+1)
            return result
        raise _Stop('NONPRIMITIVE_COMPONENT')
    result=visit(value,0)
    if len(json.dumps(result,ensure_ascii=True,allow_nan=False,separators=(',',':')).encode())>cap:raise _Stop('COMPONENT_BYTE_BOUND')
    return result


def _utc(value):
    if type(value) is not str or not 20<=len(value)<=64:raise _Stop('UTC_CLOCK_INVALID')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:raise _Stop('UTC_CLOCK_INVALID')
    return value


def _now():return datetime.now(timezone.utc).isoformat()


def _identity(value):
    if type(value) is not dict or set(value)!={'session','runtime_commit','origin_cycle_id','observation_index'}:raise _Stop('IDENTITY_INVALID')
    if type(value['session']) is not str or re.fullmatch('[0-9a-f]{32}',value['session']) is None:raise _Stop('IDENTITY_INVALID')
    if type(value['runtime_commit']) is not str or re.fullmatch('[0-9a-f]{40}',value['runtime_commit']) is None:raise _Stop('IDENTITY_INVALID')
    cycle=value['origin_cycle_id']
    if type(cycle) is not str or not cycle.startswith('shared-watch:') or len(cycle)>160:raise _Stop('IDENTITY_INVALID')
    _utc(cycle.removeprefix('shared-watch:'))
    if type(value['observation_index']) is not int or value['observation_index']!=5:raise _Stop('IDENTITY_INVALID')
    return dict(value)


def _runtime_supported(info):
    required={'platform','implementation','python_version','pointer_bytes','size_t_bytes','int_bytes','glibc_version','debug_build'}
    return (type(info) is dict and set(info)==required and info['platform']=='linux' and info['implementation']=='cpython'
        and type(info['python_version']) in (tuple,list) and len(info['python_version'])==3
        and all(type(v) is int for v in info['python_version']) and tuple(info['python_version'])==(3,11,9)
        and all(type(info[k]) is int and info[k]==n for k,n in (('pointer_bytes',8),('size_t_bytes',8),('int_bytes',4)))
        and info['glibc_version']=='2.36' and info['debug_build'] is False)


def _native_factory():
    """Lazy default only. CDLL releases the GIL; no hard call-time limit exists."""
    if sys.platform!='linux' or sys.implementation.name!='cpython' or tuple(sys.version_info[:3])!=(3,11,9):
        return {'status':'UNAVAILABLE','reason':'UNSUPPORTED_RUNTIME','info':None}
    try:
        import ctypes
        info={'platform':sys.platform,'implementation':sys.implementation.name,'python_version':list(sys.version_info[:3]),
            'pointer_bytes':ctypes.sizeof(ctypes.c_void_p),'size_t_bytes':ctypes.sizeof(ctypes.c_size_t),'int_bytes':ctypes.sizeof(ctypes.c_int),
            'glibc_version':None,'debug_build':hasattr(sys,'gettotalrefcount') or 'd' in getattr(sys,'abiflags','')}
        if info['pointer_bytes']!=8 or info['size_t_bytes']!=8 or info['int_bytes']!=4 or info['debug_build']:
            return {'status':'UNAVAILABLE','reason':'UNSUPPORTED_ABI','info':info}
        library=ctypes.CDLL(None,use_errno=True)
        version=library.gnu_get_libc_version;version.argtypes=[];version.restype=ctypes.c_char_p
        raw=version()
        if type(raw) is not bytes or len(raw)>32:
            return {'status':'UNAVAILABLE','reason':'UNSUPPORTED_GLIBC','info':info}
        info['glibc_version']=raw.decode('ascii')
        if not _runtime_supported(info):return {'status':'UNAVAILABLE','reason':'UNSUPPORTED_RUNTIME_ABI_GLIBC','info':info}
        trim=library.malloc_trim;trim.argtypes=[ctypes.c_size_t];trim.restype=ctypes.c_int
        return {'status':'READY','info':info,'trim':trim}
    except BaseException:
        return {'status':'UNAVAILABLE','reason':'NATIVE_SETUP_EXCEPTION','info':None}


def _smaps_opener():return open('/proc/self/smaps','rb',buffering=0)


def _process_snapshot():
    import research_process_memory
    return research_process_memory.sample(include_mallinfo2=True)


def _safe_process(value):
    if type(value) is not dict or set(value)!=PROCESS_KEYS or value.get('version')!='process-memory-diagnostics-v1':raise _Stop('PROCESS_SCHEMA')
    result=_copy(value)
    if result['status'] not in ('COMPLETE','PARTIAL','UNAVAILABLE'):raise _Stop('PROCESS_SCHEMA')
    rss=result['rss_bytes']
    if rss is not None and (type(rss) is not int or not 0<=rss<2**63):raise _Stop('PROCESS_SCHEMA')
    for k in ('sample_started_at_utc','sample_completed_at_utc'):
        if result[k] is not None:_utc(result[k])
    integer=lambda x:type(x) is int and 0<=x<2**63
    codes=lambda x:type(x) is list and len(x)<=32 and all(type(s) is str and re.fullmatch('[A-Z][A-Z0-9_]{0,79}',s) for s in x)
    for name,fields in (('status_read',('VmRSS',)),('smaps_rollup',_ROLLUP)):
        part=result[name]
        if type(part) is not dict or set(part)!={'status','fields_bytes','missing_required','errors'}:raise _Stop('PROCESS_SCHEMA')
        if part['status'] not in ('COMPLETE','PARTIAL','UNAVAILABLE') or type(part['fields_bytes']) is not dict or set(part['fields_bytes'])!=set(fields):raise _Stop('PROCESS_SCHEMA')
        if not all(x is None or integer(x) for x in part['fields_bytes'].values()) or not codes(part['errors']):raise _Stop('PROCESS_SCHEMA')
        if type(part['missing_required']) is not list or any(x not in fields for x in part['missing_required']):raise _Stop('PROCESS_SCHEMA')
    if result['status_read']['fields_bytes']['VmRSS']!=rss:raise _Stop('PROCESS_SCHEMA')
    py=result['python_counters']
    if type(py) is not dict or set(py)!={'allocated_blocks_count','gc_count_tuple','errors','scope'} or not codes(py['errors']):raise _Stop('PROCESS_SCHEMA')
    if py['allocated_blocks_count'] is not None and not integer(py['allocated_blocks_count']):raise _Stop('PROCESS_SCHEMA')
    if py['gc_count_tuple'] is not None and (type(py['gc_count_tuple']) is not list or len(py['gc_count_tuple'])!=3 or not all(type(x) is int and -(2**63)<x<2**63 for x in py['gc_count_tuple'])):raise _Stop('PROCESS_SCHEMA')
    a=result['allocator']
    if type(a) is not dict or set(a)!={'status','glibc_version','size_t_bytes','fields','field_units','coherence','scope','errors'} or not codes(a['errors']):raise _Stop('PROCESS_SCHEMA')
    if a['fields'] is not None:
        if type(a['fields']) is not dict or set(a['fields'])!=set(_MALL) or not all(integer(x) for x in a['fields'].values()):raise _Stop('PROCESS_SCHEMA')
        expected={k:'COUNT' if k in ('ordblks','smblks','hblks') else 'BYTES' for k in _MALL}
        if a['field_units']!=expected or type(a['size_t_bytes']) is not int or a['size_t_bytes']!=8:raise _Stop('PROCESS_SCHEMA')
        if type(a['glibc_version']) is not str or re.fullmatch(r'\d{1,3}\.\d{1,3}(?:\.\d{1,3})?',a['glibc_version']) is None:raise _Stop('PROCESS_SCHEMA')
        f=a['fields'];coherent=f['arena']==f['uordblks']+f['fordblks'] and f['fsmblks']<=f['fordblks'] and f['keepcost']<=f['fordblks']
        if type(a['coherence']) is not bool or a['coherence']!=coherent or a['status']!=('COMPLETE' if coherent else 'PARTIAL'):raise _Stop('PROCESS_SCHEMA')
    elif a['status'] not in ('UNAVAILABLE','NOT_REQUESTED') or any(a[k] is not None for k in ('glibc_version','size_t_bytes','field_units','coherence')):raise _Stop('PROCESS_SCHEMA')
    if not codes(result['errors']) or result['limits']!={'status_bytes':16384,'smaps_rollup_bytes':65536,'status_read_calls_max':1,'smaps_read_calls_max':1}:raise _Stop('PROCESS_SCHEMA')
    if result['status']=='COMPLETE' and (result['platform']!='LINUX_PROCFS' or any(result[k]['status']!='COMPLETE' for k in ('status_read','smaps_rollup','allocator'))
        or result['errors'] or py['errors'] or py['allocated_blocks_count'] is None or py['gc_count_tuple'] is None
        or rss is None or result['sample_started_at_utc'] is None or result['sample_completed_at_utc'] is None
        or type(result['elapsed_ms']) not in (int,float) or not math.isfinite(result['elapsed_ms']) or result['elapsed_ms']<0
        or any(result['smaps_rollup']['fields_bytes'][k] is None for k in ('Rss','Pss','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty','Anonymous'))):raise _Stop('PROCESS_SCHEMA')
    return result


def _safe_smaps(value):
    if type(value) is not dict or set(value)!=SMAPS_KEYS or value.get('version')!='resident-smaps-projection-v1':raise _Stop('SMAPS_SCHEMA')
    if any(value[k] is not False for k in ('addresses_or_paths_persisted','raw_mapping_payload_persisted','allocator_containment_established','atomic')):raise _Stop('SMAPS_PRIVACY_SCOPE')
    result=_copy(value)
    if result['status'] not in ('COMPLETE','PARTIAL','UNAVAILABLE') or type(result['read_complete']) is not bool:raise _Stop('SMAPS_SCHEMA')
    if type(result['classes']) is not dict or not set(result['classes'])<={'HEAP','STACK','PRIVATE_RW_ANONYMOUS','ANONYMOUS_OTHER','SHARED_ANONYMOUS','FILE_BACKED','SPECIAL_OTHER'}:raise _Stop('SMAPS_SCHEMA')
    for fields in (result['covered_totals_bytes'],):
        if type(fields) is not dict or not set(fields)<=set(_MAP_FIELDS) or any(x is not None and (type(x) is not int or x<0) for x in fields.values()):raise _Stop('SMAPS_SCHEMA')
    for group in result['classes'].values():
        if type(group) is not dict or set(group)!={'mapping_count','fields_bytes','field_available_counts'}:raise _Stop('SMAPS_SCHEMA')
        if set(group['fields_bytes'])!=set(_MAP_FIELDS) or set(group['field_available_counts'])!=set(_MAP_FIELDS):raise _Stop('SMAPS_SCHEMA')
    return result


def _unsafe_sample(value):
    if type(value) is not dict:return 'ADMISSION_SCHEMA'
    children=value.get('descendants') or {}
    for name in ('browser_named','other'):
        count=(children.get(name) or {}).get('process_count')
        if count is not None and (type(count) is not int or count<0):return 'ADMISSION_SCHEMA'
        if type(count) is int and count>0:return 'POSITIVE_DESCENDANT'
    rss=(value.get('self') or {}).get('rss_bytes')
    if rss is not None and type(rss) is not int:return 'ADMISSION_SCHEMA'
    if type(rss) is int and (rss<0 or rss>MAX_SELF_RSS_BYTES):return 'SELF_RSS_GUARD'
    group=value.get('cgroup') or {};current=group.get('current_bytes');maximum=group.get('max_bytes')
    if any(x is not None and type(x) is not int for x in (current,maximum)):return 'ADMISSION_SCHEMA'
    if type(current) is int and type(maximum) is int and (current<0 or maximum<=0 or maximum-current<512*1024*1024):return 'CGROUP_HEADROOM_GUARD'
    return None


def _admitted(value):
    if type(value) is not dict or set(value)!=ADMISSION_KEYS:raise _Stop('ADMISSION_SCHEMA')
    trace=_copy(value,MAX_ADMISSION_BYTES)
    if trace['version']!='fresh-retained-root-admission-v1' or trace['status'] not in ('ADMITTED','SKIPPED'):raise _Stop('ADMISSION_SCHEMA')
    if trace['status']!='ADMITTED':return trace,False
    samples=trace['samples']
    if (trace['reason']!='FRESH_COMPLETE_SAFE' or trace['max_samples']!=3 or type(trace['max_samples']) is not int
        or trace['budget_seconds']!=2.5 or type(samples) is not list or not 1<=len(samples)<=3
        or type(trace['selected_sample_index']) is not int or trace['selected_sample_index']!=len(samples)):
        raise _Stop('ADMISSION_SCHEMA')
    if _unsafe_sample(trace['cached_tail_memory']):return trace,False
    previous=None;sum_elapsed=0.
    for index,entry in enumerate(samples,1):
        if type(entry) is not dict or set(entry)!={'index','started_at_utc','completed_at_utc','elapsed_ms','sample','decision'}:raise _Stop('ADMISSION_ENTRY_SCHEMA')
        if type(entry['index']) is not int or entry['index']!=index:raise _Stop('ADMISSION_ENTRY_SCHEMA')
        started=_utc(entry['started_at_utc']);completed=_utc(entry['completed_at_utc'])
        sc=datetime.fromisoformat(started.replace('Z','+00:00'));ec=datetime.fromisoformat(completed.replace('Z','+00:00'))
        if sc>ec or previous is not None and previous>sc:raise _Stop('ADMISSION_ENTRY_CLOCK_ORDER')
        previous=ec;elapsed=entry['elapsed_ms']
        if type(elapsed) not in (int,float) or not math.isfinite(elapsed) or elapsed<0:raise _Stop('ADMISSION_ENTRY_SCHEMA')
        sum_elapsed+=elapsed
        if sum_elapsed>2500.01:raise _Stop('ADMISSION_COMPLETED_CALL_BUDGET')
        current=entry['sample']
        if type(current) is not dict or _unsafe_sample(current):return trace,False
        if type((current.get('limits') or {}).get('budget_ms')) is not int or current['limits']['budget_ms']!=500:raise _Stop('ADMISSION_SAMPLER_PROFILE')
        if index<len(samples):
            errors=current.get('errors')
            if (entry['decision']!='RETRY_TRANSIENT_COVERAGE' or current.get('status') not in ('PARTIAL','UNAVAILABLE')
                or type(errors) is not dict or not errors or not set(errors)<={'TIME_BUDGET','ANCESTOR_UNVERIFIED'}):
                raise _Stop('ADMISSION_RETRY_NOT_TRANSIENT_COVERAGE')
        elif entry['decision']!='ADMIT':raise _Stop('ADMISSION_FINAL_DECISION')
    sample=samples[-1]['sample']
    if type(sample) is not dict:return trace,False
    children=sample.get('descendants') or {};group=sample.get('cgroup') or {};rss=(sample.get('self') or {}).get('rss_bytes')
    current=group.get('current_bytes');maximum=group.get('max_bytes')
    safe=(sample.get('status')=='COMPLETE' and type(rss) is int and 0<=rss<=MAX_SELF_RSS_BYTES
        and all(type((children.get(g) or {}).get('process_count')) is int and (children[g]['process_count']==0) for g in ('browser_named','other'))
        and type(current) is int and type(maximum) is int and maximum-current>=512*1024*1024
        and (sample.get('limits') or {}).get('budget_ms')==500)
    return trace,safe


def _minimal_result(result,code):
    """A serialization failure must never erase a consumed native attempt."""
    trim=result['trim'];attempted=trim['attempted'] is True;returned=trim['return_value']
    return {'version':VERSION,'status':'PARTIAL' if attempted else 'UNAVAILABLE','reason_codes':[code],
        'identity':result['identity'],'started_at_utc':result['started_at_utc'],'completed_at_utc':result['completed_at_utc'],
        'elapsed_ms':result['elapsed_ms'],'budget_seconds':MAX_SECONDS,'scope':SCOPE,'components_omitted':True,
        'trim':{'attempted':attempted,'call_count':1 if attempted else 0,'pad_bytes':0,
            'return_value':returned if type(returned) is int and returned in (0,1) else None,
            'valid_return':trim['valid_return'] is True,'outcome':trim['outcome'],'started_at_utc':trim['started_at_utc'],
            'completed_at_utc':trim['completed_at_utc'],'elapsed_ms':trim['elapsed_ms'],
            'return_semantics':'ZERO_OR_ONE_NOT_BYTES_FREED','once_scope':'THIS_INVOCATION_ONLY_CONTROLLER_OWNS_DURABLE_ORIGINAL_SLOT_CLAIM'},
        'snapshots':[{'name':s['name'],'status':s['status'],'components_omitted':True} for s in result['snapshots']],
        'protocol_complete':result['protocol_complete'],'all_five_snapshots_complete':False,
        'execution_all_five_snapshot_statuses_complete':all(s['status']=='COMPLETE' for s in result['snapshots']),'raw_smaps_persisted':False,
        'conditional_resident_free_bytes':None,'deltas':{},'emissions':{},'admission':None}


def _finalize(result):
    try:
        for name,a,b in (('SHAM_AFTER_MINUS_SHAM_BEFORE',0,1),('POST_TRIM_MINUS_PRE_TRIM',2,3),('SETTLED_MINUS_PRE_TRIM',2,4)):
            before,after=result['snapshots'][a],result['snapshots'][b];valid=before['status']==after['status']=='COMPLETE'
            def difference(x,y):return y-x if type(x) is int and type(y) is int else None
            row={'from':before['name'],'to':after['name'],'complete_pair':valid,'process_rss_bytes':None,'python_allocated_blocks_count':None,
                'smaps_totals_bytes':{},'glibc_fields':{},'scope':'RAW_SEQUENTIAL_ENDPOINT_CHANGES_NO_SHAM_SUBTRACTION_CAUSAL_EFFECT_OR_ALLOCATOR_OWNERSHIP'}
            if valid:
                p,q=before['process_after'],after['process_after'];row['process_rss_bytes']=difference(p['rss_bytes'],q['rss_bytes'])
                row['python_allocated_blocks_count']=difference(p['python_counters']['allocated_blocks_count'],q['python_counters']['allocated_blocks_count'])
                x,y=before['smaps']['covered_totals_bytes'],after['smaps']['covered_totals_bytes']
                row['smaps_totals_bytes']={k:difference(x.get(k),y.get(k)) for k in x if k in y}
                x=p['allocator'].get('fields') or {};y=q['allocator'].get('fields') or {}
                row['glibc_fields']={k:difference(x.get(k),y.get(k)) for k in x if k in y}
            result['deltas'][name]=row
        if len(json.dumps(result,ensure_ascii=True,allow_nan=False,separators=(',',':')).encode())>MAX_RESULT_BYTES:
            return _minimal_result(result,'RESULT_BYTE_BOUND_COMPONENTS_OMITTED')
        return result
    except BaseException:
        return _minimal_result(result,'RESULT_FINALIZATION_FAILED_COMPONENTS_OMITTED')


def run_resident_probe(*,smaps_opener=None,process_snapshot=None,parse_smaps=None,
        monotonic=time.monotonic,utc_now=None,sleep=time.sleep,native_factory=None,
        context_check=None,admission=None,emit=None,identity=None):
    """One caller-gated trial. All tests must inject native/read/sleep callbacks.

    context_check returns exactly True only for the original still-safe context.
    admission returns the existing retained fresh-admission trace. The controller
    must also perform outer admission before invoking this function/factory.
    emit receives BEFORE_SHAM or AFTER_SETTLED plus the four-field identity.
    """
    result={'version':VERSION,'status':'UNAVAILABLE','reason_codes':[],'identity':None,'started_at_utc':None,'completed_at_utc':None,'elapsed_ms':None,
        'budget_seconds':MAX_SECONDS,'budget_semantics':'COOPERATIVE_CHECKS_NO_HARD_NATIVE_CALL_PREEMPTION',
        'snapshots':[{'name':name,'status':'NOT_CAPTURED','reason_codes':['NOT_REACHED']} for name in SNAPSHOT_NAMES],
        'native':{'status':'NOT_PREPARED','info':None,'function_serialized':False},'admission':None,'admission_return_to_trim_start_ms':None,
        'admission_valid_for_trim':False,'deltas':{},
        'trim':{'attempted':False,'call_count':0,'pad_bytes':0,'return_value':None,'valid_return':False,'outcome':'NOT_ATTEMPTED',
            'started_at_utc':None,'completed_at_utc':None,'elapsed_ms':None,'return_semantics':'ZERO_OR_ONE_NOT_BYTES_FREED','once_scope':'THIS_INVOCATION_ONLY_CONTROLLER_OWNS_DURABLE_ORIGINAL_SLOT_CLAIM'},
        'emissions':{},'protocol_complete':False,'all_five_snapshots_complete':False,'raw_smaps_persisted':False,'cache_clear_performed':False,'gc_performed':False,
        'conditional_resident_free_bytes':None,'scope':SCOPE}
    clock=monotonic;utc=utc_now or _now;opener=smaps_opener or _smaps_opener;process=process_snapshot or _process_snapshot
    factory=native_factory or _native_factory;once_lock=threading.Lock();start=None;deadline=None
    def tick():
        value=clock()
        if type(value) not in (int,float) or not math.isfinite(value):raise _Stop('MONOTONIC_CLOCK_INVALID')
        return value
    def stamp():return _utc(utc())
    def reason(code):
        if code not in result['reason_codes']:result['reason_codes'].append(code)
    def check_budget():
        if tick()>=deadline:raise _Stop('TRIAL_TIME_BOUND')
    def context():
        try:value=context_check() if context_check is not None else None
        except BaseException:return {'status':'UNAVAILABLE','is_current':None}
        return {'status':'SAFE' if value is True else 'UNSAFE' if value is False else 'UNAVAILABLE','is_current':value if type(value) is bool else None}
    def require_context():
        value=context()
        if value['is_current'] is not True:raise _Stop('WATCH_CONTEXT_UNSAFE_OR_UNKNOWN')
        return value
    def wait_until(target):
        check_budget();remaining=max(0.,target-tick())
        if target>=deadline:raise _Stop('TRIAL_TIME_BOUND')
        if remaining:
            try:sleep(remaining)
            except BaseException:raise _Stop('WAIT_EXCEPTION')
        check_budget()
        if tick()+0.000001<target:raise _Stop('WAIT_RETURNED_EARLY')
    def emission(label):
        if emit is None:result['emissions'][label]={'status':'UNAVAILABLE','reason':'NO_EMITTER'};return
        try:result['emissions'][label]=_copy(emit(label,dict(result['identity'])),4096)
        except BaseException:result['emissions'][label]={'status':'UNAVAILABLE','reason':'EMITTER_EXCEPTION_OR_SCHEMA'}
    def take(index):
        check_budget();item={'name':SNAPSHOT_NAMES[index],'status':'UNAVAILABLE','reason_codes':[],'started_at_utc':stamp(),'completed_at_utc':None,
            'elapsed_ms':None,'context_before':context(),'context_after':None,'process_before':None,'process_after':None,'smaps':None,
            'smaps_read':{'read_calls':0,'read_bytes':0,'eof':False,'reason':None,'read_parse_elapsed_ms':None,'cooperative_budget_ms':500},'scope':'SEQUENTIAL_PROCESS_SMAPS_PROCESS_GAUGES_NOT_ATOMIC_OR_ALLOCATOR_OWNERSHIP'}
        result['snapshots'][index]=item;began=tick();raw=None
        if item['context_before']['is_current'] is not True:
            item['reason_codes'].append('WATCH_CONTEXT_UNSAFE_OR_UNKNOWN')
        else:
            for which in ('process_before','smaps','process_after'):
                try:
                    check_budget()
                    if which!='smaps':item[which]=_safe_process(process());continue
                    chunks=[];count=0;eof=False;read_start=tick()
                    def smaps_time():
                        if tick()-read_start>=MAX_SMAPS_SECONDS:raise _Stop('SMAPS_READ_PARSE_TIME_BOUND')
                    with opener() as stream:
                        while item['smaps_read']['read_calls']<MAX_READ_CALLS:
                            check_budget();smaps_time();requested=min(READ_CHUNK_BYTES,MAX_SMAPS_BYTES-count+1)
                            piece=stream.read(requested);item['smaps_read']['read_calls']+=1
                            if type(piece) is not bytes:raise _Stop('SMAPS_READ_TYPE')
                            if len(piece)>requested:raise _Stop('SMAPS_READ_CHUNK_BOUND')
                            count+=len(piece);item['smaps_read']['read_bytes']=count
                            if count>MAX_SMAPS_BYTES:raise _Stop('SMAPS_BYTE_BOUND')
                            if not piece:eof=True;item['smaps_read']['eof']=True
                            smaps_time()
                            if eof:break
                            chunks.append(piece)
                    if not eof:item['smaps_read']['reason']='SMAPS_READ_CALL_BOUND'
                    item['smaps_read']['eof']=eof;raw=b''.join(chunks);del chunks
                    parser=parse_smaps
                    if parser is None:
                        from research_resident_smaps import parse_smaps as parser
                    smaps_time();item['smaps']=_safe_smaps(parser(raw,read_complete=eof));raw=None;smaps_time()
                except _Stop as error:
                    code=str(error);item['reason_codes'].append(code)
                    if which=='smaps':item['smaps_read']['reason']=code
                except BaseException:
                    code='SMAPS_EXCEPTION' if which=='smaps' else 'PROCESS_EXCEPTION';item['reason_codes'].append(code)
                    if which=='smaps':item['smaps_read']['reason']=code
                finally:
                    raw=None
                    if which=='smaps' and 'read_start' in locals():item['smaps_read']['read_parse_elapsed_ms']=round(max(0.,tick()-read_start)*1000,3)
        item['context_after']=context()
        if item['context_after']['is_current'] is not True:item['reason_codes'].append('WATCH_CONTEXT_UNSAFE_OR_UNKNOWN')
        item['completed_at_utc']=stamp();item['elapsed_ms']=round(max(0.,tick()-began)*1000,3)
        complete=(not item['reason_codes'] and item['context_before']['is_current'] is True and item['context_after']['is_current'] is True
            and item['smaps_read']['eof'] and item['smaps'] is not None and item['smaps']['status']=='COMPLETE'
            and all(item[k] is not None and item[k]['status']=='COMPLETE' for k in ('process_before','process_after')))
        item['status']='COMPLETE' if complete else 'PARTIAL' if any(item[k] is not None for k in ('process_before','smaps','process_after')) else 'UNAVAILABLE'
        return item
    try:
        result['identity']=_identity(identity);start=tick();deadline=start+MAX_SECONDS;result['started_at_utc']=stamp()
        require_context();check_budget()
        try:backend=factory()
        except BaseException:raise _Stop('NATIVE_FACTORY_EXCEPTION')
        if type(backend) is not dict:raise _Stop('NATIVE_FACTORY_SCHEMA')
        info=backend.get('info');result['native']={'status':backend.get('status') if backend.get('status') in ('READY','UNAVAILABLE') else 'UNAVAILABLE','info':_copy(info,2048),'function_serialized':False}
        if backend.get('status')!='READY' or not _runtime_supported(info) or not callable(backend.get('trim')):raise _Stop('UNSUPPORTED_RUNTIME_ABI_GLIBC')
        check_budget();require_context();emission('BEFORE_SHAM')
        if take(0)['status']!='COMPLETE':raise _Stop('SHAM_BEFORE_INCOMPLETE')
        wait_until(tick()+1.)
        if take(1)['status']!='COMPLETE':raise _Stop('SHAM_AFTER_INCOMPLETE')
        require_context();check_budget()
        if admission is None:raise _Stop('NO_FRESH_ADMISSION')
        try:
            received=admission();result['admission']=_copy(received,MAX_ADMISSION_BYTES);trace,admitted=_admitted(received)
        except _Stop:raise
        except BaseException:
            result['admission']={'status':'UNAVAILABLE','reason':'ADMISSION_EXCEPTION'};raise _Stop('ADMISSION_EXCEPTION')
        result['admission']=trace;admission_return=tick()
        if not admitted:raise _Stop('FRESH_ADMISSION_NOT_SAFE')
        result['admission_valid_for_trim']=True
        if take(2)['status']!='COMPLETE':raise _Stop('PRE_TRIM_INCOMPLETE')
        for process_key in ('process_before','process_after'):
            rss=result['snapshots'][2][process_key]['rss_bytes']
            if type(rss) is not int or not 0<=rss<=MAX_SELF_RSS_BYTES:raise _Stop('PRE_TRIM_SELF_RSS_GUARD')
        require_context();check_budget();trim_stamp=stamp();trim_start=tick();age=trim_start-admission_return
        result['admission_return_to_trim_start_ms']=round(age*1000,3)
        if age<0 or age>FRESH_AGE_SECONDS:raise _Stop('FRESH_ADMISSION_STALE')
        with once_lock:
            if result['trim']['attempted']:raise _Stop('TRIM_ALREADY_ATTEMPTED')
            result['trim'].update(attempted=True,call_count=1,started_at_utc=trim_stamp,outcome='CALL_IN_FLIGHT')
        try:
            returned=backend['trim'](0)
            if type(returned) is int and returned in (0,1):result['trim'].update(return_value=returned,valid_return=True,outcome='RETURNED')
            else:result['trim']['outcome']='INVALID_RETURN_NOT_RETRIED';reason('TRIM_INVALID_RETURN')
        except BaseException:
            result['trim']['outcome']='EXCEPTION_NOT_RETRIED';reason('TRIM_EXCEPTION')
        trim_return=tick();result['trim']['completed_at_utc']=stamp();result['trim']['elapsed_ms']=round(max(0.,trim_return-trim_start)*1000,3)
        wait_until(max(trim_return,trim_start+1.));take(3)
        wait_until(trim_return+5.);take(4)
        check_budget();emission('AFTER_SETTLED')
        result['protocol_complete']=True
        result['all_five_snapshots_complete']=all(s['status']=='COMPLETE' for s in result['snapshots'])
        result['status']='COMPLETE' if result['all_five_snapshots_complete'] and result['trim']['valid_return'] else 'PARTIAL'
    except _Stop as error:
        reason(str(error));result['status']='PARTIAL' if result['trim']['attempted'] or any(s['status']!='NOT_CAPTURED' for s in result['snapshots']) else 'UNAVAILABLE'
    except BaseException:
        reason('TRIAL_DIAGNOSTIC_EXCEPTION');result['status']='PARTIAL' if result['trim']['attempted'] or any(s['status']!='NOT_CAPTURED' for s in result['snapshots']) else 'UNAVAILABLE'
    finally:
        try:result['completed_at_utc']=stamp()
        except BaseException:reason('UTC_CLOCK_UNAVAILABLE')
        try:result['elapsed_ms']=round(max(0.,tick()-start)*1000,3) if start is not None else None
        except BaseException:reason('MONOTONIC_CLOCK_UNAVAILABLE')
        result['all_five_snapshots_complete']=all(s['status']=='COMPLETE' for s in result['snapshots'])
        result=_finalize(result)
    return result
