"""Bounded passive process gauges. No import-time I/O, native calls or workers.

Call only from the bounded diagnostic worker. Reads/native calls can exceed a
cooperative clock budget; these gauges do not identify Python/cache ownership.
"""
from __future__ import annotations
from datetime import datetime,timezone
import gc
import math
import re
import sys
import time

VERSION='process-memory-diagnostics-v1'
STATUS_CAP=16*1024
SMAPS_CAP=64*1024
MAX_VALUE=2**63-1
SMAPS_FIELDS=('Rss','Pss','Pss_Anon','Pss_File','Pss_Shmem','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty','Referenced','Anonymous','LazyFree','AnonHugePages','ShmemPmdMapped','FilePmdMapped','Shared_Hugetlb','Private_Hugetlb','Swap','SwapPss','Locked')
SMAPS_REQUIRED={'Rss','Pss','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty','Anonymous'}
MALL_FIELDS=('arena','ordblks','smblks','hblks','hblkhd','usmblks','fsmblks','uordblks','fordblks','keepcost')
MALL_COUNT_FIELDS={'ordblks','smblks','hblks'}
MALL_SCOPE='GLIBC_REPORTED_ALLOCATOR_TOTALS_VERSION_DEPENDENT_ARENA_COVERAGE_NOT_RSS_ALL_MAPPINGS_OR_PYTHON_CACHE_OWNERSHIP'
PROCESS_SCOPE='PROCESS_MAPPING_AND_ALLOCATOR_GAUGES_NONATOMIC_NOT_CACHE_BYTES_OR_ALLOCATION_OWNERSHIP'

def _integer(value):return type(value) is int and 0<=value<=MAX_VALUE
def _utc():return datetime.now(timezone.utc).isoformat()
def _read_file(path,cap):
    with open(path,'rb',buffering=0) as stream:return stream.read(cap+1)

def _parse(raw,fields,required,cap):
    values={key:None for key in fields};errors=set();seen=set()
    if type(raw) is not bytes:return {'status':'UNAVAILABLE','fields_bytes':values,'missing_required':sorted(required),'errors':['INVALID_READ_TYPE']}
    if len(raw)>cap:return {'status':'UNAVAILABLE','fields_bytes':values,'missing_required':sorted(required),'errors':['READ_CAP']}
    for line in raw.splitlines():
        key,sep,tail=line.partition(b':')
        if not sep:continue
        try:name=key.decode('ascii')
        except UnicodeError:continue
        if name not in values:continue
        if name in seen:values[name]=None;errors.add('DUPLICATE_FIELD');continue
        seen.add(name)
        match=re.fullmatch(rb'\s*([0-9]{1,18})\s+kB\s*',tail)
        if match is None:errors.add('INVALID_FIELD');continue
        value=int(match.group(1))*1024
        if value>MAX_VALUE:errors.add('FIELD_BOUND');continue
        values[name]=value
    missing=sorted(key for key in required if values[key] is None)
    if missing:errors.add('MISSING_REQUIRED_FIELD')
    status='COMPLETE' if not errors else 'PARTIAL' if any(x is not None for x in values.values()) else 'UNAVAILABLE'
    return {'status':status,'fields_bytes':values,'missing_required':missing,'errors':sorted(errors)}

def parse_status(raw):return _parse(raw,('VmRSS',),{'VmRSS'},STATUS_CAP)
def parse_smaps_rollup(raw):return _parse(raw,SMAPS_FIELDS,SMAPS_REQUIRED,SMAPS_CAP)

def _unavailable_mall(reason,status='UNAVAILABLE'):
    return {'status':status,'glibc_version':None,'size_t_bytes':None,'fields':None,'field_units':None,'coherence':None,'scope':MALL_SCOPE,'errors':[] if status=='NOT_REQUESTED' else [reason]}

def read_mallinfo2(*,platform_name=None,ctypes_api=None,loader=None):
    """One opt-in glibc ABI call; tests inject all native interfaces.

    ctypes cannot recover native crashes. Linux/64-bit size_t, symbol/version
    and exact ten-size_t layout guards precede the call. No allocator tuning.
    """
    try:
        if (sys.platform if platform_name is None else platform_name)!='linux':return _unavailable_mall('UNSUPPORTED_PLATFORM')
        if ctypes_api is None:
            import ctypes as ctypes_api
        if ctypes_api.sizeof(ctypes_api.c_size_t)!=8 or ctypes_api.sizeof(ctypes_api.c_void_p)!=8:return _unavailable_mall('UNSUPPORTED_ABI')
        libc=(ctypes_api.CDLL if loader is None else loader)(None)
        version_fn=libc.gnu_get_libc_version;version_fn.argtypes=[];version_fn.restype=ctypes_api.c_char_p
        raw_version=version_fn()
        if type(raw_version) is not bytes or len(raw_version)>32:return _unavailable_mall('INVALID_GLIBC_VERSION')
        version=raw_version.decode('ascii');match=re.fullmatch(r'(\d{1,3})\.(\d{1,3})(?:\.\d{1,3})?',version)
        if match is None or tuple(map(int,match.groups()[:2]))<(2,33):return _unavailable_mall('UNSUPPORTED_GLIBC_VERSION')
        structure=type('Mallinfo2',(ctypes_api.Structure,),{'_fields_':[(name,ctypes_api.c_size_t) for name in MALL_FIELDS]})
        if ctypes_api.sizeof(structure)!=80 or ctypes_api.alignment(structure)!=ctypes_api.alignment(ctypes_api.c_size_t):return _unavailable_mall('UNSUPPORTED_ABI')
        function=libc.mallinfo2;function.argtypes=[];function.restype=structure
        observed=function();values={name:getattr(observed,name) for name in MALL_FIELDS}
        if not all(_integer(value) for value in values.values()):return _unavailable_mall('INVALID_MALLINFO_FIELDS')
        coherent=(values['arena']==values['uordblks']+values['fordblks'] and values['fsmblks']<=values['fordblks'] and values['keepcost']<=values['fordblks'])
        return {'status':'COMPLETE' if coherent else 'PARTIAL','glibc_version':version,'size_t_bytes':8,'fields':values,'field_units':{name:'COUNT' if name in MALL_COUNT_FIELDS else 'BYTES' for name in MALL_FIELDS},'coherence':coherent,'scope':MALL_SCOPE,'errors':[] if coherent else ['INCOHERENT_MALLINFO_SNAPSHOT']}
    except (ImportError,AttributeError):return _unavailable_mall('NATIVE_API_UNAVAILABLE')
    except BaseException:return _unavailable_mall('NATIVE_READ_ERROR')

def sample(*,include_mallinfo2=False,read_file=None,clock=None,utc_now=None,platform_name=None,getallocatedblocks=None,getgccounts=None,mallinfo_reader=None):
    """Read a finite numeric snapshot; diagnostics never replace native errors."""
    monotonic=time.monotonic if clock is None else clock;wall=_utc if utc_now is None else utc_now;reader=_read_file if read_file is None else read_file
    platform_value=sys.platform if platform_name is None else platform_name
    errors=[];started_at=completed_at=None;started=ended=None
    try:started=monotonic();started_at=wall()
    except BaseException:errors.append('CLOCK_ERROR')
    unavailable=lambda reason,fields,required:{'status':'UNAVAILABLE','fields_bytes':{key:None for key in fields},'missing_required':sorted(required),'errors':[reason]}
    if platform_value!='linux':
        status=unavailable('UNSUPPORTED_PLATFORM',('VmRSS',),{'VmRSS'});smaps=unavailable('UNSUPPORTED_PLATFORM',SMAPS_FIELDS,SMAPS_REQUIRED)
    else:
        try:status=parse_status(reader('/proc/self/status',STATUS_CAP))
        except BaseException:status=unavailable('READ_ERROR',('VmRSS',),{'VmRSS'})
        try:smaps=parse_smaps_rollup(reader('/proc/self/smaps_rollup',SMAPS_CAP))
        except BaseException:smaps=unavailable('READ_ERROR',SMAPS_FIELDS,SMAPS_REQUIRED)
    blocks=counts=None;python_errors=[]
    try:
        blocks=(sys.getallocatedblocks if getallocatedblocks is None else getallocatedblocks)()
        if not _integer(blocks):blocks=None;python_errors.append('INVALID_BLOCK_COUNT')
    except BaseException:python_errors.append('BLOCK_COUNTER_UNAVAILABLE')
    try:
        value=(gc.get_count if getgccounts is None else getgccounts)()
        if type(value) not in (tuple,list) or len(value)!=3 or not all(type(x) is int and -MAX_VALUE<=x<=MAX_VALUE for x in value):python_errors.append('INVALID_GC_COUNT_TUPLE')
        else:counts=list(value)
    except BaseException:python_errors.append('GC_COUNTER_UNAVAILABLE')
    if type(include_mallinfo2) is not bool:
        allocator=_unavailable_mall('INVALID_CONFIGURATION');errors.append('INVALID_CONFIGURATION')
    elif not include_mallinfo2:allocator=_unavailable_mall('',status='NOT_REQUESTED')
    else:
        try:
            allocator=read_mallinfo2(platform_name=platform_value) if mallinfo_reader is None else mallinfo_reader()
            if type(allocator) is not dict or allocator.get('status') not in {'COMPLETE','PARTIAL','UNAVAILABLE'}:allocator=_unavailable_mall('INVALID_NATIVE_RESULT')
        except BaseException:allocator=_unavailable_mall('NATIVE_READ_ERROR')
    try:ended=monotonic();completed_at=wall()
    except BaseException:errors.append('CLOCK_ERROR')
    elapsed=(ended-started)*1000 if type(started) in (int,float) and type(ended) in (int,float) and math.isfinite(started) and math.isfinite(ended) and ended>=started else None
    if elapsed is None:errors.append('CLOCK_ERROR')
    statuses=[status['status'],smaps['status']]
    if include_mallinfo2 is True:statuses.append(allocator['status'])
    quality='COMPLETE' if all(x=='COMPLETE' for x in statuses) and not errors and not python_errors else 'PARTIAL' if any(v is not None for v in (status['fields_bytes']['VmRSS'],blocks,counts)) or any(v is not None for v in smaps['fields_bytes'].values()) else 'UNAVAILABLE'
    return {'version':VERSION,'status':quality,'platform':'LINUX_PROCFS' if platform_value=='linux' else 'UNSUPPORTED','sample_started_at_utc':started_at,'sample_completed_at_utc':completed_at,'elapsed_ms':elapsed,'rss_bytes':status['fields_bytes']['VmRSS'],'status_read':status,'smaps_rollup':smaps,'python_counters':{'allocated_blocks_count':blocks,'gc_count_tuple':counts,'errors':python_errors,'scope':'COUNTS_NOT_HEAP_BYTES_LIVE_OBJECTS_OR_COLLECTION_CAUSE'},'allocator':allocator,'limits':{'status_bytes':STATUS_CAP,'smaps_rollup_bytes':SMAPS_CAP,'status_read_calls_max':1,'smaps_read_calls_max':1},'errors':sorted(set(errors)),'scope':PROCESS_SCOPE,'observer_effect':'This diagnostic allocates temporary Python objects and native loading may allocate; sequential readings are not atomic and no overhead subtraction identifies ownership.'}
