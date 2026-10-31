"""Independent bounded durable observations associated with Watch cleanup tail.

No I/O, threads or database work happens on import. Queue acceptance is not a
durable acknowledgement. These records neither reopen the Watch journal nor
prove coroutine return, global process idleness, or allocation ownership.
"""
from __future__ import annotations
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import threading

VERSION = 'runtime-memory-observation-v1'
TABLE = 'research_runtime_memory_observations'
SCOPE = 'POST_WATCH_CLEANUP_TAIL_ASSOCIATED_MEMORY_NOT_COROUTINE_RETURN_PROCESS_IDLE_OR_ALLOCATION_OWNERSHIP'
ACTIVITY_COUNT_SCOPE = 'ACTIVE_WATCH_OBSERVATION_HANDLES_NOT_NATIVE_EXECUTION_CLEANUP_OR_GLOBAL_IDLE'
MAX_EVENT_BYTES = 16 * 1024
MAX_MEMORY_BYTES = 8192
MAX_QUEUE_ITEMS = 4
MAX_QUEUE_BYTES = 64 * 1024
CONNECT_TIMEOUT_SECONDS = 3
STATEMENT_TIMEOUT_MS = 3000
LOCK_TIMEOUT_MS = 1000
DELAYS = {2:30, 3:120, 4:300, 5:600}
SCHEDULE_TOLERANCE_MS = 1.0
SOURCE_FILES = frozenset({'main.py','research_runtime_memory.py','research_watch_runtime.py','research_runtime_memory_observations.py'})
OPTIONAL_SOURCE_FILES = frozenset({'research_runtime_journal.py'})
_FORBIDDEN = frozenset({'chat_id','recipient','recipient_id','message','message_text','text','token','secret','password','database_url','api_key','authorization','credentials','cmdline','environ','command','pid'})
_HEX = re.compile(r'[0-9a-f]{64}\Z')
_QUEUE = queue.Queue(maxsize=MAX_QUEUE_ITEMS)
_LOCK = threading.Lock()
_WORKER_LOCK = threading.Lock()
_WORKER = None
_STORE = None
_METRICS = dict(record_calls=0,enqueue_attempts=0,enqueued=0,persisted=0,failed=0,invalid=0,dropped=0,unavailable=0,
    idempotent_existing=0,queued_items=0,queued_bytes=0,last=None,last_durable=None)


class ObservationValidationError(ValueError):
    def __init__(self,code): self.code=code; super().__init__(code)


def _require(value,code):
    if not value: raise ObservationValidationError(code)


def canonical(value): return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def digest(value): return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()
def _utc_now(): return datetime.now(timezone.utc).isoformat()


def _clock():
    try: return _utc_now()
    except BaseException: return None


def _valid_clock(value):
    if type(value) is not str or len(value)>64: return False
    try: return datetime.fromisoformat(value.replace('Z','+00:00')).tzinfo is not None
    except (TypeError,ValueError): return False


def _integer(value,maximum=2**63-1): return type(value) is int and 0<=value<=maximum


def _copy_json(value,max_bytes=MAX_EVENT_BYTES):
    nodes,active=0,set()
    def visit(item,depth):
        nonlocal nodes
        nodes+=1; _require(nodes<=2048 and depth<=10,'JSON_STRUCTURE_BOUND')
        kind=type(item)
        if item is None or kind is bool: return item
        if kind is str:
            _require(len(item.encode('utf-8'))<=max_bytes,'JSON_STRING_BOUND'); return item
        if kind is int: _require(item.bit_length()<=128,'JSON_INTEGER_BOUND'); return item
        if kind is float: _require(math.isfinite(item),'JSON_NONFINITE_NUMBER'); return item
        _require(kind in (dict,list) and id(item) not in active and len(item)<=512,'JSON_TYPE_OR_COLLECTION_BOUND')
        active.add(id(item))
        try:
            if kind is list: return [visit(part,depth+1) for part in item]
            result={}
            for key,part in item.items():
                _require(type(key) is str and len(key)<=128 and key.lower() not in _FORBIDDEN,'JSON_FORBIDDEN_OR_INVALID_KEY')
                result[key]=visit(part,depth+1)
            return result
        finally: active.remove(id(item))
    result=visit(value,0)
    _require(len(canonical(result).encode('utf-8'))<=max_bytes,'JSON_BYTE_BOUND')
    return result


@lru_cache(maxsize=1)
def _writer_code_sha256(): return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _sample_valid(sample):
    try:
        from research_runtime_memory import validate_sample
        return validate_sample(sample) is True
    except BaseException: return False


def _activity(before,after,revision_before,revision_after):
    _require(_integer(before,2) and _integer(after,2) and _integer(revision_before) and _integer(revision_after) and revision_after>=revision_before,'WATCH_ACTIVITY_METADATA')
    if revision_after!=revision_before or before or after: return 'WATCH_ACTIVITY_OBSERVED_OR_CHANGED'
    return 'NO_ACTIVE_WATCH_HANDLE_OBSERVED'


def prepare(event):
    value=_copy_json(event)
    expected={'version','process_session_id','origin_cycle_id','observation_index','observation_kind','cleanup_tail_at_utc','scheduled_delay_seconds','observed_delay_ms','event_time_utc','sample_started_at_utc','sample_completed_at_utc','watch_active_before','watch_active_after','watch_activity_revision_before','watch_activity_revision_after','activity_scope','activity_count_scope','runtime_git_commit','source_code_sha256','memory','capture_flags','scope'}
    _require(type(value) is dict and set(value)==expected and value['version']==VERSION,'OBSERVATION_ENVELOPE')
    _require(type(value['process_session_id']) is str and re.fullmatch('[0-9a-f]{32}',value['process_session_id']),'PROCESS_SESSION_ID')
    cycle=value['origin_cycle_id']
    _require(type(cycle) is str and cycle.startswith('shared-watch:') and len(cycle)<=200 and _valid_clock(cycle[len('shared-watch:'):]),'ORIGIN_CYCLE_ID')
    _require(_valid_clock(value['cleanup_tail_at_utc']),'CLEANUP_TAIL_CLOCK')
    _require(value['event_time_utc'] is None or _valid_clock(value['event_time_utc']),'EVENT_CLOCK')
    _require(value['runtime_git_commit'] is None or type(value['runtime_git_commit']) is str and re.fullmatch('[0-9a-f]{40,64}',value['runtime_git_commit']),'RUNTIME_COMMIT')
    pins=value['source_code_sha256']
    _require(type(pins) is dict and SOURCE_FILES<=set(pins)<=SOURCE_FILES|OPTIONAL_SOURCE_FILES and all(type(part) is str and _HEX.fullmatch(part) for part in pins.values()),'SOURCE_PINS')
    _require(pins['research_runtime_memory_observations.py']==_writer_code_sha256(),'SOURCE_WRITER_PIN')
    flags=value['capture_flags']
    _require(type(flags) is list and len(flags)<=16 and flags==sorted(set(flags)) and all(type(flag) is str and re.fullmatch('[A-Z_]{1,80}',flag) for flag in flags),'CAPTURE_FLAGS')
    index=value['observation_index']; _require(type(index) is int and 1<=index<=5,'OBSERVATION_INDEX')
    if index==1:
        _require(value['observation_kind']=='CLEANUP_TAIL_REACHED' and type(value['scheduled_delay_seconds']) is int and value['scheduled_delay_seconds']==0 and value['observed_delay_ms'] is None,'TAIL_MARKER_SEMANTICS')
        _require(all(value[key] is None for key in ('sample_started_at_utc','sample_completed_at_utc','watch_active_before','watch_active_after','watch_activity_revision_before','watch_activity_revision_after')) and value['activity_scope']=='CLEANUP_TAIL_NOT_RETURN_OR_IDLE_PROOF' and value['memory']=={'status':'NOT_REQUESTED'},'TAIL_MARKER_METADATA')
    else:
        _require(value['observation_kind']=='POST_CLEANUP_MEMORY' and type(value['scheduled_delay_seconds']) is int and value['scheduled_delay_seconds']==DELAYS[index],'SAMPLE_SCHEDULE')
        delay=value['observed_delay_ms']
        _require(type(delay) in (int,float) and math.isfinite(delay) and 0<=delay<=24*60*60*1000,'OBSERVED_DELAY')
        _require(delay+SCHEDULE_TOLERANCE_MS>=value['scheduled_delay_seconds']*1000,'OBSERVATION_BEFORE_SCHEDULE')
        _require(_valid_clock(value['sample_started_at_utc']) and _valid_clock(value['sample_completed_at_utc']),'SAMPLE_CLOCKS')
        _require(datetime.fromisoformat(value['sample_started_at_utc'].replace('Z','+00:00'))<=datetime.fromisoformat(value['sample_completed_at_utc'].replace('Z','+00:00')),'SAMPLE_CLOCK_ORDER')
        _require(value['activity_scope']==_activity(value['watch_active_before'],value['watch_active_after'],value['watch_activity_revision_before'],value['watch_activity_revision_after']),'ACTIVITY_SCOPE')
        memory=value['memory']
        _require(type(memory) is dict and set(memory)=={'status','sample','sample_sha256'} and memory['status']=='CAPTURED' and _sample_valid(memory['sample']) and digest(memory['sample'])==memory['sample_sha256'] and len(canonical(memory['sample']).encode('utf-8'))<=MAX_MEMORY_BYTES,'MEMORY_SAMPLE')
    _require(value['scope']==SCOPE and value['activity_count_scope']==ACTIVITY_COUNT_SCOPE,'OBSERVATION_SCOPE')
    text=canonical(value)
    return {key:value[key] for key in ('process_session_id','origin_cycle_id','observation_index','observation_kind','activity_scope')} | {'payload_json':text,'payload_sha256':hashlib.sha256(text.encode('utf-8')).hexdigest(),'payload_bytes':len(text.encode('utf-8'))}


def _summary(prepared): return {key:prepared[key] for key in ('process_session_id','origin_cycle_id','observation_index','observation_kind','activity_scope','payload_sha256')}


def _error_code(error):
    if isinstance(error,ObservationValidationError): return error.code
    name=type(error).__name__
    return name if re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,79}',name) else 'Exception'


def _failure(code,status='FAILED',prepared=None):
    result={'version':VERSION,'status':status,'reason_code':code,'durable':False}
    if prepared is not None: result.update(_summary(prepared))
    try:
        with _LOCK:
            _METRICS[{'FAILED':'failed','INVALID':'invalid','DROPPED':'dropped','UNAVAILABLE':'unavailable'}.get(status,'failed')]+=1
            _METRICS['last']={**result,'at_utc':_clock()}
    except BaseException: pass
    return result


def _jsonb_equal(left,right):
    if type(left) in (int,float) and type(right) in (int,float): return left==right
    if type(left) is not type(right): return False
    if type(left) is dict: return set(left)==set(right) and all(_jsonb_equal(left[key],right[key]) for key in left)
    if type(left) is list: return len(left)==len(right) and all(_jsonb_equal(a,b) for a,b in zip(left,right))
    return left==right


def _verify_existing(row,prepared):
    fields=('process_session_id','origin_cycle_id','observation_index','observation_kind','activity_scope','payload_sha256','payload_json')
    _require(row is not None and all(type(row[key]) is type(prepared[key]) and row[key]==prepared[key] for key in fields),'OBSERVATION_IDENTITY_COLLISION')
    _require(row.get('writer_version')==VERSION and row.get('writer_code_sha256')==_writer_code_sha256(),'OBSERVATION_WRITER_IDENTITY_MISMATCH')
    _require(_jsonb_equal(row['payload'],json.loads(prepared['payload_json'])),'OBSERVATION_JSONB_MISMATCH')


class PostgresStore:
    def __init__(self,database_url): self.database_url,self.initialized=database_url,False
    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.database_url,connect_timeout=CONNECT_TIMEOUT_SECONDS,row_factory=dict_row)
    def persist(self,event,enqueued_at_utc=None): return self.persist_prepared(prepare(event),enqueued_at_utc or _clock())
    def persist_prepared(self,prepared,enqueued_at_utc):
        value=json.loads(prepared['payload_json'])
        _require(prepare(value)==prepared,'PREPARED_OBSERVATION_CHANGED')
        from psycopg.types.json import Jsonb
        with self._connect() as conn:
            conn.execute('SET LOCAL statement_timeout = 3000'); conn.execute('SET LOCAL lock_timeout = 1000')
            if not self.initialized:
                conn.execute('SELECT pg_advisory_xact_lock(%s)',(73182461390741542,))
                conn.execute('''CREATE TABLE IF NOT EXISTS research_runtime_memory_observations (
                    process_session_id TEXT NOT NULL,origin_cycle_id TEXT NOT NULL,observation_index INTEGER NOT NULL CHECK(observation_index BETWEEN 1 AND 5),
                    observation_kind TEXT NOT NULL,activity_scope TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL CHECK(octet_length(payload_json)<=16384),payload JSONB NOT NULL,enqueued_at_utc TIMESTAMPTZ,
                    writer_version TEXT NOT NULL,writer_code_sha256 TEXT NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                    PRIMARY KEY(process_session_id,origin_cycle_id,observation_index))''')
            inserted=conn.execute('''INSERT INTO research_runtime_memory_observations
                (process_session_id,origin_cycle_id,observation_index,observation_kind,activity_scope,payload_sha256,payload_json,payload,enqueued_at_utc,writer_version,writer_code_sha256)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(process_session_id,origin_cycle_id,observation_index) DO NOTHING RETURNING observation_index''',
                tuple(prepared[key] for key in ('process_session_id','origin_cycle_id','observation_index','observation_kind','activity_scope','payload_sha256','payload_json'))+(Jsonb(value),enqueued_at_utc,VERSION,_writer_code_sha256())).fetchone()
            row=conn.execute('''SELECT process_session_id,origin_cycle_id,observation_index,observation_kind,activity_scope,payload_sha256,payload_json,payload,writer_version,writer_code_sha256,created_at_utc
                FROM research_runtime_memory_observations WHERE process_session_id=%s AND origin_cycle_id=%s AND observation_index=%s''',
                (prepared['process_session_id'],prepared['origin_cycle_id'],prepared['observation_index'])).fetchone()
            _verify_existing(row,prepared)
            created=row['created_at_utc']
            if isinstance(created,datetime): created=created.astimezone(timezone.utc).isoformat()
            result={**_summary(prepared),'persisted':True,'idempotent_existing':inserted is None,'created_at_utc':created}
        self.initialized=True
        return result


def _persist_one(prepared,enqueued_at):
    try:
        ack=_STORE.persist_prepared(prepared,enqueued_at)
        with _LOCK:
            _METRICS['persisted']+=1; _METRICS['idempotent_existing']+=int(ack['idempotent_existing'])
            _METRICS['last']=_METRICS['last_durable']={**ack,'status':'PERSISTED','durable':True,'at_utc':_clock()}
    except BaseException as error: _failure(_error_code(error),prepared=prepared)
    finally:
        with _LOCK:
            _METRICS['queued_items']-=1; _METRICS['queued_bytes']-=prepared['payload_bytes']


def _run():
    while True:
        prepared,enqueued_at=_QUEUE.get()
        try: _persist_one(prepared,enqueued_at)
        finally: _QUEUE.task_done()


def _ensure_worker():
    global _WORKER,_STORE
    with _WORKER_LOCK:
        if _WORKER is not None and _WORKER.is_alive(): return True
        if _STORE is None:
            url=os.getenv('RESEARCH_DATABASE_URL','').strip() or os.getenv('DATABASE_URL','').strip()
            if not url: return False
            _STORE=PostgresStore(url)
        _WORKER=threading.Thread(target=_run,name='research-memory-observations',daemon=True); _WORKER.start()
        return True


def submit(event):
    """Validate/freeze/enqueue; no DB wait and no automatic retry. Never raise."""
    prepared=None
    try:
        with _LOCK: _METRICS['enqueue_attempts']+=1
        try: prepared=prepare(event)
        except BaseException as error: return _failure(_error_code(error),'INVALID')
        if not _ensure_worker(): return _failure('DATABASE_NOT_CONFIGURED','UNAVAILABLE',prepared)
        enqueued_at=_clock()
        with _LOCK:
            if _METRICS['queued_items']>=MAX_QUEUE_ITEMS or _METRICS['queued_bytes']+prepared['payload_bytes']>MAX_QUEUE_BYTES:
                _METRICS['dropped']+=1
                result={'version':VERSION,'status':'DROPPED','reason_code':'QUEUE_BOUND','durable':False,**_summary(prepared)}
                _METRICS['last']={**result,'at_utc':enqueued_at}; return result
            _METRICS['queued_items']+=1; _METRICS['queued_bytes']+=prepared['payload_bytes']
            try: _QUEUE.put_nowait((prepared,enqueued_at))
            except BaseException:
                _METRICS['queued_items']-=1; _METRICS['queued_bytes']-=prepared['payload_bytes']; raise
            _METRICS['enqueued']+=1
            result={'version':VERSION,'status':'QUEUED','durable':False,**_summary(prepared)}
            _METRICS['last']={**result,'at_utc':enqueued_at}; return result
    except BaseException as error: return _failure(_error_code(error),'UNAVAILABLE',prepared)


def _record(*,process_session_id,origin_cycle_id,cleanup_tail_at_utc,runtime_git_commit,source_code_sha256,observation_index,scheduled_delay_seconds,observed_delay_ms=None,sample_started_at_utc=None,sample_completed_at_utc=None,watch_active_before=None,watch_active_after=None,watch_activity_revision_before=None,watch_activity_revision_after=None,memory=None):
    try:
        with _LOCK: _METRICS['record_calls']+=1
        flags=[]; now=_clock()
        if now is None: flags.append('EVENT_CLOCK_UNAVAILABLE')
        if runtime_git_commit is None: flags.append('RUNTIME_COMMIT_UNAVAILABLE')
        if observation_index==1: envelope={'status':'NOT_REQUESTED'}; activity='CLEANUP_TAIL_NOT_RETURN_OR_IDLE_PROOF'
        else:
            frozen=_copy_json(memory,MAX_MEMORY_BYTES)
            _require(_sample_valid(frozen),'MEMORY_SAMPLE_INVALID')
            envelope={'status':'CAPTURED','sample':frozen,'sample_sha256':digest(frozen)}
            if frozen['status']!='COMPLETE': flags.append('MEMORY_SAMPLE_'+frozen['status'])
            activity=_activity(watch_active_before,watch_active_after,watch_activity_revision_before,watch_activity_revision_after)
        event={'version':VERSION,'process_session_id':process_session_id,'origin_cycle_id':origin_cycle_id,'observation_index':observation_index,'observation_kind':'CLEANUP_TAIL_REACHED' if observation_index==1 else 'POST_CLEANUP_MEMORY','cleanup_tail_at_utc':cleanup_tail_at_utc,'scheduled_delay_seconds':scheduled_delay_seconds,'observed_delay_ms':observed_delay_ms,'event_time_utc':now,'sample_started_at_utc':sample_started_at_utc,'sample_completed_at_utc':sample_completed_at_utc,'watch_active_before':watch_active_before,'watch_active_after':watch_active_after,'watch_activity_revision_before':watch_activity_revision_before,'watch_activity_revision_after':watch_activity_revision_after,'activity_scope':activity,'activity_count_scope':ACTIVITY_COUNT_SCOPE,'runtime_git_commit':runtime_git_commit,'source_code_sha256':source_code_sha256,'memory':envelope,'capture_flags':sorted(flags),'scope':SCOPE}
        return submit(event)
    except BaseException as error: return _failure(_error_code(error),'INVALID')


def record_marker(*,process_session_id,origin_cycle_id,cleanup_tail_at_utc,runtime_git_commit,source_code_sha256):
    return _record(process_session_id=process_session_id,origin_cycle_id=origin_cycle_id,cleanup_tail_at_utc=cleanup_tail_at_utc,runtime_git_commit=runtime_git_commit,source_code_sha256=source_code_sha256,observation_index=1,scheduled_delay_seconds=0)


def record_sample(**kwargs):
    try: return _record(**kwargs)
    except BaseException as error: return _failure(_error_code(error),'INVALID')


def status():
    try:
        with _LOCK: metrics=json.loads(json.dumps(_METRICS))
        return {'version':VERSION,'table':TABLE,'scope':SCOPE,'activity_count_scope':ACTIVITY_COUNT_SCOPE,'queue_is_durable':False,'worker_alive':bool(_WORKER and _WORKER.is_alive()),'configured':bool(_STORE is not None or os.getenv('RESEARCH_DATABASE_URL') or os.getenv('DATABASE_URL')),'shared_watch_journal_queue_used':False,'persistence_retry_policy':'No automatic retry; failed, unavailable and dropped observations remain explicit gaps.','session_semantics':'Original Watch journal session supplied explicitly by bridge; no new session or ContextVar lookup.','activity_semantics':'Before/after diagnostic Watch observation handles and every-begin revision witnesses only; handles close at finally entry before native cleanup. Never proof of native completion or global process idleness.','clock_semantics':'UTC fields are process/database annotations; observed_delay_ms is monotonic elapsed time supplied by the monitor. No earliest commit or coroutine-return claim.','limits':{'event_bytes':MAX_EVENT_BYTES,'memory_sample_bytes':MAX_MEMORY_BYTES,'queue_items':MAX_QUEUE_ITEMS,'queue_canonical_utf8_bytes':MAX_QUEUE_BYTES,'observations_per_origin_cycle':5,'scheduled_delays_seconds':list(DELAYS.values()),'connect_timeout_seconds':CONNECT_TIMEOUT_SECONDS,'statement_timeout_ms':STATEMENT_TIMEOUT_MS,'lock_timeout_ms':LOCK_TIMEOUT_MS},**metrics}
    except BaseException: return {'version':VERSION,'status':'UNAVAILABLE','reason_code':'STATUS_UNAVAILABLE'}
