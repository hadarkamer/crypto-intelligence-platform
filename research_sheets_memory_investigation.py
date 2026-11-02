"""One first natural Sheets pass, numeric observations only; absolute expiry."""
from datetime import datetime,timezone
import functools,hashlib,json,os,threading,time,uuid
from pathlib import Path
import research_process_memory
import research_sheets_memory

VERSION='sheets-memory-investigation-v1'
EXPIRES_AT_UTC='2026-11-02T16:00:00+00:00'
ARM_SECONDS=90
MAX_DURATION_SECONDS=900
MAX_POINTS=20
MAX_RECORD_BYTES=65536
LABELS={'BEGIN','CANDIDATES','GROUP_READY','STAGE_RECORDS','RECONCILE_DONE','DRAIN_BATCH','REQUEST_READY','ACK_READ','DRAIN_DONE','AUDIT_BEGIN','AUDIT_PAGE','CONSUME_DONE','AUDIT_DONE','RETURN','POST_AWAIT_0','POST_AWAIT_1','POST_AWAIT_3'}
_OWNER=None
def utc_now():return datetime.now(timezone.utc).isoformat()
def remaining_seconds(now=None):return max(0.,min(MAX_DURATION_SECONDS,(datetime.fromisoformat(EXPIRES_AT_UTC)-(now or datetime.now(timezone.utc))).total_seconds()))
def _copy(value,cap=MAX_RECORD_BYTES):
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
    if len(raw.encode())>cap:raise ValueError('NUMERIC_RECORD_BOUND')
    return json.loads(raw)
def _emit(record):
    try:print('[SHEETS_MEMORY_V1] '+json.dumps(record,sort_keys=True,separators=(',',':')),flush=True)
    except Exception:pass
def _live_roots(worker):
    import google_sheets_sync
    audit=worker._sheet_reconciler
    return dict(audit_expected=audit.expected,audit_seen=audit.seen,audit_unexpected=audit.unexpected,
        audit_last_complete=audit.runtime.get('last_complete'),snapshot_cache=google_sheets_sync._SNAPSHOT_CACHE,
        worker_result=worker._runtime.get('last_result'))
def _admission():
    try:
        def read(name):
            with open('/sys/fs/cgroup/'+name,'rb')as stream:raw=stream.read(65)
            if len(raw)>64 or not raw.strip().isdigit():raise ValueError('CGROUP_INTEGER')
            return int(raw)
        current,maximum=read('memory.current'),read('memory.max')
        return dict(status='COMPLETE',current_bytes=current,max_bytes=maximum,admitted=0<=current<=maximum and maximum-current>=128*1024*1024)
    except Exception:return dict(status='UNAVAILABLE',admitted=False)
class Investigation:
    def __init__(self,worker,*,duration=900,clock=time.monotonic,utc=utc_now,process=None,measure=None,roots=None,spawn=None,admission=None):
        self.worker=worker;self.clock=clock;self.utc=utc;self.process=process or (lambda:research_process_memory.sample(include_mallinfo2=False));self.measure=measure or research_sheets_memory.measure_roots
        self.roots=roots or _live_roots;self.spawn=spawn or self._spawn;self.admission=admission or _admission;self.start=clock();self.deadline=self.start+max(0.,min(duration,MAX_DURATION_SECONDS));self.arm=self.start+ARM_SECONDS
        self.lock=threading.Lock();self.local=threading.local();self.session=uuid.uuid4().hex;self.runtime=os.getenv('RENDER_GIT_COMMIT','UNKNOWN')
        self.state='WAITING_FIRST_NATURAL_PASS';self.records=[];self.labels=set();self.errors=0;self.total_calls=0;self.selected_call=None;self.finished_at=None;self.outer_at=None;self.exception_type=None;self.post_done=set();self.selected_thread_id=None;self.wrapper=None;self.original=None;self.source_pins={}
    def _spawn(self,target):threading.Thread(target=target,name='sheets-memory-tail',daemon=True).start()
    def attach(self):
        original=self.worker.run_once;self.original=original
        @functools.wraps(original)
        def wrapped(*args,**kwargs):
            try:
                with self.lock:
                    self.total_calls+=1;call=self.total_calls;now=self.clock()
                    selected=self.selected_call is None and self.arm<=now<self.deadline
                    if selected:self.selected_call=call;self.state='MEASURING';self.selected_thread_id=threading.get_ident()
                    elif self.selected_call is None and now>=self.deadline:self.state='EXPIRED_NO_SELECTED_PASS'
            except Exception:return original(*args,**kwargs)
            if not selected:
                if self.state=='EXPIRED_NO_SELECTED_PASS':self.restore()
                return original(*args,**kwargs)
            self.local.selected=True
            self.safe_point('BEGIN',persistent=True)
            try:return original(*args,**kwargs)
            except BaseException as exc:
                self.exception_type=type(exc).__name__;raise
            finally:
                try:
                    self.local.selected=False;self.finished_at=self.clock();self.safe_point('RETURN',persistent=True)
                    with self.lock:self.state='AWAITING_OUTER_RETURN'if self.exception_type is None else'COMPLETED_EXCEPTION_NO_OUTER_TAILS'
                    if self.exception_type is not None:self.restore()
                except Exception:self.errors+=1
        self.wrapper=wrapped;self.worker.run_once=wrapped
    def active(self):return bool(getattr(self.local,'selected',False)and self.clock()<self.deadline)
    def safe_point(self,label,roots=None,*,persistent=False):
        try:self._point(label,roots,persistent=persistent)
        except Exception:
            with self.lock:self.errors+=1
            if label in LABELS:
                failed=dict(version=VERSION,session=self.session,runtime_commit=self.runtime,label=label,status='DIAGNOSTIC_FAILED_NO_RETRY',observer_perturbed=True)
                with self.lock:self.records.append(failed)
                _emit(failed)
    def _point(self,label,roots=None,*,persistent=False):
        if label not in LABELS:raise ValueError('UNDECLARED_LABEL')
        with self.lock:
            if label in self.labels or len(self.records)>=MAX_POINTS:return
            self.labels.add(label)
        at=self.utc();began=self.clock()
        record=dict(version=VERSION,session=self.session,runtime_commit=self.runtime,label=label,at_utc=at,selected_call=self.selected_call,total_calls=self.total_calls,selected_thread=threading.get_ident()==self.selected_thread_id,persistent_roots=persistent,observer_perturbed=True,scope='SEQUENTIAL_PROCESS_GAUGE_AND_SELECTED_LOGICAL_ROOTS_NOT_PEAK_RSS_EXCLUSIVE_OWNERSHIP_OR_CAUSAL_MEMORY')
        if self.clock()>=self.deadline:record.update(status='MISSED_DEADLINE')
        else:
            record['process_before_graph']=self.process();rss=record['process_before_graph'].get('rss_bytes')
            if not persistent and not roots:
                record.update(status='GAUGE_ONLY',graph=None)
                record['diagnostic_elapsed_ms']=round((self.clock()-began)*1000,3)
                bounded=_copy(record)
                with self.lock:self.records.append(bounded)
                _emit(bounded)
                return
            record['admission']=self.admission()
            if type(rss)is not int or not 0<=rss<=1024*1024*1024 or record['admission'].get('admitted')is not True:
                record.update(status='GRAPH_NOT_ADMITTED_RESOURCE',graph=None)
            else:
                live=self.roots(self.worker)if persistent else(roots or {})
                try:record['graph']=self.measure(live)
                finally:del live
                record.update(status='MEASURED')
        record['diagnostic_elapsed_ms']=round((self.clock()-began)*1000,3)
        bounded=_copy(record)
        with self.lock:self.records.append(bounded)
        _emit(bounded)
    def outer_return(self,worker):
        if worker is not self.worker or self.state!='AWAITING_OUTER_RETURN':return
        self.outer_at=self.clock();self.safe_point('POST_AWAIT_0',persistent=True)
        with self.lock:self.state='OBSERVING_POST_RETURN'
        self.spawn(self._tails)
    def tail_tick(self):
        if self.state!='OBSERVING_POST_RETURN':return
        for offset in (1,3):
            if offset in self.post_done or self.clock()<self.outer_at+offset:continue
            self.post_done.add(offset)
            if self.clock()>self.outer_at+offset+.75:
                self.labels.add('POST_AWAIT_'+str(offset))
                with self.lock:self.records.append(dict(version=VERSION,session=self.session,runtime_commit=self.runtime,label='POST_AWAIT_'+str(offset),status='MISSED_TIMER_WINDOW',observer_perturbed=True,at_utc=self.utc(),selected_call=self.selected_call,total_calls=self.total_calls))
                _emit(self.records[-1])
            else:self.safe_point('POST_AWAIT_'+str(offset),persistent=True)
        if self.post_done=={1,3}:
            with self.lock:self.state='COMPLETED'
            self.restore()
    def _tails(self):
        while self.state=='OBSERVING_POST_RETURN':
            self.tail_tick()
            if self.state=='OBSERVING_POST_RETURN':threading.Event().wait(.05)
    def restore(self):
        if self.worker.run_once is self.wrapper:self.worker.run_once=self.original
    def status(self):
        with self.lock:
            return _copy(dict(version=VERSION,state=self.state,session=self.session,runtime_commit=self.runtime,source_code_sha256=self.source_pins,expires_at_utc=EXPIRES_AT_UTC,selection_rule='FIRST_NATURAL_RUN_ONCE_AT_LEAST90S_AFTER_INSTALL_NO_RESULT_OR_BYTE_FILTER_NO_REPLACEMENT',arm_seconds=ARM_SECONDS,selected_call=self.selected_call,total_calls=self.total_calls,selected_exception_type=self.exception_type,errors=self.errors,records=self.records,unattempted_labels=sorted(LABELS-self.labels),missing_measurements=sorted(LABELS-{r['label']for r in self.records if r['status']in('MEASURED','GAUGE_ONLY')}),limits=dict(declared_labels=len(LABELS),points=MAX_POINTS,record_bytes=MAX_RECORD_BYTES,duration_seconds=MAX_DURATION_SECONDS,graph_nodes=120000,graph_workspace_bytes=24*1024*1024,graph_seconds=.5),scope='CACHED_READ_ONLY_NO_VALUES_ADDRESSES_FRAMES_FORCED_SCAN_GC_TRIM_OR_CACHE_CLEAR',all_gauges_observer_perturbed=True),2*1024*1024)
def install():
    global _OWNER
    if _OWNER is not None:return _OWNER.status()
    duration=remaining_seconds()
    if duration<=0:return dict(state='EXPIRED_NO_PROBES',expires_at_utc=EXPIRES_AT_UTC)
    import research_snapshot_sync_worker
    try:
        _OWNER=Investigation(research_snapshot_sync_worker.WORKER,duration=duration)
        names=('main.py','research_snapshot_sync_worker.py','research_sheet_outbox.py','research_sheet_reconciliation.py','google_sheets_sync.py','research_sheets_memory.py','research_sheets_memory_investigation.py','research_process_memory.py')
        _OWNER.source_pins={name:hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()for name in names}
        _OWNER.attach();return _OWNER.status()
    except Exception:return dict(version=VERSION,state='INSTALL_UNAVAILABLE_NO_PROBES')
def active():
    try:return _OWNER is not None and _OWNER.active()
    except Exception:return False
def point(label,roots=None):
    if active():_OWNER.safe_point(label,roots)
def after_await(worker):
    try:
        if _OWNER is not None:_OWNER.outer_return(worker)
    except Exception:
        if _OWNER is not None:_OWNER.errors+=1
def status():return _OWNER.status()if _OWNER is not None else dict(version=VERSION,state='NOT_INSTALLED',expires_at_utc=EXPIRES_AT_UTC)
