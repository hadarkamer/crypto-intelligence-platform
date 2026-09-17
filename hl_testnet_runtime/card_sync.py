"""Opt-in continuous Testnet card observations. No execution or app delivery.

Only previously registered, durably bound exchange orders are monitored. New
record-only alerts cannot be mistaken for positions. One completed pass then
30 seconds of rest; per-bucket database claims prevent deployment overlap.
"""
from collections import Counter
from datetime import datetime, timezone
import json
import re
import threading
import time
from . import card_lifecycle as life, checks
from .card_sync_evidence import collect, now_ms, PublicReader, SyncError
from .card_sync_store import SyncStore
from .postgres_journal import PostgresJournal, JournalError
from .approved_account_assignment import public_route_env
from .trade_cards import account_routes

MODE='registered_readonly_v1'
SERVICE='srv-dakptbh594qs7395460g'
INTERVAL=30


def config(env):
    if (env.get('HL_TESTNET_CARD_SYNC')!=MODE or env.get('RENDER_SERVICE_ID')!=SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') not in ('read_only','cancel_monitor_testnet_v1')
            or env.get('HL_TESTNET_JOURNAL_BACKEND')!='staging_postgres_v1'
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION')=='approved_single_attempt_v1'):
        raise SyncError('READ_ONLY_SYNC_NOT_ENABLED')
    return account_routes(public_route_env(env))


class BudgetReader(PublicReader):
    """Reserve worst-case weight for fill pages before making the request.

    At most 400 info-weight units per pass, followed by at least 30 seconds rest.
    This is a worker safety budget, not a maximum number of trades or cards.
    """
    def __init__(self):
        super().__init__(); self.weight=0

    def read(self,kind,*args,**kwargs):
        cost=120 if kind=='userFillsByTime' else 2 if kind in ('orderStatus','clearinghouseState') else 20
        if self.weight+cost>400: raise SyncError('READ_RATE_BUDGET_DEFERRED')
        self.weight+=cost
        return super().read(kind,*args,**kwargs)


def error_code(exc):
    text=str(exc)
    return text if isinstance(exc,(life.LifecycleError,JournalError,checks.Blocked)) and re.fullmatch(r'[A-Z_]{1,100}',text) else 'SYNC_READ_FAILED'


def tick(store,routes,*,reader_factory=BudgetReader,clock=now_ms,stop_event=None):
    report=dict(status='WAITING_FOR_REGISTERED_EXECUTIONS',environment='testnet',
        continuous_sync_enabled=True,dispatch_enabled=False,order_requests_sent=0,
        app_delivery_sent=False,public_reads=0,changed=False,card_states={},closure_verified_count=0)
    if stop_event is not None and stop_event.is_set(): return {**report,'status':'STOPPED'}
    claim=store.claim()
    if claim:
        reader=reader_factory()
        try:
            for binding in claim['evidence']['bindings']:
                route=routes.get(binding['role'],{})
                if route.get('account')!=life.address(binding['account']):
                    raise SyncError('BOUND_ACCOUNT_NOT_IN_APPROVED_ROUTES')
            observation=collect(claim['evidence'],reader,cursor_ms=claim['cursor_ms'],clock=clock)
            if stop_event is not None and stop_event.is_set(): raise SyncError('WORKER_STOPPED_RECHECK_REQUIRED')
            saved=store.save(claim,observation,now_ms=clock())
            cards=saved['report']['cards']
            report.update(status='SYNC_PASS_COMPLETED' if saved['status']=='VERIFIED' else 'SYNC_REQUIRES_REVIEW',
                changed=saved['changed'],card_states=dict(Counter(c['state'] for c in cards)),
                closure_verified_count=sum(c['closure_verified'] for c in cards),
                bucket_issues=saved['report']['bucket_issues'],
                card_issue_codes=sorted({issue for c in cards for issue in c['issues']}))
        except Exception as exc:
            code=error_code(exc)
            store.fail(claim,code)
            report.update(status='SYNC_REQUIRES_REVIEW',reason=code)
        finally:
            report['public_reads']=reader.calls
    report.update(store.summary())
    if not claim and report['registered_buckets']:
        report['status']='NO_BUCKET_DUE'
    report['checked_at_utc']=datetime.now(timezone.utc).isoformat()
    return report


_lock=threading.Lock()
_thread=None
_stop=threading.Event()
_report={'status':'NOT_STARTED'}
_last=None


def _emit(report):
    global _report,_last
    with _lock:
        _report=dict(report); _last=time.monotonic()
    print(json.dumps({'testnet_card_sync':report},sort_keys=True),flush=True)


def health():
    with _lock:
        alive=_thread is not None and _thread.is_alive()
        recent=_last is not None and time.monotonic()-_last<120
        registered=_report.get('registered_buckets',0)
        verified=_report.get('fresh_verified_buckets',0)
        return dict(running=alive,recent_report=recent,read_only=True,dispatch_enabled=False,
            status=_report.get('status','NOT_STARTED'),registered_buckets=registered,
            fresh_verified_buckets=verified,problem_buckets=_report.get('problem_buckets',0),
            fully_current=alive and recent and registered>0 and registered==verified)


def run_loop(store,routes,stop_event,emit=_emit,*,runner=tick):
    failures=0; cycles=0
    while not stop_event.is_set():
        try:
            report=runner(store,routes,stop_event=stop_event)
            failures=0
        except Exception as exc:
            failures+=1
            report=dict(status='SYNC_STORAGE_OR_INITIALIZATION_REVIEW',reason=error_code(exc),
                dispatch_enabled=False,order_requests_sent=0,fully_current=False)
        cycles+=1
        report['cycles']=cycles
        report['next_pass_delay_seconds']=min(300,INTERVAL*2**min(failures,3))
        emit(report)
        stop_event.wait(report['next_pass_delay_seconds'])


def _worker(env):
    try:
        routes=config(env)
        store=SyncStore(PostgresJournal.from_env(env))
        store.initialize()  # No DDL inside the repeating pass.
        run_loop(store,routes,_stop)
    except Exception as exc:
        _emit(dict(status='SYNC_INITIALIZATION_FAILED',reason=error_code(exc),
            dispatch_enabled=False,order_requests_sent=0,fully_current=False))


def start():
    global _thread
    import os
    config(os.environ)
    with _lock:
        if _thread is not None and _thread.is_alive(): return False
        _stop.clear()
        _thread=threading.Thread(target=_worker,args=(os.environ,),daemon=True,name='testnet-card-readonly-sync')
        _thread.start()
    return True


def stop():
    _stop.set()
