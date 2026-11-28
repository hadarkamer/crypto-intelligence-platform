"""Start only the isolated numerical executable for worksheet-tagged jobs."""
import asyncio
from contextlib import suppress
import json
import os
import re
import sys
import tempfile
from pathlib import Path
import signal
from numeric_inputs import scoped_dispatch as dispatch
from time import monotonic
from uuid import UUID

ROOT = Path(__file__).resolve().parent/'numeric'
BASE = '81505afa9a235edaeac95ede7beca3850beaf1c6'
WORK_LOCK = 748230102
ENV = ('PATH','HOME','TMPDIR','LANG','LANGUAGE','LC_ALL','LC_CTYPE','TZ','LD_LIBRARY_PATH',
       'PLAYWRIGHT_BROWSERS_PATH','XDG_CACHE_HOME','XDG_RUNTIME_DIR','FONTCONFIG_PATH',
       'SSL_CERT_FILE','SSL_CERT_DIR','REQUESTS_CA_BUNDLE','NODE_EXTRA_CA_CERTS',
       'DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL','COINGLASS_LOGIN_PASSWORD')
_wakeup = None
_checks = {}
_status_lock = None


def available():
    try:
        return (all(os.environ.get(k) for k in ('DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL'))
                and (ROOT/'.ready').read_text().strip()==BASE
                and os.access(ROOT/'venv/bin/python',os.X_OK)
                and (ROOT/'runtime/scoped_entry.py').is_file()
                and os.access(ROOT/'runtime/ocr_runtime/tesseract',os.X_OK))
    except OSError:
        return False


def child_env():
    env = {key:os.environ[key] for key in ENV if key in os.environ}
    env.update(COINGLASS_MODERN_UI_ENABLED='true',COINGLASS_SESSION_STORE_ENABLED='true',
               COINGLASS_VERIFIED_SYMBOLS='BTC,ETH',COINGLASS_CYCLE_FIXED_SYMBOLS='BTC,ETH',
               COINGLASS_CYCLE_TABS='2',PYTHONNOUSERSITE='1',PYTHONDONTWRITEBYTECODE='1')
    return env


async def launch(*args, capture=False):
    control = tempfile.TemporaryFile() if not capture else None
    try:
        env=child_env()
        fds=()
        if control:
            env['COINGLASS_FIXED_REPORT_FD']=str(control.fileno())
            fds=(control.fileno(),)
        process = await asyncio.create_subprocess_exec(str(ROOT/'venv/bin/python'),'-B',
            str(ROOT/'runtime/scoped_entry.py'),*args,cwd=ROOT/'runtime',env=env,pass_fds=fds,
            stdin=asyncio.subprocess.DEVNULL,stdout=asyncio.subprocess.PIPE if capture else None,
            stderr=asyncio.subprocess.DEVNULL,start_new_session=True)
    except BaseException:
        if control:
            control.close()
        raise
    try:
        if capture:
            output,_ = await asyncio.wait_for(process.communicate(),12)
            if process.returncode or len(output)>256:
                raise RuntimeError('collector_unavailable')
            return json.loads(output)
        code = await process.wait()
        control.seek(0)
        raw=control.read(257)
        try:
            ack=json.loads(raw) if len(raw)<=256 else {}
        except (ValueError,UnicodeError):
            ack={}
        known={'completed','no_work','owner_busy','cancelled','worker_error','database_error',
               'source_login_required','source_rate_limited','interrupted'}
        reason=ack.get('reason') if isinstance(ack,dict) else None
        result={'reason':'signal_exit' if code<0 else reason if reason in known else 'exit_without_receipt',
                'returncode':code}
        phases={'worker_started','ownership_checked','owner_acquired','orphan_review_complete','job_claimed',
                'partial_receipt_committed','source_receipt_committed','source_receipt_verified','terminal_saved'}
        completed=ack.get('last_completed_stage') if isinstance(ack,dict) else None
        result['last_completed_stage']=completed if completed in phases else 'dispatch_reserved'
        if isinstance(ack,dict) and isinstance(ack.get('last_job_id'),str):
            try:
                result['last_job_id']=str(UUID(ack['last_job_id']))
            except ValueError:
                pass
        if result['reason']=='database_error' and isinstance(ack.get('sqlstate'),str) and re.fullmatch(r'[A-Z0-9]{5}',ack['sqlstate']):
            result['sqlstate']=ack['sqlstate']
        return result
    finally:
        if control:
            control.close()
        if process.returncode is None:
            # The numerical parent handles SIGTERM and closes its independently owned browser tree.
            with suppress(ProcessLookupError):
                process.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(),20)
            except asyncio.TimeoutError:
                from signal import SIGKILL, pidfd_send_signal
                # Browser and OCR may own their own process groups. Pin each descendant PID
                # before killing, just as the approved numerical reader does on an abrupt stop.
                pending,handles,seen=[process.pid],[],set()
                try:
                    while pending:
                        pid=pending.pop()
                        if pid in seen: continue
                        seen.add(pid)
                        try:
                            handles.append(os.pidfd_open(pid))
                            for path in Path(f'/proc/{pid}/task').glob('*/children'):
                                pending.extend(int(v) for v in path.read_text().split())
                        except (FileNotFoundError,ProcessLookupError):
                            pass
                    for handle in handles:
                        with suppress(ProcessLookupError):
                            pidfd_send_signal(handle,SIGKILL)
                    await process.wait()
                finally:
                    for handle in handles:os.close(handle)


async def account_status(fixed_cycle_id=None):
    global _status_lock
    if not available():
        return {'code':'collector_unavailable'}
    try:
        group = str(UUID(str(fixed_cycle_id))) if fixed_cycle_id else None
    except ValueError:
        return {'code':'collector_unavailable'}
    if _status_lock is None:
        _status_lock = asyncio.Lock()
    async with _status_lock:
        now = monotonic()
        for key in list(_checks):
            if _checks[key][0] <= now:
                del _checks[key]
        if group in _checks:
            return dict(_checks[group][1])
        try:
            result = await launch('status',*([group] if group else []),capture=True)
            if not isinstance(result,dict) or result.get('code') not in (
                    'ready','source_rate_limited','source_login_required','map_in_progress'):
                raise ValueError('invalid_code')
        except (OSError,ValueError,RuntimeError,asyncio.TimeoutError):
            result = {'code':'collector_unavailable'}
        _checks[group] = monotonic()+1,result
        return dict(result)


def pending(store):
    with store.connect() as connection:
        return bool(connection.execute(dispatch.PICK,(WORK_LOCK,)).fetchone())


async def dispatch_one(store):
    reservation=await asyncio.to_thread(dispatch.reserve,store)
    if not reservation:
        return
    _,cycle,token=reservation
    try:
        receipt=await launch('run',cycle,token)
    except OSError:
        receipt={'reason':'spawn_failed','last_completed_stage':'dispatch_reserved'}
    await asyncio.to_thread(dispatch.settle,store,reservation,receipt)


def wake():
    if _wakeup is not None:
        _wakeup.set()


def safe_db_failure(error):
    state=getattr(error,'sqlstate',None)
    if isinstance(state,str) and re.fullmatch(r'[A-Z0-9]{5}',state):
        return {'reason':'database_error','sqlstate':state}
    return {'reason':'dispatch_unavailable'}


async def run(store):
    global _wakeup
    last_failure=None
    _wakeup = asyncio.Event()
    try:
        while True:
            _wakeup.clear()
            try:
                if available():
                    await dispatch_one(store)
                last_failure=None
            except Exception as error:
                failure=safe_db_failure(error)
                if failure!=last_failure:
                    print('COINGLASS_FIXED_DISPATCH '+json.dumps(dict(state='check_failed',**failure)),flush=True)
                last_failure=failure
            try:
                await asyncio.wait_for(_wakeup.wait(),10)
            except asyncio.TimeoutError:
                pass
    finally:
        _wakeup = None


def operator(args):
    usage = ("Usage: python standalone_collector/scoped_worker.py {status|resume} CYCLE_UUID\n"
             "Inspect status and correct the reported cause before resuming. Resume releases only a stopped "
             "cycle, keeps its saved receipts, and leaves provider and account holds in force.")
    if args in (['--help'],['-h']):
        print(usage)
        return
    if len(args)!=2 or args[0] not in ('resume','status'):
        raise SystemExit(usage)
    from psycopg import connect
    from psycopg.rows import dict_row
    group=str(UUID(args[1]))
    # The operator must also be able to repair a hold when the generated worker or receipt table is unavailable.
    with connect(os.environ['DATABASE_URL'],row_factory=dict_row,connect_timeout=5,
                 options='-c statement_timeout=8000 -c lock_timeout=3000') as connection:
        if args[0]=='resume':
            result={'cycle_id':group,'resumed':dispatch.resume(connection,group)}
        else:
            row=connection.execute("""SELECT id,fixed_worker_guard AS guard,
                EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs q WHERE q.fixed_cycle_id=a.fixed_cycle_id
                    AND q.status IN ('queued','running')) AS active
                FROM public.ai_collection_bridge_jobs a WHERE a.fixed_cycle_id=%s ORDER BY a.created_at,a.id LIMIT 1""",
                (group,)).fetchone()
            guard=row['guard'] if row and row['guard'] else {}
            fallback=('eligible' if row['active'] else 'finished') if row else 'missing'
            result={'cycle_id':group,'anchor_job_id':str(row['id']) if row else None,'found':bool(row),'state':guard.get('state',fallback),
                    'attempts':guard.get('attempts',0),'last':guard.get('last')}
    print(json.dumps(result,separators=(',',':')))


if __name__=='__main__':
    try:
        operator(sys.argv[1:])
    except Exception as error:
        print(json.dumps(safe_db_failure(error),separators=(',',':')),file=sys.stderr)
        sys.exit(1)
