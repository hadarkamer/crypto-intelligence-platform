"""One bounded acceptance on the paid service; no deployment, AI or data writes."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
CG = Path('/sys/fs/cgroup')
LOCK = 748230102  # Existing single-worker lock, shared across all three models.
LIMIT = 1700 * 1024 * 1024  # Headroom within the user-upgraded 2 GiB service.
CASES = ((3, '48H'),)
OS_KEYS = ('PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL', 'TZ',
           'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE', 'PLAYWRIGHT_BROWSERS_PATH')

def stat(name):
    return {k: int(v) for k, v in (x.split() for x in (CG/name).read_text().splitlines())}

def sample():
    values = stat('memory.stat')
    values = {k: values.get(k, 0) for k in ('anon', 'file', 'kernel', 'sock')}
    values['current'] = int((CG/'memory.current').read_text())
    return values

def stop(process):
    try: os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError: pass
    try: process.wait(timeout=3)
    except subprocess.TimeoutExpired: pass
    # Our own process group only, including any surviving browser descendants.
    try: os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError: pass
    process.wait(timeout=3)

def emit(value):
    print(json.dumps(value, separators=(',', ':')), flush=True)

def pending(conn):
    return bool(conn.execute("SELECT 1 FROM public.ai_collection_bridge_jobs WHERE status IN ('queued','running') LIMIT 1").fetchone())

def main():
    import psycopg
    # Verify the running instance, not merely a pending dashboard configuration.
    if int((CG/'memory.max').read_text()) != 2 * 1024**3:
        raise RuntimeError('unexpected_runtime_memory_limit')
    quota, period = map(int, (CG/'cpu.max').read_text().split())
    if quota != period:
        raise RuntimeError('unexpected_runtime_cpu_limit')
    env = {k: os.environ[k] for k in OS_KEYS if k in os.environ}
    env.update({k: os.environ[k] for k in ('COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON') if k in os.environ})
    if not any(env.get(k) for k in ('COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON')):
        raise RuntimeError('source_session_missing')
    env.update(OPENAI_API_KEY='', DATABASE_URL='', COINGLASS_COLLECTOR_TOKEN='',
               COLLECTION_BRIDGE_ENABLED='0', MODEL1_NETWORK_DIAGNOSTICS='0', PYTHONUNBUFFERED='1')
    out = ROOT/'output'
    out.mkdir(mode=0o700, exist_ok=False)  # Refuse a second invocation.
    results = []
    with psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5, autocommit=True,
            options='-c statement_timeout=1500 -c default_transaction_read_only=on') as conn:
        locked = conn.execute('SELECT pg_try_advisory_lock(%s)', (LOCK,)).fetchone()[0]
        if not locked or pending(conn):
            emit({'kind':'PAID_RUNTIME_STOP', 'reason':'production_work_present', 'source_started':False})
            return 2
        try:
            emit({'kind':'PAID_RUNTIME_START','source_commit':'b62bce2ed6aa32282efcd9084cee1c7685897ab1',
                'cpu.max':(CG/'cpu.max').read_text().strip(),'memory.max':(CG/'memory.max').read_text().strip(),
                'memory_guard_mib':1700,'baseline_mib':{k:round(v/1024**2,2) for k,v in sample().items()},
                'ai_calls':0,'data_writes':0,'max_cases':len(CASES),'scope':'active_service_cgroup_including_existing_server'})
            for model, tf in CASES:
                if pending(conn):
                    emit({'kind':'PAID_RUNTIME_STOP','reason':'production_work_arrived','source_started':False})
                    break
                initial = sample()
                if initial['anon']+initial['kernel']+initial['sock'] >= LIMIT:
                    emit({'kind':'PAID_RUNTIME_STOP','reason':'insufficient_initial_headroom','source_started':False})
                    break
                dest = out/f'model{model}-{tf}'
                case_env = dict(env, COLLECTION_HEATMAP_MODEL=str(model))
                log = out/f'model{model}-{tf}.log'
                peak = initial.copy(); cpu_before = stat('cpu.stat'); events_before = stat('memory.events')
                reason = None; started = time.monotonic(); next_queue_check = started + 1
                # Reuse existing safe phase diagnostics for a bounded/guarded stop.
                child_code = (
                    'import sys,time; from pathlib import Path; '
                    'sys.path.insert(0,str(Path("runtime").resolve())); '
                    'import model1_execution as d; '
                    f'd._progress_path=Path({str(dest)!r})/"execution_progress.json"; '
                    'd._progress_started=time.monotonic(); '
                    f'd._progress_identity={{"heatmap_model":{model},"timeframe":{tf!r}}}; '
                    'from model3_cookie_diagnostic import install; install(); '
                    'from hover_source_check import single; '
                    f'single({model},{tf!r},Path({str(dest)!r}))'
                )
                with log.open('xb') as stream:
                    process = subprocess.Popen([sys.executable, '-c', child_code],env=case_env,
                        cwd=ROOT,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
                    try:
                        while process.poll() is None:
                            values = sample()
                            peak = {k:max(peak[k],v) for k,v in values.items()}
                            if values['anon']+values['kernel']+values['sock'] >= LIMIT:
                                reason='memory_headroom_guard'; break
                            if time.monotonic()-started >= 180:
                                reason='bounded_runtime_timeout'; break
                            if time.monotonic() >= next_queue_check:
                                if pending(conn): reason='yield_to_production_work'; break
                                next_queue_check=time.monotonic()+1
                            time.sleep(.1)
                    finally:
                        stop(process)
                result = {'model':model,'timeframe':tf,'status':'failed'}
                if (dest/'report.json').is_file():
                    result.update(json.loads((dest/'report.json').read_text()))
                progress = dest/'execution_progress.json'
                if progress.is_file():
                    sys.path.insert(0, str(ROOT/'runtime'))
                    from model1_execution import safe_progress
                    result['execution_progress'] = safe_progress(json.loads(progress.read_text()))
                result.update(kind='PAID_RUNTIME_RESULT',exit_code=process.returncode,
                    guard_reason=reason,total_seconds=round(time.monotonic()-started,3),
                    peak_mib={k:round(v/1024**2,2) for k,v in peak.items()},
                    cpu_delta={k:v-cpu_before.get(k,v) for k,v in stat('cpu.stat').items()},
                    memory_events_delta={k:v-events_before.get(k,v) for k,v in stat('memory.events').items()},
                    ai_calls=0,data_writes=0)
                results.append(result); emit(result)
                (ROOT/'summary.json').write_text(json.dumps(results,indent=2))
                if reason or result.get('status')!='captured': break
        finally:
            conn.execute('SELECT pg_advisory_unlock(%s)', (LOCK,))
    emit({'kind':'PAID_RUNTIME_DONE','cases_completed':len(results),'worker_lock_released':True,
          'ai_calls':0,'data_writes':0})
    return int(not results or any(r['status']!='captured' for r in results))

if __name__=='__main__':
    try: sys.exit(main())
    except Exception as exc:
        emit({'kind':'PAID_RUNTIME_ERROR','error_class':type(exc).__name__})
        sys.exit(1)
