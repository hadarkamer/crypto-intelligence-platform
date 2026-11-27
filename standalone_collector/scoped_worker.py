"""Start only the isolated numerical executable for worksheet-tagged jobs."""
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import signal
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
    process = await asyncio.create_subprocess_exec(str(ROOT/'venv/bin/python'),'-B',
        str(ROOT/'runtime/scoped_entry.py'),*args,cwd=ROOT/'runtime',env=child_env(),
        stdin=asyncio.subprocess.DEVNULL,stdout=asyncio.subprocess.PIPE if capture else None,
        stderr=asyncio.subprocess.DEVNULL,start_new_session=True)
    try:
        if capture:
            output,_ = await asyncio.wait_for(process.communicate(),12)
            if process.returncode or len(output)>256:
                raise RuntimeError('collector_unavailable')
            return json.loads(output)
        await process.wait()
    finally:
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
        row = connection.execute("""
            SELECT EXISTS(SELECT 1 FROM public.ai_collection_bridge_jobs q
                WHERE q.fixed_cycle_id IS NOT NULL AND q.status IN ('queued','running')
                AND (q.status='running' OR q.symbol='BTC' AND q.heatmap_model=1 AND q.timeframe='12H'
                    OR q.created_at<clock_timestamp()-interval '350 seconds'
                    OR EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs h
                        WHERE h.fixed_cycle_id=q.fixed_cycle_id AND h.symbol='BTC' AND h.heatmap_model=1
                        AND h.timeframe='12H' AND h.status IN ('ready','failed')))
                AND NOT EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs legacy
                    WHERE legacy.fixed_cycle_id IS NULL AND legacy.status='queued'
                    AND (legacy.created_at,legacy.id)<(q.created_at,q.id)))
            AND NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
                AND mode='ExclusiveLock' AND classid=0 AND objid=%s AND objsubid=1
                AND database=(SELECT oid FROM pg_database WHERE datname=current_database())) AS pending""",
            (WORK_LOCK,)).fetchone()
        return bool(row and row['pending'])


def wake():
    if _wakeup is not None:
        _wakeup.set()


async def run(store):
    global _wakeup
    _wakeup = asyncio.Event()
    try:
        while True:
            _wakeup.clear()
            try:
                if available() and await asyncio.to_thread(pending,store):
                    await launch('run')
            except (OSError,RuntimeError):
                pass
            except Exception:
                print('COINGLASS_FIXED pending work will be checked again',flush=True)
            try:
                await asyncio.wait_for(_wakeup.wait(),10)
            except asyncio.TimeoutError:
                pass
    finally:
        _wakeup = None
