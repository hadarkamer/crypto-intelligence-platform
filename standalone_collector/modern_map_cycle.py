"""Drain the existing CoinGlass ledger under one account owner and a bounded set of tabs."""
import asyncio
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
import json
import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
from time import monotonic

from model1_execution import CODES, StageFailure
from modern_map_checkpoint import CheckpointStore
from session_runtime import ManagedSession
from symbol_jobs import finish_claim, valid_result

MAX_JOBS = 96
JOB_DEADLINE = 350
_MAP_METRICS = ContextVar("coinglass_cycle_map_metrics", default=None)


def tab_limit():
    value = os.environ.get('COINGLASS_CYCLE_TABS', '1')
    if value not in tuple(str(i) for i in range(1,9)):
        raise ValueError('invalid_coinglass_cycle_tabs')
    return int(value)


def require_owner(conn):
    from collection_bridge import WORK_LOCK
    row = conn.execute("""SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory'
        AND granted AND mode='ExclusiveLock' AND pid=pg_backend_pid() AND classid=0 AND objid=%s AND objsubid=1
        AND database=(SELECT oid FROM pg_database WHERE datname=current_database())) AS held""",
        (WORK_LOCK,)).fetchone()
    if not row or not row['held']:
        raise RuntimeError('cycle_ownership_lost')


def next_claim(conn, guard):
    """The first and every subsequent tab share the same scope and owner fence."""
    from symbol_jobs import BASIC_RANGES, active_basic_symbols
    with guard, conn.transaction():
        require_owner(conn)
        return conn.execute("""UPDATE public.ai_collection_bridge_jobs SET status='running',started_at=clock_timestamp()
            WHERE id=(SELECT q.id FROM public.ai_collection_bridge_jobs q WHERE q.status='queued'
            AND q.symbol=ANY(%s) AND q.heatmap_model BETWEEN 1 AND 3 AND q.timeframe=ANY(%s)
            AND NOT EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs r WHERE r.status='running'
                AND (r.symbol,r.heatmap_model,r.timeframe)=(q.symbol,q.heatmap_model,q.timeframe))
            ORDER BY q.created_at,q.id FOR UPDATE OF q SKIP LOCKED LIMIT 1) RETURNING *""",
            (active_basic_symbols(),list(BASIC_RANGES))).fetchone()


def first_claim(store):
    """Use the existing advisory owner and stale-job policy for the numerical maps."""
    from collection_bridge import WORK_LOCK
    from contextlib import nullcontext
    from symbol_jobs import BASIC_RANGES, active_basic_symbols
    conn = store.connect()
    try:
        with conn.transaction():
            held = conn.execute("SELECT pg_try_advisory_lock(%s) AS held",(WORK_LOCK,)).fetchone()['held']
        if not held:
            conn.close()
            return None
        with conn.transaction():
            # An exclusive WORK_LOCK means the preceding session owner has gone.
            # Reopen a committed source without spending a network retry. The
            # old start timestamp stops being publishable in this transaction.
            scope = active_basic_symbols(),list(BASIC_RANGES)
            conn.execute("""UPDATE public.ai_collection_bridge_jobs j SET status='queued',started_at=NULL
                WHERE j.status='running' AND j.symbol=ANY(%s) AND j.heatmap_model BETWEEN 1 AND 3
                AND j.timeframe=ANY(%s) AND EXISTS (
                    SELECT 1 FROM public.ai_collection_bridge_evidence e WHERE e.job_id=j.id
                    AND e.image_png IS NOT NULL AND e.diagnostic ? 'measured_btc_v1')""", scope)
            conn.execute("""UPDATE public.ai_collection_bridge_jobs SET status='failed',
                error_code='interrupted',finished_at=now() WHERE status='running'
                AND started_at < now()-interval '15 minutes' AND symbol=ANY(%s)
                AND heatmap_model BETWEEN 1 AND 3 AND timeframe=ANY(%s)""",
                scope)
        row = next_claim(conn,nullcontext())
        if row:
            return conn,row
    except BaseException:
        conn.close()
        raise
    conn.close()
    return None


@asynccontextmanager
async def browser_child(owner, root, width):
    from session_runtime import WORKER_ENV
    env = {name: os.environ[name] for name in WORKER_ENV if name in os.environ
           and name not in ('OPENAI_API_KEY','OPENAI_MARKET_SCANNER_MODEL','PYTHONPATH','PYTHONHOME')}
    here = Path(__file__).resolve().parent
    transport = proc = None
    parents, children, streams = [], [], []
    try:
        await owner.begin()
        owner.message_budget = 32 + MAX_JOBS * 2
        transport = await owner.open_transport(env)
        for _ in range(width):
            local, remote = socket.socketpair()
            parents.append(local)
            children.append(remote)
        proc = await asyncio.create_subprocess_exec(sys.executable, '-B', str(here/'modern_map_cycle_child.py'),
            str(root), *(str(s.fileno()) for s in children), cwd=here, env=transport.env,
            pass_fds=(*transport.pass_fds,*(s.fileno() for s in children)),start_new_session=True,
            stderr=asyncio.subprocess.DEVNULL)
        transport.spawned()
        for s in children:
            s.close()
        for s in parents:
            streams.append(await asyncio.open_connection(sock=s,limit=4096))
        yield streams
    finally:
        try:
            for _,writer in streams:
                if not writer.is_closing():
                    with suppress(ConnectionError, OSError):
                        writer.write(b'null\n')
            if proc is not None and proc.returncode is None:
                try:
                    await asyncio.wait_for(proc.wait(),8)
                except asyncio.TimeoutError:
                    await stop_child(proc)
        finally:
            for _,writer in streams:
                writer.close()
            for sock in (*children, *parents):
                sock.close()
            await owner.finish(transport)


async def stop_child(proc):
    """Only signal verified children of this reader, including independent OCR groups."""
    pending = [proc.pid]
    handles = []
    # Hold pidfds to avoid ever signalling a recycled PID during cleanup.
    seen = set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        try:
            handles.append(os.pidfd_open(pid))
            for path in Path(f'/proc/{pid}/task').glob('*/children'):
                pending.extend(int(v) for v in path.read_text().split())
        except (FileNotFoundError,ProcessLookupError):
            pass
    try:
        for fd in handles:
            with suppress(ProcessLookupError):
                signal.pidfd_send_signal(fd,signal.SIGKILL)
        await asyncio.wait_for(proc.wait(),4)
    finally:
        for fd in handles:
            os.close(fd)


def attach_blocked_job(owner, row, code):
    """Let existing remediation wake every job actually gated on the same incident."""
    from dedicated_auth_state import INCIDENTS, JOBS, OPEN
    from symbol_jobs import canonical_auth_failure
    jid = str(row['id'])
    if code.startswith('auth_') or code=='source_login_required':
        with owner.connection_guard, owner.store.connection.transaction():
            owner.store.connection.execute(f"""INSERT INTO {JOBS}(job_id,incident_id)
                SELECT %s,id FROM {INCIDENTS} WHERE slot=%s AND {OPEN}
                ON CONFLICT(job_id) DO UPDATE SET incident_id=EXCLUDED.incident_id""",(jid,owner.store.slot))
            return canonical_auth_failure(owner.store.connection,owner.store,jid,code)
    return code


class QueueDrain:
    def __init__(self, store, held, row, root, width):
        self.store, self.held, self.root = store, held, root
        self.first, self.width, self.count = row, width, 0
        self.owner = ManagedSession.from_environment(held,str(row['id']))
        self.database = asyncio.Lock()
        self.starting = asyncio.Lock()
        self.child = self.streams = None
        self.closed = False

    async def sql(self, function, *args, **kwargs):
        # No tab gets a resident DB connection. Cancellation waits for the one
        # submitted statement before teardown, never starts a competing write.
        async with self.database:
            operation = args[0] if function == self.fenced else function
            key = {'read':'receipt_read', 'commit':'receipt_commit', 'finish_claim':'job_finish'}.get(getattr(operation,'__name__',''))
            started = monotonic()
            task = asyncio.create_task(asyncio.to_thread(function,*args,**kwargs))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                with suppress(Exception):
                    await task
                raise
            finally:
                metrics = _MAP_METRICS.get()
                if key and metrics is not None:
                    metrics[key+'_calls'] = metrics.get(key+'_calls',0) + 1
                    metrics[key+'_ms'] = metrics.get(key+'_ms',0) + round((monotonic()-started)*1000)

    def fenced(self, action, *args):
        with self.owner.connection_guard, self.held.transaction():
            require_owner(self.held)
        return action(*args)

    async def claim(self):
        async with self.database:
            if self.closed or self.count >= MAX_JOBS:
                return None
            if self.first is not None:
                row,self.first = self.first,None
            else:
                row = await asyncio.to_thread(next_claim,self.held,self.owner.connection_guard)
            if row:
                self.count += 1
            return row

    async def channel(self, index):
        async with self.starting:
            if self.closed:
                raise StageFailure('source_capture_failed')
            if self.streams is None:
                self.child = browser_child(self.owner,self.root,self.width)
                try:
                    self.streams = await self.child.__aenter__()
                except BaseException:
                    self.closed = True
                    self.child = None
                    raise
            return self.streams[index]

    async def result(self, record, root, job, dims):
        source = await self.sql(record.read)
        if source is None:
            return None
        from modern_map_task import numeric_result
        from modern_map_checkpoint import _identity
        import hashlib
        entry, image = source
        try:
            _identity(entry['source'],job,hashlib.sha256(image).hexdigest(),dims)
            result = numeric_result(entry['source']['snapshot'],job,image,restored=True)
        except (KeyError, ValueError, TypeError):
            raise StageFailure('image_evidence_invalid') from None
        if not valid_result(result,image,job,dims[2],dims[1],dims[0],True):
            raise StageFailure('image_evidence_invalid')
        return result,image

    async def process(self, index, row):
        job = str(row['id'])
        dims = row['symbol'],row['heatmap_model'],row['timeframe']
        root = self.root/job
        root.mkdir(mode=0o700)
        record = CheckpointStore(self.store,job,*dims,started_at=row['started_at'])
        # Restore a committed source even when no browser or account is available.
        retained = await self.result(record,root,job,dims)
        if retained:
            return retained
        reader, writer = await self.channel(index)
        writer.write(json.dumps([job,*dims]).encode()+b'\n')
        await writer.drain()
        while True:
            line = await reader.readline()
            if not line:
                raise EOFError('cycle_channel_closed')
            op,value = json.loads(line)
            if op=='retire':
                error = EOFError('cycle_tab_retired')
                error.code = value if value in CODES else 'source_capture_failed'
                raise error
            if op=='done':
                if value is not None:
                    raise StageFailure(value)
                break
            if op!='save' or value not in ('S','P'):
                raise StageFailure('evidence_store_failed')
            try:
                await self.sql(self.fenced,record.commit,value.encode(),root)
            except Exception:
                # Commit acknowledgement can be lost. Retry the identical
                # receipt only. No new source read can be requested here.
                try:
                    await self.sql(self.fenced,record.commit,value.encode(),root)
                except Exception:
                    writer.write(b'"failed"\n')
                    await writer.drain()
                    continue
            writer.write(b'"committed"\n')
            await writer.drain()
        result = json.loads((root/'result.json').read_text())
        image = (root/'image.png').read_bytes()
        if not valid_result(result,image,job,dims[2],dims[1],dims[0],True):
            raise StageFailure('image_evidence_invalid')
        return result,image

    async def finish(self,row,result,image,error=None):
        from collection_metrics import logged_phase
        with logged_phase(str(row['id']), 'collector_db_save_ms'):
            try:
                return await self.sql(self.fenced,finish_claim,self.store,row,result,image,error)
            except Exception:
                # A new claim has a different start time, including after a restart.
                return await self.sql(self.fenced,finish_claim,self.store,row,result,image,error)

    async def slot(self,index):
        from collection_metrics import queue_wait
        while row := await self.claim():
            queue_wait(row['id'],row)
            metrics = {'job_id':str(row['id']), 'outcome':'publication_unconfirmed'}
            token = _MAP_METRICS.set(metrics)
            started = monotonic()
            lane_broken = False
            try:
                async with asyncio.timeout(JOB_DEADLINE):
                    result,image = await self.process(index,row)
            except asyncio.CancelledError:
                applied = await self.finish(row,None,None,'interrupted')
                metrics['outcome'] = 'interrupted' if applied else 'superseded' 
                raise
            except Exception as exc:
                lane_broken = isinstance(exc,(TimeoutError,EOFError,ConnectionError))
                code = 'source_timeout' if isinstance(exc,TimeoutError) else 'source_capture_failed'
                error = getattr(exc,'code',code if lane_broken else 'scan_failed')
                if error.startswith('auth_') or error == 'source_login_required':
                    error = await self.sql(attach_blocked_job,self.owner,row,error)
                retained = None
                if lane_broken or error in ('source_capture_failed','evidence_store_failed','worker_crashed'):
                    job = str(row['id'])
                    dims = row['symbol'],row['heatmap_model'],row['timeframe']
                    record = CheckpointStore(self.store,job,*dims,started_at=row['started_at'])
                    retained = await self.result(record,self.root/job,job,dims)
                if retained:
                    applied = await self.finish(row,*retained)
                    metrics['outcome'] = 'ready' if applied else 'superseded'
                else:
                    outcome = error if error in CODES else 'scan_failed'
                    applied = await self.finish(row,None,None,outcome)
                    metrics['outcome'] = outcome if applied else 'superseded'
            else:
                applied = await self.finish(row,result,image)
                metrics['outcome'] = 'ready' if applied else 'superseded'
            finally:
                metrics['elapsed_ms'] = round((monotonic()-started)*1000)
                _MAP_METRICS.reset(token)
                print('COINGLASS_CYCLE_MAP '+json.dumps(metrics,separators=(',',':')),flush=True)
            if lane_broken:
                # Retire this pipe: unread replies from the failed job cannot
                # be attributed to another job. The other tabs keep draining.
                if self.streams:
                    self.streams[index][1].close()
                return

    async def isolated_slot(self,index):
        try:
            await self.slot(index)
        except Exception:
            # If a row write or its acknowledgement is still unavailable, leave
            # its existing lease and receipt for recovery. Other slots may be
            # able to publish their independent rows.
            print('COINGLASS_CYCLE slot stopped; pending row retains its claim',flush=True)

    async def run(self):
        try:
            async with asyncio.TaskGroup() as group:
                for index in range(self.width):
                    group.create_task(self.isolated_slot(index))
        finally:
            if self.child:
                await self.child.__aexit__(None,None,None)


async def run_cycle_worker(store):
    width = tab_limit()
    while True:
        held = None
        try:
            claimed = await asyncio.to_thread(first_claim,store)
            if claimed:
                held,row = claimed
                with tempfile.TemporaryDirectory(prefix='coinglass-cycle-') as tmp:
                    await QueueDrain(store,held,row,Path(tmp),width).run()
        except asyncio.CancelledError:
            raise
        except Exception:
            print('COINGLASS_CYCLE worker failure; retained verified job receipts remain available',flush=True)
        finally:
            if held is not None:
                await asyncio.to_thread(held.close)
        await asyncio.sleep(2)
