"""Authenticated two-symbol collection on the existing job table and model routes."""
import asyncio
import hmac
import os
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from aiohttp import web

SYMBOLS = ('BTC', 'ETH')
MODELS = (1, 2, 3)
TIMEFRAMES = ('12H', '24H', '48H', '1W')


async def capabilities(request, active, store_ready):
    import scoped_worker
    expected = os.getenv('COINGLASS_COLLECTOR_TOKEN', '')
    supplied = request.headers.get('Authorization', '')
    if (len(expected) < 32 or len(supplied) > 1024
            or not hmac.compare_digest(supplied.encode(), ('Bearer ' + expected).encode())):
        raise web.HTTPUnauthorized()
    ready = active and store_ready and scoped_worker.available()
    if ready:
        account = await scoped_worker.account_status()
        ready = (account.get('code') == 'ready' or
                 account.get('code') == 'map_in_progress' and account.get('active_fixed_cycle') is True)
    result = {'ok': bool(ready), 'fixed_cycle_id_supported': bool(ready)}
    if ready:
        result.update(supported_models=MODELS, supported_timeframes=TIMEFRAMES,
                      supported_symbols=SYMBOLS, numeric_reader='coinglass.basic.v1',
                      acceptance='verified_basic', worker_concurrency=2,
                      basic_profiles={'mode':'Coin','reader':'coinglass.basic.v1','symbols':SYMBOLS,
                                      'heatmap_models':MODELS,'timeframes':TIMEFRAMES})
    return web.json_response(result, headers={'Cache-Control':'no-store'})


def initialize(c):
    # Make the legacy uniqueness fence specific to its protocol before dropping
    # the old all-protocol index. PostgreSQL applies these changes atomically.
    c.execute("""ALTER TABLE public.ai_collection_bridge_jobs
        ADD COLUMN IF NOT EXISTS symbol text NOT NULL DEFAULT 'BTC',
        ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 1 CHECK(attempt BETWEEN 1 AND 3),
        ADD COLUMN IF NOT EXISTS logical_request_id uuid,
        ADD COLUMN IF NOT EXISTS fixed_cycle_id uuid""")
    c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_legacy_active_model_tf
        ON public.ai_collection_bridge_jobs(heatmap_model,timeframe)
        WHERE fixed_cycle_id IS NULL AND status IN ('queued','running')""")
    c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_fixed_cycle_map
        ON public.ai_collection_bridge_jobs(fixed_cycle_id,symbol,heatmap_model,timeframe)
        WHERE fixed_cycle_id IS NOT NULL""")
    c.execute("DROP INDEX IF EXISTS public.ai_collection_bridge_one_active_model_tf")


def earlier_fixed_head(c):
    import scoped_worker
    if not scoped_worker.available():
        return False
    row = c.execute("""SELECT fixed_cycle_id IS NOT NULL AS paired
        FROM public.ai_collection_bridge_jobs q
        WHERE q.status='queued' AND (q.fixed_cycle_id IS NULL
            OR (q.symbol='BTC' AND q.heatmap_model=1 AND q.timeframe='12H')
            OR EXISTS (SELECT 1 FROM public.ai_collection_bridge_evidence e WHERE e.job_id=q.id
                AND e.image_png IS NOT NULL AND e.diagnostic ? 'measured_btc_v1'))
        ORDER BY created_at,id LIMIT 1""").fetchone()
    return bool(row and row['paired'])


def validated_identity(store, body):
    from collection_bridge import BridgeError
    if set(body) != {'fixed_cycle_id','request_id','symbol','attempt','timeframe'}:
        raise BridgeError('invalid_request','Invalid fixed-cycle fields')
    try:
        cycle = UUID(body['fixed_cycle_id'])
        rid = UUID(body['request_id'])
        if cycle.version != 4 or str(cycle) != body['fixed_cycle_id']:
            raise ValueError()
    except (ValueError,TypeError,AttributeError):
        raise BridgeError('invalid_request_id','Invalid cycle or request identifier') from None
    symbol, tf = body['symbol'], body['timeframe']
    if (symbol not in SYMBOLS or tf not in TIMEFRAMES or type(body['attempt']) is not int
            or body['attempt'] != 1 or store.heatmap_model not in MODELS):
        raise BridgeError('invalid_fixed_cycle','Unsupported map or attempt')
    return cycle,rid,symbol,tf


def view(row):
    from collection_bridge import envelope
    return dict(envelope(row), symbol=row['symbol'], attempt=row['attempt'],
                request_id=str(row['logical_request_id']))


def previous(c, store, cycle, rid, symbol, tf):
    from collection_bridge import BridgeError
    row = c.execute("""SELECT j.id,j.fixed_cycle_id,j.logical_request_id,j.symbol,j.attempt,
            j.heatmap_model,j.timeframe,j.status,j.result,j.error_code
        FROM public.ai_collection_bridge_requests r
        JOIN public.ai_collection_bridge_jobs j ON j.id=r.job_id WHERE r.id=%s""",(rid,)).fetchone()
    if row and (row['fixed_cycle_id'],row['logical_request_id'],row['symbol'],row['heatmap_model'],row['timeframe']) != (
            cycle,rid,symbol,store.heatmap_model,tf):
        raise BridgeError('idempotency_conflict','Request id belongs to another map or protocol',409)
    return view(row) if row else None


def existing(store, identity):
    with store.connect() as c:
        return previous(c,store,*identity)


def insert(store, identity):
    from collection_bridge import BridgeError, CREATE_LOCK
    cycle,rid,symbol,tf = identity
    with store.connect() as c:
        c.execute('SELECT pg_advisory_xact_lock(%s)',(CREATE_LOCK,))
        saved = previous(c,store,*identity)
        if saved: return saved
        if c.execute("""SELECT 1 FROM public.ai_collection_bridge_jobs
                WHERE fixed_cycle_id=%s AND symbol=%s AND heatmap_model=%s AND timeframe=%s""",
                (cycle,symbol,store.heatmap_model,tf)).fetchone():
            raise BridgeError('idempotency_conflict','Map already belongs to a request in this cycle',409)
        # The requested cycle is isolated from automatic cache/reuse. Another
        # active fixed cycle cannot reserve this same target a second time.
        if c.execute("""SELECT 1 FROM public.ai_collection_bridge_jobs WHERE fixed_cycle_id IS NOT NULL
                AND symbol=%s AND heatmap_model=%s AND timeframe=%s AND status IN ('queued','running')""",
                (symbol,store.heatmap_model,tf)).fetchone():
            raise BridgeError('map_in_progress','This map already has an active cycle',409)
        if (symbol,store.heatmap_model,tf) == ('BTC',1,'12H'):
            # Use the legacy retention windows, only for old terminal fixed jobs.
            # A nonterminal job may still need its evidence for local recovery.
            c.execute("""UPDATE public.ai_collection_bridge_evidence e SET image_png=NULL
                FROM public.ai_collection_bridge_jobs j WHERE e.job_id=j.id AND j.fixed_cycle_id IS NOT NULL
                AND j.status IN ('ready','failed') AND j.finished_at < now()-interval '6 hours'
                AND e.image_png IS NOT NULL""")
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET image_png=NULL
                WHERE fixed_cycle_id IS NOT NULL AND status IN ('ready','failed')
                AND finished_at<now()-interval '6 hours' AND image_png IS NOT NULL""")
            c.execute("""DELETE FROM public.ai_collection_bridge_jobs WHERE fixed_cycle_id IS NOT NULL
                AND status IN ('ready','failed') AND finished_at<now()-interval '7 days'""")
        row = c.execute("""INSERT INTO public.ai_collection_bridge_jobs
            (id,logical_request_id,fixed_cycle_id,symbol,heatmap_model,timeframe,attempt,status)
            VALUES(%s,%s,%s,%s,%s,%s,1,'queued')
            RETURNING id,logical_request_id,symbol,heatmap_model,timeframe,attempt,status,result,error_code""",
            (uuid4(),rid,cycle,symbol,store.heatmap_model,tf)).fetchone()
        c.execute("""INSERT INTO public.ai_collection_bridge_requests(id,timeframe,job_id,heatmap_model)
                     VALUES(%s,%s,%s,%s)""",(rid,tf,row['id'],store.heatmap_model))
        return view(row)


async def start(store, body):
    from collection_bridge import BridgeError
    import scoped_worker
    identity = validated_identity(store,body)
    saved = await asyncio.to_thread(existing,store,identity)
    if saved: return saved
    if not scoped_worker.available():
        raise BridgeError('collector_unavailable','Fixed-cycle runtime unavailable',503)
    status = await scoped_worker.account_status(str(identity[0]))
    code = status.get('code')
    if code != 'ready':
        # A concurrent submission may have committed while the account was checked.
        saved = await asyncio.to_thread(existing,store,identity)
        if saved: return saved
        statuses = {'source_rate_limited':429,'source_login_required':409,'map_in_progress':409}
        error = BridgeError(code if code in statuses else 'collector_unavailable',
                            'Fixed-cycle account is not ready',statuses.get(code,503))
        seconds = status.get('retry_after_seconds')
        if code=='source_rate_limited' and type(seconds) is int and seconds>0:
            error.fixed_cycle_rate_limit = True
            error.retry_after_seconds = seconds
            error.retry_at = (datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat()
        raise error
    result = await asyncio.to_thread(insert,store,identity)
    scoped_worker.wake()
    return result


def exact(store, jid, symbol, evidence=False):
    if symbol not in SYMBOLS: return None
    with store.connect() as c:
        if evidence:
            row = c.execute("""SELECT COALESCE(j.image_png,e.image_png) AS image_png
                FROM public.ai_collection_bridge_jobs j LEFT JOIN public.ai_collection_bridge_evidence e ON e.job_id=j.id
                WHERE j.id=%s AND j.heatmap_model=%s AND j.symbol=%s AND j.fixed_cycle_id IS NOT NULL
                AND j.status IN ('ready','failed') AND j.created_at>now()-interval '6 hours'""",
                (jid,store.heatmap_model,symbol)).fetchone()
            return bytes(row['image_png']) if row and row['image_png'] else None
        row = c.execute("""SELECT id,logical_request_id,symbol,heatmap_model,timeframe,attempt,status,result,error_code
                  FROM public.ai_collection_bridge_jobs
                  WHERE id=%s AND heatmap_model=%s AND symbol=%s AND fixed_cycle_id IS NOT NULL""",
                  (jid,store.heatmap_model,symbol)).fetchone()
        return view(row) if row else None
