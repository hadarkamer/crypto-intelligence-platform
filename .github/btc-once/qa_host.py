"""Disposable supervisor around the frozen collector; no product or source-reader changes.

Only the pinned UI run may submit the 24 BTC and ETH identities, each at attempt 1.
The source database is opened read-only once to restore the accepted encrypted
account snapshot. All job/session/evidence writes use local disposable Postgres.
"""
import asyncio
import base64
from datetime import datetime, timezone
import gzip
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys
import time
import threading
from uuid import UUID

from aiohttp import web

HERE = Path(__file__).resolve().parent
PRODUCT = HERE/'product'
ROOT = Path(os.getenv('CG_QA_ROOT','/tmp/coinglass-btc-once'))
sys.path[:0] = [str(PRODUCT/'runtime'),str(PRODUCT),str(HERE)]
from resource_sample import ResourceSample

PIN = json.loads((HERE/'run-pin.json').read_text())
SYMBOLS = ('BTC','ETH')
ALLOWED = {(s,m,t) for s in SYMBOLS for m in (1,2,3) for t in ('12H','24H','48H','1W')}
PREVIOUS = 'aa3ab31f-828f-4465-95da-60a462e802a5'
SECRET = secrets.token_urlsafe(40)
ADMITTED = {}
EVENTS = []
SOURCE_BLOCK = None
MONITOR = ResourceSample()
EPOCH_OFFSET = time.time()-time.monotonic()
STARTED = float((HERE.parent/'build-start-epoch').read_text())
END = STARTED + 26*60
BUILD_RESOURCES = json.loads((HERE.parent/'build-resources.json').read_text())
EXPORT_BYTES = 0
RUNNER = None
PG = None
DSN = None
BOUND_RUN = None
SAMPLING_STOP = threading.Event()

def sample_setup():
    # Continue emitting bounded resource summaries if the UI client cannot export.
    last_log = time.monotonic()
    while not SAMPLING_STOP.wait(.5):
        MONITOR.update(force=True)
        if time.monotonic()-last_log>=25:
            resource_log('serving')
            last_log = time.monotonic()

def resource_log(phase):
    data = MONITOR.report()
    data.pop('samples',None)
    print('BTC_RESOURCE '+json.dumps(dict(phase=phase,epoch=time.time(),resources=data),
          separators=(',',':')),flush=True)

def save_resources(phase):
    ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
    data=dict(phase=phase,epoch=time.time(),resources=MONITOR.report(),events=EVENTS)
    (ROOT/'supervisor-resources.json').write_text(json.dumps(data,default=json_default))
    resource_log(phase)

def event(kind, **data):
    item = dict(kind=kind,epoch=time.time(),**data)
    EVENTS.append(item)
    print('BTC_EXPERIMENT '+json.dumps(item,separators=(',',':')),flush=True)

def authorized(request):
    return hmac.compare_digest(request.headers.get('Authorization','').encode(),('Bearer '+SECRET).encode())

def connect():
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(DSN,autocommit=True,row_factory=dict_row,
        options='-c statement_timeout=3000 -c lock_timeout=1000')

def rows():
    with connect() as db:
        return db.execute('SELECT id,symbol,heatmap_model,timeframe,status,created_at,started_at,finished_at,error_code,attempt FROM public.ai_collection_bridge_jobs ORDER BY created_at,id').fetchall()

def json_default(value):
    if isinstance(value,datetime): return value.isoformat()
    return str(value)

def progress():
    return dict(admitted=len(ADMITTED),jobs=rows(),source_block=SOURCE_BLOCK,
        deadline_epoch=END,epochs_offset=EPOCH_OFFSET,resources=MONITOR.report() | {'samples':None})

def restore_and_start_database():
    global PG,DSN
    from pgserver import get_server
    import psycopg
    from psycopg.rows import dict_row
    from account_large_object import load,_id
    from dedicated_auth_state import account_slot
    from qa_restore import install_provisional_snapshot
    source_dsn = os.environ.pop('DATABASE_URL')
    key = base64.b64decode(os.environ['COINGLASS_SESSION_KEY'],validate=True)
    email = os.environ['COINGLASS_LOGIN_EMAIL']
    slot = account_slot(key,email)
    with psycopg.connect(source_dsn,autocommit=True,connect_timeout=5,row_factory=dict_row,
            options='-c default_transaction_read_only=on -c statement_timeout=4000 -c lock_timeout=1000') as source:
        matches = source.execute('SELECT i.slot FROM collector_private.coinglass_dedicated_auth i JOIN collector_private.coinglass_dedicated_auth_jobs j ON j.incident_id=i.id WHERE j.job_id=%s',(PREVIOUS,)).fetchall()
        if len(matches)!=1 or not hmac.compare_digest(matches[0]['slot'],slot):
            raise ValueError('saved_account_binding_mismatch')
        state = load(source,key,slot,PREVIOUS,_id(key,slot,PREVIOUS))
    del source_dsn
    event('saved_account_restored',source_db_connections=1,source_db_read_only=True,password_submissions=0)
    PG = get_server(ROOT/'pgdata',cleanup_mode='delete')
    DSN = PG.get_uri()
    os.environ['DATABASE_URL'] = DSN
    with connect() as db:
        db.execute((PRODUCT/'session_state.sql').read_text())
        db.execute((PRODUCT/'dedicated_auth_state.sql').read_text())
    install_provisional_snapshot(state,slot,event)
    del state,key
    event('isolated_database_ready',production_writes=0)

@web.middleware
async def admission(request,handler):
    global EXPORT_BYTES
    if request.path.startswith('/qa/'):
        return await handler(request)
    match = re.fullmatch(r'/api/collection/model([123])/jobs',request.path)
    if match and request.method=='POST':
        if not authorized(request): raise web.HTTPUnauthorized()
        if SOURCE_BLOCK or time.time()>=END-60:
            return web.json_response({'error':'experiment_closed'},status=409)
        raw = await request.read()
        try:
            body = json.loads(raw)
            identity = body['symbol'],int(match[1]),body['timeframe']
            rid = str(UUID(body['request_id']))
            if identity not in ALLOWED or body.get('attempt',1)!=1:
                raise ValueError()
            previous = ADMITTED.get(identity)
            if previous and previous!=rid: raise ValueError()
            if not previous:
                ADMITTED[identity]=rid
                event('admitted',symbol=identity[0],model=identity[1],timeframe=identity[2],request_id=rid)
        except (ValueError,KeyError,TypeError):
            return web.json_response({'error':'outside_single_attempt_experiment'},status=409)
        # The unchanged handler streams its body; return the exact bytes it would read.
        request.content.unread_data(raw)
    response = await handler(request)
    if getattr(response,'body',None): EXPORT_BYTES += len(response.body)
    return response

async def ready(request):
    return web.json_response({'ready':True,'collector':PIN['collector'],'app':PIN['app'],
        'collector_commit':os.getenv('RENDER_GIT_COMMIT'),
        'affinity_sha256':PIN['affinity_sha256'],'workflow_sha':PIN['sha'],
        'symbols':SYMBOLS,'tabs':2,'deadline_epoch':END,'started_epoch':STARTED})

async def session(request):
    global BOUND_RUN
    from ci_identity import verify
    token=request.headers.get('Authorization','')
    try:
        payload=token[7:].split('.')[1]
        claims=json.loads(base64.urlsafe_b64decode(payload+'='*(-len(payload)%4)))
        run=str(claims['run_id'])
        if not re.fullmatch(r'[1-9][0-9]*',run):raise ValueError()
    except Exception:raise web.HTTPUnauthorized() from None
    selected={**PIN,'run_id':run}
    if time.time()>=END or not token.startswith('Bearer ') or not verify(token[7:],selected) or BOUND_RUN not in (None,run):
        raise web.HTTPUnauthorized()
    if BOUND_RUN is None:
        BOUND_RUN=run
        event('workflow_bound',run_id=run,workflow_sha=PIN['sha'])
    return web.json_response({'token':SECRET,'deadline_epoch':END})

async def status(request):
    if not authorized(request): raise web.HTTPUnauthorized()
    return web.Response(body=json.dumps(progress(),default=json_default).encode(),content_type='application/json')

async def export(request):
    global EXPORT_BYTES
    if not authorized(request): raise web.HTTPUnauthorized()
    with connect() as db:
        jobs=db.execute('SELECT id,symbol,heatmap_model,timeframe,status,created_at,started_at,finished_at,error_code,attempt,result,image_png FROM public.ai_collection_bridge_jobs ORDER BY created_at,id').fetchall()
        evidence=db.execute('SELECT job_id,created_at,diagnostic,image_png FROM public.ai_collection_bridge_evidence ORDER BY created_at,job_id').fetchall()
        database=db.execute("SELECT numbackends,xact_commit,xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,tup_inserted,tup_updated,deadlocks FROM pg_stat_database WHERE datname=current_database()").fetchone()
    for row in jobs+evidence:
        image=row.pop('image_png',None)
        row['image_png_base64']=base64.b64encode(image).decode() if image else None
    raw=json.dumps(dict(pin=PIN,jobs=jobs,evidence=evidence,events=EVENTS,resources=MONITOR.report(),
        build_resources=BUILD_RESOURCES,collector_commit=os.getenv('RENDER_GIT_COMMIT'),database=database,
        outbound_body_bytes=EXPORT_BYTES,source_block=SOURCE_BLOCK,model_calls=0,
        metrics_log='\n'.join(line for line in (ROOT/'metrics.log').read_text().splitlines()
            if line.startswith(('COINGLASS_CAPACITY_BROWSER ','COINGLASS_CYCLE_MAP ','COINGLASS_METRICS ','BTC_EXPERIMENT ')))[-1500000:]),default=json_default,separators=(',',':')).encode()
    data=gzip.compress(raw,mtime=0)
    if len(data)>24*1024*1024: raise web.HTTPRequestEntityTooLarge(max_size=24*1024*1024,actual_size=len(data))
    EXPORT_BYTES+=len(data)
    return web.Response(body=data,content_type='application/gzip')

async def observe_limits():
    global SOURCE_BLOCK,RUNNER
    while True:
        MONITOR.update(force=True)
        sample=MONITOR.samples[-1]
        if sample[2] is not None and sample[2]>int(3.65*1024**3): SOURCE_BLOCK='memory_limit_approaching'
        # Reserve the bounded export and cleanup traffic after acquisition stops.
        if sample[4] is not None and MONITOR.samples[0][4] is not None and sample[4]-MONITOR.samples[0][4]>15_000_000:
            SOURCE_BLOCK='outbound_budget_approaching'
        if time.time()>=END-60: SOURCE_BLOCK='time_limit_approaching'
        try:
            jobs=await asyncio.to_thread(rows)
        except Exception:
            SOURCE_BLOCK='database_unavailable'
            jobs=[]
        blocked=next((r for r in jobs if r['error_code'] in ('source_login_required','source_rate_limited','auth_rate_limited','evidence_store_failed')),None)
        if blocked: SOURCE_BLOCK=blocked['error_code']
        if SOURCE_BLOCK:
            event('controlled_stop',reason=SOURCE_BLOCK)
            if RUNNER:
                # Stop the frozen worker lifecycle. Keep authenticated GETs open
                # until teardown so the UI can publish rows already committed.
                await RUNNER.app.cleanup()
            return
        await asyncio.sleep(.5)

async def main():
    global RUNNER
    shutdown=asyncio.Event()
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        loop.add_signal_handler(sig,shutdown.set)
    os.umask(0o077)
    if os.environ.get('RENDER_SERVICE_ID')=='srv-dakhacou01pc73f58aug': raise ValueError('production_service_rejected')
    if not os.environ.get('RENDER_SERVICE_NAME','').startswith('cg-btc-eth-ops-20261127'): raise ValueError('wrong_service')
    if os.environ.get('RENDER_GIT_BRANCH')!='qa/coinglass-btc-eth-operational-20261127': raise ValueError('wrong_branch')
    if time.time()>=END-120: raise ValueError('insufficient_experiment_time')
    ROOT.mkdir(mode=0o700,exist_ok=True)
    (ROOT/'started').touch(exist_ok=False)
    for name in ('OPENAI_API_KEY','COINGLASS_LOGIN_PASSWORD'):
        if os.environ.get(name): raise ValueError('forbidden_provider_secret')
    os.environ.update(COINGLASS_COLLECTOR_TOKEN=SECRET,COLLECTION_BRIDGE_ENABLED='true',
        COINGLASS_MODERN_UI_ENABLED='true',COINGLASS_SESSION_STORE_ENABLED='true',
        COINGLASS_VERIFIED_SYMBOLS='BTC,ETH',COINGLASS_CYCLE_TABS='2',COINGLASS_CYCLE_FIXED_SYMBOLS='BTC,ETH')
    await asyncio.to_thread(restore_and_start_database)
    import app as product_app
    product=product_app.create_app()
    product.middlewares.append(admission)
    # Public supervisor survives a controlled stop so the UI can export saved evidence.
    RUNNER=web.AppRunner(product,access_log=None)
    await RUNNER.setup()
    await web.TCPSite(RUNNER,'127.0.0.1',10001).start()
    import aiohttp
    client=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    async with client.get('http://127.0.0.1:10001/health') as response:
        health=await response.json()
        if (response.status!=200 or health.get('mode')!='collection'
                or health.get('store_connectivity',{}).get('ready') is not True
                or health.get('worker_concurrency') != 2
                or set(health.get('basic_profiles',{}).get('symbols',[])) != set(SYMBOLS)):
            await client.close()
            await RUNNER.cleanup()
            RUNNER=None
            raise ValueError('collector_not_active')
    # A successful read proves that startup applied the actual queue schema.
    rows()
    event('collector_health_verified',mode=health['mode'],tabs=health['worker_concurrency'],
          symbols=health['basic_profiles']['symbols'])
    async def proxy(request):
        if SOURCE_BLOCK and request.method=='POST':
            return web.json_response({'error':'experiment_closed'},status=409)
        async with client.request(request.method,'http://127.0.0.1:10001'+request.rel_url.path_qs,
                headers={'Authorization':request.headers.get('Authorization','')},data=await request.read()) as resp:
            data=await resp.read()
            return web.Response(body=data,status=resp.status,content_type=resp.content_type)
    public=web.Application(client_max_size=32768)
    public.add_routes([web.get('/qa/ready',ready),web.post('/qa/session',session),web.get('/qa/status',status),
        web.get('/qa/export',export),web.route('*','/{tail:.*}',proxy)])
    outer=web.AppRunner(public,access_log=None)
    await outer.setup()
    await web.TCPSite(outer,'0.0.0.0',int(os.getenv('PORT','10000'))).start()
    watcher=asyncio.create_task(observe_limits())
    event('armed',deadline_epoch=END,collector=PIN['collector'],app=PIN['app'])
    try:
        try:
            await asyncio.wait_for(shutdown.wait(),max(0,END-time.time()))
        except asyncio.TimeoutError:
            pass
    finally:
        watcher.cancel()
        if RUNNER: await RUNNER.cleanup()
        await outer.cleanup()
        await client.close()
        save_resources('service_stopped_before_database_cleanup')

def run():
    sampler=threading.Thread(target=sample_setup,daemon=True)
    sampler.start()
    try:
        asyncio.run(main())
    except BaseException as error:
        # Exception text can contain database connection details: record only
        # controlled codes/class names and retain metrics before cleanup.
        event('supervisor_failed',condition=getattr(error,'code',type(error).__name__))
        raise SystemExit(1) from None
    finally:
        save_resources('before_database_cleanup')
        if PG:
            PG.cleanup()
            event('isolated_database_cleaned')
        SAMPLING_STOP.set()
        sampler.join(timeout=2)
        save_resources('cleanup_complete')

if __name__=='__main__':
    run()
