"""Independent Model1 service. No Telegram/bot imports. No scheduled collection.

--probe runs ONE public 12H capture, with no model request. The web process serves
only health and disabled collection APIs until the required configuration exists.
"""
import asyncio
import contextlib
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone

HERE=Path(__file__).resolve().parent
RUNTIME=HERE/'runtime'
sys.path.insert(0,str(RUNTIME))
DIAG=HERE/'diagnostics'

def probe():
    DIAG.mkdir(exist_ok=True)
    # Source-only test uses no authenticated browser session and no AI key.
    # No existing service's settings or credentials are read or transferred.
    for name in ('COINGLASS_STORAGE_STATE_JSON','COINGLASS_COOKIE_HEADER'):
        if os.environ.get(name):
            raise RuntimeError('Public diagnostic requires an empty source session')
    from market_vision.coinglass_heatmap_capture import capture_heatmaps
    report={'timeframe_requested':'12H','source_attempts':1,'ai_calls':0,
            'time_utc':datetime.now(timezone.utc).isoformat(),'status':'failed'}
    try:
        images=capture_heatmaps(DIAG,timeframes=('12h',))
        report.update(status='render_checks_passed',image=Path(images[0]['image']).name,
                      note='Human visual inspection still required before analysis or saving.')
    except Exception as exc:
        report['error_type']=type(exc).__name__
        report['error']=str(exc).split('\n')[0][:240]
        state=DIAG/'not-ready.json'
        if state.exists(): report['observed_state']=json.loads(state.read_text())
        if (DIAG/'not-ready.png').exists(): report['image']='not-ready.png'
    (DIAG/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print('MODEL1_SOURCE_PROBE '+json.dumps(report),flush=True)

async def run_worker(store):
    # Independent queue consumer. It intentionally does not use ai_market_vision
    # or import the bot's own event loop, Telegram handlers, or trading workers.
    from collection_bridge import run_existing_scanner, BridgeError
    while True:
        held=None
        try:
            claimed=await asyncio.to_thread(store.claim)
            if claimed:
                held,row=claimed
                jid=str(row['id'])
                try:
                    result,image=await run_existing_scanner(row['timeframe'],jid)
                    await asyncio.to_thread(store.finish,jid,result,image)
                except asyncio.CancelledError:
                    await asyncio.to_thread(store.finish,jid,None,None,'interrupted')
                    raise
                except BridgeError as exc:
                    await asyncio.to_thread(store.finish,jid,None,None,exc.code)
                except Exception:
                    await asyncio.to_thread(store.finish,jid,None,None,'scan_failed')
        except asyncio.CancelledError:
            raise
        except Exception:
            print('MODEL1_WORKER temporary store failure',flush=True)
        finally:
            if held is not None: await asyncio.to_thread(held.close)
        await asyncio.sleep(2)

def create_app():
    from aiohttp import web
    from collection_bridge import JobStore, register_collection_routes
    application=web.Application(client_max_size=2048)
    enabled=os.getenv('COLLECTION_BRIDGE_ENABLED','').lower()=='true'
    configured=bool(os.getenv('DATABASE_URL') and os.getenv('OPENAI_API_KEY')
                    and len(os.getenv('COINGLASS_COLLECTOR_TOKEN',''))>=32)
    active=enabled and configured
    async def health(_):
        result={'ok':True,'service':'decision-hub-model1-collector',
                'mode':'collection' if active else 'not_configured',
                'bot_started':False,'scheduled_collection':False}
        report=DIAG/'report.json'
        if report.exists(): result['source_probe']=json.loads(report.read_text())
        return web.json_response(result,headers={'Cache-Control':'no-store'})
    application.router.add_get('/',health)
    application.router.add_get('/health',health)
    # Public diagnostics contain only the unauthenticated public chart. Fixed
    # allowlisted filenames; no filesystem browsing and no source/model actions.
    async def diagnostic_image(request):
        report=DIAG/'report.json'
        if not report.exists(): raise web.HTTPNotFound()
        name=json.loads(report.read_text()).get('image')
        if name not in {'not-ready.png','coinglass_btc_heatmap_12h.png'}: raise web.HTTPNotFound()
        return web.FileResponse(DIAG/name,headers={'Cache-Control':'no-store'})
    if os.getenv('PUBLIC_SOURCE_DIAGNOSTIC','').lower()=='true':
        application.router.add_get('/source-probe.png',diagnostic_image)
    store=JobStore(os.getenv('DATABASE_URL',''),hourly_limit=2) if active else None
    register_collection_routes(application,store=store,enabled=active,start_worker=False)
    if active:
        async def lifecycle(app):
            await asyncio.to_thread(store.initialize)
            task=asyncio.create_task(run_worker(store))
            yield
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await task
        application.cleanup_ctx.append(lifecycle)
    return application

if __name__=='__main__':
    if '--probe' in sys.argv:
        probe()
    else:
        from aiohttp import web
        web.run_app(create_app(),host='0.0.0.0',port=int(os.getenv('PORT','10000')),access_log=None)
