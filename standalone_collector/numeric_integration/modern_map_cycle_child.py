"""One secret-isolated Firefox process for a drain of the numerical map queue."""
import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import socket
import sys
import time
from uuid import UUID
from weakref import WeakValueDictionary

from model1_execution import AD_HOSTS, StageFailure
from modern_map_cycle_browser import ReaderPageAPI, SharedAccount, run_reader, wait
from modern_map_task import _process_basic_btc, check_ocr_runtime

MAP_TIMEOUT = 300


async def write(stream, message):
    stream.write(json.dumps(message, separators=(',', ':')).encode() + b'\n')
    await stream.drain()


async def read(stream):
    line = await stream.readline()
    if not line:
        raise EOFError()
    return json.loads(line)


async def lane(reader, writer, context, account, directory, index=0, first_observation=None):
    """A tab is reused only after its own job is closed and independently acknowledged."""
    from dedicated_map_flow import MapTarget
    from modern_map_basic_numbers import basic_target
    page = None
    page_number = 0
    try:
        while msg := await read(reader):
            trace_source = len(msg) == 5 and msg[-1] == 'trace_source'
            job, symbol, model, timeframe = msg[:4] if trace_source else msg
            if str(UUID(job)) != job or not basic_target(symbol, model, timeframe):
                raise ValueError('invalid_cycle_map')
            root = directory / job
            reading = False
            def source_event(event):
                if not trace_source:
                    return
                print('COINGLASS_CYCLE_SOURCE '+json.dumps(dict(
                    job_id=job, symbol=symbol, model=model, timeframe=timeframe, tab_index=index,
                    browser_child_pid=os.getpid(), page_number=page_number, event=event,
                    monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns()),separators=(',',':')),flush=True)
            async def save(action):
                await write(writer, ['save', action.decode('ascii')])
                if (await read(reader)) != 'committed':
                    raise StageFailure('evidence_store_failed')
            def capture(s, m, t, image, *, observe, base_only):
                assert base_only
                def market_event(kind, data):
                    nonlocal reading
                    if kind == 'range_selection':
                        from modern_map_basic_numbers import range_matches
                        import re
                        label,value = data.get('label'),data.get('value')
                        if range_matches(t,label) and (value is None or re.fullmatch('[a-zA-Z0-9 ._-]{1,32}',value)):
                            print('COINGLASS_CYCLE_RANGE '+json.dumps(dict(job_id=job,label=label,value=value)),flush=True)
                    else:
                        if kind == 'stage' and data.get('status') == 'ok':
                            name = data.get('name')
                            if name == 'basic_identity_start':
                                reading = True
                                source_event('read_start')
                                if (first_observation is not None and account.checkpointed
                                        and (s,m,t)==('BTC',1,'12H')):
                                    first_observation.set()
                            elif name == 'read_market_overview' and reading:
                                source_event('overview_read')
                            elif name == 'basic_end_observation' and reading:
                                source_event('read_end')
                                reading = False
                        observe(kind, data)
                return wait(account.read(page, MapTarget(s,m,t), image, market_event))
            error = None
            retire = False
            try:
                async with asyncio.timeout(MAP_TIMEOUT):
                    if trace_source and symbol=='ETH' and first_observation is not None:
                        await first_observation.wait()
                        if not account.checkpointed:
                            raise StageFailure('source_login_required')
                    account.allowed()
                    if not context.raw.browser.is_connected():
                        raise StageFailure('source_capture_failed')
                    if page is None:
                        page = context.wrap(await context.raw.new_page())
                        page_number += 1
                        source_event('page_open')
                    await run_reader(_process_basic_btc, root, job, lambda action: wait(save(action)),
                        base_only=True, target=(symbol, model, timeframe), capture_fn=capture)
            except Exception as failure:
                if reading:
                    source_event('read_error')
                error = ('source_timeout' if isinstance(failure, TimeoutError) else
                         getattr(failure, 'code', 'source_capture_failed'))
                if page is not None:
                    try:
                        async with asyncio.timeout(4):
                            await page.raw.close()
                    except Exception:
                        if context.raw.browser.is_connected() and not page.raw.is_closed():
                            # A tab which cannot close is never returned to the pool.
                            retire = True
                    page = None
                if not context.raw.browser.is_connected():
                    retire = True
            if first_observation is not None and (symbol,model,timeframe)==('BTC',1,'12H'):
                first_observation.set()
            await write(writer, ['retire' if retire else 'done', error])
            if retire:
                return
    except (ConnectionError, EOFError):
        # Parent cancellation or a retired pipe affects only this slot.
        return
    finally:
        writer.close()
        if page is not None and not page.raw.is_closed():
            try:
                async with asyncio.timeout(4):
                    await page.raw.close()
            except Exception:
                # The context/process owner guarantees final teardown.
                pass


async def serve(folder, fds):
    from playwright.async_api import async_playwright
    from session_capture import CaptureSession
    # Strip ownership/IPC metadata before starting any browser descendants.
    session = CaptureSession.from_environment()
    browser = context = None
    async with AsyncExitStack() as cleanup:
        streams = []
        for fd in fds:
            os.set_inheritable(fd, False)
            reader, writer = await asyncio.open_connection(sock=socket.socket(fileno=fd),limit=4096)
            cleanup.callback(writer.close)
            streams.append((reader, writer))
        try:
            try:
                check_ocr_runtime()
                check = session.source_cooldown()
                if check['code'] != 'allowed':
                    raise StageFailure(check['code'])
            except StageFailure as failure:
                # Deliver the real setup/cooldown reason for each incoming job.
                # No Firefox launch or authentication attempt is required.
                async def reject(reader, writer, code):
                    try:
                        while await read(reader):
                            await write(writer,['done',code])
                    except (ConnectionError,EOFError):
                        pass
                await asyncio.gather(*(reject(r,w,failure.code) for r,w in streams))
                return
            pw = await cleanup.enter_async_context(async_playwright())
            browser = await pw.firefox.launch(headless=True, timeout=20000,
                env={k:os.environ[k] for k in ('HOME','LANG','PATH','LD_LIBRARY_PATH') if k in os.environ})
            cleanup.push_async_callback(browser.close)
            raw = await browser.new_context(storage_state=session.state,locale='en-US',timezone_id='UTC',
                color_scheme='dark',device_scale_factor=1,viewport={'width':1440,'height':1280})
            cleanup.push_async_callback(raw.close)
            await raw.route(AD_HOSTS, lambda route: route.abort())
            cache = WeakValueDictionary()
            context = ReaderPageAPI(raw, cache)
            cache[raw] = context
            account = SharedAccount(context, session)
            first_observation = asyncio.Event()
            async with asyncio.TaskGroup() as group:
                for index,(reader, writer) in enumerate(streams):
                    group.create_task(lane(reader,writer,context,account,folder,index,first_observation))
        finally:
            await run_reader(session.finish,context)


if __name__ == '__main__':
    asyncio.run(serve(Path(sys.argv[1]), [int(v) for v in sys.argv[2:]]))
