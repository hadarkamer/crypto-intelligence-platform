"""Offline scheduler/IPC integration on the production queue drain and numerical reader.

Only the provider and persistence transport are replaced here. SQL ownership
and real browser binaries have separate acceptance coverage.
"""
import asyncio
from contextlib import asynccontextmanager, nullcontext
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

import modern_map_cycle as cycle
import modern_map_cycle_child as child
from modern_map_cycle_browser import SharedAccount, ReaderPageAPI, run_reader, wait
from dedicated_map_flow import MapTarget
from model1_execution import StageFailure
from modern_map_basic_numbers import build_basic_snapshot, basic_source_url
from test_modern_map_basic_numbers import live_inputs

PNG = b'\x89PNG\r\n\x1a\nsynthetic-offline-map'
TARGETS = [('BTC',1,'12H'),('DOGE',2,'1W'),('ZEC',3,'48H'),('ETH',2,'24H')]


def row(target):
    symbol,model,timeframe = target
    return dict(id=str(uuid4()),symbol=symbol,heatmap_model=model,timeframe=timeframe,
                attempt=1,started_at='test-lease')


def sample(target):
    market,args = live_inputs()
    symbol,model,timeframe = target
    label = {'1W':'7 days','48H':'48 hour','24H':'24 hour','12H':'12 hour'}[timeframe]
    args.update(symbol=symbol,model=model,timeframe=timeframe,source_url=basic_source_url(symbol),
                source_sha256=hashlib.sha256(PNG).hexdigest())
    args['end_observation'].update(symbol=symbol,heatmap_model=model,timeframe=timeframe,
                                  range_control={'label':label,'value':timeframe.lower()})
    return build_basic_snapshot(market,**args)


class Page:
    def __init__(self):
        self.raw = self
        self.closed = False

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True


class DrainTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self,width, *, lost_ack=False, fail_first=False, close_after_save=False):
        rows = [row(t) for t in TARGETS]
        queue = iter(rows[1:])
        owner = SimpleNamespace(connection_guard=threading.RLock())
        context = Mock()
        context.raw.browser.is_connected.return_value = True
        pages = []
        async def new_page():
            p = Page()
            pages.append(p)
            return p
        context.raw.new_page = new_page
        context.wrap = lambda p:p
        active = 0
        peak = 0
        starts = []
        # Hold until all configured lanes are reading the first wave.
        barrier = asyncio.Event()
        async def source(page,target,path,observe):
            nonlocal active, peak
            target = target.symbol,target.model,target.timeframe
            starts.append(target)
            active += 1
            peak=max(peak,active)
            if active==width:
                barrier.set()
            try:
                await asyncio.wait_for(barrier.wait(),2)
                if fail_first and target==TARGETS[0]:
                    raise StageFailure('source_map_unavailable')
                path.write_bytes(PNG)
                return sample(target)
            finally:
                active-=1
        account = SimpleNamespace(read=source,allowed=lambda:None)
        stored, saved, finished = {}, [], {}
        class Record:
            def __init__(self,store,job,*dims,started_at):
                self.job=job
            def read(self):
                return stored.get(self.job)
            def commit(self,action,root):
                assert action==b'S'
                saved.append(self.job)
                rec=json.loads((root/'measured-source.json').read_text())
                stored[self.job]=({'source':rec},(root/'image.png').read_bytes())
                if lost_ack and len(saved)==1:
                    raise OSError('simulated lost acknowledgement')
        def finish(store,r,result,image,error):
            finished[r['id']] = (result,error)
            return True
        @asynccontextmanager
        async def channels(o,root,n):
            self.assertIs(o,owner)
            streams,tasks=[],[]
            for _ in range(n):
                left,right=socket.socketpair()
                rd,wr = await asyncio.open_connection(sock=left)
                streams.append((rd,wr))
                rd2,wr2 = await asyncio.open_connection(sock=right)
                tasks.append(asyncio.create_task(child.lane(rd2,wr2,context,account,root)))
            try:
                yield streams
            finally:
                for _,w in streams:
                    w.close()
                await asyncio.gather(*tasks)
        original_write = child.write
        async def disconnect(writer,message):
            if close_after_save and message==['done',None]:
                writer.close()
                raise EOFError()
            await original_write(writer,message)
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(cycle.ManagedSession,'from_environment',return_value=owner), \
                patch.object(cycle,'next_claim',side_effect=lambda *_:next(queue,None)), \
                patch.object(cycle,'require_owner'), patch.object(cycle,'browser_child',channels), \
                patch.object(cycle,'CheckpointStore',Record), patch.object(cycle,'finish_claim',finish), \
                patch.object(child,'write',disconnect):
            from modern_map_checkpoint import SOURCE
            self.assertEqual(SOURCE,'measured-source.json')
            held=SimpleNamespace(transaction=nullcontext)
            drain=cycle.QueueDrain(object(),held,rows[0],Path(temp),width)
            await asyncio.wait_for(drain.run(),4)
        self.assertTrue(all(p.closed for p in pages))
        return rows,starts,saved,finished,peak,len(pages)

    async def test_parallel_lanes_reuse_pages_and_independently_commit_all_dimensions(self):
        rows,starts,saved,finished,peak,pages = await self.exercise(2,lost_ack=True)
        self.assertEqual((peak,pages,len(starts),len(set(saved)),len(saved)),(2,2,4,4,5))
        for work,target in zip(rows,TARGETS):
            result,error = finished[work['id']]
            self.assertIsNone(error)
            self.assertEqual((result['symbol'],result['heatmap_model'],result['timeframe']),target)
            self.assertEqual(result['result_type'],'verified_basic')

    async def test_single_slot_and_unavailable_map_preserve_other_result_publications(self):
        rows,starts,saved,finished,peak,pages=await self.exercise(1,fail_first=True)
        self.assertEqual(peak,1)
        self.assertEqual(finished[rows[0]['id']],(None,'source_map_unavailable'))
        self.assertEqual((len(saved),len(finished),pages),(3,4,2))

    async def test_worker_eof_after_verified_receipt_publishes_without_recapture(self):
        rows,starts,saved,finished,peak,pages=await self.exercise(2,close_after_save=True)
        self.assertEqual(len(set(starts)),2)
        self.assertEqual(len(saved),2)
        self.assertTrue(all(res and err is None for res,err in finished.values()))

    async def test_stalled_channel_retires_only_that_slot(self):
        jobs=[row(t) for t in TARGETS]
        pending=iter(jobs[1:])
        entered=asyncio.Event()
        done={}
        async def work(index,r):
            if r==jobs[0]:
                entered.set()
                raise TimeoutError()
            await entered.wait()
            return {},b'ok'
        async def finish(r,result,image,err=None):
            done[r['id']]=err
            return True
        class Empty:
            def __init__(self,*args,**kwargs):pass
            def read(self):return None
        with tempfile.TemporaryDirectory() as tmp,patch.object(cycle.ManagedSession,'from_environment',
                return_value=SimpleNamespace(connection_guard=threading.RLock())), \
                patch.object(cycle,'next_claim',side_effect=lambda *_:next(pending,None)), \
                patch.object(cycle,'CheckpointStore',Empty):
            drain=cycle.QueueDrain(object(),object(),jobs[0],Path(tmp),2)
            drain.process,drain.finish=work,finish
            await drain.run()
        self.assertEqual([done[r['id']] for r in jobs],['source_timeout',None,None,None])


class SharedAccountTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_anonymous_tabs_renew_once_and_subsequent_jobs_reuse(self):
        account=Mock()
        account.state={}
        shared=SharedAccount(object(),account)
        pages=[Mock(),Mock()]
        calls=[]
        def login(*args):
            calls.append('login')
            wait(asyncio.sleep(.01))
        def select(_,page,target,*args):
            return ('map',target)
        target=MapTarget('DOGE',2,'1W')
        with patch('dedicated_map_flow._authenticate',login),patch('dedicated_map_flow._checked_action',select):
            results=await asyncio.gather(*(shared.read(p,target,'image',None) for p in pages))
            self.assertEqual(len(results),2)
            await shared.read(pages[0],target,'image',None)
        self.assertEqual((calls,shared.generation),(['login'],1))
        self.assertIsNone(account.page)

    async def test_explicit_cooldown_stops_next_navigation_in_an_already_running_reader(self):
        account=Mock()
        account.state={'origins':[{}]}
        shared=SharedAccount(object(),account)
        from weakref import WeakValueDictionary
        raw=Mock()
        protected=ReaderPageAPI(raw,WeakValueDictionary())
        ready=asyncio.Event()
        observed=asyncio.Event()
        def select(action,page,target,*args):
            if target.symbol=='ZEC':
                ready.set()
                wait(observed.wait())
                page.goto('https://www.coinglass.com/')
            else:
                wait(ready.wait())
                raise StageFailure('source_rate_limited')
        async def read(symbol):
            try:
                await shared.read(protected,MapTarget(symbol,2,'12H'),'image',None)
            except StageFailure as error:
                observed.set()
                return error.code
        with patch('dedicated_map_flow._checked_action',select):
            result = await asyncio.gather(read('ZEC'),read('ETH'))
        self.assertEqual(result,['source_rate_limited']*2)
        raw.goto.assert_not_called()
        account.source_cooldown.assert_called_once()

    async def test_number_of_tabs_defaults_to_one_and_rejects_invalid_input(self):
        for val in ('0','9','2.5','unlimited'):
            with patch.dict(os.environ,{'COINGLASS_CYCLE_TABS':val}), self.assertRaises(ValueError):
                cycle.tab_limit()
        with patch.dict(os.environ,{},clear=True):
            self.assertEqual(cycle.tab_limit(),1)


class TeardownTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_cooldown_stops_whole_new_child_before_launch_and_releases_session(self):
        import session_capture
        account=Mock()
        account.source_cooldown.return_value={'code':'source_rate_limited'}
        left,right=socket.socketpair()
        reader,writer=await asyncio.open_connection(sock=left)
        target=row(TARGETS[1])
        with tempfile.TemporaryDirectory() as tmp, patch.object(child,'check_ocr_runtime'), \
                patch.object(session_capture.CaptureSession,'from_environment',return_value=account), \
                patch('playwright.async_api.async_playwright') as engine:
            serve=asyncio.create_task(child.serve(Path(tmp),[right.detach()]))
            try:
                await child.write(writer,[target['id'],*TARGETS[1]])
                self.assertEqual(await child.read(reader),['done','source_rate_limited'])
                await child.write(writer,None)
                await asyncio.wait_for(serve,2)
            finally:
                writer.close()
                await writer.wait_closed()
        account.finish.assert_called_once_with(None)
        engine.assert_not_called()

    async def test_worker_cancel_shuts_both_tabs_context_browser_and_account(self):
        from unittest.mock import AsyncMock
        import session_capture
        account=Mock()
        account.state={}
        account.source_cooldown.return_value={'code':'allowed'}
        context=Mock()
        context.close=AsyncMock()
        context.route=AsyncMock()
        browser=Mock()
        browser.new_context=AsyncMock(return_value=context)
        browser.close=AsyncMock()
        driver=Mock()
        driver.firefox.launch=AsyncMock(return_value=browser)
        @asynccontextmanager
        async def engine():
            yield driver
        started=asyncio.Event()
        stops=[]
        active=0
        async def busy(r,w,ctx,acct,folder):
            nonlocal active
            active+=1
            if active==2:
                started.set()
            try:
                await asyncio.Future()
            finally:
                stops.append(w)
                w.close()
        pairs=[socket.socketpair() for _ in range(2)]
        with tempfile.TemporaryDirectory() as tmp,patch.object(child,'check_ocr_runtime'), \
                patch.object(session_capture.CaptureSession,'from_environment',return_value=account), \
                patch('playwright.async_api.async_playwright',engine),patch.object(child,'lane',busy):
            serve=asyncio.create_task(child.serve(Path(tmp),[r.detach() for _,r in pairs]))
            try:
                await asyncio.wait_for(started.wait(),2)
                serve.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await serve
            finally:
                for l,r in pairs:
                    l.close()
                    r.close()
        self.assertEqual(len(stops),2)
        driver.firefox.launch.assert_awaited_once()
        browser.new_context.assert_awaited_once()
        browser.close.assert_awaited_once()
        context.close.assert_awaited_once()
        account.finish.assert_called_once()

    async def test_parent_spawn_failure_always_releases_lease_and_strips_secrets(self):
        from unittest.mock import AsyncMock
        transport=SimpleNamespace(env={},pass_fds=())
        owner=Mock()
        owner.begin=AsyncMock()
        owner.finish=AsyncMock()
        owner.open_transport=AsyncMock(return_value=transport)
        fake_env={key:'test-only' for key in ('DATABASE_URL','OPENAI_API_KEY','COINGLASS_LOGIN_PASSWORD',
                                              'COINGLASS_LOGIN_EMAIL','COINGLASS_SESSION_KEY','PYTHONPATH')}
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ,fake_env), \
                patch.object(asyncio,'create_subprocess_exec',side_effect=OSError('fixture spawn failure')):
            with self.assertRaises(OSError):
                async with cycle.browser_child(owner,Path(tmp),2):
                    self.fail('spawn should not reach a ready channel')
        owner.begin.assert_awaited_once()
        owner.finish.assert_awaited_once_with(transport)
        self.assertFalse(fake_env.keys() & owner.open_transport.await_args.args[0].keys())
