"""Focused managed-session tests: synthetic account events, real pipe, real build.

No browser, source, API, package install or live database is opened. The complete
runtime generation uses the real project adapters with browser preparation and
subprocess test invocations substituted explicitly; generated code is inspected.
"""
import ast
import asyncio
import copy
from contextlib import ExitStack, nullcontext, redirect_stdout
import fcntl
import io
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
from typing import Any
import unittest
from types import SimpleNamespace
from urllib.parse import urlsplit
from unittest.mock import Mock, patch

from model1_execution import StageFailure
from session_capture import (ACCOUNT_URL, CaptureSession, FRAME_LIMIT, METADATA,
                             note_protected_map_ready, snapshot_coinglass)

RUN_ID = '10000000-0000-4000-8000-000000000001'
COOKIE = {'name':'next-auth.session-token','value':'DUMMY_STATE_NOT_SECRET',
          'domain':'.coinglass.com','path':'/','expires':-1,
          'httpOnly':True,'secure':True,'sameSite':'Lax'}
STATE = {'cookies':[COOKIE], 'origins':[]}


class Page:
    def __init__(self):
        self.url = 'https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'
        self.main_frame = SimpleNamespace(url=self.url)
        self.listeners = {}
    def on(self, event, callback): self.listeners.setdefault(event, []).append(callback)
    def remove_listener(self, event, callback): self.listeners[event].remove(callback)
    def emit(self, event, request):
        for callback in list(self.listeners.get(event, [])): callback(request)


class Context:
    def __init__(self, cookies=None):
        self.values = copy.deepcopy(cookies if cookies is not None else [COOKIE])
        self.reads = 0
    def new_cdp_session(self, page):
        def send(method, params):
            self.reads += 1
            assert method == 'Network.getCookies'
            assert params['urls'] and all(url.startswith('https://') for url in params['urls'])
            assert all('.invalid' not in url for url in params['urls'])
            return {'cookies': self.values}
        return SimpleNamespace(send=send, detach=lambda: None)


def request(page, body=b'{"success":true,"data":{"private":"DO_NOT_EMIT"}}', *, status=200, url=ACCOUNT_URL):
    response = SimpleNamespace(status=status, body=lambda:body)
    return SimpleNamespace(url=url, method='GET', resource_type='fetch',
        redirected_from=None, redirected_to=None, frame=page.main_frame, response=lambda:response)


class CaptureSessionTests(unittest.TestCase):
    def open(self):
        read_fd, write_fd = os.pipe()
        environment = {'COINGLASS_SESSION_RETURN_FD':str(write_fd),
            'COINGLASS_SESSION_RUN_ID':RUN_ID,'COINGLASS_SESSION_REVISION':'4',
            'COINGLASS_STORAGE_STATE_JSON':json.dumps(STATE),'COINGLASS_COOKIE_HEADER':'',
            'COINGLASS_SESSION_KEY':'DUMMY_PARENT_ONLY_KEY'}
        with patch.dict(os.environ, environment):
            session = CaptureSession.from_environment()
            self.assertFalse(any(key in os.environ for key in (*METADATA,
                'COINGLASS_STORAGE_STATE_JSON','COINGLASS_COOKIE_HEADER','COINGLASS_SESSION_KEY')))
        self.assertTrue(fcntl.fcntl(write_fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC)
        self.addCleanup(os.close, read_fd)
        return session, read_fd

    def receive(self, read_fd):
        prefix = os.read(read_fd, 4)
        self.assertEqual(len(prefix), 4)
        length = struct.unpack('!I', prefix)[0]; self.assertLessEqual(length, FRAME_LIMIT)
        raw = b''
        while len(raw) < length: raw += os.read(read_fd, length - len(raw))
        self.assertEqual(os.read(read_fd, 1), b'')
        value = json.loads(raw)
        self.assertEqual(set(value), {'run_id','revision','outcome','state'})
        self.assertEqual(value['run_id'], RUN_ID); self.assertEqual(value['revision'], 4)
        return value

    def test_load_excludes_old_header_and_parent_key_before_driver(self):
        session, read_fd = self.open()
        self.assertEqual(session.state, STATE)
        session.finish(None)
        self.assertEqual(self.receive(read_fd)['outcome'], 'unconfirmed')
        self.assertEqual(session.state, {})

    def test_firefox_restored_state_rotates_only_after_protected_confirmation(self):
        page=Page()
        context=Mock(browser=SimpleNamespace(browser_type=SimpleNamespace(name='firefox')))
        context.cookies.return_value=[dict(COOKIE)]
        session=CaptureSession(copy.deepcopy(STATE),None,RUN_ID,4)
        session.dedicated=True
        session.attach(context,page)
        self.assertFalse(session.state_changed(context))
        rotated=dict(COOKIE,value='SYNTHETIC_ROTATION')
        context.cookies.return_value=[rotated,dict(COOKIE,domain='unrelated.invalid')]
        self.assertTrue(session.state_changed(context))
        with self.assertRaises(StageFailure) as caught:session.checkpoint(context)
        self.assertEqual(caught.exception.code,'session_observation_failed')
        note_protected_map_ready(page)
        with patch.object(session,'_request',return_value={'revision':5}) as write:
            session.checkpoint(context)
        expected={'cookies':[rotated],'origins':[]}
        write.assert_called_once_with('checkpoint',state=expected,incident_id=None)
        self.assertEqual((session.state,session.revision),(expected,5))
        for call in context.cookies.call_args_list:
            self.assertTrue(call.args[0])
            self.assertTrue(all(urlsplit(u).hostname in
                ('coinglass.com','www.coinglass.com','fapi.coinglass.com','capi.coinglass.com') for u in call.args[0]))
        context.new_cdp_session.assert_not_called()
        context.storage_state.assert_not_called()
        context.cookies.return_value=[dict(rotated,partitionKey='unsupported')]
        with self.assertRaises(StageFailure) as caught:session.state_changed(context)
        self.assertEqual(caught.exception.code,'session_snapshot_failed')
        self.assertEqual((session.state,session.revision),(expected,5))

    def test_partial_metadata_never_falls_back_to_legacy(self):
        for metadata in ({METADATA[1]:RUN_ID}, {METADATA[2]:'4'}):
            with self.subTest(metadata=list(metadata)):
                with patch.dict(os.environ, {**metadata,'COINGLASS_STORAGE_STATE_JSON':json.dumps(STATE),
                                            'COINGLASS_COOKIE_HEADER':'obe=DUMMY_OLD'}):
                    with self.assertRaises(StageFailure) as caught: CaptureSession.from_environment()
                    self.assertEqual(caught.exception.code, 'session_input_invalid')

    def test_no_metadata_keeps_legacy_state_but_never_parent_key(self):
        with patch.dict(os.environ, {'COINGLASS_COOKIE_HEADER':'legacy-dummy',
                                    'COINGLASS_SESSION_KEY':'DUMMY_PARENT_ONLY_KEY'}, clear=True):
            self.assertIsNone(CaptureSession.from_environment())
            self.assertEqual(os.environ['COINGLASS_COOKIE_HEADER'], 'legacy-dummy')
            self.assertNotIn('COINGLASS_SESSION_KEY', os.environ)

    def test_nonempty_child_legacy_header_is_rejected(self):
        read_fd, write_fd=os.pipe();self.addCleanup(os.close,read_fd)
        with patch.dict(os.environ, {METADATA[0]:str(write_fd),METADATA[1]:RUN_ID,METADATA[2]:'1',
            'COINGLASS_STORAGE_STATE_JSON':json.dumps(STATE),'COINGLASS_COOKIE_HEADER':'obe=DUMMY_OLD'}):
            with self.assertRaises(StageFailure):CaptureSession.from_environment()
        self.assertEqual(os.read(read_fd,1),b'')

    def test_ready_state_survives_unrelated_hover_failure_and_includes_new_cg_cookie(self):
        session, read_fd=self.open();page=Page()
        new_cookie={**COOKIE,'name':'new-session-cookie','domain':'www.coinglass.com','value':'DUMMY_NEW'}
        foreign={**COOKIE,'domain':'.elsewhere.invalid','value':'DUMMY_FOREIGN'}
        context=Context([COOKIE,new_cookie,foreign]);session.attach(context,page)
        note_protected_map_ready(page)
        session.note_source_error(StageFailure('source_capture_failed'))
        output=io.StringIO()
        with redirect_stdout(output):session.finish(context)
        value=self.receive(read_fd)
        self.assertEqual(output.getvalue(),'')
        self.assertEqual(value['outcome'],'ready')
        self.assertEqual(value['state']['cookies'],[COOKIE,new_cookie])
        self.assertEqual(context.reads,1);self.assertFalse(any(page.listeners.values()))
        session.finish(context);self.assertEqual(context.reads,1)

    def test_protected_gate_wins_even_after_earlier_ready(self):
        session, read_fd = self.open()
        page, context = Page(), Context()
        session.attach(context, page)
        note_protected_map_ready(page)
        session.note_source_error(StageFailure('source_login_required'))
        session.finish(context)
        value = self.receive(read_fd)
        self.assertEqual(value['outcome'], 'blocked')
        self.assertIsNone(value['state'])
        self.assertEqual(context.reads, 0)

    def test_public_page_or_account_success_does_not_establish_access(self):
        session, read_fd = self.open()
        page, context = Page(), Context()
        session.attach(context, page)
        req = request(page)
        page.emit('request', req)
        page.emit('requestfinished', req)
        note_protected_map_ready(Page())  # a different page is not the target
        session.finish(context)
        self.assertEqual(self.receive(read_fd)['outcome'], 'unconfirmed')
        self.assertEqual(context.reads, 0)

    def test_snapshot_failure_preserves_map_result_and_is_not_ready(self):
        session, read_fd = self.open()
        page, context = Page(), Context([])
        session.attach(context, page)
        note_protected_map_ready(page)
        with redirect_stdout(io.StringIO()) as events:
            session.finish(context)
        self.assertEqual(self.receive(read_fd)['outcome'], 'unconfirmed')
        self.assertEqual(events.getvalue().strip(), 'COINGLASS_SESSION session_snapshot_failed')

    def test_broken_transport_and_foreign_origin_fail_closed(self):
        session, read_fd = self.open()
        page, context = Page(), Context()
        session.attach(context, page)
        page.url = 'https://example.invalid/login'
        with self.assertRaisesRegex(Exception, 'session_snapshot_failed'):
            snapshot_coinglass(context, page)
        self.assertEqual(context.reads, 0)
        with patch('session_capture.os.write', side_effect=BrokenPipeError):
            with self.assertRaises(StageFailure) as caught:
                session.finish(context)
        self.assertEqual(caught.exception.code, 'session_transport_failed')
        self.assertEqual(session.state, {})

    def test_absent_same_site_stays_absent(self):
        cookie = {k: v for k, v in COOKIE.items() if k != 'sameSite'}
        snapshot = snapshot_coinglass(Context([cookie]), Page())
        self.assertEqual(snapshot['cookies'], [cookie])


class BuildIntegrationTests(unittest.TestCase):
    def test_managed_presence_configuration_matches_parent_flag_case(self):
        import app
        for flag in ('true','TRUE','True'):
            with patch.dict(os.environ,{'COINGLASS_SESSION_STORE_ENABLED':flag,
                                        'COINGLASS_SESSION_KEY':'DUMMY_PARENT_KEY'},clear=True):
                self.assertTrue(app.source_session_configured())
        with patch.dict(os.environ,{'COINGLASS_SESSION_STORE_ENABLED':'false'},clear=True):
            self.assertFalse(app.source_session_configured())

    def test_actual_generated_bridge_orders_parent_state_around_source_cleanup(self):
        import modern_map_checkpoint, prepare, symbol_jobs
        from heatmap_models import child_environment
        from session_runtime import SessionError
        with tempfile.TemporaryDirectory(prefix='session-bridge-build-') as temporary:
            runtime=Path(temporary)/'runtime'
            with (patch.object(prepare,'RUNTIME',runtime),patch.object(prepare,'prepare_browser'),
                  patch.object(prepare.subprocess,'run'),redirect_stdout(io.StringIO())):
                prepare.prepare()
            tree=ast.parse((runtime/'collection_bridge.py').read_text())
            actual=[n for n in tree.body if isinstance(n,ast.ClassDef) and n.name in ('BridgeError','JobStore')
                    or isinstance(n,ast.AsyncFunctionDef) and n.name=='run_existing_scanner']
            scope={'asyncio':asyncio,'tempfile':tempfile,'sys':sys,'Path':Path,'Any':Any,
                   'modern_map_checkpoint':modern_map_checkpoint,'symbol_jobs':symbol_jobs,
                   'child_environment':child_environment,'__file__':str(runtime/'collection_bridge.py')}
            exec(compile(ast.Module(body=actual,type_ignores=[]),'<actual-generated-bridge>','exec'),scope)
            async def scenario(*, managed=True, spawn_error=False, finish_error=False):
                events=[];spawn_env={};source_temp=[]
                class Process:
                    returncode=None
                    terminated=False
                    async def wait(self):
                        if not self.terminated:
                            events.append('source_wait_failure')
                            # Fail at the source-process boundary before the
                            # existing DB evidence writer. No store is mocked.
                            raise RuntimeError('controlled_source_wait_failure')
                        self.returncode=-15;events.append('process_stopped');return self.returncode
                    def terminate(self):self.terminated=True;events.append('terminate')
                    def kill(self):self.terminated=True;events.append('kill')
                process=Process()
                class Session:
                    async def begin(self):events.append('begin')
                    async def open_transport(self,environment):
                        events.append('open');self_test.assertNotIn('COINGLASS_SESSION_KEY',environment)
                        return SimpleNamespace(env=dict(environment),pass_fds=(123,),spawned=lambda:events.append('spawned'))
                    async def finish(self,transport):
                        events.append('finish')
                        self_test.assertTrue(spawn_error or process.returncode is not None)
                        self_test.assertIsNotNone(transport)
                        if finish_error:raise SessionError('session_store_unavailable')
                self_test=self
                async def spawn(*command,**kwargs):
                    events.append('spawn');spawn_env.update(kwargs['env']);source_temp.append(command[-1])
                    self.assertEqual(kwargs['pass_fds'],(123,) if managed else ())
                    if spawn_error:raise OSError('controlled_spawn_failure')
                    return process
                with (patch.dict(os.environ,{'COINGLASS_SESSION_KEY':'DUMMY_PARENT_ONLY_KEY',
                                                  'COINGLASS_MODERN_UI_ENABLED':'false'}),
                      patch.object(asyncio,'create_subprocess_exec',spawn)):
                    try:await scope['run_existing_scanner']('12H',RUN_ID,1,session=Session() if managed else None)
                    except (RuntimeError,OSError) as error:
                        self.assertFalse(finish_error)
                    except scope['BridgeError'] as error:
                        self.assertTrue(finish_error);self.assertEqual(error.code,'session_store_unavailable')
                    else:self.fail('controlled source failure must not be success')
                self.assertNotIn('COINGLASS_SESSION_KEY',spawn_env)
                self.assertTrue(source_temp and not Path(source_temp[0]).exists())
                if managed:
                    self.assertEqual(events[:3],['begin','open','spawn'])
                    self.assertEqual(events[-1],'finish')
                else:self.assertNotIn('begin',events);self.assertNotIn('finish',events)
                if spawn_error:self.assertNotIn('spawned',events);self.assertNotIn('terminate',events)
                else:
                    if managed:self.assertLess(events.index('spawned'),events.index('source_wait_failure'))
                    self.assertLess(events.index('terminate'),events.index('process_stopped'))
                    if managed:self.assertLess(events.index('process_stopped'),events.index('finish'))
            async def cases():
                await scenario()
                await scenario(spawn_error=True)
                await scenario(managed=False)
                await scenario(finish_error=True)
            asyncio.run(cases())

    def test_generated_lifecycle_keeps_cleanup_order_when_hover_or_save_fails(self):
        from install_capture_readiness import expand_session_capture
        source = '''def capture_heatmaps():
    with sync_playwright():
        storage_state = _load_authenticated_storage_state()
        cookies = _load_cookie_header()
        context = make_context(storage_state, cookies)
        page = context.new_page()
        try:
            original_work()
        except Exception as exc:
            observe_capture_failure(page, 1, None, exc)
            raise
        finally:
            try:
                with capture_operation('context_close'):
                    context.close()
            finally:
                with capture_operation('browser_close'):
                    browser.close()
'''
        generated=ast.parse(expand_session_capture(source))
        # Execute the transformed function itself with explicit fake service
        # objects; this exercises ordering, not browser/site compatibility.
        function=next(n for n in generated.body if isinstance(n,ast.FunctionDef))
        for hover_fails, save_fails in ((False,False),(True,False),(True,True)):
            with self.subTest(hover=hover_fails,save=save_fails):
                events=[]
                class Session:
                    state=STATE
                    finished=False
                    @classmethod
                    def from_environment(cls):events.append('load');return cls()
                    def attach(self, context, page):events.append('attach')
                    def note_source_error(self,error):events.append('error:'+error.code)
                    def finish(self,context):
                        if self.finished:return
                        self.finished=True;events.append('session_finish')
                        if save_fails:raise StageFailure('session_transport_failed')
                def legacy_loader():raise AssertionError('legacy_state_used')
                def work():
                    events.append('work')
                    if hover_fails:raise StageFailure('source_capture_failed')
                context=SimpleNamespace(new_page=lambda:object(),close=lambda:events.append('context_close'))
                scope={'CaptureSession':Session,'sync_playwright':nullcontext,'capture_operation':lambda phase:nullcontext(),
                    '_load_authenticated_storage_state':legacy_loader,'_load_cookie_header':legacy_loader,
                    'make_context':lambda state,cookies:context,'original_work':work,
                    'observe_capture_failure':lambda *args:events.append('failure_observer'),
                    'browser':SimpleNamespace(close=lambda:events.append('browser_close'))}
                exec(compile(ast.Module(body=[function],type_ignores=[]),'<generated-session-order>','exec'),scope)
                try:scope['capture_heatmaps']()
                except StageFailure:
                    self.assertTrue(hover_fails or save_fails)
                self.assertLess(events.index('attach'),events.index('work'))
                self.assertLess(events.index('session_finish'),events.index('context_close'))
                self.assertLess(events.index('context_close'),events.index('browser_close'))
                if hover_fails:self.assertLess(events.index('error:source_capture_failed'),events.index('session_finish'))
                self.assertEqual(events.count('session_finish'),1)

    def assert_generated_child_descriptors(self,bridge_text,runtime):
        from modern_map_checkpoint import CheckpointChannel, FD
        fn, = [n for n in ast.parse(bridge_text).body if isinstance(n,ast.AsyncFunctionDef)
               and n.name=='run_existing_scanner']
        launch, = [n for n in ast.walk(fn) if isinstance(n,ast.Call)
                   and ast.unparse(n.func)=='asyncio.create_subprocess_exec']
        pass_fds, = [k.value for k in launch.keywords if k.arg=='pass_fds']
        block, = [node.body for node in ast.walk(fn) if isinstance(node,ast.Try) and
                  any(isinstance(stmt,ast.Assign) and launch in ast.walk(stmt) for stmt in node.body)]
        launch_at = next(i for i,stmt in enumerate(block) if isinstance(stmt,ast.Assign) and launch in ast.walk(stmt))
        preparation = compile(ast.Module(body=block[launch_at-2:launch_at],type_ignores=[]),'<fd-preparation>','exec')
        inherited = compile(ast.Expression(pass_fds),'<fd-inheritance>','eval')
        with ExitStack() as resources:
            session_fds = os.pipe()
            for fd in session_fds:
                resources.callback(os.close,fd)
            channel = CheckpointChannel(None,runtime)
            resources.callback(channel.channel.close)
            resources.callback(channel.child.close)
            for transport in (None,SimpleNamespace(pass_fds=session_fds)):
                for market in (None,channel):
                    with self.subTest(managed=bool(transport),market=bool(market)):
                        env = {}
                        state = dict(_session_transport=transport,_market_channel=market,_session_child_env=env)
                        exec(preparation,state)
                        expected = session_fds if transport else ()
                        if market:
                            expected += (channel.child.fileno(),)
                            self.assertEqual(env[FD],str(channel.child.fileno()))
                        else:
                            self.assertNotIn(FD,env)
                        self.assertEqual(eval(inherited,state),expected)

    def test_complete_runtime_generation_uses_real_adapters_without_install_or_source(self):
        import prepare
        root=Path(__file__).resolve().parent.parent
        native=(root/'market_vision/coinglass_heatmap_capture.py').read_bytes()
        bridge=(root/'collection_bridge.py').read_bytes()
        with tempfile.TemporaryDirectory(prefix='session-build-') as temporary:
            runtime=Path(temporary)/'runtime'
            with (patch.object(prepare,'RUNTIME',runtime), patch.object(prepare,'prepare_browser') as browser_prepare,
                    patch.object(prepare.subprocess,'run') as commands, redirect_stdout(io.StringIO())):
                prepare.prepare()
            browser_prepare.assert_called_once()
            self.assertTrue(all(call.args[0][1:3]==['-m','unittest'] for call in commands.call_args_list))
            capture_text=(runtime/'market_vision/coinglass_heatmap_capture.py').read_text()
            bridge_text=(runtime/'collection_bridge.py').read_text()
            for name in ('session_capture.py','session_state.py','session_runtime.py','modern_map_checkpoint.py'):
                self.assertEqual((runtime/name).read_bytes(),(root/'standalone_collector'/name).read_bytes())
            capture_tree=ast.parse(capture_text)
            function=next(node for node in capture_tree.body if isinstance(node,ast.FunctionDef) and node.name=='capture_heatmaps')
            calls=[ast.unparse(node.func) for node in ast.walk(function) if isinstance(node,ast.Call)]
            self.assertEqual(calls.count('CaptureSession.from_environment'),1)
            self.assertEqual(calls.count('_capture_session.attach'),1)
            self.assertEqual(calls.count('_capture_session.finish'),2) # before-close + idempotent fallback
            self.assertIn('require_unblocked_source',calls)
            self.assertIn('capture_column',calls)
            self.assertEqual(calls.count('page.screenshot'),1)
            self.assertIn("if _capture_session else _load_authenticated_storage_state()",capture_text)
            self.assertIn("[] if _capture_session else _load_cookie_header()",capture_text)
            self.assertIn("_session_child_env.pop('COINGLASS_SESSION_KEY', None)",bridge_text)
            self.assertIn('await session.finish(_session_transport)',bridge_text)
            self.assert_generated_child_descriptors(bridge_text,runtime)
            self.assertEqual(prepare.BROWSER_REQUIREMENTS['pycryptodome'],'3.21.0')
            compile(capture_text,'<generated-capture>','exec');compile(bridge_text,'<generated-bridge>','exec')
        self.assertEqual((root/'market_vision/coinglass_heatmap_capture.py').read_bytes(),native)
        self.assertEqual((root/'collection_bridge.py').read_bytes(),bridge)


if __name__ == '__main__':unittest.main(verbosity=2)
