"""Exercise the actual generated scanner timeout, with no subprocess, DB or source calls."""
import ast
import asyncio
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
from heatmap_models import child_environment,schema_version,source_url

PNG=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jR3sAAAAASUVORK5CYII=')


def scanner_function(store):
    # Compile precisely the installed function, omitting unrelated HTTP imports.
    source=(HERE/'runtime/collection_bridge.py').read_text(encoding='utf-8')
    tree=ast.parse(source)
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))
        and n.name in ('BridgeError','run_existing_scanner')]
    scope={'asyncio':asyncio,'hashlib':hashlib,'json':json,'os':os,'Path':Path,
        'sys':sys,'tempfile':tempfile,'Any':object,'MAX_RESULT':64*1024,'MAX_IMAGE':4*1024*1024,
        'schema_version':schema_version,'source_url':source_url,'child_environment':child_environment,
        '__file__':str(HERE/'runtime/collection_bridge.py'),'JobStore':lambda *args,**kwargs:store}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'installed_timeout_scanner','exec'),scope)
    return scope['run_existing_scanner'],scope['BridgeError']


class TimeoutRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self,*,image=PNG,sidecar=True,kill=False,store_error=False,invalid_sidecar=False,progress=False):
        events=[];directories=[];saved=[];timeouts=[]
        class Process:
            returncode=None
            waits=0
            def terminate(self):events.append('terminate')
            def kill(self):events.append('kill')
            async def wait(self):
                self.waits+=1;events.append('wait')
                if self.waits==1 or (kill and self.waits==2):raise asyncio.TimeoutError('PRIVATE')
                self.returncode=-9 if kill else -15
                return self.returncode
        process=Process()
        class Store:
            def retain_evidence(inner,jid,shot,diagnostic):
                self.assertIsNotNone(process.returncode)
                self.assertTrue(Path(directories[0]).exists())
                events.append('retain')
                if store_error:raise RuntimeError('PRIVATE')
                saved.append((jid,shot,diagnostic))
        scanner,BridgeError=scanner_function(Store())
        async def launch(*args,**kwargs):
            root=Path(args[4]);directories.append(str(root));capture=root/'capture';capture.mkdir()
            if image is not None:(capture/'coinglass_btc_heatmap_12h.png').write_bytes(image)
            if progress:
                (root/'execution_progress.json').write_text(json.dumps({
                    'format_version':'execution-progress.v1','stage':'capture','capture_phase':'navigation',
                    'elapsed_seconds':299,'heatmap_model':3,'timeframe':'12H','token':'PRIVATE',
                    'capture_operations':[{'phase':'browser_launch','status':'completed','duration_seconds':1}]}))
            if sidecar:
                data={'source_readiness':{'ready':False,'reason':'loading-indicator','label':'12 hour',
                    'login_link_visible':True,'account_link_present':False,'cookie':'PRIVATE'},
                    'source_network':{'http_errors':[{'host_group':'coinglass','status':503,'count':1,'url':'PRIVATE'}],
                        'network_errors':[],'page_error_count':0},'token':'PRIVATE'}
                text='PRIVATE{broken' if invalid_sidecar else json.dumps(data)
                (capture/'coinglass_btc_heatmap_12h.render.json').write_text(text)
            # Even a result file cannot turn a timed-out run into an accepted result.
            (root/'result.json').write_text('{"ready":true,"token":"PRIVATE"}')
            return process
        real_wait=asyncio.wait_for
        async def wait(awaitable,*,timeout):
            timeouts.append(timeout)
            return await real_wait(awaitable,timeout=timeout)
        with patch.object(asyncio,'create_subprocess_exec',side_effect=launch),\
            patch.object(asyncio,'wait_for',side_effect=wait),\
            contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(BridgeError) as failure:await scanner('12H','job',3)
        self.assertEqual(failure.exception.code,'scan_timeout');self.assertEqual(failure.exception.status,504)
        self.assertEqual(timeouts,[300,5]);self.assertEqual(events.count('terminate'),1)
        self.assertEqual(events.count('kill'),1 if kill else 0)
        self.assertLess(events.index('terminate'),events.index('retain'))
        self.assertEqual(process.waits,3 if kill else 2)
        self.assertFalse(Path(directories[0]).exists())
        self.assertNotIn('PRIVATE',output.getvalue()+json.dumps([entry[2] for entry in saved]))
        return saved,output.getvalue()

    async def test_timeout_reaps_child_then_retains_available_png_and_safe_metadata(self):
        saved,output=await self.exercise()
        self.assertEqual(len(saved),1);jid,image,diagnostic=saved[0]
        self.assertEqual(image,PNG);self.assertEqual(diagnostic['failure_code'],'scan_timeout')
        self.assertEqual(diagnostic['timeout_seconds'],300);self.assertFalse(diagnostic['assessment_validated'])
        self.assertEqual(diagnostic['source_readiness']['reason'],'loading-indicator')
        self.assertEqual(diagnostic['source_network']['http_errors'][0]['status'],503)
        self.assertTrue(json.loads(output.split('MODEL1_EVIDENCE ')[1])['image_retained'])

    async def test_timeout_without_png_still_retains_failure_diagnostic(self):
        saved,output=await self.exercise(image=None,sidecar=False)
        self.assertIsNone(saved[0][1]);self.assertFalse(saved[0][2]['image_retained'])
        self.assertEqual(saved[0][2]['failure_code'],'scan_timeout')
        self.assertFalse(json.loads(output.split('MODEL1_EVIDENCE ')[1])['image_retained'])

    async def test_timeout_before_png_retains_active_stage_after_reaping_child(self):
        saved,output=await self.exercise(image=None,sidecar=False,progress=True)
        self.assertIsNone(saved[0][1]);diagnostic=saved[0][2]
        self.assertFalse(diagnostic['assessment_validated'])
        self.assertEqual(diagnostic['failure_code'],'scan_timeout')
        self.assertEqual(diagnostic['timeout_seconds'],300)
        progress=diagnostic['execution_progress']
        self.assertEqual((progress['stage'],progress['capture_phase']),('capture','navigation'))
        self.assertEqual((progress['heatmap_model'],progress['timeframe']),(3,'12H'))
        self.assertEqual(progress['capture_operations'][0]['phase'],'browser_launch')
        self.assertNotIn('PRIVATE',json.dumps(diagnostic)+output)

    async def test_uncooperative_child_is_killed_and_reaped_before_retention(self):await self.exercise(kill=True)

    async def test_retention_failure_never_replaces_original_timeout(self):
        saved,output=await self.exercise(store_error=True)
        self.assertEqual(saved,[])
        self.assertFalse(json.loads(output.split('MODEL1_EVIDENCE ')[1])['diagnostic_retained'])

    async def test_invalid_available_evidence_and_partial_sidecar_are_not_trusted(self):
        saved,output=await self.exercise(image=b'not-a-png',invalid_sidecar=True)
        self.assertIsNone(saved[0][1]);self.assertFalse(saved[0][2]['assessment_validated'])
        self.assertEqual(saved[0][2]['source_readiness'],{'ready':False,'reason':'unverified'})


if __name__=='__main__':unittest.main()
