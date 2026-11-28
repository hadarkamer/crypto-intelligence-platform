"""Postgres and loopback HTTP regression for both wire contracts. No source/provider access."""
import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
from collection_bridge import JobStore, register_collection_routes
import scoped_http
import scoped_worker


class ProtocolHTTP(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        cluster=Path(self.temp.name)/'db'
        initdb, pgctl=shutil.which('initdb'),shutil.which('pg_ctl')
        if not initdb or not pgctl: self.skipTest('Local PostgreSQL binaries required')
        password=uuid4().hex
        pwfile=Path(self.temp.name)/'password'
        pwfile.write_text(password)
        subprocess.run([initdb,'-D',str(cluster),'-A','scram-sha-256','-U','test','--pwfile',str(pwfile)],
                       check=True,capture_output=True)
        pwfile.unlink()
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as sock:
            sock.bind(('127.0.0.1',0))
            port=sock.getsockname()[1]
        subprocess.run([pgctl,'-D',str(cluster),'-l',str(Path(self.temp.name)/'pg.log'),'-o',
                        f"-c unix_socket_directories='' -c listen_addresses='127.0.0.1' -p {port}",'-w','start'],
                       check=True,capture_output=True)
        self.addCleanup(subprocess.run,[pgctl,'-D',str(cluster),'-m','immediate','-w','stop'],check=True,capture_output=True)
        self.store=JobStore(f'host=127.0.0.1 port={port} user=test password={password} dbname=postgres')
        self.token = 'x' * 40
        self.auth = {'Authorization':'Bearer ' + self.token}
        self.ready = patch.object(scoped_worker,'available',return_value=True).start()
        self.gate = patch.object(scoped_worker,'account_status',new=AsyncMock(return_value={'code':'ready'})).start()
        self.wake = patch.object(scoped_worker,'wake').start()
        self.addCleanup(patch.stopall)
        # Start from the deployed three-range DDL, including its old global index.
        with self.store.connect() as c:
            c.execute("""CREATE TABLE public.ai_collection_bridge_jobs (
                id uuid PRIMARY KEY,timeframe text NOT NULL CHECK(timeframe IN ('12H','24H','48H')),
                heatmap_model integer NOT NULL DEFAULT 1 CHECK(heatmap_model IN (1,2,3)),
                status text NOT NULL CHECK(status IN ('queued','running','ready','failed')),
                created_at timestamptz NOT NULL DEFAULT now(),started_at timestamptz,
                finished_at timestamptz,result jsonb,image_png bytea,error_code text)""")
            c.execute("""CREATE UNIQUE INDEX ai_collection_bridge_one_active_model_tf ON
                        public.ai_collection_bridge_jobs(heatmap_model,timeframe) WHERE status IN ('queued','running')""")
        self.store.initialize()
        app = web.Application(client_max_size=2048)
        register_collection_routes(app,store=self.store,token=self.token,enabled=True,start_worker=False)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        self.addAsyncCleanup(self.client.close)
        self.url='/api/collection/model1/jobs'
        self.cycle=str(uuid4())

    def fixed(self,symbol,tf):
        return {'request_id':str(uuid4()),'fixed_cycle_id':self.cycle,'timeframe':tf,'symbol':symbol,'attempt':1}

    async def post(self,body,expected=202):
        response=await self.client.post(self.url,json=body,headers=self.auth)
        result=await response.json()
        self.assertEqual(response.status,expected,result)
        return result

    async def test_routes_queue_and_restarts(self):
        legacy={'request_id':str(uuid4()),'timeframe':'12H'}
        old=await self.post(legacy)
        self.assertNotIn('symbol',old)
        self.assertEqual(await self.post(legacy),old)
        btc,eth=self.fixed('BTC','12H'),self.fixed('ETH','12H')
        new=await self.post(btc)
        other=await self.post(eth)
        week=await self.post(self.fixed('BTC','1W'))
        self.assertEqual(week['timeframe'],'1W')
        r=await self.post({'request_id':str(uuid4()),'timeframe':'1W'},400)
        self.assertEqual(r['error']['code'],'unsupported_timeframe')
        self.assertEqual((await self.post(dict(btc,request_id=legacy['request_id']),409))['error']['code'],'idempotency_conflict')
        self.assertEqual((await self.post(dict(legacy,request_id=btc['request_id']),409))['error']['code'],'idempotency_conflict')
        self.assertEqual((await self.post(self.fixed('ETH','12H'),409))['error']['code'],'idempotency_conflict')
        self.assertEqual(await self.post(btc),new)
        self.store.initialize()
        owner,job=self.store.claim()
        self.assertEqual(str(job['id']),old['job_id'])
        self.store.finish(job['id'],None,None,'test_done')
        owner.close()
        self.assertIsNone(self.store.claim())
        result={'source':'local-test-only','captured_at':datetime.now(timezone.utc).isoformat()}
        from psycopg.types.json import Jsonb
        with self.store.connect() as c:
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET status='ready',result=%s,image_png=%s
                         WHERE id=%s""",(Jsonb(result),b'png',new['job_id']))
        exact=await self.client.get(self.url+'/'+new['job_id'],headers=self.auth)
        self.assertEqual((await exact.json())['result'],result)
        for name in ('','/evidence'):
            wrong=await self.client.get(self.url+'/'+new['job_id']+name+'?symbol=ETH',headers=self.auth)
            self.assertEqual(wrong.status,404)
        image=await self.client.get(self.url+'/'+new['job_id']+'/evidence',headers=self.auth)
        self.assertEqual(await image.read(),b'png')
        eth_url=self.url+'/'+other['job_id']
        self.assertEqual((await self.client.get(eth_url,headers=self.auth)).status,404)
        self.assertEqual((await self.client.get(eth_url+'?symbol=ETH',headers=self.auth)).status,200)
        self.assertEqual((await self.client.get(self.url+'/latest?timeframe=12H',headers=self.auth)).status,404)
        self.assertEqual((await self.client.get(self.url+'/latest?timeframe=12H&symbol=ETH',headers=self.auth)).status,404)

    async def test_missing_receipt_table_does_not_erase_the_two_launch_budget(self):
        from numeric_inputs import scoped_dispatch as dispatch
        head=await self.post(self.fixed('BTC','12H'))
        first=dispatch.reserve(self.store)
        self.assertEqual(first[:2],(head['job_id'],self.cycle))
        # PostgreSQL is still able to persist scope and job state while the evidence relation is unavailable.
        with self.store.connect() as c:
            c.execute('ALTER TABLE public.ai_collection_bridge_evidence RENAME TO fixture_hidden_evidence')
        reason={'returncode':1,'reason':'database_error','sqlstate':'42P01','last_completed_stage':'owner_acquired'}
        dispatch.settle(self.store,first,reason)
        self.assertIsNone(dispatch.reserve(self.store))
        await self.post(self.fixed('ETH','24H'))
        with self.store.connect() as c:
            c.execute("UPDATE public.ai_collection_bridge_jobs SET fixed_worker_guard=fixed_worker_guard||'{\"due\":0}'::jsonb WHERE id=%s",(head['job_id'],))
        second=dispatch.reserve(self.store)
        self.assertEqual(first[:2],second[:2])
        self.assertNotEqual(first[2],second[2])
        dispatch.settle(self.store,second,reason)
        self.assertFalse(scoped_worker.pending(self.store))
        with self.store.connect() as c:
            g=c.execute("SELECT fixed_worker_guard AS g FROM public.ai_collection_bridge_jobs WHERE id=%s",(head['job_id'],)).fetchone()['g']
            self.assertEqual((g['state'],g['attempts'],g['last']),('stopped',2,reason))
            self.assertFalse(dispatch.resume(c,str(uuid4())))
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET fixed_worker_guard=jsonb_set(fixed_worker_guard,
                '{stopped_at}',to_jsonb(extract(epoch FROM clock_timestamp())-4000)) WHERE id=%s""",(head['job_id'],))
            self.assertTrue(dispatch.resume(c,self.cycle))
            self.assertFalse(dispatch.resume(c,self.cycle))
            credit=c.execute("SELECT (fixed_worker_guard->>'paused_seconds')::numeric AS v FROM public.ai_collection_bridge_jobs WHERE id=%s",(head['job_id'],)).fetchone()['v']
            self.assertGreaterEqual(credit,4000)
        self.assertTrue(scoped_worker.pending(self.store))
        with self.store.connect() as c:
            c.execute('ALTER TABLE public.fixture_hidden_evidence RENAME TO ai_collection_bridge_evidence')

    async def test_stopped_scope_keeps_target_idempotence_and_releases_independent_heads(self):
        import json
        from numeric_inputs import scoped_dispatch as dispatch
        first_body=self.fixed('BTC','12H')
        old=await self.post(first_body)
        with self.store.connect() as c:
            c.execute("UPDATE public.ai_collection_bridge_jobs SET fixed_worker_guard=%s::jsonb WHERE id=%s",
                      (json.dumps({'state':'stopped','attempts':2,'stopped_at':1}),old['job_id']))
        self.assertEqual((await self.post(first_body))['job_id'],old['job_id'])
        self.assertEqual((await self.post(self.fixed('BTC','12H'),409))['error']['code'],'idempotency_conflict')
        self.cycle=str(uuid4())
        allowed=await self.post(self.fixed('BTC','12H'))
        self.assertNotEqual(old['job_id'],allowed['job_id'])
        eligible=dispatch.reserve(self.store)
        self.assertEqual(eligible[:2],(allowed['job_id'],self.cycle))
        self.cycle=str(uuid4())
        self.assertEqual((await self.post(self.fixed('BTC','12H'),409))['error']['code'],'map_in_progress')

    async def test_admission_requires_auth_and_canonical_ready(self):
        body=self.fixed('BTC','12H')
        response=await self.client.post(self.url,json=body)
        self.assertEqual(response.status,401)
        self.gate.assert_not_awaited()
        self.gate.return_value={'code':'source_rate_limited','retry_after_seconds':7200}
        denied=(await self.post(body,429))['error']
        self.assertEqual(denied['code'],'source_rate_limited')
        self.assertEqual(denied['retry_after_seconds'],7200)
        self.wake.assert_not_called()
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM public.ai_collection_bridge_jobs').fetchone()['n'],0)
        self.gate.return_value={'code':'ready'}
        admitted=await self.post(body)
        self.gate.return_value={'code':'source_login_required'}
        self.assertEqual(await self.post(body),admitted)
        self.assertEqual(self.wake.call_count,1)
        self.assertEqual((await self.post(self.fixed('ETH','12H'),409))['error']['code'],'source_login_required')

    async def test_health_split(self):
        # Test both health routes with an isolated store and no source workers running.
        import app
        values={'DATABASE_URL':self.store.dsn,'COINGLASS_COOKIE_HEADER':'synthetic=fixture',
                'OPENAI_API_KEY':'not-a-key','COLLECTION_BRIDGE_ENABLED':'true',
                'COINGLASS_COLLECTOR_TOKEN':self.token}
        async def dormant(_):
            await asyncio.sleep(60)
        with patch.dict(os.environ,values), patch.object(app,'run_worker',dormant),patch.object(scoped_worker,'run',dormant):
            async with TestClient(TestServer(app.create_app())) as client:
                normal=await (await client.get('/health')).json()
                self.assertEqual(normal['numeric_reader'],'displayed-hover.v1')
                self.assertEqual(normal['selection_policy'],'prominent_right_edge.v1')
                self.assertEqual(normal['supported_timeframes'],['12H','24H','48H'])
                self.assertNotIn('fixed_cycle_id_supported',normal)
                self.assertEqual((await client.get('/health?fixed_cycle=btc-eth')).status,401)
                url='/health?fixed_cycle=btc-eth'
                enabled=await (await client.get(url,headers=self.auth)).json()
                self.assertTrue(enabled['fixed_cycle_id_supported'])
                self.assertEqual(enabled['basic_profiles']['symbols'],['BTC','ETH'])
                self.gate.return_value={'code':'source_login_required'}
                self.assertFalse((await (await client.get(url,headers=self.auth)).json())['fixed_cycle_id_supported'])
                self.gate.return_value={'code':'map_in_progress','active_fixed_cycle':True}
                self.assertTrue((await (await client.get(url,headers=self.auth)).json())['fixed_cycle_id_supported'])
                self.gate.return_value={'code':'map_in_progress','active_fixed_cycle':False}
                self.assertFalse((await (await client.get(url,headers=self.auth)).json())['fixed_cycle_id_supported'])
                self.gate.return_value={'code':'ready'}
                self.ready.return_value=False
                disabled=await (await client.get(url,headers=self.auth)).json()
                self.assertFalse(disabled['ok'])
                self.assertFalse(disabled['fixed_cycle_id_supported'])
