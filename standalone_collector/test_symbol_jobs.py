"""Final generated bridge acceptance, with an ephemeral local PostgreSQL when available."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parent/'runtime'))
import collection_bridge as bridge
import symbol_jobs


class IdentityTests(unittest.TestCase):
    def test_allowlist_requires_exact_provider_symbols(self):
        rid = str(uuid4())
        with patch.dict(os.environ, {'COINGLASS_VERIFIED_SYMBOLS':'BTC'}):
            self.assertEqual(symbol_jobs.request_dimensions({'request_id':rid, 'timeframe':'12H'}), (rid,'12H'))
            for value in ('DOG', 'ZCASH', 'btc', [], None):
                with self.subTest(value=value), self.assertRaises(bridge.BridgeError):
                    symbol_jobs.request_dimensions({'request_id':rid, 'timeframe':'12H', 'symbol':value})
            for value in (True, 0, 4, '2'):
                with self.assertRaises(bridge.BridgeError):
                    symbol_jobs.request_dimensions({'request_id':rid, 'timeframe':'12H', 'attempt':value})

    def test_modern_provenance_and_artifact_guard(self):
        image = b'isolated sample'
        digest = hashlib.sha256(image).hexdigest()
        jid = str(uuid4())
        at = '2026-11-24T09:00:00Z'
        url = 'https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'
        snapshot = dict(schema=symbol_jobs.MEASURE_SCHEMA, source_url=url, symbol='BTC', heatmap_model=1,
                        timeframe='12H', captured_at=at, artifact_id=jid, sha256=digest, source_time=at,
                        source_time_after=at,column_time=at,column_time_after=at,mode='Coin',lens='Heat',
                        palette='Viridis',theme='dark',intensity=[0,100],count='Right edge')
        result = dict(schema_version='coinglass-model1.v1', symbol='BTC', heatmap_model=1, timeframe='12H',
                      captured_at=at, source_updated_at=at, source_url=url, run_id=jid,
                      selection_policy='prominent_right_edge.v1', price_reference_type='last_price_label',
                      level_contract_version='prominent-rows.v1',observed_price=81000,
                      evidence=dict(sha256=digest,artifact_id=jid,content_type='image/png'),
                      measure_snapshot=snapshot)
        valid = lambda obj: symbol_jobs.valid_result(obj,image,jid,'12H',1,'BTC',True)
        self.assertTrue(valid(result))
        for key, value in (('source_url',url+'&foo'),('symbol','X'),('heatmap_model',True),
                           ('source_updated_at',None),('hover_snapshot',{}),('run_id',str(uuid4())),
                           ('evidence',dict(result['evidence'],sha256='different'))):
            with self.subTest(key=key): self.assertFalse(valid(dict(result,**{key:value})))
        for key in snapshot:
            with self.subTest(snapshot_key=key):
                self.assertFalse(valid(dict(result,measure_snapshot=dict(snapshot,**{key:None}))))
        self.assertFalse(symbol_jobs.valid_result(result,b'wrong',jid,'12H',1,'BTC',True))
        self.assertFalse(symbol_jobs.valid_result(result,image,jid,'12H',1,'BTC',False))


class Routes(unittest.IsolatedAsyncioTestCase):
    async def test_health_reports_mode_specific_allowlist_without_starting_source(self):
        import app as service
        for modern, declared, expected in ((True,'BTC,TESTCOIN',['BTC','TESTCOIN']),(False,'BTC,TESTCOIN',['BTC']),
                                         (True,'../broken',[]),(False,'TESTCOIN',[])):
            with patch.dict(os.environ,{'COINGLASS_VERIFIED_SYMBOLS':declared,'COINGLASS_MODERN_UI_ENABLED':str(modern).lower(),
                                       'COLLECTION_BRIDGE_ENABLED':'false'}):
                async with TestClient(TestServer(service.create_app())) as client:
                    health = await (await client.get('/health')).json()
            self.assertEqual(health['supported_symbols'],expected)
            self.assertEqual(health['ok'],bool(expected))
            self.assertEqual(health['numeric_reader'],symbol_jobs.MEASURE_SCHEMA if modern else 'displayed-hover.v1')
            self.assertEqual(health['capture_flow'],'coin-map-right-edge' if modern else 'original')
        with patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true','COINGLASS_SESSION_STORE_ENABLED':'false',
                                   'COINGLASS_COOKIE_HEADER':'offline-placeholder'}):
            self.assertFalse(service.source_session_configured())

    async def test_normal_handlers_propagate_verified_job_dimensions(self):
        from unittest.mock import Mock
        store = Mock()
        jid, rid = str(uuid4()), str(uuid4())
        store.dsn = 'unused'
        store.start.return_value = {'status':'queued', 'symbol':'BTC','attempt':2}
        store.get.return_value = store.latest.return_value = {'status':'ready','symbol':'BTC'}
        store.evidence.return_value = b'PNG'
        token = 'offline-bridge-authentication-fixture'
        app = web.Application()
        bridge.register_collection_routes(app, store=store, token=token, enabled=True, start_worker=False)
        headers = {'Authorization':'Bearer '+token}
        async with TestClient(TestServer(app), headers=headers) as client:
            url = '/api/collection/model1/jobs'
            response = await client.post(url,json={'request_id':rid,'symbol':'BTC','timeframe':'12H','attempt':2})
            self.assertEqual(response.status,202)
            store.start.assert_called_once_with(rid,'12H','BTC',2)
            for route, method, args in ((url+'/latest?timeframe=12H&symbol=BTC','latest',('12H','BTC')),
                                       (url+'/'+jid+'?symbol=BTC','get',(jid,'BTC')),
                                       (url+'/'+jid+'/evidence?symbol=BTC','evidence',(jid,'BTC'))):
                self.assertEqual((await client.get(route)).status,200)
                getattr(store,method).assert_called_once_with(*args)
            self.assertEqual((await client.get(url+'/'+jid+'?symbol=UNCONFIGURED')).status,400)


class LedgerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import psycopg
        except ImportError:
            raise unittest.SkipTest('psycopg is unavailable')
        initdb, ctl = shutil.which('initdb'), shutil.which('pg_ctl')
        if not (initdb and ctl) or os.getuid() == 0:
            raise unittest.SkipTest('Local unprivileged PostgreSQL binaries are required')
        temp = tempfile.TemporaryDirectory(prefix='coin-job-ledger-')
        cls.addClassCleanup(temp.cleanup)
        cluster = Path(temp.name)/'db'
        secret = uuid4().hex
        pwfile = Path(temp.name)/'password'
        pwfile.write_text(secret)
        subprocess.run([initdb,'-D',str(cluster),'-A','scram-sha-256','-U','test','--pwfile',str(pwfile)],
                       check=True,capture_output=True)
        pwfile.unlink()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0))
            port = sock.getsockname()[1]
        subprocess.run([ctl,'-D',str(cluster),'-l',str(Path(temp.name)/'log'),'-o',
                        f"-c unix_socket_directories='' -c listen_addresses='127.0.0.1' -p {port}" ,'-w','start'],
                       check=True,capture_output=True)
        cls.addClassCleanup(subprocess.run,[ctl,'-D',str(cluster),'-m','immediate','-w','stop'],
                            check=True,capture_output=True)
        cls.store = bridge.JobStore(f'host=127.0.0.1 port={port} user=test password={secret} dbname=postgres')
        cls.old_id = str(uuid4())
        with cls.store.connect() as conn:
            conn.execute("""CREATE TABLE public.ai_collection_bridge_jobs (
                id uuid PRIMARY KEY, timeframe text NOT NULL CHECK(timeframe IN ('12H','24H','48H')),
                status text NOT NULL CHECK(status IN ('queued','running','ready','failed')),
                created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz,finished_at timestamptz,
                result jsonb,image_png bytea,error_code text)""")
            conn.execute("INSERT INTO public.ai_collection_bridge_jobs(id,timeframe,status) VALUES(%s,'12H','failed')",(cls.old_id,))
            conn.execute("CREATE SCHEMA collector_private")
            conn.execute(Path(__file__).with_name("operator_login.sql").read_text())
        cls.store.initialize()

    def test_login_retry_requires_a_verified_incident_resolution(self):
        rid, incident = str(uuid4()), str(uuid4())
        store = self.store
        with patch.dict(os.environ,{'COINGLASS_SESSION_STORE_ENABLED':'true'}):
            job = store.start(rid,'12H','BTC',1)
            jid = job['job_id']
            with store.connect() as conn:
                conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='failed',error_code='source_login_required' WHERE id=%s",(jid,))
                conn.execute("INSERT INTO collector_private.coinglass_login_incidents(id) VALUES(%s)",(incident,))
                conn.execute("INSERT INTO collector_private.coinglass_login_jobs VALUES (%s,%s,'BTC',1,'12H')",(jid,incident))
            with self.assertRaises(bridge.BridgeError) as err:
                store.start(rid,'12H','BTC',2)
            self.assertEqual(err.exception.code,'login_pending')
            self.assertEqual(store.get(jid)['attempt'],1)
            with store.connect() as conn:
                conn.execute("UPDATE collector_private.coinglass_login_incidents SET resolved_at=now(),resolved_revision=1 WHERE id=%s",(incident,))
            self.assertEqual(store.start(rid,'12H','BTC',2)['attempt'],2)

    def test_cycle_isolation_partial_completion_and_retry(self):
        store = self.store
        with patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true','COINGLASS_VERIFIED_SYMBOLS':'BTC,TESTCOIN'}):
            rid = str(uuid4())
            def start():
                return store.start(rid,'24H','BTC',1)
            with ThreadPoolExecutor(max_workers=3) as pool:
                jobs = list(pool.map(lambda _:start(),range(3)))
            jid = jobs[0]['job_id']
            self.assertTrue(all(job['job_id']==jid and job['request_id']==rid for job in jobs))
            for tf in ('12H','48H'):
                other = store.start(str(uuid4()),tf,'BTC',1)
                with store.connect() as conn:
                    conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='running' WHERE id=%s",(other['job_id'],))
                store.finish(other['job_id'],{'captured_at':'2026-01-01T00:00:00Z'},b'PNG')
                self.assertEqual(store.get(other['job_id'],'BTC')['status'],'ready')
            another = store.start(str(uuid4()),'24H','BTC',1)
            isolated = store.start(str(uuid4()),'24H','TESTCOIN',1)
            self.assertNotEqual(jid,another['job_id'])
            self.assertIsNone(store.get(isolated['job_id'],'BTC'))
            with self.assertRaisesRegex(bridge.BridgeError,'another'):
                store.start(rid,'24H','TESTCOIN',1)
            # A restart with multiple queued jobs of the same map must keep the index valid.
            store.initialize()
            with store.connect() as conn:
                conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='running' WHERE id=%s",(jid,))
            store.finish(jid,None,None,'source_timeout')
            with self.assertRaises(bridge.BridgeError):
                store.start(rid,'24H','BTC',3)
            def retry(_):
                return store.start(rid,'24H','BTC',2)
            with ThreadPoolExecutor(max_workers=2) as pool:
                attempts = list(pool.map(retry,range(2)))
            self.assertTrue(all(a['job_id']==jid and a['attempt']==2 and a['status']=='queued' for a in attempts))
            self.assertEqual(start()['attempt'],2)
            with store.connect() as conn:
                conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='running' WHERE id=%s",(jid,))
                captured = conn.execute('SELECT now() AS ts').fetchone()['ts'].isoformat()
            store.finish(jid,{'captured_at':captured},b'PNG')
            self.assertEqual(store.latest('24H','BTC')['job_id'],jid)
            self.assertIsNone(store.latest('24H','TESTCOIN'))
            self.assertEqual(store.evidence(jid,'BTC'),b'PNG')
            self.assertIsNone(store.evidence(jid,'TESTCOIN'))
            self.assertEqual(store.start(rid,'24H','BTC',3)['status'],'ready')

    def test_migrated_rows_default_to_btc_and_only_explicit_work_retries(self):
        rid = str(uuid4())
        self.assertEqual(self.store.get(self.old_id)['symbol'],'BTC')
        first = self.store.start(rid,'48H')
        shared = self.store.start(str(uuid4()),'48H')
        self.assertEqual(first['job_id'],shared['job_id'])
        self.assertEqual((first['symbol'],first['attempt'],first['request_id']),('BTC',1,None))
        with self.store.connect() as conn:
            conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='failed',error_code='source_timeout' WHERE id=%s",(first['job_id'],))
        with self.assertRaises(bridge.BridgeError):
            self.store.start(rid,'48H','BTC',2)
