"""Final generated bridge acceptance, with an ephemeral local PostgreSQL when available."""
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import json
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


MODERN_TEST_ENV = {
    'COINGLASS_MODERN_UI_ENABLED':'true','COINGLASS_VERIFIED_SYMBOLS':'BTC,ETH',
    'COINGLASS_SESSION_STORE_ENABLED':'true','COINGLASS_SESSION_KEY':base64.b64encode(bytes(32)).decode(),
    'COINGLASS_LOGIN_EMAIL':'ledger-fixture@example.invalid',
}


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
        for modern, declared, expected in ((True,'BTC,TESTCOIN',['BTC']),(False,'BTC,TESTCOIN',['BTC']),
                                         (True,'../broken',[]),(False,'TESTCOIN',[])):
            with patch.dict(os.environ,{'COINGLASS_VERIFIED_SYMBOLS':declared,'COINGLASS_MODERN_UI_ENABLED':str(modern).lower(),
                                       'COLLECTION_BRIDGE_ENABLED':'false'}):
                async with TestClient(TestServer(service.create_app())) as client:
                    health = await (await client.get('/health')).json()
            self.assertEqual(health['supported_symbols'],expected)
            self.assertEqual(health['ok'],bool(expected))
            self.assertEqual(health['numeric_reader'],'coinglass.basic.v1' if modern else 'displayed-hover.v1')
            self.assertEqual(health['capture_flow'],'coin-map-right-edge' if modern else 'original')
        with patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true','COINGLASS_SESSION_STORE_ENABLED':'false',
                                   'COINGLASS_COOKIE_HEADER':'offline-placeholder'}):
            self.assertFalse(service.source_session_configured())

    async def test_session_status_survives_retirement_of_viewer_in_both_modes(self):
        import app as service
        from unittest.mock import MagicMock
        from aiohttp.test_utils import make_mocked_request

        path = '/api/collection/coinglass/session-status'
        token = 'offline-bridge-authentication-fixture'
        for modern in ('false', 'true'):
            env = {'COINGLASS_MODERN_UI_ENABLED': modern, 'COINGLASS_OPERATOR_DESKTOP_ENABLED': 'true',
                   'COINGLASS_COLLECTOR_TOKEN': token, 'COINGLASS_SESSION_STORE_ENABLED': 'true'}
            with self.subTest(modern=modern), patch.dict(os.environ, env, clear=True):
                application = service.create_app()
                routes = {r.resource.canonical for r in application.router.routes()}
                self.assertIn(path, routes)
                self.assertNotIn('/api/collection/coinglass/session-window', routes)
                self.assertFalse(any(r.startswith('/coinglass/operator') for r in routes))
                async with TestClient(TestServer(application)) as client:
                    for method, retired in (('post','/api/collection/coinglass/session-window'),
                                            ('get','/coinglass/operator/display')):
                        self.assertEqual((await client.request(method, retired)).status, 404)
        from operator_login import register_legacy_status
        with patch.dict(os.environ, {'COINGLASS_COLLECTOR_TOKEN':token,
                                     'COINGLASS_SESSION_STORE_ENABLED':'true'}, clear=True):
            application = web.Application()
            register_legacy_status(application, MagicMock(), True)
            route = next(r.handler for r in application.router.routes() if r.method == 'GET')
            with self.assertRaises(web.HTTPUnauthorized):
                await route(make_mocked_request('GET', path))
            headers = {'Authorization': 'Bearer '+token}
            denied = await route(make_mocked_request('GET', path+'?job_id=bad', headers=headers))
            self.assertEqual(denied.status, 400)
            job, incident = str(uuid4()), str(uuid4())
            with patch('operator_login.incident_status', return_value={'status':'waiting','incident_id':incident}) as read:
                response = await route(make_mocked_request('GET', path+'?job_id='+job, headers=headers))
            self.assertEqual(read.call_args.args[1:], (None, job))
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.body), {'status':'waiting', 'incident_id':incident, 'can_launch':False})

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
            conn.execute(Path(__file__).with_name("dedicated_auth_state.sql").read_text())
        cls.store.initialize()

    def setUp(self):
        # Each case uses a separate batch of jobs in this private test database.
        with self.store.connect() as conn:
            conn.execute("DELETE FROM public.ai_collection_bridge_jobs WHERE id<>%s", (self.old_id,))

    def test_capacity_queue_scope_and_single_account_owner(self):
        from contextlib import nullcontext
        from itertools import product
        from modern_map_cycle import first_claim, next_claim
        from modern_map_basic_numbers import BASIC_SYMBOLS, BASIC_RANGES

        env = dict(MODERN_TEST_ENV, COINGLASS_VERIFIED_SYMBOLS=','.join(BASIC_SYMBOLS))
        with patch.dict(os.environ, env):
            expected = set(product(BASIC_SYMBOLS, range(1,4), BASIC_RANGES))
            for symbol, model, timeframe in sorted(expected):
                store = bridge.JobStore(self.store.dsn, model)
                row = store.start(str(uuid4()), timeframe, symbol)
                self.assertEqual(row['status'], 'queued')
            with self.store.connect() as conn:
                outsider = str(uuid4())
                conn.execute("""INSERT INTO public.ai_collection_bridge_jobs
                    (id,timeframe,status,heatmap_model,symbol) VALUES (%s,'24H','queued',1,'OUTSIDE')""", (outsider,))
            owner, first = first_claim(self.store)
            try:
                self.assertIsNone(first_claim(self.store))
                claimed = [first]
                while row := next_claim(owner, nullcontext()):
                    claimed.append(row)
                self.assertEqual(len(claimed),96)
                self.assertEqual({(row['symbol'], row['heatmap_model'], row['timeframe']) for row in claimed},expected)
            finally:
                owner.close()
            with self.store.connect() as conn:
                self.assertEqual(conn.execute("SELECT status FROM public.ai_collection_bridge_jobs WHERE id=%s",
                                              (outsider,)).fetchone()['status'], 'queued')
            with patch.dict(os.environ, dict(COINGLASS_VERIFIED_SYMBOLS='BTC,OUTSIDE', OPENAI_API_KEY='unused-test')):
                with self.assertRaises(bridge.BridgeError) as unsupported:
                    self.store.start(str(uuid4()), '24H', 'OUTSIDE')
                self.assertEqual(unsupported.exception.code, 'unsupported_symbol')

    def test_weekly_checkpoint_restore_and_stale_claim_fences(self):
        from modern_map_basic_numbers import basic_source_url, build_basic_snapshot
        from modern_map_checkpoint import CheckpointStore, SOURCE, restore
        from modern_map_task import numeric_result
        from test_modern_map_basic_numbers import live_inputs

        dims = ('ETH', 3, '1W')
        store = bridge.JobStore(self.store.dsn, 3)
        with patch.dict(os.environ, MODERN_TEST_ENV), tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job = store.start(str(uuid4()), '1W', 'ETH')['job_id']
            def claim():
                with store.connect() as conn:
                    return conn.execute("""UPDATE public.ai_collection_bridge_jobs SET status='running',
                        started_at=clock_timestamp() WHERE id=%s RETURNING *""", (job,)).fetchone()
            original, current = claim(), claim()
            stale = CheckpointStore(store,job,*dims,started_at=original['started_at'])
            active = CheckpointStore(store,job,*dims,started_at=current['started_at'])
            self.assertFalse(symbol_jobs.finish_claim(store,original,None,None,'interrupted'))
            self.assertEqual(store.get(job,'ETH')['status'],'running')
            with self.assertRaisesRegex(ValueError,'measured_job_not_active'):
                stale.commit(b'P',root)
            with self.assertRaisesRegex(ValueError,'measured_job_not_active'):
                CheckpointStore(store,job,'ETH',2,'1W').commit(b'P',root)
            partial = dict(run_id=job,status='partial',image_sha256=None,identity=dict(symbol='BTC',
                           heatmap_model=3,timeframe='1W',mode='Coin'))
            (root/'measured-partial.json').write_text(json.dumps(partial))
            with self.assertRaisesRegex(ValueError,'measured_partial_identity'):
                active.commit(b'P',root)
            partial['identity']['symbol']='ETH'
            (root/'measured-partial.json').write_text(json.dumps(partial))
            active.commit(b'P',root)
            image=(Path(__file__).parent/'fixtures/btc_12h_1211_edge.png').read_bytes()
            digest=hashlib.sha256(image).hexdigest()
            market,args=live_inputs()
            args['end_observation'].update(symbol='ETH', heatmap_model=3, timeframe='1W',
                                          range_control=dict(label='1 week', value='1w'))
            args.update(symbol='ETH',model=3,timeframe='1W',source_url=basic_source_url('ETH'),source_sha256=digest)
            snapshot=build_basic_snapshot(market,**args)
            (root/'image.png').write_bytes(image)
            (root/SOURCE).write_text(json.dumps(dict(run_id=job,image_sha256=digest,snapshot=snapshot)))
            active.commit(b'S',root)
            active.commit(b'S',root)  # an acknowledgement can be lost
            self.assertIsNone(CheckpointStore(store,job,'BTC',3,'1W').read())
            saved=active.read()
            restored=root/'restored'
            restored.mkdir()
            with self.assertRaisesRegex(ValueError,'measured_record_identity'):
                restore(restored,job,saved,identity=('ETH',3,'24H'))
            self.assertTrue(restore(restored,job,saved,identity=dims))
            self.assertEqual((restored/'image.png').read_bytes(),image)
            # The old process disappeared after committing source but before publishing.
            from modern_map_cycle import first_claim
            owner, recovered = first_claim(store)
            self.addCleanup(owner.close)
            self.assertEqual(str(recovered['id']),job)
            self.assertNotEqual(recovered['started_at'],current['started_at'])
            self.assertFalse(symbol_jobs.finish_claim(store,current,None,None,'interrupted'))
            current = recovered
            result=numeric_result(snapshot,job,image)
            self.assertTrue(symbol_jobs.valid_result(result,image,job,'1W',3,'ETH',True))
            self.assertFalse(symbol_jobs.valid_result(result,image,job,'1W',1,'BTC',True))
            self.assertTrue(symbol_jobs.finish_claim(store,current,result,image))
            self.assertTrue(symbol_jobs.finish_claim(store,current,result,image))
            self.assertFalse(symbol_jobs.finish_claim(store,current,result,None))
            self.assertFalse(symbol_jobs.finish_claim(store,original,None,None,'interrupted'))
            self.assertEqual(store.get(job,'ETH')['result'],result)

    def test_login_retry_requires_a_verified_incident_resolution(self):
        from operator_login import incident_status, record_login_required, waiting_incident
        rid = str(uuid4())
        store = self.store
        with patch.dict(os.environ,{'COINGLASS_SESSION_STORE_ENABLED':'true'}):
            job = store.start(rid,'12H','BTC',1)
            jid = job['job_id']
            with store.connect() as conn:
                conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='failed',error_code='source_login_required' WHERE id=%s",(jid,))
                incident = record_login_required(conn, 'BTC', 1, '12H', jid)
                self.assertEqual(record_login_required(conn, 'BTC', 1, '12H', jid), incident)
                self.assertEqual(waiting_incident(conn, 'BTC', 1, '12H'), incident)
                self.assertIsNone(waiting_incident(conn, 'BTC', 1, '48H'))
                self.assertEqual(incident_status(conn, job_id=jid), {'status':'waiting', 'incident_id':incident})
            with self.assertRaises(bridge.BridgeError) as err:
                store.start(rid,'12H','BTC',2)
            self.assertEqual(err.exception.code,'login_pending')
            self.assertEqual(store.get(jid)['attempt'],1)
            with store.connect() as conn:
                conn.execute("UPDATE collector_private.coinglass_login_incidents SET resolved_at=now(),resolved_revision=1 WHERE id=%s",(incident,))
            self.assertEqual(store.start(rid,'12H','BTC',2)['attempt'],2)

    def test_cycle_isolation_partial_completion_and_retry(self):
        store = self.store
        with patch.dict(os.environ,MODERN_TEST_ENV):
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
            with self.assertRaises(bridge.BridgeError) as overlap:
                store.start(str(uuid4()),'24H','BTC',1)
            self.assertEqual(overlap.exception.code,'map_in_progress')
            isolated = store.start(str(uuid4()),'24H','ETH',1)
            self.assertIsNone(store.get(isolated['job_id'],'BTC'))
            with self.assertRaisesRegex(bridge.BridgeError,'another'):
                store.start(rid,'24H','ETH',1)
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
            self.assertIsNone(store.latest('24H','ETH'))
            self.assertEqual(store.evidence(jid,'BTC'),b'PNG')
            self.assertIsNone(store.evidence(jid,'ETH'))
            self.assertEqual(store.start(rid,'24H','BTC',3)['status'],'ready')

    def test_throttle_defers_new_work_without_spending_attempt_or_blocking_saved_reads(self):
        from dedicated_auth_state import account_slot
        rid=str(uuid4())
        with patch.dict(os.environ, {**MODERN_TEST_ENV,'COINGLASS_LOGIN_EMAIL':'cooldown-fixture@example.invalid'}):
            store=self.store
            job=store.start(rid,'12H','BTC',1)
            jid=job['job_id']
            slot=account_slot(bytes(32),os.environ['COINGLASS_LOGIN_EMAIL'])
            with store.connect() as conn:
                conn.execute("UPDATE public.ai_collection_bridge_jobs SET status='failed',error_code='source_rate_limited' WHERE id=%s",(jid,))
                conn.execute("""INSERT INTO collector_private.coinglass_dedicated_sessions
                    (slot,revision,status,map_retry_after) VALUES(%s,1,'unconfirmed',now()+interval '4 hours')""",(slot,))
            for run,attempt in ((rid,2),(str(uuid4()),1)):
                with self.assertRaises(bridge.BridgeError) as deferred:
                    store.start(run,'12H','BTC',attempt)
                self.assertEqual(deferred.exception.code,'source_rate_limited')
                self.assertGreater(deferred.exception.retry_after_seconds,3601)
            poll=store.get(jid,'BTC')
            self.assertEqual((poll['attempt'],poll['status']),(1,'failed'))
            self.assertIn('retry_at',poll['error'])
            self.assertEqual(store.start(rid,'12H','BTC',1)['error']['retry_at'],poll['error']['retry_at'])
            with store.connect() as conn:
                conn.execute("UPDATE collector_private.coinglass_dedicated_sessions SET map_retry_after=now()-interval '1 second' WHERE slot=%s",(slot,))
            resumed=store.start(rid,'12H','BTC',2)
            self.assertEqual((resumed['job_id'],resumed['attempt']),(jid,2))

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
