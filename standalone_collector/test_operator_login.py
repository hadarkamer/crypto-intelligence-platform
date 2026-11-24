"""Isolated route/transport tests and optional private disposable PostgreSQL cluster."""
import asyncio
import base64
from datetime import datetime, timedelta, timezone
import json
import os
import secrets
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

import operator_window
from operator_desktop import permitted_host
from operator_login import (LoginIncidentError, begin_window, close_window, incident_status,
                            record_login_required, register_operator_login, waiting_incident)
from operator_window import COOKIE, Database, Window, WindowManager
from session_state import SessionStateError, WORK_LOCK

HERE = Path(__file__).resolve().parent
ORIGIN = 'https://collector.invalid'
APP_ORIGIN = 'https://app.invalid'
OWNER = '6d7e012f-868a-4105-a19b-412cead3ed93'
INCIDENT = '576e893e-6fe1-42a7-bdb8-7bfba1aa737d'
DUMMY = {'cookies':[{'domain':'.coinglass.com','name':'dummy','value':'fixture',
                     'path':'/','secure':True,'httpOnly':True,'expires':-1}], 'origins':[]}


class SimulatedDB:
    def __init__(self):
        self.released = self.saved = 0

    def reserve(self,incident):
        assert incident == INCIDENT
        return str(uuid4()),datetime.now(timezone.utc)+timedelta(minutes=10),('BTC',1,'12H'),0

    def release(self,window):
        self.released += 1

    def finish(self,window,state):
        assert window.target==('BTC',1,'12H') and state==DUMMY
        self.saved += 1


class LocalDisplay:
    @classmethod
    async def start(cls,*target):
        assert target==('BTC',1,'12H')
        item = cls()
        item.proven = asyncio.Event()
        item.parent,item.peer = socket.socketpair()
        item.peer.setblocking(False)
        return item

    async def connect_display(self):
        return await asyncio.open_connection(sock=self.parent)

    async def viewer_attached(self):
        self.viewed = True

    async def acknowledge_commit(self):
        self.committed = True

    async def verified_state(self):
        await self.proven.wait()
        return DUMMY

    async def verify_now(self):
        self.peer.close()
        self.proven.set()

    async def close(self):
        self.parent.close()
        self.peer.close()


class PrivateViewerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = SimulatedDB()
        self.auth = {'Authorization':'Bearer '+'test'*12}
        self.env = patch.dict(os.environ,{'COINGLASS_COLLECTOR_TOKEN':'test'*12,
            'COINGLASS_OPERATOR_ID':OWNER,'COINGLASS_OPERATOR_ORIGIN':ORIGIN,'COINGLASS_APP_ORIGIN':APP_ORIGIN,
            'COINGLASS_OPERATOR_DESKTOP_ENABLED':'true','COINGLASS_SESSION_STORE_ENABLED':'true'})
        self.env.start()
        def factory(db,owner,origin,app_origin,enabled):
            async def ready(): return True
            self.manager = WindowManager(self.db,owner,origin,app_origin,enabled,LocalDisplay,ready)
            return self.manager
        self.app = web.Application()
        with patch.object(operator_window,'WindowManager',side_effect=factory):
            register_operator_login(self.app,object(),True)
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()
        await asyncio.sleep(0)
        self.launch_path = '/api/collection/coinglass/session-window'
        self.verifier = secrets.token_urlsafe(32)
        self.body = {'incident_id':INCIDENT,'operator_id':OWNER,'viewer_challenge':operator_window.viewer_challenge(self.verifier)}

    async def asyncTearDown(self):
        await self.client.close()
        self.env.stop()

    async def ticket(self):
        opened = await self.client.post(self.launch_path,json=self.body,headers=self.auth)
        self.assertEqual(opened.status,200)
        data = await opened.json()
        self.assertEqual(set(data),{'path','expires_at'})
        self.assertTrue(data['path'].startswith('/coinglass/operator#'))
        return data['path'].split('#')[1]

    async def viewer(self):
        ticket = await self.ticket()
        admitted = await self.client.post('/coinglass/operator/admit',data={'ticket':ticket,'verifier':self.verifier},allow_redirects=False,
                                           headers={'Origin':APP_ORIGIN})
        self.assertEqual(admitted.status,303)
        self.assertEqual(admitted.headers['Location'],'/coinglass/operator')
        cookie = admitted.cookies[COOKIE]
        self.assertTrue(cookie['secure'] and cookie['httponly'])
        self.assertEqual(cookie['samesite'],'Strict')
        replay = await self.client.post('/coinglass/operator/admit',data={'ticket':ticket,'verifier':self.verifier},allow_redirects=False,
                                        headers={'Origin':APP_ORIGIN})
        self.assertEqual(replay.status,403)
        return {'Cookie':COOKIE+'='+cookie.value,'Origin':ORIGIN}

    async def test_bridge_and_single_use_owner_ticket(self):
        denied = await self.client.post(self.launch_path,json=self.body)
        self.assertEqual(denied.status,401)
        wrong = await self.client.post(self.launch_path,json={**self.body,'operator_id':str(uuid4())},
                                       headers=self.auth)
        self.assertEqual(wrong.status,403)
        ticket = await self.ticket()
        missing = await self.client.post('/coinglass/operator/admit',data={'ticket':ticket},headers={'Origin':APP_ORIGIN})
        self.assertEqual(missing.status,403)
        wrong_proof = await self.client.post('/coinglass/operator/admit',
                              data={'ticket':ticket,'verifier':secrets.token_urlsafe(32)},headers={'Origin':APP_ORIGIN})
        self.assertEqual(wrong_proof.status,403)
        duplicate = await self.client.post(self.launch_path,json=self.body,headers=self.auth)
        self.assertEqual(duplicate.status,409)
        evil = await self.client.post('/coinglass/operator/admit',data={'ticket':ticket,'verifier':self.verifier},allow_redirects=False,
                                      headers={'Origin':'https://elsewhere.invalid'})
        self.assertEqual(evil.status,403)
        self.manager.current.redeem_before = 0
        expired = await self.client.post('/coinglass/operator/admit',data={'ticket':ticket,'verifier':self.verifier},allow_redirects=False,
                                         headers={'Origin':APP_ORIGIN})
        self.assertEqual(expired.status,403)
        await asyncio.sleep(.35)
        self.assertIsNone(self.manager.current)
        self.assertEqual((self.db.saved,self.db.released),(0,1))

    async def test_verified_receipt_after_private_server_closes_display(self):
        headers = await self.viewer()
        alien = await self.client.get('/coinglass/operator/display',headers={**headers,'Origin':'https://x.invalid'})
        self.assertEqual(alien.status,403)
        ws = await self.client.ws_connect('/coinglass/operator/display',headers=headers,protocols=('binary',))
        second = await self.client.get('/coinglass/operator/display',headers=headers)
        self.assertEqual(second.status,403)
        desktop = self.manager.current.desktop
        await asyncio.get_running_loop().sock_sendall(desktop.peer,b'RFB 003.008\n')
        self.assertEqual((await ws.receive()).data,b'RFB 003.008\n')
        await ws.send_bytes(b'RFB 003.008\n')
        self.assertEqual(await asyncio.get_running_loop().sock_recv(desktop.peer,12),b'RFB 003.008\n')
        verify = await self.client.post('/coinglass/operator/verify',headers=headers)
        self.assertEqual(verify.status,200)
        await ws.receive()
        for _ in range(40):
            if self.manager.current is None:
                break
            await asyncio.sleep(.025)
        self.assertTrue(desktop.viewed and desktop.committed)
        self.assertEqual((self.db.saved,self.db.released),(1,1))
        stale = await self.client.get('/coinglass/operator/display',headers=headers)
        self.assertEqual(stale.status,403)

    async def test_owner_closes_before_validation(self):
        headers = await self.viewer()
        ws = await self.client.ws_connect('/coinglass/operator/display',headers=headers,protocols=('binary',))
        await ws.close()
        for _ in range(40):
            if self.manager.current is None:
                break
            await asyncio.sleep(.025)
        self.assertEqual((self.db.saved,self.db.released),(0,1))
        self.manager.ready = False
        unavailable = await self.client.post(self.launch_path,json=self.body,headers=self.auth)
        self.assertEqual(unavailable.status,503)

    async def test_untrusted_egress_domains(self):
        for domain in ('www.coinglass.com','accounts.google.com'):
            self.assertTrue(permitted_host(domain))
        for domain in ('example.net','coinglass.com.example.net','accounts.google.com.example.net','google.com',
                       '127.0.0.1','localhost','metadata.google.internal'):
            self.assertFalse(permitted_host(domain))


@unittest.skipUnless(os.environ.get('PG_BIN_FOR_OPERATOR_TESTS'),'local isolated PostgreSQL toolchain unavailable')
class IncidentPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.rows import dict_row
        cls.directory = tempfile.TemporaryDirectory(prefix='cg-pg-test-')
        root = Path(cls.directory.name)
        binary = Path(os.environ['PG_BIN_FOR_OPERATOR_TESTS'])
        cls.pgctl = str(binary/'pg_ctl')
        cls.data = str(root/'pgdata')
        # Use loopback SCRAM with a fresh ephemeral secret. Some test sandboxes
        # deny even creating a named Unix-domain socket.
        password = secrets.token_urlsafe(32)
        pwfile = root/'initpass'
        pwfile.write_text(password)
        pwfile.chmod(0o600)
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as sock:
            sock.bind(('127.0.0.1',0))
            port = sock.getsockname()[1]
        subprocess.run([str(binary/'initdb'),'-D',cls.data,'-A','scram-sha-256',
                        '--pwfile',str(pwfile),'--no-locale','--encoding=UTF8'],
                       capture_output=True,check=True)
        pwfile.unlink()
        subprocess.run([cls.pgctl,'-D',cls.data,'-o',f"-c unix_socket_directories='' -h 127.0.0.1 -p {port}",
                        '-l',str(root/'pg.log'),'-w','start'],capture_output=True,check=True)
        class Store:
            def connect(self):
                return psycopg.connect(host='127.0.0.1',port=port,password=password,dbname='postgres',row_factory=dict_row)
        cls.store = Store()
        with cls.store.connect() as conn:
            conn.execute((HERE/'session_state.sql').read_text())
            conn.execute((HERE/'operator_login.sql').read_text())
        cls.env = patch.dict(os.environ,{'COINGLASS_SESSION_STORE_ENABLED':'true',
            'COINGLASS_SESSION_KEY':base64.b64encode(bytes(32)).decode()})
        cls.env.start()

    @classmethod
    def tearDownClass(cls):
        cls.env.stop()
        subprocess.run([cls.pgctl,'-D',cls.data,'-m','immediate','-w','stop'],capture_output=True,check=True)
        cls.directory.cleanup()

    def setUp(self):
        with self.store.connect() as conn:
            conn.execute('TRUNCATE collector_private.coinglass_login_incidents, '
                         'collector_private.coinglass_login_jobs, collector_private.coinglass_session_state CASCADE')
        self.db = Database(self.store)
        self.job = str(uuid4())

    def record(self):
        with self.store.connect() as conn:
            return record_login_required(conn,'BTC',1,'12H',self.job)

    def window(self,incident):
        wid,expires,target,revision = self.db.reserve(incident)
        return Window(wid,incident,expires,target,revision,'',0)

    def test_success_recovery_then_same_job_rechallenged(self):
        gate = self.record()
        self.assertEqual(self.record(),gate)
        with self.store.connect() as conn:
            self.assertEqual(record_login_required(conn,'ETH',2,'48H',str(uuid4())),gate)
            self.assertIsNone(waiting_incident(conn,'ETH',2,'24H'))
            self.assertEqual(conn.info.transaction_status,0)
        with self.store.connect() as public_job:
            public_job.execute('SELECT pg_advisory_lock(%s)',(WORK_LOCK,))
            window = self.window(gate)
        with self.store.connect() as conn:
            self.assertTrue(conn.execute('SELECT pg_try_advisory_lock(%s) AS held',(WORK_LOCK,)).fetchone()['held'])
            self.assertEqual(incident_status(conn,gate)['status'],'in_progress')
        with self.assertRaises(LoginIncidentError):
            self.window(gate)
        window.human_wait_ms = 234
        self.assertEqual(self.db.finish(window,DUMMY),1)
        self.db.release(window)
        with self.store.connect() as conn:
            result = incident_status(conn,gate)
            self.assertEqual((result['status'],result['human_wait_ms']),('resolved',234))
            self.assertEqual(incident_status(conn,job_id=self.job),result)
        successor = self.record()
        self.assertNotEqual(successor,gate)
        new = self.window(successor)
        self.assertEqual(new.revision,1)
        self.assertEqual(self.db.finish(new,DUMMY),2)

    def test_expiry_fences_late_completion_and_release(self):
        gate = self.record()
        old = self.window(gate)
        with self.store.connect() as conn:
            conn.execute("UPDATE collector_private.coinglass_login_incidents SET window_expires_at=now()-interval '1s'")
        current = self.window(gate)
        self.db.release(old)
        with self.assertRaises(LoginIncidentError):
            self.db.finish(old,DUMMY)
        self.assertEqual(self.db.finish(current,DUMMY),1)

    def test_revision_conflict_preserves_ready_state_and_pending_incident(self):
        from session_runtime import ManagedSession
        gate = self.record()
        window = self.window(gate)
        with self.store.connect() as conn:
            conn.execute('SELECT pg_advisory_lock(%s)',(WORK_LOCK,))
            store = ManagedSession.from_environment(conn,str(uuid4())).store
            store.renew(DUMMY,0)
        with self.assertRaises(SessionStateError):
            self.db.finish(window,DUMMY)
        self.db.release(window)
        with self.store.connect() as conn:
            self.assertEqual(incident_status(conn,gate)['status'],'waiting')


if __name__ == '__main__':
    unittest.main()
