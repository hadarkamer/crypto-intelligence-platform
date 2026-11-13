"""Real anonymous-pipe/subprocess checks with explicit test boundaries.

RuntimeTests isolates storage; StorePipeIntegrationTests combines the real
encrypted store, PostgreSQL SQL, and subprocess handoff. Neither opens a browser,
account, production database or network connection.
"""
import asyncio
import copy
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from uuid import uuid4

from session_runtime import ManagedSession, SessionError, SessionTransport, MAX_FRAME
from session_state import Lease
from test_session_state import IsolatedPg, KEY, PGLITE, PgSessionStore, WORK_LOCK
import shutil

STATE = {'cookies': [{'name': 'test-session', 'value': 'DUMMY_INITIAL_STATE',
    'domain': '.coinglass.com', 'path': '/', 'expires': -1,
    'httpOnly': True, 'secure': True, 'sameSite': 'Lax'}], 'origins': []}

CHILD = r'''
import json,os,struct,sys
def no_network(event,args):
 if event.startswith('socket.'):
  raise RuntimeError('test_network_forbidden')
sys.addaudithook(no_network)
assert 'COINGLASS_SESSION_KEY' not in os.environ
assert 'DATABASE_URL' not in os.environ
assert os.environ['COINGLASS_COOKIE_HEADER']==''
fd=int(os.environ.pop('COINGLASS_SESSION_RETURN_FD'))
os.set_inheritable(fd,False)
run_id=os.environ.pop('COINGLASS_SESSION_RUN_ID')
revision=int(os.environ.pop('COINGLASS_SESSION_REVISION'))
state=json.loads(os.environ.pop('COINGLASS_STORAGE_STATE_JSON'))
os.environ.pop('COINGLASS_COOKIE_HEADER')
state['cookies'][0]['value']='DUMMY_UPDATED_STATE'
mode=sys.argv[1]
outcome='blocked' if mode=='blocked' else 'ready'
if mode=='large': state['cookies'][0]['value']='X'*50000
if mode=='wrong_run':run_id='00000000-0000-0000-0000-000000000000'
payload=json.dumps({'run_id':run_id,'revision':revision,'outcome':outcome,
 'state':state if outcome=='ready' else None},separators=(',',':')).encode()
try:
 if mode=='missing':pass
 elif mode=='oversize':os.write(fd,struct.pack('!I',int(sys.argv[2])+1))
 else:
  data=struct.pack('!I',len(payload))+payload
  while data:
   data=data[os.write(fd,data):]
finally:os.close(fd)
print('filtered_child_completed')
if mode=='hover_failed':sys.exit(7)
'''


class StoreBoundary:
    def __init__(self, *, fail_write=False):
        self.fail_write = fail_write
        self.saved = None
        self.status = 'ready'
        self.run_id = None

    def begin(self, run_id):
        if self.status != 'ready':
            raise SessionError('state_not_ready')
        self.status = 'in_use'; self.run_id = run_id
        return Lease(2, run_id, copy.deepcopy(STATE))

    def complete(self, lease, outcome, state=None):
        if self.fail_write:
            raise SessionError('state_store_failed')
        self.status = outcome
        self.saved = copy.deepcopy(state)
        return 3


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def execute(self, mode='ready', *, fail_write=False):
        store = StoreBoundary(fail_write=fail_write)
        session = ManagedSession(store, str(uuid4()))
        await session.begin()
        env = {'PATH': os.environ['PATH'], 'PYTHONDONTWRITEBYTECODE': '1',
               'COINGLASS_SESSION_KEY': 'DUMMY_MASTER_KEY',
               'DATABASE_URL': 'postgresql://DUMMY_PRIVATE_DATABASE',
               'COINGLASS_COOKIE_HEADER': 'obe=DUMMY_OLD_HEADER'}
        transport = await session.open_transport(env)
        self.assertNotIn('COINGLASS_SESSION_KEY', transport.env)
        process = await asyncio.create_subprocess_exec(sys.executable, '-B', '-c', CHILD, mode, str(MAX_FRAME),
            env=transport.env, pass_fds=transport.pass_fds,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        transport.spawned()
        stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
        self.assertEqual(stderr, b'')
        self.assertEqual(stdout, b'filtered_child_completed\n')
        failure = None
        try:
            await session.finish(transport)
        except SessionError as error:
            failure = error.code
        self.assertIsNone(session.lease)
        self.assertTrue(transport.reader.done())
        self.assertEqual(transport.env, {})
        with self.assertRaises(OSError):
            os.fstat(transport.read_fd)
        return store, failure, process.returncode

    async def test_real_child_returns_updated_state_without_logging_it(self):
        store, error, code = await self.execute()
        self.assertIsNone(error); self.assertEqual(code, 0)
        self.assertEqual(store.status, 'ready')
        self.assertEqual(store.saved['cookies'][0]['value'], 'DUMMY_UPDATED_STATE')

    async def test_valid_state_is_saved_even_when_child_reports_hover_failure(self):
        store, error, code = await self.execute('hover_failed')
        self.assertIsNone(error); self.assertEqual(code, 7)
        self.assertEqual(store.status, 'ready')

    async def test_large_receipt_drains_while_child_runs(self):
        store, error, code = await self.execute('large')
        self.assertIsNone(error); self.assertEqual(code, 0)
        self.assertEqual(len(store.saved['cookies'][0]['value']), 50000)

    async def test_store_failure_is_not_success_or_reuse(self):
        store, error, _ = await self.execute(fail_write=True)
        self.assertEqual(error, 'state_store_failed')
        self.assertEqual(store.status, 'in_use')
        with self.assertRaises(SessionError):
            store.begin(str(uuid4()))

    async def test_explicit_access_block_does_not_return_state_to_ready(self):
        store, error, _ = await self.execute('blocked')
        self.assertEqual(error, 'session_access_requires_review')
        self.assertEqual(store.status, 'blocked')
        self.assertIsNone(store.saved)

    async def test_missing_mismatched_or_oversized_receipt_is_unconfirmed(self):
        for mode in ('missing', 'wrong_run', 'oversize'):
            with self.subTest(mode=mode):
                store, error, _ = await self.execute(mode)
                self.assertEqual(error, 'session_transport_failed')
                self.assertEqual(store.status, 'unconfirmed')

    async def test_retained_writer_and_cancellation_close_reader(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                transport = SessionTransport({}, Lease(2, str(uuid4()), copy.deepcopy(STATE)))
                retained = os.dup(transport.write_fd)
                transport.spawned()
                task = asyncio.create_task(transport.receive())
                try:
                    if cancel:
                        await asyncio.sleep(0.05)
                        task.cancel()
                    with self.assertRaises((SessionError, asyncio.CancelledError)):
                        await asyncio.wait_for(task, 3)
                    self.assertTrue(transport.reader.done())
                    with self.assertRaises(OSError):os.fstat(transport.read_fd)
                finally:os.close(retained)

    async def test_failed_spawn_cleans_pipe_and_preserves_unconfirmed(self):
        store = StoreBoundary(); session = ManagedSession(store, str(uuid4()))
        await session.begin()
        transport = await session.open_transport({})
        with self.assertRaises(SessionError):await session.finish(transport)
        self.assertTrue(transport.reader.done())
        self.assertEqual(store.status, 'unconfirmed')


@unittest.skipUnless(PGLITE.is_dir() and shutil.which('node'), 'existing isolated PGlite unavailable')
class StorePipeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_store_pipe_save_and_next_run(self):
        with tempfile.TemporaryDirectory(prefix='coinglass-state-pipe-') as temporary:
            db = await asyncio.to_thread(IsolatedPg, Path(temporary) / 'db')
            process = transport = None
            try:
                await asyncio.to_thread(db.script, Path(__file__).with_name('session_state.sql').read_text())
                await asyncio.to_thread(db.execute, 'SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
                store = PgSessionStore(db, KEY)
                await asyncio.to_thread(store.bootstrap, STATE)
                session = ManagedSession(store, str(uuid4()))
                await session.begin()
                transport = await session.open_transport({'PATH': os.environ['PATH']})
                process = await asyncio.create_subprocess_exec(sys.executable, '-B', '-c', CHILD,
                    'hover_failed', str(MAX_FRAME), env=transport.env, pass_fds=transport.pass_fds,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                transport.spawned()
                stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
                self.assertEqual((stdout, stderr, process.returncode),
                    (b'filtered_child_completed\n', b'', 7))
                await session.finish(transport)
                row = await asyncio.to_thread(db.execute,
                    'SELECT revision,status,encrypted_payload FROM collector_private.coinglass_session_state')
                saved = row.fetchone()
                self.assertEqual(saved['status'], 'ready')
                self.assertNotIn('DUMMY_UPDATED_STATE', saved['encrypted_payload'])
                later = ManagedSession(PgSessionStore(db, KEY), str(uuid4()))
                await later.begin()
                self.assertEqual(later.lease.state['cookies'][0]['value'], 'DUMMY_UPDATED_STATE')
                self.assertGreater(later.lease.revision, saved['revision'])
                # A later aborted process cannot silently reuse the stored state.
                with self.assertRaises(SessionError):
                    await later.finish(None)
                with self.assertRaises(SessionError):
                    await asyncio.to_thread(store.begin, str(uuid4()))
            finally:
                if process is not None and process.returncode is None:
                    process.kill(); await process.wait()
                if transport is not None and not transport.reader.done():
                    try: await transport.receive()
                    except SessionError: pass
                await asyncio.to_thread(db.close)
        self.assertFalse(Path(temporary).exists())


if __name__ == '__main__':unittest.main()
