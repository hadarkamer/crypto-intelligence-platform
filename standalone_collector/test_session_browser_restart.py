"""Offline cold-browser persistence fixture. All page responses are synthetic."""
import asyncio
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import unittest
from uuid import uuid4

from session_runtime import ManagedSession
from session_state import PgSessionStore, WORK_LOCK
from test_session_state import HERE, IsolatedPg, KEY, PGLITE

BROWSER_CHILD = r"""
import os,sys
from playwright.sync_api import sync_playwright
from session_capture import CaptureSession,note_protected_map_ready
from model1_execution import StageFailure

mode=sys.argv[1]
managed=CaptureSession.from_environment()
assert 'COINGLASS_SESSION_KEY' not in os.environ and 'DATABASE_URL' not in os.environ
with sync_playwright() as playwright:
    browser=playwright.chromium.launch(executable_path=sys.argv[2],headless=True,
        args=['--host-resolver-rules=MAP * ~NOTFOUND','--disable-background-networking'])
    context=browser.new_context(storage_state=managed.state,service_workers='block')
    page=context.new_page()
    managed.attach(context,page)
    def fixture(route):
        request=route.request
        if request.url != 'https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin':
            route.abort()
            return
        accepted='dummy_session=rotated_local_fixture' in (request.header_value('cookie') or '')
        marker='protected fixture' if mode=='rotate' or accepted else 'missing session'
        route.fulfill(content_type='text/html',body='<main>'+marker+'</main>')
    context.route('**/*',fixture)
    try:
        page.goto('https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin')
        assert page.locator('main').inner_text()=='protected fixture'
        if mode=='rotate':
            page.evaluate("document.cookie='dummy_session=rotated_local_fixture; Secure; Path=/'")
        note_protected_map_ready(page)
        if mode=='rotate':
            managed.note_source_error(StageFailure('source_not_readable'))
    finally:
        managed.finish(context)
        context.close()
        browser.close()
print('local_browser_fixture_finished')
"""


@unittest.skipUnless(PGLITE.is_dir() and shutil.which('chromium') and shutil.which('node'),
                     'offline browser/PGlite fixture unavailable')
class BrowserRestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_browser_and_database_restart_reuse_scoped_checkpoint_after_capture_failure(self):
        try:
            with socket.socket(socket.AF_UNIX):
                pass
        except PermissionError:
            self.skipTest('Chromium process socket is forbidden by this sandbox')
        with tempfile.TemporaryDirectory(prefix='cg-browser-fixture-') as directory:
            for mode in ('rotate', 'restore'):
                db = IsolatedPg(Path(directory) / 'db')
                try:
                    if mode == 'rotate':
                        db.script((HERE / 'session_state.sql').read_text())
                    db.execute('SELECT pg_advisory_lock(%s)', (WORK_LOCK,))
                    store = PgSessionStore(db, KEY)
                    managed = ManagedSession(store, str(uuid4()))
                    await managed.begin()
                    transport = await managed.open_transport({'PATH': os.environ['PATH'], 'HOME': directory,
                        'PYTHONDONTWRITEBYTECODE': '1', 'DATABASE_URL': 'synthetic',
                        'COINGLASS_SESSION_KEY': 'synthetic'})
                    proc = await asyncio.create_subprocess_exec(sys.executable, '-B', '-c', BROWSER_CHILD,
                        mode, shutil.which('chromium'), cwd=HERE, env=transport.env, pass_fds=transport.pass_fds,
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    transport.spawned()
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=40)
                    try:
                        self.assertEqual(proc.returncode, 0, stderr.decode())
                    finally:
                        if proc.returncode == 0:
                            await managed.finish(transport)
                        else:
                            try:
                                await managed.finish(transport)
                            except Exception:
                                pass
                    self.assertEqual(stdout, b'local_browser_fixture_finished\n')
                    saved = store.read()['cookies']
                    self.assertEqual([c['value'] for c in saved], ['rotated_local_fixture'])
                    self.assertNotIn('sameSite', saved[0])
                finally:
                    db.close()
