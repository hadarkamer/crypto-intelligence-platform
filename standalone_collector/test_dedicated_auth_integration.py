"""Offline form → protected map → IPC → PostgreSQL integration across two OS workers.

The first-party DOM, chart pixels and Chromium cookie interface are synthetic.
The form state machine, protected-map verification sequence, process sockets,
owner, encryption and PostgreSQL operations run through product code.
"""
import asyncio
from contextlib import ExitStack
from io import BytesIO
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from dedicated_login_form import HOME_URL
from test_dedicated_login_form import Clock, Locator, Page


FIXTURE_EMAIL = 'synthetic@example.invalid'
FIXTURE_PASSWORD = 'fixture-only-password-token'
COOKIE = {'name': 'fixture_session', 'value': 'synthetic-only-cookie-token', 'domain': '.coinglass.com',
          'path': '/', 'secure': True, 'httpOnly': True, 'expires': -1}
TARGET_URL = 'https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'


class OfficialFixture(Page):
    def __init__(self, state):
        super().__init__(Clock())
        self.url = 'about:blank'
        self.authenticated = state['cookies'] == [COOKIE]
        self.proof_images = 0
        self.form_opens = 0
        self.range = '24 hour'
        self.after_submit = self.accept

    def goto(self, url, **kwargs):
        if url == HOME_URL:
            assert not self.authenticated
            self.form_opens += 1
            return super().goto(url, **kwargs)
        assert url == TARGET_URL and self.authenticated
        self.url = url
        return Mock(status=200)

    def accept(self, _):
        assert self.ops == ['check','email','password','check','submit']
        self.authenticated = True
        self.url = 'https://www.coinglass.com/'

    def get_by_text(self, pattern, **kwargs):
        if self.url == TARGET_URL:
            labels = ['BTC Liquidation Heatmap', 'Market overview']
            hits = sum(label == pattern if isinstance(pattern, str) else bool(pattern.search(label)) for label in labels)
            return Locator(self, hits)
        return super().get_by_text(pattern)

    def set_default_timeout(self, ms):
        assert ms > 0

    def screenshot(self, *, clip, mask, animations):
        from PIL import Image, ImageDraw
        assert self.authenticated and self.url == TARGET_URL and self.range == '12 hour' and mask
        assert clip == {'x': 0, 'y': 0, 'width': 480, 'height': 240}
        self.proof_images += 1
        output = BytesIO()
        with Image.new('RGB', (480,240)) as image:
            pen = ImageDraw.Draw(image)
            for row in range(240):
                pen.line((0,row,479,row), fill=(240,row % 220,5))
            image.save(output, format='PNG')
        return output.getvalue()


def run_child(mode):
    from dedicated_map_flow import MapTarget, collect_with_dedicated_session
    from session_capture import CaptureSession
    import modern_map_reader as reader

    assert 'COINGLASS_LOGIN_EMAIL' not in os.environ and 'COINGLASS_LOGIN_PASSWORD' not in os.environ
    session = CaptureSession.from_environment()
    assert session.dedicated
    page = OfficialFixture(session.state)
    context = Mock()
    def get_cookies(op, args):
        assert op == 'Network.getCookies' and page.authenticated and page.proof_images > 0
        return {'cookies': [COOKIE]}
    context.new_cdp_session.return_value.send.side_effect = get_cookies
    session.attach(context, page)
    target = MapTarget('BTC', 1, '12H')
    def select_range(page, name, value):
        assert (name, value) == ('Range', '12 hour')
        page.range = value
    def synthetic_measurement(page, symbol, model, timeframe, output, *, guard):
        guard()
        assert (symbol,model,timeframe) == ('BTC',1,'12H') and page.proof_images == 1
        # A real IPC checkpoint must have already returned its committed revision.
        assert session.state.get('cookies') == [COOKIE]
        return 'synthetic-measurement'
    with ExitStack() as stack:
        for name, value in {
            'choice_target': lambda p, name: name == 'Model 1',
            'choice': lambda p, name: name in ('Model 1','Coin'),
            'option_control': lambda p,name: Mock(inner_text=lambda: p.range),
            'select_option': select_range,
            'symbol_control': lambda p,symbol: symbol == 'BTC',
            'details': lambda p: {'source_time':'2026-01-01T00:00:00Z'},
            'rail_anchor': lambda p: '1.00', 'chart_box': lambda p: (0,0,480,240),
            'prefer_dark': lambda p: None,
        }.items():
            stack.enter_context(patch.object(reader, name, value))
        stack.enter_context(patch('dedicated_map_flow.read_selected_map', synthetic_measurement))
        value = collect_with_dedicated_session(session, context, page, target, Path('unused'))
    assert value == 'synthetic-measurement'
    if mode == 'first':
        assert page.submissions == page.form_opens == 1
        os._exit(46)  # Worker loss after checkpoint, before AI or a final frame.
    assert mode == 'reuse' and page.submissions == page.form_opens == 0
    session.finish(context)


class DedicatedIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_first_form_checkpoint_survives_analysis_crash_then_a_fresh_worker_reuses(self):
        from dedicated_auth_state import DedicatedSessionStore
        from session_runtime import ManagedSession, SessionError
        from test_dedicated_auth_state import DisposablePostgres, lock
        key = bytes(range(32))  # Synthetic per-test encryption key.
        env = {'COINGLASS_LOGIN_EMAIL': FIXTURE_EMAIL, 'COINGLASS_LOGIN_PASSWORD': FIXTURE_PASSWORD}
        with DisposablePostgres() as db, patch.dict(os.environ, env):
            db.initialize()
            for mode in ('first', 'reuse'):
                with db.connect() as conn:
                    self.assertTrue(lock(conn))
                    store = DedicatedSessionStore(conn,key,FIXTURE_EMAIL)
                    owner = ManagedSession(store, str(uuid4()))
                    await owner.begin()
                    self.assertEqual(owner.lease.state['cookies'], [] if mode == 'first' else [COOKIE])
                    transport = await owner.open_transport({'PATH': os.environ['PATH'],
                        'PYTHONPATH': os.pathsep.join(str(p) for p in sys.path if p), **env})
                    child = await asyncio.create_subprocess_exec(sys.executable, '-B', str(Path(__file__).resolve()), mode,
                        pass_fds=transport.pass_fds, env=transport.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    transport.spawned()
                    try:
                        stdout, stderr = await asyncio.wait_for(child.communicate(), 15)
                    finally:
                        if child.returncode is None:
                            child.kill()
                            await child.wait()
                        if mode == 'first':
                            with self.assertRaisesRegex(SessionError, 'session_transport_failed'):
                                await owner.finish(transport)
                        else:
                            await owner.finish(transport)
                    self.assertEqual(child.returncode, 46 if mode == 'first' else 0)
                    self.assertEqual((stdout,stderr),(b'',b''))
                    self.assertIsNone(owner.lease)
                    saved = conn.execute('SELECT status,run_id,encrypted_payload FROM collector_private.coinglass_dedicated_sessions').fetchone()
                    self.assertEqual(saved['status'], 'ready')
                    self.assertIsNone(saved['run_id'])
                    self.assertTrue(saved['encrypted_payload'])
                    for secret in (FIXTURE_EMAIL,FIXTURE_PASSWORD,COOKIE['value']):
                        self.assertNotIn(secret, saved['encrypted_payload'])
                    event = conn.execute('SELECT count(*) AS total, sum(attempts) AS attempts,'
                                         " bool_and(phase='verified') AS verified FROM collector_private.coinglass_dedicated_auth").fetchone()
                    self.assertEqual((event['total'],event['attempts'],event['verified']), (1,1,True))


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] in ('first','reuse'):
        run_child(sys.argv[1])
    else:
        unittest.main()
