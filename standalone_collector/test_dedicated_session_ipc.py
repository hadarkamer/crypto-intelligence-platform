"""Offline worker-process acceptance of the dedicated private channel.

The source, browser and database are replaced here. No provider request is made.
The separate state tests cover the actual SQL fence and CAS.
"""
import asyncio
from copy import deepcopy
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

from session_runtime import ManagedSession, SessionError
from session_state import Lease

HERE = Path(__file__).resolve().parent
INCIDENT = '04896351-6d79-4e38-8576-bb0aa56a4f14'
EMPTY = {'cookies': [], 'origins': []}
COOKIE = {'name': 'sample', 'value': 'synthetic', 'domain': '.coinglass.com',
          'path': '/', 'secure': True, 'expires': -1, 'httpOnly': True}
FIXTURE = {'COINGLASS_LOGIN_EMAIL': 'fixture@example.invalid',
           'COINGLASS_LOGIN_PASSWORD': 'dummy-test-only'}


class Store:
    def __init__(self):
        self.state = deepcopy(EMPTY)
        self.events = []
        self.submitted = False
        self.revision = 0
        self.cooldown = None

    def begin(self, run_id, **flags):
        self.revision += 1
        return Lease(self.revision, run_id, deepcopy(self.state))

    def source_cooldown(self, lease, *, limited=False, retry_after_seconds=None):
        if limited:
            self.cooldown = retry_after_seconds or 60
            self.events.append('map-cooldown')
        return {'code': 'source_rate_limited' if self.cooldown else 'allowed',
                'retry_after_seconds': self.cooldown}

    def auth_start(self, lease, *, observed_code=None, retry_after_seconds=None):
        if observed_code is not None:
            self.events.append((observed_code, retry_after_seconds))
            return {'code': observed_code, 'incident_id': INCIDENT}
        self.events.append('gate')
        return {'code': 'allowed', 'incident_id': INCIDENT}

    def auth_submit(self, lease, incident):
        if self.submitted:
            raise SessionError('state_revision_conflict')
        self.submitted = True
        self.events.append('fence')

    def auth_result(self, lease, incident, code, retry_after_seconds=None):
        self.events.append(code)
        return {'code': code, 'incident_id': incident}

    def checkpoint(self, lease, state, **kwargs):
        if lease.revision != self.revision:
            raise SessionError('state_revision_conflict')
        self.revision += 1
        self.state = deepcopy(state)
        self.events.append('checkpoint')
        return Lease(self.revision, lease.run_id, deepcopy(state))

    def complete(self, lease, outcome, state):
        assert lease.revision == self.revision
        self.events.append('release')


class IPC(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, store, mode, *, crash=False):
        session = ManagedSession(store, str(uuid4()))
        await session.begin()
        env = {'PATH': os.environ['PATH'], 'PYTHONPATH': os.pathsep.join(map(str, sys.path)),
               'COINGLASS_COOKIE_HEADER': 'legacy-fixture', 'COINGLASS_STORAGE_STATE_JSON': 'old-fixture',
               'COINGLASS_SESSION_KEY': 'fake', 'DATABASE_URL': 'dummy', 'SERVICE_TOKEN': 'dummy',
               **FIXTURE}
        with patch.dict(os.environ, FIXTURE):
            transport = await session.open_transport(env)
            child = await asyncio.create_subprocess_exec(
                sys.executable, '-B', str(Path(__file__).resolve()), mode,
                env=transport.env, pass_fds=transport.pass_fds,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            transport.spawned()
            try:
                stdout, stderr = await asyncio.wait_for(child.communicate(), 6)
            finally:
                if child.returncode is None:
                    child.kill()
                    await child.wait()
                if crash:
                    with self.assertRaisesRegex(SessionError, 'session_transport_failed'):
                        await session.finish(transport)
                else:
                    await session.finish(transport)
            self.assertEqual(child.returncode, 91 if crash else 0)
            self.assertEqual((stdout, stderr), (b'', b''))
            self.assertTrue(transport.control_task.done())
            self.assertIsNone(session.lease)

    async def test_fenced_form_checkpoint_survives_crash_then_fresh_process_reuses(self):
        store = Store()
        await self.exercise(store, 'login-crash', crash=True)
        await self.exercise(store, 'reuse')
        self.assertEqual(store.events, ['gate', 'fence', 'checkpoint', 'release', 'checkpoint', 'release'])

    async def test_cookie_rotation_needs_distinct_protected_observation_and_never_authenticates(self):
        store = Store()
        other = dict(COOKIE, name='auxiliary')
        store.state = {'cookies': [COOKIE, other], 'origins': []}
        await self.exercise(store, 'rotate')
        self.assertEqual(store.events, ['checkpoint', 'release'])
        self.assertEqual(store.state['cookies'], [other, dict(COOKIE, value='rotated')])

    async def test_challenge_closes_credential_access_without_checkpoint(self):
        store = Store()
        await self.exercise(store, 'challenge')
        self.assertEqual(store.events, ['gate', 'auth_challenge', 'release'])

    async def test_map_rate_limit_survives_new_worker_without_touching_auth(self):
        store = Store()
        await self.exercise(store, 'map-limit')
        await self.exercise(store, 'map-cooldown')
        self.assertEqual(store.events, ['map-cooldown', 'release', 'release'])

    async def test_observed_rate_limit_never_releases_credentials(self):
        store = Store()
        await self.exercise(store, 'rate-limit')
        self.assertEqual(store.events, [('auth_rate_limited', 23), 'release'])

    async def test_source_proof_marker_alone_does_not_persist_state(self):
        store = Store()
        await self.exercise(store, 'no-checkpoint')
        self.assertEqual(store.events, ['release'])
        self.assertEqual(store.state, EMPTY)

    def test_modern_without_managed_store_cannot_use_legacy_runtime(self):
        with patch.dict(os.environ, {'COINGLASS_MODERN_UI_ENABLED': 'true'}, clear=True):
            with self.assertRaisesRegex(SessionError, 'session_configuration_invalid'):
                ManagedSession.from_environment(None, str(uuid4()))


def child(mode):
    from types import SimpleNamespace
    from model1_execution import StageFailure
    from session_capture import CaptureSession, note_protected_map_ready

    class Page:
        url = 'https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'

    class Context:
        def __init__(self):
            self.cookies = [COOKIE]
            self.last_page = None

        def new_cdp_session(self, page):
            self.last_page = page
            return SimpleNamespace(send=lambda method, params: {'cookies': self.cookies}, detach=lambda: None)

    def denied(call):
        try:
            call()
        except StageFailure:
            return
        raise AssertionError('missing fence')

    assert not any(name in os.environ for name in (*FIXTURE, 'COINGLASS_COOKIE_HEADER',
                   'COINGLASS_STORAGE_STATE_JSON', 'DATABASE_URL', 'SERVICE_TOKEN', 'COINGLASS_SESSION_KEY'))
    session = CaptureSession.from_environment()
    assert session.dedicated and not os.get_inheritable(session.control.fileno())
    page, ctx = Page(), Context()
    session.attach(ctx, page)
    denied(lambda: session.credentials(INCIDENT))
    if mode == 'login-crash':
        assert session.state == EMPTY
        event = session.auth_start()
        assert event['code'] == 'allowed'
        denied(lambda: session.checkpoint(ctx, event['incident_id']))
        denied(lambda: session.auth_submit(event['incident_id']))
        credentials = session.credentials(event['incident_id'])
        assert credentials == tuple(FIXTURE.values())
        del credentials
        denied(lambda: session.credentials(event['incident_id']))
        assert session.auth_submit(event['incident_id'])
        note_protected_map_ready(page)
        session.checkpoint(ctx, event['incident_id'])
        os._exit(91)  # Child dies before an independent analysis/final receipt.
    if mode == 'reuse':
        assert session.state['cookies'] == [COOKIE]
        note_protected_map_ready(page)
        session.checkpoint(ctx)
    if mode == 'rotate':
        other = dict(COOKIE, name='auxiliary')
        ctx.cookies = [other, COOKIE]  # Chromium is allowed to reorder cookies.
        assert not session.state_changed(ctx)
        ctx.cookies = [other, dict(COOKIE, httpOnly=False)]
        assert session.state_changed(ctx)
        ctx.cookies = [other, dict(COOKIE, value='rotated')]
        assert session.state_changed(ctx)
        proof = Page()
        proof.context = ctx
        foreign = Page()
        foreign.context = Context()
        denied(lambda: session.observe_proof(foreign).__enter__())
        with session.observe_proof(proof):
            # Neither a public/unknown auxiliary page nor a marker for the original
            # page can authorize the auxiliary checkpoint.
            note_protected_map_ready(page)
            denied(lambda: session.checkpoint(ctx, proof_page=proof))
            note_protected_map_ready(proof)
            proof.url = 'https://www.coinglass.com/'
            denied(lambda: session.checkpoint(ctx, proof_page=proof))
            proof.url = page.url
            denied(lambda: session.checkpoint(ctx, proof_page=proof))
            note_protected_map_ready(proof)
            session.checkpoint(ctx, proof_page=proof)
            assert ctx.last_page is proof
            assert not session.state_changed(ctx)
        denied(lambda: session.checkpoint(ctx, proof_page=proof))
    if mode == 'no-checkpoint':
        note_protected_map_ready(page)
    if mode == 'map-limit':
        assert session.source_cooldown()['code'] == 'allowed'
        assert session.source_cooldown(limited=True, retry_after_seconds=24)['retry_after_seconds'] == 24
    if mode == 'map-cooldown':
        assert session.source_cooldown() == {'code': 'source_rate_limited', 'retry_after_seconds': 24}
        denied(lambda: session.credentials(INCIDENT))
    if mode == 'rate-limit':
        event = session.auth_start(observed_code='auth_rate_limited', retry_after_seconds=23)
        assert event['code'] == 'auth_rate_limited'
        denied(lambda: session.credentials(event['incident_id']))
    if mode == 'challenge':
        event = session.auth_start()
        response = session.auth_result(event['incident_id'], 'auth_challenge')
        assert response['code'] == 'auth_challenge'
        denied(lambda: session.credentials(event['incident_id']))
    session.finish(ctx)


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] in ('login-crash', 'reuse', 'no-checkpoint', 'challenge', 'rate-limit', 'map-limit', 'map-cooldown', 'rotate'):
        child(sys.argv[1])
    else:
        unittest.main()
