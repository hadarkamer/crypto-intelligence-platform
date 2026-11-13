"""Managed browser-session handoff; no account requests, files or raw logging.

Only a naturally completed, exact account response is read. Playwright returns
its whole body in memory; only the outer success boolean is inspected. This is
not a byte limit on the page's natural download, nor proof of session renewal.
"""
from __future__ import annotations

import fcntl
import json
import os
import stat
import struct
from urllib.parse import urlsplit
from uuid import UUID

from model1_execution import StageFailure
from session_state import STATE_LIMIT, SessionStateError, validate_state

FRAME_LIMIT = STATE_LIMIT + 1024
ACCOUNT_URL = 'https://capi.coinglass.com/coin-community/api/userapi/info'
METADATA = ('COINGLASS_SESSION_RETURN_FD', 'COINGLASS_SESSION_RUN_ID',
            'COINGLASS_SESSION_REVISION')


class _Envelope(list):
    pass


class CaptureSession:
    def __init__(self, state, descriptor, run_id, revision):
        self.state, self.descriptor = state, descriptor
        self.run_id, self.revision = run_id, revision
        self.page = self.context = self.latest = None
        self.success = None
        self.pending = self.blocked = self.finished = False
        self.listeners = []

    @classmethod
    def from_environment(cls):
        # Also protect direct legacy diagnostic processes from inheriting the
        # parent-only encryption key into a Playwright driver.
        os.environ.pop('COINGLASS_SESSION_KEY', None)
        supplied = {name: os.environ.pop(name, None) for name in METADATA}
        raw_fd = supplied[METADATA[0]]
        if all(value is None for value in supplied.values()):
            return None
        descriptor = None
        raw_state = None
        try:
            run_id = supplied[METADATA[1]]
            raw_revision = supplied[METADATA[2]]
            raw_state = os.environ.pop('COINGLASS_STORAGE_STATE_JSON', None)
            old_header = os.environ.pop('COINGLASS_COOKIE_HEADER', None)
            if not isinstance(raw_fd, str) or not raw_fd.isdecimal() or int(raw_fd) <= 2:
                raise ValueError()
            descriptor = int(raw_fd)
            if (not stat.S_ISFIFO(os.fstat(descriptor).st_mode)
                    or fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY):
                raise ValueError()
            flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
            fcntl.fcntl(descriptor, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
            if (not isinstance(run_id, str) or not isinstance(raw_revision, str)
                    or str(UUID(run_id)) != run_id or not raw_revision.isdecimal()
                    or not 0 < int(raw_revision) < 2**63 or old_header != ''
                    or not isinstance(raw_state, str) or len(raw_state.encode()) > FRAME_LIMIT):
                raise ValueError()
            state = validate_state(json.loads(raw_state))
            return cls(state, descriptor, run_id, int(raw_revision))
        except (OSError, ValueError, TypeError, SessionStateError):
            if descriptor is not None:
                try: os.close(descriptor)
                except OSError: pass
            raise StageFailure('session_input_invalid') from None
        finally:
            raw_state = None
            for name in (*METADATA, 'COINGLASS_STORAGE_STATE_JSON', 'COINGLASS_COOKIE_HEADER'):
                os.environ.pop(name, None)

    def _eligible(self, request):
        try:
            target, frame = urlsplit(request.url), urlsplit(request.frame.url)
            # Match the existing passive observer. Natural query parameters
            # are neither interpreted nor retained, and no request is created.
            return (target.scheme == 'https' and target.hostname == 'capi.coinglass.com'
                    and target.port in (None, 443) and target.username is None and target.password is None
                    and target.path == '/coin-community/api/userapi/info' and not target.fragment
                    and request.method == 'GET'
                    and request.resource_type in ('xhr', 'fetch')
                    and request.redirected_from is None
                    and request.frame == self.page.main_frame
                    and frame.scheme == 'https' and frame.hostname == 'www.coinglass.com'
                    and frame.port in (None, 443) and frame.username is None and frame.password is None)
        except Exception:
            return False

    def attach(self, context, page):
        if self.finished or self.page is not None:
            raise StageFailure('session_observation_failed')
        self.context, self.page = context, page
        self.listeners = [('request', self._requested), ('requestfinished', self._completed),
                          ('requestfailed', self._failed)]
        try:
            for event, callback in self.listeners:
                page.on(event, callback)
        except Exception:
            raise StageFailure('session_observation_failed') from None

    def _requested(self, request):
        if self.finished or not self._eligible(request): return
        self.latest, self.success, self.pending = request, None, True

    def _failed(self, request):
        if self.finished or request is not self.latest: return
        self.success, self.pending = None, False

    def _completed(self, request):
        if self.finished or request is not self.latest: return
        self.pending, self.success = False, None
        body = payload = response = None
        try:
            if request.redirected_to is not None: return
            response = request.response()
            if response is None or not 200 <= response.status < 300: return
            body = response.body()
            if not isinstance(body, bytes) or len(body) > 2 * 1024 * 1024: return
            payload = json.loads(body, object_pairs_hook=_Envelope)
            if not isinstance(payload, _Envelope): return
            values = [value for name, value in payload if name == 'success']
            if len(values) == 1 and type(values[0]) is bool:
                self.success = values[0]
        except Exception:
            self.success = None
        finally:
            body = payload = response = None

    def note_source_error(self, error):
        if getattr(error, 'code', None) == 'source_login_required':
            # Access requires review; this does not diagnose token expiry.
            self.blocked = True

    def _snapshot(self, context):
        urls = sorted({'https://' + cookie['domain'].lstrip('.') + cookie['path']
                       for cookie in self.state['cookies']} | {
            'https://www.coinglass.com/pro/futures/LiquidationHeatMap', ACCOUNT_URL,
            'https://fapi.coinglass.com/coin-community/api/user/web/session'})
        cookies = context.cookies(urls=urls)
        # Natural renewal can introduce a new cookie name or switch from a
        # parent-domain cookie to a host-only cookie. Keep valid CoinGlass
        # cookies returned by these scoped URLs, not only initial identities.
        def coinglass(cookie):
            domain = cookie.get('domain')
            host = domain.lstrip('.').lower() if isinstance(domain, str) else ''
            return host == 'coinglass.com' or host.endswith('.coinglass.com')
        return validate_state({'cookies': [cookie for cookie in cookies if coinglass(cookie)],
                               'origins': []})

    def finish(self, context):
        if self.finished: return
        self.finished = True
        state = payload = frame = None
        failure = None
        outcome = 'blocked' if self.blocked or self.success is False else 'unconfirmed'
        try:
            for event, callback in self.listeners:
                try: self.page.remove_listener(event, callback)
                except Exception: failure = 'session_observation_failed'
            if not failure and not self.blocked and not self.pending and self.success is True:
                try:
                    state = self._snapshot(context)
                    outcome = 'ready'
                except Exception:
                    failure = 'session_snapshot_failed'
            payload = {'run_id': self.run_id, 'revision': self.revision, 'outcome': outcome, 'state': state}
            frame = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode()
            if len(frame) > FRAME_LIMIT:
                state = None; outcome = 'unconfirmed'; failure = 'session_snapshot_failed'
                payload.update(outcome=outcome, state=None)
                frame = json.dumps(payload, separators=(',', ':')).encode()
            packet = memoryview(struct.pack('!I', len(frame)) + frame)
            while packet:
                written = os.write(self.descriptor, packet)
                if written <= 0: raise OSError()
                packet = packet[written:]
        except Exception:
            failure = 'session_transport_failed'
        finally:
            if isinstance(state, dict): state.clear()
            if isinstance(payload, dict): payload.clear()
            self.state.clear()
            self.latest = self.page = self.context = None
            self.listeners.clear()
            frame = state = payload = None
            try: os.close(self.descriptor)
            except OSError: failure = failure or 'session_transport_failed'
            self.descriptor = None
        if failure:
            raise StageFailure(failure) from None
