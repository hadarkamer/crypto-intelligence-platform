"""Private owner-to-worker handoff for the existing CoinGlass session.

A chart adapter confirms the exact requested protected map before capture and
analysis. Public pages, account API responses and timeouts do not establish
that access was granted or revoked.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
import stat
import socket

import session_channel
import struct
from urllib.parse import urlsplit
from weakref import WeakKeyDictionary
from uuid import UUID

from model1_execution import StageFailure
from session_state import COOKIE_FIELDS, STATE_LIMIT, SessionStateError, validate_state

FRAME_LIMIT = STATE_LIMIT + 1024
ACCOUNT_URL = 'https://capi.coinglass.com/coin-community/api/userapi/info'
METADATA = ('COINGLASS_SESSION_RETURN_FD', 'COINGLASS_SESSION_RUN_ID',
            'COINGLASS_SESSION_REVISION')


_managed_pages = WeakKeyDictionary()


def note_protected_map_ready(page):
    """Called by the adapter after positive protected-target verification only."""
    session = _managed_pages.get(page)
    if session is not None:
        if page is session.page:
            session.verified = True
        elif page is session._proof_page:
            session._proof_url = page.url


def snapshot_coinglass(context, page, previous_state=None):
    """Read only cookies applicable to approved CoinGlass URLs, before close.

    Chromium's scoped cookie view preserves an absent SameSite. Playwright
    1.55 normalizes it to Lax. Firefox supplies its own scoped cookie view
    through BrowserContext.cookies. No account-provider profile is requested.
    """
    page_url = urlsplit(page.url)
    host = page_url.hostname
    if (page_url.scheme != 'https' or host not in ('www.coinglass.com', 'coinglass.com')
            or page_url.port not in (None, 443) or page_url.username or page_url.password):
        raise SessionStateError('session_snapshot_failed')
    urls = {
        f'https://{host}{page_url.path}', 'https://www.coinglass.com/',
        'https://capi.coinglass.com/coin-community/api/userapi/info',
        'https://fapi.coinglass.com/coin-community/api/user/web/session',
    }
    if previous_state and previous_state.get('cookies'):
        previous_state = validate_state(previous_state)
        urls.update('https://' + c['domain'].lstrip('.') + c['path']
                    for c in previous_state['cookies'])
    browser=getattr(context,'browser',None)
    if browser is not None and browser.browser_type.name=='firefox':
        native=context.cookies(sorted(urls))
    else:
        client=context.new_cdp_session(page)
        try:
            native=client.send('Network.getCookies',{'urls':sorted(urls)})['cookies']
        finally:
            client.detach()
    if not isinstance(native,list):
        raise SessionStateError('session_snapshot_failed')
    cookies=[]
    for item in native:
        domain=item['domain'].lstrip('.').lower()
        if domain!='coinglass.com' and not domain.endswith('.coinglass.com'):
            continue
        if item.get('partitionKey') is not None or item.get('partitionKeyOpaque') is True:
            raise SessionStateError('session_cookie_attributes_unsupported')
        cookies.append({key:item[key] for key in COOKIE_FIELDS if key in item})
    return validate_state({'cookies':cookies,'origins':[]})


class CaptureSession:
    def __init__(self, state, descriptor, run_id, revision):
        self.state, self.descriptor = state, descriptor
        self.run_id, self.revision = run_id, revision
        self.page = self.context = None
        self.verified = self.blocked = self.finished = False
        self.control = None
        self.dedicated = False
        self._proof_page = self._proof_url = None

    @classmethod
    def from_environment(cls):
        # Also protect direct legacy diagnostic processes from inheriting the
        # parent-only encryption key into a Playwright driver.
        for name in ('COINGLASS_SESSION_KEY', 'COINGLASS_LOGIN_EMAIL', 'COINGLASS_LOGIN_PASSWORD'):
            os.environ.pop(name, None)
        control_fd = os.environ.pop('COINGLASS_SESSION_CONTROL_FD', None)
        supplied = {name: os.environ.pop(name, None) for name in METADATA}
        raw_fd = supplied[METADATA[0]]
        if control_fd is None and all(value is None for value in supplied.values()):
            return None
        descriptor = control = None
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
                    or not 0 < int(raw_revision) < 2**63):
                raise ValueError()
            if control_fd is not None:
                if (not control_fd.isdecimal() or int(control_fd) <= 2 or int(control_fd) == descriptor
                        or raw_state is not None or old_header is not None
                        or not stat.S_ISSOCK(os.fstat(int(control_fd)).st_mode)):
                    raise ValueError()
                os.set_inheritable(int(control_fd), False)
                control = socket.socket(fileno=int(control_fd))
                if control.family != socket.AF_UNIX or control.type != socket.SOCK_STREAM:
                    raise ValueError()
                control.settimeout(12)
                session = cls({}, descriptor, run_id, int(raw_revision))
                session.control = control
                session.dedicated = True
                session.state = validate_state(session._request('load')['state'], allow_empty=True)
                return session
            if old_header != '' or not isinstance(raw_state, str) or len(raw_state.encode()) > FRAME_LIMIT:
                raise ValueError()
            state = validate_state(json.loads(raw_state), allow_empty=True)
            return cls(state, descriptor, run_id, int(raw_revision))
        except (OSError, ValueError, TypeError, KeyError, SessionStateError, StageFailure):
            session_channel.disconnect(control)
            if descriptor is not None:
                try: os.close(descriptor)
                except OSError: pass
            raise StageFailure('session_input_invalid') from None
        finally:
            raw_state = None
            for name in (*METADATA, 'COINGLASS_STORAGE_STATE_JSON', 'COINGLASS_COOKIE_HEADER'):
                os.environ.pop(name, None)

    def _request(self, operation, **fields):
        if not self.dedicated or self.control is None or self.finished:
            raise StageFailure('session_transport_failed')
        try:
            session_channel.send(self.control, dict(op=operation, revision=self.revision, **fields))
            response = session_channel.receive(self.control)
            if set(response) == {'error'} and isinstance(response['error'], str):
                raise StageFailure(response['error'])
            if set(response) != {'result'} or not isinstance(response['result'], dict):
                raise ValueError()
            return response['result']
        except (OSError, EOFError, ValueError, TypeError, SessionStateError):
            raise StageFailure('session_transport_failed') from None

    def source_cooldown(self, *, limited=False, retry_after_seconds=None):
        return self._request('source_cooldown', limited=limited, retry_after_seconds=retry_after_seconds)

    def auth_start(self, *, observed_code=None, retry_after_seconds=None):
        if observed_code is None and retry_after_seconds is None:
            return self._request('auth_start')
        return self._request('auth_start', observed_code=observed_code,
                             retry_after_seconds=retry_after_seconds)

    def credentials(self, incident_id):
        envelope = self._request('credentials', incident_id=incident_id)
        return envelope.pop('email'), envelope.pop('password')

    def auth_submit(self, incident_id):
        self._request('auth_submit', incident_id=incident_id)
        return True

    def auth_result(self, incident_id, code, retry_after_seconds=None):
        return self._request('auth_result', incident_id=incident_id, code=code,
                             retry_after_seconds=retry_after_seconds)

    def _snapshot(self, context, page):
        try:
            return snapshot_coinglass(context, page, self.state)
        except Exception:
            raise StageFailure('session_snapshot_failed') from None

    def state_changed(self, context):
        """Compare first-party cookie values and attributes without persisting them."""
        if not self.dedicated or self.finished or self.blocked or context is not self.context:
            raise StageFailure('session_observation_failed')
        snapshot = self._snapshot(context, self.page)

        def indexed(state):
            return {(c['domain'], c['path'], c['name'], c.get('partitionKey')): c
                    for c in state['cookies']}

        return indexed(self.state) != indexed(snapshot)

    @contextmanager
    def observe_proof(self, page):
        """Bind one temporary proof page in this worker's existing browser context."""
        if (not self.dedicated or self.finished or self.blocked or self.context is None
                or page.context is not self.context or page in _managed_pages
                or self._proof_page is not None):
            raise StageFailure('session_observation_failed')
        _managed_pages[page] = self
        self._proof_page, self._proof_url = page, None
        try:
            yield
        finally:
            _managed_pages.pop(page, None)
            self._proof_page = self._proof_url = None

    def checkpoint(self, context, incident_id=None, *, proof_page=None):
        if self.finished or self.blocked or context is not self.context:
            raise StageFailure('session_observation_failed')
        if proof_page is None:
            if not self.verified:
                raise StageFailure('session_observation_failed')
            page = self.page
        else:
            if proof_page is not self._proof_page or incident_id is not None or not self._proof_url:
                raise StageFailure('session_observation_failed')
            if proof_page.url != self._proof_url:
                self._proof_url = None
                raise StageFailure('session_observation_failed')
            page = proof_page
        state = self._snapshot(context, page)
        result = self._request('checkpoint', state=state, incident_id=incident_id)
        if type(result.get('revision')) is not int or result['revision'] <= self.revision:
            raise StageFailure('session_transport_failed')
        self.state = state
        self.revision = result['revision']

    def attach(self, context, page):
        if self.finished or self.page is not None:
            raise StageFailure('session_observation_failed')
        self.context, self.page = context, page
        _managed_pages[page] = self

    def note_source_error(self, error):
        if getattr(error, 'code', None) == 'source_login_required':
            # Access requires review; this does not diagnose token expiry.
            self.blocked = True

    def finish(self, context):
        if self.finished: return
        self.finished = True
        state = payload = frame = None
        failure = None
        outcome = 'blocked' if self.blocked else 'unconfirmed'
        try:
            if self.page is not None:
                _managed_pages.pop(self.page, None)
            if self._proof_page is not None:
                _managed_pages.pop(self._proof_page, None)
                self._proof_page = self._proof_url = None
            if not self.dedicated and self.verified and not self.blocked:
                try:
                    state = snapshot_coinglass(context, self.page, self.state)
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
            self.page = self.context = None
            frame = state = payload = None
            try: os.close(self.descriptor)
            except OSError: failure = failure or 'session_transport_failed'
            self.descriptor = None
            session_channel.disconnect(self.control)
            self.control = None
        if failure:
            # The work result and its independent session writeback are reported
            # separately. An unconfirmed snapshot never overwrites saved state.
            if failure == 'session_snapshot_failed':
                print('COINGLASS_SESSION session_snapshot_failed', flush=True)
            else:
                raise StageFailure(failure) from None

