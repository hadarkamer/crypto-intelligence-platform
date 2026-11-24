"""Private owner-to-worker handoff for the existing CoinGlass session.

A chart adapter confirms the exact requested protected map before capture and
analysis. Public pages, account API responses and timeouts do not establish
that access was granted or revoked.
"""
from __future__ import annotations

import fcntl
import json
import os
import stat
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
        session.verified = True


def snapshot_coinglass(context, page, previous_state=None):
    """Read only cookies applicable to approved CoinGlass URLs, before close.

    Chromium's scoped cookie view preserves an absent SameSite. Playwright
    1.55 normalizes it to Lax. No account-provider profile is requested.
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
    client = context.new_cdp_session(page)
    try:
        native = client.send('Network.getCookies', {'urls': sorted(urls)})['cookies']
        if not isinstance(native, list):
            raise SessionStateError('session_snapshot_failed')
        cookies = []
        for item in native:
            domain = item['domain'].lstrip('.').lower()
            if domain != 'coinglass.com' and not domain.endswith('.coinglass.com'):
                continue
            if item.get('partitionKey') is not None or item.get('partitionKeyOpaque') is True:
                raise SessionStateError('session_cookie_attributes_unsupported')
            cookies.append({key: item[key] for key in COOKIE_FIELDS if key in item})
        return validate_state({'cookies': cookies, 'origins': []})
    finally:
        client.detach()


class CaptureSession:
    def __init__(self, state, descriptor, run_id, revision):
        self.state, self.descriptor = state, descriptor
        self.run_id, self.revision = run_id, revision
        self.page = self.context = None
        self.verified = self.blocked = self.finished = False

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
            state = validate_state(json.loads(raw_state), allow_empty=True)
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
            if self.verified and not self.blocked:
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
        if failure:
            # The work result and its independent session writeback are reported
            # separately. An unconfirmed snapshot never overwrites saved state.
            if failure == 'session_snapshot_failed':
                print('COINGLASS_SESSION session_snapshot_failed', flush=True)
            else:
                raise StageFailure(failure) from None
