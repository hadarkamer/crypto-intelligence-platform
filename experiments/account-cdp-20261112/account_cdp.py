"""Temporary account-request experiment; not imported by the collector.

Only the original page-created account GET can acquire the existing decoded
credential. CDP's per-request override is deliberately used instead of
Playwright route header overrides, which can persist across redirects.
No response body is requested, replaced, decoded or retained by this module.

Evidence is a conservative statement about the browser's network event, never
about account acceptance. Redirect chains and missing/ambiguous ExtraInfo are
not reported as verified. The fixture independently checks the mock server.
"""
from __future__ import annotations

import hmac
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'standalone_collector'))
from source_account_diagnostic import _session_value  # unchanged existing decoder

ACCOUNT_URL = 'https://capi.coinglass.com/coin-community/api/userapi/info'
PAGE_ORIGIN = 'https://www.coinglass.com'
ACCOUNT_PATH = '/coin-community/api/userapi/info'
_TECHNICAL_ID = re.compile(r'^[A-Za-z0-9._:-]{1,128}$')
_MAX_REQUESTS = 512
_MAX_EXTRA_INFO = 16
_MAX_PENDING = 8


def _origin(url):
    try:
        value = urlsplit(url)
        if value.username is not None or value.password is not None:
            return None
        if value.scheme not in ('http', 'https') or not value.hostname:
            return None
        port = value.port or (443 if value.scheme == 'https' else 80)
        return value.scheme, value.hostname, port
    except (TypeError, ValueError):
        return None


def _header(headers, name):
    """Consume an event locally; never retain its raw header dictionary."""
    if not isinstance(headers, dict):
        return None
    values = [value for key, value in headers.items()
              if isinstance(key, str) and key.lower() == name and value is not None]
    return values[0] if len(values) == 1 and isinstance(values[0], str) else None


class AccountHeaderExperiment:
    def __init__(self, page, environment, *, target_url=ACCOUNT_URL,
                 page_origin=PAGE_ORIGIN):
        # Alternate endpoints are fixtures only, never arbitrary external hosts.
        target = urlsplit(target_url)
        page_parts = urlsplit(page_origin)
        production = target_url == ACCOUNT_URL and page_origin == PAGE_ORIGIN
        fixture = (target.scheme == page_parts.scheme == 'http'
                   and target.hostname == page_parts.hostname == '127.0.0.1'
                   and target.port is not None and page_parts.port is not None)
        if (not (production or fixture) or target.path != ACCOUNT_PATH
                or target.query or target.fragment or page_parts.path
                or page_parts.query or page_parts.fragment
                or _origin(target_url) is None or _origin(page_origin) is None):
            raise ValueError('account_experiment_target_not_allowed')
        self._page = page
        self._target_url = target_url
        self._target_origin = _origin(target_url)
        self._page_origin = _origin(page_origin)
        self._origin_header = page_origin
        self._value = _session_value(environment)
        self._session = None
        self._frame_id = None
        self._records = {}
        self._paused = set()
        self._pending = {}
        self._listeners = []
        self._errors = []
        self._injections = 0
        self._closed = False
        self._fetch_enabled = False
        self._cleanup = {'paused_remaining': 0, 'listeners_removed': False,
                         'fetch_disabled': False, 'detached': False,
                         'reference_cleared': False}

    def _error(self, code):
        if code not in self._errors:
            self._errors.append(code)

    def _target(self, url):
        try:
            parsed = urlsplit(url)
            return (_origin(url) == self._target_origin
                    and parsed.path == ACCOUNT_PATH and not parsed.fragment)
        except (TypeError, ValueError):
            return False

    def _record(self, request_id):
        if not isinstance(request_id, str) or not _TECHNICAL_ID.fullmatch(request_id):
            self._error('network_id_unavailable')
            return None
        if request_id not in self._records:
            if len(self._records) >= _MAX_REQUESTS:
                self._error('measurement_limit')
                return None
            self._records[request_id] = {
                'network_id': request_id, 'hop_count': 0, 'target_seen': False,
                'original_target': False, 'page_request': False,
                'redirected': False, 'injected': False,
                'extra_info': [], 'terminal': False,
            }
        return self._records[request_id]

    def _on_request(self, event):
        record = self._record(event.get('requestId'))
        if record is None:
            return
        request = event.get('request', {})
        record['hop_count'] += 1
        redirect = bool(event.get('redirectResponse')) or record['hop_count'] > 1
        record['redirected'] = record['redirected'] or redirect
        target = self._target(request.get('url', ''))
        record['target_seen'] = record['target_seen'] or target
        if record['hop_count'] == 1:
            record['original_target'] = target and request.get('method') == 'GET'
            record['page_request'] = (
                event.get('frameId') == self._frame_id
                and event.get('type') in ('XHR', 'Fetch')
                and _origin(event.get('documentURL', '')) == self._page_origin
                and event.get('initiator', {}).get('type') == 'script'
            )
        # Chromium does not promise whether Network or Fetch arrives first.
        # Release only the pause belonging to this particular network ID.
        for fetch_id, paused in tuple(self._pending.items()):
            if paused.get('networkId') == event.get('requestId'):
                self._pending.pop(fetch_id)
                self._continue_paused(paused)

    def _on_extra_info(self, event):
        record = self._record(event.get('requestId'))
        if record is None:
            return
        if len(record['extra_info']) >= _MAX_EXTRA_INFO:
            self._error('measurement_limit')
            return
        headers = event.get('headers', {})
        values = ([value for name, value in headers.items()
                   if isinstance(name, str) and name.lower() == 'obe' and value is not None]
                  if isinstance(headers, dict) else [])
        value = values[0] if len(values) == 1 and isinstance(values[0], str) else None
        record['extra_info'].append({
            'header_present': bool(values),
            'header_matches': bool(value is not None and self._value is not None
                                   and value.isascii()
                                   and hmac.compare_digest(value, self._value)),
        })

    def _on_terminal(self, event):
        record = self._record(event.get('requestId'))
        if record is not None:
            record['terminal'] = True

    def _on_paused(self, event):
        request_id = event.get('requestId')
        if not isinstance(request_id, str) or not _TECHNICAL_ID.fullmatch(request_id):
            self._error('fetch_id_unavailable')
            return
        self._paused.add(request_id)
        request = event.get('request', {})
        record = self._records.get(event.get('networkId'))
        if (not self._closed and not self._errors and self._injections == 0
                and self._target(request.get('url', '')) and request.get('method') == 'GET'
                and event.get('resourceType') in ('XHR', 'Fetch')
                and not event.get('redirectedRequestId') and event.get('networkId')
                and (record is None or record['hop_count'] == 0)):
            if len(self._pending) < _MAX_PENDING:
                # Short-lived command input in memory, never evidence. It is
                # discarded as soon as its Network event arrives, or at close.
                self._pending[request_id] = event
                return
            self._error('pending_limit')
        self._continue_paused(event)

    def _continue_paused(self, event):
        request_id = event['requestId']
        try:
            request = event.get('request', {})
            headers = request.get('headers', {})
            record = self._records.get(event.get('networkId'))
            # In particular, an unknown ordering/provenance never permits an
            # injection. Do not infer "original" merely from a matching URL.
            eligible = (
                not self._closed and self._value is not None
                and not self._errors and self._injections == 0
                and record is not None and record['hop_count'] == 1
                and record['original_target'] and record['page_request']
                and not record['redirected'] and not event.get('redirectedRequestId')
                and self._target(request.get('url', ''))
                and request.get('method') == 'GET'
                and event.get('resourceType') in ('XHR', 'Fetch')
                and event.get('frameId') == self._frame_id
                and _origin(self._page.url) == self._page_origin
                and _header(headers, 'origin') == self._origin_header
                and isinstance(headers, dict)
                and not any(isinstance(name, str) and name.lower() == 'obe'
                            and value is not None for name, value in headers.items())
            )
            params = {'requestId': request_id}
            if eligible:
                # Keep every existing header untouched. A None placeholder is
                # absence, while an existing empty/string credential is kept.
                if not all(isinstance(key, str) and (value is None or isinstance(value, str))
                           for key, value in headers.items()):
                    self._error('header_shape_unavailable')
                else:
                    params['headers'] = [
                        {'name': key, 'value': value} for key, value in headers.items()
                        if value is not None
                    ] + [{'name': 'obe', 'value': self._value}]
                    # Count the attempt before sending: an uncertain command
                    # result must never cause a second injection or request.
                    self._injections += 1
            self._session.send('Fetch.continueRequest', params)
            if 'headers' in params:
                record['injected'] = True
            self._paused.discard(request_id)
        except Exception:
            self._error('request_continue_failed')
            try:
                self._session.send('Fetch.failRequest', {
                    'requestId': request_id, 'errorReason': 'Aborted'})
                self._paused.discard(request_id)
            except Exception:
                self._error('paused_request_abort_failed')

    def start(self):
        if self._session is not None or self._closed:
            raise RuntimeError('account_experiment_already_started')
        if self._value is None:
            raise ValueError('account_experiment_reference_unavailable')
        try:
            self._session = self._page.context.new_cdp_session(self._page)
            self._frame_id = self._session.send('Page.getFrameTree')['frameTree']['frame']['id']
            self._listeners = [
                ('Network.requestWillBeSent', self._on_request),
                ('Network.requestWillBeSentExtraInfo', self._on_extra_info),
                ('Network.loadingFinished', self._on_terminal),
                ('Network.loadingFailed', self._on_terminal),
                ('Fetch.requestPaused', self._on_paused),
            ]
            for name, callback in self._listeners:
                self._session.on(name, callback)
            self._session.send('Network.enable')
            # A narrow pause pattern, plus the strict predicate above. Maps and
            # unrelated URLs are not paused by this experimental CDP session.
            self._session.send('Fetch.enable', {'patterns': [{
                'urlPattern': self._target_url + '*', 'requestStage': 'Request'}]})
            self._fetch_enabled = True
        except Exception:
            self._error('setup_failed')
            self.close()
            raise RuntimeError('account_experiment_setup_failed') from None
        return self

    def evidence(self):
        requests = []
        for record in self._records.values():
            if not (record['target_seen'] or record['injected']):
                continue
            exact = (record['injected'] and record['hop_count'] == 1
                     and not record['redirected'] and record['terminal']
                     and len(record['extra_info']) == 1 and not self._errors)
            extra = record['extra_info'][0] if exact else None
            requests.append({
                'network_id': record['network_id'], 'hop_count': record['hop_count'],
                'injected': record['injected'], 'redirected': record['redirected'],
                'extra_info_count': len(record['extra_info']),
                'send_state': 'verified' if exact else 'not_verified',
                'header_present': extra['header_present'] if extra else None,
                'header_matches': extra['header_matches'] if extra else None,
            })
        verified = [item for item in requests if item['send_state'] == 'verified']
        unique = verified[0] if len(verified) == 1 else None
        return {
            'injections': self._injections,
            'send_state': 'verified' if unique else 'not_verified',
            'header_present': unique['header_present'] if unique else None,
            'header_matches': unique['header_matches'] if unique else None,
            'requests': requests, 'errors': list(self._errors),
            'cleanup': dict(self._cleanup),
        }

    def close(self):
        if self._closed:
            return
        self._closed = True
        session = self._session
        if session is not None:
            for request_id in tuple(self._paused):
                try:
                    session.send('Fetch.failRequest', {
                        'requestId': request_id, 'errorReason': 'Aborted'})
                    self._paused.discard(request_id)
                except Exception:
                    self._error('cleanup_abort_failed')
            try:
                session.send('Fetch.disable')
                self._fetch_enabled = False
                self._cleanup['fetch_disabled'] = True
            except Exception:
                self._error('cleanup_disable_failed')
            removed = True
            for name, callback in self._listeners:
                try:
                    session.remove_listener(name, callback)
                except Exception:
                    removed = False
                    self._error('cleanup_listener_failed')
            self._listeners.clear()
            self._cleanup['listeners_removed'] = removed
            try:
                session.detach()
                self._cleanup['detached'] = True
            except Exception:
                self._error('cleanup_detach_failed')
        self._cleanup['paused_remaining'] = len(self._paused)
        self._pending.clear()
        self._value = None
        self._cleanup['reference_cleared'] = True
