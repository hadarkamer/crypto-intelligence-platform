"""Parent-owned persistence and bounded private communication with the map worker.

The owner holds the existing WORK_LOCK. The dedicated worker requests state,
credentials and database acknowledgments over an inherited Unix socket.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import select
import socket

import session_channel
import struct
import threading

from session_state import PgSessionStore, SessionStateError, STATE_LIMIT

SessionError = SessionStateError
MAX_FRAME = STATE_LIMIT + 1024
KEY_ENV = 'COINGLASS_SESSION_KEY'
ENABLED_ENV = 'COINGLASS_SESSION_STORE_ENABLED'
CHILD_KEYS = ('COINGLASS_SESSION_RETURN_FD', 'COINGLASS_SESSION_RUN_ID',
              'COINGLASS_SESSION_REVISION', 'COINGLASS_SESSION_CONTROL_FD')
CREDENTIAL_ENV = ('COINGLASS_LOGIN_EMAIL', 'COINGLASS_LOGIN_PASSWORD')
# The task calls the existing AI scanner after Chromium exits. Only its own
# analysis key and the OS/browser runtime settings are needed in this process.
WORKER_ENV = frozenset((
    'PATH', 'HOME', 'TMPDIR', 'LANG', 'LANGUAGE', 'LC_ALL', 'LC_CTYPE', 'TZ',
    'LD_LIBRARY_PATH', 'VIRTUAL_ENV', 'PYTHONPATH', 'PYTHONHOME', 'PYTHONDONTWRITEBYTECODE',
    'PLAYWRIGHT_BROWSERS_PATH', 'XDG_CACHE_HOME', 'XDG_RUNTIME_DIR', 'FONTCONFIG_PATH',
    'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE', 'NODE_EXTRA_CA_CERTS',
    'OPENAI_API_KEY', 'OPENAI_MARKET_SCANNER_MODEL', 'COLLECTION_HEATMAP_MODEL',
    'COLLECTION_HEATMAP_SYMBOL', 'COINGLASS_MODERN_UI_ENABLED',
))


def _read_chunk(fd, count, stop):
    while not stop.is_set():
        if select.select([fd], [], [], 0.1)[0]:
            return os.read(fd, count)
    raise SessionError('session_transport_failed')


def _read_exact(fd, count, stop):
    parts = []
    while count:
        chunk = _read_chunk(fd, count, stop)
        if not chunk:
            raise SessionError('session_transport_failed')
        parts.append(chunk)
        count -= len(chunk)
    return b''.join(parts)


def _read_frame(fd, stop):
    raw = None
    try:
        size = struct.unpack('!I', _read_exact(fd, 4, stop))[0]
        if not 1 <= size <= MAX_FRAME:
            raise SessionError('session_transport_failed')
        raw = _read_exact(fd, size, stop)
        if _read_chunk(fd, 1, stop):
            raise SessionError('session_transport_failed')
        return json.loads(raw)
    except SessionError:
        raise
    except Exception:
        raise SessionError('session_transport_failed') from None
    finally:
        raw = None
        os.close(fd)


class SessionTransport:
    def __init__(self, environment, lease, owner=None):
        self.owner = owner
        self.control = self.peer = None
        self.control_task = None
        self.active_incident = None
        self.credentials_sent = False
        self.env = (dict(environment) if owner is None else
                    {key: environment[key] for key in WORKER_ENV if key in environment})
        # The database writer/key remain in the parent. Neither is needed by
        # the browser/AI task. A legacy header can never override managed state.
        for key in (KEY_ENV, 'DATABASE_URL', ENABLED_ENV, *CHILD_KEYS, *CREDENTIAL_ENV):
            self.env.pop(key, None)
        encoded = json.dumps(lease.state, separators=(',', ':'), allow_nan=False)
        # Reserve room for the fixed receipt envelope and its run identity.
        if len(encoded.encode()) > STATE_LIMIT:
            self.env.clear()
            raise SessionError('session_input_invalid')
        self.read_fd, self.write_fd = os.pipe2(os.O_CLOEXEC)
        self.pass_fds = (self.write_fd,)
        self.env.update(
            COINGLASS_SESSION_RETURN_FD=str(self.write_fd),
            COINGLASS_SESSION_RUN_ID=lease.run_id,
            COINGLASS_SESSION_REVISION=str(lease.revision),
        )
        if owner is None:
            self.env.update(COINGLASS_COOKIE_HEADER='', COINGLASS_STORAGE_STATE_JSON=encoded)
        else:
            try:
                self.control, self.peer = socket.socketpair()
                self.env['COINGLASS_SESSION_CONTROL_FD'] = str(self.peer.fileno())
                self.pass_fds += (self.peer.fileno(),)
                self.control_task = asyncio.create_task(asyncio.to_thread(self._serve))
            except BaseException:
                self.env.clear()
                os.close(self.read_fd)
                os.close(self.write_fd)
                session_channel.disconnect(self.control)
                session_channel.disconnect(self.peer)
                raise
        self.stop = threading.Event()
        self.reader = asyncio.create_task(asyncio.to_thread(_read_frame, self.read_fd, self.stop))
        self.closed = False

    def _dispatch(self, message):
        owner, lease = self.owner, self.owner.lease
        op = message.pop('op', None)
        revision = message.pop('revision', None)
        if type(revision) is not int or revision != lease.revision:
            raise SessionError('state_revision_conflict')
        if op == 'load' and not message:
            return {'state': lease.state}
        if op == 'source_cooldown' and set(message) == {'limited', 'retry_after_seconds'}:
            if type(message['limited']) is not bool:
                raise SessionError('session_transport_failed')
            return owner.store.source_cooldown(lease, **message)
        if op == 'auth_start' and set(message) in (set(), {'observed_code', 'retry_after_seconds'}):
            observed = message.get('observed_code')
            if message and observed not in ('auth_challenge', 'auth_code_required', 'auth_rate_limited', 'auth_invalid_credentials'):
                raise SessionError('session_transport_failed')
            result = owner.store.auth_start(lease, **message)
            self.credentials_sent = False
            self.active_incident = result['incident_id'] if result['code'] == 'allowed' and not observed else None
            if observed and result['code'] == 'allowed':
                raise SessionError('session_transport_failed')
            return result
        incident = message.pop('incident_id', None)
        if op == 'checkpoint' and set(message) == {'state'}:
            if incident != self.active_incident:
                raise SessionError('state_revision_conflict')
            new = owner.store.checkpoint(lease, message['state'], incident_id=incident)
            owner.lease = new
            self.active_incident = None
            return {'revision': new.revision}
        if not self.active_incident or incident != self.active_incident:
            raise SessionError('state_revision_conflict')
        if op == 'credentials' and not message:
            if self.credentials_sent:
                raise SessionError('state_revision_conflict')
            self.credentials_sent = True
            email, password = (os.environ.get(key, '') for key in CREDENTIAL_ENV)
            if len(email) > 320 or len(password) > 4096:
                raise SessionError('auth_configuration_invalid')
            return {'email': email, 'password': password}
        if op == 'auth_submit' and not message:
            if not self.credentials_sent:
                raise SessionError('state_revision_conflict')
            owner.store.auth_submit(lease, incident)
            return {}
        if op == 'auth_result' and set(message) == {'code', 'retry_after_seconds'}:
            result = owner.store.auth_result(lease, incident, message['code'],
                                           retry_after_seconds=message['retry_after_seconds'])
            self.active_incident = None
            return result
        raise SessionError('session_transport_failed')

    def _serve(self):
        # The child has a deadline for each response and the job manager owns
        # its overall deadline. Closing this socket wakes an idle reader.
        try:
            for _ in range(32):
                message = session_channel.receive(self.control)
                result = None
                try:
                    result = self._dispatch(message)
                    session_channel.send(self.control, {'result': result})
                except SessionError as error:
                    session_channel.send(self.control, {'error': error.code})
                finally:
                    if result is not None:
                        result.clear()
                    message.clear()
        except (EOFError, OSError, ValueError, TypeError, KeyError, SessionError):
            pass
        finally:
            session_channel.disconnect(self.control)

    def spawned(self):
        """Call immediately after spawn; only the child retains the write end."""
        if self.write_fd is not None:
            os.close(self.write_fd)
            self.write_fd = None
        if self.peer is not None:
            # The child shares the same endpoint. shutdown() here would tear
            # down its communication before the first request.
            self.peer.close()
            self.peer = None
        self.env.clear()

    async def receive(self):
        if self.closed:
            raise SessionError('session_transport_failed')
        self.closed = True
        self.spawned()  # Also handles a failed spawn without a dangling writer.
        try:
            # The child has already terminated and the FD is CLOEXEC before
            # Chromium starts. EOF must be immediate; never wait indefinitely.
            return await asyncio.wait_for(asyncio.shield(self.reader), timeout=2)
        except SessionError:
            raise
        except Exception:
            raise SessionError('session_transport_failed') from None
        finally:
            # A retained writer or cancellation cannot strand a blocking read.
            self.stop.set()
            if self.control_task is not None:
                try:
                    self.control.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                await asyncio.shield(self.control_task)
            if not self.reader.done():
                try:
                    await asyncio.shield(self.reader)
                except (Exception, asyncio.CancelledError):
                    pass


class ManagedSession:
    def __init__(self, store, run_id):
        self.store, self.run_id = store, str(run_id)
        self.lease = None
        self.finished = False

    @classmethod
    def from_environment(cls, connection, run_id):
        modern = os.environ.get('COINGLASS_MODERN_UI_ENABLED', '').lower() == 'true'
        if os.environ.get(ENABLED_ENV, '').lower() != 'true':
            if modern:
                raise SessionError('session_configuration_invalid')
            return None
        key = None
        try:
            key = base64.b64decode(os.environ.get(KEY_ENV, ''), validate=True)
            if len(key) != 32:
                raise ValueError()
            if modern:
                from dedicated_auth_state import DedicatedSessionStore
                store = DedicatedSessionStore(connection, key, os.environ.get(CREDENTIAL_ENV[0], ''))
            else:
                store = PgSessionStore(connection, key)
            return cls(store, run_id)
        except SessionError:
            raise
        except Exception:
            raise SessionError('session_configuration_invalid') from None
        finally:
            key = None

    async def begin(self):
        if self.lease is not None or self.finished:
            raise SessionError('session_conflict')
        from collection_metrics import logged_phase
        with logged_phase(self.run_id, 'session_load_ms'):
            self.lease = await asyncio.to_thread(self.store.begin, self.run_id,
                                             recover_orphan=True, allow_initial_probe=True)

    async def open_transport(self, environment):
        if self.lease is None or self.finished:
            raise SessionError('session_conflict')
        owner = self if callable(getattr(self.store, 'auth_start', None)) else None
        return SessionTransport(environment, self.lease, owner)

    async def finish(self, transport):
        if self.finished:
            return
        self.finished = True
        if self.lease is None:
            return
        receipt = state = None
        outcome = 'unconfirmed'
        failure = None
        try:
            if transport is None:
                raise SessionError('session_transport_failed')
            receipt = await transport.receive()
            if (not isinstance(receipt, dict)
                    or set(receipt) != {'run_id', 'revision', 'outcome', 'state'}
                    or receipt['run_id'] != self.lease.run_id
                    or type(receipt['revision']) is not int
                    or receipt['revision'] != self.lease.revision
                    or receipt['outcome'] not in ('ready', 'blocked', 'unconfirmed')
                    or (receipt['outcome'] == 'ready') != isinstance(receipt['state'], dict)
                    or receipt['outcome'] != 'ready' and receipt['state'] is not None):
                raise SessionError('session_transport_failed')
            outcome, state = receipt['outcome'], receipt['state']
        except asyncio.CancelledError:
            # The persisted in_use marker survives cancellation. It cannot be
            # reloaded as ready; there is no stale-state fallback on restart.
            self.lease.state.clear()
            self.lease = None
            raise
        except SessionError as error:
            failure = error
        except Exception:
            failure = SessionError('session_transport_failed')
        try:
            # A failed write leaves the row non-ready. No stale ENV fallback.
            # This commit is independent of Hover/AI/result success.
            from collection_metrics import logged_phase
            with logged_phase(self.run_id, 'session_store_ms'):
                await asyncio.to_thread(self.store.complete, self.lease, outcome, state)
            if failure is not None:
                raise failure
            if outcome == 'blocked':
                raise SessionError('source_login_required')
        finally:
            if isinstance(state, dict):
                state.clear()
            if isinstance(receipt, dict):
                receipt.clear()
            self.lease.state.clear()
            self.lease = None

