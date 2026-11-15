"""Parent-only session persistence and one bounded private child pipe.

The existing worker owns WORK_LOCK. Browser state never enters result/evidence
files, stdout, or stderr. This module neither authenticates nor calls a source.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import select
import struct
import threading

from session_state import PgSessionStore, SessionStateError, STATE_LIMIT

SessionError = SessionStateError
MAX_FRAME = STATE_LIMIT + 1024
KEY_ENV = 'COINGLASS_SESSION_KEY'
ENABLED_ENV = 'COINGLASS_SESSION_STORE_ENABLED'
CHILD_KEYS = ('COINGLASS_SESSION_RETURN_FD', 'COINGLASS_SESSION_RUN_ID',
              'COINGLASS_SESSION_REVISION')


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
    def __init__(self, environment, lease):
        self.env = dict(environment)
        # The database writer/key remain in the parent. Neither is needed by
        # the browser/AI task. A legacy header can never override managed state.
        for key in (KEY_ENV, 'DATABASE_URL', ENABLED_ENV, *CHILD_KEYS):
            self.env.pop(key, None)
        encoded = json.dumps(lease.state, separators=(',', ':'), allow_nan=False)
        # Reserve room for the fixed receipt envelope and its run identity.
        if len(encoded.encode()) > STATE_LIMIT:
            self.env.clear()
            raise SessionError('session_input_invalid')
        self.read_fd, self.write_fd = os.pipe2(os.O_CLOEXEC)
        self.pass_fds = (self.write_fd,)
        self.env.update(
            COINGLASS_COOKIE_HEADER='',
            COINGLASS_STORAGE_STATE_JSON=encoded,
            COINGLASS_SESSION_RETURN_FD=str(self.write_fd),
            COINGLASS_SESSION_RUN_ID=lease.run_id,
            COINGLASS_SESSION_REVISION=str(lease.revision),
        )
        self.stop = threading.Event()
        self.reader = asyncio.create_task(asyncio.to_thread(_read_frame, self.read_fd, self.stop))
        self.closed = False

    def spawned(self):
        """Call immediately after spawn; only the child retains the write end."""
        if self.write_fd is not None:
            os.close(self.write_fd)
            self.write_fd = None
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
        if os.environ.get(ENABLED_ENV, '').lower() != 'true':
            return None
        key = None
        try:
            key = base64.b64decode(os.environ.get(KEY_ENV, ''), validate=True)
            if len(key) != 32:
                raise ValueError()
            return cls(PgSessionStore(connection, key), run_id)
        except SessionError:
            raise
        except Exception:
            raise SessionError('session_configuration_invalid') from None
        finally:
            key = None

    async def begin(self):
        if self.lease is not None or self.finished:
            raise SessionError('session_conflict')
        self.lease = await asyncio.to_thread(self.store.begin, self.run_id)

    async def open_transport(self, environment):
        if self.lease is None or self.finished:
            raise SessionError('session_conflict')
        return SessionTransport(environment, self.lease)

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
            await asyncio.to_thread(self.store.complete, self.lease, outcome, state)
            if failure is not None:
                raise failure
            if outcome != 'ready':
                raise SessionError('session_access_requires_review' if outcome == 'blocked'
                                   else 'session_unconfirmed')
        finally:
            if isinstance(state, dict):
                state.clear()
            if isinstance(receipt, dict):
                receipt.clear()
            self.lease.state.clear()
            self.lease = None
