"""Bounded messages on a private, close-on-exec Unix stream socket."""
from __future__ import annotations

import json
import socket
import struct

from session_state import STATE_LIMIT, SessionStateError

MAX_BYTES = STATE_LIMIT + 1024


def _read(sock, size):
    parts = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise EOFError
        size -= len(chunk)
        parts.append(chunk)
    return b''.join(parts)


def receive(sock):
    length, = struct.unpack('!I', _read(sock, 4))
    if not 0 < length <= MAX_BYTES:
        raise SessionStateError('session_transport_failed')
    message = json.loads(_read(sock, length))
    if not isinstance(message, dict):
        raise SessionStateError('session_transport_failed')
    return message


def send(sock, message):
    data = json.dumps(message, allow_nan=False, separators=(',', ':')).encode()
    if len(data) > MAX_BYTES:
        raise SessionStateError('session_transport_failed')
    sock.sendall(struct.pack('!I', len(data)) + data)


def disconnect(sock):
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
