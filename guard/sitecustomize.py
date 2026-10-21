"""Test-only Python socket and psycopg guard; not an OS network sandbox."""
import json
import os
from pathlib import Path
import socket
import sys

LOG = Path(os.environ['DOGE_ISOLATED_NETWORK_LOG'])
if not LOG.is_absolute() or not LOG.parent.is_dir():
    raise RuntimeError('ISOLATED_GUARD_LOG_REQUIRED')


def blocked(kind):
    with LOG.open('a') as output:
        output.write(json.dumps({'pid': os.getpid(), 'kind': kind}) + '\n')
    raise AssertionError('ISOLATED_TEST_NETWORK_BLOCKED')


def no_connect(*args, **kwargs):
    blocked('socket')


def audit(event, args):
    if event in ('socket.connect', 'socket.sendto', 'socket.getaddrinfo'):
        blocked(event)


sys.addaudithook(audit)
socket.socket.connect = no_connect
socket.socket.connect_ex = no_connect
socket.create_connection = no_connect
socket.getaddrinfo = no_connect

try:
    import psycopg
except ImportError:
    pass
else:
    def no_database(*args, **kwargs):
        blocked('psycopg')
    psycopg.connect = no_database

GUARD_ACTIVE = True
