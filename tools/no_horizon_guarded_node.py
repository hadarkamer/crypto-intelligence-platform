#!/usr/bin/env python3
"""Linux child lifetime/registry lock guard for the unchanged frozen Node server."""
import ctypes
import fcntl
import os
from pathlib import Path
import signal
import sys


def main():
    parent = os.getppid()
    expected_parent = int(os.environ['NO_HORIZON_RUNNER_PID'])
    if parent == 1 or parent != expected_parent or not sys.platform.startswith('linux'):
        raise RuntimeError('EXPLICIT_LINUX_PARENT_REQUIRED')
    # Kernel sends SIGTERM if the Python runner dies, including SIGKILL. The
    # unchanged Node server then closes and dumps; its inherited lock remains
    # held until exit, so a restart cannot open the same registry concurrently.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0 or os.getppid() != parent:
        raise RuntimeError('PARENT_LIFETIME_GUARD_FAILED')
    data = Path(os.environ['NO_HORIZON_REGISTRY_DATA_DIR']).resolve()
    descriptor = os.open(data.parent / 'registry-server.lock',
                         os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(descriptor, True)
    node = os.environ['NO_HORIZON_RUNNER_NODE']
    os.execv(node, [node, *sys.argv[1:]])


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('REGISTRY_GUARD_FAILED: ' + type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
