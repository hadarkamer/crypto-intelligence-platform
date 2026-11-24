"""Authorize one local synthetic test account on a fresh approved VM for up to 12 minutes."""
import argparse
import os
from pathlib import Path
import pwd
import secrets
import stat
import subprocess
import time
from uuid import UUID

HANDOFF = Path('/run/coinglass-window-handoff')
CONFIG = Path('/run/coinglass-fixture-config/operator.env')
UNIT = 'coinglass-window-fixture.service'


def private_write(path, value, mode, owner=None):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'w', encoding='ascii') as output:
        output.write(value)
        if owner:
            os.fchown(output.fileno(), owner.pw_uid, owner.pw_gid)


def get_actor(tester, deadline):
    while time.monotonic() < deadline:
        try:
            fd = os.open(HANDOFF / 'actor', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            time.sleep(.25)
            continue
        with os.fdopen(fd, 'r', encoding='ascii') as data:
            meta = os.fstat(data.fileno())
            if not stat.S_ISREG(meta.st_mode) or meta.st_uid != tester.pw_uid or meta.st_mode & 0o077:
                raise ValueError('unexpected_fixture_actor_file')
            if meta.st_size < 36:
                time.sleep(.1)
                continue
            value = data.read(40).strip()
            if len(value) != 36 or meta.st_size > 38:
                raise ValueError('invalid_fixture_actor')
            return str(UUID(value))
    raise TimeoutError('fixture_account_not_created')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('qa_test_account')
    tester = pwd.getpwnam(parser.parse_args().qa_test_account)
    if os.getuid() != 0 or tester.pw_uid == 0 or tester.pw_name == 'cg-window-fixture':
        raise SystemExit('a distinct unprivileged fixture test account is required')
    meta = CONFIG.parent.stat()
    if meta.st_uid or stat.S_IMODE(meta.st_mode) != 0o700 or CONFIG.exists():
        raise SystemExit('fresh root-only runtime configuration required')
    os.umask(0o077)
    token = secrets.token_urlsafe(40)
    HANDOFF.mkdir(mode=0o700, exist_ok=False)
    created_config = False
    try:
        # Prepare before transferring ownership, then the test user can publish its actor.
        private_write(HANDOFF / 'bridge', token, 0o400, owner=tester)
        os.chown(HANDOFF, tester.pw_uid, tester.pw_gid)
        actor = get_actor(tester, time.monotonic() + 300)
        private_write(CONFIG, f'QA_OWNER_UUID={actor}\nQA_BRIDGE_TOKEN={token}\n', 0o600)
        created_config = True
        subprocess.run(['systemctl', 'start', UNIT], check=True, timeout=30)
        deadline = time.monotonic() + 720
        while time.monotonic() < deadline and not os.path.lexists(HANDOFF / 'done'):
            time.sleep(.25)
        if not os.path.lexists(HANDOFF / 'done'):
            raise TimeoutError('fixture_session_expired')
    finally:
        try:
            if created_config:
                subprocess.run(['systemctl', 'stop', UNIT], check=True, timeout=40)
        finally:
            if created_config:
                CONFIG.unlink(missing_ok=True)
            for name in ('bridge', 'actor', 'done'):
                (HANDOFF / name).unlink(missing_ok=True)
            HANDOFF.rmdir()


if __name__ == '__main__':
    main()
