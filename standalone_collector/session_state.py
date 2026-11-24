"""Private CoinGlass cookie state. One parent writer, the existing WORK_LOCK.

No schema creation, implicit bootstrap, network requests, or credential logging.
The browser child may import validate_state without receiving the encryption key.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import json
import math
import re
import time
from uuid import UUID

WORK_LOCK = 748230102
STATE_LIMIT = 65536
FORMAT = 'coinglass-cookie-state.v1'
SLOT = 'coinglass'
COOKIE_FIELDS = frozenset(('name', 'value', 'domain', 'path', 'expires',
                          'httpOnly', 'secure', 'sameSite', 'partitionKey'))


class SessionStateError(Exception):
    """Only controlled, nonsecret codes cross the storage boundary."""
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def validate_state(value: dict, *, now: float | None = None, allow_empty: bool = False) -> dict:
    """Validate scope/structure, not server-side authentication validity.

    Expired auxiliary cookies do not prove that the session expired. A snapshot
    with no potentially usable cookies is rejected. All supported attributes are
    preserved; no cookie is invented, renewed or silently replaced.
    """
    try:
        if not isinstance(value, dict) or set(value) != {'cookies', 'origins'} or value['origins'] != []:
            raise ValueError()
        raw = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(',', ':'))
        if len(raw.encode()) > STATE_LIMIT:
            raise SessionStateError('state_too_large')
        cookies = value['cookies']
        if allow_empty and cookies == []:
            return {'cookies': [], 'origins': []}
        if not isinstance(cookies, list) or not 1 <= len(cookies) <= 256:
            raise ValueError()
        current = time.time() if now is None else now
        usable = False
        identities = set()
        for cookie in cookies:
            if not isinstance(cookie, dict) or set(cookie) - COOKIE_FIELDS:
                raise ValueError()
            if not all(type(cookie.get(k)) is str for k in ('name', 'value', 'domain', 'path')):
                raise ValueError()
            if not cookie['name'] or not cookie['path'].startswith('/'):
                raise ValueError()
            if any('\x00' in cookie[k] for k in ('name', 'value', 'domain', 'path')):
                raise ValueError()
            domain = cookie['domain']
            host = domain.lstrip('.').lower()
            if (domain.startswith('..') or len(host) > 253
                    or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', host)
                    or any(not label or len(label) > 63 or label.startswith('-') or label.endswith('-')
                           for label in host.split('.'))
                    or not (host == 'coinglass.com' or host.endswith('.coinglass.com'))):
                raise ValueError()
            if any(type(cookie.get(k)) is not bool for k in ('httpOnly', 'secure')):
                raise ValueError()
            # Preserve an omitted SameSite rather than inventing a policy.
            if 'sameSite' in cookie and cookie['sameSite'] not in ('Strict', 'Lax', 'None'):
                raise ValueError()
            expires = cookie.get('expires')
            if type(expires) not in (int, float) or not math.isfinite(expires):
                raise ValueError()
            if 'partitionKey' in cookie and type(cookie['partitionKey']) is not str:
                raise ValueError()
            identity = (cookie['name'], domain, cookie['path'], cookie.get('partitionKey'))
            if identity in identities:
                raise ValueError()
            identities.add(identity)
            usable |= expires == -1 or expires > current
        if not usable:
            raise SessionStateError('state_expired')
        return json.loads(raw)
    except SessionStateError:
        raise
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise SessionStateError('state_invalid') from None


@dataclass(frozen=True)
class Lease:
    revision: int
    run_id: str
    state: dict = field(repr=False)
    read_only: bool = False


class PgSessionStore:
    """Use the parent's held PostgreSQL connection; never acquire a new lock."""
    def __init__(self, connection, key: bytes):
        if type(key) is not bytes or len(key) != 32:
            raise SessionStateError('state_key_invalid')
        self.connection, self._key = connection, key

    @staticmethod
    def _aad(revision):
        return f'{FORMAT}:{SLOT}:{revision}'.encode('ascii')

    def _seal(self, state, revision, *, allow_empty=False):
        try:
            from Crypto.Cipher import AES
            from Crypto.Random import get_random_bytes
            clear = json.dumps(validate_state(state, allow_empty=allow_empty), ensure_ascii=True, allow_nan=False,
                               separators=(',', ':')).encode()
            cipher = AES.new(self._key, AES.MODE_GCM, nonce=get_random_bytes(12), mac_len=16)
            cipher.update(self._aad(revision))
            ciphertext, tag = cipher.encrypt_and_digest(clear)
            return json.dumps({'format': FORMAT, 'nonce': base64.b64encode(cipher.nonce).decode(),
                               'ciphertext': base64.b64encode(ciphertext).decode(),
                               'tag': base64.b64encode(tag).decode()}, separators=(',', ':'))
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_encrypt_failed') from None

    def _open(self, payload, revision, *, allow_empty=False):
        try:
            from Crypto.Cipher import AES
            if type(payload) is not str or len(payload) > 2 * STATE_LIMIT:
                raise ValueError()
            envelope = json.loads(payload)
            if (not isinstance(envelope, dict) or set(envelope) != {'format', 'nonce', 'ciphertext', 'tag'}
                    or envelope['format'] != FORMAT):
                raise ValueError()
            nonce, ciphertext, tag = (base64.b64decode(envelope[k], validate=True)
                                      for k in ('nonce', 'ciphertext', 'tag'))
            if len(nonce) != 12 or len(tag) != 16 or len(ciphertext) > STATE_LIMIT:
                raise ValueError()
            cipher = AES.new(self._key, AES.MODE_GCM, nonce=nonce, mac_len=16)
            cipher.update(self._aad(revision))
            clear = cipher.decrypt_and_verify(ciphertext, tag)
            return validate_state(json.loads(clear), allow_empty=allow_empty)
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_corrupt') from None

    def _require_lock(self):
        row = self.connection.execute("""SELECT EXISTS (
            SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND mode='ExclusiveLock'
            AND pid=pg_backend_pid()
            AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
            AND classid=0 AND objid=%s AND objsubid=1) AS held""", (WORK_LOCK,)).fetchone()
        if not row or row['held'] is not True:
            raise SessionStateError('state_coordination_required')

    def read(self):
        try:
            with self.connection.transaction():
                self._require_lock()
                row = self.connection.execute("""SELECT revision,status,encrypted_payload
                    FROM collector_private.coinglass_session_state WHERE slot=%s""", (SLOT,)).fetchone()
                if row is None:
                    raise SessionStateError('state_missing')
                if row['status'] != 'ready':
                    raise SessionStateError('state_not_ready')
                return self._open(row['encrypted_payload'], row['revision'])
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None

    def bootstrap(self, state):
        """Explicit operator action only; refuse any existing state or incident."""
        payload = self._seal(state, 1)
        try:
            with self.connection.transaction():
                self._require_lock()
                row = self.connection.execute("""INSERT INTO collector_private.coinglass_session_state
                    (slot,revision,status,run_id,encrypted_payload)
                    VALUES(%s,1,'ready',NULL,%s) ON CONFLICT(slot) DO NOTHING RETURNING revision""",
                    (SLOT, payload)).fetchone()
                if row is None:
                    raise SessionStateError('state_exists')
                return row['revision']
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None

    def revision(self):
        """Observe only the opaque CAS version without blocking scans; renew rechecks it."""
        try:
            with self.connection.transaction():
                row = self.connection.execute(
                    'SELECT revision FROM collector_private.coinglass_session_state WHERE slot=%s',
                    (SLOT,)).fetchone()
                return row['revision'] if row else 0
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None

    def _previous_run_inactive(self, row):
        # All managed writers must own the same exclusive connection lock.
        # The worker's existing job ledger must also no longer name that job
        # as running. A clock or expired UI ticket alone never fences a writer.
        return self.connection.execute(
            "SELECT id FROM public.ai_collection_bridge_jobs WHERE id=%s AND status='running'",
            (row['run_id'],)).fetchone() is None

    def renew(self, state, expected_revision):
        """Server-only replacement after operator identity and map verification.

        The caller owns the existing worker lock for this short transition.
        A late operator or scan cannot replace subsequently saved state.
        """
        if type(expected_revision) is not int or expected_revision < 0:
            raise SessionStateError('state_revision_conflict')
        if expected_revision == 0:
            return self.bootstrap(state)
        try:
            with self.connection.transaction():
                self._require_lock()
                row = self.connection.execute(
                    'SELECT revision,status,run_id FROM collector_private.coinglass_session_state '
                    'WHERE slot=%s FOR UPDATE', (SLOT,)).fetchone()
                if not row or row['revision'] != expected_revision:
                    raise SessionStateError('state_revision_conflict')
                if row['status'] == 'in_use' and not self._previous_run_inactive(row):
                    raise SessionStateError('state_not_ready')
                revision = row['revision'] + 1
                self.connection.execute(
                    "UPDATE collector_private.coinglass_session_state SET status='ready',"
                    " revision=%s,run_id=NULL,encrypted_payload=%s,updated_at=now()"
                    " WHERE slot=%s AND revision=%s",
                    (revision, self._seal(state, revision), SLOT, expected_revision))
                return revision
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None

    def begin(self, run_id, *, recover_orphan=False, allow_initial_probe=False):
        try:
            run_id = str(UUID(run_id))
        except (ValueError, TypeError, AttributeError):
            raise SessionStateError('state_run_id_invalid') from None
        empty = {'cookies': [], 'origins': []}
        try:
            with self.connection.transaction():
                self._require_lock()
                row = self.connection.execute("""SELECT revision,status,run_id,encrypted_payload
                    FROM collector_private.coinglass_session_state WHERE slot=%s FOR UPDATE""",
                    (SLOT,)).fetchone()
                if row is None:
                    if not allow_initial_probe:
                        raise SessionStateError('state_missing')
                    self.connection.execute("""INSERT INTO collector_private.coinglass_session_state
                        (slot,revision,status,run_id,encrypted_payload) VALUES(%s,1,'in_use',%s,%s)""",
                        (SLOT, run_id, self._seal(empty, 1, allow_empty=True)))
                    return Lease(1, run_id, empty)
                status = row['status']
                if status == 'blocked' and allow_initial_probe:
                    # The incident ledger suppresses only previously gated
                    # targets. Public targets may run anonymously without
                    # changing the operator's expected revision.
                    return Lease(row['revision'], run_id, empty, read_only=True)
                if status == 'in_use':
                    if (not recover_orphan or str(row['run_id']) == run_id
                            or not self._previous_run_inactive(row)):
                        raise SessionStateError('state_not_ready')
                elif status != 'ready' and not (status == 'unconfirmed' and allow_initial_probe):
                    raise SessionStateError('state_not_ready')
                try:
                    state = (self._open(row['encrypted_payload'], row['revision'], allow_empty=True)
                             if row['encrypted_payload'] is not None else empty)
                except SessionStateError as exc:
                    if exc.code != 'state_expired' or not allow_initial_probe:
                        raise
                    # Establish the actual source gate in a clean context.
                    state = empty
                revision = row['revision'] + 1
                payload = self._seal(state, revision, allow_empty=True)
                updated = self.connection.execute("""UPDATE collector_private.coinglass_session_state
                    SET revision=%s,status='in_use',run_id=%s,encrypted_payload=%s,updated_at=now()
                    WHERE slot=%s AND revision=%s AND status=%s RETURNING revision""",
                    (revision, run_id, payload, SLOT, row['revision'], status)).fetchone()
                if updated is None:
                    raise SessionStateError('state_revision_conflict')
                return Lease(revision, run_id, state)
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None

    def complete(self, lease, outcome, state=None):
        if (not isinstance(lease, Lease) or type(lease.revision) is not int or lease.revision <= 0
                or type(lease.run_id) is not str or outcome not in ('ready', 'blocked', 'unconfirmed')):
            raise SessionStateError('state_outcome_invalid')
        if outcome != 'ready' and state is not None:
            raise SessionStateError('state_outcome_invalid')
        if lease.read_only:
            return lease.revision
        revision = lease.revision + 1
        # 'ready' represents the availability of previously verified state
        # for a new attempt. An uncertain observation is tracked independently
        # from that saved state by the job/incident ledger.
        if outcome == 'unconfirmed' and lease.state['cookies']:
            outcome, state = 'ready', lease.state
        payload = self._seal(state, revision) if outcome == 'ready' else None
        try:
            with self.connection.transaction():
                self._require_lock()
                row = self.connection.execute("""UPDATE collector_private.coinglass_session_state
                    SET revision=%s,status=%s,run_id=NULL,encrypted_payload=%s,updated_at=now()
                    WHERE slot=%s AND revision=%s AND run_id=%s AND status='in_use' RETURNING revision""",
                    (revision, outcome, payload, SLOT, lease.revision, lease.run_id)).fetchone()
                if row is None:
                    raise SessionStateError('state_revision_conflict')
                return row['revision']
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None
