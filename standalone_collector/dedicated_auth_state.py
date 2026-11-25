"""Private dedicated-account session and login evidence, owned by the collector parent."""
from contextlib import contextmanager
import hashlib
import hmac
from uuid import uuid4

from session_state import Lease, PgSessionStore, SessionStateError

AUTH_RESULTS = frozenset({
    'auth_network_transient', 'auth_invalid_credentials', 'auth_challenge', 'auth_code_required',
    'auth_rate_limited', 'auth_form_unavailable', 'auth_configuration_invalid', 'auth_unconfirmed',
    'auth_gate_after_verification',
})
OBSERVED_BLOCKS = frozenset({'auth_challenge','auth_code_required','auth_rate_limited','auth_invalid_credentials'})
RETRY_SECONDS = 120
IMMEDIATE_GATE_SECONDS = 120
OPEN = "phase NOT IN ('verified','superseded')"
FIELDS = """id,phase,reason,attempts,owner_job,owner_revision,notified_at,
    retry_until>clock_timestamp() AS within_budget,
    CASE WHEN retry_after='infinity'::timestamptz THEN 9223372036854775807::bigint
         ELSE greatest(0,ceil(extract(epoch FROM retry_after-clock_timestamp())))::bigint END AS remaining"""
INCIDENTS = 'collector_private.coinglass_dedicated_auth'
JOBS = 'collector_private.coinglass_dedicated_auth_jobs'


def account_slot(key, email):
    # The private key and address never enter the database, browser logs or returned status.
    if not isinstance(email, str) or not 3 <= len(email) <= 254 or email.count('@') != 1:
        raise SessionStateError('auth_configuration_invalid')
    local, domain = email.split('@')
    if not local or not domain or any(c.isspace() or ord(c) < 32 for c in email):
        raise SessionStateError('auth_configuration_invalid')
    material = ('coinglass-dedicated-email-v1\0' + local + '@' + domain.lower()).encode()
    return 'email-v1:' + hmac.new(key, material, hashlib.sha256).hexdigest()


def _reply(row, code, notify=False):
    return {'incident_id': str(row['id']), 'code': code, 'notify': notify,
            'retry_after_seconds': int(row['remaining']) if row.get('remaining') else None}


def _cooldown(code, seconds):
    if seconds is None:
        return None
    if code != 'auth_rate_limited' or type(seconds) is not int or seconds < 0:
        raise SessionStateError('auth_result_invalid')
    # Dates outside the bounded parser/DB range require explicit intervention.
    # Keep an indefinite hold rather than interpreting arithmetic overflow as an early retry.
    return seconds if seconds <= 10**12 else -1


def incident_status(conn, slot, incident_id=None, job_id=None):
    # Follow explicit operator remediations, preserving the IDs already parked in the app.
    if incident_id and job_id:
        raise SessionStateError('auth_invalid_selector')
    if job_id is not None:
        selector = f'id=(SELECT incident_id FROM {JOBS} WHERE job_id=%s)'
        args = (slot, job_id)
    elif incident_id is not None:
        selector = 'id=%s'
        args = (slot, incident_id)
    else:
        selector = OPEN
        args = (slot,)
    with conn.transaction():
        row = conn.execute(f"""
            WITH RECURSIVE linked AS (
                SELECT id,slot,phase,reason,superseded_by,retry_until,attempts FROM {INCIDENTS} WHERE slot=%s AND {selector}
                UNION ALL
                SELECT i.id,i.slot,i.phase,i.reason,i.superseded_by,i.retry_until,i.attempts FROM {INCIDENTS} i
                JOIN linked l ON l.superseded_by=i.id AND l.slot=i.slot
            )
            SELECT id,phase,reason,attempts,retry_until>clock_timestamp() AS within_budget FROM linked WHERE phase<>'superseded'
        """, args).fetchone()
    if not row:
        return {'incident_id': None, 'status': 'none', 'reason': None, 'can_launch': False}
    phase = row['phase']
    selected_id = str(incident_id or row['id'])
    if phase in ('preparing','submitted','retry') and row['attempts'] and not row['within_budget']:
        return {'incident_id': selected_id, 'status': 'waiting', 'can_launch': False,
                'reason': 'auth_unconfirmed' if phase == 'submitted' else 'auth_retry_exhausted'}
    status = 'resolved' if phase == 'verified' else ('in_progress' if phase in ('preparing','submitted') else 'waiting')
    return {'incident_id': selected_id, 'status': status, 'can_launch': False,
            'reason': 'auth_in_progress' if status == 'in_progress' else row['reason']}


class DedicatedSessionStore(PgSessionStore):
    table = 'collector_private.coinglass_dedicated_sessions'

    def __init__(self, connection, key, account_email):
        super().__init__(connection, key)
        self.slot = account_slot(key, account_email)

    @contextmanager
    def _atomic(self):
        # A successful pre-submit acknowledgment must represent a committed transaction.
        # Refuse an ambient transaction, where psycopg.transaction() would be a savepoint.
        if self.connection.info.transaction_status.name != 'IDLE':
            raise SessionStateError('state_transaction_not_idle')
        try:
            with self.connection.transaction():
                self._require_lock()
                yield
        except SessionStateError:
            raise
        except Exception:
            raise SessionStateError('state_store_failed') from None

    def _owner(self, lease):
        if not isinstance(lease, Lease) or lease.read_only:
            raise SessionStateError('state_revision_conflict')
        row = self.connection.execute(
            f"SELECT revision FROM {self.table} WHERE slot=%s AND revision=%s AND run_id=%s"
            " AND status='in_use' FOR UPDATE", (self.slot,lease.revision,lease.run_id)).fetchone()
        if row is None:
            raise SessionStateError('state_revision_conflict')

    def _incident(self, incident_id):
        return self.connection.execute(
            f'SELECT {FIELDS} FROM {INCIDENTS} WHERE slot=%s AND id=%s AND {OPEN} FOR UPDATE',
            (self.slot,incident_id)).fetchone()

    def source_cooldown(self, lease, *, limited=False, retry_after_seconds=None):
        """Share map-origin throttling across jobs without opening a credential event."""
        if type(limited) is not bool or (not limited and retry_after_seconds is not None):
            raise SessionStateError('auth_result_invalid')
        seconds = _cooldown('auth_rate_limited',retry_after_seconds)
        with self._atomic():
            self._owner(lease)
            if limited:
                seconds = 60 if seconds is None else seconds
                self.connection.execute(f"""
                    UPDATE {self.table} SET map_retry_after=greatest(map_retry_after,
                       CASE WHEN %s::bigint=-1 THEN 'infinity'::timestamptz
                            ELSE clock_timestamp()+%s::bigint*interval '1 second' END)
                    WHERE slot=%s""", (seconds,seconds,self.slot))
            row = self.connection.execute(f"""
                SELECT CASE WHEN map_retry_after='infinity'::timestamptz THEN 9223372036854775807::bigint
                      ELSE greatest(0,ceil(extract(epoch FROM map_retry_after-clock_timestamp())))::bigint
                      END AS remaining FROM {self.table} WHERE slot=%s""", (self.slot,)).fetchone()
        remaining = int(row['remaining'])
        return {'code': 'source_rate_limited' if limited or remaining else 'allowed',
                'retry_after_seconds': remaining if limited or remaining else None}

    def _block(self, row, code, retry_after=None):
        notify = row['notified_at'] is None
        row = self.connection.execute(f"""
            UPDATE {INCIDENTS} SET phase='blocked',reason=%s,owner_job=NULL,owner_revision=NULL,
              notified_at=coalesce(notified_at,clock_timestamp()),
              retry_after=greatest(retry_after,CASE WHEN %s::bigint=-1 THEN 'infinity'::timestamptz
                               ELSE clock_timestamp()+(%s::bigint * interval '1 second') END)
            WHERE id=%s RETURNING {FIELDS}""", (code,retry_after,retry_after,row['id'])).fetchone()
        return _reply(row, code, notify)

    def _new(self, incident_id):
        return self.connection.execute(f"""
            INSERT INTO {INCIDENTS}(id,slot,phase,attempts,retry_until,reason)
              VALUES(%s,%s,'retry',0,clock_timestamp()+%s*interval '1 second','auth_retry_pending')
              RETURNING {FIELDS}""", (incident_id,self.slot,RETRY_SECONDS)).fetchone()

    def auth_start(self, lease, *, observed_code=None, retry_after_seconds=None):
        """Reserve form preparation on first boot or after an explicit map gate only."""
        if observed_code is not None and observed_code not in OBSERVED_BLOCKS:
            raise SessionStateError('auth_result_invalid')
        cooldown = _cooldown(observed_code,retry_after_seconds)
        with self._atomic():
            self._owner(lease)
            row = self.connection.execute(
                f'SELECT {FIELDS} FROM {INCIDENTS} WHERE slot=%s AND {OPEN} FOR UPDATE',
                (self.slot,)).fetchone()
            fresh = row is None
            if fresh:
                row = self._new(str(uuid4()))
            self.connection.execute(f"""
                INSERT INTO {JOBS}(job_id,incident_id) VALUES (%s,%s)
                ON CONFLICT(job_id) DO UPDATE SET incident_id=excluded.incident_id""",
                (lease.run_id,row['id']))
            if observed_code is not None:
                return self._block(row,observed_code,cooldown)
            if row['phase'] == 'blocked':
                return _reply(row, row['reason'])
            if row['phase'] == 'submitted':
                return self._block(row, 'auth_unconfirmed')
            if (row['phase'] == 'preparing' and str(row['owner_job']) == lease.run_id
                    and row['owner_revision'] == lease.revision):
                return _reply(row,'auth_in_progress')
            if fresh:
                recent = self.connection.execute(f"""
                    SELECT 1 FROM {INCIDENTS} WHERE slot=%s AND phase='verified'
                    AND (verified_job=%s OR verified_at > clock_timestamp()-%s*interval '1 second') LIMIT 1""",
                    (self.slot,lease.run_id,IMMEDIATE_GATE_SECONDS)).fetchone()
                if recent:
                    return self._block(row,'auth_gate_after_verification')
            if row['attempts'] >= 2 or (row['attempts'] and not row['within_budget']):
                return self._block(row,'auth_retry_exhausted')
            row = self.connection.execute(f"""
                UPDATE {INCIDENTS} SET phase='preparing',attempts=attempts+1,reason=NULL,
                    retry_until=CASE WHEN attempts=0 THEN clock_timestamp()+%s*interval '1 second' ELSE retry_until END,
                    owner_job=%s,owner_revision=%s WHERE id=%s RETURNING {FIELDS}""",
                (RETRY_SECONDS,lease.run_id,lease.revision,row['id'])).fetchone()
            return _reply(row,'allowed')

    def auth_submit(self, lease, incident_id):
        """Commit before the official form click. An uncertain commit cannot authorize a click."""
        with self._atomic():
            self._owner(lease)
            row = self.connection.execute(f"""
                UPDATE {INCIDENTS} SET phase='submitted'
                WHERE slot=%s AND id=%s AND owner_job=%s AND owner_revision=%s
                  AND phase='preparing' AND retry_until>clock_timestamp() RETURNING id""",
                (self.slot,incident_id,lease.run_id,lease.revision)).fetchone()
            if not row:
                raise SessionStateError('auth_attempt_not_reserved')

    def auth_result(self, lease, incident_id, code, retry_after_seconds=None):
        if code not in AUTH_RESULTS:
            raise SessionStateError('auth_result_invalid')
        cooldown = _cooldown(code,retry_after_seconds)
        with self._atomic():
            self._owner(lease)
            row = self._incident(incident_id)
            if not row or str(row['owner_job']) != lease.run_id or row['owner_revision'] != lease.revision:
                raise SessionStateError('auth_attempt_not_reserved')
            if code == 'auth_network_transient':
                if row['phase'] == 'submitted':
                    return self._block(row,'auth_unconfirmed')
                if row['attempts'] < 2 and row['within_budget']:
                    row = self.connection.execute(f"""
                        UPDATE {INCIDENTS} SET phase='retry',reason='auth_retry_pending',
                            owner_job=NULL,owner_revision=NULL WHERE id=%s RETURNING {FIELDS}""",
                        (incident_id,)).fetchone()
                    return _reply(row,'auth_retry_pending')
                return self._block(row,'auth_retry_exhausted')
            return self._block(row,code,cooldown)

    def checkpoint(self, lease, state, *, incident_id=None):
        """Commit protected proof and the corresponding encrypted snapshot before analysis."""
        revision = lease.revision + 1
        payload = self._seal(state,revision)
        with self._atomic():
            self._owner(lease)
            row = self.connection.execute(
                f'SELECT id FROM {INCIDENTS} WHERE slot=%s AND {OPEN} FOR UPDATE',
                (self.slot,)).fetchone()
            if incident_id is not None and (not row or str(row['id']) != str(incident_id)):
                raise SessionStateError('auth_attempt_not_reserved')
            self.connection.execute(f"""
                UPDATE {self.table} SET encrypted_payload=%s,revision=%s,updated_at=clock_timestamp()
                WHERE slot=%s AND revision=%s AND run_id=%s AND status='in_use'""",
                (payload,revision,self.slot,lease.revision,lease.run_id))
            if row:
                self.connection.execute(f"""
                    UPDATE {INCIDENTS} SET phase='verified',verified_at=clock_timestamp(),verified_job=%s,
                        owner_job=NULL,owner_revision=NULL,reason=NULL WHERE id=%s""",
                    (lease.run_id,row['id']))
        return Lease(revision,lease.run_id,state)

    def complete(self, lease, outcome, state=None):
        # Only checkpoint accepts state from protected proof. A late or public final receipt,
        # a challenge, a capture error or EOF cannot replace the last verified snapshot.
        with self._atomic():
            self._owner(lease)
            row = self.connection.execute(f"""
                SELECT {FIELDS} FROM {INCIDENTS} WHERE slot=%s AND owner_job=%s
                    AND owner_revision=%s AND phase IN ('preparing','submitted') FOR UPDATE""",
                (self.slot,lease.run_id,lease.revision)).fetchone()
            if row:
                if row['phase'] == 'submitted':
                    self._block(row,'auth_unconfirmed')
                elif row['attempts'] >= 2 or not row['within_budget']:
                    self._block(row,'auth_retry_exhausted')
                else:
                    self.connection.execute(f"""
                        UPDATE {INCIDENTS} SET phase='retry',reason='auth_retry_pending',
                            owner_job=NULL,owner_revision=NULL WHERE id=%s""", (row['id'],))
            # Use the authoritative encrypted checkpoint, not a mutable browser input copy
            # (the clean-context pilot deliberately starts Chromium with an empty copy).
            saved = self.connection.execute(f"""
                SELECT encrypted_payload FROM {self.table} WHERE slot=%s AND revision=%s""",
                (self.slot,lease.revision)).fetchone()
            try:
                state = self._open(saved['encrypted_payload'],lease.revision,allow_empty=True)
            except SessionStateError as error:
                if error.code != 'state_expired':
                    raise
                state = {'cookies': [], 'origins': []}
            return super().complete(Lease(lease.revision,lease.run_id,state),'unconfirmed')

    def bootstrap(self, state):
        raise SessionStateError('auth_protected_proof_required')

    def renew(self, state, expected_revision):
        raise SessionStateError('auth_protected_proof_required')

    def acknowledge_remediation(self, incident_id):
        """Trusted explicit operator action AFTER remedy; no credential or HTTP endpoint.

        Caller must own WORK_LOCK. The old incident remains queryable. Repeating an old
        acknowledgment cannot open another event or grant extra attempts.
        """
        successor = str(uuid4())
        with self._atomic():
            row = self._incident(incident_id)
            if not row or row['phase'] != 'blocked' or row['remaining']:
                raise SessionStateError('auth_remediation_not_available')
            self.connection.execute(f"""
                UPDATE {INCIDENTS} SET phase='superseded',superseded_by=%s WHERE id=%s""",
                (successor,incident_id))
            self._new(successor)
        return successor
