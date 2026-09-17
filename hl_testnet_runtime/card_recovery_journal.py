"""Durable OFFLINE corrective-request rehearsal; never an exchange dispatcher.

Explicit migration only. Reuse the staging/loopback transaction boundary. Nothing
runs on import, no runtime hook, transport, signer, secret or expiration unlock.
A pre-attempt marker survives even if the subsequent reply/commit ACK is lost.
Callers must NOT attach an order sender to this software-only rehearsal.
"""
from copy import deepcopy
from . import card_exit_recovery as recovery, card_lifecycle as life
from .card_lifecycle_store import _continues
from .postgres_journal import JournalError

VERSION = 'offline-durable-card-recovery-v1'
SCHEMA = 'hl_testnet_recovery_rehearsal_v1'
TERMINAL = ('OBSERVED', 'ABORTED_NOT_SENT')
PHASES = ('PREPARED_NOT_SENT', 'OUTCOME_UNKNOWN', 'WAITING_FOR_EVIDENCE',
          'REJECTED_DO_NOT_RETRY', 'CONFLICT_RECONCILIATION_REQUIRED') + TERMINAL


class RecoveryStorageError(JournalError, recovery.RecoveryError):
    """Fixed codes survive rollback without exposing database or remote text."""


def _encode(value):
    encoded = life.encoded(value)
    if len(encoded.encode('utf-8')) > 262144:
        raise RecoveryStorageError('RECOVERY_RECORD_TOO_LARGE')
    return encoded


def _hex(value):
    return life.ident(value, r'[0-9a-f]{64}')


def _evidence(bindings, snapshot, context):
    life.validate_bindings(bindings)
    account, symbol = life.validate_snapshot(snapshot)
    if any(life.address(b['account']) != account or b['symbol'] != symbol for b in bindings):
        raise RecoveryStorageError('ONE_ACCOUNT_SYMBOL_REQUIRED')
    # Reuse all existing context validation, but do not treat this as freshness.
    recovery.plan(bindings, snapshot, context, now_ms=snapshot['at_ms'])
    value = deepcopy(dict(bindings=bindings, snapshot=snapshot, context=context))
    _encode(value)
    return value


def _plan(evidence, now):
    return recovery.plan(evidence['bindings'], evidence['snapshot'], evidence['context'], now_ms=now)


def _reply(value):
    life.shape(value, 'state code oid')
    state, code, oid = value['state'], value['code'], value['oid']
    if state == 'ACCEPTED_UNVERIFIED':
        life.ident(oid, r'[1-9][0-9]{0,19}')
        if int(oid) >= 2**64 or code is not None:
            raise RecoveryStorageError('INVALID_NORMALIZED_REPLY')
    elif state == 'REJECTED':
        if oid is not None or code not in set(recovery.ERRORS.values()) | {'OTHER_REJECTION'}:
            raise RecoveryStorageError('INVALID_NORMALIZED_REPLY')
    elif state != 'OUTCOME_UNKNOWN' or oid is not None or code is not None:
        raise RecoveryStorageError('INVALID_NORMALIZED_REPLY')
    return deepcopy(value)


class RecoveryJournal:
    def __init__(self, journal):
        self.journal = journal

    @staticmethod
    def bucket(account, symbol):
        return life.digest(['testnet', life.address(account), life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')])

    def initialize(self):
        """Call explicitly in isolated software tests; not from a recurring task."""
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (1729048230,))
            exists = conn.execute('SELECT to_regnamespace(%s)', (SCHEMA,)).fetchone()[0]
            if exists is not None:
                self.ready(conn)
                return False
            conn.execute(f'CREATE SCHEMA {SCHEMA}')
            conn.execute(f'''CREATE TABLE {SCHEMA}.metadata (
                singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL)''')
            conn.execute(f'INSERT INTO {SCHEMA}.metadata VALUES(true,%s)', (VERSION,))
            conn.execute(f'''CREATE TABLE {SCHEMA}.buckets (
                bucket text PRIMARY KEY CHECK(length(bucket)=64),
                revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0), active_id text, last_id text)''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.requests (
                request_id text PRIMARY KEY CHECK(length(request_id)=64),
                bucket text NOT NULL REFERENCES {SCHEMA}.buckets(bucket),
                phase text NOT NULL CHECK(phase IN {str(PHASES)}),
                data jsonb NOT NULL, checksum text NOT NULL,
                environment text NOT NULL DEFAULT 'testnet' CHECK(environment='testnet'),
                simulation_only boolean NOT NULL DEFAULT true CHECK(simulation_only))''')
            conn.execute(f'''CREATE UNIQUE INDEX recovery_one_unresolved_per_bucket
                ON {SCHEMA}.requests(bucket) WHERE phase NOT IN ('OBSERVED','ABORTED_NOT_SENT')''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.events (
                bucket text NOT NULL REFERENCES {SCHEMA}.buckets(bucket),
                event_id text NOT NULL, revision bigint NOT NULL CHECK(revision>0),
                request_id text NOT NULL REFERENCES {SCHEMA}.requests(request_id),
                input_hash text NOT NULL, record jsonb NOT NULL, checksum text NOT NULL,
                PRIMARY KEY(bucket,event_id), UNIQUE(bucket,revision))''')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
            for name in ('metadata','buckets','requests','events'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{name} FROM PUBLIC')
        return True

    def ready(self, conn):
        if conn.execute(f'SELECT version FROM {SCHEMA}.metadata WHERE singleton=true').fetchone() != (VERSION,):
            raise RecoveryStorageError('RECOVERY_SCHEMA_REQUIRES_REVIEW')
        for name in ('buckets','requests','events'):
            if conn.execute('SELECT to_regclass(%s)', (SCHEMA+'.'+name,)).fetchone()[0] is None:
                raise RecoveryStorageError('RECOVERY_SCHEMA_REQUIRES_REVIEW')

    @staticmethod
    def _read(conn, request_id):
        row = conn.execute(f'SELECT bucket,phase,data,checksum FROM {SCHEMA}.requests WHERE request_id=%s', (request_id,)).fetchone()
        if row is None:
            raise RecoveryStorageError('UNKNOWN_RECOVERY_REQUEST')
        bucket, phase, data, checksum = row
        if (life.digest(data) != checksum or data.get('request_id') != request_id
                or data.get('bucket') != bucket or data.get('phase') != phase
                or data.get('version') != VERSION or data.get('environment') != 'testnet'
                or data.get('simulation_only') is not True):
            raise RecoveryStorageError('RECOVERY_RECORD_INTEGRITY_FAILURE')
        return data

    @staticmethod
    def _result(data, revision, *, duplicate=False):
        instructions = dict(PREPARED_NOT_SENT='REVALIDATE_BEFORE_SIMULATED_ATTEMPT',
            OUTCOME_UNKNOWN='RECONCILE_DO_NOT_RESEND', WAITING_FOR_EVIDENCE='VERIFY_EFFECT_NOT_RECEIPT',
            REJECTED_DO_NOT_RETRY='REJECTION_REQUIRES_REVIEW',
            CONFLICT_RECONCILIATION_REQUIRED='CONFLICT_REQUIRES_REVIEW',
            OBSERVED='DONE', ABORTED_NOT_SENT='REPLAN_FROM_FRESH_EVIDENCE')
        return dict(request=deepcopy(data), revision=revision, duplicate=duplicate,
            restart_action=instructions[data['phase']], simulation_only=True,
            dispatch_enabled=False, order_requests_sent=0, live_recovery_ready=False)

    @staticmethod
    def _lock(conn, bucket):
        lock = int(life.digest([VERSION,bucket])[:16],16) % (2**63)
        conn.execute('SELECT pg_advisory_xact_lock(%s)', (lock,))

    def _change(self, bucket, event_id, kind, args, expected_revision, now_ms, change):
        _hex(bucket); life.ident(event_id); life.moment(now_ms)
        if type(expected_revision) is not int or expected_revision < 0:
            raise RecoveryStorageError('EXPECTED_BUCKET_REVISION_REQUIRED')
        args = deepcopy(args); _encode(args)
        input_hash = life.digest([kind, args])
        with self.journal._transaction() as conn:
            self.ready(conn)
            self._lock(conn,bucket)
            conn.execute(f'INSERT INTO {SCHEMA}.buckets(bucket) VALUES(%s) ON CONFLICT DO NOTHING', (bucket,))
            revision, active, last = conn.execute(f'SELECT revision,active_id,last_id FROM {SCHEMA}.buckets WHERE bucket=%s FOR UPDATE', (bucket,)).fetchone()
            event = conn.execute(f'SELECT request_id,input_hash,record,checksum FROM {SCHEMA}.events WHERE bucket=%s AND event_id=%s', (bucket,event_id)).fetchone()
            if event:
                if (life.digest(event[2]) != event[3] or event[2].get('input_hash') != event[1]
                        or event[2].get('request_id') != event[0]):
                    raise RecoveryStorageError('RECOVERY_EVENT_INTEGRITY_FAILURE')
                if event[1] != input_hash:
                    raise RecoveryStorageError('IDEMPOTENCY_KEY_CONTENT_CHANGED')
                # Return CURRENT durable state, never an old permission to retry.
                return self._result(self._read(conn,event[0]), revision, duplicate=True)
            if expected_revision != revision:
                raise RecoveryStorageError('CONCURRENT_RECOVERY_RELOAD_REQUIRED')
            try:
                data = change(conn, active, revision, last)
            except life.LifecycleError as exc:
                # LifecycleStorageError also inherits JournalError: normalize it
                # first so callers receive one stable recovery-domain exception.
                raise RecoveryStorageError(str(exc)) from None
            except JournalError:
                raise
            if now_ms < data['last_event_at_ms']:
                raise RecoveryStorageError('RECOVERY_TIME_MOVED_BACKWARDS')
            data.update(last_event_at_ms=now_ms, request_revision=revision+1)
            phase = data['phase']; request_id = data['request_id']
            payload = _encode(data)
            conn.execute(f'''INSERT INTO {SCHEMA}.requests(request_id,bucket,phase,data,checksum)
                VALUES(%s,%s,%s,%s::jsonb,%s) ON CONFLICT(request_id) DO UPDATE
                SET phase=EXCLUDED.phase,data=EXCLUDED.data,checksum=EXCLUDED.checksum''',
                (request_id,bucket,phase,payload,life.digest(data)))
            record = dict(kind=kind,request_id=request_id,input_hash=input_hash,
                at_ms=now_ms,phase=phase,reply=data['reply'],
                received_reply=args['args'] if kind == 'REPLY' else None,state_hash=life.digest(data))
            conn.execute(f'''INSERT INTO {SCHEMA}.events VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s)''',
                (bucket,event_id,revision+1,request_id,input_hash,_encode(record),life.digest(record)))
            conn.execute(f'UPDATE {SCHEMA}.buckets SET revision=%s,active_id=%s,last_id=%s WHERE bucket=%s',
                (revision+1,None if phase in TERMINAL else request_id,request_id,bucket))
        return self._result(data,revision+1)

    def prepare(self, bindings, snapshot, context, *, event_id, expected_revision, now_ms):
        evidence = _evidence(bindings,snapshot,context)
        bucket = self.bucket(snapshot['account'],snapshot['symbol'])
        def change(conn, active, revision, last):
            if active is not None:
                raise RecoveryStorageError('PENDING_RECOVERY_MUST_BE_RESOLVED')
            if last is not None:
                previous = self._read(conn,last)
                if previous['bucket'] != bucket:
                    raise RecoveryStorageError('RECOVERY_BUCKET_INTEGRITY_FAILURE')
                prior = (previous['confirmation']['evidence'] if previous['confirmation']
                         else previous['original_evidence'])
                _continues(prior,evidence)
                if evidence['snapshot']['at_ms'] <= previous['last_event_at_ms']:
                    raise RecoveryStorageError('REPLAN_REQUIRES_NEW_OBSERVATION')
            proposal = _plan(evidence,now_ms)
            if proposal['state'] != 'PROPOSED_OFFLINE' or proposal['bucket'] != bucket:
                raise RecoveryStorageError('FRESH_SAFE_CORRECTION_PROPOSAL_REQUIRED')
            request_id = life.digest([VERSION,bucket,proposal['next_step']['intent_id'],revision+1])
            return dict(version=VERSION,environment='testnet',simulation_only=True,
                request_id=request_id,bucket=bucket,phase='PREPARED_NOT_SENT',
                client_reference='0x'+request_id[:32],proposal=proposal,original_evidence=evidence,
                prepared_at_ms=now_ms,last_event_at_ms=now_ms,attempt_started_at_ms=None,
                simulated_attempt_count=0,reply=None,reply_at_ms=None,confirmation=None)
        return self._change(bucket,event_id,'PREPARE',evidence,expected_revision,now_ms,change)

    def _for_request(self, request_id, event_id, kind, args, expected_revision, now_ms, mutate):
        _hex(request_id)
        with self.journal._transaction() as conn:
            self.ready(conn)
            bucket = self._read(conn,request_id)['bucket']
        def change(conn, active, revision, last):
            data = self._read(conn,request_id)
            if active != request_id or data['phase'] in TERMINAL:
                raise RecoveryStorageError('REQUEST_NOT_ACTIVE_DO_NOT_REOPEN')
            return mutate(data)
        return self._change(bucket,event_id,kind,dict(request_id=request_id,args=args),
            expected_revision,now_ms,change)

    def begin_simulated_attempt(self, request_id, bindings, snapshot, context, *, event_id, expected_revision, now_ms):
        """Persist uncertainty BEFORE the test's pretend request, never send here.

        A crash after this marker cannot be distinguished from a sent request.
        Hence there is no claim timeout, takeover resend or repeat permission.
        """
        evidence = _evidence(bindings,snapshot,context)
        def mutate(data):
            if data['phase'] != 'PREPARED_NOT_SENT':
                raise RecoveryStorageError('ATTEMPT_ALREADY_MARKED_DO_NOT_REPEAT')
            original = data['proposal']; fresh = _plan(evidence,now_ms)
            if (not original['observed_at_ms'] <= now_ms <= original['valid_until_ms']
                    or fresh['state'] != 'PROPOSED_OFFLINE'
                    or fresh['next_step'] != original['next_step'] or fresh['basis'] != original['basis']):
                raise RecoveryStorageError('STALE_OR_CHANGED_PLAN_REPLAN_UNSENT')
            data.update(phase='OUTCOME_UNKNOWN',attempt_started_at_ms=now_ms,simulated_attempt_count=1)
            return data
        return self._for_request(request_id,event_id,'BEGIN_SIMULATED_ATTEMPT',evidence,expected_revision,now_ms,mutate)

    def record_reply(self, request_id, reply, *, event_id, expected_revision, now_ms):
        reply = _reply(reply)
        def mutate(data):
            if data['attempt_started_at_ms'] is None:
                raise RecoveryStorageError('UNSTARTED_REQUEST_CANNOT_HAVE_REPLY')
            old = data['reply']
            if data['phase'] == 'CONFLICT_RECONCILIATION_REQUIRED':
                return data  # A later message never clears a recorded conflict.
            if old and old['state'] != 'OUTCOME_UNKNOWN':
                if reply['state'] == 'OUTCOME_UNKNOWN' or old == reply:
                    return data  # Do not downgrade known facts on a late timeout.
                data['phase'] = 'CONFLICT_RECONCILIATION_REQUIRED'
                # Save both normalized claims, then COMMIT the conflict, not rollback.
                data['conflicting_reply'] = reply
                return data
            data.update(reply=reply,reply_at_ms=now_ms,
                phase={'ACCEPTED_UNVERIFIED':'WAITING_FOR_EVIDENCE',
                       'OUTCOME_UNKNOWN':'OUTCOME_UNKNOWN','REJECTED':'REJECTED_DO_NOT_RETRY'}[reply['state']])
            return data
        return self._for_request(request_id,event_id,'REPLY',reply,expected_revision,now_ms,mutate)

    def confirm_observed(self, request_id, bindings, snapshot, context, *, event_id, expected_revision, now_ms):
        evidence = _evidence(bindings,snapshot,context)
        def mutate(data):
            bindings, snapshot, context = (evidence[k] for k in ('bindings','snapshot','context'))
            if data['phase'] != 'WAITING_FOR_EVIDENCE':
                raise RecoveryStorageError('ACKNOWLEDGED_REQUEST_REQUIRES_OBSERVED_EFFECT')
            if snapshot['at_ms'] <= max(data['attempt_started_at_ms'],data['reply_at_ms']):
                raise RecoveryStorageError('POST_REPLY_EVIDENCE_REQUIRED')
            _continues(data['original_evidence'],evidence)
            pending = dict(request_id=request_id,step=data['proposal']['next_step'],basis=data['proposal']['basis'],
                reserved_at_ms=data['attempt_started_at_ms'],reply=data['reply'])
            rehearsal = recovery.Rehearsal(data['bucket'],dict(bucket=data['bucket'],revision=1,
                status='WAITING_FOR_EVIDENCE',pending=pending,attempts=1,history=[]))
            result = rehearsal.confirm(bindings,snapshot,context,now_ms=now_ms)
            data.update(phase='OBSERVED',confirmation=dict(evidence=evidence,
                acknowledged_effect_observed=True,
                recovered_card_needs_more_work=result['recovered_card_needs_more_work']))
            return data
        return self._for_request(request_id,event_id,'CONFIRM_OBSERVED',evidence,expected_revision,now_ms,mutate)

    def abandon_unsent(self, request_id, *, event_id, expected_revision, now_ms):
        """Only an unattempted plan can be retired locally without exchange proof."""
        def mutate(data):
            if data['phase'] != 'PREPARED_NOT_SENT' or data['attempt_started_at_ms'] is not None:
                raise RecoveryStorageError('UNCERTAIN_REQUEST_CANNOT_BE_ABANDONED')
            data['phase'] = 'ABORTED_NOT_SENT'
            return data
        return self._for_request(request_id,event_id,'ABANDON_UNSENT',{},expected_revision,now_ms,mutate)

    def load(self, request_id):
        _hex(request_id)
        with self.journal._transaction() as conn:
            self.ready(conn)
            bucket = self._read(conn,request_id)['bucket']
            self._lock(conn,bucket)
            data = self._read(conn,request_id)
            revision = conn.execute(f'SELECT revision FROM {SCHEMA}.buckets WHERE bucket=%s', (data['bucket'],)).fetchone()[0]
        return self._result(data,revision)

    def pending(self, account, symbol):
        bucket = self.bucket(account,symbol)
        with self.journal._transaction() as conn:
            self.ready(conn)
            self._lock(conn,bucket)
            row = conn.execute(f'SELECT revision,active_id FROM {SCHEMA}.buckets WHERE bucket=%s', (bucket,)).fetchone()
            if row is None or row[1] is None:
                return dict(revision=0 if row is None else row[0],request=None,
                    simulation_only=True,dispatch_enabled=False,order_requests_sent=0)
            data = self._read(conn,row[1])
            if data['bucket'] != bucket or data['phase'] in TERMINAL:
                raise RecoveryStorageError('RECOVERY_BUCKET_INTEGRITY_FAILURE')
        return self._result(data,row[0])
