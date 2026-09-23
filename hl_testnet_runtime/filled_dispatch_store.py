"""Persistent new-style Testnet requests; never convert software rehearsals.

Use the existing PostgreSQL connection/expiry policy and recovery lock. DDL is
explicit and occurs once. No venue calls, keys, retries or scheduling here.
"""
from copy import deepcopy
from . import card_lifecycle as life
from .postgres_journal import JournalError
from .card_recovery_journal import RecoveryJournal

SCHEMA = 'hl_testnet_filled_dispatch_v1'
VERSION = 'filled-dispatch-store-v1'
DONE = ('OBSERVED', 'ABORTED_UNSENT')
PHASES = ('PREPARED', 'OUTCOME_UNKNOWN', 'ACK_UNVERIFIED', 'REJECTED', 'CONFLICT') + DONE


class DispatchError(JournalError, life.LifecycleError):
    """Stable redacted errors only."""


def encode(value):
    text = life.encoded(value)
    if len(text.encode()) > 4 * 1024 * 1024:
        raise DispatchError('DISPATCH_RECORD_TOO_LARGE')
    return text


def checked(row):
    if row is None or life.digest(row[0]) != row[1]:
        raise DispatchError('DISPATCH_RECORD_INTEGRITY_FAILURE')
    return deepcopy(row[0])


class DispatchStore:
    def __init__(self, journal):
        self.journal = journal
        self.domain = 'software' if journal._ci else 'testnet'

    def ready(self, conn):
        if conn.execute(f'SELECT version,domain FROM {SCHEMA}.metadata WHERE singleton').fetchone() != (VERSION,self.domain):
            raise DispatchError('DISPATCH_SCHEMA_OR_DOMAIN_MISMATCH')

    def initialize(self):
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (1729048230,))
            if conn.execute('SELECT to_regnamespace(%s)', (SCHEMA,)).fetchone()[0] is not None:
                self.ready(conn)
                return False
            conn.execute(f'CREATE SCHEMA {SCHEMA}')
            conn.execute(f'CREATE TABLE {SCHEMA}.metadata(singleton boolean PRIMARY KEY CHECK(singleton),version text NOT NULL,domain text NOT NULL)')
            conn.execute(f'INSERT INTO {SCHEMA}.metadata VALUES(true,%s,%s)',(VERSION,self.domain))
            conn.execute(f'''CREATE TABLE {SCHEMA}.buckets(
                bucket text PRIMARY KEY,revision bigint NOT NULL,value jsonb NOT NULL,digest text NOT NULL)''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.requests(
                request_id text PRIMARY KEY,bucket text NOT NULL REFERENCES {SCHEMA}.buckets,
                phase text NOT NULL CHECK(phase IN {str(PHASES)}),value jsonb NOT NULL,digest text NOT NULL)''')
            conn.execute(f'''CREATE UNIQUE INDEX one_unresolved_filled_request ON {SCHEMA}.requests(bucket)
                WHERE phase NOT IN ('OBSERVED','ABORTED_UNSENT')''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.ownership(
                account text NOT NULL,oid text NOT NULL,cloid text NOT NULL,card_id text NOT NULL,
                leg text NOT NULL,request_id text NOT NULL REFERENCES {SCHEMA}.requests,
                PRIMARY KEY(account,oid),UNIQUE(account,cloid))''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.nonces(agent text PRIMARY KEY,nonce bigint NOT NULL)''')
            conn.execute(f'''CREATE TABLE {SCHEMA}.events(
                bucket text NOT NULL,revision bigint NOT NULL,event text NOT NULL,request_id text,
                at_ms bigint NOT NULL,record_hash text NOT NULL,PRIMARY KEY(bucket,revision))''')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
            for name in ('metadata','buckets','requests','ownership','nonces','events'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{name} FROM PUBLIC')
        return True

    def load(self, bucket):
        life.ident(bucket,r'[0-9a-f]{64}')
        with self.journal._transaction() as conn:
            self.ready(conn)
            row=conn.execute(f'SELECT value,digest FROM {SCHEMA}.buckets WHERE bucket=%s',(bucket,)).fetchone()
            return checked(row)

    def request(self, request_id):
        with self.journal._transaction() as conn:
            self.ready(conn)
            row=conn.execute(f'SELECT value,digest,phase FROM {SCHEMA}.requests WHERE request_id=%s',(request_id,)).fetchone()
            result=checked(row)
            if result['phase'] != row[2] or result['domain'] != self.domain:
                raise DispatchError('DISPATCH_PHASE_OR_DOMAIN_MISMATCH')
            return result

    def create_bucket(self, account, symbol):
        account=life.address(account);life.ident(symbol,r'[A-Z][A-Z0-9]{0,19}')
        bucket=RecoveryJournal.bucket(account,symbol)
        value=dict(version=VERSION,domain=self.domain,bucket=bucket,account=account,symbol=symbol,
                   revision=0,originals={},bindings=[],evidence=None,pending=None,last_request=None)
        with self.journal._transaction() as conn:
            self.ready(conn);RecoveryJournal._lock(conn,bucket)
            conn.execute(f'INSERT INTO {SCHEMA}.buckets VALUES(%s,0,%s::jsonb,%s) ON CONFLICT DO NOTHING',
                         (bucket,encode(value),life.digest(value)))
        return self.load(bucket)

    def change(self, bucket, revision, event, now_ms, update):
        """An uncertain COMMIT raises; caller MUST NOT proceed to signing."""
        life.moment(now_ms)
        if type(revision) is not int or revision < 0:
            raise DispatchError('EXACT_REVISION_REQUIRED')
        with self.journal._transaction() as conn:
            self.ready(conn);RecoveryJournal._lock(conn,bucket)
            row=conn.execute(f'SELECT value,digest,revision FROM {SCHEMA}.buckets WHERE bucket=%s FOR UPDATE',(bucket,)).fetchone()
            state=checked(row)
            if row[2] != revision or state['revision'] != revision or state['domain'] != self.domain:
                raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
            # The offline recovery journal shares this lock. Never bypass its uncertainty.
            exists=conn.execute('SELECT to_regclass(%s)',('hl_testnet_recovery_rehearsal_v1.buckets',)).fetchone()[0]
            if exists:
                old=conn.execute('SELECT active_id FROM hl_testnet_recovery_rehearsal_v1.buckets WHERE bucket=%s',(bucket,)).fetchone()
                if old and old[0] is not None:
                    raise DispatchError('PRIOR_RECOVERY_MUST_BE_RESOLVED')
            try:
                request=update(conn,state)
            except life.LifecycleError as exc:
                raise DispatchError(str(exc)) from None
            state['revision']+=1
            conn.execute(f'UPDATE {SCHEMA}.buckets SET revision=%s,value=%s::jsonb,digest=%s WHERE bucket=%s',
                         (state['revision'],encode(state),life.digest(state),bucket))
            if request is not None:
                request['updated_at_ms']=now_ms
                conn.execute(f'''INSERT INTO {SCHEMA}.requests VALUES(%s,%s,%s,%s::jsonb,%s)
                    ON CONFLICT(request_id) DO UPDATE SET phase=EXCLUDED.phase,value=EXCLUDED.value,digest=EXCLUDED.digest''',
                    (request['request_id'],bucket,request['phase'],encode(request),life.digest(request)))
            conn.execute(f'INSERT INTO {SCHEMA}.events VALUES(%s,%s,%s,%s,%s,%s)',
                         (bucket,state['revision'],event,request['request_id'] if request else None,now_ms,life.digest([state,request])))
        return self.load(bucket)

    @staticmethod
    def pending_record(conn, state):
        if state['pending'] is None:
            return None
        row=conn.execute(f'SELECT value,digest,phase FROM {SCHEMA}.requests WHERE request_id=%s',(state['pending'],)).fetchone()
        value=checked(row)
        if value['phase'] != row[2] or value['phase'] in DONE:
            raise DispatchError('PENDING_RECORD_INTEGRITY_FAILURE')
        return value

    def reserve(self, state, proposal, now_ms):
        def update(conn, value):
            if value['pending'] is not None:
                raise DispatchError('UNRESOLVED_REQUEST_NO_NEW_INTENT')
            if proposal is None or proposal.get('basis') != life.digest(value['evidence']):
                raise DispatchError('PROPOSAL_EVIDENCE_CHANGED')
            from .residual_exit_fence import validate_proposal
            validate_proposal(value, proposal, now_ms=now_ms)
            rid=life.digest([VERSION,value['bucket'],value['revision']+1,proposal])
            request=dict(request_id=rid,domain=self.domain,bucket=value['bucket'],phase='PREPARED',
                proposal=deepcopy(proposal),prepared_at_ms=now_ms,attempt_at_ms=None,nonce=None,
                reply=None,observed_oid=None,attempts=0,updated_at_ms=now_ms)
            value['pending']=rid;value['last_request']=rid
            return request
        return self.change(state['bucket'],state['revision'],'PREPARE',now_ms,update)

    def begin(self, state, proposal, agent, now_ms):
        def update(conn, value):
            request=self.pending_record(conn,value)
            if request is None or request['phase'] != 'PREPARED':
                raise DispatchError('ATTEMPT_MAY_HAVE_BEEN_SENT_NO_REPEAT')
            if request['proposal'] != proposal or now_ms < request['prepared_at_ms']:
                raise DispatchError('PRE_SEND_PLAN_CHANGED')
            at=value['evidence']['snapshot']['at_ms']
            if not 0 <= now_ms-at <= 15000:
                raise DispatchError('PRE_SEND_EVIDENCE_EXPIRED')
            from .residual_exit_fence import validate_proposal
            validate_proposal(value, proposal, now_ms=now_ms)
            agent=life.address(agent_address)
            nonce=conn.execute(f'''INSERT INTO {SCHEMA}.nonces VALUES(%s,%s)
                ON CONFLICT(agent) DO UPDATE SET nonce=GREATEST({SCHEMA}.nonces.nonce+1,EXCLUDED.nonce)
                RETURNING nonce''',(agent,now_ms)).fetchone()[0]
            if nonce > now_ms+1000:
                raise DispatchError('NONCE_CLOCK_REQUIRES_REVIEW')
            request.update(phase='OUTCOME_UNKNOWN',nonce=nonce,attempt_at_ms=now_ms,attempts=1)
            return request
        agent_address=agent
        return self.change(state['bucket'],state['revision'],'ATTEMPT_BEGUN',now_ms,update)

    def reply(self, state, reply, now_ms):
        def update(conn,value):
            request=self.pending_record(conn,value)
            if request is None or request['attempt_at_ms'] is None:
                raise DispatchError('NO_STARTED_REQUEST')
            old=request['reply']
            if old and old != reply:
                request['phase']='CONFLICT'
            elif request['phase'] != 'CONFLICT':
                request['reply']=deepcopy(reply)
                request['phase']={'ACCEPTED_UNVERIFIED':'ACK_UNVERIFIED','REJECTED':'REJECTED',
                                  'OUTCOME_UNKNOWN':'OUTCOME_UNKNOWN'}[reply['state']]
            if now_ms < request['attempt_at_ms']:
                raise DispatchError('REPLY_TIME_MOVED_BACKWARDS')
            return request
        return self.change(state['bucket'],state['revision'],'REPLY',now_ms,update)
