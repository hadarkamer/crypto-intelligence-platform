"""Durable bounded ENTRY trials per role/release epoch, Testnet only.

Every begun attempt consumes the cap, including unknown, rejected and provably
unsent outcomes. Maintenance is unaffected. The fast scan grants no authority;
the final guard runs under the dispatch store's common PostgreSQL locks.
"""
from . import card_lifecycle as life, emergency_close as emergency
from .filled_dispatch_store import DispatchError, SCHEMA
from .source_window import timestamp

MODE = 'one_per_role_v1'
WINDOW_MODE = 'five_per_role_48h_v1'
WINDOW_MS = 48 * 60 * 60 * 1000
MAX_ATTEMPTS = 5


def configuration(env, role, account=None):
    mode=env.get('HL_TESTNET_ENTRY_ATTEMPT_CAP','')
    if mode=='':
        return None
    if mode not in (MODE, WINDOW_MODE) or not emergency.continuous_configuration(env):
        raise DispatchError('BOUNDED_ENTRY_TRIAL_CONFIGURATION_REQUIRED')
    route=emergency.dispatch.roles.route_for(env,role,account)
    key='HL_TESTNET_LONG_NOT_BEFORE' if role=='long_account' else 'HL_TESTNET_SHORT_NOT_BEFORE'
    try:
        epoch=int(timestamp(env[key]).timestamp()*1000)
        life.moment(epoch)
    except (KeyError,TypeError,ValueError):
        raise DispatchError('BOUNDED_ENTRY_TRIAL_ORIGINAL_EPOCH_REQUIRED') from None
    scope = dict(role=role,account=route['account'],epoch_ms=epoch)
    if mode == WINDOW_MODE:
        scope.update(max_attempts=MAX_ATTEMPTS, deadline_ms=epoch+WINDOW_MS)
    return scope


def _window_active(conn, scope):
    if 'deadline_ms' not in scope:
        return True
    # Database wall time is read at each gate, including after lock waits.
    # A restart or a stale caller timestamp cannot extend the authorization.
    now = conn.execute("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint").fetchone()[0]
    if type(now) is not int:
        raise DispatchError('BOUNDED_ENTRY_TRIAL_CLOCK_REQUIRED')
    return scope['epoch_ms'] <= now < scope['deadline_ms']


def _limit(scope):
    return scope.get('max_attempts', 1)


def _attempts(conn, scope):
    # These are the existing durable request records; no extra schema or
    # process-local counter can reset the cap after a restart or lost reply.
    rows=conn.execute(f'''SELECT value,digest FROM {SCHEMA}.requests
        WHERE value->'proposal'->>'operation'='ENTRY'
          AND value->'proposal'->>'role'=%s
          AND value->'proposal'->>'account'=%s
          AND (value->>'attempts')::integer>0
          AND (value->>'attempt_at_ms')::bigint>=%s LIMIT {_limit(scope)+1}''',
        (scope['role'],scope['account'],scope['epoch_ms'])).fetchall()
    attempts=[]
    for value,digest in rows:
        if life.digest(value)!=digest:
            raise DispatchError('BOUNDED_ENTRY_TRIAL_REQUEST_INTEGRITY_REQUIRED')
        attempts.append(value)
    return attempts


def cap_reached(store, env, role, account=None):
    scope=configuration(env,role,account)
    if scope is None:
        return False
    with store.journal._transaction() as conn:
        store.ready(conn)
        return not _window_active(conn,scope) or len(_attempts(conn,scope)) >= _limit(scope)


def check_entry(store, env, proposal):
    """Read-only early/final boundary, allowing only the exact just-begun send."""
    if proposal['operation']!='ENTRY':
        return
    scope=configuration(env,proposal['role'],proposal['account'])
    if scope is None:
        return
    with store.journal._transaction() as conn:
        store.ready(conn)
        if not _window_active(conn,scope):
            raise DispatchError('BOUNDED_ENTRY_TRIAL_WINDOW_CLOSED')
        attempts=_attempts(conn,scope)
        own = [r for r in attempts if r['proposal']==proposal]
        if not own and len(attempts) < _limit(scope):
            if any(r['phase']=='OUTCOME_UNKNOWN' for r in attempts):
                raise DispatchError('BOUNDED_ENTRY_TRIAL_UNRESOLVED_ATTEMPT')
            return
        # send() gates again AFTER the positively acknowledged durable begin.
        # Only this exact one-attempt unresolved request can finish that send;
        # a new card/proposal cannot borrow the already-consumed permission.
        if (len(own)==1 and len(attempts)<=_limit(scope)
                and own[0]['phase']=='OUTCOME_UNKNOWN' and own[0]['attempts']==1
                and all(r is own[0] or r['phase']!='OUTCOME_UNKNOWN' for r in attempts)):
            row=conn.execute(f'SELECT value,digest FROM {SCHEMA}.buckets WHERE bucket=%s',
                             (own[0]['bucket'],)).fetchone()
            if row and life.digest(row[0])==row[1] and row[0].get('pending')==own[0]['request_id']:
                return
        raise DispatchError('BOUNDED_ENTRY_TRIAL_CAP_REACHED')


def entry_guard(env, proposal):
    """Return optional transaction-local guard; call BEFORE nonce/attempt writes."""
    if proposal['operation']!='ENTRY':
        return None
    scope=configuration(env,proposal['role'],proposal['account'])
    if scope is None:
        return None
    def guard(conn,state,current):
        if (current!=proposal or state['account']!=scope['account']
                or current['role']!=scope['role']):
            raise DispatchError('BOUNDED_ENTRY_TRIAL_SCOPE_CHANGED')
        if not _window_active(conn,scope):
            raise DispatchError('BOUNDED_ENTRY_TRIAL_WINDOW_CLOSED')
        attempts = _attempts(conn,scope)
        if len(attempts) >= _limit(scope):
            raise DispatchError('BOUNDED_ENTRY_TRIAL_CAP_REACHED')
        if any(r['phase']=='OUTCOME_UNKNOWN' for r in attempts):
            raise DispatchError('BOUNDED_ENTRY_TRIAL_UNRESOLVED_ATTEMPT')
    return guard
