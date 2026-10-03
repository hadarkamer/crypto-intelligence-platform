"""Audited, read-only recovery of old, already final Testnet market history.

Never changes an order, discovers ownership, fabricates an evidence clock, or
increases request quotas. Each successful step stages one independently checked
12-hour window. Only a final fresh two-pass lifecycle proof replaces evidence.
"""
from contextlib import nullcontext
from copy import deepcopy
import time

from . import card_lifecycle as life, card_sync_evidence as evidence
from .dispatch_concurrency import market_lane

VERSION='history_gap_recovery_v1'
KEY='history_gap_recovery'
WINDOW_MS=evidence.DAY_MS//2


class RecoveryError(evidence.SyncError):
    """Fixed codes only; callers keep entries blocked and retain old facts."""


def _basis(state):
    value=deepcopy(state)
    for key in ('revision',KEY,'history_gap_recovery_last'):
        value.pop(key,None)
    return life.digest(value)


def _final_proof(state):
    """Revalidate durable ownership and final quantities at the original clock."""
    if (not isinstance(state,dict) or state.get('pending') is not None
            or state.get('emergency') is not None or not state.get('bindings')
            or not isinstance(state.get('evidence'),dict)):
        raise RecoveryError('HISTORY_RECOVERY_FINAL_IDLE_BUCKET_REQUIRED')
    bs=state['bindings']; value=state['evidence']; snap=value['snapshot']
    account,symbol=life.validate_snapshot(snap)
    if (account!=state['account'] or symbol!=state['symbol'] or bs!=value['bindings']
            or not snap['history_complete'] or not snap['orders_complete']
            or snap['open_orders'] or life.number(snap['position_quantity'],signed=True)!=0):
        raise RecoveryError('HISTORY_RECOVERY_FINAL_COMPLETE_PROOF_REQUIRED')
    for binding in bs:
        if binding['account']!=account or binding['symbol']!=symbol:
            raise RecoveryError('HISTORY_RECOVERY_MIXED_BUCKET')
        original=state['originals'].get(binding['card_id'])
        if not isinstance(original,dict) or 'card' not in original:
            raise RecoveryError('HISTORY_RECOVERY_IMMUTABLE_ORIGINAL_REQUIRED')
        reconstructed=life.binding_from_card(original['card'],account,
            {binding['role']:dict(account=account)},binding['orders'])
        if reconstructed!=binding:
            raise RecoveryError('HISTORY_RECOVERY_IMMUTABLE_BINDING_CHANGED')
    certificates=evidence.terminal_certificates(bs,snap)
    links={oid for binding in bs for leg in life.order_legs(binding)
           for oid in binding['orders'][leg]}
    if set(certificates)!=links:
        raise RecoveryError('HISTORY_RECOVERY_ALL_OWNED_ORDERS_FINAL_REQUIRED')
    report=life.review(bs,snap,now_ms=snap['at_ms'])
    if (report['needs_review'] or report['bucket_issues']
            or any(row['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')
                   or row['issues'] or life.number(row['remaining_quantity'])!=0
                   for row in report['cards'])):
        raise RecoveryError('HISTORY_RECOVERY_CLEAN_FINAL_REVIEW_REQUIRED')
    return snap


def needed(state, now_ms):
    """An existing staged record always fences ordinary refresh and entries."""
    if isinstance(state,dict) and KEY in state:
        return True
    try:
        snap=_final_proof(state)
        return life.moment(now_ms)-snap['at_ms']>evidence.MAX_CATCHUP_MS
    except (life.LifecycleError,KeyError,TypeError,ValueError):
        return False


def _anchor_basis(state):
    snap=_final_proof(state)
    originals={binding['card_id']:state['originals'][binding['card_id']]
               for binding in state['bindings']}
    return life.digest(dict(account=state['account'],symbol=state['symbol'],
        bindings=state['bindings'],originals=originals,snapshot=evidence.facts(snap)))


def _select_anchor(controller, state, base):
    candidates=[]
    # The venue retains account-wide fills, so a verified fill from another
    # market can bound availability for a zero-fill, canceled-only bucket.
    for other in controller.store.for_account(state['account']):
        try:
            snap=_final_proof(other); digest=_anchor_basis(other)
            for fill in snap['fills']:
                if fill['at_ms']<=base and fill['fill_id'].startswith('hl:'):
                    candidates.append(dict(bucket=other['bucket'],basis_digest=digest,
                                           fill=deepcopy(fill)))
        except (life.LifecycleError,KeyError,TypeError,ValueError):
            continue
    if not candidates:
        raise RecoveryError('HISTORY_RETENTION_DURABLE_ANCHOR_REQUIRED')
    return max(candidates,key=lambda row:(row['fill']['at_ms'],row['fill']['fill_id'],row['bucket']))


def _check_anchor(controller, anchor, account, base):
    if (not isinstance(anchor,dict) or set(anchor)!={'bucket','basis_digest','fill'}
            or anchor['fill']['account']!=account or anchor['fill']['at_ms']>base):
        raise RecoveryError('HISTORY_RECOVERY_ANCHOR_INVALID')
    other=controller.store.load(anchor['bucket'])
    if (_anchor_basis(other)!=anchor['basis_digest']
            or anchor['fill'] not in other['evidence']['snapshot']['fills']):
        raise RecoveryError('HISTORY_RECOVERY_ANCHOR_CHANGED')


def _stage(controller,state,now):
    snap=_final_proof(state); base=snap['at_ms']
    if now<base:
        raise RecoveryError('HISTORY_RECOVERY_CLOCK_REGRESSED')
    stage=state.get(KEY)
    if KEY not in state:
        if now-base<=evidence.MAX_CATCHUP_MS:
            raise RecoveryError('HISTORY_RECOVERY_NOT_REQUIRED')
        return dict(version=VERSION,base_at_ms=base,cursor_ms=base,chunks=0,
                    basis_digest=_basis(state),anchor=_select_anchor(controller,state,base),
                    staged_fills=deepcopy(snap['fills']),started_at_ms=now,last_checked_at_ms=now)
    if (not isinstance(stage,dict) or set(stage)!={'version','base_at_ms','cursor_ms','chunks',
            'basis_digest','anchor','staged_fills','started_at_ms','last_checked_at_ms'}
            or stage['version']!=VERSION or stage['base_at_ms']!=base
            or stage['basis_digest']!=_basis(state)
            or type(stage['chunks']) is not int or stage['chunks']<0
            or stage['cursor_ms']!=base+stage['chunks']*WINDOW_MS
            or not base<=stage['cursor_ms']<=now
            or stage['last_checked_at_ms']>now or stage['started_at_ms']>now
            or stage['staged_fills']!=snap['fills']):
        raise RecoveryError('HISTORY_RECOVERY_STAGE_CHANGED_OR_CLOCK_REGRESSED')
    for key in ('base_at_ms','cursor_ms','started_at_ms','last_checked_at_ms'):
        life.moment(stage[key])
    return deepcopy(stage)


def _history_body(account,start,end):
    return dict(type='userFillsByTime',user=account,startTime=start,endTime=end,
                aggregateByTime=False)


def planned_chunk_reads(account, anchor, start, end):
    stamp=life.moment(anchor['fill']['at_ms'])
    return [_history_body(account,stamp,stamp),_history_body(account,start,end),
            _history_body(account,start,end),_history_body(account,stamp,stamp)]


def _reader(controller):
    return evidence.PublicReader(parallel=True,budget=controller.venue.request_budget(),
                                 priority='background')


def _feed_stamp(controller,account):
    method=getattr(type(controller),'_feed_stamp',None)
    return controller._feed_stamp(account) if callable(method) else None


def step(controller, state, *, reader=None, clock=None, elapsed=time.monotonic):
    return _step(controller,state['bucket'],state,reader=reader,clock=clock,elapsed=elapsed)


@market_lane
def _step(controller, bucket, state, *, reader=None, clock=None, elapsed=time.monotonic):
    """Stage one read-only chunk, or commit the final real current observation.

    Exceptions, including an uncertain COMMIT acknowledgement, require a fresh
    durable reload. There is no retry loop or order capability in this module.
    Optional readers/clocks replace only external boundaries for offline tests.
    """
    clock=controller.venue.now if clock is None else clock
    now,started=life.moment(clock()),elapsed()
    if (controller.store.domain not in ('testnet','software')
            or controller.store.domain!=controller.venue.domain):
        raise RecoveryError('HISTORY_RECOVERY_TESTNET_DOMAIN_REQUIRED')
    if controller.store.load(state['bucket'])!=state:
        raise RecoveryError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
    stage=_stage(controller,state,now); base=stage['base_at_ms']; cursor=stage['cursor_ms']
    anchor=stage['anchor']; account=state['account']; symbol=state['symbol']
    _check_anchor(controller,anchor,account,base)
    stamp=_feed_stamp(controller,account)
    if getattr(controller.venue,'domain',None)=='testnet' and stamp is None:
        raise RecoveryError('HISTORY_RECOVERY_LIVE_FEED_BOUNDARY_REQUIRED')
    if KEY not in state:
        # Establish the durable account-wide ENTRY fence before any exchange
        # read. Failed/truncated history never silently releases this latch.
        def start_recovery(conn,current):
            if current!=state or _basis(current)!=stage['basis_digest']:
                raise RecoveryError('HISTORY_RECOVERY_BASIS_CHANGED')
            if _feed_stamp(controller,account)!=stamp:
                raise RecoveryError('HISTORY_RECOVERY_FEED_CHANGED')
            if conn is not None:
                from .filled_dispatch_store import SCHEMA,checked
                row=conn.execute(f'SELECT value,digest FROM {SCHEMA}.buckets WHERE bucket=%s',
                                 (anchor['bucket'],)).fetchone()
                source=checked(row)
                if (_anchor_basis(source)!=anchor['basis_digest']
                        or anchor['fill'] not in source['evidence']['snapshot']['fills']):
                    raise RecoveryError('HISTORY_RECOVERY_ANCHOR_CHANGED')
            current[KEY]=deepcopy(stage)
        committed=controller._checkpoint(state['bucket'],state,'HISTORY_GAP_RECOVERY_STARTED',now,start_recovery)
        return dict(status='HISTORY_GAP_RECOVERY_PROGRESS',state=committed,cursor_ms=cursor,
                    remaining_ms=now-cursor,order_requests_sent=0)
    reader=_reader(controller) if reader is None else reader
    complete=now-cursor<=WINDOW_MS
    account_states=None
    if complete:
        account_states=sorted(controller.store.for_account(account),key=lambda row:row['bucket'])
        selected_roles=[role for role,route in controller.routes.items()
                        if life.address(route['account'])==account]
        if len(selected_roles)!=1:
            raise RecoveryError('HISTORY_RECOVERY_EXACT_ACCOUNT_ROLE_REQUIRED')
        def inventory_guard(orders,positions):
            from .long_stream_runtime import _validate_account_inventory
            _validate_account_inventory(account,account_states,orders,positions,role=selected_roles[0])
        result=evidence._collect_retained(dict(bindings=state['bindings'],
            snapshot=state['evidence']['snapshot']),reader,cursor_ms=cursor,
            anchor=anchor['fill'],clock=lambda:now,elapsed=elapsed,inventory_guard=inventory_guard)
        snap=result['snapshot']; report=result['report']
        if (snap['fills']!=sorted(stage['staged_fills'],key=lambda row:(row['at_ms'],row['fill_id']))
                or evidence.facts(snap)!=evidence.facts(state['evidence']['snapshot'])
                or report['needs_review'] or report['bucket_issues']
                or snap['open_orders'] or life.number(snap['position_quantity'],signed=True)!=0):
            raise RecoveryError('HISTORY_RECOVERY_NEW_OR_UNRESOLVED_ACTIVITY')
        new_cursor=now
    else:
        new_cursor=cursor+WINDOW_MS; start=max(1,cursor-evidence.OVERLAP_MS)
        plan=getattr(type(reader),'observation_batch',None)
        context=(reader.observation_batch(planned_chunk_reads(account,anchor,start,new_cursor))
                 if callable(plan) else nullcontext())
        with context:
            evidence.retained_anchor(reader,account,anchor['fill'])
            first=evidence.merge_fills(stage['staged_fills'],
                evidence.history(reader,account,start,new_cursor),account,symbol,start,new_cursor)
            second=evidence.merge_fills(stage['staged_fills'],
                evidence.history(reader,account,start,new_cursor),account,symbol,start,new_cursor)
            if first!=second:
                raise RecoveryError('OBSERVATION_CHANGED_RETRY')
            if first!=sorted(stage['staged_fills'],key=lambda row:(row['at_ms'],row['fill_id'])):
                raise RecoveryError('HISTORY_RECOVERY_NEW_OR_UNRESOLVED_ACTIVITY')
            evidence.retained_anchor(reader,account,anchor['fill'])
    finished=life.moment(clock())
    if elapsed()-started>15 or not now<=finished<=now+15000:
        raise RecoveryError('OBSERVATION_TOO_SLOW_OR_CLOCK_REGRESSED')
    if _feed_stamp(controller,account)!=stamp:
        raise RecoveryError('HISTORY_RECOVERY_FEED_CHANGED')
    _check_anchor(controller,anchor,account,base)
    event='HISTORY_GAP_RECOVERY_COMPLETE' if complete else 'HISTORY_GAP_RECOVERY_CHUNK'
    def update(conn,current):
        if current!=state or _basis(current)!=stage['basis_digest']:
            raise RecoveryError('HISTORY_RECOVERY_BASIS_CHANGED')
        if _feed_stamp(controller,account)!=stamp:
            raise RecoveryError('HISTORY_RECOVERY_FEED_CHANGED')
        # Anchor rows and target state share DispatchStore's global transaction
        # lock. Verify the other immutable bucket inside this same CAS as well.
        if conn is not None:
            from .filled_dispatch_store import SCHEMA,checked
            row=conn.execute(f'SELECT value,digest FROM {SCHEMA}.buckets WHERE bucket=%s',
                             (anchor['bucket'],)).fetchone()
            source=checked(row)
            if (_anchor_basis(source)!=anchor['basis_digest']
                    or anchor['fill'] not in source['evidence']['snapshot']['fills']):
                raise RecoveryError('HISTORY_RECOVERY_ANCHOR_CHANGED')
        if complete:
            if conn is not None:
                rows=conn.execute(f'''SELECT value,digest FROM {SCHEMA}.buckets
                    WHERE value->>'account'=%s ORDER BY bucket LIMIT 257''',(account,)).fetchall()
                current_account=[checked(row) for row in rows]
            else:
                current_account=sorted(controller.store.for_account(account),key=lambda row:row['bucket'])
            if current_account!=account_states:
                raise RecoveryError('HISTORY_RECOVERY_ACCOUNT_BASIS_CHANGED')
            from .card_lifecycle_store import _continues
            updated=dict(bindings=deepcopy(state['bindings']),snapshot=deepcopy(snap))
            _continues(state['evidence'],updated)
            current['evidence']=updated
            current.pop(KEY,None)
            current['history_gap_recovery_last']=dict(version=VERSION,base_at_ms=base,
                cursor_ms=new_cursor,chunks=stage['chunks'],basis_digest=stage['basis_digest'],
                anchor_digest=life.digest(anchor),completed_at_ms=finished)
        else:
            stage['cursor_ms']=new_cursor; stage['chunks']+=1; stage['last_checked_at_ms']=finished
            current[KEY]=deepcopy(stage)
    committed=controller._checkpoint(state['bucket'],state,event,finished,update)
    return dict(status='HISTORY_GAP_RECOVERY_COMPLETE' if complete else 'HISTORY_GAP_RECOVERY_PROGRESS',
                state=committed,cursor_ms=new_cursor,remaining_ms=max(0,finished-new_cursor),
                order_requests_sent=0)
