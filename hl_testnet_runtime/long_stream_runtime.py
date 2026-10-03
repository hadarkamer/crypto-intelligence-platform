"""One durable Testnet worker for delivered cards in two separate accounts.

The producer selects formulas. This worker resumes every owned market bucket
before considering fresh cards. It never takes execution instructions from the
application or an HTTP request. Disabling new entries keeps exits running.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import re
import threading
import traceback

from . import approved_alert_selection as selection, card_lifecycle as life
from . import history_gap_recovery as gap
from . import filled_quantity_dispatch as dispatch, two_account_execution as roles
from .filled_dispatch_store import DispatchError
from .source_window import source_fresh, timestamp
from .trade_card_store import CardStore
from . import trade_cards

MODE = 'long_stream_testnet_v1'
RELEASE = 'approved_long_stream_v1'
SOURCE = 'approved_alerts_v1'
_stop = threading.Event()
_wake = threading.Event()
_lock = threading.Lock()
_thread = None
_app_thread = None
_fill_wakeups = None
_health = dict(configured=False, running=False, last_status='DISABLED',
               cycles=0, order_requests_sent=0, new_entries_enabled=False,
               short_entries_enabled=False, short_stream_configured=False,
               app_delivery_status='DISABLED')


def configuration(env):
    if (env.get('RENDER_SERVICE_ID') != roles.SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') != MODE
            or env.get('HL_TESTNET_FILLED_DISPATCH') != RELEASE
            or env.get('HL_TESTNET_LONG_STREAM') != SOURCE
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') not in ('true','false')
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY') != dispatch.AFTER_EXIT
            or env.get('HL_TESTNET_SAFETY_PIPELINE')
            or env.get('HL_TESTNET_CARD_SYNC')
            or env.get('HL_TESTNET_FILLED_AUTOWAIT')
            or env.get('HL_TESTNET_FILLED_CARD_ID')
            or env.get('HL_TESTNET_CARDS_PHASE1') != 'record_only_v1'
            or env.get('HL_TESTNET_CARDS_INTAKE') != 'record_only_v1'):
        raise DispatchError('LONG_STREAM_CONFIGURATION_REQUIRED')
    from .alert_cards_intake import enabled as intake_enabled
    if not intake_enabled(env):
        raise DispatchError('LONG_STREAM_AUTHENTICATED_INTAKE_REQUIRED')
    try:
        start = timestamp(env['HL_TESTNET_LONG_NOT_BEFORE'])
    except (KeyError, TypeError, ValueError):
        raise DispatchError('LONG_STREAM_START_TIME_REQUIRED') from None
    if start > datetime.now(timezone.utc):
        raise DispatchError('LONG_STREAM_START_IN_FUTURE')
    route = roles.route_for(env, 'long_account', side='LONG')
    return route, start


def short_configuration(env):
    """Absence preserves the deployed long-only release; a partial opt-in fails."""
    keys = ('HL_TESTNET_SHORT_STREAM', 'HL_TESTNET_SHORT_ENTRY_ENABLED',
            'HL_TESTNET_SHORT_NOT_BEFORE')
    if not any(env.get(key) for key in keys):
        return None
    if (env.get('HL_TESTNET_SHORT_STREAM') != SOURCE
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') not in ('true', 'false')):
        raise DispatchError('SHORT_STREAM_CONFIGURATION_REQUIRED')
    try:
        start = timestamp(env['HL_TESTNET_SHORT_NOT_BEFORE'])
    except (KeyError, TypeError, ValueError):
        raise DispatchError('SHORT_STREAM_START_TIME_REQUIRED') from None
    if start > datetime.now(timezone.utc):
        raise DispatchError('SHORT_STREAM_START_IN_FUTURE')
    route = roles.route_for(env, 'short_account', side='SHORT')
    if env['HL_TESTNET_SHORT_ENTRY_ENABLED'] == 'true':
        roles.short_entry_scope(env)
        # Check the signer before the worker can reserve a durable request.
        roles.wallet_for_role(env, 'short_account', route['account'], route['agent'])
    return route, start


def _safe_failure(exc):
    """Expose only fixed internal codes and a source line; never private input."""
    from .request_budget import BudgetError
    code = str(exc)
    known = isinstance(exc, (DispatchError, roles.checks.Blocked, life.LifecycleError,BudgetError))
    safe_code = code if known and re.fullmatch(r'[A-Z][A-Z0-9_]{2,99}', code) else type(exc).__name__
    origin = traceback.extract_tb(exc.__traceback__)[-1]
    result = dict(failure_code=safe_code,
                  failure_origin=f'{os.path.basename(origin.filename)}:{origin.lineno}')
    if isinstance(exc, BudgetError):
        if exc.stage is not None:
            result['budget_stage'] = exc.stage
        if exc.elapsed_ms is not None:
            result['budget_elapsed_ms'] = exc.elapsed_ms
        for field in ('used_weight', 'requested_weight', 'ceiling', 'retry_after_ms'):
            value = getattr(exc, field, None)
            if value is not None:
                result['budget_' + field] = value
    return result


def _unfinished(state, now):
    if state['pending'] is not None:
        return True
    if state['bindings']:
        if state['evidence'] is None:
            return True
        snap = state['evidence']['snapshot']
        view = life.review(state['bindings'],snap,now_ms=snap['at_ms'])
        if (view['bucket_issues'] or snap['open_orders']
                or life.number(snap['position_quantity'],signed=True) != 0
                or any(c['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')
                       or c['issues'] for c in view['cards'])):
            return True
    bound = {b['card_id'] for b in state['bindings']}
    from .execution_occurrence import duplicate_attempt
    return any(cid not in bound and original.get('entry_rejected_no_retry') is not True
        and original.get('entry_unsent_no_retry') is not True and
        duplicate_attempt(state,cid) is None and
        'source_expires_at' in original['card'] and
        source_fresh(timestamp(original['card']['prepared']['source']['at']),
                     original['card']['source_expires_at'], now=now)
        for cid,original in state['originals'].items())


def idle_flat(state):
    """No in-flight work and every previously bound card/order is final."""
    if state['pending'] is not None or state['evidence'] is None:
        return False
    snap=state['evidence']['snapshot']
    if snap['open_orders'] or life.number(snap['position_quantity'],signed=True)!=0:
        return False
    if not state['bindings']:
        return True
    report=life.review(state['bindings'],snap,now_ms=snap['at_ms'])
    return not report['bucket_issues'] and all(not row['issues']
        and row['state'] in ('CLOSED','CANCELED_WITHOUT_FILL') for row in report['cards'])


def _account_owned(venue, account, states, *, role=None, priority='background', budget=None):
    """Unknown positions/orders block *new* entries, never exit maintenance."""
    from .card_sync_evidence import PublicReader
    reader = PublicReader(priority=priority,budget=budget)
    if getattr(venue,'parallel_preflight',False) is True:
        orders,positions=dispatch.joined_public_reads(
            lambda:reader.read('frontendOpenOrders',account),
            lambda:reader.read('clearinghouseState',account))
    else:
        orders = reader.read('frontendOpenOrders',account)
        positions = reader.read('clearinghouseState',account)
    return _validate_account_inventory(account, states, orders, positions, role=role)


def _validate_account_inventory(account, states, orders, positions, *, role=None):
    """Validate complete public account inventory without additional HTTP.

    Historical recovery reuses this gate on each already funded independent
    observation pass. The same ownership checks gate ordinary registration.
    """
    if (not isinstance(orders,list) or len(orders)>10000
            or not isinstance(positions,dict)
            or not isinstance(positions.get('assetPositions'),list)):
        raise DispatchError('LONG_ACCOUNT_INVENTORY_INVALID')
    by_symbol = {s['symbol']:s for s in states}
    if len(by_symbol) != len(states):
        raise DispatchError('DUPLICATE_MARKET_BUCKET')
    if any(state.get('account')!=account for state in states):
        raise DispatchError('ACCOUNT_INVENTORY_BUCKET_MISMATCH')
    if role is not None:
        if role not in roles.ROLES:
            raise DispatchError('EXPLICIT_ACCOUNT_ROLE_REQUIRED')
        if any(b['role'] != role for s in states for b in s['bindings']):
            raise DispatchError('ACCOUNT_BINDING_ROLE_MISMATCH')
    known = {(s['symbol'],oid) for s in states for b in s['bindings']
             for leg in life.LEGS for oid in b['orders'][leg]}
    final_orders={(s['symbol'],row['oid']) for s in states if s['evidence'] is not None
        for row in s['evidence']['snapshot']['terminal_orders']}
    expected = {s['symbol']:(life.number(s['evidence']['snapshot']['position_quantity'],signed=True)
               if s['evidence'] is not None else 0) for s in states}
    if role is not None and any(q and (q > 0) != (role == 'long_account')
                                for q in expected.values()):
        raise DispatchError('OPPOSITE_DIRECTION_ACCOUNT_EXPOSURE_NO_NEW_ENTRY')
    if any(s['pending'] is not None for s in states):
        raise DispatchError('UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')
    for row in orders:
        if (not isinstance(row,dict) or not isinstance(row.get('coin'),str)
                or type(row.get('oid')) is not int
                or row['coin'] not in by_symbol
                or (row['coin'],str(row['oid'])) not in known):
            raise DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')
        if (row['coin'],str(row['oid'])) in final_orders:
            raise DispatchError('FINAL_ACCOUNT_ORDER_REAPPEARED_RECONCILE_FIRST')
    seen=set()
    for row in positions['assetPositions']:
        if (not isinstance(row,dict) or not isinstance(row.get('position'),dict)
                or not isinstance(row['position'].get('coin'),str)):
            raise DispatchError('LONG_ACCOUNT_POSITION_INVALID')
        p=row['position']
        if p['coin'] in seen:
            raise DispatchError('DUPLICATE_ACCOUNT_POSITION')
        seen.add(p['coin'])
        actual=life.number(p.get('szi'),signed=True)
        if role is not None and actual and (actual > 0) != (role == 'long_account'):
            raise DispatchError('OPPOSITE_DIRECTION_ACCOUNT_EXPOSURE_NO_NEW_ENTRY')
        if actual!=expected.get(p['coin'],0):
            raise DispatchError('UNOWNED_ACCOUNT_POSITION_NO_NEW_ENTRY')
    if any(quantity!=0 and symbol not in seen for symbol,quantity in expected.items()):
        raise DispatchError('ACCOUNT_POSITION_DISAPPEARED_RECONCILE_FIRST')
    return True


def _maintain_bucket(controller, bucket):
    """Reconcile between bounded exit actions without waiting another sweep.

    cycle() refreshes public evidence and resolves the durable pending request
    before selecting each action. An uncertain or rejected reply ends this pass;
    it never authorizes a resend. New entries stay disabled throughout.
    """
    for _ in range(3):
        try:
            result = controller.cycle(bucket, send=True, allow_new_entries=False)
        except (DispatchError, life.LifecycleError):
            load = getattr(controller.store, 'load', None)
            before_manual = load(bucket) if callable(load) else None
            manual = _reconcile_manual_flat(controller, bucket)
            after_manual = manual.get('state') if isinstance(manual, dict) else None
            if (not isinstance(before_manual, dict) or not isinstance(after_manual, dict)
                    or type(before_manual.get('revision')) is not int
                    or type(after_manual.get('revision')) is not int
                    or after_manual['revision'] <= before_manual['revision']):
                # An already-bound close awaiting cleanup is not recovery from
                # an unrelated failure. Surface it to the maintenance health
                # report instead of concealing a quota, freshness or DB fault.
                raise
            result = manual
        manual = _reconcile_manual_flat(controller, bucket)
        yield result
        if manual is not None and manual['status']=='MANUAL_EXIT_BOUND_AWAITING_OWNED_CLEANUP':
            continue
        if (result.get('status') != 'ACCEPTED_UNVERIFIED'
                or result['order_requests_sent'] != 1):
            break


def _reconcile_manual_flat(controller, bucket):
    """Only a complete saved flat discrepancy triggers independent close proof.

    No extra public scan is scheduled for healthy active positions. The helper
    never sends: it binds one unambiguous reducing manual close and leaves only
    existing owned exit cancellation to the ordinary/emergency executor.
    """
    load=getattr(controller.store,'load',None)
    if not callable(load):
        return None
    state=load(bucket)
    if not isinstance(state,dict) or not state.get('bindings') or state.get('pending'):
        return None
    ev=state.get('evidence')
    if not ev or life.number(ev['snapshot']['position_quantity'],signed=True)!=0:
        return None
    own=life.validate_bindings(state['bindings'])
    unknown=any((state['account'],fill['oid']) not in own
                for fill in ev['snapshot']['fills'])
    unfinished=any(audit.get('automatic') is True and
        audit.get('phase')=='OWNED_ORDER_CLEANUP_PENDING'
        for audit in state.get('manual_exit_audit',[]))
    if not unknown and not unfinished:
        return None
    from .manual_exit_reconciliation import reconcile_controller
    result=reconcile_controller(controller,bucket)
    return None if result['status']=='NO_MANUAL_EXIT_CANDIDATE' else result


def _maintenance_priority(state, dirty_symbols=()):
    """Hints reorder work; only the ordinary committed observer proves fills."""
    dirty=dirty_symbols is None or state.get('symbol') in dirty_symbols
    if state.get('pending') is not None:
        return 0
    if state.get('evidence') is not None and state.get('bindings'):
        snap=state['evidence']['snapshot']
        entry_ids={oid for binding in state['bindings']
                   for oid in binding.get('orders',{}).get('ENTRY',[])}
        if any(o['oid'] in entry_ids for o in snap.get('open_orders',[])):
            return 0
        try:
            report=life.review(state['bindings'],snap,now_ms=snap['at_ms'])
            if report['bucket_issues'] or any(row['issues'] for row in report['cards']):
                return 0
        except (KeyError,life.LifecycleError):
            return 0
        if life.number(snap.get('position_quantity','0'),signed=True)!=0:
            return 0 if dirty else 1
    return 2 if dirty else 3


def _immutable_flat_checkpoint(state):
    """Historical terminal proof may be retained; inventory must verify flat now."""
    if 'history_gap_recovery' in state:
        # Final old orders remain final, but a staged history cursor has not
        # yet established the account's current reconciliation boundary.
        return False
    ev=state.get('evidence')
    emergency=state.get('emergency')
    if emergency is not None and (emergency['phase']!='CLOSED_VERIFIED'
            or emergency.get('pending_close') or emergency.get('pending_cancel')):
        return False
    return (ev is not None and ev.get('bindings')==state.get('bindings')
        and ev['snapshot']['account']==state['account']
        and ev['snapshot']['symbol']==state['symbol']
        and ev['snapshot']['history_complete'] and ev['snapshot']['orders_complete']
        and idle_flat(state))


def _recover_history(controller, state):
    """Per-market scheduling hints never change the durable recovery cursor.

    A failed history observation delays only this read-only market. Live exits
    in every other bucket still run first. Restart can discard these hints;
    the shared quota and audited cursor remain the authoritative fences.
    """
    retries=vars(controller).setdefault('_history_gap_retries', {})
    previous=retries.get(state['bucket'])
    now=controller.venue.now()
    revision=state.get('revision')
    if previous is not None and previous['revision']==revision and now<previous['due_ms']:
        return dict(status='HISTORY_GAP_RECOVERY_WAIT', order_requests_sent=0,
            state=state, cursor_ms=previous['cursor_ms'],
            remaining_ms=max(0, now-previous['cursor_ms']),
            retry_after_ms=previous['due_ms']-now,
            failure_code=previous['failure_code'])
    try:
        result=gap.step(controller, state)
    except Exception as exc:
        from .request_budget import BudgetError, fresh_entry_retry_ms
        count=min(6, previous['count']+1 if previous is not None
            and previous['revision']==revision else 1)
        delay=(fresh_entry_retry_ms(exc, consecutive_failures=count)
               if isinstance(exc, BudgetError) else min(30000, 1000*2**count))
        snapshot=(state.get('evidence') or {}).get('snapshot', {})
        cursor=(state.get('history_gap_recovery') or {}).get('cursor_ms',
            snapshot.get('at_ms', now))
        retries[state['bucket']]=dict(revision=revision, count=count,
            due_ms=now+(delay if delay is not None else 30000), cursor_ms=cursor,
            failure_code=_safe_failure(exc)['failure_code'])
        raise
    retries.pop(state['bucket'], None)
    return result


def _history_recovery_report(results):
    statuses={item['status'] for item in results}
    status=('HISTORY_GAP_RECOVERY_PROGRESS' if 'HISTORY_GAP_RECOVERY_PROGRESS' in statuses
        else 'HISTORY_GAP_RECOVERY_WAIT' if 'HISTORY_GAP_RECOVERY_WAIT' in statuses
        else 'HISTORY_GAP_RECOVERY_COMPLETE')
    latest=results[-1]
    report=dict(status=status, recovery_buckets=len(results),
        recovery_cursor_ms=latest['cursor_ms'], recovery_remaining_ms=latest['remaining_ms'])
    waits=[item for item in results if item['status']=='HISTORY_GAP_RECOVERY_WAIT']
    if waits:
        report.update(retry_after_ms=min(item['retry_after_ms'] for item in waits),
                      failure_code=waits[0]['failure_code'])
    return report


def _quiet_protected_checkpoint(controller, state, *, now_ms):
    """Share the safety supervisor's current quiet proof, never its read flight.

    Only a saved complete STOP/TAKE proof and a still reconciled live feed can
    suppress both supervisors' duplicate probing up to the existing fifteen-
    second checkpoint bound. Notifications and gaps require public observation
    again; no evidence timestamp or emergency close deadline is extended.
    """
    if not dispatch._fully_protected_no_work(state,now_ms):
        return False
    feed=vars(controller.venue).get('fill_wakeups')
    if feed is None:
        # Preserve the older bounded behavior for callers with an explicit
        # continuity assertion but no local notification transport.
        return 0<=now_ms-state['evidence']['snapshot']['at_ms']<10000
    healthy=getattr(type(feed),'entry_allowed',None)
    if not callable(healthy) or feed.entry_allowed(state['account']) is not True:
        return False
    from .emergency_close import recent_normal_checkpoint
    return (recent_normal_checkpoint(state,now_ms=now_ms,fill_wakeups=feed)
            and feed.entry_allowed(state['account']) is True)


def _current_reconciliation_checkpoint(state, started_ms, now_ms):
    """A successful member of this still-pending account pass need not poll twice.

    Another market may prevent finishing the notification gate. Retain only a
    complete, current STOP/TAKE checkpoint collected after this pass began, for
    less than the ordinary five-second safety bound. This is scheduling only:
    ENTRY remains closed, unknown work still reconciles, and the independent
    emergency supervisor keeps running. Neither clock is moved forward.
    """
    if (type(started_ms) is not int or state.get('emergency') is not None
            or not dispatch._fully_protected_no_work(state,now_ms)):
        return False
    at=state['evidence']['snapshot']['at_ms']
    return started_ms<=at<=now_ms and now_ms-at<5000


def tick(controller, route, not_before, *, new_entries, role='long_account',
         dirty_symbols=(), full_reconciliation=False, notification_continuity=False,
         reconciliation_started_ms=None):
    """One bounded sweep. Every old exposure is serviced before new cards."""
    now = datetime.fromtimestamp(controller.venue.now()/1000,timezone.utc)
    states = sorted(controller.store.for_account(route['account']),
                    key=lambda state:_maintenance_priority(state,dirty_symbols))
    sent = 0
    errors = 0
    maintenance_active = 0
    history_recovery = []
    first_failure = None
    rejection = None
    for state in states:
        try:
            retained=full_reconciliation and _immutable_flat_checkpoint(state)
            current_reconciliation=(not new_entries and full_reconciliation and
                _current_reconciliation_checkpoint(state,reconciliation_started_ms,
                                                   controller.venue.now()))
            forced=((full_reconciliation and not retained)
                    or state.get('symbol') in dirty_symbols)
            # A closed SHORT bucket can still be the predecessor of a new card.
            # Refresh its aged evidence periodically even while entries are off,
            # so a later entry cannot inherit an unreviewed history gap.
            if state.get('emergency') is not None:
                maintenance_active += int(state['emergency']['phase'] != 'CLOSED_VERIFIED')
                if forced and state['emergency']['phase']=='CLOSED_VERIFIED':
                    from .emergency_close import Controller as EmergencyController
                    EmergencyController(controller).cycle(state['bucket'],send=False)
                _reconcile_manual_flat(controller,state['bucket'])
                continue
            aged_closed_short = (not retained and role == 'short_account' and state['bindings']
                and state['evidence'] is not None
                and 3600000 < int(now.timestamp()*1000)-state['evidence']['snapshot']['at_ms'])
            recover_history = gap.needed(state, now_ms=controller.venue.now())
            unfinished = _unfinished(state,now)
            # A locally registered, never-attempted candidate requires no
            # maintenance while entries are disabled. Preserve aged SHORT
            # history catch-up and all pending/working/exposed buckets.
            if not forced and not new_entries and not aged_closed_short and idle_flat(state):
                unfinished=False
            if not unfinished and not aged_closed_short and not forced and not recover_history:
                continue
            maintenance_active += int(unfinished)
            if recover_history:
                # A missing-history page is read-only work. Its separately
                # committed cursor is never an entry or fill checkpoint. Keep
                # servicing all live exposure first, then finish this bounded
                # stage without ordinary dispatch or entry in this sweep.
                history_recovery.append(_recover_history(controller, state))
                continue
            if current_reconciliation:
                continue
            if (notification_continuity and not forced and state['pending'] is None
                    and _quiet_protected_checkpoint(controller,state,
                                                    now_ms=controller.venue.now())):
                # Both supervisors share the same saved quiet proof. Live
                # notifications still wake authoritative reconciliation at once.
                continue
            for result in _maintain_bucket(controller, state['bucket']):
                sent += result['order_requests_sent']
                if result.get('status')=='REJECTED' and result['order_requests_sent']==1:
                    rejection=dict(rejection_code=result.get('rejection_code'),
                                   rejection_reason=result.get('rejection_reason'),
                                   rejection_subject=result.get('rejection_subject'),symbol=state['symbol'])
        except Exception as exc:
            errors += 1
            if first_failure is None:
                first_failure = _safe_failure(exc)
                if isinstance(state.get('symbol'),str):
                    first_failure['failure_symbol']=state['symbol']
    if errors or not new_entries:
        result = dict(status='EXISTING_RECONCILIATION_REQUIRED' if errors else 'ENTRIES_DISABLED',
                      active_buckets=len(states),
                      maintenance_active=maintenance_active,
                      order_requests_sent=sent,new_cards_registered=0)
        if first_failure is not None:
            result.update(first_failure)
        if history_recovery:
            recovered=_history_recovery_report(history_recovery)
            if errors:
                recovered.pop('status'); recovered.pop('failure_code', None)
            result.update(recovered)
        return result
    if history_recovery:
        return dict(**_history_recovery_report(history_recovery),
            active_buckets=len(states), maintenance_active=maintenance_active,
            order_requests_sent=sent, new_cards_registered=0,
            )
    from .bounded_entry_trial import cap_reached
    if cap_reached(controller.store, getattr(controller.venue, 'env', {}), role, route['account']):
        return dict(status='BOUNDED_ENTRY_TRIAL_CAP_REACHED', active_buckets=len(states),
                    maintenance_active=maintenance_active, order_requests_sent=sent,
                    new_cards_registered=0)
    states = controller.store.for_account(route['account'])
    trial_id = (roles.short_entry_scope(getattr(controller.venue, 'env', {}))
                if role == 'short_account' else None)
    timing_id=getattr(controller.venue,'env',{}).get('HL_TESTNET_PROTECTION_TIMING_CARD_ID','')
    if timing_id:
        if not re.fullmatch(r'[0-9a-f]{64}',timing_id) or (trial_id and trial_id!=timing_id):
            raise DispatchError('EXACT_TIMING_TRIAL_APPROVAL_REQUIRED')
        trial_id=timing_id
    known_cards = {cid for state in states for cid in state['originals']}
    from .execution_occurrence import duplicate_attempt
    touched = [state['bucket'] for state in states if state['pending'] is None
               and any((trial_id is None or cid == trial_id)
                       and cid not in {b['card_id'] for b in state['bindings']}
                       and original.get('entry_rejected_no_retry') is not True
                       and original.get('entry_unsent_no_retry') is not True
                       and duplicate_attempt(state,cid) is None
                       and 'source_expires_at' in original['card']
                       and source_fresh(timestamp(original['card']['prepared']['source']['at']),
                           original['card']['source_expires_at'],now=now)
                       for cid,original in state['originals'].items())]
    cursor = None
    registered = 0
    fresh_cards = []
    for _ in range(100):
        candidates,cursor = selection.page(controller.store.journal,
            not_before=not_before.isoformat(),now=now,after=cursor)
        for cid,card_role in candidates:
            if role != card_role or cid in known_cards or (trial_id is not None and cid != trial_id):
                continue
            fresh_cards.append((cid, card_role))
            known_cards.add(cid)
        if cursor is None:
            break
    else:
        raise DispatchError('LONG_ALERT_SCAN_BUDGET_REQUIRES_REVIEW')
    # A database-only source scan avoids spending venue quota every two
    # seconds while both accounts are flat and no fresh candidate exists.
    # Current inventory still gates EVERY registration and entry pass.
    if fresh_cards or touched:
        _account_owned(controller.venue,route['account'],states,role=role)
    for cid,card_role in fresh_cards:
        card=CardStore(controller.store.journal).load(cid)
        if card['account_role'] != card_role:
            raise DispatchError('STREAM_SOURCE_ROLE_CHANGED')
        try:
            state=controller.register(cid)
        except DispatchError as exc:
            if str(exc) in ('ORIGINAL_SOURCE_EXPIRED',
                            'REGISTER_WHILE_REQUEST_UNRESOLVED'):
                continue
            raise
        if state['account']!=route['account']:
            raise DispatchError('STREAM_CARD_ACCOUNT_MISMATCH')
        registered += 1
        if state['bucket'] not in touched and duplicate_attempt(state,cid) is None:
            touched.append(state['bucket'])
    for bucket in touched:
        result=controller.cycle(bucket,send=True,allowed_entry_card_id=trial_id)
        sent += result['order_requests_sent']
        if result.get('status')=='REJECTED' and result['order_requests_sent']==1:
            rejection=dict(rejection_code=result.get('rejection_code'),
                           rejection_reason=result.get('rejection_reason'),
                           rejection_subject=result.get('rejection_subject'),symbol=result['symbol'])
        if result.get('status') == 'ACCEPTED_UNVERIFIED' and result['order_requests_sent'] == 1:
            for maintenance in _maintain_bucket(controller, bucket):
                sent += maintenance['order_requests_sent']
                if maintenance.get('status') == 'REJECTED' and maintenance['order_requests_sent'] == 1:
                    rejection=dict(rejection_code=maintenance.get('rejection_code'),
                                   rejection_reason=maintenance.get('rejection_reason'),
                                   rejection_subject=maintenance.get('rejection_subject'),symbol=maintenance['symbol'])
    summary=dict(status='SWEEP_COMPLETE',active_buckets=len(states),
                 maintenance_active=maintenance_active,
                 order_requests_sent=sent,new_cards_registered=registered)
    if rejection is not None:
        summary.update(rejection)
    return summary


def observed_trades(controller, route, *, role='long_account', historical=False):
    """Read owned fills and working exits for monitoring; never authorize an order."""
    result = []
    for state in controller.store.for_account(route['account']):
        if not state['bindings'] or state['evidence'] is None:
            continue
        snap = state['evidence']['snapshot']
        # Reports of stored evidence must be evaluated at the observation time.
        # The caller must still show its age; this does not refresh live state.
        from .emergency_close import view as emergency_view
        # Terminal, complete closure is an immutable historical fact. Its
        # observation time stays visible and never becomes current inventory
        # authority. Active or uncertain cards still require fresh evidence.
        terminal_history = _immutable_flat_checkpoint(state)
        view = emergency_view(state, snap['at_ms'] if historical or terminal_history
                              else controller.venue.now())
        bindings = {b['card_id']:b for b in state['bindings']}
        for row in view['cards']:
            if life.number(row['entry_quantity']) <= 0:
                continue
            original = state['originals'].get(row['card_id'])
            if original is None:
                raise DispatchError('OBSERVED_TRADE_SOURCE_MISSING')
            card = trade_cards.validate_card(original['card'])
            if (card['card_id'] != row['card_id'] or
                    card['record_kind'] != 'received_alert' or
                    card['account_role'] != role):
                raise DispatchError('OBSERVED_TRADE_SOURCE_MISMATCH')
            remaining = life.number(row['remaining_quantity'])
            binding = bindings[row['card_id']]
            entry_ids = set(binding['orders']['ENTRY'])
            exit_ids = set(binding['orders']['STOP']+binding['orders']['TAKE_PROFIT']+
                           binding['orders'].get('MANUAL_EXIT', []))
            entries = [f for f in snap['fills'] if f['oid'] in entry_ids]
            exits = [f for f in snap['fills'] if f['oid'] in exit_ids]
            def weighted_price(fills):
                quantity = sum((Decimal(f['quantity']) for f in fills),Decimal(0))
                return (life.text(sum((Decimal(f['quantity'])*Decimal(f['price'])
                         for f in fills),Decimal(0))/quantity) if quantity else None)
            active = [o['oid'] for o in snap['open_orders']
                      if o['oid'] in entry_ids | exit_ids]
            protected = (remaining > 0 and not view['bucket_issues'] and not row['issues'] and
                life.number(row['stop_quantity_observed']) >= remaining and
                life.number(row['take_profit_quantity_observed']) >= remaining)
            result.append(dict(card_id=row['card_id'],
                source_event_id=card['prepared']['source']['event_id'],
                account_role=role,symbol=state['symbol'],state=row['state'],
                planned_prices=deepcopy(binding['prices']),
                entry_quantity=row['entry_quantity'],exit_quantity=row['exit_quantity'],
                remaining_quantity=row['remaining_quantity'],
                actual_entry_price=weighted_price(entries),
                actual_exit_price=weighted_price(exits),
                first_entry_at_ms=min(f['at_ms'] for f in entries),
                last_exit_at_ms=max((f['at_ms'] for f in exits),default=None),
                active_order_ids=active,
                issues=sorted(set(view['bucket_issues']) | set(row['issues'])),
                protection_verified=protected,
                closure_verified=row['closure_verified'],
                closure_origin=row.get('closure_origin'),
                protection_timing=deepcopy(state.get('protection_timing',{}).get(row['card_id'])),
                emergency_status=(state.get('emergency') or {}).get('phase'),
                evidence_at_ms=snap['at_ms'],order_requests_sent=0))
    return result


def _finish_notification_reconciliation(controller, feed, token, symbols, started_ms):
    """Release ENTRY only after exact current durable proofs and fresh inventory."""
    states=controller.store.for_account(token.account)
    expected=[state for state in states if
        (symbols is None and not _immutable_flat_checkpoint(state))
        or (symbols is not None and state['symbol'] in symbols)]
    if symbols is not None and set(symbols)-{state['symbol'] for state in states}:
        # An unowned market hint must not be silently discarded as harmless.
        return False
    now=controller.venue.now()
    for state in expected:
        ev=state['evidence']
        if (ev is None or ev['bindings']!=state['bindings']
                or not ev['snapshot']['history_complete'] or not ev['snapshot']['orders_complete']
                or not started_ms<=ev['snapshot']['at_ms']<=now
                or not 0<=now-ev['snapshot']['at_ms']<=15000):
            return False
        if state['bindings']:
            from .emergency_close import view
            report=view(state,now)
            if report['bucket_issues'] or any(row['issues'] for row in report['cards']):
                return False
    role=next(role for role,route in controller.routes.items() if route['account']==token.account)
    _account_owned(controller.venue,token.account,states,role=role,priority='protection')
    return feed.finish_reconciliation(token,complete=True)


def _loop(controller, streams):
    reported = set()
    reconciliation_passes = {}
    entry_retry_at = {}
    entry_failures = {}
    while not _stop.is_set():
        feed=vars(controller.venue).get('fill_wakeups')
        if feed is not None:
            _wake.clear()
            pending=set(feed.pending_accounts())
            def account_priority(item):
                account=item[1]['account']
                hints=feed.dirty_symbols(account) if account in pending else ()
                states=controller.store.for_account(account)
                return (min((_maintenance_priority(state,hints) for state in states),default=3),
                        account not in pending)
            try:
                scheduled=sorted(streams,key=account_priority)
            except Exception:
                # Scheduling is optional. Each account's guarded tick retains
                # its own diagnostic and retry path after a transient store read.
                scheduled=streams
        else:
            scheduled=streams
        results = []
        for role,route,start,enabled_key in scheduled:
            before=getattr(controller.venue,'sent',0)
            try:
                token=None
                notification_args={}
                notification_ready=False
                if feed is not None and route['account'] in pending:
                    notification_health=feed.health()[role]
                    notification_ready=(notification_health['connected']
                        and notification_health['snapshot_received']
                        and notification_health['subscriptions_acknowledged']==2)
                if notification_ready:
                    candidate=feed.begin_reconciliation(route['account'])
                    started_ms=controller.venue.now()
                    previous=reconciliation_passes.get(route['account'])
                    if (previous is not None
                            and (previous[0].generation,previous[0].revision)==
                                (candidate.generation,candidate.revision)
                            and 0<=started_ms-previous[1]<15000):
                        token,started_ms=previous
                    else:
                        token=candidate
                        reconciliation_passes[route['account']]=(token,started_ms)
                    symbols=feed.dirty_symbols(route['account'])
                    if symbols==():
                        symbols=None
                    notification_args=dict(dirty_symbols=() if symbols is None else symbols,
                                           full_reconciliation=symbols is None,
                                           reconciliation_started_ms=started_ms)
                elif feed is not None:
                    reconciliation_passes.pop(route['account'],None)
                result=tick(controller,route,start,
                            new_entries=controller.venue.env[enabled_key]=='true'
                                and (role not in entry_retry_at or
                                     controller.venue.now() >= entry_retry_at[role])
                                and (feed is None or feed.entry_allowed(route['account'])),
                            role=role,notification_continuity=feed is not None
                                and feed.entry_allowed(route['account']),**notification_args)
                if token is not None:
                    if _finish_notification_reconciliation(controller,feed,token,symbols,started_ms):
                        reconciliation_passes.pop(route['account'],None)
            except Exception as exc:
                failure = _safe_failure(exc)
                result=dict(status='RECONCILIATION_REQUIRED_NO_BLIND_RETRY',
                    order_requests_sent=max(0,getattr(controller.venue,'sent',0)-before),
                    new_cards_registered=0, **failure)
            # Quota waits suppress only new-entry work. The next pass still
            # reconciles pending requests and maintains every existing exit.
            # A later acquisition remains mandatory; no ambiguous submission
            # is retried and no original source clock is extended.
            quota_codes = {'TESTNET_REQUEST_BUDGET_EXHAUSTED',
                           'TESTNET_REQUEST_BUDGET_BUSY',
                           'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED'}
            if result.get('failure_code') in quota_codes:
                from .request_budget import BudgetError, fresh_entry_retry_ms
                entry_failures[role] = min(6, entry_failures.get(role, 0) + 1)
                error = BudgetError(result['failure_code'],
                    requested_weight=result.get('budget_requested_weight'),
                    ceiling=result.get('budget_ceiling'),
                    retry_after_ms=result.get('budget_retry_after_ms'))
                delay_ms = fresh_entry_retry_ms(error,
                    consecutive_failures=entry_failures[role])
                if delay_ms is not None:
                    entry_retry_at[role] = controller.venue.now() + delay_ms
                    result['fresh_entry_retry_after_ms'] = delay_ms
            elif role in entry_retry_at and controller.venue.now() >= entry_retry_at[role]:
                entry_failures.pop(role, None)
                entry_retry_at.pop(role, None)
            results.append(result)
            with _lock:
                _health['cycles'] += 1
                _health['last_status']=result['status']
                _health['order_requests_sent']=getattr(controller.venue,'sent',0)
            print(json.dumps({('testnet_long_stream' if role=='long_account' else 'testnet_short_stream'):result},sort_keys=True),flush=True)
            if result.get('active_buckets',0) or result.get('new_cards_registered',0):
                try:
                    for trade in observed_trades(controller,route,role=role):
                        marker = (trade['card_id'],trade['state'],
                                  trade['protection_verified'],trade['closure_verified'],
                                  tuple(trade['issues']))
                        if marker not in reported:
                            label='testnet_long_trade_observed' if role=='long_account' else 'testnet_short_trade_observed'
                            print(json.dumps({label:trade},sort_keys=True),flush=True)
                            reported.add(marker)
                except Exception:
                    label='testnet_long_trade_observation' if role=='long_account' else 'testnet_short_trade_observation'
                    print(json.dumps({label:'UNAVAILABLE_RETRY'}),flush=True)
        delay=2 if (all(r['status']=='SWEEP_COMPLETE' for r in results)
                    or any(r.get('maintenance_active',0) for r in results)) else 10
        (_wake if feed is not None else _stop).wait(delay)
    with _lock:
        _health['running']=False


def _app_loop(controller, route, private_key):
    from .app_card_delivery import Publisher
    publisher = Publisher()
    while not _stop.is_set():
        try:
            count = publisher.pass_once(controller, route, private_key)
            status = publisher.last_status
        except Exception:
            count = 0
            status = 'DELIVERY_UNAVAILABLE_RETRY'
        with _lock:
            _health['app_delivery_status'] = status
        print(json.dumps({'testnet_app_delivery': {'status':status,
            'cards_sent':count,'order_requests_sent':0}},sort_keys=True),flush=True)
        _stop.wait(10 if status != 'DELIVERY_UNAVAILABLE_RETRY' else 30)


def _short_account_readiness(route):
    """One public, read-only snapshot of the approved second account."""
    report = dict(status='READ_ONLY_REVIEW_UNAVAILABLE', order_requests_sent=0,
                  account_settings_changes=0, transfers_sent=0)
    try:
        from .checks import InfoReader, Blocked
        reader = InfoReader()
        mode = reader.read('userAbstraction', user=route['account'])
        if mode == 'default':
            result = roles.default_native_snapshot(
                route, reader, 'BTC', allow_owned_exposure=True)
            report.update(status=result['status'], account_mode=result['account_mode'],
                          balance_usd=result['balance_usd'],
                          exchange_reported_available_usd=result['exchange_reported_available_usd'],
                          account_mapping_verified=result['account_mapping_verified'])
        elif mode in ('disabled', 'unifiedAccount'):
            result = roles.checks.run_check({
                'HL_TESTNET_RUNTIME_MODE': 'read_only',
                'HL_TESTNET_ACCOUNT_ADDRESS': route['account'],
                'HL_TESTNET_AGENT_ADDRESS': route['agent'],
                'HL_TESTNET_CHECK_SYMBOL': 'BTC',
            }, client=reader)
            report.update(status=result['status'], account_mode=mode,
                          account_mapping_verified=result['account_mapping_verified'],
                          positive_usdc_observed=result['positive_usdc_observed'],
                          unheld_balance_observed=result['unheld_balance_observed'],
                          exchange_capacity_observed=result['exchange_capacity_observed'])
        else:
            raise Blocked('ACCOUNT_MODE_REQUIRES_REVIEW')
    except Blocked as exc:
        code = str(exc)
        if re.fullmatch(r'[A-Z][A-Z0-9_]{2,99}', code):
            report['status'] = code
    except Exception:
        pass
    print(json.dumps({'testnet_short_account_readiness': report}, sort_keys=True), flush=True)


def _short_card_readiness(controller, route):
    """Inspect recent recorded SHORT plans with current public data, without reserving work."""
    report = dict(status='READ_ONLY_REVIEW_UNAVAILABLE', cards=[], buckets=[],
                  order_requests_sent=0, account_settings_changes=0, transfers_sent=0)
    try:
        states = controller.store.for_account(route['account'])
        report['buckets'] = [dict(symbol=s['symbol'], registered_cards=len(s['originals']),
                                  bound_cards=len(s['bindings']), pending_request=bool(s['pending']),
                                  evidence_at_ms=(s['evidence']['snapshot']['at_ms']
                                                  if s.get('evidence') is not None else None),
                                  evidence_fill_count=(len(s['evidence']['snapshot']['fills'])
                                                       if s.get('evidence') is not None else None))
                             for s in states[:8]]
        with controller.store.journal._transaction() as conn:
            CardStore(controller.store.journal).ready(conn)
            rows = conn.execute('''SELECT card_id FROM hl_testnet_cards_v1.cards
                WHERE created_at >= now() - interval '12 hours'
                  AND manifest->>'account_role'='short_account'
                  AND manifest->>'record_kind'='received_alert'
                ORDER BY created_at DESC LIMIT 3''').fetchall()
        for (card_id,) in rows:
            card = CardStore(controller.store.journal).load(card_id)
            source = card['prepared']['execution']
            plan = {key:source[key] for key in ('symbol','side','entry','stop','take_profit')}
            item = dict(card_id=card_id, symbol=plan['symbol'],
                        source_at=card['prepared']['source']['at'],
                        source_expires_at=card.get('source_expires_at'),
                        current_budget_status='READ_ONLY_REVIEW_UNAVAILABLE')
            try:
                outcome = roles.budget_for_role(controller.venue.env,'short_account',
                    route['account'],route['agent'],plan,roles.checks.InfoReader())
                item['current_budget_status'] = outcome['status']
                diagnostic = outcome.get('budget_diagnostics') or {}
                item['current_budget_failed_checks'] = diagnostic.get('failed_checks',[])
            except roles.checks.Blocked as exc:
                code = str(exc)
                if re.fullmatch(r'[A-Z][A-Z0-9_]{2,99}', code):
                    item['current_budget_status'] = code
            except Exception:
                pass
            report['cards'].append(item)
        report['status'] = 'CURRENT_ACCOUNT_AND_STORED_CARDS_OBSERVED'
    except Exception:
        pass
    print(json.dumps({'testnet_short_card_readiness': report}, sort_keys=True), flush=True)


def _short_pending_readiness(controller, route):
    """Report bounded public evidence for uncertain SHORT requests; never reconcile or send."""
    report = dict(status='READ_ONLY_REVIEW_UNAVAILABLE', pending=[],
                  order_requests_sent=0, account_settings_changes=0, transfers_sent=0)
    try:
        states = controller.store.for_account(route['account'])
        pending = [state for state in states if state['pending'] is not None]
        if len(pending) > 8:
            report['status'] = 'TOO_MANY_PENDING_REQUESTS'
        else:
            from .card_sync_evidence import PublicReader
            reader = PublicReader()
            for state in pending:
                request = controller.store.request(state['pending'])
                item = dict(symbol=state['symbol'], phase=request['phase'],
                            reply_state=(request.get('reply') or {}).get('state'))
                proposal = request.get('proposal') or {}
                leg = proposal.get('leg')
                item['leg'] = leg if leg in life.LEGS else 'UNRECOGNIZED'
                reply_code = (request.get('reply') or {}).get('code')
                item['reply_code'] = (reply_code if reply_code in
                    set(dispatch.recovery.ERRORS.values()) | {'OTHER_REJECTION', None}
                    else 'UNRECOGNIZED')
                reply = request.get('reply') or {}
                reason = reply.get('venue_reason')
                if (reply.get('state') == 'REJECTED' and isinstance(reason,str)
                        and 0 < len(reason) <= 240
                        and re.fullmatch(r'[\x20-\x7e]+',reason)
                        and re.search(r'0x[0-9a-fA-F]{8,}',reason) is None):
                    item['rejection_reason'] = reason
                subject = reply.get('rejection_subject')
                if subject in {'AGENT','ACCOUNT','UNEXPECTED_SIGNER'}:
                    item['rejection_subject'] = subject
                if (request.get('attempt_at_ms') is not None
                        and request['proposal']['action']['type'] == 'order'):
                    cloid = proposal['action']['orders'][0]['c']
                    raw = controller.venue.lookup(route['account'], cloid)
                    status = raw.get('status') if isinstance(raw, dict) else None
                    item['lookup_status'] = (status if isinstance(status, str)
                        and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', status)
                        else 'UNRECOGNIZED')
                    if status == 'order':
                        order_status = (raw.get('order') or {}).get('status')
                        item['order_status'] = (order_status if isinstance(order_status, str)
                            and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', order_status)
                            else 'UNRECOGNIZED')
                    orders = reader.read('frontendOpenOrders', route['account'])
                    positions = reader.read('clearinghouseState', route['account'])
                    if (not isinstance(orders, list) or not isinstance(positions, dict)
                            or not isinstance(positions.get('assetPositions'), list)
                            or any(not isinstance(o, dict) or not isinstance(o.get('coin'), str)
                                   for o in orders)
                            or any(not isinstance(p, dict) or not isinstance(p.get('position'), dict)
                                   or not isinstance(p['position'].get('coin'), str)
                                   for p in positions['assetPositions'])):
                        raise DispatchError('PUBLIC_ACCOUNT_STATE_INVALID')
                    item['symbol_open_orders_present'] = any(
                        o['coin'] == state['symbol'] for o in orders)
                    quantities = [(p['position']['coin'],life.number(
                        p['position'].get('szi'),signed=True))
                        for p in positions['assetPositions']]
                    item['symbol_position_present'] = any(
                        symbol == state['symbol'] and quantity != 0
                        for symbol,quantity in quantities)
                    end = controller.venue.now()
                    start = request['attempt_at_ms']
                    item['fill_window_complete'] = (0 <= end-start <= 86400000)
                    if item['fill_window_complete']:
                        from .card_sync_evidence import history
                        fills = history(reader,route['account'],start,end)
                        if any(not isinstance(fill,dict) or not isinstance(fill.get('coin'),str)
                               for fill in fills):
                            raise DispatchError('PUBLIC_FILL_HISTORY_INVALID')
                        item['symbol_fills_since_attempt'] = any(
                            fill['coin'] == state['symbol'] for fill in fills)
                report['pending'].append(item)
            report['status'] = 'PENDING_PUBLIC_EVIDENCE_OBSERVED'
    except Exception:
        pass
    print(json.dumps({'testnet_short_pending_readiness': report}, sort_keys=True), flush=True)


def start():
    global _thread,_app_thread,_fill_wakeups
    env=dict(os.environ)
    route,not_before=configuration(env)
    short=short_configuration(env)
    app_mode=env.get('HL_TESTNET_APP_DELIVERY','')
    if app_mode not in ('','ed25519_signed_v1'):
        raise DispatchError('APP_DELIVERY_MODE_INVALID')
    private_key=env.get('HL_TESTNET_APP_SIGNING_KEY','') if app_mode else ''
    if app_mode and not private_key:
        raise DispatchError('APP_DELIVERY_SIGNING_KEY_REQUIRED')
    controller=dispatch.controller_from_env(env)
    with controller.store.journal._transaction() as conn:
        controller.store.ready(conn)
        CardStore(controller.store.journal).ready(conn)
    from .alert_cards_intake import initialize as initialize_intake
    initialize_intake(controller.store.journal)
    controller.store.for_account(route['account'])
    if short:
        controller.store.for_account(short[0]['account'])
    from .request_budget import Budget
    Budget(controller.store.journal).initialize()
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop.clear()
        _wake.clear()
        _health.update(configured=True,running=True,last_status='STARTING',cycles=0,
                       order_requests_sent=0,
                       new_entries_enabled=env['HL_TESTNET_LONG_ENTRY_ENABLED']=='true',
                       short_entries_enabled=bool(short and env['HL_TESTNET_SHORT_ENTRY_ENABLED']=='true'),
                       short_stream_configured=bool(short),
                       app_delivery_status='STARTING' if app_mode else 'DISABLED')
        streams=[('long_account',route,not_before,'HL_TESTNET_LONG_ENTRY_ENABLED')]
        if short:
            streams.append(('short_account',short[0],short[1],'HL_TESTNET_SHORT_ENTRY_ENABLED'))
        from .fill_wakeups import FillWakeups
        _fill_wakeups=FillWakeups({role:route['account'] for role,route,*_ in streams},wake_event=_wake)
        controller.venue.fill_wakeups=_fill_wakeups
        _fill_wakeups.start()
        from .emergency_close import start as start_emergency
        start_emergency(controller,streams,_stop)
        _thread=threading.Thread(target=_loop,args=(controller,streams),
                                 daemon=True,name='testnet-card-stream')
        _thread.start()
        if short:
            threading.Thread(target=_short_account_readiness,args=(short[0],),
                             daemon=True,name='testnet-short-account-readiness').start()
            threading.Thread(target=_short_card_readiness,args=(controller,short[0]),
                             daemon=True,name='testnet-short-card-readiness').start()
            threading.Thread(target=_short_pending_readiness,args=(controller,short[0]),
                             daemon=True,name='testnet-short-pending-readiness').start()
        if app_mode:
            _app_thread=threading.Thread(target=_app_loop,args=(controller,route,private_key),
                daemon=True,name='testnet-app-card-delivery')
            _app_thread.start()
    return True


def stop():
    _stop.set()
    _wake.set()
    if _fill_wakeups is not None:
        _fill_wakeups.stop()


def health():
    with _lock:
        result=deepcopy(_health)
    from .emergency_close import health as emergency_health
    result['emergency_close']=emergency_health()
    if _fill_wakeups is not None:
        result['fill_notifications']=_fill_wakeups.health()
    return result
