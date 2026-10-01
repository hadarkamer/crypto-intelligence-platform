"""One explicitly invoked, fresh received-alert Testnet timing experiment.

Never called from HTTP, startup, intake, monitors or a schedule. The service's
continuous entry flags remain false; an expiring in-process grant names exactly
one card. No invented alert, source refresh, price chasing or risk-profile change.
A still-working limit entry retains normal source-expiry/half-threshold handling.
"""
from datetime import datetime, timezone
import json
import os
import re
import threading
import time

from . import emergency_close as emergency, filled_quantity_dispatch as dispatch
from . import card_lifecycle as life, long_stream_runtime as stream
from .source_window import source_fresh,timestamp
from .trade_card_store import CardStore
from .filled_dispatch_store import DispatchError

MAX_OBSERVATION_MS = 600000
FINALITY_GRACE_MS = 15000
NOTIFICATION_BOOTSTRAP_SECONDS = 10


def outcome(state, card_id, now_ms):
    """Describe durable evidence; an entry receipt is never a fill or finality."""
    pending=state.get('pending')
    evidence=state.get('evidence')
    result=dict(lifecycle='UNBOUND',entry_quantity='0',remaining_quantity='0',
        working_entry_order_ids=[],working_exit_order_ids=[],pending_request_id=pending,
        evidence_at_ms=None,issues=[],protection_verified=False,closure_verified=False,
        terminal_verified=False)
    if not evidence:
        result['issues']=['BOT_EXECUTION_EVIDENCE_REQUIRED']
        return result
    snap=evidence['snapshot'];result['evidence_at_ms']=snap['at_ms']
    life.validate_snapshot(snap)
    if (evidence['bindings']!=state['bindings'] or not snap['history_complete']
            or not snap['orders_complete'] or not 0<=now_ms-snap['at_ms']<=15000):
        result['issues']=['OBSERVATION_INCOMPLETE_OR_STALE']
        return result
    binding=next((b for b in state['bindings'] if b['card_id']==card_id),None)
    if binding is None:
        if state['originals'][card_id].get('entry_unsent_no_retry') is True and pending is None:
            result.update(lifecycle='ABORTED_UNSENT',terminal_verified=True)
        elif state['originals'][card_id].get('entry_rejected_no_retry') and pending is None:
            result.update(lifecycle='REJECTED_WITHOUT_FILL',terminal_verified=True)
        return result
    review=emergency.view(state,now_ms)
    view=next(v for v in review['cards'] if v['card_id']==card_id)
    entry=set(binding['orders']['ENTRY'])
    exits=set(binding['orders']['STOP']+binding['orders']['TAKE_PROFIT'])
    result.update(lifecycle=view['state'],entry_quantity=view['entry_quantity'],
        remaining_quantity=view['remaining_quantity'],
        working_entry_order_ids=[o['oid'] for o in snap['open_orders'] if o['oid'] in entry],
        working_exit_order_ids=[o['oid'] for o in snap['open_orders'] if o['oid'] in exits],
        issues=sorted(set(review['bucket_issues']+view['issues'])),
        closure_verified=view['closure_verified'])
    clean=not result['issues'] and pending is None
    result['terminal_verified']=clean and view['state'] in ('CLOSED','CANCELED_WITHOUT_FILL')
    remaining=life.number(view['remaining_quantity'],signed=True)
    result['protection_verified']=(clean and remaining>0
        and life.number(view['entry_quantity'])>0
        and life.number(view['stop_quantity_observed'])==remaining
        and life.number(view['take_profit_quantity_observed'])==remaining)
    return result


def measured_status(state, card_id, observation):
    if observation['terminal_verified']:
        return observation['lifecycle']
    timing=state.get('protection_timing',{}).get(card_id) or {}
    if observation['protection_verified']:
        if (type(timing.get('fill_to_stop_public_ms')) is int
                and timing['fill_to_stop_public_ms']>=0):
            return 'PROTECTED_TIMING_COMPLETE'
        return 'PROTECTED_VENUE_TIME_UNKNOWN'
    return None


def entry_attempts(store, state, card_id):
    # begin() arms timing in the same transaction as the sole ENTRY attempt.
    attempts=int(card_id in state.get('entry_timing_armed',{}))
    if state.get('pending'):
        request=store.request(state['pending'])
        if request['proposal']['card_id']==card_id and request['proposal']['operation']=='ENTRY':
            attempts=max(attempts,request['attempts'])
    return attempts


def durable_timing(state, card_id, read_at_ms):
    """A successful store.load proves commit preceded this read, not its exact time."""
    item=state.get('protection_timing',{}).get(card_id)
    if item is None:
        return None
    item=dict(item)
    first=item.get('first_fill_at_ms')
    stop=item.get('stop_observed_at_ms',item.get('stop_verified_at_ms'))
    if type(first) is int and type(stop) is int and first<=stop<=read_at_ms:
        item.update(stop_record_read_at_ms=read_at_ms,
            fill_to_durable_record_read_upper_bound_ms=read_at_ms-first)
    return item


def validate(env, card, now_ms):
    if (env.get('HL_TESTNET_EMERGENCY_CLOSE')!=emergency.APPROVAL
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED')!='false'
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED')!='false'
            or env.get('RENDER_SERVICE_ID')!=dispatch.roles.SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE')!='long_stream_testnet_v1'):
        raise DispatchError('TIMING_TRIAL_REQUIRES_DEPLOYED_EMERGENCY_AND_ENTRIES_DISABLED')
    if (card['record_kind']!='received_alert' or card['prepared']['execution'] is None
            or not source_fresh(timestamp(card['prepared']['source']['at']),card.get('source_expires_at'),
                now=datetime.fromtimestamp(now_ms/1000,timezone.utc))):
        raise DispatchError('TIMING_TRIAL_REQUIRES_FRESH_DELIVERED_ALERT')
    return min(now_ms+90000,int(timestamp(card['source_expires_at']).timestamp()*1000))


def run_one(env, card_id):
    """Caller must explicitly select a genuine eligible card; at most one entry."""
    if not isinstance(card_id,str) or not re.fullmatch(r'[0-9a-f]{64}',card_id):
        raise DispatchError('EXACT_TIMING_CARD_ID_REQUIRED')
    environment=dict(env)
    base=dispatch.controller_from_env(environment)
    card=CardStore(base.store.journal).load(card_id)
    now=base.venue.now()
    expiry=validate(environment,card,now)
    feed=None
    try:
        if isinstance(base.venue,dispatch.TestnetVenue):
            from .fill_wakeups import FillWakeups
            role=card['account_role'];route=base.routes[role]
            feed=FillWakeups({role:route['account']})
            base.venue.fill_wakeups=feed
            feed.start()
        return _run_one(environment,card_id,base,card,now,expiry,feed)
    finally:
        if feed is not None:
            feed.stop()


def _reconcile_notifications(base, feed, role, route, card):
    """An active socket never replaces authoritative saved account observation."""
    if feed.entry_allowed(route['account']):
        return True
    health=feed.health()[role]
    if (not health['connected'] or not health['snapshot_received']
            or health['subscriptions_acknowledged']!=2):
        return False
    token=feed.begin_reconciliation(route['account'])
    symbols=feed.dirty_symbols(route['account'])
    if symbols==():
        symbols=None
    started_ms=base.venue.now()
    result=stream.tick(base,route,timestamp(card['prepared']['source']['at']),
        new_entries=False,role=role,dirty_symbols=() if symbols is None else symbols,
        full_reconciliation=symbols is None)
    if result['status'] not in ('ENTRIES_DISABLED','SWEEP_COMPLETE'):
        return False
    return stream._finish_notification_reconciliation(base,feed,token,symbols,started_ms)


def _wait_notifications(base, feed, role, route, card, expiry, stopped):
    deadline=time.monotonic()+NOTIFICATION_BOOTSTRAP_SECONDS
    while base.venue.now()<expiry and time.monotonic()<deadline:
        if _reconcile_notifications(base,feed,role,route,card):
            return
        stopped.wait(.1)
    raise DispatchError('TIMING_TRIAL_FILL_NOTIFICATION_GAP_NO_NEW_ENTRY')


def _trial_pause(state, observation, *, feed, account, now_ms, expiry_ms,
                 source_expiry_ms, observation_deadline_ms, wall_remaining,
                 cycle_result=None):
    """Wait only for a proven quiet entry; new hints interrupt every wait."""
    if feed is None:
        return .25  # Preserve socket-free software fixtures and their clocks.
    sent=(cycle_result or {}).get('order_requests_sent',0)
    delay=0 if type(sent) is int and sent>0 else .25
    if delay:
        try:
            quiet=(feed.entry_allowed(account) and state.get('pending') is None
                and not state.get('emergency') and not observation['issues']
                and observation['lifecycle']=='WAITING_ENTRY'
                and observation['working_entry_order_ids']
                and not observation['working_exit_order_ids']
                and life.number(observation['entry_quantity'])==0
                and life.number(observation['remaining_quantity'])==0
                and life.number(state['evidence']['snapshot']['position_quantity'],signed=True)==0
                and type(observation['evidence_at_ms']) is int
                and 0<=now_ms-observation['evidence_at_ms']<=5000)
        except (KeyError,TypeError,life.LifecycleError):
            quiet=False
        if quiet:
            delay=5
    # Expired grants never become a renewed deadline or terminate management.
    # Reach each still-future source/grant boundary before another quiet pause.
    bounds=[max(0,wall_remaining),max(0,observation_deadline_ms-now_ms)/1000]
    bounds.extend((bound-now_ms)/1000 for bound in (expiry_ms,source_expiry_ms) if bound>now_ms)
    return min(delay,*bounds)


def _run_one(environment, card_id, base, card, now, expiry, feed):
    """Run with the original grant timestamp, never renewed during bootstrap."""
    source_expiry=int(timestamp(card['source_expires_at']).timestamp()*1000)
    observation_deadline=min(source_expiry+FINALITY_GRACE_MS,now+MAX_OBSERVATION_MS)
    wall_deadline=time.monotonic()+max(0,observation_deadline-now)/1000
    role=card['account_role'];route=base.routes[role]
    states=base.store.for_account(route['account'])
    stream._account_owned(base.venue,route['account'],states,role=role)
    if any(card_id in {b['card_id'] for b in state['bindings']} for state in states):
        raise DispatchError('TIMING_TRIAL_CARD_ALREADY_ATTEMPTED')
    # Snapshot config copies only: never update os.environ or Render settings.
    trial_env={**environment,'HL_TESTNET_PROTECTION_TIMING_CARD_ID':card_id,
               'HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS':str(expiry)}
    key='HL_TESTNET_LONG_ENTRY_ENABLED' if role=='long_account' else 'HL_TESTNET_SHORT_ENTRY_ENABLED'
    trial_env[key]='true'
    if role=='short_account':
        trial_env['HL_TESTNET_SHORT_TRIAL_CARD_ID']=card_id
    controlled=dispatch.controller_from_env(trial_env)
    if feed is not None:
        controlled.venue.fill_wakeups=feed
    state=controlled.register(card_id)
    # A free symbol is required; historical cards must have public finality.
    state=base.refresh(state['bucket'])
    if state['bindings']:
        report=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=base.venue.now())
        if report['bucket_issues'] or any(v['issues'] or v['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL') for v in report['cards']):
            raise DispatchError('TIMING_TRIAL_MARKET_PREDECESSOR_NOT_FINAL')
    stopped=threading.Event()
    streams=[(role,route,None,key)]
    started=emergency.start(base,streams,stopped,only_bucket=state['bucket'])
    if not started:
        raise DispatchError('TIMING_TRIAL_REQUIRES_OWN_SUPERVISOR')
    start=time.monotonic();status='SUPERVISOR_STARTING'
    cycle_status_counts={}
    result=None
    try:
        while time.monotonic()-start<10 and not emergency.healthy(base.venue.now()):
            stopped.wait(.25)
        if not emergency.healthy(base.venue.now()):
            raise DispatchError('EMERGENCY_SUPERVISOR_NOT_FRESH_NO_NEW_ENTRY')
        if feed is not None:
            _wait_notifications(base,feed,role,route,card,expiry,stopped)
        while base.venue.now()<observation_deadline and time.monotonic()<wall_deadline:
            if feed is not None:
                # Clear before reads, so hints arriving during reconciliation
                # remain set and cannot be lost before the following wait.
                feed.wake_event.clear()
            cycle_result=None
            state=base.store.load(state['bucket'])
            incident=state.get('emergency')
            observation=outcome(state,card_id,base.venue.now())
            complete=measured_status(state,card_id,observation)
            if complete:
                status=complete
                break
            if incident:
                status='EMERGENCY_'+incident['phase']
                if incident['phase']=='CLOSED_VERIFIED' and observation['terminal_verified']:
                    break
            else:
                try:
                    may_enter=(base.venue.now()<expiry
                               and entry_attempts(base.store,state,card_id)==0)
                    notification_ready=(feed is None or
                        _reconcile_notifications(base,feed,role,route,card))
                    if may_enter and notification_ready:
                        result=controlled.cycle(state['bucket'],send=True,allowed_entry_card_id=card_id)
                    else:
                        result=controlled.cycle(state['bucket'],send=True,allow_new_entries=False)
                    cycle_result=result
                    status=result['status']
                except (DispatchError,life.LifecycleError) as exc:
                    status=str(exc)
                cycle_status_counts[status]=cycle_status_counts.get(status,0)+1
                state=base.store.load(state['bucket'])
                observation=outcome(state,card_id,base.venue.now())
                complete=measured_status(state,card_id,observation)
                if complete:
                    status=complete
                    break
                if base.venue.now()>=expiry and entry_attempts(base.store,state,card_id)==0 and not state.get('pending'):
                    status='ENTRY_NOT_SUBMITTED'
                    break
            pause=_trial_pause(state,observation,feed=feed,account=route['account'],
                now_ms=base.venue.now(),expiry_ms=expiry,source_expiry_ms=source_expiry,
                observation_deadline_ms=observation_deadline,
                wall_remaining=wall_deadline-time.monotonic(),cycle_result=cycle_result)
            (feed.wake_event if feed is not None else stopped).wait(pause)
        state=base.store.load(state['bucket'])
        observation=outcome(state,card_id,base.venue.now())
        if state.get('pending'):
            pending=base.store.request(state['pending'])
            observation.update(pending_request_phase=pending['phase'],
                               pending_request_attempts=pending['attempts'])
        else:
            observation.update(pending_request_phase=None,pending_request_attempts=0)
        complete=measured_status(state,card_id,observation)
        last_cycle_status=status
        status=complete or ('ENTRY_NOT_SUBMITTED' if status=='ENTRY_NOT_SUBMITTED'
            and not state.get('pending') and not entry_attempts(base.store,state,card_id)
            else 'RECONCILIATION_REQUIRED')
        result=dict(status=status,last_cycle_status=last_cycle_status,
            card_id=card_id,account_role=role,symbol=state['symbol'],
            source_event_id=card['prepared']['source']['event_id'],
            timing=durable_timing(state,card_id,base.venue.now()),
            observation=observation,entry_authorization_expires_at_ms=expiry,
            observation_deadline_ms=observation_deadline,
            ongoing_management_required=not observation['terminal_verified'] and
                bool(state.get('pending') or any(b['card_id']==card_id for b in state['bindings'])),
            deployed_worker_handoff_verified=False,
            emergency_status=(state.get('emergency') or {}).get('phase'),
            continuous_entry_flags_changed=False,entry_attempts=entry_attempts(base.store,state,card_id),
            cycle_status_counts=cycle_status_counts,
            order_requests_sent=controlled.venue.sent,
            mainnet_enabled=False)
    finally:
        shutdown=emergency.stop_supervisor(stopped,timeout=2)
        if result is not None and 'observation' in result:
            result['trial_supervisor_stopped']=shutdown
            if not shutdown:
                result['status']='RECONCILIATION_REQUIRED'
    return result


if __name__=='__main__':
    import sys
    if len(sys.argv)!=2:
        raise SystemExit('EXACT_CARD_ID_ARGUMENT_REQUIRED')
    print(json.dumps({'testnet_new_fill_protection_timing':run_one(dict(os.environ),sys.argv[1])},sort_keys=True))
