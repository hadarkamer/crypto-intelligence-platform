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
    expiry=validate(environment,card,base.venue.now())
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
    start=time.monotonic();status='SUPERVISOR_STARTING';entry_invoked=False
    cycle_status_counts={}
    try:
        while time.monotonic()-start<10 and not emergency.healthy(base.venue.now()):
            stopped.wait(.25)
        if not emergency.healthy(base.venue.now()):
            raise DispatchError('EMERGENCY_SUPERVISOR_NOT_FRESH_NO_NEW_ENTRY')
        while base.venue.now()<expiry:
            state=base.store.load(state['bucket'])
            incident=state.get('emergency')
            timing=state.get('protection_timing',{}).get(card_id)
            if incident:
                status='EMERGENCY_'+incident['phase']
                if incident['phase']=='CLOSED_VERIFIED':
                    break
            else:
                try:
                    if not entry_invoked:
                        result=controlled.cycle(state['bucket'],send=True,allowed_entry_card_id=card_id)
                    else:
                        result=controlled.cycle(state['bucket'],send=True,allow_new_entries=False)
                    status=result['status']
                except (DispatchError,life.LifecycleError) as exc:
                    status=str(exc)
                cycle_status_counts[status]=cycle_status_counts.get(status,0)+1
                state=base.store.load(state['bucket'])
                if any(b['card_id']==card_id for b in state['bindings']):
                    entry_invoked=True
                if state['pending']:
                    pending=base.store.request(state['pending'])
                    entry_invoked=entry_invoked or (pending['proposal']['card_id']==card_id
                        and pending['proposal']['operation']=='ENTRY' and pending['attempts']==1)
                if state['originals'][card_id].get('entry_rejected_no_retry'):
                    entry_invoked=True
                if card_id in state.get('protection_timing',{}) and 'stop_verified_at_ms' in state['protection_timing'][card_id]:
                    observed=stream.observed_trades(base,route,role=role,historical=True)
                    trade=next((t for t in observed if t['card_id']==card_id),None)
                    if trade and trade['protection_verified']:
                        status='NEW_FILL_STOP_AND_TAKE_VERIFIED'
                        break
            stopped.wait(.25)
        state=base.store.load(state['bucket'])
        return dict(status=status,card_id=card_id,account_role=role,symbol=state['symbol'],
            source_event_id=card['prepared']['source']['event_id'],
            timing=state.get('protection_timing',{}).get(card_id),
            emergency_status=(state.get('emergency') or {}).get('phase'),
            continuous_entry_flags_changed=False,entry_attempts=1 if entry_invoked else 0,
            cycle_status_counts=cycle_status_counts,
            order_requests_sent=controlled.venue.sent,
            mainnet_enabled=False)
    finally:
        stopped.set()


if __name__=='__main__':
    import sys
    if len(sys.argv)!=2:
        raise SystemExit('EXACT_CARD_ID_ARGUMENT_REQUIRED')
    print(json.dumps({'testnet_new_fill_protection_timing':run_one(dict(os.environ),sys.argv[1])},sort_keys=True))
