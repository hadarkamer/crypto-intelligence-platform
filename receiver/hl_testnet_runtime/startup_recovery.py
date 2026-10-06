"""Explicit, disabled-entry Testnet incident archival. No orders or flag changes."""
import json

from . import emergency_close as emergency
from .filled_dispatch_store import DispatchError

APPROVAL='verified_closed_incidents_v1'
KEY='HL_TESTNET_STARTUP_RECOVERY'


def run(normal):
    env=normal.venue.env
    mode=env.get(KEY,'')
    if mode and (mode!=APPROVAL or not emergency.continuous_configuration(env)
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED')!='false'
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED')!='false'):
        raise DispatchError('STARTUP_RECOVERY_REQUIRES_DISABLED_TESTNET_ENTRIES')
    controller=emergency.Controller(normal) if mode else None
    from .bounded_entry_trial import configuration
    for role,route in normal.routes.items():
        scope=configuration(env,role,route['account'])
        states=normal.store.for_account(route['account'])
        report=dict(account_role=role,entry_trial=scope and {
            key:scope[key] for key in ('epoch_ms','deadline_ms','max_attempts') if key in scope},
            buckets=[dict(symbol=s['symbol'],pending=s.get('pending') is not None,
                owned_cards=len(s['bindings']),
                emergency_phase=(s.get('emergency') or {}).get('phase')) for s in states])
        print(json.dumps({'testnet_startup_inventory':report},sort_keys=True),flush=True)
        if not mode:
            continue
        for state in states:
            incident=state.get('emergency')
            if incident is None:
                continue
            if incident.get('phase')!='CLOSED_VERIFIED':
                print(json.dumps({'testnet_startup_recovery':dict(account_role=role,
                    symbol=state['symbol'],status='ACTIVE_INCIDENT_RETAINED',
                    order_requests_sent=0)},sort_keys=True),flush=True)
                continue
            try:
                result=controller.release_closed_incident(state['bucket'],
                    account=state['account'],incident_id=emergency.incident_identity(state))
                report=dict(account_role=role,symbol=state['symbol'],status=result['status'],
                            order_requests_sent=0)
            except Exception as exc:
                from .long_stream_runtime import _safe_failure
                report=dict(account_role=role,symbol=state['symbol'],
                    status='INCIDENT_RETAINED_ENTRIES_DISABLED',order_requests_sent=0,
                    **_safe_failure(exc))
            print(json.dumps({'testnet_startup_recovery':report},sort_keys=True),flush=True)
