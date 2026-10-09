"""Disabled prospective-plan signing boundary and isolated wire rehearsal.

This module is not imported by startup and has no HTTP, wallet/key discovery or
live sender. `send` always refuses. `rehearse` exercises the real canonical SDK
wire shape with software-only signer/transport ports and the existing one-use
shared-budget admission. Its source contract is never forged into a delivered
legacy alert. Live activation still requires a separately reviewed integration
of trusted collectors, account-wide journal ownership and release authority.
"""
from copy import deepcopy
from decimal import Decimal
import json

import approved_alert_contract as contract
from . import card_lifecycle as life, filled_quantity_dispatch as wire
from . import price_precision, risk_policy, two_account_execution as roles
from . import experimental_execution_prices as execution_prices
from .filled_dispatch_store import DefinitelyUnsent
from .long_stream_runtime import _validate_account_inventory

VERSION = 'experimental-disabled-dispatch-boundary-v1'
HOST = 'api.hyperliquid-testnet.xyz'


class BoundaryError(ValueError):
    pass


def readiness():
    return dict(version=VERSION, dispatch_enabled=False, signer_connected=False,
        production_collector_connected=False, production_release_authorized=False,
        shared_market_dispatch_enabled=False, order_requests_sent=0)


def _fresh(at, now, age, reason):
    if type(at) is not int or type(now) is not int or not 0 <= now-at <= age:
        raise BoundaryError(reason)


def _source(source, current, now, *, entry):
    source = contract.validate(source)
    latest = contract.validate(current['source'])
    if (source['occurrence_id'] != latest['occurrence_id']
            or contract.plan_digest(source) != contract.plan_digest(latest)
            or current['plan_digest'] != contract.plan_digest(source)):
        raise BoundaryError('FROZEN_PROSPECTIVE_SOURCE_CHANGED')
    if entry:
        approved = contract.is_approved(latest)
        if (current['entry_permission'] != 'WAITING' or current.get('cancellation') is not None
                or latest['kind'] == 'CANCEL'
                or latest['source_state'] != ('APPROVED' if approved else 'PENDING')):
            raise BoundaryError('SOURCE_ENTRY_PERMISSION_RETIRED')
        if (not contract.moment_ms(latest['arm_at']) <= now
                or contract.moment_ms(latest['source_as_of']) > now
                or latest['valid_until'] is not None and now >= contract.moment_ms(latest['valid_until'])
                or latest['expires_at'] is not None and now >= contract.moment_ms(latest['expires_at'])):
            raise BoundaryError('ORIGINAL_SOURCE_WINDOW_EXPIRED')
    return source


def _terms(request, source, metadata):
    p=request['proposal']
    role='long_account' if source['side']=='LONG' else 'short_account'
    if (p['card_id'] != source['occurrence_id'] or p['symbol'] != source['symbol']
            or p['role'] != role or p['source_at'] != source['source_at']):
        raise BoundaryError('SOURCE_REQUEST_IDENTITY_MISMATCH')
    prepared=execution_prices.prepare(source,metadata)
    index,decimals=wire.asset(metadata,source['symbol'])
    action=wire.canonical_wire_action(p['action'])
    if action != p['action']:
        raise BoundaryError('EXACT_CANONICAL_ACTION_REQUIRED')
    if action['type']=='cancel':
        item=action['cancels'][0]
        if item['a'] != index or type(item['o']) is not int or not 0<item['o']<2**64:
            raise BoundaryError('EXACT_CANCEL_ASSET_REQUIRED')
    else:
        o=wire.requested_order(action)
        if (type(o['a']) is not int or o['a']!=index or type(o['b']) is not bool
                or type(o['r']) is not bool or life.number(o['s'],positive=True)!=life.number(p['quantity'],positive=True)):
            raise BoundaryError('EXACT_ORDER_ASSET_AND_SIZE_REQUIRED')
        life.ident(o['c'],r'0x[0-9a-f]{32}')
        wire.precise(o['p'],o['s'],decimals)
        levels=prepared['execution']
        if p['operation']=='ENTRY':
            if (p['leg']!='ENTRY' or action['type']!='order' or o['r'] is not False
                    or o['b'] is not (source['side']=='LONG')
                    or o['p']!=levels['entry'] or o['t']!={'limit':{'tif':('Gtc' if source['family'] in ('maxpain','sol_g65') else 'Ioc')}}):
                raise BoundaryError('EXACT_FROZEN_ENTRY_ACTION_REQUIRED')
            # The live filled-quantity lane submits an entry before observed-fill
            # exits. Apply the SAME risk rule to an exact synthetic bracket;
            # canonical syntax alone is not a risk check.
            bracket=deepcopy(o)
            bracket['t']={'limit':{'tif':'Gtc'}}
            exits=[]
            for leg,field in (('sl','stop'),('tp','take_profit')):
                exits.append(dict(a=index,b=not o['b'],p=levels[field],s=o['s'],r=True,
                    t=dict(trigger=dict(isMarket=leg=='sl',triggerPx=levels[field],tpsl=leg)),c=o['c']))
            risk_policy.assert_new_entry_budget(dict(type='order',orders=[bracket]+exits,grouping='normalTpsl'))
        elif (p['leg'] not in ('STOP','TAKE_PROFIT') or o['r'] is not True
                or o['b'] is not (source['side']=='SHORT')):
            raise BoundaryError('REDUCE_ONLY_EXACT_OWNER_EXIT_REQUIRED')
        elif p['operation']=='EMERGENCY_CLOSE':
            if action['type']!='order' or p['leg']!='STOP' or o['t']!={'limit':{'tif':'Ioc'}}:
                raise BoundaryError('EXACT_EMERGENCY_IOC_REQUIRED')
        else:
            expected=levels['stop' if p['leg']=='STOP' else 'take_profit']
            allowed={expected}
            if p['leg']=='STOP' and source['policy'].get('locked_stop'):
                allowed.add(price_precision.round_price(source['policy']['locked_stop'],decimals))
            if (o['p'] not in allowed or o['t']!=dict(trigger=dict(isMarket=p['leg']=='STOP',
                    triggerPx=o['p'],tpsl='sl' if p['leg']=='STOP' else 'tp'))):
                raise BoundaryError('FROZEN_EXIT_TERMS_REQUIRED')
    return prepared, action


def _lock(source, proof, now, *, current):
    if source['family']=='r2732':
        from . import r2732_conditional_stop as reducer
    elif source['family']=='sol_g65':
        from . import sol_g65_conditional_stop as reducer
    else:
        raise BoundaryError('FORMULA_HAS_NO_STOP_LOCK')
    value=reducer.validate(proof)
    original=reducer.initialize_from_contract(source['occurrence_id'],source)
    for key in ('card_id','source_event_id','price_source','levels'):
        if value.get(key)!=original.get(key):
            raise BoundaryError('STOP_LOCK_CONTRACT_CHANGED')
    if value['lock_effective_at_ms'] is None or value['lock_effective_at_ms']>now:
        raise BoundaryError('CLOSED_MINUTE_STOP_LOCK_NOT_DUE')
    if current:
        if value['status'] not in ('OPEN','AWAITING_CLOSED_BAR') or value['outcome'] is not None:
            raise BoundaryError('CLOSED_MINUTE_STOP_LOCK_PROOF_REQUIRED')
        _fresh(value['cursor_ms']+60000,now,15000,'STOP_LOCK_CANDLE_EVIDENCE_EXPIRED')
    return value


def _owned_snapshot(owner,snapshot,source,context,now):
    """Project only proven replacement OIDs into legacy immutable-price review."""
    projected=deepcopy(snapshot)
    for row in projected['open_orders']:
        if row['oid'] not in owner['orders']['STOP'] or row['trigger_price']==owner['prices']['stop']:
            continue
        matches=[r for r in context.get('observed_stop_requests',[]) if r.get('phase')=='OBSERVED'
                 and r.get('observed_oid')==row['oid']]
        if len(matches)!=1:
            raise BoundaryError('REPLACEMENT_STOP_NOT_BOUND_TO_DURABLE_REQUEST')
        request=matches[0];_,action=_terms(request,source,context['metadata'])
        order=wire.requested_order(action)
        if (request['proposal']['account']!=owner['account'] or request['proposal']['leg']!='STOP'
                or row['order_type']!='SL_MARKET' or row['reduce_only'] is not True
                or row['trigger_price']!=order['p'] or row['price']!=order['p']):
            raise BoundaryError('REPLACEMENT_STOP_TERMS_CHANGED')
        _lock(source,context['lock_proof'],now,current=False)
        row['trigger_price']=owner['prices']['stop']
    return projected


def review(request, context, *, now_ms):
    """Validate exact frozen source, authoritative ownership and safety inputs.

    Context is supplied by a trusted collector/worker, never by an HTTP client.
    It retains legacy market buckets for complete account inventory review.
    This pure result is a diagnostic, not transport or release authorization.
    """
    if request.get('domain') not in ('software','testnet'):
        raise BoundaryError('EXPLICIT_REQUEST_DOMAIN_REQUIRED')
    DefinitelyUnsent.identity(request)
    p=request['proposal']; entry=p['operation']=='ENTRY'
    _fresh(request['attempt_at_ms'],now_ms,5000,'DURABLE_ATTEMPT_EXPIRED')
    _fresh(p['observed_at_ms'],now_ms,15000,'FINAL_EVIDENCE_EXPIRED')
    if (request['prepared_at_ms']>request['attempt_at_ms']
            or p['observed_at_ms']>request['attempt_at_ms']
            or not request['attempt_at_ms']<=request['nonce']<=request['attempt_at_ms']+1000):
        raise BoundaryError('DURABLE_ATTEMPT_TIMELINE_INVALID')
    source=_source(context['source'],context['current_source'],now_ms,entry=entry)
    route=roles.route_for(context['env'],p['role'],p['account'],context['agent'],source['side'])
    other='short_account' if p['role']=='long_account' else 'long_account'
    if roles.route_for(context['env'],other)['account']==route['account']:
        raise BoundaryError('INDEPENDENT_ACCOUNT_ROUTES_REQUIRED')
    if context['host']!=HOST:
        raise BoundaryError('TESTNET_HOST_REQUIRED')
    prepared,action=_terms(request,source,context['metadata'])
    safety=context['safety']
    _fresh(safety['at_ms'],now_ms,15000,'ACCOUNT_SAFETY_EVIDENCE_EXPIRED')
    if safety['account']!=p['account'] or safety['role']!=p['role']:
        raise BoundaryError('ACCOUNT_SAFETY_SCOPE_CHANGED')
    if entry:
        if (safety['entry_enabled'] is not True or safety['emergency_healthy'] is not True
                or safety['feed_reconciled'] is not True or safety['entry_circuit_clear'] is not True):
            raise BoundaryError('NEW_ENTRY_SAFETY_GATE_CLOSED')
        _fresh(safety['supervisor_at_ms'],now_ms,15000,'EMERGENCY_SUPERVISOR_STALE')
        if contract.moment_ms(source['approved_at'] if contract.is_approved(source) else source['created_at'])<safety['not_before_ms']:
            raise BoundaryError('SOURCE_PRECEDES_RELEASE_WINDOW')
        # Unknown orders, foreign/manual position, opposite exposure and ANY
        # unresolved request close only the entry lane, preserving exit work.
        _validate_account_inventory(p['account'],context['buckets'],context['open_orders'],
                                    context['positions'],role=p['role'])
        bucket=next((s for s in context['buckets'] if s['symbol']==p['symbol']),None)
        if bucket and bucket['bindings']:
            view=life.review(bucket['bindings'],bucket['evidence']['snapshot'],now_ms=now_ms)
            if view['bucket_issues'] or any(v['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')
                    or v['issues'] for v in view['cards']):
                raise BoundaryError('SHARED_MARKET_PREDECESSOR_NOT_FINAL')
        market=context['market']
        if market['environment']!='testnet' or market['symbol']!=p['symbol']:
            raise BoundaryError('ACTUAL_TESTNET_MARK_REQUIRED')
        _fresh(market['at_ms'],now_ms,15000,'ACTUAL_TESTNET_MARK_EXPIRED')
        mark=life.number(market['mark_price'],positive=True)
        levels=prepared['execution']
        if not min(Decimal(levels['stop']),Decimal(levels['take_profit']))<mark<max(Decimal(levels['stop']),Decimal(levels['take_profit'])):
            raise BoundaryError('TESTNET_MARK_OUTSIDE_FROZEN_EXITS')
        if (not contract.is_approved(source) and source['family'] in ('maxpain','sol_g65')
                and (mark<=Decimal(levels['entry']) if source['side']=='LONG' else mark>=Decimal(levels['entry']))):
            raise BoundaryError('PROSPECTIVE_TESTNET_ENTRY_ALREADY_REACHED')
        # The configured signer must belong to this actual exchange account,
        # with room left for the exits. Margin acceptance belongs to the venue;
        # local order limits remain independent of its available balance.
        account_check=context['entry_account']
        if (account_check.get('account')!=route['account']
                or account_check.get('agent')!=route['agent']
                or type(account_check.get('action_headroom')) is not int
                or account_check['action_headroom']<roles.ENTRY_ACTION_HEADROOM):
            raise BoundaryError('EXACT_CURRENT_ENTRY_ACCOUNT_REQUIRED')
        _fresh(account_check.get('at_ms'),now_ms,15000,'ENTRY_ACCOUNT_SAMPLE_EXPIRED')
        for field,limit in (('signer_at_ms',299999),('allowance_at_ms',59999)):
            _fresh(account_check.get(field,account_check['at_ms']),now_ms,limit,'ENTRY_ACCOUNT_SAMPLE_EXPIRED')
        quantity=life.number(p['quantity'],positive=True)
        if not Decimal(10)<=quantity*Decimal(levels['entry'])<=Decimal(5000):
            raise BoundaryError('OUTSIDE_LAB_SIZE_BOUNDS')
        if quantity*mark>Decimal(5000):
            raise BoundaryError('MARK_NOTIONAL_EXCEEDS_LAB_CAP')
    else:
        owner=context['owner']
        life.validate_bindings([owner])
        snap=context['owner_snapshot']; life.validate_snapshot(snap)
        if (owner['card_id']!=p['card_id'] or owner['account']!=p['account']
                or owner['symbol']!=p['symbol'] or owner['role']!=p['role']
                or owner['side']!=source['side'] or owner['card_digest']!=life.digest(source)
                or owner['prices']!={k:prepared['execution'][k] for k in ('entry','stop','take_profit')}
                or snap['account']!=p['account']
                or snap['symbol']!=p['symbol']):
            raise BoundaryError('EXACT_EXIT_OWNER_REQUIRED')
        view=life.review([owner],_owned_snapshot(owner,snap,source,context,now_ms),now_ms=now_ms)
        allowed={'STOP_COVERAGE_MISSING','STOP_EXCEEDS_CARD_REMAINDER',
                 'TAKE_PROFIT_COVERAGE_MISSING','TAKE_PROFIT_EXCEEDS_CARD_REMAINDER','FLAT_WITH_WORKING_ORDERS'}
        if view['bucket_issues'] or set(view['cards'][0]['issues'])-allowed:
            raise BoundaryError('EXIT_OWNERSHIP_UNRECONCILED')
        active_ids={r['oid'] for r in snap['open_orders']}
        if action['type']=='cancel':
            if (p['operation']!='CANCEL' or str(action['cancels'][0]['o']) not in active_ids
                    or str(action['cancels'][0]['o']) not in sum(owner['orders'].values(),[])):
                raise BoundaryError('CANCEL_OID_NOT_OWNED')
        else:
            if life.number(p['quantity'],positive=True)!=life.number(view['cards'][0]['remaining_quantity']):
                raise BoundaryError('EXIT_EXCEEDS_OBSERVED_OWN_REMAINING')
            own_active=active_ids & set(owner['orders'][p['leg']])
            if action['type']=='batchModify':
                if (p['operation'] not in ('MODIFY_EXIT','AMEND_EXIT')
                        or own_active!={str(action['modifies'][0]['oid'])}):
                    raise BoundaryError('MODIFY_OID_NOT_EXCLUSIVELY_ACTIVE')
            elif p['operation']!='EMERGENCY_CLOSE' and (p['operation']!='CREATE_EXIT' or own_active):
                raise BoundaryError('CREATE_EXIT_ALREADY_ACTIVE_OR_UNKNOWN_OPERATION')
            # Closed-minute proof must be exact to the contract and target;
            # a passed mark or caller-supplied locked price is never enough.
            o=wire.requested_order(action)
            old=prepared['execution']['stop']
            if p['operation']=='EMERGENCY_CLOSE':
                from .emergency_close import close_price, trigger, DEADLINE_MS
                sample=p['sample']
                _fresh(sample['at_ms'],now_ms,DEADLINE_MS,'EMERGENCY_MARK_EXPIRED')
                _fresh(snap['at_ms'],now_ms,DEADLINE_MS,'EMERGENCY_OWNER_SNAPSHOT_EXPIRED')
                _,decimals=wire.asset(context['metadata'],p['symbol'])
                if life.number(o['p'])!=life.number(close_price(sample['mark_price'],decimals,buy=o['b'])):
                    raise BoundaryError('EXACT_EMERGENCY_MARK_LIMIT_REQUIRED')
                if life.number(p['quantity'])!=life.number(view['cards'][0]['remaining_quantity']):
                    raise BoundaryError('EMERGENCY_EXACT_REMAINDER_REQUIRED')
                if active_ids & set(owner['orders']['ENTRY']):
                    raise BoundaryError('EMERGENCY_ENTRY_REMAINDER_NOT_FINAL')
                stop=life.number(prepared['execution']['stop'])
                lock=context.get('lock_proof')
                if lock and lock.get('lock_effective_at_ms') is not None:
                    _lock(source,lock,now_ms,current=False)
                    stop=life.number(price_precision.round_price(source['policy']['locked_stop'],decimals))
                mark=life.number(sample['mark_price'],positive=True)
                take=life.number(prepared['execution']['take_profit'])
                crossed=(mark<=stop or mark>=take) if source['side']=='LONG' else (mark>=stop or mark<=take)
                if not crossed:
                    # Reuse the established protection deadline exactly. Its
                    # proof is derived from independently observed fills and
                    # stop coverage, never from a proposal reason or boolean.
                    # An uncertain STOP is not replayed; this independent
                    # reduce-only IOC remains bound to the sole exact owner.
                    deadline=trigger(dict(bindings=[owner],evidence=dict(snapshot=
                        _owned_snapshot(owner,snap,source,context,now_ms))),now_ms=now_ms)
                    if (deadline is None or deadline['card_id']!=p['card_id']
                            or deadline['reason']!='STOP_VERIFICATION_DEADLINE'):
                        raise BoundaryError('EMERGENCY_REQUIRES_CROSSED_FROZEN_EXIT_OR_UNCOVERED_DEADLINE')
            elif p['leg']=='STOP' and o['p']!=old:
                _lock(source,context['lock_proof'],now_ms,current=True)
    return dict(**readiness(),reviewed=True,request_digest=DefinitelyUnsent.identity(request),
                context_digest=life.digest(context),action=action,agent=route['agent'])


class DisabledDispatchPort:
    """No live override. Replay uses explicit software-only collaborators."""
    def send(self, *args, **kwargs):
        raise BoundaryError('EXPERIMENTAL_LIVE_DISPATCH_NOT_RELEASED')

    def rehearse(self, request, context, *, admission, signer, transport, clock):
        if (request.get('domain')!='software' or getattr(signer,'domain',None)!='software'
                or getattr(transport,'domain',None)!='software'):
            raise BoundaryError('ISOLATED_SIGNER_AND_TRANSPORT_REQUIRED')
        if type(admission) is not wire.TransportAdmission:
            raise BoundaryError('EXACT_SINGLE_USE_TRANSPORT_ADMISSION_REQUIRED')
        value=deepcopy(request); frozen=deepcopy(context)
        first=review(value,frozen,now_ms=clock())
        if life.address(signer.address)!=first['agent']:
            raise BoundaryError('ROLE_SIGNER_MISMATCH')
        admission.validate(value)
        action=deepcopy(first['action']); expires=value['nonce']+15000
        signature=signer.sign(action,None,value['nonce'],expires,False)
        if action!=first['action']:
            raise BoundaryError('SIGNER_MUTATED_FROZEN_ACTION')
        body=dict(action=action,nonce=value['nonce'],signature=signature,expiresAfter=expires)
        if len(json.dumps(body,allow_nan=False).encode())>16384:
            raise BoundaryError('REQUEST_TOO_LARGE')
        # Source cancellation/safety changes during signing must be reloaded by
        # the caller. Rehearsal validates a current provider, never stale copy.
        latest=transport.current_context()
        second=review(value,latest,now_ms=clock())
        if first['context_digest']!=second['context_digest']:
            raise BoundaryError('DISPATCH_CONTEXT_CHANGED_RECONCILE_FIRST')
        admission.consume(value)
        # At most once, even if the response is lost. Caller retains its durable
        # OUTCOME_UNKNOWN fence; neither this adapter nor transport retries.
        raw=transport.exchange(HOST,'/exchange',body)
        return dict(**readiness(),status='SOFTWARE_TRANSPORT_REPLIED',
                    normalized_reply=wire.normalized_reply(raw,action['type']),
                    simulated_transport_requests=1)
