"""OFFLINE exit-recovery rehearsal. No transport, signer, environment or timers.

Consume existing lifecycle evidence; emit one descriptive correction at a time.
This is NOT a wire payload or permission to trade. Grouping, request outcomes and
a fresh mark must be supplied explicitly by a future verified adapter. Linked
parent handoff and emergency flattening remain policy gates.
"""
from copy import deepcopy
from decimal import Decimal, localcontext
from . import card_lifecycle as life

VERSION = 'offline-card-exit-recovery-v1'
EXITS = ('STOP', 'TAKE_PROFIT')
REMEDIABLE = frozenset(('STOP_COVERAGE_MISSING', 'TAKE_PROFIT_COVERAGE_MISSING',
    'STOP_EXCEEDS_CARD_REMAINDER', 'TAKE_PROFIT_EXCEEDS_CARD_REMAINDER',
    'FLAT_WITH_WORKING_ORDERS'))
REQUEST_STATES = ('NONE', 'ACCEPTED_UNVERIFIED', 'OUTCOME_UNKNOWN', 'REJECTED')
ERRORS = {
    'Price must be divisible by tick size.': 'INVALID_PRICE',
    'Order must have minimum value of $10.': 'MINIMUM_NOTIONAL',
    'Insufficient margin to place order.': 'INSUFFICIENT_MARGIN',
    'Reduce only order would increase position.': 'REDUCE_ONLY',
    'Invalid TP/SL price.': 'INVALID_TRIGGER',
    'No liquidity available for market order.': 'NO_LIQUIDITY',
}


class RecoveryError(life.LifecycleError):
    """Fixed local codes only; do not include raw replies, keys or addresses."""


def evidence_key(bindings, snapshot, context):
    """Heartbeat timestamps and list delivery order do not create new work."""
    snap = deepcopy(snapshot)
    snap.pop('at_ms')
    snap['fills'] = sorted({f['fill_id']: f for f in snap['fills']}.values(), key=lambda f: f['fill_id'])
    for name in ('open_orders', 'terminal_orders'):
        snap[name] = sorted(snap[name], key=lambda row: row['oid'])
    normalized = deepcopy(bindings)
    for binding in normalized:
        for leg in life.LEGS:
            binding['orders'][leg] = sorted(binding['orders'][leg])
    ctx = deepcopy(context)
    ctx.pop('at_ms')
    return life.digest([sorted(normalized, key=lambda b: b['card_id']), snap, ctx])


def classify_reply(raw, legs):
    """Interpret per-leg AND whole-batch rejection; an OK response is not a fill.

    Ambiguous bodies are UNKNOWN, never a confirmed rejection permitting resend.
    Even a filled receipt needs independent evidence. No remote error is echoed.
    The whole-batch singleton-error representation is distinct from a truncated
    success vector, which must never be assigned to several legs.
    """
    if not isinstance(legs, (list, tuple)) or not legs or len(set(legs)) != len(legs) or any(x not in life.LEGS for x in legs):
        raise RecoveryError('EXPLICIT_DISTINCT_REPLY_LEGS_REQUIRED')
    def unknown():
        return {leg: dict(state='OUTCOME_UNKNOWN', code=None, oid=None) for leg in legs}
    def rejected(message):
        return dict(state='REJECTED', code=ERRORS.get(message, 'OTHER_REJECTION'), oid=None)
    if not isinstance(raw, dict):
        return unknown()
    if raw.get('status') == 'err' and isinstance(raw.get('response'), str):
        return {leg: rejected(raw['response']) for leg in legs}
    response = raw.get('response')
    if raw.get('status') != 'ok' or not isinstance(response, dict) or response.get('type') != 'order':
        return unknown()
    data = response.get('data')
    items = data.get('statuses') if isinstance(data, dict) else None
    if (isinstance(items, list) and len(items) == 1 and isinstance(items[0], dict)
            and set(items[0]) == {'error'} and isinstance(items[0]['error'], str)):
        return {leg: rejected(items[0]['error']) for leg in legs}
    if not isinstance(items, list) or len(items) != len(legs):
        return unknown()
    out = unknown()
    seen_oids = set()
    for leg, item in zip(legs, items):
        if not isinstance(item, dict) or len(item) != 1:
            continue
        if 'error' in item and isinstance(item['error'], str):
            out[leg] = rejected(item['error'])
        elif set(item) in ({'resting'}, {'filled'}):
            body = next(iter(item.values()))
            oid = body.get('oid') if isinstance(body, dict) else None
            if type(oid) is int and 0 < oid < 2**64:
                if oid in seen_oids:
                    return unknown()
                seen_oids.add(oid)
                out[leg] = dict(state='ACCEPTED_UNVERIFIED', code=None, oid=str(oid))
    return out


def plan(bindings, snapshot, context, *, now_ms):
    """One OFFLINE proposal; preserve entry, original prices and unrelated cards.

    context supplies a fresh mark and a complete control map. independent_fixed
    requires verified grouping and NONE requires no unsettled request in the
    future durable sender. Neither is inferred here. A rejection without an OID
    belongs in controls, not an invented binding. Returned proposals still need
    exchange precision/capacity validation, serialization and explicit authority.
    """
    view = life.review(bindings, snapshot, now_ms=now_ms)
    life.shape(context, 'symbol mark_price at_ms cards')
    if context['symbol'] != snapshot['symbol']:
        raise RecoveryError('MARK_SYMBOL_MISMATCH')
    mark = life.number(context['mark_price'], positive=True)
    at = life.moment(context['at_ms'])
    chosen = {b['card_id']: b for b in bindings if life.address(b['account']) == life.address(snapshot['account']) and b['symbol'] == snapshot['symbol']}
    controls = context['cards']
    if not isinstance(controls, dict) or set(controls) != set(chosen):
        raise RecoveryError('ALL_BUCKET_CONTROLS_REQUIRED')
    reasons = set(view['bucket_issues'])
    if not 0 <= now_ms-at <= 15000 or abs(at-snapshot['at_ms']) > 15000:
        reasons.add('MARK_STALE_OR_NOT_COMPARABLE')
    for control in controls.values():
        life.shape(control, 'grouping requests')
        if control['grouping'] not in ('independent_fixed', 'parent_linked', 'unknown'):
            raise RecoveryError('INVALID_GROUPING_EVIDENCE')
        life.shape(control['requests'], 'STOP TAKE_PROFIT')
        for request in control['requests'].values():
            life.shape(request, 'state code')
            if request['state'] not in REQUEST_STATES:
                raise RecoveryError('INVALID_REQUEST_STATE')
            if request['state'] == 'REJECTED':
                if request['code'] not in set(ERRORS.values()) | {'OTHER_REJECTION'}:
                    raise RecoveryError('NORMALIZED_REJECTION_REQUIRED')
            elif request['code'] is not None:
                raise RecoveryError('UNEXPECTED_ERROR_CODE')
            if request['state'] in ('OUTCOME_UNKNOWN', 'ACCEPTED_UNVERIFIED'):
                reasons.add('PRIOR_REQUEST_RECONCILIATION_REQUIRED')
    for card in view['cards']:
        reasons.update(set(card['issues'])-REMEDIABLE)
    terminal = {o['oid']: o for o in snapshot['terminal_orders']}
    for fill in snapshot['fills']:
        ending = terminal.get(fill['oid'])
        if ending and (fill['at_ms'] > ending['at_ms'] or ending['state'] == 'REJECTED'):
            reasons.add('INCONSISTENT_TERMINAL_FILL')
    opens = {o['oid']: o for o in snapshot['open_orders']}
    proposals, desired, urgent = [], [], []
    for card in view['cards']:
        cid = card['card_id']; binding = chosen[cid]; control = controls[cid]
        remaining = life.number(card['remaining_quantity'], signed=True)
        entered = life.number(card['entry_quantity'])
        own_open = [opens[oid] for leg in life.LEGS for oid in binding['orders'][leg] if oid in opens]
        desired.append(dict(card_id=cid, remaining_quantity=card['remaining_quantity'],
            stop_price=binding['prices']['stop'], take_profit_price=binding['prices']['take_profit'],
            planned_entry_quantity=binding['planned_quantity']))
        if remaining > 0 and life.number(card['stop_quantity_observed']) < remaining:
            urgent.append(cid)
        active_entries = any(o['oid'] in binding['orders']['ENTRY'] for o in own_open)
        working_exits = any(o['oid'] not in binding['orders']['ENTRY'] for o in own_open)
        if remaining == 0 and active_entries:
            if entered > 0:
                reasons.add('FLAT_WITH_ENTRY_REMAINDER_POLICY_REQUIRED')
            elif working_exits:
                reasons.add('UNFILLED_ENTRY_WITH_WORKING_EXITS_REQUIRES_REVIEW')
        corrections_needed = bool(set(card['issues']) & REMEDIABLE) or (remaining == 0 and working_exits)
        if not corrections_needed:
            continue
        if control['grouping'] != 'independent_fixed' or any(o['state'] == 'WAITING_PARENT' for o in own_open):
            reasons.add('LINKED_EXIT_HANDOFF_REQUIRES_REVIEW')
            continue
        for leg in EXITS:
            orders = [opens[oid] for oid in binding['orders'][leg] if oid in opens]
            if remaining < 0:
                continue
            if remaining == 0:
                for order in orders:
                    proposals.append((0, _step(binding, leg, 'CANCEL_ORPHAN_EXIT', '0', order['oid'])))
                continue
            with localcontext() as ctx:
                ctx.prec = 80
                current = sum((life.number(o['quantity']) for o in orders), Decimal(0))
            if current == remaining:
                continue
            if len(orders) > 1:
                reasons.add('MULTIPLE_EXIT_ORDERS_REQUIRE_REVIEW')
                continue
            request = control['requests'][leg]
            if request['state'] == 'REJECTED':
                reasons.add('REJECTED_EXIT_REQUIRES_RECHECK_' + request['code'])
                continue
            price = life.number(binding['prices']['stop' if leg == 'STOP' else 'take_profit'])
            crossed = (mark <= price if leg == 'STOP' else mark >= price) if binding['side'] == 'LONG' else (mark >= price if leg == 'STOP' else mark <= price)
            if crossed:
                reasons.add('EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE')
                continue
            operation = 'RESIZE_EXIT' if orders else 'CREATE_EXIT'
            proposals.append((1 if leg == 'STOP' else 2,
                _step(binding, leg, operation, life.text(remaining), orders[0]['oid'] if orders else None)))
    proposals.sort(key=lambda item: (item[0], item[1]['card_id'], item[1]['leg'], item[1]['order_id'] or ''))
    next_step = proposals[0][1] if proposals and not reasons else None
    return dict(version=VERSION, environment='testnet', simulation_only=True,
        bucket=life.digest(['testnet', life.address(snapshot['account']), snapshot['symbol']]),
        basis=evidence_key(bindings,snapshot,context), observed_at_ms=snapshot['at_ms'],
        valid_until_ms=min(snapshot['at_ms'],at)+15000,
        next_step=next_step, desired_exits=desired, urgent_card_ids=sorted(urgent),
        reasons=sorted(reasons), state='REVIEW_REQUIRED' if reasons else 'PROPOSED_OFFLINE' if next_step else 'NO_CORRECTION_NEEDED',
        dispatch_enabled=False, order_requests_sent=0, app_delivery_enabled=False,
        preserve_entry_orders=True, live_recovery_ready=False)


def _step(binding, leg, operation, quantity, oid):
    value = dict(card_id=binding['card_id'], card_digest=binding['card_digest'],
        account=life.address(binding['account']), symbol=binding['symbol'], role=binding['role'],
        leg=leg, operation=operation, order_id=oid, target_quantity=quantity,
        original_price=binding['prices']['stop' if leg == 'STOP' else 'take_profit'],
        original_prices=deepcopy(binding['prices']), planned_quantity=binding['planned_quantity'],
        reduce_only=True, side='A' if binding['side']=='LONG' else 'B')
    return {**value, 'intent_id': life.digest([VERSION,value])}


class Rehearsal:
    """Single-process OFFLINE state machine; JSON export is for restart tests only.

    One unsettled operation per bucket. Timeout/rejection cannot unlock by time
    alone. A receipt is not protection. This is NOT a durable live dispatch queue
    or a cross-process lock. Never attach a sender to this class.
    """
    def __init__(self, bucket, state=None):
        life.ident(bucket, r'[0-9a-f]{64}')
        self.state = deepcopy(state) if state is not None else dict(bucket=bucket, revision=0,
            status='IDLE', pending=None, attempts=0, history=[])
        life.shape(self.state, 'bucket revision status pending attempts history')
        if self.state['bucket'] != bucket:
            raise RecoveryError('REHEARSAL_BUCKET_MISMATCH')
        if (type(self.state['revision']) is not int or self.state['revision'] < 0
                or type(self.state['attempts']) is not int or self.state['attempts'] < 0
                or not isinstance(self.state['history'], list)
                or self.state['status'] not in ('IDLE','AWAITING_REPLY','WAITING_FOR_EVIDENCE',
                    'UNCERTAIN_DO_NOT_RETRY','REJECTED_DO_NOT_RETRY','CONFLICT_RECONCILIATION_REQUIRED')
                or (self.state['status']=='IDLE') != (self.state['pending'] is None)):
            raise RecoveryError('INVALID_REHEARSAL_STATE')
        self.bucket = bucket

    def export(self):
        return deepcopy(self.state)

    def reserve(self, proposal, *, expected_revision, now_ms):
        if (proposal.get('version') != VERSION or proposal.get('simulation_only') is not True
                or proposal.get('dispatch_enabled') is not False or proposal.get('bucket') != self.bucket
                or proposal.get('state') != 'PROPOSED_OFFLINE' or not proposal.get('next_step')):
            raise RecoveryError('OFFLINE_PROPOSAL_REQUIRED')
        if type(expected_revision) is not int or expected_revision != self.state['revision']:
            raise RecoveryError('STALE_REHEARSAL_REVISION')
        life.moment(now_ms)
        if not proposal['observed_at_ms'] <= now_ms <= proposal['valid_until_ms']:
            raise RecoveryError('RECHECK_BEFORE_CORRECTION')
        step=proposal['next_step']
        if step['intent_id'] != life.digest([VERSION,{k:v for k,v in step.items() if k!='intent_id'}]):
            raise RecoveryError('RECOVERY_PROPOSAL_CHANGED')
        if self.state['status'] != 'IDLE':
            raise RecoveryError('PREVIOUS_OUTCOME_MUST_BE_RESOLVED')
        request_id = life.digest([self.bucket, step['intent_id'], self.state['revision']])
        self.state.update(status='AWAITING_REPLY', revision=self.state['revision']+1,
            attempts=self.state['attempts']+1,
            pending=dict(request_id=request_id, step=deepcopy(step),
                         basis=proposal['basis'], reserved_at_ms=now_ms, reply=None))
        return request_id

    def reply(self, request_id, result):
        pending = self.state['pending']
        if pending is None or request_id != pending['request_id']:
            raise RecoveryError('UNRELATED_RECOVERY_REPLY')
        life.shape(result, 'state code oid')
        if result['state'] not in ('ACCEPTED_UNVERIFIED','OUTCOME_UNKNOWN','REJECTED'):
            raise RecoveryError('INVALID_RECOVERY_REPLY')
        if result['state']=='ACCEPTED_UNVERIFIED':
            life.ident(result['oid'], r'[1-9][0-9]{0,19}')
            if int(result['oid']) >= 2**64 or result['code'] is not None:
                raise RecoveryError('INVALID_RECOVERY_REPLY')
        elif result['oid'] is not None:
            raise RecoveryError('INVALID_RECOVERY_REPLY')
        if result['state']=='REJECTED' and result['code'] not in set(ERRORS.values())|{'OTHER_REJECTION'}:
            raise RecoveryError('INVALID_RECOVERY_REPLY')
        if result['state']=='OUTCOME_UNKNOWN' and result['code'] is not None:
            raise RecoveryError('INVALID_RECOVERY_REPLY')
        if pending['reply'] is not None:
            if pending['reply']==result: return False
            self.state.update(status='CONFLICT_RECONCILIATION_REQUIRED',revision=self.state['revision']+1)
            raise RecoveryError('CONFLICTING_RECOVERY_REPLY_RECONCILE')
        pending['reply']=deepcopy(result)
        self.state.update(status='WAITING_FOR_EVIDENCE' if result['state']=='ACCEPTED_UNVERIFIED'
            else 'UNCERTAIN_DO_NOT_RETRY' if result['state']=='OUTCOME_UNKNOWN' else 'REJECTED_DO_NOT_RETRY',
            revision=self.state['revision']+1)
        return True

    def confirm(self, bindings, snapshot, context, *, now_ms):
        """Observe a simulated acknowledged effect, then recompute the next step.

        Effect observed and safety restored are DIFFERENT. A late entry fill can
        make the just-observed stop too small: release the resolved request, keep
        the gap visible, and propose a fresh resize instead of resending creation.
        An unknown receipt never comes here. A future adapter must independently
        bind the returned OID/client ID before this normalized rehearsal boundary.
        """
        pending=self.state['pending']
        if self.state['status']!='WAITING_FOR_EVIDENCE' or not pending:
            raise RecoveryError('ACKNOWLEDGED_OPERATION_REQUIRED')
        current=plan(bindings,snapshot,context,now_ms=now_ms)
        if current['bucket']!=self.bucket or snapshot['at_ms']<=pending['reserved_at_ms']:
            raise RecoveryError('NEW_SAME_BUCKET_EVIDENCE_REQUIRED')
        step=pending['step']; selected=[b for b in bindings if b['card_id']==step['card_id']]
        if len(selected)!=1 or selected[0]['card_digest']!=step['card_digest']:
            raise RecoveryError('ORIGINAL_CARD_REQUIRED')
        binding=selected[0]; oid=pending['reply']['oid']
        if (binding['prices']!=step['original_prices'] or binding['planned_quantity']!=step['planned_quantity']
                or life.address(binding['account'])!=step['account'] or binding['role']!=step['role']
                or binding['symbol']!=step['symbol']):
            raise RecoveryError('ORIGINAL_RECOVERY_TERMS_CHANGED')
        if oid not in binding['orders'][step['leg']]:
            raise RecoveryError('ACK_ORDER_NOT_BOUND_TO_OWN_CARD')
        if current['reasons']:
            raise RecoveryError('RECOVERY_EVIDENCE_STILL_UNSAFE')
        view=life.review(bindings,snapshot,now_ms=now_ms)
        card=next(c for c in view['cards'] if c['card_id']==step['card_id'])
        if step['operation']=='CANCEL_ORPHAN_EXIT':
            if oid!=step['order_id'] or not any(o['oid']==oid for o in snapshot['terminal_orders']):
                raise RecoveryError('EXACT_ORPHAN_NOT_TERMINAL')
        elif life.number(card['remaining_quantity'],signed=True)>0:
            working=[o for o in snapshot['open_orders'] if o['oid']==oid]
            if (len(working)!=1 or working[0]['state']!='ACTIVE'
                    or life.number(working[0]['quantity'])!=life.number(step['target_quantity'])):
                raise RecoveryError('ACKNOWLEDGED_EFFECT_NOT_OBSERVED')
        elif not card['closure_verified']:
            raise RecoveryError('FLAT_CARD_NOT_FINAL')
        self.state['history'].append(deepcopy(pending))
        self.state.update(status='IDLE',pending=None,revision=self.state['revision']+1)
        return {**current, 'acknowledged_effect_observed':True,
                'recovered_card_needs_more_work':bool(card['issues'])}
