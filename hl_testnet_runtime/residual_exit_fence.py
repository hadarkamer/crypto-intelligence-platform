"""Exact-owner residual cleanup and quantity-isolation barriers.

No network, signer, timer, app input or trading-policy switch. Reuse the existing
controller and durable request lock. A cancel acknowledgement is NOT retirement.
These guards do NOT turn two independent full-size exits into exchange-native
OCO. The shared-market release blocker remains: a local lock cannot prevent a
previously accepted sibling from filling before its cancellation reaches the venue.
"""
from copy import deepcopy
from decimal import Decimal, localcontext
from . import card_lifecycle as life

VERSION = 'residual-exit-fence-v1'
EXITS = ('STOP', 'TAKE_PROFIT')
AFTER_EXIT = 'cancel_remainder_after_exit_v1'
CLEANUP_ISSUES = {
    'FLAT_WITH_WORKING_ORDERS', 'STOP_COVERAGE_MISSING',
    'TAKE_PROFIT_COVERAGE_MISSING', 'STOP_EXCEEDS_CARD_REMAINDER',
    'TAKE_PROFIT_EXCEEDS_CARD_REMAINDER', 'BOTH_EXIT_LEGS_FILLED_REVIEW',
    'EXIT_EXCEEDS_CARD_QUANTITY',
}


class FenceError(life.LifecycleError):
    """Stable local diagnostics only."""


def _read(state, now_ms):
    if not isinstance(state, dict) or not isinstance(state.get('evidence'), dict):
        raise FenceError('BOT_EXECUTION_EVIDENCE_REQUIRED')
    life.shape(state['evidence'], 'bindings snapshot')
    bs = state['bindings']; snap = state['evidence']['snapshot']
    account, symbol = life.validate_snapshot(snap)
    if (state['evidence']['bindings'] != bs or state['account'] != account
            or state['symbol'] != symbol or state['bucket'] != life.digest(['testnet', account, symbol])):
        raise FenceError('RESIDUAL_BUCKET_IDENTITY_MISMATCH')
    life.moment(now_ms)
    if not snap['history_complete'] or not snap['orders_complete'] or not 0 <= now_ms-snap['at_ms'] <= 15000:
        raise FenceError('RESIDUAL_EVIDENCE_INCOMPLETE_OR_STALE')
    if not bs:
        if snap['open_orders'] or snap['terminal_orders'] or snap['fills'] or life.number(snap['position_quantity'], signed=True):
            raise FenceError('UNREGISTERED_ACTIVITY_REQUIRES_REVIEW')
        return bs, snap, dict(cards=[], bucket_issues=[], needs_review=False)
    from .trade_cards import validate_card
    life.validate_bindings(bs)
    for b in bs:
        if b['account'] != account or b['symbol'] != symbol:
            raise FenceError('ONE_EXACT_REGISTERED_MARKET_REQUIRED')
        original = state['originals'].get(b['card_id'])
        if not isinstance(original, dict):
            raise FenceError('IMMUTABLE_ORIGINAL_REQUIRED')
        card = validate_card(original['card'])
        expected = life.binding_from_card(card, account, {b['role']:dict(account=account)}, b['orders'])
        if b != expected:
            raise FenceError('RESIDUAL_OWNER_BINDING_CHANGED')
    view = life.review(bs, snap, now_ms=now_ms)
    return bs, snap, view


def _open_for(binding, snap, legs=EXITS):
    own = {oid:leg for leg in legs for oid in binding['orders'][leg]}
    return [(own[o['oid']], o) for o in snap['open_orders'] if o['oid'] in own]


def exposure(state, *, now_ms):
    """Worst-case independent exit capacity, NOT max(TP, SL) or net position.

    If sum(outstanding independent exits) <= own remainder, arbitrary fills of
    those exits cannot exceed this card's allocation. Two full-size siblings
    violate that condition; reduceOnly on the aggregate position does not fix it.
    Pending request uncertainty is also a barrier, not zero capacity.
    """
    bs, snap, view = _read(state, now_ms)
    rows = []
    with localcontext() as ctx:
        ctx.prec = 80
        for b in bs:
            card = next(v for v in view['cards'] if v['card_id'] == b['card_id'])
            remaining = life.number(card['remaining_quantity'], signed=True)
            exits = _open_for(b, snap)
            total = sum((life.number(o['quantity']) for _,o in exits), Decimal(0))
            working = bool(_open_for(b, snap, life.LEGS))
            rows.append(dict(card_id=b['card_id'], remaining_quantity=life.text(remaining),
                outstanding_exit_quantity=life.text(total),
                excess_independent_capacity=life.text(max(Decimal(0), total-max(remaining, Decimal(0)))),
                flat_with_residual_orders=remaining <= 0 and bool(exits),
                active_or_pending=remaining > 0 or working,
                all_registered_orders_terminal=all(oid in {t['oid'] for t in snap['terminal_orders']}
                    for leg in life.LEGS for oid in b['orders'][leg])))
    return dict(version=VERSION, cards=rows, bucket_issues=deepcopy(view['bucket_issues']),
        pending_request_blocks_new_work=state.get('pending') is not None,
        native_per_card_oco_verified=False, shared_market_release_authorized=False)


def cleanup(state, *, now_ms, after_exit_policy='NOT_SELECTED'):
    """Cancel only an exact registered residual, without touching another card.

    Cleanup must not be suppressed merely because another card's TP/SL level
    has been crossed. Missing/contradictory ownership/history still blocks it.
    Entry remainder cancellation keeps the already-approved policy and priority.
    A partial exit can ALSO leave an oversized sibling, not just a flat card.
    Retire that exact order through the existing cancel/observe/replan path;
    do not create a replacement here or cancel a correctly-sized other leg.
    Never infer closure from the account's net position, an app row or a receipt.
    """
    bs, snap, view = _read(state, now_ms)
    if view['bucket_issues']:
        return None
    candidates = []
    for b in bs:
        card = next(v for v in view['cards'] if v['card_id'] == b['card_id'])
        if set(card['issues']) - CLEANUP_ISSUES:
            continue
        entered = life.number(card['entry_quantity'])
        exited = life.number(card['exit_quantity'])
        remaining = life.number(card['remaining_quantity'], signed=True)
        if entered <= 0 or exited <= 0:
            continue
        parents = _open_for(b, snap, ('ENTRY',))
        if parents:
            if after_exit_policy != AFTER_EXIT:
                continue
            for leg,o in parents:
                candidates.append((0, b['card_id'], leg, o['oid'], 'CANCEL_ENTRY_AFTER_EXIT'))
        elif remaining <= 0:
            for leg,o in _open_for(b, snap):
                if o['state'] == 'ACTIVE':
                    candidates.append((1, b['card_id'], leg, o['oid'], 'CANCEL_ORPHAN_EXIT'))
        else:
            # Preserve stop-first recovery ordering; only an individual order
            # that can exceed its owner's remainder is a cleanup candidate.
            # Two correctly-sized independent siblings still require the
            # separate capacity fence, never an invented native-OCO guarantee.
            for leg,o in _open_for(b, snap):
                if o['state'] == 'ACTIVE' and life.number(o['quantity']) > remaining:
                    candidates.append((2 if leg == 'STOP' else 3, b['card_id'], leg,
                                       o['oid'], 'CANCEL_FOR_RESIZE'))
    if not candidates:
        return None
    _, cid, leg, oid, operation = sorted(candidates)[0]
    return dict(card_id=cid, leg=leg, oid=oid, operation=operation)


def validate_proposal(state, proposal, *, now_ms):
    """Called INSIDE the existing DB lock at reserve and again before nonce use.

    A software record, stale quantity or reused sibling cannot authorize new
    size. Cancellation is narrowly targeted and can still reduce residual risk.
    The prospective independent-capacity guard is not a new trading approval.
    """
    bs, snap, view = _read(state, now_ms)
    if not isinstance(proposal, dict):
        raise FenceError('EXACT_RESIDUAL_PROPOSAL_REQUIRED')
    cid = proposal['card_id']; leg = proposal['leg']; action = proposal['action']
    original = state['originals'].get(cid)
    if (not isinstance(original, dict) or proposal['account'] != state['account']
            or proposal['symbol'] != state['symbol']
            or proposal['role'] != original['card']['account_role']
            or proposal['basis'] != life.digest(state['evidence'])):
        raise FenceError('RESIDUAL_PROPOSAL_NOT_BOUND_TO_STATE')
    b = next((x for x in bs if x['card_id'] == cid), None)
    cv = next((x for x in view['cards'] if x['card_id'] == cid), None)
    if action.get('type') == 'cancel':
        items = action.get('cancels')
        if b is None or leg not in life.LEGS or not isinstance(items, list) or len(items) != 1:
            raise FenceError('ONE_OWNED_CANCEL_REQUIRED')
        oid = proposal.get('old_oid')
        if (oid not in b['orders'][leg] or type(items[0].get('o')) is not int
                or str(items[0]['o']) != oid
                or not any(o['oid'] == oid for o in snap['open_orders'])):
            raise FenceError('CANCEL_TARGET_NOT_OWNED_AND_WORKING')
        if proposal['operation'] == 'CANCEL_ORPHAN_EXIT' and life.number(cv['remaining_quantity'], signed=True) > 0:
            raise FenceError('ORPHAN_GAINED_QUANTITY_REPLAN_REQUIRED')
        return True
    if action.get('type') != 'order' or not isinstance(action.get('orders'), list) or len(action['orders']) != 1:
        raise FenceError('ONE_EXACT_ORDER_REQUIRED')
    if view['bucket_issues']:
        raise FenceError('ACCOUNT_RECONCILIATION_REQUIRED_BEFORE_ORDER')
    o = action['orders'][0]
    q = life.number(o.get('s'), positive=True)
    if q != life.number(proposal['quantity'], positive=True):
        raise FenceError('WIRE_AND_RESERVED_QUANTITY_DIFFER')
    rows = exposure(state, now_ms=now_ms)['cards']
    if any(x['flat_with_residual_orders'] for x in rows):
        raise FenceError('RESIDUAL_EXITS_MUST_BE_RETIRED_BEFORE_ORDER')
    if leg == 'ENTRY':
        if b is not None:
            raise FenceError('REGISTERED_CARD_ENTRY_CANNOT_BE_REOPENED')
        if any(x['active_or_pending'] and life.number(x['excess_independent_capacity']) > 0 for x in rows):
            raise FenceError('SHARED_MARKET_INDEPENDENT_PAIR_NOT_ISOLATED')
        return True
    if leg not in EXITS or b is None or cv is None:
        raise FenceError('OWNED_EXIT_CARD_REQUIRED')
    remaining = life.number(cv['remaining_quantity'], signed=True)
    if remaining <= 0 or q > remaining:
        raise FenceError('EXIT_EXCEEDS_OWN_CONFIRMED_REMAINDER')
    if o.get('r') is not True or o.get('b') is not (b['side'] == 'SHORT'):
        raise FenceError('EXACT_REDUCING_EXIT_REQUIRED')
    if _open_for(b, snap, (leg,)):
        raise FenceError('PREVIOUS_SAME_LEG_NOT_TERMINAL')
    others = [x for x in rows if x['card_id'] != cid and x['active_or_pending']]
    if others:
        own = next(x for x in rows if x['card_id'] == cid)
        if life.number(own['outstanding_exit_quantity']) + q > remaining:
            raise FenceError('SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')
    return True


def confirm_cancel(state, request, snapshot, bindings, *, now_ms):
    """Exact terminal evidence only; a filled race is not a successful cancel."""
    current = {**state, 'bindings':bindings, 'evidence':dict(bindings=bindings,snapshot=snapshot)}
    bs, snap, view = _read(current, now_ms)
    p = request['proposal']; oid = p['old_oid']
    binding = next((b for b in bs if b['card_id'] == p['card_id']), None)
    if binding is None or oid not in binding['orders'][p['leg']]:
        raise FenceError('CANCEL_CONFIRMATION_OWNER_MISMATCH')
    if any(o['oid'] == oid for o in snap['open_orders']):
        raise FenceError('CANCEL_TARGET_STILL_WORKING')
    endings = [o for o in snap['terminal_orders'] if o['oid'] == oid]
    if len(endings) != 1:
        raise FenceError('CANCEL_TERMINAL_EVIDENCE_REQUIRED')
    ending = endings[0]
    if not p['observed_at_ms'] < ending['at_ms'] <= snap['at_ms']:
        raise FenceError('CANCEL_TERMINAL_TIME_CONFLICT')
    with localcontext() as ctx:
        ctx.prec = 80
        fills = {f['fill_id']:f for f in snap['fills'] if f['oid'] == oid}
        q = sum((life.number(f['quantity']) for f in fills.values()), Decimal(0))
    if (life.number(ending['filled_quantity']) != q
            or any(f['at_ms'] > ending['at_ms'] for f in fills.values())
            or ending['state'] == 'REJECTED' and q != 0):
        raise FenceError('CANCEL_FINAL_FILL_TOTAL_NOT_VERIFIED')
    card = next(x for x in view['cards'] if x['card_id'] == p['card_id'])
    return dict(version=VERSION, exact_order_terminal=True,
        terminal_state=ending['state'], cancellation_caused_terminal_state=False,
        own_remaining_quantity=card['remaining_quantity'],
        owner_requires_review=bool(card['issues']),
        other_cards_reassigned=False, native_oco_or_pre_cancel_isolation_proven=False)


def retire_obsolete_unsent(store, state, *, now_ms):
    """An unsent exit intention cannot later attach itself to a different trade.

    Only PREPARED + zero attempts + no nonce/reply may be retired automatically.
    OUTCOME_UNKNOWN is never cleared, even after a long delay or apparent closure.
    Fills and order ownership are retained unchanged in the original journal.
    """
    if not state.get('pending'):
        return state
    request = store.request(state['pending'])
    if (request['phase'] != 'PREPARED' or request['attempts'] != 0
            or request['nonce'] is not None or request['attempt_at_ms'] is not None
            or request['reply'] is not None):
        return state
    bs, snap, view = _read(state, now_ms)
    p = request['proposal']
    if snap['at_ms'] <= p['observed_at_ms']:
        return state
    b = next((x for x in bs if x['card_id'] == p['card_id']), None)
    card = next((x for x in view['cards'] if x['card_id'] == p['card_id']), None)
    if b is None or card is None or view['bucket_issues']:
        return state
    obsolete = False
    if p['action']['type'] == 'order' and p['leg'] in EXITS:
        obsolete = life.number(card['remaining_quantity'], signed=True) != life.number(p['quantity']) or bool(_open_for(b,snap,(p['leg'],)))
    elif p['operation'] in ('CANCEL_ORPHAN_EXIT', 'CANCEL_FOR_RESIZE'):
        obsolete = any(o['oid'] == p['old_oid'] for o in snap['terminal_orders'])
        remaining = life.number(card['remaining_quantity'], signed=True)
        if p['operation'] == 'CANCEL_ORPHAN_EXIT' and remaining > 0:
            obsolete = True
        elif p['operation'] == 'CANCEL_FOR_RESIZE':
            own = [o for _,o in _open_for(b,snap,(p['leg'],)) if o['oid'] == p['old_oid']]
            # A fresh fill can make the size correct, or finish the card while
            # this cancellation was only PREPARED. Replan rather than remove a
            # now-correct exit or leave stale resize intent blocking cleanup.
            if remaining <= 0 or any(life.number(o['quantity']) == remaining for o in own):
                obsolete = True
    if not obsolete:
        return state
    def update(conn, value):
        current = store.pending_record(conn, value)
        if current != request or value['evidence'] != state['evidence']:
            raise FenceError('UNSENT_EXIT_RETIREMENT_RELOAD_REQUIRED')
        current['phase'] = 'ABORTED_UNSENT'
        current['retired_reason'] = 'EXIT_OBSOLETE_AFTER_FRESH_OBSERVATION'
        current['retired_evidence_digest'] = life.digest(value['evidence'])
        value['pending'] = None
        return current
    return store.change(state['bucket'],state['revision'],'RETIRE_OBSOLETE_UNSENT_EXIT',now_ms,update)
