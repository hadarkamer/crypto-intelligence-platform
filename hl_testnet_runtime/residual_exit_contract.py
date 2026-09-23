"""Bind every residual-cleanup action to the immutable bot-owned order contract.

Called under the EXISTING reservation and attempt locks, before nonce/key use.
No I/O, app input, alternate sender, policy selection or native-OCO claim. This
closes cross-card/cross-asset and stale-intent paths; it cannot cancel an order
before the exchange receives a cancellation. Shared-market release stays locked.
"""
from . import card_lifecycle as life, filled_quantity_exits as selected
from . import residual_exit_fence as fence

DISPATCH_VERSION = 'connected-filled-quantity-dispatch-v1'
CANCEL_LEGS = {
    'CANCEL_ORPHAN_EXIT': ('STOP', 'TAKE_PROFIT'),
    'CANCEL_FOR_RESIZE': ('STOP', 'TAKE_PROFIT'),
    'CANCEL_ENTRY_AFTER_EXIT': ('ENTRY',),
    'CANCEL_UNFILLED_HALF_THRESHOLD': ('ENTRY',),
}
HARD_QUANTITY_ISSUES = {
    'FILL_SIDE_MISMATCH', 'TERMINAL_FILL_TOTAL_MISMATCH',
    'EXIT_EXCEEDS_CARD_QUANTITY', 'BOTH_EXIT_LEGS_FILLED_REVIEW',
    'ORDER_TERMS_MISMATCH', 'ENTRY_EXCEEDS_PLAN',
    'ENTRY_PENDING_EXCEEDS_PLAN', 'ORDER_STATUS_UNRESOLVED',
}


class ContractError(life.LifecycleError):
    """Fixed local codes; never include raw orders or configuration."""


def validate_wire_proposal(state, proposal, *, now_ms):
    """Reconstruct the ONLY permitted payload; labels never overrule wire data.

    Policy checks and the independent-exit capacity fence remain separate and
    mandatory. This check adds identity/terms/incident validation, not approval.
    It is intentionally applied both at reserve and immediately before begin.
    """
    try:
        return _validate(state, proposal, now_ms)
    except ContractError:
        raise
    except (KeyError, IndexError, TypeError, ValueError, AttributeError):
        raise ContractError('RESIDUAL_WIRE_CONTRACT_INVALID') from None


def _validate(state, p, now_ms):
    bs, snap, view = fence._read(state, now_ms)
    cid = p['card_id']; leg = p['leg']; op = p['operation']
    original = state['originals'][cid]
    routes = {p['role']: dict(account=state['account'])}
    draft = selected.validate_draft(original['card'], original['draft'], routes)
    if (draft['card_id'] != cid or draft['account'] != state['account']
            or draft['symbol'] != state['symbol'] or draft['role'] != p['role']
            or p['account'] != state['account'] or p['symbol'] != state['symbol']
            or p['basis'] != life.digest(state['evidence'])):
        raise ContractError('RESIDUAL_WIRE_OWNER_MISMATCH')
    binding = next((b for b in bs if b['card_id'] == cid), None)
    owner = next((v for v in view['cards'] if v['card_id'] == cid), None)
    action = p['action']; asset = draft['entry_action']['orders'][0]['a']
    if action['type'] == 'cancel':
        life.shape(action, 'type cancels')
        if (not isinstance(action['cancels'], list) or len(action['cancels']) != 1
                or op not in CANCEL_LEGS or leg not in CANCEL_LEGS[op] or binding is None):
            raise ContractError('EXACT_CLEANUP_OPERATION_REQUIRED')
        item = action['cancels'][0]
        life.shape(item, 'a o')
        if type(item['a']) is not int or item['a'] != asset:
            raise ContractError('CANCEL_ASSET_DIFFERS_FROM_OWN_CARD')
        if (type(item['o']) is not int or not 0 < item['o'] < 2**64
                or str(item['o']) != p['old_oid'] or p['old_oid'] not in binding['orders'][leg]):
            raise ContractError('CANCEL_ORDER_DIFFERS_FROM_OWN_CARD')
        own_open = [o for o in snap['open_orders'] if o['oid'] == p['old_oid']]
        if len(own_open) != 1 or own_open[0]['state'] != 'ACTIVE':
            raise ContractError('EXACT_ACTIVE_CLEANUP_TARGET_REQUIRED')
        entered = life.number(owner['entry_quantity'])
        exited = life.number(owner['exit_quantity'])
        remaining = life.number(owner['remaining_quantity'], signed=True)
        if op == 'CANCEL_ENTRY_AFTER_EXIT' and (entered <= 0 or exited <= 0):
            raise ContractError('CLEANUP_REQUIRES_CONFIRMED_OWN_EXIT')
        if op == 'CANCEL_UNFILLED_HALF_THRESHOLD' and (entered != 0 or exited != 0):
            raise ContractError('HALF_THRESHOLD_CANNOT_CANCEL_FILLED_ENTRY')
        if op == 'CANCEL_ORPHAN_EXIT' and remaining > 0:
            raise ContractError('LIVE_CARD_EXIT_IS_NOT_AN_ORPHAN')
        if op == 'CANCEL_FOR_RESIZE' and (remaining <= 0
                or life.number(own_open[0]['quantity']) == remaining):
            raise ContractError('RESIZE_REASON_NOT_PRESENT')
        return True
    if action['type'] != 'order':
        raise ContractError('UNSUPPORTED_RESIDUAL_ACTION')
    # Recording an overclose incident must not silently authorize another entry.
    # Exact cancellation above remains possible without relabeling the incident.
    if view['bucket_issues'] or any(set(v['issues']) & HARD_QUANTITY_ISSUES for v in view['cards']):
        raise ContractError('QUANTITY_INCIDENT_REQUIRES_RECONCILIATION')
    # A previously opened card may look flat while its original entry still
    # rests. Retire that entry before admitting any new order in this market.
    for b in bs:
        v = next(v for v in view['cards'] if v['card_id'] == b['card_id'])
        if (life.number(v['entry_quantity']) > 0
                and life.number(v['remaining_quantity'], signed=True) <= 0
                and any(o['oid'] in b['orders']['ENTRY'] for o in snap['open_orders'])):
            raise ContractError('CLOSED_CARD_ENTRY_REMAINDER_NOT_RETIRED')
    life.shape(action, 'type orders grouping')
    if not isinstance(action['orders'], list) or len(action['orders']) != 1:
        raise ContractError('EXACT_SINGLE_ORDER_REQUIRED')
    order = action['orders'][0]
    life.shape(order, 'a b p s r t c')
    if (type(order['a']) is not int or order['a'] != asset
            or type(order['b']) is not bool or type(order['r']) is not bool):
        raise ContractError('WIRE_ASSET_OR_BOOLEAN_FIELDS_INVALID')
    q = life.number(p['quantity'], positive=True)
    if life.number(order['s'], positive=True) != q:
        raise ContractError('WIRE_QUANTITY_DIFFERS_FROM_RESERVATION')
    if leg == 'ENTRY':
        if op != 'ENTRY' or binding is not None or action != draft['entry_action']:
            raise ContractError('ENTRY_WIRE_DIFFERS_FROM_IMMUTABLE_DRAFT')
        return True
    if leg not in fence.EXITS or op != 'CREATE_EXIT' or binding is None:
        raise ContractError('EXACT_OWNED_EXIT_CREATION_REQUIRED')
    sequence = p['sequence']
    if type(sequence) is not int or sequence <= 0 or p['version'] != DISPATCH_VERSION:
        raise ContractError('EXIT_REQUEST_IDENTITY_REQUIRED')
    price = binding['prices']['stop' if leg == 'STOP' else 'take_profit']
    cloid = '0x' + life.digest([DISPATCH_VERSION, state['bucket'], cid, leg, sequence])[:32]
    trigger = order['t']['trigger']
    if type(trigger['isMarket']) is not bool:
        raise ContractError('EXIT_TRIGGER_BOOLEAN_REQUIRED')
    expected = dict(type='order', grouping='na', orders=[dict(
        a=asset, b=binding['side']=='SHORT', p=price, s=p['quantity'], r=True,
        t=dict(trigger=dict(isMarket=leg=='STOP', triggerPx=price,
                            tpsl='sl' if leg=='STOP' else 'tp')), c=cloid)])
    if action != expected:
        raise ContractError('EXIT_WIRE_DIFFERS_FROM_OWN_CARD_CONTRACT')
    return True
