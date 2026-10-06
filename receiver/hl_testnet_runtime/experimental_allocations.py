"""Pure rehearsal ledger for exact-owned orders in a netted Testnet market.

An allocation reconciles accounting; it does NOT isolate exchange reduce-only
orders. In particular two sibling exits may consume another occurrence before
OCO cancellation. Every result deliberately keeps shared-market dispatch off.
No requests, exchange adapters, signer, database or environment reads exist.
"""
from copy import deepcopy
from decimal import Decimal
import re

VERSION = 'experimental-allocation-software-v1'
TERMINAL = frozenset(('FILLED', 'CANCELED', 'REJECTED'))


class AllocationError(ValueError):
    pass


def _number(value, *, signed=False, positive=False):
    if not isinstance(value, str) or len(value) > 80:
        raise AllocationError('DECIMAL_STRING_REQUIRED')
    try:
        number = Decimal(value)
    except Exception:
        raise AllocationError('FINITE_DECIMAL_REQUIRED') from None
    if not number.is_finite() or (not signed and number < 0) or positive and number <= 0:
        raise AllocationError('FINITE_DECIMAL_REQUIRED')
    return number


def _shape(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields.split()):
        raise AllocationError('EXACT_EVIDENCE_SHAPE_REQUIRED')


def _identity(value, pattern):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise AllocationError('EXACT_IDENTITY_REQUIRED')


def _oid(value):
    _identity(value, r'[1-9][0-9]{0,19}')
    if int(value) >= 2**64:
        raise AllocationError('EXACT_ORDER_ID_REQUIRED')


def _empty():
    return dict(version=VERSION, status='QUARANTINED', allocations={}, diagnostics=[],
        dispatch_enabled=False, shared_market_dispatch_enabled=False,
        signed_expected_position=None)


def assess(bindings, snapshot, *, now_ms, previous=None):
    """Reconcile a complete account/symbol snapshot against persisted ownership.

    Caller obtains authoritative complete evidence and durably CAS-persists the
    result before acting. `previous` detects history removal and changes across
    restart; absence is appropriate only for a first observation of the ledger.
    """
    result = _empty()
    try:
        result.update(_assess(bindings, snapshot, now_ms=now_ms, previous=previous))
    except (AllocationError, KeyError, TypeError, ValueError) as exc:
        result['diagnostics'] = [str(exc) if isinstance(exc, AllocationError) else 'MALFORMED_EVIDENCE']
    return result


def _assess(bindings, snapshot, *, now_ms, previous):
    _shape(snapshot, 'environment account account_role symbol at_ms history_complete orders_complete '
                     'position_complete position_quantity orders fills')
    if (snapshot['environment'] != 'testnet'
            or snapshot['account_role'] not in ('long_account','short_account')
            or any(snapshot[k] is not True for k in ('history_complete','orders_complete','position_complete'))
            or type(now_ms) is not int or type(snapshot['at_ms']) is not int
            or snapshot['at_ms'] < 0 or not 0 <= now_ms-snapshot['at_ms'] <= 15000):
        raise AllocationError('COMPLETE_FRESH_TESTNET_EVIDENCE_REQUIRED')
    _identity(snapshot['account'], r'0x[0-9a-f]{40}')
    _identity(snapshot['symbol'], r'[A-Z0-9]{1,20}')
    actual_position = _number(snapshot['position_quantity'], signed=True)
    if not isinstance(bindings, list) or not isinstance(snapshot['orders'], list) or not isinstance(snapshot['fills'], list):
        raise AllocationError('EXACT_ORDER_AND_FILL_LISTS_REQUIRED')
    position_side = 'LONG' if snapshot['account_role'] == 'long_account' else 'SHORT'
    entry_side = 'BUY' if position_side == 'LONG' else 'SELL'
    sign = Decimal(1) if position_side == 'LONG' else Decimal(-1)
    owners, cards = {}, {}
    for binding in bindings:
        _shape(binding, 'occurrence_id account account_role symbol side orders')
        cid = binding['occurrence_id']
        _identity(cid, r'[0-9a-f]{64}')
        if cid in cards or any(binding[k] != snapshot[k] for k in ('account','account_role','symbol')):
            raise AllocationError('EXACT_ACCOUNT_AND_OCCURRENCE_REQUIRED')
        if binding['side'] != position_side or not isinstance(binding['orders'], list):
            raise AllocationError('ACCOUNT_DIRECTION_FENCE')
        cards[cid] = dict(entry=Decimal(0), exit=Decimal(0), orders=[], changes={})
        entry_count = 0
        for order in binding['orders']:
            _shape(order, 'order_id kind quantity reduce_only')
            oid = order['order_id']; _oid(oid)
            if oid in owners:
                raise AllocationError('ORDER_HAS_MULTIPLE_OWNERS')
            if order['kind'] not in ('ENTRY','STOP','TAKE_PROFIT'):
                raise AllocationError('KNOWN_ORDER_KIND_REQUIRED')
            _number(order['quantity'], positive=True)
            if order['reduce_only'] is not (order['kind'] != 'ENTRY'):
                raise AllocationError('EXACT_REDUCE_ONLY_REQUIRED')
            entry_count += order['kind'] == 'ENTRY'
            owners[oid] = (cid, order)
            cards[cid]['orders'].append(oid)
        if entry_count != 1:
            raise AllocationError('ONE_OWNED_ENTRY_ORDER_REQUIRED')
    observed_orders = {}
    for order in snapshot['orders']:
        _shape(order, 'order_id status quantity filled_quantity reduce_only side')
        oid = order['order_id']; _oid(oid)
        if oid in observed_orders or oid not in owners:
            raise AllocationError('EXTERNAL_OR_DUPLICATE_ORDER_QUARANTINE')
        cid, binding = owners[oid]
        if (order['status'] not in TERMINAL | {'OPEN'}
                or order['quantity'] != binding['quantity']
                or order['reduce_only'] is not binding['reduce_only']
                or order['side'] != (entry_side if binding['kind'] == 'ENTRY' else
                                     'SELL' if entry_side == 'BUY' else 'BUY')):
            raise AllocationError('OWN_ORDER_TERMS_CHANGED')
        quantity = _number(order['quantity'], positive=True)
        filled = _number(order['filled_quantity'])
        if (filled > quantity or order['status'] == 'FILLED' and filled != quantity
                or order['status'] == 'REJECTED' and filled != 0
                or order['status'] == 'OPEN' and filled == quantity):
            raise AllocationError('ORDER_QUANTITY_NOT_RECONCILED')
        observed_orders[oid] = deepcopy(order)
    if set(observed_orders) != set(owners):
        raise AllocationError('OWN_ORDER_MISSING_FROM_COMPLETE_HISTORY')
    fill_totals = {oid: Decimal(0) for oid in owners}
    observed_fills = {}
    for fill in snapshot['fills']:
        _shape(fill, 'fill_id order_id quantity price at_ms')
        _identity(fill['fill_id'], r'[A-Za-z0-9_.:-]{1,100}')
        oid = fill['order_id']; _oid(oid)
        if fill['fill_id'] in observed_fills or oid not in owners:
            raise AllocationError('EXTERNAL_OR_DUPLICATE_FILL_QUARANTINE')
        if type(fill['at_ms']) is not int or not 0 <= fill['at_ms'] <= snapshot['at_ms']:
            raise AllocationError('EXACT_FILL_TIME_REQUIRED')
        quantity = _number(fill['quantity'], positive=True); _number(fill['price'], positive=True)
        fill_totals[oid] += quantity
        cid, binding = owners[oid]
        cards[cid]['entry' if binding['kind'] == 'ENTRY' else 'exit'] += quantity
        cards[cid]['changes'][fill['at_ms']] = cards[cid]['changes'].get(fill['at_ms'], Decimal(0)) + (
            quantity if binding['kind'] == 'ENTRY' else -quantity)
        observed_fills[fill['fill_id']] = deepcopy(fill)
    if any(fill_totals[oid] != _number(row['filled_quantity']) for oid, row in observed_orders.items()):
        raise AllocationError('INCOMPLETE_OWN_FILL_HISTORY')
    if previous is not None:
        if previous.get('status') != 'RECONCILED' or previous.get('version') != VERSION:
            raise AllocationError('RECONCILED_PREDECESSOR_REQUIRED')
        old = previous['evidence']
        if any(old[k] != snapshot[k] for k in ('environment','account','account_role','symbol')):
            raise AllocationError('ACCOUNT_EVIDENCE_CHANGED')
        if snapshot['at_ms'] < old['at_ms']:
            raise AllocationError('EVIDENCE_REGRESSION')
        for row in old['fills']:
            if observed_fills.get(row['fill_id']) != row:
                raise AllocationError('PREVIOUS_FILL_REMOVED_OR_CHANGED')
        new_bindings = {b['occurrence_id']: b for b in bindings}
        for old_binding in previous['bindings']:
            new_binding = new_bindings.get(old_binding['occurrence_id'])
            if (new_binding is None or any(new_binding[k] != old_binding[k] for k in old_binding if k != 'orders')
                    or any(order not in new_binding['orders'] for order in old_binding['orders'])):
                raise AllocationError('PREVIOUS_OWNERSHIP_REMOVED_OR_CHANGED')
        for row in old['orders']:
            current = observed_orders.get(row['order_id'])
            if current is None or row['status'] in TERMINAL and current != row:
                raise AllocationError('FINAL_ORDER_REMOVED_OR_CHANGED')
        if snapshot['at_ms'] == old['at_ms'] and snapshot != old:
            raise AllocationError('SAME_TIME_EVIDENCE_CONFLICT')
    allocations, expected = {}, Decimal(0)
    for cid, card in cards.items():
        if card['exit'] > card['entry']:
            raise AllocationError('EXIT_CONSUMED_ANOTHER_ALLOCATION')
        cumulative = Decimal(0)
        for moment in sorted(card['changes']):
            cumulative += card['changes'][moment]
            if cumulative < 0:
                raise AllocationError('EXIT_PRECEDES_OWN_ENTRY_ALLOCATION')
        remaining = card['entry']-card['exit']
        if remaining < 0:
            raise AllocationError('EXIT_CONSUMED_ANOTHER_ALLOCATION')
        expected += remaining*sign
        working_exits = {kind: [] for kind in ('STOP','TAKE_PROFIT')}
        final = remaining == 0
        for oid in card['orders']:
            _, binding = owners[oid]
            observed = observed_orders[oid]
            if observed['status'] == 'OPEN':
                final = False
                if binding['kind'] != 'ENTRY':
                    working_exits[binding['kind']].append(_number(observed['quantity'])-fill_totals[oid])
        protected = bool(remaining) and all(working_exits[k] == [remaining] for k in working_exits)
        allocations[cid] = dict(entered=format(card['entry'],'f'), exited=format(card['exit'],'f'),
            remaining=format(remaining,'f'), all_orders_final=final, quantity_coverage_observed=protected)
    if expected != actual_position:
        raise AllocationError('POSITION_DOES_NOT_EQUAL_OWNED_ALLOCATIONS')
    return dict(status='RECONCILED', allocations=allocations, diagnostics=[],
        signed_expected_position=format(expected,'f'), evidence=deepcopy(snapshot), bindings=deepcopy(bindings))


def unsigned_reduction(assessment, occurrence_id, quantity):
    """Return a capped rehearsal reduction; never permit a pooled venue send."""
    if assessment.get('status') != 'RECONCILED' or assessment.get('version') != VERSION:
        raise AllocationError('RECONCILED_ALLOCATION_REQUIRED')
    amount = _number(quantity, positive=True)
    card = assessment['allocations'].get(occurrence_id)
    if card is None or amount > _number(card['remaining']):
        raise AllocationError('REDUCTION_EXCEEDS_OWN_REMAINING')
    return dict(occurrence_id=occurrence_id, quantity=quantity, reduce_only=True,
                dispatch_enabled=False, shared_market_dispatch_enabled=False)
