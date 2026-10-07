"""Executable exit capacity in the experimental netted-account ledger.

Order ownership proves accounting, not per-trade OCO. Two independently
executable full-size exits reserve twice their owner's quantity. This module
exposes that distinction and prevents additional unsafe work in an imported
shared lane. It deliberately does not authorize concurrent same-symbol entry.
No network, exchange capabilities, policy changes, or synthetic TP are added.
"""
from decimal import Decimal

from . import card_lifecycle as life

VERSION = 'experimental-shared-market-safety-v1'
FINAL = frozenset(('CLOSED', 'CANCELED_WITHOUT_FILL'))
SETTLED = frozenset(('OBSERVED', 'ABORTED_UNSENT'))
ENTRY_BLOCK = 'SHARED_MARKET_INDEPENDENT_EXITS_NOT_ISOLATED'


class SharedMarketError(ValueError):
    pass


def _number(value):
    return life.number(value, positive=True)


def _sum(rows):
    return sum((_number(row['quantity']) for row in rows), Decimal(0))


def _order(action):
    if action['type'] == 'order':
        if len(action['orders']) != 1 or action.get('grouping') != 'na':
            raise SharedMarketError('UNPROVEN_SHARED_ORDER_GROUPING')
        return action['orders'][0]
    if action['type'] == 'batchModify':
        if len(action['modifies']) != 1:
            raise SharedMarketError('EXACT_SHARED_MODIFICATION_REQUIRED')
        return action['modifies'][0]['order']
    raise SharedMarketError('EXACT_SHARED_ORDER_ACTION_REQUIRED')


def assess(state):
    """Describe current ownership and worst-case independently executable size.

    A pending modification conservatively counts the prior live order AND the
    attempted replacement. A pending cancellation does not release capacity.
    Source prices, planned quantity, aggregate position and ACKs never stand in
    for confirmed per-order fills. Call only on the verified durable state.
    """
    requests = {}
    for request in state['requests'].values():
        requests.setdefault(request['proposal']['card_id'], []).append(request)
    lanes = {}
    owners = set()
    fill_owners = set()
    for cid, trade in sorted(state['trades'].items()):
        if cid != trade['cid']:
            raise SharedMarketError('EXACT_SHARED_TRADE_IDENTITY_REQUIRED')
        entered, exited = (_sum(trade[key].values()) for key in ('entry_fills', 'exit_fills'))
        for kind in ('entry_fills', 'exit_fills'):
            for fid, fill in trade[kind].items():
                identity = (trade['account'], fid)
                if identity in fill_owners or fid != fill['fill_id']:
                    raise SharedMarketError('FILL_HAS_MULTIPLE_SHARED_OWNERS')
                fill_owners.add(identity)
        remaining = entered - exited
        if remaining < 0:
            raise SharedMarketError('EXIT_EXCEEDED_OWNED_QUANTITY_PEER_RISK')
        capacity = Decimal(0)
        open_ids = []
        for oid, row in trade['orders'].items():
            identity = (trade['account'], oid)
            if oid != row['oid'] or oid not in trade['order_legs'] or identity in owners:
                raise SharedMarketError('EXACT_SHARED_ORDER_OWNERSHIP_REQUIRED')
            owners.add(identity)
            if row['status'] != 'OPEN':
                continue
            open_ids.append(oid)
            if trade['order_legs'][oid] != 'ENTRY':
                outstanding = _number(row['wire_order']['s']) - _sum(row['fills'])
                if outstanding <= 0 or row['wire_order']['r'] is not True:
                    raise SharedMarketError('EXACT_SHARED_REDUCING_CAPACITY_REQUIRED')
                capacity += outstanding
        unresolved = []
        for request in requests.get(cid, []):
            if request['phase'] in SETTLED:
                continue
            unresolved.append(request['request_id'])
            proposal = request['proposal']
            if proposal['leg'] != 'ENTRY' and proposal['action']['type'] != 'cancel':
                capacity += _number(_order(proposal['action'])['s'])
        active = trade['phase'] not in FINAL or remaining > 0 or bool(open_ids or unresolved)
        key = life.digest([trade['account'], trade['symbol']])
        lane = lanes.setdefault(key, dict(account=trade['account'], symbol=trade['symbol'],
                                         trades=[], active_occurrences=0))
        lane['active_occurrences'] += bool(active)
        lane['trades'].append(dict(occurrence_id=cid, active=active,
            remaining_quantity=life.text(remaining),
            independent_exit_capacity=life.text(capacity),
            excess_independent_capacity=life.text(max(Decimal(0), capacity - remaining)),
            open_order_ids=sorted(open_ids), unresolved_request_ids=sorted(unresolved)))
    for lane in lanes.values():
        lane['shared'] = lane['active_occurrences'] > 1
        lane['capacity_safe_for_arbitrary_fills'] = all(
            Decimal(row['excess_independent_capacity']) == 0 for row in lane['trades'])
        lane['new_shared_entry_blocked_reason'] = ENTRY_BLOCK if lane['active_occurrences'] else None
    return dict(version=VERSION, native_per_trade_oco_verified=False,
                shared_symbol_parallel_enabled=False, lanes=lanes)


def proposal_reason(state, proposal):
    """Return a specific shared-lane barrier; None adds no new restriction.

    Existing checks still establish exact ownership, price, freshness, budget
    and account. This additional test protects pooled peers from over-reduction.
    Cancellation is permitted because it can only remove executable capacity.
    A modification cannot prove a race-safe new quantity from an old snapshot.
    """
    key = life.digest([proposal['account'], proposal['symbol']])
    lane = assess(state)['lanes'].get(key)
    if lane is None:
        return None
    cid = proposal['card_id']
    peers = [row for row in lane['trades'] if row['occurrence_id'] != cid and row['active']]
    if not peers:
        return None
    if proposal['operation'] == 'ENTRY':
        return ENTRY_BLOCK
    trade = state['trades'].get(cid)
    if trade is None or trade['account'] != proposal['account'] or trade['symbol'] != proposal['symbol']:
        return 'EXACT_SHARED_EXIT_OWNER_REQUIRED'
    action = proposal['action']
    if action['type'] == 'cancel':
        cancels = action.get('cancels', [])
        oid = str(cancels[0].get('o')) if len(cancels) == 1 else None
        if (oid not in trade['orders'] or trade['orders'][oid]['status'] != 'OPEN'
                or trade['order_legs'][oid] != proposal['leg']
                or cancels[0].get('a') != trade['asset']['index']):
            return 'EXACT_SHARED_CANCEL_OWNER_REQUIRED'
        return None
    own = next(row for row in lane['trades'] if row['occurrence_id'] == cid)
    if own['unresolved_request_ids']:
        return 'SHARED_EXIT_UNCERTAINTY_REQUIRES_RECONCILIATION'
    if action['type'] == 'batchModify':
        return 'SHARED_EXIT_MODIFICATION_FILL_RACE_UNPROVEN'
    order = _order(action)
    if (proposal['leg'] not in ('STOP', 'TAKE_PROFIT') or order['r'] is not True
            or order['b'] is not (trade['side'] == 'SHORT')
            or order['a'] != trade['asset']['index']):
        return 'EXACT_SHARED_REDUCING_OWNER_REQUIRED'
    quantity = _number(order['s'])
    if quantity != _number(proposal['quantity']):
        return 'SHARED_WIRE_AND_RESERVED_QUANTITY_DIFFER'
    if Decimal(own['independent_exit_capacity']) + quantity > Decimal(own['remaining_quantity']):
        return 'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED'
    return None
