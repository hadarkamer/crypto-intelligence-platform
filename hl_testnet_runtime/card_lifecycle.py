"""Read-only card lifecycle reconciliation. No transport, signer, timer or orders.

Inputs are normalized evidence, NOT raw API replies. A future adapter must prove
history completeness and comparable snapshot times. This module never treats an
order acknowledgement or an empty position as proof of a particular card closing.
"""
from copy import deepcopy
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re

VERSION = 'card-lifecycle-observation-v1'
ROLES = {'long_account': 'LONG', 'short_account': 'SHORT'}
LEGS = ('ENTRY', 'TAKE_PROFIT', 'STOP')


class LifecycleError(ValueError):
    """Fixed codes only; do not interpolate private input."""


def encoded(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (ValueError, TypeError, RecursionError):
        raise LifecycleError('INVALID_RECORD') from None


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def shape(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields.split()):
        raise LifecycleError('UNEXPECTED_FIELDS')


def ident(value, pattern=r'[A-Za-z0-9_.:-]{1,100}'):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise LifecycleError('INVALID_IDENTIFIER')
    return value


def address(value):
    ident(value, r'0x[0-9a-fA-F]{40}')
    if int(value[2:], 16) == 0:
        raise LifecycleError('INVALID_ACCOUNT')
    return value.lower()


def number(value, *, positive=False, signed=False):
    if not isinstance(value, str) or len(value) > 80:
        raise LifecycleError('DECIMAL_STRING_REQUIRED')
    try:
        n = Decimal(value)
    except InvalidOperation:
        raise LifecycleError('INVALID_NUMBER') from None
    if (not n.is_finite() or abs(n) > Decimal('1e18')
            or len(n.as_tuple().digits) > 28 or n.as_tuple().exponent < -18
            or (positive and n <= 0) or (not signed and n < 0)):
        raise LifecycleError('INVALID_NUMBER')
    return n


def text(value):
    return format(value, 'f')


def moment(value):
    if type(value) is not int or not 0 < value < 10**15:
        raise LifecycleError('INVALID_TIME')
    return value


def binding_from_card(card, account, routes, orders):
    """Bind an existing immutable record to public routing and observed order IDs.

    This is NOT submission authority. Unsupported assets and archived reviews
    cannot silently turn into new positions. Existing records are not modified.
    """
    from .trade_cards import validate_card, checksum
    card = validate_card(card)
    if (card['state'] != 'RECORDED_ONLY' or card['prepared']['execution'] is None
            or card['record_kind'] not in ('received_alert', 'synthetic_test')):
        raise LifecycleError('CARD_NOT_ELIGIBLE_FOR_BINDING')
    role = card['account_role']
    if address(routes[role]['account']) != address(account):
        raise LifecycleError('ROUTING_MISMATCH')
    signal = card['prepared']['execution']
    binding = dict(card_id=card['card_id'], card_digest=checksum(card), account=address(account),
        role=role, symbol=signal['symbol'], side=signal['side'],
        planned_quantity=card['planning']['quantity'],
        prices={key: signal[key] for key in ('entry', 'stop', 'take_profit')},
        orders=deepcopy(orders), environment='testnet')
    validate_bindings([binding])
    return binding


def validate_bindings(bindings):
    if not isinstance(bindings, list) or not bindings:
        raise LifecycleError('BINDINGS_REQUIRED')
    ids, links, account_roles = set(), {}, {}
    for b in bindings:
        shape(b, 'card_id card_digest account role symbol side planned_quantity prices orders environment')
        cid = ident(b['card_id'], r'[0-9a-f]{64}')
        ident(b['card_digest'], r'[0-9a-f]{64}')
        if cid in ids:
            raise LifecycleError('DUPLICATE_CARD_ID')
        ids.add(cid)
        account = address(b['account'])
        if b['environment'] != 'testnet' or ROLES.get(b['role']) != b['side']:
            raise LifecycleError('TESTNET_FINAL_DIRECTION_REQUIRED')
        if account in account_roles and account_roles[account] != b['role']:
            raise LifecycleError('OPPOSITE_ROLES_REQUIRE_DIFFERENT_ACCOUNTS')
        account_roles[account] = b['role']
        ident(b['symbol'], r'[A-Z][A-Z0-9]{0,19}')
        number(b['planned_quantity'], positive=True)
        shape(b['prices'], 'entry stop take_profit')
        e, s, t = [number(b['prices'][key], positive=True) for key in ('entry', 'stop', 'take_profit')]
        if not (s < e < t if b['side'] == 'LONG' else t < e < s):
            raise LifecycleError('INVALID_PRICE_ORDER')
        shape(b['orders'], 'ENTRY TAKE_PROFIT STOP')
        for leg in LEGS:
            values = b['orders'][leg]
            if not isinstance(values, list) or (leg == 'ENTRY' and not values):
                raise LifecycleError('ENTRY_ID_REQUIRED')
            for oid in values:
                key = (account, ident(oid, r'[0-9]{1,30}'))
                if key in links:
                    raise LifecycleError('ORDER_ID_ALREADY_BOUND')
                links[key] = (cid, leg)
    return links


def validate_snapshot(snapshot):
    shape(snapshot, 'environment account symbol at_ms history_complete orders_complete position_quantity fills open_orders terminal_orders')
    if snapshot['environment'] != 'testnet':
        raise LifecycleError('TESTNET_ONLY')
    account, symbol = address(snapshot['account']), ident(snapshot['symbol'], r'[A-Z][A-Z0-9]{0,19}')
    at = moment(snapshot['at_ms'])
    number(snapshot['position_quantity'], signed=True)
    if any(type(snapshot[key]) is not bool for key in ('history_complete', 'orders_complete')):
        raise LifecycleError('EXPLICIT_COMPLETENESS_REQUIRED')
    seen_fills, seen_orders, seen_terminal = {}, set(), set()
    for collection in ('fills', 'open_orders', 'terminal_orders'):
        if not isinstance(snapshot[collection], list):
            raise LifecycleError('EVIDENCE_LIST_REQUIRED')
        for row in snapshot[collection]:
            extra = ('fill_id quantity price fee fee_token side at_ms' if collection == 'fills' else
                     'quantity price trigger_price side reduce_only state order_type' if collection == 'open_orders'
                     else 'state filled_quantity at_ms')
            shape(row, 'account symbol oid ' + extra)
            if address(row['account']) != account or row['symbol'] != symbol:
                raise LifecycleError('EVIDENCE_ACCOUNT_OR_SYMBOL_MISMATCH')
            ident(row['oid'], r'[0-9]{1,30}')
            if collection == 'fills':
                fid = ident(row['fill_id'])
                value = encoded(row)
                if fid in seen_fills and seen_fills[fid] != value:
                    raise LifecycleError('CONFLICTING_FILL_REPLAY')
                seen_fills[fid] = value
                number(row['quantity'], positive=True); number(row['price'], positive=True)
                number(row['fee'], signed=True); ident(row['fee_token'])
                if row['side'] not in ('B', 'A'):
                    raise LifecycleError('INVALID_FILL_SIDE')
            elif collection == 'open_orders':
                if row['oid'] in seen_orders:
                    raise LifecycleError('DUPLICATE_OPEN_ORDER')
                seen_orders.add(row['oid'])
                number(row['quantity'], positive=True); number(row['price'], positive=True)
                if row['trigger_price'] is not None: number(row['trigger_price'], positive=True)
                if (row['side'] not in ('B', 'A') or type(row['reduce_only']) is not bool
                        or row['state'] not in ('ACTIVE', 'WAITING_PARENT')
                        or row['order_type'] not in ('LIMIT', 'TP_LIMIT', 'SL_MARKET')):
                    raise LifecycleError('INVALID_OPEN_ORDER_EVIDENCE')
            else:
                if row['oid'] in seen_terminal or row['state'] not in ('FILLED', 'CANCELED', 'REJECTED'):
                    raise LifecycleError('INVALID_TERMINAL_ORDER')
                seen_terminal.add(row['oid']); number(row['filled_quantity'])
            if collection != 'open_orders' and moment(row['at_ms']) > at:
                raise LifecycleError('EVIDENCE_NEWER_THAN_SNAPSHOT')
    return account, symbol


def plain_tp_ids(bindings, account, symbol, values):
    """Explicit internal read model, never infer a plain TP from an order label.

    Default callers remain trigger-only. The exclusive protocol obtains these
    IDs from its immutable request/ownership records, never from the app.
    This read-only option grants no order or policy authority.
    """
    if not isinstance(values, (tuple, list, set, frozenset)):
        raise LifecycleError('EXPLICIT_PLAIN_TP_IDS_REQUIRED')
    ids = frozenset(ident(v, r'[0-9]{1,30}') for v in values)
    allowed = {oid for b in bindings if address(b['account']) == address(account)
               and b['symbol'] == symbol for oid in b['orders']['TAKE_PROFIT']}
    if not ids <= allowed:
        raise LifecycleError('PLAIN_TP_ID_NOT_OWNED')
    return ids


def review(bindings, snapshot, *, now_ms, max_age_ms=15000, plain_take_profit_oids=()):
    """Derive card states from comparable, complete evidence; never place/cancel.

    Gross card PnL is cash-flow based only once the card is verifiably flat and
    its entry/exit orders are all terminal. Account-average closedPnl is not
    allocated to individual cards. Funding stays unknown, never silently zero.
    """
    links = validate_bindings(bindings)
    account, symbol = validate_snapshot(snapshot)
    plain = plain_tp_ids(bindings, account, symbol, plain_take_profit_oids)
    moment(now_ms)
    if type(max_age_ms) is not int or not 0 < max_age_ms <= 60000:
        raise LifecycleError('INVALID_MAX_AGE')
    selected = sorted((b for b in bindings if address(b['account']) == account and b['symbol'] == symbol), key=lambda b: b['card_id'])
    if not selected:
        raise LifecycleError('BUCKET_NOT_REGISTERED')
    bucket_links = {oid: (b['card_id'], leg) for b in selected for leg in LEGS for oid in b['orders'][leg]}
    issues = set()
    if not 0 <= now_ms - snapshot['at_ms'] <= max_age_ms: issues.add('STALE_OR_FUTURE_SNAPSHOT')
    if not snapshot['history_complete']: issues.add('FILL_HISTORY_INCOMPLETE')
    if not snapshot['orders_complete']: issues.add('ORDER_INVENTORY_INCOMPLETE')
    fills = {f['fill_id']: f for f in snapshot['fills']}.values()
    opens = {o['oid']: o for o in snapshot['open_orders']}
    terminal = {o['oid']: o for o in snapshot['terminal_orders']}
    if set(opens) & set(terminal): issues.add('ORDER_OPEN_AND_TERMINAL')
    if (set(opens) | set(terminal) | {f['oid'] for f in fills}) - set(bucket_links):
        issues.add('UNASSIGNED_EXCHANGE_ACTIVITY')
    views = []
    with localcontext() as ctx:
        ctx.prec = 80
        total_remaining = Decimal(0)
        for b in selected:
            local = set(); quantities = {leg: Decimal(0) for leg in LEGS}
            cash = {leg: Decimal(0) for leg in LEGS}; fees = {}; filled_by_oid = {}
            own = {oid: leg for leg in LEGS for oid in b['orders'][leg]}
            entry_side = 'B' if b['side'] == 'LONG' else 'A'
            for f in fills:
                if f['oid'] not in own: continue
                leg = own[f['oid']]; q = number(f['quantity']); px = number(f['price'])
                if f['side'] != (entry_side if leg == 'ENTRY' else ('A' if entry_side == 'B' else 'B')):
                    local.add('FILL_SIDE_MISMATCH')
                quantities[leg] += q; cash[leg] += q * px
                fees[f['fee_token']] = fees.get(f['fee_token'], Decimal(0)) + number(f['fee'], signed=True)
                filled_by_oid[f['oid']] = filled_by_oid.get(f['oid'], Decimal(0)) + q
            entered = quantities['ENTRY']; exited = quantities['TAKE_PROFIT'] + quantities['STOP']
            remaining = entered - exited
            if entered > number(b['planned_quantity']): local.add('ENTRY_EXCEEDS_PLAN')
            if remaining < 0: local.add('EXIT_EXCEEDS_CARD_QUANTITY')
            total_remaining += remaining * (1 if b['side'] == 'LONG' else -1)
            if quantities['TAKE_PROFIT'] > 0 and quantities['STOP'] > 0: local.add('BOTH_EXIT_LEGS_FILLED_REVIEW')
            for oid, t in terminal.items():
                if oid in own and number(t['filled_quantity']) != filled_by_oid.get(oid, Decimal(0)):
                    local.add('TERMINAL_FILL_TOTAL_MISMATCH')
            coverage = {leg: Decimal(0) for leg in ('TAKE_PROFIT', 'STOP')}
            pending_entry = Decimal(0)
            own_opens = sorted(set(own) & set(opens))
            for oid in own_opens:
                o = opens[oid]; leg = own[oid]
                side = entry_side if leg == 'ENTRY' else ('A' if entry_side == 'B' else 'B')
                valid = o['side'] == side and o['reduce_only'] == (leg != 'ENTRY')
                if leg == 'ENTRY':
                    pending_entry += number(o['quantity'])
                    valid = valid and o['order_type'] == 'LIMIT' and o['trigger_price'] is None and number(o['price']) == number(b['prices']['entry'])
                else:
                    price = b['prices']['stop' if leg == 'STOP' else 'take_profit']
                    if leg == 'TAKE_PROFIT' and oid in plain:
                        valid = valid and o['order_type'] == 'LIMIT' and o['trigger_price'] is None
                    else:
                        valid = valid and o['order_type'] == ('SL_MARKET' if leg == 'STOP' else 'TP_LIMIT') and o['trigger_price'] is not None and number(o['trigger_price']) == number(price)
                    if leg == 'TAKE_PROFIT': valid = valid and number(o['price']) == number(price)
                    if valid and o['state'] == 'ACTIVE': coverage[leg] += number(o['quantity'])
                if not valid: local.add('ORDER_TERMS_MISMATCH')
            if entered + pending_entry > number(b['planned_quantity']): local.add('ENTRY_PENDING_EXCEEDS_PLAN')
            if set(own) - set(opens) - set(terminal): local.add('ORDER_STATUS_UNRESOLVED')
            all_terminal = all(oid in terminal for oid in own)
            flat = entered > 0 and remaining == 0
            if remaining > 0:
                for leg in coverage:
                    if coverage[leg] < remaining: local.add(leg + '_COVERAGE_MISSING')
                    if coverage[leg] > remaining: local.add(leg + '_EXCEEDS_CARD_REMAINDER')
            state = ('PARTIALLY_CLOSED' if remaining > 0 and exited > 0 else
                     'PARTIALLY_OPEN' if remaining > 0 and entered < number(b['planned_quantity']) else
                     'OPEN' if remaining > 0 else 'FLAT_AWAITING_FINALITY' if flat else 'WAITING_ENTRY')
            leftover = [oid for oid in own_opens if own[oid] != 'ENTRY'] if remaining <= 0 else []
            if flat and own_opens: local.add('FLAT_WITH_WORKING_ORDERS')
            if all_terminal and not own_opens and remaining == 0:
                state = 'CLOSED' if entered > 0 else 'CANCELED_WITHOUT_FILL'
            if any(token != 'USDC' for token in fees): local.add('NON_USDC_FEE_REQUIRES_CONVERSION')
            gross = (cash['TAKE_PROFIT'] + cash['STOP'] - cash['ENTRY']) * (1 if b['side'] == 'LONG' else -1)
            views.append(dict(card_id=b['card_id'], role=b['role'], state=state,
                entry_quantity=text(entered), exit_quantity=text(exited), remaining_quantity=text(remaining),
                issues=sorted(local), stop_quantity_observed=text(coverage['STOP']),
                take_profit_quantity_observed=text(coverage['TAKE_PROFIT']), leftover_order_ids=leftover,
                fees_by_token={k:text(v) for k,v in sorted(fees.items())},
                gross_pnl_usdc=text(gross) if state == 'CLOSED' else None,
                net_before_funding_usdc=text(gross-fees.get('USDC',Decimal(0))) if state == 'CLOSED' else None,
                funding_usdc=None, final_net_usdc=None, closure_verified=False))
        if number(snapshot['position_quantity'], signed=True) != total_remaining:
            issues.add('POSITION_DOES_NOT_MATCH_CARDS')
    for v in views:
        v['closure_verified'] = v['state'] == 'CLOSED' and not issues and not v['issues']
        if not v['closure_verified']:
            v['gross_pnl_usdc'] = v['net_before_funding_usdc'] = None
    return dict(version=VERSION, environment='testnet', evidence_at_ms=snapshot['at_ms'],
        account=account, symbol=symbol, bucket_issues=sorted(issues), cards=views,
        needs_review=bool(issues or any(v['issues'] for v in views)),
        order_requests_sent=0, dispatch_enabled=False, app_delivery_enabled=False)
