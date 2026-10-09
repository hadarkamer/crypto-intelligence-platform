"""Frozen g65 source-clock rehearsal; never submits or asserts a venue fill.

Reference-minute entry/cancel ordering and fill-minute CLOSE promotion match the
existing source. A source outcome requests reconciliation, not an exchange exit.
Actual filled ownership and venue protection remain separate caller evidence.
"""
from copy import deepcopy
from decimal import Decimal

import experimental_execution_contract as contract
from . import card_lifecycle as life
from . import r2732_conditional_stop as common

VERSION = 'sol-g65-causal-stop-v1'
SOURCE = 'HYPERLIQUID_SOL_PERPETUAL_TRADE_1M'
MINUTE_MS = 60000


class ConditionalStopError(life.LifecycleError):
    pass


def initialize_from_contract(card_id, message):
    value = contract.validate(message)
    life.ident(card_id, r'[0-9a-f]{64}')
    if value['family'] != 'sol_g65':
        raise ConditionalStopError('G65_CONTRACT_REQUIRED')
    reference = contract.moment_ms(value['source_at'])
    return dict(version=VERSION, card_id=card_id, source_event_id=value['occurrence_id'],
        contract=contract.immutable(value), plan_digest=contract.plan_digest(value),
        price_source=SOURCE, phase='PENDING', status='PENDING',
        reference_at_ms=reference, entry_at_ms=None, cursor_ms=reference-MINUTE_MS,
        last_bar_digest=None, last_evaluated_at_ms=None, next_expected_bar_ms=None,
        trigger_bar_at_ms=None, lock_effective_at_ms=None, formula_stop=value['stop'],
        outcome=None, levels=dict(entry=value['entry'], initial_stop=value['stop'],
            take_profit=value['take_profit'], pending_cancel=value['policy']['pending_cancel_price'],
            original_take_distance=value['policy']['original_take_distance'],
            lock_trigger_price=value['policy']['lock_trigger_price'],
            locked_stop=value['policy']['locked_stop']))


def validate(state):
    try:
        value = deepcopy(state)
        source = deepcopy(value['contract'])
        stamp = contract.moment_ms(source['source_at'])
        source.update(kind='PLAN', source_as_of=contract.iso_ms(stamp),
            source_sequence=stamp*10+1, source_state='PENDING',
            valid_until=contract.iso_ms(stamp+contract.LEASE_MS), cancel_reason=None)
        expected = initialize_from_contract(value['card_id'], source)
        if (set(value) != set(expected) or value['version'] != VERSION
                or any(value[k] != expected[k] for k in ('contract', 'plan_digest',
                    'source_event_id', 'price_source', 'reference_at_ms', 'levels'))):
            raise ValueError()
        cursor = value['cursor_ms']
        common._minute(cursor)
        if cursor < expected['cursor_ms'] or value['phase'] not in ('PENDING', 'OPEN'):
            raise ValueError()
        if value['status'] not in ('PENDING', 'OPEN', 'PRICE_GAP', 'NO_PRICE_DATA',
                'AWAITING_CLOSED_BAR', 'EXIT_RECONCILIATION_REQUIRED', 'AMBIGUOUS', 'CANCELLED'):
            raise ValueError()
        if value['status'] in ('PENDING', 'OPEN') and value['status'] != value['phase']:
            raise ValueError()
        if cursor == expected['cursor_ms']:
            if value['last_bar_digest'] is not None:
                raise ValueError()
        else:
            life.ident(value['last_bar_digest'], r'[0-9a-f]{64}')
        entered = value['entry_at_ms']
        if value['phase'] == 'OPEN':
            common._minute(entered)
            if not stamp <= entered <= cursor:
                raise ValueError()
        elif entered is not None:
            raise ValueError()
        trigger = value['trigger_bar_at_ms']
        if trigger is None:
            if value['lock_effective_at_ms'] is not None or value['formula_stop'] != source['stop']:
                raise ValueError()
        else:
            common._minute(trigger)
            if (entered is None or not entered <= trigger <= cursor
                    or value['lock_effective_at_ms'] != trigger+MINUTE_MS
                    or value['formula_stop'] != value['levels']['locked_stop']):
                raise ValueError()
        at = value['last_evaluated_at_ms']
        if at is not None:
            life.moment(at)
            if cursor >= stamp and at < cursor+MINUTE_MS:
                raise ValueError()
        elif cursor >= stamp:
            raise ValueError()
        if value['next_expected_bar_ms'] != (cursor+MINUTE_MS if value['status'] == 'PRICE_GAP' else None):
            raise ValueError()
        terminal = value['status'] in ('AMBIGUOUS', 'CANCELLED', 'EXIT_RECONCILIATION_REQUIRED')
        if terminal != (value['outcome'] is not None):
            raise ValueError()
        if terminal:
            result = value['outcome']
            life.shape(result, 'kind at_ms price reason')
            if result['at_ms'] != cursor+MINUTE_MS or result['kind'] not in ('SL', 'TP', 'AMBIGUOUS', 'CANCEL'):
                raise ValueError()
            if result['price'] is not None:
                common._price(result['price'])
            if value['status'] == 'CANCELLED':
                if (value['phase'] != 'PENDING' or result['kind'] != 'CANCEL'
                        or result['reason'] != 'CANCELLED_BEFORE_FILL' or result['price'] is not None):
                    raise ValueError()
            elif value['status'] == 'AMBIGUOUS':
                allowed = ('ENTRY_CANCEL_ORDER_UNKNOWN',) if value['phase'] == 'PENDING' else (
                    'STOP_TAKE_ORDER_UNKNOWN', 'ENTRY_TAKE_ORDER_UNKNOWN')
                if result['kind'] != 'AMBIGUOUS' or result['price'] is not None or result['reason'] not in allowed:
                    raise ValueError()
            else:
                expected_reason = ('TAKE_PROFIT' if result['kind'] == 'TP' else
                    'PROFIT_LOCK_STOP' if trigger is not None else 'INITIAL_STOP')
                if (value['phase'] != 'OPEN' or result['kind'] not in ('SL', 'TP')
                        or result['price'] is None or result['reason'] != expected_reason):
                    raise ValueError()
        return value
    except (KeyError, TypeError, ValueError, life.LifecycleError):
        raise ConditionalStopError('G65_STATE_INVALID') from None


def advance(state, bars, *, now_ms, price_source=SOURCE):
    value = validate(state)
    life.moment(now_ms)
    if price_source != SOURCE:
        raise ConditionalStopError('G65_PRICE_SOURCE_MISMATCH')
    if value['last_evaluated_at_ms'] is not None and now_ms < value['last_evaluated_at_ms']:
        raise ConditionalStopError('G65_CLOCK_REGRESSION')
    if not isinstance(bars, list) or len(bars) > common.MAX_BAR_BATCH:
        raise ConditionalStopError('G65_BOUNDED_BAR_LIST_REQUIRED')
    rows = [common._bar(bar) for bar in bars]
    if any(a[0] >= b[0] for a, b in zip(rows, rows[1:])):
        raise ConditionalStopError('G65_UNIQUE_ASCENDING_BARS_REQUIRED')
    if value['outcome'] is not None:
        return value
    value['last_evaluated_at_ms'] = now_ms
    if not rows:
        value.update(status='NO_PRICE_DATA', next_expected_bar_ms=None)
        return validate(value)

    def finish(kind, reason, price=None):
        status = {'AMBIGUOUS': 'AMBIGUOUS', 'CANCEL': 'CANCELLED'}.get(kind, 'EXIT_RECONCILIATION_REQUIRED')
        value.update(status=status, outcome=dict(kind=kind, at_ms=value['cursor_ms']+MINUTE_MS,
            price=None if price is None else str(price), reason=reason))

    for when, prices, fingerprint in rows:
        if when <= value['cursor_ms']:
            if when == value['cursor_ms'] and value['last_bar_digest'] is not None:
                if fingerprint != value['last_bar_digest']:
                    raise ConditionalStopError('G65_LAST_CLOSED_BAR_CHANGED')
                if value['status'] == 'NO_PRICE_DATA':
                    value['status'] = value['phase']
            continue
        if when != value['cursor_ms']+MINUTE_MS:
            value.update(status='PRICE_GAP', next_expected_bar_ms=value['cursor_ms']+MINUTE_MS)
            break
        if when+MINUTE_MS > now_ms:
            value.update(status='AWAITING_CLOSED_BAR', next_expected_bar_ms=None)
            break
        value.update(cursor_ms=when, last_bar_digest=fingerprint,
            status=value['phase'], next_expected_bar_ms=None)
        just_filled, entered_at_open = False, False
        if value['phase'] == 'PENDING':
            entry, cancel = Decimal(value['levels']['entry']), Decimal(value['levels']['pending_cancel'])
            if prices['open'] <= cancel:
                finish('CANCEL', 'CANCELLED_BEFORE_FILL'); break
            entered_at_open = prices['open'] >= entry
            if not entered_at_open and prices['high'] >= entry and prices['low'] <= cancel:
                finish('AMBIGUOUS', 'ENTRY_CANCEL_ORDER_UNKNOWN'); break
            if not entered_at_open and prices['low'] <= cancel:
                finish('CANCEL', 'CANCELLED_BEFORE_FILL'); break
            if not entered_at_open and prices['high'] < entry:
                continue
            value.update(phase='OPEN', status='OPEN', entry_at_ms=when)
            just_filled = True
        stop, take = Decimal(value['formula_stop']), Decimal(value['levels']['take_profit'])
        kind, price = None, None
        if prices['open'] >= stop:
            kind, price = 'SL', prices['open']
        elif prices['open'] <= take and not just_filled:
            kind, price = 'TP', take
        elif prices['high'] >= stop and prices['low'] <= take:
            finish('AMBIGUOUS', 'STOP_TAKE_ORDER_UNKNOWN'); break
        elif prices['high'] >= stop:
            kind, price = 'SL', stop
        elif prices['low'] <= take:
            if just_filled and not entered_at_open and prices['close'] > take:
                finish('AMBIGUOUS', 'ENTRY_TAKE_ORDER_UNKNOWN'); break
            kind, price = 'TP', take
        if kind:
            finish(kind, 'TAKE_PROFIT' if kind == 'TP' else
                'PROFIT_LOCK_STOP' if value['lock_effective_at_ms'] else 'INITIAL_STOP', price)
            break
        # Preserve source float arithmetic and the source's fill-minute CLOSE.
        excursion = float(prices['close'] if just_filled else prices['low'])
        if (value['trigger_bar_at_ms'] is None and
                (float(value['levels']['entry'])-excursion) /
                float(value['levels']['original_take_distance']) >= .75):
            value.update(trigger_bar_at_ms=when, lock_effective_at_ms=when+MINUTE_MS,
                formula_stop=value['levels']['locked_stop'])
    return validate(value)


def promotion(state, *, now_ms, remaining_quantity, observed_quantity, old_oid,
              observed_stop, mark_price, snapshot_at_ms, pending, max_source_lag_ms=15000):
    value = validate(state)
    life.moment(now_ms); life.moment(snapshot_at_ms)
    if value['outcome'] is not None or value['status'] not in ('OPEN', 'AWAITING_CLOSED_BAR'):
        return None
    if value['lock_effective_at_ms'] is None:
        return None
    if pending is not None:
        raise ConditionalStopError('G65_UNRESOLVED_REQUEST_NO_NEW_INTENT')
    if type(max_source_lag_ms) is not int or not 0 <= max_source_lag_ms <= MINUTE_MS:
        raise ConditionalStopError('G65_FRESHNESS_POLICY_INVALID')
    if not (0 <= now_ms-snapshot_at_ms <= 15000 and
            value['lock_effective_at_ms'] <= now_ms and
            0 <= now_ms-value['cursor_ms']-MINUTE_MS <= max_source_lag_ms):
        raise ConditionalStopError('G65_FRESH_SOURCE_AND_EXCHANGE_REQUIRED')
    life.ident(old_oid, r'[1-9][0-9]{0,19}')
    if int(old_oid) >= 2**64:
        raise ConditionalStopError('G65_ACTIVE_OWNED_STOP_REQUIRED')
    remaining = life.number(remaining_quantity, positive=True)
    observed = life.number(observed_quantity, positive=True)
    old, mark = common._price(observed_stop), common._price(mark_price)
    target = Decimal(value['levels']['locked_stop'])
    if old == target and observed == remaining:
        return None
    if old not in (Decimal(value['levels']['initial_stop']), target):
        raise ConditionalStopError('G65_OBSERVED_STOP_DIFFERS_FROM_RULE')
    if not Decimal(value['levels']['take_profit']) < mark < target:
        raise ConditionalStopError('G65_LOCK_OR_TAKE_ALREADY_CROSSED_RECONCILE')
    intent = dict(version=VERSION, card_id=value['card_id'],
        source_event_id=value['source_event_id'], old_oid=old_oid, quantity=str(remaining),
        original_stop=value['levels']['initial_stop'], desired_stop=value['levels']['locked_stop'],
        reduce_only=True, effective_at_ms=value['lock_effective_at_ms'],
        source_state_digest=life.digest(value), exchange_evidence_at_ms=snapshot_at_ms)
    return {**intent, 'intent_id': life.digest(intent), 'order_requests_sent': 0,
        'exchange_protection_confirmed': False}
