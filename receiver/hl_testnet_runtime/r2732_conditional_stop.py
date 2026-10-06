"""Causal, serializable R2732 stop-lock state; no exchange I/O or scheduler.

The formula clock and the exchange clock are different. A closed source candle
can make a lock *due*, but never proves that a venue amended a stop or filled an
exit. The caller persists the returned state in the existing locked journal and
uses its normal owned-order amendment/reconciliation path. This module creates
neither orders nor a second sender and never recomputes risk from partial fills.
"""
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import math

from . import card_lifecycle as life

VERSION = 'r2732-causal-stop-state-v1'
SOURCE = 'HYPERLIQUID_XRP_PERPETUAL_TRADE_1M'
MINUTE_MS = 60000
MAX_BAR_BATCH = 10000
STATES = frozenset(('OPEN', 'PRICE_GAP', 'AWAITING_CLOSED_BAR',
                    'NO_PRICE_DATA', 'EXIT_RECONCILIATION_REQUIRED', 'AMBIGUOUS'))


class ConditionalStopError(life.LifecycleError):
    """Fixed errors only; no raw source messages or account data."""


def _price(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ConditionalStopError('R2732_PRICE_REQUIRED')
    if len(str(value)) > 80:
        raise ConditionalStopError('R2732_PRICE_INVALID')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ConditionalStopError('R2732_PRICE_INVALID') from None
    if (not result.is_finite() or not Decimal('1e-15') <= result <= Decimal('1e18')
            or len(result.as_tuple().digits) > 28):
        raise ConditionalStopError('R2732_PRICE_INVALID')
    return result


def _minute(value):
    life.moment(value)
    if value % MINUTE_MS:
        raise ConditionalStopError('R2732_MINUTE_BOUNDARY_REQUIRED')
    return value


def frozen_levels(reference_entry):
    """Match the deployed source's IEEE-754 operations, including edge cases."""
    entry = float(_price(reference_entry))
    stop = entry * 1.005
    risk = abs(entry - stop)
    take = entry * .92
    trigger, locked = entry - 2 * risk, entry - .5 * risk
    if not all(math.isfinite(v) and v > 0 for v in
               (entry, stop, risk, take, trigger, locked)):
        raise ConditionalStopError('R2732_REPRESENTABLE_POSITIVE_RISK_REQUIRED')
    return dict(entry=str(entry), initial_stop=str(stop), take_profit=str(take),
                original_risk_distance=str(risk), lock_trigger_price=str(trigger),
                locked_stop=str(locked))


def initialize(*, card_id, source_event_id, reference_entry, entry_at_ms,
               price_source=SOURCE):
    life.ident(card_id, r'[0-9a-f]{64}')
    life.ident(source_event_id)
    _minute(entry_at_ms)
    if entry_at_ms < 2 * MINUTE_MS or price_source != SOURCE:
        raise ConditionalStopError('R2732_SOURCE_OR_ENTRY_TIME_INVALID')
    levels = frozen_levels(reference_entry)
    return dict(version=VERSION, card_id=card_id, source_event_id=source_event_id,
                price_source=SOURCE, entry_at_ms=entry_at_ms, levels=levels,
                cursor_ms=entry_at_ms-MINUTE_MS, last_bar_digest=None,
                trigger_bar_at_ms=None, lock_effective_at_ms=None,
                formula_stop=levels['initial_stop'], status='OPEN', outcome=None,
                next_expected_bar_ms=None, last_evaluated_at_ms=None)


def initialize_from_contract(card_id, contract):
    """Freeze a normalized producer envelope; never substitute execution prices."""
    from experimental_execution_contract import normalize
    from .source_window import timestamp
    value = normalize(contract)
    if value['family'] != 'r2732':
        raise ConditionalStopError('R2732_CONTRACT_REQUIRED')
    return initialize(card_id=card_id, source_event_id=value['occurrence_id'],
        reference_entry=value['entry'],
        entry_at_ms=int(timestamp(value['source_at']).timestamp()*1000),
        price_source=value['policy']['source_price'])


def validate(state):
    try:
        life.shape(state, 'version card_id source_event_id price_source entry_at_ms '
            'levels cursor_ms last_bar_digest trigger_bar_at_ms lock_effective_at_ms '
            'formula_stop status outcome next_expected_bar_ms last_evaluated_at_ms')
        expected = initialize(card_id=state['card_id'], source_event_id=state['source_event_id'],
            reference_entry=state['levels']['entry'], entry_at_ms=state['entry_at_ms'],
            price_source=state['price_source'])
        if state['version'] != VERSION or state['levels'] != expected['levels']:
            raise ConditionalStopError('R2732_FROZEN_RULE_CHANGED')
        cursor = _minute(state['cursor_ms'])
        if cursor < expected['cursor_ms'] or state['status'] not in STATES:
            raise ConditionalStopError('R2732_STATE_INVALID')
        if cursor == expected['cursor_ms']:
            if state['last_bar_digest'] is not None:
                raise ConditionalStopError('R2732_CURSOR_PROOF_INVALID')
        else:
            life.ident(state['last_bar_digest'], r'[0-9a-f]{64}')
        trigger, effective = state['trigger_bar_at_ms'], state['lock_effective_at_ms']
        if trigger is None:
            if effective is not None or state['formula_stop'] != state['levels']['initial_stop']:
                raise ConditionalStopError('R2732_PREMATURE_LOCK')
        elif (_minute(trigger) < state['entry_at_ms'] or trigger > cursor
              or effective != trigger+MINUTE_MS
              or state['formula_stop'] != state['levels']['locked_stop']):
            raise ConditionalStopError('R2732_LOCK_TIMING_INVALID')
        observed = state['last_evaluated_at_ms']
        if observed is not None:
            life.moment(observed)
            if cursor >= state['entry_at_ms'] and cursor+MINUTE_MS > observed:
                raise ConditionalStopError('R2732_OPEN_BAR_IN_STATE')
        elif cursor >= state['entry_at_ms']:
            raise ConditionalStopError('R2732_CURSOR_WITHOUT_OBSERVATION')
        gap = state['next_expected_bar_ms']
        if state['status'] == 'PRICE_GAP':
            if gap != cursor+MINUTE_MS:
                raise ConditionalStopError('R2732_GAP_CURSOR_INVALID')
        elif gap is not None:
            raise ConditionalStopError('R2732_UNEXPECTED_GAP_CURSOR')
        outcome = state['outcome']
        if state['status'] in ('EXIT_RECONCILIATION_REQUIRED', 'AMBIGUOUS'):
            life.shape(outcome, 'kind at_ms price reason')
            if (outcome['at_ms'] != cursor+MINUTE_MS
                    or outcome['kind'] not in ('SL', 'TP', 'AMBIGUOUS')):
                raise ConditionalStopError('R2732_OUTCOME_INVALID')
            if state['status'] == 'AMBIGUOUS':
                if outcome['kind'] != 'AMBIGUOUS' or outcome['price'] is not None:
                    raise ConditionalStopError('R2732_AMBIGUITY_INVALID')
            else:
                _price(outcome['price'])
                if outcome['kind'] == 'AMBIGUOUS':
                    raise ConditionalStopError('R2732_OUTCOME_INVALID')
            if outcome['reason'] not in ('INITIAL_STOP', 'PROFIT_LOCK_STOP',
                                          'TAKE_PROFIT', 'BOTH_BARRIERS'):
                raise ConditionalStopError('R2732_OUTCOME_INVALID')
        elif outcome is not None:
            raise ConditionalStopError('R2732_UNEXPECTED_OUTCOME')
        return deepcopy(state)
    except ConditionalStopError:
        raise
    except (life.LifecycleError, KeyError, TypeError, ValueError, OverflowError):
        raise ConditionalStopError('R2732_STATE_INVALID') from None


def _bar(value):
    try:
        life.shape(value, 'open_at_ms open high low close')
        when = _minute(value['open_at_ms'])
        prices = {key: _price(value[key]) for key in ('open', 'high', 'low', 'close')}
        if not (prices['low'] <= min(prices['open'], prices['close'])
                <= max(prices['open'], prices['close']) <= prices['high']):
            raise ConditionalStopError('R2732_OHLC_INVALID')
        # Numeric spelling changes are benign; corrections are not.
        canonical = dict(open_at_ms=when, **{key: format(price.normalize(), 'f')
                                           for key, price in prices.items()})
        return when, prices, life.digest(canonical)
    except ConditionalStopError:
        raise
    except (life.LifecycleError, KeyError, TypeError, ValueError):
        raise ConditionalStopError('R2732_BAR_INVALID') from None


def advance(state, bars, *, now_ms, price_source=SOURCE):
    """Return the next journal value, never mutate inputs or assert venue fills.

    A gap is repairable using the missing contiguous bars. A touched stop/take
    or ambiguous candle requires reconciliation and cannot be reset by replay.
    Existing exchange protection remains in force while this source is missing.
    """
    value = validate(state)
    life.moment(now_ms)
    if price_source != value['price_source']:
        raise ConditionalStopError('R2732_PRICE_SOURCE_MISMATCH')
    if (value['last_evaluated_at_ms'] is not None
            and now_ms < value['last_evaluated_at_ms']):
        raise ConditionalStopError('R2732_CLOCK_REGRESSION')
    if not isinstance(bars, list) or len(bars) > MAX_BAR_BATCH:
        raise ConditionalStopError('R2732_BOUNDED_BAR_LIST_REQUIRED')
    normalized = [_bar(bar) for bar in bars]
    if any(left[0] >= right[0] for left, right in zip(normalized, normalized[1:])):
        raise ConditionalStopError('R2732_UNIQUE_ASCENDING_BARS_REQUIRED')
    if value['outcome'] is not None:
        return value
    value['last_evaluated_at_ms'] = now_ms
    if not normalized:
        value['status'] = 'NO_PRICE_DATA'
        value['next_expected_bar_ms'] = None
        return value
    take = Decimal(value['levels']['take_profit'])
    for when, prices, fingerprint in normalized:
        if when <= value['cursor_ms']:
            if (when == value['cursor_ms'] and value['last_bar_digest'] is not None
                    and fingerprint != value['last_bar_digest']):
                raise ConditionalStopError('R2732_LAST_CLOSED_BAR_CHANGED')
            if (when == value['cursor_ms'] and value['last_bar_digest'] is not None
                    and value['status'] == 'NO_PRICE_DATA'):
                value['status'] = 'OPEN'
            continue
        if when != value['cursor_ms']+MINUTE_MS:
            value.update(status='PRICE_GAP', next_expected_bar_ms=value['cursor_ms']+MINUTE_MS)
            break
        if when+MINUTE_MS > now_ms:
            value.update(status='AWAITING_CLOSED_BAR', next_expected_bar_ms=None)
            break
        value.update(cursor_ms=when, last_bar_digest=fingerprint, status='OPEN',
                     next_expected_bar_ms=None)
        stop = Decimal(value['formula_stop'])
        kind, price = None, None
        if prices['open'] >= stop:
            kind, price = 'SL', prices['open']
        elif prices['open'] <= take:
            kind, price = 'TP', take
        elif prices['high'] >= stop and prices['low'] <= take:
            kind = 'AMBIGUOUS'
        elif prices['high'] >= stop:
            kind, price = 'SL', stop
        elif prices['low'] <= take:
            kind, price = 'TP', take
        if kind is not None:
            reason = ('BOTH_BARRIERS' if kind == 'AMBIGUOUS' else 'TAKE_PROFIT'
                      if kind == 'TP' else 'PROFIT_LOCK_STOP'
                      if value['lock_effective_at_ms'] is not None else 'INITIAL_STOP')
            value.update(status='AMBIGUOUS' if kind == 'AMBIGUOUS'
                         else 'EXIT_RECONCILIATION_REQUIRED',
                outcome=dict(kind=kind, at_ms=when+MINUTE_MS,
                             price=None if price is None else str(price), reason=reason))
            break
        # Identical to the frozen producer: do NOT replace this expression with
        # a Decimal comparison or with entry*.99 at near-threshold boundaries.
        if (value['trigger_bar_at_ms'] is None
                and (float(value['levels']['entry'])-float(prices['low']))
                    / float(value['levels']['original_risk_distance']) >= 2.0):
            value.update(trigger_bar_at_ms=when, lock_effective_at_ms=when+MINUTE_MS,
                         formula_stop=value['levels']['locked_stop'])
    return validate(value)


def promotion(state, *, now_ms, remaining_quantity, observed_quantity, old_oid,
              observed_stop, mark_price, snapshot_at_ms, pending,
              max_source_lag_ms=15000):
    """Describe a due amendment; caller still proves owner, rounds and fences.

    The observed old stop stays on the venue until its atomic amendment is
    independently observed. A pending/unknown amendment is never duplicated.
    Partial fills change only the exact remaining quantity, never lock levels.
    """
    value = validate(state)
    life.moment(now_ms)
    life.moment(snapshot_at_ms)
    if type(max_source_lag_ms) is not int or not 0 <= max_source_lag_ms <= MINUTE_MS:
        raise ConditionalStopError('R2732_SOURCE_FRESHNESS_POLICY_INVALID')
    if (value['status'] not in ('OPEN', 'AWAITING_CLOSED_BAR') or value['outcome'] is not None
            or value['lock_effective_at_ms'] is None):
        return None
    if pending is not None:
        raise ConditionalStopError('R2732_UNRESOLVED_REQUEST_NO_NEW_INTENT')
    if not 0 <= now_ms-snapshot_at_ms <= 15000:
        raise ConditionalStopError('R2732_FRESH_EXCHANGE_EVIDENCE_REQUIRED')
    if not (value['lock_effective_at_ms'] <= now_ms
            and 0 <= now_ms-value['cursor_ms']-MINUTE_MS <= max_source_lag_ms):
        raise ConditionalStopError('R2732_FRESH_CLOSED_SOURCE_REQUIRED')
    life.ident(old_oid, r'[0-9]{1,30}')
    if int(old_oid) == 0:
        raise ConditionalStopError('R2732_ACTIVE_OWNED_STOP_REQUIRED')
    remaining = life.number(remaining_quantity, positive=True)
    observed = life.number(observed_quantity, positive=True)
    old, mark = _price(observed_stop), _price(mark_price)
    target = Decimal(value['levels']['locked_stop'])
    # The caller passes an actual verified venue trigger. Precision adjustment
    # belongs to that adapter; this pure unrounded plan permits the two frozen
    # source levels only and must not be used to authorize an arbitrary reprice.
    if old == target and observed == remaining:
        return None
    if old not in (Decimal(value['levels']['initial_stop']), target):
        raise ConditionalStopError('R2732_OBSERVED_STOP_DIFFERS_FROM_RULE')
    if mark >= target:
        raise ConditionalStopError('R2732_LOCK_ALREADY_CROSSED_RECONCILE')
    if mark <= Decimal(value['levels']['take_profit']):
        raise ConditionalStopError('R2732_TAKE_ALREADY_CROSSED_RECONCILE')
    intent = dict(version=VERSION, card_id=value['card_id'],
        source_event_id=value['source_event_id'], old_oid=old_oid,
        quantity=str(remaining), original_stop=value['levels']['initial_stop'],
        desired_stop=value['levels']['locked_stop'], reduce_only=True,
        effective_at_ms=value['lock_effective_at_ms'],
        source_state_digest=life.digest(value), exchange_evidence_at_ms=snapshot_at_ms)
    return {**intent, 'intent_id': life.digest(intent), 'order_requests_sent': 0,
            'exchange_protection_confirmed': False}
