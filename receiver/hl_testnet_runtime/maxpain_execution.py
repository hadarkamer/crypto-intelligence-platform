"""Prospective MaxPain receiver and unsigned lifecycle, with no live capability.

The caller must authenticate source messages, persist each returned state with
compare-and-swap, and supply independently verified venue evidence. This module
does not turn a source-market touch into a Testnet fill or lift the existing
one-active-card-per-account-and-symbol fence. No network, timers or environment.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import re

from . import price_precision
from .trade_cards import checksum
from .risk_policy import budget

VERSION = 'prospective-maxpain-execution-software-v1'
TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')
TIERS = tuple(map(Decimal, ('.15', '.20', '.15', '.25', '.25', '.30')))
LIFECYCLE_FIELDS = frozenset(('kind', 'source_sequence', 'source_as_of',
                            'source_state', 'valid_until', 'cancel_reason'))


class PlanError(ValueError):
    """Fixed local diagnostic codes; no private data."""


def _ms(value):
    try:
        instant = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if instant.utcoffset() is None:
            raise ValueError()
        return int(instant.astimezone(timezone.utc).timestamp() * 1000)
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise PlanError('TIMEZONE_AWARE_SOURCE_TIME_REQUIRED') from None


def _number(value, *, zero=False):
    try:
        if not isinstance(value, str) or len(value) > 80:
            raise ValueError()
        result = Decimal(value)
        if not result.is_finite() or result < 0 or (not zero and result == 0):
            raise ValueError()
        return result
    except Exception:
        raise PlanError('FINITE_DECIMAL_STRING_REQUIRED') from None


def _time(now_ms):
    if type(now_ms) is not int or now_ms < 0:
        raise PlanError('INTEGER_CLOCK_REQUIRED')
    return now_ms


def _validated(message):
    from experimental_execution_contract import validate
    value = validate(message)
    if value['family'] != 'maxpain':
        raise PlanError('MAXPAIN_PLAN_REQUIRED')
    return value


def _terms(message):
    from experimental_execution_contract import immutable
    return immutable(message)


def initial():
    return dict(version=VERSION, environment='testnet', revision=0, records={})


def _copy(state):
    if (not isinstance(state, dict) or set(state) != {'version', 'environment', 'revision', 'records'}
            or state['version'] != VERSION or state['environment'] != 'testnet'
            or type(state['revision']) is not int or state['revision'] < 0
            or not isinstance(state['records'], dict) or len(state['records']) > 4096):
        raise PlanError('INVALID_MAXPAIN_STATE')
    result = deepcopy(state)
    for record in result['records'].values():
        record.setdefault('entry_evidence', None)
        record.setdefault('exits', {})
        record.setdefault('finality', None)
    return result


def _changed(state):
    state['revision'] += 1
    return state


def receive(state, message, *, now_ms):
    """Apply a monotonically sequenced authenticated source event, idempotently.

    First PLAN must arrive before its original arm boundary. A heartbeat can
    only continue a known plan; CANCEL can establish a tombstone before PLAN.
    Recipient identity is absent from the occurrence key and semantic contract.
    """
    value = _validated(message)
    _time(now_ms)
    result = advance(state, now_ms=now_ms)
    if _ms(value['source_as_of']) > now_ms:
        raise PlanError('FUTURE_SOURCE_EVIDENCE')
    cid = value['occurrence_id']
    previous = result['records'].get(cid)
    if previous is not None:
        if _terms(value) != _terms(previous['plan']):
            raise PlanError('IMMUTABLE_PLAN_CHANGED')
        last = previous['source']
        if value['source_sequence'] < last['source_sequence']:
            if value['kind'] == 'CANCEL':
                # Terminal source evidence wins even if a newer heartbeat was
                # delivered first. Keep its newer watermark, retain tombstone.
                if previous['cancel_reason'] is None:
                    previous['cancel_reason'] = value['cancel_reason']
                    previous['source_cancellation'] = deepcopy(value)
                    return _changed(result)
                return result
            raise PlanError('SOURCE_SEQUENCE_REGRESSION')
        if value['source_sequence'] == last['source_sequence']:
            if any(value[key] != last[key] for key in LIFECYCLE_FIELDS):
                raise PlanError('SOURCE_SEQUENCE_CONFLICT')
            return result
        if _ms(value['source_as_of']) < _ms(last['source_as_of']):
            raise PlanError('SOURCE_CLOCK_REGRESSION')
        # Observe lease gaps BEFORE accepting a renewal; lost monitoring is not
        # proof that the original target remained untouched in the gap.
        if now_ms >= _ms(last['valid_until']) and previous['cancel_reason'] is None:
            previous['cancel_reason'] = 'SOURCE_LEASE_EXPIRED'
    else:
        if len(result['records']) >= 4096:
            raise PlanError('MAXPAIN_STATE_CAPACITY_REQUIRES_REVIEW')
        if value['kind'] == 'HEARTBEAT':
            raise PlanError('HEARTBEAT_CANNOT_CREATE_PLAN')
        if value['kind'] == 'PLAN' and not (_ms(value['created_at']) <= now_ms < _ms(value['arm_at'])
                                            and now_ms < _ms(value['valid_until'])):
            raise PlanError('PROSPECTIVE_PREARM_PLAN_REQUIRED')
        previous = dict(plan=deepcopy(value), source=deepcopy(value), received_at_ms=now_ms,
                        cancel_reason=None, request=None, order=None,
                        fills={}, terminal=None, entry_observation_at_ms=None,
                        entry_evidence=None, exits={}, finality=None)
        result['records'][cid] = previous
    previous['source'] = deepcopy(value)
    if value['kind'] == 'CANCEL':
        previous['cancel_reason'] = previous['cancel_reason'] or value['cancel_reason']
    if now_ms >= _ms(value['expires_at']):
        previous['cancel_reason'] = previous['cancel_reason'] or 'SOURCE_PLAN_EXPIRED'
    return _changed(result)


def advance(state, *, now_ms):
    """Persist source expiry/liveness cancellation without deleting filled work."""
    _time(now_ms)
    result = _copy(state)
    changed = False
    for record in result['records'].values():
        if record['cancel_reason'] is None:
            reason = ('SOURCE_PLAN_EXPIRED' if now_ms >= _ms(record['plan']['expires_at']) else
                      'SOURCE_LEASE_EXPIRED' if now_ms >= _ms(record['source']['valid_until']) else None)
            if reason:
                record['cancel_reason'] = reason
                changed = True
    return _changed(result) if changed else result


def near(first, second):
    a, b = _number(first), _number(second)
    return abs(a - b) <= Decimal('.002') * min(a, b)


def growth_allows(incoming, previous):
    """Match the producer's complete adjacent liquidity-tier proof exactly."""
    if incoming['policy']['liquidity_growth'] is not True or incoming['side'] != previous['side']:
        return False
    old_tf, new_tf = previous['proof']['timeframe'], incoming['proof']['timeframe']
    old, new = TIMEFRAMES.index(old_tf), TIMEFRAMES.index(new_tf)
    if new <= old:
        return False
    direction = 1 if incoming['side'] == 'LONG' else -1
    matches = [row for row in incoming['proof']['cluster_comparisons']
               if row['previous_timeframe'] == old_tf
               and _number(row['previous_target']) == _number(previous['original_target'])
               and row['previous_direction'] == direction]
    if len(matches) != 1:
        return False
    chain = matches[0]['tiers']
    if [row['timeframe'] for row in chain] != list(TIMEFRAMES[old:new+1]):
        return False
    side = 'SHORT' if direction == 1 else 'LONG'
    if any(row['provider_valid'] is not True or row['source_side'] != side for row in chain):
        return False
    targets = [_number(row['target_price']) for row in chain]
    if (targets[0] != _number(previous['original_target'])
            or targets[-1] != _number(incoming['original_target'])
            or max(targets)-min(targets) > Decimal('.002') * min(targets)):
        return False
    amounts = [_number(row['liquidation_amount']) for row in chain]
    return all(amounts[i+1] >= amounts[i]*(1+TIERS[old+i]) for i in range(len(amounts)-1))


def overlap_admission(incoming, peers):
    """Compare this formula's same-coin predecessors, not other strategies.

    The caller supplies prior admissions. Source formula scope is independent
    of the exchange's same-account/symbol exposure fence, checked separately.
    """
    incoming = _validated(incoming)
    for peer in peers:
        from experimental_execution_contract import validate
        peer = validate(peer)
        if peer['family'] != 'maxpain':
            # Other experimental families have their own source admissions.
            # This is not permission to share their live market exposure.
            continue
        if peer['occurrence_id'] == incoming['occurrence_id']:
            return 'DUPLICATE_OCCURRENCE'
        if peer['symbol'] != incoming['symbol'] or peer['rule_id'] != incoming['rule_id']:
            continue
        if near(incoming['original_target'], peer['original_target']) and not growth_allows(incoming, peer):
            return 'OVERLAPPING_TARGET'
    return 'FORMULA_OVERLAP_ALLOWED'


def _source_relation(incoming, peer):
    """Prove predecessor order; recipient arrival/hash are never chronology.

    Source ingest sorts by score before timeframe. Same-time plans therefore
    require the captured comparison proving which plan was already present.
    Without that proof their relative source order is explicitly ambiguous.
    """
    current_time, peer_time = _ms(incoming['created_at']), _ms(peer['created_at'])
    if peer_time != current_time:
        return 'PRIOR' if peer_time < current_time else 'LATER'
    if incoming['proof']['cycle_id'] == peer['proof']['cycle_id']:
        if growth_allows(incoming, peer):
            return 'PRIOR'
        if growth_allows(peer, incoming):
            return 'LATER'
    return 'AMBIGUOUS'


def _market(market, plan, now_ms):
    if (not isinstance(market, dict)
            or set(market) != {'environment', 'symbol', 'at_ms', 'mark_price'}
            or market['environment'] != 'testnet' or market['symbol'] != plan['symbol']
            or type(market['at_ms']) is not int or not 0 <= now_ms-market['at_ms'] <= 15000):
        raise PlanError('FRESH_EXACT_TESTNET_MARKET_REQUIRED')
    return _number(market['mark_price'])


def observe_market(state, cid, market, *, now_ms):
    """Persist demo barrier exclusions; never manufacture a source or demo fill."""
    result = advance(state, now_ms=now_ms)
    record = result['records'][cid]
    plan = record['plan']
    mark = _market(market, plan, now_ms)
    target = _number(plan['original_target'])
    touched = mark >= target if plan['side'] == 'LONG' else mark <= target
    if touched and record['cancel_reason'] is None:
        # This is a venue safety exclusion, NOT a source formula outcome.
        # Source and demo quotes can diverge; no source win/entry is asserted.
        record['cancel_reason'] = 'TESTNET_TARGET_SAFETY_CANCEL_REMAINDER'
        return _changed(result)
    entry = _number(plan['entry'])
    entry_touched = mark <= entry if plan['side'] == 'LONG' else mark >= entry
    if (entry_touched and now_ms >= _ms(plan['arm_at']) and record['request'] is None
            and record['cancel_reason'] is None):
        record['cancel_reason'] = 'TESTNET_ENTRY_SAFETY_WINDOW_MISSED'
        return _changed(result)
    return result


def entry_admission(state, cid, metadata, market, ownership, *, now_ms):
    """Prepare exact rounded prices/quantity, retaining the shared-market fence.

    A mark already past entry is not a prospective resting-limit opportunity.
    The token is a software proposal, never authority to send an exchange order.
    """
    _time(now_ms)
    state = _copy(state)
    record = state['records'][cid]
    plan = _validated(record['plan'])
    mark = _market(market, plan, now_ms)
    def blocked(reason):
        return dict(status=reason, action=None, dispatch_enabled=False)
    if record['cancel_reason'] is not None:
        return blocked('SOURCE_CANCELED')
    if record['request'] is not None:
        return blocked('ENTRY_ALREADY_ATTEMPTED')
    if now_ms < _ms(plan['arm_at']):
        return blocked('WAITING_FOR_ORIGINAL_ARM')
    if now_ms >= _ms(plan['expires_at']):
        return blocked('SOURCE_PLAN_EXPIRED')
    if now_ms >= _ms(record['source']['valid_until']):
        return blocked('SOURCE_LEASE_EXPIRED')
    if record['source']['source_state'] != 'PENDING':
        return blocked('SOURCE_NOT_PENDING')
    peers = []
    for other, peer in state['records'].items():
        if (other == cid or peer['plan']['symbol'] != plan['symbol']
                or peer['plan']['rule_id'] != plan['rule_id']):
            continue
        active = (peer.get('finality') is None and (peer['cancel_reason'] is None or bool(peer['fills'])
                  or peer['request'] is not None and peer['terminal'] is None))
        if not active:
            continue
        if peer['request'] is not None:
            peers.append(peer['plan'])
            continue
        relation = _source_relation(plan, peer['plan'])
        if relation == 'AMBIGUOUS' and near(plan['original_target'], peer['plan']['original_target']):
            return blocked('OVERLAP_SOURCE_ORDER_AMBIGUOUS')
        if relation == 'PRIOR':
            peers.append(peer['plan'])
    overlap = overlap_admission(plan, peers)
    if overlap != 'FORMULA_OVERLAP_ALLOWED':
        return blocked(overlap)
    role = 'long_account' if plan['side'] == 'LONG' else 'short_account'
    if (not isinstance(ownership, dict)
            or set(ownership) != {'account_role', 'symbol', 'all_prior_cards_final', 'unresolved_request'}
            or ownership['account_role'] != role or ownership['symbol'] != plan['symbol']
            or ownership['all_prior_cards_final'] is not True
            or ownership['unresolved_request'] is not False):
        return blocked('SHARED_MARKET_PREDECESSOR_NOT_FINAL')
    signal = dict(kind='SIGNAL', event_id=cid, symbol=plan['symbol'], side=plan['side'],
                  entry=plan['entry'], stop=plan['stop'], take_profit=plan['take_profit'],
                  at=plan['source_at'])
    try:
        prepared = price_precision.prepare_signal(signal, metadata)
    except price_precision.PrecisionError as exc:
        return blocked(str(exc))
    execution = prepared['execution']
    entry, stop, take = (_number(execution[k]) for k in ('entry', 'stop', 'take_profit'))
    if not min(stop, take) < mark < max(stop, take):
        return blocked('PRICE_OUTSIDE_ORIGINAL_EXITS')
    if mark <= entry if plan['side'] == 'LONG' else mark >= entry:
        return blocked('ENTRY_ALREADY_REACHED_NO_LATE_ORDER')
    step = Decimal(1).scaleb(-prepared['audit']['sz_decimals'])
    quantity = ((budget()/abs(entry-stop))/step).to_integral_value(rounding=ROUND_DOWN)*step
    if quantity <= 0:
        return blocked('ZERO_PLANNED_QUANTITY')
    action = dict(version=VERSION, kind='UNSIGNED_ENTRY', occurrence_id=cid,
                  account_role=role, prepared=prepared, quantity=format(quantity, 'f'),
                  state_digest=checksum(state), state_revision=state['revision'],
                  source_sequence=record['source']['source_sequence'], evidence=deepcopy(market),
                  prepared_at_ms=now_ms, source_expires_at=plan['expires_at'],
                  source_valid_until=record['source']['valid_until'], dispatch_enabled=False)
    action['proposal_id'] = checksum(action)
    return dict(status='PROPOSED_SOFTWARE_ONLY', action=action, dispatch_enabled=False)


def begin_entry(state, action, metadata, market, ownership, *, now_ms):
    """Journal-before-send transition for rehearsal; returns no transport action.

    Production integration must re-run source, ownership, metadata and exchange
    budget gates immediately before the existing durable dispatcher begins.
    """
    _time(now_ms)
    result = _copy(state)
    fields = {'version', 'kind', 'occurrence_id', 'account_role', 'prepared', 'quantity',
              'state_digest', 'state_revision', 'source_sequence', 'evidence', 'prepared_at_ms',
              'source_expires_at', 'source_valid_until', 'dispatch_enabled', 'proposal_id'}
    if (not isinstance(action, dict) or action.get('kind') != 'UNSIGNED_ENTRY'
            or set(action) != fields
            or action.get('dispatch_enabled') is not False
            or checksum({k: v for k, v in action.items() if k != 'proposal_id'}) != action.get('proposal_id')
            or action['state_revision'] != result['revision'] or action['state_digest'] != checksum(result)
            or not 0 <= now_ms-action['prepared_at_ms'] <= 15000
            or now_ms >= min(_ms(action['source_valid_until']), _ms(action['source_expires_at']))):
        raise PlanError('EXACT_FRESH_ENTRY_PROPOSAL_REQUIRED')
    record = result['records'][action['occurrence_id']]
    if record['request'] is not None or record['cancel_reason'] is not None:
        raise PlanError('ENTRY_ALREADY_ATTEMPTED_OR_CANCELED')
    # Rebuild all source prices, rounded terms, size, ownership and liveness
    # using a fresh external observation. A recomputed self-hash is no proof.
    rebuilt = entry_admission(result, action['occurrence_id'], metadata, market, ownership, now_ms=now_ms)
    expected = rebuilt['action']
    if expected is None or any(action.get(key) != expected[key] for key in (
            'version', 'kind', 'occurrence_id', 'account_role', 'prepared', 'quantity',
            'state_digest', 'state_revision', 'source_sequence', 'source_expires_at',
            'source_valid_until', 'dispatch_enabled')):
        raise PlanError('ENTRY_REVALIDATION_FAILED')
    record['request'] = dict(request_id=action['proposal_id'], attempted_at_ms=now_ms,
                             action=deepcopy(action), phase='OUTCOME_UNKNOWN')
    return _changed(result)


def observe_entry(state, cid, evidence, *, now_ms):
    """Apply complete, externally verified exact-order fill history monotonically.

    A canceled reply cannot erase a racing fill. Neither a source alert nor a
    venue acknowledgement counts as a fill. Evidence belongs to the persisted
    request and exact Testnet order; transport must verify that correspondence.
    """
    _time(now_ms)
    result = _copy(state)
    record = result['records'][cid]
    req = record['request']
    required = {'environment', 'symbol', 'side', 'request_id', 'order_id', 'at_ms',
                'status', 'history_complete', 'fills'}
    if (req is None or not isinstance(evidence, dict) or set(evidence) != required
            or evidence['environment'] != 'testnet' or evidence['symbol'] != record['plan']['symbol']
            or evidence['side'] != record['plan']['side'] or evidence['request_id'] != req['request_id']
            or evidence['history_complete'] is not True
            or not isinstance(evidence['order_id'], str)
            or not re.fullmatch(r'[1-9][0-9]{0,19}', evidence['order_id'])
            or not 0 < int(evidence['order_id']) < 2**64
            or type(evidence['at_ms']) is not int
            or not req['attempted_at_ms'] <= evidence['at_ms'] <= now_ms
            or evidence['status'] not in ('OPEN', 'CANCELED', 'FILLED', 'REJECTED')
            or not isinstance(evidence['fills'], list)):
        raise PlanError('EXACT_COMPLETE_ENTRY_EVIDENCE_REQUIRED')
    if record['order'] is not None and record['order'] != evidence['order_id']:
        raise PlanError('ENTRY_ORDER_OWNERSHIP_CHANGED')
    previous_at = record['entry_observation_at_ms']
    if previous_at is not None and evidence['at_ms'] <= previous_at:
        if evidence == record.get('entry_evidence'):
            return result
        raise PlanError('ENTRY_OBSERVATION_NOT_NEWER')
    fills = {}
    for fill in evidence['fills']:
        if (not isinstance(fill, dict) or set(fill) != {'fill_id', 'quantity', 'price', 'at_ms'}
                or not isinstance(fill['fill_id'], str) or not 1 <= len(fill['fill_id']) <= 100
                or fill['fill_id'] in fills or type(fill['at_ms']) is not int
                or not req['attempted_at_ms'] <= fill['at_ms'] <= evidence['at_ms']):
            raise PlanError('EXACT_ENTRY_FILL_HISTORY_REQUIRED')
        _number(fill['quantity'])
        fill_price = _number(fill['price'])
        limit = _number(req['action']['prepared']['execution']['entry'])
        if fill_price > limit if record['plan']['side'] == 'LONG' else fill_price < limit:
            raise PlanError('FILL_WORSE_THAN_OWN_LIMIT')
        fills[fill['fill_id']] = deepcopy(fill)
    _assert_unique_fills(result, cid, None, fills)
    if any(fills.get(key) != fill for key, fill in record['fills'].items()):
        raise PlanError('PREVIOUS_ENTRY_FILL_REMOVED_OR_CHANGED')
    if record['terminal'] is not None:
        if evidence['status'] != record['terminal']['status'] or fills != record['fills']:
            raise PlanError('FINAL_ENTRY_EVIDENCE_CHANGED')
    quantity = sum((_number(fill['quantity']) for fill in fills.values()), Decimal(0))
    planned = _number(req['action']['quantity'])
    if quantity > planned or evidence['status'] == 'FILLED' and quantity != planned:
        raise PlanError('ENTRY_QUANTITY_NOT_RECONCILED')
    if evidence['status'] == 'REJECTED' and quantity != 0:
        raise PlanError('REJECTED_ENTRY_CANNOT_HAVE_FILLS')
    if record.get('finality') is not None and fills != record['fills']:
        raise PlanError('FINAL_ALLOCATION_CHANGED')
    _assert_unique_order(result, cid, None, evidence['order_id'])
    record.update(order=evidence['order_id'], fills=fills, entry_observation_at_ms=evidence['at_ms'],
                  entry_evidence=deepcopy(evidence))
    req['phase'] = 'OBSERVED'
    if evidence['status'] != 'OPEN' and record['terminal'] is None:
        record['terminal'] = dict(status=evidence['status'], at_ms=evidence['at_ms'])
    return _changed(result)


def maintenance(state, cid, *, now_ms):
    """Return needs for the existing owner-bound protection/cancel dispatcher.

    The two needs coexist during a cancel/fill race: protect confirmed quantity
    and cancel only the unfilled remainder. This module never claims protection
    was installed, never flattens a pooled position, and never resends ambiguity.
    """
    result = advance(state, now_ms=now_ms)
    record = result['records'][cid]
    entered = sum((_number(fill['quantity']) for fill in record['fills'].values()), Decimal(0))
    quantity = remaining_quantity(record)
    needs = []
    if quantity:
        needs.append(dict(kind='MAINTAIN_OWN_FILLED_PROTECTION', occurrence_id=cid,
                          confirmed_entry_quantity=format(entered, 'f'),
                          confirmed_remaining_quantity=format(quantity, 'f'),
                          original_stop=record['plan']['stop'], original_take_profit=record['plan']['take_profit']))
    if record['request'] is not None and record['order'] is None:
        needs.append(dict(kind='RECONCILE_ENTRY_OUTCOME', request_id=record['request']['request_id']))
    elif record['cancel_reason'] is not None and record['order'] is not None and record['terminal'] is None:
        remainder = _number(record['request']['action']['quantity'])-entered
        if remainder > 0:
            needs.append(dict(kind='CANCEL_OWN_ENTRY_REMAINDER', order_id=record['order'],
                              remainder_quantity=format(remainder, 'f'), reason=record['cancel_reason']))
    for request in record['exits'].values():
        if request['order_id'] is None:
            needs.append(dict(kind='RECONCILE_EXIT_OUTCOME', request_id=request['request_id']))
        elif quantity == 0 and request['terminal'] is None:
            needs.append(dict(kind='CANCEL_OWN_EXIT_REMAINDER', order_id=request['order_id'],
                              request_id=request['request_id']))
    if (record['request'] is not None and quantity == 0 and record['terminal'] is not None
            and record['finality'] is None):
        needs.append(dict(kind='RECONCILE_ALLOCATION_FINALITY', occurrence_id=cid))
    return dict(state=result, needs=needs, dispatch_enabled=False,
                finality_verified=record['finality'] is not None,
                protection_verified=False, shared_market_isolation_verified=False)


def remaining_quantity(record):
    """Actual entry fills minus actual fills of explicitly owned exit orders."""
    entered = sum((_number(f['quantity']) for f in record['fills'].values()), Decimal(0))
    exited = sum((_number(f['quantity']) for r in record.get('exits', {}).values()
                  for f in r['fills'].values()), Decimal(0))
    if exited > entered:
        raise PlanError('OWN_EXIT_EXCEEDS_CONFIRMED_ENTRY')
    return entered - exited


def _assert_unique_order(state, cid, request_id, order_id):
    role = 'long_account' if state['records'][cid]['plan']['side'] == 'LONG' else 'short_account'
    for other_cid, record in state['records'].items():
        other_role = 'long_account' if record['plan']['side'] == 'LONG' else 'short_account'
        if other_role != role:
            continue
        if record['order'] == order_id and (other_cid != cid or request_id is not None):
            raise PlanError('ORDER_ALREADY_OWNED')
        for other_id, request in record.get('exits', {}).items():
            if request['order_id'] == order_id and (other_cid, other_id) != (cid, request_id):
                raise PlanError('ORDER_ALREADY_OWNED')


def _assert_unique_fills(state, cid, request_id, fills):
    plan = state['records'][cid]['plan']
    for other_cid, record in state['records'].items():
        if record['plan']['symbol'] != plan['symbol'] or record['plan']['side'] != plan['side']:
            continue
        if (other_cid, None) != (cid, request_id) and set(fills) & set(record['fills']):
            raise PlanError('FILL_ALREADY_OWNED')
        for other_id, request in record.get('exits', {}).items():
            if (other_cid, other_id) != (cid, request_id) and set(fills) & set(request['fills']):
                raise PlanError('FILL_ALREADY_OWNED')


def begin_exit(state, cid, kind, quantity, *, now_ms):
    """Durably reserve one unsigned protective request before simulated sending.

    Each exit is capped at this occurrence's remaining actual fills. This is an
    allocation invariant, not a claim that pooled exchange positions support
    independent exits; the live exclusive-market fence remains mandatory.
    """
    _time(now_ms)
    result = _copy(state)
    record = result['records'][cid]
    amount = _number(quantity)
    if (kind not in ('STOP', 'TAKE_PROFIT') or record['request'] is None
            or record['order'] is None or record['finality'] is not None
            or amount > remaining_quantity(record)
            or record['entry_observation_at_ms'] > now_ms):
        raise PlanError('EXACT_OWN_REMAINING_EXIT_REQUIRED')
    observation_times = [record['entry_observation_at_ms']] + [
        r['evidence']['at_ms'] for r in record['exits'].values() if r['evidence'] is not None]
    if (not 0 <= now_ms-max(observation_times) <= 15000
            or any(r['evidence'] is None or r['terminal'] is None
                   and not 0 <= now_ms-r['evidence']['at_ms'] <= 15000 for r in record['exits'].values())
            or record['terminal'] is None and not 0 <= now_ms-record['entry_observation_at_ms'] <= 15000):
        raise PlanError('FRESH_RECONCILED_EXIT_ALLOCATION_REQUIRED')
    if any(r['kind'] == kind and r['terminal'] is None for r in record['exits'].values()):
        raise PlanError('EXIT_KIND_ALREADY_PENDING')
    request = dict(occurrence_id=cid, kind=kind, quantity=quantity, attempted_at_ms=now_ms,
                   state_revision=result['revision'], dispatch_enabled=False)
    request_id = checksum(request)
    record['exits'][request_id] = dict(**request, request_id=request_id,
        order_id=None, fills={}, terminal=None, evidence=None)
    return _changed(result)


def observe_exit(state, cid, request_id, evidence, *, now_ms):
    """Reconcile complete exact-order exit history, including cancel/fill races.

    A transport adapter must prove request/OID correspondence independently.
    A reply acknowledgement alone must never be passed as complete evidence.
    """
    _time(now_ms)
    result = _copy(state)
    record = result['records'][cid]
    request = record['exits'].get(request_id)
    role = 'long_account' if record['plan']['side'] == 'LONG' else 'short_account'
    required = {'environment', 'account_role', 'symbol', 'side', 'request_id', 'order_id',
                'at_ms', 'status', 'history_complete', 'reduce_only', 'fills'}
    if (request is None or not isinstance(evidence, dict) or set(evidence) != required
            or evidence['environment'] != 'testnet' or evidence['account_role'] != role
            or evidence['symbol'] != record['plan']['symbol']
            or evidence['side'] != ('SELL' if record['plan']['side'] == 'LONG' else 'BUY')
            or evidence['request_id'] != request_id or evidence['reduce_only'] is not True
            or evidence['history_complete'] is not True
            or not isinstance(evidence['order_id'], str)
            or not re.fullmatch(r'[1-9][0-9]{0,19}', evidence['order_id'])
            or not 0 < int(evidence['order_id']) < 2**64
            or type(evidence['at_ms']) is not int
            or not request['attempted_at_ms'] <= evidence['at_ms'] <= now_ms
            or evidence['status'] not in ('OPEN', 'CANCELED', 'FILLED', 'REJECTED')
            or not isinstance(evidence['fills'], list)):
        raise PlanError('EXACT_COMPLETE_EXIT_EVIDENCE_REQUIRED')
    if request['order_id'] is not None and request['order_id'] != evidence['order_id']:
        raise PlanError('EXIT_ORDER_OWNERSHIP_CHANGED')
    previous = request['evidence']
    if previous is not None and evidence['at_ms'] <= previous['at_ms']:
        if evidence == previous:
            return result
        raise PlanError('EXIT_OBSERVATION_NOT_NEWER')
    _assert_unique_order(result, cid, request_id, evidence['order_id'])
    fills = {}
    other_fill_ids = set(record['fills'])
    other_fill_ids.update(f for rid, r in record['exits'].items() if rid != request_id for f in r['fills'])
    for fill in evidence['fills']:
        if (not isinstance(fill, dict) or set(fill) != {'fill_id', 'quantity', 'price', 'at_ms'}
                or not isinstance(fill['fill_id'], str) or not 1 <= len(fill['fill_id']) <= 100
                or fill['fill_id'] in fills or fill['fill_id'] in other_fill_ids
                or type(fill['at_ms']) is not int
                or not request['attempted_at_ms'] <= fill['at_ms'] <= evidence['at_ms']):
            raise PlanError('EXACT_EXIT_FILL_HISTORY_REQUIRED')
        _number(fill['quantity']); _number(fill['price'])
        fills[fill['fill_id']] = deepcopy(fill)
    _assert_unique_fills(result, cid, request_id, fills)
    if any(fills.get(key) != fill for key, fill in request['fills'].items()):
        raise PlanError('PREVIOUS_EXIT_FILL_REMOVED_OR_CHANGED')
    if request['terminal'] is not None and (evidence['status'] != request['terminal']['status']
                                           or fills != request['fills']):
        raise PlanError('FINAL_EXIT_EVIDENCE_CHANGED')
    quantity = sum((_number(f['quantity']) for f in fills.values()), Decimal(0))
    planned = _number(request['quantity'])
    if quantity > planned or evidence['status'] == 'FILLED' and quantity != planned:
        raise PlanError('EXIT_QUANTITY_NOT_RECONCILED')
    if evidence['status'] == 'REJECTED' and quantity:
        raise PlanError('REJECTED_EXIT_CANNOT_HAVE_FILLS')
    request.update(order_id=evidence['order_id'], fills=fills, evidence=deepcopy(evidence))
    remaining_quantity(record)
    if evidence['status'] != 'OPEN' and request['terminal'] is None:
        request['terminal'] = dict(status=evidence['status'], at_ms=evidence['at_ms'])
    return _changed(result)


def reconcile_finality(state, cid, evidence, *, now_ms):
    """Release only after flat authoritative market evidence and all OIDs final.

    This function intentionally supports the existing exclusive-market mode.
    Pooled positions must instead use a complete account allocation ledger; a
    per-card zero or a canceled source message is insufficient market finality.
    """
    _time(now_ms)
    result = _copy(state)
    record = result['records'][cid]
    role = 'long_account' if record['plan']['side'] == 'LONG' else 'short_account'
    required = {'environment', 'account_role', 'symbol', 'at_ms', 'history_complete',
                'orders_complete', 'position_complete', 'position_quantity', 'open_order_ids',
                'terminal_order_ids'}
    if (not isinstance(evidence, dict) or set(evidence) != required
            or evidence['environment'] != 'testnet' or evidence['account_role'] != role
            or evidence['symbol'] != record['plan']['symbol']
            or any(evidence[k] is not True for k in ('history_complete','orders_complete','position_complete'))
            or type(evidence['at_ms']) is not int or not 0 <= now_ms-evidence['at_ms'] <= 15000
            or _number(evidence['position_quantity'], zero=True) != 0
            or evidence['open_order_ids'] != []
            or not isinstance(evidence['terminal_order_ids'], list)
            or any(not isinstance(oid, str) for oid in evidence['terminal_order_ids'])
            or len(set(evidence['terminal_order_ids'])) != len(evidence['terminal_order_ids'])):
        raise PlanError('COMPLETE_FLAT_EXCLUSIVE_MARKET_REQUIRED')
    if (record['request'] is None or record['terminal'] is None
            or remaining_quantity(record) != 0
            or any(r['terminal'] is None for r in record['exits'].values())):
        raise PlanError('OWN_ALLOCATION_NOT_FINAL')
    owned = {record['order']} | {r['order_id'] for r in record['exits'].values()}
    if None in owned or not owned.issubset(set(evidence['terminal_order_ids'])):
        raise PlanError('OWN_TERMINAL_ORDERS_NOT_RECONCILED')
    latest = max([record['entry_observation_at_ms']] +
                 [r['evidence']['at_ms'] for r in record['exits'].values()])
    if evidence['at_ms'] < latest:
        raise PlanError('FINALITY_PRECEDES_LATEST_FILL_EVIDENCE')
    if record['finality'] is not None:
        if evidence['at_ms'] < record['finality']['at_ms']:
            raise PlanError('FINALITY_EVIDENCE_REGRESSION')
        return result
    record['finality'] = deepcopy(evidence)
    return _changed(result)
