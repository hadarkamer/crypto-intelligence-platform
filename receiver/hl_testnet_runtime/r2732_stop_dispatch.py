"""Isolated R2732 stop-amendment protocol; no transport or production hook.

Every returned transition must be committed under the experimental source
store's revision lock BEFORE a caller acts on it. In particular begin() is an
at-most-once durable attempt fence, not a request to retry after a restart.
Only a verified Testnet registrar may supply the initial binding. This module
never admits an entry or relaxes the legacy card/dispatch validators.

The original card prices remain immutable. Observed replacement stop prices
are associated with exact order IDs and exact persisted amendment requests.
Receipts alone never establish protection, retirement, or a fill.
"""
from copy import deepcopy

from . import card_lifecycle as life
from . import filled_quantity_dispatch as wire
from . import r2732_conditional_stop as condition
from . import price_precision

VERSION = 'r2732-stop-amendment-protocol-v1'
PHASES = {'RESERVED', 'ATTEMPTED', 'ACKNOWLEDGED', 'UNKNOWN', 'REJECTED',
          'OBSERVED', 'REJECTED_PROVEN', 'ABANDONED_UNSENT'}
REMEDIABLE = {'STOP_COVERAGE_MISSING', 'STOP_EXCEEDS_CARD_REMAINDER',
              'TAKE_PROFIT_COVERAGE_MISSING', 'TAKE_PROFIT_EXCEEDS_CARD_REMAINDER'}


class StopDispatchError(life.LifecycleError):
    """Fixed diagnostic codes; no raw source, account or venue errors."""


def _condition(contract, card_id):
    return condition.initialize_from_contract(card_id, contract)


def prepare_execution(contract, metadata):
    """Separate venue rounding under the existing approved precision policy."""
    from experimental_execution_contract import normalize
    value = normalize(contract)
    if value['family'] != 'r2732':
        raise StopDispatchError('R2732_CONTRACT_REQUIRED')
    index, decimals = wire.asset(metadata, 'XRP')
    signal = dict(kind='SIGNAL', event_id=value['occurrence_id'], symbol='XRP', side='SHORT',
        entry=value['entry'], stop=value['stop'], take_profit=value['take_profit'],
        at=value['source_at'])
    prepared = price_precision.prepare_signal(signal, metadata)
    locked = price_precision.round_price(value['policy']['locked_stop'], decimals)
    p = prepared['execution']
    if not (life.number(p['take_profit']) < life.number(locked) < life.number(p['entry'])):
        raise StopDispatchError('R2732_LOCK_COLLAPSED_AFTER_ROUNDING')
    return dict(market=dict(asset_index=index, size_decimals=decimals),
                prepared=prepared, locked_stop=locked)


def _owner(contract, binding, execution):
    life.validate_bindings([binding])
    expected = _condition(contract, binding['card_id'])
    if (binding['symbol'] != 'XRP' or binding['side'] != 'SHORT'
            or binding['role'] != 'short_account'
            or binding['card_digest'] != life.digest(contract)
            or binding['prices'] != {k: execution['prepared']['execution'][k]
                                    for k in ('entry', 'stop', 'take_profit')}
            or len(binding['orders']['ENTRY']) != 1
            or len(binding['orders']['STOP']) != 1
            or 'MANUAL_EXIT' in binding['orders']):
        raise StopDispatchError('IMMUTABLE_R2732_OWNER_REQUIRED')
    return expected


def _binding(state, *, extra_oid=None):
    binding = deepcopy(state['original_binding'])
    binding['orders']['STOP'] = list(state['stop_terms'])
    if extra_oid is not None and extra_oid not in binding['orders']['STOP']:
        binding['orders']['STOP'].append(extra_oid)
    return binding


def _review(state, snapshot, *, now_ms, terms=None):
    """Private read projection after exact per-OID validation, never wire input."""
    terms = state['stop_terms'] if terms is None else terms
    binding = _binding(state)
    binding['orders']['STOP'] = list(terms)
    life.validate_snapshot(snapshot)
    if (snapshot['account'] != binding['account'] or snapshot['symbol'] != 'XRP'
            or snapshot['history_complete'] is not True
            or snapshot['orders_complete'] is not True
            or not 0 <= now_ms - snapshot['at_ms'] <= 15000):
        raise StopDispatchError('COMPLETE_FRESH_OWN_TESTNET_EVIDENCE_REQUIRED')
    projected = deepcopy(snapshot)
    for row in projected['open_orders']:
        if row['oid'] not in terms:
            continue
        expected = life.number(terms[row['oid']], positive=True)
        if (row['order_type'] != 'SL_MARKET' or row['trigger_price'] is None
                or row['state'] != 'ACTIVE' or row['reduce_only'] is not True
                or row['side'] != 'B'
                or life.number(row['trigger_price'], positive=True) != expected
                or life.number(row['price'], positive=True) != expected):
            raise StopDispatchError('STOP_TERMS_DIFFER_FROM_OWN_REQUEST')
        # Lifecycle quantities/PnL still derive from ORIGINAL fills, prices and
        # immutable binding. Only the already-proven stop observation is mapped
        # back to the old lifecycle's fixed trigger-price vocabulary.
        row['trigger_price'] = binding['prices']['stop']
    return life.review([binding], projected, now_ms=now_ms)


def validate(state):
    life.shape(state, 'version contract original_binding market execution original_digest condition '
               'stop_terms snapshot requests pending halted_reason')
    if state['version'] != VERSION:
        raise StopDispatchError('STOP_PROTOCOL_VERSION_REQUIRED')
    life.shape(state['market'], 'asset_index size_decimals')
    market = state['market']
    if (type(market['asset_index']) is not int or not 0 <= market['asset_index'] < 10000
            or type(market['size_decimals']) is not int or not 0 <= market['size_decimals'] <= 6):
        raise StopDispatchError('FROZEN_TESTNET_MARKET_REQUIRED')
    metadata = {'universe': [{'name': '_unused'} for _ in range(market['asset_index'])]
                + [{'name': 'XRP', 'szDecimals': market['size_decimals']}]}
    if state['execution'] != prepare_execution(state['contract'], metadata):
        raise StopDispatchError('FROZEN_R2732_EXECUTION_CHANGED')
    initial = _owner(state['contract'], state['original_binding'], state['execution'])
    if state['original_digest'] != life.digest([
            state['contract'], state['original_binding'], market, state['execution']]):
        raise StopDispatchError('STOP_ORIGINAL_CHANGED')
    current = condition.validate(state['condition'])
    # Compare the immutable reducer contract through its initializer; state
    # evolution is independently validated by the reducer itself.
    for key in ('card_id', 'source_event_id', 'levels', 'entry_at_ms', 'price_source'):
        if current[key] != initial[key]:
            raise StopDispatchError('STOP_CONDITION_OWNER_CHANGED')
    if not isinstance(state['requests'], list) or len(state['requests']) > 10000:
        raise StopDispatchError('STOP_REQUEST_HISTORY_REQUIRED')
    expected_terms = {state['original_binding']['orders']['STOP'][0]:
                      state['original_binding']['prices']['stop']}
    request_ids = set()
    for sequence, request in enumerate(state['requests'], 1):
        life.shape(request, 'request_id proposal phase attempt_at_ms reply observed_oid '
                   'observed_at_ms')
        proposal = request['proposal']
        life.shape(proposal, 'version card_id account symbol role leg operation quantity '
                   'old_oid sequence action basis observed_at_ms condition_digest')
        if (request['phase'] not in PHASES or proposal['version'] != VERSION
                or request['request_id'] != life.digest([VERSION, proposal])
                or request['request_id'] in request_ids or proposal['sequence'] != sequence
                or proposal['card_id'] != current['card_id']
                or proposal['account'] != state['original_binding']['account']
                or proposal['symbol'] != 'XRP' or proposal['role'] != 'short_account'
                or proposal['leg'] != 'STOP' or proposal['operation'] != 'MODIFY_EXIT'
                or proposal['old_oid'] not in expected_terms):
            raise StopDispatchError('STOP_REQUEST_IDENTITY_CHANGED')
        request_ids.add(request['request_id'])
        life.ident(proposal['basis'], r'[0-9a-f]{64}')
        life.ident(proposal['condition_digest'], r'[0-9a-f]{64}')
        life.moment(proposal['observed_at_ms'])
        action = wire.canonical_wire_action(proposal['action'])
        if action != proposal['action'] or action['type'] != 'batchModify':
            raise StopDispatchError('EXACT_STOP_MODIFICATION_REQUIRED')
        order = wire.requested_order(action)
        if (str(action['modifies'][0]['oid']) != proposal['old_oid']
                or order['a'] != market['asset_index']
                or order['b'] is not True or order['r'] is not True
                or life.number(order['s'], positive=True) != life.number(proposal['quantity'], positive=True)
                or order['p'] != state['execution']['locked_stop']
                or order['t'] != dict(trigger=dict(isMarket=True, triggerPx=order['p'], tpsl='sl'))
                or order['c'] != '0x' + life.digest([VERSION, state['original_digest'], sequence])[:32]):
            raise StopDispatchError('STOP_REQUEST_TERMS_CHANGED')
        wire.precise(order['p'], order['s'], market['size_decimals'])
        attempted = request['attempt_at_ms'] is not None
        if attempted:
            life.moment(request['attempt_at_ms'])
            if request['attempt_at_ms'] < proposal['observed_at_ms']:
                raise StopDispatchError('STOP_ATTEMPT_PRECEDES_EVIDENCE')
        elif request['phase'] not in ('RESERVED', 'ABANDONED_UNSENT'):
            raise StopDispatchError('DURABLE_STOP_ATTEMPT_REQUIRED')
        if request['phase'] in ('RESERVED', 'ABANDONED_UNSENT') and attempted:
            raise StopDispatchError('ATTEMPT_CANNOT_BECOME_UNSENT')
        if request['phase'] == 'OBSERVED':
            oid = life.ident(request['observed_oid'], r'[0-9]{1,30}')
            if oid in expected_terms or oid in {
                    x for leg, ids in state['original_binding']['orders'].items()
                    if leg != 'STOP' for x in ids}:
                raise StopDispatchError('REPLACEMENT_ORDER_ID_NOT_NEW')
            life.moment(request['observed_at_ms'])
            if request['observed_at_ms'] <= request['attempt_at_ms']:
                raise StopDispatchError('POST_ATTEMPT_STOP_EVIDENCE_REQUIRED')
            expected_terms[oid] = order['p']
        elif request['observed_oid'] is not None:
            raise StopDispatchError('UNPROVEN_REPLACEMENT_ORDER_ID')
    if state['stop_terms'] != expected_terms:
        raise StopDispatchError('UNPROVEN_EFFECTIVE_STOP_TERMS')
    unresolved = [r['request_id'] for r in state['requests']
                  if r['phase'] not in ('OBSERVED', 'REJECTED_PROVEN', 'ABANDONED_UNSENT')]
    if unresolved != ([] if state['pending'] is None else [state['pending']]):
        raise StopDispatchError('ONE_UNRESOLVED_STOP_REQUEST_REQUIRED')
    if state['pending'] is not None and state['requests'][-1]['request_id'] != state['pending']:
        raise StopDispatchError('STOP_PENDING_REQUEST_MUST_BE_LATEST')
    if state['halted_reason'] not in (None, 'STOP_AMENDMENT_REJECTED'):
        raise StopDispatchError('STOP_HALT_REASON_INVALID')
    if any(r['phase'] == 'REJECTED_PROVEN' for r in state['requests']) != bool(state['halted_reason']):
        raise StopDispatchError('REJECTED_STOP_MUST_REMAIN_HALTED')
    _review(state, state['snapshot'], now_ms=state['snapshot']['at_ms'])
    return deepcopy(state)


def initialize(contract, binding, snapshot, *, metadata, now_ms):
    """Start from an already-owned, originally priced STOP; never creates entry."""
    execution = prepare_execution(contract, metadata)
    initial = _owner(contract, binding, execution)
    market = execution['market']
    result = dict(version=VERSION, contract=deepcopy(contract),
        original_binding=deepcopy(binding), market=market, execution=execution,
        original_digest=life.digest([contract, binding, market, execution]),
        condition=initial, stop_terms={binding['orders']['STOP'][0]: binding['prices']['stop']},
        snapshot=deepcopy(snapshot), requests=[], pending=None, halted_reason=None)
    view = _review(result, snapshot, now_ms=now_ms)
    owner = view['cards'][0]
    if (view['bucket_issues'] or set(owner['issues']) - REMEDIABLE
            or life.number(owner['remaining_quantity'], positive=True) <= 0
            or not any(o['oid'] == binding['orders']['STOP'][0] for o in snapshot['open_orders'])):
        raise StopDispatchError('CONFIRMED_OWN_INITIAL_STOP_REQUIRED')
    return validate(result)


def advance(state, bars, *, now_ms, price_source=condition.SOURCE):
    result = validate(state)
    result['condition'] = condition.advance(result['condition'], bars,
        now_ms=now_ms, price_source=price_source)
    return validate(result)


def _decision(state, metadata, sample, *, now_ms):
    if state['pending'] is not None:
        raise StopDispatchError('STOP_OUTCOME_UNRESOLVED_NO_NEW_REQUEST')
    if state['halted_reason'] is not None:
        raise StopDispatchError('STOP_AMENDMENT_REJECTED_REVIEW_REQUIRED')
    life.shape(sample, 'mark_price at_ms')
    life.moment(sample['at_ms'])
    if not 0 <= now_ms - sample['at_ms'] <= 15000:
        raise StopDispatchError('FRESH_STOP_MARK_REQUIRED')
    snapshot = state['snapshot']
    view = _review(state, snapshot, now_ms=now_ms)
    owner = view['cards'][0]
    if view['bucket_issues'] or set(owner['issues']) - REMEDIABLE:
        raise StopDispatchError('STOP_QUANTITY_RECONCILIATION_REQUIRED')
    remaining = life.number(owner['remaining_quantity'], signed=True)
    if remaining <= 0:
        return None
    stops = [o for o in snapshot['open_orders'] if o['oid'] in state['stop_terms']]
    if len(stops) != 1:
        raise StopDispatchError('ONE_EXACT_ACTIVE_STOP_REQUIRED')
    old = stops[0]
    actual_price = state['stop_terms'][old['oid']]
    execution = state['execution']
    initial_price = execution['prepared']['execution']['stop']
    rounded_lock = execution['locked_stop']
    if actual_price == initial_price:
        source_stop = state['condition']['levels']['initial_stop']
    elif actual_price == rounded_lock:
        source_stop = state['condition']['levels']['locked_stop']
    else:
        raise StopDispatchError('STOP_EXECUTION_PRICE_NOT_BOUND_TO_POLICY')
    desired = condition.promotion(state['condition'], now_ms=now_ms,
        remaining_quantity=life.text(remaining), observed_quantity=old['quantity'],
        old_oid=old['oid'], observed_stop=source_stop,
        mark_price=sample['mark_price'], snapshot_at_ms=snapshot['at_ms'], pending=None)
    if desired is None:
        return None
    if life.number(sample['mark_price'], positive=True) >= life.number(rounded_lock):
        raise StopDispatchError('R2732_ROUNDED_LOCK_ALREADY_CROSSED_RECONCILE')
    price = rounded_lock
    quantity = life.text(remaining)
    index, decimals = wire.asset(metadata, 'XRP')
    if state['market'] != dict(asset_index=index, size_decimals=decimals):
        raise StopDispatchError('TESTNET_MARKET_CHANGED_REVIEW_REQUIRED')
    wire.precise(price, quantity, decimals)
    sequence = len(state['requests']) + 1
    cloid = '0x' + life.digest([VERSION, state['original_digest'], sequence])[:32]
    order = dict(a=index, b=True, p=price, s=quantity, r=True,
                 t=dict(trigger=dict(isMarket=True, triggerPx=price, tpsl='sl')), c=cloid)
    action = wire.canonical_wire_action(dict(type='batchModify', modifies=[
        dict(oid=int(old['oid']), order=order)]))
    return dict(version=VERSION, card_id=state['original_binding']['card_id'],
        account=state['original_binding']['account'], symbol='XRP', role='short_account',
        leg='STOP', operation='MODIFY_EXIT', quantity=quantity, old_oid=old['oid'],
        sequence=sequence, action=action, basis=life.digest(snapshot),
        observed_at_ms=snapshot['at_ms'], condition_digest=life.digest(state['condition']))


def reserve(state, metadata, sample, *, now_ms):
    """Persist returned state with CAS; no caller-supplied wire terms accepted."""
    result = validate(state)
    proposal = _decision(result, metadata, sample, now_ms=now_ms)
    if proposal is None:
        return result
    request_id = life.digest([VERSION, proposal])
    result['requests'].append(dict(request_id=request_id, proposal=proposal,
        phase='RESERVED', attempt_at_ms=None, reply=None, observed_oid=None, observed_at_ms=None))
    result['pending'] = request_id
    return validate(result)


def _pending(state, request_id):
    if state['pending'] != request_id:
        raise StopDispatchError('EXACT_PENDING_STOP_REQUEST_REQUIRED')
    return next(r for r in state['requests'] if r['request_id'] == request_id)


def begin(state, request_id, metadata, sample, *, now_ms):
    """Commit before sending ONCE. A crash after commit requires observation."""
    result = validate(state)
    request = _pending(result, request_id)
    if request['phase'] != 'RESERVED' or request['attempt_at_ms'] is not None:
        raise StopDispatchError('STOP_ATTEMPT_ALREADY_CONSUMED')
    # Rebuild using the same sequence. Changed quantity, evidence, condition,
    # asset index or mark makes the reservation stale, never rebases its intent.
    unreserved = deepcopy(result)
    unreserved['requests'].pop()
    unreserved['pending'] = None
    if _decision(unreserved, metadata, sample, now_ms=now_ms) != request['proposal']:
        raise StopDispatchError('STOP_OBSERVATION_CHANGED_REPLAN_REQUIRED')
    request.update(phase='ATTEMPTED', attempt_at_ms=now_ms)
    return validate(result)


def abandon_unsent(state, request_id):
    result = validate(state)
    request = _pending(result, request_id)
    if request['phase'] != 'RESERVED' or request['attempt_at_ms'] is not None:
        raise StopDispatchError('ATTEMPTED_STOP_CANNOT_BE_ABANDONED')
    request['phase'] = 'ABANDONED_UNSENT'
    result['pending'] = None
    return validate(result)


def record_reply(state, request_id, reply):
    result = validate(state)
    request = _pending(result, request_id)
    if request['phase'] != 'ATTEMPTED':
        raise StopDispatchError('STOP_REPLY_REPLAY_OR_NO_ATTEMPT')
    normalized = wire.normalized_reply(reply, 'batchModify')
    # Store only bounded classification fields, never venue error text.
    request['reply'] = {k: normalized[k] for k in ('state', 'code', 'oid')}
    request['phase'] = {'ACCEPTED_UNVERIFIED': 'ACKNOWLEDGED',
                        'OUTCOME_UNKNOWN': 'UNKNOWN', 'REJECTED': 'REJECTED'}[normalized['state']]
    return validate(result)


def _continues(previous, snapshot):
    if snapshot['at_ms'] < previous['at_ms']:
        raise StopDispatchError('STOP_EVIDENCE_CLOCK_REGRESSION')
    for collection, key in (('fills', 'fill_id'), ('terminal_orders', 'oid')):
        before = {row[key]: row for row in previous[collection]}
        after = {row[key]: row for row in snapshot[collection]}
        if any(after.get(k) != row for k, row in before.items()):
            raise StopDispatchError('STOP_IMMUTABLE_EVIDENCE_REGRESSION')


def observe(state, snapshot, *, now_ms, lookup=None):
    """Reconcile exact cloid lookup PLUS complete account evidence; never resend."""
    result = validate(state)
    life.validate_snapshot(snapshot)
    _continues(result['snapshot'], snapshot)
    if result['pending'] is None:
        _review(result, snapshot, now_ms=now_ms)
        result['snapshot'] = deepcopy(snapshot)
        return validate(result)
    request = _pending(result, result['pending'])
    if request['attempt_at_ms'] is None:
        _review(result, snapshot, now_ms=now_ms)
        result['snapshot'] = deepcopy(snapshot)
        return validate(result)
    if snapshot['at_ms'] <= request['attempt_at_ms']:
        raise StopDispatchError('POST_ATTEMPT_STOP_EVIDENCE_REQUIRED')
    if lookup == {'status': 'unknownOid'} and request['phase'] == 'REJECTED':
        view = _review(result, snapshot, now_ms=now_ms)
        old = [o for o in snapshot['open_orders'] if o['oid'] == request['proposal']['old_oid']]
        if len(old) != 1 or view['bucket_issues'] or set(view['cards'][0]['issues']) - REMEDIABLE:
            raise StopDispatchError('REJECTED_STOP_ORIGINAL_NOT_PROVEN_ACTIVE')
        request.update(phase='REJECTED_PROVEN', observed_at_ms=snapshot['at_ms'])
        result.update(pending=None, halted_reason='STOP_AMENDMENT_REJECTED', snapshot=deepcopy(snapshot))
        return validate(result)
    oid = wire.identity(lookup, request, now_ms)
    if lookup['order']['statusTimestamp'] > snapshot['at_ms']:
        raise StopDispatchError('STOP_LOOKUP_NEWER_THAN_SNAPSHOT')
    if oid in result['stop_terms'] or oid in {
            value for ids in result['original_binding']['orders'].values() for value in ids}:
        raise StopDispatchError('REPLACEMENT_ORDER_ID_NOT_NEW')
    terms = {**result['stop_terms'], oid: wire.requested_order(request['proposal']['action'])['p']}
    old_oid = request['proposal']['old_oid']
    endings = [o for o in snapshot['terminal_orders'] if o['oid'] == old_oid]
    if (any(o['oid'] == old_oid for o in snapshot['open_orders'])
            or len(endings) != 1 or endings[0]['state'] != 'CANCELED'
            or endings[0]['at_ms'] < request['attempt_at_ms']):
        raise StopDispatchError('AMENDED_ORIGINAL_STOP_NOT_TERMINAL')
    view = _review(result, snapshot, now_ms=now_ms, terms=terms)
    if view['bucket_issues'] or set(view['cards'][0]['issues']) - REMEDIABLE - {'FLAT_WITH_WORKING_ORDERS'}:
        raise StopDispatchError('REPLACEMENT_STOP_EVIDENCE_CONFLICT')
    own_fills = {f['fill_id']: f for f in snapshot['fills'] if f['oid'] == oid}
    filled = sum((life.number(f['quantity']) for f in own_fills.values()), life.number('0'))
    requested = life.number(request['proposal']['quantity'], positive=True)
    active = [o for o in snapshot['open_orders'] if o['oid'] == oid]
    terminal = [o for o in snapshot['terminal_orders'] if o['oid'] == oid]
    if (filled > requested or (len(active) == 1 and (
            life.number(active[0]['quantity'], positive=True) + filled != requested
            or lookup['order']['status'] != 'open'))
            or (len(terminal) == 1 and terminal[0]['state'] == 'FILLED' and filled != requested)):
        raise StopDispatchError('REPLACEMENT_STOP_QUANTITY_NOT_PROVEN')
    # No remaining-quantity claim comes from the receipt/lookup. A partial fill
    # can leave a replacement oversized; retain that observed fact, then plan
    # another exact-quantity amendment from a new complete snapshot.
    request.update(phase='OBSERVED', observed_oid=oid, observed_at_ms=snapshot['at_ms'])
    result.update(pending=None, stop_terms=terms, snapshot=deepcopy(snapshot))
    return validate(result)


def review(state, *, now_ms):
    state = validate(state)
    return _review(state, state['snapshot'], now_ms=now_ms)
