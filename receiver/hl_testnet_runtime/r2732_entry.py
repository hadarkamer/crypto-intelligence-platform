"""Record-only R2732 entry admission. No sender, signer, timer or live hook.

Persist every transition with compare-and-swap under the account/symbol lane.
The ownership and price ranges here must come from the trusted bot collectors;
neither Telegram delivery nor producer hypothetical positions prove demo fills.
An unsigned proposal is not permission to send or proof of protective orders.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN, localcontext

import experimental_execution_contract as contract_api
from . import card_lifecycle as life, r2732_conditional_stop as condition
from . import r2732_stop_dispatch as stops
from .experimental_plan_store import reduce_source
from .risk_policy import budget, VERSION as RISK_VERSION

VERSION = 'r2732-entry-admission-software-v1'
FINAL = frozenset(('CLOSED', 'CANCELED_WITHOUT_FILL', 'REJECTED_FINAL'))
PHASES = FINAL | frozenset(('PREPARED', 'OUTCOME_UNKNOWN', 'ACK_UNVERIFIED',
                           'OPEN', 'PARTIALLY_OPEN', 'PARTIALLY_CLOSED'))


class EntryError(life.LifecycleError):
    """Fixed local errors only."""


def initial(routes, *, not_before_ms):
    life.moment(not_before_ms)
    try:
        account = life.address(routes['short_account']['account'])
        other = life.address(routes['long_account']['account'])
        if account == other:
            raise EntryError('R2732_INDEPENDENT_ACCOUNTS_REQUIRED')
    except (KeyError, TypeError):
        raise EntryError('R2732_TWO_EXPLICIT_ACCOUNT_ROUTES_REQUIRED') from None
    return dict(version=VERSION, environment='testnet', account=account,
        account_role='short_account', not_before_ms=not_before_ms,
        latest_source_ms=0, revision=0, records={})


def _copy(state):
    life.shape(state, 'version environment account account_role not_before_ms '
               'latest_source_ms revision records')
    if (state['version'] != VERSION or state['environment'] != 'testnet'
            or state['account_role'] != 'short_account'
            or type(state['revision']) is not int or state['revision'] < 0
            or type(state['latest_source_ms']) is not int or state['latest_source_ms'] < 0
            or not isinstance(state['records'], dict) or len(state['records']) > 4096):
        raise EntryError('R2732_ENTRY_STATE_INVALID')
    life.address(state['account']); life.moment(state['not_before_ms'])
    latest = 0
    for cid, record in state['records'].items():
        life.ident(cid, r'[0-9a-f]{64}')
        life.shape(record, 'source_record request')
        source = record['source_record']
        message = contract_api.validate(source['source'])
        if (message['family'] != 'r2732' or message['occurrence_id'] != cid
                or source['occurrence_id'] != cid or source['domain'] != 'software'
                or source['plan_digest'] != contract_api.plan_digest(message)
                or source['source_sequence'] != message['source_sequence']):
            raise EntryError('R2732_SOURCE_STATE_CHANGED')
        latest = max(latest, contract_api.moment_ms(message['source_at']))
        request = record['request']
        if request is not None:
            life.shape(request, 'request_id attempted_at_ms proposal phase')
            life.moment(request['attempted_at_ms'])
            if (request['phase'] != 'OUTCOME_UNKNOWN'
                    or request['request_id'] != request['proposal']['proposal_id']
                    or request['proposal']['occurrence_id'] != cid
                    or life.digest({k: v for k, v in request['proposal'].items()
                                    if k != 'proposal_id'}) != request['request_id']):
                raise EntryError('R2732_ENTRY_REQUEST_CHANGED')
    if latest != state['latest_source_ms']:
        raise EntryError('R2732_SOURCE_WATERMARK_CHANGED')
    return deepcopy(state)


def receive(state, message, *, now_ms):
    """Record a new reference or monotonic source update; retain all attempts."""
    result = _copy(state); life.moment(now_ms)
    value = contract_api.validate(message)
    if value['family'] != 'r2732':
        raise EntryError('R2732_CONTRACT_REQUIRED')
    cid = value['occurrence_id']; previous = result['records'].get(cid)
    reference = contract_api.moment_ms(value['source_at'])
    if (previous is None and value['kind'] != 'CANCEL'
            and reference < result['latest_source_ms']):
        raise EntryError('R2732_OLDER_REFERENCE_NO_REPLAY')
    if previous is None and len(result['records']) >= 4096:
        raise EntryError('R2732_DEDUP_CAPACITY_REQUIRES_REVIEW')
    source, changed = reduce_source(None if previous is None else previous['source_record'],
        value, now=contract_api.iso_ms(now_ms),
        not_before=contract_api.iso_ms(result['not_before_ms']), domain='software')
    if not changed:
        return result
    result['records'][cid] = dict(source_record=source,
                                 request=None if previous is None else previous['request'])
    result['latest_source_ms'] = max(result['latest_source_ms'], reference)
    result['revision'] += 1
    return _copy(result)


def _ownership(state, evidence, *, now_ms):
    life.shape(evidence, 'environment account account_role at_ms complete unresolved_request occurrences')
    if (evidence['environment'] != 'testnet' or evidence['account'] != state['account']
            or evidence['account_role'] != 'short_account' or evidence['complete'] is not True
            or evidence['unresolved_request'] is not False
            or not isinstance(evidence['occurrences'], list)
            or len(evidence['occurrences']) > 4096):
        raise EntryError('R2732_COMPLETE_SETTLED_DEMO_OWNERSHIP_REQUIRED')
    life.moment(evidence['at_ms'])
    if not 0 <= now_ms-evidence['at_ms'] <= 15000:
        raise EntryError('R2732_FRESH_DEMO_OWNERSHIP_REQUIRED')
    rows = {}
    for row in evidence['occurrences']:
        life.shape(row, 'occurrence_id request_id family symbol phase remaining_quantity working_orders')
        cid = life.ident(row['occurrence_id'], r'[0-9a-f]{64}')
        life.ident(row['request_id'], r'[0-9a-f]{64}')
        life.ident(row['family']); life.ident(row['symbol'], r'[A-Z][A-Z0-9]{0,19}')
        remaining = life.number(row['remaining_quantity'])
        if (cid in rows or row['phase'] not in PHASES or type(row['working_orders']) is not bool
                or row['phase'] in FINAL and (remaining != 0 or row['working_orders'])):
            raise EntryError('R2732_DEMO_OCCURRENCE_EVIDENCE_INVALID')
        rows[cid] = row
    for cid, record in state['records'].items():
        request = record['request']
        if request is None:
            continue
        observed = rows.get(cid)
        if (observed is None or observed['request_id'] != request['request_id']
                or observed['family'] != 'r2732' or observed['symbol'] != 'XRP'
                or evidence['at_ms'] < request['attempted_at_ms']
                or observed['phase'] in ('PREPARED', 'OUTCOME_UNKNOWN', 'ACK_UNVERIFIED')):
            raise EntryError('R2732_PRIOR_ATTEMPT_UNRESOLVED')
    return rows


def _range(value, *, now_ms, reference, source, account=None):
    fields = ('environment symbol reference_at_ms at_ms history_complete high low price '
              + ('price_source' if source else 'account price_kind'))
    life.shape(value, fields)
    if (value['environment'] != ('mainnet' if source else 'testnet')
            or value['symbol'] != 'XRP' or value['reference_at_ms'] != reference
            or value['history_complete'] is not True
            or source and value['price_source'] != condition.SOURCE
            or not source and (value['account'] != account or value['price_kind'] != 'MARK')):
        raise EntryError('R2732_REFERENCE_PRICE_EVIDENCE_REQUIRED')
    life.moment(value['at_ms'])
    if not reference <= value['at_ms'] <= now_ms or now_ms-value['at_ms'] > 15000:
        raise EntryError('R2732_FRESH_REFERENCE_PRICE_EVIDENCE_REQUIRED')
    low, high, price = (life.number(value[k], positive=True) for k in ('low', 'high', 'price'))
    if not low <= price <= high:
        raise EntryError('R2732_REFERENCE_PRICE_RANGE_INVALID')
    return low, high, price


def entry_admission(state, cid, metadata, market, source_market, ownership, *, now_ms):
    """Prepare one unsigned entry at the frozen reference, under unchanged risk.

    Incomplete current source bars may veto stale entry. They never count as a
    completed formula candle or as a demo trade. The source and demo ranges are
    separately required and cannot be substituted for each other.
    """
    state = _copy(state); life.moment(now_ms)
    life.ident(cid, r'[0-9a-f]{64}')
    if cid not in state['records']:
        raise EntryError('R2732_KNOWN_FRESH_REFERENCE_REQUIRED')
    record = state['records'][cid]; stored = record['source_record']
    value = contract_api.validate(stored['source'])
    reference = contract_api.moment_ms(value['source_at'])
    if record['request'] is not None:
        raise EntryError('R2732_OCCURRENCE_ALREADY_ATTEMPTED')
    if reference != state['latest_source_ms'] or reference < state['not_before_ms']:
        raise EntryError('R2732_LATEST_NEW_REFERENCE_REQUIRED')
    if (stored['entry_permission'] != 'WAITING' or stored.get('cancellation') is not None
            or value['kind'] == 'CANCEL' or value['source_state'] != 'PENDING'):
        raise EntryError('R2732_SOURCE_ENTRY_PERMISSION_RETIRED')
    if (not reference <= now_ms < reference+90000
            or now_ms >= min(contract_api.moment_ms(value['expires_at']),
                             contract_api.moment_ms(value['valid_until']))
            or contract_api.moment_ms(value['source_as_of']) > now_ms):
        raise EntryError('R2732_ORIGINAL_WINDOW_OR_SOURCE_LEASE_EXPIRED')
    owned = _ownership(state, ownership, now_ms=now_ms)
    if cid in owned:
        raise EntryError('R2732_OCCURRENCE_ALREADY_KNOWN_IN_DEMO')
    for row in owned.values():
        if row['phase'] not in FINAL:
            if row['phase'] in ('PREPARED', 'OUTCOME_UNKNOWN', 'ACK_UNVERIFIED'):
                raise EntryError('R2732_PRIOR_ATTEMPT_UNRESOLVED')
            if row['family'] == 'r2732':
                raise EntryError('R2732_DEMO_FORMULA_CAP_ONE')
            if row['symbol'] == 'XRP':
                raise EntryError('R2732_SHARED_MARKET_PREDECESSOR_NOT_FINAL')
    execution = stops.prepare_execution(value, metadata)
    source_low, source_high, _ = _range(source_market, now_ms=now_ms, reference=reference, source=True)
    low, high, mark = _range(market, now_ms=now_ms, reference=reference,
                            source=False, account=state['account'])
    levels = condition.frozen_levels(value['entry'])
    if source_high >= Decimal(levels['initial_stop']) or source_low <= Decimal(levels['take_profit']):
        raise EntryError('R2732_SOURCE_INITIAL_EXIT_ALREADY_REACHED')
    if (float(levels['entry'])-float(source_low))/float(levels['original_risk_distance']) >= 2:
        raise EntryError('R2732_SOURCE_LOCK_ALREADY_REACHED')
    prices = execution['prepared']['execution']
    entry, stop, take = (life.number(prices[k], positive=True) for k in ('entry', 'stop', 'take_profit'))
    # The original 2R rule also vetoes an already-consumed demo path; use frozen
    # original risk and the approved rounding only for executable barrier prices.
    trigger = life.number(stops.price_precision.round_price(levels['lock_trigger_price'],
                            execution['market']['size_decimals']), positive=True)
    if high >= stop or low <= take:
        raise EntryError('R2732_TESTNET_INITIAL_EXIT_ALREADY_REACHED')
    if low <= trigger:
        raise EntryError('R2732_TESTNET_LOCK_ALREADY_REACHED')
    if not take < mark < stop:
        raise EntryError('R2732_TESTNET_PRICE_OUTSIDE_ORIGINAL_EXITS')
    with localcontext() as ctx:
        ctx.prec = 60
        step = Decimal(1).scaleb(-execution['market']['size_decimals'])
        quantity = ((budget()/abs(entry-stop))/step).to_integral_value(rounding=ROUND_DOWN)*step
        if quantity <= 0 or quantity*entry < 10:
            raise EntryError('R2732_VALID_MINIMUM_ENTRY_SIZE_REQUIRED')
        risk = quantity*abs(entry-stop)
    proposal = dict(version=VERSION, kind='UNSIGNED_ENTRY', environment='testnet',
        account=state['account'], account_role='short_account', occurrence_id=cid,
        source=deepcopy(value), execution=execution, quantity=life.text(quantity),
        planned_risk_usd=life.text(risk), risk_policy=RISK_VERSION,
        state_revision=state['revision'], state_digest=life.digest(state),
        source_sequence=value['source_sequence'], prepared_at_ms=now_ms,
        evidence_digest=life.digest([market, source_market, ownership]),
        dispatch_enabled=False, order_requests_sent=0)
    proposal['proposal_id'] = life.digest(proposal)
    return proposal


def begin_entry(state, proposal, metadata, market, source_market, ownership, *, now_ms):
    """Consume once in the caller's CAS transaction; never returns a wire order."""
    result = _copy(state); life.moment(now_ms)
    if (not isinstance(proposal, dict) or proposal.get('state_revision') != result['revision']
            or proposal.get('state_digest') != life.digest(result)
            or proposal.get('proposal_id') != life.digest({k: v for k, v in proposal.items()
                                                         if k != 'proposal_id'})
            or type(proposal.get('prepared_at_ms')) is not int
            or not 0 <= now_ms-proposal['prepared_at_ms'] <= 15000):
        raise EntryError('R2732_EXACT_FRESH_REVISION_PROPOSAL_REQUIRED')
    expected = entry_admission(result, proposal['occurrence_id'], metadata, market,
                               source_market, ownership, now_ms=now_ms)
    changing = {'proposal_id', 'prepared_at_ms', 'evidence_digest'}
    if {k: v for k, v in proposal.items() if k not in changing} != {
            k: v for k, v in expected.items() if k not in changing}:
        raise EntryError('R2732_ENTRY_REVALIDATION_FAILED')
    result['records'][proposal['occurrence_id']]['request'] = dict(
        request_id=proposal['proposal_id'], attempted_at_ms=now_ms,
        proposal=deepcopy(proposal), phase='OUTCOME_UNKNOWN')
    result['revision'] += 1
    return _copy(result)
