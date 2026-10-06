"""Disabled-by-default experimental source projection; no order/HTTP API.

The reader reuses durable producer state and never queries an exchange. MaxPain
evidence is captured in the same existing source transaction as its new plan,
before any touch. Legacy plans without this evidence cannot be backfilled into
execution. Only the explicit, default-off experimental_execution_forwarder
invokes this projection on its own schedule.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
import time

import experimental_execution_contract as contract

MODE = 'experimental_testnet_bridge_v1'
EVIDENCE_VERSION = 'prospective-maxpain-execution-evidence-v1'
MAX_SOURCE_BYTES = 1024 * 1024


def enabled(env=None):
    return (os.environ if env is None else env).get('EXPERIMENTAL_EXECUTION_BRIDGE_MODE') == MODE


def _maxpain_evidence(plan, state, decoded, considered, config_sha256):
    import sol_proximity_experimental_signal as signal
    if plan['status'] != 'PENDING':
        raise contract.ContractError('NEW_PENDING_PLAN_REQUIRED')
    row = next(r for r in decoded['rows'] if r['key'] == plan['episode_key'] and
               r['timeframe'] == plan['timeframe'] and r['source_side'] == plan['source_side'])
    episode = state['episodes'][plan['episode_key']]
    blockers = [p for p in considered if signal.near(p['target_price'], plan['target_price'])]
    blockers += list(episode.get('filled_legs', []))
    comparisons, seen = [], set()
    for previous in blockers:
        identity = (previous['target_price'], previous['timeframe'], previous['direction'])
        if identity in seen:
            continue
        seen.add(identity)
        if not signal.growth_allows(row, previous, decoded['rows']):
            raise contract.ContractError('SOURCE_GROWTH_PROOF_INCONSISTENT')
        start, end = signal.TIMEFRAMES.index(previous['timeframe']), signal.TIMEFRAMES.index(plan['timeframe'])
        by_tf = {r['timeframe']: r for r in decoded['rows']
                 if r['source_side'] == plan['source_side'] and r['provider_valid']}
        tiers = [dict(timeframe=tf, target_price=contract.price(by_tf[tf]['target_price']),
                      liquidation_amount=contract.price(by_tf[tf]['liquidation_amount']),
                      source_side=by_tf[tf]['source_side'], provider_valid=True)
                 for tf in signal.TIMEFRAMES[start:end + 1]]
        comparisons.append(dict(previous_target=contract.price(previous['target_price']),
            previous_timeframe=previous['timeframe'], previous_direction=previous['direction'], tiers=tiers))
    amount = row['liquidation_amount']
    if amount is None and comparisons:
        raise contract.ContractError('CLUSTER_INCOMING_AMOUNT_REQUIRED')
    return dict(version=EVIDENCE_VERSION,
        generated_ms=decoded['computed_ms'], captured_ms=plan['decision_ms'],
        source_contract_version=contract.SOURCE_CONTRACT, source_config_sha256=config_sha256,
        episode_first_ms=episode['first_ms'], liquidation_amount=None if amount is None else contract.price(amount),
        cluster_comparisons=comparisons)


def capture_maxpain_evidence(state, decoded, prior, *, config_sha256, max_bytes=768 * 1024):
    """Attach proof to NEW pending plans only, after the unchanged pure ingest.

    Actual adjacent source rows establish the growth chain; no scalar flag or
    maximum cluster score can replace these rows. State writes stay atomic with
    the existing admission transaction. This does not change strategy hashing.

    Missing evidence disables only the new execution plan. It must not roll
    back a valid notification observation, including when the proof is large.
    """
    def fits():
        return len(json.dumps(state, sort_keys=True, separators=(',', ':'),
                              ensure_ascii=False, allow_nan=False).encode()) <= max_bytes
    previous_ids = {p['position_id'] for p in prior['active']}
    considered = list(prior['active']) + list(prior.get('legacy_source_state', {}).get('active', []))
    for plan in state['active']:
        if plan['position_id'] in previous_ids:
            continue
        try:
            plan['execution_evidence'] = _maxpain_evidence(plan, state, decoded, considered, config_sha256)
            if not fits():
                raise contract.ContractError('EVIDENCE_CAPACITY')
        except (ValueError, TypeError, KeyError, StopIteration, ArithmeticError):
            plan.pop('execution_evidence', None)
            plan['execution_evidence_error'] = 'PROOF_UNAVAILABLE'
            # This best-effort fixed audit flag cannot itself exhaust the
            # original notification store's byte allowance.
            if not fits():
                plan.pop('execution_evidence_error', None)
        considered.append(plan)


def _base(family, rule, symbol, side, source, created, arm, expiry, entry, stop, take, target, policy, proof):
    value = dict(version=contract.VERSION, family=family, rule_id=rule, symbol=symbol, side=side,
        source_environment='mainnet', execution_environment='testnet', source_price_kind='TRADE_1M',
        source_at=contract.iso_ms(source), created_at=contract.iso_ms(created),
        arm_at=contract.iso_ms(arm), expires_at=None if expiry is None else contract.iso_ms(expiry), entry=contract.price(entry),
        stop=contract.price(stop), take_profit=contract.price(take),
        original_target=None if target is None else contract.price(target), policy=policy, proof=proof)
    value['occurrence_id'] = contract.occurrence_id(value)
    return value


def _event(base, *, kind, as_of, now, source_state='PENDING', reason=None):
    if as_of > now:
        raise contract.ContractError('FUTURE_SOURCE_STATE')
    expiry = contract.moment_ms(base['expires_at']) if base['expires_at'] is not None else as_of + contract.LEASE_MS
    lease = min(expiry, as_of + contract.LEASE_MS)
    if kind != 'CANCEL' and now >= lease:
        return None
    result = dict(base, kind=kind, source_sequence=as_of * 10 + contract.RANK[kind],
        source_as_of=contract.iso_ms(as_of), source_state=source_state,
        valid_until=contract.iso_ms(lease), cancel_reason=reason)
    return contract.validate(result)


def r2732_messages(state, *, now_ms, fence_ms):
    """Delivered fresh R2732 references with deterministic decision identity."""
    import xrp_r2732_experimental_store as store
    if (state.get('version') != store.STORE_VERSION or state.get('config_version') != store.CONFIG_VERSION
            or state.get('source_contract_version') != contract.SOURCE_CONTRACT):
        raise contract.ContractError('R2732_SOURCE_VERSION')
    result = []
    intents = state.get('intents')
    if not isinstance(intents, list) or len(intents) > 128:
        raise contract.ContractError('SOURCE_INTENTS_INVALID')
    for intent in intents:
        if intent.get('status') != 'DELIVERED':
            continue
        p = intent['payload']
        entry_at = contract.moment_ms(p['entry_at'])
        if entry_at < fence_ms or p.get('source_contract_version') != contract.SOURCE_CONTRACT:
            continue
        proof = dict(source_contract_version=p['source_contract_version'],
            source_config_sha256=p['config_sha256'], decision_at=p['decision_at'])
        policy = dict(name='R2732_CLOSED_MINUTE_LOCK_V1',
            original_risk_distance=contract.price(p['original_risk_distance']),
            lock_trigger_price=contract.price(p['lock_trigger_price']),
            locked_stop=contract.price(p['lock_stop_price']), trigger_R='2', profit_R='0.5',
            effective='NEXT_MINUTE', barrier_precedence='OLD_STOP_TAKE_FIRST', source_price=p['price_source'])
        base = _base('r2732', p['rule_id'], p['symbol'], p['direction'], entry_at, entry_at,
            entry_at, contract.moment_ms(intent['expires_at']), p['entry_price_decimal'],
            p['stop_price_decimal'], p['take_price_decimal'], None, policy, proof)
        active = state.get('active')
        matching = active and active['position_id'] == p['position_id']
        as_of = max(contract.moment_ms(intent['acknowledged_at']),
                    contract.moment_ms(active['closed_through']) if matching else entry_at)
        reason = None
        if now_ms >= contract.moment_ms(intent['expires_at']):
            as_of = max(as_of, contract.moment_ms(intent['expires_at']))
            reason = 'EXPIRED_REFERENCE'
        elif not matching:
            reason = 'SOURCE_OBSERVATION_ENDED'
        elif active.get('lock_triggered_at'):
            reason = 'SOURCE_LOCK_ALREADY_OBSERVED'
        elif active.get('monitor_status') not in ('AWAITING_CLOSED_BAR', 'VERIFIED'):
            reason = 'SOURCE_MONITOR_UNVERIFIED'
        elif contract.moment_ms(active['closed_through']) < now_ms // 60_000 * 60_000:
            reason = 'SOURCE_MONITOR_NOT_CAUGHT_UP'
        message = _event(base, kind='CANCEL' if reason else 'PLAN', as_of=as_of,
            now=now_ms, source_state='ENDED' if reason else 'PENDING', reason=reason)
        if message:
            result.append(message)
    return result


def hype_row71205_messages(state, *, now_ms, fence_ms):
    """Read only the approved live-source variant's delivered fresh reference.

    A delivered notification proves neither a Testnet fill nor executable price.
    Admission and real fills remain the receiver's responsibility.
    """
    import hype_row71205_experimental_store as store
    if (state.get('version') != store.STORE_VERSION or state.get('config_version') != store.CONFIG_VERSION
            or state.get('source_contract_version') != contract.SOURCE_CONTRACT
            or state.get('price_source') != store.PRICE_SOURCE):
        raise contract.ContractError('HYPE_ROW71205_SOURCE_VERSION')
    intents = state.get('intents')
    if not isinstance(intents, list) or len(intents) > 128:
        raise contract.ContractError('SOURCE_INTENTS_INVALID')
    result = []
    for intent in intents:
        if intent.get('status') != 'DELIVERED':
            continue
        p = intent['payload']
        reference = contract.moment_ms(p['entry_at'])
        if (reference < fence_ms or p.get('source_contract_version') != contract.SOURCE_CONTRACT
                or p.get('price_source') != store.PRICE_SOURCE):
            continue
        proof = dict(source_contract_version=p['source_contract_version'],
            source_config_sha256=p['config_sha256'], decision_at=p['decision_at'])
        policy = dict(name='HYPE_ROW71205_MARKET_NEXT_MINUTE_V1', entry_kind='MARKET_REFERENCE',
            capacity=1, holding_timeout=None, source_price=p['price_source'])
        base = _base('hype_row71205', p['rule_id'], p['symbol'], p['direction'],
            reference, reference, reference, contract.moment_ms(intent['expires_at']),
            p['entry_price_decimal'], p['stop_price_decimal'], p['take_price_decimal'], None, policy, proof)
        active = state.get('active')
        matching = active and active['position_id'] == p['position_id']
        as_of = max(contract.moment_ms(intent['acknowledged_at']),
                    contract.moment_ms(active['closed_through']) if matching else reference)
        reason = None
        if now_ms >= contract.moment_ms(intent['expires_at']):
            reason = 'EXPIRED_REFERENCE'
            as_of = max(as_of, contract.moment_ms(intent['expires_at']))
        elif not matching:
            reason = 'SOURCE_OBSERVATION_ENDED'
        elif active.get('monitor_status') not in ('AWAITING_CLOSED_BAR', 'VERIFIED'):
            reason = 'SOURCE_MONITOR_UNVERIFIED'
        elif contract.moment_ms(active['closed_through']) < now_ms // 60_000 * 60_000:
            reason = 'SOURCE_MONITOR_NOT_CAUGHT_UP'
        message = _event(base, kind='CANCEL' if reason else 'PLAN', as_of=as_of,
            now=now_ms, source_state='ENDED' if reason else 'PENDING', reason=reason)
        if message:
            result.append(message)
    return result


def sol_g65_messages(state, *, now_ms, fence_ms):
    """Project the existing pre-touch reservation without changing source state.

    No fixed pending expiry exists. A lease expires on missing source evidence;
    this execution-safety timeout never changes the notification observation.
    Initial admission additionally needs fresh intraminute pre-touch evidence:
    a PENDING source snapshot alone cannot prove the current minute's path.
    """
    import sol_g65_experimental_store as store
    if (state.get('version') != store.STORE_VERSION or state.get('config_version') != store.CONFIG_VERSION
            or state.get('source_contract_version') != contract.SOURCE_CONTRACT
            or state.get('price_source') != store.PRICE_SOURCE):
        raise contract.ContractError('SOL_G65_SOURCE_VERSION')
    history, active = state.get('history'), state.get('active')
    if not isinstance(history, list) or len(history) > 256 or (active is not None and not isinstance(active, dict)):
        raise contract.ContractError('SOURCE_POSITIONS_INVALID')
    result = []
    for p in ([active] if active else []) + history:
        reference, reserved = map(contract.moment_ms, (p['reference_at'], p['reserved_at']))
        if (min(reference, reserved) < fence_ms or p.get('source_contract_version') != contract.SOURCE_CONTRACT
                or p.get('price_source') != store.PRICE_SOURCE):
            continue
        proof = dict(source_contract_version=p['source_contract_version'],
            source_config_sha256=p['config_sha256'], decision_at=p['decision_at'],
            reference_price=contract.price(p['reference_price']))
        policy = dict(name='SOL_G65_CLOSED_MINUTE_LOCK_V1', entry_kind='LIMIT_PRETOUCH', capacity=1,
            pending_timeout=None, holding_timeout=None, pending_cancel_price=contract.price(p['cancel_price']),
            original_take_distance=contract.price(p['original_take_distance']),
            lock_trigger_price=contract.price(p['lock_trigger_price']), locked_stop=contract.price(p['lock_stop_price']),
            trigger_take_fraction='0.75', profit_take_fraction='0.25', effective='NEXT_MINUTE',
            entry_bar_trigger='CLOSE_ONLY', later_bar_trigger='LOW',
            barrier_precedence='OLD_STOP_TAKE_FIRST', source_price=p['price_source'])
        base = _base('sol_g65', p['rule_id'], p['symbol'], p['direction'], reference, reference, reference,
            None, p['entry_price'], p['original_stop_price'], p['take_price'], None, policy, proof)
        as_of = max(reserved, contract.moment_ms(p['closed_through']),
                    contract.moment_ms(p['closed_at']) if p.get('closed_at') else 0)
        reason = None
        if p.get('outcome'):
            reason = 'SOURCE_OBSERVATION_ENDED'
        elif not active or active['position_id'] != p['position_id']:
            reason = 'SOURCE_OBSERVATION_ENDED'
        elif p.get('phase') != 'PENDING':
            reason = 'SOURCE_ENTRY_OBSERVED'
        elif p.get('monitor_status') not in ('AWAITING_CLOSED_BAR', 'VERIFIED'):
            reason = 'SOURCE_MONITOR_UNVERIFIED'
        elif contract.moment_ms(p['closed_through']) < now_ms // 60_000 * 60_000:
            reason = 'SOURCE_MONITOR_NOT_CAUGHT_UP'
        kind = 'CANCEL' if reason else ('PLAN' if now_ms < reference + 60_000 else 'HEARTBEAT')
        message = _event(base, kind=kind, as_of=as_of, now=now_ms,
            source_state='ENDED' if reason else 'PENDING', reason=reason)
        if message:
            result.append(message)
    return result


def maxpain_messages(state, *, now_ms, fence_ms):
    """Only evidence captured at creation can create a prospective plan.

    A later heartbeat can maintain a previously admitted plan, never create it.
    A source OPEN record cancels the remaining demo entry; it is not a demo fill.
    """
    if state.get('version') != 'sol-proximity-notification-store-v1':
        raise contract.ContractError('MAXPAIN_SOURCE_VERSION')
    result = []
    for p in state.get('active', []) + state.get('history', []):
        if (p.get('rule_id') not in contract.SPECS or p.get('legacy_formula')
                or p.get('manual_source_restore')):
            continue
        evidence = p.get('execution_evidence')
        if not evidence:
            continue
        if evidence.get('version') != EVIDENCE_VERSION:
            raise contract.ContractError('MAXPAIN_EVIDENCE_VERSION')
        if evidence['captured_ms'] < fence_ms or evidence['captured_ms'] >= p['arm_ms']:
            continue
        proof = {key: deepcopy(evidence[key]) for key in
                 ('source_contract_version', 'source_config_sha256', 'episode_first_ms',
                  'liquidation_amount', 'cluster_comparisons')}
        proof.update(cycle_id=p['cycle_id'], episode_key=p['episode_key'],
            episode_generation=p['episode_generation'], timeframe=p['timeframe'], source_side=p['source_side'],
            source_observed_ms=p['source_observed_ms'], source_quote=deepcopy(p['source_quote']),
            range24_low=None if p['range24_low'] is None else contract.price(p['range24_low']),
            range24_high=None if p['range24_high'] is None else contract.price(p['range24_high']),
            source_price=contract.price(p['source_price']))
        policy = dict(name='MAXPAIN_LIMIT_PRETOUCH_V1', entry_adverse=contract.price(p['entry_adverse']),
            take_fraction=contract.price(p['take_fraction']), stop_distance_multiplier='5',
            overlap_target_fraction='0.002', liquidity_growth=p['liquidity_growth'],
            source_price=p['source_quote']['price_source'])
        base = _base('maxpain', p['rule_id'], p['coin'], 'LONG' if p['direction'] == 1 else 'SHORT',
            p['source_observed_ms'], evidence['generated_ms'], p['arm_ms'], p['expires_ms'],
            p['entry_price'], p['stop_price'], p['take_price'], p['target_price'], policy, proof)
        # Source snapshots cannot renew a lease when closed-price monitoring
        # has a gap. Only creation evidence or contiguous monitored bars may.
        as_of = max(evidence['captured_ms'], state['bar_cursor_ms'] + 60_000,
                    p.get('terminal_ms') or 0)
        reason = None
        if now_ms >= p['expires_ms']:
            reason, as_of = 'EXPIRED_PENDING', max(as_of, p['expires_ms'])
        elif p['status'] != 'PENDING':
            reason = 'SOURCE_ENTRY_OBSERVED' if p['status'] == 'OPEN' else p['status']
        else:
            episode = state['episodes'].get(p['episode_key'])
            if episode and episode.get('touched_ms'):
                reason = 'SOURCE_TARGET_TOUCHED'
                as_of = max(as_of, episode['touched_ms'])
        kind = 'CANCEL' if reason else ('PLAN' if now_ms < p['arm_ms'] else 'HEARTBEAT')
        message = _event(base, kind=kind, as_of=as_of, now=now_ms,
            source_state=p['status'], reason=reason)
        if message:
            result.append(message)
    return result


def read_experimental(scopes, fence, *, now=None, env=None, connect=None, deadline_monotonic=None, clock=None):
    """Opt-in read-only API for a separately scheduled, reviewed bridge.

    Disabled means no DB import/connection. The separately configured
    experimental_execution_forwarder uses the read-only source connection.
    """
    if not enabled(env):
        return []
    clock = clock or time.monotonic
    def deadline():
        if deadline_monotonic is not None and clock() >= deadline_monotonic:
            raise contract.ContractError('SOURCE_READ_DEADLINE')
    deadline()
    from alert_cards_forwarder import _source_dsn
    if connect is None:
        import psycopg
        from psycopg.rows import dict_row
        connect = lambda dsn, **kw: psycopg.connect(dsn, row_factory=dict_row, **kw)
    if now is None:
        current = datetime.now(timezone.utc)
        now = current.replace(microsecond=current.microsecond // 1000 * 1000)
    now_ms, fence_ms = contract.moment_ms(now.isoformat()), contract.moment_ms(fence.isoformat())
    result = []
    with connect(_source_dsn(), connect_timeout=3,
                 options='-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=1000') as conn:
        for scope in sorted(scopes):
            if not isinstance(scope, str) or not contract.HEX.fullmatch(scope):
                raise contract.ContractError('SOURCE_SCOPE_HASH')
            subscription = 'general-watch:' + scope
            hashed_subscription = hashlib.sha256(subscription.encode()).hexdigest()
            keys = [('xrp-r2732-weekdays-lock-cap1-v1:' + hashed_subscription, r2732_messages),
                    ('hype-row71205-experimental-cap1-v1:' + hashed_subscription, hype_row71205_messages),
                    ('sol-g65-k49-lock-cap1-v1:' + hashed_subscription, sol_g65_messages)]
            for rule, spec in contract.SPECS.items():
                source_scope = subscription if spec[0] == 'SOL' else subscription + ':' + rule
                keys.append(('sol-proximity-notification-store-v1:' + hashlib.sha256(source_scope.encode()).hexdigest(), maxpain_messages))
            for key, builder in keys:
                deadline()
                row = conn.execute('SELECT value FROM bot_settings WHERE key=%s', (key,)).fetchone()
                if row is None:
                    continue
                raw = row['value']
                if not isinstance(raw, str) or len(raw.encode()) > MAX_SOURCE_BYTES:
                    raise contract.ContractError('SOURCE_STATE_TOO_LARGE')
                state = json.loads(raw)
                result.extend(builder(state, now_ms=now_ms, fence_ms=fence_ms))
    # Duplicate recipients must never manufacture additional occurrences. Keep
    # the freshest lease if immutable content agrees; fail closed on conflict.
    by_id = {}
    for value in result:
        previous = by_id.get(value['occurrence_id'])
        if previous and contract.plan_digest(previous) != contract.plan_digest(value):
            raise contract.ContractError('CONFLICTING_SOURCE_OCCURRENCE')
        # One recipient's observed cancellation is irreversible even if a
        # second, stale recipient later emits a numerically newer heartbeat.
        if (not previous or (value['kind'] == 'CANCEL' and previous['kind'] != 'CANCEL')
                or (value['kind'] == previous['kind'] and value['source_sequence'] > previous['source_sequence'])
                or (previous['kind'] != 'CANCEL' and value['kind'] != 'CANCEL'
                    and value['source_sequence'] > previous['source_sequence'])):
            by_id[value['occurrence_id']] = value
    return list(by_id.values())
