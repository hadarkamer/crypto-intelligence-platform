"""Project durable, engine-approved MaxPain notifications without strategy math.

Approval is a completed source decision, not a Telegram delivery acknowledgement
and never an exchange fill. The displayed prices are copied with the renderer's
existing eight-significant-digit formatting. Pending source plans cannot enter
this path. The old prospective proof/lease is deliberately not required.
"""
import approved_alert_contract as contract

FORMULA_STORES = {
    "xrp-r2732-weekdays-lock-cap1-v1": "r2732",
    "hype-row71205-experimental-cap1-v1": "hype_row71205",
    "sol-g65-k49-lock-cap1-v1": "sol_g65",
}

MODE = 'experimental_approved_alert_bridge_v2'
SOURCE_VERSION = 'sol-proximity-notification-store-v1'


def displayed_price(value):
    """Exactly the numeric text in the existing MaxPain Telegram renderer."""
    return contract.price(format(value, '.8g'))


def _previous_source_intent(state, payload, approved_ms, now_ms):
    """Recognize the documented predecessor, without rewriting its prices.

    Source migration retains old notification intents for history. They are
    not approvals under the new source configuration. Previously admitted
    outbox rows still withdraw through synchronize's stored-payload path.
    Incomplete migration evidence grants no exemption from normal validation.
    """
    migration = state.get('source_migration')
    previous = state.get('legacy_source_state')
    if not isinstance(migration, dict) or not isinstance(previous, dict):
        return False
    cutoff, decision = migration.get('migrated_ms'), payload.get('decision_ms')
    old, new = migration.get('from_config_sha256'), migration.get('to_config_sha256')
    return (migration.get('policy') == 'CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE'
        and type(cutoff) is int and 0 < cutoff <= now_ms
        and cutoff == state.get('source_activated_ms')
        and type(decision) is int and 0 < decision <= approved_ms < cutoff
        and migration.get('coin') == payload.get('coin')
        and migration.get('new_price_source') == f"HYPERLIQUID_{payload.get('coin')}_PERPETUAL_TRADE_1M"
        and isinstance(old, str) and contract.HEX.fullmatch(old) is not None
        and isinstance(new, str) and contract.HEX.fullmatch(new) is not None
        and old != new and new == state.get('config_sha256')
        and old == previous.get('from_config_sha256'))


def approved_messages(state, *, now_ms, fence_ms):
    if state.get('version') not in (SOURCE_VERSION, *FORMULA_STORES):
        raise contract.ContractError('MAXPAIN_SOURCE_VERSION')
    intents = state.get('intents')
    if not isinstance(intents, list) or len(intents) > 1024:
        raise contract.ContractError('SOURCE_INTENTS_INVALID')
    positions = source_positions(state)
    current = {p['position_id']: p for p in positions}
    messages = []
    for intent in intents:
        value = _approved_intent(intent, state, current, now_ms=now_ms, fence_ms=fence_ms)
        if value is not None:
            messages.append(value)
    return messages


def _approved_intent(intent, state, current, *, now_ms, fence_ms):
    if state.get('version') in FORMULA_STORES:
        return _formula_intent(intent, state, current, now_ms=now_ms, fence_ms=fence_ms)
    p = intent.get('payload', {})
    if (p.get('rule_id') not in contract.SPECS or p.get('legacy_formula')
            or p.get('manual_source_restore') or p.get('status') != 'OPEN'):
        return None
    if (intent.get('position_id') != p.get('position_id')
            or intent.get('intent_id') != p.get('position_id')):
        raise contract.ContractError('APPROVED_SOURCE_IDENTITY')
    # The source intent is created only after this whole minute has closed.
    # Never replace this with forwarding/Telegram time on retry or restart.
    approved = p['fill_ms'] + 60_000
    if approved < fence_ms or approved > now_ms:
        return None
    if _previous_source_intent(state, p, approved, now_ms):
        return None
    expiry = intent['expires_ms']
    if expiry != approved + contract.LEASE_MS:
        raise contract.ContractError('APPROVED_SOURCE_EXPIRY')
    observed = current.get(p['position_id'])
    reason = None
    as_of = approved
    if intent.get('status') == 'CANCELLED_STALE_PRICE':
        reason = 'SOURCE_STALE_PRICE'
        as_of = max(approved, intent['acknowledged_ms'])
    elif observed is None:
        # A durable approval whose source lifecycle has ended must never
        # become another entry. This only withdraws remaining entry orders.
        reason = 'SOURCE_OBSERVATION_ENDED'
        as_of = max(approved, state['bar_cursor_ms'] + 60_000)
    elif observed.get('status') != 'OPEN':
        reason = 'SOURCE_OBSERVATION_ENDED'
        as_of = max(approved, (observed.get('terminal_ms') or
                    observed.get('unknown_ms') or state['bar_cursor_ms']) + 60_000)
    if as_of > now_ms:
        raise contract.ContractError('FUTURE_SOURCE_STATE')
    if not reason and now_ms >= expiry:
        # Admission freshness is finite. Working GTC orders have no lease
        # and must not be canceled merely because this deadline elapsed.
        return None
    proof = dict(source_contract_version=contract.SOURCE_CONTRACT,
        source_config_sha256=state['config_sha256'], cycle_id=p['cycle_id'],
        episode_key=p['episode_key'], timeframe=p['timeframe'])
    kind = 'CANCEL' if reason else 'ALERT'
    value = dict(version=contract.VERSION, kind=kind, family='maxpain',
        rule_id=p['rule_id'], symbol=p['coin'], side='LONG' if p['direction'] == 1 else 'SHORT',
        source_environment='mainnet', execution_environment='testnet', source_price_kind='TRADE_1M',
        source_at=contract.iso_ms(p['source_observed_ms']), created_at=contract.iso_ms(p['decision_ms']),
        approved_at=contract.iso_ms(approved), arm_at=contract.iso_ms(approved),
        expires_at=contract.iso_ms(expiry), entry=displayed_price(p['fill_price']),
        stop=displayed_price(p['stop_price']), take_profit=displayed_price(p['take_price']),
        original_target=contract.price(p['target_price']),
        policy=dict(name='APPROVED_ALERT_LIMIT_GTC_V1', entry_kind='LIMIT_AFTER_ALERT',
                    source_price=p['source_quote']['price_source']), proof=proof,
        source_sequence=as_of*10+contract.RANK[kind], source_as_of=contract.iso_ms(as_of),
        source_state='CANCELED' if reason else 'APPROVED', valid_until=None, cancel_reason=reason)
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)


def isolated_records(state, *, now_ms, fence_ms):
    """Isolate invalid occurrence payloads; structural source failures stay fatal.

    Original source evidence is retained unchanged. Failed items are neither
    acknowledged nor assigned new identities/deadlines. Retained outbox rows
    independently preserve cancellation through their original signed plan.
    """
    if state.get('version') not in (SOURCE_VERSION, *FORMULA_STORES):
        raise contract.ContractError('MAXPAIN_SOURCE_VERSION')
    intents = state.get('intents')
    if not isinstance(intents, list) or len(intents) > 1024:
        raise contract.ContractError('SOURCE_INTENTS_INVALID')
    current = {p['position_id']: p for p in source_positions(state)}
    records, positions, errors, blocked = {}, {}, [], set()
    for intent in intents:
        try:
            value = _approved_intent(intent, state, current, now_ms=now_ms, fence_ms=fence_ms)
            if value is None:
                continue
            identity = value['occurrence_id']
            if identity in blocked:
                continue
            previous = records.get(identity)
            if previous and (contract.plan_digest(previous) != contract.plan_digest(value) or
                    (previous['source_sequence'] == value['source_sequence'] and previous != value)):
                blocked.add(identity)
                records.pop(identity, None)
                positions.pop(identity, None)
                raise contract.ContractError('CONFLICTING_SOURCE_OCCURRENCE')
            if previous and (previous['kind'] == 'CANCEL' or
                    (value['kind'] != 'CANCEL' and value['source_sequence'] <= previous['source_sequence'])):
                continue
            records[identity] = value
            positions[identity] = intent['position_id']
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as exc:
            errors.append(type(exc).__name__)
    return records, positions, errors


def source_positions(state):
    active = state.get('active')
    return (active if isinstance(active, list) else [active] if active else []) + state.get('history', [])


def source_open(state, position):
    if position is None:
        return False
    if state['version'] == SOURCE_VERSION:
        return position.get('status') == 'OPEN'
    return (position.get('outcome') is None and not position.get('closed_at')
            and (state['version'] != 'sol-g65-k49-lock-cap1-v1' or position.get('phase') == 'OPEN'))


def source_end_ms(state, position, value):
    approved = contract.moment_ms(value['approved_at'])
    if state['version'] == SOURCE_VERSION:
        return max(approved, ((position or {}).get('terminal_ms') or
            (position or {}).get('unknown_ms') or state['bar_cursor_ms']) + 60_000)
    stamps = [(position or {}).get(k) for k in ('exit_at', 'closed_at', 'closed_through')]
    if not position:
        stamps.append(state.get('last_exit_at'))
    return max([approved] + [contract.moment_ms(stamp) for stamp in stamps if stamp])


def _formula_intent(intent, state, current, *, now_ms, fence_ms):
    family = FORMULA_STORES[state['version']]
    p = intent['payload']
    if (state.get('source_contract_version') != contract.SOURCE_CONTRACT
            or p.get('source_contract_version') != contract.SOURCE_CONTRACT
            or p.get('config_sha256') != state.get('config_sha256')
            or p.get('price_source') != state.get('price_source')):
        return None  # Pre-migration observations cannot become new approvals.
    if intent['position_id'] != p['position_id']:
        raise contract.ContractError('APPROVED_SOURCE_IDENTITY')
    if family == 'sol_g65':
        if p.get('phase') != 'OPEN' or not p.get('fill_confirmed_at'):
            return None
        approved = contract.moment_ms(p['fill_confirmed_at'])
        reference = contract.moment_ms(p['reference_at'])
        entry, stop, take = (contract.price(p[k]) for k in ('entry_price', 'original_stop_price', 'take_price'))
        policy = dict(name='SOL_G65_CLOSED_MINUTE_LOCK_V1', entry_kind='LIMIT_PRETOUCH', capacity=1,
            pending_timeout=None, holding_timeout=None, pending_cancel_price=contract.price(p['cancel_price']),
            original_take_distance=contract.price(p['original_take_distance']),
            lock_trigger_price=contract.price(p['lock_trigger_price']), locked_stop=contract.price(p['lock_stop_price']),
            trigger_take_fraction='0.75', profit_take_fraction='0.25', effective='NEXT_MINUTE',
            entry_bar_trigger='CLOSE_ONLY', later_bar_trigger='LOW', barrier_precedence='OLD_STOP_TAKE_FIRST',
            source_price=p['price_source'])
    else:
        approved = reference = contract.moment_ms(p['entry_at'])
        entry, stop, take = (p[k] for k in ('entry_price_decimal', 'stop_price_decimal', 'take_price_decimal'))
        if family == 'r2732':
            policy = dict(name='R2732_CLOSED_MINUTE_LOCK_V1',
                original_risk_distance=contract.price(p['original_risk_distance']),
                lock_trigger_price=contract.price(p['lock_trigger_price']), locked_stop=contract.price(p['lock_stop_price']),
                trigger_R='2', profit_R='0.5', effective='NEXT_MINUTE',
                barrier_precedence='OLD_STOP_TAKE_FIRST', source_price=p['price_source'])
        else:
            policy = dict(name='HYPE_ROW71205_MARKET_NEXT_MINUTE_V1', entry_kind='MARKET_REFERENCE',
                capacity=1, holding_timeout=None, source_price=p['price_source'])
    if approved < fence_ms or approved > now_ms:
        return None
    expiry = contract.moment_ms(intent['expires_at'])
    if expiry != approved + contract.LEASE_MS:
        raise contract.ContractError('APPROVED_SOURCE_EXPIRY')
    proof = dict(source_contract_version=contract.SOURCE_CONTRACT,
        source_config_sha256=p['config_sha256'], decision_at=p['decision_at'])
    if family == 'sol_g65':
        proof['reference_price'] = contract.price(p['reference_price'])
    observed = current.get(p['position_id'])
    reason = None if source_open(state, observed) else 'SOURCE_OBSERVATION_ENDED'
    if intent.get('status') in ('CANCELLED', 'CANCELED', 'CANCELLED_STALE_PRICE'):
        reason = 'SOURCE_NOTIFICATION_CANCELED'
    if not reason and now_ms >= expiry:
        return None
    as_of = approved
    if reason:
        as_of = source_end_ms(state, observed, dict(approved_at=contract.iso_ms(approved)))
        if intent.get('acknowledged_at'):
            as_of = max(as_of, contract.moment_ms(intent['acknowledged_at']))
    if as_of > now_ms:
        raise contract.ContractError('FUTURE_SOURCE_STATE')
    kind = 'CANCEL' if reason else 'ALERT'
    value = dict(version=contract.VERSION, kind=kind, family=family, rule_id=p['rule_id'],
        symbol=p['symbol'], side=p['direction'], source_environment='mainnet', execution_environment='testnet',
        source_price_kind='TRADE_1M', source_at=contract.iso_ms(reference), created_at=contract.iso_ms(reference),
        approved_at=contract.iso_ms(approved), arm_at=contract.iso_ms(approved), expires_at=contract.iso_ms(expiry),
        entry=entry, stop=stop, take_profit=take, original_target=None, policy=policy, proof=proof,
        source_sequence=as_of*10+contract.RANK[kind], source_as_of=contract.iso_ms(as_of),
        source_state='CANCELED' if reason else 'APPROVED', valid_until=None, cancel_reason=reason)
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)
