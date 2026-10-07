"""Project durable, engine-approved MaxPain notifications without strategy math.

Approval is a completed source decision, not a Telegram delivery acknowledgement
and never an exchange fill. The displayed prices are copied with the renderer's
existing eight-significant-digit formatting. Pending source plans cannot enter
this path. The old prospective proof/lease is deliberately not required.
"""
import approved_alert_contract as contract

MODE = 'experimental_approved_alert_bridge_v2'
SOURCE_VERSION = 'sol-proximity-notification-store-v1'


def displayed_price(value):
    """Exactly the numeric text in the existing MaxPain Telegram renderer."""
    return contract.price(format(value, '.8g'))


def approved_messages(state, *, now_ms, fence_ms):
    if state.get('version') != SOURCE_VERSION:
        raise contract.ContractError('MAXPAIN_SOURCE_VERSION')
    intents = state.get('intents')
    if not isinstance(intents, list) or len(intents) > 1024:
        raise contract.ContractError('SOURCE_INTENTS_INVALID')
    positions = state.get('active', []) + state.get('history', [])
    current = {p['position_id']: p for p in positions}
    messages = []
    for intent in intents:
        p = intent.get('payload', {})
        if (p.get('rule_id') not in contract.SPECS or p.get('legacy_formula')
                or p.get('manual_source_restore') or p.get('status') != 'OPEN'):
            continue
        if (intent.get('position_id') != p.get('position_id')
                or intent.get('intent_id') != p.get('position_id')):
            raise contract.ContractError('APPROVED_SOURCE_IDENTITY')
        # The source intent is created only after this whole minute has closed.
        # Never replace this with forwarding/Telegram time on retry or restart.
        approved = p['fill_ms'] + 60_000
        if approved < fence_ms or approved > now_ms:
            continue
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
            continue
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
        messages.append(contract.validate(value))
    return messages
