"""Synthetic approved-alert data for isolated tests; no source or venue I/O."""
import approved_alert_contract as contract

BASE = 1791288060000  # A fixed, closed-minute timestamp, unrelated to live data.


def maxpain_alert(*, approved_ms=BASE, as_of_ms=None, kind='ALERT',
                  cancel_reason=None, entry='93.544', stop='91.405',
                  take_profit='95.3265'):
    as_of = approved_ms if as_of_ms is None else as_of_ms
    value = dict(version=contract.VERSION, kind=kind, family='maxpain',
        occurrence_id='', rule_id='HYPE_MAXPAIN_DIST05_15_LONG_TF',
        symbol='HYPE', side='LONG', source_environment='mainnet',
        execution_environment='mainnet', source_price_kind='TRADE_1M',
        source_at=contract.iso_ms(approved_ms - 180_000),
        created_at=contract.iso_ms(approved_ms - 170_000),
        approved_at=contract.iso_ms(approved_ms), arm_at=contract.iso_ms(approved_ms),
        expires_at=contract.iso_ms(approved_ms + 90_000),
        entry=entry, stop=stop, take_profit=take_profit, original_target='96.039',
        policy=dict(name='APPROVED_ALERT_LIMIT_GTC_V1', entry_kind='LIMIT_AFTER_ALERT',
                    source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M'),
        proof=dict(source_contract_version=contract.SOURCE_CONTRACT,
                   source_config_sha256='a' * 64,
                   cycle_id='synthetic-approved-cycle', episode_key='96.039',
                   timeframe='3d'),
        source_sequence=as_of * 10 + contract.RANK[kind],
        source_as_of=contract.iso_ms(as_of),
        source_state='CANCELED' if kind == 'CANCEL' else 'APPROVED',
        valid_until=None, cancel_reason=cancel_reason)
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)


def formula_alert(family, *, approved_ms=None):
    from experimental_execution_fixtures import r2732_message, hype_row71205_message, sol_g65_message
    builder = {'r2732': r2732_message, 'hype_row71205': hype_row71205_message,
               'sol_g65': sol_g65_message}[family]
    if approved_ms is None:
        from experimental_execution_fixtures import BASE as decision
        approved_ms = decision + (120000 if family == 'sol_g65' else 60000)
    value = builder(decision_ms=approved_ms-(120000 if family == 'sol_g65' else 60000))
    value.update(version=contract.VERSION, kind='ALERT', source_state='APPROVED',
        approved_at=contract.iso_ms(approved_ms), arm_at=contract.iso_ms(approved_ms),
        expires_at=contract.iso_ms(approved_ms+contract.LEASE_MS),
        source_as_of=contract.iso_ms(approved_ms), source_sequence=approved_ms*10+1, valid_until=None)
    return contract.validate(value)


def doge_partial_alert(*, side='LONG', approved_ms=BASE, cycle_id='synthetic-doge-partial-cycle',
                       as_of_ms=None, kind='ALERT', cancel_reason=None):
    """Frozen source prices and two take levels, independent of venue fills."""
    values = (('99', '98', '101.6', '100.95', '102') if side == 'LONG'
              else ('101', '102', '98.4', '99.05', '98'))
    entry, stop, take, partial, target = values
    value = maxpain_alert(approved_ms=approved_ms, as_of_ms=as_of_ms,
                          kind=kind, cancel_reason=cancel_reason)
    value.update(rule_id=contract.DOGE_PARTIAL_RULE, symbol='DOGE', side=side,
                 entry=entry, stop=stop, take_profit=take, original_target=target)
    value['proof'].update(source_config_sha256=contract.DOGE_SOURCE_CONFIG_SHA256,
                          cycle_id=cycle_id, episode_key=target)
    value['policy'] = dict(name='DOGE_PARTIAL_TAKE_75_H24_V1',
        entry_kind='LIMIT_AFTER_ALERT', source_price='HYPERLIQUID_DOGE_PERPETUAL_TRADE_1M',
        partial_take_price=partial, partial_fraction='0.75', stop_policy='KEEP_ORIGINAL',
        hold_until=contract.iso_ms(approved_ms - 60_000 + 86_400_000))
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)
