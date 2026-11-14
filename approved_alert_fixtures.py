"""Synthetic approved-alert data for isolated tests; no source or venue I/O."""
import approved_alert_contract as contract
from decimal import Decimal, localcontext

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


def selected_formula_alert(family, *, symbol=None, reference_ms=BASE,
                           approved_ms=None, cycle='selected-fixture'):
    """Frozen synthetic decisions for the four explicit November adapters."""
    rule, symbols = contract.SELECTED_FORMULAS[family]
    symbol = symbol or ('SOL' if family == 'sol_sff1361' else 'XRP')
    approved_ms = reference_ms + 1000 if approved_ms is None else approved_ms
    p, target = Decimal('100'), Decimal('98')
    policy = dict(name=rule + '_V1', entry_kind='LIMIT_AFTER_ALERT',
        source_price=f'HYPERLIQUID_{symbol}_PERPETUAL_TRADE_1M')
    with localcontext() as ctx:
        ctx.prec = 60
        if family == 'sol_sff1361':
            entry, stop, take = p, p * Decimal('1.01'), p * Decimal('.98')
        else:
            adverse, stop_fraction, fraction = {
                'oconf0034_13': ('.2', '.012', '1'),
                'ct009': ('0.1814560294151306', '0.0040077843998312', '0.9365681871771814'),
                'xrp_liquidity_inverse': ('0.2342784544453025', '0.0105286625253104', '0.8671878740191461'),
            }[family]
            entry = p + Decimal(adverse) * (p - target)
            stop, take = entry * (1 + Decimal(stop_fraction)), entry + Decimal(fraction) * (target - entry)
            policy.update(pending_expires_at=contract.iso_ms(reference_ms + 86_400_000),
                          cancel_before_fill_price=str(target))
    value = dict(version=contract.VERSION, kind='ALERT', family=family,
        occurrence_id='', rule_id=rule, symbol=symbol, side='SHORT',
        source_environment='mainnet', execution_environment='mainnet', source_price_kind='TRADE_1M',
        source_at=contract.iso_ms(reference_ms), created_at=contract.iso_ms(approved_ms),
        approved_at=contract.iso_ms(approved_ms), arm_at=contract.iso_ms(approved_ms),
        expires_at=contract.iso_ms(approved_ms + contract.LEASE_MS),
        entry=contract.selected_price(entry), stop=contract.selected_price(stop),
        take_profit=contract.selected_price(take),
        original_target=None if family == 'sol_sff1361' else str(target), policy=policy,
        proof=dict(source_contract_version=contract.SOURCE_CONTRACT,
            source_config_sha256='a' * 64, cycle_id=cycle, episode_key='episode-' + cycle,
            timeframe='30m' if family == 'sol_sff1361' else '3d',
            decision_at=contract.iso_ms(reference_ms - 120000),
            data_available_at=contract.iso_ms(reference_ms - 1),
            reference_at=contract.iso_ms(reference_ms), reference_price=str(p),
            source_target=None if family == 'sol_sff1361' else
                '102' if family == 'xrp_liquidity_inverse' else str(target)),
        source_sequence=approved_ms * 10 + contract.RANK['ALERT'],
        source_as_of=contract.iso_ms(approved_ms), source_state='APPROVED',
        valid_until=None, cancel_reason=None)
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)
