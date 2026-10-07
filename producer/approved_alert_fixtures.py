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
        execution_environment='testnet', source_price_kind='TRADE_1M',
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
