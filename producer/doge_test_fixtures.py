import approved_alert_contract as contract
from approved_alert_fixtures import BASE, maxpain_alert


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
