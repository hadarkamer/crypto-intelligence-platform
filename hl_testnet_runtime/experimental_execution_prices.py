"""Prepare exchange prices without worsening an approved alert's entry limit.

Source fields remain immutable. Legacy plans retain their existing precision
policy. For approved alerts, a buy is never rounded above its stated limit and
a sell is never rounded below it. No price, venue or network lookup occurs here.
"""
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, localcontext

import approved_alert_contract as contract
from . import price_precision


def prepare(source, metadata):
    signal = dict(kind='SIGNAL', event_id=source['occurrence_id'],
        symbol=source['symbol'], side=source['side'], entry=source['entry'],
        stop=source['stop'], take_profit=source['take_profit'], at=source['source_at'])
    result = price_precision.prepare_signal(signal, metadata)
    if not contract.is_approved(source):
        return result
    execution, audit = result['execution'], result['audit']
    original = price_precision.decimal_price(source['entry'])
    nearest = Decimal(execution['entry'])
    buy = source['side'] == 'LONG'
    if (nearest > original if buy else nearest < original):
        with localcontext() as ctx:
            ctx.prec = 60
            exponent = max(audit['sz_decimals'] - 6, min(0, original.adjusted() - 4))
            value = original.quantize(Decimal(1).scaleb(exponent),
                rounding=ROUND_FLOOR if buy else ROUND_CEILING)
            execution['entry'] = format(value.normalize(), 'f')
        if price_precision.round_price(execution['entry'], audit['sz_decimals']) != execution['entry']:
            raise price_precision.PrecisionError('APPROVED_LIMIT_PRECISION_INVALID')
    entry, stop, take = (Decimal(execution[k]) for k in ('entry', 'stop', 'take_profit'))
    if not (stop < entry < take if buy else take < entry < stop):
        raise price_precision.PrecisionError('PRICES_COLLAPSED_AFTER_ROUNDING')
    if (entry > original if buy else entry < original):
        raise price_precision.PrecisionError('APPROVED_LIMIT_WOULD_WORSEN')
    if entry != original:
        audit['changes']['entry'] = dict(before=source['entry'], after=execution['entry'])
    else:
        audit['changes'].pop('entry', None)
    audit.update(policy='approved-limit-bound-with-existing-exit-precision-v1',
        execution_digest=price_precision.digest(execution), entry_limit_worsened=False)
    return result
