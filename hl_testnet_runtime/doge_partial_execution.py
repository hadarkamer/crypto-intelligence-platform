"""Frozen DOGE split-exit arithmetic from owned fills; no transport or I/O.

The intermediate target and holding deadline are the original source terms.
Lot rounding is applied to 75% of observed entry quantity, with the exact
remainder assigned to the final target. Replays derive the same outstanding
quantities from durable fills, never from a transient 'partial taken' flag.
"""
from decimal import Decimal, ROUND_DOWN, localcontext

import approved_alert_contract as contract
from . import card_lifecycle as life, price_precision

RULE = 'DOGE_MAXPAIN_ADVERSE_HALF_PART75_H24'


def is_doge(source):
    return contract.is_approved(source) and source.get('rule_id') == RULE


def partial_price(source, decimals):
    if not is_doge(source):
        raise ValueError('DOGE_SPLIT_SOURCE_REQUIRED')
    price = price_precision.round_price(source['policy']['partial_take_price'], decimals)
    stop, entry, take = (Decimal(price_precision.round_price(source[key], decimals))
                         for key in ('stop', 'entry', 'take_profit'))
    if not (stop < entry < Decimal(price) < take if source['side'] == 'LONG'
            else take < Decimal(price) < entry < stop):
        raise ValueError('DOGE_PARTIAL_TARGET_COLLAPSED_AFTER_ROUNDING')
    return price


def timeout_due(source, now):
    return is_doge(source) and now >= contract.moment_ms(source['policy']['hold_until'])


def time_exit_due(trade, now):
    return timeout_due(trade['source'], now)


def _sum(rows):
    return sum((life.number(row['quantity'], positive=True) for row in rows), Decimal(0))


def take_orders(trade):
    """Outstanding TP terms, including partial-entry and partial-exit races."""
    entered = _sum(trade['entry_fills'].values())
    remaining = entered - _sum(trade['exit_fills'].values())
    if remaining < 0:
        raise ValueError('EXIT_EXCEEDED_OWNED_QUANTITY_PEER_RISK')
    if not remaining:
        return []
    final_price = trade['prices']['take_profit']
    if not is_doge(trade['source']):
        return [dict(price=final_price, quantity=life.text(remaining))]
    decimals = trade['asset']['decimals']
    partial = partial_price(trade['source'], decimals)
    with localcontext() as ctx:
        ctx.prec = 60
        step = Decimal(1).scaleb(-decimals)
        planned = (entered * Decimal('.75') / step).to_integral_value(rounding=ROUND_DOWN) * step
        consumed = Decimal(0)
        for fill in trade['exit_fills'].values():
            oid = fill['order_id']
            if (trade['order_legs'][oid] == 'TAKE_PROFIT'
                    and Decimal(trade['orders'][oid]['wire_order']['p']) == Decimal(partial)):
                consumed += life.number(fill['quantity'], positive=True)
        first = min(remaining, max(Decimal(0), planned - consumed))
        last = remaining - first
    return [dict(price=price, quantity=life.text(quantity))
            for price, quantity in ((partial, first), (final_price, last)) if quantity]


def desired_orders(trade):
    """Expected working STOP and TP terms; aggregate TP equals actual remainder."""
    takes = take_orders(trade)
    if not takes:
        return []
    remaining = sum((Decimal(row['quantity']) for row in takes), Decimal(0))
    stop = trade['prices']['stop'] if is_doge(trade['source']) else trade['desired_stop']
    return [('STOP', stop, life.text(remaining))] + [
        ('TAKE_PROFIT', row['price'], row['quantity']) for row in takes]
