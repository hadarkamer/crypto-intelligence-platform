"""Versioned planned price-distance budget. Never reads credentials or settings.

The current $10 budget is before fees, funding and slippage; NOT a loss cap.
The old $20 profile is retained only for exact historical action reconstruction.
Existing positions and immutable order records must never be resized by a config
change. Signing and transport independently reject NEW entries above $10.
"""
from decimal import Decimal, InvalidOperation, localcontext

CURRENT_RISK_USD = '10'
LEGACY_RISK_USD = '20'
VERSION = 'planned-distance-risk-10usd-v2'


class RiskError(ValueError):
    """Fixed codes only."""


def budget(historical=None):
    if historical is None:
        return Decimal(CURRENT_RISK_USD)
    if type(historical) is not str or historical not in (CURRENT_RISK_USD, LEGACY_RISK_USD):
        raise RiskError('UNKNOWN_HISTORICAL_RISK_PROFILE')
    return Decimal(historical)


def _positive(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 80:
        raise RiskError('INVALID_RISK_NUMBER')
    try:
        n = Decimal(value)
    except InvalidOperation:
        raise RiskError('INVALID_RISK_NUMBER') from None
    if (not n.is_finite() or not Decimal('1e-15') <= n <= Decimal('1e15')
            or len(n.as_tuple().digits) > 28):
        raise RiskError('INVALID_RISK_NUMBER')
    return n


def assert_new_entry_budget(action):
    """Defense at signing/transport. Reduce-only repair is not a new entry.

    Historical profiles may be rebuilt and inspected, but may not authorize
    another $20 entry. Do not change order sizes, prices, IDs or exit types here.
    """
    try:
        if not isinstance(action, dict) or action.get('type') != 'order':
            raise RiskError('RISK_ACTION_REQUIRES_REVIEW')
        orders = action.get('orders')
        if not isinstance(orders, list) or not orders:
            raise RiskError('RISK_ACTION_REQUIRES_REVIEW')
        if any(not isinstance(o, dict) or type(o.get('r')) is not bool for o in orders):
            raise RiskError('RISK_ACTION_REQUIRES_REVIEW')
        entries = [o for o in orders if o['r'] is False]
        if not entries:
            return  # Existing exit repair has separate immutable-action checks.
        if len(orders) != 3 or len(entries) != 1 or action.get('grouping') != 'normalTpsl':
            raise RiskError('BRACKET_REQUIRED_FOR_NEW_ENTRY')
        entry = entries[0]
        exits = [o for o in orders if o['r']]
        if (entry.get('t') != {'limit': {'tif': 'Gtc'}} or type(entry.get('b')) is not bool
                or type(entry.get('a')) is not int):
            raise RiskError('RISK_ACTION_REQUIRES_REVIEW')
        stop = [o for o in exits if o.get('t', {}).get('trigger', {}).get('tpsl') == 'sl']
        take = [o for o in exits if o.get('t', {}).get('trigger', {}).get('tpsl') == 'tp']
        if len(stop) != 1 or len(take) != 1:
            raise RiskError('BRACKET_REQUIRED_FOR_NEW_ENTRY')
        size, price = _positive(entry['s']), _positive(entry['p'])
        for o in exits:
            if (o.get('a') != entry['a'] or type(o.get('b')) is not bool
                    or o['b'] == entry['b'] or _positive(o['s']) != size):
                raise RiskError('RISK_BRACKET_SIZE_OR_SIDE_MISMATCH')
        stop_price = _positive(stop[0]['t']['trigger']['triggerPx'])
        take_price = _positive(take[0]['t']['trigger']['triggerPx'])
        if not (stop_price < price < take_price if entry['b'] else take_price < price < stop_price):
            raise RiskError('RISK_BRACKET_PRICE_ORDER_INVALID')
        with localcontext() as ctx:
            ctx.prec = 60
            if size * abs(price - stop_price) > budget():
                raise RiskError('NEW_ENTRY_EXCEEDS_CURRENT_TEN_DOLLAR_RISK')
    except RiskError:
        raise
    except (KeyError, TypeError, AttributeError, InvalidOperation):
        raise RiskError('RISK_ACTION_REQUIRES_REVIEW') from None
