"""Versioned planned price-distance budget. Never reads credentials or settings.

The legacy $2 budget is before fees, funding and slippage; NOT a loss cap.
Old $10/$20 profiles are retained only for exact historical action reconstruction.
Existing positions and immutable order records must never be resized by a config
change. Approved formula entries can use a separately validated journal-bound
5% risk snapshot; every other entry retains the legacy $2 ceiling.
"""
from decimal import Decimal, InvalidOperation, localcontext

CURRENT_RISK_USD = '2'
LEGACY_RISK_USD = '20'
HISTORICAL_RISK_PROFILES = ('10', LEGACY_RISK_USD)
VERSION = 'planned-distance-risk-2usd-v3'


class RiskError(ValueError):
    """Fixed codes only."""


def card_profile(historical=None):
    """Exact profiles used by stored cards; not permission to send an entry."""
    value = CURRENT_RISK_USD if historical is None else historical
    versions = {'2': VERSION, '10': 'planned-distance-risk-10usd-v2'}
    if type(value) is not str or value not in versions:
        raise RiskError('UNKNOWN_HISTORICAL_CARD_RISK_PROFILE')
    return dict(planned_usd=value, policy=versions[value], costs_included=False)


def budget(historical=None):
    if historical is None:
        return Decimal(CURRENT_RISK_USD)
    if type(historical) is not str or historical not in (CURRENT_RISK_USD, *HISTORICAL_RISK_PROFILES):
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


def assert_new_entry_budget(action, *, formula_risk=None):
    """Defense at signing/transport. Reduce-only repair is not a new entry.

    Historical profiles cannot authorize another $10/$20 entry. A dynamic
    snapshot must be bound to trusted durable context by the caller. Do not
    change order sizes, prices, IDs or exit types here.
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
            limit = budget()
            if formula_risk is not None:
                from .formula_accounting import validate_risk
                limit = validate_risk(formula_risk)
            if size * abs(price - stop_price) > limit:
                raise RiskError('NEW_ENTRY_EXCEEDS_CURRENT_RISK_BUDGET')
    except RiskError:
        raise
    except (KeyError, TypeError, AttributeError, InvalidOperation):
        raise RiskError('RISK_ACTION_REQUIRES_REVIEW') from None
