"""Owner-approved price rounding and preconfigured TP/SL preparation.

No keys, signatures, orders, transfers or network work on import. Keep the source
unchanged. This module prepares data, not a new trading strategy. The strict
sender and the existing durable-storage/freshness/exit-type guards remain intact.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import hashlib
import json
import re

POLICY = 'nearest-half-up-perp-v1'
FIELDS = {'kind', 'event_id', 'symbol', 'side', 'entry', 'stop', 'take_profit', 'at'}
PRICES = ('entry', 'stop', 'take_profit')


class PrecisionError(ValueError):
    """Fixed error codes only; never include source data or credentials."""


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def decimal_price(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 80:
        raise PrecisionError('DECIMAL_STRING_REQUIRED')
    try:
        price = Decimal(value)
    except InvalidOperation:
        raise PrecisionError('INVALID_PRICE') from None
    if (not price.is_finite() or not Decimal('1e-15') <= price <= Decimal('1e15')
            or len(price.as_tuple().digits) > 28):
        raise PrecisionError('INVALID_PRICE')
    return price


def round_price(value, sz_decimals):
    """Nearest permitted perp price; exact halfway rounds up. No binary floats.

    Five significant figures AND at most 6-szDecimals fractional digits.
    Integer prices are permitted regardless of significant figures. Revalidate
    after a carry. Zero rounding is rejected, not replaced with a minimum price.
    """
    if type(sz_decimals) is not int or not 0 <= sz_decimals <= 6:
        raise PrecisionError('INVALID_ASSET_PRECISION')
    price = decimal_price(value)
    with localcontext() as ctx:
        ctx.prec = 60
        exponent = max(sz_decimals - 6, min(0, price.adjusted() - 4))
        rounded = price.quantize(Decimal(1).scaleb(exponent), rounding=ROUND_HALF_UP)
        if rounded <= 0:
            raise PrecisionError('ROUNDING_REACHED_ZERO')
        normalized = rounded.normalize()
        if (normalized != normalized.to_integral_value()
                and (len(normalized.as_tuple().digits) > 5
                     or normalized.as_tuple().exponent < -(6-sz_decimals))):
            raise PrecisionError('ROUNDED_PRICE_NOT_PERMITTED')
        return format(normalized, 'f')


def prepare_signal(message, metadata, *, policy=POLICY):
    """Return separate source/execution records and their exact change audit.

    Never change identity, time, symbol or side. Invalid source ordering is not
    fixed by rounding. Reject coalesced prices: no zero-risk sizing or inversion.
    Audit values belong in private execution storage, never public runtime logs.
    """
    if policy != POLICY:
        raise PrecisionError('PRICE_ROUNDING_POLICY_NOT_APPROVED')
    if not isinstance(message, dict) or set(message) != FIELDS or message.get('kind') != 'SIGNAL':
        raise PrecisionError('COMPLETE_SOURCE_SIGNAL_REQUIRED')
    if (not isinstance(message['event_id'], str)
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', message['event_id'])
            or not isinstance(message['symbol'], str)
            or not re.fullmatch(r'[A-Z][A-Z0-9]{0,19}', message['symbol'])
            or message['side'] not in ('LONG', 'SHORT')):
        raise PrecisionError('INVALID_SOURCE_IDENTITY')
    try:
        if not isinstance(message['at'], str) or len(message['at']) > 40:
            raise ValueError()
        if datetime.fromisoformat(message['at'].replace('Z', '+00:00')).utcoffset() is None:
            raise ValueError()
    except ValueError:
        raise PrecisionError('SOURCE_TIME_REQUIRED') from None
    source = deepcopy(message)
    before = [decimal_price(source[key]) for key in PRICES]
    valid_order = lambda p: p[1] < p[0] < p[2] if source['side'] == 'LONG' else p[2] < p[0] < p[1]
    if not valid_order(before):
        raise PrecisionError('INVALID_SOURCE_PRICE_ORDER')
    universe = metadata.get('universe') if isinstance(metadata, dict) else None
    if not isinstance(universe, list) or not 1 <= len(universe) <= 10000:
        raise PrecisionError('INVALID_METADATA')
    matches = [a for a in universe if isinstance(a, dict) and a.get('name') == source['symbol']]
    if len(matches) != 1 or matches[0].get('isDelisted', False) is not False:
        raise PrecisionError('ASSET_UNAVAILABLE')
    precision = matches[0].get('szDecimals')
    execution = deepcopy(source)
    changes = {}
    for key in PRICES:
        execution[key] = round_price(source[key], precision)
        if decimal_price(execution[key]) != decimal_price(source[key]):
            changes[key] = {'before': source[key], 'after': execution[key]}
    if not valid_order([decimal_price(execution[key]) for key in PRICES]):
        raise PrecisionError('PRICES_COLLAPSED_AFTER_ROUNDING')
    return {'source': source, 'execution': execution,
            'audit': {'policy': POLICY, 'sz_decimals': precision, 'changes': changes,
                      'source_digest': digest(source), 'execution_digest': digest(execution),
                      'source_time_changed': False, 'risk_rule_changed': False,
                      'size_must_use_rounded_entry_and_stop': True}}


def build_prepared_orders(message, metadata, account, *, exit_type):
    """Pure request preparation: entry + preconfigured take-profit + stop-loss.

    Exit type stays explicit. A take-profit label does not choose Market vs
    Limit. No call to submit_once is made here. Size is recalculated by the
    existing builder from the rounded prices under the unchanged $20 risk rule.
    """
    import hyperliquid_testnet_executor as sender
    prepared = prepare_signal(message, metadata)
    prepared['action'] = sender.build_action(prepared['execution'], metadata, account,
                                              exit_type=exit_type)
    action = prepared['action']
    orders = action['orders']
    if (action.get('grouping') != 'normalTpsl' or len(orders) != 3
            or orders[1].get('t', {}).get('trigger', {}).get('tpsl') != 'tp'
            or orders[2].get('t', {}).get('trigger', {}).get('tpsl') != 'sl'
            or any(o.get('r') is not True for o in orders[1:])):
        raise PrecisionError('PRECONFIGURED_EXITS_NOT_VERIFIED')
    return prepared


class MetadataReader:
    """Reuse the same metadata snapshot for normalization and budget validation."""
    def __init__(self, delegate):
        self.delegate = delegate
        self.meta = None
    @property
    def calls(self):
        return getattr(self.delegate, 'calls', 0)
    def read(self, kind, *, user=None, coin=None):
        if kind == 'meta' and self.meta is not None:
            return deepcopy(self.meta)
        result = self.delegate.read(kind, user=user, coin=coin)
        if kind == 'meta':
            self.meta = deepcopy(result)
        return result


def review_rounded_signal(message, *, account, agent, journal=None, exit_type=None,
                          client=None, now=None, env=None):
    """Read-only integration; keep all strict guard blockers other than precision.

    The original source remains in the configured input. Reports expose only
    digests and field names, never prices, addresses, balances or secret values.
    This function neither persists an execution journal nor enables a sender.
    """
    from . import checks, guarded_execution
    report = {'phase': 'review_only', 'mode': 'testnet', 'rounding_policy': POLICY,
              'eligible_for_controlled_attempt': False, 'order_requests_sent': 0,
              'signing_tested': False, 'prices_changed': False, 'blockers': []}
    reader = None
    try:
        # Validate shape/addresses before requesting public metadata.
        source, _ = guarded_execution._frozen(message)
        account, agent = checks.address(account), checks.address(agent)
        reader = MetadataReader(checks.InfoReader() if client is None else client)
        prepared = prepare_signal(source, reader.read('meta'))
        result = guarded_execution.review_only(prepared['execution'], account=account,
            agent=agent, journal=journal, exit_type=exit_type, client=reader, now=now, env=env)
        audit = prepared['audit']
        report.update(result)
        report.update(rounding_policy=POLICY, original_source_digest=audit['source_digest'],
            execution_digest=audit['execution_digest'], rounded_fields=list(audit['changes']),
            prices_changed=bool(audit['changes']), source_time_changed=False,
            source_record_preserved=True, size_basis='rounded_entry_stop',
            take_profit_in_request_template=True, stop_loss_in_request_template=True)
    except (PrecisionError, checks.Blocked, guarded_execution.GuardError) as exc:
        report['blockers'].append(str(exc))
    except Exception:
        report['blockers'].append('ROUNDING_REVIEW_UNAVAILABLE')
    report['public_reads'] = getattr(reader, 'calls', 0)
    return report
