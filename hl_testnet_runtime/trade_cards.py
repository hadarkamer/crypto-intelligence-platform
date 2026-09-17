"""Phase 1: pure Testnet records and logical two-account routing, NEVER orders.

The direction is the final LONG/SHORT supplied by the alert, not recalculated.
No account key, signer, HTTP client, execution queue, or work on import.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN, localcontext
import hashlib
import json
import re

from . import price_precision as precision
from .risk_policy import budget, CURRENT_RISK_USD, VERSION as RISK_VERSION

VERSION = 'testnet-trade-card-v1'
SCOPES = ('received_alert', 'historical_review', 'synthetic_test')
ROLES = {'LONG': 'long_account', 'SHORT': 'short_account'}
IDENTIFIER = re.compile(r'[A-Za-z0-9_.:-]{1,100}\Z')
ADDRESS = re.compile(r'0x[0-9a-fA-F]{40}\Z')


class CardError(ValueError):
    """Fixed codes; no source or private configuration in errors."""


def canonical(value):
    try:
        text = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        if len(text.encode()) > 32768:
            raise ValueError()
        return text
    except (ValueError, TypeError, RecursionError):
        raise CardError('INVALID_CARD_RECORD') from None


def checksum(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise CardError('STABLE_SOURCE_IDENTIFIER_REQUIRED')
    return value


def text(value):
    return format(value.normalize(), 'f')


def prepare_card(source, metadata, *, rule_id, threshold_pct, record_kind,
                 source_stream='bot_experimental'):
    """Record a valid alert even when accounts are unconfigured; do not dispatch.

    Source IDs identify individual alerts, not a market wave or message text.
    Identity excludes account, risk and timestamps of receipt: retries cannot
    create extra cards by rerouting or restarting. Distinct IDs stay distinct.
    A record is NOT a pending exchange order and contains NO asserted fills/PnL.
    """
    if record_kind not in SCOPES:
        raise CardError('EXPLICIT_RECORD_KIND_REQUIRED')
    identifier(source_stream)
    identifier(rule_id)
    try:
        threshold = precision.decimal_price(threshold_pct)
        if threshold > 100:
            raise CardError('FORMULA_THRESHOLD_MUST_BE_PERCENT')
        prepared = precision.prepare_signal(source, metadata)
    except precision.PrecisionError as exc:
        raise CardError(str(exc)) from None
    execution = prepared['execution']
    with localcontext() as ctx:
        ctx.prec = 60
        entry, stop = (Decimal(execution[k]) for k in ('entry', 'stop'))
        step = Decimal(1).scaleb(-prepared['audit']['sz_decimals'])
        size = ((budget() / abs(entry-stop)) / step).to_integral_value(rounding=ROUND_DOWN) * step
        direction = Decimal(1) if execution['side'] == 'LONG' else Decimal(-1)
        cancel_price = entry * (1 + direction * threshold / 200)
        planning = dict(quantity=text(size), distance_risk_usd=text(size * abs(entry-stop)),
                        cancel_price=text(cancel_price), positive_quantity=size > 0)
    return dict(version=VERSION, environment='testnet',
        card_id=checksum(['testnet', source_stream, execution['event_id']]),
        source_stream=source_stream, event_id=execution['event_id'], record_kind=record_kind,
        rule=dict(id=rule_id, threshold_pct=threshold_pct), prepared=prepared,
        account_role=ROLES[execution['side']], planning=planning,
        risk=dict(planned_usd=CURRENT_RISK_USD, policy=RISK_VERSION, costs_included=False),
        state='RECORDED_ONLY', actual_execution=None, dispatch_enabled=False, revision=1)


def validate_card(card):
    try:
        prepared = card['prepared']
        meta = {'universe': [{'name': prepared['source']['symbol'],
                             'szDecimals': prepared['audit']['sz_decimals']}]}
        expected = prepare_card(prepared['source'], meta, rule_id=card['rule']['id'],
            threshold_pct=card['rule']['threshold_pct'], record_kind=card['record_kind'],
            source_stream=card['source_stream'])
        if canonical(expected) != canonical(card):
            raise ValueError()
        return deepcopy(card)
    except (KeyError, TypeError, ValueError):
        raise CardError('CARD_CONTENT_NOT_VERIFIED') from None


def account_routes(env):
    """Public addresses only. Configuration is NOT authenticated authorization.

    Existing single-account PUBLIC settings are reused only via the explicitly
    approved long-account alias. Missing slots are never substituted silently.
    This assigns future card destinations, not any existing exchange position.
    """
    from .approved_account_assignment import public_route_env, AssignmentError
    try:
        env = public_route_env(env)
    except AssignmentError as exc:
        raise CardError(str(exc)) from None
    routes, used = {}, set()
    for side, role in ROLES.items():
        keys = [f'HL_TESTNET_{side}_ACCOUNT_ADDRESS', f'HL_TESTNET_{side}_AGENT_ADDRESS']
        values = [env.get(k, '') for k in keys]
        if not any(values):
            routes[role] = dict(status='WAITING_FOR_ACCOUNT', account=None, agent=None)
            continue
        if not all(isinstance(v, str) and ADDRESS.fullmatch(v) and int(v[2:], 16) for v in values):
            raise CardError('COMPLETE_PUBLIC_ACCOUNT_PAIR_REQUIRED')
        account, agent = [v.lower() for v in values]
        if account == agent or used.intersection((account, agent)):
            raise CardError('TWO_DISTINCT_ACCOUNTS_AND_AGENTS_REQUIRED')
        used.update((account, agent))
        routes[role] = dict(status='CONFIGURED_NOT_VERIFIED', account=account, agent=agent)
    return routes


def route_summary(card, env):
    card = validate_card(card)
    routes = account_routes(env)
    return dict(account_role=card['account_role'],
                account_status=routes[card['account_role']]['status'],
                dispatch_enabled=False, exchange_mapping_verified=False)


def journal_projection(card):
    """Contract draft for the EXISTING app, not a network sender or new screen.

    Only bot-owned fields. Never notes, conclusions, credentials or app user IDs.
    Null actuals must stay null, not become zero-profit trades in app statistics.
    """
    card = validate_card(card)
    return dict(contract='bot-journal-record-v1', external_id=card['card_id'], revision=1,
        environment='testnet', record_kind=card['record_kind'], delivery_enabled=False,
        machine_fields=dict(source_event_id=card['event_id'], rule=card['rule'],
            account_role=card['account_role'], source=card['prepared']['source'],
            rounded=card['prepared']['execution'], risk=card['risk'],
            planned_quantity=card['planning']['quantity'], status='RECORDED_ONLY',
            actual_execution=None, pnl=None))
