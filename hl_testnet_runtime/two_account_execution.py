"""Role-specific TESTNET connection preparation. No requests to /exchange here.

A default abstraction label is NOT renamed to disabled. For the approved empty
second account, independently check native perp USDC and exchange capacity.
The actual sender remains explicit, journaled, one-attempt and locked by default.
"""
from datetime import datetime, timezone
from decimal import Decimal
import importlib.metadata
import json
import re
import time

from . import checks
from .trade_cards import account_routes
from .approved_account_assignment import MODE as LEGACY_ALIAS

PHANTOM = '0x6059e209cc4b1173eb30a7ac3740b24d33ce8075'
SERVICE = 'srv-dakptbh594qs7395460g'
ROLES = {'long_account': 'LONG', 'short_account': 'SHORT'}


def route_for(env, role, account=None, agent=None, side=None):
    if role not in ROLES:
        raise checks.Blocked('EXPLICIT_ACCOUNT_ROLE_REQUIRED')
    route = account_routes(env)[role]
    if route['status'] == 'WAITING_FOR_ACCOUNT':
        raise checks.Blocked('ROLE_ACCOUNT_NOT_CONFIGURED')
    if ((account is not None and checks.address(account) != route['account'])
            or (agent is not None and checks.address(agent) != route['agent'])
            or (side is not None and side != ROLES[role])):
        raise checks.Blocked('ROLE_ACCOUNT_OR_DIRECTION_MISMATCH')
    return route


def key_field(env, role):
    if role == 'short_account':
        return 'HL_TESTNET_SHORT_AGENT_KEY'
    if role != 'long_account':
        raise checks.Blocked('EXPLICIT_ACCOUNT_ROLE_REQUIRED')
    return ('HL_TESTNET_AGENT_KEY' if env.get('HL_TESTNET_LONG_ACCOUNT_SOURCE') == LEGACY_ALIAS
            else 'HL_TESTNET_LONG_AGENT_KEY')


def wallet_for_role(env, role, account, agent):
    """Local only; never copy a key into another environment variable or log."""
    route = route_for(env, role, account, agent)
    key = env.get(key_field(env, role), '')
    if not key:
        raise checks.Blocked('ROLE_AGENT_KEY_NOT_CONFIGURED')
    if not isinstance(key, str) or not re.fullmatch(r'(?:0x)?[0-9a-fA-F]{64}', key):
        raise checks.Blocked('ROLE_AGENT_KEY_INVALID')
    try:
        if importlib.metadata.version('hyperliquid-python-sdk') != '0.24.0':
            raise ValueError()
        from eth_account import Account
        wallet = Account.from_key(key)
        if checks.address(wallet.address) != route['agent']:
            raise checks.Blocked('ROLE_AGENT_KEY_ADDRESS_MISMATCH')
        return wallet
    except checks.Blocked:
        raise
    except Exception:
        raise checks.Blocked('ROLE_LOCAL_KEY_CHECK_UNAVAILABLE') from None


def execution_unlocked(env):
    # No call or startup path in this change sets either of these approvals.
    return (env.get('RENDER_SERVICE_ID') == SERVICE
        and env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') == 'approved_single_attempt_v1'
        and env.get('HL_TESTNET_RUNTIME_MODE') == 'single_testnet_attempt_v1')


def default_native_snapshot(route, reader, symbol, *, plan=None, allow_owned_exposure=False):
    """Conservative evidence for this one account/native USDC only, not all default users.

    No summed balances, guessed mode or changed leverage. Existing exposure is
    permitted only when the continuous worker has reconciled its owned markets.
    """
    account, agent = route['account'], route['agent']
    if account != PHANTOM:
        raise checks.Blocked('DEFAULT_SCOPE_NOT_APPROVED')
    started = time.monotonic()
    first = reader.read('userAbstraction', user=account)
    if first != 'default':
        raise checks.Blocked('ACCOUNT_MODE_CHANGED_RECHECK')
    if reader.read('userRole', user=account) != {'role': 'user'}:
        raise checks.Blocked('INDEPENDENT_TEST_ACCOUNT_REQUIRED')
    link = reader.read('userRole', user=agent)
    if (not isinstance(link, dict) or link.get('role') != 'agent'
            or checks.address((link.get('data') or {}).get('user')) != account):
        raise checks.Blocked('AGENT_ACCOUNT_MISMATCH')
    perp = reader.read('clearinghouseState', user=account)
    if not isinstance(perp, dict) or not isinstance(perp.get('assetPositions'), list):
        raise checks.Blocked('INVALID_ACCOUNT_STATE')
    if not allow_owned_exposure and any(checks.number(p['position']['szi'], signed=True) != 0 for p in perp['assetPositions']):
        raise checks.Blocked('INITIAL_ACCOUNT_NOT_EMPTY')
    summary = perp.get('marginSummary', {})
    equity = checks.number(summary.get('accountValue'))
    raw = checks.number(summary.get('totalRawUsd'))
    used = checks.number(summary.get('totalMarginUsed'))
    notional = checks.number(summary.get('totalNtlPos'))
    unheld = checks.number(perp.get('withdrawable'))
    if not (equity > 0 and 0 < unheld <= equity and raw >= 0 and used >= 0 and notional >= 0):
        raise checks.Blocked('DEFAULT_NATIVE_BALANCE_NOT_RECONCILED')
    if not allow_owned_exposure and not (equity == raw and used == 0 and notional == 0):
        raise checks.Blocked('DEFAULT_NATIVE_BALANCE_NOT_RECONCILED')
    spot = reader.read('spotClearinghouseState', user=account)
    total, _ = checks.unified_usdc(spot)
    if total != 0 or any(checks.number(row.get('total')) > 0 for row in spot['balances']):
        raise checks.Blocked('DEFAULT_OTHER_BALANCES_REQUIRE_REVIEW')
    active = reader.read('activeAssetData', user=account, coin=symbol)
    available, max_size = checks.capacity(active, account, symbol)
    if available <= 0 or max_size <= 0:
        raise checks.Blocked('NO_USABLE_CAPACITY_OBSERVED')
    result = dict(status='NATIVE_CAPACITY_OBSERVED_NO_ORDER_CHECKED',
        account_mode='default', mode_was_renamed=False, account_mapping_verified=True,
        balance_source='observed_native_perp_usdc', balance_usd=str(equity),
        exchange_reported_available_usd=str(available), test_plan_checked=False,
        budget_diagnostics=None, order_requests_sent=0, trade_authorized=False)
    if plan is not None:
        meta = reader.read('meta')
        assets = [x for x in meta.get('universe', []) if x.get('name') == symbol]
        if len(assets) != 1 or assets[0].get('isDelisted', False) is not False:
            raise checks.Blocked('ASSET_UNAVAILABLE')
        diagnostics = {}
        checks.plan_check(plan, symbol, assets[0].get('szDecimals'), unheld, available,
            max_size, active=active, metadata_max_leverage=assets[0].get('maxLeverage'),
            diagnostics=diagnostics)
        result.update(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',
                      test_plan_checked=True, budget_diagnostics=diagnostics)
    if reader.read('userAbstraction', user=account) != first:
        raise checks.Blocked('ACCOUNT_MODE_CHANGED_RECHECK')
    if time.monotonic() - started > 15:
        raise checks.Blocked('ACCOUNT_SNAPSHOT_EXPIRED')
    return result


def budget_for_role(env, role, account, agent, plan, reader):
    route = route_for(env, role, account, agent, plan['side'])
    mode = reader.read('userAbstraction', user=route['account'])
    if mode == 'default':
        return default_native_snapshot(route, reader, plan['symbol'], plan=plan,
            allow_owned_exposure=(env.get('HL_TESTNET_RUNTIME_MODE')=='long_stream_testnet_v1'
                                  and role=='short_account'))
    if mode not in ('disabled', 'unifiedAccount'):
        raise checks.Blocked('ACCOUNT_MODE_REQUIRES_REVIEW')
    return checks.run_check({'HL_TESTNET_ACCOUNT_ADDRESS': account,
        'HL_TESTNET_AGENT_ADDRESS': agent, 'HL_TESTNET_RUNTIME_MODE': 'read_only',
        'HL_TESTNET_CHECK_SYMBOL': plan['symbol'], 'HL_TESTNET_CHECK_PLAN': json.dumps(plan)}, client=reader)


def inspect_second(env, *, reader=None):
    """No signature, no reservation and no execution imports/calls."""
    report = dict(status='DISABLED', environment='testnet', order_requests_sent=0,
        signing_tested=False, entry_sending_enabled=False, key_present=False,
        key_address_verified=False, specific_order_checked=False)
    if env.get('HL_TESTNET_CONNECTION_REVIEW') != 'two_account_no_orders_v1':
        return report
    if (env.get('RENDER_SERVICE_ID') != SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') not in ('read_only', 'cancel_monitor_testnet_v1')):
        return {**report, 'status': 'REVIEW_MODE_NOT_ALLOWED'}
    try:
        route = route_for(env, 'short_account')
        reader = checks.InfoReader() if reader is None else reader
        report['public_account'] = default_native_snapshot(route, reader, 'BTC')
        report['key_present'] = bool(env.get(key_field(env, 'short_account'), ''))
        if report['key_present']:
            wallet_for_role(env, 'short_account', route['account'], route['agent'])
            report['key_address_verified'] = True
        report['status'] = ('LOCAL_KEY_AND_PUBLIC_CAPACITY_CHECKED_NO_ORDERS' if report['key_address_verified']
                            else 'PUBLIC_CAPACITY_CHECKED_WAITING_FOR_PRIVATE_KEY')
    except checks.Blocked as exc:
        report['status'] = str(exc)
    except Exception:
        report['status'] = 'CONNECTION_REVIEW_UNAVAILABLE'
    report['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
    report['public_reads'] = getattr(reader, 'calls', 0)
    return report
