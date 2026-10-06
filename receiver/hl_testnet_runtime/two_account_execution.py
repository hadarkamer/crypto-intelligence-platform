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
# Minimum observed address allowance: entry, stop, take, remainder cancellation
# and one reduce-only close. This is neither a reservation nor a guarantee of
# future capacity, retry coverage, IP headroom or an implemented emergency path.
ENTRY_ACTION_HEADROOM = 5
CONTINUOUS_RELEASE = 'continuous_testnet_v1'


def short_entry_scope(env, card_id=None):
    """Return an optional exact trial fence, never an execution authorization.

    The original short release always needs its exact card. The continuous
    Testnet release may omit that fence only beside the approved emergency
    supervisor and the explicit two-account stream. An exact card, when set,
    still restricts the continuous release to that one experiment. All ordinary
    account, freshness, fill-notification and durable dispatch gates remain.
    """
    release = env.get('HL_TESTNET_EMERGENCY_RELEASE', '')
    if release not in ('', CONTINUOUS_RELEASE):
        raise checks.Blocked('SHORT_ENTRY_SCOPE_CONFIGURATION_REQUIRED')
    trial = env.get('HL_TESTNET_SHORT_TRIAL_CARD_ID', '')
    if trial and (not isinstance(trial, str) or not re.fullmatch(r'[0-9a-f]{64}', trial)):
        raise checks.Blocked('SHORT_ENTRY_OUTSIDE_EXACT_TRIAL_CARD')
    if not trial:
        if (release != CONTINUOUS_RELEASE
                or env.get('RENDER_SERVICE_ID') != SERVICE
                or env.get('HL_TESTNET_RUNTIME_MODE') != 'long_stream_testnet_v1'
                or env.get('HL_TESTNET_FILLED_DISPATCH') != 'approved_long_stream_v1'
                or env.get('HL_TESTNET_LONG_STREAM') != 'approved_alerts_v1'
                or env.get('HL_TESTNET_SHORT_STREAM') != 'approved_alerts_v1'
                or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') not in ('true', 'false')
                or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') not in ('true', 'false')
                or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'
                or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
                or env.get('HL_TESTNET_EMERGENCY_CLOSE') != 'approved_testnet_v1'
                or env.get('HL_TESTNET_FILLED_CARD_ID')
                or env.get('HL_TESTNET_SAFETY_PIPELINE')
                or env.get('HL_TESTNET_CARD_SYNC')):
            raise checks.Blocked('SHORT_ENTRY_OUTSIDE_EXACT_TRIAL_CARD')
    if card_id is not None:
        if (not isinstance(card_id, str) or not re.fullmatch(r'[0-9a-f]{64}', card_id)
                or (trial and trial != card_id)):
            raise checks.Blocked('SHORT_ENTRY_OUTSIDE_EXACT_TRIAL_CARD')
    return trial or None


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
    return _default_native_snapshot(route,reader,symbol,first,started,plan=plan,
        allow_owned_exposure=allow_owned_exposure)


def _default_native_snapshot(route, reader, symbol, first, started, *, plan=None,
                             allow_owned_exposure=False):
    """Use only this invocation's first mode probe; never reuse prior evidence."""
    account, agent = route['account'], route['agent']
    if account != PHANTOM:
        raise checks.Blocked('DEFAULT_SCOPE_NOT_APPROVED')
    if first != 'default':
        raise checks.Blocked('ACCOUNT_MODE_CHANGED_RECHECK')
    observations = None
    if (getattr(reader, 'parallel', False) is True
            and callable(getattr(reader, 'read_many', None))):
        requests = [('userRole', {'user': account}),
                    ('userRole', {'user': agent}),
                    ('clearinghouseState', {'user': account}),
                    ('spotClearinghouseState', {'user': account}),
                    ('activeAssetData', {'user': account, 'coin': symbol})]
        if plan is not None:
            requests.append(('meta', {}))
        # All independent responses must complete before any validation. The
        # first/final mode probes are deliberately uncached and sequential.
        values = reader.read_many(requests)
        observations = {(kind, kwargs.get('user'), kwargs.get('coin')): value
                        for (kind, kwargs), value in zip(requests, values)}

    def sample(kind, *, user=None, coin=None):
        if observations is not None:
            return observations[(kind, user, coin)]
        kwargs = {}
        if user is not None:
            kwargs['user'] = user
        if coin is not None:
            kwargs['coin'] = coin
        return reader.read(kind, **kwargs)

    if sample('userRole', user=account) != {'role': 'user'}:
        raise checks.Blocked('INDEPENDENT_TEST_ACCOUNT_REQUIRED')
    link = sample('userRole', user=agent)
    if (not isinstance(link, dict) or link.get('role') != 'agent'
            or checks.address((link.get('data') or {}).get('user')) != account):
        raise checks.Blocked('AGENT_ACCOUNT_MISMATCH')
    perp = sample('clearinghouseState', user=account)
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
    spot = sample('spotClearinghouseState', user=account)
    total, _ = checks.unified_usdc(spot)
    if total != 0 or any(checks.number(row.get('total')) > 0 for row in spot['balances']):
        raise checks.Blocked('DEFAULT_OTHER_BALANCES_REQUIRE_REVIEW')
    active = sample('activeAssetData', user=account, coin=symbol)
    available, max_size = checks.capacity(active, account, symbol)
    if available <= 0 or max_size <= 0:
        raise checks.Blocked('NO_USABLE_CAPACITY_OBSERVED')
    result = dict(status='NATIVE_CAPACITY_OBSERVED_NO_ORDER_CHECKED',
        account_mode='default', mode_was_renamed=False, account_mapping_verified=True,
        balance_source='observed_native_perp_usdc', balance_usd=str(equity),
        exchange_reported_available_usd=str(available), test_plan_checked=False,
        budget_diagnostics=None, order_requests_sent=0, trade_authorized=False)
    if plan is not None:
        meta = sample('meta')
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
    # Begin the original fifteen-second sample clock before its first mode read.
    # The default path keeps this probe and its independent final probe, without
    # immediately asking the same question a third time inside that invocation.
    started = time.monotonic()
    mode = reader.read('userAbstraction', user=route['account'])
    if mode == 'default':
        return _default_native_snapshot(route,reader,plan['symbol'],mode,started,plan=plan,
            allow_owned_exposure=(env.get('HL_TESTNET_RUNTIME_MODE')=='long_stream_testnet_v1'
                                  and role=='short_account'))
    if mode not in ('disabled', 'unifiedAccount'):
        raise checks.Blocked('ACCOUNT_MODE_REQUIRES_REVIEW')
    return checks.run_check({'HL_TESTNET_ACCOUNT_ADDRESS': account,
        'HL_TESTNET_AGENT_ADDRESS': agent, 'HL_TESTNET_RUNTIME_MODE': 'read_only',
        'HL_TESTNET_CHECK_SYMBOL': plan['symbol'], 'HL_TESTNET_CHECK_PLAN': json.dumps(plan)}, client=reader)


def entry_action_headroom(account, reader):
    """Read-only entry gate. Never apply it to protection or closing requests.

    The venue nets reserved capacity into used/surplus. Reject ambiguous or
    malformed counters rather than interpreting missing capacity as unlimited.
    No allowance is purchased or reserved and no order is sent here.
    """
    raw = reader.read('userRateLimit', user=checks.address(account))
    names = ('nRequestsCap', 'nRequestsUsed', 'nRequestsSurplus')
    if (not isinstance(raw, dict) or any(type(raw.get(k)) is not int
            or not 0 <= raw[k] < 2**63 for k in names)):
        raise checks.Blocked('ENTRY_ACTION_CAPACITY_NOT_VERIFIED')
    cap, used, surplus = (raw[k] for k in names)
    if used and surplus:
        raise checks.Blocked('ENTRY_ACTION_CAPACITY_NOT_VERIFIED')
    remaining = cap - used + surplus
    if remaining < ENTRY_ACTION_HEADROOM:
        raise checks.Blocked('ENTRY_ACTION_HEADROOM_INSUFFICIENT')
    return remaining


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
