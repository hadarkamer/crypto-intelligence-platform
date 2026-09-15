"""Read-only Testnet readiness. No signing, orders, transfers, or account changes.

Unified account balances come from spotClearinghouseState, not perp equity.
Unheld balance is NOT buying power: also inspect the exchange's activeAssetData.
All output is a fixed, redacted report. A pass is not order acceptance.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, localcontext
import http.client
import hashlib
import importlib.metadata
import json
import re
import time

HOST = 'api.hyperliquid-testnet.xyz'
ADDRESS = re.compile(r'0x[0-9a-fA-F]{40}\Z')
SYMBOL = re.compile(r'[A-Z][A-Z0-9]{0,19}\Z')
MAX_BYTES = 2 * 1024 * 1024
BUDGET_VERSION = 'current-settings-budget-v2'


class Blocked(ValueError):
    """Fixed local codes only; never include remote text or configuration."""


def address(value):
    if not isinstance(value, str) or not ADDRESS.fullmatch(value) or int(value[2:], 16) == 0:
        raise Blocked('PUBLIC_ADDRESS_REQUIRED')
    return value.lower()


def number(value, *, signed=False):
    if not isinstance(value, str) or not 1 <= len(value) <= 80:
        raise Blocked('INVALID_NUMBER')
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise Blocked('INVALID_NUMBER') from None
    if (not result.is_finite() or len(result.as_tuple().digits) > 28
            or abs(result) > Decimal('1e15') or (not signed and result < 0)
            or (result != 0 and abs(result) < Decimal('1e-15'))):
        raise Blocked('INVALID_NUMBER')
    return result


def decode(raw):
    def pairs(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise Blocked('INVALID_RESPONSE')
            obj[key] = value
        return obj
    def invalid(_):
        raise Blocked('INVALID_RESPONSE')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)
    except (ValueError, UnicodeError, RecursionError):
        raise Blocked('INVALID_RESPONSE') from None


class InfoReader:
    """Fixed Testnet /info, fixed read types, no retries or redirects."""
    def __init__(self):
        self.calls = 0

    def read(self, kind, *, user=None, coin=None):
        if HOST != 'api.hyperliquid-testnet.xyz':
            raise Blocked('TESTNET_ONLY')
        if kind == 'meta' and user is None and coin is None:
            body = {'type': kind}
        elif kind in ('userRole', 'userAbstraction', 'spotClearinghouseState', 'clearinghouseState') and coin is None:
            body = {'type': kind, 'user': address(user)}
        elif kind == 'activeAssetData' and isinstance(coin, str) and SYMBOL.fullmatch(coin):
            body = {'type': kind, 'user': address(user), 'coin': coin}
        else:
            raise Blocked('READ_TYPE_NOT_ALLOWED')
        connection = http.client.HTTPSConnection(HOST, timeout=4)
        self.calls += 1
        started = time.monotonic()
        try:
            connection.request('POST', '/info', json.dumps(body).encode(),
                               {'Content-Type': 'application/json', 'Accept': 'application/json'})
            response = connection.getresponse()
            if response.status != 200:
                raise Blocked('READ_UNAVAILABLE')
            raw = response.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES or time.monotonic() - started > 8:
                raise Blocked('RESPONSE_BOUND_EXCEEDED')
            return decode(raw)
        except (OSError, http.client.HTTPException):
            raise Blocked('READ_UNAVAILABLE') from None
        finally:
            connection.close()


def unified_usdc(state):
    """Return total and unheld USDC; neither is labeled available perp margin."""
    rows = state.get('balances') if isinstance(state, dict) else None
    if not isinstance(rows, list) or len(rows) > 10000:
        raise Blocked('INVALID_BALANCE_RESPONSE')
    matches = []
    for row in rows:
        if not isinstance(row, dict):
            raise Blocked('INVALID_BALANCE_RESPONSE')
        if row.get('coin') == 'USDC' or row.get('token') == 0:
            if row.get('coin') != 'USDC' or type(row.get('token')) is not int or row['token'] != 0:
                raise Blocked('WRONG_USDC_TOKEN')
            matches.append(row)
    if len(matches) > 1:
        raise Blocked('DUPLICATE_USDC')
    if not matches:
        return Decimal(0), Decimal(0)
    total, hold = (number(matches[0].get(k)) for k in ('total', 'hold'))
    if hold > total:
        raise Blocked('INVALID_HOLD')
    return total, total - hold


def capacity(data, account, symbol):
    if (not isinstance(data, dict) or data.get('coin') != symbol
            or address(data.get('user')) != address(account)):
        raise Blocked('CAPACITY_ACCOUNT_OR_ASSET_MISMATCH')
    values = []
    for field in ('availableToTrade', 'maxTradeSzs'):
        pair = data.get(field)
        if not isinstance(pair, list) or len(pair) != 2:
            raise Blocked('INVALID_CAPACITY')
        # Deliberately conservative: no undocumented long/short array ordering.
        values.append(min(number(x) for x in pair))
    if number(data.get('markPx')) <= 0:
        raise Blocked('INVALID_CAPACITY')
    return tuple(values)


def plan_check(plan, symbol, decimals, unheld, available, max_size, *,
               active=None, metadata_max_leverage=None, diagnostics=None):
    """Read-only estimate using the account's EXISTING leverage, never setting it.

    $20 distance risk and original prices/quantity are unchanged. Margin follows
    Hyperliquid's size * mark / leverage, plus adverse entry-vs-mark loss and a
    separate 1% notional lab reserve (not a claim about actual fees).
    Exchange size and available-margin caps AND unheld USDC remain enforced.
    Both array minima remain conservative; no undocumented side index is assumed.

    With no active context retain the v1 cash-only check for offline callers.
    Production run_check always supplies active context; invalid/missing leverage
    then blocks, never falls back to a convenient multiplier. This is not order
    authorization, liquidation validation, or proof of future available funds.
    """
    if not isinstance(plan, dict) or set(plan) != {'symbol', 'side', 'entry', 'stop', 'take_profit'}:
        raise Blocked('INVALID_TEST_PLAN')
    if plan['symbol'] != symbol or plan['side'] not in ('LONG', 'SHORT'):
        raise Blocked('INVALID_TEST_PLAN')
    if type(decimals) is not int or not 0 <= decimals <= 6:
        raise Blocked('ASSET_UNAVAILABLE')
    entry, stop, take = (number(plan[k]) for k in ('entry', 'stop', 'take_profit'))
    if min(entry, stop, take) <= 0 or not (stop < entry < take if plan['side'] == 'LONG' else take < entry < stop):
        raise Blocked('INVALID_TEST_PLAN')
    for value in (entry, stop, take):
        value = value.normalize()
        if value != value.to_integral_value() and (len(value.as_tuple().digits) > 5 or value.as_tuple().exponent < -(6 - decimals)):
            raise Blocked('PRICE_PRECISION_NO_ROUNDING')
    with localcontext() as ctx:
        ctx.prec = 50
        step = Decimal(1).scaleb(-decimals)
        size = ((Decimal(20) / abs(entry - stop)) / step).to_integral_value(rounding=ROUND_DOWN) * step
        notional = size * entry
        if size <= 0 or not Decimal(10) <= notional <= Decimal(5000):
            raise Blocked('OUTSIDE_LAB_SIZE_BOUNDS')
        full_cash = notional * Decimal('1.01')
        legacy = {
            'quantity_within_exchange_cap': size <= max_size,
            'full_cash_within_unheld_balance': full_cash <= unheld,
            'full_cash_within_exchange_available': full_cash <= available,
        }
        if diagnostics is not None:
            diagnostics.update(version=BUDGET_VERSION,
                plan_sha256=hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                legacy_checks_same_sample=legacy, legacy_passed=all(legacy.values()),
                capacity_selection='minimum_of_both_sides', current_settings_checks=None,
                failed_checks=[], existing_leverage_used=None, account_settings_changed=False,
                prices_changed=False, risk_rule_changed=False, order_authorization=False)
        if active is None:
            if not all(legacy.values()):
                raise Blocked('OUTSIDE_CONSERVATIVE_LAB_BUDGET')
            return True
        leverage = active.get('leverage') if isinstance(active, dict) else None
        if (not isinstance(leverage, dict) or leverage.get('type') not in ('cross', 'isolated')
                or type(leverage.get('value')) is not int or leverage['value'] < 1
                or type(metadata_max_leverage) is not int
                or not 1 <= metadata_max_leverage <= 100
                or leverage['value'] > metadata_max_leverage):
            raise Blocked('CURRENT_LEVERAGE_NOT_VERIFIED')
        mark = number(active.get('markPx'))
        if mark <= 0:
            raise Blocked('INVALID_CAPACITY')
        initial_margin = size * mark / Decimal(leverage['value'])
        # Conservative check at the requested limit, not a promised fill price.
        adverse_price = max(entry - mark if plan['side'] == 'LONG' else mark - entry, Decimal(0))
        adverse_loss = size * adverse_price
        reserve = size * max(entry, mark) * Decimal('0.01')
        required = initial_margin + adverse_loss + reserve
        current = {
            'quantity_within_exchange_cap': size <= max_size,
            'estimated_margin_within_unheld_balance': required <= unheld,
            'estimated_margin_within_exchange_available': required <= available,
            'mark_notional_within_lab_cap': size * mark <= Decimal(5000),
        }
        failures = [name for name, passed in current.items() if not passed]
        if diagnostics is not None:
            diagnostics.update(current_settings_checks=current, failed_checks=failures,
                existing_leverage_used=leverage['value'], margin_mode=leverage['type'],
                adverse_entry_mark_loss_included=adverse_loss > 0,
                reserve_basis='one_percent_of_max_entry_mark_notional',
                # A market-context warning only; not an invented entry filter.
                mark_within_supplied_exit_range=min(stop, take) <= mark <= max(stop, take),
                current_settings_passed=not failures)
        if failures:
            codes = {
                'quantity_within_exchange_cap': 'QUANTITY_EXCEEDS_EXCHANGE_CAP',
                'estimated_margin_within_unheld_balance': 'ESTIMATED_MARGIN_EXCEEDS_UNHELD_USDC',
                'estimated_margin_within_exchange_available': 'ESTIMATED_MARGIN_EXCEEDS_EXCHANGE_AVAILABLE',
                'mark_notional_within_lab_cap': 'MARK_NOTIONAL_EXCEEDS_LAB_CAP',
            }
            raise Blocked(codes[failures[0]])
    return True


def key_matches(key, expected_agent):
    """Optional local derivation only. No signature and no key in any request."""
    if not isinstance(key, str) or not re.fullmatch(r'(?:0x)?[0-9a-fA-F]{64}', key):
        raise Blocked('INVALID_AGENT_KEY')
    try:
        if importlib.metadata.version('hyperliquid-python-sdk') != '0.24.0':
            raise Blocked('SDK_VERSION_MISMATCH')
        from eth_account import Account
        derived = Account.from_key(key).address
    except Blocked:
        raise
    except Exception:
        raise Blocked('LOCAL_KEY_CHECK_UNAVAILABLE') from None
    if address(derived) != expected_agent:
        raise Blocked('KEY_DOES_NOT_MATCH_AGENT')
    return True


def run_check(env, *, client=None, local_key_check=key_matches):
    result = {'mode': 'testnet_read_only', 'status': 'WAITING_FOR_ADDRESSES',
              'account_mapping_verified': False, 'balance_source': None,
              'positive_usdc_observed': False, 'unheld_balance_observed': False,
              'exchange_capacity_observed': False, 'test_plan_checked': False,
              'local_key_address_checked': False, 'signing_tested': False,
              'budget_diagnostics': None,
              'order_requests_sent': 0, 'public_reads': 0,
              'checked_at_utc': datetime.now(timezone.utc).isoformat()}
    started = time.monotonic()
    try:
        if env.get('HL_TESTNET_RUNTIME_MODE', 'read_only') != 'read_only':
            raise Blocked('READ_ONLY_RUNTIME_ONLY')
        raw_account, raw_agent = (env.get(k, '') for k in ('HL_TESTNET_ACCOUNT_ADDRESS', 'HL_TESTNET_AGENT_ADDRESS'))
        # Reject accidental keys in address fields even when other fields missing.
        account = address(raw_account) if raw_account else None
        agent = address(raw_agent) if raw_agent else None
        if not account or not agent:
            return result
        if account == agent:
            raise Blocked('ACCOUNT_AND_AGENT_MUST_DIFFER')
        symbol = env.get('HL_TESTNET_CHECK_SYMBOL', 'BTC')
        if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
            raise Blocked('EXACT_SYMBOL_REQUIRED')
        raw_plan = env.get('HL_TESTNET_CHECK_PLAN', '')
        if not isinstance(raw_plan, str) or len(raw_plan) > 1024:
            raise Blocked('INVALID_TEST_PLAN')
        plan = decode(raw_plan) if raw_plan else None
        client = client if client is not None else InfoReader()
        if client.read('userRole', user=account) != {'role': 'user'}:
            raise Blocked('INDEPENDENT_TEST_ACCOUNT_REQUIRED')
        role = client.read('userRole', user=agent)
        if (not isinstance(role, dict) or role.get('role') != 'agent'
                or not isinstance(role.get('data'), dict) or address(role['data'].get('user')) != account):
            raise Blocked('AGENT_ACCOUNT_MISMATCH')
        result['account_mapping_verified'] = True
        mode = client.read('userAbstraction', user=account)
        if mode == 'unifiedAccount':
            total, unheld = unified_usdc(client.read('spotClearinghouseState', user=account))
            result['balance_source'] = 'unified_spot_state'
        elif mode == 'disabled':
            state = client.read('clearinghouseState', user=account)
            summary = state.get('marginSummary') if isinstance(state, dict) else None
            if not isinstance(summary, dict):
                raise Blocked('INVALID_BALANCE_RESPONSE')
            total = number(summary.get('accountValue'), signed=True)
            unheld = number(state.get('withdrawable'))
            result['balance_source'] = 'standard_perp_state'
        else:
            raise Blocked('ACCOUNT_MODE_REQUIRES_REVIEW')
        result['positive_usdc_observed'] = total > 0
        result['unheld_balance_observed'] = unheld > 0
        active = client.read('activeAssetData', user=account, coin=symbol)
        available, max_size = capacity(active, account, symbol)
        result['exchange_capacity_observed'] = available > 0 and max_size > 0
        key = env.get('HL_TESTNET_AGENT_KEY', '')
        if key:
            result['local_key_address_checked'] = local_key_check(key, agent)
        if plan is not None:
            meta = client.read('meta')
            universe = meta.get('universe') if isinstance(meta, dict) else None
            if not isinstance(universe, list) or len(universe) > 10000:
                raise Blocked('INVALID_METADATA')
            assets = [a for a in universe if isinstance(a, dict) and a.get('name') == symbol]
            if (len(assets) != 1 or assets[0].get('isDelisted', False) is not False
                    or type(assets[0].get('szDecimals')) is not int or not 0 <= assets[0]['szDecimals'] <= 6):
                raise Blocked('ASSET_UNAVAILABLE')
            result['budget_diagnostics'] = {}
            result['test_plan_checked'] = plan_check(
                plan, symbol, assets[0]['szDecimals'], unheld, available, max_size,
                active=active, metadata_max_leverage=assets[0].get('maxLeverage'),
                diagnostics=result['budget_diagnostics'])
        if time.monotonic() - started > 20:
            raise Blocked('SAMPLE_EXPIRED')
        if not result['positive_usdc_observed'] or not result['exchange_capacity_observed'] or unheld <= 0:
            result['status'] = 'NO_USABLE_CAPACITY_OBSERVED'
        else:
            result['status'] = 'PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION' if plan is not None else 'ACCOUNT_CHECKED_WAITING_FOR_TEST_PLAN'
    except Blocked as exc:
        result['status'] = str(exc)
        result['test_plan_checked'] = False
    except Exception:
        result['status'] = 'CHECK_UNAVAILABLE'
        result['test_plan_checked'] = False
    finally:
        result['public_reads'] = getattr(client, 'calls', 0)
    return result
