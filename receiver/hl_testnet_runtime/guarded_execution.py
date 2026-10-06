"""One-shot Testnet dispatch through the current-settings budget precheck.

No work on import, no HTTP endpoint, no automatic signing from the web service.
The deployed web service calls review_only; sending remains an explicit separate
call and is refused on ephemeral Render storage. No source times are rewritten.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from . import checks

FIELDS = {'kind', 'event_id', 'symbol', 'side', 'entry', 'stop', 'take_profit', 'at'}
PLAN_FIELDS = ('symbol', 'side', 'entry', 'stop', 'take_profit')
MAX_SOURCE_AGE_SECONDS = 60  # Same bound as the existing one-shot test sender.


class GuardError(ValueError):
    """Fixed local error codes only."""


def _frozen(message):
    if not isinstance(message, dict) or set(message) != FIELDS or message.get('kind') != 'SIGNAL':
        raise GuardError('COMPLETE_SOURCE_SIGNAL_REQUIRED')
    if not isinstance(message['event_id'], str) or not checks.SYMBOL.fullmatch(message['symbol']):
        raise GuardError('INVALID_SOURCE_SIGNAL')
    if not message['event_id'] or len(message['event_id']) > 100:
        raise GuardError('INVALID_SOURCE_SIGNAL')
    if message['side'] not in ('LONG', 'SHORT'):
        raise GuardError('INVALID_SOURCE_SIGNAL')
    for field in ('entry', 'stop', 'take_profit'):
        if checks.number(message[field]) <= 0:
            raise GuardError('INVALID_SOURCE_PRICE')
    if not isinstance(message['at'], str) or len(message['at']) > 40:
        raise GuardError('SOURCE_TIME_REQUIRED')
    try:
        at = datetime.fromisoformat(message['at'].replace('Z', '+00:00'))
        if at.utcoffset() is None:
            raise ValueError()
    except ValueError:
        raise GuardError('SOURCE_TIME_REQUIRED') from None
    return deepcopy(message), at


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def journal_status(value, *, env=None):
    """No file creation. Render local SQLite must be on an existing data mount.

    This check deliberately refuses the currently deployed Free web service.
    A persistent directory is necessary, not a complete durability proof. The
    existing AttemptLog additionally validates private file ownership and schema.
    """
    env = os.environ if env is None else env
    if not isinstance(value, (str, Path)) or not str(value):
        return 'PERSISTENT_JOURNAL_NOT_CONFIGURED'
    try:
        path = Path(value)
        parent = path.parent
        if (not path.is_absolute() or not path.name.endswith('.hl-testnet.sqlite3')
                or parent.resolve(strict=True) != parent or path.is_symlink()
                or not parent.is_dir() or parent.stat().st_mode & 0o077
                or parent.stat().st_uid != os.geteuid()
                or path.is_relative_to(Path(__file__).resolve().parents[1])):
            return 'PRIVATE_JOURNAL_PATH_REQUIRED'
        if path.exists():
            meta = path.stat()
            if (not stat.S_ISREG(meta.st_mode) or meta.st_mode & 0o077
                    or meta.st_uid != os.geteuid() or meta.st_nlink != 1):
                return 'PRIVATE_JOURNAL_PATH_REQUIRED'
        if env.get('RENDER') or env.get('RENDER_SERVICE_ID'):
            # System/tmpfs mounts do not provide a durable order journal.
            mounts = [p for p in (parent, *parent.parents)
                      if str(p) not in ('/', '/tmp', '/run', '/dev', '/proc', '/sys', '/opt', '/opt/render')
                      and not str(p).startswith(('/dev/', '/proc/', '/sys/', '/run/', '/tmp/'))
                      and os.path.ismount(p)]
            if not mounts:
                return 'RENDER_PERSISTENT_STORAGE_REQUIRED'
    except (OSError, ValueError):
        return 'PRIVATE_JOURNAL_PATH_REQUIRED'
    return 'JOURNAL_PATH_CHECKED'


class RecordedReader:
    def __init__(self, delegate):
        self.delegate = delegate
        self.active = None
        self.metadata = None
    @property
    def calls(self):
        return getattr(self.delegate, 'calls', 0)
    def read(self, kind, *, user=None, coin=None):
        value = self.delegate.read(kind, user=user, coin=coin)
        if kind == 'activeAssetData':
            self.active = deepcopy(value)
        if kind == 'meta':
            self.metadata = deepcopy(value)
        return value


def review_only(message, *, account, agent, journal=None, exit_type=None, client=None, now=None, env=None):
    """Diagnose all prerequisites; never read a key, sign, or send an order.

    A historical source can be inspected here, but its age remains a blocker for
    dispatch. Missing exit-type or persistence is reported, never guessed away.
    """
    report = {'version': 'budget-to-dispatch-v1', 'mode': 'testnet', 'phase': 'review_only',
              'eligible_for_controlled_attempt': False, 'blockers': [], 'price_precision_fields': [],
              'prices_changed': False, 'source_time_changed': False, 'risk_rule_changed': False,
              'signing_tested': False, 'order_requests_sent': 0}
    try:
        signal, at = _frozen(message)
        account, agent = checks.address(account), checks.address(agent)
        report['source_digest'] = _digest(signal)
        instant = datetime.now(timezone.utc) if now is None else now
        if instant.utcoffset() is None:
            raise GuardError('UTC_CLOCK_REQUIRED')
        age = (instant - at).total_seconds()
        if not 0 <= age <= MAX_SOURCE_AGE_SECONDS:
            report['blockers'].append('SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST')
        if exit_type not in ('market', 'limit', 'tp_limit_sl_market'):
            report['blockers'].append('EXPLICIT_EXIT_TYPE_REQUIRED')
        else:
            report['take_profit_type'] = 'limit' if exit_type == 'tp_limit_sl_market' else exit_type
            report['stop_loss_type'] = 'market' if exit_type == 'tp_limit_sl_market' else exit_type
        storage = journal_status(journal, env=env)
        report['journal_status'] = storage
        if storage != 'JOURNAL_PATH_CHECKED':
            report['blockers'].append(storage)
        plan = {key: signal[key] for key in PLAN_FIELDS}
        reader = RecordedReader(checks.InfoReader() if client is None else client)
        budget = checks.run_check({'HL_TESTNET_RUNTIME_MODE': 'read_only',
            'HL_TESTNET_ACCOUNT_ADDRESS': account, 'HL_TESTNET_AGENT_ADDRESS': agent,
            'HL_TESTNET_CHECK_SYMBOL': signal['symbol'], 'HL_TESTNET_CHECK_PLAN': json.dumps(plan)}, client=reader)
        report['budget_status'] = budget.get('status')
        report['budget_passed'] = (budget.get('status') == 'PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION'
                                   and budget.get('test_plan_checked') is True)
        if not report['budget_passed']:
            report['blockers'].append('BUDGET_OR_INPUT_PRECHECK_FAILED')
        if isinstance(reader.metadata, dict):
            items = [x for x in reader.metadata.get('universe', [])
                     if isinstance(x, dict) and x.get('name') == signal['symbol']]
            if len(items) == 1 and type(items[0].get('szDecimals')) is int:
                decimals = items[0]['szDecimals']
                for key in ('entry', 'stop', 'take_profit'):
                    price = checks.number(signal[key]).normalize()
                    if price != price.to_integral_value() and (len(price.as_tuple().digits) > 5
                            or price.as_tuple().exponent < -(6 - decimals)):
                        report['price_precision_fields'].append(key)
        if isinstance(reader.active, dict):
            mark = checks.number(reader.active.get('markPx'))
            low, high = sorted((checks.number(signal['stop']), checks.number(signal['take_profit'])))
            report['testnet_price_within_exit_range'] = low < mark < high
            if not report['testnet_price_within_exit_range']:
                report['blockers'].append('TESTNET_PRICE_OUTSIDE_SUPPLIED_EXIT_RANGE')
        else:
            report['blockers'].append('TESTNET_PRICE_NOT_VERIFIED')
        diagnostics = budget.get('budget_diagnostics')
        report['budget_plan_bound'] = (isinstance(diagnostics, dict)
            and diagnostics.get('plan_sha256') == _digest(plan)
            and diagnostics.get('current_settings_passed') is True)
        if report['budget_passed'] and not report['budget_plan_bound']:
            report['blockers'].append('BUDGET_NOT_BOUND_TO_EXACT_PLAN')
        report['public_reads'] = reader.calls
        report['eligible_for_controlled_attempt'] = not report['blockers']
    except (GuardError, checks.Blocked) as exc:
        report['blockers'].append(str(exc))
    except Exception:
        report['blockers'].append('GUARD_INPUT_OR_CHECK_UNAVAILABLE')
    return report


def submit_checked(message, *, account, agent, journal, exit_type, enable_testnet=False):
    """Budget gate -> original one-shot sender; never usable via web requests.

    The caller must execute this in a supervised process with durable storage.
    Authorization is an actual bool, not an environment string. The legacy
    sender retains account/nonce/response/read-back checks. No source rewriting.
    """
    if enable_testnet is not True:
        return {'mode': 'testnet', 'status': 'DISABLED', 'order_requests_sent': 0}
    signal, at = _frozen(message)
    started = time.monotonic()
    review = review_only(signal, account=account, agent=agent, journal=journal, exit_type=exit_type)
    if not review['eligible_for_controlled_attempt']:
        return {**review, 'status': 'BLOCKED_BEFORE_SIGNING'}
    # Bound the budget observation before entering the sender's own fresh checks.
    if time.monotonic() - started > 8:
        return {**review, 'eligible_for_controlled_attempt': False,
                'status': 'BUDGET_SAMPLE_EXPIRED_BEFORE_DISPATCH', 'order_requests_sent': 0}
    if not 0 <= (datetime.now(timezone.utc) - at).total_seconds() <= MAX_SOURCE_AGE_SECONDS:
        return {**review, 'eligible_for_controlled_attempt': False,
                'status': 'SOURCE_EXPIRED_BEFORE_DISPATCH', 'order_requests_sent': 0}
    import hyperliquid_testnet_executor as sender
    # Passing the exact frozen message avoids replacing old signals with a new at.
    result = sender.submit_once(signal, account=account, journal=journal,
                                exit_type=exit_type, enable_testnet=True)
    return {'mode': 'testnet', 'budget_gate_passed': True, **result}
