"""Approved half-threshold cancellation rule; evaluation and read-only review.

This module NEVER signs or sends cancellations. A cancellation candidate is not
an exchange receipt or an atomic cancel-if-unfilled. The later cancellation
transport must handle fills racing with cancellation, including child TP/SL.
No background poller, time-based cancellation, formulas, or entry orders added.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import os
import re
import time

POLICY = 'favorable-half-formula-threshold-v1'
SERVICE = 'srv-dakptbh594qs7395460g'
REVIEW_ENV = 'HL_TESTNET_CANCELLATION_RULE_REVIEW'
TABLE = 'hl_testnet_execution_v1.limit_cancel_rules'


class RuleError(ValueError):
    """Only local codes, never API response text, prices or credentials."""


def decimal(value, *, positive=True):
    if not isinstance(value, str) or not 1 <= len(value) <= 80:
        raise RuleError('INVALID_CANCEL_RULE_NUMBER')
    try:
        n = Decimal(value)
    except InvalidOperation:
        raise RuleError('INVALID_CANCEL_RULE_NUMBER') from None
    if (not n.is_finite() or len(n.as_tuple().digits) > 28 or abs(n) > Decimal('1e15')
            or n != 0 and abs(n) < Decimal('1e-15') or positive and n <= 0):
        raise RuleError('INVALID_CANCEL_RULE_NUMBER')
    return n


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def text(n):
    with localcontext() as ctx:
        ctx.prec = 60
        return format(n.normalize(), 'f')


def make_rule(*, rule_id, event_id, symbol, side, entry, size, entry_cloid,
              threshold_pct, source_digest, execution_digest):
    """Freeze formula threshold separately from rounded TP and SL distances.

    All percentages are percent units: '1.5' means 1.5%, not a 0.015 fraction.
    Entry MUST be the submitted limit price, not the average fill or the mark.
    There is deliberately no default threshold or time-to-cancel setting.
    """
    if (not isinstance(rule_id, str) or not re.fullmatch(r'[A-Z0-9_]{1,40}', rule_id)
            or not isinstance(event_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', event_id)
            or not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9]{0,19}', symbol)
            or side not in ('LONG', 'SHORT') or not isinstance(entry_cloid, str)
            or not re.fullmatch(r'0x[0-9a-f]{32}', entry_cloid)
            or any(not isinstance(x, str) or not re.fullmatch(r'[0-9a-f]{64}', x)
                   for x in (source_digest, execution_digest))):
        raise RuleError('INVALID_CANCEL_RULE_IDENTITY')
    price, qty, threshold = decimal(entry), decimal(size), decimal(threshold_pct)
    if threshold >= 100:
        raise RuleError('INVALID_FORMULA_THRESHOLD')
    with localcontext() as ctx:
        ctx.prec = 60
        half = threshold / 2
        boundary = price * (1 + (half / 100 if side == 'LONG' else -half / 100))
    return dict(policy=POLICY, rule_id=rule_id, event_id=event_id, symbol=symbol,
                side=side, entry=text(price), size=text(qty), entry_cloid=entry_cloid,
                threshold_pct=text(threshold), cancel_move_pct=text(half),
                cancel_price=text(boundary), source_digest=source_digest,
                execution_digest=execution_digest, reference='submitted_limit_price',
                direction='forecast_only', price_source='hyperliquid_testnet_mark',
                time_cancel_enabled=False)


def validate_rule(rule):
    try:
        fields = ('rule_id','event_id','symbol','side','entry','size','entry_cloid',
                  'threshold_pct','source_digest','execution_digest')
        rebuilt = make_rule(**{k: rule[k] for k in fields})
        if canonical(rebuilt) != canonical(rule):
            raise RuleError('CANCEL_RULE_CHANGED')
        return rebuilt
    except (KeyError, TypeError):
        raise RuleError('INVALID_CANCEL_RULE') from None


def evaluate(rule, order, position_size, mark, *, sample_age_seconds, threshold_seen=False):
    """Pure decision. Any observed fill excludes cancellation, even if flat now.

    A previously observed crossing can be latched by the private journal; a price
    returning inside the boundary does not erase it. Missing/stale samples do not
    authorize action. Future execution needs another check and post-cancel repair
    of protection if a fill races with the request; this decision alone is NOT
    that safety mechanism.
    """
    rule = validate_rule(rule)
    report = {'decision':'REVIEW_REQUIRED', 'cancel_candidate':False,
              'threshold_crossed':False, 'any_fill_observed':False,
              'cancel_requests_sent':0, 'time_cancel_enabled':False}
    if (type(sample_age_seconds) not in (int, float)
            or not 0 <= sample_age_seconds <= 10 or type(threshold_seen) is not bool):
        report['decision'] = 'STALE_OR_INVALID_OBSERVATION'
        return report
    try:
        wrapper, actual = order['order'], order['order']['order']
        status = wrapper['status']
        if (order['status'] != 'order' or actual['cloid'] != rule['entry_cloid']
                or actual['coin'] != rule['symbol']
                or actual['side'] != ('B' if rule['side'] == 'LONG' else 'A')
                or actual['reduceOnly'] is not False or actual['isTrigger'] is not False
                or actual['orderType'] != 'Limit'
                or decimal(actual['limitPx']) != decimal(rule['entry'])
                or decimal(actual['origSz']) != decimal(rule['size'])):
            raise RuleError('ORDER_NOT_BOUND_TO_CANCEL_RULE')
        remaining = decimal(actual['sz'], positive=False)
        original = decimal(actual['origSz'])
        position = decimal(position_size, positive=False)
        if not 0 <= remaining <= original:
            raise RuleError('INVALID_REMAINING_SIZE')
        # Status and historical remaining size take priority over price movement.
        if status == 'filled':
            report.update(decision='ENTRY_ALREADY_FILLED_NO_CANCEL', any_fill_observed=True)
            return report
        if remaining < original or position != 0:
            report.update(decision='FILL_OBSERVED_DO_NOT_CANCEL', any_fill_observed=True)
            return report
        if status != 'open':
            report['decision'] = 'ENTRY_NOT_OPEN_NO_CANCEL'
            return report
        current, boundary = decimal(mark), decimal(rule['cancel_price'])
        crossed = current >= boundary if rule['side'] == 'LONG' else current <= boundary
        candidate = crossed or threshold_seen
        report.update(threshold_crossed=crossed, cancel_candidate=candidate,
                      decision='CANCEL_CANDIDATE_NOT_SENT' if candidate else 'WAITING_BELOW_CANCEL_THRESHOLD')
    except (RuleError, KeyError, TypeError):
        report['decision'] = 'ORDER_OR_MARK_REQUIRES_REVIEW'
    return report


def rule_from_record(record, spec):
    """Use the exact committed submitted order, never infer a formula from a price."""
    from . import postgres_journal as pg
    if (not isinstance(spec, dict) or set(spec) != {'event_id','rule_id','threshold_pct','source_digest'}):
        raise RuleError('EXACT_CANCEL_RULE_SPEC_REQUIRED')
    prepared = pg.validate_prepared(record['prepared'])
    source, execution = prepared['source'], prepared['execution']
    if source['event_id'] != spec['event_id'] or digest(source) != spec['source_digest']:
        raise RuleError('CANCEL_RULE_SOURCE_MISMATCH')
    if record.get('action') is None:
        raise RuleError('NO_SUBMITTED_ORDER_TO_REVIEW')
    action = pg.validate_action(record['action'], prepared, record['account'])
    entry = action['orders'][0]
    return make_rule(rule_id=spec['rule_id'], event_id=source['event_id'],
                     symbol=execution['symbol'], side=execution['side'],
                     entry=entry['p'], size=entry['s'], entry_cloid=entry['c'],
                     threshold_pct=spec['threshold_pct'], source_digest=digest(source),
                     execution_digest=digest(execution))


def register_rule(journal, key, rule):
    """Explicit startup-only registration in staging, outside recurring checks.

    A later formula revision cannot replace the rule for an existing order.
    The original prepared record, attempts, account reservation and receipt are
    never updated by this feature. No credentials or signatures are persisted.
    """
    from .postgres_journal import LOCK
    rule = validate_rule(rule)
    with journal._transaction() as conn:
        journal._ready(conn)
        conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
        conn.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
            plan_key text PRIMARY KEY REFERENCES hl_testnet_execution_v1.prepared(plan_key),
            policy jsonb NOT NULL, threshold_seen boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
        conn.execute(f'REVOKE ALL ON {TABLE} FROM PUBLIC')
        conn.execute(f'INSERT INTO {TABLE}(plan_key,policy) VALUES(%s,%s::jsonb) ON CONFLICT DO NOTHING',
                     (key, canonical(rule)))
        previous = conn.execute(f'SELECT policy FROM {TABLE} WHERE plan_key=%s', (key,)).fetchone()
    if previous is None or canonical(previous[0]) != canonical(rule):
        raise RuleError('EXISTING_CANCEL_RULE_CHANGED_NO_OVERWRITE')
    with journal._transaction() as conn:
        stored = conn.execute(f'SELECT policy,threshold_seen FROM {TABLE} WHERE plan_key=%s', (key,)).fetchone()
    if stored is None or canonical(stored[0]) != canonical(rule):
        raise RuleError('CANCEL_RULE_READBACK_FAILED')
    return bool(stored[1])


def remember_crossing(journal, key, rule, result):
    """Sticky crossing evidence, but NEVER label a candidate as a cancellation."""
    if result['threshold_crossed'] is not True or result['cancel_candidate'] is not True:
        return
    with journal._transaction() as conn:
        changed = conn.execute(f'UPDATE {TABLE} SET threshold_seen=true WHERE plan_key=%s AND policy=%s::jsonb',
                               (key, canonical(rule))).rowcount
        if changed != 1:
            raise RuleError('CANCEL_RULE_READBACK_FAILED')


def review_configured(env):
    """Exactly one observation, no signing key read, cancel request or poller."""
    report = {'mode':'testnet_cancel_rule_review', 'status':'DISABLED',
              'cancel_requests_sent':0, 'signing_tested':False, 'continuous_monitoring':False}
    if (env.get('RENDER_SERVICE_ID') != SERVICE or env.get('HL_TESTNET_RUNTIME_MODE') != 'read_only'
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'):
        return report
    try:
        from . import checks, postgres_journal as pg
        import hyperliquid_testnet_executor as sender
        raw = env.get(REVIEW_ENV, '')
        if not isinstance(raw, str) or not 1 <= len(raw) <= 1024:
            raise RuleError('EXACT_CANCEL_RULE_SPEC_REQUIRED')
        spec = checks.decode(raw)
        account = pg.account_address(env.get('HL_TESTNET_ACCOUNT_ADDRESS'))
        journal = pg.PostgresJournal.from_env(env)
        key = pg.digest(['testnet', account, spec['event_id']])
        record = journal.load(key)
        if record['account'] != account:
            raise RuleError('CANCEL_RULE_SOURCE_MISMATCH')
        rule = rule_from_record(record, spec)
        threshold_seen = register_rule(journal, key, rule)
        from .pending_cancel_executor import Operations
        Operations(journal).initialize()
        http = sender.TestnetHTTP()  # Orders disabled; only /info is callable below.
        start = time.monotonic()
        order = http.info('orderStatus', user=account, oid=rule['entry_cloid'])
        state = http.info('clearinghouseState', user=account)
        position = text(sender._position(state, rule['symbol']))
        active = checks.InfoReader().read('activeAssetData', user=account, coin=rule['symbol'])
        if (not isinstance(active, dict) or active.get('coin') != rule['symbol']
                or checks.address(active.get('user')) != account):
            raise RuleError('CANCEL_MARK_IDENTITY_MISMATCH')
        result = evaluate(rule, order, position, active.get('markPx'),
                          sample_age_seconds=time.monotonic()-start, threshold_seen=threshold_seen)
        remember_crossing(journal, key, rule, result)
        report.update(result, status='RULE_REVIEW_COMPLETED', rule_id=rule['rule_id'],
                      symbol=rule['symbol'], threshold_pct=rule['threshold_pct'],
                      cancel_move_pct=rule['cancel_move_pct'], rule_saved_and_read_back=True,
                      rule_digest=digest(rule), measurement_reference='submitted_limit_price',
                      price_source='hyperliquid_testnet_mark', cancellation_sending_enabled=False,
                      observed_at_utc=datetime.now(timezone.utc).isoformat())
    except RuleError as exc:
        report['status'] = str(exc)
    except Exception:
        report['status'] = 'CANCEL_RULE_REVIEW_UNAVAILABLE'
    return report


def startup_review():
    print(json.dumps({'testnet_cancel_rule_review':review_configured(os.environ)}, sort_keys=True), flush=True)
