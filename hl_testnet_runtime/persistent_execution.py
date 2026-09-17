"""Durable rounded Testnet execution. Submission requires an explicit caller.

Storage startup never submits. Controlled attempts retain source timestamps,
use the original bounded outbox expiry and an additional operator deadline.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import time

from .postgres_journal import PostgresJournal, JournalError, digest, account_address
from .source_window import source_fresh


def prepare_and_store(message, *, account, journal, client=None):
    from . import guarded_execution as guard, checks, price_precision as precision
    source, _ = guard._frozen(message)
    account = account_address(account)
    reader = precision.MetadataReader(checks.InfoReader() if client is None else client)
    prepared = precision.prepare_signal(source, reader.read('meta'))
    key, created = journal.save_prepared(account, prepared)
    stored = journal.load(key)
    if digest(stored['prepared']) != digest(prepared) or stored['account'] != account:
        raise JournalError('PREPARED_RECORD_READBACK_MISMATCH')
    return key, prepared, reader, created


def startup_storage_check():
    report = {'mode': 'testnet_storage_only', 'status': 'WAITING_FOR_DATABASE_CONNECTION',
              'persistent_prepared_record': False, 'order_requests_sent': 0, 'signing_tested': False}
    try:
        if os.environ.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1':
            return
        if not os.environ.get('HL_TESTNET_DATABASE_URL'):
            print(json.dumps({'testnet_journal': report}), flush=True)
            return
        journal = PostgresJournal.from_env(os.environ)
        report['schema_created'] = journal.bootstrap()
        report.update(journal.readiness_probe())
        raw = os.environ.get('HL_TESTNET_REVIEW_SIGNAL', '')
        if raw:
            from . import checks
            if len(raw) > 2048:
                raise JournalError('INVALID_JOURNAL_RECORD')
            key, prepared, _, created = prepare_and_store(checks.decode(raw),
                account=os.environ.get('HL_TESTNET_ACCOUNT_ADDRESS', ''), journal=journal)
            report.update(persistent_prepared_record=True, new_prepared_record=created,
                          original_time_preserved=True, rounding_audit_preserved=True)
        report['storage_expiry_utc'] = '2026-10-01T07:24:32Z'
    except JournalError as exc:
        report['status'] = str(exc)
    except Exception:
        report['status'] = 'STORAGE_CHECK_UNAVAILABLE'
    print(json.dumps({'testnet_journal': report}, sort_keys=True), flush=True)


def submit_persisted(message, *, account, agent, exit_type=None, enable_testnet=False,
                     journal=None, source_expires_at=None, approval_expires_at=None,
                     account_role=None):
    """One explicit call, commit-before-signing; one reservation per test account.

    Role-bound calls additionally require the new default-locked role switch.
    Startup review NEVER calls this. Legacy callers keep their existing path.
    Defaults retain the legacy minute window. Only a supervised caller supplies
    the ORIGINAL source expiry (<=10 minutes) and a shorter approval deadline.
    An expired or uncertain attempt is never reset or blindly submitted again.
    """
    result = {'mode': 'testnet', 'status': 'DISABLED', 'order_requests_sent': 0,
              'signing_tested': False, 'verified': False}
    if enable_testnet is not True:
        return result
    from . import checks, guarded_execution as guard, price_precision as precision
    import hyperliquid_testnet_executor as sender
    if account_role is not None:
        from . import two_account_execution as roles
        if not roles.execution_unlocked(os.environ):
            return {**result, 'status': 'ROLE_EXECUTION_LOCKED'}
    http, reserved, key = None, False, None
    started = time.monotonic()
    try:
        source, at = guard._frozen(message)
        account, agent = account_address(account), account_address(agent)
        if account == agent:
            raise JournalError('USE_DEDICATED_AGENT_NOT_MASTER_KEY')
        if account_role is not None:
            roles.route_for(os.environ, account_role, account, agent, source['side'])
        journal = PostgresJournal.from_env(os.environ) if journal is None else journal
        key, prepared, reader, _ = prepare_and_store(source, account=account, journal=journal)
        previous = journal.load(key)
        if previous['result'] is not None:
            return {**result, **previous['result'], 'replayed': True, 'order_requests_sent': 0}
        if not source_fresh(at, source_expires_at, approval_expires_at):
            raise JournalError('SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST')
        if exit_type not in ('market', 'limit', 'tp_limit_sl_market'):
            raise JournalError('EXPLICIT_EXIT_TYPE_REQUIRED')
        action = sender.build_action(prepared['execution'], reader.read('meta'), account, exit_type=exit_type)
        plan = {k: prepared['execution'][k] for k in guard.PLAN_FIELDS}
        if account_role is not None:
            report = roles.budget_for_role(os.environ, account_role, account, agent, plan, reader)
        else:
            report = checks.run_check({'HL_TESTNET_ACCOUNT_ADDRESS': account,
                'HL_TESTNET_AGENT_ADDRESS': agent, 'HL_TESTNET_RUNTIME_MODE': 'read_only',
                'HL_TESTNET_CHECK_SYMBOL': plan['symbol'], 'HL_TESTNET_CHECK_PLAN': json.dumps(plan)}, client=reader)
        diagnostics = report.get('budget_diagnostics') or {}
        if (report.get('status') != 'PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION'
                or report.get('test_plan_checked') is not True
                or diagnostics.get('plan_sha256') != precision.digest(plan)
                or diagnostics.get('current_settings_passed') is not True):
            raise JournalError('BUDGET_NOT_PASSED_FOR_EXACT_ROUNDED_PLAN')
        if diagnostics.get('mark_within_supplied_exit_range') is not True:
            raise JournalError('TESTNET_PRICE_OUTSIDE_SUPPLIED_EXIT_RANGE')
        active = reader.read('activeAssetData', user=account, coin=plan['symbol'])
        mark = checks.number(active.get('markPx'))
        if not min(Decimal(plan['stop']), Decimal(plan['take_profit'])) < mark < max(Decimal(plan['stop']), Decimal(plan['take_profit'])):
            raise JournalError('TESTNET_PRICE_OUTSIDE_SUPPLIED_EXIT_RANGE')
        wallet = (roles.wallet_for_role(os.environ, account_role, account, agent)
                  if account_role is not None else sender._wallet())
        if account_address(wallet.address) != agent:
            raise JournalError('KEY_DOES_NOT_MATCH_AGENT')
        http = sender.TestnetHTTP(allow_orders=True)
        role = http.info('userRole', user=agent)
        if (not isinstance(role, dict) or role.get('role') != 'agent'
                or not isinstance(role.get('data'), dict)
                or account_address(role['data'].get('user')) != account):
            raise JournalError('AGENT_NOT_AUTHORIZED_FOR_TEST_ACCOUNT')
        if http.info('userRole', user=account) != {'role': 'user'}:
            raise JournalError('INDEPENDENT_TEST_ACCOUNT_REQUIRED')
        open_orders = http.info('frontendOpenOrders', user=account)
        if not isinstance(open_orders, list) or open_orders:
            raise JournalError('EMPTY_DEDICATED_TEST_ACCOUNT_REQUIRED')
        state = http.info('clearinghouseState', user=account)
        sender._position(state, plan['symbol'])
        if any(sender._num(p['position'].get('szi'), zero=True, signed=True) != 0 for p in state['assetPositions']):
            raise JournalError('EMPTY_DEDICATED_TEST_ACCOUNT_REQUIRED')
        if time.monotonic() - started > 15:
            raise JournalError('BUDGET_SAMPLE_EXPIRED_BEFORE_DISPATCH')
        fresh, nonce, previous = journal.reserve(key, action)
        if not fresh:
            return {**result, **previous, 'replayed': True, 'order_requests_sent': 0}
        reserved = True
        if (time.monotonic() - started > 15
                or not source_fresh(at, source_expires_at, approval_expires_at)
                or not 0 <= sender.now_ms() - nonce <= 5000):
            raise JournalError('SOURCE_OR_RESERVATION_EXPIRED_NO_SEND')
        if account_role is not None and not roles.execution_unlocked(os.environ):
            raise JournalError('ROLE_EXECUTION_LOCKED')
        body = sender._signed_body(wallet, action, nonce)
        result['signing_tested'] = True
        response = http._post('/exchange', body)
        ack = sender.acknowledgement(response)
        result.update(status=ack, order_requests_sent=http.order_attempts)
        if ack == 'ACKNOWLEDGED_NOT_VERIFIED':
            result.update(sender.verify_orders(http, {'signal': prepared['execution'],
                                                      'account': account, 'action': action}))
        journal.save_result(key, result)
        return result
    except JournalError as exc:
        result.update(status=str(exc), verified=False, protection_active=False)
    except Exception:
        result.update(status='UNCERTAIN_REQUIRES_REVIEW' if reserved else 'PRECHECK_UNAVAILABLE_NO_SEND',
                      verified=False, protection_active=False)
    result['order_requests_sent'] = getattr(http, 'order_attempts', 0)
    if reserved:
        try:
            journal.save_result(key, result)
        except Exception:
            result['status'] = 'PERSISTED_RESERVATION_REQUIRES_RECONCILIATION'
    return result


def inspect_persisted(key, *, journal=None):
    """Stored-action reconciliation, public reads only; never re-submits."""
    import hyperliquid_testnet_executor as sender
    journal = PostgresJournal.from_env(os.environ) if journal is None else journal
    record = journal.load(key)
    if record['action'] is None:
        return {'status': 'PREPARED_NOT_SUBMITTED', 'verified': False, 'order_requests_sent': 0}
    report = sender.verify_orders(sender.TestnetHTTP(), {'signal': record['prepared']['execution'],
        'account': record['account'], 'action': record['action']})
    journal.save_result(key, report)
    return {**report, 'mode': 'testnet', 'order_requests_sent': 0, 'signing_tested': False}
