"""Explicit operator-authorized ONE Testnet attempt, not a trading daemon.

Only a short-lived manifest on the dedicated Render test service can activate
this task. Read-only mode, HTTP requests and an ordinary deployment cannot.
Persistent account reservation prevents multiple sends after restart/races.
No production DB, keys returned to the operator, poller, automatic retry or
arbitrary commands/URLs. A separate inspect mode can only read receipts.
"""
from datetime import datetime, timezone
import json
import re
import os
from .source_window import timestamp, source_fresh
from .postgres_journal import PostgresJournal, JournalError, digest, account_address

SERVICE = 'srv-dakptbh594qs7395460g'
MODE = 'single_testnet_attempt_v1'
INSPECT = 'inspect_testnet_attempt_v1'
FIELDS = {'version','run_id','signal','source_expires_at','issued_at','expires_at',
          'message_id','rule_id','signal_sha256','delivery_verified'}


def validate_ticket(raw, env, *, now=None, inspection=False):
    from .checks import decode
    from .price_precision import POLICY
    from .guarded_execution import _frozen
    if (env.get('RENDER_SERVICE_ID') != SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') != (INSPECT if inspection else MODE)
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_PRICE_ROUNDING') != POLICY
            or env.get('HL_TESTNET_EXIT_TYPE') != 'tp_limit_sl_market'):
        raise JournalError('CONTROLLED_MODE_NOT_AUTHORIZED')
    if not isinstance(raw,str) or not 1 <= len(raw) <= 4096:
        raise JournalError('EXACT_SINGLE_TICKET_REQUIRED')
    ticket = decode(raw)
    if (not isinstance(ticket,dict) or set(ticket) != FIELDS
            or ticket['version'] != MODE or ticket['delivery_verified'] is not True
            or not isinstance(ticket['run_id'],str)
            or not re.fullmatch(r'[a-z0-9-]{8,80}',ticket['run_id'])
            or not isinstance(ticket['message_id'],str) or not ticket['message_id'].isdigit()
            or not isinstance(ticket['rule_id'],str) or not re.fullmatch(r'[A-Z0-9_]{1,40}',ticket['rule_id'])):
        raise JournalError('EXACT_SINGLE_TICKET_REQUIRED')
    signal, at = _frozen(ticket['signal'])
    if digest(signal) != ticket['signal_sha256']:
        raise JournalError('TICKET_SOURCE_DIGEST_MISMATCH')
    issued, end = timestamp(ticket['issued_at']), timestamp(ticket['expires_at'])
    instant = datetime.now(timezone.utc) if now is None else now
    if not 0 < (end-issued).total_seconds() <= 180 or issued < at:
        raise JournalError('INVALID_AUTHORIZATION_WINDOW')
    native_end = timestamp(ticket['source_expires_at'])
    if not 0 < (native_end-at).total_seconds() <= 600:
        raise JournalError('INVALID_ORIGINAL_SOURCE_EXPIRY')
    if not inspection and (not issued <= instant < end or not source_fresh(at, ticket['source_expires_at'],ticket['expires_at'],now=instant)):
        raise JournalError('AUTHORIZATION_OR_SOURCE_EXPIRED_NO_SEND')
    return ticket


def run_configured(env, *, now=None):
    """No network or secret access until exact mode and manifest validation."""
    result = {'mode':'testnet_single_attempt','status':'DISABLED','order_requests_sent':0,
              'signing_tested':False,'verified':False,'continuous_trading':False}
    if env.get('HL_TESTNET_RUNTIME_MODE') not in (MODE,INSPECT):
        return result
    try:
        inspection = env['HL_TESTNET_RUNTIME_MODE'] == INSPECT
        ticket = validate_ticket(env.get('HL_TESTNET_ATTEMPT_TICKET',''),env,now=now,inspection=inspection)
        account = account_address(env.get('HL_TESTNET_ACCOUNT_ADDRESS'))
        agent = account_address(env.get('HL_TESTNET_AGENT_ADDRESS'))
        if account == agent:
            raise JournalError('USE_DEDICATED_AGENT_NOT_MASTER_KEY')
        result.update(run_id=ticket['run_id'],message_id=ticket['message_id'],
                      rule_id=ticket['rule_id'],symbol=ticket['signal']['symbol'])
        journal = PostgresJournal.from_env(env)
        # No bootstrap/repair here. Existing verified storage is mandatory.
        from .persistent_execution import submit_persisted, inspect_persisted
        key = digest(['testnet',account,ticket['signal']['event_id']])
        if inspection:
            result.update(inspect_persisted(key,journal=journal))
        else:
            result.update(submit_persisted(ticket['signal'],account=account,agent=agent,
                exit_type='tp_limit_sl_market',enable_testnet=True,journal=journal,
                source_expires_at=ticket['source_expires_at'],approval_expires_at=ticket['expires_at']))
        result['source_time_changed'] = False
        result['take_profit_type'] = 'limit'
        result['stop_loss_type'] = 'market'
        # A replay has no new signature; inspect only, never resubmit.
        if result.get('replayed'):
            result['receipt_inspection'] = inspect_persisted(key,journal=journal)
    except JournalError as exc:
        result.update(status=str(exc),verified=False)
    except Exception:
        # Never echo exception text, connection URL or environment values.
        result.update(status='CONTROLLED_ATTEMPT_REQUIRES_REVIEW',verified=False)
    return result


def startup_single_attempt():
    report = run_configured(os.environ)
    print(json.dumps({'testnet_controlled_attempt':report},sort_keys=True),flush=True)
