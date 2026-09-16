"""Explicit Testnet-only cancellation, separate from the read-only rule review.

No endpoint, startup hook or polling loop calls this dispatcher. The regular
one-shot entry sender is not changed. Cancellation requires an immutable saved
rule, a still-unfilled entry, a committed operation reservation and fresh reads.
A fill racing with cancellation is inspected; if its original children were
canceled, recreate only those approved reduce-only exits for the observed size.
Network failure can still leave uncertain protection: never claim a guarantee.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import http.client
import json
import time

from . import half_threshold_cancel as rulemod

OPS = 'hl_testnet_execution_v1.limit_cancel_operations'
HOST = 'api.hyperliquid-testnet.xyz'
TERMINAL_CANCEL = {'canceled', 'siblingFilledCanceled', 'reduceOnlyCanceled',
                   'scheduledCancel', 'marginCanceled', 'selfTradeCanceled'}


class CancelError(ValueError):
    """Local codes only; never include exchange error text."""


def cancel_action(record):
    entry = record['action']['orders'][0]
    return {'type':'cancelByCloid', 'cancels':[{'asset':entry['a'], 'cloid':entry['c']}]}


def repair_action(record, position):
    """Original approved exit prices/types only, no new entry or increased size."""
    position = rulemod.decimal(position, positive=False)
    original = rulemod.decimal(record['action']['orders'][0]['s'])
    execution = record['prepared']['execution']
    if not 0 < abs(position) <= original or (position > 0) != (execution['side'] == 'LONG'):
        raise CancelError('REPAIR_POSITION_NOT_ATTRIBUTABLE')
    orders = deepcopy(record['action']['orders'][1:])
    if len(orders) != 2 or any(o.get('r') is not True for o in orders):
        raise CancelError('ORIGINAL_PROTECTION_NOT_VERIFIED')
    if (orders[0]['t']['trigger'].get('isMarket') is not False
            or orders[1]['t']['trigger'].get('isMarket') is not True
            or orders[0]['t']['trigger'].get('tpsl') != 'tp'
            or orders[1]['t']['trigger'].get('tpsl') != 'sl'):
        raise CancelError('REPAIR_PROFILE_NOT_APPROVED')
    for role, order in zip(('tp', 'sl'), orders):
        order['s'] = rulemod.text(abs(position))
        order['c'] = '0x' + rulemod.digest(['cancel-repair-v1', record['account'], execution['event_id'], role])[:32]
    return {'type':'order', 'grouping':'positionTpsl', 'orders':orders}


class Operations:
    """Own durable rows. Never release, erase or retry a reserved operation."""
    def __init__(self, journal):
        self.journal = journal

    def initialize(self):
        # Explicit setup only, before monitoring/dispatch. No schema DDL in tick().
        from .postgres_journal import LOCK, JournalError
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {OPS} (
                plan_key text NOT NULL REFERENCES hl_testnet_execution_v1.prepared(plan_key),
                phase text NOT NULL CHECK(phase IN ('cancel','repair')), action jsonb NOT NULL,
                nonce bigint NOT NULL, outcome text NOT NULL DEFAULT 'RESERVED_OR_UNCERTAIN',
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY(plan_key,phase))''')
            conn.execute(f'REVOKE ALL ON {OPS} FROM PUBLIC')

    def get(self, key, phase):
        with self.journal._transaction() as conn:
            row = conn.execute(f'SELECT action,nonce,outcome FROM {OPS} WHERE plan_key=%s AND phase=%s',
                               (key,phase)).fetchone()
        return None if row is None else dict(action=row[0], nonce=row[1], outcome=row[2])

    def reserve(self, key, phase, action):
        from .postgres_journal import LOCK, JournalError
        if phase not in ('cancel','repair'):
            raise CancelError('INVALID_CANCEL_PHASE')
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            row = conn.execute(f'SELECT action,nonce FROM {OPS} WHERE plan_key=%s AND phase=%s',
                               (key,phase)).fetchone()
            if row:
                if rulemod.canonical(row[0]) != rulemod.canonical(action):
                    raise JournalError('RESERVED_OPERATION_CHANGED_NO_RESEND')
                return False, row[1]
            # Ensure unique monotone nonce even across cancel and repair in one ms.
            nonce = conn.execute(f'''SELECT greatest(
                floor(extract(epoch FROM clock_timestamp())*1000)::bigint,
                coalesce((SELECT max(nonce)+1 FROM {OPS}),0))''').fetchone()[0]
            conn.execute(f'INSERT INTO {OPS}(plan_key,phase,action,nonce) VALUES(%s,%s,%s::jsonb,%s)',
                         (key,phase,rulemod.canonical(action),nonce))
        return True, nonce

    def save(self, key, phase, status):
        import re
        if not isinstance(status,str) or not re.fullmatch(r'[A-Z_]{1,80}',status):
            raise CancelError('INVALID_CANCEL_OUTCOME')
        with self.journal._transaction() as conn:
            if conn.execute(f'UPDATE {OPS} SET outcome=%s WHERE plan_key=%s AND phase=%s',
                            (status,key,phase)).rowcount != 1:
                raise CancelError('MISSING_CANCEL_RESERVATION')


class Exchange:
    def __init__(self, record, agent):
        import hyperliquid_testnet_executor as sender
        self.sender, self.record, self.agent = sender, record, agent
        self.reader = sender.TestnetHTTP()  # Original order transport stays disabled.
        self.requests_attempted = 0
        self.cancel_requests_attempted = 0

    def observe(self):
        from .checks import InfoReader, address
        start = time.monotonic()
        record, account = self.record, self.record['account']
        order = self.reader.info('orderStatus', user=account, oid=record['action']['orders'][0]['c'])
        state = self.reader.info('clearinghouseState', user=account)
        symbol = record['prepared']['execution']['symbol']
        position = rulemod.text(self.sender._position(state,symbol))
        active = InfoReader().read('activeAssetData',user=account,coin=symbol)
        if active.get('coin') != symbol or address(active.get('user')) != account:
            raise CancelError('CANCEL_OBSERVATION_IDENTITY_MISMATCH')
        return dict(order=order, position_size=position, mark=active.get('markPx'),
                    sample_age_seconds=time.monotonic()-start)

    def child_states(self):
        replies = [self.reader.info('orderStatus',user=self.record['account'],oid=o['c'])
                   for o in self.record['action']['orders'][1:]]
        states = []
        for expected, reply in zip(self.record['action']['orders'][1:],replies):
            try:
                actual = reply['order']['order']
                if (reply['status'] != 'order' or actual['cloid'] != expected['c']
                        or actual['coin'] != self.record['prepared']['execution']['symbol']):
                    raise ValueError()
                states.append(reply['order']['status'])
            except (KeyError,TypeError,ValueError):
                raise CancelError('CHILD_STATE_NOT_VERIFIED') from None
        return states

    def write(self, action, nonce, *, phase, position=None):
        expected = cancel_action(self.record) if phase == 'cancel' else repair_action(self.record, position)
        if phase not in ('cancel','repair') or rulemod.canonical(action) != rulemod.canonical(expected):
            raise CancelError('CANCEL_ACTION_NOT_AUTHORIZED')
        sender = self.sender
        if HOST != 'api.hyperliquid-testnet.xyz' or sender.TESTNET_HOST != HOST:
            raise CancelError('TESTNET_ONLY')
        if not -1 <= sender.now_ms()-nonce <= 5000:
            raise CancelError('CANCEL_NONCE_EXPIRED_NO_SEND')
        wallet = sender._wallet()
        if sender._account(wallet.address) != self.agent or self.agent == self.record['account']:
            raise CancelError('CANCEL_AGENT_MISMATCH')
        role = self.reader.info('userRole',user=self.agent)
        if (not isinstance(role,dict) or role.get('role') != 'agent'
                or not isinstance(role.get('data'),dict)
                or sender._account(role['data'].get('user')) != self.record['account']):
            raise CancelError('CANCEL_AGENT_NOT_AUTHORIZED')
        if sender.now_ms()-nonce > 5000:
            raise CancelError('CANCEL_NONCE_EXPIRED_NO_SEND')
        from hyperliquid.utils.signing import sign_l1_action
        expiry = nonce + 5000
        body = dict(action=action, nonce=nonce, expiresAfter=expiry,
                    signature=sign_l1_action(wallet,action,None,nonce,expiry,False))
        connection = http.client.HTTPSConnection(HOST, timeout=4)
        try:
            self.requests_attempted += 1
            if phase == 'cancel':
                self.cancel_requests_attempted += 1
            connection.request('POST','/exchange',sender._json(body).encode(),{'Content-Type':'application/json'})
            response = connection.getresponse()
            raw = response.read(sender.MAX_BYTES+1)
            if response.status != 200 or len(raw)>sender.MAX_BYTES:
                raise CancelError('CANCEL_TRANSPORT_UNCERTAIN')
            # Never use an acknowledgement as proof of cancellation/protection.
            sender._decode(raw)
        except Exception:
            raise CancelError('CANCEL_TRANSPORT_UNCERTAIN') from None
        finally:
            connection.close()

    def verify_original(self):
        return self.sender.verify_orders(self.reader, {
            'signal':self.record['prepared']['execution'], 'account':self.record['account'],
            'action':self.record['action']})

    def verify_repair(self, action, position):
        start = time.monotonic()
        for expected, label in zip(action['orders'],('Take Profit Limit','Stop Market')):
            reply = self.reader.info('orderStatus',user=self.record['account'],oid=expected['c'])
            try:
                actual = reply['order']['order']
                if (reply['status'] != 'order' or reply['order']['status'] != 'open'
                        or actual['cloid'] != expected['c'] or actual['reduceOnly'] is not True
                        or actual['isTrigger'] is not True or actual['orderType'] != label
                        or actual['coin'] != self.record['prepared']['execution']['symbol']
                        or actual['side'] != ('B' if expected['b'] else 'A')
                        or rulemod.decimal(actual['origSz']) != abs(rulemod.decimal(position,positive=False))
                        or rulemod.decimal(actual['sz']) != rulemod.decimal(expected['s'])
                        or rulemod.decimal(actual['limitPx']) != rulemod.decimal(expected['p'])
                        or rulemod.decimal(actual['triggerPx']) != rulemod.decimal(expected['t']['trigger']['triggerPx'])):
                    return False
            except (KeyError,TypeError,rulemod.RuleError):
                return False
        state = self.reader.info('clearinghouseState',user=self.record['account'])
        size = self.sender._position(state,self.record['prepared']['execution']['symbol'])
        return time.monotonic()-start <= 10 and size == rulemod.decimal(position,positive=False)


def reconcile(key, record, rule, ops, exchange):
    """Read back, then protect only a demonstrable partial-fill cancellation race.

    No normal cancellation is allowed for a pre-existing partial fill. Repair is
    limited to the already approved TP Limit / SL Market combination. A timeout
    is NOT retried, whether for the cancel or the repair operation.
    """
    sample = exchange.observe()
    parent = sample['order']['order']
    actual = parent['order']
    # Reuse identity/size checks. Canceled and filled states are deliberately safe
    # exclusions for another cancellation, but are inspected further below.
    decision = rulemod.evaluate(rule,**sample)
    if decision['decision'] in ('ORDER_OR_MARK_REQUIRES_REVIEW','STALE_OR_INVALID_OBSERVATION'):
        return 'CANCEL_RECONCILIATION_REQUIRES_REVIEW'
    position = rulemod.decimal(sample['position_size'],positive=False)
    if parent['status'] == 'filled':
        checked = exchange.verify_original()
        return ('FILLED_BEFORE_CANCEL_PROTECTION_VERIFIED' if checked.get('verified') is True
                else 'FILLED_BEFORE_CANCEL_PROTECTION_REQUIRES_REVIEW')
    if parent['status'] not in TERMINAL_CANCEL:
        return 'CANCELLATION_NOT_VERIFIED_NO_RESEND'
    children = exchange.child_states()
    if position == 0:
        if all(s in TERMINAL_CANCEL for s in children):
            return 'CANCELLATION_VERIFIED_FLAT'
        return 'FLAT_BUT_CHILD_STATUS_REQUIRES_REVIEW'
    if (parent['status'] != 'canceled' or not all(s in TERMINAL_CANCEL for s in children)
            or rulemod.decimal(actual['origSz'])-rulemod.decimal(actual['sz'],positive=False) != abs(position)):
        return 'POSITION_OR_PROTECTION_REQUIRES_REVIEW'
    # Second sample prevents using an already changed position for repair.
    again = exchange.observe()
    second = rulemod.evaluate(rule,**again)
    if (second['decision'] in ('ORDER_OR_MARK_REQUIRES_REVIEW','STALE_OR_INVALID_OBSERVATION')
            or again['position_size'] != sample['position_size']
            or again['order']['order']['status'] not in TERMINAL_CANCEL):
        return 'POSITION_OR_PROTECTION_REQUIRES_REVIEW'
    action = repair_action(record,sample['position_size'])
    if ops.get(key,'repair') is None:
        fresh, nonce = ops.reserve(key,'repair',action)
        if fresh:
            try:
                exchange.write(action,nonce,phase='repair',position=sample['position_size'])
            except CancelError:
                pass  # Receipt inspection only. Never retry an uncertain write.
    prior = ops.get(key,'repair')
    if prior is None or rulemod.canonical(prior['action']) != rulemod.canonical(action):
        return 'PROTECTION_REPAIR_REQUIRES_REVIEW'
    status = ('RACED_FILL_PROTECTION_RESTORED' if exchange.verify_repair(action,sample['position_size'])
              else 'PROTECTION_REPAIR_UNVERIFIED_DO_NOT_RESEND')
    ops.save(key,'repair',status)
    return status


def cancel_registered_once(key, *, journal=None, agent=None, enable_testnet=False):
    """Explicit dispatcher; NEVER invoked by current web startup or HTTP input."""
    report = {'mode':'testnet_cancel_once','status':'DISABLED','cancel_requests_sent':0,
              'exchange_write_attempts':0,'continuous_monitoring':False}
    if enable_testnet is not True:
        return report
    exchange = None
    try:
        import os
        from .postgres_journal import PostgresJournal, account_address
        journal = PostgresJournal.from_env(os.environ) if journal is None else journal
        record = journal.load(key)
        with journal._transaction() as conn:
            row = conn.execute(f'SELECT policy,threshold_seen FROM {rulemod.TABLE} WHERE plan_key=%s',(key,)).fetchone()
        if row is None:
            raise CancelError('CANCEL_RULE_NOT_REGISTERED')
        rule = rulemod.validate_rule(row[0])
        # Revalidate saved source and original action against the frozen rule.
        spec = {k:rule[k] for k in ('event_id','rule_id','threshold_pct','source_digest')}
        if rulemod.rule_from_record(record,spec) != rule:
            raise CancelError('CANCEL_RULE_SOURCE_MISMATCH')
        agent = account_address(agent)
        if agent == record['account']:
            raise CancelError('USE_DEDICATED_AGENT_NOT_MASTER_KEY')
        ops = Operations(journal)
        exchange = Exchange(record,agent)
        report.update(run_once(key,record,rule,ops,exchange,threshold_seen=bool(row[1]),journal=journal))
    except Exception:
        # A reserved request may already have reached the exchange. No replay.
        report['status'] = 'CANCEL_OR_PROTECTION_REQUIRES_REVIEW'
    report['exchange_write_attempts'] = getattr(exchange,'requests_attempted',0)
    report['cancel_requests_sent'] = getattr(exchange,'cancel_requests_attempted',0)
    return report


def run_once(key, record, rule, ops, exchange, *, threshold_seen=False, journal=None):
    """Core state machine; dependencies permit offline and real-PG testing."""
    previous = ops.get(key,'cancel')
    if previous is not None:
        return {'status':settle(key,record,rule,ops,exchange,previous['nonce']), 'cancel_requests_sent':0, 'replayed':True}
    first = rulemod.evaluate(rule,**exchange.observe(),threshold_seen=threshold_seen)
    if journal is not None:
        rulemod.remember_crossing(journal,key,rule,first)
    if not first['cancel_candidate']:
        return {'status':first['decision'], 'cancel_requests_sent':0}
    second = rulemod.evaluate(rule,**exchange.observe(),threshold_seen=True)
    if not second['cancel_candidate']:
        return {'status':second['decision'], 'cancel_requests_sent':0}
    action = cancel_action(record)
    fresh, nonce = ops.reserve(key,'cancel',action)  # Must COMMIT before any signature.
    before = exchange.cancel_requests_attempted
    if fresh:
        try:
            exchange.write(action,nonce,phase='cancel')
        except CancelError:
            pass
    status = settle(key,record,rule,ops,exchange,nonce)
    ops.save(key,'cancel',status)
    return {'status':status, 'cancel_requests_sent':exchange.cancel_requests_attempted-before, 'replayed':not fresh}


def settle(key, record, rule, ops, exchange, nonce):
    """Wait out a possibly in-flight cancellation before the final read.

    At most three readbacks; no cancel replay. The signature expires 5 seconds
    after the committed nonce. Remaining uncertainty is explicit, not a claim
    that the parent is canceled or the position protected.
    """
    for attempt in range(3):
        try:
            result = reconcile(key,record,rule,ops,exchange)
        except Exception:
            result = 'CANCEL_OR_PROTECTION_REQUIRES_REVIEW'
        if result in ('CANCELLATION_VERIFIED_FLAT','RACED_FILL_PROTECTION_RESTORED',
                       'FILLED_BEFORE_CANCEL_PROTECTION_VERIFIED'):
            return result
        if attempt < 2:
            delay = min(5.5,max(0.5,(nonce+5500-time.time_ns()//1_000_000)/1000))
            time.sleep(delay)
    return result
