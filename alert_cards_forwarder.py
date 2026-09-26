"""One-way copies of delivered notifications to Testnet RECORDS, never orders.

No source writes, Telegram calls, trading keys, or changes to alert generation.
Retries are safe because the receiving store commits immutable unique cards.
The source retains manual receipts for about one hour. This is not a permanent
source archive; a long outage/restart requires a coverage review.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
import os
import time
from urllib.parse import urlsplit

import alert_cards_wire as wire

SERVICE = 'srv-d94ek17lk1mc73b4tb90'
MODE = 'record_only_v1'
_TASK = None
_SCOPES = set()
DELAY = 30
MAX_PER_PASS = 16
MAX_BUFFER = 8192


def config(env):
    if env.get('ALERT_CARDS_FORWARD_MODE') != MODE:
        return None
    key = env.get('ALERT_CARDS_FORWARD_SECRET', '')
    if env.get('RENDER_SERVICE_ID') != SERVICE or not wire.HEX.fullmatch(key):
        raise wire.WireError('FORWARD_CONFIGURATION_REQUIRED')
    fence = wire.moment(env.get('ALERT_CARDS_FORWARD_NOT_BEFORE', ''))
    return key, fence


def _source_dsn():
    dsn = os.environ.get('DATABASE_URL', '')
    u = urlsplit(dsn)
    if (u.scheme not in ('postgres', 'postgresql') or u.hostname not in
            ('dpg-d94d641kh4rs73evvih0-a', 'dpg-d94d641kh4rs73evvih0-a.oregon-postgres.render.com')
            or u.path != '/crypto_intelligence_db'):
        raise wire.WireError('EXPECTED_SOURCE_DATABASE_REQUIRED')
    return dsn


def read_delivered(scopes, fence):
    import psycopg
    from psycopg.rows import dict_row
    cutoff = max(fence, datetime.now(timezone.utc)-timedelta(hours=1))
    result = []
    with psycopg.connect(_source_dsn(), connect_timeout=3, row_factory=dict_row,
            options='-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=1000') as conn:
        for scope in sorted(scopes):
            row = conn.execute('SELECT value FROM bot_settings WHERE key=%s',
                               ('manual-four-formulas-outbox-v1:'+scope,)).fetchone()
            if row:
                raw = row['value']
                if not isinstance(raw, str) or len(raw.encode()) > 1024*1024:
                    raise wire.WireError('SOURCE_STATE_TOO_LARGE')
                state = json.loads(raw)
                if state.get('version') != 'manual-four-formulas-outbox-v1':
                    raise wire.WireError('UNSUPPORTED_SOURCE_VERSION')
                intents = state.get('intents')
                if not isinstance(intents, list):
                    raise wire.WireError('SOURCE_STATE_INVALID')
                for item in intents:
                    if item.get('status') == 'DELIVERED' and wire.moment(item['acknowledged_at']) >= cutoff:
                        result.append(wire.manual_delivery(item, scope))
            # U21 receives subscription_scope(chat_id) = general-watch:<hash>;
            # its key_for() hashes that whole scope a second time.
            u21_scope = hashlib.sha256(('general-watch:'+scope).encode()).hexdigest()
            row = conn.execute('SELECT value FROM bot_settings WHERE key=%s',
                               ('u21-xrp-experimental-cap1-v1:'+u21_scope,)).fetchone()
            if row:
                raw = row['value']
                if not isinstance(raw, str) or len(raw.encode()) > 1024*1024:
                    raise wire.WireError('SOURCE_STATE_TOO_LARGE')
                state = json.loads(raw)
                if state.get('version') != 'u21-xrp-experimental-cap1-v1':
                    raise wire.WireError('UNSUPPORTED_U21_SOURCE_VERSION')
                intents = state.get('intents')
                if not isinstance(intents, list) or len(intents)>128:
                    raise wire.WireError('SOURCE_STATE_INVALID')
                for item in intents:
                    if item.get('status') == 'DELIVERED' and wire.moment(item['acknowledged_at']) >= cutoff:
                        result.append(wire.u21_delivery(item, scope, state.get('config_version')))
            rows = conn.execute('''SELECT intent_id,rule_id,symbol,direction,source_at_utc,
                    expires_at,finished_at_utc,text,payload FROM dual_cvd65_intents
                WHERE subscription_scope=%s AND status='DELIVERED' AND finished_at_utc>=%s
                ORDER BY finished_at_utc,intent_id LIMIT 513''', ('general-watch:'+scope, cutoff)).fetchall()
            if len(rows) > 512:
                raise wire.WireError('SOURCE_PAGE_OVERFLOW_REQUIRES_REVIEW')
            result.extend(wire.dual_delivery(row, scope) for row in rows)
    return result


def post_record(value, key):
    if wire.HOST != 'hl-testnet-check-yoyo.onrender.com' or wire.PATH != '/internal/testnet-cards/v1':
        raise wire.WireError('FIXED_RECORD_RECEIVER_REQUIRED')
    raw = wire.encoded(value)
    stamp = str(int(time.time()))
    conn = http.client.HTTPSConnection(wire.HOST, timeout=4)
    try:
        conn.request('POST', wire.PATH, raw, {'Content-Type': 'application/json',
            'X-Card-Timestamp': stamp, 'X-Card-Signature': wire.signature(key, stamp, raw)})
        reply = conn.getresponse()
        payload = reply.read(2049)
        if reply.status != 200 or len(payload) > 2048:
            raise wire.WireError('CARD_RECEIVER_UNAVAILABLE')
        out = wire.decoded(payload)
        if (not isinstance(out, dict) or out.get('receipt_id') != hashlib.sha256(raw).hexdigest()
                or out.get('record_only') is not True
                or out.get('status') not in ('RECORDED', 'DUPLICATE', 'REJECTED')):
            raise wire.WireError('CARD_ACKNOWLEDGEMENT_NOT_VERIFIED')
        return out['status']
    finally:
        conn.close()


class Forwarder:
    def __init__(self):
        self.done = {}
        self.pending = {}
        self.retry_after = {}
        self.last_ok = None
        self.started = time.monotonic()
        self.coverage_gap = False
        self.cycles = 0

    def pass_once(self, scopes, key, fence, *, reader=read_delivered, post=post_record):
        started = time.monotonic()
        now = datetime.now(timezone.utc)
        report = dict(mode=MODE, status='FORWARD_PASS_COMPLETED', recorded=0, duplicates=0,
            rejected=0, deferred=0, attempted=0, source_records_changed=0,
            order_requests_sent=0, observed_at_utc=now.isoformat(), scopes=len(scopes),
            source_retention_seconds=3600, prior_process_coverage_verified=False)
        self.cycles += 1
        if started-(self.last_ok if self.last_ok is not None else self.started) >= 3600:
            self.coverage_gap = True
        source_failed = False
        try:
            values = reader(scopes, fence)
            if not isinstance(values, list) or len(values) > MAX_BUFFER:
                raise wire.WireError('SOURCE_PAGE_OVERFLOW_REQUIRES_REVIEW')
            for value in values:
                try:
                    raw = wire.encoded(value)
                except wire.WireError:
                    report['rejected'] += 1
                    continue
                identity = hashlib.sha256(raw).hexdigest()
                if identity in self.done or identity in self.pending:
                    continue
                if len(self.pending) >= MAX_BUFFER:
                    self.coverage_gap = True
                    report['deferred'] += 1
                    continue
                # Immutable serialized copy, retained even after source expiry.
                self.pending[identity] = (raw, now)
        except Exception:
            source_failed = True
        for identity, (raw, received) in list(self.pending.items()):
            if self.retry_after.get(identity, 0) > time.monotonic():
                report['deferred'] += 1
                continue
            if report['attempted'] >= MAX_PER_PASS or time.monotonic()-started > 12:
                report['deferred'] += 1
                continue
            report['attempted'] += 1
            try:
                status = post(wire.decoded(raw), key)
                if status not in ('RECORDED', 'DUPLICATE', 'REJECTED'):
                    raise wire.WireError('CARD_ACKNOWLEDGEMENT_NOT_VERIFIED')
                self.done[identity] = now
                report[{'RECORDED': 'recorded', 'DUPLICATE': 'duplicates', 'REJECTED': 'rejected'}[status]] += 1
                self.pending.pop(identity)
                self.retry_after.pop(identity, None)
            except Exception:
                self.retry_after[identity] = time.monotonic()+60
                report['deferred'] += 1
        self.done = {k: v for k, v in self.done.items() if (now-v).total_seconds() < 7200}
        # Removing an optimization cache can cause duplicate DATA requests only.
        if len(self.done) > MAX_BUFFER:
            self.done.clear()
        if source_failed:
            report['status'] = 'FORWARD_SOURCE_UNAVAILABLE'
        elif report['deferred']:
            report['status'] = 'FORWARD_PENDING_RETRY'
        elif report['rejected']:
            report['status'] = 'FORWARD_RECORD_REJECTED_REVIEW'
        else:
            self.last_ok = time.monotonic()
        report.update(coverage_gap_observed=self.coverage_gap, cycles=self.cycles,
                      pending_records=len(self.pending))
        return report


def audit_existing_delivery(scopes, key, identity, *, post=post_record):
    """Explicit one-record historical DATA replay, never a fresh trading signal.

    Reads exactly the chosen DELIVERED receipt in an already active subscription.
    Preserves its original identity, times and prices; sends it twice to prove
    receiver idempotency. No automatic selection or manufactured sample.
    """
    import psycopg
    from psycopg.rows import dict_row
    if not isinstance(identity, str) or not wire.HEX.fullmatch(identity) or not scopes:
        raise wire.WireError('EXPLICIT_ARCHIVED_RECEIPT_REQUIRED')
    with psycopg.connect(_source_dsn(), connect_timeout=3, row_factory=dict_row,
            options='-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=1000') as conn:
        rows = conn.execute('''SELECT intent_id,rule_id,symbol,direction,source_at_utc,
              expires_at,finished_at_utc,text,payload,subscription_scope
            FROM dual_cvd65_intents WHERE intent_id=%s AND status='DELIVERED'
              AND subscription_scope=ANY(%s)''',
            (identity, ['general-watch:'+s for s in scopes])).fetchall()
    if len(rows) != 1:
        raise wire.WireError('ARCHIVED_RECEIPT_NOT_FOUND')
    row = rows[0]
    value = wire.dual_delivery(row, row['subscription_scope'].removeprefix('general-watch:'))
    wire.normalize(value)
    first, second = post(value, key), post(value, key)
    return dict(status='HISTORICAL_RECORD_AND_REPLAY_VERIFIED' if first in ('RECORDED','DUPLICATE') and second == 'DUPLICATE' else 'HISTORICAL_RECORD_REQUIRES_REVIEW',
        first_ack=first, second_ack=second, receipt_id=hashlib.sha256(wire.encoded(value)).hexdigest(),
        symbol=value['symbol'], rule_id=value['rule_id'], source_at=value['source_at'],
        historical_validation_only=True, source_time_changed=False, order_requests_sent=0)


async def _loop(key, fence):
    worker = Forwarder()
    audit_id = os.environ.get('ALERT_CARDS_FORWARD_AUDIT_INTENT_ID', '')
    if audit_id:
        try:
            audit = await asyncio.to_thread(audit_existing_delivery, tuple(_SCOPES), key, audit_id)
        except Exception:
            audit = dict(status='HISTORICAL_RECORD_AUDIT_UNAVAILABLE', order_requests_sent=0)
        print(json.dumps({'alert_cards_forward_audit': audit}, sort_keys=True), flush=True)
    while True:
        report = await asyncio.to_thread(worker.pass_once, tuple(_SCOPES), key, fence)
        if worker.cycles % 10 == 1 or report['attempted'] or report['status'] != 'FORWARD_PASS_COMPLETED':
            print(json.dumps({'alert_cards_forwarder': report}, sort_keys=True), flush=True)
        await asyncio.sleep(DELAY)


def maybe_start(chat_id):
    """Nonblocking side task; never makes Telegram wait for a card acknowledgement."""
    global _TASK
    try:
        cfg = config(os.environ)
        if cfg is None or chat_id is None:
            return
        scope = hashlib.sha256(str(int(chat_id)).encode()).hexdigest()
        if len(_SCOPES) >= 4 and scope not in _SCOPES:
            print('[alert-cards] SOURCE_SCOPE_LIMIT_REQUIRES_REVIEW', flush=True)
            return
        _SCOPES.add(scope)
        if _TASK is None or _TASK.done():
            _TASK = asyncio.get_running_loop().create_task(_loop(*cfg), name='delivered-alert-cards')
    except Exception:
        print('[alert-cards] FORWARD_INITIALIZATION_UNAVAILABLE', flush=True)
