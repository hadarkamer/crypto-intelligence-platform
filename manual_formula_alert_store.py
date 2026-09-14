"""Isolated, bounded user-authorized outbox in the existing settings table.

No DDL, research qualification mutations, price reads, or Telegram calls.
One locked versioned settings row per destination commits detection, receipts,
and frozen messages together. Source IDs are receipts, never a high-water mark:
late commits remain discoverable throughout the fresh timestamp window.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from uuid import uuid4

import manual_formula_alert as rules
import manual_formula_alert_source as source
from watch_transition_store import _connect

STORE_VERSION = 'manual-four-formulas-outbox-v1'
MAX_STATE_BYTES = 1024 * 1024
MAX_RECEIPTS = 4096
TTL = timedelta(minutes=10)
RECEIPT_TTL = timedelta(minutes=20)
TERMINAL_TTL = timedelta(hours=1)
ORPHAN_TTL = timedelta(minutes=2)


def utc(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Timezone-aware timestamp required')
    return value.astimezone(timezone.utc)


def iso(value):
    return utc(value).isoformat()


def key_for(chat_id):
    return STORE_VERSION + ':' + hashlib.sha256(str(int(chat_id)).encode()).hexdigest()


def _initial(now):
    return {'version': STORE_VERSION, 'rule_version': rules.VERSION, 'ruleset_sha256': rules.RULESET_SHA256,
            'activated_at': iso(now), 'receipts': {}, 'dedup': {}, 'retry': {},
            'intents': [], 'counts': {'checked': 0, 'created': 0}}


def _encode(state):
    value = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if len(value.encode()) > MAX_STATE_BYTES or len(state['receipts']) > MAX_RECEIPTS:
        raise ValueError('Manual formula state capacity exceeded; no partial commit')
    return value


def _locked(conn, key, now):
    conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING',
                 (key, _encode(_initial(now))))
    state = json.loads(conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()['value'])
    if (state.get('version') != STORE_VERSION or state.get('rule_version') != rules.VERSION
            or state.get('ruleset_sha256') != rules.RULESET_SHA256):
        raise ValueError('Frozen manual formula version mismatch')
    return state


def _save(conn, key, state):
    conn.execute('UPDATE bot_settings SET value=%s WHERE key=%s', (_encode(state), key))


def _count(state, name):
    state['counts'][name] = state['counts'].get(name, 0) + 1


def prune(state, now):
    """Expired evidence cannot re-enter the ten-minute source window."""
    now = utc(now)
    for field in ('receipts', 'dedup'):
        state[field] = {k: v for k, v in state[field].items() if utc(v) >= now - RECEIPT_TTL}
    for identity, retry in list(state.setdefault('retry', {}).items()):
        if utc(retry['event_time']) <= now - TTL:
            del state['retry'][identity]
            _count(state, 'sequence_retry_expired')
    keep = []
    for item in state['intents']:
        if item['status'] == 'IN_FLIGHT' and utc(item['attempted_at']) <= now - ORPHAN_TTL:
            item.update(status='UNKNOWN', acknowledged_at=iso(now))
            _count(state, 'unknown')
        elif item['status'] == 'PENDING' and utc(item['expires_at']) <= now:
            item.update(status='EXPIRED', acknowledged_at=iso(now))
            _count(state, 'expired')
        if item['status'] in {'PENDING', 'IN_FLIGHT'} or utc(item.get('acknowledged_at') or item['created_at']) >= now - TERMINAL_TTL:
            keep.append(item)
    state['intents'] = keep


def _identity(event, payload):
    snapshot = event.get('engine_snapshot') or {}
    # Exactly the report's scan-or-minute occurrence unit; each rule is separate.
    scan = snapshot.get('watch_scan_id') or utc(event['alert_time_utc']).replace(second=0, microsecond=0).isoformat()
    raw = [payload['rule_id'], str(scan), payload['symbol'], payload['direction']]
    return hashlib.sha256(json.dumps(raw, separators=(',', ':')).encode()).hexdigest()


def record_events(state, pairs, now):
    """Pure state reducer, also used by offline/concurrency-boundary tests."""
    now = utc(now)
    prune(state, now)
    added = 0
    for event, features in pairs:
        identity = str(event['event_id'])
        when = utc(event['alert_time_utc'])
        if ((identity in state['receipts'] and identity not in state['retry'])
                or not (utc(state['activated_at']) < when <= now and when > now - TTL)):
            continue
        payloads = rules.evaluate_event(event, features, now)
        for payload in payloads:
            dedup = _identity(event, payload)
            if dedup in state['dedup']:
                continue
            state['dedup'][dedup] = iso(when)
            state['intents'].append({
                'intent_id': uuid4().hex, 'dedup_key': dedup, 'payload': payload,
                'text': payload['text'], 'status': 'PENDING', 'created_at': iso(now),
                'expires_at': iso(when + TTL), 'attempt_token': None,
                'attempted_at': None, 'acknowledged_at': None,
            })
            added += 1
            _count(state, 'created')
        state['receipts'][identity] = iso(when)
        retry_sequence = (event.get('symbol') in rules.RULES['PRICE_OI_ENTRY2']['symbols']
                          and rules.source_is_eligible(event, features, now)
                          and rules._score65(features, 'price_oi')
                          and features.get('sequence.capture_status') in {'SOURCE_UNAVAILABLE', 'SOURCE_OVERFLOW'})
        if retry_sequence:
            state['retry'][identity] = {'event_time': iso(when), 'last_attempt': iso(now)}
        else:
            state['retry'].pop(identity, None)
        _count(state, 'checked')
    _encode(state)
    return added


def schema_ready(database_url=None):
    with _connect(database_url) as conn:
        return bool(conn.execute("SELECT to_regclass('bot_settings') IS NOT NULL AND "
                                 "to_regclass('research_events') IS NOT NULL AS ready").fetchone()['ready'])


def initialize_scope(chat_id, now=None, *, database_url=None):
    with _connect(database_url) as conn:
        # A persistent database-clock fence, never reset on deployment/restart.
        now = now or conn.execute('SELECT clock_timestamp() AS now').fetchone()['now']
        state = _locked(conn, key_for(chat_id), now)
        return {'activated_at': state['activated_at'], 'counts': deepcopy(state['counts'])}


def collect(chat_id, now, *, database_url=None):
    now = utc(now)
    with _connect(database_url) as conn:
        key = key_for(chat_id)
        state = _locked(conn, key, now)
        prune(state, now)
        retry_ids = [int(i) for i in sorted(state['retry'], key=lambda i: (state['retry'][i]['last_attempt'], int(i)))[:8]]
        pairs, stats = source.load_batch(conn, activated_at=utc(state['activated_at']), now=now,
                                         processed_ids=[int(i) for i in state['receipts']], limit=24)
        added = record_events(state, pairs, now)
        if retry_ids:
            retry_pairs, retry_stats = source.load_batch(conn, activated_at=utc(state['activated_at']), now=now,
                                                         processed_ids=[], limit=8, retry_event_ids=retry_ids)
            added += record_events(state, retry_pairs, now)
            stats['retry_source'] = retry_stats
        _save(conn, key, state)
        return {**stats, 'created_intents': added, 'activated_at': state['activated_at'],
                'sequence_retry_pending': len(state['retry']),
                'pending': sum(i['status'] == 'PENDING' for i in state['intents']),
                'counts': deepcopy(state['counts'])}


def claim(chat_id, now, *, database_url=None):
    with _connect(database_url) as conn:
        key = key_for(chat_id)
        state = _locked(conn, key, now)
        prune(state, now)
        result = None
        for item in state['intents']:
            if item['status'] == 'PENDING':
                item.update(status='IN_FLIGHT', attempt_token=uuid4().hex, attempted_at=iso(now))
                result = deepcopy(item)
                break
        _save(conn, key, state)
        return result


def finish(chat_id, intent_id, token, terminal, now, *, message_id=None, database_url=None):
    if terminal not in {'DELIVERED', 'UNKNOWN', 'FAILED', 'CANCELLED'}:
        raise ValueError('Invalid terminal state')
    if terminal == 'DELIVERED' and (type(message_id) is not int or message_id <= 0):
        raise ValueError('Positive Telegram message id required')
    with _connect(database_url) as conn:
        key = key_for(chat_id)
        state = _locked(conn, key, now)
        for item in state['intents']:
            if item['intent_id'] == intent_id and item['status'] == 'IN_FLIGHT' and item['attempt_token'] == token:
                item.update(status=terminal, acknowledged_at=iso(now), message_id=message_id)
                _count(state, terminal.lower())
                _save(conn, key, state)
                return True
        return False
