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
import alert_delivery_policy
from watch_transition_store import _connect

STORE_VERSION = 'manual-four-formulas-outbox-v1'
MAX_STATE_BYTES = 1024 * 1024
MAX_RECEIPTS = 4096
TTL = timedelta(minutes=10)
RECEIPT_TTL = timedelta(minutes=20)
C1274_RECEIPT_TTL = timedelta(hours=48)
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
            'c1274_candles': {},
            'intents': [], 'counts': {'checked': 0, 'created': 0}}


def _encode(state):
    value = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if (len(value.encode()) > MAX_STATE_BYTES
            or len(state['receipts']) + len(state.get('c1274_candles', {})) > MAX_RECEIPTS):
        raise ValueError('Manual formula state capacity exceeded; no partial commit')
    return value


def _locked(conn, key, now):
    conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING',
                 (key, _encode(_initial(now))))
    state = json.loads(conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()['value'])
    from_previous = (state.get('rule_version') == rules.PREVIOUS_VERSION
                     and state.get('ruleset_sha256') == rules.PREVIOUS_RULESET_SHA256)
    from_legacy = (state.get('rule_version') == rules.LEGACY_VERSION
                   and state.get('ruleset_sha256') == rules.LEGACY_RULESET_SHA256)
    if state.get('version') == STORE_VERSION and (from_previous or from_legacy):
        # The v4 addition must not replay earlier signals, reset any existing
        # activation fence, or change previously frozen messages/attempts.
        state.update(rule_version=rules.VERSION, ruleset_sha256=rules.RULESET_SHA256)
        state.setdefault('rule_activated_at', {})['MAGNET_OBSERVATION_DOGE_SHORT'] = iso(now)
        state.setdefault('c1274_candles', {})
        if from_legacy:
            # Only v2 still has the replaced C1274 definition. Preserve its
            # original upgrade policy when a destination skipped v3 entirely.
            state['rule_activated_at']['C1274'] = iso(now)
            for item in state['intents']:
                if item['status'] == 'PENDING':
                    if item.get('payload', {}).get('rule_id') == 'C1274':
                        item.update(status='CANCELLED', acknowledged_at=iso(now),
                                    cancellation_reason='C1274_RULE_REPLACED')
                        _count(state, 'cancelled')
                        continue
                    item['payload']['predicate_version'] = rules.VERSION
                    item['text'] = rules.render_message(item['payload'])
                    item['payload']['text'] = item['text']
        _save(conn, key, state)
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
    for field in ('planned_scans', 'planned_sequence_recovery_scans'):
        state[field] = {k: v for k, v in state.get(field, {}).items()
                        if utc(v) >= now - RECEIPT_TTL}
    state['c1274_candles'] = {
        key: value for key, value in state.get('c1274_candles', {}).items()
        if utc(value['observed_at']) >= now - C1274_RECEIPT_TTL
    }
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


def _c1274_identity(payload):
    raw = [payload['rule_id'], payload['symbol'], payload['source_candle_close_utc'],
           payload['direction']]
    return hashlib.sha256(json.dumps(raw, separators=(',', ':')).encode()).hexdigest()


def record_c1274_scan(state, bundle, references, now):
    """Freeze at most one C1274 decision for each observed Futures candle."""
    if not alert_delivery_policy.manual_rule_enabled('C1274'):
        return 0, 'ALERT_POLICY_DISABLED'
    now = utc(now)
    result = rules.evaluate_c1274_scan(bundle, references, now)
    candle_key = hashlib.sha256(
        ("C1274|SOL|" + result['source_candle_close_utc']).encode()
    ).hexdigest()
    existing = state.setdefault('c1274_candles', {}).get(candle_key)
    if existing is not None:
        status = ('DUPLICATE' if existing.get('bundle_sha256') == result['bundle_sha256']
                  else 'SOURCE_REVISION_IGNORED')
        _count(state, 'c1274_' + status.lower())
        return 0, status
    state['c1274_candles'][candle_key] = {
        'observed_at': iso(now), 'source_candle_close_utc': result['source_candle_close_utc'],
        'watch_scan_id': result['watch_scan_id'], 'bundle_sha256': result['bundle_sha256'],
        'matched': result['payload'] is not None,
    }
    _count(state, 'c1274_scans_checked')
    payload = result['payload']
    if payload is None:
        return 0, 'NO_MATCH'
    when = utc(payload['event_time'])
    activation = state.get('rule_activated_at', {}).get('C1274', state['activated_at'])
    if when <= utc(activation) or not (when <= now and when > now - TTL):
        return 0, 'BEFORE_ACTIVATION_OR_STALE'
    dedup = _c1274_identity(payload)
    if dedup in state['dedup']:
        return 0, 'DUPLICATE'
    state['dedup'][dedup] = iso(when)
    state['intents'].append({
        'intent_id': uuid4().hex, 'dedup_key': dedup, 'payload': payload,
        'text': payload['text'], 'status': 'PENDING', 'created_at': iso(now),
        'expires_at': iso(when + TTL), 'attempt_token': None,
        'attempted_at': None, 'acknowledged_at': None,
    })
    _count(state, 'created')
    _count(state, 'c1274_created')
    return 1, 'MATCH'


def record_events(state, pairs, now, *, planned=False):
    """Pure state reducer, also used by offline/concurrency-boundary tests."""
    now = utc(now)
    prune(state, now)
    added = 0
    for event, features in pairs:
        identity = str(event['event_id'])
        when = utc(event['alert_time_utc'])
        scan = (event.get('engine_snapshot') or {}).get('watch_scan_id')
        sequence_recovery = (not planned
                             and scan in state.get('planned_sequence_recovery_scans', {}))
        if not planned and scan in state.get('planned_scans', {}) and not sequence_recovery:
            # This scan was already evaluated before ordinary transport. A
            # later DB delivery receipt must not create tail-of-batch alerts.
            state['receipts'][identity] = iso(when)
            state['retry'].pop(identity, None)
            continue
        if ((identity in state['receipts'] and identity not in state['retry'])
                or not (utc(state['activated_at']) < when <= now and when > now - TTL)):
            continue
        payloads = rules.evaluate_event(event, features, now, planned=planned)
        if sequence_recovery:
            # Only the previously unavailable sequence rule may use this
            # exception. Never turn later transport receipts into a replay of
            # the rules already evaluated in the priority group.
            payloads = [p for p in payloads if p['rule_id'] == 'PRICE_OI_ENTRY2']
        for payload in payloads:
            if not alert_delivery_policy.manual_rule_enabled(payload['rule_id']):
                continue
            activation = state.get('rule_activated_at', {}).get(payload['rule_id'], state['activated_at'])
            if when <= utc(activation):
                continue
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
            if sequence_recovery:
                _count(state, 'planned_sequence_recovered')
        state['receipts'][identity] = iso(when)
        retry_sequence = (not planned and event.get('symbol') in rules.RULES['PRICE_OI_ENTRY2']['symbols']
                          and alert_delivery_policy.manual_rule_enabled('PRICE_OI_ENTRY2')
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
        recovered_before = state['counts'].get('planned_sequence_recovered', 0)
        retry_ids = [int(i) for i in sorted(state['retry'], key=lambda i: (state['retry'][i]['last_attempt'], int(i)))[:8]]
        pairs, stats = source.load_batch(conn, activated_at=utc(state['activated_at']), now=now,
                                         processed_ids=[int(i) for i in state['receipts'] if i.isdecimal()], limit=24)
        added = record_events(state, pairs, now)
        if retry_ids:
            retry_pairs, retry_stats = source.load_batch(conn, activated_at=utc(state['activated_at']), now=now,
                                                         processed_ids=[], limit=8, retry_event_ids=retry_ids)
            added += record_events(state, retry_pairs, now)
            stats['retry_source'] = retry_stats
        _save(conn, key, state)
        return {**stats, 'created_intents': added, 'activated_at': state['activated_at'],
                'planned_sequence_recovery_intents': state['counts'].get('planned_sequence_recovered', 0) - recovered_before,
                'sequence_retry_pending': len(state['retry']),
                'pending': sum(i['status'] == 'PENDING' for i in state['intents']),
                'counts': deepcopy(state['counts'])}


def record_watch_events(chat_id, events, now, *, c1274_bundle=None,
                        price_references=None, database_url=None):
    """Freeze live planned alerts in the outbox before ordinary Watch transport.

    This never inserts or marks a research event DELIVERED. Real transport
    captures retain their existing path, while rule/scan dedup joins both lanes.
    """
    now = utc(now)
    with _connect(database_url) as conn:
        key = key_for(chat_id)
        state = _locked(conn, key, now)
        pairs, stats = source.prepare_watch_pairs(conn, events, now)
        added = record_events(state, pairs, now, planned=True)
        c1274_status = 'NOT_PROVIDED'
        if c1274_bundle is not None:
            try:
                c1274_added, c1274_status = record_c1274_scan(
                    state, c1274_bundle, price_references or {}, now,
                )
                added += c1274_added
            except Exception as exc:
                # One malformed optional score bundle cannot suppress the
                # other four event-based experimental rules.
                c1274_status = 'INVALID:' + type(exc).__name__
                _count(state, 'c1274_invalid')
        recovery_scans = set()
        for event, features in pairs:
            scan = event['engine_snapshot']['watch_scan_id']
            state.setdefault('planned_scans', {})[scan] = iso(now)
            if (alert_delivery_policy.manual_rule_enabled('PRICE_OI_ENTRY2')
                    and event['symbol'] in rules.RULES['PRICE_OI_ENTRY2']['symbols']
                    and rules._score65(features, 'price_oi')
                    and features.get('sequence.capture_status') in {'SOURCE_UNAVAILABLE', 'SOURCE_OVERFLOW'}):
                state.setdefault('planned_sequence_recovery_scans', {})[scan] = iso(now)
                recovery_scans.add(scan)
        _save(conn, key, state)
        return {**stats, 'created_intents': added,
                'c1274_scan_status': c1274_status,
                'planned_sequence_recovery_scans': len(recovery_scans),
                'pending': sum(i['status'] == 'PENDING' for i in state['intents'])}


def claim(chat_id, now, *, database_url=None):
    with _connect(database_url) as conn:
        key = key_for(chat_id)
        state = _locked(conn, key, now)
        prune(state, now)
        result = None
        for item in state['intents']:
            if item['status'] == 'PENDING':
                if not alert_delivery_policy.manual_rule_enabled(item.get('payload', {}).get('rule_id')):
                    item.update(status='CANCELLED', acknowledged_at=iso(now),
                                cancellation_reason='ALERT_POLICY_DISABLED')
                    _count(state, 'cancelled')
                    continue
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
