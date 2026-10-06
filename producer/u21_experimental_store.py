"""Durable U21 cap-one observation state and at-most-one-attempt outbox.

Only the existing ``bot_settings`` table is used. No DDL, market fetches,
Telegram calls, trading orders, or time-based position expiry belong here.
Initialization is explicit; ordinary operations cannot silently create/reset a
scope. A row lock and a per-scope transaction advisory lock serialize writers.
Unknown delivery outcomes retain the position and are never resent.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from uuid import uuid4

from watch_transition_store import _connect

STORE_VERSION = 'u21-xrp-experimental-cap1-v1'
CONFIG_VERSION = 'u21-xrp-short-sl005-tp08-corrected-closed1m-v1'
SIGNAL_TTL = timedelta(seconds=90)
ORPHAN_TIMEOUT = timedelta(minutes=2)
MINUTE = timedelta(minutes=1)
MAX_HISTORY = 256
MAX_INTENTS = 128
MAX_STATE_BYTES = 512 * 1024


def utc(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Timezone-aware timestamp required')
    return value.astimezone(timezone.utc)


def iso(value):
    return utc(value).isoformat()


def key_for(scope):
    if not str(scope).strip():
        raise ValueError('Nonempty scope required')
    return STORE_VERSION + ':' + hashlib.sha256(str(scope).encode()).hexdigest()


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError('Finite positive price required')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('Finite positive price required') from None
    if not result.is_finite() or result <= 0:
        raise ValueError('Finite positive price required')
    return result


def _encode(state):
    encoded = json.dumps(state, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    if len(encoded.encode()) > MAX_STATE_BYTES or len(state['history']) > MAX_HISTORY or len(state['intents']) > MAX_INTENTS:
        raise ValueError('U21 state capacity exceeded; no partial commit')
    return encoded


def _initial(now, config_sha256=None):
    return {'version': STORE_VERSION, 'config_version': CONFIG_VERSION,
            'config_sha256': config_sha256, 'activated_at': iso(now),
            'decision_cursor': None, 'last_exit_at': None, 'active': None,
            'history': [], 'intents': [], 'counts': {}}


def _validate(state, config_sha256=None):
    if state.get('version') != STORE_VERSION or state.get('config_version') != CONFIG_VERSION:
        raise ValueError('U21 frozen state version mismatch; explicit migration required')
    if config_sha256 is not None and state.get('config_sha256') != config_sha256:
        raise ValueError('U21 frozen configuration hash mismatch; explicit migration required')


def _lock(conn, key):
    number = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big', signed=True)
    conn.execute('SELECT pg_advisory_xact_lock(%s)', (number,))


def _locked(conn, key, config_sha256=None):
    _lock(conn, key)
    row = conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()
    if row is None:
        raise ValueError('U21 scope is not initialized')
    state = json.loads(row['value'])
    _validate(state, config_sha256)
    return state


def _save(conn, key, state):
    conn.execute('UPDATE bot_settings SET value=%s WHERE key=%s', (_encode(state), key))


def _count(state, name):
    state['counts'][name] = state['counts'].get(name, 0) + 1


def _maintain(state, now):
    for intent in state['intents']:
        if intent['status'] == 'PENDING' and utc(intent['expires_at']) <= now:
            intent.update(status='EXPIRED', acknowledged_at=iso(now))
            _count(state, 'expired')
        elif intent['status'] == 'IN_FLIGHT' and utc(intent['attempted_at']) + ORPHAN_TIMEOUT <= now:
            intent.update(status='UNKNOWN', acknowledged_at=iso(now), error_type='ORPHANED_ATTEMPT')
            _count(state, 'unknown')
    live = [i for i in state['intents'] if i['status'] in {'PENDING', 'IN_FLIGHT'}]
    terminal = [i for i in state['intents'] if i['status'] not in {'PENDING', 'IN_FLIGHT'}]
    if len(live) > MAX_INTENTS:
        raise ValueError('U21 live outbox capacity exceeded')
    state['intents'] = terminal[-max(0, MAX_INTENTS-len(live)):] + live if len(live) < MAX_INTENTS else live
    state['history'] = state['history'][-MAX_HISTORY:]
    # Count limits alone are insufficient because feature snapshots vary in
    # size. Drop only oldest completed evidence, preserving active exposure,
    # its delivery receipt, all uncompleted attempts and monotonic fences.
    while len(json.dumps(state, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()) > MAX_STATE_BYTES:
        if len(state['history']) > 1:
            del state['history'][:max(1, len(state['history'])//2)]
            continue
        active_id = (state['active'] or {}).get('position_id')
        removable = [i for i in state['intents'][:-1]
                     if i['status'] not in {'PENDING', 'IN_FLIGHT'} and i['position_id'] != active_id]
        if not removable:
            break  # Oversized active evidence fails atomically in _encode.
        identities = {i['intent_id'] for i in removable[:max(1, len(removable)//2)]}
        state['intents'] = [i for i in state['intents'] if i['intent_id'] not in identities]


def initialize_scope(scope, now=None, *, config_sha256=None, database_url=None):
    """Persist activation once. Repeated deploys preserve all state/fences."""
    key = key_for(scope)
    with _connect(database_url) as conn:
        _lock(conn, key)
        moment = utc(now or conn.execute('SELECT clock_timestamp() AS now').fetchone()['now'])
        conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING',
                     (key, _encode(_initial(moment, config_sha256))))
        row = conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()
        state = json.loads(row['value'])
        _validate(state, config_sha256)
        _maintain(state, moment)
        _save(conn, key, state)
        return deepcopy(state)


def snapshot(scope, *, database_url=None):
    """Read-only snapshot; missing scopes are not implicitly activated."""
    with _connect(database_url) as conn:
        row = conn.execute('SELECT value FROM bot_settings WHERE key=%s', (key_for(scope),)).fetchone()
        if row is None:
            return None
        state = json.loads(row['value'])
        _validate(state)
        return state


def _decision_status(state, decision, now):
    if decision.second or decision.microsecond or decision.minute % 15:
        raise ValueError('U21 decision must be on a UTC 15-minute boundary')
    if decision <= utc(state['activated_at']):
        return 'BEFORE_ACTIVATION'
    if decision > now:
        return 'FUTURE_DECISION'
    if state['decision_cursor'] is not None and decision <= utc(state['decision_cursor']):
        return 'ALREADY_PROCESSED'
    return None


def record_no_signal(scope, decision_at, now, *, reason='NO_MATCH', config_sha256=None, database_url=None):
    """Freeze the decision once, including unavailable/stale/blocked slots."""
    decision, moment = utc(decision_at), utc(now)
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        status = _decision_status(state, decision, moment)
        if status is None:
            state['decision_cursor'] = iso(decision)
            state['last_decision'] = {'decision_at': iso(decision), 'status': str(reason), 'observed_at': iso(moment)}
            _count(state, 'decisions')
            status = str(reason)
        _save(conn, key, state)
        return {'status': status, 'decision_cursor': state['decision_cursor']}


def reserve_signal(scope, decision_at, entry_at, entryprice, features, now, *, text=None,
                   config_sha256=None, database_url=None):
    """Atomically accept a fresh signal only when the continuous scope is flat."""
    decision, entry, moment = utc(decision_at), utc(entry_at), utc(now)
    price = _decimal(entryprice)
    # Preserve the corrected research engine's IEEE-754 threshold arithmetic.
    # Decimal is used only to compare the frozen resulting values, never to
    # recompute a mathematically rounded threshold different from that engine.
    stop = _decimal(float(price) * 1.005)
    take = _decimal(float(price) * .92)
    if entry != decision + MINUTE:
        raise ValueError('U21 entry must be the next minute open after decision')
    if not isinstance(features, dict):
        raise ValueError('Frozen features must be an object')
    # Freeze/validate before touching the state, including finite JSON numbers.
    frozen_features = json.loads(json.dumps(features, allow_nan=False))
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        status = _decision_status(state, decision, moment)
        if status is not None:
            _save(conn, key, state)
            return {'status': status, 'position': deepcopy(state['active']), 'intent': None}
        if entry > moment:
            # Unlike stale/occupied decisions this slot has not matured yet.
            _save(conn, key, state)
            return {'status': 'ENTRY_NOT_AVAILABLE', 'position': deepcopy(state['active']), 'intent': None}
        state['decision_cursor'] = iso(decision)
        _count(state, 'decisions')
        if moment >= entry + SIGNAL_TTL:
            status = 'STALE_SIGNAL'
        elif state['active'] is not None:
            status = 'ACTIVE_POSITION'
        elif state['last_exit_at'] is not None and decision <= utc(state['last_exit_at']):
            status = 'DECISION_NOT_AFTER_LAST_EXIT'
        else:
            status = 'RESERVED'
        state['last_decision'] = {'decision_at': iso(decision), 'status': status, 'observed_at': iso(moment)}
        if status != 'RESERVED':
            _count(state, 'suppressed_' + status.lower())
            _save(conn, key, state)
            return {'status': status, 'position': deepcopy(state['active']), 'intent': None}
        position = {'position_id': uuid4().hex, 'rule_id': 'U21', 'symbol': 'XRP', 'direction': 'SHORT',
                    'decision_at': iso(decision), 'entry_at': iso(entry), 'entry_price': float(price),
                    'stop_price': float(stop), 'take_price': float(take),
                    'entry_price_decimal': str(price), 'stop_price_decimal': str(stop),
                    'take_price_decimal': str(take), 'features': frozen_features,
                    'reserved_at': iso(moment), 'bar_cursor': iso(entry-MINUTE),
                    'closed_through': iso(entry), 'monitor_status': 'AWAITING_CLOSED_BAR'}
        intent = {'intent_id': uuid4().hex, 'position_id': position['position_id'],
                  'payload': deepcopy(position), 'text': text, 'status': 'PENDING',
                  'created_at': iso(moment), 'expires_at': iso(entry+SIGNAL_TTL),
                  'attempt_token': None, 'attempted_at': None, 'acknowledged_at': None}
        state['active'] = position
        state['intents'].append(intent)
        _maintain(state, moment)
        _count(state, 'reserved')
        _save(conn, key, state)
        return {'status': status, 'position': deepcopy(position), 'intent': deepcopy(intent)}


def _bar(raw):
    when = utc(raw['open_at'])
    if when.second or when.microsecond:
        raise ValueError('Closed bar must start on a UTC minute boundary')
    prices = {key: _decimal(raw[key]) for key in ('open', 'high', 'low', 'close')}
    if not (prices['low'] <= min(prices['open'], prices['close'])
            <= max(prices['open'], prices['close']) <= prices['high']):
        raise ValueError('Invalid closed OHLC bar')
    return when, prices


def advance_position(scope, closedbars, now, *, position_id=None, config_sha256=None, database_url=None):
    """Advance only over contiguous, completed 1m bars; gaps retain exposure.

    The outcome timestamp is the first-touch minute's END timestamp, matching
    the corrected research ledger. A dual touch is permanently ambiguous and
    retains capacity, unless the bar OPEN already determines the first hit.
    Adverse opening gaps fill at the open; favorable gaps receive no improvement
    beyond the frozen target.
    """
    moment = utc(now)
    bars = [_bar(raw) for raw in closedbars]
    if any(bars[i][0] >= bars[i+1][0] for i in range(len(bars)-1)):
        raise ValueError('Closed bars must be unique and ascending')
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        active = state['active']
        if active is None or (position_id is not None and active['position_id'] != position_id):
            _save(conn, key, state)
            return {'status': 'NO_ACTIVE_POSITION' if active is None else 'POSITION_CHANGED', 'processed_bars': 0}
        if active.get('monitor_status') == 'AMBIGUOUS':
            _save(conn, key, state)
            return {'status': 'AMBIGUOUS', 'processed_bars': 0, 'position': deepcopy(active)}
        processed = 0
        status = 'NO_NEW_CLOSED_BARS'
        stop, take = _decimal(active['stop_price_decimal']), _decimal(active['take_price_decimal'])
        for when, prices in bars:
            cursor = utc(active['bar_cursor'])
            if when <= cursor:
                continue
            expected = cursor + MINUTE
            if when != expected:
                status = 'PRICE_GAP'
                active.update(monitor_status=status, next_expected_bar=iso(expected), observed_gap_at=iso(when))
                break
            if when + MINUTE > moment:
                status = 'AWAITING_CLOSED_BAR'
                break
            active.update(bar_cursor=iso(when), closed_through=iso(when+MINUTE), monitor_status='VERIFIED')
            active.pop('next_expected_bar', None)
            active.pop('observed_gap_at', None)
            processed += 1
            outcome, exit_price = None, None
            if prices['open'] >= stop:
                outcome, exit_price = 'SL', prices['open']
            elif prices['open'] <= take:
                outcome, exit_price = 'TP', take
            elif prices['high'] >= stop and prices['low'] <= take:
                active.update(monitor_status='AMBIGUOUS', ambiguous_at=iso(when+MINUTE),
                              ambiguous_bar={k: float(v) for k, v in prices.items()})
                _count(state, 'ambiguous')
                _save(conn, key, state)
                return {'status': 'AMBIGUOUS', 'processed_bars': processed, 'position': deepcopy(active)}
            elif prices['high'] >= stop:
                outcome, exit_price = 'SL', stop
            elif prices['low'] <= take:
                outcome, exit_price = 'TP', take
            if outcome:
                active.update(outcome=outcome, exit_at=iso(when+MINUTE), exit_price=float(exit_price),
                              exit_price_decimal=str(exit_price), closed_at=iso(moment),
                              exit_bar={k: float(v) for k, v in prices.items()})
                finished = deepcopy(active)
                state['history'].append(finished)
                state['last_exit_at'] = iso(when+MINUTE)
                state['active'] = None
                for intent in state['intents']:
                    if intent['position_id'] == finished['position_id'] and intent['status'] == 'PENDING':
                        intent.update(status='CANCELLED', acknowledged_at=iso(moment), error_type='POSITION_ALREADY_CLOSED')
                        _count(state, 'cancelled')
                _count(state, outcome.lower())
                _maintain(state, moment)
                _save(conn, key, state)
                return {'status': 'CLOSED', 'processed_bars': processed, 'position': finished}
            status = 'OPEN'
        if not bars:
            active['monitor_status'] = 'NO_PRICE_DATA'
        _save(conn, key, state)
        return {'status': status, 'processed_bars': processed, 'position': deepcopy(active)}


def claim_pending(scope, now, *, config_sha256=None, database_url=None):
    """Commit the unique attempt token before the caller contacts Telegram."""
    moment = utc(now)
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        result = None
        for intent in state['intents']:
            if intent['status'] != 'PENDING':
                continue
            if state['active'] is None or state['active']['position_id'] != intent['position_id']:
                intent.update(status='CANCELLED', acknowledged_at=iso(moment), error_type='POSITION_ALREADY_CLOSED')
                _count(state, 'cancelled')
                continue
            if state['active'].get('monitor_status') == 'AMBIGUOUS':
                intent.update(status='CANCELLED', acknowledged_at=iso(moment), error_type='POSITION_ALREADY_AMBIGUOUS')
                _count(state, 'cancelled')
                continue
            intent.update(status='IN_FLIGHT', attempt_token=uuid4().hex, attempted_at=iso(moment))
            result = deepcopy(intent)
            _count(state, 'attempted')
            break
        _save(conn, key, state)
        return result


def cancel_pending(scope, position_id, now, *, reason='PRE_SEND_VETO', config_sha256=None, database_url=None):
    """Cancel only an unattempted message, never the tracked position.

    The caller may veto delivery using provisional-price evidence. That evidence
    is deliberately incapable of closing/freeing the position: only the frozen
    completed-bar oracle can do so. Claimed/ambiguous deliveries cannot be reset.
    """
    moment = utc(now)
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        changed = 0
        for intent in state['intents']:
            if intent['position_id'] == position_id and intent['status'] == 'PENDING':
                intent.update(status='CANCELLED', acknowledged_at=iso(moment), error_type=str(reason)[:160])
                _count(state, 'cancelled')
                changed += 1
        _save(conn, key, state)
        return changed


def finish_attempt(scope, intent_id, attempt_token, status, now, *, message_id=None,
                   error_type=None, config_sha256=None, database_url=None):
    if status not in {'DELIVERED', 'FAILED', 'UNKNOWN'}:
        raise ValueError('Invalid terminal attempt status')
    if status == 'DELIVERED' and (type(message_id) is not int or message_id <= 0):
        raise ValueError('Positive Telegram message id required')
    moment = utc(now)
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        for intent in state['intents']:
            if intent['intent_id'] == intent_id and intent['status'] == 'IN_FLIGHT' and intent['attempt_token'] == attempt_token:
                intent.update(status=status, acknowledged_at=iso(moment), message_id=message_id,
                              error_type=str(error_type)[:160] if error_type else None)
                _count(state, status.lower())
                _save(conn, key, state)
                return True
        _save(conn, key, state)
        return False
