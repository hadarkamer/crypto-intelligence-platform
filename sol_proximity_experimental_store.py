"""Dedicated durable SOL notification state; existing bot_settings, no DDL/orders."""
from __future__ import annotations
from copy import deepcopy
import hashlib
import json
from uuid import uuid4
from watch_transition_store import _connect
import sol_proximity_experimental_signal as signal

VERSION = 'sol-proximity-notification-store-v1'
MAX_BYTES = 768*1024


def key_for(scope):
    if not str(scope).strip():
        raise ValueError('Scope required')
    return VERSION+':'+hashlib.sha256(str(scope).encode()).hexdigest()


def _validate(state, config):
    if state.get('version') != VERSION or state.get('config_sha256') != config:
        raise ValueError('Frozen SOL configuration mismatch; explicit migration required')


def _maintain(state, now):
    for intent in state['intents']:
        if intent['status'] == 'PENDING' and now >= intent['expires_ms']:
            intent['status'] = 'EXPIRED'
        if intent['status'] == 'IN_FLIGHT' and now >= intent['attempt_ms']+120_000:
            intent['status'] = 'UNKNOWN'
    state['history'] = state['history'][-128:]
    live = [i for i in state['intents'] if i['status'] in ('PENDING', 'IN_FLIGHT')]
    done = [i for i in state['intents'] if i['status'] not in ('PENDING', 'IN_FLIGHT')]
    state['intents'] = done[-max(0, 128-len(live)):] + live if len(live) < 128 else live


def _encode(state):
    raw = json.dumps(state, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    if len(raw.encode()) > MAX_BYTES:
        raise ValueError('SOL durable state capacity exceeded; transaction rolled back')
    return raw


def transact(scope, now, config_sha256, action, *, database_url=None):
    key = key_for(scope)
    with _connect(database_url) as conn:
        lock = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big', signed=True)
        conn.execute('SELECT pg_advisory_xact_lock(%s)', (lock,))
        row = conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()
        if row is None:
            if action is not None:
                raise ValueError('SOL scope not initialized')
            state = {**signal.initial(now), 'version': VERSION, 'config_sha256': config_sha256}
            conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s)', (key, _encode(state)))
        else:
            state = json.loads(row['value'])
            _validate(state, config_sha256)
        _maintain(state, now)
        result = action(state) if action else deepcopy(state)
        _maintain(state, now)
        conn.execute('UPDATE bot_settings SET value=%s WHERE key=%s', (_encode(state), key))
        return deepcopy(result)


def snapshot(scope, *, database_url=None):
    with _connect(database_url) as conn:
        row = conn.execute('SELECT value FROM bot_settings WHERE key=%s', (key_for(scope),)).fetchone()
        return json.loads(row['value']) if row else None


def initialize_scope(scope, now, *, config_sha256, database_url=None):
    return transact(scope, now, config_sha256, None, database_url=database_url)


def ingest(scope, decoded, bars, now, *, config_sha256, database_url=None):
    return transact(scope, now, config_sha256, lambda s: signal.ingest(s, decoded, now, bars), database_url=database_url)


def advance(scope, bars, now, *, config_sha256, database_url=None):
    return transact(scope, now, config_sha256, lambda s: signal.advance(s, bars, now), database_url=database_url)


def claim_pending(scope, now, *, config_sha256, database_url=None):
    def claim(s):
        live = {p['position_id'] for p in s['active'] if p['status'] == 'OPEN'}
        for i in s['intents']:
            if i['status'] == 'PENDING' and i['position_id'] in live and now < i['expires_ms']:
                i.update(status='IN_FLIGHT', attempt_ms=now, attempt_token=uuid4().hex)
                return deepcopy(i)
        return None
    return transact(scope, now, config_sha256, claim, database_url=database_url)


def finish_attempt(scope, intent_id, token, outcome, now, *, message_id=None, config_sha256, database_url=None):
    if outcome not in ('DELIVERED', 'UNKNOWN', 'FAILED', 'CANCELLED_STALE_PRICE'):
        raise ValueError('Invalid delivery terminal state')
    def finish(s):
        for i in s['intents']:
            if i['intent_id'] == intent_id and i.get('attempt_token') == token and i['status'] == 'IN_FLIGHT':
                i.update(status=outcome, acknowledged_ms=now, message_id=message_id)
                signal.count(s, 'DELIVERY_'+outcome)
                return True
        return False
    return transact(scope, now, config_sha256, finish, database_url=database_url)
