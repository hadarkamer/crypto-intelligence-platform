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
LEGACY_SOL_CONFIG_SHA256 = '698fec7779c0b692d1c939f9541aad2c79c3781b24a140c26c9741c95e2d3941'


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


def migrate_sol_state(state, now, config_sha256):
    """Explicit one-way migration: keep old filled exits; no old pending entries."""
    if state.get('version') != VERSION or state.get('config_sha256') != LEGACY_SOL_CONFIG_SHA256:
        raise ValueError('Unsupported SOL predecessor configuration')
    if len(config_sha256) != 64 or any(c not in '0123456789abcdef' for c in config_sha256):
        raise ValueError('Invalid migration destination fingerprint')
    _maintain(state, now)
    if any(i['status'] == 'IN_FLIGHT' for i in state['intents']):
        raise ValueError('SOL delivery in flight; wait for its terminal state before migration')
    preserved = cancelled = 0
    for p in list(state['active']):
        p.setdefault('rule_id', 'SOL_MAXPAIN_PROXIMITY_GT15')
        p['legacy_formula'] = True
        if p['status'] == 'PENDING':
            signal._finish(state, p, 'CANCELLED_FORMULA_REPLACED', now)
            ep = state['episodes'].get(p['episode_key'])
            if ep and ep['generation'] == p['episode_generation']:
                ep.update(consumed=True, consume_reason='FORMULA_REPLACED')
            cancelled += 1
        else:
            # OPEN/UNKNOWN levels and identity remain unchanged.
            preserved += 1
    for intent in state['intents']:
        if intent['status'] == 'PENDING':
            intent['status'] = 'CANCELLED_FORMULA_REPLACED'
    state['legacy_counts'] = deepcopy(state['counts'])
    state['counts'] = {}
    state['formula_migration'] = {'from_config_sha256': LEGACY_SOL_CONFIG_SHA256,
        'to_config_sha256': config_sha256, 'migrated_ms': now,
        'cancelled_pending': cancelled, 'preserved_filled_or_unknown': preserved,
        'from_rule_id': 'SOL_MAXPAIN_PROXIMITY_GT15', 'to_rule_id': signal.RULE_ID}
    state['config_sha256'] = config_sha256
    state['formula_activated_ms'] = now


def transact(scope, now, config_sha256, action, *, database_url=None, migrate_legacy_sol=False):
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
            if migrate_legacy_sol and state.get('config_sha256') != config_sha256:
                migrate_sol_state(state, now, config_sha256)
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


def initialize_scope(scope, now, *, config_sha256, database_url=None, migrate_legacy_sol=False):
    return transact(scope, now, config_sha256, None, database_url=database_url, migrate_legacy_sol=migrate_legacy_sol)


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
