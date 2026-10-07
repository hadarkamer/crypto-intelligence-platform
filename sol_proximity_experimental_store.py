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
PRE_HYPERLIQUID_CONFIGS = {
    'SOL': '6f293a821de4ce65629e654f12d2854e6c1bc73e3b825c7217a225a7f9c0f06a',
    'HYPE': 'ca897b4a125f38ac464327376b64dc354717ee781b8a3ee9402695e991df969d',
    'DOGE': '2f1cc90bacf40e62e1564ba15fd26bbba1e06eab2e2128d291217aa8ca66efd2',
    'XRP': 'ddfff137a407b8e9fac63e7393e8aee7af1fefdb4f141464319b4d0e7d5a1443',
    'ETH': '0a534d611816646be5bad2cfb571a74a2bcd234e059b1ac070fd3414c628d854',
}


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
    if state.get('legacy_source_state'):
        state['legacy_source_state']['history'] = state['legacy_source_state']['history'][-128:]
    live = [i for i in state['intents'] if i['status'] in ('PENDING', 'IN_FLIGHT')]
    done = [i for i in state['intents'] if i['status'] not in ('PENDING', 'IN_FLIGHT')]
    state['intents'] = done[-max(0, 128-len(live)):] + live if len(live) < 128 else live


def _encode(state):
    def encode():
        return json.dumps(state, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    raw = encode()
    if len(raw.encode()) > MAX_BYTES:
        # Optional execution proof must never exhaust the original notification
        # lifecycle's state capacity (for example when OPEN duplicates a payload
        # into the notification outbox). Dropped proof cannot create/revive an
        # execution plan; its receiver lease expires without a new heartbeat.
        candidates = []
        for source_state in (state, state.get('legacy_source_state', {})):
            candidates += list(source_state.get('history', []))
            candidates += [i.get('payload', {}) for i in source_state.get('intents', [])]
            candidates += list(source_state.get('active', []))
        for plan in candidates:
            removed = plan.pop('execution_evidence', None)
            diagnostic = plan.pop('execution_evidence_error', None)
            if removed is not None or diagnostic is not None:
                raw = encode()
                if len(raw.encode()) <= MAX_BYTES:
                    break
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


def migrate_price_source(state, now, config_sha256, coin):
    """Drain filled predecessors on their original route; rebase only new plans.

    A source change cannot certify absence, resurrect consumed targets, or
    silently reprice an already filled observation. The legacy cursor is kept
    independently; it must catch up before new target intake can proceed.
    """
    if coin not in PRE_HYPERLIQUID_CONFIGS:
        raise ValueError('Unsupported MaxPain source migration coin')
    previous = PRE_HYPERLIQUID_CONFIGS[coin]
    if state.get('version') != VERSION or state.get('config_sha256') != previous:
        raise ValueError('Unsupported MaxPain predecessor configuration')
    if len(config_sha256) != 64 or any(c not in '0123456789abcdef' for c in config_sha256):
        raise ValueError('Invalid migration destination fingerprint')
    if state.get('legacy_source_state') or state.get('source_migration'):
        raise ValueError('Source migration already exists under another configuration')
    _maintain(state, now)
    if any(i['status'] == 'IN_FLIGHT' for i in state['intents']):
        raise ValueError('MaxPain delivery in flight; wait for terminal state before migration')
    if any(p['status'] not in ('PENDING', 'OPEN', 'UNKNOWN') for p in state['active']):
        raise ValueError('Unsupported active state during source migration')
    cancelled = 0
    for p in list(state['active']):
        if p['status'] == 'PENDING':
            signal._finish(state, p, 'CANCELLED_SOURCE_REPLACED', now)
            cancelled += 1
    for i in state['intents']:
        if i['status'] == 'PENDING':
            i['status'] = 'CANCELLED_SOURCE_REPLACED'
    old_route = ('HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M' if coin == 'HYPE'
                 else f'BINANCE_SPOT_{coin}USDT_TRADE_1M')
    legacy = signal.initial(now)
    legacy.update(active=deepcopy(state['active']), bar_cursor_ms=state['bar_cursor_ms'],
                  source_route=old_route, from_config_sha256=previous)
    state['legacy_source_state'] = legacy
    state['source_previous_counts'] = deepcopy(state['counts'])
    state['counts'] = {}
    state['active'] = []
    for episode in state['episodes'].values():
        # Never clear touched/filled history. Absence and a later return remain
        # the only way for an existing target number to start a new episode.
        if episode['present']:
            episode.update(consumed=True, bootstrap_unverified=True,
                           consume_reason='SOURCE_REBASE_UNVERIFIED')
    state.update(config_sha256=config_sha256, initialized_universe=False,
                 last_snapshot_ms=None, last_cycle_id=None, last_snapshot_complete=False,
                 bar_cursor_ms=now//signal.MINUTE*signal.MINUTE-signal.MINUTE,
                 source_activated_ms=now)
    state['source_migration'] = dict(from_config_sha256=previous, to_config_sha256=config_sha256,
        coin=coin, migrated_ms=now, cancelled_pending=cancelled,
        preserved_filled_or_unknown=len(legacy['active']), legacy_price_source=old_route,
        new_price_source=f'HYPERLIQUID_{coin}_PERPETUAL_TRADE_1M',
        policy='CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE')


def transact(scope, now, config_sha256, action, *, database_url=None, migrate_legacy_sol=False,
             migrate_source_coin=None):
    key = key_for(scope)
    execution_changed = False
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
            if migrate_source_coin and state.get('config_sha256') != config_sha256:
                if migrate_source_coin == 'SOL' and state.get('config_sha256') == LEGACY_SOL_CONFIG_SHA256:
                    migrate_sol_state(state, now, PRE_HYPERLIQUID_CONFIGS['SOL'])
                migrate_price_source(state, now, config_sha256, migrate_source_coin)
            elif migrate_legacy_sol and state.get('config_sha256') != config_sha256:
                migrate_sol_state(state, now, config_sha256)
            _validate(state, config_sha256)
        _maintain(state, now)
        result = action(state) if action else deepcopy(state)
        _maintain(state, now)
        conn.execute('UPDATE bot_settings SET value=%s WHERE key=%s', (_encode(state), key))
        # Additive transport only. Its explicit migration is never run here;
        # failure must not roll back an otherwise valid source observation.
        from experimental_execution_bridge import approved_enabled
        if approved_enabled():
            conn.execute('SAVEPOINT approved_execution_outbox')
            try:
                from approved_alert_outbox import synchronize
                execution_changed = synchronize(conn, key, state, now)
            except Exception:
                conn.execute('ROLLBACK TO SAVEPOINT approved_execution_outbox')
                print('[approved-execution] outbox unavailable; source decision preserved', flush=True)
            finally:
                conn.execute('RELEASE SAVEPOINT approved_execution_outbox')
        result = deepcopy(result)
    # The context manager has committed successfully before waking the sender.
    # An exception/rollback above never produces a pre-commit execution wake.
    if execution_changed:
        try:
            from experimental_execution_forwarder import notify_source_commit
            notify_source_commit(key)
        except Exception:
            print('[approved-execution] wake unavailable; durable recovery pending', flush=True)
    return result


def snapshot(scope, *, database_url=None):
    with _connect(database_url) as conn:
        row = conn.execute('SELECT value FROM bot_settings WHERE key=%s', (key_for(scope),)).fetchone()
        return json.loads(row['value']) if row else None


def initialize_scope(scope, now, *, config_sha256, database_url=None, migrate_legacy_sol=False,
                     migrate_source_coin=None):
    return transact(scope, now, config_sha256, None, database_url=database_url,
                    migrate_legacy_sol=migrate_legacy_sol, migrate_source_coin=migrate_source_coin)


def ingest(scope, decoded, bars, now, *, config_sha256, database_url=None, execution_enabled=None):
    # Optional evidence capture shares the existing transaction and never
    # changes formula admission or schedules an additional database operation.
    from experimental_execution_bridge import enabled, capture_maxpain_evidence
    capture = enabled() if execution_enabled is None else execution_enabled
    if type(capture) is not bool:
        raise ValueError('Explicit boolean execution evidence mode required')
    def action(state):
        prior = deepcopy(state) if capture else None
        result = signal.ingest(state, decoded, now, bars)
        if capture:
            capture_maxpain_evidence(state, decoded, prior, config_sha256=config_sha256, max_bytes=MAX_BYTES)
        return result
    return transact(scope, now, config_sha256, action, database_url=database_url)


def advance(scope, bars, now, *, config_sha256, database_url=None):
    return transact(scope, now, config_sha256, lambda s: signal.advance(s, bars, now), database_url=database_url)


def advance_legacy_source(scope, bars, now, *, config_sha256, database_url=None):
    return transact(scope, now, config_sha256,
                    lambda s: signal.advance(s['legacy_source_state'], bars, now), database_url=database_url)


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
