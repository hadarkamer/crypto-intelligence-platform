"""Durable SOL g65/k49 pending/next-bar profit-lock observation state and outbox.

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
import sol_g65_experimental_signal as signal

STORE_VERSION = 'sol-g65-k49-lock-cap1-v1'
CONFIG_VERSION = 'sol-g65-k49-reference-limit-lock075tp025tp-v1'
SOURCE_CONTRACT_VERSION = 'all-hyperliquid-perpetual-trade1m-v2'
LEGACY_MIXED_SOURCE_CONFIG_SHA256 = '49d5aac673ade1ef25337cae7310a3520041d0f7bd237f0407da4d9225927e00'
PRICE_SOURCE = 'HYPERLIQUID_SOL_PERPETUAL_TRADE_1M'
LEGACY_POSITION_PRICE_SOURCE = 'BINANCE_SPOT_TRADE_1M'

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
        raise ValueError('SOL g65/k49 state capacity exceeded; no partial commit')
    return encoded


def _initial(now, config_sha256=None):
    return {'version': STORE_VERSION, 'config_version': CONFIG_VERSION,
            'config_sha256': config_sha256, 'source_contract_version': SOURCE_CONTRACT_VERSION,
            'price_source': PRICE_SOURCE, 'activated_at': iso(now),
            'decision_cursor': None, 'last_exit_at': None, 'active': None,
            'history': [], 'intents': [], 'counts': {}}


def _validate(state, config_sha256=None):
    if state.get('version') != STORE_VERSION or state.get('config_version') != CONFIG_VERSION:
        raise ValueError('SOL g65/k49 frozen state version mismatch; explicit migration required')
    if config_sha256 is not None and state.get('config_sha256') != config_sha256:
        raise ValueError('SOL g65/k49 frozen configuration hash mismatch; explicit migration required')


def _migrate_all_prices(state, moment, config_sha256):
    """One reviewed source migration; retain old open exposure and its venue.

    New notifications are fenced at migration time. Frozen old-source opens
    continue on their original source until closed; unfilled limits are
    cancelled explicitly, and uncertain sends are never retried.
    """
    _validate(state, LEGACY_MIXED_SOURCE_CONFIG_SHA256)
    if (not isinstance(config_sha256, str) or len(config_sha256) != 64 or
            any(c not in '0123456789abcdef' for c in config_sha256) or
            config_sha256 == LEGACY_MIXED_SOURCE_CONFIG_SHA256):
        raise ValueError('Reviewed source migration needs a new SHA-256 destination')
    if state.get('all_prices_source_migration'):
        raise ValueError('All-price source migration already recorded')
    _maintain(state, moment)
    if any(i['status'] == 'IN_FLIGHT' for i in state['intents']):
        raise ValueError('Source migration waits for in-flight delivery settlement')
    active = state.get('active')
    if active:
        active.setdefault('price_source', LEGACY_POSITION_PRICE_SOURCE)
        active.setdefault('config_sha256', LEGACY_MIXED_SOURCE_CONFIG_SHA256)
        active.setdefault('source_contract_version', 'legacy-source-contract')
        if active.get('phase') == 'PENDING' and active.get('monitor_status') != 'AMBIGUOUS':
            active.update(outcome='CANCELLED_SOURCE_CHANGE', cancelled_at=iso(moment),
                          exit_reason='UNFILLED_LIMIT_SOURCE_CHANGED')
            state['history'].append(deepcopy(active))
            state['active'] = None
            _count(state, 'cancelled_source_change')
    for intent in state['intents']:
        if intent['status'] == 'PENDING':
            intent.update(status='CANCELLED', acknowledged_at=iso(moment), error_type='SOURCE_CHANGED')
            _count(state, 'source_change_cancelled_notifications')
    original_activation = state['activated_at']
    state.update(config_sha256=config_sha256, source_contract_version=SOURCE_CONTRACT_VERSION,
                 price_source=PRICE_SOURCE, activated_at=iso(max(utc(original_activation), moment)))
    state['all_prices_source_migration'] = {
        'version': SOURCE_CONTRACT_VERSION, 'migrated_at': iso(moment),
        'original_activated_at': original_activation,
        'from_config_sha256': LEGACY_MIXED_SOURCE_CONFIG_SHA256,
        'to_config_sha256': config_sha256, 'from_position_source': LEGACY_POSITION_PRICE_SOURCE,
        'to_position_source': PRICE_SOURCE, 'legacy_open_preserved': bool(state.get('active'))}
    _maintain(state, moment)


def _lock(conn, key):
    number = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big', signed=True)
    conn.execute('SELECT pg_advisory_xact_lock(%s)', (number,))


def _locked(conn, key, config_sha256=None):
    _lock(conn, key)
    row = conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()
    if row is None:
        raise ValueError('SOL g65/k49 scope is not initialized')
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
        raise ValueError('SOL g65/k49 live outbox capacity exceeded')
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


def initialize_scope(scope, now=None, *, config_sha256=None, database_url=None, migrate_all_prices=False):
    """Persist activation once. Repeated deploys preserve all state/fences."""
    key = key_for(scope)
    with _connect(database_url) as conn:
        _lock(conn, key)
        moment = utc(now or conn.execute('SELECT clock_timestamp() AS now').fetchone()['now'])
        conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING',
                     (key, _encode(_initial(moment, config_sha256))))
        row = conn.execute('SELECT value FROM bot_settings WHERE key=%s FOR UPDATE', (key,)).fetchone()
        state = json.loads(row['value'])
        if migrate_all_prices and state.get('config_sha256') != config_sha256:
            _migrate_all_prices(state, moment, config_sha256)
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
    if decision.second or decision.microsecond or decision.minute % 30:
        raise ValueError('SOL g65/k49 decision must be on a UTC 30-minute boundary')
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


def reserve_signal(scope, decision_at, reference_at, reference_price, features, now, *,
                   config_sha256=None, database_url=None):
    """Reserve one pending limit observation, never a historical notification.

    The reference is D+1 OPEN. The pending reservation has no time expiry.
    The durable notification is created only after a verified closed-bar fill.
    """
    decision, reference, moment = map(utc, (decision_at, reference_at, now))
    levels = signal.build_levels(float(_decimal(reference_price)))
    if reference != decision + MINUTE:
        raise ValueError('SOL g65 reference must be next minute after decision')
    if not isinstance(features, dict) or not features.get('valid') or not features.get('signal'):
        raise ValueError('Verified matching frozen features required')
    frozen_features = json.loads(json.dumps(features, allow_nan=False))
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        status = _decision_status(state, decision, moment)
        if status is not None:
            _save(conn, key, state)
            return {'status': status}
        if reference > moment:
            return {'status': 'REFERENCE_NOT_AVAILABLE'}
        state['decision_cursor'] = iso(decision)
        _count(state, 'decisions')
        if moment >= reference + SIGNAL_TTL:
            status = 'STALE_SIGNAL'
        elif state['active'] is not None:
            status = 'ACTIVE_POSITION'
        elif state['last_exit_at'] and decision <= utc(state['last_exit_at']):
            status = 'DECISION_NOT_AFTER_LAST_EXIT'
        else:
            status = 'RESERVED_PENDING'
        state['last_decision'] = {'decision_at': iso(decision), 'status': status, 'observed_at': iso(moment)}
        if status == 'RESERVED_PENDING':
            state['active'] = {
                'position_id': uuid4().hex, 'rule_id': signal.RULE_ID, 'symbol': 'SOL',
                'direction': 'SHORT', 'phase': 'PENDING',
                'price_source': PRICE_SOURCE, 'source_contract_version': SOURCE_CONTRACT_VERSION,
                'config_sha256': config_sha256, 'decision_at': iso(decision),
                'reference_at': iso(reference), 'reference_price': float(reference_price),
                'entry_at': None, 'entry_price': levels['entry_price'],
                'stop_price': levels['stop_loss'], 'take_price': levels['take_profit'],
                'cancel_price': levels['pending_cancel_price'],
                'original_stop_price': levels['stop_loss'],
                'lock_trigger_price': levels['lock_trigger_price'],
                'lock_stop_price': levels['locked_stop_loss'],
                'original_take_distance': levels['original_take_distance'],
                'lock_triggered_at': None, 'lock_effective_at': None,
                'features': frozen_features, 'reserved_at': iso(moment),
                'bar_cursor': iso(reference - MINUTE), 'closed_through': iso(reference),
                'monitor_status': 'AWAITING_CLOSED_BAR'}
            _count(state, 'reserved')
        else:
            _count(state, 'suppressed_' + status.lower())
        _save(conn, key, state)
        return {'status': status, 'position': deepcopy(state['active'])}


def _bar(raw):
    when = utc(raw['open_at'])
    if when.second or when.microsecond:
        raise ValueError('Closed bar must start on a UTC minute boundary')
    prices = {key: _decimal(raw[key]) for key in ('open', 'high', 'low', 'close')}
    if not (prices['low'] <= min(prices['open'], prices['close'])
            <= max(prices['open'], prices['close']) <= prices['high']):
        raise ValueError('Invalid closed OHLC bar')
    return when, prices


def _close(state, active, when, moment, outcome, price=None, reason=None):
    active.update(outcome=outcome, exit_at=iso(when+MINUTE), closed_at=iso(moment),
                  exit_price=float(price) if price is not None else None, exit_reason=reason or outcome)
    state['history'].append(deepcopy(active))
    state['last_exit_at'] = iso(when+MINUTE)
    state['active'] = None
    for intent in state['intents']:
        if intent['position_id'] == active['position_id'] and intent['status'] == 'PENDING':
            intent.update(status='CANCELLED', acknowledged_at=iso(moment), error_type='POSITION_ALREADY_CLOSED')
    _count(state, outcome.lower())


def _ambiguous(state, active, when, prices, reason):
    active.update(monitor_status='AMBIGUOUS', ambiguous_at=iso(when+MINUTE),
                  ambiguity_reason=reason, ambiguous_bar={k: float(v) for k,v in prices.items()})
    _count(state, 'ambiguous')


def advance_position(scope, closedbars, now, *, position_id=None, config_sha256=None, database_url=None):
    """Closed contiguous minutes only. Pending fills/cancels, then monitors SL/TP.

    Unknown intraminute order is retained as AMBIGUOUS, never classified as a
    win or used to release capacity. On the fill minute only surviving CLOSE
    may trigger promotion; later completed minutes use LOW. Every promotion
    becomes effective on the following minute, after this minute's exits.
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
        if active is None or (position_id and active['position_id'] != position_id):
            _save(conn, key, state)
            return {'status': 'NO_ACTIVE_POSITION' if active is None else 'POSITION_CHANGED', 'processed_bars': 0}
        if active.get('monitor_status') == 'AMBIGUOUS':
            return {'status': 'AMBIGUOUS', 'processed_bars': 0, 'position': deepcopy(active)}
        processed, status = 0, 'NO_NEW_CLOSED_BARS'
        for when, prices in bars:
            cursor = utc(active['bar_cursor'])
            if when <= cursor:
                continue
            if when != cursor + MINUTE:
                status = 'PRICE_GAP'
                active.update(monitor_status=status, next_expected_bar=iso(cursor+MINUTE))
                break
            if when + MINUTE > moment:
                status = 'AWAITING_CLOSED_BAR'
                break
            processed += 1
            active.update(bar_cursor=iso(when), closed_through=iso(when+MINUTE), monitor_status='VERIFIED')
            active.pop('next_expected_bar', None)
            just_filled = False
            entered_at_open = False
            if active['phase'] == 'PENDING':
                entry, cancel = _decimal(active['entry_price']), _decimal(active['cancel_price'])
                if prices['open'] <= cancel:
                    _close(state, active, when, moment, 'CANCELLED_BEFORE_FILL')
                    status = 'CANCELLED_BEFORE_FILL'
                    break
                entered_at_open = prices['open'] >= entry
                if not entered_at_open and prices['high'] >= entry and prices['low'] <= cancel:
                    _ambiguous(state, active, when, prices, 'ENTRY_CANCEL_ORDER_UNKNOWN')
                    status = 'AMBIGUOUS'
                    break
                if not entered_at_open and prices['low'] <= cancel:
                    _close(state, active, when, moment, 'CANCELLED_BEFORE_FILL')
                    status = 'CANCELLED_BEFORE_FILL'
                    break
                if not entered_at_open and prices['high'] < entry:
                    status = 'PENDING'
                    continue
                active.update(phase='OPEN', entry_at=iso(when), fill_confirmed_at=iso(when+MINUTE))
                just_filled = True
                _count(state, 'filled')
            stop, take = _decimal(active['stop_price']), _decimal(active['take_price'])
            outcome, exit_price = None, None
            if prices['open'] >= stop:
                outcome, exit_price = 'SL', prices['open']
            elif prices['open'] <= take and not just_filled:
                outcome, exit_price = 'TP', take
            elif prices['high'] >= stop and prices['low'] <= take:
                _ambiguous(state, active, when, prices, 'STOP_TAKE_ORDER_UNKNOWN')
                status = 'AMBIGUOUS'
                break
            elif prices['high'] >= stop:
                outcome, exit_price = 'SL', stop
            elif prices['low'] <= take:
                if just_filled and not entered_at_open and prices['close'] > take:
                    _ambiguous(state, active, when, prices, 'ENTRY_TAKE_ORDER_UNKNOWN')
                    status = 'AMBIGUOUS'
                    break
                outcome, exit_price = 'TP', take
            if outcome:
                reason = ('TAKE_PROFIT' if outcome == 'TP' else 'PROFIT_LOCK_STOP'
                          if active['lock_effective_at'] else 'INITIAL_STOP')
                _close(state, active, when, moment, outcome, exit_price, reason)
                status = 'CLOSED'
                break
            excursion_price = float(prices['close'] if just_filled else prices['low'])
            if (active['lock_triggered_at'] is None and
                    (active['entry_price'] - excursion_price) / active['original_take_distance'] >= .75):
                active.update(stop_price=active['lock_stop_price'], lock_triggered_at=iso(when+MINUTE),
                              lock_effective_at=iso(when+MINUTE), lock_trigger_bar_at=iso(when))
                _count(state, 'profit_lock_activated')
            if just_filled:
                # Stale backfill fills remain observations but never become old alerts.
                expires = when + MINUTE + SIGNAL_TTL
                state['intents'].append({
                    'intent_id': uuid4().hex, 'position_id': active['position_id'],
                    'payload': deepcopy(active), 'status': 'PENDING', 'created_at': iso(moment),
                    'expires_at': iso(expires), 'attempt_token': None, 'attempted_at': None,
                    'acknowledged_at': None})
            status = 'OPEN'
        if not bars:
            active['monitor_status'] = 'NO_PRICE_DATA'
        _maintain(state, moment)
        _save(conn, key, state)
        return {'status': status, 'processed_bars': processed, 'position': deepcopy(active)}


def claim_pending(scope, now, *, expected_position_id=None, config_sha256=None, database_url=None):
    """Commit one attempt, optionally fenced to the caller's vetted position.

    A replacement position can appear while the caller awaits a price request.
    The identity fence prevents claiming a different, unvetted notification.
    """
    moment = utc(now)
    with _connect(database_url) as conn:
        key = key_for(scope)
        state = _locked(conn, key, config_sha256)
        _maintain(state, moment)
        result = None
        for intent in state['intents']:
            if intent['status'] != 'PENDING':
                continue
            if expected_position_id is not None and intent['position_id'] != expected_position_id:
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
