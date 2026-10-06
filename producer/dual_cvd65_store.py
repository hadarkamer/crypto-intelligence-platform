"""Durable independent dual-CVD Watch episodes and one-attempt delivery.

Only explicit migration 053 creates tables. No providers, research, old Watch
state, schema creation or message transport are called here. Generation intents
are retained as durable deduplication evidence; receipt cleanup is bounded.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import re
from uuid import uuid4

import dual_cvd65_alert as detector
from experimental_reference_price import select_reference
from watch_transition_store import _connect, _digest, _freeze, _iso, _name, canonical

RULE_ID = detector.RULE_ID
DELIVERY_TTL = timedelta(minutes=10)
ORPHAN_TIMEOUT = timedelta(minutes=2)
RECEIPT_RETENTION = timedelta(hours=48)
CLEANUP_LIMIT = 128
MAX_CLAIM_LIMIT = 8
SYMBOLS = frozenset(('BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'HYPE', 'ZEC'))
HASH = re.compile(r'^[0-9a-f]{64}$')


def _utc(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Dual CVD timestamps must include a timezone')
    return parsed.astimezone(timezone.utc)


def schema_ready(*, database_url=None):
    with _connect(database_url) as conn:
        return all(conn.execute('SELECT to_regclass(%s) AS relation', ('public.' + name,)).fetchone()['relation']
            for name in ('dual_cvd65_scopes', 'dual_cvd65_receipts', 'dual_cvd65_intents'))


def initialize_scope(scope, now, *, database_url=None):
    """Persist the first activation; restarts never move or remove this fence."""
    scope, moment = _name(scope, 'scope'), _utc(now)
    with _connect(database_url) as conn:
        conn.execute('''INSERT INTO dual_cvd65_scopes
            (subscription_scope,rule_id,activated_at_utc,updated_at_utc) VALUES (%s,%s,%s,%s)
            ON CONFLICT DO NOTHING''', (scope, RULE_ID, moment, moment))
        row = conn.execute('''SELECT activated_at_utc FROM dual_cvd65_scopes
            WHERE subscription_scope=%s AND rule_id=%s''', (scope, RULE_ID)).fetchone()
        return {'activated_at_utc': _iso(row['activated_at_utc'])}


def _evaluation(value):
    if not isinstance(value, dict) or value.get('rule_id') != RULE_ID:
        raise ValueError('Invalid dual CVD rule identity')
    watch = _name(value.get('watch_scan_id'), 'watch_scan_id')
    source = _utc(value.get('source_at_utc'))
    bundle = value.get('bundle_sha256')
    if not isinstance(bundle, str) or not HASH.fullmatch(bundle):
        raise ValueError('Invalid dual CVD bundle identity')
    observations = value.get('observations')
    if not isinstance(observations, list) or not 1 <= len(observations) <= len(SYMBOLS):
        raise ValueError('Invalid dual CVD observation coverage')
    seen = set()
    for observation in observations:
        if not isinstance(observation, dict) or observation.get('symbol') not in SYMBOLS:
            raise ValueError('Invalid dual CVD symbol')
        if observation['symbol'] in seen:
            raise ValueError('Duplicate dual CVD symbol')
        seen.add(observation['symbol'])
        status = observation.get('status')
        if status not in ('MATCH', 'NO_MATCH', 'UNKNOWN'):
            raise ValueError('Invalid dual CVD observation status')
        if status in ('MATCH', 'NO_MATCH'):
            for key in ('futures_score', 'spot_score'):
                score = observation.get(key)
                if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score) or abs(score) > 100:
                    raise ValueError('Invalid dual CVD known score')
            for key in ('futures_close_at_utc', 'spot_close_at_utc'):
                closed = _utc(observation.get(key))
                if not source-timedelta(minutes=30) <= closed <= source:
                    raise ValueError('Invalid dual CVD candle freshness')
            matching = (abs(observation['futures_score']) >= 65 and abs(observation['spot_score']) >= 65
                and observation['futures_score']*observation['spot_score'] > 0)
            if matching != (status == 'MATCH'):
                raise ValueError('Contradictory dual CVD known status')
        if status == 'MATCH':
            generation = observation.get('generation_key')
            if observation.get('direction') not in ('LONG', 'SHORT') or not isinstance(generation, str) or not HASH.fullmatch(generation):
                raise ValueError('Invalid dual CVD matching generation')
            if (observation['futures_score'] > 0) != (observation['direction'] == 'LONG'):
                raise ValueError('Contradictory dual CVD matching direction')
            expected_generation = detector._digest({'symbol': observation['symbol'],
                'direction': observation['direction'],
                **{key: _utc(observation[key]).isoformat() for key in
                    ('futures_close_at_utc', 'spot_close_at_utc')}})
            if generation != expected_generation:
                raise ValueError('Invalid dual CVD candle generation identity')
    frozen = _freeze(value)
    frozen['source_at_utc'] = _iso(source)
    if len(canonical(frozen).encode('utf-8')) > 64 * 1024:
        raise ValueError('Dual CVD evaluation exceeds size limit')
    return watch, source, frozen


def _cleanup(conn, scope, moment):
    conn.execute('''WITH old AS (
        SELECT subscription_scope,rule_id,watch_scan_id FROM dual_cvd65_receipts
        WHERE subscription_scope=%s AND created_at_utc<%s
        ORDER BY created_at_utc LIMIT %s FOR UPDATE SKIP LOCKED)
        DELETE FROM dual_cvd65_receipts r USING old
        WHERE r.subscription_scope=old.subscription_scope AND r.rule_id=old.rule_id
          AND r.watch_scan_id=old.watch_scan_id''', (scope, moment-RECEIPT_RETENTION, CLEANUP_LIMIT))


def _expire(conn, scope, moment):
    conn.execute('''WITH old AS (
        SELECT intent_id FROM dual_cvd65_intents WHERE subscription_scope=%s AND status='PENDING'
        AND (expires_at<=%s OR (source_at_utc<=%s AND NOT (symbol=ANY(%s))))
        ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED)
        UPDATE dual_cvd65_intents i SET status='EXPIRED',finished_at_utc=%s
        FROM old WHERE i.intent_id=old.intent_id''',
        (scope, moment, moment, list(detector.NOTIFICATION_SYMBOLS), CLEANUP_LIMIT, moment))


def record_cycle(scope, evaluation, now, *, database_url=None, price_references=None):
    """Atomically freeze one fresh source, symbol episodes and outgoing text.

    A first fresh MATCH fires. UNKNOWN preserves the prior direction; a valid
    NO_MATCH resets immediately, without a secondary threshold. Same-generation
    matches never create another intent, even after a valid reset or restart.
    """
    from psycopg.types.json import Jsonb
    scope, moment = _name(scope, 'scope'), _utc(now)
    watch, source, frozen = _evaluation(evaluation)
    if source > moment:
        raise ValueError('Dual CVD source is in the future')
    input_hash = _digest(frozen)
    with _connect(database_url) as conn:
        current = conn.execute('''SELECT * FROM dual_cvd65_scopes
            WHERE subscription_scope=%s AND rule_id=%s FOR UPDATE''', (scope, RULE_ID)).fetchone()
        if current is None:
            raise RuntimeError('Dual CVD scope is not initialized')
        existing = conn.execute('''SELECT input_sha256,result FROM dual_cvd65_receipts
            WHERE subscription_scope=%s AND rule_id=%s AND watch_scan_id=%s''',
            (scope, RULE_ID, watch)).fetchone()
        if existing:
            if existing['input_sha256'] != input_hash:
                raise ValueError('Dual CVD Watch identity collision')
            return {**existing['result'], 'record_status': 'REPLAY', 'created_intents': 0}
        status = 'ACCEPTED'
        if source <= current['activated_at_utc']:
            status = 'PRE_ACTIVATION'
        elif current['last_source_at_utc'] is not None and source <= current['last_source_at_utc']:
            status = 'OUT_OF_ORDER'
        elif source + DELIVERY_TTL <= moment:
            status = 'EXPIRED'
        state = _freeze(current['state'])
        counts = {'evaluated': 0, 'MATCH': 0, 'NO_MATCH': 0, 'UNKNOWN': 0,
                  'episodes_started': 0, 'resets': 0, 'duplicate_generations': 0}
        created = 0
        if status == 'ACCEPTED':
            for observation in frozen['observations']:
                symbol, observed = observation['symbol'], observation['status']
                counts['evaluated'] += 1
                counts[observed] += 1
                # Keep the complete capture/receipt, but only eligible coins
                # participate in live notification episodes.
                if symbol not in detector.NOTIFICATION_SYMBOLS:
                    continue
                previous = state.get(symbol) or {'active_direction': None, 'episode': 0}
                direction, episode = previous['active_direction'], previous['episode']
                if observed == 'NO_MATCH':
                    counts['resets'] += int(direction is not None)
                    direction = None
                elif observed == 'MATCH' and direction != observation['direction']:
                    direction, episode = observation['direction'], episode+1
                    counts['episodes_started'] += 1
                    intent_id = _digest([scope, RULE_ID, symbol, observation['generation_key']])
                    # Display evidence is frozen only when the intent is created.
                    # It must not change the capture, receipt hash or CVD episode.
                    references = price_references if isinstance(price_references, dict) else {}
                    display_observation = {**observation, 'price_reference': select_reference(
                        references.get(symbol), ('FUTURES_CVD', 'SPOT_CVD'), symbol=symbol, as_of=source)}
                    message = detector.render_message(display_observation, source)
                    if not isinstance(message, str) or not message or len(message.encode('utf-8')) > 16 * 1024:
                        raise ValueError('Invalid dual CVD rendered message')
                    payload = {'rule_id': RULE_ID, 'watch_scan_id': watch, 'source_at_utc': _iso(source),
                        'bundle_sha256': frozen['bundle_sha256'], 'observation': display_observation, 'text': message}
                    expires = min(source+DELIVERY_TTL, *(_utc(observation[key])+timedelta(minutes=30)
                        for key in ('futures_close_at_utc', 'spot_close_at_utc')))
                    result = conn.execute('''INSERT INTO dual_cvd65_intents
                        (intent_id,subscription_scope,rule_id,watch_scan_id,symbol,direction,episode,
                         generation_key,bundle_sha256,source_at_utc,expires_at,text,payload,created_at_utc)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(subscription_scope,rule_id,symbol,generation_key) DO NOTHING''',
                        (intent_id, scope, RULE_ID, watch, symbol, direction, episode,
                         observation['generation_key'], frozen['bundle_sha256'], source, expires,
                         message, Jsonb(payload), moment))
                    created += result.rowcount
                    counts['duplicate_generations'] += int(not result.rowcount)
                state[symbol] = {'active_direction': direction, 'episode': episode, 'last_status': observed}
            conn.execute('''UPDATE dual_cvd65_scopes SET state=%s,last_source_at_utc=%s,updated_at_utc=%s
                WHERE subscription_scope=%s AND rule_id=%s''', (Jsonb(state), source, moment, scope, RULE_ID))
        result = {'record_status': status, 'created_intents': created, 'counts': counts}
        conn.execute('''INSERT INTO dual_cvd65_receipts
            (subscription_scope,rule_id,watch_scan_id,source_at_utc,input_sha256,result,created_at_utc)
            VALUES (%s,%s,%s,%s,%s,%s,%s)''', (scope, RULE_ID, watch, source, input_hash, Jsonb(result), moment))
        _cleanup(conn, scope, moment)
        return result


def _public(row):
    result = dict(row)
    for key in ('source_at_utc', 'expires_at', 'attempted_at_utc', 'finished_at_utc', 'created_at_utc'):
        result[key] = _iso(result[key]) if result.get(key) is not None else None
    return result


def claim_pending(scope, now, limit=1, *, database_url=None):
    scope, moment = _name(scope, 'scope'), _utc(now)
    if type(limit) is not int or not 1 <= limit <= MAX_CLAIM_LIMIT:
        raise ValueError('Invalid dual CVD claim limit')
    with _connect(database_url) as conn:
        _expire(conn, scope, moment)
        rows = conn.execute('''SELECT intent_id FROM dual_cvd65_intents
            WHERE subscription_scope=%s AND status='PENDING' AND source_at_utc<=%s AND expires_at>%s
              AND symbol=ANY(%s)
            ORDER BY source_at_utc,intent_id LIMIT %s FOR UPDATE SKIP LOCKED''',
            (scope, moment, moment, list(detector.NOTIFICATION_SYMBOLS), limit)).fetchall()
        claimed = []
        for row in rows:
            claimed.append(_public(conn.execute('''UPDATE dual_cvd65_intents
                SET status='IN_FLIGHT',attempt_token=%s,attempted_at_utc=%s
                WHERE intent_id=%s AND status='PENDING' RETURNING *''',
                (uuid4().hex, moment, row['intent_id'])).fetchone()))
        _cleanup(conn, scope, moment)
        return claimed


def release_unattempted(intent_id, attempt_token, now, *, database_url=None):
    """Only for cancellation before any transport call; never retry ambiguity."""
    intent_id, attempt_token, moment = _name(intent_id, 'intent_id'), _name(attempt_token, 'attempt_token'), _utc(now)
    with _connect(database_url) as conn:
        return bool(conn.execute('''UPDATE dual_cvd65_intents
            SET status=CASE WHEN expires_at<=%s THEN 'EXPIRED' ELSE 'PENDING' END,
                attempt_token=NULL,attempted_at_utc=NULL,
                finished_at_utc=CASE WHEN expires_at<=%s THEN %s ELSE NULL END
            WHERE intent_id=%s AND attempt_token=%s AND status='IN_FLIGHT' AND attempted_at_utc<=%s
            RETURNING intent_id''', (moment, moment, moment, intent_id, attempt_token, moment)).fetchone())


def finish_attempt(intent_id, attempt_token, status, now, *, database_url=None):
    if status not in ('DELIVERED', 'UNKNOWN', 'FAILED'):
        raise ValueError('Invalid dual CVD delivery outcome')
    intent_id, attempt_token, moment = _name(intent_id, 'intent_id'), _name(attempt_token, 'attempt_token'), _utc(now)
    with _connect(database_url) as conn:
        return bool(conn.execute('''UPDATE dual_cvd65_intents SET status=%s,finished_at_utc=%s
            WHERE intent_id=%s AND attempt_token=%s AND status='IN_FLIGHT' AND attempted_at_utc<=%s
            RETURNING intent_id''', (status, moment, intent_id, attempt_token, moment)).fetchone())


def settle_orphans(scope, now, *, database_url=None):
    scope, moment = _name(scope, 'scope'), _utc(now)
    with _connect(database_url) as conn:
        settled = conn.execute('''WITH orphan AS (
            SELECT intent_id FROM dual_cvd65_intents WHERE subscription_scope=%s AND status='IN_FLIGHT'
            AND attempted_at_utc<=%s ORDER BY attempted_at_utc LIMIT %s FOR UPDATE SKIP LOCKED)
            UPDATE dual_cvd65_intents i SET status='UNKNOWN',finished_at_utc=%s
            FROM orphan WHERE i.intent_id=orphan.intent_id RETURNING i.intent_id''',
            (scope, moment-ORPHAN_TIMEOUT, CLEANUP_LIMIT, moment)).fetchall()
        _expire(conn, scope, moment)
        _cleanup(conn, scope, moment)
        return len(settled)
