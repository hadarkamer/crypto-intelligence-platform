"""Frozen SOL MaxPain proximity >15 experiment; pure prospective lifecycle.

Notification-only. Episodes are shared across timeframes and liquidated sides.
A target is new only after a complete seven-timeframe snapshot proved absence.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import math

MINUTE = 60_000
TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')
RULE_ID = 'SOL_MAXPAIN_PROXIMITY_GT15'
CONFIG_VERSION = 'sol-proximity-gt15-adverse2-stop5d-concurrent-v1'
MAX_EPISODES = 4096
MAX_ACTIVE = 256


def milliseconds(value):
    if isinstance(value, (int, float)):
        return int(value)
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Timezone-aware timestamp required')
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def number(value):
    if isinstance(value, bool):
        raise ValueError('Positive finite price required')
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError('Positive finite price required')
    return value


def target_key(value):
    return format(Decimal(str(number(value))).normalize(), 'f')


def near(first, second):
    a, b = Decimal(str(first)), Decimal(str(second))
    return abs(a-b) <= Decimal('.002') * min(a, b)


def levels(source, target):
    source, target = number(source), number(target)
    signed = target-source
    if signed == 0:
        raise ValueError('Zero target distance')
    result = {'source_price': source, 'target_price': target, 'direction': 1 if signed > 0 else -1,
              'entry_price': source-2*signed, 'stop_price': source-5*signed, 'take_price': target}
    for key in ('entry_price', 'stop_price', 'take_price'):
        number(result[key])
    return result


def decode_bundle(bundle, now):
    """Validate only the SOL source generation, not unrelated derivatives."""
    if not isinstance(bundle, dict) or not bundle.get('cycle_id'):
        raise ValueError('Missing Watch generation')
    computed = milliseconds(bundle['computed_at_utc'])
    if computed > now or now-computed > 5*MINUTE:
        raise ValueError('Stale or future Watch generation')
    coin = bundle.get('coins', {}).get('SOL', {})
    slots = {(s['timeframe'], s['source_side']): s for s in coin.get('maxpain', [])}
    if len(slots) != len(coin.get('maxpain', [])):
        raise ValueError('Duplicate frozen score slots')
    raw_rows = coin.get('sources', {}).get('maxpain_operational_rows', [])
    rows, targets, complete = [], set(), True
    if len(raw_rows) != 7 or {r.get('timeframe') for r in raw_rows} != set(TIMEFRAMES):
        complete = False
    seen = set()
    for row in raw_rows:
        tf = row.get('timeframe')
        if tf not in TIMEFRAMES or tf in seen:
            raise ValueError('Duplicate or invalid timeframe')
        seen.add(tf)
        try:
            source = number(row['current_price'])
            observed = milliseconds(row['source_observed_at_utc'])
            price_at = milliseconds(row['price_fetched_at_utc'])
            if max(observed, price_at) > computed or min(observed, price_at) < computed-30*MINUTE:
                raise ValueError('Source clock outside bounded generation')
        except (KeyError, TypeError, ValueError):
            complete = False
            continue
        for side, field in (('LONG', 'long_max_pain'), ('SHORT', 'short_max_pain')):
            try:
                target = number(row[field])
            except (KeyError, TypeError, ValueError):
                complete = False
                continue
            key = target_key(target)
            targets.add(key)
            slot = slots.get((tf, side), {})
            try:
                score = float(slot.get('components', {}).get('target_proximity'))
                eligible = (slot.get('status') == 'SCORED' and math.isfinite(score) and round(score, 2) > 15
                            and number(slot.get('target_price')) == target)
                total_score = float(slot.get('score') or 0)
                if not math.isfinite(total_score):
                    raise ValueError('Nonfinite total score')
                # Inverted MaxPain side is part of the original contract.
                eligible = eligible and ((side == 'SHORT' and target > source) or (side == 'LONG' and target < source))
                geometry = levels(source, target) if eligible else None
            except (ValueError, TypeError):
                score, total_score, eligible, geometry = 0, 0, False, None
            rows.append({'key': key, 'timeframe': tf, 'source_side': side, 'score': score,
                         'total_score': total_score, 'eligible': eligible, 'provider_valid': ((side == 'SHORT' and target > source) or (side == 'LONG' and target < source)), 'observed_ms': observed, 'price_ms': price_at,
                         'target_price': target, 'levels': geometry})
    rows.sort(key=lambda r: (-r['total_score'], TIMEFRAMES.index(r['timeframe']), r['source_side'], r['target_price']))
    return {'cycle_id': str(bundle['cycle_id']), 'computed_ms': computed, 'complete': complete,
            'targets': sorted(targets), 'rows': rows}


def initial(now):
    return {'activated_ms': now, 'initialized_universe': False, 'last_snapshot_ms': None,
            'last_cycle_id': None, 'bar_cursor_ms': now//MINUTE*MINUTE-MINUTE,
            'episodes': {}, 'active': [], 'history': [], 'intents': [], 'counts': {},
            'last_snapshot_complete': False}


def count(state, name):
    state['counts'][name] = state['counts'].get(name, 0)+1


def _finish(state, p, status, t):
    p.update(status=status, terminal_ms=t)
    state['history'].append(deepcopy(p))
    state['active'].remove(p)
    count(state, status)
    for intent in state['intents']:
        if intent['position_id'] == p['position_id'] and intent['status'] == 'PENDING':
            intent['status'] = 'CANCELLED_POSITION_ENDED'


def ingest(state, decoded, now, guard_bars):
    """Called only after the global minute monitor caught up to intake time."""
    if state['last_snapshot_ms'] is not None and decoded['computed_ms'] <= state['last_snapshot_ms']:
        return 'ALREADY_PROCESSED'
    if state['bar_cursor_ms'] < now//MINUTE*MINUTE-MINUTE:
        raise ValueError('Price monitor must catch up before source intake')
    if decoded['computed_ms'] > now or now-decoded['computed_ms'] > 5*MINUTE:
        raise ValueError('Source generation became stale before intake')
    if decoded['rows']:
        required_start = min(min(r['observed_ms'], r['price_ms'])//MINUTE*MINUTE for r in decoded['rows'])
        required_end = now//MINUTE*MINUTE
        if [r[0] for r in guard_bars] != list(range(required_start, required_end+MINUTE, MINUTE)):
            raise ValueError('Source guard must reach current intake minute without gaps')
        for t, o, h, l, c in guard_bars:
            o, h, l, c = map(number, (o, h, l, c))
            if h < max(o, l, c) or l > min(o, h, c):
                raise ValueError('Invalid source guard OHLC')
    state['last_snapshot_ms'] = decoded['computed_ms']
    state['last_cycle_id'] = decoded['cycle_id']
    state['last_snapshot_complete'] = decoded['complete']
    current = set(decoded['targets'])
    bootstrapping = not state['initialized_universe']
    if decoded['complete']:
        for key, ep in list(state['episodes'].items()):
            if key not in current:
                ep.update(present=False, absent_ms=now)
        state['initialized_universe'] = True
    for key in current:
        ep = state['episodes'].get(key)
        if ep is None or not ep['present']:
            # An unseen number following a COMPLETE baseline is newly observed.
            # Partial cold-start generations never establish absence.
            eligible_episode = not bootstrapping and state['initialized_universe']
            state['episodes'][key] = {'present': True, 'generation': (ep or {}).get('generation', 0)+1,
                                     'first_ms': now, 'consumed': not eligible_episode,
                                     'bootstrap_unverified': not eligible_episode,
                                     'touched_ms': None, 'directions': []}
        else:
            ep['present'] = True
        ep = state['episodes'][key]
        if ep['touched_ms'] is not None and now-ep['touched_ms'] >= 60*MINUTE:
            ep['suspicious_unchanged_1h'] = True
    # Conflicting valid directions in the initial source generation are not
    # a verifiable episode. Freeze a single provider-correct initial direction.
    initial_signs = {}
    for row in decoded['rows']:
        if row['provider_valid']:
            initial_signs.setdefault(row['key'], set()).add(1 if row['source_side'] == 'SHORT' else -1)
    for key in current:
        ep = state['episodes'][key]
        if not ep['directions']:
            signs = initial_signs.get(key, set())
            if len(signs) == 1:
                ep['directions'] = sorted(signs)
            else:
                ep.update(consumed=True, consume_reason='UNVERIFIED_INITIAL_SIDE')
    # Guard ALL source windows before accepting ANY representative. Otherwise
    # a later timeframe carrying an older clock could invalidate an earlier
    # representative only after its pending plan had already been created.
    for row in decoded['rows']:
        key = row['key']
        ep = state['episodes'][key]
        if ep['consumed']:
            continue
        if not ep['directions']:
            ep.update(consumed=True, consume_reason='UNVERIFIED_INITIAL_SIDE')
            continue
        a = ep['directions'][0]
        start = min(row['observed_ms'], row['price_ms'])//MINUTE*MINUTE
        touch_minutes = [t for t, o, h, l, c in guard_bars
                         if t >= start and (h >= row['target_price'] if a == 1 else l <= row['target_price'])]
        if touch_minutes:
            ep.update(consumed=True, touched_ms=now, consume_reason='SOURCE_WINDOW_TOUCH')
            count(state, 'SOURCE_WINDOW_TOUCH')
            for p in list(state['active']):
                if p['episode_key'] == key and p['episode_generation'] == ep['generation'] and p['status'] == 'PENDING':
                    if min(touch_minutes) < p['arm_ms']:
                        _finish(state, p, 'TARGET_BEFORE_ARM', now)
                    # Otherwise keep pending capacity until this live minute
                    # closes. Closed-bar advance alone decides entry ordering;
                    # partial extrema must not create a permanent UNKNOWN.
    for row in decoded['rows']:
        key = row['key']
        ep = state['episodes'][key]
        if ep['consumed'] or not row['eligible'] or row['levels']['direction'] != ep['directions'][0]:
            continue
        if any(near(p['target_price'], row['target_price']) for p in state['active']):
            count(state, 'NEAR_TARGET_BLOCKED')
            continue
        if len(state['active']) >= MAX_ACTIVE:
            count(state, 'CAPACITY_FAIL_CLOSED')
            continue
        arm = (now//MINUTE+1)*MINUTE
        identity = hashlib.sha256(f"{decoded['cycle_id']}|{key}|{ep['generation']}|{row['timeframe']}".encode()).hexdigest()
        p = {**row['levels'], 'position_id': identity, 'episode_key': key, 'episode_generation': ep['generation'],
             'status': 'PENDING', 'decision_ms': now, 'arm_ms': arm, 'expires_ms': arm+24*60*MINUTE,
             'score': row['score'], 'timeframe': row['timeframe'], 'source_side': row['source_side'],
             'source_observed_ms': row['observed_ms'], 'cycle_id': decoded['cycle_id']}
        state['active'].append(p)
        count(state, 'PENDING_CREATED')
    # Forget only absent episodes with no active position. Complete absence is
    # sufficient to renew them; retained active positions still block prices.
    livekeys = {p['episode_key'] for p in state['active']}
    state['episodes'] = {k: ep for k, ep in state['episodes'].items() if ep['present'] or k in livekeys}
    if len(state['episodes']) > MAX_EPISODES:
        raise ValueError('Episode capacity exceeded')
    return 'BOOTSTRAP_UNVERIFIED' if bootstrapping else 'SNAPSHOT_ACCEPTED'


def advance(state, bars, now):
    """Closed OHLC only; ambiguous order never becomes a fabricated fill/win."""
    for raw in bars:
        if len(raw) != 5:
            raise ValueError('Malformed OHLC')
        t, o, h, l, c = raw
        t = int(t)
        o, h, l, c = map(number, (o, h, l, c))
        if t % MINUTE or t+MINUTE > now or h < max(o, l, c) or l > min(o, h, c):
            raise ValueError('Invalid or unfinished minute')
        if t <= state['bar_cursor_ms']:
            continue
        if t != state['bar_cursor_ms']+MINUTE:
            raise ValueError('Noncontiguous price evidence')
        for key, ep in state['episodes'].items():
            if any(h >= float(key) if a == 1 else l <= float(key) for a in ep['directions']):
                ep.update(consumed=True, touched_ms=ep.get('touched_ms') or t+MINUTE, consume_reason='TARGET_TOUCHED')
        for p in list(state['active']):
            if p['status'] == 'UNKNOWN':
                continue
            a, e, sl, tp = p['direction'], p['entry_price'], p['stop_price'], p['take_price']
            touched_tp = h >= tp if a == 1 else l <= tp
            touched_sl = l <= sl if a == 1 else h >= sl
            at_open_entry = o <= e if a == 1 else o >= e
            if p['status'] == 'PENDING':
                if t >= p['expires_ms']:
                    _finish(state, p, 'EXPIRED_PENDING', t)
                    continue
                if t < p['arm_ms']:
                    if touched_tp:
                        _finish(state, p, 'TARGET_BEFORE_ARM', t)
                    continue
                hit_entry = l <= e if a == 1 else h >= e
                if touched_tp and (not hit_entry or not at_open_entry):
                    if not hit_entry:
                        _finish(state, p, 'TARGET_BEFORE_ENTRY', t)
                    else:
                        p.update(status='UNKNOWN', unknown_reason='ENTRY_TARGET_ORDER_UNKNOWN', unknown_ms=t)
                        count(state, 'ENTRY_TARGET_ORDER_UNKNOWN')
                    continue
                if not hit_entry:
                    continue
                fill = min(o, e) if a == 1 else max(o, e)
                if a*(fill-sl) <= 0:
                    _finish(state, p, 'GAP_THROUGH_STOP_NO_VALID_FILL', t)
                    continue
                p.update(status='OPEN', fill_price=fill, fill_ms=t)
                ep = state['episodes'].get(p['episode_key'])
                if ep and ep['generation'] == p['episode_generation']:
                    ep.update(consumed=True, consume_reason='FILLED')
                count(state, 'FILLED')
                # Only a recent still-open fill may be sent; no outage replay.
                if now < t+MINUTE+90_000:
                    state['intents'].append({'intent_id': p['position_id'], 'position_id': p['position_id'],
                                             'status': 'PENDING', 'expires_ms': t+MINUTE+90_000,
                                             'payload': deepcopy(p)})
            if p['status'] == 'OPEN':
                open_stop = o <= sl if a == 1 else o >= sl
                open_tp = o >= tp if a == 1 else o <= tp
                if touched_tp and touched_sl and not (open_stop or open_tp):
                    p['status'] = 'UNKNOWN'
                    count(state, 'BARRIER_ORDER_UNKNOWN')
                    for intent in state['intents']:
                        if intent['position_id'] == p['position_id'] and intent['status'] == 'PENDING':
                            intent['status'] = 'CANCELLED_AMBIGUOUS'
                elif open_stop or (touched_sl and not open_tp):
                    _finish(state, p, 'STOP_TOUCHED', t)
                elif touched_tp:
                    _finish(state, p, 'TAKE_TOUCHED', t)
        state['bar_cursor_ms'] = t
    return 'MONITORED'
