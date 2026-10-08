"""Frozen DOGE half-adverse/75%-partial observation; never exchange execution."""
from copy import deepcopy
import sol_proximity_experimental_signal as base

MINUTE = base.MINUTE
CONFIG_VERSION = 'doge-half-adverse-part75-h24-v1'
MAX_ACTIVE = 1

def __getattr__(name):
    return getattr(base, name)

def decode_bundle(bundle, now, spec, range_bars=None):
    decoded = base.decode_bundle(bundle, now, spec, range_bars)
    for row in decoded['rows']:
        if row['eligible']:
            p = row['levels']
            p['stop_price'] = base.number(2*p['source_price']-p['target_price'])
    return decoded

def ingest(state, decoded, now, guard_bars):
    decoded = deepcopy(decoded)
    if state['active'] or state.get('legacy_source_state', {}).get('active'):
        for row in decoded['rows']:
            row['eligible'] = False
        base.count(state, 'CAP1_RESERVED')
    before = {p['position_id'] for p in state['active']}
    result = base.ingest(state, decoded, now, guard_bars)
    # The source reducer validates every target/clock before accepting sorted
    # representatives. Retain the first accepted reservation only. Discarded
    # unfilled reservations never consume an episode or create an intent.
    added = [p for p in state['active'] if p['position_id'] not in before]
    for p in added[1:]:
        state['active'].remove(p)
        state['counts']['PENDING_CREATED'] -= 1
        base.count(state, 'CAP1_RESERVED')
    for p in added[:1]:
        p.update(partial_fraction=.75, partial_take_price=p['entry_price']+.75*(p['take_price']-p['entry_price']),
                 holding_limit_ms=24*60*MINUTE, remaining_fraction=1., realized_price_move=0.,
                 notification_only=True, stop_source_distance_multiple=1.)
    state['notification_only'] = True
    return result

def _unknown(state, p, reason, t):
    p.update(status='UNKNOWN', unknown_reason=reason, unknown_ms=t)
    base.count(state, reason)
    for intent in state['intents']:
        if intent['position_id'] == p['position_id'] and intent['status'] == 'PENDING':
            intent['status'] = 'CANCELLED_AMBIGUOUS'

def _partial(state, p, price, t):
    p.update(partial_taken=True, partial_ms=t, partial_fill_price=price, remaining_fraction=.25)
    p['realized_price_move'] += .75*p['direction']*(price-p['fill_price'])
    base.count(state, 'PARTIAL_TAKE_75')

def _close(state, p, status, price, t):
    p['realized_price_move'] += p['remaining_fraction']*p['direction']*(price-p['fill_price'])
    p.update(exit_price=price, remaining_fraction=0.)
    base._finish(state, p, status, t)

def advance(state, bars, now):
    for raw in bars:
        if len(raw) != 5:
            raise ValueError('Malformed OHLC')
        t, o, h, l, c = raw
        t = int(t)
        o, h, l, c = map(base.number, (o, h, l, c))
        if t % MINUTE or t+MINUTE > now or h < max(o,l,c) or l > min(o,h,c):
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
            a,e,sl,tp = p['direction'],p['entry_price'],p['stop_price'],p['take_price']
            stop_hit = l <= sl if a == 1 else h >= sl
            target_hit = h >= p['target_price'] if a == 1 else l <= p['target_price']
            at_entry_open = a*(o-e) <= 0
            just_filled = False
            if p['status'] == 'PENDING':
                if t >= p['expires_ms']:
                    base._finish(state,p,'EXPIRED_PENDING',t); continue
                if t < p['arm_ms']:
                    if target_hit:base._finish(state,p,'TARGET_BEFORE_ARM',t)
                    continue
                entry_hit = l <= e if a == 1 else h >= e
                if target_hit and (not entry_hit or not at_entry_open):
                    if entry_hit:_unknown(state,p,'ENTRY_TARGET_ORDER_UNKNOWN',t)
                    else:base._finish(state,p,'TARGET_BEFORE_ENTRY',t)
                    continue
                if not entry_hit:continue
                fill = min(o,e) if a == 1 else max(o,e)
                if a*(fill-sl) <= 0:
                    base._finish(state,p,'GAP_THROUGH_STOP_NO_VALID_FILL',t);continue
                p.update(status='OPEN',fill_price=fill,fill_ms=t,hold_until_ms=t+24*60*MINUTE,
                         partial_take_price=fill+.75*(tp-fill))
                ep=state['episodes'].get(p['episode_key'])
                if ep and ep['generation']==p['episode_generation']:
                    ep.update(consumed=True,consume_reason='FILLED')
                    ep.setdefault('filled_legs',[]).append({k:p[k]for k in ('target_price','timeframe','direction')})
                base.count(state,'FILLED');just_filled=True
            if p['status'] != 'OPEN':continue
            if not just_filled and t >= p['hold_until_ms']:
                _close(state,p,'TIME_EXIT_24H',o,t);continue
            part=p['partial_take_price'];part_hit=h >= part if a==1 else l <= part
            final_hit=h >= tp if a==1 else l <= tp
            open_stop=a*(o-sl)<=0
            open_part=a*(o-part)>=0
            open_final=a*(o-tp)>=0
            if just_filled and not at_entry_open and part_hit and a*(c-part)<0:
                _unknown(state,p,'ENTRY_PARTIAL_TAKE_ORDER_UNKNOWN',t);continue
            if open_stop:
                _close(state,p,'STOP_TOUCHED',o,t);continue
            if not p.get('partial_taken'):
                if part_hit and stop_hit and not open_part:
                    _unknown(state,p,'PARTIAL_STOP_ORDER_UNKNOWN',t);continue
                if part_hit:
                    _partial(state,p,o if open_part and not just_filled else part,t)
            if final_hit and stop_hit and not open_final:
                # An opening partial is known, but the remaining TP/SL order is not.
                _unknown(state,p,'BARRIER_ORDER_UNKNOWN',t);continue
            if open_final or final_hit:
                _close(state,p,'TAKE_TOUCHED',o if open_final and not just_filled else tp,t)
            elif stop_hit:
                _close(state,p,'STOP_TOUCHED',sl,t)
            elif just_filled and now < t+MINUTE+90_000 and not p.get('partial_taken'):
                state['intents'].append(dict(intent_id=p['position_id'],position_id=p['position_id'],
                    status='PENDING',expires_ms=t+MINUTE+90_000,payload=deepcopy(p)))
        state['bar_cursor_ms']=t
    return 'MONITORED'
