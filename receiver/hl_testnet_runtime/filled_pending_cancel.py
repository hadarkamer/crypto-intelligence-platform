"""Approved half-threshold rule for NEW independent entries; no transport.

Reuse the legacy rule's exact percentage math and decision function. Input is
verified bot-owned card/order evidence, never the display app. A crossing is
latched in the existing durable bucket, but ANY confirmed fill takes priority.
No scheduler, DDL, signature, new release flag or time-based cancel is added.
"""
from copy import deepcopy
from . import card_lifecycle as life, filled_quantity_exits as selected
from . import half_threshold_cancel as rule_engine
from .filled_dispatch_store import DispatchError

POLICY = rule_engine.POLICY
OPERATION = 'CANCEL_UNFILLED_HALF_THRESHOLD'
FIELD = 'half_threshold_observations'
U21_OWNER_POLICY = dict(version='u21-owner-cancel-threshold-v1',
                        threshold_pct='0.5', cancel_move_pct='0.25')


def threshold_for(original):
    """Keep a source formula threshold distinct from the U21 owner's policy."""
    card = original['card']
    source_threshold = card['rule']['threshold_pct']
    if source_threshold is not None:
        if 'cancel_policy' in original:
            raise DispatchError('UNEXPECTED_CANCEL_POLICY_OVERRIDE')
        return source_threshold
    if (card['rule']['id'] != 'U21_XRP_SHORT'
            or original.get('cancel_policy') != U21_OWNER_POLICY):
        raise DispatchError('SOURCE_CANCEL_POLICY_REQUIRES_OWNER_DECISION')
    return U21_OWNER_POLICY['threshold_pct']


def _rule(original, draft):
    card = original['card']; source = card['prepared']['source']
    entry = draft['entry_action']['orders'][0]
    # The newer card ID namespace permits characters the legacy rule name does
    # not. Use a deterministic INTERNAL alias; keep source rule identity in the
    # immutable card checksum. Never rename or rewrite the source formula.
    return rule_engine.make_rule(rule_id='CARD_'+card['card_id'][:32].upper(),
        event_id=card['event_id'], symbol=draft['symbol'], side=draft['side'],
        entry=entry['p'], size=entry['s'], entry_cloid=entry['c'],
        threshold_pct=threshold_for(original),
        source_digest=life.digest(source), execution_digest=life.digest(card))


def scan(state, routes, sample, *, now_ms):
    """Return exact candidates and newly observed crossings, without mutation.

    Price must move in the forecast direction, relative to the submitted limit.
    A latched crossing is not a fill waiver or a stale-evidence waiver. The
    account position is reconciled globally, while fill exclusion is per card.
    """
    life.moment(now_ms); life.shape(sample, 'mark_price at_ms')
    life.moment(sample['at_ms']); life.number(sample['mark_price'], positive=True)
    bs = state['bindings']; snap = state['evidence']['snapshot']
    account, symbol = life.validate_snapshot(snap)
    if account != state['account'] or symbol != state['symbol']:
        raise DispatchError('HALF_THRESHOLD_BUCKET_MISMATCH')
    stored = state.get(FIELD, {})
    if not isinstance(stored, dict) or not set(stored) <= set(state['originals']):
        raise DispatchError('HALF_THRESHOLD_OBSERVATION_INVALID')
    result = dict(candidates=[], new_observations={}, no_longer_unfilled={})
    if not bs:
        return result  # No placed entry exists yet; never invent a cancellation.
    view = life.review(bs, snap, now_ms=now_ms)
    if view['bucket_issues']:
        raise DispatchError('HALF_THRESHOLD_EVIDENCE_REQUIRES_REVIEW')
    age_ms = now_ms-min(snap['at_ms'], sample['at_ms'])
    comparable = (0 <= now_ms-sample['at_ms'] <= 10000 and 0 <= age_ms <= 10000)
    opens = {o['oid']:o for o in snap['open_orders']}
    terminals = {o['oid']:o for o in snap['terminal_orders']}
    for b in sorted(bs, key=lambda b:b['card_id']):
        cid = b['card_id']; original = state['originals'][cid]
        draft = selected.validate_draft(original['card'], original['draft'], routes)
        expected = life.binding_from_card(original['card'], account, routes, b['orders'])
        if b != expected or len(b['orders']['ENTRY']) != 1:
            raise DispatchError('HALF_THRESHOLD_ORIGINAL_BINDING_MISMATCH')
        oid = b['orders']['ENTRY'][0]
        life.ident(oid, r'[1-9][0-9]{0,19}')
        if int(oid) >= 2**64:
            raise DispatchError('HALF_THRESHOLD_ORDER_ID_INVALID')
        rule = _rule(original, draft); rule_hash = life.digest(rule)
        previous = stored.get(cid)
        if previous is not None:
            life.shape(previous, 'card_digest oid rule_digest snapshot_at_ms sample_at_ms mark_price')
            old_snap = life.moment(previous['snapshot_at_ms'])
            old_sample = life.moment(previous['sample_at_ms'])
            old_mark = life.number(previous['mark_price'], positive=True)
            boundary = life.number(rule['cancel_price'], positive=True)
            crossed = old_mark >= boundary if b['side']=='LONG' else old_mark <= boundary
            if (previous['card_digest'] != b['card_digest'] or previous['oid'] != oid
                    or previous['rule_digest'] != rule_hash or not crossed
                    or max(old_snap, old_sample) > now_ms or abs(old_snap-old_sample) > 10000):
                raise DispatchError('HALF_THRESHOLD_LATCH_NOT_BOUND_TO_CARD')
        own = next(v for v in view['cards'] if v['card_id']==cid)
        entered = life.number(own['entry_quantity'])
        if entered > 0 or oid in terminals:
            # This also retires an UNSENT cancel proposal if a fill raced with
            # preparation. A started/uncertain request is NEVER retired here.
            result['no_longer_unfilled'][cid] = oid
            continue
        if life.number(own['exit_quantity']) > 0 or own['issues']:
            continue
        order = opens.get(oid)
        if (not comparable or order is None or order['state']!='ACTIVE'
                or life.number(order['quantity']) != life.number(draft['planned_quantity'])
                or any(o in opens for leg in ('STOP','TAKE_PROFIT') for o in b['orders'][leg])):
            continue
        # Normalize the already-registered order for the existing pure rule.
        # Registry + lifecycle validation above are required before this adapter.
        raw = dict(status='order', order=dict(status='open', order=dict(
            cloid=rule['entry_cloid'], coin=b['symbol'], side=order['side'],
            reduceOnly=order['reduce_only'], isTrigger=False, orderType='Limit',
            limitPx=order['price'], origSz=rule['size'], sz=order['quantity'])))
        decision = rule_engine.evaluate(rule, raw, '0', sample['mark_price'],
            sample_age_seconds=age_ms/1000, threshold_seen=previous is not None)
        if not decision['cancel_candidate']:
            continue
        proof = previous
        if proof is None:
            proof = dict(card_digest=b['card_digest'], oid=oid, rule_digest=rule_hash,
                snapshot_at_ms=snap['at_ms'], sample_at_ms=sample['at_ms'],
                mark_price=sample['mark_price'])
            result['new_observations'][cid] = proof
        result['candidates'].append(dict(card_id=cid, oid=oid,
            rule_digest=rule_hash, first_crossing_at_ms=proof['sample_at_ms'],
            sample_at_ms=sample['at_ms']))
    return result


def checkpoint(store, state, routes, sample, *, now_ms):
    """Save only observations, plus safely retire an obsolete ZERO-attempt cancel.

    Uses the existing revision and shared transaction lock. No schema changes.
    Never unlocks an uncertain attempt on a price change, a timeout or a restart.
    """
    report = scan(state, routes, sample, now_ms=now_ms)
    pending = store.request(state['pending']) if state['pending'] else None
    retire = False
    if (pending and pending['phase']=='PREPARED'
            and pending['proposal']['operation']==OPERATION):
        p = pending['proposal']
        retire = report['no_longer_unfilled'].get(p['card_id']) == p['old_oid']
    if not report['new_observations'] and not retire:
        return state
    def update(conn, current):
        if report['new_observations']:
            current[FIELD] = {**current.get(FIELD, {}), **deepcopy(report['new_observations'])}
        if not retire:
            return None
        req = store.pending_record(conn, current)
        if (req is None or req['request_id']!=pending['request_id']
                or req['phase']!='PREPARED' or req['attempts']!=0
                or req['attempt_at_ms'] is not None or req['nonce'] is not None
                or req['reply'] is not None
                or current['evidence']['snapshot']['at_ms']<=req['proposal']['observed_at_ms']):
            raise DispatchError('HALF_THRESHOLD_STARTED_OR_UNPROVEN_CANCEL_CANNOT_ABORT')
        req.update(phase='ABORTED_UNSENT', abort_reason='ENTRY_NO_LONGER_UNFILLED',
                   aborted_at_ms=now_ms)
        current['pending'] = None
        return req
    return store.change(state['bucket'], state['revision'],
        'HALF_THRESHOLD_UNSENT_SUPERSEDED' if retire else 'HALF_THRESHOLD_OBSERVED', now_ms, update)


def final_freshness(request, *, now_ms):
    """Ten-second legacy sample bound checked again at the actual send boundary."""
    p = request['proposal']
    if p['operation'] != OPERATION:
        return
    at = life.moment(p.get('cancel_sample_at_ms'))
    if (at > request['attempt_at_ms'] or not 0 <= now_ms-at <= 10000
            or not 0 <= now_ms-p['observed_at_ms'] <= 10000):
        raise DispatchError('HALF_THRESHOLD_FINAL_EVIDENCE_EXPIRED')
