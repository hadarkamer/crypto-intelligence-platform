"""Proved Testnet manual-exit adoption. Public reads + audited CAS, no sends.

Explicit adoption accepts an operator's exact card/order identities. Automatic
adoption additionally requires one uniquely owned active card and one exact
reducing terminal order. A flat position alone never proves a manual exit.
The original alert, stop and target stay immutable. Manual exits have their own
observation leg and cannot count as a successful stop experiment.
"""
from copy import deepcopy
from decimal import Decimal
from . import card_lifecycle as life, card_sync_evidence as sync
from .filled_dispatch_store import DispatchError, SCHEMA


def candidate(state, card_id, order_id, *, now_ms, allow_owned_leftovers=False):
    life.ident(card_id, r'[0-9a-f]{64}')
    life.ident(order_id, r'[0-9]{1,30}')
    ev = state.get('evidence')
    incident = state.get('emergency') or {}
    if (ev is None or ev['bindings'] != state['bindings'] or state.get('pending')
            or incident.get('pending_close') or incident.get('pending_cancel')):
        raise DispatchError('MANUAL_EXIT_UNCERTAINTY_REQUIRES_REVIEW')
    snap = ev['snapshot']
    life.validate_snapshot(snap)
    if (snap['account'] != state['account'] or snap['symbol'] != state['symbol']
            or not snap['history_complete'] or not snap['orders_complete']
            or not 0 <= now_ms-snap['at_ms'] <= 15000
            or life.number(snap['position_quantity'], signed=True) != 0
            or (snap['open_orders'] and not allow_owned_leftovers)):
        raise DispatchError('MANUAL_EXIT_COMPLETE_FRESH_FLAT_REQUIRED')
    bindings = deepcopy(state['bindings'])
    own = life.validate_bindings(bindings)
    if (state['account'], order_id) in own:
        raise DispatchError('MANUAL_EXIT_ORDER_ALREADY_BOUND')
    report = life.review(bindings, snap, now_ms=now_ms)
    if set(report['bucket_issues']) != {'POSITION_DOES_NOT_MATCH_CARDS', 'UNASSIGNED_EXCHANGE_ACTIVITY'}:
        raise DispatchError('MANUAL_EXIT_UNRELATED_ISSUES')
    remaining = [row for row in report['cards']
                 if life.number(row['remaining_quantity'], signed=True) != 0]
    if len(remaining) != 1 or remaining[0]['card_id'] != card_id:
        raise DispatchError('MANUAL_EXIT_EXCLUSIVE_CARD_REQUIRED')
    if any(set(row['issues']) - {'STOP_COVERAGE_MISSING', 'TAKE_PROFIT_COVERAGE_MISSING'}
           for row in report['cards']):
        raise DispatchError('MANUAL_EXIT_UNRELATED_CARD_ISSUES')
    binding = next(b for b in bindings if b['card_id'] == card_id)
    # Duplicate normalized fill rows must not manufacture a different closing
    # quantity. Lifecycle review applies this same exact fill identity rule.
    unique = list({f['fill_id']:f for f in snap['fills']}.values())
    fills = [f for f in unique if f['oid'] == order_id]
    unknown = {f['oid'] for f in unique if (state['account'], f['oid']) not in own}
    unknown.update(t['oid'] for t in snap['terminal_orders']
                   if (state['account'], t['oid']) not in own)
    if unknown != {order_id} or not fills:
        raise DispatchError('MANUAL_EXIT_EXACT_UNKNOWN_ORDER_REQUIRED')
    if allow_owned_leftovers:
        # An outstanding ENTRY could reopen the exposure after this proof.
        # Only this exact card's already-verified reducing exits may remain.
        exits = set(binding['orders']['STOP'] + binding['orders']['TAKE_PROFIT'])
        if any(o['oid'] not in exits or o['state'] != 'ACTIVE'
               or o['reduce_only'] is not True for o in snap['open_orders']):
            raise DispatchError('MANUAL_EXIT_ONLY_OWNED_EXIT_LEFTOVERS_REQUIRED')
    side = 'A' if binding['side'] == 'LONG' else 'B'
    entries = [f for f in unique if f['oid'] in binding['orders']['ENTRY']]
    preceding = [f for f in unique if f['oid'] in
                 binding['orders']['ENTRY'] + binding['orders']['STOP'] + binding['orders']['TAKE_PROFIT']]
    if (not entries or any(f['side'] != side or f['at_ms'] < max(e['at_ms'] for e in entries)
                          for f in fills)
            or (allow_owned_leftovers and any(f['at_ms'] < max(e['at_ms'] for e in preceding)
                                              for f in fills))
            or sum((life.number(f['quantity']) for f in fills), Decimal(0))
               != life.number(remaining[0]['remaining_quantity'], positive=True)):
        raise DispatchError('MANUAL_EXIT_EXACT_CLOSING_QUANTITY_REQUIRED')
    binding['orders'].setdefault('MANUAL_EXIT', []).append(order_id)
    life.validate_bindings(bindings)
    proposed_snapshot = deepcopy(snap)
    # An unowned terminal row has never proved reduceOnly/side/size against
    # this binding. It must receive an independent orderStatus read in BOTH
    # passes, rather than becoming a reusable certificate by being relabeled.
    proposed_snapshot['terminal_orders'] = [t for t in proposed_snapshot['terminal_orders']
                                            if t['oid'] != order_id]
    return dict(bindings=bindings, snapshot=proposed_snapshot)


def _verified(observed, card_id, order_id, *, allow_owned_leftovers):
    """Manual execution is final even while its bot-owned siblings need cleanup."""
    snap, report = observed['snapshot'], observed['report']
    allowed = {'FLAT_WITH_WORKING_ORDERS'} if allow_owned_leftovers else set()
    if (life.number(snap['position_quantity'], signed=True) != 0
            or report['bucket_issues']
            or any(life.number(row['remaining_quantity'], signed=True) != 0
                   or set(row['issues']) - allowed for row in report['cards'])
            or (snap['open_orders'] and not allow_owned_leftovers)):
        raise DispatchError('MANUAL_EXIT_PUBLIC_CLOSURE_NOT_VERIFIED')
    binding = next(b for b in observed['bindings'] if b['card_id'] == card_id)
    exits = set(binding['orders']['STOP'] + binding['orders']['TAKE_PROFIT'])
    if any(o['oid'] not in exits or o['reduce_only'] is not True or o['state'] != 'ACTIVE'
           for o in snap['open_orders']):
        raise DispatchError('MANUAL_EXIT_ONLY_OWNED_EXIT_LEFTOVERS_REQUIRED')
    terminal = next((t for t in snap['terminal_orders'] if t['oid'] == order_id), None)
    if terminal is None or terminal['state'] != 'FILLED':
        raise DispatchError('MANUAL_EXIT_FILLED_TERMINAL_REQUIRED')
    if not snap['open_orders'] and any(not row['closure_verified'] for row in report['cards']):
        raise DispatchError('MANUAL_EXIT_PUBLIC_CLOSURE_NOT_VERIFIED')
    return not snap['open_orders']


def _closed_incident(current, audit, snapshot):
    if current.get('emergency'):
        # Retain the existing circuit latch. Only its separate independently
        # verified release protocol can authorize new entries again.
        current['emergency'].update(phase='CLOSED_VERIFIED', closure_origin='MANUAL',
            closed_at_ms=snapshot['at_ms'], closure_proof_digest=audit['proof_digest'])


def adopt(store, reader, bucket, card_id, order_id, *, clock=sync.now_ms):
    """Two independent agreeing public passes precede one guarded journal write."""
    before = store.load(bucket)
    proposed = candidate(before, card_id, order_id, now_ms=clock())
    # observe() requires the exact order identity, opposite side, reduceOnly,
    # complete filled size and terminal status in BOTH independent passes.
    observed = sync.collect(proposed, reader, clock=clock, reuse_verified_terminals=True)
    snap = observed['snapshot']
    _verified(observed, card_id, order_id, allow_owned_leftovers=False)
    audit = dict(card_id=card_id, order_id=order_id, origin='MANUAL',
                 observed_at_ms=snap['at_ms'], proof_digest=life.digest(observed),
                 entry_or_exit_order_sent=False)
    committed_at = clock()
    if not 0 <= committed_at-snap['at_ms'] <= 5000:
        raise DispatchError('MANUAL_EXIT_COMMIT_EVIDENCE_EXPIRED')
    def commit(conn, current):
        if current != before:
            raise DispatchError('MANUAL_EXIT_BUCKET_CHANGED')
        if conn.execute(f'SELECT 1 FROM {SCHEMA}.ownership WHERE account=%s AND oid=%s',
                        (current['account'], order_id)).fetchone():
            raise DispatchError('MANUAL_EXIT_ORDER_ALREADY_OWNED')
        if not 0 <= clock()-snap['at_ms'] <= 5000:
            raise DispatchError('MANUAL_EXIT_COMMIT_EVIDENCE_EXPIRED')
        current['bindings'] = observed['bindings']
        current['evidence'] = dict(bindings=deepcopy(observed['bindings']), snapshot=snap)
        current.setdefault('manual_exit_audit', []).append(audit)
        _closed_incident(current, audit, snap)
        return None
    return store.change(bucket, before['revision'], 'MANUAL_EXIT_PUBLICLY_RECONCILED',
                        committed_at, commit)


def automatic_candidate(state, *, now_ms):
    """Discover identity only from complete exact history of one active owner.

    Ambiguous/manual partial closes remain discrepancies; they cannot become
    fabricated per-card allocations. An old closed card may share this history,
    but it must have no working orders or unresolved quantity facts.
    """
    ev = state.get('evidence')
    if not ev or not state.get('bindings'):
        return None
    snap = ev['snapshot']
    life.validate_snapshot(snap)
    if life.number(snap['position_quantity'], signed=True) != 0:
        return None
    own = life.validate_bindings(state['bindings'])
    unknown = {f['oid'] for f in snap['fills'] if (state['account'], f['oid']) not in own}
    if not unknown:
        return None
    if len(unknown) != 1:
        raise DispatchError('MANUAL_EXIT_EXACT_UNKNOWN_ORDER_REQUIRED')
    report = life.review(state['bindings'], snap, now_ms=now_ms)
    active = [row for row in report['cards']
              if life.number(row['remaining_quantity'], signed=True) != 0]
    if len(active) != 1:
        raise DispatchError('MANUAL_EXIT_EXCLUSIVE_CARD_REQUIRED')
    card_id, order_id = active[0]['card_id'], next(iter(unknown))
    for owner in state['bindings']:
        original = state.get('originals', {}).get(owner['card_id'])
        if not isinstance(original, dict) or 'card' not in original:
            raise DispatchError('MANUAL_EXIT_IMMUTABLE_ORIGINAL_REQUIRED')
        expected = life.binding_from_card(original['card'], state['account'],
            {owner['role']: dict(account=state['account'])}, owner['orders'])
        if owner != expected:
            raise DispatchError('MANUAL_EXIT_IMMUTABLE_OWNER_CHANGED')
    incident = state.get('emergency') or {}
    if incident and incident.get('card_id') != card_id:
        raise DispatchError('MANUAL_EXIT_EMERGENCY_OWNER_CHANGED')
    proposed = candidate(state, card_id, order_id, now_ms=now_ms, allow_owned_leftovers=True)
    return card_id, order_id, proposed


def _commit_automatic(store, before, observed, card_id, order_id, *, clock, previous_audit=None):
    closed = _verified(observed, card_id, order_id, allow_owned_leftovers=True)
    snap = observed['snapshot']
    audit = dict(card_id=card_id, order_id=order_id, origin='MANUAL', automatic=True,
        phase='CLOSED_VERIFIED' if closed else 'OWNED_ORDER_CLEANUP_PENDING',
        observed_at_ms=snap['at_ms'], proof_digest=life.digest(observed),
        entry_or_exit_order_sent=False)
    if previous_audit is not None:
        audit['initial_proof_digest'] = previous_audit['proof_digest']
    if ((before.get('emergency') or {}).get('provisional') is True
            or (previous_audit or {}).get('provisional_emergency_confirmed_by_manual_proof') is True):
        audit['provisional_emergency_confirmed_by_manual_proof'] = True
    committed_at = clock()
    if not 0 <= committed_at - snap['at_ms'] <= 5000:
        raise DispatchError('MANUAL_EXIT_COMMIT_EVIDENCE_EXPIRED')
    def commit(conn, current):
        if current != before:
            raise DispatchError('MANUAL_EXIT_BUCKET_CHANGED')
        if current.get('pending') or (current.get('emergency') or {}).get('pending_close') \
                or (current.get('emergency') or {}).get('pending_cancel'):
            raise DispatchError('MANUAL_EXIT_UNCERTAINTY_REQUIRES_REVIEW')
        if previous_audit is None and conn.execute(
                f'SELECT 1 FROM {SCHEMA}.ownership WHERE account=%s AND oid=%s',
                (current['account'], order_id)).fetchone():
            raise DispatchError('MANUAL_EXIT_ORDER_ALREADY_OWNED')
        if not 0 <= clock() - snap['at_ms'] <= 5000:
            raise DispatchError('MANUAL_EXIT_COMMIT_EVIDENCE_EXPIRED')
        current['bindings'] = observed['bindings']
        current['evidence'] = dict(bindings=deepcopy(observed['bindings']), snapshot=snap)
        audits = current.setdefault('manual_exit_audit', [])
        if previous_audit is None:
            audits.append(audit)
        else:
            index = audits.index(previous_audit)
            audits[index] = audit
        incident = current.get('emergency')
        if incident is not None and incident.get('provisional') is True:
            if incident.get('phase') != 'ACTIVE' or incident.get('requests'):
                raise DispatchError('MANUAL_EXIT_PROVISIONAL_INCIDENT_CHANGED')
            # This exact two-pass full-close proof confirms the provisional
            # incident while RETAINING its entry circuit latch. The emergency
            # executor now sees remainder zero and can only cancel the already
            # owned leftovers; it cannot send a new entry or protective close.
            incident['provisional'] = False
        if closed:
            _closed_incident(current, audit, snap)
        return None
    event = ('MANUAL_EXIT_AUTOMATIC_CLOSURE_VERIFIED' if closed
             else 'MANUAL_EXIT_AUTOMATIC_BOUND_OWNED_CLEANUP_PENDING')
    saved = store.change(before['bucket'], before['revision'], event, committed_at, commit)
    return dict(status='MANUAL_EXIT_CLOSURE_VERIFIED' if closed
                else 'MANUAL_EXIT_BOUND_AWAITING_OWNED_CLEANUP',
                card_id=card_id, order_id=order_id, state=saved, order_requests_sent=0)


def reconcile_automatic(store, bucket, *, collect, clock=sync.now_ms):
    """No sends: prove/bind a full manual close, then let owned cleanup run.

    collect MUST be the ordinary two-pass Testnet checkpoint collector. The
    caller continues the existing fenced dispatcher (or emergency lane) for
    each owned cancellation. A later call verifies finality after those orders
    have all become terminal. No new orders or circuit release occur here.
    """
    before = store.load(bucket)
    pending = [a for a in before.get('manual_exit_audit', [])
               if a.get('automatic') is True and a.get('phase') == 'OWNED_ORDER_CLEANUP_PENDING']
    if len(pending) > 1:
        raise DispatchError('MANUAL_EXIT_EXCLUSIVE_CARD_REQUIRED')
    if pending:
        audit = pending[0]
        ev = before.get('evidence')
        incident = before.get('emergency') or {}
        if (not ev or ev['bindings'] != before['bindings'] or before.get('pending')
                or incident.get('pending_close') or incident.get('pending_cancel')):
            raise DispatchError('MANUAL_EXIT_UNCERTAINTY_REQUIRES_REVIEW')
        # No second read flight while a saved current owned order still needs
        # cancellation. Existing cleanup preserves its normal freshness fences.
        observed = dict(bindings=before['bindings'], snapshot=ev['snapshot'],
            report=life.review(before['bindings'], ev['snapshot'], now_ms=clock()))
        closed = _verified(observed, audit['card_id'], audit['order_id'], allow_owned_leftovers=True)
        if not closed:
            return dict(status='MANUAL_EXIT_BOUND_AWAITING_OWNED_CLEANUP',
                card_id=audit['card_id'], order_id=audit['order_id'], state=before, order_requests_sent=0)
        observed = collect(deepcopy(ev))
        return _commit_automatic(store, before, observed, audit['card_id'], audit['order_id'],
                                 clock=clock, previous_audit=audit)
    proposed = automatic_candidate(before, now_ms=clock())
    if proposed is None:
        return dict(status='NO_MANUAL_EXIT_CANDIDATE', order_requests_sent=0)
    card_id, order_id, ev = proposed
    observed = collect(ev)
    return _commit_automatic(store, before, observed, card_id, order_id, clock=clock)


def reconcile_controller(controller, bucket):
    """Use the existing venue observer and its reserved protection read budget."""
    checkpoint = getattr(type(controller.venue), 'collect_checkpoint', None)
    collect = (lambda ev: controller.venue.collect_checkpoint(
        ev, pending=None, emergency_active=True)) if callable(checkpoint) else controller.venue.collect
    return reconcile_automatic(controller.store, bucket, collect=collect, clock=controller.venue.now)
