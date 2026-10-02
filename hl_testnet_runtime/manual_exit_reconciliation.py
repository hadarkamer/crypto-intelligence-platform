"""Explicit Testnet manual-exit adoption. Public reads + audited CAS, no sends.

An operator identifies the exact card and exchange order. Never infer ownership
from a flat position or allocate a manual exit between overlapping live cards.
The original alert, stop and target stay immutable. Manual exits have their own
observation leg and cannot count as a successful stop experiment.
"""
from copy import deepcopy
from decimal import Decimal
from . import card_lifecycle as life, card_sync_evidence as sync
from .filled_dispatch_store import DispatchError, SCHEMA


def candidate(state, card_id, order_id, *, now_ms):
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
            or snap['open_orders']):
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
    fills = [f for f in snap['fills'] if f['oid'] == order_id]
    unknown = {f['oid'] for f in snap['fills'] if (state['account'], f['oid']) not in own}
    if unknown != {order_id} or not fills:
        raise DispatchError('MANUAL_EXIT_EXACT_UNKNOWN_ORDER_REQUIRED')
    side = 'A' if binding['side'] == 'LONG' else 'B'
    entries = [f for f in snap['fills'] if f['oid'] in binding['orders']['ENTRY']]
    if (not entries or any(f['side'] != side or f['at_ms'] < max(e['at_ms'] for e in entries)
                          for f in fills)
            or sum((life.number(f['quantity']) for f in fills), Decimal(0))
               != life.number(remaining[0]['remaining_quantity'], positive=True)):
        raise DispatchError('MANUAL_EXIT_EXACT_CLOSING_QUANTITY_REQUIRED')
    binding['orders'].setdefault('MANUAL_EXIT', []).append(order_id)
    life.validate_bindings(bindings)
    return dict(bindings=bindings, snapshot=deepcopy(snap))


def adopt(store, reader, bucket, card_id, order_id, *, clock=sync.now_ms):
    """Two independent agreeing public passes precede one guarded journal write."""
    before = store.load(bucket)
    proposed = candidate(before, card_id, order_id, now_ms=clock())
    # observe() requires the exact order identity, opposite side, reduceOnly,
    # complete filled size and terminal status in BOTH independent passes.
    observed = sync.collect(proposed, reader, clock=clock, reuse_verified_terminals=True)
    snap = observed['snapshot']
    report = observed['report']
    if (snap['open_orders'] or life.number(snap['position_quantity'], signed=True) != 0
            or report['bucket_issues'] or any(not row['closure_verified'] for row in report['cards'])):
        raise DispatchError('MANUAL_EXIT_PUBLIC_CLOSURE_NOT_VERIFIED')
    terminal = next((t for t in snap['terminal_orders'] if t['oid'] == order_id), None)
    if terminal is None or terminal['state'] != 'FILLED':
        raise DispatchError('MANUAL_EXIT_FILLED_TERMINAL_REQUIRED')
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
        if current.get('emergency'):
            # Keep the incident latched until the existing explicit release
            # protocol independently proves account-wide closure again.
            current['emergency'].update(phase='CLOSED_VERIFIED', closure_origin='MANUAL',
                closed_at_ms=snap['at_ms'], closure_proof_digest=life.digest(observed))
        return None
    return store.change(bucket, before['revision'], 'MANUAL_EXIT_PUBLICLY_RECONCILED',
                        committed_at, commit)
