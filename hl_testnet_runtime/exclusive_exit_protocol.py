"""Software-only one-exit-at-a-time protocol, NOT a selected trading policy.

Use the existing DispatchStore, its account/market lock, request table and
continuity checks. Never connect this protocol to a signer or a user database.
A local caller selects the desired leg for a rehearsal; no app input, market
watcher, execution worker, emergency-close policy or live release is added.

The invariant is the SUM of all independently executable exit quantities for
one card <= that card's remaining allocation. This protocol narrows it to one
outstanding exit per card. Cancellation acknowledgements do not release capacity.
The invariant assumes complete venue evidence, enforced quantities, immutable
order ownership and no outside writer trading the same position.
"""
from copy import deepcopy
from decimal import Decimal, localcontext

from . import card_lifecycle as life, residual_exit_fence as fence
from .card_lifecycle_store import _continues
from .filled_dispatch_store import DispatchError
from . import card_sync_evidence as venue_evidence

VERSION = 'exclusive-exit-protocol-software-v1'
BENIGN = {'STOP_COVERAGE_MISSING', 'TAKE_PROFIT_COVERAGE_MISSING',
          'BOTH_EXIT_LEGS_FILLED_REVIEW'}


def _read(state, now_ms):
    if not isinstance(state, dict) or state.get('domain') != 'software':
        raise DispatchError('EXCLUSIVE_PROTOCOL_SOFTWARE_ONLY')
    bs, snap, view = fence._read(state, now_ms)
    if view['bucket_issues'] or any(set(c['issues']) - BENIGN for c in view['cards']):
        raise DispatchError('EXCLUSIVE_EVIDENCE_REQUIRES_REVIEW')
    rows = {c['card_id']: c for c in view['cards']}
    for b in bs:
        own = fence._open_for(b, snap)
        q = life.number(rows[b['card_id']]['remaining_quantity'], signed=True)
        with localcontext() as ctx:
            ctx.prec = 80
            total = sum((life.number(o['quantity']) for _, o in own), Decimal(0))
        if len(own) > 1 or total > max(q, Decimal(0)):
            raise DispatchError('EXISTING_EXIT_CAPACITY_NOT_EXCLUSIVE')
        if any(o['state'] != 'ACTIVE' for _, o in own):
            raise DispatchError('DORMANT_NATIVE_CHILDREN_NOT_ADOPTABLE')
    return bs, snap, rows, view


def plan(state, targets, *, now_ms):
    """One safe next step, not execution authority or a complete exit strategy.

    A TAKE_PROFIT target produces a plain reducing GTC limit at the ORIGINAL
    target price, not an already-crossed trigger order. Deciding when to switch
    from a native stop, and how to protect a resting TP, needs a separate policy.
    This function does not make either decision on the user's behalf.
    """
    bs, snap, rows, view = _read(state, now_ms)
    if not isinstance(targets, dict) or set(targets) != {b['card_id'] for b in bs}:
        raise DispatchError('EXACT_LOCAL_TARGETS_REQUIRED')
    if any(leg not in fence.EXITS for leg in targets.values()):
        raise DispatchError('EXACT_LOCAL_EXIT_LEG_REQUIRED')
    out = dict(version=VERSION, software_only=True, trading_policy_selected=False,
        dispatch_enabled=False, app_controls=False, gap_free_guaranteed=False,
        original_lifecycle_requires_review=view['needs_review'], step=None,
        status='PENDING_REQUEST_MUST_BE_RECONCILED' if state.get('pending') else 'NO_HANDOFF_NEEDED')
    if state.get('pending'):
        return out
    candidates = []
    for b in bs:
        cid = b['card_id']; own = fence._open_for(b, snap)
        row = rows[cid]; q = life.number(row['remaining_quantity'], signed=True)
        if q <= 0:
            continue
        desired = targets[cid]
        if own and own[0][0] == desired and life.number(own[0][1]['quantity']) == q:
            continue
        # Previously approved: exit fills stop future entry into THAT card.
        # Do not build an alternate entry-cancellation engine here.
        if life.number(row['exit_quantity']) > 0 and fence._open_for(b, snap, ('ENTRY',)):
            raise DispatchError('APPROVED_ENTRY_REMAINDER_CLEANUP_MUST_RUN_FIRST')
        original = state['originals'][cid]
        from .filled_quantity_exits import validate_draft
        draft = validate_draft(original['card'], original['draft'],
                               {b['role']: dict(account=b['account'])})
        asset = draft['entry_action']['orders'][0]['a']
        step = dict(card_id=cid, card_digest=b['card_digest'], account=b['account'],
            symbol=b['symbol'], role=b['role'], leg=own[0][0] if own else desired,
            desired_leg=desired, original_prices=deepcopy(b['prices']),
            observed_at_ms=snap['at_ms'], evidence_digest=life.digest(state['evidence']),
            quantity=life.text(q), old_oid=own[0][1]['oid'] if own else None,
            operation=('CANCEL_BEFORE_SWITCH' if own[0][0] != desired else 'CANCEL_BEFORE_RESIZE')
                      if own else 'PLACE_ONLY_EXIT')
        if own:
            step['wire_action'] = dict(type='cancel', cancels=[dict(a=asset, o=int(step['old_oid']))])
        else:
            # Every earlier exit must have terminal evidence; absence isn't enough.
            terminals = {o['oid'] for o in snap['terminal_orders']}
            if any(oid not in terminals for leg in fence.EXITS for oid in b['orders'][leg]):
                raise DispatchError('ALL_PREVIOUS_EXITS_REQUIRE_FINALITY')
            px = b['prices']['stop' if desired == 'STOP' else 'take_profit']
            from .filled_quantity_dispatch import precise
            precise(px, life.text(q), draft['size_decimals'])
            kind = (dict(trigger=dict(isMarket=True, triggerPx=px, tpsl='sl'))
                    if desired == 'STOP' else dict(limit=dict(tif='Gtc')))
            cloid = '0x' + life.digest([VERSION, state['bucket'], cid, desired,
                                       state['revision'], snap['at_ms']])[:32]
            step['wire_action'] = dict(type='order', grouping='na', orders=[dict(
                a=asset, b=b['side']=='SHORT', p=px, s=life.text(q), r=True, t=kind, c=cloid)])
        step['intent_id'] = life.digest([VERSION, step])
        candidates.append((0 if own else 1 if desired == 'STOP' else 2, cid, step))
    if candidates:
        out.update(status='PROPOSED_SOFTWARE_ONLY', step=sorted(candidates)[0][2])
    return out


def _lookup(raw, request, now_ms):
    """Match exact persisted wire terms, not a caller-supplied order number."""
    try:
        p = request['proposal']; wire = p['wire_action']['orders'][0]
        env = raw['order']; order = env['order']; oid = order['oid']
        trigger = p['leg'] == 'STOP'
        if (raw['status'] != 'order' or type(oid) is not int or not 0 < oid < 2**64
                or env['status'] not in {'open', 'filled'} | venue_evidence.CANCELED | venue_evidence.REJECTED
                or not request['attempt_at_ms'] <= life.moment(env['statusTimestamp']) <= now_ms
                or order['cloid'] != wire['c'] or order['coin'] != p['symbol']
                or order['side'] != ('B' if wire['b'] else 'A')
                or order['reduceOnly'] is not True or order['isTrigger'] is not trigger
                or order.get('isPositionTpsl', False) is not False
                or order['orderType'] != ('Stop Market' if trigger else 'Limit')
                or life.number(order['origSz'], positive=True) != life.number(wire['s'])
                or life.number(order['limitPx'], positive=True) != life.number(wire['p'])
                or trigger and life.number(order['triggerPx'], positive=True) != life.number(wire['p'])):
            raise ValueError()
        known = (request.get('reply') or {}).get('oid')
        if known is not None and known != str(oid):
            raise ValueError()
        return str(oid)
    except (KeyError, TypeError, ValueError, life.LifecycleError):
        raise DispatchError('EXCLUSIVE_ORDER_LOOKUP_NOT_VERIFIED') from None


class Rehearsal:
    """Durable protocol on an actual disposable DB. There is NO send method.

    Requests have no nonce and no dispatcher's action field, remain in the
    software domain, and cannot be passed to the Testnet send adapter. Existing
    pending recovery/dispatch blocks this workflow through the same store lock.
    """
    def __init__(self, store):
        if store.domain != 'software' or store.journal._ci is not True:
            raise DispatchError('DISPOSABLE_PROTOCOL_STORE_REQUIRED')
        self.store = store

    def prepare(self, state, targets, *, now_ms):
        result = plan(state, targets, now_ms=now_ms)
        if result['step'] is None:
            raise DispatchError('NO_EXCLUSIVE_STEP_TO_PREPARE')
        step = result['step']
        def update(conn, value):
            if value['pending'] is not None:
                raise DispatchError('UNRESOLVED_REQUEST_NO_NEW_INTENT')
            if plan(value, targets, now_ms=now_ms)['step'] != step:
                raise DispatchError('EXCLUSIVE_PROPOSAL_CHANGED')
            rid = life.digest([VERSION, value['bucket'], value['revision']+1, step])
            value['pending'] = value['last_request'] = rid
            return dict(request_id=rid, bucket=value['bucket'], domain='software',
                protocol=VERSION, phase='PREPARED', proposal=deepcopy(step),
                targets=deepcopy(targets), original_evidence=deepcopy(value['evidence']),
                original_records=deepcopy(value['originals']), prepared_at_ms=now_ms,
                attempt_at_ms=None, attempts=0, nonce=None, reply=None, observed_oid=None)
        return self.store.change(state['bucket'], state['revision'], 'EXCLUSIVE_PREPARE', now_ms, update)

    def begin(self, state, *, now_ms):
        def update(conn, value):
            req = self.store.pending_record(conn, value)
            if (req is None or req.get('protocol') != VERSION or req['phase'] != 'PREPARED'
                    or req['attempts'] != 0 or req['nonce'] is not None):
                raise DispatchError('EXCLUSIVE_ATTEMPT_NOT_REPEATABLE')
            if now_ms < req['prepared_at_ms']:
                raise DispatchError('EXCLUSIVE_CLOCK_MOVED_BACKWARDS')
            candidate = deepcopy(value); candidate['pending'] = None
            # A reservation changes revision, therefore compare frozen semantics
            # and evidence, NOT a freshly generated client order ID.
            _read(candidate, now_ms)
            if (candidate['evidence'] != req['original_evidence']
                    or candidate['originals'] != req['original_records']):
                raise DispatchError('EXCLUSIVE_EVIDENCE_CHANGED_REPLAN_UNSENT')
            req.update(phase='OUTCOME_UNKNOWN', attempt_at_ms=now_ms, attempts=1)
            return req
        return self.store.change(state['bucket'], state['revision'], 'EXCLUSIVE_ATTEMPT', now_ms, update)

    def confirm(self, state, snapshot, *, now_ms, order_lookup=None):
        """Only post-attempt, monotonic, complete evidence frees the next exit.

        Correct allocation is not proof of timely protection, price quality,
        settlement, or that the cancellation caused an order to become terminal.
        """
        def update(conn, value):
            req = self.store.pending_record(conn, value)
            if (req is None or req.get('protocol') != VERSION or req['attempt_at_ms'] is None
                    or req['phase'] not in ('OUTCOME_UNKNOWN', 'ACK_UNVERIFIED')):
                raise DispatchError('UNCONFLICTED_EXCLUSIVE_ATTEMPT_REQUIRED')
            if (snapshot['at_ms'] <= max(req['attempt_at_ms'], req.get('updated_at_ms', 0))
                    or value['originals'] != req['original_records']):
                raise DispatchError('NEW_EXCLUSIVE_OBSERVATION_REQUIRED')
            bs = deepcopy(value['bindings']); p = req['proposal']
            b = next(b for b in bs if b['card_id'] == p['card_id'])
            if p['operation'] == 'PLACE_ONLY_EXIT':
                oid = _lookup(order_lookup, req, now_ms)
                if any(oid in item['orders'][leg] for item in bs for leg in life.LEGS):
                    raise DispatchError('EXCLUSIVE_ORDER_ALREADY_OWNED')
                b['orders'][p['leg']].append(oid)
            else:
                oid = p['old_oid']
                if order_lookup is not None:
                    raise DispatchError('CANCEL_CONFIRMATION_DOES_NOT_ADOPT_ORDER')
            ev = dict(bindings=bs, snapshot=deepcopy(snapshot))
            _continues(req['original_evidence'], ev)
            _continues(value['evidence'], ev)
            candidate = {**value, 'bindings':bs, 'evidence':ev}
            _, snap, rows, view = _read(candidate, now_ms)
            if p['operation'] != 'PLACE_ONLY_EXIT':
                proof = fence.confirm_cancel(value, req, snap, bs, now_ms=now_ms)
            else:
                found = [o for group in ('open_orders','terminal_orders') for o in snap[group] if o['oid']==oid]
                if len(found) != 1:
                    raise DispatchError('PLACED_EXCLUSIVE_ORDER_NOT_OBSERVED')
                wire = p['wire_action']['orders'][0]
                unique = {f['fill_id']:f for f in snap['fills'] if f['oid']==oid}
                with localcontext() as ctx:
                    ctx.prec = 80
                    filled = sum((life.number(f['quantity']) for f in unique.values()), Decimal(0))
                record = found[0]; venue_status = order_lookup['order']['status']
                if any(not req['attempt_at_ms'] <= f['at_ms'] <= snap['at_ms'] for f in unique.values()):
                    raise DispatchError('EXCLUSIVE_FILL_TIME_NOT_BOUND_TO_ATTEMPT')
                open_record = any(o['oid']==oid for o in snap['open_orders'])
                if open_record:
                    if venue_status != 'open' or filled + life.number(record['quantity']) != life.number(wire['s']):
                        raise DispatchError('EXCLUSIVE_PLACEMENT_QUANTITY_NOT_RECONCILED')
                else:
                    expected_state = ('FILLED' if venue_status=='filled' else
                                      'CANCELED' if venue_status in venue_evidence.CANCELED else 'REJECTED')
                    if (venue_status == 'open' or record['state'] != expected_state
                            or life.number(record['filled_quantity']) != filled
                            or filled > life.number(wire['s'])
                            or expected_state=='FILLED' and filled != life.number(wire['s'])
                            or any(f['at_ms'] > record['at_ms'] for f in unique.values())):
                        raise DispatchError('EXCLUSIVE_PLACEMENT_QUANTITY_NOT_RECONCILED')
                proof = dict(exact_order_observed=True, native_oco_or_pre_cancel_isolation_proven=False)
            value.update(bindings=bs, evidence=ev, pending=None)
            req.update(phase='OBSERVED', observed_oid=oid, confirmation=proof,
                remaining_quantity=rows[p['card_id']]['remaining_quantity'],
                original_lifecycle_requires_review=view['needs_review'])
            return req
        return self.store.change(state['bucket'], state['revision'], 'EXCLUSIVE_OBSERVED', now_ms, update)
