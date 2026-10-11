"""Software-only handoff of registered parent-linked exits. No transport or signer.

The default NEVER cancels an entry. The alternative is an explicit REHEARSAL,
not approved trading policy. Normalized linkage must come from a verified local
order registry, never from a display application or inferred by symbol alone.
No child is counted as protection while WAITING_PARENT. No replacement is
proposed until the parent and ALL original children are observed terminal.
"""
from copy import deepcopy
from . import card_lifecycle as life, card_exit_recovery as recovery
from . import card_recovery_journal as durable

VERSION = 'offline-parent-exit-handoff-v1'
PRESERVE = 'preserve_entry'
REHEARSE = 'rehearse_cancel_then_rebuild_v1'
CANCELS = ('CANCEL_PARTIAL_PARENT_OFFLINE', 'CANCEL_LINKED_CHILD_OFFLINE')


class HandoffError(durable.RecoveryStorageError):
    """Fixed diagnostics; no remote text or credentials."""


def _link(bindings, snapshot, link):
    life.shape(link, 'card_id card_digest grouping parent_oid children')
    if link['grouping'] != 'normalTpsl':
        raise HandoffError('EXPLICIT_ORIGINAL_PARENT_LINK_REQUIRED')
    life.shape(link['children'], 'STOP TAKE_PROFIT')
    life.ident(link['parent_oid'], r'[1-9][0-9]{0,19}')
    if int(link['parent_oid']) >= 2**64:
        raise HandoffError('INVALID_ORIGINAL_PARENT_ID')
    selected = [b for b in bindings if b['card_id'] == link['card_id']]
    if len(selected) != 1:
        raise HandoffError('EXACT_LINKED_CARD_REQUIRED')
    b = selected[0]
    if (b['card_digest'] != link['card_digest']
            or life.address(b['account']) != life.address(snapshot['account'])
            or b['symbol'] != snapshot['symbol']
            or b['orders']['ENTRY'] != [link['parent_oid']]):
        raise HandoffError('ORIGINAL_PARENT_BINDING_MISMATCH')
    for leg, oid in link['children'].items():
        life.ident(oid, r'[1-9][0-9]{0,19}')
        if int(oid) >= 2**64 or oid not in b['orders'][leg]:
            raise HandoffError('ORIGINAL_CHILD_BINDING_MISMATCH')
    if len({link['parent_oid'], *link['children'].values()}) != 3:
        raise HandoffError('PARENT_AND_CHILDREN_MUST_BE_DISTINCT')
    return b


def assess(bindings, snapshot, context, link, *, now_ms, policy=PRESERVE):
    """Describe one next step; it is never executable authority.

    Preserve is the default. Rehearse models the documented cancel-parent path,
    followed by observed child retirement and independent fixed-size recovery.
    It can have an unprotected interval; that fact remains explicit throughout.
    """
    if policy not in (PRESERVE, REHEARSE):
        raise HandoffError('EXPLICIT_OFFLINE_POLICY_REQUIRED')
    # Validate the complete evidence and control map using existing contracts.
    base = recovery.plan(bindings, snapshot, context, now_ms=now_ms)
    view = life.review(bindings, snapshot, now_ms=now_ms)
    b = _link(bindings, snapshot, link)
    cid = b['card_id']; card = next(c for c in view['cards'] if c['card_id'] == cid)
    reasons = set(view['bucket_issues'])
    for c in view['cards']:
        reasons.update(set(c['issues']) - recovery.REMEDIABLE)
    if not 0 <= now_ms-context['at_ms'] <= 15000 or abs(context['at_ms']-snapshot['at_ms']) > 15000:
        reasons.add('MARK_STALE_OR_NOT_COMPARABLE')
    for control in context['cards'].values():
        for req in control['requests'].values():
            if req['state'] in ('ACCEPTED_UNVERIFIED', 'OUTCOME_UNKNOWN'):
                reasons.add('PRIOR_REQUEST_RECONCILIATION_REQUIRED')
            elif req['state'] == 'REJECTED':
                reasons.add('REJECTED_REQUEST_REQUIRES_RECHECK')
    opens = {o['oid']: o for o in snapshot['open_orders']}
    terminals = {o['oid']: o for o in snapshot['terminal_orders']}
    for f in snapshot['fills']:
        t = terminals.get(f['oid'])
        if t and (f['at_ms'] > t['at_ms'] or t['state'] == 'REJECTED'):
            reasons.add('INCONSISTENT_TERMINAL_FILL')
    parent = opens.get(link['parent_oid'])
    children = [opens.get(link['children'][leg]) for leg in recovery.EXITS]
    retired = all(oid in terminals for oid in link['children'].values())
    extra = set(b['orders']['STOP'] + b['orders']['TAKE_PROFIT']) - set(link['children'].values())
    # Matching aggregate size does not prove that a mixture of native and
    # independent exits is one safely isolated pair. Check before success too.
    if not retired and any(oid in opens for oid in extra):
        reasons.add('MIXED_NATIVE_AND_REPLACEMENT_EXITS_REVIEW')
    remaining = life.number(card['remaining_quantity'], signed=True)
    entered = life.number(card['entry_quantity'])
    protected = remaining > 0 and life.number(card['stop_quantity_observed']) == remaining and not reasons and not card['issues']
    out = dict(version=VERSION, environment='testnet', simulation_only=True,
        bucket=base['bucket'], card_id=cid, observed_at_ms=snapshot['at_ms'],
        valid_until_ms=base['valid_until_ms'], basis=life.digest([base['basis'], link, policy]),
        state='REVIEW_REQUIRED', reasons=[], next_step=None, recovery_proposal=None,
        independent_context=None, policy=policy, policy_approved_for_trading=False,
        parent_remainder_preserved=True, replacement_must_wait_for_originals=True,
        remaining_quantity=card['remaining_quantity'], stop_verified=protected,
        unprotected_quantity=card['remaining_quantity'] if remaining > 0 and not protected else '0',
        gap_free_guaranteed=False, dispatch_enabled=False, order_requests_sent=0,
        app_delivery_enabled=False, live_recovery_ready=False)
    if context['cards'][cid]['grouping'] not in ('parent_linked', 'independent_fixed'):
        reasons.add('LINKED_GROUPING_UNRESOLVED')
    if context['cards'][cid]['grouping'] == 'independent_fixed' and (parent or not retired):
        reasons.add('CANNOT_RELABEL_WORKING_LINKED_ORDERS')
    if reasons:
        return {**out, 'reasons': sorted(reasons), 'stop_verified': False,
                'unprotected_quantity': card['remaining_quantity'] if remaining > 0 else '0'}
    if parent:
        if parent['state'] != 'ACTIVE' or entered >= life.number(b['planned_quantity']):
            return {**out, 'reasons': ['PARENT_STATUS_REQUIRES_RECHECK']}
        if entered == 0:
            return {**out, 'state': 'WAITING_ENTRY'}
        if life.number(card['exit_quantity']) != 0 or remaining <= 0:
            return {**out, 'reasons': ['ENTRY_REMAINDER_AFTER_EXIT_REQUIRES_POLICY']}
        if not all(o is not None and o['state'] == 'WAITING_PARENT' for o in children):
            return {**out, 'reasons': ['LINKED_ACTIVATION_RACE_RECHECK']}
        if policy == PRESERVE:
            return {**out, 'state': 'POLICY_APPROVAL_REQUIRED',
                    'reasons': ['PARTIAL_ENTRY_REMAINDER_CANCELLATION_NOT_APPROVED']}
        step = _cancel(b, 'ENTRY', link['parent_oid'], CANCELS[0])
        return {**out, 'state': 'PROPOSED_OFFLINE', 'next_step': step,
                'parent_remainder_preserved': False}
    if link['parent_oid'] not in terminals:
        return {**out, 'reasons': ['PARENT_TERMINAL_EVIDENCE_REQUIRED']}
    if any(o is not None and o['state'] == 'WAITING_PARENT' for o in children):
        return {**out, 'state': 'WAITING_CHILD_FINALITY',
                'reasons': ['CHILDREN_NOT_YET_OBSERVED_ACTIVE_OR_TERMINAL']}
    if retired:
        # Original identities remain in the registry. Only FUTURE exits use
        # independent sizing; no active native group is renamed or adopted.
        adapted = deepcopy(context)
        adapted['cards'][cid]['grouping'] = 'independent_fixed'
        proposal = recovery.plan(bindings, snapshot, adapted, now_ms=now_ms)
        return {**out, 'state': 'ORIGINAL_GROUP_RETIRED',
                'reasons': proposal['reasons'], 'independent_context': adapted,
                'recovery_proposal': proposal}
    if remaining > 0 and not card['issues']:
        return {**out, 'state': 'NATIVE_EXITS_VERIFIED', 'stop_verified': True,
                'unprotected_quantity': '0'}
    # A terminal parent cannot acquire more size. A nonconforming native pair
    # may be retired in the explicit rehearsal, one exact child at a time.
    # Keep a working stop until last; never add a replacement alongside it.
    if policy == PRESERVE:
        return {**out, 'state': 'POLICY_APPROVAL_REQUIRED',
                'reasons': ['LINKED_CHILD_RETIREMENT_NOT_APPROVED']}
    for leg in ('TAKE_PROFIT', 'STOP'):
        oid = link['children'][leg]
        if oid in opens:
            return {**out, 'state': 'PROPOSED_OFFLINE',
                    'next_step': _cancel(b, leg, oid, CANCELS[1])}
    return {**out, 'reasons': ['CHILD_TERMINAL_EVIDENCE_REQUIRED']}


def _cancel(b, leg, oid, operation):
    step = dict(card_id=b['card_id'], card_digest=b['card_digest'],
        account=life.address(b['account']), symbol=b['symbol'], role=b['role'],
        leg=leg, operation=operation, order_id=oid,
        original_prices=deepcopy(b['prices']), planned_quantity=b['planned_quantity'])
    return {**step, 'intent_id': life.digest([VERSION, step])}
