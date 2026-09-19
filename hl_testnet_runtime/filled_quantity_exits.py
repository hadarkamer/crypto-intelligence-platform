"""Selected filled-quantity policy; unsigned planning and software tests ONLY.

A new entry is independent, never a normalTpsl parent with dormant children.
Each exit targets that card's observed entries minus exits, not account size.
Reuse existing reconciliation, recovery journal, precision and $10 sizing.
This module has NO signer, transport, scheduler, environment or startup hook.
Policy selection is not permission to send, cancel, modify or flatten an order.
"""
from copy import deepcopy
from . import card_lifecycle as life, card_exit_recovery as recovery
from . import trade_cards as cards

POLICY = 'per-card-filled-quantity-v1'
VERSION = 'unsigned-filled-entry-draft-v1'


def selection():
    return dict(policy=POLICY, design_selected=True, execution_authorized=False,
        exit_quantity='confirmed_card_entries_minus_confirmed_card_exits',
        preserve_unfilled_entry_on_partial_fill=True, close_entire_account_position=False,
        quantity_updates_by='bot_not_exchange_auto_resize', partial_cancel_policy_selected=False,
        entry_remainder_after_exit_policy='NOT_SELECTED', dispatch_enabled=False)


def prepare_entry(card, metadata, account, routes):
    """Draft one entry only. Never convert a previously submitted native group.

    The existing senders deliberately do NOT accept this one-leg draft. Enabling
    them is a separate integration, requiring a verified registrar and exit
    sender. An entry without those would expose an unprotected partial fill.
    """
    import hyperliquid_testnet_executor as builder
    card = cards.validate_card(card)
    if card['state'] != 'RECORDED_ONLY' or card['record_kind'] not in ('received_alert','synthetic_test'):
        raise life.LifecycleError('NEW_RECORDED_CARD_REQUIRED')
    account = life.address(account)
    if life.address(routes.get(card['account_role'],{}).get('account')) != account:
        raise life.LifecycleError('FILLED_ENTRY_ROUTE_MISMATCH')
    source = card['prepared']['execution']
    native = builder.build_action(source,metadata,account,exit_type='tp_limit_sl_market')
    if life.number(native['orders'][0]['s']) != life.number(card['planning']['quantity']):
        raise life.LifecycleError('FILLED_ENTRY_QUANTITY_MISMATCH')
    entry = deepcopy(native['orders'][0])
    entry['c'] = '0x'+life.digest([POLICY,account,card['card_id'],'ENTRY'])[:32]
    return dict(version=VERSION,policy=POLICY,environment='testnet',card_id=card['card_id'],
        card_digest=cards.checksum(card),account=account,role=card['account_role'],
        symbol=source['symbol'],side=source['side'],planned_quantity=entry['s'],
        prices={key:source[key] for key in ('entry','stop','take_profit')},
        source_event_id=source['event_id'],source_at=source['at'],
        size_decimals=card['prepared']['audit']['sz_decimals'],
        entry_action=dict(type='order',orders=[entry],grouping='na'),
        source_freshness_checked=False,unsigned_draft_only=True,dispatch_enabled=False)


def validate_draft(card, draft, routes):
    """Rebuild an exact draft from a trusted immutable local card and routing.

    Routes/card must be supplied by the bot's stores, not a display application.
    No draft, self-reported receipt or display copy proves exchange execution.
    """
    if not isinstance(draft,dict):
        raise life.LifecycleError('FILLED_ENTRY_DRAFT_REQUIRED')
    try:
        index = draft['entry_action']['orders'][0]['a']
        if type(index) is not int or not 0 <= index < 10000:
            raise life.LifecycleError('FILLED_ENTRY_ASSET_INVALID')
        meta = {'universe':[{'name':'_unused'} for _ in range(index)]+[
            {'name':card['prepared']['execution']['symbol'],
             'szDecimals':card['prepared']['audit']['sz_decimals']} ]}
        expected = prepare_entry(card,meta,draft['account'],routes)
        if draft != expected:
            raise life.LifecycleError('FILLED_ENTRY_DRAFT_CHANGED')
    except (KeyError,IndexError,TypeError):
        raise life.LifecycleError('FILLED_ENTRY_DRAFT_INVALID') from None
    return deepcopy(expected)


def assess(bindings, snapshot, context, *, originals, routes, now_ms):
    """Use existing fixed-size recovery on verified NEW-style registrations.

    originals maps each card_id to its original card and unsigned entry draft.
    A future registrar MUST prove entry cloid -> oid and exit ownership before
    producing bindings. This pure function cannot discover or authenticate them.
    It never relabels existing parent-linked orders or trusts an app's state.
    """
    life.validate_bindings(bindings)
    account,symbol = life.validate_snapshot(snapshot)
    if any(life.address(b['account']) != account or b['symbol'] != symbol for b in bindings):
        raise life.LifecycleError('ONE_FILLED_ENTRY_BUCKET_REQUIRED')
    if not isinstance(originals,dict) or set(originals) != {b['card_id'] for b in bindings}:
        raise life.LifecycleError('ALL_ORIGINAL_FILLED_ENTRY_RECORDS_REQUIRED')
    for b in bindings:
        original = originals[b['card_id']]
        life.shape(original,'card draft')
        draft = validate_draft(original['card'],original['draft'],routes)
        expected = life.binding_from_card(original['card'],account,routes,b['orders'])
        if b != expected or len(b['orders']['ENTRY']) != 1:
            raise life.LifecycleError('FILLED_ENTRY_BINDING_CHANGED')
        if context.get('cards',{}).get(b['card_id'],{}).get('grouping') != 'independent_fixed':
            raise life.LifecycleError('ORIGINAL_INDEPENDENT_GROUPING_REQUIRED')
    if any(o['state']=='WAITING_PARENT' for o in snapshot['open_orders']):
        raise life.LifecycleError('LEGACY_CHILDREN_CANNOT_BECOME_INDEPENDENT')
    result = recovery.plan(bindings,snapshot,context,now_ms=now_ms)
    view = life.review(bindings,snapshot,now_ms=now_ms)
    reasons = set(result['reasons'])
    # Selecting protection of fills does not silently choose what to do with
    # an outstanding entry AFTER an exit has begun. Keep that decision explicit.
    for b in bindings:
        card = next(c for c in view['cards'] if c['card_id']==b['card_id'])
        live_entry = any(o['oid'] in b['orders']['ENTRY'] for o in snapshot['open_orders'])
        if live_entry and life.number(card['exit_quantity']) > 0:
            reasons.add('ENTRY_REMAINDER_AFTER_EXIT_POLICY_REQUIRED')
    if reasons:
        result.update(state='REVIEW_REQUIRED',reasons=sorted(reasons),next_step=None)
    return {**result, 'selected_policy':POLICY, 'design_selected':True,
        'execution_authorized':False, 'entry_waiting_for_full_fill':False,
        'close_entire_account_position':False, 'native_per_card_isolation':False,
        'gap_free_protection_guaranteed':False,
        'requires_verified_execution_registrar':True,
        'requires_sibling_exit_race_handling':True}
