"""Connected filled-quantity execution path, LOCKED by default.

Real PostgreSQL + existing card/lifecycle/reader + Testnet-only transport. The
same controller is tested by replacing ONLY the external venue boundary. No
HTTP control route, automatic startup, application callbacks or extra timer.
No existing normalTpsl order is converted. Release requires a separate approval.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import http.client
import json
import re
import time

from . import card_lifecycle as life, filled_quantity_exits as selected
from . import card_exit_recovery as recovery, card_sync_evidence as evidence
from . import filled_pending_cancel as half_cancel
from . import residual_exit_fence as residual
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .trade_card_store import CardStore
from .dispatch_concurrency import market_lane
from . import checks, two_account_execution as roles

VERSION = 'connected-filled-quantity-dispatch-v1'
AFTER_EXIT = 'cancel_remainder_after_exit_v1'
HOST = 'api.hyperliquid-testnet.xyz'


def rejection_reason(raw):
    """Bound and redact a venue error before it reaches durable state or logs."""
    if not isinstance(raw, dict):
        return None
    message = raw.get('response') if raw.get('status') == 'err' else None
    if message is None and raw.get('status') == 'ok':
        response = raw.get('response')
        data = response.get('data') if isinstance(response, dict) else None
        statuses = data.get('statuses') if isinstance(data, dict) else None
        if (isinstance(statuses, list) and len(statuses) == 1
                and isinstance(statuses[0], dict) and set(statuses[0]) == {'error'}):
            message = statuses[0]['error']
    if not isinstance(message, str) or not 0 < len(message) <= 1024:
        return None
    message = re.sub(r'0x[0-9a-fA-F]{8,}', '[address]', message)
    message = re.sub(r'\b[0-9a-fA-F]{32,}\b', '[hex]', message)
    message = re.sub(r'\b[A-Za-z0-9_+/=-]{48,}\b', '[token]', message)
    message = re.sub(r'\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b', '[email]', message)
    if len(message) > 240 or re.search(r'[^\x20-\x7e]', message):
        return None
    return message


def rejection_subject(raw, account, agent):
    """Classify a recovered signer without persisting either wallet address."""
    if not isinstance(raw, dict):
        return None
    message = raw.get('response') if raw.get('status') == 'err' else None
    match = (re.search(r'User or API Wallet (0x[0-9a-fA-F]{40}) does not exist', message)
             if isinstance(message, str) and len(message) <= 1024 else None)
    if match is None:
        return None
    recovered = match.group(1).lower()
    if recovered == str(agent).lower():
        return 'AGENT'
    if recovered == str(account).lower():
        return 'ACCOUNT'
    return 'UNEXPECTED_SIGNER'


def canonical_wire_action(action):
    """Rebuild the exact SDK wire order after JSONB has discarded key order."""
    if not isinstance(action, dict):
        raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
    kind = action.get('type')
    if kind == 'batchModify':
        if (set(action) != {'type','modifies'} or not isinstance(action['modifies'],list)
                or len(action['modifies']) != 1):
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        item=action['modifies'][0]
        if (not isinstance(item,dict) or set(item) != {'oid','order'}
                or type(item['oid']) is not int or not 0 < item['oid'] < 2**64):
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        order=canonical_wire_action(dict(type='order',orders=[item['order']],grouping='na'))['orders'][0]
        if order['r'] is not True or 'trigger' not in order['t']:
            raise DispatchError('ONLY_PROTECTIVE_EXIT_MODIFICATION_ALLOWED')
        return dict(type='batchModify',modifies=[dict(oid=item['oid'],order=order)])
    if kind == 'order':
        if (set(action) != {'type','orders','grouping'} or action.get('grouping') != 'na'
                or not isinstance(action.get('orders'), list) or len(action['orders']) != 1):
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        source = action['orders'][0]
        if not isinstance(source, dict) or set(source) != {'a','b','p','s','r','t','c'}:
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        source_type = source['t']
        if (isinstance(source_type, dict) and set(source_type) == {'limit'}
                and isinstance(source_type['limit'], dict)
                and set(source_type['limit']) == {'tif'}):
            order_type = {'limit': {'tif': source_type['limit']['tif']}}
        elif (isinstance(source_type, dict) and set(source_type) == {'trigger'}
                and isinstance(source_type['trigger'], dict)
                and set(source_type['trigger']) == {'isMarket','triggerPx','tpsl'}):
            trigger = source_type['trigger']
            order_type = {'trigger': {'isMarket': trigger['isMarket'],
                                      'triggerPx': trigger['triggerPx'],
                                      'tpsl': trigger['tpsl']}}
        else:
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        def wire_number(value):
            number = life.number(value,positive=True)
            rendered = format(number,'f')
            if '.' in rendered:
                rendered = rendered.rstrip('0').rstrip('.')
            if not rendered or rendered == '0':
                raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
            return rendered
        if 'trigger' in order_type:
            order_type['trigger']['triggerPx'] = wire_number(
                order_type['trigger']['triggerPx'])
        order = {'a': source['a'], 'b': source['b'], 'p': wire_number(source['p']),
                 's': wire_number(source['s']), 'r': source['r'], 't': order_type,
                 'c': source['c']}
        return {'type': 'order', 'orders': [order], 'grouping': 'na'}
    if kind == 'cancel':
        if (set(action) != {'type','cancels'} or not isinstance(action.get('cancels'), list)
                or len(action['cancels']) != 1):
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        source = action['cancels'][0]
        if not isinstance(source, dict) or set(source) != {'a','o'}:
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        return {'type': 'cancel', 'cancels': [{'a': source['a'], 'o': source['o']}]}
    raise DispatchError('ACTION_WIRE_SHAPE_INVALID')


def asset(meta, symbol):
    universe=meta.get('universe') if isinstance(meta,dict) else None
    if not isinstance(universe,list) or not 0 < len(universe) <= 10000:
        raise DispatchError('CURRENT_METADATA_REQUIRED')
    found=[(i,x) for i,x in enumerate(universe) if isinstance(x,dict) and x.get('name')==symbol]
    if len(found)!=1:
        raise DispatchError('ASSET_NOT_FOUND')
    index,row=found[0];decimals=row.get('szDecimals')
    if type(decimals) is not int or not 0<=decimals<=6 or row.get('isDelisted',False) is not False:
        raise DispatchError('ASSET_PRECISION_OR_LISTING_INVALID')
    return index,decimals


def precise(price, quantity, decimals):
    p=life.number(price,positive=True);q=life.number(quantity,positive=True)
    n=p.normalize()
    if (p!=p.to_integral_value() and (len(n.as_tuple().digits)>5 or n.as_tuple().exponent<-(6-decimals))
            or q!=q.quantize(Decimal(1).scaleb(-decimals))):
        raise DispatchError('CURRENT_PRICE_OR_SIZE_PRECISION_REJECTED')


def normalized_reply(raw, kind, *, account=None, agent=None):
    if kind != 'cancel':
        result = recovery.classify_reply(raw,['ENTRY'])['ENTRY']
        if result['state'] == 'REJECTED':
            reason = rejection_reason(raw)
            if reason is not None:
                result['venue_reason'] = reason
            if account is not None and agent is not None:
                subject = rejection_subject(raw, account, agent)
                if subject is not None:
                    result['rejection_subject'] = subject
        return result
    unknown=dict(state='OUTCOME_UNKNOWN',code=None,oid=None)
    if not isinstance(raw,dict): return unknown
    if raw.get('status')=='err' and isinstance(raw.get('response'),str):
        return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
    response=raw.get('response'); data=response.get('data') if isinstance(response,dict) else None
    items=data.get('statuses') if isinstance(data,dict) else None
    if raw.get('status')!='ok' or not isinstance(response,dict) or response.get('type')!='cancel' or not isinstance(items,list) or len(items)!=1:
        return unknown
    if items[0]=='success':
        return dict(state='ACCEPTED_UNVERIFIED',code=None,oid=None)
    # "Already filled/canceled" is NOT proof of cancellation or permission to recreate.
    if isinstance(items[0],dict) and set(items[0])=={'error'}:
        return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
    return unknown


def requested_order(action):
    return action['modifies'][0]['order'] if action['type']=='batchModify' else action['orders'][0]


def identity(raw, request, now_ms):
    """Verify a venue lookup of the PERSISTED cloid, not a caller's three IDs."""
    if not isinstance(raw,dict) or raw.get('status')!='order':
        # A normalized exchange rejection plus an exact public unknownOid result
        # is not an ambiguous transport outcome. It is still terminal for this
        # request: never retry it, and never invent an order or binding.
        reply = request.get('reply') or {}
        if (isinstance(raw,dict) and raw.get('status')=='unknownOid'
                and reply.get('state')=='REJECTED'
                and request.get('proposal',{}).get('leg')=='ENTRY'):
            raise DispatchError('ENTRY_REJECTED_NO_ORDER_NO_RETRY')
        raise DispatchError('OUTCOME_UNRESOLVED_NO_NEW_REQUEST')
    env=raw.get('order');o=env.get('order') if isinstance(env,dict) else None
    p=request['proposal'];expected=requested_order(p['action'])
    if not isinstance(o,dict): raise DispatchError('INVALID_ORDER_LOOKUP')
    oid=o.get('oid');stamp=life.moment(env.get('statusTimestamp'))
    if type(oid) is not int or not 0<oid<2**64 or not request['attempt_at_ms']<=stamp<=now_ms:
        raise DispatchError('ORDER_LOOKUP_ID_OR_TIME_MISMATCH')
    leg=p['leg'];immediate=p['operation'] in ('CLOSE_PASSED_TAKE','EMERGENCY_CLOSE')
    typ='Limit' if leg=='ENTRY' or immediate else 'Stop Market' if leg=='STOP' else 'Take Profit Limit'
    if (o.get('cloid')!=expected['c'] or o.get('coin')!=p['symbol']
            or o.get('side')!=('B' if expected['b'] else 'A')
            or type(o.get('reduceOnly')) is not bool or o['reduceOnly']!=expected['r']
            or life.number(o.get('origSz'),positive=True)!=life.number(expected['s'])
            or life.number(o.get('limitPx'),positive=True)!=life.number(expected['p'])
            or o.get('orderType')!=typ or o.get('isPositionTpsl',False) is not False
            or type(o.get('isTrigger')) is not bool or o['isTrigger']!=(leg!='ENTRY' and not immediate)):
        raise DispatchError('ORDER_TERMS_NOT_BOUND_TO_INTENT')
    if immediate and (expected['t'] != {'limit':{'tif':'Ioc'}}
                      or env.get('status')=='open'):
        raise DispatchError('IMMEDIATE_TAKE_NOT_TERMINAL')
    if leg!='ENTRY' and not immediate and life.number(o.get('triggerPx'),positive=True)!=life.number(expected['t']['trigger']['triggerPx']):
        raise DispatchError('TRIGGER_NOT_BOUND_TO_INTENT')
    if (request.get('reply') or {}).get('state')=='REJECTED':
        raise DispatchError('REJECTION_CONFLICTS_WITH_PUBLIC_ORDER')
    known=(request.get('reply') or {}).get('oid')
    if known is not None and known!=str(oid):
        raise DispatchError('RECEIPT_AND_PUBLIC_ORDER_CONFLICT')
    if env.get('status') not in {'open','filled'} | evidence.CANCELED | evidence.REJECTED:
        raise DispatchError('ORDER_STATE_REQUIRES_REVIEW')
    return str(oid)


def exit_modify_proposal(state, meta, sample, *, card_id, leg, old_oid, now_ms, sequence=None):
    """Preview one owned exit amendment; no sender, key or alternate authority.

    Also permits a same-quantity amendment for a supervised Testnet probe.
    The caller must still use authorize/reserve/begin/send and public reconciliation.
    """
    from .residual_exit_contract import validate_wire_proposal
    snap=state['evidence']['snapshot']
    if not 0<=now_ms-snap['at_ms']<=15000 or not 0<=now_ms-sample['at_ms']<=15000:
        raise DispatchError('FRESH_EVIDENCE_REQUIRED')
    index,decimals=asset(meta,state['symbol'])
    sequence=state['revision']+1 if sequence is None else sequence
    b=next(b for b in state['bindings'] if b['card_id']==card_id)
    if leg not in recovery.EXITS:
        raise DispatchError('ONLY_PROTECTIVE_EXIT_MODIFICATION_ALLOWED')
    view=life.review(state['bindings'],snap,now_ms=now_ms)
    v=next(v for v in view['cards'] if v['card_id']==card_id)
    q=life.text(life.number(v['remaining_quantity'],positive=True))
    price=b['prices']['stop' if leg=='STOP' else 'take_profit']
    mark=life.number(sample['mark_price'],positive=True);target=life.number(price)
    crossed=(mark<=target if leg=='STOP' else mark>=target) if b['side']=='LONG' else (mark>=target if leg=='STOP' else mark<=target)
    if crossed:
        raise DispatchError('EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE')
    precise(price,q,decimals)
    cloid='0x'+life.digest([VERSION,state['bucket'],card_id,leg,sequence])[:32]
    order=dict(a=index,b=b['side']=='SHORT',p=price,s=q,r=True,
        t=dict(trigger=dict(isMarket=leg=='STOP',triggerPx=price,tpsl='sl' if leg=='STOP' else 'tp')),c=cloid)
    card=state['originals'][card_id]['card']
    p=dict(version=VERSION,card_id=card_id,account=state['account'],symbol=state['symbol'],
        role=b['role'],leg=leg,operation='MODIFY_EXIT',quantity=q,old_oid=old_oid,sequence=sequence,
        action=dict(type='batchModify',modifies=[dict(oid=int(old_oid),order=order)]),
        source_at=card['prepared']['execution']['at'],basis=life.digest(state['evidence']),observed_at_ms=snap['at_ms'])
    if 'source_expires_at' in card:p['source_expires_at']=card['source_expires_at']
    residual.validate_proposal(state,p,now_ms=now_ms)
    validate_wire_proposal(state,p,now_ms=now_ms)
    return p


def choose(state, routes, meta, sample, *, now_ms, sequence=None, after_exit_policy='NOT_SELECTED',
           allowed_entry_card_id=None):
    """Build the next exact action from verified card quantities; never sends."""
    ev=state['evidence'];snap=ev['snapshot'];bs=state['bindings']
    if not 0<=now_ms-snap['at_ms']<=15000 or not 0<=now_ms-sample['at_ms']<=15000:
        raise DispatchError('FRESH_EVIDENCE_REQUIRED')
    index,decimals=asset(meta,state['symbol'])
    sequence=state['revision']+1 if sequence is None else sequence
    def make(cid,leg,operation,action,quantity,old_oid=None):
        card=state['originals'][cid]['card']
        result=dict(version=VERSION,card_id=cid,account=state['account'],symbol=state['symbol'],
            role=state['originals'][cid]['draft']['role'],leg=leg,operation=operation,
            action=action,quantity=quantity,old_oid=old_oid,sequence=sequence,
            source_at=card['prepared']['execution']['at'],
            basis=life.digest(ev),observed_at_ms=snap['at_ms'])
        if 'source_expires_at' in card:
            result['source_expires_at']=card['source_expires_at']
        return result
    def modify_exit(cid,leg,oid):
        return exit_modify_proposal(state,meta,sample,card_id=cid,leg=leg,old_oid=oid,
                                    now_ms=now_ms,sequence=sequence)
    bound={b['card_id'] for b in bs}
    # Existing exposure is serviced first, never delayed by a new entry.
    if bs:
        # An unrelated crossed exit level must not suppress exact-owner cleanup.
        # No sibling is considered retired until public finality is reconciled.
        orphan=residual.cleanup(state,now_ms=now_ms,after_exit_policy=after_exit_policy)
        if orphan:
            if orphan['operation']=='CANCEL_FOR_RESIZE':
                return modify_exit(orphan['card_id'],orphan['leg'],orphan['oid'])
            return make(orphan['card_id'],orphan['leg'],orphan['operation'],
                dict(type='cancel',cancels=[dict(a=index,o=int(orphan['oid']))]),'0',orphan['oid'])
        # Source expiry used to stop NEW submissions. A GTC order already on
        # the book needs its own exact cancellation, including after a partial
        # fill. Do this before exit recovery can stop on a crossed old target.
        views=life.review(bs,snap,now_ms=now_ms)
        if not views['bucket_issues']:
            from .source_window import timestamp
            opens={o['oid']:o for o in snap['open_orders']}
            for b in sorted(bs,key=lambda item:item['card_id']):
                card=state['originals'][b['card_id']]['card']
                expiry=card.get('source_expires_at')
                v=next(row for row in views['cards'] if row['card_id']==b['card_id'])
                if (card['record_kind']!='received_alert' or expiry is None
                        or now_ms<int(timestamp(expiry).timestamp()*1000)
                        or life.number(v['exit_quantity'])!=0
                        or set(v['issues'])-{'STOP_COVERAGE_MISSING','TAKE_PROFIT_COVERAGE_MISSING'}
                        or len(b['orders']['ENTRY'])!=1):
                    continue
                oid=b['orders']['ENTRY'][0]
                own=opens.get(oid)
                if (own is None or own['state']!='ACTIVE'
                        or life.number(own['quantity'],positive=True)
                            +life.number(v['entry_quantity'])!=life.number(b['planned_quantity'])):
                    continue
                return make(b['card_id'],'ENTRY','CANCEL_EXPIRED_ENTRY_REMAINDER',
                    dict(type='cancel',cancels=[dict(a=index,o=int(oid))]),'0',oid)
        originals={cid:state['originals'][cid] for cid in bound}
        context=dict(symbol=state['symbol'],mark_price=sample['mark_price'],at_ms=sample['at_ms'],cards={
            b['card_id']:dict(grouping='independent_fixed',requests={leg:dict(state='NONE',code=None) for leg in recovery.EXITS}) for b in bs})
        report=selected.assess(bs,snap,context,originals=originals,routes=routes,now_ms=now_ms)
        # The old trigger cannot be installed after its level has passed. A
        # single final-entry card may instead try one reduce-only IOC limit at
        # the immutable target: it fills at that price or better, or vanishes.
        # Never infer closure from the mark, and never retry this attempt after
        # a terminal IOC. Public fills and terminal status decide what happened.
        if report['reasons']==['EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE'] and len(bs)==1:
            b=bs[0];v=views['cards'][0]
            target=life.number(b['prices']['take_profit'])
            mark=life.number(sample['mark_price'],positive=True)
            favorable=mark>=target if b['side']=='LONG' else mark<=target
            remaining=life.number(v['remaining_quantity'],signed=True)
            terminal_ids={o['oid'] for o in snap['terminal_orders']}
            if (favorable and not views['bucket_issues'] and remaining>0
                    and set(v['issues']) <= {'STOP_COVERAGE_MISSING','TAKE_PROFIT_COVERAGE_MISSING'}
                    and not b['orders']['TAKE_PROFIT']
                    and all(oid in terminal_ids for oid in b['orders']['ENTRY'])
                    and all(o['oid'] in b['orders']['STOP'] for o in snap['open_orders'])
                    and life.number(v['stop_quantity_observed']) in (0,remaining)):
                q=life.text(remaining);price=b['prices']['take_profit']
                precise(price,q,decimals)
                cloid='0x'+life.digest([VERSION,state['bucket'],b['card_id'],
                                       'CLOSE_PASSED_TAKE',sequence])[:32]
                order=dict(a=index,b=b['side']=='SHORT',p=price,s=q,r=True,
                           t=dict(limit=dict(tif='Ioc')),c=cloid)
                return make(b['card_id'],'TAKE_PROFIT','CLOSE_PASSED_TAKE',
                            dict(type='order',orders=[order],grouping='na'),q)
        # Cancelling a still-pending entry after any exit is a separate, explicit policy.
        if 'ENTRY_REMAINDER_AFTER_EXIT_POLICY_REQUIRED' in report['reasons']:
            if after_exit_policy!=AFTER_EXIT:
                raise DispatchError('ENTRY_REMAINDER_AFTER_EXIT_POLICY_REQUIRED')
            allowed=set(recovery.REMEDIABLE)|{'FLAT_WITH_WORKING_ORDERS'}
            if views['bucket_issues'] or any(set(v['issues'])-allowed for v in views['cards']):
                raise DispatchError('LIFECYCLE_DISCREPANCY_REQUIRES_REVIEW')
            for b in bs:
                v=next(v for v in views['cards'] if v['card_id']==b['card_id'])
                if life.number(v['exit_quantity'])>0:
                    working=[o for o in snap['open_orders'] if o['oid'] in b['orders']['ENTRY']]
                    if working:
                        return make(b['card_id'],'ENTRY','CANCEL_ENTRY_AFTER_EXIT',
                            dict(type='cancel',cancels=[dict(a=index,o=int(working[0]['oid']))]),'0',working[0]['oid'])
        if report['reasons']:
            # Once the original take level has passed, do not place that stale
            # take order. A sole card with a final entry, confirmed position,
            # no exits and a still-valid original stop can be protected first.
            # Every other review reason continues to block the dispatcher.
            stop=report.get('protective_stop_step')
            if (report['reasons'] != ['EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE']
                    or stop is None or len(bs)!=1 or views['bucket_issues']
                    or 'STOP_COVERAGE_MISSING' not in views['cards'][0]['issues']
                    or set(views['cards'][0]['issues']) -
                        {'STOP_COVERAGE_MISSING','TAKE_PROFIT_COVERAGE_MISSING'}
                    or life.number(views['cards'][0]['entry_quantity'])<=0
                    or life.number(views['cards'][0]['remaining_quantity'],signed=True)<=0
                    or snap['open_orders']
                    or not any(o['oid'] in bs[0]['orders']['ENTRY']
                        for o in snap['terminal_orders'])):
                raise DispatchError('LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW')
            step=stop
        else:
            step=report['next_step']
        if step:
            cid,leg,op=step['card_id'],step['leg'],step['operation']
            b=next(b for b in bs if b['card_id']==cid)
            if op in ('RESIZE_EXIT','CANCEL_ORPHAN_EXIT'):
                oid=step['order_id']
                if op=='RESIZE_EXIT':
                    return modify_exit(cid,leg,oid)
                return make(cid,leg,'CANCEL_ORPHAN_EXIT',
                    dict(type='cancel',cancels=[dict(a=index,o=int(oid))]),'0',oid)
            q=step['target_quantity'];price=step['original_price'];precise(price,q,decimals)
            cloid='0x'+life.digest([VERSION,state['bucket'],cid,leg,sequence])[:32]
            order=dict(a=index,b=b['side']=='SHORT',p=price,s=q,r=True,
                t=dict(trigger=dict(isMarket=leg=='STOP',triggerPx=price,tpsl='sl' if leg=='STOP' else 'tp')),c=cloid)
            return make(cid,leg,'CREATE_EXIT',dict(type='order',orders=[order],grouping='na'),q)
        # This is the pre-existing half-formula rule, now connected to the SAME
        # durable cancel/receipt/reconciliation path. Never cancel partial fills.
        candidates=half_cancel.scan(state,routes,sample,now_ms=now_ms)['candidates']
        if candidates:
            candidate=candidates[0];cid=candidate['card_id'];oid=candidate['oid']
            proposal=make(cid,'ENTRY',half_cancel.OPERATION,
                dict(type='cancel',cancels=[dict(a=index,o=int(oid))]),'0',oid)
            proposal.update(cancel_rule_digest=candidate['rule_digest'],
                cancel_first_crossing_at_ms=candidate['first_crossing_at_ms'],
                cancel_sample_at_ms=candidate['sample_at_ms'])
            return proposal
        # Hyperliquid nets positions by account and symbol. Independent TP and
        # stop orders are not per-card OCO, so another card cannot enter this
        # market until every older card and every one of its orders is final.
        # Keep the newer alert recorded; source expiry is checked again below.
        if any(v['state'] not in ('CLOSED', 'CANCELED_WITHOUT_FILL')
               or v['issues'] for v in views['cards']):
            return None
    # Distinct alerts stay distinct. When a symbol is free again, consider
    # their original source time, never the outcome or the card hash order.
    unbound=sorted((cid for cid in state['originals'] if cid not in bound),
        key=lambda cid:(state['originals'][cid]['card']['prepared']['source']['at'],cid))
    for cid in unbound:
        if allowed_entry_card_id is not None and cid != allowed_entry_card_id:
            continue
        original=state['originals'][cid]
        if original.get('entry_rejected_no_retry') is True:
            continue
        if 'source_expires_at' in original['card']:
            from .source_window import source_fresh, timestamp
            if not source_fresh(timestamp(original['card']['prepared']['source']['at']),
                    original['card']['source_expires_at'],
                    now=datetime.fromtimestamp(now_ms/1000,timezone.utc)):
                continue
        draft=selected.validate_draft(original['card'],original['draft'],routes)
        if draft['entry_action']['orders'][0]['a']!=index or draft['size_decimals']!=decimals:
            raise DispatchError('ENTRY_METADATA_CHANGED_REVIEW_REQUIRED')
        # Existing release scope is a controlled trial, not an unlimited deployment.
        p=draft['prices'];mark=life.number(sample['mark_price'],positive=True)
        if not min(life.number(p['stop']),life.number(p['take_profit']))<mark<max(life.number(p['stop']),life.number(p['take_profit'])):
            raise DispatchError('PRICE_OUTSIDE_ORIGINAL_EXITS')
        return make(cid,'ENTRY','ENTRY',deepcopy(draft['entry_action']),draft['planned_quantity'])
    return None


class Controller:
    """One bounded cycle. No loops or app input; caller controls scheduling."""
    def __init__(self, store, venue, routes, *, after_exit_policy='NOT_SELECTED'):
        if store.domain!=venue.domain:
            raise DispatchError('SOFTWARE_AND_ACCOUNT_STORAGE_MUST_NOT_MIX')
        self.store,self.venue,self.routes=store,venue,deepcopy(routes)
        # Final account-ownership authorization needs every durable bucket for
        # this account, not only the bucket passed to authorize().
        self.venue.store=self.store
        self.after_exit_policy=after_exit_policy

    def register(self, card_id, *, single_card=False):
        card=CardStore(self.store.journal).load(card_id)
        owner_policy=None
        if card['rule']['threshold_pct'] is None:
            if card['rule']['id']!='U21_XRP_SHORT' or 'source_expires_at' not in card:
                raise DispatchError('SOURCE_CANCEL_POLICY_REQUIRES_OWNER_DECISION')
            owner_policy=deepcopy(half_cancel.U21_OWNER_POLICY)
        account=life.address(self.routes[card['account_role']]['account'])
        draft=selected.prepare_entry(card,self.venue.metadata(),account,self.routes)
        state=self.store.create_bucket(account,draft['symbol'])
        original=dict(card=card,draft=draft)
        if owner_policy is not None:
            original['cancel_policy']=owner_policy
        if card_id in state['originals']:
            if original!=state['originals'][card_id]: raise DispatchError('IMMUTABLE_ORIGINAL_CHANGED')
            if single_card and set(state['originals'])!={card_id}:
                raise DispatchError('ONE_EXPLICIT_SECOND_ACCOUNT_TRIAL_REQUIRED')
            return state
        if 'source_expires_at' in card:
            from .source_window import source_fresh, timestamp
            if not source_fresh(timestamp(card['prepared']['source']['at']),
                    card['source_expires_at'],
                    now=datetime.fromtimestamp(self.venue.now()/1000,timezone.utc)):
                raise DispatchError('U21_ORIGINAL_SOURCE_EXPIRED' if owner_policy is not None
                                    else 'ORIGINAL_SOURCE_EXPIRED')
        def update(conn,s):
            if s['pending']: raise DispatchError('REGISTER_WHILE_REQUEST_UNRESOLVED')
            if single_card and s['originals']:
                raise DispatchError('ONE_EXPLICIT_SECOND_ACCOUNT_TRIAL_REQUIRED')
            s['originals'][card_id]=original
        return self.store.change(state['bucket'],state['revision'],'REGISTER_LOCAL_CARD',self.venue.now(),update)

    @market_lane
    def refresh(self,bucket):
        """Public reads first; one transaction records ownership, evidence and outcome."""
        state=self.store.load(bucket);now=self.venue.now();request=None;oid=None;raw=None
        bs=deepcopy(state['bindings'])
        if state['pending']:
            request=self.store.request(state['pending'])
            if request['phase']=='CONFLICT': raise DispatchError('CONFLICT_REQUIRES_REVIEW')
            if request['attempt_at_ms'] is not None and request['proposal']['action']['type'] in ('order','batchModify'):
                cloid=requested_order(request['proposal']['action'])['c']
                raw=self.venue.lookup(state['account'],cloid)
                if (raw == {'status':'unknownOid'} and request['phase']=='REJECTED'
                        and request['proposal']['leg']=='ENTRY' and not bs
                        and (request.get('reply') or {}).get('state')=='REJECTED'):
                    # Only a confirmed venue rejection can use this path. The
                    # original intent remains terminal and can never be resent.
                    snap=self.venue.rejected_entry_evidence(state,request,cloid)
                    life.validate_snapshot(snap)
                    if (snap['at_ms']<=request['attempt_at_ms'] or
                            snap['account']!=state['account'] or snap['symbol']!=state['symbol'] or
                            snap['position_quantity']!='0' or snap['fills'] or
                            snap['open_orders'] or snap['terminal_orders'] or
                            snap['history_complete'] is not True or
                            snap['orders_complete'] is not True):
                        raise DispatchError('POST_REJECTION_EVIDENCE_REQUIRED')
                    def retire(conn,s):
                        current=self.store.pending_record(conn,s)
                        if (current!=request or s['bindings'] or s['pending']!=request['request_id']
                                or s['originals'][request['proposal']['card_id']].get('entry_rejected_no_retry')):
                            raise DispatchError('REJECTION_RECONCILIATION_CHANGED')
                        s['originals'][request['proposal']['card_id']]['entry_rejected_no_retry']=True
                        s['evidence']=dict(bindings=[],snapshot=snap)
                        s['pending']=None
                        current['phase']='OBSERVED'
                        current['terminal_state']='REJECTED_NO_ORDER'
                        current['observed_at_ms']=snap['at_ms']
                        return current
                    return self.store.change(bucket,state['revision'],
                        'REJECTED_ENTRY_NO_ORDER_RECONCILED',self.venue.now(),retire)
                if (raw == {'status':'unknownOid'} and request['phase']=='REJECTED'
                        and request['proposal']['leg'] in recovery.EXITS and bs
                        and (request.get('reply') or {}).get('state')=='REJECTED'
                        and (request.get('reply') or {}).get('rejection_subject')=='UNEXPECTED_SIGNER'
                        and not state['originals'][request['proposal']['card_id']].get(
                            'exit_signature_recovery_used')):
                    snap=self.venue.rejected_exit_evidence(state,request,cloid)
                    life.validate_snapshot(snap)
                    if (snap['at_ms']<=request['attempt_at_ms'] or
                            snap['account']!=state['account'] or snap['symbol']!=state['symbol'] or
                            snap['history_complete'] is not True or
                            snap['orders_complete'] is not True):
                        raise DispatchError('POST_REJECTION_EVIDENCE_REQUIRED')
                    def retire_exit(conn,s):
                        current=self.store.pending_record(conn,s)
                        original=s['originals'][request['proposal']['card_id']]
                        if (current!=request or s['bindings']!=bs
                                or s['pending']!=request['request_id']
                                or original.get('exit_signature_recovery_used')):
                            raise DispatchError('REJECTION_RECONCILIATION_CHANGED')
                        original['exit_signature_recovery_used']=True
                        s['evidence']=dict(bindings=deepcopy(bs),snapshot=snap)
                        s['pending']=None
                        current['phase']='OBSERVED'
                        current['terminal_state']='REJECTED_NO_ORDER'
                        current['observed_at_ms']=snap['at_ms']
                        return current
                    return self.store.change(bucket,state['revision'],
                        'REJECTED_EXIT_SIGNATURE_NO_ORDER_RECONCILED',
                        self.venue.now(),retire_exit)
                oid=identity(raw,request,self.venue.now())
                cid=request['proposal']['card_id'];leg=request['proposal']['leg']
                b=next((b for b in bs if b['card_id']==cid),None)
                if b is None:
                    if leg!='ENTRY': raise DispatchError('EXIT_BEFORE_REGISTERED_ENTRY')
                    b=life.binding_from_card(state['originals'][cid]['card'],state['account'],self.routes,
                                            dict(ENTRY=[oid],STOP=[],TAKE_PROFIT=[]));bs.append(b)
                elif oid not in b['orders'][leg]: b['orders'][leg].append(oid)
        if not bs:
            snap=self.venue.empty_snapshot(state['account'],state['symbol'])
        else:
            if state['evidence'] is None:
                raise DispatchError('COMPLETE_PRE_ENTRY_CHECKPOINT_REQUIRED')
            snap=self.venue.collect(dict(bindings=bs,snapshot=state['evidence']['snapshot']))['snapshot']
        now=self.venue.now();life.validate_snapshot(snap)
        if not snap['history_complete'] or not snap['orders_complete'] or not 0<=now-snap['at_ms']<=15000:
            raise DispatchError('OBSERVATION_INCOMPLETE_OR_STALE')
        def update(conn,s):
            if s['evidence'] is not None and s['bindings']:
                from .card_lifecycle_store import _continues
                _continues(s['evidence'],dict(bindings=bs,snapshot=snap))
            s['bindings']=bs;s['evidence']=dict(bindings=bs,snapshot=snap)
            from .emergency_close import record_timing
            record_timing(s,request,raw,now)
            current=self.store.pending_record(conn,s)
            if current is None or current['attempt_at_ms'] is None: return None
            if snap['at_ms']<=current['attempt_at_ms']:
                raise DispatchError('POST_ATTEMPT_OBSERVATION_REQUIRED')
            p=current['proposal']
            if p['action']['type'] in ('order','batchModify'):
                if oid is None: raise DispatchError('EXACT_ORDER_OWNERSHIP_REQUIRED')
                cloid=requested_order(p['action'])['c']
                if p['action']['type']=='batchModify':
                    ending=[o for o in snap['terminal_orders'] if o['oid']==p['old_oid']]
                    if (len(ending)!=1 or ending[0]['state'] not in ('CANCELED','FILLED')
                            or ending[0]['at_ms']<current['attempt_at_ms']):
                        raise DispatchError('MODIFIED_EXIT_PREDECESSOR_NOT_FINAL')
                conn.execute(f'''INSERT INTO {SCHEMA}.ownership VALUES(%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING''',(s['account'],oid,cloid,p['card_id'],p['leg'],current['request_id']))
                saved=conn.execute(f'SELECT card_id,leg,request_id,cloid FROM {SCHEMA}.ownership WHERE account=%s AND oid=%s',
                                   (s['account'],oid)).fetchone()
                if saved!=(p['card_id'],p['leg'],current['request_id'],cloid):
                    raise DispatchError('ORDER_OWNERSHIP_COLLISION')
                if not any(o['oid']==oid for group in ('open_orders','terminal_orders') for o in snap[group]):
                    raise DispatchError('REGISTERED_ORDER_NOT_OBSERVED')
                current['observed_oid']=oid
            else:
                ending=[o for o in snap['terminal_orders'] if o['oid']==p['old_oid']]
                if not ending: return current  # A receipt alone cannot unlock replacement.
                if ending[0]['at_ms']<=p['observed_at_ms']:
                    raise DispatchError('TERMINAL_BEFORE_WORKING_OBSERVATION')
                current['residual_verification']=residual.confirm_cancel(s,current,snap,bs,now_ms=now)
                current['terminal_state']=ending[0]['state']
                current['cancellation_caused_terminal_state']=False
            current['phase']='OBSERVED';current['observed_at_ms']=snap['at_ms'];s['pending']=None
            return current
        return self.store.change(bucket,state['revision'],'PUBLIC_RECONCILIATION',now,update)

    def cycle(self,bucket,*,send=False,allow_new_entries=True,allowed_entry_card_id=None):
        """A false send flag never reserves, signs, cancels or places an order."""
        prepared=self._prepare_cycle(bucket,send=send,allow_new_entries=allow_new_entries,
                                     allowed_entry_card_id=allowed_entry_card_id)
        if isinstance(prepared,dict):
            return prepared
        state,request,proposal,route=prepared
        # Release the local reconciliation lane before transport. A delayed
        # stop/entry HTTP reply must not prevent the independent emergency worker
        # from observing a fill and acting on its durable request identity.
        sent_before=getattr(self.venue,'sent',0)
        try:
            raw=self.venue.send(request)
            reply=normalized_reply(raw,proposal['action']['type'],
                                   account=proposal['account'],agent=route['agent'])
        except Exception:
            return dict(status='OUTCOME_UNKNOWN',order_requests_sent=max(0,getattr(self.venue,'sent',0)-sent_before))
        try:
            self.store.reply(state,reply,self.venue.now())
        except DispatchError as exc:
            if str(exc)!='CONCURRENT_DISPATCH_RELOAD_REQUIRED':
                raise
            # Public reconciliation may overtake an HTTP receipt. Preserve its
            # newer checkpoint and resolve through the existing pending fence;
            # never replay or conceal that this transport was invoked.
            return dict(status='OUTCOME_UNKNOWN',
                        order_requests_sent=max(0,getattr(self.venue,'sent',0)-sent_before))
        result=dict(status=reply['state'],request_id=request['request_id'],
                    order_requests_sent=max(0,getattr(self.venue,'sent',0)-sent_before))
        if reply['state']=='REJECTED':
            result['rejection_code']=reply['code']
            result['rejection_reason']=reply.get('venue_reason')
            result['rejection_subject']=reply.get('rejection_subject')
            result['symbol']=proposal['symbol']
        return result

    @market_lane
    def _prepare_cycle(self,bucket,*,send,allow_new_entries,allowed_entry_card_id):
        state=self.refresh(bucket)
        if state['pending']:
            pending=self.store.request(state['pending'])
            if pending['phase']!='PREPARED':
                return dict(status=pending['phase'],order_requests_sent=0)
        else: pending=None
        sample=self.venue.sample(state['account'],state['symbol']);meta=self.venue.metadata()
        # Retire only provably NEVER-ATTEMPTED obsolete exit work. Unknown
        # requests retain the same durable barrier across closure and restart.
        state=residual.retire_obsolete_unsent(self.store,state,now_ms=self.venue.now())
        # Observation only, including during preview. The existing durable state
        # remembers crossings; an obsolete NEVER-SENT cancel can yield to a fill.
        state=half_cancel.checkpoint(self.store,state,self.routes,sample,now_ms=self.venue.now())
        pending=self.store.request(state['pending']) if state['pending'] else None
        sequence=pending['proposal']['sequence'] if pending else None
        proposal=choose(state,self.routes,meta,sample,now_ms=self.venue.now(),sequence=sequence,
                        after_exit_policy=self.after_exit_policy,
                        allowed_entry_card_id=allowed_entry_card_id)
        if send is not True:
            return dict(status='PREVIEW_ONLY',proposal=proposal,order_requests_sent=0)
        if proposal is None: return dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        if proposal['operation']=='ENTRY' and allow_new_entries is not True:
            return dict(status='NEW_ENTRIES_DISABLED',order_requests_sent=0)
        # All release checks occur BEFORE reservation or any key access.
        self.venue.authorize(state,proposal,self.after_exit_policy)
        if pending:
            prior=deepcopy(pending['proposal']);fresh=deepcopy(proposal)
            for item in (prior,fresh):
                item.pop('basis');item.pop('observed_at_ms')
                item.pop('cancel_sample_at_ms',None)
            if prior!=fresh:
                raise DispatchError('UNSENT_PLAN_CHANGED_EXPLICIT_REPLAN_REQUIRED')
            # Preserve the frozen action identity but refresh the evidence used at begin.
            def rebase(conn,s):
                req=self.store.pending_record(conn,s)
                if req['phase']!='PREPARED': raise DispatchError('CANNOT_REBASE_STARTED_REQUEST')
                req['proposal']=deepcopy(proposal)
                return req
            state=self.store.change(bucket,state['revision'],'REVALIDATE_UNSENT',self.venue.now(),rebase)
        else:
            state=self.store.reserve(state,proposal,self.venue.now())
        route=self.routes[proposal['role']]
        state=self.store.begin(state,proposal,route['agent'],self.venue.now())
        request=self.store.request(state['pending'])
        # A lost commit reply never reaches transport.
        return state,request,proposal,route


class TestnetVenue:
    """Actual Testnet adapter with explicit per-role gates and no host injection.

    Independent same-market OCO is not assumed; later cards wait for finality.
    """
    domain='testnet'
    def __init__(self,env): self.env=env;self.sent=0
    @staticmethod
    def now(): return time.time_ns()//1000000
    def metadata(self): return checks.InfoReader().read('meta')
    def sample(self,account,symbol):
        start=self.now();raw=checks.InfoReader().read('activeAssetData',user=account,coin=symbol)
        checks.capacity(raw,account,symbol)
        return dict(mark_price=raw['markPx'],at_ms=start)
    def lookup(self,account,cloid):
        import hyperliquid_testnet_executor as legacy
        return legacy.TestnetHTTP().info('orderStatus',user=account,oid=cloid)
    def collect(self,value): return evidence.collect(value,evidence.PublicReader(parallel=True))
    def empty_snapshot(self,account,symbol):
        reader=evidence.PublicReader();start=self.now()
        for _ in range(2):
            orders=reader.read('frontendOpenOrders',account);state=reader.read('clearinghouseState',account)
            if (not isinstance(orders,list) or not isinstance(state,dict)
                    or not isinstance(state.get('assetPositions'),list)
                    or any(not isinstance(o,dict) or not isinstance(o.get('coin'),str)
                           for o in orders)
                    or any(not isinstance(p,dict) or not isinstance(p.get('position'),dict)
                           or not isinstance(p['position'].get('coin'),str)
                           for p in state['assetPositions'])):
                raise DispatchError('FIRST_TRIAL_REQUIRES_EMPTY_DEDICATED_ACCOUNT')
            if (any(o['coin']==symbol for o in orders)
                    or any(p['position']['coin']==symbol and
                           life.number(p['position']['szi'],signed=True)!=0
                           for p in state['assetPositions'])):
                raise DispatchError('UNOWNED_SYMBOL_EXPOSURE_REQUIRES_REVIEW')
            if self.env.get('HL_TESTNET_RUNTIME_MODE')!='long_stream_testnet_v1' and (
                    orders or any(life.number(p['position']['szi'],signed=True)!=0
                                  for p in state['assetPositions'])):
                raise DispatchError('FIRST_TRIAL_REQUIRES_EMPTY_DEDICATED_ACCOUNT')
        if self.now()-start>15000: raise DispatchError('PRE_ENTRY_CHECKPOINT_EXPIRED')
        return dict(environment='testnet',account=account,symbol=symbol,at_ms=start,history_complete=True,
            orders_complete=True,position_quantity='0',fills=[],open_orders=[],terminal_orders=[])
    def rejected_entry_evidence(self,state,request,cloid):
        """Independent public observations; no signer and no order endpoint."""
        from .card_sync_evidence import PublicReader, history, DAY_MS
        start=request['attempt_at_ms'];now=self.now()
        if (state['bindings'] or request['attempts']!=1 or request['reply']['oid'] is not None
                or not 120000<=now-start<DAY_MS):
            raise DispatchError('REJECTION_HISTORY_WINDOW_REQUIRES_REVIEW')
        reader=PublicReader()
        for _ in range(2):
            if self.lookup(state['account'],cloid)!={'status':'unknownOid'}:
                raise DispatchError('REJECTION_ORDER_LOOKUP_CHANGED')
            if any(row.get('coin')==state['symbol'] for row in
                   history(reader,state['account'],start,self.now())):
                raise DispatchError('REJECTION_FILL_FOUND_NO_RELEASE')
            snap=self.empty_snapshot(state['account'],state['symbol'])
        if self.now()-start>=DAY_MS:
            raise DispatchError('REJECTION_HISTORY_WINDOW_REQUIRES_REVIEW')
        return snap
    def rejected_exit_evidence(self,state,request,cloid):
        """One-time recovery for the diagnosed pre-canonical exit signature."""
        from .card_sync_evidence import PublicReader, history, DAY_MS
        start=request['attempt_at_ms'];now=self.now()
        original=state['originals'][request['proposal']['card_id']]
        if (not state['bindings'] or state['evidence'] is None or request['attempts']!=1
                or request['reply']['oid'] is not None
                or request['reply'].get('rejection_subject')!='UNEXPECTED_SIGNER'
                or original.get('exit_signature_recovery_used')
                or not 120000<=now-start<DAY_MS):
            raise DispatchError('REJECTION_HISTORY_WINDOW_REQUIRES_REVIEW')
        reader=PublicReader()
        for _ in range(2):
            if self.lookup(state['account'],cloid)!={'status':'unknownOid'}:
                raise DispatchError('REJECTION_ORDER_LOOKUP_CHANGED')
            if any(row.get('coin')==state['symbol'] for row in
                   history(reader,state['account'],start,self.now())):
                raise DispatchError('REJECTION_FILL_FOUND_NO_RELEASE')
        observed=self.collect(state['evidence'])
        if observed['bindings']!=state['bindings'] or self.now()-start>=DAY_MS:
            raise DispatchError('POST_REJECTION_EVIDENCE_REQUIRED')
        return observed['snapshot']
    def _gate(self,proposal,after_exit_policy):
        env=self.env
        if hasattr(self,'store'):
            self.store.action_allowed(proposal)
        if proposal.get('operation')=='ENTRY' and env.get('HL_TESTNET_EMERGENCY_CLOSE'):
            from .emergency_close import APPROVAL, healthy
            if env['HL_TESTNET_EMERGENCY_CLOSE']!=APPROVAL or not healthy(self.now()):
                raise DispatchError('EMERGENCY_SUPERVISOR_NOT_FRESH_NO_NEW_ENTRY')
            trial=env.get('HL_TESTNET_PROTECTION_TIMING_CARD_ID','')
            try: trial_expires=int(env.get('HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS',''))
            except (ValueError,TypeError):
                raise DispatchError('EXACT_TIMING_TRIAL_APPROVAL_REQUIRED') from None
            if (not re.fullmatch(r'[0-9a-f]{64}',trial) or trial!=proposal.get('card_id')
                    or not 0<trial_expires-self.now()<=120000):
                raise DispatchError('EXACT_TIMING_TRIAL_APPROVAL_REQUIRED')
        stream=(env.get('HL_TESTNET_RUNTIME_MODE')=='long_stream_testnet_v1'
            and env.get('HL_TESTNET_FILLED_DISPATCH')=='approved_long_stream_v1'
            and env.get('HL_TESTNET_LONG_STREAM')=='approved_alerts_v1'
            and (proposal['role']=='long_account' or
                 (proposal['role']=='short_account' and
                  env.get('HL_TESTNET_SHORT_STREAM')=='approved_alerts_v1' and
                  env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') in ('true','false')))
            and env.get('HL_TESTNET_FILLED_CARD_ID','')=='')
        single=(env.get('HL_TESTNET_RUNTIME_MODE')=='filled_card_controlled_v1'
            and env.get('HL_TESTNET_FILLED_DISPATCH')=='approved_single_card_v1'
            and env.get('HL_TESTNET_FILLED_CARD_ID')==proposal['card_id'])
        if (env.get('RENDER_SERVICE_ID')!=roles.SERVICE or not (single or stream)
                or env.get('HL_TESTNET_SAFETY_PIPELINE') or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION')!='disabled'
                or env.get('HL_TESTNET_CARD_SYNC')):
            raise DispatchError('FILLED_DISPATCH_NOT_AUTHORIZED')
        if after_exit_policy!=AFTER_EXIT or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY')!=AFTER_EXIT:
            raise DispatchError('AFTER_EXIT_REMAINDER_DECISION_REQUIRED_BEFORE_TRIAL')
        if single:
            try: expires=int(env.get('HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS',''))
            except (ValueError,TypeError): raise DispatchError('EXACT_APPROVAL_DEADLINE_REQUIRED') from None
            if expires<=0 or (proposal['operation']=='ENTRY' and not 0<expires-self.now()<=86400000):
                raise DispatchError('TRIAL_ENTRY_APPROVAL_EXPIRED')
        elif proposal['operation']=='ENTRY':
            flag=('HL_TESTNET_LONG_ENTRY_ENABLED' if proposal['role']=='long_account'
                  else 'HL_TESTNET_SHORT_ENTRY_ENABLED')
            if env.get(flag)!='true':
                raise DispatchError('STREAM_ENTRIES_DISABLED_MANAGEMENT_CONTINUES'
                    if proposal['role']=='short_account' else 'LONG_ENTRIES_DISABLED_MANAGEMENT_CONTINUES')
            if (proposal['role']=='short_account' and
                    (not re.fullmatch(r'[0-9a-f]{64}', env.get('HL_TESTNET_SHORT_TRIAL_CARD_ID',''))
                     or proposal['card_id']!=env['HL_TESTNET_SHORT_TRIAL_CARD_ID'])):
                raise DispatchError('SHORT_ENTRY_OUTSIDE_EXACT_TRIAL_CARD')
        # Check original source age again at the final boundary, including after
        # a slow budget read. Never refresh an alert timestamp on retry.
        if proposal['operation']=='ENTRY':
            from .source_window import source_fresh, timestamp
            try:
                at=timestamp(proposal['source_at'])
            except (KeyError,TypeError,ValueError):
                raise DispatchError('ORIGINAL_SOURCE_TIME_REQUIRED') from None
            if stream:
                key=('HL_TESTNET_LONG_NOT_BEFORE' if proposal['role']=='long_account'
                     else 'HL_TESTNET_SHORT_NOT_BEFORE')
                try: start=timestamp(env[key])
                except (KeyError,TypeError,ValueError):
                    raise DispatchError('STREAM_START_TIME_REQUIRED') from None
                if at < start:
                    raise DispatchError('STREAM_SOURCE_BEFORE_RELEASE_WINDOW')
            if not source_fresh(at,proposal.get('source_expires_at',
                    env.get('HL_TESTNET_FILLED_SOURCE_EXPIRES_AT')),
                    now=datetime.fromtimestamp(self.now()/1000,timezone.utc)):
                raise DispatchError('NEW_TRIAL_SOURCE_NOT_FRESH')
        # Entry approval expiry must NOT silently terminate management of an open card.
        return roles.route_for(env,proposal['role'],proposal['account'])
    def authorize(self,state,proposal,after_exit_policy):
        route=self._gate(proposal,after_exit_policy)
        if proposal['operation']=='ENTRY':
            self._fresh_entry_evidence(proposal)
        if self.env.get('HL_TESTNET_RUNTIME_MODE')=='long_stream_testnet_v1':
            # A missing or mismatched signer must fail before a durable attempt
            # is begun, where its outcome would otherwise remain uncertain.
            roles.wallet_for_role(self.env,proposal['role'],route['account'],route['agent'])
        if (self.env.get('HL_TESTNET_RUNTIME_MODE')=='filled_card_controlled_v1'
                and any(b['card_id']!=proposal['card_id'] for b in state['bindings'])):
            raise DispatchError('SHARED_MARKET_TRIAL_ISOLATION_NOT_VERIFIED')
        if self.env.get('HL_TESTNET_RUNTIME_MODE')=='long_stream_testnet_v1':
            original=state['originals'][proposal['card_id']]['card']
            if (original['record_kind']!='received_alert'
                    or original['account_role']!=proposal['role']
                    or 'source_expires_at' not in original):
                raise DispatchError('DELIVERED_STREAM_SOURCE_REQUIRED')
            # The first card in a market bucket has no predecessor bindings.
            # Its empty pre-entry checkpoint and account-ownership sweep are the
            # applicable safeguards; lifecycle.review intentionally requires at
            # least one binding and is only meaningful for existing cards.
            if state['bindings']:
                snap=state['evidence']['snapshot']
                view=life.review(state['bindings'],snap,now_ms=self.now())
                if view['bucket_issues'] or any(v['card_id']!=proposal['card_id']
                        and (v['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL') or v['issues'])
                        for v in view['cards']):
                    raise DispatchError('SHARED_MARKET_PREDECESSOR_NOT_FINAL')
        if proposal['operation']=='ENTRY':
            if self.env.get('HL_TESTNET_RUNTIME_MODE')=='long_stream_testnet_v1':
                # An unrelated position or order appearing since the worker's
                # sweep must block new entries at the final authorization gate.
                from .long_stream_runtime import _account_owned
                _account_owned(self,route['account'],self.store.for_account(route['account']),
                               role=proposal['role'])
            source=state['originals'][proposal['card_id']]['card']['prepared']['execution']
            from .source_window import source_fresh, timestamp
            expiry=state['originals'][proposal['card_id']]['card'].get('source_expires_at',
                self.env.get('HL_TESTNET_FILLED_SOURCE_EXPIRES_AT'))
            if not source_fresh(timestamp(source['at']),expiry,
                    now=datetime.fromtimestamp(self.now()/1000,timezone.utc)):
                raise DispatchError('NEW_TRIAL_SOURCE_NOT_FRESH')
            plan={k:source[k] for k in ('symbol','side','entry','stop','take_profit')}
            report=roles.budget_for_role(self.env,proposal['role'],route['account'],route['agent'],plan,checks.InfoReader())
            if report.get('status')!='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION' or report.get('test_plan_checked') is not True:
                raise DispatchError('EXACT_ENTRY_BUDGET_NOT_VERIFIED')
            roles.entry_action_headroom(route['account'],checks.InfoReader())
            # Slow account/budget reads must fail BEFORE a durable reservation
            # and attempt, not leave an unsent entry marked outcome-unknown.
            self._gate(proposal,after_exit_policy)
            self._fresh_entry_evidence(proposal)
    def _fresh_entry_evidence(self,proposal):
        try:
            observed=life.moment(proposal['observed_at_ms'])
            now=life.moment(self.now())
        except (KeyError,TypeError,life.LifecycleError):
            raise DispatchError('ENTRY_EVIDENCE_TIME_REQUIRED') from None
        if not 0<=now-observed<=15000:
            raise DispatchError('ENTRY_EVIDENCE_EXPIRED_BEFORE_RESERVATION')
    def _fresh_attempt(self,request):
        """Check both the attempt AND its evidence, including after local signing.

        Approval lifetime and signature expiry are different clocks from evidence
        freshness. A slow local preparation cannot extend either freshness bound.
        This is a guard, not permission or proof that quantities cannot change.
        """
        if (not isinstance(request,dict) or request.get('domain')!='testnet'
                or request.get('phase')!='OUTCOME_UNKNOWN'
                or type(request.get('attempts')) is not int or request['attempts']!=1):
            raise DispatchError('DURABLE_TESTNET_REQUEST_REQUIRED')
        try:
            at=life.moment(request['attempt_at_ms'])
            prepared=life.moment(request['prepared_at_ms'])
            observed=life.moment(request['proposal']['observed_at_ms'])
            nonce=life.moment(request['nonce']);now=life.moment(self.now())
        except (KeyError,TypeError,life.LifecycleError):
            raise DispatchError('DURABLE_ATTEMPT_TIMELINE_REQUIRED') from None
        if prepared>at or observed>at or not 0<=now-at<=5000:
            raise DispatchError('DURABLE_ATTEMPT_EXPIRED')
        if not 0<=now-observed<=15000:
            raise DispatchError('FINAL_EVIDENCE_EXPIRED')
        if not at<=nonce<=at+1000:
            raise DispatchError('PERSISTED_NONCE_TIME_INVALID')
        half_cancel.final_freshness(request,now_ms=now)
    def send(self,request):
        p=request['proposal'];route=self._gate(p,self.env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY'))
        self._fresh_attempt(request)
        wallet=roles.wallet_for_role(self.env,p['role'],route['account'],route['agent'])
        from hyperliquid.utils.signing import sign_l1_action
        expires=request['nonce']+15000
        action=canonical_wire_action(p['action'])
        if HOST!='api.hyperliquid-testnet.xyz':
            raise DispatchError('ACTION_OR_HOST_FORBIDDEN')
        signature=sign_l1_action(wallet,action,None,request['nonce'],expires,False)
        body=json.dumps(dict(action=action,nonce=request['nonce'],signature=signature,expiresAfter=expires)).encode()
        if len(body)>16384: raise DispatchError('REQUEST_TOO_LARGE')
        self._gate(p,self.env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY'))
        self._fresh_attempt(request)
        connection=http.client.HTTPSConnection(HOST,timeout=4)
        try:
            self.sent+=1
            connection.request('POST','/exchange',body,{'Content-Type':'application/json'})
            response=connection.getresponse();raw=response.read(checks.MAX_BYTES+1)
            if response.status!=200 or len(raw)>checks.MAX_BYTES: raise DispatchError('TRANSPORT_OUTCOME_UNKNOWN')
            return checks.decode(raw)
        finally: connection.close()


def controller_from_env(env):
    """An explicit internal entry point, NEVER called by app HTTP or startup."""
    from .postgres_journal import PostgresJournal
    from .trade_cards import account_routes
    return Controller(DispatchStore(PostgresJournal.from_env(env)),TestnetVenue(env),
                      account_routes(env),
                      after_exit_policy=env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY','NOT_SELECTED'))
