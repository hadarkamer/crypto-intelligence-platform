"""Exclusive per-card exit allocation on the existing durable SOFTWARE journal.

At most one independently executable exit per card, never larger than its
remaining confirmed allocation. A replacement cannot consume capacity until
its predecessor is terminal and all fills are reconciled. No sender, keys,
runtime switch or formula selection. This is NOT uninterrupted stop protection.
The alternative trading policy remains separate from permission to test code.
"""
from copy import deepcopy
from decimal import Decimal, localcontext

from . import card_lifecycle as life, residual_exit_fence as fence
from .card_lifecycle_store import _continues
from .filled_dispatch_store import DispatchError, SCHEMA
from . import card_sync_evidence as venue_evidence

VERSION = 'exclusive-exit-protocol-software-v2'
# These warnings remain visible. They are not claims of adequate protection.
EXPECTED_WARNINGS = {'STOP_COVERAGE_MISSING', 'TAKE_PROFIT_COVERAGE_MISSING',
                     'BOTH_EXIT_LEGS_FILLED_REVIEW', 'FLAT_WITH_WORKING_ORDERS'}


def _contracts(state):
    contracts = state.get('exclusive_exit_contracts', {})
    if not isinstance(contracts, dict):
        raise DispatchError('EXCLUSIVE_ORDER_CONTRACTS_REQUIRED')
    plain = set()
    for oid, record in contracts.items():
        life.ident(oid, r'[1-9][0-9]{0,19}')
        if int(oid) >= 2**64:
            raise DispatchError('EXCLUSIVE_ORDER_ID_INVALID')
        life.shape(record, 'card_id leg wire_order attempt_at_ms request_id')
        life.ident(record['request_id'], r'[0-9a-f]{64}')
        life.moment(record['attempt_at_ms'])
        b = next((b for b in state['bindings'] if b['card_id']==record['card_id']), None)
        leg = record['leg']; wire = record['wire_order']
        if b is None or leg not in fence.EXITS or oid not in b['orders'][leg]:
            raise DispatchError('EXCLUSIVE_ORDER_CONTRACT_OWNER_MISMATCH')
        life.shape(wire, 'a b p s r t c')
        from .filled_quantity_exits import validate_draft
        original = state['originals'][b['card_id']]
        draft = validate_draft(original['card'], original['draft'], {b['role']:dict(account=b['account'])})
        price = b['prices']['stop' if leg=='STOP' else 'take_profit']
        kind = (dict(trigger=dict(isMarket=True,triggerPx=price,tpsl='sl'))
                if leg=='STOP' else dict(limit=dict(tif='Gtc')))
        if (type(wire['a']) is not int or wire['a']!=draft['entry_action']['orders'][0]['a']
                or wire['b'] is not (b['side']=='SHORT') or wire['r'] is not True
                or wire['p']!=price or wire['t']!=kind):
            raise DispatchError('EXCLUSIVE_ORDER_CONTRACT_TERMS_MISMATCH')
        life.number(wire['s'],positive=True);life.ident(wire['c'],r'0x[0-9a-f]{32}')
        if leg=='TAKE_PROFIT': plain.add(oid)
    return contracts, plain


def _read(state, now_ms):
    if not isinstance(state,dict) or state.get('domain')!='software':
        raise DispatchError('EXCLUSIVE_PROTOCOL_SOFTWARE_ONLY')
    bs,snap,_ = fence._read(state,now_ms)
    contracts,plain = _contracts(state)
    view = life.review(bs,snap,now_ms=now_ms,plain_take_profit_oids=plain)
    if view['bucket_issues'] or any(set(c['issues'])-EXPECTED_WARNINGS for c in view['cards']):
        raise DispatchError('EXCLUSIVE_EVIDENCE_REQUIRES_REVIEW')
    rows = {c['card_id']:c for c in view['cards']}
    fills = {f['fill_id']:f for f in snap['fills']}.values()
    terminals = {t['oid']:t for t in snap['terminal_orders']}
    with localcontext() as ctx:
        ctx.prec=80
        for f in fills:
            t=terminals.get(f['oid'])
            if t and (f['at_ms']>t['at_ms'] or t['state']=='REJECTED'):
                raise DispatchError('EXCLUSIVE_TERMINAL_FILL_CONFLICT')
        for b in bs:
            own=fence._open_for(b,snap);q=life.number(rows[b['card_id']]['remaining_quantity'],signed=True)
            total=sum((life.number(o['quantity']) for _,o in own),Decimal(0))
            if len(own)>1 or total>max(q,Decimal(0)):
                raise DispatchError('EXISTING_EXIT_CAPACITY_NOT_EXCLUSIVE')
            if any(o['state']!='ACTIVE' for _,o in own):
                raise DispatchError('DORMANT_NATIVE_CHILDREN_NOT_ADOPTABLE')
        for oid,contract in contracts.items():
            actual=[f for f in fills if f['oid']==oid]
            done=sum((life.number(f['quantity']) for f in actual),Decimal(0))
            size=life.number(contract['wire_order']['s'])
            working=[o for o in snap['open_orders'] if o['oid']==oid]
            if (done>size or any(f['at_ms']<contract['attempt_at_ms'] for f in actual)
                    or working and done+life.number(working[0]['quantity'])!=size
                    or oid in terminals and terminals[oid]['state']=='FILLED' and done!=size):
                raise DispatchError('EXCLUSIVE_ORDER_BUDGET_NOT_RECONCILED')
    return bs,snap,rows,view


def plan(state, targets, *, now_ms):
    """Plan a local chosen leg, not when to trade or remove native protection.

    A plain take-profit limit is honestly represented as LIMIT, not a trigger.
    Selecting when to switch and how to restore protection is trading policy,
    still outside this allocation mechanism. Cancellation is never atomic.
    """
    bs,snap,rows,view=_read(state,now_ms)
    if not isinstance(targets,dict) or set(targets)!={b['card_id'] for b in bs}:
        raise DispatchError('EXACT_LOCAL_TARGETS_REQUIRED')
    if any(leg not in fence.EXITS for leg in targets.values()):
        raise DispatchError('EXACT_LOCAL_EXIT_LEG_REQUIRED')
    out=dict(version=VERSION,software_only=True,trading_policy_selected=False,
        dispatch_enabled=False,app_controls=False,gap_free_guaranteed=False,
        original_lifecycle_requires_review=view['needs_review'],step=None,
        status='PENDING_REQUEST_MUST_BE_RECONCILED' if state.get('pending') else 'NO_HANDOFF_NEEDED')
    if state.get('pending'):return out
    candidates=[]
    for b in bs:
        cid=b['card_id'];row=rows[cid];own=fence._open_for(b,snap)
        q=life.number(row['remaining_quantity'],signed=True);desired=targets[cid]
        # Approved cancellation of still-waiting entry comes BEFORE flat/no-op
        # checks, including a partial TP that leaves its own limit correctly sized.
        parents=fence._open_for(b,snap,('ENTRY',))
        after_exit=life.number(row['exit_quantity'])>0 and bool(parents)
        if not after_exit and (q<=0 or own and own[0][0]==desired and life.number(own[0][1]['quantity'])==q):
            continue
        from .filled_quantity_exits import validate_draft
        original=state['originals'][cid]
        draft=validate_draft(original['card'],original['draft'],{b['role']:dict(account=b['account'])})
        asset=draft['entry_action']['orders'][0]['a']
        previous=parents[0] if after_exit else own[0] if own else None
        operation=('CANCEL_ENTRY_AFTER_EXIT' if after_exit else
                   'CANCEL_BEFORE_SWITCH' if own and own[0][0]!=desired else
                   'CANCEL_BEFORE_RESIZE' if own else 'PLACE_ONLY_EXIT')
        step=dict(card_id=cid,card_digest=b['card_digest'],account=b['account'],symbol=b['symbol'],
            role=b['role'],leg=previous[0] if previous else desired,desired_leg=desired,
            original_prices=deepcopy(b['prices']),observed_at_ms=snap['at_ms'],
            evidence_digest=life.digest(state['evidence']),quantity=life.text(q),
            old_oid=previous[1]['oid'] if previous else None,operation=operation)
        if previous:
            if previous[1]['state']!='ACTIVE':raise DispatchError('EXACT_ACTIVE_EXIT_HANDOFF_REQUIRED')
            step['wire_action']=dict(type='cancel',cancels=[dict(a=asset,o=int(step['old_oid']))])
        else:
            terminals={t['oid'] for t in snap['terminal_orders']}
            if any(oid not in terminals for leg in fence.EXITS for oid in b['orders'][leg]):
                raise DispatchError('ALL_PREVIOUS_EXITS_REQUIRE_FINALITY')
            px=b['prices']['stop' if desired=='STOP' else 'take_profit']
            from .filled_quantity_dispatch import precise
            precise(px,life.text(q),draft['size_decimals'])
            kind=(dict(trigger=dict(isMarket=True,triggerPx=px,tpsl='sl'))
                  if desired=='STOP' else dict(limit=dict(tif='Gtc')))
            cloid='0x'+life.digest([VERSION,state['bucket'],cid,desired,state['revision'],snap['at_ms']])[:32]
            step['wire_action']=dict(type='order',grouping='na',orders=[dict(
                a=asset,b=b['side']=='SHORT',p=px,s=life.text(q),r=True,t=kind,c=cloid)])
        step['intent_id']=life.digest([VERSION,step])
        candidates.append((-1 if after_exit else 0 if own else 1 if desired=='STOP' else 2,cid,step))
    if candidates:out.update(status='PROPOSED_SOFTWARE_ONLY',step=sorted(candidates)[0][2])
    return out


def _frozen_request(req):
    """Reconstruct the persisted wire intent; a label or recomputed row hash is not authority."""
    if (req.get('protocol')!=VERSION or req.get('domain')!='software' or req.get('nonce') is not None
            or type(req.get('attempts')) is not int or req['attempts'] not in (0,1)):
        raise DispatchError('EXCLUSIVE_REQUEST_DOMAIN_OR_ATTEMPT_INVALID')
    p=req['proposal']
    frozen=dict(domain='software',bucket=req['bucket'],account=p['account'],symbol=p['symbol'],
        revision=req['planning_revision'],originals=deepcopy(req['original_records']),
        bindings=deepcopy(req['original_evidence']['bindings']),evidence=deepcopy(req['original_evidence']),
        exclusive_exit_contracts=deepcopy(req['original_contracts']),pending=None)
    if plan(frozen,req['targets'],now_ms=req['prepared_at_ms'])['step']!=p:
        raise DispatchError('EXCLUSIVE_FROZEN_WIRE_CHANGED')
    return frozen


def _lookup(raw,request,now_ms):
    """Verify the exact persisted cloid and terms; never adopt caller-supplied IDs."""
    try:
        p=request['proposal'];wire=p['wire_action']['orders'][0]
        env=raw['order'];o=env['order'];oid=o['oid'];trigger=p['leg']=='STOP'
        if (raw['status']!='order' or type(oid) is not int or not 0<oid<2**64
                or env['status'] not in {'open','filled'}|venue_evidence.CANCELED|venue_evidence.REJECTED
                or not request['attempt_at_ms']<=life.moment(env['statusTimestamp'])<=now_ms
                or o['cloid']!=wire['c'] or o['coin']!=p['symbol']
                or o['side']!=('B' if wire['b'] else 'A') or o['reduceOnly'] is not True
                or o['isTrigger'] is not trigger or o.get('isPositionTpsl',False) is not False
                or o['orderType']!=('Stop Market' if trigger else 'Limit')
                or life.number(o['origSz'],positive=True)!=life.number(wire['s'])
                or life.number(o['limitPx'],positive=True)!=life.number(wire['p'])
                or trigger and life.number(o['triggerPx'],positive=True)!=life.number(wire['p'])):
            raise ValueError()
        known=(request.get('reply') or {}).get('oid')
        if known is not None and known!=str(oid):raise ValueError()
        return str(oid)
    except (KeyError,TypeError,ValueError,life.LifecycleError):
        raise DispatchError('EXCLUSIVE_ORDER_LOOKUP_NOT_VERIFIED') from None


def _candidate(value,req,raw,now_ms):
    """Temporary ownership used only to read/validate, committed after complete proof."""
    result=deepcopy(value);p=req['proposal'];b=next(b for b in result['bindings'] if b['card_id']==p['card_id'])
    if p['operation']!='PLACE_ONLY_EXIT':
        if raw is not None:raise DispatchError('CANCEL_CONFIRMATION_DOES_NOT_ADOPT_ORDER')
        return result,p['old_oid']
    oid=_lookup(raw,req,now_ms)
    if any(oid in x['orders'][leg] for x in result['bindings'] for leg in life.LEGS):
        raise DispatchError('EXCLUSIVE_ORDER_ALREADY_OWNED')
    b['orders'][p['leg']].append(oid)
    contracts=result.setdefault('exclusive_exit_contracts',{})
    if oid in contracts:raise DispatchError('EXCLUSIVE_ORDER_ALREADY_OWNED')
    contracts[oid]=dict(card_id=p['card_id'],leg=p['leg'],wire_order=deepcopy(p['wire_action']['orders'][0]),
        attempt_at_ms=req['attempt_at_ms'],request_id=req['request_id'])
    return result,oid


class Rehearsal:
    """Full durable allocation protocol; only disposable software storage, never a sender."""
    def __init__(self,store):
        if store.domain!='software' or store.journal._ci is not True:
            raise DispatchError('DISPOSABLE_PROTOCOL_STORE_REQUIRED')
        self.store=store

    def prepare(self,state,targets,*,now_ms):
        result=plan(state,targets,now_ms=now_ms)
        if result['step'] is None:raise DispatchError('NO_EXCLUSIVE_STEP_TO_PREPARE')
        step=result['step']
        def update(conn,value):
            if value['pending'] is not None:raise DispatchError('UNRESOLVED_REQUEST_NO_NEW_INTENT')
            if plan(value,targets,now_ms=now_ms)['step']!=step:raise DispatchError('EXCLUSIVE_PROPOSAL_CHANGED')
            rid=life.digest([VERSION,value['bucket'],value['revision']+1,step])
            value['pending']=value['last_request']=rid
            return dict(request_id=rid,bucket=value['bucket'],domain='software',protocol=VERSION,
                phase='PREPARED',proposal=deepcopy(step),targets=deepcopy(targets),
                original_evidence=deepcopy(value['evidence']),original_records=deepcopy(value['originals']),
                original_contracts=deepcopy(value.get('exclusive_exit_contracts',{})),
                planning_revision=value['revision'],prepared_at_ms=now_ms,attempt_at_ms=None,
                attempts=0,nonce=None,reply=None,observed_oid=None)
        return self.store.change(state['bucket'],state['revision'],'EXCLUSIVE_PREPARE',now_ms,update)

    def begin(self,state,*,now_ms):
        def update(conn,value):
            req=self.store.pending_record(conn,value)
            if (req is None or req.get('protocol')!=VERSION or req['phase']!='PREPARED'
                    or req['attempts']!=0 or req['nonce'] is not None):
                raise DispatchError('EXCLUSIVE_ATTEMPT_NOT_REPEATABLE')
            _frozen_request(req)
            if now_ms<req['prepared_at_ms']:raise DispatchError('EXCLUSIVE_CLOCK_MOVED_BACKWARDS')
            _read(value,now_ms)
            if (value['evidence']!=req['original_evidence'] or value['originals']!=req['original_records']
                    or value.get('exclusive_exit_contracts',{})!=req['original_contracts']):
                raise DispatchError('EXCLUSIVE_EVIDENCE_CHANGED_REPLAN_UNSENT')
            req.update(phase='OUTCOME_UNKNOWN',attempt_at_ms=now_ms,attempts=1)
            return req
        return self.store.change(state['bucket'],state['revision'],'EXCLUSIVE_ATTEMPT',now_ms,update)

    def observe(self,state,snapshot,*,now_ms):
        """Persist new fills without forgetting an unresolved request or freeing its capacity."""
        def update(conn,value):
            ev=dict(bindings=deepcopy(value['bindings']),snapshot=deepcopy(snapshot))
            _continues(value['evidence'],ev)
            _read({**value,'evidence':ev},now_ms)
            value['evidence']=ev
        return self.store.change(state['bucket'],state['revision'],'EXCLUSIVE_OBSERVATION',now_ms,update)

    def abort_unsent(self,state,*,now_ms):
        """Explicit local replanning only: never clear a possibly attempted cancellation."""
        def update(conn,value):
            req=self.store.pending_record(conn,value)
            if (req is None or req.get('protocol')!=VERSION or req['phase']!='PREPARED'
                    or req['attempts']!=0 or req['attempt_at_ms'] is not None
                    or req['nonce'] is not None or req['reply'] is not None):
                raise DispatchError('ONLY_NEVER_ATTEMPTED_EXCLUSIVE_REQUEST_CAN_ABORT')
            _frozen_request(req);_read(value,now_ms)
            req['phase']='ABORTED_UNSENT';value['pending']=None
            return req
        return self.store.change(state['bucket'],state['revision'],'EXCLUSIVE_ABORT_UNSENT',now_ms,update)

    def read_snapshot(self,state,reader,*,clock,elapsed,order_lookup=None):
        """Use the existing raw /info evidence collector, not invented normalized fills.

        Reader injection here is software-only. This tests production parsing but
        does not open any network connection or adopt an execution in a real DB.
        """
        if getattr(reader,'domain',None)!='software':
            raise DispatchError('SOFTWARE_EVIDENCE_READER_REQUIRED')
        candidate=deepcopy(state)
        if state.get('pending'):
            req=self.store.request(state['pending']);_frozen_request(req)
            if req['phase'] not in ('OUTCOME_UNKNOWN','ACK_UNVERIFIED') or req['attempt_at_ms'] is None:
                raise DispatchError('STARTED_EXCLUSIVE_REQUEST_REQUIRED_FOR_LOOKUP')
            candidate,_=_candidate(state,req,order_lookup,clock())
        elif order_lookup is not None:
            raise DispatchError('UNREQUESTED_EXCLUSIVE_ORDER_LOOKUP')
        _,plain=_contracts(candidate)
        ev=dict(bindings=candidate['bindings'],snapshot=state['evidence']['snapshot'])
        return venue_evidence.collect(ev,reader,clock=clock,elapsed=elapsed,
                                      plain_take_profit_oids=plain)['snapshot']

    def confirm(self,state,snapshot,*,now_ms,order_lookup=None):
        """Commit ownership and retirement atomically, after exact fresh venue evidence."""
        def update(conn,value):
            req=self.store.pending_record(conn,value)
            if (req is None or req.get('protocol')!=VERSION or req['attempt_at_ms'] is None
                    or req['phase'] not in ('OUTCOME_UNKNOWN','ACK_UNVERIFIED')):
                raise DispatchError('UNCONFLICTED_EXCLUSIVE_ATTEMPT_REQUIRED')
            _frozen_request(req)
            if (life.moment(snapshot['at_ms'])<=max(req['attempt_at_ms'],req.get('updated_at_ms',0))
                    or value['originals']!=req['original_records']):
                raise DispatchError('NEW_EXCLUSIVE_OBSERVATION_REQUIRED')
            candidate,oid=_candidate(value,req,order_lookup,now_ms);p=req['proposal']
            ev=dict(bindings=candidate['bindings'],snapshot=deepcopy(snapshot))
            _continues(req['original_evidence'],ev);_continues(value['evidence'],ev)
            candidate['evidence']=ev
            bs,snap,rows,view=_read(candidate,now_ms)
            if p['operation']!='PLACE_ONLY_EXIT':
                proof=fence.confirm_cancel(value,req,snap,bs,now_ms=now_ms)
            else:
                wire=p['wire_action']['orders'][0]
                found=[o for group in ('open_orders','terminal_orders') for o in snap[group] if o['oid']==oid]
                if len(found)!=1:raise DispatchError('PLACED_EXCLUSIVE_ORDER_NOT_OBSERVED')
                record=found[0];status=order_lookup['order']['status'];raw=order_lookup['order']['order']
                unique={f['fill_id']:f for f in snap['fills'] if f['oid']==oid}
                with localcontext() as ctx:
                    ctx.prec=80
                    filled=sum((life.number(f['quantity']) for f in unique.values()),Decimal(0))
                    if any(not req['attempt_at_ms']<=f['at_ms']<=snap['at_ms'] for f in unique.values()):
                        raise DispatchError('EXCLUSIVE_FILL_TIME_NOT_BOUND_TO_ATTEMPT')
                    if any(o['oid']==oid for o in snap['open_orders']):
                        if (status!='open' or life.number(record['quantity'])!=life.number(raw['sz'])
                                or filled+life.number(record['quantity'])!=life.number(wire['s'])):
                            raise DispatchError('EXCLUSIVE_PLACEMENT_QUANTITY_NOT_RECONCILED')
                    else:
                        expected=('FILLED' if status=='filled' else 'CANCELED' if status in venue_evidence.CANCELED else 'REJECTED')
                        if (status=='open' or record['state']!=expected
                                or record['at_ms']!=order_lookup['order']['statusTimestamp']
                                or life.number(record['filled_quantity'])!=filled or filled>life.number(wire['s'])
                                or expected=='FILLED' and filled!=life.number(wire['s'])
                                or expected=='REJECTED' and filled!=0
                                or any(f['at_ms']>record['at_ms'] for f in unique.values())):
                            raise DispatchError('EXCLUSIVE_PLACEMENT_QUANTITY_NOT_RECONCILED')
                conn.execute(f'''INSERT INTO {SCHEMA}.ownership VALUES(%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING''',(value['account'],oid,wire['c'],p['card_id'],p['leg'],req['request_id']))
                row=conn.execute(f'SELECT card_id,leg,request_id,cloid FROM {SCHEMA}.ownership WHERE account=%s AND oid=%s',
                                 (value['account'],oid)).fetchone()
                if row!=(p['card_id'],p['leg'],req['request_id'],wire['c']):
                    raise DispatchError('EXCLUSIVE_PERSISTED_OWNERSHIP_COLLISION')
                proof=dict(exact_order_observed=True,native_oco_or_pre_cancel_isolation_proven=False)
            value.update(bindings=bs,evidence=ev,pending=None,
                         exclusive_exit_contracts=candidate.get('exclusive_exit_contracts',{}))
            req.update(phase='OBSERVED',observed_oid=oid,confirmation=proof,
                remaining_quantity=rows[p['card_id']]['remaining_quantity'],
                original_lifecycle_requires_review=view['needs_review'])
            return req
        return self.store.change(state['bucket'],state['revision'],'EXCLUSIVE_OBSERVED',now_ms,update)
