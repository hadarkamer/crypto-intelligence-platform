"""Durable Testnet-only loss-of-stop containment, disabled unless explicitly enabled.

A separate request lane can reduce confirmed exposure while a normal stop reply
is uncertain. All lanes share bucket locks and agent nonces. No request is ever
replayed. IOC exits are recorded as STOP exits only AFTER exact intent identity,
terminal status and two comparable public fill/position observations agree.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import json
import threading

from . import card_lifecycle as life, filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchError, SCHEMA
from .card_lifecycle_store import _continues
from .dispatch_concurrency import market_lane

VERSION = 'testnet-emergency-close-v1'
APPROVAL = 'approved_testnet_v1'
DEADLINE_MS = 5000  # Testnet experiment setting, not a Mainnet latency guarantee.
SLIPPAGE = Decimal('0.01')
LOCK = 1729048241
MAX_REQUESTS = 128
DONE = ('OBSERVED', 'EXPIRED_NO_PUBLIC_ORDER', 'EXPIRED_NO_PUBLIC_TERMINAL', 'ABORTED_UNSENT')
_health = dict(running=False, last_pass_at_ms=None, last_status='DISABLED')
_thread = None
_stop_event = None
_lock = threading.Lock()


def fence(conn, state, operation):
    """Called under the common global+bucket locks before normal reserve/begin."""
    if state.get('emergency') is not None:
        raise DispatchError('EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE')
    if operation == 'ENTRY' and conn.execute(
            f"SELECT 1 FROM {SCHEMA}.buckets WHERE value->'emergency' IS NOT NULL LIMIT 1").fetchone():
        raise DispatchError('EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY')


def view(state, now_ms):
    ids = [r['observed_oid'] for r in (state.get('emergency') or {}).get('requests', [])
           if r['proposal']['operation'] == 'EMERGENCY_CLOSE'
           and r['phase'] == 'OBSERVED' and r.get('observed_oid')]
    return life.review(state['bindings'], state['evidence']['snapshot'], now_ms=now_ms,
                       emergency_stop_oids=ids)


def trigger(state, *, now_ms, mark=None):
    """Oldest still-uncovered fill tranche; a new partial fill cannot reset it."""
    if not state['bindings'] or state['evidence'] is None:
        return None
    snap = state['evidence']['snapshot']
    report = life.review(state['bindings'], snap, now_ms=snap['at_ms'])
    for binding in state['bindings']:
        row = next(v for v in report['cards'] if v['card_id'] == binding['card_id'])
        remaining = life.number(row['remaining_quantity'], signed=True)
        covered = life.number(row['stop_quantity_observed'])
        if remaining <= 0 or covered >= remaining:
            continue
        entries = sorted((f for f in snap['fills'] if f['oid'] in binding['orders']['ENTRY']),
                         key=lambda f: (f['at_ms'], f['fill_id']))
        skip = life.number(row['exit_quantity']) + covered
        oldest = None
        for fill in entries:
            quantity = life.number(fill['quantity'])
            if skip >= quantity:
                skip -= quantity
                continue
            oldest = fill['at_ms']
            break
        if oldest is None or now_ms < oldest:
            raise DispatchError('UNPROTECTED_FILL_TIMELINE_INVALID')
        crossed = False
        if mark is not None:
            px, stop = life.number(mark, positive=True), life.number(binding['prices']['stop'])
            crossed = px <= stop if binding['side'] == 'LONG' else px >= stop
        if crossed or now_ms-oldest >= DEADLINE_MS:
            return dict(card_id=binding['card_id'],
                        reason='STOP_LEVEL_PASSED_UNPROTECTED' if crossed else 'STOP_VERIFICATION_DEADLINE',
                        uncovered_since_ms=oldest, uncovered_quantity=life.text(remaining-covered))
    return None


def close_price(mark, decimals, *, buy):
    """Aggressive IOC within 1% of the sampled mark, rounded towards the mark."""
    px = life.number(mark, positive=True) * (1+SLIPPAGE if buy else 1-SLIPPAGE)
    exponent = max(-(6-decimals), px.adjusted()-4) if px < Decimal('100000') else 0
    step = Decimal(1).scaleb(exponent)
    px = px.quantize(step, rounding=ROUND_DOWN if buy else ROUND_UP)
    mark_px=life.number(mark,positive=True)
    if px <= 0 or (px < mark_px if buy else px > mark_px):
        raise DispatchError('EMERGENCY_PRICE_NOT_REPRESENTABLE')
    return life.text(px)


def safe_card(state, cid, now_ms):
    """Close only a uniquely owned remainder reconciled with the net position."""
    report = view(state, now_ms)
    if report['bucket_issues']:
        raise DispatchError('EMERGENCY_POSITION_OR_HISTORY_NOT_VERIFIED')
    row = next(v for v in report['cards'] if v['card_id'] == cid)
    allowed = {'STOP_COVERAGE_MISSING', 'TAKE_PROFIT_COVERAGE_MISSING',
               'STOP_EXCEEDS_CARD_REMAINDER', 'TAKE_PROFIT_EXCEEDS_CARD_REMAINDER',
               'FLAT_WITH_WORKING_ORDERS', 'BOTH_EXIT_LEGS_FILLED_REVIEW'}
    if any(set(v['issues'])-allowed for v in report['cards']):
        raise DispatchError('EMERGENCY_CARD_QUANTITY_NOT_VERIFIED')
    active = [v for v in report['cards'] if life.number(v['remaining_quantity'], signed=True) > 0]
    if active and (len(active) != 1 or active[0]['card_id'] != cid):
        raise DispatchError('EMERGENCY_SHARED_MARKET_NOT_EXCLUSIVE')
    binding = next(b for b in state['bindings'] if b['card_id'] == cid)
    original = state['originals'][cid]['card']
    rebound = life.binding_from_card(original, state['account'],
        {binding['role']: {'account': state['account']}}, binding['orders'])
    if rebound != binding:
        raise DispatchError('EMERGENCY_IMMUTABLE_CARD_CHANGED')
    return binding, row


def record_timing(state,request,lookup,now_ms):
    """Keep venue activation separate from completed public observation.

    Never use acknowledgement time. Pre-existing stops without their exact
    creation lookup retain unknown venue latency, not an invented measurement.
    Observation precedes the database commit; it is not a persistence timer.
    """
    if not state['bindings'] or state['evidence'] is None:
        return
    report=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=now_ms)
    if report['bucket_issues']:
        return
    timing=state.setdefault('protection_timing',{})
    snap=state['evidence']['snapshot']
    for binding in state['bindings']:
        entries=[f for f in snap['fills'] if f['oid'] in binding['orders']['ENTRY']]
        if not entries:
            continue
        cid=binding['card_id'];first=min(f['at_ms'] for f in entries)
        armed=state.get('entry_timing_armed',{}).get(cid)
        if armed is None or first<armed:
            continue
        item=timing.setdefault(cid,dict(first_fill_at_ms=first,first_fill_observed_at_ms=now_ms,
            timing_semantics='public_observation_before_commit'))
        row=next(v for v in report['cards'] if v['card_id']==cid)
        remaining=life.number(row['remaining_quantity'],signed=True)
        if (remaining<=0 or life.number(row['stop_quantity_observed'])!=remaining
                or set(row['issues'])-{'TAKE_PROFIT_COVERAGE_MISSING','TAKE_PROFIT_EXCEEDS_CARD_REMAINDER'}
                or 'stop_observed_at_ms' in item or 'stop_verified_at_ms' in item):
            continue
        activated=None
        if (request and request['proposal']['card_id']==cid and request['proposal']['leg']=='STOP'
                and request['proposal']['operation'] in ('CREATE_EXIT','MODIFY_EXIT')
                and isinstance(lookup,dict) and lookup.get('status')=='order'
                and lookup['order'].get('status')=='open'
                and str(lookup['order']['order']['oid']) in binding['orders']['STOP']):
            stamp=life.moment(lookup['order']['statusTimestamp'])
            if first<=stamp<=now_ms:
                activated=stamp
        item.update(stop_observed_at_ms=now_ms,stop_public_status_at_ms=activated,
                    quantity_at_stop_verification=life.text(remaining),
                    fill_to_stop_public_ms=activated-first if activated is not None else None,
                    fill_to_stop_observation_ms=now_ms-first)


class Controller:
    def __init__(self, normal, venue=None):
        self.normal, self.store = normal, normal.store
        self.venue = venue or Venue(normal.venue.env)
        self.venue.store = self.store
        if self.venue.domain != self.store.domain:
            raise DispatchError('SOFTWARE_AND_ACCOUNT_STORAGE_MUST_NOT_MIX')

    def _save(self, state, event, update):
        return self.store.change(state['bucket'], state['revision'], event,
                                 self.venue.now(), update)

    def latch(self, state, cause):
        def update(conn, current):
            if current.get('emergency') is not None:
                raise DispatchError('EMERGENCY_ALREADY_LATCHED_RELOAD')
            current['emergency'] = dict(version=VERSION, phase='ACTIVE',
                latched_at_ms=self.venue.now(), requests=[], pending_close=None,
                pending_cancel=None, **deepcopy(cause))
            return None
        return self._save(state, 'EMERGENCY_LATCHED_NEW_ENTRIES_BLOCKED', update)

    def _refresh_normal(self, state):
        """Resolve an accepted delayed stop; unknown replies do not bar the close lane."""
        try:
            return self.normal.refresh(state['bucket'])
        except (DispatchError, life.LifecycleError) as exc:
            if str(exc) not in {'OUTCOME_UNRESOLVED_NO_NEW_REQUEST', 'CONFLICT_REQUIRES_REVIEW',
                    'REJECTION_HISTORY_WINDOW_REQUIRES_REVIEW', 'REJECTION_FILL_FOUND_NO_RELEASE'}:
                raise
            state = self.store.load(state['bucket'])
            if not state['bindings'] or state['evidence'] is None:
                raise
            evidence = self.venue.collect(state['evidence'])
            def update(conn, current):
                _continues(current['evidence'], evidence)
                current['evidence'] = dict(bindings=current['bindings'], snapshot=evidence['snapshot'])
                return None
            return self._save(state, 'EMERGENCY_PUBLIC_CHECKPOINT', update)

    def _retire_normal(self,state):
        if not state['pending']:
            return state
        request=self.store.request(state['pending'])
        if request['phase']=='PREPARED' and request['attempts']==0:
            def unsent(conn,current):
                r=self.store.pending_record(conn,current)
                if r!=request:
                    raise DispatchError('EMERGENCY_NORMAL_REQUEST_CHANGED')
                r.update(phase='ABORTED_UNSENT',abort_reason='EMERGENCY_SUPERSEDED_UNSENT')
                current['pending']=None
                return r
            return self._save(state,'EMERGENCY_NORMAL_UNSENT_INTENT_RETIRED',unsent)
        if (request['phase'] not in ('OUTCOME_UNKNOWN','ACK_UNVERIFIED','REJECTED')
                or request['proposal']['leg'] not in ('STOP','TAKE_PROFIT')
                or request['proposal']['action']['type'] not in ('order','batchModify')
                or request['attempt_at_ms'] is None
                or self.venue.now()-request['attempt_at_ms']<120000):
            return state
        cloid=dispatch.requested_order(request['proposal']['action'])['c']
        if any(self.venue.lookup(state['account'],cloid)!={'status':'unknownOid'} for _ in range(2)):
            return state
        evidence=self.venue.collect(state['evidence'])
        safe_card({**state,'evidence':evidence},state['emergency']['card_id'],self.venue.now())
        def expired(conn,current):
            r=self.store.pending_record(conn,current)
            if r!=request:
                raise DispatchError('EMERGENCY_NORMAL_REQUEST_CHANGED')
            if current['evidence']['snapshot']!=evidence['snapshot']:
                _continues(current['evidence'],evidence)
                current['evidence']=dict(bindings=current['bindings'],snapshot=evidence['snapshot'])
            r.update(phase='OBSERVED',terminal_state='NO_PUBLIC_ORDER_AFTER_SIGNATURE_EXPIRY',
                     observed_at_ms=evidence['snapshot']['at_ms'])
            current['pending']=None
            return r
        return self._save(state,'EMERGENCY_UNKNOWN_NORMAL_EXIT_RECONCILED_AFTER_EXPIRY',expired)

    def _requests(self, state):
        return state['emergency']['requests']

    def _resolve_close(self, state):
        eid = state['emergency']['pending_close']
        if eid is None:
            return state
        request = next(r for r in self._requests(state) if r['request_id'] == eid)
        raw = self.venue.lookup(state['account'], request['proposal']['action']['orders'][0]['c'])
        if raw == {'status':'unknownOid'}:
            # A response timeout never authorizes another IOC. Even a rejection
            # waits beyond signature expiry for two independent no-order reads.
            if self.venue.now()-request['attempt_at_ms'] < 120000:
                raise DispatchError('EMERGENCY_CLOSE_OUTCOME_UNKNOWN_NO_RESEND')
            if self.venue.lookup(state['account'], request['proposal']['action']['orders'][0]['c']) != raw:
                raise DispatchError('EMERGENCY_ORDER_LOOKUP_CHANGED')
            evidence = self.venue.collect(state['evidence'])
            safe_card({**state, 'evidence': evidence}, state['emergency']['card_id'], self.venue.now())
            def expired(conn, current):
                r = next(r for r in self._requests(current) if r['request_id'] == eid)
                r.update(phase='EXPIRED_NO_PUBLIC_ORDER', observed_at_ms=evidence['snapshot']['at_ms'])
                current['emergency']['pending_close'] = None
                current['evidence'] = dict(bindings=current['bindings'], snapshot=evidence['snapshot'])
                return None
            return self._save(state, 'EMERGENCY_EXPIRED_INTENT_RECONCILED_NO_REPLAY', expired)
        oid = dispatch.identity(raw, request, self.venue.now())
        bindings = deepcopy(state['bindings'])
        binding = next(b for b in bindings if b['card_id'] == request['proposal']['card_id'])
        if oid not in binding['orders']['STOP']:
            binding['orders']['STOP'].append(oid)
        evidence = self.venue.collect(dict(bindings=bindings, snapshot=state['evidence']['snapshot']))
        terminal = next((o for o in evidence['snapshot']['terminal_orders'] if o['oid']==oid), None)
        if terminal is None or evidence['snapshot']['at_ms'] <= request['attempt_at_ms']:
            raise DispatchError('EMERGENCY_TERMINAL_AND_FILLS_NOT_VERIFIED')
        def resolved(conn, current):
            _continues(current['evidence'], evidence)
            r = next(r for r in self._requests(current) if r['request_id'] == eid)
            r.update(phase='OBSERVED', observed_oid=oid,
                     observed_at_ms=evidence['snapshot']['at_ms'],
                     terminal_state=terminal['state'], filled_quantity=terminal['filled_quantity'])
            current['bindings'] = bindings
            current['evidence'] = dict(bindings=bindings, snapshot=evidence['snapshot'])
            current['emergency']['pending_close'] = None
            return None
        return self._save(state, 'EMERGENCY_IOC_PUBLIC_FILL_RECONCILED', resolved)

    def _resolve_cancel(self, state):
        eid = state['emergency']['pending_cancel']
        if eid is None:
            return state
        request = next(r for r in self._requests(state) if r['request_id']==eid)
        terminal = next((o for o in state['evidence']['snapshot']['terminal_orders']
                         if o['oid']==request['proposal']['old_oid']), None)
        if terminal is None:
            # Only a fresh two-pass checkpoint of the same still-active owned
            # order after signature expiry may release this cancellation lane.
            if (self.venue.now()-request['attempt_at_ms']<120000
                    or state['evidence']['snapshot']['at_ms']<=request['attempt_at_ms']+15000
                    or not any(o['oid']==request['proposal']['old_oid']
                               for o in state['evidence']['snapshot']['open_orders'])):
                return state
            def expired(conn,current):
                r=next(r for r in self._requests(current) if r['request_id']==eid)
                r.update(phase='EXPIRED_NO_PUBLIC_TERMINAL',observed_at_ms=current['evidence']['snapshot']['at_ms'])
                current['emergency']['pending_cancel']=None
                return None
            return self._save(state,'EMERGENCY_CANCEL_EXPIRED_STILL_ACTIVE_RECONCILED',expired)
        if terminal['at_ms'] < request['proposal']['observed_at_ms']:
            raise DispatchError('EMERGENCY_CANCEL_TERMINAL_TIME_INVALID')
        def resolved(conn, current):
            r = next(r for r in self._requests(current) if r['request_id']==eid)
            r.update(phase='OBSERVED', observed_at_ms=current['evidence']['snapshot']['at_ms'],
                     terminal_state=terminal['state'], cancellation_caused_terminal_state=False)
            current['emergency']['pending_cancel'] = None
            return None
        return self._save(state, 'EMERGENCY_OWNED_ORDER_TERMINAL_RECONCILED', resolved)

    def proposal(self, state, *, metadata=None, sample=None):
        emergency = state['emergency']; cid = emergency['card_id']; now = self.venue.now()
        binding, row = safe_card(state, cid, now)
        snapshot = state['evidence']['snapshot']
        index, decimals = dispatch.asset(
            self.venue.metadata() if metadata is None else metadata, state['symbol'])
        quantity = life.number(row['remaining_quantity'], signed=True)
        requests = self._requests(state)
        attempted_cancels = {r['proposal']['old_oid'] for r in requests
                             if r['proposal']['operation']=='EMERGENCY_CANCEL' and r['phase'] not in DONE}
        entry_ids = set(binding['orders']['ENTRY'])
        owned = {oid for leg in life.LEGS for oid in binding['orders'][leg]}
        candidates = [o for o in snapshot['open_orders'] if o['oid'] in owned
                      and (o['oid'] in entry_ids or quantity==0)
                      and o['oid'] not in attempted_cancels]
        candidates.sort(key=lambda o: (o['oid'] not in entry_ids, int(o['oid'])))
        if candidates and emergency['pending_cancel'] is None:
            order = candidates[0]; operation='EMERGENCY_CANCEL'
            old_oid=order['oid']; q='0'
            leg = next(leg for leg in life.LEGS if old_oid in binding['orders'][leg])
            action=dict(type='cancel', cancels=[dict(a=index, o=int(old_oid))])
            sample=None
        elif quantity>0 and emergency['pending_close'] is None:
            operation='EMERGENCY_CLOSE'; old_oid=None; leg='STOP'; q=life.text(quantity)
            if sample is None:
                sample = self.venue.sample(state['account'], state['symbol'])
            if not 0<=self.venue.now()-sample['at_ms']<=5000:
                raise DispatchError('EMERGENCY_PRICE_SAMPLE_EXPIRED')
            price=close_price(sample['mark_price'], decimals, buy=binding['side']=='SHORT')
            dispatch.precise(price, q, decimals)
            cloid='0x'+life.digest([VERSION,state['bucket'],cid,len(requests)+1])[:32]
            action=dict(type='order', grouping='na', orders=[dict(a=index,
                b=binding['side']=='SHORT', p=price, s=q, r=True,
                t=dict(limit=dict(tif='Ioc')), c=cloid)])
        else:
            return None
        return dict(version=VERSION, card_id=cid, role=binding['role'], account=state['account'],
                    symbol=state['symbol'], bucket=state['bucket'], operation=operation, leg=leg,
                    action=action, quantity=q, old_oid=old_oid,
                    observed_at_ms=snapshot['at_ms'], basis=life.digest(state['evidence']),
                    asset_index=index, size_decimals=decimals, sample=sample)

    def _begin(self, state, proposal):
        self.venue.authorize(state, proposal)
        now = self.venue.now()
        def begin(conn, current):
            if len(self._requests(current)) >= MAX_REQUESTS:
                raise DispatchError('EMERGENCY_ACTION_BUDGET_REQUIRES_REVIEW')
            if current['emergency']['phase'] != 'ACTIVE' or proposal['basis']!=life.digest(current['evidence']):
                raise DispatchError('EMERGENCY_PLAN_CHANGED_NO_SEND')
            safe_card(current, proposal['card_id'], now)
            if self.proposal_without_io(current, proposal) is not True:
                raise DispatchError('EMERGENCY_WIRE_NOT_VERIFIED')
            key = 'pending_close' if proposal['operation']=='EMERGENCY_CLOSE' else 'pending_cancel'
            if current['emergency'][key] is not None:
                raise DispatchError('EMERGENCY_PENDING_REQUEST_NO_REPEAT')
            route=self.normal.routes[proposal['role']]
            agent=life.address(route['agent'])
            nonce=conn.execute(f'''INSERT INTO {SCHEMA}.nonces VALUES(%s,%s)
                ON CONFLICT(agent) DO UPDATE SET nonce=GREATEST({SCHEMA}.nonces.nonce+1,EXCLUDED.nonce)
                RETURNING nonce''',(agent,now)).fetchone()[0]
            if nonce>now+1000:
                raise DispatchError('NONCE_CLOCK_REQUIRES_REVIEW')
            eid=life.digest([VERSION,current['bucket'],len(self._requests(current))+1,proposal])
            request=dict(request_id=eid, domain=self.store.domain, bucket=current['bucket'],
                         phase='OUTCOME_UNKNOWN', proposal=deepcopy(proposal), attempts=1,
                         nonce=nonce, prepared_at_ms=now, attempt_at_ms=now,
                         reply=None, observed_oid=None)
            current['emergency']['requests'].append(request)
            current['emergency'][key]=eid
            return None
        # Capture the exact acknowledged commit, not a later load which another
        # worker may already have advanced. An uncertain COMMIT returns nothing
        # and therefore never grants this process a sender token.
        state,_=self.store.change(state['bucket'],state['revision'],
            'EMERGENCY_ATTEMPT_BEGUN',now,begin,_return_committed_request=True)
        return state, deepcopy(self._requests(state)[-1])

    @staticmethod
    def proposal_without_io(state, proposal):
        binding,row=safe_card(state,proposal['card_id'],proposal['observed_at_ms'])
        if (proposal['version']!=VERSION or proposal['account']!=state['account']
                or proposal['symbol']!=state['symbol'] or proposal['role']!=binding['role']
                or proposal['bucket']!=state['bucket'] or proposal['basis']!=life.digest(state['evidence'])
                or proposal['observed_at_ms']!=state['evidence']['snapshot']['at_ms']):
            return False
        index,decimals=proposal['asset_index'],proposal['size_decimals']
        draft=state['originals'][proposal['card_id']]['draft']
        if index!=draft['entry_action']['orders'][0]['a'] or decimals!=draft['size_decimals']:
            return False
        if type(index) is not int or not 0<=index<10000 or type(decimals) is not int or not 0<=decimals<=6:
            return False
        if proposal['operation']=='EMERGENCY_CLOSE':
            sample=proposal['sample']
            if not isinstance(sample,dict) or set(sample)!={'mark_price','at_ms'}:
                return False
            q=life.text(life.number(row['remaining_quantity'],positive=True))
            price=close_price(sample['mark_price'],decimals,buy=binding['side']=='SHORT')
            dispatch.precise(price,q,decimals)
            cloid='0x'+life.digest([VERSION,state['bucket'],proposal['card_id'],len(state['emergency']['requests'])+1])[:32]
            # Before send the persisted request already occupies the last slot.
            matching=[r for r in state['emergency']['requests'] if r['proposal']==proposal]
            if matching:
                cloid=matching[0]['proposal']['action']['orders'][0]['c']
            expected=dict(type='order',grouping='na',orders=[dict(a=index,b=binding['side']=='SHORT',
                p=price,s=q,r=True,t=dict(limit=dict(tif='Ioc')),c=cloid)])
            return (proposal['action']==expected and proposal['quantity']==q
                    and proposal['leg']=='STOP' and proposal['old_oid'] is None)
        if proposal['operation']=='EMERGENCY_CANCEL':
            oid=proposal['old_oid']
            expected=dict(type='cancel',cancels=[dict(a=index,o=int(oid))])
            return (proposal['action']==expected and oid in binding['orders'][proposal['leg']]
                and any(o['oid']==oid for o in state['evidence']['snapshot']['open_orders'])
                and proposal['quantity']=='0' and proposal['sample'] is None)
        return False

    @market_lane
    def cycle(self, bucket, *, send=False):
        state=self.store.load(bucket)
        # Static metadata precedes quantity reconciliation. The live price is
        # obtained AFTER the final checkpoint, so a slow collection cannot age
        # a prefetched price before planning. Neither timestamp is relabeled:
        # a slow collection or price read still fails its five-second bound.
        sample=None;sample_basis=None
        try:
            metadata=self.venue.metadata()
        except Exception:
            # A failed independent pre-read still freezes entries from known
            # uncovered fills. It never authorizes a close using old quantities.
            state=self.store.load(bucket)
            cause=trigger(state,now_ms=self.venue.now())
            if cause and state.get('emergency') is None and send:
                self.latch(state,cause)
            raise
        before_revision=state['revision']
        if state.get('emergency') is not None:
            state=self._resolve_close(state)
        try:
            if state['revision']==before_revision:
                state=self._refresh_normal(state)
        except (DispatchError, life.LifecycleError):
            # Freeze new entries using already stored fill evidence. Never send
            # a close from that old evidence; a fresh verified checkpoint is required.
            state=self.store.load(bucket)
            cause=trigger(state,now_ms=self.venue.now())
            if cause and state.get('emergency') is None and send:
                self.latch(state,cause)
            raise
        if state.get('emergency') is None:
            try:
                sample=self.venue.sample(state['account'],state['symbol']) if state['bindings'] else None
            except Exception:
                cause=trigger(state,now_ms=self.venue.now())
                if cause and send:self.latch(state,cause)
                raise
            sample_basis=life.digest(state['evidence'])
            fresh_sample=(sample is not None and 0<=self.venue.now()-sample['at_ms']<=5000)
            cause=trigger(state,now_ms=self.venue.now(), mark=sample['mark_price'] if fresh_sample else None)
            if cause is None:
                return dict(status='STOP_OBSERVED_OR_NO_EXPOSURE',order_requests_sent=0)
            if not send:
                return dict(status='EMERGENCY_PREVIEW',cause=cause,order_requests_sent=0)
            state=self.latch(state,cause)
        state=self._retire_normal(state)
        state=self._resolve_cancel(state)
        if sample_basis!=life.digest(state['evidence']):
            sample=None
        proposal=self.proposal(state,metadata=metadata,sample=sample)
        if proposal is None:
            report=view(state,self.venue.now())
            row=next(r for r in report['cards'] if r['card_id']==state['emergency']['card_id'])
            if row['closure_verified'] and not state['emergency']['pending_close'] and not state['emergency']['pending_cancel'] and not state['pending']:
                if state['emergency']['phase']!='CLOSED_VERIFIED':
                    def closed(conn,current):
                        current['emergency'].update(phase='CLOSED_VERIFIED', closed_at_ms=current['evidence']['snapshot']['at_ms'])
                        return None
                    state=self._save(state,'EMERGENCY_CLOSURE_VERIFIED_CIRCUIT_REMAINS_LATCHED',closed)
            return dict(status=('FLAT_AWAITING_ORDER_FINALITY' if row['remaining_quantity']=='0' and state['emergency']['phase']!='CLOSED_VERIFIED' else state['emergency']['phase']),
                        card_id=state['emergency']['card_id'],remaining_quantity=row['remaining_quantity'],order_requests_sent=0)
        if not send:
            return dict(status='EMERGENCY_PREVIEW',proposal=proposal,order_requests_sent=0)
        state,request=self._begin(state,proposal)
        before=self.venue.sent
        try:
            raw=self.venue.send(request)
            route=self.normal.routes[proposal['role']]
            reply=dispatch.normalized_reply(raw,proposal['action']['type'],account=state['account'],agent=route['agent'])
        except Exception:
            reply=dict(state='OUTCOME_UNKNOWN',code=None,oid=None)
        def replied(conn,current):
            r=next(r for r in self._requests(current) if r['request_id']==request['request_id'])
            r['reply']=reply
            return None
        self._save(state,'EMERGENCY_REPLY_NOT_FILL_PROOF',replied)
        return dict(status=reply['state'],operation=proposal['operation'],card_id=proposal['card_id'],
                    order_requests_sent=self.venue.sent-before)


class Venue(dispatch.TestnetVenue):
    def authorize(self,state,proposal):
        env=self.env
        if (env.get('HL_TESTNET_EMERGENCY_CLOSE')!=APPROVAL
                or env.get('RENDER_SERVICE_ID')!=dispatch.roles.SERVICE
                or env.get('HL_TESTNET_RUNTIME_MODE')!='long_stream_testnet_v1'
                or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION')!='disabled'
                or env.get('HL_TESTNET_FILLED_DISPATCH')!='approved_long_stream_v1'
                or env.get('HL_TESTNET_LONG_STREAM')!='approved_alerts_v1'
                or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY')!=dispatch.AFTER_EXIT
                or env.get('HL_TESTNET_SAFETY_PIPELINE') or env.get('HL_TESTNET_CARD_SYNC')):
            raise DispatchError('EMERGENCY_TESTNET_NOT_AUTHORIZED')
        binding,_=safe_card(state,proposal['card_id'],self.now())
        route=dispatch.roles.route_for(env,binding['role'],state['account'])
        dispatch.roles.wallet_for_role(env,binding['role'],route['account'],route['agent'])
        if not Controller.proposal_without_io(state,proposal):
            raise DispatchError('EMERGENCY_WIRE_NOT_VERIFIED')
        if (proposal['operation']=='EMERGENCY_CLOSE'
                and not 0<=self.now()-proposal['sample']['at_ms']<=5000):
            raise DispatchError('EMERGENCY_PRICE_SAMPLE_EXPIRED')
        if not 0<=self.now()-proposal['observed_at_ms']<=5000:
            raise DispatchError('EMERGENCY_FINAL_QUANTITY_EVIDENCE_EXPIRED')
        return route

    def _gate(self,proposal,after_exit_policy):
        state=self.store.load(proposal['bucket'])
        emergency=state.get('emergency')
        if not emergency or emergency['version']!=VERSION or emergency['phase']!='ACTIVE':
            raise DispatchError('DURABLE_EMERGENCY_INCIDENT_REQUIRED')
        matches=[r for r in emergency['requests'] if r['proposal']==proposal and r['phase']=='OUTCOME_UNKNOWN' and r['attempts']==1]
        if len(matches)!=1 or proposal['basis']!=life.digest(state['evidence']):
            raise DispatchError('DURABLE_EMERGENCY_INTENT_CHANGED')
        return self.authorize(state,proposal)


def healthy(now_ms):
    with _lock:
        return (_health['running'] and _health['last_status']=='PASS_COMPLETE' and _health['last_pass_at_ms'] is not None
                and 0<=now_ms-_health['last_pass_at_ms']<=15000)


def health():
    with _lock:
        return deepcopy(_health)


def start(normal, streams, stop_event, *, only_bucket=None):
    global _thread,_stop_event
    env=normal.venue.env
    if env.get('HL_TESTNET_EMERGENCY_CLOSE','')=='':
        return False
    if env.get('HL_TESTNET_EMERGENCY_CLOSE')!=APPROVAL:
        raise DispatchError('EMERGENCY_APPROVAL_MODE_INVALID')
    if env.get('HL_TESTNET_LONG_ENTRY_ENABLED')!='false' or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED')!='false':
        raise DispatchError('EMERGENCY_RELEASE_REQUIRES_CONTINUOUS_ENTRIES_DISABLED')
    if only_bucket is not None:
        life.ident(only_bucket, r'[0-9a-f]{64}')
        scoped = normal.store.load(only_bucket)
        if scoped['account'] not in {route['account'] for _,route,*_ in streams}:
            raise DispatchError('EMERGENCY_SCOPE_ACCOUNT_NOT_SELECTED')
    controller=Controller(normal)
    def loop():
        with _lock:
            _health.update(running=True,last_status='STARTING',last_pass_at_ms=None)
        while not stop_event.is_set():
            status='PASS_COMPLETE'
            for _,route,*_ in streams:
                try:
                    for state in controller.store.for_account(route['account']):
                        if only_bucket is not None and state['bucket'] != only_bucket:
                            continue
                        from .long_stream_runtime import _unfinished,idle_flat
                        from datetime import datetime,timezone
                        if not _unfinished(state,datetime.fromtimestamp(controller.venue.now()/1000,timezone.utc)):
                            continue
                        if (not state.get('emergency') and not state['pending'] and state['evidence']
                                and controller.venue.now()-state['evidence']['snapshot']['at_ms']<5000
                                and trigger(state,now_ms=controller.venue.now()) is None
                                and not any(o['oid'] in b['orders']['ENTRY']
                                    for o in state['evidence']['snapshot']['open_orders'] for b in state['bindings'])):
                            continue
                        if not state.get('emergency') and idle_flat(state):
                            continue
                        if state.get('emergency',{}).get('phase')=='CLOSED_VERIFIED':
                            continue
                        # Reconcile entry/stop independently of the ordinary
                        # sweep. A normal lane stall cannot stop this worker.
                        before=controller.venue.sent
                        try:
                            result=controller.cycle(state['bucket'],send=True)
                            if result.get('operation')=='EMERGENCY_CANCEL':
                                controller.cycle(state['bucket'],send=True)
                            if result['status']!='STOP_OBSERVED_OR_NO_EXPOSURE':
                                print(json.dumps({'testnet_emergency_close':result},sort_keys=True),flush=True)
                        except Exception as exc:
                            from .long_stream_runtime import _safe_failure
                            status='RECONCILIATION_REQUIRED_NO_BLIND_RETRY'
                            print(json.dumps({'testnet_emergency_close':dict(status=status,
                                order_requests_sent=controller.venue.sent-before,**_safe_failure(exc))},sort_keys=True),flush=True)
                except Exception:
                    status='STORAGE_OR_SCAN_UNAVAILABLE'
            with _lock:
                _health.update(last_pass_at_ms=controller.venue.now(),last_status=status)
            stop_event.wait(1)
        with _lock:
            _health['running']=False
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop_event=stop_event
        _thread=threading.Thread(target=loop,daemon=True,name='testnet-emergency-stop-supervisor')
        _thread.start()
    return True


def stop_supervisor(stop_event, *, timeout=5):
    """Stop and join the caller's supervisor; a timeout is not verified shutdown."""
    if isinstance(timeout,bool) or not isinstance(timeout,(int,float)) or not 0<=timeout<=30:
        raise DispatchError('EMERGENCY_SHUTDOWN_TIMEOUT_INVALID')
    with _lock:
        thread=_thread
        if thread is not None and _stop_event is not stop_event:
            raise DispatchError('EMERGENCY_SUPERVISOR_STOP_EVENT_MISMATCH')
    stop_event.set()
    if thread is None:
        return True
    if thread is threading.current_thread():
        return False
    thread.join(timeout)
    return not thread.is_alive()
