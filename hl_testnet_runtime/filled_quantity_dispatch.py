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
import time

from . import card_lifecycle as life, filled_quantity_exits as selected
from . import card_exit_recovery as recovery, card_sync_evidence as evidence
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .trade_card_store import CardStore
from . import checks, two_account_execution as roles

VERSION = 'connected-filled-quantity-dispatch-v1'
AFTER_EXIT = 'cancel_remainder_after_exit_v1'
HOST = 'api.hyperliquid-testnet.xyz'


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


def normalized_reply(raw, kind):
    if kind != 'cancel':
        return recovery.classify_reply(raw,['ENTRY'])['ENTRY']
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


def identity(raw, request, now_ms):
    """Verify a venue lookup of the PERSISTED cloid, not a caller's three IDs."""
    if not isinstance(raw,dict) or raw.get('status')!='order':
        raise DispatchError('OUTCOME_UNRESOLVED_NO_NEW_REQUEST')
    env=raw.get('order');o=env.get('order') if isinstance(env,dict) else None
    p=request['proposal'];expected=p['action']['orders'][0]
    if not isinstance(o,dict): raise DispatchError('INVALID_ORDER_LOOKUP')
    oid=o.get('oid');stamp=life.moment(env.get('statusTimestamp'))
    if type(oid) is not int or not 0<oid<2**64 or not request['attempt_at_ms']<=stamp<=now_ms:
        raise DispatchError('ORDER_LOOKUP_ID_OR_TIME_MISMATCH')
    leg=p['leg'];typ='Limit' if leg=='ENTRY' else 'Stop Market' if leg=='STOP' else 'Take Profit Limit'
    if (o.get('cloid')!=expected['c'] or o.get('coin')!=p['symbol']
            or o.get('side')!=('B' if expected['b'] else 'A')
            or type(o.get('reduceOnly')) is not bool or o['reduceOnly']!=expected['r']
            or life.number(o.get('origSz'),positive=True)!=life.number(expected['s'])
            or life.number(o.get('limitPx'),positive=True)!=life.number(expected['p'])
            or o.get('orderType')!=typ or o.get('isPositionTpsl',False) is not False
            or type(o.get('isTrigger')) is not bool or o['isTrigger']!=(leg!='ENTRY')):
        raise DispatchError('ORDER_TERMS_NOT_BOUND_TO_INTENT')
    if leg!='ENTRY' and life.number(o.get('triggerPx'),positive=True)!=life.number(expected['t']['trigger']['triggerPx']):
        raise DispatchError('TRIGGER_NOT_BOUND_TO_INTENT')
    if (request.get('reply') or {}).get('state')=='REJECTED':
        raise DispatchError('REJECTION_CONFLICTS_WITH_PUBLIC_ORDER')
    known=(request.get('reply') or {}).get('oid')
    if known is not None and known!=str(oid):
        raise DispatchError('RECEIPT_AND_PUBLIC_ORDER_CONFLICT')
    if env.get('status') not in {'open','filled'} | evidence.CANCELED | evidence.REJECTED:
        raise DispatchError('ORDER_STATE_REQUIRES_REVIEW')
    return str(oid)


def choose(state, routes, meta, sample, *, now_ms, sequence=None, after_exit_policy='NOT_SELECTED'):
    """Build the next exact action from verified card quantities; never sends."""
    ev=state['evidence'];snap=ev['snapshot'];bs=state['bindings']
    if not 0<=now_ms-snap['at_ms']<=15000 or not 0<=now_ms-sample['at_ms']<=15000:
        raise DispatchError('FRESH_EVIDENCE_REQUIRED')
    index,decimals=asset(meta,state['symbol'])
    sequence=state['revision']+1 if sequence is None else sequence
    def make(cid,leg,operation,action,quantity,old_oid=None):
        return dict(version=VERSION,card_id=cid,account=state['account'],symbol=state['symbol'],
            role=state['originals'][cid]['draft']['role'],leg=leg,operation=operation,
            action=action,quantity=quantity,old_oid=old_oid,sequence=sequence,
            source_at=state['originals'][cid]['card']['prepared']['execution']['at'],
            basis=life.digest(ev),observed_at_ms=snap['at_ms'])
    bound={b['card_id'] for b in bs}
    # Existing exposure is serviced first, never delayed by a new entry.
    if bs:
        originals={cid:state['originals'][cid] for cid in bound}
        context=dict(symbol=state['symbol'],mark_price=sample['mark_price'],at_ms=sample['at_ms'],cards={
            b['card_id']:dict(grouping='independent_fixed',requests={leg:dict(state='NONE',code=None) for leg in recovery.EXITS}) for b in bs})
        report=selected.assess(bs,snap,context,originals=originals,routes=routes,now_ms=now_ms)
        views=life.review(bs,snap,now_ms=now_ms)
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
            raise DispatchError('LIFECYCLE_OR_RECOVERY_REQUIRES_REVIEW')
        step=report['next_step']
        if step:
            cid,leg,op=step['card_id'],step['leg'],step['operation']
            b=next(b for b in bs if b['card_id']==cid)
            if op in ('RESIZE_EXIT','CANCEL_ORPHAN_EXIT'):
                oid=step['order_id']
                # Current API does not offer a safe conditional trigger replacement.
                # Cancel exact old order, OBSERVE its finality, then recalculate/create.
                return make(cid,leg,'CANCEL_FOR_RESIZE' if op=='RESIZE_EXIT' else 'CANCEL_ORPHAN_EXIT',
                    dict(type='cancel',cancels=[dict(a=index,o=int(oid))]),'0',oid)
            q=step['target_quantity'];price=step['original_price'];precise(price,q,decimals)
            cloid='0x'+life.digest([VERSION,state['bucket'],cid,leg,sequence])[:32]
            order=dict(a=index,b=b['side']=='SHORT',p=price,s=q,r=True,
                t=dict(trigger=dict(isMarket=leg=='STOP',triggerPx=price,tpsl='sl' if leg=='STOP' else 'tp')),c=cloid)
            return make(cid,leg,'CREATE_EXIT',dict(type='order',orders=[order],grouping='na'),q)
    unbound=[cid for cid in state['originals'] if cid not in bound]
    if unbound:
        cid=sorted(unbound)[0];original=state['originals'][cid]
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
        self.after_exit_policy=after_exit_policy

    def register(self, card_id):
        card=CardStore(self.store.journal).load(card_id)
        account=life.address(self.routes[card['account_role']]['account'])
        draft=selected.prepare_entry(card,self.venue.metadata(),account,self.routes)
        state=self.store.create_bucket(account,draft['symbol'])
        original=dict(card=card,draft=draft)
        if card_id in state['originals']:
            if original!=state['originals'][card_id]: raise DispatchError('IMMUTABLE_ORIGINAL_CHANGED')
            return state
        def update(conn,s):
            if s['pending']: raise DispatchError('REGISTER_WHILE_REQUEST_UNRESOLVED')
            s['originals'][card_id]=original
        return self.store.change(state['bucket'],state['revision'],'REGISTER_LOCAL_CARD',self.venue.now(),update)

    def refresh(self,bucket):
        """Public reads first; one transaction records ownership, evidence and outcome."""
        state=self.store.load(bucket);now=self.venue.now();request=None;oid=None
        bs=deepcopy(state['bindings'])
        if state['pending']:
            request=self.store.request(state['pending'])
            if request['phase']=='CONFLICT': raise DispatchError('CONFLICT_REQUIRES_REVIEW')
            if request['attempt_at_ms'] is not None and request['proposal']['action']['type']=='order':
                cloid=request['proposal']['action']['orders'][0]['c']
                raw=self.venue.lookup(state['account'],cloid)
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
            current=self.store.pending_record(conn,s)
            if current is None or current['attempt_at_ms'] is None: return None
            if snap['at_ms']<=current['attempt_at_ms']:
                raise DispatchError('POST_ATTEMPT_OBSERVATION_REQUIRED')
            p=current['proposal']
            if p['action']['type']=='order':
                if oid is None: raise DispatchError('EXACT_ORDER_OWNERSHIP_REQUIRED')
                cloid=p['action']['orders'][0]['c']
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
                current['terminal_state']=ending[0]['state']
                current['cancellation_caused_terminal_state']=False
            current['phase']='OBSERVED';current['observed_at_ms']=snap['at_ms'];s['pending']=None
            return current
        return self.store.change(bucket,state['revision'],'PUBLIC_RECONCILIATION',now,update)

    def cycle(self,bucket,*,send=False):
        """A false send flag never reserves, signs, cancels or places an order."""
        state=self.refresh(bucket)
        if state['pending']:
            pending=self.store.request(state['pending'])
            if pending['phase']!='PREPARED':
                return dict(status=pending['phase'],order_requests_sent=0)
        else: pending=None
        sample=self.venue.sample(state['account'],state['symbol']);meta=self.venue.metadata()
        sequence=pending['proposal']['sequence'] if pending else None
        proposal=choose(state,self.routes,meta,sample,now_ms=self.venue.now(),sequence=sequence,
                        after_exit_policy=self.after_exit_policy)
        if send is not True:
            return dict(status='PREVIEW_ONLY',proposal=proposal,order_requests_sent=0)
        if proposal is None: return dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        # All release checks occur BEFORE reservation or any key access.
        self.venue.authorize(state,proposal,self.after_exit_policy)
        if pending:
            prior=deepcopy(pending['proposal']);fresh=deepcopy(proposal)
            for item in (prior,fresh):
                item.pop('basis');item.pop('observed_at_ms')
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
        # A lost commit reply never reaches here. Once here, any error remains uncertain.
        sent_before=getattr(self.venue,'sent',0)
        try:
            raw=self.venue.send(request)
            reply=normalized_reply(raw,proposal['action']['type'])
        except Exception:
            return dict(status='OUTCOME_UNKNOWN',order_requests_sent=max(0,getattr(self.venue,'sent',0)-sent_before))
        self.store.reply(state,reply,self.venue.now())
        return dict(status=reply['state'],request_id=request['request_id'],order_requests_sent=max(0,getattr(self.venue,'sent',0)-sent_before))


class TestnetVenue:
    """Actual adapter, no endpoint/host injection; both old and new gates stay off.

    This release is a separately approved SINGLE-CARD trial, not certification
    of simultaneous same-market independent OCO. The general strategy is unchanged.
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
    def collect(self,value): return evidence.collect(value,evidence.PublicReader())
    def empty_snapshot(self,account,symbol):
        reader=evidence.PublicReader();start=self.now()
        for _ in range(2):
            orders=reader.read('frontendOpenOrders',account);state=reader.read('clearinghouseState',account)
            if (not isinstance(orders,list) or orders or not isinstance(state,dict)
                    or not isinstance(state.get('assetPositions'),list)
                    or any(life.number(p['position']['szi'],signed=True)!=0 for p in state['assetPositions'])):
                raise DispatchError('FIRST_TRIAL_REQUIRES_EMPTY_DEDICATED_ACCOUNT')
        if self.now()-start>15000: raise DispatchError('PRE_ENTRY_CHECKPOINT_EXPIRED')
        return dict(environment='testnet',account=account,symbol=symbol,at_ms=start,history_complete=True,
            orders_complete=True,position_quantity='0',fills=[],open_orders=[],terminal_orders=[])
    def _gate(self,proposal,after_exit_policy):
        env=self.env
        if (env.get('RENDER_SERVICE_ID')!=roles.SERVICE or env.get('HL_TESTNET_RUNTIME_MODE')!='filled_card_controlled_v1'
                or env.get('HL_TESTNET_FILLED_DISPATCH')!='approved_single_card_v1'
                or env.get('HL_TESTNET_SAFETY_PIPELINE') or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION')!='disabled'
                or env.get('HL_TESTNET_FILLED_CARD_ID')!=proposal['card_id']):
            raise DispatchError('FILLED_DISPATCH_NOT_AUTHORIZED')
        if after_exit_policy!=AFTER_EXIT or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY')!=AFTER_EXIT:
            raise DispatchError('AFTER_EXIT_REMAINDER_DECISION_REQUIRED_BEFORE_TRIAL')
        try: expires=int(env.get('HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS',''))
        except (ValueError,TypeError): raise DispatchError('EXACT_APPROVAL_DEADLINE_REQUIRED') from None
        if expires<=0 or (proposal['operation']=='ENTRY' and not 0<expires-self.now()<=86400000):
            raise DispatchError('TRIAL_ENTRY_APPROVAL_EXPIRED')
        # Check original source age again at the final boundary, including after
        # a slow budget read. Never refresh an alert timestamp on retry.
        if proposal['operation']=='ENTRY':
            from .source_window import source_fresh, timestamp
            try:
                at=timestamp(proposal['source_at'])
            except (KeyError,TypeError,ValueError):
                raise DispatchError('ORIGINAL_SOURCE_TIME_REQUIRED') from None
            if not source_fresh(at,env.get('HL_TESTNET_FILLED_SOURCE_EXPIRES_AT'),
                    now=datetime.fromtimestamp(self.now()/1000,timezone.utc)):
                raise DispatchError('NEW_TRIAL_SOURCE_NOT_FRESH')
        # Entry approval expiry must NOT silently terminate management of an open card.
        return roles.route_for(env,proposal['role'],proposal['account'])
    def authorize(self,state,proposal,after_exit_policy):
        route=self._gate(proposal,after_exit_policy)
        if any(b['card_id']!=proposal['card_id'] for b in state['bindings']):
            raise DispatchError('SHARED_MARKET_TRIAL_ISOLATION_NOT_VERIFIED')
        if proposal['operation']=='ENTRY':
            source=state['originals'][proposal['card_id']]['card']['prepared']['execution']
            from .source_window import source_fresh, timestamp
            if not source_fresh(timestamp(source['at']),self.env.get('HL_TESTNET_FILLED_SOURCE_EXPIRES_AT'),
                    now=datetime.fromtimestamp(self.now()/1000,timezone.utc)):
                raise DispatchError('NEW_TRIAL_SOURCE_NOT_FRESH')
            plan={k:source[k] for k in ('symbol','side','entry','stop','take_profit')}
            report=roles.budget_for_role(self.env,proposal['role'],route['account'],route['agent'],plan,checks.InfoReader())
            if report.get('status')!='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION' or report.get('test_plan_checked') is not True:
                raise DispatchError('EXACT_ENTRY_BUDGET_NOT_VERIFIED')
    def send(self,request):
        p=request['proposal'];route=self._gate(p,self.env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY'))
        if request['domain']!='testnet' or request['phase']!='OUTCOME_UNKNOWN' or request['attempts']!=1:
            raise DispatchError('DURABLE_TESTNET_REQUEST_REQUIRED')
        if not 0<=self.now()-request['attempt_at_ms']<=5000:
            raise DispatchError('DURABLE_ATTEMPT_EXPIRED')
        wallet=roles.wallet_for_role(self.env,p['role'],route['account'],route['agent'])
        from hyperliquid.utils.signing import sign_l1_action
        expires=request['nonce']+15000
        action=deepcopy(p['action'])
        if action['type'] not in ('order','cancel') or HOST!='api.hyperliquid-testnet.xyz':
            raise DispatchError('ACTION_OR_HOST_FORBIDDEN')
        signature=sign_l1_action(wallet,action,None,request['nonce'],expires,False)
        body=json.dumps(dict(action=action,nonce=request['nonce'],signature=signature,expiresAfter=expires)).encode()
        if len(body)>16384: raise DispatchError('REQUEST_TOO_LARGE')
        self._gate(p,self.env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY'))
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
