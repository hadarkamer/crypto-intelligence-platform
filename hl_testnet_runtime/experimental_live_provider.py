"""Read-only bridge from budgeted Testnet collectors to experimental lifecycle.

Importing or constructing this class starts no work. The caller must explicitly
schedule ``collect``; startup registration and release remain separate gates.
Account ownership is derived from the two journals and exact exchange CLOIDs,
never from an alert, acknowledgement, or caller supplied completeness flag.
"""
from copy import deepcopy
from decimal import Decimal
import time
import threading

import approved_alert_contract as contract
from . import card_lifecycle as life, card_sync_evidence as sync, checks
from . import experimental_execution_evidence as proof
from . import experimental_execution_dispatch as boundary
from . import experimental_execution_runtime as runtime
from . import filled_quantity_dispatch as wire
from . import two_account_execution as roles
from .long_stream_runtime import _validate_account_inventory
from .experimental_market_context import MarketSnapshot
from .request_budget import BudgetError, fresh_entry_retry_ms, request_weight


class ProviderError(ValueError):
    pass


def _empty(account, symbol, at_ms):
    return dict(environment='testnet', account=account, symbol=symbol,
        at_ms=at_ms, history_complete=True, orders_complete=True,
        position_quantity='0', fills=[], open_orders=[], terminal_orders=[])


def _binding(trade, ids):
    return dict(card_id=trade['cid'], card_digest=life.digest(trade['source']),
        account=trade['account'], role=trade['role'], symbol=trade['symbol'],
        side=trade['side'], planned_quantity=trade['quantity'],
        prices=deepcopy(trade['prices']), orders=ids, environment='testnet')


def _only(snapshot, ids, *, position):
    value=deepcopy(snapshot)
    for field in ('fills','open_orders','terminal_orders'):
        value[field]=[r for r in value[field] if r['oid'] in ids]
    value['position_quantity']=position
    return value


def _position(fills):
    return sum((life.number(f['quantity'],positive=True)*(1 if f['side']=='B' else -1)
                for f in fills), Decimal(0))


def _error(exc):
    # Fixed domain error codes only; do not propagate transport/DSN contents.
    value=str(exc)
    return value if value and len(value)<140 and all(c.isupper() or c.isdigit() or c=='_' for c in value) else 'EVIDENCE_COLLECTION_REQUIRES_REVIEW'


def _original_stop_needed(state, trade):
    """Read only verified quantities; this decides source-read priority only."""
    if trade['phase'] in runtime.FINAL:
        return False
    remaining=runtime._remaining(trade)
    stops=[row for oid,row in trade['orders'].items()
        if row['status']=='OPEN' and trade['order_legs'][oid]=='STOP']
    covered=(len(stops)==1 and stops[0]['wire_order']['r'] is True
        and life.number(stops[0]['wire_order']['s'])-sum(
            (life.number(f['quantity']) for f in stops[0]['fills']),Decimal(0))==remaining)
    unknown_entry=any(r['phase'] not in ('OBSERVED','ABORTED_UNSENT')
        and r['proposal']['card_id']==trade['cid'] and r['proposal']['operation']=='ENTRY'
        for r in state['requests'].values())
    return unknown_entry or remaining>0 and not covered


def _retired_experimental(state, now):
    """Find immutable final facts without refreshing their evidence clocks."""
    from .experimental_execution_archive import _eligible
    from .experimental_live_startup import COLD_HISTORY_RETRY_ERRORS
    eligible_state = dict(state, blocked_lanes={lane:reason for lane,reason
        in state.get('blocked_lanes',{}).items() if reason not in COLD_HISTORY_RETRY_ERRORS})
    result = {}
    for cid, trade in state['trades'].items():
        if cid not in state['sources'] or not _eligible(eligible_state, cid, now):
            continue
        ids = set(trade['orders'])
        if not ids:
            result[cid] = dict(binding=None, snapshot=None, orders=[])
            continue
        lane = runtime._lane(trade['account'], trade['symbol'])
        previous = state.get('collector_checkpoints', {}).get(lane)
        if previous is None:
            continue
        binding = _binding(trade, {leg:[oid for oid in ids if trade['order_legs'][oid] == leg]
                                  for leg in ('ENTRY','STOP','TAKE_PROFIT')})
        snapshot = _only(previous, ids, position='0')
        try:
            certificates = sync.terminal_certificates([binding], snapshot)
            review = life.review([binding], snapshot, now_ms=snapshot['at_ms'])
            if (set(certificates) != ids or review['bucket_issues']
                    or any(c['issues'] or c['state'] not in runtime.FINAL for c in review['cards'])):
                continue
            # Preserve the exact normalized order/fill records already accepted
            # by the runtime; a final state label alone supplies no authority.
            normalized={o['oid']:o for o in state.get('snapshots',{}).get(lane,{}).get('orders',[])}
            for oid, order in trade['orders'].items():
                if normalized.get(oid)!=order or not 0<order['at_ms']<=snapshot['at_ms']:
                    raise ProviderError('FINAL_EXPERIMENTAL_CHECKPOINT_MISSING')
                owners=[r for r in state['requests'].values() if r['proposal']['card_id']==cid
                        and r['proposal']['action']['type']!='cancel' and r.get('observed_oid')==oid]
                if (len(owners)!=1 or owners[0]['phase']!='OBSERVED'
                        or wire.requested_order(owners[0]['proposal']['action'])!=order['wire_order']
                        or order['cloid']!=order['wire_order']['c']):
                    raise ProviderError('FINAL_EXPERIMENTAL_OWNER_CHANGED')
                fills = [{k:f[k] for k in ('fill_id','quantity','price','at_ms')}
                         for f in snapshot['fills'] if f['oid']==oid]
                if (order['status'] != certificates[oid]['state']
                        or sorted(order['fills'],key=lambda f:f['fill_id']) != sorted(fills,key=lambda f:f['fill_id'])):
                    raise ProviderError('FINAL_EXPERIMENTAL_FACTS_CHANGED')
        except (ValueError, KeyError, TypeError):
            continue
        result[cid] = dict(binding=binding, snapshot=snapshot,
                          orders=deepcopy(list(trade['orders'].values())))
    return result


def _merge_bucket(target, cold):
    """Add immutable zero-position ownership to current inventory checks only."""
    target = deepcopy(target)
    target['bindings'] += deepcopy(cold['bindings'])
    for field in ('fills','open_orders','terminal_orders'):
        target['evidence']['snapshot'][field] += deepcopy(cold['evidence']['snapshot'][field])
    return target


class LiveEvidenceProvider:
    domain='testnet'

    def __init__(self, env, *, legacy_store, experimental_store, price_evidence,
                 budget, clock=None, info_reader=None, public_reader=None,
                 lookup_reader=None, safety_provider=None, entry_reader=None):
        if budget is None:
            raise ProviderError('SHARED_TESTNET_REQUEST_BUDGET_REQUIRED')
        self.env={k:v for k,v in env.items() if not any(x in k for x in ('KEY','SECRET','PASSWORD','TOKEN','DATABASE','DSN'))}
        self.legacy_store=legacy_store; self.experimental_store=experimental_store
        self.prices=price_evidence; self.budget=budget; self.safety=safety_provider
        self._clock=clock or (lambda:time.time_ns()//1000000)
        self._thread=threading.local()
        self._info_reader=info_reader;self._public_reader=public_reader
        self._entry_reader=entry_reader or info_reader
        self.info=info_reader or checks.InfoReader(budget=budget,priority='protection')
        self.public=public_reader or sync.PublicReader(budget=budget,priority='protection')
        self.entry_info=self._entry_reader or checks.InfoReader(budget=budget,priority='background')
        if any(reader.budget is not budget for reader in (self.info,self.public,self.entry_info)):
            raise ProviderError('ONE_SHARED_TESTNET_REQUEST_BUDGET_REQUIRED')
        self._lookup_reader=lookup_reader
        self._market=None

    @property
    def info(self):
        return self._thread.info

    @info.setter
    def info(self,value):
        self._thread.info=value

    @property
    def public(self):
        return self._thread.public

    @public.setter
    def public(self,value):
        self._thread.public=value

    @property
    def entry_info(self):
        return self._thread.entry_info

    @entry_info.setter
    def entry_info(self,value):
        self._thread.entry_info=value

    @property
    def _last(self):
        return getattr(self._thread,'last',None)

    @_last.setter
    def _last(self,value):
        self._thread.last=value

    def now(self):
        return self._clock()

    def _lookup(self, account, cloid):
        if self._lookup_reader is not None:
            return self._lookup_reader(account,cloid)
        import hyperliquid_testnet_executor as legacy
        return legacy.TestnetHTTP(budget=self.budget,priority='protection').info(
            'orderStatus',user=account,oid=cloid)

    def _legacy(self, state):
        result={}
        for role,account in state['routes'].items():
            roles.route_for(self.env,role,account)
            rows=self.legacy_store.for_account(account)
            if (not isinstance(rows,list) or len(rows)>256 or any(s.get('account')!=account for s in rows)
                    or len({s['symbol'] for s in rows})!=len(rows)):
                raise ProviderError('COMPLETE_LEGACY_ACCOUNT_JOURNAL_REQUIRED')
            result[account]=deepcopy(rows)
        return result

    @staticmethod
    def _revision(legacy):
        return life.digest({a:[dict(bucket=s['bucket'],revision=s['revision'],pending=s['pending'])
                            for s in rows] for a,rows in legacy.items()})

    def _inventory(self, account, reader=None):
        reader=self.public if reader is None else reader
        at_ms=self.now()
        orders=reader.read('frontendOpenOrders',account)
        positions=reader.read('clearinghouseState',account)
        if (not isinstance(orders,list) or len(orders)>10000 or not isinstance(positions,dict)
                or not isinstance(positions.get('assetPositions'),list)):
            raise ProviderError('COMPLETE_RAW_ACCOUNT_INVENTORY_REQUIRED')
        ids=set();symbols=set()
        for row in orders:
            if (not isinstance(row,dict) or type(row.get('oid')) is not int
                    or not 0<row['oid']<2**64 or row['oid'] in ids):
                raise ProviderError('EXACT_UNIQUE_ACCOUNT_ORDER_IDS_REQUIRED')
            life.ident(row.get('coin'),r'[A-Z][A-Z0-9]{0,19}');ids.add(row['oid'])
        for row in positions['assetPositions']:
            p=row.get('position') if isinstance(row,dict) else None
            if not isinstance(p,dict) or p.get('coin') in symbols:
                raise ProviderError('EXACT_UNIQUE_ACCOUNT_POSITIONS_REQUIRED')
            life.ident(p.get('coin'),r'[A-Z][A-Z0-9]{0,19}')
            life.number(p.get('szi'),signed=True);symbols.add(p['coin'])
        # Margin totals change with marks; ownership comparison uses positions.
        return dict(orders=deepcopy(sorted(orders,key=lambda x:x['oid'])),
            positions=dict(assetPositions=deepcopy(sorted(positions['assetPositions'],key=lambda x:x['position']['coin']))),
            account_state=deepcopy(positions),at_ms=at_ms)

    def _experimental_bindings(self, state, account, symbol, *, observation_cache=None, inventory=None):
        lookups={};ids={};seen=set()
        changing={str(row.get('o',row.get('oid'))) for r in state['requests'].values()
            if r['phase'] not in ('OBSERVED','ABORTED_UNSENT')
            and r['proposal']['account']==account and r['proposal']['symbol']==symbol
            for row in r['proposal']['action'].get('cancels',r['proposal']['action'].get('modifies',[]))}
        for request in state['requests'].values():
            p=request['proposal']
            if (p['account']!=account or p['symbol']!=symbol or p['action']['type']=='cancel'
                    or request['phase']=='ABORTED_UNSENT'):
                continue
            order=wire.requested_order(p['action']);cloid=order['c']
            oid=request.get('observed_oid')
            prior=state['trades'][p['card_id']]['orders'].get(oid)
            if oid is not None and request['phase']=='OBSERVED':
                if prior is None or prior['oid']!=oid or prior['wire_order']!=order or prior['cloid']!=cloid:
                    raise ProviderError('PREVIOUSLY_OWNED_ORDER_TERMS_CHANGED')
                checkpoint=state.get('collector_checkpoints',{}).get(runtime._lane(account,symbol),{})
                certified=any(row['oid']==oid for row in checkpoint.get('terminal_orders',[]))
                if prior['status']!='OPEN' and certified:
                    raw=None  # Immutable terminal proof is checked by the collector.
                else:
                    opened=next((row for row in (inventory or {}).get('orders',[])
                                 if str(row['oid'])==oid),None)
                    trigger=order['t'].get('trigger')
                    expected_type=('Limit' if trigger is None else
                        'Stop Market' if trigger['tpsl']=='sl' else 'Take Profit Limit')
                    if (prior['status']=='OPEN' and oid not in changing and opened is not None
                            and opened.get('orderType')==expected_type
                            and opened.get('isTrigger') is (trigger is not None)):
                        # The OID/CLOID binding was already observed. Current
                        # complete inventory proves it remains open; asking its
                        # status again adds no fact. Adapt that evidence to the
                        # existing collector, preserving its prior status clock.
                        # frontendOpenOrders may omit CLOID. A supplied changed
                        # CLOID is still rejected by the normal identity check.
                        current=deepcopy(opened);current.setdefault('cloid',cloid)
                        stamp=life.moment(current.get('timestamp'))
                        if not request['attempt_at_ms']<=stamp<=inventory['at_ms']:
                            raise ProviderError('ORDER_LOOKUP_ID_OR_TIME_MISMATCH')
                        raw=dict(status='order',order=dict(status='open',
                            statusTimestamp=prior['at_ms'],order=current))
                    else:
                        # Missing orders and activated triggers need their real
                        # status; inventory alone cannot prove finality.
                        reader=observation_cache.pass_reader(0) if observation_cache is not None else self.public
                        raw=reader.read('orderStatus',account,oid=oid)
            else:
                raw=self._lookup(account,cloid)
            if raw is not None:lookups[cloid]=raw
            if raw=={'status':'unknownOid'}:
                if request.get('observed_oid') is not None:
                    raise ProviderError('PREVIOUSLY_OWNED_ORDER_LOOKUP_MISSING')
                continue
            if raw is not None:
                observed=wire.identity(raw,request,self.now())
                if oid is not None and oid!=observed:
                    raise ProviderError('PREVIOUSLY_OWNED_ORDER_ID_CHANGED')
                oid=observed
                if observation_cache is not None:
                    # A successful CLOID lookup already returned the complete
                    # status for this exact OID. Do not fetch it a second time.
                    key=observation_cache._key('orderStatus',account,oid=oid)
                    observation_cache.samples[0][key]=deepcopy(raw)
            if oid in seen:
                raise ProviderError('EXCHANGE_ORDER_HAS_MULTIPLE_DURABLE_OWNERS')
            seen.add(oid)
            ids.setdefault(p['card_id'],{k:[] for k in ('ENTRY','STOP','TAKE_PROFIT')})[p['leg']].append(oid)
        bindings=[]
        for cid,orders in ids.items():
            if not orders['ENTRY']:
                raise ProviderError('OBSERVED_ENTRY_OWNERSHIP_REQUIRED')
            binding=_binding(state['trades'][cid],orders)
            life.validate_bindings([binding]);bindings.append(binding)
        return bindings,lookups

    def _lane(self, state, account, symbol, legacy, inventory, *, observation_cache=None, observation_end_ms=None, historical=None):
        lane=runtime._lane(account,symbol)
        exp,lookups=self._experimental_bindings(state,account,symbol,
            observation_cache=observation_cache,inventory=inventory)
        old=next((s for s in legacy if s['symbol']==symbol),None)
        old_bindings=[] if old is None else old['bindings']
        bindings=old_bindings+exp
        if bindings: life.validate_bindings(bindings)
        previous=state.get('collector_checkpoints',{}).get(lane)
        if previous is None:
            attempts=[r['attempt_at_ms'] for r in state['requests'].values()
                      if r['proposal']['account']==account and r['proposal']['symbol']==symbol
                      and r['phase']!='ABORTED_UNSENT']
            beginning=max(state['not_before_ms'],min(attempts)) if attempts else state['not_before_ms']
            previous=(deepcopy(old['evidence']['snapshot']) if old and old.get('evidence')
                      else _empty(account,symbol,beginning))
        if (previous['account']!=account or previous['symbol']!=symbol
                or previous['at_ms']>self.now()):
            raise ProviderError('COLLECTOR_CHECKPOINT_SCOPE_CHANGED')
        if bindings:
            receipt=(self.safety._receipt(state)
                if callable(getattr(type(self.safety),'_receipt',None)) else None)
            unchanged=(receipt is not None and self.safety._continuous(receipt,account)
                and not any(r['proposal']['account']==account
                    and r['phase'] not in ('OBSERVED','ABORTED_UNSENT')
                    for r in state['requests'].values()))
            observed=sync.collect(dict(bindings=bindings,snapshot=previous),self.public,
                clock=self.now,reuse_verified_terminals=True,verification_passes=1,
                observation_cache=observation_cache,observation_end_ms=observation_end_ms,
                reuse_unchanged_fills=unchanged)['snapshot']
        else:
            # No owned request exists: prove current flatness from the complete
            # account inventory, and never infer absence from an alert.
            if (any(o['coin']==symbol for o in inventory['orders'])
                    or any(p['position']['coin']==symbol and life.number(p['position']['szi'],signed=True)
                           for p in inventory['positions']['assetPositions'])):
                raise ProviderError('UNOWNED_MARKET_EXPOSURE_REQUIRES_RECONCILIATION')
            observed=_empty(account,symbol,self.now() if observation_end_ms is None else observation_end_ms)
        # A bounded overlap read may repeat fills from an already archived
        # final trade. Only exact immutable archived facts may be excluded;
        # changed facts and unknown activity remain reconciliation failures.
        if state.get('history'):
            observed=self.experimental_store.strip_archived_collector_snapshot(observed)
        if historical:
            ids={oid for b in historical['bindings'] for values in b['orders'].values() for oid in values}
            facts={f['fill_id']:f for f in historical['evidence']['snapshot']['fills']}
            for fill in observed['fills']:
                if fill['oid'] in ids and facts.get(fill['fill_id'])!=fill:
                    raise ProviderError('IMMUTABLE_FINAL_FILL_CHANGED')
            observed['fills']=[f for f in observed['fills'] if f['oid'] not in ids]
        all_ids={o for b in bindings for rows in b['orders'].values() for o in rows}
        if any(f['oid'] not in all_ids for f in observed['fills']):
            raise ProviderError('UNOWNED_ACCOUNT_FILL_REQUIRES_RECONCILIATION')
        old_ids={o for b in old_bindings for rows in b['orders'].values() for o in rows}
        old_snapshot=_only(observed,old_ids,position=life.text(_position([f for f in observed['fills'] if f['oid'] in old_ids])))
        legacy_active=False
        if old_bindings:
            report=life.review(old_bindings,old_snapshot,now_ms=self.now())
            legacy_active=bool(old['pending'] is not None or report['bucket_issues'] or any(
                c['state'] not in runtime.FINAL or c['issues'] for c in report['cards']))
        exp_ids=all_ids-old_ids
        # Removing known final legacy history is sound only at zero net size;
        # any unresolved predecessor retains the same-market fence.
        experimental=_only(observed,exp_ids,position=observed['position_quantity'])
        if old_ids and not legacy_active and life.number(old_snapshot['position_quantity'],signed=True)!=0:
            raise ProviderError('LEGACY_FINALITY_POSITION_MISMATCH')
        if legacy_active and exp:
            raise ProviderError('SHARED_MARKET_LEGACY_EXPOSURE_CONFLICT')
        normalized=None if legacy_active else proof.normalize_snapshot(state,experimental,lookups,now_ms=self.now(),
            reuse_verified_terminals=True)
        bucket=dict(account=account,symbol=symbol,bindings=bindings,
                    pending=old['pending'] if old else None,evidence=dict(snapshot=observed))
        return normalized,observed,bucket,legacy_active,exp,lookups

    def _entry_account(self, account, role, *, entry_reads=None):
        """Bind the signer to the routed account and leave allowance for exits.

        Alert prices and local risk determine the order. Margin, leverage and
        account-mode suitability are enforced by the exchange when it accepts
        or rejects that order; they do not require another read-only trading
        model before sending. Only successful account facts are reused within
        this collection, never across submissions or later collections.
        """
        route=roles.route_for(self.env,role,account)
        entry_reads={} if entry_reads is None else entry_reads
        if account in entry_reads:
            return deepcopy(entry_reads[account])
        at=self.now()
        link=self.entry_info.read('userRole',user=route['agent'])
        if (not isinstance(link,dict) or link.get('role')!='agent'
                or not isinstance(link.get('data'),dict)
                or checks.address(link['data'].get('user'))!=account):
            raise ProviderError('AGENT_ACCOUNT_MISMATCH')
        headroom=roles.entry_action_headroom(account,self.entry_info)
        value=dict(account=account,agent=route['agent'],at_ms=at,
                   action_headroom=headroom)
        entry_reads[account]=value
        return deepcopy(value)

    def collect(self, state, *, entries_enabled=False):
        if state.get('domain')!='testnet':
            raise ProviderError('NATIVE_TESTNET_STATE_REQUIRED_NO_SOFTWARE_RELABELING')
        # Per-collection transport byte/call ceilings stay bounded. The durable
        # IP budget is SHARED across these fresh readers and never reset.
        self.info=self._info_reader or checks.InfoReader(budget=self.budget,priority='protection')
        self.public=self._public_reader or sync.PublicReader(budget=self.budget,priority='protection')
        self.entry_info=self._entry_reader or checks.InfoReader(budget=self.budget,priority='background')
        life.moment(state['not_before_ms'])
        before=getattr(self.safety,'before_collect',None)
        safety_token=before(state) if callable(before) else None
        now=self.now()
        legacy=self._legacy(state);ownership=self._revision(legacy)
        result=dict(basis_revision=state['revision'],metadata={},snapshots=[],marks={},
            ranges={},bars={},entry_accounts={},entry_blocked={},entry_retry_after_ms={},account_entry_blocked={},blocked_lanes={},
            collector_checkpoints={},ownership_revision=ownership,rejected_requests=[],
            inventory_account_errors={},account_inventory_at_ms={},
            legacy_account_revisions={a:self._revision({a:rows}) for a,rows in legacy.items()})
        cache=sync.ObservationReadCache(self.public)
        inventories={};account_retries={}
        def remember_retry(exc, accounts):
            if not isinstance(exc,BudgetError):return
            try:delay=fresh_entry_retry_ms(exc)
            except BudgetError:return
            if delay is not None:
                for account in accounts:account_retries[account]=self.now()+delay

        from .experimental_live_startup import retired_legacy_rows
        cold_legacy=retired_legacy_rows(state,legacy)
        legacy_live={a:[] if a in cold_legacy else rows for a,rows in legacy.items()}
        retired=_retired_experimental(state,now)
        working=deepcopy(state)
        working['trades']={cid:t for cid,t in working['trades'].items() if cid not in retired}
        working['requests']={rid:r for rid,r in working['requests'].items()
                             if r['proposal']['card_id'] not in retired}
        cold={a:{} for a in state['routes'].values()}
        for account,rows in cold_legacy.items():
            for row in rows:
                if row.get('evidence') is not None:
                    cold[account][row['symbol']]=deepcopy(row)
        for cid,item in retired.items():
            if item['binding'] is None:continue
            trade=state['trades'][cid];account=trade['account'];symbol=trade['symbol']
            bucket=dict(account=account,symbol=symbol,pending=None,bindings=[item['binding']],
                        evidence=dict(snapshot=item['snapshot']))
            cold[account][symbol]=(_merge_bucket(cold[account][symbol],bucket)
                                  if symbol in cold[account] else bucket)
        # Cold facts remain in the journal. Strip them only from this private
        # collector input, retaining exact facts separately for overlap checks.
        for account,rows in cold.items():
            for symbol,bucket in rows.items():
                lane=runtime._lane(account,symbol)
                prior=working.get('collector_checkpoints',{}).get(lane)
                if prior is None:continue
                ids={oid for b in bucket['bindings'] for values in b['orders'].values() for oid in values}
                active_ids={row['oid'] for field in ('fills','open_orders','terminal_orders')
                            for row in prior[field]}-ids
                clean=_only(prior,active_ids,position=prior['position_quantity'])
                # A newly submitted owned request cannot have a fill before its
                # durable attempt. Do not reopen an unrelated years-old cursor.
                attempts=[r['attempt_at_ms'] for r in working['requests'].values()
                          if r['proposal']['account']==account and r['proposal']['symbol']==symbol
                          and r['phase']!='ABORTED_UNSENT']
                if not active_ids and attempts:
                    clean['at_ms']=max(clean['at_ms'],min(attempts))
                working.setdefault('collector_checkpoints',{})[lane]=clean
        buckets={a:list(rows.values()) for a,rows in cold.items()}
        raw_snapshots={};owners={};lookup_facts={}
        lanes={(s['account'],s['symbol']) for rows in legacy_live.values() for s in rows
               if s['bindings'] or s.get('pending') is not None or s.get('emergency') is not None}
        lanes|={(t['account'],t['symbol']) for t in working['trades'].values()}
        available=getattr(self.budget,'capacity',None)
        from .experimental_live_runtime import entry_retry_ready, TestnetExecutionRuntime
        deferred={cid:decision['retry_after_ms'] for cid,decision in state.get('entry_decisions',{}).items()
                  if decision.get('reason') in ('TESTNET_REQUEST_BUDGET_EXHAUSTED','TESTNET_REQUEST_BUDGET_BUSY',
                      'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED','TESTNET_OBSERVATION_BATCH_EXPIRED')
                  and type(decision.get('retry_after_ms')) is int and decision['retry_after_ms']>now}
        unresolved_accounts={r['proposal']['account'] for r in state['requests'].values()
                             if r['phase'] not in ('OBSERVED','ABORTED_UNSENT')}
        protection_accounts={t['account'] for t in state['trades'].values() if _original_stop_needed(state,t)}
        # Reject locally impossible admissions before reading their market or
        # order allowance. These are the same unchanged gates as _admit.
        candidates=set()
        for cid,source in state['sources'].items():
            msg=source['source'];role='long_account' if msg['side']=='LONG' else 'short_account'
            account=state['routes'][role]
            if (not entries_enabled or cid in retired or cid in deferred
                    or cid in state['trades'] and not entry_retry_ready(state,cid,now)
                    or not runtime._source_active(source,now)
                    or account in unresolved_accounts|protection_accounts):
                continue
            peers=[t for k,t in state['trades'].items() if k!=cid
                   and t['account']==account and t['phase'] not in runtime.FINAL]
            if any(t['symbol']==msg['symbol'] for t in peers):
                result['entry_blocked'][cid]=runtime.shared_market.ENTRY_BLOCK
                continue
            if (msg['family'] in ('r2732','hype_row71205','sol_g65')
                    and any(t['source']['family']==msg['family'] for t in peers)):
                continue
            candidates.add(cid)
        participating={account for account,_ in lanes}|{
            state['routes']['long_account' if state['sources'][cid]['source']['side']=='LONG'
                else 'short_account'] for cid in candidates}
        for cid in list(candidates):
            msg=state['sources'][cid]['source']
            account=state['routes']['long_account' if msg['side']=='LONG' else 'short_account']
            # Move the existing single allowance check ahead of source-only
            # I/O. Include the inventory and market reads not yet performed;
            # each transport still acquires its ordinary unchanged permit.
            if callable(available):
                try:
                    planned=['userRole','userRateLimit']
                    if self._market is None or not 0<=now-self._market[0]<5000:
                        planned.append('metaAndAssetCtxs')
                    planned+=['frontendOpenOrders','clearinghouseState']*len(participating)
                    # Existing lanes are collected before entry. Estimate
                    # their settled recent fill pages and possible status
                    # reads. The transport still reserves the full 120 per
                    # page; this local estimate grants no read permission.
                    planned+=['orderStatus' for r in working['requests'].values()
                        if r['phase']!='ABORTED_UNSENT' and r['proposal']['action']['type']!='cancel'
                        and state['trades'][r['proposal']['card_id']]['orders'].get(
                            r.get('observed_oid'),{}).get('status') not in ('FILLED','CANCELED','REJECTED')]
                    weight=request_weight('/exchange',dict(action=dict(type='order',orders=[{}])))
                    weight+=sum(request_weight('/info',dict(type=kind)) for kind in planned)
                    for owned_account,owned_symbol in lanes:
                        prior=working.get('collector_checkpoints',{}).get(runtime._lane(owned_account,owned_symbol),{})
                        rows=sum(f['at_ms']>=prior.get('at_ms',now)-sync.OVERLAP_MS for f in prior.get('fills',[]))
                        weight+=20+(rows+19)//20
                    capacity=available(requested_weight=weight,priority='background')
                    if not capacity['eligible']:
                        raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
                            **{key:capacity[key] for key in ('used_weight','requested_weight','ceiling','retry_after_ms')})
                except Exception as exc:
                    result['entry_blocked'][cid]=_error(exc)
                    remember_retry(exc,[account])
                    if account in account_retries:result['entry_retry_after_ms'][cid]=account_retries[account]
                    candidates.discard(cid)
                    continue
        source_lanes={(state['routes']['long_account' if state['sources'][cid]['source']['side']=='LONG'
                                    else 'short_account'],state['sources'][cid]['source']['symbol'])
                      for cid in candidates}
        lanes|=source_lanes
        # A known source-only allowance wait must not consume its own future
        # budget through empty scans. It supplies no fresh account evidence.
        waiting=bool(deferred or any(reason in ('TESTNET_REQUEST_BUDGET_EXHAUSTED',
            'TESTNET_REQUEST_BUDGET_BUSY','TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',
            'TESTNET_OBSERVATION_BATCH_EXPIRED') for reason in result['entry_blocked'].values()))
        for account in sorted({account for account,_ in lanes} or (set() if waiting else set(state['routes'].values()))):
            result['account_inventory_at_ms'][account]=now
            try:
                inventories[account]=self._inventory(account,cache.pass_reader(0))
            except Exception as exc:
                result['inventory_account_errors'][account]=_error(exc)
                result['account_entry_blocked'][account]=_error(exc)
                remember_retry(exc,[account])
        for account,symbol in sorted(lanes):
            lane=runtime._lane(account,symbol)
            if account not in inventories:
                result['blocked_lanes'][lane]=result['account_entry_blocked'][account]
                continue
            try:
                normalized,raw,bucket,occupied,bindings,lookups=self._lane(
                    working,account,symbol,legacy_live[account],inventories[account],
                    observation_cache=cache,observation_end_ms=now,historical=cold[account].get(symbol))
                historical=cold[account].get(symbol)
                if historical:
                    facts={f['fill_id']:f for f in historical['evidence']['snapshot']['fills']}
                    ids={oid for b in historical['bindings'] for values in b['orders'].values() for oid in values}
                    for fill in raw['fills']:
                        if fill['oid'] in ids and facts.get(fill['fill_id'])!=fill:
                            raise ProviderError('IMMUTABLE_FINAL_FILL_CHANGED')
                    # _lane normally rejects unknown OIDs before this point;
                    # immutable overlap is handled in its scoped cold input.
                    bucket=_merge_bucket(bucket,historical)
                    buckets[account]=[b for b in buckets[account] if b['symbol']!=symbol]
                result['collector_checkpoints'][lane]=bucket['evidence']['snapshot']
                raw_snapshots[lane]=bucket['evidence']['snapshot'];lookup_facts[lane]=lookups
                buckets[account].append(bucket)
                for binding in bindings:owners[binding['card_id']]=binding
                if occupied:result['blocked_lanes'][lane]='LEGACY_PREDECESSOR_NOT_FINAL'
                elif normalized is not None:
                    for cid,item in retired.items():
                        t=state['trades'][cid]
                        if t['account']==account and t['symbol']==symbol:
                            normalized['orders']+=deepcopy(item['orders'])
                    result['snapshots'].append(normalized)
            except (ValueError,KeyError,TypeError) as exc:
                result['blocked_lanes'][lane]=_error(exc)
                result['account_entry_blocked'].setdefault(account,_error(exc))
                remember_retry(exc,[account])
                continue
        current_legacy=self._legacy(state)
        for role,account in state['routes'].items():
            if account not in inventories:continue
            try:
                if self._revision({account:current_legacy[account]})!=self._revision({account:legacy[account]}):
                    raise ProviderError('ACCOUNT_OBSERVATION_OR_JOURNAL_CHANGED_RETRY')
                if not 0<=self.now()-result['account_inventory_at_ms'][account]<=15000:
                    raise ProviderError('ACCOUNT_COLLECTION_EXPIRED')
            except Exception as exc:
                result['inventory_account_errors'][account]=_error(exc)
                result['account_entry_blocked'].setdefault(account,_error(exc))
                inventories.pop(account,None)
                result['snapshots']=[s for s in result['snapshots'] if s['account']!=account]
                for lane in [k for k,v in result['collector_checkpoints'].items() if v['account']==account]:
                    result['collector_checkpoints'].pop(lane)
                for a,symbol in lanes:
                    if a==account:result['blocked_lanes'][runtime._lane(account,symbol)]=_error(exc)
                continue
            try:
                _validate_account_inventory(account,buckets[account],inventories[account]['orders'],inventories[account]['positions'],role=role)
            except ValueError as exc:result['account_entry_blocked'].setdefault(account,_error(exc))
        for rid,request in state['requests'].items():
            p=request['proposal'];lane=runtime._lane(p['account'],p['symbol'])
            if (request['phase']!='OUTCOME_UNKNOWN' or p['action']['type'] not in ('order','batchModify','cancel')
                    or (request.get('reply') or {}).get('state')!='REJECTED'
                    or request.get('observed_oid') is not None or lane in result['blocked_lanes']):
                continue
            action=p['action'];kind=action['type'];old_oid=None
            if kind!='cancel':
                cloid=wire.requested_order(action)['c']
                if lookup_facts.get(lane,{}).get(cloid)!={'status':'unknownOid'}:continue
            if kind in ('batchModify','cancel'):
                old_oid=str(action['modifies'][0]['oid'] if kind=='batchModify' else action['cancels'][0]['o'])
                normalized=next((s for s in result['snapshots'] if runtime._lane(s['account'],s['symbol'])==lane),None)
                old=next((o for o in (normalized or {}).get('orders',[]) if o['oid']==old_oid),None)
                prior=state['trades'][p['card_id']]['orders'].get(old_oid)
                if old is None or old['status']!='OPEN' or prior!=old:continue
            fact=dict(request_id=rid,account=p['account'],symbol=p['symbol'],at_ms=now,
                lookup_status='orderStillOpen' if kind=='cancel' else 'unknownOid',reply_digest=life.digest(request['reply']))
            if old_oid is not None:fact['old_order_id']=old_oid
            result['rejected_requests'].append(fact)
        # Reuse the lifecycle's pure reconciliation before deciding whether
        # optional source reads may run. An old durable STOP can be canceled
        # or undersized by a newly observed fill. This throwaway projection
        # never persists state, advances a source cursor or proposes an action.
        price_state=deepcopy(state)
        reconciler=object.__new__(runtime.IsolatedExecutionRuntime)
        source_io_allowed={a:True for a in state['routes'].values()}
        for snapshot in result['snapshots']:
            candidate_state=deepcopy(price_state)
            try:
                reconciler._snapshot(candidate_state,snapshot,self.now())
            except (ValueError,KeyError,TypeError):
                source_io_allowed[snapshot['account']]=False
                continue
            price_state=candidate_state
        for trade in price_state['trades'].values():
            if _original_stop_needed(price_state,trade):source_io_allowed[trade['account']]=False
        unresolved={r['proposal']['account'] for r in state['requests'].values()
                    if r['phase'] not in ('OBSERVED','ABORTED_UNSENT')}
        # Stable fixed-exit trades consume no live mark: use the CURRENT
        # reconciled fill/order facts, so newly missing or undersized exits
        # still request a fresh market before the runtime repairs them.
        market_lanes=set(source_lanes);cancellation_lanes=set()
        for trade in price_state['trades'].values():
            if trade['phase'] in runtime.FINAL:continue
            active=[(trade['order_legs'][oid],order) for oid,order in trade['orders'].items()
                    if order['status']=='OPEN']
            remaining=runtime._remaining(trade)
            if remaining==0 and active and not any(leg=='ENTRY' for leg,order in active):
                # Closing has already been observed. Cancel the leftover owned
                # exit by ID; a price quote cannot change that decision.
                cancellation_lanes.add((trade['account'],trade['symbol']))
                continue
            stable=(trade['source']['family'] not in ('r2732','sol_g65') and remaining>0
                and not any(leg=='ENTRY' for leg,order in active)
                and trade['account'] not in unresolved
                and all(len([o for own_leg,o in active if own_leg==leg])==1
                    and all(o['wire_order']['r'] is True
                        and life.number(o['wire_order']['s'])-sum(
                            (life.number(f['quantity']) for f in o['fills']),Decimal(0))==remaining
                        and life.number(o['wire_order']['p'])==life.number(price)
                        for own_leg,o in active if own_leg==leg)
                    for leg,price in (('STOP',trade['prices']['stop']),
                                      ('TAKE_PROFIT',trade['prices']['take_profit']))))
            if not stable:market_lanes.add((trade['account'],trade['symbol']))
        market=None;market_error=None
        if market_lanes:
            market_at=self.now()
            try:
                cached=self._market
                if cached is not None and 0<=market_at-cached[0]<5000:
                    market_at,market=cached
                else:
                    self._market=None
                    market=MarketSnapshot(self.info.read('metaAndAssetCtxs'),observed_at_ms=market_at)
                result['metadata']=market.metadata
            except Exception as exc:
                self._market=None
                market_error=_error(exc);remember_retry(exc,[account for account,_ in market_lanes])
        if cancellation_lanes and not result['metadata']:
            try:
                # Cancellation needs the owned asset index, not a live mark.
                # Normally the accepted entry supplied this metadata already.
                result['metadata']=(self._market[1].metadata if self._market is not None
                    else self.info.read('meta'))
                for account,symbol in cancellation_lanes:
                    wire.asset(result['metadata'],symbol)
            except (ValueError,KeyError,TypeError) as exc:
                for account,symbol in cancellation_lanes:
                    result['blocked_lanes'][runtime._lane(account,symbol)]=_error(exc)
        for account,symbol in sorted(market_lanes):
            lane=runtime._lane(account,symbol)
            if account not in inventories or lane in result['blocked_lanes']:continue
            try:
                if market is None:raise ProviderError(market_error)
                result['marks'][lane]=market.mark(account=account,symbol=symbol,now_ms=self.now())
            except (ValueError,KeyError,TypeError) as exc:
                result['blocked_lanes'][lane]=_error(exc)
                market_error=_error(exc)
        if market is not None:
            # Share one actual mark across the immediate entry/exit steps.
            # Preserve its original clock and refresh within the strictest
            # existing emergency-price window; do not cache malformed prices.
            self._market=(market_at,market) if market_error is None else None
        # Reuse only this collection; existing exposure is still collected
        # while a source-only entry waits for its recorded budget retry.
        entry_reads={};entry_selected=False
        entry_planner=object.__new__(TestnetExecutionRuntime)
        if callable(getattr(self.safety,'release_loader',None)):
            entry_planner.release_loader=self.safety.release_loader
        for cid in sorted(state['sources'],key=lambda k:(state['sources'][k]['source']['source_at'],k)):
            record=state['sources'][cid]
            msg=contract.validate(record['source']);role='long_account' if msg['side']=='LONG' else 'short_account'
            account=state['routes'][role];lane=runtime._lane(account,msg['symbol'])
            trade=price_state['trades'].get(cid)
            active_trade=trade is not None and trade['phase'] not in runtime.FINAL
            candidate=(trade is None or entry_retry_ready(state,cid,self.now())) and entries_enabled is True and runtime._source_active(record,self.now())
            if candidate and cid in deferred:
                result['entry_blocked'][cid]=state['entry_decisions'][cid]['reason']
                result['entry_retry_after_ms'][cid]=deferred[cid]
                candidate=False
            if candidate and (account in unresolved or not source_io_allowed[account]):
                result['entry_blocked'][cid]='ENTRY_DEFERRED_FOR_OWNED_PROTECTION_AND_RECONCILIATION'
                candidate=False
            candidate=candidate and cid in candidates
            if candidate and entry_selected:
                result['entry_blocked'][cid]='ENTRY_DEFERRED_TO_NEXT_CYCLE'
                candidate=False
            # Keep formula stop updates and pending-order cancellation alive
            # during an entry halt. Entry ranges/allowance have no maintenance
            # authority and must never delay protection of an existing fill.
            if msg['family'] in ('r2732','sol_g65') and (active_trade or candidate) and source_io_allowed[account]:
                try:
                    cursor=(trade or {}).get('condition') or state.get('formula_states',{}).get(cid) or {}
                    since=getattr(self.prices,'closed_bars_since',None)
                    result['bars'][cid]=(since(msg,self.now(),cursor.get('cursor_ms'))
                        if callable(since) else self.prices.closed_bars(msg,self.now()))
                except Exception:
                    # Source/DB transport failures are not exchange-ownership
                    # failures. Retain the last verified stop and allow its
                    # original initial protection to proceed.
                    result['bars'][cid]=[]
            if not candidate: continue
            if account in result['account_entry_blocked'] or lane in result['blocked_lanes']:
                result['entry_blocked'][cid]=result['account_entry_blocked'].get(account,result['blocked_lanes'].get(lane))
                if account in account_retries:result['entry_retry_after_ms'][cid]=account_retries[account]
                continue
            try:
                # An authenticated approved alert already carries the formula's
                # entry decision. Its sender must not reconstruct a second
                # prospective strategy using historical source/Testnet paths.
                # Current market, account ownership and order allowance remain
                # checked using the unchanged request budget.
                if not contract.is_approved(msg):
                    source=self.prices.source_range(msg,self.now())
                    testnet=self.prices.mark_window(account,msg['symbol'],contract.moment_ms(msg['source_at']),self.now())
                    runtime._range(source,msg,self.now())
                    proof.require_mark_window(testnet,account=account,symbol=msg['symbol'],reference_at_ms=contract.moment_ms(msg['source_at']),now_ms=self.now())
                    result['ranges'][cid]=dict(source=source,testnet=testnet)
                if self.safety is None:
                    raise ProviderError('VERIFIED_SUPERVISOR_AND_FEED_CAPABILITY_REQUIRED')
                result['entry_accounts'][cid]=self._entry_account(account,role,entry_reads=entry_reads)
                # The worker sends at most one proposal per cycle. Reuse its
                # unchanged pure admission to stop preparing accounts that
                # cannot be used. A refused first candidate leaves the next
                # candidate eligible; the projection never writes the journal.
                entry_selected=entry_planner._admit(deepcopy(price_state),cid,result,self.now()) is not None
            except Exception as exc:
                # This entire block is entry-only evidence. A failed source
                # cache or background allowance must not abort owned exits.
                entry_reads.pop(account,None)
                result['entry_blocked'][cid]=_error(exc)
                remember_retry(exc,[account])
                if account in account_retries:result['entry_retry_after_ms'][cid]=account_retries[account]
        # Expired evidence closes only its account; no later optional read
        # refreshes the original observation clock.
        for account in list(inventories):
            if not 0<=self.now()-result['account_inventory_at_ms'][account]<=15000:
                result['inventory_account_errors'][account]='ACCOUNT_COLLECTION_EXPIRED'
                result['account_entry_blocked'][account]='ACCOUNT_COLLECTION_EXPIRED'
                inventories.pop(account)
                result['snapshots']=[s for s in result['snapshots'] if s['account']!=account]
                for lane in [k for k,v in result['collector_checkpoints'].items() if v['account']==account]:
                    result['collector_checkpoints'].pop(lane)
                for a,symbol in lanes:
                    if a==account:result['blocked_lanes'][runtime._lane(account,symbol)]='ACCOUNT_COLLECTION_EXPIRED'
        result['account_inventory_at_ms']={a:result['account_inventory_at_ms'][a] for a in inventories}
        result.update(inventory_complete=True,inventory_accounts=sorted(inventories),inventory_at_ms=now)
        stage=getattr(self.safety,'stage_observation',None)
        if callable(stage):
            result['safety_checkpoint_id']=stage(safety_token,state=state,context=deepcopy(result))
        self._last=dict(context=deepcopy(result),state=deepcopy(state),legacy_revision=ownership,
            inventories=inventories,buckets=buckets,raw_snapshots=raw_snapshots,owners=owners,
            legacy_account_revisions={a:self._revision({a:rows}) for a,rows in legacy.items()})
        return deepcopy(result)

    def observation_committed(self, context):
        """Complete a feed reconciliation only after the worker's DB commit."""
        if 'safety_checkpoint_id' not in context:
            return
        method=getattr(self.safety,'after_committed',None)
        if not callable(method):
            raise ProviderError('DURABLE_SAFETY_CHECKPOINT_CALLBACK_REQUIRED')
        verified=method(checkpoint_id=context['safety_checkpoint_id'],state=self.experimental_store.load())
        if verified is not True:
            raise ProviderError('DURABLE_FEED_CHECKPOINT_NOT_RECONCILED')
        return True

    def dispatch_context(self, request):
        """Recheck journals around signing, retaining original collector clocks.

        No HTTP occurs after the one-second transport permit is committed.
        A feed/supervisor adapter must independently verify its observation
        checkpoint; a dictionary of caller supplied safety booleans is refused.
        """
        owner=getattr(self,'startup_owner',None)
        if owner is not None:
            owner.verify()
        cached=self._last
        if cached is None or not 0<=self.now()-cached['context']['inventory_at_ms']<=15000:
            raise ProviderError('FRESH_COLLECTOR_CHECKPOINT_REQUIRED_BEFORE_DISPATCH')
        state=self.experimental_store.load();p=request['proposal'];cid=p['card_id'];entry=p['operation']=='ENTRY'
        if state.get('domain')!='testnet' or state['requests'].get(request['request_id'])!=request:
            raise ProviderError('EXACT_NATIVE_DURABLE_REQUEST_REQUIRED')
        if p['basis']!=life.digest(state['snapshots']):
            raise ProviderError('EXPERIMENTAL_OWNERSHIP_CHANGED_RECONCILE_FIRST')
        for rid,peer in state['requests'].items():
            if rid==request['request_id'] or peer['phase'] in ('OBSERVED','ABORTED_UNSENT'):
                continue
            same_market=(peer['proposal']['account']==p['account'] and peer['proposal']['symbol']==p['symbol'])
            pp=peer['proposal']
            safety_action=(p['operation']=='EMERGENCY_CLOSE' or
                p['operation']=='CANCEL' and p['leg']=='ENTRY' and p['action']['type']=='cancel')
            emergency_exit_uncertainty=(safety_action and same_market
                and pp['card_id']==cid and pp['leg'] in ('STOP','TAKE_PROFIT')
                and pp['operation'] in ('CREATE_EXIT','AMEND_EXIT')
                and pp['action']['type'] in ('order','batchModify')
                and wire.requested_order(pp['action'])['r'] is True)
            if entry and pp['account']==p['account'] or same_market and not emergency_exit_uncertainty:
                raise ProviderError('UNRESOLVED_EXPERIMENTAL_PEER_REQUIRES_RECONCILIATION')
        current_legacy=self._legacy(state)
        if self._revision({p['account']:current_legacy[p['account']]})!=cached['legacy_account_revisions'][p['account']]:
            raise ProviderError('LEGACY_OWNERSHIP_CHANGED_RECONCILE_FIRST')
        ctx=cached['context'];trade=state['trades'][cid];account=p['account'];lane=runtime._lane(account,p['symbol'])
        if account not in ctx['inventory_accounts']:
            raise ProviderError(ctx.get('inventory_account_errors',{}).get(account,'COMPLETE_ACCOUNT_OBSERVATION_REQUIRED'))
        if lane in ctx['blocked_lanes']:
            raise ProviderError(ctx['blocked_lanes'][lane])
        if entry and (cid in ctx['entry_blocked'] or account in ctx['account_entry_blocked']):
            raise ProviderError(ctx['entry_blocked'].get(cid,ctx['account_entry_blocked'].get(account)))
        if entry:
            # Re-run the identical pure planner at the final clock. It catches
            # new source precedence, expired windows, formula overlap and
            # quantity changes without constructing a second strategy model.
            from .experimental_live_runtime import TestnetExecutionRuntime
            replay=deepcopy(state)
            if trade['orders'] or trade['entry_fills'] or trade['exit_fills']:
                raise ProviderError('ENTRY_ALREADY_HAS_OBSERVED_EXPOSURE')
            del replay['trades'][cid];del replay['requests'][request['request_id']]
            planner=object.__new__(TestnetExecutionRuntime)
            expected=planner._admit(replay,cid,ctx,self.now())
            if expected is None:
                raise ProviderError('FINAL_SOURCE_ENTRY_ADMISSION_RETIRED')
            expected['observed_at_ms']=p['observed_at_ms']
            if expected!=p:
                raise ProviderError('FINAL_SOURCE_ENTRY_PROPOSAL_CHANGED')
        # The only accepted safety capability executes its own journal/feed
        # checkpoint verification. An absent capability cannot enable entries.
        safety=dict(account=account,role=p['role'],at_ms=ctx['inventory_at_ms'],entry_enabled=False,
            emergency_healthy=False,feed_reconciled=False,entry_circuit_clear=False,
            supervisor_at_ms=ctx['inventory_at_ms'],not_before_ms=state['not_before_ms'])
        if self.safety is not None:
            method=getattr(type(self.safety),'verify_checkpoint',None)
            if not callable(method): raise ProviderError('VERIFIED_SAFETY_CAPABILITY_REQUIRED')
            safety=self.safety.verify_checkpoint(state=state,request=request,
                collected_at_ms=ctx['inventory_at_ms'],ownership_revision=cached['legacy_revision'])
        route=roles.route_for(self.env,p['role'],account)
        result=dict(source=deepcopy(trade['source']),current_source=deepcopy(state['sources'][cid]),
            env=self.env,agent=route['agent'],host=boundary.HOST,metadata=ctx['metadata'],safety=safety,
            buckets=cached['buckets'][account],open_orders=cached['inventories'][account]['orders'],
            positions=cached['inventories'][account]['positions'])
        if p['action']['type']!='cancel':
            result['market']={k:v for k,v in ctx['marks'][lane].items() if k!='account'}
        if entry:
            result['entry_account']=deepcopy(ctx['entry_accounts'][cid])
            if not contract.is_approved(trade['source']):
                result.update(source_range=ctx['ranges'][cid]['source'],testnet_range=ctx['ranges'][cid]['testnet'])
        else:
            owner=cached['owners'].get(cid)
            if owner is None: raise ProviderError('OBSERVED_EXIT_OWNER_REQUIRED')
            ids={o for values in owner['orders'].values() for o in values}
            raw=cached['raw_snapshots'][lane]
            owner_snapshot=_only(raw,ids,position=life.text(_position([f for f in raw['fills'] if f['oid'] in ids])))
            result.update(owner=owner,owner_snapshot=owner_snapshot,lock_proof=trade['condition'],
                observed_stop_requests=[deepcopy(r) for r in state['requests'].values()
                    if r['proposal']['card_id']==cid and r['proposal']['leg']=='STOP' and r['phase']=='OBSERVED'])
        return deepcopy(result)
