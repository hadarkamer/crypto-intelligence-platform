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
from . import experimental_execution_prices as execution_prices
from . import filled_quantity_dispatch as wire
from . import two_account_execution as roles
from .long_stream_runtime import _validate_account_inventory
from .experimental_market_context import MarketSnapshot


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

    def _inventory(self, account):
        orders=self.public.read('frontendOpenOrders',account)
        positions=self.public.read('clearinghouseState',account)
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
            positions=dict(assetPositions=deepcopy(sorted(positions['assetPositions'],key=lambda x:x['position']['coin']))))

    def _experimental_bindings(self, state, account, symbol):
        lookups={};ids={};seen=set()
        for request in state['requests'].values():
            p=request['proposal']
            if (p['account']!=account or p['symbol']!=symbol or p['action']['type']=='cancel'
                    or request['phase']=='ABORTED_UNSENT'):
                continue
            order=wire.requested_order(p['action']);cloid=order['c']
            raw=self._lookup(account,cloid);lookups[cloid]=raw
            if raw=={'status':'unknownOid'}:
                if request.get('observed_oid') is not None:
                    raise ProviderError('PREVIOUSLY_OWNED_ORDER_LOOKUP_MISSING')
                continue
            oid=wire.identity(raw,request,self.now())
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

    def _lane(self, state, account, symbol, legacy, inventory):
        lane=runtime._lane(account,symbol)
        exp,lookups=self._experimental_bindings(state,account,symbol)
        old=next((s for s in legacy if s['symbol']==symbol),None)
        old_bindings=[] if old is None else old['bindings']
        bindings=old_bindings+exp
        if bindings: life.validate_bindings(bindings)
        previous=state.get('collector_checkpoints',{}).get(lane)
        if previous is None:
            previous=(deepcopy(old['evidence']['snapshot']) if old and old.get('evidence')
                      else _empty(account,symbol,state['not_before_ms']))
        if (previous['account']!=account or previous['symbol']!=symbol
                or previous['at_ms']>self.now()):
            raise ProviderError('COLLECTOR_CHECKPOINT_SCOPE_CHANGED')
        if bindings:
            observed=sync.collect(dict(bindings=bindings,snapshot=previous),self.public,
                clock=self.now,reuse_verified_terminals=True,verification_passes=2)['snapshot']
        else:
            # No owned request exists: prove CURRENT flatness from the two
            # account inventories, and never infer absence from an alert.
            if (any(o['coin']==symbol for o in inventory['orders'])
                    or any(p['position']['coin']==symbol and life.number(p['position']['szi'],signed=True)
                           for p in inventory['positions']['assetPositions'])):
                raise ProviderError('UNOWNED_MARKET_EXPOSURE_REQUIRES_RECONCILIATION')
            observed=_empty(account,symbol,self.now())
        # A bounded overlap read may repeat fills from an already archived
        # final trade. Only exact immutable archived facts may be excluded;
        # changed facts and unknown activity remain reconciliation failures.
        if state.get('history'):
            observed=self.experimental_store.strip_archived_collector_snapshot(observed)
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
        normalized=None if legacy_active else proof.normalize_snapshot(state,experimental,lookups,now_ms=self.now())
        bucket=dict(account=account,symbol=symbol,bindings=bindings,
                    pending=old['pending'] if old else None,evidence=dict(snapshot=observed))
        return normalized,observed,bucket,legacy_active,exp,lookups

    def _capacity(self, message, account, role, metadata, *, buckets, inventory, market):
        route=roles.route_for(self.env,role,account)
        prepared=execution_prices.prepare(message,metadata)['execution']
        plan={k:prepared[k] for k in ('symbol','side','entry','stop','take_profit')}
        at=self.now()
        # Reuse role-specific actual account identity, abstraction, budget and
        # address allowance checks. The recording reader retains raw facts.
        recorded={};outer=self
        class RecordingReader:
            parallel=False
            def read(self,kind,**kwargs):
                # Admission preflight cannot consume the protection reserve.
                value=market.metadata if kind=='meta' else outer.entry_info.read(kind,**kwargs)
                if kind=='activeAssetData':
                    value=market.active_asset_data(value,account=account,
                        symbol=message['symbol'],now_ms=outer.now())
                recorded[(kind,kwargs.get('user'),kwargs.get('coin'))]=deepcopy(value)
                return value
        reader=RecordingReader();started=time.monotonic()
        mode=reader.read('userAbstraction',user=account)
        if mode=='default':
            # The old helper's owned-exposure flag was tied to the legacy
            # scheduler name. Here its actual prerequisite is independently
            # proved from BOTH journals and current exchange inventory.
            _validate_account_inventory(account,buckets,inventory['orders'],inventory['positions'],role=role)
            checked=dict(budget_report=roles._default_native_snapshot(route,reader,message['symbol'],mode,started,
                plan=plan,allow_owned_exposure=True),entry_action_headroom=roles.entry_action_headroom(account,reader))
        else:
            checked=boundary.account_preflight(self.env,role,account,route['agent'],plan,reader)
        report=checked['budget_report']
        if (report.get('status')!='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION'
                or report.get('test_plan_checked') is not True):
            raise ProviderError('CURRENT_ACCOUNT_BUDGET_PRECHECK_FAILED')
        mode=recorded[('userAbstraction',account,None)]
        if mode=='unifiedAccount':
            _,unheld=checks.unified_usdc(recorded[('spotClearinghouseState',account,None)])
        elif mode in ('disabled','default'):
            unheld=checks.number(recorded[('clearinghouseState',account,None)]['withdrawable'])
        else: raise ProviderError('ACCOUNT_MODE_NOT_VERIFIED')
        active=recorded[('activeAssetData',account,message['symbol'])]
        available,size=checks.capacity(active,account,message['symbol'])
        asset=next(x for x in metadata['universe'] if x['name']==message['symbol'])
        return dict(account=account,at_ms=at,unheld=life.text(unheld),available=life.text(available),
            max_size=life.text(size),active=active,max_leverage=asset['maxLeverage'],
            action_headroom=checked['entry_action_headroom'],account_mode_verified=True),report

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
        first={a:self._inventory(a) for a in state['routes'].values()}
        market_at=self.now()
        market=MarketSnapshot(self.info.read('metaAndAssetCtxs'),observed_at_ms=market_at)
        metadata=market.metadata
        result=dict(basis_revision=state['revision'],metadata=metadata,snapshots=[],marks={},
            ranges={},bars={},capacity={},entry_blocked={},account_entry_blocked={},blocked_lanes={},
            collector_checkpoints={},ownership_revision=ownership,rejected_requests=[])
        buckets={a:[] for a in state['routes'].values()};raw_snapshots={};owners={};reports={}
        lookup_facts={}
        lanes={(s['account'],s['symbol']) for rows in legacy.values() for s in rows}
        lanes|={(t['account'],t['symbol']) for t in state['trades'].values()}
        lanes|={(state['routes']['long_account' if s['source']['side']=='LONG' else 'short_account'],s['source']['symbol'])
                for s in state['sources'].values()}
        for account,symbol in sorted(lanes):
            lane=runtime._lane(account,symbol)
            try:
                normalized,raw,bucket,occupied,bindings,lookups=self._lane(state,account,symbol,legacy[account],first[account])
                result['collector_checkpoints'][lane]=raw;raw_snapshots[lane]=raw
                lookup_facts[lane]=lookups
                buckets[account].append(bucket)
                for binding in bindings: owners[binding['card_id']]=binding
                if occupied: result['blocked_lanes'][lane]='LEGACY_PREDECESSOR_NOT_FINAL'
                elif normalized is not None: result['snapshots'].append(normalized)
                result['marks'][lane]=market.mark(account=account,symbol=symbol,now_ms=self.now())
            except (ValueError,KeyError,TypeError) as exc:
                result['blocked_lanes'][lane]=_error(exc)
                result['account_entry_blocked'][account]=_error(exc)
        second={a:self._inventory(a) for a in state['routes'].values()}
        if first!=second or self._revision(self._legacy(state))!=ownership:
            raise ProviderError('ACCOUNT_OBSERVATION_OR_JOURNAL_CHANGED_RETRY')
        for role,account in state['routes'].items():
            try:
                _validate_account_inventory(account,buckets[account],second[account]['orders'],second[account]['positions'],role=role)
            except ValueError as exc: result['account_entry_blocked'][account]=_error(exc)
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
        source_io_allowed=True
        for snapshot in result['snapshots']:
            candidate_state=deepcopy(price_state)
            try:
                reconciler._snapshot(candidate_state,snapshot,self.now())
            except (ValueError,KeyError,TypeError):
                source_io_allowed=False
                continue
            price_state=candidate_state
        source_io_allowed=source_io_allowed and not any(
            _original_stop_needed(price_state,trade) for trade in price_state['trades'].values())
        unresolved=any(r['phase'] not in ('OBSERVED','ABORTED_UNSENT') for r in state['requests'].values())
        for cid,record in state['sources'].items():
            msg=contract.validate(record['source']);role='long_account' if msg['side']=='LONG' else 'short_account'
            account=state['routes'][role];lane=runtime._lane(account,msg['symbol'])
            trade=price_state['trades'].get(cid)
            active_trade=trade is not None and trade['phase'] not in runtime.FINAL
            candidate=trade is None and entries_enabled is True and runtime._source_active(record,self.now())
            if candidate and (unresolved or not source_io_allowed):
                result['entry_blocked'][cid]='ENTRY_DEFERRED_FOR_OWNED_PROTECTION_AND_RECONCILIATION'
                candidate=False
            # Keep formula stop updates and pending-order cancellation alive
            # during an entry halt. Entry ranges/capacity have no maintenance
            # authority and must never delay protection of an existing fill.
            if msg['family'] in ('r2732','sol_g65') and (active_trade or candidate) and source_io_allowed:
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
                result['entry_blocked'][cid]=result['account_entry_blocked'].get(account,result['blocked_lanes'].get(lane));continue
            try:
                # An authenticated approved alert already carries the formula's
                # entry decision. Its sender must not reconstruct a second
                # prospective strategy using historical source/Testnet paths.
                # Current market metadata, account inventory, ownership and
                # capacity remain mandatory and use the unchanged budget.
                if not contract.is_approved(msg):
                    source=self.prices.source_range(msg,self.now())
                    testnet=self.prices.mark_window(account,msg['symbol'],contract.moment_ms(msg['source_at']),self.now())
                    runtime._range(source,msg,self.now())
                    proof.require_mark_window(testnet,account=account,symbol=msg['symbol'],reference_at_ms=contract.moment_ms(msg['source_at']),now_ms=self.now())
                    result['ranges'][cid]=dict(source=source,testnet=testnet)
                if self.safety is None:
                    raise ProviderError('VERIFIED_SUPERVISOR_AND_FEED_CAPABILITY_REQUIRED')
                result['capacity'][cid],reports[cid]=self._capacity(msg,account,role,metadata,
                    buckets=buckets[account],inventory=second[account],market=market)
            except Exception as exc:
                # This entire block is entry-only evidence. A failed source
                # cache or background allowance must not abort owned exits.
                result['entry_blocked'][cid]=_error(exc)
        if not 0<=self.now()-now<=15000:
            raise ProviderError('ACCOUNT_COLLECTION_EXPIRED')
        result.update(inventory_complete=True,inventory_accounts=sorted(state['routes'].values()),inventory_at_ms=now)
        stage=getattr(self.safety,'stage_observation',None)
        if callable(stage):
            result['safety_checkpoint_id']=stage(safety_token,state=state,context=deepcopy(result))
        self._last=dict(context=deepcopy(result),state=deepcopy(state),legacy_revision=ownership,
            inventories=second,buckets=buckets,raw_snapshots=raw_snapshots,owners=owners,budget_reports=reports)
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
        if cached is None or not 0<=self.now()-cached['context']['inventory_at_ms']<=5000:
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
            if entry or same_market and not emergency_exit_uncertainty:
                raise ProviderError('UNRESOLVED_EXPERIMENTAL_PEER_REQUIRES_RECONCILIATION')
        if self._revision(self._legacy(state))!=cached['legacy_revision']:
            raise ProviderError('LEGACY_OWNERSHIP_CHANGED_RECONCILE_FIRST')
        ctx=cached['context'];trade=state['trades'][cid];account=p['account'];lane=runtime._lane(account,p['symbol'])
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
            positions=cached['inventories'][account]['positions'],market={k:v for k,v in ctx['marks'][lane].items() if k!='account'})
        if entry:
            cap=ctx['capacity'][cid]
            result.update(budget_at_ms=cap['at_ms'],entry_action_headroom=cap['action_headroom'],
                budget_report=cached['budget_reports'][cid])
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
