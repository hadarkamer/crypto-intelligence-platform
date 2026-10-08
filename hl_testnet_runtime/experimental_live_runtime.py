"""Explicit Testnet composition of the already-tested experimental lifecycle.

Not registered in startup, and never activated by importing this module. Only
concrete Testnet state/evidence/transport ports are accepted. Observations are
prepared without I/O inside transactions, then compared and committed under the
state revision lock. The shared IP request budget is acquired before commit;
nonces use the existing per-agent dispatch allocator in that same transaction.
"""
from copy import deepcopy
import threading

from . import card_lifecycle as life, filled_quantity_dispatch as wire
from .experimental_execution_runtime import (
    IsolatedExecutionRuntime, RuntimeError, _lane, _source_active, _remaining, FINAL, TERMINAL,
)
from .experimental_live_state import TestnetExecutionState
from . import r2732_entry
from . import experimental_live_release as releases
import approved_alert_contract as contract

VERSION = 'experimental-testnet-worker-v1'
MODE = 'explicit_approved_testnet_worker_v1'

# These are expected candidate refusals, not permission to swallow corrupt
# state, missing fields, ownership conflicts or an arbitrary implementation bug.
LOCAL_ENTRY_REFUSALS = frozenset((
    'TESTNET_MARK_OUTSIDE_ORIGINAL_EXITS', 'ASSET_NOT_FOUND',
    'ASSET_PRECISION_OR_LISTING_INVALID', 'ASSET_UNAVAILABLE',
    'PRICES_COLLAPSED_AFTER_ROUNDING', 'PRICE_PRECISION_NO_ROUNDING',
    'CURRENT_PRICE_OR_SIZE_PRECISION_REJECTED', 'OUTSIDE_LAB_SIZE_BOUNDS',
    'QUANTITY_EXCEEDS_EXCHANGE_CAP', 'MARK_NOTIONAL_EXCEEDS_LAB_CAP',
    'ESTIMATED_MARGIN_EXCEEDS_UNHELD_USDC', 'ESTIMATED_MARGIN_EXCEEDS_EXCHANGE_AVAILABLE',
    'OUTSIDE_CONSERVATIVE_LAB_BUDGET', 'CURRENT_LEVERAGE_NOT_VERIFIED',
    'FRESH_EXACT_ACCOUNT_CAPACITY_REQUIRED', 'EXACT_FRESH_TESTNET_MARK_SAMPLE_REQUIRED',
    'PROSPECTIVE_ENTRY_ALREADY_CONSUMED', 'FORMULA_OVERLAP_NOT_ALLOWED',
    'G65_PENDING_ENTRY_OR_CANCEL_ALREADY_CONSUMED', 'HYPE_ORIGINAL_ENTRY_WINDOW_CONSUMED',
    'R2732_SOURCE_INITIAL_EXIT_ALREADY_REACHED', 'R2732_SOURCE_LOCK_ALREADY_REACHED',
    'R2732_TESTNET_INITIAL_EXIT_ALREADY_REACHED', 'R2732_TESTNET_LOCK_ALREADY_REACHED',
    'R2732_TESTNET_PRICE_OUTSIDE_ORIGINAL_EXITS', 'R2732_VALID_MINIMUM_ENTRY_SIZE_REQUIRED',
))
RETRYABLE_UNSENT = frozenset((
    'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED', 'FINAL_EVIDENCE_EXPIRED',
    'DURABLE_ATTEMPT_EXPIRED', 'FRESH_COLLECTOR_CHECKPOINT_REQUIRED_BEFORE_DISPATCH',
    'EXPERIMENTAL_OWNERSHIP_CHANGED_RECONCILE_FIRST',
    'LEGACY_OWNERSHIP_CHANGED_RECONCILE_FIRST', 'DISPATCH_CONTEXT_CHANGED_RECONCILE_FIRST',
    'OBSERVED_SAFETY_CHECKPOINT_PENDING',
))
MAX_ENTRY_ATTEMPTS = 3
ENTRY_RETRY_DELAY_MS = 1000


def entry_retry_ready(state, cid, now):
    """Positive durable never-sent proof; no timeouts or status-only retries."""
    trade = state['trades'].get(cid)
    if trade is None or trade['phase'] != 'RETRY_WAIT_UNSENT':
        return False
    if (trade['orders'] or trade['entry_fills'] or trade['exit_fills']
            or not _source_active(state['sources'][cid], now)):
        return False
    attempts = [r for r in state['requests'].values() if r['proposal']['card_id'] == cid]
    if not 0 < len(attempts) < MAX_ENTRY_ATTEMPTS:
        return False
    if any(r['phase'] != 'ABORTED_UNSENT' or r.get('certified_unsent') is not True
            or r.get('observed_oid') is not None or r.get('reply') is not None
            or r['proposal']['operation'] != 'ENTRY'
            or r.get('unsent_reason') not in RETRYABLE_UNSENT for r in attempts):
        return False
    retry = trade.get('entry_retry', {})
    return (type(retry.get('after_ms')) is int and now >= retry['after_ms']
        and retry.get('attempts') == len(attempts))


def _entry_decision(state, cid, context, now, reason, *, scope=None, retry_after_ms=None):
    source = state['sources'][cid]['source']
    role = 'long_account' if source['side'] == 'LONG' else 'short_account'
    account = state['routes'][role]
    if retry_after_ms is None:
        retry_after_ms = context.get('entry_retry_after_ms', {}).get(cid)
    if scope is None:
        scope = ('account' if account in context.get('account_entry_blocked', {})
            else 'market' if _lane(account, source['symbol']) in context.get('blocked_lanes', {})
            else 'occurrence')
    state.setdefault('entry_blocked', {})[cid] = reason
    decision = dict(stage='ENTRY_ADMISSION', scope=scope,
        account_role=role, symbol=source['symbol'], occurrence_id=cid, reason=reason,
        at_ms=now, retry_after_ms=retry_after_ms,
        deadline=source.get('expires_at') or source.get('valid_until'))
    previous = state.setdefault('entry_decisions', {}).get(cid)
    fields = ('stage', 'scope', 'reason', 'retry_after_ms')
    if previous is None or any(previous.get(key) != decision[key] for key in fields):
        state['events'].append(dict(kind='ENTRY_DEFERRED', **deepcopy(decision)))
    state['entry_decisions'][cid] = decision


def readiness():
    return dict(version=VERSION, mode='testnet_explicit_release',
        live_dispatch_enabled=False, production_ready=False,
        shared_symbol_parallel_enabled=False)


class TestnetExecutionRuntime(IsolatedExecutionRuntime):
    """Shares strategy, fills, protection and recovery transitions unchanged.

    The public isolated worker retains all software-only constructor fences.
    This class requires its own persisted TESTNET domain and concrete adapters.
    """
    def __init__(self, store, provider, dispatch, *, release_loader, mode=None):
        from .experimental_live_provider import LiveEvidenceProvider
        from .experimental_live_dispatch import LiveDispatchPort
        if (mode != MODE or type(store) is not TestnetExecutionState
                or type(provider) is not LiveEvidenceProvider
                or type(dispatch) is not LiveDispatchPort
                or not callable(release_loader)):
            raise RuntimeError('EXPLICIT_TESTNET_WORKER_REQUIRED')
        self.store, self.venue, self.dispatch = store, provider, dispatch
        # Source worker and supervisor share one collector/dispatch capability.
        # The supervisor is a fallback, never a second concurrent REST scan.
        self._cycle_lock = threading.Lock()
        self.release_loader = release_loader
        state = store.load()
        if state['domain'] != 'testnet':
            raise RuntimeError('DURABLE_TESTNET_STATE_REQUIRED')
        self._release(state, provider.now())

    def _release(self, state, now):
        """Expiry closes only admissions. Existing exposure keeps its exits."""
        try:
            release = deepcopy(self.release_loader())
            if (release.get('domain') != 'testnet'
                    or release.get('dispatch_enabled') is not True
                    or release.get('protection_enabled') is not True
                    or type(release.get('entries_enabled')) is not bool
                    or release.get('routes') != state['routes']
                    or release.get('not_before_ms') != state['not_before_ms']
                    or not state['not_before_ms'] <= now
                    or not releases.entry_window_valid(release)):
                raise ValueError()
            life.ident(release['release_id'], r'[0-9a-f]{64}')
        except Exception:
            raise RuntimeError('EXACT_CURRENT_TESTNET_RELEASE_REQUIRED') from None
        return release

    def receive(self, messages):
        return self._receive(messages, domain='testnet', status=readiness())

    def _proposal(self, state, trade, leg, action, quantity, now, *, operation):
        proposal = super()._proposal(state, trade, leg, action, quantity, now, operation=operation)
        proposal['version'] = VERSION
        return proposal

    _r2732_initial = staticmethod(r2732_entry.initial_testnet)
    _r2732_admission = staticmethod(r2732_entry.testnet_entry_admission)

    def _admit(self, state, cid, context, now):
        msg = state['sources'][cid]['source']
        # Provider replays the pure admission with an unconstructed planner;
        # the actual worker and final sender independently enforce release.
        release = self._release(state, now) if callable(getattr(self,'release_loader',None)) else {}
        if (release.get('entry_policy') == releases.CONTINUOUS_ENTRY
                and msg['family'] == 'maxpain' and not contract.is_approved(msg)):
            state.setdefault('entry_blocked', {})[cid] = 'CONTINUOUS_MAXPAIN_REQUIRES_APPROVED_ALERT'
            return None
        role = 'long_account' if msg['side'] == 'LONG' else 'short_account'
        account = state['routes'][role]
        reason = (context.get('entry_blocked', {}).get(cid)
            or context.get('account_entry_blocked', {}).get(account)
            or context.get('blocked_lanes', {}).get(_lane(account, msg['symbol'])))
        if reason:
            _entry_decision(state, cid, context, now, reason)
            return None
        state.setdefault('entry_blocked', {}).pop(cid, None)
        # Retry only an empty trade backed by exact never-sent certificates.
        # Earlier attempts remain immutable and keep the CLOID serial unique.
        candidate = deepcopy(state)
        retry = entry_retry_ready(candidate, cid, now)
        previous = candidate['trades'].pop(cid) if retry else None
        try:
            proposal = super()._admit(candidate, cid, context, now)
        except ValueError as exc:
            reason = str(exc)
            if reason not in LOCAL_ENTRY_REFUSALS:
                raise
            _entry_decision(state, cid, context, now, reason)
            return None
        if previous is not None and proposal is None:
            candidate['trades'][cid] = previous
        if proposal is not None:
            prior = candidate.setdefault('entry_decisions', {}).pop(cid, None)
            if prior is not None:
                candidate['events'].append(dict(at_ms=now, kind='ENTRY_ADMISSION_PASSED',
                    occurrence_id=cid, previous_reason=prior['reason']))
            if previous is not None:
                candidate['trades'][cid]['first_entry_decision_at_ms'] = previous.get(
                    'first_entry_decision_at_ms', previous.get('execution_audit', {}).get('entry_decision_at_ms'))
        state.clear(); state.update(candidate)
        if proposal is None and cid in state.get('entry_blocked', {}):
            _entry_decision(state, cid, context, now, state['entry_blocked'][cid])
        return proposal

    def _abort_entry_unsent(self, state, request, now, reason):
        """Called only before send or after an exact transport certificate."""
        request.update(phase='ABORTED_UNSENT', unsent_reason=reason, certified_unsent=True)
        cid = request['proposal']['card_id']
        if request['proposal']['operation'] == 'ENTRY':
            trade = state['trades'][cid]
            if trade['orders'] or trade['entry_fills'] or trade['exit_fills']:
                raise RuntimeError('UNSENT_ENTRY_CONFLICTS_WITH_OBSERVED_EXPOSURE')
            attempts = [r for r in state['requests'].values() if r['proposal']['card_id'] == cid]
            retry = (reason in RETRYABLE_UNSENT and len(attempts) < MAX_ENTRY_ATTEMPTS
                and _source_active(state['sources'][cid], now)
                and all(r['phase'] == 'ABORTED_UNSENT' and r.get('certified_unsent') is True
                    and r.get('unsent_reason') in RETRYABLE_UNSENT for r in attempts))
            trade['phase'] = 'RETRY_WAIT_UNSENT' if retry else 'CANCELED_WITHOUT_FILL'
            if retry:
                trade['entry_retry'] = dict(after_ms=now+ENTRY_RETRY_DELAY_MS,
                    reason=reason, attempts=len(attempts))
            else:
                trade.pop('entry_retry', None)
            _entry_decision(state, cid, {}, now, reason,
                retry_after_ms=now+ENTRY_RETRY_DELAY_MS if retry else None)
        state['events'].append(dict(at_ms=now, kind='DEFINITELY_NOT_SUBMITTED',
            request_id=request['request_id'], occurrence_id=cid, reason=reason))

    @staticmethod
    def _rejection_key(proposal):
        action = deepcopy(proposal['action'])
        for order in action.get('orders', []): order.pop('c', None)
        for item in action.get('modifies', []): item['order'].pop('c', None)
        return life.digest([proposal['operation'], proposal['leg'], action])

    def _rejections(self, state, context, now):
        for fact in context.get('rejected_requests', []):
            required = {'request_id', 'account', 'symbol', 'at_ms', 'lookup_status', 'reply_digest'}
            if (not isinstance(fact, dict) or set(fact) not in (required, required | {'old_order_id'})
                    or type(fact['at_ms']) is not int or not 0 <= now-fact['at_ms'] <= 15000):
                raise RuntimeError('EXACT_REJECTED_NO_ORDER_EVIDENCE_REQUIRED')
            request = state['requests'].get(fact['request_id'])
            if (request is None or request['phase'] != 'OUTCOME_UNKNOWN'
                    or request['observed_oid'] is not None
                    or (request.get('reply') or {}).get('state') != 'REJECTED'
                    or life.digest(request['reply']) != fact['reply_digest']
                    or fact['at_ms'] < request['attempt_at_ms']
                    or request['proposal']['account'] != fact['account']
                    or request['proposal']['symbol'] != fact['symbol']):
                raise RuntimeError('EXACT_REJECTED_DURABLE_REQUEST_REQUIRED')
            action=request['proposal']['action'];kind=action['type']
            if fact['lookup_status'] != ('orderStillOpen' if kind=='cancel' else 'unknownOid'):
                raise RuntimeError('EXACT_REJECTION_LOOKUP_REQUIRED')
            lane = _lane(fact['account'], fact['symbol'])
            if lane in state.get('blocked_lanes', {}) or lane not in state['snapshots']:
                raise RuntimeError('REJECTED_REQUEST_FULL_OWNERSHIP_REQUIRED')
            trade = state['trades'][request['proposal']['card_id']]
            if kind in ('batchModify','cancel'):
                oid=str(action['modifies'][0]['oid'] if kind=='batchModify' else action['cancels'][0]['o'])
                row=next((o for o in state['snapshots'][lane]['orders'] if o['oid']==oid),None)
                if (fact.get('old_order_id') != oid or row is None or row['status'] != 'OPEN'
                        or row != trade['orders'].get(oid)):
                    raise RuntimeError('REJECTED_ACTION_ORIGINAL_ORDER_CHANGED')
            elif kind != 'order' or 'old_order_id' in fact:
                raise RuntimeError('SUPPORTED_REJECTED_ACTION_REQUIRED')
            request['phase'] = 'OBSERVED'
            request['terminal_state'] = 'REJECTED_NO_EFFECT' if kind=='cancel' else 'REJECTED_NO_ORDER'
            if request['proposal']['operation'] == 'ENTRY':
                if trade['entry_fills'] or trade['orders']:
                    raise RuntimeError('REJECTED_ENTRY_CONFLICTS_WITH_OBSERVED_FILL')
                trade['phase'] = 'CANCELED_WITHOUT_FILL'
            elif kind in ('batchModify','cancel'):
                # Keep the current protective order and block identical retries.
                # A changed desired price, size or original OID is a new action.
                trade.setdefault('rejection_circuit', {})[self._rejection_key(request['proposal'])] = request['request_id']
            state['events'].append(dict(at_ms=now,kind=request['terminal_state'],
                request_id=request['request_id'],occurrence_id=trade['cid']))

    def _deadline_view(self, state, trade, context, now):
        from . import experimental_execution_dispatch as boundary
        raw=context.get('collector_checkpoints', {}).get(_lane(trade['account'],trade['symbol']))
        if raw is None:
            return None
        if type(raw.get('at_ms')) is not int or not 0 <= now-raw['at_ms'] <= 5000:
            # Insufficient emergency freshness cannot authorize a close; it
            # also must not suppress ordinary protection with its own gates.
            return None
        orders={leg:[oid for oid,v in trade['order_legs'].items() if v==leg]
                for leg in ('ENTRY','STOP','TAKE_PROFIT')}
        if not orders['ENTRY']: return None
        owner=dict(card_id=trade['cid'],card_digest=life.digest(trade['source']),account=trade['account'],
            role=trade['role'],symbol=trade['symbol'],side=trade['side'],planned_quantity=trade['quantity'],
            prices=deepcopy(trade['prices']),orders=orders,environment='testnet')
        ids={oid for values in orders.values() for oid in values}
        snap=deepcopy(raw)
        for key in ('fills','open_orders','terminal_orders'):snap[key]=[r for r in snap[key] if r['oid'] in ids]
        # The independently normalized lane already proved the net position.
        snap['position_quantity']=life.text(_remaining(trade)*(1 if trade['side']=='LONG' else -1))
        proof=dict(metadata=context['metadata'],lock_proof=trade['condition'],
            observed_stop_requests=[deepcopy(r) for r in state['requests'].values()
                if r['proposal']['card_id']==trade['cid'] and r['proposal']['leg']=='STOP' and r['phase']=='OBSERVED'])
        snap=boundary._owned_snapshot(owner,snap,trade['source'],proof,now)
        view=life.review([owner],snap,now_ms=now)
        allowed={'STOP_COVERAGE_MISSING','STOP_EXCEEDS_CARD_REMAINDER',
            'TAKE_PROFIT_COVERAGE_MISSING','TAKE_PROFIT_EXCEEDS_CARD_REMAINDER','FLAT_WITH_WORKING_ORDERS'}
        if view['bucket_issues'] or set(view['cards'][0]['issues'])-allowed:
            raise RuntimeError('EMERGENCY_EXACT_OWNER_UNPROVEN')
        return dict(bindings=[owner],evidence=dict(snapshot=snap))

    def _maintain_live(self, state, context, now):
        from . import emergency_close
        for trade in sorted(state['trades'].values(),key=lambda t:t['cid']):
            if trade['phase'] in FINAL: continue
            own={**state,'trades':{trade['cid']:trade}}
            view=self._deadline_view(state,trade,context,now) if _remaining(trade)>0 else None
            trigger=emergency_close.trigger(view,now_ms=now) if view else None
            if trigger and trigger['reason']=='STOP_VERIFICATION_DEADLINE':
                trade['emergency_reason']=trigger['reason']
                pending=[r for r in state['requests'].values() if r['proposal']['card_id']==trade['cid']
                    and r['phase'] not in ('OBSERVED','ABORTED_UNSENT')]
                if any(r['proposal']['operation'] not in ('CREATE_EXIT','AMEND_EXIT')
                        or r['proposal']['leg'] not in ('STOP','TAKE_PROFIT')
                        or r['proposal']['action']['type'] not in ('order','batchModify')
                        or wire.requested_order(r['proposal']['action'])['r'] is not True for r in pending):
                    continue
                working=[oid for oid,o in trade['orders'].items() if o['status']=='OPEN' and trade['order_legs'][oid]=='ENTRY']
                if working:
                    proposal=self._cancel(state,trade,working[0],now)
                else:
                    closes=[r for r in state['requests'].values() if r['proposal']['card_id']==trade['cid']
                        and r['proposal']['operation']=='EMERGENCY_CLOSE']
                    if len(closes)>=emergency_close.MAX_REQUESTS: continue
                    if any(r['phase'] not in ('OBSERVED','ABORTED_UNSENT') for r in closes):continue
                    if any(r['phase']=='OBSERVED' and r.get('terminal_state')!='REJECTED_NO_ORDER'
                            and trade['orders'].get(r['observed_oid'],{}).get('status') not in TERMINAL for r in closes):continue
                    sample=self._mark(trade,context,now,max_age=5000)
                    quantity=life.text(_remaining(trade))
                    price=emergency_close.close_price(sample['mark_price'],trade['asset']['decimals'],buy=trade['side']=='SHORT')
                    order=dict(a=trade['asset']['index'],b=trade['side']=='SHORT',p=price,s=quantity,r=True,
                        t=dict(limit=dict(tif='Ioc')),c='0x'+life.digest([VERSION,trade['cid'],'EMERGENCY_CLOSE',len(closes)])[:32])
                    proposal=self._proposal(state,trade,'STOP',dict(type='order',orders=[order],grouping='na'),quantity,now,operation='EMERGENCY_CLOSE')
                    proposal['sample']=dict(mark_price=sample['mark_price'],at_ms=sample['at_ms'])
                proposal['emergency_reason']='STOP_VERIFICATION_DEADLINE'
            else:
                proposal=super()._maintain(own,context,now)
            if proposal is None:continue
            from .experimental_shared_market import proposal_reason
            reason=proposal_reason(state,proposal)
            if reason:
                trade['shared_market_blocked_reason']=reason
                continue
            if self._rejection_key(proposal) in trade.get('rejection_circuit',{}):
                trade['rejected_action_requires_material_change']=True
                continue
            return proposal
        return None

    def _cycle_proposal(self, state, context, now, *, entries_enabled):
        # Reconcile each market independently. An unproven lane cannot block
        # protection in another lane whose full ownership is proven.
        if context.get('basis_revision') != state['revision']:
            raise RuntimeError('CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
        accounts = context.get('inventory_accounts')
        account_errors = context.get('inventory_account_errors', {})
        if (context.get('inventory_complete') is not True or not isinstance(accounts, list)
                or accounts != sorted(set(accounts)) or not set(accounts) <= set(state['routes'].values())
                or not isinstance(account_errors, dict)
                or not (set(accounts) | set(account_errors)) <= set(state['routes'].values())
                or set(accounts) & set(account_errors)
                or type(context.get('inventory_at_ms')) is not int
                or not 0 <= now-context['inventory_at_ms'] <= 15000):
            raise RuntimeError('COMPLETE_FRESH_TWO_ACCOUNT_INVENTORY_REQUIRED')
        scoped = deepcopy(context)
        scoped.setdefault('account_entry_blocked', {}).update(account_errors)
        for account in set(state['routes'].values()) - set(accounts) - set(account_errors):
            scoped['account_entry_blocked'][account] = 'ACCOUNT_INVENTORY_NOT_COLLECTED'
        blocked = scoped.setdefault('blocked_lanes', {})
        observed = set()
        for snapshot in context['snapshots']:
            if snapshot.get('account') not in accounts:
                raise RuntimeError('SNAPSHOT_OUTSIDE_VERIFIED_ACCOUNT_INVENTORY')
            lane = _lane(snapshot['account'], snapshot['symbol'])
            candidate = deepcopy(state)
            try:
                self._snapshot(candidate, snapshot, now)
            except (ValueError, KeyError, TypeError):
                blocked[lane] = 'MARKET_OWNERSHIP_RECONCILIATION_REQUIRED'
                scoped.setdefault('account_entry_blocked', {})[snapshot['account']] = blocked[lane]
                continue
            state.clear(); state.update(candidate); observed.add(lane)
        for trade in state['trades'].values():
            lane = _lane(trade['account'], trade['symbol'])
            if trade['phase'] not in FINAL and lane not in observed:
                blocked[lane] = 'ACTIVE_MARKET_INVENTORY_UNPROVEN'
            if trade['phase'] == 'RETRY_WAIT_UNSENT' and not _source_active(state['sources'][trade['cid']], now):
                trade['phase'] = 'CANCELED_WITHOUT_FILL'
                trade.pop('entry_retry', None)
                state['events'].append(dict(at_ms=now, kind='CANCELED_WITHOUT_FILL',
                    occurrence_id=trade['cid'], reason='ORIGINAL_SOURCE_NO_LONGER_ACTIVE'))
        state['blocked_lanes'] = deepcopy(blocked)
        self._rejections(state, scoped, now)
        for cid, condition in state.get('formula_states', {}).items():
            from . import sol_g65_conditional_stop
            try:
                condition = sol_g65_conditional_stop.advance(condition, scoped.get('bars', {}).get(cid, []),
                    now_ms=now, price_source=state['sources'][cid]['source']['policy']['source_price'])
                state.setdefault('source_condition_errors', {}).pop(cid, None)
            except (ValueError, KeyError, TypeError):
                state.setdefault('source_condition_errors', {})[cid] = 'SOURCE_CANDLE_RECONCILIATION_REQUIRED'
            state['formula_states'][cid] = condition
            if cid in state['trades']: state['trades'][cid]['condition'] = deepcopy(condition)
        # A shallow aggregate keeps the verified trades as shared references;
        # maintenance changes persist while unproven markets remain untouched.
        safe = {**state, 'trades': {cid:t for cid,t in state['trades'].items()
            if _lane(t['account'], t['symbol']) not in blocked}}
        proposal = self._maintain_live(safe, scoped, now)
        unavailable = {r['proposal']['account'] for r in state['requests'].values()
            if r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT')}
        emergency_accounts = {t['account'] for t in state['trades'].values()
            if t['phase'] not in FINAL and t.get('emergency_reason')}
        if proposal is None and entries_enabled:
            for cid in sorted(state['sources'], key=lambda k:(state['sources'][k]['source']['source_at'], k)):
                source = state['sources'][cid]
                if not _source_active(source, now):
                    continue
                role = 'long_account' if source['source']['side'] == 'LONG' else 'short_account'
                account = state['routes'][role]
                if account in unavailable or account in emergency_accounts:
                    _entry_decision(state, cid, scoped, now,
                        'ACCOUNT_OUTCOME_UNKNOWN' if account in unavailable else 'ACCOUNT_PROTECTION_PENDING', scope='account')
                    continue
                proposal = self._admit(state, cid, scoped, now)
                if proposal is not None: break
        if 'collector_checkpoints' in context:
            # Failed lanes retain their previous checkpoint for reconciliation.
            state.setdefault('collector_checkpoints', {}).update(deepcopy(context['collector_checkpoints']))
        if 'ownership_revision' in context:
            state['ownership_revision'] = deepcopy(context['ownership_revision'])
        from .experimental_live_safety import _marker
        marker_context = {**context, 'ownership_revision': context.get('ownership_revision')}
        state['account_inventory_checkpoint'] = _marker(marker_context, context.get('safety_checkpoint_id'))
        return proposal

    def _reserve_live(self, state, proposal, now, nonce):
        from .experimental_shared_market import proposal_reason
        reason=proposal_reason(state,proposal)
        if reason:
            raise RuntimeError(reason)
        rid = life.digest([VERSION, proposal,
            state.get('archived_request_count', 0) + len(state['requests']), now])
        request = dict(request_id=rid, domain='testnet',
            bucket=_lane(proposal['account'], proposal['symbol']), proposal=proposal,
            nonce=nonce, attempt_at_ms=now, prepared_at_ms=now, attempts=1,
            phase='OUTCOME_UNKNOWN', reply=None, observed_oid=None)
        state['requests'][rid] = request
        if proposal['operation'] == 'ENTRY':
            state['trades'][proposal['card_id']]['entry_request'] = rid
        state['events'].append(dict(at_ms=now, kind='ATTEMPT_COMMITTED',
            request_id=rid, occurrence_id=proposal['card_id']))
        return request

    def run_once(self, *, entries_enabled=False):
        """New entries default off; valid release is independently required."""
        if not self._cycle_lock.acquire(blocking=False):
            return dict(status='OBSERVATION_CYCLE_ALREADY_RUNNING', **readiness())
        try:
            return self._run_once(entries_enabled=entries_enabled)
        finally:
            self._cycle_lock.release()

    def _run_once(self, *, entries_enabled):
        owner=getattr(self,'startup_owner',None)
        if owner is not None:
            owner.verify()
        history_ok=self._compact_history()
        before = self.store.load()
        admission_release = self._release(before, self.venue.now())
        collect_entries = (entries_enabled is True and history_ok
            and releases.entry_enabled(admission_release,self.venue.now()))
        context = self.venue.collect(deepcopy(before), entries_enabled=collect_entries)
        now = self.venue.now()
        life.moment(now)
        release = self._release(before, now)
        enabled = (collect_entries and release == admission_release and releases.entry_enabled(release,now))
        # No persistence or I/O in this simulation: the exact same pure
        # transition is checked again after budget admission under SQL lock.
        prepared_state = deepcopy(before)
        proposal = self._cycle_proposal(prepared_state, context, now, entries_enabled=enabled)
        admission = self.dispatch.reserve_transport(proposal) if proposal is not None else None

        def transition(state, nonce=None):
            current = self._cycle_proposal(state, context, now, entries_enabled=enabled)
            if current != proposal:
                raise RuntimeError('CONCURRENT_PROPOSAL_RELOAD_REQUIRED')
            return self._reserve_live(state, current, now, nonce) if current else None

        request = (self.store.commit_attempt(transition, role=proposal['role'], now_ms=now)
            if proposal is not None else self.store.mutate(transition))
        # A committed inventory checkpoint may release a feed generation;
        # collection alone never certifies it. Failure closes only admissions.
        observation_verified = True
        try:
            self.venue.observation_committed(context)
        except Exception:
            observation_verified = False
        if request is None:
            return dict(status='OBSERVED_NO_ACTION' if observation_verified else
                'OBSERVED_SAFETY_CHECKPOINT_PENDING', **readiness())

        # A canceled/expired never-submitted entry keeps a durable tombstone.
        latest_release = self._release(self.store.load(), self.venue.now())
        def final_check(state):
            current = state['requests'][request['request_id']]
            if current != request:
                raise RuntimeError('EXACT_DURABLE_ATTEMPT_REQUIRED')
            if (request['proposal']['operation'] == 'ENTRY'
                    and (not observation_verified or not releases.entry_enabled(latest_release,self.venue.now())
                        or latest_release != release
                        or not _source_active(state['sources'][request['proposal']['card_id']], self.venue.now()))):
                reason = ('OBSERVED_SAFETY_CHECKPOINT_PENDING' if not observation_verified
                    and releases.entry_enabled(latest_release, self.venue.now()) and latest_release == release
                    and _source_active(state['sources'][request['proposal']['card_id']], self.venue.now())
                    else 'SOURCE_OR_RELEASE_CLOSED_BEFORE_TRANSPORT')
                self._abort_entry_unsent(state, current, self.venue.now(), reason)
                return False
            return True
        if not self.store.mutate(final_check):
            return dict(status='CANCELED_BEFORE_TRANSPORT', **readiness())
        admission.bind(request)
        try:
            reply = wire.send_admitted(self.dispatch, request, admission)
        except wire.DefinitelyUnsent as certificate:
            def abort_unsent(state):
                current = state['requests'][request['request_id']]
                if not certificate.matches(current):
                    raise RuntimeError('EXACT_UNSENT_CERTIFICATE_REQUIRED')
                self._abort_entry_unsent(state, current, self.venue.now(), certificate.reason)
            self.store.mutate(abort_unsent)
            return dict(status='DEFINITELY_NOT_SUBMITTED_REOBSERVE', **readiness())
        except Exception:
            # Includes pre-wire errors without an explicit durable unsent
            # certificate. Conservatively reconcile; never repeat the request.
            return dict(status='OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED', **readiness())
        def receipt(state):
            current = state['requests'][request['request_id']]
            if current['phase'] == 'OUTCOME_UNKNOWN':
                current['reply'] = deepcopy(reply)
        self.store.mutate(receipt)
        return dict(status='TESTNET_ATTEMPT_RECORDED_AWAITING_OBSERVATION',
            operation=request['proposal']['operation'], **readiness())

    def report(self):
        """Domain-correct operational status, never a software P/L projection."""
        state = self.store.load()
        from .experimental_shared_market import assess
        from .experimental_execution_cards import cards_from_state
        release = self._release(state, self.venue.now())
        capabilities = readiness()
        capabilities['live_dispatch_enabled'] = release.get('dispatch_enabled') is True
        return dict(domain='testnet', revision=state['revision'],
            entries_enabled=releases.entry_enabled(release, self.venue.now()),
            sources=len(state['sources']), trades=len(state['trades']),
            history=deepcopy(state.get('history', {})),
            history_maintenance_error=getattr(self,'_history_maintenance_error',None),
            shared_market=assess(state),
            unresolved_attempts=sum(r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT')
                for r in state['requests'].values()),
            entry_blocked=deepcopy(state.get('entry_blocked', {})),
            entry_decisions=deepcopy(state.get('entry_decisions', {})),
            cards=cards_from_state(state, domain='testnet'),
            blocked_lanes=deepcopy(state.get('blocked_lanes', {})), **capabilities)
