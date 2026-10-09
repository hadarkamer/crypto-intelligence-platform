"""Concrete feed continuity and independently observed protection capability.

Nothing starts at import or construction. The provider takes real FillWakeups
tokens before REST collection. Each account token can be completed only after
its exact observation has been reloaded from the durable execution store.
The independent supervisor verifies committed observations and runs the same
reviewed lifecycle with entries off whenever maintenance or a fresh read is
needed. Reusing a receipt never renews its original observation timestamp.
"""
from collections import OrderedDict
from copy import deepcopy
from decimal import Decimal
import threading
import time
import uuid

from . import card_lifecycle as life
from . import experimental_live_release as releases
from .experimental_execution_runtime import FINAL, _lane, _remaining, _working_entry_active
from .experimental_live_state import TestnetExecutionState
from .filled_dispatch_store import DispatchStore
from .fill_wakeups import FillWakeups

MAX_CHECKPOINTS = 64
FRESH_MS = 15000
COALESCE_MS = 10000
EMPTY_RECHECK_MS = 60000


class SafetyError(ValueError):
    pass


def _fresh(value, now):
    return type(value) is int and type(now) is int and 0 <= now - value <= FRESH_MS


def _marker(context, checkpoint_id):
    result = dict(at_ms=context['inventory_at_ms'], accounts=context['inventory_accounts'],
                ownership_revision=context['ownership_revision'], safety_checkpoint_id=checkpoint_id)
    for key in ('inventory_account_errors', 'account_inventory_at_ms'):
        if key in context:
            result[key] = deepcopy(context[key])
    return result


class LiveSafetyProvider:
    def __init__(self, *, feed, legacy_store, experimental_store, release_loader, clock=None):
        if (type(feed) is not FillWakeups or type(legacy_store) is not DispatchStore
                or type(experimental_store) is not TestnetExecutionState
                or legacy_store.journal is not experimental_store.journal
                or not callable(release_loader)):
            raise SafetyError('CONCRETE_SHARED_TESTNET_SAFETY_PORTS_REQUIRED')
        self.feed, self.legacy_store, self.store = feed, legacy_store, experimental_store
        self.release_loader = release_loader
        self.clock = clock or (lambda: time.time_ns() // 1000000)
        self._lock = threading.RLock()
        self._begun = OrderedDict()
        self._staged = OrderedDict()
        self._committed = OrderedDict()
        self.supervisor = None

    @staticmethod
    def _bounded(mapping):
        while len(mapping) > MAX_CHECKPOINTS:
            mapping.popitem(last=False)

    def before_collect(self, state):
        """Capture feed generation and revision before ANY exchange account read."""
        if state.get('domain') != 'testnet' or set(state['routes']) != {'long_account', 'short_account'}:
            raise SafetyError('COMPLETE_TESTNET_ROUTES_REQUIRED')
        tokens = {role: self.feed.begin_reconciliation(account) for role, account in state['routes'].items()}
        identity = uuid.uuid4().hex
        with self._lock:
            self._begun[identity] = dict(tokens=tokens, routes=deepcopy(state['routes']),
                basis_revision=state['revision'], started_ms=self.clock(),
                not_before_ms=state['not_before_ms'])
            self._bounded(self._begun)
        return identity

    def stage_observation(self, token, *, state, context):
        """Keep bounded evidence; staging does not clear the feed entry gate."""
        with self._lock:
            begun = self._begun.pop(token, None)
            if (begun is None or state['routes'] != begun['routes']
                    or state['revision'] != begun['basis_revision']
                    or context.get('basis_revision') != state['revision']
                    or context.get('inventory_complete') is not True
                    or not isinstance(context.get('inventory_accounts'), list)
                    or context['inventory_accounts'] != sorted(set(context['inventory_accounts']))
                    or not set(context['inventory_accounts']) <= set(state['routes'].values())
                    or not _fresh(context.get('inventory_at_ms'), self.clock())
                    or context['inventory_at_ms'] < begun['started_ms']
                    or not isinstance(context.get('collector_checkpoints'), dict)):
                raise SafetyError('COMPLETE_CURRENT_ACCOUNT_OBSERVATION_REQUIRED')
            life.ident(context.get('ownership_revision'), r'[0-9a-f]{64}')
            account_times = context.get('account_inventory_at_ms', {})
            if (not isinstance(account_times, dict)
                    or (account_times and set(account_times) != set(context['inventory_accounts']))
                    or any(not _fresh(at, self.clock()) or at < begun['started_ms']
                           for at in account_times.values())):
                raise SafetyError('EXACT_ACCOUNT_OBSERVATION_TIMES_REQUIRED')
            identity = life.digest([token, context['inventory_at_ms'], context['ownership_revision']])
            # Inventory/ownership errors prevent only that account's receipt.
            # Candidate-only market eligibility is not ownership uncertainty.
            complete = set(context['inventory_accounts']) - set(context.get('account_entry_blocked', {}))
            complete -= set(context.get('inventory_account_errors', {}))
            self._staged[identity] = dict(**begun, context=deepcopy(context), complete_accounts=complete,
                marker=_marker(context, identity), checkpoint_id=identity)
            self._bounded(self._staged)
            return identity

    def _durable_matches(self, receipt, state, *, max_age_ms=FRESH_MS):
        context = receipt['context']
        if (state.get('domain') != 'testnet' or state['routes'] != receipt['routes']
                or state['not_before_ms'] != receipt['not_before_ms']
                or state['revision'] <= receipt['basis_revision']
                or state.get('account_inventory_checkpoint') != receipt['marker']
                or state.get('ownership_revision') != context['ownership_revision']
                or not 0 <= self.clock() - context['inventory_at_ms'] <= max_age_ms):
            return False
        saved = state.get('collector_checkpoints', {})
        return all(saved.get(lane) == observation
                   for lane, observation in context['collector_checkpoints'].items())

    def after_committed(self, *, checkpoint_id, state):
        """Only a fresh exact read-back permits completing the real feed token."""
        try:
            # Do not accept a caller assertion that a transaction committed.
            durable = self.store.load()
            if durable != state:
                return False
            with self._lock:
                receipt = self._staged.pop(checkpoint_id, None)
                if receipt is None or not self._durable_matches(receipt, durable):
                    return False
                finished = {role: (token.account in receipt['complete_accounts']
                            and self.feed.finish_reconciliation(token, complete=True))
                            for role, token in receipt['tokens'].items()}
                receipt['finished_accounts'] = {receipt['routes'][role] for role, ok in finished.items() if ok}
                receipt['feed_finished'] = all(finished.values())
                self._committed[checkpoint_id] = receipt
                self._bounded(self._committed)
                # This return certifies durable receipt registration only.
                # Entry authority is account-specific in verify_checkpoint;
                # one unfinished feed must not abort the other account.
                return True
        except Exception:
            return False

    def _receipt(self, state, *, max_age_ms=FRESH_MS):
        identity = state.get('account_inventory_checkpoint', {}).get('safety_checkpoint_id')
        with self._lock:
            receipt = self._committed.get(identity)
            if receipt is None or not self._durable_matches(receipt, state, max_age_ms=max_age_ms):
                return None
            return deepcopy(receipt)

    def _continuous(self, receipt, account=None):
        wanted = set(receipt['routes'].values()) if account is None else {account}
        if not wanted <= receipt.get('finished_accounts', set()):
            return False
        health = self.feed.health()
        for role, token in receipt['tokens'].items():
            if token.account not in wanted:
                continue
            row = health.get(role, {})
            if (row.get('generation'), row.get('revision')) != (token.generation, token.revision):
                return False
            if not self.feed.entry_allowed(token.account):
                return False
        return True

    def _legacy_clear(self, state, ownership_revision, account=None, *, account_ownership_revision=None):
        rows = {a: self.legacy_store.for_account(a) for a in state['routes'].values()}
        checked_rows = rows if account_ownership_revision is None else {account: rows[account]}
        revision = life.digest({a: [dict(bucket=s['bucket'], revision=s['revision'], pending=s['pending'])
                                   for s in values] for a, values in checked_rows.items()})
        if revision != (ownership_revision if account_ownership_revision is None else account_ownership_revision):
            return False
        from .experimental_live_startup import retired_legacy_rows
        retired = retired_legacy_rows(state, rows)
        # An old worker with pending work has not handed over safely. Existing
        # legacy exposure is intentionally a handover blocker for new entries;
        # the new supervisor cannot claim to service the legacy controller.
        for owner, values in rows.items():
            if account is not None and owner != account:
                continue
            for value in values:
                if (value.get('pending') is not None or value.get('emergency') is not None
                        or 'history_gap_recovery' in value):
                    return False
                if value in retired.get(owner, []):
                    continue
                if value['bindings']:
                    from .experimental_live_provider import _only, _position
                    snapshot = state.get('collector_checkpoints', {}).get(_lane(owner, value['symbol']))
                    if snapshot is None:
                        return False
                    ids = {oid for b in value['bindings'] for orders in b['orders'].values() for oid in orders}
                    selected = _only(snapshot, ids, position=life.text(_position(
                        [fill for fill in snapshot['fills'] if fill['oid'] in ids])))
                    report = life.review(value['bindings'], selected, now_ms=self.clock())
                    if report['bucket_issues'] or any(c['state'] not in FINAL or c['issues'] for c in report['cards']):
                        return False
        return True

    def _peers_protected(self, state, *, request_id=None, account=None):
        if any(rid != request_id and r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT')
               and (account is None or r.get('proposal', {}).get('account') in (None, account))
               for rid, r in state['requests'].items()):
            return False
        for trade in state['trades'].values():
            if trade['phase'] in FINAL or (account is not None and trade['account'] != account):
                continue
            from . import experimental_external_activity as external
            if external.managed(state,trade['account'],trade['symbol']):
                # The operator explicitly owns this coin's further management;
                # the bot has no authority to resize/cancel/reopen its orders.
                # Unknown bot submissions remain checked above independently.
                continue
            if _lane(trade['account'], trade['symbol']) in state.get('blocked_lanes', {}):
                return False
            remaining = _remaining(trade)
            if remaining < 0 or trade.get('emergency_reason'):
                return False
            active = [(oid, row) for oid, row in trade['orders'].items() if row['status'] == 'OPEN']
            if remaining == 0:
                if any(trade['order_legs'][oid] != 'ENTRY' for oid, row in active):
                    return False
                continue
            lane = _lane(trade['account'], trade['symbol'])
            if not _fresh(state.get('snapshots', {}).get(lane, {}).get('at_ms'), self.clock()):
                return False
            for leg, price in (('STOP', trade['desired_stop']), ('TAKE_PROFIT', trade['prices']['take_profit'])):
                owned = [row for oid, row in active if trade['order_legs'][oid] == leg]
                if len(owned) != 1:
                    return False
                row = owned[0]
                unfilled = life.number(row['wire_order']['s']) - sum(
                    (life.number(f['quantity']) for f in row['fills']), Decimal(0))
                if (unfilled != remaining or row['wire_order']['r'] is not True
                        or life.number(row['wire_order']['p']) != life.number(price)):
                    return False
        return True

    def verify_exit_observation(self, *, state, request):
        """A newly received relevant event invalidates the prepared exit size.

        This consumes existing feed tokens, not REST. A disconnected feed alone
        retains the existing reduce-only fallback; a known newer same-market
        event is affirmative evidence that the current snapshot is stale.
        """
        proposal=request['proposal']
        if proposal['operation']=='ENTRY':return
        receipt=self._receipt(state)
        if receipt is None:return
        account=proposal['account']
        role=next((r for r,a in receipt['routes'].items() if a==account),None)
        if role is None:return
        token=receipt['tokens'][role]
        current=self.feed.health().get(role,{})
        dirty=self.feed.dirty_symbols(account)
        changed=((current.get('generation')==token.generation and current.get('revision')!=token.revision)
                 or (current.get('generation')!=token.generation and current.get('connected') is True
                     and current.get('snapshot_received') is True))
        if (changed
                and (dirty is None or proposal['symbol'] in dirty)):
            raise SafetyError('ACCOUNT_ACTIVITY_CHANGED_REOBSERVE_BEFORE_EXIT')

    def verify_checkpoint(self, *, state, request, collected_at_ms, ownership_revision):
        proposal = request['proposal']
        result = dict(account=proposal['account'], role=proposal['role'], at_ms=collected_at_ms,
            entry_enabled=False, emergency_healthy=False, feed_reconciled=False,
            entry_circuit_clear=False, supervisor_at_ms=0, not_before_ms=state['not_before_ms'])
        # Feed/supervisor outage never blocks verified reduce-only protection.
        if proposal['operation'] != 'ENTRY':
            return result
        try:
            receipt = self._receipt(state)
            if (receipt is None or receipt['context']['inventory_at_ms'] != collected_at_ms
                    or receipt['context']['ownership_revision'] != ownership_revision
                    or state['requests'].get(request['request_id']) != request):
                return result
            release = self.release_loader()
            result['entry_enabled'] = bool(release and release['routes'] == state['routes']
                and release['not_before_ms'] == state['not_before_ms']
                and release['protection_enabled'] is True and release['dispatch_enabled'] is True
                and releases.entry_enabled(release, self.clock()))
            account = proposal['account']
            if self.supervisor is not None:
                # One verification covers feed continuity, legacy ownership and
                # peer protection for this exact state. Health still rechecks
                # the feed and supervisor liveness before granting authority.
                verified = self.supervisor._verified_accounts(state, receipt, request_id=request['request_id'])
                result['feed_reconciled'] = result['entry_circuit_clear'] = account in verified
                self.supervisor._record_observation(state, verified)
                health = self.supervisor.health(account=account)
                result['emergency_healthy'] = health['healthy']
                result['supervisor_at_ms'] = health['observed_at_ms'] or 0
        except Exception:
            # Reset every gate after any proof failure; never expose raw errors.
            result.update(entry_enabled=False, emergency_healthy=False,
                          feed_reconciled=False, entry_circuit_clear=False, supervisor_at_ms=0)
        return result


class IndependentProtectionSupervisor:
    """Independent durable-state validator and entries-off fallback worker.

    A normal worker's committed observation is usable evidence, not another
    observation. Only a new REST collection can advance its timestamp. Failed
    protection/feed proofs trigger this worker's own maintenance regardless of
    whether the source worker is busy, stalled, or has entries disabled.
    """
    def __init__(self, runtime, safety, *, interval_seconds=5):
        from .experimental_live_runtime import TestnetExecutionRuntime
        if (type(runtime) is not TestnetExecutionRuntime or type(safety) is not LiveSafetyProvider
                or runtime.store is not safety.store or runtime.venue.safety is not safety
                or type(interval_seconds) not in (int, float) or not 1 <= interval_seconds <= 5
                or safety.supervisor is not None):
            raise SafetyError('INDEPENDENT_CONCRETE_PROTECTION_SUPERVISOR_REQUIRED')
        self.runtime, self.safety, self.interval = runtime, safety, interval_seconds
        self.primary_service = None
        self._lock = threading.RLock()
        self._pass_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._observed_at_ms = None
        self._account_proofs = {}
        self._status = 'SUPERVISOR_NOT_STARTED'
        safety.supervisor = self

    def _verified_accounts(self, state, receipt, *, request_id=None):
        if receipt is None:
            return {}
        verified = {}
        for account in receipt['context']['inventory_accounts']:
            # Expired observations cannot become health proofs, so do not
            # repeat ownership reads and protection checks for them.
            at = receipt['context'].get('account_inventory_at_ms', {}).get(
                account, receipt['context']['inventory_at_ms'])
            if not _fresh(at, self.safety.clock()):
                continue
            if (self.safety._continuous(receipt, account)
                    and self.safety._peers_protected(state, account=account, request_id=request_id)
                    and self.safety._legacy_clear(state, receipt['context']['ownership_revision'], account,
                        account_ownership_revision=receipt['context'].get('legacy_account_revisions', {}).get(account))):
                # Preserve the actual read's time, including when a later
                # independent pass merely verifies its persisted proof.
                verified[account] = dict(at_ms=at, checkpoint_id=receipt['checkpoint_id'],
                    token=deepcopy(next(t for t in receipt['tokens'].values() if t.account == account)))
        return verified

    def _record_observation(self, state, verified):
        with self._lock:
            # Replace, never merge: missing or failed account proof does not
            # inherit authority from a previously healthy observation.
            self._account_proofs = verified
            complete = set(verified) == set(state['routes'].values())
            self._observed_at_ms = min((p['at_ms'] for p in verified.values()), default=None)
            self._status = ('SUPERVISOR_OBSERVATION_VERIFIED' if complete else
                'SUPERVISOR_ACCOUNT_PARTIALLY_VERIFIED' if verified else 'SUPERVISOR_RECONCILIATION_REQUIRED')

    def _can_reuse(self, state, receipt, verified):
        if receipt is None or self._feed_changed(receipt):
            return False
        # A partial observation may keep the other account's health, but an
        # outstanding live request/protection must still get immediate work.
        if not self.safety._peers_protected(state):
            return False
        if not any(t['phase'] not in FINAL for t in state['trades'].values()):
            # Empty account audits need not run at live-position frequency.
            # This only skips work: health and entry proofs still expire at
            # FRESH_MS, and every actionable source requires a new collection.
            return (set(state['routes'].values()) <= receipt['complete_accounts']
                and self.safety._continuous(receipt)
                and (set(state['routes'].values()) <= set(verified)
                    or self.safety._legacy_clear(state, receipt['context']['ownership_revision']))
                and 0 <= self.safety.clock() - receipt['context']['inventory_at_ms'] < EMPTY_RECHECK_MS)
        if not verified:
            return False
        for t in state['trades'].values():
            if t['phase'] in FINAL:
                continue
            # A known, unchanged GTC order with no fills needs only the same
            # ten-second poll as settled protection. Feed changes, retirement
            # and every unresolved request still force immediate observation.
            request = state['requests'].get(t.get('entry_request'), {})
            if (t['source']['family'] == 'maxpain' and t['account'] in verified
                    and t['phase'] == 'OUTCOME_UNKNOWN' and not t['entry_fills'] and not t['exit_fills']
                    and not t.get('entry_retired_reason')
                    and _working_entry_active(state['sources'][t['cid']], self.safety.clock())
                    and request.get('phase') == 'OBSERVED' and len(t['orders']) == 1
                    and all(o['status'] == 'OPEN' and not o['fills']
                        and t['order_legs'][oid] == 'ENTRY' and request.get('observed_oid') == oid
                        and o['wire_order']['t'] == {'limit': {'tif': 'Gtc'}}
                        for oid, o in t['orders'].items())):
                continue
            if (t['phase'] != 'OPEN' or _remaining(t) <= 0
                    or t['source']['family'] in ('r2732', 'sol_g65')
                    or any(o['status'] == 'OPEN' and t['order_legs'][oid] == 'ENTRY'
                        for oid, o in t['orders'].items())):
                # Entry settlement, partial exits and final cleanup still
                # require the next lifecycle pass without coalescing.
                return False
        return 0 <= self.safety.clock() - receipt['context']['inventory_at_ms'] < COALESCE_MS

    def _feed_changed(self, receipt):
        health = self.safety.feed.health()
        return any((health.get(role, {}).get('generation'), health.get(role, {}).get('revision'))
               != (token.generation, token.revision) for role, token in receipt['tokens'].items()
               if token.account in receipt['context']['inventory_accounts'])

    def idle_receipt(self):
        """Reuse idle or settled protected inventory until work or feed changes."""
        from .experimental_execution_runtime import _source_active
        state = self.safety.store.load()
        now = self.safety.clock()
        waits = state.get('entry_decisions', {})
        def ready(cid, row):
            if cid in state['trades'] and state['trades'][cid]['phase'] != 'RETRY_WAIT_UNSENT':
                return False
            decision = waits.get(cid, {})
            deferred = (decision.get('reason') in ('TESTNET_REQUEST_BUDGET_EXHAUSTED',
                'TESTNET_REQUEST_BUDGET_BUSY', 'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',
                'TESTNET_OBSERVATION_BATCH_EXPIRED')
                and type(decision.get('retry_after_ms')) is int and now < decision['retry_after_ms'])
            return _source_active(row, now) and not deferred
        if (any(r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT') for r in state['requests'].values())
                or any(ready(cid, row) for cid, row in state['sources'].items())):
            return None
        receipt = self.safety._receipt(state, max_age_ms=EMPTY_RECHECK_MS)
        verified = self._verified_accounts(state, receipt)
        required = {t['account'] for t in state['trades'].values() if t['phase'] not in FINAL}
        if (not required <= set(verified)
                or not self._can_reuse(state, receipt, verified)):
            return None
        return receipt['checkpoint_id']

    def pass_once(self, *, force_collect=False):
        """A manual pass can test maintenance but cannot advertise a live loop."""
        if not self._pass_lock.acquire(blocking=False):
            return dict(status='SUPERVISOR_CYCLE_ALREADY_RUNNING')
        try:
            state = self.safety.store.load()
            receipt = self.safety._receipt(state, max_age_ms=EMPTY_RECHECK_MS)
            verified = self._verified_accounts(state, receipt)
            if force_collect or not self._can_reuse(state, receipt, verified):
                primary = self.primary_service
                if not force_collect and primary is not None and primary.primary_available():
                    # Only a new feed event wakes the normal loop. Pending
                    # lifecycle work already has its next scheduled tick;
                    # waking it again would repeat the just-completed scan.
                    if receipt is not None and self._feed_changed(receipt):
                        primary._wake.set()
                else:
                    self.runtime.run_once(entries_enabled=False)
                    state = self.safety.store.load()
                    receipt = self.safety._receipt(state)
                    verified = self._verified_accounts(state, receipt)
            self._record_observation(state, verified)
        except Exception:
            with self._lock:
                self._observed_at_ms = None
                self._account_proofs = {}
                self._status = 'SUPERVISOR_RECONCILIATION_REQUIRED'
        finally:
            self._pass_lock.release()
        return dict(status=self._status)

    def health(self, *, account=None):
        with self._lock:
            routes = self.safety.feed._routes
            accounts = set(routes.values()) if account is None else {account}
            proofs = [self._account_proofs.get(a) for a in accounts]
            current = self.safety.feed.health()
            valid = all(p is not None and _fresh(p['at_ms'], self.safety.clock())
                and any(row.get('generation') == p['token'].generation
                    and row.get('revision') == p['token'].revision
                    and row.get('entry_allowed') is True
                    for role, row in current.items() if routes[role] == p['token'].account)
                for p in proofs)
            at = min((p['at_ms'] for p in proofs if p is not None), default=None)
            return dict(healthy=bool(self._thread is not None and self._thread.is_alive()
                and not self._stop.is_set() and valid),
                observed_at_ms=at, status=self._status)

    def _run(self):
        while not self._stop.is_set():
            # Clear BEFORE collection. A fill arriving during REST/commit
            # stays set and schedules an immediate subsequent safety pass.
            # This sole feed-event consumer coalesces bursts; every pass uses
            # the existing shared REST/action request budget unchanged.
            self.safety.feed.wake_event.clear()
            self.pass_once()
            self.safety.feed.wake_event.wait(self.interval)

    def start(self):
        with self._lock:
            if self._stop.is_set():
                raise SafetyError('STOPPED_SUPERVISOR_REQUIRES_NEW_STARTUP')
            if self._thread is not None:
                return False
            self._thread = threading.Thread(target=self._run, name='experimental-protection-supervisor', daemon=True)
            self._thread.start()
            return True

    def stop(self):
        self._stop.set()
        self.safety.feed.wake_event.set()
        with self._lock:
            self._observed_at_ms = None
            self._account_proofs = {}
            self._status = 'SUPERVISOR_STOPPED'
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
