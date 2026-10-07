"""Concrete feed continuity and independently observed protection capability.

Nothing starts at import or construction. The provider takes real FillWakeups
tokens before REST collection. Tokens can be completed only after the exact
two-account observation has been reloaded from the durable execution store.
The independent supervisor runs the same reviewed lifecycle with entries off;
its health records an actual committed observation, never a timer tick.
"""
from collections import OrderedDict
from copy import deepcopy
from decimal import Decimal
import threading
import time
import uuid

from . import card_lifecycle as life
from . import experimental_live_release as releases
from .experimental_execution_runtime import FINAL, _lane, _remaining
from .experimental_live_state import TestnetExecutionState
from .filled_dispatch_store import DispatchStore
from .fill_wakeups import FillWakeups

MAX_CHECKPOINTS = 64
FRESH_MS = 15000


class SafetyError(ValueError):
    pass


def _fresh(value, now):
    return type(value) is int and type(now) is int and 0 <= now - value <= FRESH_MS


def _marker(context, checkpoint_id):
    return dict(at_ms=context['inventory_at_ms'], accounts=context['inventory_accounts'],
                ownership_revision=context['ownership_revision'], safety_checkpoint_id=checkpoint_id)


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
                    or context.get('inventory_accounts') != sorted(state['routes'].values())
                    or not _fresh(context.get('inventory_at_ms'), self.clock())
                    or context['inventory_at_ms'] < begun['started_ms']
                    or not isinstance(context.get('collector_checkpoints'), dict)):
                raise SafetyError('COMPLETE_CURRENT_ACCOUNT_OBSERVATION_REQUIRED')
            life.ident(context.get('ownership_revision'), r'[0-9a-f]{64}')
            identity = life.digest([token, context['inventory_at_ms'], context['ownership_revision']])
            # A blocked lane means some account ownership/exposure was not
            # fully reconciled. It can never claim feed reconciliation.
            complete = not context.get('blocked_lanes') and not context.get('account_entry_blocked')
            self._staged[identity] = dict(**begun, context=deepcopy(context), complete=complete,
                marker=_marker(context, identity), checkpoint_id=identity)
            self._bounded(self._staged)
            return identity

    def _durable_matches(self, receipt, state):
        context = receipt['context']
        if (state.get('domain') != 'testnet' or state['routes'] != receipt['routes']
                or state['not_before_ms'] != receipt['not_before_ms']
                or state['revision'] <= receipt['basis_revision']
                or state.get('account_inventory_checkpoint') != receipt['marker']
                or state.get('ownership_revision') != context['ownership_revision']
                or not _fresh(context['inventory_at_ms'], self.clock())
                or not receipt['complete']):
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
                finished = {role: self.feed.finish_reconciliation(token, complete=True)
                            for role, token in receipt['tokens'].items()}
                receipt['feed_finished'] = all(finished.values())
                self._committed[checkpoint_id] = receipt
                self._bounded(self._committed)
                return receipt['feed_finished']
        except Exception:
            return False

    def _receipt(self, state):
        identity = state.get('account_inventory_checkpoint', {}).get('safety_checkpoint_id')
        with self._lock:
            receipt = self._committed.get(identity)
            if receipt is None or not self._durable_matches(receipt, state):
                return None
            return deepcopy(receipt)

    def _continuous(self, receipt):
        if not receipt['feed_finished']:
            return False
        health = self.feed.health()
        for role, token in receipt['tokens'].items():
            row = health.get(role, {})
            if (row.get('generation'), row.get('revision')) != (token.generation, token.revision):
                return False
            if not self.feed.entry_allowed(token.account):
                return False
        return True

    def _legacy_clear(self, state, ownership_revision):
        if self.legacy_store.entry_blocker() is not None:
            return False
        rows = {a: self.legacy_store.for_account(a) for a in state['routes'].values()}
        revision = life.digest({a: [dict(bucket=s['bucket'], revision=s['revision'], pending=s['pending'])
                                   for s in values] for a, values in rows.items()})
        if revision != ownership_revision:
            return False
        # An old worker with pending work has not handed over safely. Existing
        # legacy exposure is intentionally a handover blocker for new entries;
        # the new supervisor cannot claim to service the legacy controller.
        for account, values in rows.items():
            for value in values:
                if value.get('pending') is not None or value.get('emergency') is not None:
                    return False
                if value['bindings']:
                    from .experimental_live_provider import _only, _position
                    snapshot = state.get('collector_checkpoints', {}).get(_lane(account, value['symbol']))
                    if snapshot is None:
                        return False
                    ids = {oid for b in value['bindings'] for orders in b['orders'].values() for oid in orders}
                    selected = _only(snapshot, ids, position=life.text(_position(
                        [fill for fill in snapshot['fills'] if fill['oid'] in ids])))
                    report = life.review(value['bindings'], selected, now_ms=self.clock())
                    if report['bucket_issues'] or any(c['state'] not in FINAL or c['issues'] for c in report['cards']):
                        return False
        return True

    def _peers_protected(self, state, *, request_id=None):
        if state.get('blocked_lanes'):
            return False
        if any(rid != request_id and r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT')
               for rid, r in state['requests'].items()):
            return False
        for trade in state['trades'].values():
            if trade['phase'] in FINAL:
                continue
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
            result['feed_reconciled'] = self._continuous(receipt)
            result['entry_circuit_clear'] = (self._legacy_clear(state, ownership_revision)
                and self._peers_protected(state, request_id=request['request_id']))
            if self.supervisor is not None:
                health = self.supervisor.health()
                result['emergency_healthy'] = health['healthy']
                result['supervisor_at_ms'] = health['observed_at_ms'] or 0
        except Exception:
            # Reset every gate after any proof failure; never expose raw errors.
            result.update(entry_enabled=False, emergency_healthy=False,
                          feed_reconciled=False, entry_circuit_clear=False, supervisor_at_ms=0)
        return result


class IndependentProtectionSupervisor:
    """One explicitly started worker; every pass uses entries_enabled=False."""
    def __init__(self, runtime, safety, *, interval_seconds=5):
        from .experimental_live_runtime import TestnetExecutionRuntime
        if (type(runtime) is not TestnetExecutionRuntime or type(safety) is not LiveSafetyProvider
                or runtime.store is not safety.store or runtime.venue.safety is not safety
                or type(interval_seconds) not in (int, float) or not 1 <= interval_seconds <= 5
                or safety.supervisor is not None):
            raise SafetyError('INDEPENDENT_CONCRETE_PROTECTION_SUPERVISOR_REQUIRED')
        self.runtime, self.safety, self.interval = runtime, safety, interval_seconds
        self._lock = threading.RLock()
        self._pass_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._observed_at_ms = None
        self._status = 'SUPERVISOR_NOT_STARTED'
        safety.supervisor = self

    def pass_once(self):
        """A manual pass can test maintenance but cannot advertise a live loop."""
        if not self._pass_lock.acquire(blocking=False):
            return dict(status='SUPERVISOR_CYCLE_ALREADY_RUNNING')
        try:
            result = self.runtime.run_once(entries_enabled=False)
            state = self.safety.store.load()
            receipt = self.safety._receipt(state)
            # The provider cache is thread-local: a concurrent normal worker's
            # newer commit is not proof that THIS independent pass succeeded.
            own = self.runtime.venue._last
            own_id = own['context'].get('safety_checkpoint_id') if own else None
            verified = (receipt is not None and receipt['checkpoint_id'] == own_id
                and self.safety._continuous(receipt)
                and self.safety._peers_protected(state)
                and self.safety._legacy_clear(state, receipt['context']['ownership_revision'])
                and result.get('status') == 'OBSERVED_NO_ACTION')
            with self._lock:
                self._observed_at_ms = receipt['context']['inventory_at_ms'] if verified else None
                self._status = 'SUPERVISOR_OBSERVATION_VERIFIED' if verified else 'SUPERVISOR_RECONCILIATION_REQUIRED'
        except Exception:
            with self._lock:
                self._observed_at_ms = None
                self._status = 'SUPERVISOR_RECONCILIATION_REQUIRED'
        finally:
            self._pass_lock.release()
        return dict(status=self._status)

    def health(self):
        with self._lock:
            return dict(healthy=bool(self._thread is not None and self._thread.is_alive()
                and not self._stop.is_set() and _fresh(self._observed_at_ms, self.safety.clock())),
                observed_at_ms=self._observed_at_ms, status=self._status)

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
            self._status = 'SUPERVISOR_STOPPED'
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
