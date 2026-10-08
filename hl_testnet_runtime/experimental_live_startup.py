"""Default-off local ownership handover; no migration or import-time work.

Legacy exit management remains running until its journal is final and fresh,
complete exchange inventories are flat. The old loop, emergency supervisor and
notification sockets must stop before the new service starts. A durable receipt
allows a later restart to resume new-engine protection with an open position.

This proves ownership inside one worker process. The first production switch
from an older binary still needs independently verified termination of the old
deployment: account inventory cannot prove that a different process has stopped.
Entry activation therefore also requires an operator's predecessor-retired
attestation pinned to this exact release. That assertion must only be installed
after a separately approved deployment verifies predecessor termination; it is
not presented as automatically observed process evidence.
"""
from copy import deepcopy
import json
import os
import threading

from . import card_lifecycle as life
from . import experimental_live_release as releases
from .experimental_execution_dispatch import BoundaryError

HANDOVER = releases.HANDOVER
RECEIPT = 'experimental_startup_handover_v1'
# These failures provide no contradictory ownership/fill facts. Every other
# historical reconciliation error retains its fence until explicitly resolved.
COLD_HISTORY_RETRY_ERRORS = frozenset(('HISTORY_GAP_REQUIRES_REVIEW',
    'EXACT_TRADABLE_MARK_ASSET_REQUIRED','FRESH_TESTNET_MARK_CONTEXT_REQUIRED',
    'VALID_TESTNET_MARK_PRICE_REQUIRED','TESTNET_REQUEST_BUDGET_EXHAUSTED',
    'PUBLIC_READ_UNAVAILABLE','READ_BUDGET_EXCEEDED','ACCOUNT_COLLECTION_EXPIRED',
    'OBSERVATION_TOO_SLOW','ACCOUNT_OBSERVATION_OR_JOURNAL_CHANGED_RETRY',
    'OBSERVATION_CHANGED_RETRY'))
_lock = threading.RLock()
_coordinator = None


def legacy_environment(env):
    """Keep every legacy approval verbatim; change only adapter selection."""
    if (not releases.configured(env)
            or env.get('HL_TESTNET_EXPERIMENTAL_HANDOVER') != HANDOVER
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') != 'false'
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') != 'false'):
        raise BoundaryError('EXPLICIT_PROTECTION_ONLY_HANDOVER_REQUIRED')
    from . import long_stream_runtime as legacy
    result = dict(env)
    result['HL_TESTNET_RUNTIME_MODE'] = legacy.MODE
    # App delivery is separate from protection and must not gain a duplicate
    # worker during ownership transfer. Do not add any legacy trading approval.
    result['HL_TESTNET_APP_DELIVERY'] = ''
    legacy.configuration(result)
    if legacy.short_configuration(result) is None:
        raise BoundaryError('HANDOVER_BOTH_LEGACY_ACCOUNTS_REQUIRED')
    from . import emergency_close
    if (result.get('HL_TESTNET_EMERGENCY_CLOSE') != emergency_close.APPROVAL
            or not emergency_close.continuous_configuration(result)):
        raise BoundaryError('HANDOVER_LEGACY_EMERGENCY_SUPERVISOR_REQUIRED')
    return result


def _legacy_fingerprint_rows(rows):
    """Immutable ownership facts; fresh inventory is a separate requirement."""
    from .long_stream_runtime import _immutable_flat_checkpoint
    for account, values in rows.items():
        if len(values) > 256 or len({r['symbol'] for r in values}) != len(values):
            raise BoundaryError('HANDOVER_LEGACY_INVENTORY_INVALID')
        for row in values:
            if (row['account'] != account or row.get('pending') is not None
                    or row.get('emergency') is not None or 'history_gap_recovery' in row
                    or (row['bindings'] and not _immutable_flat_checkpoint(row))):
                raise BoundaryError('HANDOVER_LEGACY_MANAGEMENT_RETAINED')
    return life.digest({a: [dict(bucket=r['bucket'], bindings=r['bindings'],
                                last_request=r.get('last_request'))
                           for r in values if r['bindings'] or r.get('last_request')]
                        for a, values in rows.items()})


def legacy_fingerprint(store, routes):
    """Read-only finality gate. Pending or ambiguous work retains its manager."""
    if store.entry_blocker() is not None:
        raise BoundaryError('HANDOVER_LEGACY_EMERGENCY_UNRESOLVED')
    return _legacy_fingerprint_rows({a: store.for_account(a) for a in routes.values()})


def handover_receipt_matches(state, fingerprint):
    row = state.get(RECEIPT)
    return bool(isinstance(row, dict) and row.get('schema') == RECEIPT
        and isinstance(row.get('release_id'), str) and len(row['release_id']) == 64
        and all(c in '0123456789abcdef' for c in row['release_id'])
        and row.get('routes') == state['routes']
        and row.get('not_before_ms') == state['not_before_ms']
        and row.get('legacy_fingerprint') == fingerprint
        and type(row.get('flat_verified_at_ms')) is int
        and row['flat_verified_at_ms'] >= state['not_before_ms'])


def retired_legacy_rows(state, rows):
    """Return cold ownership only under the actual persisted handover proof.

    Rows/evidence are not rewritten or timestamped. Callers must compare current
    complete account inventory, including every cold terminal order identity.
    An absent/changed receipt or nonfinal row cannot retire any legacy work.
    """
    if set(rows) != set(state['routes'].values()):
        return {}
    try:
        for account, values in rows.items():
            for value in values:
                reason=state.get('blocked_lanes',{}).get(life.digest([account,value['symbol']]))
                if reason and reason not in COLD_HISTORY_RETRY_ERRORS:
                    return {}
        fingerprint = _legacy_fingerprint_rows(rows)
        if not handover_receipt_matches(state, fingerprint):
            return {}
    except (ValueError, KeyError, TypeError):
        return {}
    return deepcopy(rows)


class LegacyOwner:
    """Adapter over the existing loop, never a second legacy implementation."""
    def __init__(self, env):
        self.env = legacy_environment(env)
        self.owner_lease = None

    def start(self):
        from . import long_stream_runtime as legacy
        legacy.start(protection_env=self.env, owner_lease=self.owner_lease)
        controller, streams = legacy.protection_instance()
        expected = {role: row['account'] for role, row, *_ in streams}
        from . import two_account_execution as roles
        actual = {role: roles.route_for(self.env, role)['account'] for role in roles.ROLES}
        if expected != actual:
            raise BoundaryError('HANDOVER_EXACT_LEGACY_ROUTES_REQUIRED')
        return True

    def stop_and_join(self):
        from .long_stream_runtime import stop_and_join
        return stop_and_join()

    def stop(self):
        from .long_stream_runtime import stop
        stop()


class HandoverCoordinator:
    def __init__(self, env, *, service_factory=None, legacy_owner=None, owner_lease=None):
        self.env = env
        self.legacy = legacy_owner if legacy_owner is not None else LegacyOwner(env)
        self.factory = service_factory or self._compose
        self.service = None
        self.status = 'NOT_STARTED'
        self._legacy_started = False
        self._active = False
        self._thread = None
        self._stop = threading.Event()
        self._cycle = threading.Lock()
        self._start_gate = threading.Lock()
        self._legacy_join_pending = False
        self._candidate_join_pending = False
        self.owner_lease = owner_lease
        self._lease_acquired = False
        if owner_lease is None and legacy_owner is None and service_factory is None:
            from .postgres_journal import PostgresJournal
            from .experimental_runtime_owner import ProcessLease
            self.owner_lease = ProcessLease(PostgresJournal.from_env(env))
        if isinstance(self.legacy, LegacyOwner):
            self.legacy.owner_lease = self.owner_lease

    @staticmethod
    def _compose(env):
        from .experimental_live_service import compose
        return compose(env)

    def _retain_legacy(self):
        with self._start_gate:
            if self._stop.is_set():
                raise BoundaryError('HANDOVER_SHUTDOWN_REQUESTED')
            if not self._legacy_started:
                self.legacy.start()
                self._legacy_started = True

    def _fingerprint(self):
        return legacy_fingerprint(self.service.runtime.venue.legacy_store,
                                  self.service.runtime.store.load()['routes'])

    def _receipt_matches(self, state, fingerprint):
        return handover_receipt_matches(state, fingerprint)

    def _flat(self, fingerprint):
        """Shared-budget snapshots; no source-price or formula dependency."""
        from .card_sync_evidence import PublicReader
        provider = self.service.runtime.venue
        routes = self.service.runtime.store.load()['routes']
        reader = PublicReader(priority='background', budget=provider.budget)
        started = provider.now()
        inventories = []
        for _ in range(2):
            current = {}
            for account in routes.values():
                orders = reader.read('frontendOpenOrders', account)
                positions = reader.read('clearinghouseState', account)
                if (orders != [] or not isinstance(positions, dict)
                        or not isinstance(positions.get('assetPositions'), list)):
                    raise BoundaryError('HANDOVER_COMPLETE_FLAT_INVENTORY_REQUIRED')
                seen = set()
                for row in positions['assetPositions']:
                    p = row.get('position') if isinstance(row, dict) else None
                    if (not isinstance(p, dict) or not isinstance(p.get('coin'), str)
                            or p['coin'] in seen or life.number(p.get('szi'), signed=True) != 0):
                        raise BoundaryError('HANDOVER_COMPLETE_FLAT_INVENTORY_REQUIRED')
                    seen.add(p['coin'])
                current[account] = sorted(seen)
            inventories.append(current)
        if (inventories[0] != inventories[1] or self._fingerprint() != fingerprint
                or not 0 <= provider.now() - started <= 5000):
            raise BoundaryError('HANDOVER_FLAT_OBSERVATION_CHANGED_RETRY')
        return started

    def _activate(self):
        # Operator attestation is pinned to the immutable release. It is not
        # evidence that this process automatically inspected old deployments.
        release = self.service.release_loader()
        if self.owner_lease is not None:
            self.owner_lease.verify()
            self.service.runtime.startup_owner = self.owner_lease
            self.service.runtime.venue.startup_owner = self.owner_lease
        self.service.startup_entries_allowed = (
            self.env.get('HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE') == release['release_id'])
        self.service.startup_entry_gate = lambda: (
            self.env.get('HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE') ==
                self.service.release_loader()['release_id'])
        with self._start_gate:
            if self._stop.is_set():
                raise BoundaryError('HANDOVER_SHUTDOWN_REQUESTED')
            self.service.start()
            self._active = True
        self.status = ('EXPERIMENTAL_OWNER_RUNNING' if self.service.startup_entries_allowed
                       else 'PROTECTION_RUNNING_PREDECESSOR_ATTESTATION_REQUIRED')

    def pass_once(self):
        if not self._cycle.acquire(blocking=False):
            return dict(status='HANDOVER_CYCLE_ALREADY_RUNNING')
        try:
            if self._active or self._stop.is_set():
                return self.health()
            if self.owner_lease is not None:
                try:
                    if not self._lease_acquired:
                        self.owner_lease.acquire()
                        self._lease_acquired = True
                    self.owner_lease.verify()
                except Exception:
                    self.status = 'PROCESS_OWNERSHIP_UNAVAILABLE'
                    return self.health()
            if self._candidate_join_pending:
                if self.service.stop() is not True:
                    self.status = 'CANDIDATE_SHUTDOWN_STILL_PENDING'
                    return self.health()
                self._candidate_join_pending = False
                # Stopped feed/supervisor objects are one-use. Recompose after
                # verified shutdown instead of reviving their stop events.
                self.service = None
            if self._legacy_join_pending:
                if not self.legacy.stop_and_join():
                    self.status = 'LEGACY_SHUTDOWN_STILL_PENDING'
                    return self.health()
                self._legacy_join_pending = False
                self._legacy_started = False
            if self.service is None:
                try:
                    self.service = self.factory(self.env)
                except Exception:
                    if self._stop.is_set():
                        self.status = 'SHUTDOWN_REQUESTED'
                        return self.health()
                    self._retain_legacy()
                    self.status = 'LEGACY_RETAINED_CANDIDATE_UNAVAILABLE'
                    return self.health()
            try:
                fingerprint = self._fingerprint()
                state = self.service.runtime.store.load()
                if self._receipt_matches(state, fingerprint):
                    # A restart can own an open candidate position. Requiring
                    # flat here would abandon its exits after process restart.
                    if self._legacy_started:
                        if not self.legacy.stop_and_join():
                            self._legacy_join_pending = True
                            self.status = 'LEGACY_SHUTDOWN_STILL_PENDING'
                            return self.health()
                        self._legacy_started = False
                    self._activate()
                    return self.health()
                self._retain_legacy()
                fingerprint = self._fingerprint()
                # Old candidate execution without a matching receipt needs
                # explicit migration review, never a fabricated flat receipt.
                if state['trades'] or state['requests']:
                    raise BoundaryError('HANDOVER_UNRECEIPTED_CANDIDATE_REQUIRES_REVIEW')
                self._flat(fingerprint)
                if self._stop.is_set():
                    self.status = 'SHUTDOWN_REQUESTED'
                    return self.health()
                if not self.legacy.stop_and_join():
                    self._legacy_join_pending = True
                    self.status = 'LEGACY_SHUTDOWN_STILL_PENDING'
                    return self.health()
                self._legacy_started = False
                at = self._flat(fingerprint)
                if self._stop.is_set():
                    self.status = 'SHUTDOWN_REQUESTED'
                    return self.health()
                release = self.service.release_loader()
                def commit(value):
                    if (value['routes'] != state['routes'] or value['not_before_ms'] != state['not_before_ms']
                            or value['trades'] or value['requests']):
                        raise BoundaryError('HANDOVER_STATE_CHANGED_RETRY')
                    value[RECEIPT] = dict(schema=RECEIPT, release_id=release['release_id'],
                        routes=deepcopy(value['routes']), not_before_ms=value['not_before_ms'],
                        legacy_fingerprint=fingerprint, flat_verified_at_ms=at)
                self.service.runtime.store.mutate(commit)
                saved = self.service.runtime.store.load()
                if not self._receipt_matches(saved, self._fingerprint()):
                    raise BoundaryError('HANDOVER_COMMIT_READBACK_REQUIRED')
                self._activate()
            except Exception:
                if self._stop.is_set():
                    self.status = 'SHUTDOWN_REQUESTED'
                    return self.health()
                # A source/archive/composition/flatness failure cannot stop the
                # old owner. A startup failure occurs after verified flatness;
                # stop any partially started candidate before restoring legacy.
                if not self._legacy_started:
                    if self.service is not None:
                        if self.service.stop() is not True:
                            self._candidate_join_pending = True
                            self.status = 'CANDIDATE_SHUTDOWN_STILL_PENDING'
                            return self.health()
                        self.service = None
                    self._retain_legacy()
                self.status = 'LEGACY_PROTECTION_RETAINED_HANDOVER_PENDING'
            return self.health()
        finally:
            self._cycle.release()

    def _run(self):
        while not self._stop.is_set() and not self._active:
            try:
                self.pass_once()
            except Exception:
                self.status = 'HANDOVER_REQUIRES_REVIEW'
            self._stop.wait(5)

    def start(self):
        # Gunicorn init must return before slow public reads. The first pass
        # occurs only in this owned background thread, outside the global lock.
        if self._thread is None:
            self._thread = threading.Thread(target=self._run,
                name='experimental-testnet-handover', daemon=True)
            self._thread.start()

    def stop(self):
        with self._start_gate:
            self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        # Do not release the shared owner while the handover pass itself might
        # still be starting/stopping a manager after a timed join.
        if self._thread is not None and self._thread.is_alive():
            return False
        joined = self.service is None or self.service.stop() is True
        if self._legacy_started or self._legacy_join_pending:
            joined = self.legacy.stop_and_join() is True and joined
        if joined and self.owner_lease is not None:
            joined = self.owner_lease.release() is True
        return joined

    def health(self):
        attested = enabled = False
        owner_health = (self.owner_lease.health() if self.owner_lease is not None
                        else dict(status='NOT_ATTACHED', held=False))
        try:
            if self.service is not None:
                release = self.service.release_loader()
                attested = self.env.get('HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE') == release['release_id']
                enabled = self._active and attested and releases.entry_enabled(
                    release, self.service.runtime.venue.now())
        except Exception:
            pass
        if self.owner_lease is not None and owner_health['held'] is not True:
            enabled = False
        return dict(configured=True, running=self._active and (
                self.owner_lease is None or owner_health['held'] is True),
            status=self.status, legacy_protection_retained=self._legacy_started,
            new_entries_enabled=bool(enabled),
            predecessor_retirement_operator_attested=attested,
            process_ownership=owner_health,
            last_cycle_error_code=getattr(self.service,'last_cycle_error_code',None),
            last_cycle_at_ms=getattr(self.service,'last_cycle_at_ms',None),
            last_entry_decisions=deepcopy(getattr(self.service,'last_entry_decisions',{})),
            cross_process_ownership_verified=False)


def start(env=None):
    global _coordinator
    env = os.environ if env is None else env
    if not releases.configured(env):
        return False
    with _lock:
        if _coordinator is not None:
            return False
        coordinator = HandoverCoordinator(env)
        # Publish before scheduling so intake cannot accidentally fall through
        # to the old prospective inbox while startup is incomplete.
        _coordinator = coordinator
    coordinator.start()
    return True


def stop():
    with _lock:
        coordinator = _coordinator
    if coordinator is not None:
        coordinator.stop()


def health():
    with _lock:
        coordinator = _coordinator
    return coordinator.health() if coordinator is not None else dict(
        configured=False, running=False, status='STARTUP_NOT_READY',
        new_entries_enabled=False, cross_process_ownership_verified=False)


def application(environ, start_response):
    with _lock:
        coordinator = _coordinator
    service = coordinator.service if coordinator is not None else None
    if service is not None:
        return service.application(environ, start_response)
    body = json.dumps(dict(status='EXPERIMENTAL_STARTUP_NOT_READY')).encode()
    start_response('503 Service Unavailable', [('Content-Type','application/json'),
        ('Content-Length',str(len(body))), ('Cache-Control','no-store')])
    return [body]
