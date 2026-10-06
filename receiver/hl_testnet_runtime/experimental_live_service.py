"""Reviewed composition and authenticated intake for experimental execution.

This module does not start threads at import. ``compose`` builds the concrete
archive price and safety providers; unimplemented evidence must not become an
environment flag claiming readiness. Explicit startup owns thread registration.
Schema initialization remains a separate operator migration, never an HTTP or
recurring-work side effect. No deployment/activation settings are written here.
"""
import json
import threading

import experimental_execution_contract as contract
from . import experimental_plan_intake as intake
from . import experimental_live_release as releases
from .experimental_execution_dispatch import BoundaryError
from .postgres_journal import JournalError


class ConnectionService:
    def __init__(self, runtime, *, key, release_loader, feed=None, supervisor=None):
        from .experimental_live_runtime import TestnetExecutionRuntime
        if (type(runtime) is not TestnetExecutionRuntime
                or not isinstance(key, str) or not intake.HEX.fullmatch(key)
                or not callable(release_loader)):
            raise BoundaryError('EXPLICIT_TESTNET_CONNECTION_SERVICE_REQUIRED')
        self.runtime, self.key, self.release_loader = runtime, key, release_loader
        self._run_lock = threading.Lock()
        self.last_status = 'NOT_STARTED'
        self.cycles = 0
        self.feed, self.supervisor = feed, supervisor
        self._thread = None
        self._stop = threading.Event()
        self._start_lock = threading.Lock()
        self.startup_entries_allowed = True
        self.startup_entry_gate = None

    def ingest(self, message, *, now, not_before):
        # intake.accept calls this after schema/authentication checks. This is
        # an inbox operation only; source delivery can never call run_once.
        state = self.runtime.store.load()
        if (contract.moment_ms(not_before) != state['not_before_ms']
                or contract.moment_ms(now) != self.runtime.venue.now()):
            raise BoundaryError('EXACT_SOURCE_INTAKE_FENCE_REQUIRED')
        return self.runtime.receive([message])['receipts'][0]

    def accept_authenticated(self, raw, headers, *, path=intake.PATH, method='POST'):
        if method != 'POST' or path != intake.PATH:
            raise BoundaryError('EXACT_PROSPECTIVE_INTAKE_PATH_REQUIRED')
        now = self.runtime.venue.now()
        if not intake.authenticate(self.key, headers.get('X-Plan-Timestamp'),
                                   headers.get('X-Plan-Signature'), raw, now / 1000):
            raise BoundaryError('EXPERIMENTAL_GATEWAY_AUTHENTICATION_REQUIRED')
        release = self.release_loader()
        state = self.runtime.store.load()
        if release['routes'] != state['routes'] or release['not_before_ms'] != state['not_before_ms']:
            raise BoundaryError('EXACT_SOURCE_INTAKE_FENCE_REQUIRED')
        # Do not impose entry expiry on cancellations or existing protection.
        # receive applies the original source's freshness and immutable rules.
        message = contract.validate(intake.decoded(raw))
        return self.runtime.receive([message])['receipts'][0]

    def application(self, environ, start_response):
        status, value = '404 Not Found', dict(status='NOT_FOUND')
        if environ.get('PATH_INFO') == intake.PATH:
            try:
                if environ.get('REQUEST_METHOD') != 'POST' or environ.get('QUERY_STRING'):
                    status, value = '405 Method Not Allowed', dict(status='POST_ONLY')
                elif environ.get('CONTENT_TYPE') != 'application/json':
                    status, value = '415 Unsupported Media Type', dict(status='JSON_REQUIRED')
                else:
                    size = environ.get('CONTENT_LENGTH', '')
                    if not isinstance(size, str) or not size.isdecimal() or not 0 < int(size) <= intake.MAX_BYTES:
                        status, value = '413 Content Too Large', dict(status='BOUNDED_BODY_REQUIRED')
                    else:
                        raw = environ['wsgi.input'].read(int(size))
                        if len(raw) != int(size):
                            raise BoundaryError('EXPERIMENTAL_BODY_INVALID')
                        value = self.accept_authenticated(raw, {
                            'X-Plan-Timestamp': environ.get('HTTP_X_PLAN_TIMESTAMP'),
                            'X-Plan-Signature': environ.get('HTTP_X_PLAN_SIGNATURE')})
                        status = '200 OK'
            except BoundaryError as exc:
                status, value = (('403 Forbidden', dict(status='AUTHENTICATION_REQUIRED'))
                    if str(exc) == 'EXPERIMENTAL_GATEWAY_AUTHENTICATION_REQUIRED'
                    else ('503 Service Unavailable', dict(status='EXPERIMENTAL_RECORDING_UNAVAILABLE')))
            except JournalError:
                status, value = '503 Service Unavailable', dict(status='EXPERIMENTAL_RECORDING_UNAVAILABLE')
            except (ValueError, TypeError, KeyError):
                status, value = '400 Bad Request', dict(status='INVALID_EXPERIMENTAL_PLAN')
            except Exception:
                status, value = '503 Service Unavailable', dict(status='EXPERIMENTAL_RECORDING_UNAVAILABLE')
        body = json.dumps(value, separators=(',', ':'), allow_nan=False).encode()
        start_response(status, [('Content-Type', 'application/json'),
            ('Content-Length', str(len(body))), ('Cache-Control', 'no-store'),
            ('X-Content-Type-Options', 'nosniff')])
        return [body]

    def tick(self):
        release = self.release_loader()
        startup_allowed = self.startup_entries_allowed
        if self.startup_entry_gate is not None:
            startup_allowed = self.startup_entry_gate() is True
        result = self.runtime.run_once(entries_enabled=startup_allowed and
            releases.entry_enabled(release, self.runtime.venue.now()))
        self.cycles += 1
        self.last_status = result['status']
        return result

    def run(self, stop_event, *, interval_seconds=5):
        """Explicit, single caller scheduling; never retries an order itself."""
        if type(interval_seconds) not in (int, float) or not 1 <= interval_seconds <= 30:
            raise BoundaryError('BOUNDED_CONNECTION_INTERVAL_REQUIRED')
        if not self._run_lock.acquire(blocking=False):
            raise BoundaryError('EXPERIMENTAL_CONNECTION_LOOP_ALREADY_RUNNING')
        try:
            while not stop_event.is_set():
                try:
                    self.tick()
                except Exception:
                    # State/transport uncertainty stays in the durable runtime.
                    # No raw failure, key, request body or DB URL is logged here.
                    self.last_status = 'CONNECTION_CYCLE_RECONCILIATION_REQUIRED'
                stop_event.wait(interval_seconds)
        finally:
            self._run_lock.release()

    def start(self):
        """Explicit activation by a separately reviewed startup only."""
        if self.feed is None or self.supervisor is None:
            raise BoundaryError('INDEPENDENT_PROTECTION_AND_NOTIFICATION_FEED_REQUIRED')
        with self._start_lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self.feed.start()
            try:
                self.supervisor.start()
                self._thread = threading.Thread(target=self.run, args=(self._stop,),
                    name='experimental-testnet-source-worker', daemon=True)
                self._thread.start()
            except BaseException:
                self.supervisor.stop()
                self.feed.stop()
                raise
            return True

    def stop(self):
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        if self.supervisor is not None:
            self.supervisor.stop()
        if self.feed is not None:
            self.feed.stop()
        return self.stopped()

    def stopped(self):
        """A timed join is not proof that an owner relinquished its threads."""
        threads = [self._thread, getattr(self.supervisor, '_thread', None)]
        threads.extend(getattr(self.feed, '_threads', ()))
        return all(thread is None or not thread.is_alive() for thread in threads)


def compose(env, *, price_evidence=None, safety_provider=None, clock=None):
    """Construct concrete DB/provider/sender/worker; never migrate, sign or send.

    The archive adapter reports its unavailable authoritative MARK-history
    capability truthfully; merely setting a mode cannot silently replace it
    with booleans. The built-in safety adapter remains closed until its
    independent supervisor and account feed prove current reconciliation.
    """
    from .postgres_journal import PostgresJournal
    from .filled_dispatch_store import DispatchStore
    from .request_budget import Budget
    from .experimental_live_state import TestnetExecutionState
    from .experimental_live_provider import LiveEvidenceProvider
    from .experimental_live_dispatch import LiveDispatchPort
    from .experimental_live_runtime import TestnetExecutionRuntime, MODE
    from .experimental_live_safety import LiveSafetyProvider, IndependentProtectionSupervisor
    from .fill_wakeups import FillWakeups

    release_loader = releases.ReleaseLoader(env)
    key = env.get('HL_TESTNET_EXPERIMENTAL_PLAN_SECRET', '')
    if not intake.HEX.fullmatch(key):
        raise BoundaryError('EXPERIMENTAL_AUTH_NOT_CONFIGURED')
    journal = PostgresJournal.from_env(env)
    store, legacy_store, budget = TestnetExecutionState(journal), DispatchStore(journal), Budget(journal)
    if price_evidence is None:
        from .experimental_price_evidence import build_price_evidence
        price_evidence = build_price_evidence(env, journal=journal, budget=budget, clock=clock)
    feed = None
    if safety_provider is None:
        feed = FillWakeups(release_loader()['routes'])
        safety_provider = LiveSafetyProvider(feed=feed, legacy_store=legacy_store,
            experimental_store=store, release_loader=release_loader, clock=clock)
    elif not callable(getattr(type(safety_provider), 'verify_checkpoint', None)):
        raise BoundaryError('CONCRETE_VERIFIED_SAFETY_PROVIDER_REQUIRED')
    provider = LiveEvidenceProvider(env, legacy_store=legacy_store, experimental_store=store,
        price_evidence=price_evidence, budget=budget, clock=clock, safety_provider=safety_provider)
    sender = LiveDispatchPort(env, context_loader=provider.dispatch_context, request_loader=store.request,
        claim_transport=store.claim_transport, release_loader=release_loader, budget=budget, clock=clock)
    runtime = TestnetExecutionRuntime(store, provider, sender, release_loader=release_loader, mode=MODE)
    supervisor = IndependentProtectionSupervisor(runtime, safety_provider) if feed is not None else None
    return ConnectionService(runtime, key=key, release_loader=release_loader,
                             feed=feed, supervisor=supervisor)
