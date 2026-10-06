"""Durable SOFTWARE-ONLY rehearsal for one R2732 source occurrence.

Uses the existing source inbox row and its revision CAS. No sender, signer,
collector, scheduler, HTTP registration or new storage schema. Single-row CAS
cannot prove atomic account-wide admission across different occurrences.
Production dispatch is unconditionally unavailable.
"""
from copy import deepcopy

import experimental_execution_contract as contract
from . import card_lifecycle as life, experimental_plan_store as plans
from . import r2732_entry as entry, r2732_stop_dispatch as stops
from .postgres_journal import PostgresJournal

VERSION = 'experimental-local-controller-v1'


class LocalControllerError(plans.PlanStoreError):
    """Fixed local errors, never raw connection or source details."""


def readiness():
    return dict(mode='software_only', production_ready=False, dispatch_enabled=False,
        signer_connected=False, live_collector_connected=False,
        global_account_dispatch_connected=False, initial_exit_dispatch_connected=False,
        order_requests_sent=0)


class LocalController:
    def __init__(self, store, occurrence_id, routes, *, not_before_ms):
        if (type(store) is not plans.PlanStore or store.domain != 'software'
                or getattr(store.journal, '_ci', None) is not True):
            raise LocalControllerError('SOFTWARE_JOURNAL_REQUIRED')
        if isinstance(store.journal, PostgresJournal):
            p = store.journal._parameters
            if (p.get('host') not in ('127.0.0.1', 'localhost')
                    or p.get('dbname') != 'hl_journal_ci' or p.get('sslmode') != 'disable'):
                raise LocalControllerError('SOFTWARE_LOOPBACK_JOURNAL_REQUIRED')
        life.ident(occurrence_id, r'[0-9a-f]{64}')
        self.store, self.cid = store, occurrence_id
        self.initial_entry = entry.initial(routes, not_before_ms=not_before_ms)
        self.account = self.initial_entry['account']
        self.routes = {role: dict(account=life.address(routes[role]['account']))
                       for role in ('short_account', 'long_account')}

    def _load(self):
        source = self.store.load(self.cid)
        if source is None:
            raise LocalControllerError('LOCAL_EXISTING_SOURCE_REQUIRED')
        value = contract.validate(source['source'])
        if (source['domain'] != 'software' or value['family'] != 'r2732'
                or value['occurrence_id'] != self.cid
                or source['plan_digest'] != contract.plan_digest(value)):
            raise LocalControllerError('LOCAL_R2732_SOURCE_IDENTITY_REQUIRED')
        return source

    @staticmethod
    def _source_view(source):
        # Strategy commits change row revision/time, not source permission.
        # Exclude their bookkeeping and the recursive strategy payload.
        result = deepcopy(source)
        result.update(strategy=None, revision=1, updated_at=source['source']['source_as_of'])
        return result

    def _validate(self, source, state):
        life.shape(state, 'version domain occurrence_id account routes_digest not_before_ms '
                   'plan_digest entry prepared stop')
        if (state['version'] != VERSION or state['domain'] != 'software'
                or state['occurrence_id'] != self.cid or state['account'] != self.account
                or state['routes_digest'] != life.digest(self.routes)
                or state['not_before_ms'] != self.initial_entry['not_before_ms']
                or state['plan_digest'] != source['plan_digest']):
            raise LocalControllerError('LOCAL_CONTROLLER_IDENTITY_CHANGED')
        observed = entry._copy(state['entry'])
        if (observed['account'] != self.account
                or observed['not_before_ms'] != self.initial_entry['not_before_ms']
                or set(observed['records']) != {self.cid}
                or contract.plan_digest(observed['records'][self.cid]['source_record']['source']) != source['plan_digest']):
            raise LocalControllerError('LOCAL_ENTRY_OWNER_OR_SOURCE_CHANGED')
        p = state['prepared']
        if p is not None and (p['occurrence_id'] != self.cid or p['account'] != self.account
                or contract.plan_digest(contract.validate(p['source'])) != source['plan_digest']
                or p['proposal_id'] != life.digest({k: v for k, v in p.items() if k != 'proposal_id'})):
            raise LocalControllerError('LOCAL_PREPARED_ENTRY_CHANGED')
        request = observed['records'][self.cid]['request']
        if request is not None:
            attempted = request['proposal']
            if (attempted != p or attempted['account'] != self.account
                    or contract.plan_digest(attempted['source']) != source['plan_digest']):
                raise LocalControllerError('LOCAL_CONSUMED_ENTRY_CHANGED')
        if state['stop'] is not None:
            stop = stops.validate(state['stop'])
            if (request is None or stop['contract'] != request['proposal']['source']
                    or stop['execution'] != request['proposal']['execution']
                    or stop['original_binding']['account'] != self.account
                    or stop['original_binding']['card_id'] != self.cid
                    or stop['original_binding']['planned_quantity'] != request['proposal']['quantity']):
                raise LocalControllerError('LOCAL_STOP_NOT_BOUND_TO_CONSUMED_ENTRY')
        return deepcopy(state)

    def _synchronize(self, source, state):
        result = self._validate(source, state)
        previous = result['entry']['records'][self.cid]
        view = self._source_view(source)
        if previous['source_record'] != view:
            previous['source_record'] = view
            result['entry']['revision'] += 1
        return result

    def load(self):
        source = self._load()
        if source['strategy'] is None:
            raise LocalControllerError('LOCAL_CONTROLLER_NOT_INITIALIZED')
        return dict(source_revision=source['revision'], state=self._synchronize(source, source['strategy']), **readiness())

    def initialize(self, *, now_ms):
        life.moment(now_ms)
        source = self._load()
        if source['strategy'] is not None:
            return dict(source_revision=source['revision'], state=self._synchronize(source, source['strategy']), **readiness())
        if now_ms < self.initial_entry['not_before_ms']:
            raise LocalControllerError('LOCAL_ACTIVATION_IS_IN_FUTURE')
        local_entry = deepcopy(self.initial_entry)
        local_entry['records'][self.cid] = dict(source_record=self._source_view(source), request=None)
        local_entry['latest_source_ms'] = contract.moment_ms(source['source']['source_at'])
        local_entry['revision'] = 1
        state = dict(version=VERSION, domain='software', occurrence_id=self.cid, account=self.account,
            routes_digest=life.digest(self.routes), not_before_ms=self.initial_entry['not_before_ms'],
            plan_digest=source['plan_digest'], entry=local_entry, prepared=None, stop=None)
        self._validate(source, state)
        written = self.store.change_strategy(self.cid, source['revision'], state, now=contract.iso_ms(now_ms))
        return dict(source_revision=written['revision'], state=state, **readiness())

    def _change(self, expected_revision, now_ms, transition):
        life.moment(now_ms)
        source = self._load()
        if type(expected_revision) is not int or source['revision'] != expected_revision:
            raise LocalControllerError('LOCAL_CONCURRENT_RELOAD_REQUIRED')
        if source['strategy'] is None:
            raise LocalControllerError('LOCAL_CONTROLLER_NOT_INITIALIZED')
        prior = self._synchronize(source, source['strategy'])
        state = deepcopy(prior)
        result = transition(state)
        self._validate(source, state)
        before = prior['entry']['records'][self.cid]['request']
        after = state['entry']['records'][self.cid]['request']
        if before is not None and after != before:
            raise LocalControllerError('LOCAL_CONSUMED_ATTEMPT_IS_IMMUTABLE')
        # Same row CAS: a concurrent source/cancellation revision rejects this
        # commit. No unsigned intent leaves before COMMIT acknowledges.
        written = self.store.change_strategy(self.cid, expected_revision, state,
                                            now=contract.iso_ms(now_ms))
        return dict(source_revision=written['revision'], state=deepcopy(state),
                    result=deepcopy(result), **readiness())

    def prepare_entry(self, expected_revision, metadata, market, source_market, ownership, *, now_ms):
        def transition(state):
            p = entry.entry_admission(state['entry'], self.cid, metadata, market, source_market, ownership, now_ms=now_ms)
            state['prepared'] = p
            return dict(proposal_id=p['proposal_id'], persisted_preparation=True)
        return self._change(expected_revision, now_ms, transition)

    def begin_entry(self, proposal_id, expected_revision, metadata, market, source_market, ownership, *, now_ms):
        def transition(state):
            p = state['prepared']
            if p is None or p['proposal_id'] != proposal_id:
                raise LocalControllerError('LOCAL_EXACT_PREPARED_ENTRY_REQUIRED')
            state['entry'] = entry.begin_entry(state['entry'], p, metadata, market, source_market, ownership, now_ms=now_ms)
            return dict(unsigned_proposal=p, live_dispatch_authorized=False)
        return self._change(expected_revision, now_ms, transition)

    def attach_simulated_protection(self, expected_revision, binding, snapshot, metadata, *, now_ms):
        """Rehearse ownership handoff; never creates real initial stop/take orders."""
        def transition(state):
            request = state['entry']['records'][self.cid]['request']
            if request is None or state['stop'] is not None:
                raise LocalControllerError('LOCAL_CONSUMED_UNREGISTERED_ENTRY_REQUIRED')
            proposal = request['proposal']
            if (binding['card_id'] != self.cid or binding['account'] != self.account
                    or binding['planned_quantity'] != proposal['quantity']
                    or snapshot['at_ms'] <= request['attempted_at_ms']):
                raise LocalControllerError('LOCAL_PROTECTION_OWNER_MISMATCH')
            if any(row['at_ms'] <= request['attempted_at_ms']
                   for key in ('fills', 'terminal_orders') for row in snapshot[key]):
                raise LocalControllerError('LOCAL_PROTECTION_EVIDENCE_NOT_AFTER_ATTEMPT')
            state['stop'] = stops.initialize(proposal['source'], binding, snapshot,
                metadata=metadata, now_ms=now_ms)
            return dict(simulated_registrar_handoff=True, real_ownership_proven=False)
        return self._change(expected_revision, now_ms, transition)

    def advance_stop(self, expected_revision, bars, *, now_ms, price_source=stops.condition.SOURCE):
        def transition(state):
            state['stop'] = stops.advance(state['stop'], bars, now_ms=now_ms, price_source=price_source)
        return self._change(expected_revision, now_ms, transition)

    def reserve_stop(self, expected_revision, metadata, sample, *, now_ms):
        def transition(state):
            state['stop'] = stops.reserve(state['stop'], metadata, sample, now_ms=now_ms)
            return dict(request_id=state['stop']['pending'])
        return self._change(expected_revision, now_ms, transition)

    def begin_stop(self, request_id, expected_revision, metadata, sample, *, now_ms):
        def transition(state):
            value = stops.begin(state['stop'], request_id, metadata, sample, now_ms=now_ms)
            state['stop'] = value
            request = next(r for r in value['requests'] if r['request_id'] == request_id)
            return dict(unsigned_action=request['proposal']['action'], live_dispatch_authorized=False)
        return self._change(expected_revision, now_ms, transition)

    def stop_reply(self, request_id, expected_revision, reply, *, now_ms):
        def transition(state):
            state['stop'] = stops.record_reply(state['stop'], request_id, reply)
        return self._change(expected_revision, now_ms, transition)

    def observe_stop(self, expected_revision, snapshot, *, now_ms, lookup=None):
        def transition(state):
            state['stop'] = stops.observe(state['stop'], snapshot, now_ms=now_ms, lookup=lookup)
        return self._change(expected_revision, now_ms, transition)
