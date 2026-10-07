"""Offline real feed tokens, durable checkpoints and independent supervision."""
from copy import deepcopy
import json
import threading
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from . import card_lifecycle as life
from . import fill_wakeups as wake
from . import experimental_live_safety as safety
from .filled_dispatch_store import DispatchStore
from . import test_experimental_live_service as service_tests
from .test_experimental_execution_runtime import T


class LiveSafetyTests(unittest.TestCase):
    def setUp(self):
        self.fx = service_tests.FullConnectionTests(methodName='runTest')
        self.fx.setUp(); self.addCleanup(self.fx.doCleanups)
        self.legacy = DispatchStore(self.fx.store.journal)
        self.legacy.for_account = lambda account: []
        self.legacy.entry_blocker = lambda: None
        self.feed = wake.FillWakeups(self.fx.release['routes'])
        self.capability = safety.LiveSafetyProvider(feed=self.feed, legacy_store=self.legacy,
            experimental_store=self.fx.store, release_loader=lambda: deepcopy(self.fx.release),
            clock=self.fx.oracle.now)
        self.fx.provider.legacy_store = self.legacy
        self.fx.provider.safety = self.capability
        self.supervisor = safety.IndependentProtectionSupervisor(self.fx.worker, self.capability)
        self.addCleanup(self.supervisor.stop)

    def ready_feed(self):
        for account in self.fx.release['routes'].values():
            generation = self.feed._opened(account)
            for channel in wake.CHANNELS:
                self.assertTrue(self.feed._receive(account, generation, json.dumps(dict(
                    channel='subscriptionResponse', data=dict(method='subscribe',
                        subscription=dict(type=channel, user=account))))))
            self.assertTrue(self.feed._receive(account, generation, json.dumps(dict(
                channel='userFills', data=dict(user=account, isSnapshot=True, fills=[])))))

    def observed(self):
        return self.fx.worker.run_once(entries_enabled=False)

    def start_supervisor(self):
        ran = threading.Event()
        original = self.supervisor.pass_once
        def once():
            try: return original()
            finally: ran.set()
        self.supervisor.pass_once = once
        self.assertTrue(self.supervisor.start())
        self.assertTrue(ran.wait(2))

    def test_constructor_and_manual_pass_cannot_claim_running_supervisor(self):
        self.assertFalse(self.supervisor.health()['healthy'])
        self.assertEqual(self.fx.http, [])
        self.ready_feed()
        self.supervisor.pass_once()
        self.assertEqual(self.supervisor.health()['status'], 'SUPERVISOR_OBSERVATION_VERIFIED')
        self.assertFalse(self.supervisor.health()['healthy'])
        self.assertTrue(all(self.feed.entry_allowed(a) for a in self.fx.release['routes'].values()))

    def test_uncommitted_collection_cannot_clear_real_feed_gate(self):
        self.ready_feed()
        before = self.fx.store.load()
        context = self.fx.provider.collect(before)
        self.assertFalse(any(self.feed.entry_allowed(a) for a in before['routes'].values()))
        self.assertFalse(self.capability.after_committed(
            checkpoint_id=context['safety_checkpoint_id'], state=before))
        self.assertFalse(any(self.feed.entry_allowed(a) for a in before['routes'].values()))
        self.assertEqual(self.observed()['status'], 'OBSERVED_NO_ACTION')
        self.assertTrue(all(self.feed.entry_allowed(a) for a in before['routes'].values()))

    def test_fabricated_durable_marker_and_commit_failure_cannot_finish_feed_token(self):
        self.ready_feed(); before = self.fx.store.load()
        context = self.fx.provider.collect(before)
        fake = deepcopy(before); fake['revision'] += 1
        fake['ownership_revision'] = context['ownership_revision']
        fake['collector_checkpoints'] = context['collector_checkpoints']
        fake['account_inventory_checkpoint'] = safety._marker(context, context['safety_checkpoint_id'])
        self.assertFalse(self.capability.after_committed(checkpoint_id=context['safety_checkpoint_id'], state=fake))
        with patch.object(self.fx.store, 'mutate', side_effect=RuntimeError('private db details')):
            with self.assertRaises(RuntimeError): self.observed()
        self.assertFalse(any(self.feed.entry_allowed(a) for a in before['routes'].values()))

    def test_notification_during_collect_or_after_commit_revokes_entry_continuity(self):
        self.ready_feed(); before = self.fx.store.load()
        context = self.fx.provider.collect(before)
        account = before['routes']['short_account']
        generation = self.feed.health()['short_account']['generation']
        message = dict(channel='orderUpdates', data=[dict(order=dict(coin='XRP', oid=5),
            status='filled', statusTimestamp=T+10000)])
        self.feed._receive(account, generation, json.dumps(message))
        def save(state):
            state['collector_checkpoints'] = deepcopy(context['collector_checkpoints'])
            state['ownership_revision'] = context['ownership_revision']
            state['account_inventory_checkpoint'] = safety._marker(context, context['safety_checkpoint_id'])
        self.fx.store.mutate(save)
        self.assertFalse(self.capability.after_committed(checkpoint_id=context['safety_checkpoint_id'],
                                                        state=self.fx.store.load()))
        self.assertFalse(self.feed.entry_allowed(account))
        self.assertEqual(self.observed()['status'], 'OBSERVED_NO_ACTION')
        receipt = self.capability._receipt(self.fx.store.load())
        self.assertTrue(self.capability._continuous(receipt))
        self.feed._disconnected(account, generation)
        self.assertFalse(self.capability._continuous(receipt))

    def test_supervisor_actual_pass_success_stale_failure_and_stop(self):
        self.ready_feed(); self.start_supervisor()
        self.assertTrue(self.supervisor.health()['healthy'])
        at = self.supervisor.health()['observed_at_ms']
        self.assertEqual(at, self.fx.oracle.now())
        self.assertFalse(self.supervisor.start())
        self.fx.oracle.t += 15001
        self.assertFalse(self.supervisor.health()['healthy'])
        self.fx.oracle.t -= 15001
        with patch.object(self.fx.worker, 'run_once', side_effect=RuntimeError('private key or URL')):
            self.supervisor.pass_once()
        self.assertFalse(self.supervisor.health()['healthy'])
        self.assertNotIn('private', str(self.supervisor.health()))
        self.supervisor.stop()
        self.assertFalse(self.supervisor.health()['healthy'])
        with self.assertRaisesRegex(safety.SafetyError, 'STOPPED_SUPERVISOR'):
            self.supervisor.start()

    def test_restart_needs_new_committed_feed_and_independent_protection_pass(self):
        self.ready_feed(); self.start_supervisor()
        self.assertTrue(self.supervisor.health()['healthy'])
        self.supervisor.stop()
        restarted_feed = wake.FillWakeups(self.fx.release['routes'])
        restarted = safety.LiveSafetyProvider(feed=restarted_feed, legacy_store=self.legacy,
            experimental_store=self.fx.store, release_loader=lambda: deepcopy(self.fx.release),
            clock=self.fx.oracle.now)
        self.assertIsNone(restarted._receipt(self.fx.store.load()))
        self.assertFalse(any(restarted_feed.entry_allowed(a) for a in self.fx.release['routes'].values()))

    def test_actual_entry_requires_supervisor_and_shared_circuit_then_exits_continue(self):
        self.ready_feed(); self.start_supervisor()
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        result = self.fx.worker.run_once(entries_enabled=True)
        self.assertEqual(result.get('operation'), 'ENTRY')
        for _ in range(4): self.observed()
        trade = next(iter(self.fx.store.load()['trades'].values()))
        self.assertEqual(trade['phase'], 'OPEN')
        self.supervisor.stop()
        for account in self.fx.release['routes'].values():
            self.feed._disconnected(account, self.feed.begin_reconciliation(account).generation)
        self.legacy.entry_blocker = lambda: 'EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY'
        self.fx.oracle.fill(self.fx.oracle.oid('TAKE_PROFIT'), trade['quantity'])
        for _ in range(3): self.observed()
        self.assertEqual(next(iter(self.fx.store.load()['trades'].values()))['phase'], 'CLOSED')

    def test_shared_circuit_and_failed_supervisor_never_allow_entry_submission(self):
        self.ready_feed(); self.start_supervisor()
        self.legacy.entry_blocker = lambda: 'EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY'
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        self.fx.worker.run_once(entries_enabled=True)
        self.assertEqual(self.fx.http, [])

    def test_unresolved_peer_and_missing_observed_protection_cannot_claim_health(self):
        state = self.fx.store.load()
        state['requests']['peer'] = {'phase': 'OUTCOME_UNKNOWN'}
        self.assertFalse(self.capability._peers_protected(state))
        self.ready_feed(); self.start_supervisor()
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        self.fx.worker.run_once(entries_enabled=True)
        self.observed()  # Entry fill reconciled; STOP attempt remains unobserved.
        self.assertFalse(self.capability._peers_protected(self.fx.store.load()))
        for _ in range(4): self.observed()
        self.assertTrue(self.capability._peers_protected(self.fx.store.load()))
        state = self.fx.store.load()
        trade = next(iter(state['trades'].values()))
        for oid, row in trade['orders'].items():
            if trade['order_legs'][oid] == 'STOP': row['status'] = 'CANCELED'
        self.assertFalse(self.capability._peers_protected(state))

    def test_fill_wakeup_during_pass_is_not_lost_and_uses_same_budget(self):
        calls = []
        original_budget = self.fx.provider.budget
        class Event:
            def __init__(event): event.pending = True
            def clear(event): event.pending = False
            def set(event): event.pending = True
            def wait(event, seconds):
                calls.append(('wait', seconds, event.pending))
                if not event.pending: self.supervisor._stop.set()
                return event.pending
        event = Event()
        self.feed.wake_event = event
        def actual_pass():
            calls.append(('pass', self.fx.provider.budget is original_budget))
            if sum(c[0] == 'pass' for c in calls) == 1:
                event.set()  # A notification arrives while collection runs.
            return dict(status='SUPERVISOR_RECONCILIATION_REQUIRED')
        with patch.object(self.supervisor, 'pass_once', side_effect=actual_pass):
            self.supervisor._run()
        self.assertEqual(calls, [('pass', True), ('wait', 5, True),
                                 ('pass', True), ('wait', 5, False)])


if __name__ == '__main__': unittest.main()
