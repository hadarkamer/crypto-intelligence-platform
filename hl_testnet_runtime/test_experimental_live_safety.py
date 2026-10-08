"""Offline real feed tokens, durable checkpoints and independent supervision."""
from copy import deepcopy
from decimal import Decimal
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from approved_alert_fixtures import maxpain_alert
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
        def once(**kwargs):
            try: return original(**kwargs)
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
        self.assertTrue(self.capability.after_committed(checkpoint_id=context['safety_checkpoint_id'],
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
            self.supervisor.pass_once(force_collect=True)
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

    def test_own_account_legacy_emergency_never_allows_entry_submission(self):
        self.ready_feed(); self.start_supervisor()
        account = self.fx.release['routes']['short_account']
        row = dict(account=account, symbol='XRP', bucket='b'*64, revision=1,
            pending=None, emergency='EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY', bindings=[], evidence=None)
        self.legacy.for_account = lambda owner: [deepcopy(row)] if owner == account else []
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        self.fx.worker.run_once(entries_enabled=True)
        self.assertEqual(self.fx.http, [])

    def test_other_account_disconnect_does_not_close_healthy_account_admission(self):
        self.ready_feed(); self.start_supervisor()
        other = self.fx.release['routes']['long_account']
        own = self.fx.release['routes']['short_account']
        # Hold only the test supervisor pass gate to isolate account scope
        # from the separately tested optimistic-commit concurrency retry.
        with self.supervisor._pass_lock:
            self.feed._disconnected(other, self.feed.begin_reconciliation(other).generation)
            self.assertFalse(self.supervisor.health(account=other)['healthy'])
            self.assertTrue(self.supervisor.health(account=own)['healthy'])
            self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
            result = self.fx.worker.run_once(entries_enabled=True)
        self.assertEqual(result.get('operation'), 'ENTRY', result)
        self.assertEqual(len(self.fx.http), 1)
        self.assertEqual(next(iter(self.fx.store.load()['trades'].values()))['account'], own)

    def test_partial_receipt_finishes_only_verified_account_and_is_not_global_health(self):
        self.ready_feed()
        state = self.fx.store.load(); context = self.fx.provider.collect(state)
        # Build another honest begin/stage around the same fixture observation
        # to model the provider's independently failed account inventory.
        token = self.capability.before_collect(state)
        own = state['routes']['short_account']; other = state['routes']['long_account']
        context['inventory_accounts'] = [own]
        context['account_inventory_at_ms'] = {own: context['inventory_at_ms']}
        context['inventory_account_errors'] = {other: 'ACCOUNT_INVENTORY_UNAVAILABLE'}
        context['account_entry_blocked'] = {other: 'ACCOUNT_INVENTORY_UNAVAILABLE'}
        context['safety_checkpoint_id'] = self.capability.stage_observation(token, state=state, context=context)
        def save(row):
            row['collector_checkpoints'] = deepcopy(context['collector_checkpoints'])
            row['ownership_revision'] = context['ownership_revision']
            row['account_inventory_checkpoint'] = safety._marker(context, context['safety_checkpoint_id'])
        self.fx.store.mutate(save)
        self.assertTrue(self.capability.after_committed(checkpoint_id=context['safety_checkpoint_id'],
            state=self.fx.store.load()))
        receipt = self.capability._receipt(self.fx.store.load())
        self.assertTrue(self.capability._continuous(receipt, own))
        self.assertFalse(self.capability._continuous(receipt, other))
        self.assertFalse(self.capability._continuous(receipt))
        self.assertTrue(self.feed.entry_allowed(own))
        self.assertFalse(self.feed.entry_allowed(other))

    def test_unresolved_and_candidate_only_blocks_have_correct_account_scope(self):
        state = self.fx.store.load()
        own = state['routes']['short_account']; other = state['routes']['long_account']
        state['blocked_lanes'] = {safety._lane(other, 'DOGE'): 'SYMBOL_UNAVAILABLE',
            safety._lane(own, 'SOL'): 'SYMBOL_UNAVAILABLE'}
        self.assertTrue(self.capability._peers_protected(state, account=own))
        state['requests']['peer'] = dict(phase='OUTCOME_UNKNOWN', proposal=dict(account=other))
        self.assertTrue(self.capability._peers_protected(state, account=own))
        self.assertFalse(self.capability._peers_protected(state, account=other))
        self.assertFalse(self.capability._peers_protected(state))

    def test_supervisor_reuses_durable_observation_without_reads_or_timestamp_refresh(self):
        self.ready_feed(); self.observed()
        receipt = self.capability._receipt(self.fx.store.load())
        original_at = receipt['context']['inventory_at_ms']
        reads = len(self.fx.raw.calls)
        self.fx.oracle.t += 5000
        self.supervisor.pass_once()
        self.assertEqual(len(self.fx.raw.calls), reads)
        self.assertEqual(self.supervisor.health()['observed_at_ms'], original_at)
        self.assertFalse(self.supervisor.health()['healthy'])  # Manual pass is not a running capability.
        self.fx.oracle.t += 5000
        self.supervisor.pass_once()
        self.assertEqual(len(self.fx.raw.calls), reads)
        self.assertEqual(self.supervisor.health()['observed_at_ms'], original_at)
        self.fx.oracle.t += 50000
        self.supervisor.pass_once()
        self.assertGreater(len(self.fx.raw.calls), reads)
        self.assertEqual(self.supervisor.health()['observed_at_ms'], original_at + 60000)

    def test_idle_service_reuses_only_current_observation_and_wakes_for_source(self):
        self.ready_feed(); self.start_supervisor()
        self.fx.service.supervisor = self.supervisor
        reads = len(self.fx.raw.calls)
        with patch.object(self.fx.service, 'release_loader') as release, \
                patch.object(self.fx.service, 'startup_entry_gate') as startup:
            self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_IDLE_REUSED')
        release.assert_not_called()
        startup.assert_not_called()
        self.assertEqual(len(self.fx.raw.calls), reads)
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        self.assertEqual(self.fx.service.tick().get('operation'), 'ENTRY')
        self.assertGreater(len(self.fx.raw.calls), reads)

    def test_empty_workers_do_not_both_scan_at_fallback_boundary(self):
        self.ready_feed(); self.start_supervisor()
        self.fx.service.supervisor = self.supervisor
        reads = len(self.fx.raw.calls)
        self.fx.oracle.t += 60000
        self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_NO_ACTION')
        after = len(self.fx.raw.calls)
        self.assertGreater(after, reads)
        self.supervisor.pass_once()
        self.assertEqual(len(self.fx.raw.calls), after)
        self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_IDLE_REUSED')
        self.assertEqual(len(self.fx.raw.calls), after)

    def test_running_primary_receives_fill_wakeup_without_duplicate_supervisor_scan(self):
        self.ready_feed(); self.observed()
        primary = self.fx.service
        self.supervisor.primary_service = primary
        primary._thread = SimpleNamespace(is_alive=lambda: True)
        primary.last_cycle_at_ms = self.fx.oracle.now()
        primary.last_status = 'OBSERVED_NO_ACTION'
        account = self.fx.release['routes']['short_account']
        generation = self.feed.begin_reconciliation(account).generation
        self.feed._receive(account, generation, json.dumps(dict(channel='orderUpdates',
            data=[dict(order=dict(coin='XRP', oid=5), status='filled',
                statusTimestamp=self.fx.oracle.now())])))
        reads = len(self.fx.raw.calls)
        self.supervisor.pass_once()
        self.assertTrue(primary._wake.is_set())
        self.assertEqual(len(self.fx.raw.calls), reads)
        self.assertFalse(self.feed.entry_allowed(account))
        # A failed primary relinquishes scanning immediately.
        primary.last_cycle_error_code = 'TESTNET_REQUEST_BUDGET_EXHAUSTED'
        self.supervisor.pass_once()
        self.assertGreater(len(self.fx.raw.calls), reads)
        self.assertTrue(self.feed.entry_allowed(account))

    def test_recent_primary_pending_work_does_not_trigger_duplicate_wakeup(self):
        self.ready_feed(); self.observed()
        primary = self.fx.service
        self.supervisor.primary_service = primary
        primary._thread = SimpleNamespace(is_alive=lambda: True)
        primary.last_cycle_at_ms = self.fx.oracle.now()
        primary.last_status = 'OBSERVED_NO_ACTION'
        with patch.object(self.supervisor, '_can_reuse', return_value=False), \
                patch.object(self.fx.worker, 'run_once') as collect:
            self.supervisor.pass_once()
        self.assertFalse(primary._wake.is_set())
        collect.assert_not_called()

    def test_settled_static_protection_reuses_receipt_until_fill_notification(self):
        self.ready_feed(); self.observed(); self.supervisor.pass_once()
        self.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda timeout: None)
        msg = maxpain_alert(approved_ms=T)
        account = self.fx.release['routes']['long_account']
        self.fx.oracle.mark[safety._lane(account, msg['symbol'])] = msg['entry']
        self.fx.worker.receive([msg])
        self.assertEqual(self.fx.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
        trade = self.fx.store.load()['trades'][msg['occurrence_id']]
        self.fx.oracle.fill(self.fx.oracle.oid('ENTRY'), trade['quantity'])
        for _ in range(3):
            self.observed()
        reads = len(self.fx.raw.calls)
        self.observed()
        self.assertEqual([row[0] for row in self.fx.raw.calls[reads:]],
                         ['frontendOpenOrders','clearinghouseState'])
        self.assertIsNotNone(self.supervisor.idle_receipt())
        self.fx.service.supervisor = self.supervisor
        reads = len(self.fx.raw.calls)
        self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_IDLE_REUSED')
        self.assertEqual(len(self.fx.raw.calls), reads)
        token = self.feed.begin_reconciliation(account)
        self.feed._receive(account, token.generation, json.dumps(dict(channel='orderUpdates',
            data=[dict(order=dict(coin=msg['symbol'], oid=int(self.fx.oracle.oid('TAKE_PROFIT'))),
                status='filled', statusTimestamp=self.fx.oracle.now())])))
        self.assertIsNone(self.supervisor.idle_receipt())
        reads = len(self.fx.raw.calls)
        self.observed()
        self.assertIn('userFillsByTime',[row[0] for row in self.fx.raw.calls[reads:]])
        # A restarted process has no committed in-memory receipt and must
        # fetch history even if the saved order sizes and position still match.
        self.fx.provider.safety = safety.LiveSafetyProvider(feed=self.feed,
            legacy_store=self.legacy,experimental_store=self.fx.store,
            release_loader=lambda:deepcopy(self.fx.release),clock=self.fx.oracle.now)
        reads = len(self.fx.raw.calls)
        self.fx.provider.collect(self.fx.store.load())
        self.assertIn('userFillsByTime',[row[0] for row in self.fx.raw.calls[reads:]])

    def test_known_unfilled_gtc_coalesces_until_ten_seconds_or_actionable_change(self):
        self.ready_feed(); self.observed(); self.supervisor.pass_once()
        self.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda timeout: None)
        self.fx.service.supervisor = self.supervisor
        msg = maxpain_alert(approved_ms=T)
        account = self.fx.release['routes']['long_account']
        self.fx.oracle.mark[safety._lane(account, msg['symbol'])] = msg['entry']
        self.fx.worker.receive([msg])
        self.assertEqual(self.fx.worker.run_once(entries_enabled=True).get('operation'), 'ENTRY')
        self.assertIsNone(self.supervisor.idle_receipt())  # Reply alone is not observation.
        self.observed()
        trade = self.fx.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['entry_fills'], {})
        self.assertEqual(self.fx.store.request(trade['entry_request'])['phase'], 'OBSERVED')
        reads = len(self.fx.raw.calls)
        self.fx.oracle.t += 5000
        self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_IDLE_REUSED')
        self.assertEqual(len(self.fx.raw.calls), reads)

        # Local retirement revokes reuse even while the unchanged feed is fresh.
        self.fx.store.mutate(lambda state: state['sources'][msg['occurrence_id']].update(entry_permission='RETIRED'))
        self.assertIsNone(self.supervisor.idle_receipt())
        self.fx.store.mutate(lambda state: state['sources'][msg['occurrence_id']].update(entry_permission='WAITING'))
        self.assertIsNotNone(self.supervisor.idle_receipt())
        self.fx.oracle.t += 5000
        self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_NO_ACTION')
        self.assertGreater(len(self.fx.raw.calls), reads)

        # A partial fill wakes collection immediately and resumes normal SL/TP
        # work; neither the remaining GTC quantity nor its known OID permits reuse.
        partial = (Decimal(trade['quantity']) / 2).quantize(Decimal(10) ** -trade['asset']['decimals'])
        self.fx.oracle.fill(self.fx.oracle.oid('ENTRY'), str(partial))
        token = self.feed.begin_reconciliation(account)
        self.feed._receive(account, token.generation, json.dumps(dict(channel='orderUpdates',
            data=[dict(order=dict(coin=msg['symbol'], oid=int(self.fx.oracle.oid('ENTRY'))),
                status='open', statusTimestamp=self.fx.oracle.now())])))
        self.assertIsNone(self.supervisor.idle_receipt())
        reads = len(self.fx.raw.calls)
        self.assertEqual(self.fx.service.tick().get('operation'), 'CREATE_EXIT')
        self.assertGreater(len(self.fx.raw.calls), reads)
        self.assertIsNone(self.supervisor.idle_receipt())
        for _ in range(3):
            self.observed()
        self.assertTrue(self.capability._peers_protected(self.fx.store.load()))
        self.assertIsNone(self.supervisor.idle_receipt())

    def test_known_gtc_does_not_defer_another_ready_source(self):
        self.ready_feed(); self.observed(); self.supervisor.pass_once()
        self.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda timeout: None)
        msg = maxpain_alert(approved_ms=T)
        account = self.fx.release['routes']['long_account']
        self.fx.oracle.mark[safety._lane(account, msg['symbol'])] = msg['entry']
        self.fx.worker.receive([msg])
        self.fx.worker.run_once(entries_enabled=True)
        self.observed()
        self.assertIsNotNone(self.supervisor.idle_receipt())
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        self.assertIsNone(self.supervisor.idle_receipt())

    def test_stale_primary_does_not_suppress_supervisor_fallback(self):
        self.ready_feed(); self.observed()
        primary = self.fx.service
        self.supervisor.primary_service = primary
        primary._thread = SimpleNamespace(is_alive=lambda: True)
        primary.last_cycle_at_ms = self.fx.oracle.now()
        primary.last_status = 'OBSERVED_NO_ACTION'
        reads = len(self.fx.raw.calls)
        self.fx.oracle.t += 60000
        self.supervisor.pass_once()
        self.assertGreater(len(self.fx.raw.calls), reads)

    def test_fresh_entry_on_previously_idle_account_needs_no_extra_supervisor_pass(self):
        self.ready_feed(); self.observed(); self.supervisor.pass_once()
        self.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda timeout: None)
        self.fx.oracle.t += 16000
        account = self.fx.release['routes']['short_account']
        self.assertFalse(self.supervisor.health(account=account)['healthy'])
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        with patch.object(self.capability, '_continuous', wraps=self.capability._continuous) as continuity, \
                patch.object(self.capability, '_legacy_clear', wraps=self.capability._legacy_clear) as ownership, \
                patch.object(self.capability, '_peers_protected', wraps=self.capability._peers_protected) as peers:
            result = self.fx.worker.run_once(entries_enabled=True)
        # Each side of signing gets one current account verification; the same
        # committed observation must not cause duplicate journal/safety work.
        self.assertEqual([continuity.call_count, ownership.call_count, peers.call_count], [2, 2, 2])
        self.assertEqual(result.get('operation'), 'ENTRY', result)
        self.assertEqual(self.fx.provider._last['context']['inventory_accounts'], [account])
        self.assertTrue(self.supervisor.health(account=account)['healthy'])
        self.assertEqual(len(self.fx.http), 1)

    def test_account_verification_never_reuses_changed_authority(self):
        self.ready_feed(); self.observed()
        self.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda timeout: None)
        account = self.fx.release['routes']['short_account']
        request = dict(request_id='entry', phase='OUTCOME_UNKNOWN', domain='testnet',
            proposal=dict(operation='ENTRY', account=account, role='short_account'))
        self.fx.store.mutate(lambda state: state['requests'].update(entry=deepcopy(request)))
        context = self.fx.provider._last['context']
        args = dict(request=request, collected_at_ms=context['inventory_at_ms'],
            ownership_revision=context['ownership_revision'])
        check = self.capability.verify_checkpoint
        self.assertTrue(check(state=self.fx.store.load(), **args)['emergency_healthy'])

        # A changed legacy owner must be discovered on the next verification.
        row = dict(account=account, symbol='XRP', bucket='b'*64, revision=1,
            pending=None, emergency=None, bindings=[], evidence=None)
        with patch.object(self.legacy, 'for_account',
                side_effect=lambda owner: [deepcopy(row)] if owner == account else []):
            result = check(state=self.fx.store.load(), **args)
        self.assertFalse(result['entry_circuit_clear'])
        self.assertFalse(result['emergency_healthy'])

        state = self.fx.store.load()
        state['requests']['peer'] = dict(phase='OUTCOME_UNKNOWN', proposal=dict(account=account))
        result = check(state=state, **args)
        self.assertFalse(result['entry_circuit_clear'])
        self.assertFalse(result['emergency_healthy'])

        # Even a feed change after the account proof but before health is read
        # must close admission without requiring another exchange collection.
        original = self.supervisor._record_observation
        def disconnect(state, verified):
            original(state, verified)
            self.feed._disconnected(account, self.feed.begin_reconciliation(account).generation)
        with patch.object(self.supervisor, '_record_observation', side_effect=disconnect):
            result = check(state=self.fx.store.load(), **args)
        self.assertFalse(result['emergency_healthy'])
        self.fx.oracle.t += safety.FRESH_MS + 1
        result = check(state=self.fx.store.load(), **args)
        self.assertFalse(result['entry_circuit_clear'])
        self.assertFalse(result['emergency_healthy'])

    def test_budget_wait_reuses_idle_receipt_only_before_retry_due(self):
        self.ready_feed(); self.observed()
        msg = r2732_message(entry=2.3, decision_ms=T-60000)
        self.fx.worker.receive([msg])
        self.assertIsNone(self.supervisor.idle_receipt())
        retry_at = self.fx.oracle.now() + 40000
        def defer(state):
            state.setdefault('entry_decisions', {})[msg['occurrence_id']] = dict(
                reason='TESTNET_REQUEST_BUDGET_EXHAUSTED', retry_after_ms=retry_at)
        self.fx.store.mutate(defer)
        self.assertIsNotNone(self.supervisor.idle_receipt())
        reads = len(self.fx.raw.calls)
        original_at = self.fx.oracle.now()
        self.fx.service.supervisor = self.supervisor
        for elapsed in (10000, 20000, 30000):
            self.fx.oracle.t = original_at + elapsed
            self.assertEqual(self.fx.service.tick()['status'], 'OBSERVED_IDLE_REUSED')
            self.supervisor.pass_once()
            self.assertEqual(len(self.fx.raw.calls), reads)
        # Skipping a read is not evidence that an old account snapshot is fresh.
        self.assertIsNone(self.capability._receipt(self.fx.store.load()))
        self.assertFalse(self.supervisor.health()['healthy'])
        self.fx.oracle.t = retry_at
        self.assertIsNone(self.supervisor.idle_receipt())

    def test_empty_idle_reuse_revoked_by_feed_event_or_new_source(self):
        self.ready_feed(); self.observed()
        self.fx.service.supervisor = self.supervisor
        self.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda timeout: None)
        original_at = self.fx.oracle.now()
        self.fx.oracle.t = original_at + 20000
        self.assertIsNotNone(self.supervisor.idle_receipt())
        self.assertFalse(self.supervisor.health()['healthy'])
        account = self.fx.release['routes']['short_account']
        token = self.feed.begin_reconciliation(account)
        self.feed._receive(account, token.generation, json.dumps(dict(channel='orderUpdates',
            data=[dict(order=dict(coin='XRP', oid=5), status='canceled',
                statusTimestamp=self.fx.oracle.now())])))
        self.assertIsNone(self.supervisor.idle_receipt())
        self.observed()
        self.fx.oracle.t += 20000
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        reads = len(self.fx.raw.calls)
        self.assertIsNone(self.supervisor.idle_receipt())
        self.assertEqual(self.fx.service.tick().get('operation'), 'ENTRY')
        self.assertGreater(len(self.fx.raw.calls), reads)
        self.assertTrue(self.supervisor.health(account=account)['healthy'])

    def test_idle_fallback_cannot_reuse_missing_or_partial_inventory(self):
        self.ready_feed()
        self.assertIsNone(self.supervisor.idle_receipt())
        self.observed()
        state = self.fx.store.load()
        receipt = self.capability._receipt(state)
        account = state['routes']['short_account']
        receipt['complete_accounts'].remove(account)
        with self.capability._lock:
            self.capability._committed[receipt['checkpoint_id']] = receipt
        self.assertIsNone(self.supervisor.idle_receipt())

    def test_worker_and_supervisor_share_nonblocking_cycle_lock(self):
        self.ready_feed()
        started = threading.Event(); finish = threading.Event(); errors = []
        original = self.fx.provider.collect
        def collect(*args, **kwargs):
            started.set()
            if not finish.wait(2):
                raise AssertionError('FIXTURE_WAIT_EXPIRED')
            return original(*args, **kwargs)
        def worker():
            try:
                self.observed()
            except Exception as exc:
                errors.append(exc)
        with patch.object(self.fx.provider, 'collect', side_effect=collect) as collected:
            thread = threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(started.wait(1))
                self.assertEqual(self.observed()['status'], 'OBSERVATION_CYCLE_ALREADY_RUNNING')
                self.supervisor.pass_once(force_collect=True)
                self.assertEqual(collected.call_count, 1)
            finally:
                finish.set(); thread.join(2)
        self.assertFalse(errors)
        self.supervisor.pass_once()
        self.assertEqual(self.supervisor.health()['status'], 'SUPERVISOR_OBSERVATION_VERIFIED')

    def test_stalled_source_worker_does_not_prevent_independent_exit_cleanup(self):
        self.ready_feed(); self.start_supervisor()
        self.fx.worker.receive([r2732_message(entry=2.3, decision_ms=T-60000)])
        self.fx.worker.run_once(entries_enabled=True)
        for _ in range(4): self.observed()
        trade = next(iter(self.fx.store.load()['trades'].values()))
        self.assertEqual(trade['phase'], 'OPEN')
        waiting = threading.Event(); resume = threading.Event(); failures = []
        original = self.fx.worker.run_once
        def runtime_call(**kwargs):
            if threading.current_thread().name == 'stalled-source-fixture':
                waiting.set()
                if not resume.wait(2): raise AssertionError('BOUNDED_FIXTURE_WAIT_EXPIRED')
                return dict(status='OBSERVED_NO_ACTION')
            return original(**kwargs)
        def source():
            try: self.fx.service.tick()
            except Exception as exc: failures.append(type(exc).__name__)
        with patch.object(self.fx.worker, 'run_once', side_effect=runtime_call):
            thread = threading.Thread(target=source, name='stalled-source-fixture')
            thread.start()
            try:
                self.assertTrue(waiting.wait(1))
                oid = self.fx.oracle.oid('TAKE_PROFIT')
                self.fx.oracle.fill(oid, trade['quantity'])
                token = self.feed.begin_reconciliation(trade['account'])
                self.feed._receive(trade['account'], token.generation, json.dumps(dict(
                    channel='orderUpdates', data=[dict(order=dict(coin='XRP', oid=int(oid)),
                        status='filled', statusTimestamp=self.fx.oracle.now())])))
                for _ in range(3): self.supervisor.pass_once()
                latest = next(iter(self.fx.store.load()['trades'].values()))
                self.assertEqual(latest['phase'], 'CLOSED')
                self.assertTrue(thread.is_alive())
            finally:
                resume.set(); thread.join(2)
        self.assertFalse(failures)

    def test_cycle_diagnostics_expose_fixed_codes_but_not_private_failure_messages(self):
        with patch.object(self.fx.worker, 'run_once', side_effect=ValueError('TESTNET_REQUEST_BUDGET_EXHAUSTED')):
            with self.assertRaises(ValueError): self.fx.service.tick()
        self.assertEqual(self.fx.service.last_cycle_error_code, 'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.assertEqual(self.fx.service.last_cycle_at_ms, self.fx.oracle.now())
        with patch.object(self.fx.worker, 'run_once', side_effect=RuntimeError('private://credential@example')):
            with self.assertRaises(RuntimeError): self.fx.service.tick()
        self.assertEqual(self.fx.service.last_cycle_error_code, 'CONNECTION_CYCLE_RECONCILIATION_REQUIRED')
        self.assertNotIn('private', str(self.fx.service.last_entry_decisions))

    def test_notification_forces_supervisor_to_collect_despite_fresh_worker_receipt(self):
        self.ready_feed(); self.observed()
        reads = len(self.fx.raw.calls)
        account = self.fx.release['routes']['short_account']
        generation = self.feed.begin_reconciliation(account).generation
        message = dict(channel='orderUpdates', data=[dict(order=dict(coin='XRP', oid=5),
            status='filled', statusTimestamp=T+10000)])
        self.feed._receive(account, generation, json.dumps(message))
        self.supervisor.pass_once()
        self.assertGreater(len(self.fx.raw.calls), reads)
        self.assertTrue(self.feed.entry_allowed(account))

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
        state['blocked_lanes'] = {safety._lane(trade['account'], trade['symbol']): 'LIVE_OWNERSHIP_UNAVAILABLE'}
        other = next(a for a in state['routes'].values() if a != trade['account'])
        self.assertFalse(self.capability._peers_protected(state, account=trade['account']))
        self.assertTrue(self.capability._peers_protected(state, account=other))
        state['blocked_lanes'] = {}
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
