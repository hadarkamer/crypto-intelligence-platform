"""Actual startup/HTTP routing and deterministic ownership-transfer failures."""
from copy import deepcopy
import io
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from . import experimental_live_startup as startup
from . import app, gunicorn_conf, long_stream_runtime as legacy
from .test_experimental_live_state import LIVE_ROUTES
from .test_experimental_live_runtime import MemoryTransactions
from .test_experimental_execution_runtime import T


class LegacyMemory:
    def __init__(self): self.rows = {}; self.blocker = None
    def for_account(self, account): return deepcopy(self.rows.get(account, []))
    def entry_blocker(self): return self.blocker


class LegacyOwner:
    def __init__(self): self.started = 0; self.stopped = 0; self.joined = True
    def start(self): self.started += 1; return True
    def stop_and_join(self): self.stopped += 1; return self.joined
    def stop(self): self.stopped += 1


class Service:
    def __init__(self):
        self.store = MemoryTransactions(LIVE_ROUTES, T-60000)
        self.old = LegacyMemory()
        self.runtime = SimpleNamespace(store=self.store, venue=SimpleNamespace(
            legacy_store=self.old, budget=object(), now=lambda: T))
        self.release = dict(release_id='a'*64, entries_enabled=True,
            not_before_ms=T-60000, entry_expires_at_ms=T+60000)
        self.release_loader = lambda: deepcopy(self.release)
        self.started = 0; self.stopped_count = 0; self.joined = True
        self.fail_start = False
    def start(self):
        self.started += 1
        if self.fail_start: raise ValueError('SYNTHETIC_PARTIAL_START')
    def stop(self): self.stopped_count += 1; return self.joined


class HandoverTests(unittest.TestCase):
    def setUp(self):
        guard = patch('socket.create_connection', side_effect=AssertionError('NO_NETWORK'))
        guard.start(); self.addCleanup(guard.stop)
        self.service = Service(); self.owner = LegacyOwner(); self.env = {}
        self.coordinator = startup.HandoverCoordinator(self.env,
            service_factory=lambda env: self.service, legacy_owner=self.owner)
        self.calls = []; self.nonflat = False; self.after_stop_nonflat = False
        test = self
        class Reader:
            def __init__(self, *, priority, budget):
                test.assertEqual(priority, 'background')
                test.assertIs(budget, test.service.runtime.venue.budget)
            def read(self, kind, account):
                test.calls.append((kind, account))
                if kind == 'frontendOpenOrders': return []
                nonflat = test.nonflat or (test.after_stop_nonflat and test.owner.stopped)
                return dict(assetPositions=[dict(position=dict(coin='BTC', szi='1'))] if nonflat else [])
        p = patch('hl_testnet_runtime.card_sync_evidence.PublicReader', Reader)
        p.start(); self.addCleanup(p.stop)

    def test_flat_handover_persists_receipt_then_starts_one_new_owner(self):
        result = self.coordinator.pass_once()
        self.assertTrue(result['running'])
        self.assertFalse(result['new_entries_enabled'])
        self.assertEqual(self.owner.started, 1); self.assertEqual(self.owner.stopped, 1)
        self.assertEqual(self.service.started, 1); self.assertEqual(len(self.calls), 16)
        receipt = self.service.store.load()[startup.RECEIPT]
        self.assertEqual(receipt['routes'], self.service.store.load()['routes'])
        self.coordinator.pass_once()
        self.assertEqual(self.service.started, 1); self.assertEqual(len(self.calls), 16)
        before = self.service.store.load()
        self.service.last_cycle_error_code = None
        for status in ('DEFINITELY_NOT_SUBMITTED_REOBSERVE',
                       'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED',
                       'OBSERVED_IDLE_REUSED'):
            self.service.last_status = status
            self.assertEqual(self.coordinator.health()['last_cycle_error_code'],
                None if status == 'OBSERVED_IDLE_REUSED' else status)
            self.assertIsNone(self.service.last_cycle_error_code)
        self.service.last_status = 'DEFINITELY_NOT_SUBMITTED_REOBSERVE'
        self.service.last_cycle_error_code = 'TESTNET_REQUEST_BUDGET_BUSY'
        self.assertEqual(self.coordinator.health()['last_cycle_error_code'],
            'TESTNET_REQUEST_BUDGET_BUSY')
        self.assertEqual(self.service.store.load(), before)
        self.assertEqual(len(self.calls), 16)

    def test_pending_legacy_keeps_old_protection_without_any_extra_public_read(self):
        account = LIVE_ROUTES['long_account']['account']
        self.service.old.rows[account] = [dict(account=account, symbol='BTC', bucket='b'*64,
            revision=1, pending='unknown', bindings=[])]
        result = self.coordinator.pass_once()
        self.assertTrue(result['legacy_protection_retained'])
        self.assertEqual(self.owner.started, 1); self.assertEqual(self.owner.stopped, 0)
        self.assertEqual(self.service.started, 0); self.assertEqual(self.calls, [])

    def test_missing_candidate_schema_or_price_configuration_retains_legacy(self):
        self.coordinator.factory = Mock(side_effect=ValueError('PRIVATE_CONNECTION_FAILURE'))
        result = self.coordinator.pass_once()
        self.assertTrue(result['legacy_protection_retained'])
        self.assertNotIn('PRIVATE', json.dumps(result))
        self.assertEqual(self.owner.stopped, 0); self.assertEqual(self.calls, [])

    def test_unknown_account_position_keeps_legacy_owner(self):
        self.nonflat = True
        result = self.coordinator.pass_once()
        self.assertFalse(result['running']); self.assertTrue(result['legacy_protection_retained'])
        self.assertEqual(self.owner.stopped, 0); self.assertEqual(self.service.started, 0)

    def test_changed_inventory_after_stop_restores_legacy_before_candidate_start(self):
        self.after_stop_nonflat = True
        result = self.coordinator.pass_once()
        self.assertFalse(result['running']); self.assertTrue(result['legacy_protection_retained'])
        self.assertEqual(self.owner.started, 2); self.assertEqual(self.service.started, 0)
        self.assertNotIn(startup.RECEIPT, self.service.store.load())

    def test_shutdown_timeout_latches_and_never_starts_or_restarts_owner(self):
        self.owner.joined = False
        result = self.coordinator.pass_once()
        self.assertEqual(result['status'], 'LEGACY_SHUTDOWN_STILL_PENDING')
        calls = len(self.calls)
        self.coordinator.pass_once()
        self.assertEqual(self.owner.started, 1); self.assertEqual(self.service.started, 0)
        self.assertEqual(len(self.calls), calls)
        self.owner.joined = True
        self.coordinator.pass_once()
        self.assertEqual(self.service.started, 1)

    def test_candidate_partial_start_does_not_restore_legacy_until_join_confirmed(self):
        self.service.fail_start = True; self.service.joined = False
        result = self.coordinator.pass_once()
        self.assertEqual(result['status'], 'CANDIDATE_SHUTDOWN_STILL_PENDING')
        self.assertEqual(self.owner.started, 1)
        self.coordinator.pass_once()
        self.assertEqual(self.owner.started, 1); self.assertEqual(self.service.started, 1)

    def test_restart_resumes_open_candidate_without_flat_or_legacy_start(self):
        self.coordinator.pass_once()
        self.service.store.mutate(lambda state: state['trades'].update({'owned': dict(phase='OPEN')}))
        self.calls.clear(); self.nonflat = True
        restarted_owner = LegacyOwner()
        restarted = startup.HandoverCoordinator(self.env, legacy_owner=restarted_owner,
            service_factory=lambda env: self.service)
        result = restarted.pass_once()
        self.assertTrue(result['running']); self.assertEqual(restarted_owner.started, 0)
        self.assertEqual(self.calls, [])

    def test_new_release_preserves_existing_protection_and_requires_its_own_attestation(self):
        self.env['HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE'] = 'a'*64
        self.coordinator.pass_once()
        self.service.store.mutate(lambda state: state['trades'].update({'owned': dict(phase='OPEN')}))
        self.service.release['release_id'] = 'b'*64
        restarted = startup.HandoverCoordinator(self.env, legacy_owner=LegacyOwner(),
            service_factory=lambda env: self.service)
        result = restarted.pass_once()
        self.assertTrue(result['running']); self.assertFalse(result['new_entries_enabled'])

    def test_harmless_legacy_observation_revision_does_not_invalidate_receipt(self):
        account = LIVE_ROUTES['long_account']['account']
        self.service.old.rows[account] = [dict(account=account, symbol='BTC', bucket='b'*64,
            revision=1, pending=None, bindings=[], last_request='c'*64)]
        self.coordinator.pass_once()
        self.service.old.rows[account][0]['revision'] += 1
        self.service.old.rows[account][0]['evidence'] = {'at_ms': T+1}
        self.service.store.mutate(lambda state: state['trades'].update({'owned': dict(phase='OPEN')}))
        self.calls.clear()
        restarted = startup.HandoverCoordinator(self.env, legacy_owner=LegacyOwner(),
            service_factory=lambda env: self.service)
        self.assertTrue(restarted.pass_once()['running']); self.assertEqual(self.calls, [])

    def test_uncertain_handover_commit_never_starts_new_owner(self):
        original = self.service.store.mutate
        def lost(fn):
            original(fn)
            raise ValueError('LOST_COMMIT_REPLY')
        self.service.store.mutate = lost
        result = self.coordinator.pass_once()
        self.assertFalse(result['running']); self.assertEqual(self.service.started, 0)
        self.assertTrue(result['legacy_protection_retained'])

    def test_attestation_is_release_bound_and_removal_closes_entry_gate(self):
        self.env['HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE'] = 'a'*64
        result = self.coordinator.pass_once()
        self.assertTrue(result['new_entries_enabled'])
        self.assertTrue(result['predecessor_retirement_operator_attested'])
        self.assertFalse(result['cross_process_ownership_verified'])
        self.env.clear()
        self.assertFalse(self.service.startup_entry_gate())

    def test_start_is_nonblocking_and_does_not_collect_on_gunicorn_thread(self):
        with patch.object(self.coordinator, 'pass_once', side_effect=AssertionError('NO_INLINE_READ')), \
             patch.object(startup.threading, 'Thread') as thread:
            self.coordinator.start()
            thread.return_value.start.assert_called_once()
        self.assertEqual(self.calls, [])

    def test_process_owner_is_acquired_before_any_legacy_or_candidate_start(self):
        lease=Mock(); lease.health.return_value=dict(status='HELD',held=True)
        lease.acquire.side_effect=ValueError('ANOTHER_PROCESS_OWNS_RUNTIME')
        self.coordinator.owner_lease=lease
        result=self.coordinator.pass_once()
        self.assertEqual(result['status'],'PROCESS_OWNERSHIP_UNAVAILABLE')
        self.assertEqual(self.owner.started,0); self.assertEqual(self.service.started,0)
        self.assertEqual(self.calls,[])
        lease.acquire.side_effect=None
        self.coordinator.pass_once()
        self.assertIs(self.service.runtime.startup_owner,lease)
        self.assertIs(self.service.runtime.venue.startup_owner,lease)

    def test_owner_not_released_until_candidate_threads_are_confirmed_stopped(self):
        lease=Mock(); lease.health.return_value=dict(status='HELD',held=True)
        lease.release.return_value=True
        self.coordinator.owner_lease=lease
        self.coordinator.pass_once()
        self.service.joined=False
        self.assertFalse(self.coordinator.stop()); lease.release.assert_not_called()
        self.service.joined=True
        self.assertTrue(self.coordinator.stop()); lease.release.assert_called_once_with()

    def test_lost_process_owner_cannot_report_enabled_runtime(self):
        lease=Mock(); lease.health.return_value=dict(status='HELD',held=True)
        self.coordinator.owner_lease=lease
        self.env['HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE']='a'*64
        self.coordinator.pass_once()
        lease.health.return_value=dict(status='LOST',held=False)
        result=self.coordinator.health()
        self.assertFalse(result['running']); self.assertFalse(result['new_entries_enabled'])

    def test_shutdown_during_composition_never_starts_legacy_or_candidate(self):
        def delayed(env):
            self.coordinator._stop.set()
            return self.service
        self.coordinator.factory=delayed
        result=self.coordinator.pass_once()
        self.assertEqual(result['status'],'SHUTDOWN_REQUESTED')
        self.assertEqual(self.owner.started,0); self.assertEqual(self.service.started,0)

    def test_shutdown_during_flat_check_never_commits_or_starts_new_owner(self):
        def delayed(fingerprint):
            self.coordinator._stop.set()
            return T
        with patch.object(self.coordinator,'_flat',side_effect=delayed):
            result=self.coordinator.pass_once()
        self.assertEqual(result['status'],'SHUTDOWN_REQUESTED')
        self.assertEqual(self.owner.started,1); self.assertEqual(self.service.started,0)
        self.assertNotIn(startup.RECEIPT,self.service.store.load())


class EntrypointTests(unittest.TestCase):
    def test_legacy_environment_preserves_approvals_and_never_mutates_caller(self):
        from .test_long_stream_runtime import env as old_environment
        from .test_experimental_live_release import environment as new_environment
        from . import emergency_close
        settings={**old_environment(),**new_environment(),
            'HL_TESTNET_EXPERIMENTAL_HANDOVER':startup.HANDOVER,
            'HL_TESTNET_SHORT_STREAM':'approved_alerts_v1',
            'HL_TESTNET_SHORT_NOT_BEFORE':'2026-09-26T14:00:00Z',
            'HL_TESTNET_EMERGENCY_CLOSE':emergency_close.APPROVAL,
            'HL_TESTNET_EMERGENCY_RELEASE':emergency_close.CONTINUOUS_RELEASE}
        before=deepcopy(settings)
        derived=startup.legacy_environment(settings)
        self.assertEqual(settings,before)
        self.assertEqual(derived['HL_TESTNET_RUNTIME_MODE'],legacy.MODE)
        self.assertEqual(derived['HL_TESTNET_FILLED_DISPATCH'],settings['HL_TESTNET_FILLED_DISPATCH'])
        self.assertEqual(derived['HL_TESTNET_LONG_ENTRY_ENABLED'],'false')
        for missing in ('HL_TESTNET_FILLED_DISPATCH','HL_TESTNET_EMERGENCY_RELEASE'):
            bad={k:v for k,v in settings.items() if k!=missing}
            with self.assertRaises(ValueError): startup.legacy_environment(bad)

    def test_default_off_never_constructs_owner(self):
        with patch.object(startup, 'HandoverCoordinator', side_effect=AssertionError('OFF')):
            self.assertFalse(startup.start({}))

    def test_gunicorn_selects_only_experimental_coordinator(self):
        with patch.dict(os.environ, {'HL_TESTNET_RUNTIME_MODE':'experimental_testnet_v1'}, clear=True), \
             patch.object(startup, 'start') as selected, \
             patch.object(legacy, 'start', side_effect=AssertionError('DUPLICATE')), \
             patch.object(app, 'start_read_only_check', side_effect=AssertionError('WRONG_MODE')):
            gunicorn_conf.post_worker_init(None)
            selected.assert_called_once_with()

    def test_startup_pending_returns_503_without_falling_through_to_old_inbox(self):
        with patch.dict(os.environ, {'HL_TESTNET_RUNTIME_MODE':'experimental_testnet_v1'}, clear=True), \
             patch.object(startup, '_coordinator', None), \
             patch('hl_testnet_runtime.experimental_plan_intake.application', side_effect=AssertionError('WRONG_INBOX')):
            statuses=[]
            raw=b''.join(app.application(dict(PATH_INFO='/internal/testnet-experimental-plans/v1',
                REQUEST_METHOD='POST'), lambda status,headers: statuses.append(status)))
            self.assertEqual(statuses, ['503 Service Unavailable'])
            self.assertEqual(json.loads(raw)['status'], 'EXPERIMENTAL_STARTUP_NOT_READY')

    def test_real_application_authenticates_and_records_in_candidate_inbox_during_handover(self):
        # Reuse raw fixture setup, not its tests. app, HMAC and native inbox are
        # actual production functions; no HTTP path can call tick or send.
        from .test_experimental_live_service import FullConnectionTests
        from experimental_execution_fixtures import r2732_message
        from . import experimental_plan_intake as intake
        harness=FullConnectionTests('test_intake_authentication_cannot_send_and_duplicate_is_idempotent')
        harness.setUp(); self.addCleanup(harness.doCleanups)
        msg=r2732_message(entry=2.3,decision_ms=T-60000)
        raw=json.dumps(msg,separators=(',',':')).encode(); stamp=str(harness.oracle.t//1000)
        env=dict(PATH_INFO=intake.PATH,REQUEST_METHOD='POST',CONTENT_TYPE='application/json',
            CONTENT_LENGTH=str(len(raw)),HTTP_X_PLAN_TIMESTAMP=stamp,
            HTTP_X_PLAN_SIGNATURE=intake.signature(harness.key,stamp,raw))
        with patch.dict(os.environ,{'HL_TESTNET_RUNTIME_MODE':'experimental_testnet_v1'},clear=True), \
             patch.object(startup,'_coordinator',SimpleNamespace(service=harness.service)), \
             patch.object(harness.service,'tick',side_effect=AssertionError('HTTP_EXECUTION')):
            for expected in ('RECORDED','DUPLICATE'):
                statuses=[]
                body=b''.join(app.application({**env,'wsgi.input':io.BytesIO(raw)},
                    lambda status,headers:statuses.append(status)))
                self.assertEqual(statuses,['200 OK']); self.assertEqual(json.loads(body)['status'],expected)
            env['HTTP_X_PLAN_SIGNATURE']='0'*64
            statuses=[]
            app.application({**env,'wsgi.input':io.BytesIO(raw)},lambda status,headers:statuses.append(status))
            self.assertEqual(statuses,['403 Forbidden'])
        self.assertEqual(harness.http,[]); self.assertEqual(harness.raw.calls,[])
        self.assertIn(msg['occurrence_id'],harness.store.load()['sources'])

    def test_health_is_private_value_free_and_reports_attestation_separately(self):
        with patch.dict(os.environ, {'HL_TESTNET_RUNTIME_MODE':'experimental_testnet_v1'}, clear=True), \
             patch.object(startup, '_coordinator', None):
            body=b''.join(app.application(dict(PATH_INFO='/healthz',REQUEST_METHOD='GET'),lambda *args:None))
            value=json.loads(body)
            self.assertFalse(value['read_only']); self.assertFalse(value['continuous_trading'])
            self.assertFalse(value['experimental_testnet']['cross_process_ownership_verified'])

    def test_worker_exit_stops_coordinator(self):
        with patch.object(startup,'stop') as stopped, \
             patch('hl_testnet_runtime.single_trial_controller.stop'), \
             patch('hl_testnet_runtime.pending_cancel_monitor.stop'), \
             patch('hl_testnet_runtime.card_sync.stop'), \
             patch('hl_testnet_runtime.filled_trial_runtime.stop'), patch.object(legacy,'stop'):
            gunicorn_conf.worker_exit(None,None)
            stopped.assert_called_once_with()

    def test_legacy_stop_timeout_prevents_stop_event_reuse(self):
        event=Mock(); thread=Mock(); thread.is_alive.return_value=True
        with patch.object(legacy,'_thread',thread),patch.object(legacy,'_app_thread',None), \
             patch.object(legacy,'_fill_wakeups',None),patch.object(legacy,'_stop',event), \
             patch.object(legacy,'_handover_stopping',False),patch.object(legacy,'stop'), \
             patch('hl_testnet_runtime.emergency_close.stop_supervisor',return_value=False):
            self.assertFalse(legacy.stop_and_join(timeout=0))
            with self.assertRaisesRegex(ValueError,'SHUTDOWN_STILL_PENDING'):
                legacy.start(protection_env={})
            event.clear.assert_not_called()

    def test_legacy_normal_and_emergency_boundaries_refuse_lost_owner(self):
        from .filled_quantity_dispatch import TestnetVenue
        from .emergency_close import Venue
        lease=Mock(); lease.verify.side_effect=ValueError('OWNER_LOST')
        normal=TestnetVenue({}); normal.startup_owner=lease
        emergency=Venue({}); emergency.startup_owner=lease
        with self.assertRaisesRegex(ValueError,'OWNER_LOST'):
            normal._gate({},None)
        with self.assertRaisesRegex(ValueError,'OWNER_LOST'):
            emergency.authorize({}, {})


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'), 'disposable PostgreSQL not configured')
class HandoverPostgresTests(HandoverTests):
    """Same transfer/rollback/restart contract with committed native state."""
    def setUp(self):
        super().setUp()
        from .postgres_journal import PostgresJournal
        from .filled_dispatch_store import DispatchStore
        from .experimental_live_state import TestnetExecutionState, PG_SCHEMA
        journal = PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        journal.bootstrap(); DispatchStore(journal).initialize()
        with journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {PG_SCHEMA} CASCADE')
        store = TestnetExecutionState.for_ci(journal)
        store.initialize(LIVE_ROUTES, not_before_ms=T-60000)
        self.service.store = store
        self.service.runtime.store = store
