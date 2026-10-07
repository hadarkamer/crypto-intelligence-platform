"""Authenticated source -> real adapters -> synthetic raw exchange endpoints.

Only the database driver, external HTTP and signing key are test doubles here.
No production method is replaced to create an entry, collect a fill, advance
the lifecycle, authorize a wire action or reconcile finality.
"""
from copy import deepcopy
from decimal import Decimal
import io
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message, hype_row71205_message, sol_g65_message
from . import experimental_execution_runtime as core, experimental_plan_intake as intake
from . import experimental_live_runtime as runtime, experimental_live_dispatch as dispatch
from . import request_budget
from .experimental_live_service import ConnectionService, compose
from .experimental_live_provider import LiveEvidenceProvider
from .experimental_live_state import TestnetExecutionState
from .postgres_journal import PostgresJournal, JournalError
from .test_experimental_live_state import LIVE_ROUTES
from .test_experimental_live_runtime import MemoryTransactions
from .test_experimental_live_provider import RawTestnetFixture, LegacyFixture, ENV, UnavailablePrices
from .test_experimental_execution_runtime import SoftwareExchange, T
from .test_maxpain_execution import plan, ARM, CREATED, report_plan, REPORT_FORMULAS


class SyntheticPrices:
    """Complete synthetic histories ONLY; production does not have this proof."""
    def __init__(self, oracle, store): self.oracle, self.store = oracle, store
    def source_range(self, msg, now):
        return self.oracle.collect(self.store.load())['ranges'][msg['occurrence_id']]['source']
    def mark_window(self, account, symbol, reference, now):
        state = self.store.load()
        cid = next(cid for cid, row in state['sources'].items()
                   if row['source']['symbol'] == symbol and contract.moment_ms(row['source']['source_at']) == reference)
        return self.oracle.collect(state)['ranges'][cid]['testnet']
    def closed_bars(self, msg, now): return deepcopy(self.oracle.bars.get(msg['occurrence_id'], []))


class SyntheticSafety:
    def __init__(self, release): self.release = release
    def verify_checkpoint(self, *, state, request, collected_at_ms, ownership_revision):
        p = request['proposal']
        return dict(account=p['account'], role=p['role'], at_ms=collected_at_ms,
            entry_enabled=self.release['entries_enabled'], emergency_healthy=True,
            feed_reconciled=True, entry_circuit_clear=True, supervisor_at_ms=collected_at_ms,
            not_before_ms=state['not_before_ms'])


class FullConnectionTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.create_connection', 'socket.socket.connect'):
            guard = patch(target, side_effect=AssertionError('NO_EXTERNAL_NETWORK'))
            guard.start(); self.addCleanup(guard.stop)
        self.key = '9' * 64
        self.oracle = SoftwareExchange(T + 10000)
        self.env = {**ENV, 'HL_TESTNET_EXPERIMENTAL_DISPATCH': dispatch.APPROVAL}
        self.http = []; self.fail_reply = False; self.drop_stop = False
        self.build(T - 60000)
        def wallet(_env, role, account, agent): return SimpleNamespace(address=agent)
        p = patch.object(dispatch.roles, 'wallet_for_role', wallet); p.start(); self.addCleanup(p.stop)
        p = patch.dict('sys.modules', {'hyperliquid.utils.signing': SimpleNamespace(
            sign_l1_action=lambda *args: dict(r='0x1', s='0x2', v=27))})
        p.start(); self.addCleanup(p.stop)
        test = self
        class Connection:
            def __init__(self, host, timeout):
                if host != dispatch.HOST: raise AssertionError('WRONG_EXCHANGE')
            def request(self, method, path, body, headers):
                data = json.loads(body); test.http.append(data)
                request = next(r for r in test.store.load()['requests'].values()
                    if r['nonce'] == data['nonce'] and r['proposal']['action'] == data['action'])
                if test.drop_stop and request['proposal']['operation']=='CREATE_EXIT' and request['proposal']['leg']=='STOP':
                    test.drop_stop = False
                    # The external outcome is unknown to the production sender.
                    raise TimeoutError('SYNTHETIC_STOP_NO_RESPONSE')
                result = test.oracle.send(request, admission=SimpleNamespace(consume=lambda request: None))
                row = 'success' if data['action']['type'] == 'cancel' else dict(resting=dict(oid=int(result['oid'])))
                self.reply = dict(status='ok', response=dict(type='cancel' if data['action']['type'] == 'cancel' else 'order',
                    data=dict(statuses=[row])))
            def getresponse(self):
                if test.fail_reply:
                    test.fail_reply = False
                    raise TimeoutError('SYNTHETIC_LOST_RESPONSE')
                raw = json.dumps(self.reply).encode()
                return SimpleNamespace(status=200, read=lambda n: raw[:n])
            def close(self): pass
        p = patch.object(dispatch.http.client, 'HTTPSConnection', Connection); p.start(); self.addCleanup(p.stop)

    def build(self, not_before):
        self.memory = MemoryTransactions(LIVE_ROUTES, not_before)
        journal = PostgresJournal.for_ci('postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci')
        self.store = TestnetExecutionState.for_ci(journal)
        self.store.load = self.memory.load; self.store.mutate = self.memory.mutate
        self.store.commit_attempt = self.memory.commit_attempt
        self.release = dict(domain='testnet', release_id='a'*64, dispatch_enabled=True,
            protection_enabled=True, entries_enabled=True, not_before_ms=not_before,
            entry_expires_at_ms=max(T, ARM) + 86400000,
            routes={role: row['account'] for role, row in LIVE_ROUTES.items()})
        self.budget = object.__new__(request_budget.Budget)
        self.budget.acquire = lambda *args, **kw: core._Permit(self.oracle.now, self.oracle.now())
        self.raw = RawTestnetFixture(self.oracle, self.store.load, self.budget)
        self.provider = LiveEvidenceProvider(self.env, legacy_store=LegacyFixture(), experimental_store=self.store,
            price_evidence=SyntheticPrices(self.oracle, self.store), budget=self.budget,
            clock=self.oracle.now, info_reader=self.raw, public_reader=self.raw,
            lookup_reader=self.raw.lookup, safety_provider=SyntheticSafety(self.release))
        self.reconnect()

    def reconnect(self):
        self.port = dispatch.LiveDispatchPort(self.env, context_loader=self.provider.dispatch_context,
            request_loader=self.store.request, claim_transport=self.store.claim_transport,
            release_loader=lambda: deepcopy(self.release), budget=self.budget, clock=self.oracle.now)
        self.worker = runtime.TestnetExecutionRuntime(self.store, self.provider, self.port,
            release_loader=lambda: deepcopy(self.release), mode=runtime.MODE)
        self.service = ConnectionService(self.worker, key=self.key, release_loader=lambda: deepcopy(self.release))

    def ingest(self, message):
        raw = json.dumps(message, separators=(',', ':')).encode()
        stamp = str(self.oracle.t // 1000)
        return self.service.accept_authenticated(raw, {'X-Plan-Timestamp': stamp,
            'X-Plan-Signature': intake.signature(self.key, stamp, raw)})

    def seed(self, msg):
        price = msg['proof'].get('reference_price', msg['proof'].get('source_price', msg['entry']))
        account = self.release['routes']['long_account' if msg['side'] == 'LONG' else 'short_account']
        self.oracle.mark[core._lane(account, msg['symbol'])] = str(price)
        self.ingest(msg)

    def test_intake_authentication_cannot_send_and_duplicate_is_idempotent(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000)
        first = self.ingest(msg); again = self.ingest(msg)
        self.assertEqual(first['status'], 'RECORDED'); self.assertEqual(again['status'], 'DUPLICATE')
        self.assertEqual(self.http, []); self.assertEqual(self.raw.calls, [])
        with self.assertRaisesRegex(ValueError, 'AUTHENTICATION'):
            self.service.accept_authenticated(json.dumps(msg).encode(), {})
        self.assertEqual(self.http, [])

    def test_intake_wakes_only_after_commit_and_not_for_duplicate_or_failed_commit(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000)
        receive = self.worker.receive
        observed = []
        def commit(messages):
            observed.append(self.service._wake.is_set())
            result = receive(messages)
            observed.append(self.service._wake.is_set())
            return result
        with patch.object(self.worker, 'receive', side_effect=commit):
            self.ingest(msg)
        self.assertEqual(observed, [False, False])
        self.assertTrue(self.service._wake.is_set())
        self.service._wake.clear()
        self.assertEqual(self.ingest(msg)['status'], 'DUPLICATE')
        self.assertFalse(self.service._wake.is_set())
        with patch.object(self.worker, 'receive', side_effect=JournalError('COMMIT_UNKNOWN')):
            with self.assertRaisesRegex(JournalError, 'COMMIT_UNKNOWN'):
                self.ingest(msg)
        self.assertFalse(self.service._wake.is_set())
        self.assertEqual(self.http, []); self.assertEqual(self.raw.calls, [])

    def test_authenticated_commit_wakes_sleeping_loop_without_polling_delay(self):
        waiting = threading.Event(); completed = threading.Event(); calls = []
        wait = self.service._wake.wait
        def observe_wait(timeout):
            waiting.set()
            return wait(timeout)
        def cycle():
            calls.append(True)
            if len(calls) == 2:
                completed.set(); self.service._stop.set()
        with patch.object(self.service._wake, 'wait', side_effect=observe_wait), \
             patch.object(self.service, 'tick', side_effect=cycle):
            self.service._thread = threading.Thread(target=self.service.run,
                args=(self.service._stop,), kwargs={'interval_seconds': 30})
            self.service._thread.start()
            try:
                self.assertTrue(waiting.wait(2))
                self.ingest(r2732_message(entry=2.3, decision_ms=T-60000))
                self.assertTrue(completed.wait(2))
            finally:
                self.assertTrue(self.service.stop())
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.http, []); self.assertEqual(self.raw.calls, [])

    def test_arrival_during_cycle_is_not_lost_when_loop_returns_to_wait(self):
        active = threading.Event(); finish = threading.Event(); completed = threading.Event()
        calls = []
        def cycle():
            calls.append(True)
            if len(calls) == 1:
                active.set()
                if not finish.wait(2):
                    raise AssertionError('TEST_CYCLE_NOT_RELEASED')
            else:
                completed.set(); self.service._stop.set()
        with patch.object(self.service, 'tick', side_effect=cycle):
            self.service._thread = threading.Thread(target=self.service.run,
                args=(self.service._stop,), kwargs={'interval_seconds': 30})
            self.service._thread.start()
            try:
                self.assertTrue(active.wait(2))
                self.ingest(r2732_message(entry=2.3, decision_ms=T-60000))
                finish.set()
                self.assertTrue(completed.wait(2))
            finally:
                finish.set(); self.assertTrue(self.service.stop())
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.http, []); self.assertEqual(self.raw.calls, [])

    def test_manual_tick_and_second_loop_cannot_overlap_background_cycle(self):
        active = threading.Event(); finish = threading.Event()
        def run_once(**kwargs):
            active.set()
            if not finish.wait(2):
                raise AssertionError('TEST_CYCLE_NOT_RELEASED')
            self.service._stop.set()
            return {'status': 'OBSERVED_NO_ACTION'}
        with patch.object(self.worker, 'run_once', side_effect=run_once) as operation:
            self.service._thread = threading.Thread(target=self.service.run, args=(self.service._stop,))
            self.service._thread.start()
            try:
                self.assertTrue(active.wait(2))
                with self.assertRaisesRegex(ValueError, 'CYCLE_ALREADY_RUNNING'):
                    self.service.tick()
                with self.assertRaisesRegex(ValueError, 'LOOP_ALREADY_RUNNING'):
                    self.service.run(threading.Event())
                self.assertEqual(operation.call_count, 1)
            finally:
                finish.set(); self.assertTrue(self.service.stop())
        self.assertEqual(self.service.cycles, 1)

    def test_stop_wakes_long_wait_and_restart_reads_durable_inbox(self):
        waiting = threading.Event(); wait = self.service._wake.wait
        def observe_wait(timeout):
            waiting.set()
            return wait(timeout)
        with patch.object(self.service._wake, 'wait', side_effect=observe_wait), \
             patch.object(self.service, 'tick') as tick:
            self.service._thread = threading.Thread(target=self.service.run,
                args=(self.service._stop,), kwargs={'interval_seconds': 30})
            self.service._thread.start()
            try:
                self.assertTrue(waiting.wait(2))
            finally:
                self.assertTrue(self.service.stop())
            self.assertEqual(tick.call_count, 1)
        msg = r2732_message(entry=2.3, decision_ms=T-60000)
        self.ingest(msg)
        self.reconnect()
        self.assertFalse(self.service._wake.is_set())
        self.assertIn(msg['occurrence_id'], self.store.load()['sources'])
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests), 1)

    def test_external_stop_event_remains_responsive_without_service_stop(self):
        external_stop = threading.Event(); waiting = threading.Event()
        wait = self.service._wake.wait
        def observe_wait(timeout):
            waiting.set()
            return wait(timeout)
        with patch.object(self.service._wake, 'wait', side_effect=observe_wait), \
             patch.object(self.service, 'tick') as tick:
            thread = threading.Thread(target=self.service.run,
                args=(external_stop,), kwargs={'interval_seconds': 30})
            thread.start()
            try:
                self.assertTrue(waiting.wait(2))
                external_stop.set()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
            finally:
                external_stop.set(); self.service._wake.set(); thread.join(timeout=2)
        self.assertEqual(tick.call_count, 1)

    def test_r2732_through_raw_provider_sender_partial_protection_and_close(self):
        self.oracle.instant_fraction = Decimal('.5')
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        result = self.service.tick(); self.assertEqual(result.get('operation'), 'ENTRY', result)
        self.release['entries_enabled'] = False
        for _ in range(3): self.service.tick()
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'], 'OPEN')
        for leg in ('STOP', 'TAKE_PROFIT'):
            self.assertEqual(Decimal(self.oracle.orders[self.oracle.oid(leg)]['view']['wire_order']['s']),
                             Decimal(trade['quantity'])/2)
        self.oracle.fill(self.oracle.oid('TAKE_PROFIT'), str(Decimal(trade['quantity'])/2))
        for _ in range(3): self.service.tick()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'], 'CLOSED')
        self.assertEqual(len(self.http), 4)

    def test_unknown_reply_then_new_port_reconciles_without_duplicate(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        self.fail_reply = True
        result = self.service.tick(); self.assertIn('UNKNOWN', result['status'])
        self.reconnect()
        for _ in range(3): self.service.tick()
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests), 1)
        self.assertEqual(len(self.http), 3)

    def test_all_eight_formulas_through_real_provider_and_wire_adapters(self):
        messages = [report_plan(spec, (spec[2]+spec[3])/2) for spec in REPORT_FORMULAS]
        messages += [r2732_message(entry=2.3, decision_ms=ARM-60000),
                     hype_row71205_message(entry=100, decision_ms=ARM-60000),
                     sol_g65_message(reference=100, decision_ms=ARM-60000)]
        for msg in messages:
            with self.subTest(rule=msg['rule_id']):
                self.oracle = SoftwareExchange(CREATED if msg['family']=='maxpain' else ARM+10000)
                self.build(CREATED-60000)
                self.seed(msg); self.oracle.t = max(ARM, self.oracle.t)
                result = self.service.tick()
                self.assertEqual(result.get('operation'), 'ENTRY', result)
                if msg['family'] in ('maxpain','sol_g65'):
                    oid = self.oracle.oid('ENTRY')
                    self.oracle.fill(oid, self.oracle.orders[oid]['view']['wire_order']['s'])
                    account = self.release['routes']['long_account' if msg['side']=='LONG' else 'short_account']
                    self.oracle.mark[core._lane(account,msg['symbol'])] = msg['entry']
                for _ in range(3): self.service.tick()
                self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'], 'OPEN')
                self.assertEqual({o['leg'] for o in self.oracle.orders.values()}, {'ENTRY','STOP','TAKE_PROFIT'})

    def test_two_accounts_keep_parallel_positions_and_exits_separate(self):
        self.oracle = SoftwareExchange(CREATED)
        self.build(CREATED-60000)
        long = plan(); self.seed(long); self.oracle.t = ARM
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        oid = self.oracle.oid('ENTRY')
        self.oracle.fill(oid, self.oracle.orders[oid]['view']['wire_order']['s'])
        self.oracle.mark[core._lane(self.release['routes']['long_account'], 'HYPE')] = long['entry']
        for _ in range(3): self.service.tick()
        self.oracle.t = ARM+10000
        short = r2732_message(entry=2.3, decision_ms=ARM-60000)
        self.seed(short)
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        for _ in range(3): self.service.tick()
        trades = self.store.load()['trades']
        self.assertEqual({t['phase'] for t in trades.values()}, {'OPEN'})
        self.assertEqual({t['role'] for t in trades.values()}, {'long_account', 'short_account'})
        short_take = next(oid for oid, row in self.oracle.orders.items()
                          if row['leg']=='TAKE_PROFIT' and row['account']==trades[short['occurrence_id']]['account'])
        self.oracle.fill(short_take, trades[short['occurrence_id']]['quantity'])
        for _ in range(3): self.service.tick()
        after = self.store.load()['trades']
        self.assertEqual(after[short['occurrence_id']]['phase'], 'CLOSED')
        self.assertEqual(after[long['occurrence_id']]['phase'], 'OPEN')

    def test_missing_actual_price_history_keeps_entry_closed(self):
        self.seed(r2732_message(entry=2.3, decision_ms=T-60000))
        self.provider.prices = UnavailablePrices()
        result = self.service.tick()
        self.assertEqual(result['status'], 'OBSERVED_NO_ACTION'); self.assertEqual(self.http, [])
        self.assertIn('CONTINUOUS_REFERENCE_MARK_HISTORY_UNAVAILABLE', self.store.load()['entry_blocked'].values())

    def test_source_transport_failure_does_not_delay_halted_partial_fill_protection(self):
        self.oracle.instant_fraction=Decimal('.5')
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg)
        self.assertEqual(self.service.tick().get('operation'),'ENTRY')
        self.release['entries_enabled']=False
        with patch.object(self.provider.prices,'source_range',side_effect=TimeoutError('SOURCE_TIMEOUT')) as source, \
             patch.object(self.provider.prices,'mark_window',side_effect=TimeoutError('MARK_HISTORY_TIMEOUT')) as history, \
             patch.object(self.provider.prices,'closed_bars',side_effect=OSError('SOURCE_DB_UNAVAILABLE')), \
             patch.object(self.provider,'_capacity',side_effect=AssertionError('NO_ADMISSION_DURING_HALT')):
            for _ in range(3):self.service.tick()
        self.assertEqual(source.call_count,0);self.assertEqual(history.call_count,0)
        trade=self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'],'OPEN')
        for leg in ('STOP','TAKE_PROFIT'):
            order=self.oracle.orders[self.oracle.oid(leg)]['view']['wire_order']
            self.assertEqual(Decimal(order['s']),Decimal(trade['quantity'])/2)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests),1)

    def test_admitted_trade_never_repeats_entry_preflight_when_entries_remain_enabled(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg)
        self.assertEqual(self.service.tick().get('operation'),'ENTRY')
        with patch.object(self.provider.prices,'source_range',side_effect=AssertionError('NO_RANGE_FOR_ADMITTED_TRADE')) as source, \
             patch.object(self.provider,'_capacity',side_effect=AssertionError('NO_CAPACITY_FOR_ADMITTED_TRADE')) as capacity:
            for _ in range(3):self.service.tick()
        self.assertEqual(source.call_count,0);self.assertEqual(capacity.call_count,0)
        self.assertEqual({o['leg'] for o in self.oracle.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})

    def test_unresolved_entry_defers_other_candidate_source_before_initial_stop(self):
        self.oracle=SoftwareExchange(ARM+10000);self.build(CREATED-60000)
        msg=r2732_message(entry=2.3,decision_ms=ARM-60000);self.seed(msg)
        self.assertEqual(self.service.tick().get('operation'),'ENTRY')
        other=hype_row71205_message(entry=100,decision_ms=ARM-60000);self.seed(other)
        with patch.object(self.provider.prices,'source_range',side_effect=TimeoutError('SOURCE_TIMEOUT')) as source:
            result=self.service.tick()
        self.assertEqual(result.get('operation'),'CREATE_EXIT')
        self.assertEqual(self.oracle.requests[-1]['proposal']['leg'],'STOP')
        self.assertEqual(self.oracle.requests[-1]['proposal']['card_id'],msg['occurrence_id'])
        self.assertEqual(source.call_count,0)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests),1)

    def test_halted_pending_entry_still_cancels_at_original_maxpain_target(self):
        self.oracle=SoftwareExchange(CREATED);self.build(CREATED-60000)
        msg=plan();self.seed(msg);self.oracle.t=ARM
        self.assertEqual(self.service.tick().get('operation'),'ENTRY')
        self.release['entries_enabled']=False
        self.oracle.mark[core._lane(self.release['routes']['long_account'],'HYPE')]=msg['original_target']
        with patch.object(self.provider.prices,'source_range',side_effect=TimeoutError('SOURCE_TIMEOUT')) as source, \
             patch.object(self.provider,'_capacity',side_effect=AssertionError('NO_ADMISSION_DURING_HALT')):
            result=self.service.tick()
        self.assertEqual(result.get('operation'),'CANCEL');self.assertEqual(source.call_count,0)
        self.service.tick();self.service.tick()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CANCELED_WITHOUT_FILL')

    def test_source_transport_failure_without_exposure_blocks_only_admission(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg)
        with patch.object(self.provider.prices,'source_range',side_effect=TimeoutError('SOURCE_TIMEOUT')):
            result=self.service.tick()
        self.assertEqual(result['status'],'OBSERVED_NO_ACTION')
        self.assertFalse(self.http)
        self.assertEqual(self.store.load()['entry_blocked'][msg['occurrence_id']],'SOURCE_TIMEOUT')

    def test_freshly_canceled_stop_skips_all_slow_source_reads_and_restores_protection(self):
        self.oracle=SoftwareExchange(ARM+10000);self.build(CREATED-60000)
        msg=r2732_message(entry=2.3,decision_ms=ARM-60000);self.seed(msg)
        self.service.tick()
        for _ in range(3):self.service.tick()
        trade=self.store.load()['trades'][msg['occurrence_id']]
        stop=self.oracle.oid('STOP')
        self.assertEqual(trade['orders'][stop]['status'],'OPEN')
        self.oracle.t+=1
        self.oracle.orders[stop]['view'].update(status='CANCELED',at_ms=self.oracle.t)
        self.seed(hype_row71205_message(entry=100,decision_ms=ARM-60000))
        started=self.oracle.t
        def slow_source(*args):
            self.oracle.t+=20000
            raise TimeoutError('SOURCE_DATABASE_TIMEOUT')
        with patch.object(self.provider.prices,'source_range',side_effect=slow_source) as source, \
             patch.object(self.provider.prices,'mark_window',side_effect=slow_source) as history, \
             patch.object(self.provider.prices,'closed_bars',side_effect=slow_source) as bars, \
             patch.object(self.provider,'_capacity',side_effect=slow_source) as capacity:
            result=self.service.tick()
        self.assertEqual(self.oracle.t,started+1)  # The synthetic wire advances 1 ms.
        self.assertEqual([p.call_count for p in (source,history,bars,capacity)],[0,0,0,0])
        self.assertEqual(result.get('operation'),'CREATE_EXIT')
        self.assertEqual(self.oracle.requests[-1]['proposal']['leg'],'STOP')
        self.assertEqual(self.oracle.requests[-1]['proposal']['card_id'],msg['occurrence_id'])
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests),1)

    def test_lost_process_owner_stops_before_any_exchange_collection(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg)
        def lost():raise ValueError('EXPERIMENTAL_OWNER_SESSION_LOST')
        self.worker.startup_owner=SimpleNamespace(verify=lost)
        with self.assertRaisesRegex(ValueError,'OWNER_SESSION_LOST'):
            self.service.tick()
        self.assertFalse(self.raw.calls);self.assertFalse(self.http)

    def test_process_owner_loss_during_signing_certifies_unsent_without_wire(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg)
        checks=[]
        def verify():
            checks.append(True)
            if len(checks)==2:raise ValueError('EXPERIMENTAL_OWNER_SESSION_LOST')
            return True
        self.provider.startup_owner=SimpleNamespace(verify=verify)
        result=self.service.tick()
        self.assertEqual(result['status'],'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        self.assertEqual(len(checks),2);self.assertFalse(self.http)
        request=next(iter(self.store.load()['requests'].values()))
        self.assertEqual(request['phase'],'ABORTED_UNSENT')

    def configure_attested_release(self):
        from .test_experimental_live_release import environment
        from .experimental_live_release import ReleaseLoader,HANDOVER
        env=environment()
        env.update(HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED='true',
            HL_TESTNET_EXPERIMENTAL_HANDOVER=HANDOVER,
            HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE=self.release['release_id'],
            HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE=contract.iso_ms(self.release['not_before_ms']),
            HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL=contract.iso_ms(self.release['entry_expires_at_ms']))
        loader=ReleaseLoader(env)
        self.worker.release_loader=self.service.release_loader=self.port.release_loader=loader
        return env

    def test_attestation_removed_during_signing_aborts_entry_without_wire(self):
        env=self.configure_attested_release()
        self.seed(r2732_message(entry=2.3,decision_ms=T-60000))
        def sign(*args):
            env.pop('HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE')
            return dict(r='0x1',s='0x2',v=27)
        with patch.dict('sys.modules',{'hyperliquid.utils.signing':SimpleNamespace(sign_l1_action=sign)}):
            result=self.service.tick()
        self.assertEqual(result['status'],'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        self.assertFalse(self.http)

    def test_attestation_removal_keeps_owned_fill_protection_running(self):
        env=self.configure_attested_release()
        self.seed(r2732_message(entry=2.3,decision_ms=T-60000))
        self.assertEqual(self.service.tick().get('operation'),'ENTRY')
        env.pop('HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE')
        for _ in range(3):self.service.tick()
        self.assertEqual({o['leg'] for o in self.oracle.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests),1)

    def test_unknown_stop_gets_independent_deadline_close_without_entry_replay(self):
        from .emergency_close import DEADLINE_MS
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        self.service.tick(); self.drop_stop = True
        result = self.service.tick(); self.assertIn('UNKNOWN', result['status'])
        self.oracle.t += DEADLINE_MS+1
        result = self.service.tick()
        self.assertEqual(result.get('operation'), 'EMERGENCY_CLOSE', result)
        self.service.tick()
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.oracle.requests), 1)
        self.assertEqual(sum(r['proposal']['operation']=='EMERGENCY_CLOSE' for r in self.oracle.requests), 1)
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(core._remaining(trade), 0)
        # An uncertain old STOP is retained for reconciliation, never invented
        # as terminal merely because the actual position is now flat.
        self.assertTrue(any(r['phase']=='OUTCOME_UNKNOWN' and r['proposal']['operation']=='CREATE_EXIT'
                            for r in self.store.load()['requests'].values()))

    def test_wsgi_rejects_wrong_method_oversized_body_and_auth_without_collecting(self):
        for values, expected in ((dict(REQUEST_METHOD='GET'), '405'),
                                (dict(CONTENT_LENGTH='9999999'), '413'),
                                (dict(CONTENT_LENGTH='2', **{'wsgi.input': io.BytesIO(b'{}')}), '403')):
            captured=[]
            env = dict(PATH_INFO=intake.PATH, REQUEST_METHOD='POST', CONTENT_TYPE='application/json',
                       CONTENT_LENGTH='0', **{'wsgi.input': io.BytesIO(b'')})
            env.update(values)
            self.service.application(env, lambda status, headers: captured.append(status))
            self.assertTrue(captured[0].startswith(expected))
        self.assertEqual(self.raw.calls, []); self.assertEqual(self.http, [])

    def test_unconfigured_composition_never_constructs_database(self):
        with patch.object(PostgresJournal, 'from_env', side_effect=AssertionError('NO_DB')):
            with self.assertRaisesRegex(ValueError, 'EXPLICIT_EXPERIMENTAL_RELEASE'):
                compose({}, price_evidence=None, safety_provider=None)
