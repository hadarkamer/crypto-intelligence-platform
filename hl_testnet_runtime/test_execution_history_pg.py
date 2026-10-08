"""Real loopback PostgreSQL + Testnet-domain lifecycle/history integration.

Only the exchange endpoints, budget token and signing transport are synthetic.
The provider, final proposal replay, dispatch review, domain checks, SQL locks,
nonce allocation and archive are real. These tests never connect to an exchange
and cannot select a non-CI database through PostgresJournal.for_ci.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
import os
import threading
import unittest
from unittest.mock import patch

from approved_alert_fixtures import BASE
from . import experimental_execution_runtime as core
from . import experimental_execution_dispatch as boundary
from . import experimental_live_runtime as runtime
from . import experimental_live_release as releases
from . import experimental_live_state as live_state
from . import filled_quantity_dispatch as wire
from .experimental_live_dispatch import LiveDispatchPort
from .experimental_live_provider import LiveEvidenceProvider
from .filled_dispatch_store import DispatchStore, SCHEMA as DISPATCH_SCHEMA
from .postgres_journal import PostgresJournal
from .test_approved_alert_lifecycle import CurrentOnlyExchange, alert, cancellation
from .test_experimental_live_provider import (
    ENV, LegacyFixture, RawTestnetFixture, UnavailablePrices,
)
from .test_experimental_live_service import SyntheticSafety
from .test_experimental_live_state import LIVE_ROUTES

CI = os.environ.get('HL_JOURNAL_CI_URL')


@unittest.skipUnless(CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class ExecutionHistoryPostgresTests(unittest.TestCase):
    def setUp(self):
        # for_ci validates loopback + hl_journal_ci BEFORE any connection or
        # schema reset. No production DSN/env is consulted by this fixture.
        self.journal = PostgresJournal.for_ci(CI)
        for target in ('socket.create_connection', 'http.client.HTTPSConnection',
                       'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body',
                       'hl_testnet_runtime.two_account_execution.wallet_for_role'):
            blocker = patch(target, side_effect=AssertionError('NO_EXCHANGE_IO_OR_KEYS'))
            blocker.start(); self.addCleanup(blocker.stop)
        self.journal.bootstrap()
        DispatchStore(self.journal).initialize()
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {live_state.PG_SCHEMA} CASCADE')
            conn.execute(f'DELETE FROM {DISPATCH_SCHEMA}.nonces WHERE agent=ANY(%s)',
                         ([row['agent'] for row in LIVE_ROUTES.values()],))
        self.store = live_state.TestnetExecutionState.for_ci(self.journal)
        self.store.initialize(LIVE_ROUTES, not_before_ms=BASE - 60000)
        self.store.initialize_history()
        self.oracle = CurrentOnlyExchange(BASE + 1000)
        self.oracle_requests = {}
        self.replayed_entries = []
        self.dispatch_errors = []
        self.release = dict(domain='testnet', release_id='a' * 64,
            dispatch_enabled=True, protection_enabled=True, entries_enabled=True,
            entry_policy=releases.CONTINUOUS_ENTRY, entry_expires_at_ms=None,
            not_before_ms=BASE - 60000,
            routes={role: row['account'] for role, row in LIVE_ROUTES.items()})
        self.reconnect()

    def oracle_state(self):
        # Retained requests are *exchange fixture facts* used to construct raw
        # orderStatus payloads. They never return to active worker ownership.
        value = self.store.load()
        value['requests'] = {**self.oracle_requests, **value['requests']}
        return value

    def reconnect(self):
        self.store = live_state.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        budget = object()
        self.raw = RawTestnetFixture(self.oracle, self.oracle_state, budget)
        self.provider = LiveEvidenceProvider(ENV, legacy_store=LegacyFixture(),
            experimental_store=self.store, price_evidence=UnavailablePrices(),
            budget=budget, clock=self.oracle.now, info_reader=self.raw,
            public_reader=self.raw, lookup_reader=self.raw.lookup,
            safety_provider=SyntheticSafety(self.release))
        self.port = object.__new__(LiveDispatchPort)
        self.port.domain = 'testnet'
        self.port.now = self.oracle.now
        self.port.reserve_transport = lambda proposal: wire.TransportAdmission(
            proposal, core._Permit(self.oracle.now, self.oracle.now()))

        def send(request, *, admission):
            try:
                calls = len(self.raw.calls)
                context = self.provider.dispatch_context(request)
                boundary.review(request, context, now_ms=self.oracle.now())
                self.assertEqual(len(self.raw.calls), calls,
                                 'Final replay must not make exchange reads')
                if request['proposal']['operation'] == 'ENTRY':
                    self.replayed_entries.append(request['request_id'])
                claimed = self.store.claim_transport(request, 'b' * 32)
                self.oracle_requests[claimed['request_id']] = deepcopy(claimed)
                return self.oracle.send(claimed, admission=admission)
            except Exception as error:
                self.dispatch_errors.append(error)
                raise
        self.port.send = send
        self.worker = runtime.TestnetExecutionRuntime(self.store, self.provider,
            self.port, release_loader=lambda: deepcopy(self.release), mode=runtime.MODE)

    def cycle(self, *, entries=False):
        result = self.worker.run_once(entries_enabled=entries)
        if self.dispatch_errors:
            raise self.dispatch_errors.pop(0)
        return result

    def start(self, value):
        account = self.release['routes']['long_account' if value['side'] == 'LONG' else 'short_account']
        self.oracle.mark[core._lane(account, value['symbol'])] = value['entry']
        self.worker.receive([value])
        self.assertEqual(self.cycle(entries=True).get('operation'), 'ENTRY')
        return self.oracle.oid('ENTRY')

    def close_first(self):
        value = alert(cycle='pg-history-first')
        oid = self.start(value)
        quantity = self.oracle.orders[oid]['view']['wire_order']['s']
        self.oracle.fill(oid, quantity)
        self.assertEqual(self.cycle().get('operation'), 'CREATE_EXIT')
        self.assertEqual(self.cycle().get('operation'), 'CREATE_EXIT')
        self.cycle()
        self.oracle.fill(self.oracle.oid('TAKE_PROFIT'), quantity)
        self.assertEqual(self.cycle().get('operation'), 'CANCEL')
        self.cycle()
        # The first post-cancel snapshot proves the original OID terminal and
        # settles the cancel request. Finality is evaluated before that
        # settlement, so it needs the next complete reconciled snapshot.
        settled = self.store.load()
        cancel_requests = [request for request in settled['requests'].values()
                           if request['proposal']['operation'] == 'CANCEL']
        self.assertEqual(len(cancel_requests), 1)
        self.assertEqual(cancel_requests[0]['phase'], 'OBSERVED')
        self.cycle()
        current = self.store.load()
        self.assertEqual(current['trades'][value['occurrence_id']]['phase'], 'CLOSED')
        self.assertEqual(core._remaining(current['trades'][value['occurrence_id']]), Decimal(0))
        return value, current

    def compact(self, store=None):
        return (store or self.store).compact_history(now_ms=self.oracle.now(), force=True)

    def test_native_durable_card_survives_archive_reconnect_and_late_cancel(self):
        value, _ = self.close_first()
        cid = value['occurrence_id']
        expected = self.worker.trade_card(cid)['card']
        self.assertEqual(expected['domain'], 'testnet')
        self.assertEqual(self.worker.report()['cards'], [expected])
        self.compact(); self.reconnect()
        self.assertEqual(self.worker.trade_card(cid)['card'], expected)
        self.assertEqual(self.worker.trade_card(cid)['location'], 'archived')
        self.assertEqual(self.worker.history_report()['cards'], [expected])
        cancel = cancellation(value, self.oracle.now())
        self.worker.receive([cancel]); self.reconnect()
        result = self.worker.trade_card(cid)
        self.assertEqual(result['card'], expected)
        self.assertEqual(result['current_source']['cancellation'], cancel)
        self.assertEqual(result['source_updates'][0]['source']['cancellation'], cancel)
        self.assertNotIn(cid, self.store.load()['trades'])

    def test_closed_archive_reopen_same_symbol_final_dispatch_replay_and_domain(self):
        first, before = self.close_first()
        cid = first['occurrence_id']
        count = len(before['requests'])
        old_orders = deepcopy(self.oracle.orders)
        self.assertEqual(self.compact()['archived'], 1)
        self.assertEqual(self.store.load()['archived_request_count'], count)
        self.reconnect()
        bundle = self.store.archive_record(cid)
        self.assertEqual(bundle['domain'], 'testnet')
        self.assertEqual(bundle['source']['domain'], 'testnet')
        self.assertEqual({row['domain'] for row in bundle['requests'].values()}, {'testnet'})
        self.assertEqual(bundle['requests'], before['requests'])
        self.assertEqual(bundle['trade'], before['trades'][cid])
        self.assertEqual(self.worker.history_report()['domain'], 'testnet')
        self.assertEqual(self.worker.history_report()['trades'][0]['status'], 'CLOSED')
        self.assertNotIn(cid, self.store.load()['trades'])

        second = alert(cycle='pg-history-successor')
        self.raw.calls.clear()
        self.start(second)
        current = self.store.load()
        self.assertIn(second['occurrence_id'], current['trades'])
        self.assertEqual(current['archived_request_count'], count)
        self.assertEqual(len(current['requests']), 1)
        self.assertEqual(len(self.replayed_entries), 2)
        self.assertNotEqual(self.replayed_entries[0], self.replayed_entries[1])
        self.assertEqual(len({r['proposal']['action']['orders'][0]['c']
            for r in self.oracle.requests if r['proposal']['operation'] == 'ENTRY'}), 2)
        for oid, row in old_orders.items():
            self.assertEqual(self.oracle.orders[oid], row)
        self.assertEqual(self.worker.history_report()['archived_occurrences'], [cid])
        self.assertFalse(any(kind == 'lookup' and cloid in {
            row['view']['cloid'] for row in old_orders.values()}
            for kind, _account, cloid in self.raw.calls))

    def test_concurrent_archive_and_late_receipt_use_same_testnet_transaction_lock(self):
        value, before = self.close_first()
        cid = value['occurrence_id']
        other = live_state.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        cancel = cancellation(value, self.oracle.now())
        barrier = threading.Barrier(2)
        def compact():
            barrier.wait(timeout=10)
            return self.compact(other)
        def receive():
            barrier.wait(timeout=10)
            return self.worker.receive([cancel, value])
        with ThreadPoolExecutor(2) as pool:
            results = [pool.submit(compact), pool.submit(receive)]
            for result in results:
                result.result(timeout=30)
        self.reconnect()
        hot = self.store.load()
        cold = self.store.archive_record(cid)
        self.assertNotIn(cid, hot['sources'])
        self.assertNotIn(cid, hot['trades'])
        self.assertEqual(hot['history']['archived_count'], 1)
        self.assertEqual(hot['archived_request_count'], len(before['requests']))
        self.assertEqual(cold['current_source']['domain'], 'testnet')
        self.assertEqual(cold['current_source']['entry_permission'], 'RETIRED')
        self.assertEqual(cold['current_source']['cancellation'], cancel)
        prior_count = len(self.oracle.requests)
        self.worker.receive([value, cancel, value])
        self.cycle(entries=True)
        self.assertEqual(len(self.oracle.requests), prior_count)
        self.assertEqual(len(self.replayed_entries), 1)

    def check_observed_archived_mutation(self):
        first, _ = self.close_first()
        self.compact(); self.reconnect()
        oid = self.oracle.oid('ENTRY')
        # A flat lane intentionally does not rescan old fills. Establish and
        # reconcile an unfilled owned request, whose normal overlap collection
        # actually observes old facts; only then change one of those facts.
        second = alert(cycle='active-unfilled-history-observer')
        self.start(second)
        self.cycle()
        current = self.store.load()
        self.assertFalse(current['trades'][second['occurrence_id']]['entry_fills'])
        self.assertTrue(all(r['phase'] == 'OBSERVED' for r in current['requests'].values()))
        third = alert(symbol='ETH', cycle='must-block-after-observed-history-change')
        self.worker.receive([third])
        old_fill = self.oracle.orders[oid]['view']['fills'][0]
        original_price = old_fill['price']
        old_fill['price'] = '92'
        before = len(self.oracle.requests)
        self.cycle(entries=True)
        self.assertEqual(len(self.oracle.requests), before)
        self.assertEqual(len(self.replayed_entries), 2)
        self.assertNotIn(third['occurrence_id'], self.store.load()['trades'])
        self.assertTrue(self.store.load()['blocked_lanes'])
        self.assertIn('ARCHIVED_TERMINAL_FILL_CHANGED',
                      self.provider._last['context']['blocked_lanes'].values())
        self.assertEqual(self.store.archive_record(first['occurrence_id'])['domain'], 'testnet')
        # The immutable original is retained. A restored consistent observation
        # can reconcile the lane again and admit unrelated ETH normally.
        old_fill['price'] = original_price
        self.assertEqual(self.cycle(entries=True).get('operation'), 'ENTRY')
        self.assertIn(third['occurrence_id'], self.store.load()['trades'])
        self.assertFalse(self.store.load()['blocked_lanes'])

    def test_observed_archived_fill_mutation_blocks_further_dispatch(self):
        self.check_observed_archived_mutation()

    def test_partial_fill_and_unsettled_exit_never_archive_during_protection(self):
        value = alert(cycle='partial-must-stay-hot')
        oid = self.start(value)
        self.oracle.fill(oid, '1')
        self.assertEqual(self.cycle().get('operation'), 'CREATE_EXIT')
        self.assertEqual(self.compact()['archived'], 0)
        self.reconnect()
        self.assertEqual(self.cycle().get('operation'), 'CREATE_EXIT')
        self.cycle()
        current = self.store.load()
        self.assertEqual(core._remaining(current['trades'][value['occurrence_id']]), Decimal(1))
        self.assertEqual(self.worker.history_report()['trades'], [])
        for leg in ('STOP', 'TAKE_PROFIT'):
            row = self.oracle.orders[self.oracle.oid(leg)]['view']
            self.assertEqual(row['status'], 'OPEN')
            self.assertEqual(row['wire_order']['s'], '1')


class MemoryProviderClosureSmokeTests(unittest.TestCase):
    """Exercise shared provider scenarios locally, without claiming SQL proof.

    The archive smoke uses real pure transitions with an in-memory oracle.
    PostgreSQL persistence and locking are covered only by the gated tests.
    """
    oracle_state = ExecutionHistoryPostgresTests.oracle_state
    cycle = ExecutionHistoryPostgresTests.cycle
    start = ExecutionHistoryPostgresTests.start
    close_first = ExecutionHistoryPostgresTests.close_first
    compact = ExecutionHistoryPostgresTests.compact
    check_observed_archived_mutation = ExecutionHistoryPostgresTests.check_observed_archived_mutation

    def reconnect(self):
        fixture_dsn = 'postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci'
        with patch.object(live_state.TestnetExecutionState, 'for_ci', return_value=self.store), \
                patch(__name__ + '.CI', fixture_dsn):
            ExecutionHistoryPostgresTests.reconnect(self)

    def setUp(self):
        from .test_experimental_live_runtime import MemoryTransactions
        fixture_dsn = 'postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci'
        for target in ('socket.create_connection', 'socket.socket.connect',
                       'http.client.HTTPSConnection',
                       'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body',
                       'hl_testnet_runtime.two_account_execution.wallet_for_role'):
            blocker = patch(target, side_effect=AssertionError('MEMORY_SMOKE_NO_IO_OR_KEYS'))
            blocker.start(); self.addCleanup(blocker.stop)
        memory = MemoryTransactions(LIVE_ROUTES, BASE - 60000)
        store = live_state.TestnetExecutionState.for_ci(PostgresJournal.for_ci(fixture_dsn))
        store.load = memory.load
        store.mutate = memory.mutate
        store.commit_attempt = memory.commit_attempt
        self.store, self.memory = store, memory
        self.oracle = CurrentOnlyExchange(BASE + 1000)
        self.oracle_requests, self.replayed_entries, self.dispatch_errors = {}, [], []
        self.release = dict(domain='testnet', release_id='a' * 64,
            dispatch_enabled=True, protection_enabled=True, entries_enabled=True,
            entry_policy=releases.CONTINUOUS_ENTRY, entry_expires_at_ms=None,
            not_before_ms=BASE - 60000,
            routes={role: row['account'] for role, row in LIVE_ROUTES.items()})
        self.reconnect()

    def test_real_provider_closure_waits_for_cancel_settlement_and_final_snapshot(self):
        value, state = self.close_first()
        self.assertEqual(state['domain'], 'testnet')
        self.assertEqual(state['trades'][value['occurrence_id']]['phase'], 'CLOSED')
        self.assertEqual(len(state['requests']), 4)
        self.assertEqual(len(self.replayed_entries), 1)
        self.assertTrue(all(request['phase'] == 'OBSERVED'
                            for request in state['requests'].values()))
        self.assertNotIn('history', state)

    def test_real_provider_rejects_changed_archived_fact_when_observed(self):
        """Local transition/oracle test only: no SQL, locking, or durability claim."""
        from . import experimental_execution_archive as archive
        records = {}
        store, memory = self.store, self.memory
        # This scenario receives only fresh sources. Archive deduplication and
        # concurrent receipts remain covered by the real PostgreSQL tests.
        store.mutate_sources = lambda transition, ids: memory.mutate(transition)
        def compact_history(**kwargs):
            if not kwargs.get('force'):
                return dict(archived=0)
            def transition(state):
                state.setdefault('history', dict(archived_count=0))
                count = 0
                for cid in list(state['sources']):
                    if archive._eligible(state, cid, kwargs['now_ms']):
                        record = archive._bundle(state, cid, kwargs['now_ms'])
                        records[cid] = deepcopy(record)
                        archive._prune(state, record)
                        state['history']['archived_count'] += 1
                        count += 1
                return dict(archived=count)
            return memory.mutate(transition)
        def archived_evidence(account, symbol, order_ids):
            result = {}
            for record in records.values():
                trade = record['trade']
                if trade['account'] != account or trade['symbol'] != symbol:
                    continue
                checkpoint = record['snapshots'].get('collector_checkpoints', {})
                for oid, row in trade['orders'].items():
                    if oid in order_ids:
                        result[oid] = dict(order=row,
                            collector_terminal=next((item for item in checkpoint.get('terminal_orders', [])
                                                     if item['oid'] == oid), None),
                            collector_fills=[item for item in checkpoint.get('fills', []) if item['oid'] == oid])
            return deepcopy(result)
        store.compact_history = compact_history
        store.archive_record = lambda cid: deepcopy(records.get(cid))
        store.archived_order_evidence = archived_evidence
        self.check_observed_archived_mutation()


if __name__ == '__main__':
    unittest.main()
